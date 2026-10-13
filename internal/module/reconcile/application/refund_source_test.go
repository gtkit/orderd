package application

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/gtkit/orderd/internal/models"
	"github.com/gtkit/orderd/internal/pkg/sqlutil"
	"github.com/gtkit/orderd/internal/shared/bill"
	"github.com/gtkit/orderd/internal/store"
	"github.com/gtkit/orderd/internal/store/memstore"
)

type sourceReadFault struct {
	store.Store
	id  uint64
	err error
}
type sourceReadRepos struct {
	store.Repos
	owner *sourceReadFault
}
type sourceReadTxns struct {
	store.TransactionRepo
	owner *sourceReadFault
}

func (s *sourceReadFault) Reader() store.Repos { return sourceReadRepos{s.Store.Reader(), s} }
func (r sourceReadRepos) Transactions() store.TransactionRepo {
	return sourceReadTxns{r.Repos.Transactions(), r.owner}
}
func (r sourceReadTxns) GetByID(ctx context.Context, id uint64, fu bool) (*models.PaymentTransaction, error) {
	if id == r.owner.id {
		return nil, r.owner.err
	}
	return r.TransactionRepo.GetByID(ctx, id, fu)
}

func TestBrokenRefundEvidenceDoesNotBlockValidStatementRows(t *testing.T) {
	for _, mode := range []string{"missing_source", "zero_source", "negative_source", "wrong_kind", "wrong_app", "wrong_order", "missing_order", "database_error", "timeout"} {
		t.Run(mode, func(t *testing.T) {
			m := memstore.New()
			now := time.Date(2026, 10, 12, 12, 0, 0, 0, bill.Location)
			streamAccount(m, now)
			valid := streamEntry(0)
			streamTxn(t, m, valid, now)
			e := bill.Entry{Kind: models.TxnRefund, TransactionID: "refund-txn", OriginalTransactionID: "original-pay", OutTradeNo: "original-out", OutRefundNo: "refund-out", Amount: 100, Currency: "CNY", MerchantID: "merchant", Status: "REFUND:SUCCESS"}
			source := &models.PaymentTransaction{AppID: "app", OrderNo: "order", Channel: "wechat", PayAccountID: "acc", Kind: models.TxnPay, TransactionID: e.OriginalTransactionID, OutTradeNo: e.OutTradeNo, Amount: 100, Currency: "CNY", CreatedAt: now}
			if mode == "wrong_kind" {
				source.Kind = models.TxnRefund
			}
			if mode == "wrong_app" {
				source.AppID = "other-app"
			}
			if mode == "wrong_order" {
				source.OrderNo = "other-order"
			}
			var refundID uint64
			if err := m.WithinTx(t.Context(), func(ctx context.Context, r store.Repos) error {
				if err := r.Transactions().Insert(ctx, source); err != nil {
					return err
				}
				sourceID := sqlutil.Int64(int64(source.ID))
				switch mode {
				case "missing_source":
					sourceID = sqlutil.Int64(99999)
				case "zero_source":
					sourceID = sqlutil.Int64(0)
				case "negative_source":
					sourceID = sqlutil.Int64(-1)
				case "missing_order":
					sourceID = sql.NullInt64{}
				}
				rf := &models.Refund{AppID: "app", OrderNo: "order", RefundNo: "refund", OutRefundNo: e.OutRefundNo, PayAccountID: "acc", SourceTxnID: sourceID, RefundAmount: 100, Status: models.RefundSucceeded}
				if err := r.Refunds().Insert(ctx, rf); err != nil {
					return err
				}
				refund := &models.PaymentTransaction{AppID: "app", OrderNo: "order", Channel: "wechat", PayAccountID: "acc", Kind: models.TxnRefund, RefundNo: rf.RefundNo, TransactionID: e.TransactionID, OutTradeNo: e.OutRefundNo, Amount: 100, Currency: "CNY", CreatedAt: now}
				if err := r.Transactions().Insert(ctx, refund); err != nil {
					return err
				}
				refundID = refund.ID
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			var st store.Store = m
			var injected error
			if mode == "database_error" {
				injected = errRecoveryInjected
			}
			if mode == "timeout" {
				injected = context.DeadlineExceeded
			}
			if injected != nil {
				st = &sourceReadFault{Store: m, id: source.ID, err: injected}
			}
			svc := recoveryService(st, fakeBill{b: bill.Bill{Entries: []bill.Entry{valid, e}}}, func() time.Time { return now })
			day := now.AddDate(0, 0, -1)
			result, err := svc.Reconcile(t.Context(), "app", "wechat", "acc", day)
			if injected != nil {
				if !errors.Is(err, injected) {
					t.Fatalf("storage failure swallowed: %v", err)
				}
				if b := streamBatch(t, m, day); b.PublishedRunID != "" {
					t.Fatalf("database failure published partial compare: %+v", b)
				}
				if _, _, _, err := svc.checkLocal(t.Context(), refundID); !errors.Is(err, injected) {
					t.Fatalf("local verifier swallowed transient storage failure: %v", err)
				}
				if err := svc.verifyLocal(t.Context(), "wechat", "acc"); err != nil {
					t.Fatal(err)
				}
				// 查询异常保留持久重试，不能把暂时读失败写成已完成证据。
				if c, err := m.Reader().Reconcile().ClaimLocalTxn(t.Context(), "wechat", "acc", "probe", now.Add(retryDelay), now.Add(2*retryDelay)); err != nil || c == nil {
					t.Fatalf("storage failure lost retry: %+v %v", c, err)
				}
			} else {
				if err != nil || result.Matched != 1 || result.DiffCount != 1 {
					t.Fatalf("one broken historical reference blocked whole statement: %+v %v", result, err)
				}
				for _, d := range m.Diffs() {
					if d.BatchID != 0 && d.Kind == models.TxnRefund && d.DiffType != models.DiffStatus {
						t.Fatalf("broken source did not become manual status: %+v", d)
					}
				}
			}
		})
	}
}
