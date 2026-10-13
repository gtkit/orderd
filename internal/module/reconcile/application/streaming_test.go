package application

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/gtkit/orderd/internal/models"
	"github.com/gtkit/orderd/internal/pkg/sqlutil"
	"github.com/gtkit/orderd/internal/shared/bill"
	"github.com/gtkit/orderd/internal/store"
	"github.com/gtkit/orderd/internal/store/memstore"
)

type streamSource func(context.Context, func(bill.Entry) error) (bill.Summary, error)

func (f streamSource) Fetch(ctx context.Context, _, _ string, _ time.Time, emit func(bill.Entry) error) (bill.Summary, error) {
	return f(ctx, emit)
}
func streamEntry(n int) bill.Entry {
	return bill.Entry{Kind: models.TxnPay, TransactionID: fmt.Sprintf("tx-%d", n), OutTradeNo: fmt.Sprintf("out-%d", n), Amount: 100, Currency: "CNY", Status: "SUCCESS", MerchantID: "merchant", ProviderAppID: "provider-app"}
}
func streamAccount(m *memstore.Mem, now time.Time) models.AppPayAccount {
	a := models.AppPayAccount{AppID: "app", Channel: "wechat", PayAccountID: "acc", MchID: "merchant", PayAppID: "provider-app", CreatedAt: now.AddDate(0, 0, -1)}
	m.SeedAccount(a)
	return a
}
func streamBill(count int, calls *int) streamSource {
	return func(ctx context.Context, emit func(bill.Entry) error) (bill.Summary, error) {
		*calls++
		for n := range count {
			if err := ctx.Err(); err != nil {
				return bill.Summary{}, err
			}
			if err := emit(streamEntry(n)); err != nil {
				return bill.Summary{}, err
			}
		}
		return bill.Summary{Count: int64(count), PayAmount: int64(count) * 100, DigestAlgorithm: "sha256", Digest: "audit-digest"}, nil
	}
}
func streamBatch(t *testing.T, m *memstore.Mem, day time.Time) *models.ReconcileBatch {
	t.Helper()
	b, err := m.Reader().Reconcile().GetBatchByAccountDate(t.Context(), "wechat", "acc", day, algorithmVersion)
	if err != nil || b == nil {
		t.Fatalf("batch missing: %v", err)
	}
	return b
}
func streamTxn(t *testing.T, m *memstore.Mem, e bill.Entry, now time.Time) {
	t.Helper()
	err := m.WithinTx(t.Context(), func(ctx context.Context, r store.Repos) error {
		return r.Transactions().Insert(ctx, &models.PaymentTransaction{AppID: "app", OrderNo: "order", PayAccountID: "acc", Channel: "wechat", Kind: models.TxnPay, TransactionID: e.TransactionID, OutTradeNo: e.OutTradeNo, Amount: e.Amount, Currency: e.Currency, PaidAt: sqlutil.Time(now.AddDate(-1, 0, 0)), CreatedAt: now})
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestStreamingPartialTailAndMissingStatementNeverPublish(t *testing.T) {
	for _, mode := range []string{"tail", "summary", "no_statement", "duplicate"} {
		t.Run(mode, func(t *testing.T) {
			m := memstore.New()
			now := time.Date(2026, 10, 12, 12, 0, 0, 0, bill.Location)
			streamAccount(m, now)
			day := now.AddDate(0, 0, -1)
			source := streamSource(func(_ context.Context, emit func(bill.Entry) error) (bill.Summary, error) {
				if mode == "no_statement" {
					return bill.Summary{NoStatement: true}, nil
				}
				for n := range 101 {
					if mode == "duplicate" && n == 100 {
						n = 0
					}
					if err := emit(streamEntry(n)); err != nil {
						return bill.Summary{}, err
					}
				}
				if mode == "tail" {
					return bill.Summary{}, errors.New("truncated tail")
				}
				if mode == "summary" {
					return bill.Summary{Count: 101, PayAmount: 10099}, nil
				}
				return bill.Summary{Count: 101, PayAmount: 10100}, nil
			})
			svc := recoveryService(m, source, func() time.Time { return now })
			if _, err := svc.Reconcile(t.Context(), "app", "wechat", "acc", day); err == nil {
				t.Fatal("bad complete source accepted")
			}
			b := streamBatch(t, m, day)
			visible, err := m.Reader().Reconcile().ListDiffs(t.Context(), store.DiffFilter{}, 0, 100)
			if err != nil || b.PublishedRunID != "" || b.ImportComplete || len(visible) != 0 {
				t.Fatalf("partial evidence published: %+v visible=%v err=%v", b, visible, err)
			}
		})
	}
}

func TestStreamingSealedRunResumesBoundedPagesWithoutRefetch(t *testing.T) {
	m := memstore.New()
	now := time.Date(2026, 10, 12, 12, 0, 0, 0, bill.Location)
	streamAccount(m, now)
	day := now.AddDate(0, 0, -1)
	calls := 0
	svc := recoveryService(m, streamBill(901, &calls), func() time.Time { return now })
	if _, err := svc.Reconcile(t.Context(), "app", "wechat", "acc", day); err != nil {
		t.Fatal(err)
	}
	b := streamBatch(t, m, day)
	run := b.RunID
	if !b.ImportComplete || b.PublishedRunID != "" || b.DiffCount != 800 || b.LockedBy != "" {
		t.Fatalf("first fixed slice: %+v", b)
	}
	visible, _ := m.Reader().Reconcile().ListDiffs(t.Context(), store.DiffFilter{}, 0, 100)
	if len(visible) != 0 {
		t.Fatal("unpublished compare page visible")
	}
	restarted := New(svc.d)
	res, err := restarted.Reconcile(t.Context(), "app", "wechat", "acc", day)
	if err != nil || res.DiffCount != 901 {
		t.Fatalf("resume: %+v %v", res, err)
	}
	b = streamBatch(t, m, day)
	if calls != 1 || b.RunID != run || b.PublishedRunID != run || b.SourceCount != 901 || b.SourceAmount != 90100 || b.SourceDigest != "audit-digest" {
		t.Fatalf("refetched or double-counted: calls=%d %+v", calls, b)
	}
	visible, _ = m.Reader().Reconcile().ListDiffs(t.Context(), store.DiffFilter{}, 0, 100)
	if len(visible) != 100 {
		t.Fatalf("published page=%d", len(visible))
	}
}

func TestLatePaymentUsesSealedHistoricalEvidenceAndResolvesPendingAudit(t *testing.T) {
	m := memstore.New()
	now := time.Date(2026, 10, 12, 12, 0, 0, 0, bill.Location)
	streamAccount(m, now)
	day := now.AddDate(-1, 0, 0)
	calls := 0
	svc := recoveryService(m, streamBill(1, &calls), func() time.Time { return now })
	if _, err := svc.Reconcile(t.Context(), "app", "wechat", "acc", day); err != nil {
		t.Fatal(err)
	}
	old := streamBatch(t, m, day)
	streamTxn(t, m, streamEntry(0), now)
	if err := svc.verifyLocal(t.Context(), "wechat", "acc"); err != nil {
		t.Fatal(err)
	}
	svc.d.Bill = streamSource(func(context.Context, func(bill.Entry) error) (bill.Summary, error) {
		t.Error("re-downloaded expired historical bill")
		return bill.Summary{}, errors.New("history expired")
	})
	res, err := svc.Reconcile(t.Context(), "app", "wechat", "acc", day)
	if err != nil || res.Matched != 1 || res.DiffCount != 0 {
		t.Fatalf("late match: %+v %v", res, err)
	}
	current := streamBatch(t, m, day)
	if current.RunID == old.RunID || current.SourceRunID != old.SourceRunID || calls != 1 {
		t.Fatalf("source not reused: before=%+v after=%+v", old, current)
	}
	diffs := m.Diffs()
	if len(diffs) != 1 || diffs[0].HandleStatus != models.DiffHandleResolved || diffs[0].HandledBy != models.OpsActorSystem || len(m.Audits()) != 1 {
		t.Fatalf("pending old diff not resolved with audit: %+v audits=%v", diffs, m.Audits())
	}
	if err := svc.verifyLocal(t.Context(), "wechat", "acc"); err != nil {
		t.Fatal(err)
	}
	if b := streamBatch(t, m, day); b.Status != models.BatchDone {
		t.Fatalf("completed local job triggered endless recompare: %+v", b)
	}
}

func TestRecomparePreservesManualDisposition(t *testing.T) {
	for _, status := range []models.DiffHandleStatus{models.DiffHandleProcessing, models.DiffHandleResolved, models.DiffHandleIgnored} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			m := memstore.New()
			now := time.Date(2026, 10, 12, 12, 0, 0, 0, bill.Location)
			streamAccount(m, now)
			calls := 0
			day := now.AddDate(0, 0, -1)
			svc := recoveryService(m, streamBill(1, &calls), func() time.Time { return now })
			if _, err := svc.Reconcile(t.Context(), "app", "wechat", "acc", day); err != nil {
				t.Fatal(err)
			}
			old := m.Diffs()[0]
			if err := m.WithinTx(t.Context(), func(ctx context.Context, r store.Repos) error {
				if err := r.Reconcile().UpdateDiffHandling(ctx, old.ID, status, "operator", now, "keep this note"); err != nil {
					return err
				}
				return r.Reconcile().RequestRecheck(ctx, old.BatchID, old.EntryID)
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := svc.Reconcile(t.Context(), "app", "wechat", "acc", day); err != nil {
				t.Fatal(err)
			}
			visible, err := m.Reader().Reconcile().ListDiffs(t.Context(), store.DiffFilter{}, 0, 100)
			if err != nil || len(visible) != 1 || visible[0].ID == old.ID || visible[0].HandleStatus != status || visible[0].HandleRemark != "keep this note" || calls != 1 {
				t.Fatalf("manual disposition lost: %+v calls=%d err=%v", visible, calls, err)
			}
		})
	}
}

func TestLocalEvidenceStatusAndLaterResolution(t *testing.T) {
	m := memstore.New()
	now := time.Date(2026, 10, 12, 12, 0, 0, 0, bill.Location)
	streamAccount(m, now)
	calls := 0
	svc := recoveryService(m, streamBill(1, &calls), func() time.Time { return now })
	e := streamEntry(0)
	streamTxn(t, m, e, now)
	if err := svc.verifyLocal(t.Context(), "wechat", "acc"); err != nil {
		t.Fatal(err)
	}
	if ds := m.Diffs(); len(ds) != 1 || ds[0].BatchID != 0 || ds[0].DiffType != models.DiffStatus {
		t.Fatalf("no bill must remain manual: %+v", ds)
	}
	if _, err := svc.Reconcile(t.Context(), "app", "wechat", "acc", now.AddDate(-1, 0, 0)); err != nil {
		t.Fatal(err)
	}
	now = now.Add(retryDelay)
	if err := svc.verifyLocal(t.Context(), "wechat", "acc"); err != nil {
		t.Fatal(err)
	}
	if ds := m.Diffs(); len(ds) != 1 || ds[0].HandleStatus != models.DiffHandleResolved || len(m.Audits()) != 1 {
		t.Fatalf("stale local-only diff left pending: %+v", ds)
	}
}

func TestRefundCrossDayFullIdentity(t *testing.T) {
	for _, channel := range []string{"wechat", "alipay"} {
		for _, mismatch := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/%v", channel, mismatch), func(t *testing.T) {
				m := memstore.New()
				now := time.Date(2026, 10, 12, 12, 0, 0, 0, bill.Location)
				a := streamAccount(m, now)
				a.Channel = channel
				m.SeedAccount(a)
				day := now.AddDate(0, 0, -2)
				e := bill.Entry{Kind: models.TxnRefund, TransactionID: "gw-refund", OriginalTransactionID: "original-pay", OutTradeNo: "original-out", OutRefundNo: "out-refund", Amount: 100, Currency: "CNY", Status: "REFUND:PROCESSING", MerchantID: "merchant", EventAt: day}
				txnID := e.TransactionID
				if channel == "alipay" {
					e.TransactionID = ""
					txnID = "refund:" + e.OutRefundNo
				}
				err := m.WithinTx(t.Context(), func(ctx context.Context, r store.Repos) error {
					source := &models.PaymentTransaction{AppID: "app", OrderNo: "order", PayAccountID: "acc", Channel: channel, Kind: models.TxnPay, TransactionID: e.OriginalTransactionID, OutTradeNo: e.OutTradeNo, Amount: 100, Currency: "CNY", CreatedAt: now}
					if err := r.Transactions().Insert(ctx, source); err != nil {
						return err
					}
					rf := &models.Refund{AppID: "app", OrderNo: "order", RefundNo: "refund", OutRefundNo: e.OutRefundNo, SourceTxnID: sqlutil.Int64(int64(source.ID)), PayAccountID: "acc", RefundAmount: 100, Status: models.RefundSucceeded}
					if err := r.Refunds().Insert(ctx, rf); err != nil {
						return err
					}
					return r.Transactions().Insert(ctx, &models.PaymentTransaction{AppID: "app", OrderNo: "order", RefundNo: "refund", Channel: channel, PayAccountID: "acc", Kind: models.TxnRefund, TransactionID: txnID, OutTradeNo: e.OutRefundNo, Amount: 100, Currency: "CNY", PaidAt: sqlutil.Time(now.AddDate(0, 0, -1)), CreatedAt: now})
				})
				if err != nil {
					t.Fatal(err)
				}
				if mismatch {
					e.OriginalTransactionID = "wrong-original"
				}
				svc := recoveryService(m, streamSource(func(_ context.Context, emit func(bill.Entry) error) (bill.Summary, error) {
					if err := emit(e); err != nil {
						return bill.Summary{}, err
					}
					return bill.Summary{Count: 1, RefundAmount: 100}, nil
				}), func() time.Time { return now })
				res, err := svc.Reconcile(t.Context(), "app", channel, "acc", day)
				if err != nil {
					t.Fatal(err)
				}
				if !mismatch && res.Matched != 1 || mismatch && res.DiffCount != 1 {
					t.Fatalf("refund comparison: %+v", res)
				}
				for _, d := range m.Diffs() {
					if d.Kind == models.TxnRefund && d.DiffType == models.DiffShort {
						t.Fatalf("acceptance/success cross-day treated as refund short: %+v", d)
					}
				}
			})
		}
	}
}

func TestAccountIdentityAndRevokedEvidenceFailClosed(t *testing.T) {
	for _, mode := range []string{"missing_account", "merchant", "provider_app", "revoked"} {
		t.Run(mode, func(t *testing.T) {
			m := memstore.New()
			now := time.Now()
			if mode != "missing_account" {
				streamAccount(m, now)
			}
			e := streamEntry(0)
			if mode == "merchant" {
				e.MerchantID = "wrong"
			}
			if mode == "provider_app" {
				e.ProviderAppID = "other-app"
			}
			if mode == "revoked" {
				e.Status = "REVOKED:SUCCESS"
			}
			streamTxn(t, m, e, now)
			svc := recoveryService(m, fakeBill{b: bill.Bill{Entries: []bill.Entry{e}}}, func() time.Time { return now })
			res, err := svc.Reconcile(t.Context(), "app", "wechat", "acc", now.AddDate(0, 0, -1))
			if err != nil || res.DiffCount != 1 {
				t.Fatalf("unsafe evidence matched: %+v %v", res, err)
			}
			for _, d := range m.Diffs() {
				if d.BatchID != 0 && d.DiffType != models.DiffStatus {
					t.Fatalf("not manual status difference: %+v", d)
				}
			}
		})
	}
}

func TestManualDispositionDuringStagedComparePropagatesBeforePublish(t *testing.T) {
	m := memstore.New()
	now := time.Date(2026, 10, 12, 12, 0, 0, 0, bill.Location)
	streamAccount(m, now)
	day := now.AddDate(0, 0, -1)
	calls := 0
	svc := recoveryService(m, streamBill(801, &calls), func() time.Time { return now })
	for range 2 {
		if _, err := svc.Reconcile(t.Context(), "app", "wechat", "acc", day); err != nil {
			t.Fatal(err)
		}
	}
	old := m.Diffs()[0]
	if err := m.WithinTx(t.Context(), func(ctx context.Context, r store.Repos) error {
		return r.Reconcile().RequestRecheck(ctx, old.BatchID, old.EntryID)
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Reconcile(t.Context(), "app", "wechat", "acc", day); err != nil {
		t.Fatal(err)
	}
	b := streamBatch(t, m, day)
	if b.RunID == b.PublishedRunID {
		t.Fatal("fixture must pause with unpublished staged copy")
	}
	if err := m.WithinTx(t.Context(), func(ctx context.Context, r store.Repos) error {
		if _, err := r.Reconcile().GetDiff(ctx, old.ID, true); err != nil {
			return err
		}
		return r.Reconcile().UpdateDiffHandling(ctx, old.ID, models.DiffHandleIgnored, "human", now, "arrived after stage")
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Reconcile(t.Context(), "app", "wechat", "acc", day); err != nil {
		t.Fatal(err)
	}
	visible, err := m.Reader().Reconcile().ListDiffs(t.Context(), store.DiffFilter{}, 0, 100)
	if err != nil || len(visible) != 100 || visible[0].HandleStatus != models.DiffHandleIgnored || visible[0].HandleRemark != "arrived after stage" || calls != 1 {
		t.Fatalf("late manual status not propagated: %+v calls=%d err=%v", visible[0], calls, err)
	}
	if err := m.WithinTx(t.Context(), func(ctx context.Context, r store.Repos) error {
		_, err := r.Reconcile().GetDiff(ctx, old.ID, true)
		return err
	}); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("old visible ID remained actionable after publication: %v", err)
	}
}

func TestUnconfirmedRefundEvidenceIsStatusDifference(t *testing.T) {
	for _, status := range []string{"REFUND:PROCESSING", "REFUND:FAIL", "REFUND:CHANGE", "SUCCESS", "REFUND:SUCCESS"} {
		t.Run(status, func(t *testing.T) {
			m := memstore.New()
			now := time.Date(2026, 10, 12, 12, 0, 0, 0, bill.Location)
			streamAccount(m, now)
			e := bill.Entry{Kind: models.TxnRefund, TransactionID: "refund-txn", OriginalTransactionID: "original", OutTradeNo: "original-out", OutRefundNo: "refund-out", Amount: 100, Currency: "CNY", MerchantID: "merchant", Status: status}
			svc := recoveryService(m, fakeBill{b: bill.Bill{Entries: []bill.Entry{e}}}, func() time.Time { return now })
			if _, err := svc.Reconcile(t.Context(), "app", "wechat", "acc", now.AddDate(0, 0, -1)); err != nil {
				t.Fatal(err)
			}
			want := models.DiffStatus
			if status == "SUCCESS" || status == "REFUND:SUCCESS" {
				want = models.DiffSurplus
			}
			ds := m.Diffs()
			if len(ds) != 1 || ds[0].DiffType != want {
				t.Fatalf("historical refund status %s classified as successful money: %+v", status, ds)
			}
		})
	}
}
