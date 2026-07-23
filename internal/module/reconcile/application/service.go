// Package application(reconcile)实现渠道账单对账(DESIGN §14):拉账单 → 与本地财务流水比对 →
// 差异(长款/短款/金额不一致)落 reconcile_diffs + 更新批次 + 告警。人工处理入口在 diffs 表。
package application

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/gtkit/orderd/internal/models"
	"github.com/gtkit/orderd/internal/pkg/sqlutil"
	"github.com/gtkit/orderd/internal/shared/alert"
	"github.com/gtkit/orderd/internal/shared/bill"
	"github.com/gtkit/orderd/internal/store"
)

// Deps 是对账服务依赖。
type Deps struct {
	Store      store.Store
	Bill       bill.Source
	Alerter    alert.Alerter
	Clock      func() time.Time
	NewBatchNo func() string
}

// Service 对账应用服务。
type Service struct{ d Deps }

// New 构造对账服务。
func New(d Deps) *Service {
	if d.Clock == nil {
		d.Clock = time.Now
	}
	return &Service{d: d}
}

// Result 对账结果摘要。
type Result struct {
	BatchNo   string
	Matched   int
	DiffCount int
	Skipped   bool // 该账期已对账完成,跳过
}

// Reconcile 对某账号某日对账;幂等(已完成则跳过,差异写入去重)。
func (s *Service) Reconcile(ctx context.Context, appID, channel, payAccountID string, billDate time.Time) (Result, error) {
	// 1. 批次防重:已完成则跳过。
	existing, err := s.d.Store.Reader().Reconcile().GetBatchByAccountDate(ctx, channel, payAccountID, billDate)
	if err != nil {
		return Result{}, err
	}
	var batch *models.ReconcileBatch
	switch {
	case existing != nil && existing.Status == models.BatchDone:
		return Result{BatchNo: existing.BatchNo, Matched: int(existing.MatchedCount), DiffCount: int(existing.DiffCount), Skipped: true}, nil
	case existing != nil:
		batch = existing
	default:
		batch = &models.ReconcileBatch{
			BatchNo: s.d.NewBatchNo(), AppID: appID, Channel: channel, PayAccountID: payAccountID,
			BillDate: billDate, Status: models.BatchPending,
		}
		if err := s.d.Store.WithinTx(ctx, func(ctx context.Context, r store.Repos) error {
			return r.Reconcile().CreateBatch(ctx, batch)
		}); err != nil {
			if errors.Is(err, store.ErrDuplicate) { // 并发另一方已建 → 重取
				existing, gerr := s.d.Store.Reader().Reconcile().GetBatchByAccountDate(ctx, channel, payAccountID, billDate)
				if gerr != nil || existing == nil {
					return Result{}, fmt.Errorf("对账批次已存在但重取失败: %w", gerr)
				}
				if existing.Status == models.BatchDone {
					return Result{BatchNo: existing.BatchNo, Skipped: true}, nil
				}
				batch = existing
			} else {
				return Result{}, err
			}
		}
	}

	// 2. 拉账单(网络,不在事务内)。
	b, err := s.d.Bill.Fetch(ctx, channel, payAccountID, billDate)
	if err != nil {
		s.failBatch(ctx, batch, err.Error())
		return Result{}, err
	}
	// 3. 读本地支付流水(kind=支付)。
	local, err := s.d.Store.Reader().Transactions().ListByAccountDate(ctx, channel, payAccountID, billDate)
	if err != nil {
		return Result{}, err
	}

	// 4. 比对。
	diffs, matched, localAmount := diff(b, local, batch, appID, channel, payAccountID)

	// 5. 落差异 + 更新批次(同事务)。
	batch.Status = models.BatchDone
	batch.SourceCount = int32(len(b.Entries))
	batch.MatchedCount = int32(matched)
	batch.DiffCount = int32(len(diffs))
	batch.SourceAmount = b.TotalAmount
	batch.LocalAmount = localAmount
	batch.PulledAt = sqlutil.Time(s.d.Clock())
	batch.ReconciledAt = sqlutil.Time(s.d.Clock())
	if err := s.d.Store.WithinTx(ctx, func(ctx context.Context, r store.Repos) error {
		for i := range diffs {
			if derr := r.Reconcile().InsertDiff(ctx, &diffs[i]); derr != nil && !errors.Is(derr, store.ErrDuplicate) {
				return derr
			}
		}
		return r.Reconcile().UpdateBatch(ctx, batch)
	}); err != nil {
		return Result{}, err
	}

	if len(diffs) > 0 {
		s.d.Alerter.Anomaly(ctx, "reconcile_diff", map[string]any{
			"batch_no": batch.BatchNo, "channel": channel, "pay_account_id": payAccountID,
			"bill_date": billDate.Format("2006-01-02"), "diff_count": len(diffs),
		})
	}
	return Result{BatchNo: batch.BatchNo, Matched: matched, DiffCount: len(diffs)}, nil
}

// diff 计算长款/短款/金额不一致。以 transaction_id 为主键匹配。
func diff(b bill.Bill, local []*models.PaymentTransaction, batch *models.ReconcileBatch, appID, channel, payAccountID string) ([]models.ReconcileDiff, int, int64) {
	localByTxn := make(map[string]*models.PaymentTransaction, len(local))
	var localAmount int64
	for _, t := range local {
		if t.Kind != models.TxnPay {
			continue // V1 对账聚焦支付
		}
		localByTxn[t.TransactionID] = t
		localAmount += t.Amount
	}
	gwSeen := make(map[string]bool, len(b.Entries))
	var diffs []models.ReconcileDiff
	matched := 0
	mk := func(dt models.DiffType, orderNo, txnID, outTradeNo string, gwAmt, lcAmt int64, detail string) models.ReconcileDiff {
		return models.ReconcileDiff{
			BatchID: batch.ID, AppID: appID, Channel: channel, PayAccountID: payAccountID,
			DiffType: dt, OrderNo: orderNo, TransactionID: txnID, OutTradeNo: outTradeNo,
			GatewayAmount: gwAmt, LocalAmount: lcAmt, Detail: detail,
		}
	}
	for _, e := range b.Entries {
		gwSeen[e.TransactionID] = true
		lt, ok := localByTxn[e.TransactionID]
		if !ok {
			diffs = append(diffs, mk(models.DiffSurplus, e.OrderNo, e.TransactionID, e.OutTradeNo, e.Amount, 0, "渠道有本地无(长款)"))
			continue
		}
		if lt.Amount != e.Amount {
			diffs = append(diffs, mk(models.DiffAmount, lt.OrderNo, e.TransactionID, e.OutTradeNo, e.Amount, lt.Amount, "金额不一致"))
			continue
		}
		matched++
	}
	for txnID, lt := range localByTxn {
		if !gwSeen[txnID] {
			diffs = append(diffs, mk(models.DiffShort, lt.OrderNo, txnID, lt.OutTradeNo, 0, lt.Amount, "本地有渠道无(短款)"))
		}
	}
	return diffs, matched, localAmount
}

func (s *Service) failBatch(ctx context.Context, batch *models.ReconcileBatch, reason string) {
	batch.Status = models.BatchFailed
	batch.LastError = sqlutil.Str(reason)
	_ = s.d.Store.WithinTx(ctx, func(ctx context.Context, r store.Repos) error {
		return r.Reconcile().UpdateBatch(ctx, batch)
	})
}
