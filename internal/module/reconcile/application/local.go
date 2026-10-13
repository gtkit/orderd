package application

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/gtkit/orderd/internal/models"
	"github.com/gtkit/orderd/internal/pkg/idgen"
	"github.com/gtkit/orderd/internal/shared/bill"
	"github.com/gtkit/orderd/internal/store"
)

func (s *Service) seedLocal(ctx context.Context, a models.AppPayAccount) error {
	return s.withinTx(ctx, func(ctx context.Context, r store.Repos) error {
		current, err := r.Reconcile().GetScanAccount(ctx, a.PayAccountID, true)
		if err != nil {
			return err
		}
		txns, err := r.Reconcile().ListLocalTxnPage(ctx, a.Channel, a.PayAccountID, current.ReconcileLocalSeedCursor, pageSize)
		if err != nil {
			return err
		}
		for _, t := range txns {
			if err := r.Reconcile().EnqueueLocalTxn(ctx, t.ID, s.d.Clock()); err != nil {
				return err
			}
		}
		if len(txns) > 0 {
			return r.Reconcile().UpdateLocalSeedCursor(ctx, a.PayAccountID, txns[len(txns)-1].ID)
		}
		return nil
	})
}
func (s *Service) verifyLocal(ctx context.Context, ch, account string) error {
	for range pageSize {
		if err := ctx.Err(); err != nil {
			return err
		}
		token, err := idgen.UUIDv4()
		if err != nil {
			return err
		}
		var change *models.ReconcileLocalChange
		if err := s.withinTx(ctx, func(ctx context.Context, r store.Repos) error {
			var err error
			now := s.d.Clock()
			change, err = r.Reconcile().ClaimLocalTxn(ctx, ch, account, token, now, now.Add(leaseTTL))
			return err
		}); err != nil {
			return err
		}
		if change == nil {
			return nil
		}
		diff, matched, entry, verifyErr := s.checkLocal(ctx, change.TxnID)
		next := s.d.Clock().Add(retryDelay)
		if matched {
			next = time.Time{}
		}
		reason := ""
		if verifyErr != nil {
			reason = "local evidence verification failed"
		}
		var changed bool
		if err := s.withinTx(ctx, func(ctx context.Context, r store.Repos) error {
			applied, err := r.Reconcile().FinishLocalTxn(ctx, change.TxnID, token, s.d.Clock(), next, reason)
			if err != nil || !applied {
				return err
			}
			if verifyErr != nil {
				return nil
			}
			changed, err = r.Reconcile().SetLocalDiff(ctx, change.TxnID, diff, s.d.Clock())
			if err != nil {
				return err
			}
			if matched && entry != nil {
				// 一条唤醒只在完成时调度一次，不能让自己不断触发新一轮比较。
				return r.Reconcile().RequestRecheck(ctx, entry.BatchID, entry.ID)
			}
			return nil
		}); err != nil {
			return err
		}
		if changed && diff != nil && s.d.Alerter != nil {
			s.d.Alerter.Anomaly(ctx, "reconcile_local_diff", map[string]any{"channel": ch, "pay_account_id": account, "local_txn_id": change.TxnID, "kind": diff.Kind, "diff_type": diff.DiffType})
		}
	}
	return nil
}
func (s *Service) checkLocal(ctx context.Context, id uint64) (*models.ReconcileDiff, bool, *models.ReconcileEntry, error) {
	r := s.d.Store.Reader()
	t, err := r.Transactions().GetByID(ctx, id, false)
	if err != nil {
		return nil, false, nil, err
	}
	d := &models.ReconcileDiff{Kind: t.Kind, IdentityKey: identity(t.Kind, fmt.Sprintf("local:%d", id), "", fmt.Sprintf("local:%d", id)), LocalTxnID: id, AppID: t.AppID, Channel: t.Channel, PayAccountID: t.PayAccountID, OrderNo: t.OrderNo, TransactionID: t.TransactionID, OutTradeNo: t.OutTradeNo, RefundNo: t.RefundNo, Currency: t.Currency, LocalAmount: t.Amount, DiffType: models.DiffStatus, Detail: "本地成功流水的账单证据待核实（可能跨日或历史窗口不足）"}
	key := identity(t.Kind, t.TransactionID, t.OutTradeNo, "")
	if t.Kind == models.TxnRefund {
		rf, err := r.Refunds().GetByRefundNo(ctx, t.RefundNo, false)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return d, false, nil, nil
			}
			return nil, false, nil, err
		}
		original, outTrade, _, _, _, err := s.refundSource(ctx, rf)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) || errors.Is(err, errInvalidRefundSource) {
				return d, false, nil, nil
			}
			return nil, false, nil, err
		}
		d.OriginalTransactionID = original
		d.OutRefundNo = rf.OutRefundNo
		key = identity(t.Kind, "", outTrade, rf.OutRefundNo)
	}
	e, err := r.Reconcile().FindPublishedEntry(ctx, t.Channel, t.PayAccountID, t.Kind, key)
	if errors.Is(err, store.ErrAmbiguous) {
		d.Detail = "同一资金身份存在多个已发布账单证据，需人工核实"
		return d, false, nil, nil
	}
	if err != nil {
		return nil, false, nil, err
	}
	if e != nil {
		mismatch, _, err := s.compareEntry(ctx, &models.ReconcileBatch{ID: e.BatchID, Channel: t.Channel, PayAccountID: t.PayAccountID}, *e)
		if err != nil {
			return nil, false, nil, err
		}
		if mismatch == nil {
			return nil, true, e, nil
		}
		d.Detail = mismatch.Detail
		d.DiffType = mismatch.DiffType
		d.GatewayAmount = e.Amount
		return d, false, e, nil
	}
	if t.Kind == models.TxnPay && t.PaidAt.Valid {
		batch, err := r.Reconcile().GetBatchByAccountDate(ctx, t.Channel, t.PayAccountID, calendarDate(t.PaidAt.Time, bill.Location), algorithmVersion)
		if err != nil {
			return nil, false, nil, err
		}
		if batch != nil && batch.PublishedRunID != "" {
			d.DiffType = models.DiffShort
			d.Detail = "完整支付账单中缺少本地成功流水（短款）"
		}
	}
	return d, false, nil, nil
}
func (s *Service) repair(ctx context.Context, ch, account string) error {
	if !s.d.AutoConfirm || s.d.Repair == nil {
		return nil
	}
	for range executionBatchSize {
		if err := ctx.Err(); err != nil {
			return err
		}
		token, err := idgen.UUIDv4()
		if err != nil {
			return err
		}
		var d *models.ReconcileDiff
		if err := s.withinTx(ctx, func(ctx context.Context, r store.Repos) error {
			var err error
			now := s.d.Clock()
			d, err = r.Reconcile().ClaimRepair(ctx, ch, account, token, now, now.Add(leaseTTL))
			return err
		}); err != nil {
			return err
		}
		if d == nil {
			return nil
		}
		e, err := s.d.Store.Reader().Reconcile().GetEntry(ctx, d.EntryID)
		var success bool
		if err == nil {
			repairCtx, cancel := context.WithTimeout(ctx, fetchBudget)
			success, err = s.d.Repair.VerifyAndConfirmFromBill(repairCtx, bill.RepairRequest{AppID: d.AppID, Channel: d.Channel, PayAccountID: d.PayAccountID, Entry: toEntry(*e)})
			cancel()
		}
		reason := "positive confirmation not available"
		if success {
			reason = ""
		} else if err != nil {
			reason = "positive confirmation failed"
		}
		// 退避不依赖批次是否仍会运行；一次失败至多一天，永久异常不占据下一个认领槽。
		delay := retryDelay
		for range min(d.VerifyAttempts, 8) {
			delay *= 2
		}
		delay = min(delay, 24*time.Hour)
		if err := s.withinTx(ctx, func(ctx context.Context, r store.Repos) error {
			_, err := r.Reconcile().FinishRepair(ctx, d.ID, token, s.d.Clock(), s.d.Clock().Add(delay), success, reason)
			return err
		}); err != nil {
			return err
		}
	}
	return nil
}
