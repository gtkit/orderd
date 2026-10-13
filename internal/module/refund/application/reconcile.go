package application

import (
	"context"
	"fmt"
	"strings"

	"github.com/gtkit/orderd/internal/models"
	"github.com/gtkit/orderd/internal/shared/bill"
)

// VerifyAndConfirmFromBill 只确认现有退款的成功事实；账单受理状态与负面查单结果不改变业务状态。
func (s *Service) VerifyAndConfirmFromBill(ctx context.Context, in bill.RepairRequest) (bool, error) {
	e := in.Entry
	if s.d.BillVerifier == nil {
		return false, fmt.Errorf("refund: bill verifier unavailable")
	}
	if e.Kind != models.TxnRefund || (e.Status != "SUCCESS" && !strings.HasPrefix(e.Status, "REFUND:")) ||
		e.OutTradeNo == "" || e.OutRefundNo == "" || e.OriginalTransactionID == "" || e.Amount <= 0 || e.Currency != "CNY" || e.MerchantID == "" {
		return false, nil
	}
	rf, err := s.d.Store.Reader().Refunds().FindByOutRefundNo(ctx, in.AppID, e.OutRefundNo)
	if err != nil {
		return false, err
	}
	if rf == nil || rf.AppID != in.AppID || rf.PayAccountID != in.PayAccountID || rf.RefundAmount != e.Amount ||
		(rf.Status != models.RefundCreated && rf.Status != models.RefundProcessing && rf.Status != models.RefundSucceeded) {
		return false, nil
	}
	tg, err := resolveTarget(ctx, s.d.Store.Reader(), rf)
	if err != nil {
		return false, err
	}
	if tg.channel != in.Channel || tg.payAccountID != in.PayAccountID || tg.outTradeNo != e.OutTradeNo || tg.currency != e.Currency {
		return false, nil
	}
	acct, err := s.d.Accounts.ResolveByID(ctx, in.PayAccountID)
	if err != nil {
		return false, err
	}
	if acct.AppID != in.AppID || acct.Channel != in.Channel || acct.MchID != e.MerchantID || (e.ProviderAppID != "" && acct.PayAppID != e.ProviderAppID) {
		return false, nil
	}
	original, err := s.d.Store.Reader().Transactions().GetByTxnID(ctx, in.Channel, in.PayAccountID, e.OriginalTransactionID, false)
	if err != nil {
		return false, err
	}
	if original == nil || original.Kind != models.TxnPay || original.AppID != in.AppID || original.OrderNo != rf.OrderNo || original.OutTradeNo != e.OutTradeNo || original.Amount != tg.total || original.Currency != e.Currency {
		return false, nil
	}
	if rf.SourceTxnID.Valid && (rf.SourceTxnID.Int64 <= 0 || uint64(rf.SourceTxnID.Int64) != original.ID) {
		return false, nil
	}

	// 退款补偿还必须证明原支付的冻结商户/应用；没有该快照的异常来源保留人工处理。
	o, err := s.d.Store.Reader().Orders().GetByOrderNo(ctx, rf.AppID, rf.OrderNo, false)
	if err != nil {
		return false, err
	}
	if original.Anomaly || o.TradeNo != original.TransactionID || o.SettledOutTradeNo != e.OutTradeNo ||
		o.SettledPayAccountID != in.PayAccountID || o.SettledChannel != in.Channel || o.SettledMchID == "" || o.SettledPayAppID == "" ||
		o.SettledMchID != acct.MchID || o.SettledPayAppID != acct.PayAppID || o.SettledConfigVersion <= 0 || o.SettledConfigVersion != acct.ConfigVersion {
		return false, nil
	}
	qctx, cancel := context.WithTimeout(ctx, s.d.GatewayTimeout)
	defer cancel()
	q, err := s.d.BillVerifier.Verify(qctx, in.Channel, in.PayAccountID, e)
	if err != nil {
		return false, err
	}
	if !q.Confirmed || q.TransactionID == "" || q.OriginalTransactionID != e.OriginalTransactionID || q.OutTradeNo != e.OutTradeNo ||
		q.OutRefundNo != e.OutRefundNo || q.Amount != e.Amount || q.Currency != e.Currency || (e.TransactionID != "" && e.TransactionID != q.TransactionID) {
		return false, nil
	}
	if rf.GatewayRefundID != "" && rf.GatewayRefundID != q.TransactionID {
		return false, nil
	}
	if err := s.ConfirmRefundSuccess(ctx, rf.RefundNo, q.TransactionID, q.Amount, q.At); err != nil {
		return false, err
	}
	after, err := s.d.Store.Reader().Refunds().GetByRefundNo(ctx, rf.RefundNo, false)
	if err != nil {
		return false, err
	}
	if after.Status != models.RefundSucceeded {
		return false, nil
	}
	key := q.TransactionID
	if in.Channel == "alipay" {
		key = "refund:" + e.OutRefundNo
	}
	t, err := s.d.Store.Reader().Transactions().GetByTxnID(ctx, in.Channel, in.PayAccountID, key, false)
	if err != nil {
		return false, err
	}
	return t != nil && !t.Anomaly && t.Kind == models.TxnRefund && t.AppID == in.AppID && t.OrderNo == rf.OrderNo && t.RefundNo == rf.RefundNo &&
		t.Channel == in.Channel && t.PayAccountID == in.PayAccountID && t.OutTradeNo == e.OutRefundNo && t.Amount == e.Amount && t.Currency == e.Currency, nil
}
