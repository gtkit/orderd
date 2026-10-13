package application

import (
	"context"
	"fmt"

	"github.com/gtkit/orderd/internal/models"
	"github.com/gtkit/orderd/internal/shared/bill"
)

// VerifyAndConfirmFromBill 对既有尝试进行独立成功查单；不处理负面查询状态。
func (s *Service) VerifyAndConfirmFromBill(ctx context.Context, in bill.RepairRequest) (bool, error) {
	e := in.Entry
	if s.d.BillVerifier == nil {
		return false, fmt.Errorf("payment: bill verifier unavailable")
	}
	if e.Kind != models.TxnPay || e.Status != "SUCCESS" || e.TransactionID == "" || e.OutTradeNo == "" || e.Amount <= 0 || e.Currency != "CNY" || e.MerchantID == "" {
		return false, nil
	}
	a, err := s.d.Store.Reader().Attempts().GetByOutTradeNo(ctx, e.OutTradeNo, false)
	if err != nil {
		return false, err
	}
	if a == nil || a.AppID != in.AppID || a.Channel != in.Channel || a.PayAccountID != in.PayAccountID {
		return false, nil
	}
	o, err := s.d.Store.Reader().Orders().GetByOrderNo(ctx, in.AppID, a.OrderNo, false)
	if err != nil {
		return false, err
	}
	if o.PayAmount != e.Amount || o.Currency != e.Currency {
		return false, nil
	}
	acct, err := s.d.Accounts.ResolveByID(ctx, in.PayAccountID)
	if err != nil {
		return false, err
	}
	if acct.AppID != in.AppID || acct.Channel != in.Channel || acct.MchID != e.MerchantID || (e.ProviderAppID != "" && acct.PayAppID != e.ProviderAppID) {
		return false, nil
	}

	// 同一账号ID的凭据/商户变更必须递增ConfigVersion；缺失或过期快照留给人工。
	if a.ConfigVersion <= 0 || a.ConfigVersion != acct.ConfigVersion {
		return false, nil
	}
	if o.PayAccountID == a.PayAccountID && (o.PayMchID == "" || o.PayAppID == "" || o.PayMchID != acct.MchID || o.PayAppID != acct.PayAppID || o.ConfigVersion != a.ConfigVersion) {
		return false, nil
	}
	if o.TradeNo != "" && (o.SettledMchID != acct.MchID || o.SettledPayAppID != acct.PayAppID || o.SettledConfigVersion != acct.ConfigVersion) {
		return false, nil
	}
	// 已有别笔付款不能通过补偿偷偷成为异常入账路径。
	if o.TradeNo != "" && (o.TradeNo != e.TransactionID || o.SettledPayAccountID != in.PayAccountID || o.SettledOutTradeNo != e.OutTradeNo) {
		return false, nil
	}
	qctx, cancel := context.WithTimeout(ctx, s.gwTimeout())
	defer cancel()
	q, err := s.d.BillVerifier.Verify(qctx, in.Channel, in.PayAccountID, e)
	if err != nil {
		return false, err
	}
	if !q.Confirmed || q.TransactionID != e.TransactionID || q.OutTradeNo != e.OutTradeNo || q.Amount != e.Amount || q.Currency != e.Currency || q.At.IsZero() {
		return false, nil
	}
	result, err := s.ConfirmPaySuccess(ctx, ConfirmInput{reconcileAccount: &acct, AppID: in.AppID, OrderNo: a.OrderNo, Channel: in.Channel, PayAccountID: in.PayAccountID, OutTradeNo: q.OutTradeNo, TransactionID: q.TransactionID, Amount: q.Amount, Currency: q.Currency, PaidAt: q.At})
	if err != nil {
		return false, err
	}
	if result.Anomaly {
		return false, nil
	}
	t, err := s.d.Store.Reader().Transactions().GetByTxnID(ctx, in.Channel, in.PayAccountID, e.TransactionID, false)
	if err != nil {
		return false, err
	}
	return t != nil && !t.Anomaly && t.Kind == models.TxnPay && t.AppID == in.AppID && t.OrderNo == a.OrderNo &&
		t.Channel == in.Channel && t.PayAccountID == in.PayAccountID && t.OutTradeNo == e.OutTradeNo && t.Amount == e.Amount && t.Currency == e.Currency, nil
}

// safeReconcileConfirmation 在既有订单→尝试锁内复核，关闭查单等待期间另一笔付款胜出的TOCTOU窗口。
// 普通支付回调仍保留异常资金入账语义，只有本包的账单补偿使用该限制。
func safeReconcileConfirmation(o *models.Order, a *models.PaymentAttempt, current models.AppPayAccount, in ConfirmInput) bool {
	expected := in.reconcileAccount
	if expected == nil {
		return false
	}
	if current.PayAccountID != expected.PayAccountID || current.AppID != expected.AppID || current.Channel != expected.Channel ||
		current.MchID != expected.MchID || current.PayAppID != expected.PayAppID || current.ConfigVersion != expected.ConfigVersion ||
		a.AppID != in.AppID || a.OrderNo != in.OrderNo || a.Channel != in.Channel || a.PayAccountID != in.PayAccountID || a.ConfigVersion <= 0 || a.ConfigVersion != current.ConfigVersion ||
		o.AppID != in.AppID || o.PayAmount != in.Amount || o.Currency != in.Currency {
		return false
	}
	if o.PayAccountID == a.PayAccountID && (o.PayMchID != current.MchID || o.PayAppID != current.PayAppID || o.ConfigVersion != a.ConfigVersion) {
		return false
	}
	if o.TradeNo != "" {
		return o.TradeNo == in.TransactionID && o.SettledOutTradeNo == in.OutTradeNo && o.SettledPayAccountID == in.PayAccountID &&
			o.SettledChannel == in.Channel && o.SettledMchID == current.MchID && o.SettledPayAppID == current.PayAppID && o.SettledConfigVersion == current.ConfigVersion
	}
	return models.CanSettlePayment(o.Status)
}
