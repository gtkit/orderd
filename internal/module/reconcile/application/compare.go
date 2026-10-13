package application

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/gtkit/orderd/internal/models"
	"github.com/gtkit/orderd/internal/store"
)

var errInvalidRefundSource = errors.New("reconcile: invalid refund source evidence")

// compareEntry 只读不可变流水和身份快照；账单退款的历史状态不推翻后来已成功的退款。
func (s *Service) compareEntry(ctx context.Context, b *models.ReconcileBatch, e models.ReconcileEntry) (*models.ReconcileDiff, *models.PaymentTransaction, error) {
	r := s.d.Store.Reader()
	d := &models.ReconcileDiff{BatchID: b.ID, RunID: b.RunID, EntryID: e.ID, Kind: e.Kind, IdentityKey: e.IdentityKey, Channel: b.Channel, PayAccountID: b.PayAccountID, TransactionID: e.TransactionID, OriginalTransactionID: e.OriginalTransactionID, OutTradeNo: e.OutTradeNo, OutRefundNo: e.OutRefundNo, Currency: e.Currency, GatewayAmount: e.Amount}
	// AppID 只能由已存在的本地对象证明，不能从商户级账单请求的账号 AppID 猜测。
	a, err := r.Attempts().GetByOutTradeNo(ctx, e.OutTradeNo, false)
	if err != nil {
		return nil, nil, err
	}
	if a != nil && a.Channel == b.Channel && a.PayAccountID == b.PayAccountID {
		d.AppID = a.AppID
		d.OrderNo = a.OrderNo
	}
	txnID := e.TransactionID
	if e.Kind == models.TxnRefund && b.Channel == "alipay" {
		txnID = "refund:" + e.OutRefundNo
	}
	var local *models.PaymentTransaction
	if txnID != "" {
		local, err = r.Transactions().GetByTxnID(ctx, b.Channel, b.PayAccountID, txnID, false)
		if err != nil {
			return nil, nil, err
		}
	}
	if local != nil {
		d.LocalTxnID = local.ID
		d.AppID = local.AppID
		d.OrderNo = local.OrderNo
		d.RefundNo = local.RefundNo
		d.LocalAmount = local.Amount
	}
	statusDiff := func(reason string) (*models.ReconcileDiff, *models.PaymentTransaction, error) {
		d.DiffType = models.DiffStatus
		d.Detail = reason
		return d, local, nil
	}
	acct, err := r.Reconcile().GetScanAccount(ctx, b.PayAccountID, false)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return nil, nil, err
	}
	if acct == nil {
		d.AppID = ""
		return statusDiff("支付账号身份不可读取，需人工核实")
	}
	if acct != nil && (acct.Channel != b.Channel || e.MerchantID != acct.MchID || (e.ProviderAppID != "" && e.ProviderAppID != acct.PayAppID)) {
		d.AppID = ""
		return statusDiff("账单商户或应用身份不一致，需人工核实")
	}
	if e.SourceStatus == "REVOKED" || strings.HasPrefix(e.SourceStatus, "REVOKED:") {
		return statusDiff("账单撤销状态需人工核实")
	}
	if local == nil {
		if e.Kind == models.TxnRefund && e.SourceStatus != "SUCCESS" && e.SourceStatus != "REFUND:SUCCESS" {
			return statusDiff("渠道退款受理或历史状态记录，本地无成功流水，待独立查单核实")
		}
		d.DiffType = models.DiffSurplus
		d.Detail = "渠道有记录，本地无对应成功资金流水"
		return d, nil, nil
	}
	if local.Kind != e.Kind || local.Anomaly {
		return statusDiff("资金种类或本地异常流水需人工核实")
	}
	if a != nil && (a.AppID != local.AppID || a.Channel != b.Channel || a.PayAccountID != b.PayAccountID || a.OrderNo != local.OrderNo) {
		return statusDiff("账单与支付尝试身份不一致")
	}
	if local.Currency != e.Currency {
		return statusDiff("币种不一致")
	}
	if e.Kind == models.TxnPay {
		if local.OutTradeNo != e.OutTradeNo {
			return statusDiff("商户支付单号不一致")
		}
	} else {
		if local.OutTradeNo != e.OutRefundNo || local.RefundNo == "" {
			return statusDiff("商户退款单号不一致")
		}
		rf, err := r.Refunds().GetByRefundNo(ctx, local.RefundNo, false)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return statusDiff("退款对象缺失，需人工核实")
			}
			return nil, nil, err
		}
		if rf.AppID != local.AppID || rf.PayAccountID != b.PayAccountID || rf.OutRefundNo != e.OutRefundNo || rf.OrderNo != local.OrderNo || rf.Status != models.RefundSucceeded {
			return statusDiff("退款归属或成功状态不一致")
		}
		original, outTrade, channel, account, currency, err := s.refundSource(ctx, rf)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) || errors.Is(err, errInvalidRefundSource) {
				return statusDiff("退款原支付证据缺失或身份异常，需人工核实")
			}
			return nil, nil, err
		}
		if original != e.OriginalTransactionID || outTrade != e.OutTradeNo || channel != b.Channel || account != b.PayAccountID || currency != e.Currency {
			return statusDiff("退款原支付身份不一致")
		}
	}
	if local.Amount != e.Amount {
		d.DiffType = models.DiffAmount
		d.Detail = "本金金额不一致"
		return d, local, nil
	}
	return nil, local, nil
}
func (s *Service) refundSource(ctx context.Context, rf *models.Refund) (txn, outTrade, ch, account, currency string, err error) {
	r := s.d.Store.Reader()
	if rf.SourceTxnID.Valid {
		if rf.SourceTxnID.Int64 <= 0 {
			err = fmt.Errorf("%w: nonpositive source transaction", errInvalidRefundSource)
			return
		}
		var t *models.PaymentTransaction
		t, err = r.Transactions().GetByID(ctx, uint64(rf.SourceTxnID.Int64), false)
		if err != nil {
			return
		}
		if t.Kind != models.TxnPay || t.AppID != rf.AppID || t.OrderNo != rf.OrderNo {
			err = fmt.Errorf("%w: source kind/app/order mismatch", errInvalidRefundSource)
			return
		}
		return t.TransactionID, t.OutTradeNo, t.Channel, t.PayAccountID, t.Currency, nil
	}
	o, err := r.Orders().GetByOrderNo(ctx, rf.AppID, rf.OrderNo, false)
	if err != nil {
		return
	}
	return o.TradeNo, o.SettledOutTradeNo, o.SettledChannel, o.SettledPayAccountID, o.Currency, nil
}
