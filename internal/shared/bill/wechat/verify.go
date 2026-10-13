package wechat

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/gtkit/orderd/internal/models"
	"github.com/gtkit/orderd/internal/shared/bill"
	"github.com/wechatpay-apiv3/wechatpay-go/services/payments/app"
	"github.com/wechatpay-apiv3/wechatpay-go/services/refunddomestic"
)

var _ bill.Verifier = (*Source)(nil)

// Verify only returns independently authenticated, completely matching success
// facts. Refund responses omit merchant/AppID; a second signed original-payment
// lookup binds the refund to that merchant/application before it can be confirmed.
// This method never submits payments, refunds, closures or negative local updates.
func (s *Source) Verify(ctx context.Context, channel, account string, e bill.Entry) (bill.Success, error) {
	if channel != "wechat" || account != s.accountID || e.MerchantID != s.mchID || e.ProviderAppID != s.appID || e.Currency != "CNY" || e.Amount <= 0 || !identity(e.OutTradeNo) || !identity(e.TransactionID) {
		return bill.Success{}, errors.New("wechat bill: invalid verification scope")
	}
	expectedPayID := e.TransactionID
	switch e.Kind {
	case models.TxnPay:
		if e.Status != "SUCCESS" || e.OutRefundNo != "" || e.OriginalTransactionID != "" {
			return bill.Success{}, errors.New("wechat bill: invalid payment evidence")
		}
	case models.TxnRefund:
		if strings.HasPrefix(e.Status, "REVOKED:") {
			return bill.Success{}, nil
		}
		switch e.Status {
		case "REFUND:SUCCESS", "REFUND:PROCESSING", "REFUND:FAIL", "REFUND:CHANGE":
		default:
			return bill.Success{}, errors.New("wechat bill: invalid refund evidence")
		}
		if !identity(e.OriginalTransactionID) || !identity(e.OutRefundNo) {
			return bill.Success{}, errors.New("wechat bill: incomplete refund identity")
		}
		expectedPayID = e.OriginalTransactionID
	default:
		return bill.Success{}, errors.New("wechat bill: unsupported verification kind")
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	paymentAPI := app.AppApiService{Client: s.client}
	payment, result, err := paymentAPI.QueryOrderByOutTradeNo(ctx, app.QueryOrderByOutTradeNoRequest{Mchid: new(s.mchID), OutTradeNo: new(e.OutTradeNo)})
	defer closeResult(result)
	if err != nil {
		return bill.Success{}, requestError(ctx, "verify payment", err)
	}
	if payment == nil || payment.TradeState == nil {
		return bill.Success{}, errors.New("wechat bill: incomplete payment response")
	}
	if *payment.TradeState != "SUCCESS" && *payment.TradeState != "REFUND" {
		return bill.Success{}, nil
	}
	if !equal(payment.Appid, s.appID) || !equal(payment.Mchid, s.mchID) || !equal(payment.OutTradeNo, e.OutTradeNo) || !equal(payment.TransactionId, expectedPayID) || payment.Amount == nil || !equal(payment.Amount.Currency, e.Currency) || payment.Amount.Total == nil || *payment.Amount.Total <= 0 {
		return bill.Success{}, errors.New("wechat bill: payment response identity or amount mismatch")
	}
	if e.Kind == models.TxnPay {
		if *payment.Amount.Total != e.Amount {
			return bill.Success{}, errors.New("wechat bill: payment principal mismatch")
		}
		var at time.Time
		if payment.SuccessTime != nil && *payment.SuccessTime != "" {
			at, err = time.Parse(time.RFC3339, *payment.SuccessTime)
			if err != nil {
				return bill.Success{}, errors.New("wechat bill: invalid payment success time")
			}
		}
		return bill.Success{Confirmed: true, TransactionID: *payment.TransactionId, OutTradeNo: *payment.OutTradeNo, Amount: *payment.Amount.Total, Currency: *payment.Amount.Currency, At: at}, nil
	}
	refundAPI := refunddomestic.RefundsApiService{Client: s.client}
	refund, result, err := refundAPI.QueryByOutRefundNo(ctx, refunddomestic.QueryByOutRefundNoRequest{OutRefundNo: new(e.OutRefundNo)})
	defer closeResult(result)
	if err != nil {
		return bill.Success{}, requestError(ctx, "verify refund", err)
	}
	if refund == nil || refund.Status == nil {
		return bill.Success{}, errors.New("wechat bill: incomplete refund response")
	}
	if *refund.Status != refunddomestic.STATUS_SUCCESS {
		return bill.Success{}, nil
	}
	if !equal(refund.RefundId, e.TransactionID) || !equal(refund.OutRefundNo, e.OutRefundNo) || !equal(refund.TransactionId, e.OriginalTransactionID) || !equal(refund.OutTradeNo, e.OutTradeNo) || refund.Amount == nil || !equal(refund.Amount.Currency, e.Currency) || refund.Amount.Refund == nil || *refund.Amount.Refund != e.Amount || refund.Amount.Total == nil || *refund.Amount.Total != *payment.Amount.Total || *refund.Amount.Refund > *refund.Amount.Total {
		return bill.Success{}, errors.New("wechat bill: refund response identity or principal mismatch")
	}
	var at time.Time
	if refund.SuccessTime != nil {
		at = *refund.SuccessTime
	}
	return bill.Success{Confirmed: true, TransactionID: *refund.RefundId, OriginalTransactionID: *refund.TransactionId, OutTradeNo: *refund.OutTradeNo, OutRefundNo: *refund.OutRefundNo, Amount: *refund.Amount.Refund, Currency: *refund.Amount.Currency, At: at}, nil
}

func equal(actual *string, expected string) bool { return actual != nil && *actual == expected }
