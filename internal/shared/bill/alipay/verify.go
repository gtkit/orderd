package alipay

import (
	"context"
	"time"

	"github.com/gtkit/orderd/internal/models"
	"github.com/gtkit/orderd/internal/shared/bill"
	"github.com/gtkit/orderd/internal/shared/bill/billio"
	sdk "github.com/smartwalle/alipay/v3"
)

type paymentResponse struct {
	sdk.TradeQueryRsp
	AdditionalStatus string `json:"additional_status"`
}

// Verify 只查询已经归属本账号的账单身份；任何不明确或不匹配结果均不确认资金。
// 本源仅支持国内人民币 trade 账单。国内响应以元表示本金，币种字段可省略；
// 若返回跨境币种、信用支付或未收款附加状态则拒绝，不能将它们默认为人民币成功。
func (s *Source) Verify(ctx context.Context, channel, accountID string, e bill.Entry) (bill.Success, error) {
	if channel != "alipay" || accountID != s.accountID || e.MerchantID != s.sellerID || e.OutTradeNo == "" || e.Amount <= 0 || e.Currency != "CNY" || e.ProviderAppID != "" {
		return bill.Success{}, ErrInvalidBill
	}
	original := e.TransactionID
	if e.Kind == models.TxnRefund {
		original = e.OriginalTransactionID
		if e.OutRefundNo == "" || e.TransactionID != "" {
			return bill.Success{}, ErrInvalidBill
		}
	} else if e.Kind != models.TxnPay || e.OutRefundNo != "" || e.OriginalTransactionID != "" {
		return bill.Success{}, ErrInvalidBill
	}
	if original == "" {
		return bill.Success{}, ErrInvalidBill
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	var payment *paymentResponse
	if err := s.api.Request(ctx, sdk.TradeQuery{OutTradeNo: e.OutTradeNo}, &payment); err != nil {
		return bill.Success{}, fetchError(ctx)
	}
	if payment == nil || !payment.IsSuccess() {
		return bill.Success{}, nil
	}
	if payment.OutTradeNo != e.OutTradeNo || payment.TradeNo != original {
		return bill.Success{}, ErrInvalidBill
	}
	if !domesticCurrency(payment.TransCurrency, payment.PayCurrency, payment.SettleCurrency) ||
		payment.CreditPayMode != "" || payment.AuthTradePayMode != "" || payment.AlipaySubMerchantId != "" || payment.AdditionalStatus != "" {
		return bill.Success{}, ErrInvalidBill
	}
	originalAmount, err := billio.ParseAmount(payment.TotalAmount)
	if err != nil || originalAmount <= 0 {
		return bill.Success{}, ErrInvalidBill
	}
	if e.Kind == models.TxnPay {
		if payment.TradeStatus != "TRADE_SUCCESS" && payment.TradeStatus != "TRADE_FINISHED" {
			return bill.Success{}, nil
		}
		if originalAmount != e.Amount {
			return bill.Success{}, ErrInvalidBill
		}
		at, err := time.ParseInLocation(time.DateTime, payment.SendPayDate, channelLocation)
		if err != nil {
			return bill.Success{}, ErrInvalidBill
		}
		return bill.Success{Confirmed: true, TransactionID: payment.TradeNo, OutTradeNo: payment.OutTradeNo, Amount: originalAmount, Currency: "CNY", At: at}, nil
	}
	if originalAmount < e.Amount {
		return bill.Success{}, ErrInvalidBill
	}
	// Fully refunded original trades can be TRADE_CLOSED. The refund query below is
	// the success evidence; the original query establishes trade and currency scope.
	if payment.TradeStatus != "TRADE_SUCCESS" && payment.TradeStatus != "TRADE_FINISHED" && payment.TradeStatus != "TRADE_CLOSED" {
		return bill.Success{}, nil
	}
	var refund *sdk.TradeFastPayRefundQueryRsp
	if err := s.api.Request(ctx, sdk.TradeFastPayRefundQuery{OutTradeNo: e.OutTradeNo, OutRequestNo: e.OutRefundNo, QueryOptions: []string{"gmt_refund_pay"}}, &refund); err != nil {
		return bill.Success{}, fetchError(ctx)
	}
	if refund == nil || !refund.IsSuccess() {
		return bill.Success{}, nil
	}
	if refund.TradeNo != original || refund.OutTradeNo != e.OutTradeNo || refund.OutRequestNo != e.OutRefundNo {
		return bill.Success{}, ErrInvalidBill
	}
	if refund.RefundStatus != "REFUND_SUCCESS" {
		return bill.Success{}, nil
	}
	amount, err := billio.ParseAmount(refund.RefundAmount)
	if err != nil || amount != e.Amount {
		return bill.Success{}, ErrInvalidBill
	}
	total, err := billio.ParseAmount(refund.TotalAmount)
	if err != nil || total != originalAmount {
		return bill.Success{}, ErrInvalidBill
	}
	at, err := time.ParseInLocation(time.DateTime, refund.GMTRefundPay, channelLocation)
	if err != nil {
		return bill.Success{}, ErrInvalidBill
	}
	return bill.Success{Confirmed: true, TransactionID: refund.TradeNo, OriginalTransactionID: refund.TradeNo, OutTradeNo: refund.OutTradeNo, OutRefundNo: refund.OutRequestNo, Amount: amount, Currency: "CNY", At: at}, nil
}

func domesticCurrency(values ...string) bool {
	for _, v := range values {
		if v != "" && v != "CNY" {
			return false
		}
	}
	return true
}
