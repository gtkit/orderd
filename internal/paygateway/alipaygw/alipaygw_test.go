package alipaygw

import (
	"context"
	"testing"

	"github.com/gtkit/go-pay/paymgr"
	"github.com/gtkit/orderd/internal/paygateway"
)

// fakeProvider 嵌入 UnimplementedProvider,只覆写被测方法(验状态映射,不触网关)。
type fakeProvider struct {
	paymgr.UnimplementedProvider
	unified *paymgr.UnifiedOrderResponse
	query   *paymgr.QueryOrderResponse
	refund  *paymgr.RefundResponse
	rquery  *paymgr.QueryRefundResponse
}

func (f *fakeProvider) Channel() paymgr.Channel { return paymgr.ChannelAlipay }
func (f *fakeProvider) UnifiedOrder(context.Context, *paymgr.UnifiedOrderRequest) (*paymgr.UnifiedOrderResponse, error) {
	return f.unified, nil
}

func (f *fakeProvider) QueryOrder(context.Context, *paymgr.QueryOrderRequest) (*paymgr.QueryOrderResponse, error) {
	return f.query, nil
}

func (f *fakeProvider) Refund(context.Context, *paymgr.RefundRequest) (*paymgr.RefundResponse, error) {
	return f.refund, nil
}

func (f *fakeProvider) QueryRefund(context.Context, *paymgr.QueryRefundRequest) (*paymgr.QueryRefundResponse, error) {
	return f.rquery, nil
}

func gwWith(p paymgr.Provider) *Gateway {
	return &Gateway{provider: p, payAccountID: "acc-alipay", tradeType: paymgr.TradeTypePage}
}

var ctx = context.Background()

func TestPrepareAppParams(t *testing.T) {
	// AppParams 空 → 退回 PayURL。
	gw := gwWith(&fakeProvider{unified: &paymgr.UnifiedOrderResponse{PayURL: "https://pay.example"}})
	r, err := gw.Prepare(ctx, paygateway.PrepareInput{OutTradeNo: "PT1", Amount: 90, Subject: "会员"})
	if err != nil || r.AppParams != "https://pay.example" {
		t.Fatalf("应取 PayURL 作 app_params: %+v err=%v", r, err)
	}
}

func TestQueryMapping(t *testing.T) {
	cases := []struct {
		ts   paymgr.TradeStatus
		want paygateway.QueryState
	}{
		{paymgr.TradeStatusPaid, paygateway.QueryPaid},
		{paymgr.TradeStatusPending, paygateway.QueryNotPay},
		{paymgr.TradeStatusClosed, paygateway.QueryClosed},
		{paymgr.TradeStatusError, paygateway.QueryError},
	}
	for _, c := range cases {
		gw := gwWith(&fakeProvider{query: &paymgr.QueryOrderResponse{TradeStatus: c.ts, TransactionID: "t", TotalAmount: 90}})
		r, err := gw.Query(ctx, paygateway.QueryInput{OutTradeNo: "PT1"})
		if err != nil || r.State != c.want {
			t.Fatalf("TradeStatus=%s 应映射 %v,实际 %v(err=%v)", c.ts, c.want, r.State, err)
		}
	}
	// Paid 需带交易号与金额。
	gw := gwWith(&fakeProvider{query: &paymgr.QueryOrderResponse{TradeStatus: paymgr.TradeStatusPaid, TransactionID: "txn", TotalAmount: 90}})
	r, _ := gw.Query(ctx, paygateway.QueryInput{OutTradeNo: "PT1"})
	if r.TransactionID != "txn" || r.Amount != 90 {
		t.Fatalf("Paid 应带交易号/金额: %+v", r)
	}
}

func TestRefundAndQueryMapping(t *testing.T) {
	gw := gwWith(&fakeProvider{refund: &paymgr.RefundResponse{RefundID: "rid"}})
	rr, err := gw.Refund(ctx, paygateway.RefundInput{OutTradeNo: "PT1", OutRefundNo: "or1", RefundAmount: 90, TotalAmount: 90})
	if err != nil || !rr.Accepted || rr.GatewayRefundID != "rid" {
		t.Fatalf("退款应受理并带渠道退款号: %+v err=%v", rr, err)
	}

	gw2 := gwWith(&fakeProvider{rquery: &paymgr.QueryRefundResponse{RefundStatus: paymgr.RefundStatusSuccess, RefundID: "rid", RefundAmount: 90}})
	q, err := gw2.RefundQuery(ctx, paygateway.RefundQueryInput{OutTradeNo: "PT1", OutRefundNo: "or1"})
	if err != nil || q.State != paygateway.RefundQuerySucceeded || q.Amount != 90 {
		t.Fatalf("退款查单成功应映射 Succeeded: %+v err=%v", q, err)
	}

	gw3 := gwWith(&fakeProvider{rquery: &paymgr.QueryRefundResponse{RefundStatus: paymgr.RefundStatusProcessing}})
	q3, _ := gw3.RefundQuery(ctx, paygateway.RefundQueryInput{OutRefundNo: "or1"})
	if q3.State != paygateway.RefundQueryProcessing {
		t.Fatalf("处理中应映射 Processing: %+v", q3)
	}
}
