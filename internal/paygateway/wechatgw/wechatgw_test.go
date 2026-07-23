package wechatgw

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gtkit/go-pay/paymgr"
	"github.com/gtkit/orderd/internal/paygateway"
)

// fakeProvider 嵌入 UnimplementedProvider,只覆写被测方法(验状态映射,不触网关);
// 额外实现 ACKNotify 以满足本包 provider 接口。
type fakeProvider struct {
	paymgr.UnimplementedProvider
	unified *paymgr.UnifiedOrderResponse
	query   *paymgr.QueryOrderResponse
	refund  *paymgr.RefundResponse
	rquery  *paymgr.QueryRefundResponse
	notify  *paymgr.NotifyResult
	acked   bool
}

func (f *fakeProvider) Channel() paymgr.Channel { return paymgr.ChannelWechat }
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

func (f *fakeProvider) ParseNotify(context.Context, *http.Request) (*paymgr.NotifyResult, error) {
	return f.notify, nil
}

func (f *fakeProvider) ACKNotify(w http.ResponseWriter) {
	f.acked = true
	w.WriteHeader(http.StatusOK)
}

func gwWith(p provider) *Gateway {
	return &Gateway{provider: p, payAccountID: "acc-wechat", tradeType: paymgr.TradeTypeNative}
}

var ctx = context.Background()

func TestPrepareAppParams(t *testing.T) {
	// AppParams 空 → 退回 CodeURL(扫码)。
	gw := gwWith(&fakeProvider{unified: &paymgr.UnifiedOrderResponse{CodeURL: "weixin://wxpay/xxx"}})
	r, err := gw.Prepare(ctx, paygateway.PrepareInput{OutTradeNo: "PT1", Amount: 90, Subject: "会员"})
	if err != nil || r.AppParams != "weixin://wxpay/xxx" {
		t.Fatalf("应取 CodeURL 作 app_params: %+v err=%v", r, err)
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

func TestParseNotifyAndAck(t *testing.T) {
	f := &fakeProvider{notify: &paymgr.NotifyResult{OutTradeNo: "PT1", TransactionID: "txn", TradeStatus: paymgr.TradeStatusPaid, TotalAmount: 90}}
	gw := gwWith(f)
	n, err := gw.ParseNotify(ctx, httptest.NewRequest(http.MethodPost, "/notify/wechat/r1", nil))
	if err != nil || !n.Paid || n.OutTradeNo != "PT1" || n.AmountFen != 90 {
		t.Fatalf("验签后应解析出已支付通知: %+v err=%v", n, err)
	}
	rec := httptest.NewRecorder()
	gw.Ack(rec)
	if !f.acked || rec.Code != http.StatusOK {
		t.Fatalf("Ack 应调 Provider.ACKNotify 并回 200: acked=%v code=%d", f.acked, rec.Code)
	}
}
