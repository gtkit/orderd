package alipaygw

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gtkit/go-pay/paymgr"
	"github.com/gtkit/orderd/internal/paygateway"
)

// fakeProvider 嵌入 UnimplementedProvider,只覆写被测方法(验状态映射,不触网关)。
type fakeProvider struct {
	paymgr.UnimplementedProvider
	unified   *paymgr.UnifiedOrderResponse
	query     *paymgr.QueryOrderResponse
	queryErr  error
	refund    *paymgr.RefundResponse
	rquery    *paymgr.QueryRefundResponse
	rqueryErr error
	notify    *paymgr.NotifyResult
	notifyErr error
}

func (f *fakeProvider) Channel() paymgr.Channel { return paymgr.ChannelAlipay }
func (f *fakeProvider) UnifiedOrder(context.Context, *paymgr.UnifiedOrderRequest) (*paymgr.UnifiedOrderResponse, error) {
	return f.unified, nil
}

func (f *fakeProvider) QueryOrder(context.Context, *paymgr.QueryOrderRequest) (*paymgr.QueryOrderResponse, error) {
	if f.queryErr != nil {
		return nil, f.queryErr
	}
	return f.query, nil
}

func (f *fakeProvider) Refund(context.Context, *paymgr.RefundRequest) (*paymgr.RefundResponse, error) {
	return f.refund, nil
}

func (f *fakeProvider) QueryRefund(context.Context, *paymgr.QueryRefundRequest) (*paymgr.QueryRefundResponse, error) {
	if f.rqueryErr != nil {
		return nil, f.rqueryErr
	}
	return f.rquery, nil
}

func (f *fakeProvider) ParseNotify(context.Context, *http.Request) (*paymgr.NotifyResult, error) {
	if f.notifyErr != nil {
		return nil, f.notifyErr
	}
	return f.notify, nil
}

func gwWith(p paymgr.Provider) *Gateway {
	return &Gateway{provider: p, payAccountID: "acc-alipay", tradeType: paymgr.TradeTypePage, payAppID: "app", sellerID: "seller"}
}

func notifyRequest() *http.Request {
	r := httptest.NewRequest(http.MethodPost, "/notify/alipay/r1", strings.NewReader("app_id=app&seller_id=seller&total_amount=0.90"))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	return r
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
		{paymgr.TradeStatusRefunded, paygateway.QueryPaid},
		{paymgr.TradeStatusPending, paygateway.QueryNotPay},
		{paymgr.TradeStatusClosed, paygateway.QueryClosed},
		{paymgr.TradeStatusError, paygateway.QueryError},
	}
	for _, c := range cases {
		gw := gwWith(&fakeProvider{query: &paymgr.QueryOrderResponse{OutTradeNo: "PT1", TradeStatus: c.ts, TransactionID: "t", TotalAmount: 90}})
		r, err := gw.Query(ctx, paygateway.QueryInput{OutTradeNo: "PT1"})
		if err != nil || r.State != c.want {
			t.Fatalf("TradeStatus=%s 应映射 %v,实际 %v(err=%v)", c.ts, c.want, r.State, err)
		}
	}
	// Paid 需带交易号与金额。
	gw := gwWith(&fakeProvider{query: &paymgr.QueryOrderResponse{OutTradeNo: "PT1", TradeStatus: paymgr.TradeStatusPaid, TransactionID: "txn", TotalAmount: 90}})
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

	gw2 := gwWith(&fakeProvider{rquery: &paymgr.QueryRefundResponse{RefundStatus: paymgr.RefundStatusSuccess, OutTradeNo: "PT1", OutRefundNo: "or1", RefundID: "rid", RefundAmount: 90}})
	q, err := gw2.RefundQuery(ctx, paygateway.RefundQueryInput{OutTradeNo: "PT1", OutRefundNo: "or1"})
	if err != nil || q.State != paygateway.RefundQuerySucceeded || q.Amount != 90 {
		t.Fatalf("退款查单成功应映射 Succeeded: %+v err=%v", q, err)
	}

	gw3 := gwWith(&fakeProvider{rquery: &paymgr.QueryRefundResponse{RefundStatus: paymgr.RefundStatusProcessing, OutTradeNo: "PT1", OutRefundNo: "or1"}})
	q3, err := gw3.RefundQuery(ctx, paygateway.RefundQueryInput{OutTradeNo: "PT1", OutRefundNo: "or1"})
	if err != nil || q3.State != paygateway.RefundQueryProcessing {
		t.Fatalf("处理中应映射 Processing: %+v", q3)
	}
}

// 错误形态取自 go-pay v1.6.1 支付宝 provider 的真实输出:业务响应 ACQ.TRADE_NOT_EXIST 与退款单号不存在
// 均返回哨兵 paymgr.ErrOrderNotFound(文本 "payment: order not found",不含渠道码);其它业务失败为 ChannelError。
func TestRefundQueryErrorMapping(t *testing.T) {
	// 退款单/交易不存在 → NotExist(无错误,供阈值终态判定)。
	gw := gwWith(&fakeProvider{rqueryErr: paymgr.ErrOrderNotFound})
	q, err := gw.RefundQuery(ctx, paygateway.RefundQueryInput{OutRefundNo: "or1"})
	if err != nil || q.State != paygateway.RefundQueryNotExist {
		t.Fatalf("ErrOrderNotFound 应映射 NotExist 且无错误: %+v err=%v", q, err)
	}
	// 无法归类的错误 → Error(继续重试),绝不猜测性落终态。
	gw2 := gwWith(&fakeProvider{rqueryErr: paymgr.NewChannelError(paymgr.ChannelAlipay, "ACQ.SYSTEM_ERROR", "系统错误", nil)})
	q2, err2 := gw2.RefundQuery(ctx, paygateway.RefundQueryInput{OutRefundNo: "or1"})
	if err2 == nil || q2.State != paygateway.RefundQueryError {
		t.Fatalf("未知错误应映射 Error 并带错误: %+v err=%v", q2, err2)
	}
}

func TestQueryErrorMapping(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want paygateway.QueryState
	}{
		{"交易不存在(预下单未扫码)", paymgr.ErrOrderNotFound, paygateway.QueryNotExist},
		{"包装后的哨兵", fmt.Errorf("query: %w", paymgr.ErrOrderNotFound), paygateway.QueryNotExist},
		{"其它业务失败", paymgr.NewChannelError(paymgr.ChannelAlipay, "ACQ.SYSTEM_ERROR", "系统错误", nil), paygateway.QueryError},
		{"网络错误", errors.New("dial tcp: i/o timeout"), paygateway.QueryError},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			q, err := gwWith(&fakeProvider{queryErr: tc.err}).Query(ctx, paygateway.QueryInput{OutTradeNo: "PT1"})
			if err != nil || q.State != tc.want {
				t.Fatalf("期望 %v 且无错误,实际 %+v err=%v", tc.want, q, err)
			}
		})
	}
}

func TestParseNotifyPaidMapping(t *testing.T) {
	gw := gwWith(&fakeProvider{notify: &paymgr.NotifyResult{OutTradeNo: "PT1", TransactionID: "txn", TradeStatus: paymgr.TradeStatusPaid, TotalAmount: 90}})
	n, err := gw.ParseNotify(ctx, notifyRequest())
	if err != nil || !n.Paid || n.OutTradeNo != "PT1" || n.AmountFen != 90 {
		t.Fatalf("验签后应解析出已支付通知: %+v err=%v", n, err)
	}
}

// TestParseNotifyRejectsVerifyFailure 反证:底层验签失败时 ParseNotify 必须传播错误,
// 绝不把验签失败误判为已支付通知——否则伪造回调可绕过验签直达结算。
func TestParseNotifyRejectsVerifyFailure(t *testing.T) {
	gw := gwWith(&fakeProvider{notifyErr: errors.New("verify signature failed")})
	n, err := gw.ParseNotify(ctx, notifyRequest())
	if err == nil {
		t.Fatal("验签失败必须返回错误,不得吞成成功")
	}
	if n.Paid {
		t.Fatalf("验签失败不得解析出已支付: %+v", n)
	}
}

// SDK v1.6.1 把支付宝无时区字符串当成 UTC;adapter 须还原 GMT+8 的真实时刻。
func TestChannelTimesUseGMT8(t *testing.T) {
	for _, tc := range []struct {
		name string
		wall time.Time
		want time.Time
	}{
		{"午夜跨UTC日期", time.Date(2026, 10, 12, 0, 30, 0, 0, time.UTC), time.Date(2026, 10, 11, 16, 30, 0, 0, time.UTC)},
		{"日间", time.Date(2026, 10, 12, 15, 20, 30, 0, time.UTC), time.Date(2026, 10, 12, 7, 20, 30, 0, time.UTC)},
		{"渠道时间缺失", time.Time{}, time.Time{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gw := gwWith(&fakeProvider{
				query:  &paymgr.QueryOrderResponse{OutTradeNo: "PT1", TradeStatus: paymgr.TradeStatusPaid, PaidAt: tc.wall},
				rquery: &paymgr.QueryRefundResponse{RefundStatus: paymgr.RefundStatusSuccess, OutTradeNo: "PT1", OutRefundNo: "refund-1", RefundedAt: tc.wall},
				notify: &paymgr.NotifyResult{TradeStatus: paymgr.TradeStatusPaid, PaidAt: tc.wall, TotalAmount: 90},
			})
			q, err := gw.Query(t.Context(), paygateway.QueryInput{OutTradeNo: "PT1"})
			if err != nil || !q.PaidAt.Equal(tc.want) {
				t.Fatalf("查单时间=%v, want %v, err=%v", q.PaidAt, tc.want, err)
			}
			r, err := gw.RefundQuery(t.Context(), paygateway.RefundQueryInput{OutTradeNo: "PT1", OutRefundNo: "refund-1"})
			if err != nil || !r.SucceededAt.Equal(tc.want) {
				t.Fatalf("退款时间=%v, want %v, err=%v", r.SucceededAt, tc.want, err)
			}
			n, err := gw.ParseNotify(t.Context(), notifyRequest())
			if err != nil || !n.PaidAt.Equal(tc.want) {
				t.Fatalf("回调时间=%v, want %v, err=%v", n.PaidAt, tc.want, err)
			}
		})
	}
}

func TestRefundQueryStatesAndChannelIdentity(t *testing.T) {
	for _, tc := range []struct {
		status paymgr.RefundStatus
		want   paygateway.RefundQueryState
		wantID string
	}{
		{paymgr.RefundStatusSuccess, paygateway.RefundQuerySucceeded, "original-payment-txn"},
		{paymgr.RefundStatusProcessing, paygateway.RefundQueryProcessing, "original-payment-txn"},
		{paymgr.RefundStatusAbnormal, paygateway.RefundQueryAbnormal, "original-payment-txn"},
		{paymgr.RefundStatusClosed, paygateway.RefundQueryFailed, ""},
		{paymgr.RefundStatus("unknown"), paygateway.RefundQueryError, ""},
	} {
		t.Run(string(tc.status), func(t *testing.T) {
			gw := gwWith(&fakeProvider{rquery: &paymgr.QueryRefundResponse{
				RefundStatus: tc.status, RefundID: "original-payment-txn", TransactionID: "original-payment-txn",
				OutTradeNo: "PT1", OutRefundNo: "refund-1", RefundAmount: 90,
			}})
			got, err := gw.RefundQuery(t.Context(), paygateway.RefundQueryInput{OutTradeNo: "PT1", OutRefundNo: "refund-1"})
			if err != nil || got.State != tc.want || got.GatewayRefundID != tc.wantID {
				t.Fatalf("退款状态=%v, want state=%v id=%q, err=%v", got, tc.want, tc.wantID, err)
			}
		})
	}
}

// 同金额不代表同一资金事实;微信查询仅按退款号,必须复核 SDK 返回的原支付身份。
func TestRefundQueryRequiresMatchingIdentity(t *testing.T) {
	for _, status := range []paymgr.RefundStatus{
		paymgr.RefundStatusSuccess, paymgr.RefundStatusProcessing, paymgr.RefundStatusAbnormal,
		paymgr.RefundStatusClosed, paymgr.RefundStatus("unknown"),
	} {
		for _, tc := range []struct {
			name, outTradeNo, outRefundNo string
		}{
			{"missing_trade", "", "refund-1"},
			{"wrong_trade", "other-payment", "refund-1"},
			{"missing_refund", "PT1", ""},
			{"wrong_refund", "PT1", "other-refund"},
		} {
			t.Run(string(status)+"/"+tc.name, func(t *testing.T) {
				gw := gwWith(&fakeProvider{rquery: &paymgr.QueryRefundResponse{
					OutTradeNo: tc.outTradeNo, OutRefundNo: tc.outRefundNo, RefundStatus: status,
					RefundID: "other-refund-id", TransactionID: "other-payment-id", RefundAmount: 90,
					RefundedAt: time.Date(2026, 10, 12, 8, 0, 0, 0, time.UTC),
				}})
				got, err := gw.RefundQuery(t.Context(), paygateway.RefundQueryInput{OutTradeNo: "PT1", OutRefundNo: "refund-1"})
				if err == nil || got != (paygateway.RefundQueryResult{State: paygateway.RefundQueryError}) {
					t.Fatalf("退款身份不符必须拒绝整份查询事实: %+v err=%v", got, err)
				}
			})
		}
	}
}

func TestRefundQueryRejectsNilResponse(t *testing.T) {
	got, err := gwWith(&fakeProvider{}).RefundQuery(t.Context(), paygateway.RefundQueryInput{OutTradeNo: "PT1", OutRefundNo: "refund-1"})
	if err == nil || got != (paygateway.RefundQueryResult{State: paygateway.RefundQueryError}) {
		t.Fatalf("空退款响应必须拒绝: %+v err=%v", got, err)
	}
}

func TestQueryRequiresMatchingIdentity(t *testing.T) {
	for _, status := range []paymgr.TradeStatus{
		paymgr.TradeStatusPaid, paymgr.TradeStatusRefunded, paymgr.TradeStatusPending,
		paymgr.TradeStatusClosed, paymgr.TradeStatusError, paymgr.TradeStatus("unknown"),
	} {
		for _, outTradeNo := range []string{"", "other-payment"} {
			t.Run(string(status)+"/"+outTradeNo, func(t *testing.T) {
				gw := gwWith(&fakeProvider{query: &paymgr.QueryOrderResponse{
					OutTradeNo: outTradeNo, TradeStatus: status, TransactionID: "other-payment-id", TotalAmount: 90,
					PaidAt: time.Date(2026, 10, 12, 8, 0, 0, 0, time.UTC),
				}})
				got, err := gw.Query(t.Context(), paygateway.QueryInput{OutTradeNo: "PT1"})
				if err == nil || got != (paygateway.QueryResult{State: paygateway.QueryError}) {
					t.Fatalf("支付身份不符必须拒绝整份查询事实: %+v err=%v", got, err)
				}
			})
		}
	}
}

func TestQueryRejectsNilResponse(t *testing.T) {
	got, err := gwWith(&fakeProvider{}).Query(t.Context(), paygateway.QueryInput{OutTradeNo: "PT1"})
	if err == nil || got != (paygateway.QueryResult{State: paygateway.QueryError}) {
		t.Fatalf("空支付查询响应必须拒绝: %+v err=%v", got, err)
	}
}
