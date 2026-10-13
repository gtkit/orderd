package wechatgw

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gtkit/go-pay/paymgr"
	"github.com/gtkit/orderd/internal/paygateway"
)

// fakeProvider 嵌入 UnimplementedProvider,只覆写被测方法(验状态映射,不触网关);
// 额外实现 ACKNotify 以满足本包 provider 接口。
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
	acked     bool
}

func (f *fakeProvider) Channel() paymgr.Channel { return paymgr.ChannelWechat }
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

func (f *fakeProvider) ACKNotify(w http.ResponseWriter) {
	f.acked = true
	w.WriteHeader(http.StatusOK)
}

func gwWith(p provider) *Gateway {
	return &Gateway{provider: p, payAccountID: "acc-wechat", tradeType: paymgr.TradeTypeNative,
		payAppID: "app", mchID: "mch", apiKey: string(make([]byte, 32))}
}

func notifyRequest(t *testing.T) *http.Request {
	t.Helper()
	block, err := aes.NewCipher(make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		t.Fatal(err)
	}
	nonce := "testnonce123"
	ciphertext := aead.Seal(nil, []byte(nonce), []byte(`{"appid":"app","mchid":"mch"}`), nil)
	body, err := json.Marshal(map[string]any{"resource": map[string]string{
		"nonce": nonce, "ciphertext": base64.StdEncoding.EncodeToString(ciphertext),
	}})
	if err != nil {
		t.Fatal(err)
	}
	return httptest.NewRequest(http.MethodPost, "/notify/wechat/r1", bytes.NewReader(body))
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

func TestQueryErrorMapping(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want paygateway.QueryState
	}{
		{"订单不存在", paymgr.NewChannelError(paymgr.ChannelWechat, "ORDER_NOT_EXIST", "订单不存在", nil), paygateway.QueryNotExist},
		{"其它渠道码描述含字样不误判", paymgr.NewChannelError(paymgr.ChannelWechat, "SYSTEM_ERROR", "order ORDER_NOT_EXIST maybe", nil), paygateway.QueryError},
		{"非渠道错误", errors.New("ORDER_NOT_EXIST"), paygateway.QueryError},
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

// 错误形态取自 go-pay v1.6.1 微信 provider 的真实输出:wrapWechatError 把 APIv3 错误码放进 ChannelError.Code。
func TestRefundQueryErrorMapping(t *testing.T) {
	// 退款单不存在(APIv3 RESOURCE_NOT_EXISTS)→ NotExist(无错误,供阈值终态判定)。
	gw := gwWith(&fakeProvider{rqueryErr: paymgr.NewChannelError(paymgr.ChannelWechat, "RESOURCE_NOT_EXISTS", "退款单不存在", nil)})
	q, err := gw.RefundQuery(ctx, paygateway.RefundQueryInput{OutRefundNo: "or1"})
	if err != nil || q.State != paygateway.RefundQueryNotExist {
		t.Fatalf("RESOURCE_NOT_EXISTS 应映射 NotExist 且无错误: %+v err=%v", q, err)
	}
	// 其它渠道码(即使描述里出现 not exist 字样)→ Error(继续重试),绝不猜测性落终态。
	gw2 := gwWith(&fakeProvider{rqueryErr: paymgr.NewChannelError(paymgr.ChannelWechat, "SYSTEM_ERROR", "resource NOT_EXISTS in cache", nil)})
	q2, err2 := gw2.RefundQuery(ctx, paygateway.RefundQueryInput{OutRefundNo: "or1"})
	if err2 == nil || q2.State != paygateway.RefundQueryError {
		t.Fatalf("未知错误应映射 Error 并带错误: %+v err=%v", q2, err2)
	}
}

func TestParseNotifyAndAck(t *testing.T) {
	f := &fakeProvider{notify: &paymgr.NotifyResult{OutTradeNo: "PT1", TransactionID: "txn", TradeStatus: paymgr.TradeStatusPaid, TotalAmount: 90}}
	gw := gwWith(f)
	n, err := gw.ParseNotify(ctx, notifyRequest(t))
	if err != nil || !n.Paid || n.OutTradeNo != "PT1" || n.AmountFen != 90 {
		t.Fatalf("验签后应解析出已支付通知: %+v err=%v", n, err)
	}
	rec := httptest.NewRecorder()
	gw.Ack(rec)
	if !f.acked || rec.Code != http.StatusOK {
		t.Fatalf("Ack 应调 Provider.ACKNotify 并回 200: acked=%v code=%d", f.acked, rec.Code)
	}
}

// TestParseNotifyRejectsVerifyFailure 反证:底层验签失败时 ParseNotify 必须传播错误,
// 绝不把验签失败误判为已支付通知——否则伪造回调可绕过验签直达结算。
func TestParseNotifyRejectsVerifyFailure(t *testing.T) {
	gw := gwWith(&fakeProvider{notifyErr: errors.New("verify signature failed")})
	n, err := gw.ParseNotify(ctx, notifyRequest(t))
	if err == nil {
		t.Fatal("验签失败必须返回错误,不得吞成成功")
	}
	if n.Paid {
		t.Fatalf("验签失败不得解析出已支付: %+v", n)
	}
}

func TestChannelTimesPreserveInstant(t *testing.T) {
	for _, raw := range []string{"2026-10-12T00:30:00+08:00", "2026-10-11T16:30:00Z", ""} {
		t.Run(raw, func(t *testing.T) {
			var original time.Time
			if raw != "" {
				var err error
				original, err = time.Parse(time.RFC3339, raw)
				if err != nil {
					t.Fatal(err)
				}
			}
			gw := gwWith(&fakeProvider{
				query:  &paymgr.QueryOrderResponse{OutTradeNo: "PT1", TradeStatus: paymgr.TradeStatusPaid, PaidAt: original},
				rquery: &paymgr.QueryRefundResponse{RefundStatus: paymgr.RefundStatusSuccess, OutTradeNo: "PT1", OutRefundNo: "refund-1", RefundedAt: original},
				notify: &paymgr.NotifyResult{TradeStatus: paymgr.TradeStatusPaid, PaidAt: original, TotalAmount: 90},
			})
			q, err := gw.Query(t.Context(), paygateway.QueryInput{OutTradeNo: "PT1"})
			if err != nil || !q.PaidAt.Equal(original) {
				t.Fatalf("查单时间=%v, want %v, err=%v", q.PaidAt, original, err)
			}
			r, err := gw.RefundQuery(t.Context(), paygateway.RefundQueryInput{OutTradeNo: "PT1", OutRefundNo: "refund-1"})
			if err != nil || !r.SucceededAt.Equal(original) {
				t.Fatalf("退款时间=%v, want %v, err=%v", r.SucceededAt, original, err)
			}
			n, err := gw.ParseNotify(t.Context(), notifyRequest(t))
			if err != nil || !n.PaidAt.Equal(original) {
				t.Fatalf("回调时间=%v, want %v, err=%v", n.PaidAt, original, err)
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
		{paymgr.RefundStatusSuccess, paygateway.RefundQuerySucceeded, "wechat-refund-id"},
		{paymgr.RefundStatusProcessing, paygateway.RefundQueryProcessing, "wechat-refund-id"},
		{paymgr.RefundStatusAbnormal, paygateway.RefundQueryAbnormal, "wechat-refund-id"},
		{paymgr.RefundStatusClosed, paygateway.RefundQueryFailed, ""},
		{paymgr.RefundStatus("unknown"), paygateway.RefundQueryError, ""},
	} {
		t.Run(string(tc.status), func(t *testing.T) {
			gw := gwWith(&fakeProvider{rquery: &paymgr.QueryRefundResponse{
				OutTradeNo: "PT1", OutRefundNo: "refund-1", RefundStatus: tc.status, RefundID: "wechat-refund-id", TransactionID: "original-payment-txn", RefundAmount: 90,
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
