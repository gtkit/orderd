package orderevents

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	orderv1 "github.com/gtkit/orderd/pb/order/v1"
	"google.golang.org/protobuf/proto"
)

const (
	testKeyID  = "k1"
	testSecret = "s3cr3t"
)

// sign 按 SDK 约定生成签名头:HMAC-SHA256(timestamp + "." + body)。
func sign(secret string, ts int64, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(strconv.FormatInt(ts, 10) + "."))
	mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

func paidBody(t *testing.T) []byte {
	t.Helper()
	env := &orderv1.EventEnvelope{
		EventId:          "evt-1",
		EventType:        "order.paid",
		AppId:            "app1",
		OrderNo:          "o1",
		AggregateVersion: 1,
		Payload:          &orderv1.EventEnvelope_Paid{Paid: &orderv1.OrderPaidEvent{OrderNo: "o1", UserId: 42, Amount: 90}},
	}
	b, err := proto.Marshal(env)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

func newReq(keyID string, ts int64, sig string, body []byte) *http.Request {
	r := httptest.NewRequest(http.MethodPost, "/webhooks/order", bytes.NewReader(body))
	r.Header.Set(HeaderKeyID, keyID)
	r.Header.Set(HeaderTimestamp, strconv.FormatInt(ts, 10))
	r.Header.Set(HeaderSignature, sig)
	return r
}

func TestServeHTTP(t *testing.T) {
	body := paidBody(t)
	now := time.Now().Unix()
	goodSig := sign(testSecret, now, body)

	tests := []struct {
		name       string
		keyID      string
		ts         int64
		sig        string
		body       []byte
		onRefunded RefundedHandler
		onClosed   ClosedHandler
		wantCode   int
		wantPaid   bool
	}{
		{name: "合法支付事件→200 且回调触发", keyID: testKeyID, ts: now, sig: goodSig, body: body, wantCode: 200, wantPaid: true},
		{name: "缺 key_id→401", keyID: "", ts: now, sig: goodSig, body: body, wantCode: 401},
		{name: "未知 key_id→401", keyID: "unknown", ts: now, sig: goodSig, body: body, wantCode: 401},
		{name: "签名错误→401", keyID: testKeyID, ts: now, sig: "sha256=deadbeef", body: body, wantCode: 401},
		{name: "时间戳超窗→401", keyID: testKeyID, ts: now - 3600, sig: sign(testSecret, now-3600, body), body: body, wantCode: 401},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			paidCalled := false
			h := NewHandler(Config{
				KeyID:  testKeyID,
				Secret: []byte(testSecret),
				OnPaid: func(context.Context, EventMeta, *orderv1.OrderPaidEvent) error { paidCalled = true; return nil },
			})
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, newReq(tc.keyID, tc.ts, tc.sig, tc.body))
			if rec.Code != tc.wantCode {
				t.Fatalf("状态码=%d,期望=%d", rec.Code, tc.wantCode)
			}
			if paidCalled != tc.wantPaid {
				t.Fatalf("OnPaid 触发=%v,期望=%v", paidCalled, tc.wantPaid)
			}
		})
	}
}

// TestRefundedFailClosed 退款事件无 OnRefunded → 5xx(不静默吞资金事件)。
func TestRefundedFailClosed(t *testing.T) {
	env := &orderv1.EventEnvelope{
		EventId: "evt-r", EventType: "order.refunded", AppId: "app1", OrderNo: "o1", AggregateVersion: 2,
		Payload: &orderv1.EventEnvelope_Refunded{Refunded: &orderv1.OrderRefundedEvent{OrderNo: "o1", RefundAmount: 90}},
	}
	body, _ := proto.Marshal(env)
	now := time.Now().Unix()
	h := NewHandler(Config{
		KeyID: testKeyID, Secret: []byte(testSecret),
		OnPaid: func(context.Context, EventMeta, *orderv1.OrderPaidEvent) error { return nil },
	})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, newReq(testKeyID, now, sign(testSecret, now, body), body))
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("退款无处理器应 500,实际=%d", rec.Code)
	}
}

// TestClosedOptional 关闭事件无 OnClosed → 200(可选消费)。
func TestClosedOptional(t *testing.T) {
	env := &orderv1.EventEnvelope{
		EventId: "evt-c", EventType: "order.closed", AppId: "app1", OrderNo: "o1", AggregateVersion: 2,
		Payload: &orderv1.EventEnvelope_Closed{Closed: &orderv1.OrderClosedEvent{OrderNo: "o1"}},
	}
	body, _ := proto.Marshal(env)
	now := time.Now().Unix()
	h := NewHandler(Config{
		KeyID: testKeyID, Secret: []byte(testSecret),
		OnPaid: func(context.Context, EventMeta, *orderv1.OrderPaidEvent) error { return nil },
	})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, newReq(testKeyID, now, sign(testSecret, now, body), body))
	if rec.Code != http.StatusOK {
		t.Fatalf("关闭事件可选消费应 200,实际=%d", rec.Code)
	}
}

func TestNewHandlerPanics(t *testing.T) {
	paid := PaidHandler(func(context.Context, EventMeta, *orderv1.OrderPaidEvent) error { return nil })
	cases := []struct {
		name string
		cfg  Config
	}{
		{"Secret 空", Config{KeyID: "k1", OnPaid: paid}},
		{"KeyID 空", Config{Secret: []byte("s"), OnPaid: paid}},
		{"OnPaid 空", Config{KeyID: "k1", Secret: []byte("s")}},
		{"Prev 只配一半", Config{KeyID: "k1", Secret: []byte("s"), OnPaid: paid, PrevKeyID: "k0"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatalf("期望 panic,但未发生")
				}
			}()
			NewHandler(tc.cfg)
		})
	}
}
