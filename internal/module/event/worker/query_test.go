package worker

import (
	"context"
	"testing"
	"time"

	"github.com/gtkit/orderd/internal/models"
	paymentapp "github.com/gtkit/orderd/internal/module/payment/application"
	"github.com/gtkit/orderd/internal/paygateway"
	"github.com/gtkit/orderd/internal/store"
	"github.com/gtkit/orderd/internal/store/memstore"
)

// --- fakes ---

type qGateway struct {
	res      paygateway.QueryResult
	resErr   error
	refundQR paygateway.RefundQueryResult
}

func (g qGateway) Prepare(context.Context, paygateway.PrepareInput) (paygateway.PrepareResult, error) {
	return paygateway.PrepareResult{}, nil
}

func (g qGateway) Query(context.Context, paygateway.QueryInput) (paygateway.QueryResult, error) {
	return g.res, g.resErr
}
func (g qGateway) Close(context.Context, paygateway.CloseInput) error { return nil }
func (g qGateway) Refund(context.Context, paygateway.RefundInput) (paygateway.RefundResult, error) {
	return paygateway.RefundResult{}, nil
}

func (g qGateway) RefundQuery(context.Context, paygateway.RefundQueryInput) (paygateway.RefundQueryResult, error) {
	return g.refundQR, nil
}

type qRouter struct{ gw paygateway.Gateway }

func (r qRouter) Resolve(string, string) (paygateway.Gateway, error) { return r.gw, nil }

type qAccounts struct{}

func (qAccounts) Resolve(_ context.Context, appID, channel string) (models.AppPayAccount, error) {
	return models.AppPayAccount{PayAccountID: "acc-" + channel, AppID: appID, Channel: channel, Enabled: true}, nil
}

func (qAccounts) ResolveByID(_ context.Context, id string) (models.AppPayAccount, error) {
	return models.AppPayAccount{PayAccountID: id, Enabled: true}, nil
}

type nopAlerter struct{}

func (nopAlerter) Anomaly(context.Context, string, map[string]any) {}

func qClock() time.Time { return time.Date(2026, 7, 17, 12, 0, 0, 0, time.UTC) }

func buildQueryWorker(mem *memstore.Mem, qr paygateway.QueryResult, maxQueries int32) *QueryWorker {
	router := qRouter{gw: qGateway{res: qr}}
	var n int64
	pay := paymentapp.New(paymentapp.Deps{
		Store: mem, Router: router, Accounts: qAccounts{}, Alerter: nopAlerter{}, Clock: qClock,
		NewEventID: func() (string, error) { n++; return "evt-q-" + itoaW(n), nil },
		OutTradeNo: func() string { n++; return "PT-q-" + itoaW(n) },
	})
	return NewQueryWorker(mem, router, pay, QueryConfig{WorkerID: "q1", MaxQueries: maxQueries}, qClock)
}

// seed 播种一个订单 + 一个尝试。
func seed(t *testing.T, mem *memstore.Mem, orderNo string, os models.OrderStatus, cr models.CloseReason, as models.AttemptStatus, queryCount int32) {
	t.Helper()
	err := mem.WithinTx(context.Background(), func(ctx context.Context, r store.Repos) error {
		if err := r.Orders().Insert(ctx, &models.Order{
			AppID: "app1", OrderNo: orderNo, OrderToken: "tok-" + orderNo, UserID: 42, Status: os, CloseReason: cr,
			ProductID: 1, ProductTitle: "x", Currency: "CNY", OriginalPrice: 90, PayAmount: 90,
			PayMethod: models.PayMethodWechat, PayChannel: "wechat", PayAccountID: "acc-wechat",
			ExpireAt: qClock().Add(time.Hour),
		}); err != nil {
			return err
		}
		return r.Attempts().Insert(ctx, &models.PaymentAttempt{
			AppID: "app1", OrderNo: orderNo, AttemptNo: 1, Channel: "wechat", PayAccountID: "acc-wechat",
			OutTradeNo: "PT-" + orderNo, Status: as, QueryCount: queryCount, ExpireAt: qClock().Add(time.Hour),
		})
	})
	if err != nil {
		t.Fatalf("播种失败: %v", err)
	}
}

func orderStatus(t *testing.T, mem *memstore.Mem, orderNo string) models.OrderStatus {
	t.Helper()
	o, err := mem.Reader().Orders().GetByOrderNo(context.Background(), "app1", orderNo, false)
	if err != nil {
		t.Fatalf("读订单失败: %v", err)
	}
	return o.Status
}

func attemptStatus(t *testing.T, mem *memstore.Mem, orderNo string) models.AttemptStatus {
	t.Helper()
	as := mem.Attempts(orderNo)
	if len(as) != 1 {
		t.Fatalf("应有 1 个尝试,实际 %d", len(as))
	}
	return as[0].Status
}

func hasEvent(mem *memstore.Mem, typ string) bool {
	for _, e := range mem.Events() {
		if e.EventType == typ {
			return true
		}
	}
	return false
}

// --- 测试 ---

func TestQueryUnknownPaid(t *testing.T) {
	mem := memstore.New()
	seed(t, mem, "OD-up", models.OrderPending, models.CloseNone, models.AttemptUnknown, 0)
	qw := buildQueryWorker(mem, paygateway.QueryResult{State: paygateway.QueryPaid, TransactionID: "txn-up", Amount: 90, PaidAt: qClock()}, 6)
	if n, err := qw.RunOnce(bg); err != nil || n != 1 {
		t.Fatalf("RunOnce: n=%d err=%v", n, err)
	}
	if orderStatus(t, mem, "OD-up") != models.OrderPaid {
		t.Fatal("UNKNOWN 查单=已付,订单应 PAID")
	}
	if attemptStatus(t, mem, "OD-up") != models.AttemptPaid {
		t.Fatal("尝试应 PAID")
	}
	if !hasEvent(mem, models.EventTypePaid) {
		t.Fatal("应发 paid 事件")
	}
}

func TestQueryClosingClosed(t *testing.T) {
	mem := memstore.New()
	seed(t, mem, "OD-cc", models.OrderClosing, models.CloseUserCancel, models.AttemptClosing, 0)
	qw := buildQueryWorker(mem, paygateway.QueryResult{State: paygateway.QueryClosed}, 6)
	if _, err := qw.RunOnce(bg); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if orderStatus(t, mem, "OD-cc") != models.OrderClosed {
		t.Fatal("CLOSING 查单=已关,订单应 CLOSED")
	}
	if attemptStatus(t, mem, "OD-cc") != models.AttemptClosed {
		t.Fatal("尝试应 CLOSED")
	}
	if !hasEvent(mem, models.EventTypeClosed) {
		t.Fatal("应发 closed 事件")
	}
}

func TestQueryClosingPaidRecovery(t *testing.T) {
	mem := memstore.New()
	seed(t, mem, "OD-cp", models.OrderClosing, models.CloseUserCancel, models.AttemptClosing, 0)
	qw := buildQueryWorker(mem, paygateway.QueryResult{State: paygateway.QueryPaid, TransactionID: "txn-cp", Amount: 90, PaidAt: qClock()}, 6)
	if _, err := qw.RunOnce(bg); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if orderStatus(t, mem, "OD-cp") != models.OrderPaid {
		t.Fatal("CLOSING 关单窗口内查单=已付,订单应恢复 PAID(§9)")
	}
}

func TestQueryUnknownNotPay(t *testing.T) {
	mem := memstore.New()
	seed(t, mem, "OD-np", models.OrderPending, models.CloseNone, models.AttemptUnknown, 0)
	qw := buildQueryWorker(mem, paygateway.QueryResult{State: paygateway.QueryNotPay}, 6)
	if _, err := qw.RunOnce(bg); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if attemptStatus(t, mem, "OD-np") != models.AttemptReady {
		t.Fatal("UNKNOWN 查单=有单未付,尝试应恢复 READY")
	}
	if orderStatus(t, mem, "OD-np") != models.OrderPending {
		t.Fatal("订单应仍 PENDING")
	}
}

func TestQueryUnknownNotExistBelowThreshold(t *testing.T) {
	mem := memstore.New()
	seed(t, mem, "OD-ne", models.OrderPending, models.CloseNone, models.AttemptUnknown, 0)
	qw := buildQueryWorker(mem, paygateway.QueryResult{State: paygateway.QueryNotExist}, 6)
	if _, err := qw.RunOnce(bg); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if attemptStatus(t, mem, "OD-ne") != models.AttemptUnknown {
		t.Fatal("未达阈值应仍 UNKNOWN(§9:查不到不能立即换号)")
	}
	as := mem.Attempts("OD-ne")[0]
	if as.QueryCount != 1 || !as.NextQueryAt.Valid || !as.NextQueryAt.Time.After(qClock()) {
		t.Fatalf("应 query_count++ 并设未来 next_query_at: %+v", as)
	}
}

func TestQueryUnknownNotExistAtThreshold(t *testing.T) {
	mem := memstore.New()
	seed(t, mem, "OD-nx", models.OrderPending, models.CloseNone, models.AttemptUnknown, 5)  // 已查 5 次
	qw := buildQueryWorker(mem, paygateway.QueryResult{State: paygateway.QueryNotExist}, 6) // 第 6 次 → 关闭
	if _, err := qw.RunOnce(bg); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if attemptStatus(t, mem, "OD-nx") != models.AttemptClosed {
		t.Fatal("达阈值多次查证不存在,尝试应 CLOSED")
	}
}

func itoaW(n int64) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
