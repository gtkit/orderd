package worker

import (
	"context"
	"testing"
	"time"

	"github.com/gtkit/orderd/internal/models"
	orderapp "github.com/gtkit/orderd/internal/module/order/application"
	paymentapp "github.com/gtkit/orderd/internal/module/payment/application"
	"github.com/gtkit/orderd/internal/platform/policy"
	"github.com/gtkit/orderd/internal/store"
	"github.com/gtkit/orderd/internal/store/memstore"
)

func buildOrderPayment(mem *memstore.Mem) (*orderapp.Service, *paymentapp.Service) {
	var n int64
	pay := paymentapp.New(paymentapp.Deps{
		Store: mem, Router: qRouter{gw: qGateway{}}, Accounts: qAccounts{}, Alerter: nopAlerter{}, Clock: qClock,
		NewEventID: func() (string, error) { n++; return "evt-t-" + itoaW(n), nil },
		OutTradeNo: func() string { n++; return "PT-t-" + itoaW(n) },
	})
	ord := orderapp.New(orderapp.Deps{
		Store: mem, Payment: pay, Pricing: policy.TrustedCallerPricing{}, Amounts: policy.DefaultAmountPolicy(),
		Accounts: qAccounts{}, Clock: qClock, NewOrderNo: func() string { n++; return "OD-t-" + itoaW(n) },
		NewToken: func() (string, error) { n++; return "tok-t-" + itoaW(n), nil }, NewEventID: func() (string, error) { n++; return "evt-t-" + itoaW(n), nil },
	})
	return ord, pay
}

func TestTimeoutWorker(t *testing.T) {
	mem := memstore.New()
	now := qClock()
	// A:过期 PENDING 订单 + 已提交(READY)尝试 → 应 CLOSING。
	// B:未过期 PENDING 订单 + 卡死 REQUESTING 尝试(updated_at 很旧) → 应 UNKNOWN。
	err := mem.WithinTx(bg, func(ctx context.Context, r store.Repos) error {
		if e := r.Orders().Insert(ctx, &models.Order{
			AppID: "app1", OrderNo: "OD-exp", OrderToken: "tok-exp", UserID: 42, Status: models.OrderPending,
			ProductID: 1, ProductTitle: "x", Currency: "CNY", OriginalPrice: 90, PayAmount: 90,
			PayMethod: models.PayMethodWechat, PayChannel: "wechat", PayAccountID: "acc-wechat",
			ExpireAt: now.Add(-time.Hour), // 已过期
		}); e != nil {
			return e
		}
		if e := r.Attempts().Insert(ctx, &models.PaymentAttempt{
			AppID: "app1", OrderNo: "OD-exp", AttemptNo: 1, Channel: "wechat", PayAccountID: "acc-wechat",
			OutTradeNo: "PT-exp", Status: models.AttemptReady, ExpireAt: now.Add(-time.Hour),
		}); e != nil {
			return e
		}
		if e := r.Orders().Insert(ctx, &models.Order{
			AppID: "app1", OrderNo: "OD-req", OrderToken: "tok-req", UserID: 42, Status: models.OrderPending,
			ProductID: 1, ProductTitle: "x", Currency: "CNY", OriginalPrice: 90, PayAmount: 90,
			PayMethod: models.PayMethodWechat, PayChannel: "wechat", PayAccountID: "acc-wechat",
			ExpireAt: now.Add(time.Hour),
		}); e != nil {
			return e
		}
		return r.Attempts().Insert(ctx, &models.PaymentAttempt{
			AppID: "app1", OrderNo: "OD-req", AttemptNo: 1, Channel: "wechat", PayAccountID: "acc-wechat",
			OutTradeNo: "PT-req", Status: models.AttemptRequesting, ExpireAt: now.Add(time.Hour),
			UpdatedAt: now.Add(-time.Hour), // 卡死很久
		})
	})
	if err != nil {
		t.Fatalf("播种失败: %v", err)
	}

	ord, pay := buildOrderPayment(mem)
	tw := NewTimeoutWorker(mem, ord, pay, TimeoutConfig{}, qClock)
	if _, err := tw.RunOnce(bg); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	exp, _ := mem.Reader().Orders().GetByOrderNo(bg, "app1", "OD-exp", false)
	if exp.Status != models.OrderClosing || exp.CloseReason != models.CloseTimeout {
		t.Fatalf("过期订单应 CLOSING(TIMEOUT),实际 status=%v reason=%v", exp.Status, exp.CloseReason)
	}
	req := mem.Attempts("OD-req")
	if len(req) != 1 || req[0].Status != models.AttemptUnknown {
		t.Fatalf("卡死 REQUESTING 应转 UNKNOWN,实际: %+v", req)
	}
}

// TestTimeoutWorkerStalledClosingWatchdog 反证 closing-watchdog:直接播种"CLOSING 且无活跃尝试"
// 孤儿单(绕过取消/超时生产路径)→ 兜底落定 CLOSED + closed 事件 + 告警恰一次;
// 有活跃尝试不动;阈值内不动;再跑多轮不重复告警。
func TestTimeoutWorkerStalledClosingWatchdog(t *testing.T) {
	mem := memstore.New()
	old := qClock().Add(-time.Hour)
	seedClosing := func(orderNo string, updatedAt time.Time, withActiveAttempt bool) {
		err := mem.WithinTx(bg, func(ctx context.Context, r store.Repos) error {
			if e := r.Orders().Insert(ctx, &models.Order{
				AppID: "app1", OrderNo: orderNo, OrderToken: "tok-" + orderNo, UserID: 42,
				Status: models.OrderClosing, CloseReason: models.CloseTimeout,
				ProductID: 1, ProductTitle: "x", Currency: "CNY", OriginalPrice: 90, PayAmount: 90,
				PayMethod: models.PayMethodWechat, PayChannel: "wechat", PayAccountID: "acc-wechat",
				ExpireAt: qClock().Add(-time.Hour), CreatedAt: updatedAt, UpdatedAt: updatedAt,
			}); e != nil {
				return e
			}
			if withActiveAttempt {
				return r.Attempts().Insert(ctx, &models.PaymentAttempt{
					AppID: "app1", OrderNo: orderNo, AttemptNo: 1, Channel: "wechat",
					PayAccountID: "acc-wechat", OutTradeNo: "PT-" + orderNo, Status: models.AttemptClosing,
					ExpireAt: qClock().Add(time.Hour),
				})
			}
			return nil
		})
		if err != nil {
			t.Fatalf("播种失败: %v", err)
		}
	}
	seedClosing("OD-orphan", old, false)                       // 孤儿:应被兜底
	seedClosing("OD-active", old, true)                        // 有活跃尝试:不动
	seedClosing("OD-fresh", qClock().Add(-time.Minute), false) // 阈值内:不动

	var n int64
	rec := &recAlerter{}
	pay := paymentapp.New(paymentapp.Deps{
		Store: mem, Router: qRouter{gw: qGateway{}}, Accounts: qAccounts{}, Alerter: nopAlerter{}, Clock: qClock,
		NewEventID: func() (string, error) { n++; return "evt-w-" + itoaW(n), nil },
		OutTradeNo: func() string { n++; return "PT-w-" + itoaW(n) },
	})
	ord := orderapp.New(orderapp.Deps{
		Store: mem, Payment: pay, Pricing: nil, Amounts: nil,
		Accounts: qAccounts{}, Clock: qClock, Alerter: rec,
		NewOrderNo: func() string { n++; return "OD-w-" + itoaW(n) },
		NewToken:   func() (string, error) { n++; return "tok-w-" + itoaW(n), nil },
		NewEventID: func() (string, error) { n++; return "evt-w-" + itoaW(n), nil },
	})
	tw := NewTimeoutWorker(mem, ord, pay, TimeoutConfig{}, qClock)

	for range 3 { // 多轮:兜底恰一次
		if _, err := tw.RunOnce(bg); err != nil {
			t.Fatalf("RunOnce: %v", err)
		}
	}

	o1, _ := mem.Reader().Orders().GetByOrderNo(bg, "app1", "OD-orphan", false)
	if o1.Status != models.OrderClosed {
		t.Fatalf("孤儿 CLOSING 应被兜底关闭,实际: %v", o1.Status)
	}
	o2, _ := mem.Reader().Orders().GetByOrderNo(bg, "app1", "OD-active", false)
	if o2.Status != models.OrderClosing {
		t.Fatalf("有活跃尝试的 CLOSING 不得被动,实际: %v", o2.Status)
	}
	o3, _ := mem.Reader().Orders().GetByOrderNo(bg, "app1", "OD-fresh", false)
	if o3.Status != models.OrderClosing {
		t.Fatalf("阈值内的 CLOSING 不得被动,实际: %v", o3.Status)
	}
	closedEvents := 0
	for _, e := range mem.Events() {
		if e.EventType == models.EventTypeClosed && e.OrderNo == "OD-orphan" {
			closedEvents++
		}
	}
	if closedEvents != 1 {
		t.Fatalf("孤儿单应恰发 1 条 closed 事件,实际 %d", closedEvents)
	}
	alerts := 0
	rec.mu.Lock()
	for _, k := range rec.kinds {
		if k == "order_closing_stalled" {
			alerts++
		}
	}
	rec.mu.Unlock()
	if alerts != 1 {
		t.Fatalf("兜底告警应恰一次,实际 %d", alerts)
	}
}
