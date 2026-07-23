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
