package grpc

import (
	"context"
	"testing"

	"github.com/gtkit/orderd/internal/models"
	orderapp "github.com/gtkit/orderd/internal/module/order/application"
	paymentapp "github.com/gtkit/orderd/internal/module/payment/application"
	"github.com/gtkit/orderd/internal/paygateway/stub"
	"github.com/gtkit/orderd/internal/pkg/auth"
	"github.com/gtkit/orderd/internal/pkg/idgen"
	"github.com/gtkit/orderd/internal/platform/policy"
	"github.com/gtkit/orderd/internal/shared/alert"
	"github.com/gtkit/orderd/internal/store/memstore"
	orderv1 "github.com/gtkit/orderd/pb/order/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fakeAccounts struct{}

func (fakeAccounts) Resolve(_ context.Context, appID, channel string) (models.AppPayAccount, error) {
	return models.AppPayAccount{PayAccountID: "acc-" + channel, AppID: appID, Channel: channel, Enabled: true}, nil
}

func (fakeAccounts) ResolveByID(_ context.Context, id string) (models.AppPayAccount, error) {
	return models.AppPayAccount{PayAccountID: id, Enabled: true}, nil
}

func newServer(t *testing.T) *Server {
	t.Helper()
	mem := memstore.New()
	var n int64
	seq := func() int64 { n++; return n }
	pay := paymentapp.New(paymentapp.Deps{
		Store: mem, Router: stub.Router{}, Accounts: fakeAccounts{}, Alerter: alert.Nop{},
		NewEventID: idgen.UUIDv4, OutTradeNo: func() string { return "PT" + itoa(seq()) },
	})
	ord := orderapp.New(orderapp.Deps{
		Store: mem, Payment: pay, Pricing: policy.TrustedCallerPricing{}, Amounts: policy.DefaultAmountPolicy(),
		Accounts: fakeAccounts{}, NewOrderNo: func() string { return "OD" + itoa(seq()) },
		NewToken: func() (string, error) { return "tok" + itoa(seq()), nil }, NewEventID: idgen.UUIDv4,
	})
	srv, err := New(Deps{Order: ord, Store: mem})
	if err != nil {
		t.Fatalf("构造 server 失败: %v", err)
	}
	return srv
}

// authCtx 模拟 AuthInterceptor 已注入的身份(直连 handler 测试不经拦截器)。
func authCtx() context.Context {
	return ctxWithClaims(context.Background(), auth.NewClaims("app1", 42, nil))
}

func validCreateReq() *orderv1.CreateOrderRequest {
	return &orderv1.CreateOrderRequest{
		UserId:    42,
		Product:   &orderv1.ProductSnapshot{ProductId: 7, Title: "会员", BusinessType: "vip"},
		Amount:    &orderv1.Money{Original: 100, Discount: 10, Payable: 90, Currency: orderv1.Currency_CURRENCY_CNY},
		PayMethod: orderv1.PayMethod_PAY_METHOD_WECHAT,
	}
}

func TestCreateOrderHappyPath(t *testing.T) {
	srv := newServer(t)
	resp, err := srv.CreateOrder(authCtx(), validCreateReq())
	if err != nil {
		t.Fatalf("下单失败: %v", err)
	}
	if resp.OrderNo == "" || resp.Status != orderv1.OrderStatus_ORDER_STATUS_PENDING {
		t.Fatalf("下单响应异常: %+v", resp)
	}
}

func TestCreateOrderUnauthenticated(t *testing.T) {
	srv := newServer(t)
	_, err := srv.CreateOrder(context.Background(), validCreateReq())
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("缺鉴权应 Unauthenticated,实际: %v", err)
	}
}

func TestCreateOrderProtoValidation(t *testing.T) {
	srv := newServer(t)
	bad := validCreateReq()
	bad.Amount.Payable = 0 // 违反 payable > 0 约束
	_, err := srv.CreateOrder(authCtx(), bad)
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("非法请求应 InvalidArgument,实际: %v", err)
	}
	bad2 := validCreateReq()
	bad2.Product = nil // required
	if _, err := srv.CreateOrder(authCtx(), bad2); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("缺 product 应 InvalidArgument,实际: %v", err)
	}
}

func TestCreateOrderIdempotencyConflict(t *testing.T) {
	srv := newServer(t)
	req := validCreateReq()
	req.IdempotencyKey = "k1"
	if _, err := srv.CreateOrder(authCtx(), req); err != nil {
		t.Fatalf("首次下单失败: %v", err)
	}
	req2 := validCreateReq()
	req2.IdempotencyKey = "k1"
	req2.Amount = &orderv1.Money{Original: 200, Payable: 200, Currency: orderv1.Currency_CURRENCY_CNY}
	_, err := srv.CreateOrder(authCtx(), req2)
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("同键异请求应 AlreadyExists,实际: %v", err)
	}
}

func itoa(n int64) string {
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
