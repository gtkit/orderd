package apptest

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/gtkit/orderd/internal/models"
	orderapp "github.com/gtkit/orderd/internal/module/order/application"
	paymentapp "github.com/gtkit/orderd/internal/module/payment/application"
	refundapp "github.com/gtkit/orderd/internal/module/refund/application"
	"github.com/gtkit/orderd/internal/paygateway"
	"github.com/gtkit/orderd/internal/shared/obs"
	"github.com/gtkit/orderd/internal/store/memstore"
)

// --- fakes ---

type fakePricing struct{ err error }

func (f fakePricing) Authorize(string, models.Money, string) error { return f.err }
func (fakePricing) Kind() string                                   { return "trusted-caller" }

type fakeAmounts struct {
	b   models.AmountBounds
	err error // 非 nil 时模拟策略查询失败(fail-closed 反证用)
}

func (f fakeAmounts) Bounds(context.Context, string) (models.AmountBounds, bool, error) {
	return f.b, true, f.err
}

type fakeAccounts struct{}

func (fakeAccounts) Resolve(_ context.Context, appID, channel string) (models.AppPayAccount, error) {
	id := "acc-" + channel
	return models.AppPayAccount{
		PayAccountID: id, AppID: appID, Channel: channel,
		MchID: "mch-" + channel, PayAppID: "app-" + channel, ConfigVersion: 1, Enabled: true,
	}, nil
}

func (fakeAccounts) ResolveByID(_ context.Context, payAccountID string) (models.AppPayAccount, error) {
	return models.AppPayAccount{
		PayAccountID: payAccountID, AppID: "app1", MchID: "mch-" + payAccountID, PayAppID: "app-" + payAccountID,
		ConfigVersion: 1, Enabled: true,
	}, nil
}

// refundCall 记录一次退款调用的关键入参(校验结算渠道/账号/原商户单号)。
type refundCall struct {
	Channel      string
	PayAccountID string
	OutTradeNo   string
	OutRefundNo  string
}

// fakeGateway 记录退款调用,可注入错误。
type fakeGateway struct {
	mu              sync.Mutex
	prepareErr      error
	lastPrepare     paygateway.PrepareInput
	refunds         []refundCall
	refundNotAccept bool
	refundErr       error // Refund 返回该错误(模拟超时/网络错误,结果未知)
	// refundQuerySet=true 时 RefundQuery 返回 refundQuery(测试自定义退款查单结果)。
	refundQuery    paygateway.RefundQueryResult
	refundQuerySet bool
	closeErr       error    // Close 返回该错误(模拟渠道关单失败)
	closes         []string // 关单调用的 out_trade_no
	closeHook      func()   // Close 内同步执行(模拟关单调用期间的并发事件,不持 g.mu)
}

func (g *fakeGateway) Prepare(_ context.Context, in paygateway.PrepareInput) (paygateway.PrepareResult, error) {
	g.mu.Lock()
	g.lastPrepare = in
	g.mu.Unlock()
	if g.prepareErr != nil {
		return paygateway.PrepareResult{}, g.prepareErr
	}
	return paygateway.PrepareResult{PrepayID: "pp-" + in.OutTradeNo, AppParams: "params-" + in.Channel}, nil
}

func (g *fakeGateway) Query(context.Context, paygateway.QueryInput) (paygateway.QueryResult, error) {
	return paygateway.QueryResult{State: paygateway.QueryNotPay}, nil
}
func (g *fakeGateway) Close(_ context.Context, in paygateway.CloseInput) error {
	g.mu.Lock()
	g.closes = append(g.closes, in.OutTradeNo)
	hook, err := g.closeHook, g.closeErr
	g.mu.Unlock()
	if hook != nil {
		hook()
	}
	return err
}
func (g *fakeGateway) Refund(_ context.Context, in paygateway.RefundInput) (paygateway.RefundResult, error) {
	g.mu.Lock()
	g.refunds = append(g.refunds, refundCall{Channel: in.Channel, PayAccountID: in.PayAccountID, OutTradeNo: in.OutTradeNo, OutRefundNo: in.OutRefundNo})
	g.mu.Unlock()
	if g.refundErr != nil {
		return paygateway.RefundResult{}, g.refundErr
	}
	if g.refundNotAccept {
		return paygateway.RefundResult{Accepted: false}, nil
	}
	return paygateway.RefundResult{GatewayRefundID: "gwr-" + in.OutRefundNo, Accepted: true}, nil
}

// RefundQuery 供退款查单 worker(测试默认返回成功,可用 refundQuerySet 覆盖)。
func (g *fakeGateway) RefundQuery(_ context.Context, in paygateway.RefundQueryInput) (paygateway.RefundQueryResult, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.refundQuerySet {
		return g.refundQuery, nil
	}
	return paygateway.RefundQueryResult{State: paygateway.RefundQuerySucceeded, GatewayRefundID: "gwr-" + in.OutRefundNo, TransactionID: "rtx-" + in.OutRefundNo, Amount: 90}, nil
}

type fakeRouter struct{ gw *fakeGateway }

func (r fakeRouter) Resolve(string, string) (paygateway.Gateway, error) { return r.gw, nil }

type recordingAlerter struct {
	mu    sync.Mutex
	kinds []string
}

func (a *recordingAlerter) Anomaly(_ context.Context, kind string, _ map[string]any) {
	a.mu.Lock()
	a.kinds = append(a.kinds, kind)
	a.mu.Unlock()
}

func (a *recordingAlerter) count(kind string) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	n := 0
	for _, k := range a.kinds {
		if k == kind {
			n++
		}
	}
	return n
}

// --- harness ---

type harness struct {
	mem     *memstore.Mem
	order   *orderapp.Service
	payment *paymentapp.Service
	refund  *refundapp.Service
	gateway *fakeGateway
	alerter *recordingAlerter
}

func newHarness(refundEnabled bool) *harness {
	mem := memstore.New()
	gw := &fakeGateway{}
	router := fakeRouter{gw: gw}
	alerter := &recordingAlerter{}
	clock := func() time.Time { return time.Date(2026, 7, 17, 12, 0, 0, 0, time.UTC) }

	var evtN, tokN, ordN, tradeN, refN int64
	var idMu sync.Mutex
	next := func(p *int64) int64 { idMu.Lock(); defer idMu.Unlock(); *p++; return *p }

	pay := paymentapp.New(paymentapp.Deps{
		Store: mem, Router: router, Accounts: fakeAccounts{}, Alerter: alerter, Clock: clock,
		NewEventID: func() (string, error) { return fmt.Sprintf("evt-%d", next(&evtN)), nil },
		OutTradeNo: func() string { return fmt.Sprintf("PT%d", next(&tradeN)) },
	})
	ord := orderapp.New(orderapp.Deps{
		Store: mem, Payment: pay, Pricing: fakePricing{},
		Amounts:  fakeAmounts{b: models.AmountBounds{MinPayable: 1, MaxPayable: 1_000_000, AllowedBusiness: map[string]bool{"vip": true}}},
		Accounts: fakeAccounts{}, Clock: clock,
		NewOrderNo: func() string { return fmt.Sprintf("OD%d", next(&ordN)) },
		NewToken:   func() (string, error) { return fmt.Sprintf("tok-%d", next(&tokN)), nil },
		NewEventID: func() (string, error) { return fmt.Sprintf("evt-%d", next(&evtN)), nil },
	})
	ref := refundapp.New(refundapp.Deps{
		Store: mem, Router: router, Accounts: fakeAccounts{}, Alerter: alerter, Clock: clock,
		NewRefundNo: func() string { return fmt.Sprintf("RF%d", next(&refN)) },
		NewEventID:  func() (string, error) { return fmt.Sprintf("evt-%d", next(&evtN)), nil },
		Enabled:     refundEnabled,
	})
	return &harness{mem: mem, order: ord, payment: pay, refund: ref, gateway: gw, alerter: alerter}
}

// newOrderWithCap 用指定未付单上限重建 order 服务(其余依赖沿用 harness)。
func newOrderWithCap(h *harness, maxN int) *orderapp.Service {
	var n int64
	var mu sync.Mutex
	next := func() int64 { mu.Lock(); defer mu.Unlock(); n++; return n }
	clock := func() time.Time { return time.Date(2026, 7, 17, 12, 0, 0, 0, time.UTC) }
	return orderapp.New(orderapp.Deps{
		Store: h.mem, Payment: h.payment, Pricing: fakePricing{},
		Amounts:  fakeAmounts{b: models.AmountBounds{MinPayable: 1, MaxPayable: 1_000_000, AllowedBusiness: map[string]bool{"vip": true}}},
		Accounts: fakeAccounts{}, Clock: clock,
		NewOrderNo:        func() string { return fmt.Sprintf("OD-cap-%d", next()) },
		NewToken:          func() (string, error) { return fmt.Sprintf("tok-cap-%d", next()), nil },
		NewEventID:        func() (string, error) { return fmt.Sprintf("evt-cap-%d", next()), nil },
		MaxPendingPerUser: maxN,
	})
}

// orderWithAmounts 用指定金额策略重建 order 服务(其余依赖沿用 harness)。
func orderWithAmounts(h *harness, amounts fakeAmounts) *orderapp.Service {
	var n int64
	var mu sync.Mutex
	next := func() int64 { mu.Lock(); defer mu.Unlock(); n++; return n }
	clock := func() time.Time { return time.Date(2026, 7, 17, 12, 0, 0, 0, time.UTC) }
	return orderapp.New(orderapp.Deps{
		Store: h.mem, Payment: h.payment, Pricing: fakePricing{}, Amounts: amounts,
		Accounts: fakeAccounts{}, Clock: clock,
		NewOrderNo: func() string { return fmt.Sprintf("OD-amt-%d", next()) },
		NewToken:   func() (string, error) { return fmt.Sprintf("tok-amt-%d", next()), nil },
		NewEventID: func() (string, error) { return fmt.Sprintf("evt-amt-%d", next()), nil },
	})
}

// paymentWithObs 用指定埋点端口重建 payment 服务(其余依赖沿用 harness)。
func paymentWithObs(h *harness, ob obs.FlowObserver) *paymentapp.Service {
	var n int64
	var mu sync.Mutex
	next := func() int64 { mu.Lock(); defer mu.Unlock(); n++; return n }
	clock := func() time.Time { return time.Date(2026, 7, 17, 12, 0, 0, 0, time.UTC) }
	return paymentapp.New(paymentapp.Deps{
		Store: h.mem, Router: fakeRouter{gw: h.gateway}, Accounts: fakeAccounts{}, Alerter: h.alerter, Clock: clock,
		NewEventID: func() (string, error) { return fmt.Sprintf("evt-obs-%d", next()), nil },
		OutTradeNo: func() string { return fmt.Sprintf("PT-obs-%d", next()) },
		Obs:        ob,
	})
}

// baseCreate 返回一份合法下单入参。
func baseCreate() orderapp.CreateInput {
	return orderapp.CreateInput{
		AppID: "app1", UserID: 42, ProductID: 7, ProductTitle: "会员月卡", BusinessType: "vip",
		Amount:    models.Money{Original: 100, Discount: 10, Payable: 90, Currency: "CNY"},
		PayMethod: models.PayMethodWechat, ChannelID: 3,
	}
}
