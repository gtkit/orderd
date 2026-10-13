package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gtkit/orderd/internal/models"
	paymentapp "github.com/gtkit/orderd/internal/module/payment/application"
	"github.com/gtkit/orderd/internal/paygateway"
	"github.com/gtkit/orderd/internal/shared/alert"
	"github.com/gtkit/orderd/internal/store"
	"github.com/gtkit/orderd/internal/store/memstore"
)

// --- fakes ---

type qGateway struct {
	res      paygateway.QueryResult
	resErr   error
	closeErr error
	refundQR paygateway.RefundQueryResult
}

func (g qGateway) Prepare(context.Context, paygateway.PrepareInput) (paygateway.PrepareResult, error) {
	return paygateway.PrepareResult{}, nil
}

func (g qGateway) Query(context.Context, paygateway.QueryInput) (paygateway.QueryResult, error) {
	return g.res, g.resErr
}
func (g qGateway) Close(context.Context, paygateway.CloseInput) error { return g.closeErr }
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
	return buildQueryWorkerWith(mem, qGateway{res: qr}, maxQueries, nopAlerter{})
}

// buildQueryWorkerWith 用指定网关与告警端口构造查单 worker(关单失败 / 滞留告警用例)。
func buildQueryWorkerWith(mem *memstore.Mem, gw paygateway.Gateway, maxQueries int32, alerter alert.Alerter) *QueryWorker {
	router := qRouter{gw: gw}
	var n int64
	pay := paymentapp.New(paymentapp.Deps{
		Store: mem, Router: router, Accounts: qAccounts{}, Alerter: nopAlerter{}, Clock: qClock,
		NewEventID: func() (string, error) { n++; return "evt-q-" + itoaW(n), nil },
		OutTradeNo: func() string { n++; return "PT-q-" + itoaW(n) },
	})
	return NewQueryWorker(mem, router, pay, alerter, QueryConfig{WorkerID: "q1", MaxQueries: maxQueries}, qClock)
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

// TestRescheduleDoesNotOverwriteConcurrentState 反证 queryworker-state-safety:
// worker 认领快照后,尝试被应用路径推进(取消→CLOSING、回调→PAID),重排绝不回写陈旧状态
// (旧实现整行回写含 Status,本测试失败);状态未变时正常重排。
func TestRescheduleDoesNotOverwriteConcurrentState(t *testing.T) {
	tests := []struct {
		name        string
		concurrent  models.AttemptStatus // 认领快照(UNKNOWN)之后库内被推进到的状态
		wantStatus  models.AttemptStatus
		wantCount   int32 // 收敛集合外放弃重排(count 不变);集合内正常推进调度字段
		wantNextSet bool
	}{
		// CLOSING 仍在收敛集合内:状态保持 CLOSING(不打回 UNKNOWN),调度字段照常推进。
		{"被取消推进为CLOSING不回写", models.AttemptClosing, models.AttemptClosing, 1, true},
		// PAID 在集合外:放弃重排,任何字段都不动。
		{"被回调推进为PAID不回写", models.AttemptPaid, models.AttemptPaid, 0, false},
		{"状态未变正常重排", models.AttemptUnknown, models.AttemptUnknown, 1, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mem := memstore.New()
			seed(t, mem, "OD-rs", models.OrderPending, models.CloseNone, models.AttemptUnknown, 0)
			qw := buildQueryWorker(mem, paygateway.QueryResult{State: paygateway.QueryError}, 6)

			// 认领快照(UNKNOWN,query_count=0)。
			stale, err := mem.Reader().Attempts().GetByOutTradeNo(context.Background(), "PT-OD-rs", false)
			if err != nil || stale == nil {
				t.Fatalf("取快照失败: %v", err)
			}
			// 模拟网关调用期间应用路径推进状态。
			if tc.concurrent != models.AttemptUnknown {
				err := mem.WithinTx(context.Background(), func(ctx context.Context, r store.Repos) error {
					cur, e := r.Attempts().GetByOutTradeNo(ctx, "PT-OD-rs", true)
					if e != nil {
						return e
					}
					cur.Status = tc.concurrent
					return r.Attempts().Update(ctx, cur)
				})
				if err != nil {
					t.Fatalf("推进状态失败: %v", err)
				}
			}

			qw.reschedule(context.Background(), stale)

			got, err := mem.Reader().Attempts().GetByOutTradeNo(context.Background(), "PT-OD-rs", false)
			if err != nil || got == nil {
				t.Fatalf("读尝试失败: %v", err)
			}
			if got.Status != tc.wantStatus {
				t.Fatalf("状态期望 %v,实际 %v(陈旧回写)", tc.wantStatus, got.Status)
			}
			if got.QueryCount != tc.wantCount {
				t.Fatalf("query_count 期望 %d,实际 %d", tc.wantCount, got.QueryCount)
			}
			if got.NextQueryAt.Valid != tc.wantNextSet {
				t.Fatalf("next_query_at 设置期望 %v,实际 %v", tc.wantNextSet, got.NextQueryAt.Valid)
			}
		})
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

func TestQueryClosingNotPayKeepsObserving(t *testing.T) {
	tests := []struct {
		name     string
		closeErr error
	}{
		{"关单成功", nil},
		{"关单失败", errors.New("网关关单失败")},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mem := memstore.New()
			seed(t, mem, "OD-cn", models.OrderClosing, models.CloseUserCancel, models.AttemptClosing, 0)
			qw := buildQueryWorkerWith(mem,
				qGateway{res: paygateway.QueryResult{State: paygateway.QueryNotPay}, closeErr: tc.closeErr},
				6, nopAlerter{})
			if _, err := qw.RunOnce(bg); err != nil {
				t.Fatalf("RunOnce: %v", err)
			}
			if attemptStatus(t, mem, "OD-cn") != models.AttemptClosing {
				t.Fatal("CLOSING 查单=有单未付,关单结果不改变尝试状态")
			}
			if orderStatus(t, mem, "OD-cn") != models.OrderClosing {
				t.Fatal("订单应仍 CLOSING")
			}
			a := mem.Attempts("OD-cn")[0]
			if a.QueryCount != 1 || !a.NextQueryAt.Valid || !a.NextQueryAt.Time.After(qClock()) {
				t.Fatalf("应 query_count++ 并设未来 next_query_at: %+v", a)
			}
		})
	}
}

func TestQueryClosingStallAlertEdgeTriggered(t *testing.T) {
	tests := []struct {
		name       string
		queryCount int32 // 播种时已查次数;本轮重排后为 queryCount+1
		wantAlert  bool
	}{
		{"未达阈值不告警", 4, false},
		{"跨过阈值告警一次", 5, true},
		{"已超过阈值不再告警", 6, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mem := memstore.New()
			seed(t, mem, "OD-st", models.OrderClosing, models.CloseUserCancel, models.AttemptClosing, tc.queryCount)
			rec := &recAlerter{}
			qw := buildQueryWorkerWith(mem,
				qGateway{res: paygateway.QueryResult{State: paygateway.QueryNotPay}, closeErr: errors.New("关不掉")},
				6, rec)
			if _, err := qw.RunOnce(bg); err != nil {
				t.Fatalf("RunOnce: %v", err)
			}
			if got := rec.has("attempt_close_stalled"); got != tc.wantAlert {
				t.Fatalf("attempt_close_stalled 告警期望 %v,实际 %v", tc.wantAlert, got)
			}
			if attemptStatus(t, mem, "OD-st") != models.AttemptClosing {
				t.Fatal("达阈值只告警,尝试状态不变")
			}
		})
	}
}

// TestQueryClosingNotExistWaitsForExpiry 反证 CLOSED 判据:CLOSING 尝试查得"渠道无此单"时,
// 商户单截止 + 宽限(默认 2m)之前只重排、不关单(预下单未扫码时二维码仍可付);过期后才确认关闭。
func TestQueryClosingNotExistWaitsForExpiry(t *testing.T) {
	tests := []struct {
		name       string
		expireAt   time.Time
		wantClosed bool
	}{
		{"商户单未到期保持CLOSING", qClock().Add(10 * time.Minute), false},
		{"已到期但在宽限内保持CLOSING", qClock().Add(-time.Minute), false},
		{"超过截止加宽限确认关闭", qClock().Add(-3 * time.Minute), true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mem := memstore.New()
			seed(t, mem, "OD-cne", models.OrderClosing, models.CloseUserCancel, models.AttemptClosing, 0)
			err := mem.WithinTx(context.Background(), func(ctx context.Context, r store.Repos) error {
				a, e := r.Attempts().GetByOutTradeNo(ctx, "PT-OD-cne", true)
				if e != nil {
					return e
				}
				a.ExpireAt = tc.expireAt
				return r.Attempts().Update(ctx, a)
			})
			if err != nil {
				t.Fatalf("设截止失败: %v", err)
			}
			qw := buildQueryWorker(mem, paygateway.QueryResult{State: paygateway.QueryNotExist}, 6)
			if _, err := qw.RunOnce(bg); err != nil {
				t.Fatalf("RunOnce: %v", err)
			}
			if !tc.wantClosed {
				a := mem.Attempts("OD-cne")[0]
				if a.Status != models.AttemptClosing || orderStatus(t, mem, "OD-cne") != models.OrderClosing {
					t.Fatalf("未过截止+宽限不得关闭: attempt=%v order=%v", a.Status, orderStatus(t, mem, "OD-cne"))
				}
				if a.QueryCount != 1 || !a.NextQueryAt.Valid || !a.NextQueryAt.Time.After(qClock()) {
					t.Fatalf("应重排继续观察: %+v", a)
				}
				if hasEvent(mem, models.EventTypeClosed) {
					t.Fatal("未关闭不得发 order.closed")
				}
				return
			}
			if attemptStatus(t, mem, "OD-cne") != models.AttemptClosed || orderStatus(t, mem, "OD-cne") != models.OrderClosed {
				t.Fatal("过截止+宽限后尝试与订单应 CLOSED")
			}
			if !hasEvent(mem, models.EventTypeClosed) {
				t.Fatal("应发 order.closed")
			}
		})
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

func TestQueryUnknownNotExistThresholdDoesNotReleaseValidCode(t *testing.T) {
	mem := memstore.New()
	seed(t, mem, "OD-nx", models.OrderPending, models.CloseNone, models.AttemptUnknown, 5)  // 已查 5 次
	qw := buildQueryWorker(mem, paygateway.QueryResult{State: paygateway.QueryNotExist}, 6) // 第 6 次 → 关闭
	if _, err := qw.RunOnce(bg); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if attemptStatus(t, mem, "OD-nx") != models.AttemptUnknown {
		t.Fatal("累计查询次数不能提前关闭仍有效的支付码")
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
