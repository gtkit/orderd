package apptest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gtkit/orderd/internal/models"
	paymentapp "github.com/gtkit/orderd/internal/module/payment/application"
	"github.com/gtkit/orderd/internal/store"
	orderv1 "github.com/gtkit/orderd/pb/order/v1"
	"google.golang.org/protobuf/proto"
)

// hclock 与 newHarness 注入的固定时钟一致。
var hclock = time.Date(2026, 7, 17, 12, 0, 0, 0, time.UTC)

// seedExpiringOrder 播种一个指定截止时间、无尝试的 PENDING 订单。
func seedExpiringOrder(t *testing.T, h *harness, orderNo string, expireAt time.Time) {
	t.Helper()
	err := h.mem.WithinTx(bg, func(ctx context.Context, r store.Repos) error {
		return r.Orders().Insert(ctx, &models.Order{
			AppID: "app1", OrderNo: orderNo, OrderToken: "tok-" + orderNo, UserID: 42,
			Status: models.OrderPending, ProductID: 1, ProductTitle: "x",
			Currency: "CNY", OriginalPrice: 90, PayAmount: 90, PayMethod: models.PayMethodWechat,
			PayChannel: "wechat", PayAccountID: "acc-wechat", ExpireAt: expireAt,
		})
	})
	if err != nil {
		t.Fatalf("播种订单失败: %v", err)
	}
}

// attemptByOutTradeNo 读尝试当前值。
func attemptByOutTradeNo(t *testing.T, h *harness, outTradeNo string) *models.PaymentAttempt {
	t.Helper()
	a, err := h.mem.Reader().Attempts().GetByOutTradeNo(bg, outTradeNo, false)
	if err != nil || a == nil {
		t.Fatalf("读尝试 %s: a=%v err=%v", outTradeNo, a, err)
	}
	return a
}

func orderOf(t *testing.T, h *harness, orderNo string) *models.Order {
	t.Helper()
	o, err := h.mem.Reader().Orders().GetByOrderNo(bg, "app1", orderNo, false)
	if err != nil {
		t.Fatalf("读订单 %s: %v", orderNo, err)
	}
	return o
}

// lastEnvelope 解码某类型的最后一条事件。
func lastEnvelope(t *testing.T, h *harness, eventType string) *orderv1.EventEnvelope {
	t.Helper()
	var env *orderv1.EventEnvelope
	for _, e := range h.mem.Events() {
		if e.EventType != eventType {
			continue
		}
		env = &orderv1.EventEnvelope{}
		if err := proto.Unmarshal(e.Payload, env); err != nil {
			t.Fatalf("解码事件: %v", err)
		}
	}
	if env == nil {
		t.Fatalf("无 %s 事件", eventType)
	}
	return env
}

func switchToAlipay(h *harness, orderNo string) (paymentapp.PrepareOutcome, error) {
	return h.payment.PreparePayment(bg, paymentapp.PrepareInput{
		AppID: "app1", OrderNo: orderNo, UserID: 42, PayMethod: models.PayMethodAlipay,
	})
}

// TestSwitchChannelCloseSucceeded 换渠道且渠道关单成功:旧尝试 CLOSED、新建 alipay 尝试 READY;
// 随后以新尝试结算,order.paid 携带结算渠道(alipay)而非下单渠道(wechat)。
func TestSwitchChannelCloseSucceeded(t *testing.T) {
	h := newHarness(false)
	out, err := h.order.CreateOrder(bg, baseCreate())
	if err != nil {
		t.Fatalf("下单失败: %v", err)
	}
	oldNo := h.mem.Attempts(out.OrderNo)[0].OutTradeNo

	res, err := switchToAlipay(h, out.OrderNo)
	if err != nil {
		t.Fatalf("换渠道应成功: %v", err)
	}
	if res.Reused || res.AppParams != "params-alipay" || res.AttemptNo != 2 {
		t.Fatalf("应新建第 2 个 alipay 尝试: %+v", res)
	}
	if got := attemptByOutTradeNo(t, h, oldNo).Status; got != models.AttemptClosed {
		t.Fatalf("关单成功后旧尝试应 CLOSED,实际 %v", got)
	}
	h.gateway.mu.Lock()
	closes := append([]string(nil), h.gateway.closes...)
	h.gateway.mu.Unlock()
	if len(closes) != 1 || closes[0] != oldNo {
		t.Fatalf("应恰对旧尝试关单一次: %v", closes)
	}
	var newAttempt *models.PaymentAttempt
	for _, a := range h.mem.Attempts(out.OrderNo) {
		if a.OutTradeNo != oldNo {
			newAttempt = &a
		}
	}
	if newAttempt == nil || newAttempt.Channel != "alipay" || newAttempt.Status != models.AttemptReady {
		t.Fatalf("新尝试应为 alipay READY: %+v", newAttempt)
	}

	if _, err := h.payment.ConfirmPaySuccess(bg, paymentapp.ConfirmInput{
		OutTradeNo: newAttempt.OutTradeNo, Channel: "alipay", PayAccountID: newAttempt.PayAccountID,
		TransactionID: "tx-ali-switch", Amount: 90, Currency: "CNY", PaidAt: hclock,
	}); err != nil {
		t.Fatalf("结算失败: %v", err)
	}
	paid := lastEnvelope(t, h, models.EventTypePaid).GetPaid()
	if paid.PayChannel != "alipay" || paid.PayMethod != int32(models.PayMethodAlipay) {
		t.Fatalf("order.paid 应携带结算渠道 alipay,实际 channel=%q method=%d", paid.PayChannel, paid.PayMethod)
	}
}

// TestSwitchChannelCloseFailedKeepsClosing 换渠道但渠道关单失败:不在仍可支付的旧单旁开新单——
// 旧尝试 CLOSING、不新建、返回 ErrActiveAttemptExists,订单保持 PENDING。
func TestSwitchChannelCloseFailedKeepsClosing(t *testing.T) {
	h := newHarness(false)
	out, err := h.order.CreateOrder(bg, baseCreate())
	if err != nil {
		t.Fatalf("下单失败: %v", err)
	}
	oldNo := h.mem.Attempts(out.OrderNo)[0].OutTradeNo
	h.gateway.closeErr = errors.New("ACQ.TRADE_NOT_EXIST")

	if _, err := switchToAlipay(h, out.OrderNo); !errors.Is(err, models.ErrActiveAttemptExists) {
		t.Fatalf("关单失败应返回 ErrActiveAttemptExists,实际: %v", err)
	}
	if n := len(h.mem.Attempts(out.OrderNo)); n != 1 {
		t.Fatalf("关单失败不得新建尝试,实际尝试数 %d", n)
	}
	if got := attemptByOutTradeNo(t, h, oldNo).Status; got != models.AttemptClosing {
		t.Fatalf("旧尝试应转 CLOSING 交查单收敛,实际 %v", got)
	}
	if got := orderOf(t, h, out.OrderNo).Status; got != models.OrderPending {
		t.Fatalf("订单应保持 PENDING,实际 %v", got)
	}
	// 旧尝试 CLOSING 期间再调起一律 Aborted(同渠道亦然)。
	_, err = h.payment.PreparePayment(bg, paymentapp.PrepareInput{AppID: "app1", OrderNo: out.OrderNo, UserID: 42, PayMethod: models.PayMethodWechat})
	if !errors.Is(err, models.ErrActiveAttemptExists) {
		t.Fatalf("CLOSING 期间调起应 Aborted,实际: %v", err)
	}
}

// TestSwitchChannelSettledDuringClose 关单调用期间旧尝试的回调已把订单结算:复核事务读到订单非 PENDING,
// 返回 ErrOrderNotPending,旧尝试保持 PAID、不新建尝试。
func TestSwitchChannelSettledDuringClose(t *testing.T) {
	h := newHarness(false)
	out, err := h.order.CreateOrder(bg, baseCreate())
	if err != nil {
		t.Fatalf("下单失败: %v", err)
	}
	old := h.mem.Attempts(out.OrderNo)[0]
	h.gateway.closeHook = func() {
		if _, err := h.payment.ConfirmPaySuccess(bg, confirmInput(h, out.OrderNo, old.PayAccountID, "tx-during-close", 90)); err != nil {
			t.Errorf("关单期间结算失败: %v", err)
		}
	}
	h.gateway.closeErr = errors.New("TRADE_SUCCESS 不可关")

	if _, err := switchToAlipay(h, out.OrderNo); !errors.Is(err, models.ErrOrderNotPending) {
		t.Fatalf("关单期间已结算应返回 ErrOrderNotPending,实际: %v", err)
	}
	if got := attemptByOutTradeNo(t, h, old.OutTradeNo).Status; got != models.AttemptPaid {
		t.Fatalf("旧尝试应保持 PAID(不被改写为 CLOSING/CLOSED),实际 %v", got)
	}
	if n := len(h.mem.Attempts(out.OrderNo)); n != 1 {
		t.Fatalf("不得新建尝试,实际尝试数 %d", n)
	}
	if got := orderOf(t, h, out.OrderNo).Status; got != models.OrderPaid {
		t.Fatalf("订单应 PAID,实际 %v", got)
	}
}

// TestAttemptExpireClampedToOrder 新尝试截止取 AttemptTTL 与订单截止的较早者;剩余窗口不足 1 分钟拒绝调起且不调网关。
func TestAttemptExpireClampedToOrder(t *testing.T) {
	tests := []struct {
		name       string
		orderLeft  time.Duration
		wantErr    error
		wantExpire time.Duration // 相对 hclock
	}{
		{"订单截止早于尝试TTL则夹紧", 5 * time.Minute, nil, 5 * time.Minute},
		{"订单截止晚于尝试TTL取TTL", time.Hour, nil, 15 * time.Minute},
		{"剩余窗口不足1分钟拒绝", 30 * time.Second, models.ErrOrderNotPending, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(false)
			seedExpiringOrder(t, h, "OD-clamp", hclock.Add(tc.orderLeft))
			res, err := h.payment.PreparePayment(bg, paymentapp.PrepareInput{AppID: "app1", OrderNo: "OD-clamp", UserID: 42, PayMethod: models.PayMethodWechat})
			h.gateway.mu.Lock()
			sent := h.gateway.lastPrepare
			h.gateway.mu.Unlock()
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("应拒绝(%v),实际: %v", tc.wantErr, err)
				}
				if sent.OutTradeNo != "" || len(h.mem.Attempts("OD-clamp")) != 0 {
					t.Fatalf("拒绝路径不得建尝试或调网关: sent=%+v", sent)
				}
				return
			}
			if err != nil {
				t.Fatalf("调起失败: %v", err)
			}
			want := hclock.Add(tc.wantExpire)
			if !res.ExpireAt.Equal(want) || !sent.ExpireAt.Equal(want) || !h.mem.Attempts("OD-clamp")[0].ExpireAt.Equal(want) {
				t.Fatalf("尝试截止应为 %v,实际 返回=%v 传网关=%v", want, res.ExpireAt, sent.ExpireAt)
			}
		})
	}
}

// TestLatePaymentVisibility CLOSED 订单迟到付款:结算 PAID、清 close_reason、Recovered、
// 埋点 paid_after_closed、告警恰一次;CLOSING 窗口内付款是预期路径,埋点仍为 paid、不告警。
func TestLatePaymentVisibility(t *testing.T) {
	tests := []struct {
		name        string
		order       models.OrderStatus
		attempt     models.AttemptStatus
		wantOutcome string
		wantAlert   int
	}{
		{"CLOSED迟到付款", models.OrderClosed, models.AttemptClosed, "paid_after_closed", 1},
		{"CLOSING窗口内付款", models.OrderClosing, models.AttemptClosing, "paid", 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(false)
			rec := &obsRecorder{}
			h.payment = paymentWithObs(h, rec)
			out, err := h.order.CreateOrder(bg, baseCreate())
			if err != nil {
				t.Fatalf("下单失败: %v", err)
			}
			err = h.mem.WithinTx(bg, func(ctx context.Context, r store.Repos) error {
				a, e := r.Attempts().GetActiveByOrder(ctx, out.OrderNo, true)
				if e != nil {
					return e
				}
				a.Status = tc.attempt
				if e := r.Attempts().Update(ctx, a); e != nil {
					return e
				}
				o, e := r.Orders().GetByOrderNo(ctx, "app1", out.OrderNo, true)
				if e != nil {
					return e
				}
				o.Status, o.CloseReason = tc.order, models.CloseTimeout
				return r.Orders().Update(ctx, o)
			})
			if err != nil {
				t.Fatalf("播种失败: %v", err)
			}
			res, err := h.payment.ConfirmPaySuccess(bg, confirmInput(h, out.OrderNo, "acc-wechat", "tx-late", 90))
			if err != nil || res.Outcome != "paid" || res.Recovered != (tc.order == models.OrderClosed) {
				t.Fatalf("结算结果异常: res=%+v err=%v", res, err)
			}
			o := orderOf(t, h, out.OrderNo)
			if o.Status != models.OrderPaid || o.CloseReason != models.CloseNone {
				t.Fatalf("应 PAID 且 close_reason 清零,实际 status=%v reason=%v", o.Status, o.CloseReason)
			}
			if prev := lastEnvelope(t, h, models.EventTypePaid).PreviousStatus; prev != tc.order.Proto() {
				t.Fatalf("order.paid previous_status 应为 %v,实际 %v", tc.order.Proto(), prev)
			}
			rec.mu.Lock()
			outcomes := append([]string(nil), rec.outcomes...)
			rec.mu.Unlock()
			if len(outcomes) != 1 || outcomes[0] != tc.wantOutcome {
				t.Fatalf("埋点应为 [%s],实际 %v", tc.wantOutcome, outcomes)
			}
			if got := h.alerter.count("paid_after_closed"); got != tc.wantAlert {
				t.Fatalf("paid_after_closed 告警应 %d 次,实际 %d", tc.wantAlert, got)
			}
		})
	}
}

// TestUnknownOutTradeNoRecorded 商户单号无法绑定尝试的真实付款:按路由账号归属落 anomaly 流水
// (order_no 空),告警,不结算;重复回调不新增流水。
func TestUnknownOutTradeNoRecorded(t *testing.T) {
	h := newHarness(false)
	in := paymentapp.ConfirmInput{
		OutTradeNo: "PT-nobody", Channel: "wechat", PayAccountID: "acc-wechat",
		TransactionID: "tx-unknown", Amount: 90, Currency: "CNY", PaidAt: hclock,
	}
	for i := range 2 {
		res, err := h.payment.ConfirmPaySuccess(bg, in)
		if err != nil || !res.Anomaly || res.AnomalyKind != models.AnomalyKindUnknownOutTradeNo {
			t.Fatalf("第 %d 次: 应判 unknown_out_trade_no: res=%+v err=%v", i+1, res, err)
		}
	}
	txns := h.mem.Txns()
	if len(txns) != 1 {
		t.Fatalf("重复回调不得新增流水,实际 %d 条", len(txns))
	}
	tx := txns[0]
	if !tx.Anomaly || tx.AnomalyKind != models.AnomalyKindUnknownOutTradeNo || tx.OrderNo != "" || tx.AppID != "app1" || tx.PaymentAttemptID.Valid {
		t.Fatalf("异常流水字段不符: %+v", tx)
	}
	listed, err := h.mem.Reader().Transactions().ListAnomalies(bg, store.AnomalyFilter{AppID: "app1"}, 0, 10)
	if err != nil || len(listed) != 1 {
		t.Fatalf("应可在异常清单查到: %+v err=%v", listed, err)
	}
	if h.alerter.count(models.AnomalyKindUnknownOutTradeNo) != 2 {
		t.Fatalf("每次回调都应告警")
	}
	if len(h.mem.Events()) != 0 {
		t.Fatal("不得产生任何订单事件")
	}
}

// TestAmountMismatchReleasesSlot 金额不符的真实付款:尝试收敛 PAID(释放活跃槽);订单到期时因无活跃尝试
// 直接落定 CLOSED,不进入 CLOSING 滞留。
func TestAmountMismatchReleasesSlot(t *testing.T) {
	h := newHarness(false)
	out, err := h.order.CreateOrder(bg, baseCreate())
	if err != nil {
		t.Fatalf("下单失败: %v", err)
	}
	outTradeNo := h.mem.Attempts(out.OrderNo)[0].OutTradeNo
	res, err := h.payment.ConfirmPaySuccess(bg, confirmInput(h, out.OrderNo, "acc-wechat", "tx-short", 80))
	if err != nil || res.AnomalyKind != models.AnomalyKindAmountMismatch {
		t.Fatalf("应判 amount_mismatch: res=%+v err=%v", res, err)
	}
	if got := attemptByOutTradeNo(t, h, outTradeNo).Status; got != models.AttemptPaid {
		t.Fatalf("金额不符后尝试应 PAID 释放活跃槽,实际 %v", got)
	}
	err = h.mem.WithinTx(bg, func(ctx context.Context, r store.Repos) error {
		o, e := r.Orders().GetByOrderNo(ctx, "app1", out.OrderNo, true)
		if e != nil {
			return e
		}
		o.ExpireAt = hclock.Add(-time.Minute)
		return r.Orders().Update(ctx, o)
	})
	if err != nil {
		t.Fatalf("改过期失败: %v", err)
	}
	st, err := h.order.CloseIfExpired(bg, "app1", out.OrderNo)
	if err != nil || st != models.OrderClosed {
		t.Fatalf("到期应直接落定 CLOSED,实际 st=%v err=%v", st, err)
	}
	if countEvents(h, models.EventTypeClosed) != 1 {
		t.Fatal("应发 1 条 order.closed")
	}
}

// TestCancelClosesAtGateway 取消有已提交尝试的订单:事务后向渠道关单一次;关单失败不影响返回。
func TestCancelClosesAtGateway(t *testing.T) {
	for _, closeErr := range []error{nil, errors.New("网关超时")} {
		h := newHarness(false)
		h.gateway.closeErr = closeErr
		seedOrder(t, h, "OD-cancel-close", true)
		st, err := h.order.CancelOrder(bg, "app1", "OD-cancel-close", 42)
		if err != nil || st != models.OrderClosing {
			t.Fatalf("closeErr=%v: 应返回 CLOSING 且无错误,实际 st=%v err=%v", closeErr, st, err)
		}
		h.gateway.mu.Lock()
		closes := append([]string(nil), h.gateway.closes...)
		h.gateway.mu.Unlock()
		if len(closes) != 1 || closes[0] != "PT-OD-cancel-close" {
			t.Fatalf("closeErr=%v: 应对活跃尝试关单一次,实际 %v", closeErr, closes)
		}
		if got := attemptByOutTradeNo(t, h, "PT-OD-cancel-close").Status; got != models.AttemptClosing {
			t.Fatalf("closeErr=%v: 尝试应保持 CLOSING 待查单确认,实际 %v", closeErr, got)
		}
	}
	// 从未提交网关的取消不调渠道。
	h := newHarness(false)
	seedOrder(t, h, "OD-cancel-nosubmit", false)
	if _, err := h.order.CancelOrder(bg, "app1", "OD-cancel-nosubmit", 42); err != nil {
		t.Fatalf("取消失败: %v", err)
	}
	if len(h.gateway.closes) != 0 {
		t.Fatalf("未提交网关不应关单: %v", h.gateway.closes)
	}
}
