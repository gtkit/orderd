package apptest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gtkit/orderd/internal/models"
	paymentapp "github.com/gtkit/orderd/internal/module/payment/application"
	refundapp "github.com/gtkit/orderd/internal/module/refund/application"
	"github.com/gtkit/orderd/internal/store"
)

// TestSecondPaymentDuringRefundIsAnomaly 反证 payment-settlement-guard:退款中的订单收到
// 第二笔真实付款(另一 CLOSING 尝试、新交易号、等额)必须落 anomaly,绝不结算——
// 旧实现借道 CanOrderTransit(REFUNDING,PAID) 走胜出分支:订单拉回 PAID、settled 快照被覆盖、
// order.paid 二次发出,本测试失败。
func TestSecondPaymentDuringRefundIsAnomaly(t *testing.T) {
	h := newHarness(true)
	out, err := h.order.CreateOrder(bg, baseCreate())
	if err != nil {
		t.Fatalf("下单失败: %v", err)
	}
	firstOutTradeNo := h.mem.Attempts(out.OrderNo)[0].OutTradeNo
	acct := h.mem.Attempts(out.OrderNo)[0].PayAccountID
	if _, err := h.payment.ConfirmPaySuccess(bg, confirmInput(h, out.OrderNo, acct, "tx-first", 90)); err != nil {
		t.Fatalf("首笔确认失败: %v", err)
	}
	// 播种第二尝试 CLOSING(支付成功时 closeOtherActiveAttempts 留下的、网关侧关单未生效)。
	err = h.mem.WithinTx(bg, func(ctx context.Context, r store.Repos) error {
		return r.Attempts().Insert(ctx, &models.PaymentAttempt{
			AppID: "app1", OrderNo: out.OrderNo, AttemptNo: 2, Channel: "wechat",
			PayAccountID: "acc-wechat", OutTradeNo: "PT-second", Status: models.AttemptClosing,
			ExpireAt: time.Now().Add(time.Hour),
		})
	})
	if err != nil {
		t.Fatalf("播种第二尝试失败: %v", err)
	}
	// 发起退款 → 订单 REFUNDING(在途退款存在)。
	if _, err := h.refund.RefundOrder(bg, refundapp.RefundInput{AppID: "app1", OrderNo: out.OrderNo, RefundAmount: 90, OutRefundNo: "r1"}); err != nil {
		t.Fatalf("发起退款失败: %v", err)
	}
	paidEventsBefore := countEvents(h, models.EventTypePaid)

	// 用户在渠道侧支付了 CLOSING 尝试:新交易号、等额。
	res, err := h.payment.ConfirmPaySuccess(bg, paymentapp.ConfirmInput{
		AppID: "app1", OrderNo: out.OrderNo, OutTradeNo: "PT-second", Channel: "wechat",
		PayAccountID: "acc-wechat", TransactionID: "tx-second", Amount: 90, Currency: "CNY", PaidAt: time.Now(),
	})
	if err != nil {
		t.Fatalf("第二笔确认不应报错(须落 anomaly 流水): %v", err)
	}
	if !res.Anomaly || res.AnomalyKind != "second_real_payment" {
		t.Fatalf("退款中收到新真实交易应判 second_real_payment: %+v", res)
	}
	o, _ := h.mem.Reader().Orders().GetByOrderNo(bg, "app1", out.OrderNo, false)
	if o.Status != models.OrderRefunding {
		t.Fatalf("订单必须保持 REFUNDING,实际: %v(被第二笔拉回)", o.Status)
	}
	if o.SettledOutTradeNo != firstOutTradeNo {
		t.Fatalf("settled 快照不得被覆盖: 期望 %s 实际 %s", firstOutTradeNo, o.SettledOutTradeNo)
	}
	if got := countEvents(h, models.EventTypePaid); got != paidEventsBefore {
		t.Fatalf("不得二次发 order.paid: before=%d after=%d", paidEventsBefore, got)
	}
	// 资金真相已落库:第二笔为 anomaly 流水;尝试收敛为 PAID 释放活跃槽。
	anomalies := 0
	for _, tx := range h.mem.Txns() {
		if tx.Anomaly {
			anomalies++
		}
	}
	if anomalies != 1 {
		t.Fatalf("应恰有 1 条 anomaly 流水,实际 %d", anomalies)
	}
	for _, a := range h.mem.Attempts(out.OrderNo) {
		if a.OutTradeNo == "PT-second" && a.Status != models.AttemptPaid {
			t.Fatalf("被支付的尝试应收敛 PAID(释放活跃槽),实际: %v", a.Status)
		}
	}
}

// TestClosedOrderRecoveryStillSettles 回归护栏:CLOSED 订单收到真实付款仍可异常恢复为 PAID。
func TestClosedOrderRecoveryStillSettles(t *testing.T) {
	h := newHarness(false)
	out, err := h.order.CreateOrder(bg, baseCreate())
	if err != nil {
		t.Fatalf("下单失败: %v", err)
	}
	acct := h.mem.Attempts(out.OrderNo)[0].PayAccountID
	// 播种"已关单":尝试 CLOSED + 订单 CLOSED。
	err = h.mem.WithinTx(bg, func(ctx context.Context, r store.Repos) error {
		a, e := r.Attempts().GetActiveByOrder(ctx, out.OrderNo, true)
		if e != nil {
			return e
		}
		a.Status = models.AttemptClosed
		if e := r.Attempts().Update(ctx, a); e != nil {
			return e
		}
		o, e := r.Orders().GetByOrderNo(ctx, "app1", out.OrderNo, true)
		if e != nil {
			return e
		}
		o.Status = models.OrderClosed
		return r.Orders().Update(ctx, o)
	})
	if err != nil {
		t.Fatalf("播种关单失败: %v", err)
	}
	res, err := h.payment.ConfirmPaySuccess(bg, confirmInput(h, out.OrderNo, acct, "tx-late", 90))
	if err != nil || res.Outcome != "paid" {
		t.Fatalf("CLOSED→PAID 异常恢复应仍生效: res=%+v err=%v", res, err)
	}
	o, _ := h.mem.Reader().Orders().GetByOrderNo(bg, "app1", out.OrderNo, false)
	if o.Status != models.OrderPaid {
		t.Fatalf("订单应恢复 PAID,实际: %v", o.Status)
	}
}

// TestDuplicateConfirmHealsAttempt 反证:交易已结算本订单而尝试被打回活跃态(UNKNOWN)时,
// 同交易号再确认 → duplicate 且尝试收敛 PAID(旧实现不动尝试,查单 worker 无限轮询)。
func TestDuplicateConfirmHealsAttempt(t *testing.T) {
	h := newHarness(false)
	orderNo, _ := h.createAndPay(t, "")
	outTradeNo := h.mem.Attempts(orderNo)[0].OutTradeNo
	acct := h.mem.Attempts(orderNo)[0].PayAccountID
	// 模拟尝试被并发陈旧回写打回 UNKNOWN(订单仍 PAID、流水在库)。
	err := h.mem.WithinTx(bg, func(ctx context.Context, r store.Repos) error {
		a, e := r.Attempts().GetByOutTradeNo(ctx, outTradeNo, true)
		if e != nil {
			return e
		}
		a.Status = models.AttemptUnknown
		return r.Attempts().Update(ctx, a)
	})
	if err != nil {
		t.Fatalf("打回尝试失败: %v", err)
	}
	res, err := h.payment.ConfirmPaySuccess(bg, confirmInput(h, orderNo, acct, "tx-"+orderNo, 90))
	if err != nil || res.Outcome != "duplicate" {
		t.Fatalf("同交易号再确认应 duplicate: res=%+v err=%v", res, err)
	}
	if got := h.mem.Attempts(orderNo)[0].Status; got != models.AttemptPaid {
		t.Fatalf("duplicate 应把打回的尝试收敛 PAID,实际: %v", got)
	}
}

// TestCancelAfterAttemptsConfirmedClosed 反证 order-close-convergence:唯一提交过的尝试
// 已被查单确认 CLOSED(无活跃尝试)后用户取消 → 订单同事务落定 CLOSED + closed 事件
// (旧实现停在 CLOSING、无事件、无 worker 认领,本测试失败)。
func TestCancelAfterAttemptsConfirmedClosed(t *testing.T) {
	h := newHarness(false)
	out, err := h.order.CreateOrder(bg, baseCreate())
	if err != nil {
		t.Fatalf("下单失败: %v", err)
	}
	outTradeNo := h.mem.Attempts(out.OrderNo)[0].OutTradeNo
	// 模拟查单收敛:尝试 UNKNOWN → 查证不存在 → CLOSED(订单仍 PENDING)。
	err = h.mem.WithinTx(bg, func(ctx context.Context, r store.Repos) error {
		a, e := r.Attempts().GetByOutTradeNo(ctx, outTradeNo, true)
		if e != nil {
			return e
		}
		a.Status = models.AttemptUnknown
		return r.Attempts().Update(ctx, a)
	})
	if err != nil {
		t.Fatalf("置 UNKNOWN 失败: %v", err)
	}
	if err := h.payment.CloseAttemptAndMaybeOrder(bg, outTradeNo); err != nil {
		t.Fatalf("关闭尝试失败: %v", err)
	}

	st, err := h.order.CancelOrder(bg, "app1", out.OrderNo, 42)
	if err != nil {
		t.Fatalf("取消失败: %v", err)
	}
	if st != models.OrderClosed {
		t.Fatalf("无活跃尝试的取消应落定 CLOSED,实际: %v(滞留 CLOSING)", st)
	}
	if got := countEvents(h, models.EventTypeClosed); got != 1 {
		t.Fatalf("应发 1 条 order.closed 事件,实际 %d", got)
	}
}

// TestExpireAfterAttemptsConfirmedClosed 同上前置,过期路径由超时关单落定 CLOSED。
func TestExpireAfterAttemptsConfirmedClosed(t *testing.T) {
	h := newHarness(false)
	out, err := h.order.CreateOrder(bg, baseCreate())
	if err != nil {
		t.Fatalf("下单失败: %v", err)
	}
	outTradeNo := h.mem.Attempts(out.OrderNo)[0].OutTradeNo
	err = h.mem.WithinTx(bg, func(ctx context.Context, r store.Repos) error {
		a, e := r.Attempts().GetByOutTradeNo(ctx, outTradeNo, true)
		if e != nil {
			return e
		}
		a.Status = models.AttemptUnknown
		if e := r.Attempts().Update(ctx, a); e != nil {
			return e
		}
		return nil
	})
	if err != nil {
		t.Fatalf("置 UNKNOWN 失败: %v", err)
	}
	if err := h.payment.CloseAttemptAndMaybeOrder(bg, outTradeNo); err != nil {
		t.Fatalf("关闭尝试失败: %v", err)
	}
	// 把订单改为已过期(memstore Update 写整行,可改 ExpireAt)。
	err = h.mem.WithinTx(bg, func(ctx context.Context, r store.Repos) error {
		o, e := r.Orders().GetByOrderNo(ctx, "app1", out.OrderNo, true)
		if e != nil {
			return e
		}
		o.ExpireAt = time.Date(2026, 7, 17, 11, 0, 0, 0, time.UTC) // 早于 harness 时钟 12:00
		return r.Orders().Update(ctx, o)
	})
	if err != nil {
		t.Fatalf("改过期时间失败: %v", err)
	}
	st, err := h.order.CloseIfExpired(bg, "app1", out.OrderNo)
	if err != nil {
		t.Fatalf("超时关单失败: %v", err)
	}
	if st != models.OrderClosed {
		t.Fatalf("无活跃尝试的超时关单应落定 CLOSED,实际: %v", st)
	}
	if got := countEvents(h, models.EventTypeClosed); got != 1 {
		t.Fatalf("应发 1 条 order.closed 事件,实际 %d", got)
	}
}

// TestClientOrderNoConflict 同 app 同 client_order_no(无幂等键)二次下单 → 可判定冲突错误。
func TestClientOrderNoConflict(t *testing.T) {
	h := newHarness(false)
	in := baseCreate()
	in.ClientOrderNo = "biz-1"
	if _, err := h.order.CreateOrder(bg, in); err != nil {
		t.Fatalf("首单失败: %v", err)
	}
	_, err := h.order.CreateOrder(bg, in)
	if !errors.Is(err, models.ErrClientOrderNoConflict) {
		t.Fatalf("重复 client_order_no 应返回 ErrClientOrderNoConflict,实际: %v", err)
	}
}

// countEvents 统计某类型事件条数。
func countEvents(h *harness, eventType string) int {
	n := 0
	for _, e := range h.mem.Events() {
		if e.EventType == eventType {
			n++
		}
	}
	return n
}
