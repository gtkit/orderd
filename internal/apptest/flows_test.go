package apptest

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/gtkit/orderd/internal/models"
	paymentapp "github.com/gtkit/orderd/internal/module/payment/application"
	refundapp "github.com/gtkit/orderd/internal/module/refund/application"
	"github.com/gtkit/orderd/internal/store"
)

var bg = context.Background()

func confirmInput(h *harness, orderNo, payAcct, txnID string, amount int64) paymentapp.ConfirmInput {
	atts := h.mem.Attempts(orderNo)
	outTradeNo := ""
	if len(atts) > 0 {
		outTradeNo = atts[0].OutTradeNo
	}
	return paymentapp.ConfirmInput{
		AppID: "app1", OrderNo: orderNo, OutTradeNo: outTradeNo, Channel: "wechat",
		PayAccountID: payAcct, TransactionID: txnID, Amount: amount, Currency: "CNY",
		PaidAt: time.Date(2026, 7, 17, 12, 0, 0, 0, time.UTC),
	}
}

// --- 下单 ---

func TestCreateOrderBasic(t *testing.T) {
	h := newHarness(false)
	out, err := h.order.CreateOrder(bg, baseCreate())
	if err != nil {
		t.Fatalf("下单失败: %v", err)
	}
	if out.Status != models.OrderPending || out.AppParams == "" || out.OrderToken == "" {
		t.Fatalf("下单结果异常: %+v", out)
	}
	atts := h.mem.Attempts(out.OrderNo)
	if len(atts) != 1 || atts[0].Status != models.AttemptReady {
		t.Fatalf("应有 1 个 READY 尝试,实际: %+v", atts)
	}
}

func TestCreateOrderIdempotency(t *testing.T) {
	h := newHarness(false)
	in := baseCreate()
	in.IdempotencyKey = "idem-1"
	first, err := h.order.CreateOrder(bg, in)
	if err != nil {
		t.Fatalf("首次下单失败: %v", err)
	}
	// 同 key 同 hash → 复用同一订单。
	again, err := h.order.CreateOrder(bg, in)
	if err != nil {
		t.Fatalf("复用下单失败: %v", err)
	}
	if !again.Reused || again.OrderNo != first.OrderNo {
		t.Fatalf("应复用原订单: first=%s again=%+v", first.OrderNo, again)
	}
	// 同 key 异 hash → 冲突。
	in2 := in
	in2.Amount = models.Money{Original: 200, Discount: 0, Payable: 200, Currency: "CNY"}
	if _, err := h.order.CreateOrder(bg, in2); !errors.Is(err, models.ErrIdempotencyConflict) {
		t.Fatalf("同键异请求应冲突,实际: %v", err)
	}
}

func TestCreateOrderConcurrentSameKey(t *testing.T) {
	h := newHarness(false)
	in := baseCreate()
	in.IdempotencyKey = "idem-concurrent"
	const n = 12
	var wg sync.WaitGroup
	var mu sync.Mutex
	orderNos := map[string]struct{}{}
	errs := 0
	for range n {
		wg.Go(func() {
			out, err := h.order.CreateOrder(bg, in)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errs++
				return
			}
			orderNos[out.OrderNo] = struct{}{}
		})
	}
	wg.Wait()
	if errs != 0 {
		t.Fatalf("并发同键下单不应报错,错误数=%d", errs)
	}
	if len(orderNos) != 1 {
		t.Fatalf("并发同键应只产生 1 个订单,实际=%d", len(orderNos))
	}
}

func TestCreateOrderAmountRejected(t *testing.T) {
	h := newHarness(false)
	bad := baseCreate()
	bad.BusinessType = "not-whitelisted"
	if _, err := h.order.CreateOrder(bg, bad); !errors.Is(err, models.ErrAmountPolicy) {
		t.Fatalf("非白名单业务类型应被拒,实际: %v", err)
	}
	bad2 := baseCreate()
	bad2.Amount = models.Money{Original: 100, Discount: 10, Payable: 80, Currency: "CNY"} // 破坏不变量
	if _, err := h.order.CreateOrder(bg, bad2); !errors.Is(err, models.ErrInvalidAmount) {
		t.Fatalf("金额不变量违反应被拒,实际: %v", err)
	}
}

func TestGetOrderStatus(t *testing.T) {
	h := newHarness(false)
	out, _ := h.order.CreateOrder(bg, baseCreate())
	o, err := h.order.GetOrderStatus(bg, "app1", out.OrderToken, 42)
	if err != nil || o.Status != models.OrderPending {
		t.Fatalf("查询失败: o=%+v err=%v", o, err)
	}
	if _, err := h.order.GetOrderStatus(bg, "app1", out.OrderToken, 999); !errors.Is(err, models.ErrOrderNotOwned) {
		t.Fatalf("越权查询应被拒,实际: %v", err)
	}
}

// TestCreateOrderPassesAmountSubject 评审 P0:统一下单必须带金额与标题。
func TestCreateOrderPassesAmountSubject(t *testing.T) {
	h := newHarness(false)
	if _, err := h.order.CreateOrder(bg, baseCreate()); err != nil {
		t.Fatalf("下单失败: %v", err)
	}
	h.gateway.mu.Lock()
	defer h.gateway.mu.Unlock()
	if h.gateway.lastPrepare.Amount != 90 || h.gateway.lastPrepare.Subject != "会员月卡" {
		t.Fatalf("Prepare 应收到金额=90/标题=会员月卡,实际: amount=%d subject=%q", h.gateway.lastPrepare.Amount, h.gateway.lastPrepare.Subject)
	}
}

func TestGetOrderStatusCrossTenant(t *testing.T) {
	h := newHarness(false)
	out, _ := h.order.CreateOrder(bg, baseCreate())
	if _, err := h.order.GetOrderStatus(bg, "app2", out.OrderToken, 42); !errors.Is(err, models.ErrOrderNotFound) {
		t.Fatalf("跨租户查询应等同不存在(评审 P1),实际: %v", err)
	}
}

// --- 支付成功(§7)---

// TestConfirmPayAccountMismatch 评审 P0:通知账号≠尝试冻结账号 → anomaly,不结算。
func TestConfirmPayAccountMismatch(t *testing.T) {
	h := newHarness(false)
	out, _ := h.order.CreateOrder(bg, baseCreate())
	res, err := h.payment.ConfirmPaySuccess(bg, confirmInput(h, out.OrderNo, "acc-wrong", "tx-m", 90))
	if err != nil {
		t.Fatalf("不应返回错误: %v", err)
	}
	if !res.Anomaly || res.AnomalyKind != "account_channel_mismatch" {
		t.Fatalf("应判 account_channel_mismatch: %+v", res)
	}
	o, _ := h.order.GetOrderStatus(bg, "app1", out.OrderToken, 42)
	if o.Status != models.OrderPending {
		t.Fatalf("账号不符不应结算,实际: %v", o.Status)
	}
	for _, tx := range h.mem.Txns() {
		if tx.Anomaly && tx.AnomalyKind != models.AnomalyKindAccountChannelMismatch {
			t.Fatalf("异常流水应落成因 account_channel_mismatch: %+v", tx)
		}
	}
}

// TestConfirmPayCrossOrderTxnReuse 评审 P0:交易号被另一订单占用 → 禁止结算本订单。
func TestConfirmPayCrossOrderTxnReuse(t *testing.T) {
	h := newHarness(false)
	a, _ := h.order.CreateOrder(bg, baseCreate())
	b, _ := h.order.CreateOrder(bg, baseCreate())
	if _, err := h.payment.ConfirmPaySuccess(bg, confirmInput(h, a.OrderNo, "acc-wechat", "tx-shared", 90)); err != nil {
		t.Fatalf("A 确认失败: %v", err)
	}
	res, err := h.payment.ConfirmPaySuccess(bg, confirmInput(h, b.OrderNo, "acc-wechat", "tx-shared", 90))
	if err != nil {
		t.Fatalf("不应返回错误: %v", err)
	}
	if !res.Anomaly {
		t.Fatalf("跨订单复用交易号应判异常: %+v", res)
	}
	ob, _ := h.order.GetOrderStatus(bg, "app1", b.OrderToken, 42)
	if ob.Status == models.OrderPaid {
		t.Fatal("B 不应被结算(交易号属 A)")
	}
}

func TestConfirmPaySuccessBasic(t *testing.T) {
	h := newHarness(false)
	out, _ := h.order.CreateOrder(bg, baseCreate())
	acct := h.mem.Attempts(out.OrderNo)[0].PayAccountID
	res, err := h.payment.ConfirmPaySuccess(bg, confirmInput(h, out.OrderNo, acct, "tx-1", 90))
	if err != nil || res.Outcome != "paid" {
		t.Fatalf("确认支付失败: res=%+v err=%v", res, err)
	}
	o, _ := h.order.GetOrderStatus(bg, "app1", out.OrderToken, 42)
	if o.Status != models.OrderPaid || o.AggregateVersion != 1 || o.SettledPayAccountID != acct {
		t.Fatalf("订单支付后状态异常: %+v", o)
	}
	if len(h.mem.Events()) != 1 || h.mem.Events()[0].EventType != models.EventTypePaid {
		t.Fatalf("应恰好产生 1 条 paid 事件: %+v", h.mem.Events())
	}
	if len(h.mem.Txns()) != 1 || h.mem.Txns()[0].Anomaly {
		t.Fatalf("应有 1 条正常流水: %+v", h.mem.Txns())
	}
	if len(h.mem.Outbox()) != 1 {
		t.Fatalf("应有 1 条 outbox")
	}
}

func TestConfirmPaySuccessDuplicateCallback(t *testing.T) {
	h := newHarness(false)
	out, _ := h.order.CreateOrder(bg, baseCreate())
	acct := h.mem.Attempts(out.OrderNo)[0].PayAccountID
	in := confirmInput(h, out.OrderNo, acct, "tx-dup", 90)
	if _, err := h.payment.ConfirmPaySuccess(bg, in); err != nil {
		t.Fatalf("首次确认失败: %v", err)
	}
	res, err := h.payment.ConfirmPaySuccess(bg, in) // 同 txn 重复回调
	if err != nil || res.Outcome != "duplicate" {
		t.Fatalf("重复回调应幂等: res=%+v err=%v", res, err)
	}
	if len(h.mem.Events()) != 1 {
		t.Fatalf("重复回调不应产生第二条事件: %d", len(h.mem.Events()))
	}
}

// TestConfirmPaySuccessSecondRealPayment 评审 #4:第二笔真实付款必须落不可变流水(anomaly)并告警,
// 不改订单、不重复发事件、不回滚丢证。
func TestConfirmPaySuccessSecondRealPayment(t *testing.T) {
	h := newHarness(false)
	out, _ := h.order.CreateOrder(bg, baseCreate())
	acct := h.mem.Attempts(out.OrderNo)[0].PayAccountID
	if _, err := h.payment.ConfirmPaySuccess(bg, confirmInput(h, out.OrderNo, acct, "tx-win", 90)); err != nil {
		t.Fatalf("首笔确认失败: %v", err)
	}
	// 第二笔不同交易号(另一渠道/重复扣款)真实到账。
	res, err := h.payment.ConfirmPaySuccess(bg, confirmInput(h, out.OrderNo, acct, "tx-second", 90))
	if err != nil {
		t.Fatalf("第二笔确认不应返回错误(须提交 anomaly 流水): %v", err)
	}
	if !res.Anomaly || res.AnomalyKind != "second_real_payment" {
		t.Fatalf("第二笔应判为 second_real_payment: %+v", res)
	}
	txns := h.mem.Txns()
	if len(txns) != 2 {
		t.Fatalf("两笔真实付款应各落一条流水,实际=%d", len(txns))
	}
	anomalies := 0
	for _, tx := range txns {
		if tx.Anomaly {
			anomalies++
			if tx.AnomalyKind != models.AnomalyKindSecondRealPayment {
				t.Fatalf("异常流水应落成因 second_real_payment: %+v", tx)
			}
		} else if tx.AnomalyKind != "" {
			t.Fatalf("正常流水成因应为空: %+v", tx)
		}
	}
	if anomalies != 1 {
		t.Fatalf("应恰有 1 条 anomaly 流水,实际=%d", anomalies)
	}
	if len(h.mem.Events()) != 1 {
		t.Fatalf("第二笔不应产生第二条 paid 事件: %d", len(h.mem.Events()))
	}
	if h.alerter.count("second_real_payment") != 1 {
		t.Fatalf("应告警 second_real_payment 一次")
	}
}

func TestConfirmPaySuccessAmountMismatch(t *testing.T) {
	h := newHarness(false)
	out, _ := h.order.CreateOrder(bg, baseCreate())
	acct := h.mem.Attempts(out.OrderNo)[0].PayAccountID
	res, err := h.payment.ConfirmPaySuccess(bg, confirmInput(h, out.OrderNo, acct, "tx-bad", 80)) // 金额不符
	if err != nil {
		t.Fatalf("金额不符不应返回错误(须落 anomaly 流水): %v", err)
	}
	if !res.Anomaly || res.AnomalyKind != "amount_mismatch" {
		t.Fatalf("应判为 amount_mismatch: %+v", res)
	}
	o, _ := h.order.GetOrderStatus(bg, "app1", out.OrderToken, 42)
	if o.Status != models.OrderPending {
		t.Fatalf("金额不符不应改订单状态,实际: %v", o.Status)
	}
	if len(h.mem.Txns()) != 1 || !h.mem.Txns()[0].Anomaly || h.mem.Txns()[0].AnomalyKind != models.AnomalyKindAmountMismatch {
		t.Fatalf("应落 1 条成因 amount_mismatch 的 anomaly 流水: %+v", h.mem.Txns())
	}
}

// --- 取消(§8,评审 #5)---

// seedOrder 直接向 store 播种订单(可选一个已提交网关的历史尝试),用于精确构造取消场景。
func seedOrder(t *testing.T, h *harness, orderNo string, withSubmittedAttempt bool) {
	t.Helper()
	err := h.mem.WithinTx(bg, func(ctx context.Context, r store.Repos) error {
		o := &models.Order{
			AppID: "app1", OrderNo: orderNo, OrderToken: "tok-" + orderNo, UserID: 42,
			Status: models.OrderPending, ProductID: 1, ProductTitle: "x",
			Currency: "CNY", OriginalPrice: 90, PayAmount: 90, PayMethod: models.PayMethodWechat,
			PayChannel: "wechat", PayAccountID: "acc-wechat", ExpireAt: time.Now().Add(time.Hour),
		}
		if err := r.Orders().Insert(ctx, o); err != nil {
			return err
		}
		if withSubmittedAttempt {
			return r.Attempts().Insert(ctx, &models.PaymentAttempt{
				AppID: "app1", OrderNo: orderNo, AttemptNo: 1, Channel: "wechat",
				PayAccountID: "acc-wechat", OutTradeNo: "PT-" + orderNo, Status: models.AttemptReady,
				ExpireAt: time.Now().Add(time.Hour),
			})
		}
		return nil
	})
	if err != nil {
		t.Fatalf("播种订单失败: %v", err)
	}
}

func TestCancelNeverSubmittedGoesCancelled(t *testing.T) {
	h := newHarness(false)
	seedOrder(t, h, "OD-nosubmit", false)
	st, err := h.order.CancelOrder(bg, "app1", "OD-nosubmit", 42)
	if err != nil || st != models.OrderCancelled {
		t.Fatalf("从未提交网关应进 CANCELLED: st=%v err=%v", st, err)
	}
	// CANCELLED 发 order.closed 事件(reason=user_cancel)。
	evs := h.mem.Events()
	if len(evs) != 1 || evs[0].EventType != models.EventTypeClosed {
		t.Fatalf("CANCELLED 应发 1 条 closed 事件: %+v", evs)
	}
}

func TestCancelWithSubmittedGoesClosing(t *testing.T) {
	h := newHarness(false)
	seedOrder(t, h, "OD-submitted", true)
	st, err := h.order.CancelOrder(bg, "app1", "OD-submitted", 42)
	if err != nil || st != models.OrderClosing {
		t.Fatalf("有已提交网关历史应进 CLOSING(评审 #5): st=%v err=%v", st, err)
	}
	// CLOSING 未确认关闭,暂不发 closed 事件。
	if len(h.mem.Events()) != 0 {
		t.Fatalf("CLOSING 暂不发事件: %+v", h.mem.Events())
	}
	// 活跃尝试应被置为 CLOSING。
	for _, a := range h.mem.Attempts("OD-submitted") {
		if a.Status == models.AttemptReady {
			t.Fatalf("取消后不应残留 READY 尝试: %+v", a)
		}
	}
}

// TestCancelVsPrepareRace 评审 #8:取消与建尝试并发,结果不得出现"订单已取消却仍有可支付尝试"。
func TestCancelVsPrepareRace(t *testing.T) {
	for iter := range 50 {
		h := newHarness(false)
		out, err := h.order.CreateOrder(bg, baseCreate())
		if err != nil {
			t.Fatalf("下单失败: %v", err)
		}
		var wg sync.WaitGroup
		wg.Go(func() { _, _ = h.order.CancelOrder(bg, "app1", out.OrderNo, 42) })
		wg.Go(func() {
			_, _ = h.payment.PreparePayment(bg, paymentapp.PrepareInput{
				AppID: "app1", OrderNo: out.OrderNo, UserID: 42, PayMethod: models.PayMethodWechat,
			})
		})
		wg.Wait()

		o, _ := h.order.GetOrderStatus(bg, "app1", out.OrderToken, 42)
		// 已提交网关,取消只能进 CLOSING(不可能 CANCELLED),或竞争中 prepare 先赢仍 PENDING。
		if o.Status != models.OrderClosing && o.Status != models.OrderPending {
			t.Fatalf("iter %d: 竞态后订单状态非法: %v", iter, o.Status)
		}
		if o.Status == models.OrderClosing {
			for _, a := range h.mem.Attempts(out.OrderNo) {
				if a.Status == models.AttemptReady {
					t.Fatalf("iter %d: 订单 CLOSING 却残留 READY 尝试(双支付风险)", iter)
				}
			}
		}
	}
}

// --- 退款(§10,评审 #6/#7)---

func (h *harness) createAndPay(t *testing.T, payAcct string) (orderNo, token string) {
	t.Helper()
	out, err := h.order.CreateOrder(bg, baseCreate())
	if err != nil {
		t.Fatalf("下单失败: %v", err)
	}
	acct := payAcct
	if acct == "" {
		acct = h.mem.Attempts(out.OrderNo)[0].PayAccountID
	}
	if _, err := h.payment.ConfirmPaySuccess(bg, confirmInput(h, out.OrderNo, acct, "tx-"+out.OrderNo, 90)); err != nil {
		t.Fatalf("确认支付失败: %v", err)
	}
	return out.OrderNo, out.OrderToken
}

func TestRefundDisabledByConfig(t *testing.T) {
	h := newHarness(false) // refund.enabled=false
	orderNo, _ := h.createAndPay(t, "")
	_, err := h.refund.RefundOrder(bg, refundapp.RefundInput{AppID: "app1", OrderNo: orderNo, RefundAmount: 90, OutRefundNo: "r1"})
	if !errors.Is(err, models.ErrRefundNotAllowed) {
		t.Fatalf("V1 授权关闭应拒退款,实际: %v", err)
	}
}

func TestRefundFullFlow(t *testing.T) {
	h := newHarness(true)
	orderNo, token := h.createAndPay(t, "")
	out, err := h.refund.RefundOrder(bg, refundapp.RefundInput{AppID: "app1", OrderNo: orderNo, RefundAmount: 90, OutRefundNo: "r1"})
	if err != nil || out.Status != models.RefundProcessing {
		t.Fatalf("退款受理失败: out=%+v err=%v", out, err)
	}
	// 网关确认退款成功 → 订单 REFUNDED + refunded 事件 + 退款流水(回指 refund_no)。
	if err := h.refund.ConfirmRefundSuccess(bg, out.RefundNo, "gwr", 90, time.Now()); err != nil {
		t.Fatalf("确认退款成功失败: %v", err)
	}
	o, _ := h.order.GetOrderStatus(bg, "app1", token, 42)
	if o.Status != models.OrderRefunded || o.RefundedAmount != 90 {
		t.Fatalf("退款后订单状态异常: %+v", o)
	}
	// 注意:memstore.Txns() 从 map 迭代返回,顺序随机——只取一次快照再遍历。
	txns := h.mem.Txns()
	var refundTxn *models.PaymentTransaction
	for i := range txns {
		if txns[i].Kind == models.TxnRefund {
			refundTxn = &txns[i]
		}
	}
	if refundTxn == nil || refundTxn.RefundNo != out.RefundNo {
		t.Fatalf("退款流水应回指 refund_no: %+v", refundTxn)
	}
	refunded := false
	for _, e := range h.mem.Events() {
		if e.EventType == models.EventTypeRefunded {
			refunded = true
		}
	}
	if !refunded {
		t.Fatalf("应产生 refunded 事件")
	}
}

// TestRefundUsesSettledAccount 评审 #6/P1:退款用成功交易的渠道+账号+原商户单号,而非下单首账号/首渠道。
func TestRefundUsesSettledAccount(t *testing.T) {
	h := newHarness(true)
	out, _ := h.order.CreateOrder(bg, baseCreate()) // 下单冻结 wechat / acc-wechat
	// 模拟换渠道:关首个尝试,播种胜出的第二尝试(alipay / acc-alipay),再按其确认支付。
	err := h.mem.WithinTx(bg, func(ctx context.Context, r store.Repos) error {
		first, _ := r.Attempts().GetActiveByOrder(ctx, out.OrderNo, true)
		first.Status = models.AttemptClosed
		if e := r.Attempts().Update(ctx, first); e != nil {
			return e
		}
		return r.Attempts().Insert(ctx, &models.PaymentAttempt{
			AppID: "app1", OrderNo: out.OrderNo, AttemptNo: 2, Channel: "alipay",
			PayAccountID: "acc-alipay", OutTradeNo: "PT-alipay-2", Status: models.AttemptReady,
			ExpireAt: time.Now().Add(time.Hour),
		})
	})
	if err != nil {
		t.Fatalf("播种第二尝试失败: %v", err)
	}
	win := paymentapp.ConfirmInput{
		AppID: "app1", OrderNo: out.OrderNo, OutTradeNo: "PT-alipay-2", Channel: "alipay",
		PayAccountID: "acc-alipay", TransactionID: "tx-ali", Amount: 90, Currency: "CNY", PaidAt: time.Now(),
	}
	if _, err := h.payment.ConfirmPaySuccess(bg, win); err != nil {
		t.Fatalf("确认支付失败: %v", err)
	}
	if _, err := h.refund.RefundOrder(bg, refundapp.RefundInput{AppID: "app1", OrderNo: out.OrderNo, RefundAmount: 90, OutRefundNo: "r1"}); err != nil {
		t.Fatalf("退款失败: %v", err)
	}
	h.gateway.mu.Lock()
	defer h.gateway.mu.Unlock()
	if len(h.gateway.refunds) != 1 {
		t.Fatalf("应调一次退款,实际 %d", len(h.gateway.refunds))
	}
	rf := h.gateway.refunds[0]
	if rf.PayAccountID != "acc-alipay" || rf.Channel != "alipay" || rf.OutTradeNo != "PT-alipay-2" {
		t.Fatalf("退款应用结算渠道/账号/原商户单号(alipay/acc-alipay/PT-alipay-2),实际: %+v", rf)
	}
}

// TestGetRefundCrossTenant 评审 P1:按 refund_no 查退款须校验 app_id,跨租户等同不存在。
func TestGetRefundCrossTenant(t *testing.T) {
	h := newHarness(true)
	orderNo, _ := h.createAndPay(t, "")
	out, err := h.refund.RefundOrder(bg, refundapp.RefundInput{AppID: "app1", OrderNo: orderNo, RefundAmount: 90, OutRefundNo: "r1"})
	if err != nil {
		t.Fatalf("退款失败: %v", err)
	}
	if _, err := h.refund.GetRefund(bg, "app2", out.RefundNo, ""); !errors.Is(err, models.ErrRefundNotFound) {
		t.Fatalf("跨租户按 refund_no 查退款应等同不存在,实际: %v", err)
	}
	if _, err := h.refund.GetRefund(bg, "app1", out.RefundNo, ""); err != nil {
		t.Fatalf("同租户查退款应成功: %v", err)
	}
}

// TestRefundActiveGuardBlocksAbnormal 评审 #7:异常退款未人工确认前阻断再次发起退款。
func TestRefundActiveGuardBlocksAbnormal(t *testing.T) {
	h := newHarness(true)
	orderNo, _ := h.createAndPay(t, "")
	// 播种一笔 ABNORMAL 退款(活跃)。
	err := h.mem.WithinTx(bg, func(ctx context.Context, r store.Repos) error {
		return r.Refunds().Insert(ctx, &models.Refund{
			AppID: "app1", RefundNo: "RF-abn", OutRefundNo: "r-abn", OrderNo: orderNo,
			RefundAmount: 90, Status: models.RefundAbnormal,
		})
	})
	if err != nil {
		t.Fatalf("播种异常退款失败: %v", err)
	}
	_, err = h.refund.RefundOrder(bg, refundapp.RefundInput{AppID: "app1", OrderNo: orderNo, RefundAmount: 90, OutRefundNo: "r-new"})
	if !errors.Is(err, models.ErrActiveRefundExists) {
		t.Fatalf("ABNORMAL 活跃退款应阻断再次退款,实际: %v", err)
	}
	// 订单应回滚保持 PAID(CAS 事务整体回滚)。
	o, err := h.mem.Reader().Orders().GetByOrderNo(bg, "app1", orderNo, false)
	if err != nil || o.Status != models.OrderPaid {
		t.Fatalf("退款被阻断后订单应仍 PAID: o=%+v err=%v", o, err)
	}
}
