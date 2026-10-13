package apptest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gtkit/orderd/internal/models"
	refundapp "github.com/gtkit/orderd/internal/module/refund/application"
	"github.com/gtkit/orderd/internal/paygateway"
	"github.com/gtkit/orderd/internal/store"
)

// refundTxns 返回指定退款单的全部 kind=2 流水快照。
func refundTxns(h *harness, refundNo string) []models.PaymentTransaction {
	var out []models.PaymentTransaction
	for _, tx := range h.mem.Txns() {
		if tx.Kind == models.TxnRefund && tx.RefundNo == refundNo {
			out = append(out, tx)
		}
	}
	return out
}

// TestRefundTxnUsesGatewayRefundID 反证 refund-ledger:退款查单返回的渠道交易号与原支付相同时,
// kind=2 流水仍必须存在且 transaction_id 为渠道退款单号(旧实现复用原支付交易号,
// 与 kind=1 流水撞 uk_txn 被静默吞掉,本测试在旧实现下失败)。
func TestRefundTxnUsesGatewayRefundID(t *testing.T) {
	h := newHarness(true)
	orderNo, token := h.createAndPay(t, "") // 支付流水 transaction_id = "tx-"+orderNo
	out, err := h.refund.RefundOrder(bg, refundapp.RefundInput{AppID: "app1", OrderNo: orderNo, RefundAmount: 90, OutRefundNo: "r1"})
	if err != nil || out.Status != models.RefundProcessing {
		t.Fatalf("退款受理失败: out=%+v err=%v", out, err)
	}
	// 渠道退款查单:渠道交易号 = 原支付交易号(支付宝/微信的真实语义),渠道退款单号 = gwr-1。
	h.gateway.mu.Lock()
	h.gateway.refundQuery = paygateway.RefundQueryResult{
		State: paygateway.RefundQuerySucceeded, GatewayRefundID: "gwr-1", TransactionID: "tx-" + orderNo, Amount: 90,
	}
	h.gateway.refundQuerySet = true
	h.gateway.mu.Unlock()

	rf, err := h.mem.Reader().Refunds().GetByRefundNo(bg, out.RefundNo, false)
	if err != nil {
		t.Fatalf("读退款失败: %v", err)
	}
	if err := h.refund.QueryRefund(bg, rf); err != nil {
		t.Fatalf("退款查单失败: %v", err)
	}

	txns := refundTxns(h, out.RefundNo)
	if len(txns) != 1 {
		t.Fatalf("退款成功必须产生一条 kind=2 流水,实际 %d 条", len(txns))
	}
	if txns[0].TransactionID != "gwr-1" || txns[0].Amount != 90 {
		t.Fatalf("退款流水 transaction_id 应为渠道退款单号 gwr-1: %+v", txns[0])
	}
	o, _ := h.order.GetOrderStatus(bg, "app1", token, 42)
	if o.Status != models.OrderRefunded {
		t.Fatalf("订单应 REFUNDED,实际: %v", o.Status)
	}
}

// TestRefundTxnFallsBackToOutRefundNo 渠道退款单号为空时,流水 transaction_id 回退 out_refund_no。
func TestRefundTxnFallsBackToOutRefundNo(t *testing.T) {
	h := newHarness(true)
	orderNo, _ := h.createAndPay(t, "")
	out, err := h.refund.RefundOrder(bg, refundapp.RefundInput{AppID: "app1", OrderNo: orderNo, RefundAmount: 90, OutRefundNo: "r1"})
	if err != nil {
		t.Fatalf("退款受理失败: %v", err)
	}
	if err := h.refund.ConfirmRefundSuccess(bg, out.RefundNo, "", 90, time.Now()); err != nil {
		t.Fatalf("确认退款成功失败: %v", err)
	}
	txns := refundTxns(h, out.RefundNo)
	if len(txns) != 1 || txns[0].TransactionID != "r1" {
		t.Fatalf("渠道退款单号为空应回退 out_refund_no: %+v", txns)
	}
}

// TestRefundConfirmIdempotent 同一退款重复确认:流水仅一条、已退金额只累计一次、refunded 事件只有一条。
func TestRefundConfirmIdempotent(t *testing.T) {
	h := newHarness(true)
	orderNo, token := h.createAndPay(t, "")
	out, err := h.refund.RefundOrder(bg, refundapp.RefundInput{AppID: "app1", OrderNo: orderNo, RefundAmount: 90, OutRefundNo: "r1"})
	if err != nil {
		t.Fatalf("退款受理失败: %v", err)
	}
	for range 2 {
		if err := h.refund.ConfirmRefundSuccess(bg, out.RefundNo, "gwr-9", 90, time.Now()); err != nil {
			t.Fatalf("确认退款成功失败: %v", err)
		}
	}
	if txns := refundTxns(h, out.RefundNo); len(txns) != 1 {
		t.Fatalf("重复确认应只有一条退款流水,实际 %d 条", len(txns))
	}
	o, _ := h.order.GetOrderStatus(bg, "app1", token, 42)
	if o.RefundedAmount != 90 {
		t.Fatalf("已退金额应只累计一次(90),实际: %d", o.RefundedAmount)
	}
	refunded := 0
	for _, e := range h.mem.Events() {
		if e.EventType == models.EventTypeRefunded {
			refunded++
		}
	}
	if refunded != 1 {
		t.Fatalf("refunded 事件应只有一条,实际 %d", refunded)
	}
}

// TestRefundGatewayErrorGoesProcessing 反证 refund-convergence:网关退款调用返回错误(结果未知)
// 不得判失败——退款置 PROCESSING 交查单收敛,订单保持 REFUNDING(旧实现 FAILED+回退 PAID,本测试失败)。
func TestRefundGatewayErrorGoesProcessing(t *testing.T) {
	h := newHarness(true)
	orderNo, token := h.createAndPay(t, "")
	h.gateway.mu.Lock()
	h.gateway.refundErr = errors.New("context deadline exceeded")
	h.gateway.mu.Unlock()

	out, err := h.refund.RefundOrder(bg, refundapp.RefundInput{AppID: "app1", OrderNo: orderNo, RefundAmount: 90, OutRefundNo: "r1"})
	if err != nil || out.Status != models.RefundProcessing {
		t.Fatalf("结果未知应返回 PROCESSING 且无错误: out=%+v err=%v", out, err)
	}
	rf, err := h.mem.Reader().Refunds().GetByRefundNo(bg, out.RefundNo, false)
	if err != nil || rf.Status != models.RefundProcessing || !rf.NextQueryAt.Valid {
		t.Fatalf("退款应 PROCESSING 且已设查单时间: rf=%+v err=%v", rf, err)
	}
	o, _ := h.order.GetOrderStatus(bg, "app1", token, 42)
	if o.Status != models.OrderRefunding {
		t.Fatalf("订单应保持 REFUNDING,实际: %v", o.Status)
	}
}

// TestRefundGatewayRejectedFails 回归护栏:网关同步应答明确未受理 → FAILED + 订单回退 PAID。
func TestRefundGatewayRejectedFails(t *testing.T) {
	h := newHarness(true)
	orderNo, token := h.createAndPay(t, "")
	h.gateway.mu.Lock()
	h.gateway.refundNotAccept = true
	h.gateway.mu.Unlock()

	out, err := h.refund.RefundOrder(bg, refundapp.RefundInput{AppID: "app1", OrderNo: orderNo, RefundAmount: 90, OutRefundNo: "r1"})
	if err != nil || out.Status != models.RefundFailed {
		t.Fatalf("明确拒绝应返回 FAILED: out=%+v err=%v", out, err)
	}
	rf, _ := h.mem.Reader().Refunds().GetByRefundNo(bg, out.RefundNo, false)
	if rf.Status != models.RefundFailed {
		t.Fatalf("退款应 FAILED,实际: %v", rf.Status)
	}
	o, _ := h.order.GetOrderStatus(bg, "app1", token, 42)
	if o.Status != models.OrderPaid {
		t.Fatalf("订单应回退 PAID,实际: %v", o.Status)
	}
}

// TestRefundQueryNotExistThreshold NotExist 阈值终态:未达阈值保持 PROCESSING 观察;
// 达阈值转人工并保留退款守卫;渠道报处理中永不因次数落终态。
func TestRefundQueryNotExistThreshold(t *testing.T) {
	notExist := paygateway.RefundQueryResult{State: paygateway.RefundQueryNotExist}
	processing := paygateway.RefundQueryResult{State: paygateway.RefundQueryProcessing}
	tests := []struct {
		name          string
		notExistCount int32 // 已连续查证不存在次数,不复用总查询次数
		qr            paygateway.RefundQueryResult
		wantRefund    models.RefundStatus
		wantOrder     models.OrderStatus
	}{
		{"NotExist 未达阈值保持观察", 4, notExist, models.RefundProcessing, models.OrderRefunding},
		{"NotExist 达阈值转人工", 5, notExist, models.RefundAbnormal, models.OrderRefundException},
		{"Processing 超次数不落终态", 100, processing, models.RefundProcessing, models.OrderRefunding},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(true)
			orderNo, token := h.createAndPay(t, "")
			h.gateway.refundErr = errors.New("提交结果未知")
			out, err := h.refund.RefundOrder(bg, refundapp.RefundInput{AppID: "app1", OrderNo: orderNo, RefundAmount: 90, OutRefundNo: "r1"})
			if err != nil {
				t.Fatalf("退款受理失败: %v", err)
			}
			// 播种已查次数。
			err = h.mem.WithinTx(bg, func(ctx context.Context, r store.Repos) error {
				rf, e := r.Refunds().GetByRefundNo(ctx, out.RefundNo, true)
				if e != nil {
					return e
				}
				rf.NotExistCount = tc.notExistCount
				return r.Refunds().Update(ctx, rf)
			})
			if err != nil {
				t.Fatalf("播种查询次数失败: %v", err)
			}
			h.gateway.mu.Lock()
			h.gateway.refundQuery = tc.qr
			h.gateway.refundQuerySet = true
			h.gateway.mu.Unlock()

			rf, _ := h.mem.Reader().Refunds().GetByRefundNo(bg, out.RefundNo, false)
			if err := h.refund.QueryRefund(bg, rf); err != nil {
				t.Fatalf("退款查单失败: %v", err)
			}
			got, _ := h.mem.Reader().Refunds().GetByRefundNo(bg, out.RefundNo, false)
			if got.Status != tc.wantRefund {
				t.Fatalf("退款状态期望 %v,实际 %v", tc.wantRefund, got.Status)
			}
			o, _ := h.order.GetOrderStatus(bg, "app1", token, 42)
			if o.Status != tc.wantOrder {
				t.Fatalf("订单状态期望 %v,实际 %v", tc.wantOrder, o.Status)
			}
		})
	}
}

// TestRefundAmountMismatchGoesAbnormal 反证 refund-abnormal-terminal:退款成功金额与退款单
// 不符 → 同事务落人工终态(退款 ABNORMAL + 订单 REFUND_EXCEPTION),告警一次,无流水无事件;
// 重复确认幂等;再次发起退款被拒(旧实现停 PROCESSING + 返回 error,本测试失败)。
func TestRefundAmountMismatchGoesAbnormal(t *testing.T) {
	h := newHarness(true)
	orderNo, token := h.createAndPay(t, "")
	out, err := h.refund.RefundOrder(bg, refundapp.RefundInput{AppID: "app1", OrderNo: orderNo, RefundAmount: 90, OutRefundNo: "r1"})
	if err != nil {
		t.Fatalf("退款受理失败: %v", err)
	}
	h.gateway.mu.Lock()
	h.gateway.refundQuery = paygateway.RefundQueryResult{
		State: paygateway.RefundQuerySucceeded, GatewayRefundID: "gwr", Amount: 50, // ≠90
	}
	h.gateway.refundQuerySet = true
	h.gateway.mu.Unlock()

	rf, _ := h.mem.Reader().Refunds().GetByRefundNo(bg, out.RefundNo, false)
	if err := h.refund.QueryRefund(bg, rf); err != nil {
		t.Fatalf("金额不符已终态处置,不应返回错误: %v", err)
	}
	got, _ := h.mem.Reader().Refunds().GetByRefundNo(bg, out.RefundNo, false)
	if got.Status != models.RefundAbnormal || !got.LastError.Valid {
		t.Fatalf("退款应 ABNORMAL 且记 last_error,实际: %+v", got)
	}
	o, _ := h.order.GetOrderStatus(bg, "app1", token, 42)
	if o.Status != models.OrderRefundException {
		t.Fatalf("订单应 REFUND_EXCEPTION,实际: %v", o.Status)
	}
	if o.RefundedAmount != 0 {
		t.Fatalf("不得累计已退金额: %d", o.RefundedAmount)
	}
	if n := h.alerter.count("refund_amount_mismatch"); n != 1 {
		t.Fatalf("应告警恰一次,实际 %d", n)
	}
	if txns := refundTxns(h, out.RefundNo); len(txns) != 0 {
		t.Fatalf("金额不符不得写退款流水: %+v", txns)
	}
	if n := countEvents(h, models.EventTypeRefunded); n != 0 {
		t.Fatalf("金额不符不得发 refunded 事件: %d", n)
	}
	// 重复的金额不符确认:幂等,不重复告警。
	if err := h.refund.ConfirmRefundSuccess(bg, out.RefundNo, "gwr", 50, time.Now()); err != nil {
		t.Fatalf("重复确认应幂等: %v", err)
	}
	if n := h.alerter.count("refund_amount_mismatch"); n != 1 {
		t.Fatalf("重复确认不得再告警,实际 %d", n)
	}
	// 人工确认前阻断再次退款(订单 REFUND_EXCEPTION → 非已支付被拒)。
	if _, err := h.refund.RefundOrder(bg, refundapp.RefundInput{AppID: "app1", OrderNo: orderNo, RefundAmount: 90, OutRefundNo: "r2"}); !errors.Is(err, models.ErrRefundNotAllowed) {
		t.Fatalf("REFUND_EXCEPTION 订单应拒绝再次退款,实际: %v", err)
	}
	o2, _ := h.order.GetOrderStatus(bg, "app1", token, 42)
	if o2.Status != models.OrderRefundException {
		t.Fatalf("订单应保持 REFUND_EXCEPTION,实际: %v", o2.Status)
	}
}
