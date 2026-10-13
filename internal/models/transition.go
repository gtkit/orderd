package models

// 状态机转移表(权威规格,与 DESIGN §9/§10/§15 的 mermaid 图逐条对应)。
//
// 定位:CanX 是【规格 + 测试断言目标】——生产路径用各场景的显式判定(锁行 + 状态复核、
// CanSettlePayment 白名单等),不在运行时查本表;表由 domain_test 的跃迁矩阵逐条钉住,
// 防止实现偏离规格。这是有意保留的规格代码,不是待清理的死代码。

// ---------------------------------------------------------------------
// 订单状态机(DESIGN §15)
// ---------------------------------------------------------------------
//
// PENDING → PAID / CANCELLED(无支付单) / CLOSING(有支付单/超时/被取代)
// CLOSING → CLOSED / PAID(关单窗口内已付)
// CLOSED  → PAID(回调或查单验证真实付款后恢复)
// PAID    → REFUNDING
// REFUNDING → REFUNDED / PAID(退款失败回退) / REFUND_EXCEPTION
// REFUND_EXCEPTION → REFUNDED / PAID(仅人工落定 ResolveRefundAbnormal,带审计)
// CANCELLED → PAID(已验证付款恢复);REFUNDED:支付不可恢复
var orderTransitions = map[OrderStatus]map[OrderStatus]bool{
	OrderPending:         {OrderPaid: true, OrderCancelled: true, OrderClosing: true},
	OrderClosing:         {OrderClosed: true, OrderPaid: true},
	OrderClosed:          {OrderPaid: true},
	OrderCancelled:       {OrderPaid: true},
	OrderPaid:            {OrderRefunding: true},
	OrderRefunding:       {OrderRefunded: true, OrderPaid: true, OrderRefundException: true},
	OrderRefundException: {OrderRefunded: true, OrderPaid: true},
}

// CanOrderTransit 报告订单能否从 from 跃迁到 to。
func CanOrderTransit(from, to OrderStatus) bool { return orderTransitions[from][to] }

// CanSettlePayment 报告订单当前状态能否接受一笔支付成功结算(胜出:跃迁 PAID + 写快照 + 发事件)。
// 注意它不是 CanOrderTransit(s, OrderPaid):REFUNDING→PAID 是【退款失败回退】专用边,
// 支付确认路径绝不允许借道——退款中/已退款订单收到的新真实交易只能落 anomaly 流水
// (second_real_payment),否则会把订单从退款域拉回 PAID、覆盖 settled_* 快照并二次发 order.paid。
// 白名单:PENDING/CLOSING 正常结算;CLOSED/CANCELLED→PAID 为已验证真实付款恢复。
func CanSettlePayment(s OrderStatus) bool {
	switch s {
	case OrderPending, OrderClosing, OrderClosed, OrderCancelled:
		return true
	default:
		return false
	}
}

// ---------------------------------------------------------------------
// 支付尝试状态机(DESIGN §9)
// ---------------------------------------------------------------------
//
// CREATED → REQUESTING
// REQUESTING → READY / UNKNOWN / PAID(回调早于下单返回) / FAILED / CLOSING
// READY → REQUESTING(同渠道重入) / PAID / CLOSING / CLOSED(渠道确认关闭或到期后查证不存在)
// UNKNOWN → REQUESTING(同渠道重入) / READY(查单=有单) / PAID / CLOSING / CLOSED(到期加宽限后查证不存在)
// CLOSING → CLOSED / PAID(关单窗口内已付)
// CLOSED → PAID(异常恢复)
// PAID / FAILED:终态
var attemptTransitions = map[AttemptStatus]map[AttemptStatus]bool{
	AttemptCreated:    {AttemptRequesting: true},
	AttemptRequesting: {AttemptReady: true, AttemptUnknown: true, AttemptPaid: true, AttemptFailed: true, AttemptClosing: true},
	AttemptReady:      {AttemptRequesting: true, AttemptPaid: true, AttemptClosing: true, AttemptClosed: true},
	AttemptUnknown:    {AttemptRequesting: true, AttemptClosing: true, AttemptReady: true, AttemptPaid: true, AttemptClosed: true},
	AttemptClosing:    {AttemptClosed: true, AttemptPaid: true},
	AttemptClosed:     {AttemptPaid: true},
}

// CanAttemptTransit 报告支付尝试能否从 from 跃迁到 to。
func CanAttemptTransit(from, to AttemptStatus) bool { return attemptTransitions[from][to] }

// ---------------------------------------------------------------------
// 退款状态机(DESIGN §10)
// ---------------------------------------------------------------------
//
// CREATED → PROCESSING / FAILED(确定未发出或渠道明确拒绝)
// CREATED → SUCCEEDED / ABNORMAL(受理写回未完成时已获得渠道事实)
// PROCESSING → SUCCEEDED / FAILED / CLOSED / ABNORMAL
// ABNORMAL → SUCCEEDED / FAILED(仅人工落定 ResolveRefundAbnormal,带审计)
// SUCCEEDED / FAILED / CLOSED:终态
var refundTransitions = map[RefundStatus]map[RefundStatus]bool{
	RefundCreated:    {RefundProcessing: true, RefundSucceeded: true, RefundFailed: true, RefundAbnormal: true},
	RefundProcessing: {RefundSucceeded: true, RefundFailed: true, RefundClosed: true, RefundAbnormal: true},
	RefundAbnormal:   {RefundSucceeded: true, RefundFailed: true},
}

// CanRefundTransit 报告退款能否从 from 跃迁到 to。
func CanRefundTransit(from, to RefundStatus) bool { return refundTransitions[from][to] }
