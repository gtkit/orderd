package models

// 状态机转移表(权威,与 DESIGN §9/§10/§15 的 mermaid 图逐条对应)。
// CanX 只判定"是否允许该跃迁";具体 CAS 与副作用由应用层在事务内完成。

// ---------------------------------------------------------------------
// 订单状态机(DESIGN §15)
// ---------------------------------------------------------------------
//
// PENDING → PAID / CANCELLED(无支付单) / CLOSING(有支付单/超时/被取代)
// CLOSING → CLOSED / PAID(关单窗口内已付)
// CLOSED  → PAID(异常恢复:网关查单证明真实付款)
// PAID    → REFUNDING
// REFUNDING → REFUNDED / PAID(退款失败回退) / REFUND_EXCEPTION
// CANCELLED / REFUNDED / REFUND_EXCEPTION:终态(自动机无出边;异常人工处理走独立审计路径)
var orderTransitions = map[OrderStatus]map[OrderStatus]bool{
	OrderPending:   {OrderPaid: true, OrderCancelled: true, OrderClosing: true},
	OrderClosing:   {OrderClosed: true, OrderPaid: true},
	OrderClosed:    {OrderPaid: true},
	OrderPaid:      {OrderRefunding: true},
	OrderRefunding: {OrderRefunded: true, OrderPaid: true, OrderRefundException: true},
}

// CanOrderTransit 报告订单能否从 from 跃迁到 to。
func CanOrderTransit(from, to OrderStatus) bool { return orderTransitions[from][to] }

// ---------------------------------------------------------------------
// 支付尝试状态机(DESIGN §9)
// ---------------------------------------------------------------------
//
// CREATED → REQUESTING
// REQUESTING → READY / UNKNOWN / PAID(回调早于下单返回) / FAILED
// READY → PAID / CLOSING
// UNKNOWN → READY(查单=有单) / PAID(查单=已付) / CLOSED(多次查证不存在)
// CLOSING → CLOSED / PAID(关单窗口内已付)
// CLOSED → PAID(异常恢复)
// PAID / FAILED:终态
var attemptTransitions = map[AttemptStatus]map[AttemptStatus]bool{
	AttemptCreated:    {AttemptRequesting: true},
	AttemptRequesting: {AttemptReady: true, AttemptUnknown: true, AttemptPaid: true, AttemptFailed: true},
	AttemptReady:      {AttemptPaid: true, AttemptClosing: true},
	AttemptUnknown:    {AttemptReady: true, AttemptPaid: true, AttemptClosed: true},
	AttemptClosing:    {AttemptClosed: true, AttemptPaid: true},
	AttemptClosed:     {AttemptPaid: true},
}

// CanAttemptTransit 报告支付尝试能否从 from 跃迁到 to。
func CanAttemptTransit(from, to AttemptStatus) bool { return attemptTransitions[from][to] }

// ---------------------------------------------------------------------
// 退款状态机(DESIGN §10)
// ---------------------------------------------------------------------
//
// CREATED → PROCESSING
// PROCESSING → SUCCEEDED / FAILED / CLOSED / ABNORMAL
// SUCCEEDED / FAILED / CLOSED / ABNORMAL:终态(ABNORMAL 人工处理走独立路径)
var refundTransitions = map[RefundStatus]map[RefundStatus]bool{
	RefundCreated:    {RefundProcessing: true},
	RefundProcessing: {RefundSucceeded: true, RefundFailed: true, RefundClosed: true, RefundAbnormal: true},
}

// CanRefundTransit 报告退款能否从 from 跃迁到 to。
func CanRefundTransit(from, to RefundStatus) bool { return refundTransitions[from][to] }
