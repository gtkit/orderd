// Package obs 定义关键路径埋点端口(纯基础包,不依赖任何指标库)。
// 实现:internal/platform/metrics.FlowMetrics(Prometheus);测试/最小装配用 Nop。
package obs

// 投递结果(互斥:一次失败要么重排 failed,要么转死信 dead)。
const (
	DeliveryDelivered = "delivered"
	DeliveryFailed    = "failed"
	DeliveryDead      = "dead"
)

// FlowObserver 关键路径埋点端口。实现必须是事务外的纯内存计数,不得影响业务行为。
type FlowObserver interface {
	// DeliveryResult 记录一次 webhook 投递的结果与耗时(秒)。
	DeliveryResult(outcome string, seconds float64)
	// ConfirmOutcome 记录一次支付确认的结果(paid/paid_after_closed/duplicate/anomaly)。
	ConfirmOutcome(outcome string)
	// ClosingSettled 记录一次订单 CLOSING→CLOSED 落定的耗时(秒);via 为 ClosingVia*。
	ClosingSettled(via string, seconds float64)
	// PrepareRejected 记录一次调起支付被拒;reason 为 PrepareReject*。
	PrepareRejected(reason string)
}

// CLOSING 落定途径。
const (
	ClosingViaQuery    = "query"    // 查单 worker 确认渠道侧已关闭/不存在
	ClosingViaWatchdog = "watchdog" // 滞留兜底(无活跃尝试)
)

// 调起支付被拒原因。
const (
	PrepareRejectClosing         = "closing"          // 活跃尝试关单确认中
	PrepareRejectForeignInflight = "foreign_inflight" // 异渠道尝试提交结果未定
	PrepareRejectCloseFailed     = "close_failed"     // 换渠道时渠道关单失败
	PrepareRejectPayWindow       = "pay_window"       // 订单剩余支付窗口不足
)

// Nop 空实现。
type Nop struct{}

func (Nop) DeliveryResult(string, float64) {}
func (Nop) ConfirmOutcome(string)          {}
func (Nop) ClosingSettled(string, float64) {}
func (Nop) PrepareRejected(string)         {}
