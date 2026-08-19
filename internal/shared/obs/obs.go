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
	// ConfirmOutcome 记录一次支付确认的结果(paid/duplicate/anomaly)。
	ConfirmOutcome(outcome string)
}

// Nop 空实现。
type Nop struct{}

func (Nop) DeliveryResult(string, float64) {}
func (Nop) ConfirmOutcome(string)          {}
