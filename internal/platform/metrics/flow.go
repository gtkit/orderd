package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// FlowMetrics 实现 obs.FlowObserver:关键路径速率/时延指标(label 仅低基数枚举)。
type FlowMetrics struct {
	deliveryTotal   *prometheus.CounterVec
	deliverySeconds prometheus.Histogram
	confirmTotal    *prometheus.CounterVec
}

// NewFlowMetrics 构造并注册到 reg。
func NewFlowMetrics(reg prometheus.Registerer) *FlowMetrics {
	f := &FlowMetrics{
		deliveryTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "orderd_webhook_delivery_total",
			Help: "webhook 投递次数(delivered=成功,failed=失败重排,dead=该次失败转死信;三者互斥)",
		}, []string{"outcome"}),
		deliverySeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "orderd_webhook_delivery_seconds",
			Help:    "单次 webhook 投递耗时(含接入方端点响应)",
			Buckets: prometheus.DefBuckets,
		}),
		confirmTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "orderd_pay_confirm_total",
			Help: "支付确认结果次数(paid/duplicate/anomaly)",
		}, []string{"outcome"}),
	}
	reg.MustRegister(f.deliveryTotal, f.deliverySeconds, f.confirmTotal)
	return f
}

// DeliveryResult 实现 obs.FlowObserver。
func (f *FlowMetrics) DeliveryResult(outcome string, seconds float64) {
	f.deliveryTotal.WithLabelValues(outcome).Inc()
	f.deliverySeconds.Observe(seconds)
}

// ConfirmOutcome 实现 obs.FlowObserver。
func (f *FlowMetrics) ConfirmOutcome(outcome string) {
	f.confirmTotal.WithLabelValues(outcome).Inc()
}
