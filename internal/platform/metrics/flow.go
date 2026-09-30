package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// FlowMetrics 实现 obs.FlowObserver:关键路径速率/时延指标(label 仅低基数枚举)。
type FlowMetrics struct {
	deliveryTotal   *prometheus.CounterVec
	deliverySeconds prometheus.Histogram
	confirmTotal    *prometheus.CounterVec
	closingSeconds  *prometheus.HistogramVec
	prepareRejected *prometheus.CounterVec
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
			Help: "支付确认结果次数(paid/paid_after_closed/duplicate/anomaly;paid_after_closed=结算前订单已 CLOSED 的迟到付款)",
		}, []string{"outcome"}),
		closingSeconds: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "orderd_order_closing_seconds",
			Help:    "订单从进入 CLOSING 到落定 CLOSED 的耗时(via=query 查单确认 / watchdog 滞留兜底)",
			Buckets: []float64{5, 15, 30, 60, 120, 300, 600, 900, 1200, 1800, 3600, 5400},
		}, []string{"via"}),
		prepareRejected: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "orderd_prepare_rejected_total",
			Help: "调起支付被拒次数(closing/foreign_inflight/close_failed/pay_window)",
		}, []string{"reason"}),
	}
	reg.MustRegister(f.deliveryTotal, f.deliverySeconds, f.confirmTotal, f.closingSeconds, f.prepareRejected)
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

// ClosingSettled 实现 obs.FlowObserver。
func (f *FlowMetrics) ClosingSettled(via string, seconds float64) {
	f.closingSeconds.WithLabelValues(via).Observe(seconds)
}

// PrepareRejected 实现 obs.FlowObserver。
func (f *FlowMetrics) PrepareRejected(reason string) {
	f.prepareRejected.WithLabelValues(reason).Inc()
}
