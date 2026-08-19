// Package metrics 暴露积压类 Prometheus 指标(值班的眼睛:趋势与阈值告警的数据源)。
//
// 采集模型:抓取时(scrape-time)对 DB 做只读聚合,全部命中既有索引,单次抓取强制超时;
// 任一查询失败只跳过该指标并递增 scrape_errors_total,绝不影响业务路径、不使 /metrics 5xx。
// 聚合 SQL 为 MySQL 方言,故本包是基础设施型包(仅 runtime/platform 可 import),不走 store 端口。
// label 只用低基数状态枚举,禁 user_id/order_no(observability guide)。
package metrics

import (
	"context"
	"time"

	"github.com/gtkit/logger"
	"github.com/prometheus/client_golang/prometheus"
	"gorm.io/gorm"
)

const scrapeTimeout = 5 * time.Second

var (
	descOutboxBacklog = prometheus.NewDesc("orderd_outbox_backlog",
		"事件发件箱积压行数(pending=待投递,dead=死信)", []string{"status"}, nil)
	descOutboxOldestAge = prometheus.NewDesc("orderd_outbox_oldest_pending_age_seconds",
		"最老待投递事件的应投时间距今秒数(无待投递为 0)", nil, nil)
	descAttemptsConverging = prometheus.NewDesc("orderd_attempts_converging",
		"收敛中的支付尝试数(unknown=结果未知,closing=关单中)", []string{"status"}, nil)
	descAttemptsStuckReq = prometheus.NewDesc("orderd_attempts_stuck_requesting",
		"停留在 REQUESTING 超过 2 分钟的支付尝试数(卡死候选)", nil, nil)
	descRefundsInFlight = prometheus.NewDesc("orderd_refunds_in_flight",
		"在途退款数(created=已建未受理,processing=处理中)", []string{"status"}, nil)
	descOrdersExpiredPending = prometheus.NewDesc("orderd_orders_expired_pending",
		"已过期仍未关单的 PENDING 订单数", nil, nil)
	descAnomalyTxns = prometheus.NewDesc("orderd_anomaly_txns_total",
		"资金异常流水(anomaly=1)表内总量;gauge 语义,观察增长用 delta", nil, nil)
)

// BacklogCollector 实现 prometheus.Collector。
type BacklogCollector struct {
	db           *gorm.DB
	scrapeErrors prometheus.Counter
	now          func() time.Time
}

// NewBacklogCollector 构造采集器(db 只读使用)。
func NewBacklogCollector(db *gorm.DB) *BacklogCollector {
	return &BacklogCollector{
		db: db,
		scrapeErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "orderd_metrics_scrape_errors_total",
			Help: "积压指标采集失败次数(查询错误/超时)",
		}),
		now: time.Now,
	}
}

// Describe 实现 prometheus.Collector。
func (c *BacklogCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- descOutboxBacklog
	ch <- descOutboxOldestAge
	ch <- descAttemptsConverging
	ch <- descAttemptsStuckReq
	ch <- descRefundsInFlight
	ch <- descOrdersExpiredPending
	ch <- descAnomalyTxns
	c.scrapeErrors.Describe(ch)
}

// Collect 实现 prometheus.Collector:逐项聚合,失败跳过并计数。
func (c *BacklogCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), scrapeTimeout)
	defer cancel()
	db := c.db.WithContext(ctx)
	now := c.now()

	c.gaugeCount(ch, db, descOutboxBacklog, "SELECT COUNT(*) FROM event_outbox WHERE status = 0", "pending")
	c.gaugeCount(ch, db, descOutboxBacklog, "SELECT COUNT(*) FROM event_outbox WHERE status = 2", "dead")
	// 最老待投递年龄:无待投递为 0(COALESCE 到 now)。
	c.gaugeQuery(ch, db, descOutboxOldestAge,
		"SELECT COALESCE(TIMESTAMPDIFF(SECOND, MIN(next_attempt_at), ?), 0) FROM event_outbox WHERE status = 0", now)
	c.gaugeCount(ch, db, descAttemptsConverging, "SELECT COUNT(*) FROM payment_attempts WHERE status = 3", "unknown")
	c.gaugeCount(ch, db, descAttemptsConverging, "SELECT COUNT(*) FROM payment_attempts WHERE status = 5", "closing")
	c.gaugeQuery(ch, db, descAttemptsStuckReq,
		"SELECT COUNT(*) FROM payment_attempts WHERE status = 1 AND updated_at < ?", now.Add(-2*time.Minute))
	c.gaugeCount(ch, db, descRefundsInFlight, "SELECT COUNT(*) FROM refunds WHERE status = 0", "created")
	c.gaugeCount(ch, db, descRefundsInFlight, "SELECT COUNT(*) FROM refunds WHERE status = 1", "processing")
	c.gaugeQuery(ch, db, descOrdersExpiredPending,
		"SELECT COUNT(*) FROM orders WHERE status = 0 AND expire_at < ?", now)
	c.gaugeQuery(ch, db, descAnomalyTxns, "SELECT COUNT(*) FROM payment_transactions WHERE anomaly = 1")

	c.scrapeErrors.Collect(ch)
}

// gaugeCount 单值聚合 → 带 label 的 gauge;失败跳过并计数。
func (c *BacklogCollector) gaugeCount(ch chan<- prometheus.Metric, db *gorm.DB, desc *prometheus.Desc, sql, label string) {
	var v float64
	if err := db.Raw(sql).Scan(&v).Error; err != nil {
		c.scrapeErrors.Inc()
		logger.Warnw("metrics: 聚合查询失败", "metric", desc.String(), "err", err)
		return
	}
	ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, v, label)
}

// gaugeQuery 单值聚合 → 无 label 的 gauge;失败跳过并计数。
func (c *BacklogCollector) gaugeQuery(ch chan<- prometheus.Metric, db *gorm.DB, desc *prometheus.Desc, sql string, args ...any) {
	var v float64
	if err := db.Raw(sql, args...).Scan(&v).Error; err != nil {
		c.scrapeErrors.Inc()
		logger.Warnw("metrics: 聚合查询失败", "metric", desc.String(), "err", err)
		return
	}
	ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, v)
}
