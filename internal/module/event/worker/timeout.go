package worker

import (
	"context"
	"time"

	"github.com/gtkit/logger"
	orderapp "github.com/gtkit/orderd/internal/module/order/application"
	paymentapp "github.com/gtkit/orderd/internal/module/payment/application"
	"github.com/gtkit/orderd/internal/store"
)

// TimeoutConfig 超时 watchdog 参数。
type TimeoutConfig struct {
	WorkerID     string
	PollInterval time.Duration
	BatchSize    int
	// RequestingStuckAfter:REQUESTING 停留超过此时长视为卡死 → UNKNOWN(交查单收敛)。
	RequestingStuckAfter time.Duration
}

func (c *TimeoutConfig) withDefaults() {
	if c.WorkerID == "" {
		c.WorkerID = "timeout-1"
	}
	if c.PollInterval == 0 {
		c.PollInterval = 30 * time.Second
	}
	if c.BatchSize == 0 {
		c.BatchSize = 100
	}
	if c.RequestingStuckAfter == 0 {
		c.RequestingStuckAfter = 2 * time.Minute
	}
}

// TimeoutWorker 兜底扫描(评审 P0):过期 PENDING 订单关单、卡死 REQUESTING 尝试转 UNKNOWN。
// 多副本安全靠行锁 + 状态复核(幂等),无需租约。
type TimeoutWorker struct {
	store   store.Store
	order   *orderapp.Service
	payment *paymentapp.Service
	cfg     TimeoutConfig
	clock   func() time.Time
}

// NewTimeoutWorker 构造超时 watchdog。
func NewTimeoutWorker(st store.Store, ord *orderapp.Service, pay *paymentapp.Service, cfg TimeoutConfig, clock func() time.Time) *TimeoutWorker {
	cfg.withDefaults()
	if clock == nil {
		clock = time.Now
	}
	return &TimeoutWorker{store: st, order: ord, payment: pay, cfg: cfg, clock: clock}
}

// RunOnce 扫一轮:关过期订单 + 恢复卡死尝试;返回处理条数。
func (w *TimeoutWorker) RunOnce(ctx context.Context) (int, error) {
	now := w.clock()
	n := 0

	orders, err := w.store.Reader().Orders().ListExpiredPending(ctx, now, w.cfg.BatchSize)
	if err != nil {
		return n, err
	}
	for _, o := range orders {
		if _, cerr := w.order.CloseIfExpired(ctx, o.AppID, o.OrderNo); cerr != nil {
			logger.Warnw("超时关单失败", "order_no", o.OrderNo, "err", cerr)
			continue
		}
		n++
	}

	stuck, err := w.store.Reader().Attempts().ListStuckRequesting(ctx, now.Add(-w.cfg.RequestingStuckAfter), w.cfg.BatchSize)
	if err != nil {
		return n, err
	}
	for _, a := range stuck {
		if rerr := w.payment.RecoverStuckRequesting(ctx, a.OutTradeNo); rerr != nil {
			logger.Warnw("恢复卡死尝试失败", "out_trade_no", a.OutTradeNo, "err", rerr)
			continue
		}
		n++
	}
	return n, nil
}
