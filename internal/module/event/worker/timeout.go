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
	// StalledClosingAfter:CLOSING 停留超过此时长且无活跃尝试 → 兜底落定 CLOSED(默认 30m)。
	StalledClosingAfter time.Duration
	// ExpireCloseBudget:每轮超时关单的时间预算(默认 20s,小于 PollInterval,轮次不重叠)。
	ExpireCloseBudget time.Duration
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
	if c.StalledClosingAfter == 0 {
		c.StalledClosingAfter = 30 * time.Minute
	}
	if c.ExpireCloseBudget == 0 {
		c.ExpireCloseBudget = 20 * time.Second
	}
}

// TimeoutWorker 兜底扫描(评审 P0):过期 PENDING 订单关单、卡死 REQUESTING 尝试转 UNKNOWN。
// 超时关单按行认领(FOR UPDATE SKIP LOCKED),多副本各取不同订单;其余两轮靠行锁 + 状态复核(幂等)。
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

	// 第一轮:认领式超时关单,持续到无可认领 / 时间预算用尽 / ctx 取消。预算按真实流逝时间计(不用注入时钟)。
	// 本轮失败的订单加入排除集合:按 expire_at 排序时它总排在最前,不排除会反复卡住后续订单。
	started := time.Now()
	var failed []uint64
	for ctx.Err() == nil && time.Since(started) < w.cfg.ExpireCloseBudget {
		res, cerr := w.order.CloseNextExpired(ctx, failed)
		if cerr != nil {
			if res.OrderID == 0 {
				return n, cerr // 认领本身失败(DB 不可用等):本轮终止
			}
			logger.Warnw("超时关单失败", "order_no", res.OrderNo, "err", cerr)
			failed = append(failed, res.OrderID)
			continue
		}
		if res.OrderID == 0 {
			break
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

	// 第三轮:CLOSING 滞留兜底(与生产路径解耦;有活跃尝试的单在应用层复核后跳过)。
	stalled, err := w.store.Reader().Orders().ListStalledClosing(ctx, now.Add(-w.cfg.StalledClosingAfter), w.cfg.BatchSize)
	if err != nil {
		return n, err
	}
	for _, o := range stalled {
		closed, cerr := w.order.CloseStalledClosing(ctx, o.AppID, o.OrderNo)
		if cerr != nil {
			logger.Warnw("CLOSING 滞留兜底失败", "order_no", o.OrderNo, "err", cerr)
			continue
		}
		if closed {
			n++
		}
	}
	return n, nil
}
