// Package worker 实现后台 worker:Outbox 事件投递(DESIGN §12,租约+退避+死信告警)与
// 支付查单收敛(§9,见 query.go)。对账 worker(§14)仍为后续。
package worker

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/gtkit/logger"
	"github.com/gtkit/orderd/internal/models"
	"github.com/gtkit/orderd/internal/pkg/webhook"
	"github.com/gtkit/orderd/internal/shared/alert"
	"github.com/gtkit/orderd/internal/shared/endpoint"
	"github.com/gtkit/orderd/internal/shared/secret"
	"github.com/gtkit/orderd/internal/store"
)

// Config 投递 worker 调优参数。
type Config struct {
	WorkerID     string        // 副本实例标识(租约持有者)
	BatchSize    int           // 每轮认领上限
	LeaseTTL     time.Duration // 租约时长
	MaxAttempts  int32         // 超过此次数转死信
	PollInterval time.Duration // 轮询间隔
	BaseBackoff  time.Duration // 退避基数
	MaxBackoff   time.Duration // 退避上限
}

func (c *Config) withDefaults() {
	if c.WorkerID == "" {
		c.WorkerID = "worker-1"
	}
	if c.BatchSize == 0 {
		c.BatchSize = 50
	}
	if c.LeaseTTL == 0 {
		c.LeaseTTL = 30 * time.Second
	}
	if c.MaxAttempts == 0 {
		c.MaxAttempts = 8
	}
	if c.PollInterval == 0 {
		c.PollInterval = 2 * time.Second
	}
	if c.BaseBackoff == 0 {
		c.BaseBackoff = 5 * time.Second
	}
	if c.MaxBackoff == 0 {
		c.MaxBackoff = 10 * time.Minute
	}
}

// Delivery 是投递逻辑核心(可 RunOnce 单测)。
type Delivery struct {
	store     store.Store
	endpoints endpoint.Resolver
	secrets   secret.Resolver
	sender    *webhook.Sender
	alerter   alert.Alerter
	cfg       Config
	clock     func() time.Time
}

// NewDelivery 构造投递逻辑。
func NewDelivery(st store.Store, ep endpoint.Resolver, sec secret.Resolver, sender *webhook.Sender, alerter alert.Alerter, cfg Config, clock func() time.Time) *Delivery {
	cfg.withDefaults()
	if clock == nil {
		clock = time.Now
	}
	return &Delivery{store: st, endpoints: ep, secrets: sec, sender: sender, alerter: alerter, cfg: cfg, clock: clock}
}

// RunOnce 认领一批并逐条投递;返回本轮成功/失败数。
func (d *Delivery) RunOnce(ctx context.Context) (delivered, failed int, err error) {
	now := d.clock()
	var batch []*models.EventOutbox
	err = d.store.WithinTx(ctx, func(ctx context.Context, r store.Repos) error {
		var e error
		batch, e = r.Outbox().ClaimDue(ctx, d.cfg.WorkerID, now, now.Add(d.cfg.LeaseTTL), d.cfg.BatchSize)
		return e
	})
	if err != nil {
		return 0, 0, err
	}
	for _, ob := range batch {
		ok, bkErr := d.deliverOne(ctx, ob)
		if bkErr != nil {
			logger.Warnw("Outbox 记账失败", "event_id", ob.EventID, "err", bkErr)
			failed++
			continue
		}
		if ok {
			delivered++
		} else {
			failed++
		}
	}
	return delivered, failed, nil
}

// deliverOne 投递一条并落地结果;返回(是否投递成功, 记账错误)。
func (d *Delivery) deliverOne(ctx context.Context, ob *models.EventOutbox) (bool, error) {
	sendErr := d.trySend(ctx, ob)
	if sendErr == nil {
		return true, d.store.WithinTx(ctx, func(ctx context.Context, r store.Repos) error {
			return r.Outbox().MarkDelivered(ctx, ob.ID)
		})
	}
	attempts := ob.Attempts + 1
	if attempts >= d.cfg.MaxAttempts {
		markErr := d.store.WithinTx(ctx, func(ctx context.Context, r store.Repos) error {
			return r.Outbox().MarkDead(ctx, ob.ID, sendErr.Error())
		})
		d.alerter.Anomaly(ctx, "outbox_dead_letter", map[string]any{
			"event_id": ob.EventID, "app_id": ob.AppID, "order_no": ob.OrderNo,
			"attempts": attempts, "last_error": sendErr.Error(),
		})
		logger.Errorw("Outbox 事件转死信", "event_id", ob.EventID, "attempts", attempts, "err", sendErr)
		return false, markErr
	}
	next := d.clock().Add(d.backoff(attempts))
	return false, d.store.WithinTx(ctx, func(ctx context.Context, r store.Repos) error {
		return r.Outbox().Reschedule(ctx, ob.ID, next, attempts, sendErr.Error())
	})
}

// trySend 解析端点+密钥并发起投递。
func (d *Delivery) trySend(ctx context.Context, ob *models.EventOutbox) error {
	ep, err := d.endpoints.Resolve(ctx, ob.AppID)
	if err != nil {
		return err
	}
	sec, err := d.secrets.Resolve(ctx, ep.HMACSecretRef)
	if err != nil {
		return err
	}
	return d.sender.Send(ctx, webhook.Message{
		URL: ep.WebhookURL, KeyID: ep.KeyID, Secret: sec,
		EventID: ob.EventID, EventType: ob.EventType, Body: ob.Payload,
	})
}

// backoff 指数退避 + 抖动(见 jitterBackoff)。
func (d *Delivery) backoff(attempts int32) time.Duration {
	return jitterBackoff(d.cfg.BaseBackoff, d.cfg.MaxBackoff, attempts)
}

// Set 是可启动的后台 worker 集合(任一为 nil 则不启动)。
type Set struct {
	Delivery    *Delivery
	Query       *QueryWorker
	RefundQuery *RefundQueryWorker
	Timeout     *TimeoutWorker
	Reconcile   *ReconcileWorker
}

// Workers 聚合后台 worker 的启停(供 bootstrap)。
type Workers struct {
	set    Set
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// New 构造 workers。
func New(set Set) *Workers { return &Workers{set: set} }

// Start 启动各 worker 循环。
func (w *Workers) Start(ctx context.Context) {
	runCtx, cancel := context.WithCancel(ctx)
	w.cancel = cancel
	s := w.set
	if s.Delivery != nil {
		w.spawn(runCtx, s.Delivery.cfg.PollInterval, "outbox 投递", func(c context.Context) error {
			_, _, err := s.Delivery.RunOnce(c)
			return err
		})
	}
	if s.Query != nil {
		w.spawn(runCtx, s.Query.cfg.PollInterval, "支付查单", func(c context.Context) error {
			_, err := s.Query.RunOnce(c)
			return err
		})
	}
	if s.RefundQuery != nil {
		w.spawn(runCtx, s.RefundQuery.cfg.PollInterval, "退款查单", func(c context.Context) error {
			_, err := s.RefundQuery.RunOnce(c)
			return err
		})
	}
	if s.Timeout != nil {
		w.spawn(runCtx, s.Timeout.cfg.PollInterval, "超时关单", func(c context.Context) error {
			_, err := s.Timeout.RunOnce(c)
			return err
		})
	}
	if s.Reconcile != nil {
		w.spawn(runCtx, s.Reconcile.interval, "对账", func(c context.Context) error {
			_, err := s.Reconcile.RunOnce(c)
			return err
		})
	}
	logger.Info("event/worker: 后台 worker 已启动")
}

func (w *Workers) spawn(ctx context.Context, interval time.Duration, name string, run func(context.Context) error) {
	w.wg.Go(func() {
		// 初始相位抖动(0~interval):让多副本 ticker 错开,避免同一时刻集体打库(thundering herd)。
		// 不改变轮询间隔,故不影响投递/查单/关单的时效;仅平摊瞬时负载。
		if interval > 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Duration(rand.Int64N(int64(interval)))):
			}
		}
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if err := run(ctx); err != nil && !errors.Is(err, context.Canceled) {
					logger.Warnw("worker 轮次出错", "worker", name, "err", err)
				}
			}
		}
	})
}

// Stop 停止并等待所有循环退出。
func (w *Workers) Stop() {
	if w.cancel != nil {
		w.cancel()
		w.wg.Wait()
	}
}
