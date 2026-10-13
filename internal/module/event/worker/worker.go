// Package worker 实现后台 worker:Outbox 事件投递(DESIGN §12,租约+退避+死信告警)与
// 支付查单收敛(§9,见 query.go)和对账恢复调度(§14,见 reconcile.go)。
package worker

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/gtkit/logger"
	"github.com/gtkit/orderd/internal/models"
	"github.com/gtkit/orderd/internal/pkg/idgen"
	"github.com/gtkit/orderd/internal/pkg/webhook"
	"github.com/gtkit/orderd/internal/shared/alert"
	"github.com/gtkit/orderd/internal/shared/endpoint"
	"github.com/gtkit/orderd/internal/shared/obs"
	"github.com/gtkit/orderd/internal/shared/secret"
	"github.com/gtkit/orderd/internal/store"
)

// Config 投递 worker 调优参数。
type Config struct {
	WorkerID     string        // 保留实例标识兼容既有装配;租约隔离使用每次认领的新 token
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
	obs       obs.FlowObserver
	cfg       Config
	clock     func() time.Time
}

// NewDelivery 构造投递逻辑。ob 为关键路径埋点端口(nil → Nop,行为不变)。
func NewDelivery(st store.Store, ep endpoint.Resolver, sec secret.Resolver, sender *webhook.Sender, alerter alert.Alerter, ob obs.FlowObserver, cfg Config, clock func() time.Time) *Delivery {
	cfg.withDefaults()
	if clock == nil {
		clock = time.Now
	}
	if ob == nil {
		ob = obs.Nop{}
	}
	return &Delivery{store: st, endpoints: ep, secrets: sec, sender: sender, alerter: alerter, obs: ob, cfg: cfg, clock: clock}
}

// RunOnce 逐条认领并立即投递;一轮最多 BatchSize 条,不让等待队列消耗租约。
func (d *Delivery) RunOnce(ctx context.Context) (delivered, failed int, err error) {
	for range d.cfg.BatchSize {
		if err := ctx.Err(); err != nil {
			return delivered, failed, err
		}
		token, err := idgen.UUIDv4()
		if err != nil {
			return delivered, failed, err
		}
		now := d.clock()
		var batch []*models.EventOutbox
		if err := d.store.WithinTx(ctx, func(ctx context.Context, r store.Repos) error {
			var e error
			batch, e = r.Outbox().ClaimDue(ctx, token, now, now.Add(d.cfg.LeaseTTL), 1)
			return e
		}); err != nil {
			return delivered, failed, err
		}
		if len(batch) == 0 {
			break
		}
		ob := batch[0]
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
// 埋点(事务外纯内存计数):delivered/failed/dead 互斥,耗时含端点响应。
func (d *Delivery) deliverOne(ctx context.Context, ob *models.EventOutbox) (bool, error) {
	started := time.Now()
	sendErr := d.trySend(ctx, ob)
	elapsed := time.Since(started).Seconds()
	if sendErr == nil {
		d.obs.DeliveryResult(obs.DeliveryDelivered, elapsed)
		return true, d.store.WithinTx(ctx, func(ctx context.Context, r store.Repos) error {
			return r.Outbox().MarkDelivered(ctx, ob.ID)
		})
	}
	attempts := ob.Attempts + 1
	if attempts >= d.cfg.MaxAttempts {
		var applied bool
		markErr := d.store.WithinTx(ctx, func(ctx context.Context, r store.Repos) error {
			var err error
			applied, err = r.Outbox().MarkDead(ctx, ob.ID, ob.LockedBy, attempts, sendErr.Error())
			return err
		})
		if markErr != nil || !applied {
			return false, markErr
		}
		d.obs.DeliveryResult(obs.DeliveryDead, elapsed)
		d.alerter.Anomaly(ctx, "outbox_dead_letter", map[string]any{
			"event_id": ob.EventID, "app_id": ob.AppID, "order_no": ob.OrderNo,
			"attempts": attempts, "last_error": sendErr.Error(),
		})
		logger.Errorw("Outbox 事件转死信", "event_id", ob.EventID, "attempts", attempts, "err", sendErr)
		return false, markErr
	}
	next := d.clock().Add(d.backoff(attempts))
	var applied bool
	err := d.store.WithinTx(ctx, func(ctx context.Context, r store.Repos) error {
		var err error
		applied, err = r.Outbox().Reschedule(ctx, ob.ID, ob.LockedBy, next, attempts, sendErr.Error())
		return err
	})
	if err == nil && applied {
		d.obs.DeliveryResult(obs.DeliveryFailed, elapsed)
	}
	return false, err
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
	Delivery          *Delivery
	Query             *QueryWorker
	RefundQuery       *RefundQueryWorker
	Timeout           *TimeoutWorker
	Reconcile         *ReconcileWorker
	DeadLetterRequeue *DeadLetterRequeueWorker
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
	if s.DeadLetterRequeue != nil {
		w.spawn(runCtx, s.DeadLetterRequeue.cfg.Interval, "死信兜底重放", func(c context.Context) error {
			_, err := s.DeadLetterRequeue.RunOnce(c)
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
