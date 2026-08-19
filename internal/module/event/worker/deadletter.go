package worker

import (
	"context"
	"fmt"
	"time"

	"github.com/gtkit/orderd/internal/models"
	"github.com/gtkit/orderd/internal/store"
)

// DeadLetterRequeueConfig 死信兜底重放 worker 参数(DESIGN 评审 D4)。
type DeadLetterRequeueConfig struct {
	Enabled  bool          // 默认关闭:上线观察死信量级与端点健康后再开
	Interval time.Duration // 轮询间隔(小时级,默认 1h——不是分钟级,避免放大对端点压力)
	Cooldown time.Duration // 冷却:转死信后至少隔此时长才自动重放(默认 1h)
	// MaxAttempts 与投递 worker 的死信阈值同值(Config.MaxAttempts,默认 8);
	// MaxAutoRequeues 为自动重放的追加次数上限。attempts 不重置(一次重放=一次投递机会),
	// 故 attempts < MaxAttempts+MaxAutoRequeues 即为次数上限(单调计数器,免加列)。
	MaxAttempts     int32
	MaxAutoRequeues int32         // 默认 3
	MaxLifetime     time.Duration // 存活上限:created_at 早于 now-MaxLifetime 的永久停手(默认 7d)
	BatchSize       int           // 每轮上限(默认 50)
}

func (c *DeadLetterRequeueConfig) withDefaults() {
	if c.Interval == 0 {
		c.Interval = time.Hour
	}
	if c.Cooldown == 0 {
		c.Cooldown = time.Hour
	}
	if c.MaxAttempts == 0 {
		c.MaxAttempts = 8
	}
	if c.MaxAutoRequeues == 0 {
		c.MaxAutoRequeues = 3
	}
	if c.MaxLifetime == 0 {
		c.MaxLifetime = 7 * 24 * time.Hour
	}
	if c.BatchSize == 0 {
		c.BatchSize = 50
	}
}

// DeadLetterRequeueWorker 定时兜底重放死信:满足冷却 + 次数上限 + 存活上限的死信
// 置回 Pending,与 actor=system 的审计同事务(认领用行锁 SKIP LOCKED,多副本恰好一次)。
// 达任一上限的记录永久跳过(保持 Dead,不重复告警——死信告警在转死信时已发过)。
type DeadLetterRequeueWorker struct {
	store store.Store
	cfg   DeadLetterRequeueConfig
	clock func() time.Time
}

// NewDeadLetterRequeueWorker 构造兜底重放 worker。
func NewDeadLetterRequeueWorker(st store.Store, cfg DeadLetterRequeueConfig, clock func() time.Time) *DeadLetterRequeueWorker {
	cfg.withDefaults()
	if clock == nil {
		clock = time.Now
	}
	return &DeadLetterRequeueWorker{store: st, cfg: cfg, clock: clock}
}

// RunOnce 单事务完成"认领(行锁)→ 逐条 Requeue → 逐条审计";返回重放条数。
// Enabled=false 双保险短路(装配层本就不挂载,此处保证即使被误装配也不动任何记录)。
func (w *DeadLetterRequeueWorker) RunOnce(ctx context.Context) (int, error) {
	if !w.cfg.Enabled {
		return 0, nil
	}
	now := w.clock()
	n := 0
	err := w.store.WithinTx(ctx, func(ctx context.Context, r store.Repos) error {
		batch, err := r.Outbox().ClaimDeadForRequeue(ctx, store.DeadRequeueCond{
			UpdatedBefore: now.Add(-w.cfg.Cooldown),
			AttemptsBelow: w.cfg.MaxAttempts + w.cfg.MaxAutoRequeues,
			CreatedAfter:  now.Add(-w.cfg.MaxLifetime),
		}, w.cfg.BatchSize)
		if err != nil {
			return err
		}
		for _, ob := range batch {
			if err := r.Outbox().Requeue(ctx, ob.ID, now); err != nil {
				return err
			}
			if err := r.OpsAudit().Insert(ctx, &models.OpsAuditLog{
				Actor: models.OpsActorSystem, Action: models.OpsActionDeadLetterRequeue,
				TargetType: models.OpsTargetEventOutbox, TargetID: ob.EventID,
				Detail: fmt.Sprintf("attempts=%d app=%s order=%s auto=true", ob.Attempts, ob.AppID, ob.OrderNo),
			}); err != nil {
				return err
			}
			n++
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return n, nil
}
