package worker

import (
	"context"
	"database/sql"
	"time"

	"github.com/gtkit/logger"
	"github.com/gtkit/orderd/internal/models"
	refundapp "github.com/gtkit/orderd/internal/module/refund/application"
	"github.com/gtkit/orderd/internal/shared/alert"
	"github.com/gtkit/orderd/internal/store"
)

// RefundQueryConfig 退款查单 worker 参数。
type RefundQueryConfig struct {
	WorkerID     string
	PollInterval time.Duration
	BatchSize    int
	LeaseTTL     time.Duration
	MaxQueries   int32 // 退款仍处理中且查询次数达此值时告警一次(滞留可见,状态不变)
	BaseBackoff  time.Duration
	MaxBackoff   time.Duration
	// CreatedStuckAfter:CREATED 停留超过此时长视为滞留(建单事务已提交但网关调用结果丢失,
	// 如进程崩溃/PROCESSING 更新失败),恢复为 PROCESSING 交查单收敛。
	CreatedStuckAfter time.Duration
}

func (c *RefundQueryConfig) withDefaults() {
	if c.WorkerID == "" {
		c.WorkerID = "refundquery-1"
	}
	if c.PollInterval == 0 {
		c.PollInterval = 10 * time.Second
	}
	if c.BatchSize == 0 {
		c.BatchSize = 50
	}
	if c.LeaseTTL == 0 {
		c.LeaseTTL = 30 * time.Second
	}
	if c.MaxQueries == 0 {
		c.MaxQueries = 6
	}
	if c.BaseBackoff == 0 {
		c.BaseBackoff = 15 * time.Second
	}
	if c.MaxBackoff == 0 {
		c.MaxBackoff = 10 * time.Minute
	}
	if c.CreatedStuckAfter == 0 {
		c.CreatedStuckAfter = 2 * time.Minute
	}
}

// RefundQueryWorker 认领处理中的退款,向网关查单收敛为成功/失败(DESIGN §10)。
type RefundQueryWorker struct {
	store   store.Store
	refund  *refundapp.Service
	alerter alert.Alerter
	cfg     RefundQueryConfig
	clock   func() time.Time
}

// NewRefundQueryWorker 构造退款查单 worker。
func NewRefundQueryWorker(st store.Store, ref *refundapp.Service, alerter alert.Alerter, cfg RefundQueryConfig, clock func() time.Time) *RefundQueryWorker {
	cfg.withDefaults()
	if clock == nil {
		clock = time.Now
	}
	return &RefundQueryWorker{store: st, refund: ref, alerter: alerter, cfg: cfg, clock: clock}
}

// RunOnce 认领一批 PROCESSING 退款并查单;仍处理中的按退避重排。
// 认领前先恢复滞留 CREATED 的退款(崩溃/更新失败兜底),使其进入正常收敛链路。
func (w *RefundQueryWorker) RunOnce(ctx context.Context) (int, error) {
	now := w.clock()
	w.recoverStuckCreated(ctx, now)
	var batch []*models.Refund
	err := w.store.WithinTx(ctx, func(ctx context.Context, r store.Repos) error {
		var e error
		batch, e = r.Refunds().ClaimQueryDue(ctx, w.cfg.WorkerID, now, now.Add(w.cfg.LeaseTTL), w.cfg.BatchSize)
		return e
	})
	if err != nil {
		return 0, err
	}
	for _, rf := range batch {
		if qerr := w.refund.QueryRefund(ctx, rf); qerr != nil {
			logger.Warnw("退款查单失败", "refund_no", rf.RefundNo, "err", qerr)
		}
		w.rescheduleIfProcessing(ctx, rf.RefundNo, rf.QueryCount)
	}
	return len(batch), nil
}

// recoverStuckCreated 把滞留 CREATED 的退款恢复为 PROCESSING + 立即查单。
// 经 CREATED→PROCESSING 合法状态边进入正常收敛链路,不从 CREATED 直跳终态;
// 事务内按 refund_no 复核状态,已被并发推进(非 CREATED)的跳过不动。
func (w *RefundQueryWorker) recoverStuckCreated(ctx context.Context, now time.Time) {
	stuck, err := w.store.Reader().Refunds().ListStuckCreated(ctx, now.Add(-w.cfg.CreatedStuckAfter), w.cfg.BatchSize)
	if err != nil {
		logger.Warnw("扫描滞留 CREATED 退款失败", "err", err)
		return
	}
	for _, rf := range stuck {
		rerr := w.store.WithinTx(ctx, func(ctx context.Context, r store.Repos) error {
			cur, err := r.Refunds().GetByRefundNo(ctx, rf.RefundNo, true) // 行锁:读-复核-写原子
			if err != nil {
				return err
			}
			if cur.Status != models.RefundCreated {
				return nil // 已被并发推进,跳过
			}
			cur.Status = models.RefundProcessing
			cur.NextQueryAt = sql.NullTime{Time: now, Valid: true}
			return r.Refunds().Update(ctx, cur)
		})
		if rerr != nil {
			logger.Warnw("恢复滞留 CREATED 退款失败", "refund_no", rf.RefundNo, "err", rerr)
		}
	}
}

// rescheduleIfProcessing 若退款仍 PROCESSING,设下次查单时间 + query_count++ + 清租约。
//
// 查询次数恰好跨过 MaxQueries 时告警一次(边沿触发,不会每轮刷);告警在事务外发出,
// 不把外部 IO 放进持锁期间。达阈值只告警不落终态:钱可能实际已退,本地判失败会造成账实不符,
// 滞留由退款通知或人工介入收敛。
func (w *RefundQueryWorker) rescheduleIfProcessing(ctx context.Context, refundNo string, prevCount int32) {
	var (
		stalled bool
		orderNo string
	)
	err := w.store.WithinTx(ctx, func(ctx context.Context, r store.Repos) error {
		rf, err := r.Refunds().GetByRefundNo(ctx, refundNo, true)
		if err != nil {
			return err
		}
		if rf.Status != models.RefundProcessing {
			return nil // 已终态,无需重排(不会被再次认领)
		}
		rf.QueryCount = prevCount + 1
		rf.NextQueryAt = sql.NullTime{Time: w.clock().Add(jitterBackoff(w.cfg.BaseBackoff, w.cfg.MaxBackoff, rf.QueryCount)), Valid: true}
		rf.LockedBy = ""
		rf.LockedUntil = sql.NullTime{}
		stalled, orderNo = rf.QueryCount == w.cfg.MaxQueries, rf.OrderNo
		return r.Refunds().Update(ctx, rf)
	})
	if err != nil {
		logger.Warnw("退款查单重排失败", "refund_no", refundNo, "err", err)
		return
	}
	if stalled {
		w.alerter.Anomaly(ctx, "refund_query_stalled", map[string]any{
			"refund_no": refundNo, "order_no": orderNo, "query_count": prevCount + 1,
		})
	}
}
