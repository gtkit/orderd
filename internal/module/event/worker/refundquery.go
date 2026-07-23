package worker

import (
	"context"
	"database/sql"
	"time"

	"github.com/gtkit/logger"
	"github.com/gtkit/orderd/internal/models"
	refundapp "github.com/gtkit/orderd/internal/module/refund/application"
	"github.com/gtkit/orderd/internal/store"
)

// RefundQueryConfig 退款查单 worker 参数。
type RefundQueryConfig struct {
	WorkerID     string
	PollInterval time.Duration
	BatchSize    int
	LeaseTTL     time.Duration
	BaseBackoff  time.Duration
	MaxBackoff   time.Duration
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
	if c.BaseBackoff == 0 {
		c.BaseBackoff = 15 * time.Second
	}
	if c.MaxBackoff == 0 {
		c.MaxBackoff = 10 * time.Minute
	}
}

// RefundQueryWorker 认领处理中的退款,向网关查单收敛为成功/失败(DESIGN §10)。
type RefundQueryWorker struct {
	store  store.Store
	refund *refundapp.Service
	cfg    RefundQueryConfig
	clock  func() time.Time
}

// NewRefundQueryWorker 构造退款查单 worker。
func NewRefundQueryWorker(st store.Store, ref *refundapp.Service, cfg RefundQueryConfig, clock func() time.Time) *RefundQueryWorker {
	cfg.withDefaults()
	if clock == nil {
		clock = time.Now
	}
	return &RefundQueryWorker{store: st, refund: ref, cfg: cfg, clock: clock}
}

// RunOnce 认领一批 PROCESSING 退款并查单;仍处理中的按退避重排。
func (w *RefundQueryWorker) RunOnce(ctx context.Context) (int, error) {
	now := w.clock()
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

// rescheduleIfProcessing 若退款仍 PROCESSING,设下次查单时间 + query_count++ + 清租约。
func (w *RefundQueryWorker) rescheduleIfProcessing(ctx context.Context, refundNo string, prevCount int32) {
	err := w.store.WithinTx(ctx, func(ctx context.Context, r store.Repos) error {
		rf, err := r.Refunds().GetByRefundNo(ctx, refundNo)
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
		return r.Refunds().Update(ctx, rf)
	})
	if err != nil {
		logger.Warnw("退款查单重排失败", "refund_no", refundNo, "err", err)
	}
}
