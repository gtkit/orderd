package worker

import (
	"context"
	"database/sql"
	"math/rand/v2"
	"time"

	"github.com/gtkit/logger"
	"github.com/gtkit/orderd/internal/models"
	paymentapp "github.com/gtkit/orderd/internal/module/payment/application"
	"github.com/gtkit/orderd/internal/paygateway"
	"github.com/gtkit/orderd/internal/store"
)

// QueryConfig 查单 worker 调优参数。
type QueryConfig struct {
	WorkerID       string
	PollInterval   time.Duration
	BatchSize      int
	LeaseTTL       time.Duration
	MaxQueries     int32 // UNKNOWN 连续"不存在"达此次数后才关闭尝试(§9:不能查不到就换号)
	BaseBackoff    time.Duration
	MaxBackoff     time.Duration
	GatewayTimeout time.Duration // 单次网关调用强制超时(评审 P1)
}

func (c *QueryConfig) withDefaults() {
	if c.WorkerID == "" {
		c.WorkerID = "query-1"
	}
	if c.PollInterval == 0 {
		c.PollInterval = 5 * time.Second
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
		c.BaseBackoff = 10 * time.Second
	}
	if c.GatewayTimeout == 0 {
		c.GatewayTimeout = 10 * time.Second
	}
	if c.MaxBackoff == 0 {
		c.MaxBackoff = 5 * time.Minute
	}
}

// QueryWorker 收敛"结果未知"支付尝试(UNKNOWN/CLOSING):向网关查单并落地状态(§9)。
type QueryWorker struct {
	store   store.Store
	router  paygateway.Router
	payment *paymentapp.Service
	cfg     QueryConfig
	clock   func() time.Time
}

// NewQueryWorker 构造查单 worker。
func NewQueryWorker(st store.Store, router paygateway.Router, pay *paymentapp.Service, cfg QueryConfig, clock func() time.Time) *QueryWorker {
	cfg.withDefaults()
	if clock == nil {
		clock = time.Now
	}
	return &QueryWorker{store: st, router: router, payment: pay, cfg: cfg, clock: clock}
}

// RunOnce 认领一批到期尝试并逐个查单收敛;返回处理条数。
func (q *QueryWorker) RunOnce(ctx context.Context) (int, error) {
	now := q.clock()
	var batch []*models.PaymentAttempt
	err := q.store.WithinTx(ctx, func(ctx context.Context, r store.Repos) error {
		var e error
		batch, e = r.Attempts().ClaimQueryDue(ctx, q.cfg.WorkerID, now, now.Add(q.cfg.LeaseTTL), q.cfg.BatchSize)
		return e
	})
	if err != nil {
		return 0, err
	}
	for _, a := range batch {
		q.handle(ctx, a)
	}
	return len(batch), nil
}

func (q *QueryWorker) handle(ctx context.Context, a *models.PaymentAttempt) {
	gw, err := q.router.Resolve(a.Channel, a.PayAccountID)
	if err != nil {
		q.reschedule(ctx, a)
		return
	}
	gctx, cancel := context.WithTimeout(ctx, q.cfg.GatewayTimeout)
	defer cancel()
	qr, err := gw.Query(gctx, paygateway.QueryInput{Channel: a.Channel, PayAccountID: a.PayAccountID, OutTradeNo: a.OutTradeNo})
	if err != nil {
		q.reschedule(ctx, a)
		return
	}
	switch qr.State {
	case paygateway.QueryPaid:
		// 查单=已付 → §7 支付成功(UNKNOWN→PAID / CLOSING→PAID 恢复)。终态,无需再调度。
		if _, cerr := q.payment.ConfirmPayFromQuery(ctx, a, qr); cerr != nil {
			logger.Warnw("查单确认支付失败", "out_trade_no", a.OutTradeNo, "err", cerr)
			q.reschedule(ctx, a)
		}
	case paygateway.QueryNotPay:
		if a.Status == models.AttemptUnknown {
			_ = q.payment.ApplyQueryNotPaid(ctx, a.OutTradeNo) // →READY,交回调驱动
		} else { // CLOSING:网关仍有开放单 → 关单后继续观察
			_ = gw.Close(gctx, paygateway.CloseInput{Channel: a.Channel, PayAccountID: a.PayAccountID, OutTradeNo: a.OutTradeNo})
			q.reschedule(ctx, a)
		}
	case paygateway.QueryClosed:
		_ = q.payment.CloseAttemptAndMaybeOrder(ctx, a.OutTradeNo)
	case paygateway.QueryNotExist:
		if a.Status == models.AttemptClosing || a.QueryCount+1 >= q.cfg.MaxQueries {
			// CLOSING 确证不存在,或 UNKNOWN 多次查证仍不存在 → 关闭(§9)。
			_ = q.payment.CloseAttemptAndMaybeOrder(ctx, a.OutTradeNo)
		} else {
			q.reschedule(ctx, a) // UNKNOWN 未达阈值,继续退避观察
		}
	default: // QueryError
		q.reschedule(ctx, a)
	}
}

// reschedule 保持当前查单状态,设下次查单时间(退避)+ query_count++ + 清租约。
// 仅用于状态未被应用层改变的分支(a.Status 仍为 UNKNOWN/CLOSING),避免回写覆盖。
func (q *QueryWorker) reschedule(ctx context.Context, a *models.PaymentAttempt) {
	a.QueryCount++
	a.NextQueryAt = sql.NullTime{Time: q.clock().Add(jitterBackoff(q.cfg.BaseBackoff, q.cfg.MaxBackoff, a.QueryCount)), Valid: true}
	a.LockedBy = ""
	a.LockedUntil = sql.NullTime{}
	if err := q.store.WithinTx(ctx, func(ctx context.Context, r store.Repos) error {
		return r.Attempts().Update(ctx, a)
	}); err != nil {
		logger.Warnw("查单重排失败", "out_trade_no", a.OutTradeNo, "err", err)
	}
}

// jitterBackoff 指数退避 + [50%,100%] 抖动,上限 maxD。
func jitterBackoff(base, maxD time.Duration, step int32) time.Duration {
	d := base
	for i := int32(1); i < step && d < maxD; i++ {
		d *= 2
	}
	if d > maxD {
		d = maxD
	}
	return d/2 + time.Duration(rand.Int64N(int64(d)/2+1))
}
