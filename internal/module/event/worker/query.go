package worker

import (
	"context"
	"database/sql"
	"math"
	"math/rand/v2"
	"time"

	"github.com/gtkit/logger"
	"github.com/gtkit/orderd/internal/models"
	paymentapp "github.com/gtkit/orderd/internal/module/payment/application"
	"github.com/gtkit/orderd/internal/paygateway"
	"github.com/gtkit/orderd/internal/pkg/idgen"
	"github.com/gtkit/orderd/internal/shared/alert"
	"github.com/gtkit/orderd/internal/store"
)

// QueryConfig 查单 worker 调优参数。
type QueryConfig struct {
	WorkerID     string
	PollInterval time.Duration
	BatchSize    int
	LeaseTTL     time.Duration
	// MaxQueries 为 CLOSING 仍未关闭时的告警阈值；不作为释放支付槽位的证据。
	MaxQueries     int32
	BaseBackoff    time.Duration
	MaxBackoff     time.Duration
	GatewayTimeout time.Duration // 单次网关调用强制超时(评审 P1)
	// ExpireGrace:CLOSING 尝试查得"渠道无此单"时,商户单截止后再宽限此时长才确认关闭(吸收时钟偏差)。
	ExpireGrace time.Duration
}

func (c *QueryConfig) withDefaults() {
	if c.WorkerID == "" {
		c.WorkerID = "query-1"
	}
	if c.PollInterval <= 0 {
		c.PollInterval = 5 * time.Second
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 50
	}
	if c.LeaseTTL <= 0 {
		c.LeaseTTL = 30 * time.Second
	}
	if c.MaxQueries <= 0 {
		c.MaxQueries = 6
	}
	if c.BaseBackoff <= 0 {
		c.BaseBackoff = 10 * time.Second
	}
	if c.GatewayTimeout <= 0 {
		c.GatewayTimeout = 10 * time.Second
	}
	if c.MaxBackoff <= 0 {
		c.MaxBackoff = 5 * time.Minute
	}
	if c.ExpireGrace <= 0 {
		c.ExpireGrace = 2 * time.Minute
	}
}

// QueryWorker 收敛"结果未知"支付尝试(READY/UNKNOWN/CLOSING):向网关查单并落地状态(§9)。
type QueryWorker struct {
	store   store.Store
	router  paygateway.Router
	payment *paymentapp.Service
	alerter alert.Alerter
	cfg     QueryConfig
	clock   func() time.Time
}

// NewQueryWorker 构造查单 worker。
func NewQueryWorker(st store.Store, router paygateway.Router, pay *paymentapp.Service, alerter alert.Alerter, cfg QueryConfig, clock func() time.Time) *QueryWorker {
	cfg.withDefaults()
	if clock == nil {
		clock = time.Now
	}
	return &QueryWorker{store: st, router: router, payment: pay, alerter: alerter, cfg: cfg, clock: clock}
}

// RunOnce 认领一批到期尝试并逐个查单收敛;返回处理条数。
func (q *QueryWorker) RunOnce(ctx context.Context) (int, error) {
	processed := 0
	for range q.cfg.BatchSize {
		if err := ctx.Err(); err != nil {
			return processed, err
		}
		token, err := idgen.UUIDv4()
		if err != nil {
			return processed, err
		}
		now := q.clock()
		var batch []*models.PaymentAttempt
		err = q.store.WithinTx(ctx, func(ctx context.Context, r store.Repos) error {
			var e error
			batch, e = r.Attempts().ClaimQueryDue(ctx, token, now, now.Add(q.cfg.LeaseTTL), 1)
			return e
		})
		if err != nil {
			return processed, err
		}
		if len(batch) == 0 {
			break
		}
		q.handle(ctx, batch[0])
		processed++
	}
	return processed, nil
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
		// 查单=已付 → §7 支付成功(READY/UNKNOWN/CLOSING→PAID)。终态,无需再调度。
		if _, cerr := q.payment.ConfirmPayFromQuery(ctx, a, qr); cerr != nil {
			logger.Warnw("查单确认支付失败", "out_trade_no", a.OutTradeNo, "err", cerr)
			q.reschedule(ctx, a)
		}
	case paygateway.QueryNotPay:
		closing := a.Status == models.AttemptClosing || !q.clock().Before(a.ExpireAt)
		if closing {
			closeCtx, closeCancel := context.WithTimeout(ctx, q.cfg.GatewayTimeout)
			cerr := gw.Close(closeCtx, paygateway.CloseInput{Channel: a.Channel, PayAccountID: a.PayAccountID, OutTradeNo: a.OutTradeNo})
			closeCancel()
			if cerr != nil {
				logger.Warnw("关单失败", "out_trade_no", a.OutTradeNo, "err", cerr)
			}
		} else if err := q.payment.ApplyQueryNotPaid(ctx, a.OutTradeNo, a.LockedBy); err != nil {
			logger.Warnw("未支付结果写回失败", "out_trade_no", a.OutTradeNo, "err", err)
		}
		if count, advanced := q.reschedule(ctx, a); closing && advanced {
			q.alertIfCloseStalled(ctx, a, count)
		}
	case paygateway.QueryClosed:
		if err := q.payment.CloseAttemptAndMaybeOrder(ctx, a.OutTradeNo, a.LockedBy); err != nil {
			logger.Warnw("关闭结果写回失败", "out_trade_no", a.OutTradeNo, "err", err)
			q.reschedule(ctx, a)
		}
	case paygateway.QueryNotExist:
		// 预下单可能直到扫码才可查询；任何次数都不能否定有效支付码。
		if q.clock().Before(a.ExpireAt.Add(q.cfg.ExpireGrace)) {
			q.reschedule(ctx, a)
		} else if err := q.payment.CloseAttemptAndMaybeOrder(ctx, a.OutTradeNo, a.LockedBy); err != nil {
			logger.Warnw("到期结果写回失败", "out_trade_no", a.OutTradeNo, "err", err)
			q.reschedule(ctx, a)
		}
	default: // QueryError
		q.reschedule(ctx, a)
	}
}

// alertIfCloseStalled 仅在当前认领的重排已提交、查询次数实际增长到 MaxQueries 时告警。
// count 来自事务内重读后的新值;旧租约、写回失败和计数饱和均不触发。
//
// 达阈值只告警,不落终态。滞留仍由渠道查询确证关闭或后续真实付款确认收敛。
func (q *QueryWorker) alertIfCloseStalled(ctx context.Context, a *models.PaymentAttempt, count int32) {
	if count != q.cfg.MaxQueries {
		return
	}
	q.alerter.Anomaly(ctx, "attempt_close_stalled", map[string]any{
		"channel": a.Channel, "out_trade_no": a.OutTradeNo, "query_count": count,
	})
}

// reschedule 设下次查单时间(退避)+ query_count++ + 清租约;返回提交后的计数及是否实际增长。
// 事务内按行锁重读并复核状态:仅当尝试仍为 READY/UNKNOWN/CLOSING 才重排;已被应用路径推进
// (取消→CLOSING→…、回调→PAID)时直接放弃,绝不回写认领时的陈旧状态——认领快照到重排之间
// 隔着最长 GatewayTimeout 的网关调用,租约只互斥其他 worker,不互斥应用路径写。
func (q *QueryWorker) reschedule(ctx context.Context, a *models.PaymentAttempt) (count int32, advanced bool) {
	if err := q.store.WithinTx(ctx, func(ctx context.Context, r store.Repos) error {
		cur, err := r.Attempts().GetByOutTradeNo(ctx, a.OutTradeNo, true) // 行锁:读-复核-写原子
		if err != nil || cur == nil {
			return err
		}
		if cur.LockedBy != a.LockedBy || (cur.Status != models.AttemptReady && cur.Status != models.AttemptUnknown && cur.Status != models.AttemptClosing) {
			return nil // 已被推进到收敛集合之外(PAID/CLOSED),放弃重排
		}
		// 仍在收敛集合(READY/UNKNOWN,或期间被取消/关单路径推进为 CLOSING):状态取重读值不回写,
		// 调度字段为 worker 独占,基于重读计数推进。
		if cur.QueryCount < math.MaxInt32 {
			cur.QueryCount++
			advanced = true
		}
		count = cur.QueryCount
		cur.NextQueryAt = sql.NullTime{Time: q.clock().Add(jitterBackoff(q.cfg.BaseBackoff, q.cfg.MaxBackoff, cur.QueryCount)), Valid: true}
		cur.LockedBy = ""
		cur.LockedUntil = sql.NullTime{}
		return r.Attempts().Update(ctx, cur)
	}); err != nil {
		logger.Warnw("查单重排失败", "out_trade_no", a.OutTradeNo, "err", err)
		return 0, false
	}
	return count, advanced
}

// jitterBackoff 指数退避 + [50%,100%] 抖动,上限 maxD。
func jitterBackoff(base, maxD time.Duration, step int32) time.Duration {
	d := base
	for i := int32(1); i < step && d < maxD; i++ {
		if d >= maxD/2 {
			d = maxD
			break
		}
		d *= 2
	}
	if d > maxD {
		d = maxD
	}
	return d/2 + time.Duration(rand.Int64N(int64(d)/2+1))
}
