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
	"github.com/gtkit/orderd/internal/shared/alert"
	"github.com/gtkit/orderd/internal/store"
)

// QueryConfig 查单 worker 调优参数。
type QueryConfig struct {
	WorkerID     string
	PollInterval time.Duration
	BatchSize    int
	LeaseTTL     time.Duration
	// MaxQueries 查询次数阈值,两处用途:
	//   1. UNKNOWN 连续"不存在"达此次数后才关闭尝试(§9:不能查不到就换号);
	//   2. CLOSING 仍报"有单未支付"达此次数时告警一次(滞留可见,状态不变)。
	MaxQueries     int32
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
			if cerr := gw.Close(gctx, paygateway.CloseInput{Channel: a.Channel, PayAccountID: a.PayAccountID, OutTradeNo: a.OutTradeNo}); cerr != nil {
				logger.Warnw("关单失败", "channel", a.Channel, "out_trade_no", a.OutTradeNo, "err", cerr)
			}
			q.alertIfCloseStalled(ctx, a)
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

// alertIfCloseStalled 在本轮重排使查询次数恰好跨过 MaxQueries 时告警一次(边沿触发:
// reschedule 会 query_count++,故该等式在单个尝试生命周期内只成立一次,不会每轮刷)。
//
// 达阈值只告警,不落终态:关单未生效不代表用户不会在渠道侧付款,强行 CLOSED 会把"结果未知"
// 变成错误的确定——资金随后入账时只能落异常流水并人工退款。滞留由渠道单自然过期或人工介入收敛。
func (q *QueryWorker) alertIfCloseStalled(ctx context.Context, a *models.PaymentAttempt) {
	if a.QueryCount+1 != q.cfg.MaxQueries {
		return
	}
	q.alerter.Anomaly(ctx, "attempt_close_stalled", map[string]any{
		"channel": a.Channel, "out_trade_no": a.OutTradeNo, "query_count": a.QueryCount + 1,
	})
}

// reschedule 设下次查单时间(退避)+ query_count++ + 清租约。
// 事务内按行锁重读并复核状态:仅当尝试仍为 UNKNOWN/CLOSING 才重排;已被应用路径推进
// (取消→CLOSING→…、回调→PAID)时直接放弃,绝不回写认领时的陈旧状态——认领快照到重排之间
// 隔着最长 GatewayTimeout 的网关调用,租约只互斥其他 worker,不互斥应用路径写。
func (q *QueryWorker) reschedule(ctx context.Context, a *models.PaymentAttempt) {
	if err := q.store.WithinTx(ctx, func(ctx context.Context, r store.Repos) error {
		cur, err := r.Attempts().GetByOutTradeNo(ctx, a.OutTradeNo, true) // 行锁:读-复核-写原子
		if err != nil || cur == nil {
			return err
		}
		if cur.Status != models.AttemptUnknown && cur.Status != models.AttemptClosing {
			return nil // 已被推进到收敛集合之外(PAID/CLOSED),放弃重排
		}
		// 仍在收敛集合(UNKNOWN,或期间被取消/关单路径推进为 CLOSING):状态取重读值不回写,
		// 调度字段为 worker 独占,基于重读计数推进。
		cur.QueryCount++
		cur.NextQueryAt = sql.NullTime{Time: q.clock().Add(jitterBackoff(q.cfg.BaseBackoff, q.cfg.MaxBackoff, cur.QueryCount)), Valid: true}
		cur.LockedBy = ""
		cur.LockedUntil = sql.NullTime{}
		return r.Attempts().Update(ctx, cur)
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
