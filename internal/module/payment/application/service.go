// Package application(payment)实现支付主链:支付准备(§8/§9)与支付成功确认(§7)。
//
// 边界:只依赖 store 端口 / paygateway / account / alert,不 import repository/gorm/gin。
// 铁律:网关调用绝不在 DB 事务内(持锁期间做网络调用会放大锁竞争);事务只做本地读改写。
package application

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/gtkit/logger"
	"github.com/gtkit/orderd/internal/models"
	eventapp "github.com/gtkit/orderd/internal/module/event/application"
	refundapp "github.com/gtkit/orderd/internal/module/refund/application"
	"github.com/gtkit/orderd/internal/paygateway"
	"github.com/gtkit/orderd/internal/pkg/sqlutil"
	"github.com/gtkit/orderd/internal/shared/account"
	"github.com/gtkit/orderd/internal/shared/alert"
	"github.com/gtkit/orderd/internal/shared/obs"
	"github.com/gtkit/orderd/internal/store"
)

// minPayWindow 调起支付要求的最小剩余支付窗口:订单或待复用尝试距截止不足此值时不再向渠道下单。
const minPayWindow = time.Minute

// Deps 是 payment 服务的依赖。
type Deps struct {
	Store          store.Store
	Router         paygateway.Router
	Accounts       account.Resolver
	Alerter        alert.Alerter
	Clock          func() time.Time
	NewEventID     func() (string, error)
	AttemptTTL     time.Duration
	OutTradeNo     func() string // 生成商户订单号(每尝试独立)
	GatewayTimeout time.Duration // 单次网关调用强制超时(评审 P1)
	// Obs 关键路径埋点端口(nil → Nop,行为不变)。
	Obs obs.FlowObserver
	// LateRefundApps 已关闭订单收到付款时自动退款(不恢复订单)的 app;LateRefunder 执行退款。
	// 列表为空时 LateRefunder 可为 nil。
	LateRefundApps map[string]bool
	LateRefunder   LateRefunder
}

// LateRefunder 迟到付款自动退款端口(refund 服务实现:按异常流水退款,out_refund_no 幂等)。
type LateRefunder interface {
	RefundAnomalyTxn(ctx context.Context, in refundapp.AnomalyRefundInput) (refundapp.RefundOutcome, error)
}

// Service 支付应用服务。
type Service struct{ d Deps }

// New 构造 payment 服务。
func New(d Deps) *Service {
	if d.Clock == nil {
		d.Clock = time.Now
	}
	if d.AttemptTTL == 0 {
		d.AttemptTTL = 15 * time.Minute
	}
	if d.GatewayTimeout == 0 {
		d.GatewayTimeout = 10 * time.Second
	}
	if d.Obs == nil {
		d.Obs = obs.Nop{}
	}
	return &Service{d: d}
}

func (s *Service) gwTimeout() time.Duration { return s.d.GatewayTimeout }

// PrepareInput 支付准备入参(CreateOrder 首次调起 / PreparePayment RPC 换渠道再调起共用)。
type PrepareInput struct {
	AppID     string
	OrderNo   string
	UserID    int64
	PayMethod models.PayMethod
	ChannelID int64
	ClientIP  string
}

// PrepareOutcome 支付准备结果。
type PrepareOutcome struct {
	AttemptNo int32
	AppParams string
	Status    models.OrderStatus
	ExpireAt  time.Time
	Reused    bool
}

// attemptPlan 阶段 A 的决策:已选定可下单的尝试(复用或新建),或指出须先在渠道侧关闭的旧尝试。
type attemptPlan struct {
	attemptNo  int32
	outTradeNo string
	payAcct    string
	expireAt   time.Time
	reused     bool
	amount     int64
	subject    string
	currency   string
	replace    *models.PaymentAttempt // 非 nil:换渠道 / 旧尝试将到期,须先关闭它再新建
}

func (p *attemptPlan) take(a *models.PaymentAttempt, reused bool) {
	p.attemptNo, p.outTradeNo, p.payAcct, p.expireAt, p.reused = a.AttemptNo, a.OutTradeNo, a.PayAccountID, a.ExpireAt, reused
}

// rejection 调起支付的业务拒绝原因(obs.PrepareReject*),经 %w 挂在领域错误上供出口埋点,
// 不改变领域错误的 errors.Is 判定;文本附在错误信息末尾便于排查。
type rejection string

func (r rejection) Error() string { return "(reason=" + string(r) + ")" }

// PreparePayment 为已存在的 PENDING 订单(重新)调起支付;业务拒绝按原因计数。
func (s *Service) PreparePayment(ctx context.Context, in PrepareInput) (PrepareOutcome, error) {
	out, err := s.preparePayment(ctx, in)
	if r, ok := errors.AsType[rejection](err); ok {
		s.d.Obs.PrepareRejected(string(r))
	}
	return out, err
}

// preparePayment 调起支付主流程。
//
// 阶段化(网关调用不在事务内):
//
//	A(tx):锁订单确认 PENDING → 按活跃尝试状态决定复用 / 替换 / 新建 → 选定尝试置 REQUESTING;
//	A2    :须替换旧 READY 尝试时,事务外向渠道关单,再在事务内复核后 CLOSED 旧尝试 + 新建;
//	B     :调网关下单(不持锁);
//	C(tx):复核订单仍 PENDING → 尝试置 READY + prepay。
func (s *Service) preparePayment(ctx context.Context, in PrepareInput) (PrepareOutcome, error) {
	if !in.PayMethod.Valid() {
		return PrepareOutcome{}, fmt.Errorf("%w: 支付方式非法", models.ErrIllegalTransition)
	}
	channel := in.PayMethod.Channel()

	// --- 阶段 A ---
	var plan attemptPlan
	err := s.d.Store.WithinTx(ctx, func(ctx context.Context, r store.Repos) error {
		o, err := s.lockPendingOrder(ctx, r, in)
		if err != nil {
			return err
		}
		// 金额/标题/币种来自订单权威快照,传给网关(评审 P0:不可漏传导致 0 金额空标题)。
		plan.amount, plan.subject, plan.currency = o.PayAmount, o.ProductTitle, sqlutil.OrDefault(o.Currency, "CNY")
		active, err := r.Attempts().GetActiveByOrder(ctx, in.OrderNo, true)
		if err != nil {
			return err
		}
		switch {
		case active == nil:
			return s.newAttempt(ctx, r, in, channel, o, &plan)
		case active.Status == models.AttemptClosing:
			return fmt.Errorf("%w: 活跃尝试关单确认中,待查单收敛后重试%w", models.ErrActiveAttemptExists, rejection(obs.PrepareRejectClosing))
		case active.Status == models.AttemptReady && (active.Channel != channel || !active.ExpireAt.After(s.d.Clock().Add(minPayWindow))):
			// 换渠道,或同渠道旧尝试即将到期:不能在渠道侧仍可支付的旧单旁边开新单,先关旧单(事务外)。
			plan.replace = active
			return nil
		case active.Channel != channel:
			// 异渠道 REQUESTING/UNKNOWN:提交结果未定,不能并发开新单;分别由超时 watchdog / 查单 worker 收敛。
			return fmt.Errorf("%w: 存在结果未定的活跃尝试,换渠道需待其收敛%w", models.ErrActiveAttemptExists, rejection(obs.PrepareRejectForeignInflight))
		default:
			// 同渠道 REQUESTING/READY/UNKNOWN:重新驱动到 REQUESTING 以便再取 prepay
			// (并发幂等重放共用同一在途商户单号,不拒绝)。
			active.Status = models.AttemptRequesting
			if err := r.Attempts().Update(ctx, active); err != nil {
				return err
			}
			plan.take(active, true)
			return nil
		}
	})
	if err != nil {
		return PrepareOutcome{}, err
	}
	if plan.replace != nil {
		if err := s.replaceAttempt(ctx, in, channel, &plan); err != nil {
			return PrepareOutcome{}, err
		}
	}

	// --- 阶段 B:调网关(不持锁,强制超时)---
	gw, err := s.d.Router.Resolve(channel, plan.payAcct)
	if err != nil {
		s.markAttemptUnknown(plan.outTradeNo)
		return PrepareOutcome{}, err
	}
	gctx, cancel := context.WithTimeout(ctx, s.gwTimeout())
	defer cancel()
	pr, err := gw.Prepare(gctx, paygateway.PrepareInput{
		Channel: channel, PayAccountID: plan.payAcct, OutTradeNo: plan.outTradeNo,
		Amount: plan.amount, Subject: plan.subject, Currency: plan.currency, ClientIP: in.ClientIP, ExpireAt: plan.expireAt,
	})
	if err != nil {
		s.markAttemptUnknown(plan.outTradeNo) // 结果未知 → 交由查单 worker
		return PrepareOutcome{}, fmt.Errorf("网关下单失败: %w", err)
	}

	// --- 阶段 C:复核订单仍 PENDING 后落 READY ---
	err = s.d.Store.WithinTx(ctx, func(ctx context.Context, r store.Repos) error {
		o, err := r.Orders().GetByOrderNo(ctx, in.AppID, in.OrderNo, true)
		if err != nil {
			return err
		}
		if o.Status != models.OrderPending {
			// 期间被取消/关单:不再置就绪,尝试留待 reconcile(评审 #8 取消 vs 建尝试竞态)。
			return models.ErrOrderNotPending
		}
		a, err := r.Attempts().GetByOutTradeNo(ctx, plan.outTradeNo, false)
		if err != nil {
			return err
		}
		if a == nil {
			return store.ErrNotFound
		}
		a.Status = models.AttemptReady
		a.PrepayID = pr.PrepayID
		return r.Attempts().Update(ctx, a)
	})
	if err != nil {
		return PrepareOutcome{}, err
	}
	return PrepareOutcome{AttemptNo: plan.attemptNo, AppParams: pr.AppParams, Status: models.OrderPending, ExpireAt: plan.expireAt, Reused: plan.reused}, nil
}

// lockPendingOrder 锁订单并校验归属、PENDING 与剩余支付窗口。
func (s *Service) lockPendingOrder(ctx context.Context, r store.Repos, in PrepareInput) (*models.Order, error) {
	o, err := r.Orders().GetByOrderNo(ctx, in.AppID, in.OrderNo, true)
	if err != nil {
		return nil, err
	}
	if o.UserID != in.UserID {
		return nil, models.ErrOrderNotOwned
	}
	if o.Status != models.OrderPending {
		return nil, models.ErrOrderNotPending
	}
	if !o.ExpireAt.After(s.d.Clock().Add(minPayWindow)) {
		return nil, fmt.Errorf("%w: 订单剩余支付窗口不足 %s%w", models.ErrOrderNotPending, minPayWindow, rejection(obs.PrepareRejectPayWindow))
	}
	return o, nil
}

// newAttempt 解析渠道账号并新建 REQUESTING 尝试。截止取 AttemptTTL 与订单截止的较早者:
// 该值作为 time_expire 传给渠道,订单到期时渠道侧二维码/交易随之失效。
func (s *Service) newAttempt(ctx context.Context, r store.Repos, in PrepareInput, channel string, o *models.Order, plan *attemptPlan) error {
	acct, err := s.d.Accounts.Resolve(ctx, in.AppID, channel)
	if err != nil {
		return err
	}
	cnt, err := r.Attempts().CountByOrder(ctx, in.OrderNo)
	if err != nil {
		return err
	}
	expireAt := s.d.Clock().Add(s.d.AttemptTTL)
	if o.ExpireAt.Before(expireAt) {
		expireAt = o.ExpireAt
	}
	na := &models.PaymentAttempt{
		AppID: in.AppID, OrderNo: in.OrderNo, AttemptNo: int32(cnt + 1),
		Channel: channel, PayAccountID: acct.PayAccountID, ConfigVersion: acct.ConfigVersion,
		OutTradeNo: s.d.OutTradeNo(), Status: models.AttemptRequesting, ExpireAt: expireAt,
	}
	if err := r.Attempts().Insert(ctx, na); err != nil {
		return err
	}
	plan.take(na, false)
	return nil
}

// replaceAttempt 换渠道 / 旧尝试将到期:事务外向渠道关闭旧 READY 尝试,再在事务内锁订单→锁旧尝试复核后替换。
// 关单成功 → 旧尝试 CLOSED + 新建;失败 → 旧尝试 CLOSING(提交后返回 ErrActiveAttemptExists),
// 由查单 worker 按渠道查单结果收敛,期间不在渠道侧仍可支付的旧单旁边开新单。
func (s *Service) replaceAttempt(ctx context.Context, in PrepareInput, channel string, plan *attemptPlan) error {
	closed := s.CloseAtGateway(ctx, plan.replace)
	var closingKept bool
	err := s.d.Store.WithinTx(ctx, func(ctx context.Context, r store.Repos) error {
		o, err := s.lockPendingOrder(ctx, r, in)
		if err != nil {
			return err
		}
		old, err := r.Attempts().GetByOutTradeNo(ctx, plan.replace.OutTradeNo, true)
		if err != nil {
			return err
		}
		if old == nil || old.Status != models.AttemptReady {
			return fmt.Errorf("%w: 旧尝试已被并发推进", models.ErrActiveAttemptExists)
		}
		if !closed {
			old.Status = models.AttemptClosing
			closingKept = true
			return r.Attempts().Update(ctx, old)
		}
		old.Status = models.AttemptClosed
		if err := r.Attempts().Update(ctx, old); err != nil {
			return err
		}
		return s.newAttempt(ctx, r, in, channel, o, plan)
	})
	if err != nil {
		return err
	}
	if closingKept {
		return fmt.Errorf("%w: 旧尝试渠道关单未确认,待查单收敛后重试%w", models.ErrActiveAttemptExists, rejection(obs.PrepareRejectCloseFailed))
	}
	return nil
}

// CloseAtGateway 事务外尽力向渠道关闭一个尝试,返回是否成功。它只负责缩窄"渠道侧仍可支付"的窗口:
// 失败只记日志;尝试/订单从 CLOSING 到 CLOSED 的落定一律由查单 worker 按渠道查单结果完成。
func (s *Service) CloseAtGateway(ctx context.Context, a *models.PaymentAttempt) bool {
	gw, err := s.d.Router.Resolve(a.Channel, a.PayAccountID)
	if err == nil {
		gctx, cancel := context.WithTimeout(ctx, s.gwTimeout())
		defer cancel()
		err = gw.Close(gctx, paygateway.CloseInput{Channel: a.Channel, PayAccountID: a.PayAccountID, OutTradeNo: a.OutTradeNo})
	}
	if err != nil {
		logger.Warnw("渠道关单失败,交查单 worker 收敛", "channel", a.Channel, "out_trade_no", a.OutTradeNo, "err", err)
		return false
	}
	return true
}

// markAttemptUnknown 网关下单失败/超时后,把该尝试置为 UNKNOWN(结果未知),交查单 worker 收敛。
// 用与请求解耦的 ctx(评审 P0:失败原因常是请求超时/取消,复用原 ctx 会让这次 DB 更新也失败,
// 尝试永久卡在 REQUESTING 且查单 worker 不扫描)。
func (s *Service) markAttemptUnknown(outTradeNo string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = s.d.Store.WithinTx(ctx, func(ctx context.Context, r store.Repos) error {
		a, err := r.Attempts().GetByOutTradeNo(ctx, outTradeNo, true) // 行锁:只写尝试单行,读-复核-写原子
		if err != nil || a == nil {
			return err
		}
		if a.Status == models.AttemptRequesting {
			a.Status = models.AttemptUnknown
			return r.Attempts().Update(ctx, a)
		}
		return nil
	})
}

// ---------------------------------------------------------------------
// ConfirmPaySuccess:支付成功核心事务(§7)
// ---------------------------------------------------------------------

// ConfirmInput 网关确认的一笔成功支付(回调或查单)。
type ConfirmInput struct {
	AppID         string
	OrderNo       string
	OutTradeNo    string
	Channel       string
	PayAccountID  string
	TransactionID string
	Amount        int64
	Currency      string
	PaidAt        time.Time
	NotifyDigest  string
}

// ConfirmResult 确认结果。
type ConfirmResult struct {
	Outcome     string // paid / duplicate / anomaly
	Anomaly     bool
	AnomalyKind string
	// Recovered 结算前订单为 CLOSED(关闭后迟到付款):Outcome 仍为 paid,埋点与告警单列。
	Recovered bool
}

// ConfirmPaySuccess 在单事务内完成 §7,并遵守两条资金铁律:
//   - 资金真相必落库:第二笔真实付款/金额账号不符/无法绑定尝试的真实交易,追加 anomaly 流水后提交
//     (不回滚丢证),再事务外告警(评审 #4);
//   - CAS 唯一成功:仅一笔跃迁订单到 PAID,胜出后关其它活跃尝试并写成功账号快照(评审 #6)。
func (s *Service) ConfirmPaySuccess(ctx context.Context, in ConfirmInput) (ConfirmResult, error) {
	if in.Amount <= 0 {
		return ConfirmResult{}, fmt.Errorf("%w: 确认金额必须为正", models.ErrInvalidAmount)
	}
	// 路由账号在事务外解析:供结算快照(mch_id/pay_app_id)与无法绑定尝试时的流水归属。
	// 解析失败返回错误让渠道重投,不静默写空快照。
	acct, err := s.d.Accounts.ResolveByID(ctx, in.PayAccountID)
	if err != nil {
		return ConfirmResult{}, fmt.Errorf("解析路由账号 %s 失败: %w", in.PayAccountID, err)
	}
	var (
		res          ConfirmResult
		alertOrderNo string
		prevReason   models.CloseReason
		lateTxnID    uint64 // 非零:事务提交后对该 paid_after_closed 流水发起自动退款
	)
	err = s.d.Store.WithinTx(ctx, func(ctx context.Context, r store.Repos) error {
		// 权威身份由 out_trade_no 唯一派生(评审 P0):不信任回调传入的 order_no/app_id。
		attempt, err := r.Attempts().GetByOutTradeNo(ctx, in.OutTradeNo, false)
		if err != nil {
			return err
		}
		if attempt == nil {
			// 未知商户单号的真实通知:无法安全绑定订单 → 资金事实按路由账号归属落 anomaly 流水(order_no 为空),不结算。
			if err := insertAnomalyTxn(ctx, r, acct.AppID, "", in, nil, models.AnomalyKindUnknownOutTradeNo); err != nil {
				return err
			}
			res = ConfirmResult{Outcome: "anomaly", Anomaly: true, AnomalyKind: models.AnomalyKindUnknownOutTradeNo}
			return nil
		}
		alertOrderNo = attempt.OrderNo
		o, err := r.Orders().GetByOrderNo(ctx, attempt.AppID, attempt.OrderNo, true)
		if err != nil {
			return err
		}

		// 通知路由到的渠道/账号必须与该尝试冻结的一致;不一致=错误路由/映射 → anomaly 落库不结算。
		if in.Channel != attempt.Channel || in.PayAccountID != attempt.PayAccountID {
			if err := insertAnomalyTxn(ctx, r, o.AppID, o.OrderNo, in, attempt, models.AnomalyKindAccountChannelMismatch); err != nil {
				return err
			}
			res = ConfirmResult{Outcome: "anomaly", Anomaly: true, AnomalyKind: models.AnomalyKindAccountChannelMismatch}
			return nil
		}

		// 1) 重复回调:命中唯一流水 → 重读校验一致性(权威订单为派生 o.OrderNo)。
		if dup, err := r.Transactions().GetByTxnID(ctx, attempt.Channel, attempt.PayAccountID, in.TransactionID); err != nil {
			return err
		} else if dup != nil {
			if dup.OrderNo != o.OrderNo || dup.Amount != in.Amount || dup.PayAccountID != attempt.PayAccountID {
				res = ConfirmResult{Outcome: "anomaly", Anomaly: true, AnomalyKind: "duplicate_txn_mismatch"}
				return nil
			}
			// 该交易已结算过本订单:若尝试仍占活跃槽(状态被并发回写打回/前次事务半途),
			// 补推进到 PAID 释放槽位;不推进则查单 worker 每轮"查到已付→duplicate→重排"永不收敛。
			if err := settleAttempt(ctx, r, attempt); err != nil {
				return err
			}
			// 迟到付款退款流水的重复通知:再次触发自动退款(退款号幂等),覆盖"提交后、退款前进程崩溃"。
			if dup.AnomalyKind == models.AnomalyKindPaidAfterClosed && s.d.LateRefundApps[o.AppID] {
				lateTxnID = dup.ID
			}
			res = ConfirmResult{Outcome: "duplicate"}
			return nil
		}

		mismatch := in.Amount != o.PayAmount || (in.Currency != "" && in.Currency != o.Currency)

		// 2) 订单状态不在结算白名单(含 PAID/退款域/已取消):第二笔真实付款 → 落 anomaly 流水,
		// 不改订单(评审 #4)。准入用 CanSettlePayment 而非状态机全表:REFUNDING→PAID 是
		// 退款失败回退专用边,支付确认借道会把退款中订单错误拉回 PAID(覆盖快照+二次发事件)。
		if !models.CanSettlePayment(o.Status) {
			if err := insertAnomalyTxn(ctx, r, o.AppID, o.OrderNo, in, attempt, models.AnomalyKindSecondRealPayment); err != nil {
				return err
			}
			if err := settleAttempt(ctx, r, attempt); err != nil {
				return err
			}
			res = ConfirmResult{Outcome: "anomaly", Anomaly: true, AnomalyKind: models.AnomalyKindSecondRealPayment}
			return nil
		}

		// 3) 金额/币种不符的真实交易:落 anomaly 流水,不改订单。渠道侧该单已支付终态,尝试同样
		// 收敛 PAID 释放活跃槽——否则订单卡在"PENDING 但无可用尝试",到期后只能靠滞留兜底关闭。
		if mismatch {
			if err := insertAnomalyTxn(ctx, r, o.AppID, o.OrderNo, in, attempt, models.AnomalyKindAmountMismatch); err != nil {
				return err
			}
			if err := settleAttempt(ctx, r, attempt); err != nil {
				return err
			}
			res = ConfirmResult{Outcome: "anomaly", Anomaly: true, AnomalyKind: models.AnomalyKindAmountMismatch}
			return nil
		}

		// 4) 已关闭订单 + app 启用迟到付款退款:接受资金事实但不恢复订单(接入方收到 order.closed 后可能已
		// 释放资源)——落 paid_after_closed 流水、尝试收敛 PAID,订单保持 CLOSED 不发 order.paid;提交后自动退款。
		// CLOSING 仍走正常结算:接入方尚未收到 order.closed。
		if o.Status == models.OrderClosed && s.d.LateRefundApps[o.AppID] {
			t := payTxn(o.AppID, o.OrderNo, in, attempt, true, models.AnomalyKindPaidAfterClosed)
			if err := r.Transactions().Insert(ctx, t); errors.Is(err, store.ErrDuplicate) {
				// 并发同笔通知:第 1 步的快照读看不到另一路刚提交的流水,插入才撞唯一键。
				// 先提交的一路已在事务后触发自动退款,本路按重复处理。
				res = ConfirmResult{Outcome: "duplicate"}
				return nil
			} else if err != nil {
				return err
			}
			// 订单 CLOSED 时其尝试通常已 CLOSED(非活跃,settleAttempt 不处理):按渠道事实显式置 PAID(CLOSED→PAID 合法)。
			if attempt.Status != models.AttemptPaid {
				attempt.Status = models.AttemptPaid
				if err := r.Attempts().Update(ctx, attempt); err != nil {
					return err
				}
			}
			lateTxnID = t.ID
			res = ConfirmResult{Outcome: "anomaly", Anomaly: true, AnomalyKind: models.AnomalyKindPaidAfterClosed}
			return nil
		}

		// 5) 正常胜出:落流水(冲突重读比对,评审 P0)+ 尝试 PAID + 关其它活跃 + 订单 CAS→PAID + 快照 + 事件。
		ins, err := insertWinningTxn(ctx, r, o, in, attempt)
		if err != nil {
			return err
		}
		switch ins {
		case txnDupConsistent:
			res = ConfirmResult{Outcome: "duplicate"} // 并发同笔,已由另一路结算
			return nil
		case txnDupConflict:
			res = ConfirmResult{Outcome: "anomaly", Anomaly: true, AnomalyKind: "txn_conflict"}
			return nil // 该交易号已属其它订单 → 禁止结算本订单
		}

		attempt.Status = models.AttemptPaid
		if err := r.Attempts().Update(ctx, attempt); err != nil {
			return err
		}
		if err := closeOtherActiveAttempts(ctx, r, o.OrderNo, attempt); err != nil {
			return err
		}

		prev := o.Status
		prevReason = o.CloseReason
		o.Status = models.OrderPaid
		o.CloseReason = models.CloseNone // CLOSING/CLOSED 迟到付款胜出:不留"已支付但带关单原因"的矛盾读数
		o.AggregateVersion++
		o.TradeNo = in.TransactionID
		o.PaidAt = sqlutil.Time(in.PaidAt)
		// 成功快照:渠道/商户单号/账号全取胜出尝试(退款以此为权威,评审 #6 / P1)。
		o.SettledAttemptID = sqlutil.Int64(int64(attempt.ID))
		o.SettledChannel = attempt.Channel
		o.SettledOutTradeNo = attempt.OutTradeNo
		o.SettledPayAccountID = attempt.PayAccountID
		o.SettledConfigVersion = attempt.ConfigVersion
		o.SettledMchID, o.SettledPayAppID = acct.MchID, acct.PayAppID
		if err := r.Orders().Update(ctx, o); err != nil {
			return err
		}
		if err := eventapp.WriteLog(ctx, r, o, prev, "system", "支付成功"); err != nil {
			return err
		}
		eid, err := s.d.NewEventID()
		if err != nil {
			return err
		}
		env := models.BuildPaidEnvelope(o, prev, eid, s.d.Clock())
		if err := eventapp.WriteEvent(ctx, r, o.AppID, o.OrderNo, env); err != nil {
			return err
		}
		res = ConfirmResult{Outcome: "paid", Recovered: prev == models.OrderClosed}
		return nil
	})
	if err != nil {
		return ConfirmResult{}, err
	}
	if res.Anomaly {
		s.d.Alerter.Anomaly(ctx, res.AnomalyKind, map[string]any{
			"order_no": alertOrderNo, "out_trade_no": in.OutTradeNo, "txn_id": in.TransactionID,
			"amount": in.Amount, "channel": in.Channel, "pay_account_id": in.PayAccountID,
		})
	}
	outcome := res.Outcome
	if res.Recovered {
		// 已关闭订单收到真实付款:资金已正确结算,但说明关单收敛与渠道侧可付窗口存在偏差,须可见。
		outcome = "paid_after_closed"
		s.d.Alerter.Anomaly(ctx, "paid_after_closed", map[string]any{
			"order_no": alertOrderNo, "out_trade_no": in.OutTradeNo, "txn_id": in.TransactionID,
			"amount": in.Amount, "channel": in.Channel, "close_reason": int(prevReason),
		})
	}
	s.d.Obs.ConfirmOutcome(outcome) // 事务外纯内存计数
	if lateTxnID != 0 {
		s.refundLatePayment(ctx, lateTxnID, alertOrderNo)
	}
	return res, nil
}

// refundLatePayment 对 paid_after_closed 流水发起自动退款(事务外;退款号 late-<流水id> 保证重复触发幂等)。
// 失败只告警:流水已落库,运维可经 RefundAnomalyTxn 手动退款;不影响本次回调的应答。
func (s *Service) refundLatePayment(ctx context.Context, txnID uint64, orderNo string) {
	out, err := s.d.LateRefunder.RefundAnomalyTxn(ctx, refundapp.AnomalyRefundInput{
		TxnID: txnID, OutRefundNo: "late-" + strconv.FormatUint(txnID, 10),
		Reason: "订单已关闭后付款,自动退款", Actor: models.OpsActorSystem,
	})
	if err != nil {
		s.d.Alerter.Anomaly(ctx, "late_payment_refund_failed", map[string]any{
			"txn_id": txnID, "order_no": orderNo, "err": err.Error(),
		})
		return
	}
	logger.Infow("迟到付款已发起自动退款", "txn_id", txnID, "order_no", orderNo, "refund_no", out.RefundNo, "status", out.Status, "reused", out.Reused)
}

// ---------------------------------------------------------------------
// 查单结果处理(查单 worker 调用,§9 收敛"结果未知")
// ---------------------------------------------------------------------

// ConfirmPayFromQuery 查单=已支付 → 复用 §7 支付成功核心事务。
func (s *Service) ConfirmPayFromQuery(ctx context.Context, a *models.PaymentAttempt, qr paygateway.QueryResult) (ConfirmResult, error) {
	return s.ConfirmPaySuccess(ctx, ConfirmInput{
		AppID: a.AppID, OrderNo: a.OrderNo, OutTradeNo: a.OutTradeNo, Channel: a.Channel,
		PayAccountID: a.PayAccountID, TransactionID: qr.TransactionID, Amount: qr.Amount,
		Currency: "CNY", PaidAt: qr.PaidAt, NotifyDigest: "query",
	})
}

// RecoverStuckRequesting 卡死在 REQUESTING 的尝试 → UNKNOWN(交查单 worker 收敛,超时 watchdog 调用)。
func (s *Service) RecoverStuckRequesting(ctx context.Context, outTradeNo string) error {
	return s.d.Store.WithinTx(ctx, func(ctx context.Context, r store.Repos) error {
		a, err := r.Attempts().GetByOutTradeNo(ctx, outTradeNo, true) // 行锁:只写尝试单行,读-复核-写原子
		if err != nil || a == nil {
			return err
		}
		if a.Status == models.AttemptRequesting {
			a.Status = models.AttemptUnknown
			return r.Attempts().Update(ctx, a)
		}
		return nil
	})
}

// ApplyQueryNotPaid 查单=有单未支付 → UNKNOWN 尝试恢复为 READY(可继续等待回调)。
func (s *Service) ApplyQueryNotPaid(ctx context.Context, outTradeNo string) error {
	return s.d.Store.WithinTx(ctx, func(ctx context.Context, r store.Repos) error {
		// 行锁:只写尝试单行,读-复核-写原子。防并发取消/超时把该尝试置 CLOSING 后,
		// 本函数基于陈旧 UNKNOWN 盲写 READY → 订单 CLOSING+尝试 READY 无人收敛的永久滞留(P1-A)。
		a, err := r.Attempts().GetByOutTradeNo(ctx, outTradeNo, true)
		if err != nil || a == nil {
			return err
		}
		if a.Status == models.AttemptUnknown {
			a.Status = models.AttemptReady
			return r.Attempts().Update(ctx, a)
		}
		return nil
	})
}

// CloseAttemptAndMaybeOrder 查单确证不存在/已关闭 → 尝试转 CLOSED;
// 若订单处于 CLOSING 且已无活跃尝试,则订单转 CLOSED 并发 order.closed 事件(完成取消/关单闭环)。
func (s *Service) CloseAttemptAndMaybeOrder(ctx context.Context, outTradeNo string) error {
	var closingSince time.Time // 非零=本次把订单由 CLOSING 落定为 CLOSED(updated_at 即进入 CLOSING 的时刻)
	err := s.d.Store.WithinTx(ctx, func(ctx context.Context, r store.Repos) error {
		// 统一锁序订单→尝试:先无锁读取不可变的 app_id/order_no,再锁订单,再锁尝试重读复核。
		// (旧实现"先写尝试后锁订单"是全库唯一逆序,与五条订单→尝试路径互为死锁环;
		//  且无锁复核会把并发已 PAID 的尝试盲写覆盖成 CLOSED。)
		a0, err := r.Attempts().GetByOutTradeNo(ctx, outTradeNo, false)
		if err != nil || a0 == nil {
			return err
		}
		o, err := r.Orders().GetByOrderNo(ctx, a0.AppID, a0.OrderNo, true)
		if err != nil {
			return err
		}
		a, err := r.Attempts().GetByOutTradeNo(ctx, outTradeNo, true)
		if err != nil || a == nil {
			return err
		}
		if a.Status == models.AttemptUnknown || a.Status == models.AttemptClosing {
			a.Status = models.AttemptClosed
			if err := r.Attempts().Update(ctx, a); err != nil {
				return err
			}
		}
		if o.Status != models.OrderClosing {
			return nil
		}
		active, err := r.Attempts().GetActiveByOrder(ctx, a.OrderNo, true)
		if err != nil {
			return err
		}
		if active != nil {
			return nil // 仍有活跃尝试,暂不关单
		}
		prev, since := o.Status, o.UpdatedAt
		o.Status = models.OrderClosed
		o.AggregateVersion++
		if err := r.Orders().Update(ctx, o); err != nil {
			return err
		}
		if err := eventapp.WriteLog(ctx, r, o, prev, "system", "关单确认"); err != nil {
			return err
		}
		eid, err := s.d.NewEventID()
		if err != nil {
			return err
		}
		env := models.BuildClosedEnvelope(o, prev, eid, s.d.Clock())
		if err := eventapp.WriteEvent(ctx, r, o.AppID, o.OrderNo, env); err != nil {
			return err
		}
		closingSince = since
		return nil
	})
	if err == nil && !closingSince.IsZero() {
		s.d.Obs.ClosingSettled(obs.ClosingViaQuery, max(s.d.Clock().Sub(closingSince).Seconds(), 0))
	}
	return err
}

// settleAttempt 把渠道侧已支付终态的尝试收敛为 PAID(释放活跃槽);已非活跃则不动。
// 置 PAID 只释放 active_order 槽位、不申请,不会触发唯一守卫冲突。
func settleAttempt(ctx context.Context, r store.Repos, a *models.PaymentAttempt) error {
	if !a.Status.Active() {
		return nil
	}
	a.Status = models.AttemptPaid
	return r.Attempts().Update(ctx, a)
}

// payTxn 由流水归属(appID/orderNo)+ 通知 in + 尝试 attempt(可为 nil)组装一条支付流水。
// anomalyKind:异常成因(models.AnomalyKind*),正常流水传空串——处置方式因成因而异,必须随流水落库。
func payTxn(appID, orderNo string, in ConfirmInput, attempt *models.PaymentAttempt, anomaly bool, anomalyKind string) *models.PaymentTransaction {
	t := &models.PaymentTransaction{
		AppID: appID, OrderNo: orderNo, PayAccountID: in.PayAccountID,
		Channel: in.Channel, Kind: models.TxnPay, OutTradeNo: in.OutTradeNo,
		TransactionID: in.TransactionID, Amount: in.Amount, Currency: sqlutil.OrDefault(in.Currency, "CNY"),
		Anomaly: anomaly, AnomalyKind: anomalyKind, PaidAt: sqlutil.Time(in.PaidAt), NotifyDigest: in.NotifyDigest,
	}
	if attempt != nil {
		t.PaymentAttemptID = sqlutil.Int64(int64(attempt.ID))
	}
	return t
}

// insertAnomalyTxn 插入异常流水(anomaly=1,携带成因);命中唯一键视为已记录,忽略(异常事实已在库)。
func insertAnomalyTxn(ctx context.Context, r store.Repos, appID, orderNo string, in ConfirmInput, attempt *models.PaymentAttempt, anomalyKind string) error {
	if err := r.Transactions().Insert(ctx, payTxn(appID, orderNo, in, attempt, true, anomalyKind)); err != nil && !errors.Is(err, store.ErrDuplicate) {
		return err
	}
	return nil
}

type txnInsertResult int

const (
	txnInserted      txnInsertResult = iota // 首次成功插入
	txnDupConsistent                        // 命中唯一键且与本订单一致(并发同笔)
	txnDupConflict                          // 命中唯一键但属其它订单/金额/账号(禁止结算)
)

// insertWinningTxn 结算前插入正常流水;命中 uk_txn 时重读比对,区分"并发同笔"与"跨订单冲突"(评审 P0)。
// 注:RR 隔离下本事务的重读是快照读,看不到并发【同订单】另一事务刚提交的流水行——但同订单并发确认
// 已被订单行锁 + CanSettlePayment 前置串行化(第二个确认见 PAID 走 second_real_payment,不到此),故
// txnDupConsistent 在正常并发下罕见,是防御性保留;此处主要拦【跨订单复用同交易号】(不同订单锁,
// 能并发到此并撞 uk_txn,重读到他订单已提交流水 → txnDupConflict 禁止结算本单)。
func insertWinningTxn(ctx context.Context, r store.Repos, o *models.Order, in ConfirmInput, attempt *models.PaymentAttempt) (txnInsertResult, error) {
	err := r.Transactions().Insert(ctx, payTxn(o.AppID, o.OrderNo, in, attempt, false, ""))
	if err == nil {
		return txnInserted, nil
	}
	if !errors.Is(err, store.ErrDuplicate) {
		return txnInserted, err
	}
	existing, gerr := r.Transactions().GetByTxnID(ctx, in.Channel, in.PayAccountID, in.TransactionID)
	if gerr != nil {
		return txnInserted, gerr
	}
	if existing == nil || existing.OrderNo != o.OrderNo || existing.Amount != in.Amount || existing.PayAccountID != in.PayAccountID {
		return txnDupConflict, nil // 该交易号已属其它订单/金额/账号 → 禁止结算本订单
	}
	return txnDupConsistent, nil
}

// closeOtherActiveAttempts 把除胜出尝试外的其它活跃尝试转 CLOSING(已提交网关,由查单 worker 确认,§7)。
// 逐个行锁重读后再改:ListByOrder 的无锁快照整行回写会抹掉 worker 刚打的租约与调度字段。
func closeOtherActiveAttempts(ctx context.Context, r store.Repos, orderNo string, winner *models.PaymentAttempt) error {
	list, err := r.Attempts().ListByOrder(ctx, orderNo)
	if err != nil {
		return err
	}
	for _, a := range list {
		if a.ID == winner.ID || !a.Status.Active() {
			continue
		}
		cur, err := r.Attempts().GetByOutTradeNo(ctx, a.OutTradeNo, true)
		if err != nil {
			return err
		}
		if cur == nil || !cur.Status.Active() {
			continue
		}
		cur.Status = models.AttemptClosing
		if err := r.Attempts().Update(ctx, cur); err != nil {
			return err
		}
	}
	return nil
}
