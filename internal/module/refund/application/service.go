// Package application(refund)实现退款闭环(§10,V1 全额,授权关闭)。
//
// V1 双重关闭(评审 #7 措辞修正):refund.enabled=false 配置开关【或】未发放 refund:execute 权限,
// 任一不满足即不允许退款(两者都满足才放行);权限在 transport 校验,配置开关在此校验。
//
// 退款有两种来源:订单级退款(RefundOrder,驱动订单 PAID→REFUNDING→REFUNDED)与按异常流水退款
// (RefundAnomalyTxn,source_txn_id 非空,退回从未对应订单权益的真实付款,不触碰订单)。
// 查单收敛、失败回退、成功确认共用同一套路径,按来源分流。
package application

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/gtkit/orderd/internal/models"
	eventapp "github.com/gtkit/orderd/internal/module/event/application"
	"github.com/gtkit/orderd/internal/paygateway"
	"github.com/gtkit/orderd/internal/pkg/sqlutil"
	"github.com/gtkit/orderd/internal/shared/account"
	"github.com/gtkit/orderd/internal/shared/alert"
	"github.com/gtkit/orderd/internal/shared/bill"
	"github.com/gtkit/orderd/internal/store"
)

// Deps 是 refund 服务依赖。
type Deps struct {
	BillVerifier   bill.Verifier // 对账补偿专用的完整验签查单端口；nil时拒绝补偿。
	Store          store.Store
	Router         paygateway.Router
	Accounts       account.Resolver
	Alerter        alert.Alerter
	Clock          func() time.Time
	NewRefundNo    func() string
	NewEventID     func() (string, error)
	Enabled        bool          // refund.enabled 配置开关(V1=false)
	GatewayTimeout time.Duration // 单次网关调用强制超时(评审 P1)
	// NotExistMaxQueries:退款查单连续"网关无此退款单"达此次数 → ABNORMAL 转人工核实,
	// 始终保留活跃退款守卫(无法仅凭未查到证明资金未退)。0 取默认 6。
	// 与查单 worker 的 MaxQueries(滞留告警阈值,只告警不落终态)语义不同,不复用。
	NotExistMaxQueries int32
}

// Service 退款应用服务。
type Service struct{ d Deps }

// New 构造退款服务。
func New(d Deps) *Service {
	if d.Clock == nil {
		d.Clock = time.Now
	}
	if d.GatewayTimeout == 0 {
		d.GatewayTimeout = 10 * time.Second
	}
	if d.NotExistMaxQueries == 0 {
		d.NotExistMaxQueries = 6
	}
	return &Service{d: d}
}

// gwCtx 返回与请求解耦的网关调用上下文(评审 P1:失败/回退不应因请求 ctx 取消而卡住)。
func (s *Service) gwCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), s.d.GatewayTimeout)
}

// RefundInput 退款入参。
type RefundInput struct {
	AppID        string
	OrderNo      string
	RefundAmount int64
	Reason       string
	OutRefundNo  string
	Operator     string
}

// RefundOutcome 退款结果。
type RefundOutcome struct {
	RefundNo string
	Status   models.RefundStatus
	Reused   bool
}

// refundTarget 退款落到渠道的定位:订单级退款取结算快照,按异常流水退款取源流水。
type refundTarget struct {
	channel      string
	payAccountID string
	outTradeNo   string // 原支付商户单号
	total        int64  // 原交易总额
	currency     string
}

// resolveTarget 解析退款的渠道定位(字段均为落库后不可变值,无锁读即可)。
func resolveTarget(ctx context.Context, r store.Repos, rf *models.Refund) (refundTarget, error) {
	if rf.SourceTxnID.Valid {
		t, err := r.Transactions().GetByID(ctx, uint64(rf.SourceTxnID.Int64), false)
		if err != nil {
			return refundTarget{}, err
		}
		return refundTarget{channel: t.Channel, payAccountID: t.PayAccountID, outTradeNo: t.OutTradeNo, total: t.Amount, currency: t.Currency}, nil
	}
	o, err := r.Orders().GetByOrderNo(ctx, rf.AppID, rf.OrderNo, false)
	if err != nil {
		return refundTarget{}, err
	}
	return refundTarget{
		channel: sqlutil.OrDefault(o.SettledChannel, o.PayChannel), payAccountID: rf.PayAccountID,
		outTradeNo: o.SettledOutTradeNo, total: o.PayAmount, currency: o.Currency,
	}, nil
}

// reuseRefund 按 (app_id, out_refund_no) 幂等:命中且请求哈希一致复用,不一致冲突;未命中返回 nil。
func reuseRefund(ctx context.Context, r store.Repos, appID, outRefundNo, hash string) (*RefundOutcome, error) {
	existing, err := r.Refunds().FindByOutRefundNo(ctx, appID, outRefundNo)
	if err != nil || existing == nil {
		return nil, err
	}
	if existing.RequestHash != hash {
		return nil, fmt.Errorf("%w: 同退款号绑定了不同请求", models.ErrIdempotencyConflict)
	}
	return &RefundOutcome{RefundNo: existing.RefundNo, Status: existing.Status, Reused: true}, nil
}

// RefundOrder 发起全额退款(V1)。
func (s *Service) RefundOrder(ctx context.Context, in RefundInput) (RefundOutcome, error) {
	if !s.d.Enabled {
		return RefundOutcome{}, fmt.Errorf("%w: refund.enabled=false(V1 授权关闭)", models.ErrRefundNotAllowed)
	}
	if in.OutRefundNo == "" {
		return RefundOutcome{}, fmt.Errorf("%w: 缺少 out_refund_no", models.ErrRefundNotAllowed)
	}
	hash := models.RefundRequestHash(in.OutRefundNo, in.OrderNo, in.RefundAmount, in.Reason)
	if out, err := reuseRefund(ctx, s.d.Store.Reader(), in.AppID, in.OutRefundNo, hash); err != nil || out != nil {
		if out != nil {
			return *out, nil
		}
		return RefundOutcome{}, err
	}

	var (
		rf     *models.Refund
		tg     refundTarget
		reused *RefundOutcome
	)
	err := s.d.Store.WithinTx(ctx, func(ctx context.Context, r store.Repos) error {
		o, err := r.Orders().GetByOrderNo(ctx, in.AppID, in.OrderNo, true)
		if err != nil {
			return err
		}
		reused, err = reuseRefund(ctx, r, in.AppID, in.OutRefundNo, hash)
		if err != nil || reused != nil {
			return err
		}
		if o.Status != models.OrderPaid {
			return fmt.Errorf("%w: 订单非已支付,不能退款", models.ErrRefundNotAllowed)
		}
		if in.RefundAmount != o.PayAmount {
			return fmt.Errorf("%w: V1 仅支持全额退款", models.ErrRefundNotAllowed)
		}
		// CAS 订单 PAID→REFUNDING(与建退款记录同事务)。
		prev := o.Status
		o.Status = models.OrderRefunding
		o.AggregateVersion++
		if err := r.Orders().Update(ctx, o); err != nil {
			return err
		}
		// 退款用【成功交易】的账号+渠道+原商户单号(评审 #6/P1),非下单首账号/首渠道。
		rf = &models.Refund{
			AppID: in.AppID, RefundNo: s.d.NewRefundNo(), OutRefundNo: in.OutRefundNo, RequestHash: hash,
			OrderNo: in.OrderNo, PayAccountID: o.RefundAccountID(), RefundAmount: in.RefundAmount,
			Status: models.RefundCreated, Reason: in.Reason,
		}
		tg = refundTarget{
			channel: sqlutil.OrDefault(o.SettledChannel, o.PayChannel), payAccountID: rf.PayAccountID,
			outTradeNo: o.SettledOutTradeNo, total: o.PayAmount, currency: o.Currency,
		}
		if err := r.Refunds().Insert(ctx, rf); err != nil {
			return err // 命中 active_guard(含 ABNORMAL)→ ErrDuplicate
		}
		return eventapp.WriteLog(ctx, r, o, prev, sqlutil.OrDefault(in.Operator, "admin"), "发起退款")
	})
	if err != nil {
		if errors.Is(err, store.ErrDuplicate) {
			if out, readErr := reuseRefund(ctx, s.d.Store.Reader(), in.AppID, in.OutRefundNo, hash); readErr != nil || out != nil {
				if out != nil {
					return *out, nil
				}
				return RefundOutcome{}, readErr
			}
			return RefundOutcome{}, fmt.Errorf("%w: 已存在活跃退款", models.ErrActiveRefundExists)
		}
		return RefundOutcome{}, err
	}
	if reused != nil {
		return *reused, nil
	}
	return s.submit(rf, tg)
}

// AnomalyRefundInput 按异常流水退款入参。
type AnomalyRefundInput struct {
	TxnID       uint64
	OutRefundNo string
	Reason      string
	Actor       string // 审计发起者(取自 token)
}

// RefundAnomalyTxn 按资金异常流水全额退款:退到该流水实际入账的渠道账号与原商户单号,不触碰订单。
// 同一流水同时最多一笔在途退款(活跃守卫 'txn:<id>'),成功后不可再退(源流水行锁内判定);发起与审计同事务。
func (s *Service) RefundAnomalyTxn(ctx context.Context, in AnomalyRefundInput) (RefundOutcome, error) {
	if !s.d.Enabled {
		return RefundOutcome{}, fmt.Errorf("%w: refund.enabled=false", models.ErrRefundNotAllowed)
	}
	t0, err := s.d.Store.Reader().Transactions().GetByID(ctx, in.TxnID, false)
	if err != nil {
		return RefundOutcome{}, err
	}
	// 流水不可变:金额与归属直接取无锁读的值;哈希绑定源流水,同退款号换流水视为冲突。
	hash := models.RefundRequestHash(in.OutRefundNo, "txn:"+strconv.FormatUint(in.TxnID, 10), t0.Amount, in.Reason)
	if out, err := reuseRefund(ctx, s.d.Store.Reader(), t0.AppID, in.OutRefundNo, hash); err != nil || out != nil {
		if out != nil {
			return *out, nil
		}
		return RefundOutcome{}, err
	}

	var rf *models.Refund
	var reused *RefundOutcome
	err = s.d.Store.WithinTx(ctx, func(ctx context.Context, r store.Repos) error {
		t, err := r.Transactions().GetByID(ctx, in.TxnID, true) // 行锁:串行化以该流水为源的退款发起
		if err != nil {
			return err
		}
		reused, err = reuseRefund(ctx, r, t.AppID, in.OutRefundNo, hash)
		if err != nil || reused != nil {
			return err
		}
		if !t.Anomaly || t.Kind != models.TxnPay {
			return fmt.Errorf("%w: 仅资金异常的支付流水可按流水退款", models.ErrRefundNotAllowed)
		}
		prior, err := r.Refunds().ListBySourceTxn(ctx, t.ID)
		if err != nil {
			return err
		}
		for _, p := range prior {
			if p.Status != models.RefundFailed && p.Status != models.RefundClosed {
				return fmt.Errorf("%w: 该流水已有在途或已完成的退款 %s", models.ErrActiveRefundExists, p.RefundNo)
			}
		}
		rf = &models.Refund{
			AppID: t.AppID, RefundNo: s.d.NewRefundNo(), OutRefundNo: in.OutRefundNo, RequestHash: hash,
			OrderNo: t.OrderNo, SourceTxnID: sqlutil.Int64(int64(t.ID)), PayAccountID: t.PayAccountID,
			RefundAmount: t.Amount, Status: models.RefundCreated, Reason: in.Reason,
		}
		if err := r.Refunds().Insert(ctx, rf); err != nil {
			return err
		}
		return r.OpsAudit().Insert(ctx, &models.OpsAuditLog{
			Actor: in.Actor, Action: models.OpsActionAnomalyTxnRefund,
			TargetType: models.OpsTargetPaymentTxn, TargetID: strconv.FormatUint(t.ID, 10),
			Detail: fmt.Sprintf("refund_no=%s out_refund_no=%s amount=%d kind=%s order=%s reason=%q",
				rf.RefundNo, in.OutRefundNo, t.Amount, t.AnomalyKind, t.OrderNo, in.Reason),
		})
	})
	if err != nil {
		if errors.Is(err, store.ErrDuplicate) {
			if out, readErr := reuseRefund(ctx, s.d.Store.Reader(), t0.AppID, in.OutRefundNo, hash); readErr != nil || out != nil {
				if out != nil {
					return *out, nil
				}
				return RefundOutcome{}, readErr
			}
			return RefundOutcome{}, fmt.Errorf("%w: 该流水已存在活跃退款或退款号已被占用", models.ErrActiveRefundExists)
		}
		return RefundOutcome{}, err
	}
	if reused != nil {
		return *reused, nil
	}
	return s.submit(rf, refundTarget{
		channel: t0.Channel, payAccountID: t0.PayAccountID, outTradeNo: t0.OutTradeNo, total: t0.Amount, currency: t0.Currency,
	})
}

// submit 向渠道提交已落库(CREATED)的退款并按受理结果推进(不在事务内,强制超时;失败/回退用解耦 ctx)。
func (s *Service) submit(rf *models.Refund, tg refundTarget) (RefundOutcome, error) {
	gctx, cancel := s.gwCtx()
	defer cancel()
	gw, gerr := s.d.Router.Resolve(tg.channel, tg.payAccountID)
	if gerr != nil {
		// 本地路由失败:请求从未发出,确定失败。
		s.failRefund(rf.RefundNo, gerr.Error())
		return RefundOutcome{}, gerr
	}
	rr, gerr := gw.Refund(gctx, paygateway.RefundInput{
		Channel: tg.channel, PayAccountID: tg.payAccountID, OutTradeNo: tg.outTradeNo, OutRefundNo: rf.OutRefundNo,
		RefundAmount: rf.RefundAmount, TotalAmount: tg.total, Currency: sqlutil.OrDefault(tg.currency, "CNY"),
	})
	if gerr != nil {
		// 结果未知(超时/网络/未知错误):渠道可能已受理,绝不判失败——错判会造成
		// "钱已退、本地 FAILED、订单回 PAID、refunded 事件不发"的账实与权益双错。
		// 置 PROCESSING 交退款查单 worker 收敛;连续 NotExist 达阈值仍须转人工保留守卫。
		s.markRefundProcessing(rf.RefundNo, "", false)
		return RefundOutcome{RefundNo: rf.RefundNo, Status: models.RefundProcessing}, nil
	}
	if !rr.Accepted {
		s.failRefund(rf.RefundNo, "网关未受理退款")
		return RefundOutcome{RefundNo: rf.RefundNo, Status: models.RefundFailed}, nil
	}
	s.markRefundProcessing(rf.RefundNo, rr.GatewayRefundID, true)
	return RefundOutcome{RefundNo: rf.RefundNo, Status: models.RefundProcessing}, nil
}

// markRefundProcessing 把退款推进到 PROCESSING 并设查单时间(网关已受理带渠道退款单号;
// 结果未知传空串)。用解耦 ctx,不吞错误;更新失败告警,退款留在 CREATED,
// 由退款查单 worker 的 CREATED 滞留恢复扫描兜底进入查单收敛。
func (s *Service) markRefundProcessing(refundNo, gatewayRefundID string, confirmed bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := s.d.Store.WithinTx(ctx, func(ctx context.Context, r store.Repos) error {
		rf, err := r.Refunds().GetByRefundNo(ctx, refundNo, true) // 行锁:只写退款单行,与并发写回串行
		if err != nil {
			return err
		}
		if rf.Status != models.RefundCreated && rf.Status != models.RefundProcessing {
			return nil // 已终态,不回写
		}
		if confirmed {
			rf.GatewayConfirmed = true
			rf.NotExistCount = 0
		}
		rf.Status = models.RefundProcessing
		if gatewayRefundID != "" {
			rf.GatewayRefundID = gatewayRefundID
		}
		if !rf.NextQueryAt.Valid {
			rf.NextQueryAt = sqlutil.Time(s.d.Clock().Add(10 * time.Second))
		}
		return r.Refunds().Update(ctx, rf)
	})
	if err != nil {
		s.d.Alerter.Anomaly(context.Background(), "refund_processing_update_failed",
			map[string]any{"refund_no": refundNo, "err": err.Error()})
	}
}

// lockRefund 统一锁序加锁:先无锁读取不可变的来源与 order_no;订单级退款先锁订单再锁退款
// (与全部"订单→子行"路径同序,消除死锁环),按异常流水退款只锁退款行(不触碰订单)。
// 订单级退款返回的 o 非 nil。
func lockRefund(ctx context.Context, r store.Repos, refundNo string) (*models.Order, *models.Refund, error) {
	rf0, err := r.Refunds().GetByRefundNo(ctx, refundNo, false)
	if err != nil {
		return nil, nil, err
	}
	var o *models.Order
	if !rf0.SourceTxnID.Valid {
		if o, err = r.Orders().GetByOrderNo(ctx, rf0.AppID, rf0.OrderNo, true); err != nil {
			return nil, nil, err
		}
	}
	rf, err := r.Refunds().GetByRefundNo(ctx, refundNo, true)
	if err != nil {
		return nil, nil, err
	}
	return o, rf, nil
}

// failRefund 网关退款失败:退款 → FAILED;订单级退款同时回退订单 REFUNDING→PAID(DESIGN §10)。
// 用解耦 ctx,不吞错误。
func (s *Service) failRefund(refundNo, reason string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := s.d.Store.WithinTx(ctx, func(ctx context.Context, r store.Repos) error {
		o, rf, err := lockRefund(ctx, r, refundNo)
		if err != nil {
			return err
		}
		return failRefundLocked(ctx, r, o, rf, reason)
	})
	if err != nil {
		s.d.Alerter.Anomaly(context.Background(), "refund_fail_rollback_failed",
			map[string]any{"refund_no": refundNo, "err": err.Error()})
	}
}

func failRefundLocked(ctx context.Context, r store.Repos, o *models.Order, rf *models.Refund, reason string) error {
	if rf.Status != models.RefundCreated && rf.Status != models.RefundProcessing {
		return nil
	}
	rf.Status = models.RefundFailed
	rf.LastError = sqlutil.Str(reason)
	if err := r.Refunds().Update(ctx, rf); err != nil {
		return err
	}
	if o != nil && o.Status == models.OrderRefunding {
		prev := o.Status
		o.Status = models.OrderPaid
		o.AggregateVersion++
		if err := r.Orders().Update(ctx, o); err != nil {
			return err
		}
		return eventapp.WriteLog(ctx, r, o, prev, "system", "退款失败回退")
	}
	return nil
}

// abnormalRefundLocked 的调用方已按订单→退款加锁并复核在途状态。
func abnormalRefundLocked(ctx context.Context, r store.Repos, o *models.Order, rf *models.Refund, reason string) error {
	rf.Status = models.RefundAbnormal
	rf.LastError = sqlutil.Str(reason)
	if err := r.Refunds().Update(ctx, rf); err != nil {
		return err
	}
	if o != nil && o.Status == models.OrderRefunding {
		prev := o.Status
		o.Status = models.OrderRefundException
		o.AggregateVersion++
		if err := r.Orders().Update(ctx, o); err != nil {
			return err
		}
		return eventapp.WriteLog(ctx, r, o, prev, "system", reason)
	}
	return nil
}

// recordRefundTxn 写退款成功不可变流水(kind=2,回指 refund_no;渠道用退款实际落地渠道)。
// 支付宝没有独立渠道退款号,用 refund:<out_refund_no> 区分资金腿;微信取渠道退款号(空则回退请求号)。
// 唯一冲突仅在完整资金身份一致时视为幂等,避免吞掉支付或另一笔退款。
func recordRefundTxn(ctx context.Context, r store.Repos, rf *models.Refund, tg refundTarget, gatewayRefundID, digest string, amount int64, at time.Time) error {
	txnID := sqlutil.OrDefault(gatewayRefundID, rf.OutRefundNo)
	if tg.channel == "alipay" {
		txnID = "refund:" + rf.OutRefundNo
	}
	txn := &models.PaymentTransaction{
		AppID: rf.AppID, OrderNo: rf.OrderNo, RefundNo: rf.RefundNo, PayAccountID: rf.PayAccountID,
		Channel: tg.channel, Kind: models.TxnRefund, OutTradeNo: rf.OutRefundNo,
		TransactionID: txnID, Amount: amount,
		Currency: sqlutil.OrDefault(tg.currency, "CNY"), PaidAt: sqlutil.Time(at), NotifyDigest: digest,
	}
	err := r.Transactions().Insert(ctx, txn)
	if !errors.Is(err, store.ErrDuplicate) {
		return err
	}
	existing, err := r.Transactions().GetByTxnID(ctx, tg.channel, rf.PayAccountID, txnID, true)
	if err != nil {
		return err
	}
	if existing == nil || existing.Kind != models.TxnRefund || existing.AppID != txn.AppID ||
		existing.OrderNo != txn.OrderNo || existing.RefundNo != txn.RefundNo || existing.OutTradeNo != txn.OutTradeNo ||
		existing.Channel != txn.Channel || existing.PayAccountID != txn.PayAccountID ||
		existing.Amount != txn.Amount || existing.Currency != txn.Currency {
		return fmt.Errorf("%w: 退款流水身份冲突 refund=%s txn=%s", models.ErrPaymentAnomaly, rf.RefundNo, txnID)
	}
	return nil
}

// markOrderRefunded 订单 from→REFUNDED + 累计已退 + 日志 + order.refunded 事件(订单级退款成功落定)。
func (s *Service) markOrderRefunded(ctx context.Context, r store.Repos, o *models.Order, rf *models.Refund, amount int64, operator, remark string) error {
	prev := o.Status
	o.Status = models.OrderRefunded
	o.RefundedAmount += amount
	o.AggregateVersion++
	if err := r.Orders().Update(ctx, o); err != nil {
		return err
	}
	if err := eventapp.WriteLog(ctx, r, o, prev, operator, remark); err != nil {
		return err
	}
	eid, err := s.d.NewEventID()
	if err != nil {
		return err
	}
	return eventapp.WriteEvent(ctx, r, o.AppID, o.OrderNo, models.BuildRefundedEnvelope(o, rf, prev, eid, s.d.Clock()))
}

// ConfirmRefundSuccess 退款成功单事务(与支付成功同等级,评审 #7):
// 网关确认退款成功 → 退款 SUCCEEDED + 不可变退款流水;订单级退款同时 REFUNDING→REFUNDED + 累计已退
// + order.refunded 事件。
// 金额不符 → 同事务落人工终态(退款 ABNORMAL;订单级退款的订单 REFUND_EXCEPTION),事务外告警,返回成功。
func (s *Service) ConfirmRefundSuccess(ctx context.Context, refundNo, gatewayRefundID string, amount int64, at time.Time) error {
	var (
		mismatch              bool
		conflict              bool // 已 FAILED/CLOSED 但渠道确认成功 → 资金分歧,告警人工
		wantAmount, gotAmount int64
		orderNo               string
	)
	err := s.d.Store.WithinTx(ctx, func(ctx context.Context, r store.Repos) error {
		o, rf, err := lockRefund(ctx, r, refundNo)
		if err != nil {
			return err
		}
		orderNo = rf.OrderNo
		if rf.Status == models.RefundSucceeded || rf.Status == models.RefundAbnormal {
			return nil // 幂等:成功与人工终态都不重复处置、不重复告警
		}
		if rf.Status == models.RefundFailed || rf.Status == models.RefundClosed {
			// 失败路径已抢先(如历史 NotExist 误判/渠道摆动),而此刻渠道确认成功:
			// 钱可能已退而本地已判失败 → 绝不静默,高等级告警交人工核查渠道并调账。
			// 不自动改状态(避免与可能已发起的第二笔退款撞 active_guard),把 P1 静默资损降级为可见分歧。
			conflict = true
			return nil
		}
		rf.GatewayConfirmed = true
		rf.NotExistCount = 0
		if gatewayRefundID != "" {
			rf.GatewayRefundID = gatewayRefundID
		}
		rf.SucceededAt = sqlutil.Time(at)
		// 校验成功金额与退款单金额一致(评审 P1):不一致=资金异常,同事务落人工终态——
		// 退款 ABNORMAL(活跃守卫内,人工确认前阻断再次退款)+ 订单 REFUNDING→REFUND_EXCEPTION。
		// 留在 PROCESSING 会被退款查单 worker 无限轮询;告警在事务提交后发出(不在持锁期间做外部 IO)。
		if amount != rf.RefundAmount {
			mismatch, wantAmount, gotAmount = true, rf.RefundAmount, amount
			return abnormalRefundLocked(ctx, r, o, rf, fmt.Sprintf("退款成功金额 %d 与退款单金额 %d 不符", amount, rf.RefundAmount))
		}
		tg, err := resolveTarget(ctx, r, rf)
		if err != nil {
			return err
		}
		if err := recordRefundTxn(ctx, r, rf, tg, gatewayRefundID, "", amount, at); err != nil {
			return err
		}
		rf.Status = models.RefundSucceeded
		if err := r.Refunds().Update(ctx, rf); err != nil {
			return err
		}
		if o != nil && o.Status == models.OrderRefunding {
			return s.markOrderRefunded(ctx, r, o, rf, amount, "system", "退款成功")
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, models.ErrPaymentAnomaly) {
			s.d.Alerter.Anomaly(ctx, "refund_ledger_conflict", map[string]any{"refund_no": refundNo, "err": err.Error()})
		}
		return err
	}
	if mismatch {
		s.d.Alerter.Anomaly(ctx, "refund_amount_mismatch", map[string]any{
			"refund_no": refundNo, "order_no": orderNo, "expected": wantAmount, "got": gotAmount,
		})
	}
	if conflict {
		s.d.Alerter.Anomaly(ctx, "refund_terminal_conflict", map[string]any{
			"refund_no": refundNo, "order_no": orderNo,
			"detail": "退款已判 FAILED/CLOSED 但渠道确认成功:钱可能已退而本地已判失败,人工核查渠道并调账",
		})
	}
	return nil
}

// ResolveInput ABNORMAL 退款人工落定入参。
type ResolveInput struct {
	RefundNo     string
	Target       models.RefundStatus // RefundSucceeded / RefundFailed
	ActualAmount int64               // Target=SUCCEEDED 时须等于退款单金额(双录确认)
	Remark       string
	Actor        string // 审计发起者(取自 token)
}

// ResolveOutcome 人工落定结果;HasOrder=true 时 OrderStatus 为订单落定后状态(按异常流水退款不涉及订单)。
type ResolveOutcome struct {
	Status      models.RefundStatus
	OrderStatus models.OrderStatus
	HasOrder    bool
}

// ResolveRefundAbnormal 人工核实渠道后落定 ABNORMAL 退款(与审计同事务,释放活跃退款守卫):
//   - FAILED:退款 FAILED;订单级退款的订单 REFUND_EXCEPTION→PAID(可重新发起退款);
//   - SUCCEEDED:actual_amount 须等于退款单金额;写退款流水;订单 REFUND_EXCEPTION→REFUNDED
//   - 累计已退 + order.refunded 事件。
//
// 人工操作时间由审计记录承载;渠道成功时间仅沿用已知值,未知时保留 NULL。
func (s *Service) ResolveRefundAbnormal(ctx context.Context, in ResolveInput) (ResolveOutcome, error) {
	if in.Target != models.RefundSucceeded && in.Target != models.RefundFailed {
		return ResolveOutcome{}, fmt.Errorf("%w: 人工落定目标只能是 SUCCEEDED 或 FAILED", models.ErrIllegalTransition)
	}
	var out ResolveOutcome
	err := s.d.Store.WithinTx(ctx, func(ctx context.Context, r store.Repos) error {
		o, rf, err := lockRefund(ctx, r, in.RefundNo)
		if err != nil {
			return err
		}
		if rf.Status != models.RefundAbnormal {
			return fmt.Errorf("%w: 仅 ABNORMAL 退款可人工落定(当前 status=%d)", models.ErrIllegalTransition, rf.Status)
		}
		if in.Target == models.RefundSucceeded && in.ActualAmount != rf.RefundAmount {
			return fmt.Errorf("%w: 实际退款金额 %d 须等于退款单金额 %d", models.ErrInvalidAmount, in.ActualAmount, rf.RefundAmount)
		}
		operator := sqlutil.OrDefault(in.Actor, "admin")
		detail := fmt.Sprintf("refund ABNORMAL→%d actual=%d remark=%q", in.Target, in.ActualAmount, in.Remark)
		var prevOrder models.OrderStatus
		if o != nil {
			prevOrder = o.Status
		}
		if in.Target == models.RefundFailed {
			rf.Status = models.RefundFailed
			rf.LastError = sqlutil.Str("人工落定未退款: " + in.Remark)
			if err := r.Refunds().Update(ctx, rf); err != nil {
				return err
			}
			if o != nil && o.Status == models.OrderRefundException {
				o.Status = models.OrderPaid
				o.AggregateVersion++
				if err := r.Orders().Update(ctx, o); err != nil {
					return err
				}
				if err := eventapp.WriteLog(ctx, r, o, prevOrder, operator, "异常退款人工落定:未退款"); err != nil {
					return err
				}
			}
		} else {
			tg, err := resolveTarget(ctx, r, rf)
			if err != nil {
				return err
			}
			var succeededAt time.Time
			if rf.SucceededAt.Valid {
				succeededAt = rf.SucceededAt.Time
			}
			if err := recordRefundTxn(ctx, r, rf, tg, rf.GatewayRefundID, "manual", in.ActualAmount, succeededAt); err != nil {
				return err
			}
			rf.Status = models.RefundSucceeded
			if err := r.Refunds().Update(ctx, rf); err != nil {
				return err
			}
			if o != nil && o.Status == models.OrderRefundException {
				if err := s.markOrderRefunded(ctx, r, o, rf, in.ActualAmount, operator, "异常退款人工落定:已退款"); err != nil {
					return err
				}
			}
		}
		out = ResolveOutcome{Status: rf.Status}
		if o != nil {
			out.OrderStatus, out.HasOrder = o.Status, true
			detail += fmt.Sprintf(" order %s %d→%d", o.OrderNo, prevOrder, o.Status)
		}
		return r.OpsAudit().Insert(ctx, &models.OpsAuditLog{
			Actor: in.Actor, Action: models.OpsActionRefundAbnormalSettle,
			TargetType: models.OpsTargetRefund, TargetID: in.RefundNo, Detail: detail,
		})
	})
	if err != nil {
		if errors.Is(err, models.ErrPaymentAnomaly) {
			s.d.Alerter.Anomaly(ctx, "refund_ledger_conflict", map[string]any{"refund_no": in.RefundNo, "err": err.Error()})
		}
		return ResolveOutcome{}, err
	}
	return out, nil
}

// GetRefund 按 refund_no 或 out_refund_no 查退款;多租户查询始终校验 app_id(评审 P1)。
func (s *Service) GetRefund(ctx context.Context, appID, refundNo, outRefundNo string) (*models.Refund, error) {
	r := s.d.Store.Reader().Refunds()
	if refundNo != "" {
		rf, err := r.GetByRefundNo(ctx, refundNo, false)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return nil, models.ErrRefundNotFound
			}
			return nil, err
		}
		if rf.AppID != appID { // 跨租户越权:对外等同不存在,不泄露存在性
			return nil, models.ErrRefundNotFound
		}
		return rf, nil
	}
	rf, err := r.FindByOutRefundNo(ctx, appID, outRefundNo)
	if err != nil {
		return nil, err
	}
	if rf == nil {
		return nil, models.ErrRefundNotFound
	}
	return rf, nil
}

// QueryRefund 退款查单:向网关查该退款结果并落地(退款查单 worker 调用)。
func (s *Service) QueryRefund(ctx context.Context, rf *models.Refund) error {
	tg, err := resolveTarget(ctx, s.d.Store.Reader(), rf)
	if err != nil {
		return errors.Join(err, s.applyRefundQuery(ctx, rf, paygateway.RefundQueryResult{}))
	}
	gw, err := s.d.Router.Resolve(tg.channel, rf.PayAccountID)
	if err != nil {
		return errors.Join(err, s.applyRefundQuery(ctx, rf, paygateway.RefundQueryResult{}))
	}
	gctx, cancel := context.WithTimeout(ctx, s.d.GatewayTimeout)
	defer cancel()
	qr, err := gw.RefundQuery(gctx, paygateway.RefundQueryInput{
		Channel: tg.channel, PayAccountID: rf.PayAccountID, OutRefundNo: rf.OutRefundNo, OutTradeNo: tg.outTradeNo,
	})
	if err != nil {
		return errors.Join(err, s.applyRefundQuery(ctx, rf, paygateway.RefundQueryResult{}))
	}
	if qr.State == paygateway.RefundQuerySucceeded {
		// 真实成功优先于查询租约,其自身事务负责终态幂等。
		// 缺失/零金额不能用请求金额补成成功事实,由金额核对进入人工状态。
		return s.ConfirmRefundSuccess(ctx, rf.RefundNo, qr.GatewayRefundID, qr.Amount, qr.SucceededAt)
	}
	return s.applyRefundQuery(ctx, rf, qr)
}

// applyRefundQuery 在同一事务内保存最新证据与非成功结果;旧认领不得写入新持有者的状态。
func (s *Service) applyRefundQuery(ctx context.Context, claimed *models.Refund, qr paygateway.RefundQueryResult) error {
	var anomaly string
	err := s.d.Store.WithinTx(ctx, func(ctx context.Context, r store.Repos) error {
		o, rf, err := lockRefund(ctx, r, claimed.RefundNo)
		if err != nil {
			return err
		}
		if rf.Status != models.RefundCreated && rf.Status != models.RefundProcessing {
			return nil
		}
		if claimed.LockedBy != "" && claimed.LockedBy != rf.LockedBy {
			return nil
		}
		if qr.State != paygateway.RefundQueryNotExist {
			rf.NotExistCount = 0
		}
		switch qr.State {
		case paygateway.RefundQueryProcessing, paygateway.RefundQueryAbnormal:
			rf.GatewayConfirmed = true
			if qr.GatewayRefundID != "" {
				rf.GatewayRefundID = qr.GatewayRefundID
			}
			if qr.State == paygateway.RefundQueryAbnormal {
				anomaly = "refund_channel_abnormal"
				return abnormalRefundLocked(ctx, r, o, rf, "渠道退款异常,需人工核实")
			}
		case paygateway.RefundQueryFailed:
			return failRefundLocked(ctx, r, o, rf, "网关退款失败")
		case paygateway.RefundQueryNotExist:
			rf.NotExistCount++
			if rf.NotExistCount >= s.d.NotExistMaxQueries {
				if rf.GatewayConfirmed {
					anomaly = "refund_query_evidence_conflict"
					return abnormalRefundLocked(ctx, r, o, rf, "渠道已确认退款存在,后续连续查证不存在,需人工核实")
				}
				anomaly = "refund_query_acceptance_unknown"
				return abnormalRefundLocked(ctx, r, o, rf, "渠道连续查证不存在,受理结果未知,需人工核实")
			}
		}
		return r.Refunds().Update(ctx, rf)
	})
	if err == nil && anomaly != "" {
		s.d.Alerter.Anomaly(ctx, anomaly, map[string]any{"refund_no": claimed.RefundNo, "order_no": claimed.OrderNo})
	}
	return err
}
