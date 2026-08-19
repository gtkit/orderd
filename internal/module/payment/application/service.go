// Package application(payment)实现支付主链:支付准备(§8/§9)与支付成功确认(§7)。
//
// 边界:只依赖 store 端口 / paygateway / account / alert,不 import repository/gorm/gin。
// 铁律:网关调用绝不在 DB 事务内(持锁期间做网络调用会放大锁竞争);事务只做本地读改写。
package application

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/gtkit/orderd/internal/models"
	eventapp "github.com/gtkit/orderd/internal/module/event/application"
	"github.com/gtkit/orderd/internal/paygateway"
	"github.com/gtkit/orderd/internal/pkg/sqlutil"
	"github.com/gtkit/orderd/internal/shared/account"
	"github.com/gtkit/orderd/internal/shared/alert"
	"github.com/gtkit/orderd/internal/store"
)

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

// PreparePayment 为已存在的 PENDING 订单(重新)调起支付。
//
// 阶段化(网关调用不在事务内):
//
//	A(tx):锁订单确认 PENDING → 决定复用/换渠道/新建尝试 → 尝试置 REQUESTING(标记已提交网关);
//	B     :调网关下单(不持锁);
//	C(tx):复核订单仍 PENDING → 尝试置 READY + prepay。
func (s *Service) PreparePayment(ctx context.Context, in PrepareInput) (PrepareOutcome, error) {
	if !in.PayMethod.Valid() {
		return PrepareOutcome{}, fmt.Errorf("%w: 支付方式非法", models.ErrIllegalTransition)
	}
	channel := in.PayMethod.Channel()

	// --- 阶段 A ---
	var (
		attemptNo  int32
		outTradeNo string
		payAcct    string
		reused     bool
		expireAt   time.Time
		amount     int64
		subject    string
		currency   string
	)
	err := s.d.Store.WithinTx(ctx, func(ctx context.Context, r store.Repos) error {
		o, err := r.Orders().GetByOrderNo(ctx, in.AppID, in.OrderNo, true)
		if err != nil {
			return err
		}
		if o.UserID != in.UserID {
			return models.ErrOrderNotOwned
		}
		if o.Status != models.OrderPending {
			return models.ErrOrderNotPending
		}
		// 金额/标题/币种来自订单权威快照,传给网关(评审 P0:不可漏传导致 0 金额空标题)。
		amount, subject, currency = o.PayAmount, o.ProductTitle, sqlutil.OrDefault(o.Currency, "CNY")
		active, err := r.Attempts().GetActiveByOrder(ctx, in.OrderNo, true)
		if err != nil {
			return err
		}
		if active != nil && active.Channel == channel {
			// 复用当前活跃尝试(同渠道):重新驱动到 REQUESTING 以便再取 prepay。
			if active.Status == models.AttemptCreated || active.Status == models.AttemptReady || active.Status == models.AttemptUnknown {
				active.Status = models.AttemptRequesting
				if err := r.Attempts().Update(ctx, active); err != nil {
					return err
				}
			}
			attemptNo, outTradeNo, payAcct, expireAt, reused = active.AttemptNo, active.OutTradeNo, active.PayAccountID, active.ExpireAt, true
			return nil
		}
		if active != nil {
			// 换渠道:活跃尝试若已提交网关(结果未知/关单中),不能并发新建 → 等 reconcile 关闭(评审 #5)。
			if active.Status.SubmittedToGateway() {
				return fmt.Errorf("%w: 存在已提交网关的活跃尝试,换渠道需待其关闭", models.ErrActiveAttemptExists)
			}
			// 未提交(CREATED/READY):可安全关闭以腾出活跃槽。
			active.Status = models.AttemptClosed
			if err := r.Attempts().Update(ctx, active); err != nil {
				return err
			}
		}
		// 解析新渠道账号并新建尝试。
		acct, err := s.d.Accounts.Resolve(ctx, in.AppID, channel)
		if err != nil {
			return err
		}
		cnt, err := r.Attempts().CountByOrder(ctx, in.OrderNo)
		if err != nil {
			return err
		}
		na := &models.PaymentAttempt{
			AppID: in.AppID, OrderNo: in.OrderNo, AttemptNo: int32(cnt + 1),
			Channel: channel, PayAccountID: acct.PayAccountID, ConfigVersion: acct.ConfigVersion,
			OutTradeNo: s.d.OutTradeNo(), Status: models.AttemptRequesting,
			ExpireAt: s.d.Clock().Add(s.d.AttemptTTL),
		}
		if err := r.Attempts().Insert(ctx, na); err != nil {
			return err
		}
		attemptNo, outTradeNo, payAcct, expireAt, reused = na.AttemptNo, na.OutTradeNo, na.PayAccountID, na.ExpireAt, false
		return nil
	})
	if err != nil {
		return PrepareOutcome{}, err
	}

	// --- 阶段 B:调网关(不持锁,强制超时)---
	gw, err := s.d.Router.Resolve(channel, payAcct)
	if err != nil {
		s.markAttemptUnknown(outTradeNo)
		return PrepareOutcome{}, err
	}
	gctx, cancel := context.WithTimeout(ctx, s.gwTimeout())
	defer cancel()
	pr, err := gw.Prepare(gctx, paygateway.PrepareInput{
		Channel: channel, PayAccountID: payAcct, OutTradeNo: outTradeNo,
		Amount: amount, Subject: subject, Currency: currency, ClientIP: in.ClientIP, ExpireAt: expireAt,
	})
	if err != nil {
		s.markAttemptUnknown(outTradeNo) // 结果未知 → 交由查单 worker
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
		a, err := r.Attempts().GetByOutTradeNo(ctx, outTradeNo, false)
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
	return PrepareOutcome{AttemptNo: attemptNo, AppParams: pr.AppParams, Status: models.OrderPending, ExpireAt: expireAt, Reused: reused}, nil
}

// markAttemptUnknown 网关下单失败/超时后,把该尝试置为 UNKNOWN(结果未知),交查单 worker 收敛。
// 用与请求解耦的 ctx(评审 P0:失败原因常是请求超时/取消,复用原 ctx 会让这次 DB 更新也失败,
// 尝试永久卡在 REQUESTING 且查单 worker 不扫描)。
func (s *Service) markAttemptUnknown(outTradeNo string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = s.d.Store.WithinTx(ctx, func(ctx context.Context, r store.Repos) error {
		a, err := r.Attempts().GetByOutTradeNo(ctx, outTradeNo, false)
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
}

// ConfirmPaySuccess 在单事务内完成 §7,并遵守两条资金铁律:
//   - 资金真相必落库:第二笔真实付款/金额账号不符的真实交易,追加 anomaly 流水后提交(不回滚丢证),
//     再事务外告警(评审 #4);
//   - CAS 唯一成功:仅一笔跃迁订单到 PAID,胜出后关其它活跃尝试并写成功账号快照(评审 #6)。
func (s *Service) ConfirmPaySuccess(ctx context.Context, in ConfirmInput) (ConfirmResult, error) {
	if in.Amount <= 0 {
		return ConfirmResult{}, fmt.Errorf("%w: 确认金额必须为正", models.ErrInvalidAmount)
	}
	var res ConfirmResult
	var alertOrderNo string
	err := s.d.Store.WithinTx(ctx, func(ctx context.Context, r store.Repos) error {
		// 权威身份由 out_trade_no 唯一派生(评审 P0):不信任回调传入的 order_no/app_id。
		attempt, err := r.Attempts().GetByOutTradeNo(ctx, in.OutTradeNo, false)
		if err != nil {
			return err
		}
		if attempt == nil {
			// 未知商户单号的真实通知:无法安全绑定订单 → 拒绝结算 + 高等级告警(不静默)。
			res = ConfirmResult{Outcome: "anomaly", Anomaly: true, AnomalyKind: "unknown_out_trade_no"}
			return nil
		}
		alertOrderNo = attempt.OrderNo
		o, err := r.Orders().GetByOrderNo(ctx, attempt.AppID, attempt.OrderNo, true)
		if err != nil {
			return err
		}

		// 通知路由到的渠道/账号必须与该尝试冻结的一致;不一致=错误路由/映射 → anomaly 落库不结算。
		if in.Channel != attempt.Channel || in.PayAccountID != attempt.PayAccountID {
			if err := s.insertAnomalyTxn(ctx, r, o, in, attempt, models.AnomalyKindAccountChannelMismatch); err != nil {
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
			// 置 PAID 只释放 active_order 槽位、不申请,不会触发唯一守卫冲突。
			if attempt.Status.Active() {
				attempt.Status = models.AttemptPaid
				if err := r.Attempts().Update(ctx, attempt); err != nil {
					return err
				}
			}
			res = ConfirmResult{Outcome: "duplicate"}
			return nil
		}

		mismatch := in.Amount != o.PayAmount || (in.Currency != "" && in.Currency != o.Currency)

		// 2) 订单状态不在结算白名单(含 PAID/退款域/已取消):第二笔真实付款 → 落 anomaly 流水,
		// 不改订单(评审 #4)。准入用 CanSettlePayment 而非状态机全表:REFUNDING→PAID 是
		// 退款失败回退专用边,支付确认借道会把退款中订单错误拉回 PAID(覆盖快照+二次发事件)。
		if !models.CanSettlePayment(o.Status) {
			if err := s.insertAnomalyTxn(ctx, r, o, in, attempt, models.AnomalyKindSecondRealPayment); err != nil {
				return err
			}
			if attempt.Status.Active() {
				attempt.Status = models.AttemptPaid
				if err := r.Attempts().Update(ctx, attempt); err != nil {
					return err
				}
			}
			res = ConfirmResult{Outcome: "anomaly", Anomaly: true, AnomalyKind: models.AnomalyKindSecondRealPayment}
			return nil
		}

		// 3) 金额/币种不符的真实交易:落 anomaly 流水,不改订单。
		if mismatch {
			if err := s.insertAnomalyTxn(ctx, r, o, in, attempt, models.AnomalyKindAmountMismatch); err != nil {
				return err
			}
			res = ConfirmResult{Outcome: "anomaly", Anomaly: true, AnomalyKind: models.AnomalyKindAmountMismatch}
			return nil
		}

		// 4) 正常胜出:落流水(冲突重读比对,评审 P0)+ 尝试 PAID + 关其它活跃 + 订单 CAS→PAID + 快照 + 事件。
		ins, err := s.insertWinningTxn(ctx, r, o, in, attempt)
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
		if err := s.closeOtherActiveAttempts(ctx, r, o.OrderNo, attempt); err != nil {
			return err
		}

		prev := o.Status
		o.Status = models.OrderPaid
		o.AggregateVersion++
		o.TradeNo = in.TransactionID
		o.PaidAt = sqlutil.Time(in.PaidAt)
		// 成功快照:渠道/商户单号/账号全取胜出尝试(退款以此为权威,评审 #6 / P1)。
		o.SettledAttemptID = sqlutil.Int64(int64(attempt.ID))
		o.SettledChannel = attempt.Channel
		o.SettledOutTradeNo = attempt.OutTradeNo
		o.SettledPayAccountID = attempt.PayAccountID
		o.SettledConfigVersion = attempt.ConfigVersion
		if acct, aerr := s.d.Accounts.ResolveByID(ctx, attempt.PayAccountID); aerr == nil {
			o.SettledMchID = acct.MchID
			o.SettledPayAppID = acct.PayAppID
		}
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
		res = ConfirmResult{Outcome: "paid"}
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
	return res, nil
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
		a, err := r.Attempts().GetByOutTradeNo(ctx, outTradeNo, false)
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
		a, err := r.Attempts().GetByOutTradeNo(ctx, outTradeNo, false)
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
	return s.d.Store.WithinTx(ctx, func(ctx context.Context, r store.Repos) error {
		a, err := r.Attempts().GetByOutTradeNo(ctx, outTradeNo, false)
		if err != nil || a == nil {
			return err
		}
		if a.Status == models.AttemptUnknown || a.Status == models.AttemptClosing {
			a.Status = models.AttemptClosed
			if err := r.Attempts().Update(ctx, a); err != nil {
				return err
			}
		}
		o, err := r.Orders().GetByOrderNo(ctx, a.AppID, a.OrderNo, true)
		if err != nil {
			return err
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
		prev := o.Status
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
		return eventapp.WriteEvent(ctx, r, o.AppID, o.OrderNo, env)
	})
}

// payTxn 由权威订单 o + 通知 in + 胜出尝试 attempt 组装一条支付流水。
// anomalyKind:异常成因(models.AnomalyKind*),正常流水传空串——处置方式因成因而异,必须随流水落库。
func payTxn(o *models.Order, in ConfirmInput, attempt *models.PaymentAttempt, anomaly bool, anomalyKind string) *models.PaymentTransaction {
	t := &models.PaymentTransaction{
		AppID: o.AppID, OrderNo: o.OrderNo, PayAccountID: in.PayAccountID,
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
func (s *Service) insertAnomalyTxn(ctx context.Context, r store.Repos, o *models.Order, in ConfirmInput, attempt *models.PaymentAttempt, anomalyKind string) error {
	if err := r.Transactions().Insert(ctx, payTxn(o, in, attempt, true, anomalyKind)); err != nil && !errors.Is(err, store.ErrDuplicate) {
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

// insertWinningTxn 结算前插入正常流水;命中唯一键时重读比对,区分"并发同笔"与"跨订单冲突"(评审 P0)。
func (s *Service) insertWinningTxn(ctx context.Context, r store.Repos, o *models.Order, in ConfirmInput, attempt *models.PaymentAttempt) (txnInsertResult, error) {
	err := r.Transactions().Insert(ctx, payTxn(o, in, attempt, false, ""))
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

// closeOtherActiveAttempts 关闭除胜出尝试外的其它活跃尝试(§7)。
func (s *Service) closeOtherActiveAttempts(ctx context.Context, r store.Repos, orderNo string, winner *models.PaymentAttempt) error {
	list, err := r.Attempts().ListByOrder(ctx, orderNo)
	if err != nil {
		return err
	}
	for _, a := range list {
		if winner != nil && a.ID == winner.ID {
			continue
		}
		if a.Status.Active() {
			a.Status = models.AttemptClosing // 已提交网关的其它尝试转关单中,由 reconcile 确认
			if err := r.Attempts().Update(ctx, a); err != nil {
				return err
			}
		}
	}
	return nil
}
