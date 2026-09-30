// Package application(order)实现下单、取消(§8)、查询。
// 复用 payment 应用服务完成支付准备(order → payment 单向依赖,无环)。
package application

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/gtkit/orderd/internal/models"
	eventapp "github.com/gtkit/orderd/internal/module/event/application"
	paymentapp "github.com/gtkit/orderd/internal/module/payment/application"
	"github.com/gtkit/orderd/internal/shared/account"
	"github.com/gtkit/orderd/internal/shared/alert"
	"github.com/gtkit/orderd/internal/shared/obs"
	"github.com/gtkit/orderd/internal/store"
)

// Deps 是 order 服务依赖。
type Deps struct {
	Store      store.Store
	Payment    *paymentapp.Service
	Pricing    models.PricingAuthority
	Amounts    models.AmountPolicy
	Accounts   account.Resolver
	Clock      func() time.Time
	NewOrderNo func() string
	NewToken   func() (string, error)
	NewEventID func() (string, error)
	OrderTTL   time.Duration
	// MaxPendingPerUser 单用户 PENDING 订单数上限(<=0 不限)。近似防刷阈值,非资金不变量:
	// 计数与插入非原子,并发窗口内可短暂超出。
	MaxPendingPerUser int
	// Alerter 高等级告警(CLOSING 滞留兜底触发时上报;nil → Nop)。
	Alerter alert.Alerter
	// Obs 关键路径埋点端口(nil → Nop)。
	Obs obs.FlowObserver
}

// Service 订单应用服务。
type Service struct{ d Deps }

// New 构造 order 服务。
func New(d Deps) *Service {
	if d.Clock == nil {
		d.Clock = time.Now
	}
	if d.OrderTTL == 0 {
		d.OrderTTL = 30 * time.Minute
	}
	if d.Alerter == nil {
		d.Alerter = alert.Nop{}
	}
	if d.Obs == nil {
		d.Obs = obs.Nop{}
	}
	return &Service{d: d}
}

// CreateInput 下单入参(app_id/user_id 来自鉴权上下文,非请求可伪造)。
type CreateInput struct {
	AppID           string
	UserID          int64
	ProductID       uint64
	ProductTitle    string
	BusinessType    string
	MetadataType    string
	MetadataVersion int32
	Metadata        []byte
	Amount          models.Money
	PayMethod       models.PayMethod
	ChannelID       int64
	ClientIP        string
	IdempotencyKey  string
	ClientOrderNo   string
	QuoteID         string // 定价报价号(QuotePricing 边界用)
}

// CreateOutcome 下单结果。
type CreateOutcome struct {
	OrderNo    string
	OrderToken string
	AppParams  string
	Status     models.OrderStatus
	ExpireAt   time.Time
	Reused     bool
}

// CreateOrder 幂等下单:金额权威校验 → 幂等三件套 → 冻结账号建单 → 调起首次支付。
func (s *Service) CreateOrder(ctx context.Context, in CreateInput) (CreateOutcome, error) {
	if !in.PayMethod.Valid() {
		return CreateOutcome{}, fmt.Errorf("%w: 支付方式非法", models.ErrIllegalTransition)
	}
	// 金额权威:基础不变量 + app 白名单/上下限 + 定价可信边界(评审 #8)。
	if err := models.ValidateAmount(ctx, s.d.Amounts, in.AppID, in.Amount, in.BusinessType); err != nil {
		return CreateOutcome{}, err
	}
	if err := s.d.Pricing.Authorize(in.AppID, in.Amount, in.QuoteID); err != nil {
		return CreateOutcome{}, err
	}

	hash := models.HashInput{
		AppID: in.AppID, UserID: in.UserID, ProductID: in.ProductID, ProductTitle: in.ProductTitle,
		BusinessType: in.BusinessType, Original: in.Amount.Original, Discount: in.Amount.Discount,
		Payable: in.Amount.Payable, Currency: in.Amount.Currency, PayMethod: in.PayMethod,
		ChannelID: in.ChannelID, ClientOrderNo: in.ClientOrderNo,
		MetadataType: in.MetadataType, MetadataVersion: in.MetadataVersion, Metadata: in.Metadata,
	}.RequestHash()

	idemKey := models.NormalizeKey(in.IdempotencyKey)

	// 幂等命中:同 key 同 hash 复用;同 key 异 hash 冲突。
	if idemKey.Valid {
		if existing, err := s.d.Store.Reader().Orders().FindByIdempotencyKey(ctx, in.AppID, idemKey.String); err != nil {
			return CreateOutcome{}, err
		} else if existing != nil {
			return s.reuse(ctx, existing, hash, in)
		}
	}

	acct, err := s.d.Accounts.Resolve(ctx, in.AppID, in.PayMethod.Channel())
	if err != nil {
		return CreateOutcome{}, err
	}
	token, err := s.d.NewToken()
	if err != nil {
		return CreateOutcome{}, err
	}
	now := s.d.Clock()
	o := &models.Order{
		AppID: in.AppID, IdempotencyKey: idemKey, ClientOrderNo: models.NormalizeKey(in.ClientOrderNo),
		OrderNo: s.d.NewOrderNo(), OrderToken: token, UserID: in.UserID, Status: models.OrderPending,
		ProductID: in.ProductID, ProductTitle: in.ProductTitle, BusinessType: in.BusinessType,
		MetadataType: in.MetadataType, MetadataVersion: in.MetadataVersion, Metadata: in.Metadata,
		Currency: in.Amount.Currency, OriginalPrice: in.Amount.Original, DiscountAmount: in.Amount.Discount,
		PayAmount: in.Amount.Payable, PayMethod: in.PayMethod, PayChannel: in.PayMethod.Channel(),
		PayAccountID: acct.PayAccountID, PayMchID: acct.MchID, PayAppID: acct.PayAppID, ConfigVersion: acct.ConfigVersion,
		ChannelID: in.ChannelID, ClientIPStr: in.ClientIP, ExpireAt: now.Add(s.d.OrderTTL),
	}
	if idemKey.Valid {
		o.RequestHash = models.NormalizeKey(hash)
	}

	insertErr := s.d.Store.WithinTx(ctx, func(ctx context.Context, r store.Repos) error {
		// 单用户未付订单数上限(近似,事务内计数使窗口最小;超限在网关调用之前拒绝,零副作用)。
		if s.d.MaxPendingPerUser > 0 {
			n, err := r.Orders().CountPendingByUser(ctx, in.AppID, in.UserID, now)
			if err != nil {
				return err
			}
			if n >= s.d.MaxPendingPerUser {
				return fmt.Errorf("%w: 已有 %d 笔待支付订单(上限 %d)", models.ErrTooManyPendingOrders, n, s.d.MaxPendingPerUser)
			}
		}
		return r.Orders().Insert(ctx, o)
	})
	if insertErr != nil {
		// 并发同幂等键:一方胜出,另一方命中唯一键 → 重新按幂等语义收敛(复用或冲突)。
		if errors.Is(insertErr, store.ErrDuplicate) {
			if idemKey.Valid {
				existing, ferr := s.d.Store.Reader().Orders().FindByIdempotencyKey(ctx, in.AppID, idemKey.String)
				if ferr != nil {
					return CreateOutcome{}, ferr
				}
				if existing != nil {
					return s.reuse(ctx, existing, hash, in)
				}
			}
			// 幂等键路径无法复用:client_order_no 归一化非空(空串/空白落库为 NULL,不参与唯一键)
			// 时冲突来自 uk_app_client_order(业务单号被其它订单占用),返回可判定错误,
			// 调用方据此查旧单或换单号;否则只可能是 order_no/order_token 罕见碰撞,
			// 按未分类错误返回(重试即换号)。
			if o.ClientOrderNo.Valid {
				return CreateOutcome{}, fmt.Errorf("%w: client_order_no=%q", models.ErrClientOrderNoConflict, o.ClientOrderNo.String)
			}
		}
		return CreateOutcome{}, insertErr
	}

	// 调起首次支付(网关调用在 payment 服务内,不在建单事务里)。
	out, err := s.d.Payment.PreparePayment(ctx, paymentapp.PrepareInput{
		AppID: in.AppID, OrderNo: o.OrderNo, UserID: in.UserID,
		PayMethod: in.PayMethod, ChannelID: in.ChannelID, ClientIP: in.ClientIP,
	})
	if err != nil {
		return CreateOutcome{}, err
	}
	return CreateOutcome{
		OrderNo: o.OrderNo, OrderToken: token, AppParams: out.AppParams,
		Status: models.OrderPending, ExpireAt: o.ExpireAt, Reused: false,
	}, nil
}

// reuse 处理幂等复用/冲突:同 hash 复用(重新调起支付),异 hash 冲突。
func (s *Service) reuse(ctx context.Context, existing *models.Order, hash string, in CreateInput) (CreateOutcome, error) {
	if !existing.RequestHash.Valid || existing.RequestHash.String != hash {
		return CreateOutcome{}, fmt.Errorf("%w: 同幂等键绑定了不同请求", models.ErrIdempotencyConflict)
	}
	out := CreateOutcome{
		OrderNo: existing.OrderNo, OrderToken: existing.OrderToken,
		Status: existing.Status, ExpireAt: existing.ExpireAt, Reused: true,
	}
	// 仅 PENDING 单可重新调起支付;已支付/终态直接返回状态。
	if existing.Status == models.OrderPending {
		p, err := s.d.Payment.PreparePayment(ctx, paymentapp.PrepareInput{
			AppID: existing.AppID, OrderNo: existing.OrderNo, UserID: in.UserID,
			PayMethod: in.PayMethod, ChannelID: in.ChannelID, ClientIP: in.ClientIP,
		})
		if err != nil {
			return CreateOutcome{}, err
		}
		out.AppParams = p.AppParams
	}
	return out, nil
}

// PreparePayment 换渠道 / 重新调起支付(委托 payment 服务;供 PreparePayment RPC 用)。
func (s *Service) PreparePayment(ctx context.Context, appID string, userID int64, orderNo string, pm models.PayMethod, channelID int64, clientIP string) (paymentapp.PrepareOutcome, error) {
	return s.d.Payment.PreparePayment(ctx, paymentapp.PrepareInput{
		AppID: appID, OrderNo: orderNo, UserID: userID,
		PayMethod: pm, ChannelID: channelID, ClientIP: clientIP,
	})
}

// CancelOrder 用户取消待支付订单(§8,判活与置状态同事务)。
// 有活跃尝试时,事务提交后事务外尽力向渠道关单一次以缩窄"渠道侧仍可支付"的窗口;
// 关单结果不改变已提交状态,CLOSING→CLOSED 的落定由查单 worker 按渠道查单结果完成。
func (s *Service) CancelOrder(ctx context.Context, appID, orderNo string, userID int64) (models.OrderStatus, error) {
	var (
		result  models.OrderStatus
		closing *models.PaymentAttempt
	)
	err := s.d.Store.WithinTx(ctx, func(ctx context.Context, r store.Repos) error {
		o, err := r.Orders().GetByOrderNo(ctx, appID, orderNo, true)
		if err != nil {
			return err
		}
		if o.UserID != userID {
			return models.ErrOrderNotOwned
		}
		if o.Status != models.OrderPending {
			result = o.Status
			return models.ErrOrderNotPending
		}
		// 区分"从未提交网关"与"存在已提交历史"(评审 #5)。
		submitted, err := r.Attempts().ExistsSubmitted(ctx, orderNo)
		if err != nil {
			return err
		}
		active, err := r.Attempts().GetActiveByOrder(ctx, orderNo, true)
		if err != nil {
			return err
		}
		prev := o.Status
		switch {
		case !submitted:
			// 从未提交网关(尝试一律以 REQUESTING 起始,故此时必无活跃尝试):安全进入 CANCELLED。
			o.Status = models.OrderCancelled
			o.CloseReason = models.CloseUserCancel
			o.AggregateVersion++
			if err := r.Orders().Update(ctx, o); err != nil {
				return err
			}
			// CANCELLED(从未提交网关):发 order.closed(reason=user_cancel),告知接入方此单不会支付。
			eid, err := s.d.NewEventID()
			if err != nil {
				return err
			}
			env := models.BuildClosedEnvelope(o, prev, eid, s.d.Clock())
			if err := eventapp.WriteEvent(ctx, r, o.AppID, o.OrderNo, env); err != nil {
				return err
			}
			if err := eventapp.WriteLog(ctx, r, o, prev, "user", "用户取消"); err != nil {
				return err
			}
		case active != nil:
			// 存在活跃的已提交尝试:转 CLOSING(user_cancel),由查单 worker 网关确认后才 CLOSED。
			active.Status = models.AttemptClosing
			if err := r.Attempts().Update(ctx, active); err != nil {
				return err
			}
			closing = active
			o.Status = models.OrderClosing
			o.CloseReason = models.CloseUserCancel
			o.AggregateVersion++
			if err := r.Orders().Update(ctx, o); err != nil {
				return err
			}
			// CLOSING 未确认关闭,暂不发 order.closed(待查单 worker CLOSING→CLOSED 时发)。
			if err := eventapp.WriteLog(ctx, r, o, prev, "user", "用户取消"); err != nil {
				return err
			}
		default:
			// 曾提交但已无活跃尝试:全部已提交尝试均被网关确认终态(CLOSED/FAILED),无需再求证。
			// 同事务 CLOSING→CLOSED 落定 + closed 事件——否则查单 worker 只认领活跃尝试,
			// 没有任何 worker 会再碰这笔订单,将永久滞留 CLOSING。选 CLOSED 不选 CANCELLED:
			// 保留 CLOSED→PAID 异常恢复边,应对渠道侧延迟入账;CANCELLED 保持"从未提交"强语义。
			o.Status = models.OrderClosing
			o.CloseReason = models.CloseUserCancel
			o.AggregateVersion++
			if err := r.Orders().Update(ctx, o); err != nil {
				return err
			}
			if err := eventapp.WriteLog(ctx, r, o, prev, "user", "用户取消"); err != nil {
				return err
			}
			prev2 := o.Status
			o.Status = models.OrderClosed
			o.AggregateVersion++
			if err := r.Orders().Update(ctx, o); err != nil {
				return err
			}
			if err := eventapp.WriteLog(ctx, r, o, prev2, "system", "关单确认(无活跃尝试)"); err != nil {
				return err
			}
			eid, err := s.d.NewEventID()
			if err != nil {
				return err
			}
			env := models.BuildClosedEnvelope(o, prev2, eid, s.d.Clock())
			if err := eventapp.WriteEvent(ctx, r, o.AppID, o.OrderNo, env); err != nil {
				return err
			}
		}
		result = o.Status
		return nil
	})
	if err == nil && closing != nil {
		s.d.Payment.CloseAtGateway(ctx, closing)
	}
	return result, err
}

// ExpiredClose 一次超时关单认领的结果;OrderID=0 表示没有可认领的过期订单。
type ExpiredClose struct {
	OrderID uint64
	OrderNo string
	Status  models.OrderStatus // 关单后状态(CLOSING / CLOSED)
}

// CloseNextExpired 超时关单(超时 watchdog 调用,DESIGN §15):同一事务内认领一笔已过期 PENDING 订单
// (FOR UPDATE SKIP LOCKED,多副本各取不同行;exclude 为本轮已失败的订单 id)并关单——
// → CLOSING(TIMEOUT);无活跃尝试(从未提交网关,或全部尝试已被网关确认终态)→ 同事务 CLOSING→CLOSED
// + order.closed;仍有活跃已提交尝试才留 CLOSING,由查单 worker 确认。尝试截止不晚于订单截止,订单到期时
// 其全部尝试在渠道侧也已到期,故此路径不做同步关单。事务失败时返回值仍带已认领订单的 id,供调用方排除。
func (s *Service) CloseNextExpired(ctx context.Context, exclude []uint64) (ExpiredClose, error) {
	var res ExpiredClose
	err := s.d.Store.WithinTx(ctx, func(ctx context.Context, r store.Repos) error {
		o, err := r.Orders().ClaimExpiredPending(ctx, s.d.Clock(), exclude)
		if err != nil || o == nil {
			return err
		}
		res.OrderID, res.OrderNo = o.ID, o.OrderNo
		active, err := r.Attempts().GetActiveByOrder(ctx, o.OrderNo, true)
		if err != nil {
			return err
		}
		prev := o.Status
		if active != nil {
			active.Status = models.AttemptClosing
			if err := r.Attempts().Update(ctx, active); err != nil {
				return err
			}
		}
		o.Status = models.OrderClosing
		o.CloseReason = models.CloseTimeout
		o.AggregateVersion++
		if err := r.Orders().Update(ctx, o); err != nil {
			return err
		}
		if err := eventapp.WriteLog(ctx, r, o, prev, "system", "支付超时"); err != nil {
			return err
		}
		if active == nil {
			// 立即确认关闭:无活跃尝试意味着从未提交网关,或全部尝试均被网关确认终态,无需再求证
			// ——不落定则无 worker 认领,订单永久滞留 CLOSING。
			prev2 := o.Status
			o.Status = models.OrderClosed
			o.AggregateVersion++
			if err := r.Orders().Update(ctx, o); err != nil {
				return err
			}
			if err := eventapp.WriteLog(ctx, r, o, prev2, "system", "超时关闭"); err != nil {
				return err
			}
			eid, err := s.d.NewEventID()
			if err != nil {
				return err
			}
			env := models.BuildClosedEnvelope(o, prev2, eid, s.d.Clock())
			if err := eventapp.WriteEvent(ctx, r, o.AppID, o.OrderNo, env); err != nil {
				return err
			}
		}
		res.Status = o.Status
		return nil
	})
	return res, err
}

// GetOrder 按订单号或业务单号回查(镜像失步兜底,恢复 token);恰一个键由 transport 校验,
// 此处再守。租户不符等同不存在,归属不符拒绝(与 GetOrderStatus 一致)。
func (s *Service) GetOrder(ctx context.Context, appID string, userID int64, orderNo, clientOrderNo string) (*models.Order, error) {
	if (orderNo == "") == (clientOrderNo == "") {
		return nil, fmt.Errorf("%w: order_no 与 client_order_no 须恰好提供一个", models.ErrIllegalTransition)
	}
	var (
		o   *models.Order
		err error
	)
	if orderNo != "" {
		o, err = s.d.Store.Reader().Orders().GetByOrderNo(ctx, appID, orderNo, false)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return nil, models.ErrOrderNotFound
			}
			return nil, err
		}
	} else {
		o, err = s.d.Store.Reader().Orders().FindByClientOrderNo(ctx, appID, clientOrderNo)
		if err != nil {
			return nil, err
		}
		if o == nil {
			return nil, models.ErrOrderNotFound
		}
	}
	if o.UserID != userID {
		return nil, models.ErrOrderNotOwned
	}
	return o, nil
}

// CloseStalledClosing 滞留 CLOSING 兜底(超时 watchdog 第三轮):锁行复核仍 CLOSING 且
// 无活跃尝试 → 同事务落定 CLOSED + 日志 + closed 事件;有活跃尝试不动(查单 worker 职责)。
// 兜底与 CLOSING 的生产路径解耦:任何来源的孤儿单最终都被关闭。落定后事务外告警一次
// order_closing_stalled(不落定不告警,对单笔订单天然恰好一次)——兜底被触发说明存在
// 未收敛的生产路径,需排查。
func (s *Service) CloseStalledClosing(ctx context.Context, appID, orderNo string) (closed bool, err error) {
	var closingSince time.Time
	err = s.d.Store.WithinTx(ctx, func(ctx context.Context, r store.Repos) error {
		o, err := r.Orders().GetByOrderNo(ctx, appID, orderNo, true)
		if err != nil {
			return err
		}
		if o.Status != models.OrderClosing {
			return nil // 已被并发收敛,幂等返回
		}
		active, err := r.Attempts().GetActiveByOrder(ctx, orderNo, true)
		if err != nil {
			return err
		}
		if active != nil {
			return nil // 仍有活跃尝试:正常收敛属查单 worker,不越权
		}
		prev := o.Status
		closingSince = o.UpdatedAt // CLOSING 期间订单无其它写入,updated_at 即进入 CLOSING 的时刻
		o.Status = models.OrderClosed
		o.AggregateVersion++
		if err := r.Orders().Update(ctx, o); err != nil {
			return err
		}
		if err := eventapp.WriteLog(ctx, r, o, prev, "system", "关单兜底(CLOSING 滞留)"); err != nil {
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
		closed = true
		return nil
	})
	if err != nil {
		return false, err
	}
	if closed {
		s.d.Obs.ClosingSettled(obs.ClosingViaWatchdog, max(s.d.Clock().Sub(closingSince).Seconds(), 0))
		s.d.Alerter.Anomaly(ctx, "order_closing_stalled", map[string]any{
			"app_id": appID, "order_no": orderNo,
		})
	}
	return closed, nil
}

// GetOrderStatus 按 order_token 查订单(前端反馈 + 对账兜底);校验租户 + 归属(评审 P1)。
func (s *Service) GetOrderStatus(ctx context.Context, appID, orderToken string, userID int64) (*models.Order, error) {
	o, err := s.d.Store.Reader().Orders().GetByToken(ctx, orderToken)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, models.ErrOrderNotFound
		}
		return nil, err
	}
	// 多租户:app_id 不符对外等同不存在(不同接入方 user_id 可能重复,必须先隔离租户)。
	if o.AppID != appID {
		return nil, models.ErrOrderNotFound
	}
	if o.UserID != userID {
		return nil, models.ErrOrderNotOwned
	}
	return o, nil
}
