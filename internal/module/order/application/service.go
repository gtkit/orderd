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
	if err := models.ValidateAmount(s.d.Amounts, in.AppID, in.Amount, in.BusinessType); err != nil {
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
		return r.Orders().Insert(ctx, o)
	})
	if insertErr != nil {
		// 并发同幂等键:一方胜出,另一方命中唯一键 → 重新按幂等语义收敛(复用或冲突)。
		if errors.Is(insertErr, store.ErrDuplicate) && idemKey.Valid {
			existing, ferr := s.d.Store.Reader().Orders().FindByIdempotencyKey(ctx, in.AppID, idemKey.String)
			if ferr != nil {
				return CreateOutcome{}, ferr
			}
			if existing != nil {
				return s.reuse(ctx, existing, hash, in)
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
func (s *Service) CancelOrder(ctx context.Context, appID, orderNo string, userID int64) (models.OrderStatus, error) {
	var result models.OrderStatus
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
		if !submitted {
			// 从未提交网关:安全进入 CANCELLED;顺手关闭未提交的活跃尝试(CREATED)。
			if active != nil {
				active.Status = models.AttemptClosed
				if err := r.Attempts().Update(ctx, active); err != nil {
					return err
				}
			}
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
		} else {
			// 存在已提交网关的尝试:转 CLOSING(user_cancel),由 reconcile 关单确认后才 CLOSED。
			if active != nil && active.Status.Active() {
				active.Status = models.AttemptClosing
				if err := r.Attempts().Update(ctx, active); err != nil {
					return err
				}
			}
			o.Status = models.OrderClosing
			o.CloseReason = models.CloseUserCancel
			o.AggregateVersion++
			if err := r.Orders().Update(ctx, o); err != nil {
				return err
			}
			// CLOSING 未确认关闭,暂不发 order.closed(待 reconcile CLOSING→CLOSED 时发)。
		}
		if err := eventapp.WriteLog(ctx, r, o, prev, "user", "用户取消"); err != nil {
			return err
		}
		result = o.Status
		return nil
	})
	return result, err
}

// CloseIfExpired 超时关单(超时 watchdog 调用,DESIGN §15):过期 PENDING → CLOSING(TIMEOUT);
// 从未提交网关则同事务 CLOSING→CLOSED + order.closed;有提交历史留 CLOSING,由查单 worker 确认。
// 行锁 + 状态复核保证多副本幂等。
func (s *Service) CloseIfExpired(ctx context.Context, appID, orderNo string) (models.OrderStatus, error) {
	var result models.OrderStatus
	err := s.d.Store.WithinTx(ctx, func(ctx context.Context, r store.Repos) error {
		o, err := r.Orders().GetByOrderNo(ctx, appID, orderNo, true)
		if err != nil {
			return err
		}
		result = o.Status
		if o.Status != models.OrderPending || !o.ExpireAt.Before(s.d.Clock()) {
			return nil // 已被处理或未真正过期:幂等返回
		}
		submitted, err := r.Attempts().ExistsSubmitted(ctx, orderNo)
		if err != nil {
			return err
		}
		active, err := r.Attempts().GetActiveByOrder(ctx, orderNo, true)
		if err != nil {
			return err
		}
		prev := o.Status
		if active != nil && active.Status.Active() {
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
		if !submitted {
			// 无网关交互 → 立即确认关闭:活跃尝试(CREATED)→CLOSED,订单 CLOSING→CLOSED + closed 事件。
			if active != nil {
				active.Status = models.AttemptClosed
				if err := r.Attempts().Update(ctx, active); err != nil {
					return err
				}
			}
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
		result = o.Status
		return nil
	})
	return result, err
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
