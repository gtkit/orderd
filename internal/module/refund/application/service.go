// Package application(refund)实现退款闭环(§10,V1 全额,授权关闭)。
//
// V1 双重关闭(评审 #7 措辞修正):refund.enabled=false 配置开关【或】未发放 refund:execute 权限,
// 任一不满足即不允许退款(两者都满足才放行);权限在 transport 校验,配置开关在此校验。
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

// Deps 是 refund 服务依赖。
type Deps struct {
	Store          store.Store
	Router         paygateway.Router
	Accounts       account.Resolver
	Alerter        alert.Alerter
	Clock          func() time.Time
	NewRefundNo    func() string
	NewEventID     func() (string, error)
	Enabled        bool          // refund.enabled 配置开关(V1=false)
	GatewayTimeout time.Duration // 单次网关调用强制超时(评审 P1)
}

// Service 退款应用服务。
type Service struct{ d Deps }

// New 构造 refund 服务。
func New(d Deps) *Service {
	if d.Clock == nil {
		d.Clock = time.Now
	}
	if d.GatewayTimeout == 0 {
		d.GatewayTimeout = 10 * time.Second
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

// RefundOrder 发起全额退款(V1)。
func (s *Service) RefundOrder(ctx context.Context, in RefundInput) (RefundOutcome, error) {
	if !s.d.Enabled {
		return RefundOutcome{}, fmt.Errorf("%w: refund.enabled=false(V1 授权关闭)", models.ErrRefundNotAllowed)
	}
	if in.OutRefundNo == "" {
		return RefundOutcome{}, fmt.Errorf("%w: 缺少 out_refund_no", models.ErrRefundNotAllowed)
	}
	hash := models.RefundRequestHash(in.OutRefundNo, in.OrderNo, in.RefundAmount, in.Reason)

	// 幂等:同 out_refund_no 复用/冲突。
	if existing, err := s.d.Store.Reader().Refunds().FindByOutRefundNo(ctx, in.AppID, in.OutRefundNo); err != nil {
		return RefundOutcome{}, err
	} else if existing != nil {
		if existing.RequestHash != hash {
			return RefundOutcome{}, fmt.Errorf("%w: 同退款号绑定了不同请求", models.ErrIdempotencyConflict)
		}
		return RefundOutcome{RefundNo: existing.RefundNo, Status: existing.Status, Reused: true}, nil
	}

	var (
		refundNo          string
		payAcct           string
		settledChannel    string
		settledOutTradeNo string
		totalAmount       int64
	)
	err := s.d.Store.WithinTx(ctx, func(ctx context.Context, r store.Repos) error {
		o, err := r.Orders().GetByOrderNo(ctx, in.AppID, in.OrderNo, true)
		if err != nil {
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
		payAcct = o.RefundAccountID()
		settledChannel = sqlutil.OrDefault(o.SettledChannel, o.PayChannel)
		settledOutTradeNo = o.SettledOutTradeNo
		totalAmount = o.PayAmount
		refundNo = s.d.NewRefundNo()
		rf := &models.Refund{
			AppID: in.AppID, RefundNo: refundNo, OutRefundNo: in.OutRefundNo, RequestHash: hash,
			OrderNo: in.OrderNo, PayAccountID: payAcct, RefundAmount: in.RefundAmount,
			Status: models.RefundCreated, Reason: in.Reason,
		}
		if err := r.Refunds().Insert(ctx, rf); err != nil {
			return err // 命中 active_guard(含 ABNORMAL)→ ErrDuplicate
		}
		return eventapp.WriteLog(ctx, r, o, prev, sqlutil.OrDefault(in.Operator, "admin"), "发起退款")
	})
	if err != nil {
		if errors.Is(err, store.ErrDuplicate) {
			return RefundOutcome{}, fmt.Errorf("%w: 已存在活跃退款", models.ErrActiveRefundExists)
		}
		return RefundOutcome{}, err
	}

	// 调网关退款(不在事务内,强制超时;失败/回退用解耦 ctx)。
	gctx, cancel := s.gwCtx()
	defer cancel()
	gw, gerr := s.d.Router.Resolve(settledChannel, payAcct)
	if gerr != nil {
		s.failRefund(in.AppID, in.OrderNo, refundNo, gerr.Error())
		return RefundOutcome{}, gerr
	}
	rr, gerr := gw.Refund(gctx, paygateway.RefundInput{
		Channel: settledChannel, PayAccountID: payAcct, OutTradeNo: settledOutTradeNo, OutRefundNo: in.OutRefundNo,
		RefundAmount: in.RefundAmount, TotalAmount: totalAmount, Currency: "CNY",
	})
	if gerr != nil || !rr.Accepted {
		s.failRefund(in.AppID, in.OrderNo, refundNo, "网关未受理退款")
		if gerr != nil {
			return RefundOutcome{}, gerr
		}
		return RefundOutcome{RefundNo: refundNo, Status: models.RefundFailed}, nil
	}

	// 受理成功 → PROCESSING + 设查单时间(不吞错误;失败告警,交退款查单 worker 收敛)。
	uctx, ucancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer ucancel()
	uerr := s.d.Store.WithinTx(uctx, func(ctx context.Context, r store.Repos) error {
		rf, err := r.Refunds().GetByRefundNo(ctx, refundNo)
		if err != nil {
			return err
		}
		rf.Status = models.RefundProcessing
		rf.GatewayRefundID = rr.GatewayRefundID
		rf.NextQueryAt = sqlutil.Time(s.d.Clock().Add(10 * time.Second))
		return r.Refunds().Update(ctx, rf)
	})
	if uerr != nil {
		// 网关已受理但本地未标 PROCESSING:告警,交退款查单 worker 兜底(仍会按 refundNo 查单)。
		s.d.Alerter.Anomaly(context.Background(), "refund_processing_update_failed",
			map[string]any{"refund_no": refundNo, "order_no": in.OrderNo, "err": uerr.Error()})
	}
	return RefundOutcome{RefundNo: refundNo, Status: models.RefundProcessing}, nil
}

// failRefund 网关退款失败:退款 → FAILED,订单 REFUNDING→PAID 回退(DESIGN §10)。用解耦 ctx,不吞错误。
func (s *Service) failRefund(appID, orderNo, refundNo, reason string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := s.d.Store.WithinTx(ctx, func(ctx context.Context, r store.Repos) error {
		rf, err := r.Refunds().GetByRefundNo(ctx, refundNo)
		if err != nil {
			return err
		}
		rf.Status = models.RefundFailed
		rf.LastError = sqlutil.Str(reason)
		if err := r.Refunds().Update(ctx, rf); err != nil {
			return err
		}
		o, err := r.Orders().GetByOrderNo(ctx, appID, orderNo, true)
		if err != nil {
			return err
		}
		if o.Status == models.OrderRefunding {
			prev := o.Status
			o.Status = models.OrderPaid
			o.AggregateVersion++
			if err := r.Orders().Update(ctx, o); err != nil {
				return err
			}
			return eventapp.WriteLog(ctx, r, o, prev, "system", "退款失败回退")
		}
		return nil
	})
	if err != nil {
		s.d.Alerter.Anomaly(context.Background(), "refund_fail_rollback_failed",
			map[string]any{"refund_no": refundNo, "order_no": orderNo, "err": err.Error()})
	}
}

// ConfirmRefundSuccess 退款成功单事务(与支付成功同等级,评审 #7):
// 网关确认退款成功 → 退款 SUCCEEDED + 订单 REFUNDING→REFUNDED + 累计已退 + 不可变退款流水
// (流水回指 refund_no)+ order.refunded 事件。
func (s *Service) ConfirmRefundSuccess(ctx context.Context, appID, refundNo, gatewayRefundID, transactionID string, amount int64, at time.Time) error {
	return s.d.Store.WithinTx(ctx, func(ctx context.Context, r store.Repos) error {
		rf, err := r.Refunds().GetByRefundNo(ctx, refundNo)
		if err != nil {
			return err
		}
		if rf.Status == models.RefundSucceeded {
			return nil // 幂等
		}
		// 校验成功金额与退款单金额一致(评审 P1):不一致=资金异常,不结算,告警。
		if amount != rf.RefundAmount {
			s.d.Alerter.Anomaly(ctx, "refund_amount_mismatch", map[string]any{
				"refund_no": refundNo, "expected": rf.RefundAmount, "got": amount,
			})
			return fmt.Errorf("%w: 退款成功金额 %d 与退款单金额 %d 不符", models.ErrPaymentAnomaly, amount, rf.RefundAmount)
		}
		o, err := r.Orders().GetByOrderNo(ctx, appID, rf.OrderNo, true)
		if err != nil {
			return err
		}
		// 退款成功不可变流水(kind=2,回指 refund_no;渠道用结算渠道)。
		if err := r.Transactions().Insert(ctx, &models.PaymentTransaction{
			AppID: appID, OrderNo: rf.OrderNo, RefundNo: refundNo, PayAccountID: rf.PayAccountID,
			Channel: sqlutil.OrDefault(o.SettledChannel, o.PayChannel), Kind: models.TxnRefund, OutTradeNo: rf.OutRefundNo,
			TransactionID: transactionID, Amount: amount, Currency: o.Currency, PaidAt: sqlutil.Time(at),
		}); err != nil && !errors.Is(err, store.ErrDuplicate) {
			return err
		}
		rf.Status = models.RefundSucceeded
		rf.GatewayRefundID = gatewayRefundID
		rf.SucceededAt = sqlutil.Time(at)
		if err := r.Refunds().Update(ctx, rf); err != nil {
			return err
		}
		if o.Status == models.OrderRefunding {
			prev := o.Status
			o.Status = models.OrderRefunded
			o.RefundedAmount += amount
			o.AggregateVersion++
			if err := r.Orders().Update(ctx, o); err != nil {
				return err
			}
			if err := eventapp.WriteLog(ctx, r, o, prev, "system", "退款成功"); err != nil {
				return err
			}
			eid, err := s.d.NewEventID()
			if err != nil {
				return err
			}
			env := models.BuildRefundedEnvelope(o, rf, prev, eid, s.d.Clock())
			return eventapp.WriteEvent(ctx, r, o.AppID, o.OrderNo, env)
		}
		return nil
	})
}

// GetRefund 按 refund_no 或 out_refund_no 查退款;多租户查询始终校验 app_id(评审 P1)。
func (s *Service) GetRefund(ctx context.Context, appID, refundNo, outRefundNo string) (*models.Refund, error) {
	r := s.d.Store.Reader().Refunds()
	if refundNo != "" {
		rf, err := r.GetByRefundNo(ctx, refundNo)
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
	settledChannel, settledOutTradeNo, err := s.settledForOrder(ctx, rf.AppID, rf.OrderNo)
	if err != nil {
		return err
	}
	gw, err := s.d.Router.Resolve(settledChannel, rf.PayAccountID)
	if err != nil {
		return err
	}
	gctx, cancel := s.gwCtx()
	defer cancel()
	qr, err := gw.RefundQuery(gctx, paygateway.RefundQueryInput{
		Channel: settledChannel, PayAccountID: rf.PayAccountID, OutRefundNo: rf.OutRefundNo, OutTradeNo: settledOutTradeNo,
	})
	if err != nil {
		return err
	}
	switch qr.State {
	case paygateway.RefundQuerySucceeded:
		amt := qr.Amount
		if amt == 0 {
			amt = rf.RefundAmount
		}
		return s.ConfirmRefundSuccess(ctx, rf.AppID, rf.RefundNo, qr.GatewayRefundID, qr.TransactionID, amt, at(qr, s.d.Clock()))
	case paygateway.RefundQueryFailed:
		s.failRefund(rf.AppID, rf.OrderNo, rf.RefundNo, "网关退款失败")
		return nil
	default: // Processing / Error → 保持,交下次查单
		return nil
	}
}

func at(qr paygateway.RefundQueryResult, now time.Time) time.Time {
	if qr.SucceededAt.IsZero() {
		return now
	}
	return qr.SucceededAt
}

// settledForOrder 取订单结算渠道 + 原商户单号(退款查单用)。
func (s *Service) settledForOrder(ctx context.Context, appID, orderNo string) (channel, outTradeNo string, err error) {
	o, err := s.d.Store.Reader().Orders().GetByOrderNo(ctx, appID, orderNo, false)
	if err != nil {
		return "", "", err
	}
	return sqlutil.OrDefault(o.SettledChannel, o.PayChannel), o.SettledOutTradeNo, nil
}
