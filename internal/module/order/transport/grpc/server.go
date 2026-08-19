// Package grpc 是 OrderService 的 gRPC 传输适配层:鉴权 → protovalidate 校验 →
// 转换到应用层入参 → 调用应用服务 → 领域错误映射到 gRPC status。不含业务规则。
package grpc

import (
	"context"
	"strconv"

	"buf.build/go/protovalidate"
	"github.com/gtkit/orderd/internal/models"
	orderapp "github.com/gtkit/orderd/internal/module/order/application"
	refundapp "github.com/gtkit/orderd/internal/module/refund/application"
	"github.com/gtkit/orderd/internal/store"
	orderv1 "github.com/gtkit/orderd/pb/order/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Deps 是传输层依赖。
type Deps struct {
	Order  *orderapp.Service
	Refund *refundapp.Service
	Store  store.Store // 只读:ListOrderEvents 游标查询
}

// Server 实现 orderv1.OrderServiceServer。
type Server struct {
	orderv1.UnimplementedOrderServiceServer
	d         Deps
	validator protovalidate.Validator
}

// New 构造 gRPC 服务器(初始化 protovalidate 校验器)。
func New(d Deps) (*Server, error) {
	v, err := protovalidate.New()
	if err != nil {
		return nil, err
	}
	return &Server{d: d, validator: v}, nil
}

func (s *Server) validate(msg proto.Message) error {
	if err := s.validator.Validate(msg); err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	return nil
}

// CreateOrder 下单。
func (s *Server) CreateOrder(ctx context.Context, req *orderv1.CreateOrderRequest) (*orderv1.CreateOrderResponse, error) {
	if err := s.validate(req); err != nil {
		return nil, err
	}
	c, err := authFromContext(ctx)
	if err != nil {
		return nil, err
	}
	pm, ok := models.PayMethodFromProto(req.PayMethod)
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "支付方式非法")
	}
	out, err := s.d.Order.CreateOrder(ctx, orderapp.CreateInput{
		AppID: c.AppID, UserID: c.UserID,
		ProductID: req.Product.ProductId, ProductTitle: req.Product.Title, BusinessType: req.Product.BusinessType,
		MetadataType: req.Product.MetadataType, MetadataVersion: req.Product.MetadataVersion, Metadata: req.Product.Metadata,
		Amount:    models.MoneyFromProto(req.Amount),
		PayMethod: pm, ChannelID: req.ChannelId, ClientIP: req.ClientIp,
		IdempotencyKey: req.IdempotencyKey, ClientOrderNo: req.ClientOrderNo,
	})
	if err != nil {
		return nil, toStatus(err)
	}
	return &orderv1.CreateOrderResponse{
		OrderNo: out.OrderNo, OrderToken: out.OrderToken, AppParams: out.AppParams,
		Status: out.Status.Proto(), ExpireAt: timestamppb.New(out.ExpireAt), Reused: out.Reused,
	}, nil
}

// GetOrderStatus 查询订单状态。
func (s *Server) GetOrderStatus(ctx context.Context, req *orderv1.GetOrderStatusRequest) (*orderv1.GetOrderStatusResponse, error) {
	if err := s.validate(req); err != nil {
		return nil, err
	}
	c, err := authFromContext(ctx)
	if err != nil {
		return nil, err
	}
	o, err := s.d.Order.GetOrderStatus(ctx, c.AppID, req.OrderToken, c.UserID)
	if err != nil {
		return nil, toStatus(err)
	}
	resp := &orderv1.GetOrderStatusResponse{
		Status: o.Status.Proto(), StatusText: o.Status.Text(), CloseReason: o.CloseReason.Proto(),
		Amount:           &orderv1.Money{Original: o.OriginalPrice, Discount: o.DiscountAmount, Payable: o.PayAmount, Currency: currencyProto(o.Currency)},
		TradeNo:          o.TradeNo,
		RefundedAmount:   o.RefundedAmount,
		AggregateVersion: o.AggregateVersion,
		Metadata:         o.Metadata,
	}
	if o.PaidAt.Valid {
		resp.PaidAt = timestamppb.New(o.PaidAt.Time)
	}
	return resp, nil
}

// GetOrder 按订单号/业务单号回查(镜像失步兜底)。
func (s *Server) GetOrder(ctx context.Context, req *orderv1.GetOrderRequest) (*orderv1.GetOrderResponse, error) {
	if err := s.validate(req); err != nil {
		return nil, err
	}
	c, err := authFromContext(ctx)
	if err != nil {
		return nil, err
	}
	o, err := s.d.Order.GetOrder(ctx, c.AppID, c.UserID, req.OrderNo, req.ClientOrderNo)
	if err != nil {
		return nil, toStatus(err)
	}
	resp := &orderv1.GetOrderResponse{
		OrderNo: o.OrderNo, ClientOrderNo: o.ClientOrderNo.String, OrderToken: o.OrderToken,
		Status: o.Status.Proto(), StatusText: o.Status.Text(), CloseReason: o.CloseReason.Proto(),
		Amount:           &orderv1.Money{Original: o.OriginalPrice, Discount: o.DiscountAmount, Payable: o.PayAmount, Currency: currencyProto(o.Currency)},
		TradeNo:          o.TradeNo,
		RefundedAmount:   o.RefundedAmount,
		AggregateVersion: o.AggregateVersion,
		Metadata:         o.Metadata,
		ExpireAt:         timestamppb.New(o.ExpireAt),
	}
	if o.PaidAt.Valid {
		resp.PaidAt = timestamppb.New(o.PaidAt.Time)
	}
	return resp, nil
}

// CancelOrder 取消订单。
func (s *Server) CancelOrder(ctx context.Context, req *orderv1.CancelOrderRequest) (*orderv1.CancelOrderResponse, error) {
	if err := s.validate(req); err != nil {
		return nil, err
	}
	c, err := authFromContext(ctx)
	if err != nil {
		return nil, err
	}
	st, err := s.d.Order.CancelOrder(ctx, c.AppID, req.OrderNo, c.UserID)
	if err != nil {
		return nil, toStatus(err)
	}
	return &orderv1.CancelOrderResponse{Status: st.Proto()}, nil
}

// PreparePayment 换渠道 / 重新调起支付。
func (s *Server) PreparePayment(ctx context.Context, req *orderv1.PreparePaymentRequest) (*orderv1.PreparePaymentResponse, error) {
	if err := s.validate(req); err != nil {
		return nil, err
	}
	c, err := authFromContext(ctx)
	if err != nil {
		return nil, err
	}
	pm, ok := models.PayMethodFromProto(req.PayMethod)
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "支付方式非法")
	}
	out, err := s.d.Order.PreparePayment(ctx, c.AppID, c.UserID, req.OrderNo, pm, req.ChannelId, req.ClientIp)
	if err != nil {
		return nil, toStatus(err)
	}
	return &orderv1.PreparePaymentResponse{
		AttemptNo: out.AttemptNo, AppParams: out.AppParams,
		Status: out.Status.Proto(), ExpireAt: timestamppb.New(out.ExpireAt), Reused: out.Reused,
	}, nil
}

// RefundOrder 发起退款(V1 授权关闭:transport 层还应有 refund:execute 权限门禁)。
func (s *Server) RefundOrder(ctx context.Context, req *orderv1.RefundOrderRequest) (*orderv1.RefundOrderResponse, error) {
	if err := s.validate(req); err != nil {
		return nil, err
	}
	c, err := authFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if s.d.Refund == nil {
		return nil, status.Error(codes.Unimplemented, "退款未启用")
	}
	out, err := s.d.Refund.RefundOrder(ctx, refundapp.RefundInput{
		AppID: c.AppID, OrderNo: req.OrderNo, RefundAmount: req.RefundAmount,
		Reason: req.Reason, OutRefundNo: req.OutRefundNo, Operator: strconv.FormatInt(c.UserID, 10),
	})
	if err != nil {
		return nil, toStatus(err)
	}
	return &orderv1.RefundOrderResponse{RefundNo: out.RefundNo, Status: out.Status.Proto()}, nil
}

// GetRefund 查询退款。
func (s *Server) GetRefund(ctx context.Context, req *orderv1.GetRefundRequest) (*orderv1.GetRefundResponse, error) {
	if err := s.validate(req); err != nil {
		return nil, err
	}
	c, err := authFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if s.d.Refund == nil {
		return nil, status.Error(codes.Unimplemented, "退款未启用")
	}
	rf, err := s.d.Refund.GetRefund(ctx, c.AppID, req.RefundNo, req.OutRefundNo)
	if err != nil {
		return nil, toStatus(err)
	}
	resp := &orderv1.GetRefundResponse{
		RefundNo: rf.RefundNo, OutRefundNo: rf.OutRefundNo, OrderNo: rf.OrderNo,
		RefundAmount: rf.RefundAmount, Status: rf.Status.Proto(), GatewayRefundId: rf.GatewayRefundID,
	}
	if rf.SucceededAt.Valid {
		resp.SucceededAt = timestamppb.New(rf.SucceededAt.Time)
	}
	return resp, nil
}

// ListOrderEvents 游标拉取事件(接入方漏投补偿)。
func (s *Server) ListOrderEvents(ctx context.Context, req *orderv1.ListOrderEventsRequest) (*orderv1.ListOrderEventsResponse, error) {
	if err := s.validate(req); err != nil {
		return nil, err
	}
	c, err := authFromContext(ctx)
	if err != nil {
		return nil, err
	}
	var afterID uint64
	if req.Cursor != "" {
		afterID, err = strconv.ParseUint(req.Cursor, 10, 64)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "游标非法")
		}
	}
	limit := int(req.Limit)
	events, err := s.d.Store.Reader().Events().ListByApp(ctx, c.AppID, afterID, limit)
	if err != nil {
		return nil, toStatus(err)
	}
	resp := &orderv1.ListOrderEventsResponse{}
	var last uint64
	for i := range events {
		e := events[i]
		resp.Events = append(resp.Events, &orderv1.EventRecord{
			EventId: e.EventID, EventType: e.EventType, OrderNo: e.OrderNo,
			AggregateVersion: e.AggregateVersion, OccurredAt: timestamppb.New(e.CreatedAt), Envelope: e.Payload,
		})
		last = e.ID
	}
	if len(events) == limit && last > 0 {
		resp.NextCursor = strconv.FormatUint(last, 10)
	}
	return resp, nil
}

func currencyProto(c string) orderv1.Currency {
	if c == "CNY" {
		return orderv1.Currency_CURRENCY_CNY
	}
	return orderv1.Currency_CURRENCY_UNSPECIFIED
}
