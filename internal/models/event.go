package models

import (
	"time"

	orderv1 "github.com/gtkit/orderd/pb/order/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// 事件类型(webhook event_type / order_events.event_type)。
const (
	EventTypePaid     = "order.paid"
	EventTypeRefunded = "order.refunded"
	EventTypeClosed   = "order.closed"
)

// 事件信封构建为纯函数:event_id、occurredAt 由应用层注入(不在此生成),便于确定性测试。
// current_status 取自订单当前状态;previous_status 由调用方显式传入(仅它知道跃迁前值)。
//
// 安全:order_token 绝不进事件(评审补充,已从 OrderPaidEvent 删除)。

// BuildPaidEnvelope 构造 order.paid 事件信封。
func BuildPaidEnvelope(o *Order, prev OrderStatus, eventID string, occurredAt time.Time) *orderv1.EventEnvelope {
	var paidAt *timestamppb.Timestamp
	if o.PaidAt.Valid {
		paidAt = timestamppb.New(o.PaidAt.Time)
	}
	env := newEnvelope(o, prev, eventID, occurredAt, EventTypePaid)
	env.Payload = &orderv1.EventEnvelope_Paid{Paid: &orderv1.OrderPaidEvent{
		OrderNo:         o.OrderNo,
		UserId:          o.UserID,
		ProductId:       o.ProductID,
		ProductTitle:    o.ProductTitle,
		Amount:          o.PayAmount,
		Currency:        o.Currency,
		TradeNo:         o.TradeNo,
		PayMethod:       int32(o.PayMethod),
		PayChannel:      o.PayChannel,
		PaidAt:          paidAt,
		MetadataType:    o.MetadataType,
		MetadataVersion: o.MetadataVersion,
		Metadata:        o.Metadata,
	}}
	return env
}

// BuildRefundedEnvelope 构造 order.refunded 事件信封。
func BuildRefundedEnvelope(o *Order, r *Refund, prev OrderStatus, eventID string, occurredAt time.Time) *orderv1.EventEnvelope {
	var refundedAt *timestamppb.Timestamp
	if r.SucceededAt.Valid {
		refundedAt = timestamppb.New(r.SucceededAt.Time)
	}
	env := newEnvelope(o, prev, eventID, occurredAt, EventTypeRefunded)
	env.Payload = &orderv1.EventEnvelope_Refunded{Refunded: &orderv1.OrderRefundedEvent{
		OrderNo:         o.OrderNo,
		UserId:          o.UserID,
		RefundNo:        r.RefundNo,
		RefundAmount:    r.RefundAmount,
		RefundedAt:      refundedAt,
		MetadataType:    o.MetadataType,
		MetadataVersion: o.MetadataVersion,
		Metadata:        o.Metadata,
	}}
	return env
}

// BuildClosedEnvelope 构造 order.closed 事件信封。
func BuildClosedEnvelope(o *Order, prev OrderStatus, eventID string, occurredAt time.Time) *orderv1.EventEnvelope {
	env := newEnvelope(o, prev, eventID, occurredAt, EventTypeClosed)
	env.Payload = &orderv1.EventEnvelope_Closed{Closed: &orderv1.OrderClosedEvent{
		OrderNo:  o.OrderNo,
		UserId:   o.UserID,
		Reason:   o.CloseReason.Proto(),
		ClosedAt: timestamppb.New(occurredAt),
	}}
	return env
}

func newEnvelope(o *Order, prev OrderStatus, eventID string, occurredAt time.Time, eventType string) *orderv1.EventEnvelope {
	return &orderv1.EventEnvelope{
		EventId:          eventID,
		EventType:        eventType,
		OccurredAt:       timestamppb.New(occurredAt),
		AppId:            o.AppID,
		OrderNo:          o.OrderNo,
		AggregateVersion: o.AggregateVersion,
		PreviousStatus:   prev.Proto(),
		CurrentStatus:    o.Status.Proto(),
		SchemaVersion:    1,
	}
}
