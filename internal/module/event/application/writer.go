// Package application(event)提供事件双写与订单日志的共享写入器。
//
// 事件双写(DESIGN §11):同事务写 order_events(不可变历史,游标查询源)+ event_outbox(投递任务)。
// order/payment/refund 三模块共用本包,避免各自复制序列化/双写逻辑。
package application

import (
	"context"
	"fmt"

	"github.com/gtkit/orderd/internal/models"
	"github.com/gtkit/orderd/internal/store"
	orderv1 "github.com/gtkit/orderd/pb/order/v1"
	"google.golang.org/protobuf/proto"
)

// WriteEvent 在当前事务内把事件信封双写到 order_events + event_outbox。
// aggregate_version 取自 env(应为订单跃迁后的新版本);payload 为序列化信封。
func WriteEvent(ctx context.Context, r store.Repos, appID, orderNo string, env *orderv1.EventEnvelope) error {
	payload, err := proto.Marshal(env)
	if err != nil {
		return fmt.Errorf("序列化事件失败: %w", err)
	}
	if err := r.Events().InsertEvent(ctx, &models.OrderEvent{
		EventID: env.EventId, AppID: appID, OrderNo: orderNo,
		EventType: env.EventType, AggregateVersion: env.AggregateVersion, Payload: payload,
	}); err != nil {
		return err
	}
	return r.Events().InsertOutbox(ctx, &models.EventOutbox{
		EventID: env.EventId, AppID: appID, OrderNo: orderNo,
		EventType: env.EventType, AggregateVersion: env.AggregateVersion, Payload: payload,
		Status: models.OutboxPending, NextAttemptAt: env.OccurredAt.AsTime(),
	})
}

// WriteLog 追加订单状态流转日志。
func WriteLog(ctx context.Context, r store.Repos, o *models.Order, from models.OrderStatus, operator, remark string) error {
	return r.Logs().Insert(ctx, &models.OrderLog{
		OrderID: o.ID, OrderNo: o.OrderNo, FromStatus: from, ToStatus: o.Status,
		Operator: operator, Remark: remark,
	})
}
