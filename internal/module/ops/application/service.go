// Package application(ops)实现三类异常的处置闭环(DESIGN §12/§13/§14):
// 对账差异清单与处置、Outbox 死信清单与人工重放、资金异常流水查询。
//
// 边界:只依赖 store 端口与 models,不 import gorm/gin/repository。
// 写动作(处置/重放)与审计在同一事务内完成:动作回滚则审计不存在,审计失败则动作回滚。
// 租户可见性靠权限发放边界(ops:* 不进接入方 token),列表的 app_id 只是查询意图过滤,
// 不按 token 二次裁剪——否则跨租户共享账号(app_id=”)的差异永远不可见(评审 D8)。
package application

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/gtkit/orderd/internal/models"
	"github.com/gtkit/orderd/internal/store"
)

// ErrInvalidRequest 请求参数非法(limit 超限/忽略无备注/备注超长),映射 InvalidArgument。
var ErrInvalidRequest = errors.New("ops: 请求参数非法")

const (
	defaultLimit = 50
	maxLimit     = 200
	maxRemarkLen = 512
)

// Actor 是运维动作发起者(取自 token claims,绝不取请求参数——审计不可伪造)。
type Actor struct {
	AppID  string
	UserID int64
}

func (a Actor) String() string { return fmt.Sprintf("%s:%d", a.AppID, a.UserID) }

// Deps 是 ops 服务依赖。
type Deps struct {
	Store store.Store
	Clock func() time.Time
}

// Service 运维应用服务。
type Service struct{ d Deps }

// New 构造 ops 服务。
func New(d Deps) *Service {
	if d.Clock == nil {
		d.Clock = time.Now
	}
	return &Service{d: d}
}

// normLimit 归一化分页大小:0=默认 50,上限 200(protovalidate 已拦,此处兜底)。
func normLimit(limit int32) (int, error) {
	if limit < 0 || limit > maxLimit {
		return 0, fmt.Errorf("%w: limit 须在 [0,%d]", ErrInvalidRequest, maxLimit)
	}
	if limit == 0 {
		return defaultLimit, nil
	}
	return int(limit), nil
}

// nextCursor 满页时返回末条 id 作下一页游标,不足一页返回 0(已到末尾)。
func nextCursor[T any](rows []T, limit int, lastID uint64) uint64 {
	if len(rows) == limit {
		return lastID
	}
	return 0
}

// ListReconcileDiffs 对账差异清单(只读,游标分页)。
func (s *Service) ListReconcileDiffs(ctx context.Context, f store.DiffFilter, afterID uint64, limit int32) ([]models.ReconcileDiff, uint64, error) {
	n, err := normLimit(limit)
	if err != nil {
		return nil, 0, err
	}
	rows, err := s.d.Store.Reader().Reconcile().ListDiffs(ctx, f, afterID, n)
	if err != nil {
		return nil, 0, err
	}
	var last uint64
	if len(rows) > 0 {
		last = rows[len(rows)-1].ID
	}
	return rows, nextCursor(rows, n, last), nil
}

// HandleReconcileDiff 处置一条差异:锁行 CAS(终态/同态拒绝)→ 写处置字段 → 同事务写审计。
// 返回处置后的状态。
func (s *Service) HandleReconcileDiff(ctx context.Context, actor Actor, id uint64, target models.DiffHandleStatus, remark string) (models.DiffHandleStatus, error) {
	if len(remark) > maxRemarkLen {
		return 0, fmt.Errorf("%w: 备注长度超过 %d", ErrInvalidRequest, maxRemarkLen)
	}
	if target == models.DiffHandleIgnored && remark == "" {
		// 忽略一笔长短款是有财务后果的决定,必须留下理由(评审 D7)。
		return 0, fmt.Errorf("%w: 置为已忽略必须携带备注", ErrInvalidRequest)
	}
	err := s.d.Store.WithinTx(ctx, func(ctx context.Context, r store.Repos) error {
		d, err := r.Reconcile().GetDiff(ctx, id, true) // 行锁:并发处置串行化,败方拿 FailedPrecondition
		if err != nil {
			return err
		}
		if !models.CanDiffHandleTransit(d.HandleStatus, target) {
			return fmt.Errorf("%w: 处置状态不可从 %d 迁移到 %d", models.ErrIllegalTransition, d.HandleStatus, target)
		}
		now := s.d.Clock()
		if err := r.Reconcile().UpdateDiffHandling(ctx, id, target, actor.String(), now, remark); err != nil {
			return err
		}
		return r.OpsAudit().Insert(ctx, &models.OpsAuditLog{
			Actor: actor.String(), Action: models.OpsActionReconcileDiffHandle,
			TargetType: models.OpsTargetReconcileDiff, TargetID: fmt.Sprintf("%d", id),
			Detail: fmt.Sprintf("handle_status %d→%d remark=%q", d.HandleStatus, target, remark),
		})
	})
	if err != nil {
		return 0, err
	}
	return target, nil
}

// ListDeadLetters 死信清单(只读,游标分页;不含 payload——事件体经 ListOrderEvents 获取)。
func (s *Service) ListDeadLetters(ctx context.Context, f store.DeadFilter, afterID uint64, limit int32) ([]models.EventOutbox, uint64, error) {
	n, err := normLimit(limit)
	if err != nil {
		return nil, 0, err
	}
	rows, err := s.d.Store.Reader().Outbox().ListDead(ctx, f, afterID, n)
	if err != nil {
		return nil, 0, err
	}
	var last uint64
	if len(rows) > 0 {
		last = rows[len(rows)-1].ID
	}
	return rows, nextCursor(rows, n, last), nil
}

// RequeueDeadLetter 人工重放单条死信:仅 Dead 可重放;attempts 不重置
// (一次重放 = 一次投递机会,再失败立即回死信);同事务写审计。返回重放时的尝试次数。
func (s *Service) RequeueDeadLetter(ctx context.Context, actor Actor, eventID string) (int32, error) {
	var attempts int32
	err := s.d.Store.WithinTx(ctx, func(ctx context.Context, r store.Repos) error {
		ob, err := r.Outbox().GetByEventID(ctx, eventID, true) // 行锁:与投递/兜底 worker 串行化
		if err != nil {
			return err
		}
		if ob.Status != models.OutboxDead {
			return fmt.Errorf("%w: 仅死信可重放(当前 status=%d)", models.ErrIllegalTransition, ob.Status)
		}
		attempts = ob.Attempts
		if err := r.Outbox().Requeue(ctx, ob.ID, s.d.Clock()); err != nil {
			return err
		}
		return r.OpsAudit().Insert(ctx, &models.OpsAuditLog{
			Actor: actor.String(), Action: models.OpsActionDeadLetterRequeue,
			TargetType: models.OpsTargetEventOutbox, TargetID: eventID,
			Detail: fmt.Sprintf("attempts=%d app=%s order=%s", ob.Attempts, ob.AppID, ob.OrderNo),
		})
	})
	if err != nil {
		return 0, err
	}
	return attempts, nil
}

// ListAnomalyTxns 资金异常流水清单(只读,游标分页)。
func (s *Service) ListAnomalyTxns(ctx context.Context, f store.AnomalyFilter, afterID uint64, limit int32) ([]models.PaymentTransaction, uint64, error) {
	n, err := normLimit(limit)
	if err != nil {
		return nil, 0, err
	}
	rows, err := s.d.Store.Reader().Transactions().ListAnomalies(ctx, f, afterID, n)
	if err != nil {
		return nil, 0, err
	}
	var last uint64
	if len(rows) > 0 {
		last = rows[len(rows)-1].ID
	}
	return rows, nextCursor(rows, n, last), nil
}
