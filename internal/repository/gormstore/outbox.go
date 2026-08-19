package gormstore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/gtkit/orderd/internal/models"
	"github.com/gtkit/orderd/internal/store"
	"gorm.io/gorm"
)

type outboxRepo struct{ base *gorm.DB }

// ClaimDue 用 FOR UPDATE SKIP LOCKED 认领到期的待投递行并打租约(多副本安全)。须在事务内调用。
func (r outboxRepo) ClaimDue(ctx context.Context, workerID string, now, leaseUntil time.Time, limit int) ([]*models.EventOutbox, error) {
	var out []*models.EventOutbox
	err := skipLocked(dbOf(ctx, r.base)).
		Where("status = ?", models.OutboxPending).
		Where("next_attempt_at <= ?", now).
		Where("locked_until IS NULL OR locked_until < ?", now).
		Order("next_attempt_at").Limit(limit).Find(&out).Error
	if err != nil {
		return nil, fmt.Errorf("gormstore: claim due outbox: %w", err)
	}
	idlist := make([]uint64, len(out))
	for i, o := range out {
		idlist[i] = o.ID
	}
	if err := claimLease(ctx, r.base, &models.EventOutbox{}, idlist, workerID, leaseUntil); err != nil {
		return nil, err
	}
	for _, o := range out {
		o.LockedBy = workerID
		o.LockedUntil = nullTime(leaseUntil)
	}
	return out, nil
}

func (r outboxRepo) MarkDelivered(ctx context.Context, id uint64) error {
	return r.update(ctx, id, map[string]any{
		"status": models.OutboxDelivered, "locked_by": "", "locked_until": nil,
	}, "mark delivered")
}

func (r outboxRepo) Reschedule(ctx context.Context, id uint64, nextAttemptAt time.Time, attempts int32, lastErr string) error {
	return r.update(ctx, id, map[string]any{
		"attempts": attempts, "next_attempt_at": nextAttemptAt, "last_error": nullOrNil(lastErr),
		"locked_by": "", "locked_until": nil,
	}, "reschedule")
}

func (r outboxRepo) MarkDead(ctx context.Context, id uint64, attempts int32, lastErr string) error {
	return r.update(ctx, id, map[string]any{
		"status": models.OutboxDead, "attempts": attempts, "last_error": nullOrNil(lastErr),
		"locked_by": "", "locked_until": nil,
	}, "mark dead")
}

func (r outboxRepo) Requeue(ctx context.Context, id uint64, nextAttemptAt time.Time) error {
	return r.update(ctx, id, map[string]any{
		"status": models.OutboxPending, "next_attempt_at": nextAttemptAt, "last_error": nil,
		"locked_by": "", "locked_until": nil,
	}, "requeue")
}

func (r outboxRepo) ListDead(ctx context.Context, f store.DeadFilter, afterID uint64, limit int) ([]models.EventOutbox, error) {
	q := dbOf(ctx, r.base).Where("status = ? AND id > ?", models.OutboxDead, afterID)
	if f.AppID != "" {
		q = q.Where("app_id = ?", f.AppID)
	}
	if f.EventType != "" {
		q = q.Where("event_type = ?", f.EventType)
	}
	if f.OrderNo != "" {
		q = q.Where("order_no = ?", f.OrderNo)
	}
	var out []models.EventOutbox
	if err := q.Order("id").Limit(limit).Find(&out).Error; err != nil {
		return nil, fmt.Errorf("gormstore: list dead letters: %w", err)
	}
	return out, nil
}

func (r outboxRepo) GetByEventID(ctx context.Context, eventID string, fu bool) (*models.EventOutbox, error) {
	var ob models.EventOutbox
	err := forUpdate(dbOf(ctx, r.base), fu).Where("event_id = ?", eventID).Take(&ob).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, store.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("gormstore: get outbox by event_id: %w", err)
	}
	return &ob, nil
}

// ClaimDeadForRequeue 兜底重放认领:行锁(SKIP LOCKED)保持到事务提交,多副本互斥。
// 必须在事务内调用,且同事务完成 Requeue + 审计(spec:兜底重放与审计恰好配对)。
func (r outboxRepo) ClaimDeadForRequeue(ctx context.Context, cond store.DeadRequeueCond, limit int) ([]*models.EventOutbox, error) {
	var out []*models.EventOutbox
	err := skipLocked(dbOf(ctx, r.base)).
		Where("status = ?", models.OutboxDead).
		Where("updated_at <= ?", cond.UpdatedBefore).
		Where("attempts < ?", cond.AttemptsBelow).
		Where("created_at >= ?", cond.CreatedAfter).
		Order("id").Limit(limit).Find(&out).Error
	if err != nil {
		return nil, fmt.Errorf("gormstore: claim dead for requeue: %w", err)
	}
	return out, nil
}

func (r outboxRepo) update(ctx context.Context, id uint64, fields map[string]any, what string) error {
	err := dbOf(ctx, r.base).Model(&models.EventOutbox{}).Where("id = ?", id).Updates(fields).Error
	if err != nil {
		return fmt.Errorf("gormstore: outbox %s: %w", what, err)
	}
	return nil
}

// nullOrNil 空串映射为 SQL NULL(与旧 nullStr 语义一致),否则原值。
func nullOrNil(s string) any {
	if s == "" {
		return nil
	}
	return s
}
