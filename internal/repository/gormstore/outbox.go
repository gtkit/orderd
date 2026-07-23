package gormstore

import (
	"context"
	"fmt"
	"time"

	"github.com/gtkit/orderd/internal/models"
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

func (r outboxRepo) MarkDead(ctx context.Context, id uint64, lastErr string) error {
	return r.update(ctx, id, map[string]any{
		"status": models.OutboxDead, "last_error": nullOrNil(lastErr), "locked_by": "", "locked_until": nil,
	}, "mark dead")
}

func (r outboxRepo) Requeue(ctx context.Context, id uint64, nextAttemptAt time.Time) error {
	return r.update(ctx, id, map[string]any{
		"status": models.OutboxPending, "next_attempt_at": nextAttemptAt, "last_error": nil,
		"locked_by": "", "locked_until": nil,
	}, "requeue")
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
