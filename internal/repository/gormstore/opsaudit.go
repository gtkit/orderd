package gormstore

import (
	"context"
	"fmt"

	"github.com/gtkit/orderd/internal/models"
	"gorm.io/gorm"
)

// opsAuditRepo 运维审计仓储(只插不改;更新/删除不在端口上,结构上不可达)。
type opsAuditRepo struct{ base *gorm.DB }

func (r opsAuditRepo) Insert(ctx context.Context, l *models.OpsAuditLog) error {
	if err := dbOf(ctx, r.base).Create(l).Error; err != nil {
		return fmt.Errorf("gormstore: insert ops_audit_log: %w", err)
	}
	return nil
}

func (r opsAuditRepo) ListByTarget(ctx context.Context, targetType, targetID string, afterID uint64, limit int) ([]models.OpsAuditLog, error) {
	var out []models.OpsAuditLog
	err := dbOf(ctx, r.base).
		Where("target_type = ? AND target_id = ? AND id > ?", targetType, targetID, afterID).
		Order("id").Limit(limit).Find(&out).Error
	if err != nil {
		return nil, fmt.Errorf("gormstore: list ops_audit_logs: %w", err)
	}
	return out, nil
}
