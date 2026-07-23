package gormstore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/gtkit/orderd/internal/models"
	"gorm.io/gorm"
)

const dateLayout = "2006-01-02"

type reconcileRepo struct{ base *gorm.DB }

func (r reconcileRepo) CreateBatch(ctx context.Context, b *models.ReconcileBatch) error {
	return mapDup(dbOf(ctx, r.base).Create(b).Error, "create reconcile batch")
}

func (r reconcileRepo) GetBatchByAccountDate(ctx context.Context, channel, payAccountID string, billDate time.Time) (*models.ReconcileBatch, error) {
	var b models.ReconcileBatch
	// bill_date 为 DATE 列,用 date-only 串比较,避免 DATETIME 字面量导致的不等。
	err := dbOf(ctx, r.base).
		Where("channel = ? AND pay_account_id = ? AND bill_date = ?", channel, payAccountID, billDate.Format(dateLayout)).
		Take(&b).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("gormstore: get reconcile batch: %w", err)
	}
	return &b, nil
}

func (r reconcileRepo) UpdateBatch(ctx context.Context, b *models.ReconcileBatch) error {
	err := dbOf(ctx, r.base).Model(&models.ReconcileBatch{}).Where("id = ?", b.ID).
		Select("Status", "SourceCount", "MatchedCount", "DiffCount", "SourceAmount", "LocalAmount",
			"LockedBy", "LockedUntil", "PulledAt", "ReconciledAt", "LastError").
		Updates(b).Error
	if err != nil {
		return fmt.Errorf("gormstore: update reconcile batch: %w", err)
	}
	return nil
}

func (r reconcileRepo) InsertDiff(ctx context.Context, d *models.ReconcileDiff) error {
	return mapDup(dbOf(ctx, r.base).Create(d).Error, "insert reconcile diff")
}
