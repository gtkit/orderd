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

func (r reconcileRepo) ListDiffs(ctx context.Context, f store.DiffFilter, afterID uint64, limit int) ([]models.ReconcileDiff, error) {
	q := dbOf(ctx, r.base).Where("id > ?", afterID)
	if f.HandleStatus != nil {
		q = q.Where("handle_status = ?", *f.HandleStatus)
	}
	if f.DiffType != nil {
		q = q.Where("diff_type = ?", *f.DiffType)
	}
	if f.AppID != "" {
		q = q.Where("app_id = ?", f.AppID)
	}
	if f.Channel != "" {
		q = q.Where("channel = ?", f.Channel)
	}
	if f.PayAccountID != "" {
		q = q.Where("pay_account_id = ?", f.PayAccountID)
	}
	// 账单日期经所属批次判定(差异行本身无 bill_date)。
	if !f.BillDateFrom.IsZero() || !f.BillDateTo.IsZero() {
		sub := dbOf(ctx, r.base).Model(&models.ReconcileBatch{}).Select("id")
		if !f.BillDateFrom.IsZero() {
			sub = sub.Where("bill_date >= ?", f.BillDateFrom.Format(dateLayout))
		}
		if !f.BillDateTo.IsZero() {
			sub = sub.Where("bill_date <= ?", f.BillDateTo.Format(dateLayout))
		}
		q = q.Where("batch_id IN (?)", sub)
	}
	var out []models.ReconcileDiff
	if err := q.Order("id").Limit(limit).Find(&out).Error; err != nil {
		return nil, fmt.Errorf("gormstore: list reconcile diffs: %w", err)
	}
	return out, nil
}

func (r reconcileRepo) GetDiff(ctx context.Context, id uint64, fu bool) (*models.ReconcileDiff, error) {
	var d models.ReconcileDiff
	err := forUpdate(dbOf(ctx, r.base), fu).Where("id = ?", id).Take(&d).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, store.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("gormstore: get reconcile diff: %w", err)
	}
	return &d, nil
}

func (r reconcileRepo) UpdateDiffHandling(ctx context.Context, id uint64, status models.DiffHandleStatus, handledBy string, handledAt time.Time, remark string) error {
	fields := map[string]any{
		"handle_status": status, "handled_by": handledBy, "handled_at": handledAt,
	}
	if remark != "" { // 空备注保留原值(占坑→解决的两段处置不丢首段备注)
		fields["handle_remark"] = remark
	}
	err := dbOf(ctx, r.base).Model(&models.ReconcileDiff{}).Where("id = ?", id).Updates(fields).Error
	if err != nil {
		return fmt.Errorf("gormstore: update diff handling: %w", err)
	}
	return nil
}
