package gormstore

import (
	"context"
	"crypto/sha256"
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
	if b.AlgorithmVersion == 0 {
		b.AlgorithmVersion = 1
	}
	// DATE 是日历标签，不能让驱动按 DSN loc 转换东八区午夜。
	db := dbOf(ctx, r.base)
	row := map[string]any{"batch_no": b.BatchNo, "app_id": b.AppID, "channel": b.Channel, "pay_account_id": b.PayAccountID, "bill_date": b.BillDate.Format(dateLayout), "algorithm_version": b.AlgorithmVersion, "status": b.Status,
		"locked_by": b.LockedBy, "locked_until": b.LockedUntil, "last_error": b.LastError, "source_count": b.SourceCount, "matched_count": b.MatchedCount, "diff_count": b.DiffCount, "source_amount": b.SourceAmount, "local_amount": b.LocalAmount, "source_refund_amount": b.SourceRefundAmount, "local_refund_amount": b.LocalRefundAmount, "run_id": b.RunID, "source_run_id": b.SourceRunID, "published_run_id": b.PublishedRunID, "published_source_run_id": b.PublishedSourceRunID, "import_complete": b.ImportComplete, "compare_after_id": b.CompareAfterID, "dirty_seq": b.DirtySeq, "run_dirty_seq": b.RunDirtySeq, "published_dirty_seq": b.PublishedDirtySeq, "pulled_at": b.PulledAt, "reconciled_at": b.ReconciledAt}

	if err := mapDup(db.Model(&models.ReconcileBatch{}).Create(row).Error, "create reconcile batch"); err != nil {
		return err
	}
	var stored struct{ ID uint64 }
	if err := db.Model(&models.ReconcileBatch{}).Select("id").Where("batch_no = ?", b.BatchNo).Take(&stored).Error; err != nil {
		return fmt.Errorf("read created reconcile batch: %w", err)
	}
	b.ID = stored.ID
	return nil
}

func (r reconcileRepo) GetBatchByAccountDate(ctx context.Context, channel, payAccountID string, billDate time.Time, version ...int16) (*models.ReconcileBatch, error) {
	var b models.ReconcileBatch
	// bill_date 为 DATE 列,用 date-only 串比较,避免 DATETIME 字面量导致的不等。
	q := dbOf(ctx, r.base).Where("channel = ? AND pay_account_id = ? AND bill_date = ?", channel, payAccountID, billDate.Format(dateLayout))
	if len(version) > 0 {
		q = q.Where("algorithm_version = ?", version[0])
	}
	err := q.Order("algorithm_version DESC").Take(&b).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("gormstore: get reconcile batch: %w", err)
	}
	return &b, nil
}

func (r reconcileRepo) GetScanAccount(ctx context.Context, payAccountID string, fu bool) (*models.AppPayAccount, error) {
	var a models.AppPayAccount
	err := forUpdate(dbOf(ctx, r.base), fu).Where("pay_account_id = ?", payAccountID).Take(&a).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, store.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("gormstore: get reconcile scan account: %w", err)
	}
	return &a, nil
}

func (r reconcileRepo) UpdateScanCursor(ctx context.Context, payAccountID string, through time.Time, version ...int16) error {
	fields := map[string]any{"reconcile_scanned_through": through.Format(dateLayout)}
	if len(version) > 0 {
		fields["reconcile_scan_version"] = version[0]
	}
	err := dbOf(ctx, r.base).Model(&models.AppPayAccount{}).Where("pay_account_id = ?", payAccountID).Updates(fields).Error
	if err != nil {
		return fmt.Errorf("gormstore: update reconcile scan cursor: %w", err)
	}
	return nil
}

func (r reconcileRepo) ClaimBatch(ctx context.Context, id uint64, claimToken string, now, leaseUntil time.Time) (bool, error) {
	if claimToken == "" {
		return false, nil
	}
	result := dbOf(ctx, r.base).Model(&models.ReconcileBatch{}).
		Where("id = ? AND status IN ?", id, []models.BatchStatus{models.BatchPending, models.BatchPulled, models.BatchFailed}).
		Where("locked_until IS NULL OR locked_until <= ?", now).
		Updates(map[string]any{"locked_by": claimToken, "locked_until": leaseUntil, "updated_at": now})
	if result.Error != nil {
		return false, fmt.Errorf("gormstore: claim reconcile batch: %w", result.Error)
	}
	return result.RowsAffected == 1, nil
}

func (r reconcileRepo) ClaimNextBatch(ctx context.Context, channel, payAccountID, claimToken string, now, leaseUntil time.Time, version ...int16) (*models.ReconcileBatch, error) {
	for _, status := range []models.BatchStatus{models.BatchPending, models.BatchPulled, models.BatchFailed} {
		var b models.ReconcileBatch
		q := skipLocked(dbOf(ctx, r.base))
		if len(version) > 0 {
			q = q.Where("algorithm_version = ?", version[0])
		}
		err := q.
			Where("channel = ? AND pay_account_id = ? AND status = ?", channel, payAccountID, status).
			Where("locked_until IS NULL OR locked_until <= ?", now).
			Order("locked_until, id").Limit(1).Take(&b).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("gormstore: find due reconcile batch: %w", err)
		}
		applied, err := r.ClaimBatch(ctx, b.ID, claimToken, now, leaseUntil)
		if err != nil || !applied {
			return nil, err
		}
		b.LockedBy, b.LockedUntil = claimToken, nullTime(leaseUntil)
		return &b, nil
	}
	return nil, nil
}

func (r reconcileRepo) UpdateBatch(ctx context.Context, b *models.ReconcileBatch, claimToken string) (bool, error) {
	if claimToken == "" {
		return false, nil
	}
	fields := map[string]any{
		"status": b.Status, "locked_by": b.LockedBy, "locked_until": b.LockedUntil, "last_error": b.LastError,
	}
	if b.Status == models.BatchDone {
		fields["source_count"], fields["matched_count"], fields["diff_count"] = b.SourceCount, b.MatchedCount, b.DiffCount
		fields["source_amount"], fields["local_amount"] = b.SourceAmount, b.LocalAmount
		fields["pulled_at"], fields["reconciled_at"] = b.PulledAt, b.ReconciledAt
	}
	result := dbOf(ctx, r.base).Model(&models.ReconcileBatch{}).
		Where("id = ? AND status <> ? AND locked_by = ?", b.ID, models.BatchDone, claimToken).
		Updates(fields)
	if result.Error != nil {
		return false, fmt.Errorf("gormstore: update reconcile batch: %w", result.Error)
	}
	return result.RowsAffected == 1, nil
}

func (r reconcileRepo) InsertDiff(ctx context.Context, d *models.ReconcileDiff) error {
	if d.Kind == 0 {
		d.Kind = models.TxnPay
	}
	if d.IdentityKey == "" {
		d.IdentityKey = fmt.Sprintf("%x", sha256.Sum256([]byte(d.Channel+"\x00"+d.TransactionID)))
	}
	return mapDup(dbOf(ctx, r.base).Create(d).Error, "insert reconcile diff")
}

func (r reconcileRepo) ListDiffs(ctx context.Context, f store.DiffFilter, afterID uint64, limit int) ([]models.ReconcileDiff, error) {
	q := dbOf(ctx, r.base).Where("id > ?", afterID).Where(publishedDiffSQL)
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
	db := dbOf(ctx, r.base)
	var d models.ReconcileDiff
	if !fu {
		err := db.Where("id = ?", id).Where(publishedDiffSQL).Take(&d).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, store.ErrNotFound
		}
		if err != nil {
			return nil, fmt.Errorf("gormstore: get reconcile diff: %w", err)
		}
		return &d, nil
	}
	// 人工处置必须与发布串行：先读不可变归属，再 batch→diff 当前读；禁止 diff→batch。
	var locator models.ReconcileDiff
	if err := db.Select("id", "batch_id", "run_id").Where("id = ?", id).Take(&locator).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("gormstore: locate reconcile diff: %w", err)
	}
	var batch models.ReconcileBatch
	if locator.RunID != "" {
		if err := forUpdate(db, true).Where("id = ?", locator.BatchID).Take(&batch).Error; err != nil {
			return nil, fmt.Errorf("gormstore: lock diff batch: %w", err)
		}
	}
	if err := forUpdate(db, true).Where("id = ?", id).Take(&d).Error; err != nil {
		return nil, fmt.Errorf("gormstore: lock reconcile diff: %w", err)
	}
	if d.RunID != "" && d.RunID != batch.PublishedRunID {
		return nil, store.ErrNotFound
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
	db := dbOf(ctx, r.base)
	if handledBy != models.OpsActorSystem {
		// 调用方已通过 GetDiff(forUpdate=true) 持有 batch→diff 锁。当前读不能复用旧RR快照。
		current, err := r.GetDiff(ctx, id, true)
		if err != nil {
			return err
		}
		if current.RunID != "" {
			var batch models.ReconcileBatch
			// batch 已由 GetDiff 锁定；此处复读不会引入新取锁顺序。
			if err := forUpdate(db, true).Where("id = ?", current.BatchID).Take(&batch).Error; err != nil {
				return fmt.Errorf("gormstore: read handling batch: %w", err)
			}
			if current.RunID != batch.PublishedRunID {
				return store.ErrNotFound
			}
			if batch.RunID != "" && batch.RunID != batch.PublishedRunID {
				inherited := map[string]any{"handle_status": status, "handled_by": handledBy, "handled_at": handledAt, "handle_remark": current.HandleRemark}
				if remark != "" {
					inherited["handle_remark"] = remark
				}
				if err := db.Model(&models.ReconcileDiff{}).Where("batch_id = ? AND run_id = ? AND kind = ? AND identity_key = ? AND diff_type = ?", current.BatchID, batch.RunID, current.Kind, current.IdentityKey, current.DiffType).Updates(inherited).Error; err != nil {
					return fmt.Errorf("gormstore: propagate manual disposition: %w", err)
				}
			}
		}
	}
	if err := db.Model(&models.ReconcileDiff{}).Where("id = ?", id).Updates(fields).Error; err != nil {
		return fmt.Errorf("gormstore: update diff handling: %w", err)
	}
	return nil
}
