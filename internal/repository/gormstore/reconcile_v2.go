package gormstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/gtkit/orderd/internal/models"
	"github.com/gtkit/orderd/internal/store"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const publishedDiffSQL = "(run_id = '' OR EXISTS (SELECT 1 FROM reconcile_batches b WHERE b.id = reconcile_diffs.batch_id AND b.published_run_id = reconcile_diffs.run_id))"

// SaveRun 必须在调用方的短事务内执行，行锁把旧 token、导入代次和发布切换串行化。
func (r reconcileRepo) SaveRun(ctx context.Context, b *models.ReconcileBatch, token, expectedRun string, now time.Time, entries []models.ReconcileEntry, diffs []models.ReconcileDiff, publish bool) (bool, error) {
	if len(entries) > 100 || len(diffs) > 100 {
		return false, fmt.Errorf("reconcile write page exceeds 100")
	}
	db := dbOf(ctx, r.base)
	var cur models.ReconcileBatch
	if err := forUpdate(db, true).Where("id = ?", b.ID).Take(&cur).Error; err != nil {
		return false, fmt.Errorf("load reconcile run: %w", err)
	}
	if token == "" || cur.LockedBy != token || cur.RunID != expectedRun || !cur.LockedUntil.Valid || !cur.LockedUntil.Time.After(now) {
		return false, nil
	}
	if publish && (!b.ImportComplete || b.RunID == "") {
		return false, fmt.Errorf("cannot publish incomplete import")
	}
	for i := range entries {
		entries[i].ID = 0
		entries[i].BatchID = b.ID
		entries[i].RunID = b.SourceRunID
	}
	if len(entries) > 0 {
		if err := mapDup(db.Create(&entries).Error, "stage bill entries"); err != nil {
			return false, err
		}
	}
	for i := range diffs {
		diffs[i].ID = 0
		diffs[i].BatchID = b.ID
		diffs[i].RunID = b.RunID
		if cur.PublishedRunID != "" {
			var prior models.ReconcileDiff
			err := forUpdate(db, true).Where("batch_id = ? AND run_id = ? AND kind = ? AND identity_key = ? AND diff_type = ? AND handle_status <> ? AND handled_by <> ?", b.ID, cur.PublishedRunID, diffs[i].Kind, diffs[i].IdentityKey, diffs[i].DiffType, models.DiffHandlePending, models.OpsActorSystem).Take(&prior).Error
			if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
				return false, err
			}
			if err == nil {
				diffs[i].HandleStatus = prior.HandleStatus
				diffs[i].HandledBy = prior.HandledBy
				diffs[i].HandledAt = prior.HandledAt
				diffs[i].HandleRemark = prior.HandleRemark
			}
		}
	}
	if len(diffs) > 0 {
		if err := mapDup(db.Create(&diffs).Error, "stage reconcile diffs"); err != nil {
			return false, err
		}
	}
	b.DirtySeq = cur.DirtySeq
	b.PublishedRunID = cur.PublishedRunID
	b.PublishedSourceRunID = cur.PublishedSourceRunID
	b.PublishedDirtySeq = cur.PublishedDirtySeq
	if publish {
		b.PublishedRunID = b.RunID
		b.PublishedSourceRunID = b.SourceRunID
		b.PublishedDirtySeq = b.RunDirtySeq
		if b.DirtySeq > b.RunDirtySeq {
			b.Status = models.BatchPending
		}
	}
	b.UpdatedAt = now
	err := db.Model(&models.ReconcileBatch{}).Where("id = ?", b.ID).Select("SourceDigestAlgorithm", "SourceDigest", "Status", "RunID", "SourceRunID", "PublishedSourceRunID", "PublishedRunID", "ImportComplete", "CompareAfterID", "RunDirtySeq", "PublishedDirtySeq", "SourceCount", "MatchedCount", "DiffCount", "SourceAmount", "SourceRefundAmount", "LocalAmount", "LocalRefundAmount", "LockedBy", "LockedUntil", "PulledAt", "ReconciledAt", "LastError", "UpdatedAt").Updates(b).Error
	if err != nil {
		return false, fmt.Errorf("save reconcile run: %w", err)
	}
	return true, nil
}
func (r reconcileRepo) ListEntries(ctx context.Context, id uint64, run string, after uint64, limit int) ([]models.ReconcileEntry, error) {
	var out []models.ReconcileEntry
	err := dbOf(ctx, r.base).Where("batch_id = ? AND run_id = ? AND id > ?", id, run, after).Order("id").Limit(limit).Find(&out).Error
	return out, err
}
func (r reconcileRepo) GetEntry(ctx context.Context, id uint64) (*models.ReconcileEntry, error) {
	var e models.ReconcileEntry
	err := dbOf(ctx, r.base).Where("id = ?", id).Take(&e).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, store.ErrNotFound
	}
	return &e, err
}
func (r reconcileRepo) FindPublishedEntry(ctx context.Context, ch, account string, kind models.TxnKind, key string) (*models.ReconcileEntry, error) {
	var entries []models.ReconcileEntry
	err := dbOf(ctx, r.base).Where("channel = ? AND pay_account_id = ? AND kind = ? AND identity_key = ?", ch, account, kind, key).Where("EXISTS (SELECT 1 FROM reconcile_batches b WHERE b.id = reconcile_bill_entries.batch_id AND b.published_source_run_id = reconcile_bill_entries.run_id)").Limit(2).Find(&entries).Error
	if err != nil {
		return nil, err
	}
	if len(entries) > 1 {
		return nil, store.ErrAmbiguous
	}
	if len(entries) == 0 {
		return nil, nil
	}
	return &entries[0], nil
}
func (r reconcileRepo) RequestRecheck(ctx context.Context, id, entryID uint64) error {
	// 不取资金聚合锁。当前片正在运行时仅递增 dirty_seq，由最终发布检测是否需再跑。
	return dbOf(ctx, r.base).Model(&models.ReconcileBatch{}).Where("id = ?", id).Where("EXISTS (SELECT 1 FROM reconcile_diffs d WHERE d.batch_id = reconcile_batches.id AND d.run_id = reconcile_batches.published_run_id AND d.entry_id = ?)", entryID).Updates(map[string]any{"dirty_seq": gorm.Expr("dirty_seq + 1"), "locked_until": gorm.Expr("IF(status = ?, NULL, locked_until)", models.BatchDone), "status": gorm.Expr("IF(status = ?, ?, status)", models.BatchDone, models.BatchPending)}).Error
}
func (r reconcileRepo) ListLocalTxnPage(ctx context.Context, ch, account string, after uint64, limit int) ([]models.PaymentTransaction, error) {
	var out []models.PaymentTransaction
	err := dbOf(ctx, r.base).Where("channel = ? AND pay_account_id = ? AND id > ?", ch, account, after).Order("id").Limit(limit).Find(&out).Error
	return out, err
}
func (r reconcileRepo) UpdateLocalSeedCursor(ctx context.Context, account string, after uint64) error {
	return dbOf(ctx, r.base).Model(&models.AppPayAccount{}).Where("pay_account_id = ? AND reconcile_local_seed_cursor < ?", account, after).Update("reconcile_local_seed_cursor", after).Error
}
func (r reconcileRepo) EnqueueLocalTxn(ctx context.Context, id uint64, now time.Time) error {
	var t models.PaymentTransaction
	if err := dbOf(ctx, r.base).Select("id", "channel", "pay_account_id").Where("id = ?", id).Take(&t).Error; err != nil {
		return err
	}
	return dbOf(ctx, r.base).Clauses(clause.OnConflict{DoNothing: true}).Create(&models.ReconcileLocalChange{TxnID: id, Channel: t.Channel, PayAccountID: t.PayAccountID, NextAt: now}).Error
}
func (r reconcileRepo) ClaimLocalTxn(ctx context.Context, ch, account, token string, now, until time.Time) (*models.ReconcileLocalChange, error) {
	if token == "" {
		return nil, nil
	}
	db := dbOf(ctx, r.base)
	var c models.ReconcileLocalChange
	err := skipLocked(db).Where("done = FALSE AND next_at <= ?", now).Where("locked_until IS NULL OR locked_until <= ?", now).Where("channel = ? AND pay_account_id = ?", ch, account).Order("next_at, txn_id").Take(&c).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	c.LockedBy = token
	c.LockedUntil = nullTime(until)
	if err := db.Model(&models.ReconcileLocalChange{}).Where("txn_id = ?", c.TxnID).Updates(map[string]any{"locked_by": token, "locked_until": until}).Error; err != nil {
		return nil, err
	}
	return &c, nil
}
func (r reconcileRepo) FinishLocalTxn(ctx context.Context, id uint64, token string, now, next time.Time, reason string) (bool, error) {
	if token == "" {
		return false, nil
	}
	fields := map[string]any{"done": next.IsZero(), "attempts": gorm.Expr("attempts + 1"), "locked_by": "", "locked_until": nil, "last_error": sql.NullString{String: reason, Valid: reason != ""}}
	if !next.IsZero() {
		fields["next_at"] = next
	}
	result := dbOf(ctx, r.base).Model(&models.ReconcileLocalChange{}).Where("txn_id = ? AND locked_by = ? AND locked_until > ? AND done = FALSE", id, token, now).Updates(fields)
	return result.RowsAffected == 1, result.Error
}
func (r reconcileRepo) resolve(ctx context.Context, d *models.ReconcileDiff, now time.Time, reason string) error {
	if err := r.UpdateDiffHandling(ctx, d.ID, models.DiffHandleResolved, models.OpsActorSystem, now, ""); err != nil {
		return err
	}
	return dbOf(ctx, r.base).Create(&models.OpsAuditLog{Actor: models.OpsActorSystem, Action: models.OpsActionReconcileDiffHandle, TargetType: models.OpsTargetReconcileDiff, TargetID: strconv.FormatUint(d.ID, 10), Detail: reason, CreatedAt: now}).Error
}
func (r reconcileRepo) SetLocalDiff(ctx context.Context, id uint64, want *models.ReconcileDiff, now time.Time) (bool, error) {
	db := dbOf(ctx, r.base)
	var d models.ReconcileDiff
	err := forUpdate(db, true).Where("batch_id = 0 AND local_txn_id = ?", id).Take(&d).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		if want == nil {
			return false, nil
		}
		want.LocalTxnID = id
		err := r.InsertDiff(ctx, want)
		return err == nil, err
	}
	if err != nil {
		return false, fmt.Errorf("gormstore: read local difference: %w", err)
	}
	if d.HandleStatus != models.DiffHandlePending {
		return false, nil
	}
	if want == nil {
		err := r.resolve(ctx, &d, now, `{"reason":"published_bill_matched"}`)
		return err == nil, err
	}
	changed := d.DiffType != want.DiffType || d.Detail != want.Detail || d.GatewayAmount != want.GatewayAmount
	if !changed {
		return false, nil
	}
	err = db.Model(&models.ReconcileDiff{}).Where("id = ?", d.ID).Updates(map[string]any{"diff_type": want.DiffType, "detail": want.Detail, "gateway_amount": want.GatewayAmount}).Error
	if err != nil {
		return false, fmt.Errorf("gormstore: update local difference: %w", err)
	}
	return true, nil
}
func (r reconcileRepo) ClaimRepair(ctx context.Context, ch, account, token string, now, until time.Time) (*models.ReconcileDiff, error) {
	if token == "" {
		return nil, nil
	}
	db := dbOf(ctx, r.base)
	var d models.ReconcileDiff
	err := skipLocked(db).Where("channel = ? AND pay_account_id = ? AND batch_id > 0 AND run_id <> '' AND entry_id > 0 AND app_id <> '' AND handle_status = ?", ch, account, models.DiffHandlePending).Where(publishedDiffSQL).Where("next_verify_at IS NULL OR next_verify_at <= ?", now).Where("verify_locked_until IS NULL OR verify_locked_until <= ?", now).Order("next_verify_at, id").Take(&d).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	d.VerifyLockedBy = token
	d.VerifyLockedUntil = nullTime(until)
	if err := db.Model(&models.ReconcileDiff{}).Where("id = ?", d.ID).Updates(map[string]any{"verify_locked_by": token, "verify_locked_until": until}).Error; err != nil {
		return nil, err
	}
	return &d, nil
}
func (r reconcileRepo) FinishRepair(ctx context.Context, id uint64, token string, now, next time.Time, success bool, reason string) (bool, error) {
	if token == "" {
		return false, nil
	}
	db := dbOf(ctx, r.base)
	result := db.Model(&models.ReconcileDiff{}).Where("id = ? AND verify_locked_by = ? AND verify_locked_until > ? AND handle_status = ?", id, token, now, models.DiffHandlePending).Where(publishedDiffSQL).Updates(map[string]any{"verify_attempts": gorm.Expr("verify_attempts + 1"), "next_verify_at": sql.NullTime{Time: next, Valid: !success}, "verify_locked_by": "", "verify_locked_until": nil, "verify_last_error": sql.NullString{String: reason, Valid: reason != ""}})
	if result.Error != nil || result.RowsAffected != 1 {
		return false, result.Error
	}
	if success {
		if err := r.resolve(ctx, &models.ReconcileDiff{ID: id}, now, `{"reason":"channel_positive_confirmation"}`); err != nil {
			return false, err
		}
	}
	return true, nil
}

func (r reconcileRepo) ResolveMatchedDiffs(ctx context.Context, batchID uint64, run string, ids []uint64, now time.Time) error {
	if len(ids) == 0 {
		return nil
	}
	if len(ids) > 100 {
		return fmt.Errorf("matched page exceeds 100")
	}
	var diffs []models.ReconcileDiff
	err := forUpdate(dbOf(ctx, r.base), true).Where("batch_id = ? AND run_id = ? AND entry_id IN ? AND handle_status = ?", batchID, run, ids, models.DiffHandlePending).Find(&diffs).Error
	if err != nil {
		return err
	}
	for i := range diffs {
		if err := r.resolve(ctx, &diffs[i], now, `{"reason":"published_bill_matched"}`); err != nil {
			return err
		}
	}
	return nil
}
