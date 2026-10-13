package memstore

import (
	"cmp"
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/gtkit/orderd/internal/models"
	"github.com/gtkit/orderd/internal/store"
)

func (r reconcileRepo) visible(d models.ReconcileDiff) bool {
	return d.RunID == "" || r.m.d.batches[d.BatchID].PublishedRunID == d.RunID
}

func (r reconcileRepo) SaveRun(ctx context.Context, b *models.ReconcileBatch, token, expectedRun string, now time.Time, entries []models.ReconcileEntry, diffs []models.ReconcileDiff, publish bool) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if len(entries) > 100 || len(diffs) > 100 {
		return false, fmt.Errorf("reconcile write page exceeds 100")
	}
	r.m.mu.Lock()
	defer r.m.mu.Unlock()
	cur, ok := r.m.d.batches[b.ID]
	if !ok || token == "" || cur.LockedBy != token || cur.RunID != expectedRun || !cur.LockedUntil.Valid || !cur.LockedUntil.Time.After(now) {
		return false, nil
	}
	for _, e := range entries {
		for _, existing := range r.m.d.entries {
			if existing.BatchID == b.ID && existing.RunID == b.SourceRunID && existing.Kind == e.Kind && existing.IdentityKey == e.IdentityKey {
				return false, store.ErrDuplicate
			}
		}
		r.m.d.seq++
		e.ID = r.m.d.seq
		e.BatchID = b.ID
		e.RunID = b.SourceRunID
		r.m.d.entries[e.ID] = e
	}
	for _, d := range diffs {
		for _, prior := range r.m.d.diffs {
			if prior.BatchID == b.ID && prior.RunID == cur.PublishedRunID && prior.Kind == d.Kind && prior.IdentityKey == d.IdentityKey && prior.DiffType == d.DiffType && prior.HandleStatus != models.DiffHandlePending && prior.HandledBy != models.OpsActorSystem {
				d.HandleStatus = prior.HandleStatus
				d.HandledBy = prior.HandledBy
				d.HandledAt = prior.HandledAt
				d.HandleRemark = prior.HandleRemark
				break
			}
		}
		r.m.d.seq++
		d.ID = r.m.d.seq
		d.BatchID = b.ID
		d.RunID = b.RunID
		r.m.d.diffs = append(r.m.d.diffs, d)
	}
	b.DirtySeq = cur.DirtySeq
	if publish {
		if !b.ImportComplete || b.RunID == "" {
			return false, fmt.Errorf("cannot publish incomplete import")
		}
		b.PublishedRunID = b.RunID
		b.PublishedSourceRunID = b.SourceRunID
		b.PublishedDirtySeq = b.RunDirtySeq
		if b.DirtySeq > b.RunDirtySeq {
			b.Status = models.BatchPending
		}
	} else {
		b.PublishedRunID = cur.PublishedRunID
		b.PublishedSourceRunID = cur.PublishedSourceRunID
		b.PublishedDirtySeq = cur.PublishedDirtySeq
	}
	b.UpdatedAt = now
	r.m.d.batches[b.ID] = *b
	return true, nil
}

func (r reconcileRepo) ListEntries(_ context.Context, batchID uint64, run string, afterID uint64, limit int) ([]models.ReconcileEntry, error) {
	r.m.mu.Lock()
	defer r.m.mu.Unlock()
	var out []models.ReconcileEntry
	for _, e := range r.m.d.entries {
		if e.BatchID == batchID && e.RunID == run && e.ID > afterID {
			out = append(out, e)
		}
	}
	slices.SortFunc(out, func(a, b models.ReconcileEntry) int { return cmp.Compare(a.ID, b.ID) })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}
func (r reconcileRepo) GetEntry(_ context.Context, id uint64) (*models.ReconcileEntry, error) {
	r.m.mu.Lock()
	defer r.m.mu.Unlock()
	e, ok := r.m.d.entries[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	return &e, nil
}
func (r reconcileRepo) FindPublishedEntry(_ context.Context, ch, account string, kind models.TxnKind, key string) (*models.ReconcileEntry, error) {
	r.m.mu.Lock()
	defer r.m.mu.Unlock()
	var best *models.ReconcileEntry
	for _, e := range r.m.d.entries {
		if e.Channel == ch && e.PayAccountID == account && e.Kind == kind && e.IdentityKey == key && r.m.d.batches[e.BatchID].PublishedSourceRunID == e.RunID {
			if best != nil {
				return nil, store.ErrAmbiguous
			}
			copy := e
			best = &copy
		}
	}
	return best, nil
}
func (r reconcileRepo) RequestRecheck(_ context.Context, id, entryID uint64) error {
	r.m.mu.Lock()
	defer r.m.mu.Unlock()
	b, ok := r.m.d.batches[id]
	if !ok {
		return store.ErrNotFound
	}
	found := false
	for _, d := range r.m.d.diffs {
		if d.BatchID == id && d.RunID == b.PublishedRunID && d.EntryID == entryID {
			found = true
			break
		}
	}
	if !found {
		return nil
	}
	b.DirtySeq++
	if b.Status == models.BatchDone {
		b.Status = models.BatchPending
		b.LockedUntil = sql.NullTime{}
	}
	r.m.d.batches[id] = b
	return nil
}
func (r reconcileRepo) ListLocalTxnPage(_ context.Context, ch, account string, after uint64, limit int) ([]models.PaymentTransaction, error) {
	r.m.mu.Lock()
	defer r.m.mu.Unlock()
	var out []models.PaymentTransaction
	for _, t := range r.m.d.txns {
		if t.Channel == ch && t.PayAccountID == account && t.ID > after {
			out = append(out, t)
		}
	}
	slices.SortFunc(out, func(a, b models.PaymentTransaction) int { return cmp.Compare(a.ID, b.ID) })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}
func (r reconcileRepo) UpdateLocalSeedCursor(_ context.Context, account string, after uint64) error {
	r.m.mu.Lock()
	defer r.m.mu.Unlock()
	a, ok := r.m.d.accounts[account]
	if !ok {
		return store.ErrNotFound
	}
	if after > a.ReconcileLocalSeedCursor {
		a.ReconcileLocalSeedCursor = after
		r.m.d.accounts[account] = a
	}
	return nil
}
func (r reconcileRepo) EnqueueLocalTxn(_ context.Context, id uint64, now time.Time) error {
	r.m.mu.Lock()
	defer r.m.mu.Unlock()
	if _, ok := r.m.d.changes[id]; !ok {
		t, ok := r.m.d.txns[id]
		if !ok {
			return store.ErrNotFound
		}
		r.m.d.changes[id] = models.ReconcileLocalChange{Channel: t.Channel, PayAccountID: t.PayAccountID, TxnID: id, NextAt: now, CreatedAt: now}
	}
	return nil
}
func (r reconcileRepo) ClaimLocalTxn(_ context.Context, ch, account, token string, now, until time.Time) (*models.ReconcileLocalChange, error) {
	r.m.mu.Lock()
	defer r.m.mu.Unlock()
	if token == "" {
		return nil, nil
	}
	var best *models.ReconcileLocalChange
	for _, c := range r.m.d.changes {
		t := r.m.d.txns[c.TxnID]
		if c.Done || t.Channel != ch || t.PayAccountID != account || c.NextAt.After(now) || (c.LockedUntil.Valid && c.LockedUntil.Time.After(now)) {
			continue
		}
		if best == nil || c.NextAt.Before(best.NextAt) || (c.NextAt.Equal(best.NextAt) && c.TxnID < best.TxnID) {
			copy := c
			best = &copy
		}
	}
	if best != nil {
		best.LockedBy = token
		best.LockedUntil = sql.NullTime{Time: until, Valid: true}
		r.m.d.changes[best.TxnID] = *best
	}
	return best, nil
}
func (r reconcileRepo) FinishLocalTxn(_ context.Context, id uint64, token string, now, next time.Time, reason string) (bool, error) {
	r.m.mu.Lock()
	defer r.m.mu.Unlock()
	c, ok := r.m.d.changes[id]
	if !ok || token == "" || c.LockedBy != token || !c.LockedUntil.Valid || !c.LockedUntil.Time.After(now) {
		return false, nil
	}
	c.Attempts++
	c.Done = next.IsZero()
	if !next.IsZero() {
		c.NextAt = next
	}
	c.LockedBy = ""
	c.LockedUntil = sql.NullTime{}
	c.LastError = sql.NullString{String: reason, Valid: reason != ""}
	c.UpdatedAt = now
	r.m.d.changes[id] = c
	return true, nil
}
func (r reconcileRepo) resolve(d *models.ReconcileDiff, now time.Time, detail string) {
	d.HandleStatus = models.DiffHandleResolved
	d.HandledBy = models.OpsActorSystem
	d.HandledAt = sql.NullTime{Time: now, Valid: true}
	d.UpdatedAt = now
	r.m.d.seq++
	r.m.d.audits = append(r.m.d.audits, models.OpsAuditLog{ID: r.m.d.seq, Actor: models.OpsActorSystem, Action: models.OpsActionReconcileDiffHandle, TargetType: models.OpsTargetReconcileDiff, TargetID: strconv.FormatUint(d.ID, 10), Detail: detail, CreatedAt: now})
}
func (r reconcileRepo) SetLocalDiff(_ context.Context, id uint64, want *models.ReconcileDiff, now time.Time) (bool, error) {
	r.m.mu.Lock()
	defer r.m.mu.Unlock()
	for i := range r.m.d.diffs {
		d := &r.m.d.diffs[i]
		if d.BatchID != 0 || d.LocalTxnID != id {
			continue
		}
		if d.HandleStatus != models.DiffHandlePending {
			return false, nil
		}
		if want == nil {
			r.resolve(d, now, `{"reason":"published_bill_matched"}`)
			return true, nil
		}
		changed := d.DiffType != want.DiffType || d.Detail != want.Detail || d.GatewayAmount != want.GatewayAmount
		if changed {
			d.DiffType = want.DiffType
			d.Detail = want.Detail
			d.GatewayAmount = want.GatewayAmount
			d.UpdatedAt = now
		}
		return changed, nil
	}
	if want == nil {
		return false, nil
	}
	r.m.d.seq++
	want.ID = r.m.d.seq
	want.LocalTxnID = id
	want.CreatedAt = now
	r.m.d.diffs = append(r.m.d.diffs, *want)
	return true, nil
}
func (r reconcileRepo) ClaimRepair(_ context.Context, ch, account, token string, now, until time.Time) (*models.ReconcileDiff, error) {
	r.m.mu.Lock()
	defer r.m.mu.Unlock()
	if token == "" {
		return nil, nil
	}
	best := -1
	for i, d := range r.m.d.diffs {
		if d.Channel != ch || d.PayAccountID != account || d.BatchID == 0 || d.RunID == "" || d.EntryID == 0 || d.AppID == "" || !r.visible(d) || d.HandleStatus != models.DiffHandlePending || (d.NextVerifyAt.Valid && d.NextVerifyAt.Time.After(now)) || (d.VerifyLockedUntil.Valid && d.VerifyLockedUntil.Time.After(now)) {
			continue
		}
		if best < 0 || d.NextVerifyAt.Time.Before(r.m.d.diffs[best].NextVerifyAt.Time) || (d.NextVerifyAt.Time.Equal(r.m.d.diffs[best].NextVerifyAt.Time) && d.ID < r.m.d.diffs[best].ID) {
			best = i
		}
	}
	if best < 0 {
		return nil, nil
	}
	d := &r.m.d.diffs[best]
	d.VerifyLockedBy = token
	d.VerifyLockedUntil = sql.NullTime{Time: until, Valid: true}
	copy := *d
	return &copy, nil
}
func (r reconcileRepo) FinishRepair(_ context.Context, id uint64, token string, now, next time.Time, success bool, reason string) (bool, error) {
	r.m.mu.Lock()
	defer r.m.mu.Unlock()
	for i := range r.m.d.diffs {
		d := &r.m.d.diffs[i]
		if d.ID != id {
			continue
		}
		if token == "" || d.VerifyLockedBy != token || !d.VerifyLockedUntil.Valid || !d.VerifyLockedUntil.Time.After(now) || d.HandleStatus != models.DiffHandlePending || !r.visible(*d) {
			return false, nil
		}
		d.VerifyAttempts++
		d.NextVerifyAt = sql.NullTime{Time: next, Valid: !success}
		d.VerifyLockedBy = ""
		d.VerifyLockedUntil = sql.NullTime{}
		d.VerifyLastError = sql.NullString{String: reason, Valid: reason != ""}
		d.UpdatedAt = now
		if success {
			r.resolve(d, now, `{"reason":"channel_positive_confirmation"}`)
		}
		return true, nil
	}
	return false, nil
}

func (r reconcileRepo) ResolveMatchedDiffs(_ context.Context, batchID uint64, run string, ids []uint64, now time.Time) error {
	r.m.mu.Lock()
	defer r.m.mu.Unlock()
	for i := range r.m.d.diffs {
		d := &r.m.d.diffs[i]
		if d.BatchID == batchID && d.RunID == run && d.HandleStatus == models.DiffHandlePending && slices.Contains(ids, d.EntryID) {
			r.resolve(d, now, `{"reason":"published_bill_matched"}`)
		}
	}
	return nil
}
