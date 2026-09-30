package memstore

import (
	"cmp"
	"context"
	"database/sql"
	"slices"
	"strconv"
	"time"

	"github.com/gtkit/orderd/internal/models"
	"github.com/gtkit/orderd/internal/store"
)

// ---- orders ----

type orderRepo struct{ m *Mem }

func (r orderRepo) Insert(_ context.Context, o *models.Order) error {
	r.m.mu.Lock()
	defer r.m.mu.Unlock()
	d := r.m.d
	for _, e := range d.orders {
		if e.OrderNo == o.OrderNo || e.OrderToken == o.OrderToken {
			return store.ErrDuplicate
		}
		if o.IdempotencyKey.Valid && e.AppID == o.AppID &&
			e.IdempotencyKey.Valid && e.IdempotencyKey.String == o.IdempotencyKey.String {
			return store.ErrDuplicate
		}
		if o.ClientOrderNo.Valid && e.AppID == o.AppID &&
			e.ClientOrderNo.Valid && e.ClientOrderNo.String == o.ClientOrderNo.String {
			return store.ErrDuplicate
		}
	}
	d.seq++
	o.ID = d.seq
	if o.CreatedAt.IsZero() {
		o.CreatedAt = time.Now() // 对齐 DDL DEFAULT CURRENT_TIMESTAMP
	}
	if o.UpdatedAt.IsZero() {
		o.UpdatedAt = o.CreatedAt
	}
	d.orders[o.ID] = *o
	return nil
}

func (r orderRepo) GetByOrderNo(_ context.Context, appID, orderNo string, _ bool) (*models.Order, error) {
	r.m.mu.Lock()
	defer r.m.mu.Unlock()
	for _, e := range r.m.d.orders {
		if e.OrderNo == orderNo && e.AppID == appID {
			cp := e
			return &cp, nil
		}
	}
	return nil, store.ErrNotFound
}

func (r orderRepo) GetByToken(_ context.Context, token string) (*models.Order, error) {
	r.m.mu.Lock()
	defer r.m.mu.Unlock()
	for _, e := range r.m.d.orders {
		if e.OrderToken == token {
			cp := e
			return &cp, nil
		}
	}
	return nil, store.ErrNotFound
}

func (r orderRepo) FindByIdempotencyKey(_ context.Context, appID, key string) (*models.Order, error) {
	r.m.mu.Lock()
	defer r.m.mu.Unlock()
	for _, e := range r.m.d.orders {
		if e.AppID == appID && e.IdempotencyKey.Valid && e.IdempotencyKey.String == key {
			cp := e
			return &cp, nil
		}
	}
	return nil, nil
}

func (r orderRepo) ListExpiredPending(_ context.Context, now time.Time, limit int) ([]*models.Order, error) {
	r.m.mu.Lock()
	defer r.m.mu.Unlock()
	var out []*models.Order
	for _, e := range r.m.d.orders {
		if e.Status == models.OrderPending && e.ExpireAt.Before(now) {
			cp := e
			out = append(out, &cp)
			if len(out) >= limit {
				break
			}
		}
	}
	return out, nil
}

func (r orderRepo) ListStalledClosing(_ context.Context, before time.Time, limit int) ([]*models.Order, error) {
	r.m.mu.Lock()
	defer r.m.mu.Unlock()
	var out []*models.Order
	for _, e := range r.m.d.orders {
		if e.Status == models.OrderClosing && e.UpdatedAt.Before(before) {
			cp := e
			out = append(out, &cp)
			if len(out) >= limit {
				break
			}
		}
	}
	return out, nil
}

func (r orderRepo) CountPendingByUser(_ context.Context, appID string, userID int64) (int, error) {
	r.m.mu.Lock()
	defer r.m.mu.Unlock()
	n := 0
	for _, e := range r.m.d.orders {
		if e.AppID == appID && e.UserID == userID && e.Status == models.OrderPending {
			n++
		}
	}
	return n, nil
}

func (r orderRepo) FindByClientOrderNo(_ context.Context, appID, clientOrderNo string) (*models.Order, error) {
	r.m.mu.Lock()
	defer r.m.mu.Unlock()
	for _, e := range r.m.d.orders {
		if e.AppID == appID && e.ClientOrderNo.Valid && e.ClientOrderNo.String == clientOrderNo {
			cp := e
			return &cp, nil
		}
	}
	return nil, nil
}

func (r orderRepo) Update(_ context.Context, o *models.Order) error {
	r.m.mu.Lock()
	defer r.m.mu.Unlock()
	if _, ok := r.m.d.orders[o.ID]; !ok {
		return store.ErrNotFound
	}
	o.UpdatedAt = time.Now() // 对齐 DDL ON UPDATE CURRENT_TIMESTAMP
	r.m.d.orders[o.ID] = *o
	return nil
}

// ---- payment_attempts ----

type attemptRepo struct{ m *Mem }

func (r attemptRepo) Insert(_ context.Context, a *models.PaymentAttempt) error {
	r.m.mu.Lock()
	defer r.m.mu.Unlock()
	d := r.m.d
	for _, e := range d.attempts {
		if e.OutTradeNo == a.OutTradeNo {
			return store.ErrDuplicate
		}
		if e.OrderNo == a.OrderNo && e.AttemptNo == a.AttemptNo {
			return store.ErrDuplicate
		}
		// uk_active_order:同一订单最多一个活跃尝试(活跃含 CLOSING)。
		if a.Status.Active() && e.OrderNo == a.OrderNo && e.Status.Active() {
			return store.ErrDuplicate
		}
	}
	d.seq++
	a.ID = d.seq
	d.attempts[a.ID] = *a
	return nil
}

func (r attemptRepo) GetActiveByOrder(_ context.Context, orderNo string, _ bool) (*models.PaymentAttempt, error) {
	r.m.mu.Lock()
	defer r.m.mu.Unlock()
	for _, e := range r.m.d.attempts {
		if e.OrderNo == orderNo && e.Status.Active() {
			cp := e
			return &cp, nil
		}
	}
	return nil, nil
}

func (r attemptRepo) GetByOutTradeNo(_ context.Context, outTradeNo string, _ bool) (*models.PaymentAttempt, error) {
	r.m.mu.Lock()
	defer r.m.mu.Unlock()
	for _, e := range r.m.d.attempts {
		if e.OutTradeNo == outTradeNo {
			cp := e
			return &cp, nil
		}
	}
	return nil, nil
}

func (r attemptRepo) ListByOrder(_ context.Context, orderNo string) ([]*models.PaymentAttempt, error) {
	r.m.mu.Lock()
	defer r.m.mu.Unlock()
	var out []*models.PaymentAttempt
	for _, e := range r.m.d.attempts {
		if e.OrderNo == orderNo {
			cp := e
			out = append(out, &cp)
		}
	}
	return out, nil
}

func (r attemptRepo) CountByOrder(_ context.Context, orderNo string) (int, error) {
	r.m.mu.Lock()
	defer r.m.mu.Unlock()
	n := 0
	for _, e := range r.m.d.attempts {
		if e.OrderNo == orderNo {
			n++
		}
	}
	return n, nil
}

func (r attemptRepo) ExistsSubmitted(_ context.Context, orderNo string) (bool, error) {
	r.m.mu.Lock()
	defer r.m.mu.Unlock()
	for _, e := range r.m.d.attempts {
		if e.OrderNo == orderNo && e.Status.SubmittedToGateway() {
			return true, nil
		}
	}
	return false, nil
}

func (r attemptRepo) ClaimQueryDue(_ context.Context, workerID string, now, leaseUntil time.Time, limit int) ([]*models.PaymentAttempt, error) {
	r.m.mu.Lock()
	defer r.m.mu.Unlock()
	var out []*models.PaymentAttempt
	for id, e := range r.m.d.attempts {
		if e.Status != models.AttemptUnknown && e.Status != models.AttemptClosing {
			continue
		}
		if e.NextQueryAt.Valid && e.NextQueryAt.Time.After(now) {
			continue
		}
		if e.LockedUntil.Valid && e.LockedUntil.Time.After(now) {
			continue // 租约未过期
		}
		e.LockedBy = workerID
		e.LockedUntil = sql.NullTime{Time: leaseUntil, Valid: true}
		r.m.d.attempts[id] = e
		cp := e
		out = append(out, &cp)
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

func (r attemptRepo) ListStuckRequesting(_ context.Context, before time.Time, limit int) ([]*models.PaymentAttempt, error) {
	r.m.mu.Lock()
	defer r.m.mu.Unlock()
	var out []*models.PaymentAttempt
	for _, e := range r.m.d.attempts {
		if e.Status == models.AttemptRequesting && e.UpdatedAt.Before(before) {
			cp := e
			out = append(out, &cp)
			if len(out) >= limit {
				break
			}
		}
	}
	return out, nil
}

func (r attemptRepo) Update(_ context.Context, a *models.PaymentAttempt) error {
	r.m.mu.Lock()
	defer r.m.mu.Unlock()
	if _, ok := r.m.d.attempts[a.ID]; !ok {
		return store.ErrNotFound
	}
	// 复核 uk_active_order:置为活跃时,不得与其它活跃尝试冲突。
	if a.Status.Active() {
		for id, e := range r.m.d.attempts {
			if id != a.ID && e.OrderNo == a.OrderNo && e.Status.Active() {
				return store.ErrDuplicate
			}
		}
	}
	r.m.d.attempts[a.ID] = *a
	return nil
}

// ---- refunds ----

type refundRepo struct{ m *Mem }

func (r refundRepo) Insert(_ context.Context, rf *models.Refund) error {
	r.m.mu.Lock()
	defer r.m.mu.Unlock()
	d := r.m.d
	for _, e := range d.refunds {
		if e.RefundNo == rf.RefundNo {
			return store.ErrDuplicate
		}
		if e.AppID == rf.AppID && e.OutRefundNo == rf.OutRefundNo {
			return store.ErrDuplicate
		}
		if rf.Status.Active() && e.Status.Active() && refundGuard(e) == refundGuard(*rf) {
			return store.ErrDuplicate
		}
	}
	d.seq++
	rf.ID = d.seq
	if rf.CreatedAt.IsZero() {
		rf.CreatedAt = time.Now() // 对齐 DDL DEFAULT CURRENT_TIMESTAMP(ListStuckCreated 依赖)
	}
	d.refunds[rf.ID] = *rf
	return nil
}

// refundGuard 对齐 DDL active_guard:订单级退款取 order_no,按异常流水退款取 'txn:<id>'。
func refundGuard(rf models.Refund) string {
	if rf.SourceTxnID.Valid {
		return "txn:" + strconv.FormatInt(rf.SourceTxnID.Int64, 10)
	}
	return rf.OrderNo
}

func (r refundRepo) GetActiveByOrder(_ context.Context, orderNo string, _ bool) (*models.Refund, error) {
	r.m.mu.Lock()
	defer r.m.mu.Unlock()
	for _, e := range r.m.d.refunds {
		if refundGuard(e) == orderNo && e.Status.Active() {
			cp := e
			return &cp, nil
		}
	}
	return nil, nil
}

func (r refundRepo) GetByRefundNo(_ context.Context, refundNo string, _ bool) (*models.Refund, error) {
	r.m.mu.Lock()
	defer r.m.mu.Unlock()
	for _, e := range r.m.d.refunds {
		if e.RefundNo == refundNo {
			cp := e
			return &cp, nil
		}
	}
	return nil, store.ErrNotFound
}

func (r refundRepo) FindByOutRefundNo(_ context.Context, appID, outRefundNo string) (*models.Refund, error) {
	r.m.mu.Lock()
	defer r.m.mu.Unlock()
	for _, e := range r.m.d.refunds {
		if e.AppID == appID && e.OutRefundNo == outRefundNo {
			cp := e
			return &cp, nil
		}
	}
	return nil, nil
}

func (r refundRepo) ClaimQueryDue(_ context.Context, workerID string, now, leaseUntil time.Time, limit int) ([]*models.Refund, error) {
	r.m.mu.Lock()
	defer r.m.mu.Unlock()
	var out []*models.Refund
	for id, e := range r.m.d.refunds {
		if e.Status != models.RefundProcessing {
			continue
		}
		if e.NextQueryAt.Valid && e.NextQueryAt.Time.After(now) {
			continue
		}
		if e.LockedUntil.Valid && e.LockedUntil.Time.After(now) {
			continue
		}
		e.LockedBy = workerID
		e.LockedUntil = sql.NullTime{Time: leaseUntil, Valid: true}
		r.m.d.refunds[id] = e
		cp := e
		out = append(out, &cp)
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

func (r refundRepo) ListStuckCreated(_ context.Context, before time.Time, limit int) ([]*models.Refund, error) {
	r.m.mu.Lock()
	defer r.m.mu.Unlock()
	var out []*models.Refund
	for _, e := range r.m.d.refunds {
		if e.Status != models.RefundCreated || !e.CreatedAt.Before(before) {
			continue
		}
		cp := e
		out = append(out, &cp)
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

func (r refundRepo) ListBySourceTxn(_ context.Context, txnID uint64) ([]*models.Refund, error) {
	r.m.mu.Lock()
	defer r.m.mu.Unlock()
	var out []*models.Refund
	for _, e := range r.m.d.refunds {
		if e.SourceTxnID.Valid && uint64(e.SourceTxnID.Int64) == txnID {
			cp := e
			out = append(out, &cp)
		}
	}
	return out, nil
}

func (r refundRepo) Update(_ context.Context, rf *models.Refund) error {
	r.m.mu.Lock()
	defer r.m.mu.Unlock()
	if _, ok := r.m.d.refunds[rf.ID]; !ok {
		return store.ErrNotFound
	}
	r.m.d.refunds[rf.ID] = *rf
	return nil
}

// ---- payment_transactions ----

type txnRepo struct{ m *Mem }

func (r txnRepo) Insert(_ context.Context, t *models.PaymentTransaction) error {
	r.m.mu.Lock()
	defer r.m.mu.Unlock()
	d := r.m.d
	for _, e := range d.txns {
		if e.Channel == t.Channel && e.PayAccountID == t.PayAccountID && e.TransactionID == t.TransactionID {
			return store.ErrDuplicate
		}
	}
	d.seq++
	t.ID = d.seq
	d.txns[t.ID] = *t
	return nil
}

func (r txnRepo) ListAnomalies(_ context.Context, f store.AnomalyFilter, afterID uint64, limit int) ([]models.PaymentTransaction, error) {
	r.m.mu.Lock()
	defer r.m.mu.Unlock()
	var all []models.PaymentTransaction
	for _, t := range r.m.d.txns {
		if !t.Anomaly || t.ID <= afterID {
			continue
		}
		if f.AppID != "" && t.AppID != f.AppID {
			continue
		}
		if f.PayAccountID != "" && t.PayAccountID != f.PayAccountID {
			continue
		}
		if !f.PaidFrom.IsZero() && (!t.PaidAt.Valid || t.PaidAt.Time.Before(f.PaidFrom)) {
			continue
		}
		if !f.PaidTo.IsZero() && (!t.PaidAt.Valid || t.PaidAt.Time.After(f.PaidTo)) {
			continue
		}
		all = append(all, t)
	}
	slices.SortFunc(all, func(a, b models.PaymentTransaction) int { return cmp.Compare(a.ID, b.ID) })
	if len(all) > limit {
		all = all[:limit]
	}
	return all, nil
}

func (r txnRepo) ListByAccountDate(_ context.Context, channel, payAccountID string, billDate time.Time) ([]*models.PaymentTransaction, error) {
	r.m.mu.Lock()
	defer r.m.mu.Unlock()
	y, mo, d := billDate.Date()
	var out []*models.PaymentTransaction
	for _, e := range r.m.d.txns {
		if e.Channel != channel || e.PayAccountID != payAccountID || !e.PaidAt.Valid {
			continue
		}
		ey, emo, ed := e.PaidAt.Time.Date()
		if ey == y && emo == mo && ed == d {
			cp := e
			out = append(out, &cp)
		}
	}
	return out, nil
}

func (r txnRepo) GetByID(_ context.Context, id uint64, _ bool) (*models.PaymentTransaction, error) {
	r.m.mu.Lock()
	defer r.m.mu.Unlock()
	t, ok := r.m.d.txns[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	return &t, nil
}

func (r txnRepo) GetByTxnID(_ context.Context, channel, payAccountID, txnID string) (*models.PaymentTransaction, error) {
	r.m.mu.Lock()
	defer r.m.mu.Unlock()
	for _, e := range r.m.d.txns {
		if e.Channel == channel && e.PayAccountID == payAccountID && e.TransactionID == txnID {
			cp := e
			return &cp, nil
		}
	}
	return nil, nil
}

// ---- events / outbox / logs ----

type eventRepo struct{ m *Mem }

func (r eventRepo) InsertEvent(_ context.Context, e *models.OrderEvent) error {
	r.m.mu.Lock()
	defer r.m.mu.Unlock()
	d := r.m.d
	// uk_event_id + uk_order_version(app_id, order_no, aggregate_version)。
	for _, x := range d.events {
		if x.EventID == e.EventID {
			return store.ErrDuplicate
		}
		if x.AppID == e.AppID && x.OrderNo == e.OrderNo && x.AggregateVersion == e.AggregateVersion {
			return store.ErrDuplicate
		}
	}
	d.seq++
	e.ID = d.seq
	d.events = append(d.events, *e)
	return nil
}

func (r eventRepo) ListByApp(_ context.Context, appID string, afterID uint64, limit int) ([]models.OrderEvent, error) {
	r.m.mu.Lock()
	defer r.m.mu.Unlock()
	// events 已按插入序(id 升序)存放;线性筛选 app 与游标。
	out := make([]models.OrderEvent, 0, limit)
	for _, e := range r.m.d.events {
		if e.AppID == appID && e.ID > afterID {
			out = append(out, e)
			if len(out) >= limit {
				break
			}
		}
	}
	return out, nil
}

func (r eventRepo) InsertOutbox(_ context.Context, o *models.EventOutbox) error {
	r.m.mu.Lock()
	defer r.m.mu.Unlock()
	d := r.m.d
	for _, x := range d.outbox {
		if x.EventID == o.EventID {
			return store.ErrDuplicate
		}
	}
	d.seq++
	o.ID = d.seq
	if o.CreatedAt.IsZero() {
		o.CreatedAt = time.Now() // 对齐 DDL DEFAULT CURRENT_TIMESTAMP(死信兜底条件依赖)
	}
	if o.UpdatedAt.IsZero() {
		o.UpdatedAt = o.CreatedAt
	}
	d.outbox = append(d.outbox, *o)
	return nil
}

// ---- event_outbox(投递 worker)----

type outboxRepo struct{ m *Mem }

func leaseFree(o models.EventOutbox, now time.Time) bool {
	return !o.LockedUntil.Valid || o.LockedUntil.Time.Before(now)
}

func (r outboxRepo) ClaimDue(_ context.Context, workerID string, now, leaseUntil time.Time, limit int) ([]*models.EventOutbox, error) {
	r.m.mu.Lock()
	defer r.m.mu.Unlock()
	var out []*models.EventOutbox
	for i := range r.m.d.outbox {
		o := &r.m.d.outbox[i]
		if o.Status != models.OutboxPending || o.NextAttemptAt.After(now) || !leaseFree(*o, now) {
			continue
		}
		o.LockedBy = workerID
		o.LockedUntil = sql.NullTime{Time: leaseUntil, Valid: true}
		cp := *o
		out = append(out, &cp)
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

func (r outboxRepo) mutate(id uint64, f func(*models.EventOutbox)) error {
	r.m.mu.Lock()
	defer r.m.mu.Unlock()
	for i := range r.m.d.outbox {
		if r.m.d.outbox[i].ID == id {
			f(&r.m.d.outbox[i])
			r.m.d.outbox[i].UpdatedAt = time.Now() // 对齐 DDL ON UPDATE CURRENT_TIMESTAMP
			return nil
		}
	}
	return store.ErrNotFound
}

func (r outboxRepo) MarkDelivered(_ context.Context, id uint64) error {
	return r.mutate(id, func(o *models.EventOutbox) {
		o.Status = models.OutboxDelivered
		o.LockedBy = ""
		o.LockedUntil = sql.NullTime{}
	})
}

// Reschedule / MarkDead 仅对 Pending 生效(与 gormstore 的 status=Pending 守卫一致):
// 不覆盖他副本已 MarkDelivered 的行。
func (r outboxRepo) Reschedule(_ context.Context, id uint64, nextAttemptAt time.Time, attempts int32, lastErr string) error {
	return r.mutate(id, func(o *models.EventOutbox) {
		if o.Status != models.OutboxPending {
			return
		}
		o.Attempts = attempts
		o.NextAttemptAt = nextAttemptAt
		o.LastError = sql.NullString{String: lastErr, Valid: lastErr != ""}
		o.LockedBy = ""
		o.LockedUntil = sql.NullTime{}
	})
}

func (r outboxRepo) MarkDead(_ context.Context, id uint64, attempts int32, lastErr string) error {
	return r.mutate(id, func(o *models.EventOutbox) {
		if o.Status != models.OutboxPending {
			return
		}
		o.Status = models.OutboxDead
		o.Attempts = attempts
		o.LastError = sql.NullString{String: lastErr, Valid: lastErr != ""}
		o.LockedBy = ""
		o.LockedUntil = sql.NullTime{}
	})
}

func (r outboxRepo) Requeue(_ context.Context, id uint64, nextAttemptAt time.Time) error {
	return r.mutate(id, func(o *models.EventOutbox) {
		o.Status = models.OutboxPending
		o.NextAttemptAt = nextAttemptAt
		o.LastError = sql.NullString{}
		o.LockedBy = ""
		o.LockedUntil = sql.NullTime{}
	})
}

func (r outboxRepo) ListDead(_ context.Context, f store.DeadFilter, afterID uint64, limit int) ([]models.EventOutbox, error) {
	r.m.mu.Lock()
	defer r.m.mu.Unlock()
	out := make([]models.EventOutbox, 0, limit)
	for _, o := range r.m.d.outbox {
		if o.Status != models.OutboxDead || o.ID <= afterID {
			continue
		}
		if f.AppID != "" && o.AppID != f.AppID {
			continue
		}
		if f.EventType != "" && o.EventType != f.EventType {
			continue
		}
		if f.OrderNo != "" && o.OrderNo != f.OrderNo {
			continue
		}
		out = append(out, o)
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

func (r outboxRepo) GetByEventID(_ context.Context, eventID string, _ bool) (*models.EventOutbox, error) {
	r.m.mu.Lock()
	defer r.m.mu.Unlock()
	for _, o := range r.m.d.outbox {
		if o.EventID == eventID {
			cp := o
			return &cp, nil
		}
	}
	return nil, store.ErrNotFound
}

func (r outboxRepo) ClaimDeadForRequeue(_ context.Context, cond store.DeadRequeueCond, limit int) ([]*models.EventOutbox, error) {
	r.m.mu.Lock()
	defer r.m.mu.Unlock()
	var out []*models.EventOutbox
	for _, o := range r.m.d.outbox {
		if o.Status != models.OutboxDead ||
			o.UpdatedAt.After(cond.UpdatedBefore) ||
			o.Attempts >= cond.AttemptsBelow ||
			o.CreatedAt.Before(cond.CreatedAfter) {
			continue
		}
		cp := o
		out = append(out, &cp)
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

// ---- reconcile ----

type reconcileRepo struct{ m *Mem }

func (r reconcileRepo) CreateBatch(_ context.Context, b *models.ReconcileBatch) error {
	r.m.mu.Lock()
	defer r.m.mu.Unlock()
	for _, e := range r.m.d.batches {
		if e.BatchNo == b.BatchNo {
			return store.ErrDuplicate
		}
		if e.Channel == b.Channel && e.PayAccountID == b.PayAccountID && sameDay(e.BillDate, b.BillDate) {
			return store.ErrDuplicate
		}
	}
	r.m.d.seq++
	b.ID = r.m.d.seq
	r.m.d.batches[b.ID] = *b
	return nil
}

func (r reconcileRepo) GetBatchByAccountDate(_ context.Context, channel, payAccountID string, billDate time.Time) (*models.ReconcileBatch, error) {
	r.m.mu.Lock()
	defer r.m.mu.Unlock()
	for _, e := range r.m.d.batches {
		if e.Channel == channel && e.PayAccountID == payAccountID && sameDay(e.BillDate, billDate) {
			cp := e
			return &cp, nil
		}
	}
	return nil, nil
}

func (r reconcileRepo) UpdateBatch(_ context.Context, b *models.ReconcileBatch) error {
	r.m.mu.Lock()
	defer r.m.mu.Unlock()
	if _, ok := r.m.d.batches[b.ID]; !ok {
		return store.ErrNotFound
	}
	r.m.d.batches[b.ID] = *b
	return nil
}

func (r reconcileRepo) InsertDiff(_ context.Context, d *models.ReconcileDiff) error {
	r.m.mu.Lock()
	defer r.m.mu.Unlock()
	for _, e := range r.m.d.diffs {
		if e.BatchID == d.BatchID && e.Channel == d.Channel && e.TransactionID == d.TransactionID && e.DiffType == d.DiffType {
			return store.ErrDuplicate
		}
	}
	r.m.d.seq++
	d.ID = r.m.d.seq
	r.m.d.diffs = append(r.m.d.diffs, *d)
	return nil
}

func (r reconcileRepo) ListDiffs(_ context.Context, f store.DiffFilter, afterID uint64, limit int) ([]models.ReconcileDiff, error) {
	r.m.mu.Lock()
	defer r.m.mu.Unlock()
	out := make([]models.ReconcileDiff, 0, limit)
	for _, e := range r.m.d.diffs {
		if e.ID <= afterID {
			continue
		}
		if f.HandleStatus != nil && e.HandleStatus != *f.HandleStatus {
			continue
		}
		if f.DiffType != nil && e.DiffType != *f.DiffType {
			continue
		}
		if f.AppID != "" && e.AppID != f.AppID {
			continue
		}
		if f.Channel != "" && e.Channel != f.Channel {
			continue
		}
		if f.PayAccountID != "" && e.PayAccountID != f.PayAccountID {
			continue
		}
		if !f.BillDateFrom.IsZero() || !f.BillDateTo.IsZero() {
			b, ok := r.m.d.batches[e.BatchID]
			if !ok {
				continue
			}
			if !f.BillDateFrom.IsZero() && b.BillDate.Before(f.BillDateFrom) {
				continue
			}
			if !f.BillDateTo.IsZero() && b.BillDate.After(f.BillDateTo) {
				continue
			}
		}
		out = append(out, e)
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

func (r reconcileRepo) GetDiff(_ context.Context, id uint64, _ bool) (*models.ReconcileDiff, error) {
	r.m.mu.Lock()
	defer r.m.mu.Unlock()
	for _, e := range r.m.d.diffs {
		if e.ID == id {
			cp := e
			return &cp, nil
		}
	}
	return nil, store.ErrNotFound
}

func (r reconcileRepo) UpdateDiffHandling(_ context.Context, id uint64, status models.DiffHandleStatus, handledBy string, handledAt time.Time, remark string) error {
	r.m.mu.Lock()
	defer r.m.mu.Unlock()
	for i := range r.m.d.diffs {
		if r.m.d.diffs[i].ID != id {
			continue
		}
		d := &r.m.d.diffs[i]
		d.HandleStatus = status
		d.HandledBy = handledBy
		d.HandledAt = sql.NullTime{Time: handledAt, Valid: true}
		if remark != "" { // 空备注保留原值(与 gormstore 语义一致)
			d.HandleRemark = remark
		}
		d.UpdatedAt = time.Now()
		return nil
	}
	return store.ErrNotFound
}

func sameDay(a, b time.Time) bool {
	ay, am, ad := a.Date()
	by, bm, bd := b.Date()
	return ay == by && am == bm && ad == bd
}

// Diffs 返回对账差异副本(测试用)。
func (m *Mem) Diffs() []models.ReconcileDiff {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]models.ReconcileDiff(nil), m.d.diffs...)
}

type logRepo struct{ m *Mem }

func (r logRepo) Insert(_ context.Context, l *models.OrderLog) error {
	r.m.mu.Lock()
	defer r.m.mu.Unlock()
	r.m.d.seq++
	l.ID = r.m.d.seq
	r.m.d.logs = append(r.m.d.logs, *l)
	return nil
}

// ---- 测试辅助:只读快照 ----

// Events 返回事件历史副本(按插入序)。
func (m *Mem) Events() []models.OrderEvent {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]models.OrderEvent(nil), m.d.events...)
}

// Outbox 返回发件箱副本。
func (m *Mem) Outbox() []models.EventOutbox {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]models.EventOutbox(nil), m.d.outbox...)
}

// Txns 返回财务流水副本。
func (m *Mem) Txns() []models.PaymentTransaction {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]models.PaymentTransaction, 0, len(m.d.txns))
	for _, v := range m.d.txns {
		out = append(out, v)
	}
	return out
}

// Attempts 返回某订单的支付尝试副本。
func (m *Mem) Attempts(orderNo string) []models.PaymentAttempt {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []models.PaymentAttempt
	for _, v := range m.d.attempts {
		if v.OrderNo == orderNo {
			out = append(out, v)
		}
	}
	return out
}

// ---- ops_audit_logs(只插不改)----

type opsAuditRepo struct{ m *Mem }

func (r opsAuditRepo) Insert(_ context.Context, l *models.OpsAuditLog) error {
	r.m.mu.Lock()
	defer r.m.mu.Unlock()
	r.m.d.seq++
	l.ID = r.m.d.seq
	if l.CreatedAt.IsZero() {
		l.CreatedAt = time.Now()
	}
	r.m.d.audits = append(r.m.d.audits, *l)
	return nil
}

func (r opsAuditRepo) ListByTarget(_ context.Context, targetType, targetID string, afterID uint64, limit int) ([]models.OpsAuditLog, error) {
	r.m.mu.Lock()
	defer r.m.mu.Unlock()
	out := make([]models.OpsAuditLog, 0, limit)
	for _, l := range r.m.d.audits {
		if l.TargetType != targetType || l.TargetID != targetID || l.ID <= afterID {
			continue
		}
		out = append(out, l)
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

// Audits 返回审计记录副本(测试用)。
func (m *Mem) Audits() []models.OpsAuditLog {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]models.OpsAuditLog(nil), m.d.audits...)
}
