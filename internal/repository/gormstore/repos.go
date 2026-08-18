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

// ---------------------------------------------------------------------
// orders
// ---------------------------------------------------------------------

type orderRepo struct{ base *gorm.DB }

func (r orderRepo) Insert(ctx context.Context, o *models.Order) error {
	return mapDup(dbOf(ctx, r.base).Create(o).Error, "insert order")
}

func (r orderRepo) GetByOrderNo(ctx context.Context, appID, orderNo string, fu bool) (*models.Order, error) {
	var o models.Order
	err := forUpdate(dbOf(ctx, r.base), fu).Where("app_id = ? AND order_no = ?", appID, orderNo).Take(&o).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, store.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("gormstore: get order by no: %w", err)
	}
	return &o, nil
}

func (r orderRepo) GetByToken(ctx context.Context, token string) (*models.Order, error) {
	var o models.Order
	err := dbOf(ctx, r.base).Where("order_token = ?", token).Take(&o).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, store.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("gormstore: get order by token: %w", err)
	}
	return &o, nil
}

func (r orderRepo) FindByIdempotencyKey(ctx context.Context, appID, key string) (*models.Order, error) {
	var o models.Order
	err := dbOf(ctx, r.base).Where("app_id = ? AND idempotency_key = ?", appID, key).Take(&o).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("gormstore: find order by idem: %w", err)
	}
	return &o, nil
}

func (r orderRepo) ListExpiredPending(ctx context.Context, now time.Time, limit int) ([]*models.Order, error) {
	var out []*models.Order
	// status=0(PENDING)且已过期。
	err := dbOf(ctx, r.base).Where("status = ? AND expire_at < ?", models.OrderPending, now).Limit(limit).Find(&out).Error
	if err != nil {
		return nil, fmt.Errorf("gormstore: list expired pending: %w", err)
	}
	return out, nil
}

func (r orderRepo) Update(ctx context.Context, o *models.Order) error {
	// 精确更新资金主链关心的列(与旧 raw UPDATE 列集一致);Select 强制含零值列。
	err := dbOf(ctx, r.base).Model(&models.Order{}).Where("id = ?", o.ID).
		Select("Status", "CloseReason", "RefundedAmount",
			"SettledAttemptID", "SettledChannel", "SettledOutTradeNo", "SettledPayAccountID",
			"SettledMchID", "SettledPayAppID", "SettledConfigVersion",
			"TradeNo", "PaidAt", "AggregateVersion").
		Updates(o).Error
	if err != nil {
		return fmt.Errorf("gormstore: update order: %w", err)
	}
	return nil
}

// ---------------------------------------------------------------------
// payment_attempts
// ---------------------------------------------------------------------

type attemptRepo struct{ base *gorm.DB }

func (r attemptRepo) Insert(ctx context.Context, a *models.PaymentAttempt) error {
	return mapDup(dbOf(ctx, r.base).Create(a).Error, "insert attempt")
}

func (r attemptRepo) GetActiveByOrder(ctx context.Context, orderNo string, fu bool) (*models.PaymentAttempt, error) {
	var a models.PaymentAttempt
	// active_order 为生成列(status∈活跃集时=order_no,否则 NULL),唯一守卫当前活跃尝试。
	err := forUpdate(dbOf(ctx, r.base), fu).Where("active_order = ?", orderNo).Take(&a).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("gormstore: get active attempt: %w", err)
	}
	return &a, nil
}

func (r attemptRepo) GetByOutTradeNo(ctx context.Context, outTradeNo string, fu bool) (*models.PaymentAttempt, error) {
	var a models.PaymentAttempt
	err := forUpdate(dbOf(ctx, r.base), fu).Where("out_trade_no = ?", outTradeNo).Take(&a).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("gormstore: get attempt by out_trade_no: %w", err)
	}
	return &a, nil
}

func (r attemptRepo) ListByOrder(ctx context.Context, orderNo string) ([]*models.PaymentAttempt, error) {
	var out []*models.PaymentAttempt
	err := dbOf(ctx, r.base).Where("order_no = ?", orderNo).Order("attempt_no").Find(&out).Error
	if err != nil {
		return nil, fmt.Errorf("gormstore: list attempts: %w", err)
	}
	return out, nil
}

func (r attemptRepo) CountByOrder(ctx context.Context, orderNo string) (int, error) {
	var n int64
	err := dbOf(ctx, r.base).Model(&models.PaymentAttempt{}).Where("order_no = ?", orderNo).Count(&n).Error
	if err != nil {
		return 0, fmt.Errorf("gormstore: count attempts: %w", err)
	}
	return int(n), nil
}

func (r attemptRepo) ExistsSubmitted(ctx context.Context, orderNo string) (bool, error) {
	var n int64
	// status>=1(REQUESTING)即曾提交网关。
	err := dbOf(ctx, r.base).Model(&models.PaymentAttempt{}).
		Where("order_no = ? AND status >= ?", orderNo, models.AttemptRequesting).Count(&n).Error
	if err != nil {
		return false, fmt.Errorf("gormstore: exists submitted: %w", err)
	}
	return n > 0, nil
}

func (r attemptRepo) ClaimQueryDue(ctx context.Context, workerID string, now, leaseUntil time.Time, limit int) ([]*models.PaymentAttempt, error) {
	var out []*models.PaymentAttempt
	// status∈{UNKNOWN,CLOSING} 且到期且租约空闲;FOR UPDATE SKIP LOCKED 保证多副本不重认领。
	err := skipLocked(dbOf(ctx, r.base)).
		Where("status IN ?", []models.AttemptStatus{models.AttemptUnknown, models.AttemptClosing}).
		Where("next_query_at IS NULL OR next_query_at <= ?", now).
		Where("locked_until IS NULL OR locked_until < ?", now).
		Order("next_query_at").Limit(limit).Find(&out).Error
	if err != nil {
		return nil, fmt.Errorf("gormstore: claim query due attempts: %w", err)
	}
	idlist := make([]uint64, len(out))
	for i, a := range out {
		idlist[i] = a.ID
	}
	if err := claimLease(ctx, r.base, &models.PaymentAttempt{}, idlist, workerID, leaseUntil); err != nil {
		return nil, err
	}
	for _, a := range out {
		a.LockedBy = workerID
		a.LockedUntil = nullTime(leaseUntil)
	}
	return out, nil
}

func (r attemptRepo) ListStuckRequesting(ctx context.Context, before time.Time, limit int) ([]*models.PaymentAttempt, error) {
	var out []*models.PaymentAttempt
	err := dbOf(ctx, r.base).
		Where("status = ? AND updated_at < ?", models.AttemptRequesting, before).Limit(limit).Find(&out).Error
	if err != nil {
		return nil, fmt.Errorf("gormstore: list stuck requesting: %w", err)
	}
	return out, nil
}

func (r attemptRepo) Update(ctx context.Context, a *models.PaymentAttempt) error {
	err := dbOf(ctx, r.base).Model(&models.PaymentAttempt{}).Where("id = ?", a.ID).
		Select("Status", "PrepayID", "NextQueryAt", "QueryCount", "LockedBy", "LockedUntil").
		Updates(a).Error
	// 状态跃迁可能触发 active_order 生成列唯一冲突(活跃尝试唯一守卫)。
	return mapDup(err, "update attempt")
}

// ---------------------------------------------------------------------
// refunds
// ---------------------------------------------------------------------

type refundRepo struct{ base *gorm.DB }

func (r refundRepo) Insert(ctx context.Context, rf *models.Refund) error {
	return mapDup(dbOf(ctx, r.base).Create(rf).Error, "insert refund")
}

func (r refundRepo) GetActiveByOrder(ctx context.Context, orderNo string, fu bool) (*models.Refund, error) {
	var rf models.Refund
	err := forUpdate(dbOf(ctx, r.base), fu).Where("active_guard = ?", orderNo).Take(&rf).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("gormstore: get active refund: %w", err)
	}
	return &rf, nil
}

func (r refundRepo) GetByRefundNo(ctx context.Context, refundNo string) (*models.Refund, error) {
	var rf models.Refund
	err := dbOf(ctx, r.base).Where("refund_no = ?", refundNo).Take(&rf).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, store.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("gormstore: get refund by no: %w", err)
	}
	return &rf, nil
}

func (r refundRepo) FindByOutRefundNo(ctx context.Context, appID, outRefundNo string) (*models.Refund, error) {
	var rf models.Refund
	err := dbOf(ctx, r.base).Where("app_id = ? AND out_refund_no = ?", appID, outRefundNo).Take(&rf).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("gormstore: find refund by out_refund_no: %w", err)
	}
	return &rf, nil
}

func (r refundRepo) ClaimQueryDue(ctx context.Context, workerID string, now, leaseUntil time.Time, limit int) ([]*models.Refund, error) {
	var out []*models.Refund
	// status=PROCESSING 且到期且租约空闲。
	err := skipLocked(dbOf(ctx, r.base)).
		Where("status = ?", models.RefundProcessing).
		Where("next_query_at IS NULL OR next_query_at <= ?", now).
		Where("locked_until IS NULL OR locked_until < ?", now).
		Order("next_query_at").Limit(limit).Find(&out).Error
	if err != nil {
		return nil, fmt.Errorf("gormstore: claim query due refunds: %w", err)
	}
	idlist := make([]uint64, len(out))
	for i, rf := range out {
		idlist[i] = rf.ID
	}
	if err := claimLease(ctx, r.base, &models.Refund{}, idlist, workerID, leaseUntil); err != nil {
		return nil, err
	}
	for _, rf := range out {
		rf.LockedBy = workerID
		rf.LockedUntil = nullTime(leaseUntil)
	}
	return out, nil
}

func (r refundRepo) ListStuckCreated(ctx context.Context, before time.Time, limit int) ([]*models.Refund, error) {
	var out []*models.Refund
	// status=0(CREATED)且创建早于 before(走 idx_status)。
	err := dbOf(ctx, r.base).
		Where("status = ? AND created_at < ?", models.RefundCreated, before).Limit(limit).Find(&out).Error
	if err != nil {
		return nil, fmt.Errorf("gormstore: list stuck created refunds: %w", err)
	}
	return out, nil
}

func (r refundRepo) Update(ctx context.Context, rf *models.Refund) error {
	err := dbOf(ctx, r.base).Model(&models.Refund{}).Where("id = ?", rf.ID).
		Select("Status", "GatewayRefundID", "SucceededAt", "LastError", "NextQueryAt", "QueryCount", "LockedBy", "LockedUntil").
		Updates(rf).Error
	if err != nil {
		return fmt.Errorf("gormstore: update refund: %w", err)
	}
	return nil
}

// ---------------------------------------------------------------------
// payment_transactions(不可变,只插不改)
// ---------------------------------------------------------------------

type txnRepo struct{ base *gorm.DB }

func (r txnRepo) Insert(ctx context.Context, t *models.PaymentTransaction) error {
	return mapDup(dbOf(ctx, r.base).Create(t).Error, "insert txn")
}

func (r txnRepo) GetByTxnID(ctx context.Context, channel, payAccountID, txnID string) (*models.PaymentTransaction, error) {
	var t models.PaymentTransaction
	err := dbOf(ctx, r.base).
		Where("channel = ? AND pay_account_id = ? AND transaction_id = ?", channel, payAccountID, txnID).Take(&t).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("gormstore: get txn by id: %w", err)
	}
	return &t, nil
}

func (r txnRepo) ListByAccountDate(ctx context.Context, channel, payAccountID string, billDate time.Time) ([]*models.PaymentTransaction, error) {
	start := time.Date(billDate.Year(), billDate.Month(), billDate.Day(), 0, 0, 0, 0, billDate.Location())
	end := start.Add(24 * time.Hour)
	var out []*models.PaymentTransaction
	err := dbOf(ctx, r.base).
		Where("channel = ? AND pay_account_id = ? AND paid_at >= ? AND paid_at < ?", channel, payAccountID, start, end).
		Find(&out).Error
	if err != nil {
		return nil, fmt.Errorf("gormstore: list txn by account date: %w", err)
	}
	return out, nil
}

// ---------------------------------------------------------------------
// order_events / event_outbox(事件历史)
// ---------------------------------------------------------------------

type eventRepo struct{ base *gorm.DB }

func (r eventRepo) InsertEvent(ctx context.Context, e *models.OrderEvent) error {
	return mapDup(dbOf(ctx, r.base).Create(e).Error, "insert order_event")
}

func (r eventRepo) InsertOutbox(ctx context.Context, o *models.EventOutbox) error {
	return mapDup(dbOf(ctx, r.base).Create(o).Error, "insert outbox")
}

func (r eventRepo) ListByApp(ctx context.Context, appID string, afterID uint64, limit int) ([]models.OrderEvent, error) {
	var out []models.OrderEvent
	err := dbOf(ctx, r.base).
		Where("app_id = ? AND id > ?", appID, afterID).Order("id").Limit(limit).Find(&out).Error
	if err != nil {
		return nil, fmt.Errorf("gormstore: list events by app: %w", err)
	}
	return out, nil
}

// ---------------------------------------------------------------------
// order_logs
// ---------------------------------------------------------------------

type logRepo struct{ base *gorm.DB }

func (r logRepo) Insert(ctx context.Context, l *models.OrderLog) error {
	if err := dbOf(ctx, r.base).Create(l).Error; err != nil {
		return fmt.Errorf("gormstore: insert order_log: %w", err)
	}
	return nil
}
