// Package store 定义持久化【端口接口】(仓储契约 + 事务边界)。
//
// 分层边界(DESIGN §2):application 依赖本包接口,repository 实现本包接口,runtime 注入实现。
// application 因此不直接 import repository(禁 gorm/gin/repository),满足"application 禁 import repository"。
//
// 事务模型:资金主链的所有多表写(§7 支付成功、§8 取消/建尝试、§10 退款)必须在
// WithinTx 的同一事务内完成;事务内先对订单行 SELECT ... FOR UPDATE(forUpdate=true)锁行,
// 再读改写,即构成 CAS。内存假实现用全局互斥串行化事务以忠实模拟"同行锁 + 事务"语义。
package store

import (
	"context"
	"errors"
	"time"

	"github.com/gtkit/orderd/internal/models"
)

// 端口层哨兵错误。
var (
	// ErrNotFound 记录不存在。
	ErrNotFound = errors.New("store: 记录不存在")
	// ErrDuplicate 命中唯一键(用于幂等键/交易号唯一冲突判定)。
	ErrDuplicate = errors.New("store: 唯一键冲突")
)

// Store 是持久化根:开事务 + 提供非事务只读入口。
type Store interface {
	// WithinTx 在单个数据库事务内执行 fn;fn 返回 error 则回滚,否则提交。
	WithinTx(ctx context.Context, fn func(ctx context.Context, r Repos) error) error
	// Reader 返回非事务只读仓储(GetOrderStatus/GetRefund 等只读路径)。
	Reader() Repos
}

// Repos 聚合各表仓储访问器(事务内与只读共用同一组接口)。
type Repos interface {
	Orders() OrderRepo
	Attempts() AttemptRepo
	Refunds() RefundRepo
	Transactions() TransactionRepo
	Events() EventRepo
	Outbox() OutboxRepo
	Reconcile() ReconcileRepo
	Logs() LogRepo
}

// ReconcileRepo 对账批次与差异仓储(对账 worker 用,DESIGN §14)。
type ReconcileRepo interface {
	// CreateBatch 建批次;命中 uk(channel,pay_account_id,bill_date) 或 batch_no → ErrDuplicate(防重)。
	CreateBatch(ctx context.Context, b *models.ReconcileBatch) error
	// GetBatchByAccountDate 取某账号某日批次;无则 (nil, nil)。
	GetBatchByAccountDate(ctx context.Context, channel, payAccountID string, billDate time.Time) (*models.ReconcileBatch, error)
	UpdateBatch(ctx context.Context, b *models.ReconcileBatch) error
	// InsertDiff 写差异;同批次同交易同类型幂等(命中唯一键返回 ErrDuplicate)。
	InsertDiff(ctx context.Context, d *models.ReconcileDiff) error
}

// OutboxRepo 发件箱投递仓储(投递 worker 用,DESIGN §12)。
type OutboxRepo interface {
	// ClaimDue 认领到期的待投递行(status=Pending 且 next_attempt_at<=now 且租约空闲),
	// 打上 (workerID, leaseUntil) 租约后返回;MySQL 用 FOR UPDATE SKIP LOCKED 保证多副本不重复认领。
	ClaimDue(ctx context.Context, workerID string, now, leaseUntil time.Time, limit int) ([]*models.EventOutbox, error)
	// MarkDelivered 标记已投递(终态)。
	MarkDelivered(ctx context.Context, id uint64) error
	// Reschedule 投递失败重排:attempts 递增、设下次投递时间、清租约、记错误。
	Reschedule(ctx context.Context, id uint64, nextAttemptAt time.Time, attempts int32, lastErr string) error
	// MarkDead 超过最大次数转死信(不自动再投,需人工/定时重放)。
	MarkDead(ctx context.Context, id uint64, lastErr string) error
	// Requeue 死信重放:置回 Pending 并设下次投递时间。
	Requeue(ctx context.Context, id uint64, nextAttemptAt time.Time) error
}

// OrderRepo 订单仓储。forUpdate=true 表示 SELECT ... FOR UPDATE 锁行(仅事务内有意义)。
type OrderRepo interface {
	Insert(ctx context.Context, o *models.Order) error
	GetByOrderNo(ctx context.Context, appID, orderNo string, forUpdate bool) (*models.Order, error)
	GetByToken(ctx context.Context, token string) (*models.Order, error)
	// FindByIdempotencyKey 未命中返回 (nil, nil),便于"存在即复用"分支。
	FindByIdempotencyKey(ctx context.Context, appID, key string) (*models.Order, error)
	// ListExpiredPending 列出已过期的待支付订单(status=PENDING 且 expire_at<now),供超时关单 worker。
	ListExpiredPending(ctx context.Context, now time.Time, limit int) ([]*models.Order, error)
	Update(ctx context.Context, o *models.Order) error
}

// AttemptRepo 支付尝试仓储。
type AttemptRepo interface {
	// Insert 命中 uk_active_order 时返回 ErrDuplicate(活跃尝试唯一守卫,含 CLOSING)。
	Insert(ctx context.Context, a *models.PaymentAttempt) error
	// GetActiveByOrder 返回当前活跃尝试;无则 (nil, nil)。
	GetActiveByOrder(ctx context.Context, orderNo string, forUpdate bool) (*models.PaymentAttempt, error)
	// GetByOutTradeNo 按商户订单号定位尝试(支付回调用);无则 (nil, nil)。
	GetByOutTradeNo(ctx context.Context, outTradeNo string) (*models.PaymentAttempt, error)
	ListByOrder(ctx context.Context, orderNo string) ([]*models.PaymentAttempt, error)
	CountByOrder(ctx context.Context, orderNo string) (int, error)
	// ExistsSubmitted 是否存在曾提交网关(status>=REQUESTING)的尝试(区分 CANCELLED vs CLOSED,评审 #5)。
	ExistsSubmitted(ctx context.Context, orderNo string) (bool, error)
	// ClaimQueryDue 认领待查单尝试(status∈{UNKNOWN,CLOSING} 且 next_query_at 到期/空、租约空闲),
	// 打租约后返回(MySQL 用 FOR UPDATE SKIP LOCKED)。供查单 worker 收敛"结果未知"窗口。
	ClaimQueryDue(ctx context.Context, workerID string, now, leaseUntil time.Time, limit int) ([]*models.PaymentAttempt, error)
	// ListStuckRequesting 列出停留在 REQUESTING 且 updated_at 早于 before 的尝试(卡死 watchdog)。
	ListStuckRequesting(ctx context.Context, before time.Time, limit int) ([]*models.PaymentAttempt, error)
	Update(ctx context.Context, a *models.PaymentAttempt) error
}

// RefundRepo 退款仓储。
type RefundRepo interface {
	// Insert 命中 uk_active_refund 时返回 ErrDuplicate(活跃退款唯一守卫,含 ABNORMAL)。
	Insert(ctx context.Context, r *models.Refund) error
	GetActiveByOrder(ctx context.Context, orderNo string, forUpdate bool) (*models.Refund, error)
	GetByRefundNo(ctx context.Context, refundNo string) (*models.Refund, error)
	// FindByOutRefundNo 未命中返回 (nil, nil)。
	FindByOutRefundNo(ctx context.Context, appID, outRefundNo string) (*models.Refund, error)
	// ClaimQueryDue 认领待查单退款(status=PROCESSING 且 next_query_at 到期/空、租约空闲),打租约后返回。
	ClaimQueryDue(ctx context.Context, workerID string, now, leaseUntil time.Time, limit int) ([]*models.Refund, error)
	Update(ctx context.Context, r *models.Refund) error
}

// TransactionRepo 不可变财务流水仓储(只插不改)。
type TransactionRepo interface {
	// Insert 命中 uk_txn(channel,pay_account_id,transaction_id) 时返回 ErrDuplicate。
	Insert(ctx context.Context, t *models.PaymentTransaction) error
	// GetByTxnID 未命中返回 (nil, nil)。
	GetByTxnID(ctx context.Context, channel, payAccountID, txnID string) (*models.PaymentTransaction, error)
	// ListByAccountDate 列出某账号某日(按 paid_at 归属)的流水,供对账比对。
	ListByAccountDate(ctx context.Context, channel, payAccountID string, billDate time.Time) ([]*models.PaymentTransaction, error)
}

// EventRepo 事件历史 + 发件箱(同事务双写,DESIGN §11)。
type EventRepo interface {
	InsertEvent(ctx context.Context, e *models.OrderEvent) error
	InsertOutbox(ctx context.Context, o *models.EventOutbox) error
	// ListByApp 按接入方游标(自增 id > afterID)升序拉取 ≤limit 条,供 ListOrderEvents 补投。
	ListByApp(ctx context.Context, appID string, afterID uint64, limit int) ([]models.OrderEvent, error)
}

// LogRepo 订单状态流转日志。
type LogRepo interface {
	Insert(ctx context.Context, l *models.OrderLog) error
}
