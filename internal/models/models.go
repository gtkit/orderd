package models

import (
	"database/sql"
	"time"
)

// 持久化模型:字段对齐 docs/sql/schema.sql 的表结构。领域/应用层只依赖这些结构,
// 不直接依赖 gorm/sql 具体实现(仓储在 repository/ 落地)。

// Order 订单主表(orders)。
type Order struct {
	ID             uint64
	AppID          string
	IdempotencyKey sql.NullString
	RequestHash    sql.NullString
	ClientOrderNo  sql.NullString
	OrderNo        string
	OrderToken     string
	UserID         int64
	Status         OrderStatus
	CloseReason    CloseReason

	ProductID       uint64
	ProductTitle    string
	BusinessType    string
	MetadataType    string
	MetadataVersion int32
	Metadata        []byte

	Currency       string
	OriginalPrice  int64
	DiscountAmount int64
	PayAmount      int64
	RefundedAmount int64

	// 下单账号快照(冻结)。
	PayMethod     PayMethod
	PayChannel    string
	PayAccountID  string
	PayMchID      string
	PayAppID      string
	ConfigVersion int32

	// 成功账号快照(支付成功事务写入;退款以此为权威,评审 #6 / P1)。
	SettledAttemptID     sql.NullInt64
	SettledChannel       string
	SettledOutTradeNo    string
	SettledPayAccountID  string
	SettledMchID         string
	SettledPayAppID      string
	SettledConfigVersion int32

	TradeNo          string
	PaidAt           sql.NullTime
	ChannelID        int64
	AggregateVersion int64
	ExpireAt         time.Time
	ClientIPStr      string

	CreatedAt time.Time
	UpdatedAt time.Time
}

// Money 返回订单的金额快照。
func (o *Order) Money() Money {
	return Money{Original: o.OriginalPrice, Discount: o.DiscountAmount, Payable: o.PayAmount, Currency: o.Currency}
}

// RefundAccountID 返回退款应使用的账号:优先成功账号快照,回退下单账号(评审 #6)。
func (o *Order) RefundAccountID() string {
	if o.SettledPayAccountID != "" {
		return o.SettledPayAccountID
	}
	return o.PayAccountID
}

// PaymentAttempt 支付尝试(payment_attempts)。
type PaymentAttempt struct {
	ID            uint64
	AppID         string
	OrderNo       string
	AttemptNo     int32
	Channel       string
	PayAccountID  string
	ConfigVersion int32
	OutTradeNo    string
	RequestHash   string
	Status        AttemptStatus
	PrepayID      string
	ExpireAt      time.Time

	NextQueryAt sql.NullTime
	QueryCount  int32
	LockedBy    string
	LockedUntil sql.NullTime

	CreatedAt time.Time
	UpdatedAt time.Time
}

// Refund 退款执行流水(refunds)。
type Refund struct {
	ID              uint64
	AppID           string
	RefundNo        string
	OutRefundNo     string
	RequestHash     string
	OrderNo         string
	PayAccountID    string
	RefundAmount    int64
	Status          RefundStatus
	GatewayRefundID string
	Reason          string
	SucceededAt     sql.NullTime
	LastError       sql.NullString

	NextQueryAt sql.NullTime
	QueryCount  int32
	LockedBy    string
	LockedUntil sql.NullTime

	CreatedAt time.Time
	UpdatedAt time.Time
}

// TxnKind 财务流水类型。
type TxnKind int8

const (
	TxnPay    TxnKind = 1
	TxnRefund TxnKind = 2
)

// PaymentTransaction 不可变财务流水(payment_transactions)。
type PaymentTransaction struct {
	ID               uint64
	AppID            string
	OrderNo          string
	PaymentAttemptID sql.NullInt64
	RefundNo         string
	PayAccountID     string
	Channel          string
	Kind             TxnKind
	OutTradeNo       string
	TransactionID    string
	Amount           int64
	Currency         string
	Anomaly          bool
	PaidAt           sql.NullTime
	NotifyDigest     string
	CreatedAt        time.Time
}

// OrderLog 订单状态流转日志(order_logs)。
type OrderLog struct {
	ID         uint64
	OrderID    uint64
	OrderNo    string
	FromStatus OrderStatus
	ToStatus   OrderStatus
	Operator   string
	Remark     string
	CreatedAt  time.Time
}

// OrderEvent 不可变事件历史(order_events)。
type OrderEvent struct {
	ID               uint64
	EventID          string
	AppID            string
	OrderNo          string
	EventType        string
	AggregateVersion int64
	Payload          []byte
	CreatedAt        time.Time
}

// OutboxStatus 发件箱状态。
type OutboxStatus int8

const (
	OutboxPending   OutboxStatus = 0
	OutboxDelivered OutboxStatus = 1
	OutboxDead      OutboxStatus = 2
)

// EventOutbox 事件发件箱(event_outbox)。
type EventOutbox struct {
	ID               uint64
	EventID          string
	AppID            string
	OrderNo          string
	EventType        string
	AggregateVersion int64
	Payload          []byte
	Status           OutboxStatus
	Attempts         int32
	NextAttemptAt    time.Time
	LockedBy         string
	LockedUntil      sql.NullTime
	LastError        sql.NullString
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

// BatchStatus 对账批次状态。
type BatchStatus int8

const (
	BatchPending BatchStatus = 0 // 待拉取
	BatchPulled  BatchStatus = 1 // 已拉取
	BatchDone    BatchStatus = 2 // 对账完成
	BatchFailed  BatchStatus = 3 // 失败
)

// ReconcileBatch 渠道账单对账批次(reconcile_batches)。
type ReconcileBatch struct {
	ID           uint64
	BatchNo      string
	AppID        string
	Channel      string
	PayAccountID string
	BillDate     time.Time
	Status       BatchStatus
	SourceCount  int32
	MatchedCount int32
	DiffCount    int32
	SourceAmount int64
	LocalAmount  int64
	LockedBy     string
	LockedUntil  sql.NullTime
	PulledAt     sql.NullTime
	ReconciledAt sql.NullTime
	LastError    sql.NullString
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// DiffType 对账差异类型。
type DiffType int8

const (
	DiffSurplus DiffType = 1 // 长款:渠道有本地无
	DiffShort   DiffType = 2 // 短款:本地有渠道无
	DiffAmount  DiffType = 3 // 金额不一致
	DiffStatus  DiffType = 4 // 状态不一致
)

// ReconcileDiff 对账差异(reconcile_diffs)。
type ReconcileDiff struct {
	ID            uint64
	BatchID       uint64
	AppID         string
	Channel       string
	PayAccountID  string
	DiffType      DiffType
	OrderNo       string
	TransactionID string
	OutTradeNo    string
	GatewayAmount int64
	LocalAmount   int64
	Detail        string
	HandleStatus  int8
	HandledBy     string
	HandledAt     sql.NullTime
	HandleRemark  string
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// AppPayAccount 多租户支付账号(app_pay_accounts)。
type AppPayAccount struct {
	PayAccountID  string `gorm:"primaryKey"`
	AppID         string
	Channel       string
	MchID         string
	PayAppID      string
	NotifyRouteID string
	ConfigRef     string
	ConfigVersion int32
	Enabled       bool
}

// AppEndpoint 接入方回调配置(app_endpoints)。
type AppEndpoint struct {
	AppID             string `gorm:"primaryKey"`
	WebhookURL        string
	KeyID             string
	HMACSecretRef     string
	PrevKeyID         string
	PrevHMACSecretRef string
	MaxConcurrency    int32
	Enabled           bool
}
