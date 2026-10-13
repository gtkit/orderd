package models

import (
	"database/sql"
	"time"
)

// ReconcileEntry 是完整账单的逐行证据；仅所属批次的 PublishedRunID 可对外消费。
type ReconcileEntry struct {
	ID                    uint64
	BatchID               uint64
	RunID                 string
	Kind                  TxnKind
	IdentityKey           string
	Channel               string
	PayAccountID          string
	TransactionID         string
	OriginalTransactionID string
	OutTradeNo            string
	OutRefundNo           string
	Amount                int64
	Currency              string
	EventAt               sql.NullTime
	MerchantID            string
	ProviderAppID         string
	SourceStatus          string
	CreatedAt             time.Time
}

// ReconcileLocalChange 与财务流水同事务追加；重试覆盖跨日和迟到证据。
type ReconcileLocalChange struct {
	Channel      string
	PayAccountID string
	TxnID        uint64 `gorm:"primaryKey;autoIncrement:false"`
	Done         bool
	Attempts     int32
	NextAt       time.Time
	LockedBy     string
	LockedUntil  sql.NullTime
	LastError    sql.NullString
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

func (ReconcileEntry) TableName() string       { return "reconcile_bill_entries" }
func (ReconcileLocalChange) TableName() string { return "reconcile_local_changes" }
