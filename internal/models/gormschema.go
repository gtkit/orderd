package models

// gorm 表名映射(schema.Tabler)。显式声明,消除 gorm 复数化/命名策略歧义
// (尤其 event_outbox:gorm 默认会复数化为 event_outboxes,与 DDL 不符)。
// 列名依赖 gorm 默认 snake_case 命名,并由 gormschema_test.go 对 DDL 真实列集逐字段校验。

func (Order) TableName() string              { return "orders" }
func (PaymentAttempt) TableName() string     { return "payment_attempts" }
func (Refund) TableName() string             { return "refunds" }
func (PaymentTransaction) TableName() string { return "payment_transactions" }
func (OrderLog) TableName() string           { return "order_logs" }
func (OrderEvent) TableName() string         { return "order_events" }
func (EventOutbox) TableName() string        { return "event_outbox" }
func (ReconcileBatch) TableName() string     { return "reconcile_batches" }
func (ReconcileDiff) TableName() string      { return "reconcile_diffs" }
func (AppPayAccount) TableName() string      { return "app_pay_accounts" }
func (AppEndpoint) TableName() string        { return "app_endpoints" }
func (OpsAuditLog) TableName() string        { return "ops_audit_logs" }
