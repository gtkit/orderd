package models

import (
	"sync"
	"testing"

	"gorm.io/gorm/schema"
)

// ddlColumns 是各表在 docs/sql/schema.sql 中的真实列集(权威)。
// 生成列(active_order/active_guard)由 DB 维护、不在 model,故不列入。
var ddlColumns = map[string]map[string]bool{
	"orders": set(
		"id", "app_id", "idempotency_key", "request_hash", "client_order_no", "order_no", "order_token",
		"user_id", "status", "close_reason", "product_id", "product_title", "business_type",
		"metadata_type", "metadata_version", "metadata", "currency", "original_price", "discount_amount",
		"pay_amount", "refunded_amount", "pay_method", "pay_channel", "pay_account_id", "pay_mch_id",
		"pay_app_id", "config_version", "settled_attempt_id", "settled_channel", "settled_out_trade_no",
		"settled_pay_account_id", "settled_mch_id", "settled_pay_app_id", "settled_config_version",
		"trade_no", "paid_at", "channel_id", "aggregate_version", "expire_at", "client_ip", "client_ip_str",
		"remark", "admin_remark", "created_at", "updated_at",
	),
	"payment_attempts": set(
		"id", "app_id", "order_no", "attempt_no", "channel", "pay_account_id", "config_version",
		"out_trade_no", "request_hash", "status", "prepay_id", "last_error", "expire_at", "next_query_at",
		"query_count", "locked_by", "locked_until", "created_at", "updated_at",
	),
	"refunds": set(
		"id", "app_id", "refund_no", "out_refund_no", "request_hash", "order_no", "pay_account_id",
		"refund_amount", "status", "gateway_refund_id", "reason", "succeeded_at", "last_error",
		"next_query_at", "query_count", "locked_by", "locked_until", "created_at", "updated_at",
	),
	"payment_transactions": set(
		"id", "app_id", "order_no", "payment_attempt_id", "refund_no", "pay_account_id", "channel",
		"kind", "out_trade_no", "transaction_id", "amount", "currency", "anomaly", "paid_at",
		"notify_digest", "created_at",
	),
	"order_logs": set(
		"id", "order_id", "order_no", "from_status", "to_status", "operator", "remark", "created_at",
	),
	"order_events": set(
		"id", "event_id", "app_id", "order_no", "event_type", "aggregate_version", "payload", "created_at",
	),
	"event_outbox": set(
		"id", "event_id", "app_id", "order_no", "event_type", "aggregate_version", "payload", "status",
		"attempts", "next_attempt_at", "locked_by", "locked_until", "last_error", "created_at", "updated_at",
	),
	"reconcile_batches": set(
		"id", "batch_no", "app_id", "channel", "pay_account_id", "bill_date", "status", "source_count",
		"matched_count", "diff_count", "source_amount", "local_amount", "locked_by", "locked_until",
		"pulled_at", "reconciled_at", "last_error", "created_at", "updated_at",
	),
	"reconcile_diffs": set(
		"id", "batch_id", "app_id", "channel", "pay_account_id", "diff_type", "order_no", "transaction_id",
		"out_trade_no", "gateway_amount", "local_amount", "detail", "handle_status", "handled_by",
		"handled_at", "handle_remark", "created_at", "updated_at",
	),
	"app_pay_accounts": set(
		"pay_account_id", "app_id", "channel", "mch_id", "pay_app_id", "notify_route_id", "config_ref",
		"config_version", "enabled", "created_at", "updated_at",
	),
	"app_endpoints": set(
		"app_id", "webhook_url", "key_id", "hmac_secret_ref", "prev_key_id", "prev_hmac_secret_ref",
		"max_concurrency", "enabled", "created_at", "updated_at",
	),
}

func set(cols ...string) map[string]bool {
	m := make(map[string]bool, len(cols))
	for _, c := range cols {
		m[c] = true
	}
	return m
}

// TestGormSchemaMappingMatchesDDL 用 gorm 的 schema.Parse 实测每个 model:
//   - 解析出的表名 == 预期(schema.Tabler 生效);
//   - 每个字段落库列名 ∈ DDL 真实列集(gorm 默认命名策略与 DDL 一致,无静默错列)。
//
// 这是资金链持久化的映射护栏:字段改名、gorm 版本命名变化都会在此报错。
func TestGormSchemaMappingMatchesDDL(t *testing.T) {
	cases := []struct {
		model any
		table string
	}{
		{&Order{}, "orders"},
		{&PaymentAttempt{}, "payment_attempts"},
		{&Refund{}, "refunds"},
		{&PaymentTransaction{}, "payment_transactions"},
		{&OrderLog{}, "order_logs"},
		{&OrderEvent{}, "order_events"},
		{&EventOutbox{}, "event_outbox"},
		{&ReconcileBatch{}, "reconcile_batches"},
		{&ReconcileDiff{}, "reconcile_diffs"},
		{&AppPayAccount{}, "app_pay_accounts"},
		{&AppEndpoint{}, "app_endpoints"},
	}
	namer := schema.NamingStrategy{}
	cache := &sync.Map{}
	for _, tc := range cases {
		sch, err := schema.Parse(tc.model, cache, namer)
		if err != nil {
			t.Fatalf("解析 %T schema 失败: %v", tc.model, err)
		}
		if sch.Table != tc.table {
			t.Fatalf("%T 表名=%q,期望 %q", tc.model, sch.Table, tc.table)
		}
		cols := ddlColumns[tc.table]
		for _, f := range sch.Fields {
			if f.DBName == "" {
				continue // 关联/忽略字段
			}
			if !cols[f.DBName] {
				t.Errorf("%T 字段 %s 映射到列 %q,但该列不在 %s 的 DDL 列集中(命名错位)",
					tc.model, f.Name, f.DBName, tc.table)
			}
		}
	}
}
