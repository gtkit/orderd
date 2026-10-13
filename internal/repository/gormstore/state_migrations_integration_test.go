//go:build integration

package gormstore_test

import (
	"database/sql"
	"os"
	"testing"

	"github.com/go-sql-driver/mysql"
)

// TestIntegrationStateRecoveryMigrations checks the release SQL against the old
// shape, including existing in-flight refunds. openTestStore restricts all DDL to
// a local, dedicated test database and creates every table used by this test.
func TestIntegrationStateRecoveryMigrations(t *testing.T) {
	st := openTestStore(t)
	t.Cleanup(func() { _ = st.Close() })
	cfg, err := mysql.ParseDSN(os.Getenv("ORDERD_TEST_MYSQL_DSN"))
	if err != nil {
		t.Fatal("parse test DSN")
	}
	cfg.MultiStatements = true
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx := t.Context()
	// Remove only columns and indexes introduced by this change from the fresh
	// test tables to reproduce the pre-upgrade schema.
	for _, statement := range []string{
		`ALTER TABLE refunds DROP CHECK chk_refund_not_exist_count, DROP CHECK chk_refund_gateway_confirmed, DROP COLUMN gateway_confirmed, DROP COLUMN not_exist_count`,
		`ALTER TABLE app_pay_accounts DROP COLUMN reconcile_scanned_through`,
		`ALTER TABLE reconcile_batches DROP INDEX idx_reconcile_claim`,
		`ALTER TABLE orders DROP INDEX idx_closing_scan`,
	} {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	for status := range 6 {
		_, err := db.ExecContext(ctx, `INSERT INTO refunds (app_id,refund_no,out_refund_no,order_no,refund_amount,status,query_count,gateway_refund_id)
			VALUES ('migration',CONCAT('rf-',?),CONCAT('out-',?),CONCAT('order-',?),9007199254740993,?,99,IF(?=3,'known-refund',''))`, status, status, status, status, status)
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, path := range []string{
		"../../../docs/sql/20261012000001_alter_refunds_add_query_evidence.sql",
		"../../../docs/sql/20261012000002_add_reconcile_scan_cursor.sql",
	} {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(ctx, string(raw)); err != nil {
			t.Fatalf("migration %s: %v", path, err)
		}
	}
	for status := range 6 {
		var count, queryCount int
		var confirmed bool
		var amount int64
		if err := db.QueryRowContext(ctx, `SELECT not_exist_count,gateway_confirmed,refund_amount,query_count FROM refunds WHERE app_id='migration' AND status=?`, status).
			Scan(&count, &confirmed, &amount, &queryCount); err != nil {
			t.Fatal(err)
		}
		wantConfirmed := status == 0 || status == 1 || status == 3 || status == 5
		if count != 0 || confirmed != wantConfirmed || amount != 9007199254740993 || queryCount != 99 {
			t.Fatalf("status %d: evidence=%d/%v, amount=%d, queries=%d", status, count, confirmed, amount, queryCount)
		}
	}
	var cursor sql.NullTime
	if err := db.QueryRowContext(ctx, `SELECT reconcile_scanned_through FROM app_pay_accounts WHERE pay_account_id='acc-wechat'`).Scan(&cursor); err != nil || cursor.Valid {
		t.Fatalf("existing account cursor must remain NULL: %v %v", cursor, err)
	}
	for _, index := range []struct{ table, name string }{{"orders", "idx_closing_scan"}, {"reconcile_batches", "idx_reconcile_claim"}} {
		var n int
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema=DATABASE() AND table_name=? AND index_name=?`, index.table, index.name).Scan(&n); err != nil || n == 0 {
			t.Fatalf("missing migration index %s: %d %v", index.name, n, err)
		}
	}
	for _, statement := range []string{
		`UPDATE refunds SET not_exist_count=-1 WHERE app_id='migration' AND status=0`,
		`UPDATE refunds SET gateway_confirmed=2 WHERE app_id='migration' AND status=0`,
	} {
		if _, err := db.ExecContext(ctx, statement); err == nil {
			t.Fatal("migration check constraint accepted invalid evidence")
		}
	}
}
