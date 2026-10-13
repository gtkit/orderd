//go:build integration

package gormstore_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/gtkit/orderd/internal/models"
	reconcileapp "github.com/gtkit/orderd/internal/module/reconcile/application"
	"github.com/gtkit/orderd/internal/pkg/sqlutil"
	"github.com/gtkit/orderd/internal/shared/alert"
	"github.com/gtkit/orderd/internal/shared/bill"
	"github.com/gtkit/orderd/internal/store"
)

type integrationStreamSource struct{ calls int }

func (s *integrationStreamSource) Fetch(ctx context.Context, _, _ string, _ time.Time, emit func(bill.Entry) error) (bill.Summary, error) {
	s.calls++
	if err := ctx.Err(); err != nil {
		return bill.Summary{}, err
	}
	err := emit(bill.Entry{Kind: models.TxnPay, TransactionID: "bill-pay", OutTradeNo: "bill-out", Amount: 100, Currency: "CNY", Status: "SUCCESS", MerchantID: "mch-1", ProviderAppID: "payapp-1"})
	return bill.Summary{Count: 1, PayAmount: 100, DigestAlgorithm: "sha256", Digest: "fixture-digest"}, err
}

func TestIntegrationReconcileV2DateAndLateLedger(t *testing.T) {
	st := openTestStore(t)
	t.Cleanup(func() { _ = st.Close() })
	ctx := t.Context()
	// 测试 DSN 默认 UTC；+8 零点必须作为 DATE 标签入库，不能落到前一天。
	day := time.Date(2026, 5, 1, 0, 0, 0, 0, bill.Location)
	now := time.Now().UTC().Add(time.Second).Truncate(time.Second)
	source := &integrationStreamSource{}
	seq := 0
	svc := reconcileapp.New(reconcileapp.Deps{Store: st, Bill: source, Alerter: alert.Nop{}, Clock: func() time.Time { return now }, NewBatchNo: func() string { seq++; return fmt.Sprintf("v2-%d", seq) }})
	if _, err := svc.Reconcile(ctx, "app1", "wechat", "acc-wechat", day); err != nil {
		t.Fatal(err)
	}
	b, err := st.Reader().Reconcile().GetBatchByAccountDate(ctx, "wechat", "acc-wechat", day, 2)
	if err != nil || b == nil || b.PublishedRunID == "" {
		t.Fatalf("date roundtrip: %+v %v", b, err)
	}
	var label string
	if err := st.DB().WithContext(ctx).Raw("SELECT DATE_FORMAT(bill_date,'%Y-%m-%d') FROM reconcile_batches WHERE id = ?", b.ID).Scan(&label).Error; err != nil || label != "2026-05-01" {
		t.Fatalf("DATE driver conversion: %s %v", label, err)
	}
	before := *b
	txn := &models.PaymentTransaction{AppID: "app1", OrderNo: "order", Channel: "wechat", PayAccountID: "acc-wechat", Kind: models.TxnPay, TransactionID: "bill-pay", OutTradeNo: "bill-out", Amount: 100, Currency: "CNY", PaidAt: sqlutil.Time(day.Add(time.Hour))}
	if err := st.WithinTx(ctx, func(ctx context.Context, r store.Repos) error { return r.Transactions().Insert(ctx, txn) }); err != nil {
		t.Fatal(err)
	}
	a, err := st.Reader().Reconcile().GetScanAccount(ctx, "acc-wechat", false)
	if err != nil {
		t.Fatal(err)
	}
	// through 早于账号创建日，不导入额外日期；本地消费者仍处理同事务唤醒。
	if _, err := svc.RecoverAccount(ctx, *a, day); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Reconcile(ctx, "app1", "wechat", "acc-wechat", day); err != nil {
		t.Fatal(err)
	}
	b, err = st.Reader().Reconcile().GetBatchByAccountDate(ctx, "wechat", "acc-wechat", day, 2)
	if err != nil || b.MatchedCount != 1 || b.DiffCount != 0 || b.SourceRunID != before.SourceRunID || b.RunID == before.RunID || source.calls != 1 {
		t.Fatalf("late local/reuse: %+v calls=%d err=%v", b, source.calls, err)
	}
	var c models.ReconcileLocalChange
	if err := st.DB().WithContext(ctx).Where("txn_id = ?", txn.ID).Take(&c).Error; err != nil || !c.Done {
		t.Fatalf("local wake not completed: %+v %v", c, err)
	}
	var audits int64
	st.DB().WithContext(ctx).Model(&models.OpsAuditLog{}).Where("action = ?", models.OpsActionReconcileDiffHandle).Count(&audits)
	if audits != 1 {
		t.Fatalf("matched old pending diff audit=%d", audits)
	}
}

func TestIntegrationReconcileV2LedgerWakeRollbackAndLateCommit(t *testing.T) {
	st := openTestStore(t)
	t.Cleanup(func() { _ = st.Close() })
	ctx := t.Context()
	rollback := errors.New("rollback")
	tx := func(id string) *models.PaymentTransaction {
		return &models.PaymentTransaction{AppID: "app1", OrderNo: "order", Channel: "wechat", PayAccountID: "acc-wechat", Kind: models.TxnPay, TransactionID: id, OutTradeNo: id, Amount: 100, Currency: "CNY"}
	}
	failed := tx("rolled-back")
	if err := st.WithinTx(ctx, func(ctx context.Context, r store.Repos) error {
		if err := r.Transactions().Insert(ctx, failed); err != nil {
			return err
		}
		return rollback
	}); !errors.Is(err, rollback) {
		t.Fatal(err)
	}
	var count int64
	st.DB().WithContext(ctx).Model(&models.ReconcileLocalChange{}).Where("txn_id = ?", failed.ID).Count(&count)
	if count != 0 {
		t.Fatal("rolled-back ledger left wake")
	}
	inserted, release := make(chan uint64, 1), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- st.WithinTx(ctx, func(ctx context.Context, r store.Repos) error {
			first := tx("earlier-id-late-commit")
			if err := r.Transactions().Insert(ctx, first); err != nil {
				return err
			}
			inserted <- first.ID
			<-release
			return nil
		})
	}()
	firstID := <-inserted
	second := tx("later-id-first-commit")
	if err := st.WithinTx(ctx, func(ctx context.Context, r store.Repos) error { return r.Transactions().Insert(ctx, second) }); err != nil {
		close(release)
		t.Fatal(err)
	}
	if second.ID <= firstID {
		close(release)
		t.Fatal("commit-order fixture IDs")
	}
	if err := st.WithinTx(ctx, func(ctx context.Context, r store.Repos) error {
		return r.Reconcile().UpdateLocalSeedCursor(ctx, "acc-wechat", second.ID)
	}); err != nil {
		close(release)
		t.Fatal(err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	var wake models.ReconcileLocalChange
	if err := st.DB().WithContext(ctx).Where("txn_id = ?", firstID).Take(&wake).Error; err != nil || wake.Done {
		t.Fatalf("small ID late commit lost behind seed cursor: %+v %v", wake, err)
	}
}

func TestIntegrationReconcileV2RunAndRepairCAS(t *testing.T) {
	st := openTestStore(t)
	t.Cleanup(func() { _ = st.Close() })
	ctx := t.Context()
	now := time.Now().UTC().Truncate(time.Second)
	b := &models.ReconcileBatch{AlgorithmVersion: 2, BatchNo: "stage-cas", Channel: "wechat", PayAccountID: "acc-wechat", BillDate: now}
	if err := st.Reader().Reconcile().CreateBatch(ctx, b); err != nil {
		t.Fatal(err)
	}
	claim := func(token string, at time.Time) {
		t.Helper()
		ok, err := st.Reader().Reconcile().ClaimBatch(ctx, b.ID, token, at, at.Add(time.Minute))
		if err != nil || !ok {
			t.Fatalf("claim: %v %v", ok, err)
		}
		b.LockedBy = token
		b.LockedUntil = sqlutil.Time(at.Add(time.Minute))
	}
	save := func(token, expected string, at time.Time, publish bool) bool {
		t.Helper()
		var applied bool
		err := st.WithinTx(ctx, func(ctx context.Context, r store.Repos) error {
			var err error
			applied, err = r.Reconcile().SaveRun(ctx, b, token, expected, at, nil, nil, publish)
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
		return applied
	}
	claim("same-worker-generation-1", now)
	b.RunID = "source-run"
	b.SourceRunID = b.RunID
	if !save("same-worker-generation-1", "", now, false) {
		t.Fatal("begin")
	}
	claim("same-worker-generation-2", now.Add(time.Minute))
	b.ImportComplete = true
	b.Status = models.BatchDone
	if save("same-worker-generation-1", "source-run", now.Add(time.Minute), true) {
		t.Fatal("stale source published")
	}
	if save("same-worker-generation-2", "source-run", now.Add(2*time.Minute), true) {
		t.Fatal("expired generation published")
	}
	b.LockedBy = ""
	b.LockedUntil = sql.NullTime{}
	if !save("same-worker-generation-2", "source-run", now.Add(61*time.Second), true) {
		t.Fatal("current generation could not publish")
	}
}

func TestIntegrationReconcileV2MigrationPreservesV1(t *testing.T) {
	st := openTestStore(t)
	t.Cleanup(func() { _ = st.Close() })
	cfg, err := mysql.ParseDSN(os.Getenv("ORDERD_TEST_MYSQL_DSN"))
	if err != nil {
		t.Fatal("test DSN")
	}
	cfg.MultiStatements = true
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx := t.Context()
	// 只对刚由 openTestStore 重建的专用测试表还原 V1 形状。
	alters := []string{
		"DROP TABLE reconcile_bill_entries", "DROP TABLE reconcile_local_changes",
		"ALTER TABLE app_pay_accounts DROP COLUMN reconcile_scan_version,DROP COLUMN reconcile_local_seed_cursor",
		"ALTER TABLE payment_transactions DROP INDEX idx_account_seed",
		"ALTER TABLE reconcile_batches DROP INDEX uk_account_date,ADD UNIQUE KEY uk_account_date(channel,pay_account_id,bill_date),DROP INDEX idx_reconcile_claim,ADD KEY idx_reconcile_claim(channel,pay_account_id,status,locked_until,id)",
		"ALTER TABLE reconcile_diffs DROP INDEX uk_batch_txn,ADD UNIQUE KEY uk_batch_txn(batch_id,channel,transaction_id,diff_type),DROP INDEX idx_repair_claim,DROP INDEX idx_local_diff",
	}
	for table, cols := range map[string]string{"reconcile_batches": "algorithm_version,run_id,source_run_id,published_run_id,published_source_run_id,import_complete,compare_after_id,dirty_seq,run_dirty_seq,published_dirty_seq,source_refund_amount,local_refund_amount,source_digest_algorithm,source_digest", "reconcile_diffs": "run_id,entry_id,kind,identity_key,local_txn_id,original_transaction_id,out_refund_no,refund_no,currency,verify_attempts,next_verify_at,verify_locked_by,verify_locked_until,verify_last_error"} {
		parts := strings.Split(cols, ",")
		for i := range parts {
			parts[i] = "DROP COLUMN " + parts[i]
		}
		alters = append(alters, "ALTER TABLE "+table+" "+strings.Join(parts, ","))
	}
	for _, q := range alters {
		if _, err := db.ExecContext(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO reconcile_batches(batch_no,channel,pay_account_id,bill_date,status)VALUES('legacy','wechat','acc-wechat','2026-05-01',2); INSERT INTO reconcile_diffs(batch_id,channel,pay_account_id,diff_type,transaction_id,handle_status,handled_by,handle_remark)VALUES(1,'wechat','acc-wechat',1,'legacy-txn-a',3,'human','keep'),(1,'wechat','acc-wechat',1,'legacy-txn-b',0,'','')`); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile("../../../docs/sql/20261012000003_add_streaming_reconciliation.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, string(raw)); err != nil {
		t.Fatal(err)
	}
	var version int
	var status int
	var remark, key string
	if err := db.QueryRowContext(ctx, "SELECT algorithm_version,status FROM reconcile_batches WHERE batch_no='legacy'").Scan(&version, &status); err != nil || version != 1 || status != 2 {
		t.Fatalf("legacy batch: %d/%d %v", version, status, err)
	}
	if err := db.QueryRowContext(ctx, "SELECT handle_status,handle_remark,identity_key FROM reconcile_diffs WHERE transaction_id='legacy-txn-a'").Scan(&status, &remark, &key); err != nil || status != 3 || remark != "keep" || len(key) != 64 {
		t.Fatalf("legacy manual evidence changed: %d %s %s %v", status, remark, key, err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO reconcile_batches(batch_no,channel,pay_account_id,bill_date,algorithm_version)VALUES('algorithm-2','wechat','acc-wechat','2026-05-01',2)`); err != nil {
		t.Fatalf("V1 Done swallowed new algorithm: %v", err)
	}
	for _, table := range []string{"reconcile_bill_entries", "reconcile_local_changes"} {
		var n int
		if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name=?", table).Scan(&n); err != nil || n != 1 {
			t.Fatalf("missing table %s: %d %v", table, n, err)
		}
	}
}

func TestIntegrationReconcileV2ManualPublicationBarrier(t *testing.T) {
	st := openTestStore(t)
	t.Cleanup(func() { _ = st.Close() })
	ctx := t.Context()
	now := time.Now().UTC().Truncate(time.Second)
	b := &models.ReconcileBatch{AlgorithmVersion: 2, BatchNo: "manual-barrier", Channel: "wechat", PayAccountID: "acc-wechat", BillDate: now, RunID: "old-run", SourceRunID: "source", PublishedRunID: "old-run", PublishedSourceRunID: "source", ImportComplete: true}
	if err := st.Reader().Reconcile().CreateBatch(ctx, b); err != nil {
		t.Fatal(err)
	}
	old := &models.ReconcileDiff{BatchID: b.ID, RunID: "old-run", EntryID: 1, Kind: models.TxnPay, IdentityKey: strings.Repeat("a", 64), AppID: "app1", Channel: "wechat", PayAccountID: "acc-wechat", DiffType: models.DiffSurplus, TransactionID: "money"}
	if err := st.Reader().Reconcile().InsertDiff(ctx, old); err != nil {
		t.Fatal(err)
	}
	if ok, err := st.Reader().Reconcile().ClaimBatch(ctx, b.ID, "publisher", now, now.Add(time.Minute)); err != nil || !ok {
		t.Fatalf("claim: %v %v", ok, err)
	}
	b.LockedBy = "publisher"
	b.LockedUntil = sqlutil.Time(now.Add(time.Minute))
	b.RunID = "new-run"
	copy := *old
	copy.ID = 0
	if err := st.WithinTx(ctx, func(ctx context.Context, r store.Repos) error {
		ok, err := r.Reconcile().SaveRun(ctx, b, "publisher", "old-run", now, nil, []models.ReconcileDiff{copy}, false)
		if err == nil && !ok {
			return errors.New("stage not applied")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	held, release := make(chan struct{}), make(chan struct{})
	manualDone := make(chan error, 1)
	go func() {
		manualDone <- st.WithinTx(ctx, func(ctx context.Context, r store.Repos) error {
			if _, err := r.Reconcile().GetDiff(ctx, old.ID, true); err != nil {
				return err
			}
			close(held)
			<-release
			return r.Reconcile().UpdateDiffHandling(ctx, old.ID, models.DiffHandleIgnored, "human", now, "manual after stage")
		})
	}()
	select {
	case <-held:
	case err := <-manualDone:
		t.Fatalf("manual failed before barrier: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("manual did not acquire lock")
	}
	published := make(chan error, 1)
	go func() {
		published <- st.WithinTx(ctx, func(ctx context.Context, r store.Repos) error {
			b.Status = models.BatchDone
			b.LockedBy = ""
			b.LockedUntil = sql.NullTime{}
			ok, err := r.Reconcile().SaveRun(ctx, b, "publisher", "new-run", now, nil, nil, true)
			if err == nil && !ok {
				return errors.New("publish not applied")
			}
			return err
		})
	}()
	select {
	case err := <-published:
		close(release)
		t.Fatalf("publish bypassed held manual batch lock: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	if err := <-manualDone; err != nil {
		t.Fatal(err)
	}
	if err := <-published; err != nil {
		t.Fatal(err)
	}
	visible, err := st.Reader().Reconcile().ListDiffs(ctx, store.DiffFilter{}, 0, 100)
	if err != nil || len(visible) != 1 || visible[0].ID == old.ID || visible[0].HandleStatus != models.DiffHandleIgnored || visible[0].HandleRemark != "manual after stage" {
		t.Fatalf("manual state lost at publish: %+v %v", visible, err)
	}
	if err := st.WithinTx(ctx, func(ctx context.Context, r store.Repos) error {
		_, err := r.Reconcile().GetDiff(ctx, old.ID, true)
		return err
	}); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("retired run actionable: %v", err)
	}
}
