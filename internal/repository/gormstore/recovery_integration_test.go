//go:build integration

package gormstore_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gtkit/orderd/internal/models"
	reconcileapp "github.com/gtkit/orderd/internal/module/reconcile/application"
	"github.com/gtkit/orderd/internal/shared/alert"
	"github.com/gtkit/orderd/internal/shared/bill"
	"github.com/gtkit/orderd/internal/store"
)

type recoveryIntegrationBill struct{}

func (recoveryIntegrationBill) Fetch(ctx context.Context, _, _ string, _ time.Time, _ func(bill.Entry) error) (bill.Summary, error) {
	return bill.Summary{}, ctx.Err()
}

func TestIntegrationRecoveryCalendarCursor(t *testing.T) {
	st := openTestStore(t)
	t.Cleanup(func() { _ = st.Close() })
	ctx := t.Context()
	now := time.Now().UTC().Truncate(time.Second)
	start := now.AddDate(0, 0, -75)
	start = time.Date(start.Year(), start.Month(), start.Day(), 0, 0, 0, 0, time.UTC)
	if err := st.DB().WithContext(ctx).Model(&models.AppPayAccount{}).Where("pay_account_id = ?", "acc-wechat").Update("created_at", start).Error; err != nil {
		t.Fatal(err)
	}
	a, err := st.Reader().Reconcile().GetScanAccount(ctx, "acc-wechat", false)
	if err != nil {
		t.Fatal(err)
	}
	var seq atomic.Int64
	svc := reconcileapp.New(reconcileapp.Deps{Store: st, Bill: recoveryIntegrationBill{}, Alerter: alert.Nop{}, Clock: func() time.Time { return now }, NewBatchNo: func() string { return fmt.Sprintf("calendar-%d", seq.Add(1)) }})
	through := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, -1)
	if n, err := svc.RecoverAccount(ctx, *a, through); err != nil || n != 8 {
		t.Fatalf("first page: n=%d err=%v", n, err)
	}
	a, err = st.Reader().Reconcile().GetScanAccount(ctx, a.PayAccountID, false)
	if err != nil || !a.ReconcileScannedThrough.Time.Equal(start.AddDate(0, 0, 29)) {
		t.Fatalf("cursor precision: %+v %v", a, err)
	}
	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() {
			if _, err := svc.RecoverAccount(ctx, *a, through); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	a, err = st.Reader().Reconcile().GetScanAccount(ctx, a.PayAccountID, false)
	if err != nil || !a.ReconcileScannedThrough.Time.Equal(through) {
		t.Fatalf("concurrent cursor: %+v %v", a, err)
	}
	var count int64
	if err := st.DB().WithContext(ctx).Model(&models.ReconcileBatch{}).Count(&count).Error; err != nil || count != 75 {
		t.Fatalf("calendar gaps/duplicates: count=%d err=%v", count, err)
	}
	rollback := errors.New("rollback calendar")
	err = st.WithinTx(ctx, func(ctx context.Context, r store.Repos) error {
		if _, err := r.Reconcile().GetScanAccount(ctx, a.PayAccountID, true); err != nil {
			return err
		}
		if err := r.Reconcile().CreateBatch(ctx, &models.ReconcileBatch{BatchNo: "rollback-calendar", Channel: a.Channel, PayAccountID: a.PayAccountID, BillDate: through.AddDate(0, 0, 1)}); err != nil {
			return err
		}
		if err := r.Reconcile().UpdateScanCursor(ctx, a.PayAccountID, through.AddDate(0, 0, 1)); err != nil {
			return err
		}
		return rollback
	})
	if !errors.Is(err, rollback) {
		t.Fatal(err)
	}
	a, _ = st.Reader().Reconcile().GetScanAccount(ctx, a.PayAccountID, false)
	b, err := st.Reader().Reconcile().GetBatchByAccountDate(ctx, a.Channel, a.PayAccountID, through.AddDate(0, 0, 1))
	if err != nil || b != nil || !a.ReconcileScannedThrough.Time.Equal(through) {
		t.Fatalf("rollback leaked: account=%+v batch=%+v err=%v", a, b, err)
	}
}

func TestIntegrationRecoveryLeaseCAS(t *testing.T) {
	st := openTestStore(t)
	t.Cleanup(func() { _ = st.Close() })
	ctx := t.Context()
	now := time.Now().UTC().Truncate(time.Second)
	b := &models.ReconcileBatch{BatchNo: "lease-batch", Channel: "wechat", PayAccountID: "acc-wechat", BillDate: now.AddDate(0, 0, -1)}
	if err := st.Reader().Reconcile().CreateBatch(ctx, b); err != nil {
		t.Fatal(err)
	}
	for i, token := range []string{"worker-same-generation-A", "worker-same-generation-B"} {
		at := now.Add(time.Duration(i) * 2 * time.Second)
		if ok, err := st.Reader().Reconcile().ClaimBatch(ctx, b.ID, token, at, at.Add(time.Second)); err != nil || !ok {
			t.Fatalf("claim %s: %v %v", token, ok, err)
		}
	}
	old := *b
	b.Status, b.MatchedCount = models.BatchDone, 7
	b.ReconciledAt = sql.NullTime{Time: now, Valid: true}
	if ok, err := st.Reader().Reconcile().UpdateBatch(ctx, b, "worker-same-generation-B"); err != nil || !ok {
		t.Fatalf("finish: %v %v", ok, err)
	}
	old.Status = models.BatchFailed
	if ok, err := st.Reader().Reconcile().UpdateBatch(ctx, &old, "worker-same-generation-A"); err != nil || ok {
		t.Fatalf("stale failure applied: %v %v", ok, err)
	}
	got, err := st.Reader().Reconcile().GetBatchByAccountDate(ctx, b.Channel, b.PayAccountID, b.BillDate)
	if err != nil || got.Status != models.BatchDone || got.MatchedCount != 7 || !got.ReconciledAt.Time.Equal(now) {
		t.Fatalf("completed state lost: %+v %v", got, err)
	}
	ob := &models.EventOutbox{EventID: "recovery-outbox", AppID: "app1", OrderNo: "OD-recovery", EventType: models.EventTypePaid, Payload: []byte("x"), Status: models.OutboxPending, NextAttemptAt: now}
	if err := st.Reader().Events().InsertOutbox(ctx, ob); err != nil {
		t.Fatal(err)
	}
	for i, token := range []string{"outbox-A", "outbox-B"} {
		at := now.Add(time.Duration(i) * 2 * time.Second)
		err := st.WithinTx(ctx, func(ctx context.Context, r store.Repos) error {
			rows, err := r.Outbox().ClaimDue(ctx, token, at, at.Add(time.Second), 1)
			if err == nil && len(rows) != 1 {
				return fmt.Errorf("claim returned %d rows", len(rows))
			}
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if applied, err := st.Reader().Outbox().Reschedule(ctx, ob.ID, "outbox-A", now, 1, "stale"); err != nil || applied {
		t.Fatalf("old outbox result: %v %v", applied, err)
	}
	current, err := st.Reader().Outbox().GetByEventID(ctx, ob.EventID, false)
	if err != nil || current.LockedBy != "outbox-B" || current.Attempts != 0 {
		t.Fatalf("lease overwritten: %+v %v", current, err)
	}
	if applied, err := st.Reader().Outbox().MarkDead(ctx, ob.ID, "outbox-B", 8, "failure"); err != nil || !applied {
		t.Fatalf("current failure: %v %v", applied, err)
	}
	if err := st.Reader().Outbox().MarkDelivered(ctx, ob.ID); err != nil {
		t.Fatal(err)
	}
	if applied, err := st.Reader().Outbox().MarkDead(ctx, ob.ID, "outbox-B", 9, "stale"); err != nil || applied {
		t.Fatalf("failure reversed success: %v %v", applied, err)
	}
}

func TestIntegrationRecoveryClosingScan(t *testing.T) {
	st := openTestStore(t)
	t.Cleanup(func() { _ = st.Close() })
	ctx := t.Context()
	now := time.Now().UTC()
	for i := range 101 {
		no := fmt.Sprintf("recovery-closing-%d", i)
		o := &models.Order{AppID: "app1", OrderNo: no, OrderToken: no, UserID: 1, Status: models.OrderClosing, ProductID: 1, ProductTitle: "x", Currency: "CNY", OriginalPrice: 1, PayAmount: 1, PayMethod: models.PayMethodWechat, PayChannel: "wechat", PayAccountID: "acc-wechat", ExpireAt: now.Add(-time.Hour), CreatedAt: now.Add(-time.Hour), UpdatedAt: now.Add(-time.Hour)}
		if err := st.Reader().Orders().Insert(ctx, o); err != nil {
			t.Fatal(err)
		}
		if i < 100 {
			a := &models.PaymentAttempt{AppID: "app1", OrderNo: no, AttemptNo: 1, Channel: "wechat", PayAccountID: "acc-wechat", OutTradeNo: no, Status: models.AttemptClosing, ExpireAt: now}
			if err := st.Reader().Attempts().Insert(ctx, a); err != nil {
				t.Fatal(err)
			}
		}
	}
	rows, err := st.Reader().Orders().ListStalledClosing(ctx, now, 1)
	if err != nil || len(rows) != 1 || rows[0].OrderNo != "recovery-closing-100" {
		t.Fatalf("active prefix starved orphan: rows=%+v err=%v", rows, err)
	}
	var indexCount int64
	if err := st.DB().WithContext(ctx).Raw("SELECT COUNT(DISTINCT index_name) FROM information_schema.statistics WHERE table_schema=DATABASE() AND index_name IN ('idx_closing_scan','idx_reconcile_claim')").Scan(&indexCount).Error; err != nil || indexCount != 2 {
		t.Fatalf("recovery indexes missing: count=%d err=%v", indexCount, err)
	}
	var plan []map[string]any
	if err := st.DB().WithContext(ctx).Raw("EXPLAIN SELECT id FROM reconcile_batches WHERE channel=? AND pay_account_id=? AND status=? AND (locked_until IS NULL OR locked_until<=?) ORDER BY locked_until,id LIMIT 1", "wechat", "acc-wechat", models.BatchPending, now).Scan(&plan).Error; err != nil {
		t.Fatal(err)
	}
	t.Logf("reconcile claim plan: %+v", plan)
}
