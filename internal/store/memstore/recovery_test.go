package memstore

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gtkit/orderd/internal/models"
	"github.com/gtkit/orderd/internal/store"
)

func TestRecoveryOutboxStaleFailure(t *testing.T) {
	m := New()
	ctx := t.Context()
	now := time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)
	ob := &models.EventOutbox{EventID: "stale", Status: models.OutboxPending, NextAttemptAt: now}
	if err := m.Reader().Events().InsertOutbox(ctx, ob); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Reader().Outbox().ClaimDue(ctx, "A", now, now.Add(time.Second), 1); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Reader().Outbox().ClaimDue(ctx, "B", now.Add(2*time.Second), now.Add(time.Minute), 1); err != nil {
		t.Fatal(err)
	}
	if ok, err := m.Reader().Outbox().Reschedule(ctx, ob.ID, "B", now.Add(time.Minute), 2, "new failure"); err != nil || !ok {
		t.Fatal(err)
	}
	if ok, err := m.Reader().Outbox().Reschedule(ctx, ob.ID, "A", now.Add(time.Second), 1, "old failure"); err != nil || ok {
		t.Fatal(err)
	}
	got, err := m.Reader().Outbox().GetByEventID(ctx, ob.EventID, false)
	if err != nil {
		t.Fatal(err)
	}
	if got.Attempts != 2 {
		t.Fatalf("旧结果回退次数: got=%d want=2", got.Attempts)
	}
}

func TestRecoveryClosingCandidatesExcludeActive(t *testing.T) {
	m := New()
	ctx := t.Context()
	now := time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)
	for i := range 101 {
		no := fmt.Sprintf("closing-%d", i)
		err := m.WithinTx(ctx, func(ctx context.Context, r store.Repos) error {
			if err := r.Orders().Insert(ctx, &models.Order{OrderNo: no, OrderToken: no, Status: models.OrderClosing, CreatedAt: now.Add(-time.Hour)}); err != nil {
				return err
			}
			if i < 100 {
				return r.Attempts().Insert(ctx, &models.PaymentAttempt{OrderNo: no, OutTradeNo: no, Status: models.AttemptClosing})
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	rows, err := m.Reader().Orders().ListStalledClosing(ctx, now, 200)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].OrderNo != "closing-100" {
		t.Fatalf("只应返回孤儿: count=%d", len(rows))
	}
}

func TestRecoveryReconcileStaleFailure(t *testing.T) {
	m := New()
	ctx := t.Context()
	b := &models.ReconcileBatch{BatchNo: "batch", Channel: "wechat", PayAccountID: "account", BillDate: time.Date(2026, 10, 11, 0, 0, 0, 0, time.UTC)}
	if err := m.Reader().Reconcile().CreateBatch(ctx, b); err != nil {
		t.Fatal(err)
	}
	old := *b
	if ok, err := m.Reader().Reconcile().ClaimBatch(ctx, b.ID, "B", b.BillDate, b.BillDate.Add(time.Minute)); err != nil || !ok {
		t.Fatal(err)
	}
	b.Status = models.BatchDone
	b.MatchedCount = 5
	if ok, err := m.Reader().Reconcile().UpdateBatch(ctx, b, "B"); err != nil || !ok {
		t.Fatal(err)
	}
	old.Status = models.BatchFailed
	if ok, err := m.Reader().Reconcile().UpdateBatch(ctx, &old, "A"); err != nil || ok {
		t.Fatal(err)
	}
	got, err := m.Reader().Reconcile().GetBatchByAccountDate(ctx, b.Channel, b.PayAccountID, b.BillDate)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != models.BatchDone || got.MatchedCount != 5 {
		t.Fatalf("完成结果被旧失败覆盖: %+v", got)
	}
}
