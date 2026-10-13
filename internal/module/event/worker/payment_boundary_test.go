package worker

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gtkit/orderd/internal/models"
	"github.com/gtkit/orderd/internal/paygateway"
	"github.com/gtkit/orderd/internal/store"
	"github.com/gtkit/orderd/internal/store/memstore"
)

func TestPaymentNotExistDeadlineMatrix(t *testing.T) {
	for _, status := range []models.AttemptStatus{models.AttemptReady, models.AttemptUnknown, models.AttemptClosing} {
		for _, delta := range []time.Duration{-time.Nanosecond, 0, time.Nanosecond} {
			t.Run(fmt.Sprintf("status=%d/boundary=%s", status, delta), func(t *testing.T) {
				mem := memstore.New()
				seed(t, mem, "boundary", models.OrderClosing, models.CloseTimeout, status, 100)
				err := mem.WithinTx(bg, func(ctx context.Context, r store.Repos) error {
					a, err := r.Attempts().GetByOutTradeNo(ctx, "PT-boundary", true)
					if err != nil {
						return err
					}
					a.ExpireAt = qClock().Add(-2*time.Minute - delta)
					return r.Attempts().Update(ctx, a)
				})
				if err != nil {
					t.Fatal(err)
				}
				w := buildQueryWorker(mem, paygateway.QueryResult{State: paygateway.QueryNotExist}, 6)
				if n, err := w.RunOnce(bg); err != nil || n != 1 {
					t.Fatalf("run: %d %v", n, err)
				}
				a := mem.Attempts("boundary")[0]
				if delta < 0 {
					if a.Status != status || a.QueryCount != 101 || hasEvent(mem, models.EventTypeClosed) {
						t.Fatalf("premature closure: %+v", a)
					}
				} else if a.Status != models.AttemptClosed || orderStatus(t, mem, "boundary") != models.OrderClosed || !hasEvent(mem, models.EventTypeClosed) {
					t.Fatalf("deadline not converged: %+v", a)
				}
			})
		}
	}
}

func TestPaymentQueryStaleLeaseCannotCloseOrReschedule(t *testing.T) {
	mem := memstore.New()
	seed(t, mem, "lease", models.OrderPending, models.CloseNone, models.AttemptUnknown, 8)
	a := mem.Attempts("lease")[0]
	a.LockedBy = "old-claim"
	err := mem.WithinTx(bg, func(ctx context.Context, r store.Repos) error {
		cur, e := r.Attempts().GetByOutTradeNo(ctx, a.OutTradeNo, true)
		if e != nil {
			return e
		}
		cur.LockedBy, cur.QueryCount = "new-claim", 12
		return r.Attempts().Update(ctx, cur)
	})
	if err != nil {
		t.Fatal(err)
	}
	w := buildQueryWorker(mem, paygateway.QueryResult{}, 6)
	w.reschedule(bg, &a)
	if err := w.payment.ApplyQueryNotPaid(bg, a.OutTradeNo, a.LockedBy); err != nil {
		t.Fatal(err)
	}
	if err := w.payment.CloseAttemptAndMaybeOrder(bg, a.OutTradeNo, a.LockedBy); err != nil {
		t.Fatal(err)
	}
	cur := mem.Attempts("lease")[0]
	if cur.Status != models.AttemptUnknown || cur.LockedBy != "new-claim" || cur.QueryCount != 12 {
		t.Fatalf("stale write: %+v", cur)
	}
}

func TestReadyAttemptActivelyQueriesPaidAndNotPaid(t *testing.T) {
	for _, state := range []paygateway.QueryState{paygateway.QueryPaid, paygateway.QueryNotPay} {
		t.Run(fmt.Sprint(state), func(t *testing.T) {
			mem := memstore.New()
			seed(t, mem, "ready", models.OrderPending, models.CloseNone, models.AttemptReady, 0)
			w := buildQueryWorker(mem, paygateway.QueryResult{State: state, TransactionID: "tx-ready", Amount: 90, PaidAt: qClock()}, 6)
			if n, err := w.RunOnce(bg); n != 1 || err != nil {
				t.Fatalf("run: %d %v", n, err)
			}
			a := mem.Attempts("ready")[0]
			if state == paygateway.QueryPaid {
				if a.Status != models.AttemptPaid || !hasEvent(mem, models.EventTypePaid) {
					t.Fatalf("payment not recovered: %+v", a)
				}
			} else if a.Status != models.AttemptReady || !a.NextQueryAt.Valid || a.LockedBy != "" {
				t.Fatalf("ready not rescheduled: %+v", a)
			}
		})
	}
}
