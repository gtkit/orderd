package worker

import (
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gtkit/orderd/internal/models"
	"github.com/gtkit/orderd/internal/paygateway"
	"github.com/gtkit/orderd/internal/store"
	"github.com/gtkit/orderd/internal/store/memstore"
)

type stalledAlertFunc func(context.Context, string, map[string]any)

func (f stalledAlertFunc) Anomaly(ctx context.Context, kind string, fields map[string]any) {
	f(ctx, kind, fields)
}

type stalledQueryGateway struct {
	qGateway
	query func(context.Context, paygateway.QueryInput) (paygateway.QueryResult, error)
}

func (g stalledQueryGateway) Query(ctx context.Context, in paygateway.QueryInput) (paygateway.QueryResult, error) {
	return g.query(ctx, in)
}

func TestQueryStalledAlertIgnoresSupersededLease(t *testing.T) {
	for _, sameWorker := range []bool{true, false} {
		name := "another_worker"
		if sameWorker {
			name = "same_worker"
		}
		t.Run(name, func(t *testing.T) {
			mem := memstore.New()
			seed(t, mem, "alert-lease", models.OrderClosing, models.CloseUserCancel, models.AttemptClosing, 5)
			entered, release := make(chan struct{}), make(chan struct{})
			releaseFirst := sync.OnceFunc(func() { close(release) })
			t.Cleanup(releaseFirst)
			var seconds atomic.Int64
			clock := func() time.Time { return qClock().Add(time.Duration(seconds.Load()) * time.Second) }
			var queries atomic.Int32
			tokens := make(chan string, 2)
			gw := stalledQueryGateway{query: func(ctx context.Context, _ paygateway.QueryInput) (paygateway.QueryResult, error) {
				a, err := mem.Reader().Attempts().GetByOutTradeNo(ctx, "PT-alert-lease", false)
				if err != nil {
					return paygateway.QueryResult{}, err
				}
				tokens <- a.LockedBy
				if queries.Add(1) == 1 {
					close(entered)
					select {
					case <-release:
					case <-ctx.Done():
						return paygateway.QueryResult{}, ctx.Err()
					}
				}
				return paygateway.QueryResult{State: paygateway.QueryNotPay}, nil
			}}
			alerts := make(chan int32, 4)
			alerter := stalledAlertFunc(func(_ context.Context, kind string, fields map[string]any) {
				if kind == "attempt_close_stalled" {
					alerts <- fields["query_count"].(int32)
				}
			})
			first := NewQueryWorker(mem, qRouter{gw: gw}, nil, alerter, QueryConfig{WorkerID: "worker", BatchSize: 1}, clock)
			second := first
			if !sameWorker {
				second = NewQueryWorker(mem, qRouter{gw: gw}, nil, alerter, QueryConfig{WorkerID: "other", BatchSize: 1}, clock)
			}
			done := make(chan error, 1)
			go func() { _, err := first.RunOnce(t.Context()); done <- err }()
			<-entered
			oldToken := <-tokens
			seconds.Store(31)
			if n, err := second.RunOnce(t.Context()); err != nil || n != 1 {
				t.Fatalf("takeover: n=%d err=%v", n, err)
			}
			newToken := <-tokens
			if oldToken == "" || newToken == "" || oldToken == newToken {
				t.Fatalf("lease generation reused: old=%q new=%q", oldToken, newToken)
			}
			releaseFirst()
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			if len(alerts) != 1 {
				t.Fatalf("stale claimant emitted alert: count=%d", len(alerts))
			}
			if got := <-alerts; got != 6 {
				t.Fatalf("committed query count=%d", got)
			}
			a := mem.Attempts("alert-lease")[0]
			if a.QueryCount != 6 || a.LockedBy != "" || !a.NextQueryAt.Valid {
				t.Fatalf("stale claimant changed scheduling: %+v", a)
			}
		})
	}
}

type stalledRollbackStore struct{ store.Store }

func (s stalledRollbackStore) WithinTx(ctx context.Context, fn func(context.Context, store.Repos) error) error {
	return s.Store.WithinTx(ctx, func(ctx context.Context, r store.Repos) error {
		if err := fn(ctx, r); err != nil {
			return err
		}
		return errors.New("injected reschedule rollback")
	})
}

func TestQueryStalledAlertRequiresCommittedCountAdvance(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		stored, snapshot     int32
		threshold, wantCount int32
		rollback, wantAlert  bool
	}{
		{name: "rollback", stored: 5, snapshot: 5, threshold: 6, wantCount: 5, rollback: true},
		{name: "stored_at_threshold", stored: 5, snapshot: 4, threshold: 6, wantCount: 6, wantAlert: true},
		{name: "stored_past_threshold", stored: 6, snapshot: 5, threshold: 6, wantCount: 7},
		{name: "saturated_count", stored: math.MaxInt32, snapshot: math.MaxInt32 - 1, threshold: math.MaxInt32, wantCount: math.MaxInt32},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mem := memstore.New()
			seed(t, mem, "alert-commit", models.OrderClosing, models.CloseUserCancel, models.AttemptClosing, tc.stored)
			var a *models.PaymentAttempt
			if err := mem.WithinTx(t.Context(), func(ctx context.Context, r store.Repos) error {
				rows, err := r.Attempts().ClaimQueryDue(ctx, "claim", qClock(), qClock().Add(time.Minute), 1)
				if err == nil && len(rows) == 1 {
					a = rows[0]
				}
				return err
			}); err != nil || a == nil {
				t.Fatalf("claim: %v", err)
			}
			a.QueryCount = tc.snapshot
			var st store.Store = mem
			if tc.rollback {
				st = stalledRollbackStore{mem}
			}
			alerts := 0
			alerter := stalledAlertFunc(func(_ context.Context, kind string, fields map[string]any) {
				if kind != "attempt_close_stalled" {
					return
				}
				alerts++
				current := mem.Attempts("alert-commit")[0]
				if fields["query_count"] != current.QueryCount || current.LockedBy != "" {
					t.Errorf("alert preceded committed reschedule: fields=%v current=%+v", fields, current)
				}
			})
			gw := qGateway{res: paygateway.QueryResult{State: paygateway.QueryNotPay}}
			w := NewQueryWorker(st, qRouter{gw: gw}, nil, alerter, QueryConfig{MaxQueries: tc.threshold}, qClock)
			w.handle(t.Context(), a)
			if (alerts == 1) != tc.wantAlert || alerts > 1 {
				t.Fatalf("alerts=%d wantAlert=%v", alerts, tc.wantAlert)
			}
			if current := mem.Attempts("alert-commit")[0]; current.QueryCount != tc.wantCount {
				t.Fatalf("query count=%d want=%d", current.QueryCount, tc.wantCount)
			}
		})
	}
}
