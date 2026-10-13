package worker

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/gtkit/orderd/internal/models"
	"github.com/gtkit/orderd/internal/store/memstore"
)

type deliveryEndpointFunc func(context.Context, string) (models.AppEndpoint, error)

func (f deliveryEndpointFunc) Resolve(ctx context.Context, appID string) (models.AppEndpoint, error) {
	return f(ctx, appID)
}

var errDeliveryInjected = errors.New("injected delivery failure")

func TestDeliveryRecoveryClaimsJustBeforeSend(t *testing.T) {
	for _, cancelFirst := range []bool{false, true} {
		t.Run(fmt.Sprint(cancelFirst), func(t *testing.T) {
			m := memstore.New()
			now := time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)
			for i := range 3 {
				if err := m.Reader().Events().InsertOutbox(t.Context(), &models.EventOutbox{EventID: fmt.Sprintf("just-in-time-%d", i), Status: models.OutboxPending, NextAttemptAt: now}); err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			calls := 0
			tokens := map[string]bool{}
			endpoints := deliveryEndpointFunc(func(context.Context, string) (models.AppEndpoint, error) {
				rows := m.Outbox()
				claimed := 0
				for _, row := range rows {
					if row.LockedBy == "" {
						continue
					}
					claimed++
					if !row.LockedUntil.Time.Equal(now.Add(30*time.Second)) || tokens[row.LockedBy] {
						t.Errorf("lease not fresh: %+v", row)
					}
					tokens[row.LockedBy] = true
				}
				if claimed != 1 {
					t.Errorf("claimed queued work before execution: count=%d", claimed)
				}
				calls++
				now = now.Add(20 * time.Second)
				if cancelFirst {
					cancel()
				}
				return models.AppEndpoint{}, errDeliveryInjected
			})
			d := NewDelivery(m, endpoints, nil, nil, &recAlerter{}, nil,
				Config{WorkerID: "same-worker", BatchSize: 2, BaseBackoff: time.Hour, MaxBackoff: time.Hour}, func() time.Time { return now })
			_, _, err := d.RunOnce(ctx)
			want := 2
			if cancelFirst {
				want = 1
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("cancel lost: %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if calls != want {
				t.Fatalf("round bound: got=%d want=%d", calls, want)
			}
		})
	}
}

func TestDeliveryRecoveryStaleFailureDoesNotAlert(t *testing.T) {
	for _, requeued := range []bool{false, true} {
		t.Run(fmt.Sprint(requeued), func(t *testing.T) {
			m := memstore.New()
			now := time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)
			ob := &models.EventOutbox{EventID: "stale-dead", Status: models.OutboxPending, Attempts: 8, NextAttemptAt: now}
			if err := m.Reader().Events().InsertOutbox(t.Context(), ob); err != nil {
				t.Fatal(err)
			}
			alerter := &recAlerter{}
			observer := &recObserver{}
			endpoints := deliveryEndpointFunc(func(ctx context.Context, _ string) (models.AppEndpoint, error) {
				cur, err := m.Reader().Outbox().GetByEventID(ctx, ob.EventID, false)
				if err != nil {
					t.Fatal(err)
				}
				if requeued {
					if applied, err := m.Reader().Outbox().MarkDead(ctx, ob.ID, cur.LockedBy, 9, "new failure"); err != nil || !applied {
						t.Fatalf("mark: %v %v", applied, err)
					}
					if err := m.Reader().Outbox().Requeue(ctx, ob.ID, now); err != nil {
						t.Fatal(err)
					}
				}
				later := now.Add(time.Minute)
				if batch, err := m.Reader().Outbox().ClaimDue(ctx, "replacement-token", later, later.Add(time.Minute), 1); err != nil || len(batch) != 1 {
					t.Fatalf("replacement claim: %+v %v", batch, err)
				}
				return models.AppEndpoint{}, errDeliveryInjected
			})
			d := NewDelivery(m, endpoints, nil, nil, alerter, observer, Config{BatchSize: 1}, func() time.Time { return now })
			if _, _, err := d.RunOnce(t.Context()); err != nil {
				t.Fatal(err)
			}
			cur, _ := m.Reader().Outbox().GetByEventID(t.Context(), ob.EventID, false)
			if cur.Status != models.OutboxPending || cur.LockedBy != "replacement-token" {
				t.Fatalf("stale failure changed replacement: %+v", cur)
			}
			if alerter.has("outbox_dead_letter") || len(observer.outcomes) != 0 {
				t.Fatalf("stale failure emitted false result: alert=%v outcomes=%v", alerter.kinds, observer.outcomes)
			}
		})
	}
}
