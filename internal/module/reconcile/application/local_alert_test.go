package application

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gtkit/orderd/internal/models"
	"github.com/gtkit/orderd/internal/shared/bill"
	"github.com/gtkit/orderd/internal/store"
	"github.com/gtkit/orderd/internal/store/memstore"
)

type localAlerts struct {
	keys     []string
	txActive *bool
}

func (a *localAlerts) Anomaly(_ context.Context, key string, _ map[string]any) {
	if a.txActive != nil && *a.txActive {
		panic("external alert during transaction")
	}
	a.keys = append(a.keys, key)
}
func TestLocalOnlyAlertsAfterCommitAndNoRepeat(t *testing.T) {
	for _, hasBill := range []bool{false, true} {
		t.Run(map[bool]string{false: "status_without_statement", true: "short_with_complete_statement"}[hasBill], func(t *testing.T) {
			m := memstore.New()
			now := time.Date(2026, 10, 12, 12, 0, 0, 0, bill.Location)
			streamAccount(m, now)
			streamTxn(t, m, streamEntry(0), now)
			alerts := &localAlerts{}
			svc := recoveryService(m, fakeBill{}, func() time.Time { return now })
			svc.d.Alerter = alerts
			if hasBill {
				if _, err := svc.Reconcile(t.Context(), "app", "wechat", "acc", now.AddDate(-1, 0, 0)); err != nil {
					t.Fatal(err)
				}
			} else if err := svc.verifyLocal(t.Context(), "wechat", "acc"); err != nil {
				t.Fatal(err)
			}
			want := models.DiffStatus
			if hasBill {
				want = models.DiffShort
			}
			if len(alerts.keys) != 1 || alerts.keys[0] != "reconcile_local_diff" || len(m.Diffs()) != 1 || m.Diffs()[0].DiffType != want {
				t.Fatalf("local-only alert missing: %+v %v", m.Diffs(), alerts.keys)
			}
			now = now.Add(retryDelay)
			if err := svc.verifyLocal(t.Context(), "wechat", "acc"); err != nil {
				t.Fatal(err)
			}
			if len(alerts.keys) != 1 {
				t.Fatalf("identical retry spammed: %v", alerts.keys)
			}
		})
	}
}

type localAlertFault struct {
	store.Store
	stale, rollback, active bool
}
type localAlertRepos struct {
	store.Repos
	owner *localAlertFault
}
type localAlertRepo struct {
	store.ReconcileRepo
	owner *localAlertFault
}

func (s *localAlertFault) WithinTx(ctx context.Context, fn func(context.Context, store.Repos) error) error {
	return s.Store.WithinTx(ctx, func(ctx context.Context, r store.Repos) error {
		s.active = true
		defer func() { s.active = false }()
		return fn(ctx, localAlertRepos{r, s})
	})
}
func (r localAlertRepos) Reconcile() store.ReconcileRepo {
	return localAlertRepo{r.Repos.Reconcile(), r.owner}
}
func (r localAlertRepo) FinishLocalTxn(ctx context.Context, id uint64, token string, now, next time.Time, reason string) (bool, error) {
	if r.owner.stale {
		return false, nil
	}
	return r.ReconcileRepo.FinishLocalTxn(ctx, id, token, now, next, reason)
}
func (r localAlertRepo) SetLocalDiff(ctx context.Context, id uint64, d *models.ReconcileDiff, now time.Time) (bool, error) {
	changed, err := r.ReconcileRepo.SetLocalDiff(ctx, id, d, now)
	if err == nil && r.owner.rollback {
		return changed, errRecoveryInjected
	}
	return changed, err
}
func TestLocalAlertRejectsStaleCASAndRolledBackDifference(t *testing.T) {
	for _, mode := range []string{"stale", "rollback", "commit"} {
		t.Run(mode, func(t *testing.T) {
			m := memstore.New()
			now := time.Date(2026, 10, 12, 12, 0, 0, 0, bill.Location)
			streamAccount(m, now)
			streamTxn(t, m, streamEntry(0), now)
			fault := &localAlertFault{Store: m, stale: mode == "stale", rollback: mode == "rollback"}
			alerts := &localAlerts{txActive: &fault.active}
			svc := recoveryService(fault, fakeBill{}, func() time.Time { return now })
			svc.d.Alerter = alerts
			err := svc.verifyLocal(t.Context(), "wechat", "acc")
			if mode == "rollback" && !errors.Is(err, errRecoveryInjected) || mode != "rollback" && err != nil {
				t.Fatal(err)
			}
			if mode == "commit" {
				if len(alerts.keys) != 1 {
					t.Fatal("committed new difference not alerted")
				}
			} else if len(alerts.keys) != 0 || len(m.Diffs()) != 0 {
				t.Fatalf("uncommitted difference alerted: %v %+v", alerts.keys, m.Diffs())
			}
		})
	}
}
func TestLocalStatusUpgradesToShortAndAlertsOnce(t *testing.T) {
	m := memstore.New()
	now := time.Date(2026, 10, 12, 12, 0, 0, 0, bill.Location)
	streamAccount(m, now)
	streamTxn(t, m, streamEntry(0), now)
	alerts := &localAlerts{}
	svc := recoveryService(m, fakeBill{}, func() time.Time { return now })
	svc.d.Alerter = alerts
	if err := svc.verifyLocal(t.Context(), "wechat", "acc"); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Reconcile(t.Context(), "app", "wechat", "acc", now.AddDate(-1, 0, 0)); err != nil {
		t.Fatal(err)
	}
	now = now.Add(retryDelay)
	if err := svc.verifyLocal(t.Context(), "wechat", "acc"); err != nil {
		t.Fatal(err)
	}
	if len(alerts.keys) != 2 || len(m.Diffs()) != 1 || m.Diffs()[0].DiffType != models.DiffShort {
		t.Fatalf("upgrade missing/duplicated: alerts=%v diffs=%+v", alerts.keys, m.Diffs())
	}
}
