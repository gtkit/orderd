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

type repairFunc func(context.Context, bill.RepairRequest) (bool, error)

func (f repairFunc) VerifyAndConfirmFromBill(ctx context.Context, in bill.RepairRequest) (bool, error) {
	return f(ctx, in)
}
func prepareRepair(t *testing.T, m *memstore.Mem, now *time.Time) *Service {
	t.Helper()
	streamAccount(m, *now)
	if err := m.WithinTx(t.Context(), func(ctx context.Context, r store.Repos) error {
		return r.Attempts().Insert(ctx, &models.PaymentAttempt{AppID: "app", OrderNo: "order", Channel: "wechat", PayAccountID: "acc", OutTradeNo: streamEntry(0).OutTradeNo, Status: models.AttemptClosed})
	}); err != nil {
		t.Fatal(err)
	}
	calls := 0
	svc := recoveryService(m, streamBill(1, &calls), func() time.Time { return *now })
	if _, err := svc.Reconcile(t.Context(), "app", "wechat", "acc", now.AddDate(0, 0, -1)); err != nil {
		t.Fatal(err)
	}
	return svc
}
func TestRepairDisabledAndPersistentRetryAfterBatchDone(t *testing.T) {
	m := memstore.New()
	now := time.Date(2026, 10, 12, 12, 0, 0, 0, bill.Location)
	svc := prepareRepair(t, m, &now)
	calls := 0
	svc.d.Repair = repairFunc(func(_ context.Context, in bill.RepairRequest) (bool, error) {
		calls++
		if in.AppID != "app" || in.Entry.TransactionID != streamEntry(0).TransactionID {
			t.Fatalf("repair identity: %+v", in)
		}
		if calls == 1 {
			return false, errors.New("transient query failure")
		}
		return true, nil
	})
	if err := svc.repair(t.Context(), "wechat", "acc"); err != nil || calls != 0 {
		t.Fatalf("disabled repair made call: %d %v", calls, err)
	}
	svc.d.AutoConfirm = true
	if err := svc.repair(t.Context(), "wechat", "acc"); err != nil {
		t.Fatal(err)
	}
	d := m.Diffs()[0]
	if calls != 1 || d.VerifyAttempts != 1 || !d.NextVerifyAt.Valid || d.HandleStatus != models.DiffHandlePending {
		t.Fatalf("failure not durably rescheduled: %+v", d)
	}
	if err := svc.repair(t.Context(), "wechat", "acc"); err != nil || calls != 1 {
		t.Fatalf("retry cooldown ignored: %d %v", calls, err)
	}
	now = now.Add(retryDelay)
	if err := New(svc.d).repair(t.Context(), "wechat", "acc"); err != nil {
		t.Fatal(err)
	}
	d = m.Diffs()[0]
	if calls != 2 || d.VerifyAttempts != 2 || d.HandleStatus != models.DiffHandleResolved || len(m.Audits()) != 1 {
		t.Fatalf("done batch lost repair retry: %+v calls=%d", d, calls)
	}
}
func TestRepairManualRaceAndOldLeaseCannotWrite(t *testing.T) {
	for _, mode := range []string{"manual", "takeover", "same_token_expired"} {
		t.Run(mode, func(t *testing.T) {
			m := memstore.New()
			now := time.Date(2026, 10, 12, 12, 0, 0, 0, bill.Location)
			svc := prepareRepair(t, m, &now)
			svc.d.AutoConfirm = true
			id := m.Diffs()[0].ID
			svc.d.Repair = repairFunc(func(ctx context.Context, _ bill.RepairRequest) (bool, error) {
				if mode == "manual" {
					return true, m.WithinTx(ctx, func(ctx context.Context, r store.Repos) error {
						return r.Reconcile().UpdateDiffHandling(ctx, id, models.DiffHandleProcessing, "operator", now, "manual note")
					})
				}
				now = now.Add(leaseTTL)
				if mode == "takeover" {
					if err := m.WithinTx(ctx, func(ctx context.Context, r store.Repos) error {
						d, err := r.Reconcile().ClaimRepair(ctx, "wechat", "acc", "new-generation", now, now.Add(leaseTTL))
						if err == nil && d == nil {
							t.Fatal("takeover failed")
						}
						return err
					}); err != nil {
						return false, err
					}
				}
				return true, nil
			})
			if err := svc.repair(t.Context(), "wechat", "acc"); err != nil {
				t.Fatal(err)
			}
			d := m.Diffs()[0]
			if d.HandleStatus == models.DiffHandleResolved || d.VerifyAttempts != 0 || len(m.Audits()) != 0 {
				t.Fatalf("old result overwrote current state: %+v", d)
			}
			if mode == "manual" && (d.HandledBy != "operator" || d.HandleRemark != "manual note") {
				t.Fatalf("manual note lost: %+v", d)
			}
		})
	}
}

func TestLocalOnlyDifferenceNeverRepairsMoney(t *testing.T) {
	m := memstore.New()
	now := time.Date(2026, 10, 12, 12, 0, 0, 0, bill.Location)
	streamAccount(m, now)
	streamTxn(t, m, streamEntry(0), now)
	svc := recoveryService(m, fakeBill{}, func() time.Time { return now })
	if err := svc.verifyLocal(t.Context(), "wechat", "acc"); err != nil {
		t.Fatal(err)
	}
	svc.d.AutoConfirm = true
	svc.d.Repair = repairFunc(func(context.Context, bill.RepairRequest) (bool, error) {
		t.Fatal("local-only diff attempted money confirmation")
		return false, nil
	})
	if err := svc.repair(t.Context(), "wechat", "acc"); err != nil {
		t.Fatal(err)
	}
}

type finishFaultStore struct {
	store.Store
	fail bool
}
type finishFaultRepos struct {
	store.Repos
	owner *finishFaultStore
}
type finishFaultRepo struct {
	store.ReconcileRepo
	owner *finishFaultStore
}

func (s *finishFaultStore) WithinTx(ctx context.Context, fn func(context.Context, store.Repos) error) error {
	return s.Store.WithinTx(ctx, func(ctx context.Context, r store.Repos) error { return fn(ctx, finishFaultRepos{r, s}) })
}
func (r finishFaultRepos) Reconcile() store.ReconcileRepo {
	return finishFaultRepo{r.Repos.Reconcile(), r.owner}
}
func (r finishFaultRepo) FinishRepair(ctx context.Context, id uint64, token string, now, next time.Time, success bool, reason string) (bool, error) {
	if r.owner.fail {
		r.owner.fail = false
		return false, errRecoveryInjected
	}
	return r.ReconcileRepo.FinishRepair(ctx, id, token, now, next, success, reason)
}
func TestRepairConfirmationCommitBeforeResultFailureIsReplayable(t *testing.T) {
	m := memstore.New()
	now := time.Date(2026, 10, 12, 12, 0, 0, 0, bill.Location)
	svc := prepareRepair(t, m, &now)
	fault := &finishFaultStore{Store: m, fail: true}
	svc.d.Store = fault
	svc.d.AutoConfirm = true
	calls := 0
	svc.d.Repair = repairFunc(func(ctx context.Context, in bill.RepairRequest) (bool, error) {
		calls++
		err := m.WithinTx(ctx, func(ctx context.Context, r store.Repos) error {
			return r.Transactions().Insert(ctx, &models.PaymentTransaction{AppID: "app", OrderNo: "order", Channel: "wechat", PayAccountID: "acc", Kind: models.TxnPay, TransactionID: in.Entry.TransactionID, OutTradeNo: in.Entry.OutTradeNo, Amount: in.Entry.Amount, Currency: in.Entry.Currency, CreatedAt: now})
		})
		return err == nil || errors.Is(err, store.ErrDuplicate), nil
	})
	if err := svc.repair(t.Context(), "wechat", "acc"); !errors.Is(err, errRecoveryInjected) {
		t.Fatalf("injected result failure: %v", err)
	}
	if len(m.Txns()) != 1 || m.Diffs()[0].HandleStatus != models.DiffHandlePending {
		t.Fatal("confirmation/result crash window not modeled")
	}
	now = now.Add(leaseTTL)
	if err := svc.repair(t.Context(), "wechat", "acc"); err != nil {
		t.Fatal(err)
	}
	if calls != 2 || len(m.Txns()) != 1 || len(m.Audits()) != 1 || m.Diffs()[0].HandleStatus != models.DiffHandleResolved {
		t.Fatalf("repair replay duplicated funds or lost audit: calls=%d txns=%v diffs=%v", calls, m.Txns(), m.Diffs())
	}
}
