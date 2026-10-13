package application

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/gtkit/orderd/internal/models"
	"github.com/gtkit/orderd/internal/shared/alert"
	"github.com/gtkit/orderd/internal/shared/bill"
	"github.com/gtkit/orderd/internal/store"
	"github.com/gtkit/orderd/internal/store/memstore"
)

type fetchFunc func(context.Context, string, string, time.Time) (bill.Bill, error)

func (f fetchFunc) Fetch(ctx context.Context, channel, account string, date time.Time, emit func(bill.Entry) error) (bill.Summary, error) {
	b, err := f(ctx, channel, account, date)
	if err != nil {
		return bill.Summary{}, err
	}
	return emitFixture(b, emit)
}

func recoveryService(st store.Store, source bill.Source, clock func() time.Time) *Service {
	var seq atomic.Int64
	return New(Deps{Store: st, Bill: source, Clock: clock, Alerter: alert.Nop{}, NewBatchNo: func() string { return fmt.Sprintf("recovery-%d", seq.Add(1)) }})
}

func recoveryAccount(m *memstore.Mem, created time.Time) models.AppPayAccount {
	a := models.AppPayAccount{AppID: "app", Channel: "wechat", PayAccountID: "acc", Enabled: true, CreatedAt: created}
	m.SeedAccount(a)
	return a
}

func TestRecoverAccountCalendarAndExecutionBounded(t *testing.T) {
	m := memstore.New()
	now := time.Date(2026, 10, 12, 0, 0, 0, 0, bill.Location)
	start := now.AddDate(-2, 0, 0)
	a := recoveryAccount(m, start)
	var calls int
	source := fetchFunc(func(context.Context, string, string, time.Time) (bill.Bill, error) { calls++; return bill.Bill{}, nil })
	svc := recoveryService(m, source, func() time.Time { return now })
	through := now.AddDate(0, 0, -1)
	n, err := svc.RecoverAccount(t.Context(), a, through)
	if err != nil || n != 8 || calls != 8 {
		t.Fatalf("execution bound: n=%d calls=%d err=%v", n, calls, err)
	}
	stored, err := m.Reader().Reconcile().GetScanAccount(t.Context(), a.PayAccountID, false)
	if err != nil || !stored.ReconcileScannedThrough.Time.Equal(start.AddDate(0, 0, 29)) {
		t.Fatalf("cursor: %+v err=%v", stored, err)
	}
	for i := range 30 {
		b, err := m.Reader().Reconcile().GetBatchByAccountDate(t.Context(), a.Channel, a.PayAccountID, start.AddDate(0, 0, i))
		if err != nil || b == nil {
			t.Fatalf("missing seeded date %d: %v", i, err)
		}
	}
	if b, err := m.Reader().Reconcile().GetBatchByAccountDate(t.Context(), a.Channel, a.PayAccountID, start.AddDate(0, 0, 30)); err != nil || b != nil {
		t.Fatalf("scanned beyond fixed page: %+v %v", b, err)
	}
	if b, err := m.Reader().Reconcile().GetBatchByAccountDate(t.Context(), a.Channel, a.PayAccountID, through); err != nil || b == nil || b.Status != models.BatchDone {
		t.Fatalf("yesterday must be included: %+v %v", b, err)
	}
	// 重新构造服务只换进程内对象,游标仍从数据库续跑;批次号生成继续用已有唯一生成器。
	restarted := New(svc.d)
	if _, err := restarted.RecoverAccount(t.Context(), a, through); err != nil {
		t.Fatal(err)
	}
	stored, _ = m.Reader().Reconcile().GetScanAccount(t.Context(), a.PayAccountID, false)
	if !stored.ReconcileScannedThrough.Time.Equal(start.AddDate(0, 0, 59)) {
		t.Fatalf("restart lost cursor: %+v", stored)
	}
}

func TestRecoverAccountConcurrentCalendar(t *testing.T) {
	m := memstore.New()
	now := time.Date(2026, 10, 12, 0, 0, 0, 0, bill.Location)
	start := now.AddDate(-1, 0, 0)
	a := recoveryAccount(m, start)
	svc := recoveryService(m, fakeBill{}, func() time.Time { return now })
	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() {
			if _, err := svc.RecoverAccount(t.Context(), a, now.AddDate(0, 0, -1)); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	stored, err := m.Reader().Reconcile().GetScanAccount(t.Context(), a.PayAccountID, false)
	if err != nil || !stored.ReconcileScannedThrough.Time.Equal(start.AddDate(0, 0, 59)) {
		t.Fatalf("two serialized pages: %+v %v", stored, err)
	}
}

type recoveryFaultStore struct {
	store.Store
	createAt    int
	createCalls int
	failDiff    bool
}
type recoveryFaultRepos struct {
	store.Repos
	owner *recoveryFaultStore
}
type recoveryFaultReconcile struct {
	store.ReconcileRepo
	owner *recoveryFaultStore
}

var errRecoveryInjected = errors.New("recovery injected failure")

func (s *recoveryFaultStore) WithinTx(ctx context.Context, fn func(context.Context, store.Repos) error) error {
	return s.Store.WithinTx(ctx, func(ctx context.Context, r store.Repos) error { return fn(ctx, recoveryFaultRepos{r, s}) })
}
func (r recoveryFaultRepos) Reconcile() store.ReconcileRepo {
	return recoveryFaultReconcile{r.Repos.Reconcile(), r.owner}
}
func (r recoveryFaultReconcile) CreateBatch(ctx context.Context, b *models.ReconcileBatch) error {
	r.owner.createCalls++
	if r.owner.createCalls == r.owner.createAt {
		return errRecoveryInjected
	}
	return r.ReconcileRepo.CreateBatch(ctx, b)
}
func (r recoveryFaultReconcile) SaveRun(ctx context.Context, b *models.ReconcileBatch, token, run string, now time.Time, entries []models.ReconcileEntry, diffs []models.ReconcileDiff, publish bool) (bool, error) {
	if r.owner.failDiff && len(diffs) > 0 {
		return false, errRecoveryInjected
	}
	return r.ReconcileRepo.SaveRun(ctx, b, token, run, now, entries, diffs, publish)
}

func TestRecoveryCalendarRollbackAndBatchNumberCollision(t *testing.T) {
	for _, mode := range []string{"insert_failure", "batch_number_collision"} {
		t.Run(mode, func(t *testing.T) {
			m := memstore.New()
			now := time.Date(2026, 10, 12, 0, 0, 0, 0, bill.Location)
			a := recoveryAccount(m, now.AddDate(-1, 0, 0))
			fault := &recoveryFaultStore{Store: m, createAt: 3}
			svc := recoveryService(fault, fakeBill{}, func() time.Time { return now })
			want := errRecoveryInjected
			if mode == "batch_number_collision" {
				fault.createAt = 0
				if err := m.Reader().Reconcile().CreateBatch(t.Context(), &models.ReconcileBatch{BatchNo: "collision", Channel: "other", PayAccountID: "other", BillDate: now}); err != nil {
					t.Fatal(err)
				}
				svc.d.NewBatchNo = func() string { return "collision" }
				want = store.ErrDuplicate
			}
			if _, err := svc.RecoverAccount(t.Context(), a, now.AddDate(0, 0, -1)); !errors.Is(err, want) {
				t.Fatalf("want injected failure, got %v", err)
			}
			stored, _ := m.Reader().Reconcile().GetScanAccount(t.Context(), a.PayAccountID, false)
			if stored.ReconcileScannedThrough.Valid {
				t.Fatalf("cursor advanced after rollback: %+v", stored)
			}
			if b, _ := m.Reader().Reconcile().GetBatchByAccountDate(t.Context(), a.Channel, a.PayAccountID, now.AddDate(0, 0, -1)); b != nil {
				t.Fatalf("partial batch escaped rollback: %+v", b)
			}
		})
	}
}

func TestRecoveryFailedOldPeriodAndRetryFairness(t *testing.T) {
	m := memstore.New()
	now := time.Date(2026, 10, 12, 0, 0, 0, 0, bill.Location)
	a := recoveryAccount(m, now.AddDate(0, 0, -3))
	calls := map[string]int{}
	fail := true
	svc := recoveryService(m, fetchFunc(func(_ context.Context, _, _ string, day time.Time) (bill.Bill, error) {
		calls[day.Format("2006-01-02")]++
		if fail {
			return bill.Bill{}, errRecoveryInjected
		}
		return bill.Bill{}, nil
	}), func() time.Time { return now })
	if n, err := svc.RecoverAccount(t.Context(), a, now.AddDate(0, 0, -1)); n != 3 || !errors.Is(err, errRecoveryInjected) {
		t.Fatalf("failed batches monopolized slots: n=%d err=%v", n, err)
	}
	for date, n := range calls {
		if n != 1 {
			t.Fatalf("same failed date retried immediately: %s=%d", date, n)
		}
	}
	oldDate := now.AddDate(0, 0, -3)
	now = now.AddDate(0, 0, 1)
	fail = false
	if n, err := svc.RecoverAccount(t.Context(), a, now.AddDate(0, 0, -1)); n != 4 || err != nil {
		t.Fatalf("cross-day recovery: n=%d err=%v", n, err)
	}
	b, _ := m.Reader().Reconcile().GetBatchByAccountDate(t.Context(), a.Channel, a.PayAccountID, oldDate)
	if b.Status != models.BatchDone {
		t.Fatalf("old failed period not recovered: %+v", b)
	}
}

func TestRecoveryStaleClaimCannotWriteCompletionOrFailure(t *testing.T) {
	for _, staleFailure := range []bool{false, true} {
		t.Run(fmt.Sprint(staleFailure), func(t *testing.T) {
			m := memstore.New()
			base := time.Date(2026, 10, 12, 0, 0, 0, 0, bill.Location)
			var seconds atomic.Int64
			clock := func() time.Time { return base.Add(time.Duration(seconds.Load()) * time.Second) }
			entered, release := make(chan struct{}), make(chan struct{})
			slow := recoveryService(m, fetchFunc(func(context.Context, string, string, time.Time) (bill.Bill, error) {
				close(entered)
				<-release
				if staleFailure {
					return bill.Bill{}, errRecoveryInjected
				}
				return bill.Bill{Entries: []bill.Entry{{TransactionID: "stale", Amount: 100}}}, nil
			}), clock)
			done := make(chan error, 1)
			go func() {
				_, err := slow.Reconcile(t.Context(), "app", "wechat", "acc", base.AddDate(0, 0, -1))
				done <- err
			}()
			<-entered
			seconds.Store(61)
			fast := recoveryService(m, fakeBill{}, clock)
			if _, err := fast.Reconcile(t.Context(), "app", "wechat", "acc", base.AddDate(0, 0, -1)); err != nil {
				t.Fatal(err)
			}
			close(release)
			if err := <-done; staleFailure && !errors.Is(err, errRecoveryInjected) || !staleFailure && err != nil {
				t.Fatalf("unexpected stale result: %v", err)
			}
			b, _ := m.Reader().Reconcile().GetBatchByAccountDate(t.Context(), "wechat", "acc", base.AddDate(0, 0, -1))
			if b.Status != models.BatchDone || b.DiffCount != 0 || len(m.Diffs()) != 0 {
				t.Fatalf("stale claimant wrote state/diffs: %+v diffs=%v", b, m.Diffs())
			}
		})
	}
}

func TestRecoveryDifferenceFailureRollsBackCompletion(t *testing.T) {
	m := memstore.New()
	now := time.Date(2026, 10, 12, 0, 0, 0, 0, bill.Location)
	fault := &recoveryFaultStore{Store: m, failDiff: true}
	svc := recoveryService(fault, fakeBill{b: bill.Bill{Entries: []bill.Entry{{TransactionID: "tx", Amount: 100}}}}, func() time.Time { return now })
	day := now.AddDate(0, 0, -1)
	if _, err := svc.Reconcile(t.Context(), "app", "wechat", "acc", day); !errors.Is(err, errRecoveryInjected) {
		t.Fatal(err)
	}
	b, _ := m.Reader().Reconcile().GetBatchByAccountDate(t.Context(), "wechat", "acc", day)
	if b.Status == models.BatchDone || b.LockedBy == "" || len(m.Diffs()) != 0 {
		t.Fatalf("completion escaped rollback: %+v", b)
	}
	fault.failDiff = false
	now = now.Add(time.Minute + time.Second)
	if res, err := svc.Reconcile(t.Context(), "app", "wechat", "acc", day); err != nil || res.DiffCount != 1 {
		t.Fatalf("retry failed: %+v %v", res, err)
	}
}

func TestRecoveryFetchDeadlineAndCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m := memstore.New()
		start := time.Now()
		svc := recoveryService(m, fetchFunc(func(ctx context.Context, _, _ string, _ time.Time) (bill.Bill, error) {
			<-ctx.Done()
			return bill.Bill{}, ctx.Err()
		}), time.Now)
		if _, err := svc.Reconcile(t.Context(), "app", "wechat", "acc", start.AddDate(0, 0, -1)); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("deadline not propagated: %v", err)
		}
		if elapsed := time.Since(start); elapsed != 30*time.Second {
			t.Fatalf("fetch budget: %v", elapsed)
		}
	})
	m := memstore.New()
	now := time.Date(2026, 10, 12, 0, 0, 0, 0, bill.Location)
	a := recoveryAccount(m, now.AddDate(-1, 0, 0))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	calls := 0
	svc := recoveryService(m, fetchFunc(func(ctx context.Context, _, _ string, _ time.Time) (bill.Bill, error) {
		calls++
		cancel()
		<-ctx.Done()
		return bill.Bill{}, ctx.Err()
	}), func() time.Time { return now })
	if _, err := svc.RecoverAccount(ctx, a, now.AddDate(0, 0, -1)); !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatalf("cancel did not stop recovery: calls=%d err=%v", calls, err)
	}
}
