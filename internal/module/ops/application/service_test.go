package application

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/gtkit/orderd/internal/models"
	"github.com/gtkit/orderd/internal/store"
	"github.com/gtkit/orderd/internal/store/memstore"
)

var bg = context.Background()

func opsClock() time.Time { return time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC) }

func newSvc(mem *memstore.Mem) *Service {
	return New(Deps{Store: mem, Clock: opsClock})
}

var testActor = Actor{AppID: "ops", UserID: 7}

// seedDiff 播种一条差异,返回 id。
func seedDiff(t *testing.T, mem *memstore.Mem, hs models.DiffHandleStatus, remark string) uint64 {
	t.Helper()
	d := &models.ReconcileDiff{
		BatchID: 1, AppID: "app1", Channel: "wechat", PayAccountID: "acc-w",
		DiffType: models.DiffShort, TransactionID: fmt.Sprintf("tx-%d", time.Now().UnixNano()),
		LocalAmount: 90, HandleStatus: hs, HandleRemark: remark,
	}
	err := mem.WithinTx(bg, func(ctx context.Context, r store.Repos) error {
		return r.Reconcile().InsertDiff(ctx, d)
	})
	if err != nil {
		t.Fatalf("播种差异失败: %v", err)
	}
	if hs != models.DiffHandlePending { // InsertDiff 不落处置态,补写
		err = mem.WithinTx(bg, func(ctx context.Context, r store.Repos) error {
			return r.Reconcile().UpdateDiffHandling(ctx, d.ID, hs, "seed", opsClock(), remark)
		})
		if err != nil {
			t.Fatalf("播种处置态失败: %v", err)
		}
	}
	return d.ID
}

// seedDead 播种一条出站记录,返回 event_id。
func seedOutbox(t *testing.T, mem *memstore.Mem, eventID string, st models.OutboxStatus, attempts int32) {
	t.Helper()
	err := mem.WithinTx(bg, func(ctx context.Context, r store.Repos) error {
		return r.Events().InsertOutbox(ctx, &models.EventOutbox{
			EventID: eventID, AppID: "app1", OrderNo: "OD1", EventType: models.EventTypePaid,
			Payload: []byte("p"), Status: st, Attempts: attempts, NextAttemptAt: opsClock(),
			LastError: sql.NullString{String: "boom", Valid: true},
		})
	})
	if err != nil {
		t.Fatalf("播种 outbox 失败: %v", err)
	}
}

// TestHandleDiffTransitions 处置状态机 table-driven:合法迁移 + 终态/同态/不存在/空备注忽略/备注超长。
func TestHandleDiffTransitions(t *testing.T) {
	long := make([]byte, 513)
	for i := range long {
		long[i] = 'x'
	}
	tests := []struct {
		name    string
		from    models.DiffHandleStatus
		target  models.DiffHandleStatus
		remark  string
		wantErr error // nil=成功
	}{
		{"待处理→处理中", models.DiffHandlePending, models.DiffHandleProcessing, "", nil},
		{"待处理→已解决", models.DiffHandlePending, models.DiffHandleResolved, "done", nil},
		{"待处理→已忽略", models.DiffHandlePending, models.DiffHandleIgnored, "小额忽略", nil},
		{"处理中→已解决", models.DiffHandleProcessing, models.DiffHandleResolved, "", nil},
		{"处理中→已忽略", models.DiffHandleProcessing, models.DiffHandleIgnored, "线下已处理", nil},
		{"终态已解决拒改", models.DiffHandleResolved, models.DiffHandleIgnored, "x", models.ErrIllegalTransition},
		{"终态已忽略拒改", models.DiffHandleIgnored, models.DiffHandleProcessing, "", models.ErrIllegalTransition},
		{"同态拒绝", models.DiffHandleProcessing, models.DiffHandleProcessing, "", models.ErrIllegalTransition},
		{"忽略无备注拒", models.DiffHandlePending, models.DiffHandleIgnored, "", ErrInvalidRequest},
		{"备注超长拒", models.DiffHandlePending, models.DiffHandleResolved, string(long), ErrInvalidRequest},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mem := memstore.New()
			seedRemark := ""
			if tc.from == models.DiffHandleIgnored {
				seedRemark = "seed-reason"
			}
			id := seedDiff(t, mem, tc.from, seedRemark)
			svc := newSvc(mem)
			audBefore := len(mem.Audits())

			got, err := svc.HandleReconcileDiff(bg, testActor, id, tc.target, tc.remark)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("期望错误 %v,实际 %v", tc.wantErr, err)
				}
				// 被拒:字段不变 + 不写审计。
				d, _ := mem.Reader().Reconcile().GetDiff(bg, id, false)
				if d.HandleStatus != tc.from {
					t.Fatalf("被拒后状态不得变化: %v", d.HandleStatus)
				}
				if len(mem.Audits()) != audBefore {
					t.Fatalf("被拒动作不得写审计")
				}
				return
			}
			if err != nil || got != tc.target {
				t.Fatalf("处置失败: got=%v err=%v", got, err)
			}
			d, _ := mem.Reader().Reconcile().GetDiff(bg, id, false)
			if d.HandleStatus != tc.target || d.HandledBy != "ops:7" || !d.HandledAt.Valid {
				t.Fatalf("处置字段不符(处置人须取自 token): %+v", d)
			}
			// 审计恰一条,detail 含前后状态。
			auds := mem.Audits()
			if len(auds) != audBefore+1 {
				t.Fatalf("成功动作应写恰一条审计,实际 %d", len(auds)-audBefore)
			}
			a := auds[len(auds)-1]
			if a.Actor != "ops:7" || a.Action != models.OpsActionReconcileDiffHandle ||
				a.TargetType != models.OpsTargetReconcileDiff || a.TargetID != fmt.Sprintf("%d", id) {
				t.Fatalf("审计内容不符: %+v", a)
			}
		})
	}
}

// TestHandleDiffNotFound 不存在的差异 → store.ErrNotFound。
func TestHandleDiffNotFound(t *testing.T) {
	svc := newSvc(memstore.New())
	_, err := svc.HandleReconcileDiff(bg, testActor, 999, models.DiffHandleResolved, "")
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("不存在应 ErrNotFound,实际: %v", err)
	}
}

// TestHandleDiffConcurrent 并发处置:恰一方成功,败方 FailedPrecondition 语义,备注不被覆盖。
func TestHandleDiffConcurrent(t *testing.T) {
	for range 20 {
		mem := memstore.New()
		id := seedDiff(t, mem, models.DiffHandlePending, "")
		svc := newSvc(mem)
		var wg sync.WaitGroup
		errs := make([]error, 2)
		wg.Go(func() {
			_, errs[0] = svc.HandleReconcileDiff(bg, testActor, id, models.DiffHandleResolved, "A说已解决")
		})
		wg.Go(func() {
			_, errs[1] = svc.HandleReconcileDiff(bg, Actor{AppID: "ops", UserID: 8}, id, models.DiffHandleIgnored, "B说忽略")
		})
		wg.Wait()
		ok := 0
		for _, e := range errs {
			if e == nil {
				ok++
			} else if !errors.Is(e, models.ErrIllegalTransition) {
				t.Fatalf("败方应为非法迁移错误,实际: %v", e)
			}
		}
		if ok != 1 {
			t.Fatalf("并发处置应恰一方成功,实际 %d", ok)
		}
		d, _ := mem.Reader().Reconcile().GetDiff(bg, id, false)
		if (d.HandleStatus == models.DiffHandleResolved && d.HandleRemark != "A说已解决") ||
			(d.HandleStatus == models.DiffHandleIgnored && d.HandleRemark != "B说忽略") {
			t.Fatalf("备注被覆盖: %+v", d)
		}
		if len(mem.Audits()) != 1 {
			t.Fatalf("应恰一条审计,实际 %d", len(mem.Audits()))
		}
	}
}

// TestRequeueDeadLetter 死信可重放且 attempts 不变;非死信拒;不存在 NotFound;审计恰一条。
func TestRequeueDeadLetter(t *testing.T) {
	mem := memstore.New()
	seedOutbox(t, mem, "evt-dead", models.OutboxDead, 8)
	seedOutbox(t, mem, "evt-pending", models.OutboxPending, 1)
	seedOutbox(t, mem, "evt-done", models.OutboxDelivered, 1)
	svc := newSvc(mem)

	attempts, err := svc.RequeueDeadLetter(bg, testActor, "evt-dead")
	if err != nil || attempts != 8 {
		t.Fatalf("死信重放失败: attempts=%d err=%v", attempts, err)
	}
	ob, _ := mem.Reader().Outbox().GetByEventID(bg, "evt-dead", false)
	if ob.Status != models.OutboxPending || ob.Attempts != 8 || ob.LastError.Valid || ob.LockedBy != "" {
		t.Fatalf("重放后应 Pending、attempts 不重置、错误与租约清空: %+v", ob)
	}
	if !ob.NextAttemptAt.After(opsClock().Add(-time.Second)) {
		t.Fatalf("next_attempt_at 应为当前时间: %v", ob.NextAttemptAt)
	}
	if len(mem.Audits()) != 1 || mem.Audits()[0].TargetID != "evt-dead" {
		t.Fatalf("应写恰一条审计: %+v", mem.Audits())
	}

	for _, id := range []string{"evt-pending", "evt-done"} {
		if _, err := svc.RequeueDeadLetter(bg, testActor, id); !errors.Is(err, models.ErrIllegalTransition) {
			t.Fatalf("非死信重放应拒(%s),实际: %v", id, err)
		}
	}
	if _, err := svc.RequeueDeadLetter(bg, testActor, "evt-none"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("不存在应 NotFound,实际: %v", err)
	}
	if len(mem.Audits()) != 1 {
		t.Fatalf("被拒动作不得写审计,实际 %d 条", len(mem.Audits()))
	}
}

// failAuditStore 包装 memstore:OpsAudit().Insert 注入失败(反证"审计与动作同事务")。
type failAuditStore struct{ *memstore.Mem }

type failAuditRepos struct{ store.Repos }

type failAuditRepo struct{ store.OpsAuditRepo }

func (failAuditRepo) Insert(context.Context, *models.OpsAuditLog) error {
	return errors.New("audit insert injected failure")
}
func (r failAuditRepos) OpsAudit() store.OpsAuditRepo { return failAuditRepo{r.Repos.OpsAudit()} }
func (s failAuditStore) WithinTx(ctx context.Context, fn func(ctx context.Context, r store.Repos) error) error {
	return s.Mem.WithinTx(ctx, func(ctx context.Context, r store.Repos) error {
		return fn(ctx, failAuditRepos{r})
	})
}

// TestAuditFailureRollsBackAction 反证:审计写入失败 → 动作整体回滚(状态不变、无审计)。
func TestAuditFailureRollsBackAction(t *testing.T) {
	mem := memstore.New()
	id := seedDiff(t, mem, models.DiffHandlePending, "")
	seedOutbox(t, mem, "evt-dead2", models.OutboxDead, 8)
	svc := New(Deps{Store: failAuditStore{mem}, Clock: opsClock})

	if _, err := svc.HandleReconcileDiff(bg, testActor, id, models.DiffHandleResolved, "r"); err == nil {
		t.Fatal("审计失败应使处置报错")
	}
	d, _ := mem.Reader().Reconcile().GetDiff(bg, id, false)
	if d.HandleStatus != models.DiffHandlePending {
		t.Fatalf("审计失败后处置必须回滚,实际: %v", d.HandleStatus)
	}
	if _, err := svc.RequeueDeadLetter(bg, testActor, "evt-dead2"); err == nil {
		t.Fatal("审计失败应使重放报错")
	}
	ob, _ := mem.Reader().Outbox().GetByEventID(bg, "evt-dead2", false)
	if ob.Status != models.OutboxDead {
		t.Fatalf("审计失败后重放必须回滚,实际: %v", ob.Status)
	}
	if len(mem.Audits()) != 0 {
		t.Fatalf("不得留下审计记录: %d", len(mem.Audits()))
	}
}

// TestListsReadOnlyAndPaging 三个列表只读不写审计;分页边界:0=默认、200 过、201 拒、翻页不重不漏。
func TestListsReadOnlyAndPaging(t *testing.T) {
	mem := memstore.New()
	for i := range 60 {
		seedOutbox(t, mem, fmt.Sprintf("evt-%02d", i), models.OutboxDead, 8)
	}
	svc := newSvc(mem)

	if _, _, err := svc.ListDeadLetters(bg, store.DeadFilter{}, 0, 201); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("limit=201 应拒,实际: %v", err)
	}
	if _, _, err := svc.ListDeadLetters(bg, store.DeadFilter{}, 0, 200); err != nil {
		t.Fatalf("limit=200 应过: %v", err)
	}
	// limit=0 → 默认 50;翻页取余 10,next 归零。
	page1, next, err := svc.ListDeadLetters(bg, store.DeadFilter{}, 0, 0)
	if err != nil || len(page1) != 50 || next == 0 {
		t.Fatalf("首页应 50 条且有下一页: n=%d next=%d err=%v", len(page1), next, err)
	}
	page2, next2, err := svc.ListDeadLetters(bg, store.DeadFilter{}, next, 0)
	if err != nil || len(page2) != 10 || next2 != 0 {
		t.Fatalf("末页应 10 条且 next=0: n=%d next=%d err=%v", len(page2), next2, err)
	}
	seen := map[string]bool{}
	for _, r := range append(page1, page2...) {
		if seen[r.EventID] {
			t.Fatalf("翻页出现重复: %s", r.EventID)
		}
		seen[r.EventID] = true
	}
	if len(seen) != 60 {
		t.Fatalf("翻页应覆盖全部 60 条,实际 %d", len(seen))
	}
	if len(mem.Audits()) != 0 {
		t.Fatal("只读方法不得写审计")
	}
}

// TestListAnomaliesFilterAndKind 异常清单只含 anomaly=1;anomaly_kind 与时间窗过滤可用;
// 空成因历史行正常返回。
func TestListAnomaliesFilterAndKind(t *testing.T) {
	mem := memstore.New()
	base := opsClock()
	ins := func(id string, anomaly bool, kind string, at time.Time) {
		err := mem.WithinTx(bg, func(ctx context.Context, r store.Repos) error {
			return r.Transactions().Insert(ctx, &models.PaymentTransaction{
				AppID: "app1", OrderNo: "OD-" + id, PayAccountID: "acc-w", Channel: "wechat",
				Kind: models.TxnPay, TransactionID: id, Amount: 90, Currency: "CNY",
				Anomaly: anomaly, AnomalyKind: kind, PaidAt: sql.NullTime{Time: at, Valid: true},
			})
		})
		if err != nil {
			t.Fatalf("播种流水失败: %v", err)
		}
	}
	ins("t1", false, "", base)
	ins("t2", true, models.AnomalyKindSecondRealPayment, base)
	ins("t3", true, "", base.Add(-48*time.Hour)) // 历史行:成因未落库
	svc := newSvc(mem)

	rows, _, err := svc.ListAnomalyTxns(bg, store.AnomalyFilter{}, 0, 0)
	if err != nil || len(rows) != 2 {
		t.Fatalf("应只返回 2 条异常流水: n=%d err=%v", len(rows), err)
	}
	kinds := map[string]string{}
	for _, r := range rows {
		kinds[r.TransactionID] = r.AnomalyKind
	}
	if kinds["t2"] != models.AnomalyKindSecondRealPayment || kinds["t3"] != "" {
		t.Fatalf("成因不符: %+v", kinds)
	}
	// 时间窗过滤:近 24h 只有 t2。
	rows, _, err = svc.ListAnomalyTxns(bg, store.AnomalyFilter{PaidFrom: base.Add(-24 * time.Hour)}, 0, 0)
	if err != nil || len(rows) != 1 || rows[0].TransactionID != "t2" {
		t.Fatalf("时间窗过滤不符: %+v err=%v", rows, err)
	}
}
