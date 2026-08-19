package worker

import (
	"context"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/gtkit/orderd/internal/models"
	"github.com/gtkit/orderd/internal/pkg/webhook"
	"github.com/gtkit/orderd/internal/shared/secret"
	"github.com/gtkit/orderd/internal/store"
	"github.com/gtkit/orderd/internal/store/memstore"
)

// seedDead 播种一条死信(时间戳显式,供冷却/存活条件判定)。
func seedDead(t *testing.T, mem *memstore.Mem, eventID string, attempts int32, createdAt, updatedAt time.Time) {
	t.Helper()
	err := mem.WithinTx(bg, func(ctx context.Context, r store.Repos) error {
		return r.Events().InsertOutbox(ctx, &models.EventOutbox{
			EventID: eventID, AppID: "app1", OrderNo: "OD1", EventType: models.EventTypePaid,
			Payload: []byte("p"), Status: models.OutboxDead, Attempts: attempts,
			NextAttemptAt: createdAt, CreatedAt: createdAt, UpdatedAt: updatedAt,
			LastError: sql.NullString{String: "boom", Valid: true},
		})
	})
	if err != nil {
		t.Fatalf("播种死信失败: %v", err)
	}
}

// dlDelivery 用与本文件一致的 qClock 构造投递逻辑(worker_test 的 newDelivery 固定 2023 时钟,
// 早于本文件播种时间,认领不到)。
func dlDelivery(mem *memstore.Mem, url string, alerter *recAlerter, maxAttempts int32) *Delivery {
	sender := webhook.NewSender(mustGuardT, 5*time.Second)
	return NewDelivery(mem, fakeEndpoints{url: url}, secret.NewStatic(map[string][]byte{"ref1": []byte("s3cr3t")}),
		sender, alerter, nil, Config{WorkerID: "w1", MaxAttempts: maxAttempts, BaseBackoff: time.Minute},
		func() time.Time { return qClock().Add(time.Minute) })
}

func dlWorker(mem *memstore.Mem, enabled bool) *DeadLetterRequeueWorker {
	return NewDeadLetterRequeueWorker(mem, DeadLetterRequeueConfig{
		Enabled: enabled, Cooldown: time.Hour, MaxAttempts: 8, MaxAutoRequeues: 3, MaxLifetime: 7 * 24 * time.Hour,
	}, qClock)
}

// TestDeadLetterRequeueLimits 上限矩阵:冷却内/次数达限/超存活/关闭 → 不动;全满足 → 重放 + system 审计。
func TestDeadLetterRequeueLimits(t *testing.T) {
	now := qClock()
	old := now.Add(-2 * time.Hour) // 满足冷却
	tests := []struct {
		name      string
		enabled   bool
		attempts  int32
		createdAt time.Time
		updatedAt time.Time
		want      int // 重放条数
	}{
		{"全满足被重放", true, 8, old, old, 1},
		{"冷却期内不重放", true, 8, old, now.Add(-time.Minute), 0},
		{"次数达上限永久停手", true, 11, old, old, 0}, // 8+3=11,attempts<11 不成立
		{"超存活时长永久停手", true, 8, now.Add(-8 * 24 * time.Hour), old, 0},
		{"开关关闭不动死信", false, 8, old, old, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mem := memstore.New()
			seedDead(t, mem, "evt-x", tc.attempts, tc.createdAt, tc.updatedAt)
			w := dlWorker(mem, tc.enabled)
			got, err := w.RunOnce(bg)
			if err != nil || got != tc.want {
				t.Fatalf("RunOnce: got=%d want=%d err=%v", got, tc.want, err)
			}
			ob, _ := mem.Reader().Outbox().GetByEventID(bg, "evt-x", false)
			if tc.want == 1 {
				if ob.Status != models.OutboxPending || ob.Attempts != tc.attempts {
					t.Fatalf("重放后应 Pending 且 attempts 不重置: %+v", ob)
				}
				auds := mem.Audits()
				if len(auds) != 1 || auds[0].Actor != models.OpsActorSystem || auds[0].TargetID != "evt-x" {
					t.Fatalf("应写恰一条 system 审计: %+v", auds)
				}
			} else {
				if ob.Status != models.OutboxDead {
					t.Fatalf("不满足条件的死信不得被动: %+v", ob)
				}
				if len(mem.Audits()) != 0 {
					t.Fatalf("未重放不得写审计")
				}
			}
		})
	}
}

// TestDeadLetterRequeueConcurrentExactlyOnce 反证"兜底重放与审计恰好配对":
// 两副本并发跑,每条死信恰好一次重放、恰好一条审计(移除认领/状态复核该测试必然失败)。
func TestDeadLetterRequeueConcurrentExactlyOnce(t *testing.T) {
	for range 10 {
		mem := memstore.New()
		old := qClock().Add(-2 * time.Hour)
		for _, id := range []string{"e1", "e2", "e3"} {
			seedDead(t, mem, id, 8, old, old)
		}
		w1, w2 := dlWorker(mem, true), dlWorker(mem, true)
		var wg sync.WaitGroup
		wg.Go(func() { _, _ = w1.RunOnce(bg) })
		wg.Go(func() { _, _ = w2.RunOnce(bg) })
		wg.Wait()

		byTarget := map[string]int{}
		for _, a := range mem.Audits() {
			byTarget[a.TargetID]++
		}
		for _, id := range []string{"e1", "e2", "e3"} {
			if byTarget[id] != 1 {
				t.Fatalf("死信 %s 审计条数应为 1,实际 %d(重复重放)", id, byTarget[id])
			}
			ob, _ := mem.Reader().Outbox().GetByEventID(bg, id, false)
			if ob.Status != models.OutboxPending {
				t.Fatalf("死信 %s 应已重放: %v", id, ob.Status)
			}
		}
	}
}

// TestRequeuedDeadLetterOneShot 反证"一次重放=一次投递机会":attempts 已达阈值的死信重放后
// 投递再失败 → 立即回 Dead、attempts+1、死信告警一次(若实现改为重置 attempts,本测试失败)。
func TestRequeuedDeadLetterOneShot(t *testing.T) {
	mem := memstore.New()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	old := qClock().Add(-2 * time.Hour)
	seedDead(t, mem, "evt-1", 8, old, old) // 已达 MaxAttempts=8
	// 兜底重放 → Pending。
	if got, err := dlWorker(mem, true).RunOnce(bg); err != nil || got != 1 {
		t.Fatalf("重放失败: got=%d err=%v", got, err)
	}
	// 投递失败 → 立即回死信(attempts+1),告警一次。
	alerter := &recAlerter{}
	d := dlDelivery(mem, srv.URL, alerter, 8)
	if _, failed, err := d.RunOnce(bg); err != nil || failed != 1 {
		t.Fatalf("投递轮次: failed=%d err=%v", failed, err)
	}
	ob, _ := mem.Reader().Outbox().GetByEventID(bg, "evt-1", false)
	if ob.Status != models.OutboxDead || ob.Attempts != 9 {
		t.Fatalf("重放后再失败应立即回死信且 attempts+1: %+v", ob)
	}
	if !alerter.has("outbox_dead_letter") {
		t.Fatal("回死信应告警")
	}
}

// TestRequeuedDeliveryPayloadIdentical 反证"重放不改变事件内容":两次投递的 event_id 与 body
// 字节完全相同。
func TestRequeuedDeliveryPayloadIdentical(t *testing.T) {
	mem := memstore.New()
	payload := paidPayload(t)

	var mu sync.Mutex
	var bodies [][]byte
	var eventIDs []string
	fail := true // 首次投递失败(转死信),重放后成功
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b := make([]byte, r.ContentLength)
		_, _ = r.Body.Read(b)
		mu.Lock()
		bodies = append(bodies, b)
		eventIDs = append(eventIDs, r.Header.Get("X-Order-Event-Id"))
		f := fail
		mu.Unlock()
		if f {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	enqueue(t, mem, payload, qClock().Add(-time.Minute))
	alerter := &recAlerter{}
	d := dlDelivery(mem, srv.URL, alerter, 1) // MaxAttempts=1:首败即死信
	if _, _, err := d.RunOnce(bg); err != nil {
		t.Fatalf("首轮投递: %v", err)
	}
	// 人工重放(直接走仓储语义:Requeue 不改 payload)。
	err := mem.WithinTx(bg, func(ctx context.Context, r store.Repos) error {
		ob, e := r.Outbox().GetByEventID(ctx, "evt-1", true)
		if e != nil {
			return e
		}
		return r.Outbox().Requeue(ctx, ob.ID, qClock())
	})
	if err != nil {
		t.Fatalf("重放失败: %v", err)
	}
	mu.Lock()
	fail = false
	mu.Unlock()
	if _, _, err := d.RunOnce(bg); err != nil {
		t.Fatalf("重放后投递: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(bodies) != 2 {
		t.Fatalf("应投递两次,实际 %d", len(bodies))
	}
	if eventIDs[0] != "evt-1" || eventIDs[1] != "evt-1" {
		t.Fatalf("event_id 应一致: %v", eventIDs)
	}
	if string(bodies[0]) != string(bodies[1]) || string(bodies[0]) != string(payload) {
		t.Fatal("重放投递的 body 字节必须与首次完全相同")
	}
}
