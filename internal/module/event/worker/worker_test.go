package worker

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/gtkit/logger"
	"github.com/gtkit/orderd/internal/models"
	"github.com/gtkit/orderd/internal/pkg/webhook"
	"github.com/gtkit/orderd/internal/shared/secret"
	"github.com/gtkit/orderd/internal/store"
	"github.com/gtkit/orderd/internal/store/memstore"
	orderv1 "github.com/gtkit/orderd/pb/order/v1"
	"github.com/gtkit/orderd/sdk/orderevents"
	"google.golang.org/protobuf/proto"
)

var bg = context.Background()

func TestMain(m *testing.M) {
	_ = logger.New(logger.WithConsole(true), logger.WithFile(false), logger.WithLevel("error"))
	os.Exit(m.Run())
}

type fakeEndpoints struct{ url string }

func (f fakeEndpoints) Resolve(_ context.Context, appID string) (models.AppEndpoint, error) {
	return models.AppEndpoint{AppID: appID, WebhookURL: f.url, KeyID: "k1", HMACSecretRef: "ref1", Enabled: true}, nil
}

type recAlerter struct {
	mu    sync.Mutex
	kinds []string
}

func (a *recAlerter) Anomaly(_ context.Context, kind string, _ map[string]any) {
	a.mu.Lock()
	a.kinds = append(a.kinds, kind)
	a.mu.Unlock()
}

func (a *recAlerter) has(kind string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return slices.Contains(a.kinds, kind)
}

func paidPayload(t *testing.T) []byte {
	t.Helper()
	env := &orderv1.EventEnvelope{
		EventId: "evt-1", EventType: models.EventTypePaid, AppId: "app1", OrderNo: "OD1", AggregateVersion: 1,
		Payload: &orderv1.EventEnvelope_Paid{Paid: &orderv1.OrderPaidEvent{OrderNo: "OD1", UserId: 42, Amount: 90}},
	}
	b, err := proto.Marshal(env)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

func enqueue(t *testing.T, mem *memstore.Mem, payload []byte, nextAt time.Time) {
	t.Helper()
	err := mem.WithinTx(bg, func(ctx context.Context, r store.Repos) error {
		return r.Events().InsertOutbox(ctx, &models.EventOutbox{
			EventID: "evt-1", AppID: "app1", OrderNo: "OD1", EventType: models.EventTypePaid,
			AggregateVersion: 1, Payload: payload, Status: models.OutboxPending, NextAttemptAt: nextAt,
		})
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
}

// mustGuardT 包级 SSRF 守卫(127.0.0.1 放行,httptest 用)。
var mustGuardT, _ = webhook.NewSSRFGuard([]string{"127.0.0.1"}, nil)

func newDelivery(mem *memstore.Mem, url string, alerter *recAlerter, maxAttempts int32) *Delivery {
	sender := webhook.NewSender(mustGuardT, 5*time.Second)
	return NewDelivery(mem, fakeEndpoints{url: url}, secret.NewStatic(map[string][]byte{"ref1": []byte("s3cr3t")}),
		sender, alerter, nil, Config{WorkerID: "w1", MaxAttempts: maxAttempts, BaseBackoff: time.Minute}, func() time.Time { return time.Unix(1_700_000_000, 0) })
}

func TestDeliverySuccess(t *testing.T) {
	mem := memstore.New()
	payload := paidPayload(t)

	var received int
	var mu sync.Mutex
	h := orderevents.NewHandler(orderevents.Config{
		KeyID: "k1", Secret: []byte("s3cr3t"),
		OnPaid: func(context.Context, orderevents.EventMeta, *orderv1.OrderPaidEvent) error {
			mu.Lock()
			received++
			mu.Unlock()
			return nil
		},
	})
	srv := httptest.NewServer(h)
	defer srv.Close()

	enqueue(t, mem, payload, time.Unix(1_600_000_000, 0)) // 已到期
	d := newDelivery(mem, srv.URL, &recAlerter{}, 8)
	delivered, failed, err := d.RunOnce(bg)
	if err != nil || delivered != 1 || failed != 0 {
		t.Fatalf("投递应成功: delivered=%d failed=%d err=%v", delivered, failed, err)
	}
	if received != 1 {
		t.Fatalf("接入方应收到并验签通过 1 次,实际 %d(证明签名与 SDK 对齐)", received)
	}
	if ob := mem.Outbox(); len(ob) != 1 || ob[0].Status != models.OutboxDelivered {
		t.Fatalf("outbox 应标记 Delivered: %+v", ob)
	}
}

func TestDeliveryRetryOn5xx(t *testing.T) {
	mem := memstore.New()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	enqueue(t, mem, paidPayload(t), time.Unix(1_600_000_000, 0))
	d := newDelivery(mem, srv.URL, &recAlerter{}, 8)
	_, failed, err := d.RunOnce(bg)
	if err != nil || failed != 1 {
		t.Fatalf("5xx 应计失败: failed=%d err=%v", failed, err)
	}
	ob := mem.Outbox()[0]
	if ob.Status != models.OutboxPending || ob.Attempts != 1 || !ob.NextAttemptAt.After(time.Unix(1_700_000_000, 0)) {
		t.Fatalf("失败应重排(Pending,attempts=1,next 在未来): %+v", ob)
	}
	if ob.LockedUntil.Valid {
		t.Fatalf("重排后应清租约")
	}
}

func TestDeliveryDeadLetter(t *testing.T) {
	mem := memstore.New()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	enqueue(t, mem, paidPayload(t), time.Unix(1_600_000_000, 0))
	alerter := &recAlerter{}
	d := newDelivery(mem, srv.URL, alerter, 1) // MaxAttempts=1 → 首次失败即死信
	if _, failed, err := d.RunOnce(bg); err != nil || failed != 1 {
		t.Fatalf("应失败: failed=%d err=%v", failed, err)
	}
	if ob := mem.Outbox()[0]; ob.Status != models.OutboxDead {
		t.Fatalf("超次数应转死信: %+v", ob)
	}
	if !alerter.has("outbox_dead_letter") {
		t.Fatalf("死信应告警")
	}
}

func TestDeliverySSRFBlocked(t *testing.T) {
	mem := memstore.New()
	enqueue(t, mem, paidPayload(t), time.Unix(1_600_000_000, 0))
	// 端点指向云元数据地址(链路本地),SSRF 应始终拒。
	d := newDelivery(mem, "http://169.254.169.254/", &recAlerter{}, 8)
	_, failed, err := d.RunOnce(bg)
	if err != nil || failed != 1 {
		t.Fatalf("SSRF 目标应投递失败: failed=%d err=%v", failed, err)
	}
	if ob := mem.Outbox()[0]; ob.Status != models.OutboxPending || ob.Attempts != 1 {
		t.Fatalf("SSRF 失败应重排: %+v", ob)
	}
}

// recObserver 记录型埋点端口(flow-metrics spec 验证)。
type recObserver struct {
	mu       sync.Mutex
	outcomes []string
	seconds  []float64
}

func (o *recObserver) DeliveryResult(outcome string, seconds float64) {
	o.mu.Lock()
	o.outcomes = append(o.outcomes, outcome)
	o.seconds = append(o.seconds, seconds)
	o.mu.Unlock()
}
func (o *recObserver) ConfirmOutcome(string)          {}
func (o *recObserver) ClosingSettled(string, float64) {}
func (o *recObserver) PrepareRejected(string)         {}

// TestDeliveryObserverOutcomes delivered/failed/dead 互斥计数,耗时非负。
func TestDeliveryObserverOutcomes(t *testing.T) {
	var status int
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		st := status
		mu.Unlock()
		w.WriteHeader(st)
	}))
	defer srv.Close()

	mem := memstore.New()
	ob := &recObserver{}
	sender := webhook.NewSender(mustGuardT, 5*time.Second)
	d := NewDelivery(mem, fakeEndpoints{url: srv.URL}, secret.NewStatic(map[string][]byte{"ref1": []byte("s3cr3t")}),
		sender, &recAlerter{}, ob, Config{WorkerID: "w1", MaxAttempts: 2, BaseBackoff: time.Minute},
		func() time.Time { return qClock().Add(time.Minute) })

	enqueue(t, mem, paidPayload(t), qClock().Add(-time.Minute))
	// 第一轮失败(attempts 1 < 2)→ failed;第二轮失败(attempts 2 >= 2)→ dead。
	mu.Lock()
	status = http.StatusInternalServerError
	mu.Unlock()
	if _, _, err := d.RunOnce(bg); err != nil {
		t.Fatal(err)
	}
	// 重排后立即可再投(把 next_attempt_at 拉回)。
	err := mem.WithinTx(bg, func(ctx context.Context, r store.Repos) error {
		o, e := r.Outbox().GetByEventID(ctx, "evt-1", true)
		if e != nil {
			return e
		}
		return r.Outbox().Requeue(ctx, o.ID, qClock())
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := d.RunOnce(bg); err != nil {
		t.Fatal(err)
	}
	// 重放 + 成功 → delivered。
	err = mem.WithinTx(bg, func(ctx context.Context, r store.Repos) error {
		o, e := r.Outbox().GetByEventID(ctx, "evt-1", true)
		if e != nil {
			return e
		}
		return r.Outbox().Requeue(ctx, o.ID, qClock())
	})
	if err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	status = http.StatusOK
	mu.Unlock()
	if _, _, err := d.RunOnce(bg); err != nil {
		t.Fatal(err)
	}

	ob.mu.Lock()
	defer ob.mu.Unlock()
	want := []string{"failed", "dead", "delivered"}
	if len(ob.outcomes) != 3 || ob.outcomes[0] != want[0] || ob.outcomes[1] != want[1] || ob.outcomes[2] != want[2] {
		t.Fatalf("outcome 序列应互斥且为 %v,实际 %v", want, ob.outcomes)
	}
	for i, s := range ob.seconds {
		if s < 0 {
			t.Fatalf("耗时 #%d 非法: %v", i, s)
		}
	}
}
