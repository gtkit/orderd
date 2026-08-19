package hook

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// capture 记录收到的请求(并发安全)。
type capture struct {
	mu     sync.Mutex
	urls   []string
	bodies []map[string]any
	status int
	block  chan struct{} // 非 nil 时 handler 阻塞直到该 chan 关闭
}

func (c *capture) handler(w http.ResponseWriter, r *http.Request) {
	if c.block != nil {
		<-c.block
	}
	raw, _ := io.ReadAll(r.Body)
	var m map[string]any
	_ = json.Unmarshal(raw, &m)
	c.mu.Lock()
	c.urls = append(c.urls, r.URL.String())
	c.bodies = append(c.bodies, m)
	c.mu.Unlock()
	st := c.status
	if st == 0 {
		st = http.StatusOK
	}
	w.WriteHeader(st)
}

func (c *capture) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.bodies)
}

func newAlerter(t *testing.T, cfg Config) (*Alerter, *capture) {
	t.Helper()
	c := &capture{}
	srv := httptest.NewServer(http.HandlerFunc(c.handler))
	t.Cleanup(srv.Close)
	if cfg.WebhookURL == "" {
		cfg.WebhookURL = srv.URL
	}
	a, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		a.Close(ctx)
	})
	return a, c
}

// waitCount 等待收到 n 条请求(告警是异步投递)。
func waitCount(t *testing.T, c *capture, n int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for c.count() < n {
		if time.Now().After(deadline) {
			t.Fatalf("等待 %d 条请求超时,实际 %d", n, c.count())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestNewValidation(t *testing.T) {
	if _, err := New(Config{Provider: "slack", WebhookURL: "https://x"}); err == nil {
		t.Fatal("未知 provider 应拒绝")
	}
	if _, err := New(Config{Provider: ProviderGeneric}); err == nil {
		t.Fatal("缺 webhook_url 应拒绝")
	}
}

func TestDingtalkFormatAndSign(t *testing.T) {
	a, c := newAlerter(t, Config{Provider: ProviderDingtalk, Secret: "s3cret", Service: "orderd", Env: "test"})
	fixed := time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC)
	a.now = func() time.Time { return fixed }

	a.Anomaly(context.Background(), "outbox_dead_letter", map[string]any{"b": 2, "a": "x"})
	waitCount(t, c, 1)

	// 加签:URL 携带毫秒 timestamp 与 sign=base64(HMAC-SHA256(secret, ts+"\n"+secret))。
	ts := strconv.FormatInt(fixed.UnixMilli(), 10)
	mac := hmac.New(sha256.New, []byte("s3cret"))
	mac.Write([]byte(ts + "\n" + "s3cret"))
	wantSign := base64.StdEncoding.EncodeToString(mac.Sum(nil))
	if !strings.Contains(c.urls[0], "timestamp="+ts) {
		t.Fatalf("URL 应带毫秒 timestamp: %s", c.urls[0])
	}
	gotURL := c.urls[0]
	if !strings.Contains(gotURL, "sign="+url.QueryEscape(wantSign)) {
		t.Fatalf("URL 签名不符: %s want sign %s", gotURL, wantSign)
	}
	body := c.bodies[0]
	if body["msgtype"] != "text" {
		t.Fatalf("钉钉体应为 msgtype:text: %+v", body)
	}
	text := body["text"].(map[string]any)["content"].(string)
	// 确定性渲染:前缀 + 按 key 排序字段行。
	if text != "[orderd/test] 资金异常: outbox_dead_letter\na=x\nb=2" {
		t.Fatalf("消息渲染不符: %q", text)
	}
}

func TestFeishuFormatAndSign(t *testing.T) {
	a, c := newAlerter(t, Config{Provider: ProviderFeishu, Secret: "fs", Env: "prod"})
	fixed := time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC)
	a.now = func() time.Time { return fixed }

	a.Anomaly(context.Background(), "refund_query_stalled", map[string]any{"refund_no": "RF1"})
	waitCount(t, c, 1)

	body := c.bodies[0]
	if body["msg_type"] != "text" {
		t.Fatalf("飞书体应为 msg_type:text: %+v", body)
	}
	ts := strconv.FormatInt(fixed.Unix(), 10) // 飞书用秒
	if body["timestamp"] != ts {
		t.Fatalf("飞书 timestamp 应为秒: %+v", body)
	}
	mac := hmac.New(sha256.New, []byte(ts+"\n"+"fs"))
	if body["sign"] != base64.StdEncoding.EncodeToString(mac.Sum(nil)) {
		t.Fatalf("飞书签名不符: %+v", body)
	}
	text := body["content"].(map[string]any)["text"].(string)
	if !strings.HasPrefix(text, "[orderd/prod] 资金异常: refund_query_stalled") {
		t.Fatalf("消息前缀不符: %q", text)
	}
}

func TestGenericJSON(t *testing.T) {
	a, c := newAlerter(t, Config{Provider: ProviderGeneric, Service: "orderd", Env: "dev"})
	a.Anomaly(context.Background(), "second_real_payment", map[string]any{"order_no": "OD1"})
	waitCount(t, c, 1)
	body := c.bodies[0]
	if body["kind"] != "second_real_payment" || body["service"] != "orderd" || body["env"] != "dev" {
		t.Fatalf("generic 体不符: %+v", body)
	}
	if body["fields"].(map[string]any)["order_no"] != "OD1" {
		t.Fatalf("generic fields 不符: %+v", body)
	}
}

// TestQueueFullDoesNotBlock 反证"非阻塞":发送端被阻塞、队列灌满后,再次 Anomaly 必须立即返回。
func TestQueueFullDoesNotBlock(t *testing.T) {
	c := &capture{block: make(chan struct{})}
	srv := httptest.NewServer(http.HandlerFunc(c.handler))
	defer srv.Close()
	a, err := New(Config{Provider: ProviderGeneric, WebhookURL: srv.URL, QueueSize: 2, Timeout: 2 * time.Second})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// 灌满:1 条在 handler 中阻塞,2 条占满队列,后续丢弃。
	for i := range 10 {
		done := make(chan struct{})
		go func() {
			a.Anomaly(context.Background(), "k", map[string]any{"i": i})
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(500 * time.Millisecond):
			t.Fatal("Anomaly 阻塞(必须非阻塞入队/丢弃)")
		}
	}
	close(c.block) // 放行,便于 Close 排空
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	a.Close(ctx)
}

// TestCloseFlushes 反证"退出前排空":入队后立即 Close,消息仍须送达。
func TestCloseFlushes(t *testing.T) {
	c := &capture{}
	srv := httptest.NewServer(http.HandlerFunc(c.handler))
	defer srv.Close()
	a, err := New(Config{Provider: ProviderGeneric, WebhookURL: srv.URL})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	for i := range 5 {
		a.Anomaly(context.Background(), "k", map[string]any{"i": i})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	a.Close(ctx)
	if c.count() != 5 {
		t.Fatalf("Close 应排空队列(5 条),实际送达 %d", c.count())
	}
	// 关闭后再告警:丢弃不 panic。
	a.Anomaly(context.Background(), "late", nil)
}

// TestSendFailureNoRetry 端点持续 5xx:每条只投一次,不重试。
func TestSendFailureNoRetry(t *testing.T) {
	c := &capture{status: http.StatusInternalServerError}
	srv := httptest.NewServer(http.HandlerFunc(c.handler))
	defer srv.Close()
	a, err := New(Config{Provider: ProviderGeneric, WebhookURL: srv.URL})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	a.Anomaly(context.Background(), "k", nil)
	waitCount(t, c, 1)
	time.Sleep(100 * time.Millisecond) // 若有重试会追加请求
	if c.count() != 1 {
		t.Fatalf("失败不应重试,实际请求 %d 次", c.count())
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	a.Close(ctx)
}
