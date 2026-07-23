// Package webhook 是 Outbox 事件的出口投递器:HMAC 签名 + SSRF 防护 + 禁重定向 + 限时限长。
// 签名与接入方 sdk/orderevents 验签严格对齐:sha256=hex(HMAC-SHA256(secret, ts+"."+body))。
package webhook

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/gtkit/orderd/internal/appmeta"
)

const (
	contentType = "application/x-protobuf"
	maxRespBody = 64 << 10
)

// Message 一次投递所需的全部信息。
type Message struct {
	URL       string
	KeyID     string
	Secret    []byte
	EventID   string
	EventType string
	Body      []byte // 序列化的 EventEnvelope(webhook body 原文,签名覆盖它)
}

// Sender 出口投递器。
type Sender struct {
	client *http.Client
	guard  *SSRFGuard
	now    func() time.Time
}

// Option 配置项。
type Option func(*Sender)

// WithClock 注入时钟(测试用)。
func WithClock(f func() time.Time) Option { return func(s *Sender) { s.now = f } }

// NewSender 构造投递器;guard 为 SSRF 守卫,timeout 为单次投递超时。
func NewSender(guard *SSRFGuard, timeout time.Duration, opts ...Option) *Sender {
	base := &net.Dialer{Timeout: 5 * time.Second}
	tr := &http.Transport{
		DialContext:           guard.dialContext(base),
		TLSHandshakeTimeout:   5 * time.Second,
		ResponseHeaderTimeout: timeout,
		DisableKeepAlives:     true,
	}
	s := &Sender{
		client: &http.Client{
			Timeout:   timeout,
			Transport: tr,
			// 禁止重定向:防跳转到内网/绕过 SSRF。
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
		guard: guard,
		now:   time.Now,
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

// sign 生成与 SDK 一致的签名。
func sign(secret []byte, ts string, body []byte) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(ts + "."))
	mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

// Send 投递一条 webhook;非 2xx 或网络错误返回 error(供上层重试/死信)。
func (s *Sender) Send(ctx context.Context, m Message) error {
	u, err := url.Parse(m.URL)
	if err != nil {
		return fmt.Errorf("webhook: 非法 URL: %w", err)
	}
	// 仅 https;白名单主机名可放行 http(兼容内部私网 BFF)。
	if u.Scheme != "https" && !s.guard.hostAllowed(u.Hostname()) {
		return fmt.Errorf("webhook: 仅允许 https(或白名单主机),实际 scheme=%q", u.Scheme)
	}

	ts := strconv.FormatInt(s.now().Unix(), 10)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, m.URL, bytes.NewReader(m.Body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", contentType)
	req.Header.Set(appmeta.HeaderEventID, m.EventID)
	req.Header.Set(appmeta.HeaderEventType, m.EventType)
	req.Header.Set(appmeta.HeaderKeyID, m.KeyID)
	req.Header.Set(appmeta.HeaderTimestamp, ts)
	req.Header.Set(appmeta.HeaderSignature, sign(m.Secret, ts, m.Body))

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("webhook: 投递失败: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxRespBody))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("webhook: 接入方返回 %d", resp.StatusCode)
	}
	return nil
}
