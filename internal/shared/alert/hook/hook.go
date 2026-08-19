// Package hook 是 alert.Alerter 的 webhook 实现:把告警异步送达钉钉/飞书群机器人或自建告警网关。
//
// 语义(与 alert-delivery spec 对齐):
//   - Anomaly 非阻塞——渲染后入有界队列即返回;队列满丢弃并记含 kind 的警告日志;
//   - 单发送 goroutine 顺序投递,单次 POST 强制超时;失败记日志不重试(结构化日志由装配层保底);
//   - Close 停止接收新告警,在给定预算内排空队列,超时放弃并记日志。
//
// webhook_url/secret 来自部署方挂载的配置(git 忽略的 env.yml),属可信输入,不做出口 SSRF 校验。
package hook

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gtkit/logger"
)

// 支持的消息格式。
const (
	ProviderDingtalk = "dingtalk" // 钉钉自定义机器人(可选加签)
	ProviderFeishu   = "feishu"   // 飞书自定义机器人(可选签名)
	ProviderGeneric  = "generic"  // 原样 JSON,供自建告警网关
)

// Config 是 webhook 告警配置。
type Config struct {
	Provider   string        // dingtalk / feishu / generic
	WebhookURL string        // 机器人/网关地址(敏感,可能含 token)
	Secret     string        // 可选:钉钉加签 / 飞书签名密钥
	Service    string        // 消息前缀服务名,默认 orderd
	Env        string        // 运行环境标识(dev/prod),入消息前缀
	QueueSize  int           // 待发送队列容量,默认 256
	Timeout    time.Duration // 单次 POST 超时,默认 5s
}

// Alerter 实现 alert.Alerter(异步 webhook 投递)。
type Alerter struct {
	cfg    Config
	client *http.Client
	queue  chan message
	stop   chan struct{}
	done   chan struct{}
	closed atomic.Bool
	now    func() time.Time
}

type message struct {
	kind   string
	fields map[string]any
}

// New 构造并启动发送循环;provider 非法或 url 缺失返回错误(fail-fast)。
func New(cfg Config) (*Alerter, error) {
	switch cfg.Provider {
	case ProviderDingtalk, ProviderFeishu, ProviderGeneric:
	default:
		return nil, fmt.Errorf("hook: 未知 alert.provider %q(可选 dingtalk/feishu/generic)", cfg.Provider)
	}
	if cfg.WebhookURL == "" {
		return nil, fmt.Errorf("hook: alert.webhook_url 必填")
	}
	if cfg.Service == "" {
		cfg.Service = "orderd"
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = 256
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 5 * time.Second
	}
	a := &Alerter{
		cfg:    cfg,
		client: &http.Client{Timeout: cfg.Timeout},
		queue:  make(chan message, cfg.QueueSize),
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
		now:    time.Now,
	}
	go a.loop()
	return a, nil
}

// Anomaly 渲染并非阻塞入队;队列满或已关闭时丢弃并记日志(内容始终有结构化日志保底)。
func (a *Alerter) Anomaly(_ context.Context, kind string, fields map[string]any) {
	if a.closed.Load() {
		logger.Warnw("alert/hook: 已关闭,丢弃告警", "kind", kind)
		return
	}
	select {
	case a.queue <- message{kind: kind, fields: fields}:
	default:
		logger.Warnw("alert/hook: 队列已满,丢弃告警", "kind", kind)
	}
}

// Close 停止接收新告警并在 ctx 预算内排空队列;超时放弃剩余并记日志。
func (a *Alerter) Close(ctx context.Context) {
	if a.closed.Swap(true) {
		return // 幂等
	}
	close(a.stop)
	select {
	case <-a.done:
	case <-ctx.Done():
		logger.Warnw("alert/hook: flush 超时,放弃剩余告警", "pending", len(a.queue))
	}
}

// loop 顺序投递;收到停止信号后排空队列再退出。
func (a *Alerter) loop() {
	defer close(a.done)
	for {
		select {
		case m := <-a.queue:
			a.send(m)
		case <-a.stop:
			for {
				select {
				case m := <-a.queue:
					a.send(m)
				default:
					return
				}
			}
		}
	}
}

// send 投递一条;失败只记日志(不重试:日志已保底,故障期重试只会堆积)。
func (a *Alerter) send(m message) {
	reqURL, body, err := a.buildRequest(m)
	if err != nil {
		logger.Warnw("alert/hook: 构造告警请求失败", "kind", m.kind, "err", err)
		return
	}
	req, err := http.NewRequest(http.MethodPost, reqURL, bytes.NewReader(body))
	if err != nil {
		logger.Warnw("alert/hook: 构造告警请求失败", "kind", m.kind, "err", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := a.client.Do(req)
	if err != nil {
		logger.Warnw("alert/hook: 告警投递失败", "kind", m.kind, "err", err)
		return
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		logger.Warnw("alert/hook: 告警端点非 2xx", "kind", m.kind, "status", resp.StatusCode)
	}
}

// buildRequest 按 provider 构造请求 URL 与 JSON 体。
func (a *Alerter) buildRequest(m message) (reqURL string, body []byte, err error) {
	text := renderText(a.cfg.Service, a.cfg.Env, m.kind, m.fields)
	switch a.cfg.Provider {
	case ProviderDingtalk:
		reqURL = a.cfg.WebhookURL
		if a.cfg.Secret != "" {
			// 钉钉加签:timestamp 毫秒;sign = base64(HMAC-SHA256(secret, ts+"\n"+secret)),URL 参数携带。
			ts := strconv.FormatInt(a.now().UnixMilli(), 10)
			mac := hmac.New(sha256.New, []byte(a.cfg.Secret))
			mac.Write([]byte(ts + "\n" + a.cfg.Secret))
			sign := base64.StdEncoding.EncodeToString(mac.Sum(nil))
			sep := "?"
			if strings.Contains(reqURL, "?") {
				sep = "&"
			}
			reqURL += sep + "timestamp=" + ts + "&sign=" + url.QueryEscape(sign)
		}
		body, err = json.Marshal(map[string]any{
			"msgtype": "text",
			"text":    map[string]string{"content": text},
		})
	case ProviderFeishu:
		reqURL = a.cfg.WebhookURL
		payload := map[string]any{
			"msg_type": "text",
			"content":  map[string]string{"text": text},
		}
		if a.cfg.Secret != "" {
			// 飞书签名:timestamp 秒;key = ts+"\n"+secret,对空串做 HMAC-SHA256 取 base64,body 携带。
			ts := strconv.FormatInt(a.now().Unix(), 10)
			mac := hmac.New(sha256.New, []byte(ts+"\n"+a.cfg.Secret))
			payload["timestamp"] = ts
			payload["sign"] = base64.StdEncoding.EncodeToString(mac.Sum(nil))
		}
		body, err = json.Marshal(payload)
	default: // ProviderGeneric(New 已校验枚举)
		reqURL = a.cfg.WebhookURL
		body, err = json.Marshal(map[string]any{
			"service": a.cfg.Service,
			"env":     a.cfg.Env,
			"kind":    m.kind,
			"fields":  m.fields,
			"ts":      a.now().Format(time.RFC3339),
		})
	}
	return reqURL, body, err
}

// renderText 确定性渲染消息文本:服务/环境/kind + 按 key 排序的字段行。
func renderText(service, env, kind string, fields map[string]any) string {
	var b strings.Builder
	fmt.Fprintf(&b, "[%s/%s] 资金异常: %s", service, env, kind)
	for _, k := range slices.Sorted(maps.Keys(fields)) {
		fmt.Fprintf(&b, "\n%s=%v", k, fields[k])
	}
	return b.String()
}
