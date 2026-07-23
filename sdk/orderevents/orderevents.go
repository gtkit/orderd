// Package orderevents 是订单服务事件 webhook 的【接入方侧】消费 SDK(v2)。
//
// SDK 只做三件事:验签(key_id 轮换 + 时间窗口防重放)、解码、分发。
// **它不再提供 Seen/Mark 幂等存储**——权益发放的幂等真相源必须是接入方 MySQL 的
// inbox_events(见同目录 inbox_schema.sql):在【同一 DB 事务】里 INSERT inbox(唯一键防重)
// + 按 (order_no, aggregate_version) 单调校验 + 发权益 + 记流水,提交后才回 2xx。
// Redis 只能加速,不能作权威幂等源。
//
//	h := orderevents.NewHandler(orderevents.Config{
//	    KeyID: keyID, Secret: []byte(secret),
//	    OnPaid: func(ctx context.Context, m orderevents.EventMeta, e *orderv1.OrderPaidEvent) error {
//	        return tx(ctx, func(db) error {              // 接入方的 DB 事务
//	            if seen := insertInbox(db, m.EventID, e.OrderNo, m.AggregateVersion); seen { return nil }
//	            var b membershipv1.MembershipBenefit
//	            proto.Unmarshal(e.Metadata, &b)
//	            return grantMembership(db, e.UserId, b)   // 同事务发权益
//	        })
//	    },
//	})
//	mux.Handle("/webhooks/order", h)
package orderevents

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"strconv"
	"time"

	orderv1 "github.com/gtkit/orderd/pb/order/v1"
	"google.golang.org/protobuf/proto"
)

// errUnhandledMoneyEvent 资金事件无对应处理器 / payload 无法识别 → fail-closed:
// 返回错误让 ServeHTTP 回 5xx,触发发送端重投与告警,绝不静默回 200 吞掉资金事实(评审补充项)。
var errUnhandledMoneyEvent = errors.New("orderevents: 资金事件无处理器或 payload 无法识别")

const (
	HeaderEventID   = "X-Order-Event-Id"
	HeaderEventType = "X-Order-Event-Type"
	HeaderKeyID     = "X-Order-Key-Id"
	HeaderTimestamp = "X-Order-Timestamp"
	HeaderSignature = "X-Order-Signature"
	maxBodyBytes    = 1 << 20
	defaultSkew     = 5 * time.Minute
)

// EventMeta 事件元信息,供接入方在 DB inbox 事务里做幂等 + 版本单调判断。
type EventMeta struct {
	EventID          string
	EventType        string
	AggregateVersion int64
	PreviousStatus   orderv1.OrderStatus
	CurrentStatus    orderv1.OrderStatus
}

type (
	PaidHandler     func(ctx context.Context, m EventMeta, e *orderv1.OrderPaidEvent) error
	RefundedHandler func(ctx context.Context, m EventMeta, e *orderv1.OrderRefundedEvent) error
	ClosedHandler   func(ctx context.Context, m EventMeta, e *orderv1.OrderClosedEvent) error
)

type Config struct {
	KeyID      string // 当前签名密钥ID
	Secret     []byte
	PrevKeyID  string // 轮换过渡:旧密钥ID
	PrevSecret []byte
	ClockSkew  time.Duration // 时间窗口,默认 5min
	OnPaid     PaidHandler
	OnRefunded RefundedHandler
	OnClosed   ClosedHandler
}

type Handler struct{ cfg Config }

func NewHandler(cfg Config) *Handler {
	if len(cfg.Secret) == 0 {
		panic("orderevents: Config.Secret 不能为空")
	}
	// 验签强制携带 key_id(不再兼容缺失 key_id 回退当前密钥),故当前 KeyID 必填。
	if cfg.KeyID == "" {
		panic("orderevents: Config.KeyID 不能为空")
	}
	// PrevKeyID 与 PrevSecret 必须成对:只配一半会让轮换期验签行为不可预期(评审补充)。
	if (cfg.PrevKeyID == "") != (len(cfg.PrevSecret) == 0) {
		panic("orderevents: Config.PrevKeyID 与 PrevSecret 必须同时提供或同时为空")
	}
	// 资金事件必须有处理器:OnPaid 缺失会永久静默吞掉支付成功事件(评审补充,fail-closed)。
	if cfg.OnPaid == nil {
		panic("orderevents: Config.OnPaid 不能为 nil(支付成功是资金事件,不允许静默丢弃)")
	}
	if cfg.ClockSkew == 0 {
		cfg.ClockSkew = defaultSkew
	}
	return &Handler{cfg: cfg}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes))
	if err != nil {
		http.Error(w, "read body", http.StatusBadRequest)
		return
	}
	if !h.verify(r.Header, body) {
		http.Error(w, "signature/timestamp invalid", http.StatusUnauthorized)
		return
	}

	var env orderv1.EventEnvelope
	if err := proto.Unmarshal(body, &env); err != nil {
		http.Error(w, "unmarshal envelope", http.StatusBadRequest)
		return
	}
	if env.EventId == "" {
		http.Error(w, "missing event_id", http.StatusBadRequest)
		return
	}
	meta := EventMeta{
		EventID: env.EventId, EventType: env.EventType,
		AggregateVersion: env.AggregateVersion,
		PreviousStatus:   env.PreviousStatus, CurrentStatus: env.CurrentStatus,
	}
	// 分发。幂等 + 版本单调 + 发权益全在接入方回调的 DB 事务里完成(SDK 不做 Seen/Mark)。
	if err := h.dispatch(r.Context(), meta, &env); err != nil {
		http.Error(w, "handle event", http.StatusInternalServerError) // 5xx → 重投
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) dispatch(ctx context.Context, m EventMeta, env *orderv1.EventEnvelope) error {
	switch p := env.Payload.(type) {
	case *orderv1.EventEnvelope_Paid:
		return h.cfg.OnPaid(ctx, m, p.Paid) // OnPaid 已在 NewHandler 强校验非 nil
	case *orderv1.EventEnvelope_Refunded:
		if h.cfg.OnRefunded == nil {
			return errUnhandledMoneyEvent // 退款是资金反向事件,无处理器不静默 → 5xx
		}
		return h.cfg.OnRefunded(ctx, m, p.Refunded)
	case *orderv1.EventEnvelope_Closed:
		if h.cfg.OnClosed == nil {
			return nil // 关闭事件(未支付,无资金)可选消费,无处理器可安全跳过回 2xx
		}
		return h.cfg.OnClosed(ctx, m, p.Closed)
	default:
		// 未知/空 payload:协议版本不匹配或投递异常 → fail-closed,不静默丢弃。
		return errUnhandledMoneyEvent
	}
}

// verify 校验时间窗口(防重放)+ HMAC 签名(按 key_id 选密钥,支持轮换)。
func (h *Handler) verify(hdr http.Header, body []byte) bool {
	tsStr := hdr.Get(HeaderTimestamp)
	sig := hdr.Get(HeaderSignature)
	if tsStr == "" || sig == "" {
		return false
	}
	ts, err := strconv.ParseInt(tsStr, 10, 64)
	if err != nil {
		return false
	}
	skew := int64(h.cfg.ClockSkew / time.Second)
	if d := time.Now().Unix() - ts; d > skew || d < -skew {
		return false // 超出时间窗口,拒绝(防重放)
	}
	secret := h.selectSecret(hdr.Get(HeaderKeyID))
	if secret == nil {
		return false
	}
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(tsStr + "."))
	mac.Write(body)
	want := "sha256=" + hex.EncodeToString(mac.Sum(nil))
	return hmac.Equal([]byte(sig), []byte(want))
}

func (h *Handler) selectSecret(keyID string) []byte {
	switch {
	case keyID == "":
		return nil // 强制携带 key_id,缺失即拒(评审:验签过度兼容)
	case keyID == h.cfg.KeyID:
		return h.cfg.Secret
	case h.cfg.PrevKeyID != "" && keyID == h.cfg.PrevKeyID:
		return h.cfg.PrevSecret
	default:
		return nil
	}
}
