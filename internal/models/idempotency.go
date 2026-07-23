package models

import (
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"hash"
	"strings"
)

// HashInput 幂等请求哈希的输入(归一化下单请求的语义字段全集)。
//
// 评审 #9:哈希必须覆盖所有影响订单语义的字段,遗漏 client_order_no/product_title/channel_id
// 会让"同 key 改这些字段"绕过冲突检测。序列化用【长度前缀】而非 '|' 拼接,消除字段边界歧义
// (如 "a|b" 与 "a"+"|b" 不可再混淆)。整数用定宽大端,自带边界。
type HashInput struct {
	AppID           string
	UserID          int64
	ProductID       uint64
	ProductTitle    string
	BusinessType    string
	Original        int64
	Discount        int64
	Payable         int64
	Currency        string
	PayMethod       PayMethod
	ChannelID       int64
	ClientOrderNo   string
	MetadataType    string
	MetadataVersion int32
	Metadata        []byte
}

// hashWriter 确定性哈希写入器:字符串/字节用 8 字节大端长度前缀,整数用 8 字节大端定宽。
type hashWriter struct{ h hash.Hash }

func (w *hashWriter) bytes(b []byte) {
	var n [8]byte
	binary.BigEndian.PutUint64(n[:], uint64(len(b)))
	w.h.Write(n[:])
	w.h.Write(b)
}
func (w *hashWriter) str(s string) { w.bytes([]byte(s)) }
func (w *hashWriter) u64(v uint64) {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], v)
	w.h.Write(b[:])
}
func (w *hashWriter) i64(v int64) { w.u64(uint64(v)) }

// RequestHash 计算下单请求的确定性 SHA-256(hex,64 字符,对应 orders.request_hash CHAR(64))。
// 字段顺序固定,不含 request-id/时间戳等瞬态字段;metadata 先摘要再入哈希,避免超长。
func (in HashInput) RequestHash() string {
	w := &hashWriter{h: sha256.New()}
	w.str(in.AppID)
	w.i64(in.UserID)
	w.u64(in.ProductID)
	w.str(in.ProductTitle)
	w.str(in.BusinessType)
	w.i64(in.Original)
	w.i64(in.Discount)
	w.i64(in.Payable)
	w.str(in.Currency)
	w.i64(int64(in.PayMethod))
	w.i64(in.ChannelID)
	w.str(in.ClientOrderNo)
	w.str(in.MetadataType)
	w.i64(int64(in.MetadataVersion))
	metaDigest := sha256.Sum256(in.Metadata) // 空 metadata 也有确定摘要
	w.bytes(metaDigest[:])
	sum := w.h.Sum(nil)
	return hex.EncodeToString(sum)
}

// RefundRequestHash 计算退款请求的确定性哈希(out_refund_no+order_no+amount+reason),
// 绑定 out_refund_no:同退款号不同金额/原因/订单 → 冲突(DESIGN §10)。
func RefundRequestHash(outRefundNo, orderNo string, amount int64, reason string) string {
	w := &hashWriter{h: sha256.New()}
	w.str(outRefundNo)
	w.str(orderNo)
	w.i64(amount)
	w.str(reason)
	return hex.EncodeToString(w.h.Sum(nil))
}

// NormalizeKey 归一化可选唯一键(idempotency_key / client_order_no):去首尾空白;
// 空串必须落库为 NULL,否则 (app_id,”) 唯一索引会让每个 app 只能建一条空键订单(DDL 实测已确认)。
func NormalizeKey(s string) sql.NullString {
	s = strings.TrimSpace(s)
	if s == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: s, Valid: true}
}
