// Package idgen 生成事件 ID(UUIDv4)与订单安全凭证(order_token)。
// 用标准库 crypto/rand,不引第三方 uuid 库(红线:能砍就砍)。
package idgen

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

// UUIDv4 返回 RFC 4122 v4 UUID 字符串(用于 event_id)。
func UUIDv4() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("idgen: 读取随机源失败: %w", err)
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 10
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}

// SecureToken 返回 n 字节随机数据的十六进制串(长度 2n)。order_token 用 n=32 → 64 字符。
func SecureToken(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("idgen: 读取随机源失败: %w", err)
	}
	return hex.EncodeToString(b), nil
}
