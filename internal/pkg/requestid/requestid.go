// Package requestid 提供请求标识的生成与 context 传递。
//
// 纯基础包:无业务、无协议依赖,任何层可 import(拦截器写入,深层排障日志读取)。
package requestid

import (
	"context"
	"crypto/rand"
)

type ctxKey struct{}

// New 生成一个不可预测的请求标识(crypto/rand.Text,≥128 bit 随机性,base32 文本)。
func New() string {
	return rand.Text()
}

// NewContext 返回携带 request id 的 ctx;id 为空时原样返回。
func NewContext(ctx context.Context, id string) context.Context {
	if id == "" {
		return ctx
	}
	return context.WithValue(ctx, ctxKey{}, id)
}

// FromContext 读取 ctx 中的 request id;未设置返回空串。
func FromContext(ctx context.Context) string {
	id, _ := ctx.Value(ctxKey{}).(string)
	return id
}
