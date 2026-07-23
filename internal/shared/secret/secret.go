// Package secret 定义密钥解析端口:把密钥系统引用(*_ref)解析为明文密钥。
//
// 生产用 KMS/Vault 实现;phase-2 提供 Static(内存/配置)实现便于本地与测试。
package secret

import (
	"context"
	"errors"
)

// ErrNotFound 引用无法解析。
var ErrNotFound = errors.New("secret: 引用未找到")

// Resolver 解析密钥引用为明文。
type Resolver interface {
	Resolve(ctx context.Context, ref string) ([]byte, error)
}

// Static 用内存映射实现(ref → 明文),适合本地/测试;禁止在生产存明文。
type Static struct{ m map[string][]byte }

// NewStatic 构造静态解析器。
func NewStatic(m map[string][]byte) Static { return Static{m: m} }

func (s Static) Resolve(_ context.Context, ref string) ([]byte, error) {
	v, ok := s.m[ref]
	if !ok {
		return nil, ErrNotFound
	}
	return v, nil
}
