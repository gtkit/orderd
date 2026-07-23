// Package auth 定义调用方身份与 token 校验端口(DESIGN §13)。
//
// Claims 来自 token(非请求参数):app_id 由 token 注入,BFF token 只能访问其 claims 允许的 app_id;
// 方法级权限由 permissions 承载。校验含 issuer/audience/expiry + 密钥轮换(kid)。
package auth

import (
	"context"
	"errors"
	"maps"
	"slices"
)

// ErrUnauthenticated token 缺失/非法/过期。
var ErrUnauthenticated = errors.New("auth: 身份校验失败")

// Claims 是已校验的调用方身份。
type Claims struct {
	AppID       string
	UserID      int64
	permissions map[string]struct{}
}

// NewClaims 由权限列表构造 Claims(便于测试/适配)。
func NewClaims(appID string, userID int64, perms []string) Claims {
	m := make(map[string]struct{}, len(perms))
	for _, p := range perms {
		m[p] = struct{}{}
	}
	return Claims{AppID: appID, UserID: userID, permissions: m}
}

// Has 报告是否具备某权限。
func (c Claims) Has(perm string) bool {
	_, ok := c.permissions[perm]
	return ok
}

// Permissions 返回权限列表副本(排序,便于日志/测试)。
func (c Claims) Permissions() []string {
	return slices.Sorted(maps.Keys(c.permissions))
}

// Verifier 校验 bearer token 并返回 Claims;失败返回包裹 ErrUnauthenticated 的错误。
type Verifier interface {
	Verify(ctx context.Context, token string) (Claims, error)
}
