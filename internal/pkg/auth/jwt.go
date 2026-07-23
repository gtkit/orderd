package auth

import (
	"context"
	"fmt"

	"github.com/golang-jwt/jwt/v5"
)

// jwtClaims 是 token 载荷:标准声明 + orderd 自定义声明。
type jwtClaims struct {
	AppID  string   `json:"app_id"`
	UserID int64    `json:"user_id"`
	Perms  []string `json:"perms"`
	jwt.RegisteredClaims
}

// JWTVerifier 用 HS256 校验 JWT,支持 kid 密钥轮换 + issuer/audience/expiry 强校验。
// (RS256 可后续在 keyFunc 中按 alg 扩展;安全关键逻辑复用 golang-jwt,不自造。)
type JWTVerifier struct {
	keys     map[string][]byte // kid → HS256 密钥(轮换:同时挂多把)
	issuer   string
	audience string
}

// NewJWTVerifier 构造校验器。keys 至少一把;issuer/audience 为空表示不校验该项(不建议)。
func NewJWTVerifier(keys map[string][]byte, issuer, audience string) (*JWTVerifier, error) {
	if len(keys) == 0 {
		return nil, fmt.Errorf("auth: 至少需配置一把签名密钥")
	}
	return &JWTVerifier{keys: keys, issuer: issuer, audience: audience}, nil
}

// Verify 校验 token 并返回 Claims。
func (v *JWTVerifier) Verify(_ context.Context, token string) (Claims, error) {
	opts := []jwt.ParserOption{
		jwt.WithValidMethods([]string{"HS256"}),
		jwt.WithExpirationRequired(),
	}
	if v.issuer != "" {
		opts = append(opts, jwt.WithIssuer(v.issuer))
	}
	if v.audience != "" {
		opts = append(opts, jwt.WithAudience(v.audience))
	}

	var claims jwtClaims
	_, err := jwt.ParseWithClaims(token, &claims, v.keyFunc, opts...)
	if err != nil {
		return Claims{}, fmt.Errorf("%w: %v", ErrUnauthenticated, err)
	}
	if claims.AppID == "" || claims.UserID <= 0 {
		return Claims{}, fmt.Errorf("%w: 缺少 app_id/user_id 声明", ErrUnauthenticated)
	}
	return NewClaims(claims.AppID, claims.UserID, claims.Perms), nil
}

// keyFunc 按 header 的 kid 选密钥(轮换);拒绝非 HS256 与未知 kid。
func (v *JWTVerifier) keyFunc(t *jwt.Token) (any, error) {
	if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
		return nil, fmt.Errorf("非期望签名算法: %v", t.Header["alg"])
	}
	kid, _ := t.Header["kid"].(string)
	key, ok := v.keys[kid]
	if !ok {
		return nil, fmt.Errorf("未知 kid: %q", kid)
	}
	return key, nil
}
