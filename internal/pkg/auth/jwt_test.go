package auth

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const (
	testKID    = "k1"
	testIssuer = "bff"
	testAud    = "orderd"
)

var testSecret = []byte("test-secret-32bytes-xxxxxxxxxxxx")

type tokenOpts struct {
	kid      string
	secret   []byte
	issuer   string
	audience string
	appID    string
	userID   int64
	perms    []string
	exp      time.Time
}

func signToken(o tokenOpts) string {
	c := jwtClaims{
		AppID: o.appID, UserID: o.userID, Perms: o.perms,
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    o.issuer,
			Audience:  jwt.ClaimStrings{o.audience},
			ExpiresAt: jwt.NewNumericDate(o.exp),
		},
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, c)
	tok.Header["kid"] = o.kid
	s, _ := tok.SignedString(o.secret)
	return s
}

func goodOpts() tokenOpts {
	return tokenOpts{
		kid: testKID, secret: testSecret, issuer: testIssuer, audience: testAud,
		appID: "app1", userID: 42, perms: []string{"order:create", "order:read"},
		exp: time.Now().Add(time.Hour),
	}
}

func newVerifier(t *testing.T) *JWTVerifier {
	t.Helper()
	v, err := NewJWTVerifier(map[string][]byte{testKID: testSecret}, testIssuer, testAud)
	if err != nil {
		t.Fatalf("构造 verifier: %v", err)
	}
	return v
}

func TestJWTValid(t *testing.T) {
	v := newVerifier(t)
	c, err := v.Verify(context.Background(), signToken(goodOpts()))
	if err != nil {
		t.Fatalf("有效 token 应通过: %v", err)
	}
	if c.AppID != "app1" || c.UserID != 42 || !c.Has("order:create") || c.Has("refund:execute") {
		t.Fatalf("claims 解析错误: %+v", c)
	}
}

func TestJWTRejects(t *testing.T) {
	v := newVerifier(t)
	cases := []struct {
		name string
		opts tokenOpts
	}{
		{"过期", func() tokenOpts { o := goodOpts(); o.exp = time.Now().Add(-time.Hour); return o }()},
		{"错误 audience", func() tokenOpts { o := goodOpts(); o.audience = "other"; return o }()},
		{"错误 issuer", func() tokenOpts { o := goodOpts(); o.issuer = "evil"; return o }()},
		{"错误签名密钥", func() tokenOpts { o := goodOpts(); o.secret = []byte("wrong-secret-32bytes-yyyyyyyyyyyy"); return o }()},
		{"未知 kid", func() tokenOpts { o := goodOpts(); o.kid = "unknown"; return o }()},
		{"缺 app_id", func() tokenOpts { o := goodOpts(); o.appID = ""; return o }()},
		{"缺 user_id", func() tokenOpts { o := goodOpts(); o.userID = 0; return o }()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := v.Verify(context.Background(), signToken(tc.opts))
			if !errors.Is(err, ErrUnauthenticated) {
				t.Fatalf("应拒绝(ErrUnauthenticated),实际: %v", err)
			}
		})
	}
}

func TestJWTNoExpiryRejected(t *testing.T) {
	v := newVerifier(t)
	// 不设 exp → WithExpirationRequired 应拒。
	c := jwtClaims{AppID: "app1", UserID: 42, RegisteredClaims: jwt.RegisteredClaims{Issuer: testIssuer, Audience: jwt.ClaimStrings{testAud}}}
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, c)
	tok.Header["kid"] = testKID
	s, _ := tok.SignedString(testSecret)
	if _, err := v.Verify(context.Background(), s); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("缺 exp 应拒: %v", err)
	}
}

func TestJWTRotation(t *testing.T) {
	// 两把密钥并存(轮换):新旧 kid 都应验证通过。
	v, _ := NewJWTVerifier(map[string][]byte{"k1": testSecret, "k0": []byte("old-secret-32bytes-zzzzzzzzzzzzzz")}, testIssuer, testAud)
	o := goodOpts()
	o.kid, o.secret = "k0", []byte("old-secret-32bytes-zzzzzzzzzzzzzz")
	if _, err := v.Verify(context.Background(), signToken(o)); err != nil {
		t.Fatalf("轮换旧 kid 应通过: %v", err)
	}
}
