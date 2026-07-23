package grpc

import (
	"context"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/gtkit/orderd/internal/pkg/auth"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

var itSecret = []byte("interceptor-secret-32bytes-xxxxxx")

func itVerifier(t *testing.T) auth.Verifier {
	t.Helper()
	v, err := auth.NewJWTVerifier(map[string][]byte{"k1": itSecret}, "bff", "orderd")
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func itToken(perms []string) string {
	c := jwt.MapClaims{
		"app_id": "app1", "user_id": 42, "perms": perms,
		"iss": "bff", "aud": "orderd", "exp": time.Now().Add(time.Hour).Unix(),
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, c)
	tok.Header["kid"] = "k1"
	s, _ := tok.SignedString(itSecret)
	return s
}

func bearerCtx(token string) context.Context {
	return metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+token))
}

func runInterceptor(t *testing.T, ctx context.Context, method string) (any, error) {
	t.Helper()
	inter := AuthInterceptor(itVerifier(t))
	info := &grpc.UnaryServerInfo{FullMethod: method}
	handler := func(ctx context.Context, _ any) (any, error) {
		c, ok := claimsFromCtx(ctx)
		if !ok {
			return nil, status.Error(codes.Internal, "handler 未拿到 claims")
		}
		return c.AppID, nil
	}
	return inter(ctx, nil, info, handler)
}

func TestInterceptorNoToken(t *testing.T) {
	_, err := runInterceptor(t, context.Background(), "/order.v1.OrderService/CreateOrder")
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("无 token 应 Unauthenticated,实际: %v", err)
	}
}

func TestInterceptorMissingPermission(t *testing.T) {
	// 持 order:read 却调 CreateOrder(需 order:create)→ PermissionDenied。
	ctx := bearerCtx(itToken([]string{"order:read"}))
	_, err := runInterceptor(t, ctx, "/order.v1.OrderService/CreateOrder")
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("缺权限应 PermissionDenied,实际: %v", err)
	}
}

func TestInterceptorAllowsAndInjectsClaims(t *testing.T) {
	ctx := bearerCtx(itToken([]string{"order:create"}))
	out, err := runInterceptor(t, ctx, "/order.v1.OrderService/CreateOrder")
	if err != nil {
		t.Fatalf("有效 token+权限应放行: %v", err)
	}
	if out != "app1" {
		t.Fatalf("handler 应拿到注入的 claims(app_id=app1),实际: %v", out)
	}
}

func TestInterceptorUnknownMethod(t *testing.T) {
	ctx := bearerCtx(itToken([]string{"order:create"}))
	_, err := runInterceptor(t, ctx, "/order.v1.OrderService/Unknown")
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("未登记方法应拒绝,实际: %v", err)
	}
}

func TestInterceptorBadToken(t *testing.T) {
	_, err := runInterceptor(t, bearerCtx("garbage.token.here"), "/order.v1.OrderService/GetOrderStatus")
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("非法 token 应 Unauthenticated,实际: %v", err)
	}
}
