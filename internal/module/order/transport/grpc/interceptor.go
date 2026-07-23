package grpc

import (
	"context"
	"strings"

	"github.com/gtkit/orderd/internal/pkg/auth"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// claimsKey 是 context 中已校验 Claims 的键。
type claimsKey struct{}

// ctxWithClaims 把已校验身份放入 context(拦截器 + 测试用)。
func ctxWithClaims(ctx context.Context, c auth.Claims) context.Context {
	return context.WithValue(ctx, claimsKey{}, c)
}

func claimsFromCtx(ctx context.Context) (auth.Claims, bool) {
	c, ok := ctx.Value(claimsKey{}).(auth.Claims)
	return c, ok
}

// methodPermission 方法级权限(DESIGN §13)。未登记的方法一律拒绝。
var methodPermission = map[string]string{
	"/order.v1.OrderService/CreateOrder":     "order:create",
	"/order.v1.OrderService/GetOrderStatus":  "order:read",
	"/order.v1.OrderService/CancelOrder":     "order:cancel",
	"/order.v1.OrderService/PreparePayment":  "order:create",
	"/order.v1.OrderService/RefundOrder":     "refund:execute",
	"/order.v1.OrderService/GetRefund":       "refund:read",
	"/order.v1.OrderService/ListOrderEvents": "event:read",
}

// AuthInterceptor 返回鉴权 unary 拦截器:校验 Bearer token → Claims 入 context → 方法级权限门禁。
func AuthInterceptor(v auth.Verifier) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		perm, ok := methodPermission[info.FullMethod]
		if !ok {
			return nil, status.Error(codes.PermissionDenied, "方法未授权")
		}
		token, err := bearerToken(ctx)
		if err != nil {
			return nil, err
		}
		claims, err := v.Verify(ctx, token)
		if err != nil {
			return nil, status.Error(codes.Unauthenticated, "token 校验失败")
		}
		if !claims.Has(perm) {
			return nil, status.Errorf(codes.PermissionDenied, "缺少权限 %s", perm)
		}
		return handler(ctxWithClaims(ctx, claims), req)
	}
}

// bearerToken 从 metadata 的 authorization 头提取 Bearer token。
func bearerToken(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", status.Error(codes.Unauthenticated, "缺少鉴权元数据")
	}
	vals := md.Get("authorization")
	if len(vals) == 0 {
		return "", status.Error(codes.Unauthenticated, "缺少 authorization")
	}
	const p = "Bearer "
	if !strings.HasPrefix(vals[0], p) {
		return "", status.Error(codes.Unauthenticated, "authorization 须为 Bearer")
	}
	return strings.TrimSpace(vals[0][len(p):]), nil
}
