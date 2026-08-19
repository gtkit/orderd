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

// ctxWithClaims / claimsFromCtx 委托 auth 包的共享实现(AdminService transport 同读一个键)。
func ctxWithClaims(ctx context.Context, c auth.Claims) context.Context {
	return auth.ContextWithClaims(ctx, c)
}

func claimsFromCtx(ctx context.Context) (auth.Claims, bool) {
	return auth.ClaimsFromContext(ctx)
}

// methodPermission 方法级权限(DESIGN §13)。未登记的方法一律拒绝。
// AdminService 为运维契约:三个权限(ops:reconcile/event:requeue/ops:txn:read)不进接入方
// token 模板,发放需与 refund:execute 同级审批。
var methodPermission = map[string]string{
	"/order.v1.OrderService/CreateOrder":     "order:create",
	"/order.v1.OrderService/GetOrderStatus":  "order:read",
	"/order.v1.OrderService/GetOrder":        "order:read",
	"/order.v1.OrderService/CancelOrder":     "order:cancel",
	"/order.v1.OrderService/PreparePayment":  "order:create",
	"/order.v1.OrderService/RefundOrder":     "refund:execute",
	"/order.v1.OrderService/GetRefund":       "refund:read",
	"/order.v1.OrderService/ListOrderEvents": "event:read",

	"/order.v1.AdminService/ListReconcileDiffs":  "ops:reconcile",
	"/order.v1.AdminService/HandleReconcileDiff": "ops:reconcile",
	"/order.v1.AdminService/ListDeadLetters":     "event:requeue",
	"/order.v1.AdminService/RequeueDeadLetter":   "event:requeue",
	"/order.v1.AdminService/ListAnomalyTxns":     "ops:txn:read",
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
