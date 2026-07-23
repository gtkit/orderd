package grpc

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// caller 是经鉴权的调用方身份(DESIGN §13:app_id/user_id 来自 token claims,非请求参数)。
type caller struct {
	AppID  string
	UserID int64
}

// authFromContext 读取【AuthInterceptor 已校验并注入 context】的身份。
// token 校验、密钥轮换、方法级权限均在拦截器完成;此处只取身份,不再信任请求参数。
func authFromContext(ctx context.Context) (caller, error) {
	c, ok := claimsFromCtx(ctx)
	if !ok {
		return caller{}, status.Error(codes.Unauthenticated, "未鉴权")
	}
	return caller{AppID: c.AppID, UserID: c.UserID}, nil
}
