package interceptor

import (
	"context"
	"strings"
	"time"

	"github.com/gtkit/logger"
	"github.com/gtkit/orderd/internal/pkg/requestid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

// healthServicePrefix 是标准健康检查服务的方法前缀:探针无凭据且高频,访问日志豁免防刷屏。
const healthServicePrefix = "/grpc.health.v1.Health/"

// AccessLogUnary 为每个 unary RPC 输出一条结构化访问日志:
// method / code / duration_ms / request_id,失败附加 error 并升为 Warn。
// 不记录请求体或响应体;健康检查方法豁免。挂载须在鉴权/限流之前,保证被拒请求同样留痕。
func AccessLogUnary() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		method := ""
		if info != nil {
			method = info.FullMethod
		}
		if strings.HasPrefix(method, healthServicePrefix) {
			return handler(ctx, req)
		}

		start := time.Now()
		resp, err := handler(ctx, req)

		fields := []any{
			"method", method,
			"code", status.Code(err).String(),
			"duration_ms", float64(time.Since(start).Microseconds()) / 1e3,
			"request_id", requestid.FromContext(ctx),
		}
		if err != nil {
			logger.Warnw("grpc access", append(fields, "error", err.Error())...)
		} else {
			logger.Infow("grpc access", fields...)
		}
		return resp, err
	}
}
