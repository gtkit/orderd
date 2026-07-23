package interceptor

import (
	"context"

	"github.com/gtkit/orderd/internal/pkg/requestid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// requestIDHeader 是请求标识的 metadata 键(入站读取、响应回传共用)。
const requestIDHeader = "x-request-id"

// RequestIDUnary 从入站 metadata 读取 x-request-id(缺失则生成),注入 ctx 供
// 下游读取(requestid.FromContext),并通过响应 header 回传给调用方便于两侧关联。
func RequestIDUnary() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		rid := ""
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			if vals := md.Get(requestIDHeader); len(vals) > 0 {
				rid = vals[0]
			}
		}
		if rid == "" {
			rid = requestid.New()
		}
		ctx = requestid.NewContext(ctx, rid)
		// 回传失败(如非真实 server transport)不影响业务,忽略。
		_ = grpc.SetHeader(ctx, metadata.Pairs(requestIDHeader, rid))
		return handler(ctx, req)
	}
}
