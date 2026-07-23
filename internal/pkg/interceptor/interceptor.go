// Package interceptor 提供 orderd 的 gRPC unary 拦截器(可观测性:requestid + accesslog)。
//
// 鉴权拦截器绑定 OrderService 的方法权限表,单独留在 module/order/transport/grpc;
// 本包只放与具体服务无关的通用横切。挂载顺序见 Observability。
package interceptor

import "google.golang.org/grpc"

// Observability 返回通用可观测性拦截器链(顺序:requestid → accesslog)。
// requestid 先行以便 accesslog 能读到 request_id;两者都应在鉴权/限流之前挂载,
// 保证被拒绝的请求也留痕并带同一 request_id。调用方再把鉴权拦截器追加在其后。
func Observability() []grpc.UnaryServerInterceptor {
	return []grpc.UnaryServerInterceptor{RequestIDUnary(), AccessLogUnary()}
}
