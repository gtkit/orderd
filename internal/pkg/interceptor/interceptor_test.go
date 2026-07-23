package interceptor

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/gtkit/logger"
	"github.com/gtkit/orderd/internal/pkg/requestid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestMain(m *testing.M) {
	_ = logger.New(logger.WithConsole(true), logger.WithFile(false), logger.WithLevel("error"))
	os.Exit(m.Run())
}

func TestRequestIDUnaryGeneratesWhenAbsent(t *testing.T) {
	var seen string
	h := func(ctx context.Context, _ any) (any, error) {
		seen = requestid.FromContext(ctx)
		return "ok", nil
	}
	_, err := RequestIDUnary()(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/x/Y"},
		func(ctx context.Context, req any) (any, error) { return h(ctx, req) })
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if seen == "" {
		t.Fatal("缺失 x-request-id 时应生成并注入 ctx")
	}
}

func TestRequestIDUnaryPropagatesIncoming(t *testing.T) {
	var seen string
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-request-id", "rid-abc"))
	_, err := RequestIDUnary()(ctx, nil, &grpc.UnaryServerInfo{FullMethod: "/x/Y"},
		func(ctx context.Context, _ any) (any, error) { seen = requestid.FromContext(ctx); return nil, nil })
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if seen != "rid-abc" {
		t.Fatalf("应透传入站 x-request-id,得 %q", seen)
	}
}

func TestAccessLogUnaryPassesThrough(t *testing.T) {
	// 成功与错误两条路径都应把 handler 结果原样返回(日志是副作用)。
	resp, err := AccessLogUnary()(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/order.v1.OrderService/CreateOrder"},
		func(context.Context, any) (any, error) { return "done", nil })
	if err != nil || resp != "done" {
		t.Fatalf("成功路径应原样返回: resp=%v err=%v", resp, err)
	}
	wantErr := status.Error(codes.InvalidArgument, "bad")
	_, err = AccessLogUnary()(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/order.v1.OrderService/CreateOrder"},
		func(context.Context, any) (any, error) { return nil, wantErr })
	if !errors.Is(err, wantErr) {
		t.Fatalf("错误路径应原样返回错误: %v", err)
	}
}

func TestAccessLogSkipsHealth(t *testing.T) {
	called := false
	_, _ = AccessLogUnary()(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/grpc.health.v1.Health/Check"},
		func(context.Context, any) (any, error) { called = true; return nil, nil })
	if !called {
		t.Fatal("健康检查方法应放行 handler")
	}
}

func TestObservabilityChainOrder(t *testing.T) {
	if got := Observability(); len(got) != 2 {
		t.Fatalf("应含 requestid + accesslog 两个拦截器,得 %d", len(got))
	}
}
