// Package bootstrap 编排进程生命周期:装配 App → 起 gRPC/HTTP/worker → 优雅关闭。
package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/gtkit/logger"
	"github.com/gtkit/orderd/internal/appmeta"
	orderv1gw "github.com/gtkit/orderd/internal/gateway/order/v1"
	"github.com/gtkit/orderd/internal/pkg/interceptor"
	"github.com/gtkit/orderd/internal/platform/config"
	orderruntime "github.com/gtkit/orderd/internal/runtime"
	orderv1 "github.com/gtkit/orderd/pb/order/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// 事件分页最坏情况(1000 条 × 8192B ≈ 8MB)可能超过 gRPC 默认 4MB 上限(评审补充);
// 放宽发送上限到 16MB 作为兜底,应用层仍应按累计字节数二次截断分页。
const maxSendMsgSize = 16 << 20

// newRESTGateway 构造 grpc-gateway REST 反向代理,经本地 gRPC 回环拨号(复用拦截器/校验)。
// 默认 header matcher 会把 Authorization 前缀化为 grpcgateway-authorization,鉴权拦截器读不到,
// 故显式把 authorization 透传为同名 metadata;其余头沿用默认 matcher。
func newRESTGateway(ctx context.Context, grpcAddr string) (*runtime.ServeMux, error) {
	dialAddr := grpcAddr
	if host, port, serr := net.SplitHostPort(dialAddr); serr == nil && (host == "" || host == "0.0.0.0" || host == "::") {
		dialAddr = net.JoinHostPort("127.0.0.1", port)
	}
	gwMux := runtime.NewServeMux(runtime.WithIncomingHeaderMatcher(func(key string) (string, bool) {
		if strings.EqualFold(key, "authorization") {
			return "authorization", true
		}
		return runtime.DefaultHeaderMatcher(key)
	}))
	if err := orderv1gw.RegisterOrderServiceHandlerFromEndpoint(ctx, gwMux, dialAddr,
		[]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}); err != nil {
		return nil, err
	}
	return gwMux, nil
}

// Run 初始化日志、装配并运行服务,直到 ctx 取消后优雅关闭。
// 日志装配是第一步(对齐 pay360);其后的失败都经 logger 记录,main 只负责退出码。
func Run(ctx context.Context, cfg config.Config) error {
	if err := initLogger(cfg); err != nil {
		return fmt.Errorf("init logger: %w", err) // logger 未起,由 main 兜底打 stderr
	}
	defer logger.Sync()
	logger.Infow("orderd 启动", "service", appmeta.ServiceName, "env", cfg.Env, "machine_id", cfg.MachineID)

	app, err := orderruntime.Build(ctx, cfg)
	if err != nil {
		logger.Errorw("orderd 装配失败", "err", err)
		return err
	}
	defer func() { _ = app.Close() }()

	// gRPC。
	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		logger.Errorw("gRPC 监听失败", "addr", cfg.GRPCAddr, "err", err)
		return err
	}
	// 拦截器链:requestid → accesslog → auth(可观测性在前,保证被拒请求也留痕并带同一 request_id)。
	chain := append(interceptor.Observability(), app.Interceptor)
	gs := grpc.NewServer(
		grpc.MaxSendMsgSize(maxSendMsgSize),
		grpc.ChainUnaryInterceptor(chain...),
	)
	orderv1.RegisterOrderServiceServer(gs, app.GRPC)
	// AdminService 只注册到 gRPC(:9090),绝不挂 grpc-gateway——运维契约与接入方 REST/回调面隔离。
	orderv1.RegisterAdminServiceServer(gs, app.AdminGRPC)
	go func() {
		logger.Infow("gRPC 启动", "addr", cfg.GRPCAddr)
		if serr := gs.Serve(lis); serr != nil && !errors.Is(serr, grpc.ErrServerStopped) {
			logger.Errorw("gRPC Serve 退出", "err", serr)
		}
	}()

	// HTTP(支付回调 + 健康检查 + REST 反向代理)。
	mux := http.NewServeMux()
	app.HTTP.Register(mux)

	// REST(grpc-gateway):经本地 gRPC 回环拨号转发,复用同一套鉴权拦截器与 protovalidate 校验,
	// 不旁路业务;身份仍走 authorization 头(JWT),不在 path/query 暴露。挂 /v1/ 前缀,与回调 /notify 分离。
	gwMux, err := newRESTGateway(ctx, cfg.GRPCAddr)
	if err != nil {
		logger.Errorw("REST 网关装配失败", "err", err)
		return err
	}
	mux.Handle("/v1/", gwMux)
	// 积压指标(纯计数/年龄,无敏感数据);来源限制属入口层(防火墙/网关)职责。
	mux.Handle("GET /metrics", app.Metrics)

	// 超时齐全防慢速 DoS(slowloris):/notify 无鉴权且每请求触发验签,慢 body 会占住连接与 CPU。
	hs := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	go func() {
		logger.Infow("HTTP 启动", "addr", cfg.HTTPAddr)
		if serr := hs.ListenAndServe(); serr != nil && !errors.Is(serr, http.ErrServerClosed) {
			logger.Errorw("HTTP ListenAndServe 退出", "err", serr)
		}
	}()

	app.Workers.Start(ctx)

	<-ctx.Done()
	logger.Info("收到关闭信号,开始优雅关闭")

	app.Workers.Stop()
	gs.GracefulStop()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if serr := hs.Shutdown(shutdownCtx); serr != nil {
		logger.Errorw("HTTP 关闭超时", "err", serr)
	}
	logger.Info("orderd 已退出")
	return nil
}
