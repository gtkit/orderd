// Command server 是订单 + 支付微服务入口。
//
// main 只做:解析 --config、加载配置、交给 bootstrap.Run。日志装配、启动/退出日志与
// 生命周期编排都在 bootstrap(对齐 pay360);Run 返回非 nil 即以非零码退出。
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/gtkit/orderd/internal/bootstrap"
	"github.com/gtkit/orderd/internal/platform/config"
)

func main() {
	cfgPath := flag.String("config", "config/env/env.yml", "配置文件路径")
	flag.Parse()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		// logger 尚未初始化,直接打印原因并退出。
		fmt.Fprintln(os.Stderr, "orderd: 加载配置失败:", err)
		os.Exit(1)
	}

	// 收到 SIGINT/SIGTERM 时取消 ctx,触发优雅关闭。
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if rerr := bootstrap.Run(ctx, cfg); rerr != nil {
		// 极早期(logger 装配前)错误 logger 可能未起,兜底打 stderr;其余 Run 已经记录。
		fmt.Fprintln(os.Stderr, "orderd:", rerr)
		os.Exit(1)
	}
}
