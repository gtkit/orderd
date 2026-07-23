package bootstrap

import (
	"github.com/gtkit/logger"
	"github.com/gtkit/orderd/internal/platform/config"
)

// initLogger 依配置初始化 gtkit/logger。对齐 pay360:日志装配在 bootstrap,而非 main。
//
// 级别优先配置文件,其次按 env 推导;控制台/文件都关则回退控制台避免日志无落点;
// 文件轮转参数仅在文件输出且 >0 时传(gtkit/logger 要求 maxsize>0,0 值走库默认)。
func initLogger(cfg config.Config) error {
	level := cfg.Log.Level
	if level == "" {
		level = "info"
		if cfg.Env == "dev" {
			level = "debug"
		}
	}
	console, file := cfg.Log.Console, cfg.Log.File
	if !console && !file {
		console = true
	}
	opts := []logger.Option{
		logger.WithLevel(level),
		logger.WithConsole(console),
		logger.WithFile(file),
	}
	if file {
		opts = append(opts, logger.WithCompress(cfg.Log.Compress))
		if cfg.Log.Path != "" {
			opts = append(opts, logger.WithPath(cfg.Log.Path))
		}
		if cfg.Log.MaxSize > 0 {
			opts = append(opts, logger.WithMaxSize(cfg.Log.MaxSize))
		}
		if cfg.Log.MaxAge > 0 {
			opts = append(opts, logger.WithMaxAge(cfg.Log.MaxAge))
		}
		if cfg.Log.MaxBackups > 0 {
			opts = append(opts, logger.WithMaxBackups(cfg.Log.MaxBackups))
		}
	}
	return logger.New(opts...)
}
