// Package data 管理基础设施连接(DB 等),不承载业务 DAO。
package data

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/gtkit/logger"
	"github.com/gtkit/orderd/internal/pkg/dbtx"
	"github.com/gtkit/orderd/internal/platform/config"
	"github.com/gtkit/ormx"
	"github.com/gtkit/ormx/zlogger"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
)

// NewMySQL 经 gtkit/ormx 建立 MySQL 连接。
//
// 要点(对齐 pay360):
//   - WithTranslateError:唯一键冲突翻译为 gorm.ErrDuplicatedKey(仓储据此识别冲突,不看 1062);
//   - ormx 默认 ParseTime 开、时区 Local、字符集 utf8mb4,与旧手拼 DSN 等价;
//   - StartupPing 默认开:启动即验证连通(fail-fast);
//   - 池参数在此显式设默认(MaxOpen 50 / MaxIdle≤MaxOpen / Lifetime 1h),不依赖包内默认避免漂移。
//
// 返回 *ormx.Client:调用方持有生命周期(Close/HealthCheck),业务层只消费 Client.DB()。
func NewMySQL(ctx context.Context, cfg config.Config) (*ormx.Client, error) {
	maxOpen := cfg.MySQLMaxOpen
	if maxOpen <= 0 {
		maxOpen = 50
	}
	maxIdle := cfg.MySQLMaxIdle
	if maxIdle <= 0 || maxIdle > maxOpen {
		maxIdle = maxOpen
	}
	lifetime := time.Duration(cfg.MySQLMaxLifeSec) * time.Second
	if lifetime <= 0 {
		lifetime = time.Hour
	}

	// SQL 日志按 log.sql 决定:开=Info(打全部 SQL),关=Silent(不打);
	// 慢查询阈值沿用 zlogger 默认 200ms,RecordNotFound 不作为错误输出。
	sqlLevel := gormlogger.Silent
	if cfg.Log.SQL {
		sqlLevel = gormlogger.Info
	}
	ormxLog := zlogger.New(
		zlogger.WithLogger(logger.Zap()),
		zlogger.WithLogLevel(sqlLevel),
		zlogger.WithIgnoreRecordNotFoundError(true),
	)
	cli, err := ormx.Open(ctx,
		ormx.WithHost(cfg.MySQLHost),
		ormx.WithPort(cfg.MySQLPort),
		ormx.WithUser(cfg.MySQLUser),
		ormx.WithPassword(cfg.MySQLPassword),
		ormx.WithDatabase(cfg.MySQLDBName),
		ormx.WithTranslateError(true),
		ormx.WithMaxOpenConns(maxOpen),
		ormx.WithMaxIdleConns(maxIdle),
		ormx.WithConnMaxLifetime(lifetime),
		ormx.WithGormLogger(ormxLog),
	)
	if err != nil {
		return nil, fmt.Errorf("data: open mysql via ormx: %w", err)
	}
	return cli, nil
}

// TxManager 是 application 侧跨 repo 事务端口的 gorm 实现:基于 ormx.Client.WithTx
// 开启事务并经 dbtx 放入 context,同一事务内的 repo 用 dbtx.DB(ctx, ...) 复用它。
// *gorm.DB 不出本包/repository 侧。
type TxManager struct{ cli *ormx.Client }

// NewTxManager 构造事务管理器。
func NewTxManager(cli *ormx.Client) *TxManager { return &TxManager{cli: cli} }

// Do 在一个数据库事务内执行 fn:fn 返回 nil 提交,返回 error 回滚。
// 已在事务中则直接复用(不嵌套开启),保证聚合编排的单一事务边界。
func (m *TxManager) Do(ctx context.Context, fn func(ctx context.Context) error) error {
	if dbtx.InTx(ctx) {
		return fn(ctx)
	}
	return m.cli.WithTx(ctx, &sql.TxOptions{}, func(tx *gorm.DB) error {
		return fn(dbtx.With(ctx, tx))
	})
}
