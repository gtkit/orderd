// Package dbtx 提供 context 事务传递:gormstore.Store.WithinTx 把事务 *gorm.DB 放进 context,
// 同一事务内的多个 repo 经 DB(ctx, fallback) 复用它;无事务时回退到默认连接。
//
// 依赖方向:基础设施型包(import gorm),仅 repository / runtime/module / platform 可 import。
package dbtx

import (
	"context"

	"gorm.io/gorm"
)

type ctxKey struct{}

// With 把事务 tx 绑定到 ctx。
func With(ctx context.Context, tx *gorm.DB) context.Context {
	return context.WithValue(ctx, ctxKey{}, tx)
}

// DB 取 ctx 中的事务;无事务则返回 fallback(默认连接池)。
// repo 的数据访问统一经此函数,不得绕过直接用裸 *gorm.DB。
func DB(ctx context.Context, fallback *gorm.DB) *gorm.DB {
	if tx, ok := ctx.Value(ctxKey{}).(*gorm.DB); ok && tx != nil {
		return tx.WithContext(ctx)
	}
	return fallback.WithContext(ctx)
}

// InTx 报告 ctx 当前是否已处于事务中。
func InTx(ctx context.Context) bool {
	tx, ok := ctx.Value(ctxKey{}).(*gorm.DB)
	return ok && tx != nil
}
