// Package gormstore 是 store.Store 的 gtkit/ormx + gorm 实现(替代旧 raw database/sql 实现)。
//
// 事务模型(不变,忠实资金主链语义):
//   - WithinTx 经 ormx.Client.WithTx 开事务,并用 dbtx 把 *gorm.DB 放进 context;
//     事务内各 repo 经 dbtx.DB(ctx, base) 复用该事务;行锁用 gorm Clauses(FOR UPDATE)。
//   - Reader() 走非事务连接(ctx 无事务 → dbtx.DB 回退 base)。
//   - 唯一键冲突依赖 ormx 的 TranslateError → gorm.ErrDuplicatedKey,统一翻译为 store.ErrDuplicate。
//
// 同一 *repos 同时服务事务内与只读:事务与否由 context 决定,不在构造期绑定连接。
package gormstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/gtkit/orderd/internal/pkg/dbtx"
	"github.com/gtkit/orderd/internal/store"
	"github.com/gtkit/ormx"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// 编译期保证:*Store 实现 store.Store 端口(端口不变,仅换实现)。
var _ store.Store = (*Store)(nil)

// Store 是 gorm 持久化根,持有 ormx.Client 生命周期。
type Store struct {
	cli   *ormx.Client
	repos *repos
}

// New 用已建立的 ormx.Client 构造 Store。
func New(cli *ormx.Client) *Store {
	return &Store{cli: cli, repos: &repos{base: cli.DB()}}
}

// Client 暴露底层 ormx.Client(健康检查/生命周期用)。
func (s *Store) Client() *ormx.Client { return s.cli }

// DB 暴露底层 *gorm.DB(只读解析器/健康检查用)。
func (s *Store) DB() *gorm.DB { return s.cli.DB() }

// Close 关闭连接池。
func (s *Store) Close() error { return s.cli.Close() }

// WithinTx 在单事务内执行 fn;已在事务中则复用(不嵌套),保证聚合单一事务边界。
func (s *Store) WithinTx(ctx context.Context, fn func(ctx context.Context, r store.Repos) error) error {
	if dbtx.InTx(ctx) {
		return fn(ctx, s.repos)
	}
	return s.cli.WithTx(ctx, &sql.TxOptions{}, func(tx *gorm.DB) error {
		return fn(dbtx.With(ctx, tx), s.repos)
	})
}

// Reader 返回非事务只读仓储。
func (s *Store) Reader() store.Repos { return s.repos }

// repos 在 base *gorm.DB 上实现 store.Repos;实际连接(事务/非事务)按 ctx 决定。
type repos struct{ base *gorm.DB }

func (r *repos) Orders() store.OrderRepo             { return orderRepo{r.base} }
func (r *repos) Attempts() store.AttemptRepo         { return attemptRepo{r.base} }
func (r *repos) Refunds() store.RefundRepo           { return refundRepo{r.base} }
func (r *repos) Transactions() store.TransactionRepo { return txnRepo{r.base} }
func (r *repos) Events() store.EventRepo             { return eventRepo{r.base} }
func (r *repos) Outbox() store.OutboxRepo            { return outboxRepo{r.base} }
func (r *repos) Reconcile() store.ReconcileRepo      { return reconcileRepo{r.base} }
func (r *repos) Logs() store.LogRepo                 { return logRepo{r.base} }
func (r *repos) OpsAudit() store.OpsAuditRepo        { return opsAuditRepo{r.base} }

// dbOf 取 ctx 事务或回退 base(所有数据访问统一经此,不绕过事务上下文)。
func dbOf(ctx context.Context, base *gorm.DB) *gorm.DB { return dbtx.DB(ctx, base) }

// forUpdate 按需附加行锁(FOR UPDATE),仅事务内有意义。
func forUpdate(g *gorm.DB, fu bool) *gorm.DB {
	if fu {
		return g.Clauses(clause.Locking{Strength: "UPDATE"})
	}
	return g
}

// mapDup 把唯一键冲突(gorm.ErrDuplicatedKey)翻译为端口 sentinel;其余原样带上下文返回。
func mapDup(err error, what string) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, gorm.ErrDuplicatedKey) {
		return store.ErrDuplicate
	}
	return fmt.Errorf("gormstore: %s: %w", what, err)
}

// skipLocked 附加 FOR UPDATE SKIP LOCKED(多副本认领待办行不重复、不阻塞)。
func skipLocked(g *gorm.DB) *gorm.DB {
	return g.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"})
}

// nullTime 构造有效的 sql.NullTime。
func nullTime(t time.Time) sql.NullTime { return sql.NullTime{Time: t, Valid: true} }

// claimLease 给认领到的行批量打租约(locked_by/locked_until),单条 UPDATE ... WHERE id IN。
func claimLease(ctx context.Context, base *gorm.DB, model any, idlist []uint64, workerID string, leaseUntil time.Time) error {
	if len(idlist) == 0 {
		return nil
	}
	err := dbOf(ctx, base).Model(model).Where("id IN ?", idlist).
		Updates(map[string]any{"locked_by": workerID, "locked_until": leaseUntil}).Error
	if err != nil {
		return fmt.Errorf("gormstore: claim lease: %w", err)
	}
	return nil
}
