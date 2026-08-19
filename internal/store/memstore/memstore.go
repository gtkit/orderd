// Package memstore 是 store.Store 的内存实现,供应用层单测(并发/幂等矩阵)。
//
// 忠实度:txMu 串行化事务 = 模拟"同订单行 FOR UPDATE + 单事务";每次访问用 mu 保护 map;
// 事务开始快照、失败回滚 = 模拟事务原子性;各 Insert 复刻 DDL 的唯一约束(含活跃唯一守卫)。
// 值语义存储(存副本、返副本),避免调用方持有内部指针误改状态。
package memstore

import (
	"context"
	"maps"
	"sync"

	"github.com/gtkit/orderd/internal/models"
	"github.com/gtkit/orderd/internal/store"
)

type data struct {
	orders   map[uint64]models.Order
	attempts map[uint64]models.PaymentAttempt
	refunds  map[uint64]models.Refund
	txns     map[uint64]models.PaymentTransaction
	batches  map[uint64]models.ReconcileBatch
	events   []models.OrderEvent
	outbox   []models.EventOutbox
	logs     []models.OrderLog
	diffs    []models.ReconcileDiff
	audits   []models.OpsAuditLog
	seq      uint64
}

func newData() *data {
	return &data{
		orders:   map[uint64]models.Order{},
		attempts: map[uint64]models.PaymentAttempt{},
		refunds:  map[uint64]models.Refund{},
		txns:     map[uint64]models.PaymentTransaction{},
		batches:  map[uint64]models.ReconcileBatch{},
	}
}

func (d *data) clone() *data {
	n := &data{
		orders:   make(map[uint64]models.Order, len(d.orders)),
		attempts: make(map[uint64]models.PaymentAttempt, len(d.attempts)),
		refunds:  make(map[uint64]models.Refund, len(d.refunds)),
		txns:     make(map[uint64]models.PaymentTransaction, len(d.txns)),
		batches:  make(map[uint64]models.ReconcileBatch, len(d.batches)),
		events:   append([]models.OrderEvent(nil), d.events...),
		outbox:   append([]models.EventOutbox(nil), d.outbox...),
		logs:     append([]models.OrderLog(nil), d.logs...),
		diffs:    append([]models.ReconcileDiff(nil), d.diffs...),
		audits:   append([]models.OpsAuditLog(nil), d.audits...),
		seq:      d.seq,
	}
	maps.Copy(n.orders, d.orders)
	maps.Copy(n.attempts, d.attempts)
	maps.Copy(n.refunds, d.refunds)
	maps.Copy(n.txns, d.txns)
	maps.Copy(n.batches, d.batches)
	return n
}

// Mem 是 store.Store 的内存实现。
type Mem struct {
	txMu sync.Mutex // 串行化事务(模拟同行锁 + 事务隔离)
	mu   sync.Mutex // 保护 d 的每次访问
	d    *data
}

// New 返回空的内存 store。
func New() *Mem { return &Mem{d: newData()} }

// WithinTx 串行执行事务:开始快照,fn 出错回滚。
func (m *Mem) WithinTx(ctx context.Context, fn func(ctx context.Context, r store.Repos) error) error {
	m.txMu.Lock()
	defer m.txMu.Unlock()

	m.mu.Lock()
	snap := m.d.clone()
	m.mu.Unlock()

	if err := fn(ctx, &repos{m: m}); err != nil {
		m.mu.Lock()
		m.d = snap
		m.mu.Unlock()
		return err
	}
	return nil
}

// Reader 返回非事务只读入口(读已提交)。
func (m *Mem) Reader() store.Repos { return &repos{m: m} }

// repos 在 Mem 上实现 store.Repos;每个方法自行短暂持有 m.mu。
type repos struct{ m *Mem }

func (r *repos) Orders() store.OrderRepo             { return orderRepo{r.m} }
func (r *repos) Attempts() store.AttemptRepo         { return attemptRepo{r.m} }
func (r *repos) Refunds() store.RefundRepo           { return refundRepo{r.m} }
func (r *repos) Transactions() store.TransactionRepo { return txnRepo{r.m} }
func (r *repos) Events() store.EventRepo             { return eventRepo{r.m} }
func (r *repos) Outbox() store.OutboxRepo            { return outboxRepo{r.m} }
func (r *repos) Reconcile() store.ReconcileRepo      { return reconcileRepo{r.m} }
func (r *repos) Logs() store.LogRepo                 { return logRepo{r.m} }
func (r *repos) OpsAudit() store.OpsAuditRepo        { return opsAuditRepo{r.m} }
