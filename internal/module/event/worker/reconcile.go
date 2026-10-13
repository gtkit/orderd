package worker

import (
	"context"
	"time"

	"github.com/gtkit/logger"
	reconcileapp "github.com/gtkit/orderd/internal/module/reconcile/application"
	"github.com/gtkit/orderd/internal/shared/account"
	"github.com/gtkit/orderd/internal/shared/bill"
)

// ReconcileWorker 枚举启用账号,有界补齐缺失账期并恢复未完成对账。
type ReconcileWorker struct {
	svc      *reconcileapp.Service
	lister   account.Lister
	interval time.Duration
	clock    func() time.Time
}

// NewReconcileWorker 构造对账 worker。interval 默认一分钟(测试可调小)。
func NewReconcileWorker(svc *reconcileapp.Service, lister account.Lister, interval time.Duration, clock func() time.Time) *ReconcileWorker {
	if interval == 0 {
		interval = time.Minute
	}
	if clock == nil {
		clock = time.Now
	}
	return &ReconcileWorker{svc: svc, lister: lister, interval: interval, clock: clock}
}

// RunOnce 对启用账号推进历史游标并处理到期批次;返回无错误完成本轮的账号数。
func (w *ReconcileWorker) RunOnce(ctx context.Context) (int, error) {
	accounts, err := w.lister.List(ctx)
	if err != nil {
		return 0, err
	}
	date := yesterday(w.clock())
	n := 0
	for _, a := range accounts {
		if err := ctx.Err(); err != nil {
			return n, err
		}
		if _, rerr := w.svc.RecoverAccount(ctx, a, date); rerr != nil {
			logger.Warnw("对账失败", "pay_account_id", a.PayAccountID, "bill_date", date.Format("2006-01-02"), "err", rerr)
			continue
		}
		n++
	}
	return n, nil
}

// yesterday 返回 t 的前一天零点(账期粒度到天)。
func yesterday(t time.Time) time.Time {
	d := t.In(bill.Location).AddDate(0, 0, -1)
	return time.Date(d.Year(), d.Month(), d.Day(), 0, 0, 0, 0, d.Location())
}
