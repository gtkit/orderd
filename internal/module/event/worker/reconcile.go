package worker

import (
	"context"
	"time"

	"github.com/gtkit/logger"
	reconcileapp "github.com/gtkit/orderd/internal/module/reconcile/application"
	"github.com/gtkit/orderd/internal/shared/account"
)

// ReconcileWorker 每日枚举启用账号,对前一账期执行渠道账单对账(DESIGN §14)。
type ReconcileWorker struct {
	svc      *reconcileapp.Service
	lister   account.Lister
	interval time.Duration
	clock    func() time.Time
}

// NewReconcileWorker 构造对账 worker。interval 一般为一天(测试可调小)。
func NewReconcileWorker(svc *reconcileapp.Service, lister account.Lister, interval time.Duration, clock func() time.Time) *ReconcileWorker {
	if interval == 0 {
		interval = 24 * time.Hour
	}
	if clock == nil {
		clock = time.Now
	}
	return &ReconcileWorker{svc: svc, lister: lister, interval: interval, clock: clock}
}

// RunOnce 对全部启用账号执行"前一账期"对账;返回处理账号数。
func (w *ReconcileWorker) RunOnce(ctx context.Context) (int, error) {
	accounts, err := w.lister.List(ctx)
	if err != nil {
		return 0, err
	}
	date := yesterday(w.clock())
	n := 0
	for _, a := range accounts {
		if _, rerr := w.svc.Reconcile(ctx, a.AppID, a.Channel, a.PayAccountID, date); rerr != nil {
			logger.Warnw("对账失败", "pay_account_id", a.PayAccountID, "bill_date", date.Format("2006-01-02"), "err", rerr)
			continue
		}
		n++
	}
	return n, nil
}

// yesterday 返回 t 的前一天零点(账期粒度到天)。
func yesterday(t time.Time) time.Time {
	d := t.AddDate(0, 0, -1)
	return time.Date(d.Year(), d.Month(), d.Day(), 0, 0, 0, 0, d.Location())
}
