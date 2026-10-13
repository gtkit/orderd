package worker

import (
	"context"
	"testing"
	"time"

	"github.com/gtkit/orderd/internal/models"
	reconcileapp "github.com/gtkit/orderd/internal/module/reconcile/application"
	"github.com/gtkit/orderd/internal/pkg/sqlutil"
	"github.com/gtkit/orderd/internal/shared/bill"
	"github.com/gtkit/orderd/internal/store"
	"github.com/gtkit/orderd/internal/store/memstore"
)

type fakeLister struct{ accts []models.AppPayAccount }

func (f fakeLister) List(context.Context) ([]models.AppPayAccount, error) { return f.accts, nil }

type fakeBillSrc struct{ b bill.Bill }

func (f fakeBillSrc) Fetch(_ context.Context, _, _ string, _ time.Time, emit func(bill.Entry) error) (bill.Summary, error) {
	var summary bill.Summary
	for _, e := range f.b.Entries {
		e.Kind = models.TxnPay
		e.Currency = "CNY"
		e.Status = "SUCCESS"
		if err := emit(e); err != nil {
			return summary, err
		}
		summary.Count++
		summary.PayAmount += e.Amount
	}
	return summary, nil
}

func TestReconcileWorkerRunOnce(t *testing.T) {
	mem := memstore.New()
	now := time.Date(2026, 7, 17, 3, 0, 0, 0, time.UTC)
	day := yesterday(now) // 2026-07-16
	mem.SeedAccount(models.AppPayAccount{AppID: "app1", Channel: "wechat", PayAccountID: "acc-wechat", CreatedAt: day, Enabled: true})

	// 本地一笔支付流水落在前一账期。
	if err := mem.WithinTx(bg, func(ctx context.Context, r store.Repos) error {
		return r.Transactions().Insert(ctx, &models.PaymentTransaction{
			AppID: "app1", OrderNo: "OD1", PayAccountID: "acc-wechat", Channel: "wechat", Kind: models.TxnPay,
			OutTradeNo: "o1", TransactionID: "t1", Amount: 90, Currency: "CNY",
			PaidAt: sqlutil.Time(day.Add(9 * time.Hour)),
		})
	}); err != nil {
		t.Fatalf("播种失败: %v", err)
	}

	var seq int64
	svc := reconcileapp.New(reconcileapp.Deps{
		Store: mem, Bill: fakeBillSrc{b: bill.Bill{TotalAmount: 90, Entries: []bill.Entry{{TransactionID: "t1", OutTradeNo: "o1", Amount: 90}}}},
		Alerter: nopAlerter{}, Clock: func() time.Time { return now },
		NewBatchNo: func() string { seq++; return "RC" + itoa64(seq) },
	})
	rw := NewReconcileWorker(svc, fakeLister{accts: []models.AppPayAccount{{AppID: "app1", Channel: "wechat", PayAccountID: "acc-wechat", Enabled: true}}}, time.Hour, func() time.Time { return now })

	n, err := rw.RunOnce(bg)
	if err != nil || n != 1 {
		t.Fatalf("应处理 1 个账号: n=%d err=%v", n, err)
	}
	// 该账期批次应已建立并完成。
	b, err := mem.Reader().Reconcile().GetBatchByAccountDate(bg, "wechat", "acc-wechat", day)
	if err != nil || b == nil || b.Status != models.BatchDone || b.MatchedCount != 1 {
		t.Fatalf("批次应完成且匹配 1: b=%+v err=%v", b, err)
	}
}

func itoa64(n int64) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
