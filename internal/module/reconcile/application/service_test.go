package application

import (
	"context"
	"testing"
	"time"

	"github.com/gtkit/orderd/internal/models"
	"github.com/gtkit/orderd/internal/pkg/sqlutil"
	"github.com/gtkit/orderd/internal/shared/bill"
	"github.com/gtkit/orderd/internal/store"
	"github.com/gtkit/orderd/internal/store/memstore"
)

var bg = context.Background()

var billDate = time.Date(2026, 7, 16, 0, 0, 0, 0, time.UTC)

type fakeBill struct {
	b   bill.Bill
	err error
}

func (f fakeBill) Fetch(context.Context, string, string, time.Time) (bill.Bill, error) {
	return f.b, f.err
}

type recAlerter struct{ n int }

func (a *recAlerter) Anomaly(context.Context, string, map[string]any) { a.n++ }

func newSvc(mem *memstore.Mem, b bill.Bill, alerter *recAlerter) *Service {
	var seq int64
	return New(Deps{
		Store: mem, Bill: fakeBill{b: b}, Alerter: alerter,
		Clock:      func() time.Time { return time.Date(2026, 7, 17, 3, 0, 0, 0, time.UTC) },
		NewBatchNo: func() string { seq++; return "RC" + itoa(seq) },
	})
}

// seedTxn 播种一条本地支付流水(账期落在 billDate)。
func seedTxn(t *testing.T, mem *memstore.Mem, txnID, outTradeNo, orderNo string, amount int64) {
	t.Helper()
	err := mem.WithinTx(bg, func(ctx context.Context, r store.Repos) error {
		return r.Transactions().Insert(ctx, &models.PaymentTransaction{
			AppID: "app1", OrderNo: orderNo, PayAccountID: "acc-wechat", Channel: "wechat", Kind: models.TxnPay,
			OutTradeNo: outTradeNo, TransactionID: txnID, Amount: amount, Currency: "CNY",
			PaidAt: sqlutil.Time(billDate.Add(10 * time.Hour)),
		})
	})
	if err != nil {
		t.Fatalf("播种流水失败: %v", err)
	}
}

func reconcile(t *testing.T, svc *Service) Result {
	t.Helper()
	res, err := svc.Reconcile(bg, "app1", "wechat", "acc-wechat", billDate)
	if err != nil {
		t.Fatalf("对账失败: %v", err)
	}
	return res
}

func diffByType(mem *memstore.Mem, dt models.DiffType) *models.ReconcileDiff {
	for i := range mem.Diffs() {
		if mem.Diffs()[i].DiffType == dt {
			d := mem.Diffs()[i]
			return &d
		}
	}
	return nil
}

func TestReconcileAllMatch(t *testing.T) {
	mem := memstore.New()
	seedTxn(t, mem, "t1", "o1", "OD1", 90)
	seedTxn(t, mem, "t2", "o2", "OD2", 100)
	b := bill.Bill{TotalAmount: 190, Entries: []bill.Entry{
		{TransactionID: "t1", OutTradeNo: "o1", Amount: 90},
		{TransactionID: "t2", OutTradeNo: "o2", Amount: 100},
	}}
	alerter := &recAlerter{}
	res := reconcile(t, newSvc(mem, b, alerter))
	if res.Matched != 2 || res.DiffCount != 0 {
		t.Fatalf("全一致应 0 差异: %+v", res)
	}
	if alerter.n != 0 {
		t.Fatal("无差异不应告警")
	}
}

func TestReconcileSurplus(t *testing.T) {
	mem := memstore.New()
	seedTxn(t, mem, "t1", "o1", "OD1", 90)
	// 渠道多一笔 t2(本地无)→ 长款。
	b := bill.Bill{TotalAmount: 190, Entries: []bill.Entry{
		{TransactionID: "t1", OutTradeNo: "o1", Amount: 90},
		{TransactionID: "t2", OutTradeNo: "o2", OrderNo: "OD2", Amount: 100},
	}}
	alerter := &recAlerter{}
	res := reconcile(t, newSvc(mem, b, alerter))
	d := diffByType(mem, models.DiffSurplus)
	if d == nil || d.TransactionID != "t2" || d.GatewayAmount != 100 || d.LocalAmount != 0 {
		t.Fatalf("应有长款差异 t2: %+v", d)
	}
	if res.Matched != 1 || alerter.n != 1 {
		t.Fatalf("应匹配 1 + 告警 1: %+v n=%d", res, alerter.n)
	}
}

func TestReconcileShort(t *testing.T) {
	mem := memstore.New()
	seedTxn(t, mem, "t1", "o1", "OD1", 90)
	seedTxn(t, mem, "t2", "o2", "OD2", 100) // 本地多一笔,渠道无 → 短款
	b := bill.Bill{TotalAmount: 90, Entries: []bill.Entry{{TransactionID: "t1", OutTradeNo: "o1", Amount: 90}}}
	reconcile(t, newSvc(mem, b, &recAlerter{}))
	d := diffByType(mem, models.DiffShort)
	if d == nil || d.TransactionID != "t2" || d.LocalAmount != 100 || d.GatewayAmount != 0 {
		t.Fatalf("应有短款差异 t2: %+v", d)
	}
}

func TestReconcileAmountMismatch(t *testing.T) {
	mem := memstore.New()
	seedTxn(t, mem, "t1", "o1", "OD1", 90)
	b := bill.Bill{TotalAmount: 80, Entries: []bill.Entry{{TransactionID: "t1", OutTradeNo: "o1", Amount: 80}}} // 渠道 80 本地 90
	reconcile(t, newSvc(mem, b, &recAlerter{}))
	d := diffByType(mem, models.DiffAmount)
	if d == nil || d.GatewayAmount != 80 || d.LocalAmount != 90 {
		t.Fatalf("应有金额不一致差异: %+v", d)
	}
}

func TestReconcileIdempotent(t *testing.T) {
	mem := memstore.New()
	seedTxn(t, mem, "t1", "o1", "OD1", 90)
	b := bill.Bill{TotalAmount: 90, Entries: []bill.Entry{{TransactionID: "t1", OutTradeNo: "o1", Amount: 90}}}
	svc := newSvc(mem, b, &recAlerter{})
	first := reconcile(t, svc)
	if first.Skipped {
		t.Fatal("首次不应跳过")
	}
	second := reconcile(t, svc)
	if !second.Skipped {
		t.Fatal("已完成账期二次对账应跳过")
	}
}

func itoa(n int64) string {
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
