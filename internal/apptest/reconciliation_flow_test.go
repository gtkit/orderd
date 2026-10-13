package apptest

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gtkit/orderd/internal/models"
	paymentapp "github.com/gtkit/orderd/internal/module/payment/application"
	reconcileapp "github.com/gtkit/orderd/internal/module/reconcile/application"
	refundapp "github.com/gtkit/orderd/internal/module/refund/application"
	"github.com/gtkit/orderd/internal/shared/bill"
	"github.com/gtkit/orderd/internal/store"
)

// flowEvidence replaces only the external statement/query boundary. Every import,
// comparison, confirmation, transaction, event and retry below uses real services.
type flowEvidence struct {
	entries  map[string][]bill.Entry
	fetches  map[string]int
	queries  map[models.TxnKind]int
	refundAt time.Time
}

func (f *flowEvidence) Fetch(ctx context.Context, channel, account string, day time.Time, emit func(bill.Entry) error) (bill.Summary, error) {
	if channel != "wechat" || account != "acc-wechat" {
		return bill.Summary{}, errors.New("unexpected statement scope")
	}
	key := day.In(bill.Location).Format(time.DateOnly)
	entries, ok := f.entries[key]
	if !ok {
		return bill.Summary{}, errors.New("statement unavailable")
	}
	f.fetches[key]++
	var sum bill.Summary
	for _, e := range entries {
		if err := ctx.Err(); err != nil {
			return bill.Summary{}, err
		}
		if err := emit(e); err != nil {
			return bill.Summary{}, err
		}
		sum.Count++
		if e.Kind == models.TxnPay {
			sum.PayAmount += e.Amount
		} else {
			sum.RefundAmount += e.Amount
		}
	}
	return sum, nil
}

func (f *flowEvidence) Verify(ctx context.Context, channel, account string, e bill.Entry) (bill.Success, error) {
	if err := ctx.Err(); err != nil {
		return bill.Success{}, err
	}
	if _, ok := ctx.Deadline(); !ok {
		return bill.Success{}, errors.New("query lacks deadline")
	}
	if channel != "wechat" || account != "acc-wechat" {
		return bill.Success{}, errors.New("unexpected query scope")
	}
	f.queries[e.Kind]++
	at := e.EventAt
	if e.Kind == models.TxnRefund {
		at = f.refundAt
	}
	return bill.Success{Confirmed: true, TransactionID: e.TransactionID, OriginalTransactionID: e.OriginalTransactionID,
		OutTradeNo: e.OutTradeNo, OutRefundNo: e.OutRefundNo, Amount: e.Amount, Currency: e.Currency, At: at}, nil
}

type flowRepairer struct{ payment, refund bill.Repairer }

func (f flowRepairer) VerifyAndConfirmFromBill(ctx context.Context, in bill.RepairRequest) (bool, error) {
	if in.Entry.Kind == models.TxnPay {
		return f.payment.VerifyAndConfirmFromBill(ctx, in)
	}
	return f.refund.VerifyAndConfirmFromBill(ctx, in)
}

type reconciliationFlow struct {
	h        *harness
	day, now time.Time
	account  models.AppPayAccount
	evidence *flowEvidence
	payment  *paymentapp.Service
	refund   *refundapp.Service
	seq      atomic.Int64
}

func newReconciliationFlow() *reconciliationFlow {
	f := &reconciliationFlow{h: newHarness(true), day: time.Date(2026, 10, 9, 0, 0, 0, 0, bill.Location)}
	f.now = f.day.AddDate(0, 0, 3).Add(12 * time.Hour)
	// Ledger insertion mirrors DB-generated CreatedAt using the wall clock. Keep
	// the worker clock ahead so committed wakes are due, then advance it explicitly.
	if wall := time.Now().Add(time.Hour); f.now.Before(wall) {
		f.now = wall
	}
	f.account = models.AppPayAccount{AppID: "app1", PayAccountID: "acc-wechat", Channel: "wechat", MchID: "mch-wechat", PayAppID: "app-wechat", ConfigVersion: 1, Enabled: true, CreatedAt: f.day}
	f.h.mem.SeedAccount(f.account)
	f.evidence = &flowEvidence{entries: map[string][]bill.Entry{}, fetches: map[string]int{}, queries: map[models.TxnKind]int{}}
	clock := func() time.Time { return f.now }
	event := func() (string, error) { return fmt.Sprintf("flow-event-%d", f.seq.Add(1)), nil }
	f.payment = paymentapp.New(paymentapp.Deps{Store: f.h.mem, Accounts: strictBillAccounts{}, Router: fakeRouter{f.h.gateway}, Alerter: f.h.alerter, Clock: clock, NewEventID: event, BillVerifier: f.evidence})
	f.refund = refundapp.New(refundapp.Deps{Store: f.h.mem, Accounts: strictBillAccounts{}, Router: fakeRouter{f.h.gateway}, Alerter: f.h.alerter, Clock: clock, NewEventID: event, BillVerifier: f.evidence, Enabled: true,
		NewRefundNo: func() string { return fmt.Sprintf("flow-refund-%d", f.seq.Add(1)) }})
	return f
}

func (f *reconciliationFlow) service(st store.Store) *reconcileapp.Service {
	return reconcileapp.New(reconcileapp.Deps{Store: st, Bill: f.evidence, Repair: flowRepairer{f.payment, f.refund}, AutoConfirm: true, Alerter: f.h.alerter,
		Clock: func() time.Time { return f.now }, NewBatchNo: func() string { return fmt.Sprintf("flow-batch-%d", f.seq.Add(1)) }})
}

func (f *reconciliationFlow) createPayment(t *testing.T) (string, bill.Entry) {
	t.Helper()
	out, err := f.h.order.CreateOrder(t.Context(), baseCreate())
	if err != nil {
		t.Fatal(err)
	}
	a := f.h.mem.Attempts(out.OrderNo)[0]
	return out.OrderNo, bill.Entry{Kind: models.TxnPay, TransactionID: "flow-pay", OutTradeNo: a.OutTradeNo, Amount: 90, Currency: "CNY", MerchantID: f.account.MchID, ProviderAppID: f.account.PayAppID, EventAt: f.day.Add(10 * time.Hour), Status: "SUCCESS"}
}

func (f *reconciliationFlow) batch(t *testing.T, day time.Time) *models.ReconcileBatch {
	t.Helper()
	b, err := f.h.mem.Reader().Reconcile().GetBatchByAccountDate(t.Context(), f.account.Channel, f.account.PayAccountID, day, 2)
	if err != nil || b == nil {
		t.Fatalf("batch missing: %v", err)
	}
	return b
}

func (f *reconciliationFlow) recover(t *testing.T, s *reconcileapp.Service, through time.Time) {
	t.Helper()
	if _, err := s.RecoverAccount(t.Context(), f.account, through); err != nil {
		t.Fatal(err)
	}
}

func (f *reconciliationFlow) assertNoLocalWork(t *testing.T) {
	t.Helper()
	err := f.h.mem.WithinTx(t.Context(), func(ctx context.Context, r store.Repos) error {
		change, err := r.Reconcile().ClaimLocalTxn(ctx, f.account.Channel, f.account.PayAccountID, "flow-inspect", f.now, f.now.Add(time.Minute))
		if change != nil {
			return fmt.Errorf("local wake still pending: %+v", change)
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestReconciliationFlowClosedPaymentRestoresAndRecompares(t *testing.T) {
	t.Parallel()
	f := newReconciliationFlow()
	orderNo, e := f.createPayment(t)
	// Enter CLOSED through the real cancellation and channel-close confirmation path.
	if _, err := f.h.order.CancelOrder(t.Context(), "app1", orderNo, 42); err != nil {
		t.Fatal(err)
	}
	if err := f.h.payment.CloseAttemptAndMaybeOrder(t.Context(), e.OutTradeNo); err != nil {
		t.Fatal(err)
	}
	if o := orderOf(t, f.h, orderNo); o.Status != models.OrderClosed {
		t.Fatalf("order not closed: %v", o.Status)
	}
	f.evidence.entries[f.day.Format(time.DateOnly)] = []bill.Entry{e}
	s := f.service(f.h.mem)
	result, err := s.Reconcile(t.Context(), "app1", "wechat", "acc-wechat", f.day)
	if err != nil || result.DiffCount != 1 || result.Matched != 0 {
		t.Fatalf("import=%+v err=%v", result, err)
	}
	b := f.batch(t, f.day)
	firstRun := b.PublishedRunID
	diffs := f.h.mem.Diffs()
	if firstRun == "" || len(diffs) != 1 || diffs[0].RunID != firstRun || diffs[0].HandleStatus != models.DiffHandlePending || len(f.h.mem.Txns()) != 0 || f.evidence.queries[models.TxnPay] != 0 {
		t.Fatalf("statement must publish before money changes: batch=%+v diffs=%+v", b, diffs)
	}
	f.recover(t, s, f.day)
	o := orderOf(t, f.h, orderNo)
	txns := f.h.mem.Txns()
	if o.Status != models.OrderPaid || o.TradeNo != e.TransactionID || len(txns) != 1 || txns[0].Anomaly || txns[0].Amount != e.Amount || !txns[0].PaidAt.Time.Equal(e.EventAt) || countEvents(f.h, models.EventTypePaid) != 1 {
		t.Fatalf("late payment not atomically restored: order=%+v txns=%+v", o, txns)
	}
	paid := lastEnvelope(t, f.h, models.EventTypePaid).GetPaid()
	if paid.GetTradeNo() != e.TransactionID || !paid.GetPaidAt().AsTime().Equal(e.EventAt) {
		t.Fatalf("paid event=%+v", paid)
	}
	if d := f.h.mem.Diffs()[0]; d.HandleStatus != models.DiffHandleResolved || d.VerifyAttempts != 1 {
		t.Fatalf("repair not recorded: %+v", d)
	}
	// A later cycle consumes the transaction's durable local wake and requests a
	// comparison generation; another cycle publishes that comparison without download.
	f.recover(t, s, f.day)
	if b := f.batch(t, f.day); b.DirtySeq <= b.PublishedDirtySeq {
		t.Fatalf("local wake did not request recheck: %+v", b)
	}
	f.recover(t, s, f.day)
	b = f.batch(t, f.day)
	if b.PublishedRunID == firstRun || b.DiffCount != 0 || b.MatchedCount != 1 || b.LocalAmount != 90 || b.DirtySeq != b.PublishedDirtySeq || f.evidence.fetches[f.day.Format(time.DateOnly)] != 1 || f.evidence.queries[models.TxnPay] != 1 {
		t.Fatalf("recomparison did not converge: %+v", b)
	}
	f.assertNoLocalWork(t)
	if len(f.h.mem.Txns()) != 1 || countEvents(f.h, models.EventTypePaid) != 1 || len(f.h.mem.Audits()) != 1 {
		t.Fatal("recovery duplicated ledger, event or audit")
	}
}

var errFlowFinish = errors.New("injected repair-result commit failure")

type flowFinishStore struct {
	store.Store
	fail atomic.Bool
}
type flowFinishRepos struct {
	store.Repos
	owner *flowFinishStore
}
type flowFinishRepo struct {
	store.ReconcileRepo
	owner *flowFinishStore
}

func (s *flowFinishStore) WithinTx(ctx context.Context, fn func(context.Context, store.Repos) error) error {
	return s.Store.WithinTx(ctx, func(ctx context.Context, r store.Repos) error { return fn(ctx, flowFinishRepos{r, s}) })
}
func (r flowFinishRepos) Reconcile() store.ReconcileRepo {
	return flowFinishRepo{r.Repos.Reconcile(), r.owner}
}
func (r flowFinishRepo) FinishRepair(ctx context.Context, id uint64, token string, now, next time.Time, success bool, reason string) (bool, error) {
	if success && r.owner.fail.CompareAndSwap(true, false) {
		return false, errFlowFinish
	}
	return r.ReconcileRepo.FinishRepair(ctx, id, token, now, next, success, reason)
}

func TestReconciliationFlowCrossDayRefundReplaysAfterResultFailure(t *testing.T) {
	t.Parallel()
	f := newReconciliationFlow()
	orderNo, payment := f.createPayment(t)
	if _, err := f.payment.ConfirmPaySuccess(t.Context(), paymentapp.ConfirmInput{AppID: "app1", OrderNo: orderNo, OutTradeNo: payment.OutTradeNo, Channel: "wechat", PayAccountID: "acc-wechat", TransactionID: payment.TransactionID, Amount: payment.Amount, Currency: payment.Currency, PaidAt: payment.EventAt}); err != nil {
		t.Fatal(err)
	}
	f.evidence.entries[f.day.Format(time.DateOnly)] = []bill.Entry{payment}
	fault := &flowFinishStore{Store: f.h.mem}
	s := f.service(fault)
	if result, err := s.Reconcile(t.Context(), "app1", "wechat", "acc-wechat", f.day); err != nil || result.Matched != 1 {
		t.Fatalf("original payment=%+v err=%v", result, err)
	}
	f.recover(t, s, f.day)
	rf, err := f.refund.RefundOrder(t.Context(), refundapp.RefundInput{AppID: "app1", OrderNo: orderNo, OutRefundNo: "flow-refund", RefundAmount: 90})
	if err != nil {
		t.Fatal(err)
	}
	refundDay := f.day.AddDate(0, 0, 1)
	f.evidence.refundAt = refundDay.AddDate(0, 0, 1).Add(2 * time.Minute)
	e := bill.Entry{Kind: models.TxnRefund, TransactionID: "gwr-flow-refund", OriginalTransactionID: payment.TransactionID, OutTradeNo: payment.OutTradeNo, OutRefundNo: "flow-refund", Amount: 90, Currency: "CNY", MerchantID: f.account.MchID, ProviderAppID: f.account.PayAppID, EventAt: refundDay.Add(23*time.Hour + 59*time.Minute), Status: "REFUND:PROCESSING"}
	f.evidence.entries[refundDay.Format(time.DateOnly)] = []bill.Entry{e}
	result, err := s.Reconcile(t.Context(), "app1", "wechat", "acc-wechat", refundDay)
	if err != nil || result.DiffCount != 1 || len(refundTxns(f.h, rf.RefundNo)) != 0 {
		t.Fatalf("refund import=%+v err=%v", result, err)
	}
	fault.fail.Store(true)
	if _, err := s.RecoverAccount(t.Context(), f.account, refundDay); !errors.Is(err, errFlowFinish) {
		t.Fatalf("expected result-write crash window, got %v", err)
	}
	refunded, err := f.refund.GetRefund(t.Context(), "app1", rf.RefundNo, "")
	if err != nil {
		t.Fatal(err)
	}
	diffs := f.h.mem.Diffs()
	if refunded.Status != models.RefundSucceeded || len(refundTxns(f.h, rf.RefundNo)) != 1 || countEvents(f.h, models.EventTypeRefunded) != 1 || len(diffs) != 1 || diffs[0].HandleStatus != models.DiffHandlePending || len(f.h.mem.Audits()) != 0 {
		t.Fatalf("confirmation must survive failed result transaction: refund=%+v diffs=%+v", refunded, diffs)
	}
	f.now = f.now.Add(time.Minute) // Expire the abandoned repair lease, without sleeping.
	f.recover(t, s, refundDay)
	if f.evidence.queries[models.TxnRefund] != 2 || f.h.mem.Diffs()[0].HandleStatus != models.DiffHandleResolved || len(f.h.mem.Audits()) != 1 {
		t.Fatal("successful confirmation was not safely replayed")
	}
	f.recover(t, s, refundDay)
	b := f.batch(t, refundDay)
	refunds := refundTxns(f.h, rf.RefundNo)
	o := orderOf(t, f.h, orderNo)
	if b.DiffCount != 0 || b.MatchedCount != 1 || b.LocalRefundAmount != 90 || b.SourceRefundAmount != 90 || b.DirtySeq != b.PublishedDirtySeq || f.evidence.fetches[refundDay.Format(time.DateOnly)] != 1 ||
		len(refunds) != 1 || !refunds[0].PaidAt.Time.Equal(f.evidence.refundAt) || o.Status != models.OrderRefunded || o.RefundedAmount != 90 || len(f.h.mem.Txns()) != 2 || countEvents(f.h, models.EventTypeRefunded) != 1 || len(f.h.gateway.refunds) != 1 {
		t.Fatalf("cross-day refund duplicated or failed to converge: batch=%+v refund=%+v order=%+v", b, refunds, o)
	}
	event := lastEnvelope(t, f.h, models.EventTypeRefunded).GetRefunded()
	if event.GetRefundNo() != rf.RefundNo || event.GetRefundAmount() != 90 || !event.GetRefundedAt().AsTime().Equal(f.evidence.refundAt) {
		t.Fatalf("refund event=%+v", event)
	}
	f.assertNoLocalWork(t)
}
