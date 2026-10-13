package apptest

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gtkit/orderd/internal/models"
	paymentapp "github.com/gtkit/orderd/internal/module/payment/application"
	refundapp "github.com/gtkit/orderd/internal/module/refund/application"
	"github.com/gtkit/orderd/internal/shared/bill"
	"github.com/gtkit/orderd/internal/store"
)

type strictBillAccounts struct{ fakeAccounts }

func (strictBillAccounts) ResolveByID(ctx context.Context, id string) (models.AppPayAccount, error) {
	a, err := (fakeAccounts{}).ResolveByID(ctx, id)
	a.Channel = strings.TrimPrefix(id, "acc-")
	return a, err
}

type billVerifierFunc func(context.Context, string, string, bill.Entry) (bill.Success, error)

func (f billVerifierFunc) Verify(ctx context.Context, ch, acct string, e bill.Entry) (bill.Success, error) {
	return f(ctx, ch, acct, e)
}
func billPaymentService(h *harness, v bill.Verifier) *paymentapp.Service {
	return paymentapp.New(paymentapp.Deps{Store: h.mem, Accounts: strictBillAccounts{}, Router: fakeRouter{h.gateway}, Alerter: h.alerter, BillVerifier: v, NewEventID: func() (string, error) { return "paid-repair-event", nil }})
}
func billRefundService(h *harness, v bill.Verifier) *refundapp.Service {
	return refundapp.New(refundapp.Deps{Store: h.mem, Accounts: strictBillAccounts{}, Router: fakeRouter{h.gateway}, Alerter: h.alerter, BillVerifier: v, NewEventID: func() (string, error) { return "refund-repair-event", nil }})
}
func successfulBill(e bill.Entry) bill.Success {
	id := e.TransactionID
	if id == "" {
		id = e.OriginalTransactionID
	}
	return bill.Success{Confirmed: true, TransactionID: id, OriginalTransactionID: e.OriginalTransactionID, OutTradeNo: e.OutTradeNo, OutRefundNo: e.OutRefundNo, Amount: e.Amount, Currency: e.Currency, At: time.Date(2026, 10, 10, 12, 0, 0, 0, bill.Location)}
}
func TestBillPaymentRepairOnlyExactPositiveFacts(t *testing.T) {
	for _, field := range []string{"success", "not_confirmed", "query_error", "query_currency", "query_amount", "query_transaction", "query_out_trade", "unknown_time", "foreign_app", "foreign_merchant", "foreign_account", "foreign_provider_app", "bill_currency", "bill_amount", "unknown_attempt", "stale_config", "changed_frozen_merchant", "missing_frozen_app"} {
		t.Run(field, func(t *testing.T) {
			h := newHarness(false)
			out, err := h.order.CreateOrder(t.Context(), baseCreate())
			if err != nil {
				t.Fatal(err)
			}
			a := h.mem.Attempts(out.OrderNo)[0]
			in := bill.RepairRequest{AppID: "app1", Channel: "wechat", PayAccountID: "acc-wechat", Entry: bill.Entry{Kind: models.TxnPay, TransactionID: "paid-bill", OutTradeNo: a.OutTradeNo, Amount: 90, Currency: "CNY", MerchantID: "mch-wechat", ProviderAppID: "app-wechat", Status: "SUCCESS"}}
			switch field {
			case "foreign_app":
				in.AppID = "other"
			case "foreign_merchant":
				in.Entry.MerchantID = "other"
			case "foreign_account":
				in.PayAccountID = "other"
			case "foreign_provider_app":
				in.Entry.ProviderAppID = "other"
			case "bill_currency":
				in.Entry.Currency = "USD"
			case "bill_amount":
				in.Entry.Amount++
			case "unknown_attempt":
				in.Entry.OutTradeNo = "missing"
			}
			if field == "stale_config" || field == "changed_frozen_merchant" || field == "missing_frozen_app" {
				err := h.mem.WithinTx(t.Context(), func(ctx context.Context, r store.Repos) error {
					if field == "stale_config" {
						a.ConfigVersion = 2
						return r.Attempts().Update(ctx, &a)
					}
					o, err := r.Orders().GetByOrderNo(ctx, "app1", out.OrderNo, true)
					if err != nil {
						return err
					}
					if field == "changed_frozen_merchant" {
						o.PayMchID = "old-merchant"
					} else {
						o.PayAppID = ""
					}
					return r.Orders().Update(ctx, o)
				})
				if err != nil {
					t.Fatal(err)
				}
			}
			v := billVerifierFunc(func(ctx context.Context, _, _ string, e bill.Entry) (bill.Success, error) {
				if _, ok := ctx.Deadline(); !ok {
					t.Fatal("query missing deadline")
				}
				q := successfulBill(e)
				switch field {
				case "not_confirmed":
					q.Confirmed = false
				case "query_error":
					return bill.Success{}, errors.New("query failed")
				case "query_currency":
					q.Currency = "USD"
				case "query_amount":
					q.Amount++
				case "query_transaction":
					q.TransactionID = "other"
				case "query_out_trade":
					q.OutTradeNo = "other"
				case "unknown_time":
					q.At = time.Time{}
				}
				return q, nil
			})
			ok, err := billPaymentService(h, v).VerifyAndConfirmFromBill(t.Context(), in)
			if field == "success" {
				if !ok || err != nil || len(h.mem.Txns()) != 1 || countEvents(h, models.EventTypePaid) != 1 {
					t.Fatalf("success=%v %v", ok, err)
				}
				return
			}
			if ok || len(h.mem.Txns()) != 0 || countEvents(h, models.EventTypePaid) != 0 {
				t.Fatalf("unsafe repair %v %v", ok, err)
			}
			o, _ := h.mem.Reader().Orders().GetByOrderNo(t.Context(), "app1", out.OrderNo, false)
			if o.Status != models.OrderPending {
				t.Fatalf("state changed: %v", o.Status)
			}
		})
	}
}

func TestBillPaymentRepairConcurrentLargeClosedOrder(t *testing.T) {
	for _, state := range []models.OrderStatus{models.OrderClosed, models.OrderCancelled} {
		t.Run(state.Proto().String(), func(t *testing.T) {
			h := newHarness(false)
			out, err := h.order.CreateOrder(t.Context(), baseCreate())
			if err != nil {
				t.Fatal(err)
			}
			a := h.mem.Attempts(out.OrderNo)[0]
			err = h.mem.WithinTx(t.Context(), func(ctx context.Context, r store.Repos) error {
				o, e := r.Orders().GetByOrderNo(ctx, "app1", out.OrderNo, true)
				if e != nil {
					return e
				}
				o.PayAmount = 1 << 40
				o.Status = state
				o.ExpireAt = time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
				return r.Orders().Update(ctx, o)
			})
			if err != nil {
				t.Fatal(err)
			}
			in := bill.RepairRequest{AppID: "app1", Channel: "wechat", PayAccountID: "acc-wechat", Entry: bill.Entry{Kind: models.TxnPay, TransactionID: "large-bill", OutTradeNo: a.OutTradeNo, Amount: 1 << 40, Currency: "CNY", MerchantID: "mch-wechat", Status: "SUCCESS"}}
			svc := billPaymentService(h, billVerifierFunc(func(_ context.Context, _, _ string, e bill.Entry) (bill.Success, error) {
				return successfulBill(e), nil
			}))
			var wg sync.WaitGroup
			for range 20 {
				wg.Go(func() {
					ok, err := svc.VerifyAndConfirmFromBill(t.Context(), in)
					if !ok || err != nil {
						t.Errorf("repair %v %v", ok, err)
					}
				})
			}
			wg.Wait()
			o, _ := h.mem.Reader().Orders().GetByOrderNo(t.Context(), "app1", out.OrderNo, false)
			if o.Status != models.OrderPaid || len(h.mem.Txns()) != 1 || h.mem.Txns()[0].Amount != 1<<40 || countEvents(h, models.EventTypePaid) != 1 {
				t.Fatalf("duplicate/incorrect funds: order=%+v", o)
			}
		})
	}
}

func refundBillFixture(t *testing.T) (*harness, bill.RepairRequest, string) {
	t.Helper()
	h := newHarness(true)
	orderNo, _ := h.createAndPay(t, "")
	out, err := h.refund.RefundOrder(t.Context(), refundapp.RefundInput{AppID: "app1", OrderNo: orderNo, OutRefundNo: "bill-refund", RefundAmount: 90})
	if err != nil {
		t.Fatal(err)
	}
	original := h.mem.Txns()[0]
	return h, bill.RepairRequest{AppID: "app1", Channel: "wechat", PayAccountID: "acc-wechat", Entry: bill.Entry{Kind: models.TxnRefund, TransactionID: "gwr-bill-refund", OriginalTransactionID: original.TransactionID, OutTradeNo: original.OutTradeNo, OutRefundNo: "bill-refund", Amount: 90, Currency: "CNY", MerchantID: "mch-wechat", Status: "REFUND:PROCESSING"}}, out.RefundNo
}
func TestBillRefundRepairNeverAppliesNegativeOrAmbiguousFacts(t *testing.T) {
	for _, field := range []string{"success", "not_confirmed", "query_error", "query_currency", "query_amount", "query_original", "query_refund", "query_out_trade", "query_out_refund", "foreign_app", "foreign_merchant", "unknown_refund", "bill_original", "revoked", "abnormal", "failed", "closed", "stale_settled_config", "changed_settled_merchant", "missing_settled_app"} {
		t.Run(field, func(t *testing.T) {
			h, in, refundNo := refundBillFixture(t)
			switch field {
			case "foreign_app":
				in.AppID = "other"
			case "foreign_merchant":
				in.Entry.MerchantID = "other"
			case "unknown_refund":
				in.Entry.OutRefundNo = "other"
			case "bill_original":
				in.Entry.OriginalTransactionID = "other"
			case "revoked":
				in.Entry.Status = "REVOKED:SUCCESS"
			}
			if field == "abnormal" || field == "failed" || field == "closed" {
				err := h.mem.WithinTx(t.Context(), func(ctx context.Context, r store.Repos) error {
					rf, e := r.Refunds().GetByRefundNo(ctx, refundNo, true)
					if e != nil {
						return e
					}
					switch field {
					case "abnormal":
						rf.Status = models.RefundAbnormal
					case "failed":
						rf.Status = models.RefundFailed
					case "closed":
						rf.Status = models.RefundClosed
					}
					return r.Refunds().Update(ctx, rf)
				})
				if err != nil {
					t.Fatal(err)
				}
			}
			if field == "stale_settled_config" || field == "changed_settled_merchant" || field == "missing_settled_app" {
				rf, err := h.refund.GetRefund(t.Context(), "app1", refundNo, "")
				if err != nil {
					t.Fatal(err)
				}
				err = h.mem.WithinTx(t.Context(), func(ctx context.Context, r store.Repos) error {
					o, err := r.Orders().GetByOrderNo(ctx, "app1", rf.OrderNo, true)
					if err != nil {
						return err
					}
					switch field {
					case "stale_settled_config":
						o.SettledConfigVersion = 2
					case "changed_settled_merchant":
						o.SettledMchID = "old-merchant"
					case "missing_settled_app":
						o.SettledPayAppID = ""
					}
					return r.Orders().Update(ctx, o)
				})
				if err != nil {
					t.Fatal(err)
				}
			}
			before, _ := h.refund.GetRefund(t.Context(), "app1", refundNo, "")
			v := billVerifierFunc(func(_ context.Context, _, _ string, e bill.Entry) (bill.Success, error) {
				q := successfulBill(e)
				switch field {
				case "not_confirmed":
					q.Confirmed = false
				case "query_error":
					return bill.Success{}, errors.New("query failed")
				case "query_currency":
					q.Currency = "USD"
				case "query_amount":
					q.Amount++
				case "query_original":
					q.OriginalTransactionID = "other"
				case "query_refund":
					q.TransactionID = "other"
				case "query_out_trade":
					q.OutTradeNo = "other"
				case "query_out_refund":
					q.OutRefundNo = "other"
				}
				return q, nil
			})
			ok, err := billRefundService(h, v).VerifyAndConfirmFromBill(t.Context(), in)
			after, _ := h.refund.GetRefund(t.Context(), "app1", refundNo, "")
			if field == "success" {
				if !ok || err != nil || after.Status != models.RefundSucceeded || len(refundTxns(h, refundNo)) != 1 {
					t.Fatalf("success=%v %v refund=%+v", ok, err, after)
				}
				return
			}
			if ok || after.Status != before.Status || after.NotExistCount != before.NotExistCount || after.QueryCount != before.QueryCount || len(refundTxns(h, refundNo)) != 0 || countEvents(h, models.EventTypeRefunded) != 0 {
				t.Fatalf("unsafe repair: %v %v before=%+v after=%+v", ok, err, before, after)
			}
			if len(h.gateway.refunds) != 1 {
				t.Fatal("repair submitted extra refund")
			}
		})
	}
}

func TestBillRefundConcurrentSuccessAndCancellation(t *testing.T) {
	h, in, refundNo := refundBillFixture(t)
	var calls atomic.Int64
	svc := billRefundService(h, billVerifierFunc(func(ctx context.Context, _, _ string, e bill.Entry) (bill.Success, error) {
		calls.Add(1)
		if err := ctx.Err(); err != nil {
			return bill.Success{}, err
		}
		return successfulBill(e), nil
	}))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if ok, err := svc.VerifyAndConfirmFromBill(ctx, in); ok || !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled %v %v", ok, err)
	}
	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			ok, err := svc.VerifyAndConfirmFromBill(t.Context(), in)
			if !ok || err != nil {
				t.Errorf("repair=%v %v", ok, err)
			}
		})
	}
	wg.Wait()
	rf, _ := h.refund.GetRefund(t.Context(), "app1", refundNo, "")
	o, _ := h.mem.Reader().Orders().GetByOrderNo(t.Context(), "app1", rf.OrderNo, false)
	if rf.Status != models.RefundSucceeded || o.RefundedAmount != 90 || countEvents(h, models.EventTypeRefunded) != 1 || len(refundTxns(h, refundNo)) != 1 || len(h.gateway.refunds) != 1 {
		t.Fatalf("refund duplicated or not settled: %+v", rf)
	}
	if calls.Load() < 20 {
		t.Fatal("confirmation was not independently verified")
	}
}

func TestBillPaymentLosesToOtherCallbackWithoutAnomalyInsertion(t *testing.T) {
	h := newHarness(false)
	out, err := h.order.CreateOrder(t.Context(), baseCreate())
	if err != nil {
		t.Fatal(err)
	}
	a := h.mem.Attempts(out.OrderNo)[0]
	entered, release := make(chan struct{}), make(chan struct{})
	svc := billPaymentService(h, billVerifierFunc(func(ctx context.Context, _, _ string, e bill.Entry) (bill.Success, error) {
		close(entered)
		select {
		case <-release:
			return successfulBill(e), nil
		case <-ctx.Done():
			return bill.Success{}, ctx.Err()
		}
	}))
	in := bill.RepairRequest{AppID: "app1", Channel: "wechat", PayAccountID: "acc-wechat", Entry: bill.Entry{Kind: models.TxnPay, TransactionID: "losing-bill", OutTradeNo: a.OutTradeNo, Amount: 90, Currency: "CNY", MerchantID: "mch-wechat", Status: "SUCCESS"}}
	type result struct {
		ok  bool
		err error
	}
	done := make(chan result, 1)
	go func() { ok, err := svc.VerifyAndConfirmFromBill(t.Context(), in); done <- result{ok, err} }()
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("query not entered")
	}
	if _, err := h.payment.PreparePayment(t.Context(), paymentapp.PrepareInput{AppID: "app1", OrderNo: out.OrderNo, UserID: 42, PayMethod: models.PayMethodAlipay}); err != nil {
		t.Fatal(err)
	}
	var winner models.PaymentAttempt
	for _, a := range h.mem.Attempts(out.OrderNo) {
		if a.Channel == "alipay" {
			winner = a
		}
	}
	if winner.OutTradeNo == "" {
		t.Fatal("new attempt not created")
	}
	if _, err := h.payment.ConfirmPaySuccess(t.Context(), paymentapp.ConfirmInput{AppID: "app1", OrderNo: out.OrderNo, OutTradeNo: winner.OutTradeNo, Channel: "alipay", PayAccountID: "acc-alipay", TransactionID: "winning-callback", Amount: 90, Currency: "CNY", PaidAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	close(release)
	got := <-done
	if got.ok || !errors.Is(got.err, models.ErrPaymentAnomaly) {
		t.Fatalf("losing repair result=%+v", got)
	}
	txns := h.mem.Txns()
	if len(txns) != 1 || txns[0].TransactionID != "winning-callback" || txns[0].Anomaly || countEvents(h, models.EventTypePaid) != 1 {
		t.Fatalf("repair inserted unexpected funds: %+v", txns)
	}
}
