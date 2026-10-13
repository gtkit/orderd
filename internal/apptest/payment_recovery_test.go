package apptest

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/gtkit/orderd/internal/models"
	paymentapp "github.com/gtkit/orderd/internal/module/payment/application"
	"github.com/gtkit/orderd/internal/paygateway"
	"github.com/gtkit/orderd/internal/store"
)

type delayedPrepareGateway struct {
	*fakeGateway
	entered chan struct{}
	release chan struct{}
}

func (g delayedPrepareGateway) Prepare(ctx context.Context, in paygateway.PrepareInput) (paygateway.PrepareResult, error) {
	close(g.entered)
	select {
	case <-ctx.Done():
		return paygateway.PrepareResult{}, ctx.Err()
	case <-g.release:
		return g.fakeGateway.Prepare(ctx, in)
	}
}

type recoveryRouter struct{ gateway paygateway.Gateway }

func (r recoveryRouter) Resolve(string, string) (paygateway.Gateway, error) { return r.gateway, nil }

func TestOldPrepareResponseCannotReviveReplacedAttempt(t *testing.T) {
	h := newHarness(false)
	out, err := h.order.CreateOrder(bg, baseCreate())
	if err != nil {
		t.Fatal(err)
	}
	old := h.mem.Attempts(out.OrderNo)[0]
	g := delayedPrepareGateway{h.gateway, make(chan struct{}), make(chan struct{})}
	pay := paymentapp.New(paymentapp.Deps{Store: h.mem, Router: recoveryRouter{g}, Accounts: fakeAccounts{}, Alerter: h.alerter,
		Clock: func() time.Time { return old.ExpireAt.Add(-5 * time.Minute) }})
	done := make(chan error, 1)
	in := paymentapp.PrepareInput{AppID: "app1", OrderNo: out.OrderNo, UserID: 42, PayMethod: models.PayMethodWechat}
	go func() { _, e := pay.PreparePayment(bg, in); done <- e }()
	select {
	case <-g.entered:
	case <-time.After(time.Second):
		t.Fatal("prepare never entered")
	}
	t.Cleanup(func() {
		select {
		case <-g.release:
		default:
			close(g.release)
		}
	})
	// The concurrent replay finishes and a channel switch then replaces its READY attempt.
	if _, err = h.payment.PreparePayment(bg, in); err != nil {
		t.Fatal(err)
	}
	in.PayMethod = models.PayMethodAlipay
	if _, err = h.payment.PreparePayment(bg, in); err != nil {
		t.Fatal(err)
	}
	close(g.release)
	if err = <-done; !errors.Is(err, models.ErrActiveAttemptExists) {
		t.Fatalf("old response must be rejected: %v", err)
	}
	for _, a := range h.mem.Attempts(out.OrderNo) {
		if a.OutTradeNo == old.OutTradeNo && a.Status != models.AttemptClosed {
			t.Fatalf("old revived: %+v", a)
		}
	}
}

func TestRecoveryEventFailureRollsBackMoneyAndState(t *testing.T) {
	h := newHarness(false)
	out, err := h.order.CreateOrder(bg, baseCreate())
	if err != nil {
		t.Fatal(err)
	}
	a := h.mem.Attempts(out.OrderNo)[0]
	err = h.mem.WithinTx(bg, func(ctx context.Context, r store.Repos) error {
		o, e := r.Orders().GetByOrderNo(ctx, "app1", out.OrderNo, true)
		if e != nil {
			return e
		}
		o.Status = models.OrderClosed
		return r.Orders().Update(ctx, o)
	})
	if err != nil {
		t.Fatal(err)
	}
	fault := errors.New("event id unavailable")
	pay := paymentapp.New(paymentapp.Deps{Store: h.mem, Accounts: fakeAccounts{}, Alerter: h.alerter, NewEventID: func() (string, error) { return "", fault }})
	in := confirmInput(h, out.OrderNo, a.PayAccountID, "rollback-money", 90)
	if _, err = pay.ConfirmPaySuccess(bg, in); !errors.Is(err, fault) {
		t.Fatalf("want failure: %v", err)
	}
	o, err := h.mem.Reader().Orders().GetByOrderNo(bg, "app1", out.OrderNo, false)
	if err != nil {
		t.Fatal(err)
	}
	if o.Status != models.OrderClosed || len(h.mem.Txns()) != 0 || countEvents(h, models.EventTypePaid) != 0 {
		t.Fatal("partial transaction committed")
	}
	if _, err = h.payment.ConfirmPaySuccess(bg, in); err != nil {
		t.Fatal(err)
	}
	if len(h.mem.Txns()) != 1 || countEvents(h, models.EventTypePaid) != 1 {
		t.Fatal("retry did not recover")
	}
}

func TestClosedAndCancelledPaymentRecoveryConcurrent(t *testing.T) {
	for _, status := range []models.OrderStatus{models.OrderClosed, models.OrderCancelled} {
		t.Run(status.Proto().String(), func(t *testing.T) {
			h := newHarness(false)
			out, err := h.order.CreateOrder(bg, baseCreate())
			if err != nil {
				t.Fatal(err)
			}
			a := h.mem.Attempts(out.OrderNo)[0]
			err = h.mem.WithinTx(bg, func(ctx context.Context, r store.Repos) error {
				o, e := r.Orders().GetByOrderNo(ctx, "app1", out.OrderNo, true)
				if e != nil {
					return e
				}
				o.Status, o.CloseReason = status, models.CloseTimeout
				o.ExpireAt = time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
				if e = r.Orders().Update(ctx, o); e != nil {
					return e
				}
				a.Status = models.AttemptClosed
				return r.Attempts().Update(ctx, &a)
			})
			if err != nil {
				t.Fatal(err)
			}
			in := confirmInput(h, out.OrderNo, a.PayAccountID, "late-recover", 90)
			var wg sync.WaitGroup
			errs := make(chan error, 24)
			for range 24 {
				wg.Go(func() { _, e := h.payment.ConfirmPaySuccess(bg, in); errs <- e })
			}
			wg.Wait()
			close(errs)
			for e := range errs {
				if e != nil {
					t.Fatal(e)
				}
			}
			o, err := h.mem.Reader().Orders().GetByOrderNo(bg, "app1", out.OrderNo, false)
			if err != nil {
				t.Fatal(err)
			}
			if o.Status != models.OrderPaid || o.CloseReason != models.CloseNone || o.SettledOutTradeNo != a.OutTradeNo {
				t.Fatalf("valid late payment must restore order and snapshot: %+v", o)
			}
			if len(h.mem.Txns()) != 1 || countEvents(h, models.EventTypePaid) != 1 || h.mem.Attempts(out.OrderNo)[0].Status != models.AttemptPaid {
				t.Fatal("repeated recovery must have one payment, one paid event and a paid attempt")
			}
		})
	}
}

func TestDuplicatePaymentCurrencyMustMatch(t *testing.T) {
	h := newHarness(false)
	orderNo, _ := h.createAndPay(t, "")
	a := h.mem.Attempts(orderNo)[0]
	in := confirmInput(h, orderNo, a.PayAccountID, "tx-"+orderNo, 90)
	in.Currency = "USD"
	res, err := h.payment.ConfirmPaySuccess(bg, in)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Anomaly || res.AnomalyKind != "duplicate_txn_mismatch" {
		t.Fatalf("currency conflict accepted: %+v", res)
	}
	if len(h.mem.Txns()) != 1 || countEvents(h, models.EventTypePaid) != 1 {
		t.Fatal("duplicate changed accounting")
	}
}
