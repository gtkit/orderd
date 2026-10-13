//go:build integration

package gormstore_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/gtkit/orderd/internal/models"
	paymentapp "github.com/gtkit/orderd/internal/module/payment/application"
	"github.com/gtkit/orderd/internal/pkg/idgen"
	"github.com/gtkit/orderd/internal/repository/gormstore"
	"github.com/gtkit/orderd/internal/shared/alert"
	"github.com/gtkit/orderd/internal/store"
)

func TestIntegrationLatePaymentRecoveryConcurrent(t *testing.T) {
	for _, status := range []models.OrderStatus{models.OrderClosing, models.OrderClosed, models.OrderCancelled} {
		t.Run(status.Text(), func(t *testing.T) {
			st := openTestStore(t)
			t.Cleanup(func() { _ = st.Close() })
			const amount int64 = 9007199254740993 // Above float64's exact integer range; all money remains int64.
			paidAt := time.Now().Truncate(time.Second)
			err := st.WithinTx(bg, func(ctx context.Context, r store.Repos) error {
				if err := r.Orders().Insert(ctx, &models.Order{AppID: "app1", OrderNo: "OD-recovery", OrderToken: "recovery-token", UserID: 42,
					ProductID: 1, ProductTitle: "recovery", Status: status, CloseReason: models.CloseTimeout, Currency: "CNY",
					OriginalPrice: amount, PayAmount: amount, PayMethod: models.PayMethodWechat, PayChannel: "wechat", PayAccountID: "acc-wechat",
					ExpireAt: paidAt.Add(-time.Hour)}); err != nil {
					return err
				}
				return r.Attempts().Insert(ctx, &models.PaymentAttempt{AppID: "app1", OrderNo: "OD-recovery", OutTradeNo: "PT-recovery", AttemptNo: 1,
					Status: models.AttemptClosed, Channel: "wechat", PayAccountID: "acc-wechat", ExpireAt: paidAt.Add(-time.Hour)})
			})
			if err != nil {
				t.Fatal(err)
			}
			pay := paymentapp.New(paymentapp.Deps{Store: st, Accounts: gormstore.NewAccountResolver(st.DB()), Alerter: alert.Nop{}, NewEventID: idgen.UUIDv4})
			in := paymentapp.ConfirmInput{OutTradeNo: "PT-recovery", Channel: "wechat", PayAccountID: "acc-wechat", TransactionID: "tx-recovery", Amount: amount, Currency: "CNY", PaidAt: paidAt}
			var wg sync.WaitGroup
			start := make(chan struct{})
			results := make(chan paymentapp.ConfirmResult, 32)
			errs := make(chan error, 32)
			for range 32 {
				wg.Go(func() { <-start; res, err := pay.ConfirmPaySuccess(bg, in); results <- res; errs <- err })
			}
			close(start)
			wg.Wait()
			close(results)
			close(errs)
			for err := range errs {
				if err != nil {
					t.Fatal(err)
				}
			}
			paid, duplicates := 0, 0
			for res := range results {
				switch res.Outcome {
				case "paid":
					paid++
				case "duplicate":
					duplicates++
				default:
					t.Fatalf("unexpected confirmation: %+v", res)
				}
			}
			if paid != 1 || duplicates != 31 {
				t.Fatalf("paid=%d duplicate=%d", paid, duplicates)
			}
			if err := pay.CloseAttemptAndMaybeOrder(bg, in.OutTradeNo); err != nil {
				t.Fatal(err)
			}
			o, err := st.Reader().Orders().GetByOrderNo(bg, "app1", "OD-recovery", false)
			if err != nil {
				t.Fatal(err)
			}
			if o.Status != models.OrderPaid || o.CloseReason != models.CloseNone || o.PayAmount != amount || !o.PaidAt.Time.Equal(paidAt) || o.SettledOutTradeNo != in.OutTradeNo {
				t.Fatalf("invalid recovery: %+v", o)
			}
			for _, table := range []string{"payment_transactions", "order_events", "order_logs"} {
				var count int64
				if err := st.DB().Table(table).Count(&count).Error; err != nil {
					t.Fatal(err)
				}
				if count != 1 {
					t.Fatalf("%s count=%d", table, count)
				}
			}
		})
	}
}

func TestIntegrationRecoveryFailureIsAtomic(t *testing.T) {
	st := openTestStore(t)
	t.Cleanup(func() { _ = st.Close() })
	now := time.Now().Truncate(time.Second)
	err := st.WithinTx(bg, func(ctx context.Context, r store.Repos) error {
		if err := r.Orders().Insert(ctx, &models.Order{AppID: "app1", OrderNo: "OD-rollback", OrderToken: "rollback-token", UserID: 42,
			ProductID: 1, ProductTitle: "rollback", Status: models.OrderCancelled, Currency: "CNY", OriginalPrice: 90, PayAmount: 90,
			PayMethod: models.PayMethodWechat, PayChannel: "wechat", PayAccountID: "acc-wechat", ExpireAt: now}); err != nil {
			return err
		}
		return r.Attempts().Insert(ctx, &models.PaymentAttempt{AppID: "app1", OrderNo: "OD-rollback", OutTradeNo: "PT-rollback", AttemptNo: 1,
			Status: models.AttemptClosed, Channel: "wechat", PayAccountID: "acc-wechat", ExpireAt: now})
	})
	if err != nil {
		t.Fatal(err)
	}
	fault := errors.New("event creation failure")
	pay := paymentapp.New(paymentapp.Deps{Store: st, Accounts: gormstore.NewAccountResolver(st.DB()), Alerter: alert.Nop{}, NewEventID: func() (string, error) { return "", fault }})
	_, err = pay.ConfirmPaySuccess(bg, paymentapp.ConfirmInput{OutTradeNo: "PT-rollback", Channel: "wechat", PayAccountID: "acc-wechat", TransactionID: "tx-rollback", Amount: 90, Currency: "CNY", PaidAt: now})
	if !errors.Is(err, fault) {
		t.Fatalf("fault not propagated: %v", err)
	}
	o, err := st.Reader().Orders().GetByOrderNo(bg, "app1", "OD-rollback", false)
	if err != nil {
		t.Fatal(err)
	}
	if o.Status != models.OrderCancelled || o.SettledOutTradeNo != "" {
		t.Fatalf("order partially committed: %+v", o)
	}
	for _, table := range []string{"payment_transactions", "order_events", "order_logs"} {
		var count int64
		if err := st.DB().Table(table).Count(&count).Error; err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("%s partially committed: %d", table, count)
		}
	}
}
