//go:build integration

package gormstore_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gtkit/orderd/internal/models"
	refundapp "github.com/gtkit/orderd/internal/module/refund/application"
	"github.com/gtkit/orderd/internal/paygateway"
	"github.com/gtkit/orderd/internal/shared/alert"
	"github.com/gtkit/orderd/internal/store"
)

func seedRefundRecoveryOrder(t *testing.T, st store.Store, channel string) {
	t.Helper()
	if err := st.Reader().Orders().Insert(t.Context(), &models.Order{
		AppID: "app1", OrderNo: "OD-refund-recovery", OrderToken: "refund-recovery-token", UserID: 42,
		Status: models.OrderPaid, ProductID: 1, ProductTitle: "refund", Currency: "CNY", OriginalPrice: 90, PayAmount: 90,
		PayChannel: channel, PayAccountID: "acc-" + channel, SettledChannel: channel, SettledPayAccountID: "acc-" + channel,
		SettledOutTradeNo: "original-out-trade", ExpireAt: time.Now().Add(time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
}

type countingRefundGateway struct {
	itGateway
	calls *atomic.Int32
}

func (g countingRefundGateway) Refund(context.Context, paygateway.RefundInput) (paygateway.RefundResult, error) {
	g.calls.Add(1)
	return paygateway.RefundResult{Accepted: true}, nil
}

type countingRefundRouter struct{ calls *atomic.Int32 }

func (r countingRefundRouter) Resolve(string, string) (paygateway.Gateway, error) {
	return countingRefundGateway{calls: r.calls}, nil
}

func TestIntegrationRefundConcurrentSameKeyAndEvidence(t *testing.T) {
	st := openTestStore(t)
	t.Cleanup(func() { _ = st.Close() })
	seedRefundRecoveryOrder(t, st, "wechat")
	var calls atomic.Int32
	var next atomic.Int64
	svc := refundapp.New(refundapp.Deps{
		Store: st, Router: countingRefundRouter{calls: &calls}, Alerter: alert.Nop{}, Enabled: true,
		NewRefundNo: func() string { return fmt.Sprintf("RF-concurrent-%d", next.Add(1)) },
	})
	var outs [8]refundapp.RefundOutcome
	var errs [8]error
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range len(outs) {
		wg.Go(func() {
			<-start
			outs[i], errs[i] = svc.RefundOrder(t.Context(), refundapp.RefundInput{AppID: "app1", OrderNo: "OD-refund-recovery", OutRefundNo: "same-refund", RefundAmount: 90})
		})
	}
	close(start)
	wg.Wait()
	for i, err := range errs {
		if err != nil || outs[i].RefundNo != outs[0].RefundNo {
			t.Fatalf("同键并发不一致: outs=%+v errs=%v", outs, errs)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("渠道提交%d次", calls.Load())
	}
	rf, err := st.Reader().Refunds().GetByRefundNo(t.Context(), outs[0].RefundNo, false)
	if err != nil || !rf.GatewayConfirmed || rf.GatewayRefundID != "" {
		t.Fatalf("无渠道ID的受理也须持久化: %+v %v", rf, err)
	}
	rf.NotExistCount = 4
	if err := st.Reader().Refunds().Update(t.Context(), rf); err != nil {
		t.Fatal(err)
	}
	rf.NotExistCount = 0
	if err := st.Reader().Refunds().Update(t.Context(), rf); err != nil {
		t.Fatal(err)
	}
	rollback := errors.New("rollback evidence")
	err = st.WithinTx(t.Context(), func(ctx context.Context, r store.Repos) error {
		cur, e := r.Refunds().GetByRefundNo(ctx, rf.RefundNo, true)
		if e != nil {
			return e
		}
		cur.NotExistCount = 5
		if e := r.Refunds().Update(ctx, cur); e != nil {
			return e
		}
		return rollback
	})
	if !errors.Is(err, rollback) {
		t.Fatalf("回滚错误: %v", err)
	}
	rf, err = st.Reader().Refunds().GetByRefundNo(t.Context(), rf.RefundNo, false)
	if err != nil || rf.NotExistCount != 0 || !rf.GatewayConfirmed {
		t.Fatalf("零值更新/回滚失效: %+v %v", rf, err)
	}
}

func TestIntegrationAlipayRefundLedgerAtomicity(t *testing.T) {
	st := openTestStore(t)
	t.Cleanup(func() { _ = st.Close() })
	seedRefundRecoveryOrder(t, st, "alipay")
	if err := st.Reader().Transactions().Insert(t.Context(), &models.PaymentTransaction{
		AppID: "app1", OrderNo: "OD-refund-recovery", Channel: "alipay", PayAccountID: "acc-alipay", Kind: models.TxnPay,
		OutTradeNo: "original-out-trade", TransactionID: "original-alipay-id", Amount: 90, Currency: "CNY",
	}); err != nil {
		t.Fatal(err)
	}
	rollback := errors.New("event ID unavailable")
	deps := refundapp.Deps{Store: st, Router: itRouter{}, Alerter: alert.Nop{}, Enabled: true,
		NewRefundNo: func() string { return "RF-alipay-recovery" }, NewEventID: func() (string, error) { return "", rollback },
	}
	svc := refundapp.New(deps)
	out, err := svc.RefundOrder(t.Context(), refundapp.RefundInput{AppID: "app1", OrderNo: "OD-refund-recovery", OutRefundNo: "alipay-refund", RefundAmount: 90})
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.ConfirmRefundSuccess(t.Context(), out.RefundNo, "original-alipay-id", 90, time.Now()); !errors.Is(err, rollback) {
		t.Fatalf("应事务回滚: %v", err)
	}
	var count int64
	if err := st.DB().Model(&models.PaymentTransaction{}).Where("kind = ?", models.TxnRefund).Count(&count).Error; err != nil || count != 0 {
		t.Fatalf("回滚后不得有退款流水: count=%d err=%v", count, err)
	}
	rf, _ := st.Reader().Refunds().GetByRefundNo(t.Context(), out.RefundNo, false)
	if rf.Status != models.RefundProcessing {
		t.Fatalf("回滚后状态: %+v", rf)
	}
	deps.NewEventID = func() (string, error) { return "evt-alipay-refund", nil }
	svc = refundapp.New(deps)
	for range 2 {
		if err := svc.ConfirmRefundSuccess(t.Context(), out.RefundNo, "original-alipay-id", 90, time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	txn, err := st.Reader().Transactions().GetByTxnID(t.Context(), "alipay", "acc-alipay", "refund:alipay-refund", false)
	if err != nil || txn == nil || txn.Kind != models.TxnRefund || txn.RefundNo != out.RefundNo {
		t.Fatalf("真实支付宝退款流水缺失: %+v %v", txn, err)
	}
	o, err := st.Reader().Orders().GetByOrderNo(t.Context(), "app1", "OD-refund-recovery", false)
	if err != nil || o.Status != models.OrderRefunded || o.RefundedAmount != 90 {
		t.Fatalf("落定错误: %+v %v", o, err)
	}
	if err := st.DB().Model(&models.OrderEvent{}).Where("event_type = ?", models.EventTypeRefunded).Count(&count).Error; err != nil || count != 1 {
		t.Fatalf("事件幂等错误: count=%d err=%v", count, err)
	}
}
