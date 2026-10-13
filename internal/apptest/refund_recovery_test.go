package apptest

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
	"github.com/gtkit/orderd/internal/store"
)

// 支付宝 SDK 的 RefundID 就是原支付 TradeNo,不能假设有独立渠道退款号。
func TestAlipayRefundRetainsIndependentLedger(t *testing.T) {
	h := newHarness(true)
	in := baseCreate()
	in.PayMethod = models.PayMethodAlipay
	out, err := h.order.CreateOrder(t.Context(), in)
	if err != nil {
		t.Fatal(err)
	}
	paid := confirmInput(h, out.OrderNo, "acc-alipay", "alipay-original", 90)
	paid.Channel = "alipay"
	if _, err := h.payment.ConfirmPaySuccess(t.Context(), paid); err != nil {
		t.Fatal(err)
	}
	rf, err := h.refund.RefundOrder(t.Context(), refundapp.RefundInput{
		AppID: "app1", OrderNo: out.OrderNo, RefundAmount: 90, OutRefundNo: "alipay-r1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := h.refund.ConfirmRefundSuccess(t.Context(), rf.RefundNo, "alipay-original", 90, time.Now()); err != nil {
		t.Fatal(err)
	}
	txns := refundTxns(h, rf.RefundNo)
	if len(txns) != 1 || txns[0].TransactionID != "refund:alipay-r1" {
		t.Fatalf("支付宝退款必须有独立流水: %+v", txns)
	}
}

func TestRefundLedgerConflictRequiresSameFact(t *testing.T) {
	for _, field := range []string{"same", "kind", "refund", "amount", "currency", "order", "app", "out_refund"} {
		t.Run(field, func(t *testing.T) {
			h := newHarness(true)
			orderNo, _ := h.createAndPay(t, "")
			out, err := h.refund.RefundOrder(t.Context(), refundapp.RefundInput{AppID: "app1", OrderNo: orderNo, RefundAmount: 90, OutRefundNo: "ledger"})
			if err != nil {
				t.Fatal(err)
			}
			txn := &models.PaymentTransaction{AppID: "app1", OrderNo: orderNo, RefundNo: out.RefundNo, PayAccountID: "acc-wechat", Channel: "wechat", Kind: models.TxnRefund, OutTradeNo: "ledger", TransactionID: "gwr-ledger", Amount: 90, Currency: "CNY"}
			switch field {
			case "kind":
				txn.Kind = models.TxnPay
			case "refund":
				txn.RefundNo = "other"
			case "amount":
				txn.Amount = 1
			case "currency":
				txn.Currency = "USD"
			case "order":
				txn.OrderNo = "other"
			case "app":
				txn.AppID = "other"
			case "out_refund":
				txn.OutTradeNo = "other"
			}
			if err := h.mem.Reader().Transactions().Insert(t.Context(), txn); err != nil {
				t.Fatal(err)
			}
			err = h.refund.ConfirmRefundSuccess(t.Context(), out.RefundNo, "gwr-ledger", 90, time.Now())
			if field == "same" {
				if err != nil {
					t.Fatal(err)
				}
				if len(refundTxns(h, out.RefundNo)) != 1 {
					t.Fatal("相同资金事实不得新增流水")
				}
				return
			}
			if !errors.Is(err, models.ErrPaymentAnomaly) {
				t.Fatalf("应拒绝资金冲突: %v", err)
			}
			rf, _ := h.refund.GetRefund(t.Context(), "app1", out.RefundNo, "")
			o, _ := h.mem.Reader().Orders().GetByOrderNo(t.Context(), "app1", orderNo, false)
			if rf.Status != models.RefundProcessing || o.Status != models.OrderRefunding || o.RefundedAmount != 0 || countEvents(h, models.EventTypeRefunded) != 0 || h.alerter.count("refund_ledger_conflict") != 1 {
				t.Fatalf("冲突不得落成功: refund=%+v order=%+v", rf, o)
			}
		})
	}
}

type refundQueryGateway struct {
	paygateway.Gateway
	query           func() (paygateway.RefundQueryResult, error)
	acceptWithoutID bool
}

func (g refundQueryGateway) Refund(ctx context.Context, in paygateway.RefundInput) (paygateway.RefundResult, error) {
	if g.acceptWithoutID {
		return paygateway.RefundResult{Accepted: true}, nil
	}
	return g.Gateway.Refund(ctx, in)
}

func (g refundQueryGateway) RefundQuery(context.Context, paygateway.RefundQueryInput) (paygateway.RefundQueryResult, error) {
	return g.query()
}

type refundTestRouter struct{ gateway paygateway.Gateway }

func (r refundTestRouter) Resolve(string, string) (paygateway.Gateway, error) { return r.gateway, nil }

func refundServiceForTest(h *harness, st store.Store, gateway paygateway.Gateway) *refundapp.Service {
	var next atomic.Int64
	return refundapp.New(refundapp.Deps{
		Store: st, Router: refundTestRouter{gateway}, Alerter: h.alerter, Enabled: true,
		NewRefundNo: func() string { return fmt.Sprintf("RF-new-%d", next.Add(1)) },
		NewEventID:  func() (string, error) { return fmt.Sprintf("refund-event-%d", next.Add(1)), nil },
	})
}

func TestRefundQueryEvidenceSequence(t *testing.T) {
	absent := paygateway.RefundQueryNotExist
	queryError := paygateway.RefundQueryError
	for _, tc := range []struct {
		name      string
		accepted  bool
		states    []paygateway.RefundQueryState
		want      models.RefundStatus
		count     int32
		confirmed bool
	}{
		{"errors_then_absent", false, []paygateway.RefundQueryState{queryError, queryError, queryError, queryError, queryError, absent}, models.RefundProcessing, 1, false},
		{"continuous_absent", false, []paygateway.RefundQueryState{absent, absent, absent, absent, absent, absent}, models.RefundAbnormal, 6, false},
		{"error_breaks_absence", false, []paygateway.RefundQueryState{absent, absent, queryError, absent}, models.RefundProcessing, 1, false},
		{"processing_breaks_absence", false, []paygateway.RefundQueryState{absent, absent, paygateway.RefundQueryProcessing, absent}, models.RefundProcessing, 1, true},
		{"accepted_then_absent", true, []paygateway.RefundQueryState{absent, absent, absent, absent, absent, absent}, models.RefundAbnormal, 6, true},
		{"processing_then_absent", false, []paygateway.RefundQueryState{paygateway.RefundQueryProcessing, absent, absent, absent, absent, absent, absent}, models.RefundAbnormal, 6, true},
		{"channel_abnormal", true, []paygateway.RefundQueryState{paygateway.RefundQueryAbnormal}, models.RefundAbnormal, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(true)
			orderNo, _ := h.createAndPay(t, "")
			if !tc.accepted {
				h.gateway.refundErr = errors.New("提交未知")
			}
			out, err := h.refund.RefundOrder(t.Context(), refundapp.RefundInput{AppID: "app1", OrderNo: orderNo, RefundAmount: 90, OutRefundNo: "evidence"})
			if err != nil {
				t.Fatal(err)
			}
			for _, state := range tc.states {
				gw := refundQueryGateway{Gateway: h.gateway, query: func() (paygateway.RefundQueryResult, error) {
					if state == paygateway.RefundQueryError {
						return paygateway.RefundQueryResult{}, errors.New("网络错误")
					}
					return paygateway.RefundQueryResult{State: state, GatewayRefundID: "real-refund"}, nil
				}}
				// 每次重建 Service 验证连续证据不依赖进程内状态。
				svc := refundServiceForTest(h, h.mem, gw)
				rf, _ := h.mem.Reader().Refunds().GetByRefundNo(t.Context(), out.RefundNo, false)
				err := svc.QueryRefund(t.Context(), rf)
				if (err != nil) != (state == paygateway.RefundQueryError) {
					t.Fatalf("查单错误=%v state=%v", err, state)
				}
			}
			rf, _ := h.refund.GetRefund(t.Context(), "app1", out.RefundNo, "")
			if rf.Status != tc.want || rf.NotExistCount != tc.count || rf.GatewayConfirmed != tc.confirmed {
				t.Fatalf("证据错误: %+v", rf)
			}
			if tc.want == models.RefundAbnormal {
				if _, err := h.refund.RefundOrder(t.Context(), refundapp.RefundInput{AppID: "app1", OrderNo: orderNo, RefundAmount: 90, OutRefundNo: "another"}); !errors.Is(err, models.ErrRefundNotAllowed) {
					t.Fatalf("异常退款须保持守卫: %v", err)
				}
				resolved, err := h.refund.ResolveRefundAbnormal(t.Context(), refundapp.ResolveInput{RefundNo: out.RefundNo, Target: models.RefundSucceeded, ActualAmount: 90, Actor: "tester", Remark: "核实成功"})
				if err != nil || resolved.OrderStatus != models.OrderRefunded {
					t.Fatalf("人工落定: %+v %v", resolved, err)
				}
			}
		})
	}
}

// 强制两个调用都在事务外读到幂等键缺失,确保检验锁内复用而非快查碰巧命中。
type refundBarrierStore struct {
	store.Store
	arrived chan struct{}
	release chan struct{}
	reads   atomic.Int32
}

func (s *refundBarrierStore) Reader() store.Repos {
	return refundBarrierRepos{Repos: s.Store.Reader(), owner: s}
}

type refundBarrierRepos struct {
	store.Repos
	owner *refundBarrierStore
}

func (r refundBarrierRepos) Refunds() store.RefundRepo {
	return refundBarrierRepo{RefundRepo: r.Repos.Refunds(), owner: r.owner}
}

type refundBarrierRepo struct {
	store.RefundRepo
	owner *refundBarrierStore
}

func (r refundBarrierRepo) FindByOutRefundNo(ctx context.Context, appID, no string) (*models.Refund, error) {
	rf, err := r.RefundRepo.FindByOutRefundNo(ctx, appID, no)
	if r.owner.reads.Add(1) <= 2 {
		r.owner.arrived <- struct{}{}
		<-r.owner.release
	}
	return rf, err
}

func TestRefundConcurrentIdempotency(t *testing.T) {
	for _, source := range []string{"order", "anomaly"} {
		for _, different := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/conflict=%v", source, different), func(t *testing.T) {
				h := newHarness(true)
				orderNo, _ := h.createAndPay(t, "")
				var txnID uint64
				if source == "anomaly" {
					txn := &models.PaymentTransaction{AppID: "app1", OrderNo: orderNo, PayAccountID: "acc-wechat", Channel: "wechat", Kind: models.TxnPay, OutTradeNo: "other-trade", TransactionID: "second-payment", Amount: 90, Currency: "CNY", Anomaly: true}
					if err := h.mem.Reader().Transactions().Insert(t.Context(), txn); err != nil {
						t.Fatal(err)
					}
					txnID = txn.ID
				}
				st := &refundBarrierStore{Store: h.mem, arrived: make(chan struct{}, 2), release: make(chan struct{})}
				svc := refundServiceForTest(h, st, h.gateway)
				var outs [2]refundapp.RefundOutcome
				var errs [2]error
				var wg sync.WaitGroup
				for i := range 2 {
					wg.Go(func() {
						reason := "same"
						if different && i == 1 {
							reason = "different"
						}
						if source == "order" {
							outs[i], errs[i] = svc.RefundOrder(t.Context(), refundapp.RefundInput{AppID: "app1", OrderNo: orderNo, RefundAmount: 90, OutRefundNo: "concurrent", Reason: reason})
						} else {
							outs[i], errs[i] = svc.RefundAnomalyTxn(t.Context(), refundapp.AnomalyRefundInput{TxnID: txnID, OutRefundNo: "concurrent", Reason: reason, Actor: "tester"})
						}
					})
				}
				<-st.arrived
				<-st.arrived
				close(st.release)
				wg.Wait()
				if different {
					conflicts := 0
					for _, err := range errs {
						if errors.Is(err, models.ErrIdempotencyConflict) {
							conflicts++
						} else if err != nil {
							t.Fatal(err)
						}
					}
					if conflicts != 1 {
						t.Fatalf("不同请求必须恰一冲突: %v", errs)
					}
				} else if errs[0] != nil || errs[1] != nil || outs[0].RefundNo != outs[1].RefundNo || outs[0].Reused == outs[1].Reused {
					t.Fatalf("相同请求应复用: outs=%+v errors=%v", outs, errs)
				}
				if len(h.gateway.refunds) != 1 {
					t.Fatalf("重复提交渠道: %+v", h.gateway.refunds)
				}
			})
		}
	}
}

func TestRefundAcceptedWithoutIDStillProtectsFunds(t *testing.T) {
	h := newHarness(true)
	orderNo, _ := h.createAndPay(t, "")
	gw := refundQueryGateway{Gateway: h.gateway, acceptWithoutID: true, query: func() (paygateway.RefundQueryResult, error) {
		return paygateway.RefundQueryResult{State: paygateway.RefundQueryNotExist}, nil
	}}
	svc := refundServiceForTest(h, h.mem, gw)
	out, err := svc.RefundOrder(t.Context(), refundapp.RefundInput{AppID: "app1", OrderNo: orderNo, RefundAmount: 90, OutRefundNo: "accepted-empty-id"})
	if err != nil {
		t.Fatal(err)
	}
	for range 6 {
		rf, _ := svc.GetRefund(t.Context(), "app1", out.RefundNo, "")
		if err := svc.QueryRefund(t.Context(), rf); err != nil {
			t.Fatal(err)
		}
	}
	rf, _ := svc.GetRefund(t.Context(), "app1", out.RefundNo, "")
	if !rf.GatewayConfirmed || rf.GatewayRefundID != "" || rf.Status != models.RefundAbnormal {
		t.Fatalf("明确受理不能因缺少渠道ID失去保护: %+v", rf)
	}
}

func TestRefundLargeAmountAtomicAndIdempotent(t *testing.T) {
	h := newHarness(true)
	const amount int64 = 1 << 40
	h.order = orderWithAmounts(h, fakeAmounts{b: models.AmountBounds{MinPayable: 1, MaxPayable: amount, AllowedBusiness: map[string]bool{"vip": true}}})
	in := baseCreate()
	in.Amount = models.Money{Original: amount, Payable: amount, Currency: "CNY"}
	order, err := h.order.CreateOrder(t.Context(), in)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.payment.ConfirmPaySuccess(t.Context(), confirmInput(h, order.OrderNo, "acc-wechat", "large-payment", amount)); err != nil {
		t.Fatal(err)
	}
	out, err := h.refund.RefundOrder(t.Context(), refundapp.RefundInput{AppID: "app1", OrderNo: order.OrderNo, RefundAmount: amount, OutRefundNo: "large-refund"})
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := h.refund.ConfirmRefundSuccess(t.Context(), out.RefundNo, "large-gateway-refund", amount, time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	o, _ := h.mem.Reader().Orders().GetByOrderNo(t.Context(), "app1", order.OrderNo, false)
	txns := refundTxns(h, out.RefundNo)
	if o.RefundedAmount != amount || len(txns) != 1 || txns[0].Amount != amount || countEvents(h, models.EventTypeRefunded) != 1 {
		t.Fatalf("大额退款截断或重复: amount=%d txns=%+v", o.RefundedAmount, txns)
	}
}

type refundUpdateFailStore struct {
	store.Store
	fail atomic.Bool
}

func (s *refundUpdateFailStore) WithinTx(ctx context.Context, fn func(context.Context, store.Repos) error) error {
	return s.Store.WithinTx(ctx, func(ctx context.Context, r store.Repos) error {
		return fn(ctx, refundUpdateFailRepos{Repos: r, owner: s})
	})
}

type refundUpdateFailRepos struct {
	store.Repos
	owner *refundUpdateFailStore
}

func (r refundUpdateFailRepos) Refunds() store.RefundRepo {
	return refundUpdateFailRepo{RefundRepo: r.Repos.Refunds(), owner: r.owner}
}

type refundUpdateFailRepo struct {
	store.RefundRepo
	owner *refundUpdateFailStore
}

func (r refundUpdateFailRepo) Update(ctx context.Context, rf *models.Refund) error {
	if r.owner.fail.CompareAndSwap(true, false) {
		return errors.New("退款受理证据写入失败")
	}
	return r.RefundRepo.Update(ctx, rf)
}

func TestRefundAcceptedEvidenceWriteFailureCannotReleaseGuard(t *testing.T) {
	h := newHarness(true)
	orderNo, _ := h.createAndPay(t, "")
	st := &refundUpdateFailStore{Store: h.mem}
	st.fail.Store(true)
	gw := refundQueryGateway{Gateway: h.gateway, query: func() (paygateway.RefundQueryResult, error) {
		return paygateway.RefundQueryResult{State: paygateway.RefundQueryNotExist}, nil
	}}
	svc := refundServiceForTest(h, st, gw)
	out, err := svc.RefundOrder(t.Context(), refundapp.RefundInput{AppID: "app1", OrderNo: orderNo, RefundAmount: 90, OutRefundNo: "accepted-not-persisted"})
	if err != nil {
		t.Fatal(err)
	}
	rf, _ := svc.GetRefund(t.Context(), "app1", out.RefundNo, "")
	if rf.GatewayConfirmed || rf.Status != models.RefundCreated || h.alerter.count("refund_processing_update_failed") != 1 {
		t.Fatalf("故障注入必须丢失受理证据: %+v", rf)
	}
	for range 6 {
		rf, _ := svc.GetRefund(t.Context(), "app1", out.RefundNo, "")
		if err := svc.QueryRefund(t.Context(), rf); err != nil {
			t.Fatal(err)
		}
	}
	rf, _ = svc.GetRefund(t.Context(), "app1", out.RefundNo, "")
	o, _ := h.mem.Reader().Orders().GetByOrderNo(t.Context(), "app1", orderNo, false)
	if rf.Status != models.RefundAbnormal || o.Status != models.OrderRefundException {
		t.Fatalf("丢证据不能释放资金守卫: rf=%+v order=%+v", rf, o)
	}
	if _, err := svc.RefundOrder(t.Context(), refundapp.RefundInput{AppID: "app1", OrderNo: orderNo, RefundAmount: 90, OutRefundNo: "retry-another-key"}); !errors.Is(err, models.ErrRefundNotAllowed) {
		t.Fatalf("不应允许再次退款: %v", err)
	}
}

func TestRefundQueryZeroAmountIsNotInvented(t *testing.T) {
	h := newHarness(true)
	orderNo, _ := h.createAndPay(t, "")
	out, err := h.refund.RefundOrder(t.Context(), refundapp.RefundInput{AppID: "app1", OrderNo: orderNo, RefundAmount: 90, OutRefundNo: "missing-amount"})
	if err != nil {
		t.Fatal(err)
	}
	h.gateway.refundQuerySet = true
	h.gateway.refundQuery = paygateway.RefundQueryResult{State: paygateway.RefundQuerySucceeded, GatewayRefundID: "zero-amount"}
	rf, _ := h.refund.GetRefund(t.Context(), "app1", out.RefundNo, "")
	if err := h.refund.QueryRefund(t.Context(), rf); err != nil {
		t.Fatal(err)
	}
	rf, _ = h.refund.GetRefund(t.Context(), "app1", out.RefundNo, "")
	if rf.Status != models.RefundAbnormal || len(refundTxns(h, rf.RefundNo)) != 0 || countEvents(h, models.EventTypeRefunded) != 0 {
		t.Fatalf("缺失金额不得补造成功: %+v", rf)
	}
}

func TestRefundQuerySuccessTimePreservesChannelFact(t *testing.T) {
	for _, succeededAt := range []time.Time{{}, time.Date(2026, 9, 30, 23, 59, 59, 0, time.FixedZone("UTC+8", 8*60*60))} {
		t.Run(succeededAt.String(), func(t *testing.T) {
			h := newHarness(true)
			orderNo, _ := h.createAndPay(t, "")
			out, err := h.refund.RefundOrder(t.Context(), refundapp.RefundInput{AppID: "app1", OrderNo: orderNo, RefundAmount: 90, OutRefundNo: "success-time"})
			if err != nil {
				t.Fatal(err)
			}
			h.gateway.refundQuerySet = true
			h.gateway.refundQuery = paygateway.RefundQueryResult{State: paygateway.RefundQuerySucceeded, GatewayRefundID: "timed-refund", Amount: 90, SucceededAt: succeededAt}
			rf, _ := h.refund.GetRefund(t.Context(), "app1", out.RefundNo, "")
			if err := h.refund.QueryRefund(t.Context(), rf); err != nil {
				t.Fatal(err)
			}
			rf, _ = h.refund.GetRefund(t.Context(), "app1", out.RefundNo, "")
			txns := refundTxns(h, rf.RefundNo)
			if rf.Status != models.RefundSucceeded || len(txns) != 1 || rf.SucceededAt.Valid != !succeededAt.IsZero() || txns[0].PaidAt.Valid != !succeededAt.IsZero() || !rf.SucceededAt.Time.Equal(succeededAt) || !txns[0].PaidAt.Time.Equal(succeededAt) {
				t.Fatalf("不得以本机时间代替渠道资金时间: rf=%+v txns=%+v", rf, txns)
			}
		})
	}
}

func TestRefundManualSuccessPreservesKnownOrUnknownTime(t *testing.T) {
	for _, succeededAt := range []time.Time{{}, time.Date(2026, 9, 30, 23, 59, 59, 0, time.FixedZone("UTC+8", 8*60*60))} {
		t.Run(succeededAt.String(), func(t *testing.T) {
			h := newHarness(true)
			orderNo, _ := h.createAndPay(t, "")
			out, err := h.refund.RefundOrder(t.Context(), refundapp.RefundInput{AppID: "app1", OrderNo: orderNo, RefundAmount: 90, OutRefundNo: "manual-time"})
			if err != nil {
				t.Fatal(err)
			}
			if err := h.refund.ConfirmRefundSuccess(t.Context(), out.RefundNo, "manual-time-refund", 1, succeededAt); err != nil {
				t.Fatal(err)
			}
			if _, err := h.refund.ResolveRefundAbnormal(t.Context(), refundapp.ResolveInput{RefundNo: out.RefundNo, Target: models.RefundSucceeded, ActualAmount: 90, Actor: "tester", Remark: "对账确认完整退款"}); err != nil {
				t.Fatal(err)
			}
			rf, _ := h.refund.GetRefund(t.Context(), "app1", out.RefundNo, "")
			txns := refundTxns(h, rf.RefundNo)
			if rf.Status != models.RefundSucceeded || len(txns) != 1 || rf.SucceededAt.Valid != !succeededAt.IsZero() || txns[0].PaidAt.Valid != !succeededAt.IsZero() || !rf.SucceededAt.Time.Equal(succeededAt) || !txns[0].PaidAt.Time.Equal(succeededAt) {
				t.Fatalf("人工操作时间不得替换渠道资金时间: rf=%+v txns=%+v", rf, txns)
			}
		})
	}
}

type refundCancellationGateway struct{ paygateway.Gateway }

func (refundCancellationGateway) RefundQuery(ctx context.Context, _ paygateway.RefundQueryInput) (paygateway.RefundQueryResult, error) {
	return paygateway.RefundQueryResult{}, ctx.Err()
}

func TestRefundQueryPropagatesWorkerCancellation(t *testing.T) {
	h := newHarness(true)
	orderNo, _ := h.createAndPay(t, "")
	out, err := h.refund.RefundOrder(t.Context(), refundapp.RefundInput{AppID: "app1", OrderNo: orderNo, RefundAmount: 90, OutRefundNo: "cancel-query"})
	if err != nil {
		t.Fatal(err)
	}
	rf, _ := h.refund.GetRefund(t.Context(), "app1", out.RefundNo, "")
	svc := refundServiceForTest(h, h.mem, refundCancellationGateway{Gateway: h.gateway})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := svc.QueryRefund(ctx, rf); !errors.Is(err, context.Canceled) {
		t.Fatalf("查单须传播worker取消: %v", err)
	}
}

func TestRefundAbnormalManualOutcomesBySource(t *testing.T) {
	for _, source := range []string{"order", "anomaly"} {
		for _, target := range []models.RefundStatus{models.RefundSucceeded, models.RefundFailed} {
			t.Run(fmt.Sprintf("%s/target=%d", source, target), func(t *testing.T) {
				h := newHarness(true)
				orderNo, _ := h.createAndPay(t, "")
				var txnID uint64
				if source == "anomaly" {
					txn := &models.PaymentTransaction{AppID: "app1", OrderNo: orderNo, PayAccountID: "acc-wechat", Channel: "wechat", Kind: models.TxnPay, OutTradeNo: "extra-payment", TransactionID: "extra-txn", Amount: 90, Currency: "CNY", Anomaly: true}
					if err := h.mem.Reader().Transactions().Insert(t.Context(), txn); err != nil {
						t.Fatal(err)
					}
					txnID = txn.ID
				}
				start := func(key string) (refundapp.RefundOutcome, error) {
					if source == "anomaly" {
						return h.refund.RefundAnomalyTxn(t.Context(), refundapp.AnomalyRefundInput{TxnID: txnID, OutRefundNo: key, Actor: "tester"})
					}
					return h.refund.RefundOrder(t.Context(), refundapp.RefundInput{AppID: "app1", OrderNo: orderNo, OutRefundNo: key, RefundAmount: 90})
				}
				out, err := start("manual-outcome")
				if err != nil {
					t.Fatal(err)
				}
				h.gateway.refundQuerySet = true
				h.gateway.refundQuery = paygateway.RefundQueryResult{State: paygateway.RefundQueryAbnormal, GatewayRefundID: "abnormal-refund"}
				for range 2 {
					rf, _ := h.refund.GetRefund(t.Context(), "app1", out.RefundNo, "")
					if err := h.refund.QueryRefund(t.Context(), rf); err != nil {
						t.Fatal(err)
					}
				}
				if h.alerter.count("refund_channel_abnormal") != 1 {
					t.Fatal("重复查询不应重复告警")
				}
				amount := int64(0)
				if target == models.RefundSucceeded {
					amount = 90
				}
				resolved, err := h.refund.ResolveRefundAbnormal(t.Context(), refundapp.ResolveInput{RefundNo: out.RefundNo, Target: target, ActualAmount: amount, Actor: "tester", Remark: "渠道核实"})
				if err != nil || resolved.Status != target || resolved.HasOrder != (source == "order") {
					t.Fatalf("人工处置失败: %+v %v", resolved, err)
				}
				o, _ := h.mem.Reader().Orders().GetByOrderNo(t.Context(), "app1", orderNo, false)
				wantStatus, wantAmount, wantEvents := models.OrderPaid, int64(0), 0
				if source == "order" && target == models.RefundSucceeded {
					wantStatus, wantAmount, wantEvents = models.OrderRefunded, 90, 1
				}
				if o.Status != wantStatus || o.RefundedAmount != wantAmount || countEvents(h, models.EventTypeRefunded) != wantEvents {
					t.Fatalf("退款来源不得串改订单: %+v", o)
				}
				ledgerCount := 0
				if target == models.RefundSucceeded {
					ledgerCount = 1
				}
				audit, err := h.mem.Reader().OpsAudit().ListByTarget(t.Context(), models.OpsTargetRefund, out.RefundNo, 0, 10)
				if err != nil || len(audit) != 1 || len(refundTxns(h, out.RefundNo)) != ledgerCount {
					t.Fatalf("资金流水或审计不一致: audit=%+v err=%v", audit, err)
				}
				if target == models.RefundFailed {
					if _, err := start("verified-not-refunded"); err != nil {
						t.Fatalf("人工确认未退款后须释放守卫: %v", err)
					}
				}
			})
		}
	}
}
