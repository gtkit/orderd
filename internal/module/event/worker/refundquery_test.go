package worker

import (
	"context"
	"testing"
	"time"

	"github.com/gtkit/orderd/internal/models"
	refundapp "github.com/gtkit/orderd/internal/module/refund/application"
	"github.com/gtkit/orderd/internal/paygateway"
	"github.com/gtkit/orderd/internal/shared/alert"
	"github.com/gtkit/orderd/internal/store"
	"github.com/gtkit/orderd/internal/store/memstore"
)

func TestRefundQueryWorkerSucceeds(t *testing.T) {
	mem := memstore.New()
	// 播种:REFUNDING 订单(结算渠道 wechat)+ 一笔 PROCESSING 退款。
	err := mem.WithinTx(bg, func(ctx context.Context, r store.Repos) error {
		if e := r.Orders().Insert(ctx, &models.Order{
			AppID: "app1", OrderNo: "OD-r", OrderToken: "tok-r", UserID: 42, Status: models.OrderRefunding,
			ProductID: 1, ProductTitle: "x", Currency: "CNY", OriginalPrice: 90, PayAmount: 90,
			PayMethod: models.PayMethodWechat, PayChannel: "wechat", PayAccountID: "acc-wechat",
			SettledChannel: "wechat", SettledOutTradeNo: "PT-r", SettledPayAccountID: "acc-wechat",
			ExpireAt: qClock().Add(time.Hour),
		}); e != nil {
			return e
		}
		return r.Refunds().Insert(ctx, &models.Refund{
			AppID: "app1", RefundNo: "RF-r", OutRefundNo: "or-r", OrderNo: "OD-r", PayAccountID: "acc-wechat",
			RefundAmount: 90, Status: models.RefundProcessing,
		})
	})
	if err != nil {
		t.Fatalf("播种失败: %v", err)
	}

	var n int64
	router := qRouter{gw: qGateway{refundQR: paygateway.RefundQueryResult{
		State: paygateway.RefundQuerySucceeded, GatewayRefundID: "gwr", TransactionID: "rtx", Amount: 90,
	}}}
	ref := refundapp.New(refundapp.Deps{
		Store: mem, Router: router, Accounts: qAccounts{}, Alerter: nopAlerter{}, Clock: qClock,
		NewRefundNo: func() string { n++; return "RF-x" + itoaW(n) }, NewEventID: func() (string, error) { n++; return "evt-r-" + itoaW(n), nil },
		Enabled: true,
	})
	rw := NewRefundQueryWorker(mem, ref, nopAlerter{}, RefundQueryConfig{}, qClock)
	if got, err := rw.RunOnce(bg); err != nil || got != 1 {
		t.Fatalf("RunOnce: got=%d err=%v", got, err)
	}

	rf, _ := mem.Reader().Refunds().GetByRefundNo(bg, "RF-r")
	if rf.Status != models.RefundSucceeded {
		t.Fatalf("退款应 SUCCEEDED,实际: %v", rf.Status)
	}
	o, _ := mem.Reader().Orders().GetByOrderNo(bg, "app1", "OD-r", false)
	if o.Status != models.OrderRefunded || o.RefundedAmount != 90 {
		t.Fatalf("订单应 REFUNDED 且累计已退 90,实际: status=%v refunded=%d", o.Status, o.RefundedAmount)
	}
	refunded := false
	for _, e := range mem.Events() {
		if e.EventType == models.EventTypeRefunded {
			refunded = true
		}
	}
	if !refunded {
		t.Fatal("应产生 refunded 事件")
	}
}

// seedRefund 播种一个 REFUNDING 订单 + 一笔 PROCESSING 退款(已查 queryCount 次)。
func seedRefund(t *testing.T, mem *memstore.Mem, orderNo, refundNo string, queryCount int32) {
	t.Helper()
	err := mem.WithinTx(bg, func(ctx context.Context, r store.Repos) error {
		if e := r.Orders().Insert(ctx, &models.Order{
			AppID: "app1", OrderNo: orderNo, OrderToken: "tok-" + orderNo, UserID: 42, Status: models.OrderRefunding,
			ProductID: 1, ProductTitle: "x", Currency: "CNY", OriginalPrice: 90, PayAmount: 90,
			PayMethod: models.PayMethodWechat, PayChannel: "wechat", PayAccountID: "acc-wechat",
			SettledChannel: "wechat", SettledOutTradeNo: "PT-" + orderNo, SettledPayAccountID: "acc-wechat",
			ExpireAt: qClock().Add(time.Hour),
		}); e != nil {
			return e
		}
		return r.Refunds().Insert(ctx, &models.Refund{
			AppID: "app1", RefundNo: refundNo, OutRefundNo: "or-" + refundNo, OrderNo: orderNo,
			PayAccountID: "acc-wechat", RefundAmount: 90, Status: models.RefundProcessing, QueryCount: queryCount,
		})
	})
	if err != nil {
		t.Fatalf("播种失败: %v", err)
	}
}

// buildRefundQueryWorker 用指定退款查单结果与告警端口构造退款查单 worker。
func buildRefundQueryWorker(mem *memstore.Mem, qr paygateway.RefundQueryResult, alerter alert.Alerter) *RefundQueryWorker {
	var n int64
	router := qRouter{gw: qGateway{refundQR: qr}}
	ref := refundapp.New(refundapp.Deps{
		Store: mem, Router: router, Accounts: qAccounts{}, Alerter: nopAlerter{}, Clock: qClock,
		NewRefundNo: func() string { n++; return "RF-x" + itoaW(n) },
		NewEventID:  func() (string, error) { n++; return "evt-r-" + itoaW(n), nil },
		Enabled:     true,
	})
	return NewRefundQueryWorker(mem, ref, alerter, RefundQueryConfig{MaxQueries: 6}, qClock)
}

// seedCreatedRefund 播种一个 REFUNDING 订单 + 一笔 CREATED 退款(createdAt 显式指定)。
func seedCreatedRefund(t *testing.T, mem *memstore.Mem, orderNo, refundNo string, createdAt time.Time) {
	t.Helper()
	err := mem.WithinTx(bg, func(ctx context.Context, r store.Repos) error {
		if e := r.Orders().Insert(ctx, &models.Order{
			AppID: "app1", OrderNo: orderNo, OrderToken: "tok-" + orderNo, UserID: 42, Status: models.OrderRefunding,
			ProductID: 1, ProductTitle: "x", Currency: "CNY", OriginalPrice: 90, PayAmount: 90,
			PayMethod: models.PayMethodWechat, PayChannel: "wechat", PayAccountID: "acc-wechat",
			SettledChannel: "wechat", SettledOutTradeNo: "PT-" + orderNo, SettledPayAccountID: "acc-wechat",
			ExpireAt: qClock().Add(time.Hour),
		}); e != nil {
			return e
		}
		return r.Refunds().Insert(ctx, &models.Refund{
			AppID: "app1", RefundNo: refundNo, OutRefundNo: "or-" + refundNo, OrderNo: orderNo,
			PayAccountID: "acc-wechat", RefundAmount: 90, Status: models.RefundCreated, CreatedAt: createdAt,
		})
	})
	if err != nil {
		t.Fatalf("播种失败: %v", err)
	}
}

// TestRefundQueryWorkerRecoversStuckCreated 反证"退款查单 worker 兜底 CREATED 滞留":
// 滞留 CREATED(超过阈值)被恢复为 PROCESSING 并在同轮被认领查单收敛(旧实现只认领 PROCESSING,
// CREATED 永久滞留,本测试失败);阈值内的新建 CREATED 不动。
func TestRefundQueryWorkerRecoversStuckCreated(t *testing.T) {
	t.Run("滞留CREATED恢复并收敛", func(t *testing.T) {
		mem := memstore.New()
		seedCreatedRefund(t, mem, "OD-c1", "RF-c1", qClock().Add(-10*time.Minute))
		rw := buildRefundQueryWorker(mem, paygateway.RefundQueryResult{
			State: paygateway.RefundQuerySucceeded, GatewayRefundID: "gwr", Amount: 90,
		}, nopAlerter{})
		if got, err := rw.RunOnce(bg); err != nil || got != 1 {
			t.Fatalf("恢复后应在同轮被认领: got=%d err=%v", got, err)
		}
		rf, _ := mem.Reader().Refunds().GetByRefundNo(bg, "RF-c1")
		if rf.Status != models.RefundSucceeded {
			t.Fatalf("滞留 CREATED 应恢复并收敛到 SUCCEEDED,实际: %v", rf.Status)
		}
		o, _ := mem.Reader().Orders().GetByOrderNo(bg, "app1", "OD-c1", false)
		if o.Status != models.OrderRefunded {
			t.Fatalf("订单应 REFUNDED,实际: %v", o.Status)
		}
	})
	t.Run("阈值内CREATED不动", func(t *testing.T) {
		mem := memstore.New()
		seedCreatedRefund(t, mem, "OD-c2", "RF-c2", qClock().Add(-30*time.Second)) // 阈值默认 2 分钟
		rw := buildRefundQueryWorker(mem, paygateway.RefundQueryResult{State: paygateway.RefundQuerySucceeded}, nopAlerter{})
		if got, err := rw.RunOnce(bg); err != nil || got != 0 {
			t.Fatalf("阈值内不应恢复/认领: got=%d err=%v", got, err)
		}
		rf, _ := mem.Reader().Refunds().GetByRefundNo(bg, "RF-c2")
		if rf.Status != models.RefundCreated {
			t.Fatalf("阈值内 CREATED 应保持不动,实际: %v", rf.Status)
		}
	})
	t.Run("恢复幂等", func(t *testing.T) {
		// 已恢复(PROCESSING)后再扫不会重复处理 CREATED 集合;收敛后再跑一轮无副作用。
		mem := memstore.New()
		seedCreatedRefund(t, mem, "OD-c3", "RF-c3", qClock().Add(-10*time.Minute))
		rw := buildRefundQueryWorker(mem, paygateway.RefundQueryResult{
			State: paygateway.RefundQuerySucceeded, GatewayRefundID: "gwr", Amount: 90,
		}, nopAlerter{})
		if _, err := rw.RunOnce(bg); err != nil {
			t.Fatalf("首轮失败: %v", err)
		}
		if got, err := rw.RunOnce(bg); err != nil || got != 0 {
			t.Fatalf("已收敛后再跑一轮应无认领: got=%d err=%v", got, err)
		}
		rf, _ := mem.Reader().Refunds().GetByRefundNo(bg, "RF-c3")
		if rf.Status != models.RefundSucceeded {
			t.Fatalf("状态应保持 SUCCEEDED,实际: %v", rf.Status)
		}
	})
}

func TestRefundQueryStallAlertEdgeTriggered(t *testing.T) {
	processing := paygateway.RefundQueryResult{State: paygateway.RefundQueryProcessing}
	succeeded := paygateway.RefundQueryResult{
		State: paygateway.RefundQuerySucceeded, GatewayRefundID: "gwr", TransactionID: "rtx", Amount: 90,
	}
	tests := []struct {
		name       string
		queryCount int32 // 播种时已查次数;本轮重排后为 queryCount+1
		qr         paygateway.RefundQueryResult
		wantAlert  bool
		wantStatus models.RefundStatus
	}{
		{"未达阈值不告警", 4, processing, false, models.RefundProcessing},
		{"跨过阈值告警一次", 5, processing, true, models.RefundProcessing},
		{"已超过阈值不再告警", 6, processing, false, models.RefundProcessing},
		{"阈值前已收敛不告警", 5, succeeded, false, models.RefundSucceeded},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mem := memstore.New()
			seedRefund(t, mem, "OD-rs", "RF-rs", tc.queryCount)
			rec := &recAlerter{}
			rw := buildRefundQueryWorker(mem, tc.qr, rec)
			if got, err := rw.RunOnce(bg); err != nil || got != 1 {
				t.Fatalf("RunOnce: got=%d err=%v", got, err)
			}
			if got := rec.has("refund_query_stalled"); got != tc.wantAlert {
				t.Fatalf("refund_query_stalled 告警期望 %v,实际 %v", tc.wantAlert, got)
			}
			rf, err := mem.Reader().Refunds().GetByRefundNo(bg, "RF-rs")
			if err != nil {
				t.Fatalf("读退款失败: %v", err)
			}
			if rf.Status != tc.wantStatus {
				t.Fatalf("退款状态期望 %v,实际 %v", tc.wantStatus, rf.Status)
			}
		})
	}
}
