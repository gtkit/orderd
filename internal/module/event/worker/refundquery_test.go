package worker

import (
	"context"
	"testing"
	"time"

	"github.com/gtkit/orderd/internal/models"
	refundapp "github.com/gtkit/orderd/internal/module/refund/application"
	"github.com/gtkit/orderd/internal/paygateway"
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
	rw := NewRefundQueryWorker(mem, ref, RefundQueryConfig{}, qClock)
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
