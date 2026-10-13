package worker

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/gtkit/orderd/internal/models"
	refundapp "github.com/gtkit/orderd/internal/module/refund/application"
	"github.com/gtkit/orderd/internal/paygateway"
	"github.com/gtkit/orderd/internal/store"
	"github.com/gtkit/orderd/internal/store/memstore"
)

type refundQueryHook struct {
	qGateway
	hook func(paygateway.RefundQueryInput) (paygateway.RefundQueryResult, error)
}

func (g refundQueryHook) RefundQuery(_ context.Context, in paygateway.RefundQueryInput) (paygateway.RefundQueryResult, error) {
	return g.hook(in)
}

func refundWithHook(mem *memstore.Mem, gw paygateway.Gateway, clock func() time.Time) *refundapp.Service {
	return refundapp.New(refundapp.Deps{
		Store: mem, Router: qRouter{gw: gw}, Alerter: nopAlerter{}, Clock: clock,
		NewEventID: func() (string, error) { return "refund-recovery-event", nil },
	})
}

func TestRefundQueryOldClaimCannotWrite(t *testing.T) {
	for _, state := range []paygateway.RefundQueryState{
		paygateway.RefundQueryError, paygateway.RefundQueryNotExist, paygateway.RefundQueryProcessing,
		paygateway.RefundQueryFailed, paygateway.RefundQueryAbnormal, paygateway.RefundQuerySucceeded,
	} {
		t.Run(fmt.Sprint(state), func(t *testing.T) {
			mem := memstore.New()
			seedRefund(t, mem, "OD-stale", "RF-stale", 5)
			ctx := t.Context()
			if err := mem.WithinTx(ctx, func(ctx context.Context, r store.Repos) error {
				rf, _ := r.Refunds().GetByRefundNo(ctx, "RF-stale", true)
				rf.LockedBy = "claim-A"
				rf.NotExistCount = 5
				return r.Refunds().Update(ctx, rf)
			}); err != nil {
				t.Fatal(err)
			}
			claimed, _ := mem.Reader().Refunds().GetByRefundNo(ctx, "RF-stale", false)
			newDue := qClock().Add(time.Hour)
			gw := refundQueryHook{hook: func(paygateway.RefundQueryInput) (paygateway.RefundQueryResult, error) {
				err := mem.WithinTx(ctx, func(ctx context.Context, r store.Repos) error {
					rf, _ := r.Refunds().GetByRefundNo(ctx, "RF-stale", true)
					rf.LockedBy = "claim-B"
					rf.LockedUntil = sql.NullTime{Time: newDue, Valid: true}
					rf.QueryCount = 9
					rf.NotExistCount = 2
					rf.NextQueryAt = sql.NullTime{Time: newDue, Valid: true}
					return r.Refunds().Update(ctx, rf)
				})
				if err != nil {
					return paygateway.RefundQueryResult{}, err
				}
				if state == paygateway.RefundQueryError {
					return paygateway.RefundQueryResult{}, errors.New("网络失败")
				}
				return paygateway.RefundQueryResult{State: state, Amount: 90, GatewayRefundID: "gwr-stale"}, nil
			}}
			ref := refundWithHook(mem, gw, qClock)
			err := ref.QueryRefund(ctx, claimed)
			if (err != nil) != (state == paygateway.RefundQueryError) {
				t.Fatalf("query: %v", err)
			}
			w := NewRefundQueryWorker(mem, ref, nopAlerter{}, RefundQueryConfig{}, qClock)
			w.rescheduleIfProcessing(ctx, claimed.RefundNo, "claim-A")
			current, _ := mem.Reader().Refunds().GetByRefundNo(ctx, claimed.RefundNo, false)
			if state == paygateway.RefundQuerySucceeded {
				if current.Status != models.RefundSucceeded {
					t.Fatalf("真实成功不得丢失: %+v", current)
				}
				return
			}
			if current.Status != models.RefundProcessing || current.QueryCount != 9 || current.NotExistCount != 2 || current.LockedBy != "claim-B" || !current.NextQueryAt.Time.Equal(newDue) {
				t.Fatalf("旧token覆盖当前认领: %+v", current)
			}
			w.rescheduleIfProcessing(ctx, claimed.RefundNo, "claim-B")
			current, _ = mem.Reader().Refunds().GetByRefundNo(ctx, claimed.RefundNo, false)
			if current.QueryCount != 10 || current.NotExistCount != 2 || current.LockedBy != "" {
				t.Fatalf("有效重排须基于最新计数: %+v", current)
			}
		})
	}
}

func TestRefundQueryClaimsImmediatelyBeforeEachCall(t *testing.T) {
	mem := memstore.New()
	seedRefund(t, mem, "OD-first", "RF-first", 0)
	seedRefund(t, mem, "OD-second", "RF-second", 0)
	now := qClock()
	clock := func() time.Time { return now }
	tokens := map[string]bool{}
	calls := 0
	gw := refundQueryHook{hook: func(in paygateway.RefundQueryInput) (paygateway.RefundQueryResult, error) {
		calls++
		for _, no := range []string{"RF-first", "RF-second"} {
			rf, _ := mem.Reader().Refunds().GetByRefundNo(t.Context(), no, false)
			if rf.OutRefundNo == in.OutRefundNo {
				if rf.LockedBy == "" || tokens[rf.LockedBy] || !rf.LockedUntil.Time.After(now) {
					t.Fatalf("无独立有效租约: %+v", rf)
				}
				tokens[rf.LockedBy] = true
			} else if calls == 1 && rf.LockedBy != "" {
				t.Fatalf("尾部退款不能提前消耗租约: %+v", rf)
			}
		}
		// 每条执行40秒,大于30秒默认租期;第二条仍须从开始执行时获得新租约。
		now = now.Add(40 * time.Second)
		return paygateway.RefundQueryResult{State: paygateway.RefundQueryProcessing}, nil
	}}
	ref := refundWithHook(mem, gw, clock)
	w := NewRefundQueryWorker(mem, ref, nopAlerter{}, RefundQueryConfig{BatchSize: 2}, clock)
	if n, err := w.RunOnce(t.Context()); err != nil || n != 2 || calls != 2 || len(tokens) != 2 {
		t.Fatalf("逐条认领: n=%d calls=%d tokens=%d err=%v", n, calls, len(tokens), err)
	}
}
