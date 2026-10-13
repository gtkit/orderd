package runtime

import (
	"context"
	"testing"
	"time"

	"github.com/gtkit/orderd/internal/models"
	"github.com/gtkit/orderd/internal/platform/config"
	"github.com/gtkit/orderd/internal/shared/bill"
)

type runtimeBillFake struct{ fetches, verifications int }

func (f *runtimeBillFake) Fetch(context.Context, string, string, time.Time, func(bill.Entry) error) (bill.Summary, error) {
	f.fetches++
	return bill.Summary{}, nil
}
func (f *runtimeBillFake) Verify(context.Context, string, string, bill.Entry) (bill.Success, error) {
	f.verifications++
	return bill.Success{}, nil
}
func TestBillRouteRequiresExactChannelAndAccount(t *testing.T) {
	source := &runtimeBillFake{}
	r := billRouter{"wechat": {accountID: "expected", source: source}}
	for _, tc := range []struct {
		ch, account string
		bad         bool
	}{{"wechat", "expected", false}, {"alipay", "expected", true}, {"wechat", "other", true}} {
		_, err := r.Fetch(t.Context(), tc.ch, tc.account, time.Now(), func(bill.Entry) error { return nil })
		if (err != nil) != tc.bad {
			t.Fatalf("fetch route: %v", err)
		}
		_, err = r.Verify(t.Context(), tc.ch, tc.account, bill.Entry{})
		if (err != nil) != tc.bad {
			t.Fatalf("verify route: %v", err)
		}
	}
	if source.fetches != 1 || source.verifications != 1 {
		t.Fatal("foreign account reached provider")
	}
}
func TestBillBuildRequiresEnabledRealChannel(t *testing.T) {
	if _, err := buildBillRouter(t.Context(), config.Config{ReconcileEnabled: true}, nil); err == nil {
		t.Fatal("enabled reconciliation without channels accepted")
	}
	if _, err := buildBillRouter(t.Context(), config.Config{}, nil); err != nil {
		t.Fatal(err)
	}
}

type repairFake struct{ calls int }

func (f *repairFake) VerifyAndConfirmFromBill(context.Context, bill.RepairRequest) (bool, error) {
	f.calls++
	return true, nil
}
func TestRepairRouterSeparatesFundsKinds(t *testing.T) {
	pay, refund := &repairFake{}, &repairFake{}
	r := positiveRepairRouter{pay, refund}
	for _, kind := range []models.TxnKind{models.TxnPay, models.TxnRefund} {
		if ok, err := r.VerifyAndConfirmFromBill(t.Context(), bill.RepairRequest{Entry: bill.Entry{Kind: kind}}); !ok || err != nil {
			t.Fatal(err)
		}
	}
	if ok, err := r.VerifyAndConfirmFromBill(t.Context(), bill.RepairRequest{}); ok || err == nil {
		t.Fatal("unknown kind accepted")
	}
	if pay.calls != 1 || refund.calls != 1 {
		t.Fatal("funds routing failed")
	}
}
