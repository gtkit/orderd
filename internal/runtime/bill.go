package runtime

import (
	"context"
	"errors"
	"time"

	"github.com/gtkit/orderd/internal/models"
	"github.com/gtkit/orderd/internal/platform/config"
	"github.com/gtkit/orderd/internal/shared/bill"
	alibill "github.com/gtkit/orderd/internal/shared/bill/alipay"
	wcbill "github.com/gtkit/orderd/internal/shared/bill/wechat"
)

type channelBill interface {
	bill.Source
	bill.Verifier
}
type billRoute struct {
	accountID string
	source    channelBill
}
type billRouter map[string]billRoute

func (r billRouter) Fetch(ctx context.Context, channel, accountID string, day time.Time, emit func(bill.Entry) error) (bill.Summary, error) {
	route, ok := r[channel]
	if !ok || route.accountID != accountID {
		return bill.Summary{}, errors.New("bill: account has no configured channel source")
	}
	return route.source.Fetch(ctx, channel, accountID, day, emit)
}
func (r billRouter) Verify(ctx context.Context, channel, accountID string, e bill.Entry) (bill.Success, error) {
	route, ok := r[channel]
	if !ok || route.accountID != accountID {
		return bill.Success{}, errors.New("bill: account has no configured verifier")
	}
	return route.source.Verify(ctx, channel, accountID, e)
}

type billAccountResolver interface {
	ResolveByID(context.Context, string) (models.AppPayAccount, error)
}

func buildBillRouter(ctx context.Context, cfg config.Config, accounts billAccountResolver) (billRouter, error) {
	r := billRouter{}
	if !cfg.ReconcileEnabled {
		return r, nil
	}
	if a := cfg.Pay.Alipay; a.Enabled {
		actx, cancel := context.WithTimeout(ctx, 5*time.Second)
		acct, err := accounts.ResolveByID(actx, a.AccountID)
		cancel()
		if err != nil {
			return nil, err
		}
		if !acct.Enabled || acct.Channel != "alipay" || acct.PayAppID != a.AppID || acct.MchID == "" {
			return nil, errors.New("bill: alipay account identity mismatch")
		}
		source, err := alibill.New(alibill.Config{PayAccountID: a.AccountID, SellerID: acct.MchID, AppID: a.AppID, PrivateKey: a.PrivateKey, PrivateKeyPath: a.PrivateKeyPath, IsProduction: a.IsProduction,
			AppCertPublicKey: a.AppCertPublicKey, AppCertPublicKeyPath: a.AppCertPublicKeyPath, AlipayRootCert: a.AlipayRootCert, AlipayRootCertPath: a.AlipayRootCertPath,
			AlipayCertPublicKey: a.AlipayCertPublicKey, AlipayCertPublicKeyPath: a.AlipayCertPublicKeyPath, AlipayPublicKey: a.AlipayPublicKey})
		if err != nil {
			return nil, err
		}
		r["alipay"] = billRoute{a.AccountID, source}
	}
	if w := cfg.Pay.Wechat; w.Enabled {
		actx, cancel := context.WithTimeout(ctx, 5*time.Second)
		acct, err := accounts.ResolveByID(actx, w.AccountID)
		cancel()
		if err != nil {
			return nil, err
		}
		if !acct.Enabled || acct.Channel != "wechat" || acct.PayAppID != w.AppID || acct.MchID != w.MchID {
			return nil, errors.New("bill: wechat account identity mismatch")
		}
		source, err := wcbill.New(ctx, wcbill.Config{PayAccountID: w.AccountID, AppID: w.AppID, MchID: w.MchID, MchCertSerialNumber: w.MchCertSerialNumber, MchPrivateKeyPath: w.MchPrivateKeyPath, MchPrivateKeyPEM: w.MchPrivateKeyPEM,
			WechatPayCertificatePath: w.WechatpayCertificatePath, WechatPayCertificatePEM: w.WechatpayCertificatePEM,
			WechatPayPublicKeyID: w.WechatpayPublicKeyID, WechatPayPublicKeyPath: w.WechatpayPublicKeyPath, WechatPayPublicKeyPEM: w.WechatpayPublicKeyPEM})
		if err != nil {
			return nil, err
		}
		r["wechat"] = billRoute{w.AccountID, source}
	}
	if len(r) == 0 {
		return nil, errors.New("bill: reconciliation requires an enabled real payment channel")
	}
	return r, nil
}

type positiveRepairRouter struct{ payment, refund bill.Repairer }

func (r positiveRepairRouter) VerifyAndConfirmFromBill(ctx context.Context, in bill.RepairRequest) (bool, error) {
	switch in.Entry.Kind {
	case models.TxnPay:
		return r.payment.VerifyAndConfirmFromBill(ctx, in)
	case models.TxnRefund:
		return r.refund.VerifyAndConfirmFromBill(ctx, in)
	default:
		return false, errors.New("bill: unsupported funds kind")
	}
}
