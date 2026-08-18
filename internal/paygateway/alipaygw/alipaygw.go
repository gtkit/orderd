// Package alipaygw 用 gtkit/go-pay 的支付宝 Provider(paymgr.Provider)实现 paygateway.Gateway。
//
// 多账号:每个 pay_account_id 一个独立 Provider(规避 paymgr.Manager 单渠道单 Provider 覆盖,DESIGN §6),
// 由本包 Router 按 (channel, pay_account_id) 及 notify_route_id 路由。金额单位 paymgr 与 orderd 一致(分),不转换。
package alipaygw

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	alipayp "github.com/gtkit/go-pay/alipay"
	"github.com/gtkit/go-pay/paymgr"
	"github.com/gtkit/orderd/internal/paygateway"
)

// Config 单个支付宝账号配置。支持普通公钥模式与证书模式;私钥/证书由部署方经密钥系统/挂载注入,不入库。
// 字段与 go-pay 的 alipay.Config 对齐(path/内容双形态);orderd 补路由/运营字段(PayAccountID 等)。
type Config struct {
	// orderd 路由/运营。
	PayAccountID  string
	NotifyRouteID string
	NotifyURL     string
	TradeType     string // native(扫码,默认)/page(PC 收银台)/wap/app/jsapi

	// go-pay alipay.Config 凭据(敏感,env/挂载注入)。
	AppID          string
	PrivateKey     string
	PrivateKeyPath string
	IsProduction   bool
	// 证书模式(三项全填或全空):
	AppCertPublicKey        string
	AppCertPublicKeyPath    string
	AlipayRootCert          string
	AlipayRootCertPath      string
	AlipayCertPublicKey     string
	AlipayCertPublicKeyPath string
	// 普通公钥模式(未用证书模式时必填):
	AlipayPublicKey string
}

// Gateway 包装一个支付宝 Provider,实现 paygateway.Gateway,并暴露回调解析。
type Gateway struct {
	provider     paymgr.Provider
	payAccountID string
	notifyURL    string
	tradeType    paymgr.TradeType
}

// PayAccountID 返回该网关绑定的支付账号(回调按路由定位账号后供支付确认校验)。
func (g *Gateway) PayAccountID() string { return g.payAccountID }

// NewGateway 用普通公钥模式构造支付宝网关。
func NewGateway(cfg Config) (*Gateway, error) {
	p, err := alipayp.NewProviderWithConfig(&alipayp.Config{
		AppID:                   cfg.AppID,
		PrivateKey:              cfg.PrivateKey,
		PrivateKeyPath:          cfg.PrivateKeyPath,
		IsProduction:            cfg.IsProduction,
		AppCertPublicKey:        cfg.AppCertPublicKey,
		AppCertPublicKeyPath:    cfg.AppCertPublicKeyPath,
		AlipayRootCert:          cfg.AlipayRootCert,
		AlipayRootCertPath:      cfg.AlipayRootCertPath,
		AlipayCertPublicKey:     cfg.AlipayCertPublicKey,
		AlipayCertPublicKeyPath: cfg.AlipayCertPublicKeyPath,
		AlipayPublicKey:         cfg.AlipayPublicKey,
	})
	if err != nil {
		return nil, fmt.Errorf("alipaygw: 构造 provider 失败: %w", err)
	}
	tt := paymgr.TradeType(cfg.TradeType)
	if tt == "" {
		tt = paymgr.TradeTypeNative // 默认扫码(沙箱易测:出二维码,买家扫码付款)
	}
	return &Gateway{provider: p, payAccountID: cfg.PayAccountID, notifyURL: cfg.NotifyURL, tradeType: tt}, nil
}

// Prepare 统一下单;返回前端调起参数(优先 AppParams,其次 PayURL/CodeURL/H5URL)。
func (g *Gateway) Prepare(ctx context.Context, in paygateway.PrepareInput) (paygateway.PrepareResult, error) {
	rsp, err := g.provider.UnifiedOrder(ctx, &paymgr.UnifiedOrderRequest{
		OutTradeNo:  in.OutTradeNo,
		TotalAmount: in.Amount,
		Subject:     in.Subject,
		TradeType:   g.tradeType,
		NotifyURL:   g.notifyURL,
		ClientIP:    in.ClientIP,
		ExpireAt:    in.ExpireAt,
	})
	if err != nil {
		return paygateway.PrepareResult{}, err
	}
	return paygateway.PrepareResult{PrepayID: rsp.PrepayID, AppParams: firstNonEmpty(rsp.AppParams, rsp.PayURL, rsp.CodeURL, rsp.H5URL)}, nil
}

// Query 查单并映射为 paygateway.QueryState。
func (g *Gateway) Query(ctx context.Context, in paygateway.QueryInput) (paygateway.QueryResult, error) {
	rsp, err := g.provider.QueryOrder(ctx, &paymgr.QueryOrderRequest{OutTradeNo: in.OutTradeNo})
	if err != nil {
		// paymgr 未单列"订单不存在";尽力从错误串识别(支付宝 ACQ.TRADE_NOT_EXIST)。
		if strings.Contains(strings.ToUpper(err.Error()), "NOT_EXIST") {
			return paygateway.QueryResult{State: paygateway.QueryNotExist}, nil
		}
		return paygateway.QueryResult{State: paygateway.QueryError}, nil
	}
	switch rsp.TradeStatus {
	case paymgr.TradeStatusPaid, paymgr.TradeStatusRefunded:
		return paygateway.QueryResult{State: paygateway.QueryPaid, TransactionID: rsp.TransactionID, Amount: rsp.TotalAmount, PaidAt: rsp.PaidAt}, nil
	case paymgr.TradeStatusPending:
		return paygateway.QueryResult{State: paygateway.QueryNotPay}, nil
	case paymgr.TradeStatusClosed:
		return paygateway.QueryResult{State: paygateway.QueryClosed}, nil
	default:
		return paygateway.QueryResult{State: paygateway.QueryError}, nil
	}
}

// Close 关单。
func (g *Gateway) Close(ctx context.Context, in paygateway.CloseInput) error {
	return g.provider.CloseOrder(ctx, &paymgr.CloseOrderRequest{OutTradeNo: in.OutTradeNo})
}

// Refund 申请退款(支付宝同步:无错即受理)。
func (g *Gateway) Refund(ctx context.Context, in paygateway.RefundInput) (paygateway.RefundResult, error) {
	rsp, err := g.provider.Refund(ctx, &paymgr.RefundRequest{
		OutTradeNo: in.OutTradeNo, OutRefundNo: in.OutRefundNo,
		RefundAmount: in.RefundAmount, TotalAmount: in.TotalAmount, Reason: "",
	})
	if err != nil {
		return paygateway.RefundResult{Accepted: false}, err
	}
	return paygateway.RefundResult{GatewayRefundID: rsp.RefundID, Accepted: true}, nil
}

// RefundQuery 退款查单并映射。
func (g *Gateway) RefundQuery(ctx context.Context, in paygateway.RefundQueryInput) (paygateway.RefundQueryResult, error) {
	rsp, err := g.provider.QueryRefund(ctx, &paymgr.QueryRefundRequest{OutTradeNo: in.OutTradeNo, OutRefundNo: in.OutRefundNo})
	if err != nil {
		// paymgr 未单列"退款单不存在";尽力从错误串识别(支付宝 ACQ.TRADE_NOT_EXIST)。
		// 识别不了的错误一律 RefundQueryError(继续重试),绝不猜测性落终态。
		if strings.Contains(strings.ToUpper(err.Error()), "NOT_EXIST") {
			return paygateway.RefundQueryResult{State: paygateway.RefundQueryNotExist}, nil
		}
		return paygateway.RefundQueryResult{State: paygateway.RefundQueryError}, err
	}
	switch rsp.RefundStatus {
	case paymgr.RefundStatusSuccess:
		return paygateway.RefundQueryResult{State: paygateway.RefundQuerySucceeded, GatewayRefundID: rsp.RefundID, TransactionID: rsp.TransactionID, Amount: rsp.RefundAmount, SucceededAt: rsp.RefundedAt}, nil
	case paymgr.RefundStatusProcessing, paymgr.RefundStatusAbnormal:
		return paygateway.RefundQueryResult{State: paygateway.RefundQueryProcessing}, nil
	case paymgr.RefundStatusClosed:
		return paygateway.RefundQueryResult{State: paygateway.RefundQueryFailed}, nil
	default:
		return paygateway.RefundQueryResult{State: paygateway.RefundQueryError}, nil
	}
}

// Notify 是验签后的通知结果(供回调层)。
type Notify struct {
	OutTradeNo    string
	TransactionID string
	Paid          bool
	AmountFen     int64
}

// ParseNotify 验签+解析异步通知(回调层用);验签失败返回 error。
func (g *Gateway) ParseNotify(ctx context.Context, r *http.Request) (Notify, error) {
	nr, err := g.provider.ParseNotify(ctx, r)
	if err != nil {
		return Notify{}, err
	}
	return Notify{
		OutTradeNo: nr.OutTradeNo, TransactionID: nr.TransactionID,
		Paid:      nr.TradeStatus == paymgr.TradeStatusPaid || nr.TradeStatus == paymgr.TradeStatusRefunded,
		AmountFen: nr.TotalAmount,
	}, nil
}

func firstNonEmpty(ss ...string) string {
	for _, s := range ss {
		if s != "" {
			return s
		}
	}
	return ""
}
