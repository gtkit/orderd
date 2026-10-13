// Package wechatgw 用 gtkit/go-pay 的微信 Provider(微信官方 APIv3 SDK 封装)实现 paygateway.Gateway。
//
// 多账号:每个 pay_account_id 一个独立 Provider(规避 paymgr.Manager 单渠道单 Provider 覆盖,DESIGN §6),
// 由本包 Router 按 (channel=wechat, pay_account_id) 及 notify_route_id 路由。金额单位与 orderd 一致(分),不转换。
// 与 alipaygw 同抽象;差异在:微信凭据(证书/APIv3 密钥)、回调 ACK 用 Provider.ACKNotify(v3 约定)。
package wechatgw

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/gtkit/go-pay/paymgr"
	wechatp "github.com/gtkit/go-pay/wechat"
	"github.com/gtkit/orderd/internal/paygateway"
	"github.com/wechatpay-apiv3/wechatpay-go/services/payments"
	"github.com/wechatpay-apiv3/wechatpay-go/utils"
)

// provider 是本网关依赖的微信能力子集:paymgr.Provider(下单/查单/关单/退款/查退款/验签)
// 外加微信特有的 ACKNotify(v3 成功应答)。以接口声明便于单测注入 fake。
type provider interface {
	paymgr.Provider
	ACKNotify(w http.ResponseWriter)
}

// 编译期保证真实微信 Provider 满足本接口。
var _ provider = (*wechatp.Provider)(nil)

// Config 单个微信商户账号配置。敏感项(证书序列号/APIv3 密钥/商户私钥/平台证书或公钥)
// 由部署方经环境/密钥系统注入,不入库。
type Config struct {
	PayAccountID  string
	NotifyRouteID string
	NotifyURL     string
	TradeType     string // native(扫码,默认)/jsapi/app/h5

	AppID               string
	MchID               string
	MchCertSerialNumber string
	MchAPIv3Key         string
	MchPrivateKeyPath   string
	MchPrivateKeyPEM    string

	// 平台证书模式(与公钥模式二选一):
	WechatPayCertificatePath string
	WechatPayCertificatePEM  string
	// 公钥模式(2024 起新商户;配 PublicKeyID + 公钥来源即启用):
	WechatPayPublicKeyID   string
	WechatPayPublicKeyPath string
	WechatPayPublicKeyPEM  string
}

// Gateway 包装一个微信 Provider,实现 paygateway.Gateway,并暴露回调解析与 ACK。
type Gateway struct {
	provider     provider
	payAccountID string
	notifyURL    string
	tradeType    paymgr.TradeType
	payAppID     string
	mchID        string
	apiKey       string
}

// PayAccountID 返回该网关绑定的支付账号(回调按路由定位账号后供支付确认校验)。
func (g *Gateway) PayAccountID() string { return g.payAccountID }

// NewGateway 构造微信网关。需 ctx:微信 SDK 初始化会拉取/校验平台证书(网络)。
func NewGateway(ctx context.Context, cfg Config) (*Gateway, error) {
	p, err := wechatp.NewProviderWithConfig(ctx, &wechatp.Config{
		AppID:                    cfg.AppID,
		MchID:                    cfg.MchID,
		MchCertSerialNumber:      cfg.MchCertSerialNumber,
		MchAPIv3Key:              cfg.MchAPIv3Key,
		MchPrivateKeyPath:        cfg.MchPrivateKeyPath,
		MchPrivateKeyPEM:         cfg.MchPrivateKeyPEM,
		WechatPayCertificatePath: cfg.WechatPayCertificatePath,
		WechatPayCertificatePEM:  cfg.WechatPayCertificatePEM,
		WechatPayPublicKeyID:     cfg.WechatPayPublicKeyID,
		WechatPayPublicKeyPath:   cfg.WechatPayPublicKeyPath,
		WechatPayPublicKeyPEM:    cfg.WechatPayPublicKeyPEM,
	})
	if err != nil {
		return nil, fmt.Errorf("wechatgw: 构造 provider 失败: %w", err)
	}
	tt := paymgr.TradeType(cfg.TradeType)
	if tt == "" {
		tt = paymgr.TradeTypeNative // 默认扫码(Native)
	}
	return &Gateway{provider: p, payAccountID: cfg.PayAccountID, notifyURL: cfg.NotifyURL, tradeType: tt,
		payAppID: cfg.AppID, mchID: cfg.MchID, apiKey: cfg.MchAPIv3Key}, nil
}

// Prepare 统一下单;返回前端调起参数(优先 AppParams,其次 CodeURL/PayURL/H5URL)。
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
	return paygateway.PrepareResult{PrepayID: rsp.PrepayID, AppParams: firstNonEmpty(rsp.AppParams, rsp.CodeURL, rsp.PayURL, rsp.H5URL)}, nil
}

// Query 查单并映射为 paygateway.QueryState。
func (g *Gateway) Query(ctx context.Context, in paygateway.QueryInput) (paygateway.QueryResult, error) {
	rsp, err := g.provider.QueryOrder(ctx, &paymgr.QueryOrderRequest{OutTradeNo: in.OutTradeNo})
	if err != nil {
		if channelCode(err) == "ORDER_NOT_EXIST" {
			return paygateway.QueryResult{State: paygateway.QueryNotExist}, nil
		}
		return paygateway.QueryResult{State: paygateway.QueryError}, nil
	}
	if rsp == nil || rsp.OutTradeNo == "" || rsp.OutTradeNo != in.OutTradeNo {
		return paygateway.QueryResult{State: paygateway.QueryError}, errors.New("wechatgw: 支付查询响应的商户单号缺失/不匹配")
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

// Refund 申请退款(微信异步:无错即受理,进入处理中,最终以退款查单/通知为准)。
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
		// 仅渠道码 RESOURCE_NOT_EXISTS 视为退款单不存在;其它错误一律 RefundQueryError(继续重试),绝不猜测性落终态。
		if channelCode(err) == "RESOURCE_NOT_EXISTS" {
			return paygateway.RefundQueryResult{State: paygateway.RefundQueryNotExist}, nil
		}
		return paygateway.RefundQueryResult{State: paygateway.RefundQueryError}, err
	}
	// 微信实际只按退款号查询,必须复核原支付单号,避免共用商户时误认另一笔同额退款。
	if rsp == nil || rsp.OutTradeNo == "" || rsp.OutRefundNo == "" ||
		rsp.OutTradeNo != in.OutTradeNo || rsp.OutRefundNo != in.OutRefundNo {
		return paygateway.RefundQueryResult{State: paygateway.RefundQueryError}, errors.New("wechatgw: 退款查询响应的原支付单号或退款号缺失/不匹配")
	}
	switch rsp.RefundStatus {
	case paymgr.RefundStatusSuccess:
		return paygateway.RefundQueryResult{State: paygateway.RefundQuerySucceeded, GatewayRefundID: rsp.RefundID, TransactionID: rsp.TransactionID, Amount: rsp.RefundAmount, SucceededAt: rsp.RefundedAt}, nil
	case paymgr.RefundStatusProcessing:
		return paygateway.RefundQueryResult{State: paygateway.RefundQueryProcessing, GatewayRefundID: rsp.RefundID}, nil
	case paymgr.RefundStatusAbnormal:
		return paygateway.RefundQueryResult{State: paygateway.RefundQueryAbnormal, GatewayRefundID: rsp.RefundID}, nil
	case paymgr.RefundStatusClosed:
		return paygateway.RefundQueryResult{State: paygateway.RefundQueryFailed}, nil
	default:
		return paygateway.RefundQueryResult{State: paygateway.RefundQueryError}, nil
	}
}

// channelCode 取 go-pay 包装的微信 APIv3 错误码(ChannelError.Code);非 ChannelError 返回空串。
// 按码精确判定"资源不存在"(查单 ORDER_NOT_EXIST、退款查单 RESOURCE_NOT_EXISTS),不匹配描述文本:
// 误判为确定性"无此单"会触发关单或退款失败回退。
func channelCode(err error) string {
	if ce, ok := errors.AsType[*paymgr.ChannelError](err); ok {
		return ce.Code
	}
	return ""
}

// Notify 是验签后的通知结果(供回调层)。
type Notify struct {
	OutTradeNo    string
	TransactionID string
	Paid          bool
	AmountFen     int64
	PaidAt        time.Time
}

// ParseNotify 验签+解密异步通知(回调层用);验签失败返回 error。
func (g *Gateway) ParseNotify(ctx context.Context, r *http.Request) (Notify, error) {
	// 只缓存有界原文;业务字段必须等 SDK 完成验签后才解析。
	bodyReader := r.Body
	defer func() { _ = bodyReader.Close() }()
	body, err := io.ReadAll(http.MaxBytesReader(nil, r.Body, 1<<20))
	if err != nil {
		return Notify{}, fmt.Errorf("%w: invalid notify body", paymgr.ErrInvalidNotify)
	}
	r.Body = io.NopCloser(bytes.NewReader(body))
	nr, err := g.provider.ParseNotify(ctx, r)
	if err != nil {
		return Notify{}, err
	}
	// paymgr 的统一结果未保留身份,从已验签密文用官方 SDK 解密核对,不接受缺省身份。
	var envelope struct {
		Resource struct {
			AssociatedData string `json:"associated_data"`
			Nonce          string `json:"nonce"`
			Ciphertext     string `json:"ciphertext"`
		} `json:"resource"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil || len(envelope.Resource.Nonce) != 12 {
		return Notify{}, fmt.Errorf("%w: invalid verified resource", paymgr.ErrInvalidNotify)
	}
	plain, err := utils.DecryptAES256GCM(g.apiKey, envelope.Resource.AssociatedData, envelope.Resource.Nonce, envelope.Resource.Ciphertext)
	if err != nil {
		return Notify{}, fmt.Errorf("%w: invalid verified ciphertext", paymgr.ErrInvalidNotify)
	}
	var transaction payments.Transaction
	if err := json.Unmarshal([]byte(plain), &transaction); err != nil {
		return Notify{}, fmt.Errorf("%w: invalid verified transaction", paymgr.ErrInvalidNotify)
	}
	if g.payAppID == "" || g.mchID == "" || transaction.Appid == nil || transaction.Mchid == nil ||
		*transaction.Appid != g.payAppID || *transaction.Mchid != g.mchID {
		return Notify{}, fmt.Errorf("%w: notify account identity mismatch", paymgr.ErrInvalidNotify)
	}
	paid := nr.TradeStatus == paymgr.TradeStatusPaid || nr.TradeStatus == paymgr.TradeStatusRefunded
	if paid && nr.TotalAmount <= 0 {
		return Notify{}, fmt.Errorf("%w: invalid payment amount", paymgr.ErrInvalidNotify)
	}
	return Notify{
		OutTradeNo: nr.OutTradeNo, TransactionID: nr.TransactionID,
		Paid:      paid,
		AmountFen: nr.TotalAmount, PaidAt: nr.PaidAt,
	}, nil
}

// Ack 按微信 v3 约定写成功应答(停止重投)。
func (g *Gateway) Ack(w http.ResponseWriter) { g.provider.ACKNotify(w) }

func firstNonEmpty(ss ...string) string {
	for _, s := range ss {
		if s != "" {
			return s
		}
	}
	return ""
}
