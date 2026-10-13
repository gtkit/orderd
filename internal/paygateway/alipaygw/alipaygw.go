// Package alipaygw 用 gtkit/go-pay 的支付宝 Provider(paymgr.Provider)实现 paygateway.Gateway。
//
// 多账号:每个 pay_account_id 一个独立 Provider(规避 paymgr.Manager 单渠道单 Provider 覆盖,DESIGN §6),
// 由本包 Router 按 (channel, pay_account_id) 及 notify_route_id 路由。金额单位 paymgr 与 orderd 一致(分),不转换。
package alipaygw

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

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
	SellerID      string // 必填:既有支付账号 MchID,用于验签后核对通知 seller_id。

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
	payAppID     string
	sellerID     string
}

// PayAccountID 返回该网关绑定的支付账号(回调按路由定位账号后供支付确认校验)。
func (g *Gateway) PayAccountID() string { return g.payAccountID }

// NewGateway 用普通公钥模式构造支付宝网关。
func NewGateway(cfg Config) (*Gateway, error) {
	if cfg.SellerID == "" {
		return nil, fmt.Errorf("alipaygw: seller_id is required")
	}
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
	return &Gateway{provider: p, payAccountID: cfg.PayAccountID, notifyURL: cfg.NotifyURL, tradeType: tt, payAppID: cfg.AppID, sellerID: cfg.SellerID}, nil
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
		// go-pay 把业务响应 ACQ.TRADE_NOT_EXIST 翻译为哨兵 ErrOrderNotFound(文本不含渠道码,只能按哨兵判定)。
		if errors.Is(err, paymgr.ErrOrderNotFound) {
			return paygateway.QueryResult{State: paygateway.QueryNotExist}, nil
		}
		return paygateway.QueryResult{State: paygateway.QueryError}, nil
	}
	if rsp == nil || rsp.OutTradeNo == "" || rsp.OutTradeNo != in.OutTradeNo {
		return paygateway.QueryResult{State: paygateway.QueryError}, errors.New("alipaygw: 支付查询响应的商户单号缺失/不匹配")
	}
	switch rsp.TradeStatus {
	case paymgr.TradeStatusPaid, paymgr.TradeStatusRefunded:
		return paygateway.QueryResult{State: paygateway.QueryPaid, TransactionID: rsp.TransactionID, Amount: rsp.TotalAmount, PaidAt: channelTime(rsp.PaidAt)}, nil
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
		// go-pay 对交易不存在(ACQ.TRADE_NOT_EXIST)与退款单号不存在(空响应)都返回哨兵 ErrOrderNotFound。
		// 其它错误一律 RefundQueryError(继续重试),绝不猜测性落终态。
		if errors.Is(err, paymgr.ErrOrderNotFound) {
			return paygateway.RefundQueryResult{State: paygateway.RefundQueryNotExist}, nil
		}
		return paygateway.RefundQueryResult{State: paygateway.RefundQueryError}, err
	}
	// 查询无错不等于资金归属成立;所有状态都必须绑定到本次原支付和退款请求。
	if rsp == nil || rsp.OutTradeNo == "" || rsp.OutRefundNo == "" ||
		rsp.OutTradeNo != in.OutTradeNo || rsp.OutRefundNo != in.OutRefundNo {
		return paygateway.RefundQueryResult{State: paygateway.RefundQueryError}, errors.New("alipaygw: 退款查询响应的原支付单号或退款号缺失/不匹配")
	}
	switch rsp.RefundStatus {
	case paymgr.RefundStatusSuccess:
		return paygateway.RefundQueryResult{State: paygateway.RefundQuerySucceeded, GatewayRefundID: rsp.RefundID, TransactionID: rsp.TransactionID, Amount: rsp.RefundAmount, SucceededAt: channelTime(rsp.RefundedAt)}, nil
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

// Notify 是验签后的通知结果(供回调层)。
type Notify struct {
	OutTradeNo    string
	TransactionID string
	Paid          bool
	AmountFen     int64
	PaidAt        time.Time
}

// ParseNotify 验签+解析异步通知(回调层用);验签失败返回 error。
func (g *Gateway) ParseNotify(ctx context.Context, r *http.Request) (Notify, error) {
	r.Body = http.MaxBytesReader(nil, r.Body, 1<<20)
	nr, err := g.provider.ParseNotify(ctx, r)
	if err != nil {
		return Notify{}, err
	}
	// SDK 只校验非空 app_id,并未校验 seller_id;必须在验签后核对同一份表单的完整身份。
	if err := r.ParseForm(); err != nil {
		return Notify{}, fmt.Errorf("%w: invalid notify form", paymgr.ErrInvalidNotify)
	}
	if g.payAppID == "" || g.sellerID == "" || r.Form.Get("app_id") != g.payAppID || r.Form.Get("seller_id") != g.sellerID {
		return Notify{}, fmt.Errorf("%w: notify account identity mismatch", paymgr.ErrInvalidNotify)
	}
	paid := nr.TradeStatus == paymgr.TradeStatusPaid || nr.TradeStatus == paymgr.TradeStatusRefunded
	if paid && (nr.TotalAmount <= 0 || !validNotifyAmount(r.Form.Get("total_amount"))) {
		return Notify{}, fmt.Errorf("%w: invalid payment amount", paymgr.ErrInvalidNotify)
	}
	return Notify{
		OutTradeNo: nr.OutTradeNo, TransactionID: nr.TransactionID,
		Paid:      paid,
		AmountFen: nr.TotalAmount, PaidAt: channelTime(nr.PaidAt),
	}, nil
}

// validNotifyAmount 拒绝 SDK 会截断的小数与非十进制写法,金额上界由 SDK 的 int64 解析再守。
func validNotifyAmount(amount string) bool {
	whole, fraction, decimal := strings.Cut(amount, ".")
	if whole == "" || decimal && (len(fraction) == 0 || len(fraction) > 2) {
		return false
	}
	for _, c := range whole + fraction {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

var channelLocation = time.FixedZone("GMT+8", 8*60*60)

// channelTime 修正 go-pay v1.6.1 用 time.Parse 把支付宝无时区日期解析为 UTC 的行为。
// gmt_payment/send_pay_date/gmt_refund_pay 的墙钟值属于 GMT+8,须重建而非调用 In 改显示。
// 升级 SDK 时须重验该转换,避免上游修复时区后重复修正;缺失时间保持零值。
func channelTime(t time.Time) time.Time {
	if t.IsZero() {
		return t
	}
	return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), channelLocation)
}

func firstNonEmpty(ss ...string) string {
	for _, s := range ss {
		if s != "" {
			return s
		}
	}
	return ""
}
