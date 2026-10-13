// Package http 是支付回调的 HTTP 传输层。
//
// 支付宝(普通公钥模式)、微信(APIv3)均已接:按 URL 的 notify_route_id 定位账号网关 → ParseNotify 验签(+解密) →
// 由 out_trade_no 派生订单结算(payment.ConfirmPaySuccess,严格绑定 + 幂等 + 异常落库)→ 按渠道格式 ACK。
// 微信退款独立通知(/refund-notify/wechat)为可选加速器,退款终态由 RefundQueryWorker 收敛,暂返回 501。
package http

import (
	"net/http"
	"strings"

	"github.com/gtkit/logger"
	"github.com/gtkit/orderd/internal/appmeta"
	paymentapp "github.com/gtkit/orderd/internal/module/payment/application"
	"github.com/gtkit/orderd/internal/paygateway/alipaygw"
	"github.com/gtkit/orderd/internal/paygateway/wechatgw"
)

// Handler 支付回调 mux(含健康检查)。
type Handler struct {
	alipay  *alipaygw.Router // 可为 nil(支付宝未启用)
	wechat  *wechatgw.Router // 可为 nil(微信未启用)
	payment *paymentapp.Service
}

// New 构造回调处理器(alipayRouter / wechatRouter 均可为 nil)。
func New(alipayRouter *alipaygw.Router, wechatRouter *wechatgw.Router, payment *paymentapp.Service) *Handler {
	return &Handler{alipay: alipayRouter, wechat: wechatRouter, payment: payment}
}

// Register 注册回调与健康检查路由。
func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc(appmeta.AlipayNotifyPath, h.alipayNotify)
	mux.HandleFunc(appmeta.WechatNotifyPath, h.wechatNotify)
	mux.HandleFunc(appmeta.WechatRefundNotifyPath, h.notImplemented)
}

// alipayNotify 处理支付宝异步通知。ACK 约定:成功回 "success" 停止重投,其余触发重投。
func (h *Handler) alipayNotify(w http.ResponseWriter, r *http.Request) {
	routeID := strings.TrimPrefix(r.URL.Path, appmeta.AlipayNotifyPath)
	if h.alipay == nil {
		http.Error(w, "alipay not enabled", http.StatusNotImplemented)
		return
	}
	gw := h.alipay.ResolveByRoute(routeID)
	if gw == nil {
		logger.Warnw("支付宝回调:未知 notify_route_id", "route_id", routeID)
		http.Error(w, "unknown route", http.StatusBadRequest)
		return
	}
	n, err := gw.ParseNotify(r.Context(), r)
	if err != nil {
		logger.Warnw("支付宝回调验签失败", "route_id", routeID, "err", err)
		http.Error(w, "verify failed", http.StatusUnauthorized) // 非 success → 支付宝重投
		return
	}
	if !n.Paid {
		// 非成功状态(如未支付):无需结算,ACK 停止重投。
		writeAck(w)
		return
	}
	// 由 out_trade_no 唯一派生订单;渠道/账号取本路由账号供 ConfirmPaySuccess 严格校验。
	res, err := h.payment.ConfirmPaySuccess(r.Context(), paymentapp.ConfirmInput{
		OutTradeNo: n.OutTradeNo, Channel: "alipay", PayAccountID: gw.PayAccountID(),
		TransactionID: n.TransactionID, Amount: n.AmountFen, Currency: "CNY", PaidAt: n.PaidAt, NotifyDigest: "alipay-notify",
	})
	if err != nil {
		logger.Errorw("支付宝回调入账失败", "route_id", routeID, "out_trade_no", n.OutTradeNo, "err", err)
		http.Error(w, "process failed", http.StatusInternalServerError) // 触发重投
		return
	}
	logger.Infow("支付宝回调已处理", "out_trade_no", n.OutTradeNo, "outcome", res.Outcome)
	writeAck(w) // paid / duplicate / anomaly 均已落地,回 success 停止重投
}

// wechatNotify 处理微信异步通知。ACK 按微信 v3 约定(Provider.ACKNotify);非 2xx 触发重投。
func (h *Handler) wechatNotify(w http.ResponseWriter, r *http.Request) {
	routeID := strings.TrimPrefix(r.URL.Path, appmeta.WechatNotifyPath)
	if h.wechat == nil {
		http.Error(w, "wechat not enabled", http.StatusNotImplemented)
		return
	}
	gw := h.wechat.ResolveByRoute(routeID)
	if gw == nil {
		logger.Warnw("微信回调:未知 notify_route_id", "route_id", routeID)
		http.Error(w, "unknown route", http.StatusBadRequest)
		return
	}
	n, err := gw.ParseNotify(r.Context(), r)
	if err != nil {
		logger.Warnw("微信回调验签失败", "route_id", routeID, "err", err)
		http.Error(w, "verify failed", http.StatusUnauthorized) // 非 2xx → 微信重投
		return
	}
	if !n.Paid {
		gw.Ack(w) // 非成功状态无需结算,ACK 停止重投
		return
	}
	res, err := h.payment.ConfirmPaySuccess(r.Context(), paymentapp.ConfirmInput{
		OutTradeNo: n.OutTradeNo, Channel: "wechat", PayAccountID: gw.PayAccountID(),
		TransactionID: n.TransactionID, Amount: n.AmountFen, Currency: "CNY", PaidAt: n.PaidAt, NotifyDigest: "wechat-notify",
	})
	if err != nil {
		logger.Errorw("微信回调入账失败", "route_id", routeID, "out_trade_no", n.OutTradeNo, "err", err)
		http.Error(w, "process failed", http.StatusInternalServerError) // 触发重投
		return
	}
	logger.Infow("微信回调已处理", "out_trade_no", n.OutTradeNo, "outcome", res.Outcome)
	gw.Ack(w) // paid / duplicate / anomaly 均已落地,ACK 停止重投
}

func writeAck(w http.ResponseWriter) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("success"))
}

func (h *Handler) notImplemented(w http.ResponseWriter, r *http.Request) {
	logger.Warnw("payment/http: 收到回调但真实处理未接入(微信 V3 待接)", "path", r.URL.Path)
	http.Error(w, "not implemented", http.StatusNotImplemented)
}
