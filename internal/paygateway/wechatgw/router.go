package wechatgw

import (
	"context"

	"github.com/gtkit/orderd/internal/paygateway"
)

// Router 按 pay_account_id 路由到微信 Gateway;并按 notify_route_id 供回调定位。
// 只处理 wechat 渠道账号;其它渠道由复合路由兜底到其它实现。
type Router struct {
	byAccount map[string]*Gateway
	byRoute   map[string]*Gateway
}

// NewRouter 用一组微信账号配置构造(每账号独立 Provider)。ctx 透传给 provider 初始化(拉平台证书)。
func NewRouter(ctx context.Context, cfgs []Config) (*Router, error) {
	r := &Router{byAccount: map[string]*Gateway{}, byRoute: map[string]*Gateway{}}
	for _, c := range cfgs {
		gw, err := NewGateway(ctx, c)
		if err != nil {
			return nil, err
		}
		r.byAccount[c.PayAccountID] = gw
		if c.NotifyRouteID != "" {
			r.byRoute[c.NotifyRouteID] = gw
		}
	}
	return r, nil
}

// Resolve 实现 paygateway.Router:按支付账号取网关(渠道须为 wechat)。
func (r *Router) Resolve(channel, payAccountID string) (paygateway.Gateway, error) {
	gw, ok := r.byAccount[payAccountID]
	if !ok {
		return nil, paygateway.ErrAccountNotRouted
	}
	return gw, nil
}

// ResolveByRoute 按 notify_route_id 取网关(回调层验签用);未命中返回 nil。
func (r *Router) ResolveByRoute(routeID string) *Gateway {
	return r.byRoute[routeID]
}
