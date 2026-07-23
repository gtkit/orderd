// Package appmeta 存放跨层常量(API 路径 / 事件类型 / 回调路由前缀等)。
package appmeta

const (
	// ServiceName 服务名(日志/追踪/machineID 分配等)。
	ServiceName = "orderd"

	// 微信/支付宝回调路由前缀(DESIGN §6)。route_id 不透明,由 app_pay_accounts.notify_route_id 定位账号。
	WechatNotifyPath       = "/notify/wechat/"
	WechatRefundNotifyPath = "/refund-notify/wechat/"
	AlipayNotifyPath       = "/notify/alipay/"

	// Webhook 签名头(与 sdk/orderevents 对齐)。
	HeaderEventID   = "X-Order-Event-Id"
	HeaderEventType = "X-Order-Event-Type"
	HeaderKeyID     = "X-Order-Key-Id"
	HeaderTimestamp = "X-Order-Timestamp"
	HeaderSignature = "X-Order-Signature"
)
