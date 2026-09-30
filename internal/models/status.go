// Package models 定义 orderd 的持久化模型、领域枚举与状态机(纯领域,无 DB/gRPC 依赖)。
//
// 枚举值使用【DB 存储值】作为权威内部表示;与 proto 契约的映射集中在本文件,
// 避免各处硬编码偏移。proto 值与 DB 值的关系:
//   - OrderStatus:proto = DB + 10(见 order.proto 注释);
//   - RefundStatus:proto = DB + 1;
//   - CloseReason / PayMethod:proto = DB(同值)。
package models

import orderv1 "github.com/gtkit/orderd/pb/order/v1"

// ---------------------------------------------------------------------
// OrderStatus 订单状态(DB 值)
// ---------------------------------------------------------------------

type OrderStatus int8

const (
	OrderPending         OrderStatus = 0  // 待支付
	OrderPaid            OrderStatus = 10 // 已支付
	OrderClosing         OrderStatus = 30 // 关单中(网关结果未知)
	OrderClosed          OrderStatus = 40 // 已关闭
	OrderCancelled       OrderStatus = 50 // 用户主动取消(从未提交网关)
	OrderRefunding       OrderStatus = 60 // 退款中
	OrderRefunded        OrderStatus = 70 // 已退款(全额,终态)
	OrderRefundException OrderStatus = 80 // 退款异常(需人工)
)

var orderStatusText = map[OrderStatus]string{
	OrderPending:         "待支付",
	OrderPaid:            "已支付",
	OrderClosing:         "关单中",
	OrderClosed:          "已关闭",
	OrderCancelled:       "已取消",
	OrderRefunding:       "退款中",
	OrderRefunded:        "已退款",
	OrderRefundException: "退款异常",
}

// Valid 报告该状态是否为已定义的有效订单状态。
func (s OrderStatus) Valid() bool { _, ok := orderStatusText[s]; return ok }

func (s OrderStatus) Text() string { return orderStatusText[s] }

// Proto 映射到 proto 枚举;非法状态返回 UNSPECIFIED。
func (s OrderStatus) Proto() orderv1.OrderStatus {
	if !s.Valid() {
		return orderv1.OrderStatus_ORDER_STATUS_UNSPECIFIED
	}
	return orderv1.OrderStatus(int32(s) + 10)
}

// OrderStatusFromProto 从 proto 枚举解析;非法/UNSPECIFIED 返回 ok=false。
func OrderStatusFromProto(p orderv1.OrderStatus) (OrderStatus, bool) {
	if p == orderv1.OrderStatus_ORDER_STATUS_UNSPECIFIED {
		return 0, false
	}
	s := OrderStatus(int32(p) - 10)
	return s, s.Valid()
}

// ---------------------------------------------------------------------
// CloseReason 关单原因(DB 值 = proto 值)
// ---------------------------------------------------------------------

type CloseReason int8

const (
	CloseNone       CloseReason = 0
	CloseTimeout    CloseReason = 1
	CloseSuperseded CloseReason = 2
	CloseSystem     CloseReason = 3
	CloseUserCancel CloseReason = 4
)

func (c CloseReason) Proto() orderv1.CloseReason { return orderv1.CloseReason(c) }

// ---------------------------------------------------------------------
// PayMethod 支付方式(DB 值 = proto 值)
// ---------------------------------------------------------------------

type PayMethod int8

const (
	PayMethodUnspecified PayMethod = 0
	PayMethodWechat      PayMethod = 1
	PayMethodAlipay      PayMethod = 2
)

// Channel 返回渠道字符串(与 payment_attempts.channel / app_pay_accounts.channel 对齐)。
func (m PayMethod) Channel() string {
	switch m {
	case PayMethodWechat:
		return "wechat"
	case PayMethodAlipay:
		return "alipay"
	default:
		return ""
	}
}

func (m PayMethod) Valid() bool { return m == PayMethodWechat || m == PayMethodAlipay }

// PayMethodFromChannel 由渠道字符串反推支付方式(Channel 的逆映射);未知渠道返回 Unspecified。
func PayMethodFromChannel(ch string) PayMethod {
	switch ch {
	case "wechat":
		return PayMethodWechat
	case "alipay":
		return PayMethodAlipay
	default:
		return PayMethodUnspecified
	}
}

func (m PayMethod) Proto() orderv1.PayMethod { return orderv1.PayMethod(m) }

func PayMethodFromProto(p orderv1.PayMethod) (PayMethod, bool) {
	m := PayMethod(p)
	return m, m.Valid()
}

// ---------------------------------------------------------------------
// AttemptStatus 支付尝试状态(DB 值,内部枚举,无 proto 对应)
// ---------------------------------------------------------------------

type AttemptStatus int8

const (
	AttemptCreated    AttemptStatus = 0 // 已建
	AttemptRequesting AttemptStatus = 1 // 请求中
	AttemptReady      AttemptStatus = 2 // 就绪(可调起)
	AttemptUnknown    AttemptStatus = 3 // 结果未知
	AttemptPaid       AttemptStatus = 4 // 已支付(终态)
	AttemptClosing    AttemptStatus = 5 // 关单中
	AttemptClosed     AttemptStatus = 6 // 已关闭(终态)
	AttemptFailed     AttemptStatus = 7 // 失败(终态)
)

// Active 报告该尝试是否占用"活跃唯一"槽位(与 DDL active_order 生成列一致:0,1,2,3,5)。
// CLOSING 计入活跃:已向网关发起关单但结果未知,仍可能转 PAID,期间不允许并发建新尝试(评审 #5)。
func (s AttemptStatus) Active() bool {
	switch s {
	case AttemptCreated, AttemptRequesting, AttemptReady, AttemptUnknown, AttemptClosing:
		return true
	default:
		return false
	}
}

// SubmittedToGateway 报告该尝试是否曾提交给网关(REQUESTING 及之后)。
// 用于区分"从未提交网关"(可安全 CANCELLED)与"有已提交历史"(须走 CLOSED/CLOSING,评审 #5)。
func (s AttemptStatus) SubmittedToGateway() bool { return s >= AttemptRequesting }

// ---------------------------------------------------------------------
// RefundStatus 退款状态(DB 值;proto = DB + 1)
// ---------------------------------------------------------------------

type RefundStatus int8

const (
	RefundCreated    RefundStatus = 0 // 已建
	RefundProcessing RefundStatus = 1 // 处理中
	RefundSucceeded  RefundStatus = 2 // 成功(终态)
	RefundFailed     RefundStatus = 3 // 失败(终态)
	RefundClosed     RefundStatus = 4 // 关闭(终态)
	RefundAbnormal   RefundStatus = 5 // 异常(需人工)
)

var refundStatusValid = map[RefundStatus]bool{
	RefundCreated: true, RefundProcessing: true, RefundSucceeded: true,
	RefundFailed: true, RefundClosed: true, RefundAbnormal: true,
}

func (s RefundStatus) Valid() bool { return refundStatusValid[s] }

// Active 报告该退款是否占用"活跃退款唯一"槽位(与 DDL active_guard 一致:0,1,5)。
// ABNORMAL 计入活跃:异常退款未经人工确认前须阻断再次发起退款(评审 #7)。
func (s RefundStatus) Active() bool {
	switch s {
	case RefundCreated, RefundProcessing, RefundAbnormal:
		return true
	default:
		return false
	}
}

func (s RefundStatus) Proto() orderv1.RefundStatus {
	if !s.Valid() {
		return orderv1.RefundStatus_REFUND_STATUS_UNSPECIFIED
	}
	return orderv1.RefundStatus(int32(s) + 1)
}

func RefundStatusFromProto(p orderv1.RefundStatus) (RefundStatus, bool) {
	if p == orderv1.RefundStatus_REFUND_STATUS_UNSPECIFIED {
		return 0, false
	}
	s := RefundStatus(int32(p) - 1)
	return s, s.Valid()
}
