// Package paygateway 定义支付网关端口(下单/查单/关单/退款)与多租户路由。
//
// 多租户(DESIGN §6):go-pay 的 paymgr.Manager 单渠道单 Provider 会互相覆盖,故按
// (channel, pay_account_id) 路由到各账号独立的 Gateway。真实微信/支付宝实现 V1 阶段不接,
// 由 runtime 注入(测试用 fake)。application 只依赖本接口,不感知具体渠道 SDK。
package paygateway

import (
	"context"
	"errors"
	"time"
)

// ErrAccountNotRouted 未找到 (channel, pay_account_id) 对应的网关。
var ErrAccountNotRouted = errors.New("paygateway: 支付账号无法路由")

// PrepareInput 调网关下单(统一下单)的入参。
type PrepareInput struct {
	Channel      string
	PayAccountID string
	OutTradeNo   string
	Amount       int64
	Currency     string
	Subject      string
	ClientIP     string
	ExpireAt     time.Time
}

// PrepareResult 下单结果:prepay_id(复用调起)+ 前端调起参数(app_params)。
type PrepareResult struct {
	PrepayID  string
	AppParams string
}

// QueryState 查单结果状态。
type QueryState int

const (
	QueryError    QueryState = iota // 查询失败(需重试)
	QueryPaid                       // 已支付
	QueryNotPay                     // 有单未支付
	QueryClosed                     // 已关闭
	QueryNotExist                   // 网关无此单
)

// QueryResult 查单结果。
type QueryResult struct {
	State         QueryState
	TransactionID string
	Amount        int64
	PaidAt        time.Time
}

// CloseInput 关单入参。
type CloseInput struct {
	Channel      string
	PayAccountID string
	OutTradeNo   string
}

// RefundInput 退款入参。
type RefundInput struct {
	Channel      string
	PayAccountID string
	OutTradeNo   string // 原支付商户单号
	OutRefundNo  string
	RefundAmount int64
	TotalAmount  int64
	Currency     string
}

// RefundResult 退款受理结果。
type RefundResult struct {
	GatewayRefundID string
	// Accepted=true 表示网关已受理进入处理中;最终结果以退款查单/通知为准。
	Accepted bool
}

// RefundQueryInput 退款查单入参。
type RefundQueryInput struct {
	Channel      string
	PayAccountID string
	OutRefundNo  string
	OutTradeNo   string // 原支付商户单号(部分渠道退款查单需要)
}

// RefundQueryState 退款查单结果状态。
type RefundQueryState int

const (
	RefundQueryError      RefundQueryState = iota // 查询失败(需重试)
	RefundQuerySucceeded                          // 退款成功
	RefundQueryFailed                             // 退款失败(终态)
	RefundQueryProcessing                         // 处理中
	RefundQueryNotExist                           // 网关暂未查到退款单(连续多次仍须人工核实,不能证明资金未退)
	RefundQueryAbnormal                           // 渠道明确退款异常,须人工处置
)

// RefundQueryResult 退款查单结果。
type RefundQueryResult struct {
	State           RefundQueryState
	GatewayRefundID string
	TransactionID   string
	Amount          int64
	SucceededAt     time.Time
}

// Gateway 单个支付账号的网关操作。
type Gateway interface {
	Prepare(ctx context.Context, in PrepareInput) (PrepareResult, error)
	Query(ctx context.Context, in QueryInput) (QueryResult, error)
	Close(ctx context.Context, in CloseInput) error
	Refund(ctx context.Context, in RefundInput) (RefundResult, error)
	RefundQuery(ctx context.Context, in RefundQueryInput) (RefundQueryResult, error)
}

// QueryInput 查单入参。
type QueryInput struct {
	Channel      string
	PayAccountID string
	OutTradeNo   string
}

// Router 按 (channel, pay_account_id) 解析到对应账号的 Gateway。
type Router interface {
	Resolve(channel, payAccountID string) (Gateway, error)
}
