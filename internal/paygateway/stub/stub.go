// Package stub 是支付网关的 phase-1 占位实现:不接真实微信/支付宝。
//
// 安全:桩【绝不伪造支付成功】——Prepare 只返回占位调起参数;Query 返回 QueryError、
// Refund 返回错误,避免在未接真实网关时凭空产生资金状态。真实网关(go-pay Router)后续接入。
package stub

import (
	"context"
	"errors"

	"github.com/gtkit/orderd/internal/paygateway"
)

// ErrNotImplemented 桩未实现真实网关能力。
var ErrNotImplemented = errors.New("paygateway/stub: 真实网关未接入")

// Router 对任意 (channel, account) 返回同一个桩网关。
type Router struct{}

func (Router) Resolve(string, string) (paygateway.Gateway, error) { return gateway{}, nil }

type gateway struct{}

func (gateway) Prepare(_ context.Context, in paygateway.PrepareInput) (paygateway.PrepareResult, error) {
	// 仅返回占位调起参数,便于本地跑通下单链路;不代表真实预支付。
	return paygateway.PrepareResult{
		PrepayID:  "stub-" + in.OutTradeNo,
		AppParams: `{"stub":true,"note":"paygateway stub, not a real prepay"}`,
	}, nil
}

func (gateway) Query(context.Context, paygateway.QueryInput) (paygateway.QueryResult, error) {
	return paygateway.QueryResult{State: paygateway.QueryError}, ErrNotImplemented
}

func (gateway) Close(context.Context, paygateway.CloseInput) error { return nil }

func (gateway) Refund(context.Context, paygateway.RefundInput) (paygateway.RefundResult, error) {
	return paygateway.RefundResult{Accepted: false}, ErrNotImplemented
}

func (gateway) RefundQuery(context.Context, paygateway.RefundQueryInput) (paygateway.RefundQueryResult, error) {
	return paygateway.RefundQueryResult{State: paygateway.RefundQueryError}, ErrNotImplemented
}
