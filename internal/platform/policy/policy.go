// Package policy 提供金额策略与定价可信边界的实现。
//
// 金额策略:per-app 配置在 app_amount_policies 表(gormstore.AmountPolicyResolver),
// 未配置/停用的 app 由 FallbackAmountPolicy 回退到全局默认档(StaticAmountPolicy)。
// TrustedCallerPricing 采信调用方金额,仅适用于"BFF 经严格服务身份(mTLS/内网 token)调用"
// 的可信边界;客户端不可信场景须换 QuotePricing。
package policy

import (
	"context"
	"fmt"

	"github.com/gtkit/orderd/internal/models"
)

// StaticAmountPolicy 对所有 app 返回固定上下限/白名单(回退默认档)。
type StaticAmountPolicy struct{ Bounds_ models.AmountBounds }

// DefaultAmountPolicy 返回一个宽松但非空的默认策略(实付 1 分~100 万元,CNY)。
func DefaultAmountPolicy() StaticAmountPolicy {
	return StaticAmountPolicy{Bounds_: models.AmountBounds{
		MinPayable: 1, MaxPayable: 100_000_000,
		AllowedCurrency: map[string]bool{"CNY": true},
	}}
}

func (p StaticAmountPolicy) Bounds(context.Context, string) (models.AmountBounds, bool, error) {
	return p.Bounds_, true, nil
}

// FallbackAmountPolicy 金额策略回退链:Primary(per-app 配置)命中用之;
// miss(未配置/停用)回退 Fallback(全局默认档);Primary 查询 error 直接上抛(fail-closed,
// 由 ValidateAmount 拒单)——绝不把基础设施故障降级为"用默认档放行"。
type FallbackAmountPolicy struct {
	Primary  models.AmountPolicy
	Fallback models.AmountPolicy
}

func (p FallbackAmountPolicy) Bounds(ctx context.Context, appID string) (models.AmountBounds, bool, error) {
	b, ok, err := p.Primary.Bounds(ctx, appID)
	if err != nil {
		return models.AmountBounds{}, false, err
	}
	if ok {
		return b, true, nil
	}
	return p.Fallback.Bounds(ctx, appID)
}

// TrustedCallerPricing 采信调用方金额(仅做不变量+上下限,不校验报价)。
// ★ 仅当 BFF 经严格服务身份(mTLS/内网 token)调用时才可用;须由部署方显式开启(评审 P1)。
type TrustedCallerPricing struct{}

func (TrustedCallerPricing) Authorize(string, models.Money, string) error { return nil }
func (TrustedCallerPricing) Kind() string                                 { return "trusted-caller" }

// DenyAllPricing 拒绝一切下单(fail-closed 默认)。未显式选择可信定价来源前,不采信任何金额,
// 强制部署方做出安全决策(开 TrustedCaller 或接报价校验),避免"默认信任客户端金额"上线。
type DenyAllPricing struct{}

func (DenyAllPricing) Authorize(string, models.Money, string) error {
	return fmt.Errorf("%w: 未配置可信定价来源,拒绝采信客户端金额(设 ORDERD_PRICING_TRUST_CALLER=true 或接报价校验)", models.ErrAmountPolicy)
}
func (DenyAllPricing) Kind() string { return "deny" }
