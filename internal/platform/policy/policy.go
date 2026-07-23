// Package policy 提供金额策略与定价可信边界的默认实现(phase-1 占位)。
//
// ★ 占位说明:StaticAmountPolicy 对所有 app 返回同一组上下限/白名单;生产应改为读取
// per-app 配置(配置表/配置中心)。TrustedCallerPricing 采信调用方金额,仅适用于
// "BFF 经严格服务身份(mTLS/内网 token)调用"的可信边界;客户端不可信场景须换 QuotePricing。
package policy

import (
	"fmt"

	"github.com/gtkit/orderd/internal/models"
)

// StaticAmountPolicy 对所有 app 返回固定上下限/白名单。
type StaticAmountPolicy struct{ Bounds_ models.AmountBounds }

// DefaultAmountPolicy 返回一个宽松但非空的默认策略(实付 1 分~100 万元,CNY)。
func DefaultAmountPolicy() StaticAmountPolicy {
	return StaticAmountPolicy{Bounds_: models.AmountBounds{
		MinPayable: 1, MaxPayable: 100_000_000,
		AllowedCurrency: map[string]bool{"CNY": true},
	}}
}

func (p StaticAmountPolicy) Bounds(string) (models.AmountBounds, bool) { return p.Bounds_, true }

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
