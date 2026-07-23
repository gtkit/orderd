package models

import (
	"fmt"

	orderv1 "github.com/gtkit/orderd/pb/order/v1"
)

// Money 金额权威快照(单位:分)。服务端强制不变量,不信任接入方随意传值。
type Money struct {
	Original int64  // 原价
	Discount int64  // 优惠
	Payable  int64  // 实付
	Currency string // 币种(如 CNY)
}

// Validate 校验金额基础不变量:非负、实付为正、original - discount = payable。
// 这只防"算术/异常值",不证明"真实售价"——真实性由 PricingAuthority 边界保证(见下)。
func (m Money) Validate() error {
	if m.Original < 0 || m.Discount < 0 {
		return fmt.Errorf("%w: 原价/优惠不可为负", ErrInvalidAmount)
	}
	if m.Payable <= 0 {
		return fmt.Errorf("%w: 实付必须为正", ErrInvalidAmount)
	}
	if m.Original-m.Discount != m.Payable {
		return fmt.Errorf("%w: 不满足 original-discount=payable (%d-%d≠%d)",
			ErrInvalidAmount, m.Original, m.Discount, m.Payable)
	}
	if m.Currency == "" {
		return fmt.Errorf("%w: 币种不可为空", ErrInvalidAmount)
	}
	return nil
}

// MoneyFromProto 从 proto Money 构造(不校验,交由 Validate)。
func MoneyFromProto(p *orderv1.Money) Money {
	if p == nil {
		return Money{}
	}
	cur := ""
	switch p.Currency {
	case orderv1.Currency_CURRENCY_CNY:
		cur = "CNY"
	}
	return Money{Original: p.Original, Discount: p.Discount, Payable: p.Payable, Currency: cur}
}

// AmountBounds 单个 app 的金额策略:业务类型白名单 + 实付上下限。
type AmountBounds struct {
	MinPayable      int64           // 实付下限(含),0 表示不限
	MaxPayable      int64           // 实付上限(含),0 表示不限
	AllowedBusiness map[string]bool // 业务类型白名单;nil/空=不限制
	AllowedCurrency map[string]bool // 币种白名单;nil/空=仅允许 CNY
}

// AmountPolicy 金额策略仓(按 app 查),应用层注入。
type AmountPolicy interface {
	Bounds(appID string) (AmountBounds, bool)
}

// ValidateAmount 在基础不变量之外,施加 app 级白名单与上下限校验。
func ValidateAmount(policy AmountPolicy, appID string, m Money, businessType string) error {
	if err := m.Validate(); err != nil {
		return err
	}
	b, ok := policy.Bounds(appID)
	if !ok {
		return fmt.Errorf("%w: 未配置 app=%s 的金额策略", ErrAmountPolicy, appID)
	}
	if b.MinPayable > 0 && m.Payable < b.MinPayable {
		return fmt.Errorf("%w: 实付 %d 低于下限 %d", ErrAmountPolicy, m.Payable, b.MinPayable)
	}
	if b.MaxPayable > 0 && m.Payable > b.MaxPayable {
		return fmt.Errorf("%w: 实付 %d 超过上限 %d", ErrAmountPolicy, m.Payable, b.MaxPayable)
	}
	if len(b.AllowedBusiness) > 0 && !b.AllowedBusiness[businessType] {
		return fmt.Errorf("%w: 业务类型 %q 不在 app=%s 白名单", ErrAmountPolicy, businessType, appID)
	}
	allowedCur := b.AllowedCurrency
	if len(allowedCur) == 0 {
		allowedCur = map[string]bool{"CNY": true}
	}
	if !allowedCur[m.Currency] {
		return fmt.Errorf("%w: 币种 %q 不被 app=%s 允许", ErrAmountPolicy, m.Currency, appID)
	}
	return nil
}

// PricingAuthority 定价可信边界(评审 #8)。金额真实性不能靠白名单/上下限证明——
// 那只拦异常值。真实性必须来自可信来源之一,由部署方二选一注入:
//   - TrustedCallerPricing:BFF 是受信定价方,通过严格服务身份调用(mTLS/内网 token),
//     orderd 直接采信请求金额(仅做不变量+上下限校验);
//   - QuotePricing:客户端不可信,orderd 校验价格服务签发的 price_quote(签名/quote_id),
//     确认请求金额与报价一致后才受理。
//
// 本接口把"可信边界"显式化,禁止无声地信任客户端金额。
type PricingAuthority interface {
	// Authorize 校验本次下单金额是否可信;不可信返回 error。
	Authorize(appID string, m Money, quoteID string) error
	// Kind 返回边界类型(用于日志/审计:trusted-caller / quote)。
	Kind() string
}
