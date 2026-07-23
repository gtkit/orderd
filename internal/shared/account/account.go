// Package account 定义多租户支付账号解析端口。
// order/payment application 依赖它选取 (app_id, channel) 对应账号(下单冻结 / 换渠道选新账号);
// runtime 用 app_pay_accounts 仓储实现,测试用 fake。
package account

import (
	"context"
	"errors"

	"github.com/gtkit/orderd/internal/models"
)

// ErrNoAccount 未找到可用支付账号。
var ErrNoAccount = errors.New("account: 无可用支付账号")

// Resolver 解析支付账号。
type Resolver interface {
	// Resolve 按 (appID, channel) 选取可用账号(enabled),用于下单冻结 / 换渠道。
	Resolve(ctx context.Context, appID, channel string) (models.AppPayAccount, error)
	// ResolveByID 按 pay_account_id 精确取账号(支付成功写 settled 快照 / 退款用原账号)。
	ResolveByID(ctx context.Context, payAccountID string) (models.AppPayAccount, error)
}

// Lister 列出启用账号(对账 worker 枚举待对账账号用)。
type Lister interface {
	List(ctx context.Context) ([]models.AppPayAccount, error)
}
