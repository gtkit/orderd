// Package endpoint 定义接入方回调端点解析端口(webhook_url + 签名密钥引用 + 轮换)。
package endpoint

import (
	"context"
	"errors"

	"github.com/gtkit/orderd/internal/models"
)

// ErrNoEndpoint 未配置该 app 的回调端点(或已停用)。
var ErrNoEndpoint = errors.New("endpoint: 未配置回调端点")

// Resolver 按 app_id 解析回调端点(仅启用的)。
type Resolver interface {
	Resolve(ctx context.Context, appID string) (models.AppEndpoint, error)
}
