package gormstore

import (
	"context"
	"errors"
	"fmt"

	"github.com/gtkit/orderd/internal/models"
	"github.com/gtkit/orderd/internal/shared/endpoint"
	"gorm.io/gorm"
)

// EndpointResolver 用 app_endpoints 表实现 endpoint.Resolver(只读,非事务)。
type EndpointResolver struct{ db *gorm.DB }

// NewEndpointResolver 构造端点解析器。
func NewEndpointResolver(db *gorm.DB) *EndpointResolver { return &EndpointResolver{db: db} }

// Resolve 取启用的回调端点;未配置或停用 → ErrNoEndpoint。
func (r *EndpointResolver) Resolve(ctx context.Context, appID string) (models.AppEndpoint, error) {
	var e models.AppEndpoint
	err := r.db.WithContext(ctx).Where("app_id = ? AND enabled = ?", appID, true).Take(&e).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return models.AppEndpoint{}, endpoint.ErrNoEndpoint
	}
	if err != nil {
		return models.AppEndpoint{}, fmt.Errorf("gormstore: resolve endpoint: %w", err)
	}
	return e, nil
}
