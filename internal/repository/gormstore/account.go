package gormstore

import (
	"context"
	"errors"
	"fmt"

	"github.com/gtkit/orderd/internal/models"
	"github.com/gtkit/orderd/internal/shared/account"
	"gorm.io/gorm"
)

// AccountResolver 用 app_pay_accounts 表实现 account.Resolver(只读,非事务)。
type AccountResolver struct{ db *gorm.DB }

// NewAccountResolver 构造账号解析器。
func NewAccountResolver(db *gorm.DB) *AccountResolver { return &AccountResolver{db: db} }

// Resolve 按 (appID, channel) 取一个启用账号。
func (r *AccountResolver) Resolve(ctx context.Context, appID, channel string) (models.AppPayAccount, error) {
	var a models.AppPayAccount
	err := r.db.WithContext(ctx).
		Where("app_id = ? AND channel = ? AND enabled = ?", appID, channel, true).Take(&a).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return models.AppPayAccount{}, account.ErrNoAccount
	}
	if err != nil {
		return models.AppPayAccount{}, fmt.Errorf("gormstore: resolve account: %w", err)
	}
	return a, nil
}

// ResolveByID 按 pay_account_id 精确取账号(退款/查单/结算快照用,停用账号仍可取)。
func (r *AccountResolver) ResolveByID(ctx context.Context, payAccountID string) (models.AppPayAccount, error) {
	var a models.AppPayAccount
	err := r.db.WithContext(ctx).Where("pay_account_id = ?", payAccountID).Take(&a).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return models.AppPayAccount{}, account.ErrNoAccount
	}
	if err != nil {
		return models.AppPayAccount{}, fmt.Errorf("gormstore: resolve account by id: %w", err)
	}
	return a, nil
}

// List 列出全部启用账号(对账 worker 枚举用)。
func (r *AccountResolver) List(ctx context.Context) ([]models.AppPayAccount, error) {
	var out []models.AppPayAccount
	err := r.db.WithContext(ctx).Where("enabled = ?", true).Find(&out).Error
	if err != nil {
		return nil, fmt.Errorf("gormstore: list accounts: %w", err)
	}
	return out, nil
}
