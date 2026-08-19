package gormstore

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/gtkit/orderd/internal/models"
	"gorm.io/gorm"
)

// AmountPolicyResolver 用 app_amount_policies 表实现 models.AmountPolicy(只读,非事务)。
// 未配置或 enabled=0 返回 ok=false(由装配层回退链兜底);查询错误如实上抛(fail-closed 拒单)。
type AmountPolicyResolver struct{ db *gorm.DB }

// NewAmountPolicyResolver 构造金额策略解析器。
func NewAmountPolicyResolver(db *gorm.DB) *AmountPolicyResolver { return &AmountPolicyResolver{db: db} }

// Bounds 实现 models.AmountPolicy。
func (r *AmountPolicyResolver) Bounds(ctx context.Context, appID string) (models.AmountBounds, bool, error) {
	var p models.AppAmountPolicy
	err := r.db.WithContext(ctx).Where("app_id = ? AND enabled = ?", appID, true).Take(&p).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return models.AmountBounds{}, false, nil
	}
	if err != nil {
		return models.AmountBounds{}, false, fmt.Errorf("gormstore: resolve amount policy: %w", err)
	}
	return models.AmountBounds{
		MinPayable:      p.MinPayable,
		MaxPayable:      p.MaxPayable,
		AllowedBusiness: csvSet(p.AllowedBusiness),
		AllowedCurrency: csvSet(p.AllowedCurrency),
	}, true, nil
}

// csvSet 逗号分隔白名单 → 集合(去空白,忽略空项);空输入返回 nil(=不限/仅CNY,语义在 AmountBounds)。
func csvSet(s string) map[string]bool {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	m := map[string]bool{}
	for p := range strings.SplitSeq(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			m[p] = true
		}
	}
	return m
}
