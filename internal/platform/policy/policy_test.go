package policy

import (
	"context"
	"errors"
	"testing"

	"github.com/gtkit/orderd/internal/models"
)

type stubPolicy struct {
	b   models.AmountBounds
	ok  bool
	err error
}

func (s stubPolicy) Bounds(context.Context, string) (models.AmountBounds, bool, error) {
	return s.b, s.ok, s.err
}

// TestFallbackAmountPolicy 回退链三态:primary 命中用之;miss 回退;error 上抛(fail-closed,
// 绝不降级为默认档放行)。
func TestFallbackAmountPolicy(t *testing.T) {
	fb := stubPolicy{b: models.AmountBounds{MinPayable: 1}, ok: true}
	bg := context.Background()

	// 命中:用 primary 的界。
	p := FallbackAmountPolicy{Primary: stubPolicy{b: models.AmountBounds{MinPayable: 100}, ok: true}, Fallback: fb}
	b, ok, err := p.Bounds(bg, "app1")
	if err != nil || !ok || b.MinPayable != 100 {
		t.Fatalf("primary 命中应用其界: %+v ok=%v err=%v", b, ok, err)
	}
	// miss:回退。
	p = FallbackAmountPolicy{Primary: stubPolicy{ok: false}, Fallback: fb}
	b, ok, err = p.Bounds(bg, "app1")
	if err != nil || !ok || b.MinPayable != 1 {
		t.Fatalf("miss 应回退默认档: %+v ok=%v err=%v", b, ok, err)
	}
	// error:上抛,不回退。
	boom := errors.New("db down")
	p = FallbackAmountPolicy{Primary: stubPolicy{err: boom}, Fallback: fb}
	if _, _, err = p.Bounds(bg, "app1"); !errors.Is(err, boom) {
		t.Fatalf("primary 错误必须上抛(不降级放行),实际: %v", err)
	}
}
