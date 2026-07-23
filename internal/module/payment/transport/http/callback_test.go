package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gtkit/orderd/internal/appmeta"
	"github.com/gtkit/orderd/internal/paygateway/wechatgw"
)

// 微信回调路由守卫:在触达结算前的两条分支不依赖 payment.Service,可独立验证。

func TestWechatNotifyDisabled(t *testing.T) {
	h := New(nil, nil, nil) // 微信未启用
	rec := httptest.NewRecorder()
	h.wechatNotify(rec, httptest.NewRequest(http.MethodPost, appmeta.WechatNotifyPath+"r1", nil))
	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("微信未启用应 501,实际 %d", rec.Code)
	}
}

func TestWechatNotifyUnknownRoute(t *testing.T) {
	empty, err := wechatgw.NewRouter(context.Background(), nil) // 空路由,无账号(不构造真实 provider)
	if err != nil {
		t.Fatalf("构造空路由: %v", err)
	}
	h := New(nil, empty, nil)
	rec := httptest.NewRecorder()
	h.wechatNotify(rec, httptest.NewRequest(http.MethodPost, appmeta.WechatNotifyPath+"unknown", nil))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("未知 route_id 应 400,实际 %d", rec.Code)
	}
}
