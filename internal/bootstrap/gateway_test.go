package bootstrap

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	orderv1 "github.com/gtkit/orderd/pb/order/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// echoServer 是最小 OrderService 实现:把收到的 authorization metadata 回显到 status_text,
// order_token(REST query)回显到 trade_no,用于验证 REST→gRPC 的路由、query 映射与鉴权头透传。
type echoServer struct {
	orderv1.UnimplementedOrderServiceServer
}

func (echoServer) GetOrderStatus(ctx context.Context, req *orderv1.GetOrderStatusRequest) (*orderv1.GetOrderStatusResponse, error) {
	var auth string
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if v := md.Get("authorization"); len(v) > 0 {
			auth = v[0]
		}
	}
	return &orderv1.GetOrderStatusResponse{StatusText: auth, TradeNo: req.OrderToken}, nil
}

// TestRESTGatewayForwardsAuthAndQuery 验证:grpc-gateway 把 REST 请求经回环 gRPC 转发,
// Authorization 头透传成 gRPC metadata,GET query(order_token)正确映射到请求消息。
func TestRESTGatewayForwardsAuthAndQuery(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("监听: %v", err)
	}
	gs := grpc.NewServer()
	orderv1.RegisterOrderServiceServer(gs, echoServer{})
	go func() { _ = gs.Serve(lis) }()
	defer gs.Stop()

	ctx := t.Context()
	gwMux, err := newRESTGateway(ctx, lis.Addr().String())
	if err != nil {
		t.Fatalf("构造网关: %v", err)
	}
	srv := httptest.NewServer(gwMux)
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/orders/status?order_token=tok-123&user_id=42", nil)
	req.Header.Set("Authorization", "Bearer xyz")
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("请求: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("状态码=%d,期望 200", resp.StatusCode)
	}

	var body struct {
		StatusText string `json:"statusText"`
		TradeNo    string `json:"tradeNo"`
	}
	if derr := json.NewDecoder(resp.Body).Decode(&body); derr != nil {
		t.Fatalf("解析响应: %v", derr)
	}
	if body.StatusText != "Bearer xyz" {
		t.Fatalf("Authorization 未透传到 gRPC:status_text=%q", body.StatusText)
	}
	if body.TradeNo != "tok-123" {
		t.Fatalf("query order_token 未映射:trade_no=%q", body.TradeNo)
	}
}
