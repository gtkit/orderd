// Command createorder 是下单联调客户端示例:签 JWT → CreateOrder → 打印调起参数 → GetOrderStatus。
//
// 用法(env 均有默认,与 dev 配置一致):
//
//	ORDERD_GRPC_ADDR=127.0.0.1:9090 \
//	JWT_KID=dev1 JWT_SECRET=dev-secret-change-me-32bytes-xx JWT_ISSUER=bff JWT_AUDIENCE=orderd \
//	APP_ID=app1 USER_ID=1001 PAY_METHOD=alipay \
//	go run ./examples/createorder
//
// JWT_SECRET/KID/ISSUER/AUDIENCE 必须与服务端 ORDERD_JWT_* 一致;APP_ID 必须与账号表 app_id 一致。
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/golang-jwt/jwt/v5"
	orderv1 "github.com/gtkit/orderd/pb/order/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func signJWT() (string, error) {
	appID := env("APP_ID", "app1")
	var uid int64 = 1001
	if _, err := fmt.Sscan(env("USER_ID", "1001"), &uid); err != nil {
		return "", err
	}
	claims := jwt.MapClaims{
		"app_id": appID, "user_id": uid,
		"perms": []string{"order:create", "order:read", "order:cancel", "refund:read", "event:read"},
		"iss":   env("JWT_ISSUER", "bff"), "aud": env("JWT_AUDIENCE", "orderd"),
		"exp": time.Now().Add(time.Hour).Unix(),
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tok.Header["kid"] = env("JWT_KID", "dev1")
	return tok.SignedString([]byte(env("JWT_SECRET", "dev-secret-change-me-32bytes-xx")))
}

func payMethod() orderv1.PayMethod {
	if env("PAY_METHOD", "alipay") == "wechat" {
		return orderv1.PayMethod_PAY_METHOD_WECHAT
	}
	return orderv1.PayMethod_PAY_METHOD_ALIPAY
}

func main() {
	token, err := signJWT()
	must(err)

	conn, err := grpc.NewClient(env("ORDERD_GRPC_ADDR", "127.0.0.1:9090"), grpc.WithTransportCredentials(insecure.NewCredentials()))
	must(err)
	defer func() { _ = conn.Close() }()
	cli := orderv1.NewOrderServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", "Bearer "+token))

	idem := fmt.Sprintf("demo-%d", time.Now().UnixNano())
	cr, err := cli.CreateOrder(ctx, &orderv1.CreateOrderRequest{
		UserId:         1001,
		Product:        &orderv1.ProductSnapshot{ProductId: 7, Title: "会员月卡", BusinessType: "vip"},
		Amount:         &orderv1.Money{Original: 100, Discount: 10, Payable: 90, Currency: orderv1.Currency_CURRENCY_CNY},
		PayMethod:      payMethod(),
		IdempotencyKey: idem,
		ClientOrderNo:  idem,
	})
	must(err)
	fmt.Println("=== CreateOrder ===")
	fmt.Printf("order_no    = %s\n", cr.OrderNo)
	fmt.Printf("order_token = %s\n", cr.OrderToken)
	fmt.Printf("status      = %s\n", cr.Status)
	fmt.Printf("app_params  = %s\n", cr.AppParams) // 扫码:二维码内容;收银台:支付 URL
	fmt.Println("→ 支付宝扫码(native):把 app_params 生成二维码用沙箱买家扫;收银台(page):浏览器打开该 URL")

	gs, err := cli.GetOrderStatus(ctx, &orderv1.GetOrderStatusRequest{OrderToken: cr.OrderToken, UserId: 1001})
	must(err)
	fmt.Println("=== GetOrderStatus ===")
	fmt.Printf("status=%s text=%s payable=%d ver=%d\n", gs.Status, gs.StatusText, gs.Amount.GetPayable(), gs.AggregateVersion)
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "错误:", err)
		os.Exit(1)
	}
}
