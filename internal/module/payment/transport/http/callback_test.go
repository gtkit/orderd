package http

import (
	"bytes"
	"context"
	"crypto"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"maps"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gtkit/go-pay/paymgr"
	"github.com/gtkit/orderd/internal/appmeta"
	"github.com/gtkit/orderd/internal/models"
	paymentapp "github.com/gtkit/orderd/internal/module/payment/application"
	"github.com/gtkit/orderd/internal/paygateway"
	"github.com/gtkit/orderd/internal/paygateway/alipaygw"
	"github.com/gtkit/orderd/internal/paygateway/wechatgw"
	"github.com/gtkit/orderd/internal/shared/alert"
	"github.com/gtkit/orderd/internal/store"
	"github.com/gtkit/orderd/internal/store/memstore"
	orderv1 "github.com/gtkit/orderd/pb/order/v1"
	"google.golang.org/protobuf/proto"
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

const (
	callbackAppID             = "signed-callback-app"
	callbackPayAppID          = "signed-pay-app"
	callbackMchID             = "signed-test-mch"
	callbackPublicKeyID       = "PUB_KEY_ID_SIGNED_CALLBACK_TEST"
	callbackAmount      int64 = 90_000_000
)

// signedCallbacks 使用本次测试生成的密钥走真实 SDK 的验签/解密,不访问外部支付服务。
type signedCallbacks struct {
	alipay      *alipaygw.Router
	wechat      *wechatgw.Router
	platformKey *rsa.PrivateKey
	apiKey      []byte
}

func newSignedCallbacks(t *testing.T) signedCallbacks {
	t.Helper()
	merchant, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	platform, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	private, err := x509.MarshalPKCS8PrivateKey(merchant)
	if err != nil {
		t.Fatal(err)
	}
	public, err := x509.MarshalPKIXPublicKey(&platform.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	privatePEM := string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: private}))
	publicPEM := string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: public}))
	apiKey := make([]byte, 32)
	if _, err := rand.Read(apiKey); err != nil {
		t.Fatal(err)
	}
	ali, err := alipaygw.NewRouter([]alipaygw.Config{{
		PayAccountID: "acc-alipay", NotifyRouteID: "signed", AppID: callbackPayAppID,
		PrivateKey: privatePEM, AlipayPublicKey: publicPEM, SellerID: callbackMchID,
		TradeType: "page", NotifyURL: "https://merchant.example/notify",
	}})
	if err != nil {
		t.Fatal(err)
	}
	// 公钥模式不启动平台证书下载;私钥/APIv3 key/平台公钥均为测试生成。
	wc, err := wechatgw.NewRouter(t.Context(), []wechatgw.Config{{
		PayAccountID: "acc-wechat", NotifyRouteID: "signed", AppID: callbackPayAppID, MchID: callbackMchID,
		MchCertSerialNumber: "TEST_MERCHANT_SERIAL", MchPrivateKeyPEM: privatePEM, MchAPIv3Key: string(apiKey),
		WechatPayPublicKeyID: callbackPublicKeyID, WechatPayPublicKeyPEM: publicPEM,
	}})
	if err != nil {
		t.Fatal(err)
	}
	return signedCallbacks{alipay: ali, wechat: wc, platformKey: platform, apiKey: apiKey}
}

func (s signedCallbacks) sign(t *testing.T, message []byte) string {
	t.Helper()
	digest := sha256.Sum256(message)
	sig, err := rsa.SignPKCS1v15(rand.Reader, s.platformKey, crypto.SHA256, digest[:])
	if err != nil {
		t.Fatal(err)
	}
	return base64.StdEncoding.EncodeToString(sig)
}

func (s signedCallbacks) request(t *testing.T, channel string, paid, tampered bool, overrides ...map[string]any) *http.Request {
	t.Helper()
	if channel == "alipay" {
		status := "WAIT_BUYER_PAY"
		if paid {
			status = "TRADE_SUCCESS"
		}
		values := url.Values{
			"app_id": {callbackPayAppID}, "seller_id": {callbackMchID}, "out_trade_no": {"PT-signed"}, "trade_no": {"TX-signed"},
			"trade_status": {status}, "total_amount": {"900000.00"}, "gmt_payment": {"2026-10-12 00:30:00"},
		}
		for _, fields := range overrides {
			for k, v := range fields {
				if v == nil {
					values.Del(k)
				} else {
					values.Set(k, v.(string))
				}
			}
		}
		pairs := make([]string, 0, len(values))
		for _, key := range slices.Sorted(maps.Keys(values)) {
			pairs = append(pairs, key+"="+values.Get(key))
		}
		values.Set("sign", s.sign(t, []byte(strings.Join(pairs, "&"))))
		values.Set("sign_type", "RSA2")
		if tampered {
			values.Set("gmt_payment", "2026-10-13 00:30:00")
		}
		req := httptest.NewRequest(http.MethodPost, appmeta.AlipayNotifyPath+"signed", strings.NewReader(values.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		return req.WithContext(t.Context())
	}

	status := "NOTPAY"
	if paid {
		status = "SUCCESS"
	}
	transaction := map[string]any{
		"mchid": callbackMchID, "appid": callbackPayAppID, "out_trade_no": "PT-signed", "transaction_id": "TX-signed",
		"trade_state": status, "success_time": "2026-10-12T00:30:00+08:00", "amount": map[string]int64{"total": callbackAmount},
	}
	for _, fields := range overrides {
		for k, v := range fields {
			if v == nil {
				delete(transaction, k)
			} else {
				transaction[k] = v
			}
		}
	}
	plaintext := callbackJSON(t, transaction)
	block, err := aes.NewCipher(s.apiKey)
	if err != nil {
		t.Fatal(err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		t.Fatal(err)
	}
	// 每条通知生成独立 nonce,避免同一个测试 APIv3 key 下重用 GCM nonce。
	nonce := []byte(rand.Text()[:aead.NonceSize()])
	const associated = "transaction"
	ciphertext := aead.Seal(nil, nonce, plaintext, []byte(associated))
	body := callbackJSON(t, map[string]any{
		"id": "EV-signed", "create_time": time.Now().Format(time.RFC3339), "event_type": "TRANSACTION.SUCCESS",
		"resource_type": "encrypt-resource", "resource": map[string]string{
			"algorithm": "AEAD_AES_256_GCM", "ciphertext": base64.StdEncoding.EncodeToString(ciphertext),
			"nonce": string(nonce), "associated_data": associated, "original_type": "transaction",
		},
	})
	timestamp, headerNonce := strconv.FormatInt(time.Now().Unix(), 10), rand.Text()
	signature := s.sign(t, []byte(timestamp+"\n"+headerNonce+"\n"+string(body)+"\n"))
	if tampered {
		body = bytes.Replace(body, []byte("EV-signed"), []byte("EV-tampered"), 1)
	}
	req := httptest.NewRequest(http.MethodPost, appmeta.WechatNotifyPath+"signed", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Wechatpay-Timestamp", timestamp)
	req.Header.Set("Wechatpay-Nonce", headerNonce)
	req.Header.Set("Wechatpay-Serial", callbackPublicKeyID)
	req.Header.Set("Wechatpay-Signature", signature)
	return req.WithContext(t.Context())
}

func callbackJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

type callbackAccounts struct{ account models.AppPayAccount }

func (a callbackAccounts) Resolve(context.Context, string, string) (models.AppPayAccount, error) {
	return a.account, nil
}

func (a callbackAccounts) ResolveByID(context.Context, string) (models.AppPayAccount, error) {
	return a.account, nil
}

func (s signedCallbacks) seededHandler(t *testing.T, channel string, status models.OrderStatus) (*http.ServeMux, *memstore.Mem) {
	t.Helper()
	mem := memstore.New()
	attemptStatus := models.AttemptClosed
	if status == models.OrderPending {
		attemptStatus = models.AttemptReady
	}
	err := mem.WithinTx(t.Context(), func(ctx context.Context, r store.Repos) error {
		if err := r.Orders().Insert(ctx, &models.Order{
			AppID: callbackAppID, OrderNo: "OD-signed", UserID: 42, Status: status,
			PayAmount: callbackAmount, Currency: "CNY", ProductTitle: "signed callback", CloseReason: models.CloseUserCancel,
		}); err != nil {
			return err
		}
		return r.Attempts().Insert(ctx, &models.PaymentAttempt{
			AppID: callbackAppID, OrderNo: "OD-signed", AttemptNo: 1, Channel: channel,
			PayAccountID: "acc-" + channel, OutTradeNo: "PT-signed", Status: attemptStatus,
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	pay := paymentapp.New(paymentapp.Deps{
		Store: mem, Accounts: callbackAccounts{account: models.AppPayAccount{
			AppID: callbackAppID, PayAccountID: "acc-" + channel, Channel: channel, MchID: callbackMchID, PayAppID: callbackPayAppID,
		}},
		Alerter: alert.Nop{}, NewEventID: func() (string, error) { return "event-signed", nil },
	})
	mux := http.NewServeMux()
	New(s.alipay, s.wechat, pay).Register(mux)
	return mux, mem
}

func TestSignedPaymentCallbacksRecoverAndDeduplicate(t *testing.T) {
	signed := newSignedCallbacks(t)
	for _, channel := range []string{"alipay", "wechat"} {
		for _, status := range []models.OrderStatus{models.OrderPending, models.OrderClosed, models.OrderCancelled} {
			t.Run(channel+"/"+status.Text(), func(t *testing.T) {
				mux, mem := signed.seededHandler(t, channel, status)
				rec := httptest.NewRecorder()
				mux.ServeHTTP(rec, signed.request(t, channel, true, false))
				if rec.Code != http.StatusOK {
					t.Fatalf("回调未成功: status=%d body=%s", rec.Code, rec.Body.String())
				}
				if channel == "alipay" && rec.Body.String() != "success" {
					t.Fatalf("支付宝 ACK=%q", rec.Body.String())
				}
				// 多副本式并发重复通知仍只结算一次;先在测试 goroutine 中完成签名构造。
				requests := make([]*http.Request, 8)
				for i := range requests {
					requests[i] = signed.request(t, channel, true, false)
				}
				var wg sync.WaitGroup
				for _, req := range requests {
					wg.Go(func() {
						rr := httptest.NewRecorder()
						mux.ServeHTTP(rr, req)
						if rr.Code != http.StatusOK {
							t.Errorf("重复回调未成功: status=%d body=%s", rr.Code, rr.Body.String())
						}
					})
				}
				wg.Wait()
				want := time.Date(2026, 10, 11, 16, 30, 0, 0, time.UTC)
				o, err := mem.Reader().Orders().GetByOrderNo(t.Context(), callbackAppID, "OD-signed", false)
				if err != nil {
					t.Fatal(err)
				}
				if o.Status != models.OrderPaid || o.CloseReason != models.CloseNone || !o.PaidAt.Valid || !o.PaidAt.Time.Equal(want) {
					t.Fatalf("订单未按渠道时间恢复支付: status=%v close=%v paid_at=%v", o.Status, o.CloseReason, o.PaidAt)
				}
				txns := mem.Txns()
				if len(txns) != 1 || txns[0].Anomaly || !txns[0].PaidAt.Valid || !txns[0].PaidAt.Time.Equal(want) {
					t.Fatalf("流水必须恰一条且保留渠道时间: %+v", txns)
				}
				events := mem.Events()
				if len(events) != 1 || events[0].EventType != models.EventTypePaid {
					t.Fatalf("paid事件必须恰一条: %+v", events)
				}
				var env orderv1.EventEnvelope
				if err := proto.Unmarshal(events[0].Payload, &env); err != nil {
					t.Fatal(err)
				}
				paid := env.GetPaid()
				if paid == nil || paid.PaidAt == nil || !paid.PaidAt.AsTime().Equal(want) || env.PreviousStatus != status.Proto() {
					t.Fatalf("事件时间/恢复前状态不符: %v", &env)
				}
			})
		}
	}
}

func TestSignedPaymentCallbacksRejectTamperingAndIgnoreUnpaid(t *testing.T) {
	signed := newSignedCallbacks(t)
	for _, channel := range []string{"alipay", "wechat"} {
		for _, tc := range []struct {
			name           string
			paid, tampered bool
			want           int
		}{
			{"签名后篡改", true, true, http.StatusUnauthorized},
			{"未付款通知", false, false, http.StatusOK},
		} {
			t.Run(channel+"/"+tc.name, func(t *testing.T) {
				mux, mem := signed.seededHandler(t, channel, models.OrderPending)
				rec := httptest.NewRecorder()
				mux.ServeHTTP(rec, signed.request(t, channel, tc.paid, tc.tampered))
				if rec.Code != tc.want {
					t.Fatalf("status=%d, want %d, body=%s", rec.Code, tc.want, rec.Body.String())
				}
				o, err := mem.Reader().Orders().GetByOrderNo(t.Context(), callbackAppID, "OD-signed", false)
				if err != nil {
					t.Fatal(err)
				}
				if o.Status != models.OrderPending || len(mem.Txns()) != 0 || len(mem.Events()) != 0 {
					t.Fatalf("非法/未付款通知不应结算: status=%v txns=%d events=%d", o.Status, len(mem.Txns()), len(mem.Events()))
				}
			})
		}
	}
}

func TestSignedPaymentCallbacksRequireAccountIdentity(t *testing.T) {
	signed := newSignedCallbacks(t)
	for _, field := range []struct{ channel, key string }{
		{"alipay", "app_id"}, {"alipay", "seller_id"}, {"wechat", "appid"}, {"wechat", "mchid"},
	} {
		for _, tc := range []struct {
			name  string
			value any
		}{{"缺失", nil}, {"空值", ""}, {"错误账号", "another-account"}} {
			t.Run(field.channel+"/"+field.key+"/"+tc.name, func(t *testing.T) {
				mux, mem := signed.seededHandler(t, field.channel, models.OrderClosed)
				rec := httptest.NewRecorder()
				mux.ServeHTTP(rec, signed.request(t, field.channel, true, false, map[string]any{field.key: tc.value}))
				if rec.Code != http.StatusUnauthorized {
					t.Fatalf("合法签名但身份错误应拒绝: status=%d body=%s", rec.Code, rec.Body.String())
				}
				o, err := mem.Reader().Orders().GetByOrderNo(t.Context(), callbackAppID, "OD-signed", false)
				if err != nil || o.Status != models.OrderClosed || len(mem.Txns()) != 0 || len(mem.Events()) != 0 {
					t.Fatalf("身份错误不得恢复订单: order=%+v err=%v", o, err)
				}
			})
		}
	}
}

// 超大值只验证编码与本地结算的 int64 精度,不宣称实际渠道允许对应业务金额。
func TestSignedPaymentCallbacksAmountBoundaries(t *testing.T) {
	signed := newSignedCallbacks(t)
	for _, channel := range []string{"alipay", "wechat"} {
		for _, tc := range []struct {
			name              string
			orderAmount       int64
			fen               any
			yuan              string
			wantCode          int
			wantPaid, anomaly bool
		}{
			{"超过浮点精度边界", 1<<53 + 1, int64(1<<53 + 1), "90071992547409.93", http.StatusOK, true, false},
			{"int64上界", math.MaxInt64, int64(math.MaxInt64), "92233720368547758.07", http.StatusOK, true, false},
			{"零", callbackAmount, int64(0), "0.00", http.StatusUnauthorized, false, false},
			{"负数", callbackAmount, int64(-1), "-0.01", http.StatusUnauthorized, false, false},
			{"超出int64", callbackAmount, json.Number("9223372036854775808"), "92233720368547758.08", http.StatusUnauthorized, false, false},
			{"整数金额不匹配", callbackAmount, callbackAmount - 1, "899999.99", http.StatusOK, false, true},
		} {
			t.Run(channel+"/"+tc.name, func(t *testing.T) {
				mux, mem := signed.seededHandler(t, channel, models.OrderClosed)
				if err := mem.WithinTx(t.Context(), func(ctx context.Context, r store.Repos) error {
					o, err := r.Orders().GetByOrderNo(ctx, callbackAppID, "OD-signed", true)
					if err != nil {
						return err
					}
					o.PayAmount = tc.orderAmount
					return r.Orders().Update(ctx, o)
				}); err != nil {
					t.Fatal(err)
				}
				fields := map[string]any{"total_amount": tc.yuan}
				if channel == "wechat" {
					fields = map[string]any{"amount": map[string]any{"total": tc.fen}}
				}
				rec := httptest.NewRecorder()
				mux.ServeHTTP(rec, signed.request(t, channel, true, false, fields))
				if rec.Code != tc.wantCode {
					t.Fatalf("status=%d want=%d body=%s", rec.Code, tc.wantCode, rec.Body.String())
				}
				o, err := mem.Reader().Orders().GetByOrderNo(t.Context(), callbackAppID, "OD-signed", false)
				if err != nil {
					t.Fatal(err)
				}
				wantStatus := models.OrderClosed
				if tc.wantPaid {
					wantStatus = models.OrderPaid
				}
				if o.Status != wantStatus {
					t.Fatalf("订单=%v want=%v", o.Status, wantStatus)
				}
				txns := mem.Txns()
				if tc.wantPaid || tc.anomaly {
					if len(txns) != 1 || txns[0].Amount != tc.fen.(int64) || txns[0].Anomaly != tc.anomaly {
						t.Fatalf("资金金额不应丢精度: %+v", txns)
					}
				} else if len(txns) != 0 {
					t.Fatalf("非法金额不应入账: %+v", txns)
				}
				wantEvents := 0
				if tc.wantPaid {
					wantEvents = 1
				}
				if len(mem.Events()) != wantEvents {
					t.Fatalf("paid事件数量=%d want=%d", len(mem.Events()), wantEvents)
				}
			})
		}
	}
	for _, amount := range []string{"900000.001", "9e5", "+900000.00", ".01", "900000."} {
		t.Run("alipay/非法格式/"+amount, func(t *testing.T) {
			mux, mem := signed.seededHandler(t, "alipay", models.OrderClosed)
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, signed.request(t, "alipay", true, false, map[string]any{"total_amount": amount}))
			if rec.Code != http.StatusUnauthorized || len(mem.Txns()) != 0 || len(mem.Events()) != 0 {
				t.Fatalf("不能按截断或非十进制金额结算: status=%d txns=%+v", rec.Code, mem.Txns())
			}
		})
	}
}

func TestSignedPaymentCallbacksLimitBody(t *testing.T) {
	signed := newSignedCallbacks(t)
	for _, channel := range []string{"alipay", "wechat"} {
		t.Run(channel, func(t *testing.T) {
			mux, mem := signed.seededHandler(t, channel, models.OrderClosed)
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, signed.request(t, channel, true, false, map[string]any{"padding": strings.Repeat("a", 1<<20)}))
			if rec.Code != http.StatusUnauthorized || len(mem.Txns()) != 0 || len(mem.Events()) != 0 {
				t.Fatalf("超限报文应拒绝: status=%d txns=%d events=%d", rec.Code, len(mem.Txns()), len(mem.Events()))
			}
		})
	}
}

func TestAlipaySignedPrepareAmountPrecision(t *testing.T) {
	signed := newSignedCallbacks(t)
	gw, err := signed.alipay.Resolve("alipay", "acc-alipay")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		fen  int64
		want string
	}{
		{"最小分", 1, "0.01"}, {"超过浮点精度边界", 1<<53 + 1, "90071992547409.93"},
		{"int64上界", math.MaxInt64, "92233720368547758.07"}, {"零", 0, ""}, {"负数", -1, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// page 支付仅生成签名 URL,没有出站请求。
			got, err := gw.Prepare(t.Context(), paygateway.PrepareInput{OutTradeNo: "PT-signed", Amount: tc.fen, Subject: "整数分"})
			if tc.fen <= 0 {
				if !errors.Is(err, paymgr.ErrInvalidParam) {
					t.Fatalf("非法金额应拒绝: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			u, err := url.Parse(got.AppParams)
			if err != nil {
				t.Fatal(err)
			}
			var biz struct {
				TotalAmount string `json:"total_amount"`
			}
			if err := json.Unmarshal([]byte(u.Query().Get("biz_content")), &biz); err != nil {
				t.Fatal(err)
			}
			if biz.TotalAmount != tc.want {
				t.Fatalf("total_amount=%q want=%q", biz.TotalAmount, tc.want)
			}
		})
	}
}
