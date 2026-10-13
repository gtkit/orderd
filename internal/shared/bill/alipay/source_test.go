package alipay

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gtkit/orderd/internal/models"
	"github.com/gtkit/orderd/internal/shared/bill"
	sdk "github.com/smartwalle/alipay/v3"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type apiReply struct {
	body             map[string]any
	tamper, omitSign bool
}

func signedSource(t *testing.T, reply func(*http.Request) apiReply) *Source {
	t.Helper()
	merchant, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	platform, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		sig, err := base64.StdEncoding.DecodeString(r.Form.Get("sign"))
		if err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		var keys []string
		for key := range r.Form {
			if key != "sign" && r.Form.Get(key) != "" {
				keys = append(keys, key)
			}
		}
		slices.Sort(keys)
		var pieces []string
		for _, key := range keys {
			pieces = append(pieces, key+"="+r.Form.Get(key))
		}
		digest := sha256.Sum256([]byte(strings.Join(pieces, "&")))
		if err := rsa.VerifyPKCS1v15(&merchant.PublicKey, crypto.SHA256, digest[:], sig); err != nil {
			t.Errorf("request signature: %v", err)
			w.WriteHeader(400)
			return
		}
		if r.Form.Get("app_id") != "app" || r.Form.Get("sign_type") != "RSA2" {
			t.Error("wrong API identity/signature algorithm")
		}
		result := reply(r)
		data, err := json.Marshal(result.body)
		if err != nil {
			t.Error(err)
			return
		}
		hash := sha256.Sum256(data)
		signature, err := rsa.SignPKCS1v15(rand.Reader, platform, crypto.SHA256, hash[:])
		if err != nil {
			t.Error(err)
			return
		}
		if result.tamper {
			data = []byte(`{"code":"10000","bill_download_url":"https://dwbillcenter.alipay.com/evil"}`)
		}
		envelope := map[string]any{strings.ReplaceAll(r.Form.Get("method"), ".", "_") + "_response": json.RawMessage(data)}
		if !result.omitSign {
			envelope["sign"] = base64.StdEncoding.EncodeToString(signature)
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(envelope); err != nil {
			t.Error(err)
		}
	}))
	t.Cleanup(server.Close)
	client, err := sdk.New("app", string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(merchant)})), false, sdk.WithSandboxGateway(server.URL), sdk.WithHTTPClient(server.Client()))
	if err != nil {
		t.Fatal(err)
	}
	pub, err := x509.MarshalPKIXPublicKey(&platform.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.LoadAliPayPublicKey(string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pub}))); err != nil {
		t.Fatal(err)
	}
	return &Source{api: client, accountID: "account", sellerID: fixtureSeller}
}

func downloadServer(t *testing.T, s *Source, data []byte, status int) *atomic.Int64 {
	t.Helper()
	var hits atomic.Int64
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits.Add(1); w.WriteHeader(status); _, _ = w.Write(data) }))
	t.Cleanup(server.Close)
	target, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	client := server.Client()
	transport := client.Transport
	client.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		copyRequest := r.Clone(r.Context())
		copyURL := *r.URL
		copyURL.Host = target.Host
		copyURL.Scheme = target.Scheme
		copyRequest.URL = &copyURL
		return transport.RoundTrip(copyRequest)
	})
	s.downloader = client
	return &hits
}

func TestFetchSignedSDKAndZIP(t *testing.T) {
	t.Parallel()
	s := signedSource(t, func(r *http.Request) apiReply {
		if r.Form.Get("method") != "alipay.data.dataservice.bill.downloadurl.query" {
			t.Error("wrong API")
		}
		var biz map[string]string
		if err := json.Unmarshal([]byte(r.Form.Get("biz_content")), &biz); err != nil {
			t.Error(err)
		}
		if biz["secure"] != "true" || biz["bill_type"] != "trade" || biz["bill_date"] != "2025-01-02" {
			t.Errorf("biz=%v", biz)
		}
		return apiReply{body: map[string]any{"code": "10000", "bill_download_url": "https://dwbillcenter.alipay.com/downloadBillFile.resource?token=secret"}}
	})
	data := zipped(t, []byte(fixtureCSV([]string{fixturePay, fixtureRefund}, 1, 1, "9.00", "-1.80")))
	wantDigest := fmt.Sprintf("%x", sha256.Sum256(data))
	hits := downloadServer(t, s, data, 200)
	var mu sync.Mutex
	var failures []error
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			var count int
			sum, err := s.Fetch(t.Context(), "alipay", "account", fixtureDay, func(bill.Entry) error { count++; return nil })
			if err != nil || sum.Count != 2 || count != 2 || sum.DigestAlgorithm != "SHA256" || sum.Digest != wantDigest {
				mu.Lock()
				failures = append(failures, fmt.Errorf("sum=%+v count=%d err=%w", sum, count, err))
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	if len(failures) > 0 || hits.Load() != 4 {
		t.Fatalf("failures=%v hits=%d", failures, hits.Load())
	}
}

func TestFetchRejectsMetadataAndDoesNotDownload(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		reply apiReply
	}{
		{"tamper", apiReply{body: map[string]any{"code": "10000"}, tamper: true}},
		{"unsigned", apiReply{body: map[string]any{"code": "10000"}, omitSign: true}},
		{"not_exist", apiReply{body: map[string]any{"code": "40004", "sub_code": "BILL_NOT_EXIST"}}},
		{"missing_url", apiReply{body: map[string]any{"code": "10000"}}},
		{"http", apiReply{body: map[string]any{"code": "10000", "bill_download_url": "http://dwbillcenter.alipay.com/file"}}},
		{"wrong_host", apiReply{body: map[string]any{"code": "10000", "bill_download_url": "https://dwbillcenter.alipay.com.attacker.test/file?token=secret"}}},
		{"unknown_file_code", apiReply{body: map[string]any{"code": "10000", "bill_download_url": "https://dwbillcenter.alipay.com/file", "bill_file_code": "SOMETHING_NEW"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := signedSource(t, func(*http.Request) apiReply { return tc.reply })
			s.downloader = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				t.Error("downloaded untrusted metadata")
				return nil, errors.New("must not download")
			})}
			sum, err := s.Fetch(t.Context(), "alipay", "account", fixtureDay, func(bill.Entry) error { t.Error("emitted"); return nil })
			if !errors.Is(err, ErrFetch) || sum.Count != 0 {
				t.Fatalf("sum=%+v err=%v", sum, err)
			}
		})
	}
}

func TestFetchDownloadFailuresAndFalseEmpty(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		status int
		data   []byte
		code   string
	}{
		{"http_failure", 500, []byte("secret"), ""},
		{"html_with_200", 200, []byte("<html>expired URL</html>"), ""},
		{"false_empty", 200, zipped(t, []byte(fixtureCSV([]string{fixturePay}, 1, 0, "9.00", "0.00"))), "EMPTY_DATA_WITH_BILL_FILE"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := signedSource(t, func(*http.Request) apiReply {
				return apiReply{body: map[string]any{"code": "10000", "bill_download_url": "https://dwbillcenter.alipay.com/file", "bill_file_code": tc.code}}
			})
			downloadServer(t, s, tc.data, tc.status)
			if sum, err := s.Fetch(t.Context(), "alipay", "account", fixtureDay, func(bill.Entry) error { return nil }); err == nil || sum.Count != 0 {
				t.Fatalf("sum=%+v err=%v", sum, err)
			}
		})
	}
}

func TestFetchScopeCancellationAndTemporaryCleanup(t *testing.T) {
	// This test changes only its temporary directory, so it is deliberately not parallel.
	tmp := t.TempDir()
	t.Setenv("TMPDIR", tmp)
	s := signedSource(t, func(*http.Request) apiReply {
		return apiReply{body: map[string]any{"code": "10000", "bill_download_url": "https://dwbillcenter.alipay.com/file"}}
	})
	downloadServer(t, s, zipped(t, []byte(fixtureCSV([]string{fixturePay}, 1, 0, "9.00", "0.00"))), 200)
	for _, stop := range []bool{false, true} {
		_, err := s.Fetch(t.Context(), "alipay", "account", fixtureDay, func(bill.Entry) error {
			if stop {
				return io.ErrClosedPipe
			}
			return nil
		})
		if stop && !errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("err=%v", err)
		}
		files, err := os.ReadDir(tmp)
		if err != nil {
			t.Fatal(err)
		}
		if len(files) != 0 {
			t.Fatalf("temporary files leaked: %d", len(files))
		}
	}
	for _, day := range []time.Time{time.Time{}, time.Now().AddDate(0, 0, 1), time.Now().AddDate(-7, 0, 0)} {
		if _, err := s.Fetch(t.Context(), "alipay", "account", day, func(bill.Entry) error { return nil }); !errors.Is(err, ErrFetch) {
			t.Fatalf("date error=%v", err)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := s.Fetch(ctx, "alipay", "account", fixtureDay, func(bill.Entry) error { return nil }); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel error=%v", err)
	}
	if _, err := s.Fetch(t.Context(), "alipay", "wrong", fixtureDay, func(bill.Entry) error { return nil }); !errors.Is(err, ErrFetch) {
		t.Fatalf("scope error=%v", err)
	}
}

func TestNewValidatesAndReadsExplicitCredentials(t *testing.T) {
	t.Parallel()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	private := string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}))
	publicBytes, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	public := string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: publicBytes}))
	valid := Config{PayAccountID: "account", SellerID: fixtureSeller, AppID: "app", PrivateKey: private, AlipayPublicKey: public}
	for _, production := range []bool{false, true} {
		cfg := valid
		cfg.IsProduction = production
		if _, err := New(cfg); err != nil {
			t.Fatal(err)
		}
	}
	path := filepath.Join(t.TempDir(), "generated-test-key.pem")
	if err := os.WriteFile(path, []byte(private), 0600); err != nil {
		t.Fatal(err)
	}
	pathCfg := valid
	pathCfg.PrivateKey = ""
	pathCfg.PrivateKeyPath = path
	if _, err := New(pathCfg); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*Config){func(c *Config) { c.SellerID = "" }, func(c *Config) { c.PrivateKey = "bad" }, func(c *Config) { c.AlipayPublicKey = "bad" }, func(c *Config) { c.AppCertPublicKey = "incomplete" }, func(c *Config) { c.PrivateKey = ""; c.PrivateKeyPath = "missing" }} {
		cfg := valid
		mutate(&cfg)
		if _, err := New(cfg); err == nil {
			t.Fatal("invalid config accepted")
		}
	}
	if strings.Contains(fmt.Sprint(valid), private) {
		t.Fatal("config string leaks private key")
	}
}

func TestVerifySignedPaymentAndRefund(t *testing.T) {
	t.Parallel()
	for _, kind := range []models.TxnKind{models.TxnPay, models.TxnRefund} {
		t.Run(fmt.Sprint(kind), func(t *testing.T) {
			s := signedSource(t, func(r *http.Request) apiReply {
				if r.Form.Get("method") == "alipay.trade.query" {
					return apiReply{body: map[string]any{"code": "10000", "trade_no": "trade1", "out_trade_no": "order1", "trade_status": "TRADE_SUCCESS", "total_amount": "10.00", "send_pay_date": "2025-01-02 12:00:00"}}
				}
				var params sdk.TradeFastPayRefundQuery
				if err := json.Unmarshal([]byte(r.Form.Get("biz_content")), &params); err != nil {
					t.Error(err)
				}
				if params.OutRequestNo != "refund1" || !slices.Contains(params.QueryOptions, "gmt_refund_pay") {
					t.Errorf("params=%+v", params)
				}
				return apiReply{body: map[string]any{"code": "10000", "trade_no": "trade1", "out_trade_no": "order1", "out_request_no": "refund1", "refund_status": "REFUND_SUCCESS", "refund_amount": "2.00", "total_amount": "10.00", "gmt_refund_pay": "2025-01-03 13:00:00"}}
			})
			e := bill.Entry{Kind: kind, TransactionID: "trade1", OutTradeNo: "order1", Amount: 1000, Currency: "CNY", MerchantID: fixtureSeller}
			if kind == models.TxnRefund {
				e.TransactionID = ""
				e.OriginalTransactionID = "trade1"
				e.OutRefundNo = "refund1"
				e.Amount = 200
			}
			got, err := s.Verify(t.Context(), "alipay", "account", e)
			if err != nil || !got.Confirmed || got.Amount != e.Amount || got.OutTradeNo != "order1" || got.At.IsZero() {
				t.Fatalf("got=%+v err=%v", got, err)
			}
			wantAt := fixtureDay.Add(12 * time.Hour)
			if kind == models.TxnRefund {
				wantAt = fixtureDay.Add(37 * time.Hour)
			}
			if !got.At.Equal(wantAt) {
				t.Fatalf("time=%v want=%v", got.At, wantAt)
			}
		})
	}
}

func TestVerifyRejectsWrongOrUncertainFacts(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, key, value string
		uncertain        bool
	}{
		{"wrong_trade", "trade_no", "another", false}, {"missing_trade", "trade_no", "", false},
		{"wrong_out", "out_trade_no", "another", false}, {"missing_out", "out_trade_no", "", false},
		{"wrong_amount", "total_amount", "9.00", false}, {"fraction", "total_amount", "10.001", false},
		{"wrong_currency", "trans_currency", "USD", false}, {"credit", "credit_pay_mode", "creditAdvanceV2", false},
		{"credit_preauth", "auth_trade_pay_mode", "CREDIT_PREAUTH_PAY", false}, {"submerchant", "alipay_sub_merchant_id", "submerchant", false},
		{"not_received", "additional_status", "SELLER_NOT_RECEIVED", false},
		{"closed", "trade_status", "TRADE_CLOSED", true}, {"pending", "trade_status", "WAIT_BUYER_PAY", true},
		{"no_time", "send_pay_date", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := signedSource(t, func(*http.Request) apiReply {
				body := map[string]any{"code": "10000", "trade_no": "trade1", "out_trade_no": "order1", "trade_status": "TRADE_SUCCESS", "total_amount": "10.00", "send_pay_date": "2025-01-02 12:00:00"}
				body[tc.key] = tc.value
				return apiReply{body: body}
			})
			got, err := s.Verify(t.Context(), "alipay", "account", bill.Entry{Kind: models.TxnPay, TransactionID: "trade1", OutTradeNo: "order1", Amount: 1000, Currency: "CNY", MerchantID: fixtureSeller})
			if got.Confirmed || (!tc.uncertain && !errors.Is(err, ErrInvalidBill)) || (tc.uncertain && err != nil) {
				t.Fatalf("got=%+v err=%v", got, err)
			}
		})
	}
}

var _ bill.Source = (*Source)(nil)
var _ bill.Verifier = (*Source)(nil)

func TestVerifyRefundRejectsWrongOrUncertainFacts(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, key, value string
		uncertain        bool
	}{
		{"wrong_original", "trade_no", "another", false},
		{"missing_original", "trade_no", "", false},
		{"wrong_order", "out_trade_no", "another", false},
		{"missing_order", "out_trade_no", "", false},
		{"wrong_refund", "out_request_no", "another", false},
		{"missing_refund", "out_request_no", "", false},
		{"fraction", "refund_amount", "2.001", false},
		{"wrong_amount", "refund_amount", "1.00", false},
		{"zero", "refund_amount", "0", false},
		{"wrong_original_amount", "total_amount", "9.00", false},
		{"missing_time", "gmt_refund_pay", "", false},
		{"processing", "refund_status", "REFUND_PROCESSING", true},
		{"failed", "refund_status", "REFUND_FAIL", true},
		{"no_success", "refund_status", "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := signedSource(t, func(r *http.Request) apiReply {
				if r.Form.Get("method") == "alipay.trade.query" {
					return apiReply{body: map[string]any{"code": "10000", "trade_no": "trade1", "out_trade_no": "order1", "trade_status": "TRADE_CLOSED", "total_amount": "10.00"}}
				}
				body := map[string]any{"code": "10000", "trade_no": "trade1", "out_trade_no": "order1", "out_request_no": "refund1", "refund_status": "REFUND_SUCCESS", "total_amount": "10.00", "refund_amount": "2.00", "gmt_refund_pay": "2025-01-03 13:00:00"}
				body[tc.key] = tc.value
				return apiReply{body: body}
			})
			got, err := s.Verify(t.Context(), "alipay", "account", bill.Entry{Kind: models.TxnRefund, OriginalTransactionID: "trade1", OutTradeNo: "order1", OutRefundNo: "refund1", Amount: 200, Currency: "CNY", MerchantID: fixtureSeller})
			if got.Confirmed || (!tc.uncertain && !errors.Is(err, ErrInvalidBill)) || (tc.uncertain && err != nil) {
				t.Fatalf("got=%+v err=%v", got, err)
			}
		})
	}
}

func TestVerifyPreservesLargeAmountsAndRejectsForgedScope(t *testing.T) {
	t.Parallel()
	var hits atomic.Int64
	s := signedSource(t, func(*http.Request) apiReply {
		hits.Add(1)
		return apiReply{body: map[string]any{"code": "10000", "trade_no": "trade1", "out_trade_no": "order1", "trade_status": "TRADE_FINISHED", "total_amount": "90071992547409.93", "trans_currency": "CNY", "send_pay_date": "2025-01-02 12:00:00"}}
	})
	e := bill.Entry{Kind: models.TxnPay, TransactionID: "trade1", OutTradeNo: "order1", Amount: 9007199254740993, Currency: "CNY", MerchantID: fixtureSeller}
	got, err := s.Verify(t.Context(), "alipay", "account", e)
	if err != nil || !got.Confirmed || got.Amount != e.Amount {
		t.Fatalf("got=%+v err=%v", got, err)
	}
	for _, mutate := range []func(*bill.Entry){func(e *bill.Entry) { e.MerchantID = "wrong" }, func(e *bill.Entry) { e.Currency = "USD" }, func(e *bill.Entry) { e.ProviderAppID = "invented" }, func(e *bill.Entry) { e.Kind = -1 }, func(e *bill.Entry) { e.TransactionID = "" }} {
		copyEntry := e
		mutate(&copyEntry)
		if got, err := s.Verify(t.Context(), "alipay", "account", copyEntry); got.Confirmed || !errors.Is(err, ErrInvalidBill) {
			t.Fatalf("got=%+v err=%v", got, err)
		}
	}
	if hits.Load() != 1 {
		t.Fatalf("untrusted scope reached API: %d", hits.Load())
	}
}

func TestVerifyRequiresResponseSignatureAndSuccessfulResponse(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		reply   apiReply
		wantErr bool
	}{
		{"tampered", apiReply{body: map[string]any{"code": "10000"}, tamper: true}, true},
		{"unsigned", apiReply{body: map[string]any{"code": "10000"}, omitSign: true}, true},
		{"nil_response", apiReply{}, false},
		{"business_failure", apiReply{body: map[string]any{"code": "40004", "sub_code": "ACQ.TRADE_NOT_EXIST"}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := signedSource(t, func(*http.Request) apiReply { return tc.reply })
			got, err := s.Verify(t.Context(), "alipay", "account", bill.Entry{Kind: models.TxnPay, TransactionID: "trade1", OutTradeNo: "order1", Amount: 1000, Currency: "CNY", MerchantID: fixtureSeller})
			if got.Confirmed || (tc.wantErr && !errors.Is(err, ErrFetch)) || (!tc.wantErr && err != nil) {
				t.Fatalf("got=%+v err=%v", got, err)
			}
		})
	}
}
