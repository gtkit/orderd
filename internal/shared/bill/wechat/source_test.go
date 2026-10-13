package wechat

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1" // #nosec G505 -- Official protocol test fixture.
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gtkit/orderd/internal/shared/bill"
	"github.com/gtkit/orderd/internal/shared/bill/billio"
)

type roundTripper func(*http.Request) (*http.Response, error)

func (f roundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type trackedBody struct {
	io.Reader
	closed bool
}

func (b *trackedBody) Close() error { b.closed = true; return nil }

type signingFixture struct {
	cfg                Config
	merchant, platform *rsa.PrivateKey
	serial             string
}

func signingKeys(t *testing.T) signingFixture {
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
	cfg := Config{PayAccountID: "account", AppID: "wx-app", MchID: "1234567890", MchCertSerialNumber: "AABBCC", MchPrivateKeyPEM: string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: private})), WechatPayPublicKeyID: "PUB_KEY_ID_TEST", WechatPayPublicKeyPEM: string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: public}))}
	return signingFixture{cfg: cfg, merchant: merchant, platform: platform, serial: cfg.WechatPayPublicKeyID}
}
func (f signingFixture) certificate(t *testing.T) signingFixture {
	t.Helper()
	template := x509.Certificate{SerialNumber: big.NewInt(0xABCDE), Subject: pkix.Name{CommonName: "test platform"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature}
	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &f.platform.PublicKey, f.platform)
	if err != nil {
		t.Fatal(err)
	}
	f.cfg.WechatPayCertificatePEM = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	f.cfg.WechatPayPublicKeyID = ""
	f.cfg.WechatPayPublicKeyPEM = ""
	f.serial = "0ABCDE" // SDK formats the serial's bytes, retaining the leading zero nibble.
	return f
}
func (f signingFixture) response(t *testing.T, body string, status int) *http.Response {
	t.Helper()
	stamp := strconv.FormatInt(time.Now().Unix(), 10)
	nonce := "nonce-for-local-response"
	digest := sha256.Sum256([]byte(stamp + "\n" + nonce + "\n" + body + "\n"))
	signature, err := rsa.SignPKCS1v15(rand.Reader, f.platform, crypto.SHA256, digest[:])
	if err != nil {
		t.Fatal(err)
	}
	header := make(http.Header)
	header.Set("Wechatpay-Timestamp", stamp)
	header.Set("Wechatpay-Nonce", nonce)
	header.Set("Wechatpay-Serial", f.serial)
	header.Set("Wechatpay-Signature", base64.StdEncoding.EncodeToString(signature))
	return &http.Response{StatusCode: status, Header: header, Body: &trackedBody{Reader: strings.NewReader(body)}}
}
func (f signingFixture) verifyRequest(t *testing.T, r *http.Request) {
	t.Helper()
	scheme, params, ok := strings.Cut(r.Header.Get("Authorization"), " ")
	if !ok || scheme != "WECHATPAY2-SHA256-RSA2048" {
		t.Fatal("missing merchant authorization")
	}
	fields := make(map[string]string)
	for item := range strings.SplitSeq(params, ",") {
		key, value, ok := strings.Cut(strings.TrimSpace(item), "=")
		if !ok {
			t.Fatal("invalid authorization")
		}
		fields[key] = strings.Trim(value, "\"")
	}
	if fields["mchid"] != f.cfg.MchID || fields["serial_no"] != f.cfg.MchCertSerialNumber {
		t.Fatal("wrong signing merchant")
	}
	signature, err := base64.StdEncoding.DecodeString(fields["signature"])
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte(r.Method + "\n" + r.URL.RequestURI() + "\n" + fields["timestamp"] + "\n" + fields["nonce_str"] + "\n\n"))
	if err := rsa.VerifyPKCS1v15(&f.merchant.PublicKey, crypto.SHA256, digest[:], signature); err != nil {
		t.Fatal("request signature did not cover exact raw path/query")
	}
	if _, ok := r.Context().Deadline(); !ok {
		t.Fatal("request lacks deadline")
	}
}
func (f signingFixture) source(t *testing.T, rt roundTripper) *Source {
	t.Helper()
	s, err := newSource(t.Context(), f.cfg, &http.Client{Transport: rt})
	if err != nil {
		t.Fatal(err)
	}
	s.now = func() time.Time { return fixtureDay.Add(24 * time.Hour) }
	return s
}
func metadataFor(t *testing.T, raw, downloadURL string) string {
	t.Helper()
	digest := sha1.Sum([]byte(raw)) // #nosec G401 -- Official bill checksum fixture.
	out, err := json.Marshal(map[string]string{"hash_type": "SHA1", "hash_value": hex.EncodeToString(digest[:]), "download_url": downloadURL})
	if err != nil {
		t.Fatal(err)
	}
	return string(out)
}

const downloadURL = "https://api.mch.weixin.qq.com/v3/billdownload/file?token=secret%2Fvalue&z=%2f&x=a+b"

func TestFetchSignedRequestMetadataAndUnsignedFile(t *testing.T) {
	t.Parallel()
	keys := signingKeys(t)
	for _, mode := range []string{"public-key", "certificate"} {
		t.Run(mode, func(t *testing.T) {
			f := keys
			if mode == "certificate" {
				f = f.certificate(t)
			}
			raw := "\ufeff" + strings.ReplaceAll(fixture(paymentRow(), refundRow()), "\n", "\r\n")
			var bodies []*trackedBody
			calls := 0
			s := f.source(t, func(r *http.Request) (*http.Response, error) {
				calls++
				f.verifyRequest(t, r)
				if r.URL.Path == "/v3/bill/tradebill" {
					if r.URL.Query().Get("bill_date") != "2026-10-11" || r.URL.Query().Get("bill_type") != "ALL" || r.URL.Query().Has("tar_type") || r.Header.Get("Accept") != "application/json" {
						t.Fatal("wrong bill application contract")
					}
					digest := sha1.Sum([]byte(raw)) // #nosec G401 -- Official checksum fixture.
					encoded := hex.EncodeToString(digest[:])
					metadata := strings.Replace(metadataFor(t, raw, downloadURL), encoded, strings.ToUpper(encoded), 1)
					response := f.response(t, metadata, 200)
					bodies = append(bodies, response.Body.(*trackedBody))
					return response, nil
				}
				if r.URL.String() != downloadURL {
					t.Fatal("download raw URL was rewritten")
				}
				body := &trackedBody{Reader: strings.NewReader(raw)}
				bodies = append(bodies, body)
				return &http.Response{StatusCode: 200, Header: make(http.Header), Body: body}, nil
			})
			var entries []bill.Entry
			summary, err := s.Fetch(t.Context(), "wechat", "account", fixtureDay, func(e bill.Entry) error { entries = append(entries, e); return nil })
			if err != nil || summary.Count != 2 || summary.PayAmount != 1000 || summary.RefundAmount != 700 || len(entries) != 2 || calls != 2 {
				t.Fatalf("summary=%+v emitted=%d calls=%d err=%v", summary, len(entries), calls, err)
			}
			digest := sha1.Sum([]byte(raw)) // #nosec G401 -- Official checksum fixture.
			if summary.DigestAlgorithm != "SHA1" || summary.Digest != hex.EncodeToString(digest[:]) {
				t.Fatalf("verified audit digest missing or not normalized: %+v", summary)
			}
			for _, body := range bodies {
				if !body.closed {
					t.Fatal("SDK discarded an unclosed original body")
				}
			}
		})
	}
}

func TestFetchMetadataAndDownloadFailuresNeverEmit(t *testing.T) {
	t.Parallel()
	f := signingKeys(t)
	raw := fixture(paymentRow(), refundRow())
	for _, name := range []string{"missing-signature", "wrong-signature", "expired-signature", "wrong-serial", "unknown-hash", "invalid-hash", "missing-url", "bad-json", "trailing-json", "http-url", "foreign-host", "userinfo", "fragment", "wrong-port", "encoded-path", "unknown-path", "wrong-digest", "truncated-file", "compressed", "read-error", "network-error"} {
		t.Run(name, func(t *testing.T) {
			calls := 0
			s := f.source(t, func(r *http.Request) (*http.Response, error) {
				calls++
				if r.URL.Path == "/v3/bill/tradebill" {
					if name == "network-error" {
						return nil, errors.New("GET " + downloadURL + " Authorization secret")
					}
					target := downloadURL
					switch name {
					case "http-url":
						target = strings.Replace(target, "https:", "http:", 1)
					case "foreign-host":
						target = "https://evil.example/v3/billdownload/file?secret"
					case "userinfo":
						target = "https://secret@api.mch.weixin.qq.com/v3/billdownload/file"
					case "fragment":
						target += "#secret"
					case "wrong-port":
						target = "https://api.mch.weixin.qq.com:444/v3/billdownload/file"
					case "encoded-path":
						target = "https://api.mch.weixin.qq.com/v3/%62illdownload/file"
					case "unknown-path":
						target = "https://api.mch.weixin.qq.com/v3/private"
					}
					meta := metadataFor(t, raw, target)
					switch name {
					case "unknown-hash":
						meta = strings.Replace(meta, "SHA1", "SHA256", 1)
					case "invalid-hash":
						meta = strings.Replace(meta, "hash_value\":\"", "hash_value\":\"00", 1)
					case "missing-url":
						meta = `{"hash_type":"SHA1","hash_value":"0000000000000000000000000000000000000000"}`
					case "bad-json":
						meta = "{"
					case "trailing-json":
						meta += "{}"
					}
					response := f.response(t, meta, 200)
					switch name {
					case "missing-signature":
						response.Header.Del("Wechatpay-Signature")
					case "wrong-signature":
						response.Header.Set("Wechatpay-Signature", base64.StdEncoding.EncodeToString(make([]byte, 256)))
					case "expired-signature":
						response.Header.Set("Wechatpay-Timestamp", "1")
					case "wrong-serial":
						response.Header.Set("Wechatpay-Serial", "OTHER")
					}
					return response, nil
				}
				contents := raw
				header := make(http.Header)
				if name == "wrong-digest" {
					contents = strings.Replace(contents, "product", "Product", 1)
				}
				if name == "truncated-file" {
					contents = contents[:len(contents)-1]
				}
				if name == "compressed" {
					header.Set("Content-Encoding", "gzip")
				}
				var reader io.Reader = strings.NewReader(contents)
				if name == "read-error" {
					reader = io.MultiReader(reader, errorReader{})
				}
				return &http.Response{StatusCode: 200, Header: header, Body: &trackedBody{Reader: reader}}, nil
			})
			emitted := 0
			summ, err := s.Fetch(t.Context(), "wechat", "account", fixtureDay, func(bill.Entry) error { emitted++; return nil })
			if err == nil || emitted != 0 || summ != (bill.Summary{}) {
				t.Fatalf("summ=%+v emitted=%d calls=%d err=%v", summ, emitted, calls, err)
			}
			if strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "Authorization") {
				t.Fatal("sensitive response/URL escaped into error")
			}
			if name != "wrong-digest" && name != "truncated-file" && name != "compressed" && name != "read-error" && calls != 1 {
				t.Fatalf("unsafe metadata caused download: %d calls", calls)
			}
		})
	}
}

type errorReader struct{}

func (errorReader) Read([]byte) (int, error) { return 0, errors.New("sensitive raw file read failure") }

func TestFetchExplicitNoStatementAndOtherAPIErrors(t *testing.T) {
	t.Parallel()
	f := signingKeys(t)
	for _, code := range []string{"NO_STATEMENT_EXIST", "STATEMENT_CREATING", "SIGN_ERROR", "NO_AUTH", "SYSTEM_ERROR"} {
		t.Run(code, func(t *testing.T) {
			s := f.source(t, func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 400, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"code":"` + code + `","message":"secret"}`))}, nil
			})
			summary, err := s.Fetch(t.Context(), "wechat", "account", fixtureDay, func(bill.Entry) error { t.Fatal("error response emitted rows"); return nil })
			if code == "NO_STATEMENT_EXIST" {
				if err != nil || summary != (bill.Summary{NoStatement: true}) {
					t.Fatalf("summary=%+v err=%v", summary, err)
				}
			} else if err == nil || summary != (bill.Summary{}) {
				t.Fatalf("error became empty success: %+v %v", summary, err)
			}
		})
	}
}

func TestFetchScopeHistoryAndCancellationBeforeNetwork(t *testing.T) {
	t.Parallel()
	f := signingKeys(t)
	calls := 0
	s := f.source(t, func(*http.Request) (*http.Response, error) { calls++; return nil, context.Canceled })
	for _, day := range []time.Time{fixtureDay.AddDate(0, 0, 1), fixtureDay.AddDate(0, 0, 2), fixtureDay.AddDate(0, -4, 0)} {
		if _, err := s.Fetch(t.Context(), "wechat", "account", day, func(bill.Entry) error { return nil }); err == nil {
			t.Fatal("unsupported date accepted")
		}
	}
	for _, scope := range [][2]string{{"alipay", "account"}, {"wechat", "wrong"}} {
		if _, err := s.Fetch(t.Context(), scope[0], scope[1], fixtureDay, func(bill.Entry) error { return nil }); err == nil {
			t.Fatal("wrong scope accepted")
		}
	}
	if calls != 0 {
		t.Fatalf("invalid scope performed %d calls", calls)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := s.Fetch(ctx, "wechat", "account", fixtureDay, func(bill.Entry) error { return nil }); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel error lost: %v", err)
	}
}

func TestFetchTemporaryCleanupAndCallbackError(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("TMPDIR", dir)
	f := signingKeys(t)
	raw := fixture(paymentRow(), refundRow())
	rejected := errors.New("staging rejected")
	for _, failure := range []bool{false, true} {
		s := f.source(t, func(r *http.Request) (*http.Response, error) {
			if r.URL.Path == "/v3/bill/tradebill" {
				return f.response(t, metadataFor(t, raw, downloadURL), 200), nil
			}
			contents := raw
			if failure {
				contents += "corruption"
			}
			return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(contents))}, nil
		})
		_, err := s.Fetch(t.Context(), "wechat", "account", fixtureDay, func(bill.Entry) error { return rejected })
		if err == nil || (!failure && !errors.Is(err, rejected)) {
			t.Fatalf("unexpected error: %v", err)
		}
		entries, err := os.ReadDir(dir)
		if err != nil || len(entries) != 0 {
			t.Fatalf("temp artifacts remained: count=%d err=%v", len(entries), err)
		}
	}
}

func TestNewCredentialModesAndOfflineConstruction(t *testing.T) {
	t.Parallel()
	f := signingKeys(t)
	if _, err := New(t.Context(), f.cfg); err != nil {
		t.Fatalf("offline public-key constructor failed: %v", err)
	}
	cert := f.certificate(t)
	if _, err := New(t.Context(), cert.cfg); err != nil {
		t.Fatalf("offline certificate constructor failed: %v", err)
	}
	dir := t.TempDir()
	privatePath := filepath.Join(dir, "private.pem")
	publicPath := filepath.Join(dir, "public.pem")
	certPath := filepath.Join(dir, "cert.pem")
	for path, data := range map[string]string{privatePath: f.cfg.MchPrivateKeyPEM, publicPath: f.cfg.WechatPayPublicKeyPEM, certPath: cert.cfg.WechatPayCertificatePEM} {
		if err := os.WriteFile(path, []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
	}
	cfg := f.cfg
	cfg.MchPrivateKeyPath = privatePath
	cfg.MchPrivateKeyPEM = ""
	cfg.WechatPayPublicKeyPath = publicPath
	cfg.WechatPayPublicKeyPEM = ""
	if _, err := New(t.Context(), cfg); err != nil {
		t.Fatal(err)
	}
	cfg.WechatPayPublicKeyID = ""
	cfg.WechatPayPublicKeyPath = ""
	cfg.WechatPayCertificatePath = certPath
	if _, err := New(t.Context(), cfg); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*Config){func(c *Config) { c.AppID = "" }, func(c *Config) { c.MchID = "" }, func(c *Config) { c.PayAccountID = "" }, func(c *Config) { c.MchPrivateKeyPEM = "invalid" }, func(c *Config) { c.WechatPayPublicKeyID = "" }, func(c *Config) { c.WechatPayPublicKeyPEM = "" }, func(c *Config) { c.WechatPayPublicKeyPEM = "invalid" }, func(c *Config) {
		c.WechatPayPublicKeyID = ""
		c.WechatPayPublicKeyPEM = ""
		c.WechatPayCertificatePEM = "invalid"
	}} {
		cfg := f.cfg
		mutate(&cfg)
		if _, err := New(t.Context(), cfg); err == nil {
			t.Fatal("invalid credentials accepted")
		}
	}
}

func TestFetchConcurrentReuse(t *testing.T) {
	t.Parallel()
	f := signingKeys(t)
	raw := fixture(paymentRow(), refundRow())
	s := f.source(t, func(r *http.Request) (*http.Response, error) {
		if r.URL.Path == "/v3/bill/tradebill" {
			return f.response(t, metadataFor(t, raw, downloadURL), 200), nil
		}
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(raw))}, nil
	})
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			summary, err := s.Fetch(t.Context(), "wechat", "account", fixtureDay, func(bill.Entry) error { return nil })
			if err != nil || summary.Count != 2 {
				t.Errorf("summary=%+v err=%v", summary, err)
			}
		})
	}
	wg.Wait()
}

func TestFetchDownloadHardLimit(t *testing.T) {
	t.Parallel()
	f := signingKeys(t)
	s := f.source(t, func(r *http.Request) (*http.Response, error) {
		if r.URL.Path == "/v3/bill/tradebill" {
			return f.response(t, metadataFor(t, "", downloadURL), 200), nil
		}
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(&repeatReader{row: strings.Repeat("x", 1<<20), remaining: 65})}, nil
	})
	emitted := 0
	if _, err := s.Fetch(t.Context(), "wechat", "account", fixtureDay, func(bill.Entry) error { emitted++; return nil }); !errors.Is(err, billio.ErrLimit) || emitted != 0 {
		t.Fatalf("limit not enforced: emitted=%d err=%v", emitted, err)
	}
}

func TestNewRejectsNonRSAPlatformCertificate(t *testing.T) {
	t.Parallel()
	f := signingKeys(t)
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := x509.Certificate{SerialNumber: big.NewInt(123), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour)}
	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	f.cfg.WechatPayPublicKeyID = ""
	f.cfg.WechatPayPublicKeyPEM = ""
	f.cfg.WechatPayCertificatePEM = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	if _, err := New(t.Context(), f.cfg); err == nil {
		t.Fatal("certificate that would panic inside SDK accepted")
	}
}

type cancelReader struct {
	r      io.Reader
	cancel context.CancelFunc
}

func (r cancelReader) Read(p []byte) (int, error) { n, err := r.r.Read(p); r.cancel(); return n, err }
func TestFetchCancellationDuringDownloadClosesBody(t *testing.T) {
	t.Parallel()
	f := signingKeys(t)
	raw := fixture(paymentRow(), refundRow())
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	body := &trackedBody{Reader: cancelReader{r: strings.NewReader(raw), cancel: cancel}}
	s := f.source(t, func(r *http.Request) (*http.Response, error) {
		if r.URL.Path == "/v3/bill/tradebill" {
			return f.response(t, metadataFor(t, raw, downloadURL), 200), nil
		}
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: body}, nil
	})
	count := 0
	if _, err := s.Fetch(ctx, "wechat", "account", fixtureDay, func(bill.Entry) error { count++; return nil }); !errors.Is(err, context.Canceled) || count != 0 || !body.closed {
		t.Fatalf("cancel did not release download: emitted=%d closed=%v err=%v", count, body.closed, err)
	}
}

func TestFetchVerifiedDigestCannotReplaceTailValidation(t *testing.T) {
	t.Parallel()
	f := signingKeys(t)
	raw := strings.Replace(fixture(paymentRow(), refundRow()), "\n`2,", "\n`3,", 1)
	s := f.source(t, func(r *http.Request) (*http.Response, error) {
		if r.URL.Path == "/v3/bill/tradebill" {
			return f.response(t, metadataFor(t, raw, downloadURL), 200), nil
		}
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(raw))}, nil
	})
	count := 0
	summary, err := s.Fetch(t.Context(), "wechat", "account", fixtureDay, func(bill.Entry) error { count++; return nil })
	if err == nil || count != 2 || summary != (bill.Summary{}) {
		t.Fatalf("invalid tail became publishable: emitted=%d summary=%+v err=%v", count, summary, err)
	}
}

type atomicCloseBody struct {
	io.Reader
	closes atomic.Int32
}

func (b *atomicCloseBody) Close() error { b.closes.Add(1); return nil }
func TestClosingBodyConcurrentReadClose(t *testing.T) {
	t.Parallel()
	original := &atomicCloseBody{Reader: strings.NewReader(strings.Repeat("x", 4096))}
	body := &closingBody{ReadCloser: original}
	var wg sync.WaitGroup
	wg.Go(func() {
		if _, err := io.Copy(io.Discard, body); err != nil {
			t.Error(err)
		}
	})
	for range 20 {
		wg.Go(func() {
			if err := body.Close(); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if original.closes.Load() != 1 {
		t.Fatalf("original body closed %d times", original.closes.Load())
	}
}
