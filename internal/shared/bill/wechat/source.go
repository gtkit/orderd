// Package wechat implements the ordinary-merchant APIv3 ALL bill contract.
package wechat

import (
	"context"
	"crypto/rsa"
	"crypto/sha1" // #nosec G505 -- SHA1 is required by the signed WeChat bill metadata protocol, not used for authentication.
	"crypto/subtle"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/gtkit/orderd/internal/shared/bill"
	"github.com/gtkit/orderd/internal/shared/bill/billio"
	"github.com/wechatpay-apiv3/wechatpay-go/core"
	"github.com/wechatpay-apiv3/wechatpay-go/core/auth/validators"
	"github.com/wechatpay-apiv3/wechatpay-go/core/option"
	"github.com/wechatpay-apiv3/wechatpay-go/utils"
)

const applyURL = "https://api.mch.weixin.qq.com/v3/bill/tradebill"

var hosts = []string{"api.mch.weixin.qq.com", "api2.mch.weixin.qq.com"}

// Config reuses an existing ordinary-merchant account's credentials. No new keys
// or auto-refresh workers are created. Inline PEM takes precedence over its path.
type Config struct {
	PayAccountID             string // Required routing identity; must equal Fetch/Verify's account argument.
	AppID                    string // Required configured application; file AppIDs are preserved for later ownership checks.
	MchID                    string // Required ordinary-merchant identity; every file row must match.
	MchCertSerialNumber      string // Required merchant signing certificate serial number.
	MchPrivateKeyPath        string // Merchant RSA private-key path, used when MchPrivateKeyPEM is empty.
	MchPrivateKeyPEM         string // Merchant RSA private-key PEM; empty falls back to MchPrivateKeyPath.
	WechatPayCertificatePath string // Static platform certificate path, used in certificate mode if PEM is empty.
	WechatPayCertificatePEM  string // Static platform certificate PEM; used when public-key mode is absent.
	WechatPayPublicKeyID     string // Public-key mode requires both this ID and a public-key source.
	WechatPayPublicKeyPath   string // Platform RSA public-key path, used when its PEM is empty.
	WechatPayPublicKeyPEM    string // Platform RSA public-key PEM; empty falls back to its path.
}

// Source signs both requests, validates metadata/query responses, and authenticates
// the unsigned file response using the digest in verified metadata. It is immutable after construction.
type Source struct {
	client    *core.Client
	download  *core.Client
	accountID string
	mchID     string
	appID     string
	now       func() time.Time
}

var _ bill.Source = (*Source)(nil)

// New constructs clients with static verification material and performs no network requests.
func New(ctx context.Context, cfg Config) (*Source, error) {
	client := billio.NewHTTPClient(hosts, func(r *http.Request) int64 {
		if downloadPath(r.URL.Path) {
			return billio.MaxDownloadBytes
		}
		return billio.MaxMetadataBytes
	})
	return newSource(ctx, cfg, client)
}

func newSource(ctx context.Context, cfg Config, client *http.Client) (*Source, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if cfg.PayAccountID == "" || cfg.AppID == "" || cfg.MchID == "" || cfg.MchCertSerialNumber == "" {
		return nil, errors.New("wechat bill: incomplete account identity")
	}
	var key *rsa.PrivateKey
	var err error
	if cfg.MchPrivateKeyPEM != "" {
		key, err = utils.LoadPrivateKey(cfg.MchPrivateKeyPEM)
	} else {
		key, err = utils.LoadPrivateKeyWithPath(cfg.MchPrivateKeyPath)
	}
	if err != nil {
		return nil, errors.New("wechat bill: invalid merchant private key")
	}
	opts := []core.ClientOption{option.WithMerchantCredential(cfg.MchID, cfg.MchCertSerialNumber, key)}
	publicSource := cfg.WechatPayPublicKeyPEM != "" || cfg.WechatPayPublicKeyPath != ""
	if (cfg.WechatPayPublicKeyID != "") != publicSource {
		return nil, errors.New("wechat bill: incomplete platform public key configuration")
	}
	if publicSource {
		var publicKey *rsa.PublicKey
		if cfg.WechatPayPublicKeyPEM != "" {
			publicKey, err = utils.LoadPublicKey(cfg.WechatPayPublicKeyPEM)
		} else {
			publicKey, err = utils.LoadPublicKeyWithPath(cfg.WechatPayPublicKeyPath)
		}
		if err != nil {
			return nil, errors.New("wechat bill: invalid platform public key")
		}
		opts = append(opts, option.WithWechatPayPublicKeyAuthCipher(cfg.MchID, cfg.MchCertSerialNumber, key, cfg.WechatPayPublicKeyID, publicKey))
	} else {
		var cert *x509.Certificate
		if cfg.WechatPayCertificatePEM != "" {
			cert, err = utils.LoadCertificate(cfg.WechatPayCertificatePEM)
		} else {
			cert, err = utils.LoadCertificateWithPath(cfg.WechatPayCertificatePath)
		}
		if err != nil {
			return nil, errors.New("wechat bill: invalid platform certificate")
		}
		if _, ok := cert.PublicKey.(*rsa.PublicKey); !ok {
			return nil, errors.New("wechat bill: platform certificate must contain an RSA key")
		}
		opts = append(opts, option.WithWechatPayCertificate([]*x509.Certificate{cert}))
	}
	copied := *client
	copied.Transport = acceptTransport{base: client.Transport}
	opts = append(opts, option.WithHTTPClient(&copied))
	sdk, err := core.NewClient(ctx, opts...)
	if err != nil {
		return nil, errors.New("wechat bill: cannot initialize signed client")
	}
	return &Source{client: sdk, download: core.NewClientWithValidator(sdk, &validators.NullValidator{}), accountID: cfg.PayAccountID, mchID: cfg.MchID, appID: cfg.AppID, now: time.Now}, nil
}

type acceptTransport struct{ base http.RoundTripper }

func (t acceptTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	copy := r.Clone(r.Context())
	if !downloadPath(r.URL.Path) {
		copy.Header.Set("Accept", "application/json")
	}
	response, err := t.base.RoundTrip(copy)
	if err == nil && response != nil && response.Body != nil {
		// The SDK replaces metadata/query bodies after ReadAll without closing
		// the original body. Close on EOF/error before that replacement occurs.
		response.Body = &closingBody{ReadCloser: response.Body}
	}
	return response, err
}

type closingBody struct {
	io.ReadCloser
	closeOnce sync.Once
	closeErr  error
}

func (b *closingBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err != nil {
		_ = b.Close()
	}
	return n, err
}
func (b *closingBody) Close() error {
	b.closeOnce.Do(func() { b.closeErr = b.ReadCloser.Close() })
	return b.closeErr
}

func downloadPath(path string) bool {
	return path == "/v3/billdownload/file" || path == "/v3/bill/downloadurl"
}

func closeResult(r *core.APIResult) {
	if r != nil && r.Response != nil && r.Response.Body != nil {
		_ = r.Response.Body.Close()
	}
}

// requestError keeps SDK URL tokens, response bodies and headers out of persisted errors.
func requestError(ctx context.Context, operation string, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if errors.Is(err, billio.ErrLimit) {
		return fmt.Errorf("wechat bill: %s: %w", operation, billio.ErrLimit)
	}
	return fmt.Errorf("wechat bill: %s failed", operation)
}

// Fetch streams verified ALL records. An emitted row remains provisional until
// the final Summary succeeds, including all six financial tail totals.
func (s *Source) Fetch(ctx context.Context, channel, account string, day time.Time, emit func(bill.Entry) error) (bill.Summary, error) {
	if channel != "wechat" || account != s.accountID || emit == nil {
		return bill.Summary{}, errors.New("wechat bill: invalid fetch scope")
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	day = midnight(day)
	today := midnight(s.now())
	if !day.Before(today) || day.Before(today.AddDate(0, -3, 0)) {
		return bill.Summary{}, errors.New("wechat bill: date outside supported three-month history")
	}
	query := url.Values{"bill_date": {day.Format(time.DateOnly)}, "bill_type": {"ALL"}}
	result, err := s.client.Get(ctx, applyURL+"?"+query.Encode())
	defer closeResult(result)
	if err != nil {
		if ctx.Err() != nil {
			return bill.Summary{}, ctx.Err()
		}
		if apiErr, ok := errors.AsType[*core.APIError](err); ok && apiErr.Code == "NO_STATEMENT_EXIST" {
			return bill.Summary{NoStatement: true}, nil
		}
		return bill.Summary{}, requestError(ctx, "apply", err)
	}
	var metadata struct {
		HashType    string `json:"hash_type"`
		HashValue   string `json:"hash_value"`
		DownloadURL string `json:"download_url"`
	}
	decoder := json.NewDecoder(result.Response.Body)
	if err := decoder.Decode(&metadata); err != nil {
		return bill.Summary{}, errors.New("wechat bill: invalid metadata JSON")
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return bill.Summary{}, errors.New("wechat bill: trailing metadata JSON")
	}
	expected, err := hex.DecodeString(metadata.HashValue)
	if err != nil || metadata.HashType != "SHA1" || len(expected) != sha1.Size {
		return bill.Summary{}, errors.New("wechat bill: unsupported or invalid digest")
	}
	if err := billio.ValidateURL(metadata.DownloadURL, hosts); err != nil {
		return bill.Summary{}, err
	}
	parsed, _ := url.Parse(metadata.DownloadURL) // ValidateURL already parsed and validated it.
	if !downloadPath(parsed.Path) || parsed.RawPath != "" {
		return bill.Summary{}, errors.New("wechat bill: unsupported download path")
	}
	downloaded, err := s.download.Get(ctx, metadata.DownloadURL)
	defer closeResult(downloaded)
	if err != nil {
		return bill.Summary{}, requestError(ctx, "download", err)
	}
	if encoding := downloaded.Response.Header.Get("Content-Encoding"); encoding != "" && encoding != "identity" {
		return bill.Summary{}, errors.New("wechat bill: unexpected compression")
	}
	digest := sha1.New() // #nosec G401 -- Official file digest algorithm; metadata itself is RSA authenticated.
	file, _, err := billio.Spool(ctx, io.TeeReader(downloaded.Response.Body, digest), billio.MaxDownloadBytes)
	if err != nil {
		return bill.Summary{}, requestError(ctx, "read file", err)
	}
	defer func() { _ = file.Close(); _ = os.Remove(file.Name()) }()
	if subtle.ConstantTimeCompare(expected, digest.Sum(nil)) != 1 {
		return bill.Summary{}, errors.New("wechat bill: file digest mismatch")
	}
	summary, err := parse(ctx, file, s.mchID, day, emit, billio.MaxEntries)
	if err != nil {
		return bill.Summary{}, err
	}
	summary.DigestAlgorithm = "SHA1"
	summary.Digest = hex.EncodeToString(expected)
	return summary, nil
}

func midnight(t time.Time) time.Time {
	t = t.In(bill.Location)
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, bill.Location)
}
