// Package alipay 获取直连商户人民币 trade 日账单。只接受明确支持的中文模板，
// 下载及解析失败均不返回可发布的账单证明。
package alipay

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	alipayconfig "github.com/gtkit/go-pay/alipay"
	"github.com/gtkit/orderd/internal/shared/bill"
	"github.com/gtkit/orderd/internal/shared/bill/billio"
	sdk "github.com/smartwalle/alipay/v3"
)

var (
	ErrFetch       = errors.New("alipay bill: authenticated statement unavailable")
	ErrInvalidBill = errors.New("alipay bill: invalid or unsupported statement")
)

var channelLocation = time.FixedZone("GMT+8", 8*60*60)

const downloadHost = "dwbillcenter.alipay.com"

// Config 复用支付账号的直连凭据；值优先于对应路径。SellerID 和 PayAccountID 必填。
// IsProduction=false 使用 SDK 沙箱网关，但下载仍须满足已验证的正式下载域名规则。
// 证书三组须全填或全空；全空时 AlipayPublicKey 必填。不支持服务商授权及二级商户。
type Config struct {
	PayAccountID            string
	SellerID                string
	AppID                   string
	PrivateKey              string
	PrivateKeyPath          string
	IsProduction            bool
	AppCertPublicKey        string
	AppCertPublicKeyPath    string
	AlipayRootCert          string
	AlipayRootCertPath      string
	AlipayCertPublicKey     string
	AlipayCertPublicKeyPath string
	AlipayPublicKey         string
}

// String 防止常规诊断误打印凭据；不得把 Config 作为结构化日志或 JSON 记录。
func (Config) String() string { return "alipay bill config (redacted)" }

type requester interface {
	Request(context.Context, sdk.Param, any) error
}

// Source 构造后只读，可并发 Fetch；emit 同步执行，只有 Fetch 成功后才能发布已暂存条目。
type Source struct {
	api        requester
	downloader *http.Client
	accountID  string
	sellerID   string
}

type downloadRequest struct {
	sdk.BillDownloadURLQuery
	Secure string `json:"secure"`
}

type downloadResponse struct {
	sdk.Error
	BillDownloadURL string `json:"bill_download_url"`
	BillFileCode    string `json:"bill_file_code"`
}

// New 使用已锁定 SDK 构造源，不调用渠道接口。
func New(cfg Config) (*Source, error) {
	if cfg.PayAccountID == "" || cfg.SellerID == "" {
		return nil, fmt.Errorf("alipay bill: account and seller identity required")
	}
	pc := alipayconfig.Config{
		AppID: cfg.AppID, PrivateKey: cfg.PrivateKey, PrivateKeyPath: cfg.PrivateKeyPath,
		IsProduction: cfg.IsProduction, AppCertPublicKey: cfg.AppCertPublicKey,
		AppCertPublicKeyPath: cfg.AppCertPublicKeyPath, AlipayRootCert: cfg.AlipayRootCert,
		AlipayRootCertPath: cfg.AlipayRootCertPath, AlipayCertPublicKey: cfg.AlipayCertPublicKey,
		AlipayCertPublicKeyPath: cfg.AlipayCertPublicKeyPath, AlipayPublicKey: cfg.AlipayPublicKey,
	}
	if err := pc.Validate(); err != nil {
		return nil, fmt.Errorf("alipay bill: invalid credentials configuration")
	}
	key, err := sourceValue(cfg.PrivateKey, cfg.PrivateKeyPath)
	if err != nil {
		return nil, err
	}
	host := "openapi.alipay.com"
	if !cfg.IsProduction {
		host = "openapi-sandbox.dl.alipaydev.com"
	}
	client, err := sdk.New(cfg.AppID, key, cfg.IsProduction,
		sdk.WithTimeLocation(channelLocation), sdk.WithHTTPClient(billio.NewHTTPClient([]string{host}, func(*http.Request) int64 { return billio.MaxMetadataBytes })))
	if err != nil {
		return nil, fmt.Errorf("alipay bill: invalid application private key")
	}
	if pc.UseCertMode() {
		for _, cert := range []struct {
			value, path string
			load        func(string) error
		}{
			{cfg.AppCertPublicKey, cfg.AppCertPublicKeyPath, client.LoadAppCertPublicKey},
			{cfg.AlipayRootCert, cfg.AlipayRootCertPath, client.LoadAliPayRootCert},
			{cfg.AlipayCertPublicKey, cfg.AlipayCertPublicKeyPath, client.LoadAlipayCertPublicKey},
		} {
			value, err := sourceValue(cert.value, cert.path)
			if err != nil {
				return nil, err
			}
			if err := cert.load(value); err != nil {
				return nil, fmt.Errorf("alipay bill: invalid verification certificate")
			}
		}
	} else if err := client.LoadAliPayPublicKey(cfg.AlipayPublicKey); err != nil {
		return nil, fmt.Errorf("alipay bill: invalid platform public key")
	}
	return &Source{api: client, accountID: cfg.PayAccountID, sellerID: cfg.SellerID,
		downloader: billio.NewHTTPClient([]string{downloadHost}, func(*http.Request) int64 { return billio.MaxDownloadBytes })}, nil
}

func sourceValue(value, path string) (string, error) {
	if value != "" {
		return value, nil
	}
	// Credential paths come exclusively from trusted operator configuration, never bill/API input.
	data, err := os.ReadFile(path) // #nosec G304 -- operator-selected credential file.
	if err != nil {
		return "", fmt.Errorf("alipay bill: cannot read credential source")
	}
	return string(data), nil
}

// Fetch 获取一个东八区自然日账单。无账单/尚未生成和未知格式均返回错误，不能视为空账单。
func (s *Source) Fetch(ctx context.Context, channel, accountID string, day time.Time, emit func(bill.Entry) error) (bill.Summary, error) {
	if channel != "alipay" || accountID != s.accountID || emit == nil || day.IsZero() {
		return bill.Summary{}, ErrFetch
	}
	day = day.In(channelLocation)
	start := time.Date(day.Year(), day.Month(), day.Day(), 0, 0, 0, 0, channelLocation)
	now := time.Now().In(channelLocation)
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, channelLocation)
	if !start.Before(today) || start.Before(today.AddDate(-6, 0, 0)) {
		return bill.Summary{}, fmt.Errorf("%w: unsupported bill date", ErrFetch)
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var rsp *downloadResponse
	err := s.api.Request(ctx, downloadRequest{BillDownloadURLQuery: sdk.BillDownloadURLQuery{BillType: "trade", BillDate: start.Format(time.DateOnly)}, Secure: "true"}, &rsp)
	if err != nil {
		return bill.Summary{}, fetchError(ctx)
	}
	if rsp == nil || !rsp.IsSuccess() || rsp.BillDownloadURL == "" {
		return bill.Summary{}, ErrFetch
	}
	if rsp.BillFileCode != "" && rsp.BillFileCode != "EMPTY_DATA_WITH_BILL_FILE" {
		return bill.Summary{}, ErrFetch
	}
	if err := billio.ValidateURL(rsp.BillDownloadURL, []string{downloadHost}); err != nil {
		return bill.Summary{}, ErrFetch
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rsp.BillDownloadURL, nil)
	if err != nil {
		return bill.Summary{}, ErrFetch
	}
	resp, err := s.downloader.Do(req)
	if err != nil {
		return bill.Summary{}, fetchError(ctx)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return bill.Summary{}, ErrFetch
	}
	digest := sha256.New()
	f, size, err := billio.Spool(ctx, io.TeeReader(resp.Body, digest), billio.MaxDownloadBytes)
	if err != nil {
		return bill.Summary{}, fetchError(ctx)
	}
	defer func() { _ = f.Close(); _ = os.Remove(f.Name()) }()
	summary, err := parseArchive(ctx, f, size, s.sellerID, start, emit)
	if err != nil {
		return bill.Summary{}, err
	}
	if rsp.BillFileCode == "EMPTY_DATA_WITH_BILL_FILE" && summary.Count != 0 {
		return bill.Summary{}, ErrInvalidBill
	}
	// This identifies the downloaded bytes for audit; it is not a channel file signature.
	summary.DigestAlgorithm = "SHA256"
	summary.Digest = hex.EncodeToString(digest.Sum(nil))
	return summary, nil
}

func fetchError(ctx context.Context) error {
	// SDK/HTTP errors may contain the signed download URL. Keep only safe classifications.
	if err := ctx.Err(); err != nil {
		return errors.Join(ErrFetch, err)
	}
	return ErrFetch
}
