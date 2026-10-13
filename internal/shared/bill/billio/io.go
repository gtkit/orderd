// Package billio 提供渠道账单的有界网络、临时文件和精确金额能力。
package billio

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"
)

const (
	MaxMetadataBytes int64 = 64 << 10
	MaxDownloadBytes int64 = 64 << 20
	MaxExpandedBytes int64 = 256 << 20
	MaxLineBytes           = 1 << 20
	MaxEntries       int64 = 1_000_000
)

// ErrLimit 表示硬限额被突破，不能把它当合法 EOF。
var ErrLimit = errors.New("bill: resource limit exceeded")

// ParseAmount 解析元为分，接受可选负号和一至两位小数；不舍入、不接受指数或空白。
func ParseAmount(s string) (int64, error) {
	if s == "" {
		return 0, errors.New("bill: empty amount")
	}
	negative := s[0] == '-'
	if negative {
		s = s[1:]
	}
	parts := strings.Split(s, ".")
	if len(parts) > 2 || parts[0] == "" {
		return 0, errors.New("bill: invalid amount")
	}
	frac := "00"
	if len(parts) == 2 {
		if len(parts[1]) < 1 || len(parts[1]) > 2 {
			return 0, errors.New("bill: invalid fractional amount")
		}
		frac = parts[1]
		if len(frac) == 1 {
			frac += "0"
		}
	}
	digits := parts[0] + frac
	for _, c := range digits {
		if c < '0' || c > '9' {
			return 0, errors.New("bill: invalid amount character")
		}
	}
	if negative {
		digits = "-" + digits
	}
	v, err := strconv.ParseInt(digits, 10, 64)
	if err != nil {
		return 0, errors.New("bill: amount exceeds int64 cents")
	}
	return v, nil
}

// AddAmount 累计分时显式拒绝上下溢出。
func AddAmount(a, b int64) (int64, error) {
	if (b > 0 && a > math.MaxInt64-b) || (b < 0 && a < math.MinInt64-b) {
		return 0, errors.New("bill: aggregate amount overflow")
	}
	return a + b, nil
}

// ValidateURL 只允许精确列出的 HTTPS 公网主机，不接受端口变体、凭据、fragment 或 IP 字面量。
func ValidateURL(raw string, hosts []string) error {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.User != nil || u.Fragment != "" || u.Opaque != "" || u.Host == "" ||
		(u.Port() != "" && u.Port() != "443") || !slices.Contains(hosts, u.Hostname()) || strings.HasSuffix(u.Hostname(), ".") {
		return errors.New("bill: download URL rejected")
	}
	if _, err := netip.ParseAddr(u.Hostname()); err == nil {
		return errors.New("bill: IP literal rejected")
	}
	return nil
}

// publicAddress 拒绝所有非公网、文档/基准/协议保留地址，包含IPv4映射IPv6。
func publicAddress(addr netip.Addr) bool {
	if addr.Zone() != "" {
		return false
	}
	addr = addr.Unmap()
	if !addr.IsValid() || !addr.IsGlobalUnicast() || addr.IsPrivate() || addr.IsLoopback() || addr.IsLinkLocalUnicast() {
		return false
	}
	if addr.Is6() && !publicIPv6Prefix.Contains(addr) {
		return false
	}
	for _, p := range deniedPrefixes {
		if p.Contains(addr) {
			return false
		}
	}
	return true
}

var publicIPv6Prefix = netip.MustParsePrefix("2000::/3")

var deniedPrefixes = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"), netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("192.0.0.0/24"), netip.MustParsePrefix("192.0.2.0/24"), netip.MustParsePrefix("192.88.99.0/24"),
	netip.MustParsePrefix("198.18.0.0/15"), netip.MustParsePrefix("198.51.100.0/24"), netip.MustParsePrefix("203.0.113.0/24"), netip.MustParsePrefix("240.0.0.0/4"),
	netip.MustParsePrefix("3fff::/20"), netip.MustParsePrefix("2001:db8::/32"), netip.MustParsePrefix("2001::/23"), netip.MustParsePrefix("2002::/16"), netip.MustParsePrefix("64:ff9b::/96"),
}

func dialPublic(ctx context.Context, network, address string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil || port != "443" {
		return nil, errors.New("bill: destination rejected")
	}
	ips, err := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
	if err != nil || len(ips) == 0 {
		return nil, errors.New("bill: DNS resolution failed")
	}
	for _, ip := range ips {
		if !publicAddress(ip) {
			return nil, errors.New("bill: non-public destination rejected")
		}
	}
	dialer := net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}
	for _, ip := range ips {
		conn, err := dialer.DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
		if err == nil {
			return conn, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}
	return nil, errors.New("bill: channel connection failed")
}

type guardedTransport struct {
	base  http.RoundTripper
	hosts []string
	limit func(*http.Request) int64
}

func (t *guardedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if err := ValidateURL(req.URL.String(), t.hosts); err != nil {
		return nil, err
	}
	r := req.Clone(req.Context())
	r.Header.Set("Accept-Encoding", "identity")
	rsp, err := t.base.RoundTrip(r)
	if err != nil {
		if req.Context().Err() != nil {
			return nil, req.Context().Err()
		}
		return nil, errors.New("bill: channel transport failed")
	}
	limit := t.limit(req)
	if limit <= 0 || rsp.Body == nil {
		if rsp.Body != nil {
			_ = rsp.Body.Close()
		}
		return nil, errors.New("bill: invalid response limit or body")
	}
	if rsp.ContentLength > limit || (rsp.Header.Get("Content-Encoding") != "" && rsp.Header.Get("Content-Encoding") != "identity") {
		_ = rsp.Body.Close()
		return nil, ErrLimit
	}
	rsp.Body = &limitedBody{ReadCloser: rsp.Body, remaining: limit}
	return rsp, nil
}

// NewHTTPClient 返回无代理、无重定向、TLS与DNS防护固定开启的渠道客户端。
// responseLimit 在SDK验签器读取Body之前生效，避免SDK内部ReadAll无界。
func NewHTTPClient(hosts []string, responseLimit func(*http.Request) int64) *http.Client {
	if responseLimit == nil {
		responseLimit = func(*http.Request) int64 { return MaxMetadataBytes }
	}
	transport := &http.Transport{
		Proxy: nil, DialContext: dialPublic, TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12},
		TLSHandshakeTimeout: 5 * time.Second, ResponseHeaderTimeout: 10 * time.Second, IdleConnTimeout: 30 * time.Second,
		MaxIdleConns: 4, MaxIdleConnsPerHost: 2, MaxConnsPerHost: 4, DisableCompression: true, MaxResponseHeaderBytes: 32 << 10,
	}
	return &http.Client{Transport: &guardedTransport{base: transport, hosts: slices.Clone(hosts), limit: responseLimit},
		Timeout: 30 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("bill: redirect rejected") }}
}

type limitedBody struct {
	io.ReadCloser
	remaining int64
}

func (b *limitedBody) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if b.remaining == 0 {
		var probe [1]byte
		n, err := b.ReadCloser.Read(probe[:])
		if n > 0 {
			return 0, ErrLimit
		}
		return 0, err
	}
	n, err := b.ReadCloser.Read(p[:min(int64(len(p)), b.remaining)])
	b.remaining -= int64(n)
	return n, err
}

type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}

// Spool 写入0600临时文件并复位到开头。成功后调用方必须 Close 和 Remove；失败自动清理。
func Spool(ctx context.Context, r io.Reader, maxBytes int64) (f *os.File, size int64, err error) {
	if maxBytes <= 0 || maxBytes == math.MaxInt64 {
		return nil, 0, errors.New("bill: invalid spool limit")
	}
	if err = ctx.Err(); err != nil {
		return nil, 0, err
	}
	f, err = os.CreateTemp("", "orderd-bill-*")
	if err != nil {
		return nil, 0, fmt.Errorf("bill: create temporary file: %w", err)
	}
	defer func() {
		if err != nil {
			_ = f.Close()
			_ = os.Remove(f.Name())
			f = nil
		}
	}()
	size, err = io.Copy(f, io.LimitReader(contextReader{ctx: ctx, r: r}, maxBytes+1))
	if err != nil {
		return f, size, err
	}
	if size > maxBytes {
		return f, size, ErrLimit
	}
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		return f, size, err
	}
	return f, size, nil
}
