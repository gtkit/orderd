package billio

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/netip"
	"os"
	"strings"
	"testing"
)

func TestAmounts(t *testing.T) {
	for _, tt := range []struct {
		s    string
		want int64
		bad  bool
	}{
		{"0", 0, false}, {"0.01", 1, false}, {"10995116277.76", 1 << 40, false},
		{"92233720368547758.07", math.MaxInt64, false}, {"-92233720368547758.08", math.MinInt64, false},
		{"92233720368547758.08", 0, true}, {"-92233720368547758.09", 0, true}, {"1.001", 0, true}, {"1e2", 0, true},
		{"+1", 0, true}, {"1.", 0, true}, {".01", 0, true}, {" 1", 0, true}, {"1.2.3", 0, true}, {"NaN", 0, true}, {"", 0, true},
	} {
		t.Run(tt.s, func(t *testing.T) {
			v, err := ParseAmount(tt.s)
			if (err != nil) != tt.bad || (!tt.bad && v != tt.want) {
				t.Fatalf("got %d %v", v, err)
			}
		})
	}
	for _, tt := range []struct {
		a, b int64
		bad  bool
	}{{math.MaxInt64, 1, true}, {math.MinInt64, -1, true}, {math.MaxInt64, math.MinInt64, false}, {0, math.MinInt64, false}, {math.MaxInt64 - 1, 1, false}} {
		_, err := AddAmount(tt.a, tt.b)
		if (err != nil) != tt.bad {
			t.Fatalf("%d + %d: %v", tt.a, tt.b, err)
		}
	}
}

func FuzzAmountRoundTrip(f *testing.F) {
	for _, n := range []int64{0, 1, -1, math.MaxInt64, math.MinInt64, 1 << 40} {
		f.Add(n)
	}
	f.Fuzz(func(t *testing.T, n int64) {
		// Format without absolute-value overflow for MinInt64.
		whole, frac := n/100, n%100
		if frac < 0 {
			frac = -frac
		}
		sign := ""
		if n < 0 && whole == 0 {
			sign = "-"
		}
		s := fmt.Sprintf("%s%d.%02d", sign, whole, frac)
		got, err := ParseAmount(s)
		if err != nil || got != n {
			t.Fatalf("%q: %d %v", s, got, err)
		}
	})
}

func TestURLAndDestinationValidation(t *testing.T) {
	for _, raw := range []string{"https://api.mch.weixin.qq.com/v3/bill?token=secret", "https://api.mch.weixin.qq.com:443/v3/bill"} {
		if err := ValidateURL(raw, []string{"api.mch.weixin.qq.com"}); err != nil {
			t.Fatal(err)
		}
	}
	for _, raw := range []string{"http://api.mch.weixin.qq.com/a", "https://api.mch.weixin.qq.com.evil/a", "https://user:secret@api.mch.weixin.qq.com/a", "https://api.mch.weixin.qq.com:444/a", "https://api.mch.weixin.qq.com/a#secret", "https://api.mch.weixin.qq.com./a", "https://127.0.0.1/a", "https://[::1]/a"} {
		if err := ValidateURL(raw, []string{"api.mch.weixin.qq.com", "127.0.0.1", "::1"}); err == nil {
			t.Fatalf("accepted %s", raw)
		}
	}
	for _, s := range []string{"127.0.0.1", "10.1.1.1", "169.254.169.254", "100.100.100.200", "192.168.1.1", "198.18.0.1", "192.0.2.1", "0.1.2.3", "240.0.0.1", "::1", "fc00::1", "fe80::1", "::ffff:127.0.0.1", "2001:db8::1", "64:ff9b::7f00:1", "fec0::1", "::127.0.0.1", "100::1", "100:0:0:1::1", "3fff::1", "5f00::1", "64:ff9b:1::7f00:1", "2606:4700:4700::1111%en0"} {
		if publicAddress(netip.MustParseAddr(s)) {
			t.Fatalf("private/reserved IP accepted: %s", s)
		}
	}
	if !publicAddress(netip.MustParseAddr("1.1.1.1")) || !publicAddress(netip.MustParseAddr("2606:4700:4700::1111")) {
		t.Fatal("public IP rejected")
	}
	if _, err := dialPublic(t.Context(), "tcp", "127.0.0.1:443"); err == nil {
		t.Fatal("private dial accepted")
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type closeReader struct {
	io.Reader
	closed bool
}

func (r *closeReader) Close() error { r.closed = true; return nil }
func TestGuardLimitsBeforeSDKReads(t *testing.T) {
	for _, tt := range []struct {
		name, body, encoding string
		length               int64
		bad                  bool
	}{
		{"exact", "1234", "", 4, false}, {"chunked excess", "12345", "", -1, true}, {"declared excess", "12345", "", 5, true}, {"unexpected compression", "x", "gzip", 1, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			body := &closeReader{Reader: strings.NewReader(tt.body)}
			transport := &guardedTransport{hosts: []string{"api.mch.weixin.qq.com"}, limit: func(*http.Request) int64 { return 4 }, base: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				if req.Header.Get("Accept-Encoding") != "identity" {
					t.Fatal("transparent compression not disabled")
				}
				return &http.Response{StatusCode: 200, Body: body, ContentLength: tt.length, Header: http.Header{"Content-Encoding": []string{tt.encoding}}}, nil
			})}
			req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://api.mch.weixin.qq.com/v3/bill", nil)
			rsp, err := transport.RoundTrip(req)
			if err == nil {
				_, err = io.ReadAll(rsp.Body)
				_ = rsp.Body.Close()
			}
			if tt.bad && !errors.Is(err, ErrLimit) {
				t.Fatalf("want limit, got %v", err)
			}
			if !tt.bad && err != nil {
				t.Fatal(err)
			}
			if !body.closed {
				t.Fatal("body not closed")
			}
		})
	}
	client := NewHTTPClient([]string{"api.mch.weixin.qq.com"}, nil)
	if err := client.CheckRedirect(nil, nil); err == nil {
		t.Fatal("redirect allowed")
	}
}

func TestSpoolResourceLifecycle(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())
	f, n, err := Spool(t.Context(), strings.NewReader("1234"), 4)
	if err != nil {
		t.Fatal(err)
	}
	name := f.Name()
	t.Cleanup(func() { _ = f.Close(); _ = os.Remove(name) })
	info, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if n != 4 || info.Mode().Perm() != 0600 {
		t.Fatalf("size=%d mode=%s", n, info.Mode())
	}
	got, err := io.ReadAll(f)
	if err != nil || string(got) != "1234" {
		t.Fatalf("not rewound: %q %v", got, err)
	}
	_ = f.Close()
	_ = os.Remove(name)
	if f, _, err = Spool(t.Context(), strings.NewReader("12345"), 4); !errors.Is(err, ErrLimit) || f != nil {
		t.Fatalf("limit: %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, _, err := Spool(ctx, strings.NewReader("a"), 4); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
	files, err := os.ReadDir(os.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 0 {
		t.Fatalf("temporary files leaked: %v", files)
	}
}
