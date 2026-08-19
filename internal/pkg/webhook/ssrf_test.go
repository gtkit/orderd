package webhook

import (
	"net"
	"testing"
)

// TestIPAllowedMetadataAndCGNAT 反证 ssrf-metadata-guard:CGNAT 默认拒/白名单放行、
// 云元数据 IP 恒拒(即便白名单)、公网放行、既有私网/链路本地/环回回归。
func TestIPAllowedMetadataAndCGNAT(t *testing.T) {
	plain, _ := NewSSRFGuard(nil, nil)
	// 把 CGNAT 段与元数据 IP 都加入白名单,验证元数据仍恒拒、CGNAT 才放行。
	allowCGNAT, _ := NewSSRFGuard(nil, []string{"100.64.0.0/10"})

	cases := []struct {
		name       string
		ip         string
		plainAllow bool // 无白名单时
		cgnatAllow bool // CGNAT 段在白名单时
	}{
		{"公网单播", "8.8.8.8", true, true},
		{"CGNAT", "100.64.1.2", false, true},
		{"阿里云元数据(CGNAT外形但恒拒)", "100.100.100.200", false, false},
		{"AWS元数据 link-local", "169.254.169.254", false, false},
		{"RFC1918 私网", "10.1.2.3", false, false},
		{"环回", "127.0.0.1", false, false},
		{"链路本地", "169.254.1.1", false, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ip := net.ParseIP(c.ip)
			if got := plain.ipAllowed(ip); got != c.plainAllow {
				t.Fatalf("plain ipAllowed(%s)=%v want %v", c.ip, got, c.plainAllow)
			}
			if got := allowCGNAT.ipAllowed(ip); got != c.cgnatAllow {
				t.Fatalf("allowCGNAT ipAllowed(%s)=%v want %v(元数据即便白名单也须恒拒)", c.ip, got, c.cgnatAllow)
			}
		})
	}
}
