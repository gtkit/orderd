package webhook

import (
	"context"
	"fmt"
	"net"
)

// SSRFGuard 出口 SSRF 防护(DESIGN §12:allowlist,兼容白名单内私网,而非无条件拒私网)。
//
// 策略:链路本地/元数据地址(169.254/fe80)始终拒;环回/私网仅当命中 AllowHosts(按主机名)
// 或 AllowCIDRs(按解析 IP)时放行;公网单播默认放行。校验在【拨号时】对实际解析 IP 执行,
// 防 DNS rebinding(校验后重新解析到内网)。
type SSRFGuard struct {
	allowHosts map[string]struct{}
	allowCIDRs []*net.IPNet
}

// NewSSRFGuard 构造守卫。hosts 为完全信任的主机名(绕过 IP 策略,允许 http);cidrs 为放行的私网段。
func NewSSRFGuard(hosts, cidrs []string) (*SSRFGuard, error) {
	g := &SSRFGuard{allowHosts: make(map[string]struct{}, len(hosts))}
	for _, h := range hosts {
		g.allowHosts[h] = struct{}{}
	}
	for _, c := range cidrs {
		_, n, err := net.ParseCIDR(c)
		if err != nil {
			return nil, fmt.Errorf("webhook: 非法 CIDR %q: %w", c, err)
		}
		g.allowCIDRs = append(g.allowCIDRs, n)
	}
	return g, nil
}

func (g *SSRFGuard) hostAllowed(host string) bool {
	_, ok := g.allowHosts[host]
	return ok
}

func (g *SSRFGuard) inAllowCIDR(ip net.IP) bool {
	for _, n := range g.allowCIDRs {
		if n.Contains(ip) {
			return true
		}
	}
	return false
}

// cgnatCIDR 是 RFC6598 共享地址段(100.64.0.0/10):Go 的 IsPrivate() 不覆盖它,
// 但云厂商常把内部 LB/k8s service 网段放这里,需视同私网(仅白名单放行)。
var cgnatCIDR = func() *net.IPNet { _, n, _ := net.ParseCIDR("100.64.0.0/10"); return n }()

// metadataIPs 是恒拒的云实例元数据地址(可取临时凭证):169.254.169.254(多数云,亦属链路本地)、
// 100.100.100.200(阿里云,非链路本地非私网,IsPrivate 漏判)、fd00:ec2::254(AWS IPv6)。
// 恒拒:即便落在白名单段内也不放行。
var metadataIPs = []net.IP{
	net.ParseIP("169.254.169.254"),
	net.ParseIP("100.100.100.200"),
	net.ParseIP("fd00:ec2::254"),
}

// ipAllowed 判定单个 IP 是否放行(不含 host 白名单豁免)。
func (g *SSRFGuard) ipAllowed(ip net.IP) bool {
	// 云元数据地址恒拒(优先于任何白名单)。
	for _, m := range metadataIPs {
		if m != nil && ip.Equal(m) {
			return false
		}
	}
	// 链路本地(含元数据 169.254.169.254)始终拒。
	if ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() {
		return false
	}
	if ip.IsUnspecified() || ip.IsMulticast() {
		return false
	}
	// 环回/RFC1918 私网 + RFC6598 CGNAT:仅白名单段放行。
	if ip.IsLoopback() || ip.IsPrivate() || cgnatCIDR.Contains(ip) {
		return g.inAllowCIDR(ip)
	}
	return true // 公网单播放行
}

// dialContext 返回校验过解析 IP 的拨号函数(注入 http.Transport)。
func (g *SSRFGuard) dialContext(base *net.Dialer) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, err
		}
		if g.hostAllowed(host) {
			return base.DialContext(ctx, network, addr) // 显式信任主机名
		}
		ips, err := net.DefaultResolver.LookupIP(ctx, "ip", host)
		if err != nil {
			return nil, fmt.Errorf("webhook: 解析 %q 失败: %w", host, err)
		}
		for _, ip := range ips {
			if g.ipAllowed(ip) {
				// 拨【已校验的 IP】,不再让底层重新解析(防 rebinding)。
				return base.DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
			}
		}
		return nil, fmt.Errorf("webhook: %q 解析到的地址被 SSRF 策略拒绝", host)
	}
}
