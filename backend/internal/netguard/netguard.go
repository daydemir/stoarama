// Package netguard validates user-supplied stream URLs before they reach
// ffmpeg/ffprobe, to defend against SSRF (server-side request forgery). It
// rejects non-http(s) schemes, embedded credentials, and any host that resolves
// to a loopback, link-local (incl. the cloud metadata IP 169.254.169.254),
// RFC1918/ULA private, CGNAT, multicast, or unspecified address. Resolution is
// done here and re-checked at fetch time to mitigate DNS rebinding.
package netguard

import (
	"fmt"
	"net"
	"net/url"
	"strings"
)

// ValidatePublicURL parses rawURL, requires an http/https scheme with no
// userinfo, resolves the host, and returns the first resolved IP only if EVERY
// resolved address is publicly routable. Any private/loopback/link-local/etc.
// address (or an unresolvable host) yields an error and no IP.
func ValidatePublicURL(rawURL string) (net.IP, error) {
	raw := strings.TrimSpace(rawURL)
	if raw == "" {
		return nil, fmt.Errorf("stream url is empty")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("parse stream url: %w", err)
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme != "http" && scheme != "https" {
		return nil, fmt.Errorf("stream url scheme must be http or https, got %q", u.Scheme)
	}
	if u.User != nil {
		return nil, fmt.Errorf("stream url must not contain embedded credentials")
	}
	host := u.Hostname()
	if host == "" {
		return nil, fmt.Errorf("stream url has no host")
	}

	ips, err := net.LookupIP(host)
	if err != nil {
		return nil, fmt.Errorf("resolve stream host %q: %w", host, err)
	}
	if len(ips) == 0 {
		return nil, fmt.Errorf("stream host %q did not resolve to any address", host)
	}
	for _, ip := range ips {
		if !isPublicIP(ip) {
			return nil, fmt.Errorf("stream host %q resolves to a disallowed address %s", host, ip.String())
		}
	}
	return ips[0], nil
}

// PinnedURL rewrites rawURL so its host is the literal validated IP, and returns
// the original Host value (hostname plus any explicit port) separately. ffmpeg is
// then given the IP-literal URL (so the TCP socket is pinned to the address that
// was validated and cannot be redirected by a DNS rebind at connect time) while
// the returned host string is passed back as the HTTP Host header / TLS SNI so
// virtual-hosted and SNI-dependent origins still route correctly. ip must be the
// address returned by ValidatePublicURL for rawURL.
func PinnedURL(rawURL string, ip net.IP) (pinnedURL string, host string, err error) {
	if ip == nil {
		return "", "", fmt.Errorf("pin ip is nil")
	}
	u, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return "", "", fmt.Errorf("parse stream url: %w", err)
	}
	originalHost := u.Host
	if originalHost == "" {
		return "", "", fmt.Errorf("stream url has no host")
	}
	literal := ip.String()
	if ip.To4() == nil {
		literal = "[" + literal + "]"
	}
	if port := u.Port(); port != "" {
		u.Host = literal + ":" + port
	} else {
		u.Host = literal
	}
	return u.String(), originalHost, nil
}

// cgnatNet is the RFC6598 carrier-grade NAT range 100.64.0.0/10.
var cgnatNet = mustCIDR("100.64.0.0/10")

// ulaNet is the IPv6 unique-local range fc00::/7.
var ulaNet = mustCIDR("fc00::/7")

func mustCIDR(s string) *net.IPNet {
	_, n, err := net.ParseCIDR(s)
	if err != nil {
		panic(fmt.Sprintf("netguard: bad CIDR %q: %v", s, err))
	}
	return n
}

// isPublicIP reports whether ip is a globally routable unicast address safe to
// fetch from. It rejects loopback, link-local (incl. 169.254.169.254), private
// (RFC1918/ULA), CGNAT, multicast, interface-local, and unspecified addresses.
func isPublicIP(ip net.IP) bool {
	if ip == nil {
		return false
	}
	if ip.IsLoopback() ||
		ip.IsUnspecified() ||
		ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast() ||
		ip.IsInterfaceLocalMulticast() ||
		ip.IsMulticast() ||
		ip.IsPrivate() {
		return false
	}
	if cgnatNet.Contains(ip) || ulaNet.Contains(ip) {
		return false
	}
	return true
}
