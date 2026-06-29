package netguard

import (
	"net"
	"testing"
)

func TestValidatePublicURL_RejectsDisallowed(t *testing.T) {
	cases := []struct {
		name string
		url  string
	}{
		{"cloud-metadata-ip", "http://169.254.169.254/latest/meta-data/"},
		{"localhost-ipv4", "http://127.0.0.1:8080/stream.m3u8"},
		{"localhost-ipv6", "http://[::1]/stream.m3u8"},
		{"rfc1918-10", "https://10.0.0.5/live.m3u8"},
		{"rfc1918-192-168", "http://192.168.1.10/live"},
		{"rfc1918-172-16", "http://172.16.0.1/live"},
		{"cgnat-100-64", "http://100.64.1.1/live"},
		{"unspecified", "http://0.0.0.0/live"},
		{"bad-scheme-rtsp", "rtsp://203.0.113.1/live"},
		{"bad-scheme-file", "file:///etc/passwd"},
		{"with-userinfo", "http://user:pass@203.0.113.1/live"},
		{"empty", ""},
		{"no-host", "http:///live"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ip, err := ValidatePublicURL(tc.url)
			if err == nil {
				t.Fatalf("expected error for %q, got ip=%v", tc.url, ip)
			}
		})
	}
}

func TestValidatePublicURL_AllowsPublicLiteral(t *testing.T) {
	// 203.0.113.10 is in TEST-NET-3 (RFC5737) but is a public, routable unicast
	// literal as far as the guard is concerned; using a literal avoids real DNS.
	ip, err := ValidatePublicURL("https://203.0.113.10:443/live.m3u8")
	if err != nil {
		t.Fatalf("expected public literal to pass, got err=%v", err)
	}
	if ip == nil || ip.String() != "203.0.113.10" {
		t.Fatalf("expected resolved ip 203.0.113.10, got %v", ip)
	}
}

// TestIsPublicIP_RebindingContract asserts the predicate the fetch-time re-check
// relies on: a host that (post-validation) flips an A record to a private IP is
// rejected, because EVERY resolved address must be public. We exercise the
// predicate directly since DNS rebinding cannot be reproduced deterministically.
func TestIsPublicIP_RebindingContract(t *testing.T) {
	public := net.ParseIP("203.0.113.10")
	if !isPublicIP(public) {
		t.Fatalf("expected 203.0.113.10 to be public")
	}
	rebindTargets := []string{
		"169.254.169.254", // metadata
		"127.0.0.1",       // loopback
		"10.1.2.3",        // rfc1918
		"192.168.0.1",     // rfc1918
		"100.64.0.1",      // cgnat
		"::1",             // ipv6 loopback
		"fd00::1",         // ipv6 ula
		"fe80::1",         // ipv6 link-local
	}
	for _, s := range rebindTargets {
		ip := net.ParseIP(s)
		if ip == nil {
			t.Fatalf("bad test ip %q", s)
		}
		if isPublicIP(ip) {
			t.Fatalf("expected %q to be rejected as non-public", s)
		}
	}
}

func TestControlReject(t *testing.T) {
	allowed := []string{"8.8.8.8:443", "1.1.1.1:80", "[2606:4700:4700::1111]:443"}
	for _, addr := range allowed {
		if err := ControlReject("tcp", addr, nil); err != nil {
			t.Fatalf("ControlReject(%q) = %v, want nil (public)", addr, err)
		}
	}
	rejected := []string{
		"127.0.0.1:80",
		"169.254.169.254:80",
		"10.0.0.5:443",
		"192.168.1.1:443",
		"172.16.0.1:443",
		"100.64.0.1:443",
		"[::1]:443",
		"[fc00::1]:443",
		"[fe80::1]:443",
	}
	for _, addr := range rejected {
		if err := ControlReject("tcp", addr, nil); err == nil {
			t.Fatalf("ControlReject(%q) = nil, want error (non-public)", addr)
		}
	}
	// A non-IP-literal dial address (DNS name) must be rejected: the dialer
	// passes a resolved IP:port, so a hostname here is unexpected.
	if err := ControlReject("tcp", "example.com:443", nil); err == nil {
		t.Fatal("ControlReject with a hostname should error")
	}
}
