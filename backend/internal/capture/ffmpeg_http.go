package capture

import (
	"strconv"
	"strings"
)

func appendFFmpegHTTPInputArgs(args []string, sourceURL string, reconnect bool, reconnectDelayMax int, pinHost string) []string {
	if !strings.HasPrefix(sourceURL, "http://") && !strings.HasPrefix(sourceURL, "https://") {
		return args
	}
	args = append(args,
		"-rw_timeout", "15000000",
		"-timeout", "15000000",
	)
	// SSRF hardening for the recorder path: restrict the input demuxer to
	// network protocols only. For HLS this also constrains child segment and
	// key fetches, so a crafted playlist cannot pull in local files (file://)
	// or other non-network protocols. Placed before -i so it scopes to the
	// input demuxer; the local output file still writes. Unconditional for
	// http(s) inputs: the stream URL is now handed to ffmpeg as the original
	// hostname (no IP pin) so TLS SNI + Host routing work for SNI/Host-routed
	// CDNs, and this whitelist plus the droplet egress firewall remain the
	// SSRF backstop for private-IP segment/redirect targets.
	args = append(args, "-protocol_whitelist", "https,tls,tcp,http,crypto,data")
	// When a host override is supplied, carry it as the HTTP Host header so a
	// virtual-hosted origin still routes correctly. Empty for the hostname path
	// (ffmpeg derives Host and TLS SNI from the URL itself).
	if pinHost != "" {
		args = append(args, "-headers", "Host: "+pinHost+"\r\n")
	}
	if reconnect {
		if reconnectDelayMax < 1 {
			reconnectDelayMax = 1
		}
		if reconnectDelayMax > 60 {
			reconnectDelayMax = 60
		}
		args = append(args,
			"-reconnect", "1",
			"-reconnect_streamed", "1",
			"-reconnect_on_network_error", "1",
			"-reconnect_on_http_error", "4xx,5xx",
			"-reconnect_delay_max", strconv.Itoa(reconnectDelayMax),
		)
	}
	return args
}
