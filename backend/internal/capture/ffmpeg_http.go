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
	// When the URL host has been pinned to a validated literal IP (SSRF
	// rebinding defense), carry the original hostname as the Host header so the
	// HTTP virtual host and TLS SNI still resolve to the intended origin.
	if pinHost != "" {
		args = append(args, "-headers", "Host: "+pinHost+"\r\n")
		// SSRF hardening for the recorder path: restrict the input demuxer to
		// network protocols only. For HLS this also constrains child segment
		// and key fetches, so a crafted playlist cannot pull in local files
		// (file://) or other non-network protocols. Placed before -i so it
		// scopes to the input demuxer; the local output file still writes.
		// (Private-IP segment/redirect targets are blocked at the droplet
		// egress firewall, not here, since http stays whitelisted.)
		args = append(args, "-protocol_whitelist", "https,tls,tcp,http,crypto,data")
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
