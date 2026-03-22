package capture

import (
	"strconv"
	"strings"
)

func appendFFmpegHTTPInputArgs(args []string, sourceURL string, reconnect bool, reconnectDelayMax int) []string {
	if !strings.HasPrefix(sourceURL, "http://") && !strings.HasPrefix(sourceURL, "https://") {
		return args
	}
	// Live HLS inputs can rotate CDN hosts across playlist/segment requests.
	// Disabling HTTP persistence avoids ffmpeg reusing a keepalive connection
	// for a different host, which can otherwise abort relay-backed streams.
	args = append(args,
		"-http_persistent", "0",
		"-http_multiple", "0",
	)
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
