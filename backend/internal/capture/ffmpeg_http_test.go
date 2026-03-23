package capture

import "testing"

func TestAppendFFmpegHTTPInputArgsHTTP(t *testing.T) {
	args := appendFFmpegHTTPInputArgs(nil, "https://example.com/live.m3u8", true, 10)
	joined := make(map[string]bool, len(args))
	for _, arg := range args {
		joined[arg] = true
	}
	if !joined["-http_persistent"] {
		t.Fatalf("expected -http_persistent in args: %#v", args)
	}
	if joined["-http_multiple"] {
		t.Fatalf("did not expect -http_multiple in args: %#v", args)
	}
	if !joined["-reconnect"] {
		t.Fatalf("expected reconnect args in %#v", args)
	}
}

func TestAppendFFmpegHTTPInputArgsNonHTTP(t *testing.T) {
	args := appendFFmpegHTTPInputArgs(nil, "rtsp://example.com/live", true, 10)
	if len(args) != 0 {
		t.Fatalf("expected no args for non-http input, got %#v", args)
	}
}
