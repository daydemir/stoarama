package capture

import "testing"

func TestAppendFFmpegHTTPInputArgsHTTP(t *testing.T) {
	args := appendFFmpegHTTPInputArgs(nil, "https://example.com/live.m3u8", true, 10, "")
	joined := make(map[string]bool, len(args))
	for _, arg := range args {
		joined[arg] = true
	}
	if joined["-http_persistent"] {
		t.Fatalf("did not expect -http_persistent in args: %#v", args)
	}
	if joined["-http_multiple"] {
		t.Fatalf("did not expect -http_multiple in args: %#v", args)
	}
	if !joined["-reconnect"] {
		t.Fatalf("expected reconnect args in %#v", args)
	}
	if !joined["-rw_timeout"] || !joined["15000000"] {
		t.Fatalf("expected read timeout args in %#v", args)
	}
	if !joined["-timeout"] {
		t.Fatalf("expected connection timeout args in %#v", args)
	}
	if joined["-headers"] {
		t.Fatalf("did not expect Host header args without a pin host: %#v", args)
	}
	if !joined["-protocol_whitelist"] {
		t.Fatalf("expected -protocol_whitelist on every http input: %#v", args)
	}
}

func TestAppendFFmpegHTTPInputArgsPinHost(t *testing.T) {
	args := appendFFmpegHTTPInputArgs(nil, "https://203.0.113.10/live.m3u8", false, 0, "example.com")
	foundHeaders := false
	foundWhitelist := false
	for i, arg := range args {
		if arg == "-headers" {
			foundHeaders = true
			if i+1 >= len(args) || args[i+1] != "Host: example.com\r\n" {
				t.Fatalf("expected Host header value, got %#v", args)
			}
		}
		if arg == "-protocol_whitelist" {
			foundWhitelist = true
			if i+1 >= len(args) || args[i+1] != "https,tls,tcp,http,crypto,data" {
				t.Fatalf("expected protocol whitelist value, got %#v", args)
			}
		}
	}
	if !foundHeaders {
		t.Fatalf("expected -headers with pin host in %#v", args)
	}
	if !foundWhitelist {
		t.Fatalf("expected -protocol_whitelist with pin host in %#v", args)
	}
}

func TestAppendFFmpegHTTPInputArgsNonHTTP(t *testing.T) {
	args := appendFFmpegHTTPInputArgs(nil, "rtsp://example.com/live", true, 10, "example.com")
	if len(args) != 0 {
		t.Fatalf("expected no args for non-http input, got %#v", args)
	}
}
