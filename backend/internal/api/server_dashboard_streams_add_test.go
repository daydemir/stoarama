package api

import "testing"

func TestValidatePublicHLSStreamURLAcceptsHTTPSM3U8(t *testing.T) {
	if err := validatePublicHLSStreamURL("https://example.com/live/stream.m3u8?token=abc"); err != nil {
		t.Fatalf("expected https .m3u8 URL to pass, got %v", err)
	}
}

func TestValidatePublicHLSStreamURLRejectsNonHLSOrNonHTTPS(t *testing.T) {
	tests := []string{
		"http://example.com/live/stream.m3u8",
		"https://example.com/live/stream.mp4",
		"https://www.youtube.com/watch?v=abc123",
		"not a url",
	}
	for _, raw := range tests {
		if err := validatePublicHLSStreamURL(raw); err == nil {
			t.Fatalf("expected %q to be rejected", raw)
		}
	}
}
