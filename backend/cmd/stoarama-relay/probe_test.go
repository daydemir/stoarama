package main

import "testing"

func TestClassifyFFmpegNetworkProbe(t *testing.T) {
	for in, want := range map[string]string{
		"Failed to resolve hostname manifest.googlevideo.com": "dns_failed",
		"Server returned 404 Not Found":                       "host_reached",
	} {
		if got := classifyFFmpegNetworkProbe(in); got != want {
			t.Fatalf("classifyFFmpegNetworkProbe(%q)=%q want=%q", in, got, want)
		}
	}
}
