package main

import "testing"

func TestClassifyFFmpegNetworkProbe(t *testing.T) {
	for in, want := range map[string]string{
		"Failed to resolve hostname manifest.googlevideo.com":  "dns_failed",
		"Server returned 404 Not Found":                        "host_reached",
		"HTTP error 503 Service Unavailable":                   "host_reached",
		"tls: certificate verify failed":                       "tls_verify_failed",
		"SSL routines: unable to get local issuer certificate": "tls_verify_failed",
		"The peer certificate cannot be authenticated":         "tls_verify_failed",
		"Connection refused":                                   "other_failure",
		"":                                                     "other_failure",
	} {
		if got := classifyFFmpegNetworkProbe(in); got != want {
			t.Fatalf("classifyFFmpegNetworkProbe(%q)=%q want=%q", in, got, want)
		}
	}
}
