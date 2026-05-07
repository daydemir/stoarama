package main

import (
	"os"
	"strings"
	"testing"
)

// This guard enforces CLI-first parity:
// dashboard-visible functional API routes must also be called by stoaramactl.
func TestDashboardAPISurfaceHasBackendctlParity(t *testing.T) {
	srcBytes, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatalf("read main.go: %v", err)
	}
	src := string(srcBytes)

	requiredFragments := []string{
		"/api/v1/dashboard/overview",
		"/api/v1/dashboard/queue-health",
		"/api/v1/dashboard/pipelines/overview",
		"/api/v1/dashboard/servers",
		"/api/v1/dashboard/streams?",
		"/api/v1/dashboard/countries",
		"/api/v1/dashboard/cities",
		"/api/v1/dashboard/sources",
		"/api/v1/dashboard/youtube-channels",
		"/api/v1/dashboard/tags",
		"/api/v1/dashboard/streams/image-urls",
		"/api/v1/dashboard/recording/capacity",
		"/api/v1/dashboard/recording/server-capacity",
		"/api/v1/dashboard/recording/settings",
		"/api/v1/dashboard/inference?",
		"/api/v1/dashboard/inference/cleanup-unboxed",
		"/api/v1/dashboard/streams/%d/coverage",
		"/api/v1/dashboard/streams/%d/capture-samples",
		"/api/v1/dashboard/streams/%d/timeline",
		"/api/v1/dashboard/streams/%d/pipelines",
		"/api/v1/dashboard/streams/%d/detections",
		"/api/v1/dashboard/streams/%d/recording",
		"/api/v1/frames?",
		"/api/v1/streams/%d",
		"/api/v1/recording/streams/%d/assign",
		"/api/v1/recording/streams/%d/unassign",
		"case \"korea\":",
		"runKorea(ctx, cfg, os.Args[2:])",
		"stoaramactl korea inventory",
		"stoaramactl korea audit",
	}
	for _, frag := range requiredFragments {
		if !strings.Contains(src, frag) {
			t.Fatalf("stoaramactl parity missing API fragment: %s", frag)
		}
	}
}
