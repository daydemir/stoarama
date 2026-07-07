package api

import (
	"strings"
	"testing"
)

func TestRewriteSkylineManifestProxiesOnlySegmentURIs(t *testing.T) {
	in := "#EXTM3U\n#EXTINF:3.000,\nhttps://hddn59.skylinewebcams.com/0589livic-1.ts\n#EXTINF:3.000,\n/0589livic-2.ts\n"
	out, err := rewriteSkylineManifest(14303, "https://hddn59.skylinewebcams.com/live.m3u8?a=token", in)
	if err != nil {
		t.Fatalf("rewriteSkylineManifest: %v", err)
	}
	if strings.Contains(out, "https://hddn59.skylinewebcams.com/0589livic") {
		t.Fatalf("manifest leaked upstream segment URL: %s", out)
	}
	if got := strings.Count(out, "/api/v1/dashboard/streams/14303/skyline-segment?u="); got != 2 {
		t.Fatalf("proxied segment count=%d want 2 in %s", got, out)
	}
	if !strings.Contains(out, "#EXTM3U") || !strings.Contains(out, "#EXTINF:3.000,") {
		t.Fatalf("manifest tags were not preserved: %s", out)
	}
}

func TestValidateSkylineSegmentURLRejectsNonSkylineTargets(t *testing.T) {
	good := "https://hddn59.skylinewebcams.com/0589livic-1.ts"
	if err := validateSkylineSegmentURL(good); err != nil {
		t.Fatalf("good segment rejected: %v", err)
	}
	bad := []string{
		"http://hddn59.skylinewebcams.com/0589livic-1.ts",
		"https://example.com/0589livic-1.ts",
		"https://hd-auth.skylinewebcams.com/live.m3u8?a=token",
		"https://hddn59.skylinewebcams.com/playlist.m3u8",
	}
	for _, raw := range bad {
		if err := validateSkylineSegmentURL(raw); err == nil {
			t.Fatalf("bad segment accepted: %s", raw)
		}
	}
}
