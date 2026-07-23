package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestRecordingProbeRequiresRelayForManualRelayURL(t *testing.T) {
	body := bytes.NewBufferString(`{"stream_url":"https://61e0c5d388c2e.streamlock.net/live/test/playlist.m3u8"}`)
	req := withPrincipal(
		httptest.NewRequest(http.MethodPost, "/api/v1/account/recordings/probe", body),
		accountPrincipal{AccountID: 1, UserID: 1},
		"",
	)
	rec := httptest.NewRecorder()
	(&Server{}).handleAccountRecordingsProbe(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	var response struct {
		OK               bool `json:"ok"`
		RelayRecommended bool `json:"relay_recommended"`
		RelayRequired    bool `json:"relay_required"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if !response.OK || !response.RelayRecommended || !response.RelayRequired {
		t.Fatalf("response=%+v", response)
	}
}

func TestSanitizeStorageKeyPrefix(t *testing.T) {
	ok := map[string]string{
		"":               "",
		"   ":            "",
		"/clips/":        "clips",
		"a/b/c":          "a/b/c",
		"  team-1/cams ": "team-1/cams",
	}
	for in, want := range ok {
		got, err := sanitizeStorageKeyPrefix(in)
		if err != nil {
			t.Fatalf("sanitizeStorageKeyPrefix(%q) unexpected error: %v", in, err)
		}
		if got != want {
			t.Fatalf("sanitizeStorageKeyPrefix(%q) = %q, want %q", in, got, want)
		}
	}

	bad := []string{
		"a/../b",          // parent traversal
		"../etc",          // leading parent after trim
		"a/./b",           // current-dir segment
		"a//b",            // empty interior segment
		"a\\b",            // backslash
		"a\x00b",          // null byte
		"a\tb",            // control char
		"//double//slash", // empty interior segments
	}
	for _, in := range bad {
		if _, err := sanitizeStorageKeyPrefix(in); err == nil {
			t.Fatalf("sanitizeStorageKeyPrefix(%q) expected error", in)
		}
	}
}

func TestBuildRecordingClipObjectKey(t *testing.T) {
	fire := time.Date(2026, 6, 24, 12, 0, 0, 0, time.UTC)
	ms := fire.UnixMilli()

	withPrefix := buildRecordingClipObjectKey("team-1/cams", 7, 42, fire)
	wantWith := "team-1/cams/recordings/7/42/" + itoa(ms) + ".mp4"
	if withPrefix != wantWith {
		t.Fatalf("with prefix = %q, want %q", withPrefix, wantWith)
	}

	noPrefix := buildRecordingClipObjectKey("", 7, 42, fire)
	wantNo := "recordings/7/42/" + itoa(ms) + ".mp4"
	if noPrefix != wantNo {
		t.Fatalf("no prefix = %q, want %q", noPrefix, wantNo)
	}

	// Deterministic by fire instant: same fire -> same key (overwrite dedup).
	again := buildRecordingClipObjectKey("team-1/cams", 7, 42, fire)
	if again != withPrefix {
		t.Fatalf("key not deterministic: %q vs %q", again, withPrefix)
	}

	// Defense in depth: a stored prefix with traversal segments cannot escape
	// the recordings namespace (segments dropped, not appended verbatim).
	traversal := buildRecordingClipObjectKey("a/../../etc/./b", 7, 42, fire)
	wantTraversal := "a/etc/b/recordings/7/42/" + itoa(ms) + ".mp4"
	if traversal != wantTraversal {
		t.Fatalf("traversal prefix = %q, want %q", traversal, wantTraversal)
	}
	if strings.Contains(traversal, "..") {
		t.Fatalf("object key must not contain '..': %q", traversal)
	}
}

func TestClassifyRecordingSource(t *testing.T) {
	hls := []string{
		"https://example.com/live/index.m3u8",
		"https://example.com/stream!hls",
	}
	for _, u := range hls {
		kind, err := classifyRecordingSource(u)
		if err != nil || kind != "hls_live" {
			t.Fatalf("classifyRecordingSource(%q) = %q, %v; want hls_live", u, kind, err)
		}
	}

	if kind, err := classifyRecordingSource("https://example.com/live/stream.flv"); err != nil || kind != "ffmpeg_direct" {
		t.Fatalf("expected ffmpeg_direct for a direct https stream, got %q, %v", kind, err)
	}

	// youtube/image/empty are rejected by the classifier itself. (rtsp and other
	// non-http schemes are rejected earlier by netguard.ValidatePublicURL, which
	// is exercised in the netguard package tests.)
	reject := []string{
		"https://www.youtube.com/watch?v=abc",
		"https://example.com/image.jpg",
		"",
	}
	for _, u := range reject {
		if _, err := classifyRecordingSource(u); err == nil {
			t.Fatalf("classifyRecordingSource(%q) expected rejection", u)
		}
	}
}

func TestRecordingLeaseIncludesCatalogResolveContextWithoutWeakeningTenantWall(t *testing.T) {
	for _, want := range []string{
		"n.account_id = rec.account_id",
		"LEFT JOIN streams st ON st.id = rec.stream_id",
		"COALESCE(st.provider, '')",
		"COALESCE(st.source_page_url, '')",
	} {
		if !strings.Contains(relayLeaseSQL, want) {
			t.Fatalf("relay lease SQL missing %q", want)
		}
	}
	for _, forbidden := range []string{
		"st.account_id",
		"source_page_url=$",
		"provider=$",
	} {
		if strings.Contains(relayLeaseSQL, forbidden) {
			t.Fatalf("relay lease SQL must not contain request-scoped catalog predicate %q", forbidden)
		}
	}
}

func TestFleetRelayStatsExcludeRemovedNodes(t *testing.T) {
	for _, want := range []string{
		"n.status <> 'disabled' OR EXISTS",
		"t.node_id=n.id AND t.revoked_at IS NULL",
	} {
		if !strings.Contains(visibleNodeSQL, want) {
			t.Fatalf("visible node predicate must preserve disabled nodes with active tokens and exclude removed nodes; missing %q", want)
		}
	}
}

func itoa(v int64) string {
	// small local helper to avoid importing strconv in the test for one call
	neg := v < 0
	if neg {
		v = -v
	}
	if v == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
