package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestSanitizeAccountRedirectPath(t *testing.T) {
	if got := sanitizeAccountRedirectPath(""); got != "/account" {
		t.Fatalf("blank redirect=%q want /account", got)
	}
	if got := sanitizeAccountRedirectPath("https://example.com"); got != "/account" {
		t.Fatalf("absolute redirect=%q want /account", got)
	}
	if got := sanitizeAccountRedirectPath("/account/console"); got != "/account/console" {
		t.Fatalf("path redirect=%q want /account/console", got)
	}
}

func TestBuildAccountMagicLinkFallsBackToRequestHost(t *testing.T) {
	s := &Server{}
	req := httptest.NewRequest("GET", "https://api.example.test/account", nil)
	got := s.buildAccountMagicLink(req, "abc123")
	want := "https://api.example.test/auth/complete?token=abc123"
	if got != want {
		t.Fatalf("magic link=%q want %q", got, want)
	}
}

func TestBuildAccountPostAuthRedirectPath(t *testing.T) {
	got := buildAccountPostAuthRedirectPath("/streams/1?tab=details")
	want := "/account?auth=complete&redirect_path=%2Fstreams%2F1%3Ftab%3Ddetails"
	if got != want {
		t.Fatalf("post auth redirect=%q want %q", got, want)
	}
}

func TestMaskSecretForLog(t *testing.T) {
	if got := maskSecretForLog(""); got != "" {
		t.Fatalf("blank mask=%q want empty", got)
	}
	if got := maskSecretForLog("abcdefgh"); got != "ab...gh" {
		t.Fatalf("short mask=%q want ab...gh", got)
	}
	if got := maskSecretForLog("abcdefghijklmnopqrstuvwxyz"); got != "abcdef...wxyz" {
		t.Fatalf("long mask=%q want abcdef...wxyz", got)
	}
}

func TestHashSecretStable(t *testing.T) {
	if got := hashSecret("test-token"); got != hashSecret("test-token") {
		t.Fatalf("hash should be stable")
	}
	if got := hashSecret("test-token"); got == hashSecret("other-token") {
		t.Fatalf("hashes should differ")
	}
}

func TestHandleDataAccessSpecIncludesClipBatchLimit(t *testing.T) {
	s := &Server{}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/data-access-spec", nil)
	rec := httptest.NewRecorder()

	s.handleDataAccessSpec(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d want %d", rec.Code, http.StatusOK)
	}
	var payload struct {
		BatchLimits map[string]int `json:"batch_limits"`
		Endpoints   []struct {
			Key  string `json:"key"`
			Auth string `json:"auth"`
		} `json:"endpoints"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode json: %v", err)
	}
	if got := payload.BatchLimits["clip_download_prepare_max_segments"]; got != accountClipBatchLimit {
		t.Fatalf("clip_download_prepare_max_segments=%d want %d", got, accountClipBatchLimit)
	}
	found := false
	for _, endpoint := range payload.Endpoints {
		if endpoint.Key == "account_clip_list" {
			found = true
			if endpoint.Auth != "account" {
				t.Fatalf("account_clip_list auth=%q want account", endpoint.Auth)
			}
		}
	}
	if !found {
		t.Fatalf("account_clip_list endpoint missing")
	}
}

func TestBuildAccountClipFilenameUsesStreamSlugAndMimeExtension(t *testing.T) {
	item := captureSegmentListItem{
		StreamID:       44,
		SegmentStartAt: time.Date(2026, time.March, 24, 12, 34, 56, 0, time.UTC),
		MIMEType:       ptrString("video/webm"),
	}
	got := buildAccountClipFilename("seoul-square", item)
	want := "seoul-square-20260324T123456Z.webm"
	if got != want {
		t.Fatalf("filename=%q want %q", got, want)
	}
}

func TestUniquePositiveInt64s(t *testing.T) {
	got := uniquePositiveInt64s([]int64{4, 0, 4, -1, 9, 3, 9})
	want := []int64{4, 9, 3}
	if len(got) != len(want) {
		t.Fatalf("len=%d want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got[%d]=%d want %d", i, got[i], want[i])
		}
	}
}

func ptrString(v string) *string {
	return &v
}
