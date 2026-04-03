package api

import (
	"context"
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

func TestHandleAccountMeIncludesCapabilitiesAndSession(t *testing.T) {
	s := &Server{}
	sessionID := int64(42)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/account/me", nil)
	req = req.WithContext(context.WithValue(req.Context(), accountPrincipalContextKey, accountPrincipal{
		AccountID: 7,
		Email:     "deniz@example.com",
		Name:      "Deniz",
		Role:      accountRoleAdmin,
		AuthType:  "session",
		SessionID: &sessionID,
	}))
	rec := httptest.NewRecorder()

	s.handleAccountMe(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d want %d", rec.Code, http.StatusOK)
	}
	var payload struct {
		Authenticated bool `json:"authenticated"`
		Account       struct {
			ID       int64  `json:"id"`
			Email    string `json:"email"`
			Name     string `json:"name"`
			Role     string `json:"role"`
			AuthType string `json:"auth_type"`
		} `json:"account"`
		Capabilities struct {
			CanToggleRecording bool `json:"can_toggle_recording"`
			CanManageAPIKeys   bool `json:"can_manage_api_keys"`
			CanDownloadClips   bool `json:"can_download_clips"`
			CanEditTags        bool `json:"can_edit_tags"`
		} `json:"capabilities"`
		Session struct {
			AuthType       string `json:"auth_type"`
			BrowserSession bool   `json:"browser_session"`
		} `json:"session"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode json: %v", err)
	}
	if !payload.Authenticated {
		t.Fatalf("authenticated=false want true")
	}
	if payload.Account.Email != "deniz@example.com" {
		t.Fatalf("email=%q want deniz@example.com", payload.Account.Email)
	}
	if !payload.Capabilities.CanToggleRecording || !payload.Capabilities.CanManageAPIKeys || !payload.Capabilities.CanDownloadClips {
		t.Fatalf("capabilities=%+v want browser-session capabilities enabled", payload.Capabilities)
	}
	if !payload.Capabilities.CanEditTags {
		t.Fatalf("can_edit_tags=false want true")
	}
	if payload.Session.AuthType != "session" || !payload.Session.BrowserSession {
		t.Fatalf("session=%+v want session browser_session=true", payload.Session)
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
