package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/config"
)

func TestSanitizeAccountRedirectPath(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{name: "blank", raw: "", want: "/account"},
		{name: "absolute", raw: "https://example.com", want: "/account"},
		{name: "scheme relative", raw: "//example.com/account", want: "/account"},
		{name: "path", raw: "/account/console", want: "/account/console"},
		{name: "strip stale account error", raw: "/account?error=expired_token", want: "/account"},
		{name: "strip auth flow keys", raw: "/account?auth=complete&redirect_path=%2Fstreams&token=abc&error=expired_token", want: "/account"},
		{name: "preserve stream filters", raw: "/streams?capture_types=hls,http_video&sort_by=name&sort_dir=desc&page=1&page_size=100", want: "/streams?capture_types=hls%2Chttp_video&page=1&page_size=100&sort_by=name&sort_dir=desc"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := sanitizeAccountRedirectPath(tt.raw); got != tt.want {
				t.Fatalf("redirect=%q want %q", got, tt.want)
			}
		})
	}
}

func TestBuildAccountMagicLinkErrorsOnEmptyAppBaseURL(t *testing.T) {
	s := &Server{}
	if _, err := s.buildAccountMagicLink("abc123"); err == nil {
		t.Fatalf("expected error when AppBaseURL is empty")
	}
}

func TestBuildAccountMagicLinkUsesAppBaseURL(t *testing.T) {
	s := &Server{cfg: config.Config{AppBaseURL: "https://app.example.test/"}}
	got, err := s.buildAccountMagicLink("abc123")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := "https://app.example.test/auth/complete?token=abc123"
	if got != want {
		t.Fatalf("magic link=%q want %q", got, want)
	}
}

func TestBuildAccountPostAuthRedirectPath(t *testing.T) {
	got := buildAccountPostAuthRedirectPath("/streams/1?tab=details")
	want := "/streams/1?auth=complete&tab=details"
	if got != want {
		t.Fatalf("post auth redirect=%q want %q", got, want)
	}
}

func TestBuildAccountPostAuthRedirectPathStripsStaleErrors(t *testing.T) {
	got := buildAccountPostAuthRedirectPath("/account?error=expired_token")
	want := "/account?auth=complete"
	if got != want {
		t.Fatalf("post auth redirect=%q want %q", got, want)
	}
}

func TestCanonicalAccountAuthCompleteURL(t *testing.T) {
	s := &Server{cfg: config.Config{AppBaseURL: "https://stoarama.com"}}
	req := httptest.NewRequest(http.MethodGet, "https://stoarama-api.onrender.com/auth/complete?token=abc123", nil)
	got, ok := s.canonicalAccountAuthCompleteURL(req)
	if !ok {
		t.Fatalf("expected canonical redirect")
	}
	want := "https://stoarama.com/auth/complete?token=abc123"
	if got != want {
		t.Fatalf("canonical redirect=%q want %q", got, want)
	}
}

func TestCanonicalAccountAuthCompleteURLSameHost(t *testing.T) {
	s := &Server{cfg: config.Config{AppBaseURL: "https://stoarama.com"}}
	req := httptest.NewRequest(http.MethodGet, "https://stoarama.com/auth/complete?token=abc123", nil)
	if got, ok := s.canonicalAccountAuthCompleteURL(req); ok {
		t.Fatalf("unexpected canonical redirect %q", got)
	}
}

func TestCanonicalAccountAuthCompleteURLUpgradesSameHostToHTTPS(t *testing.T) {
	s := &Server{cfg: config.Config{AppBaseURL: "https://stoarama.com"}}
	req := httptest.NewRequest(http.MethodGet, "http://stoarama.com/auth/complete?token=abc123", nil)
	got, ok := s.canonicalAccountAuthCompleteURL(req)
	if !ok {
		t.Fatalf("expected canonical redirect")
	}
	want := "https://stoarama.com/auth/complete?token=abc123"
	if got != want {
		t.Fatalf("canonical redirect=%q want %q", got, want)
	}
}

func TestHandleAccountAuthCompleteCanonicalizesBeforeTokenUse(t *testing.T) {
	s := &Server{cfg: config.Config{AppBaseURL: "https://stoarama.com"}}
	req := httptest.NewRequest(http.MethodGet, "https://stoarama-api.onrender.com/auth/complete?token=abc123", nil)
	rr := httptest.NewRecorder()

	s.handleAccountAuthComplete(rr, req)

	if rr.Code != http.StatusFound {
		t.Fatalf("status=%d want %d", rr.Code, http.StatusFound)
	}
	want := "https://stoarama.com/auth/complete?token=abc123"
	if got := rr.Header().Get("Location"); got != want {
		t.Fatalf("location=%q want %q", got, want)
	}
}

func TestAccountHTMLShowsExpiredLinkMessage(t *testing.T) {
	html, err := os.ReadFile("../../web/account.html")
	if err != nil {
		t.Fatalf("read account html: %v", err)
	}
	if !strings.Contains(string(html), "That sign-in link expired. Request a new link.") {
		t.Fatalf("account html missing expired-link message")
	}
}

func TestAccountHTMLSignInUsesSubmitForm(t *testing.T) {
	htmlBytes, err := os.ReadFile("../../web/account.html")
	if err != nil {
		t.Fatalf("read account html: %v", err)
	}
	html := string(htmlBytes)
	for _, want := range []string{
		`<form id="signinForm">`,
		`type="submit">Send sign-in link</button>`,
		`signinForm.addEventListener('submit'`,
		`event.preventDefault();`,
	} {
		if !strings.Contains(html, want) {
			t.Fatalf("account html missing %q", want)
		}
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

func TestAccountSessionCapabilitiesGatedOnBrowserSession(t *testing.T) {
	// handleAccountMe now also reads has_keys_or_nodes from the DB, so it cannot be
	// driven with a nil pool here; the DB-free capability/session shape it serves is
	// asserted through accountSessionCapabilities directly.
	sessionID := int64(42)
	caps := accountSessionCapabilities(accountPrincipal{
		AccountID: 7,
		Email:     "deniz@example.com",
		Role:      accountRoleAdmin,
		AuthType:  "session",
		SessionID: &sessionID,
	})
	for _, key := range []string{"can_toggle_recording", "can_manage_api_keys", "can_download_clips", "can_edit_tags"} {
		if v, _ := caps[key].(bool); !v {
			t.Fatalf("capability %q=false want true for a browser session", key)
		}
	}

	noSession := accountSessionCapabilities(accountPrincipal{
		AccountID: 7,
		AuthType:  "api_key",
	})
	for _, key := range []string{"can_toggle_recording", "can_manage_api_keys", "can_download_clips", "can_edit_tags"} {
		if v, _ := noSession[key].(bool); v {
			t.Fatalf("capability %q=true want false without a browser session", key)
		}
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

func TestCaptureSegmentWhereFiltersSuccessfulDownloadableClips(t *testing.T) {
	where, args := captureSegmentWhere(captureSegmentQueryOptions{
		StreamID:            17205,
		SegmentIDs:          []int64{10, 11},
		CaptureStatus:       "success",
		RequireDownloadable: true,
	})
	joined := strings.Join(where, " AND ")
	if !strings.Contains(joined, "cs.stream_id = $1") {
		t.Fatalf("where=%q missing stream filter", joined)
	}
	if !strings.Contains(joined, "cs.id = ANY($2::bigint[])") {
		t.Fatalf("where=%q missing segment id filter", joined)
	}
	if !strings.Contains(joined, "cs.capture_status = $3") {
		t.Fatalf("where=%q missing capture status filter", joined)
	}
	if !strings.Contains(joined, "cs.media_object_id IS NOT NULL") {
		t.Fatalf("where=%q missing downloadable media filter", joined)
	}
	if len(args) != 3 || args[0] != int64(17205) || args[2] != "success" {
		t.Fatalf("args=%#v want stream id, segment ids, success", args)
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
