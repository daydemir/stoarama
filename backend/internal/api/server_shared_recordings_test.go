package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/config"
)

func testSharedRecordingsSigningKey() string {
	return strings.Repeat("test", 8)
}

func TestSharedRecordingsTokenExpiresAndRotatesWithSigningKey(t *testing.T) {
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	token := sharedRecordingsToken(now.Add(time.Hour), "first-signing-key")
	if !validSharedRecordingsToken(token, "first-signing-key", now) {
		t.Fatal("fresh token rejected")
	}
	if validSharedRecordingsToken(token, "second-signing-key", now) {
		t.Fatal("signing-key rotation did not invalidate token")
	}
	if validSharedRecordingsToken(token, "first-signing-key", now.Add(time.Hour)) {
		t.Fatal("expired token accepted")
	}
}

func TestSharedRecordingsLimiterAllowsFiveFailuresPerWindow(t *testing.T) {
	limiter := newSharedRecordingsLimiter()
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	for i := 0; i < sharedRecordingsMaxFailures; i++ {
		if !limiter.allow("client", now) {
			t.Fatalf("attempt %d unexpectedly blocked", i+1)
		}
		limiter.fail("client", now)
	}
	if limiter.allow("client", now) {
		t.Fatal("sixth attempt allowed")
	}
	if !limiter.allow("different-client", now) {
		t.Fatal("one blocked client throttled a different client")
	}
	if !limiter.allow("client", now.Add(sharedRecordingsRateWindow+time.Second)) {
		t.Fatal("client remained blocked after rate window")
	}
}

func TestSharedRecordingsLimiterDoesNotBlockOtherClients(t *testing.T) {
	limiter := newSharedRecordingsLimiter()
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	for i := 0; i < sharedRecordingsMaxFailures; i++ {
		limiter.fail("blocked-client", now)
	}
	if !limiter.allow("different-client", now) {
		t.Fatal("one client blocked a different client")
	}
}

func TestSharedRecordingsLimiterBoundsTrackedClients(t *testing.T) {
	limiter := newSharedRecordingsLimiter()
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	for i := 0; i < sharedRecordingsMaxClients+10; i++ {
		limiter.fail(strconv.Itoa(i), now)
	}
	if got := len(limiter.failures); got != sharedRecordingsMaxClients {
		t.Fatalf("tracked clients=%d want=%d", got, sharedRecordingsMaxClients)
	}
	if _, exists := limiter.failures["0"]; exists {
		t.Fatal("oldest client was not evicted")
	}
	if _, exists := limiter.failures[strconv.Itoa(sharedRecordingsMaxClients+9)]; !exists {
		t.Fatal("newest client was not retained")
	}
}

func TestSharedRecordingsRateLimitUsesTrustedProxyClientIP(t *testing.T) {
	trusted := []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8")}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/shared/mit-scl/unlock", nil)
	req.RemoteAddr = "10.0.0.10:443"
	req.Header.Set("CF-Connecting-IP", "198.51.100.8")
	req.Header.Set("X-Forwarded-For", "203.0.113.42, 10.0.0.2")
	if got := sharedRecordingsRequesterIP(req, trusted); got != "198.51.100.8" {
		t.Fatalf("Cloudflare client IP=%q", got)
	}
	req.Header.Set("CF-Connecting-IP", "invalid")
	if got := sharedRecordingsRequesterIP(req, trusted); got != "203.0.113.42" {
		t.Fatalf("client IP=%q", got)
	}
	req.Header.Set("X-Forwarded-For", "spoofed")
	if got := sharedRecordingsRequesterIP(req, trusted); got != "10.0.0.10" {
		t.Fatalf("fallback client IP=%q", got)
	}
	req.RemoteAddr = "192.0.2.1:1234"
	req.Header.Set("CF-Connecting-IP", "198.51.100.8")
	req.Header.Set("X-Forwarded-For", "203.0.113.42")
	if got := sharedRecordingsRequesterIP(req, trusted); got != "192.0.2.1" {
		t.Fatalf("untrusted peer spoofed client IP=%q", got)
	}
}

func TestSharedRecordingDTOExcludesSensitiveFields(t *testing.T) {
	raw := map[string]any{
		"id": 7, "name": "Square", "status": "active", "mode": "continuous",
		"cron_expr": "", "cron_timezone": "Europe/Warsaw", "clip_duration_sec": 60,
		"daily_window_start": "08:00", "daily_window_end": "20:00", "active_weekdays": 127,
		"start_at": time.Now(), "captured_clip_count": 9, "expected_clip_count": 10,
		"capture_health": "warning", "source_kind": "hls", "capture_via": "cloud",
		"has_relay_online": false, "has_relay_assigned": false,
		"stream_url":             "https://secret.example/playlist.m3u8",
		"storage_destination_id": 55, "storage_destination_name": "private NAS",
	}
	item, err := sharedRecordingFrom(raw)
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(item)
	if err != nil {
		t.Fatal(err)
	}
	encoded := string(body)
	for _, forbidden := range []string{"stream_url", "storage_destination", "download_url", "api_key"} {
		if strings.Contains(encoded, forbidden) {
			t.Fatalf("shared DTO leaked %q: %s", forbidden, encoded)
		}
	}
}

func TestSharedRecordingsPageDisabledWithoutConfiguration(t *testing.T) {
	s := &Server{cfg: config.Config{}, sharedRecordingsHTML: []byte("secret page")}
	rec := httptest.NewRecorder()
	s.handleSharedRecordingsApp(rec, httptest.NewRequest("GET", "/shared/mit-scl/recordings", nil))
	if rec.Code != 404 {
		t.Fatalf("status=%d want 404", rec.Code)
	}
}

func TestSharedRecordingsUsesConfiguredSlug(t *testing.T) {
	for _, slug := range []string{"research-team", "second-team"} {
		router, err := NewRouter(config.Config{
			SharedRecordingsAccountID:        47,
			SharedRecordingsSlug:             slug,
			SharedRecordingsPassword:         "team-password",
			SharedRecordingsCookieSigningKey: testSharedRecordingsSigningKey(),
		}, nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/shared/"+slug+"/recordings", nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("configured slug %q status=%d", slug, rec.Code)
		}
		if body := rec.Body.String(); !strings.Contains(body, `const SHARED_PATH = '/shared/`+slug+`';`) || strings.Contains(body, "__SHARED_RECORDINGS_SLUG__") {
			t.Fatalf("configured slug %q was not independently injected into shared page", slug)
		}
	}
}

func TestSharedRecordingsNamespaceHasNoMutationRoutes(t *testing.T) {
	now := time.Now().UTC()
	s := &Server{
		cfg: config.Config{
			SharedRecordingsAccountID:        47,
			SharedRecordingsSlug:             "research-team",
			SharedRecordingsPassword:         "team-password",
			SharedRecordingsCookieSigningKey: testSharedRecordingsSigningKey(),
		},
		sharedRecordingsLimiter: newSharedRecordingsLimiter(),
	}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/shared/research-team/recordings/7/pause", nil)
	req.AddCookie(&http.Cookie{Name: sharedRecordingsCookie, Value: sharedRecordingsToken(now.Add(time.Hour), "0123456789abcdef0123456789abcdef")})
	rec := httptest.NewRecorder()
	s.router().ServeHTTP(rec, req)
	if rec.Code < 400 {
		t.Fatalf("shared mutation route status=%d want rejection", rec.Code)
	}
}

func TestSharedRecordingsPageHasAccessibleHeatmap(t *testing.T) {
	body, err := loadHTMLPage("shared-recordings.html")
	if err != nil {
		t.Fatal(err)
	}
	page := string(body)
	for _, marker := range []string{
		`role="tooltip"`,
		`aria-label="Recent scheduled capture health"`,
		`data-tip=`,
		`pointerover`,
		`focusin`,
		`Escape`,
		`const SHARED_PATH = '/shared/__SHARED_RECORDINGS_SLUG__';`,
		`Array.from({length:24},()=>[])`,
		`bins.map(bin=>({bin,hour}))`,
		`repeat(${slots.length},minmax(19px,1fr))`,
		`timeZoneName:'short'`,
	} {
		if !strings.Contains(page, marker) {
			t.Fatalf("shared recordings page missing %q", marker)
		}
	}
}
