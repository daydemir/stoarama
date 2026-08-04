package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/secretbox"
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

func TestStorageEndpointRequiresHTTPS(t *testing.T) {
	for _, endpoint := range []string{"", "http://example.test", "ftp://example.test", "https://user:pass@example.test", "https:///missing-host", "https://:443"} {
		if err := validateStorageEndpointHTTPS(endpoint); err == nil {
			t.Fatalf("endpoint %q accepted", endpoint)
		}
	}
	for _, endpoint := range []string{"https://example.test", "HTTPS://example.test/storage"} {
		if err := validateStorageEndpointHTTPS(endpoint); err != nil {
			t.Fatalf("endpoint %q rejected: %v", endpoint, err)
		}
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
	if _, exists := limiter.failures["0"]; !exists {
		t.Fatal("active failure history was evicted")
	}
	overflow := strconv.Itoa(sharedRecordingsMaxClients)
	if limiter.allow(overflow, now) {
		t.Fatal("unknown client was allowed after limiter reached capacity")
	}
	if _, exists := limiter.failures[overflow]; exists {
		t.Fatal("overflow client was tracked")
	}
	if want := now.Add(sharedRecordingsRateWindow); !limiter.nextExpiry.Equal(want) {
		t.Fatalf("next expiry=%s want %s", limiter.nextExpiry, want)
	}

	afterWindow := now.Add(sharedRecordingsRateWindow + time.Second)
	if !limiter.allow(overflow, afterWindow) {
		t.Fatal("expired entries were not pruned")
	}
	if !limiter.nextExpiry.IsZero() {
		t.Fatalf("next expiry=%s want zero", limiter.nextExpiry)
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

func TestSharedRecordingsExposeOnlyActiveAndPaused(t *testing.T) {
	s, pool, cleanup := testIdentityServer(t)
	defer cleanup()
	ctx := context.Background()

	if _, err := pool.Exec(ctx, `
		INSERT INTO accounts(id,email,name,role,status) VALUES(47,'mit-scl@example.test','MIT SCL','admin','active');
		INSERT INTO storage_destinations(id,account_id,name,endpoint,region,bucket,access_key_id,secret_access_key_enc,status)
		VALUES(1,47,'MIT storage','https://example.test','auto','clips','access',''::bytea,'verified');
	`); err != nil {
		t.Fatal(err)
	}
	ids := map[string]int64{}
	for _, status := range []string{"active", "paused", "completed", "canceled"} {
		var id int64
		if err := pool.QueryRow(ctx, `
			INSERT INTO recordings(account_id,storage_destination_id,name,stream_url,cron_expr,status)
			VALUES(47,1,$1,'https://example.test/live.m3u8','* * * * *',$2)
			RETURNING id
		`, status+" recording", status).Scan(&id); err != nil {
			t.Fatalf("insert %s recording: %v", status, err)
		}
		ids[status] = id
	}
	s.cfg = config.Config{SharedRecordingsAccountID: 47, SharedRecordingsSlug: "mit-scl", SharedRecordingsPublic: true}
	router := s.router()

	list := httptest.NewRecorder()
	router.ServeHTTP(list, httptest.NewRequest(http.MethodGet, "/api/v1/shared/mit-scl/recordings", nil))
	if list.Code != http.StatusOK {
		t.Fatalf("list status=%d body=%s", list.Code, list.Body.String())
	}
	var payload struct {
		Recordings []sharedRecording `json:"recordings"`
	}
	if err := json.Unmarshal(list.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if len(payload.Recordings) != 2 {
		t.Fatalf("list returned %d recordings, want active+paused only: %s", len(payload.Recordings), list.Body.String())
	}
	seen := map[string]bool{}
	for _, recording := range payload.Recordings {
		seen[recording.Status] = true
	}
	if !seen["active"] || !seen["paused"] || seen["completed"] || seen["canceled"] {
		t.Fatalf("visible statuses=%v, want active and paused only", seen)
	}

	for _, status := range []string{"active", "paused", "completed", "canceled"} {
		response := httptest.NewRecorder()
		path := "/api/v1/shared/mit-scl/recordings/" + strconv.FormatInt(ids[status], 10)
		router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		want := http.StatusOK
		if status == "completed" || status == "canceled" {
			want = http.StatusNotFound
		}
		if response.Code != want {
			t.Fatalf("%s detail status=%d want=%d body=%s", status, response.Code, want, response.Body.String())
		}
	}
}

func TestSharedRecordingsPageDisabledWithoutConfiguration(t *testing.T) {
	s := &Server{cfg: config.Config{}, recordingsHTML: []byte("secret page")}
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
		if body := rec.Body.String(); !strings.Contains(body, `const sharedRouteMatch =`) || !strings.Contains(body, `id="cards"`) {
			t.Fatalf("configured slug %q did not receive the standard recordings UI", slug)
		}
		detail := httptest.NewRecorder()
		router.ServeHTTP(detail, httptest.NewRequest(http.MethodGet, "/shared/"+slug+"/recordings/7", nil))
		if detail.Code != http.StatusOK {
			t.Fatalf("configured slug %q detail status=%d", slug, detail.Code)
		}
	}
}

func TestPublicSharedRecordingsNeedsNoCookie(t *testing.T) {
	s := &Server{cfg: config.Config{SharedRecordingsAccountID: 47, SharedRecordingsPublic: true}}
	called := false
	handler := s.requireSharedRecordingsAuth(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(http.StatusNoContent)
	}))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/shared/mit-scl/recordings", nil))
	if !called || rec.Code != http.StatusNoContent {
		t.Fatalf("public read called=%v status=%d", called, rec.Code)
	}
}

func TestPublicSharedClipsAreTenantScopedAndRedacted(t *testing.T) {
	pool, cleanup := testAccountClipsPool(t)
	defer cleanup()
	ctx := context.Background()
	var ownerRecordingID, foreignRecordingID int64
	for _, target := range []struct {
		accountID int64
		name      string
		out       *int64
	}{{47, "MIT recording", &ownerRecordingID}, {99, "Private recording", &foreignRecordingID}} {
		if err := pool.QueryRow(ctx, `INSERT INTO recordings(account_id,name) VALUES($1,$2) RETURNING id`, target.accountID, target.name).Scan(target.out); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO recording_clips(recording_id,size_bytes,duration_ms,actual_fps,clip_start_at,clip_end_at,display_path)
		VALUES($1,123456,60000,29.97,now()-interval '1 minute',now(),'private/object-name.mp4')
	`, ownerRecordingID); err != nil {
		t.Fatal(err)
	}
	s := &Server{pool: pool, cfg: config.Config{
		SharedRecordingsAccountID: 47, SharedRecordingsSlug: "mit-scl", SharedRecordingsPublic: true,
	}}
	rec := httptest.NewRecorder()
	s.router().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/shared/mit-scl/recordings/"+strconv.FormatInt(ownerRecordingID, 10)+"/clips", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("owner clips status=%d body=%s", rec.Code, rec.Body.String())
	}
	for _, forbidden := range []string{"object_key", "display_path", "storage_destination", "capture_generation", "capture_sequence", "private/object-name.mp4"} {
		if strings.Contains(rec.Body.String(), forbidden) {
			t.Fatalf("public clip response leaked %q: %s", forbidden, rec.Body.String())
		}
	}
	foreign := httptest.NewRecorder()
	s.router().ServeHTTP(foreign, httptest.NewRequest(http.MethodGet, "/api/v1/shared/mit-scl/recordings/"+strconv.FormatInt(foreignRecordingID, 10)+"/clips", nil))
	if foreign.Code != http.StatusNotFound {
		t.Fatalf("foreign clips status=%d want 404 body=%s", foreign.Code, foreign.Body.String())
	}
}

func TestPublicSharedClipDownloadIsTenantScopedAndNeedsNoCookie(t *testing.T) {
	pool, cleanup := testAccountClipsPool(t)
	defer cleanup()
	secrets, err := secretbox.NewFromBase64Key("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := secrets.Encrypt([]byte("secret"))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	var destinationID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO storage_destinations(account_id,secret_access_key_enc) VALUES(47,$1) RETURNING id
	`, sealed).Scan(&destinationID); err != nil {
		t.Fatal(err)
	}
	insertRecording := func(accountID int64, name string) int64 {
		t.Helper()
		var id int64
		if err := pool.QueryRow(ctx, `INSERT INTO recordings(account_id,name) VALUES($1,$2) RETURNING id`, accountID, name).Scan(&id); err != nil {
			t.Fatal(err)
		}
		return id
	}
	ownerRecordingID := insertRecording(47, "MIT recording")
	foreignRecordingID := insertRecording(99, "Private recording")
	insertClip := func(recordingID int64) int64 {
		t.Helper()
		var id int64
		if err := pool.QueryRow(ctx, `
			INSERT INTO recording_clips(recording_id,size_bytes,clip_start_at,clip_end_at,storage_destination_id)
			VALUES($1,123456,now()-interval '1 minute',now(),$2) RETURNING id
		`, recordingID, destinationID).Scan(&id); err != nil {
			t.Fatal(err)
		}
		return id
	}
	ownerClipID := insertClip(ownerRecordingID)
	foreignClipID := insertClip(foreignRecordingID)
	s := &Server{pool: pool, secrets: secrets, cfg: config.Config{
		SharedRecordingsAccountID: 47, SharedRecordingsSlug: "mit-scl", SharedRecordingsPublic: true,
		R2SignGetTTL: time.Minute,
	}}
	get := func(recordingID, clipID int64) *httptest.ResponseRecorder {
		t.Helper()
		path := "/api/v1/shared/mit-scl/recordings/" + strconv.FormatInt(recordingID, 10) + "/clips/" + strconv.FormatInt(clipID, 10) + "/download?disposition=inline"
		response := httptest.NewRecorder()
		s.router().ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		return response
	}
	owner := get(ownerRecordingID, ownerClipID)
	if owner.Code != http.StatusOK || !strings.Contains(owner.Body.String(), `"url":"https://`) || strings.Contains(owner.Body.String(), "object_key") {
		t.Fatalf("owner download status=%d body=%s", owner.Code, owner.Body.String())
	}
	foreign := get(foreignRecordingID, foreignClipID)
	if foreign.Code != http.StatusNotFound {
		t.Fatalf("foreign download status=%d want 404 body=%s", foreign.Code, foreign.Body.String())
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
	body, err := loadRecordingsHTML()
	if err != nil {
		t.Fatal(err)
	}
	page := string(body)
	for _, marker := range []string{
		`role="tooltip"`,
		`data-health-tooltip`,
		`pointerover`,
		`focusin`,
		`const sharedReadOnly = Boolean(sharedRouteMatch);`,
		`function recordingAPIPath`,
		`sharedReadOnly ? '' :`,
		`/clips?limit=`,
		`data-clipdownload`,
		`Array.from({ length: 24 }, () => [])`,
		`timeZoneName: 'short'`,
	} {
		if !strings.Contains(page, marker) {
			t.Fatalf("shared recordings page missing %q", marker)
		}
	}
}
