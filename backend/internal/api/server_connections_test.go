package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

// TestPullPathAllowed asserts the pure allowlist that confines a NAS pull key.
// Default is DENY: only the 4 pull shapes (right method + path) pass.
func TestPullPathAllowed(t *testing.T) {
	cases := []struct {
		method string
		path   string
		want   bool
	}{
		// The pull endpoints: list + heartbeat + inventory + download + release.
		{http.MethodGet, "/api/v1/account/clips", true},
		{http.MethodPost, "/api/v1/account/connections/heartbeat", true},
		{http.MethodPost, "/api/v1/account/connections/inventory", true},
		{http.MethodPost, "/api/v1/account/clips/release", true},
		{http.MethodGet, "/api/v1/account/recordings/12/clips/34/download", true},
		{http.MethodPost, "/api/v1/account/recordings/12/clips/34/release", true},

		// Wrong method on a pull path.
		{http.MethodPost, "/api/v1/account/clips", false},
		{http.MethodGet, "/api/v1/account/connections/heartbeat", false},
		{http.MethodDelete, "/api/v1/account/recordings/12/clips/34/download", false},
		{http.MethodGet, "/api/v1/account/recordings/12/clips/34/release", false},

		// Hard-delete is NO LONGER allowed for a pull key: it can release, not destroy.
		{http.MethodDelete, "/api/v1/account/recordings/12/clips/34", false},
		// Bulk delete-all (no clipId) must NOT pass: a pull key cannot wipe a recording.
		{http.MethodDelete, "/api/v1/account/recordings/12/clips", false},

		// Non-numeric params must not slip through the anchored regexps.
		{http.MethodGet, "/api/v1/account/recordings/x/clips/34/download", false},
		{http.MethodPost, "/api/v1/account/recordings/12/clips/abc/release", false},
		{http.MethodGet, "/api/v1/account/recordings/12/clips/34/download/extra", false},

		// A sampling of management/data routes that must be denied to a pull key.
		{http.MethodPost, "/api/v1/account/api-keys", false},
		{http.MethodPost, "/api/v1/account/connections", false},
		{http.MethodGet, "/api/v1/account/connections", false},
		{http.MethodGet, "/api/v1/account/billing", false},
		{http.MethodPost, "/api/v1/account/recordings", false},
		{http.MethodGet, "/api/v1/account/members", false},
		{http.MethodGet, "/api/v1/account/me", false},
	}
	for _, c := range cases {
		if got := pullPathAllowed(c.method, c.path); got != c.want {
			t.Errorf("pullPathAllowed(%s %s)=%v want %v", c.method, c.path, got, c.want)
		}
	}
}

// runConfine drives confineAccountScope around a sentinel handler with the given
// principal in context, returning the status code (200 = passed through).
func runConfine(p accountPrincipal, method, path string) int {
	s := &Server{}
	called := false
	h := s.confineAccountScope(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	}))
	req := httptest.NewRequest(method, path, nil)
	req = req.WithContext(context.WithValue(req.Context(), accountPrincipalContextKey, p))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code == http.StatusOK && !called {
		return -1
	}
	return rec.Code
}

func TestConfineAccountScopePullKeyConfined(t *testing.T) {
	keyID := int64(99)
	pull := accountPrincipal{AccountID: 7, AuthType: "api_key", APIKeyID: &keyID, KeyScopes: []string{accountScopePull}}

	// 200 on all pull endpoints.
	pullPaths := []struct {
		method, path string
	}{
		{http.MethodGet, "/api/v1/account/clips"},
		{http.MethodPost, "/api/v1/account/connections/heartbeat"},
		{http.MethodPost, "/api/v1/account/connections/inventory"},
		{http.MethodPost, "/api/v1/account/clips/release"},
		{http.MethodGet, "/api/v1/account/recordings/12/clips/34/download"},
		{http.MethodPost, "/api/v1/account/recordings/12/clips/34/release"},
	}
	for _, p := range pullPaths {
		if code := runConfine(pull, p.method, p.path); code != http.StatusOK {
			t.Errorf("pull key on %s %s = %d, want 200", p.method, p.path, code)
		}
	}

	// 403 on a sampling of non-pull endpoints, incl. the removed hard-delete paths.
	denyPaths := []struct {
		method, path string
	}{
		{http.MethodPost, "/api/v1/account/api-keys"},
		{http.MethodGet, "/api/v1/account/billing"},
		{http.MethodPost, "/api/v1/account/recordings"},
		{http.MethodPost, "/api/v1/account/connections"},
		{http.MethodDelete, "/api/v1/account/recordings/12/clips/34"},
		{http.MethodDelete, "/api/v1/account/recordings/5/clips"},
		{http.MethodGet, "/api/v1/account/me"},
	}
	for _, p := range denyPaths {
		if code := runConfine(pull, p.method, p.path); code != http.StatusForbidden {
			t.Errorf("pull key on %s %s = %d, want 403", p.method, p.path, code)
		}
	}
}

func TestConfineAccountScopeFullKeyAndSessionUnaffected(t *testing.T) {
	keyID := int64(5)
	full := accountPrincipal{AccountID: 7, AuthType: "api_key", APIKeyID: &keyID, KeyScopes: []string{accountScopeRead}}
	sessionID := int64(3)
	session := accountPrincipal{AccountID: 7, AuthType: "session", SessionID: &sessionID}

	paths := []struct {
		method, path string
	}{
		{http.MethodPost, "/api/v1/account/api-keys"},
		{http.MethodGet, "/api/v1/account/billing"},
		{http.MethodGet, "/api/v1/account/clips"},
		{http.MethodPost, "/api/v1/account/connections"},
		{http.MethodGet, "/api/v1/account/me"},
	}
	for _, principal := range []accountPrincipal{full, session} {
		for _, p := range paths {
			if code := runConfine(principal, p.method, p.path); code != http.StatusOK {
				t.Errorf("%s on %s %s = %d, want 200 (unaffected)", principal.AuthType, p.method, p.path, code)
			}
		}
	}
}

func TestClampPollIntervalSec(t *testing.T) {
	cases := map[int]int{0: 60, 5: 10, 10: 10, 90: 90, 3600: 3600, 9000: 3600, -1: 10}
	for in, want := range cases {
		if got := clampPollIntervalSec(in); got != want {
			t.Errorf("clampPollIntervalSec(%d)=%d want %d", in, got, want)
		}
	}
}

func TestConnectionListItemJSONContract(t *testing.T) {
	item := connectionListItem{
		ID:                 13,
		Health:             connectionHealthHealthy,
		ClientPhase:        "draining",
		PendingClips:       42,
		PendingBytes:       1024,
		LastCursorID:       99,
		ClientVersion:      "release",
		NASDownloadWorkers: 12,
	}
	body, err := json.Marshal(item)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`"health":"healthy"`,
		`"client_phase":"draining"`,
		`"pending_clips":42`,
		`"pending_bytes":1024`,
		`"last_cursor_id":99`,
		`"client_version":"release"`,
		`"nas_download_workers":12`,
	} {
		if !strings.Contains(string(body), want) {
			t.Errorf("connection JSON missing %s: %s", want, body)
		}
	}
}

func TestConnectionPendingClipsEligibility(t *testing.T) {
	pool, cleanup := testAccountClipsPool(t)
	defer cleanup()
	ctx := context.Background()
	const (
		accountID        = int64(47)
		foreignAccountID = int64(48)
	)

	insertRecording := func(accountID int64, delivery string) int64 {
		t.Helper()
		var id int64
		if err := pool.QueryRow(ctx, `
			INSERT INTO recordings (account_id, name, delivery)
			VALUES ($1, 'queue-test', $2)
			RETURNING id
		`, accountID, delivery).Scan(&id); err != nil {
			t.Fatalf("insert recording: %v", err)
		}
		return id
	}
	ownerNAS := insertRecording(accountID, "nas_pull")
	ownerManaged := insertRecording(accountID, "managed")
	foreignNAS := insertRecording(foreignAccountID, "nas_pull")

	insertClip := func(recordingID, size int64, createdAt time.Time, purged, released bool) int64 {
		t.Helper()
		var id int64
		var purgedAt, releasedAt any
		if purged {
			purgedAt = createdAt
		}
		if released {
			releasedAt = createdAt
		}
		if err := pool.QueryRow(ctx, `
			INSERT INTO recording_clips
				(recording_id, size_bytes, clip_start_at, clip_end_at, created_at, purged_at, released_at)
			VALUES ($1, $2, $3, $3, $3, $4, $5)
			RETURNING id
		`, recordingID, size, createdAt, purgedAt, releasedAt).Scan(&id); err != nil {
			t.Fatalf("insert clip: %v", err)
		}
		return id
	}
	old := time.Now().UTC().Add(-10 * time.Minute)
	belowCursor := insertClip(ownerNAS, 1, old, false, false)
	wanted := insertClip(ownerNAS, 2, old, false, false)
	insertClip(ownerNAS, 4, old, true, false)
	insertClip(ownerNAS, 8, old, false, true)
	insertClip(ownerNAS, 16, time.Now().UTC(), false, false)
	insertClip(ownerManaged, 32, old, false, false)
	insertClip(foreignNAS, 64, old, false, false)

	if _, err := pool.Exec(ctx, `
		INSERT INTO connections (account_id, kind, last_cursor_id)
		VALUES ($1, 'nas_pull', $2), ($1, 'nas_pull', $3), ($4, 'nas_pull', 0)
	`, accountID, belowCursor, wanted, foreignAccountID); err != nil {
		t.Fatalf("insert connections: %v", err)
	}

	rows, err := pool.Query(ctx, `
		SELECT conn.last_cursor_id, pending.clips, pending.bytes, pending.oldest_at
		FROM connections conn
		`+connectionPendingLateralSQL+`
		WHERE conn.account_id=$1
		ORDER BY conn.last_cursor_id
	`, accountID)
	if err != nil {
		t.Fatalf("summarize pending clips: %v", err)
	}
	defer rows.Close()
	type summary struct {
		cursor, clips, bytes int64
		oldest               *time.Time
	}
	var got []summary
	for rows.Next() {
		var item summary
		if err := rows.Scan(&item.cursor, &item.clips, &item.bytes, &item.oldest); err != nil {
			t.Fatalf("scan pending summary: %v", err)
		}
		got = append(got, item)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("pending summaries = %v, want two connections", got)
	}
	if got[0].cursor != belowCursor || got[0].clips != 1 || got[0].bytes != 2 || got[0].oldest == nil {
		t.Fatalf("first pending summary = %+v, want cursor %d with one 2-byte clip", got[0], belowCursor)
	}
	if got[1].cursor != wanted || got[1].clips != 0 || got[1].bytes != 0 || got[1].oldest != nil {
		t.Fatalf("second pending summary = %+v, want cursor %d with no clips", got[1], wanted)
	}
}

func TestIsPullScopedPrincipal(t *testing.T) {
	keyID := int64(1)
	if isPullScopedPrincipal(accountPrincipal{SessionID: &keyID}) {
		t.Error("session principal must not be pull-scoped")
	}
	if isPullScopedPrincipal(accountPrincipal{APIKeyID: &keyID, KeyScopes: []string{accountScopeRead}}) {
		t.Error("read key must not be pull-scoped")
	}
	if !isPullScopedPrincipal(accountPrincipal{APIKeyID: &keyID, KeyScopes: []string{accountScopePull}}) {
		t.Error("pull key must be pull-scoped")
	}
}

func TestConnectionComposeUsesDurableClientLauncher(t *testing.T) {
	compose := connectionComposeSnippet(connectionPublicAPIBase, "sir_test", 27, 60)
	for _, want := range []string{
		nasPythonImage,
		`STOARAMA_CONNECTION_ID: "27"`,
		`STOARAMA_STATE_DIR: "/state"`,
		`https://stoarama.com/nas/download/latest.json`,
		nasBootstrapURL,
		nasBootstrapSHA256,
		`NAS bootstrap checksum mismatch`,
		`cached NAS bootstrap checksum mismatch`,
		`os.replace(temporary,p)`,
		`exec(compile(source`,
	} {
		if !strings.Contains(compose, want) {
			t.Errorf("compose missing %q", want)
		}
	}
	for _, forbidden := range []string{"raw.githubusercontent.com", "python:3-slim\n", "command: |"} {
		if strings.Contains(compose, forbidden) {
			t.Errorf("compose contains unsafe mutable dependency %q", forbidden)
		}
	}
}

func TestCheckedInNASComposeUsesGeneratedLauncher(t *testing.T) {
	compose, err := os.ReadFile("../../../clients/nas-pull/docker-compose.yml")
	if err != nil {
		t.Fatal(err)
	}
	want := fmt.Sprintf(`command: ["python3", "-c", %q]`, nasLaunchCommand)
	if !strings.Contains(string(compose), want) {
		t.Fatal("checked-in NAS compose launcher differs from generated launcher")
	}
}

func testNASLauncherCommand(state, url string, bootstrap []byte) string {
	sum := sha256.Sum256(bootstrap)
	return strings.NewReplacer(
		"/state", filepath.ToSlash(state),
		nasBootstrapURL, url,
		nasBootstrapSHA256, hex.EncodeToString(sum[:]),
	).Replace(nasLaunchCommand)
}

func TestNASLauncherDownloadsAndCachesVerifiedBootstrap(t *testing.T) {
	state := t.TempDir()
	bootstrap := []byte("pass\n")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(bootstrap)
	}))
	defer server.Close()
	command := testNASLauncherCommand(state, server.URL, bootstrap)
	if output, err := exec.Command("python3", "-c", command).CombinedOutput(); err != nil {
		t.Fatalf("online bootstrap failed: %v (%s)", err, output)
	}
	got, err := os.ReadFile(filepath.Join(state, "stoarama-bootstrap-v1.py"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, bootstrap) {
		t.Fatalf("cached bootstrap = %q, want %q", got, bootstrap)
	}
}

func TestNASLauncherUsesVerifiedCacheWhenDownloadIsUnavailable(t *testing.T) {
	state := t.TempDir()
	bootstrap := []byte("pass\n")
	command := testNASLauncherCommand(state, "http://127.0.0.1:1/unavailable", bootstrap)
	if err := os.WriteFile(filepath.Join(state, "stoarama-bootstrap-v1.py"), bootstrap, 0o600); err != nil {
		t.Fatal(err)
	}
	if output, err := exec.Command("python3", "-c", command).CombinedOutput(); err != nil {
		t.Fatalf("offline cached bootstrap failed: %v (%s)", err, output)
	}
}

func TestValidateConnectionHeartbeat(t *testing.T) {
	expectedSkipReasons := map[string]bool{
		"changed_during_hash": true, "invalid_sidecar": true, "io_error": true,
		"permission_denied": true, "unexpected": true, "vanished_during_scan": true,
	}
	if !reflect.DeepEqual(inventorySkipReasons, expectedSkipReasons) {
		t.Fatalf("inventory skip reason contract drifted: %#v", inventorySkipReasons)
	}
	now := time.Now().UTC()
	future := now.Add(connectionHeartbeatFutureSkew + time.Minute)
	valid := connectionHeartbeatRequest{
		CursorID:           8,
		ClipsPulled:        5,
		BytesPulled:        1024,
		ClientVersion:      "2026.07.22-abc12345",
		ClientStartedAt:    &now,
		ClientBootID:       "boot-id",
		ClientPhase:        "draining",
		ClientPreviousExit: "clean",
		LastBatch: connectionHeartbeatBatch{
			CompletedAt: &now,
			Clips:       200,
			Bytes:       1024,
			DurationMS:  5000,
			Workers:     12,
			Retries:     1,
		},
		LastOutage: &connectionHeartbeatOutage{
			Class:        "dns_failed",
			StartedAt:    &now,
			FailureCount: 3,
		},
		Inventory: &connectionInventoryStatus{
			Generation: "scan-20260806-abcd1234", ScanStartedAt: &now,
			ScanCompletedAt: &now, Clips: 100, Bytes: 2048,
			Digest: strings.Repeat("a", 64),
		},
	}
	if err := validateConnectionHeartbeat(valid); err != nil {
		t.Fatalf("valid heartbeat rejected: %v", err)
	}
	starting := connectionHeartbeatRequest{
		ClientVersion:      "v1",
		ClientPhase:        "starting",
		ClientPreviousExit: "clean",
		LastBatch:          connectionHeartbeatBatch{Workers: 12},
	}
	if err := validateConnectionHeartbeat(starting); err != nil {
		t.Fatalf("pre-batch worker telemetry rejected: %v", err)
	}
	legacy := connectionHeartbeatRequest{CursorID: 1, ClipsPulled: 1}
	if err := validateConnectionHeartbeat(legacy); err != nil {
		t.Fatalf("legacy heartbeat rejected during rollout: %v", err)
	}
	afterCompleted := now.Add(time.Second)
	invalid := []connectionHeartbeatRequest{
		{CursorID: -1},
		{LastBatch: connectionHeartbeatBatch{CompletedAt: &now, Workers: 12}},
		{ClientVersion: "bad/version"},
		{ClientVersion: "v1", ClientPhase: "running", ClientPreviousExit: "clean"},
		{ClientVersion: "v1", ClientPhase: "idle", ClientPreviousExit: "panic"},
		{ClientVersion: "v1", ClientPhase: "idle", ClientPreviousExit: "clean", LastOutage: &connectionHeartbeatOutage{Class: "dns_failed"}},
		{ClientVersion: "v1", ClientPhase: "idle", ClientPreviousExit: "clean", LastBatch: connectionHeartbeatBatch{CompletedAt: &now, Workers: 12}},
		{ClientVersion: "v1", ClientPhase: "idle", ClientPreviousExit: "clean", LastBatch: connectionHeartbeatBatch{Workers: 33}},
		{ClientVersion: "v1", ClientPhase: "idle", ClientPreviousExit: "clean", LastBatch: connectionHeartbeatBatch{CompletedAt: &future, DurationMS: 1, Workers: 1}},
		{ClientVersion: "v1", ClientPhase: "idle", ClientPreviousExit: "clean", Inventory: &connectionInventoryStatus{Generation: "bad/generation"}},
		{ClientVersion: "v1", ClientPhase: "idle", ClientPreviousExit: "clean", Inventory: &connectionInventoryStatus{Generation: "scan", Digest: "not-a-digest"}},
		{ClientVersion: "v1", ClientPhase: "idle", ClientPreviousExit: "clean", Inventory: &connectionInventoryStatus{Generation: "scan", ScanCompletedAt: &now}},
		{ClientVersion: "v1", ClientPhase: "idle", ClientPreviousExit: "clean", Inventory: &connectionInventoryStatus{Generation: "scan", ScanStartedAt: &now, ScanCompletedAt: &now, ScanPassStartedAt: &afterCompleted, Digest: strings.Repeat("a", 64)}},
		{ClientVersion: "v1", ClientPhase: "idle", ClientPreviousExit: "clean", Inventory: &connectionInventoryStatus{Generation: "scan", ScanRowsVisited: -1}},
		{ClientVersion: "v1", ClientPhase: "idle", ClientPreviousExit: "clean", Inventory: &connectionInventoryStatus{Generation: "scan", ScanRowsSkipped: -1}},
		{ClientVersion: "v1", ClientPhase: "idle", ClientPreviousExit: "clean", Inventory: &connectionInventoryStatus{Generation: "scan", ScanRowsVisited: 1}},
		{ClientVersion: "v1", ClientPhase: "idle", ClientPreviousExit: "clean", Inventory: &connectionInventoryStatus{Generation: "scan", ScanPassStartedAt: &now, ScanRowsSkipped: 1, ScanSkipReasons: map[string]int64{"unknown": 1}}},
		{ClientVersion: "v1", ClientPhase: "idle", ClientPreviousExit: "clean", Inventory: &connectionInventoryStatus{Generation: "scan", ScanPassStartedAt: &now, ScanRowsSkipped: 2, ScanSkipReasons: map[string]int64{"io_error": 1}}},
		{Storage: &connectionStorageStatus{Available: true}},
		{Storage: &connectionStorageStatus{Available: true, TotalBytes: 100, FreeBytes: -1}},
		{Storage: &connectionStorageStatus{Available: true, TotalBytes: 100, FreeBytes: 101}},
		{Storage: &connectionStorageStatus{Available: false, TotalBytes: 100}},
	}
	for i, request := range invalid {
		if err := validateConnectionHeartbeat(request); err == nil {
			t.Errorf("invalid heartbeat %d accepted: %+v", i, request)
		}
	}
}

func TestValidateConnectionHeartbeatStorage(t *testing.T) {
	request := connectionHeartbeatRequest{Storage: &connectionStorageStatus{Available: true, TotalBytes: 1000, FreeBytes: 250}}
	if err := validateConnectionHeartbeat(request); err != nil {
		t.Fatalf("valid storage telemetry rejected: %v", err)
	}
}

func TestConnectionHeartbeatStoragePersistenceAndList(t *testing.T) {
	pool, cleanup := testAccountClipsPool(t)
	defer cleanup()
	ctx := context.Background()
	const accountID, apiKeyID = int64(47), int64(123)
	if _, err := pool.Exec(ctx, `INSERT INTO connections(account_id,kind,api_key_id) VALUES($1,'nas_pull',$2)`, accountID, apiKeyID); err != nil {
		t.Fatal(err)
	}
	s := &Server{pool: pool}
	call := func(storage *connectionStorageStatus) {
		body, err := json.Marshal(connectionHeartbeatRequest{Storage: storage})
		if err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest(http.MethodPost, "/api/v1/account/connections/heartbeat", bytes.NewReader(body))
		req = req.WithContext(context.WithValue(req.Context(), accountPrincipalContextKey, accountPrincipal{AccountID: accountID, APIKeyID: ptrInt64(apiKeyID)}))
		rec := httptest.NewRecorder()
		s.handleAccountConnectionHeartbeat(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("heartbeat status=%d body=%s", rec.Code, rec.Body.String())
		}
	}
	call(&connectionStorageStatus{Available: true, TotalBytes: 1000, FreeBytes: 250})
	var total, free *int64
	var reportedAt *time.Time
	if err := pool.QueryRow(ctx, `SELECT nas_storage_total_bytes,nas_storage_free_bytes,nas_storage_reported_at FROM connections WHERE api_key_id=$1`, apiKeyID).Scan(&total, &free, &reportedAt); err != nil {
		t.Fatal(err)
	}
	if total == nil || *total != 1000 || free == nil || *free != 250 || reportedAt == nil {
		t.Fatalf("persisted storage total=%v free=%v reported=%v", total, free, reportedAt)
	}
	storedReportedAt := *reportedAt
	call(nil)
	if err := pool.QueryRow(ctx, `SELECT nas_storage_total_bytes,nas_storage_free_bytes,nas_storage_reported_at FROM connections WHERE api_key_id=$1`, apiKeyID).Scan(&total, &free, &reportedAt); err != nil {
		t.Fatal(err)
	}
	if total == nil || *total != 1000 || free == nil || *free != 250 || reportedAt == nil || !reportedAt.Equal(storedReportedAt) {
		t.Fatalf("omitted storage did not preserve values total=%v free=%v reported=%v want_reported=%v", total, free, reportedAt, storedReportedAt)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/account/connections", nil)
	req = req.WithContext(context.WithValue(req.Context(), accountPrincipalContextKey, accountPrincipal{AccountID: accountID}))
	rec := httptest.NewRecorder()
	s.handleAccountConnectionsList(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("list status=%d body=%s", rec.Code, rec.Body.String())
	}
	for _, want := range []string{`"nas_storage_total_bytes":1000`, `"nas_storage_free_bytes":250`, `"nas_storage_reported_at":"`} {
		if !strings.Contains(rec.Body.String(), want) {
			t.Errorf("connection list missing %s: %s", want, rec.Body.String())
		}
	}

	call(&connectionStorageStatus{Available: false})
	if err := pool.QueryRow(ctx, `SELECT nas_storage_total_bytes,nas_storage_free_bytes,nas_storage_reported_at FROM connections WHERE api_key_id=$1`, apiKeyID).Scan(&total, &free, &reportedAt); err != nil {
		t.Fatal(err)
	}
	if total != nil || free != nil || reportedAt != nil {
		t.Fatalf("unavailable storage was not cleared total=%v free=%v reported=%v", total, free, reportedAt)
	}
	if _, err := pool.Exec(ctx, `UPDATE connections SET nas_storage_total_bytes=100,nas_storage_free_bytes=NULL,nas_storage_reported_at=NULL WHERE api_key_id=$1`, apiKeyID); err == nil {
		t.Fatal("partial-null NAS storage row accepted")
	}
}

func TestInventoryHeartbeatDoesNotRegressCompletedSummary(t *testing.T) {
	pool, cleanup := testAccountClipsPool(t)
	defer cleanup()
	ctx := context.Background()
	const accountID, apiKeyID = int64(42), int64(123)
	if _, err := pool.Exec(ctx, `INSERT INTO connections(account_id,kind,api_key_id) VALUES($1,'nas_pull',$2)`, accountID, apiKeyID); err != nil {
		t.Fatal(err)
	}
	s := &Server{pool: pool}
	call := func(inv connectionInventoryStatus) {
		payload, err := json.Marshal(connectionHeartbeatRequest{Inventory: &inv})
		if err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest(http.MethodPost, "/api/v1/account/connections/heartbeat", bytes.NewReader(payload))
		req = req.WithContext(context.WithValue(req.Context(), accountPrincipalContextKey, accountPrincipal{AccountID: accountID, APIKeyID: ptrInt64(apiKeyID)}))
		rec := httptest.NewRecorder()
		s.handleAccountConnectionHeartbeat(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("heartbeat status=%d body=%s", rec.Code, rec.Body.String())
		}
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	start := now.Add(-time.Hour)
	completed := now.Add(-time.Minute)
	call(connectionInventoryStatus{Generation: "complete", ScanStartedAt: &start, ScanCompletedAt: &completed, Clips: 10, Bytes: 1000, Mismatches: 7, Unmatched: 2, Digest: strings.Repeat("a", 64)})
	call(connectionInventoryStatus{Generation: "same-time-conflict", ScanStartedAt: &start, ScanCompletedAt: &completed, Clips: 1, Bytes: 1, Mismatches: 0, Unmatched: 0, Digest: strings.Repeat("c", 64)})
	progressStarted := now.Add(-time.Minute)
	passStarted := now.Add(-30 * time.Second)
	call(connectionInventoryStatus{Generation: "in-progress", ScanStartedAt: &progressStarted, ScanPassStartedAt: &passStarted, ScanRowsVisited: 12345, ScanRowsSkipped: 2, ScanSkipReasons: map[string]int64{"invalid_sidecar": 1, "permission_denied": 1}, Clips: 1, Bytes: 1, Mismatches: 0, Unmatched: 0})
	call(connectionInventoryStatus{Generation: "in-progress", ScanStartedAt: &progressStarted, ScanPassStartedAt: &passStarted, ScanRowsVisited: 12000, ScanRowsSkipped: 1, ScanSkipReasons: map[string]int64{"invalid_sidecar": 1}})
	// A legacy client may omit reason telemetry while this pass is still running;
	// it must not erase the newer client's reason counts.
	call(connectionInventoryStatus{Generation: "in-progress", ScanStartedAt: &progressStarted, ScanPassStartedAt: &passStarted, ScanRowsVisited: 12346, ScanRowsSkipped: 2})
	var preservedReasons []byte
	if err := pool.QueryRow(ctx, `SELECT inventory_scan_skip_reasons FROM connections WHERE api_key_id=$1`, apiKeyID).Scan(&preservedReasons); err != nil {
		t.Fatal(err)
	}
	var preservedReasonCounts map[string]int64
	if err := json.Unmarshal(preservedReasons, &preservedReasonCounts); err != nil {
		t.Fatal(err)
	}
	if preservedReasonCounts["invalid_sidecar"] != 1 || preservedReasonCounts["permission_denied"] != 1 {
		t.Fatalf("same-count legacy heartbeat erased reasons: %s", preservedReasons)
	}
	// If a legacy client observes another skip, retaining the old map would make
	// its total contradict the authoritative skipped-row count. Clear only the
	// unavailable reason detail while preserving the larger aggregate.
	call(connectionInventoryStatus{Generation: "in-progress", ScanStartedAt: &progressStarted, ScanPassStartedAt: &passStarted, ScanRowsVisited: 12347, ScanRowsSkipped: 3})
	delayed := now.Add(-2 * time.Minute)
	call(connectionInventoryStatus{Generation: "delayed", ScanStartedAt: &start, ScanCompletedAt: &delayed, Clips: 2, Bytes: 2, Mismatches: 0, Unmatched: 0, Digest: strings.Repeat("b", 64)})
	var clips, bytesValue, mismatches, unmatched int64
	var digest string
	var storedCompleted time.Time
	if err := pool.QueryRow(ctx, `SELECT inventory_clips,inventory_bytes,inventory_mismatches,inventory_unmatched,inventory_digest,inventory_scan_completed_at FROM connections WHERE api_key_id=$1`, apiKeyID).Scan(&clips, &bytesValue, &mismatches, &unmatched, &digest, &storedCompleted); err != nil {
		t.Fatal(err)
	}
	if clips != 10 || bytesValue != 1000 || mismatches != 7 || unmatched != 2 || digest != strings.Repeat("a", 64) || !storedCompleted.Equal(completed) {
		t.Fatalf("completed summary regressed clips=%d bytes=%d mismatches=%d unmatched=%d digest=%q completed=%s", clips, bytesValue, mismatches, unmatched, digest, storedCompleted)
	}
	var storedPass time.Time
	var visited, skipped int64
	var reasons []byte
	if err := pool.QueryRow(ctx, `SELECT inventory_scan_pass_started_at,inventory_scan_rows_visited,inventory_scan_rows_skipped,inventory_scan_skip_reasons FROM connections WHERE api_key_id=$1`, apiKeyID).Scan(&storedPass, &visited, &skipped, &reasons); err != nil {
		t.Fatal(err)
	}
	var storedReasons map[string]int64
	if err := json.Unmarshal(reasons, &storedReasons); err != nil {
		t.Fatal(err)
	}
	if !storedPass.Equal(passStarted) || visited != 12347 || skipped != 3 || len(storedReasons) != 0 {
		t.Fatalf("scan progress pass=%s visited=%d skipped=%d reasons=%s", storedPass, visited, skipped, reasons)
	}
}
