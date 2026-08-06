package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestValidateNASInventorySync(t *testing.T) {
	now := time.Now().UTC()
	start := now.Add(-time.Minute)
	valid := nasInventorySyncRequest{
		Generation: "scan-20260806-abcd1234",
		Files: []nasInventoryFileReport{{
			ClipID: 1, RecordingID: 2, RelativePath: "recording/clip.mp4", SizeBytes: 3,
			SHA256: strings.Repeat("a", 64), State: "present", VerifiedAt: &now, ClientUpdatedAt: now,
		}},
	}
	if err := validateNASInventorySync(valid, now); err != nil {
		t.Fatalf("valid incremental inventory rejected: %v", err)
	}
	completed := valid
	completed.ScanStartedAt = &start
	completed.ScanCompletedAt = &now
	completed.Digest = strings.Repeat("b", 64)
	completed.Complete = true
	if err := validateNASInventorySync(completed, now); err != nil {
		t.Fatalf("valid completed inventory rejected: %v", err)
	}
	valid.Unmatched = []nasUnmatchedFileReport{{RelativePath: "unknown.mp4", SizeBytes: 4, SHA256: strings.Repeat("c", 64), State: "present", ClientUpdatedAt: now}}
	if err := validateNASInventorySync(valid, now); err != nil {
		t.Fatalf("valid unmatched inventory rejected: %v", err)
	}
	invalid := []nasInventorySyncRequest{
		{},
		{Generation: "bad/generation"},
		{Generation: "scan", Complete: true},
		{Generation: "scan", Files: []nasInventoryFileReport{{ClipID: 1, RecordingID: 2, RelativePath: "../clip.mp4", SHA256: strings.Repeat("a", 64), State: "present", VerifiedAt: &now, ClientUpdatedAt: now}}},
		{Generation: "scan", Files: []nasInventoryFileReport{{ClipID: 1, RecordingID: 2, RelativePath: "clip.mp4", SHA256: strings.Repeat("a", 64), State: "present", ClientUpdatedAt: now}}},
	}
	for i, request := range invalid {
		if err := validateNASInventorySync(request, now); err == nil {
			t.Errorf("invalid inventory %d accepted: %+v", i, request)
		}
	}
}

func TestValidNASRelativePath(t *testing.T) {
	for _, path := range []string{"", "/absolute", "../clip.mp4", "a/../clip.mp4", "a\\clip.mp4"} {
		if validNASRelativePath(path) {
			t.Errorf("unsafe path accepted: %q", path)
		}
	}
	if !validNASRelativePath("recording/day/clip.mp4") {
		t.Fatal("safe relative path rejected")
	}
}

func TestNASInventoryReleaseGateFailsClosed(t *testing.T) {
	pool, cleanup := testAccountClipsPool(t)
	defer cleanup()
	ctx := context.Background()
	const accountID, apiKeyID = int64(42), int64(77)
	var connectionID, recordingID, clipID int64
	if err := pool.QueryRow(ctx, `INSERT INTO connections(account_id,kind,api_key_id,inventory_mode) VALUES($1,'nas_pull',$2,'enforce') RETURNING id`, accountID, apiKeyID).Scan(&connectionID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `INSERT INTO recordings(account_id,name,delivery) VALUES($1,'safe','nas_pull') RETURNING id`, accountID).Scan(&recordingID); err != nil {
		t.Fatal(err)
	}
	sha := strings.Repeat("a", 64)
	if err := pool.QueryRow(ctx, `INSERT INTO recording_clips(recording_id,size_bytes,sha256,display_path,clip_start_at,clip_end_at) VALUES($1,3,$2,'safe/clip.mp4',now()-interval '1 minute',now()) RETURNING id`, recordingID, sha).Scan(&clipID); err != nil {
		t.Fatal(err)
	}
	s := &Server{pool: pool}
	principal := accountPrincipal{AccountID: accountID, APIKeyID: ptrInt64(apiKeyID)}
	r := httptest.NewRequest("POST", "/", nil)
	confirmed, _, err := s.verifyNASInventoryForRelease(r, principal, recordingID, clipID)
	if err != nil || confirmed {
		t.Fatalf("missing inventory confirmed=%v err=%v, want false/nil", confirmed, err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO nas_inventory_files(connection_id,clip_id,recording_id,relative_path,size_bytes,sha256,state,verified_at) VALUES($1,$2,$3,'safe/clip.mp4',3,$4,'present',now())`, connectionID, clipID, recordingID, sha); err != nil {
		t.Fatal(err)
	}
	confirmed, reason, err := s.verifyNASInventoryForRelease(r, principal, recordingID, clipID)
	if err != nil || !confirmed || reason != "confirmed" {
		t.Fatalf("exact inventory confirmed=%v reason=%q err=%v", confirmed, reason, err)
	}
	if _, err := pool.Exec(ctx, `UPDATE nas_inventory_files SET size_bytes=4 WHERE connection_id=$1 AND clip_id=$2`, connectionID, clipID); err != nil {
		t.Fatal(err)
	}
	confirmed, _, err = s.verifyNASInventoryForRelease(r, principal, recordingID, clipID)
	if err != nil || confirmed {
		t.Fatalf("mismatched inventory confirmed=%v err=%v, want false/nil", confirmed, err)
	}
}

func TestSessionReleaseRemainsAvailableInObserveRollout(t *testing.T) {
	pool, cleanup := testAccountClipsPool(t)
	defer cleanup()
	ctx := context.Background()
	const accountID = int64(42)
	var recordingID, clipID int64
	if err := pool.QueryRow(ctx, `INSERT INTO recordings(account_id,name,delivery) VALUES($1,'session-release','nas_pull') RETURNING id`, accountID).Scan(&recordingID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `INSERT INTO recording_clips(recording_id,size_bytes,clip_start_at,clip_end_at) VALUES($1,3,now()-interval '1 minute',now()) RETURNING id`, recordingID).Scan(&clipID); err != nil {
		t.Fatal(err)
	}
	s := &Server{pool: pool}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/account/recordings/x/clips/y/release", nil)
	req.SetPathValue("id", fmt.Sprint(recordingID))
	req.SetPathValue("clipId", fmt.Sprint(clipID))
	req = req.WithContext(context.WithValue(req.Context(), accountPrincipalContextKey, accountPrincipal{AccountID: accountID}))
	rec := httptest.NewRecorder()
	s.handleAccountRecordingClipRelease(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("session release status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestNASBatchReleaseIsAtomicAndInventoryGated(t *testing.T) {
	pool, cleanup := testAccountClipsPool(t)
	defer cleanup()
	ctx := context.Background()
	const accountID, apiKeyID = int64(42), int64(88)
	var connectionID, recordingID, firstID, secondID int64
	if err := pool.QueryRow(ctx, `INSERT INTO connections(account_id,kind,api_key_id,inventory_mode) VALUES($1,'nas_pull',$2,'enforce') RETURNING id`, accountID, apiKeyID).Scan(&connectionID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `INSERT INTO recordings(account_id,name,delivery) VALUES($1,'batch','nas_pull') RETURNING id`, accountID).Scan(&recordingID); err != nil {
		t.Fatal(err)
	}
	sha := strings.Repeat("a", 64)
	for index, target := range []*int64{&firstID, &secondID} {
		path := fmt.Sprintf("safe/%d.mp4", index+1)
		if err := pool.QueryRow(ctx, `INSERT INTO recording_clips(recording_id,size_bytes,sha256,display_path,clip_start_at,clip_end_at) VALUES($1,3,$2,$3,now()-interval '1 minute',now()) RETURNING id`, recordingID, sha, path).Scan(target); err != nil {
			t.Fatal(err)
		}
		if index == 0 {
			if _, err := pool.Exec(ctx, `INSERT INTO nas_inventory_files(connection_id,clip_id,recording_id,relative_path,size_bytes,sha256,state,verified_at) VALUES($1,$2,$3,$4,3,$5,'present',now())`, connectionID, *target, recordingID, path, sha); err != nil {
				t.Fatal(err)
			}
		}
	}
	s := &Server{pool: pool}
	call := func() *httptest.ResponseRecorder {
		body := fmt.Sprintf(`{"clips":[{"clip_id":%d,"recording_id":%d},{"clip_id":%d,"recording_id":%d}]}`, firstID, recordingID, secondID, recordingID)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/account/clips/release", bytes.NewBufferString(body))
		req = req.WithContext(context.WithValue(req.Context(), accountPrincipalContextKey, accountPrincipal{AccountID: accountID, APIKeyID: ptrInt64(apiKeyID)}))
		rec := httptest.NewRecorder()
		s.handleAccountClipsReleaseBatch(rec, req)
		return rec
	}
	if rec := call(); rec.Code != http.StatusConflict {
		t.Fatalf("partial proof status=%d body=%s", rec.Code, rec.Body.String())
	}
	var released int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM recording_clips WHERE id IN($1,$2) AND released_at IS NOT NULL`, firstID, secondID).Scan(&released); err != nil {
		t.Fatal(err)
	}
	if released != 0 {
		t.Fatalf("partial batch released %d clips, want 0", released)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO nas_inventory_files(connection_id,clip_id,recording_id,relative_path,size_bytes,sha256,state,verified_at) VALUES($1,$2,$3,'safe/2.mp4',3,$4,'present',now())`, connectionID, secondID, recordingID, sha); err != nil {
		t.Fatal(err)
	}
	if rec := call(); rec.Code != http.StatusOK {
		t.Fatalf("complete proof status=%d body=%s", rec.Code, rec.Body.String())
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM recording_clips WHERE id IN($1,$2) AND released_at IS NOT NULL`, firstID, secondID).Scan(&released); err != nil {
		t.Fatal(err)
	}
	if released != 2 {
		t.Fatalf("complete batch released %d clips, want 2", released)
	}
}

func TestNASInventorySyncIsMonotonicAndCompletionSweepIsRaceSafe(t *testing.T) {
	pool, cleanup := testAccountClipsPool(t)
	defer cleanup()
	ctx := context.Background()
	const accountID, apiKeyID = int64(42), int64(99)
	var connectionID int64
	if err := pool.QueryRow(ctx, `INSERT INTO connections(account_id,kind,api_key_id) VALUES($1,'nas_pull',$2) RETURNING id`, accountID, apiKeyID).Scan(&connectionID); err != nil {
		t.Fatal(err)
	}
	s := &Server{pool: pool}
	callSync := func(payload nasInventorySyncRequest) *httptest.ResponseRecorder {
		body, err := json.Marshal(payload)
		if err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest(http.MethodPost, "/api/v1/account/connections/inventory/sync", bytes.NewReader(body))
		req = req.WithContext(context.WithValue(req.Context(), accountPrincipalContextKey, accountPrincipal{AccountID: accountID, APIKeyID: ptrInt64(apiKeyID)}))
		rec := httptest.NewRecorder()
		s.handleAccountConnectionInventorySync(rec, req)
		return rec
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	sha := strings.Repeat("a", 64)
	newer := nasInventorySyncRequest{Generation: "scan-new", Files: []nasInventoryFileReport{{
		ClipID: 10, RecordingID: 20, RelativePath: "clips/new.mp4", SizeBytes: 3, SHA256: sha,
		State: "present", VerifiedAt: &now, ClientUpdatedAt: now,
	}}}
	if rec := callSync(newer); rec.Code != http.StatusOK {
		t.Fatalf("new sync status=%d body=%s", rec.Code, rec.Body.String())
	}
	olderTime := now.Add(-time.Hour)
	older := newer
	older.Generation = "scan-old"
	older.Files = append([]nasInventoryFileReport(nil), newer.Files...)
	older.Files[0].RelativePath = "clips/stale.mp4"
	older.Files[0].ClientUpdatedAt = olderTime
	if rec := callSync(older); rec.Code != http.StatusOK {
		t.Fatalf("idempotent old sync status=%d body=%s", rec.Code, rec.Body.String())
	}
	var path string
	if err := pool.QueryRow(ctx, `SELECT relative_path FROM nas_inventory_files WHERE connection_id=$1 AND clip_id=10`, connectionID).Scan(&path); err != nil || path != "clips/new.mp4" {
		t.Fatalf("monotonic row path=%q err=%v", path, err)
	}
	scanStart := now.Add(-30 * time.Minute)
	if _, err := pool.Exec(ctx, `INSERT INTO nas_inventory_files(connection_id,clip_id,recording_id,relative_path,size_bytes,sha256,state,verified_at,seen_generation,client_updated_at) VALUES
		($1,11,20,'clips/old.mp4',3,$2,'present',$3,'prior',$4),
		($1,12,20,'clips/inflight.mp4',3,$2,'present',$3,'prior',$5)`, connectionID, sha, now, scanStart.Add(-time.Minute), scanStart.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	complete := nasInventorySyncRequest{Generation: "scan-complete", ScanStartedAt: &scanStart, ScanCompletedAt: &now, Digest: strings.Repeat("b", 64), Complete: true}
	if rec := callSync(complete); rec.Code != http.StatusOK {
		t.Fatalf("complete sync status=%d body=%s", rec.Code, rec.Body.String())
	}
	var oldState, inflightState string
	if err := pool.QueryRow(ctx, `SELECT max(state) FILTER(WHERE clip_id=11),max(state) FILTER(WHERE clip_id=12) FROM nas_inventory_files WHERE connection_id=$1`, connectionID).Scan(&oldState, &inflightState); err != nil {
		t.Fatal(err)
	}
	if oldState != "missing" || inflightState != "present" {
		t.Fatalf("completion sweep old=%q inflight=%q", oldState, inflightState)
	}
}

func TestNASInventoryModeRequiresCompleteExactCoverage(t *testing.T) {
	pool, cleanup := testAccountClipsPool(t)
	defer cleanup()
	ctx := context.Background()
	const accountID = int64(42)
	var connectionID, recordingID, clipID int64
	if err := pool.QueryRow(ctx, `INSERT INTO connections(account_id,kind,inventory_scan_completed_at) VALUES($1,'nas_pull',now()) RETURNING id`, accountID).Scan(&connectionID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `INSERT INTO recordings(account_id,name,delivery) VALUES($1,'mode','nas_pull') RETURNING id`, accountID).Scan(&recordingID); err != nil {
		t.Fatal(err)
	}
	sha := strings.Repeat("a", 64)
	if err := pool.QueryRow(ctx, `INSERT INTO recording_clips(recording_id,size_bytes,sha256,display_path,clip_start_at,clip_end_at) VALUES($1,3,$2,'mode/clip.mp4',now()-interval '1 minute',now()) RETURNING id`, recordingID, sha).Scan(&clipID); err != nil {
		t.Fatal(err)
	}
	s := &Server{pool: pool}
	callMode := func() *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPatch, "/api/v1/account/connections/x/inventory-mode", bytes.NewBufferString(`{"mode":"enforce"}`))
		req.SetPathValue("id", fmt.Sprint(connectionID))
		req = req.WithContext(context.WithValue(req.Context(), accountPrincipalContextKey, accountPrincipal{AccountID: accountID}))
		rec := httptest.NewRecorder()
		s.handleAccountConnectionInventoryMode(rec, req)
		return rec
	}
	if rec := callMode(); rec.Code != http.StatusConflict {
		t.Fatalf("unconfirmed enforce status=%d body=%s", rec.Code, rec.Body.String())
	}
	if _, err := pool.Exec(ctx, `INSERT INTO nas_inventory_files(connection_id,clip_id,recording_id,relative_path,size_bytes,sha256,state,verified_at) VALUES($1,$2,$3,'mode/clip.mp4',3,$4,'present',now())`, connectionID, clipID, recordingID, sha); err != nil {
		t.Fatal(err)
	}
	if rec := callMode(); rec.Code != http.StatusOK {
		t.Fatalf("ready enforce status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func ptrInt64(value int64) *int64 { return &value }
