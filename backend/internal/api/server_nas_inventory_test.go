package api

import (
	"bytes"
	"context"
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

func ptrInt64(value int64) *int64 { return &value }
