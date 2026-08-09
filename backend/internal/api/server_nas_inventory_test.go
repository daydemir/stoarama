package api

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	neturl "net/url"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
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

func TestNASInventoryTreeCursorRejectsTampering(t *testing.T) {
	want := nasInventoryTreeCursor{Kind: 0, Name: "August"}
	encoded := encodeNASInventoryTreeCursor(want)
	got, err := decodeNASInventoryTreeCursor(encoded)
	if err != nil || got != want {
		t.Fatalf("cursor round trip got=%+v err=%v", got, err)
	}
	for _, value := range []string{"not-base64!", base64.RawURLEncoding.EncodeToString([]byte(`{"kind":2,"name":"x"}`)), base64.RawURLEncoding.EncodeToString([]byte(`{"kind":0,"name":"../x"}`))} {
		if _, err := decodeNASInventoryTreeCursor(value); err == nil {
			t.Errorf("invalid cursor accepted: %q", value)
		}
	}
}

func TestNASInventoryTreeQueryUsesExactParentIndexes(t *testing.T) {
	direct := nasInventoryTreeSQL
	if strings.Contains(direct, "GROUP BY") || strings.Contains(direct, "split_part") || strings.Contains(direct, "starts_with") {
		t.Fatalf("browse query still aggregates or scans path prefixes:\n%s", direct)
	}
	for _, marker := range []string{"i.connection_id=$1 AND i.tree_parent_path=$4", "u.connection_id=$1 AND u.tree_parent_path=$4", "d.generation=$3", "row_number() OVER(PARTITION BY name"} {
		if !strings.Contains(direct, marker) {
			t.Fatalf("tree query missing bounded snapshot marker %q", marker)
		}
	}
}

func TestNASInventoryTreeBrowsesImmediateChildrenWithKeysetPagination(t *testing.T) {
	pool, cleanup := testAccountClipsPool(t)
	defer cleanup()
	ctx := context.Background()
	const accountID = int64(42)
	var connectionID, recordingID int64
	if err := pool.QueryRow(ctx, `INSERT INTO connections(account_id,kind) VALUES($1,'nas_pull') RETURNING id`, accountID).Scan(&connectionID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `INSERT INTO recordings(account_id,name,delivery) VALUES($1,'tree','nas_pull') RETURNING id`, accountID).Scan(&recordingID); err != nil {
		t.Fatal(err)
	}
	sha := strings.Repeat("a", 64)
	for index, path := range []string{"Africa/July/one.mp4", "Africa/August/two.mp4", "Europe/three.mp4", "root.mp4"} {
		var clipID int64
		if err := pool.QueryRow(ctx, `INSERT INTO recording_clips(recording_id,size_bytes,sha256,display_path,clip_start_at,clip_end_at) VALUES($1,$2,$3,$4,now()-interval '1 minute',now()) RETURNING id`, recordingID, index+1, sha, path).Scan(&clipID); err != nil {
			t.Fatal(err)
		}
		state := "present"
		if path == "Africa/August/two.mp4" {
			state = "mismatch"
		}
		if _, err := pool.Exec(ctx, `INSERT INTO nas_inventory_files(connection_id,clip_id,recording_id,relative_path,size_bytes,sha256,state,verified_at,client_updated_at) VALUES($1,$2,$3,$4,$5,$6,$7,now(),now())`, connectionID, clipID, recordingID, path, index+1, sha, state); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := pool.Exec(ctx, `INSERT INTO nas_inventory_unmatched_files(connection_id,relative_path,size_bytes,sha256,state,client_updated_at) VALUES
		($1,'Africa/July/orphan.mp4',9,$2,'present',now()),($1,'Africa/July/one.mp4',1,$2,'present',now()),($1,'root.mp4',4,$2,'present',now())`, connectionID, sha); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE nas_inventory_files SET verified_at=now()-$2::interval WHERE connection_id=$1 AND relative_path='Europe/three.mp4'`, connectionID, (nasInventoryFreshness + time.Hour).String()); err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"nas_inventory_files", "nas_inventory_unmatched_files"} {
		if _, err := pool.Exec(ctx, `UPDATE `+table+` SET
			tree_parent_path=CASE WHEN strpos(reverse(relative_path),'/')=0 THEN '' ELSE left(relative_path,length(relative_path)-strpos(reverse(relative_path),'/')) END,
			tree_name=CASE WHEN strpos(reverse(relative_path),'/')=0 THEN relative_path ELSE right(relative_path,strpos(reverse(relative_path),'/')-1) END
			WHERE connection_id=$1`, connectionID); err != nil {
			t.Fatal(err)
		}
	}
	refresh := func(generation string) {
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		if err := rebuildNASInventoryTreeDirectories(ctx, tx, connectionID, accountID, generation); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, `UPDATE connections SET inventory_generation=$1,inventory_tree_generation=$1,inventory_scan_started_at=now()-interval '1 minute',inventory_scan_completed_at=now(),inventory_reported_at=now(),inventory_in_progress_generation='' WHERE id=$2`, generation, connectionID); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	}
	refresh("tree-one")
	s := &Server{pool: pool}
	call := func(path, cursor string, limit int) (int, map[string]any) {
		url := fmt.Sprintf("/api/v1/account/connections/%d/inventory/tree?limit=%d", connectionID, limit)
		if path != "" {
			url += "&path=" + neturl.QueryEscape(path)
		}
		if cursor != "" {
			url += "&cursor=" + cursor
		}
		req := httptest.NewRequest(http.MethodGet, url, nil)
		routeCtx := chi.NewRouteContext()
		routeCtx.URLParams.Add("id", fmt.Sprint(connectionID))
		req = req.WithContext(context.WithValue(context.WithValue(req.Context(), accountPrincipalContextKey, accountPrincipal{AccountID: accountID}), chi.RouteCtxKey, routeCtx))
		rec := httptest.NewRecorder()
		s.handleAccountConnectionInventoryTree(rec, req)
		var body map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		return rec.Code, body
	}
	status, first := call("", "", 2)
	if status != http.StatusOK {
		t.Fatalf("root status=%d body=%v", status, first)
	}
	firstEntries := first["entries"].([]any)
	if len(firstEntries) != 2 || firstEntries[0].(map[string]any)["name"] != "Africa" || firstEntries[1].(map[string]any)["name"] != "Europe" {
		t.Fatalf("root first page=%v", firstEntries)
	}
	if firstEntries[1].(map[string]any)["stale_files"] != float64(1) {
		t.Fatalf("Europe directory omitted stale descendant: %v", firstEntries[1])
	}
	if first["server_only"] != float64(4) {
		t.Fatalf("root server_only=%v, want mismatch, stale, and ambiguous clips counted as unsafe", first["server_only"])
	}
	cursor, _ := first["next_cursor"].(string)
	if cursor == "" {
		t.Fatal("root first page omitted cursor")
	}
	if status, _ := call("Africa", cursor, 2); status != http.StatusConflict {
		t.Fatalf("cursor reused across directories status=%d, want 409", status)
	}
	status, second := call("", cursor, 2)
	if status != http.StatusOK {
		t.Fatalf("root second page status=%d", status)
	}
	secondEntries, _ := second["entries"].([]any)
	if len(secondEntries) != 1 || secondEntries[0].(map[string]any)["name"] != "root.mp4" {
		t.Fatalf("root second page status=%d entries=%v", status, secondEntries)
	}
	if secondEntries[0].(map[string]any)["reconciliation"] != "ambiguous" {
		t.Fatalf("duplicate root path was not ambiguous: %v", secondEntries[0])
	}
	if _, ok := second["server_only"]; ok {
		t.Fatal("continuation page recomputed server-only summary")
	}
	if first["generation"] != "tree-one" || first["fresh"] != true || first["snapshot_consistent"] != true {
		t.Fatalf("root snapshot metadata=%v", first)
	}
	if _, err := pool.Exec(ctx, `UPDATE connections SET inventory_live_revision=inventory_tree_revision+1 WHERE id=$1`, connectionID); err != nil {
		t.Fatal(err)
	}
	if status, body := call("", "", 2); status != http.StatusOK || body["snapshot_consistent"] != false {
		t.Fatalf("revision gap status=%d body=%v, want inconsistent snapshot", status, body)
	}
	if _, err := pool.Exec(ctx, `UPDATE connections SET inventory_tree_revision=inventory_live_revision WHERE id=$1`, connectionID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE connections SET inventory_in_progress_generation='tree-active',inventory_in_progress_started_at=now(),inventory_in_progress_reported_at=now() WHERE id=$1`, connectionID); err != nil {
		t.Fatal(err)
	}
	if status, _ := call("", cursor, 2); status != http.StatusConflict {
		t.Fatalf("cursor spanning active-generation change status=%d, want 409", status)
	}
	if _, err := pool.Exec(ctx, `UPDATE connections SET inventory_in_progress_reported_at=now()-$2::interval WHERE id=$1`, connectionID, (nasInventoryScanProgressTTL + time.Hour).String()); err != nil {
		t.Fatal(err)
	}
	if status, body := call("", "", 2); status != http.StatusOK || body["snapshot_consistent"] != true || body["in_progress_generation"] != "" {
		t.Fatalf("expired scan progress status=%d body=%v, want ignored stale progress", status, body)
	}
	if _, err := pool.Exec(ctx, `UPDATE connections SET inventory_in_progress_generation='',inventory_in_progress_started_at=NULL,inventory_in_progress_reported_at=NULL WHERE id=$1`, connectionID); err != nil {
		t.Fatal(err)
	}
	status, africa := call("Africa", "", 20)
	if status != http.StatusOK {
		t.Fatalf("Africa status=%d", status)
	}
	africaEntries, _ := africa["entries"].([]any)
	if len(africaEntries) != 2 || africaEntries[0].(map[string]any)["kind"] != "directory" {
		t.Fatalf("Africa status=%d entries=%v", status, africaEntries)
	}
	if africaEntries[1].(map[string]any)["ambiguous_files"] != float64(1) {
		t.Fatalf("July directory omitted ambiguous descendant: %v", africaEntries[1])
	}
	if _, ok := africa["server_only"]; ok {
		t.Fatal("nested folder first page recomputed account-wide server-only summary")
	}
	status, july := call("Africa/July", "", 20)
	if status != http.StatusOK {
		t.Fatalf("July status=%d", status)
	}
	julyEntries, _ := july["entries"].([]any)
	if len(julyEntries) != 2 {
		t.Fatalf("July status=%d entries=%v", status, julyEntries)
	}
	if julyEntries[1].(map[string]any)["reconciliation"] != "nas_only" {
		t.Fatalf("July unmatched file missing: %v", julyEntries)
	}
	if julyEntries[0].(map[string]any)["reconciliation"] != "ambiguous" {
		t.Fatalf("July duplicate file was not ambiguous: %v", julyEntries)
	}
	status, europe := call("Europe", "", 20)
	if status != http.StatusOK {
		t.Fatalf("Europe status=%d", status)
	}
	europeEntries := europe["entries"].([]any)
	if len(europeEntries) != 1 || europeEntries[0].(map[string]any)["reconciliation"] != "stale" {
		t.Fatalf("stale file not classified explicitly: %v", europeEntries)
	}
	status, julyFirst := call("Africa/July", "", 1)
	julyCursor, _ := julyFirst["next_cursor"].(string)
	if status != http.StatusOK || julyCursor == "" {
		t.Fatalf("July first file page status=%d cursor=%q", status, julyCursor)
	}
	decoded, err := decodeNASInventoryTreeCursor(julyCursor)
	if err != nil || decoded.Kind != 1 {
		t.Fatalf("July cursor=%+v err=%v, want direct-file phase", decoded, err)
	}
	status, julySecond := call("Africa/July", julyCursor, 1)
	if status != http.StatusOK {
		t.Fatalf("July direct continuation status=%d", status)
	}
	julySecondEntries, _ := julySecond["entries"].([]any)
	if len(julySecondEntries) != 1 {
		t.Fatalf("July direct continuation status=%d body=%v", status, julySecond)
	}
	if status, _ := call("../private", "", 20); status != http.StatusBadRequest {
		t.Fatalf("unsafe directory status=%d, want 400", status)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO nas_inventory_unmatched_files(connection_id,relative_path,size_bytes,sha256,state,client_updated_at,tree_parent_path,tree_name) VALUES
		($1,'100%/literal.mp4',5,$2,'present',now(),'100%','literal.mp4'),($1,'1000/sibling.mp4',6,$2,'present',now(),'1000','sibling.mp4'),
		($1,'under_score/literal.mp4',7,$2,'present',now(),'under_score','literal.mp4'),($1,'underXscore/sibling.mp4',8,$2,'present',now(),'underXscore','sibling.mp4')`, connectionID, sha); err != nil {
		t.Fatal(err)
	}
	refresh("tree-two")
	for _, path := range []string{"100%", "under_score"} {
		status, body := call(path, "", 20)
		if status != http.StatusOK {
			t.Fatalf("escaped prefix %q status=%d", path, status)
		}
		entries, _ := body["entries"].([]any)
		if len(entries) != 1 || entries[0].(map[string]any)["name"] != "literal.mp4" {
			t.Fatalf("escaped prefix %q status=%d entries=%v", path, status, entries)
		}
	}
	longDirectory := strings.Repeat("d", 600)
	if _, err := pool.Exec(ctx, `INSERT INTO nas_inventory_unmatched_files(connection_id,relative_path,size_bytes,sha256,state,client_updated_at,tree_parent_path,tree_name) VALUES
		($1,$2||'/a.mp4',1,$3,'present',now(),$2,'a.mp4'),($1,$2||'/b.mp4',1,$3,'present',now(),$2,'b.mp4')`, connectionID, longDirectory, sha); err != nil {
		t.Fatal(err)
	}
	refresh("tree-three")
	status, longFirst := call(longDirectory, "", 1)
	longCursor, _ := longFirst["next_cursor"].(string)
	if status != http.StatusOK || longCursor == "" {
		t.Fatalf("long directory first page status=%d body=%v", status, longFirst)
	}
	if status, body := call(longDirectory, longCursor, 1); status != http.StatusOK || len(body["entries"].([]any)) != 1 {
		t.Fatalf("long directory continuation status=%d body=%v", status, body)
	}
	maxGeneration := strings.Repeat("g", nasInventoryMaxGeneration)
	refresh(maxGeneration)
	if _, err := pool.Exec(ctx, `UPDATE connections SET inventory_in_progress_generation=$2,inventory_in_progress_started_at=now(),inventory_in_progress_reported_at=now() WHERE id=$1`,
		connectionID, strings.Repeat("a", nasInventoryMaxGeneration)); err != nil {
		t.Fatal(err)
	}
	status, maxStateFirst := call(longDirectory, "", 1)
	maxStateCursor, _ := maxStateFirst["next_cursor"].(string)
	if status != http.StatusOK || maxStateCursor == "" {
		t.Fatalf("maximum metadata first page status=%d body=%v", status, maxStateFirst)
	}
	if status, body := call(longDirectory, maxStateCursor, 1); status != http.StatusOK || len(body["entries"].([]any)) != 1 {
		t.Fatalf("maximum metadata continuation status=%d body=%v", status, body)
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
	confirmed, _, err := verifyNASInventoryForRelease(r.Context(), s.pool, principal, recordingID, clipID)
	if err != nil || confirmed {
		t.Fatalf("missing inventory confirmed=%v err=%v, want false/nil", confirmed, err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO nas_inventory_files(connection_id,clip_id,recording_id,relative_path,size_bytes,sha256,state,verified_at) VALUES($1,$2,$3,'safe/clip.mp4',3,$4,'present',now())`, connectionID, clipID, recordingID, sha); err != nil {
		t.Fatal(err)
	}
	confirmed, reason, err := verifyNASInventoryForRelease(r.Context(), s.pool, principal, recordingID, clipID)
	if err != nil || !confirmed || reason != "confirmed" {
		t.Fatalf("exact inventory confirmed=%v reason=%q err=%v", confirmed, reason, err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO nas_inventory_unmatched_files(connection_id,relative_path,size_bytes,sha256,state,client_updated_at) VALUES($1,'safe/clip.mp4',3,$2,'present',now())`, connectionID, sha); err != nil {
		t.Fatal(err)
	}
	confirmed, reason, err = verifyNASInventoryForRelease(r.Context(), s.pool, principal, recordingID, clipID)
	if err != nil || confirmed || !strings.Contains(reason, "multiple") {
		t.Fatalf("ambiguous path confirmed=%v reason=%q err=%v, want fail closed", confirmed, reason, err)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM nas_inventory_unmatched_files WHERE connection_id=$1 AND relative_path='safe/clip.mp4'`, connectionID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE nas_inventory_files SET size_bytes=4 WHERE connection_id=$1 AND clip_id=$2`, connectionID, clipID); err != nil {
		t.Fatal(err)
	}
	confirmed, _, err = verifyNASInventoryForRelease(r.Context(), s.pool, principal, recordingID, clipID)
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
	if _, err := pool.Exec(ctx, `INSERT INTO nas_inventory_unmatched_files(connection_id,relative_path,size_bytes,sha256,state,client_updated_at) VALUES($1,'safe/1.mp4',3,$2,'present',now())`, connectionID, sha); err != nil {
		t.Fatal(err)
	}
	if rec := call(); rec.Code != http.StatusConflict {
		t.Fatalf("ambiguous proof status=%d body=%s", rec.Code, rec.Body.String())
	}
	if _, err := pool.Exec(ctx, `DELETE FROM nas_inventory_unmatched_files WHERE connection_id=$1 AND relative_path='safe/1.mp4'`, connectionID); err != nil {
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
	olderComplete := scanStart.Add(-time.Minute)
	olderStart := olderComplete.Add(-time.Hour)
	laterClientUpdate := now.Add(time.Minute)
	stale := nasInventorySyncRequest{
		Generation: "scan-stale", ScanStartedAt: &olderStart, ScanCompletedAt: &olderComplete,
		Digest: strings.Repeat("c", 64), Complete: true,
		Files: []nasInventoryFileReport{{
			ClipID: 10, RecordingID: 20, RelativePath: "clips/stale-completion-mutation.mp4",
			SizeBytes: 9, SHA256: strings.Repeat("d", 64), State: "mismatch", ClientUpdatedAt: laterClientUpdate,
		}},
	}
	if rec := callSync(stale); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"completion_stale":true`) {
		t.Fatalf("stale complete sync status=%d body=%s", rec.Code, rec.Body.String())
	}
	var generation, digest, preservedPath, preservedState string
	var completedAt time.Time
	if err := pool.QueryRow(ctx, `SELECT inventory_generation,inventory_digest,inventory_scan_completed_at FROM connections WHERE id=$1`, connectionID).Scan(&generation, &digest, &completedAt); err != nil {
		t.Fatal(err)
	}
	if generation != "scan-complete" || digest != strings.Repeat("b", 64) || !completedAt.Equal(now) {
		t.Fatalf("stale completion replaced summary generation=%q digest=%q completed=%s", generation, digest, completedAt)
	}
	if err := pool.QueryRow(ctx, `SELECT relative_path,state FROM nas_inventory_files WHERE connection_id=$1 AND clip_id=10`, connectionID).Scan(&preservedPath, &preservedState); err != nil {
		t.Fatal(err)
	}
	if preservedPath != "clips/new.mp4" || preservedState != "present" {
		t.Fatalf("stale completion mutated row path=%q state=%q", preservedPath, preservedState)
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
