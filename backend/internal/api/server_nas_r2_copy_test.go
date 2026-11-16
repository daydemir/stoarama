package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
)

func copyTestFixture(t *testing.T) (*cleanupFixture, NASR2CopyItem) {
	t.Helper()
	f := newCleanupFixture(t, 1)
	f.useClientVersion(nasCleanupProofClientVersion)

	for _, name := range []string{"0174_r2_object_retirements.sql", "0182_r2_retirement_reservations.sql", "0193_nas_r2_copy.sql"} {
		b, err := os.ReadFile("../../../infra/sql/migrations/" + name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = f.pool.Exec(f.ctx, string(b)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := f.pool.Exec(f.ctx, `CREATE TABLE nas_inventory_files(connection_id bigint,clip_id bigint,relative_path text,sha256 text,size_bytes bigint,state text,verified_at timestamptz,server_received_at timestamptz,sidecar_relative_path text,sidecar_size_bytes bigint,sidecar_sha256 text)`); err != nil {
		t.Fatal(err)
	}
	policy, w, _ := Keep28Policy()
	body := []byte("verified kept R2 footage")
	hash := sha256.Sum256(body)
	i := NASR2CopyItem{Kind: "raw", SourceID: 901, RecordingID: 2, StreamID: 3, Bucket: "bucket", ObjectKey: "managed/acct-47/copy.mp4", RelativePath: "p/copy.mp4", SizeBytes: int64(len(body)), SHA256: hex.EncodeToString(hash[:]), StartAt: w[3].Start, EndAt: w[3].Start.Add(time.Minute)}
	f.bucket.put(i.ObjectKey, body)
	_, err := f.pool.Exec(f.ctx, `INSERT INTO recording_clips(id,recording_id,object_key,sha256,size_bytes,display_path,clip_start_at,clip_end_at,bucket) VALUES($1,2,$2,$3,$4,$5,$6,$7,'bucket')`, i.SourceID, i.ObjectKey, i.SHA256, i.SizeBytes, i.RelativePath, i.StartAt, i.EndAt)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(i)
	_, err = f.pool.Exec(f.ctx, `INSERT INTO nas_r2_copy_items(connection_id,kind,source_id,manifest_sha256,policy_sha256,item) VALUES(13,'raw',$1,repeat('a',64),$2,$3)`, i.SourceID, policy, data)
	if err != nil {
		t.Fatal(err)
	}
	return f, i
}
func copyCall(f *cleanupFixture, action string, id int64, body any) *httptest.ResponseRecorder {
	data, _ := json.Marshal(body)
	path := fmt.Sprintf("/api/v1/account/connections/r2-copy/%d/%s", id, action)
	if action == "lease" {
		path = "/api/v1/account/connections/r2-copy/lease"
	}
	req := httptest.NewRequest("POST", path, strings.NewReader(string(data)))
	req.Header.Set("User-Agent", "stoarama-nas-pull/"+f.version)
	ctx := context.WithValue(req.Context(), accountPrincipalContextKey, accountPrincipal{AccountID: 47, APIKeyID: ptrInt64(5), KeyScopes: []string{accountScopePull}})
	route := chi.NewRouteContext()
	route.URLParams.Add("id", fmt.Sprint(id))
	ctx = context.WithValue(ctx, chi.RouteCtxKey, route)
	req = req.WithContext(ctx)
	out := httptest.NewRecorder()
	handlers := map[string]http.HandlerFunc{"lease": f.s.handleNASR2CopyLease, "checkpoint": f.s.handleNASR2CopyCheckpoint, "range": f.s.handleNASR2CopyRange, "result": f.s.handleNASR2CopyResult}
	f.s.confineAccountScope(f.s.fenceNASDelivery(handlers[action])).ServeHTTP(out, req)
	return out
}
func copyLease(t *testing.T, f *cleanupFixture) nasR2CopyTask {
	t.Helper()
	out := copyCall(f, "lease", 0, nasR2CopyControl{FreeBytes: NASR2CopyFloor + 1_000_000_000})
	if out.Code != 200 {
		t.Fatalf("lease %d %s", out.Code, out.Body)
	}
	var data struct {
		Task nasR2CopyTask `json:"task"`
	}
	if err := json.Unmarshal(out.Body.Bytes(), &data); err != nil || data.Task.ID <= 0 {
		t.Fatalf("task %s %v", out.Body, err)
	}
	return data.Task
}
func TestNASR2CopyDefaultOffAndInventory(t *testing.T) {
	f, i := copyTestFixture(t)
	if out := copyCall(f, "lease", 0, nasR2CopyControl{FreeBytes: NASR2CopyFloor + 1e9}); out.Code != 409 {
		t.Fatalf("default-off copy route status=%d body=%s; want 409", out.Code, out.Body)
	}
	f.s.cfg.NASR2CopyEnabled = true
	if out := copyCall(f, "lease", 0, nasR2CopyControl{FreeBytes: NASR2CopyFloor - 1}); out.Code != 409 {
		t.Fatal("floor ignored")
	}
	task := copyLease(t, f)
	req := nasR2CopyControl{Token: task.Token, FreeBytes: NASR2CopyFloor + 1e9, Length: i.SizeBytes}
	second := copyCall(f, "lease", 0, req)
	if second.Code != 200 || !strings.Contains(second.Body.String(), `"task":null`) {
		t.Fatal("overlapping worker")
	}
	out := copyCall(f, "range", task.ID, req)
	if out.Code != 200 {
		t.Fatalf("range %d %s", out.Code, out.Body)
	}
	var grant struct {
		URL     string            `json:"url"`
		Headers map[string]string `json:"headers"`
		Length  int64             `json:"length"`
	}
	if err := json.Unmarshal(out.Body.Bytes(), &grant); err != nil || grant.Length != i.SizeBytes || grant.Headers["Range"] != fmt.Sprintf("bytes=0-%d", i.SizeBytes-1) || grant.Headers["If-Match"] == "" {
		t.Fatalf("grant %s %v", out.Body, err)
	}

	if out := copyCall(f, "result", task.ID, req); out.Code != 409 {
		t.Fatal("result without inventory")
	}
	_, err := f.pool.Exec(f.ctx, `INSERT INTO nas_inventory_files VALUES(13,$1,$2,$3,$4,'present',now(),now(),$2||'.stoarama.json',100,repeat('b',64))`, i.SourceID, i.RelativePath, i.SHA256, i.SizeBytes)
	if err != nil {
		t.Fatal(err)
	}
	if out := copyCall(f, "result", task.ID, req); out.Code != 200 {
		t.Fatalf("result %d %s", out.Code, out.Body)
	}
	var state string
	f.pool.QueryRow(f.ctx, `SELECT state FROM nas_r2_copy_items WHERE id=$1`, task.ID).Scan(&state)
	if state != "verified" {
		t.Fatal(state)
	}
	f.bucket.mu.Lock()
	defer f.bucket.mu.Unlock()
	if _, ok := f.bucket.objects[i.ObjectKey]; !ok {
		t.Fatal("source deleted")
	}
}
func TestNASR2CopyCancellationStaleTokenAndFailedRange(t *testing.T) {
	f, i := copyTestFixture(t)
	f.s.cfg.NASR2CopyEnabled = true
	task := copyLease(t, f)
	req := nasR2CopyControl{Token: task.Token, FreeBytes: NASR2CopyFloor + 1e9, Length: i.SizeBytes}
	f.bucket.mu.Lock()
	f.bucket.getFailures = 1
	f.bucket.mu.Unlock()
	out := copyCall(f, "range", task.ID, req)
	if out.Code != 200 {
		t.Fatal(out.Body)
	}
	var grant struct {
		URL     string            `json:"url"`
		Headers map[string]string `json:"headers"`
	}
	json.Unmarshal(out.Body.Bytes(), &grant)
	request, _ := http.NewRequest(http.MethodGet, grant.URL, nil)
	for k, v := range grant.Headers {
		request.Header.Set(k, v)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode == 200 {
		t.Fatal("fixture GET did not fail")
	}

	req.Token = "wrong"
	if out := copyCall(f, "range", task.ID, req); out.Code != 409 {
		t.Fatal("stale token accepted")
	}
	req.Token = task.Token
	f.s.cfg.NASR2CopyEnabled = false
	if out := copyCall(f, "checkpoint", task.ID, req); out.Code != 409 {
		t.Fatal("OFF ignored")
	}
	f.s.cfg.NASR2CopyEnabled = true
	if _, err := f.pool.Exec(f.ctx, `UPDATE nas_cleanup_maintenance SET active=true`); err != nil {
		t.Fatal(err)
	}
	if out := copyCall(f, "range", task.ID, req); out.Code != 409 {
		t.Fatal("cleanup not prioritized")
	}
}
func TestNASR2CopyNoDailyCapWithActiveCleanup(t *testing.T) {
	f, i := copyTestFixture(t)
	// A fully exhausted legacy ledger must not limit any new exact GET.
	if _, err := f.pool.Exec(f.ctx, `INSERT INTO nas_r2_copy_budgets VALUES(13,current_date,100000000000)`); err != nil {
		t.Fatal(err)
	}
	id, sha, _ := f.admit("parallel-copy", syntheticFiles(100, 2, 2))
	f.dryRun()
	f.s.cfg.NASCleanupDeletionEnabled = true
	if code, body := f.enableBatch([]int64{id}, []string{sha}, 2); code != 200 {
		t.Fatalf("enable %d %s", code, body)
	}
	tasks := f.leaseV2(2, 2, 1<<30, strings.Repeat("a", 32))
	if len(tasks) == 0 {
		t.Fatal("cleanup not leased")
	}
	f.observe(tasks, nil) // Real live permits; no trigger bypass.
	f.s.cfg.NASR2CopyEnabled = true
	task := copyLease(t, f)
	out := copyCall(f, "range", task.ID, nasR2CopyControl{Token: task.Token, FreeBytes: NASR2CopyFloor + 1e9, Length: i.SizeBytes})
	if out.Code != 200 {
		t.Fatalf("active cleanup blocked copy: %d %s", out.Code, out.Body)
	}
	var charged int64
	if err := f.pool.QueryRow(f.ctx, `SELECT charged_bytes FROM nas_r2_copy_budgets`).Scan(&charged); err != nil || charged != 100000000000 {
		t.Fatalf("historical ledger changed: %d %v", charged, err)
	}
}

func TestNASR2CopyWindowIsDisjointFromCleanup(t *testing.T) {
	_, i := copyTestFixture(t)
	p, err := cleanupEmbeddedPolicy()
	if err != nil {
		t.Fatal(err)
	}
	w := p.Streams[i.StreamID]
	for _, bounds := range [][2]time.Time{{w.start.Add(-time.Minute), w.start}, {w.start, w.start.Add(time.Minute)}, {w.start.Add(-time.Minute), w.start.Add(time.Minute)}, {w.end.Add(-time.Minute), w.end.Add(time.Minute)}, {w.end, w.end.Add(time.Minute)}} {
		i.StartAt, i.EndAt = bounds[0], bounds[1]
		copyAllowed := ValidateNASR2CopyItem(i) == nil
		if copyAllowed == p.outside(i.StreamID, i.StartAt, i.EndAt) {
			t.Fatalf("non-complementary bounds %v", bounds)
		}
	}
}

func TestNASR2CopyAliasRefused(t *testing.T) {
	f, i := copyTestFixture(t)
	f.s.cfg.NASR2CopyEnabled = true
	if _, err := f.pool.Exec(f.ctx, `INSERT INTO recording_clips(id,recording_id,object_key,sha256,size_bytes,display_path,clip_start_at,clip_end_at,bucket) SELECT 902,recording_id,object_key,sha256,size_bytes,display_path,clip_start_at,clip_end_at,bucket FROM recording_clips WHERE id=$1`, i.SourceID); err != nil {
		t.Fatal(err)
	}
	out := copyCall(f, "lease", 0, nasR2CopyControl{FreeBytes: NASR2CopyFloor + 1e9})
	if out.Code != 409 {
		t.Fatalf("path alias accepted: %d %s", out.Code, out.Body)
	}
}

func TestNASR2CopySourceHoldsStillRefused(t *testing.T) {
	f, i := copyTestFixture(t)
	f.s.cfg.NASR2CopyEnabled = true
	tx, err := BeginNASR2CopyTx(f.ctx, f.pool)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(f.ctx)
	// A stitched-source cleanup reservation can overlap keep28; it must still
	// veto copy even though ordinary outside-window plans can run concurrently.
	if _, err = tx.Exec(f.ctx, `CREATE TEMP TABLE nas_cleanup_reserved_files(kind text,source_id bigint)`); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(f.ctx, `INSERT INTO nas_cleanup_reserved_files VALUES('raw',$1)`, i.SourceID); err != nil {
		t.Fatal(err)
	}
	if _, err = LoadNASR2CopySource(f.ctx, tx, i); err == nil {
		t.Fatal("reserved source accepted")
	}
}

// Five exact source/path identities let a fifth lease expose a slot race.
func queueCopySources(t *testing.T, f *cleanupFixture, i NASR2CopyItem) {
	t.Helper()
	for n := 1; n < 5; n++ {
		next := i
		next.SourceID += int64(n)
		next.RelativePath = fmt.Sprintf("p/copy%d.mp4", n)
		if _, err := f.pool.Exec(f.ctx, `INSERT INTO recording_clips(id,recording_id,object_key,sha256,size_bytes,display_path,clip_start_at,clip_end_at,bucket) SELECT $2,recording_id,object_key,sha256,size_bytes,$3,clip_start_at,clip_end_at,bucket FROM recording_clips WHERE id=$1`, i.SourceID, next.SourceID, next.RelativePath); err != nil {
			t.Fatal(err)
		}
		data, _ := json.Marshal(next)
		if _, err := f.pool.Exec(f.ctx, `INSERT INTO nas_r2_copy_items(connection_id,kind,source_id,manifest_sha256,policy_sha256,item) SELECT 13,'raw',$1,manifest_sha256,policy_sha256,$2 FROM nas_r2_copy_items LIMIT 1`, next.SourceID, data); err != nil {
			t.Fatal(err)
		}
	}
}

func TestNASR2CopyRenewalCannotRaceExpiredSlotAdmission(t *testing.T) {
	f, i := copyTestFixture(t)
	f.s.cfg.NASR2CopyEnabled = true
	queueCopySources(t, f, i)
	tasks := make([]nasR2CopyTask, NASR2CopyWorkers)
	for n := range tasks {
		tasks[n] = copyLease(t, f)
	}
	old := tasks[0]
	expires := time.Now().Add(time.Second)
	if _, err := f.pool.Exec(f.ctx, `UPDATE nas_r2_copy_items SET lease_expires_at=$2 WHERE id=$1`, old.ID, expires); err != nil {
		t.Fatal(err)
	}
	blocker, err := BeginNASR2CopyTx(f.ctx, f.pool)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback(f.ctx)
	if _, err = blocker.Exec(f.ctx, `SELECT id FROM nas_r2_copy_items WHERE id=$1 FOR UPDATE`, old.ID); err != nil {
		t.Fatal(err)
	}
	checkpoint := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		checkpoint <- copyCall(f, "checkpoint", old.ID, nasR2CopyControl{Token: old.Token, FreeBytes: NASR2CopyFloor + 1e9})
	}()
	waitLocks := func(want int) {
		t.Helper()
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			var count int
			if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND pid<>pg_backend_pid()`).Scan(&count); err != nil {
				t.Fatal(err)
			}
			if count >= want {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatal("request did not reach lock barrier")
	}
	waitLocks(1) // Checkpoint validated its old token before expiry, then blocked.
	if delay := time.Until(expires.Add(20 * time.Millisecond)); delay > 0 {
		time.Sleep(delay)
	}
	admission := make(chan *httptest.ResponseRecorder, 1)
	go func() { admission <- copyCall(f, "lease", 0, nasR2CopyControl{FreeBytes: NASR2CopyFloor + 1e9}) }()
	waitLocks(2) // Admission is now racing that renewal across its expiry.
	if err := blocker.Rollback(f.ctx); err != nil {
		t.Fatal(err)
	}
	for _, result := range []chan *httptest.ResponseRecorder{checkpoint, admission} {
		select {
		case out := <-result:
			if out.Code != 200 && out.Code != 409 {
				t.Fatalf("race request %d %s", out.Code, out.Body)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("race request did not finish")
		}
	}
	var live int
	if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM nas_r2_copy_items WHERE state='leased' AND lease_expires_at>clock_timestamp()`).Scan(&live); err != nil {
		t.Fatal(err)
	}
	if live != NASR2CopyWorkers {
		t.Fatalf("renewal/admission produced %d live leases; want %d", live, NASR2CopyWorkers)
	}
}

func TestNASR2CopyConcurrentLeaseSlotsAndExpiredOwner(t *testing.T) {
	f, i := copyTestFixture(t)
	f.s.cfg.NASR2CopyEnabled = true
	queueCopySources(t, f, i)
	var wg sync.WaitGroup
	results := make(chan *httptest.ResponseRecorder, 5)
	for n := 0; n < 5; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- copyCall(f, "lease", 0, nasR2CopyControl{FreeBytes: NASR2CopyFloor + 1e9})
		}()
	}
	wg.Wait()
	close(results)
	tasks := map[int64]nasR2CopyTask{}
	for out := range results {
		if out.Code != 200 {
			t.Fatal(out.Body)
		}
		var body struct {
			Task *nasR2CopyTask `json:"task"`
		}
		if err := json.Unmarshal(out.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		if body.Task != nil {
			if _, exists := tasks[body.Task.ID]; exists {
				t.Fatal("duplicate lease")
			}
			tasks[body.Task.ID] = *body.Task
		}
	}
	if len(tasks) != NASR2CopyWorkers {
		t.Fatalf("leases %d", len(tasks))
	}
	for id, old := range tasks {
		if _, err := f.pool.Exec(f.ctx, `UPDATE nas_r2_copy_items SET lease_expires_at=now()-interval '1 second' WHERE id=$1`, id); err != nil {
			t.Fatal(err)
		}
		next := copyLease(t, f)
		if next.ID != id || next.Token == old.Token {
			t.Fatal("expired owner was not replaced")
		}
		if out := copyCall(f, "checkpoint", id, nasR2CopyControl{Token: old.Token, FreeBytes: NASR2CopyFloor + 1e9}); out.Code != 409 {
			t.Fatal("old owner remains active")
		}
		break
	}
}

func TestNASR2CopyConcurrentGrantsAndStaleCompletion(t *testing.T) {
	f, i := copyTestFixture(t)
	f.s.cfg.NASR2CopyEnabled = true
	task := copyLease(t, f)
	req := nasR2CopyControl{Token: task.Token, FreeBytes: NASR2CopyFloor + 1e9, Length: i.SizeBytes}
	var wg sync.WaitGroup
	outcomes := make(chan *httptest.ResponseRecorder, 2)
	for n := 0; n < 2; n++ {
		wg.Add(1)
		go func() { defer wg.Done(); outcomes <- copyCall(f, "range", task.ID, req) }()
	}
	wg.Wait()
	close(outcomes)
	var success int
	var rangeID string
	for out := range outcomes {
		if out.Code == 200 {
			success++
			var grant struct {
				RangeID string `json:"range_id"`
			}
			json.Unmarshal(out.Body.Bytes(), &grant)
			rangeID = grant.RangeID
		} else if out.Code != 409 {
			t.Fatalf("unexpected %d %s", out.Code, out.Body)
		}
	}
	if success != 1 {
		t.Fatalf("concurrent grants=%d", success)
	}
	req.RangeID = "stale"
	if out := copyCall(f, "checkpoint", task.ID, req); out.Code != 409 {
		t.Fatal("stale completion accepted")
	}
	req.RangeID = rangeID
	if out := copyCall(f, "checkpoint", task.ID, req); out.Code != 200 {
		t.Fatal(out.Body)
	}
	req.RangeID = ""
	if out := copyCall(f, "range", task.ID, req); out.Code != 200 {
		t.Fatal("completed range blocked next grant", out.Body)
	}

}

func TestNASR2CopyCleanupVersionSetIsExact(t *testing.T) {
	for _, v := range []string{nasCleanupRollingCapsClientVersion, nasCleanupInventoryClientVersion, string(nasCleanupV6R2Copy)} {
		if !nasCleanupSupportsV6(v) {
			t.Fatalf("v6 release refused %s", v)
		}
	}
	for _, v := range []string{"", "development", "r2-copy-v2", "cleanup-batch-v5", "cleanup-batch-v6-extra"} {
		if nasCleanupSupportsV6(v) {
			t.Fatalf("unreviewed v6 release accepted %s", v)
		}
	}
}

func TestNASR2CopyScopedRouteShapes(t *testing.T) {
	for _, path := range []string{"/api/v1/account/connections/r2-copy/lease", "/api/v1/account/connections/r2-copy/1/checkpoint", "/api/v1/account/connections/r2-copy/1/range", "/api/v1/account/connections/r2-copy/1/result"} {
		if !pullPathAllowed(http.MethodPost, path) {
			t.Errorf("required copy route denied: %s", path)
		}
		for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodDelete} {
			if pullPathAllowed(method, path) {
				t.Errorf("copy route admits %s %s", method, path)
			}
		}
	}
	for _, path := range []string{"/api/v1/account/connections/r2-copy/1/delete", "/api/v1/account/connections/r2-copy/1/range/extra", "/api/v1/account/connections/r2-copy/../range", "/api/v1/account/connections/r2-copy/lease/extra", "/api/v1/service/nas-r2-copy", "/api/v1/account/connections/r2-copy/1/range?extra"} {
		if pullPathAllowed(http.MethodPost, path) {
			t.Errorf("unexpected copy authority: %s", path)
		}
	}
}

func TestNASR2CopyScopedOtherConnectionDenied(t *testing.T) {
	f, _ := copyTestFixture(t)
	f.s.cfg.NASR2CopyEnabled = true
	if _, err := f.pool.Exec(f.ctx, `UPDATE connections SET client_version=$1 WHERE id=14`, f.version); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/account/connections/r2-copy/lease", strings.NewReader(fmt.Sprintf(`{"free_bytes":%d}`, NASR2CopyFloor+1_000_000_000)))
	req.Header.Set("User-Agent", "stoarama-nas-pull/"+f.version)
	req = req.WithContext(context.WithValue(req.Context(), accountPrincipalContextKey, accountPrincipal{AccountID: 48, APIKeyID: ptrInt64(6), KeyScopes: []string{accountScopePull}}))
	out := httptest.NewRecorder()
	f.s.confineAccountScope(f.s.fenceNASDelivery(http.HandlerFunc(f.s.handleNASR2CopyLease))).ServeHTTP(out, req)
	if out.Code != 403 || !strings.Contains(out.Body.String(), "connection 13") {
		t.Fatalf("foreign valid pull key response %d %s", out.Code, out.Body)
	}
}
