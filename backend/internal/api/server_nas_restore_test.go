package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/daydemir/stoarama/backend/internal/r2"
)

// fakeRestoreBucket is a path-style, in-memory S3 endpoint that enforces the
// If-None-Match: * but, like a store that ignores the query-string checksum,
// accepts any bytes of the signed length. It has no DELETE: R2 deletes are
// retired and a restore must never send one.
type fakeRestoreBucket struct {
	mu      sync.Mutex
	objects map[string][]byte
}

func (f *fakeRestoreBucket) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	etag := func(b []byte) string { s := sha256.Sum256(b); return `"` + hex.EncodeToString(s[:16]) + `"` }
	key := strings.TrimPrefix(r.URL.Path, "/bucket/")
	switch r.Method {
	case http.MethodPut:
		body, _ := io.ReadAll(r.Body)
		if _, exists := f.objects[key]; exists && r.Header.Get("If-None-Match") == "*" {
			w.WriteHeader(http.StatusPreconditionFailed)
			return
		}
		f.objects[key] = body
		w.Header().Set("ETag", etag(body))
	case http.MethodHead, http.MethodGet:
		body, ok := f.objects[key]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if m := r.Header.Get("If-Match"); m != "" && m != etag(body) {
			w.WriteHeader(http.StatusPreconditionFailed)
			return
		}
		w.Header().Set("ETag", etag(body))
		w.Header().Set("Content-Length", fmt.Sprint(len(body)))
		if r.Method == http.MethodGet {
			_, _ = w.Write(body)
		}
	default:
		http.Error(w, "unexpected "+r.Method, http.StatusBadRequest)
	}
}

func (f *fakeRestoreBucket) get(key string) ([]byte, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	b, ok := f.objects[key]
	return b, ok
}

func TestValidateNASRestoreResult(t *testing.T) {
	now := time.Now()
	started := now.Add(-time.Minute)
	valid := nasRestoreResultRequest{Attempt: 1, Outcome: "uploaded", BytesUploaded: 10, DurationMS: 5, StartedAt: &started}
	if err := validateNASRestoreResult(valid, 10, now); err != nil {
		t.Fatalf("valid result rejected: %v", err)
	}
	future := now.Add(time.Hour)
	for name, mutate := range map[string]func(*nasRestoreResultRequest){
		"zero attempt":      func(r *nasRestoreResultRequest) { r.Attempt = 0 },
		"bad outcome":       func(r *nasRestoreResultRequest) { r.Outcome = "done" },
		"oversize":          func(r *nasRestoreResultRequest) { r.BytesUploaded = 11 },
		"negative duration": func(r *nasRestoreResultRequest) { r.DurationMS = -1 },
		"future start":      func(r *nasRestoreResultRequest) { r.StartedAt = &future },
		"failure no error":  func(r *nasRestoreResultRequest) { r.Outcome = "local_missing" },
		"long error":        func(r *nasRestoreResultRequest) { r.Outcome, r.Error = "upload_failed", strings.Repeat("x", 501) },
		"bad version":       func(r *nasRestoreResultRequest) { r.ClientVersion = "a b" },
	} {
		req := valid
		mutate(&req)
		if err := validateNASRestoreResult(req, 10, now); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
}

// The SQL gate reads the same 0236 manifest the server embeds for cleanup.
func TestNASRestoreKeepEverythingMatchesEmbeddedPolicy(t *testing.T) {
	pool := dayTestPool(t)
	var sqlKE []int64
	if err := pool.QueryRow(context.Background(), `SELECT array_agg(stream_id ORDER BY stream_id) FROM nas_media_keep20_windows WHERE keep_everything`).Scan(&sqlKE); err != nil {
		t.Fatal(err)
	}
	policy, err := cleanupEmbeddedPolicy()
	if err != nil {
		t.Fatal(err)
	}
	var goKE []int64
	for id, w := range policy.Streams {
		if w.KeepEverything {
			goKE = append(goKE, id)
		}
	}
	slices.Sort(goKE)
	if len(goKE) == 0 || !slices.Equal(goKE, sqlKE) {
		t.Fatalf("embedded Keep Everything streams %v differ from SQL %v", goKE, sqlKE)
	}
}

func TestNASRestoreKeepEverythingOriginalLifecycle(t *testing.T) {
	pool := dayTestPool(t)
	ctx := context.Background()
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := pool.Exec(ctx, sql, args...); err != nil {
			t.Fatal(err)
		}
	}
	// Stream 78 is pinned keep_everything in the manifest; stream 3 is not.
	const keStream, otherStream = int64(78), int64(3)
	exec(`
INSERT INTO accounts(id,email) VALUES(47,'nas-restore@example.invalid'),(48,'other@example.invalid');
INSERT INTO account_api_keys(id,account_id,key_prefix,secret_hash) VALUES(47,47,'fixture-key','fixture-key'),(48,48,'other-key','other-key');
INSERT INTO connections(id,account_id,kind,api_key_id) VALUES(13,47,'nas_pull',47),(14,48,'nas_pull',48);
INSERT INTO streams(id,provider,external_id,name,slug,source_url,recording_state,local_timezone,capture_type,source_family,execution_class,capture_family,expected_fps)
 VALUES(3,'test','3','test','test','https://storage.example.test','off','Asia/Tokyo','hls','video_manifest','video_live','continuous_video',30),
       (78,'test','78','ke','ke','https://storage.example.test','off','Asia/Seoul','hls','video_manifest','video_live','continuous_video',30);
INSERT INTO storage_destinations(id,account_id,name,endpoint,region,bucket,access_key_id,secret_access_key_enc)
 VALUES(1,47,'test','https://storage.example.test','auto','bucket','fixture',decode('00','hex'));
INSERT INTO recordings(id,account_id,storage_destination_id,name,stream_url,cron_expr,cron_timezone,status,stream_id,paused_at)
 VALUES(30,47,1,'other','https://storage.example.test','* * * * *','Asia/Tokyo','paused',3,now()),
       (31,47,1,'ke','https://storage.example.test','* * * * *','Asia/Seoul','paused',78,now())`)

	type clip struct {
		id   int64
		key  string
		body []byte
		sha  string
	}
	nextClip := int64(900)
	newClip := func(stream int64, name string) clip {
		t.Helper()
		nextClip++
		recording := map[int64]int64{otherStream: 30, keStream: 31}[stream]
		c := clip{id: nextClip, key: "managed/acct-47/" + name + ".mp4", body: []byte("bytes of " + name)}
		sum := sha256.Sum256(c.body)
		c.sha = hex.EncodeToString(sum[:])
		exec(`INSERT INTO recording_clips(id,recording_id,storage_destination_id,endpoint,bucket,object_key,size_bytes,etag,sha256,
 fire_at,clip_start_at,clip_end_at,display_path,purged_at) VALUES($1,$2,1,'https://storage.example.test','bucket',$3,$4,'etag',$5,
 now()-interval '1 day',now()-interval '1 day',now()-interval '1 day'+interval '1 minute',$6,now()-interval '1 hour')`,
			c.id, recording, c.key, len(c.body), c.sha, name+".mp4")
		return c
	}
	request := func(c clip, target string) (int64, error) {
		key := c.key
		if target == "test" {
			key = fmt.Sprintf("nas-restore-test/t1/%d/%s", c.id, strings.TrimPrefix(c.key, "managed/acct-47/"))
		}
		var id int64
		err := pool.QueryRow(ctx, `INSERT INTO nas_restore_requests(connection_id,clip_id,recording_id,target,object_key,source_object_key,relative_path,content_type,size_bytes,sha256,label)
			SELECT 13,$1,recording_id,$2,$3,$4,$5,'video/mp4',$6,$7,'t1' FROM recording_clips WHERE id=$1 RETURNING id`,
			c.id, target, key, c.key, strings.TrimPrefix(c.key, "managed/acct-47/"), len(c.body), c.sha).Scan(&id)
		return id, err
	}
	mustRequest := func(c clip, target string) int64 {
		t.Helper()
		id, err := request(c, target)
		if err != nil {
			t.Fatal(err)
		}
		return id
	}

	// Rows queued before the fence, as in production. None of the non-KE or
	// test rows may ever move.
	otherOriginalID := mustRequest(newClip(otherStream, "other-original"), "original")
	keTestID := mustRequest(newClip(keStream, "ke-sample"), "test")
	otherLeasedID := mustRequest(newClip(otherStream, "other-leased"), "original")
	exec(`UPDATE nas_restore_requests SET state='leased',attempts=1,lease_expires_at=now()-interval '1 hour' WHERE id=$1`, otherLeasedID)
	// A KE original whose row drifted from the catalog bytes must never be signed.
	driftedID := mustRequest(newClip(keStream, "ke-drifted"), "original")
	exec(`UPDATE nas_restore_requests SET sha256=repeat('0',64) WHERE id=$1`, driftedID)
	original := newClip(keStream, "ke-original")
	originalID := mustRequest(original, "original")
	occupied := newClip(keStream, "ke-occupied")
	occupiedID := mustRequest(occupied, "original")
	tampered := newClip(keStream, "ke-tampered")
	tamperedID := mustRequest(tampered, "original")
	missing := newClip(keStream, "ke-missing")
	missingID := mustRequest(missing, "original")
	// recording_clips inserts are fenced too: catalogue every clip first.
	otherLate, keLateTest := newClip(otherStream, "other-late"), newClip(keStream, "ke-late-test")
	late, ghost := newClip(keStream, "ke-late"), newClip(keStream, "ke-ghost")

	exec(`INSERT INTO r2_retirement_writer_fences(bucket,endpoint_sha256) VALUES('stoarama',repeat('a',64))`)
	// The fence still refuses every other restore row, and admits a KE original.
	if _, err := request(otherLate, "original"); err == nil || !strings.Contains(err.Error(), "fenced") {
		t.Fatalf("non-KE original queued past the writer fence: %v", err)
	}
	if _, err := request(keLateTest, "test"); err == nil || !strings.Contains(err.Error(), "fenced") {
		t.Fatalf("KE test restore queued past the writer fence: %v", err)
	}
	lateID := mustRequest(late, "original")

	frozen := func() string {
		t.Helper()
		var raw string
		if err := pool.QueryRow(ctx, `SELECT jsonb_build_array(
			(SELECT jsonb_agg(to_jsonb(q) ORDER BY id) FROM nas_restore_requests q WHERE id=ANY($1)),
			(SELECT jsonb_agg(to_jsonb(c) ORDER BY id) FROM recording_clips c WHERE id IN (SELECT clip_id FROM nas_restore_requests WHERE id=ANY($1))))::text`,
			[]int64{otherOriginalID, keTestID, otherLeasedID, driftedID}).Scan(&raw); err != nil {
			t.Fatal(err)
		}
		return raw
	}
	before := frozen()

	bucket := &fakeRestoreBucket{objects: map[string][]byte{occupied.key: []byte("someone else's bytes")}}
	server := httptest.NewServer(bucket)
	defer server.Close()
	client, err := r2.New(ctx, r2.Config{UngatedWriter: true, AccessKey: "key", SecretKey: "secret", Region: "auto", Bucket: "bucket", Endpoint: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{pool: pool, r2: client}
	router := chi.NewRouter()
	router.Post("/lease", s.handleAccountConnectionRestoreLease)
	router.Post("/result/{taskId}", s.handleAccountConnectionRestoreResult)
	call := func(keyID int64, path, body string) (int, string) {
		req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
		req = req.WithContext(context.WithValue(req.Context(), accountPrincipalContextKey,
			accountPrincipal{AccountID: keyID, APIKeyID: ptrInt64(keyID)}))
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		return rec.Code, rec.Body.String()
	}
	lease := func(max int) map[int64]nasRestoreTask {
		t.Helper()
		code, body := call(47, "/lease", fmt.Sprintf(`{"client_version":"abc123","max_tasks":%d}`, max))
		if code != http.StatusOK {
			t.Fatalf("lease status=%d body=%s", code, body)
		}
		var resp nasRestoreLeaseResponse
		if err := json.Unmarshal([]byte(body), &resp); err != nil {
			t.Fatal(err)
		}
		out := map[int64]nasRestoreTask{}
		for _, task := range resp.Tasks {
			out[task.TaskID] = task
		}
		if len(out) > 0 && resp.RetryAfterSec != nasRestoreBusyRetrySec || len(out) == 0 && resp.RetryAfterSec != nasRestoreIdleRetrySec {
			t.Fatalf("retry_after=%d with %d tasks", resp.RetryAfterSec, len(out))
		}
		return out
	}
	put := func(task nasRestoreTask, body []byte) int {
		t.Helper()
		req, err := http.NewRequest(task.Put.Method, task.Put.URL, bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		for k, v := range task.Put.Headers {
			req.Header.Set(k, v)
		}
		req.ContentLength = int64(len(body))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	report := func(id int64, attempt int, outcome, errText string) (int, string) {
		return call(47, fmt.Sprintf("/result/%d", id), fmt.Sprintf(
			`{"attempt":%d,"outcome":%q,"error":%q,"bytes_uploaded":0,"duration_ms":7,"started_at":%q,"client_version":"abc123"}`,
			attempt, outcome, errText, time.Now().UTC().Format(time.RFC3339)))
	}
	state := func(id int64) (string, string) {
		t.Helper()
		var st, outcome string
		if err := pool.QueryRow(ctx, `SELECT state,outcome FROM nas_restore_requests WHERE id=$1`, id).Scan(&st, &outcome); err != nil {
			t.Fatal(err)
		}
		return st, outcome
	}

	if code, _ := call(47, "/lease", `{"max_tasks":0}`); code != http.StatusBadRequest {
		t.Fatalf("max_tasks=0 status=%d", code)
	}
	tasks := lease(64)
	var leased []int64
	for id := range tasks {
		leased = append(leased, id)
	}
	slices.Sort(leased)
	if want := []int64{originalID, occupiedID, tamperedID, missingID, lateID}; !slices.Equal(leased, want) {
		t.Fatalf("leased %v, want only the KE originals %v", leased, want)
	}
	if again := lease(64); len(again) != 0 {
		t.Fatalf("leased tasks were re-issued: %v", again)
	}
	// The capability is visible to the R2 delete gate for its whole life, and
	// no fenced writer admission was recorded.
	var putCovers bool
	var fencedCapabilities int
	if err := pool.QueryRow(ctx, `SELECT bool_and(put_expires_at>lease_expires_at+interval '1 minute'),(SELECT count(*) FROM r2_retirement_writer_capabilities)
		FROM nas_restore_requests WHERE state='leased' AND id<>$1`, otherLeasedID).Scan(&putCovers, &fencedCapabilities); err != nil || !putCovers || fencedCapabilities != 0 {
		t.Fatalf("put window covers=%v fenced capabilities=%d err=%v", putCovers, fencedCapabilities, err)
	}

	// Anything the lease gate would not issue is refused before verification
	// or any state change.
	for _, id := range []int64{otherOriginalID, keTestID, otherLeasedID, driftedID} {
		if code, body := report(id, 1, "uploaded", ""); code != http.StatusConflict {
			t.Fatalf("report on non-KE-original %d status=%d body=%s", id, code, body)
		}
	}
	if after := frozen(); after != before {
		t.Fatalf("non-KE-original rows or clips changed:\nbefore %s\nafter  %s", before, after)
	}

	orig := tasks[originalID]
	if orig.Attempt != 1 || orig.SHA256 != original.sha || orig.RelativePath != "ke-original.mp4" || orig.Put.Method != http.MethodPut ||
		!strings.Contains(orig.Put.URL, "/bucket/"+original.key) || !strings.Contains(orig.Put.URL, "X-Amz-Checksum-Sha256=") ||
		orig.Put.Headers["If-None-Match"] != "*" || orig.Put.Headers["Host"] != "" {
		t.Fatalf("original task=%+v", orig)
	}

	// Original restore: create-only PUT, full re-hash, purged_at cleared.
	if code := put(orig, original.body); code != http.StatusOK {
		t.Fatalf("original put status=%d", code)
	}
	if code, body := call(48, fmt.Sprintf("/result/%d", originalID), `{"attempt":1,"outcome":"uploaded"}`); code != http.StatusNotFound {
		t.Fatalf("foreign connection report status=%d body=%s", code, body)
	}
	if code, body := report(originalID, 1, "uploaded", ""); code != http.StatusOK || !strings.Contains(body, `"clip_restored":true`) {
		t.Fatalf("original report status=%d body=%s", code, body)
	}
	var purged bool
	if err := pool.QueryRow(ctx, `SELECT purged_at IS NOT NULL FROM recording_clips WHERE id=$1`, original.id).Scan(&purged); err != nil || purged {
		t.Fatalf("original clip still purged=%v err=%v", purged, err)
	}
	if code, _ := report(originalID, 1, "uploaded", ""); code != http.StatusConflict {
		t.Fatalf("duplicate report status=%d", code)
	}

	// An occupied key rejects the create-only PUT; wrong bytes there fail the
	// row and are never touched (deletes are retired).
	if code := put(tasks[occupiedID], occupied.body); code != http.StatusPreconditionFailed {
		t.Fatalf("occupied put status=%d", code)
	}
	if code, body := report(occupiedID, 1, "exists", ""); code != http.StatusOK || !strings.Contains(body, `"state":"failed"`) {
		t.Fatalf("occupied report status=%d body=%s", code, body)
	}
	if st, outcome := state(occupiedID); st != "failed" || outcome != "r2_mismatch" {
		t.Fatalf("occupied state=%s outcome=%s", st, outcome)
	}
	if b, _ := bucket.get(occupied.key); string(b) != "someone else's bytes" {
		t.Fatal("occupied object was modified")
	}

	// Storage that ignores the signed checksum: the full re-hash catches it.
	if code := put(tasks[tamperedID], []byte("bytes of KE-TAMPERED")); code != http.StatusOK {
		t.Fatalf("tampered put status=%d", code)
	}
	if code, body := report(tamperedID, 1, "uploaded", ""); code != http.StatusOK || !strings.Contains(body, `"state":"failed"`) || strings.Contains(body, `"clip_restored":true`) {
		t.Fatalf("tampered report status=%d body=%s", code, body)
	}

	// A missing local copy is permanent.
	if code, _ := report(missingID, 1, "local_missing", ""); code != http.StatusBadRequest {
		t.Fatalf("failure without error status=%d", code)
	}
	if code, body := report(missingID, 1, "local_missing", "no such file"); code != http.StatusOK || !strings.Contains(body, `"state":"failed"`) {
		t.Fatalf("missing report status=%d body=%s", code, body)
	}

	// An expired lease is re-queued; a late upload from the old lease makes the
	// next attempt report 'exists', which verifies the same bytes.
	if code := put(tasks[lateID], late.body); code != http.StatusOK {
		t.Fatalf("late put status=%d", code)
	}
	exec(`UPDATE nas_restore_requests SET lease_expires_at=now()-interval '1 minute' WHERE id=$1`, lateID)
	second := lease(64)
	if len(second) != 1 || second[lateID].Attempt != 2 {
		t.Fatalf("expired lease re-issued %+v", second)
	}
	if code := put(second[lateID], late.body); code != http.StatusPreconditionFailed {
		t.Fatalf("second put status=%d", code)
	}
	if code, body := report(lateID, 2, "exists", ""); code != http.StatusOK || !strings.Contains(body, `"clip_restored":true`) {
		t.Fatalf("exists report status=%d body=%s", code, body)
	}

	// An upload the storage never received is retried until max_attempts.
	ghostID := mustRequest(ghost, "original")
	exec(`UPDATE nas_restore_requests SET max_attempts=1 WHERE id=$1`, ghostID)
	lease(64)
	if code, body := report(ghostID, 1, "uploaded", ""); code != http.StatusOK || !strings.Contains(body, `"state":"failed"`) {
		t.Fatalf("ghost report status=%d body=%s", code, body)
	}
	if st, outcome := state(ghostID); st != "failed" || outcome != "verify_failed" {
		t.Fatalf("ghost state=%s outcome=%s", st, outcome)
	}

	if after := frozen(); after != before {
		t.Fatalf("non-KE-original rows or clips changed:\nbefore %s\nafter  %s", before, after)
	}
}

// A stitched output whose R2 object is gone goes back to its own key from the
// NAS copy the connection acknowledged, only while the controller scopes it and
// only with the output's exact identity. Nothing else about the output changes.
func TestNASRestoreScopedOutputLifecycle(t *testing.T) {
	pool := dayTestPool(t)
	ctx := context.Background()
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := pool.Exec(ctx, sql, args...); err != nil {
			t.Fatal(err)
		}
	}
	exec(`
INSERT INTO accounts(id,email) VALUES(47,'nas-restore@example.invalid');
INSERT INTO account_api_keys(id,account_id,key_prefix,secret_hash) VALUES(47,47,'fixture-key','fixture-key');
INSERT INTO connections(id,account_id,kind,api_key_id) VALUES(13,47,'nas_pull',47);
INSERT INTO streams(id,provider,external_id,name,slug,source_url,recording_state,local_timezone,capture_type,source_family,execution_class,capture_family,expected_fps)
 VALUES(3,'test','3','test','test','https://storage.example.test','off','Asia/Tokyo','hls','video_manifest','video_live','continuous_video',30);
INSERT INTO storage_destinations(id,account_id,name,endpoint,region,bucket,access_key_id,secret_access_key_enc)
 VALUES(1,47,'test','https://storage.example.test','auto','bucket','fixture',decode('00','hex'));
INSERT INTO recordings(id,account_id,storage_destination_id,name,stream_url,cron_expr,cron_timezone,status,stream_id,paused_at)
 VALUES(30,47,1,'rec','https://storage.example.test','* * * * *','Asia/Tokyo','paused',3,now());
BEGIN; SET LOCAL session_replication_role=replica;
INSERT INTO recording_collation_hours(id,hour_id,batch_id,generation,recording_id,local_date,delivery_hour,status,manifest_object_key,manifest_sha256,
 manifest,source_clip_count,included_clip_count,excluded_clip_count,join_count,split_count)
 VALUES(1,'nightly-collation-v2__recording-30__date-2026-10-01__hour-05__generation-2','nightly-collation-v2',2,30,'2026-10-01',5,'collated','m/1',repeat('a',64),'{}',1,1,0,0,0);
COMMIT`)

	// Catalogue fixtures bypass the catalogue's own admission triggers.
	replica := func(stmts ...[]any) {
		t.Helper()
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		if _, err := tx.Exec(ctx, `SET LOCAL session_replication_role=replica`); err != nil {
			t.Fatal(err)
		}
		for _, stmt := range stmts {
			if _, err := tx.Exec(ctx, stmt[0].(string), stmt[1:]...); err != nil {
				t.Fatal(err)
			}
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	}
	type output struct {
		id   int64
		key  string
		path string
		body []byte
		sha  string
	}
	newOutput := func(id int64, scoped bool) output {
		t.Helper()
		o := output{id: id, body: []byte(fmt.Sprintf("stitched hour %d", id))}
		sum := sha256.Sum256(o.body)
		o.sha = hex.EncodeToString(sum[:])
		o.key = "joined/nightly-collation-v2/objects/" + o.sha + ".mp4"
		o.path = fmt.Sprintf("Rec/October/01-Wednesday/Rec_hour_05_part_%02d_050000-060000.mp4", id)
		replica(
			[]any{`INSERT INTO recording_collation_outputs(id,collation_hour_id,part,parts,object_key,nas_relative_path,size_bytes,sha256,start_at,end_at,source_clip_ids,r2_verified_at)
 VALUES($1::bigint,1,$1::bigint::int,20,$2,$3,$4,$5,'2026-09-30T20:00:00Z','2026-09-30T21:00:00Z',ARRAY[1],now())`, o.id, o.key, o.path, len(o.body), o.sha},
			[]any{`INSERT INTO recording_collation_output_acks(output_id,connection_id,nas_relative_path,size_bytes,sha256) VALUES($1,13,$2,$3,$4)`, o.id, o.path, len(o.body), o.sha},
			[]any{`INSERT INTO r2_object_retirements(kind,source_id,stream_id,bucket,object_key,size_bytes,sha256,reason,choice,manifest_sha256,policy_sha256)
 VALUES('collated',$1,3,'bucket',$2,$3,$4,'fixture','none',repeat('e',64),repeat('e',64))`, o.id, o.key, len(o.body), o.sha})
		if scoped {
			exec(`INSERT INTO nas_restore_output_scope(output_id) VALUES($1)`, o.id)
		}
		return o
	}
	request := func(o output, mutate string) (int64, error) {
		var id int64
		err := pool.QueryRow(ctx, `INSERT INTO nas_restore_requests(connection_id,output_id,recording_id,target,object_key,source_object_key,relative_path,content_type,size_bytes,sha256,label)
			VALUES(13,$1,30,'output',$2,$2,'joined/'||$3,'video/mp4',$4,$5,'o1') RETURNING id`, o.id, o.key, o.path, len(o.body), o.sha).Scan(&id)
		if err == nil && mutate != "" {
			_, err = pool.Exec(ctx, `UPDATE nas_restore_requests SET `+mutate+` WHERE id=$1`, id)
		}
		return id, err
	}
	mustRequest := func(o output, mutate string) int64 {
		t.Helper()
		id, err := request(o, mutate)
		if err != nil {
			t.Fatal(err)
		}
		return id
	}

	// Queued before the fence. Unscoped and drifted rows must never move.
	unscopedID := mustRequest(newOutput(1, false), "")
	sizeDriftID := mustRequest(newOutput(2, true), "size_bytes=size_bytes+1")
	shaDriftID := mustRequest(newOutput(3, true), "sha256=repeat('0',64)")
	pathDriftID := mustRequest(newOutput(4, true), "relative_path='joined/Rec/October/01-Wednesday/Rec_hour_06_060000-070000.mp4'")
	ackDrift := newOutput(5, true)
	replica(
		[]any{`DELETE FROM recording_collation_output_acks WHERE output_id=$1`, ackDrift.id},
		[]any{`INSERT INTO recording_collation_output_acks(output_id,connection_id,nas_relative_path,size_bytes,sha256) VALUES($1,13,$2,$3,repeat('0',64))`, ackDrift.id, ackDrift.path, len(ackDrift.body)})
	ackDriftID := mustRequest(ackDrift, "")
	restored := newOutput(6, true)
	restoredID := mustRequest(restored, "")
	occupied := newOutput(7, true)
	occupiedID := mustRequest(occupied, "")
	unscopedLate, scopedLate := newOutput(8, false), newOutput(9, true)

	exec(`INSERT INTO r2_retirement_writer_fences(bucket,endpoint_sha256) VALUES('stoarama',repeat('a',64))`)
	if _, err := request(unscopedLate, ""); err == nil || !strings.Contains(err.Error(), "fenced") {
		t.Fatalf("unscoped output queued past the writer fence: %v", err)
	}
	lateID := mustRequest(scopedLate, "")
	// Past the fence, a scoped output row that drifted from its identity, or
	// names another media type, is refused like any other fenced writer.
	n := len(scopedLate.body)
	for _, drift := range []struct {
		size       int
		sha, ctype string
	}{{n + 1, scopedLate.sha, "video/mp4"}, {n, strings.Repeat("0", 64), "video/mp4"}, {n, scopedLate.sha, "text/html"}} {
		_, err := pool.Exec(ctx, `INSERT INTO nas_restore_requests(connection_id,output_id,recording_id,target,object_key,source_object_key,relative_path,size_bytes,sha256,content_type,label)
			VALUES(13,$1,30,'output',$2,$2,'joined/'||$3,$4,$5,$6,'o1')`, scopedLate.id, scopedLate.key, scopedLate.path, drift.size, drift.sha, drift.ctype)
		if err == nil || !strings.Contains(err.Error(), "fenced") {
			t.Fatalf("drifted output row %+v queued past the writer fence: %v", drift, err)
		}
	}

	frozen := func() string {
		t.Helper()
		var raw string
		if err := pool.QueryRow(ctx, `SELECT jsonb_build_array(
			(SELECT jsonb_agg(to_jsonb(q) ORDER BY id) FROM nas_restore_requests q WHERE id=ANY($1)),
			(SELECT jsonb_agg(to_jsonb(o) ORDER BY id) FROM recording_collation_outputs o),
			(SELECT jsonb_agg(to_jsonb(a) ORDER BY output_id) FROM recording_collation_output_acks a),
			(SELECT jsonb_agg(to_jsonb(r) ORDER BY id) FROM r2_object_retirements r))::text`,
			[]int64{unscopedID, sizeDriftID, shaDriftID, pathDriftID, ackDriftID}).Scan(&raw); err != nil {
			t.Fatal(err)
		}
		return raw
	}
	before := frozen()

	bucket := &fakeRestoreBucket{objects: map[string][]byte{occupied.key: []byte("someone else's bytes")}}
	server := httptest.NewServer(bucket)
	defer server.Close()
	client, err := r2.New(ctx, r2.Config{UngatedWriter: true, AccessKey: "key", SecretKey: "secret", Region: "auto", Bucket: "bucket", Endpoint: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{pool: pool, r2: client}
	router := chi.NewRouter()
	router.Post("/lease", s.handleAccountConnectionRestoreLease)
	router.Post("/result/{taskId}", s.handleAccountConnectionRestoreResult)
	call := func(path, body string) (int, string) {
		req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
		req = req.WithContext(context.WithValue(req.Context(), accountPrincipalContextKey,
			accountPrincipal{AccountID: 47, APIKeyID: ptrInt64(47)}))
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		return rec.Code, rec.Body.String()
	}
	lease := func(outputs bool) map[int64]nasRestoreTask {
		t.Helper()
		code, body := call("/lease", fmt.Sprintf(`{"client_version":"abc123","max_tasks":64,"output_restores":%t}`, outputs))
		if code != http.StatusOK {
			t.Fatalf("lease status=%d body=%s", code, body)
		}
		var resp nasRestoreLeaseResponse
		if err := json.Unmarshal([]byte(body), &resp); err != nil {
			t.Fatal(err)
		}
		out := map[int64]nasRestoreTask{}
		for _, task := range resp.Tasks {
			out[task.TaskID] = task
		}
		return out
	}
	put := func(task nasRestoreTask, body []byte) int {
		t.Helper()
		req, err := http.NewRequest(task.Put.Method, task.Put.URL, bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		for k, v := range task.Put.Headers {
			req.Header.Set(k, v)
		}
		req.ContentLength = int64(len(body))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	report := func(id int64, outcome string) (int, string) {
		return call(fmt.Sprintf("/result/%d", id), fmt.Sprintf(
			`{"attempt":1,"outcome":%q,"bytes_uploaded":0,"duration_ms":7,"client_version":"abc123"}`, outcome))
	}

	// A client that cannot read joined/ back never gets an output task.
	if tasks := lease(false); len(tasks) != 0 {
		t.Fatalf("output tasks leased to a client without output_restores: %v", tasks)
	}
	tasks := lease(true)
	var leased []int64
	for id := range tasks {
		leased = append(leased, id)
	}
	slices.Sort(leased)
	if want := []int64{restoredID, occupiedID, lateID}; !slices.Equal(leased, want) {
		t.Fatalf("leased %v, want only the scoped, exact outputs %v", leased, want)
	}
	var putCovers bool
	var fencedCapabilities int
	if err := pool.QueryRow(ctx, `SELECT bool_and(put_expires_at>lease_expires_at+interval '1 minute'),(SELECT count(*) FROM r2_retirement_writer_capabilities)
		FROM nas_restore_requests WHERE state='leased'`).Scan(&putCovers, &fencedCapabilities); err != nil || !putCovers || fencedCapabilities != 0 {
		t.Fatalf("put window covers=%v fenced capabilities=%d err=%v", putCovers, fencedCapabilities, err)
	}
	for _, id := range []int64{unscopedID, sizeDriftID, shaDriftID, pathDriftID, ackDriftID} {
		if code, body := report(id, "uploaded"); code != http.StatusConflict {
			t.Fatalf("report on unadmitted output restore %d status=%d body=%s", id, code, body)
		}
	}

	task := tasks[restoredID]
	if task.OutputID != restored.id || task.ClipID != 0 || task.RelativePath != "joined/"+restored.path || task.SHA256 != restored.sha ||
		task.SizeBytes != int64(len(restored.body)) || !strings.Contains(task.Put.URL, "/bucket/"+restored.key) ||
		!strings.Contains(task.Put.URL, "X-Amz-Checksum-Sha256=") || task.Put.Headers["If-None-Match"] != "*" {
		t.Fatalf("output task=%+v", task)
	}
	if code := put(task, restored.body); code != http.StatusOK {
		t.Fatalf("output put status=%d", code)
	}
	// Create-only: a second PUT at the restored key is refused by storage.
	if code := put(task, []byte("other bytes, same len")); code != http.StatusPreconditionFailed {
		t.Fatalf("repeat put status=%d", code)
	}
	if code, body := report(restoredID, "uploaded"); code != http.StatusOK || !strings.Contains(body, `"state":"verified"`) || !strings.Contains(body, `"clip_restored":false`) {
		t.Fatalf("output report status=%d body=%s", code, body)
	}
	if b, _ := bucket.get(restored.key); !bytes.Equal(b, restored.body) {
		t.Fatalf("restored object %q", b)
	}

	// An object already at the key with other bytes fails the row and stays.
	if code := put(tasks[occupiedID], occupied.body); code != http.StatusPreconditionFailed {
		t.Fatalf("occupied put status=%d", code)
	}
	if code, body := report(occupiedID, "exists"); code != http.StatusOK || !strings.Contains(body, `"state":"failed"`) {
		t.Fatalf("occupied report status=%d body=%s", code, body)
	}
	var st, outcome string
	if err := pool.QueryRow(ctx, `SELECT state,outcome FROM nas_restore_requests WHERE id=$1`, occupiedID).Scan(&st, &outcome); err != nil || st != "failed" || outcome != "r2_mismatch" {
		t.Fatalf("occupied state=%s outcome=%s err=%v", st, outcome, err)
	}
	if b, _ := bucket.get(occupied.key); string(b) != "someone else's bytes" {
		t.Fatal("occupied object was modified")
	}

	// Withdrawing the scope stops a leased restore before verification.
	exec(`DELETE FROM nas_restore_output_scope WHERE output_id=$1`, scopedLate.id)
	if code := put(tasks[lateID], scopedLate.body); code != http.StatusOK {
		t.Fatalf("late put status=%d", code)
	}
	if code, body := report(lateID, "uploaded"); code != http.StatusConflict {
		t.Fatalf("report after scope withdrawal status=%d body=%s", code, body)
	}

	if after := frozen(); after != before {
		t.Fatalf("unadmitted rows, outputs, acks or retirements changed:\nbefore %s\nafter  %s", before, after)
	}
}
