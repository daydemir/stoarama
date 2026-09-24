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
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/daydemir/stoarama/backend/internal/r2"
)

// fakeRestoreBucket is a path-style, in-memory S3 endpoint that enforces the
// If-None-Match: * but, like a store that ignores the query-string checksum,
// accepts any bytes of the signed length.
type fakeRestoreBucket struct {
	mu      sync.Mutex
	objects map[string][]byte
	puts    int
}

func (f *fakeRestoreBucket) key(r *http.Request) string {
	return strings.TrimPrefix(r.URL.Path, "/bucket/")
}

func (f *fakeRestoreBucket) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	etag := func(b []byte) string { s := sha256.Sum256(b); return `"` + hex.EncodeToString(s[:16]) + `"` }
	switch {
	case r.Method == http.MethodPost:
		if _, ok := r.URL.Query()["delete"]; !ok {
			http.Error(w, "bad", http.StatusBadRequest)
			return
		}
		body, _ := io.ReadAll(r.Body)
		for _, m := range fakeDeleteKeyRe.FindAllStringSubmatch(string(body), -1) {
			delete(f.objects, m[1])
		}
		w.Header().Set("Content-Type", "application/xml")
		_, _ = io.WriteString(w, `<?xml version="1.0" encoding="UTF-8"?><DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></DeleteResult>`)
	case r.Method == http.MethodPut:
		f.puts++
		body, _ := io.ReadAll(r.Body)
		key := f.key(r)
		if r.Header.Get("If-None-Match") == "*" {
			if _, exists := f.objects[key]; exists {
				w.WriteHeader(http.StatusPreconditionFailed)
				return
			}
		}
		f.objects[key] = body
		w.Header().Set("ETag", etag(body))
	case r.Method == http.MethodHead || r.Method == http.MethodGet:
		body, ok := f.objects[f.key(r)]
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
		http.Error(w, "unexpected", http.StatusBadRequest)
	}
}

func (f *fakeRestoreBucket) get(key string) ([]byte, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	b, ok := f.objects[key]
	return b, ok
}

func (f *fakeRestoreBucket) set(key string, b []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.objects[key] = b
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

func TestNASRestoreLifecycle(t *testing.T) {
	pool, cleanup := testAccountClipsPool(t)
	defer cleanup()
	ctx := context.Background()
	migration, err := os.ReadFile("../../../infra/sql/migrations/0157_nas_restore_requests.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, string(migration)); err != nil {
		t.Fatalf("apply migration: %v", err)
	}
	const accountID, apiKeyID, otherKeyID = int64(47), int64(123), int64(124)
	var connectionID, recordingID int64
	if err := pool.QueryRow(ctx, `INSERT INTO connections(account_id,kind,api_key_id) VALUES($1,'nas_pull',$2) RETURNING id`, accountID, apiKeyID).Scan(&connectionID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO connections(account_id,kind,api_key_id) VALUES($1,'nas_pull',$2)`, accountID, otherKeyID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `INSERT INTO recordings(account_id,name,delivery) VALUES($1,'r','nas_pull') RETURNING id`, accountID).Scan(&recordingID); err != nil {
		t.Fatal(err)
	}
	type clip struct {
		id   int64
		key  string
		body []byte
		sha  string
	}
	newClip := func(name string, purged bool) clip {
		c := clip{key: "managed/acct-47/" + name + ".mp4", body: []byte("bytes of " + name)}
		sum := sha256.Sum256(c.body)
		c.sha = hex.EncodeToString(sum[:])
		var purgedAt any
		if purged {
			purgedAt = time.Now().Add(-time.Hour)
		}
		if err := pool.QueryRow(ctx, `INSERT INTO recording_clips(recording_id,size_bytes,object_key,sha256,clip_start_at,clip_end_at,display_path,purged_at)
			VALUES($1,$2,$3,$4,now()-interval '1 day',now()-interval '1 day'+interval '1 minute',$5,$6) RETURNING id`,
			recordingID, len(c.body), c.key, c.sha, name+".mp4", purgedAt).Scan(&c.id); err != nil {
			t.Fatal(err)
		}
		return c
	}
	request := func(c clip, target, key string) int64 {
		var id int64
		if err := pool.QueryRow(ctx, `INSERT INTO nas_restore_requests(connection_id,clip_id,recording_id,target,object_key,source_object_key,relative_path,content_type,size_bytes,sha256,label)
			VALUES($1,$2,$3,$4,$5,$6,$7,'video/mp4',$8,$9,'t1') RETURNING id`,
			connectionID, c.id, recordingID, target, key, c.key, strings.TrimPrefix(c.key, "managed/acct-47/"), len(c.body), c.sha).Scan(&id); err != nil {
			t.Fatal(err)
		}
		return id
	}
	original := newClip("original", true)
	testClip := newClip("sample", false)
	occupied := newClip("occupied", true)
	missing := newClip("missing", true)
	originalID := request(original, "original", original.key)
	testKey := fmt.Sprintf("nas-restore-test/t1/%d/sample.mp4", testClip.id)
	testID := request(testClip, "test", testKey)
	occupiedID := request(occupied, "original", occupied.key)
	missingID := request(missing, "original", missing.key)

	bucket := &fakeRestoreBucket{objects: map[string][]byte{occupied.key: []byte("someone else's bytes")}}
	server := httptest.NewServer(bucket)
	defer server.Close()
	client, err := r2.New(ctx, r2.Config{AccessKey: "key", SecretKey: "secret", Region: "auto", Bucket: "bucket", Endpoint: server.URL})
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
			accountPrincipal{AccountID: accountID, APIKeyID: ptrInt64(keyID)}))
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		return rec.Code, rec.Body.String()
	}
	lease := func(max int) map[int64]nasRestoreTask {
		code, body := call(apiKeyID, "/lease", fmt.Sprintf(`{"client_version":"abc123","max_tasks":%d}`, max))
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
	report := func(task nasRestoreTask, attempt int, outcome, errText string) (int, string) {
		return call(apiKeyID, fmt.Sprintf("/result/%d", task.TaskID), fmt.Sprintf(
			`{"attempt":%d,"outcome":%q,"error":%q,"bytes_uploaded":%d,"duration_ms":7,"started_at":%q,"client_version":"abc123"}`,
			attempt, outcome, errText, task.SizeBytes, time.Now().UTC().Format(time.RFC3339)))
	}
	state := func(id int64) (string, int, string) {
		var st, outcome string
		var attempts int
		if err := pool.QueryRow(ctx, `SELECT state,attempts,outcome FROM nas_restore_requests WHERE id=$1`, id).Scan(&st, &attempts, &outcome); err != nil {
			t.Fatal(err)
		}
		return st, attempts, outcome
	}

	if code, _ := call(apiKeyID, "/lease", `{"max_tasks":0}`); code != http.StatusBadRequest {
		t.Fatalf("max_tasks=0 status=%d", code)
	}
	tasks := lease(10)
	if len(tasks) != 4 {
		t.Fatalf("leased %d tasks", len(tasks))
	}
	if again := lease(10); len(again) != 0 {
		t.Fatalf("leased tasks were re-issued: %v", again)
	}
	orig := tasks[originalID]
	if orig.Attempt != 1 || orig.SHA256 != original.sha || orig.RelativePath != "original.mp4" || orig.Put.Method != http.MethodPut ||
		!strings.Contains(orig.Put.URL, "/bucket/"+original.key) || orig.Put.Headers["If-None-Match"] != "*" || orig.Put.Headers["Host"] != "" {
		t.Fatalf("original task=%+v", orig)
	}

	// Original restore: create-only PUT, full re-hash, purged_at cleared.
	if code := put(orig, original.body); code != http.StatusOK {
		t.Fatalf("original put status=%d", code)
	}
	if code, body := call(otherKeyID, fmt.Sprintf("/result/%d", originalID), `{"attempt":1,"outcome":"uploaded"}`); code != http.StatusNotFound {
		t.Fatalf("foreign connection report status=%d body=%s", code, body)
	}
	if code, body := report(orig, 1, "uploaded", ""); code != http.StatusOK || !strings.Contains(body, `"clip_restored":true`) {
		t.Fatalf("original report status=%d body=%s", code, body)
	}
	var purged bool
	if err := pool.QueryRow(ctx, `SELECT purged_at IS NOT NULL FROM recording_clips WHERE id=$1`, original.id).Scan(&purged); err != nil || purged {
		t.Fatalf("original clip still purged=%v err=%v", purged, err)
	}
	if code, _ := report(orig, 1, "uploaded", ""); code != http.StatusConflict {
		t.Fatalf("duplicate report status=%d", code)
	}

	// A body that does not match the signed checksum is rejected by storage;
	// the client reports upload_failed and the row is re-queued.
	sample := tasks[testID]
	// The signed checksum travels in the query string; do not assume storage
	// enforces it. Wrong bytes that land are caught by the full re-hash, the
	// object is deleted, and the row is re-queued.
	if !strings.Contains(sample.Put.URL, "X-Amz-Checksum-Sha256=") || sample.Put.Headers["If-None-Match"] != "*" {
		t.Fatalf("test put capability=%+v", sample.Put)
	}
	if code := put(sample, []byte("bytes of SAMPLE")); code != http.StatusOK {
		t.Fatalf("tampered put status=%d", code)
	}
	if code, body := report(sample, 1, "uploaded", ""); code != http.StatusOK || !strings.Contains(body, `"state":"pending"`) {
		t.Fatalf("tampered upload report status=%d body=%s", code, body)
	}
	if _, ok := bucket.get(testKey); ok {
		t.Fatal("mismatched test object was not deleted")
	}
	retry := lease(10)[testID]
	if retry.Attempt != 2 {
		t.Fatalf("re-leased attempt=%d", retry.Attempt)
	}
	if code := put(retry, testClip.body); code != http.StatusOK {
		t.Fatalf("test put status=%d", code)
	}
	if code, _ := report(sample, 1, "uploaded", ""); code != http.StatusConflict {
		t.Fatalf("stale attempt report status=%d", code)
	}
	if code, body := report(retry, 2, "uploaded", ""); code != http.StatusOK || !strings.Contains(body, `"state":"verified"`) || strings.Contains(body, `"clip_restored":true`) {
		t.Fatalf("test report status=%d body=%s", code, body)
	}
	if _, ok := bucket.get(testKey); ok {
		t.Fatal("verified test object was not deleted")
	}

	// An occupied key rejects the create-only PUT. Wrong bytes at a purged
	// clip's key are garbage from a bad attempt: deleted, then re-restored.
	occ := tasks[occupiedID]
	if code := put(occ, occupied.body); code != http.StatusPreconditionFailed {
		t.Fatalf("occupied put status=%d", code)
	}
	if code, body := report(occ, 1, "exists", ""); code != http.StatusOK || !strings.Contains(body, `"state":"pending"`) {
		t.Fatalf("occupied report status=%d body=%s", code, body)
	}
	if st, _, outcome := state(occupiedID); st != "pending" || outcome != "r2_mismatch_deleted" {
		t.Fatalf("occupied state=%s outcome=%s", st, outcome)
	}
	occ2 := lease(10)[occupiedID]
	if code := put(occ2, occupied.body); code != http.StatusOK {
		t.Fatalf("occupied re-put status=%d", code)
	}
	if code, body := report(occ2, 2, "uploaded", ""); code != http.StatusOK || !strings.Contains(body, `"clip_restored":true`) {
		t.Fatalf("occupied re-report status=%d body=%s", code, body)
	}

	// Wrong bytes at the key of a clip that is NOT purged are never touched.
	live := newClip("live", false)
	liveID := request(live, "original", live.key)
	bucket.set(live.key, []byte("xxxxxxxxxxxxx"))
	lv := lease(10)[liveID]
	if code, body := report(lv, 1, "exists", ""); code != http.StatusOK || !strings.Contains(body, `"state":"failed"`) {
		t.Fatalf("live report status=%d body=%s", code, body)
	}
	if b, _ := bucket.get(live.key); string(b) != "xxxxxxxxxxxxx" {
		t.Fatal("object of an unpurged clip was modified")
	}

	// A missing local copy is permanent.
	if code, body := report(tasks[missingID], 1, "local_missing", "no such file"); code != http.StatusOK || !strings.Contains(body, `"state":"failed"`) {
		t.Fatalf("missing report status=%d body=%s", code, body)
	}
	if code, _ := report(tasks[missingID], 1, "local_missing", ""); code != http.StatusBadRequest {
		t.Fatalf("failure without error status=%d", code)
	}

	// An expired lease is re-queued; a late upload from the old lease makes the
	// next attempt report 'exists', which verifies the same bytes.
	late := newClip("late", true)
	lateID := request(late, "original", late.key)
	first := lease(10)[lateID]
	if code := put(first, late.body); code != http.StatusOK {
		t.Fatalf("late put status=%d", code)
	}
	if _, err := pool.Exec(ctx, `UPDATE nas_restore_requests SET lease_expires_at=now()-interval '1 minute' WHERE id=$1`, lateID); err != nil {
		t.Fatal(err)
	}
	second := lease(10)[lateID]
	if second.Attempt != 2 {
		t.Fatalf("expired lease attempt=%d", second.Attempt)
	}
	if code := put(second, late.body); code != http.StatusPreconditionFailed {
		t.Fatalf("second put status=%d", code)
	}
	if code, body := report(second, 2, "exists", ""); code != http.StatusOK || !strings.Contains(body, `"clip_restored":true`) {
		t.Fatalf("exists report status=%d body=%s", code, body)
	}

	// An upload the storage never received is retried until max_attempts.
	ghost := newClip("ghost", true)
	ghostID := request(ghost, "original", ghost.key)
	if _, err := pool.Exec(ctx, `UPDATE nas_restore_requests SET max_attempts=1 WHERE id=$1`, ghostID); err != nil {
		t.Fatal(err)
	}
	g := lease(10)[ghostID]
	if code, body := report(g, 1, "uploaded", ""); code != http.StatusOK || !strings.Contains(body, `"state":"failed"`) {
		t.Fatalf("ghost report status=%d body=%s", code, body)
	}
	if st, _, outcome := state(ghostID); st != "failed" || outcome != "verify_failed" {
		t.Fatalf("ghost state=%s outcome=%s", st, outcome)
	}

	// Once the test capability expired, the sweep deletes again and marks it.
	bucket.set(testKey, []byte("late replay"))
	if _, err := pool.Exec(ctx, `UPDATE nas_restore_requests SET put_expires_at=now()-interval '2 hours' WHERE id=$1`, testID); err != nil {
		t.Fatal(err)
	}
	_ = lease(1)
	var deletedMarked bool
	if err := pool.QueryRow(ctx, `SELECT object_deleted_at IS NOT NULL FROM nas_restore_requests WHERE id=$1`, testID).Scan(&deletedMarked); err != nil || !deletedMarked {
		t.Fatalf("test object not marked deleted=%v err=%v", deletedMarked, err)
	}
	if _, ok := bucket.get(testKey); ok {
		t.Fatal("sweep did not delete the replayed test object")
	}
	if _, ok := bucket.get(original.key); !ok {
		t.Fatal("original restore object missing")
	}
}
