package api

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Stream 3 is pinned to Asia/Tokyo with best_start 2026-09-19 in the embedded
// keep28 policy; this folder maps to stream 3 in the client's pinned map.
const cleanupFixtureFolder = "99991_asia_japan_tokyo_shinjuku9"

// fakeCleanupBucket is a minimal path-style S3 endpoint with content-derived
// ETags (MD5, like a single-part R2 upload), If-Match and optional latency.
type fakeCleanupBucket struct {
	mu        sync.Mutex
	objects   map[string][]byte
	gets      int
	heads     int
	headDelay time.Duration
	getDelay  time.Duration
	// etags overrides the reported ETag of a key (multipart-style or a lying
	// store) while the body stays as stored.
	etags map[string]string
}

func (b *fakeCleanupBucket) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/bucket/")
	b.mu.Lock()
	body, ok := b.objects[key]
	delay := b.headDelay
	if r.Method == http.MethodGet {
		b.gets++
		delay = b.getDelay
	} else {
		b.heads++
	}
	b.mu.Unlock()
	time.Sleep(delay)
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	sum := md5.Sum(body)
	etag := `"` + hex.EncodeToString(sum[:]) + `"`
	b.mu.Lock()
	if override, ok := b.etags[key]; ok {
		etag = `"` + override + `"`
	}
	b.mu.Unlock()
	if m := r.Header.Get("If-Match"); m != "" && m != etag {
		w.WriteHeader(http.StatusPreconditionFailed)
		return
	}
	w.Header().Set("ETag", etag)
	w.Header().Set("Content-Length", fmt.Sprint(len(body)))
	w.Header().Set("Last-Modified", time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC).Format(http.TimeFormat))
	if r.Method == http.MethodGet {
		_, _ = w.Write(body)
	}
}

func (b *fakeCleanupBucket) counts() (gets, heads int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.gets, b.heads
}

func (b *fakeCleanupBucket) put(key string, body []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.objects[key] = body
}

// cleanupFixtureFile is one raw NAS clip. Frozen identities are either real
// stat results (benchmark, written by bench_cleanup.py) or synthetic.
type cleanupFixtureFile struct {
	ClipID        int64     `json:"clip_id"`
	RelativePath  string    `json:"relative_path"`
	Path          string    `json:"path,omitempty"`
	SizeBytes     int64     `json:"size_bytes"`
	SHA256        string    `json:"sha256"`
	StartAt       time.Time `json:"start_at"`
	EndAt         time.Time `json:"end_at"`
	Frozen        [5]int64  `json:"frozen_identity"`
	FrozenSidecar [5]int64  `json:"frozen_sidecar_identity"`
	body          []byte
}

type cleanupFixture struct {
	t      *testing.T
	ctx    context.Context
	pool   *pgxpool.Pool
	s      *Server
	bucket *fakeCleanupBucket
	r2srv  *httptest.Server
	nodes  map[string]bool
}

// newCleanupFixture prepares stream 3 so the live best-14 replay yields its
// pinned best_start: seedPerDay one-hour (or shorter) clips on each of the 14
// best days, all in Asia/Tokyo.
func newCleanupFixture(t *testing.T, seedPerDay int) *cleanupFixture {
	t.Helper()
	pool := cleanupTestPool(t)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `UPDATE streams SET local_timezone='Asia/Tokyo' WHERE id=3;UPDATE recordings SET cron_timezone='Asia/Tokyo' WHERE id=2`); err != nil {
		t.Fatal(err)
	}
	step := 86400 / seedPerDay
	if _, err := pool.Exec(ctx, `INSERT INTO recording_clips(id,recording_id,object_key,sha256,size_bytes,display_path,clip_start_at,clip_end_at,bucket)
 SELECT 5000000+n,2,'seed/'||n,repeat('c',64),1,'seed/'||n||'.mp4',t,t+$2::integer*interval '1 second','bucket'
 FROM generate_series(0,14*$1::integer-1) n CROSS JOIN LATERAL (SELECT timestamptz '2026-09-19 00:00:00+09'+n*$2::integer*interval '1 second' t) x`, seedPerDay, step); err != nil {
		t.Fatal(err)
	}
	bucket := &fakeCleanupBucket{objects: map[string][]byte{}}
	srv := httptest.NewServer(bucket)
	t.Cleanup(srv.Close)
	client, err := r2.New(ctx, r2.Config{AccessKey: "key", SecretKey: "secret", Region: "auto", Bucket: "bucket", Endpoint: srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	f := &cleanupFixture{t: t, ctx: ctx, pool: pool, s: &Server{pool: pool, r2: client}, bucket: bucket, r2srv: srv, nodes: map[string]bool{}}
	var best string
	if err := pool.QueryRow(ctx, cleanupCurrentRankingsSQL, 3).Scan(&best); err != nil || best != "2026-09-19" {
		t.Fatalf("fixture best14 replay %q %v", best, err)
	}
	return f
}

// syntheticFiles returns n August clips (outside stream 3's keep window)
// spread over day folders of perDir files.
func syntheticFiles(firstClip int64, n, perDir int) []cleanupFixtureFile {
	files := make([]cleanupFixtureFile, n)
	base := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	for k := range files {
		id := firstClip + int64(k)
		body := []byte(fmt.Sprintf("clip %d media bytes", id))
		sum := sha256.Sum256(body)
		start := base.Add(time.Duration(id-firstClip) * 61 * time.Second)
		files[k] = cleanupFixtureFile{ClipID: id, RelativePath: fmt.Sprintf("%s/day-%03d/clip_%d.mp4", cleanupFixtureFolder, (id-firstClip)/int64(perDir), id),
			SizeBytes: int64(len(body)), SHA256: hex.EncodeToString(sum[:]), StartAt: start, EndAt: start.Add(time.Minute),
			Frozen: [5]int64{int64(len(body)), 1, 2, id, 77}, FrozenSidecar: [5]int64{300, 1, 2, id + 1<<40, 77}, body: body}
	}
	return files
}

// admit inserts catalog rows, R2 objects and one plan through the production
// persistence path (persistNASCleanupPlan) and returns its id, digest and
// item ids in manifest order.
func (f *cleanupFixture) admit(label string, files []cleanupFixtureFile) (int64, string, []int64) {
	t, ctx := f.t, f.ctx
	t.Helper()
	items := make([]nasCleanupItem, 0, len(files))
	for _, file := range files {
		body := file.body
		if body == nil {
			var err error
			if body, err = os.ReadFile(file.Path); err != nil {
				t.Fatal(err)
			}
		}
		key := fmt.Sprintf("bench/%d.mp4", file.ClipID)
		f.bucket.put(key, body)
		if _, err := f.pool.Exec(ctx, `INSERT INTO recording_clips(id,recording_id,object_key,sha256,size_bytes,display_path,clip_start_at,clip_end_at,bucket) VALUES($1,2,$2,$3,$4,$5,$6,$7,'bucket')`,
			file.ClipID, key, file.SHA256, file.SizeBytes, file.RelativePath, file.StartAt, file.EndAt); err != nil {
			t.Fatal(err)
		}
		item, err := cleanupCatalogItem(ctx, f.pool, "raw", file.ClipID, "bucket")
		if err != nil {
			t.Fatal(err)
		}
		item.Frozen, item.FrozenSidecar = file.Frozen, file.FrozenSidecar
		items = append(items, item)
		f.nodes[path.Dir(file.RelativePath)] = true
	}
	report := nasCleanupReport{}
	for dir := range f.nodes {
		report.Nodes = append(report.Nodes, struct {
			Path   string  `json:"path"`
			Action string  `json:"direct_files_action"`
			Owners []int64 `json:"owner_stream_ids"`
		}{Path: dir, Action: "delete", Owners: []int64{3}})
	}
	f.s.nasCleanupReportCache = &report
	tx, err := f.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	id, sha, err := persistNASCleanupPlan(ctx, tx, label, items)
	if err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	var ids []int64
	rows, err := f.pool.Query(ctx, `SELECT id FROM nas_cleanup_items WHERE plan_id=$1 ORDER BY ordinal`, id)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var item int64
		if err := rows.Scan(&item); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, item)
	}
	return id, sha, ids
}

// fixtureProof is what an honest NAS client reports for a synthetic file.
func fixtureProof(i nasCleanupItem) json.RawMessage {
	root := [2]int64{i.Frozen[4], 1}
	parts := strings.Split(i.RelativePath, "/")
	ancestors := make([][2]int64, len(parts)-1)
	for n := range ancestors {
		ancestors[n] = [2]int64{root[0], int64(n + 2)}
	}
	binding := map[string]any{"clip_id": i.SourceID, "recording_id": i.RecordingID, "relative_path": i.RelativePath, "size_bytes": i.SizeBytes, "sha256": i.SHA256,
		"clip_start_at": i.StartAt.UTC().Format(time.RFC3339Nano), "clip_end_at": i.EndAt.UTC().Format(time.RFC3339Nano)}
	b, _ := json.Marshal(map[string]any{"root": root, "ancestors": ancestors, "identity": i.Frozen, "sha256": i.SHA256, "parent_members_sha256": strings.Repeat("e", 64),
		"sidecar": map[string]any{"identity": i.FrozenSidecar, "sha256": strings.Repeat("f", 64), "binding": binding}})
	return b
}

func (f *cleanupFixture) leaseV2(limit, files int, bytes int64, requestID string) []cleanupTask {
	f.t.Helper()
	body := map[string]any{"client_version": "test", "protocol": 2, "limit": limit, "execution_files": files, "execution_bytes": bytes}
	if requestID != "" {
		body["request_id"] = requestID
	}
	w := cleanupCall(f.t, f.s.handleAccountNASCleanupLease, 47, 5, "", "", body)
	if w.Code != 200 {
		f.t.Fatalf("lease v2: %d %s", w.Code, w.Body.String())
	}
	var r struct {
		Tasks []cleanupTask `json:"tasks"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &r); err != nil {
		f.t.Fatal(err)
	}
	return r.Tasks
}

type batchResult struct {
	ItemID    int64     `json:"item_id"`
	Status    int       `json:"status"`
	Action    string    `json:"action"`
	Delete    bool      `json:"delete"`
	Recovery  string    `json:"recovery"`
	ProofSHA  string    `json:"proof_sha256"`
	ExpiresAt time.Time `json:"expires_at"`
	Paused    bool      `json:"plan_paused"`
	Replayed  bool      `json:"replayed"`
	Error     string    `json:"error"`
}

// observe sends match observations for tasks (with request ids when given).
func (f *cleanupFixture) observe(tasks []cleanupTask, requestIDs map[int64]string) []batchResult {
	return f.observeMD5(tasks, requestIDs, nil)
}

func (f *cleanupFixture) observeMD5(tasks []cleanupTask, requestIDs map[int64]string, md5s map[int64]string) []batchResult {
	f.t.Helper()
	observations := []map[string]any{}
	for _, task := range tasks {
		o := map[string]any{"item_id": task.ItemID, "attempt": task.Attempt, "plan_sha256": task.PlanSHA, "outcome": "match", "proof": fixtureProof(task.Item), "error": "", "client_version": "test"}
		if id := requestIDs[task.ItemID]; id != "" {
			o["request_id"] = id
		}
		if m := md5s[task.ItemID]; m != "" {
			o["md5"] = m
		}
		observations = append(observations, o)
	}
	w := cleanupCall(f.t, f.s.handleAccountNASCleanupObserveBatch, 47, 5, "", "", map[string]any{"client_version": "test", "observations": observations})
	if w.Code != 200 {
		f.t.Fatalf("observe batch: %d %s", w.Code, w.Body.String())
	}
	var r struct {
		Results []batchResult `json:"results"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &r); err != nil || len(r.Results) != len(tasks) {
		f.t.Fatalf("observe batch results %v %s", err, w.Body.String())
	}
	return r.Results
}

func (f *cleanupFixture) receipts(tasks []cleanupTask, results []batchResult, outcome string) []map[string]any {
	f.t.Helper()
	receipts := []map[string]any{}
	for n, task := range tasks {
		receipts = append(receipts, map[string]any{"item_id": task.ItemID, "attempt": task.Attempt, "plan_sha256": task.PlanSHA, "proof_sha256": results[n].ProofSHA, "outcome": outcome, "error": ""})
	}
	w := cleanupCall(f.t, f.s.handleAccountNASCleanupResultBatch, 47, 5, "", "", map[string]any{"client_version": "test", "receipts": receipts})
	if w.Code != 200 {
		f.t.Fatalf("result batch: %d %s", w.Code, w.Body.String())
	}
	var r struct {
		Results []map[string]any `json:"results"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &r); err != nil {
		f.t.Fatal(err)
	}
	return r.Results
}

func (f *cleanupFixture) enableBatch(ids []int64, shas []string, hours int) (int, string) {
	f.t.Helper()
	digest := cleanupBatchDigest(cleanupPolicySHA, ids, shas)
	w := cleanupCall(f.t, f.s.handleAdminNASCleanupBatchEnable, 47, 5, "", "", map[string]any{"plan_ids": ids, "batch_sha256": digest, "reason": "fixture batch approval", "approval_hours": hours, "allow_long_approval": hours > 4})
	return w.Code, w.Body.String()
}

func (f *cleanupFixture) planMode(id int64) (string, string) {
	f.t.Helper()
	var mode, reason string
	if err := f.pool.QueryRow(f.ctx, `SELECT mode,approval_reason FROM nas_cleanup_plans WHERE id=$1`, id).Scan(&mode, &reason); err != nil {
		f.t.Fatal(err)
	}
	return mode, reason
}

func (f *cleanupFixture) batchMode() (string, string) {
	f.t.Helper()
	var mode, reason string
	if err := f.pool.QueryRow(f.ctx, `SELECT mode,approval_reason FROM nas_cleanup_batches ORDER BY id DESC LIMIT 1`).Scan(&mode, &reason); err != nil {
		f.t.Fatal(err)
	}
	return mode, reason
}

// dryRun leases and observes every pending dry-run task (protocol 2).
func (f *cleanupFixture) dryRun() {
	f.t.Helper()
	for {
		tasks := f.leaseV2(64, 0, 0, "")
		if len(tasks) == 0 {
			return
		}
		for _, r := range f.observe(tasks, nil) {
			if r.Status != 200 || r.Action != "would_delete" || r.Recovery != "r2_sha_verified" || r.Delete {
				f.t.Fatalf("dry run %+v", r)
			}
		}
	}
}
