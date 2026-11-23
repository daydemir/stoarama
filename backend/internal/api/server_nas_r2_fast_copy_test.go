package api

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
)

type fastCopyLeaseReply struct {
	Tasks   []nasR2FastCopyTask `json:"tasks"`
	Expires time.Time           `json:"lease_expires_at"`
}

func useFastCopyFixture(t *testing.T, f *cleanupFixture) {
	t.Helper()
	migration, err := os.ReadFile("../../../infra/sql/migrations/0240_nas_r2_fast_copy_lease_cap.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.pool.Exec(f.ctx, string(migration)); err != nil {
		t.Fatal(err)
	}
	sum := md5.Sum([]byte("verified kept R2 footage"))
	if _, err = f.pool.Exec(f.ctx, `UPDATE recording_clips SET etag=$1 WHERE id>=901 AND id<906`, hex.EncodeToString(sum[:])); err != nil {
		t.Fatal(err)
	}
	f.useClientVersion(nasFastCopyClientVersion)
	f.s.cfg.NASR2CopyEnabled = true
}

func fastCopyLease(t *testing.T, f *cleanupFixture) fastCopyLeaseReply {
	t.Helper()
	out := copyCall(f, "lease", 0, nasR2CopyControl{FreeBytes: NASR2CopyFloor + 1e9})
	var reply fastCopyLeaseReply
	if err := json.Unmarshal(out.Body.Bytes(), &reply); err != nil || out.Code != 200 || reply.Tasks == nil {
		t.Fatalf("fast lease %d %s: %v", out.Code, out.Body, err)
	}
	return reply
}

func TestNASR2CopyV8FastBatchLeaseCapAndExactGET(t *testing.T) {
	f, item := copyTestFixture(t)
	queueCopySources(t, f, item)
	useFastCopyFixture(t, f)
	f.s.cfg.NASR2FastCopyBatchSize = 5
	// Competing batch requests rely on the transaction's live-slot recount.
	var wg sync.WaitGroup
	batches := make(chan *httptest.ResponseRecorder, 2)
	for range 2 {
		wg.Go(func() {
			batches <- copyCall(f, "lease", 0, nasR2CopyControl{FreeBytes: NASR2CopyFloor + 1e9})
		})
	}
	wg.Wait()
	var batch fastCopyLeaseReply
	for range 2 {
		out := <-batches
		var reply fastCopyLeaseReply
		if err := json.Unmarshal(out.Body.Bytes(), &reply); err != nil || out.Code != 200 {
			t.Fatalf("concurrent fast lease %d %s: %v", out.Code, out.Body, err)
		}
		if len(reply.Tasks) != 0 {
			if len(batch.Tasks) != 0 {
				t.Fatal("the same queue admitted two live batches")
			}
			batch = reply
		}
	}
	if len(batch.Tasks) != 5 || batch.Expires.Before(time.Now().Add(15*time.Minute)) {
		t.Fatalf("received %d tasks, lease expires %s", len(batch.Tasks), batch.Expires)
	}
	task := batch.Tasks[0]
	signed, err := url.Parse(task.URL)
	if err != nil || !strings.Contains(signed.Query().Get("X-Amz-SignedHeaders"), "if-match") ||
		signed.Query().Get("X-Amz-Expires") != "900" || task.Headers.Get("If-Match") != `"`+task.ETag+`"` || task.Headers.Get("Range") != "" {
		t.Fatalf("expected signed whole-object If-Match, got %v", err)
	}
	req, err := http.NewRequest(http.MethodGet, task.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header = task.Headers.Clone()
	checkGET := func(status int) {
		t.Helper()
		response, err := f.r2srv.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != status {
			t.Fatalf("GET status = %d, want %d", response.StatusCode, status)
		}
	}
	checkGET(http.StatusOK)
	f.bucket.put(item.ObjectKey, []byte(strings.Repeat("x", int(item.SizeBytes))))
	checkGET(http.StatusPreconditionFailed)
	if out := copyCall(f, "checkpoint", task.ID, nasR2CopyControl{Token: task.Token, FreeBytes: NASR2CopyFloor + 1e9}); out.Code != 409 {
		t.Fatal("legacy checkpoint accepted for fast release")
	}
}

func TestNASR2CopyV8FastResultRequiresFreshInventoryAndToken(t *testing.T) {
	f, item := copyTestFixture(t)
	useFastCopyFixture(t, f)
	task := fastCopyLease(t, f).Tasks[0]
	request := func(token string, want string) {
		t.Helper()
		req := map[string]any{"free_bytes": NASR2CopyFloor + 1_000_000_000, "results": []nasR2FastCopyResult{{
			ID: task.ID, Token: token, Completion: NASR2CopyDownloaded, DownloadedBytes: item.SizeBytes, CommittedBytes: item.SizeBytes,
		}}}
		out := copyCall(f, "result-batch", 0, req)
		if out.Code != 200 || !strings.Contains(out.Body.String(), want) {
			t.Fatalf("results status=%d %s", out.Code, out.Body)
		}
	}
	request(task.Token, `"verified":[]`)
	next := fastCopyLease(t, f).Tasks[0]
	if next.ID != task.ID || next.Token == task.Token {
		t.Fatal("proof refusal did not return the lease")
	}
	if _, err := f.pool.Exec(f.ctx, `INSERT INTO nas_inventory_files(connection_id,clip_id,relative_path,sha256,size_bytes,state,verified_at,server_received_at,sidecar_relative_path,sidecar_size_bytes,sidecar_sha256)
 VALUES(13,$1,$2,$3,$4,'present',now(),now(),$2||'.stoarama.json',10,repeat('f',64))`, item.SourceID, item.RelativePath, item.SHA256, item.SizeBytes); err != nil {
		t.Fatal(err)
	}
	request(task.Token, `"verified":[]`)
	request(next.Token, `"verified":[1]`)
	var committed, downloaded int64
	if err := f.pool.QueryRow(f.ctx, `SELECT downloaded_bytes,committed_bytes FROM nas_r2_copy_items WHERE id=$1 AND state='verified'`, task.ID).Scan(&downloaded, &committed); err != nil ||
		downloaded != item.SizeBytes || committed != item.SizeBytes {
		t.Fatalf("downloaded=%d committed=%d err=%v", downloaded, committed, err)
	}
}

func TestNASR2CopyV8FastLeaseUsesCatalogETag(t *testing.T) {
	f, item := copyTestFixture(t)
	useFastCopyFixture(t, f)
	f.bucket.put(item.ObjectKey, []byte(strings.Repeat("x", int(item.SizeBytes))))
	if reply := fastCopyLease(t, f); len(reply.Tasks) != 0 {
		t.Fatal("leased a changed object of the same size")
	}
	var reason string
	if err := f.pool.QueryRow(f.ctx, `SELECT blocked_reason FROM nas_r2_copy_items WHERE source_id=$1`, item.SourceID).Scan(&reason); err != nil || reason != string(NASR2CopyR2Identity) {
		t.Fatalf("blocked reason=%q, err=%v", reason, err)
	}
}

func TestNASR2CopyV8FastConfiguredCapAndLegacyAfterMigration(t *testing.T) {
	f, item := copyTestFixture(t)
	queueCopySources(t, f, item)
	useFastCopyFixture(t, f)
	f.s.cfg.NASR2FastCopyBatchSize = 2
	if first, second := fastCopyLease(t, f), fastCopyLease(t, f); len(first.Tasks) != 2 || len(second.Tasks) != 0 {
		t.Fatalf("configured cap returned %d + %d tasks", len(first.Tasks), len(second.Tasks))
	}
	if _, err := f.pool.Exec(f.ctx, `UPDATE nas_r2_copy_items SET lease_expires_at=now()-interval '1 second'`); err != nil {
		t.Fatal(err)
	}
	f.useClientVersion(nasKeep20CleanupClientVersion)
	task := copyLease(t, f)
	if task.Token == "" {
		t.Fatal("legacy single task missing")
	}
	out := copyCall(f, "lease", 0, nasR2CopyControl{FreeBytes: NASR2CopyFloor + 1e9})
	if out.Code != 200 || !strings.Contains(out.Body.String(), `"task":null`) {
		t.Fatalf("legacy cap changed: %d %s", out.Code, out.Body)
	}
	out = copyCall(f, "result-batch", 0, map[string]any{})
	if out.Code != 409 {
		t.Fatalf("legacy admitted to batch results: %d", out.Code)
	}
}

func TestNASR2CopyV8FastClientServerPipeline(t *testing.T) {
	f, item := copyTestFixture(t)
	queueCopySources(t, f, item)
	useFastCopyFixture(t, f)
	if _, err := f.pool.Exec(f.ctx, `ALTER TABLE connections
 ADD COLUMN inventory_scan_completed_at timestamptz,
 ADD COLUMN inventory_in_progress_generation text DEFAULT '',
 ADD COLUMN inventory_in_progress_dataset_schema text DEFAULT '',
 ADD COLUMN inventory_in_progress_root_exclusions jsonb DEFAULT '[]',
 ADD COLUMN inventory_live_revision bigint DEFAULT 0;
 ALTER TABLE nas_inventory_files
 ADD PRIMARY KEY(connection_id,clip_id),
 ADD COLUMN recording_id bigint, ADD COLUMN file_mtime_ns bigint,
 ADD COLUMN file_ctime_ns bigint, ADD COLUMN file_inode bigint, ADD COLUMN file_device bigint,
 ADD COLUMN seen_generation text, ADD COLUMN client_updated_at timestamptz,
 ADD COLUMN tree_parent_path text, ADD COLUMN tree_name text`); err != nil {
		t.Fatal(err)
	}
	router := chi.NewRouter()
	router.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			p := accountPrincipal{AccountID: 47, APIKeyID: ptrInt64(5), KeyScopes: []string{accountScopePull}}
			next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), accountPrincipalContextKey, p)))
		})
	}, nasDeliveryRequestDeadline, f.s.confineAccountScope, f.s.fenceNASDelivery)
	router.Post(copyControlLeasePath, f.s.handleNASR2CopyLease)
	router.Post("/api/v1/account/connections/inventory", f.s.handleAccountConnectionInventorySync)
	router.Post("/api/v1/account/connections/r2-copy/result-batch", f.s.handleNASR2FastCopyResults)
	api := httptest.NewServer(router)
	t.Cleanup(api.Close)

	python := os.Getenv("STOARAMA_TEST_PYTHON")
	if python == "" {
		python = "python3"
	}
	root := t.TempDir()
	ctx, cancel := context.WithTimeout(f.ctx, 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, python, "-B", "-c", `
import concurrent.futures, http.client, pathlib, sys, threading, urllib.request
from unittest import mock

import test_stoarama_pull as support

pull = support.pull
base, root, version = sys.argv[1:]
cfg = support.NASPullTests().config(pathlib.Path(root))
cfg.api_base = base + "/api/v1"
pull.CLIENT_VERSION = version
pull.USER_AGENT = "stoarama-nas-pull/" + version
free = pull.R2_COPY_FLOOR + 2_000_000_000

class LoopbackHTTP(urllib.request.HTTPSHandler):
    # Keep capability validation and the real opener, including If-Match.
    # The Go fixture serves the signed loopback destination over HTTP.
    def https_open(self, request):
        assert request.host.startswith("127.0.0.1:")
        return self.do_open(http.client.HTTPConnection, request)

build = urllib.request.build_opener
stop = threading.Event()
inventory = pull.Inventory(cfg)
try:
    with pull.R2CopyControl(cfg, stop, 0) as control, concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        batch = control.request_json(pull.R2CopyControlOperation.LEASE, "/account/connections/r2-copy/lease",
                                     body={"free_bytes": free, "verify_existing": True})
        assert len(batch["tasks"]) == 5
        for task in batch["tasks"]:
            assert task["url"].startswith("http://127.0.0.1:")
            task["url"] = task["url"].replace("http:", "https:", 1)
        with mock.patch.object(pull, "r2_copy_resources", return_value=free), \
                mock.patch.object(pull, "storage_status", return_value={"available": True, "free_bytes": free}), \
                mock.patch.object(urllib.request, "build_opener", side_effect=lambda *handlers: build(*handlers, LoopbackHTTP())):
            assert not pull.r2_fast_copy_batch(cfg, pull.Runtime(cfg), inventory, stop, control, pool, batch, False)
finally:
    inventory.close()
`, api.URL, root, nasFastCopyClientVersion)
	cmd.Dir = "../../../clients/nas-pull"
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("fast copy client pipeline: %v\n%s", err, out)
	}
	var verified, proofs int
	if err = f.pool.QueryRow(f.ctx, `SELECT count(*) FROM nas_r2_copy_items WHERE
 state='verified' AND completion='downloaded' AND committed_bytes=$1 AND downloaded_bytes=$1 AND range_id IS NULL`,
		item.SizeBytes).Scan(&verified); err != nil {
		t.Fatal(err)
	}
	if err = f.pool.QueryRow(f.ctx, `SELECT count(*) FROM nas_inventory_files WHERE state='present' AND sidecar_sha256 IS NOT NULL`).Scan(&proofs); err != nil {
		t.Fatal(err)
	}
	gets, _ := f.bucket.counts()
	if verified != 5 || proofs != 5 || gets != 5 ||
		!strings.Contains(string(out), `"result_ms":`) || !strings.Contains(string(out), `"get_first_byte_ms":`) {
		t.Fatalf("verified=%d proofs=%d GETs=%d: %s", verified, proofs, gets, out)
	}
}

func TestNASStorageBindingV8FastCopyMigration(t *testing.T) {
	s, pool := storageHeartbeatFixture(t)
	ctx := context.Background()
	for _, name := range []string{"0237_nas_storage_v8_cleanup_binding.sql", "0238_nas_storage_v8_keep20_cleanup_binding.sql", "0239_nas_storage_v8_fast_copy_binding.sql"} {
		body, err := os.ReadFile("../../../infra/sql/migrations/" + name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, string(body)); err != nil {
			t.Fatal(err)
		}
	}
	for _, version := range []string{"r2-copy-v5", NASR2CopyPagedClientVersion, nasCleanupCertifiedStitchVersion,
		nasLowMemoryCopyClientVersion, nasLowMemoryCleanupClientVersion, nasKeep20CleanupClientVersion, nasFastCopyClientVersion} {
		if _, err := pool.Exec(ctx, `UPDATE connections SET nas_storage_client_version=$1,nas_storage_budget_at=now() WHERE id=13`, version); err != nil {
			t.Fatalf("binding refused %q: %v", version, err)
		}
	}
	if _, err := pool.Exec(ctx, `UPDATE connections SET nas_storage_client_version=$1 WHERE id=13`, nasFastCopyClientVersion+"-extra"); err == nil {
		t.Fatal("binding accepted an unreviewed literal")
	}
	nonce := storageHeartbeat(t, s, nasFastCopyClientVersion, storageSample(""))
	issued := storageObservation(t, pool).issued
	storageHeartbeat(t, s, nasFastCopyClientVersion, storageSample(nonce))
	obs := storageObservation(t, pool)
	if issued == nil || obs.version != nasFastCopyClientVersion || obs.budget == nil || !obs.budget.Equal(*issued) {
		t.Fatalf("fast copy did not persist the exact storage binding: %+v", obs)
	}
}
