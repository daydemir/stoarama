package api

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/daydemir/stoarama/backend/internal/r2"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

func spaceAuditValidRequest() nasSpaceAuditRequest {
	return nasSpaceAuditRequest{AuditID: "a3bf0b5d-91dc-44d2-a66f-b4ef5dca83d0", State: "scanning", ClientVersion: "f29174eb", Summary: json.RawMessage(`{"files":3}`)}
}
func TestNASSpaceAuditValidation(t *testing.T) {
	valid := spaceAuditValidRequest()
	if err := validateNASSpaceAuditRequest(valid); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*nasSpaceAuditRequest){
		"uuid path": func(r *nasSpaceAuditRequest) { r.AuditID = "../../outside" }, "uuid suffix": func(r *nasSpaceAuditRequest) { r.AuditID += "/other" }, "state": func(r *nasSpaceAuditRequest) { r.State = "delete" }, "summary null": func(r *nasSpaceAuditRequest) { r.Summary = []byte("null") }, "summary array": func(r *nasSpaceAuditRequest) { r.Summary = []byte("[]") }, "summary large": func(r *nasSpaceAuditRequest) {
			r.Summary = []byte(`{"x":"` + strings.Repeat("a", nasSpaceAuditSummaryLimit) + `"}`)
		}, "version": func(r *nasSpaceAuditRequest) { r.ClientVersion = "a/b" }, "oversize": func(r *nasSpaceAuditRequest) { r.ArtifactSizeBytes = nasSpaceAuditArtifactLimit + 1 }, "negative": func(r *nasSpaceAuditRequest) { r.ArtifactSizeBytes = -1 }, "upload missing declaration": func(r *nasSpaceAuditRequest) { r.State = "uploading" }, "sha without size": func(r *nasSpaceAuditRequest) { r.ArtifactSHA256 = strings.Repeat("a", 64) }, "scan artifact": func(r *nasSpaceAuditRequest) { r.ArtifactSizeBytes = 1; r.ArtifactSHA256 = strings.Repeat("a", 64) },
	} {
		t.Run(name, func(t *testing.T) {
			r := valid
			mutate(&r)
			if validateNASSpaceAuditRequest(r) == nil {
				t.Fatal("accepted invalid request")
			}
		})
	}
}
func TestNASSpaceAuditFrozenAndCompletedTransitions(t *testing.T) {
	req := spaceAuditValidRequest()
	req.State = "uploading"
	req.ArtifactSizeBytes = 123
	req.ArtifactSHA256 = strings.Repeat("a", 64)
	if validateNASSpaceAuditTransition(nasSpaceAuditStored{}, req) != nil {
		t.Fatal("first upload refused")
	}
	frozen := nasSpaceAuditStored{State: "uploading", SizeBytes: 123, SHA256: req.ArtifactSHA256}
	if validateNASSpaceAuditTransition(frozen, req) != nil {
		t.Fatal("upload retry refused")
	}
	changed := req
	changed.ArtifactSHA256 = strings.Repeat("b", 64)
	if validateNASSpaceAuditTransition(frozen, changed) == nil {
		t.Fatal("frozen hash replaced")
	}
	changed = req
	changed.ArtifactSizeBytes++
	if validateNASSpaceAuditTransition(frozen, changed) == nil {
		t.Fatal("frozen size replaced")
	}
	if validateNASSpaceAuditTransition(frozen, spaceAuditValidRequest()) == nil {
		t.Fatal("frozen scan restarted")
	}
	failed := spaceAuditValidRequest()
	failed.State = "failed"
	if validateNASSpaceAuditTransition(frozen, failed) != nil {
		t.Fatal("failure summary refused")
	}
	req.State = "complete"
	if validateNASSpaceAuditTransition(nasSpaceAuditStored{}, req) == nil {
		t.Fatal("completion before upload accepted")
	}
	done := frozen
	done.State = "complete"
	done.ClientVersion = req.ClientVersion
	done.Summary = []byte(`{ "files":3 }`)
	if validateNASSpaceAuditTransition(done, req) != nil {
		t.Fatal("matching completed replay refused")
	}
	changed = req
	changed.Summary = []byte(`{"files":4}`)
	if validateNASSpaceAuditTransition(done, changed) == nil {
		t.Fatal("completed summary overwritten")
	}
	if validateNASSpaceAuditTransition(done, failed) == nil {
		t.Fatal("completed audit downgraded")
	}
	if validateNASSpaceAuditHead(r2.ObjectHead{SizeBytes: 123, SHA256Metadata: req.ArtifactSHA256}, frozen) != nil {
		t.Fatal("matching HEAD refused")
	}
	if validateNASSpaceAuditHead(r2.ObjectHead{SizeBytes: 123}, frozen) == nil {
		t.Fatal("HEAD without SHA metadata accepted")
	}
	if validateNASSpaceAuditHead(r2.ObjectHead{SizeBytes: 124, SHA256Metadata: req.ArtifactSHA256}, frozen) == nil {
		t.Fatal("wrong HEAD size accepted")
	}
}
func TestNASSpaceAuditRejectsOversizedBodyBeforeDatabase(t *testing.T) {
	req := httptest.NewRequest("POST", "/", strings.NewReader(`{"summary":{"x":"`+strings.Repeat("a", nasSpaceAuditSummaryLimit+4096)+`"}}`))
	rec := httptest.NewRecorder()
	(&Server{}).handleAccountConnectionSpaceAudit(rec, req)
	if rec.Code != 400 {
		t.Fatalf("status=%d", rec.Code)
	}
	if !pullPathAllowed("POST", "/api/v1/account/connections/space-audit") || pullPathAllowed("DELETE", "/api/v1/account/connections/space-audit") || pullPathAllowed("POST", "/api/v1/account/connections/space-audit/extra") {
		t.Fatal("pull capability confinement changed")
	}
}
func TestNASSpaceAuditLifecycle(t *testing.T) {
	pool, cleanup := testAccountClipsPool(t)
	defer cleanup()
	ctx := context.Background()
	migration, err := os.ReadFile("../../../infra/sql/migrations/0163_nas_space_audits.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, string(migration)); err != nil {
		t.Fatal(err)
	}
	var cid int64
	if err = pool.QueryRow(ctx, `INSERT INTO connections(account_id,kind,api_key_id)VALUES(47,'nas_pull',123)RETURNING id`).Scan(&cid); err != nil {
		t.Fatal(err)
	}
	sha := strings.Repeat("a", 64)
	headSize := "123"
	headSHA := sha
	heads := 0
	bucket := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		heads++
		if r.Method != "HEAD" {
			t.Errorf("unexpected object payload request %s", r.Method)
		}
		w.Header().Set("Content-Length", headSize)
		w.Header().Set("X-Amz-Meta-Sha256", headSHA)
	}))
	defer bucket.Close()
	storage, err := r2.New(ctx, r2.Config{AccessKey: "key", SecretKey: "secret", Region: "auto", Bucket: "bucket", Endpoint: bucket.URL})
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{pool: pool, r2: storage}
	call := func(key int64, req nasSpaceAuditRequest) *httptest.ResponseRecorder {
		b, _ := json.Marshal(req)
		r := httptest.NewRequest("POST", "/", strings.NewReader(string(b)))
		r = r.WithContext(context.WithValue(r.Context(), accountPrincipalContextKey, accountPrincipal{AccountID: 47, APIKeyID: ptrInt64(key)}))
		w := httptest.NewRecorder()
		s.handleAccountConnectionSpaceAudit(w, r)
		return w
	}
	req := spaceAuditValidRequest()
	if w := call(124, req); w.Code != 403 {
		t.Fatalf("unbound key status=%d", w.Code)
	}
	if w := call(123, req); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	if heads != 0 {
		t.Fatal("scanning read R2")
	}
	req.State = "uploading"
	req.ArtifactSizeBytes = 123
	req.ArtifactSHA256 = sha
	w := call(123, req)
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	var resp struct {
		Upload struct {
			URL     string            `json:"url"`
			Headers map[string]string `json:"headers"`
		} `json:"upload"`
	}
	if json.Unmarshal(w.Body.Bytes(), &resp) != nil {
		t.Fatal("invalid upload response")
	}
	if !strings.Contains(resp.Upload.URL, fmt.Sprintf("nas-space-audits/%d/%s/%s.jsonl.gz", cid, req.AuditID, sha)) || resp.Upload.Headers["If-None-Match"] != "*" {
		t.Fatal("wrong immutable upload capability")
	}
	if heads != 0 {
		t.Fatal("upload intent read R2")
	}
	changed := req
	changed.ArtifactSizeBytes++
	if w = call(123, changed); w.Code != 409 {
		t.Fatal("changed upload declaration accepted")
	}
	req.State = "complete"
	headSHA = ""
	if w = call(123, req); w.Code != 409 {
		t.Fatal("missing metadata accepted")
	}
	headSHA = sha
	headSize = "124"
	if w = call(123, req); w.Code != 409 {
		t.Fatal("wrong size accepted")
	}
	headSize = "123"
	if w = call(123, req); w.Code != 200 || !strings.Contains(w.Body.String(), nasSpaceAuditHeadEvidence) {
		t.Fatalf("completion: %d %s", w.Code, w.Body.String())
	}
	before := heads
	if w = call(123, req); w.Code != 200 || heads != before {
		t.Fatal("completed replay not idempotent")
	}
	req.State = "failed"
	if w = call(123, req); w.Code != 409 {
		t.Fatal("completed audit downgraded")
	}
}
