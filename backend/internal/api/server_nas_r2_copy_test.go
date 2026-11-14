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

	for _, name := range []string{"0174_r2_object_retirements.sql", "0182_r2_retirement_reservations.sql", "0192_nas_r2_copy.sql"} {
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
	req := httptest.NewRequest("POST", "/", strings.NewReader(string(data)))
	ctx := context.WithValue(req.Context(), accountPrincipalContextKey, accountPrincipal{AccountID: 47, APIKeyID: ptrInt64(5), KeyScopes: []string{accountScopePull}})
	route := chi.NewRouteContext()
	route.URLParams.Add("id", fmt.Sprint(id))
	ctx = context.WithValue(ctx, chi.RouteCtxKey, route)
	req = req.WithContext(ctx)
	out := httptest.NewRecorder()
	handlers := map[string]http.HandlerFunc{"lease": f.s.handleNASR2CopyLease, "checkpoint": f.s.handleNASR2CopyCheckpoint, "range": f.s.handleNASR2CopyRange, "result": f.s.handleNASR2CopyResult}
	handlers[action](out, req)
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
func TestNASR2CopyDefaultOffBudgetAndInventory(t *testing.T) {
	f, i := copyTestFixture(t)
	if out := copyCall(f, "lease", 0, nasR2CopyControl{FreeBytes: NASR2CopyFloor + 1e9}); out.Code != 409 {
		t.Fatal("default enabled")
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

	var charged int64
	if err := f.pool.QueryRow(f.ctx, `SELECT charged_bytes FROM nas_r2_copy_budgets WHERE day=(now() AT TIME ZONE 'UTC')::date`).Scan(&charged); err != nil || charged != i.SizeBytes {
		t.Fatalf("charge %d %v", charged, err)
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
func TestNASR2CopyCancellationStaleTokenAndFailedRangeCharge(t *testing.T) {
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

	var charged int64
	f.pool.QueryRow(f.ctx, `SELECT charged_bytes FROM nas_r2_copy_budgets`).Scan(&charged)
	if charged != i.SizeBytes {
		t.Fatal("failed GET uncharged")
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
func TestNASR2CopyBudgetAcrossMidnightAndExhaustion(t *testing.T) {
	f, _ := copyTestFixture(t)
	now := time.Date(2026, 11, 14, 23, 59, 55, 0, time.UTC)
	tx, err := BeginNASR2CopyTx(f.ctx, f.pool)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(f.ctx)
	if err = nasR2CopyCharge(f.ctx, tx, NASR2CopyDailyBytes, now); err != nil {
		t.Fatal(err)
	}
	var n int
	tx.QueryRow(f.ctx, `SELECT count(*) FROM nas_r2_copy_budgets WHERE charged_bytes=$1`, NASR2CopyDailyBytes).Scan(&n)
	if n != 2 {
		t.Fatal("midnight bytes not charged to both days")
	}
	if err = nasR2CopyCharge(f.ctx, tx, 1, now); err == nil {
		t.Fatal("daily cap exceeded")
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
	var charged int64
	f.pool.QueryRow(f.ctx, `SELECT charged_bytes FROM nas_r2_copy_budgets`).Scan(&charged)
	if charged != i.SizeBytes {
		t.Fatal("duplicate admission charged or issued")
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
	f.pool.QueryRow(f.ctx, `SELECT charged_bytes FROM nas_r2_copy_budgets`).Scan(&charged)
	if charged != 2*i.SizeBytes {
		t.Fatal("second GET not charged")
	}
}
