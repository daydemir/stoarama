package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/r2"
)

func TestNASUploadProbePartSizesSumExactly(t *testing.T) {
	for _, tc := range []struct {
		size    int64
		streams int
	}{{256 << 20, 4}, {1000, 3}, {7, 1}, {17, 16}} {
		parts := nasUploadProbePartSizes(tc.size, tc.streams)
		var total int64
		for _, p := range parts {
			if p <= 0 && tc.size >= int64(tc.streams) {
				t.Fatalf("size=%d streams=%d produced empty part %v", tc.size, tc.streams, parts)
			}
			total += p
		}
		if len(parts) != tc.streams || total != tc.size {
			t.Fatalf("size=%d streams=%d parts=%v", tc.size, tc.streams, parts)
		}
	}
}

func TestNASUploadProbeDue(t *testing.T) {
	now := time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC)
	at := func(d time.Duration) *time.Time { v := now.Add(d); return &v }
	interval := 6 * time.Hour
	if due, _ := nasUploadProbeDue(now, nil, nil, interval); !due {
		t.Fatal("first probe not due")
	}
	if due, wait := nasUploadProbeDue(now, at(-time.Hour), nil, interval); due || wait != 5*time.Hour {
		t.Fatalf("recent probe due=%v wait=%v", due, wait)
	}
	if due, _ := nasUploadProbeDue(now, at(-6*time.Hour), nil, interval); !due {
		t.Fatal("probe at exact interval not due")
	}
	if due, wait := nasUploadProbeDue(now, at(-7*time.Hour), at(10*time.Minute), interval); due || wait != 10*time.Minute {
		t.Fatalf("pending unexpired probe due=%v wait=%v", due, wait)
	}
	if due, _ := nasUploadProbeDue(now, at(-7*time.Hour), at(-time.Minute), interval); !due {
		t.Fatal("expired pending probe blocked a new one")
	}
}

func TestNASUploadProbeMbps(t *testing.T) {
	if got := nasUploadProbeMbps(125_000_000, 10_000); got != 100 {
		t.Fatalf("mbps=%v want 100", got)
	}
	if got := nasUploadProbeMbps(1, 0); got != 0 {
		t.Fatalf("zero duration mbps=%v", got)
	}
}

func TestValidateNASUploadProbeResult(t *testing.T) {
	now := time.Now()
	started := now.Add(-time.Minute)
	valid := nasUploadProbeResultRequest{BytesUploaded: 100, DurationMS: 1000, StartedAt: &started, ClientPhaseStart: "draining", ClientPhaseEnd: "idle"}
	if err := validateNASUploadProbeResult(valid, 100, now); err != nil {
		t.Fatalf("valid result rejected: %v", err)
	}
	future := now.Add(time.Hour)
	for name, mutate := range map[string]func(*nasUploadProbeResultRequest){
		"oversize":         func(r *nasUploadProbeResultRequest) { r.BytesUploaded = 101 },
		"negative":         func(r *nasUploadProbeResultRequest) { r.BytesUploaded = -1 },
		"zero duration":    func(r *nasUploadProbeResultRequest) { r.DurationMS = 0 },
		"no start":         func(r *nasUploadProbeResultRequest) { r.StartedAt = nil },
		"future start":     func(r *nasUploadProbeResultRequest) { r.StartedAt = &future },
		"partial no error": func(r *nasUploadProbeResultRequest) { r.BytesUploaded = 50 },
		"long error":       func(r *nasUploadProbeResultRequest) { r.Error = strings.Repeat("x", 501) },
		"bad phase":        func(r *nasUploadProbeResultRequest) { r.ClientPhaseEnd = "sleeping" },
		"bad version":      func(r *nasUploadProbeResultRequest) { r.ClientVersion = "a b" },
		"negative pulled":  func(r *nasUploadProbeResultRequest) { r.BytesPulledDuring = -1 },
	} {
		req := valid
		mutate(&req)
		if err := validateNASUploadProbeResult(req, 100, now); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
	partial := valid
	partial.BytesUploaded, partial.Error = 50, "timeout"
	if err := validateNASUploadProbeResult(partial, 100, now); err != nil {
		t.Fatalf("partial result with error rejected: %v", err)
	}
}

type fakeProbeBucket struct {
	mu      sync.Mutex
	deleted []string
}

var fakeDeleteKeyRe = regexp.MustCompile(`<Key>([^<]+)</Key>`)

func (f *fakeProbeBucket) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if _, ok := r.URL.Query()["delete"]; !ok || r.Method != http.MethodPost {
		http.Error(w, "unexpected request", http.StatusBadRequest)
		return
	}
	body, _ := io.ReadAll(r.Body)
	f.mu.Lock()
	for _, m := range fakeDeleteKeyRe.FindAllStringSubmatch(string(body), -1) {
		f.deleted = append(f.deleted, m[1])
	}
	f.mu.Unlock()
	w.Header().Set("Content-Type", "application/xml")
	_, _ = io.WriteString(w, `<?xml version="1.0" encoding="UTF-8"?><DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></DeleteResult>`)
}

func (f *fakeProbeBucket) take() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := f.deleted
	f.deleted = nil
	sort.Strings(out)
	return out
}

func TestNASUploadProbeLifecycle(t *testing.T) {
	pool, cleanup := testAccountClipsPool(t)
	defer cleanup()
	ctx := context.Background()
	migration, err := os.ReadFile("../../../infra/sql/migrations/0154_nas_upload_probes.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, string(migration)); err != nil {
		t.Fatalf("apply migration: %v", err)
	}
	const accountID, apiKeyID, otherKeyID = int64(47), int64(123), int64(124)
	var connectionID int64
	if err := pool.QueryRow(ctx, `INSERT INTO connections(account_id,kind,api_key_id) VALUES($1,'nas_pull',$2) RETURNING id`, accountID, apiKeyID).Scan(&connectionID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO connections(account_id,kind,api_key_id) VALUES($1,'nas_pull',$2)`, accountID, otherKeyID); err != nil {
		t.Fatal(err)
	}
	bucket := &fakeProbeBucket{}
	server := httptest.NewServer(bucket)
	defer server.Close()
	client, err := r2.New(ctx, r2.Config{AccessKey: "key", SecretKey: "secret", Region: "auto", Bucket: "bucket", Endpoint: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{pool: pool, r2: client, cfg: config.Config{
		NASUploadProbeEnabled: false, NASUploadProbeBytes: 1000, NASUploadProbeStreams: 3, NASUploadProbeInterval: 6 * time.Hour,
	}}
	router := chi.NewRouter()
	router.Post("/probe", s.handleAccountConnectionUploadProbe)
	router.Post("/probe/{probeId}/result", s.handleAccountConnectionUploadProbeResult)
	call := func(keyID int64, path, body string) (int, string) {
		req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
		req = req.WithContext(context.WithValue(req.Context(), accountPrincipalContextKey,
			accountPrincipal{AccountID: accountID, APIKeyID: ptrInt64(keyID)}))
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		return rec.Code, rec.Body.String()
	}
	probe := func() nasUploadProbeResponse {
		code, body := call(apiKeyID, "/probe", `{"client_version":"abc123"}`)
		if code != http.StatusOK {
			t.Fatalf("probe status=%d body=%s", code, body)
		}
		var resp nasUploadProbeResponse
		if err := json.Unmarshal([]byte(body), &resp); err != nil {
			t.Fatal(err)
		}
		return resp
	}

	if resp := probe(); resp.Enabled || resp.Due || resp.RetryAfterSec != nasUploadProbeDisabledSec {
		t.Fatalf("disabled probe response=%+v", resp)
	}
	s.cfg.NASUploadProbeEnabled = true
	first := probe()
	if !first.Enabled || !first.Due || first.ProbeID <= 0 || first.SizeBytes != 1000 || first.Streams != 3 || len(first.Parts) != 3 {
		t.Fatalf("first probe response=%+v", first)
	}
	var total int64
	for i, part := range first.Parts {
		total += part.SizeBytes
		wantKey := fmt.Sprintf("/bucket/nas-probes/%d/", connectionID)
		if part.Method != http.MethodPut || !strings.Contains(part.URL, wantKey) || !strings.Contains(part.URL, fmt.Sprintf("/part-%d?", i)) ||
			part.Headers["Content-Length"] != fmt.Sprint(part.SizeBytes) || part.Headers["Host"] != "" {
			t.Fatalf("part %d=%+v", i, part)
		}
	}
	if total != 1000 {
		t.Fatalf("parts total=%d", total)
	}
	if again := probe(); !again.Enabled || again.Due || again.RetryAfterSec < 1 || again.RetryAfterSec > int(nasUploadProbeTTL.Seconds())+1 {
		t.Fatalf("pending probe response=%+v", again)
	}

	result := fmt.Sprintf(`{"bytes_uploaded":1000,"duration_ms":2000,"started_at":%q,"error":"","client_version":"abc123",
		"client_phase_start":"draining","client_phase_end":"idle","joined_transfer_active":false,"bytes_pulled_during":5}`,
		time.Now().UTC().Format(time.RFC3339))
	resultPath := fmt.Sprintf("/probe/%d/result", first.ProbeID)
	if code, body := call(otherKeyID, resultPath, result); code != http.StatusNotFound {
		t.Fatalf("foreign connection report status=%d body=%s", code, body)
	}
	if code, body := call(apiKeyID, resultPath, result); code != http.StatusOK || !strings.Contains(body, `"mbps":0.004`) {
		t.Fatalf("report status=%d body=%s", code, body)
	}
	deleted := bucket.take()
	if len(deleted) != 3 || !strings.HasSuffix(deleted[0], "/part-0") || !strings.HasPrefix(deleted[0], fmt.Sprintf("nas-probes/%d/", connectionID)) {
		t.Fatalf("deleted keys after report=%v", deleted)
	}
	var objectsDeleted, reported bool
	var phaseStart string
	if err := pool.QueryRow(ctx, `SELECT objects_deleted_at IS NOT NULL, reported_at IS NOT NULL, client_phase_start FROM nas_upload_probes WHERE id=$1`, first.ProbeID).Scan(&objectsDeleted, &reported, &phaseStart); err != nil {
		t.Fatal(err)
	}
	if !objectsDeleted || !reported || phaseStart != "draining" {
		t.Fatalf("reported probe deleted=%v reported=%v phase=%q", objectsDeleted, reported, phaseStart)
	}
	if code, _ := call(apiKeyID, resultPath, result); code != http.StatusConflict {
		t.Fatalf("duplicate report status=%d", code)
	}
	if resp := probe(); resp.Due || resp.RetryAfterSec < int((6*time.Hour-time.Minute).Seconds()) {
		t.Fatalf("probe within interval response=%+v", resp)
	}

	// An unreported probe whose target expired is swept on the next request.
	if _, err := pool.Exec(ctx, `UPDATE nas_upload_probes SET created_at=created_at-interval '7 hours'`); err != nil {
		t.Fatal(err)
	}
	orphan := probe()
	if !orphan.Due || orphan.ProbeID == first.ProbeID {
		t.Fatalf("second probe response=%+v", orphan)
	}
	if _, err := pool.Exec(ctx, `UPDATE nas_upload_probes SET created_at=created_at-interval '7 hours', expires_at=now()-interval '2 hours' WHERE id=$1`, orphan.ProbeID); err != nil {
		t.Fatal(err)
	}
	third := probe()
	if !third.Due {
		t.Fatalf("third probe response=%+v", third)
	}
	deleted = bucket.take()
	if len(deleted) != 3 || !strings.Contains(deleted[0], fmt.Sprintf("-%d/part-0", orphan.ProbeID)) {
		t.Fatalf("orphan sweep deleted=%v", deleted)
	}
	if err := pool.QueryRow(ctx, `SELECT objects_deleted_at IS NOT NULL, reported_at IS NOT NULL FROM nas_upload_probes WHERE id=$1`, orphan.ProbeID).Scan(&objectsDeleted, &reported); err != nil {
		t.Fatal(err)
	}
	if !objectsDeleted || reported {
		t.Fatalf("orphan probe deleted=%v reported=%v", objectsDeleted, reported)
	}
	partial := fmt.Sprintf(`{"bytes_uploaded":400,"duration_ms":1000,"started_at":%q,"error":"timeout","client_version":"",
		"client_phase_start":"idle","client_phase_end":"idle","joined_transfer_active":true,"bytes_pulled_during":0}`,
		time.Now().UTC().Format(time.RFC3339))
	if code, body := call(apiKeyID, fmt.Sprintf("/probe/%d/result", third.ProbeID), partial); code != http.StatusOK {
		t.Fatalf("partial report status=%d body=%s", code, body)
	}
}
