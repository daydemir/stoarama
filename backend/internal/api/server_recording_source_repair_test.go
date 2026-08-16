package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestSourceURLHashTrimsOnlyOuterWhitespace(t *testing.T) {
	if sourceURLHash("  https://example.com/live.m3u8?a=1  ") != sourceURLHash("https://example.com/live.m3u8?a=1") {
		t.Fatal("outer whitespace changed hash")
	}
	if sourceURLHash("https://example.com/live.m3u8?a=1") == sourceURLHash("https://example.com/live.m3u8?a=2") {
		t.Fatal("distinct signed sources collided")
	}
}

func sourceRepairRequest(id int64, body any) *http.Request {
	b, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/api/v1/admin/recordings/%d/repair-source", id), bytes.NewReader(b))
	rc := chi.NewRouteContext()
	rc.URLParams.Add("id", fmt.Sprint(id))
	return req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rc))
}

func TestRecordingSourceRepairDBFencesAndIdempotency(t *testing.T) {
	databaseURL := strings.TrimSpace(os.Getenv("STOARAMA_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL to run source-repair DB regression")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema := fmt.Sprintf("source_repair_%d", time.Now().UnixNano())
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	defer admin.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE")
	cfg, _ := pgxpool.ParseConfig(databaseURL)
	if cfg.ConnConfig.RuntimeParams == nil {
		cfg.ConnConfig.RuntimeParams = map[string]string{}
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err = migrateAPITestSchema(ctx, pool, filepath.Join("..", "..", "..", "infra", "sql", "migrations")); err != nil {
		t.Fatal(err)
	}
	var accountID, destID, streamID, recordingID, jobID int64
	if err = pool.QueryRow(ctx, `INSERT INTO accounts(email,name,status,role) VALUES('repair@example.test','repair','active','admin') RETURNING id`).Scan(&accountID); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, `INSERT INTO storage_destinations(account_id,name,provider,endpoint,region,bucket,status,managed) VALUES($1,'managed','r2_managed','https://example.com','auto','test','active',true) RETURNING id`, accountID).Scan(&destID); err != nil {
		t.Fatal(err)
	}
	oldURL := "https://example.com/old.m3u8"
	newURL := "https://example.com/new.m3u8"
	if err = pool.QueryRow(ctx, `INSERT INTO streams(provider,external_id,name,slug,source_url,capture_type,source_family,execution_class,capture_family,recording_state) VALUES('test','repair','repair','repair',$1,'hls','video_manifest','video_live','continuous_video','on') RETURNING id`, oldURL).Scan(&streamID); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, `INSERT INTO recordings(account_id,storage_destination_id,name,stream_url,stream_id,source_kind,mode,status,start_at,capture_via) VALUES($1,$2,'repair',$3,$4,'hls_live','continuous','active',now()-interval '1 hour','cloud') RETURNING id`, accountID, destID, oldURL, streamID).Scan(&recordingID); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, `INSERT INTO recording_jobs(recording_id,fire_at,scheduled_for,clip_duration_sec,status,idempotency_key,kind,window_end_at) VALUES($1,now()-interval '1 hour',now()-interval '1 hour',60,'pending','repair-job','continuous_window',now()+interval '1 hour') RETURNING id`, recordingID).Scan(&jobID); err != nil {
		t.Fatal(err)
	}
	oldProbe := probeRecordingSourceRepair
	probeRecordingSourceRepair = func(context.Context, string, net.IP, string) error { return nil }
	defer func() { probeRecordingSourceRepair = oldProbe }()
	body := map[string]any{"account_id": accountID, "stream_id": streamID, "job_id": jobID, "expected_current_source_sha256": sourceURLHash(oldURL), "replacement_source_url": newURL, "reason": "replace dead official camera"}
	s := &Server{pool: pool}
	rr := httptest.NewRecorder()
	s.handleAdminRecordingSourceRepair(rr, sourceRepairRequest(recordingID, body))
	if rr.Code != http.StatusOK {
		t.Fatalf("repair status=%d body=%s", rr.Code, rr.Body.String())
	}
	var gotRec, gotStream, status string
	var attempts int
	if err = pool.QueryRow(ctx, `SELECT r.stream_url,s.source_url,j.status,j.attempt_count FROM recordings r JOIN streams s ON s.id=r.stream_id JOIN recording_jobs j ON j.id=$2 WHERE r.id=$1`, recordingID, jobID).Scan(&gotRec, &gotStream, &status, &attempts); err != nil {
		t.Fatal(err)
	}
	if gotRec != newURL || gotStream != newURL || status != "pending" || attempts != 0 {
		t.Fatalf("rec=%q stream=%q status=%q attempts=%d", gotRec, gotStream, status, attempts)
	}
	// Exact retry is idempotent and does not require weakening the old-source hash fence.
	rr = httptest.NewRecorder()
	s.handleAdminRecordingSourceRepair(rr, sourceRepairRequest(recordingID, body))
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"idempotent":true`) {
		t.Fatalf("idempotent status=%d body=%s", rr.Code, rr.Body.String())
	}
	// A changed job state fails closed and cannot rewrite either source.
	if _, err = pool.Exec(ctx, `UPDATE recording_jobs SET status='leased',lease_owner='worker',lease_expires_at=now()+interval '5 minutes' WHERE id=$1`, jobID); err != nil {
		t.Fatal(err)
	}
	body["replacement_source_url"] = "https://example.com/third.m3u8"
	body["expected_current_source_sha256"] = sourceURLHash(newURL)
	rr = httptest.NewRecorder()
	s.handleAdminRecordingSourceRepair(rr, sourceRepairRequest(recordingID, body))
	if rr.Code != http.StatusConflict {
		t.Fatalf("leased status=%d body=%s", rr.Code, rr.Body.String())
	}
}
