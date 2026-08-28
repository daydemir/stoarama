package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/daydemir/stoarama/backend/internal/db"
)

func TestRecordingUploadIntentReconcileExpiresOnlyStaleGenerations(t *testing.T) {
	databaseURL := strings.TrimSpace(os.Getenv("STOARAMA_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL to run upload-intent reconciliation regression")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema := fmt.Sprintf("recording_intent_reconcile_%d", time.Now().UnixNano())
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	defer admin.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE")
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ConnConfig.RuntimeParams == nil {
		cfg.ConnConfig.RuntimeParams = map[string]string{}
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err = db.MigrateUp(ctx, pool, filepath.Join("..", "..", "..", "infra", "sql", "migrations")); err != nil {
		t.Fatal(err)
	}

	var accountID, destinationID, recordingID int64
	if err = pool.QueryRow(ctx, `INSERT INTO accounts(email,name,status,role) VALUES('intent@example.test','intent','active','admin') RETURNING id`).Scan(&accountID); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, `INSERT INTO storage_destinations(account_id,name,provider,endpoint,region,bucket,access_key_id,secret_access_key_enc,status,managed) VALUES($1,'managed','r2_managed','https://example.com','auto','test','test',decode('00','hex'),'verified',true) RETURNING id`, accountID).Scan(&destinationID); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, `INSERT INTO recordings(account_id,storage_destination_id,name,stream_url,source_kind,cron_expr,cron_timezone,clip_duration_sec,mode,daily_window_start,daily_window_end,active_weekdays,status,start_at,capture_via) VALUES($1,$2,'intent','https://example.com/live.m3u8','hls_live','0 8 * * *','UTC',60,'continuous','08:00','20:00',127,'active',now()-interval '1 hour','relay') RETURNING id`, accountID, destinationID).Scan(&recordingID); err != nil {
		t.Fatal(err)
	}

	currentToken := uuid.New()
	var terminalJob, pendingJob, liveJob int64
	if err = pool.QueryRow(ctx, `INSERT INTO recording_jobs(recording_id,fire_at,scheduled_for,clip_duration_sec,status,idempotency_key,kind,window_end_at,completed_at) VALUES($1,now()-interval '1 day',now()-interval '1 day',60,'done','intent-terminal','continuous_window',now()-interval '12 hours',now()-interval '12 hours') RETURNING id`, recordingID).Scan(&terminalJob); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, `INSERT INTO recording_jobs(recording_id,fire_at,scheduled_for,clip_duration_sec,status,idempotency_key,kind,window_end_at,lease_owner,lease_token,lease_expires_at) VALUES($1,now()-interval '1 hour',now()-interval '1 hour',60,'leased','intent-live','continuous_window',now()+interval '1 hour','node:7',$2,now()+interval '3 minutes') RETURNING id`, recordingID, currentToken).Scan(&liveJob); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, `INSERT INTO recording_jobs(recording_id,fire_at,scheduled_for,clip_duration_sec,status,idempotency_key,kind,window_end_at) VALUES($1,now()-interval '1 hour',now()-interval '1 hour',60,'pending','intent-pending','continuous_window',now()+interval '1 hour') RETURNING id`, recordingID).Scan(&pendingJob); err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC().Truncate(time.Second)
	expired := now.Add(-10 * time.Minute)
	future := now.Add(10 * time.Minute)
	terminalIntent := uuid.New()
	unleasedIntent := uuid.New()
	currentGenerationIntent := uuid.New()
	futureIntent := uuid.New()
	consumedIntent := uuid.New()
	insertIntent := func(id uuid.UUID, jobID int64, status string, expiresAt time.Time) {
		t.Helper()
		if _, insertErr := pool.Exec(ctx, `INSERT INTO recording_upload_intents(id,recording_id,recording_job_id,storage_destination_id,endpoint,bucket,object_key,display_path,mime_type,max_size_bytes,status,expires_at) VALUES($1,$2,$3,$4,'https://example.com','test',$5,$5,'video/mp4',1000,$6,$7)`, id, recordingID, jobID, destinationID, id.String()+".mp4", status, expiresAt); insertErr != nil {
			t.Fatal(insertErr)
		}
	}
	insertIntent(terminalIntent, terminalJob, "pending", expired)
	insertIntent(unleasedIntent, pendingJob, "pending", expired)
	insertIntent(currentGenerationIntent, liveJob, "pending", expired)
	insertIntent(futureIntent, terminalJob, "pending", future)
	insertIntent(consumedIntent, terminalJob, "consumed", expired)

	request := recordingUploadIntentReconcileRequest{
		AccountID:     accountID,
		RecordingIDs:  []int64{recordingID},
		ExpiresBefore: now.Add(-5 * time.Minute),
		Reason:        "clear expired generation-fenced recording intents",
	}
	call := func(body recordingUploadIntentReconcileRequest) *httptest.ResponseRecorder {
		t.Helper()
		raw, _ := json.Marshal(body)
		rec := httptest.NewRecorder()
		(&Server{pool: pool}).handleAdminRecordingUploadIntentReconcile(rec, httptest.NewRequest(http.MethodPost, "/api/v1/recordings/upload-intents/reconcile-expired", bytes.NewReader(raw)))
		return rec
	}

	dry := call(request)
	if dry.Code != http.StatusOK {
		t.Fatalf("dry run status=%d body=%s", dry.Code, dry.Body.String())
	}
	var plan recordingUploadIntentReconcileResponse
	if err = json.Unmarshal(dry.Body.Bytes(), &plan); err != nil {
		t.Fatal(err)
	}
	if plan.Apply || plan.CandidateCount != 2 || len(plan.Candidates) != 2 || len(plan.PlanSHA256) != 64 {
		t.Fatalf("dry plan=%+v", plan)
	}
	if plan.Candidates[0].IntentID != terminalIntent.String() && plan.Candidates[1].IntentID != terminalIntent.String() {
		t.Fatalf("terminal intent missing from plan: %+v", plan.Candidates)
	}
	if plan.Candidates[0].IntentID != unleasedIntent.String() && plan.Candidates[1].IntentID != unleasedIntent.String() {
		t.Fatalf("unleased intent missing from plan: %+v", plan.Candidates)
	}

	request.Apply = true
	request.ExpectedPlanSHA256 = strings.Repeat("0", 64)
	conflict := call(request)
	if conflict.Code != http.StatusConflict {
		t.Fatalf("wrong plan status=%d body=%s", conflict.Code, conflict.Body.String())
	}
	request.ExpectedPlanSHA256 = plan.PlanSHA256
	applied := call(request)
	if applied.Code != http.StatusOK {
		t.Fatalf("apply status=%d body=%s", applied.Code, applied.Body.String())
	}
	var result recordingUploadIntentReconcileResponse
	if err = json.Unmarshal(applied.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if !result.Apply || result.ExpiredCount != 2 {
		t.Fatalf("apply result=%+v", result)
	}
	for id, want := range map[uuid.UUID]string{
		terminalIntent:          "expired",
		unleasedIntent:          "expired",
		currentGenerationIntent: "pending",
		futureIntent:            "pending",
		consumedIntent:          "consumed",
	} {
		var got string
		if err = pool.QueryRow(ctx, `SELECT status FROM recording_upload_intents WHERE id=$1`, id).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("intent=%s status=%q want=%q", id, got, want)
		}
	}
}

func TestRecordingUploadIntentReconcileRequiresAdminAuth(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/api/v1/recording/upload-intents/reconcile-expired", bytes.NewBufferString(`{}`))
	rec := httptest.NewRecorder()
	s := &Server{}
	s.requireAdminAuth(http.HandlerFunc(s.handleAdminRecordingUploadIntentReconcile)).ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d body=%s, want unauthorized", rec.Code, rec.Body.String())
	}
}
