package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestNASCleanupProofAndConfinementPolicy(t *testing.T) {
	i := nasCleanupItem{SourceID: 1, RecordingID: 2, StreamID: 3, RelativePath: "a/b.mp4", SizeBytes: 3, SHA256: strings.Repeat("a", 64), Frozen: [5]int64{3, 1, 2, 4, 5}, FrozenSidecar: [5]int64{100, 1, 2, 6, 5}, SidecarRequired: true, StartAt: time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC), EndAt: time.Date(2026, 9, 1, 0, 1, 0, 0, time.UTC)}
	p := map[string]any{"root": []int64{5, 10}, "ancestors": [][2]int64{{5, 11}}, "identity": i.Frozen, "sha256": i.SHA256, "parent_members_sha256": strings.Repeat("b", 64), "sidecar": map[string]any{"identity": i.FrozenSidecar, "sha256": strings.Repeat("c", 64), "binding": map[string]any{"clip_id": 1, "recording_id": 2, "relative_path": "a/b.mp4", "size_bytes": 3, "sha256": i.SHA256, "clip_start_at": "2026-09-01T00:00:00Z", "clip_end_at": "2026-09-01T00:01:00Z"}}}
	raw, _ := json.Marshal(p)
	if !validCleanupProof(raw, i) {
		t.Fatal("valid physical proof rejected")
	}
	for _, k := range []string{"root", "ancestors", "identity", "sha256", "parent_members_sha256", "sidecar"} {
		t.Run(k, func(t *testing.T) {
			var bad map[string]any
			json.Unmarshal(raw, &bad)
			delete(bad, k)
			b, _ := json.Marshal(bad)
			if validCleanupProof(b, i) {
				t.Fatal("partial proof accepted")
			}
		})
	}
	for _, p := range []string{"../a", "/a", "a//b", "a/./b", "#recycle/x", "a\x00b", "a\\b"} {
		if validCleanupPath(p) {
			t.Fatalf("unsafe path %q", p)
		}
	}
	p["parent_members_sha256"] = strings.Repeat("d", 64)
	next, _ := json.Marshal(p)
	if !cleanupSameFileProof(raw, next) {
		t.Fatal("safe sibling deletion invalidates exact file")
	}
	p["identity"] = []int64{3, 1, 2, 999, 5}
	next, _ = json.Marshal(p)
	if cleanupSameFileProof(raw, next) {
		t.Fatal("different file identity accepted")
	}
}
func cleanupTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("STOARAMA_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("cleanup_%d", time.Now().UnixNano())
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	cfg, _ := pgxpool.ParseConfig(dsn)
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		admin.Close()
	})
	_, err = pool.Exec(ctx, `CREATE TABLE accounts(id bigint PRIMARY KEY);CREATE TABLE connections(id bigint PRIMARY KEY,account_id bigint,kind text,api_key_id bigint);
 CREATE TABLE streams(id bigint PRIMARY KEY,local_timezone text);CREATE TABLE recordings(id bigint PRIMARY KEY,account_id bigint,stream_id bigint,cron_timezone text);
 CREATE TABLE recording_clips(id bigint PRIMARY KEY,recording_id bigint,purged_at timestamptz,object_key text,sha256 text,size_bytes bigint,display_path text,clip_start_at timestamptz,clip_end_at timestamptz,bucket text,etag text NOT NULL DEFAULT '');
 CREATE TABLE recording_collation_hours(id bigint PRIMARY KEY,recording_id bigint,excluded_clip_count integer,manifest jsonb);
 CREATE TABLE recording_collation_outputs(id bigint PRIMARY KEY,collation_hour_id bigint,nas_relative_path text,object_key text,sha256 text,size_bytes bigint,start_at timestamptz,end_at timestamptz,source_clip_ids bigint[],created_at timestamptz DEFAULT now());
 CREATE TABLE recording_joined_source_snapshots(clip_id bigint);
 INSERT INTO accounts VALUES(47),(48);INSERT INTO connections VALUES(13,47,'nas_pull',5),(14,48,'nas_pull',6);INSERT INTO streams VALUES(3,'UTC');INSERT INTO recordings VALUES(2,47,3,'UTC');`)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"0167_nas_cleanup_dryruns.sql", "0168_nas_cleanup_stage_timings.sql", "0169_nas_cleanup_queue_priority.sql", "0170_nas_cleanup_retired_plans.sql", "0171_nas_cleanup_safeguards.sql", "0172_nas_cleanup_safeguards_validate.sql", "0173_nas_cleanup_throughput.sql"} {
		migration, err := os.ReadFile("../../../infra/sql/migrations/" + name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = pool.Exec(ctx, string(migration)); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	return pool
}
func cleanupCall(t *testing.T, h http.HandlerFunc, account, key int64, idParam, id string, body any) *httptest.ResponseRecorder {
	t.Helper()
	b, _ := json.Marshal(body)
	req := httptest.NewRequest("POST", "/cleanup", bytes.NewReader(b))
	route := chi.NewRouteContext()
	route.URLParams.Add(idParam, id)
	req = req.WithContext(context.WithValue(context.WithValue(req.Context(), chi.RouteCtxKey, route), accountPrincipalContextKey, accountPrincipal{AccountID: account, APIKeyID: &key}))
	w := httptest.NewRecorder()
	h(w, req)
	return w
}
func TestNASCleanupLeaseRetriesAndMasterOff(t *testing.T) {
	pool := cleanupTestPool(t)
	ctx := context.Background()
	s := &Server{pool: pool}
	_, err := pool.Exec(ctx, `INSERT INTO nas_cleanup_plans(id,account_id,connection_id,label,report_sha256,manifest,plan_sha256) VALUES(1,47,13,'fixture','060aa7070c7909d78013415f83b76bd0c027a885caf91c67c8b709ac1f4b5811','[{"kind":"raw","source_id":1}]',repeat('b',64));INSERT INTO nas_cleanup_items(id,plan_id,ordinal) VALUES(1,1,1);`)
	if err != nil {
		t.Fatal(err)
	}
	lease := func(account, key int64) cleanupTask {
		w := cleanupCall(t, s.handleAccountNASCleanupLease, account, key, "", "", map[string]any{"client_version": "test", "protocol": 1})
		if w.Code != 200 {
			t.Fatal(w.Body.String())
		}
		var r struct {
			Tasks []cleanupTask `json:"tasks"`
		}
		json.Unmarshal(w.Body.Bytes(), &r)
		if account == 48 {
			if len(r.Tasks) != 0 {
				t.Fatal("foreign plan leaked")
			}
			return cleanupTask{}
		}
		if len(r.Tasks) != 1 {
			t.Fatalf("no task %s", w.Body.String())
		}
		return r.Tasks[0]
	}
	lease(48, 6)
	first := lease(47, 5)
	if first.Attempt != 1 {
		t.Fatal("bad first attempt")
	}
	body := cleanupObserve{Attempt: 1, PlanSHA: strings.Repeat("b", 64), Outcome: "missing", Error: "missing", ClientVersion: "test"}
	if w := cleanupCall(t, s.handleAccountNASCleanupObserve, 48, 6, "itemId", "1", body); w.Code != 409 {
		t.Fatal("foreign observation accepted")
	}
	if _, err = pool.Exec(ctx, `UPDATE nas_cleanup_items SET lease_expires_at=now()-interval '1 minute' WHERE id=1`); err != nil {
		t.Fatal(err)
	}
	if w := cleanupCall(t, s.handleAccountNASCleanupObserve, 47, 5, "itemId", "1", body); w.Code != 409 {
		t.Fatal("expired observation accepted")
	}
	next := lease(47, 5)
	if next.Attempt != 2 {
		t.Fatal("lease attempt not fenced")
	}
	body.Attempt = 2
	for n := 0; n < 2; n++ {
		w := cleanupCall(t, s.handleAccountNASCleanupObserve, 47, 5, "itemId", "1", body)
		if w.Code != 200 || !strings.Contains(w.Body.String(), `"delete":false`) {
			t.Fatalf("dry run or ACK retry failed: %s", w.Body.String())
		}
	}
	w := cleanupCall(t, s.handleAdminNASCleanupControl, 47, 5, "id", "1", map[string]any{"plan_sha256": strings.Repeat("b", 64), "mode": "enabled", "reason": "fixture"})
	if w.Code != 409 || !strings.Contains(w.Body.String(), "master switch") {
		t.Fatal("master OFF allowed arming")
	}
	s.cfg.NASCleanupDeletionEnabled = true
	if _, err = pool.Exec(ctx, `UPDATE nas_cleanup_items SET state='would_delete',proof='{}',recovery='unchecked' WHERE id=1`); err != nil {
		t.Fatal(err)
	}
	w = cleanupCall(t, s.handleAdminNASCleanupControl, 47, 5, "id", "1", map[string]any{"plan_sha256": strings.Repeat("b", 64), "mode": "enabled", "allow_nas_only": true, "reason": "fixture"})
	if w.Code != 400 || !strings.Contains(w.Body.String(), "NAS-only deletion was removed") {
		t.Fatalf("allow-nas-only was not rejected: %d %s", w.Code, w.Body.String())
	}
	w = cleanupCall(t, s.handleAdminNASCleanupControl, 47, 5, "id", "1", map[string]any{"plan_sha256": strings.Repeat("b", 64), "mode": "enabled", "reason": "fixture"})
	if w.Code != 409 {
		t.Fatal("enable accepted unchecked recovery")
	}
	s.cfg.NASCleanupDeletionEnabled = false
	if _, err = pool.Exec(ctx, `UPDATE nas_cleanup_plans SET manifest='[]' WHERE id=1`); err == nil {
		t.Fatal("immutable plan was editable")
	}
	w = cleanupCall(t, s.handleAccountNASCleanupResult, 47, 5, "itemId", "1", map[string]any{"attempt": 2, "plan_sha256": strings.Repeat("b", 64), "proof_sha256": strings.Repeat("a", 64), "outcome": "deleted", "error": ""})
	if w.Code != 409 {
		t.Fatal("unissued delete acknowledged")
	}
}
func TestNASCleanupRecoveryHoldFencesOldPurgeAndMetadata(t *testing.T) {
	pool := cleanupTestPool(t)
	ctx := context.Background()
	_, err := pool.Exec(ctx, `INSERT INTO recording_clips VALUES(1,2,NULL,'object',repeat('a',64),3,'a/b.mp4','2026-09-01','2026-09-01 00:01:00','bucket');INSERT INTO nas_cleanup_plans(id,account_id,connection_id,label,report_sha256,manifest,plan_sha256) VALUES(1,47,13,'fixture','060aa7070c7909d78013415f83b76bd0c027a885caf91c67c8b709ac1f4b5811','[{"kind":"raw","source_id":1,"stream_id":3}]',repeat('b',64));INSERT INTO nas_cleanup_items(id,plan_id,ordinal) VALUES(1,1,1);INSERT INTO nas_cleanup_source_holds VALUES(1,1);`)
	if err != nil {
		t.Fatal(err)
	}
	for _, sql := range []string{`UPDATE recording_clips SET purged_at=now() WHERE id=1`, `UPDATE recording_clips SET clip_start_at='2026-10-01' WHERE id=1`, `UPDATE streams SET local_timezone='America/New_York' WHERE id=3`, `UPDATE recordings SET cron_timezone='Europe/London' WHERE id=2`, `DELETE FROM recording_clips WHERE id=1`} {
		if _, err = pool.Exec(ctx, sql); err == nil {
			t.Fatalf("hold bypass: %s", sql)
		}
	}
	if _, err = pool.Exec(ctx, `UPDATE nas_cleanup_items SET state='unverified',delete_ever_issued=true,recovery='r2_sha_verified' WHERE id=1;UPDATE nas_cleanup_plans SET mode='paused' WHERE id=1`); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `UPDATE recording_clips SET purged_at=now() WHERE id=1`); err == nil {
		t.Fatal("completed NAS deletion lost R2 recovery hold")
	}
}
func TestNASCleanupReplaysBest14WinnerAndSourceFingerprint(t *testing.T) {
	pool := cleanupTestPool(t)
	ctx := context.Background()
	_, err := pool.Exec(ctx, `INSERT INTO recording_clips SELECT n,2,NULL,'object'||n,repeat('a',64),3,'a/'||n||'.mp4', '2026-09-01'::timestamptz+(n-1)*interval '1 day','2026-09-01'::timestamptz+(n-1)*interval '1 day'+interval '1 hour','bucket' FROM generate_series(1,20) n`)
	if err != nil {
		t.Fatal(err)
	}
	winner := func() string {
		t.Helper()
		var raw []byte
		if err := pool.QueryRow(ctx, cleanupCurrentRankingsSQL, 3).Scan(&raw); err != nil {
			t.Fatal(err)
		}
		return string(raw)
	}
	if got := winner(); got != "2026-09-01" {
		t.Fatalf("ties must resolve to the earliest best14 start: %s", got)
	}
	// Collation-quarantined clips do not score (select.py), but still extend the
	// candidate range; R2 availability is not required.
	if _, err = pool.Exec(ctx, `INSERT INTO recording_collation_hours VALUES(1,2,1,'{"clips":[{"clip":{"clip_id":1},"disposition":"quarantined"}]}');UPDATE recording_clips SET purged_at=now(),bucket='' WHERE id=2`); err != nil {
		t.Fatal(err)
	}
	if got := winner(); got != "2026-09-02" {
		t.Fatalf("quarantined clip still scored: %s", got)
	}
	// Invalid intervals are ignored by the selection (they can never be
	// cleanup targets themselves because outside() rejects them).
	if _, err = pool.Exec(ctx, `UPDATE recording_clips SET clip_end_at=clip_start_at WHERE id=19`); err != nil {
		t.Fatal(err)
	}
	if got := winner(); got != "2026-09-02" {
		t.Fatalf("invalid interval changed the winner: %s", got)
	}
	if _, err = pool.Exec(ctx, `UPDATE recording_clips SET bucket='bucket',purged_at=NULL WHERE id=2`); err != nil {
		t.Fatal(err)
	}
	i, err := cleanupCatalogItem(ctx, pool, "raw", 1, "bucket")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `UPDATE recording_clips SET clip_start_at='2026-09-01 00:00:01' WHERE id=1`); err != nil {
		t.Fatal(err)
	}
	next, err := cleanupCatalogItem(ctx, pool, "raw", 1, "bucket")
	if err != nil {
		t.Fatal(err)
	}
	if next.SourceManifestSHA == i.SourceManifestSHA {
		t.Fatal("changed registered bounds left source proof unchanged")
	}
}

// Start an old purge UPDATE before admission commits. Its statement snapshot
// predates the hold; after the clip lock is released the trigger must still see
// the committed recovery hold and prevent the existing R2 delete path.
func TestNASCleanupHoldClosesConcurrentOldPurgeAdmissionRace(t *testing.T) {
	pool := cleanupTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := pool.Exec(ctx, `INSERT INTO recording_clips VALUES(1,2,NULL,'object',repeat('a',64),3,'a/b.mp4','2026-09-01','2026-09-01 00:01:00','bucket')`)
	if err != nil {
		t.Fatal(err)
	}
	admission, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer admission.Rollback(ctx)
	if _, err = admission.Exec(ctx, `SELECT id FROM recording_clips WHERE id=1 FOR UPDATE`); err != nil {
		t.Fatal(err)
	}
	purge, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer purge.Release()
	finished := make(chan error, 1)
	go func() {
		_, e := purge.Exec(ctx, `UPDATE recording_clips SET purged_at=now() WHERE id=1`)
		finished <- e
	}()
	waiting := false
	for n := 0; n < 100; n++ {
		if err = pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_locks WHERE pid=$1 AND NOT granted)`, purge.Conn().PgConn().PID()).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !waiting {
		t.Fatal("old purge did not reach the expected row-lock fence")
	}
	if _, err = admission.Exec(ctx, `INSERT INTO nas_cleanup_plans(id,account_id,connection_id,label,report_sha256,manifest,plan_sha256) VALUES(1,47,13,'fixture','060aa7070c7909d78013415f83b76bd0c027a885caf91c67c8b709ac1f4b5811','[]',repeat('b',64));INSERT INTO nas_cleanup_source_holds VALUES(1,1)`); err != nil {
		t.Fatal(err)
	}
	if err = admission.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err = <-finished; err == nil || !strings.Contains(err.Error(), "retention protected") {
		t.Fatalf("old purge crossed recovery hold: %v", err)
	}
}

func TestNASCleanupNewReferencesAndPublicGenerationAreFenced(t *testing.T) {
	pool := cleanupTestPool(t)
	ctx := context.Background()
	_, err := pool.Exec(ctx, `INSERT INTO recording_clips VALUES(1,2,NULL,'object',repeat('a',64),3,'a/b.mp4','2026-09-01','2026-09-01 00:01:00','bucket');INSERT INTO nas_cleanup_plans(id,account_id,connection_id,label,report_sha256,manifest,plan_sha256,mode,approved_until,approval_reason) VALUES(1,47,13,'fixture','060aa7070c7909d78013415f83b76bd0c027a885caf91c67c8b709ac1f4b5811','[]',repeat('b',64),'enabled',now()+interval '1 hour','fixture');INSERT INTO nas_cleanup_source_holds VALUES(1,1);`)
	if err != nil {
		t.Fatal(err)
	}
	for _, sql := range []string{`INSERT INTO recording_joined_source_snapshots VALUES(1)`, `INSERT INTO recording_collation_hours VALUES(1,2,0,'{"clips":[{"clip":{"clip_id":1}}]}')`, `UPDATE recording_clips SET bucket='different' WHERE id=1`} {
		if _, err = pool.Exec(ctx, sql); err == nil {
			t.Fatalf("source/recovery namespace fence bypass: %s", sql)
		}
	}
	old := &globalDeliveryRegistry{Generation: "c4049f6dc0f940be8b7eb8b9bef6f42f"}
	s := &Server{pool: pool, publicPinnedDeliveryRegistry: old}
	got, err := s.loadGlobalDeliveryRegistryScope(ctx, true, "")
	if err != nil || got != old {
		t.Fatalf("existing public delivery was not preserved: %v", err)
	}
	if _, err = s.loadGlobalDeliveryRegistryScope(ctx, true, strings.Repeat("1", 32)); err == nil {
		t.Fatal("new public generation bypassed cleanup fence")
	}
	if _, err = pool.Exec(ctx, `INSERT INTO nas_cleanup_items(id,plan_id,ordinal,state,delete_ever_issued) VALUES(1,1,1,'unverified',true);UPDATE nas_cleanup_plans SET mode='paused',approved_until=NULL WHERE id=1`); err != nil {
		t.Fatal(err)
	}
	got, err = s.loadGlobalDeliveryRegistryScope(ctx, true, "")
	if err != nil || got != old {
		t.Fatal("in-flight grace fence disappeared on pause")
	}
}

func TestNASCleanupOperatorRoutesRequireServiceCredential(t *testing.T) {
	s := &Server{}
	s.cfg.ServiceToken = "fixture-service"
	for _, token := range []string{"", "wrong", "fixture-service"} {
		r := httptest.NewRequest("POST", "/api/v1/service/nas-cleanup/plans", strings.NewReader(`{"label":"","items":[]}`))
		r.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		s.router().ServeHTTP(w, r)
		want := http.StatusUnauthorized
		if token == "fixture-service" {
			want = http.StatusBadRequest
		}
		if w.Code != want {
			t.Fatalf("service cleanup route token class=%t: got%d want%d", token == "fixture-service", w.Code, want)
		}
	}
	if pullPathAllowed("POST", "/api/v1/service/nas-cleanup/plans") || pullPathAllowed("POST", "/api/v1/service/nas-cleanup/plans/1/control") {
		t.Fatal("pull key can control cleanup")
	}
}

func TestNASCleanupCollatedHoldAllowsNonIdentityBookkeeping(t *testing.T) {
	pool := cleanupTestPool(t)
	ctx := context.Background()
	_, err := pool.Exec(ctx, `INSERT INTO recording_clips VALUES(1,2,NULL,'object',repeat('a',64),3,'a/b.mp4','2026-09-01','2026-09-01 00:01:00','bucket');INSERT INTO recording_collation_hours VALUES(1,2,0,'{"clips":[{"clip":{"clip_id":1}}]}');INSERT INTO recording_collation_outputs(id,collation_hour_id,nas_relative_path,object_key,sha256,size_bytes,start_at,end_at,source_clip_ids) VALUES(9,1,'a/b.mp4','joined/object',repeat('a',64),3,'2026-09-01','2026-09-01 00:01:00',ARRAY[1::bigint]);INSERT INTO nas_cleanup_plans(id,account_id,connection_id,label,report_sha256,manifest,plan_sha256) VALUES(1,47,13,'fixture','060aa7070c7909d78013415f83b76bd0c027a885caf91c67c8b709ac1f4b5811','[{"kind":"collated","source_id":9}]',repeat('b',64));INSERT INTO nas_cleanup_source_holds VALUES(1,1);`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `UPDATE recording_collation_outputs SET created_at=created_at+interval '1 hour' WHERE id=9`); err != nil {
		t.Fatalf("nonidentity bookkeeping blocked: %v", err)
	}
	if _, err = pool.Exec(ctx, `UPDATE recording_collation_outputs SET size_bytes=size_bytes+1 WHERE id=9`); err == nil {
		t.Fatal("held output identity changed")
	}
}

func TestNASCleanupReferenceAdmissionSerializesWithArmingButNotPermanentRecovery(t *testing.T) {
	pool := cleanupTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := pool.Exec(ctx, `INSERT INTO recording_clips VALUES(1,2,NULL,'object',repeat('a',64),3,'a/b.mp4','2026-09-01','2026-09-01 00:01:00','bucket');INSERT INTO nas_cleanup_plans(id,account_id,connection_id,label,report_sha256,manifest,plan_sha256) VALUES(1,47,13,'fixture','060aa7070c7909d78013415f83b76bd0c027a885caf91c67c8b709ac1f4b5811','[{"kind":"raw","source_id":1,"stream_id":3}]',repeat('b',64));INSERT INTO nas_cleanup_source_holds VALUES(1,1);INSERT INTO nas_cleanup_items(id,plan_id,ordinal,state,proof,recovery,recovery_evidence) VALUES(1,1,1,'would_delete','{}','r2_sha_verified','{"etag":"fixture"}');`)
	if err != nil {
		t.Fatal(err)
	}
	reference, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer reference.Rollback(ctx)
	if _, err = reference.Exec(ctx, `INSERT INTO recording_joined_source_snapshots VALUES(1)`); err != nil {
		t.Fatalf("dry run blocked new stitching: %v", err)
	}
	s := &Server{pool: pool}
	s.cfg.NASCleanupDeletionEnabled = true
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		done <- cleanupCall(t, s.handleAdminNASCleanupControl, 47, 5, "id", "1", map[string]any{"plan_sha256": strings.Repeat("b", 64), "mode": "enabled", "reason": "disposable concurrency fixture"})
	}()
	waiting := false
	for n := 0; n < 100; n++ {
		if err = pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND NOT granted)`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !waiting {
		t.Fatal("arming did not wait for in-flight source-reference transaction")
	}
	if err = reference.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if w := <-done; w.Code != 200 {
		t.Fatalf("arming failed after source transaction committed: %s", w.Body.String())
	}
	var referenced bool
	if err = pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM recording_joined_source_snapshots WHERE clip_id=ANY($1))`, []int64{1}).Scan(&referenced); err != nil || !referenced {
		t.Fatal("grant-side current-source check cannot see admitted reference")
	}
	if _, err = pool.Exec(ctx, `INSERT INTO recording_joined_source_snapshots VALUES(1)`); err == nil {
		t.Fatal("new stitching crossed active execution fence")
	}
	if _, err = pool.Exec(ctx, `UPDATE nas_cleanup_plans SET mode='paused',approved_until=NULL WHERE id=1;UPDATE nas_cleanup_items SET state='unverified',delete_ever_issued=true,updated_at=now()-interval '3 minutes' WHERE id=1`); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO recording_joined_source_snapshots VALUES(1)`); err != nil {
		t.Fatalf("completed recovery hold permanently blocked future stitching: %v", err)
	}
	if _, err = pool.Exec(ctx, `UPDATE recording_clips SET purged_at=now() WHERE id=1`); err == nil {
		t.Fatal("allowing future stitching released permanent R2 recovery hold")
	}
}

func TestNASCleanupAdmissionPersistsExactItemsAndRecoveryHolds(t *testing.T) {
	pool := cleanupTestPool(t)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `INSERT INTO recording_clips VALUES(1,2,NULL,'object',repeat('a',64),3,'a/b.mp4','2026-09-01','2026-09-01 00:01:00','bucket')`); err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	id, sha, err := persistNASCleanupPlan(ctx, tx, "admission-fixture", []nasCleanupItem{{Kind: "raw", SourceID: 1, SourceIDs: []int64{1}, StreamID: 3, RecordingID: 2, RelativePath: "a/b.mp4", Bucket: "bucket"}})
	if err != nil {
		t.Fatalf("actual production persistence failed: %v", err)
	}
	if id <= 0 || len(sha) != 64 {
		t.Fatal("invalid immutable admission identity")
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	var items, holds int
	if err = pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM nas_cleanup_items WHERE plan_id=$1),(SELECT count(*) FROM nas_cleanup_source_holds WHERE plan_id=$1)`, id).Scan(&items, &holds); err != nil || items != 1 || holds != 1 {
		t.Fatalf("incomplete admission: items%d holds%d err%v", items, holds, err)
	}
}

func TestNASCleanupAdmissionHeartbeatConcurrentParentRow(t *testing.T) {
	pool := cleanupTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := pool.Exec(ctx, `INSERT INTO recording_clips VALUES(1,2,NULL,'object',repeat('a',64),3,'a/b.mp4','2026-09-01','2026-09-01 00:01:00','bucket')`); err != nil {
		t.Fatal(err)
	}
	tx, err := pool.BeginTx(ctx, nasCleanupAdmissionOptions)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	var exists bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM connections WHERE id=13)`).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	heartbeat, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer heartbeat.Rollback(ctx)
	if _, err = heartbeat.Exec(ctx, `SELECT id FROM connections WHERE id=13 FOR UPDATE;UPDATE connections SET kind=kind WHERE id=13`); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, _, e := persistNASCleanupPlan(ctx, tx, "heartbeat-fixture", []nasCleanupItem{{Kind: "raw", SourceID: 1, SourceIDs: []int64{1}}})
		done <- e
	}()
	waiting := false
	for n := 0; n < 100; n++ {
		if err = pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_locks WHERE pid=$1 AND NOT granted)`, tx.Conn().PgConn().PID()).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !waiting {
		t.Fatal("admission did not reach actual heartbeat FORUPDATE fence")
	}
	if err = heartbeat.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err = <-done; err != nil {
		t.Fatalf("actual heartbeat interrupted production admission: %v", err)
	}
}

// Prove a changed actual PG winner is visible on the enabled path after a
// cached dry run. No cached-result stub stands in for the live ranking inputs.
func TestNASCleanupDryRunCacheCannotAuthorizeEnabledPolicy(t *testing.T) {
	pool := cleanupTestPool(t)
	ctx := context.Background()
	s := &Server{}
	_, err := pool.Exec(ctx, `INSERT INTO recording_clips VALUES(1,2,NULL,'o',repeat('a',64),3,'a/1.mp4','2026-09-01','2026-09-01 01:00','bucket')`)
	if err != nil {
		t.Fatal(err)
	}
	old, at, err := s.cleanupRankings(ctx, pool, 3, true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `UPDATE recording_clips SET clip_start_at='2026-10-01',clip_end_at='2026-10-01 01:00' WHERE id=1`); err != nil {
		t.Fatal(err)
	}
	cached, cachedAt, err := s.cleanupRankings(ctx, pool, 3, true)
	if err != nil || !bytes.Equal(cached, old) || !cachedAt.Equal(at) {
		t.Fatal("dry-run snapshot was not explicitly bounded")
	}
	s.nasCleanupRankingCache[3] = nasCleanupRankingsSnapshot{Raw: old, At: time.Now().Add(-4 * time.Minute)}
	fourMinute, _, err := s.cleanupRankings(ctx, pool, 3, true)
	if err != nil || !bytes.Equal(fourMinute, old) {
		t.Fatal("bounded five-minute advisory snapshot unavailable")
	}
	fresh, _, err := s.cleanupRankings(ctx, pool, 3, false)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(fresh, old) {
		t.Fatal("enabled policy reused advisory dry-run rankings")
	}
	s.nasCleanupRankingCache[3] = nasCleanupRankingsSnapshot{Raw: old, At: time.Now().Add(-6 * time.Minute)}
	refreshed, _, err := s.cleanupRankings(ctx, pool, 3, true)
	if err != nil || !bytes.Equal(refreshed, fresh) {
		t.Fatal("expired preflight snapshot did not refresh")
	}
}
