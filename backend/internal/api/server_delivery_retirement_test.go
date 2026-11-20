package api

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// deliveryExemptionSchemaStub is the minimum schema 0225 needs: every table its
// triggers and functions read, with only the columns they touch. It is a fixture,
// not a product schema: the migration under test is applied verbatim on top.
const deliveryExemptionSchemaStub = `
CREATE TABLE delivery_keep_holds(bucket text NOT NULL,object_key text NOT NULL,etag text NOT NULL,size_bytes bigint NOT NULL,
 recording_id bigint,kind text NOT NULL,sha256 text NOT NULL DEFAULT '',relative_path text NOT NULL DEFAULT '',
 imported_at timestamptz NOT NULL DEFAULT clock_timestamp(),PRIMARY KEY(bucket,object_key));
CREATE TABLE recording_collation_outputs(id bigint PRIMARY KEY,object_key text NOT NULL,size_bytes bigint NOT NULL,sha256 text NOT NULL);
CREATE TABLE recording_clips(id bigint PRIMARY KEY,bucket text,object_key text,recording_id bigint,recording_job_id bigint,purged_at timestamptz);
CREATE TABLE nas_cleanup_incident_roster(bucket text,object_key text);
CREATE TABLE nas_cleanup_incident_sources(clip_id bigint);
CREATE TABLE nas_cleanup_reserved_files(kind text,source_id bigint);
CREATE TABLE nas_cleanup_plans(id bigint PRIMARY KEY,mode text NOT NULL,approved_until timestamptz);
CREATE TABLE nas_cleanup_items(plan_id bigint,delete_ever_issued boolean,state text,updated_at timestamptz);
CREATE TABLE nas_cleanup_source_holds(plan_id bigint,clip_id bigint);
CREATE TABLE nas_r2_copy_items(state text,blocked_reason text,item jsonb);
CREATE TABLE nas_restore_requests(clip_id bigint,state text);
CREATE TABLE r2_object_retirements(kind text,source_id bigint,stream_id bigint,bucket text,object_key text,size_bytes bigint,
 sha256 text,reason text,choice text,manifest_sha256 text,policy_sha256 text);
CREATE TABLE r2_inverse_intents(bucket text,object_key text);
CREATE TABLE r2_inverse_events(bucket text,object_key text,event text);
CREATE TABLE r2_inverse_events_absent(bucket text,object_key text);
CREATE TABLE r2_keep28_intents(bucket text,object_key text);
CREATE TABLE r2_keep28_events(bucket text,object_key text,event text);
CREATE TABLE r2_nonraw_intents(bucket text,object_key text,kind text,source_id bigint,source_ids bigint[],size_bytes bigint,
 sha256 text,etag text,stream_id bigint,nas_relative_path text,last_modified timestamptz,verification_at timestamptz,
 manifest_sha256 text,policy_sha256 text,picks_sha256 text,drain_evidence_sha256 text,endpoint_sha256 text,
 retirement_class text,choice text);
CREATE TABLE r2_nonraw_events(bucket text,object_key text,event text);
CREATE TABLE r2_nonraw_attempts(bucket text,object_key text);
CREATE TABLE r2_retirement_reservations(kind text,source_id bigint,bucket text,object_key text,size_bytes bigint,
 sha256 text,manifest_sha256 text,retirement_class text);
CREATE TABLE nas_stitch_r2_input_holds(bucket text,object_key text,released_at timestamptz);
CREATE TABLE nas_stitch_r2_stage_objects(id bigint PRIMARY KEY,bucket text,object_key text);
CREATE TABLE nas_stitch_r2_stage_retirements(object_id bigint,absence_observed_at timestamptz);
CREATE TABLE recording_joined_source_snapshots(clip_id bigint);
CREATE TABLE recording_joined_snapshot_scopes(recording_id bigint,recording_job_id bigint,high_water_clip_id bigint,batch_record_id bigint);
CREATE TABLE recording_joined_batches(id bigint PRIMARY KEY,state text);
CREATE TABLE recording_joined_dry_run_scopes(recording_id bigint,recording_job_id bigint,high_water_clip_id bigint);
ALTER TABLE r2_nonraw_intents ADD CONSTRAINT r2_nonraw_intents_kind_check CHECK (true);
ALTER TABLE r2_nonraw_intents ADD CONSTRAINT r2_nonraw_intents_source_ids_check CHECK (true);
ALTER TABLE r2_retirement_reservations ADD CONSTRAINT r2_retirement_reservations_kind_check CHECK (true);
ALTER TABLE r2_object_retirements ADD CONSTRAINT r2_object_retirements_kind_check CHECK (true);
ALTER TABLE r2_object_retirements ADD CONSTRAINT r2_object_retirements_collated_prefix_check CHECK (true);
CREATE FUNCTION r2_inverse_delivery_held(p_clip bigint,p_bucket text,p_key text,p_path text,p_sha text,p_size bigint)
 RETURNS boolean LANGUAGE sql STABLE AS $$ SELECT false $$;
CREATE FUNCTION r2_owner_keep_held(p_bucket text,p_key text) RETURNS boolean LANGUAGE sql STABLE AS $$ SELECT false $$;
CREATE FUNCTION guard_r2_owner_keep_retirement() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
CREATE FUNCTION guard_r2_retirement_stitch_input() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
`

// deliveryExemptionTestPool is a disposable schema with 0225 applied verbatim.
func deliveryExemptionTestPool(t *testing.T) *pgxpool.Pool {
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
	schema := fmt.Sprintf("delivery_exempt_%d", time.Now().UnixNano())
	if _, err = admin.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	cfg, err := pgxpool.ParseConfig(dsn)
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
	t.Cleanup(func() {
		pool.Close()
		_, _ = admin.Exec(context.Background(), `DROP SCHEMA `+schema+` CASCADE`)
		admin.Close()
	})
	if _, err = pool.Exec(ctx, deliveryExemptionSchemaStub); err != nil {
		t.Fatalf("stub schema: %v", err)
	}
	body, err := os.ReadFile("../../../infra/sql/migrations/0225_r2_delivery_exemptions.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, string(body)); err != nil {
		t.Fatalf("apply 0225: %v", err)
	}
	return pool
}

// The repair object the fixture exempts, and the content hash it binds.
var (
	exemptOutputSHA = strings.Repeat("b", 64)
	exemptRepairObj = "experimental/stitching-confident-repairs/20261009/30/" + exemptOutputSHA + "/30_hour_01_parts_01_02_v799.mp4"
)

// TestDeliveryPageRetirementIsTheServingState is the P1 integration test: with both
// public registry caches warm, the committed retirement state must make page,
// metadata, ticket and file routes refuse, and only then must
// r2_delivery_page_retired() return true and open the confident_repair gate.
func TestDeliveryPageRetirementIsTheServingState(t *testing.T) {
	pool := deliveryExemptionTestPool(t)
	ctx := context.Background()
	reg := publicDeliveryRegistryFixture()
	store := &globalDeliveryTestStore{body: []byte("hello")}
	s := &Server{pool: pool, globalDeliveryStore: store,
		publicGlobalDeliveryRegistry: reg, publicGlobalDeliveryRegistryAt: time.Now(), publicPinnedDeliveryRegistry: reg}
	router := publicDeliveryRouter(s)

	// A confident_repair exemption for one exact page key, approved and seeded the
	// way the CLI does it: approval first, then the exemption row.
	if _, err := pool.Exec(ctx, `INSERT INTO delivery_keep_holds(bucket,object_key,etag,size_bytes,recording_id,kind,sha256,relative_path)
 VALUES('stoarama',$1,'repair-etag',700,30,'video_assembly',$2,'30_nonraw/June/Monday/30_hour_01_part_01.mp4')`, exemptRepairObj, exemptOutputSHA); err != nil {
		t.Fatal(err)
	}
	decision := `deniz 2026-11-19 confident_repair`
	var approvalID int64
	if err := pool.QueryRow(ctx, `INSERT INTO r2_delivery_exemption_approvals(class,roster_sha256,decision)
 VALUES('confident_repair',repeat('a',64),$1) RETURNING id`, decision).Scan(&approvalID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO r2_delivery_exemptions(bucket,object_key,etag,size_bytes,class,approval_id,decision,roster_sha256)
 VALUES('stoarama',$1,'repair-etag',700,'confident_repair',$2,$3,repeat('a',64))`, exemptRepairObj, approvalID, decision); err != nil {
		t.Fatal(err)
	}
	var retired, exempt bool
	if err := pool.QueryRow(ctx, `SELECT r2_delivery_page_retired(),r2_delivery_exempt('stoarama',$1)`, exemptRepairObj).Scan(&retired, &exempt); err != nil {
		t.Fatal(err)
	}
	if retired || exempt {
		t.Fatalf("before retirement: retired=%v exempt=%v", retired, exempt)
	}

	// Prewarm BOTH caches while the page is still serving.
	if _, err := s.loadGlobalDeliveryRegistryScope(ctx, true, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := s.loadGlobalDeliveryRegistryScope(ctx, true, reg.Generation); err != nil {
		t.Fatal(err)
	}
	file := reg.Files["1"]
	requests := []struct{ name, method, target string }{
		{"page", "GET", "/global-street-scores-delivery"},
		{"file_list_page", "GET", "/global-street-scores-delivery/files"},
		{"metadata", "GET", publicGlobalDeliveryAPI + "/metadata?path=/public/catalog.json"},
		{"metadata_pinned", "GET", publicGlobalDeliveryAPI + "/metadata?path=/public/catalog.json&generation=" + reg.Generation},
		{"ticket", "GET", publicGlobalDeliveryAPI + "/ticket/1?sha256=" + file.SHA256},
		{"ticket_pinned", "GET", publicGlobalDeliveryAPI + "/ticket/1?sha256=" + file.SHA256 + "&generation=" + reg.Generation},
		{"file", "GET", publicGlobalDeliveryAPI + "/file/1?sha256=" + file.SHA256},
		{"file_pinned", "GET", publicGlobalDeliveryAPI + "/file/1?sha256=" + file.SHA256 + "&generation=" + reg.Generation},
	}
	for _, r := range requests {
		w := httptest.NewRecorder()
		router.ServeHTTP(w, httptest.NewRequest(r.method, r.target, nil))
		if w.Code >= 400 {
			t.Fatalf("serving page: %s refused with %d", r.name, w.Code)
		}
	}

	// Take the page down: the one committed serving state, then the live function
	// and the confident_repair gate must agree in the same breath.
	if _, err := pool.Exec(ctx, `INSERT INTO r2_delivery_page_retirement(generation,decision)
 VALUES(repeat('c',32),'test: the old page is down')`); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT r2_delivery_page_retired(),r2_delivery_exempt('stoarama',$1)`, exemptRepairObj).Scan(&retired, &exempt); err != nil {
		t.Fatal(err)
	}
	if !retired {
		t.Fatal("the committed retirement state did not move r2_delivery_page_retired()")
	}
	if !exempt {
		t.Fatal("retirement did not open the confident_repair gate")
	}

	// Every public delivery route now refuses, current and pinned alike.
	before, presignedBefore := store.opened, store.presigned
	for _, r := range requests {
		w := httptest.NewRecorder()
		router.ServeHTTP(w, httptest.NewRequest(r.method, r.target, nil))
		if w.Code != http.StatusGone {
			t.Fatalf("retired page: %s served %d", r.name, w.Code)
		}
	}
	if store.opened != before || store.presigned != presignedBefore {
		t.Fatalf("retired page reached storage (opened %d, presigned %d)", store.opened-before, store.presigned-presignedBefore)
	}
	// And republication is refused while retired.
	if _, err := pool.Exec(ctx, `INSERT INTO r2_delivery_page_retirement(generation,decision) VALUES(repeat('d',32),'again')`); err == nil {
		t.Fatal("a second retirement state landed")
	}
}
