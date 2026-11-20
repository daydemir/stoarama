package api

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
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
CREATE VIEW nas_cleanup_r2_reserved_files AS SELECT kind,source_id FROM nas_cleanup_reserved_files;
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

	// A direct retirement insert is REFUSED while the page is not quiesced: the
	// destructive predicate is proved in SQL, not asserted by the caller.
	if _, err := pool.Exec(ctx, `INSERT INTO r2_delivery_page_retirement(generation,decision)
 VALUES(repeat('c',32),'test: retire early')`); err == nil || !strings.Contains(err.Error(), "quiesce") {
		t.Fatalf("retirement without quiesce was not refused in SQL: %v", err)
	}
	// Arm retirement: every new public reader is now refused, and no exemption
	// class opens yet.
	if _, err := pool.Exec(ctx, `INSERT INTO r2_delivery_page_quiesce(reason) VALUES('test: arming')`); err != nil {
		t.Fatal(err)
	}
	var serving bool
	if err := pool.QueryRow(ctx, `SELECT r2_delivery_public_serving(),r2_delivery_page_retired()`).Scan(&serving, &retired); err != nil {
		t.Fatal(err)
	}
	if serving || retired {
		t.Fatalf("after quiesce: serving=%v retired=%v, want serving=false retired=false", serving, retired)
	}
	for _, r := range requests {
		w := httptest.NewRecorder()
		router.ServeHTTP(w, httptest.NewRequest(r.method, r.target, nil))
		if w.Code != http.StatusServiceUnavailable {
			t.Fatalf("quiesced page: %s served %d, want 503", r.name, w.Code)
		}
	}
	// The serving loop minted recorded capabilities, and there is no cutover
	// authority at all, so retirement is refused on BOTH counts. An empty tracked
	// ledger would not be enough: the authority is what proves the untracked
	// legacy grant and the last real byte.
	var liveTickets int64
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM r2_delivery_download_tickets WHERE expires_at>clock_timestamp()`).Scan(&liveTickets); err != nil {
		t.Fatal(err)
	}
	if liveTickets == 0 {
		t.Fatal("the serving loop minted no recorded capability")
	}
	if _, err := pool.Exec(ctx, `INSERT INTO r2_delivery_page_retirement(generation,decision)
 VALUES(repeat('c',32),'test: retire before drain')`); err == nil || !strings.Contains(err.Error(), "cutover authority") {
		t.Fatalf("retirement without cutover authority was not refused in SQL: %v", err)
	}
	// Drain the recorded capabilities, then write reviewed cutover authority for
	// this exact generation whose derived deadline has already passed.
	if _, err := pool.Exec(ctx, `UPDATE r2_delivery_download_tickets SET expires_at=clock_timestamp()-interval '1 second'`); err != nil {
		t.Fatal(err)
	}
	writeCutoverAuthority(t, pool, repeatHex('c', 32))
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
	// The staging and retirement states are append-only facts: no caller can
	// un-arm or un-retire the page.
	for _, q := range []string{`DELETE FROM r2_delivery_page_quiesce`, `TRUNCATE r2_delivery_page_quiesce`,
		`UPDATE r2_delivery_page_quiesce SET reason='x'`, `DELETE FROM r2_delivery_page_retirement`,
		`UPDATE r2_delivery_page_retirement SET decision='x'`} {
		if _, err := pool.Exec(ctx, q); err == nil {
			t.Fatalf("state change %q succeeded", q)
		}
	}
}

// repeatHex is a hex literal of length n, for a generation id.
func repeatHex(c byte, n int) string { return strings.Repeat(string(c), n) }

// writeCutoverAuthority records the reviewed external cutover evidence for one
// generation, exactly as the page-cutover CLI does after the operator proves the
// legacy origins are gone, the caches expired and the transfers ended. The
// derived deadline is the latest bound plus the five-minute legacy grant window,
// so a bound older than that already yields a passed deadline and a newer one
// yields a future deadline. The row is append-only, so a test picks its bound at
// write time; nothing here can revise it afterwards.
func writeCutoverAuthorityWithin(t *testing.T, pool *pgxpool.Pool, generation string, bound time.Time) {
	t.Helper()
	// The authority must be bound to the live serving generation, so record the
	// pinned generation row it is admitted against.
	seedServingGeneration(t, pool, generation)
	if _, err := pool.Exec(context.Background(), `INSERT INTO r2_delivery_cutover_authority(
 generation,decision,legacy_grant_boundary_at,legacy_signed_grant_sha256,legacy_origin_removed_at,
 legacy_origin_evidence_sha256,cache_expired_at,cache_evidence_sha256,rollback_routing_sha256,
 transfer_bound_at,transfer_evidence_sha256,approval_sha256)
 VALUES($1,'test: cutover',$2,$3,$2,$4,$2,$5,$6,$2,$7,$8)`,
		generation, bound, strings.Repeat("a", 64), strings.Repeat("b", 64),
		strings.Repeat("c", 64), strings.Repeat("d", 64), strings.Repeat("e", 64),
		strings.Repeat("f", 64)); err != nil {
		t.Fatalf("write cutover authority: %v", err)
	}
}

// seedServingGeneration records the pinned generation row the authority is
// admitted against.
func seedServingGeneration(t *testing.T, pool *pgxpool.Pool, generation string) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), `INSERT INTO delivery_keep_holds(bucket,object_key,etag,size_bytes,kind,sha256)
 VALUES('stoarama',$1,'gen-etag',1,'registry',repeat('0',64)) ON CONFLICT(bucket,object_key) DO NOTHING`,
		globalDeliveryPrefix+"public/generations/"+generation+".json"); err != nil {
		t.Fatalf("seed serving generation: %v", err)
	}
}

// writeCutoverAuthority writes evidence whose derived deadline has already
// passed: every bound is ten minutes old and the window is five.
func writeCutoverAuthority(t *testing.T, pool *pgxpool.Pool, generation string) {
	t.Helper()
	writeCutoverAuthorityWithin(t, pool, generation, time.Now().Add(-10*time.Minute))
}

// TestDeliveryPageRetirementPausesAdmittedReader is the serialization regression:
// a real public file response is paused mid-body by its object store, retirement
// is attempted while it is provably admitted and streaming, and the same INSERT
// that was refused then commits once the response wrote its last byte.
func TestDeliveryPageRetirementPausesAdmittedReader(t *testing.T) {
	pool := deliveryExemptionTestPool(t)
	ctx := context.Background()
	reg := publicDeliveryRegistryFixture()
	store := newPausedDeliveryStore([]byte("hello"))
	s := &Server{pool: pool, globalDeliveryStore: store,
		publicGlobalDeliveryRegistry: reg, publicGlobalDeliveryRegistryAt: time.Now(), publicPinnedDeliveryRegistry: reg}
	router := publicDeliveryRouter(s)
	file := reg.Files["1"]

	// Serve the file in its own goroutine; its body blocks after the first chunk.
	done := make(chan int, 1)
	go func() {
		w := httptest.NewRecorder()
		router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, publicGlobalDeliveryAPI+"/file/1?sha256="+file.SHA256, nil))
		done <- w.Code
	}()
	<-store.streaming
	var live int64
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM r2_delivery_public_readers WHERE expires_at>clock_timestamp()`).Scan(&live); err != nil || live != 1 {
		t.Fatalf("streaming response holds %d leases: %v", live, err)
	}

	// Arm retirement while the response is paused. New readers are refused, the
	// paused one keeps its lease.
	if _, err := pool.Exec(ctx, `INSERT INTO r2_delivery_page_quiesce(reason) VALUES('test: paused reader')`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO r2_delivery_public_readers(reader_id,expires_at)
 VALUES('ffffffffffffffffffffffffffffffff',clock_timestamp()+interval '1 minute')`); err == nil {
		t.Fatal("a new reader was admitted after quiesce")
	}
	// Retirement is refused while the paused response holds its lease. The cutover
	// authority exists and its deadline has passed, so the ONLY remaining hold is
	// the admitted reader.
	writeCutoverAuthority(t, pool, repeatHex('a', 32))
	if _, err := pool.Exec(ctx, `INSERT INTO r2_delivery_page_retirement(generation,decision)
 VALUES(repeat('a',32),'test: retire during a streaming response')`); err == nil || !strings.Contains(err.Error(), "admitted public readers") {
		t.Fatalf("retirement during a streaming response was not refused: %v", err)
	}
	// A fresh public request is refused before it reaches storage.
	before := store.openedCount()
	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, publicGlobalDeliveryAPI+"/file/1?sha256="+file.SHA256, nil))
	if w.Code != http.StatusServiceUnavailable || store.openedCount() != before {
		t.Fatalf("quiesced page admitted a new reader: %d, opened %d", w.Code, store.openedCount()-before)
	}

	// Release the body: the handler writes its last byte, then drops its lease.
	close(store.release)
	if code := <-done; code != http.StatusOK {
		t.Fatalf("paused response finished with %d", code)
	}
	if live := store.leaseCount(t, pool); live != 0 {
		t.Fatalf("the response did not release its lease: %d", live)
	}
	// The recorded capabilities must also drain, and the reviewed cutover
	// authority must exist, before the same statement commits.
	if _, err := pool.Exec(ctx, `UPDATE r2_delivery_download_tickets SET expires_at=clock_timestamp()-interval '1 second'`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO r2_delivery_page_retirement(generation,decision)
 VALUES(repeat('a',32),'test: retire after the last byte')`); err != nil {
		t.Fatalf("retirement after the drain was refused: %v", err)
	}
	var retired, serving bool
	if err := pool.QueryRow(ctx, `SELECT r2_delivery_page_retired(),r2_delivery_public_serving()`).Scan(&retired, &serving); err != nil || !retired || serving {
		t.Fatalf("drained retirement did not take effect: retired=%v serving=%v %v", retired, serving, err)
	}
}

// TestDeliveryPageRetirementRequiresAuthorityAndDerivedDeadline is the P1 fix for
// the "empty ledger or a hardcoded two minutes is proof" hole:
//
//   - quiesce alone, with an EMPTY recorded ledger, is not retirement authority;
//   - the authority row may not be written before quiesce;
//   - the authority row may not claim a future transfer bound;
//   - even with an empty ledger, retirement is refused until the DERIVED deadline
//     (last legacy grant, removed origin, expired caches and verified transfer
//     bound, plus the legacy grant window) is in the past by the database clock.
//
// It then commits, which proves the gate is a proof and not a wall.
func TestDeliveryPageRetirementRequiresAuthorityAndDerivedDeadline(t *testing.T) {
	pool := deliveryExemptionTestPool(t)
	ctx := context.Background()
	generation := repeatHex('d', 32)

	seedServingGeneration(t, pool, generation)
	// Authority before quiesce proves nothing about the cutover, so it is refused.
	authority := func(transferBound string) error {
		_, err := pool.Exec(ctx, `INSERT INTO r2_delivery_cutover_authority(
 generation,decision,legacy_grant_boundary_at,legacy_signed_grant_sha256,legacy_origin_removed_at,
 legacy_origin_evidence_sha256,cache_expired_at,cache_evidence_sha256,rollback_routing_sha256,
 transfer_bound_at,transfer_evidence_sha256,approval_sha256)
 VALUES($1,'test',clock_timestamp()-interval '10 minutes',repeat('a',64),clock_timestamp()-interval '10 minutes',
   repeat('b',64),clock_timestamp()-interval '10 minutes',repeat('c',64),repeat('d',64),clock_timestamp()+$2::interval,repeat('e',64),repeat('f',64))`,
			generation, transferBound)
		return err
	}
	if err := authority("-1 second"); err == nil || !strings.Contains(err.Error(), "quiesce") {
		t.Fatalf("cutover authority before quiesce was accepted: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO r2_delivery_page_quiesce(reason) VALUES('test: authority')`); err != nil {
		t.Fatal(err)
	}
	// With an EMPTY recorded ledger and no authority, retirement is refused. This
	// is the exact state a fabricated "no tickets, so it is drained" claim would
	// see, and it must not be proof.
	if _, err := pool.Exec(ctx, `INSERT INTO r2_delivery_page_retirement(generation,decision) VALUES($1,'test: empty ledger')`, generation); err == nil || !strings.Contains(err.Error(), "cutover authority") {
		t.Fatalf("an empty ledger without authority retired the page: %v", err)
	}
	// A future transfer bound is not verified evidence.
	if err := authority("10 minutes"); err == nil || !strings.Contains(err.Error(), "future") {
		t.Fatalf("a future transfer bound was accepted as verified: %v", err)
	}
	// A bound one minute ago is admitted, and its DERIVED deadline (one minute ago
	// plus the five-minute legacy grant window) is still in the future, so
	// retirement is refused even with an empty ledger.
	if err := authority("-1 minute"); err != nil {
		t.Fatalf("a past verified bound was refused: %v", err)
	}
	var deadlineRemaining bool
	if err := pool.QueryRow(ctx, `SELECT deadline_at>clock_timestamp() FROM r2_delivery_cutover_authority WHERE singleton`).Scan(&deadlineRemaining); err != nil || !deadlineRemaining {
		t.Fatalf("derived deadline is not in the future: %v %v", deadlineRemaining, err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO r2_delivery_page_retirement(generation,decision) VALUES($1,'test: derived deadline')`, generation); err == nil || !strings.Contains(err.Error(), "deadline") {
		t.Fatalf("retirement before the derived deadline retired the page: %v", err)
	}
	// The authority is bound to one generation: a different one is refused.
	if _, err := pool.Exec(ctx, `INSERT INTO r2_delivery_page_retirement(generation,decision) VALUES($1,'test: other generation')`, repeatHex('e', 32)); err == nil || !strings.Contains(err.Error(), "generation") {
		t.Fatalf("retirement for a generation other than the authority's was accepted: %v", err)
	}
}

// TestDeliveryPageRetirementCommitsAfterDerivedDeadline proves the other side of
// the same gate, in its own schema because the authority row is immutable and
// singleton: an authority whose derived deadline is already past (its latest
// bound ten minutes old) lets the same statement commit.
func TestDeliveryPageRetirementCommitsAfterDerivedDeadline(t *testing.T) {
	pool := deliveryExemptionTestPool(t)
	ctx := context.Background()
	generation := repeatHex('d', 32)
	if _, err := pool.Exec(ctx, `INSERT INTO r2_delivery_page_quiesce(reason) VALUES('test: committed')`); err != nil {
		t.Fatal(err)
	}
	writeCutoverAuthority(t, pool, generation)
	var deadlineRemaining bool
	if err := pool.QueryRow(ctx, `SELECT deadline_at>clock_timestamp() FROM r2_delivery_cutover_authority WHERE singleton`).Scan(&deadlineRemaining); err != nil || deadlineRemaining {
		t.Fatalf("derived deadline should be in the past: %v %v", deadlineRemaining, err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO r2_delivery_page_retirement(generation,decision) VALUES($1,'test: committed')`, generation); err != nil {
		t.Fatalf("retirement after the derived deadline was refused: %v", err)
	}
}

// TestDeliveryPageRetirementTicketBoundary is the recorded-capability boundary:
// while any tracked capability is unexpired, retirement is refused; once every
// tracked capability has expired AND the authority's derived deadline has passed,
// retirement commits. The transfer bound itself lives only in the authority:
// there is no per-ticket fabrication.
func TestDeliveryPageRetirementTicketBoundary(t *testing.T) {
	pool := deliveryExemptionTestPool(t)
	ctx := context.Background()
	generation := repeatHex('b', 32)
	if _, err := pool.Exec(ctx, `INSERT INTO r2_delivery_download_tickets(bucket,object_key,etag,size_bytes,object_path_sha256,expires_at)
 VALUES('stoarama','original/x.mp4','e',5,repeat('a',64),clock_timestamp()+interval '5 minutes')`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO r2_delivery_page_quiesce(reason) VALUES('test: ticket boundary')`); err != nil {
		t.Fatal(err)
	}
	// A capability minted after quiesce is refused by SQL itself.
	if _, err := pool.Exec(ctx, `INSERT INTO r2_delivery_download_tickets(bucket,object_key,etag,size_bytes,object_path_sha256,expires_at)
 VALUES('stoarama','original/y.mp4','e',5,repeat('a',64),clock_timestamp()+interval '5 minutes')`); err == nil {
		t.Fatal("a capability was minted after quiesce")
	}
	writeCutoverAuthority(t, pool, generation)
	// Deadline passed, but a tracked capability is still live: refuse.
	if _, err := pool.Exec(ctx, `INSERT INTO r2_delivery_page_retirement(generation,decision) VALUES($1,'test: live capability')`, generation); err == nil || !strings.Contains(err.Error(), "unexpired") {
		t.Fatalf("retirement ignored a live recorded capability: %v", err)
	}
	// Drain the recorded capability; the same statement now commits.
	if _, err := pool.Exec(ctx, `UPDATE r2_delivery_download_tickets SET expires_at=clock_timestamp()-interval '1 second'`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO r2_delivery_page_retirement(generation,decision) VALUES($1,'test: drained')`, generation); err != nil {
		t.Fatalf("retirement after the capability drain was refused: %v", err)
	}
}

// pausedDeliveryStore serves a body that blocks mid-stream, so a response can be
// proven to be admitted and still writing its last bytes when retirement runs.
type pausedDeliveryStore struct {
	globalDeliveryTestStore
	release   chan struct{}
	streaming chan struct{}
	once      sync.Once
}

func newPausedDeliveryStore(body []byte) *pausedDeliveryStore {
	return &pausedDeliveryStore{release: make(chan struct{}), streaming: make(chan struct{})}
}

func (p *pausedDeliveryStore) OpenExact(ctx context.Context, key, etag, version string) (io.ReadCloser, error) {
	p.opened++
	return &pausedReader{Reader: bytes.NewReader(bytes.Repeat([]byte("hello"), 64)), store: p}, nil
}

func (p *pausedDeliveryStore) openedCount() int { return p.opened }

// leaseCount is how many admission leases the pool still has unexpired.
func (p *pausedDeliveryStore) leaseCount(t *testing.T, pool *pgxpool.Pool) int64 {
	t.Helper()
	var live int64
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM r2_delivery_public_readers WHERE expires_at>clock_timestamp()`).Scan(&live); err != nil {
		t.Fatal(err)
	}
	return live
}

// pausedReader yields one chunk, signals that it is streaming, waits for the
// test to release it, then yields the rest. Because io.CopyN writes each chunk
// before reading the next, the response is mid-body while it waits.
type pausedReader struct {
	*bytes.Reader
	store *pausedDeliveryStore
}

func (r *pausedReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if n > 0 {
		r.store.once.Do(func() { close(r.store.streaming) })
		<-r.store.release
	}
	return n, err
}

func (r *pausedReader) Close() error { return nil }

// TestDeliveryPublicLockMatchesSQL pins the Go constant to 0225's function, so
// the API's publication lock and the migration's admission/retirement lock can
// never drift apart.
func TestDeliveryPublicLockMatchesSQL(t *testing.T) {
	pool := deliveryExemptionTestPool(t)
	var sqlLock int64
	if err := pool.QueryRow(context.Background(), `SELECT r2_delivery_public_lock()`).Scan(&sqlLock); err != nil {
		t.Fatal(err)
	}
	if sqlLock != deliveryPublicLock {
		t.Fatalf("Go deliveryPublicLock %d != SQL r2_delivery_public_lock() %d", deliveryPublicLock, sqlLock)
	}
}

// TestDeliveryPageRetirementAuthorityBindsServingGeneration: the authority row is
// bound to the live serving registry generation, not to a second string the
// caller picks, and one artifact cannot stand in for several facts.
func TestDeliveryPageRetirementAuthorityBindsServingGeneration(t *testing.T) {
	pool := deliveryExemptionTestPool(t)
	ctx := context.Background()
	serving := repeatHex('a', 32)
	other := repeatHex('9', 32)
	if _, err := pool.Exec(ctx, `INSERT INTO r2_delivery_page_quiesce(reason) VALUES('test: generation binding')`); err != nil {
		t.Fatal(err)
	}
	insert := func(generation string, grant, origin, cache, rollback, transfer, approval string) error {
		_, err := pool.Exec(ctx, `INSERT INTO r2_delivery_cutover_authority(
 generation,decision,legacy_grant_boundary_at,legacy_signed_grant_sha256,legacy_origin_removed_at,
 legacy_origin_evidence_sha256,cache_expired_at,cache_evidence_sha256,rollback_routing_sha256,
 transfer_bound_at,transfer_evidence_sha256,approval_sha256)
 VALUES($1,'test',clock_timestamp()-interval '10 minutes',$2,clock_timestamp()-interval '10 minutes',$3,
  clock_timestamp()-interval '10 minutes',$4,$5,clock_timestamp()-interval '10 minutes',$6,$7)`,
			generation, grant, origin, cache, rollback, transfer, approval)
		return err
	}
	// No live serving generation row: the authority cannot be bound, so it is refused.
	if err := insert(serving, repeatHex('1', 64), repeatHex('2', 64), repeatHex('3', 64), repeatHex('4', 64), repeatHex('5', 64), repeatHex('6', 64)); err == nil || !strings.Contains(err.Error(), "live serving registry generation") {
		t.Fatalf("authority with no live serving generation was accepted: %v", err)
	}
	seedServingGeneration(t, pool, serving)
	// A generation other than the live serving one is refused.
	if err := insert(other, repeatHex('1', 64), repeatHex('2', 64), repeatHex('3', 64), repeatHex('4', 64), repeatHex('5', 64), repeatHex('6', 64)); err == nil || !strings.Contains(err.Error(), "not the live serving generation") {
		t.Fatalf("authority for a non-serving generation was accepted: %v", err)
	}
	// One artifact reused for several facts is refused.
	if err := insert(serving, repeatHex('1', 64), repeatHex('1', 64), repeatHex('3', 64), repeatHex('4', 64), repeatHex('5', 64), repeatHex('6', 64)); err == nil {
		t.Fatal("authority reused one evidence digest for two facts")
	}
	// The approval digest may not equal an evidence digest.
	if err := insert(serving, repeatHex('1', 64), repeatHex('2', 64), repeatHex('3', 64), repeatHex('4', 64), repeatHex('5', 64), repeatHex('1', 64)); err == nil {
		t.Fatal("authority reused an evidence digest as the approval digest")
	}
	// A complete, distinct, correctly bound record is admitted.
	if err := insert(serving, repeatHex('1', 64), repeatHex('2', 64), repeatHex('3', 64), repeatHex('4', 64), repeatHex('5', 64), repeatHex('6', 64)); err != nil {
		t.Fatalf("a complete bound authority was refused: %v", err)
	}
}
