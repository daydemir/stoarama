package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/smithy-go"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/daydemir/stoarama/backend/internal/db"
	"github.com/daydemir/stoarama/backend/internal/r2"
)

func TestParseJoinedSourcePurgeRunArgsDefaultsToVerifiedDryRun(t *testing.T) {
	opts, err := parseJoinedSourcePurgeRunArgs([]string{"--log", "/tmp/x.log"})
	if err != nil {
		t.Fatal(err)
	}
	if opts.apply || opts.checkR2 || !opts.requireNAS || opts.shaEvery != 50 || opts.rate != 10 || opts.pageSize != 500 || opts.limit != 0 || opts.logPath != "/tmp/x.log" {
		t.Fatalf("unexpected defaults: %+v", opts)
	}
	opts, err = parseJoinedSourcePurgeRunArgs(nil)
	if err != nil || !strings.Contains(opts.logPath, "joined-source-purge-") {
		t.Fatalf("default log path %q err=%v", opts.logPath, err)
	}
	for _, bad := range [][]string{
		{"--rate", "0"}, {"--rate", "500"}, {"--page-size", "0"}, {"--page-size", "9999"}, {"--limit", "-1"},
		{"--sha-every", "-1"}, {"--max-errors", "0"}, {"--apply", "--check-r2"}, {"--hour-record-id", "-4"}, {"extra"},
	} {
		if _, err := parseJoinedSourcePurgeRunArgs(bad); err == nil {
			t.Fatalf("expected error for %v", bad)
		}
	}
}

func TestClassifyJoinedPurgeCandidateReportsFirstFailingRule(t *testing.T) {
	ok := joinedPurgeCandidate{ClipID: 1, SizeBytes: 10, SHA256: strings.Repeat("a", 64), ObjectKey: "k", ClipBucket: "stoarama",
		Managed: true, DestBucket: "stoarama", Disposition: "included", HourFinal: true, MediaID: 9, MediaKey: "m",
		MediaSize: 5, MediaSHA256: strings.Repeat("b", 64), MediaPublished: true, StructEligible: true, NASVerified: true}
	if got := classifyJoinedPurgeCandidate(ok, "stoarama", true); got != "" {
		t.Fatalf("eligible candidate classified %q", got)
	}
	cases := map[string]func(*joinedPurgeCandidate){
		"not_included":             func(c *joinedPurgeCandidate) { c.Disposition = "quarantined" },
		"hour_held":                func(c *joinedPurgeCandidate) { c.HourHeld, c.HourFinal = true, false },
		"hour_not_final":           func(c *joinedPurgeCandidate) { c.HourFinal = false },
		"media_unpublished":        func(c *joinedPurgeCandidate) { c.MediaPublished = false },
		"structurally_ineligible":  func(c *joinedPurgeCandidate) { c.StructEligible = false },
		"not_managed_storage":      func(c *joinedPurgeCandidate) { c.Managed = false },
		"clip_identity_incomplete": func(c *joinedPurgeCandidate) { c.SHA256 = "" },
		"nas_unverified":           func(c *joinedPurgeCandidate) { c.NASVerified = false },
	}
	for want, mutate := range cases {
		c := ok
		mutate(&c)
		if got := classifyJoinedPurgeCandidate(c, "stoarama", true); got != want {
			t.Fatalf("want %q got %q", want, got)
		}
	}
	c := ok
	c.DestBucket, c.ClipBucket = "other", "other"
	if got := classifyJoinedPurgeCandidate(c, "stoarama", true); got != "not_managed_storage" {
		t.Fatalf("foreign bucket classified %q", got)
	}
	c = ok
	c.NASVerified = false
	if got := classifyJoinedPurgeCandidate(c, "stoarama", false); got != "" {
		t.Fatalf("--require-nas=false still required NAS: %q", got)
	}
}

const joinedPurgeMigration = "0155_joined_source_retention_purge.sql"

func joinedPurgeMigrationsDir() string {
	return filepath.Join("..", "..", "..", "infra", "sql", "migrations")
}

func joinedPurgeTestPool(t *testing.T, prefix string) *pgxpool.Pool {
	t.Helper()
	databaseURL := strings.TrimSpace(os.Getenv("STOARAMA_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		_, _ = admin.Exec(context.Background(), "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
		admin.Close()
	})
	return pool
}

// joinedPurgeFixture builds only the columns the retention guard, the 0155
// functions and the candidate scan read, applies 0155 verbatim, and attaches
// the production triggers (0137/0140) to the retention guard.
func joinedPurgeFixture(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool := joinedPurgeTestPool(t, "joined_purge")
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `
CREATE TABLE storage_destinations(id bigint PRIMARY KEY, managed boolean NOT NULL, bucket text);
CREATE TABLE recording_clips(id bigint PRIMARY KEY, recording_id bigint NOT NULL, recording_job_id bigint, storage_destination_id bigint,
  endpoint text, bucket text, object_key text, etag text, size_bytes bigint, sha256 text, display_path text,
  clip_start_at timestamptz, clip_end_at timestamptz, created_at timestamptz NOT NULL DEFAULT now(), purged_at timestamptz, released_at timestamptz);
CREATE TABLE recording_joined_batches(id bigint PRIMARY KEY, batch_id text NOT NULL UNIQUE, connection_id bigint NOT NULL, generation int NOT NULL, state text NOT NULL);
CREATE TABLE recording_joined_hours(id bigint PRIMARY KEY, batch_record_id bigint NOT NULL, hour_id text NOT NULL, state text NOT NULL, sealed_at timestamptz);
CREATE TABLE recording_joined_source_snapshots(id bigint PRIMARY KEY, batch_record_id bigint NOT NULL, clip_id bigint NOT NULL);
CREATE TABLE recording_joined_sources(id bigint PRIMARY KEY, source_snapshot_id bigint NOT NULL UNIQUE, hour_record_id bigint NOT NULL, clip_id bigint NOT NULL);
CREATE TABLE recording_joined_artifacts(id bigint PRIMARY KEY, batch_record_id bigint NOT NULL, scope_kind text NOT NULL, scope_id text NOT NULL,
  hour_record_id bigint, artifact_kind text NOT NULL, publication_state text, published_at timestamptz, object_key text NOT NULL,
  etag text, version_id text, expected_size_bytes bigint NOT NULL, expected_sha256 text NOT NULL);
CREATE TABLE recording_joined_hour_dispositions(hour_record_id bigint NOT NULL, source_id bigint NOT NULL, disposition text NOT NULL,
  media_artifact_id bigint, PRIMARY KEY(hour_record_id, source_id));
CREATE TABLE recording_joined_media_sources(artifact_id bigint NOT NULL, source_id bigint NOT NULL UNIQUE, ordinal int NOT NULL DEFAULT 1, PRIMARY KEY(artifact_id, source_id));
CREATE TABLE recording_joined_snapshot_scopes(id bigserial PRIMARY KEY, batch_record_id bigint NOT NULL, recording_id bigint NOT NULL, recording_job_id bigint NOT NULL, high_water_clip_id bigint NOT NULL);
CREATE TABLE recording_joined_dry_runs(id uuid PRIMARY KEY, batch_id text NOT NULL, connection_id bigint NOT NULL, generation int NOT NULL);
CREATE TABLE recording_joined_dry_run_scopes(dry_run_id uuid NOT NULL, recording_id bigint NOT NULL, recording_job_id bigint NOT NULL, high_water_clip_id bigint NOT NULL);
CREATE TABLE nas_inventory_files(connection_id bigint NOT NULL, clip_id bigint NOT NULL, state text NOT NULL, sha256 text, size_bytes bigint,
  relative_path text, verified_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY(connection_id, clip_id));
`); err != nil {
		t.Fatal(err)
	}
	// Seed hour that the migration's hold backfill must pick up by hour_id.
	if _, err := pool.Exec(ctx, `
INSERT INTO recording_joined_batches VALUES (99,'goodplus-20260821-generation-1',13,1,'frozen');
INSERT INTO recording_joined_hours VALUES (990,99,'goodplus-20260821-generation-1__recording-382__date-2026-08-01__hour-09__generation-1','sealed',now()-interval '2 hours');`); err != nil {
		t.Fatal(err)
	}
	migration, err := os.ReadFile(filepath.Join(joinedPurgeMigrationsDir(), joinedPurgeMigration))
	if err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, string(migration)); err != nil {
		t.Fatalf("apply %s: %v", joinedPurgeMigration, err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
CREATE TRIGGER recording_joined_clip_retention_update BEFORE UPDATE OF purged_at ON recording_clips
FOR EACH ROW EXECUTE FUNCTION guard_recording_joined_clip_retention();
CREATE TRIGGER recording_joined_clip_identity_update BEFORE UPDATE OF id ON recording_clips
FOR EACH ROW EXECUTE FUNCTION guard_recording_joined_clip_retention();
CREATE TRIGGER recording_joined_clip_retention_delete BEFORE DELETE ON recording_clips
FOR EACH ROW EXECUTE FUNCTION guard_recording_joined_clip_retention();
CREATE TRIGGER recording_joined_scoped_clip_identity_update BEFORE UPDATE OF recording_id,recording_job_id,
  storage_destination_id,endpoint,bucket,object_key,etag,size_bytes,sha256,clip_start_at,clip_end_at,created_at
  ON recording_clips FOR EACH ROW EXECUTE FUNCTION guard_recording_joined_clip_retention();

INSERT INTO storage_destinations VALUES (20,true,'stoarama'),(21,false,'byo');
INSERT INTO recording_joined_batches VALUES (1,'b1',13,1,'frozen'),(2,'b2',13,1,'snapshotting');
-- Run A was consumed by frozen batch b1; run B (b3) never froze a batch.
INSERT INTO recording_joined_dry_runs VALUES ('00000000-0000-0000-0000-00000000000a','b1',13,1),('00000000-0000-0000-0000-00000000000b','b3',13,1);
INSERT INTO recording_joined_dry_run_scopes VALUES ('00000000-0000-0000-0000-00000000000a',1,100,1000),('00000000-0000-0000-0000-00000000000b',2,200,1000);
INSERT INTO recording_joined_snapshot_scopes(batch_record_id,recording_id,recording_job_id,high_water_clip_id) VALUES (2,3,300,1000);
INSERT INTO recording_joined_hours VALUES
  (11,1,'h1','sealed',now()-interval '2 hours'),   -- final
  (12,1,'h2','sealed',now()-interval '2 hours'),   -- held
  (13,1,'h3','sealed',now()-interval '2 hours'),   -- one media part unpublished
  (14,1,'h4','sealed',now()-interval '2 hours'),   -- manifest still sealed, not published
  (15,1,'h5','sealed',now()-interval '5 minutes'); -- final but sealed too recently
INSERT INTO recording_joined_source_retention_holds(hour_record_id,reason_code) VALUES (12,'dual_recorder_overlap');
INSERT INTO recording_joined_artifacts VALUES
  (111,1,'hour','h1',11,'hour_manifest','published',now(),'joined/h1.json','e',  '',10,repeat('0',64)),
  (112,1,'hour','h1',11,'media',NULL,now(),'joined/m1.mp4','etag-m1','',7,'`+joinedPurgeSHA("media-1")+`'),
  (113,1,'hour','h1',11,'media',NULL,now(),'joined/m2.mp4','etag-m2','',7,'`+joinedPurgeSHA("media-2")+`'),
  (121,1,'hour','h2',12,'hour_manifest','published',now(),'joined/h2.json','e','',10,repeat('0',64)),
  (122,1,'hour','h2',12,'media',NULL,now(),'joined/m3.mp4','etag-m3','',7,'`+joinedPurgeSHA("media-3")+`'),
  (131,1,'hour','h3',13,'hour_manifest','published',now(),'joined/h3.json','e','',10,repeat('0',64)),
  (132,1,'hour','h3',13,'media',NULL,now(),'joined/m4.mp4','etag-m4','',7,'`+joinedPurgeSHA("media-4")+`'),
  (133,1,'hour','h3',13,'media',NULL,NULL,'joined/m5.mp4',NULL,NULL,7,'`+joinedPurgeSHA("media-5")+`'),
  (141,1,'hour','h4',14,'hour_manifest','sealed',NULL,'joined/h4.json',NULL,NULL,10,repeat('0',64)),
  (142,1,'hour','h4',14,'media',NULL,now(),'joined/m6.mp4','etag-m6','',7,'`+joinedPurgeSHA("media-6")+`'),
  (151,1,'hour','h5',15,'hour_manifest','published',now(),'joined/h5.json','e','',10,repeat('0',64)),
  (152,1,'hour','h5',15,'media',NULL,now(),'joined/m7.mp4','etag-m7','',7,'`+joinedPurgeSHA("media-7")+`');
`); err != nil {
		t.Fatal(err)
	}
	// clip id, recording, job, hour, disposition, media artifact, NAS copy
	type clipSpec struct {
		id, rec, job, hour int64
		disp               string
		media              int64
		nas                bool
	}
	specs := []clipSpec{
		{1, 1, 100, 11, "included", 112, true},  // eligible (dry-run A consumed)
		{2, 1, 100, 11, "included", 113, true},  // eligible
		{3, 1, 100, 11, "quarantined", 0, true}, // not included
		{4, 1, 100, 12, "included", 122, true},  // held hour
		{5, 1, 100, 13, "included", 132, true},  // hour has an unpublished part
		{6, 1, 100, 11, "included", 112, true},  // also snapshotted by batch 2, unallocated
		{7, 1, 100, 11, "included", 113, false}, // no NAS copy
		{8, 2, 200, 11, "included", 112, true},  // unconsumed dry-run scope
		{10, 3, 300, 11, "included", 113, true}, // active snapshotting scope
		{12, 1, 100, 14, "included", 142, true}, // manifest not published
		{13, 1, 100, 15, "included", 152, true}, // sealed under an hour ago
	}
	for i, s := range specs {
		snap, src := 500+int64(i), 700+int64(i)
		if _, err := pool.Exec(ctx, `INSERT INTO recording_clips(id,recording_id,recording_job_id,storage_destination_id,bucket,object_key,size_bytes,sha256,display_path)
			VALUES ($1,$2,$3,20,'stoarama',$4,5,$5,$6)`, s.id, s.rec, s.job, joinedPurgeSourceKey(s.id), joinedPurgeSHA(fmt.Sprintf("clip-%d", s.id)), fmt.Sprintf("rec/%d.mp4", s.id)); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `INSERT INTO recording_joined_source_snapshots VALUES ($1,1,$2)`, snap, s.id); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `INSERT INTO recording_joined_sources VALUES ($1,$2,$3,$4)`, src, snap, s.hour, s.id); err != nil {
			t.Fatal(err)
		}
		var media any
		if s.media != 0 {
			media = s.media
			if _, err := pool.Exec(ctx, `INSERT INTO recording_joined_media_sources(artifact_id,source_id,ordinal) VALUES ($1,$2,$3)`, s.media, src, i+1); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := pool.Exec(ctx, `INSERT INTO recording_joined_hour_dispositions VALUES ($1,$2,$3,$4)`, s.hour, src, s.disp, media); err != nil {
			t.Fatal(err)
		}
		if s.nas {
			if _, err := pool.Exec(ctx, `INSERT INTO nas_inventory_files(connection_id,clip_id,state,sha256,size_bytes,relative_path) VALUES (13,$1,'present',$2,5,$3)`,
				s.id, joinedPurgeSHA(fmt.Sprintf("clip-%d", s.id)), fmt.Sprintf("rec/%d.mp4", s.id)); err != nil {
				t.Fatal(err)
			}
		}
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO recording_joined_source_snapshots VALUES (900,2,6);
-- An unprotected clip (never snapshotted) keeps ordinary purge semantics.
INSERT INTO recording_clips(id,recording_id,recording_job_id,storage_destination_id,bucket,object_key,size_bytes,sha256,display_path)
  VALUES (11,9,900,20,'stoarama','src/11.mp4',5,repeat('c',64),'rec/11.mp4');`); err != nil {
		t.Fatal(err)
	}
	return pool
}

func joinedPurgeSHA(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func joinedPurgeSourceKey(id int64) string { return fmt.Sprintf("src/%d.mp4", id) }

func joinedPurgeTryPurge(ctx context.Context, pool *pgxpool.Pool, clipID int64, verified bool, iso pgx.TxIsoLevel) error {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: iso})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if verified {
		if _, err := tx.Exec(ctx, `SELECT set_config('stoarama.joined_source_purge','verified',true)`); err != nil {
			return err
		}
	}
	if _, err := tx.Exec(ctx, `UPDATE recording_clips SET purged_at=now() WHERE id=$1`, clipID); err != nil {
		return err
	}
	return nil // rolled back: callers only probe the guard
}

func TestJoinedSourcePurgeTriggerAllowsOnlyVerifiedEligibleSources(t *testing.T) {
	pool := joinedPurgeFixture(t)
	ctx := context.Background()

	var heldBackfill string
	if err := pool.QueryRow(ctx, `SELECT reason_code FROM recording_joined_source_retention_holds WHERE hour_record_id=990`).Scan(&heldBackfill); err != nil || heldBackfill != "dual_recorder_overlap" {
		t.Fatalf("migration did not backfill the known overlap hold: %q %v", heldBackfill, err)
	}
	for _, id := range []int64{1, 2} {
		if err := joinedPurgeTryPurge(ctx, pool, id, true, pgx.ReadCommitted); err != nil {
			t.Fatalf("eligible clip %d refused: %v", id, err)
		}
		if err := joinedPurgeTryPurge(ctx, pool, id, false, pgx.ReadCommitted); err == nil || !strings.Contains(err.Error(), "retention protected") {
			t.Fatalf("clip %d purged without the verified GUC: %v", id, err)
		}
		if err := joinedPurgeTryPurge(ctx, pool, id, true, pgx.RepeatableRead); err == nil || !strings.Contains(err.Error(), "read committed") {
			t.Fatalf("clip %d purged outside read committed: %v", id, err)
		}
	}
	for id, why := range map[int64]string{3: "quarantined", 4: "held hour", 5: "unpublished media part", 6: "unallocated second snapshot",
		8: "unconsumed dry-run scope", 10: "snapshotting scope", 12: "manifest not published", 13: "sealed under an hour ago"} {
		if err := joinedPurgeTryPurge(ctx, pool, id, true, pgx.ReadCommitted); err == nil || !strings.Contains(err.Error(), "retention protected") {
			t.Fatalf("clip %d (%s) was purgeable: %v", id, why, err)
		}
	}
	// NAS is enforced by the command, not the trigger: clip 7 is structurally eligible.
	if err := joinedPurgeTryPurge(ctx, pool, 7, true, pgx.ReadCommitted); err != nil {
		t.Fatalf("clip 7 structurally eligible but refused: %v", err)
	}
	if err := joinedPurgeTryPurge(ctx, pool, 11, false, pgx.ReadCommitted); err != nil {
		t.Fatalf("unprotected clip purge changed semantics: %v", err)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM recording_clips WHERE id=1`); err == nil || !strings.Contains(err.Error(), "retention protected") {
		t.Fatalf("frozen source row delete allowed: %v", err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = tx.Exec(ctx, `SELECT set_config('stoarama.joined_source_purge','verified',true)`)
	if _, err := tx.Exec(ctx, `UPDATE recording_clips SET purged_at=now(), object_key='elsewhere' WHERE id=1`); err == nil || !strings.Contains(err.Error(), "immutable") {
		t.Fatalf("purge smuggled an identity change: %v", err)
	}
	_ = tx.Rollback(ctx)
}

type fakePurgeStore struct {
	mu        sync.Mutex
	bucket    string
	objects   map[string][]byte
	etags     map[string]string
	deleted   []string
	deleteErr error
}

func (s *fakePurgeStore) Bucket() string { return s.bucket }

func (s *fakePurgeStore) Head(_ context.Context, key string) (r2.ObjectHead, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.objects[key]
	if !ok {
		return r2.ObjectHead{}, &smithy.GenericAPIError{Code: "NotFound", Message: "not found"}
	}
	return r2.ObjectHead{SizeBytes: int64(len(b)), ETag: s.etags[key]}, nil
}

func (s *fakePurgeStore) HeadExact(ctx context.Context, key, etag, _ string) (r2.ObjectHead, error) {
	h, err := s.Head(ctx, key)
	if err == nil && h.ETag != etag {
		return r2.ObjectHead{}, errors.New("etag changed")
	}
	return h, err
}

func (s *fakePurgeStore) OpenExact(ctx context.Context, key, etag, v string) (io.ReadCloser, error) {
	if _, err := s.HeadExact(ctx, key, etag, v); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return io.NopCloser(bytes.NewReader(s.objects[key])), nil
}

func (s *fakePurgeStore) DeleteObjects(_ context.Context, keys []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.deleteErr != nil {
		return s.deleteErr
	}
	for _, k := range keys {
		delete(s.objects, k)
		s.deleted = append(s.deleted, k)
	}
	return nil
}

func newFakePurgeStore() *fakePurgeStore {
	s := &fakePurgeStore{bucket: "stoarama", objects: map[string][]byte{}, etags: map[string]string{}}
	for i := 1; i <= 6; i++ {
		key := fmt.Sprintf("joined/m%d.mp4", i)
		s.objects[key] = []byte(fmt.Sprintf("media-%d", i))
		s.etags[key] = fmt.Sprintf("etag-m%d", i)
	}
	for _, id := range []int64{1, 2, 3, 4, 5, 6, 7, 8, 10, 12, 13} {
		s.objects[joinedPurgeSourceKey(id)] = []byte(fmt.Sprintf("src-%d", id%10))
	}
	return s
}

func joinedPurgeTestOpts(t *testing.T, apply bool) joinedSourcePurgeOptions {
	opts, err := parseJoinedSourcePurgeRunArgs([]string{"--rate", "200", "--sha-every", "1", "--log", filepath.Join(t.TempDir(), "purge.log")})
	if err != nil {
		t.Fatal(err)
	}
	opts.apply = apply
	return opts
}

func joinedPurgeLogActions(t *testing.T, raw []byte) map[int64]string {
	t.Helper()
	out := map[int64]string{}
	sc := bufio.NewScanner(bytes.NewReader(raw))
	for sc.Scan() {
		var line joinedPurgeLogLine
		if err := json.Unmarshal(sc.Bytes(), &line); err != nil {
			t.Fatalf("bad log line %q: %v", sc.Text(), err)
		}
		out[line.ClipID] = line.Action + ":" + line.Reason
	}
	return out
}

func TestJoinedSourcePurgeRunDryRunThenApplyIsResumable(t *testing.T) {
	pool := joinedPurgeFixture(t)
	ctx := context.Background()
	store := newFakePurgeStore()

	var dryLog bytes.Buffer
	dry, err := runJoinedSourcePurgePass(ctx, pool, store, joinedPurgeTestOpts(t, false), &dryLog)
	if err != nil {
		t.Fatal(err)
	}
	if dry.Eligible != 2 || dry.EligibleBytes != 10 || len(store.deleted) != 0 {
		t.Fatalf("dry run: %+v deleted=%v", dry, store.deleted)
	}
	want := map[string]int64{"not_included": 1, "hour_held": 1, "hour_not_final": 3, "structurally_ineligible": 3, "nas_unverified": 1}
	for k, v := range want {
		if dry.Skipped[k] != v {
			t.Fatalf("dry-run skipped[%s]=%d want %d (all=%v)", k, dry.Skipped[k], v, dry.Skipped)
		}
	}
	if acts := joinedPurgeLogActions(t, dryLog.Bytes()); acts[1] != "eligible:" || acts[7] != "skip:nas_unverified" {
		t.Fatalf("dry-run log: %v", acts)
	}

	// A failed delete rolls purged_at back.
	store.deleteErr = errors.New("r2 unavailable")
	failed, err := runJoinedSourcePurgePass(ctx, pool, store, joinedPurgeTestOpts(t, true), io.Discard)
	if err != nil || failed.Purged != 0 || failed.Errors != 2 {
		t.Fatalf("delete failure: %+v err=%v", failed, err)
	}
	var purgedCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM recording_clips WHERE purged_at IS NOT NULL`).Scan(&purgedCount); err != nil || purgedCount != 0 {
		t.Fatalf("purged_at survived a failed delete: %d %v", purgedCount, err)
	}
	store.deleteErr = nil

	// Limit to one clip first (the canary pattern), then finish.
	opts := joinedPurgeTestOpts(t, true)
	opts.limit = 1
	var applyLog bytes.Buffer
	first, err := runJoinedSourcePurgePass(ctx, pool, store, opts, &applyLog)
	if err != nil || first.Purged != 1 || !first.LimitReached {
		t.Fatalf("limited apply: %+v err=%v", first, err)
	}
	delete(store.objects, joinedPurgeSourceKey(2)) // simulate a crash after delete, before commit
	rest, err := runJoinedSourcePurgePass(ctx, pool, store, joinedPurgeTestOpts(t, true), &applyLog)
	if err != nil || rest.Purged != 1 || rest.SourceAbsent != 1 {
		t.Fatalf("resumed apply: %+v err=%v", rest, err)
	}
	rows, err := pool.Query(ctx, `SELECT id FROM recording_clips WHERE purged_at IS NOT NULL ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[int64])
	if err != nil || fmt.Sprint(ids) != "[1 2]" {
		t.Fatalf("purged ids %v err=%v", ids, err)
	}
	var rowsLeft int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM recording_clips WHERE id IN (1,2) AND object_key IS NOT NULL`).Scan(&rowsLeft); err != nil || rowsLeft != 2 {
		t.Fatalf("clip rows not retained: %d %v", rowsLeft, err)
	}
	if _, ok := store.objects["joined/m1.mp4"]; !ok {
		t.Fatal("joined media deleted")
	}
	for _, key := range store.deleted {
		if !strings.HasPrefix(key, "src/") {
			t.Fatalf("deleted non-source key %q", key)
		}
	}
	if acts := joinedPurgeLogActions(t, applyLog.Bytes()); acts[1] != "purged:" || acts[2] != "purged:source_already_absent" {
		t.Fatalf("apply log: %v", acts)
	}
	again, err := runJoinedSourcePurgePass(ctx, pool, store, joinedPurgeTestOpts(t, true), io.Discard)
	if err != nil || again.Purged != 0 || again.Eligible != 0 {
		t.Fatalf("rerun not idempotent: %+v err=%v", again, err)
	}
}

func TestJoinedSourcePurgeRunRefusesUnverifiedJoinedMedia(t *testing.T) {
	pool := joinedPurgeFixture(t)
	ctx := context.Background()

	store := newFakePurgeStore()
	store.objects["joined/m1.mp4"] = []byte("short") // size mismatch -> skip clip 1 only
	s, err := runJoinedSourcePurgePass(ctx, pool, store, joinedPurgeTestOpts(t, true), io.Discard)
	if err != nil || s.Purged != 1 || s.Skipped["media_size_mismatch"] != 1 {
		t.Fatalf("size mismatch: %+v err=%v", s, err)
	}

	pool = joinedPurgeFixture(t)
	store = newFakePurgeStore()
	store.objects["joined/m1.mp4"] = []byte("MEDIA-1") // same size, wrong bytes -> abort
	s, err = runJoinedSourcePurgePass(ctx, pool, store, joinedPurgeTestOpts(t, true), io.Discard)
	if err == nil || !strings.Contains(err.Error(), "sha256") || s.Purged != 0 || len(store.deleted) != 0 {
		t.Fatalf("sha mismatch did not abort before any delete: %+v err=%v", s, err)
	}
}

func TestJoinedSourcePurgeMigrationRunsThroughMigrateUp(t *testing.T) {
	pool := joinedPurgeTestPool(t, "joined_purge_migrate")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	if err := db.MigrateUp(ctx, pool, joinedPurgeMigrationsDir()); err != nil {
		t.Fatalf("MigrateUp: %v", err)
	}
	var fns int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
		WHERE n.nspname=current_schema() AND p.proname IN ('recording_joined_hour_retention_final','recording_joined_source_purge_eligible')`).Scan(&fns); err != nil || fns != 2 {
		t.Fatalf("retention functions missing: %d %v", fns, err)
	}
	var eligible bool
	if err := pool.QueryRow(ctx, `SELECT recording_joined_source_purge_eligible(1,1,1)`).Scan(&eligible); err != nil || eligible {
		t.Fatalf("unknown clip eligible=%t err=%v", eligible, err)
	}
	var holds int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM recording_joined_source_retention_holds`).Scan(&holds); err != nil || holds != 0 {
		t.Fatalf("holds on an empty schema: %d %v", holds, err)
	}
}
