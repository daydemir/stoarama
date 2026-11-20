package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/api"
	"github.com/jackc/pgx/v5"
)

func TestDeliveryNASHandoffMatchesFullLiveRosterBeforeQueueing(t *testing.T) {
	pool := joinedPurgeTestPool(t, "delivery_nas_handoff")
	ctx := context.Background()
	_, err := pool.Exec(ctx, `
CREATE TABLE connections(id bigint PRIMARY KEY,account_id bigint,kind text);
CREATE TABLE recordings(id bigint PRIMARY KEY,account_id bigint,stream_id bigint,delivery text);
CREATE TABLE recording_collation_hours(id bigint PRIMARY KEY,recording_id bigint,generation int,status text);
CREATE TABLE recording_collation_outputs(id bigint PRIMARY KEY,collation_hour_id bigint,object_key text,nas_relative_path text,sha256 text,size_bytes bigint,start_at timestamptz,end_at timestamptz);
CREATE TABLE delivery_keep_holds(bucket text,object_key text,kind text,etag text,size_bytes bigint,sha256 text);
CREATE TABLE recording_collation_output_acks(output_id bigint,connection_id bigint,nas_relative_path text,size_bytes bigint,sha256 text);
CREATE TABLE r2_object_retirements(kind text,source_id bigint);
CREATE FUNCTION nas_delivery_allowed(bigint,text,bigint) RETURNS boolean LANGUAGE sql STABLE AS $$ SELECT true $$;
INSERT INTO connections(id,account_id,kind) VALUES(13,47,'nas_pull');
INSERT INTO recordings VALUES(1,47,100,'nas_pull'),(2,47,200,'nas_pull');
INSERT INTO recording_collation_hours VALUES (11,1,2,'collated'),(12,2,2,'collated');
INSERT INTO recording_collation_outputs
SELECT g,CASE WHEN g=4 THEN 12 ELSE 11 END,'joined/goodplus-test/objects/'||g||'.mp4',
 'f/July/26-Sunday/f_hour_0'||g||'_000000-005900.mp4',repeat('a',64),100,
 CASE WHEN g=3 THEN '2026-08-28T00:00Z'::timestamptz ELSE '2026-08-05T00:00Z'::timestamptz END,
 CASE WHEN g=3 THEN '2026-08-28T01:00Z'::timestamptz ELSE '2026-08-05T01:00Z'::timestamptz END
FROM generate_series(1,4) g;
INSERT INTO delivery_keep_holds SELECT 'stoarama',object_key,
 CASE WHEN id=2 THEN 'video_assembly' ELSE 'video_collated' END,'etag',size_bytes,sha256
 FROM recording_collation_outputs;
`)
	if err != nil {
		t.Fatal(err)
	}
	migration, err := os.ReadFile("../../../infra/sql/migrations/0224_delivery_nas_handoff.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, string(migration)); err != nil {
		t.Fatal(err)
	}
	windows, err := deliveryNASWindows(r2RetirePicks{Choices: map[int64]r2RetirePick{
		100: {Choice: "none", KeepStartUTC: "2026-08-01T00:00:00Z", KeepEndUTC: "2026-08-28T00:00:00Z"},
		200: {Choice: "all"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(windows) != 1 || !windows[0].Start.Equal(time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("windows: %+v", windows)
	}
	canonical := `{"etag": "etag", "bucket": "stoarama", "end_at": "2026-08-05T01:00:00+00:00", "sha256": "` +
		strings.Repeat("a", 64) + `", "start_at": "2026-08-05T00:00:00+00:00", "output_id": 1, "stream_id": 100, "object_key": "joined/goodplus-test/objects/1.mp4", "size_bytes": 100, "recording_id": 1}` + "\n"
	if !json.Valid([]byte(canonical)) {
		t.Fatal("invalid fixture")
	}
	run := func(raw string, apply bool) (deliveryNASManifestSummary, error) {
		t.Helper()
		access := pgx.ReadOnly
		if apply {
			access = pgx.ReadWrite
		}
		tx, err := pool.BeginTx(ctx, pgx.TxOptions{AccessMode: access, IsoLevel: pgx.RepeatableRead})
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		var mount *[2]int64
		if apply {
			mount = &[2]int64{5, 9}
		}
		sum, err := queueDeliveryNASHandoff(ctx, tx, []byte(raw), windows, deliveryNASManifestSummary{
			RosterSHA: strings.Repeat("b", 64), PicksSHA: strings.Repeat("c", 64),
		}, mount)
		if err != nil {
			return sum, err
		}
		return sum, tx.Commit(ctx)
	}
	count := func() int {
		t.Helper()
		var result int
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM delivery_nas_handoff_outputs").Scan(&result); err != nil {
			t.Fatal(err)
		}
		return result
	}
	if sum, err := run(canonical, false); err != nil || sum.Rows != 1 || sum.Bytes != 100 || sum.Queued || count() != 0 {
		t.Fatalf("preview: %+v %v", sum, err)
	}
	if _, err := run(canonical+canonical, true); err == nil || count() != 0 {
		t.Fatalf("extra frozen row: %v", err)
	}
	for range 2 {
		if sum, err := run(canonical, true); err != nil || !sum.Queued || count() != 1 {
			t.Fatalf("queue: %+v %v", sum, err)
		}
	}
	key := "joined/goodplus-test/objects/1.mp4"
	keep := api.DeliveryKeepSnapshot{PublicGeneration: strings.Repeat("a", 32), PublicFiles: map[string][]api.DeliveryPublicFile{
		key: {{Identity: api.DeliveryKeepIdentity{Key: key, ETag: "etag", Size: 100, SHA256: strings.Repeat("a", 64), RecordingID: 1},
			FileURL: "https://stoarama.com/api/v1/global-street-scores-delivery/file/1"}},
	}}
	summary := deliveryNASManifestSummary{Rows: 1, Bytes: 100, RosterSHA: strings.Repeat("b", 64), PicksSHA: strings.Repeat("c", 64)}
	if _, err := deliveryNASCollectFileList(ctx, pool, summary, keep); err == nil {
		t.Fatal("file list exported without a readback")
	}
	if _, err := pool.Exec(ctx, `
 INSERT INTO delivery_nas_handoff_readbacks(output_id,connection_id,size_bytes,mtime_ns,ctime_ns,inode,device,root_inode)
 VALUES (1,13,100,2,3,4,5,9);
 INSERT INTO recording_collation_output_acks SELECT output_id,13,nas_relative_path,size_bytes,sha256 FROM delivery_nas_handoff_outputs`); err != nil {
		t.Fatal(err)
	}
	if _, err := deliveryNASCollectFileList(ctx, pool, summary, keep); err == nil {
		t.Fatal("the initial copy round was accepted as final file list evidence")
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if round, err := startDeliveryNASFinalRecheck(ctx, tx, summary); err != nil || round != 1 {
		tx.Rollback(ctx)
		t.Fatalf("start final recheck round=%d err=%v", round, err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := deliveryNASCollectFileList(ctx, pool, summary, keep); err == nil {
		t.Fatal("the final recheck was accepted without a current readback")
	}
	if _, err := pool.Exec(ctx, `
 INSERT INTO delivery_nas_handoff_readbacks(output_id,round,connection_id,size_bytes,mtime_ns,ctime_ns,inode,device,root_inode)
 VALUES (1,1,13,100,2,3,4,5,9)`); err != nil {
		t.Fatal(err)
	}
	if _, err := deliveryNASCollectFileList(ctx, pool, summary, keep); err == nil {
		t.Fatal("file list exported without a post-readback path check")
	}
	tx, err = pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if round, err := requestDeliveryNASFinalCheck(ctx, tx, summary); err != nil || round != 1 {
		tx.Rollback(ctx)
		t.Fatalf("request final check round=%d err=%v", round, err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := deliveryNASCollectFileList(ctx, pool, summary, keep); err == nil {
		t.Fatal("file list exported before the NAS completed the path check")
	}
	if _, err := pool.Exec(ctx, `UPDATE delivery_nas_handoff_checks SET checked_at=clock_timestamp() WHERE round=1`); err != nil {
		t.Fatal(err)
	}
	list, err := deliveryNASCollectFileList(ctx, pool, summary, keep)
	if err != nil || len(list.Files) != 1 || list.Files[0].NASRelativePath != "joined/f/July/26-Sunday/f_hour_01_000000-005900.mp4" {
		t.Fatalf("file list: %+v %v", list, err)
	}
	dest := filepath.Join(t.TempDir(), "delivery-file-list.json")
	if err := writeDeliveryNASFileList(dest, list); err != nil {
		t.Fatal(err)
	}
	if err := writeDeliveryNASFileList(dest, list); err == nil {
		t.Fatal("file list overwrote a published artifact")
	}
	tx, err = pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := requestDeliveryNASFinalCheck(ctx, tx, summary); err != nil {
		tx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	tx, err = pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := pauseDeliveryNASHandoff(ctx, tx); err != nil {
		tx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	var accepted, cancelled int
	if err := pool.QueryRow(ctx, `SELECT count(*) FILTER(WHERE checked_at IS NOT NULL),
 count(*) FILTER(WHERE cancelled_at IS NOT NULL) FROM delivery_nas_handoff_checks`).Scan(&accepted, &cancelled); err != nil ||
		accepted != 1 || cancelled != 1 {
		t.Fatalf("pause did not resolve the unanswered check: accepted=%d cancelled=%d err=%v", accepted, cancelled, err)
	}
	if _, err := deliveryNASCollectFileList(ctx, pool, summary, keep); err == nil {
		t.Fatal("file list exported using a cancelled latest check")
	}
	if _, err := pool.Exec(ctx, "UPDATE delivery_keep_holds SET etag='changed' WHERE object_key LIKE '%/1.mp4'"); err != nil {
		t.Fatal(err)
	}
	if _, err := run(canonical, true); err == nil || count() != 1 {
		t.Fatalf("stale frozen identity: %v", err)
	}
}
