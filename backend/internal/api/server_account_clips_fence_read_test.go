package api

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// This frozen production query predates the join rewrite. Keeping the reference
// independent catches accidental changes to common filters and returned fields.
func legacyClipsSQL(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile("testdata/account_clips_cursor_legacy.sql")
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func feedFenceFixture(t *testing.T) (*pgxpool.Pool, Keep28Window) {
	t.Helper()
	pool, cleanup := testAccountClipsPool(t)
	t.Cleanup(cleanup)
	_, windows, err := Keep28Policy()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(context.Background(), `INSERT INTO connections(id,account_id,kind) VALUES(13,47,'nas_pull');
 INSERT INTO streams(id,local_timezone) VALUES(3,'Asia/Tokyo');
 INSERT INTO recordings(id,account_id,name,delivery,stream_id,cron_timezone) VALUES
 (1,47,'good','nas_pull',3,'Asia/Tokyo'),(2,47,'fallback','nas_pull',3,'UTC'),
 (3,47,'null-zone','nas_pull',3,NULL),(4,47,'unknown-stream','nas_pull',99999,'Asia/Tokyo'),
 (5,99,'foreign','nas_pull',3,'Asia/Tokyo'),(6,47,'managed','managed',3,'Asia/Tokyo'),
 (7,47,'null-stream','nas_pull',NULL,'Asia/Tokyo'),
 (8,2147483648,'big-account','nas_pull',3,'Asia/Tokyo')`); err != nil {
		t.Fatal(err)
	}
	return pool, windows[3]
}

func activateFeedFence(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if err = ActivateNASDeliveryFence(ctx, tx); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}

func clipQueryRows(t *testing.T, tx pgx.Tx, sql string, account, after int64, limit int) [][]any {
	t.Helper()
	if sql == accountClipsCursorSQL {
		var err error
		sql, err = accountClipsQuery(context.Background(), tx, account)
		if err != nil {
			t.Fatal(err)
		}
	}
	rows, err := tx.Query(context.Background(), sql, account, after, limit)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	result := [][]any{}
	for rows.Next() {
		v, err := rows.Values()
		if err != nil {
			t.Fatal(err)
		}
		result = append(result, v)
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}

func compareFeedQueries(t *testing.T, pool *pgxpool.Pool, account, after int64, limit int) [][]any {
	t.Helper()
	ctx := context.Background()
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	before := clipQueryRows(t, tx, legacyClipsSQL(t), account, after, limit)
	afterRows := clipQueryRows(t, tx, accountClipsCursorSQL, account, after, limit)
	if !reflect.DeepEqual(before, afterRows) {
		t.Fatalf("feed differs account=%d cursor=%d limit=%d: legacy=%v joined=%v", account, after, limit, before, afterRows)
	}
	return afterRows
}

func TestNASKeep20SwitchChangesWorkWithoutChangingHistoricalFence(t *testing.T) {
	pool, oldWindow := feedFenceFixture(t)
	ctx := context.Background()
	activateFeedFence(t, pool)
	for _, ddl := range []string{
		`CREATE TABLE nas_cleanup_plans(mode text); CREATE TABLE nas_cleanup_batches(mode text);
CREATE TABLE nas_cleanup_items(state text); CREATE TABLE nas_cleanup_maintenance(active boolean)`,
		`CREATE TABLE nas_r2_copy_items(connection_id bigint,state text,item jsonb,lease_expires_at timestamptz);
CREATE TABLE nas_stitch_r2_hours(state text,lease_expires_at timestamptz,scope_id bigint,plan jsonb);
CREATE TABLE nas_stitch_tasks(state text,lease_expires_at timestamptz,scope_id bigint,plan jsonb,cleanup_plan_id bigint,registration_abandoned_at timestamptz);
CREATE TABLE nas_stitch_scopes(id bigint GENERATED ALWAYS AS IDENTITY,stream_id bigint,start_at timestamptz,end_at timestamptz,paused boolean,approved_until timestamptz)`,
	} {
		if _, err := pool.Exec(ctx, ddl); err != nil {
			t.Fatal(err)
		}
	}
	sql, err := os.ReadFile("../../../infra/sql/migrations/0236_nas_media_work_keep20.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, string(sql)); err != nil {
		t.Fatal(err)
	}
	var keptStart time.Time
	if err = pool.QueryRow(ctx, `SELECT start_at FROM nas_media_keep20_windows WHERE stream_id=3`).Scan(&keptStart); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO recording_clips(id,recording_id,size_bytes,clip_start_at,clip_end_at,display_path)
 VALUES(901,1,10,$1,$1::timestamptz+interval '1 minute','drop'),(902,1,10,$2,$2::timestamptz+interval '1 minute','keep')`, oldWindow.Start, keptStart); err != nil {
		t.Fatal(err)
	}
	pending := `INSERT INTO nas_r2_copy_items VALUES(13,'pending',jsonb_build_object('stream_id',3,'start_at',$1::timestamptz,'end_at',$1::timestamptz+interval '1 minute'),NULL)`
	if _, err = pool.Exec(ctx, pending, oldWindow.Start); err != nil {
		t.Fatal(err)
	}
	activate := `INSERT INTO nas_media_keep20_epoch(connection_id) VALUES(13)`
	if _, err = pool.Exec(ctx, activate); err == nil {
		t.Fatal("activation accepted a pending job outside the new window")
	}
	if _, err = pool.Exec(ctx, `DELETE FROM nas_r2_copy_items`); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, pending, keptStart); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, activate); err != nil {
		t.Fatal(err)
	}
	var legacy int
	var allowed, kept bool
	if err = pool.QueryRow(ctx, `SELECT nas_delivery_object_span('raw',901),nas_delivery_allowed(13,'raw',901),nas_delivery_allowed(13,'raw',902)`).Scan(&legacy, &allowed, &kept); err != nil ||
		legacy != 1 || allowed || !kept {
		t.Fatalf("legacy=%d old-day-work=%t kept-work=%t: %v", legacy, allowed, kept, err)
	}
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if rows := clipQueryRows(t, tx, accountClipsCursorSQL, 47, 0, 20); len(rows) != 1 {
		t.Fatalf("the scoped RAW page returned %d clips", len(rows))
	}
	if _, err = pool.Exec(ctx, pending, oldWindow.Start); err == nil {
		t.Fatal("old-day COPY admitted after activation")
	}
	if _, err = pool.Exec(ctx, `INSERT INTO nas_stitch_scopes(stream_id,start_at,end_at,paused,approved_until)
 VALUES(3,$1,$1::timestamptz+interval '1 minute',false,now()+interval '1 hour')`, oldWindow.Start); err == nil {
		t.Fatal("old-day stitch scope admitted after activation")
	}
	if _, err = pool.Exec(ctx, `INSERT INTO nas_stitch_r2_hours(state,plan) VALUES('pending',
 jsonb_build_object('stream_id',3,'clips',jsonb_build_array(jsonb_build_object('start_at',$1::timestamptz,'end_at',$1::timestamptz+interval '1 minute'))))`, oldWindow.Start); err == nil {
		t.Fatal("old-day stitch plan admitted after activation")
	}
}

func TestAccountClipsCursorFencePredicateEquivalent(t *testing.T) {
	pool, w := feedFenceFixture(t)
	ctx := context.Background()
	type span struct {
		name       string
		start, end any
	}
	start2020 := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	end2028 := time.Date(2028, 1, 1, 0, 0, 0, 0, time.UTC)
	spans := []span{
		{"inside", w.Start, w.Start.Add(time.Minute)},
		{"before", w.Start.Add(-2 * time.Minute), w.Start.Add(-time.Minute)},
		{"touch-start", w.Start.Add(-time.Minute), w.Start},
		{"touch-end", w.End, w.End.Add(time.Minute)},
		{"cross-start", w.Start.Add(-time.Minute), w.Start.Add(time.Minute)},
		{"cross-end", w.End.Add(-time.Minute), w.End.Add(time.Minute)},
		{"spans-window", w.Start.Add(-time.Minute), w.End.Add(time.Minute)},
		{"null-start", nil, w.End}, {"null-end", w.Start, nil},
		{"reverse", w.End, w.Start}, {"zero", w.Start, w.Start},
		{"old-invalid", start2020.Add(-time.Second), w.End},
		{"old-valid", start2020, w.Start.Add(time.Minute)},
		{"future-invalid", w.Start, end2028}, {"future-valid", w.Start, end2028.Add(-time.Second)},
	}
	id := int64(1)
	for rec := int64(1); rec <= 8; rec++ {
		for _, s := range spans {
			if _, err := pool.Exec(ctx, `INSERT INTO recording_clips(id,recording_id,size_bytes,clip_start_at,clip_end_at,display_path) VALUES($1,$2,10,$3,$4,$5)`, id, rec, s.start, s.end, s.name); err != nil {
				t.Fatal(err)
			}
			id++
		}
	}
	for _, mutation := range []string{
		`INSERT INTO recording_clips(id,recording_id,size_bytes,clip_start_at,clip_end_at) SELECT 201,1,0,$1,$2`,
		`INSERT INTO recording_clips(id,recording_id,size_bytes,clip_start_at,clip_end_at,purged_at) SELECT 202,1,10,$1,$2,now()`,
		`INSERT INTO recording_clips(id,recording_id,size_bytes,clip_start_at,clip_end_at,released_at) SELECT 203,1,10,$1,$2,now()`,
		`INSERT INTO recording_clips(id,recording_id,size_bytes,clip_start_at,clip_end_at,created_at) SELECT 204,1,10,$1,$2,now()+interval '1 hour'`,
		`INSERT INTO recording_clips(id,recording_id,size_bytes,clip_start_at,clip_end_at) SELECT 205,1,10,$1,$2`,
		`INSERT INTO recording_clips(id,recording_id,size_bytes,clip_start_at,clip_end_at) SELECT 206,1,10,$1,$2`,
	} {
		if _, err := pool.Exec(ctx, mutation, w.Start, w.End); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := pool.Exec(ctx, `INSERT INTO clip_storage_billing_contracts(clip_id,mode,authoritative) VALUES(205,'nas_collated_hold',false),(206,'other',true)`); err != nil {
		t.Fatal(err)
	}
	for _, fenced := range []bool{false, true} {
		if fenced {
			activateFeedFence(t, pool)
		}
		for _, zone := range []any{"Asia/Tokyo", "UTC", "", nil} {
			if _, err := pool.Exec(ctx, `UPDATE streams SET local_timezone=$1 WHERE id=3`, zone); err != nil {
				t.Fatal(err)
			}
			for _, account := range []int64{47, 99, 100, 2147483648} {
				for _, cursor := range []int64{0, 1, 7, 40, 200, 1000} {
					for _, limit := range []int{1, 3, 500} {
						t.Run(fmt.Sprintf("fence%v/zone%v/account%d/after%d/limit%d", fenced, zone, account, cursor, limit), func(t *testing.T) { compareFeedQueries(t, pool, account, cursor, limit) })
					}
				}
			}
		}
	}
}

func TestAccountClipsCursorExcludedTailStillFindsLaterKeptClip(t *testing.T) {
	pool, w := feedFenceFixture(t)
	ctx := context.Background()
	activateFeedFence(t, pool)
	if _, err := pool.Exec(ctx, `INSERT INTO recording_clips(id,recording_id,size_bytes,clip_start_at,clip_end_at)
 SELECT n,1,10,$1::timestamptz-interval '2 minutes',$1::timestamptz-interval '1 minute' FROM generate_series(1000,5999) n`, w.Start); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO recording_clips(id,recording_id,size_bytes,clip_start_at,clip_end_at) VALUES(6000,1,10,$1,$2)`, w.Start, w.End); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	rows := compareFeedQueries(t, pool, 47, 0, 500)
	if len(rows) != 1 || rows[0][0] != int64(6000) {
		t.Fatalf("tail swallowed eligible clip: %v", rows)
	}
	t.Logf("legacy and joined5000-row excluded-tail equivalence: %s", time.Since(started))
	if rows = compareFeedQueries(t, pool, 47, 6000, 500); len(rows) != 0 {
		t.Fatalf("terminal page changed: %v", rows)
	}
}
