package api

import (
	"context"
	"github.com/jackc/pgx/v5"
	"os"
	"strings"
	"testing"
	"time"
)

// Do not let deploying the feed query build a blocking index over existing data.
func TestAccountClipsWindowIndexMigration(t *testing.T) {
	body, err := os.ReadFile("../../../infra/sql/migrations/0197_nas_delivery_window_index.sql")
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"empty", "populated-missing", "precreated", "wrong-definition", "invalid"} {
		t.Run(kind, func(t *testing.T) {
			pool, _ := feedFenceFixture(t)
			ctx := context.Background()
			if _, err := pool.Exec(ctx, `DELETE FROM recording_clips`); err != nil {
				t.Fatal(err)
			}
			if kind != "empty" {
				if _, err := pool.Exec(ctx, `INSERT INTO recording_clips(recording_id,size_bytes,display_path) VALUES(1,1,'test')`); err != nil {
					t.Fatal(err)
				}
			}
			switch kind {
			case "precreated":
				if _, err := pool.Exec(ctx, `CREATE INDEX idx_recording_clips_active_window ON recording_clips(recording_id,id,clip_start_at,clip_end_at,size_bytes,created_at) WHERE purged_at IS NULL AND released_at IS NULL`); err != nil {
					t.Fatal(err)
				}
			case "wrong-definition":
				if _, err := pool.Exec(ctx, `CREATE INDEX idx_recording_clips_active_window ON recording_clips(recording_id,id) WHERE purged_at IS NULL AND released_at IS NULL`); err != nil {
					t.Fatal(err)
				}
			}
			if kind == "invalid" {
				reader, e := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
				if e != nil {
					t.Fatal(e)
				}
				defer reader.Rollback(ctx)
				var n int
				if e = reader.QueryRow(ctx, "SELECT count(*) FROM recording_clips").Scan(&n); e != nil {
					t.Fatal(e)
				}
				writer, e := pool.Acquire(ctx)
				if e != nil {
					t.Fatal(e)
				}
				defer writer.Release()
				if _, e = writer.Exec(ctx, "SET statement_timeout='150ms'"); e != nil {
					t.Fatal(e)
				}
				_, e = writer.Exec(ctx, `CREATE INDEX CONCURRENTLY idx_recording_clips_active_window ON recording_clips(recording_id,id,clip_start_at,clip_end_at,size_bytes,created_at) WHERE purged_at IS NULL AND released_at IS NULL`)
				if e == nil {
					t.Fatal("old snapshot did not interrupt concurrent build")
				}
				reader.Rollback(ctx)
				if _, e = writer.Exec(ctx, "SET statement_timeout=0"); e != nil {
					t.Fatal(e)
				}
				var valid bool
				if e = pool.QueryRow(ctx, "SELECT indisvalid FROM pg_index WHERE indexrelid='idx_recording_clips_active_window'::regclass").Scan(&valid); e != nil || valid {
					t.Fatalf("invalid fixture not established: %v %v", valid, e)
				}
			}
			_, err := pool.Exec(ctx, string(body))
			if kind == "populated-missing" {
				if err == nil || !strings.Contains(err.Error(), "precreate") {
					t.Fatalf("populated startup must refuse missing precreation: %v", err)
				}
				var exists bool
				if e := pool.QueryRow(ctx, `SELECT to_regclass('idx_recording_clips_active_window') IS NOT NULL`).Scan(&exists); e != nil || exists {
					t.Fatal("refusal created an index", e)
				}
			} else if kind == "wrong-definition" || kind == "invalid" {
				if err == nil || !strings.Contains(err.Error(), "another definition") {
					t.Fatalf("mismatched index accepted: %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
		})
	}
}

// An uncommitted first writer makes an unlocked emptiness check lie. Wait for
// that writer before checking: a populated database must use reviewed CIC.
func TestAccountClipsWindowIndexConcurrentFirstWriter(t *testing.T) {
	pool, _ := feedFenceFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := pool.Exec(ctx, `DELETE FROM recording_clips`); err != nil {
		t.Fatal(err)
	}
	writer, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Rollback(context.Background())
	if _, err = writer.Exec(ctx, `INSERT INTO recording_clips(recording_id,size_bytes,display_path) VALUES(1,1,'concurrent-first')`); err != nil {
		t.Fatal(err)
	}
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	pid := conn.Conn().PgConn().PID()
	body, err := os.ReadFile("../../../infra/sql/migrations/0197_nas_delivery_window_index.sql")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, e := conn.Exec(ctx, string(body)); done <- e }()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		var waiting bool
		if err = pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_locks WHERE pid=$1 AND NOT granted AND relation='recording_clips'::regclass)`, pid).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		select {
		case e := <-done:
			t.Fatalf("migration finished before writer: %v", e)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-tick.C:
		}
	}
	if err = writer.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err = <-done:
		if err == nil || !strings.Contains(err.Error(), "precreate") {
			t.Fatalf("committed first row escaped precreation guard: %v", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	var exists bool
	if err = pool.QueryRow(ctx, `SELECT to_regclass('idx_recording_clips_active_window') IS NOT NULL`).Scan(&exists); err != nil || exists {
		t.Fatalf("guard created index over concurrent data: %v %v", exists, err)
	}
}
