package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestNASEvidenceLockCopyForeignKey(t *testing.T) {
	for _, mode := range []struct {
		name, clause string
		blocked      bool
	}{
		{"legacy", " FOR UPDATE", true},
		{"evidence", nasConnectionEvidenceLock, false},
	} {
		t.Run(mode.name, func(t *testing.T) {
			f, item := copyTestFixture(t)
			if _, err := f.pool.Exec(f.ctx, `DELETE FROM nas_r2_copy_items`); err != nil {
				t.Fatal(err)
			}
			parent, err := f.pool.Begin(f.ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer parent.Rollback(f.ctx)
			if _, err = parent.Exec(f.ctx, `SELECT id FROM connections WHERE id=13`+mode.clause); err != nil {
				t.Fatal(err)
			}
			child, err := BeginNASR2CopyTx(f.ctx, f.pool)
			if err != nil {
				t.Fatal(err)
			}
			defer child.Rollback(f.ctx)
			if _, err = LoadNASR2CopySource(f.ctx, child, item); err != nil {
				t.Fatal(err)
			}
			_, err = child.Exec(f.ctx, `INSERT INTO nas_r2_copy_items(connection_id,kind,source_id,manifest_sha256,policy_sha256,item)
    VALUES(13,'raw',$1,repeat('a',64),repeat('b',64),'{}'::jsonb)`, item.SourceID)
			if mode.blocked {
				var pg *pgconn.PgError
				if !errors.As(err, &pg) || pg.Code != "55P03" || !strings.Contains(pg.Where, "connections") || !strings.Contains(pg.Where, "FOR KEY SHARE") {
					t.Fatalf("want actual FK lock timeout/context, got %v", err)
				}
				t.Log("legacy parent FOR UPDATE reproduced 55P03 at connections FK FOR KEY SHARE")
			} else if err != nil {
				t.Fatalf("metadata writer blocked the real copy FK: %v", err)
			} else if err = child.Commit(f.ctx); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestNASEvidenceLockPreservesExclusion(t *testing.T) {
	for _, other := range []struct{ name, sql string }{
		{"metadata_writer", `SELECT id FROM connections WHERE id=13` + nasConnectionEvidenceLock},
		{"cleanup_session", `SELECT id FROM connections WHERE id=13 FOR SHARE`},
		{"key_rotation", `SELECT id FROM connections WHERE id=13 FOR UPDATE`},
		{"delete_parent", `DELETE FROM connections WHERE id=13`},
	} {
		t.Run(other.name, func(t *testing.T) {
			f, _ := copyTestFixture(t)
			holder, err := f.pool.Begin(f.ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer holder.Rollback(f.ctx)
			if _, err = holder.Exec(f.ctx, `SELECT id FROM connections WHERE id=13`+nasConnectionEvidenceLock); err != nil {
				t.Fatal(err)
			}
			contender, err := BeginNASR2CopyTx(f.ctx, f.pool)
			if err != nil {
				t.Fatal(err)
			}
			defer contender.Rollback(f.ctx)
			_, err = contender.Exec(f.ctx, other.sql)
			var pg *pgconn.PgError
			if !errors.As(err, &pg) || pg.Code != "55P03" {
				t.Fatalf("required exclusion lost: %v", err)
			}
		})
	}
}

// Pause each real handler after its writes, with the declared connection unique
// indexes and triggers present. A parent UPDATE can otherwise silently strengthen
// an initially compatible lock and defeat the copy foreign-key check.
func TestNASEvidenceLockActualWriters(t *testing.T) {
	for _, writer := range []string{"heartbeat", "inventory", "release"} {
		t.Run(writer, func(t *testing.T) {
			pool, cleanup := testAccountClipsPool(t)
			defer cleanup()
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			exec := func(sql string, args ...any) {
				t.Helper()
				if _, err := pool.Exec(ctx, sql, args...); err != nil {
					t.Fatal(err)
				}
			}
			migration := func(name string) string {
				t.Helper()
				data, err := os.ReadFile("../../../infra/sql/migrations/" + name)
				if err != nil {
					t.Fatal(err)
				}
				return string(data)
			}
			initial := migration("0001_init.sql")
			exec(initial[strings.Index(initial, "CREATE OR REPLACE FUNCTION set_updated_at()"):strings.Index(initial, "CREATE TABLE IF NOT EXISTS streams")])
			connection := migration("0071_nas_pull_connections.sql")
			exec(connection[strings.Index(connection, "DROP TRIGGER IF EXISTS trg_connections_updated_at"):strings.Index(connection, "COMMIT;")])
			resilience := migration("0093_nas_pull_resilience.sql")
			exec(resilience[strings.Index(resilience, "CREATE UNIQUE INDEX IF NOT EXISTS idx_connections_one_nas_pull_per_account"):])
			listing := migration("0183_nas_listing_proof.sql")
			exec(listing[strings.Index(listing, "CREATE FUNCTION fence_nas_listing_evidence()"):strings.Index(listing, "CREATE TRIGGER nas_listing_evidence_fence")])
			a := strings.Index(listing, "CREATE TRIGGER nas_listing_cursor_fence")
			exec(listing[a : a+strings.Index(listing[a:], ";")+1])
			exec(migration("0193_nas_r2_copy.sql"))
			exec(`INSERT INTO connections(id,account_id,kind,api_key_id) VALUES(13,47,'nas_pull',5);
			 INSERT INTO recordings(id,account_id,name,delivery) VALUES(2,47,'writer-test','nas_pull');
			 INSERT INTO recording_clips(id,recording_id,size_bytes) VALUES(901,2,3);
			 CREATE FUNCTION require_unchanged_connection_keys() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
			 IF (NEW.id,NEW.api_key_id,NEW.account_id,NEW.kind) IS DISTINCT FROM (OLD.id,OLD.api_key_id,OLD.account_id,OLD.kind)
			 THEN RAISE EXCEPTION 'connection key changed'; END IF; RETURN NEW; END $$;
			 CREATE TRIGGER check_connection_keys BEFORE UPDATE ON connections FOR EACH ROW EXECUTE FUNCTION require_unchanged_connection_keys()`)
			gate := time.Now().UnixNano()
			controller, err := pool.Acquire(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer controller.Release()
			if _, err := controller.Exec(ctx, `SELECT pg_advisory_lock($1)`, gate); err != nil {
				t.Fatal(err)
			}
			defer controller.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, gate)
			exec(fmt.Sprintf(`CREATE FUNCTION pause_evidence_writer() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
			 PERFORM pg_advisory_xact_lock(%d); RETURN NEW; END $$`, gate))
			table := "connections"
			if writer == "release" {
				table = "recording_clips"
			}
			exec(`CREATE TRIGGER pause_writer AFTER UPDATE ON ` + table + ` FOR EACH ROW EXECUTE FUNCTION pause_evidence_writer()`)
			s := &Server{pool: pool}
			var body any = connectionHeartbeatRequest{CursorID: 12}
			handler := s.handleAccountConnectionHeartbeat
			switch writer {
			case "inventory":
				handler = s.handleAccountConnectionInventorySync
				now := time.Now().UTC()
				body = nasInventorySyncRequest{Generation: "live", DatasetSchema: "raw-dataset-v2", RootExclusions: []string{"#recycle"}, Files: []nasInventoryFileReport{{ClipID: 901, RecordingID: 2, RelativePath: "clips/test.mp4", SizeBytes: 3, SHA256: strings.Repeat("a", 64), State: "present", VerifiedAt: &now, ClientUpdatedAt: now}}}
			case "release":
				handler = s.handleAccountClipsReleaseBatch
				body = map[string]any{"clips": []map[string]int64{{"clip_id": 901, "recording_id": 2}}}
			}
			data, _ := json.Marshal(body)
			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(data))
			req = req.WithContext(context.WithValue(ctx, accountPrincipalContextKey, accountPrincipal{AccountID: 47, APIKeyID: ptrInt64(5)}))
			out := httptest.NewRecorder()
			done := make(chan struct{})
			go func() { defer close(done); handler(out, req) }()
			defer func() { cancel(); <-done }()
			for {
				var waiting bool
				if err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND NOT granted AND classid=$1::oid AND objid=$2::oid AND objsubid=1)`, uint32(uint64(gate)>>32), uint32(gate)).Scan(&waiting); err != nil {
					t.Fatal(err)
				}
				if waiting {
					break
				}
				select {
				case <-done:
					t.Fatalf("handler ended before its post-write gate: %d %s", out.Code, out.Body)
				case <-time.After(5 * time.Millisecond):
				}
			}
			child, err := BeginNASR2CopyTx(ctx, pool)
			if err != nil {
				t.Fatal(err)
			}
			defer child.Rollback(context.Background())
			if _, err = child.Exec(ctx, `INSERT INTO nas_r2_copy_items(connection_id,kind,source_id,manifest_sha256,policy_sha256,item) VALUES(13,'raw',902,repeat('a',64),repeat('b',64),'{}')`); err != nil {
				t.Fatalf("actual writer blocked FK after its writes: %v", err)
			}
			if err = child.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			if _, err = controller.Exec(ctx, `SELECT pg_advisory_unlock($1)`, gate); err != nil {
				t.Fatal(err)
			}
			<-done
			if out.Code != http.StatusOK {
				t.Fatalf("writer failed: %d %s", out.Code, out.Body)
			}
			var keys bool
			if err := pool.QueryRow(ctx, `SELECT (id,api_key_id,account_id,kind)=(13::bigint,5::bigint,47::bigint,'nas_pull') FROM connections WHERE id=13`).Scan(&keys); err != nil || !keys {
				t.Fatalf("connection keys changed: %v", err)
			}
		})
	}
}
