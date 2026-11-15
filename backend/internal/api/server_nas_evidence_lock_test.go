package api

import (
	"errors"
	"strings"
	"testing"

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
