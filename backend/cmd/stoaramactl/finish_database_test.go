package main

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"strings"
	"syscall"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestFinishDatabaseFailureDoesNotExposeDriverSecrets(t *testing.T) {
	for _, test := range []struct {
		err   error
		cause finishDBCause
		state string
	}{
		{&pgconn.PgError{Code: "57014", Message: "canceling statement due to statement timeout", Detail: "postgres://user:SECRET@host/db"}, finishDBStatementTimeout, "57014"},
		{&pgconn.PgError{Code: "57014", Message: "canceling statement due to user request"}, finishDBQueryCanceled, "57014"},
		{&pgconn.PgError{Code: "42P01", Message: "SECRET"}, finishDBSQL, "42P01"},
		{context.DeadlineExceeded, finishDBDeadline, ""},
		{context.Canceled, finishDBCanceled, ""},
		{&net.DNSError{Err: "SECRET", Name: "SECRET"}, finishDBDNS, ""},
		{&net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}, finishDBRefused, ""},
		{net.ErrClosed, finishDBClosed, ""},
	} {
		err := finishDBError(finishCandidateQuery, finishExactInventory, test.err)
		var failure *finishDatabaseFailure
		var stop finishStop
		if !errors.Is(err, test.err) || !errors.As(err, &stop) || stop != finishDatabase || !errors.As(err, &failure) {
			t.Fatalf("error chain lost: %v", err)
		}
		b, _ := json.Marshal(failure)
		if failure.Cause != test.cause || failure.SQLState != test.state || failure.Tier != finishExactInventory || strings.Contains(string(b)+err.Error(), "SECRET") {
			t.Fatalf("unsafe/wrong diagnostic: %s %v", b, err)
		}
	}
}

func TestFinishDatabaseStatusPreservesStatementAndTier(t *testing.T) {
	pool, safety, copy := finishFixture(t, 1)
	if _, err := pool.Exec(context.Background(), `DROP TABLE nas_inventory_files`); err != nil {
		t.Fatal(err)
	}
	status, err := finishTick(context.Background(), pool, safety, copy)
	if !errors.Is(err, finishDatabase) || status.Stop != finishDatabase || status.ImportedItems != 0 || status.CleanupShouldRun {
		t.Fatalf("unsafe failure: %+v %v", status, err)
	}
	f := status.DatabaseFailure
	if f == nil || f.Statement != finishCandidateQuery || f.Tier != finishExactInventory || f.SQLState != "42P01" {
		t.Fatalf("lost candidate diagnostic: %+v", f)
	}
	path := t.TempDir() + "/status.json"
	if err = finishWriteStatus(path, status); err != nil {
		t.Fatal(err)
	}
	var read finishStatus
	b, _ := os.ReadFile(path)
	if err = json.Unmarshal(b, &read); err != nil || read.DatabaseFailure == nil || read.DatabaseFailure.SQLState != "42P01" {
		t.Fatalf("lost durable failure: %s %v", b, err)
	}
}

func TestFinishCandidateReservationLookupRetainsOrdinalVeto(t *testing.T) {
	pool, _, _ := finishFixture(t, 7)
	ctx := context.Background()
	// Scope and plan mode do not dilute reservations. A retired plan releases
	// only explicit never-issued rows; a missing item and duplicate issued ordinal hold.
	_, err := pool.Exec(ctx, `INSERT INTO nas_cleanup_plans(id,mode,account_id,connection_id,manifest) VALUES
 (101,'paused',48,14,'[{"kind":"raw","source_id":1}]'),
 (102,'retired',47,13,'[{"kind":"raw","source_id":2},{"kind":"raw","source_id":3},{"kind":"raw","source_id":4},{"kind":"raw","source_id":5},{"kind":"raw","source_id":5}]');
 INSERT INTO nas_cleanup_items(plan_id,ordinal,delete_ever_issued) VALUES(102,1,true),(102,2,false),(102,4,false),(102,5,true);
 INSERT INTO nas_inventory_files SELECT 13,id,display_path,'present',sha256,size_bytes,now() FROM recording_clips;`)
	if err != nil {
		t.Fatal(err)
	}
	// Limit must be applied after the reservation veto, not before it.
	items, tier, err := finishCandidates(ctx, pool, 2)
	if err != nil || tier != finishExactInventory || len(items) != 2 || items[0].SourceID != 3 || items[1].SourceID != 6 {
		t.Fatalf("reservation/limit changed: %v %v %v", tier, items, err)
	}
	data, _ := finishPolicyJSON()
	var explain []byte
	if err = pool.QueryRow(ctx, `EXPLAIN (FORMAT JSON) `+finishCandidateSQL(`n.clip_id IS NOT NULL`), data, int64(100000000000), 2).Scan(&explain); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(explain), `"CTE Name": "eligible"`) || !strings.Contains(string(explain), "nas_cleanup_file_reserved") || strings.Contains(string(explain), "jsonb_array_elements") {
		t.Fatalf("unbounded reservation expansion: %s", explain)
	}
}

func TestFinishRefreshAndImportDatabaseDiagnostics(t *testing.T) {
	t.Run("refresh_control_query", func(t *testing.T) {
		pool, safety, copy := finishFixture(t, 1)
		if _, err := pool.Exec(context.Background(), `DROP TABLE r2_keep28_refresh_control`); err != nil {
			t.Fatal(err)
		}
		status, err := finishTickWithRefresh(context.Background(), pool, safety, copy, finishRefreshOptions{Picks: "not-read-before-control"})
		f := status.DatabaseFailure
		if !errors.Is(err, finishDatabase) || f == nil || f.Statement != finishRefreshControl || f.SQLState != "42P01" || status.ImportedItems != 0 {
			t.Fatalf("refresh error hidden: %+v %v", status, err)
		}
	})
	t.Run("import_insert", func(t *testing.T) {
		pool, safety, copy := finishFixture(t, 1)
		if _, err := pool.Exec(context.Background(), `CREATE FUNCTION fail_import() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'secret driver detail'; END $$; CREATE TRIGGER fail_import BEFORE INSERT ON nas_r2_copy_items FOR EACH ROW EXECUTE FUNCTION fail_import()`); err != nil {
			t.Fatal(err)
		}
		status, err := finishTick(context.Background(), pool, safety, copy)
		f := status.DatabaseFailure
		if !errors.Is(err, finishImport) || status.Stop != finishImport || f == nil || f.Statement != finishImportInsert || f.SQLState != "P0001" || f.Tier != finishLateUnknown || status.ImportedItems != 0 {
			t.Fatalf("import error hidden: %+v %v", status, err)
		}
		if strings.Contains(f.Error(), "secret") {
			t.Fatal("raw driver text escaped")
		}
	})
	validation := errors.New("identity mismatch")
	if got := finishKnownDBError(finishImportSource, validation); got != validation {
		t.Fatal("validation became a database failure")
	}
}
