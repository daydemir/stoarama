package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// The cap is enforced by the import entry itself, before any hold lock.
func TestR2StitchImportBoundsHeldInputs(t *testing.T) {
	dsn := os.Getenv("STOARAMA_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	for inputs, admitted := range map[int]bool{1000: true, 1001: false} {
		set := DurhamStitchHoldSet{Bucket: "stoarama", EndpointSHA: strings.Repeat("a", 64)}
		for left := inputs; left > 0; left -= 200 {
			set.Hours = append(set.Hours, DurhamStitchHour{Inputs: make([]DurhamStitchInput, min(left, 200))})
		}
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		// An empty schema: a set within the cap fails later, at the queue control row.
		if _, err = tx.Exec(ctx, `SET LOCAL search_path TO pg_temp`); err != nil {
			t.Fatal(err)
		}
		_, err = ImportDurhamStitchHoldSet(ctx, tx, set)
		tx.Rollback(ctx)
		if err == nil || strings.Contains(err.Error(), "bounded") == admitted {
			t.Fatalf("import cap at %d inputs: %v", inputs, err)
		}
	}
}

// rawRetirementKey replays the deployed inverse reserve (or wire), which
// waits for its copy key and row, up to its ledger write: epoch, protection
// locks, copy key, clip row, then the held check.
func rawRetirementKey(ctx context.Context, pool *pgxpool.Pool, key string, wire bool) (pgx.Tx, error) {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, err
	}
	protection := `SELECT pg_advisory_xact_lock(7343226011103),pg_advisory_xact_lock(7343226011101)`
	if wire {
		protection = `SELECT pg_advisory_xact_lock_shared(7343226011103),pg_advisory_xact_lock_shared(7343226011101)`
	}
	var held bool
	for _, step := range []string{`SELECT epoch FROM r2_inverse_ke_epoch FOR SHARE`, `SELECT pg_advisory_xact_lock_shared(137,1)`, protection} {
		if _, err = tx.Exec(ctx, step); err != nil {
			tx.Rollback(context.Background())
			return nil, err
		}
	}
	if _, err = tx.Exec(ctx, `SELECT lock_nas_r2_copy_key('stoarama',$1)`, key); err == nil {
		err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM nas_stitch_r2_input_holds h WHERE h.bucket=c.bucket AND h.object_key=c.object_key AND h.released_at IS NULL)
 FROM recording_clips c WHERE c.bucket='stoarama' AND c.object_key=$1 FOR UPDATE OF c`, key).Scan(&held)
	}
	if err == nil && held {
		err = errors.New("live key protection veto")
	}
	if err == nil && !wire {
		_, err = tx.Exec(ctx, `INSERT INTO r2_retirement_reservations(kind,source_id,bucket,object_key,size_bytes,sha256,manifest_sha256)
 SELECT 'raw',id,bucket,object_key,size_bytes,lower(sha256),repeat('a',64) FROM recording_clips WHERE bucket='stoarama' AND object_key=$1`, key)
	}
	if err != nil {
		tx.Rollback(context.Background())
		return nil, err
	}
	return tx, nil
}

func backendPID(t *testing.T, tx pgx.Tx) uint32 {
	t.Helper()
	var pid uint32
	if err := tx.QueryRow(context.Background(), `SELECT pg_backend_pid()`).Scan(&pid); err != nil {
		t.Fatal(err)
	}
	return pid
}

// blockedBy waits until a backend waits on holder and returns its PID.
func blockedBy(t *testing.T, pool *pgxpool.Pool, holder uint32, what string) uint32 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(5 * time.Millisecond) {
		var waiter uint32
		err := pool.QueryRow(ctx, `SELECT pid FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid))`, holder).Scan(&waiter)
		if err == nil {
			return waiter
		}
		if !errors.Is(err, pgx.ErrNoRows) || time.Now().After(deadline) {
			t.Fatalf("%s never waited: %v", what, err)
		}
	}
}

func within[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(5 * time.Second):
		t.Fatalf("%s waited", what)
		panic("unreachable")
	}
}

// The receipt owns its copy row, so any busy lock the certificate needs
// refuses it at once and frees the copy row; the client retries.
func testStitchReceiptNeverWaits(t *testing.T, pool *pgxpool.Pool, post func() int, copyID, hourID int64, inputKey, secondKey string) {
	t.Helper()
	// Bounded: a starved pool or a stuck lock fails here instead of hanging CI.
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	for _, holder := range []struct {
		name, sql string
		args      []any
	}{
		{"input key", `SELECT lock_nas_r2_copy_key('stoarama',$1)`, []any{inputKey}},
		{"catalog epoch", `SELECT 1 FROM r2_inverse_ke_epoch FOR UPDATE`, nil},
		{"hour", `SELECT 1 FROM nas_stitch_r2_hours WHERE id=$1 FOR UPDATE`, []any{hourID}},
		{"input clip", `SELECT 1 FROM recording_clips WHERE object_key=$1 FOR UPDATE`, []any{inputKey}},
		{"input inventory", `SELECT 1 FROM nas_inventory_files f JOIN recording_clips c ON c.id=f.clip_id
 WHERE f.connection_id=13 AND c.object_key=$1 FOR UPDATE OF f`, []any{inputKey}},
		{"connection 13", `SELECT 1 FROM connections WHERE id=13 FOR UPDATE`, nil},
		{"second input hold", `SELECT 1 FROM nas_stitch_r2_input_holds WHERE object_key=$1 FOR UPDATE`, []any{secondKey}},
		// The receipt's own completion UPDATE needs ROW EXCLUSIVE on the copy
		// table, so a session holding it in SHARE mode must refuse the receipt
		// at once instead of queuing that UPDATE behind it with the row held.
		{"copy table", `LOCK TABLE nas_r2_copy_items IN SHARE MODE`, nil},
		// The certificate INSERT and its release trigger write these tables, so
		// the receipt takes their locks without waiting.
		{"certificate table", `LOCK TABLE nas_stitch_r2_certificates,nas_stitch_r2_hours,nas_stitch_r2_input_holds IN SHARE MODE`, nil},
	} {
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		if _, err = tx.Exec(ctx, holder.sql, holder.args...); err != nil {
			t.Fatal(err)
		}
		code := make(chan int, 1)
		began := time.Now()
		go func() { code <- post() }()
		// Request lock timeouts end a wait too; only an immediate refusal counts.
		if got := within(t, code, "receipt behind its "+holder.name); got != http.StatusConflict || time.Since(began) > time.Second {
			t.Fatalf("receipt waited on or crossed its busy %s: %d after %v", holder.name, got, time.Since(began))
		}
		copyRow, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = copyRow.Exec(ctx, `SELECT 1 FROM nas_r2_copy_items WHERE id=$1 FOR UPDATE NOWAIT`, copyID); err != nil {
			t.Fatalf("receipt kept the copy row behind its busy %s: %v", holder.name, err)
		}
		copyRow.Rollback(ctx)
		var verified bool
		var certificates, released int
		if err = pool.QueryRow(ctx, `SELECT (SELECT state='verified' FROM nas_r2_copy_items WHERE id=$1),
 (SELECT count(*) FROM nas_stitch_r2_certificates WHERE hour_id=$2),
 (SELECT count(*) FROM nas_stitch_r2_input_holds WHERE hour_id=$2 AND released_at IS NOT NULL)`, copyID, hourID).
			Scan(&verified, &certificates, &released); err != nil || verified || certificates != 0 || released != 0 {
			t.Fatalf("refused receipt behind its busy %s left durable state: verified %v certificates %d released %d %v",
				holder.name, verified, certificates, released, err)
		}
		tx.Rollback(ctx)
	}
}

type durhamExtraHours struct {
	keys      []string
	importSet func(context.Context) error
}

// addDurhamHours stores two further whole hours whose keys sort against their
// plan order, and returns their later planned import.
func addDurhamHours(t *testing.T, pool *pgxpool.Pool, hour time.Time, endpoint, sha string) durhamExtraHours {
	t.Helper()
	// Bounded: a starved pool or a stuck lock fails here instead of hanging CI.
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	var hours durhamExtraHours
	for n, start := range []time.Time{hour.Add(time.Hour), hour.Add(time.Hour + time.Minute), hour.Add(2 * time.Hour), hour.Add(2*time.Hour + time.Minute)} {
		id, key, path := 911+n, fmt.Sprintf("managed/durham-%c.mp4", 'z'-n), fmt.Sprintf("fixture/extra-%d.mp4", n)
		hours.keys = append(hours.keys, key)
		if _, err := pool.Exec(ctx, `INSERT INTO recording_clips(id,recording_id,storage_destination_id,endpoint,bucket,object_key,size_bytes,etag,sha256,
 fire_at,clip_start_at,clip_end_at,display_path) VALUES($1,30,1,$2,'stoarama',$3,100,'extra-etag',$4,$5,$5,$6,$7)`,
			id, endpoint, key, sha, start, start.Add(time.Minute), path); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `INSERT INTO nas_inventory_files(connection_id,clip_id,recording_id,relative_path,size_bytes,sha256,state,verified_at,client_updated_at)
 VALUES(13,$1,30,$2,100,$3,'present',now(),now())`, id, path, sha); err != nil {
			t.Fatal(err)
		}
	}
	hours.importSet = func(ctx context.Context) error {
		plan, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
		if err != nil {
			return err
		}
		defer plan.Rollback(context.Background())
		set, err := PlanDurhamStitchHoldSet(ctx, plan, []int64{1}, "stoarama", endpoint, 2)
		if err != nil || len(set.Hours) != 2 {
			return fmt.Errorf("extra Durham hours not planned: %w", err)
		}
		tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
		if err != nil {
			return err
		}
		defer tx.Rollback(context.Background())
		if _, err = ImportDurhamStitchHoldSet(ctx, tx, set); err != nil {
			return err
		}
		return tx.Commit(ctx)
	}
	return hours
}

// testStitchCertKeyContention holds a certificate open: it owns the epoch,
// the hour's input keys and their rows. Unrelated RAW keys and a multi-hour
// import proceed meanwhile; a deployed same-key reserve waits and then sees
// every input released.
func testStitchCertKeyContention(t *testing.T, pool *pgxpool.Pool, certify func(pgx.Tx) error, inputKey, freeKey string, extra durhamExtraHours) error {
	t.Helper()
	// Bounded: a starved pool or a stuck lock fails here instead of hanging CI.
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	// A running RAW batch owns the executors' outer lock throughout.
	defer holdSessionLock(t, pool, "7343226011173")()
	// Another RAW key and row stay locked across the whole certificate.
	other, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Rollback(ctx)
	if _, err = other.Exec(ctx, `SELECT lock_nas_r2_copy_key('stoarama',$1)`, freeKey); err != nil {
		t.Fatal(err)
	}
	if _, err = other.Exec(ctx, `SELECT 1 FROM recording_clips WHERE object_key=$1 FOR UPDATE`, freeKey); err != nil {
		t.Fatal(err)
	}
	cert, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		t.Fatal(err)
	}
	defer cert.Rollback(ctx)
	if err = certify(cert); err != nil {
		return err
	}
	if _, err = pool.Exec(ctx, `SELECT 1 FROM r2_inverse_ke_epoch FOR UPDATE NOWAIT`); err == nil || !strings.Contains(err.Error(), "could not obtain lock") {
		t.Fatalf("catalog epoch changed under an open certificate: %v", err)
	}

	for _, wire := range []bool{false, true} {
		unrelated := make(chan error, 1)
		go func() {
			tx, err := rawRetirementKey(ctx, pool, extra.keys[0], wire)
			if err == nil {
				tx.Rollback(ctx)
			}
			unrelated <- err
		}()
		if err = within(t, unrelated, "unrelated RAW key"); err != nil {
			t.Fatal(err)
		}
	}

	// The import's hold waits only for an in-flight same-key reservation.
	if _, err = pool.Exec(ctx, `UPDATE nas_stitch_r2_control SET enabled=false`); err != nil {
		t.Fatal(err)
	}
	race, err := rawRetirementKey(ctx, pool, extra.keys[1], false)
	if err != nil {
		t.Fatal(err)
	}
	// Registered before the race so a failed assertion below rolls the race
	// back instead of leaving its connection checked out for pool.Close. The
	// explicit defer runs when this helper returns, still before the parent
	// test's deferred DROP SCHEMA, which would otherwise wait on the race tx.
	var raced sync.Once
	releaseRace := func() { raced.Do(func() { _ = race.Rollback(context.Background()) }) }
	t.Cleanup(releaseRace)
	defer releaseRace()
	imported := make(chan error, 1)
	go func() { imported <- extra.importSet(ctx) }()
	blockedBy(t, pool, backendPID(t, race), "import behind a same-key reservation")
	releaseRace()
	if err = within(t, imported, "multi-hour import beside an open certificate"); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `UPDATE nas_stitch_r2_control SET enabled=true`); err != nil {
		t.Fatal(err)
	}

	type reservation struct {
		tx  pgx.Tx
		err error
	}
	same := make(chan reservation, 1)
	go func() {
		tx, err := rawRetirementKey(ctx, pool, inputKey, false)
		same <- reservation{tx, err}
	}()
	blockedBy(t, pool, backendPID(t, cert), "same-key reservation")
	if err = cert.Commit(ctx); err != nil {
		return err
	}
	reserved := within(t, same, "same-key reservation after the certificate")
	if reserved.err != nil {
		t.Fatalf("same-key reservation saw partly released coverage: %v", reserved.err)
	}
	reserved.tx.Rollback(ctx)
	return nil
}

func stitchStageGuard(objectID int64) string {
	return fmt.Sprintf("hashtextextended('%d',7343226011222)", objectID)
}

func holdSessionLock(t *testing.T, pool *pgxpool.Pool, key string) func() {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = conn.Exec(ctx, `SELECT pg_advisory_lock(`+key+`)`); err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			conn.Exec(context.Background(), `SELECT pg_advisory_unlock(`+key+`)`)
			conn.Release()
		})
	}
}
