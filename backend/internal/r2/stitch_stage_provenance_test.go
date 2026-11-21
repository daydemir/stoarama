package r2

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/db"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const stageFixtureEndpoint = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"

// A stage absence is only publishable by the invocation that saw its own
// answered DELETE and its own empty-inventory strict readback. This witness
// pins that against real PostgreSQL schemas.
//
// The "held" schema carries every shape a deployed pre-0230 binary could have
// left behind: a bare intent, 0217's bare `absence_observed_at`, and 0219's
// strict stamp recorded by a *later* reconciliation pass. Each stays durable,
// preserved, un-upgradable and held by dependency admission, fence removal, the
// certified NAS cleanup gate and a new 0220 owner keep hold.
//
// The "wire" schema carries only the atomic three-column stamp the current
// command writes, and every one of those same guards proceeds.
func TestR2StitchStageProvenanceSeparatesLegacyFromWireEvidence(t *testing.T) {
	held := newStageFixtureBase(t, "held")
	wire := newStageFixtureBase(t, "wire")
	held.seedHour(t, 500)
	wire.seedHour(t, 500)

	bare := held.stage(t, 601)
	legacy404 := held.stage(t, 602)
	legacyStrict := held.stage(t, 603)
	// Every intent is created bare first, as the command creates it. The legacy
	// shapes are then stamped the only way a pre-0230 binary could have written
	// them: its own function ran under a weaker guard, so the fixture installs
	// them with that guard disabled and nothing else changed. This is fixture
	// setup for rows that already exist in production, never a command path.
	held.insert(`INSERT INTO nas_stitch_r2_stage_retirements(object_id) VALUES(601),(602),(603)`)
	// 0217: a bare 404, with no strict confirmation at all.
	held.stampLegacy(t, 602, `clock_timestamp()`, `NULL`)
	// 0219: a strict HEAD, but recorded by a later reconciliation pass.
	held.stampLegacy(t, 603, `clock_timestamp()`, `clock_timestamp()`)
	wire.stage(t, 601)
	wire.insert(`INSERT INTO nas_stitch_r2_stage_retirements(object_id) VALUES(601)`)
	// 0230: one stamp in all three columns, written by the invocation that saw
	// its own answered DELETE and its own strict readback. The current guard
	// admits exactly this transition, so no privilege is needed here.
	wire.insert(`WITH at AS MATERIALIZED (SELECT clock_timestamp() AS value)
 UPDATE nas_stitch_r2_stage_retirements SET absence_observed_at=at.value,strict_absence_observed_at=at.value,
  wire_absence_observed_at=at.value FROM at WHERE object_id=601`)

	for _, c := range []struct {
		fixture  *stageFixture
		id       int64
		resolved bool
	}{
		{held, 601, false}, {held, 602, false}, {held, 603, false}, {wire, 601, true},
	} {
		var resolved bool
		c.fixture.row(`SELECT nas_stitch_r2_stage_retirement_resolved(absence_observed_at,strict_absence_observed_at,
 wire_absence_observed_at) FROM nas_stitch_r2_stage_retirements WHERE object_id=$1`, &resolved, c.id)
		if resolved != c.resolved {
			t.Fatalf("%s intent %d resolved=%v, want %v", c.fixture.name, c.id, resolved, c.resolved)
		}
	}
	// A missing retirement row is unresolved, never accidentally resolved.
	for _, f := range []*stageFixture{held, wire} {
		var orphan bool
		f.row(`SELECT nas_stitch_r2_stage_retirement_resolved(NULL,NULL,NULL)`, &orphan)
		if orphan {
			t.Fatalf("%s: a missing retirement row read as resolved", f.name)
		}
	}

	// No statement may upgrade a bare or legacy intent into the wire provenance,
	// however the timestamps are spelled.
	for _, id := range []int64{601, 602, 603} {
		for _, upgrade := range []string{
			`SET wire_absence_observed_at=clock_timestamp()`,
			`SET strict_absence_observed_at=clock_timestamp(),wire_absence_observed_at=clock_timestamp()`,
			`SET wire_absence_observed_at=absence_observed_at,strict_absence_observed_at=absence_observed_at`,
		} {
			err := held.exec(`UPDATE nas_stitch_r2_stage_retirements `+upgrade+` WHERE object_id=$1`, id)
			if err == nil || !strings.Contains(err.Error(), "one-shot") {
				t.Fatalf("intent %d upgraded by %q: %v", id, upgrade, err)
			}
		}
	}
	// The one allowed transition is the atomic three-column stamp on a row that
	// had none; it is what the command writes and what makes 0230's proof one
	// indivisible value.
	if err := held.exec(`WITH at AS MATERIALIZED (SELECT clock_timestamp() AS value)
 UPDATE nas_stitch_r2_stage_retirements SET absence_observed_at=at.value,strict_absence_observed_at=at.value,
  wire_absence_observed_at=at.value FROM at WHERE object_id=601`); err != nil {
		t.Fatalf("the atomic wire stamp was refused on a bare intent: %v", err)
	}

	// Dependency admission holds every unresolved key and admits the resolved
	// one. The same guard protects derived collation and raw paths, so one real
	// delivery hold per key is the representative witness.
	type dependency struct {
		fixture *stageFixture
		intent  stageIntent
		// resolved is this intent's state after the atomic stamp above.
		resolved bool
	}
	for _, c := range []dependency{
		{held, bare, true}, // 601 received the allowed atomic stamp
		{held, legacy404, false},
		{held, legacyStrict, false},
		{wire, wire.stageObject(t, 601), true},
	} {
		err := c.fixture.exec(`INSERT INTO delivery_keep_holds(bucket,object_key,etag,size_bytes,kind)
 VALUES('stoarama',$1,'stage-etag',100,'metadata')`, c.intent.key)
		heldNow := err != nil && strings.Contains(err.Error(), "protects dependency admission")
		if heldNow == c.resolved {
			t.Fatalf("%s key %s dependency guard held=%v, want held=%v: %v",
				c.fixture.name, c.intent.key, heldNow, !c.resolved, err)
		}
	}

	// Fence removal waits for every held intent; the wire-resolved schema has
	// nothing left to hold it.
	held.insert(`INSERT INTO r2_retirement_writer_fences(bucket,endpoint_sha256,drain_not_before)
 VALUES('stoarama',$1,clock_timestamp()-interval '1 second')`, stageFixtureEndpoint)
	if err := held.exec(`DELETE FROM r2_retirement_writer_fences`); err == nil ||
		!strings.Contains(err.Error(), "have not drained") {
		t.Fatalf("a held stage intent allowed fence removal: %v", err)
	}
	wire.insert(`INSERT INTO r2_retirement_writer_fences(bucket,endpoint_sha256,drain_not_before)
 VALUES('stoarama',$1,clock_timestamp()-interval '1 second')`, stageFixtureEndpoint)
	if err := wire.exec(`DELETE FROM r2_retirement_writer_fences`); err != nil {
		t.Fatalf("wire-resolved stage intents still blocked fence removal: %v", err)
	}

	// The certified NAS cleanup gate accepts exactly the wire proof.
	held.seedCleanupCertificate(t, 500)
	wire.seedCleanupCertificate(t, 500)
	if held.cleanupAbsent() {
		t.Fatal("legacy absence evidence cleared the certified NAS cleanup gate")
	}
	if !wire.cleanupAbsent() {
		t.Fatal("the wire absence proof did not clear the certified NAS cleanup gate")
	}

	// A new 0220 owner keep hold over a held prefix is refused; over the wire
	// prefix it lands. That is the admission the retirement's shared
	// 7343226011220 lock serializes with.
	if err := held.exec(`INSERT INTO r2_owner_keep_prefixes(bucket,prefix,decision)
 VALUES('stoarama','stitch-staging/','test hold')`); err == nil ||
		!strings.Contains(err.Error(), "overlaps a pending R2 retirement") {
		t.Fatalf("a legacy-held stage intent admitted an owner keep hold: %v", err)
	}
	if err := wire.exec(`INSERT INTO r2_owner_keep_prefixes(bucket,prefix,decision)
 VALUES('stoarama','stitch-staging/','test hold')`); err != nil {
		t.Fatalf("the wire proof still blocked an owner keep prefix: %v", err)
	}
}

// stampLegacy writes a pre-0230 absence stamp. The old binaries wrote these
// under their own weaker guard, so the fixture reproduces an existing row
// exactly by disabling only that trigger for the statement; every other guard
// the row then meets stays enabled.
func (f *stageFixture) stampLegacy(t *testing.T, id int64, absence, strict string) {
	t.Helper()
	if err := f.toggleGuard(false); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := f.toggleGuard(true); err != nil {
			t.Fatal(err)
		}
	}()
	if err := f.exec(`UPDATE nas_stitch_r2_stage_retirements SET absence_observed_at=`+absence+
		`,strict_absence_observed_at=`+strict+` WHERE object_id=$1`, id); err != nil {
		t.Fatalf("legacy stamp for %d: %v", id, err)
	}
}

func (f *stageFixture) toggleGuard(enable bool) error {
	verb := "DISABLE"
	if enable {
		verb = "ENABLE"
	}
	return f.exec(`ALTER TABLE nas_stitch_r2_stage_retirements ` + verb + ` TRIGGER nas_stitch_r2_stage_retirement_guard`)
}

// stageIntent is one fixture stage object and its exact generated key.
type stageIntent struct {
	id  int64
	key string
}

// This witness drives the retirement's real protection order concurrently with
// a new owner-prefix hold, and proves the two properties that order exists for.
//
// A retirement-shaped transaction takes shared 7343226011103/7343226011101, the
// same shared pair a live cleanup admission would hold. While it does, a new
// 0220 owner hold — which takes all three locks exclusively — must wait on it,
// proven from pg_blocking_pids rather than inferred. So a hold cannot be
// admitted inside the retirement's provenance-clearance-to-DELETE window. Once
// the retirement finishes, the admission proceeds and its own guard sees the
// pending intent and refuses the hold.
//
// The inner shared 1220 request is deliberately taken by the retirement's own
// intent insert, before the session asks for it itself. Taking it earlier would
// put a shared request of this session's own ahead of that insert behind a
// pending exclusive admission, and the insert — the one thing the admission
// must observe — could never commit. This witness therefore also asserts the
// insert's 1220 shared request is the session's *first* one.
func TestR2StitchStageRetirementSerializesWithOwnerKeepAdmission(t *testing.T) {
	f := newStageFixtureBase(t, "serialize")
	f.seedHour(t, 500)
	f.stage(t, 601)

	// The retirement's protection order, up to the intent insert.
	retirement, err := f.pool.Begin(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer retirement.Rollback(context.Background())
	for _, sql := range []string{
		`SELECT pg_advisory_xact_lock_shared(7343226011187)`,
		`SELECT epoch FROM r2_inverse_ke_epoch FOR SHARE`,
		`SELECT pg_advisory_xact_lock_shared(7343226011103)`,
		`SELECT pg_advisory_xact_lock_shared(7343226011101)`,
	} {
		if _, err := retirement.Exec(f.ctx, sql); err != nil {
			t.Fatal(err)
		}
	}
	retirementPID := stageBackendPID(t, retirement)
	// Its intent insert is what takes 1220 shared inside 0230's own guards; the
	// session must not have asked for 1220 before it.
	var alreadyHolds1220 bool
	if err := retirement.QueryRow(f.ctx, `SELECT EXISTS(SELECT 1 FROM pg_locks
 WHERE locktype='advisory' AND pid=pg_backend_pid() AND objsubid=1 AND classid=1709 AND objid=3126902356)`).
		Scan(&alreadyHolds1220); err != nil {
		t.Fatal(err)
	}
	if alreadyHolds1220 {
		t.Fatal("the retirement requested 1220 before its intent insert")
	}

	// A new owner hold takes all three protection locks exclusively. It must
	// wait on the retirement's shared pair.
	admission, err := f.pool.Begin(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer admission.Rollback(context.Background())
	if _, err := admission.Exec(f.ctx, `SELECT set_config('lock_timeout','5s',true)`); err != nil {
		t.Fatal(err)
	}
	admitted := make(chan error, 1)
	admissionPID := stageBackendPID(t, admission)
	go func() {
		_, err := admission.Exec(context.Background(), `SELECT pg_advisory_xact_lock(7343226011103)`)
		admitted <- err
	}()
	if waiter := stageBlockedBy(t, f.pool, retirementPID); waiter != admissionPID {
		t.Fatalf("owner admission %d did not wait on the retirement %d (waiter %d)",
			admissionPID, retirementPID, waiter)
	}

	// The retirement now advertises its own held intent and finishes its window.
	// The key is frozen by the hold the admission has not landed yet, so the
	// retirement's read still sees a clean stage; releasing protection then lets
	// one admission through.
	if _, err := retirement.Exec(f.ctx, `INSERT INTO nas_stitch_r2_stage_retirements(object_id) VALUES(601)`); err != nil {
		t.Fatal(err)
	}
	if err := retirement.Commit(f.ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-admitted; err != nil {
		t.Fatalf("owner admission did not proceed after protection cleared: %v", err)
	}
	// The admission now holds all three exclusively and runs 0220's own overlap
	// check against the durable intent, which is unresolved: it must refuse.
	if _, err := admission.Exec(f.ctx, `SELECT pg_advisory_xact_lock(7343226011101),
  pg_advisory_xact_lock(7343226011220)`); err != nil {
		t.Fatal(err)
	}
	_, err = admission.Exec(f.ctx, `INSERT INTO r2_owner_keep_prefixes(bucket,prefix,decision)
 VALUES('stoarama','stitch-staging/','admitted after the retirement')`)
	if err == nil || !strings.Contains(err.Error(), "overlaps a pending R2 retirement") {
		t.Fatalf("a hold landed over the retirement's held intent: %v", err)
	}
}

func stageBackendPID(t *testing.T, tx pgx.Tx) uint32 {
	t.Helper()
	var pid uint32
	if err := tx.QueryRow(context.Background(), `SELECT pg_backend_pid()`).Scan(&pid); err != nil {
		t.Fatal(err)
	}
	return pid
}

// stageBlockedBy returns the pid waiting on holder, proving the wait is real.
func stageBlockedBy(t *testing.T, pool *pgxpool.Pool, holder uint32) uint32 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	deadline := time.Now().Add(10 * time.Second)
	for {
		var waiter uint32
		err := pool.QueryRow(ctx, `SELECT pid FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid))`, holder).Scan(&waiter)
		if err == nil {
			return waiter
		}
		if time.Now().After(deadline) {
			t.Fatalf("no backend waited on %d: %v", holder, err)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

type stageFixture struct {
	t    *testing.T
	ctx  context.Context
	pool *pgxpool.Pool
	name string
}

func (f *stageFixture) begin() pgx.Tx {
	f.t.Helper()
	tx, err := f.pool.Begin(f.ctx)
	if err != nil {
		f.t.Fatal(err)
	}
	return tx
}

func (f *stageFixture) insert(sql string, args ...any) {
	f.t.Helper()
	tx := f.begin()
	defer tx.Rollback(context.Background())
	if _, err := tx.Exec(f.ctx, `SET LOCAL session_replication_role=replica`); err != nil {
		f.t.Fatal(err)
	}
	if _, err := tx.Exec(f.ctx, sql, args...); err != nil {
		f.t.Fatalf("%s: %v: %v", f.name, err, sql)
	}
	if err := tx.Commit(f.ctx); err != nil {
		f.t.Fatal(err)
	}
}

func (f *stageFixture) exec(sql string, args ...any) error {
	f.t.Helper()
	tx := f.begin()
	defer tx.Rollback(context.Background())
	if _, err := tx.Exec(f.ctx, sql, args...); err != nil {
		return err
	}
	return tx.Commit(f.ctx)
}

func (f *stageFixture) row(sql string, dest any, args ...any) {
	f.t.Helper()
	if err := f.pool.QueryRow(f.ctx, sql, args...).Scan(dest); err != nil {
		f.t.Fatalf("%s: %v: %v", f.name, err, sql)
	}
}

// seedHour installs the one certified hour and its scope that the stage and
// cleanup guards join through.
func (f *stageFixture) seedHour(t *testing.T, hourID int64) {
	t.Helper()
	hour := time.Now().UTC().Truncate(time.Hour)
	// pgx uses the extended protocol for a parameterized statement, which
	// PostgreSQL refuses for a multi-statement body. The hour here is computed
	// by this fixture, never input, so it is inlined as a quoted literal.
	stamp := `'` + hour.Format("2006-01-02T15:04:05Z07:00") + `'`
	f.insert(`
INSERT INTO accounts(id,email) VALUES(47,'stage-provenance@example.invalid');
INSERT INTO account_api_keys(id,account_id,key_prefix,secret_hash) VALUES(47,47,'fixture-key','fixture-key');
INSERT INTO connections(id,account_id,kind,api_key_id) VALUES(13,47,'nas_pull',47);
INSERT INTO streams(id,provider,external_id,name,slug,source_url,recording_state,local_timezone,capture_type,source_family,execution_class,capture_family,expected_fps)
 VALUES(3,'test','3','test','test','https://storage.example.test','off','Asia/Tokyo','hls','video_manifest','video_live','continuous_video',30);
INSERT INTO storage_destinations(id,account_id,name,endpoint,region,bucket,access_key_id,secret_access_key_enc)
 VALUES(1,47,'test','https://storage.example.test','auto','stoarama','fixture',decode('00','hex'));
INSERT INTO recordings(id,account_id,storage_destination_id,name,stream_url,cron_expr,cron_timezone,status,stream_id,paused_at)
 VALUES(30,47,1,'test','https://storage.example.test','* * * * *','Asia/Tokyo','paused',3,now());
INSERT INTO nas_stitch_scopes(id,stream_id,start_at,end_at,reason,approved_until)
 VALUES(1,3,` + stamp + `,` + stamp + `::timestamptz+interval '6 hours','stage provenance',clock_timestamp()+interval '1 hour')`)
	f.insert(`
INSERT INTO nas_stitch_r2_hours(id,scope_id,recording_id,hour_at,plan,plan_sha256,input_bytes,
 state,attempt,lease_token,staged_manifest,staged_manifest_sha256,lease_toolchain_attempt,lease_toolchain_sha256)
 OVERRIDING SYSTEM VALUE VALUES($1,1,30,$2,'{"policy":"v85r","clips":[]}'::jsonb,repeat('a',64),1,
 'staged',2,repeat('e',64),'{"policy":"v85r","clips":[],"outputs":[]}'::jsonb,repeat('b',64),2,repeat('c',64))`, hourID, hour)
}

// seedCleanupCertificate installs the certificate the NAS cleanup gate reads
// for seedHour's hour.
func (f *stageFixture) seedCleanupCertificate(t *testing.T, hourID int64) {
	t.Helper()
	f.insert(`INSERT INTO nas_stitch_r2_certificates(hour_id,connection_id,attempt,plan_sha256,manifest_sha256,receipt)
 VALUES($1,13,2,repeat('a',64),repeat('b',64),'{}'::jsonb)`, hourID)
}

// stage installs one staged object with its own SHA, so the dependency guard's
// SHA/size branch cannot tie one fixture key to another's.
func (f *stageFixture) stage(t *testing.T, id int64) stageIntent {
	t.Helper()
	sha := fmt.Sprintf("%064d", id)
	f.insert(`INSERT INTO nas_stitch_r2_stage_objects(id,hour_id,attempt,api_key_id,bucket,endpoint_sha256,generation,
 relative_path,sha256,size_bytes,state,upload_id,etag,last_modified,grants_drain_at)
 OVERRIDING SYSTEM VALUE VALUES($1,500,2,47,'stoarama',$2,gen_random_uuid(),$3,$4,100,'staged','up','stage-etag',
 clock_timestamp()-interval '1 hour',clock_timestamp()-interval '1 hour')`,
		id, stageFixtureEndpoint, fmt.Sprintf("fixture/output-%d.mp4", id), sha)
	return f.stageObject(t, id)
}

func (f *stageFixture) stageObject(t *testing.T, id int64) stageIntent {
	t.Helper()
	var key string
	f.row(`SELECT object_key FROM nas_stitch_r2_stage_objects WHERE id=$1`, &key, id)
	return stageIntent{id: id, key: key}
}

func (f *stageFixture) cleanupAbsent() bool {
	f.t.Helper()
	certificate := fmt.Sprintf(`{"hour_id":500,"attempt":2,"plan_sha256":%q,"manifest_sha256":%q}`,
		strings.Repeat("a", 64), strings.Repeat("b", 64))
	var absent bool
	f.row(`SELECT nas_cleanup_certified_stitch_staging_absent($1::jsonb)`, &absent, certificate)
	return absent
}

func newStageFixtureBase(t *testing.T, name string) *stageFixture {
	return newStageFixtureAt(t, name, "")
}

// newStageFixtureAt migrates a disposable schema through the migration files
// named below cutoff, exactly as a deployed schema sits before that migration.
// An empty cutoff applies every file. The files themselves are copied into a
// temporary directory, so the real migration bodies run unmodified and the
// repository is never touched.
func newStageFixtureAt(t *testing.T, name, cutoff string) *stageFixture {
	t.Helper()
	dsn := strings.TrimSpace(os.Getenv("STOARAMA_TEST_DATABASE_URL"))
	if dsn == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("stage_provenance_%s_%d", name, time.Now().UnixNano())
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		admin.Close()
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	cfg.MaxConns = 8
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		admin.Close()
		t.Fatal(err)
	}
	source, err := filepath.Abs(filepath.Join("..", "..", "..", "infra", "sql", "migrations"))
	if err != nil {
		t.Fatal(err)
	}
	dir := source
	if cutoff != "" {
		dir = t.TempDir()
		entries, err := os.ReadDir(source)
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			// Schema fixtures apply migrations in name order, so a lexical bound
			// selects exactly the deployed prefix.
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") || entry.Name() >= cutoff {
				continue
			}
			body, err := os.ReadFile(filepath.Join(source, entry.Name()))
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, entry.Name()), body, 0600); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := db.MigrateUp(ctx, pool, dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		_, _ = admin.Exec(context.Background(), "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
		admin.Close()
	})
	return &stageFixture{t: t, ctx: ctx, pool: pool, name: name}
}
