package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	osexec "os/exec"
	"strings"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/db"
	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Stream 3 (Asia/Tokyo) keeps local [2026-09-16, 2026-10-06); 78 is Keep Everything.
const (
	dayFixtureFolder = "99991_asia_japan_tokyo_shinjuku9"
	dayFixtureKE     = "04_Asia_South_Korea_Seoul_Daechi_Station_Intersection"
)

const dayGoldenDigest = "9116c84bd7061b32fa4560b4d0eeafafb47928ed9482772d4ae6d6e2cca5feb3"

func dayPlazaDir(folder string, d time.Time) string {
	return fmt.Sprintf("%s/%s/%02d-%s", folder, d.Month(), d.Day(), d.Weekday())
}

func dayPlazaName(d time.Time, hhmmss string) string {
	return fmt.Sprintf("03_shinjuku_%04d_%s_W%d_%s_hour_%s.mp4", d.Year(), d.Month(), (d.Day()-1)/7+1, d.Weekday(), hhmmss)
}

func dayDate(s string) time.Time {
	d, _ := time.Parse(time.DateOnly, s)
	return d
}

func TestNASCleanupDayClientVersion(t *testing.T) {
	v := nasTunableCopyClientVersion
	if !nasCleanupSupportsExecution(v) || !nasCleanupSupportsBulk(v) || !nasCopySupportsVersion(v) || !SupportsNASStorageBudget(v) ||
		!nasLowMemoryRawCopyOnly(v) || nasClientSupportsStitchProof(v) || !nasFastCopyLine(v) || nasFastCopyLine(nasKeep20CleanupClientVersion) {
		t.Fatal("the day-cleanup release must keep the fast-copy line capabilities")
	}
	if !pullPathAllowed("POST", "/api/v1/account/connections/cleanup/day-permit") || !pullPathAllowed("POST", "/api/v1/account/connections/cleanup/day-receipt") {
		t.Fatal("NAS key cannot reach the day cleanup routes")
	}
}

func TestNASCleanupDayFolderMapMatchesClient(t *testing.T) {
	client, err := os.ReadFile("../../../clients/nas-pull/nas_cleanup_folder_streams.json")
	if err != nil || !bytes.Equal(client, cleanupFolderStreamsJSON) {
		t.Fatal("server folder map differs from the NAS client's pinned map")
	}
	if _, err := cleanupFolderStreams(); err != nil {
		t.Fatal(err)
	}
}

func TestNASCleanupDayPathDates(t *testing.T) {
	tokyo, _ := time.LoadLocation("Asia/Tokyo")
	day := dayDate("2026-09-12")
	dir := dayPlazaDir(dayFixtureFolder, day)
	for _, tc := range []struct {
		dir, name string
		want      string
	}{
		{dir, dayPlazaName(day, "103000"), "2026-09-12"},
		{dir, dayPlazaName(day, "01"), "2026-09-12"},
		{dir, dayPlazaName(dayDate("2020-09-12"), "103000"), "2020-09-12"}, // same month/day/weekday, other year
		{dir, strings.Replace(dayPlazaName(day, "103000"), "_W2_", "_W3_", 1), ""},
		{dir, strings.Replace(dayPlazaName(day, "103000"), "September", "October", 1), ""},
		{dir, dayPlazaName(day, "103000") + ".stoarama.json", ""},
		{dir, "notes.mp4", ""},
		// 2026-09-12T15:30:00Z is 2026-09-13 00:30 in Tokyo.
		{"recordings/366/continuous", "1789227000.mp4", "2026-09-13"},
		{"recordings/366/77", "1789227000000.mp4", "2026-09-13"},
		{"recordings/366/77", "1789227000.mp4", ""},
		{"recordings/366/continuous/x", "1789227000.mp4", ""},
	} {
		got, ok := cleanupDayFileDate(tc.dir, tc.name, tokyo)
		if (tc.want == "") == ok || (ok && got.Format(time.DateOnly) != tc.want) {
			t.Errorf("%s/%s: got %v %v, want %q", tc.dir, tc.name, got, ok, tc.want)
		}
	}
}

func TestNASCleanupDayWindowAndScope(t *testing.T) {
	for date, ok := range map[string]bool{
		"2026-09-13": true, "2026-09-14": true, "2026-09-15": false, // boundary before the window
		"2026-09-16": false, "2026-09-30": false, "2026-10-05": false, // inside
		"2026-10-06": false, "2026-10-07": true, "2026-11-20": true, // boundary after, then outside
	} {
		d := dayDate(date)
		_, _, _, err := cleanupDayStatic(nasCleanupDayUnit{RelativeDir: dayPlazaDir(dayFixtureFolder, d), LocalDate: date, StreamID: 3, Files: 1, Bytes: 1})
		if (err == nil) != ok {
			t.Errorf("%s: err=%v, want eligible=%v", date, err, ok)
		}
	}
	d := dayDate("2026-10-10")
	for name, u := range map[string]nasCleanupDayUnit{
		"keep everything": {RelativeDir: dayPlazaDir(dayFixtureKE, d), LocalDate: "2026-10-10", StreamID: 78},
		"wrong stream":    {RelativeDir: dayPlazaDir(dayFixtureKE, d), LocalDate: "2026-10-10", StreamID: 3},
		"date mismatch":   {RelativeDir: dayPlazaDir(dayFixtureFolder, d), LocalDate: "2026-10-17", StreamID: 3},
		"unmapped folder": {RelativeDir: dayPlazaDir("unreviewed", d), LocalDate: "2026-10-10", StreamID: 3},
		"undated layout":  {RelativeDir: dayFixtureFolder + "/misc", LocalDate: "2026-10-10", StreamID: 3},
		"joined root":     {RelativeDir: "joined/" + dayPlazaDir(dayFixtureFolder, d), LocalDate: "2026-10-10", StreamID: 3},
	} {
		if _, _, _, err := cleanupDayStatic(u); err == nil {
			t.Errorf("%s unit qualified", name)
		}
	}
	// Golden digest shared with prepare_keep28_plans.day_digest and the runner.
	units := []nasCleanupDayUnit{{RelativeDir: "a/September/12-Saturday", LocalDate: "2026-09-12", StreamID: 3, Files: 2, Bytes: 300}}
	if got := cleanupDayDigest(units); got != dayGoldenDigest {
		t.Fatalf("day digest %s", got)
	}
}

func dayTestPool(t *testing.T) *pgxpool.Pool {
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
	t.Cleanup(admin.Close)
	schema := pgx.Identifier{fmt.Sprintf("nas_day_%d", time.Now().UnixNano())}.Sanitize()
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { admin.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE") })
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err = db.MigrateUp(ctx, pool, findMigrationsDir(t)); err != nil {
		t.Fatal(err)
	}
	return pool
}

func dayCall(h func(http.ResponseWriter, *http.Request), id string, body any) *httptest.ResponseRecorder {
	method, payload := "GET", []byte(nil)
	if body != nil {
		method, payload = "POST", must(json.Marshal(body))
	}
	req := httptest.NewRequest(method, "/service", bytes.NewReader(payload))
	route := chi.NewRouteContext()
	route.URLParams.Add("id", id)
	w := httptest.NewRecorder()
	h(w, req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, route)))
	return w
}

func must(b []byte, err error) []byte {
	if err != nil {
		panic(err)
	}
	return b
}

var dayTokyo, _ = time.LoadLocation("Asia/Tokyo")

// dayFixture is a migrated schema with stream 3's NAS connection and an active
// keep20 epoch. clip catalogues one NAS file in the directory named for day,
// spanning [start, end).
func dayFixture(t *testing.T) (pool *pgxpool.Pool, exec func(string, ...any), clip func(id int64, day, hhmmss string, size int64, start, end time.Time)) {
	pool = dayTestPool(t)
	ctx := context.Background()
	exec = func(sql string, args ...any) {
		t.Helper()
		if _, err := pool.Exec(ctx, sql, args...); err != nil {
			t.Fatal(err)
		}
	}
	exec(`
INSERT INTO accounts(id,email) VALUES(47,'nas-day@example.invalid');
INSERT INTO account_api_keys(id,account_id,key_prefix,secret_hash) VALUES(47,47,'fixture-key','fixture-key');
INSERT INTO connections(id,account_id,kind,api_key_id) VALUES(13,47,'nas_pull',47);
INSERT INTO streams(id,provider,external_id,name,slug,source_url,recording_state,local_timezone,capture_type,source_family,execution_class,capture_family,expected_fps)
 VALUES(3,'test','3','test','test','https://storage.example.test','off','Asia/Tokyo','hls','video_manifest','video_live','continuous_video',30);
INSERT INTO storage_destinations(id,account_id,name,endpoint,region,bucket,access_key_id,secret_access_key_enc)
 VALUES(1,47,'test','https://storage.example.test','auto','fixture','fixture',decode('00','hex'));
INSERT INTO recordings(id,account_id,storage_destination_id,name,stream_url,cron_expr,cron_timezone,status,stream_id,paused_at)
 VALUES(30,47,1,'test','https://storage.example.test','* * * * *','Asia/Tokyo','paused',3,now())`)
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = ActivateNASDeliveryFence(ctx, tx); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	exec(`SELECT activate_nas_media_keep20($1,'')`, cleanupPolicySHA)
	clip = func(id int64, day, hhmmss string, size int64, start, end time.Time) {
		t.Helper()
		d := dayDate(day)
		p := dayPlazaDir(dayFixtureFolder, d) + "/" + dayPlazaName(d, hhmmss)
		exec(`INSERT INTO recording_clips(id,recording_id,storage_destination_id,endpoint,bucket,object_key,size_bytes,etag,sha256,
 fire_at,clip_start_at,clip_end_at,display_path,purged_at) VALUES($1,30,1,'https://storage.example.test','fixture',$2,$3,'etag',repeat('a',64),$4,$4,$5,$6,now())`,
			id, fmt.Sprintf("obj-%d", id), size, start, end, p)
		exec(`INSERT INTO nas_inventory_files(connection_id,clip_id,recording_id,relative_path,size_bytes,sha256,state,client_updated_at)
 VALUES(13,$1,30,$2,$3,repeat('a',64),'present',now())`, id, p, size)
	}
	return pool, exec, clip
}

func dayAt(day, hhmmss string) time.Time {
	at, _ := time.ParseInLocation("2006-01-02 150405", day+" "+hhmmss, dayTokyo)
	return at
}

// Owner rule (Mooney/Deniz: "the nas keeps all keep 20 footage"): a bulk
// unit is a whole stream-day lying entirely outside the pinned window with a
// full guard day each side; boundary days stay per-file.
func TestNASCleanupDayNeverBulkDeletesBoundaryDay(t *testing.T) {
	policy, err := cleanupEmbeddedPolicy()
	if err != nil {
		t.Fatal(err)
	}
	for id, w := range policy.Streams {
		if w.KeepEverything {
			continue
		}
		loc, err := time.LoadLocation(w.Timezone)
		if err != nil {
			t.Fatal(err)
		}
		first := w.start.In(loc)
		day := time.Date(first.Year(), first.Month(), first.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, -5)
		for ; !day.After(w.end.AddDate(0, 0, 5)); day = day.AddDate(0, 0, 1) {
			local := time.Date(day.Year(), day.Month(), day.Day(), 0, 0, 0, 0, loc)
			// The local day widened by a guard day each side must miss [start, end).
			clear := !local.AddDate(0, 0, 2).After(w.start) || !local.AddDate(0, 0, -1).Before(w.end)
			if cleanupDayOutside(w, day) && !clear {
				t.Errorf("stream %d: %s eligible within a day of its window", id, day.Format(time.DateOnly))
			}
		}
	}

	pool, _, clip := dayFixture(t)
	ctx := context.Background()
	unit := func(day string) nasCleanupDayUnit {
		return nasCleanupDayUnit{RelativeDir: dayPlazaDir(dayFixtureFolder, dayDate(day)), LocalDate: day, StreamID: 3, Files: 1, Bytes: 1}
	}
	// Both edges of stream 3's local window [09-16, 10-06): the guard days
	// are refused even with only outside-looking catalogued clips.
	clip(1, "2026-09-15", "000500", 100, dayAt("2026-09-15", "000500"), dayAt("2026-09-15", "000600"))
	clip(2, "2026-10-06", "120000", 100, dayAt("2026-10-06", "120000"), dayAt("2026-10-06", "120100"))
	for _, day := range []string{"2026-09-15", "2026-10-06"} {
		if _, _, err := cleanupDayCheck(ctx, pool, unit(day)); err == nil || !strings.Contains(err.Error(), "adjacent") {
			t.Errorf("boundary day %s: %v", day, err)
		}
	}
	// Day 2026-09-14 passes the static rule; one catalogued clip running into
	// the window refuses the whole directory-day.
	clip(3, "2026-09-14", "103000", 100, dayAt("2026-09-14", "103000"), dayAt("2026-09-14", "103100"))
	if files, _, err := cleanupDayCheck(ctx, pool, unit("2026-09-14")); err != nil || files != 1 {
		t.Fatalf("outside day: %d files %v", files, err)
	}
	clip(4, "2026-09-14", "235000", 100, dayAt("2026-09-14", "235000"), dayAt("2026-09-16", "000100"))
	if _, _, err := cleanupDayCheck(ctx, pool, unit("2026-09-14")); err == nil || !strings.Contains(err.Error(), "intersects the pinned keep window") {
		t.Fatalf("clip reaching into the window did not refuse its day: %v", err)
	}
	// A clip named for eligible day 2026-09-13 but catalogued days later,
	// inside the window, is still checked and refuses its day.
	clip(5, "2026-09-13", "103000", 100, dayAt("2026-09-25", "103000"), dayAt("2026-09-25", "103100"))
	if _, _, err := cleanupDayCheck(ctx, pool, unit("2026-09-13")); err == nil || !strings.Contains(err.Error(), "intersects the pinned keep window") {
		t.Fatalf("clip whose name and time disagree escaped the window check: %v", err)
	}
}

// Another stream's catalogued clip in the directory refuses every unit there,
// whatever its date: its own window and Keep Everything are never assumed.
func TestNASCleanupDayRefusesAnotherStreamsClip(t *testing.T) {
	pool, exec, clip := dayFixture(t)
	ctx := context.Background()
	unit := nasCleanupDayUnit{RelativeDir: dayPlazaDir(dayFixtureFolder, dayDate("2026-09-12")), LocalDate: "2026-09-12", StreamID: 3, Files: 1, Bytes: 1}
	clip(1, "2026-09-12", "103000", 100, dayAt("2026-09-12", "103000"), dayAt("2026-09-12", "103100"))
	if files, _, err := cleanupDayCheck(ctx, pool, unit); err != nil || files != 1 {
		t.Fatalf("own-stream directory-day: %d files %v", files, err)
	}
	exec(`INSERT INTO streams(id,provider,external_id,name,slug,source_url,recording_state,local_timezone,capture_type,source_family,execution_class,capture_family,expected_fps)
 VALUES(78,'test','78','ke','ke','https://storage.example.test','off','Asia/Tokyo','hls','video_manifest','video_live','continuous_video',30);
INSERT INTO recordings(id,account_id,storage_destination_id,name,stream_url,cron_expr,cron_timezone,status,stream_id,paused_at)
 VALUES(31,47,1,'ke','https://storage.example.test','* * * * *','Asia/Tokyo','paused',78,now())`)
	// Same date and name shape as stream 3's own clip: only the stream differs.
	d := dayDate("2026-09-12")
	at := dayAt("2026-09-12", "113000")
	foreign := func(id int64, dir string) {
		t.Helper()
		exec(`INSERT INTO recording_clips(id,recording_id,storage_destination_id,endpoint,bucket,object_key,size_bytes,etag,sha256,
 fire_at,clip_start_at,clip_end_at,display_path,purged_at) VALUES($1,31,1,'https://storage.example.test','fixture',$2,100,'etag',repeat('a',64),$3,$3,$4,$5,now())`,
			id, fmt.Sprintf("obj-%d", id), at, at.Add(time.Minute), dir+"/"+dayPlazaName(d, "113000"))
	}
	// A LIKE wildcard must not reach a lookalike directory ('_' is literal).
	foreign(3, strings.Replace(unit.RelativeDir, "_", "x", 1))
	if files, _, err := cleanupDayCheck(ctx, pool, unit); err != nil || files != 1 {
		t.Fatalf("lookalike directory leaked into the unit: %d files %v", files, err)
	}
	foreign(2, unit.RelativeDir)
	if _, _, err := cleanupDayCheck(ctx, pool, unit); err == nil || !strings.Contains(err.Error(), "another stream") {
		t.Fatalf("another stream's clip did not refuse the directory-day: %v", err)
	}
	// The directory read is an index range scan on 0248's pattern index.
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `SET LOCAL enable_seqscan=off`); err != nil {
		t.Fatal(err)
	}
	rows, err := tx.Query(ctx, "EXPLAIN "+cleanupDayUnitSQL, unit.RelativeDir, cleanupDayDirPattern.Replace(unit.RelativeDir)+"/%")
	if err != nil {
		t.Fatal(err)
	}
	plan, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil || !strings.Contains(strings.Join(plan, "\n"), "recording_clips_display_path_pattern") {
		t.Fatalf("directory read does not use the pattern index (%v):\n%s", err, strings.Join(plan, "\n"))
	}
}

func TestNASCleanupDayPermitsDirectoriesAndAccountsReceipts(t *testing.T) {
	pool, exec, insert := dayFixture(t)
	ctx := context.Background()
	clip := func(id int64, day, hhmmss string, size int64) {
		t.Helper()
		at := dayAt(day, hhmmss)
		insert(id, day, hhmmss, size, at, at.Add(time.Minute))
	}
	clip(1, "2026-09-12", "103000", 100)
	clip(2, "2026-09-12", "113000", 200)
	clip(3, "2026-09-15", "103000", 300) // boundary day
	clip(4, "2026-09-20", "103000", 400) // inside the window
	clip(5, "2026-10-10", "103000", 500)
	unit := func(day string, files, bytes int64) nasCleanupDayUnit {
		return nasCleanupDayUnit{RelativeDir: dayPlazaDir(dayFixtureFolder, dayDate(day)), LocalDate: day, StreamID: 3, Files: files, Bytes: bytes}
	}
	if files, size, err := cleanupDayCheck(ctx, pool, unit("2026-09-12", 0, 0)); err != nil || files != 2 || size != 300 {
		t.Fatalf("outside directory-day: %d files %d bytes %v", files, size, err)
	}
	for _, day := range []string{"2026-09-15", "2026-09-20"} {
		if _, _, err := cleanupDayCheck(ctx, pool, unit(day, 1, 1)); err == nil {
			t.Fatalf("%s qualified", day)
		}
	}
	exec(`BEGIN; SET LOCAL session_replication_role=replica; INSERT INTO delivery_nas_source_holds(clip_id) VALUES(2); COMMIT`)
	if _, _, err := cleanupDayCheck(ctx, pool, unit("2026-09-12", 2, 300)); err == nil || !strings.Contains(err.Error(), "held") {
		t.Fatalf("held file did not refuse its directory: %v", err)
	}
	exec(`BEGIN; SET LOCAL session_replication_role=replica; DELETE FROM delivery_nas_source_holds; COMMIT`)

	units := []nasCleanupDayUnit{unit("2026-09-12", 2, 300), unit("2026-10-10", 1, 500)}
	// The preparer's shipping query and unit rule select exactly these units.
	python := os.Getenv("STOARAMA_TEST_PYTHON")
	if python == "" {
		python = "python3"
	}
	script := `import json, sys
sys.path.insert(0, "../../scripts/nas-cleanup")
import prepare_keep28_plans as k
w = k.load_policy(k.POLICY_PATH)[3]
if sys.argv[1] == "sql":
    k.query = lambda sql, psql: print(sql)
    k.day_stream_rows(w, "psql")
else:
    print(json.dumps(k.day_units(w, k.load_folders(), json.load(sys.stdin))))`
	sql, err := osexec.Command(python, "-B", "-c", script, "sql").Output()
	if err != nil {
		t.Fatal(err)
	}
	var facts []byte
	if err := pool.QueryRow(ctx, string(sql)).Scan(&facts); err != nil {
		t.Fatal(err)
	}
	prepare := osexec.Command(python, "-B", "-c", script, "units")
	prepare.Stdin = bytes.NewReader(facts)
	prepared, err := prepare.Output()
	if err != nil {
		t.Fatal(err)
	}
	var preparedUnits []nasCleanupDayUnit
	if err := json.Unmarshal(prepared, &preparedUnits); err != nil || fmt.Sprint(preparedUnits) != fmt.Sprint(units) {
		t.Fatalf("preparer units %s differ from %v (%v)", prepared, units, err)
	}
	// The runner's independent catalog query runs read-only on the real schema.
	runnerCatalog := func() string {
		t.Helper()
		plan, _ := json.Marshal(map[string]any{"units": []map[string]any{
			{"relative_dir": units[0].RelativeDir, "local_date": units[0].LocalDate, "stream_id": 3},
			{"relative_dir": unit("2026-09-20", 1, 1).RelativeDir, "local_date": "2026-09-20", "stream_id": 3}}})
		sql, err := osexec.Command(python, "-B", "-c", `import json, sys
from pathlib import Path
sys.path.insert(0, "../../../ops/jelly/cleanup-runner")
import runner as r
job = r.Runner(Path("."), Path("."))
job.windows = {3: (r.timestamp("2026-09-15T15:00:00+00:00"), r.timestamp("2026-10-05T15:00:00+00:00"))}
job.timezones = {3: "Asia/Tokyo"}
job.query = lambda sql, step=None: print(sql)
job.day_catalog(json.loads(sys.argv[1]))`, string(plan)).Output()
		if err != nil {
			t.Fatal(err)
		}
		readTx, err := pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
		if err != nil {
			t.Fatal(err)
		}
		defer readTx.Rollback(ctx)
		var raw []byte
		if err := readTx.QueryRow(ctx, string(sql)).Scan(&raw); err != nil {
			t.Fatal(err)
		}
		return string(raw)
	}
	if got := runnerCatalog(); !strings.Contains(got, `"local_date" : "2026-09-12", "held" : 0, "overlap" : 0, "dir_held" : false`) ||
		!strings.Contains(got, `"local_date" : "2026-09-20", "held" : 0, "overlap" : 1`) {
		t.Fatalf("runner catalog: %s", got)
	}
	exec(`BEGIN; SET LOCAL session_replication_role=replica; INSERT INTO delivery_nas_source_holds(clip_id) VALUES(2); COMMIT`)
	if got := runnerCatalog(); !strings.Contains(got, `"local_date" : "2026-09-12", "held" : 1`) {
		t.Fatalf("runner catalog missed a held clip: %s", got)
	}
	exec(`BEGIN; SET LOCAL session_replication_role=replica; DELETE FROM delivery_nas_source_holds; COMMIT`)
	s := &Server{pool: pool, cfg: config.Config{NASCleanupDeletionEnabled: true}}
	sha := cleanupDayDigest(units)
	create := map[string]any{"label": "kday-s3-0001", "policy_sha256": cleanupPolicySHA, "plan_sha256": sha, "files": 3, "bytes": 800, "units": units}
	if w := dayCall(s.handleServiceNASCleanupDayCreate, "", map[string]any{"label": "kday-s3-bad", "policy_sha256": cleanupPolicySHA,
		"plan_sha256": cleanupDayDigest([]nasCleanupDayUnit{unit("2026-09-15", 1, 300)}), "files": 1, "bytes": 300,
		"units": []nasCleanupDayUnit{unit("2026-09-15", 1, 300)}}); w.Code != 409 {
		t.Fatalf("boundary plan admitted: %d %s", w.Code, w.Body)
	}
	w := dayCall(s.handleServiceNASCleanupDayCreate, "", create)
	if w.Code != 200 {
		t.Fatalf("admission: %d %s", w.Code, w.Body)
	}
	var admitted struct{ ID int64 }
	json.Unmarshal(w.Body.Bytes(), &admitted)
	planID := fmt.Sprint(admitted.ID)
	if w := dayCall(s.handleServiceNASCleanupDayCreate, "", map[string]any{"label": "kday-s3-0002", "policy_sha256": cleanupPolicySHA,
		"plan_sha256": cleanupDayDigest(units[:1]), "files": 2, "bytes": 300, "units": units[:1]}); w.Code != 409 {
		t.Fatalf("live directory-day planned twice: %d", w.Code)
	}
	exec(`UPDATE connections SET client_version=$1,client_boot_id='fixture-boot',last_seen_at=now() WHERE id=13`, nasTunableCopyClientVersion)
	permit := func(version string) (int, *nasCleanupDayPermit) {
		t.Helper()
		w := cleanupCall(t, s.handleAccountNASCleanupDayPermit, 47, 47, "", "", map[string]any{"client_version": version}, "fixture-boot "+version)
		var out struct {
			Unit *nasCleanupDayPermit `json:"unit"`
		}
		json.Unmarshal(w.Body.Bytes(), &out)
		return w.Code, out.Unit
	}
	if code, p := permit(nasTunableCopyClientVersion); code != 200 || p != nil {
		t.Fatalf("dry-run plan issued a permit: %d %+v", code, p)
	}
	if w := dayCall(s.handleServiceNASCleanupDayControl, planID, map[string]any{"plan_sha256": sha, "mode": "enabled", "reason": "fixture", "approval_hours": 1}); w.Code != 200 {
		t.Fatalf("enable: %d %s", w.Code, w.Body)
	}
	for _, older := range []string{nasFastCopyClientVersion, nasKeep20CleanupClientVersion} {
		exec(`UPDATE connections SET client_version=$1 WHERE id=13`, older)
		if code, p := permit(older); code != 409 || p != nil {
			t.Fatalf("%s received a day permit: %d", older, code)
		}
	}
	exec(`UPDATE connections SET client_version=$1 WHERE id=13`, nasTunableCopyClientVersion)
	receipt := func(p *nasCleanupDayPermit, outcome nasCleanupDayOutcome, media, size, remaining int64) *httptest.ResponseRecorder {
		body := map[string]any{"client_version": nasTunableCopyClientVersion, "unit_id": p.UnitID, "attempt": p.Attempt,
			"permit_token": p.Token, "outcome": outcome, "media_files": media, "media_bytes": size, "sidecar_files": media,
			"sidecar_bytes": media * 10, "remaining_files": remaining, "refusal": ""}
		if outcome == nasCleanupDayRefused {
			body["refusal"] = "client guard"
		}
		return cleanupCall(t, s.handleAccountNASCleanupDayReceipt, 47, 47, "", "", body, "fixture-boot "+nasTunableCopyClientVersion)
	}
	code, first := permit(nasTunableCopyClientVersion)
	if code != 200 || first == nil || first.LocalDate != "2026-09-12" || first.Files != 2 || first.Timezone != "Asia/Tokyo" || first.Attempt != 1 {
		t.Fatalf("first permit: %d %+v", code, first)
	}
	if w := receipt(first, nasCleanupDayPartial, 1, 100, 1); w.Code != 200 {
		t.Fatalf("partial receipt: %d %s", w.Code, w.Body)
	}
	if w := receipt(first, nasCleanupDayPartial, 1, 100, 1); w.Code != 200 {
		t.Fatalf("identical receipt retry: %d", w.Code)
	}
	if w := receipt(first, nasCleanupDayDeleted, 2, 300, 0); w.Code != 409 {
		t.Fatalf("contradictory receipt accepted: %d", w.Code)
	}
	_, again := permit(nasTunableCopyClientVersion)
	if again == nil || again.UnitID != first.UnitID || again.Attempt != 2 {
		t.Fatalf("partial unit was not re-permitted: %+v", again)
	}
	if w := receipt(again, nasCleanupDayDeleted, 1, 200, 0); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	_, second := permit(nasTunableCopyClientVersion)
	if second == nil || second.LocalDate != "2026-10-10" {
		t.Fatalf("second unit: %+v", second)
	}
	if w := receipt(second, nasCleanupDayRefused, 0, 0, 0); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	if code, p := permit(nasTunableCopyClientVersion); code != 200 || p != nil {
		t.Fatalf("finished plan issued a permit: %+v", p)
	}
	w = dayCall(s.handleServiceNASCleanupDayStatus, planID, nil)
	var status struct {
		Files, DeletedMediaFiles, DeletedMediaBytes int64
		Units                                       []struct{ State string }
	}
	if json.Unmarshal(w.Body.Bytes(), &status) != nil {
		t.Fatal(w.Body.String())
	}
	var raw map[string]any
	json.Unmarshal(w.Body.Bytes(), &raw)
	if raw["deleted_media_files"] != float64(2) || raw["deleted_media_bytes"] != float64(300) ||
		len(status.Units) != 2 || status.Units[0].State != "deleted" || status.Units[1].State != "client_refused" {
		t.Fatalf("receipt accounting: %s", w.Body)
	}
	if _, err := pool.Exec(ctx, `UPDATE nas_cleanup_day_units SET receipts='[]' WHERE id=$1`, first.UnitID); err == nil {
		t.Fatal("receipts are not append-only")
	}
	if w := dayCall(s.handleServiceNASCleanupDayClose, planID, map[string]any{"plan_sha256": sha, "reason": "fixture"}); w.Code != 409 {
		t.Fatal("enabled plan closed without a pause")
	}
	dayCall(s.handleServiceNASCleanupDayControl, planID, map[string]any{"plan_sha256": sha, "mode": "paused", "reason": "fixture done"})
	if w := dayCall(s.handleServiceNASCleanupDayClose, planID, map[string]any{"plan_sha256": sha, "reason": "fixture"}); w.Code != 200 {
		t.Fatalf("close: %d %s", w.Code, w.Body)
	}
}

// A flat directory holds every day of its recording. Only the clips named for
// the unit's date are held-checked, so another day's hold never refuses it,
// while every clip of the directory still gets the stream and name checks.
func TestNASCleanupDayFlatDirectoryHoldsOnlyItsNamedDay(t *testing.T) {
	pool, exec, _ := dayFixture(t)
	ctx := context.Background()
	dir := dayFixtureFolder + "/30/continuous"
	flat := func(id int64, day, hhmmss string) {
		t.Helper()
		at := dayAt(day, hhmmss)
		p := fmt.Sprintf("%s/%d.mp4", dir, at.Unix())
		exec(`INSERT INTO recording_clips(id,recording_id,storage_destination_id,endpoint,bucket,object_key,size_bytes,etag,sha256,
 fire_at,clip_start_at,clip_end_at,display_path,purged_at) VALUES($1,30,1,'https://storage.example.test','fixture',$2,100,'etag',repeat('a',64),$3,$3,$4,$5,now())`,
			id, fmt.Sprintf("obj-%d", id), at, at.Add(time.Minute), p)
		exec(`INSERT INTO nas_inventory_files(connection_id,clip_id,recording_id,relative_path,size_bytes,sha256,state,client_updated_at)
 VALUES(13,$1,30,$2,100,repeat('a',64),'present',now())`, id, p)
	}
	hold := func(id int64) {
		exec(fmt.Sprintf(`BEGIN; SET LOCAL session_replication_role=replica; INSERT INTO delivery_nas_source_holds(clip_id) VALUES(%d); COMMIT`, id))
	}
	flat(1, "2026-09-12", "103000")
	flat(2, "2026-09-12", "235900")
	flat(3, "2026-09-10", "103000")
	unit := nasCleanupDayUnit{RelativeDir: dir, LocalDate: "2026-09-12", StreamID: 3, Files: 2, Bytes: 200}
	hold(3)
	if files, size, err := cleanupDayCheck(ctx, pool, unit); err != nil || files != 2 || size != 200 {
		t.Fatalf("another day's hold refused the flat directory-day: %d files %d bytes %v", files, size, err)
	}
	if _, _, err := cleanupDayCheck(ctx, pool, nasCleanupDayUnit{RelativeDir: dir, LocalDate: "2026-09-10", StreamID: 3, Files: 1, Bytes: 100}); err == nil || !strings.Contains(err.Error(), "held") {
		t.Fatalf("held clip did not refuse its own day: %v", err)
	}
	hold(2)
	if _, _, err := cleanupDayCheck(ctx, pool, unit); err == nil || !strings.Contains(err.Error(), "held") {
		t.Fatalf("held clip named for the day did not refuse it: %v", err)
	}
	// An undated name anywhere in the directory still refuses every unit there.
	exec(`BEGIN; SET LOCAL session_replication_role=replica; DELETE FROM delivery_nas_source_holds; COMMIT`)
	exec(`UPDATE recording_clips SET display_path=$1 WHERE id=3`, dir+"/renamed.mp4")
	if _, _, err := cleanupDayCheck(ctx, pool, unit); err == nil || !strings.Contains(err.Error(), "does not name a local date") {
		t.Fatalf("undated name in the flat directory escaped: %v", err)
	}
}
