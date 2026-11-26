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

// A Keep Everything unit's names follow its unit line (preparer and runner pin the same).
const dayGoldenKEDigest = "e49626b3909930c3726bae74feccaae438249f9a49dc43c9ada3a2877e8e98f9"

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
	for _, v := range nasCleanupDayVersions {
		if !nasCleanupSupportsExecution(v) || !nasCleanupSupportsBulk(v) || !nasCopySupportsVersion(v) || !SupportsNASStorageBudget(v) ||
			!nasLowMemoryRawCopyOnly(v) || nasClientSupportsStitchProof(v) || !nasFastCopyLine(v) || nasFastCopyLine(nasKeep20CleanupClientVersion) {
			t.Fatalf("day-cleanup release %s must keep the fast-copy line capabilities", v)
		}
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
	ke := []nasCleanupDayUnit{{RelativeDir: "a/September/12-Saturday", LocalDate: "2026-09-12", StreamID: 78, Files: 2, Bytes: 300,
		Names: []nasCleanupDayName{{Name: "a.mp4", SizeBytes: 100}, {Name: "b.mp4", SizeBytes: 200}}}}
	if got := cleanupDayDigest(ke); got != dayGoldenKEDigest {
		t.Fatalf("Keep Everything day digest %s", got)
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
		if _, _, err := dayCheck(ctx, pool, unit(day)); err == nil || !strings.Contains(err.Error(), "adjacent") {
			t.Errorf("boundary day %s: %v", day, err)
		}
	}
	// Day 2026-09-14 passes the static rule; one catalogued clip running into
	// the window refuses the whole directory-day.
	clip(3, "2026-09-14", "103000", 100, dayAt("2026-09-14", "103000"), dayAt("2026-09-14", "103100"))
	if files, _, err := dayCheck(ctx, pool, unit("2026-09-14")); err != nil || files != 1 {
		t.Fatalf("outside day: %d files %v", files, err)
	}
	clip(4, "2026-09-14", "235000", 100, dayAt("2026-09-14", "235000"), dayAt("2026-09-16", "000100"))
	if _, _, err := dayCheck(ctx, pool, unit("2026-09-14")); err == nil || !strings.Contains(err.Error(), "intersects the pinned keep window") {
		t.Fatalf("clip reaching into the window did not refuse its day: %v", err)
	}
	// A clip named for eligible day 2026-09-13 but catalogued days later,
	// inside the window, is still checked and refuses its day.
	clip(5, "2026-09-13", "103000", 100, dayAt("2026-09-25", "103000"), dayAt("2026-09-25", "103100"))
	if _, _, err := dayCheck(ctx, pool, unit("2026-09-13")); err == nil || !strings.Contains(err.Error(), "intersects the pinned keep window") {
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
	if files, _, err := dayCheck(ctx, pool, unit); err != nil || files != 1 {
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
	if files, _, err := dayCheck(ctx, pool, unit); err != nil || files != 1 {
		t.Fatalf("lookalike directory leaked into the unit: %d files %v", files, err)
	}
	foreign(2, unit.RelativeDir)
	if _, _, err := dayCheck(ctx, pool, unit); err == nil || !strings.Contains(err.Error(), "another stream") {
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
	clip(dayIncidentClip, "2026-10-12", "103000", 600) // a live NAS cleanup incident hold
	unit := func(day string, files, bytes int64) nasCleanupDayUnit {
		return nasCleanupDayUnit{RelativeDir: dayPlazaDir(dayFixtureFolder, dayDate(day)), LocalDate: day, StreamID: 3, Files: files, Bytes: bytes}
	}
	if files, size, err := dayCheck(ctx, pool, unit("2026-09-12", 0, 0)); err != nil || files != 2 || size != 300 {
		t.Fatalf("outside directory-day: %d files %d bytes %v", files, size, err)
	}
	for _, day := range []string{"2026-09-15", "2026-09-20"} {
		if _, _, err := dayCheck(ctx, pool, unit(day, 1, 1)); err == nil {
			t.Fatalf("%s qualified", day)
		}
	}
	if _, _, err := dayCheck(ctx, pool, unit("2026-10-12", 1, 600)); err == nil || !strings.Contains(err.Error(), "held") {
		t.Fatalf("incident-held clip did not refuse its directory: %v", err)
	}
	// Outside Keep Everything an incident or delivery hold protects a directory-day;
	// the R2 retirement and per-file plan holds below do not, and stay for the rest
	// of the test, through the preparer, runner, admission and permits.
	dayDeliveryHoldsKeep(t, ctx, exec, pool, 2, unit("2026-09-12", 0, 0))
	dayOtherHolds(t, exec, 2, unit("2026-09-12", 0, 0).RelativeDir)
	if files, size, err := dayCheck(ctx, pool, unit("2026-09-12", 0, 0)); err != nil || files != 2 || size != 300 {
		t.Fatalf("non-incident holds kept a non-KE directory-day: %d files %d bytes %v", files, size, err)
	}

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
    print(json.dumps(k.day_units(w, k.load_folders(), json.load(sys.stdin))[0]))`
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
			{"relative_dir": unit("2026-09-20", 1, 1).RelativeDir, "local_date": "2026-09-20", "stream_id": 3},
			{"relative_dir": unit("2026-10-12", 1, 1).RelativeDir, "local_date": "2026-10-12", "stream_id": 3}}})
		sql, err := osexec.Command(python, "-B", "-c", `import json, sys
from pathlib import Path
sys.path.insert(0, "../../../ops/jelly/cleanup-runner")
import runner as r
job = r.Runner(Path("../../.."), Path("."))
job.windows = {3: (r.timestamp("2026-09-15T15:00:00+00:00"), r.timestamp("2026-10-05T15:00:00+00:00"))}
job.timezones = {3: "Asia/Tokyo"}
job.keep_everything = set()
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
	if got := runnerCatalog(); !strings.Contains(got, `"local_date" : "2026-09-12", "held" : 0, "overlap" : 0, "dir_held" : false, "unlive" : 0`) ||
		!strings.Contains(got, `"local_date" : "2026-09-20", "held" : 0, "overlap" : 1`) ||
		!strings.Contains(got, `"local_date" : "2026-10-12", "held" : 1`) {
		t.Fatalf("runner catalog: %s", got)
	}
	s := &Server{pool: pool, cfg: config.Config{NASCleanupDeletionEnabled: true, R2Bucket: "fixture"}}
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
	for _, version := range nasCleanupDayVersions {
		exec(`UPDATE connections SET client_version=$1 WHERE id=13`, version)
		if code, p := permit(version); code != 200 || p != nil {
			t.Fatalf("dry-run plan issued %s a permit: %d %+v", version, code, p)
		}
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
	exec(`UPDATE connections SET client_version=$1 WHERE id=13`, nasTunableDayClientVersion)
	receipt := func(p *nasCleanupDayPermit, outcome nasCleanupDayOutcome, media, size, remaining int64) *httptest.ResponseRecorder {
		body := map[string]any{"client_version": nasTunableDayClientVersion, "unit_id": p.UnitID, "attempt": p.Attempt,
			"permit_token": p.Token, "outcome": outcome, "media_files": media, "media_bytes": size, "sidecar_files": media,
			"sidecar_bytes": media * 10, "remaining_files": remaining, "refusal": ""}
		if outcome == nasCleanupDayRefused {
			body["refusal"] = "client guard"
		}
		return cleanupCall(t, s.handleAccountNASCleanupDayReceipt, 47, 47, "", "", body, "fixture-boot "+nasTunableDayClientVersion)
	}
	code, first := permit(nasTunableDayClientVersion)
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
	_, again := permit(nasTunableDayClientVersion)
	if again == nil || again.UnitID != first.UnitID || again.Attempt != 2 {
		t.Fatalf("partial unit was not re-permitted: %+v", again)
	}
	if w := receipt(again, nasCleanupDayDeleted, 1, 200, 0); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	_, second := permit(nasTunableDayClientVersion)
	if second == nil || second.LocalDate != "2026-10-10" {
		t.Fatalf("second unit: %+v", second)
	}
	if w := receipt(second, nasCleanupDayRefused, 0, 0, 0); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	if code, p := permit(nasTunableDayClientVersion); code != 200 || p != nil {
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
	flat(1, "2026-09-12", "103000")
	flat(2, "2026-09-12", "235900")
	flat(dayIncidentClip, "2026-09-10", "103000")
	unit := nasCleanupDayUnit{RelativeDir: dir, LocalDate: "2026-09-12", StreamID: 3, Files: 2, Bytes: 200}
	if files, size, err := dayCheck(ctx, pool, unit); err != nil || files != 2 || size != 200 {
		t.Fatalf("another day's hold refused the flat directory-day: %d files %d bytes %v", files, size, err)
	}
	if _, _, err := dayCheck(ctx, pool, nasCleanupDayUnit{RelativeDir: dir, LocalDate: "2026-09-10", StreamID: 3, Files: 1, Bytes: 100}); err == nil || !strings.Contains(err.Error(), "held") {
		t.Fatalf("held clip did not refuse its own day: %v", err)
	}
	dayDeliveryHoldsKeep(t, ctx, exec, pool, 2, unit)
	dayOtherHolds(t, exec, 2, dir)
	if files, size, err := dayCheck(ctx, pool, unit); err != nil || files != 2 || size != 200 {
		t.Fatalf("non-incident holds kept a non-KE flat directory-day: %d files %d bytes %v", files, size, err)
	}
	// An undated name anywhere in the directory still refuses every unit there.
	exec(fmt.Sprintf(`BEGIN; SET LOCAL session_replication_role=replica; UPDATE recording_clips SET display_path='%s/renamed.mp4' WHERE id=%d; COMMIT`, dir, dayIncidentClip))
	if _, _, err := dayCheck(ctx, pool, unit); err == nil || !strings.Contains(err.Error(), "does not name a local date") {
		t.Fatalf("undated name in the flat directory escaped: %v", err)
	}
}

// Close retires never-permitted units, so a retired plan's digest must not
// block admitting the same directory-days again; a live plan's still does.
func TestNASCleanupDayRetiredDigestIsAdmissibleAgain(t *testing.T) {
	pool, _, clip := dayFixture(t)
	clip(1, "2026-09-12", "103000", 100, dayAt("2026-09-12", "103000"), dayAt("2026-09-12", "103100"))
	units := []nasCleanupDayUnit{{RelativeDir: dayPlazaDir(dayFixtureFolder, dayDate("2026-09-12")), LocalDate: "2026-09-12", StreamID: 3, Files: 1, Bytes: 100}}
	sha := cleanupDayDigest(units)
	s := &Server{pool: pool, cfg: config.Config{NASCleanupDeletionEnabled: true}}
	create := func(label string) *httptest.ResponseRecorder {
		return dayCall(s.handleServiceNASCleanupDayCreate, "", map[string]any{"label": label, "policy_sha256": cleanupPolicySHA,
			"plan_sha256": sha, "files": 1, "bytes": 100, "units": units})
	}
	w := create("kday-a")
	if w.Code != 200 {
		t.Fatalf("admission: %d %s", w.Code, w.Body)
	}
	var first struct{ ID int64 }
	json.Unmarshal(w.Body.Bytes(), &first)
	if w := create("kday-b"); w.Code != 409 {
		t.Fatalf("live digest admitted twice: %d %s", w.Code, w.Body)
	}
	if w := dayCall(s.handleServiceNASCleanupDayClose, fmt.Sprint(first.ID), map[string]any{"plan_sha256": sha, "reason": "fixture"}); w.Code != 200 {
		t.Fatalf("close: %d %s", w.Code, w.Body)
	}
	if w := create("kday-c"); w.Code != 200 {
		t.Fatalf("retired digest blocked re-admission: %d %s", w.Code, w.Body)
	}
	if w := create("kday-c"); w.Code != 409 {
		t.Fatalf("label admitted twice: %d %s", w.Code, w.Body)
	}
}

// dayIncidentClip is a raw clip on the permanent NAS cleanup incident roster (0202).
const dayIncidentClip = 296368

// dayDeliveryHoldsKeep checks that a delivery source hold, a GSS source hold on
// clip, and a delivery path hold in dir each keep a non-KE directory-day (0261,
// GSS delivery freeze), then removes them.
func dayDeliveryHoldsKeep(t *testing.T, ctx context.Context, exec func(string, ...any), q cleanupDayQuerier, clip int64, u nasCleanupDayUnit) {
	t.Helper()
	exec(fmt.Sprintf(`BEGIN; SET LOCAL session_replication_role=replica; INSERT INTO delivery_nas_source_holds(clip_id) VALUES(%d); COMMIT`, clip))
	if _, _, err := dayCheck(ctx, q, u); err == nil || !strings.Contains(err.Error(), "held") {
		t.Fatalf("delivery source hold did not keep a non-KE directory-day: %v", err)
	}
	exec(fmt.Sprintf(`BEGIN; SET LOCAL session_replication_role=replica; DELETE FROM delivery_nas_source_holds WHERE clip_id=%d;
INSERT INTO nas_cleanup_gss_source_holds(clip_id) VALUES(%[1]d); COMMIT`, clip))
	if _, _, err := dayCheck(ctx, q, u); err == nil || !strings.Contains(err.Error(), "held") {
		t.Fatalf("GSS source hold did not keep a non-KE directory-day: %v", err)
	}
	exec(fmt.Sprintf(`BEGIN; SET LOCAL session_replication_role=replica; DELETE FROM nas_cleanup_gss_source_holds WHERE clip_id=%[1]d;
INSERT INTO delivery_keep_holds(bucket,object_key,etag,size_bytes,kind,relative_path) VALUES('fixture','delivery/%[1]d','etag',1,'raw','%[2]s/delivered.mp4');
COMMIT`, clip, u.RelativeDir))
	if _, _, err := dayCheck(ctx, q, u); err == nil || !strings.Contains(err.Error(), "delivery path hold") {
		t.Fatalf("delivery path hold did not keep a non-KE directory-day: %v", err)
	}
	exec(fmt.Sprintf(`BEGIN; SET LOCAL session_replication_role=replica; DELETE FROM delivery_keep_holds WHERE object_key='delivery/%d'; COMMIT`, clip))
}

// dayOtherHolds puts the non-incident, non-delivery holds 0241 counted on clip:
// an R2 retirement reservation and a dry-run per-file cleanup plan's source hold.
func dayOtherHolds(t *testing.T, exec func(string, ...any), clip int64, dir string) {
	t.Helper()
	exec(fmt.Sprintf(`BEGIN; SET LOCAL session_replication_role=replica;
INSERT INTO r2_retirement_reservations(kind,source_id,bucket,object_key,size_bytes,sha256,manifest_sha256)
 SELECT 'raw',id,bucket,object_key,size_bytes,sha256,repeat('e',64) FROM recording_clips WHERE id=%[1]d;
INSERT INTO nas_cleanup_plans(id,account_id,connection_id,label,report_sha256,manifest,plan_sha256,mode)
 VALUES(%[1]d,47,13,'perfile-%[1]d',repeat('d',64),'[]',repeat('e',64),'dry_run');
INSERT INTO nas_cleanup_source_holds(plan_id,clip_id) VALUES(%[1]d,%[1]d);
COMMIT`, clip, dir))
}

// dayCheck totals cleanupDayCheck's media entries.
func dayCheck(ctx context.Context, q cleanupDayQuerier, u nasCleanupDayUnit) (int64, int64, error) {
	entries, err := cleanupDayCheck(ctx, q, u, "fixture")
	files, bytes := cleanupDayMedia(entries)
	return files, bytes, err
}

// The tunable-modes client receives names mode (exact inventory entries) for a
// flat directory and dir mode for a plaza day directory; older clients get
// today's permit. Expiry no longer follows the last heartbeat.
func TestNASCleanupDayPermitModes(t *testing.T) {
	pool, exec, insert := dayFixture(t)
	ctx := context.Background()
	flatDir := dayFixtureFolder + "/30/continuous"
	flat := func(id int64, day, hhmmss string) string {
		t.Helper()
		at := dayAt(day, hhmmss)
		name := fmt.Sprintf("%d.mp4", at.Unix())
		exec(`INSERT INTO recording_clips(id,recording_id,storage_destination_id,endpoint,bucket,object_key,size_bytes,etag,sha256,
 fire_at,clip_start_at,clip_end_at,display_path,purged_at) VALUES($1,30,1,'https://storage.example.test','fixture',$2,100,'etag',repeat('a',64),$3,$3,$4,$5,now())`,
			id, fmt.Sprintf("obj-%d", id), at, at.Add(time.Minute), flatDir+"/"+name)
		exec(`INSERT INTO nas_inventory_files(connection_id,clip_id,recording_id,relative_path,size_bytes,sha256,state,client_updated_at)
 VALUES(13,$1,30,$2,$3,repeat('a',64),'present',now())`, id, flatDir+"/"+name, 100+id)
		return name
	}
	a := flat(1, "2026-09-12", "103000")
	b := flat(2, "2026-09-12", "113000")
	flat(3, "2026-09-11", "103000") // another day in the same directory
	exec(`UPDATE nas_inventory_files SET file_ctime_ns=1,file_inode=1,file_device=1,sidecar_relative_path=relative_path||'.stoarama.json',
 sidecar_size_bytes=50,sidecar_sha256=repeat('b',64) WHERE clip_id=1`)
	at := dayAt("2026-10-10", "103000")
	insert(4, "2026-10-10", "103000", 500, at, at.Add(time.Minute))
	units := []nasCleanupDayUnit{
		{RelativeDir: flatDir, LocalDate: "2026-09-12", StreamID: 3, Files: 2, Bytes: 203},
		{RelativeDir: dayPlazaDir(dayFixtureFolder, dayDate("2026-10-10")), LocalDate: "2026-10-10", StreamID: 3, Files: 1, Bytes: 500},
	}
	s := &Server{pool: pool, cfg: config.Config{NASCleanupDeletionEnabled: true}}
	sha := cleanupDayDigest(units)
	if w := dayCall(s.handleServiceNASCleanupDayCreate, "", map[string]any{"label": "kday-modes", "policy_sha256": cleanupPolicySHA,
		"plan_sha256": sha, "files": 3, "bytes": 703, "units": units}); w.Code != 200 {
		t.Fatalf("admission: %d %s", w.Code, w.Body)
	}
	var planID int64
	if err := pool.QueryRow(ctx, `SELECT id FROM nas_cleanup_day_plans WHERE label='kday-modes'`).Scan(&planID); err != nil {
		t.Fatal(err)
	}
	if w := dayCall(s.handleServiceNASCleanupDayControl, fmt.Sprint(planID), map[string]any{"plan_sha256": sha, "mode": "enabled", "reason": "fixture", "approval_hours": 1}); w.Code != 200 {
		t.Fatalf("enable: %d %s", w.Code, w.Body)
	}
	permit := func(version string) *nasCleanupDayPermit {
		t.Helper()
		// A heartbeat 3.5 minutes old used to cap the permit at 1.5 minutes.
		exec(`UPDATE connections SET client_version=$1,client_boot_id='fixture-boot',last_seen_at=now()-interval '210 seconds' WHERE id=13`, version)
		w := cleanupCall(t, s.handleAccountNASCleanupDayPermit, 47, 47, "", "", map[string]any{"client_version": version}, "fixture-boot "+version)
		var out struct {
			Unit *nasCleanupDayPermit `json:"unit"`
		}
		if json.Unmarshal(w.Body.Bytes(), &out) != nil || w.Code != 200 || out.Unit == nil {
			t.Fatalf("%s permit: %d %s", version, w.Code, w.Body)
		}
		if out.Unit.ExpiresAt.Before(time.Now().Add(nasCleanupDayPermitTTL - time.Minute)) {
			t.Fatalf("permit expiry follows the heartbeat: %v", out.Unit.ExpiresAt)
		}
		exec(`UPDATE nas_cleanup_day_units SET permit_expires_at=now() WHERE id=$1`, out.Unit.UnitID)
		return out.Unit
	}
	if old := permit(nasTunableDayClientVersion); old.Mode != "" || old.Entries != nil || old.RelativeDir != flatDir {
		t.Fatalf("tunable-day permit changed shape: %+v", old)
	}
	names := permit(nasCleanupDayModesClientVersion)
	want := []nasCleanupDayEntry{{Name: a, SizeBytes: 101}, {Name: a + ".stoarama.json", SizeBytes: 50, Sidecar: true}, {Name: b, SizeBytes: 102}}
	if names.Mode != nasCleanupDayNames || fmt.Sprint(names.Entries) != fmt.Sprint(want) {
		t.Fatalf("names permit: %+v", names)
	}
	if w := cleanupCall(t, s.handleAccountNASCleanupDayReceipt, 47, 47, "", "", map[string]any{"client_version": nasCleanupDayModesClientVersion,
		"unit_id": names.UnitID, "attempt": names.Attempt, "permit_token": names.Token, "outcome": nasCleanupDayDeleted, "media_files": 2,
		"media_bytes": 203, "sidecar_files": 1, "sidecar_bytes": 50, "remaining_files": 0, "refusal": ""}, "fixture-boot "+nasCleanupDayModesClientVersion); w.Code != 200 {
		t.Fatalf("names receipt: %d %s", w.Code, w.Body)
	}
	if dir := permit(nasCleanupDayModesClientVersion); dir.Mode != nasCleanupDayDir || dir.Entries != nil || dir.LocalDate != "2026-10-10" {
		t.Fatalf("dir permit: %+v", dir)
	}
}

// Keep Everything (Deniz: "nas shoul dnot have any of the keep everything
// (except the keep 20)"): a KE unit names its media, goes only to the ke-day
// release, always in names mode, and the server permits a named file only
// while its R2 counterpart is live: the raw object, or a stitched output
// listing the clip that is not retired. The preparer and runner apply the
// same rule to this schema.
func TestNASCleanupDayKeepEverythingNeedsALiveR2Counterpart(t *testing.T) {
	pool, exec, _ := dayFixture(t)
	ctx := context.Background()
	exec(`INSERT INTO streams(id,provider,external_id,name,slug,source_url,recording_state,local_timezone,capture_type,source_family,execution_class,capture_family,expected_fps)
 VALUES(78,'test','78','ke','ke','https://storage.example.test','off','Asia/Seoul','hls','video_manifest','video_live','continuous_video',30);
INSERT INTO recordings(id,account_id,storage_destination_id,name,stream_url,cron_expr,cron_timezone,status,stream_id,paused_at)
 VALUES(31,47,1,'ke','https://storage.example.test','* * * * *','Asia/Seoul','paused',78,now())`)
	seoul, _ := time.LoadLocation("Asia/Seoul")
	// Stream 78 keeps Seoul [2026-08-24, 2026-09-13); 2026-10-10 is outside.
	day := dayDate("2026-10-10")
	dir := dayPlazaDir(dayFixtureKE, day)
	name := func(hhmmss string) string { return dayPlazaName(day, hhmmss) }
	keClip := func(id int64, hhmmss string, size, nasSize int64, purged bool) {
		t.Helper()
		at, _ := time.ParseInLocation("2006-01-02 150405", "2026-10-10 "+hhmmss, seoul)
		exec(`INSERT INTO recording_clips(id,recording_id,storage_destination_id,endpoint,bucket,object_key,size_bytes,etag,sha256,
 fire_at,clip_start_at,clip_end_at,display_path,purged_at) VALUES($1,31,1,'https://storage.example.test','fixture',$2,$3,'etag',repeat('a',64),$4,$4,$5,$6,
 CASE WHEN $7 THEN now() END)`, id, fmt.Sprintf("managed/ke/%d.mp4", id), size, at, at.Add(time.Minute), dir+"/"+name(hhmmss), purged)
		exec(`INSERT INTO nas_inventory_files(connection_id,clip_id,recording_id,relative_path,size_bytes,sha256,state,client_updated_at)
 VALUES(13,$1,31,$2,$3,repeat('a',64),'present',now())`, id, dir+"/"+name(hhmmss), nasSize)
	}
	keClip(11, "100000", 100, 100, false) // raw object live
	keClip(12, "110000", 100, 100, true)  // raw purged, no stitched output
	keClip(13, "120000", 100, 100, true)  // raw purged, covered by a live stitched output
	keClip(14, "130000", 100, 99, false)  // raw live, but the NAS file has another size
	keClip(15, "140000", 100, 100, true)  // raw purged, its only stitched output retired
	exec(`UPDATE nas_inventory_files SET sidecar_relative_path=relative_path||'.stoarama.json',sidecar_size_bytes=7,sidecar_sha256=repeat('b',64),
 file_ctime_ns=1,file_inode=1,file_device=1 WHERE clip_id=13`)
	exec(`BEGIN; SET LOCAL session_replication_role=replica;
INSERT INTO recording_collation_hours(id,hour_id,batch_id,generation,recording_id,local_date,delivery_hour,status,manifest_object_key,manifest_sha256,
 manifest,source_clip_count,included_clip_count,excluded_clip_count,join_count,split_count)
 VALUES(1,'goodplus-x__recording-31__date-2026-10-10__hour-10__generation-2','goodplus-x',2,31,'2026-10-10',10,'collated','m/1',repeat('a',64),'{}',2,2,0,0,0);
INSERT INTO recording_collation_outputs(id,collation_hour_id,part,parts,object_key,nas_relative_path,size_bytes,sha256,start_at,end_at,source_clip_ids,r2_verified_at)
 VALUES(1,1,1,2,'joined/goodplus-x/objects/'||repeat('c',64)||'.mp4','x/1.mp4',900,repeat('c',64),'2026-10-10T03:00:00Z','2026-10-10T04:00:00Z',ARRAY[13],now()),
  (2,1,2,2,'joined/goodplus-x/objects/'||repeat('d',64)||'.mp4','x/2.mp4',800,repeat('d',64),'2026-10-10T05:00:00Z','2026-10-10T06:00:00Z',ARRAY[15],now());
INSERT INTO r2_object_retirements(kind,source_id,stream_id,bucket,object_key,size_bytes,sha256,reason,choice,manifest_sha256,policy_sha256)
 VALUES('collated',2,78,'fixture','joined/goodplus-x/objects/'||repeat('d',64)||'.mp4',800,repeat('d',64),'fixture','none',repeat('e',64),repeat('e',64));
COMMIT`)
	named := func(ids ...int64) []nasCleanupDayName {
		var out []nasCleanupDayName
		for _, id := range ids {
			size := int64(100)
			if id == 14 {
				size = 99 // the NAS size; the catalogued clip is 100
			}
			out = append(out, nasCleanupDayName{Name: name(fmt.Sprintf("%d0000", id-1)), SizeBytes: size})
		}
		return out
	}
	unit := func(names []nasCleanupDayName) nasCleanupDayUnit {
		u := nasCleanupDayUnit{RelativeDir: dir, LocalDate: "2026-10-10", StreamID: 78, Names: names, Files: int64(len(names))}
		for _, n := range names {
			u.Bytes += n.SizeBytes
		}
		return u
	}
	all := unit(named(11, 12, 13, 14, 15))
	// Static rules: KE names its media, others never; window and guard days stay.
	for label, u := range map[string]nasCleanupDayUnit{
		"KE without names":  {RelativeDir: dir, LocalDate: "2026-10-10", StreamID: 78, Files: 1, Bytes: 100},
		"names on non-KE":   {RelativeDir: dayPlazaDir(dayFixtureFolder, dayDate("2026-10-10")), LocalDate: "2026-10-10", StreamID: 3, Files: 1, Bytes: 100, Names: named(11)},
		"KE boundary day":   {RelativeDir: dayPlazaDir(dayFixtureKE, dayDate("2026-09-13")), LocalDate: "2026-09-13", StreamID: 78, Files: 1, Bytes: 100, Names: []nasCleanupDayName{{Name: dayPlazaName(dayDate("2026-09-13"), "100000"), SizeBytes: 100}}},
		"KE in-window day":  {RelativeDir: dayPlazaDir(dayFixtureKE, dayDate("2026-09-01")), LocalDate: "2026-09-01", StreamID: 78, Files: 1, Bytes: 100, Names: []nasCleanupDayName{{Name: dayPlazaName(dayDate("2026-09-01"), "100000"), SizeBytes: 100}}},
		"KE totals differ":  func() nasCleanupDayUnit { u := unit(named(11, 13)); u.Bytes++; return u }(),
		"KE names unsorted": unit([]nasCleanupDayName{named(13)[0], named(11)[0]}),
		"KE another date":   unit([]nasCleanupDayName{{Name: dayPlazaName(dayDate("2026-10-17"), "100000"), SizeBytes: 100}}),
	} {
		if _, _, _, err := cleanupDayStatic(u); err == nil {
			t.Errorf("%s qualified", label)
		}
	}
	if files, bytes, err := dayCheck(ctx, pool, all); err != nil || files != 2 || bytes != 200 {
		t.Fatalf("KE evidence: %d files %d bytes %v", files, bytes, err)
	}
	if _, _, err := dayCheck(ctx, pool, unit(named(12))); err == nil || !strings.Contains(err.Error(), "live R2 counterpart") {
		t.Fatalf("KE file without a counterpart qualified: %v", err)
	}
	// A KE unit keeps every 0241 hold: a delivery source hold or a delivery path
	// hold in the directory still keeps it.
	exec(`BEGIN; SET LOCAL session_replication_role=replica; INSERT INTO delivery_nas_source_holds(clip_id) VALUES(13); COMMIT`)
	if _, _, err := dayCheck(ctx, pool, all); err == nil || !strings.Contains(err.Error(), "held") {
		t.Fatalf("KE delivery-held clip qualified: %v", err)
	}
	exec(fmt.Sprintf(`BEGIN; SET LOCAL session_replication_role=replica; DELETE FROM delivery_nas_source_holds;
INSERT INTO delivery_keep_holds(bucket,object_key,etag,size_bytes,kind,relative_path) VALUES('fixture','delivery/ke','etag',1,'raw','%s/delivered.mp4');
COMMIT`, dir))
	if _, _, err := dayCheck(ctx, pool, all); err == nil || !strings.Contains(err.Error(), "delivery path hold") {
		t.Fatalf("KE directory with a delivery path hold qualified: %v", err)
	}
	exec(`BEGIN; SET LOCAL session_replication_role=replica; DELETE FROM delivery_keep_holds WHERE object_key='delivery/ke'; COMMIT`)

	// The preparer, given an R2 listing of every counterpart at its catalogued
	// size, names exactly the server-evidenced files; the runner finds them live.
	python := os.Getenv("STOARAMA_TEST_PYTHON")
	if python == "" {
		python = "python3"
	}
	script := `import json, os, sys
sys.path.insert(0, "../../scripts/nas-cleanup")
import prepare_keep28_plans as k
w = k.load_policy(k.POLICY_PATH)[78]
if sys.argv[1] == "sql":
    k.query = lambda sql, psql: print(sql)
    k.day_stream_rows(w, "psql")
else:
    facts = json.load(sys.stdin)
    sizes = {c[7]: c[6] for c in facts["clips"] if c[7]} | {o[0]: o[1] for o in facts["outputs"]}
    print(json.dumps(k.day_units(w, k.load_folders(), facts, lambda keys: {x: sizes[x] for x in keys if x in sizes})))`
	env := append(os.Environ(), "R2_BUCKET=fixture")
	sqlCmd := osexec.Command(python, "-B", "-c", script, "sql")
	sqlCmd.Env = env
	sql, err := sqlCmd.Output()
	if err != nil {
		t.Fatal(err)
	}
	var facts []byte
	if err := pool.QueryRow(ctx, string(sql)).Scan(&facts); err != nil {
		t.Fatal(err)
	}
	prepare := osexec.Command(python, "-B", "-c", script, "units")
	prepare.Env, prepare.Stdin = env, bytes.NewReader(facts)
	prepared, err := prepare.Output()
	if err != nil {
		t.Fatal(err)
	}
	var out []json.RawMessage
	var preparedUnits []nasCleanupDayUnit
	var excluded struct{ Files, Bytes int64 }
	if json.Unmarshal(prepared, &out) != nil || json.Unmarshal(out[0], &preparedUnits) != nil || json.Unmarshal(out[1], &excluded) != nil {
		t.Fatalf("preparer output %s", prepared)
	}
	want := unit(named(11, 13))
	if fmt.Sprint(preparedUnits) != fmt.Sprint([]nasCleanupDayUnit{want}) || excluded.Files != 3 || excluded.Bytes != 299 {
		t.Fatalf("preparer KE units %s, want %v and 3 files / 299 bytes excluded", prepared, want)
	}
	runnerUnlive := func(u nasCleanupDayUnit) string {
		t.Helper()
		plan, _ := json.Marshal(map[string]any{"units": []nasCleanupDayUnit{u}})
		cmd := osexec.Command(python, "-B", "-c", `import json, sys
from pathlib import Path
sys.path.insert(0, "../../../ops/jelly/cleanup-runner")
import runner as r
job = r.Runner(Path("../../.."), Path("."))
job.windows = {78: (r.timestamp("2026-08-23T15:00:00+00:00"), r.timestamp("2026-09-12T15:00:00+00:00"))}
job.timezones = {78: "Asia/Seoul"}
job.keep_everything = {78}
job.query = lambda sql, step=None: print(sql)
job.day_catalog(json.loads(sys.argv[1]))`, string(plan))
		cmd.Env = env
		sql, err := cmd.Output()
		if err != nil {
			t.Fatal(err)
		}
		var raw []byte
		if err := pool.QueryRow(ctx, string(sql)).Scan(&raw); err != nil {
			t.Fatal(err)
		}
		return string(raw)
	}
	if got := runnerUnlive(want); !strings.Contains(got, `"unlive" : 0`) {
		t.Fatalf("runner KE catalog: %s", got)
	}
	if got := runnerUnlive(all); !strings.Contains(got, `"unlive" : 3`) {
		t.Fatalf("runner missed KE names without a live counterpart: %s", got)
	}

	s := &Server{pool: pool, cfg: config.Config{NASCleanupDeletionEnabled: true, R2Bucket: "fixture"}}
	admit := func(label string, u nasCleanupDayUnit) *httptest.ResponseRecorder {
		units := []nasCleanupDayUnit{u}
		return dayCall(s.handleServiceNASCleanupDayCreate, "", map[string]any{"label": label, "policy_sha256": cleanupPolicySHA,
			"plan_sha256": cleanupDayDigest(units), "files": u.Files, "bytes": u.Bytes, "units": units})
	}
	if w := admit("kday-s78-all", all); w.Code != 409 || !strings.Contains(w.Body.String(), "2 files, 200 bytes") {
		t.Fatalf("KE plan naming files without counterparts admitted: %d %s", w.Code, w.Body)
	}
	w := admit("kday-s78-day-0001", want)
	if w.Code != 200 {
		t.Fatalf("KE admission: %d %s", w.Code, w.Body)
	}
	var admitted struct {
		ID         int64
		PlanSHA256 string `json:"plan_sha256"`
	}
	json.Unmarshal(w.Body.Bytes(), &admitted)
	var status struct {
		Units []struct{ Names []nasCleanupDayName }
	}
	if json.Unmarshal(dayCall(s.handleServiceNASCleanupDayStatus, fmt.Sprint(admitted.ID), nil).Body.Bytes(), &status) != nil ||
		len(status.Units) != 1 || fmt.Sprint(status.Units[0].Names) != fmt.Sprint(want.Names) {
		t.Fatalf("status names: %+v", status)
	}
	if w := dayCall(s.handleServiceNASCleanupDayControl, fmt.Sprint(admitted.ID), map[string]any{"plan_sha256": admitted.PlanSHA256,
		"mode": "enabled", "reason": "fixture", "approval_hours": 1}); w.Code != 200 {
		t.Fatalf("enable: %d %s", w.Code, w.Body)
	}
	permit := func(version string) *nasCleanupDayPermit {
		t.Helper()
		exec(`UPDATE connections SET client_version=$1,client_boot_id='fixture-boot',last_seen_at=now() WHERE id=13`, version)
		w := cleanupCall(t, s.handleAccountNASCleanupDayPermit, 47, 47, "", "", map[string]any{"client_version": version}, "fixture-boot "+version)
		var out struct {
			Unit *nasCleanupDayPermit `json:"unit"`
		}
		if json.Unmarshal(w.Body.Bytes(), &out) != nil || w.Code != 200 {
			t.Fatalf("%s permit: %d %s", version, w.Code, w.Body)
		}
		return out.Unit
	}
	for _, older := range []string{nasTunableDayClientVersion, nasCleanupDayModesClientVersion} {
		if p := permit(older); p != nil {
			t.Fatalf("%s received a Keep Everything permit: %+v", older, p)
		}
	}
	p := permit(nasCleanupDayKEClientVersion)
	wantEntries := []nasCleanupDayEntry{{Name: name("100000"), SizeBytes: 100}, {Name: name("120000"), SizeBytes: 100},
		{Name: name("120000") + ".stoarama.json", SizeBytes: 7, Sidecar: true}}
	if p == nil || p.Mode != nasCleanupDayNames || fmt.Sprint(p.Entries) != fmt.Sprint(wantEntries) || p.DroppedNames != 0 {
		t.Fatalf("KE plaza day must go in names mode with evidenced entries only: %+v", p)
	}
	// The raw object of clip 11 leaves R2 before the next attempt: its NAS copy stays.
	exec(`UPDATE nas_cleanup_day_units SET permit_expires_at=now() WHERE id=$1`, p.UnitID)
	exec(`UPDATE recording_clips SET purged_at=now() WHERE id=11`)
	again := permit(nasDayInventoryClientVersion)
	if again == nil || fmt.Sprint(again.Entries) != fmt.Sprint(wantEntries[1:]) || again.DroppedNames != 1 {
		t.Fatalf("re-permit kept a file whose counterpart left R2: %+v", again)
	}
	var dropped struct {
		Units []struct {
			DroppedNames int64 `json:"dropped_names"`
		}
	}
	if json.Unmarshal(dayCall(s.handleServiceNASCleanupDayStatus, fmt.Sprint(admitted.ID), nil).Body.Bytes(), &dropped) != nil ||
		len(dropped.Units) != 1 || dropped.Units[0].DroppedNames != 1 {
		t.Fatalf("status does not surface the dropped name: %+v", dropped)
	}
}
