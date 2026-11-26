package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/daydemir/stoarama/backend/internal/r2"
)

func TestValidateNASDayInventoryResult(t *testing.T) {
	now := time.Now()
	sha := strings.Repeat("a", 64)
	valid := nasDayInventoryResult{Attempt: 1, StartedAt: now.Add(-time.Minute), ClientVersion: nasDayInventoryClientVersion, Files: []nasDayInventoryFile{
		{RelativePath: "f/September/25-Friday/a.mp4", Kind: nasDayInventoryRaw, SizeBytes: 1, SHA256: sha},
		{RelativePath: "joined/f/September/25-Friday/a.mp4", Kind: nasDayInventoryJoined, SizeBytes: 1, SHA256: sha},
	}}
	if err := validateNASDayInventoryResult(valid, now); err != nil {
		t.Fatalf("valid result rejected: %v", err)
	}
	for name, mutate := range map[string]func(*nasDayInventoryResult){
		"zero attempt":     func(r *nasDayInventoryResult) { r.Attempt = 0 },
		"future start":     func(r *nasDayInventoryResult) { r.StartedAt = now.Add(time.Hour) },
		"files with error": func(r *nasDayInventoryResult) { r.Error = "boom" },
		"raw under joined": func(r *nasDayInventoryResult) { r.Files[0].RelativePath = "joined/x.mp4" },
		"joined elsewhere": func(r *nasDayInventoryResult) { r.Files[1].RelativePath = "x/y.mp4" },
		"recycle":          func(r *nasDayInventoryResult) { r.Files[0].RelativePath = "#recycle/x.mp4" },
		"unknown kind":     func(r *nasDayInventoryResult) { r.Files[0].Kind = "stitched" },
		"duplicate":        func(r *nasDayInventoryResult) { r.Files[1] = r.Files[0] },
		"not media":        func(r *nasDayInventoryResult) { r.Files[0].RelativePath = "f/a.json" },
		"escape":           func(r *nasDayInventoryResult) { r.Files[0].RelativePath = "f/../a.mp4" },
		"bad sha":          func(r *nasDayInventoryResult) { r.Files[0].SHA256 = strings.ToUpper(sha) },
	} {
		req := valid
		req.Files = append([]nasDayInventoryFile(nil), valid.Files...)
		mutate(&req)
		if err := validateNASDayInventoryResult(req, now); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
}

// A unit lists one stream-day; only a file whose name and size are those of
// exactly one clip of that day is matched, only a match with the catalogued
// sha256 is restorable, and only to the clip's own key from the path read.
func TestNASDayInventoryLifecycleAndRestore(t *testing.T) {
	pool := dayTestPool(t)
	ctx := context.Background()
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := pool.Exec(ctx, sql, args...); err != nil {
			t.Fatal(err)
		}
	}
	// Stream 17216 (Europe/Warsaw) is pinned and not Keep Everything.
	exec(`
INSERT INTO accounts(id,email) VALUES(47,'nas-inventory@example.invalid'),(48,'other@example.invalid');
INSERT INTO account_api_keys(id,account_id,key_prefix,secret_hash) VALUES(47,47,'fixture-key','fixture-key'),(48,48,'other-key','other-key');
INSERT INTO connections(id,account_id,kind,api_key_id,client_version) VALUES(13,47,'nas_pull',47,'r2-copy-v8-inventory'),(14,48,'nas_pull',48,'r2-copy-v8-output-restore');
INSERT INTO streams(id,provider,external_id,name,slug,source_url,recording_state,local_timezone,capture_type,source_family,execution_class,capture_family,expected_fps)
 VALUES(17216,'test','17216','brzeg','brzeg','https://storage.example.test','off','Europe/Warsaw','hls','video_manifest','video_live','continuous_video',30);
INSERT INTO storage_destinations(id,account_id,name,endpoint,region,bucket,access_key_id,secret_access_key_enc)
 VALUES(1,47,'test','https://storage.example.test','auto','bucket','fixture',decode('00','hex'));
INSERT INTO recordings(id,account_id,storage_destination_id,name,stream_url,cron_expr,cron_timezone,status,stream_id,paused_at)
 VALUES(335,47,1,'brzeg','https://storage.example.test','* * * * *','Europe/Warsaw','paused',17216,now())`)
	var ke bool
	if err := pool.QueryRow(ctx, `SELECT keep_everything FROM nas_media_keep20_windows WHERE stream_id=17216`).Scan(&ke); err != nil || ke {
		t.Fatalf("fixture stream must be pinned and not Keep Everything: ke=%v err=%v", ke, err)
	}

	type clip struct {
		id         int64
		name, key  string
		body       []byte
		sha        string
		start      string
		size       int
		purgedNull bool
	}
	clips := map[string]*clip{}
	add := func(label, name, start string, body []byte, catalogSize int) *clip {
		t.Helper()
		c := &clip{id: int64(900 + len(clips)), name: name, key: "managed/acct-47/" + label + ".mp4", body: body, start: start, size: catalogSize}
		sum := sha256.Sum256(body)
		c.sha = hex.EncodeToString(sum[:])
		exec(`INSERT INTO recording_clips(id,recording_id,storage_destination_id,endpoint,bucket,object_key,size_bytes,etag,sha256,
 fire_at,clip_start_at,clip_end_at,display_path) VALUES($1,335,1,'https://storage.example.test','bucket',$2,$3,'etag',$4,
 $5::timestamptz,$5::timestamptz,$5::timestamptz+interval '2 minutes',$6)`, c.id, c.key, catalogSize, c.sha, start, "recordings/335/continuous/"+name)
		clips[label] = c
		return c
	}
	day := "2026-09-25T08:00:00+02:00"
	moved := add("moved", "1758780000.mp4", day, []byte("moved bytes"), len("moved bytes"))
	changed := add("changed", "1758780120.mp4", day, []byte("catalog bytes"), len("NAS bytes!!!!"))
	resized := add("resized", "1758780240.mp4", day, []byte("resized"), 99)
	twinA := add("twin-a", "twin.mp4", day, []byte("twin"), 4)
	add("twin-b", "twin.mp4", "2026-09-25T09:00:00+02:00", []byte("twin"), 4)
	twice := add("twice", "1758780360.mp4", day, []byte("twice"), 5)
	otherDay := add("other-day", "1758866400.mp4", "2026-09-26T08:00:00+02:00", []byte("next day"), 8)
	exec(`UPDATE recording_clips SET display_path='recordings/335/4411/twin.mp4' WHERE id=$1`, twinA.id)

	bucket := &fakeRestoreBucket{objects: map[string][]byte{}}
	storage := httptest.NewServer(bucket)
	defer storage.Close()
	client, err := r2.New(ctx, r2.Config{UngatedWriter: true, AccessKey: "key", SecretKey: "secret", Region: "auto", Bucket: "bucket", Endpoint: storage.URL})
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{pool: pool, r2: client}
	router := chi.NewRouter()
	router.Post("/day-inventory/lease", s.handleAccountConnectionDayInventoryLease)
	router.Post("/day-inventory/{unitId}/result", s.handleAccountConnectionDayInventoryResult)
	router.Post("/restore/lease", s.handleAccountConnectionRestoreLease)
	router.Post("/restore/{taskId}/result", s.handleAccountConnectionRestoreResult)
	call := func(keyID int64, version, path string, body any) (int, string) {
		req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(must(json.Marshal(body))))
		req.Header.Set("User-Agent", "stoarama-nas-pull/"+version)
		req = req.WithContext(context.WithValue(req.Context(), accountPrincipalContextKey,
			accountPrincipal{AccountID: keyID, APIKeyID: ptrInt64(keyID)}))
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		return rec.Code, rec.Body.String()
	}
	lease := func() nasDayInventoryLeaseResponse {
		t.Helper()
		code, body := call(47, nasDayInventoryClientVersion, "/day-inventory/lease", map[string]any{"client_version": nasDayInventoryClientVersion})
		var resp nasDayInventoryLeaseResponse
		if code != http.StatusOK || json.Unmarshal([]byte(body), &resp) != nil {
			t.Fatalf("lease status=%d body=%s", code, body)
		}
		return resp
	}

	exec(`INSERT INTO nas_day_inventory_units(connection_id,label,recording_id,local_date) VALUES(13,'gss-inv',335,'2026-09-25')`)
	if _, err := pool.Exec(ctx, `INSERT INTO nas_day_inventory_units(connection_id,label,recording_id,local_date) VALUES(13,'again',335,'2026-09-25')`); err == nil {
		t.Fatal("a second live unit for the same stream-day was queued")
	}

	// Every other release is refused before anything is leased.
	for _, version := range []string{nasOutputRestoreClientVersion, nasDayInventoryClientVersion + "-extra", "development"} {
		if code, body := call(47, version, "/day-inventory/lease", map[string]any{}); code != http.StatusConflict {
			t.Fatalf("release %q leased an inventory: %d %s", version, code, body)
		}
	}
	if code, body := call(48, nasDayInventoryClientVersion, "/day-inventory/lease", map[string]any{}); code != http.StatusForbidden {
		t.Fatalf("connection running another release leased: %d %s", code, body)
	}

	got := lease()
	u := got.Unit
	if u == nil || u.Attempt != 1 || u.RecordingID != 335 || u.StreamID != 17216 || u.LocalDate != "2026-09-25" || u.Timezone != "Europe/Warsaw" ||
		strings.Join(u.ExpectedDirs, ",") != "recordings/335/4411,recordings/335/continuous" {
		t.Fatalf("leased unit %+v", got)
	}
	if again := lease(); again.Unit != nil || again.RetryAfterSec != nasDayInventoryIdleRetry {
		t.Fatalf("a leased unit was re-issued: %+v", again)
	}

	file := func(path string, body []byte) nasDayInventoryFile {
		sum := sha256.Sum256(body)
		return nasDayInventoryFile{RelativePath: path, Kind: nasDayInventoryRaw, SizeBytes: int64(len(body)), MTimeNS: 1, SHA256: hex.EncodeToString(sum[:])}
	}
	plaza := "03_Brzeg/September/25-Friday/"
	files := []nasDayInventoryFile{
		file(plaza+moved.name, moved.body),                // alternate layout, exact bytes
		file(plaza+changed.name, []byte("NAS bytes!!!!")), // name and size, other bytes
		file(plaza+resized.name, resized.body),            // name, other size
		file(plaza+"twin.mp4", twinA.body),                // two clips of the day share name and size
		file(plaza+twice.name, twice.body),                // one clip, two files
		file("recordings/335/continuous/"+twice.name, twice.body),
		file(plaza+otherDay.name, otherDay.body), // a clip of another day
		file(plaza+"uncatalogued.mp4", []byte("unknown")),
		{RelativePath: "joined/03_Brzeg/September/25-Friday/x_hour_08_080000-085959.mp4", Kind: nasDayInventoryJoined, SizeBytes: 3, SHA256: strings.Repeat("b", 64)},
	}
	report := func(attempt int, files []nasDayInventoryFile, errText string) (int, string) {
		return call(47, nasDayInventoryClientVersion, fmt.Sprintf("/day-inventory/%d/result", u.UnitID), nasDayInventoryResult{
			Attempt: attempt, StartedAt: time.Now().Add(-time.Minute), ClientVersion: nasDayInventoryClientVersion, Files: files, Error: errText})
	}
	if code, _ := report(2, files, ""); code != http.StatusConflict {
		t.Fatalf("stale attempt accepted: %d", code)
	}
	// A failed scan records nothing and requeues the unit.
	if code, body := report(1, nil, "PermissionError: denied"); code != http.StatusOK || !strings.Contains(body, `"retry"`) {
		t.Fatalf("failure report %d %s", code, body)
	}
	if u = lease().Unit; u == nil || u.Attempt != 2 {
		t.Fatalf("failed unit not requeued: %+v", u)
	}
	if code, body := report(2, files, ""); code != http.StatusOK || !strings.Contains(body, `"matched":2`) || !strings.Contains(body, `"raw":8`) || !strings.Contains(body, `"joined":1`) {
		t.Fatalf("result %d %s", code, body)
	}
	if code, _ := report(2, files, ""); code != http.StatusConflict {
		t.Fatal("a completed unit accepted a second result")
	}
	var matched string
	if err := pool.QueryRow(ctx, `SELECT string_agg(relative_path||'='||clip_id, ',' ORDER BY relative_path) FROM nas_day_inventory_files WHERE clip_id IS NOT NULL`).Scan(&matched); err != nil {
		t.Fatal(err)
	}
	if want := fmt.Sprintf("%s%s=%d,%s%s=%d", plaza, moved.name, moved.id, plaza, changed.name, changed.id); matched != want {
		t.Fatalf("matched %s, want %s", matched, want)
	}
	var ledgerRows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM nas_inventory_files`).Scan(&ledgerRows); err != nil || ledgerRows != 0 {
		t.Fatalf("inventory wrote %d rows into the client's sidecar ledger: %v", ledgerRows, err)
	}

	// Restores: only the exact-bytes match is admitted past the writer fence,
	// to its own key, from the path the inventory read.
	exec(`INSERT INTO r2_retirement_writer_fences(bucket,endpoint_sha256) VALUES('stoarama',repeat('a',64))`)
	queue := func(c *clip, path string, sha string) (int64, error) {
		var id int64
		err := pool.QueryRow(ctx, `INSERT INTO nas_restore_requests(connection_id,clip_id,recording_id,target,object_key,source_object_key,relative_path,content_type,size_bytes,sha256,label)
			VALUES(13,$1,335,'original',$2,$2,$3,'video/mp4',$4,$5,'gss') RETURNING id`, c.id, c.key, path, c.size, sha).Scan(&id)
		return id, err
	}
	for name, try := range map[string]func() (int64, error){
		"other bytes":  func() (int64, error) { return queue(changed, plaza+changed.name, changed.sha) },
		"catalog path": func() (int64, error) { return queue(moved, "recordings/335/continuous/"+moved.name, moved.sha) },
		"unmatched":    func() (int64, error) { return queue(twice, plaza+twice.name, twice.sha) },
	} {
		if _, err := try(); err == nil || !strings.Contains(err.Error(), "fenced") {
			t.Fatalf("%s restore queued past the writer fence: %v", name, err)
		}
	}
	restoreID, err := queue(moved, plaza+moved.name, moved.sha)
	if err != nil {
		t.Fatal(err)
	}
	code, body := call(47, "abc", "/restore/lease", map[string]any{"client_version": "abc", "max_tasks": 8})
	var tasks nasRestoreLeaseResponse
	if code != http.StatusOK || json.Unmarshal([]byte(body), &tasks) != nil || len(tasks.Tasks) != 1 || tasks.Tasks[0].TaskID != restoreID ||
		tasks.Tasks[0].RelativePath != plaza+moved.name || !strings.Contains(tasks.Tasks[0].Put.URL, "/bucket/"+moved.key) || tasks.Tasks[0].Put.Headers["If-None-Match"] != "*" {
		t.Fatalf("restore lease %d %s", code, body)
	}
	task := tasks.Tasks[0]
	put, _ := http.NewRequest(task.Put.Method, task.Put.URL, bytes.NewReader(moved.body))
	for k, v := range task.Put.Headers {
		put.Header.Set(k, v)
	}
	if resp, err := http.DefaultClient.Do(put); err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("restore put %v %v", resp, err)
	}
	if code, body := call(47, "abc", fmt.Sprintf("/restore/%d/result", restoreID), map[string]any{"attempt": 1, "outcome": "uploaded", "duration_ms": 1}); code != http.StatusOK || !strings.Contains(body, `"state":"verified"`) {
		t.Fatalf("restore report %d %s", code, body)
	}
	if b, _ := bucket.get(moved.key); string(b) != string(moved.body) {
		t.Fatal("restored object differs")
	}

	// Canceling the unit withdraws the scope: a pending restore is no longer leased.
	again, err := queue(moved, plaza+moved.name, moved.sha)
	if err != nil {
		t.Fatal(err)
	}
	exec(`UPDATE nas_day_inventory_units SET state='canceled' WHERE label='gss-inv'`)
	if code, body := call(47, "abc", "/restore/lease", map[string]any{"client_version": "abc", "max_tasks": 8}); code != http.StatusOK || strings.Contains(body, fmt.Sprintf(`"task_id":%d`, again)) {
		t.Fatalf("withdrawn restore leased: %d %s", code, body)
	}
}
