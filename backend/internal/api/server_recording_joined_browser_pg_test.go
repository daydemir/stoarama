package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func joinedBrowserTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	databaseURL := strings.TrimSpace(os.Getenv("STOARAMA_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL to run joined browser DB regressions")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("joined_browser_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, `CREATE SCHEMA `+pgx.Identifier{schema}.Sanitize()); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ConnConfig.RuntimeParams == nil {
		cfg.ConnConfig.RuntimeParams = map[string]string{}
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		_, _ = admin.Exec(context.Background(), `DROP SCHEMA `+pgx.Identifier{schema}.Sanitize()+` CASCADE`)
		admin.Close()
	})
	if _, err := pool.Exec(ctx, `
		CREATE TABLE recordings(id bigint PRIMARY KEY,account_id bigint NOT NULL,status text NOT NULL,created_at timestamptz NOT NULL DEFAULT now());
		CREATE TABLE recording_clips(id bigint PRIMARY KEY,recording_id bigint NOT NULL,clip_start_at timestamptz NOT NULL,clip_end_at timestamptz NOT NULL,purged_at timestamptz);
		CREATE TABLE recording_joined_hours(id bigint PRIMARY KEY,batch_record_id bigint NOT NULL,account_id bigint NOT NULL,recording_id bigint NOT NULL,state text NOT NULL,hour_id text NOT NULL,local_date date NOT NULL,delivery_hour integer NOT NULL,scheduled_start_at timestamptz NOT NULL,scheduled_end_at timestamptz NOT NULL);
		CREATE TABLE recording_joined_sources(id bigint PRIMARY KEY,hour_record_id bigint NOT NULL,batch_record_id bigint NOT NULL,account_id bigint NOT NULL,recording_id bigint NOT NULL,clip_id bigint NOT NULL,start_at timestamptz NOT NULL DEFAULT '2026-05-04 08:00:00Z',end_at timestamptz NOT NULL DEFAULT '2026-05-04 08:01:00Z');
		CREATE TABLE recording_joined_artifacts(id bigint PRIMARY KEY,hour_record_id bigint NOT NULL,batch_record_id bigint NOT NULL,account_id bigint NOT NULL,artifact_kind text NOT NULL,publication_state text,published_at timestamptz,etag text,version_id text,content_type text NOT NULL,relative_path text NOT NULL,expected_size_bytes bigint NOT NULL,expected_sha256 text NOT NULL,object_key text NOT NULL,ordinal integer NOT NULL);
		CREATE TABLE recording_joined_media_sources(artifact_id bigint NOT NULL,source_id bigint NOT NULL,ordinal integer NOT NULL,PRIMARY KEY(artifact_id,source_id));
		CREATE TABLE recording_joined_hour_dispositions(hour_record_id bigint NOT NULL,source_id bigint NOT NULL,disposition text NOT NULL,PRIMARY KEY(hour_record_id,source_id));
	`); err != nil {
		t.Fatal(err)
	}
	return pool
}

func seedJoinedBrowserTestData(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `
		INSERT INTO recordings(id,account_id,status) VALUES (10,47,'completed'),(20,47,'completed'),(30,47,'completed'),(40,47,'completed'),(50,99,'completed');
		INSERT INTO recording_clips VALUES
			(101,10,'2026-05-04 08:00:00Z','2026-05-04 08:01:00Z',NULL),
			(102,20,'2026-05-04 08:00:00Z','2026-05-04 08:01:00Z',NULL),
			(103,20,'2026-05-04 08:01:00Z','2026-05-04 08:02:00Z',NULL),
			(104,20,'2026-05-04 08:02:00Z','2026-05-04 08:03:00Z',NULL),
			(105,30,'2026-05-04 08:59:30Z','2026-05-04 09:00:30Z',NULL),
			(106,50,'2026-05-04 08:00:00Z','2026-05-04 08:01:00Z',NULL);
		INSERT INTO recording_joined_hours VALUES
			(201,1,47,20,'sealed','partial-hour','2026-05-04',1,'2026-05-04 08:00:00Z','2026-05-04 09:00:00Z'),
			(202,1,47,30,'sealed','full-hour','2026-05-04',2,'2026-05-04 09:00:00Z','2026-05-04 10:00:00Z'),
			(203,1,47,10,'pending','unsealed-hour','2026-05-04',3,'2026-05-04 10:00:00Z','2026-05-04 11:00:00Z'),
			(204,2,99,50,'sealed','foreign-hour','2026-05-04',1,'2026-05-04 08:00:00Z','2026-05-04 09:00:00Z');
		INSERT INTO recording_joined_sources VALUES
			(401,201,1,47,20,102),(402,201,1,47,20,103),(403,201,1,47,20,104),
			(404,202,1,47,30,105),(405,203,1,47,10,101),(406,204,2,99,50,106);
		INSERT INTO recording_joined_artifacts VALUES
			(301,201,1,47,'hour_manifest','published',now(),'manifest-1','','application/json','20_Europe_Poland_Luban/coverage/hours/hour_01.json',10,repeat('a',64),'joined/private/manifest-1.json',1),
			(302,201,1,47,'media',NULL,now(),'media-1','','video/mp4','20_Europe_Poland_Luban/May/Monday/hour_01_part_01_0800-0801.mp4',10,repeat('b',64),'joined/private/media-1.mp4',1),
			(303,201,1,47,'media',NULL,now(),'media-2','','video/mp4','20_Europe_Poland_Luban/May/Monday/hour_01_part_02_0801-0802.mp4',10,repeat('c',64),'joined/private/media-2.mp4',2),
			(304,202,1,47,'hour_manifest','published',now(),'manifest-2','','application/json','30_Europe_Poland_Test/coverage/hours/hour_02.json',10,repeat('d',64),'joined/private/manifest-2.json',1),
			(305,202,1,47,'media',NULL,now(),'media-3','','video/mp4','30_Europe_Poland_Test/May/Monday/hour_02_part_01_0859-0900.mp4',10,repeat('e',64),'joined/private/media-3.mp4',1),
			(306,203,1,47,'hour_manifest','published',now(),'manifest-3','','application/json','10_Europe_Poland_Test/coverage/hours/hour_03.json',10,repeat('f',64),'joined/private/manifest-3.json',1),
			(307,203,1,47,'media',NULL,now(),'media-4','','video/mp4','10_Europe_Poland_Test/May/Monday/hour_03_part_01_1000-1001.mp4',10,repeat('1',64),'joined/private/media-4.mp4',1),
			(308,204,2,99,'hour_manifest','published',now(),'manifest-4','','application/json','50_Europe_Poland_Foreign/coverage/hours/hour_01.json',10,repeat('2',64),'joined/foreign/manifest.json',1),
			(309,204,2,99,'media',NULL,now(),'media-5','','video/mp4','50_Europe_Poland_Foreign/May/Monday/hour_01_part_01.mp4',10,repeat('3',64),'joined/foreign/media.mp4',1);
		INSERT INTO recording_joined_media_sources VALUES (302,401,1),(303,402,1),(305,404,1),(307,405,1),(309,406,1);
		INSERT INTO recording_joined_hour_dispositions VALUES (201,401,'included'),(201,402,'included'),(201,403,'quarantined');
	`); err != nil {
		t.Fatal(err)
	}
}

func TestRecordingJoinedProgressUsesExactPublishedMediaProvenance(t *testing.T) {
	pool := joinedBrowserTestPool(t)
	seedJoinedBrowserTestData(t, pool)
	s := &Server{pool: pool}
	progress, err := s.recordingJoinedProgressForAccount(context.Background(), 47, []int64{10, 20, 30, 40, 50})
	if err != nil {
		t.Fatal(err)
	}
	for recordingID, want := range map[int64]*int{10: intPtr(0), 20: intPtr(66), 30: intPtr(100), 40: nil} {
		got, ok := progress[recordingID]
		if !ok || (want == nil) != (got.Percent == nil) || (want != nil && *want != *got.Percent) {
			t.Fatalf("recording=%d progress=%+v wantPercent=%v", recordingID, got, want)
		}
	}
	if _, leaked := progress[50]; leaked {
		t.Fatal("foreign-account recording appeared in joined progress")
	}

	bins := make([]recordingHealthBin, 2)
	starts := []time.Time{time.Date(2026, 5, 4, 8, 59, 0, 0, time.UTC), time.Date(2026, 5, 4, 9, 0, 0, 0, time.UTC)}
	ends := []time.Time{starts[1], starts[1].Add(time.Minute)}
	if err := s.populateRecordingJoinedProgressBins(context.Background(), 47, 30, starts, ends, bins); err != nil {
		t.Fatal(err)
	}
	for i, bin := range bins {
		if bin.SourceDurationMS != 30_000 || bin.JoinedReadyMS != 30_000 {
			t.Fatalf("boundary bin %d=%+v want 30000/30000", i, bin)
		}
	}
	listBins, err := s.recordingJoinedProgressForBins(context.Background(), 47,
		[]int64{20, 30, 30},
		[]time.Time{time.Date(2026, 5, 4, 8, 0, 0, 0, time.UTC), starts[0], starts[1]},
		[]time.Time{time.Date(2026, 5, 4, 9, 0, 0, 0, time.UTC), ends[0], ends[1]},
	)
	if err != nil {
		t.Fatal(err)
	}
	wantBins := []recordingJoinedBinProgress{
		{SourceDurationMS: 180_000, JoinedReadyMS: 120_000},
		{SourceDurationMS: 30_000, JoinedReadyMS: 30_000},
		{SourceDurationMS: 30_000, JoinedReadyMS: 30_000},
	}
	for i, want := range wantBins {
		if listBins[i] != want {
			t.Fatalf("list joined bin %d=%+v want %+v", i, listBins[i], want)
		}
	}
	lazyBins := map[int64][]recordingHealthBin{
		20: {{Start: time.Date(2026, 5, 4, 8, 0, 0, 0, time.UTC), End: time.Date(2026, 5, 4, 9, 0, 0, 0, time.UTC)}},
		30: {
			{Start: starts[0], End: ends[0]},
			{Start: starts[1], End: ends[1]},
		},
	}
	if err := s.populateRecordingListJoinedProgressBins(context.Background(), 47, []int64{20, 30}, lazyBins); err != nil {
		t.Fatal(err)
	}
	for i, want := range wantBins {
		var got recordingHealthBin
		if i == 0 {
			got = lazyBins[20][0]
		} else {
			got = lazyBins[30][i-1]
		}
		if got.SourceDurationMS != want.SourceDurationMS || got.JoinedReadyMS != want.JoinedReadyMS {
			t.Fatalf("lazy heatmap joined bin %d=%+v want %+v", i, got, want)
		}
	}
}

func TestRecordingJoinedProgressDoesNotReusePublishedOlderGeneration(t *testing.T) {
	pool := joinedBrowserTestPool(t)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `
		INSERT INTO recordings(id,account_id,status) VALUES (60,47,'completed');
		INSERT INTO recording_clips VALUES
			(107,60,'2026-05-04 08:00:00Z','2026-05-04 08:02:00Z',NULL);
		INSERT INTO recording_joined_hours VALUES
			(205,1,47,60,'sealed','old-generation','2026-05-04',1,'2026-05-04 08:00:00Z','2026-05-04 09:00:00Z'),
			(206,2,47,60,'pending','new-generation','2026-05-04',1,'2026-05-04 08:00:00Z','2026-05-04 09:00:00Z');
		INSERT INTO recording_joined_sources(id,hour_record_id,batch_record_id,account_id,recording_id,clip_id,start_at,end_at) VALUES
			(407,205,1,47,60,107,'2026-05-04 08:00:00Z','2026-05-04 08:01:00Z'),
			(408,206,2,47,60,107,'2026-05-04 08:00:00Z','2026-05-04 08:02:00Z');
		INSERT INTO recording_joined_artifacts VALUES
			(310,205,1,47,'hour_manifest','published',now(),'old-manifest','old-manifest-version','application/json','May/Monday/hour_01.json',10,repeat('4',64),'joined/private/old-manifest.json',1),
			(311,205,1,47,'media','published',now(),'old-media','old-media-version','video/mp4','May/Monday/hour_01_part_01_0800-0801.mp4',10,repeat('5',64),'joined/private/old-media.mp4',1);
		INSERT INTO recording_joined_media_sources VALUES (311,407,1);
	`); err != nil {
		t.Fatal(err)
	}

	progress, err := (&Server{pool: pool}).recordingJoinedProgressForAccount(ctx, 47, []int64{60})
	if err != nil {
		t.Fatal(err)
	}
	got, ok := progress[60]
	if !ok || got.SourceDurationMS != 120_000 || got.JoinedReadyMS != 0 || got.Percent == nil || *got.Percent != 0 {
		t.Fatalf("new frozen generation progress=%+v present=%t want source=120000 ready=0 percent=0", got, ok)
	}
}

func TestRecordingJoinedBinProgressRejectsUnpublishedMismatchedAndPurgedSources(t *testing.T) {
	pool := joinedBrowserTestPool(t)
	seedJoinedBrowserTestData(t, pool)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `
		INSERT INTO recording_clips VALUES
			(107,40,'2026-05-04 08:10:00Z','2026-05-04 08:11:00Z',NULL),
			(108,40,'2026-05-04 08:11:00Z','2026-05-04 08:12:00Z',NULL),
			(109,40,'2026-05-04 08:12:00Z','2026-05-04 08:13:00Z',now());
		INSERT INTO recording_joined_hours VALUES
			(205,1,47,40,'sealed','missing-manifest','2026-05-04',4,'2026-05-04 08:00:00Z','2026-05-04 09:00:00Z'),
			(206,1,47,40,'sealed','unpublished-media','2026-05-04',5,'2026-05-04 08:00:00Z','2026-05-04 09:00:00Z'),
			(207,1,47,40,'sealed','purged-source','2026-05-04',6,'2026-05-04 08:00:00Z','2026-05-04 09:00:00Z');
		INSERT INTO recording_joined_sources VALUES
			(407,205,1,47,40,107),(408,206,1,47,40,108),(409,207,1,47,40,109);
		INSERT INTO recording_joined_artifacts VALUES
			(310,205,1,47,'media',NULL,now(),'media-no-manifest','','video/mp4','missing-manifest.mp4',10,repeat('4',64),'joined/private/missing-manifest.mp4',1),
			(311,206,1,47,'hour_manifest','published',now(),'manifest-unpublished-media','','application/json','unpublished-media.json',10,repeat('5',64),'joined/private/unpublished-media.json',1),
			(312,206,1,47,'media',NULL,NULL,'media-unpublished','','video/mp4','unpublished-media.mp4',10,repeat('6',64),'joined/private/unpublished-media.mp4',1),
			(313,207,1,47,'hour_manifest','published',now(),'manifest-purged','','application/json','purged.json',10,repeat('7',64),'joined/private/purged.json',1),
			(314,207,1,47,'media',NULL,now(),'media-purged','','video/mp4','purged.mp4',10,repeat('8',64),'joined/private/purged.mp4',1);
		INSERT INTO recording_joined_media_sources VALUES
			(310,407,1),(312,408,1),(314,409,1),(314,407,2);
	`); err != nil {
		t.Fatal(err)
	}
	start := time.Date(2026, 5, 4, 8, 0, 0, 0, time.UTC)
	progress, err := (&Server{pool: pool}).recordingJoinedProgressForBins(ctx, 47, []int64{40}, []time.Time{start}, []time.Time{start.Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	if len(progress) != 1 || progress[0].SourceDurationMS != 120_000 || progress[0].JoinedReadyMS != 0 {
		t.Fatalf("joined bin=%+v want source=120000 ready=0", progress)
	}
}

func TestRecordingJoinedBinProgressSingleHighVolumeRecordingIsBounded(t *testing.T) {
	pool := joinedBrowserTestPool(t)
	ctx := context.Background()
	migration, err := os.ReadFile(filepath.Join("..", "..", "..", "infra", "sql", "migrations", "0149_joined_recording_browser_indexes.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, string(migration)); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		CREATE INDEX idx_recording_clips_recording_started ON recording_clips(recording_id,clip_start_at DESC);
		INSERT INTO recordings(id,account_id,status) VALUES (70,47,'completed');
		INSERT INTO recording_clips(id,recording_id,clip_start_at,clip_end_at)
		SELECT 2000000+value,70,'2026-05-01 00:00:00Z'::timestamptz+value*interval '2 minutes','2026-05-01 00:00:00Z'::timestamptz+value*interval '2 minutes'+interval '1 minute'
		FROM generate_series(1,15000) value;
		INSERT INTO recording_joined_hours VALUES
			(270,7,47,70,'sealed','eligible','2026-05-21',1,'2026-05-20 00:00:00Z','2026-05-22 00:00:00Z'),
			(271,7,47,70,'pending','historical','2026-05-01',2,'2026-05-01 00:00:00Z','2026-05-20 00:00:00Z');
		INSERT INTO recording_joined_sources(id,hour_record_id,batch_record_id,account_id,recording_id,clip_id)
		SELECT 3000000+value,CASE WHEN value>14400 THEN 270 ELSE 271 END,7,47,70,2000000+value
		FROM generate_series(1,15000) value;
		INSERT INTO recording_joined_artifacts VALUES
			(370,270,7,47,'hour_manifest','published',now(),'manifest-70','','application/json','manifest.json',10,repeat('a',64),'joined/70/manifest.json',1),
			(371,270,7,47,'media',NULL,now(),'media-70','','video/mp4','media.mp4',10,repeat('b',64),'joined/70/media.mp4',1);
		INSERT INTO recording_joined_media_sources(artifact_id,source_id,ordinal)
		SELECT 371,3000000+value,value-14400 FROM generate_series(14401,15000) value;
		ANALYZE recordings; ANALYZE recording_clips; ANALYZE recording_joined_hours;
		ANALYZE recording_joined_sources; ANALYZE recording_joined_artifacts; ANALYZE recording_joined_media_sources;
	`); err != nil {
		t.Fatal(err)
	}
	windowEnd := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC).Add(15001 * 2 * time.Minute)
	starts, ends, recordingIDs := make([]time.Time, 12), make([]time.Time, 12), make([]int64, 12)
	for i := range starts {
		starts[i] = windowEnd.Add(time.Duration(i-12) * 2 * time.Hour)
		ends[i] = starts[i].Add(2 * time.Hour)
		recordingIDs[i] = 70
	}
	started := time.Now()
	progress, err := (&Server{pool: pool}).recordingJoinedProgressForBins(ctx, 47, recordingIDs, starts, ends)
	if err != nil {
		t.Fatal(err)
	}
	var sourceMS, readyMS int64
	for _, bin := range progress {
		sourceMS += bin.SourceDurationMS
		readyMS += bin.JoinedReadyMS
	}
	if sourceMS != 43_200_000 || readyMS != 36_000_000 {
		t.Fatalf("source_ms=%d ready_ms=%d want 43200000/36000000", sourceMS, readyMS)
	}
	rows, err := pool.Query(ctx, "EXPLAIN (ANALYZE,BUFFERS,TIMING OFF,FORMAT JSON) "+recordingJoinedProgressBinsSQL, recordingIDs, starts, ends, int64(47))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var raw []byte
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		t.Fatal("joined-bin plan returned no rows")
	}
	if err := rows.Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var document []map[string]any
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	visits := joinedBinPlanRelationVisits(document[0]["Plan"].(map[string]any), "recording_clips")
	t.Logf("single-recording joined bins elapsed=%s recording_clips_visits=%d", time.Since(started), visits)
	if visits > 17_000 {
		t.Fatalf("single-recording joined bins visited %d recording_clips rows; want one bounded historical pass", visits)
	}
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("single-recording joined bins took %s", elapsed)
	}
}

func joinedBinPlanRelationVisits(plan map[string]any, relation string) int64 {
	var total float64
	if plan["Relation Name"] == relation {
		rows, _ := plan["Actual Rows"].(float64)
		loops, _ := plan["Actual Loops"].(float64)
		removed, _ := plan["Rows Removed by Filter"].(float64)
		total += (rows + removed) * loops
	}
	children, _ := plan["Plans"].([]any)
	for _, child := range children {
		total += float64(joinedBinPlanRelationVisits(child.(map[string]any), relation))
	}
	return int64(total)
}

func TestRecordingJoinedProgressLazyEndpointAuthPublicParityAndIsolation(t *testing.T) {
	pool := joinedBrowserTestPool(t)
	seedJoinedBrowserTestData(t, pool)
	s := &Server{pool: pool, cfg: config.Config{SharedRecordingsAccountID: 47, SharedRecordingsSlug: "mit-scl", SharedRecordingsPublic: true}}

	type response struct {
		Items []recordingJoinedProgressItem `json:"items"`
	}
	call := func(t *testing.T, shared bool, path string) response {
		t.Helper()
		req := httptest.NewRequest(http.MethodGet, path, nil)
		if !shared {
			req = withPrincipal(req, accountPrincipal{AccountID: 47}, "")
		}
		rec := httptest.NewRecorder()
		if shared {
			s.router().ServeHTTP(rec, req)
		} else {
			s.handleAccountRecordingJoinedProgress(rec, req)
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
		}
		var payload response
		if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
			t.Fatal(err)
		}
		return payload
	}
	auth := call(t, false, "/api/v1/account/recordings/joined-progress?sort=joined_desc")
	shared := call(t, true, "/api/v1/shared/mit-scl/recordings/joined-progress?sort=joined_desc")
	if !reflect.DeepEqual(auth, shared) {
		t.Fatalf("auth/public mismatch:\nauth=%+v\npublic=%+v", auth, shared)
	}
	wantOrder := []int64{30, 20, 10, 40}
	if len(auth.Items) != len(wantOrder) {
		t.Fatalf("items=%d want=%d: %+v", len(auth.Items), len(wantOrder), auth.Items)
	}
	for i, item := range auth.Items {
		if item.RecordingID != wantOrder[i] || item.RecordingID == 50 {
			t.Fatalf("item[%d]=%+v want recording %d and no foreign recording", i, item, wantOrder[i])
		}
	}

	foreign := httptest.NewRecorder()
	s.router().ServeHTTP(foreign, httptest.NewRequest(http.MethodGet,
		"/api/v1/shared/mit-scl/recordings/joined-progress?recording_id=50", nil))
	if foreign.Code != http.StatusNotFound {
		t.Fatalf("foreign status=%d want=404 body=%s", foreign.Code, foreign.Body.String())
	}
}

func TestRecordingJoinedProgressExplainAnalyzeUsesBoundedIndexes(t *testing.T) {
	pool := joinedBrowserTestPool(t)
	seedJoinedBrowserTestData(t, pool)
	ctx := context.Background()
	migration, err := os.ReadFile(filepath.Join("..", "..", "..", "infra", "sql", "migrations", "0149_joined_recording_browser_indexes.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, string(migration)); err != nil {
		t.Fatal(err)
	}
	// A deployment after an operator's concurrent precreate takes this path.
	// Reapplying must validate and accept each exact existing index.
	if _, err := pool.Exec(ctx, string(migration)); err != nil {
		t.Fatalf("validate precreated joined browser indexes: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		CREATE INDEX idx_recording_clips_recording_started ON recording_clips(recording_id,clip_start_at DESC);
		INSERT INTO recordings(id,account_id,status) VALUES (60,47,'completed');
		INSERT INTO recording_joined_hours VALUES
			(205,3,47,60,'sealed','same-account-noise','2026-05-04',4,'2026-05-04 11:00:00Z','2026-05-04 12:00:00Z');
		INSERT INTO recording_clips(id,recording_id,clip_start_at,clip_end_at)
		SELECT 1000000+value,60,'2026-05-04 08:00:00Z'::timestamptz+value*interval '1 second','2026-05-04 08:00:01Z'::timestamptz+value*interval '1 second'
		FROM generate_series(1,50000) value;
		INSERT INTO recording_joined_sources(id,hour_record_id,batch_record_id,account_id,recording_id,clip_id)
		SELECT 1000000+value,205,3,47,60,1000000+value FROM generate_series(1,50000) value;
		INSERT INTO recording_joined_artifacts(id,hour_record_id,batch_record_id,account_id,artifact_kind,publication_state,published_at,etag,version_id,content_type,relative_path,expected_size_bytes,expected_sha256,object_key,ordinal)
		SELECT 1000000+value,205,3,47,'media',NULL,now(),'noise-'||value,'','video/mp4','noise/'||value||'.mp4',10,repeat('9',64),'joined/noise/'||value||'.mp4',value
		FROM generate_series(1,50000) value;
		INSERT INTO recording_joined_media_sources(artifact_id,source_id,ordinal)
		SELECT 1000000+value,1000000+value,1 FROM generate_series(1,50000) value;
		ANALYZE recordings; ANALYZE recording_clips; ANALYZE recording_joined_hours;
		ANALYZE recording_joined_sources; ANALYZE recording_joined_artifacts; ANALYZE recording_joined_media_sources;
	`); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	// Production requests every visible recording at once. Keep the large
	// recording in scope so this catches a plan that only looks bounded when
	// the noise belongs to an unrequested recording.
	rows, err := pool.Query(ctx, "EXPLAIN (ANALYZE,BUFFERS,TIMING,FORMAT TEXT) "+recordingJoinedProgressSQL, int64(47), []int64{10, 20, 30, 40, 60})
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	planLines := []string{}
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatal(err)
		}
		planLines = append(planLines, line)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	plan := strings.Join(planLines, "\n")
	t.Logf("joined progress EXPLAIN ANALYZE (%s):\n%s", time.Since(started).Round(time.Millisecond), plan)
	for _, index := range []string{"recording_joined_sources_account_recording_idx", "recording_joined_media_sources_source_artifact_idx", "recording_joined_artifacts_published_hour_idx"} {
		if !strings.Contains(plan, index) {
			t.Fatalf("plan did not use %s:\n%s", index, plan)
		}
	}
	if strings.Contains(plan, "recording_clips") {
		t.Fatalf("joined progress scanned mutable raw clips instead of the frozen source set:\n%s", plan)
	}
	bufferMatch := regexp.MustCompile(`Buffers: shared hit=([0-9]+)`).FindStringSubmatch(plan)
	if len(bufferMatch) != 2 {
		t.Fatalf("plan did not report shared buffers:\n%s", plan)
	}
	sharedHits, err := strconv.Atoi(bufferMatch[1])
	if err != nil {
		t.Fatal(err)
	}
	if sharedHits > 1000 {
		t.Fatalf("joined progress touched %d shared buffers; want at most 1000:\n%s", sharedHits, plan)
	}
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("indexed joined progress EXPLAIN took %s", elapsed)
	}
}

func TestJoinedBrowserIndexMigrationIsTransactionalAndRejectsWrongPrecreate(t *testing.T) {
	pool := joinedBrowserTestPool(t)
	ctx := context.Background()
	migration, err := os.ReadFile(filepath.Join("..", "..", "..", "infra", "sql", "migrations", "0149_joined_recording_browser_indexes.sql"))
	if err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, string(migration)); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("apply migration inside runner transaction: %v", err)
	}
	if _, err := tx.Exec(ctx, `SELECT 1`); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("migration closed its owning transaction: %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		CREATE INDEX recording_joined_sources_account_recording_idx
		ON recording_joined_sources(clip_id)
	`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, string(migration)); err == nil || !strings.Contains(err.Error(), "not the valid exact joined-browser source index") {
		t.Fatalf("wrong precreated index error=%v", err)
	}
}

func TestRecordingJoinedBrowserAuthPublicParityAndIsolation(t *testing.T) {
	pool := joinedBrowserTestPool(t)
	seedJoinedBrowserTestData(t, pool)
	s := &Server{pool: pool}
	auth := accountPrincipal{AccountID: 47, AuthType: "session"}
	public := accountPrincipal{AccountID: 47, AuthType: "shared"}
	authList, err := s.recordingJoinedFiles(context.Background(), auth, 20, []string{"media"}, 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	publicList, err := s.recordingJoinedFiles(context.Background(), public, 20, []string{"media"}, 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	if authList.Total != 2 || publicList.Total != authList.Total || len(publicList.Files) != len(authList.Files) {
		t.Fatalf("auth=%+v public=%+v", authList, publicList)
	}
	for i := range authList.Files {
		left, right := authList.Files[i], publicList.Files[i]
		if left.ArtifactID != right.ArtifactID || left.RelativePath != right.RelativePath || left.SizeBytes != right.SizeBytes {
			t.Fatalf("auth/public mismatch at %d: %+v %+v", i, left, right)
		}
		if !strings.HasPrefix(left.DownloadPath, "/api/v1/account/") || !strings.HasPrefix(right.DownloadPath, "/api/v1/shared/") {
			t.Fatalf("scoped download paths auth=%q public=%q", left.DownloadPath, right.DownloadPath)
		}
	}
	folder, err := s.recordingJoinedFiles(context.Background(), auth, 20, []string{"hour_manifest", "media"}, 100, 0)
	if err != nil || folder.Total != 3 {
		t.Fatalf("folder total=%d err=%v", folder.Total, err)
	}
	if _, err := s.recordingJoinedFiles(context.Background(), auth, 50, []string{"media"}, 100, 0); err != pgx.ErrNoRows {
		t.Fatalf("foreign recording error=%v want no rows", err)
	}
	for _, request := range []struct {
		path   string
		params map[string]string
		call   func(*Server, http.ResponseWriter, *http.Request)
	}{
		{path: "/api/v1/account/recordings/20/joined", params: map[string]string{"id": "20"}, call: func(server *Server, w http.ResponseWriter, r *http.Request) {
			server.handleAccountRecordingJoinedList(w, r)
		}},
		{path: "/api/v1/account/recordings/20/joined/302/download", params: map[string]string{"id": "20", "joinedId": "302"}, call: func(server *Server, w http.ResponseWriter, r *http.Request) {
			server.handleAccountRecordingJoinedDownload(w, r)
		}},
	} {
		req := httptest.NewRequest(http.MethodGet, request.path, nil)
		route := chi.NewRouteContext()
		for key, value := range request.params {
			route.URLParams.Add(key, value)
		}
		ctx := context.WithValue(req.Context(), chi.RouteCtxKey, route)
		ctx = context.WithValue(ctx, accountPrincipalContextKey, accountPrincipal{AccountID: 99, AuthType: "session"})
		response := httptest.NewRecorder()
		request.call(s, response, req.WithContext(ctx))
		if response.Code != http.StatusNotFound {
			t.Fatalf("foreign request %s status=%d body=%s", request.path, response.Code, response.Body.String())
		}
	}
}

type joinedBrowserRangeStore struct {
	joinedOutputStoreStub
	body       []byte
	objectKey  string
	etag       string
	versionID  string
	start, end int64
}

func (s *joinedBrowserRangeStore) OpenExactRange(_ context.Context, key, etag, versionID string, start, end int64) (io.ReadCloser, error) {
	s.objectKey, s.etag, s.versionID, s.start, s.end = key, etag, versionID, start, end
	return io.NopCloser(bytes.NewReader(s.body[start : end+1])), nil
}

func (s *joinedBrowserRangeStore) OpenExact(_ context.Context, key, etag, versionID string) (io.ReadCloser, error) {
	s.objectKey, s.etag, s.versionID, s.start, s.end = key, etag, versionID, 0, int64(len(s.body)-1)
	return io.NopCloser(bytes.NewReader(s.body)), nil
}

func TestRecordingJoinedDownloadSupportsUnversionedExactRange(t *testing.T) {
	pool := joinedBrowserTestPool(t)
	seedJoinedBrowserTestData(t, pool)
	store := &joinedBrowserRangeStore{
		joinedOutputStoreStub: joinedOutputStoreStub{head: r2.ObjectHead{ETag: "media-1", SizeBytes: 10, VersionID: ""}},
		body:                  []byte("0123456789"),
	}
	s := &Server{pool: pool, joinedOutputStorage: store}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/account/recordings/20/joined/302/download?disposition=inline", nil)
	req.Header.Set("Range", "bytes=2-5")
	route := chi.NewRouteContext()
	route.URLParams.Add("id", "20")
	route.URLParams.Add("joinedId", "302")
	ctx := context.WithValue(req.Context(), chi.RouteCtxKey, route)
	ctx = context.WithValue(ctx, accountPrincipalContextKey, accountPrincipal{AccountID: 47, AuthType: "session"})
	req = req.WithContext(ctx)
	response := httptest.NewRecorder()
	s.handleAccountRecordingJoinedDownload(response, req)
	if response.Code != http.StatusPartialContent || response.Body.String() != "2345" || response.Header().Get("Content-Range") != "bytes 2-5/10" {
		t.Fatalf("status=%d range=%q body=%q", response.Code, response.Header().Get("Content-Range"), response.Body.String())
	}
	if store.objectKey != "joined/private/media-1.mp4" || store.etag != "media-1" || store.versionID != "" || store.start != 2 || store.end != 5 {
		t.Fatalf("exact range call key=%q etag=%q version=%q range=%d-%d", store.objectKey, store.etag, store.versionID, store.start, store.end)
	}
	for _, unsupported := range []string{"items=2-5", "bytes=2-3,5-6"} {
		fullRequest := req.Clone(req.Context())
		fullRequest.Header.Set("Range", unsupported)
		full := httptest.NewRecorder()
		s.handleAccountRecordingJoinedDownload(full, fullRequest)
		if full.Code != http.StatusOK || full.Body.String() != "0123456789" || full.Header().Get("Content-Range") != "" {
			t.Fatalf("unsupported range %q status=%d content-range=%q body=%q", unsupported, full.Code, full.Header().Get("Content-Range"), full.Body.String())
		}
	}

	store.head.ETag = "changed"
	changed := httptest.NewRecorder()
	s.handleAccountRecordingJoinedDownload(changed, req)
	if changed.Code != http.StatusConflict {
		t.Fatalf("changed identity status=%d body=%s", changed.Code, changed.Body.String())
	}
}

func TestJoinedFolderIsSameOriginScopedAndRedacted(t *testing.T) {
	pool := joinedBrowserTestPool(t)
	seedJoinedBrowserTestData(t, pool)
	s := &Server{pool: pool}
	for _, test := range []struct {
		path string
		want []string
	}{
		{path: "/api/v1/account/recordings/20/joined/folder", want: []string{"20_Europe_Poland_Luban", "May", "folder=May", "2 MP4", "All joined recordings"}},
		{path: "/api/v1/account/recordings/20/joined/folder?folder=May", want: []string{"Monday", "folder=May%2FMonday"}},
		{path: "/api/v1/account/recordings/20/joined/folder?folder=May%2FMonday", want: []string{"hour_01_part_01_0800-0801.mp4", `class="type mp4">MP4`, ">View</a>", ">Download</a>"}},
	} {
		req := httptest.NewRequest(http.MethodGet, test.path, nil)
		route := chi.NewRouteContext()
		route.URLParams.Add("id", strconv.FormatInt(20, 10))
		ctx := context.WithValue(req.Context(), chi.RouteCtxKey, route)
		ctx = context.WithValue(ctx, accountPrincipalContextKey, accountPrincipal{AccountID: 47, AuthType: "session"})
		response := httptest.NewRecorder()
		s.handleAccountRecordingJoinedFolder(response, req.WithContext(ctx))
		if response.Code != http.StatusOK {
			t.Fatalf("path=%s status=%d body=%s", test.path, response.Code, response.Body.String())
		}
		for _, want := range test.want {
			if !strings.Contains(response.Body.String(), want) {
				t.Fatalf("path=%s missing %q body=%s", test.path, want, response.Body.String())
			}
		}
		for _, forbidden := range []string{"/archive", "joined/private/", "cloudflarestorage.com", "access_key", "secret"} {
			if strings.Contains(response.Body.String(), forbidden) {
				t.Fatalf("folder leaked %q", forbidden)
			}
		}
	}
}

func TestJoinedFolderRootListsCanonicalFoldersWithoutStorageAuthority(t *testing.T) {
	pool := joinedBrowserTestPool(t)
	seedJoinedBrowserTestData(t, pool)
	s := &Server{pool: pool}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/account/recordings/joined/folder", nil)
	ctx := context.WithValue(req.Context(), accountPrincipalContextKey, accountPrincipal{AccountID: 47, AuthType: "session"})
	response := httptest.NewRecorder()
	s.handleAccountJoinedFolderRoot(response, req.WithContext(ctx))
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	for _, want := range []string{
		"20_Europe_Poland_Luban", "/recordings/20/joined/folder",
		"30_Europe_Poland_Test", "Open a folder",
	} {
		if !strings.Contains(response.Body.String(), want) {
			t.Fatalf("missing %q body=%s", want, response.Body.String())
		}
	}
	for _, forbidden := range []string{"50_Europe_Poland_Foreign", "joined/private/", "cloudflarestorage.com", "access_key", "secret"} {
		if strings.Contains(response.Body.String(), forbidden) {
			t.Fatalf("root leaked %q", forbidden)
		}
	}
}

func TestPublicJoinedBrowserRoutesEndToEndWithoutR2Credentials(t *testing.T) {
	pool := joinedBrowserTestPool(t)
	seedJoinedBrowserTestData(t, pool)
	store := &joinedBrowserRangeStore{
		joinedOutputStoreStub: joinedOutputStoreStub{head: r2.ObjectHead{ETag: "media-1", SizeBytes: 10}},
		body:                  []byte("0123456789"),
	}
	s := &Server{pool: pool, joinedOutputStorage: store, cfg: config.Config{
		SharedRecordingsAccountID: 47,
		SharedRecordingsSlug:      "mit-scl",
		SharedRecordingsPublic:    true,
	}}
	router := s.router()
	for _, test := range []struct {
		path, rangeHeader string
		wantCode          int
		wantBody          string
	}{
		{path: "/api/v1/shared/mit-scl/recordings/20/joined", wantCode: http.StatusOK, wantBody: `"total":2`},
		{path: "/api/v1/shared/mit-scl/recordings/joined/folder", wantCode: http.StatusOK, wantBody: "20_Europe_Poland_Luban"},
		{path: "/api/v1/shared/mit-scl/recordings/20/joined/folder", wantCode: http.StatusOK, wantBody: "Joined clips"},
		{path: "/api/v1/shared/mit-scl/recordings/20/joined/302/download?disposition=inline", rangeHeader: "bytes=2-5", wantCode: http.StatusPartialContent, wantBody: "2345"},
		{path: "/api/v1/shared/mit-scl/recordings/50/joined", wantCode: http.StatusNotFound, wantBody: "recording not found"},
	} {
		req := httptest.NewRequest(http.MethodGet, test.path, nil)
		if test.rangeHeader != "" {
			req.Header.Set("Range", test.rangeHeader)
		}
		response := httptest.NewRecorder()
		router.ServeHTTP(response, req)
		if response.Code != test.wantCode || !strings.Contains(response.Body.String(), test.wantBody) {
			t.Fatalf("path=%s status=%d body=%s", test.path, response.Code, response.Body.String())
		}
		for _, forbidden := range []string{"joined/private/", "cloudflarestorage.com", "access_key", "secret_access"} {
			if strings.Contains(response.Body.String(), forbidden) {
				t.Fatalf("path=%s leaked %q", test.path, forbidden)
			}
		}
	}
}
