package db

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type rosterV2PG struct {
	pool   *pgxpool.Pool
	admin  *pgxpool.Pool
	schema string
}

func openRosterV2PGThrough(t *testing.T, lastMigration string) *rosterV2PG {
	t.Helper()
	databaseURL := strings.TrimSpace(os.Getenv("STOARAMA_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("campaign_v2_r1_upgrade_%d", time.Now().UnixNano())
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		admin.Close()
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams = map[string]string{"search_path": schema}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		admin.Close()
		t.Fatal(err)
	}
	sourceDir := filepath.Join("..", "..", "..", "infra", "sql", "migrations")
	partialDir := t.TempDir()
	entries, err := os.ReadDir(sourceDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") || entry.Name() > lastMigration {
			continue
		}
		body, readErr := os.ReadFile(filepath.Join(sourceDir, entry.Name()))
		if readErr != nil {
			t.Fatal(readErr)
		}
		if writeErr := os.WriteFile(filepath.Join(partialDir, entry.Name()), body, 0o600); writeErr != nil {
			t.Fatal(writeErr)
		}
	}
	if err = MigrateUp(ctx, pool, partialDir); err != nil {
		pool.Close()
		_, _ = admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		admin.Close()
		t.Fatalf("MigrateUp through %s: %v", lastMigration, err)
	}
	t.Cleanup(func() {
		pool.Close()
		_, _ = admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		admin.Close()
	})
	return &rosterV2PG{pool: pool, admin: admin, schema: schema}
}

func openRosterV2PG(t *testing.T) *rosterV2PG {
	t.Helper()
	databaseURL := strings.TrimSpace(os.Getenv("STOARAMA_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("campaign_v2_r1_%d", time.Now().UnixNano())
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		admin.Close()
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams = map[string]string{"search_path": schema}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		admin.Close()
		t.Fatal(err)
	}
	if err = MigrateUp(ctx, pool, filepath.Join("..", "..", "..", "infra", "sql", "migrations")); err != nil {
		pool.Close()
		_, _ = admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		admin.Close()
		t.Fatalf("MigrateUp with campaign roster v2 R1: %v", err)
	}
	t.Cleanup(func() {
		pool.Close()
		_, _ = admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		admin.Close()
	})
	return &rosterV2PG{pool: pool, admin: admin, schema: schema}
}

func TestRecordingCampaignV2R1MigrationIsEmptyAndPreservesLegacyProtectionView(t *testing.T) {
	f := openRosterV2PG(t)
	ctx := context.Background()
	for _, table := range []string{
		"recording_campaign_v2_campaigns", "recording_campaign_v2_policy_versions",
		"recording_campaign_v2_revisions", "recording_campaign_v2_revision_commits",
		"recording_campaign_v2_slots", "recording_campaign_v2_filled_evidence",
		"recording_campaign_v2_prospective_evidence", "recording_campaign_v2_slot_versions",
		"recording_campaign_v2_revision_slot_changes", "recording_campaign_v2_recording_occupancy",
		"recording_campaign_v2_scene_occupancy", "recording_campaign_v2_idempotency",
	} {
		var count int
		if err := f.pool.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&count); err != nil || count != 0 {
			t.Fatalf("%s is not present and empty: count=%d err=%v", table, count, err)
		}
	}
	var baselineCount int
	if err := f.pool.QueryRow(ctx, `SELECT count(*) FROM stream_source_v2_baselines`).Scan(&baselineCount); err != nil {
		t.Fatalf("source baseline table missing: %v", err)
	}
	for _, forbidden := range []string{"recording_campaign_v2_seed", "recording_campaign_v2_apply", "recording_campaign_v2_swap"} {
		var present *string
		if err := f.pool.QueryRow(ctx, `SELECT to_regprocedure($1)::text`, forbidden+"()").Scan(&present); err != nil || present != nil {
			t.Fatalf("forbidden writer function %s present=%v err=%v", forbidden, present, err)
		}
	}
	var unpinnedFunctions int
	if err := f.pool.QueryRow(ctx, `
		SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
		WHERE n.nspname=current_schema() AND (
			p.proname LIKE 'recording_campaign_v2_%' OR p.proname LIKE 'validate_recording_campaign_v2_%'
			OR p.proname IN('reject_recording_campaign_v2_mutation','freeze_stream_source_v2_baseline','validate_stream_source_revision_v2_append','enforce_recording_campaign_v2_source_stream_fence','recording_campaign_v2_fence_presentation_admission','fence_recording_presentation_job_against_campaign_v2','lock_recording_campaign_protection'))
		AND NOT COALESCE(array_to_string(p.proconfig,','),'') LIKE 'search_path='||current_schema()||', pg_catalog%'`).Scan(&unpinnedFunctions); err != nil || unpinnedFunctions != 0 {
		t.Fatalf("R1 functions with caller-controlled search_path=%d err=%v", unpinnedFunctions, err)
	}

	var account, actor, stream, recording, track int64
	if err := f.pool.QueryRow(ctx, `INSERT INTO accounts(email,name,status,role) VALUES('legacy-roster@example.test','legacy','active','admin') RETURNING id`).Scan(&account); err != nil {
		t.Fatal(err)
	}
	if err := f.pool.QueryRow(ctx, `INSERT INTO users(email,name,is_operator,email_verified_at) VALUES('legacy-operator@example.test','operator',true,now()) RETURNING id`).Scan(&actor); err != nil {
		t.Fatal(err)
	}
	if err := f.pool.QueryRow(ctx, `INSERT INTO streams(provider,external_id,name,slug,source_url,source_page_url,capture_type,source_family,execution_class,capture_family,expected_fps,recording_state) VALUES('test','legacy','legacy','legacy-roster-v2','https://example.test/legacy.m3u8','https://example.test/legacy','hls','video_manifest','video_live','continuous_video',30,'on') RETURNING id`).Scan(&stream); err != nil {
		t.Fatal(err)
	}
	if err := f.pool.QueryRow(ctx, `SELECT count(*) FROM stream_source_v2_baselines WHERE stream_id=$1`, stream).Scan(&baselineCount); err != nil || baselineCount != 1 {
		t.Fatalf("new stream baseline count=%d err=%v", baselineCount, err)
	}
	destination := seedRosterV2Destination(t, f.pool, account)
	if err := f.pool.QueryRow(ctx, `INSERT INTO recordings(account_id,storage_destination_id,name,stream_url,stream_id,source_kind,cron_timezone,clip_duration_sec,mode,daily_window_start,daily_window_end,active_weekdays,target_fps,status,start_at,capture_via) VALUES($1,$2,'legacy','https://example.test/legacy.m3u8',$3,'hls_live','UTC',60,'continuous','08:00','20:00',127,NULL,'active',now()-interval '1 hour','cloud') RETURNING id`, account, destination, stream).Scan(&recording); err != nil {
		t.Fatal(err)
	}
	if err := f.pool.QueryRow(ctx, `INSERT INTO recording_campaign_tracks(account_id,campaign_key,label,deadline_at,target_count,grade_floor,created_by_user_id) VALUES($1,'legacy-r1','legacy',now()+interval '1 day',1,'GOOD',$2) RETURNING id`, account, actor).Scan(&track); err != nil {
		t.Fatal(err)
	}
	if _, err := f.pool.Exec(ctx, `INSERT INTO recording_campaign_roster_entries(track_id,recording_id,stream_id,scene_identity_sha256,role,rank,status,reason_codes,effective_at,decision_at,evidence_observed_at,evidence_sha256,updated_by_user_id) VALUES($1,$2,$3,repeat('a',64),'primary',1,'protect',ARRAY['legacy'],now(),now(),now(),repeat('b',64),$4)`, track, recording, stream, actor); err != nil {
		t.Fatal(err)
	}
	if _, err := f.pool.Exec(ctx, `SELECT transition_recording_campaign_track($1,'active',ARRAY['legacy'], $2,now())`, track, actor); err != nil {
		t.Fatal(err)
	}
	var protected int
	if err := f.pool.QueryRow(ctx, `SELECT count(*) FROM protected_campaign_recordings WHERE account_id=$1 AND recording_id=$2`, account, recording).Scan(&protected); err != nil || protected != 1 {
		t.Fatalf("legacy protection changed: count=%d err=%v", protected, err)
	}

	var oidBefore, oidAfter uint32
	var ownerBefore, ownerAfter string
	var optionsBefore, optionsAfter []string
	if err := f.pool.QueryRow(ctx, `SELECT c.oid,c.relowner::regrole::text,COALESCE(c.reloptions,'{}') FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=current_schema() AND c.relname='protected_campaign_recordings'`).Scan(&oidBefore, &ownerBefore, &optionsBefore); err != nil {
		t.Fatal(err)
	}
	rollback, err := os.ReadFile(filepath.Join("..", "..", "..", "infra", "sql", "rollbacks", "0136_recording_campaign_roster_v2.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.pool.Exec(ctx, string(rollback)); err != nil {
		t.Fatal(err)
	}
	if err = f.pool.QueryRow(ctx, `SELECT c.oid,c.relowner::regrole::text,COALESCE(c.reloptions,'{}') FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=current_schema() AND c.relname='protected_campaign_recordings'`).Scan(&oidAfter, &ownerAfter, &optionsAfter); err != nil {
		t.Fatal(err)
	}
	if oidAfter != oidBefore || ownerAfter != ownerBefore || strings.Join(optionsAfter, ",") != strings.Join(optionsBefore, ",") {
		t.Fatalf("rollback changed view metadata oid=%d/%d owner=%q/%q options=%v/%v", oidBefore, oidAfter, ownerBefore, ownerAfter, optionsBefore, optionsAfter)
	}
	var definition string
	if err = f.pool.QueryRow(ctx, `SELECT pg_get_viewdef('protected_campaign_recordings'::regclass,true)`).Scan(&definition); err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{"recording_campaign_tracks", "recording_campaign_roster_entries", "protect", "probation"} {
		if !strings.Contains(definition, required) {
			t.Fatalf("rollback view missing %q: %s", required, definition)
		}
	}
	if strings.Contains(definition, "recording_campaign_v2") {
		t.Fatalf("rollback retained v2 branch: %s", definition)
	}
}

func seedRosterV2Destination(t *testing.T, pool *pgxpool.Pool, account int64) int64 {
	t.Helper()
	var destination int64
	if err := pool.QueryRow(context.Background(), `
		WITH inserted AS (
			INSERT INTO storage_destinations(account_id,name,provider,endpoint,region,bucket,key_prefix,access_key_id,secret_access_key_enc,status,verified_at,managed,shared)
			VALUES($1::bigint,'r1','r2_managed','https://storage.example.test','auto','r1','','access',decode('00','hex'),'verified',now(),true,false)
			ON CONFLICT (account_id) WHERE managed DO NOTHING
			RETURNING id
		)
		SELECT id FROM inserted
		UNION ALL
		SELECT id FROM storage_destinations WHERE account_id=$1::bigint AND managed
		LIMIT 1`, account).Scan(&destination); err != nil {
		t.Fatal(err)
	}
	return destination
}

func rosterV2Digest(domain string, fields ...string) string {
	b := strings.Builder{}
	b.WriteString(domain)
	b.WriteByte('\n')
	for _, field := range fields {
		fmt.Fprintf(&b, "%d:%s\n", len([]byte(field)), field)
	}
	return fmt.Sprintf("%x", sha256.Sum256([]byte(b.String())))
}

func rosterV2RosterDigest(items map[int]string) string {
	ordinals := make([]int, 0, len(items))
	for ordinal := range items {
		ordinals = append(ordinals, ordinal)
	}
	sort.Ints(ordinals)
	slotDigests := make([]string, 0, len(ordinals))
	for _, ordinal := range ordinals {
		slotDigests = append(slotDigests, rosterV2Digest("campaign-roster-v2-r1-slot", strings.Split(items[ordinal], "|")...))
	}
	return rosterV2Digest("campaign-roster-v2-r1-roster", slotDigests...)
}

func rosterV2ReasonDigest(reasons ...string) string {
	return rosterV2Digest("campaign-roster-v2-r1-reasons", reasons...)
}

func TestRecordingCampaignV2R1SourceLineageIsAppendOnlyAndBaselineFailsClosed(t *testing.T) {
	f := openRosterV2PG(t)
	ctx := context.Background()
	var stream int64
	if err := f.pool.QueryRow(ctx, `INSERT INTO streams(provider,external_id,name,slug,source_url,source_page_url,capture_type,source_family,execution_class,capture_family,expected_fps,recording_state) VALUES('test','lineage','lineage','r1-lineage','https://example.test/a.m3u8','https://example.test/a','hls','video_manifest','video_live','continuous_video',30,'on') RETURNING id`).Scan(&stream); err != nil {
		t.Fatal(err)
	}
	if _, err := f.pool.Exec(ctx, `UPDATE streams SET source_url='https://example.test/a-v2.m3u8' WHERE id=$1`, stream); err != nil {
		t.Fatal(err)
	}
	var first int64
	if err := f.pool.QueryRow(ctx, `INSERT INTO stream_source_revisions(stream_id,actor,reason,previous_source_url,new_source_url,previous_source_page_url,new_source_page_url,previous_source_family,new_source_family,previous_capture_type,new_capture_type,previous_execution_class,new_execution_class) SELECT id,'test','baseline','https://example.test/a.m3u8',source_url,source_page_url,source_page_url,source_family,source_family,capture_type,capture_type,execution_class,execution_class FROM streams WHERE id=$1 RETURNING id`, stream).Scan(&first); err != nil {
		t.Fatal(err)
	}
	if _, err := f.pool.Exec(ctx, `UPDATE stream_source_revisions SET reason='forged' WHERE id=$1`, first); err == nil {
		t.Fatal("source revision update bypass succeeded")
	}
	if _, err := f.pool.Exec(ctx, `DELETE FROM stream_source_revisions WHERE id=$1`, first); err == nil {
		t.Fatal("source revision delete bypass succeeded")
	}
	if _, err := f.pool.Exec(ctx, `TRUNCATE stream_source_revisions`); err == nil {
		t.Fatal("source revision truncate bypass succeeded")
	}
	if _, err := f.pool.Exec(ctx, `INSERT INTO stream_source_revisions(stream_id,actor,reason,previous_source_url,new_source_url,previous_source_page_url,new_source_page_url,previous_source_family,new_source_family,previous_capture_type,new_capture_type,previous_execution_class,new_execution_class) SELECT id,'test','stale','wrong',source_url,source_page_url,source_page_url,source_family,source_family,capture_type,capture_type,execution_class,execution_class FROM streams WHERE id=$1`, stream); err == nil {
		t.Fatal("source revision branch/stale predecessor succeeded")
	}
	var firstInvalidStream int64
	if err := f.pool.QueryRow(ctx, `INSERT INTO streams(provider,external_id,name,slug,source_url,source_page_url,capture_type,source_family,execution_class,capture_family,expected_fps,recording_state) VALUES('test','lineage-first-invalid','lineage first invalid','r1-lineage-first-invalid','https://example.test/first.m3u8','https://example.test/first','hls','video_manifest','video_live','continuous_video',30,'on') RETURNING id`).Scan(&firstInvalidStream); err != nil {
		t.Fatal(err)
	}
	if _, err := f.pool.Exec(ctx, `INSERT INTO stream_source_revisions(stream_id,actor,reason,previous_source_url,new_source_url,previous_source_page_url,new_source_page_url,previous_source_family,new_source_family,previous_capture_type,new_capture_type,previous_execution_class,new_execution_class) SELECT id,'test','forged first','https://attacker.test/previous',source_url,source_page_url,source_page_url,source_family,source_family,capture_type,capture_type,execution_class,execution_class FROM streams WHERE id=$1`, firstInvalidStream); err == nil || !strings.Contains(err.Error(), "frozen baseline") {
		t.Fatalf("forged first source predecessor err=%v", err)
	}
	if _, err := f.pool.Exec(ctx, `INSERT INTO stream_source_revisions(stream_id,actor,reason,previous_source_url,new_source_url,previous_source_page_url,new_source_page_url,previous_source_family,new_source_family,previous_capture_type,new_capture_type,previous_execution_class,new_execution_class) SELECT id,'test','forged no-change',source_url,source_url,source_page_url,source_page_url,source_family,source_family,capture_type,capture_type,execution_class,execution_class FROM streams WHERE id=$1`, firstInvalidStream); err == nil || !strings.Contains(err.Error(), "actual source change") {
		t.Fatalf("forged no-change first source revision err=%v", err)
	}

	// A coherent historical row whose terminal no longer matches a later
	// catalog classification remains preserved but cannot be used as current
	// evidence. R1 does not rewrite that history.
	if _, err := f.pool.Exec(ctx, `UPDATE streams SET execution_class='youtube_direct' WHERE id=$1`, stream); err != nil {
		t.Fatal(err)
	}
	var mismatch bool
	if err := f.pool.QueryRow(ctx, `SELECT (r.new_source_url,r.new_source_page_url,r.new_source_family,r.new_capture_type,r.new_execution_class) IS DISTINCT FROM (s.source_url,s.source_page_url,s.source_family,s.capture_type,s.execution_class) FROM stream_source_revisions r JOIN streams s ON s.id=r.stream_id WHERE r.id=$1`, first).Scan(&mismatch); err != nil || !mismatch {
		t.Fatalf("terminal/current divergence not preserved mismatch=%t err=%v", mismatch, err)
	}

	var raceStream int64
	if err := f.pool.QueryRow(ctx, `INSERT INTO streams(provider,external_id,name,slug,source_url,source_page_url,capture_type,source_family,execution_class,capture_family,expected_fps,recording_state) VALUES('test','lineage-race','lineage race','r1-lineage-race','https://example.test/race-baseline.m3u8','https://example.test/race','hls','video_manifest','video_live','continuous_video',30,'on') RETURNING id`).Scan(&raceStream); err != nil {
		t.Fatal(err)
	}
	if _, err := f.pool.Exec(ctx, `UPDATE streams SET source_url='https://example.test/race-a.m3u8' WHERE id=$1`, raceStream); err != nil {
		t.Fatal(err)
	}
	if _, err := f.pool.Exec(ctx, `INSERT INTO stream_source_revisions(stream_id,actor,reason,previous_source_url,new_source_url,previous_source_page_url,new_source_page_url,previous_source_family,new_source_family,previous_capture_type,new_capture_type,previous_execution_class,new_execution_class) SELECT id,'test','initial','https://example.test/race-baseline.m3u8',source_url,source_page_url,source_page_url,source_family,source_family,capture_type,capture_type,execution_class,execution_class FROM streams WHERE id=$1`, raceStream); err != nil {
		t.Fatal(err)
	}
	if _, err := f.pool.Exec(ctx, `UPDATE streams SET source_url='https://example.test/race-b.m3u8' WHERE id=$1`, raceStream); err != nil {
		t.Fatal(err)
	}
	firstTx, err := f.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer firstTx.Rollback(ctx)
	insertNext := func(tx pgx.Tx) error {
		_, insertErr := tx.Exec(ctx, `INSERT INTO stream_source_revisions(stream_id,actor,reason,previous_source_url,new_source_url,previous_source_page_url,new_source_page_url,previous_source_family,new_source_family,previous_capture_type,new_capture_type,previous_execution_class,new_execution_class) VALUES($1,'test','race','https://example.test/race-a.m3u8','https://example.test/race-b.m3u8','https://example.test/race','https://example.test/race','video_manifest','video_manifest','hls','hls','video_live','video_live')`, raceStream)
		return insertErr
	}
	if err = insertNext(firstTx); err != nil {
		t.Fatal(err)
	}
	pidCh := make(chan int32, 1)
	resultCh := make(chan error, 1)
	go func() {
		secondTx, beginErr := f.pool.Begin(ctx)
		if beginErr != nil {
			resultCh <- beginErr
			return
		}
		defer secondTx.Rollback(ctx)
		var pid int32
		if beginErr = secondTx.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&pid); beginErr != nil {
			resultCh <- beginErr
			return
		}
		pidCh <- pid
		insertErr := insertNext(secondTx)
		if insertErr == nil {
			insertErr = secondTx.Commit(ctx)
		}
		resultCh <- insertErr
	}()
	waitForRosterV2PostgresLock(t, f.pool, <-pidCh)
	if err = firstTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err = <-resultCh; err == nil || !strings.Contains(err.Error(), "does not extend current lineage") {
		t.Fatalf("concurrent stale source append err=%v", err)
	}
	var revisionCount int
	if err = f.pool.QueryRow(ctx, `SELECT count(*) FROM stream_source_revisions WHERE stream_id=$1`, raceStream).Scan(&revisionCount); err != nil || revisionCount != 2 {
		t.Fatalf("concurrent append count=%d err=%v", revisionCount, err)
	}

	// A concurrent job heartbeat/state transition is serialized behind the
	// same exact job-row lock used by roster activation's negative C1 check.
	// C1 admission insertion races are covered separately below.
}

func TestRecordingCampaignV2R1DeployedUpgradeValidatesExistingSourceLineage(t *testing.T) {
	for _, tc := range []struct {
		name      string
		broken    bool
		equalTime bool
	}{
		{name: "coherent terminal divergence is preserved"},
		{name: "branched legacy history blocks migration", broken: true},
		{name: "equal timestamp legacy history blocks migration", broken: true, equalTime: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := openRosterV2PGThrough(t, "0135_recording_presentation_evidence_v2.sql")
			ctx := context.Background()
			var legacyDefinition, legacyOwner string
			var legacyOID uint32
			var legacyOptions []string
			if err := f.pool.QueryRow(ctx, `SELECT pg_get_viewdef(c.oid,true),c.oid,c.relowner::regrole::text,COALESCE(c.reloptions,'{}') FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=current_schema() AND c.relname='protected_campaign_recordings'`).Scan(&legacyDefinition, &legacyOID, &legacyOwner, &legacyOptions); err != nil {
				t.Fatal(err)
			}
			var stream int64
			if err := f.pool.QueryRow(ctx, `INSERT INTO streams(provider,external_id,name,slug,source_url,source_page_url,capture_type,source_family,execution_class,capture_family,expected_fps,recording_state) VALUES('test',$1,$1,$2,'https://example.test/final.m3u8','https://example.test/page','hls','video_manifest','video_live','continuous_video',30,'on') RETURNING id`, "upgrade-"+strings.ReplaceAll(tc.name, " ", "-"), "upgrade-"+fmt.Sprint(time.Now().UnixNano())).Scan(&stream); err != nil {
				t.Fatal(err)
			}
			firstAge := "2 minutes"
			if tc.equalTime {
				firstAge = "1 minute"
			}
			if _, err := f.pool.Exec(ctx, `INSERT INTO stream_source_revisions(stream_id,actor,reason,previous_source_url,new_source_url,previous_source_page_url,new_source_page_url,previous_source_family,new_source_family,previous_capture_type,new_capture_type,previous_execution_class,new_execution_class,created_at) VALUES($1,'test','first','https://example.test/original.m3u8','https://example.test/mid.m3u8','https://example.test/page','https://example.test/page','video_manifest','video_manifest','hls','hls','video_live','video_live',now()-$2::interval)`, stream, firstAge); err != nil {
				t.Fatal(err)
			}
			previous := "https://example.test/mid.m3u8"
			if tc.broken && !tc.equalTime {
				previous = "https://example.test/branched.m3u8"
			}
			if _, err := f.pool.Exec(ctx, `INSERT INTO stream_source_revisions(stream_id,actor,reason,previous_source_url,new_source_url,previous_source_page_url,new_source_page_url,previous_source_family,new_source_family,previous_capture_type,new_capture_type,previous_execution_class,new_execution_class,created_at) VALUES($1,'test','second',$2,'https://example.test/final.m3u8','https://example.test/page','https://example.test/page','video_manifest','video_manifest','hls','hls','video_live','video_live',now()-interval '1 minute')`, stream, previous); err != nil {
				t.Fatal(err)
			}
			if !tc.broken {
				// A coherent chain may have a current catalog divergence. R1 keeps
				// the history but later evidence acceptance fails closed.
				if _, err := f.pool.Exec(ctx, `UPDATE streams SET execution_class='youtube_direct' WHERE id=$1`, stream); err != nil {
					t.Fatal(err)
				}
			}
			err := MigrateUp(ctx, f.pool, filepath.Join("..", "..", "..", "infra", "sql", "migrations"))
			if tc.broken {
				if err == nil || !strings.Contains(err.Error(), "structurally incoherent") {
					t.Fatalf("broken legacy lineage migration err=%v", err)
				}
				var exists *string
				if scanErr := f.pool.QueryRow(ctx, `SELECT to_regclass('recording_campaign_v2_campaigns')::text`).Scan(&exists); scanErr != nil || exists != nil {
					t.Fatalf("failed migration left roster schema exists=%v err=%v", exists, scanErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("coherent deployed upgrade: %v", err)
			}
			var mismatch bool
			if err = f.pool.QueryRow(ctx, `SELECT (r.new_source_url,r.new_source_page_url,r.new_source_family,r.new_capture_type,r.new_execution_class) IS DISTINCT FROM (s.source_url,s.source_page_url,s.source_family,s.capture_type,s.execution_class) FROM stream_source_revisions r JOIN streams s ON s.id=r.stream_id WHERE r.stream_id=$1 ORDER BY r.created_at DESC,r.id DESC LIMIT 1`, stream).Scan(&mismatch); err != nil || !mismatch {
				t.Fatalf("upgrade rewrote or hid terminal divergence mismatch=%t err=%v", mismatch, err)
			}
			rollback, readErr := os.ReadFile(filepath.Join("..", "..", "..", "infra", "sql", "rollbacks", "0136_recording_campaign_roster_v2.sql"))
			if readErr != nil {
				t.Fatal(readErr)
			}
			if _, err = f.pool.Exec(ctx, string(rollback)); err != nil {
				t.Fatal(err)
			}
			var restoredDefinition, restoredOwner string
			var restoredOID uint32
			var restoredOptions []string
			if err = f.pool.QueryRow(ctx, `SELECT pg_get_viewdef(c.oid,true),c.oid,c.relowner::regrole::text,COALESCE(c.reloptions,'{}') FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=current_schema() AND c.relname='protected_campaign_recordings'`).Scan(&restoredDefinition, &restoredOID, &restoredOwner, &restoredOptions); err != nil {
				t.Fatal(err)
			}
			if restoredDefinition != legacyDefinition || restoredOID != legacyOID || restoredOwner != legacyOwner || strings.Join(restoredOptions, ",") != strings.Join(legacyOptions, ",") {
				t.Fatalf("rollback is not legacy-view equivalent definition=%t oid=%d/%d owner=%q/%q options=%v/%v", restoredDefinition == legacyDefinition, legacyOID, restoredOID, legacyOwner, restoredOwner, legacyOptions, restoredOptions)
			}
		})
	}
}

func TestRecordingCampaignV2R1BaselineSnapshotFencesConcurrentStreamInsert(t *testing.T) {
	f := openRosterV2PGThrough(t, "0135_recording_presentation_evidence_v2.sql")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Hold a lock needed by the first post-stream-lock statement. This leaves
	// migration 0136 observably holding SHARE ROW EXCLUSIVE on streams while it
	// waits, so a concurrent stream INSERT must wait for trigger installation.
	blocker, err := f.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback(context.Background())
	if _, err = blocker.Exec(ctx, `LOCK TABLE recordings IN ROW EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}
	migrateResult := make(chan error, 1)
	go func() {
		migrateResult <- MigrateUp(ctx, f.pool, filepath.Join("..", "..", "..", "infra", "sql", "migrations"))
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var locked bool
		err = f.pool.QueryRow(ctx, `
			SELECT EXISTS(
				SELECT 1 FROM pg_locks l
				JOIN pg_class c ON c.oid=l.relation
				JOIN pg_namespace n ON n.oid=c.relnamespace
				WHERE n.nspname=current_schema() AND c.relname='streams'
				  AND l.mode='ShareRowExclusiveLock' AND l.granted
			)`).Scan(&locked)
		if err != nil {
			t.Fatal(err)
		}
		if locked {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("migration never acquired the stream baseline fence")
		}
		time.Sleep(10 * time.Millisecond)
	}

	pidCh := make(chan int32, 1)
	insertResult := make(chan struct {
		id  int64
		err error
	}, 1)
	go func() {
		conn, acquireErr := f.pool.Acquire(ctx)
		if acquireErr != nil {
			insertResult <- struct {
				id  int64
				err error
			}{err: acquireErr}
			return
		}
		defer conn.Release()
		var pid int32
		if acquireErr = conn.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&pid); acquireErr != nil {
			insertResult <- struct {
				id  int64
				err error
			}{err: acquireErr}
			return
		}
		pidCh <- pid
		var stream int64
		acquireErr = conn.QueryRow(ctx, `INSERT INTO streams(provider,external_id,name,slug,source_url,source_page_url,capture_type,source_family,execution_class,capture_family,expected_fps,recording_state) VALUES('test','baseline-race','baseline race',$1,'https://example.test/baseline-race.m3u8','https://example.test/baseline-race','hls','video_manifest','video_live','continuous_video',30,'on') RETURNING id`, "baseline-race-"+fmt.Sprint(time.Now().UnixNano())).Scan(&stream)
		insertResult <- struct {
			id  int64
			err error
		}{id: stream, err: acquireErr}
	}()
	waitForRosterV2PostgresLock(t, f.pool, <-pidCh)
	if err = blocker.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err = <-migrateResult; err != nil {
		t.Fatalf("migration after stream fence: %v", err)
	}
	inserted := <-insertResult
	if inserted.err != nil {
		t.Fatalf("stream insert after migration fence: %v", inserted.err)
	}
	var baselineCount, missingCount int
	if err = f.pool.QueryRow(ctx, `SELECT count(*) FROM stream_source_v2_baselines WHERE stream_id=$1`, inserted.id).Scan(&baselineCount); err != nil || baselineCount != 1 {
		t.Fatalf("racing stream baseline count=%d err=%v", baselineCount, err)
	}
	if err = f.pool.QueryRow(ctx, `SELECT count(*) FROM streams s LEFT JOIN stream_source_v2_baselines b ON b.stream_id=s.id WHERE b.stream_id IS NULL`).Scan(&missingCount); err != nil || missingCount != 0 {
		t.Fatalf("streams missing frozen baseline=%d err=%v", missingCount, err)
	}
}

func TestRecordingCampaignV2R1RejectsAmbiguousOrTemporarySearchPath(t *testing.T) {
	f := openRosterV2PGThrough(t, "0135_recording_presentation_evidence_v2.sql")
	ctx := context.Background()
	decoy := fmt.Sprintf("campaign_v2_decoy_%d", time.Now().UnixNano())
	if _, err := f.admin.Exec(ctx, "CREATE SCHEMA "+decoy); err != nil {
		t.Fatal(err)
	}
	defer f.admin.Exec(context.Background(), "DROP SCHEMA "+decoy+" CASCADE")
	body, err := os.ReadFile(filepath.Join("..", "..", "..", "infra", "sql", "migrations", "0136_recording_campaign_roster_v2.sql"))
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{decoy + "," + f.schema, "pg_temp," + f.schema} {
		cfg, parseErr := pgxpool.ParseConfig(strings.TrimSpace(os.Getenv("STOARAMA_TEST_DATABASE_URL")))
		if parseErr != nil {
			t.Fatal(parseErr)
		}
		cfg.ConnConfig.RuntimeParams = map[string]string{"search_path": path}
		pool, openErr := pgxpool.NewWithConfig(ctx, cfg)
		if openErr != nil {
			t.Fatal(openErr)
		}
		conn, acquireErr := pool.Acquire(ctx)
		if acquireErr != nil {
			pool.Close()
			t.Fatal(acquireErr)
		}
		if strings.HasPrefix(path, "pg_temp,") {
			if _, openErr = conn.Exec(ctx, `CREATE TEMP TABLE roster_v2_shadow(id integer)`); openErr != nil {
				conn.Release()
				pool.Close()
				t.Fatal(openErr)
			}
		}
		_, execErr := conn.Exec(ctx, string(body))
		conn.Release()
		pool.Close()
		if execErr == nil || !strings.Contains(execErr.Error(), "one controlled non-temporary schema") {
			t.Fatalf("ambiguous search_path %q err=%v", path, execErr)
		}
	}
	for _, schema := range []string{f.schema, decoy} {
		var present *string
		if err = f.admin.QueryRow(ctx, `SELECT to_regclass($1)::text`, schema+".recording_campaign_v2_campaigns").Scan(&present); err != nil || present != nil {
			t.Fatalf("failed search_path migration created authority in %s: %v err=%v", schema, present, err)
		}
	}
}

func TestRecordingCampaignV2R1RejectsUnsealedForksAndDirectMutation(t *testing.T) {
	f := openRosterV2PG(t)
	ctx := context.Background()
	account, actor := seedRosterV2Actor(t, f.pool, "append")
	tx, err := f.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	var campaign, policy int64
	var createdAt time.Time
	if err = tx.QueryRow(ctx, `INSERT INTO recording_campaign_v2_campaigns(account_id,campaign_key,kind,created_by_user_id) VALUES($1,'append-only','reserve',$2) RETURNING id,created_at`, account, actor).Scan(&campaign, &createdAt); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().UTC().Add(48 * time.Hour).Truncate(time.Microsecond)
	policyDigest := rosterV2Digest("campaign-roster-v2-r1-policy", "reserve", "1", "1", fmt.Sprint(deadline.UnixMicro()), "GOOD", "0", "3600", "operational-v1", "operational-disjoint-v1", "campaign-roster-v2-r1")
	if err = tx.QueryRow(ctx, `INSERT INTO recording_campaign_v2_policy_versions(campaign_id,account_id,version,policy_schema,target_count,deadline_at,grade_floor,required_consecutive_windows,evidence_freshness_seconds,selection_group,overlap_policy,definition_sha256) VALUES($1,$2,1,'campaign-roster-v2-r1',1,$3,'GOOD',0,3600,'operational-v1','operational-disjoint-v1',$4) RETURNING id`, campaign, account, deadline, policyDigest).Scan(&policy); err != nil {
		t.Fatal(err)
	}
	rosterDigest := rosterV2RosterDigest(nil)
	reasonDigest := rosterV2ReasonDigest("campaign_created")
	planDigest := rosterV2Digest("campaign-roster-v2-r1-plan", fmt.Sprint(campaign), "1", "create", "", "draft", policyDigest, reasonDigest, rosterDigest)
	requestDigest := rosterV2Digest("campaign-roster-v2-r1-request", fmt.Sprint(campaign), fmt.Sprint(account), "1", "create", "", "draft", policyDigest, "create", reasonDigest, planDigest)
	var revision int64
	if err = tx.QueryRow(ctx, `INSERT INTO recording_campaign_v2_revisions(campaign_id,account_id,revision_number,decision_uuid,event_type,from_state,to_state,policy_version_id,actor_user_id,actor_role,decided_at,request_sha256,plan_sha256,idempotency_key,reason_codes) VALUES($1,$2,1,$3,'create',NULL,'draft',$4,$5,'owner',$6,$7,$8,'create',ARRAY['campaign_created']) RETURNING id`, campaign, account, uuid.New(), policy, actor, createdAt, requestDigest, planDigest).Scan(&revision); err != nil {
		t.Fatal(err)
	}
	response := []byte(fmt.Sprintf(`{"campaign_id":%d,"revision":1}`, campaign))
	if _, err = tx.Exec(ctx, `INSERT INTO recording_campaign_v2_idempotency(account_id,campaign_id,idempotency_key,request_sha256,plan_sha256,response_sha256,response_bytes,revision_id) VALUES($1,$2,'create',$3,$4,encode(sha256($5),'hex'),$5,$6)`, account, campaign, requestDigest, planDigest, response, revision); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(ctx, `INSERT INTO recording_campaign_v2_revision_commits(revision_id,campaign_id,account_id,roster_sha256,filled_count,prospective_count,vacant_count,replace_count,protected_count) VALUES($1,$2,$3,$4,0,0,0,0,0)`, revision, campaign, account, rosterDigest); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatalf("valid sealed draft: %v", err)
	}

	for label, check := range map[string]struct {
		query string
		args  []any
	}{
		"campaign update":      {`UPDATE recording_campaign_v2_campaigns SET campaign_key='forged' WHERE id=$1`, []any{campaign}},
		"revision delete":      {`DELETE FROM recording_campaign_v2_revisions WHERE campaign_id=$1`, []any{campaign}},
		"idempotency truncate": {`TRUNCATE recording_campaign_v2_idempotency`, nil},
	} {
		if _, err = f.pool.Exec(ctx, check.query, check.args...); err == nil {
			t.Fatalf("%s bypass succeeded", label)
		}
	}
	if _, err = f.pool.Exec(ctx, `INSERT INTO recording_campaign_v2_revisions(campaign_id,account_id,revision_number,predecessor_revision_id,decision_uuid,event_type,from_state,to_state,policy_version_id,actor_user_id,actor_role,request_sha256,plan_sha256,idempotency_key,reason_codes) VALUES($1,$2,3,$3,$4,'retire','draft','retired',$5,$6,'owner',repeat('a',64),repeat('b',64),'skip',ARRAY['campaign_retired'])`, campaign, account, revision, uuid.New(), policy, actor); err == nil {
		t.Fatal("skipped campaign revision succeeded")
	}
}

func seedRosterV2Actor(t *testing.T, pool *pgxpool.Pool, suffix string) (int64, int64) {
	t.Helper()
	ctx := context.Background()
	var account, actor int64
	if err := pool.QueryRow(ctx, `INSERT INTO accounts(email,name,status,role) VALUES($1,$2,'active','admin') RETURNING id`, "roster-"+suffix+"@example.test", "roster-"+suffix).Scan(&account); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `INSERT INTO users(email,name,is_operator,email_verified_at) VALUES($1,$2,false,now()) RETURNING id`, "actor-"+suffix+"@example.test", "actor-"+suffix).Scan(&actor); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO memberships(user_id,org_id,role,accepted_at) VALUES($1,$2,'owner',now())`, actor, account); err != nil {
		t.Fatal(err)
	}
	return account, actor
}

type rosterV2Subject struct {
	streamID         int64
	recordingID      int64
	sceneEvidenceID  int64
	sceneSHA         string
	sceneEvidenceSHA string
	frameID          int64
	mediaID          int64
	frameSHA         string
	frameCapturedAt  time.Time
	sceneVerifiedAt  time.Time
	sourceURL        string
	pageURL          string
	provider         string
	externalID       string
	sourceFamily     string
	captureType      string
	executionClass   string
}

func seedRosterV2Subjects(t *testing.T, pool *pgxpool.Pool, account, actor int64, suffix string, count int, filled bool) []rosterV2Subject {
	return seedRosterV2SubjectsAt(t, pool, account, actor, suffix, count, filled, "", -time.Minute, 0)
}

func seedRosterV2SubjectsWithScene(t *testing.T, pool *pgxpool.Pool, account, actor int64, suffix string, count int, filled bool, sceneOverride string) []rosterV2Subject {
	return seedRosterV2SubjectsAt(t, pool, account, actor, suffix, count, filled, sceneOverride, -time.Minute, 0)
}

func seedRosterV2SubjectsAt(t *testing.T, pool *pgxpool.Pool, account, actor int64, suffix string, count int, filled bool, sceneOverride string, capturedOffset, verifiedOffset time.Duration) []rosterV2Subject {
	t.Helper()
	ctx := context.Background()
	destination := seedRosterV2Destination(t, pool, account)
	out := make([]rosterV2Subject, 0, count)
	for i := 1; i <= count; i++ {
		s := rosterV2Subject{
			sceneSHA:       rosterV2SHA(fmt.Sprintf("%s:scene:%d", suffix, i)),
			frameSHA:       rosterV2SHA(fmt.Sprintf("%s:frame:%d", suffix, i)),
			sourceURL:      fmt.Sprintf("https://source.example.test/%s/%d.m3u8", suffix, i),
			pageURL:        fmt.Sprintf("https://source.example.test/%s/page/%d", suffix, i),
			provider:       "test",
			externalID:     fmt.Sprintf("%s-%d", suffix, i),
			sourceFamily:   "video_manifest",
			captureType:    "hls",
			executionClass: "video_live",
		}
		if sceneOverride != "" {
			s.sceneSHA = sceneOverride
		}
		if err := pool.QueryRow(ctx, `INSERT INTO streams(provider,external_id,name,slug,source_url,source_page_url,capture_type,source_family,execution_class,capture_family,expected_fps,recording_state) VALUES($1,$2,$2,$3,$4,$5,$6,$7,$8,'continuous_video',30,'on') RETURNING id`, s.provider, s.externalID, "r1-"+s.externalID, s.sourceURL, s.pageURL, s.captureType, s.sourceFamily, s.executionClass).Scan(&s.streamID); err != nil {
			t.Fatal(err)
		}
		if filled {
			if err := pool.QueryRow(ctx, `INSERT INTO recordings(account_id,storage_destination_id,name,stream_url,stream_id,source_kind,cron_timezone,clip_duration_sec,mode,daily_window_start,daily_window_end,active_weekdays,target_fps,status,start_at,capture_via) VALUES($1,$2,$3,$4,$5,'hls_live','UTC',60,'continuous','08:00','20:00',127,NULL,'active',now()-interval '1 hour','cloud') RETURNING id`, account, destination, "rec-"+s.externalID, s.sourceURL, s.streamID).Scan(&s.recordingID); err != nil {
				t.Fatal(err)
			}
		}
		if err := pool.QueryRow(ctx, `INSERT INTO media_objects(storage_provider,bucket,object_key,mime_type,size_bytes,etag,sha256,width,height) VALUES('r2',$1,$2,'image/jpeg',1024,'etag',$3,1920,1080) RETURNING id`, "r1-"+suffix, fmt.Sprintf("frame-%d.jpg", i), s.frameSHA).Scan(&s.mediaID); err != nil {
			t.Fatal(err)
		}
		if err := pool.QueryRow(ctx, `INSERT INTO frames(stream_id,captured_at,raw_media_object_id,capture_status,source_kind) VALUES($1,now()+make_interval(secs=>$2),$3,'success','authoritative_frame_refresh') RETURNING id,captured_at`, s.streamID, int(capturedOffset.Seconds()), s.mediaID).Scan(&s.frameID, &s.frameCapturedAt); err != nil {
			t.Fatal(err)
		}
		if err := pool.QueryRow(ctx, `INSERT INTO recording_scene_frame_evidence(account_id,stream_id,frame_id,media_object_id,captured_at,frame_sha256,scene_identity_sha256,verification_method,verified_by_user_id,verified_at,evidence_sha256) VALUES($1,$2,$3,$4,$5,$6,$7,'operator_visual',$8,now()+make_interval(secs=>$9),repeat('0',64)) RETURNING id,evidence_sha256,verified_at`, account, s.streamID, s.frameID, s.mediaID, s.frameCapturedAt, s.frameSHA, s.sceneSHA, actor, int(verifiedOffset.Seconds())).Scan(&s.sceneEvidenceID, &s.sceneEvidenceSHA, &s.sceneVerifiedAt); err != nil {
			t.Fatal(err)
		}
		out = append(out, s)
	}
	return out
}

func cloneRosterV2ProspectiveEvidence(t *testing.T, pool *pgxpool.Pool, account, actor int64, suffix string, base []rosterV2Subject) []rosterV2Subject {
	t.Helper()
	ctx := context.Background()
	out := make([]rosterV2Subject, 0, len(base))
	for i, original := range base {
		s := original
		s.recordingID = 0
		s.sceneSHA = rosterV2SHA(fmt.Sprintf("%s:scene:%d", suffix, i+1))
		s.frameSHA = rosterV2SHA(fmt.Sprintf("%s:frame:%d", suffix, i+1))
		if err := pool.QueryRow(ctx, `INSERT INTO media_objects(storage_provider,bucket,object_key,mime_type,size_bytes,etag,sha256,width,height) VALUES('r2',$1,$2,'image/jpeg',1024,'etag',$3,1920,1080) RETURNING id`, "r1-"+suffix, fmt.Sprintf("frame-%d.jpg", i+1), s.frameSHA).Scan(&s.mediaID); err != nil {
			t.Fatal(err)
		}
		if err := pool.QueryRow(ctx, `INSERT INTO frames(stream_id,captured_at,raw_media_object_id,capture_status,source_kind) VALUES($1,now()-interval '1 minute',$2,'success','authoritative_frame_refresh') RETURNING id,captured_at`, s.streamID, s.mediaID).Scan(&s.frameID, &s.frameCapturedAt); err != nil {
			t.Fatal(err)
		}
		if err := pool.QueryRow(ctx, `INSERT INTO recording_scene_frame_evidence(account_id,stream_id,frame_id,media_object_id,captured_at,frame_sha256,scene_identity_sha256,verification_method,verified_by_user_id,verified_at,evidence_sha256) VALUES($1,$2,$3,$4,$5,$6,$7,'operator_visual',$8,now(),repeat('0',64)) RETURNING id,evidence_sha256,verified_at`, account, s.streamID, s.frameID, s.mediaID, s.frameCapturedAt, s.frameSHA, s.sceneSHA, actor).Scan(&s.sceneEvidenceID, &s.sceneEvidenceSHA, &s.sceneVerifiedAt); err != nil {
			t.Fatal(err)
		}
		out = append(out, s)
	}
	return out
}

func materializeRosterV2Recordings(t *testing.T, pool *pgxpool.Pool, account int64, suffix string, subjects []rosterV2Subject) {
	t.Helper()
	ctx := context.Background()
	destination := seedRosterV2Destination(t, pool, account)
	for i := range subjects {
		if subjects[i].recordingID != 0 {
			continue
		}
		if err := pool.QueryRow(ctx, `INSERT INTO recordings(account_id,storage_destination_id,name,stream_url,stream_id,source_kind,cron_timezone,clip_duration_sec,mode,daily_window_start,daily_window_end,active_weekdays,target_fps,status,start_at,capture_via) VALUES($1,$2,$3,$4,$5,'hls_live','UTC',60,'continuous','08:00','20:00',127,NULL,'active',now()-interval '1 hour','cloud') RETURNING id`, account, destination, fmt.Sprintf("rec-%s-%d", suffix, i+1), subjects[i].sourceURL, subjects[i].streamID).Scan(&subjects[i].recordingID); err != nil {
			t.Fatal(err)
		}
	}
}

type rosterV2Campaign struct {
	id           int64
	account      int64
	actor        int64
	kind         string
	policyID     int64
	policyDigest string
	target       int
	deadline     time.Time
	revisionID   int64
	revisionNo   int
	state        string
	slots        []int64
	heads        []int64
	items        map[int]string
}

func createRosterV2Draft(t *testing.T, pool *pgxpool.Pool, account, actor int64, key, kind string, target int, overlap string) rosterV2Campaign {
	return createRosterV2DraftWithFreshness(t, pool, account, actor, key, kind, target, overlap, 3600)
}

func createRosterV2DraftWithFreshness(t *testing.T, pool *pgxpool.Pool, account, actor int64, key, kind string, target int, overlap string, freshness int) rosterV2Campaign {
	t.Helper()
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	var c rosterV2Campaign
	c.account, c.actor, c.kind, c.target = account, actor, kind, target
	var createdAt time.Time
	if err = tx.QueryRow(ctx, `INSERT INTO recording_campaign_v2_campaigns(account_id,campaign_key,kind,created_by_user_id) VALUES($1,$2,$3,$4) RETURNING id,created_at`, account, key, kind, actor).Scan(&c.id, &createdAt); err != nil {
		t.Fatal(err)
	}
	c.deadline = time.Now().UTC().Add(72 * time.Hour).Truncate(time.Microsecond)
	group := "operational-v1"
	if kind == "strict50" {
		group = "strict-v1"
	}
	c.policyDigest = rosterV2Digest("campaign-roster-v2-r1-policy", kind, "1", fmt.Sprint(target), fmt.Sprint(c.deadline.UnixMicro()), "GOOD", "0", fmt.Sprint(freshness), group, overlap, "campaign-roster-v2-r1")
	if err = tx.QueryRow(ctx, `INSERT INTO recording_campaign_v2_policy_versions(campaign_id,account_id,version,policy_schema,target_count,deadline_at,grade_floor,required_consecutive_windows,evidence_freshness_seconds,selection_group,overlap_policy,definition_sha256) VALUES($1,$2,1,'campaign-roster-v2-r1',$3,$4,'GOOD',0,$5,$6,$7,$8) RETURNING id`, c.id, account, target, c.deadline, freshness, group, overlap, c.policyDigest).Scan(&c.policyID); err != nil {
		t.Fatal(err)
	}
	rosterDigest := rosterV2RosterDigest(nil)
	reasonDigest := rosterV2ReasonDigest("campaign_created")
	planDigest := rosterV2Digest("campaign-roster-v2-r1-plan", fmt.Sprint(c.id), "1", "create", "", "draft", c.policyDigest, reasonDigest, rosterDigest)
	requestDigest := rosterV2Digest("campaign-roster-v2-r1-request", fmt.Sprint(c.id), fmt.Sprint(account), "1", "create", "", "draft", c.policyDigest, "create", reasonDigest, planDigest)
	if err = tx.QueryRow(ctx, `INSERT INTO recording_campaign_v2_revisions(campaign_id,account_id,revision_number,decision_uuid,event_type,from_state,to_state,policy_version_id,actor_user_id,actor_role,decided_at,request_sha256,plan_sha256,idempotency_key,reason_codes) VALUES($1,$2,1,$3,'create',NULL,'draft',$4,$5,'owner',$6,$7,$8,'create',ARRAY['campaign_created']) RETURNING id`, c.id, account, uuid.New(), c.policyID, actor, createdAt, requestDigest, planDigest).Scan(&c.revisionID); err != nil {
		t.Fatal(err)
	}
	insertRosterV2Seal(t, tx, c, "create", requestDigest, planDigest, rosterDigest, 0, 0, 0, 0, 0)
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	c.revisionNo, c.state, c.items = 1, "draft", map[int]string{}
	return c
}

func insertRosterV2Seal(t *testing.T, tx pgx.Tx, c rosterV2Campaign, key, requestDigest, planDigest, rosterDigest string, filled, prospective, vacant, replace, protected int) {
	t.Helper()
	ctx := context.Background()
	response := []byte(fmt.Sprintf(`{"campaign_id":%d,"revision":%d}`, c.id, c.revisionNo))
	if key == "create" {
		response = []byte(fmt.Sprintf(`{"campaign_id":%d,"revision":1}`, c.id))
	}
	if _, err := tx.Exec(ctx, `INSERT INTO recording_campaign_v2_idempotency(account_id,campaign_id,idempotency_key,request_sha256,plan_sha256,response_sha256,response_bytes,revision_id) VALUES($1,$2,$3,$4,$5,encode(sha256($6),'hex'),$6,$7)`, c.account, c.id, key, requestDigest, planDigest, response, c.revisionID); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO recording_campaign_v2_revision_commits(revision_id,campaign_id,account_id,roster_sha256,filled_count,prospective_count,vacant_count,replace_count,protected_count) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)`, c.revisionID, c.id, c.account, rosterDigest, filled, prospective, vacant, replace, protected); err != nil {
		t.Fatal(err)
	}
}

func rosterV2SHA(value string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(value)))
}

func enterRosterV2Planning(t *testing.T, pool *pgxpool.Pool, c rosterV2Campaign, subjects []rosterV2Subject, filledCount int) rosterV2Campaign {
	t.Helper()
	if len(subjects) != c.target || filledCount < 0 || filledCount > len(subjects) {
		t.Fatalf("invalid planning fixture target=%d subjects=%d filled=%d", c.target, len(subjects), filledCount)
	}
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)

	role := "primary"
	if c.kind == "repair21" {
		role = "repair"
	} else if c.kind == "reserve" {
		role = "reserve"
	}
	c.slots = make([]int64, len(subjects))
	for i := range subjects {
		if err = tx.QueryRow(ctx, `SELECT nextval(pg_get_serial_sequence('recording_campaign_v2_slots','id'))`).Scan(&c.slots[i]); err != nil {
			t.Fatal(err)
		}
	}
	reasons := []string{"member_admitted", "planning_manifest_frozen"}
	if filledCount < len(subjects) {
		reasons = append(reasons, "prospective_added")
	}
	c.items = make(map[int]string, len(subjects))
	for i, subject := range subjects {
		ordinal := i + 1
		if i < filledCount {
			recordingSHA := rosterV2SHA(subject.sourceURL)
			streamSHA := rosterV2SHA(subject.sourceURL)
			pageSHA := rosterV2SHA(subject.pageURL)
			snapshot := rosterV2Digest("campaign-roster-v2-r1-source",
				fmt.Sprint(c.account), fmt.Sprint(subject.recordingID), fmt.Sprint(subject.streamID), "0",
				recordingSHA, streamSHA, pageSHA, subject.provider, subject.externalID,
				subject.sourceFamily, subject.captureType, subject.executionClass)
			c.items[ordinal] = strings.Join([]string{
				fmt.Sprint(c.slots[i]), fmt.Sprint(ordinal), "1", role, fmt.Sprint(ordinal), "protect",
				fmt.Sprint(subject.recordingID), fmt.Sprint(subject.streamID), subject.sceneSHA,
				snapshot, subject.sceneEvidenceSHA, "0", "",
			}, "|")
		} else {
			c.items[ordinal] = strings.Join([]string{
				fmt.Sprint(c.slots[i]), fmt.Sprint(ordinal), "1", role, fmt.Sprint(ordinal), "prospective",
				"0", fmt.Sprint(subject.streamID), subject.sceneSHA,
				rosterV2SHA(subject.pageURL), subject.sceneEvidenceSHA, "0", rosterV2SHA(subject.sourceURL),
			}, "|")
		}
	}
	rosterDigest := rosterV2RosterDigest(c.items)
	reasonDigest := rosterV2ReasonDigest(reasons...)
	planDigest := rosterV2Digest("campaign-roster-v2-r1-plan", fmt.Sprint(c.id), "2", "enter_planning", "draft", "planning", c.policyDigest, reasonDigest, rosterDigest)
	requestDigest := rosterV2Digest("campaign-roster-v2-r1-request", fmt.Sprint(c.id), fmt.Sprint(c.account), "2", "enter_planning", "draft", "planning", c.policyDigest, "planning", reasonDigest, planDigest)
	var decidedAt time.Time
	if err = tx.QueryRow(ctx, `INSERT INTO recording_campaign_v2_revisions(campaign_id,account_id,revision_number,predecessor_revision_id,decision_uuid,event_type,from_state,to_state,policy_version_id,actor_user_id,actor_role,request_sha256,plan_sha256,idempotency_key,reason_codes) VALUES($1,$2,2,$3,$4,'enter_planning','draft','planning',$5,$6,'owner',$7,$8,'planning',$9) RETURNING id,decided_at`, c.id, c.account, c.revisionID, uuid.New(), c.policyID, c.actor, requestDigest, planDigest, reasons).Scan(&c.revisionID, &decidedAt); err != nil {
		t.Fatal(err)
	}
	c.revisionNo = 2
	c.heads = make([]int64, len(subjects))
	for i, subject := range subjects {
		ordinal := i + 1
		var evidenceID int64
		if i < filledCount {
			recordingSHA := rosterV2SHA(subject.sourceURL)
			streamSHA := rosterV2SHA(subject.sourceURL)
			pageSHA := rosterV2SHA(subject.pageURL)
			snapshot := rosterV2Digest("campaign-roster-v2-r1-source",
				fmt.Sprint(c.account), fmt.Sprint(subject.recordingID), fmt.Sprint(subject.streamID), "0",
				recordingSHA, streamSHA, pageSHA, subject.provider, subject.externalID,
				subject.sourceFamily, subject.captureType, subject.executionClass)
			evidenceSHA := rosterV2Digest("campaign-roster-v2-r1-filled-evidence",
				fmt.Sprint(c.id), fmt.Sprint(c.account), fmt.Sprint(c.revisionID), fmt.Sprint(c.policyID),
				fmt.Sprint(subject.recordingID), fmt.Sprint(subject.streamID), "0", snapshot,
				fmt.Sprint(subject.sceneEvidenceID), subject.sceneSHA, subject.sceneEvidenceSHA,
				fmt.Sprint(subject.frameID), fmt.Sprint(subject.mediaID), subject.frameSHA,
				fmt.Sprint(subject.frameCapturedAt.UnixMicro()), fmt.Sprint(subject.sceneVerifiedAt.UnixMicro()), fmt.Sprint(decidedAt.UnixMicro()))
			if err = tx.QueryRow(ctx, `INSERT INTO recording_campaign_v2_filled_evidence(campaign_id,account_id,created_revision_id,policy_version_id,recording_id,stream_id,source_revision_id,recording_source_sha256,stream_source_sha256,source_page_sha256,provider,external_id,source_family,capture_type,execution_class,source_snapshot_sha256,scene_frame_evidence_id,scene_identity_sha256,scene_evidence_sha256,frame_id,media_object_id,frame_sha256,frame_captured_at,scene_verified_at,accepted_at_decision,evidence_sha256) VALUES($1,$2,$3,$4,$5,$6,NULL,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25) RETURNING id`, c.id, c.account, c.revisionID, c.policyID, subject.recordingID, subject.streamID, recordingSHA, streamSHA, pageSHA, subject.provider, subject.externalID, subject.sourceFamily, subject.captureType, subject.executionClass, snapshot, subject.sceneEvidenceID, subject.sceneSHA, subject.sceneEvidenceSHA, subject.frameID, subject.mediaID, subject.frameSHA, subject.frameCapturedAt, subject.sceneVerifiedAt, decidedAt, evidenceSHA).Scan(&evidenceID); err != nil {
				t.Fatal(err)
			}
		} else {
			evidenceSHA := rosterV2Digest("campaign-roster-v2-r1-prospective-evidence",
				fmt.Sprint(c.id), fmt.Sprint(c.account), fmt.Sprint(c.revisionID), fmt.Sprint(c.policyID),
				fmt.Sprint(subject.streamID), rosterV2SHA(subject.pageURL), rosterV2SHA(subject.sourceURL), subject.provider,
				fmt.Sprint(subject.sceneEvidenceID), subject.sceneSHA, subject.sceneEvidenceSHA,
				fmt.Sprint(subject.frameID), fmt.Sprint(subject.mediaID), subject.frameSHA,
				fmt.Sprint(subject.frameCapturedAt.UnixMicro()), fmt.Sprint(subject.sceneVerifiedAt.UnixMicro()), fmt.Sprint(decidedAt.UnixMicro()))
			if err = tx.QueryRow(ctx, `INSERT INTO recording_campaign_v2_prospective_evidence(campaign_id,account_id,created_revision_id,policy_version_id,stream_id,official_page_sha256,source_sha256,provider,scene_frame_evidence_id,scene_identity_sha256,scene_evidence_sha256,frame_id,media_object_id,frame_sha256,frame_captured_at,scene_verified_at,accepted_at_decision,evidence_sha256) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18) RETURNING id`, c.id, c.account, c.revisionID, c.policyID, subject.streamID, rosterV2SHA(subject.pageURL), rosterV2SHA(subject.sourceURL), subject.provider, subject.sceneEvidenceID, subject.sceneSHA, subject.sceneEvidenceSHA, subject.frameID, subject.mediaID, subject.frameSHA, subject.frameCapturedAt, subject.sceneVerifiedAt, decidedAt, evidenceSHA).Scan(&evidenceID); err != nil {
				t.Fatal(err)
			}
		}
		if _, err = tx.Exec(ctx, `INSERT INTO recording_campaign_v2_slots(id,campaign_id,account_id,ordinal,created_revision_id) VALUES($1,$2,$3,$4,$5)`, c.slots[i], c.id, c.account, ordinal, c.revisionID); err != nil {
			t.Fatal(err)
		}
		if i < filledCount {
			if err = tx.QueryRow(ctx, `INSERT INTO recording_campaign_v2_slot_versions(campaign_id,account_id,slot_id,campaign_revision_id,version,role,rank,disposition,filled_evidence_id,recording_id,stream_id,scene_identity_sha256,effective_from) VALUES($1,$2,$3,$4,1,$5,$6,'protect',$7,$8,$9,$10,$11) RETURNING id`, c.id, c.account, c.slots[i], c.revisionID, role, ordinal, evidenceID, subject.recordingID, subject.streamID, subject.sceneSHA, decidedAt).Scan(&c.heads[i]); err != nil {
				t.Fatal(err)
			}
		} else if err = tx.QueryRow(ctx, `INSERT INTO recording_campaign_v2_slot_versions(campaign_id,account_id,slot_id,campaign_revision_id,version,role,rank,disposition,prospective_evidence_id,stream_id,scene_identity_sha256,effective_from) VALUES($1,$2,$3,$4,1,$5,$6,'prospective',$7,$8,$9,$10) RETURNING id`, c.id, c.account, c.slots[i], c.revisionID, role, ordinal, evidenceID, subject.streamID, subject.sceneSHA, decidedAt).Scan(&c.heads[i]); err != nil {
			t.Fatal(err)
		}
		if _, err = tx.Exec(ctx, `INSERT INTO recording_campaign_v2_revision_slot_changes(revision_id,campaign_id,account_id,slot_id,before_version_id,after_version_id) VALUES($1,$2,$3,$4,NULL,$5)`, c.revisionID, c.id, c.account, c.slots[i], c.heads[i]); err != nil {
			t.Fatal(err)
		}
		scope := "operational"
		if c.kind == "strict50" {
			scope = "strict"
		}
		if i < filledCount {
			if _, err = tx.Exec(ctx, `INSERT INTO recording_campaign_v2_recording_occupancy(account_id,occupancy_scope,recording_id,campaign_id,slot_id,slot_version_id) VALUES($1,$2,$3,$4,$5,$6)`, c.account, scope, subject.recordingID, c.id, c.slots[i], c.heads[i]); err != nil {
				t.Fatal(err)
			}
		}
		if _, err = tx.Exec(ctx, `INSERT INTO recording_campaign_v2_scene_occupancy(account_id,occupancy_scope,scene_identity_sha256,campaign_id,slot_id,slot_version_id) VALUES($1,$2,$3,$4,$5,$6)`, c.account, scope, subject.sceneSHA, c.id, c.slots[i], c.heads[i]); err != nil {
			t.Fatal(err)
		}
	}
	insertRosterV2Seal(t, tx, c, "planning", requestDigest, planDigest, rosterDigest, filledCount, len(subjects)-filledCount, 0, 0, filledCount)
	if err = tx.Commit(ctx); err != nil {
		t.Fatalf("seal %s planning roster: %v", c.kind, err)
	}
	c.state = "planning"
	return c
}

func writeOneRosterV2Planning(tx pgx.Tx, c rosterV2Campaign, subject rosterV2Subject) (rosterV2Campaign, error) {
	ctx := context.Background()
	var slotID int64
	if err := tx.QueryRow(ctx, `SELECT nextval(pg_get_serial_sequence('recording_campaign_v2_slots','id'))`).Scan(&slotID); err != nil {
		return c, err
	}
	recordingSHA := rosterV2SHA(subject.sourceURL)
	streamSHA := rosterV2SHA(subject.sourceURL)
	pageSHA := rosterV2SHA(subject.pageURL)
	snapshot := rosterV2Digest("campaign-roster-v2-r1-source",
		fmt.Sprint(c.account), fmt.Sprint(subject.recordingID), fmt.Sprint(subject.streamID), "0",
		recordingSHA, streamSHA, pageSHA, subject.provider, subject.externalID,
		subject.sourceFamily, subject.captureType, subject.executionClass)
	c.items = map[int]string{1: strings.Join([]string{
		fmt.Sprint(slotID), "1", "1", "reserve", "1", "protect", fmt.Sprint(subject.recordingID), fmt.Sprint(subject.streamID),
		subject.sceneSHA, snapshot, subject.sceneEvidenceSHA, "0", "",
	}, "|")}
	rosterDigest := rosterV2RosterDigest(c.items)
	reasonDigest := rosterV2ReasonDigest("member_admitted", "planning_manifest_frozen")
	planDigest := rosterV2Digest("campaign-roster-v2-r1-plan", fmt.Sprint(c.id), "2", "enter_planning", "draft", "planning", c.policyDigest, reasonDigest, rosterDigest)
	requestDigest := rosterV2Digest("campaign-roster-v2-r1-request", fmt.Sprint(c.id), fmt.Sprint(c.account), "2", "enter_planning", "draft", "planning", c.policyDigest, "planning", reasonDigest, planDigest)
	var decidedAt time.Time
	if err := tx.QueryRow(ctx, `INSERT INTO recording_campaign_v2_revisions(campaign_id,account_id,revision_number,predecessor_revision_id,decision_uuid,event_type,from_state,to_state,policy_version_id,actor_user_id,actor_role,request_sha256,plan_sha256,idempotency_key,reason_codes) VALUES($1,$2,2,$3,$4,'enter_planning','draft','planning',$5,$6,'owner',$7,$8,'planning',ARRAY['member_admitted','planning_manifest_frozen']) RETURNING id,decided_at`, c.id, c.account, c.revisionID, uuid.New(), c.policyID, c.actor, requestDigest, planDigest).Scan(&c.revisionID, &decidedAt); err != nil {
		return c, err
	}
	c.revisionNo = 2
	evidenceSHA := rosterV2Digest("campaign-roster-v2-r1-filled-evidence",
		fmt.Sprint(c.id), fmt.Sprint(c.account), fmt.Sprint(c.revisionID), fmt.Sprint(c.policyID),
		fmt.Sprint(subject.recordingID), fmt.Sprint(subject.streamID), "0", snapshot,
		fmt.Sprint(subject.sceneEvidenceID), subject.sceneSHA, subject.sceneEvidenceSHA,
		fmt.Sprint(subject.frameID), fmt.Sprint(subject.mediaID), subject.frameSHA,
		fmt.Sprint(subject.frameCapturedAt.UnixMicro()), fmt.Sprint(subject.sceneVerifiedAt.UnixMicro()), fmt.Sprint(decidedAt.UnixMicro()))
	var evidenceID, headID int64
	if err := tx.QueryRow(ctx, `INSERT INTO recording_campaign_v2_filled_evidence(campaign_id,account_id,created_revision_id,policy_version_id,recording_id,stream_id,source_revision_id,recording_source_sha256,stream_source_sha256,source_page_sha256,provider,external_id,source_family,capture_type,execution_class,source_snapshot_sha256,scene_frame_evidence_id,scene_identity_sha256,scene_evidence_sha256,frame_id,media_object_id,frame_sha256,frame_captured_at,scene_verified_at,accepted_at_decision,evidence_sha256) VALUES($1,$2,$3,$4,$5,$6,NULL,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25) RETURNING id`, c.id, c.account, c.revisionID, c.policyID, subject.recordingID, subject.streamID, recordingSHA, streamSHA, pageSHA, subject.provider, subject.externalID, subject.sourceFamily, subject.captureType, subject.executionClass, snapshot, subject.sceneEvidenceID, subject.sceneSHA, subject.sceneEvidenceSHA, subject.frameID, subject.mediaID, subject.frameSHA, subject.frameCapturedAt, subject.sceneVerifiedAt, decidedAt, evidenceSHA).Scan(&evidenceID); err != nil {
		return c, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO recording_campaign_v2_slots(id,campaign_id,account_id,ordinal,created_revision_id) VALUES($1,$2,$3,1,$4)`, slotID, c.id, c.account, c.revisionID); err != nil {
		return c, err
	}
	if err := tx.QueryRow(ctx, `INSERT INTO recording_campaign_v2_slot_versions(campaign_id,account_id,slot_id,campaign_revision_id,version,role,rank,disposition,filled_evidence_id,recording_id,stream_id,scene_identity_sha256,effective_from) VALUES($1,$2,$3,$4,1,'reserve',1,'protect',$5,$6,$7,$8,$9) RETURNING id`, c.id, c.account, slotID, c.revisionID, evidenceID, subject.recordingID, subject.streamID, subject.sceneSHA, decidedAt).Scan(&headID); err != nil {
		return c, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO recording_campaign_v2_revision_slot_changes(revision_id,campaign_id,account_id,slot_id,before_version_id,after_version_id) VALUES($1,$2,$3,$4,NULL,$5)`, c.revisionID, c.id, c.account, slotID, headID); err != nil {
		return c, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO recording_campaign_v2_recording_occupancy(account_id,occupancy_scope,recording_id,campaign_id,slot_id,slot_version_id) VALUES($1,'operational',$2,$3,$4,$5)`, c.account, subject.recordingID, c.id, slotID, headID); err != nil {
		return c, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO recording_campaign_v2_scene_occupancy(account_id,occupancy_scope,scene_identity_sha256,campaign_id,slot_id,slot_version_id) VALUES($1,'operational',$2,$3,$4,$5)`, c.account, subject.sceneSHA, c.id, slotID, headID); err != nil {
		return c, err
	}
	response := []byte(fmt.Sprintf(`{"campaign_id":%d,"revision":2}`, c.id))
	if _, err := tx.Exec(ctx, `INSERT INTO recording_campaign_v2_idempotency(account_id,campaign_id,idempotency_key,request_sha256,plan_sha256,response_sha256,response_bytes,revision_id) VALUES($1,$2,'planning',$3,$4,encode(sha256($5),'hex'),$5,$6)`, c.account, c.id, requestDigest, planDigest, response, c.revisionID); err != nil {
		return c, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO recording_campaign_v2_revision_commits(revision_id,campaign_id,account_id,roster_sha256,filled_count,prospective_count,vacant_count,replace_count,protected_count) VALUES($1,$2,$3,$4,1,0,0,0,1)`, c.revisionID, c.id, c.account, rosterDigest); err != nil {
		return c, err
	}
	c.slots, c.heads, c.state = []int64{slotID}, []int64{headID}, "planning"
	return c, nil
}

func activateRosterV2(t *testing.T, pool *pgxpool.Pool, c rosterV2Campaign, filled, prospective int) (rosterV2Campaign, error) {
	t.Helper()
	tx, next, err := prepareActivateRosterV2(pool, c, filled, prospective)
	if err != nil {
		return c, err
	}
	defer tx.Rollback(context.Background())
	if err = tx.Commit(context.Background()); err != nil {
		return c, err
	}
	next.state = "active"
	return next, nil
}

func prepareActivateRosterV2(pool *pgxpool.Pool, c rosterV2Campaign, filled, prospective int) (pgx.Tx, rosterV2Campaign, error) {
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, c, err
	}
	c, err = writeActivateRosterV2(tx, c, filled, prospective)
	if err != nil {
		_ = tx.Rollback(ctx)
		return nil, c, err
	}
	return tx, c, nil
}

func writeActivateRosterV2(tx pgx.Tx, c rosterV2Campaign, filled, prospective int) (rosterV2Campaign, error) {
	ctx := context.Background()
	var err error
	revisionNo := c.revisionNo + 1
	key := fmt.Sprintf("activate-%d", revisionNo)
	rosterDigest := rosterV2RosterDigest(c.items)
	reasonDigest := rosterV2ReasonDigest("activation_gates_met")
	planDigest := rosterV2Digest("campaign-roster-v2-r1-plan", fmt.Sprint(c.id), fmt.Sprint(revisionNo), "activate", c.state, "active", c.policyDigest, reasonDigest, rosterDigest)
	requestDigest := rosterV2Digest("campaign-roster-v2-r1-request", fmt.Sprint(c.id), fmt.Sprint(c.account), fmt.Sprint(revisionNo), "activate", c.state, "active", c.policyDigest, key, reasonDigest, planDigest)
	priorRevision := c.revisionID
	if err = tx.QueryRow(ctx, `INSERT INTO recording_campaign_v2_revisions(campaign_id,account_id,revision_number,predecessor_revision_id,decision_uuid,event_type,from_state,to_state,policy_version_id,actor_user_id,actor_role,request_sha256,plan_sha256,idempotency_key,reason_codes) VALUES($1,$2,$3,$4,$5,'activate',$6,'active',$7,$8,'owner',$9,$10,$11,ARRAY['activation_gates_met']) RETURNING id`, c.id, c.account, revisionNo, priorRevision, uuid.New(), c.state, c.policyID, c.actor, requestDigest, planDigest, key).Scan(&c.revisionID); err != nil {
		return c, err
	}
	c.revisionNo = revisionNo
	response := []byte(fmt.Sprintf(`{"campaign_id":%d,"revision":%d}`, c.id, c.revisionNo))
	if _, err = tx.Exec(ctx, `INSERT INTO recording_campaign_v2_idempotency(account_id,campaign_id,idempotency_key,request_sha256,plan_sha256,response_sha256,response_bytes,revision_id) VALUES($1,$2,$3,$4,$5,encode(sha256($6),'hex'),$6,$7)`, c.account, c.id, key, requestDigest, planDigest, response, c.revisionID); err != nil {
		return c, err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO recording_campaign_v2_revision_commits(revision_id,campaign_id,account_id,roster_sha256,filled_count,prospective_count,vacant_count,replace_count,protected_count) VALUES($1,$2,$3,$4,$5,$6,0,0,$5)`, c.revisionID, c.id, c.account, rosterDigest, filled, prospective); err != nil {
		return c, err
	}
	return c, nil
}

type rosterV2PresentationLease struct {
	nodeID int64
	jobID  int64
	lease  uuid.UUID
}

func applyRosterV2StreamPatchOrder(ctx context.Context, tx pgx.Tx, subject rosterV2Subject, nextURL string) error {
	if _, err := tx.Exec(ctx, `SELECT recording_campaign_v2_lock_source_stream($1)`, subject.streamID); err != nil {
		return err
	}
	var currentURL string
	if err := tx.QueryRow(ctx, `SELECT source_url FROM streams WHERE id=$1 FOR UPDATE`, subject.streamID).Scan(&currentURL); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE streams SET source_url=$2,updated_at=now() WHERE id=$1`, subject.streamID, nextURL); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO stream_source_revisions(stream_id,actor,reason,previous_source_url,new_source_url,previous_source_page_url,new_source_page_url,previous_source_family,new_source_family,previous_capture_type,new_capture_type,previous_execution_class,new_execution_class) SELECT id,'test','source patch',$2,$3,source_page_url,source_page_url,source_family,source_family,capture_type,capture_type,execution_class,execution_class FROM streams WHERE id=$1`, subject.streamID, currentURL, nextURL); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `UPDATE recordings SET stream_url=$2,updated_at=now() WHERE id=$1`, subject.recordingID, nextURL)
	return err
}

func seedRosterV2PresentationLease(t *testing.T, pool *pgxpool.Pool, account int64, subject rosterV2Subject, suffix string) rosterV2PresentationLease {
	t.Helper()
	ctx := context.Background()
	var fixture rosterV2PresentationLease
	fixture.lease = uuid.New()
	if _, err := pool.Exec(ctx, `UPDATE recordings SET capture_via='relay' WHERE id=$1 AND account_id=$2`, subject.recordingID, account); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `INSERT INTO nodes(account_id,display_name,node_type,status,last_heartbeat_at,relay_max_streams) VALUES($1,$2,'relay','active',now(),4) RETURNING id`, account, "r1-node-"+suffix).Scan(&fixture.nodeID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `INSERT INTO recording_jobs(recording_id,fire_at,scheduled_for,clip_duration_sec,status,lease_owner,lease_expires_at,lease_token,idempotency_key,kind,window_end_at) VALUES($1,now(),now(),60,'leased',$2,now()+interval '1 hour',$3,$4,'continuous_window',now()+interval '1 hour') RETURNING id`, subject.recordingID, fmt.Sprintf("node:%d", fixture.nodeID), fixture.lease, "r1-job-"+suffix).Scan(&fixture.jobID); err != nil {
		t.Fatal(err)
	}
	return fixture
}

func insertRosterV2PresentationAdmission(ctx context.Context, tx pgx.Tx, account int64, subject rosterV2Subject, fixture rosterV2PresentationLease, admission uuid.UUID) error {
	_, err := tx.Exec(ctx, `INSERT INTO recording_presentation_v2_admissions(id,account_id,recording_id,stream_id,recording_job_id,lease_token,node_id,source_revision_id,recording_stream_url_sha256,stream_source_url_sha256,stream_source_page_url_sha256,source_snapshot_sha256,provider,external_id,source_family,capture_type,execution_class,capture_mode,audio_selection,policy_version,parser_schema,capture_tool_identity_sha256,deadline_at)
		SELECT $1,r.account_id,r.id,r.stream_id,j.id,j.lease_token,n.id,(SELECT max(id) FROM stream_source_revisions WHERE stream_id=s.id),encode(sha256(convert_to(r.stream_url,'UTF8')),'hex'),encode(sha256(convert_to(s.source_url,'UTF8')),'hex'),encode(sha256(convert_to(s.source_page_url,'UTF8')),'hex'),encode(sha256(convert_to(jsonb_build_array(r.account_id,r.id,r.stream_id,j.id,j.lease_token,n.id,r.stream_url,s.source_url,s.source_page_url,s.provider,s.external_id,s.source_family,s.capture_type,s.execution_class,COALESCE((SELECT max(id) FROM stream_source_revisions WHERE stream_id=s.id),0),'source_copy','first_optional')::text,'UTF8')),'hex'),s.provider,s.external_id,s.source_family,s.capture_type,s.execution_class,'source_copy','first_optional','continuous-source-presentation-edge-v2','presentation-probe-v2',recording_presentation_v2_tool_identity('8.1','8.1','62','62','60',repeat('b',64),'mov','h264','aac','presentation-probe-v2'),now()+interval '14 minutes'
		FROM recordings r JOIN streams s ON s.id=r.stream_id JOIN recording_jobs j ON j.recording_id=r.id JOIN nodes n ON n.id=$2
		WHERE r.account_id=$3 AND r.id=$4 AND r.stream_id=$5 AND j.id=$6 AND j.lease_token=$7`, admission, fixture.nodeID, account, subject.recordingID, subject.streamID, fixture.jobID, fixture.lease)
	return err
}

func waitForRosterV2PostgresLock(t *testing.T, pool *pgxpool.Pool, pid int32) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var waitType *string
		err := pool.QueryRow(ctx, `SELECT wait_event_type FROM pg_stat_activity WHERE pid=$1`, pid).Scan(&waitType)
		if err == nil && waitType != nil && *waitType == "Lock" {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("backend %d did not block on the shared protection fence: %v", pid, err)
		case <-ticker.C:
		}
	}
}

func TestRecordingCampaignV2R1PresentationAdmissionProtectionRaceBothCommitOrders(t *testing.T) {
	f := openRosterV2PG(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	setup := func(suffix string) (rosterV2Campaign, rosterV2Subject, rosterV2PresentationLease) {
		account, actor := seedRosterV2Actor(t, f.pool, suffix)
		subject := seedRosterV2Subjects(t, f.pool, account, actor, suffix, 1, true)[0]
		lease := seedRosterV2PresentationLease(t, f.pool, account, subject, suffix)
		campaign := createRosterV2Draft(t, f.pool, account, actor, suffix, "reserve", 1, "operational-disjoint-v1")
		campaign = enterRosterV2Planning(t, f.pool, campaign, []rosterV2Subject{subject}, 1)
		return campaign, subject, lease
	}

	t.Run("admission commits first", func(t *testing.T) {
		campaign, subject, lease := setup("race-admission-first")
		admissionTx, err := f.pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer admissionTx.Rollback(ctx)
		admission := uuid.New()
		if err = insertRosterV2PresentationAdmission(ctx, admissionTx, campaign.account, subject, lease, admission); err != nil {
			t.Fatal(err)
		}
		pidCh := make(chan int32, 1)
		resultCh := make(chan error, 1)
		go func() {
			protectTx, beginErr := f.pool.Begin(ctx)
			if beginErr != nil {
				resultCh <- beginErr
				return
			}
			defer protectTx.Rollback(ctx)
			var pid int32
			if beginErr = protectTx.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&pid); beginErr != nil {
				resultCh <- beginErr
				return
			}
			pidCh <- pid
			_, protectErr := writeActivateRosterV2(protectTx, campaign, 1, 0)
			if protectErr == nil {
				protectErr = protectTx.Commit(ctx)
			}
			resultCh <- protectErr
		}()
		waitForRosterV2PostgresLock(t, f.pool, <-pidCh)
		if err = admissionTx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		if err = <-resultCh; err == nil || !strings.Contains(err.Error(), "presentation admission vetoes") {
			t.Fatalf("protection did not lose admission-first race: %v", err)
		}
		var state string
		if err = f.pool.QueryRow(ctx, `SELECT state FROM recording_campaign_v2_current_revisions WHERE campaign_id=$1`, campaign.id).Scan(&state); err != nil || state != "planning" {
			t.Fatalf("admission-first campaign state=%q err=%v", state, err)
		}
	})

	t.Run("protection commits first", func(t *testing.T) {
		campaign, subject, lease := setup("race-protection-first")
		protectTx, err := f.pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer protectTx.Rollback(ctx)
		if _, err = writeActivateRosterV2(protectTx, campaign, 1, 0); err != nil {
			t.Fatal(err)
		}
		pidCh := make(chan int32, 1)
		resultCh := make(chan error, 1)
		go func() {
			admissionTx, beginErr := f.pool.Begin(ctx)
			if beginErr != nil {
				resultCh <- beginErr
				return
			}
			defer admissionTx.Rollback(ctx)
			var pid int32
			if beginErr = admissionTx.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&pid); beginErr != nil {
				resultCh <- beginErr
				return
			}
			pidCh <- pid
			insertErr := insertRosterV2PresentationAdmission(ctx, admissionTx, campaign.account, subject, lease, uuid.New())
			if insertErr == nil {
				insertErr = admissionTx.Commit(ctx)
			}
			resultCh <- insertErr
		}()
		waitForRosterV2PostgresLock(t, f.pool, <-pidCh)
		if err = protectTx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		if err = <-resultCh; err == nil || !strings.Contains(err.Error(), "campaign-protected") {
			t.Fatalf("admission did not lose protection-first race: %v", err)
		}
		var protected int
		if err = f.pool.QueryRow(ctx, `SELECT count(*) FROM protected_campaign_recordings WHERE account_id=$1 AND recording_id=$2`, campaign.account, subject.recordingID).Scan(&protected); err != nil || protected != 1 {
			t.Fatalf("protection-first view count=%d err=%v", protected, err)
		}
	})

	t.Run("activation negative check fences concurrent heartbeat revival", func(t *testing.T) {
		campaign, subject, lease := setup("race-heartbeat")
		admissionTx, err := f.pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err = insertRosterV2PresentationAdmission(ctx, admissionTx, campaign.account, subject, lease, uuid.New()); err != nil {
			t.Fatal(err)
		}
		if err = admissionTx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err = f.pool.Exec(ctx, `UPDATE recording_jobs SET status='pending',lease_owner=NULL,lease_expires_at=NULL WHERE id=$1`, lease.jobID); err != nil {
			t.Fatal(err)
		}
		activationTx, err := f.pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer activationTx.Rollback(ctx)
		if _, err = writeActivateRosterV2(activationTx, campaign, 1, 0); err != nil {
			t.Fatalf("negative admission check activation: %v", err)
		}
		pidCh := make(chan int32, 1)
		resultCh := make(chan error, 1)
		go func() {
			heartbeatTx, beginErr := f.pool.Begin(ctx)
			if beginErr != nil {
				resultCh <- beginErr
				return
			}
			defer heartbeatTx.Rollback(ctx)
			var pid int32
			if beginErr = heartbeatTx.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&pid); beginErr != nil {
				resultCh <- beginErr
				return
			}
			pidCh <- pid
			_, updateErr := heartbeatTx.Exec(ctx, `UPDATE recording_jobs SET status='leased',lease_owner=$2,lease_expires_at=now()+interval '1 hour' WHERE id=$1`, lease.jobID, fmt.Sprintf("node:%d", lease.nodeID))
			if updateErr == nil {
				updateErr = heartbeatTx.Commit(ctx)
			}
			resultCh <- updateErr
		}()
		waitForRosterV2PostgresLock(t, f.pool, <-pidCh)
		if err = activationTx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		if err = <-resultCh; err == nil || !strings.Contains(err.Error(), "cannot revive or renew") {
			t.Fatalf("heartbeat revival after protection commit err=%v", err)
		}
	})

	t.Run("crossing heartbeat row lock cannot deadlock activation", func(t *testing.T) {
		campaign, subject, lease := setup("race-heartbeat-crossing")
		admissionTx, err := f.pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err = insertRosterV2PresentationAdmission(ctx, admissionTx, campaign.account, subject, lease, uuid.New()); err != nil {
			t.Fatal(err)
		}
		if err = admissionTx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err = f.pool.Exec(ctx, `UPDATE recording_jobs SET status='pending',lease_owner=NULL,lease_expires_at=NULL WHERE id=$1`, lease.jobID); err != nil {
			t.Fatal(err)
		}
		heartbeatTx, err := f.pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer heartbeatTx.Rollback(context.Background())
		// Take the job row exactly as UPDATE does, then let activation acquire the
		// recording fence. Activation must not wait back on this job row.
		if _, err = heartbeatTx.Exec(ctx, `SELECT 1 FROM recording_jobs WHERE id=$1 FOR UPDATE`, lease.jobID); err != nil {
			t.Fatal(err)
		}
		resultCh := make(chan error, 1)
		go func() {
			activationTx, beginErr := f.pool.Begin(ctx)
			if beginErr != nil {
				resultCh <- beginErr
				return
			}
			defer activationTx.Rollback(context.Background())
			_, activationErr := writeActivateRosterV2(activationTx, campaign, 1, 0)
			if activationErr == nil {
				activationErr = activationTx.Commit(ctx)
			}
			resultCh <- activationErr
		}()
		select {
		case err = <-resultCh:
			if err != nil {
				t.Fatalf("activation waited on or failed behind heartbeat row lock: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("activation deadlocked waiting on heartbeat-owned job row")
		}
		if _, err = heartbeatTx.Exec(ctx, `UPDATE recording_jobs SET status='leased',lease_owner=$2,lease_expires_at=now()+interval '1 hour' WHERE id=$1`, lease.jobID, fmt.Sprintf("node:%d", lease.nodeID)); err == nil || !strings.Contains(err.Error(), "cannot revive or renew") {
			t.Fatalf("crossing heartbeat was not rejected after activation err=%v", err)
		}
	})

	t.Run("heartbeat revival commits first and vetoes activation", func(t *testing.T) {
		campaign, subject, lease := setup("race-heartbeat-first")
		admissionTx, err := f.pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err = insertRosterV2PresentationAdmission(ctx, admissionTx, campaign.account, subject, lease, uuid.New()); err != nil {
			t.Fatal(err)
		}
		if err = admissionTx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err = f.pool.Exec(ctx, `UPDATE recording_jobs SET status='pending',lease_owner=NULL,lease_expires_at=NULL WHERE id=$1`, lease.jobID); err != nil {
			t.Fatal(err)
		}
		heartbeatTx, err := f.pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer heartbeatTx.Rollback(ctx)
		if _, err = heartbeatTx.Exec(ctx, `UPDATE recording_jobs SET status='leased',lease_owner=$2,lease_expires_at=now()+interval '1 hour' WHERE id=$1`, lease.jobID, fmt.Sprintf("node:%d", lease.nodeID)); err != nil {
			t.Fatal(err)
		}
		pidCh := make(chan int32, 1)
		resultCh := make(chan error, 1)
		go func() {
			activationTx, beginErr := f.pool.Begin(ctx)
			if beginErr != nil {
				resultCh <- beginErr
				return
			}
			defer activationTx.Rollback(ctx)
			var pid int32
			if beginErr = activationTx.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&pid); beginErr != nil {
				resultCh <- beginErr
				return
			}
			pidCh <- pid
			_, activationErr := writeActivateRosterV2(activationTx, campaign, 1, 0)
			if activationErr == nil {
				activationErr = activationTx.Commit(ctx)
			}
			resultCh <- activationErr
		}()
		waitForRosterV2PostgresLock(t, f.pool, <-pidCh)
		if err = heartbeatTx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		if err = <-resultCh; err == nil || !strings.Contains(err.Error(), "presentation admission vetoes") {
			t.Fatalf("heartbeat-first activation err=%v", err)
		}
	})
}

func TestRecordingCampaignV2R1SourceWriterFencesEvidenceAndActivation(t *testing.T) {
	f := openRosterV2PG(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	run := func(t *testing.T, activation bool) {
		suffix := "source-vs-evidence"
		if activation {
			suffix = "source-vs-activation"
		}
		account, actor := seedRosterV2Actor(t, f.pool, suffix)
		subject := seedRosterV2Subjects(t, f.pool, account, actor, suffix, 1, true)[0]
		campaign := createRosterV2Draft(t, f.pool, account, actor, suffix, "reserve", 1, "operational-disjoint-v1")
		if activation {
			campaign = enterRosterV2Planning(t, f.pool, campaign, []rosterV2Subject{subject}, 1)
		}
		writer, err := f.pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer writer.Rollback(context.Background())
		if err = applyRosterV2StreamPatchOrder(ctx, writer, subject, subject.sourceURL+"?patched=1"); err != nil {
			t.Fatal(err)
		}
		pidCh := make(chan int32, 1)
		resultCh := make(chan error, 1)
		go func() {
			tx, beginErr := f.pool.Begin(ctx)
			if beginErr != nil {
				resultCh <- beginErr
				return
			}
			defer tx.Rollback(context.Background())
			var pid int32
			if beginErr = tx.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&pid); beginErr != nil {
				resultCh <- beginErr
				return
			}
			pidCh <- pid
			var writeErr error
			if activation {
				_, writeErr = writeActivateRosterV2(tx, campaign, 1, 0)
			} else {
				_, writeErr = writeOneRosterV2Planning(tx, campaign, subject)
			}
			resultCh <- writeErr
		}()
		waitForRosterV2PostgresLock(t, f.pool, <-pidCh)
		if err = writer.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		if err = <-resultCh; err == nil || !strings.Contains(err.Error(), "source") {
			t.Fatalf("source writer did not fence roster mutation activation=%t err=%v", activation, err)
		}
	}
	t.Run("stream patch versus filled evidence", func(t *testing.T) { run(t, false) })
	t.Run("stream patch versus activation", func(t *testing.T) { run(t, true) })
}

func TestRecordingCampaignV2R1SourceWriterFencesRealPresentationAdmission(t *testing.T) {
	f := openRosterV2PG(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	account, actor := seedRosterV2Actor(t, f.pool, "source-vs-admission")
	subject := seedRosterV2Subjects(t, f.pool, account, actor, "source-vs-admission", 1, true)[0]
	lease := seedRosterV2PresentationLease(t, f.pool, account, subject, "source-vs-admission")
	writer, err := f.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Rollback(context.Background())
	if err = applyRosterV2StreamPatchOrder(ctx, writer, subject, subject.sourceURL+"?patched=1"); err != nil {
		t.Fatal(err)
	}
	pidCh := make(chan int32, 1)
	resultCh := make(chan error, 1)
	go func() {
		admissionTx, beginErr := f.pool.Begin(ctx)
		if beginErr != nil {
			resultCh <- beginErr
			return
		}
		defer admissionTx.Rollback(context.Background())
		var pid int32
		if beginErr = admissionTx.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&pid); beginErr != nil {
			resultCh <- beginErr
			return
		}
		pidCh <- pid
		resultCh <- insertRosterV2PresentationAdmission(ctx, admissionTx, account, subject, lease, uuid.New())
	}()
	waitForRosterV2PostgresLock(t, f.pool, <-pidCh)
	if err = writer.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err = <-resultCh; err == nil || !strings.Contains(err.Error(), "source") {
		t.Fatalf("source-first real admission did not fail coherently err=%v", err)
	}
	var admissions int
	if err = f.pool.QueryRow(ctx, `SELECT count(*) FROM recording_presentation_v2_admissions WHERE recording_job_id=$1`, lease.jobID).Scan(&admissions); err != nil || admissions != 0 {
		t.Fatalf("source-first admission count=%d err=%v", admissions, err)
	}
}

func TestRecordingCampaignV2R1OccupancyRacesAreGloballyDisjoint(t *testing.T) {
	f := openRosterV2PG(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	runRace := func(t *testing.T, winner, loser rosterV2Campaign, winnerSubject, loserSubject rosterV2Subject) {
		t.Helper()
		winnerTx, err := f.pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer winnerTx.Rollback(ctx)
		winner, err = writeOneRosterV2Planning(winnerTx, winner, winnerSubject)
		if err != nil {
			t.Fatal(err)
		}
		pidCh := make(chan int32, 1)
		resultCh := make(chan error, 1)
		go func() {
			loserTx, beginErr := f.pool.Begin(ctx)
			if beginErr != nil {
				resultCh <- beginErr
				return
			}
			defer loserTx.Rollback(ctx)
			var pid int32
			if beginErr = loserTx.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&pid); beginErr != nil {
				resultCh <- beginErr
				return
			}
			pidCh <- pid
			_, writeErr := writeOneRosterV2Planning(loserTx, loser, loserSubject)
			if writeErr == nil {
				writeErr = loserTx.Commit(ctx)
			}
			resultCh <- writeErr
		}()
		waitForRosterV2PostgresLock(t, f.pool, <-pidCh)
		if err = winnerTx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		if err = <-resultCh; err == nil {
			t.Fatal("concurrent duplicate campaign occupancy committed twice")
		}
		var winnerState, loserState string
		if err = f.pool.QueryRow(ctx, `SELECT state FROM recording_campaign_v2_current_revisions WHERE campaign_id=$1`, winner.id).Scan(&winnerState); err != nil {
			t.Fatal(err)
		}
		if err = f.pool.QueryRow(ctx, `SELECT state FROM recording_campaign_v2_current_revisions WHERE campaign_id=$1`, loser.id).Scan(&loserState); err != nil {
			t.Fatal(err)
		}
		if winnerState != "planning" || loserState != "draft" {
			t.Fatalf("race states winner=%q loser=%q", winnerState, loserState)
		}
	}

	for _, tc := range []struct {
		name      string
		sameScene bool
	}{
		{name: "same recording left wins"},
		{name: "same recording right wins"},
		{name: "same physical scene left wins", sameScene: true},
		{name: "same physical scene right wins", sameScene: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			suffix := strings.ReplaceAll(tc.name, " ", "-")
			account, actor := seedRosterV2Actor(t, f.pool, "occupancy-"+suffix)
			var leftSubject, rightSubject rosterV2Subject
			if tc.sameScene {
				scene := rosterV2SHA("shared-scene-" + suffix)
				leftSubject = seedRosterV2SubjectsWithScene(t, f.pool, account, actor, suffix+"-left", 1, true, scene)[0]
				rightSubject = seedRosterV2SubjectsWithScene(t, f.pool, account, actor, suffix+"-right", 1, true, scene)[0]
			} else {
				leftSubject = seedRosterV2Subjects(t, f.pool, account, actor, suffix, 1, true)[0]
				rightSubject = leftSubject
			}
			left := createRosterV2Draft(t, f.pool, account, actor, suffix+"-left", "reserve", 1, "operational-disjoint-v1")
			right := createRosterV2Draft(t, f.pool, account, actor, suffix+"-right", "reserve", 1, "operational-disjoint-v1")
			if strings.Contains(tc.name, "right wins") {
				runRace(t, right, left, rightSubject, leftSubject)
			} else {
				runRace(t, left, right, leftSubject, rightSubject)
			}
		})
	}
}

func admitRosterV2Prospectives(t *testing.T, pool *pgxpool.Pool, c rosterV2Campaign, subjects []rosterV2Subject, first int) rosterV2Campaign {
	t.Helper()
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	revisionNo := c.revisionNo + 1
	key := fmt.Sprintf("admit-%d", revisionNo)
	for i := first; i < len(subjects); i++ {
		subject := subjects[i]
		ordinal := i + 1
		recordingSHA := rosterV2SHA(subject.sourceURL)
		streamSHA := rosterV2SHA(subject.sourceURL)
		pageSHA := rosterV2SHA(subject.pageURL)
		snapshot := rosterV2Digest("campaign-roster-v2-r1-source",
			fmt.Sprint(c.account), fmt.Sprint(subject.recordingID), fmt.Sprint(subject.streamID), "0",
			recordingSHA, streamSHA, pageSHA, subject.provider, subject.externalID,
			subject.sourceFamily, subject.captureType, subject.executionClass)
		c.items[ordinal] = strings.Join([]string{
			fmt.Sprint(c.slots[i]), fmt.Sprint(ordinal), "2", "repair", fmt.Sprint(ordinal), "protect",
			fmt.Sprint(subject.recordingID), fmt.Sprint(subject.streamID), subject.sceneSHA,
			snapshot, subject.sceneEvidenceSHA, "0", "",
		}, "|")
	}
	rosterDigest := rosterV2RosterDigest(c.items)
	reasonDigest := rosterV2ReasonDigest("member_admitted")
	planDigest := rosterV2Digest("campaign-roster-v2-r1-plan", fmt.Sprint(c.id), fmt.Sprint(revisionNo), "roster_change", "planning", "planning", c.policyDigest, reasonDigest, rosterDigest)
	requestDigest := rosterV2Digest("campaign-roster-v2-r1-request", fmt.Sprint(c.id), fmt.Sprint(c.account), fmt.Sprint(revisionNo), "roster_change", "planning", "planning", c.policyDigest, key, reasonDigest, planDigest)
	priorRevision := c.revisionID
	var decidedAt time.Time
	if err = tx.QueryRow(ctx, `INSERT INTO recording_campaign_v2_revisions(campaign_id,account_id,revision_number,predecessor_revision_id,decision_uuid,event_type,from_state,to_state,policy_version_id,actor_user_id,actor_role,request_sha256,plan_sha256,idempotency_key,reason_codes) VALUES($1,$2,$3,$4,$5,'roster_change','planning','planning',$6,$7,'owner',$8,$9,$10,ARRAY['member_admitted']) RETURNING id,decided_at`, c.id, c.account, revisionNo, priorRevision, uuid.New(), c.policyID, c.actor, requestDigest, planDigest, key).Scan(&c.revisionID, &decidedAt); err != nil {
		t.Fatal(err)
	}
	c.revisionNo = revisionNo
	for i := first; i < len(subjects); i++ {
		subject := subjects[i]
		ordinal := i + 1
		recordingSHA := rosterV2SHA(subject.sourceURL)
		streamSHA := rosterV2SHA(subject.sourceURL)
		pageSHA := rosterV2SHA(subject.pageURL)
		snapshot := rosterV2Digest("campaign-roster-v2-r1-source",
			fmt.Sprint(c.account), fmt.Sprint(subject.recordingID), fmt.Sprint(subject.streamID), "0",
			recordingSHA, streamSHA, pageSHA, subject.provider, subject.externalID,
			subject.sourceFamily, subject.captureType, subject.executionClass)
		evidenceSHA := rosterV2Digest("campaign-roster-v2-r1-filled-evidence",
			fmt.Sprint(c.id), fmt.Sprint(c.account), fmt.Sprint(c.revisionID), fmt.Sprint(c.policyID),
			fmt.Sprint(subject.recordingID), fmt.Sprint(subject.streamID), "0", snapshot,
			fmt.Sprint(subject.sceneEvidenceID), subject.sceneSHA, subject.sceneEvidenceSHA,
			fmt.Sprint(subject.frameID), fmt.Sprint(subject.mediaID), subject.frameSHA,
			fmt.Sprint(subject.frameCapturedAt.UnixMicro()), fmt.Sprint(subject.sceneVerifiedAt.UnixMicro()), fmt.Sprint(decidedAt.UnixMicro()))
		var evidenceID, headID int64
		if err = tx.QueryRow(ctx, `INSERT INTO recording_campaign_v2_filled_evidence(campaign_id,account_id,created_revision_id,policy_version_id,recording_id,stream_id,source_revision_id,recording_source_sha256,stream_source_sha256,source_page_sha256,provider,external_id,source_family,capture_type,execution_class,source_snapshot_sha256,scene_frame_evidence_id,scene_identity_sha256,scene_evidence_sha256,frame_id,media_object_id,frame_sha256,frame_captured_at,scene_verified_at,accepted_at_decision,evidence_sha256) VALUES($1,$2,$3,$4,$5,$6,NULL,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25) RETURNING id`, c.id, c.account, c.revisionID, c.policyID, subject.recordingID, subject.streamID, recordingSHA, streamSHA, pageSHA, subject.provider, subject.externalID, subject.sourceFamily, subject.captureType, subject.executionClass, snapshot, subject.sceneEvidenceID, subject.sceneSHA, subject.sceneEvidenceSHA, subject.frameID, subject.mediaID, subject.frameSHA, subject.frameCapturedAt, subject.sceneVerifiedAt, decidedAt, evidenceSHA).Scan(&evidenceID); err != nil {
			t.Fatal(err)
		}
		if err = tx.QueryRow(ctx, `INSERT INTO recording_campaign_v2_slot_versions(campaign_id,account_id,slot_id,campaign_revision_id,version,predecessor_version_id,role,rank,disposition,filled_evidence_id,recording_id,stream_id,scene_identity_sha256,effective_from) VALUES($1,$2,$3,$4,2,$5,'repair',$6,'protect',$7,$8,$9,$10,$11) RETURNING id`, c.id, c.account, c.slots[i], c.revisionID, c.heads[i], ordinal, evidenceID, subject.recordingID, subject.streamID, subject.sceneSHA, decidedAt).Scan(&headID); err != nil {
			t.Fatal(err)
		}
		if _, err = tx.Exec(ctx, `INSERT INTO recording_campaign_v2_revision_slot_changes(revision_id,campaign_id,account_id,slot_id,before_version_id,after_version_id) VALUES($1,$2,$3,$4,$5,$6)`, c.revisionID, c.id, c.account, c.slots[i], c.heads[i], headID); err != nil {
			t.Fatal(err)
		}
		if _, err = tx.Exec(ctx, `INSERT INTO recording_campaign_v2_recording_occupancy(account_id,occupancy_scope,recording_id,campaign_id,slot_id,slot_version_id) VALUES($1,'operational',$2,$3,$4,$5)`, c.account, subject.recordingID, c.id, c.slots[i], headID); err != nil {
			t.Fatal(err)
		}
		if _, err = tx.Exec(ctx, `UPDATE recording_campaign_v2_scene_occupancy SET slot_version_id=$1 WHERE account_id=$2 AND occupancy_scope='operational' AND scene_identity_sha256=$3 AND campaign_id=$4 AND slot_id=$5`, headID, c.account, subject.sceneSHA, c.id, c.slots[i]); err != nil {
			t.Fatal(err)
		}
		c.heads[i] = headID
	}
	insertRosterV2Seal(t, tx, c, key, requestDigest, planDigest, rosterDigest, len(subjects), 0, 0, 0, len(subjects))
	if err = tx.Commit(ctx); err != nil {
		t.Fatalf("admit repair prospectives: %v", err)
	}
	return c
}

func TestRecordingCampaignV2R1ExactLifecycleDenominatorsAndOccupancy(t *testing.T) {
	f := openRosterV2PG(t)
	account, actor := seedRosterV2Actor(t, f.pool, "lifecycle")
	tests := []struct {
		key     string
		kind    string
		target  int
		filled  int
		overlap string
	}{
		{key: "delivery30", kind: "delivery30", target: 30, filled: 30, overlap: "operational-disjoint-v1"},
		{key: "repair21", kind: "repair21", target: 21, filled: 17, overlap: "operational-disjoint-v1"},
		{key: "reserve5", kind: "reserve", target: 5, filled: 3, overlap: "operational-disjoint-v1"},
		{key: "strict50", kind: "strict50", target: 50, filled: 50, overlap: "strict-may-overlap-operational-v1"},
	}
	for _, tc := range tests {
		t.Run(tc.key, func(t *testing.T) {
			subjects := seedRosterV2Subjects(t, f.pool, account, actor, tc.key, tc.target, tc.filled == tc.target)
			if tc.filled < tc.target {
				materializeRosterV2Recordings(t, f.pool, account, tc.key, subjects[:tc.filled])
			}
			campaign := createRosterV2Draft(t, f.pool, account, actor, tc.key, tc.kind, tc.target, tc.overlap)
			campaign = enterRosterV2Planning(t, f.pool, campaign, subjects, tc.filled)
			if tc.kind == "repair21" {
				if _, err := activateRosterV2(t, f.pool, campaign, tc.filled, tc.target-tc.filled); err == nil {
					t.Fatal("repair21 activated with four prospective slots")
				}
				materializeRosterV2Recordings(t, f.pool, account, tc.key+"-admit", subjects[tc.filled:])
				campaign = admitRosterV2Prospectives(t, f.pool, campaign, subjects, tc.filled)
				var err error
				campaign, err = activateRosterV2(t, f.pool, campaign, tc.target, 0)
				if err != nil {
					t.Fatalf("activate exact repair21: %v", err)
				}
			} else if tc.kind == "delivery30" || tc.kind == "strict50" {
				var err error
				campaign, err = activateRosterV2(t, f.pool, campaign, tc.filled, tc.target-tc.filled)
				if err != nil {
					t.Fatalf("activate exact %s: %v", tc.kind, err)
				}
			}
			var slots, heads, occupiedRecordings, occupiedScenes int
			if err := f.pool.QueryRow(context.Background(), `SELECT count(*) FROM recording_campaign_v2_slots WHERE campaign_id=$1`, campaign.id).Scan(&slots); err != nil {
				t.Fatal(err)
			}
			if err := f.pool.QueryRow(context.Background(), `SELECT count(*) FROM recording_campaign_v2_current_slot_heads WHERE campaign_id=$1`, campaign.id).Scan(&heads); err != nil {
				t.Fatal(err)
			}
			if err := f.pool.QueryRow(context.Background(), `SELECT count(*) FROM recording_campaign_v2_recording_occupancy WHERE campaign_id=$1`, campaign.id).Scan(&occupiedRecordings); err != nil {
				t.Fatal(err)
			}
			if err := f.pool.QueryRow(context.Background(), `SELECT count(*) FROM recording_campaign_v2_scene_occupancy WHERE campaign_id=$1`, campaign.id).Scan(&occupiedScenes); err != nil {
				t.Fatal(err)
			}
			if slots != tc.target || heads != tc.target || occupiedRecordings != tc.filled || occupiedScenes != tc.target {
				t.Fatalf("projection mismatch slots=%d heads=%d recordings=%d scenes=%d", slots, heads, occupiedRecordings, occupiedScenes)
			}
		})
	}
}

func TestRecordingCampaignV2R1ActivationRevalidatesCurrentEvidenceAndSource(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate string
	}{
		{name: "paused recording", mutate: `UPDATE recordings SET paused_at=now() WHERE id=$1`},
		{name: "source drift", mutate: `UPDATE streams SET source_url=source_url||'?drift=1' WHERE id=(SELECT stream_id FROM recordings WHERE id=$1)`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := openRosterV2PG(t)
			account, actor := seedRosterV2Actor(t, f.pool, "activation-"+strings.ReplaceAll(tc.name, " ", "-"))
			subject := seedRosterV2Subjects(t, f.pool, account, actor, "activation-"+tc.name, 1, true)[0]
			campaign := createRosterV2Draft(t, f.pool, account, actor, "activation-"+strings.ReplaceAll(tc.name, " ", "-"), "reserve", 1, "operational-disjoint-v1")
			campaign = enterRosterV2Planning(t, f.pool, campaign, []rosterV2Subject{subject}, 1)
			if _, err := f.pool.Exec(context.Background(), tc.mutate, subject.recordingID, campaign.slots[0]); err != nil {
				t.Fatal(err)
			}
			if _, err := activateRosterV2(t, f.pool, campaign, 1, 0); err == nil || !strings.Contains(err.Error(), "stale or source-divergent") {
				t.Fatalf("activation mutation %q err=%v", tc.name, err)
			}
		})
	}
	t.Run("evidence ages between planning and activation", func(t *testing.T) {
		f := openRosterV2PG(t)
		account, actor := seedRosterV2Actor(t, f.pool, "activation-aged")
		subject := seedRosterV2SubjectsAt(t, f.pool, account, actor, "activation-aged", 1, true, "", -55*time.Second, 0)[0]
		campaign := createRosterV2DraftWithFreshness(t, f.pool, account, actor, "activation-aged", "reserve", 1, "operational-disjoint-v1", 60)
		campaign = enterRosterV2Planning(t, f.pool, campaign, []rosterV2Subject{subject}, 1)
		if wait := time.Until(subject.frameCapturedAt.Add(60*time.Second + 250*time.Millisecond)); wait > 0 {
			time.Sleep(wait)
		}
		var evidenceCurrent bool
		if err := f.pool.QueryRow(context.Background(), `SELECT evidence_current FROM recording_campaign_v2_evidence_freshness WHERE campaign_id=$1`, campaign.id).Scan(&evidenceCurrent); err != nil || evidenceCurrent {
			t.Fatalf("stale frame/fresh verification projection current=%t err=%v", evidenceCurrent, err)
		}
		if _, err := activateRosterV2(t, f.pool, campaign, 1, 0); err == nil || !strings.Contains(err.Error(), "stale or source-divergent") {
			t.Fatalf("aged activation err=%v", err)
		}
	})
	for _, tc := range []struct {
		name   string
		mutate string
	}{
		{name: "prospective source drift", mutate: `UPDATE streams SET source_url=source_url||'?drift=1' WHERE id=$1`},
		{name: "prospective page drift", mutate: `UPDATE streams SET source_page_url=source_page_url||'?drift=1' WHERE id=$1`},
		{name: "prospective provider drift", mutate: `UPDATE streams SET provider='drifted-provider' WHERE id=$1`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := openRosterV2PG(t)
			account, actor := seedRosterV2Actor(t, f.pool, "activation-"+strings.ReplaceAll(tc.name, " ", "-"))
			subject := seedRosterV2Subjects(t, f.pool, account, actor, "activation-"+tc.name, 1, false)[0]
			campaign := createRosterV2Draft(t, f.pool, account, actor, "activation-"+strings.ReplaceAll(tc.name, " ", "-"), "reserve", 1, "operational-disjoint-v1")
			campaign = enterRosterV2Planning(t, f.pool, campaign, []rosterV2Subject{subject}, 0)
			if _, err := f.pool.Exec(context.Background(), tc.mutate, subject.streamID); err != nil {
				t.Fatal(err)
			}
			if _, err := activateRosterV2(t, f.pool, campaign, 0, 1); err == nil || !strings.Contains(err.Error(), "prospective evidence is stale or source-divergent") {
				t.Fatalf("activation mutation %q err=%v", tc.name, err)
			}
		})
	}
	t.Run("prospective evidence ages between planning and activation", func(t *testing.T) {
		f := openRosterV2PG(t)
		account, actor := seedRosterV2Actor(t, f.pool, "activation-prospective-aged")
		subject := seedRosterV2SubjectsAt(t, f.pool, account, actor, "activation-prospective-aged", 1, false, "", -55*time.Second, 0)[0]
		campaign := createRosterV2DraftWithFreshness(t, f.pool, account, actor, "activation-prospective-aged", "reserve", 1, "operational-disjoint-v1", 60)
		campaign = enterRosterV2Planning(t, f.pool, campaign, []rosterV2Subject{subject}, 0)
		if wait := time.Until(subject.frameCapturedAt.Add(60*time.Second + 250*time.Millisecond)); wait > 0 {
			time.Sleep(wait)
		}
		if _, err := activateRosterV2(t, f.pool, campaign, 0, 1); err == nil || !strings.Contains(err.Error(), "prospective evidence is stale or source-divergent") {
			t.Fatalf("aged prospective activation err=%v", err)
		}
	})
}

func TestRecordingCampaignV2R1ActivationLocksOppositeResourceMappingsWithoutDeadlock(t *testing.T) {
	f := openRosterV2PG(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	account, actor := seedRosterV2Actor(t, f.pool, "activation-opposite")
	leftSubjects := seedRosterV2Subjects(t, f.pool, account, actor, "activation-opposite-left", 2, false)
	rightSubjects := cloneRosterV2ProspectiveEvidence(t, f.pool, account, actor, "activation-opposite-right", []rosterV2Subject{leftSubjects[1], leftSubjects[0]})
	left := createRosterV2Draft(t, f.pool, account, actor, "activation-opposite-left", "reserve", 2, "operational-disjoint-v1")
	right := createRosterV2Draft(t, f.pool, account, actor, "activation-opposite-right", "reserve", 2, "operational-disjoint-v1")
	left = enterRosterV2Planning(t, f.pool, left, leftSubjects, 0)
	right = enterRosterV2Planning(t, f.pool, right, rightSubjects, 0)

	leftTx, leftNext, err := prepareActivateRosterV2(f.pool, left, 0, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer leftTx.Rollback(context.Background())
	pidCh := make(chan int32, 1)
	resultCh := make(chan error, 1)
	go func() {
		rightTx, beginErr := f.pool.Begin(ctx)
		if beginErr != nil {
			resultCh <- beginErr
			return
		}
		defer rightTx.Rollback(context.Background())
		var pid int32
		if beginErr = rightTx.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&pid); beginErr != nil {
			resultCh <- beginErr
			return
		}
		pidCh <- pid
		_, activateErr := writeActivateRosterV2(rightTx, right, 0, 2)
		if activateErr == nil {
			activateErr = rightTx.Commit(ctx)
		}
		resultCh <- activateErr
	}()
	waitForRosterV2PostgresLock(t, f.pool, <-pidCh)
	if err = leftTx.Commit(ctx); err != nil {
		t.Fatalf("first opposite-order activation commit: %v", err)
	}
	leftNext.state = "active"
	if err = <-resultCh; err != nil {
		t.Fatalf("second opposite-order activation deadlocked or failed: %v", err)
	}
	var active int
	if err = f.pool.QueryRow(ctx, `SELECT count(*) FROM recording_campaign_v2_current_revisions WHERE campaign_id IN($1,$2) AND state='active'`, leftNext.id, right.id).Scan(&active); err != nil || active != 2 {
		t.Fatalf("opposite resource activation coherent states=%d err=%v", active, err)
	}
}

func TestRecordingCampaignV2R1EvidenceFreshnessRejectsStaleFrameAndFutureObservation(t *testing.T) {
	for _, tc := range []struct {
		name           string
		capturedOffset time.Duration
		verifiedOffset time.Duration
	}{
		{name: "stale frame with fresh verification", capturedOffset: -2 * time.Hour},
		{name: "future frame and verification", capturedOffset: time.Minute, verifiedOffset: time.Minute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := openRosterV2PG(t)
			account, actor := seedRosterV2Actor(t, f.pool, "freshness-"+strings.ReplaceAll(tc.name, " ", "-"))
			subject := seedRosterV2SubjectsAt(t, f.pool, account, actor, "freshness-"+tc.name, 1, true, "", tc.capturedOffset, tc.verifiedOffset)[0]
			campaign := createRosterV2Draft(t, f.pool, account, actor, "freshness-"+strings.ReplaceAll(tc.name, " ", "-"), "reserve", 1, "operational-disjoint-v1")
			tx, err := f.pool.Begin(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(context.Background())
			if _, err = writeOneRosterV2Planning(tx, campaign, subject); err == nil || !strings.Contains(err.Error(), "freshness mismatch") {
				t.Fatalf("freshness case %q err=%v", tc.name, err)
			}
		})
	}
	t.Run("null revision rejects current source diverged from frozen baseline", func(t *testing.T) {
		f := openRosterV2PG(t)
		account, actor := seedRosterV2Actor(t, f.pool, "baseline-diverged")
		subject := seedRosterV2Subjects(t, f.pool, account, actor, "baseline-diverged", 1, true)[0]
		newURL := subject.sourceURL + "?unattested=1"
		if _, err := f.pool.Exec(context.Background(), `UPDATE streams SET source_url=$2 WHERE id=$1`, subject.streamID, newURL); err != nil {
			t.Fatal(err)
		}
		if _, err := f.pool.Exec(context.Background(), `UPDATE recordings SET stream_url=$2 WHERE id=$1`, subject.recordingID, newURL); err != nil {
			t.Fatal(err)
		}
		subject.sourceURL = newURL
		campaign := createRosterV2Draft(t, f.pool, account, actor, "baseline-diverged", "reserve", 1, "operational-disjoint-v1")
		tx, err := f.pool.Begin(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(context.Background())
		if _, err = writeOneRosterV2Planning(tx, campaign, subject); err == nil || !strings.Contains(err.Error(), "exact frozen source") {
			t.Fatalf("null revision accepted source diverged from baseline err=%v", err)
		}
	})
}

func TestRecordingCampaignV2R1DirectSQLBypassesFailClosed(t *testing.T) {
	f := openRosterV2PG(t)
	ctx := context.Background()
	account, actor := seedRosterV2Actor(t, f.pool, "bypass")
	subject := seedRosterV2Subjects(t, f.pool, account, actor, "bypass", 1, true)[0]
	campaign := createRosterV2Draft(t, f.pool, account, actor, "bypass", "reserve", 1, "operational-disjoint-v1")
	campaign = enterRosterV2Planning(t, f.pool, campaign, []rosterV2Subject{subject}, 1)
	multiTx, err := f.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer multiTx.Rollback(context.Background())
	var sameTxRevision int64
	if err = multiTx.QueryRow(ctx, `INSERT INTO recording_campaign_v2_revisions(campaign_id,account_id,revision_number,predecessor_revision_id,decision_uuid,event_type,from_state,to_state,policy_version_id,actor_user_id,actor_role,request_sha256,plan_sha256,idempotency_key,reason_codes) VALUES($1,$2,3,$3,$4,'roster_change','planning','planning',$5,$6,'owner',repeat('a',64),repeat('b',64),'same-tx-first',ARRAY['member_probation']) RETURNING id`, campaign.id, account, campaign.revisionID, uuid.New(), campaign.policyID, actor).Scan(&sameTxRevision); err != nil {
		t.Fatal(err)
	}
	if _, err = multiTx.Exec(ctx, `INSERT INTO recording_campaign_v2_revision_commits(revision_id,campaign_id,account_id,roster_sha256,filled_count,prospective_count,vacant_count,replace_count,protected_count) VALUES($1,$2,$3,recording_campaign_v2_roster_digest($2),1,0,0,0,1)`, sameTxRevision, campaign.id, account); err != nil {
		t.Fatal(err)
	}
	if _, err = multiTx.Exec(ctx, `INSERT INTO recording_campaign_v2_revisions(campaign_id,account_id,revision_number,predecessor_revision_id,decision_uuid,event_type,from_state,to_state,policy_version_id,actor_user_id,actor_role,request_sha256,plan_sha256,idempotency_key,reason_codes) VALUES($1,$2,4,$3,$4,'roster_change','planning','planning',$5,$6,'owner',repeat('c',64),repeat('d',64),'same-tx-second',ARRAY['member_probation'])`, campaign.id, account, sameTxRevision, uuid.New(), campaign.policyID, actor); err == nil || !strings.Contains(err.Error(), "sealed exact predecessor") {
		t.Fatalf("second same-campaign revision in one transaction err=%v", err)
	}
	_ = multiTx.Rollback(ctx)

	if _, err := f.pool.Exec(ctx, `INSERT INTO recording_campaign_v2_slots(campaign_id,account_id,ordinal,created_revision_id) VALUES($1,$2,2,$3)`, campaign.id, account, campaign.revisionID); err == nil {
		t.Fatal("late slot insertion after revision seal succeeded")
	}
	if _, err := f.pool.Exec(ctx, `INSERT INTO recording_campaign_v2_revisions(campaign_id,account_id,revision_number,predecessor_revision_id,decision_uuid,event_type,from_state,to_state,policy_version_id,actor_user_id,actor_role,request_sha256,plan_sha256,idempotency_key,reason_codes) VALUES($1,$2,3,$3,$4,'complete','planning','complete',$5,$6,'owner',repeat('a',64),repeat('b',64),'complete',ARRAY['completion_gates_met'])`, campaign.id, account, campaign.revisionID, uuid.New(), campaign.policyID, actor); err == nil {
		t.Fatal("R1 completion bypass succeeded without later typed qualification evidence")
	}
	deleteTx, err := f.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = deleteTx.Exec(ctx, `DELETE FROM recording_campaign_v2_recording_occupancy WHERE account_id=$1 AND recording_id=$2`, account, subject.recordingID); err != nil {
		_ = deleteTx.Rollback(ctx)
		t.Fatal(err)
	}
	if err = deleteTx.Commit(ctx); err == nil {
		t.Fatal("occupancy registry deletion committed while current head still requires it")
	}
	var recordingOccupancy, sceneOccupancy int
	if err = f.pool.QueryRow(ctx, `SELECT count(*) FROM recording_campaign_v2_recording_occupancy WHERE account_id=$1 AND recording_id=$2`, account, subject.recordingID).Scan(&recordingOccupancy); err != nil {
		t.Fatal(err)
	}
	if err = f.pool.QueryRow(ctx, `SELECT count(*) FROM recording_campaign_v2_scene_occupancy WHERE account_id=$1 AND scene_identity_sha256=$2`, account, subject.sceneSHA).Scan(&sceneOccupancy); err != nil {
		t.Fatal(err)
	}
	if recordingOccupancy != 1 || sceneOccupancy != 1 {
		t.Fatalf("failed occupancy bypass changed registry recordings=%d scenes=%d", recordingOccupancy, sceneOccupancy)
	}
	if _, err = f.pool.Exec(ctx, `TRUNCATE recording_campaign_v2_recording_occupancy`); err == nil {
		t.Fatal("recording occupancy TRUNCATE bypass succeeded")
	}
	if _, err = f.pool.Exec(ctx, `TRUNCATE recording_campaign_v2_scene_occupancy`); err == nil {
		t.Fatal("scene occupancy TRUNCATE bypass succeeded")
	}
	if _, err = f.pool.Exec(ctx, `INSERT INTO recording_campaign_v2_idempotency(account_id,campaign_id,idempotency_key,request_sha256,plan_sha256,response_sha256,response_bytes,revision_id) SELECT account_id,campaign_id,idempotency_key,repeat('f',64),plan_sha256,response_sha256,response_bytes,revision_id FROM recording_campaign_v2_idempotency WHERE revision_id=$1`, campaign.revisionID); err == nil {
		t.Fatal("changed idempotency replay succeeded")
	}
}

func TestRecordingCampaignV2R1CreatorAndCanonicalDigestParity(t *testing.T) {
	f := openRosterV2PG(t)
	ctx := context.Background()
	account, actor := seedRosterV2Actor(t, f.pool, "digest")
	var otherActor int64
	if err := f.pool.QueryRow(ctx, `INSERT INTO users(email,name,is_operator,email_verified_at) VALUES($1,'other',true,now()) RETURNING id`, fmt.Sprintf("other-%d@example.test", time.Now().UnixNano())).Scan(&otherActor); err != nil {
		t.Fatal(err)
	}
	forgedTx, err := f.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var campaign, policy int64
	var createdAt time.Time
	if err = forgedTx.QueryRow(ctx, `INSERT INTO recording_campaign_v2_campaigns(account_id,campaign_key,kind,created_by_user_id) VALUES($1,'forged-creator','reserve',$2) RETURNING id,created_at`, account, actor).Scan(&campaign, &createdAt); err != nil {
		t.Fatal(err)
	}
	deadline := createdAt.Add(24 * time.Hour)
	policyDigest := rosterV2Digest("campaign-roster-v2-r1-policy", "reserve", "1", "1", fmt.Sprint(deadline.UnixMicro()), "GOOD", "0", "3600", "operational-v1", "operational-disjoint-v1", "campaign-roster-v2-r1")
	if err = forgedTx.QueryRow(ctx, `INSERT INTO recording_campaign_v2_policy_versions(campaign_id,account_id,version,policy_schema,target_count,deadline_at,grade_floor,required_consecutive_windows,evidence_freshness_seconds,selection_group,overlap_policy,definition_sha256) VALUES($1,$2,1,'campaign-roster-v2-r1',1,$3,'GOOD',0,3600,'operational-v1','operational-disjoint-v1',$4) RETURNING id`, campaign, account, deadline, policyDigest).Scan(&policy); err != nil {
		t.Fatal(err)
	}
	if _, err = forgedTx.Exec(ctx, `INSERT INTO recording_campaign_v2_revisions(campaign_id,account_id,revision_number,decision_uuid,event_type,from_state,to_state,policy_version_id,actor_user_id,actor_role,decided_at,request_sha256,plan_sha256,idempotency_key,reason_codes) VALUES($1,$2,1,$3,'create',NULL,'draft',$4,$5,'operator',$6,repeat('a',64),repeat('b',64),'forged',ARRAY['campaign_created'])`, campaign, account, uuid.New(), policy, otherActor, createdAt); err == nil {
		t.Fatal("create revision accepted actor different from immutable campaign creator")
	}
	_ = forgedTx.Rollback(ctx)

	wrongResponseTx, err := f.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer wrongResponseTx.Rollback(context.Background())
	var responseCampaign, responsePolicy, responseRevision int64
	var responseCreated time.Time
	if err = wrongResponseTx.QueryRow(ctx, `INSERT INTO recording_campaign_v2_campaigns(account_id,campaign_key,kind,created_by_user_id) VALUES($1,'forged-response','reserve',$2) RETURNING id,created_at`, account, actor).Scan(&responseCampaign, &responseCreated); err != nil {
		t.Fatal(err)
	}
	responseDeadline := responseCreated.Add(24 * time.Hour)
	responsePolicyDigest := rosterV2Digest("campaign-roster-v2-r1-policy", "reserve", "1", "1", fmt.Sprint(responseDeadline.UnixMicro()), "GOOD", "0", "3600", "operational-v1", "operational-disjoint-v1", "campaign-roster-v2-r1")
	if err = wrongResponseTx.QueryRow(ctx, `INSERT INTO recording_campaign_v2_policy_versions(campaign_id,account_id,version,policy_schema,target_count,deadline_at,grade_floor,required_consecutive_windows,evidence_freshness_seconds,selection_group,overlap_policy,definition_sha256) VALUES($1,$2,1,'campaign-roster-v2-r1',1,$3,'GOOD',0,3600,'operational-v1','operational-disjoint-v1',$4) RETURNING id`, responseCampaign, account, responseDeadline, responsePolicyDigest).Scan(&responsePolicy); err != nil {
		t.Fatal(err)
	}
	requestSHA, planSHA := rosterV2SHA("wrong-response-request"), rosterV2SHA("wrong-response-plan")
	if err = wrongResponseTx.QueryRow(ctx, `INSERT INTO recording_campaign_v2_revisions(campaign_id,account_id,revision_number,decision_uuid,event_type,from_state,to_state,policy_version_id,actor_user_id,actor_role,decided_at,request_sha256,plan_sha256,idempotency_key,reason_codes) VALUES($1,$2,1,$3,'create',NULL,'draft',$4,$5,'owner',$6,$7,$8,'wrong-response',ARRAY['campaign_created']) RETURNING id`, responseCampaign, account, uuid.New(), responsePolicy, actor, responseCreated, requestSHA, planSHA).Scan(&responseRevision); err != nil {
		t.Fatal(err)
	}
	wrong := []byte("evil")
	if _, err = wrongResponseTx.Exec(ctx, `INSERT INTO recording_campaign_v2_idempotency(account_id,campaign_id,idempotency_key,request_sha256,plan_sha256,response_sha256,response_bytes,revision_id) VALUES($1,$2,'wrong-response',$3,$4,encode(sha256($5),'hex'),$5,$6)`, account, responseCampaign, requestSHA, planSHA, wrong, responseRevision); err == nil || !strings.Contains(err.Error(), "response mismatch") {
		t.Fatalf("self-consistent noncanonical response err=%v", err)
	}
	_ = wrongResponseTx.Rollback(ctx)

	subject := seedRosterV2Subjects(t, f.pool, account, actor, "digest", 1, true)[0]
	valid := createRosterV2Draft(t, f.pool, account, actor, "digest-valid", "reserve", 1, "operational-disjoint-v1")
	valid = enterRosterV2Planning(t, f.pool, valid, []rosterV2Subject{subject}, 1)
	var sqlRoster, sqlPlan, sqlRequest string
	if err = f.pool.QueryRow(ctx, `SELECT recording_campaign_v2_roster_digest($1),recording_campaign_v2_plan_digest($2),recording_campaign_v2_request_digest($2)`, valid.id, valid.revisionID).Scan(&sqlRoster, &sqlPlan, &sqlRequest); err != nil {
		t.Fatal(err)
	}
	if sqlRoster != rosterV2RosterDigest(valid.items) {
		t.Fatalf("Go/SQL roster digest mismatch go=%s sql=%s", rosterV2RosterDigest(valid.items), sqlRoster)
	}
	forgedItems := map[int]string{1: valid.items[1]}
	parts := strings.Split(forgedItems[1], "|")
	parts[0] = fmt.Sprint(valid.slots[0] + 1)
	forgedItems[1] = strings.Join(parts, "|")
	if rosterV2RosterDigest(forgedItems) == sqlRoster {
		t.Fatal("stable-slot identity substitution did not change canonical roster digest")
	}
	var storedPlan, storedRequest string
	if err = f.pool.QueryRow(ctx, `SELECT plan_sha256,request_sha256 FROM recording_campaign_v2_revisions WHERE id=$1`, valid.revisionID).Scan(&storedPlan, &storedRequest); err != nil {
		t.Fatal(err)
	}
	if sqlPlan != storedPlan || sqlRequest != storedRequest {
		t.Fatalf("Go/SQL plan/request mismatch plan=%s/%s request=%s/%s", storedPlan, sqlPlan, storedRequest, sqlRequest)
	}
}
