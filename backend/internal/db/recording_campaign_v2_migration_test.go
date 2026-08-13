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
	for _, forbidden := range []string{"recording_campaign_v2_seed", "recording_campaign_v2_apply", "recording_campaign_v2_swap"} {
		var present *string
		if err := f.pool.QueryRow(ctx, `SELECT to_regprocedure($1)::text`, forbidden+"()").Scan(&present); err != nil || present != nil {
			t.Fatalf("forbidden writer function %s present=%v err=%v", forbidden, present, err)
		}
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
	b := strings.Builder{}
	for _, ordinal := range ordinals {
		item := items[ordinal]
		fmt.Fprintf(&b, "%d:%s\n", len([]byte(item)), item)
	}
	return rosterV2Digest("campaign-roster-v2-r1-roster", b.String())
}

func TestRecordingCampaignV2R1SourceLineageIsAppendOnlyAndBaselineFailsClosed(t *testing.T) {
	f := openRosterV2PG(t)
	ctx := context.Background()
	var stream int64
	if err := f.pool.QueryRow(ctx, `INSERT INTO streams(provider,external_id,name,slug,source_url,source_page_url,capture_type,source_family,execution_class,capture_family,expected_fps,recording_state) VALUES('test','lineage','lineage','r1-lineage','https://example.test/a.m3u8','https://example.test/a','hls','video_manifest','video_live','continuous_video',30,'on') RETURNING id`).Scan(&stream); err != nil {
		t.Fatal(err)
	}
	var first int64
	if err := f.pool.QueryRow(ctx, `INSERT INTO stream_source_revisions(stream_id,actor,reason,previous_source_url,new_source_url,previous_source_page_url,new_source_page_url,previous_source_family,new_source_family,previous_capture_type,new_capture_type,previous_execution_class,new_execution_class) SELECT id,'test','baseline','','source_url','','source_page_url','','source_family','','capture_type','','execution_class' FROM streams WHERE id=$1 RETURNING id`, stream).Scan(&first); err != nil {
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
	if err := f.pool.QueryRow(ctx, `INSERT INTO streams(provider,external_id,name,slug,source_url,source_page_url,capture_type,source_family,execution_class,capture_family,expected_fps,recording_state) VALUES('test','lineage-race','lineage race','r1-lineage-race','https://example.test/race-a.m3u8','https://example.test/race','hls','video_manifest','video_live','continuous_video',30,'on') RETURNING id`).Scan(&raceStream); err != nil {
		t.Fatal(err)
	}
	if _, err := f.pool.Exec(ctx, `INSERT INTO stream_source_revisions(stream_id,actor,reason,previous_source_url,new_source_url,previous_source_page_url,new_source_page_url,previous_source_family,new_source_family,previous_capture_type,new_capture_type,previous_execution_class,new_execution_class) SELECT id,'test','initial','','source_url','','source_page_url','','source_family','','capture_type','','execution_class' FROM streams WHERE id=$1`, raceStream); err != nil {
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
}

func TestRecordingCampaignV2R1DeployedUpgradeValidatesExistingSourceLineage(t *testing.T) {
	for _, tc := range []struct {
		name   string
		broken bool
	}{
		{name: "coherent terminal divergence is preserved"},
		{name: "branched legacy history blocks migration", broken: true},
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
			if _, err := f.pool.Exec(ctx, `INSERT INTO stream_source_revisions(stream_id,actor,reason,previous_source_url,new_source_url,previous_source_page_url,new_source_page_url,previous_source_family,new_source_family,previous_capture_type,new_capture_type,previous_execution_class,new_execution_class,created_at) VALUES($1,'test','first','https://example.test/original.m3u8','https://example.test/mid.m3u8','https://example.test/page','https://example.test/page','video_manifest','video_manifest','hls','hls','video_live','video_live',now()-interval '2 minutes')`, stream); err != nil {
				t.Fatal(err)
			}
			previous := "https://example.test/mid.m3u8"
			if tc.broken {
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
	if err = tx.QueryRow(ctx, `INSERT INTO recording_campaign_v2_campaigns(account_id,campaign_key,kind,created_by_user_id) VALUES($1,'append-only','reserve',$2) RETURNING id`, account, actor).Scan(&campaign); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().UTC().Add(48 * time.Hour).Truncate(time.Microsecond)
	policyDigest := rosterV2Digest("campaign-roster-v2-r1-policy", "reserve", "1", "1", fmt.Sprint(deadline.UnixMicro()), "GOOD", "0", "3600", "operational-v1", "operational-disjoint-v1", "campaign-roster-v2-r1")
	if err = tx.QueryRow(ctx, `INSERT INTO recording_campaign_v2_policy_versions(campaign_id,account_id,version,policy_schema,target_count,deadline_at,grade_floor,required_consecutive_windows,evidence_freshness_seconds,selection_group,overlap_policy,definition_sha256) VALUES($1,$2,1,'campaign-roster-v2-r1',1,$3,'GOOD',0,3600,'operational-v1','operational-disjoint-v1',$4) RETURNING id`, campaign, account, deadline, policyDigest).Scan(&policy); err != nil {
		t.Fatal(err)
	}
	rosterDigest := rosterV2RosterDigest(nil)
	planDigest := rosterV2Digest("campaign-roster-v2-r1-plan", fmt.Sprint(campaign), "1", "create", "", "draft", policyDigest, "campaign_created", rosterDigest)
	requestDigest := rosterV2Digest("campaign-roster-v2-r1-request", fmt.Sprint(campaign), fmt.Sprint(account), "1", "create", "", "draft", policyDigest, "create", "campaign_created", planDigest)
	var revision int64
	if err = tx.QueryRow(ctx, `INSERT INTO recording_campaign_v2_revisions(campaign_id,account_id,revision_number,decision_uuid,event_type,from_state,to_state,policy_version_id,actor_user_id,actor_role,request_sha256,plan_sha256,idempotency_key,reason_codes) VALUES($1,$2,1,$3,'create',NULL,'draft',$4,$5,'owner',$6,$7,'create',ARRAY['campaign_created']) RETURNING id`, campaign, account, uuid.New(), policy, actor, requestDigest, planDigest).Scan(&revision); err != nil {
		t.Fatal(err)
	}
	response := []byte(`{"state":"draft"}`)
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
	return seedRosterV2SubjectsWithScene(t, pool, account, actor, suffix, count, filled, "")
}

func seedRosterV2SubjectsWithScene(t *testing.T, pool *pgxpool.Pool, account, actor int64, suffix string, count int, filled bool, sceneOverride string) []rosterV2Subject {
	t.Helper()
	ctx := context.Background()
	destination := seedRosterV2Destination(t, pool, account)
	out := make([]rosterV2Subject, 0, count)
	for i := 1; i <= count; i++ {
		s := rosterV2Subject{
			sceneSHA:       fmt.Sprintf("%064x", 100000+i),
			frameSHA:       fmt.Sprintf("%064x", 200000+i),
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
	t.Helper()
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	var c rosterV2Campaign
	c.account, c.actor, c.kind, c.target = account, actor, kind, target
	if err = tx.QueryRow(ctx, `INSERT INTO recording_campaign_v2_campaigns(account_id,campaign_key,kind,created_by_user_id) VALUES($1,$2,$3,$4) RETURNING id`, account, key, kind, actor).Scan(&c.id); err != nil {
		t.Fatal(err)
	}
	c.deadline = time.Now().UTC().Add(72 * time.Hour).Truncate(time.Microsecond)
	group := "operational-v1"
	if kind == "strict50" {
		group = "strict-v1"
	}
	c.policyDigest = rosterV2Digest("campaign-roster-v2-r1-policy", kind, "1", fmt.Sprint(target), fmt.Sprint(c.deadline.UnixMicro()), "GOOD", "0", "3600", group, overlap, "campaign-roster-v2-r1")
	if err = tx.QueryRow(ctx, `INSERT INTO recording_campaign_v2_policy_versions(campaign_id,account_id,version,policy_schema,target_count,deadline_at,grade_floor,required_consecutive_windows,evidence_freshness_seconds,selection_group,overlap_policy,definition_sha256) VALUES($1,$2,1,'campaign-roster-v2-r1',$3,$4,'GOOD',0,3600,$5,$6,$7) RETURNING id`, c.id, account, target, c.deadline, group, overlap, c.policyDigest).Scan(&c.policyID); err != nil {
		t.Fatal(err)
	}
	rosterDigest := rosterV2RosterDigest(nil)
	planDigest := rosterV2Digest("campaign-roster-v2-r1-plan", fmt.Sprint(c.id), "1", "create", "", "draft", c.policyDigest, "campaign_created", rosterDigest)
	requestDigest := rosterV2Digest("campaign-roster-v2-r1-request", fmt.Sprint(c.id), fmt.Sprint(account), "1", "create", "", "draft", c.policyDigest, "create", "campaign_created", planDigest)
	if err = tx.QueryRow(ctx, `INSERT INTO recording_campaign_v2_revisions(campaign_id,account_id,revision_number,decision_uuid,event_type,from_state,to_state,policy_version_id,actor_user_id,actor_role,request_sha256,plan_sha256,idempotency_key,reason_codes) VALUES($1,$2,1,$3,'create',NULL,'draft',$4,$5,'owner',$6,$7,'create',ARRAY['campaign_created']) RETURNING id`, c.id, account, uuid.New(), c.policyID, actor, requestDigest, planDigest).Scan(&c.revisionID); err != nil {
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
				fmt.Sprint(ordinal), "1", role, fmt.Sprint(ordinal), "protect",
				fmt.Sprint(subject.recordingID), fmt.Sprint(subject.streamID), subject.sceneSHA,
				snapshot, subject.sceneEvidenceSHA, "0", "",
			}, "|")
		} else {
			c.items[ordinal] = strings.Join([]string{
				fmt.Sprint(ordinal), "1", role, fmt.Sprint(ordinal), "prospective",
				"0", fmt.Sprint(subject.streamID), subject.sceneSHA,
				rosterV2SHA(subject.pageURL), subject.sceneEvidenceSHA, "0", rosterV2SHA(subject.sourceURL),
			}, "|")
		}
	}
	rosterDigest := rosterV2RosterDigest(c.items)
	planDigest := rosterV2Digest("campaign-roster-v2-r1-plan", fmt.Sprint(c.id), "2", "enter_planning", "draft", "planning", c.policyDigest, strings.Join(reasons, ","), rosterDigest)
	requestDigest := rosterV2Digest("campaign-roster-v2-r1-request", fmt.Sprint(c.id), fmt.Sprint(c.account), "2", "enter_planning", "draft", "planning", c.policyDigest, "planning", strings.Join(reasons, ","), planDigest)
	var decidedAt time.Time
	if err = tx.QueryRow(ctx, `INSERT INTO recording_campaign_v2_revisions(campaign_id,account_id,revision_number,predecessor_revision_id,decision_uuid,event_type,from_state,to_state,policy_version_id,actor_user_id,actor_role,request_sha256,plan_sha256,idempotency_key,reason_codes) VALUES($1,$2,2,$3,$4,'enter_planning','draft','planning',$5,$6,'owner',$7,$8,'planning',$9) RETURNING id,decided_at`, c.id, c.account, c.revisionID, uuid.New(), c.policyID, c.actor, requestDigest, planDigest, reasons).Scan(&c.revisionID, &decidedAt); err != nil {
		t.Fatal(err)
	}
	c.revisionNo = 2
	c.slots = make([]int64, len(subjects))
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
				fmt.Sprintf("test:%d", ordinal), fmt.Sprint(subject.sceneEvidenceID), subject.sceneSHA, subject.sceneEvidenceSHA,
				fmt.Sprint(subject.frameID), fmt.Sprint(subject.mediaID), subject.frameSHA,
				fmt.Sprint(subject.frameCapturedAt.UnixMicro()), fmt.Sprint(subject.sceneVerifiedAt.UnixMicro()), fmt.Sprint(decidedAt.UnixMicro()))
			if err = tx.QueryRow(ctx, `INSERT INTO recording_campaign_v2_prospective_evidence(campaign_id,account_id,created_revision_id,policy_version_id,stream_id,official_page_sha256,source_sha256,provider,failure_domain,scene_frame_evidence_id,scene_identity_sha256,scene_evidence_sha256,frame_id,media_object_id,frame_sha256,frame_captured_at,scene_verified_at,accepted_at_decision,evidence_sha256) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19) RETURNING id`, c.id, c.account, c.revisionID, c.policyID, subject.streamID, rosterV2SHA(subject.pageURL), rosterV2SHA(subject.sourceURL), subject.provider, fmt.Sprintf("test:%d", ordinal), subject.sceneEvidenceID, subject.sceneSHA, subject.sceneEvidenceSHA, subject.frameID, subject.mediaID, subject.frameSHA, subject.frameCapturedAt, subject.sceneVerifiedAt, decidedAt, evidenceSHA).Scan(&evidenceID); err != nil {
				t.Fatal(err)
			}
		}
		if err = tx.QueryRow(ctx, `INSERT INTO recording_campaign_v2_slots(campaign_id,account_id,ordinal,created_revision_id) VALUES($1,$2,$3,$4) RETURNING id`, c.id, c.account, ordinal, c.revisionID).Scan(&c.slots[i]); err != nil {
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
	recordingSHA := rosterV2SHA(subject.sourceURL)
	streamSHA := rosterV2SHA(subject.sourceURL)
	pageSHA := rosterV2SHA(subject.pageURL)
	snapshot := rosterV2Digest("campaign-roster-v2-r1-source",
		fmt.Sprint(c.account), fmt.Sprint(subject.recordingID), fmt.Sprint(subject.streamID), "0",
		recordingSHA, streamSHA, pageSHA, subject.provider, subject.externalID,
		subject.sourceFamily, subject.captureType, subject.executionClass)
	c.items = map[int]string{1: strings.Join([]string{
		"1", "1", "reserve", "1", "protect", fmt.Sprint(subject.recordingID), fmt.Sprint(subject.streamID),
		subject.sceneSHA, snapshot, subject.sceneEvidenceSHA, "0", "",
	}, "|")}
	rosterDigest := rosterV2RosterDigest(c.items)
	reasons := "member_admitted,planning_manifest_frozen"
	planDigest := rosterV2Digest("campaign-roster-v2-r1-plan", fmt.Sprint(c.id), "2", "enter_planning", "draft", "planning", c.policyDigest, reasons, rosterDigest)
	requestDigest := rosterV2Digest("campaign-roster-v2-r1-request", fmt.Sprint(c.id), fmt.Sprint(c.account), "2", "enter_planning", "draft", "planning", c.policyDigest, "planning", reasons, planDigest)
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
	var evidenceID, slotID, headID int64
	if err := tx.QueryRow(ctx, `INSERT INTO recording_campaign_v2_filled_evidence(campaign_id,account_id,created_revision_id,policy_version_id,recording_id,stream_id,source_revision_id,recording_source_sha256,stream_source_sha256,source_page_sha256,provider,external_id,source_family,capture_type,execution_class,source_snapshot_sha256,scene_frame_evidence_id,scene_identity_sha256,scene_evidence_sha256,frame_id,media_object_id,frame_sha256,frame_captured_at,scene_verified_at,accepted_at_decision,evidence_sha256) VALUES($1,$2,$3,$4,$5,$6,NULL,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25) RETURNING id`, c.id, c.account, c.revisionID, c.policyID, subject.recordingID, subject.streamID, recordingSHA, streamSHA, pageSHA, subject.provider, subject.externalID, subject.sourceFamily, subject.captureType, subject.executionClass, snapshot, subject.sceneEvidenceID, subject.sceneSHA, subject.sceneEvidenceSHA, subject.frameID, subject.mediaID, subject.frameSHA, subject.frameCapturedAt, subject.sceneVerifiedAt, decidedAt, evidenceSHA).Scan(&evidenceID); err != nil {
		return c, err
	}
	if err := tx.QueryRow(ctx, `INSERT INTO recording_campaign_v2_slots(campaign_id,account_id,ordinal,created_revision_id) VALUES($1,$2,1,$3) RETURNING id`, c.id, c.account, c.revisionID).Scan(&slotID); err != nil {
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
	planDigest := rosterV2Digest("campaign-roster-v2-r1-plan", fmt.Sprint(c.id), fmt.Sprint(revisionNo), "activate", c.state, "active", c.policyDigest, "activation_gates_met", rosterDigest)
	requestDigest := rosterV2Digest("campaign-roster-v2-r1-request", fmt.Sprint(c.id), fmt.Sprint(c.account), fmt.Sprint(revisionNo), "activate", c.state, "active", c.policyDigest, key, "activation_gates_met", planDigest)
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
			fmt.Sprint(ordinal), "2", "repair", fmt.Sprint(ordinal), "protect",
			fmt.Sprint(subject.recordingID), fmt.Sprint(subject.streamID), subject.sceneSHA,
			snapshot, subject.sceneEvidenceSHA, "0", "",
		}, "|")
	}
	rosterDigest := rosterV2RosterDigest(c.items)
	planDigest := rosterV2Digest("campaign-roster-v2-r1-plan", fmt.Sprint(c.id), fmt.Sprint(revisionNo), "roster_change", "planning", "planning", c.policyDigest, "member_admitted", rosterDigest)
	requestDigest := rosterV2Digest("campaign-roster-v2-r1-request", fmt.Sprint(c.id), fmt.Sprint(c.account), fmt.Sprint(revisionNo), "roster_change", "planning", "planning", c.policyDigest, key, "member_admitted", planDigest)
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

func TestRecordingCampaignV2R1DirectSQLBypassesFailClosed(t *testing.T) {
	f := openRosterV2PG(t)
	ctx := context.Background()
	account, actor := seedRosterV2Actor(t, f.pool, "bypass")
	subject := seedRosterV2Subjects(t, f.pool, account, actor, "bypass", 1, true)[0]
	campaign := createRosterV2Draft(t, f.pool, account, actor, "bypass", "reserve", 1, "operational-disjoint-v1")
	campaign = enterRosterV2Planning(t, f.pool, campaign, []rosterV2Subject{subject}, 1)

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
	if _, err = f.pool.Exec(ctx, `INSERT INTO recording_campaign_v2_idempotency(account_id,campaign_id,idempotency_key,request_sha256,plan_sha256,response_sha256,response_bytes,revision_id) SELECT account_id,campaign_id,idempotency_key,repeat('f',64),plan_sha256,response_sha256,response_bytes,revision_id FROM recording_campaign_v2_idempotency WHERE revision_id=$1`, campaign.revisionID); err == nil {
		t.Fatal("changed idempotency replay succeeded")
	}
}
