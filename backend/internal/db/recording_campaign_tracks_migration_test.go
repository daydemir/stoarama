package db_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestRecordingCampaignTracksAuditAndProtection(t *testing.T) {
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		t.Skip("DATABASE_URL is required")
	}
	ctx := context.Background()
	c, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)
	schema := fmt.Sprintf("campaign_%d", time.Now().UnixNano())
	if _, err = c.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	defer c.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE")
	if _, err = c.Exec(ctx, "SET search_path TO "+schema); err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{
		`CREATE TABLE accounts(id bigint primary key)`, `CREATE TABLE users(id bigint primary key,is_operator boolean not null default false)`, `CREATE TABLE memberships(user_id bigint,org_id bigint,role text,accepted_at timestamptz)`, `CREATE TABLE streams(id bigint primary key)`,
		`CREATE TABLE recordings(id bigint primary key,account_id bigint,stream_id bigint)`, `CREATE TABLE recording_jobs(id bigint primary key)`,
	} {
		if _, err = c.Exec(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	m, err := os.ReadFile(filepath.Join("..", "..", "..", "infra", "sql", "migrations", "0129_recording_campaign_tracks.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.Exec(ctx, string(m)); err != nil {
		t.Fatal(err)
	}
	if _, err = c.Exec(ctx, `INSERT INTO accounts VALUES(1); INSERT INTO users VALUES(2,true); INSERT INTO streams VALUES(3); INSERT INTO recordings VALUES(4,1,3)`); err != nil {
		t.Fatal(err)
	}
	var track int64
	if err = c.QueryRow(ctx, `INSERT INTO recording_campaign_tracks(account_id,campaign_key,label,deadline_at,target_count,grade_floor,created_by_user_id) VALUES(1,'delivery30','Delivery 30',now()+interval '7 days',1,'GOOD',2) RETURNING id`).Scan(&track); err != nil {
		t.Fatal(err)
	}
	if _, err = c.Exec(ctx, `INSERT INTO recording_campaign_roster_entries(track_id,recording_id,stream_id,scene_identity_sha256,role,rank,status,reason_codes,effective_at,decision_at,evidence_observed_at,evidence_sha256,updated_by_user_id) VALUES($1,4,3,repeat('a',64),'primary',1,'protect',ARRAY['current_stable'],now(),now(),now(),repeat('b',64),2)`, track); err != nil {
		t.Fatal(err)
	}
	if _, err = c.Exec(ctx, `SELECT transition_recording_campaign_track($1,'active',ARRAY['approved_roster'],2,now())`, track); err != nil {
		t.Fatal(err)
	}
	var protected, events int
	if err = c.QueryRow(ctx, `SELECT count(*) FROM protected_campaign_recordings WHERE account_id=1 AND recording_id=4`).Scan(&protected); err != nil || protected != 1 {
		t.Fatalf("protected=%d err=%v", protected, err)
	}
	if _, err = c.Exec(ctx, `UPDATE recording_campaign_roster_entries SET status='removed',decision_at=now(),updated_by_user_id=2 WHERE track_id=$1 AND recording_id=4`, track); err == nil {
		t.Fatal("active target drift succeeded")
	}
	if _, err = c.Exec(ctx, `UPDATE recording_campaign_tracks SET target_count=2 WHERE id=$1`, track); err == nil {
		t.Fatal("active definition mutation succeeded")
	}
	if _, err = c.Exec(ctx, `SELECT transition_recording_campaign_track($1,'complete',ARRAY['deadline_complete'],2,now())`, track); err != nil {
		t.Fatal(err)
	}
	if err = c.QueryRow(ctx, `SELECT count(*) FROM protected_campaign_recordings WHERE account_id=1 AND recording_id=4`).Scan(&protected); err != nil || protected != 1 {
		t.Fatalf("complete protection=%d err=%v", protected, err)
	}
	if _, err = c.Exec(ctx, `SELECT transition_recording_campaign_track($1,'retired',ARRAY['approved_retire'],2,now())`, track); err != nil {
		t.Fatal(err)
	}
	if _, err = c.Exec(ctx, `UPDATE recording_campaign_roster_entries SET status='removed',decision_at=now(),updated_by_user_id=2 WHERE track_id=$1 AND recording_id=4`, track); err != nil {
		t.Fatal(err)
	}
	if err = c.QueryRow(ctx, `SELECT count(*) FROM recording_campaign_roster_events WHERE track_id=$1`, track).Scan(&events); err != nil || events != 2 {
		t.Fatalf("events=%d err=%v", events, err)
	}
	if _, err = c.Exec(ctx, `DELETE FROM recording_campaign_roster_events WHERE track_id=$1`, track); err == nil {
		t.Fatal("append-only event delete succeeded")
	}
	if _, err = c.Exec(ctx, `UPDATE recording_campaign_tracks SET state='active' WHERE id=$1`, track); err == nil {
		t.Fatal("direct lifecycle update succeeded")
	}
}
