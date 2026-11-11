package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		panic(err)
	}
	defer pool.Close()

	queries := []struct {
		name string
		sql  string
	}{
		{"summary", `SELECT jsonb_build_object(
			'now',now(),
			'live_droplets',(SELECT count(*) FROM recorder_droplets WHERE state IN ('provisioning','active','draining','destroying')),
			'active_capacity',(SELECT coalesce(sum(capacity),0) FROM recorder_droplets WHERE state='active' AND last_seen_at>now()-interval '60 seconds'),
			'live_cloud_leases',(SELECT count(*) FROM recording_jobs j JOIN recordings r ON r.id=j.recording_id WHERE j.status='leased' AND j.lease_expires_at>now() AND r.capture_via='cloud'),
			'pending_cloud_jobs',(SELECT count(*) FROM recording_jobs j JOIN recordings r ON r.id=j.recording_id WHERE j.status='pending' AND r.capture_via='cloud'),
			'pending_live_upload_intents',(SELECT count(*) FROM recording_upload_intents WHERE status='pending' AND expires_at>now()),
			'next_cloud_fire',(SELECT min(j.scheduled_for) FROM recording_jobs j JOIN recordings r ON r.id=j.recording_id WHERE j.status='pending' AND r.capture_via='cloud')
		)`},
		{"droplets", `SELECT coalesce(jsonb_agg(x ORDER BY x.created_at,x.id),'[]'::jsonb) FROM (
			SELECT d.id,d.name,d.do_droplet_id,d.state,d.capacity,d.build_sha,d.created_at,d.last_seen_at,d.idle_since,d.drain_started_at,
				(SELECT count(*) FROM recording_jobs j WHERE j.lease_owner=d.name AND j.status='leased' AND j.lease_expires_at>now()) live_leases,
				(SELECT coalesce(jsonb_agg(q.recording_id ORDER BY q.recording_id),'[]'::jsonb) FROM (SELECT DISTINCT j.recording_id FROM recording_jobs j WHERE j.lease_owner=d.name AND j.status='leased' AND j.lease_expires_at>now()) q) recording_ids,
				(SELECT max(c.created_at) FROM recording_clips c JOIN recording_jobs j ON j.id=c.recording_job_id WHERE j.lease_owner=d.name) last_clip_ingested_at,
				(SELECT count(*) FROM recording_clips c JOIN recording_jobs j ON j.id=c.recording_job_id WHERE j.lease_owner=d.name AND c.created_at>now()-interval '10 minutes') clips_ingested_10m,
				(SELECT count(*) FROM recording_upload_intents u JOIN recording_jobs j ON j.id=u.recording_job_id WHERE j.lease_owner=d.name AND u.status='pending' AND u.expires_at>now()) pending_intents
			FROM recorder_droplets d WHERE d.state IN ('provisioning','active','draining','destroying')
		) x`},
		{"leases", `SELECT coalesce(jsonb_agg(x ORDER BY x.lease_owner,x.recording_id),'[]'::jsonb) FROM (
			SELECT j.id job_id,j.recording_id,j.lease_owner,j.fire_at,j.window_end_at,j.lease_expires_at,j.attempt_count,
				(SELECT count(*) FROM recording_clips c WHERE c.recording_job_id=j.id) clip_count,
				(SELECT max(c.created_at) FROM recording_clips c WHERE c.recording_job_id=j.id) last_clip_ingested_at,
				(SELECT max(c.clip_end_at) FROM recording_clips c WHERE c.recording_job_id=j.id) latest_media_end_at,
				(SELECT count(*) FROM recording_upload_intents u WHERE u.recording_job_id=j.id AND u.status='pending' AND u.expires_at>now()) pending_intents
			FROM recording_jobs j JOIN recordings r ON r.id=j.recording_id
			WHERE j.status='leased' AND j.lease_expires_at>now() AND r.capture_via='cloud'
		) x`},
		{"recent_outcomes", `SELECT coalesce(jsonb_agg(x ORDER BY x.worker,x.status),'[]'::jsonb) FROM (
			SELECT coalesce(j.lease_owner,'') worker,j.status,count(*) jobs,min(j.completed_at) first_completed,max(j.completed_at) last_completed,
				count(*) FILTER (WHERE j.status='error') errors
			FROM recording_jobs j JOIN recordings r ON r.id=j.recording_id
			WHERE r.capture_via='cloud' AND j.updated_at>now()-interval '90 minutes'
			GROUP BY coalesce(j.lease_owner,''),j.status
		) x`},
		{"late_jobs", `SELECT coalesce(jsonb_agg(x ORDER BY x.recording_id),'[]'::jsonb) FROM (
			SELECT r.id recording_id,r.status,r.end_at,r.next_fire_at,r.updated_at recording_updated_at,
				j.id job_id,j.status job_status,j.fire_at,j.window_end_at,j.created_at job_created_at,j.updated_at job_updated_at,
				j.lease_owner,j.attempt_count,
				(SELECT count(*) FROM recording_clips c WHERE c.recording_job_id=j.id) clip_count,
				(SELECT min(c.clip_start_at) FROM recording_clips c WHERE c.recording_job_id=j.id) first_media_at,
				(SELECT max(c.clip_end_at) FROM recording_clips c WHERE c.recording_job_id=j.id) last_media_at
			FROM recordings r JOIN recording_jobs j ON j.recording_id=r.id
			WHERE r.id IN (405,407) AND j.fire_at::date='2026-08-25'::date
		) x`},
		{"cohort_settlement", `SELECT coalesce(jsonb_agg(x ORDER BY x.recording_id),'[]'::jsonb) FROM (
			SELECT r.id recording_id,r.status recording_status,r.end_at,r.next_fire_at,
				j.id job_id,j.status job_status,j.fire_at,j.window_end_at,j.completed_at,j.lease_owner,j.lease_expires_at,j.handoff_until,j.attempt_count,j.error_text,
				(SELECT count(*) FROM recording_clips c WHERE c.recording_job_id=j.id) clip_count,
				(SELECT min(c.clip_start_at) FROM recording_clips c WHERE c.recording_job_id=j.id) first_media_at,
				(SELECT max(c.clip_end_at) FROM recording_clips c WHERE c.recording_job_id=j.id) last_media_at,
				(SELECT count(*) FROM recording_upload_intents u WHERE u.recording_job_id=j.id AND u.status IN ('pending','uploading')) live_intents,
				h.calculated_at,h.metric_version,h.coverage_pct,h.largest_gap_seconds,h.gap_over_30s_count,h.gap_over_5m_count,h.overlap_count,h.clip_count health_clip_count,
				CASE WHEN h.metric_version IS NULL OR h.metric_version<>2 OR h.gap_over_30s_count IS NULL OR h.gap_over_5m_count IS NULL THEN 'MISSING'
					WHEN h.overlap_count=0 AND h.coverage_pct>=99 AND h.largest_gap_seconds<=120 THEN 'A'
					WHEN h.overlap_count=0 AND h.coverage_pct>=95 AND h.largest_gap_seconds<=900 AND h.gap_over_5m_count<=1 AND h.gap_over_30s_count<=6 THEN 'B'
					WHEN h.overlap_count=0 AND h.coverage_pct>=90 AND h.largest_gap_seconds<=1800 AND h.gap_over_5m_count<=2 THEN 'C'
					WHEN h.clip_count=0 OR h.coverage_pct=0 THEN 'F' WHEN h.coverage_pct<80 THEN 'E' ELSE 'D' END grade
			FROM recordings r
			JOIN LATERAL (SELECT * FROM recording_jobs q WHERE q.recording_id=r.id AND q.fire_at::date=date '2026-08-25' ORDER BY q.fire_at DESC,q.id DESC LIMIT 1) j ON true
			LEFT JOIN recording_window_health h ON h.recording_id=r.id AND h.job_id=j.id
			WHERE r.id IN (339,408,417,418,419,420,428,431)
		) x`},
		{"cohort_gates", `SELECT jsonb_build_object(
			'pending_jobs',(SELECT count(*) FROM recording_jobs WHERE recording_id IN (339,408,417,418,419,420,428,431) AND status='pending'),
			'live_leases',(SELECT count(*) FROM recording_jobs WHERE recording_id IN (339,408,417,418,419,420,428,431) AND status='leased' AND lease_expires_at>now()),
			'live_handoffs',(SELECT count(*) FROM recording_jobs WHERE recording_id IN (339,408,417,418,419,420,428,431) AND handoff_until>now()),
			'live_intents',(SELECT count(*) FROM recording_upload_intents WHERE recording_id IN (339,408,417,418,419,420,428,431) AND status IN ('pending','uploading'))
		)`},
		{"cohort_open_intents", `SELECT coalesce(jsonb_agg(jsonb_build_object(
			'id',id,'recording_id',recording_id,'recording_job_id',recording_job_id,'status',status,'expires_at',expires_at
		) ORDER BY recording_id,id),'[]'::jsonb) FROM recording_upload_intents
		WHERE recording_id IN (339,408,417,418,419,420,428,431) AND status IN ('pending','uploading')`},
		{"future_gates", `SELECT coalesce(jsonb_agg(jsonb_build_object(
			'id',r.id,'status',r.status,'capture_via',r.capture_via,'end_at',r.end_at,'next_fire_at',r.next_fire_at,
			'pending',(SELECT count(*) FROM recording_jobs j WHERE j.recording_id=r.id AND j.status='pending'),
			'leased',(SELECT count(*) FROM recording_jobs j WHERE j.recording_id=r.id AND j.status='leased' AND j.lease_expires_at>now()),
			'handoff',(SELECT count(*) FROM recording_jobs j WHERE j.recording_id=r.id AND j.handoff_until>now()),
			'live_intents',(SELECT count(*) FROM recording_upload_intents u WHERE u.recording_id=r.id AND u.status='pending' AND u.expires_at>now())
		) ORDER BY r.id),'[]'::jsonb) FROM recordings r WHERE r.id IN (419,420,445)`},
		{"materializer_history", `SELECT jsonb_build_object(
			'global_last_calculated_at',(SELECT max(calculated_at) FROM recording_window_health),
			'cohort_today_v2_count',(SELECT count(*) FROM recording_window_health h JOIN recording_jobs j ON j.id=h.job_id WHERE h.recording_id IN (339,408,417,418,419,420,428,431) AND j.fire_at::date=date '2026-08-25' AND h.metric_version=2),
			'recent_batches',(SELECT coalesce(jsonb_agg(jsonb_build_object('calculated_at',x.calculated_at,'rows',x.rows) ORDER BY x.calculated_at DESC),'[]'::jsonb) FROM (
				SELECT calculated_at,count(*) rows FROM recording_window_health WHERE calculated_at>now()-interval '3 hours' GROUP BY calculated_at ORDER BY calculated_at DESC LIMIT 8
			) x)
		)`},
		{"rolling_loop_audit", `SELECT jsonb_build_object(
			'events',(SELECT coalesce(jsonb_agg(jsonb_build_object(
				'created_at',e.created_at,'api_key_id',e.api_key_id,'recording_id',(e.detail_jsonb->>'recording_id')::bigint
			) ORDER BY e.created_at),'[]'::jsonb) FROM account_auth_events e
			WHERE e.account_id=47 AND e.event_type='recording_schedule_changed'
			AND e.created_at>='2026-08-25T15:55:00Z'::timestamptz
			AND (e.detail_jsonb->>'recording_id')::bigint IN (406,408,409,410,411,412,414,417,432,433,434,435,436,442,445)),
			'catalog',(SELECT coalesce(jsonb_agg(jsonb_build_object(
				'id',r.id,'status',r.status,'end_at',r.end_at,'updated_at',r.updated_at,
				'rolling_target',(date '2026-09-08'::timestamp AT TIME ZONE r.cron_timezone),
				'at_rolling_target',r.end_at=(date '2026-09-08'::timestamp AT TIME ZONE r.cron_timezone)
			) ORDER BY r.id),'[]'::jsonb) FROM recordings r
			WHERE r.id IN (406,408,409,410,411,412,414,417,432,433,434,435,436,442,445))
		)`},
	}

	for _, q := range queries {
		var raw []byte
		if err := pool.QueryRow(ctx, q.sql).Scan(&raw); err != nil {
			panic(fmt.Errorf("%s: %w", q.name, err))
		}
		var value any
		if err := json.Unmarshal(raw, &value); err != nil {
			panic(err)
		}
		out, err := json.Marshal(value)
		if err != nil {
			panic(err)
		}
		fmt.Printf("%s=%s\n", q.name, out)
	}
}
