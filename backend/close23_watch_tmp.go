package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func emit(v any) {
	raw, _ := json.Marshal(v)
	fmt.Println(string(raw))
}

func snapshot(ctx context.Context, db *pgxpool.Pool, event string) (int, error) {
	var raw []byte
	err := db.QueryRow(ctx, `WITH snapshot AS (SELECT coalesce(jsonb_agg(jsonb_build_object(
		'recording_id',r.id,'event',$1::text,'recording_status',r.status,'end_at',r.end_at,'next_fire_at',r.next_fire_at,
		'job_id',j.id,'job_status',j.status,'attempt_count',j.attempt_count,'completed_at',j.completed_at,
		'lease_expires_at',j.lease_expires_at,'handoff_until',j.handoff_until,
		'clips',(SELECT count(*) FROM recording_clips c WHERE c.recording_job_id=j.id),
		'latest_media_at',(SELECT max(c.clip_end_at) FROM recording_clips c WHERE c.recording_job_id=j.id),
		'latest_ingest_at',(SELECT max(c.created_at) FROM recording_clips c WHERE c.recording_job_id=j.id),
		'pending',(SELECT count(*) FROM recording_jobs q WHERE q.recording_id=r.id AND q.status='pending'),
		'leased',(SELECT count(*) FROM recording_jobs q WHERE q.recording_id=r.id AND q.status='leased' AND q.lease_expires_at>now()),
		'handoff',(SELECT count(*) FROM recording_jobs q WHERE q.recording_id=r.id AND q.handoff_until>now()),
		'live_intents',(SELECT count(*) FROM recording_upload_intents u WHERE u.recording_id=r.id AND u.status='pending' AND u.expires_at>now()),
		'metric_version',h.metric_version,'calculated_at',h.calculated_at,'coverage_pct',h.coverage_pct,'largest_gap_seconds',h.largest_gap_seconds,
		'grade',CASE WHEN h.metric_version IS NULL OR h.metric_version<>2 OR h.gap_over_30s_count IS NULL OR h.gap_over_5m_count IS NULL THEN 'MISSING'
			WHEN h.overlap_count=0 AND h.coverage_pct>=99 AND h.largest_gap_seconds<=120 THEN 'A'
			WHEN h.overlap_count=0 AND h.coverage_pct>=95 AND h.largest_gap_seconds<=900 AND h.gap_over_5m_count<=1 AND h.gap_over_30s_count<=6 THEN 'B'
			WHEN h.overlap_count=0 AND h.coverage_pct>=90 AND h.largest_gap_seconds<=1800 AND h.gap_over_5m_count<=2 THEN 'C'
			WHEN h.clip_count=0 OR h.coverage_pct=0 THEN 'F' WHEN h.coverage_pct<80 THEN 'E' ELSE 'D' END
	) ORDER BY r.id),'[]'::jsonb) rows
	FROM recordings r
	LEFT JOIN LATERAL (SELECT * FROM recording_jobs q WHERE q.recording_id=r.id AND q.fire_at='2026-08-25T11:00:00Z'::timestamptz ORDER BY q.id DESC LIMIT 1) j ON true
	LEFT JOIN recording_window_health h ON h.recording_id=r.id AND h.job_id=j.id
	WHERE r.id IN (405,406))
	SELECT jsonb_build_object('event',$1::text,'at',now(),'rows',rows) FROM snapshot`, event).Scan(&raw)
	if err != nil {
		return 0, err
	}
	var value any
	if err = json.Unmarshal(raw, &value); err != nil {
		return 0, err
	}
	emit(value)
	var ready int
	err = db.QueryRow(ctx, `SELECT count(*) FROM recording_window_health h JOIN recording_jobs j ON j.id=h.job_id
		WHERE h.recording_id IN (405,406) AND j.fire_at='2026-08-25T11:00:00Z'::timestamptz AND h.metric_version=2`).Scan(&ready)
	return ready, err
}

func waitUntil(t time.Time) {
	if d := time.Until(t); d > 0 {
		time.Sleep(d)
	}
}

func main() {
	ctx := context.Background()
	db, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		panic(err)
	}
	defer db.Close()
	emit(map[string]any{
		"event": "armed", "at": time.Now().UTC(), "recordings": []int64{405, 406},
		"preclose_55": "2026-08-25T22:05:00Z", "preclose_25": "2026-08-25T22:35:00Z",
		"close": "2026-08-25T23:00:00Z", "materializer_watch_start": "2026-08-25T23:35:00Z",
	})
	for _, event := range []struct {
		at    time.Time
		label string
	}{
		{time.Date(2026, 8, 25, 22, 5, 0, 0, time.UTC), "preclose_55"},
		{time.Date(2026, 8, 25, 22, 35, 0, 0, time.UTC), "preclose_25"},
	} {
		waitUntil(event.at)
		if _, err = snapshot(ctx, db, event.label); err != nil {
			panic(err)
		}
	}
	waitUntil(time.Date(2026, 8, 25, 23, 0, 15, 0, time.UTC))
	settleDeadline := time.Date(2026, 8, 25, 23, 8, 0, 0, time.UTC)
	for {
		if _, err = snapshot(ctx, db, "close_settlement"); err != nil {
			panic(err)
		}
		var settled int
		err = db.QueryRow(ctx, `SELECT count(*) FROM recording_jobs j WHERE j.recording_id IN (405,406)
			AND j.fire_at='2026-08-25T11:00:00Z'::timestamptz AND j.status='done'
			AND j.lease_expires_at IS NULL AND (j.handoff_until IS NULL OR j.handoff_until<=now())
			AND NOT EXISTS(SELECT 1 FROM recording_upload_intents u WHERE u.recording_job_id=j.id AND u.status='pending' AND u.expires_at>now())`).Scan(&settled)
		if err != nil {
			panic(err)
		}
		if settled == 2 || time.Now().After(settleDeadline) {
			break
		}
		time.Sleep(30 * time.Second)
	}
	waitUntil(time.Date(2026, 8, 25, 23, 35, 0, 0, time.UTC))
	metricDeadline := time.Date(2026, 8, 25, 23, 50, 0, 0, time.UTC)
	for {
		ready, err := snapshot(ctx, db, "post_materializer")
		if err != nil {
			panic(err)
		}
		if ready == 2 || time.Now().After(metricDeadline) {
			return
		}
		time.Sleep(30 * time.Second)
	}
}
