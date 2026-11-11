package main

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	ctx := context.Background()
	p, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		panic(err)
	}
	defer p.Close()
	var live, fresh, stale int
	err = p.QueryRow(ctx, `SELECT count(*),count(*) FILTER (WHERE latest.created_at>=now()-interval '15 minutes' AND latest.clip_end_at>=now()-interval '15 minutes'),count(*) FILTER (WHERE latest.created_at<now()-interval '15 minutes' OR latest.clip_end_at<now()-interval '15 minutes' OR latest.clip_end_at IS NULL) FROM recordings r JOIN LATERAL (SELECT j.id FROM recording_jobs j WHERE j.recording_id=r.id AND j.kind='continuous_window' AND j.fire_at<=now() AND j.window_end_at>now() ORDER BY j.id DESC LIMIT 1) j ON true LEFT JOIN LATERAL (SELECT max(created_at) created_at,max(clip_end_at) clip_end_at FROM recording_clips c WHERE c.recording_job_id=j.id) latest ON true WHERE r.status='active' AND r.mode='continuous'`).Scan(&live, &fresh, &stale)
	if err != nil {
		panic(err)
	}
	fmt.Printf("live_windows=%d fresh_media=%d stale_or_missing=%d\n", live, fresh, stale)
	rows, err := p.Query(ctx, `SELECT recording_id,signal,to_char(last_detected_at AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI:SSZ') FROM recorder_health_alerts WHERE resolved_at IS NULL AND signal IN ('continuous_silent_death','continuous_window_ended_early','job_retries_exhausted','stuck_lease','clip_timestamp_drift') ORDER BY recording_id,signal`)
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		var id int64
		var signal, detected string
		if err := rows.Scan(&id, &signal, &detected); err != nil {
			panic(err)
		}
		fmt.Printf("unresolved_live recording=%d signal=%s detected=%s\n", id, signal, detected)
		n++
	}
	if err := rows.Err(); err != nil {
		panic(err)
	}
	if n == 0 {
		fmt.Println("unresolved_live=0")
	}
	rows, err = p.Query(ctx, `SELECT r.id,r.name,j.id,j.status,to_char(j.window_end_at AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI:SSZ'),coalesce(to_char(latest.created_at AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI:SSZ'),''),coalesce(to_char(latest.clip_end_at AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI:SSZ'),'') FROM recordings r JOIN LATERAL (SELECT j.id,j.status,j.window_end_at FROM recording_jobs j WHERE j.recording_id=r.id AND j.kind='continuous_window' AND j.fire_at<=now() AND j.window_end_at>now() ORDER BY j.id DESC LIMIT 1) j ON true LEFT JOIN LATERAL (SELECT max(created_at) created_at,max(clip_end_at) clip_end_at FROM recording_clips c WHERE c.recording_job_id=j.id) latest ON true WHERE r.id IN (385,402,405,433) ORDER BY r.id`)
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	for rows.Next() {
		var id, job int64
		var name, status, end, created, clipEnd string
		if err := rows.Scan(&id, &name, &job, &status, &end, &created, &clipEnd); err != nil {
			panic(err)
		}
		fmt.Printf("alert_recording id=%d name=%s job=%d status=%s window_end=%s latest_created=%s latest_clip_end=%s\n", id, name, job, status, end, created, clipEnd)
	}
}
