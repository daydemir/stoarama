package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type checkpoint struct {
	at    time.Time
	label string
}

func emit(v any) {
	raw, _ := json.Marshal(v)
	fmt.Println(string(raw))
}

func main() {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		panic(err)
	}
	defer pool.Close()

	ids := []int64{419, 420, 445}
	fires := map[int64]*time.Time{}
	rows, err := pool.Query(ctx, `SELECT id,next_fire_at FROM recordings WHERE id=any($1) ORDER BY id`, ids)
	if err != nil {
		panic(err)
	}
	for rows.Next() {
		var id int64
		var fire *time.Time
		if err = rows.Scan(&id, &fire); err != nil {
			panic(err)
		}
		fires[id] = fire
	}
	rows.Close()
	emit(map[string]any{"event": "armed", "at": time.Now().UTC(), "fires": fires})

	base := fires[419]
	if base == nil || fires[420] == nil || !base.Equal(*fires[420]) {
		emit(map[string]any{"event": "arm_failed", "reason": "419/420 next fire missing or unequal", "fires": fires})
		return
	}
	checks := []checkpoint{
		{base.Add(-2*time.Hour + 5*time.Minute), "t_minus_2h_observed"},
		{base.Add(-30*time.Minute + 5*time.Minute), "t_minus_30m_observed"},
		{base.Add(30 * time.Second), "opening"},
		{base.Add(3*time.Minute + 30*time.Second), "first3"},
	}
	sort.Slice(checks, func(i, j int) bool { return checks[i].at.Before(checks[j].at) })
	for _, check := range checks {
		if delay := time.Until(check.at); delay > 0 {
			time.Sleep(delay)
		}
		var raw []byte
		err = pool.QueryRow(ctx, `WITH snapshot AS (SELECT coalesce(jsonb_agg(jsonb_build_object(
				'recording_id',r.id,'status',r.status,'next_fire_at',r.next_fire_at,
				'early_result',early.result,'early_checked_at',early.checked_at,
				'confirm_result',confirm.result,'confirm_checked_at',confirm.checked_at,
				'job_id',j.id,'job_status',j.status,'attempt_count',j.attempt_count,
				'lease_expires_at',j.lease_expires_at,'handoff_until',j.handoff_until,
				'clip_count',(SELECT count(*) FROM recording_clips c WHERE c.recording_job_id=j.id),
				'latest_media_at',(SELECT max(c.clip_end_at) FROM recording_clips c WHERE c.recording_job_id=j.id),
				'latest_ingest_at',(SELECT max(c.created_at) FROM recording_clips c WHERE c.recording_job_id=j.id),
				'live_intents',(SELECT count(*) FROM recording_upload_intents u WHERE u.recording_id=r.id AND u.status='pending' AND u.expires_at>now())
			) ORDER BY r.id),'[]'::jsonb) rows
		FROM recordings r
		LEFT JOIN recording_preopen_checks early ON early.recording_id=r.id AND early.window_start_at=$1 AND early.stage='early'
		LEFT JOIN recording_preopen_checks confirm ON confirm.recording_id=r.id AND confirm.window_start_at=$1 AND confirm.stage='confirm'
		LEFT JOIN LATERAL (SELECT * FROM recording_jobs q WHERE q.recording_id=r.id AND q.fire_at=$1 ORDER BY q.id DESC LIMIT 1) j ON true
		WHERE r.id IN (419,420))
		SELECT jsonb_build_object('event',$2,'at',now(),'rows',rows) FROM snapshot`, *base, check.label).Scan(&raw)
		if err != nil {
			panic(err)
		}
		var value any
		if err = json.Unmarshal(raw, &value); err != nil {
			panic(err)
		}
		emit(value)
	}
}
