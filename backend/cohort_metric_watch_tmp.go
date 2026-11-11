package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		panic(err)
	}
	defer pool.Close()
	deadline := time.Date(2026, 8, 25, 18, 42, 0, 0, time.UTC)
	poll := 0
	for {
		var raw []byte
		err = pool.QueryRow(ctx, `WITH snapshot AS (SELECT coalesce(jsonb_agg(jsonb_build_object(
				'recording_id',r.id,'job_id',j.id,'job_status',j.status,
				'pending',(SELECT count(*) FROM recording_jobs q WHERE q.recording_id=r.id AND q.status='pending'),
				'leased',(SELECT count(*) FROM recording_jobs q WHERE q.recording_id=r.id AND q.status='leased' AND q.lease_expires_at>now()),
				'handoff',(SELECT count(*) FROM recording_jobs q WHERE q.recording_id=r.id AND q.handoff_until>now()),
				'live_intents',(SELECT count(*) FROM recording_upload_intents u WHERE u.recording_id=r.id AND u.status='pending' AND u.expires_at>now()),
				'metric_version',h.metric_version,'calculated_at',h.calculated_at,
				'coverage_pct',h.coverage_pct,'largest_gap_seconds',h.largest_gap_seconds,
				'grade',CASE WHEN h.metric_version IS NULL OR h.metric_version<>2 OR h.gap_over_30s_count IS NULL OR h.gap_over_5m_count IS NULL THEN 'MISSING'
					WHEN h.overlap_count=0 AND h.coverage_pct>=99 AND h.largest_gap_seconds<=120 THEN 'A'
					WHEN h.overlap_count=0 AND h.coverage_pct>=95 AND h.largest_gap_seconds<=900 AND h.gap_over_5m_count<=1 AND h.gap_over_30s_count<=6 THEN 'B'
					WHEN h.overlap_count=0 AND h.coverage_pct>=90 AND h.largest_gap_seconds<=1800 AND h.gap_over_5m_count<=2 THEN 'C'
					WHEN h.clip_count=0 OR h.coverage_pct=0 THEN 'F' WHEN h.coverage_pct<80 THEN 'E' ELSE 'D' END
			) ORDER BY r.id),'[]'::jsonb) rows
		FROM recordings r
		JOIN LATERAL (SELECT * FROM recording_jobs q WHERE q.recording_id=r.id AND q.fire_at::date=date '2026-08-25' ORDER BY q.fire_at DESC,q.id DESC LIMIT 1) j ON true
		LEFT JOIN recording_window_health h ON h.recording_id=r.id AND h.job_id=j.id
			WHERE r.id IN (339,408,417,418,419,420,428,431))
			SELECT jsonb_build_object('now',now(),'rows',rows) FROM snapshot`).Scan(&raw)
		if err != nil {
			panic(err)
		}
		var v any
		if err = json.Unmarshal(raw, &v); err != nil {
			panic(err)
		}
		out, _ := json.Marshal(v)
		var ready int
		if err = pool.QueryRow(ctx, `SELECT count(*) FROM recording_window_health h
			JOIN recording_jobs j ON j.id=h.job_id
			WHERE h.recording_id IN (339,408,417,418,419,420,428,431)
			AND j.fire_at::date=date '2026-08-25' AND h.metric_version=2`).Scan(&ready); err != nil {
			panic(err)
		}
		poll++
		if poll == 1 || poll%10 == 0 || ready == 8 || time.Now().After(deadline) {
			fmt.Println(string(out))
		}
		if ready == 8 || time.Now().After(deadline) {
			return
		}
		time.Sleep(30 * time.Second)
	}
}
