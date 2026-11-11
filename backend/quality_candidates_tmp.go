package main

import (
	"context"
	"fmt"
	"os"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	ctx := context.Background()
	db, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL")); if err != nil { panic(err) }; defer db.Close()
	rows, err := db.Query(ctx, `
WITH graded AS (
 SELECT h.recording_id,h.job_id,(h.window_start_at AT TIME ZONE r.cron_timezone)::date d,
 CASE WHEN h.metric_version IS NULL OR h.metric_version<>2 OR h.gap_over_30s_count IS NULL OR h.gap_over_5m_count IS NULL THEN 'U'
      WHEN h.overlap_count=0 AND h.coverage_pct>=99 AND h.largest_gap_seconds<=120 THEN 'A'
      WHEN h.overlap_count=0 AND h.coverage_pct>=95 AND h.largest_gap_seconds<=900 AND h.gap_over_5m_count<=1 AND h.gap_over_30s_count<=6 THEN 'B'
      WHEN h.overlap_count=0 AND h.coverage_pct>=90 AND h.largest_gap_seconds<=1800 AND h.gap_over_5m_count<=2 THEN 'C'
      WHEN h.clip_count=0 OR h.coverage_pct=0 THEN 'F' WHEN h.coverage_pct<80 THEN 'E' ELSE 'D' END g
 FROM recording_window_health h JOIN recordings r ON r.id=h.recording_id
 WHERE r.account_id=47 AND r.mode='continuous'
), w AS (
 SELECT *,lead(d,13) OVER(PARTITION BY recording_id ORDER BY d,job_id) ed,
 count(*) OVER(PARTITION BY recording_id ORDER BY d,job_id ROWS BETWEEN CURRENT ROW AND 13 FOLLOWING) n,
 count(*) FILTER(WHERE g IN('F','U')) OVER(PARTITION BY recording_id ORDER BY d,job_id ROWS BETWEEN CURRENT ROW AND 13 FOLLOWING) bad,
 count(*) FILTER(WHERE g='E') OVER(PARTITION BY recording_id ORDER BY d,job_id ROWS BETWEEN CURRENT ROW AND 13 FOLLOWING) es
 FROM graded
), q AS (
 SELECT recording_id,
  bool_or(n=14 AND ed=d+13) fine,
  bool_or(n=14 AND ed=d+13 AND bad=0 AND es<=2) good,
  bool_or(n=14 AND ed=d+13 AND bad=0 AND es=0 AND NOT EXISTS(SELECT 1 FROM w x WHERE x.recording_id=w.recording_id AND x.d BETWEEN w.d AND w.d+13 AND x.g NOT IN('A','B','C'))) great
 FROM w GROUP BY recording_id
), active AS (
 SELECT r.id,r.end_at,
  COALESCE((SELECT count(*) FROM generate_series((now() AT TIME ZONE r.cron_timezone)::date,date '2026-09-09',interval '1 day') d
    WHERE (r.active_weekdays&(1<<(extract(isodow FROM d)::int-1)))<>0
      AND ((d::date+r.daily_window_end) AT TIME ZONE r.cron_timezone)>now()
      AND ((d::date+r.daily_window_end) AT TIME ZONE r.cron_timezone)<=r.end_at),0) future_windows
 FROM recordings r WHERE r.account_id=47 AND r.mode='continuous' AND r.status='active'
)
SELECT a.id,coalesce(q.fine,false),coalesce(q.good,false),coalesce(q.great,false),a.future_windows,a.end_at
FROM active a LEFT JOIN q ON q.recording_id=a.id ORDER BY a.id`)
	if err != nil { panic(err) }; defer rows.Close()
	for rows.Next() { var id,fw int64; var fine,good,great bool; var end any; if err:=rows.Scan(&id,&fine,&good,&great,&fw,&end);err!=nil{panic(err)}; fmt.Printf("%d\t fine_completed=%t\tgood_completed=%t\tgreat_completed=%t\tfuture_windows=%d\tend=%v\n",id,fine,good,great,fw,end) }
	if err:=rows.Err();err!=nil{panic(err)}
}
