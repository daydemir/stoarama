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
 SELECT h.recording_id,h.job_id,(h.window_start_at AT TIME ZONE r.cron_timezone)::date local_day,
 CASE WHEN h.metric_version IS NULL OR h.metric_version<>2 OR h.gap_over_30s_count IS NULL OR h.gap_over_5m_count IS NULL THEN 'UNKNOWN'
      WHEN h.overlap_count=0 AND h.coverage_pct>=99 AND h.largest_gap_seconds<=120 THEN 'A'
      WHEN h.overlap_count=0 AND h.coverage_pct>=95 AND h.largest_gap_seconds<=900 AND h.gap_over_5m_count<=1 AND h.gap_over_30s_count<=6 THEN 'B'
      WHEN h.overlap_count=0 AND h.coverage_pct>=90 AND h.largest_gap_seconds<=1800 AND h.gap_over_5m_count<=2 THEN 'C'
      WHEN h.clip_count=0 OR h.coverage_pct=0 THEN 'F' WHEN h.coverage_pct<80 THEN 'E' ELSE 'D' END grade
 FROM recording_window_health h JOIN recordings r ON r.id=h.recording_id
 WHERE r.account_id=47 AND r.mode='continuous'
), windows AS (
 SELECT recording_id,local_day,
   lead(local_day,13) OVER (PARTITION BY recording_id ORDER BY local_day,job_id) end_day,
   count(*) OVER (PARTITION BY recording_id ORDER BY local_day,job_id ROWS BETWEEN CURRENT ROW AND 13 FOLLOWING) n,
   count(*) FILTER (WHERE grade NOT IN ('A','B','C')) OVER (PARTITION BY recording_id ORDER BY local_day,job_id ROWS BETWEEN CURRENT ROW AND 13 FOLLOWING) bad
 FROM graded
), qualified AS (
 SELECT recording_id,min(local_day) best_start FROM windows WHERE n=14 AND end_day=local_day+13 AND bad=0 GROUP BY recording_id
)
SELECT r.id,r.status,r.end_at,r.next_fire_at,q.best_start
FROM qualified q JOIN recordings r ON r.id=q.recording_id ORDER BY r.id`)
	if err != nil { panic(err) }
	defer rows.Close()
	for rows.Next() { var id int64; var status string; var end,next,best any; if err:=rows.Scan(&id,&status,&end,&next,&best);err!=nil{panic(err)}; fmt.Printf("GREAT\t%d\t%s\t%v\t%v\t%v\n",id,status,end,next,best) }
	if err:=rows.Err();err!=nil{panic(err)}
}
