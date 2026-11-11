package main

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	ctx:=context.Background(); db,err:=pgxpool.New(ctx,os.Getenv("DATABASE_URL")); if err!=nil{panic(err)}; defer db.Close()
	rows,err:=db.Query(ctx,`
WITH graded AS (
 SELECT h.recording_id,h.job_id,(h.window_start_at AT TIME ZONE r.cron_timezone)::date d,
 CASE WHEN h.metric_version IS NULL OR h.metric_version<>2 OR h.gap_over_30s_count IS NULL OR h.gap_over_5m_count IS NULL THEN 'U'
      WHEN h.overlap_count=0 AND h.coverage_pct>=99 AND h.largest_gap_seconds<=120 THEN 'A'
      WHEN h.overlap_count=0 AND h.coverage_pct>=95 AND h.largest_gap_seconds<=900 AND h.gap_over_5m_count<=1 AND h.gap_over_30s_count<=6 THEN 'B'
      WHEN h.overlap_count=0 AND h.coverage_pct>=90 AND h.largest_gap_seconds<=1800 AND h.gap_over_5m_count<=2 THEN 'C'
      WHEN h.clip_count=0 OR h.coverage_pct=0 THEN 'F' WHEN h.coverage_pct<80 THEN 'E' ELSE 'D' END g
 FROM recording_window_health h JOIN recordings r ON r.id=h.recording_id WHERE r.account_id=47 AND r.mode='continuous'
), good_only AS (
 SELECT * FROM graded WHERE g IN ('A','B','C')
), islands AS (
 SELECT *, d - (row_number() OVER (PARTITION BY recording_id ORDER BY d,job_id))::int grp FROM good_only
), runs AS (
 SELECT recording_id,min(d) start_d,max(d) end_d,count(*) n FROM islands WHERE g IN ('A','B','C') GROUP BY recording_id,grp
), best AS (
 SELECT recording_id,max(n) max_good_streak FROM runs GROUP BY recording_id
), counts AS (
 SELECT recording_id,count(*) total,
  count(*) FILTER (WHERE g='A') a,count(*) FILTER (WHERE g='B') b,count(*) FILTER (WHERE g='C') c,
  count(*) FILTER (WHERE g='D') d,count(*) FILTER (WHERE g='E') e,count(*) FILTER (WHERE g='F') f,
  count(*) FILTER (WHERE g='U') u FROM graded GROUP BY recording_id
), latest AS (SELECT DISTINCT ON (recording_id) recording_id,d,g FROM graded ORDER BY recording_id,d DESC,job_id DESC)
SELECT r.id,r.status,coalesce(b.max_good_streak,0),coalesce(c.total,0),coalesce(c.a,0),coalesce(c.b,0),coalesce(c.c,0),coalesce(c.d,0),coalesce(c.e,0),coalesce(c.f,0),coalesce(c.u,0),l.d,l.g,r.end_at
FROM recordings r LEFT JOIN best b ON b.recording_id=r.id LEFT JOIN counts c ON c.recording_id=r.id LEFT JOIN latest l ON l.recording_id=r.id
WHERE r.account_id=47 AND r.mode='continuous' ORDER BY coalesce(b.max_good_streak,0) DESC,r.id`)
	if err!=nil{panic(err)}; defer rows.Close()
	for rows.Next(){var id int64;var status,lg string;var streak,total,a,b,c,d,e,f,u int;var ld,end any;if err:=rows.Scan(&id,&status,&streak,&total,&a,&b,&c,&d,&e,&f,&u,&ld,&lg,&end);err!=nil{panic(err)};fmt.Printf("HIST\t%d\t%s\tstreak=%d\ttotal=%d\tA=%d B=%d C=%d D=%d E=%d F=%d U=%d\tlatest=%v/%s\tend=%v\n",id,status,streak,total,a,b,c,d,e,f,u,ld,lg,end)}
	if err:=rows.Err();err!=nil{panic(err)}
}
