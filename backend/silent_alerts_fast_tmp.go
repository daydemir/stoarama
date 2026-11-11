package main

import (
  "context"
  "fmt"
  "os"
  "github.com/jackc/pgx/v5/pgxpool"
)

func main() {
  ctx := context.Background()
  db, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL")); if err != nil { panic(err) }
  defer db.Close()
  rows, err := db.Query(ctx, `
SELECT r.id,r.name,j.id,
       latest.latest_start,latest.latest_end,latest.latest_created,
       EXTRACT(EPOCH FROM (now()-latest.latest_start))::bigint,
       EXTRACT(EPOCH FROM (now()-latest.latest_end))::bigint,
       EXTRACT(EPOCH FROM (now()-latest.latest_created))::bigint
FROM recordings r
JOIN LATERAL (
  SELECT id
  FROM recording_jobs
  WHERE recording_id=r.id AND kind='continuous_window' AND fire_at<=now() AND window_end_at>now()
  ORDER BY id DESC LIMIT 1
) j ON true
LEFT JOIN LATERAL (
  SELECT max(clip_start_at) latest_start,max(clip_end_at) latest_end,max(created_at) latest_created
  FROM recording_clips WHERE recording_job_id=j.id
) latest ON true
WHERE r.account_id=47 AND r.status='active' AND r.mode='continuous'
ORDER BY r.id`,)
  if err != nil { panic(err) }
  defer rows.Close()
  for rows.Next() {
    var id,job int64; var name string; var start,end,created any; var sl,el,il *int64
    if err:=rows.Scan(&id,&name,&job,&start,&end,&created,&sl,&el,&il); err!=nil { panic(err) }
    fmt.Printf("id=%d name=%s job=%d latest_start=%v latest_end=%v latest_created=%v start_lag_s=%v end_lag_s=%v ingest_lag_s=%v\n",id,name,job,start,end,created,sl,el,il)
  }
  if err:=rows.Err();err!=nil { panic(err) }
}
