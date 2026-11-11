package main

import (

    "context"
    "fmt"
    "os"

    "github.com/jackc/pgx/v5/pgxpool"
)

func main() {
    ctx := context.Background()
    db, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
    if err != nil { panic(err) }
    defer db.Close()
    rows, err := db.Query(ctx, `
WITH cont AS (
  SELECT r.id, r.name, r.cron_timezone,
    ((now() AT TIME ZONE r.cron_timezone)::date + r.daily_window_start) AT TIME ZONE r.cron_timezone AS win_open,
    ((now() AT TIME ZONE r.cron_timezone)::date + r.daily_window_end) AT TIME ZONE r.cron_timezone AS win_close
  FROM recordings r JOIN account_billing b ON b.account_id=r.account_id
  WHERE r.status='active' AND r.mode='continuous' AND b.has_payment_method=true
), latest AS (
  SELECT c.id,c.name,c.win_open,c.win_close,
    max(cl.clip_start_at) FILTER (WHERE cl.clip_start_at>=c.win_open) AS latest_start,
    max(cl.clip_end_at) FILTER (WHERE cl.clip_start_at>=c.win_open) AS latest_end,
    max(cl.created_at) FILTER (WHERE cl.clip_start_at>=c.win_open) AS latest_created
  FROM cont c LEFT JOIN recording_clips cl ON cl.recording_id=c.id
  GROUP BY c.id,c.name,c.win_open,c.win_close
)
SELECT id,name,latest_start,latest_end,latest_created,
  EXTRACT(EPOCH FROM (now()-latest_start))::bigint AS start_lag_s,
  EXTRACT(EPOCH FROM (now()-latest_end))::bigint AS end_lag_s,
  EXTRACT(EPOCH FROM (now()-latest_created))::bigint AS ingest_lag_s
FROM latest
WHERE now()>=win_open+interval '10 minutes' AND now()<win_close
  AND (latest_start IS NULL OR latest_start < now()-interval '10 minutes')
ORDER BY id`)
    if err != nil { panic(err) }
    defer rows.Close()
    for rows.Next() {
        var id int64
        var name string
        var start, end, created any
        var startLag, endLag, ingestLag *int64
        if err := rows.Scan(&id,&name,&start,&end,&created,&startLag,&endLag,&ingestLag); err != nil { panic(err) }
        fmt.Printf("id=%d name=%s latest_start=%v latest_end=%v latest_created=%v start_lag_s=%v end_lag_s=%v ingest_lag_s=%v\n", id,name,start,end,created,startLag,endLag,ingestLag)
    }
    if err := rows.Err(); err != nil { panic(err) }
}
