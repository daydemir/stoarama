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
  rows, err := db.Query(ctx, `SELECT c.clip_start_at,c.clip_end_at,c.created_at,c.size_bytes FROM recording_clips c WHERE c.recording_job_id=4249381 ORDER BY c.clip_start_at DESC LIMIT 12`)
  if err != nil { panic(err) }
  defer rows.Close()
  for rows.Next() { var start,end,created any; var bytes int64; if err:=rows.Scan(&start,&end,&created,&bytes); err!=nil { panic(err) }; fmt.Printf("start=%v end=%v created=%v bytes=%d\n",start,end,created,bytes) }
  if err:=rows.Err(); err!=nil { panic(err) }
}
