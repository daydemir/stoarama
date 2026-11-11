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
  var pending,expired,consumed int
  var oldest any
  err = db.QueryRow(ctx, `SELECT count(*) FILTER (WHERE status='pending'),count(*) FILTER (WHERE status='expired'),count(*) FILTER (WHERE status='consumed'),min(created_at) FILTER (WHERE status='pending') FROM recording_upload_intents WHERE recording_job_id=4249381`).Scan(&pending,&expired,&consumed,&oldest)
  if err != nil { panic(err) }
  fmt.Printf("pending=%d expired=%d consumed=%d oldest_pending=%v\n",pending,expired,consumed,oldest)
  rows, err := db.Query(ctx, `SELECT id,object_key,status,expires_at,created_at FROM recording_upload_intents WHERE recording_job_id=4249381 AND status='pending' ORDER BY created_at`)
  if err != nil { panic(err) }
  defer rows.Close()
  for rows.Next() { var id,key,status string; var exp,created any; if err:=rows.Scan(&id,&key,&status,&exp,&created);err!=nil { panic(err) }; fmt.Printf("intent=%s key=%s status=%s expires=%v created=%v\n",id,key,status,exp,created) }
  if err:=rows.Err();err!=nil { panic(err) }
}
