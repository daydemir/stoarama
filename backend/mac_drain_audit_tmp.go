package main

import (
  "context"
  "encoding/json"
  "fmt"
  "os"
  "github.com/jackc/pgx/v5/pgxpool"
)

func main() {
  ctx:=context.Background(); p,err:=pgxpool.New(ctx,os.Getenv("DATABASE_URL")); if err!=nil { panic(err) }; defer p.Close()
  rows,err:=p.Query(ctx,`SELECT jsonb_build_object('job',j.id,'recording',j.recording_id,'status',j.status,'fire',j.fire_at,'window_end',j.window_end_at,'owner',j.lease_owner,'lease_expires',j.lease_expires_at,'handoff_until',j.handoff_until,'pending_jobs',(SELECT count(*) FROM recording_jobs q WHERE q.recording_id=j.recording_id AND q.status='pending'),'pending_intents',(SELECT count(*) FROM recording_upload_intents u WHERE u.recording_id=j.recording_id AND u.status='pending' AND u.expires_at>now())) FROM recording_jobs j WHERE j.lease_owner IN ('node:93','node:150','node:276') AND (j.status IN ('pending','leased') OR j.handoff_until>now()) ORDER BY j.lease_owner,j.fire_at`); if err!=nil { panic(err) }; defer rows.Close()
  for rows.Next(){ var b []byte; if err:=rows.Scan(&b); err!=nil {panic(err)}; var v any; _=json.Unmarshal(b,&v); o,_:=json.Marshal(v); fmt.Println(string(o)) }
}
