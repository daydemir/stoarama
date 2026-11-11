package main

import (
  "context"
  "encoding/json"
  "fmt"
  "os"
  "github.com/jackc/pgx/v5/pgxpool"
)

func main() {
  ctx:=context.Background(); p,e:=pgxpool.New(ctx,os.Getenv("DATABASE_URL")); if e!=nil { panic(e) }; defer p.Close()
  var b []byte
  q:=`select jsonb_build_object('recording',r.id,'status',r.status,'mode',r.mode,'timezone',r.cron_timezone,'daily_start',r.daily_window_start,'daily_end',r.daily_window_end,'start_at',r.start_at,'end_at',r.end_at,'next_fire_at',r.next_fire_at,'capture_via',r.capture_via,'source_kind',r.source_kind,'stream_url',r.stream_url,'stream_id',r.stream_id,'runtime_status',coalesce(rt.status,''),'runtime_last_frame',rt.last_frame_at,'runtime_errors',coalesce(rt.consecutive_errors,0),'pending',(select count(*) from recording_jobs j where j.recording_id=r.id and j.status='pending'),'leased',(select count(*) from recording_jobs j where j.recording_id=r.id and j.status='leased' and j.lease_expires_at>now()),'handoff',(select count(*) from recording_jobs j where j.recording_id=r.id and j.handoff_until>now()),'pending_intents',(select count(*) from recording_upload_intents u where u.recording_id=r.id and u.status='pending' and u.expires_at>now()),'expired_intents',(select count(*) from recording_upload_intents u where u.recording_id=r.id and u.status='pending' and u.expires_at<=now())) from recordings r left join stream_capture_runtime rt on rt.stream_id=r.stream_id where r.id=426 and r.account_id=47`
  if e=p.QueryRow(ctx,q).Scan(&b); e!=nil { panic(e) }; var x any; _=json.Unmarshal(b,&x); o,_:=json.Marshal(x); fmt.Println(string(o))
}
