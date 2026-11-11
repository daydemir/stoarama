package main

import (
 "bytes"
 "context"
 "encoding/json"
 "fmt"
 "io"
 "net/http"
 "os"
 "strings"
 "time"
 "github.com/jackc/pgx/v5/pgxpool"
)

var ids = []int64{406,408,409,410,411,412,414,417,432,433,434,435,436,442,445}

func main() {
 ctx:=context.Background(); db,e:=pgxpool.New(ctx,os.Getenv("DATABASE_URL")); if e!=nil {panic(e)}; defer db.Close()
 base:=strings.TrimRight(os.Getenv("BACKEND_API_URL"),"/"); token:=os.Getenv("MITSCL_API_TOKEN"); if token=="" {token=os.Getenv("API_TOKEN")}
 for _,id:=range ids {
  var mode,tz,ds,de string; var clip,leased,pending,handoff,intents int; var start,end time.Time
  e=db.QueryRow(ctx,`select r.mode,r.cron_timezone,r.daily_window_start::text,r.daily_window_end::text,r.clip_duration_sec,r.start_at,r.end_at,
   (select count(*) from recording_jobs j where j.recording_id=r.id and j.status='leased' and j.lease_expires_at>now()),
   (select count(*) from recording_jobs j where j.recording_id=r.id and j.status='pending'),
   (select count(*) from recording_jobs j where j.recording_id=r.id and j.handoff_until>now()),
   (select count(*) from recording_upload_intents u where u.recording_id=r.id and u.status='pending' and u.expires_at>now())
   from recordings r where r.id=$1 and r.account_id=47 and r.status='active'`,id).Scan(&mode,&tz,&ds,&de,&clip,&start,&end,&leased,&pending,&handoff,&intents)
  if e!=nil {fmt.Printf("BLOCK %d load=%v\n",id,e); continue}
  loc,e:=time.LoadLocation(tz); if e!=nil {fmt.Printf("BLOCK %d timezone=%v\n",id,e); continue}; want:=time.Date(2026,9,8,0,0,0,0,loc)
  if !end.Before(want) {fmt.Printf("SKIP %d end=%s target=%s\n",id,end.UTC().Format(time.RFC3339),want.UTC().Format(time.RFC3339)); continue}
  if leased+pending+handoff+intents!=0 {fmt.Printf("HOLD %d leased=%d pending=%d handoff=%d intents=%d end=%s\n",id,leased,pending,handoff,intents,end.UTC().Format(time.RFC3339)); continue}
  body:=map[string]any{"mode":mode,"cron_timezone":tz,"daily_window_start":ds[:5],"daily_window_end":de[:5],"clip_duration_sec":clip,"start_at":start.UTC().Format(time.RFC3339Nano),"end_at":want.UTC().Format(time.RFC3339Nano)}; raw,_:=json.Marshal(body)
  req,e:=http.NewRequestWithContext(ctx,http.MethodPatch,fmt.Sprintf("%s/api/v1/account/recordings/%d/schedule",base,id),bytes.NewReader(raw)); if e!=nil {fmt.Printf("BLOCK %d request=%v\n",id,e);continue}; req.Header.Set("Authorization","Bearer "+token); req.Header.Set("Content-Type","application/json")
  resp,e:=http.DefaultClient.Do(req); if e!=nil {fmt.Printf("BLOCK %d http=%v\n",id,e);continue}; out,_:=io.ReadAll(io.LimitReader(resp.Body,1<<20));resp.Body.Close();if resp.StatusCode/100!=2 {fmt.Printf("BLOCK %d status=%d body=%s\n",id,resp.StatusCode,strings.TrimSpace(string(out)));continue}
  var got time.Time; var next *time.Time; e=db.QueryRow(ctx,`select end_at,next_fire_at from recordings where id=$1`,id).Scan(&got,&next); ok:=e==nil&&got.Equal(want);fmt.Printf("APPLY %d old=%s new=%s next=%v verified=%t\n",id,end.UTC().Format(time.RFC3339),got.UTC().Format(time.RFC3339),next,ok)
 }
}
