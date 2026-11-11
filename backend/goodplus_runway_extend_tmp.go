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

// Keep every currently active streak candidate alive long enough to complete
// its 14-window run. This only uses the supported schedule API and requires a
// zero-busy safety gate before each patch.
var ids = []int64{337,339,355,381,385,402,405,407,408,418,419,420,428,431}

func main(){
 ctx:=context.Background(); db,e:=pgxpool.New(ctx,os.Getenv("DATABASE_URL"));if e!=nil{panic(e)};defer db.Close(); base:=strings.TrimRight(os.Getenv("BACKEND_API_URL"),"/"); token:=os.Getenv("MITSCL_API_TOKEN");if base==""||token==""{panic("missing API endpoint/token")}
 for _,id:=range ids { extend(ctx,db,base,token,id) }
}

func extend(ctx context.Context,db *pgxpool.Pool,base,token string,id int64){
 var status,mode,tz,ds,de string;var clip,week,p,l,h,i int;var start,end time.Time;var next *time.Time
 q:=`select r.status,r.mode,r.cron_timezone,r.daily_window_start::text,r.daily_window_end::text,r.clip_duration_sec,r.active_weekdays,r.start_at,r.end_at,r.next_fire_at,(select count(*) from recording_jobs j where j.recording_id=r.id and j.status='pending'),(select count(*) from recording_jobs j where j.recording_id=r.id and j.status='leased' and j.lease_expires_at>now()),(select count(*) from recording_jobs j where j.recording_id=r.id and j.handoff_until>now()),(select count(*) from recording_upload_intents u where u.recording_id=r.id and u.status='pending' and u.expires_at>now()) from recordings r where r.id=$1 and r.account_id=47`
 if e:=db.QueryRow(ctx,q,id).Scan(&status,&mode,&tz,&ds,&de,&clip,&week,&start,&end,&next,&p,&l,&h,&i);e!=nil{fmt.Printf("HOLD %d load=%v\n",id,e);return}
 if status!="active"{fmt.Printf("HOLD %d status=%s\n",id,status);return};if p+l+h+i!=0{fmt.Printf("HOLD %d busy=p%d/l%d/h%d/i%d\n",id,p,l,h,i);return}
 loc,e:=time.LoadLocation(tz);if e!=nil{fmt.Printf("HOLD %d tz=%v\n",id,e);return};want:=time.Date(2026,10,1,0,0,0,0,loc);if !end.Before(want){fmt.Printf("SKIP %d end=%s\n",id,end.UTC().Format(time.RFC3339));return}
 body:=map[string]any{"mode":mode,"cron_timezone":tz,"daily_window_start":ds[:5],"daily_window_end":de[:5],"clip_duration_sec":clip,"start_at":start.UTC().Format(time.RFC3339Nano),"end_at":want.UTC().Format(time.RFC3339Nano)};raw,_:=json.Marshal(body);req,e:=http.NewRequestWithContext(ctx,http.MethodPatch,fmt.Sprintf("%s/api/v1/account/recordings/%d/schedule",base,id),bytes.NewReader(raw));if e!=nil{fmt.Printf("HOLD %d req=%v\n",id,e);return};req.Header.Set("Authorization","Bearer "+token);req.Header.Set("Content-Type","application/json");resp,e:=http.DefaultClient.Do(req);if e!=nil{fmt.Printf("HOLD %d http=%v\n",id,e);return};out,_:=io.ReadAll(io.LimitReader(resp.Body,1<<20));resp.Body.Close();if resp.StatusCode/100!=2{fmt.Printf("HOLD %d http_status=%d body=%s\n",id,resp.StatusCode,strings.TrimSpace(string(out)));return}
 var gotStatus,gotMode,gotTZ,gotDS,gotDE string;var gotClip,gotWeek int;var gotStart,gotEnd time.Time;var gotNext *time.Time;e=db.QueryRow(ctx,`select status,mode,cron_timezone,daily_window_start::text,daily_window_end::text,clip_duration_sec,active_weekdays,start_at,end_at,next_fire_at from recordings where id=$1 and account_id=47`,id).Scan(&gotStatus,&gotMode,&gotTZ,&gotDS,&gotDE,&gotClip,&gotWeek,&gotStart,&gotEnd,&gotNext);ok:=e==nil&&gotStatus==status&&gotMode==mode&&gotTZ==tz&&gotDS==ds&&gotDE==de&&gotClip==clip&&gotWeek==week&&gotStart.Equal(start)&&gotEnd.Equal(want)&&((next==nil&&gotNext==nil)||(next!=nil&&gotNext!=nil&&gotNext.Equal(*next)));fmt.Printf("APPLY %d old=%s new=%s next_preserved=%t verified=%t\n",id,end.UTC().Format(time.RFC3339),gotEnd.UTC().Format(time.RFC3339),preserved(next,gotNext),ok)
}
func preserved(a,b *time.Time)bool{if a==nil||b==nil{return a==nil&&b==nil};return a.Equal(*b)}
