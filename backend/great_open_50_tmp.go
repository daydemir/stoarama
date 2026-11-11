package main

import("bytes";"context";"encoding/json";"fmt";"io";"net/http";"os";"strings";"time";"github.com/jackc/pgx/v5/pgxpool")

var targets=[]int64{348,349,350,381,382,384,402,403,404,408,410,411,412,414,432,433,434,435,436,442}

type rec struct{id int64;status,mode,tz,ds,de string;clip,week int;start time.Time;end *time.Time}

func main(){
	ctx:=context.Background();db,e:=pgxpool.New(ctx,os.Getenv("DATABASE_URL"));if e!=nil{panic(e)};defer db.Close()
	base:=strings.TrimRight(os.Getenv("BACKEND_API_URL"),"/");token:=os.Getenv("API_TOKEN");if base==""||token==""{panic("missing API_TOKEN or BACKEND_API_URL")}
	rows,e:=db.Query(ctx,`select id,status,mode,cron_timezone,daily_window_start::text,daily_window_end::text,clip_duration_sec,active_weekdays,start_at,end_at from recordings where account_id=47 and id=any($1::bigint[]) order by id`,targets);if e!=nil{panic(e)}
	var rs []rec
	for rows.Next(){var x rec;if e:=rows.Scan(&x.id,&x.status,&x.mode,&x.tz,&x.ds,&x.de,&x.clip,&x.week,&x.start,&x.end);e!=nil{panic(e)};rs=append(rs,x)};rows.Close();if e:=rows.Err();e!=nil{panic(e)}
	if len(rs)!=len(targets){panic(fmt.Sprintf("target rows %d/%d",len(rs),len(targets)))}
	// A single fence covers every target before the first mutation.
	for _,x:=range rs{var p,l,h,i int; e=db.QueryRow(ctx,`select (select count(*) from recording_jobs where recording_id=$1 and status='pending'),(select count(*) from recording_jobs where recording_id=$1 and status='leased' and lease_expires_at>now()),(select count(*) from recording_jobs where recording_id=$1 and handoff_until>now()),(select count(*) from recording_upload_intents where recording_id=$1 and status='pending' and expires_at>now())`,x.id).Scan(&p,&l,&h,&i);if e!=nil{panic(e)};if p+l+h+i>0{fmt.Printf("STOP preflight id=%d status=%s gates=p%d/l%d/h%d/i%d\n",x.id,x.status,p,l,h,i);return};if x.status!="active"&&x.status!="completed"{fmt.Printf("STOP preflight id=%d status=%s\n",x.id,x.status);return}}
	fmt.Printf("PREFLIGHT_OK targets=%d\n",len(rs))
	for _,x:=range rs{loc,e:=time.LoadLocation(x.tz);if e!=nil{fmt.Printf("STOP id=%d timezone=%v\n",x.id,e);return};want:=time.Date(2026,9,9,20,0,0,0,loc).UTC();start:=x.start.UTC();if x.status=="completed"{start=time.Now().UTC()};body:=map[string]any{"mode":x.mode,"cron_timezone":x.tz,"daily_window_start":x.ds[:5],"daily_window_end":x.de[:5],"clip_duration_sec":x.clip,"start_at":start.Format(time.RFC3339Nano),"end_at":want.Format(time.RFC3339Nano)};raw,_:=json.Marshal(body);req,e:=http.NewRequestWithContext(ctx,http.MethodPatch,fmt.Sprintf("%s/api/v1/account/recordings/%d/schedule",base,x.id),bytes.NewReader(raw));if e!=nil{fmt.Printf("STOP id=%d request=%v\n",x.id,e);return};req.Header.Set("Authorization","Bearer "+token);req.Header.Set("Content-Type","application/json");resp,e:=http.DefaultClient.Do(req);if e!=nil{fmt.Printf("STOP id=%d do=%v\n",x.id,e);return};out,_:=io.ReadAll(io.LimitReader(resp.Body,1<<20));resp.Body.Close();if resp.StatusCode/100!=2{fmt.Printf("STOP id=%d http=%d body=%s\n",x.id,resp.StatusCode,strings.TrimSpace(string(out)));return};var got rec;var gotNext *time.Time;e=db.QueryRow(ctx,`select id,status,mode,cron_timezone,daily_window_start::text,daily_window_end::text,clip_duration_sec,active_weekdays,start_at,end_at,next_fire_at from recordings where id=$1 and account_id=47`,x.id).Scan(&got.id,&got.status,&got.mode,&got.tz,&got.ds,&got.de,&got.clip,&got.week,&got.start,&got.end,&gotNext);ok:=e==nil&&got.status=="active"&&got.mode==x.mode&&got.tz==x.tz&&got.clip==x.clip&&got.week==x.week&&got.end!=nil&&got.end.Equal(want)&&gotNext!=nil;if x.status=="active"{ok=ok&&got.start.Equal(x.start)};nextText:="<nil>";if gotNext!=nil{nextText=gotNext.UTC().Format(time.RFC3339)};endText:="<nil>";if got.end!=nil{endText=got.end.UTC().Format(time.RFC3339)};fmt.Printf("APPLY id=%d prior=%s start=%s end=%s next=%s verified=%t\n",x.id,x.status,got.start.UTC().Format(time.RFC3339),endText,nextText,ok);if !ok{return}}
}
