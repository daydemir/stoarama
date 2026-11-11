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

type target struct{ id int64 }

func main() {
	ctx := context.Background()
	db, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL")); if err != nil { panic(err) }; defer db.Close()
	base := strings.TrimRight(os.Getenv("BACKEND_API_URL"), "/")
	token := os.Getenv("API_TOKEN")
	if base == "" || token == "" { panic("missing API_TOKEN or BACKEND_API_URL") }
	for _, t := range []target{{406},{409},{417},{427},{401}} { extend(ctx, db, base, token, t.id) }
}

func extend(ctx context.Context, db *pgxpool.Pool, base, token string, id int64) {
	var status, mode, tz, ds, de string
	var clip, weekdays int
	var start, end time.Time
	var next *time.Time
	err := db.QueryRow(ctx, `SELECT status,mode,cron_timezone,daily_window_start::text,daily_window_end::text,clip_duration_sec,active_weekdays,start_at,end_at,next_fire_at FROM recordings WHERE id=$1 AND account_id=47`, id).Scan(&status,&mode,&tz,&ds,&de,&clip,&weekdays,&start,&end,&next)
	if err != nil { fmt.Printf("HOLD %d catalog=%v\n",id,err); return }
	loc, err := time.LoadLocation(tz); if err != nil { fmt.Printf("HOLD %d timezone=%v\n",id,err); return }
	want := time.Date(2026,9,9,20,0,0,0,loc)
	for {
		var p,l,h,i int
		err = db.QueryRow(ctx, `SELECT (SELECT count(*) FROM recording_jobs j WHERE j.recording_id=$1 AND j.status='pending'),(SELECT count(*) FROM recording_jobs j WHERE j.recording_id=$1 AND j.status='leased' AND j.lease_expires_at>now()),(SELECT count(*) FROM recording_jobs j WHERE j.recording_id=$1 AND j.handoff_until>now()),(SELECT count(*) FROM recording_upload_intents u WHERE u.recording_id=$1 AND u.status='pending' AND u.expires_at>now())`, id).Scan(&p,&l,&h,&i)
		if err != nil { fmt.Printf("HOLD %d gate_query=%v\n",id,err); return }
		if p+l+h+i == 0 { break }
		fmt.Printf("WAIT %d gates=p%d/l%d/h%d/i%d\n",id,p,l,h,i)
		time.Sleep(30*time.Second)
	}
	if status != "active" { fmt.Printf("HOLD %d status=%s\n",id,status); return }
	if !end.Before(want) { fmt.Printf("SKIP %d already_end=%s\n",id,end.UTC().Format(time.RFC3339)); return }
	body := map[string]any{"mode":mode,"cron_timezone":tz,"daily_window_start":ds[:5],"daily_window_end":de[:5],"clip_duration_sec":clip,"start_at":start.UTC().Format(time.RFC3339Nano),"end_at":want.UTC().Format(time.RFC3339Nano)}
	raw,_ := json.Marshal(body)
	req,err := http.NewRequestWithContext(ctx,http.MethodPatch,fmt.Sprintf("%s/api/v1/account/recordings/%d/schedule",base,id),bytes.NewReader(raw)); if err != nil { fmt.Printf("HOLD %d request=%v\n",id,err); return }
	req.Header.Set("Authorization","Bearer "+token); req.Header.Set("Content-Type","application/json")
	resp,err := http.DefaultClient.Do(req); if err != nil { fmt.Printf("HOLD %d do=%v\n",id,err); return }
	out,_ := io.ReadAll(io.LimitReader(resp.Body,1<<20)); resp.Body.Close()
	if resp.StatusCode/100 != 2 { fmt.Printf("HOLD %d status=%d body=%s\n",id,resp.StatusCode,strings.TrimSpace(string(out))); return }
	var gotStatus,gotMode,gotTZ,gotDS,gotDE string; var gotClip,gotWeek int; var gotStart,gotEnd time.Time; var gotNext *time.Time
	err = db.QueryRow(ctx, `SELECT status,mode,cron_timezone,daily_window_start::text,daily_window_end::text,clip_duration_sec,active_weekdays,start_at,end_at,next_fire_at FROM recordings WHERE id=$1 AND account_id=47`, id).Scan(&gotStatus,&gotMode,&gotTZ,&gotDS,&gotDE,&gotClip,&gotWeek,&gotStart,&gotEnd,&gotNext)
	verified := err==nil && gotStatus==status && gotMode==mode && gotTZ==tz && gotDS==ds && gotDE==de && gotClip==clip && gotWeek==weekdays && gotStart.Equal(start) && gotEnd.Equal(want) && ((next==nil&&gotNext==nil)||(next!=nil&&gotNext!=nil&&gotNext.Equal(*next)))
	fmt.Printf("APPLY %d old=%s new=%s next_preserved=%t verified=%t\n",id,end.UTC().Format(time.RFC3339),gotEnd.UTC().Format(time.RFC3339),nextPreserved(next,gotNext),verified)
}

func nextPreserved(a,b *time.Time) bool { if a==nil||b==nil{return a==nil&&b==nil}; return a.Equal(*b) }
