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

var targets = []int64{418, 419, 420, 428, 431}

type row struct {
	id int64
	status, mode, tz, ds, de string
	clip int
	start, end time.Time
	pending, leased, handoff, intents int
}

func main() {
	ctx := context.Background()
	db, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil { panic(err) }
	defer db.Close()
	base := strings.TrimRight(os.Getenv("BACKEND_API_URL"), "/")
	token := os.Getenv("MITSCL_API_TOKEN")
	if base == "" || token == "" { panic("missing schedule API environment") }
	failed := false
	waiting := false
	for _, id := range targets {
		var r row
		err := db.QueryRow(ctx, `
			SELECT r.id,r.status,r.mode,r.cron_timezone,r.daily_window_start::text,
			       r.daily_window_end::text,r.clip_duration_sec,r.start_at,r.end_at,
			       (SELECT count(*) FROM recording_jobs j WHERE j.recording_id=r.id AND j.status='pending'),
			       (SELECT count(*) FROM recording_jobs j WHERE j.recording_id=r.id AND j.status='leased' AND j.lease_expires_at>now()),
			       (SELECT count(*) FROM recording_jobs j WHERE j.recording_id=r.id AND j.handoff_until>now()),
			       (SELECT count(*) FROM recording_upload_intents u WHERE u.recording_id=r.id AND u.status='pending' AND u.expires_at>now())
			FROM recordings r WHERE r.id=$1 AND r.account_id=47`, id).
			Scan(&r.id,&r.status,&r.mode,&r.tz,&r.ds,&r.de,&r.clip,&r.start,&r.end,
				&r.pending,&r.leased,&r.handoff,&r.intents)
		if err != nil { fmt.Printf("HOLD %d load=%v\n", id, err); failed = true; continue }
		if r.status != "active" || r.mode != "continuous" { fmt.Printf("HOLD %d status=%s mode=%s\n", id, r.status, r.mode); failed = true; continue }
		loc, err := time.LoadLocation(r.tz)
		if err != nil { fmt.Printf("HOLD %d timezone=%v\n", id, err); failed = true; continue }
		want := time.Date(2026, 9, 9, 20, 0, 0, 0, loc)
		if !r.end.Before(want) {
			fmt.Printf("SKIP %d already_end=%s\n", id, r.end.UTC().Format(time.RFC3339))
			continue
		}
		if r.pending+r.leased+r.handoff+r.intents != 0 {
			fmt.Printf("WAIT %d gates=p%d/l%d/h%d/i%d end=%s\n", id, r.pending,r.leased,r.handoff,r.intents,r.end.UTC().Format(time.RFC3339))
			waiting = true
			continue
		}
		body := map[string]any{"mode":r.mode,"cron_timezone":r.tz,"daily_window_start":r.ds[:5],"daily_window_end":r.de[:5],"clip_duration_sec":r.clip,"start_at":r.start.UTC().Format(time.RFC3339Nano),"end_at":want.UTC().Format(time.RFC3339Nano)}
		raw, _ := json.Marshal(body)
		req, err := http.NewRequestWithContext(ctx,http.MethodPatch,fmt.Sprintf("%s/api/v1/account/recordings/%d/schedule",base,id),bytes.NewReader(raw))
		if err != nil { fmt.Printf("HOLD %d request=%v\n", id, err); failed=true; continue }
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil { fmt.Printf("HOLD %d http=%v\n", id, err); failed=true; continue }
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20)); _ = resp.Body.Close()
		if resp.StatusCode/100 != 2 { fmt.Printf("HOLD %d http_status=%d\n", id, resp.StatusCode); failed=true; continue }
		var got row
		var next *time.Time
		err = db.QueryRow(ctx, `SELECT status,mode,cron_timezone,daily_window_start::text,daily_window_end::text,clip_duration_sec,start_at,end_at,next_fire_at FROM recordings WHERE id=$1`, id).
			Scan(&got.status,&got.mode,&got.tz,&got.ds,&got.de,&got.clip,&got.start,&got.end,&next)
		ok := err == nil && got.status == r.status && got.mode == r.mode && got.tz == r.tz && got.ds == r.ds && got.de == r.de && got.clip == r.clip && got.start.Equal(r.start) && got.end.Equal(want) && next != nil
		fmt.Printf("APPLY %d old_end=%s new_end=%s next=%s verified=%t\n", id,r.end.UTC().Format(time.RFC3339),got.end.UTC().Format(time.RFC3339),func() string { if next==nil{return "<nil>"}; return next.UTC().Format(time.RFC3339) }(),ok)
		if !ok { failed=true }
	}
	if failed { os.Exit(1) }
	if waiting { os.Exit(2) }
}
