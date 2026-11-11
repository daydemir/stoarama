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

func main() {
	ctx := context.Background()
	p, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		panic(err)
	}
	defer p.Close()
	base := strings.TrimRight(os.Getenv("BACKEND_API_URL"), "/")
	for _, id := range []int{379, 380, 383, 404, 413, 416, 421, 422, 423, 424, 425, 429, 430, 439, 440, 441, 444} {
		var status, mode, tz, ds, de string
		var clip, week, pending, leased, handoff, intents int
		var start, end time.Time
		var next *time.Time
		err = p.QueryRow(ctx, `select r.status,r.mode,r.cron_timezone,r.daily_window_start::text,r.daily_window_end::text,r.clip_duration_sec,r.active_weekdays,r.start_at,r.end_at,r.next_fire_at,
			(select count(*) from recording_jobs j where j.recording_id=r.id and j.status='pending'),
			(select count(*) from recording_jobs j where j.recording_id=r.id and j.status='leased' and j.lease_expires_at>now()),
			(select count(*) from recording_jobs j where j.recording_id=r.id and j.handoff_until>now()),
			(select count(*) from recording_upload_intents u where u.recording_id=r.id and u.status='pending' and u.expires_at>now())
			from recordings r where r.account_id=47 and r.id=$1`, id).Scan(&status, &mode, &tz, &ds, &de, &clip, &week, &start, &end, &next, &pending, &leased, &handoff, &intents)
		if err != nil {
			fmt.Printf("ERROR %d load=%v\n", id, err)
			continue
		}
		if pending+leased+handoff+intents != 0 {
			fmt.Printf("HOLD %d p%d/l%d/h%d/i%d\n", id, pending, leased, handoff, intents)
			continue
		}
		var settledEnd time.Time
		var jobStatus string
		err = p.QueryRow(ctx, `select window_end_at,status from recording_jobs where recording_id=$1 and kind='continuous_window' and window_end_at<=now() and status in('done','error','canceled') order by window_end_at desc,id desc limit 1`, id).Scan(&settledEnd, &jobStatus)
		if err != nil {
			fmt.Printf("ERROR %d settled_job=%v\n", id, err)
			continue
		}
		if end.Equal(settledEnd) || end.Before(settledEnd) || status == "completed" {
			fmt.Printf("SKIP %d status=%s end=%s settled=%s\n", id, status, end.UTC().Format(time.RFC3339), settledEnd.UTC().Format(time.RFC3339))
			continue
		}
		body := map[string]any{"mode": mode, "cron_timezone": tz, "daily_window_start": ds[:5], "daily_window_end": de[:5], "clip_duration_sec": clip, "start_at": start.UTC().Format(time.RFC3339Nano), "end_at": settledEnd.UTC().Format(time.RFC3339Nano)}
		raw, _ := json.Marshal(body)
		req, _ := http.NewRequestWithContext(ctx, http.MethodPatch, fmt.Sprintf("%s/api/v1/account/recordings/%d/schedule", base, id), bytes.NewReader(raw))
		req.Header.Set("Authorization", "Bearer "+os.Getenv("MITSCL_API_TOKEN"))
		req.Header.Set("Content-Type", "application/json")
		resp, doErr := http.DefaultClient.Do(req)
		if doErr != nil {
			fmt.Printf("ERROR %d request=%v\n", id, doErr)
			continue
		}
		out, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if resp.StatusCode/100 != 2 {
			fmt.Printf("ERROR %d status=%d body=%s\n", id, resp.StatusCode, string(out))
			continue
		}
		var gotStatus, gotMode, gotTZ, gotDS, gotDE string
		var gotEnd time.Time
		var gotNext *time.Time
		var gotClip, gotWeek int
		err = p.QueryRow(ctx, `select status,mode,cron_timezone,daily_window_start::text,daily_window_end::text,clip_duration_sec,active_weekdays,end_at,next_fire_at from recordings where id=$1`, id).Scan(&gotStatus, &gotMode, &gotTZ, &gotDS, &gotDE, &gotClip, &gotWeek, &gotEnd, &gotNext)
		ok := err == nil && gotEnd.Equal(settledEnd) && gotNext == nil && gotMode == mode && gotTZ == tz && gotDS == ds && gotDE == de && gotClip == clip && gotWeek == week
		fmt.Printf("STOP %d job_status=%s old=%s new=%s status=%s next_nil=%t verified=%t\n", id, jobStatus, end.UTC().Format(time.RFC3339), gotEnd.UTC().Format(time.RFC3339), gotStatus, gotNext == nil, ok)
	}
}
