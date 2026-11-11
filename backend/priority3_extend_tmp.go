package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

func main() {
	c := context.Background()
	p, e := pgxpool.New(c, os.Getenv("DATABASE_URL"))
	if e != nil {
		panic(e)
	}
	defer p.Close()
	base := strings.TrimRight(os.Getenv("BACKEND_API_URL"), "/")
	token := os.Getenv("MITSCL_API_TOKEN")
	for id, delta := range map[int]int{409: 3} {
		var mode, tz, ds, de string
		var clip, week, pending, leased, handoff, intents int
		var start, end, next time.Time
		e = p.QueryRow(c, `SELECT r.mode,r.cron_timezone,r.daily_window_start::text,r.daily_window_end::text,r.clip_duration_sec,r.active_weekdays,r.start_at,r.end_at,r.next_fire_at,(SELECT count(*) FROM recording_jobs j WHERE j.recording_id=r.id AND j.status='pending'),(SELECT count(*) FROM recording_jobs j WHERE j.recording_id=r.id AND j.status='leased' AND j.lease_expires_at>now()),(SELECT count(*) FROM recording_jobs j WHERE j.recording_id=r.id AND j.handoff_until>now()),(SELECT count(*) FROM recording_upload_intents u WHERE u.recording_id=r.id AND u.status='pending' AND u.expires_at>now()) FROM recordings r WHERE r.id=$1 AND r.account_id=47 AND r.status='active'`, id).Scan(&mode, &tz, &ds, &de, &clip, &week, &start, &end, &next, &pending, &leased, &handoff, &intents)
		if e != nil {
			panic(e)
		}
		if pending+leased+handoff+intents > 0 {
			fmt.Printf("HOLD %d p%d/l%d/h%d/i%d\n", id, pending, leased, handoff, intents)
			continue
		}
		loc, _ := time.LoadLocation(tz)
		want := end.In(loc).AddDate(0, 0, delta)
		if !end.Before(time.Date(2026, 8, 25, 21, 0, 0, 0, time.UTC)) {
			fmt.Printf("SKIP %d target already reached end=%s\n", id, end.UTC().Format(time.RFC3339))
			continue
		}
		body := map[string]any{"mode": mode, "cron_timezone": tz, "daily_window_start": ds[:5], "daily_window_end": de[:5], "clip_duration_sec": clip, "start_at": start.UTC().Format(time.RFC3339Nano), "end_at": want.UTC().Format(time.RFC3339Nano)}
		raw, _ := json.Marshal(body)
		req, _ := http.NewRequestWithContext(c, http.MethodPatch, fmt.Sprintf("%s/api/v1/account/recordings/%d/schedule", base, id), bytes.NewReader(raw))
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		resp, e := http.DefaultClient.Do(req)
		if e != nil {
			panic(e)
		}
		out, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if resp.StatusCode/100 != 2 {
			fmt.Printf("ERROR %d status=%d body=%s\n", id, resp.StatusCode, string(out))
			continue
		}
		var gotEnd, gotNext time.Time
		var gotMode, gotTZ, gotDS, gotDE string
		var gotClip, gotWeek int
		e = p.QueryRow(c, `SELECT end_at,next_fire_at,mode,cron_timezone,daily_window_start::text,daily_window_end::text,clip_duration_sec,active_weekdays FROM recordings WHERE id=$1`, id).Scan(&gotEnd, &gotNext, &gotMode, &gotTZ, &gotDS, &gotDE, &gotClip, &gotWeek)
		if e != nil {
			panic(e)
		}
		ok := gotEnd.Equal(want) && gotNext.Equal(next) && gotMode == mode && gotTZ == tz && gotDS == ds && gotDE == de && gotClip == clip && gotWeek == week
		fmt.Printf("APPLY %d delta=%d old=%s new=%s next=%s verified=%t\n", id, delta, end.UTC().Format(time.RFC3339), gotEnd.UTC().Format(time.RFC3339), gotNext.UTC().Format(time.RFC3339), ok)
	}
}
