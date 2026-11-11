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

var ids = []int64{337, 339, 348, 350, 355, 382, 385, 403, 405, 407, 408, 409, 410, 411, 412, 414, 418, 419, 420, 428, 431, 432, 433, 434, 435, 436, 442}

const lockID int64 = 0x4752454154455854

type recording struct {
	id int64
	status, mode, timezone, dailyStart, dailyEnd string
	clip, weekdays int
	start, end time.Time
	pending, leased, handoff, intents int
}

func load(ctx context.Context, db *pgxpool.Pool, id int64) (recording, error) {
	var r recording
	err := db.QueryRow(ctx, `SELECT r.id,r.status,r.mode,r.cron_timezone,r.daily_window_start::text,r.daily_window_end::text,r.clip_duration_sec,r.active_weekdays,r.start_at,r.end_at,
		(SELECT count(*) FROM recording_jobs j WHERE j.recording_id=r.id AND j.status='pending'),
		(SELECT count(*) FROM recording_jobs j WHERE j.recording_id=r.id AND j.status='leased' AND j.lease_expires_at>now()),
		(SELECT count(*) FROM recording_jobs j WHERE j.recording_id=r.id AND j.handoff_until>now()),
		(SELECT count(*) FROM recording_upload_intents u WHERE u.recording_id=r.id AND u.status='pending' AND u.expires_at>now())
		FROM recordings r WHERE r.account_id=47 AND r.id=$1`, id).Scan(&r.id,&r.status,&r.mode,&r.timezone,&r.dailyStart,&r.dailyEnd,&r.clip,&r.weekdays,&r.start,&r.end,&r.pending,&r.leased,&r.handoff,&r.intents)
	return r, err
}

func main() {
	ctx := context.Background()
	db, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL")); if err != nil { panic(err) }; defer db.Close()
	var locked bool
	if err = db.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, lockID).Scan(&locked); err != nil { panic(err) }
	if !locked { fmt.Println("HOLD advisory_lock_busy"); os.Exit(2) }
	defer db.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, lockID)
	base := strings.TrimRight(os.Getenv("BACKEND_API_URL"), "/")
	token := os.Getenv("MITSCL_API_TOKEN")
	if base == "" || token == "" { panic("missing BACKEND_API_URL or MITSCL_API_TOKEN") }
	done := map[int64]bool{}
	deadline := time.Now().Add(8 * time.Hour)
	for time.Now().Before(deadline) {
		pending := 0
		for _, id := range ids {
			if done[id] { continue }
			r, e := load(ctx, db, id)
			if e != nil { fmt.Printf("HOLD id=%d load=%v\n", id, e); pending++; continue }
			loc, e := time.LoadLocation(r.timezone); if e != nil { fmt.Printf("HOLD id=%d timezone=%v\n", id, e); pending++; continue }
			localEnd := r.end.In(loc)
			if !time.Date(2026, 9, 16, 0, 0, 0, 0, loc).After(localEnd) { fmt.Printf("SKIP id=%d end=%s\n", id, r.end.UTC().Format(time.RFC3339)); done[id] = true; continue }
			if r.status != "active" || r.mode != "continuous" { fmt.Printf("HOLD id=%d status=%s mode=%s\n", id, r.status, r.mode); pending++; continue }
			if r.pending+r.leased+r.handoff+r.intents != 0 { pending++; continue }
			want := time.Date(2026, 9, 16, localEnd.Hour(), localEnd.Minute(), localEnd.Second(), localEnd.Nanosecond(), loc)
			body := map[string]any{"mode":r.mode,"cron_timezone":r.timezone,"daily_window_start":r.dailyStart[:5],"daily_window_end":r.dailyEnd[:5],"clip_duration_sec":r.clip,"start_at":r.start.UTC().Format(time.RFC3339Nano),"end_at":want.UTC().Format(time.RFC3339Nano)}
			raw, _ := json.Marshal(body)
			req, e := http.NewRequestWithContext(ctx, http.MethodPatch, fmt.Sprintf("%s/api/v1/account/recordings/%d/schedule", base, id), bytes.NewReader(raw)); if e != nil { fmt.Printf("HOLD id=%d request=%v\n", id, e); pending++; continue }
			req.Header.Set("Authorization", "Bearer "+token); req.Header.Set("Content-Type", "application/json")
			resp, e := http.DefaultClient.Do(req); if e != nil { fmt.Printf("HOLD id=%d http=%v\n", id, e); pending++; continue }
			io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20)); resp.Body.Close()
			if resp.StatusCode/100 != 2 { fmt.Printf("HOLD id=%d http_status=%d\n", id, resp.StatusCode); pending++; continue }
			var got time.Time
			e = db.QueryRow(ctx, `SELECT end_at FROM recordings WHERE id=$1 AND account_id=47`, id).Scan(&got)
			ok := e == nil && got.Equal(want)
			fmt.Printf("APPLY id=%d new_end=%s verified=%t\n", id, got.UTC().Format(time.RFC3339), ok)
			if ok { done[id] = true } else { pending++ }
		}
		if pending == 0 { break }
		time.Sleep(30 * time.Second)
	}
	fmt.Printf("SUMMARY selected=%d completed=%d pending=%d\n", len(ids), len(done), len(ids)-len(done))
	if len(done) < len(ids) { os.Exit(2) }
}
