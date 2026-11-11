package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	if len(os.Args) != 2 {
		panic("usage: great5_extend_tmp RECORDING_ID")
	}
	id, err := strconv.Atoi(os.Args[1])
	if err != nil {
		panic(err)
	}
	allowed := map[int]bool{401: true, 406: true, 409: true, 417: true, 427: true}
	if !allowed[id] {
		panic("recording not in authorized Great+ cohort")
	}
	ctx := context.Background()
	p, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		panic(err)
	}
	defer p.Close()
	var mode, tz, ds, de string
	var clip, week, pending, leased, handoff, intents int
	var start, end, next time.Time
	err = p.QueryRow(ctx, `select r.mode,r.cron_timezone,r.daily_window_start::text,r.daily_window_end::text,r.clip_duration_sec,r.active_weekdays,r.start_at,r.end_at,r.next_fire_at,
		(select count(*) from recording_jobs j where j.recording_id=r.id and j.status='pending'),
		(select count(*) from recording_jobs j where j.recording_id=r.id and j.status='leased' and j.lease_expires_at>now()),
		(select count(*) from recording_jobs j where j.recording_id=r.id and j.handoff_until>now()),
		(select count(*) from recording_upload_intents u where u.recording_id=r.id and u.status='pending' and u.expires_at>now())
		from recordings r where r.id=$1 and r.account_id=47 and r.status='active'`, id).Scan(&mode, &tz, &ds, &de, &clip, &week, &start, &end, &next, &pending, &leased, &handoff, &intents)
	if err != nil {
		panic(err)
	}
	if pending+leased+handoff+intents != 0 {
		fmt.Printf("HOLD %d p%d/l%d/h%d/i%d\n", id, pending, leased, handoff, intents)
		return
	}
	loc, err := time.LoadLocation(tz)
	if err != nil {
		panic(err)
	}
	want := time.Date(2026, 9, 3, 0, 0, 0, 0, loc)
	if end.Equal(want) || end.After(want) {
		fmt.Printf("SKIP %d already end=%s target=%s\n", id, end.UTC().Format(time.RFC3339), want.UTC().Format(time.RFC3339))
		return
	}
	body := map[string]any{"mode": mode, "cron_timezone": tz, "daily_window_start": ds[:5], "daily_window_end": de[:5], "clip_duration_sec": clip, "start_at": start.UTC().Format(time.RFC3339Nano), "end_at": want.UTC().Format(time.RFC3339Nano)}
	raw, _ := json.Marshal(body)
	base := strings.TrimRight(os.Getenv("BACKEND_API_URL"), "/")
	req, _ := http.NewRequestWithContext(ctx, http.MethodPatch, fmt.Sprintf("%s/api/v1/account/recordings/%d/schedule", base, id), bytes.NewReader(raw))
	req.Header.Set("Authorization", "Bearer "+os.Getenv("MITSCL_API_TOKEN"))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}
	out, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		fmt.Printf("ERROR %d status=%d body=%s\n", id, resp.StatusCode, string(out))
		return
	}
	var gotEnd, gotNext time.Time
	var gotMode, gotTZ, gotDS, gotDE string
	var gotClip, gotWeek int
	err = p.QueryRow(ctx, `select end_at,next_fire_at,mode,cron_timezone,daily_window_start::text,daily_window_end::text,clip_duration_sec,active_weekdays from recordings where id=$1`, id).Scan(&gotEnd, &gotNext, &gotMode, &gotTZ, &gotDS, &gotDE, &gotClip, &gotWeek)
	if err != nil {
		panic(err)
	}
	ok := gotEnd.Equal(want) && gotNext.Equal(next) && gotMode == mode && gotTZ == tz && gotDS == ds && gotDE == de && gotClip == clip && gotWeek == week
	fmt.Printf("APPLY %d old=%s new=%s next=%s verified=%t\n", id, end.UTC().Format(time.RFC3339), gotEnd.UTC().Format(time.RFC3339), gotNext.UTC().Format(time.RFC3339), ok)
}
