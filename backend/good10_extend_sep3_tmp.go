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
	for _, id := range []int{410, 411, 412, 414, 432, 433, 434, 435, 436, 442} {
		var mode, tz, ds, de, grade string
		var clip, week, pending, leased, handoff, intents int
		var start, end, next time.Time
		err = p.QueryRow(ctx, `select r.mode,r.cron_timezone,r.daily_window_start::text,r.daily_window_end::text,r.clip_duration_sec,r.active_weekdays,r.start_at,r.end_at,r.next_fire_at,
			(select count(*) from recording_jobs j where j.recording_id=r.id and j.status='pending'),
			(select count(*) from recording_jobs j where j.recording_id=r.id and j.status='leased' and j.lease_expires_at>now()),
			(select count(*) from recording_jobs j where j.recording_id=r.id and j.handoff_until>now()),
			(select count(*) from recording_upload_intents u where u.recording_id=r.id and u.status='pending' and u.expires_at>now()),
			coalesce((select case when h.metric_version is null or h.metric_version<>2 or h.gap_over_30s_count is null or h.gap_over_5m_count is null then 'UNKNOWN'
				when h.overlap_count=0 and h.coverage_pct>=99 and h.largest_gap_seconds<=120 then 'A'
				when h.overlap_count=0 and h.coverage_pct>=95 and h.largest_gap_seconds<=900 and h.gap_over_5m_count<=1 and h.gap_over_30s_count<=6 then 'B'
				when h.overlap_count=0 and h.coverage_pct>=90 and h.largest_gap_seconds<=1800 and h.gap_over_5m_count<=2 then 'C'
				when h.clip_count=0 or h.coverage_pct=0 then 'F' when h.coverage_pct<80 then 'E' else 'D' end
			from recording_window_health h where h.recording_id=r.id and (h.window_start_at at time zone r.cron_timezone)::date=date '2026-08-22' order by h.calculated_at desc limit 1),'MISSING')
			from recordings r where r.account_id=47 and r.id=$1 and r.status='active'`, id).Scan(&mode, &tz, &ds, &de, &clip, &week, &start, &end, &next, &pending, &leased, &handoff, &intents, &grade)
		if err != nil {
			fmt.Printf("ERROR %d load=%v\n", id, err)
			continue
		}
		if pending+leased+handoff+intents != 0 {
			fmt.Printf("HOLD %d grade=%s p%d/l%d/h%d/i%d\n", id, grade, pending, leased, handoff, intents)
			continue
		}
		if grade != "E" {
			fmt.Printf("HOLD %d grade=%s requires=E\n", id, grade)
			continue
		}
		loc, err := time.LoadLocation(tz)
		if err != nil {
			panic(err)
		}
		want := time.Date(2026, 9, 4, 0, 0, 0, 0, loc)
		if !end.Before(want) {
			fmt.Printf("SKIP %d grade=%s end=%s\n", id, grade, end.UTC().Format(time.RFC3339))
			continue
		}
		body := map[string]any{"mode": mode, "cron_timezone": tz, "daily_window_start": ds[:5], "daily_window_end": de[:5], "clip_duration_sec": clip, "start_at": start.UTC().Format(time.RFC3339Nano), "end_at": want.UTC().Format(time.RFC3339Nano)}
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
		var gotEnd, gotNext time.Time
		var gotMode, gotTZ, gotDS, gotDE string
		var gotClip, gotWeek int
		err = p.QueryRow(ctx, `select end_at,next_fire_at,mode,cron_timezone,daily_window_start::text,daily_window_end::text,clip_duration_sec,active_weekdays from recordings where id=$1`, id).Scan(&gotEnd, &gotNext, &gotMode, &gotTZ, &gotDS, &gotDE, &gotClip, &gotWeek)
		ok := err == nil && gotEnd.Equal(want) && gotNext.Equal(next) && gotMode == mode && gotTZ == tz && gotDS == ds && gotDE == de && gotClip == clip && gotWeek == week
		fmt.Printf("APPLY %d grade=%s old=%s new=%s next=%s verified=%t\n", id, grade, end.UTC().Format(time.RFC3339), gotEnd.UTC().Format(time.RFC3339), gotNext.UTC().Format(time.RFC3339), ok)
	}
}
