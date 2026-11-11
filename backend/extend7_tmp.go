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

type rec struct {
	id, sid                           int64
	mode, tz, ds, de                  string
	clip, week                        int
	start, end, next                  time.Time
	pending, leased, handoff, intents int
}

func clock(s string) (int, int) {
	var h, m int
	fmt.Sscanf(s, "%d:%d", &h, &m)
	return h, m
}

func windows(r rec, end, now time.Time) int {
	loc, _ := time.LoadLocation(r.tz)
	h, m := clock(r.de)
	from := now.In(loc).AddDate(0, 0, -1)
	to := end.In(loc)
	n := 0
	for d := time.Date(from.Year(), from.Month(), from.Day(), 0, 0, 0, 0, loc); !d.After(to); d = d.AddDate(0, 0, 1) {
		if r.week&(1<<((int(d.Weekday())+6)%7)) == 0 {
			continue
		}
		close := time.Date(d.Year(), d.Month(), d.Day(), h, m, 0, 0, loc)
		if close.After(now) && !close.After(end) {
			n++
		}
	}
	return n
}

func main() {
	ctx := context.Background()
	db, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		panic(err)
	}
	defer db.Close()

	now := time.Now()
	rows, err := db.Query(ctx, `
		SELECT r.id,r.stream_id,r.mode,r.cron_timezone,
		       r.daily_window_start::text,r.daily_window_end::text,
		       r.clip_duration_sec,r.active_weekdays,r.start_at,r.end_at,r.next_fire_at,
		       count(j.id) FILTER (WHERE j.status='pending'),
		       count(j.id) FILTER (WHERE j.status='leased' AND j.lease_expires_at>now()),
		       count(j.id) FILTER (WHERE j.handoff_until>now()),
		       (SELECT count(*) FROM recording_upload_intents u
		        WHERE u.recording_id=r.id AND u.status='pending' AND u.expires_at>now())
		FROM recordings r
		LEFT JOIN recording_jobs j ON j.recording_id=r.id
		WHERE r.account_id=47 AND r.status='active' AND r.mode='continuous' AND r.end_at IS NOT NULL
		GROUP BY r.id ORDER BY r.id`)
	if err != nil {
		panic(err)
	}
	var rs []rec
	for rows.Next() {
		var r rec
		if err := rows.Scan(&r.id, &r.sid, &r.mode, &r.tz, &r.ds, &r.de, &r.clip, &r.week, &r.start, &r.end, &r.next, &r.pending, &r.leased, &r.handoff, &r.intents); err != nil {
			panic(err)
		}
		rs = append(rs, r)
	}
	if err := rows.Err(); err != nil {
		panic(err)
	}
	rows.Close()

	base := strings.TrimRight(os.Getenv("BACKEND_API_URL"), "/")
	token := os.Getenv("MITSCL_API_TOKEN")
	for _, r := range rs {
		runway := windows(r, r.end, now)
		delta := 0
		candidate := r.end
		for runway < 7 && delta < 14 {
			delta++
			loc, _ := time.LoadLocation(r.tz)
			candidate = r.end.In(loc).AddDate(0, 0, delta)
			runway = windows(r, candidate, now)
		}
		if delta == 0 {
			fmt.Printf("SKIP %d/%d runway=%d end=%s next=%s\n", r.id, r.sid, runway, r.end.UTC().Format(time.RFC3339), r.next.UTC().Format(time.RFC3339))
			continue
		}
		if err := db.QueryRow(ctx, `SELECT count(*) FILTER (WHERE status='pending'), count(*) FILTER (WHERE status='leased' AND lease_expires_at>now()), count(*) FILTER (WHERE handoff_until>now()), (SELECT count(*) FROM recording_upload_intents WHERE recording_id=$1 AND status='pending' AND expires_at>now()) FROM recording_jobs WHERE recording_id=$1`, r.id).Scan(&r.pending, &r.leased, &r.handoff, &r.intents); err != nil {
			fmt.Printf("ERROR %d regate=%v\n", r.id, err)
			continue
		}
		if r.pending+r.leased+r.handoff+r.intents > 0 {
			fmt.Printf("HOLD %d/%d delta=%d runway=%d gates=p%d/l%d/h%d/i%d end=%s next=%s\n", r.id, r.sid, delta, windows(r, r.end, now), r.pending, r.leased, r.handoff, r.intents, r.end.UTC().Format(time.RFC3339), r.next.UTC().Format(time.RFC3339))
			continue
		}

		body := map[string]any{
			"mode": r.mode, "cron_timezone": r.tz, "clip_duration_sec": r.clip,
			"daily_window_start": r.ds[:5], "daily_window_end": r.de[:5],
			"start_at": r.start.UTC().Format(time.RFC3339Nano),
			"end_at":   candidate.UTC().Format(time.RFC3339Nano),
		}
		raw, _ := json.Marshal(body)
		req, _ := http.NewRequestWithContext(ctx, http.MethodPatch, fmt.Sprintf("%s/api/v1/account/recordings/%d/schedule", base, r.id), bytes.NewReader(raw))
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			fmt.Printf("ERROR %d request=%v\n", r.id, err)
			continue
		}
		out, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if resp.StatusCode/100 != 2 {
			fmt.Printf("ERROR %d status=%d body=%s\n", r.id, resp.StatusCode, string(out))
			continue
		}

		var endRead, nextRead time.Time
		var mode, tz, ds, de string
		var clip, week int
		if err := db.QueryRow(ctx, `SELECT end_at,next_fire_at,mode,cron_timezone,daily_window_start::text,daily_window_end::text,clip_duration_sec,active_weekdays FROM recordings WHERE id=$1`, r.id).Scan(&endRead, &nextRead, &mode, &tz, &ds, &de, &clip, &week); err != nil {
			fmt.Printf("ERROR %d readback=%v\n", r.id, err)
			continue
		}
		ok := endRead.Equal(candidate) && nextRead.Equal(r.next) && mode == r.mode && tz == r.tz && ds == r.ds && de == r.de && clip == r.clip && week == r.week
		fmt.Printf("APPLY %d/%d delta=%d old_end=%s new_end=%s runway=%d next=%s verified=%t\n", r.id, r.sid, delta, r.end.UTC().Format(time.RFC3339), endRead.UTC().Format(time.RFC3339), windows(r, endRead, time.Now()), nextRead.UTC().Format(time.RFC3339), ok)
	}
}
