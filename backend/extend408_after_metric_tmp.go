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
	db, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		panic(err)
	}
	defer db.Close()

	const id int64 = 408
	oldEnd := time.Date(2026, 8, 25, 22, 0, 0, 0, time.UTC)
	newEnd := time.Date(2026, 9, 7, 22, 0, 0, 0, time.UTC)
	var status, mode, tz, dailyStart, dailyEnd string
	var clip, weekdays int
	var startAt, endAt time.Time
	var nextFire *time.Time
	err = db.QueryRow(ctx, `SELECT status,mode,cron_timezone,daily_window_start::text,daily_window_end::text,
		clip_duration_sec,active_weekdays,start_at,end_at,next_fire_at
		FROM recordings WHERE id=$1 AND account_id=47`, id).Scan(
		&status, &mode, &tz, &dailyStart, &dailyEnd, &clip, &weekdays, &startAt, &endAt, &nextFire)
	if err != nil {
		panic(err)
	}
	if status != "active" || mode != "continuous" || tz != "Africa/Johannesburg" || !endAt.Equal(oldEnd) || nextFire != nil {
		fmt.Printf("HOLD catalog status=%s mode=%s timezone=%s end=%s next=%v\n", status, mode, tz, endAt.UTC().Format(time.RFC3339), nextFire)
		return
	}

	var jobID int64
	var jobStatus, grade string
	var metricVersion, pending, leased, handoff, intents int
	err = db.QueryRow(ctx, `WITH latest AS (
		SELECT * FROM recording_jobs WHERE recording_id=$1 AND fire_at='2026-08-25T06:00:00Z'::timestamptz ORDER BY id DESC LIMIT 1
	), health AS (
		SELECT h.*,CASE
			WHEN h.metric_version IS NULL OR h.metric_version<>2 OR h.gap_over_30s_count IS NULL OR h.gap_over_5m_count IS NULL THEN 'MISSING'
			WHEN h.overlap_count=0 AND h.coverage_pct>=99 AND h.largest_gap_seconds<=120 THEN 'A'
			WHEN h.overlap_count=0 AND h.coverage_pct>=95 AND h.largest_gap_seconds<=900 AND h.gap_over_5m_count<=1 AND h.gap_over_30s_count<=6 THEN 'B'
			WHEN h.overlap_count=0 AND h.coverage_pct>=90 AND h.largest_gap_seconds<=1800 AND h.gap_over_5m_count<=2 THEN 'C'
			WHEN h.clip_count=0 OR h.coverage_pct=0 THEN 'F'
			WHEN h.coverage_pct<80 THEN 'E' ELSE 'D' END grade
		FROM recording_window_health h JOIN latest j ON j.id=h.job_id
	)
	SELECT j.id,j.status,coalesce(h.metric_version,0),coalesce(h.grade,'MISSING'),
		(SELECT count(*) FROM recording_jobs q WHERE q.recording_id=$1 AND q.status='pending'),
		(SELECT count(*) FROM recording_jobs q WHERE q.recording_id=$1 AND q.status='leased' AND q.lease_expires_at>now()),
		(SELECT count(*) FROM recording_jobs q WHERE q.recording_id=$1 AND q.handoff_until>now()),
		(SELECT count(*) FROM recording_upload_intents u WHERE u.recording_id=$1 AND u.status='pending' AND u.expires_at>now())
	FROM latest j LEFT JOIN health h ON true`, id).Scan(&jobID, &jobStatus, &metricVersion, &grade, &pending, &leased, &handoff, &intents)
	if err != nil {
		panic(err)
	}
	if jobStatus != "done" || metricVersion != 2 || grade == "MISSING" || grade == "F" || pending+leased+handoff+intents != 0 {
		fmt.Printf("HOLD job=%d status=%s metric=%d grade=%s gates=p%d/l%d/h%d/i%d\n", jobID, jobStatus, metricVersion, grade, pending, leased, handoff, intents)
		return
	}

	body := map[string]any{
		"mode": mode, "cron_timezone": tz, "clip_duration_sec": clip,
		"daily_window_start": dailyStart[:5], "daily_window_end": dailyEnd[:5],
		"start_at": startAt.UTC().Format(time.RFC3339Nano), "end_at": newEnd.Format(time.RFC3339),
	}
	raw, _ := json.Marshal(body)
	base := strings.TrimRight(os.Getenv("BACKEND_API_URL"), "/")
	token := os.Getenv("MITSCL_API_TOKEN")
	if base == "" || token == "" {
		panic("missing BACKEND_API_URL or MITSCL_API_TOKEN")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPatch, fmt.Sprintf("%s/api/v1/account/recordings/%d/schedule", base, id), bytes.NewReader(raw))
	if err != nil {
		panic(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
	_ = resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		panic(fmt.Sprintf("schedule PATCH status=%d", resp.StatusCode))
	}

	var gotStatus, gotMode, gotTZ, gotStart, gotEnd string
	var gotClip, gotWeekdays, postPending, postLeased, postHandoff, postIntents int
	var gotEndAt, gotNext time.Time
	err = db.QueryRow(ctx, `SELECT status,mode,cron_timezone,daily_window_start::text,daily_window_end::text,
		clip_duration_sec,active_weekdays,end_at,next_fire_at,
		(SELECT count(*) FROM recording_jobs q WHERE q.recording_id=$1 AND q.status='pending'),
		(SELECT count(*) FROM recording_jobs q WHERE q.recording_id=$1 AND q.status='leased' AND q.lease_expires_at>now()),
		(SELECT count(*) FROM recording_jobs q WHERE q.recording_id=$1 AND q.handoff_until>now()),
		(SELECT count(*) FROM recording_upload_intents u WHERE u.recording_id=$1 AND u.status='pending' AND u.expires_at>now())
		FROM recordings WHERE id=$1 AND account_id=47`, id).Scan(
		&gotStatus, &gotMode, &gotTZ, &gotStart, &gotEnd, &gotClip, &gotWeekdays, &gotEndAt, &gotNext,
		&postPending, &postLeased, &postHandoff, &postIntents)
	if err != nil {
		panic(err)
	}
	verified := gotStatus == status && gotMode == mode && gotTZ == tz && gotStart == dailyStart && gotEnd == dailyEnd &&
		gotClip == clip && gotWeekdays == weekdays && gotEndAt.Equal(newEnd) && gotNext.Equal(time.Date(2026, 8, 26, 6, 0, 0, 0, time.UTC)) &&
		postPending+postLeased+postHandoff+postIntents == 0
	fmt.Printf("APPLY recording=%d grade=%s old_end=%s new_end=%s next=%s gates=p%d/l%d/h%d/i%d verified=%t\n",
		id, grade, oldEnd.Format(time.RFC3339), gotEndAt.UTC().Format(time.RFC3339), gotNext.UTC().Format(time.RFC3339),
		postPending, postLeased, postHandoff, postIntents, verified)
	if !verified {
		panic("post-PATCH readback verification failed")
	}
}
