package main

import (
    "context"
    "fmt"
    "os"
    "time"

    "github.com/jackc/pgx/v5/pgxpool"
)

func main() {
    ctx := context.Background()
    p, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
    if err != nil { panic(err) }
    defer p.Close()
    rows, err := p.Query(ctx, `
      SELECT h.window_start_at, h.window_end_at, h.metric_version,
             h.coverage_pct, h.largest_gap_seconds, h.gap_over_30s_count,
             h.gap_over_5m_count, h.overlap_count, h.clip_count,
             (h.window_start_at AT TIME ZONE r.cron_timezone)::date
      FROM recording_window_health h
      JOIN recordings r ON r.id=h.recording_id
      WHERE h.recording_id=420
      ORDER BY h.window_start_at
    `)
    if err != nil { panic(err) }
    defer rows.Close()
    n := 0
    for rows.Next() {
        var start, end time.Time
        var version, over30, over5, overlaps *int
        var coverage, gap *float64
        var clips int
        var local time.Time
        if err := rows.Scan(&start, &end, &version, &coverage, &gap, &over30, &over5, &overlaps, &clips, &local); err != nil { panic(err) }
        vi, ci, gi, o30i, o5i, oi := "null", "null", "null", "null", "null", "null"
        if version != nil { vi = fmt.Sprint(*version) }
        if coverage != nil { ci = fmt.Sprintf("%.4f", *coverage) }
        if gap != nil { gi = fmt.Sprintf("%.1f", *gap) }
        if over30 != nil { o30i = fmt.Sprint(*over30) }
        if over5 != nil { o5i = fmt.Sprint(*over5) }
        if overlaps != nil { oi = fmt.Sprint(*overlaps) }
        fmt.Printf("%s start=%s end=%s version=%s coverage=%s gap=%s over30=%s over5=%s overlaps=%s clips=%d\n", local.Format("2006-01-02"), start.UTC().Format(time.RFC3339), end.UTC().Format(time.RFC3339), vi, ci, gi, o30i, o5i, oi, clips)
        n++
    }
    if err := rows.Err(); err != nil { panic(err) }
    var accountID int64
    if err := p.QueryRow(ctx, `SELECT account_id FROM recordings WHERE id=420`).Scan(&accountID); err != nil { panic(err) }
    var summaries int
    if err := p.QueryRow(ctx, `SELECT count(*) FROM recording_health_summaries WHERE recording_id=420`).Scan(&summaries); err != nil { panic(err) }
    fmt.Printf("ACCOUNT_ID=%d WINDOW_ROWS=%d SUMMARY_ROWS=%d\n", accountID, n, summaries)
}
