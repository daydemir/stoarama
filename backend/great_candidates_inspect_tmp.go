package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func grade(coverage *float64, largest *float64, over30, over5, overlaps *int, version *int, clips int) string {
	if version == nil || *version != 2 || coverage == nil || largest == nil || over30 == nil || over5 == nil || overlaps == nil { return "?" }
	if *overlaps == 0 && *coverage >= 99 && *largest <= 120 { return "A" }
	if *overlaps == 0 && *coverage >= 95 && *largest <= 900 && *over5 <= 1 && *over30 <= 6 { return "B" }
	if *overlaps == 0 && *coverage >= 90 && *largest <= 1800 && *over5 <= 2 { return "C" }
	if clips == 0 || *coverage == 0 { return "F" }
	if *coverage < 80 { return "E" }
	return "D"
}

func main() {
	ctx := context.Background()
	db, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL")); if err != nil { panic(err) }; defer db.Close()
	ids := []int64{337,339,355,385,401,405,406,407,409,417,418,419,420,427,428,431}
	rows, err := db.Query(ctx, `SELECT j.recording_id,j.fire_at,h.coverage_pct,h.largest_gap_seconds,h.gap_over_30s_count,h.gap_over_5m_count,h.overlap_count,h.metric_version,COALESCE(h.clip_count,0) FROM recording_jobs j LEFT JOIN recording_window_health h ON h.job_id=j.id WHERE j.recording_id=ANY($1::bigint[]) AND j.kind='continuous_window' AND j.fire_at >= '2026-08-01T00:00:00Z' ORDER BY j.recording_id,j.fire_at`, ids)
	if err != nil { panic(err) }; defer rows.Close()
	var current int64 = -1; var seq string
	for rows.Next() {
		var id int64; var fire time.Time; var coverage, largest *float64; var over30, over5, overlaps, version *int; var clips int
		if err := rows.Scan(&id,&fire,&coverage,&largest,&over30,&over5,&overlaps,&version,&clips); err != nil { panic(err) }
		if id != current { if current >= 0 { fmt.Printf("\n") }; current=id; seq=""; fmt.Printf("%d\t", id) }
		seq += grade(coverage,largest,over30,over5,overlaps,version,clips)
		fmt.Printf("%s:%s ", fire.UTC().Format("01/02"), grade(coverage,largest,over30,over5,overlaps,version,clips))
	}
	if err := rows.Err(); err != nil { panic(err) }
	fmt.Printf("\n")
}
