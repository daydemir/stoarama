package main

import (
	"context"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type rec struct {
	id       int64
	status   string
	tz       string
	end      time.Time
	days     map[string]string
	complete map[string]bool
}

var plannedGreatIDs = map[int64]bool{339: true, 348: true, 349: true, 350: true, 381: true, 382: true, 385: true, 402: true, 403: true, 405: true, 407: true, 408: true, 410: true, 411: true, 412: true, 414: true, 418: true, 419: true, 420: true, 428: true, 431: true, 432: true, 433: true, 434: true, 435: true, 436: true, 442: true}

func qualifies(grades []string, tier string) bool {
	if len(grades) != 14 {
		return false
	}
	es := 0
	for _, g := range grades {
		if g == "U" || g == "F" {
			return false
		}
		if tier == "great" && g != "A" && g != "B" && g != "C" {
			return false
		}
		if g == "E" {
			es++
		}
	}
	return tier != "good" || es <= 2
}

func projected(r *rec, tier string, through time.Time) bool {
	if r.complete[tier] {
		return true
	}
	if r.status != "active" {
		return false
	}
	loc, err := time.LoadLocation(r.tz)
	if err != nil {
		return false
	}
	endDay := r.end.In(loc)
	throughDay := through.In(loc)
	if endDay.Before(throughDay) {
		throughDay = endDay
	}
	keys := make([]string, 0, len(r.days))
	for k := range r.days {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	grades := make(map[string]string, len(r.days)+40)
	for k, v := range r.days {
		grades[k] = v
	}
	start := time.Now().In(loc)
	today := time.Date(start.Year(), start.Month(), start.Day(), 0, 0, 0, 0, loc)
	if len(keys) > 0 {
		if d, e := time.ParseInLocation("2006-01-02", keys[len(keys)-1], loc); e == nil {
			start = d.AddDate(0, 0, 1)
		}
	}
	if start.Before(today) {
		start = today
	}
	for d := start; !d.After(throughDay); d = d.AddDate(0, 0, 1) {
		grades[d.Format("2006-01-02")] = "A"
	}
	all := make([]string, 0, len(grades))
	for k := range grades {
		all = append(all, k)
	}
	sort.Strings(all)
	for i := 0; i+13 < len(all); i++ {
		a, _ := time.ParseInLocation("2006-01-02", all[i], loc)
		b, _ := time.ParseInLocation("2006-01-02", all[i+13], loc)
		if int(b.Sub(a).Hours()/24) != 13 {
			continue
		}
		window := make([]string, 14)
		for j := 0; j < 14; j++ {
			window[j] = grades[all[i+j]]
		}
		if qualifies(window, tier) {
			return true
		}
	}
	return false
}

func main() {
	ctx := context.Background()
	db, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		panic(err)
	}
	defer db.Close()
	rows, err := db.Query(ctx, `WITH graded AS (
		SELECT DISTINCT ON (h.recording_id,(h.window_start_at AT TIME ZONE r.cron_timezone)::date)
		h.recording_id,(h.window_start_at AT TIME ZONE r.cron_timezone)::date d,
		CASE WHEN h.metric_version IS NULL OR h.metric_version<>2 OR h.gap_over_30s_count IS NULL OR h.gap_over_5m_count IS NULL THEN 'U'
		WHEN h.overlap_count=0 AND h.coverage_pct>=99 AND h.largest_gap_seconds<=120 THEN 'A'
		WHEN h.overlap_count=0 AND h.coverage_pct>=95 AND h.largest_gap_seconds<=900 AND h.gap_over_5m_count<=1 AND h.gap_over_30s_count<=6 THEN 'B'
		WHEN h.overlap_count=0 AND h.coverage_pct>=90 AND h.largest_gap_seconds<=1800 AND h.gap_over_5m_count<=2 THEN 'C'
		WHEN h.clip_count=0 OR h.coverage_pct=0 THEN 'F' WHEN h.coverage_pct<80 THEN 'E' ELSE 'D' END g
		FROM recording_window_health h JOIN recordings r ON r.id=h.recording_id WHERE r.account_id=47 AND r.mode='continuous'
		ORDER BY h.recording_id,(h.window_start_at AT TIME ZONE r.cron_timezone)::date,h.job_id DESC)
		SELECT r.id,r.status,r.cron_timezone,r.end_at,g.d,g.g FROM recordings r LEFT JOIN graded g ON g.recording_id=r.id
		WHERE r.account_id=47 AND r.mode='continuous' ORDER BY r.id,g.d`)
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	recs := map[int64]*rec{}
	for rows.Next() {
		var id int64
		var status, tz string
		var end *time.Time
		var d *time.Time
		var g *string
		if err := rows.Scan(&id, &status, &tz, &end, &d, &g); err != nil {
			panic(err)
		}
		x := recs[id]
		if x == nil {
			endValue := time.Time{}
			if end != nil {
				endValue = *end
			}
			x = &rec{id: id, status: status, tz: tz, end: endValue, days: map[string]string{}, complete: map[string]bool{}}
			recs[id] = x
		}
		if d != nil && g != nil {
			x.days[d.Format("2006-01-02")] = *g
		}
	}
	if err := rows.Err(); err != nil {
		panic(err)
	}
	for _, r := range recs {
		keys := make([]string, 0, len(r.days))
		for k := range r.days {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for i := 0; i+13 < len(keys); i++ {
			loc, _ := time.LoadLocation(r.tz)
			a, _ := time.ParseInLocation("2006-01-02", keys[i], loc)
			b, _ := time.ParseInLocation("2006-01-02", keys[i+13], loc)
			if int(b.Sub(a).Hours()/24) != 13 {
				continue
			}
			w := make([]string, 14)
			for j := 0; j < 14; j++ {
				w[j] = r.days[keys[i+j]]
			}
			for _, tier := range []string{"good", "great"} {
				if qualifies(w, tier) {
					r.complete[tier] = true
				}
			}
		}
	}
	fmt.Println("ASSUMPTION measured days preserved; unmeasured scheduled dates grade A")
	for d := time.Date(2026, 9, 8, 23, 59, 0, 0, time.UTC); !d.After(time.Date(2026, 9, 30, 23, 59, 0, 0, time.UTC)); d = d.AddDate(0, 0, 1) {
		good, great, plannedGood, plannedGreat := 0, 0, 0, 0
		for _, r := range recs {
			if projected(r, "good", d) {
				good++
			}
			if projected(r, "great", d) {
				great++
			}
			copy := *r
			if plannedGreatIDs[r.id] {
				loc, _ := time.LoadLocation(r.tz)
				copy.end = time.Date(2026, 9, 23, 23, 59, 0, 0, loc)
			}
			if projected(&copy, "good", d) {
				plannedGood++
			}
			if projected(&copy, "great", d) {
				plannedGreat++
			}
		}
		fmt.Printf("DATE %s current_schedule good=%d great=%d planned good=%d great=%d\n", d.Format("2006-01-02"), good, great, plannedGood, plannedGreat)
	}
	for _, tier := range []string{"good", "great"} {
		fmt.Printf("CANDIDATES %s\n", tier)
		ids := make([]int, 0)
		for id, r := range recs {
			if r.status == "active" && !r.complete[tier] {
				ids = append(ids, int(id))
			}
		}
		sort.Ints(ids)
		for _, n := range ids {
			r := recs[int64(n)]
			keys := make([]string, 0, len(r.days))
			aCount := 0
			for k, g := range r.days {
				keys = append(keys, k)
				if g == "A" {
					aCount++
				}
			}
			sort.Strings(keys)
			latestGrade := "none"
			trailingABC := 0
			if len(keys) > 0 {
				latestGrade = r.days[keys[len(keys)-1]]
				loc, _ := time.LoadLocation(r.tz)
				for i := len(keys) - 1; i >= 0; i-- {
					if r.days[keys[i]] != "A" && r.days[keys[i]] != "B" && r.days[keys[i]] != "C" {
						break
					}
					if i < len(keys)-1 {
						d1, _ := time.ParseInLocation("2006-01-02", keys[i], loc)
						d2, _ := time.ParseInLocation("2006-01-02", keys[i+1], loc)
						if int(d2.Sub(d1).Hours()/24) != 1 {
							break
						}
					}
					trailingABC++
				}
			}
			earliest := "none"
			for d := time.Date(2026, 9, 8, 23, 59, 0, 0, time.UTC); !d.After(time.Date(2026, 9, 30, 23, 59, 0, 0, time.UTC)); d = d.AddDate(0, 0, 1) {
				copy := *r
				copy.end = time.Date(2026, 9, 30, 23, 59, 0, 0, time.UTC)
				if projected(&copy, tier, d) {
					earliest = d.Format("2006-01-02")
					break
				}
			}
			fmt.Printf("%d earliest_if_A=%s schedule_end=%s A_days=%d latest=%s trailing_ABC=%d\n", n, earliest, r.end.Format("2006-01-02"), aCount, latestGrade, trailingABC)
		}
	}
}
