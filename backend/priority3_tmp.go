package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"os"
)

func main() {
	c := context.Background()
	p, e := pgxpool.New(c, os.Getenv("DATABASE_URL"))
	if e != nil {
		panic(e)
	}
	defer p.Close()
	rows, e := p.Query(c, `SELECT jsonb_build_object('recording',r.id,'stream',r.stream_id,'status',r.status,'next_fire',r.next_fire_at,'end_at',r.end_at,'timezone',r.cron_timezone,'daily_start',r.daily_window_start,'daily_end',r.daily_window_end,'runway',(SELECT count(*) FROM generate_series((now() AT TIME ZONE r.cron_timezone)::date-1,(r.end_at AT TIME ZONE r.cron_timezone)::date,interval '1 day') d WHERE (r.active_weekdays&(1<<(extract(isodow FROM d)::int-1)))<>0 AND ((d::date+r.daily_window_end)AT TIME ZONE r.cron_timezone)>now() AND ((d::date+r.daily_window_end)AT TIME ZONE r.cron_timezone)<=r.end_at),'days',COALESCE((SELECT jsonb_agg(jsonb_build_object('local_date',(h.window_start_at AT TIME ZONE r.cron_timezone)::date,'grade',CASE WHEN h.metric_version IS NULL OR h.metric_version<>2 OR h.gap_over_30s_count IS NULL OR h.gap_over_5m_count IS NULL THEN 'UNKNOWN' WHEN h.overlap_count=0 AND h.coverage_pct>=99 AND h.largest_gap_seconds<=120 THEN 'A' WHEN h.overlap_count=0 AND h.coverage_pct>=95 AND h.largest_gap_seconds<=900 AND h.gap_over_5m_count<=1 AND h.gap_over_30s_count<=6 THEN 'B' WHEN h.overlap_count=0 AND h.coverage_pct>=90 AND h.largest_gap_seconds<=1800 AND h.gap_over_5m_count<=2 THEN 'C' WHEN h.clip_count=0 OR h.coverage_pct=0 THEN 'F' WHEN h.coverage_pct<80 THEN 'E' ELSE 'D' END,'coverage',round(h.coverage_pct::numeric,2),'largest_gap',round(h.largest_gap_seconds::numeric,1),'overlaps',h.overlap_count,'clips',h.clip_count) ORDER BY h.window_start_at) FROM recording_window_health h WHERE h.recording_id=r.id),'[]'::jsonb)) FROM recordings r WHERE r.account_id=47 AND r.mode='continuous' ORDER BY r.id`)
	if e != nil {
		panic(e)
	}
	defer rows.Close()
	for rows.Next() {
		var b []byte
		if e = rows.Scan(&b); e != nil {
			panic(e)
		}
		var x any
		_ = json.Unmarshal(b, &x)
		o, _ := json.Marshal(x)
		fmt.Println(string(o))
	}
}
