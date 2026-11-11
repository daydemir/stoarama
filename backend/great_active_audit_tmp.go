package main

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	ctx := context.Background()
	p, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		panic(err)
	}
	defer p.Close()
	rows, err := p.Query(ctx, `with graded as(
		select h.recording_id,h.job_id hack,(h.window_start_at at time zone r.cron_timezone)::date local_day,
		case when h.metric_version is null or h.metric_version<>2 or h.gap_over_30s_count is null or h.gap_over_5m_count is null then 'UNKNOWN'
		when h.overlap_count=0 and h.coverage_pct>=99 and h.largest_gap_seconds<=120 then 'A'
		when h.overlap_count=0 and h.coverage_pct>=95 and h.largest_gap_seconds<=900 and h.gap_over_5m_count<=1 and h.gap_over_30s_count<=6 then 'B'
		when h.overlap_count=0 and h.coverage_pct>=90 and h.largest_gap_seconds<=1800 and h.gap_over_5m_count<=2 then 'C'
		when h.clip_count=0 or h.coverage_pct=0 then 'F' when h.coverage_pct<80 then 'E' else 'D' end grade
		from recording_window_health h join recordings r on r.id=h.recording_id where r.account_id=47 and r.mode='continuous'),
	w as(select recording_id,local_day,lead(local_day,13)over(partition by recording_id order by local_day,hack) end_day,
		count(*)over(partition by recording_id order by local_day,hack rows between current row and 13 following)n,
		count(*)filter(where grade not in('A','B','C'))over(partition by recording_id order by local_day,hack rows between current row and 13 following)bad
		from graded),q as(select distinct recording_id from w where n=14 and end_day=local_day+13 and bad=0)
	select jsonb_build_object('id',r.id,'timezone',r.cron_timezone,'start',r.daily_window_start,'close',r.daily_window_end,
		'next_fire',r.next_fire_at,'end_at',r.end_at,'relay_group',r.preferred_relay_group_id,
		'execution_class',a.execution_class,
		'pending',(select count(*) from recording_jobs j where j.recording_id=r.id and j.status='pending'),
		'leased',(select count(*) from recording_jobs j where j.recording_id=r.id and j.status='leased' and j.lease_expires_at>now()),
		'handoff',(select count(*) from recording_jobs j where j.recording_id=r.id and j.handoff_until>now()),
		'intents',(select count(*) from recording_upload_intents u where u.recording_id=r.id and u.status='pending' and u.expires_at>now()))
	from recordings r join q on q.recording_id=r.id left join recording_assignments a on a.stream_id=r.stream_id
	where r.status='active' and r.id<>all(array[401,406,409,417,427]::bigint[]) order by r.id`)
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	for rows.Next() {
		var s string
		if err = rows.Scan(&s); err != nil {
			panic(err)
		}
		fmt.Println(s)
	}
	if err = rows.Err(); err != nil {
		panic(err)
	}
}
