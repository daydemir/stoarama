package main

import (
	"context"
	"encoding/json"
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
	rows, err := p.Query(ctx, `
		select r.id,r.status,r.next_fire_at,r.end_at,r.stream_url=s.source_url,
		 (select count(*) from generate_series((now() at time zone r.cron_timezone)::date,(r.end_at at time zone r.cron_timezone)::date,interval '1 day') d
		  where (r.active_weekdays&(1<<(extract(isodow from d)::int-1)))<>0
		    and ((d::date+r.daily_window_end) at time zone r.cron_timezone)>now()
		    and ((d::date+r.daily_window_end) at time zone r.cron_timezone)<=r.end_at) runway,
		 exists(select 1 from pg_locks where locktype='advisory' and granted and ((classid::bigint<<32)|(objid::bigint&4294967295))=$2) lock_held,
		j.status,j.lease_owner,j.lease_expires_at,j.handoff_until,j.attempt_count,j.error_text,
		 (select count(*) from recording_clips c where c.recording_job_id=j.id),
		 (select extract(epoch from now()-max(c.clip_end_at)) from recording_clips c where c.recording_job_id=j.id),
		 (select count(*) from recording_upload_intents u where u.recording_job_id=j.id and u.status in('pending','uploading'))
		from recordings r join streams s on s.id=r.stream_id
		left join lateral(select * from recording_jobs x where x.recording_id=r.id order by fire_at desc limit 1)j on true
		where r.id=any($1) order by r.id`, []int64{406, 408, 409, 417, 427, 444}, int64(0x4650365741544348))
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	for rows.Next() {
		var id, runway, clips, intents int64
		var status string
		var fire, end any
		var held, binding bool
		var jobStatus, owner, jobError *string
		var attempts *int
		var lease, handoff, lag any
		if err := rows.Scan(&id, &status, &fire, &end, &binding, &runway, &held, &jobStatus, &owner, &lease, &handoff, &attempts, &jobError, &clips, &lag, &intents); err != nil {
			panic(err)
		}
		b, _ := json.Marshal(map[string]any{"id": id, "status": status, "next_fire": fire, "end_at": end, "binding_equal": binding, "runway": runway, "watch_lock": held, "job_status": jobStatus, "lease_owner": owner, "lease_expires": lease, "handoff_until": handoff, "attempts": attempts, "job_error": jobError, "clips": clips, "media_lag_seconds": lag, "live_intents": intents})
		fmt.Println(string(b))
	}
	var qualified int
	var qualifiedIDs []int64
	err = p.QueryRow(ctx, `with cohort(id) as (select unnest(array[335,337,348,349,350,355,377,379,380,381,382,383,384,385,401,402,403,404,406,408,409,410,411,412,413,414,416,417,418,419,420,421,422,423,424,425,427,428,429,430,431,432,433,434,435,436,437,439,440,441,442,444,445]::bigint[])),
	graded as(select h.recording_id,(h.window_start_at at time zone r.cron_timezone)::date local_day,
	 case when h.metric_version is null or h.metric_version<>2 or h.gap_over_30s_count is null or h.gap_over_5m_count is null then 'UNKNOWN'
	 when h.overlap_count=0 and h.coverage_pct>=99 and h.largest_gap_seconds<=120 then 'A'
	 when h.overlap_count=0 and h.coverage_pct>=95 and h.largest_gap_seconds<=900 and h.gap_over_5m_count<=1 and h.gap_over_30s_count<=6 then 'B'
	 when h.overlap_count=0 and h.coverage_pct>=90 and h.largest_gap_seconds<=1800 and h.gap_over_5m_count<=2 then 'C'
	 when h.clip_count=0 or h.coverage_pct=0 then 'F' when h.coverage_pct<80 then 'E' else 'D' end grade
	 from recording_window_health h join recordings r on r.id=h.recording_id join cohort c on c.id=h.recording_id),
	good as(select *,local_day-(row_number() over(partition by recording_id order by local_day))::int grp from graded where grade not in('F','UNKNOWN')),
	q as(select recording_id from good group by recording_id,grp having count(*)>=14)
	select count(distinct recording_id),array_agg(distinct recording_id order by recording_id) from q`).Scan(&qualified, &qualifiedIDs)
	if err != nil {
		panic(err)
	}
	b, _ := json.Marshal(map[string]any{"persisted_fineplus_qualified": qualified, "qualified_ids": qualifiedIDs})
	fmt.Println(string(b))
	var goodCount int
	var goodIDs []int64
	err = p.QueryRow(ctx, `with graded as(select h.recording_id,h.job_id hack,(h.window_start_at at time zone r.cron_timezone)::date local_day,
	 case when h.metric_version is null or h.metric_version<>2 or h.gap_over_30s_count is null or h.gap_over_5m_count is null then 'UNKNOWN'
	 when h.overlap_count=0 and h.coverage_pct>=99 and h.largest_gap_seconds<=120 then 'A'
	 when h.overlap_count=0 and h.coverage_pct>=95 and h.largest_gap_seconds<=900 and h.gap_over_5m_count<=1 and h.gap_over_30s_count<=6 then 'B'
	 when h.overlap_count=0 and h.coverage_pct>=90 and h.largest_gap_seconds<=1800 and h.gap_over_5m_count<=2 then 'C'
	 when h.clip_count=0 or h.coverage_pct=0 then 'F' when h.coverage_pct<80 then 'E' else 'D' end grade
	 from recording_window_health h join recordings r on r.id=h.recording_id where r.account_id=47 and r.mode='continuous'),
	w as(select recording_id,local_day,lead(local_day,13)over(partition by recording_id order by local_day,hack) end_day,
	 count(*)over(partition by recording_id order by local_day,hack rows between current row and 13 following)n,
	 count(*)filter(where grade in('F','UNKNOWN'))over(partition by recording_id order by local_day,hack rows between current row and 13 following)bad,
	 count(*)filter(where grade='E')over(partition by recording_id order by local_day,hack rows between current row and 13 following)e_count
	 from graded),q as(select distinct recording_id from w where n=14 and end_day=local_day+13 and bad=0 and e_count<=2)
	 select count(*),coalesce(array_agg(recording_id order by recording_id),'{}')from q`).Scan(&goodCount, &goodIDs)
	if err != nil {
		panic(err)
	}
	b, _ = json.Marshal(map[string]any{"persisted_goodplus_completed": goodCount, "goodplus_ids": goodIDs})
	fmt.Println(string(b))
}
