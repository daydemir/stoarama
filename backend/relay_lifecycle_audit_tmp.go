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
	rows, e := p.Query(c, `with ids(id)as(select unnest(array[410,411,412,414,432,433,434,435,436,442,445]::bigint[])),j as(select distinct on(j.recording_id)j.* from recording_jobs j join ids on ids.id=j.recording_id order by j.recording_id,j.fire_at desc),x as(select c.recording_id,c.resolved_url,c.clip_start_at,c.clip_end_at,extract(epoch from(c.clip_end_at-c.clip_start_at))duration,extract(epoch from(c.clip_start_at-lag(c.clip_end_at)over(partition by c.recording_id order by c.clip_start_at,c.id)))gap from recording_clips c join j on j.id=c.recording_job_id)select jsonb_build_object('recording',r.id,'provider',s.provider,'source_kind',r.source_kind,'capture_type',s.capture_type,'resolved_family',case when bool_or(x.resolved_url like '%googlevideo.com/%hls%')then'youtube_googlevideo_hls' when bool_or(x.resolved_url like '%.m3u8%')then'hls_manifest' else'other' end,'owner',j.lease_owner,'attempt',j.attempt_count,'job_error',j.error_text,'clips',count(x.*),'duration_min',min(x.duration),'duration_avg',avg(x.duration),'duration_max',max(x.duration),'gap_count',count(*)filter(where x.gap>0),'gap_avg',avg(x.gap)filter(where x.gap>0),'gap_max',max(x.gap),'node_log_categories',coalesce(n.capabilities_jsonb->'recent_log_categories','[]'::jsonb),'node_offline_open',coalesce(jsonb_array_length(n.capabilities_jsonb#>'{offline_diagnostics,events}'),0)) from ids join recordings r on r.id=ids.id join streams s on s.id=r.stream_id join j on j.recording_id=r.id left join x on x.recording_id=r.id left join nodes n on j.lease_owner='node:'||n.id::text group by r.id,s.provider,s.capture_type,j.lease_owner,j.attempt_count,j.error_text,n.capabilities_jsonb order by r.id`)
	if e != nil {
		panic(e)
	}
	for rows.Next() {
		var b []byte
		if e = rows.Scan(&b); e != nil {
			panic(e)
		}
		var v any
		json.Unmarshal(b, &v)
		o, _ := json.Marshal(v)
		fmt.Println(string(o))
	}
}
