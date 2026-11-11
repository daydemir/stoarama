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
	rows, e := p.Query(c, `with j as(select id from recording_jobs where recording_id=408 order by fire_at desc limit 1),x as(select c.id,c.clip_start_at,c.clip_end_at,c.created_at,lag(c.clip_end_at)over(order by c.clip_start_at,c.id) prev_end from recording_clips c join j on j.id=c.recording_job_id),g as(select *,extract(epoch from clip_start_at-prev_end) gap_seconds from x where prev_end is not null)select jsonb_build_object('id',id,'prev_end',prev_end,'clip_start',clip_start_at,'clip_end',clip_end_at,'created_at',created_at,'gap_seconds',gap_seconds)from g order by gap_seconds desc nulls last limit 12`)
	if e != nil {
		panic(e)
	}
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
	var b []byte
	e = p.QueryRow(c, `with j as(select id,fire_at,window_end_at from recording_jobs where recording_id=408 order by fire_at desc limit 1),x as(select c.clip_start_at,c.clip_end_at,lag(c.clip_end_at)over(order by c.clip_start_at,c.id) prev_end from recording_clips c join j on j.id=c.recording_job_id),g as(select extract(epoch from clip_start_at-prev_end) gap_seconds from x where prev_end is not null)select jsonb_build_object('positive_gap_seconds',coalesce(sum(gap_seconds)filter(where gap_seconds>0),0),'largest_gap_seconds',coalesce(max(gap_seconds),0),'gaps_over_30s',count(*)filter(where gap_seconds>30),'gaps_over_5m',count(*)filter(where gap_seconds>300),'overlaps',count(*)filter(where gap_seconds<0))from g`).Scan(&b)
	if e != nil {
		panic(e)
	}
	fmt.Println(string(b))
}
