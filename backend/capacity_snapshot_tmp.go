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
	var b []byte
	e = p.QueryRow(c, `select jsonb_build_object(
'six_z_load',(select count(*) from recordings where status='active' and mode='continuous' and next_fire_at='2026-08-21 06:00:00+00'),
'six_z_cloud',(select count(*) from recordings where status='active' and mode='continuous' and next_fire_at='2026-08-21 06:00:00+00' and capture_via='cloud'),
'six_z_relay',(select count(*) from recordings where status='active' and mode='continuous' and next_fire_at='2026-08-21 06:00:00+00' and capture_via='relay'),
		'healthy_cloud_workers',(select count(*) from recorder_droplets where state='active' and last_seen_at>now()-interval '5 minutes'),
		'healthy_cloud_capacity',(select coalesce(sum(capacity),0) from recorder_droplets where state='active' and last_seen_at>now()-interval '5 minutes'),
		'all_ready_capacity',(select coalesce(sum(capacity),0) from recorder_droplets where state='ready'),
		'droplet_states',(select jsonb_agg(x) from(select state,count(*) workers,coalesce(sum(capacity),0) capacity,max(last_seen_at) latest from recorder_droplets group by state order by state)x))`).Scan(&b)
	if e != nil {
		panic(e)
	}
	var x any
	_ = json.Unmarshal(b, &x)
	o, _ := json.Marshal(x)
	fmt.Println(string(o))
}
