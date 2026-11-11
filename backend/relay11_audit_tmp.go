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
	rows, e := p.Query(c, `select jsonb_build_object('id',r.id,'stream',r.stream_id,'group',r.preferred_relay_group_id,'assignment_server',a.server_id,'assignment_class',a.execution_class,'next_fire',r.next_fire_at,'pending',(select count(*) from recording_jobs j where j.recording_id=r.id and j.status='pending'),'leased',(select count(*) from recording_jobs j where j.recording_id=r.id and j.status='leased' and j.lease_expires_at>now()),'handoff',(select count(*) from recording_jobs j where j.recording_id=r.id and j.handoff_until>now()),'intents',(select count(*) from recording_upload_intents u where u.recording_id=r.id and u.status='pending' and u.expires_at>now()),'nodes',coalesce((select jsonb_agg(jsonb_build_object('id',n.id,'label',n.display_name,'status',n.status,'heartbeat',n.last_heartbeat_at,'active_jobs',n.capabilities_jsonb->>'active_jobs','max',n.relay_max_streams)) from nodes n where n.relay_group_id=r.preferred_relay_group_id),'[]'::jsonb),'reservations',coalesce((select jsonb_agg(jsonb_build_object('node',x.node_id,'expires',x.expires_at,'stage',x.preopen_stage)) from recording_canary_reservations x where x.recording_id=r.id and x.expires_at>now()),'[]'::jsonb)) from recordings r left join recording_assignments a on a.stream_id=r.stream_id where r.id=any(array[410,411,412,414,432,433,434,435,436,442,445]::bigint[]) order by r.id`)
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
	rows.Close()
	rows, e = p.Query(c, `select jsonb_build_object('node',id,'name',display_name,'hostname',hostname,'group',relay_group_id,'status',status,'heartbeat',last_heartbeat_at,'active_jobs',capabilities_jsonb->>'active_jobs') from nodes where node_type='relay' and status='active' order by id`)
	if e != nil {
		panic(e)
	}
	for rows.Next() {
		var b []byte
		if e = rows.Scan(&b); e != nil {
			panic(e)
		}
		fmt.Println(string(b))
	}
	rows.Close()
	rows, e = p.Query(c, `select jsonb_build_object('job',j.id,'recording',j.recording_id,'owner',j.lease_owner,'attempt',j.attempt_count,'error',j.error_text,'expires',j.lease_expires_at,'window_end',j.window_end_at) from recording_jobs j where j.status='leased' and j.lease_expires_at>now() and j.recording_id=any(array[408,410,411,412,414,417,432,433,434,435,436,442,445]::bigint[]) order by j.recording_id`)
	if e != nil {
		panic(e)
	}
	for rows.Next() {
		var b []byte
		if e = rows.Scan(&b); e != nil {
			panic(e)
		}
		fmt.Println(string(b))
	}
}
