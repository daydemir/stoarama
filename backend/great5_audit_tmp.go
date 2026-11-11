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
	for _, id := range []int{401, 406, 409, 417, 427} {
		var row map[string]any
		raw := []byte{}
		err = p.QueryRow(ctx, `select jsonb_build_object(
			'id',r.id,'status',r.status,'timezone',r.cron_timezone,
			'window_start',r.daily_window_start,'window_end',r.daily_window_end,
			'clip_seconds',r.clip_duration_sec,'weekdays',r.active_weekdays,
			'start_at',r.start_at,'end_at',r.end_at,'next_fire',r.next_fire_at,
			'pending',(select count(*) from recording_jobs j where j.recording_id=r.id and j.status='pending'),
			'leased',(select count(*) from recording_jobs j where j.recording_id=r.id and j.status='leased' and j.lease_expires_at>now()),
			'handoff',(select count(*) from recording_jobs j where j.recording_id=r.id and j.handoff_until>now()),
			'intents',(select count(*) from recording_upload_intents u where u.recording_id=r.id and u.status='pending' and u.expires_at>now()))::text
			from recordings r where r.account_id=47 and r.id=$1`, id).Scan(&raw)
		if err != nil {
			panic(err)
		}
		if err = json.Unmarshal(raw, &row); err != nil {
			panic(err)
		}
		out, _ := json.Marshal(row)
		fmt.Println(string(out))
	}
}
