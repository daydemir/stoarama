package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const watchLock int64 = 0x4650365741544348

type gate struct {
	at   time.Time
	kind string
	ids  []int64
}

func emit(v any) {
	b, _ := json.Marshal(v)
	fmt.Println(string(b))
}

func sleepUntil(t time.Time) {
	for {
		d := time.Until(t)
		if d <= 0 {
			return
		}
		if d > 30*time.Second {
			d = 30 * time.Second
		}
		time.Sleep(d)
	}
}

func main() {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		panic(err)
	}
	defer pool.Close()
	conn, err := pool.Acquire(ctx)
	if err != nil {
		panic(err)
	}
	defer conn.Release()
	var locked bool
	if err = conn.QueryRow(ctx, `select pg_try_advisory_lock($1)`, watchLock).Scan(&locked); err != nil {
		panic(err)
	}
	if !locked {
		emit(map[string]any{"event": "overlap_refused", "lock": watchLock, "at": time.Now().UTC()})
		return
	}
	defer conn.Exec(context.Background(), `select pg_advisory_unlock($1)`, watchLock)

	ids := []int64{406, 408, 409, 417, 427, 444}
	rows, err := pool.Query(ctx, `select id,next_fire_at from recordings where id=any($1) and status='active' order by id`, ids)
	if err != nil {
		panic(err)
	}
	var gates []gate
	fires := map[int64]time.Time{}
	for rows.Next() {
		var id int64
		var fire time.Time
		if err := rows.Scan(&id, &fire); err != nil {
			panic(err)
		}
		fires[id] = fire
		gates = append(gates, gate{fire.Add(-2*time.Hour + 3*time.Second), "t_minus_2", []int64{id}})
		gates = append(gates, gate{fire.Add(-30*time.Minute + 3*time.Second), "t_minus_30", []int64{id}})
		gates = append(gates, gate{fire.Add(3 * time.Minute), "first3", []int64{id}})
	}
	rows.Close()
	if len(fires) != len(ids) {
		emit(map[string]any{"event": "arm_failed", "reason": "missing_active_recording", "fires": fires})
		os.Exit(2)
	}
	sort.Slice(gates, func(i, j int) bool { return gates[i].at.Before(gates[j].at) })
	emit(map[string]any{"event": "armed", "at": time.Now().UTC(), "lock": watchLock, "recordings": ids, "fires": fires, "gate_count": len(gates)})
	for len(gates) > 0 {
		g := gates[0]
		gates = gates[1:]
		for len(gates) > 0 && gates[0].kind == g.kind && gates[0].at.Sub(g.at) < time.Minute {
			g.ids = append(g.ids, gates[0].ids...)
			gates = gates[1:]
		}
		sleepUntil(g.at)
		if g.kind != "first3" {
			cmd := exec.CommandContext(ctx, "/tmp/stoaramactl-fineplus-watch", "recording-preopen", "run", "--concurrency", "4", "--probe-sec", "20")
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			err := cmd.Run()
			emit(map[string]any{"event": g.kind, "at": time.Now().UTC(), "recordings": g.ids, "error": fmt.Sprint(err)})
			continue
		}
		for attempt := 1; attempt <= 8; attempt++ {
			var states []map[string]any
			for _, id := range g.ids {
				var jobID *int64
				var state *string
				var clips int
				var latestMedia, latestIngest *time.Time
				err := pool.QueryRow(ctx, `select j.id,j.status,count(c.id)::int,max(c.clip_end_at),max(c.created_at) from recordings r left join lateral (select * from recording_jobs where recording_id=r.id and fire_at=$2 order by id desc limit 1) j on true left join recording_clips c on c.recording_job_id=j.id where r.id=$1 group by j.id,j.status`, id, fires[id]).Scan(&jobID, &state, &clips, &latestMedia, &latestIngest)
				states = append(states, map[string]any{"recording": id, "job": jobID, "state": state, "clips": clips, "latest_media": latestMedia, "latest_ingest": latestIngest, "error": fmt.Sprint(err)})
			}
			emit(map[string]any{"event": "first3", "at": time.Now().UTC(), "attempt": attempt, "states": states})
			all := true
			for _, s := range states {
				if s["clips"].(int) < 3 {
					all = false
				}
			}
			if all {
				break
			}
			time.Sleep(time.Minute)
		}
	}
	emit(map[string]any{"event": "complete", "at": time.Now().UTC(), "recordings": ids})
}
