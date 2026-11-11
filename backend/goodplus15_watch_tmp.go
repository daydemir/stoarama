package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"os"
	"os/exec"
	"sort"
	"time"
)

const lock int64 = 0x4750313557415443

type gate struct {
	at   time.Time
	kind string
	ids  []int64
}

func emit(v any) { b, _ := json.Marshal(v); fmt.Println(string(b)) }
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
func state(ctx context.Context, p *pgxpool.Pool, id int64, fire time.Time) map[string]any {
	var job *int64
	var status string
	var clips, intents int
	var lease, handoff, media, ingest *time.Time
	e := p.QueryRow(ctx, `select j.id,coalesce(j.status,''),count(c.id)::int,j.lease_expires_at,j.handoff_until,max(c.clip_end_at),max(c.created_at),(select count(*) from recording_upload_intents u where u.recording_id=r.id and u.status='pending' and u.expires_at>now()) from recordings r left join lateral(select * from recording_jobs where recording_id=r.id and fire_at=$2 order by id desc limit 1)j on true left join recording_clips c on c.recording_job_id=j.id where r.id=$1 group by j.id,j.status,j.lease_expires_at,j.handoff_until,r.id`, id, fire).Scan(&job, &status, &clips, &lease, &handoff, &media, &ingest, &intents)
	return map[string]any{"recording": id, "job": job, "status": status, "clips": clips, "lease_expires": lease, "handoff": handoff, "latest_media": media, "latest_ingest": ingest, "live_intents": intents, "error": fmt.Sprint(e)}
}
func health(ctx context.Context, p *pgxpool.Pool, id int64, fire time.Time) map[string]any {
	var calc time.Time
	var version int
	var coverage, gap float64
	var gaps, overlaps int
	e := p.QueryRow(ctx, `select calculated_at,metric_version,coverage_pct,largest_gap_seconds,gap_over_5m_count,overlap_count from recording_window_health where recording_id=$1 and window_start_at=$2 order by calculated_at desc limit 1`, id, fire).Scan(&calc, &version, &coverage, &gap, &gaps, &overlaps)
	return map[string]any{"recording": id, "calculated_at": calc, "metric_version": version, "coverage_pct": coverage, "largest_gap_seconds": gap, "gap_over_5m_count": gaps, "overlap_count": overlaps, "error": fmt.Sprint(e)}
}
func cadence(ctx context.Context, p *pgxpool.Pool, id int64, fire time.Time) map[string]any {
	var clips int
	var first, last *time.Time
	var positive, largest, coverage float64
	var over30, over300, overlap int
	e := p.QueryRow(ctx, `with j as(select id,fire_at from recording_jobs where recording_id=$1 and fire_at=$2 order by id desc limit 1),x as(select c.clip_start_at,c.clip_end_at,lag(c.clip_end_at)over(order by c.clip_start_at,c.id)prev_end from recording_clips c join j on j.id=c.recording_job_id),g as(select extract(epoch from clip_start_at-prev_end)gap from x where prev_end is not null),a as(select count(*)::int clips,min(clip_start_at)first,max(clip_end_at)last from x)select a.clips,a.first,a.last,coalesce(sum(g.gap)filter(where g.gap>0),0),coalesce(max(g.gap),0),count(*)filter(where g.gap>30),count(*)filter(where g.gap>300),count(*)filter(where g.gap<0),coalesce(100*(extract(epoch from(a.last-a.first))-coalesce(sum(g.gap)filter(where g.gap>0),0))/nullif(extract(epoch from(a.last-j.fire_at)),0),0) from a cross join j left join g on true group by a.clips,a.first,a.last,j.fire_at`, id, fire).Scan(&clips, &first, &last, &positive, &largest, &over30, &over300, &overlap, &coverage)
	return map[string]any{"recording": id, "clips": clips, "first": first, "last": last, "positive_gap_seconds": positive, "largest_gap_seconds": largest, "gaps_over_30s": over30, "gaps_over_5m": over300, "overlaps": overlap, "coverage_to_latest_pct": coverage, "error": fmt.Sprint(e)}
}
func main() {
	ctx := context.Background()
	p, e := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if e != nil {
		panic(e)
	}
	defer p.Close()
	c, e := p.Acquire(ctx)
	if e != nil {
		panic(e)
	}
	defer c.Release()
	var ok bool
	if e = c.QueryRow(ctx, `select pg_try_advisory_lock($1)`, lock).Scan(&ok); e != nil {
		panic(e)
	}
	if !ok {
		emit(map[string]any{"event": "overlap_refused", "lock": lock})
		return
	}
	defer c.Exec(context.Background(), `select pg_advisory_unlock($1)`, lock)
	ids := []int64{406, 408, 409, 410, 411, 412, 414, 417, 432, 433, 434, 435, 436, 442, 445}
	fires := map[int64]time.Time{}
	rows, e := p.Query(ctx, `select r.id,coalesce((select j.fire_at from recording_jobs j where j.recording_id=r.id and j.kind='continuous_window' and j.fire_at<=now() and j.window_end_at>now() and j.status in('pending','leased') order by j.id desc limit 1),r.next_fire_at) from recordings r where r.id=any($1) and r.status='active' order by r.id`, ids)
	if e != nil {
		panic(e)
	}
	for rows.Next() {
		var id int64
		var f time.Time
		if e = rows.Scan(&id, &f); e != nil {
			panic(e)
		}
		fires[id] = f
	}
	rows.Close()
	if len(fires) != len(ids) {
		emit(map[string]any{"event": "arm_failed", "fires": fires})
		os.Exit(2)
	}
	var gs []gate
	for id, f := range fires {
		for _, x := range []struct {
			d time.Duration
			k string
		}{{-2*time.Hour + 3*time.Second, "t_minus_2"}, {-30*time.Minute + 3*time.Second, "t_minus_30"}, {3 * time.Minute, "first3"}, {6 * time.Hour, "sustained"}, {12*time.Hour + 3*time.Minute, "close"}, {12*time.Hour + 35*time.Minute, "metric"}} {
			gs = append(gs, gate{f.Add(x.d), x.k, []int64{id}})
		}
		if id == 410 || id == 411 || id == 412 || id == 414 || id == 432 || id == 433 || id == 434 || id == 435 || id == 436 || id == 442 || id == 445 {
			gs = append(gs, gate{f.Add(20 * time.Minute), "cadence", []int64{id}})
		}
	}
	sort.Slice(gs, func(i, j int) bool { return gs[i].at.Before(gs[j].at) })
	emit(map[string]any{"event": "armed", "at": time.Now().UTC(), "lock": lock, "fires": fires, "gate_count": len(gs)})
	for len(gs) > 0 {
		g := gs[0]
		gs = gs[1:]
		for len(gs) > 0 && gs[0].kind == g.kind && gs[0].at.Sub(g.at) < time.Minute {
			g.ids = append(g.ids, gs[0].ids...)
			gs = gs[1:]
		}
		sleepUntil(g.at)
		if g.kind == "t_minus_2" || g.kind == "t_minus_30" {
			cmd := exec.CommandContext(ctx, "/tmp/stoaramactl-fineplus-watch", "recording-preopen", "run", "--concurrency", "4", "--probe-sec", "20")
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			e = cmd.Run()
			emit(map[string]any{"event": g.kind, "at": time.Now().UTC(), "recordings": g.ids, "error": fmt.Sprint(e)})
			continue
		}
		if g.kind == "metric" {
			cmd := exec.CommandContext(ctx, "/tmp/stoaramactl-fineplus-watch", "recording-health", "run")
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			e = cmd.Run()
			hs := []map[string]any{}
			for _, id := range g.ids {
				hs = append(hs, health(ctx, p, id, fires[id]))
			}
			emit(map[string]any{"event": "metric_health", "at": time.Now().UTC(), "recordings": g.ids, "health": hs, "command_error": fmt.Sprint(e)})
		}
		if g.kind == "cadence" {
			cs := []map[string]any{}
			for _, id := range g.ids {
				cs = append(cs, cadence(ctx, p, id, fires[id]))
			}
			emit(map[string]any{"event": "cadence", "at": time.Now().UTC(), "recordings": g.ids, "cadence": cs})
			continue
		}
		attempts := 1
		if g.kind == "first3" || g.kind == "close" {
			attempts = 8
		}
		for n := 1; n <= attempts; n++ {
			ss := []map[string]any{}
			pass := true
			for _, id := range g.ids {
				s := state(ctx, p, id, fires[id])
				ss = append(ss, s)
				if g.kind == "first3" && s["clips"].(int) < 3 {
					pass = false
				}
				if g.kind == "close" && (s["status"] != "done" || s["lease_expires"] != nil || s["handoff"] != nil || s["live_intents"].(int) > 0) {
					pass = false
				}
			}
			emit(map[string]any{"event": g.kind, "at": time.Now().UTC(), "attempt": n, "recordings": g.ids, "states": ss, "command_error": fmt.Sprint(e)})
			if pass {
				break
			}
			time.Sleep(time.Minute)
		}
	}
	emit(map[string]any{"event": "complete", "at": time.Now().UTC(), "recordings": ids})
}
