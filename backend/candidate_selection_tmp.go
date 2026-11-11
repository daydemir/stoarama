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
	if err != nil { panic(err) }
	defer p.Close()
	ids := []int64{413,416,439,440,421,422,423,424,425,429,430,441}
	rows, err := p.Query(ctx, `
		SELECT r.id, r.status, r.stream_id, r.cron_timezone, r.end_at,
			s.name, s.location_country, s.location_city,
			COALESCE((SELECT count(*) FROM recording_jobs j WHERE j.recording_id=r.id AND j.kind='continuous_window' AND j.status='done'),0),
			COALESCE((SELECT max(j.window_end_at) FROM recording_jobs j WHERE j.recording_id=r.id AND j.kind='continuous_window' AND j.status='done'),NULL),
			COALESCE((SELECT count(*) FROM recording_window_health h WHERE h.recording_id=r.id AND h.metric_version=2 AND h.clip_count>0 AND h.overlap_count=0),0),
			COALESCE((SELECT count(*) FROM recording_window_health h WHERE h.recording_id=r.id AND (h.metric_version IS NULL OR h.metric_version<>2 OR h.clip_count=0 OR h.overlap_count<>0)),0)
		FROM recordings r JOIN streams s ON s.id=r.stream_id WHERE r.id=ANY($1) ORDER BY r.id`, ids)
	if err != nil { panic(err) }
	defer rows.Close()
	for rows.Next() {
		var id, sid, done, good, bad int64
		var status, tz, name, country, city string
		var end, last any
		if err := rows.Scan(&id,&status,&sid,&tz,&end,&name,&country,&city,&done,&last,&good,&bad); err != nil { panic(err) }
		b,_ := json.Marshal(map[string]any{"recording":id,"status":status,"stream_id":sid,"timezone":tz,"end_at":end,"name":name,"country":country,"city":city,"done_jobs":done,"last_done_window":last,"good_health_rows":good,"bad_health_rows":bad})
		fmt.Println(string(b))
	}
	rows.Close()
	rows, err = p.Query(ctx, `
		SELECT r.id, j.id, j.fire_at, j.window_end_at, j.status, j.attempt_count,
			COALESCE(count(c.id),0), COALESCE(sum(c.size_bytes),0),
			COALESCE(max(h.coverage_pct),0), COALESCE(max(h.largest_gap_seconds),0),
			COALESCE(max(h.gap_over_5m_count),0), COALESCE(max(h.overlap_count),0),
			COALESCE(max(c.clip_end_at),NULL)
		FROM recordings r JOIN recording_jobs j ON j.recording_id=r.id AND j.kind='continuous_window'
		LEFT JOIN recording_clips c ON c.recording_job_id=j.id
		LEFT JOIN recording_window_health h ON h.recording_id=r.id AND h.job_id=j.id
		WHERE r.id=ANY($1) AND j.status='done'
		GROUP BY r.id,j.id,j.fire_at,j.window_end_at,j.status,j.attempt_count
		ORDER BY r.id,j.fire_at`, ids)
	if err != nil { panic(err) }
	defer rows.Close()
	for rows.Next() {
		var rid, jid, attempts, clips, bytes, gaps5, overlaps int64
		var fire, end, last any
		var status string
		var coverage, gap float64
		if err := rows.Scan(&rid,&jid,&fire,&end,&status,&attempts,&clips,&bytes,&coverage,&gap,&gaps5,&overlaps,&last); err != nil { panic(err) }
		b,_ := json.Marshal(map[string]any{"recording":rid,"job":jid,"fire_at":fire,"window_end_at":end,"status":status,"attempts":attempts,"clips":clips,"bytes":bytes,"coverage_pct":coverage,"largest_gap_seconds":gap,"gap_over_5m":gaps5,"overlaps":overlaps,"last_clip_end":last})
		fmt.Println(string(b))
	}
	rows.Close()
	rows, err = p.Query(ctx, `
		SELECT c.recording_id, c.recording_job_id, count(*)::bigint,
			count(*) FILTER (WHERE c.purged_at IS NULL)::bigint,
			count(*) FILTER (WHERE c.released_at IS NULL)::bigint,
			count(*) FILTER (WHERE c.sha256 <> '' AND c.size_bytes > 0)::bigint,
			min(c.clip_start_at), max(c.clip_end_at), min(c.bucket), min(c.object_key), min(c.storage_destination_id)
		FROM recording_clips c WHERE c.recording_job_id=ANY($1)
		GROUP BY c.recording_id,c.recording_job_id ORDER BY c.recording_id`, []int64{3503400,3503405,3503406})
	if err != nil { panic(err) }
	defer rows.Close()
	for rows.Next() {
		var rid, jid, total, retained, unreleased, hashed, dest int64
		var first, last any
		var bucket, key string
		if err := rows.Scan(&rid,&jid,&total,&retained,&unreleased,&hashed,&first,&last,&bucket,&key,&dest); err != nil { panic(err) }
		b,_ := json.Marshal(map[string]any{"recording":rid,"job":jid,"clip_rows":total,"purged_at_null":retained,"released_at_null":unreleased,"hashed_positive":hashed,"first_clip":first,"last_clip":last,"bucket":bucket,"sample_object_key":key,"storage_destination_id":dest})
		fmt.Println(string(b))
	}
	rows.Close()
	rows, err = p.Query(ctx, `
		SELECT q.id,q.status,q.target_recording_count,count(m.recording_id)::bigint,
			count(*) FILTER (WHERE m.recording_id=ANY($1))::bigint,
			COALESCE(array_agg(m.recording_id ORDER BY m.recording_id),'{}')
		FROM recording_qualification_runs q LEFT JOIN recording_qualification_members m ON m.run_id=q.id
		WHERE q.account_id=47 GROUP BY q.id,q.status,q.target_recording_count ORDER BY q.id DESC LIMIT 5`, ids)
	if err != nil { panic(err) }
	defer rows.Close()
	for rows.Next() {
		var qid, target, members, selected int64
		var status string
		var memberIDs []int64
		if err := rows.Scan(&qid,&status,&target,&members,&selected,&memberIDs); err != nil { panic(err) }
		b,_ := json.Marshal(map[string]any{"qualification_run":qid,"status":status,"target":target,"members":members,"selected_candidates":selected,"member_ids":memberIDs})
		fmt.Println(string(b))
	}
}
