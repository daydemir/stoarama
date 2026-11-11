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
	var out []byte
	err = p.QueryRow(ctx, `
SELECT jsonb_build_object(
  'recording', r.id,
  'recording_status', r.status,
  'job_id', j.id,
  'job_status', j.status,
  'lease_owner', j.lease_owner,
  'lease_expires_at', j.lease_expires_at,
  'window_end_at', j.window_end_at,
  'attempt_count', j.attempt_count,
  'error_text', j.error_text,
  'pending_jobs', (SELECT count(*) FROM recording_jobs x WHERE x.recording_id=r.id AND x.status='pending'),
  'live_leases', (SELECT count(*) FROM recording_jobs x WHERE x.recording_id=r.id AND x.status='leased' AND x.lease_expires_at>now()),
  'last_clip_end', (SELECT max(c.clip_end_at) FROM recording_clips c WHERE c.recording_job_id=j.id),
  'last_clip_created', (SELECT max(c.created_at) FROM recording_clips c WHERE c.recording_job_id=j.id),
  'now', now()
)
FROM recordings r
LEFT JOIN LATERAL (
  SELECT * FROM recording_jobs x
  WHERE x.recording_id=r.id AND x.kind='continuous_window'
  ORDER BY x.fire_at DESC, x.id DESC LIMIT 1
) j ON true
WHERE r.id=402`).Scan(&out)
	if err != nil { panic(err) }
	var v any
	_ = json.Unmarshal(out, &v)
	b, _ := json.Marshal(v)
	fmt.Println(string(b))
}
