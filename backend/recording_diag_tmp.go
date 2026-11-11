package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	p, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil { panic(err) }
	defer p.Close()
	rows, err := p.Query(ctx, `
SELECT r.id,r.name,r.status,r.mode,r.capture_via,
       j.id,j.status,j.fire_at,j.window_end_at,j.lease_expires_at,
       s.id,s.provider,s.source_url
FROM recordings r
LEFT JOIN LATERAL (SELECT id,status,fire_at,window_end_at,lease_expires_at FROM recording_jobs WHERE recording_id=r.id AND kind='continuous_window' ORDER BY id DESC LIMIT 1) j ON true
LEFT JOIN streams s ON s.id=r.stream_id
WHERE r.id IN (405,433)
ORDER BY r.id`)

	if err != nil { panic(err) }
	defer rows.Close()
	for rows.Next() {
		var id, jobID, streamID int64
		var name, status, mode, via, jobStatus, provider, sourceURL string
		var fireAt, windowEnd, leaseExpires *time.Time
		if err := rows.Scan(&id,&name,&status,&mode,&via,&jobID,&jobStatus,&fireAt,&windowEnd,&leaseExpires,&streamID,&provider,&sourceURL); err != nil { panic(err) }
		fmt.Printf("recording=%d name=%q status=%s mode=%s capture_via=%s job=%d job_status=%s fire_at=%v window_end=%v lease_expires=%v stream=%d provider=%s source_url_present=%t\n",id,name,status,mode,via,jobID,jobStatus,fireAt,windowEnd,leaseExpires,streamID,provider,sourceURL != "")
	}
	if err := rows.Err(); err != nil { panic(err) }
}
