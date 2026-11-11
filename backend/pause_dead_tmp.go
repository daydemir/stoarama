package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	ctx := context.Background()
	db, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil { panic(err) }
	defer db.Close()
	for _, id := range []int64{426, 438} {
		var status string
		var pending, pendingWithClips, leased, handoff, intents int
		err = db.QueryRow(ctx, `SELECT r.status,
			(SELECT count(*) FROM recording_jobs j WHERE j.recording_id=r.id AND j.status='pending'),
			(SELECT count(*) FROM recording_jobs j WHERE j.recording_id=r.id AND j.status='pending' AND EXISTS (SELECT 1 FROM recording_clips c WHERE c.recording_job_id=j.id)),
			(SELECT count(*) FROM recording_jobs j WHERE j.recording_id=r.id AND j.status='leased' AND j.lease_expires_at>now()),
			(SELECT count(*) FROM recording_jobs j WHERE j.recording_id=r.id AND j.handoff_until>now()),
			(SELECT count(*) FROM recording_upload_intents u WHERE u.recording_id=r.id AND u.status='pending' AND u.expires_at>now())
			FROM recordings r WHERE r.id=$1 AND r.account_id=47`, id).Scan(&status, &pending, &pendingWithClips, &leased, &handoff, &intents)
		if err != nil { panic(err) }
		if status == "paused" { fmt.Printf("VERIFIED %d paused\n", id); continue }
		if status != "active" || pendingWithClips+leased+handoff+intents != 0 {
			fmt.Printf("HOLD %d status=%s p%d(pc%d)/l%d/h%d/i%d\n", id, status, pending, pendingWithClips, leased, handoff, intents)
			continue
		}
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("%s/api/v1/account/recordings/%d/pause", strings.TrimRight(os.Getenv("BACKEND_API_URL"), "/"), id), nil)
		req.Header.Set("Authorization", "Bearer "+os.Getenv("MITSCL_API_TOKEN"))
		resp, err := http.DefaultClient.Do(req)
		if err != nil { panic(err) }
		resp.Body.Close()
		if resp.StatusCode/100 != 2 { fmt.Printf("ERROR %d status=%d\n", id, resp.StatusCode); continue }
		var got string
		var next any
		if err = db.QueryRow(ctx, `SELECT status,next_fire_at FROM recordings WHERE id=$1 AND account_id=47`, id).Scan(&got, &next); err != nil { panic(err) }
		fmt.Printf("APPLY %d status=%s next=%v verified=%t\n", id, got, next, got == "paused" && next == nil)
	}
}
