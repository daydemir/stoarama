package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const joinedBatch = "goodplus-20260821-generation-1"

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	p, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil { panic(err) }
	defer p.Close()

	var total, sealed, pending, leased, failed int
	err = p.QueryRow(ctx, `
SELECT count(*), count(*) FILTER (WHERE state='sealed'), count(*) FILTER (WHERE state='pending'),
       count(*) FILTER (WHERE state='leased'), count(*) FILTER (WHERE state='terminal_failed')
FROM recording_joined_hours WHERE batch_id=$1`, joinedBatch).
		Scan(&total, &sealed, &pending, &leased, &failed)
	if err != nil { panic(err) }

	var h1, m15, m5 int
	var latest *time.Time
	err = p.QueryRow(ctx, `
SELECT count(*) FILTER (WHERE sealed_at >= now()-interval '1 hour'),
       count(*) FILTER (WHERE sealed_at >= now()-interval '15 minutes'),
       count(*) FILTER (WHERE sealed_at >= now()-interval '5 minutes')
FROM recording_joined_hours WHERE batch_id=$1 AND state='sealed'`, joinedBatch).
		Scan(&h1, &m15, &m5)
	if err != nil { panic(err) }
	if err := p.QueryRow(ctx, `SELECT max(sealed_at) FROM recording_joined_hours WHERE batch_id=$1 AND state='sealed'`, joinedBatch).Scan(&latest); err != nil { panic(err) }
	var paused bool
	if err := p.QueryRow(ctx, `SELECT claims_paused FROM recording_joined_admission_controls WHERE batch_id=$1`, joinedBatch).Scan(&paused); err != nil { panic(err) }
	var ready, leasedCurrent int
	if err := p.QueryRow(ctx, `SELECT count(*) FILTER (WHERE state='pending' AND next_attempt_at<=now()), count(*) FILTER (WHERE state='leased' AND lease_expires_at>now()) FROM recording_joined_hours WHERE batch_id=$1`, joinedBatch).Scan(&ready,&leasedCurrent); err != nil { panic(err) }
	var eligible int
	if err := p.QueryRow(ctx, `SELECT count(*) FROM recording_joined_hours h JOIN recording_joined_artifacts ledger ON ledger.stream_day_id=h.stream_day_id AND ledger.artifact_kind='allocation_ledger' AND ledger.publication_state='published' JOIN connections c ON c.id=h.connection_id AND c.joined_protocol_version=1 JOIN recording_joined_batches b ON b.id=h.batch_record_id WHERE h.batch_id=$1 AND b.state='frozen' AND h.source_clip_count>0 AND h.attempt_count<10 AND ((h.state='pending' AND h.next_attempt_at<=now()) OR (h.state='leased' AND h.lease_expires_at<=now()))`, joinedBatch).Scan(&eligible); err != nil { panic(err) }
	var connectionEligible int
	if err := p.QueryRow(ctx, `SELECT count(*) FROM recording_joined_hours h JOIN recording_joined_artifacts ledger ON ledger.stream_day_id=h.stream_day_id AND ledger.artifact_kind='allocation_ledger' AND ledger.publication_state='published' JOIN recording_joined_batches b ON b.id=h.batch_record_id WHERE h.batch_id=$1 AND h.connection_id=13 AND b.state='frozen' AND h.source_clip_count>0 AND h.attempt_count<10 AND ((h.state='pending' AND h.next_attempt_at<=now()) OR (h.state='leased' AND h.lease_expires_at<=now()))`, joinedBatch).Scan(&connectionEligible); err != nil { panic(err) }
	var under25GB, maxSource int64
	if err := p.QueryRow(ctx, `SELECT count(*) FILTER (WHERE source_bytes<=25::bigint*1024*1024*1024),coalesce(max(source_bytes),0) FROM recording_joined_hours WHERE batch_id=$1 AND state='pending'`, joinedBatch).Scan(&under25GB,&maxSource); err != nil { panic(err) }
	rowsA, err := p.Query(ctx, `SELECT attempt_count,count(*) FROM recording_joined_hours WHERE batch_id=$1 AND state='pending' GROUP BY attempt_count ORDER BY attempt_count`, joinedBatch); if err != nil { panic(err) }; defer rowsA.Close(); for rowsA.Next(){var attempt,count int; if err:=rowsA.Scan(&attempt,&count);err!=nil{panic(err)};fmt.Printf("pending_attempt_count=%d count=%d\n",attempt,count)}
	var protocol0, protocol1 int
	if err := p.QueryRow(ctx, `SELECT count(*) FILTER (WHERE c.joined_protocol_version=0),count(*) FILTER (WHERE c.joined_protocol_version=1) FROM recording_joined_hours h JOIN connections c ON c.id=h.connection_id WHERE h.batch_id=$1 AND h.state='pending'`, joinedBatch).Scan(&protocol0,&protocol1); err != nil { panic(err) }
	fmt.Printf("observed_at=%s total=%d sealed=%d pending=%d leased=%d terminal_failed=%d sealed_last_1h=%d sealed_last_15m=%d sealed_last_5m=%d\n",
		time.Now().UTC().Format(time.RFC3339), total, sealed, pending, leased, failed, h1, m15, m5)
	fmt.Printf("latest_sealed_at=%v\n", latest)
	fmt.Printf("admission claims_paused=%t\n", paused)
	fmt.Printf("claimable_pending=%d current_leased=%d\n", ready, leasedCurrent)
	fmt.Printf("eligible_protocol1_unbounded=%d\n", eligible)
	fmt.Printf("eligible_configured_connection_13=%d\n", connectionEligible)
	fmt.Printf("pending_under_25GiB=%d max_pending_source_bytes=%d\n", under25GB,maxSource)
	fmt.Printf("pending_connection_protocol0=%d pending_connection_protocol1=%d\n", protocol0, protocol1)
	rows, err := p.Query(ctx, `SELECT h.connection_id,count(*),c.joined_protocol_version FROM recording_joined_hours h JOIN connections c ON c.id=h.connection_id WHERE h.batch_id=$1 AND h.state='pending' GROUP BY h.connection_id,c.joined_protocol_version ORDER BY h.connection_id`, joinedBatch)
	if err != nil { panic(err) }
	defer rows.Close()
	for rows.Next() { var connectionID, count, version int; if err := rows.Scan(&connectionID,&count,&version); err != nil { panic(err) }; fmt.Printf("pending_connection id=%d count=%d protocol=%d\n", connectionID,count,version) }
	rows2, err := p.Query(ctx, `SELECT hour_id,claimed_by,extract(epoch from now()-heartbeat_at)::int FROM recording_joined_hours WHERE batch_id=$1 AND state='leased' ORDER BY claimed_by`, joinedBatch)
	if err != nil { panic(err) }
	defer rows2.Close()
	for rows2.Next() { var hour,worker string; var age int; if err := rows2.Scan(&hour,&worker,&age); err != nil { panic(err) }; fmt.Printf("leased_hour=%s worker=%s heartbeat_age=%ds\n", hour,worker,age) }
}
