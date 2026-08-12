package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/email"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const recordingHealthDigestLock int64 = 0x53544f4152414d44

type digestRecording struct {
	ID, StreamID                                           int64
	Name, Bucket, CurrentCause, HistoryBucket, HistoryNote string
	Scheduled                                              bool
}

type digestNAS struct {
	Label, Phase, CapacityTransitionState          string
	Free, Total                                    *int64
	ReportedAt, BatchCompletedAt, InventoryAt      *time.Time
	CapacityTransitionAt                           *time.Time
	Blocked                                        bool
	BatchClips, BatchFailures, ServerOnly          int64
	InventoryClips, InventoryMismatches, Unmatched int64
}

func healthDigestBucket(now time.Time) time.Time {
	now = now.UTC()
	return time.Date(now.Year(), now.Month(), now.Day(), (now.Hour()/8)*8, 0, 0, 0, time.UTC)
}

func runRecordingHealthSummary(ctx context.Context, cfg config.Config) {
	pool := mustOpenPool(ctx, cfg)
	defer pool.Close()
	conn, err := pool.Acquire(ctx)
	if err != nil {
		log.Fatalf("acquire digest db: %v", err)
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, recordingHealthDigestLock); err != nil {
		log.Fatalf("lock digest: %v", err)
	}
	defer func() {
		_, _ = conn.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, recordingHealthDigestLock)
	}()

	now := time.Now().UTC()
	items, err := loadHealthDigestRecordings(ctx, pool)
	if err != nil {
		log.Fatalf("query digest recordings: %v", err)
	}
	nas, err := loadDigestNAS(ctx, pool)
	if err != nil {
		log.Fatalf("load digest NAS telemetry: %v", err)
	}
	body := composeHealthDigest(cfg.AppBaseURL, now, items, nas)
	recipients := operatorRecipients(ctx, pool)
	if len(recipients) == 0 {
		log.Fatalf("no operator recipients")
	}
	mailer, err := email.NewSender(email.Config{Provider: cfg.EmailProvider, From: cfg.EmailFrom, ReplyTo: cfg.EmailReplyTo, ResendKey: cfg.EmailResendAPIKey})
	if err != nil {
		log.Fatalf("init digest sender: %v", err)
	}
	bucket := healthDigestBucket(now)
	sent := 0
	for _, recipient := range recipients {
		claimed, err := claimHealthDigestDelivery(ctx, pool, bucket, recipient)
		if err != nil {
			log.Fatalf("claim digest: %v", err)
		}
		if !claimed {
			continue
		}
		if _, err := mailer.Send(ctx, email.Message{To: recipient, Subject: fmt.Sprintf("[Stoarama] 8-hour recording health: %s", digestCounts(items)), PlainText: body, MessageType: "recording_health_digest", IdempotencyKey: healthDigestIdempotencyKey(bucket, recipient)}); err != nil {
			log.Fatalf("send digest: %v", err)
		}
		if _, err := pool.Exec(ctx, `UPDATE recording_health_digest_deliveries SET delivered_at=now() WHERE bucket_start_at=$1 AND recipient=$2`, bucket, recipient); err != nil {
			log.Fatalf("mark digest delivered: %v", err)
		}
		sent++
	}
	printJSON(map[string]any{"bucket_start_at": bucket, "recordings": len(items), "sent": sent})
}

func loadHealthDigestRecordings(ctx context.Context, pool *pgxpool.Pool) ([]digestRecording, error) {
	rows, err := pool.Query(ctx, `
		SELECT r.id,COALESCE(r.stream_id,0),r.name,
		       live.id IS NOT NULL,
		       CASE
		         WHEN live.id IS NULL THEN 'unknown'
		         WHEN live.status IN ('error','canceled') THEN 'failing'
		         WHEN inc.incident_type IN ('continuous_silent_death','fresh_ingest_stale_media','job_retries_exhausted','clip_timestamp_drift') THEN 'failing'
		         WHEN latest.clip_end_at IS NULL OR latest.created_at < now()-interval '5 minutes' OR latest.clip_end_at < now()-interval '5 minutes' THEN 'failing'
		         WHEN inc.incident_type IS NOT NULL THEN 'degraded'
		         ELSE 'stable'
		       END,
		       CASE
		         WHEN live.id IS NULL THEN 'off-window / not currently assessed'
		         WHEN live.status IN ('error','canceled') THEN 'current job '||live.status
		         WHEN inc.incident_type IS NOT NULL THEN replace(inc.incident_type,'_',' ')
		         WHEN latest.clip_end_at IS NULL THEN 'no current clip'
		         WHEN latest.created_at < now()-interval '5 minutes' THEN 'no fresh ingest'
		         WHEN latest.clip_end_at < now()-interval '5 minutes' THEN 'stale media timeline'
		         ELSE 'current capture progressing'
		       END,
		       CASE
		         WHEN wh.recording_id IS NULL OR wh.calculated_at < now()-interval '2 hours' OR wh.gap_over_30s_count IS NULL OR wh.gap_over_5m_count IS NULL THEN 'unknown'
		         WHEN wh.coverage_pct>=99 AND wh.largest_gap_seconds<=120 AND wh.overlap_count=0 THEN 'stable'
		         WHEN wh.coverage_pct>=90 AND wh.largest_gap_seconds<=1800 AND wh.gap_over_5m_count<=2 AND wh.overlap_count=0 THEN 'degraded'
		         ELSE 'failing'
		       END,
		       CASE WHEN wh.recording_id IS NULL OR wh.calculated_at < now()-interval '2 hours' THEN 'latest completed window not freshly measured'
		            ELSE format('latest completed %.2f%%; largest gap %ss; >5m gaps %s; overlaps %s',wh.coverage_pct,round(wh.largest_gap_seconds),COALESCE(wh.gap_over_5m_count::text,'unknown'),wh.overlap_count) END
		FROM recordings r
		LEFT JOIN LATERAL (SELECT j.id,j.status FROM recording_jobs j WHERE j.recording_id=r.id AND j.kind='continuous_window' AND j.fire_at<=now() AND j.window_end_at>now() ORDER BY j.id DESC LIMIT 1) live ON true
		LEFT JOIN LATERAL (SELECT max(c.clip_end_at) clip_end_at,max(c.created_at) created_at FROM recording_clips c WHERE c.recording_job_id=live.id) latest ON true
		LEFT JOIN LATERAL (SELECT a.signal incident_type FROM recorder_health_alerts a WHERE a.recording_id=r.id AND a.resolved_at IS NULL ORDER BY a.last_detected_at DESC NULLS LAST LIMIT 1) inc ON true
		LEFT JOIN LATERAL (SELECT h.* FROM recording_window_health h WHERE h.recording_id=r.id ORDER BY h.window_end_at DESC LIMIT 1) wh ON true
		WHERE r.status='active' AND r.start_at<=now() AND (r.end_at IS NULL OR now()<r.end_at)
		ORDER BY r.id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []digestRecording
	for rows.Next() {
		var item digestRecording
		if err := rows.Scan(&item.ID, &item.StreamID, &item.Name, &item.Scheduled, &item.Bucket, &item.CurrentCause, &item.HistoryBucket, &item.HistoryNote); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func claimHealthDigestDelivery(ctx context.Context, pool *pgxpool.Pool, bucket time.Time, recipient string) (bool, error) {
	var claimed bool
	err := pool.QueryRow(ctx, `
		INSERT INTO recording_health_digest_deliveries(bucket_start_at,recipient,attempted_at)
		VALUES($1,$2,now())
		ON CONFLICT(bucket_start_at,recipient) DO UPDATE SET attempted_at=now()
		WHERE recording_health_digest_deliveries.delivered_at IS NULL
		  AND (recording_health_digest_deliveries.attempted_at IS NULL OR recording_health_digest_deliveries.attempted_at<now()-interval '10 minutes')
		RETURNING true`, bucket, recipient).Scan(&claimed)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return claimed, err
}

func loadDigestNAS(ctx context.Context, pool *pgxpool.Pool) (digestNAS, error) {
	var n digestNAS
	err := pool.QueryRow(ctx, `
		SELECT c.label,c.client_phase,c.nas_storage_free_bytes,c.nas_storage_total_bytes,c.nas_storage_reported_at,c.nas_capacity_blocked,
		       c.nas_batch_clips,c.nas_batch_failures,c.nas_batch_completed_at,c.inventory_clips,c.inventory_mismatches,c.inventory_unmatched,c.inventory_reported_at,
		       (SELECT count(*) FROM recording_clips rc JOIN recordings r ON r.id=rc.recording_id
		         WHERE r.account_id=c.account_id AND r.delivery='nas_pull' AND rc.purged_at IS NULL AND rc.released_at IS NULL
		           AND NOT EXISTS (SELECT 1 FROM nas_inventory_files i WHERE i.connection_id=c.id AND i.clip_id=rc.id AND i.state='present'
		             AND i.relative_path=rc.display_path AND i.size_bytes=rc.size_bytes AND i.sha256=lower(rc.sha256))),
		       COALESCE(e.state::text,''),e.observed_at
		FROM connections c
		LEFT JOIN LATERAL (SELECT state,observed_at FROM nas_storage_capacity_alert_events WHERE connection_id=c.id ORDER BY id DESC LIMIT 1) e ON true
		WHERE c.kind='nas_pull' ORDER BY c.last_seen_at DESC LIMIT 1`).Scan(
		&n.Label, &n.Phase, &n.Free, &n.Total, &n.ReportedAt, &n.Blocked,
		&n.BatchClips, &n.BatchFailures, &n.BatchCompletedAt, &n.InventoryClips, &n.InventoryMismatches, &n.Unmatched, &n.InventoryAt,
		&n.ServerOnly, &n.CapacityTransitionState, &n.CapacityTransitionAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return digestNAS{}, nil
	}
	return n, err
}

func healthDigestIdempotencyKey(bucket time.Time, recipient string) string {
	sum := sha256.Sum256([]byte(strings.ToLower(strings.TrimSpace(recipient))))
	return fmt.Sprintf("recording-health-digest:%s:%x", bucket.UTC().Format("20060102T15"), sum[:12])
}

func digestCounts(items []digestRecording) string {
	c := map[string]int{}
	for _, item := range items {
		if item.Scheduled {
			c[item.Bucket]++
		}
	}
	return fmt.Sprintf("%d stable, %d degraded, %d failing, %d unknown (of %d live)", c["stable"], c["degraded"], c["failing"], c["unknown"], c["stable"]+c["degraded"]+c["failing"]+c["unknown"])
}

func composeHealthDigest(base string, now time.Time, items []digestRecording, nas digestNAS) string {
	base = strings.TrimRight(base, "/")
	scheduled := 0
	current := map[string]int{}
	historical := map[string]int{}
	for _, item := range items {
		historical[item.HistoryBucket]++
		if item.Scheduled {
			scheduled++
			current[item.Bucket]++
		}
	}
	var b strings.Builder
	fmt.Fprintf(&b, "Stoarama 8-hour health summary at %s\n\nActive fleet: %d total; %d currently scheduled/live; %d off-window/not assessed.\nCurrent operational health: %s.\nLatest completed-window quality (historical, not current status): %d stable, %d degraded, %d failing, %d unknown.\n\n", now.UTC().Format(time.RFC3339), len(items), scheduled, len(items)-scheduled, digestCounts(items), historical["stable"], historical["degraded"], historical["failing"], historical["unknown"])
	for _, bucket := range []string{"failing", "degraded", "unknown"} {
		if current[bucket] == 0 {
			continue
		}
		fmt.Fprintf(&b, "CURRENT %s (%d)\n", strings.ToUpper(bucket), current[bucket])
		for _, item := range items {
			if item.Scheduled && item.Bucket == bucket {
				fmt.Fprintf(&b, "  #%d %s — %s; historical %s: %s", item.ID, item.Name, item.CurrentCause, item.HistoryBucket, item.HistoryNote)
				if base != "" {
					fmt.Fprintf(&b, " — %s/recordings/%d", base, item.ID)
				}
				b.WriteString("\n")
			}
		}
		b.WriteString("\n")
	}
	stable := []int64{}
	for _, item := range items {
		if item.Scheduled && item.Bucket == "stable" {
			stable = append(stable, item.ID)
		}
	}
	sort.Slice(stable, func(i, j int) bool { return stable[i] < stable[j] })
	fmt.Fprintf(&b, "CURRENT STABLE (%d)\n  IDs: %v\n\n", len(stable), stable)
	b.WriteString("NAS\n")
	state := nasStorageStateAt(nas.Total, nas.Free, nas.ReportedAt, now)
	if state == nasStorageUnknown {
		b.WriteString("  Current capacity telemetry: unknown/stale.\n")
	} else {
		percent := float64(*nas.Free) * 100 / float64(*nas.Total)
		fmt.Fprintf(&b, "  Current: state=%s phase=%s free=%s/%s (%.2f%%) capacity_blocked=%t telemetry_age=%s. Gates: warning <=10%%, critical <=5%%, stale >=15m is unknown.\n", state, nas.Phase, formatDigestBytes(nas.Free), formatDigestBytes(nas.Total), percent, nas.Blocked, now.Sub(nas.ReportedAt.UTC()).Round(time.Second))
	}
	fmt.Fprintf(&b, "  Delivery: server_only=%d; last_batch clips=%d failures=%d completed=%s. Inventory: clips=%d mismatches=%d unmatched=%d reported=%s.\n", nas.ServerOnly, nas.BatchClips, nas.BatchFailures, formatDigestTime(nas.BatchCompletedAt), nas.InventoryClips, nas.InventoryMismatches, nas.Unmatched, formatDigestTime(nas.InventoryAt))
	if nas.CapacityTransitionState != "" && nas.CapacityTransitionAt != nil {
		fmt.Fprintf(&b, "  Latest historical storage-capacity transition: %s at %s (not the current derived state above).\n", nas.CapacityTransitionState, nas.CapacityTransitionAt.UTC().Format(time.RFC3339))
	}
	b.WriteString("\nUrgent alerts remain independent and are not suppressed by this summary.\n")
	return b.String()
}

func formatDigestTime(v *time.Time) string {
	if v == nil {
		return "unknown"
	}
	return v.UTC().Format(time.RFC3339)
}

func formatDigestBytes(v *int64) string {
	if v == nil {
		return "unknown"
	}
	return fmt.Sprintf("%.2f TB", float64(*v)/1e12)
}
