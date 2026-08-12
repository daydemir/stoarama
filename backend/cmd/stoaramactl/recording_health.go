package main

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/daydemir/stoarama/backend/internal/capture"
	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/email"
	"github.com/daydemir/stoarama/backend/internal/r2"
)

// Signal identifiers persisted in recorder_health_alerts.signal and their
// human-readable labels + severities used in the operator email.
const (
	signalContinuousSilentDeath      = "continuous_silent_death"
	signalContinuousWindowEndedEarly = "continuous_window_ended_early"
	signalJobRetriesExhausted        = "job_retries_exhausted"
	signalStuckLease                 = "stuck_lease"
	signalSampledOverdue             = "sampled_overdue"
	signalClipTimestampDrift         = "clip_timestamp_drift"
	signalContinuousCoverageLow      = "continuous_coverage_low"
	signalContinuousOverlap          = "continuous_timeline_overlap"
	signalContinuousLongGap          = "continuous_long_gap"
	signalContinuousFragmented       = "continuous_fragmented"
	signalContinuousLayoutChange     = "continuous_layout_change"
	signalStoredClipInvalid          = "stored_clip_invalid"
	signalPreopenQualityGate         = "preopen_quality_gate"
)

// clipTimestampDriftLimitSec is how far a clip's start may lead its own ingest
// before the timestamp is treated as fiction rather than jitter. It matches
// capture's continuousChainDriftLimit, so the alert and the recorder agree on
// what "drifted" means.
const clipTimestampDriftLimitSec = 90

// Failed or ambiguous email delivery remains retryable, but the live sweep
// must not hammer the provider or duplicate mail every five minutes.
const healthAlertDeliveryRetryBackoff = 15 * time.Minute

const (
	healthAlertSilentDeathMaturity = 4 * time.Minute
	// The hourly full sweep deliberately occupies the missing :30 live slot and
	// evaluates the same live registry under the same timeline lock. Therefore
	// either timeline run refreshes this continuity interval.
	healthAlertDetectionContinuity = 12 * time.Minute
	healthAlertReopenCooldown      = 30 * time.Minute
)

var healthSignalLabels = map[string]string{
	signalContinuousSilentDeath:      "Continuous recording stopped producing clips mid-window",
	signalContinuousWindowEndedEarly: "Continuous recording window ended early with no footage",
	signalJobRetriesExhausted:        "Recording jobs failed after exhausting retries",
	signalStuckLease:                 "Recording job lease stuck (scheduler reclaim may be stalled)",
	signalSampledOverdue:             "Sampled recording is overdue to fire",
	signalClipTimestampDrift:         "Clip timestamps are running ahead of real time (capture chain has lost its epoch)",
	signalContinuousCoverageLow:      "Completed continuous window captured less than 95% of its timeline",
	signalContinuousOverlap:          "Completed continuous window contains overlapping clip timelines",
	signalContinuousLongGap:          "Completed continuous window contains a gap longer than five minutes",
	signalContinuousFragmented:       "Completed continuous window is fragmented by frequent reconnect gaps",
	signalContinuousLayoutChange:     "Adjacent native clips changed media layout and may not losslessly stitch",
	signalStoredClipInvalid:          "Latest stored clip is missing, truncated, or not decodable",
	signalPreopenQualityGate:         "Pre-open source validation needs attention",
}

var healthSignalSeverity = map[string]string{
	signalContinuousSilentDeath:      "CRITICAL",
	signalContinuousWindowEndedEarly: "CRITICAL",
	signalJobRetriesExhausted:        "HIGH",
	signalStuckLease:                 "HIGH",
	signalSampledOverdue:             "HIGH",
	signalClipTimestampDrift:         "CRITICAL",
	signalContinuousCoverageLow:      "CRITICAL",
	signalContinuousOverlap:          "HIGH",
	signalContinuousLongGap:          "HIGH",
	signalContinuousFragmented:       "HIGH",
	signalContinuousLayoutChange:     "HIGH",
	signalStoredClipInvalid:          "CRITICAL",
}

// healthIncident is one detected recording-health problem, enriched with the
// owning org so an operator can act without a lookup.
type healthIncident struct {
	RecordingID int64
	StreamID    int64
	AccountID   int64
	OrgName     string
	OrgEmail    string
	RecName     string
	StreamURL   string
	Signal      string
	Severity    string
	SinceText   string
	Diag        string
	Immediate   bool
}

func runRecordingHealth(ctx context.Context, cfg config.Config, args []string) {
	if len(args) < 1 {
		log.Fatalf("usage: stoaramactl recording-health run [--dry-run --live-only --freshness-min 10] | summary")
	}
	switch args[0] {
	case "run":
		runRecordingHealthRun(ctx, cfg, args[1:])
	case "summary":
		if len(args) != 1 {
			log.Fatalf("usage: stoaramactl recording-health summary")
		}
		runRecordingHealthSummary(ctx, cfg)
	default:
		log.Fatalf("unknown recording-health subcommand: %s", args[0])
	}
}

// runRecordingHealthRun performs one hourly health sweep: detect incidents,
// dedup against recorder_health_alerts, resolve cleared incidents, and email
// operators about the ones that are newly seen or due for a re-notify. In
// --dry-run mode it only detects + prints, never writing rows or sending mail.
func runRecordingHealthRun(ctx context.Context, cfg config.Config, args []string) {
	fs := flag.NewFlagSet("recording-health run", flag.ExitOnError)
	dryRun := fs.Bool("dry-run", false, "detect + print incidents only; do not email or write dedup rows")
	liveOnly := fs.Bool("live-only", false, "check only current capture progress, jobs, and timestamps")
	freshnessMin := fs.Int("freshness-min", 10, "continuous silent-death freshness window in minutes")
	verifyMedia := fs.Bool("verify-media", false, "download and ffprobe the newest adjacent clip pair for each paid active recording with recent retained clips, then decode-verify their concatenation")
	_ = fs.Parse(args)
	if *freshnessMin <= 0 {
		log.Fatalf("--freshness-min must be > 0")
	}
	if *liveOnly && *verifyMedia {
		log.Fatalf("--live-only and --verify-media are mutually exclusive")
	}

	pool := mustOpenPool(ctx, cfg)
	defer pool.Close()
	releaseRunLock, err := acquireRecordingHealthRunLock(ctx, pool, *verifyMedia)
	if err != nil {
		log.Fatalf("acquire recording health run lock: %v", err)
	}
	defer releaseRunLock()

	var incidents []healthIncident
	if *verifyMedia {
		incidents = detectLatestStoredClipHealth(ctx, pool, cfg)
	} else if *liveOnly {
		incidents = detectLiveRecordingHealthIncidents(ctx, pool, *freshnessMin)
	} else {
		incidents = detectRecordingHealthIncidents(ctx, pool, *freshnessMin)
	}
	bySignal := map[string]int{}
	for _, inc := range incidents {
		bySignal[inc.Signal]++
	}

	if *dryRun {
		for _, inc := range incidents {
			fmt.Printf("[dry-run] recording=%d signal=%s severity=%s org=%q name=%q since=%q diag=%q\n",
				inc.RecordingID, inc.Signal, inc.Severity, inc.OrgName, inc.RecName, inc.SinceText, inc.Diag)
		}
		printJSON(map[string]any{
			"dry_run":   true,
			"run_class": healthRunClass(*verifyMedia, *liveOnly),
			"detected":  len(incidents),
			"by_signal": bySignal,
			"notified":  0,
			"emailed":   0,
		})
		return
	}
	now := time.Now()
	if !*verifyMedia && !*liveOnly {
		if err := materializeRecordingWindowHealth(ctx, pool, now); err != nil {
			log.Fatalf("materialize recording window health: %v", err)
		}
	}

	toNotify := make([]healthIncident, 0, len(incidents))
	for _, inc := range incidents {
		newlyInserted, episodeStartedAt, lastAlertedAt, lastDeliveryAttemptAt, err := upsertHealthAlert(ctx, pool, inc.RecordingID, inc.Signal)
		if err != nil {
			log.Fatalf("upsert health alert recording=%d signal=%s: %v", inc.RecordingID, inc.Signal, err)
		}
		if shouldNotifyHealthIncident(inc, newlyInserted, episodeStartedAt, lastAlertedAt, lastDeliveryAttemptAt, now) {
			toNotify = append(toNotify, inc)
		}
	}
	if err := resolveClearedHealthAlerts(ctx, pool, incidents, evaluatedHealthSignals(*verifyMedia, *liveOnly)); err != nil {
		log.Fatalf("resolve cleared health alerts: %v", err)
	}

	emailed := 0
	if len(toNotify) > 0 {
		if err := markHealthAlertsDeliveryAttempted(ctx, pool, toNotify); err != nil {
			log.Fatalf("mark recording health alert delivery attempted: %v", err)
		}
		var err error
		emailed, err = deliverRecordingHealthEmail(ctx, pool, cfg, toNotify)
		if err != nil {
			// last_alerted_at is deliberately unchanged until delivery succeeds,
			// so the next cron retries instead of suppressing this alert for 24h.
			log.Fatalf("deliver recording health alerts: %v", err)
		}
		if err := markHealthAlertsDelivered(ctx, pool, toNotify); err != nil {
			log.Fatalf("mark recording health alerts delivered: %v", err)
		}
	}

	maintenance := transientLedgerMaintenanceResult{}
	if !*verifyMedia && !*liveOnly {
		maintenance, err = maintainTransientLedgers(ctx, pool, time.Now())
		if err != nil {
			// Alert detection and delivery have already completed. Fail the cron now so
			// Render exposes the maintenance error without suppressing health email.
			log.Fatalf("maintain transient database ledgers: %v", err)
		}
	}

	printJSON(map[string]any{
		"run_class":                healthRunClass(*verifyMedia, *liveOnly),
		"dry_run":                  false,
		"detected":                 len(incidents),
		"by_signal":                bySignal,
		"notified":                 len(toNotify),
		"emailed":                  emailed,
		"upload_intents_deleted":   maintenance.UploadIntentsDeleted,
		"idempotency_keys_deleted": maintenance.IdempotencyKeysDeleted,
	})
}

const (
	recordingHealthTimelineAdvisoryLock int64 = 0x53544f4152414d41
	recordingHealthMediaAdvisoryLock    int64 = 0x53544f4152414d42
)

// acquireRecordingHealthRunLock serializes runs within one signal class. The
// expensive daily object/decode verifier deliberately uses a different lock
// from the hourly timeline sweep, so it cannot delay a silent-death alert.
// PostgreSQL owns the session lock, so process death releases it.
func acquireRecordingHealthRunLock(ctx context.Context, pool *pgxpool.Pool, verifyMedia bool) (func(), error) {
	lockID := recordingHealthTimelineAdvisoryLock
	if verifyMedia {
		lockID = recordingHealthMediaAdvisoryLock
	}
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, lockID); err != nil {
		conn.Release()
		return nil, err
	}
	return func() {
		_, _ = conn.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, lockID)
		conn.Release()
	}, nil
}

// shouldNotifyHealthIncident decides whether an incident warrants an email.
// last_alerted_at means successfully delivered, never merely attempted.
func shouldNotifyHealthIncident(inc healthIncident, newlyInserted bool, episodeStartedAt, lastAlertedAt time.Time, lastDeliveryAttemptAt *time.Time, now time.Time) bool {
	if inc.Signal == signalContinuousSilentDeath && !inc.Immediate && now.Before(episodeStartedAt.Add(healthAlertSilentDeathMaturity)) {
		return false
	}
	if !newlyInserted && lastDeliveryAttemptAt != nil && !lastDeliveryAttemptAt.Before(now.Add(-healthAlertDeliveryRetryBackoff)) {
		return false
	}
	if inc.Signal == signalContinuousSilentDeath && lastAlertedAt.Before(episodeStartedAt) {
		return lastAlertedAt.Before(now.Add(-healthAlertReopenCooldown))
	}
	return newlyInserted || lastAlertedAt.Before(now.Add(-24*time.Hour))
}

func upsertHealthAlert(ctx context.Context, pool *pgxpool.Pool, recordingID int64, signal string) (bool, time.Time, time.Time, *time.Time, error) {
	var newlyInserted bool
	var episodeStartedAt time.Time
	var lastAlertedAt time.Time
	var lastDeliveryAttemptAt *time.Time
	err := pool.QueryRow(ctx, `
		INSERT INTO recorder_health_alerts (recording_id, signal, last_alerted_at, episode_started_at, last_detected_at)
		VALUES ($1,$2,'1970-01-01 00:00:00+00'::timestamptz,now(),now())
		ON CONFLICT (recording_id,signal) DO UPDATE
		  SET episode_started_at = CASE
		        WHEN recorder_health_alerts.resolved_at IS NOT NULL
		          OR recorder_health_alerts.last_detected_at IS NULL
		          OR recorder_health_alerts.last_detected_at < now()-$3::interval
		        THEN now() ELSE recorder_health_alerts.episode_started_at END,
		      last_alerted_at = CASE
		        WHEN recorder_health_alerts.resolved_at IS NOT NULL AND $2<>$4
		        THEN '1970-01-01 00:00:00+00'::timestamptz
		        ELSE recorder_health_alerts.last_alerted_at END,
		      last_detected_at = now(),
		      last_delivery_attempt_at = CASE WHEN recorder_health_alerts.resolved_at IS NOT NULL THEN NULL ELSE recorder_health_alerts.last_delivery_attempt_at END,
		      resolved_at = NULL
		RETURNING (xmax=0) AS newly_inserted, episode_started_at, last_alerted_at, last_delivery_attempt_at
	`, recordingID, signal, healthAlertDetectionContinuity.String(), signalContinuousSilentDeath).Scan(&newlyInserted, &episodeStartedAt, &lastAlertedAt, &lastDeliveryAttemptAt)
	return newlyInserted, episodeStartedAt, lastAlertedAt, lastDeliveryAttemptAt, err
}

func markHealthAlertsDeliveryAttempted(ctx context.Context, pool *pgxpool.Pool, incidents []healthIncident) error {
	recIDs := make([]int64, 0, len(incidents))
	signals := make([]string, 0, len(incidents))
	for _, inc := range incidents {
		recIDs = append(recIDs, inc.RecordingID)
		signals = append(signals, inc.Signal)
	}
	_, err := pool.Exec(ctx, `
		UPDATE recorder_health_alerts a SET last_delivery_attempt_at=now()
		FROM unnest($1::bigint[], $2::text[]) AS attempted(recording_id, signal)
		WHERE a.recording_id=attempted.recording_id AND a.signal=attempted.signal
	`, recIDs, signals)
	return err
}

func markHealthAlertsDelivered(ctx context.Context, pool *pgxpool.Pool, incidents []healthIncident) error {
	recIDs := make([]int64, 0, len(incidents))
	signals := make([]string, 0, len(incidents))
	for _, inc := range incidents {
		recIDs = append(recIDs, inc.RecordingID)
		signals = append(signals, inc.Signal)
	}
	_, err := pool.Exec(ctx, `
		UPDATE recorder_health_alerts a SET last_alerted_at=now()
		FROM unnest($1::bigint[], $2::text[]) AS delivered(recording_id, signal)
		WHERE a.recording_id=delivered.recording_id AND a.signal=delivered.signal
	`, recIDs, signals)
	return err
}

func resolveClearedHealthAlerts(ctx context.Context, pool *pgxpool.Pool, incidents []healthIncident, evaluatedSignals []string) error {
	recIDs := make([]int64, 0, len(incidents))
	signals := make([]string, 0, len(incidents))
	for _, inc := range incidents {
		recIDs = append(recIDs, inc.RecordingID)
		signals = append(signals, inc.Signal)
	}
	_, err := pool.Exec(ctx, `
		UPDATE recorder_health_alerts a SET resolved_at=now()
		WHERE a.resolved_at IS NULL
		  AND a.signal=ANY($3::text[])
		  AND NOT EXISTS (
		    SELECT 1 FROM unnest($1::bigint[], $2::text[]) AS d(recording_id, signal)
		    WHERE d.recording_id = a.recording_id AND d.signal = a.signal
		  )
	`, recIDs, signals, evaluatedSignals)
	return err
}

func evaluatedHealthSignals(verifyMedia, liveOnly bool) []string {
	if verifyMedia {
		return []string{signalStoredClipInvalid}
	}
	if liveOnly {
		return liveRecordingHealthSignals()
	}
	signals := []string{
		signalContinuousSilentDeath, signalContinuousWindowEndedEarly,
		signalJobRetriesExhausted, signalStuckLease, signalSampledOverdue,
		signalClipTimestampDrift, signalContinuousCoverageLow,
		signalContinuousOverlap, signalContinuousLongGap, signalContinuousFragmented,
		signalContinuousLayoutChange,
	}
	return signals
}

func liveRecordingHealthSignals() []string {
	detectors := liveRecordingHealthDetectors()
	signals := make([]string, 0, len(detectors))
	for _, detector := range detectors {
		signals = append(signals, detector.signal)
	}
	return signals
}

func healthRunClass(verifyMedia, liveOnly bool) string {
	if verifyMedia {
		return "media"
	}
	if liveOnly {
		return "live"
	}
	return "full"
}

// deliverRecordingHealthEmail sends one summary email per operator recipient.
// Misconfiguration or zero delivery returns an error; the caller exits nonzero
// without advancing last_alerted_at, so the next cron retries.
func deliverRecordingHealthEmail(ctx context.Context, pool *pgxpool.Pool, cfg config.Config, incidents []healthIncident) (int, error) {
	if strings.ToLower(strings.TrimSpace(cfg.EmailProvider)) != "resend" {
		return 0, fmt.Errorf("EMAIL_PROVIDER=%q is not resend; %d incident(s) not delivered", cfg.EmailProvider, len(incidents))
	}

	recipients := operatorRecipients(ctx, pool)
	if len(recipients) == 0 {
		return 0, fmt.Errorf("no operator recipients; %d incident(s) not delivered", len(incidents))
	}

	mailer, err := email.NewSender(email.Config{
		Provider:  cfg.EmailProvider,
		From:      cfg.EmailFrom,
		ReplyTo:   cfg.EmailReplyTo,
		ResendKey: cfg.EmailResendAPIKey,
	})
	if err != nil {
		return 0, fmt.Errorf("init email sender: %w", err)
	}

	subject := composeHealthEmailSubject(incidents)
	body := composeHealthEmailBody(cfg.AppBaseURL, incidents)
	sent := 0
	for _, addr := range recipients {
		if _, err := mailer.Send(ctx, email.Message{
			To:          addr,
			Subject:     subject,
			PlainText:   body,
			MessageType: "recording_health_alert",
		}); err != nil {
			return sent, fmt.Errorf("send recording health alert to %s: %w", addr, err)
		}
		sent++
	}
	return sent, nil
}

func operatorRecipients(ctx context.Context, pool *pgxpool.Pool) []string {
	rows, err := pool.Query(ctx, `SELECT email FROM users WHERE is_operator=true ORDER BY email ASC`)
	if err != nil {
		log.Fatalf("query operator recipients: %v", err)
	}
	defer rows.Close()
	recipients := []string{}
	for rows.Next() {
		var addr string
		if err := rows.Scan(&addr); err != nil {
			log.Fatalf("scan operator recipient: %v", err)
		}
		if addr = strings.TrimSpace(addr); addr != "" {
			recipients = append(recipients, addr)
		}
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("iterate operator recipients: %v", err)
	}
	return recipients
}

func composeHealthEmailSubject(incidents []healthIncident) string {
	if len(incidents) == 1 {
		inc := incidents[0]
		return fmt.Sprintf("[Stoarama] Recording %d unhealthy: %s", inc.RecordingID, healthSignalLabels[inc.Signal])
	}
	return fmt.Sprintf("[Stoarama] %d recording health alert(s)", len(incidents))
}

func composeHealthEmailBody(baseURL string, incidents []healthIncident) string {
	baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	var b strings.Builder
	fmt.Fprintf(&b, "Stoarama detected %d recording health issue(s) this hour.\n\n", len(incidents))
	for i, inc := range incidents {
		if i > 0 {
			b.WriteString("\n")
		}
		fmt.Fprintf(&b, "Recording #%d %q\n", inc.RecordingID, inc.RecName)
		fmt.Fprintf(&b, "  Org:      %s <%s>\n", inc.OrgName, inc.OrgEmail)
		if inc.StreamURL != "" {
			fmt.Fprintf(&b, "  Stream:   %s\n", inc.StreamURL)
		}
		if baseURL != "" && inc.StreamID > 0 {
			fmt.Fprintf(&b, "  Stoarama: %s/streams/%d\n", baseURL, inc.StreamID)
		}
		if baseURL != "" {
			fmt.Fprintf(&b, "  Recording: %s/recordings/%d\n", baseURL, inc.RecordingID)
		}
		fmt.Fprintf(&b, "  Signal:   %s [%s]\n", healthSignalLabels[inc.Signal], inc.Severity)
		if inc.SinceText != "" {
			fmt.Fprintf(&b, "  Since:    %s\n", inc.SinceText)
		}
		if inc.Diag != "" {
			fmt.Fprintf(&b, "  Details:  %s\n", inc.Diag)
		}
	}
	return b.String()
}

// detectRecordingHealthIncidents runs the read-only signal queries and
// returns the union of detected incidents, ordered by severity then recording.
func detectRecordingHealthIncidents(ctx context.Context, pool *pgxpool.Pool, freshnessMin int) []healthIncident {
	incidents := make([]healthIncident, 0, 16)
	incidents = append(incidents, detectContinuousSilentDeath(ctx, pool, freshnessMin)...)
	incidents = append(incidents, detectContinuousWindowEndedEarly(ctx, pool)...)
	incidents = append(incidents, detectJobRetriesExhausted(ctx, pool)...)
	incidents = append(incidents, detectStuckLease(ctx, pool)...)
	incidents = append(incidents, detectSampledOverdue(ctx, pool)...)
	incidents = append(incidents, detectClipTimestampDrift(ctx, pool)...)
	incidents = append(incidents, detectCompletedWindowStitchHealth(ctx, pool)...)
	incidents = append(incidents, detectCompletedWindowLayoutChanges(ctx, pool)...)

	severityRank := map[string]int{"CRITICAL": 0, "HIGH": 1}
	sort.SliceStable(incidents, func(i, j int) bool {
		ri, rj := severityRank[incidents[i].Severity], severityRank[incidents[j].Severity]
		if ri != rj {
			return ri < rj
		}
		return incidents[i].RecordingID < incidents[j].RecordingID
	})
	return incidents
}

// detectLiveRecordingHealthIncidents is the cheap, current-window subset used
// between hourly full sweeps. It never scans completed-window clip timelines,
// downloads objects, materializes summaries, or runs ledger maintenance.
func detectLiveRecordingHealthIncidents(ctx context.Context, pool *pgxpool.Pool, freshnessMin int) []healthIncident {
	incidents := make([]healthIncident, 0, 8)
	for _, detector := range liveRecordingHealthDetectors() {
		incidents = append(incidents, detector.detect(ctx, pool, freshnessMin)...)
	}

	severityRank := map[string]int{"CRITICAL": 0, "HIGH": 1}
	sort.SliceStable(incidents, func(i, j int) bool {
		ri, rj := severityRank[incidents[i].Severity], severityRank[incidents[j].Severity]
		if ri != rj {
			return ri < rj
		}
		return incidents[i].RecordingID < incidents[j].RecordingID
	})
	return incidents
}

type liveHealthDetector struct {
	signal string
	detect func(context.Context, *pgxpool.Pool, int) []healthIncident
}

// Keep detector execution and cleared-signal resolution derived from one list:
// omitting a detector while still resolving its signal would falsely close a
// real incident on every live run.
func liveRecordingHealthDetectors() []liveHealthDetector {
	return []liveHealthDetector{
		{signalContinuousSilentDeath, detectContinuousSilentDeath},
		{signalContinuousWindowEndedEarly, func(ctx context.Context, pool *pgxpool.Pool, _ int) []healthIncident {
			return detectContinuousWindowEndedEarly(ctx, pool)
		}},
		{signalJobRetriesExhausted, func(ctx context.Context, pool *pgxpool.Pool, _ int) []healthIncident {
			return detectJobRetriesExhausted(ctx, pool)
		}},
		{signalStuckLease, func(ctx context.Context, pool *pgxpool.Pool, _ int) []healthIncident {
			return detectStuckLease(ctx, pool)
		}},
		{signalClipTimestampDrift, func(ctx context.Context, pool *pgxpool.Pool, _ int) []healthIncident {
			return detectClipTimestampDrift(ctx, pool)
		}},
	}
}

func humanSince(t *time.Time) string {
	if t == nil {
		return "never"
	}
	return t.UTC().Format(time.RFC3339)
}

func continuousSilenceDetail(observedAt, winOpen time.Time, latestMediaEnd, latestIngestedAt, lastClipAt *time.Time, mediaLagSec int64, freshness time.Duration, lastErr string) (string, string) {
	mediaLag := ""
	if latestMediaEnd != nil {
		mediaLag = (time.Duration(mediaLagSec) * time.Second).Round(time.Second).String()
	}
	transport := "no_recent_ingest"
	if latestIngestedAt != nil && observedAt.Sub(*latestIngestedAt) <= freshness {
		transport = "fresh_ingest_stale_media"
	}
	return fmt.Sprintf("window opened %s, latest media ended %s, latest ingest %s",
			winOpen.UTC().Format(time.RFC3339), humanSince(latestMediaEnd), humanSince(latestIngestedAt)),
		diagText("capture_state", transport, "media_behind", mediaLag, "recording_last_clip", humanSince(lastClipAt), "last_error", lastErr)
}

type stitchWindow struct {
	incidentBase healthIncident
	open         time.Time
	close        time.Time
	clips        [][2]time.Time
}

type stitchWindowMetrics struct {
	coveragePct    float64
	covered        time.Duration
	overlapClips   int
	overlapSeconds float64
	maxGap         time.Duration
	gapClips       int
	// Threshold counts use the exact uncovered UNION intervals, including the
	// leading and trailing window edges. They intentionally do not reuse
	// gapClips: that operational counter ignores edge gaps and joins seams up to
	// one second, while qualification grades require strict >30s and >5m facts.
	gapsOver30s int
	gapsOver5m  int
	longestRun  time.Duration
}

// detectCompletedWindowStitchHealth scores the most recently completed window
// for every active continuous recording. Coverage is the UNION of clipped media
// intervals, so overlaps never inflate it above 100%. Overlaps and internal gaps
// are reported separately: operators can distinguish duplicated capture chains
// from honest missing footage, and researchers know exactly when a long-video or
// CV track must break.
func detectCompletedWindowStitchHealth(ctx context.Context, pool *pgxpool.Pool) []healthIncident {
	rows, err := pool.Query(ctx, `
		WITH latest AS (
		  SELECT DISTINCT ON (r.id)
		    r.id, COALESCE(r.stream_id,0) AS stream_id, r.account_id,
		    r.name AS recording_name, r.stream_url,
		    acc.name AS org_name, acc.email AS org_email, j.fire_at, j.window_end_at
		  FROM recordings r
		  JOIN accounts acc ON acc.id=r.account_id
		  JOIN account_billing b ON b.account_id=r.account_id AND b.has_payment_method=true
		  JOIN recording_jobs j ON j.recording_id=r.id AND j.kind='continuous_window'
		  WHERE r.status='active' AND r.mode='continuous'
		    AND j.window_end_at <= now()
		    AND j.window_end_at >= now()-interval '48 hours'
		  ORDER BY r.id, j.window_end_at DESC, j.id DESC
		)
		SELECT l.id,l.stream_id,l.account_id,l.recording_name,l.stream_url,l.org_name,l.org_email,
		       l.fire_at,l.window_end_at,c.clip_start_at,c.clip_end_at
		FROM latest l
		LEFT JOIN recording_clips c ON c.recording_id=l.id
		  AND c.clip_end_at>l.fire_at AND c.clip_start_at<l.window_end_at
		ORDER BY l.id,c.clip_start_at,c.id
	`)
	if err != nil {
		log.Fatalf("signal completed_window_stitch_health: %v", err)
	}
	defer rows.Close()
	windows := map[int64]*stitchWindow{}
	order := []int64{}
	for rows.Next() {
		var id, streamID, accountID int64
		var recName, streamURL, orgName, orgEmail string
		var open, close time.Time
		var clipStart, clipEnd *time.Time
		if err := rows.Scan(&id, &streamID, &accountID, &recName, &streamURL, &orgName, &orgEmail,
			&open, &close, &clipStart, &clipEnd); err != nil {
			log.Fatalf("scan completed_window_stitch_health: %v", err)
		}
		win := windows[id]
		if win == nil {
			win = &stitchWindow{incidentBase: healthIncident{
				RecordingID: id, StreamID: streamID, AccountID: accountID,
				OrgName: orgName, OrgEmail: orgEmail, RecName: recName, StreamURL: streamURL,
			}, open: open, close: close}
			windows[id] = win
			order = append(order, id)
		}
		if clipStart != nil && clipEnd != nil {
			win.clips = append(win.clips, [2]time.Time{*clipStart, *clipEnd})
		}
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("iterate completed_window_stitch_health: %v", err)
	}

	out := []healthIncident{}
	for _, id := range order {
		win := windows[id]
		m := measureStitchWindow(win.open, win.close, win.clips)
		since := fmt.Sprintf("completed window %s to %s", win.open.UTC().Format(time.RFC3339), win.close.UTC().Format(time.RFC3339))
		if m.coveragePct < 95 {
			inc := win.incidentBase
			inc.Signal, inc.Severity, inc.SinceText = signalContinuousCoverageLow, healthSignalSeverity[signalContinuousCoverageLow], since
			inc.Diag = diagText("coverage", fmt.Sprintf("%.2f%%", m.coveragePct), "longest_run", m.longestRun.Round(time.Second).String())
			out = append(out, inc)
		}
		if m.overlapClips > 0 {
			inc := win.incidentBase
			inc.Signal, inc.Severity, inc.SinceText = signalContinuousOverlap, healthSignalSeverity[signalContinuousOverlap], since
			inc.Diag = diagText("overlap_clips", fmt.Sprint(m.overlapClips), "overlap_time", time.Duration(m.overlapSeconds*float64(time.Second)).Round(time.Second).String())
			out = append(out, inc)
		}
		if m.maxGap > 5*time.Minute {
			inc := win.incidentBase
			inc.Signal, inc.Severity, inc.SinceText = signalContinuousLongGap, healthSignalSeverity[signalContinuousLongGap], since
			inc.Diag = diagText("largest_gap", m.maxGap.Round(time.Second).String(), "longest_run", m.longestRun.Round(time.Second).String())
			out = append(out, inc)
		}
		if m.gapClips > 10 {
			inc := win.incidentBase
			inc.Signal, inc.Severity, inc.SinceText = signalContinuousFragmented, healthSignalSeverity[signalContinuousFragmented], since
			inc.Diag = diagText("reconnect_gaps", fmt.Sprint(m.gapClips), "largest_gap", m.maxGap.Round(time.Second).String())
			out = append(out, inc)
		}
	}
	return out
}

// detectCompletedWindowLayoutChanges checks every adjacent seam in the latest
// completed window using metadata already measured by ffprobe before ingest.
// It is O(clips) SQL metadata work and never downloads, rewrites, or normalizes
// source-native media.
func detectCompletedWindowLayoutChanges(ctx context.Context, pool *pgxpool.Pool) []healthIncident {
	rows, err := pool.Query(ctx, `
		WITH latest AS (
		  SELECT DISTINCT ON (r.id)
		    r.id,COALESCE(r.stream_id,0) AS stream_id,r.account_id,r.name AS recording_name,r.stream_url,
		    acc.name AS org_name,acc.email AS org_email,j.fire_at,j.window_end_at
		  FROM recordings r
		  JOIN accounts acc ON acc.id=r.account_id
		  JOIN account_billing b ON b.account_id=r.account_id AND b.has_payment_method=true
		  JOIN recording_jobs j ON j.recording_id=r.id AND j.kind='continuous_window'
		  WHERE r.status='active' AND r.mode='continuous' AND j.window_end_at<=now()
		    AND j.window_end_at>=now()-interval '48 hours'
		  ORDER BY r.id,j.window_end_at DESC,j.id DESC
		), ordered AS (
		  SELECT l.*,c.id AS clip_id,COALESCE(c.video_codec,'') AS video_codec,COALESCE(c.audio_codec,'') AS audio_codec,
		    c.audio_present,COALESCE(c.actual_fps,0) AS actual_fps,
		    COALESCE(c.video_width,0) AS video_width,COALESCE(c.video_height,0) AS video_height,
		    lag(c.id) OVER seam AS previous_clip_id,
		    lag(COALESCE(c.video_codec,'')) OVER seam AS previous_video_codec,
		    lag(COALESCE(c.audio_codec,'')) OVER seam AS previous_audio_codec,
		    lag(c.audio_present) OVER seam AS previous_audio_present,
		    lag(COALESCE(c.actual_fps,0)) OVER seam AS previous_actual_fps,
		    lag(COALESCE(c.video_width,0)) OVER seam AS previous_video_width,
		    lag(COALESCE(c.video_height,0)) OVER seam AS previous_video_height
		  FROM latest l
		  JOIN recording_clips c ON c.recording_id=l.id
		    AND c.clip_end_at>l.fire_at AND c.clip_start_at<l.window_end_at
		  WINDOW seam AS (PARTITION BY l.id ORDER BY c.clip_start_at,c.id)
		), changed AS (
		  SELECT * FROM ordered
		  WHERE previous_clip_id IS NOT NULL AND (
		    video_codec IS DISTINCT FROM previous_video_codec OR
		    audio_present IS DISTINCT FROM previous_audio_present OR
		    (audio_present AND audio_codec IS DISTINCT FROM previous_audio_codec) OR
		    (video_width>0 AND previous_video_width>0 AND
		      (video_width<>previous_video_width OR video_height<>previous_video_height))
		  )
		)
		SELECT DISTINCT ON (id)
		  id,stream_id,account_id,recording_name,stream_url,org_name,org_email,fire_at,window_end_at,
		  previous_clip_id,clip_id,previous_video_codec,video_codec,previous_audio_present,audio_present,
		  previous_audio_codec,audio_codec,previous_actual_fps,actual_fps,
		  previous_video_width,previous_video_height,video_width,video_height
		FROM changed ORDER BY id,clip_id
	`)
	if err != nil {
		log.Fatalf("signal continuous_layout_change: %v", err)
	}
	defer rows.Close()
	out := []healthIncident{}
	for rows.Next() {
		var inc healthIncident
		var open, close time.Time
		var previousClipID, clipID int64
		var previousVideoCodec, videoCodec, previousAudioCodec, audioCodec string
		var previousAudioPresent, audioPresent bool
		var previousFPS, actualFPS float64
		var previousWidth, previousHeight, width, height int
		if err := rows.Scan(&inc.RecordingID, &inc.StreamID, &inc.AccountID, &inc.RecName, &inc.StreamURL,
			&inc.OrgName, &inc.OrgEmail, &open, &close, &previousClipID, &clipID,
			&previousVideoCodec, &videoCodec, &previousAudioPresent, &audioPresent,
			&previousAudioCodec, &audioCodec, &previousFPS, &actualFPS,
			&previousWidth, &previousHeight, &width, &height); err != nil {
			log.Fatalf("scan continuous_layout_change: %v", err)
		}
		inc.Signal, inc.Severity = signalContinuousLayoutChange, healthSignalSeverity[signalContinuousLayoutChange]
		inc.SinceText = fmt.Sprintf("completed window %s to %s", open.UTC().Format(time.RFC3339), close.UTC().Format(time.RFC3339))
		inc.Diag = diagText("seam", fmt.Sprintf("%d->%d", previousClipID, clipID),
			"video", fmt.Sprintf("%s/%s", previousVideoCodec, videoCodec),
			"audio", fmt.Sprintf("%t:%s/%t:%s", previousAudioPresent, previousAudioCodec, audioPresent, audioCodec),
			"fps", fmt.Sprintf("%.3f/%.3f", previousFPS, actualFPS),
			"dimensions", fmt.Sprintf("%d×%d/%d×%d", previousWidth, previousHeight, width, height))
		out = append(out, inc)
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("iterate continuous_layout_change: %v", err)
	}
	return out
}

func measureStitchWindow(open, close time.Time, clips [][2]time.Time) stitchWindowMetrics {
	const joinTolerance = time.Second
	intervals := make([][2]time.Time, 0, len(clips))
	for _, clip := range clips {
		start, end := clip[0], clip[1]
		if start.Before(open) {
			start = open
		}
		if end.After(close) {
			end = close
		}
		if end.After(start) {
			intervals = append(intervals, [2]time.Time{start, end})
		}
	}
	sort.Slice(intervals, func(i, j int) bool { return intervals[i][0].Before(intervals[j][0]) })
	m := stitchWindowMetrics{}
	if len(intervals) == 0 && close.After(open) {
		m.maxGap = close.Sub(open)
		m.countQualificationGap(m.maxGap)
	}
	var covered time.Duration
	var unionStart, unionEnd time.Time
	var runStart, runEnd time.Time
	if len(intervals) > 0 {
		leading := intervals[0][0].Sub(open)
		m.maxGap = leading
		m.countQualificationGap(leading)
	}
	for _, iv := range intervals {
		if unionEnd.IsZero() {
			unionStart, unionEnd = iv[0], iv[1]
			runStart, runEnd = iv[0], iv[1]
			continue
		}
		// Coverage and overlaps are exact. Tolerance is used only to decide
		// whether a tiny seam should split the operational "longest run".
		if iv[0].Before(unionEnd) {
			m.overlapClips++
			overlapEnd := iv[1]
			if overlapEnd.After(unionEnd) {
				overlapEnd = unionEnd
			}
			m.overlapSeconds += overlapEnd.Sub(iv[0]).Seconds()
		}
		if !iv[0].After(unionEnd) {
			if iv[1].After(unionEnd) {
				unionEnd = iv[1]
			}
		} else {
			covered += unionEnd.Sub(unionStart)
			gap := iv[0].Sub(unionEnd)
			m.countQualificationGap(gap)
			if gap > m.maxGap {
				m.maxGap = gap
			}
			unionStart, unionEnd = iv[0], iv[1]
		}
		if !iv[0].After(runEnd.Add(joinTolerance)) {
			if iv[1].After(runEnd) {
				runEnd = iv[1]
			}
		} else {
			if run := runEnd.Sub(runStart); run > m.longestRun {
				m.longestRun = run
			}
			m.gapClips++
			runStart, runEnd = iv[0], iv[1]
		}
	}
	if !unionEnd.IsZero() {
		covered += unionEnd.Sub(unionStart)
		trail := close.Sub(unionEnd)
		m.countQualificationGap(trail)
		if trail > m.maxGap {
			m.maxGap = trail
		}
	}
	if !runEnd.IsZero() {
		run := runEnd.Sub(runStart)
		if run > m.longestRun {
			m.longestRun = run
		}
	}
	if expected := close.Sub(open); expected > 0 {
		m.covered = covered
		m.coveragePct = 100 * float64(covered) / float64(expected)
	}
	return m
}

func (m *stitchWindowMetrics) countQualificationGap(gap time.Duration) {
	if gap > 30*time.Second {
		m.gapsOver30s++
	}
	if gap > 5*time.Minute {
		m.gapsOver5m++
	}
}

const (
	mediaHealthRecordingTimeout = 5 * time.Minute
	mediaHealthDiagnosticLimit  = 4096
)

type storedClipObject struct {
	clipID, expectedSize, destinationID                    int64
	objectKey, sha256, endpoint, region, bucket, accessKey string
	secretEnc                                              []byte
}

type storedClipCandidate struct {
	recordingID, streamID, accountID      int64
	recName, streamURL, orgName, orgEmail string
	latest                                storedClipObject
	previous                              *storedClipObject
}

// detectLatestStoredClipHealth verifies one recent object per active recording.
// Ingest already HEAD-checks an upload before committing its row; this daily
// sweep catches later disappearance, byte truncation, or a formally present MP4
// whose trailer/streams cannot be decoded. It reads and probes only—never rewrites
// or normalizes media, so quality and source-native FPS remain untouched.
func detectLatestStoredClipHealth(ctx context.Context, pool *pgxpool.Pool, cfg config.Config) []healthIncident {
	cipher := mustBuildStorageCipher(cfg)
	rows, err := pool.Query(ctx, `
		SELECT
		  r.id,COALESCE(r.stream_id,0),r.account_id,r.name,r.stream_url,
		  acc.name,acc.email,c.id,c.object_key,c.size_bytes,COALESCE(c.sha256,''),
		  sd.id,sd.endpoint,sd.region,sd.bucket,sd.access_key_id,sd.secret_access_key_enc,
		  prev.id,prev.object_key,prev.size_bytes,prev.sha256,
		  psd.id,psd.endpoint,psd.region,psd.bucket,psd.access_key_id,psd.secret_access_key_enc
		FROM recordings r
		JOIN accounts acc ON acc.id=r.account_id
		JOIN account_billing b ON b.account_id=r.account_id AND b.has_payment_method=true
		JOIN LATERAL (
		  SELECT c.* FROM recording_clips c
		  WHERE c.recording_id=r.id AND c.purged_at IS NULL AND c.released_at IS NULL
		    AND c.created_at>=now()-interval '24 hours'
		  ORDER BY c.clip_start_at DESC,c.id DESC LIMIT 1
		) c ON true
		JOIN storage_destinations sd ON sd.id=c.storage_destination_id
		LEFT JOIN LATERAL (
		  SELECT p.id,p.object_key,p.size_bytes,COALESCE(p.sha256,'') AS sha256,p.storage_destination_id
		  FROM recording_clips p
		  WHERE p.recording_id=r.id AND p.purged_at IS NULL AND p.released_at IS NULL AND p.id<>c.id
		    AND (
		      (c.capture_lease_token IS NOT NULL AND c.capture_sequence IS NOT NULL
		       AND p.capture_lease_token=c.capture_lease_token AND p.capture_sequence<c.capture_sequence)
		      OR
		      ((c.capture_lease_token IS NULL OR c.capture_sequence IS NULL
		        OR p.capture_lease_token IS DISTINCT FROM c.capture_lease_token)
		       AND (p.clip_start_at,p.id)<(c.clip_start_at,c.id))
		    )
		  ORDER BY (p.capture_lease_token=c.capture_lease_token AND p.capture_sequence IS NOT NULL) DESC,
		           CASE WHEN p.capture_lease_token=c.capture_lease_token THEN p.capture_sequence END DESC NULLS LAST,
		           p.clip_start_at DESC,p.id DESC LIMIT 1
		) prev ON true
		LEFT JOIN storage_destinations psd ON psd.id=prev.storage_destination_id
		WHERE r.status='active' AND now()>=r.start_at AND now()<COALESCE(r.end_at,'infinity'::timestamptz)
		ORDER BY r.id
	`)
	if err != nil {
		log.Fatalf("signal stored_clip_invalid: %v", err)
	}
	candidates := []storedClipCandidate{}
	for rows.Next() {
		var c storedClipCandidate
		var prevID, prevSize, prevDestID *int64
		var prevKey, prevSHA, prevEndpoint, prevRegion, prevBucket, prevAccessKey *string
		var prevSecret []byte
		if err := rows.Scan(&c.recordingID, &c.streamID, &c.accountID, &c.recName, &c.streamURL,
			&c.orgName, &c.orgEmail, &c.latest.clipID, &c.latest.objectKey, &c.latest.expectedSize, &c.latest.sha256,
			&c.latest.destinationID, &c.latest.endpoint, &c.latest.region, &c.latest.bucket, &c.latest.accessKey, &c.latest.secretEnc,
			&prevID, &prevKey, &prevSize, &prevSHA,
			&prevDestID, &prevEndpoint, &prevRegion, &prevBucket, &prevAccessKey, &prevSecret); err != nil {
			log.Fatalf("scan stored_clip_invalid: %v", err)
		}
		if prevID != nil {
			c.previous = &storedClipObject{clipID: *prevID, expectedSize: *prevSize, destinationID: *prevDestID,
				objectKey: *prevKey, sha256: *prevSHA, endpoint: *prevEndpoint, region: *prevRegion,
				bucket: *prevBucket, accessKey: *prevAccessKey, secretEnc: prevSecret}
		}
		candidates = append(candidates, c)
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("iterate stored_clip_invalid: %v", err)
	}
	rows.Close()

	out := []healthIncident{}
	for _, candidate := range candidates {
		candidateCtx, cancel := context.WithTimeout(ctx, mediaHealthRecordingTimeout)
		recID, streamID, accountID := candidate.recordingID, candidate.streamID, candidate.accountID
		recName, streamURL, orgName, orgEmail := candidate.recName, candidate.streamURL, candidate.orgName, candidate.orgEmail
		latest := candidate.latest
		base := healthIncident{
			RecordingID: recID, StreamID: streamID, AccountID: accountID,
			OrgName: orgName, OrgEmail: orgEmail, RecName: recName, StreamURL: streamURL,
			Signal: signalStoredClipInvalid, Severity: healthSignalSeverity[signalStoredClipInvalid],
			SinceText: fmt.Sprintf("latest clip id=%d", latest.clipID),
		}
		problemFor := func(stage string, failedClipID, failedDestinationID int64, verifyErr error) {
			inc := base
			inc.Diag = diagText("stage", stage, "clip_id", fmt.Sprint(failedClipID), "destination_id", fmt.Sprint(failedDestinationID), "error", boundedHealthDiagnostic(verifyErr.Error()))
			out = append(out, inc)
		}
		problem := func(stage string, verifyErr error) { problemFor(stage, latest.clipID, latest.destinationID, verifyErr) }
		if cipher == nil {
			problem("configuration", fmt.Errorf("STORAGE_CRED_KEY is unset"))
			cancel()
			continue
		}
		secret, err := cipher.Decrypt(latest.secretEnc)
		if err != nil {
			problem("decrypt_destination", err)
			cancel()
			continue
		}
		client, err := r2.New(candidateCtx, r2.Config{AccessKey: latest.accessKey, SecretKey: string(secret), Region: latest.region, Bucket: latest.bucket, Endpoint: latest.endpoint})
		if err != nil {
			problem("storage_client", err)
			cancel()
			continue
		}
		latestPath, err := downloadAndProbeStoredClip(candidateCtx, client, latest.objectKey, latest.expectedSize, latest.sha256)
		if err != nil {
			problem("latest_clip", err)
			cancel()
			continue
		}
		if candidate.previous == nil {
			_ = os.Remove(latestPath)
			cancel()
			continue
		}
		previous := *candidate.previous
		prevSecret, err := cipher.Decrypt(previous.secretEnc)
		if err != nil {
			_ = os.Remove(latestPath)
			problemFor("decrypt_predecessor_destination", previous.clipID, previous.destinationID, err)
			cancel()
			continue
		}
		prevClient, err := r2.New(candidateCtx, r2.Config{AccessKey: previous.accessKey, SecretKey: string(prevSecret), Region: previous.region, Bucket: previous.bucket, Endpoint: previous.endpoint})
		if err != nil {
			_ = os.Remove(latestPath)
			problemFor("predecessor_storage_client", previous.clipID, previous.destinationID, err)
			cancel()
			continue
		}
		prevPath, err := downloadAndProbeStoredClip(candidateCtx, prevClient, previous.objectKey, previous.expectedSize, previous.sha256)
		if err != nil {
			_ = os.Remove(latestPath)
			problemFor("predecessor_clip", previous.clipID, previous.destinationID, err)
			cancel()
			continue
		}
		concatErr := capture.ValidateConcatFiles(candidateCtx, []string{prevPath, latestPath})
		_ = os.Remove(prevPath)
		_ = os.Remove(latestPath)
		if concatErr != nil {
			problem("concat_decode", concatErr)
		}
		cancel()
	}
	return out
}

func downloadAndProbeStoredClip(ctx context.Context, client *r2.Client, objectKey string, expectedSize int64, expectedSHA string) (string, error) {
	head, err := client.Head(ctx, objectKey)
	if err != nil {
		return "", fmt.Errorf("head: %w", err)
	}
	if head.SizeBytes != expectedSize {
		return "", fmt.Errorf("size: stored=%d database=%d", head.SizeBytes, expectedSize)
	}
	body, err := client.Open(ctx, objectKey)
	if err != nil {
		return "", fmt.Errorf("download: %w", err)
	}
	tmp, err := os.CreateTemp("", "stoarama-media-health-*.mp4")
	if err != nil {
		_ = body.Close()
		return "", fmt.Errorf("tempfile: %w", err)
	}
	hasher := sha256.New()
	copied, copyErr := io.Copy(io.MultiWriter(tmp, hasher), io.LimitReader(body, expectedSize+1))
	bodyErr := body.Close()
	closeErr := tmp.Close()
	if copyErr != nil || bodyErr != nil || closeErr != nil {
		_ = os.Remove(tmp.Name())
		return "", fmt.Errorf("download: %w", errors.Join(copyErr, bodyErr, closeErr))
	}
	if copied != expectedSize {
		_ = os.Remove(tmp.Name())
		return "", fmt.Errorf("downloaded size=%d database=%d", copied, expectedSize)
	}
	if err := verifyStoredClipSHA(hasher.Sum(nil), expectedSHA); err != nil {
		_ = os.Remove(tmp.Name())
		return "", err
	}
	if err := capture.ValidateSegmentFile(ctx, tmp.Name()); err != nil {
		_ = os.Remove(tmp.Name())
		return "", fmt.Errorf("ffprobe: %w", err)
	}
	return tmp.Name(), nil
}

func verifyStoredClipSHA(actual []byte, expected string) error {
	expected = strings.ToLower(strings.TrimSpace(expected))
	if expected == "" {
		return nil // legacy rows predate stored checksums; size+decode still apply.
	}
	want, err := hex.DecodeString(expected)
	if err != nil || len(want) != sha256.Size {
		return fmt.Errorf("invalid stored sha256 metadata")
	}
	if subtle.ConstantTimeCompare(actual, want) != 1 {
		return fmt.Errorf("sha256 mismatch: downloaded object differs from ingest metadata")
	}
	return nil
}

func boundedHealthDiagnostic(value string) string {
	value = strings.TrimSpace(value)
	if len(value) <= mediaHealthDiagnosticLimit {
		return value
	}
	cut := mediaHealthDiagnosticLimit
	for cut > 0 && !utf8.RuneStart(value[cut]) {
		cut--
	}
	return value[:cut] + "…[truncated]"
}

// detectClipTimestampDrift finds recordings whose newest clip is stamped for an
// instant that had not happened yet when the clip was ingested.
//
// Continuous capture chains each segment's start from the previous segment's end
// along the media timeline, which assumes a live source never hands ffmpeg more
// media than wall-clock time. DVR-backed playlists break that assumption and the
// chain compounds without bound. Nothing about this looks like a failure: clips
// keep arriving, coverage reads at or above 100%, and every existing signal here
// stays quiet, because the recording is producing footage -- just labelled for
// the future.
//
// It is silent AND destructive. The Plaza naming profile derives each clip's
// stored path from clip_start_at, so drifted clips are filed under the wrong day,
// and no stitch can place them on a real timeline afterwards. On 2026-08-01 stream
// 14782 (Zanzibar) drifted roughly five hours ahead and filed 84 clips under the
// following day before anyone noticed, because nothing was watching for it.
//
// A healthy clip is always stamped BEFORE it is ingested (capture, then upload),
// so leading at all is already wrong; the limit only tolerates end-of-window clips
// truncated to a few seconds, which routinely lead by a handful.
//
// It reports the WORST clip of the last hour rather than the newest one. A drifted
// chain re-anchors whenever its ffmpeg restarts, so the most recent clip is often
// healthy again while the hour behind it is full of clips filed into the future --
// checking only the newest clip would have called Zanzibar fine at exactly the
// moment its footage was being misfiled.
func detectClipTimestampDrift(ctx context.Context, pool *pgxpool.Pool) []healthIncident {
	rows, err := pool.Query(ctx, `
		SELECT r.id, COALESCE(r.stream_id,0), r.account_id, r.name, r.stream_url,
		       acc.name, acc.email,
		       worst.clip_start_at, worst.created_at,
		       EXTRACT(EPOCH FROM (worst.clip_start_at - worst.created_at))::bigint AS lead_sec
		FROM recordings r
		JOIN accounts acc ON acc.id = r.account_id
		JOIN LATERAL (
		  SELECT c.clip_start_at, c.created_at
		  FROM recording_clips c
		  WHERE c.recording_id = r.id
		    AND c.created_at > now() - interval '1 hour'
		    -- Every positive drift candidate necessarily starts after this bound:
		    -- created_at > now()-1h AND clip_start_at > created_at+90s implies
		    -- clip_start_at > now()-1h. Stating it lets PostgreSQL use the existing
		    -- (recording_id, clip_start_at DESC) index instead of walking years of
		    -- retained clips for every active recording.
		    AND c.clip_start_at > now() - interval '1 hour'
		  ORDER BY (c.clip_start_at - c.created_at) DESC
		  LIMIT 1
		) worst ON true
		WHERE r.status='active' AND r.mode='continuous'
		  AND worst.clip_start_at > worst.created_at + make_interval(secs => $1)
		ORDER BY lead_sec DESC
	`, clipTimestampDriftLimitSec)
	if err != nil {
		log.Fatalf("signal clip_timestamp_drift: %v", err)
	}
	defer rows.Close()
	out := []healthIncident{}
	for rows.Next() {
		var (
			id, streamID, accountID  int64
			leadSec                  int64
			name, streamURL, orgName string
			orgEmail                 string
			clipStartAt, createdAt   time.Time
		)
		if err := rows.Scan(&id, &streamID, &accountID, &name, &streamURL, &orgName, &orgEmail,
			&clipStartAt, &createdAt, &leadSec); err != nil {
			log.Fatalf("scan clip_timestamp_drift: %v", err)
		}
		out = append(out, healthIncident{
			RecordingID: id, StreamID: streamID, AccountID: accountID, OrgName: orgName, OrgEmail: orgEmail,
			RecName: name, StreamURL: streamURL,
			Signal: signalClipTimestampDrift, Severity: healthSignalSeverity[signalClipTimestampDrift],
			SinceText: fmt.Sprintf("worst clip in the last hour is stamped %s but was ingested %s",
				clipStartAt.UTC().Format(time.RFC3339), createdAt.UTC().Format(time.RFC3339)),
			Diag: diagText("ahead_of_real_time", fmt.Sprintf("%ds", leadSec)),
		})
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("iterate clip_timestamp_drift: %v", err)
	}
	return out
}

func detectContinuousSilentDeath(ctx context.Context, pool *pgxpool.Pool, freshnessMin int) []healthIncident {
	rows, err := pool.Query(ctx, `
		WITH cont AS (
		  SELECT r.id, COALESCE(r.stream_id,0) AS stream_id, r.account_id, r.name, r.stream_url, r.cron_timezone, r.last_clip_at, r.last_error_text,
		    ((now() AT TIME ZONE r.cron_timezone)::date + r.daily_window_start) AT TIME ZONE r.cron_timezone AS win_open,
		    ((now() AT TIME ZONE r.cron_timezone)::date + r.daily_window_end)   AT TIME ZONE r.cron_timezone AS win_close
		  FROM recordings r JOIN account_billing b ON b.account_id=r.account_id
		  WHERE r.status='active' AND r.mode='continuous' AND b.has_payment_method=true
		    AND now()>=r.start_at AND now()<COALESCE(r.end_at,'infinity'::timestamptz))
		SELECT c.id,c.stream_id,c.account_id,c.name,c.stream_url,c.win_open,c.win_close,c.last_clip_at,c.last_error_text,
		       acc.name,acc.email,media.latest_media_end,media.latest_ingested_at,
		       COALESCE(EXTRACT(EPOCH FROM (now()-media.latest_media_end))::bigint,0),now()
		FROM cont c JOIN accounts acc ON acc.id=c.account_id
		LEFT JOIN LATERAL (
		  SELECT max(cl.clip_end_at) AS latest_media_end,max(cl.created_at) AS latest_ingested_at
		  FROM recording_clips cl WHERE cl.recording_id=c.id AND cl.clip_start_at>=c.win_open
		) media ON true
		WHERE now() >= c.win_open + make_interval(mins=>$1) AND now() < c.win_close
		  AND NOT EXISTS (SELECT 1 FROM recording_clips cl WHERE cl.recording_id=c.id AND cl.clip_start_at >= now()-make_interval(mins=>$1))
	`, freshnessMin)
	if err != nil {
		log.Fatalf("signal continuous_silent_death: %v", err)
	}
	defer rows.Close()
	out := []healthIncident{}
	for rows.Next() {
		var (
			id, streamID, accountID  int64
			name, streamURL, orgName string
			orgEmail, lastErr        string
			winOpen, winClose        time.Time
			lastClipAt               *time.Time
			latestMediaEnd           *time.Time
			latestIngestedAt         *time.Time
			mediaLagSec              int64
			observedAt               time.Time
		)
		if err := rows.Scan(&id, &streamID, &accountID, &name, &streamURL, &winOpen, &winClose, &lastClipAt, &lastErr,
			&orgName, &orgEmail, &latestMediaEnd, &latestIngestedAt, &mediaLagSec, &observedAt); err != nil {
			log.Fatalf("scan continuous_silent_death: %v", err)
		}
		sinceText, diag := continuousSilenceDetail(observedAt, winOpen, latestMediaEnd, latestIngestedAt, lastClipAt, mediaLagSec, time.Duration(freshnessMin)*time.Minute, lastErr)
		out = append(out, healthIncident{
			RecordingID: id, StreamID: streamID, AccountID: accountID, OrgName: orgName, OrgEmail: orgEmail,
			RecName: name, StreamURL: streamURL,
			Signal: signalContinuousSilentDeath, Severity: healthSignalSeverity[signalContinuousSilentDeath],
			SinceText: sinceText,
			Diag:      diag,
			Immediate: strings.Contains(diag, "capture_state=fresh_ingest_stale_media") || strings.TrimSpace(lastErr) != "",
		})
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("iterate continuous_silent_death: %v", err)
	}
	return out
}

func detectContinuousWindowEndedEarly(ctx context.Context, pool *pgxpool.Pool) []healthIncident {
	rows, err := pool.Query(ctx, `
		SELECT j.recording_id,COALESCE(r.stream_id,0),r.account_id,r.name,r.stream_url,j.id,j.fire_at,j.window_end_at,j.status,j.error_text,
		       acc.name, acc.email
		FROM recording_jobs j JOIN recordings r ON r.id=j.recording_id JOIN account_billing b ON b.account_id=r.account_id
		JOIN accounts acc ON acc.id=r.account_id
		WHERE j.kind='continuous_window' AND j.status IN ('done','error') AND j.window_end_at>now()
		  AND r.status='active' AND b.has_payment_method=true
		  AND NOT EXISTS (SELECT 1 FROM recording_clips cl WHERE cl.recording_id=j.recording_id AND cl.clip_start_at >= j.fire_at)
	`)
	if err != nil {
		log.Fatalf("signal continuous_window_ended_early: %v", err)
	}
	defer rows.Close()
	out := []healthIncident{}
	for rows.Next() {
		var (
			recID, streamID, accountID int64
			jobID                      int64
			name, streamURL            string
			orgName, orgEmail          string
			status, errText            string
			fireAt, windowEndAt        time.Time
		)
		if err := rows.Scan(&recID, &streamID, &accountID, &name, &streamURL, &jobID, &fireAt, &windowEndAt, &status, &errText, &orgName, &orgEmail); err != nil {
			log.Fatalf("scan continuous_window_ended_early: %v", err)
		}
		out = append(out, healthIncident{
			RecordingID: recID, StreamID: streamID, AccountID: accountID, OrgName: orgName, OrgEmail: orgEmail,
			RecName: name, StreamURL: streamURL,
			Signal: signalContinuousWindowEndedEarly, Severity: healthSignalSeverity[signalContinuousWindowEndedEarly],
			SinceText: fmt.Sprintf("job fired %s, window ends %s", fireAt.UTC().Format(time.RFC3339), windowEndAt.UTC().Format(time.RFC3339)),
			Diag:      diagText("job_id", fmt.Sprint(jobID), "job_status", status, "error", errText),
		})
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("iterate continuous_window_ended_early: %v", err)
	}
	return out
}

func detectJobRetriesExhausted(ctx context.Context, pool *pgxpool.Pool) []healthIncident {
	rows, err := pool.Query(ctx, `
		SELECT j.recording_id,COALESCE(r.stream_id,0),r.account_id,r.name,j.id,j.kind,j.attempt_count,j.max_attempts,j.error_text,COALESCE(j.completed_at,j.updated_at),
		       acc.name, acc.email, r.stream_url
		FROM recording_jobs j JOIN recordings r ON r.id=j.recording_id JOIN account_billing b ON b.account_id=r.account_id
		JOIN accounts acc ON acc.id=r.account_id
		WHERE j.status='error' AND j.attempt_count>=j.max_attempts AND COALESCE(j.completed_at,j.updated_at)>=now()-interval '90 minutes'
		  AND r.status='active' AND b.has_payment_method=true
	`)
	if err != nil {
		log.Fatalf("signal job_retries_exhausted: %v", err)
	}
	defer rows.Close()
	out := []healthIncident{}
	for rows.Next() {
		var (
			recID, streamID, accountID   int64
			jobID                        int64
			name, kind, errText          string
			orgName, orgEmail, streamURL string
			attemptCount, maxAttempts    int
			failedAt                     time.Time
		)
		if err := rows.Scan(&recID, &streamID, &accountID, &name, &jobID, &kind, &attemptCount, &maxAttempts, &errText, &failedAt, &orgName, &orgEmail, &streamURL); err != nil {
			log.Fatalf("scan job_retries_exhausted: %v", err)
		}
		out = append(out, healthIncident{
			RecordingID: recID, StreamID: streamID, AccountID: accountID, OrgName: orgName, OrgEmail: orgEmail,
			RecName: name, StreamURL: streamURL,
			Signal: signalJobRetriesExhausted, Severity: healthSignalSeverity[signalJobRetriesExhausted],
			SinceText: failedAt.UTC().Format(time.RFC3339),
			Diag:      diagText("job_id", fmt.Sprint(jobID), "kind", kind, "attempts", fmt.Sprintf("%d/%d", attemptCount, maxAttempts), "error", errText),
		})
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("iterate job_retries_exhausted: %v", err)
	}
	return out
}

func detectStuckLease(ctx context.Context, pool *pgxpool.Pool) []healthIncident {
	rows, err := pool.Query(ctx, `
		SELECT j.recording_id,COALESCE(r.stream_id,0),r.account_id,r.name,j.id,j.kind,j.lease_owner,j.lease_expires_at,
		       acc.name, acc.email, r.stream_url
		FROM recording_jobs j JOIN recordings r ON r.id=j.recording_id
		JOIN accounts acc ON acc.id=r.account_id
		WHERE j.status='leased' AND j.lease_expires_at < now()-interval '15 minutes' AND r.status='active'
	`)
	if err != nil {
		log.Fatalf("signal stuck_lease: %v", err)
	}
	defer rows.Close()
	out := []healthIncident{}
	for rows.Next() {
		var (
			recID, streamID, accountID   int64
			jobID                        int64
			name, kind                   string
			orgName, orgEmail, streamURL string
			leaseOwner                   *string
			leaseExpiresAt               *time.Time
		)
		if err := rows.Scan(&recID, &streamID, &accountID, &name, &jobID, &kind, &leaseOwner, &leaseExpiresAt, &orgName, &orgEmail, &streamURL); err != nil {
			log.Fatalf("scan stuck_lease: %v", err)
		}
		owner := ""
		if leaseOwner != nil {
			owner = *leaseOwner
		}
		out = append(out, healthIncident{
			RecordingID: recID, StreamID: streamID, AccountID: accountID, OrgName: orgName, OrgEmail: orgEmail,
			RecName: name, StreamURL: streamURL,
			Signal: signalStuckLease, Severity: healthSignalSeverity[signalStuckLease],
			SinceText: fmt.Sprintf("lease expired %s", humanSince(leaseExpiresAt)),
			Diag:      diagText("job_id", fmt.Sprint(jobID), "kind", kind, "lease_owner", owner),
		})
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("iterate stuck_lease: %v", err)
	}
	return out
}

func detectSampledOverdue(ctx context.Context, pool *pgxpool.Pool) []healthIncident {
	rows, err := pool.Query(ctx, `
		SELECT r.id,COALESCE(r.stream_id,0),r.account_id,r.name,r.stream_url,r.next_fire_at,r.last_clip_at,r.consecutive_failures,r.last_error_text,
		       acc.name, acc.email
		FROM recordings r JOIN account_billing b ON b.account_id=r.account_id
		JOIN accounts acc ON acc.id=r.account_id
		WHERE r.status='active' AND r.mode='sampled' AND b.has_payment_method=true
		  AND now()>=r.start_at AND now()<COALESCE(r.end_at,'infinity'::timestamptz)
		  AND r.next_fire_at IS NOT NULL AND r.next_fire_at < now()-interval '15 minutes'
	`)
	if err != nil {
		log.Fatalf("signal sampled_overdue: %v", err)
	}
	defer rows.Close()
	out := []healthIncident{}
	for rows.Next() {
		var (
			id, streamID, accountID    int64
			name, streamURL            string
			orgName, orgEmail, lastErr string
			consecutiveFailures        int
			nextFireAt, lastClipAt     *time.Time
		)
		if err := rows.Scan(&id, &streamID, &accountID, &name, &streamURL, &nextFireAt, &lastClipAt, &consecutiveFailures, &lastErr, &orgName, &orgEmail); err != nil {
			log.Fatalf("scan sampled_overdue: %v", err)
		}
		out = append(out, healthIncident{
			RecordingID: id, StreamID: streamID, AccountID: accountID, OrgName: orgName, OrgEmail: orgEmail,
			RecName: name, StreamURL: streamURL,
			Signal: signalSampledOverdue, Severity: healthSignalSeverity[signalSampledOverdue],
			SinceText: fmt.Sprintf("next fire due %s, last clip %s", humanSince(nextFireAt), humanSince(lastClipAt)),
			Diag:      diagText("consecutive_failures", fmt.Sprint(consecutiveFailures), "last_error", lastErr),
		})
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("iterate sampled_overdue: %v", err)
	}
	return out
}

// diagText joins key/value diagnostics, dropping pairs whose value is blank so
// empty error columns don't clutter the email.
func diagText(kv ...string) string {
	parts := make([]string, 0, len(kv)/2)
	for i := 0; i+1 < len(kv); i += 2 {
		v := strings.TrimSpace(kv[i+1])
		if v == "" {
			continue
		}
		parts = append(parts, fmt.Sprintf("%s=%s", kv[i], v))
	}
	return strings.Join(parts, " ")
}
