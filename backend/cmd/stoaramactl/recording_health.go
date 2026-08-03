package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strings"
	"time"

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
	signalStoredClipInvalid          = "stored_clip_invalid"
)

// clipTimestampDriftLimitSec is how far a clip's start may lead its own ingest
// before the timestamp is treated as fiction rather than jitter. It matches
// capture's continuousChainDriftLimit, so the alert and the recorder agree on
// what "drifted" means.
const clipTimestampDriftLimitSec = 90

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
	signalStoredClipInvalid:          "Latest stored clip is missing, truncated, or not decodable",
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
}

func runRecordingHealth(ctx context.Context, cfg config.Config, args []string) {
	if len(args) < 1 {
		log.Fatalf("usage: stoaramactl recording-health run [--dry-run --freshness-min 10]")
	}
	switch args[0] {
	case "run":
		runRecordingHealthRun(ctx, cfg, args[1:])
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
	freshnessMin := fs.Int("freshness-min", 10, "continuous silent-death freshness window in minutes")
	verifyMedia := fs.Bool("verify-media", false, "download and ffprobe the newest adjacent clip pair for each paid active recording with recent retained clips, then decode-verify their concatenation")
	_ = fs.Parse(args)
	if *freshnessMin <= 0 {
		log.Fatalf("--freshness-min must be > 0")
	}

	pool := mustOpenPool(ctx, cfg)
	defer pool.Close()

	incidents := detectRecordingHealthIncidents(ctx, pool, *freshnessMin)
	if *verifyMedia {
		incidents = append(incidents, detectLatestStoredClipHealth(ctx, pool, cfg)...)
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
			"detected":  len(incidents),
			"by_signal": bySignal,
			"notified":  0,
			"emailed":   0,
		})
		return
	}

	// runStart anchors the notify predicate: the UPSERT sets last_alerted_at to
	// the DB now() (which is >= runStart) exactly when this cycle should notify.
	runStart := time.Now()
	toNotify := make([]healthIncident, 0, len(incidents))
	for _, inc := range incidents {
		newlyInserted, lastAlertedAt, err := upsertHealthAlert(ctx, pool, inc.RecordingID, inc.Signal)
		if err != nil {
			log.Fatalf("upsert health alert recording=%d signal=%s: %v", inc.RecordingID, inc.Signal, err)
		}
		if shouldNotifyHealthIncident(newlyInserted, lastAlertedAt, runStart) {
			toNotify = append(toNotify, inc)
		}
	}
	if err := resolveClearedHealthAlerts(ctx, pool, incidents); err != nil {
		log.Fatalf("resolve cleared health alerts: %v", err)
	}

	emailed := 0
	if len(toNotify) > 0 {
		emailed = deliverRecordingHealthEmail(ctx, pool, cfg, toNotify)
	}

	printJSON(map[string]any{
		"dry_run":   false,
		"detected":  len(incidents),
		"by_signal": bySignal,
		"notified":  len(toNotify),
		"emailed":   emailed,
	})
}

// shouldNotifyHealthIncident decides whether an incident warrants an email this
// cycle: it is newly inserted, or its last_alerted_at was (re)stamped to the DB
// now() this run (which is at-or-after runStart) because it had resolved or aged
// past the 24h re-notify threshold. A stale last_alerted_at (before runStart)
// means an active, already-notified incident that must stay quiet.
func shouldNotifyHealthIncident(newlyInserted bool, lastAlertedAt, runStart time.Time) bool {
	return newlyInserted || !lastAlertedAt.Before(runStart)
}

func upsertHealthAlert(ctx context.Context, pool *pgxpool.Pool, recordingID int64, signal string) (bool, time.Time, error) {
	var newlyInserted bool
	var lastAlertedAt time.Time
	err := pool.QueryRow(ctx, `
		INSERT INTO recorder_health_alerts (recording_id, signal) VALUES ($1,$2)
		ON CONFLICT (recording_id,signal) DO UPDATE
		  SET last_alerted_at = CASE WHEN recorder_health_alerts.resolved_at IS NOT NULL OR recorder_health_alerts.last_alerted_at < now()-interval '24 hours' THEN now() ELSE recorder_health_alerts.last_alerted_at END,
		      resolved_at = NULL
		RETURNING (xmax=0) AS newly_inserted, last_alerted_at
	`, recordingID, signal).Scan(&newlyInserted, &lastAlertedAt)
	return newlyInserted, lastAlertedAt, err
}

func resolveClearedHealthAlerts(ctx context.Context, pool *pgxpool.Pool, incidents []healthIncident) error {
	recIDs := make([]int64, 0, len(incidents))
	signals := make([]string, 0, len(incidents))
	for _, inc := range incidents {
		recIDs = append(recIDs, inc.RecordingID)
		signals = append(signals, inc.Signal)
	}
	_, err := pool.Exec(ctx, `
		UPDATE recorder_health_alerts a SET resolved_at=now()
		WHERE a.resolved_at IS NULL
		  AND NOT EXISTS (
		    SELECT 1 FROM unnest($1::bigint[], $2::text[]) AS d(recording_id, signal)
		    WHERE d.recording_id = a.recording_id AND d.signal = a.signal
		  )
	`, recIDs, signals)
	return err
}

// deliverRecordingHealthEmail sends one summary email per operator recipient. If
// email is not configured (provider != resend) it loudly logs that N incidents
// went un-emailed rather than silently succeeding. Returns the number of Send
// calls made.
func deliverRecordingHealthEmail(ctx context.Context, pool *pgxpool.Pool, cfg config.Config, incidents []healthIncident) int {
	if strings.ToLower(strings.TrimSpace(cfg.EmailProvider)) != "resend" {
		log.Printf("recording-health: EMAIL_PROVIDER=%q is not resend; email not sent for %d recording health incident(s) (dedup/resolve bookkeeping still applied)", cfg.EmailProvider, len(incidents))
		return 0
	}

	recipients := operatorRecipients(ctx, pool)
	if len(recipients) == 0 {
		log.Printf("recording-health: no operator recipients (users.is_operator=true); %d incident(s) not emailed", len(incidents))
		return 0
	}

	mailer, err := email.NewSender(email.Config{
		Provider:  cfg.EmailProvider,
		From:      cfg.EmailFrom,
		ReplyTo:   cfg.EmailReplyTo,
		ResendKey: cfg.EmailResendAPIKey,
	})
	if err != nil {
		log.Fatalf("init email sender: %v", err)
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
			log.Fatalf("send recording health alert to %s: %v", addr, err)
		}
		sent++
	}
	return sent
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

func humanSince(t *time.Time) string {
	if t == nil {
		return "never"
	}
	return t.UTC().Format(time.RFC3339)
}

type stitchWindow struct {
	incidentBase healthIncident
	open         time.Time
	close        time.Time
	clips        [][2]time.Time
}

type stitchWindowMetrics struct {
	coveragePct    float64
	overlapClips   int
	overlapSeconds float64
	maxGap         time.Duration
	gapClips       int
	longestRun     time.Duration
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
	var covered time.Duration
	var runStart, runEnd time.Time
	if len(intervals) > 0 {
		m.maxGap = intervals[0][0].Sub(open)
	}
	for _, iv := range intervals {
		if runEnd.IsZero() {
			runStart, runEnd = iv[0], iv[1]
			continue
		}
		if iv[0].Before(runEnd.Add(-joinTolerance)) {
			m.overlapClips++
			overlapEnd := iv[1]
			if overlapEnd.After(runEnd) {
				overlapEnd = runEnd
			}
			m.overlapSeconds += overlapEnd.Sub(iv[0]).Seconds()
		}
		if !iv[0].After(runEnd.Add(joinTolerance)) {
			if iv[1].After(runEnd) {
				runEnd = iv[1]
			}
			continue
		}
		run := runEnd.Sub(runStart)
		covered += run
		if run > m.longestRun {
			m.longestRun = run
		}
		if gap := iv[0].Sub(runEnd); gap > m.maxGap {
			m.maxGap = gap
		}
		m.gapClips++
		runStart, runEnd = iv[0], iv[1]
	}
	if !runEnd.IsZero() {
		run := runEnd.Sub(runStart)
		covered += run
		if run > m.longestRun {
			m.longestRun = run
		}
		if trail := close.Sub(runEnd); trail > m.maxGap {
			m.maxGap = trail
		}
	}
	if expected := close.Sub(open); expected > 0 {
		m.coveragePct = 100 * float64(covered) / float64(expected)
	}
	return m
}

// detectLatestStoredClipHealth verifies one recent object per active recording.
// Ingest already HEAD-checks an upload before committing its row; this daily
// sweep catches later disappearance, byte truncation, or a formally present MP4
// whose trailer/streams cannot be decoded. It reads and probes only—never rewrites
// or normalizes media, so quality and source-native FPS remain untouched.
func detectLatestStoredClipHealth(ctx context.Context, pool *pgxpool.Pool, cfg config.Config) []healthIncident {
	cipher := mustBuildStorageCipher(cfg)
	rows, err := pool.Query(ctx, `
		SELECT DISTINCT ON (r.id)
		  r.id,COALESCE(r.stream_id,0),r.account_id,r.name,r.stream_url,
		  acc.name,acc.email,c.id,c.object_key,c.size_bytes,
		  sd.id,sd.endpoint,sd.region,sd.bucket,sd.access_key_id,sd.secret_access_key_enc,
		  prev.id,prev.object_key,prev.size_bytes,
		  psd.id,psd.endpoint,psd.region,psd.bucket,psd.access_key_id,psd.secret_access_key_enc
		FROM recordings r
		JOIN accounts acc ON acc.id=r.account_id
		JOIN recording_clips c ON c.recording_id=r.id AND c.purged_at IS NULL
		JOIN storage_destinations sd ON sd.id=c.storage_destination_id
		JOIN LATERAL (
		  SELECT p.id,p.object_key,p.size_bytes,p.storage_destination_id
		  FROM recording_clips p
		  WHERE p.recording_id=r.id AND p.purged_at IS NULL AND p.id<>c.id
		    AND (p.created_at,p.id)<(c.created_at,c.id)
		  ORDER BY p.created_at DESC,p.id DESC LIMIT 1
		) prev ON true
		JOIN storage_destinations psd ON psd.id=prev.storage_destination_id
		WHERE r.status='active' AND c.created_at>=now()-interval '24 hours'
		ORDER BY r.id,c.created_at DESC,c.id DESC
	`)
	if err != nil {
		log.Fatalf("signal stored_clip_invalid: %v", err)
	}
	defer rows.Close()
	out := []healthIncident{}
	for rows.Next() {
		var recID, streamID, accountID, clipID, expectedSize, destinationID int64
		var prevClipID, prevExpectedSize, prevDestinationID int64
		var recName, streamURL, orgName, orgEmail, objectKey string
		var endpoint, region, bucket, accessKey string
		var secretEnc []byte
		var prevObjectKey, prevEndpoint, prevRegion, prevBucket, prevAccessKey string
		var prevSecretEnc []byte
		if err := rows.Scan(&recID, &streamID, &accountID, &recName, &streamURL,
			&orgName, &orgEmail, &clipID, &objectKey, &expectedSize,
			&destinationID, &endpoint, &region, &bucket, &accessKey, &secretEnc,
			&prevClipID, &prevObjectKey, &prevExpectedSize,
			&prevDestinationID, &prevEndpoint, &prevRegion, &prevBucket, &prevAccessKey, &prevSecretEnc); err != nil {
			log.Fatalf("scan stored_clip_invalid: %v", err)
		}
		base := healthIncident{
			RecordingID: recID, StreamID: streamID, AccountID: accountID,
			OrgName: orgName, OrgEmail: orgEmail, RecName: recName, StreamURL: streamURL,
			Signal: signalStoredClipInvalid, Severity: healthSignalSeverity[signalStoredClipInvalid],
			SinceText: fmt.Sprintf("latest clip id=%d", clipID),
		}
		problemFor := func(stage string, failedClipID, failedDestinationID int64, verifyErr error) {
			inc := base
			inc.Diag = diagText("stage", stage, "clip_id", fmt.Sprint(failedClipID), "destination_id", fmt.Sprint(failedDestinationID), "error", verifyErr.Error())
			out = append(out, inc)
		}
		problem := func(stage string, verifyErr error) { problemFor(stage, clipID, destinationID, verifyErr) }
		if cipher == nil {
			problem("configuration", fmt.Errorf("STORAGE_CRED_KEY is unset"))
			continue
		}
		secret, err := cipher.Decrypt(secretEnc)
		if err != nil {
			problem("decrypt_destination", err)
			continue
		}
		client, err := r2.New(ctx, r2.Config{AccessKey: accessKey, SecretKey: string(secret), Region: region, Bucket: bucket, Endpoint: endpoint})
		if err != nil {
			problem("storage_client", err)
			continue
		}
		latestPath, err := downloadAndProbeStoredClip(ctx, client, objectKey, expectedSize)
		if err != nil {
			problem("latest_clip", err)
			continue
		}
		prevSecret, err := cipher.Decrypt(prevSecretEnc)
		if err != nil {
			_ = os.Remove(latestPath)
			problemFor("decrypt_predecessor_destination", prevClipID, prevDestinationID, err)
			continue
		}
		prevClient, err := r2.New(ctx, r2.Config{AccessKey: prevAccessKey, SecretKey: string(prevSecret), Region: prevRegion, Bucket: prevBucket, Endpoint: prevEndpoint})
		if err != nil {
			_ = os.Remove(latestPath)
			problemFor("predecessor_storage_client", prevClipID, prevDestinationID, err)
			continue
		}
		prevPath, err := downloadAndProbeStoredClip(ctx, prevClient, prevObjectKey, prevExpectedSize)
		if err != nil {
			_ = os.Remove(latestPath)
			problemFor("predecessor_clip", prevClipID, prevDestinationID, err)
			continue
		}
		concatErr := capture.ValidateConcatFiles(ctx, []string{prevPath, latestPath})
		_ = os.Remove(prevPath)
		_ = os.Remove(latestPath)
		if concatErr != nil {
			problem("concat_decode", concatErr)
		}
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("iterate stored_clip_invalid: %v", err)
	}
	return out
}

func downloadAndProbeStoredClip(ctx context.Context, client *r2.Client, objectKey string, expectedSize int64) (string, error) {
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
	_, copyErr := io.Copy(tmp, body)
	bodyErr := body.Close()
	closeErr := tmp.Close()
	if copyErr != nil || bodyErr != nil || closeErr != nil {
		_ = os.Remove(tmp.Name())
		return "", fmt.Errorf("download: %w", errors.Join(copyErr, bodyErr, closeErr))
	}
	if err := capture.ValidateSegmentFile(ctx, tmp.Name()); err != nil {
		_ = os.Remove(tmp.Name())
		return "", fmt.Errorf("ffprobe: %w", err)
	}
	return tmp.Name(), nil
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
		       acc.name, acc.email
		FROM cont c JOIN accounts acc ON acc.id=c.account_id
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
		)
		if err := rows.Scan(&id, &streamID, &accountID, &name, &streamURL, &winOpen, &winClose, &lastClipAt, &lastErr, &orgName, &orgEmail); err != nil {
			log.Fatalf("scan continuous_silent_death: %v", err)
		}
		out = append(out, healthIncident{
			RecordingID: id, StreamID: streamID, AccountID: accountID, OrgName: orgName, OrgEmail: orgEmail,
			RecName: name, StreamURL: streamURL,
			Signal: signalContinuousSilentDeath, Severity: healthSignalSeverity[signalContinuousSilentDeath],
			SinceText: fmt.Sprintf("window opened %s, last clip %s", winOpen.UTC().Format(time.RFC3339), humanSince(lastClipAt)),
			Diag:      diagText("last_error", lastErr),
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
