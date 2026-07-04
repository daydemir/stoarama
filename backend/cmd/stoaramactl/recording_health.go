package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/email"
)

// Signal identifiers persisted in recorder_health_alerts.signal and their
// human-readable labels + severities used in the operator email.
const (
	signalContinuousSilentDeath      = "continuous_silent_death"
	signalContinuousWindowEndedEarly = "continuous_window_ended_early"
	signalJobRetriesExhausted        = "job_retries_exhausted"
	signalStuckLease                 = "stuck_lease"
	signalSampledOverdue             = "sampled_overdue"
)

var healthSignalLabels = map[string]string{
	signalContinuousSilentDeath:      "Continuous recording stopped producing clips mid-window",
	signalContinuousWindowEndedEarly: "Continuous recording window ended early with no footage",
	signalJobRetriesExhausted:        "Recording jobs failed after exhausting retries",
	signalStuckLease:                 "Recording job lease stuck (scheduler reclaim may be stalled)",
	signalSampledOverdue:             "Sampled recording is overdue to fire",
}

var healthSignalSeverity = map[string]string{
	signalContinuousSilentDeath:      "CRITICAL",
	signalContinuousWindowEndedEarly: "CRITICAL",
	signalJobRetriesExhausted:        "HIGH",
	signalStuckLease:                 "HIGH",
	signalSampledOverdue:             "HIGH",
}

// healthIncident is one detected recording-health problem, enriched with the
// owning org so an operator can act without a lookup.
type healthIncident struct {
	RecordingID int64
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
	_ = fs.Parse(args)
	if *freshnessMin <= 0 {
		log.Fatalf("--freshness-min must be > 0")
	}

	pool := mustOpenPool(ctx, cfg)
	defer pool.Close()

	incidents := detectRecordingHealthIncidents(ctx, pool, *freshnessMin)
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
	body := composeHealthEmailBody(incidents)
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

func composeHealthEmailBody(incidents []healthIncident) string {
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

// detectRecordingHealthIncidents runs the five read-only signal queries and
// returns the union of detected incidents, ordered by severity then recording.
func detectRecordingHealthIncidents(ctx context.Context, pool *pgxpool.Pool, freshnessMin int) []healthIncident {
	incidents := make([]healthIncident, 0, 16)
	incidents = append(incidents, detectContinuousSilentDeath(ctx, pool, freshnessMin)...)
	incidents = append(incidents, detectContinuousWindowEndedEarly(ctx, pool)...)
	incidents = append(incidents, detectJobRetriesExhausted(ctx, pool)...)
	incidents = append(incidents, detectStuckLease(ctx, pool)...)
	incidents = append(incidents, detectSampledOverdue(ctx, pool)...)

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

func detectContinuousSilentDeath(ctx context.Context, pool *pgxpool.Pool, freshnessMin int) []healthIncident {
	rows, err := pool.Query(ctx, `
		WITH cont AS (
		  SELECT r.id, r.account_id, r.name, r.stream_url, r.cron_timezone, r.last_clip_at, r.last_error_text,
		    ((now() AT TIME ZONE r.cron_timezone)::date + r.daily_window_start) AT TIME ZONE r.cron_timezone AS win_open,
		    ((now() AT TIME ZONE r.cron_timezone)::date + r.daily_window_end)   AT TIME ZONE r.cron_timezone AS win_close
		  FROM recordings r JOIN account_billing b ON b.account_id=r.account_id
		  WHERE r.status='active' AND r.mode='continuous' AND b.has_payment_method=true
		    AND now()>=r.start_at AND now()<COALESCE(r.end_at,'infinity'::timestamptz))
		SELECT c.id,c.account_id,c.name,c.stream_url,c.win_open,c.win_close,c.last_clip_at,c.last_error_text,
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
			id, accountID            int64
			name, streamURL, orgName string
			orgEmail, lastErr        string
			winOpen, winClose        time.Time
			lastClipAt               *time.Time
		)
		if err := rows.Scan(&id, &accountID, &name, &streamURL, &winOpen, &winClose, &lastClipAt, &lastErr, &orgName, &orgEmail); err != nil {
			log.Fatalf("scan continuous_silent_death: %v", err)
		}
		out = append(out, healthIncident{
			RecordingID: id, AccountID: accountID, OrgName: orgName, OrgEmail: orgEmail,
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
		SELECT j.recording_id,r.account_id,r.name,r.stream_url,j.id,j.fire_at,j.window_end_at,j.status,j.error_text,
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
			recID, accountID    int64
			jobID               int64
			name, streamURL     string
			orgName, orgEmail   string
			status, errText     string
			fireAt, windowEndAt time.Time
		)
		if err := rows.Scan(&recID, &accountID, &name, &streamURL, &jobID, &fireAt, &windowEndAt, &status, &errText, &orgName, &orgEmail); err != nil {
			log.Fatalf("scan continuous_window_ended_early: %v", err)
		}
		out = append(out, healthIncident{
			RecordingID: recID, AccountID: accountID, OrgName: orgName, OrgEmail: orgEmail,
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
		SELECT j.recording_id,r.account_id,r.name,j.id,j.kind,j.attempt_count,j.max_attempts,j.error_text,COALESCE(j.completed_at,j.updated_at),
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
			recID, accountID             int64
			jobID                        int64
			name, kind, errText          string
			orgName, orgEmail, streamURL string
			attemptCount, maxAttempts    int
			failedAt                     time.Time
		)
		if err := rows.Scan(&recID, &accountID, &name, &jobID, &kind, &attemptCount, &maxAttempts, &errText, &failedAt, &orgName, &orgEmail, &streamURL); err != nil {
			log.Fatalf("scan job_retries_exhausted: %v", err)
		}
		out = append(out, healthIncident{
			RecordingID: recID, AccountID: accountID, OrgName: orgName, OrgEmail: orgEmail,
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
		SELECT j.recording_id,r.account_id,r.name,j.id,j.kind,j.lease_owner,j.lease_expires_at,
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
			recID, accountID             int64
			jobID                        int64
			name, kind                   string
			orgName, orgEmail, streamURL string
			leaseOwner                   *string
			leaseExpiresAt               *time.Time
		)
		if err := rows.Scan(&recID, &accountID, &name, &jobID, &kind, &leaseOwner, &leaseExpiresAt, &orgName, &orgEmail, &streamURL); err != nil {
			log.Fatalf("scan stuck_lease: %v", err)
		}
		owner := ""
		if leaseOwner != nil {
			owner = *leaseOwner
		}
		out = append(out, healthIncident{
			RecordingID: recID, AccountID: accountID, OrgName: orgName, OrgEmail: orgEmail,
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
		SELECT r.id,r.account_id,r.name,r.stream_url,r.next_fire_at,r.last_clip_at,r.consecutive_failures,r.last_error_text,
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
			id, accountID              int64
			name, streamURL            string
			orgName, orgEmail, lastErr string
			consecutiveFailures        int
			nextFireAt, lastClipAt     *time.Time
		)
		if err := rows.Scan(&id, &accountID, &name, &streamURL, &nextFireAt, &lastClipAt, &consecutiveFailures, &lastErr, &orgName, &orgEmail); err != nil {
			log.Fatalf("scan sampled_overdue: %v", err)
		}
		out = append(out, healthIncident{
			RecordingID: id, AccountID: accountID, OrgName: orgName, OrgEmail: orgEmail,
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
