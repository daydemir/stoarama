package main

// Built-in stability monitoring. These checks ride the existing recording-health
// crons (5-minute live sweep, hourly full sweep, 8-hour digest) and their
// operator email path, so recording stability no longer depends on an external
// watcher process that can die unnoticed.

import (
	"context"
	"crypto/sha256"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/email"
	"github.com/daydemir/stoarama/backend/internal/qualitygrade"
)

const (
	signalWindowGradePoor  = "window_grade_poor"
	signalRelayResolveRate = "relay_resolve_churn"
	signalDropletStale     = "recorder_droplet_stale"
)

// windowGradeAlertHorizon bounds which completed window may raise the grade
// alert: only a recording's latest window, and only while it is the current
// day's result. An older latest window means the recording has no newer
// schedule, which other signals own.
const windowGradeAlertHorizon = 36 * time.Hour

// windowGradeHistoryDays is enough history to evaluate any 14-day tier run.
const windowGradeHistoryDays = 45

// windowGradeStageTimeout bounds the grade scan so it can never stall the
// hourly sweep's alert delivery.
const windowGradeStageTimeout = time.Minute

// runWindowGradeStage appends the grade incidents and marks the grade signal
// evaluated only when the scan succeeded, so a failed scan neither raises nor
// resolves grade alerts and leaves every other signal untouched.
func runWindowGradeStage(ctx context.Context, base recordingHealthDetection, detect func(context.Context) ([]healthIncident, error)) recordingHealthDetection {
	incidents, err := detect(ctx)
	if err != nil {
		log.Printf("recording health: window grade stage skipped: %v", err)
		return base
	}
	return recordingHealthDetection{
		incidents:        append(base.incidents, incidents...),
		evaluatedSignals: append(base.evaluatedSignals, signalWindowGradePoor),
	}
}

// detectPoorWindowGrades raises one incident per paying active continuous
// recording whose latest completed window graded E, F, or unknown.
func detectPoorWindowGrades(ctx context.Context, pool *pgxpool.Pool, now time.Time) ([]healthIncident, error) {
	recs, err := qualitygrade.Load(ctx, pool, 0, now, windowGradeHistoryDays)
	if err != nil {
		return nil, err
	}
	poor := poorWindowGradeRecordings(recs, now)
	if len(poor) == 0 {
		return nil, nil
	}
	ids := make([]int64, 0, len(poor))
	for _, r := range poor {
		ids = append(ids, r.RecordingID)
	}
	bases, err := loadPayingRecordingIncidentBases(ctx, pool, ids)
	if err != nil {
		return nil, err
	}
	out := make([]healthIncident, 0, len(poor))
	for _, r := range poor {
		base, ok := bases[r.RecordingID]
		if !ok {
			continue
		}
		out = append(out, windowGradeIncident(base, r))
	}
	return out, nil
}

func poorWindowGradeRecordings(recs []qualitygrade.Recording, now time.Time) []qualitygrade.Recording {
	out := []qualitygrade.Recording{}
	for _, r := range recs {
		latest, ok := r.Latest()
		if ok && latest.Grade.Poor() && now.Sub(latest.WindowEndAt) <= windowGradeAlertHorizon {
			out = append(out, r)
		}
	}
	return out
}

func windowGradeIncident(base healthIncident, r qualitygrade.Recording) healthIncident {
	latest, _ := r.Latest()
	good := r.TierProgress(qualitygrade.TierGood)
	great := r.TierProgress(qualitygrade.TierGreat)
	coverage := "unmeasured"
	if latest.CoveragePct != nil {
		coverage = fmt.Sprintf("%.2f%%", *latest.CoveragePct)
	}
	inc := base
	inc.Signal, inc.Severity = signalWindowGradePoor, healthSignalSeverity[signalWindowGradePoor]
	inc.SinceText = fmt.Sprintf("window %s closed %s", latest.LocalDate.Format("2006-01-02"), latest.WindowEndAt.UTC().Format(time.RFC3339))
	inc.Diag = diagText("grade", string(latest.Grade), "coverage", coverage,
		"last14", gradeStrip(r.Windows, qualitygrade.RunLength),
		"good+", fmt.Sprintf("run=%d/14 E=%d F=%d completed=%t", good.RunDays, good.ECount, good.FCount, good.Completed),
		"great+", fmt.Sprintf("run=%d/14 completed=%t", great.RunDays, great.Completed))
	return inc
}

// loadPayingRecordingIncidentBases returns the org/stream context for the given
// recordings, limited to paying accounts like every other recording alert.
func loadPayingRecordingIncidentBases(ctx context.Context, pool *pgxpool.Pool, ids []int64) (map[int64]healthIncident, error) {
	rows, err := pool.Query(ctx, `
		SELECT r.id, COALESCE(r.stream_id,0), r.account_id, r.name, r.stream_url, acc.name, acc.email
		FROM recordings r
		JOIN accounts acc ON acc.id=r.account_id
		JOIN account_billing b ON b.account_id=r.account_id AND b.has_payment_method=true
		WHERE r.id=ANY($1::bigint[])`, ids)
	if err != nil {
		return nil, fmt.Errorf("load recording alert context: %w", err)
	}
	defer rows.Close()
	out := map[int64]healthIncident{}
	for rows.Next() {
		var inc healthIncident
		if err := rows.Scan(&inc.RecordingID, &inc.StreamID, &inc.AccountID, &inc.RecName, &inc.StreamURL, &inc.OrgName, &inc.OrgEmail); err != nil {
			return nil, fmt.Errorf("scan recording alert context: %w", err)
		}
		out[inc.RecordingID] = inc
	}
	return out, rows.Err()
}

// Relay resolve churn. Relays report per-job diagnostics in their heartbeat
// (nodes.capabilities_jsonb->'recording_job'->'active'). A job entry may carry
// a numeric "resolve_count": how many times the relay resolved the source for
// that lease since "started_at". Relays that do not report it are skipped.
const (
	relayResolveChurnPerHour  = 30.0
	relayResolveChurnMinCount = 10
	relayResolveChurnMinAge   = 15 * time.Minute
)

type relayResolveSample struct {
	base         healthIncident
	nodeID       int64
	nodeName     string
	resolveCount float64
	startedAt    string
}

// resolveChurnRate returns resolves/hour for a sample and whether it breaches
// the churn threshold. Malformed or too-young samples never breach.
func resolveChurnRate(count float64, startedAt string, now time.Time) (float64, bool) {
	started, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(startedAt))
	if err != nil || count < 0 {
		return 0, false
	}
	age := now.Sub(started)
	if age < relayResolveChurnMinAge {
		return 0, false
	}
	rate := count / age.Hours()
	return rate, count >= relayResolveChurnMinCount && rate > relayResolveChurnPerHour
}

func detectRelayResolveChurn(ctx context.Context, pool *pgxpool.Pool, _ int) []healthIncident {
	rows, err := pool.Query(ctx, `
		WITH active_jobs AS (
		  SELECT n.id AS node_id, n.display_name, a
		  FROM nodes n
		  CROSS JOIN LATERAL jsonb_array_elements(
		    CASE WHEN jsonb_typeof(n.capabilities_jsonb->'recording_job'->'active')='array'
		         THEN n.capabilities_jsonb->'recording_job'->'active' ELSE '[]'::jsonb END) a
		  WHERE n.node_type='relay' AND n.status='active'
		    AND n.last_heartbeat_at >= now()-interval '5 minutes'
		    AND jsonb_typeof(a->'recording_id')='number'
		    AND jsonb_typeof(a->'resolve_count')='number'
		)
		SELECT j.node_id, j.display_name, r.id, COALESCE(r.stream_id,0), r.account_id, r.name, r.stream_url,
		       acc.name, acc.email, (j.a->>'resolve_count')::float8, COALESCE(j.a->>'started_at','')
		FROM active_jobs j
		JOIN recordings r ON r.id=(j.a->>'recording_id')::numeric::bigint
		JOIN accounts acc ON acc.id=r.account_id
		JOIN account_billing b ON b.account_id=r.account_id AND b.has_payment_method=true
		WHERE r.status='active'`)
	if err != nil {
		log.Fatalf("signal %s: %v", signalRelayResolveRate, err)
	}
	defer rows.Close()
	now := time.Now().UTC()
	out := []healthIncident{}
	seen := map[int64]bool{}
	for rows.Next() {
		var s relayResolveSample
		if err := rows.Scan(&s.nodeID, &s.nodeName, &s.base.RecordingID, &s.base.StreamID, &s.base.AccountID, &s.base.RecName,
			&s.base.StreamURL, &s.base.OrgName, &s.base.OrgEmail, &s.resolveCount, &s.startedAt); err != nil {
			log.Fatalf("scan %s: %v", signalRelayResolveRate, err)
		}
		rate, breach := resolveChurnRate(s.resolveCount, s.startedAt, now)
		if !breach || seen[s.base.RecordingID] {
			continue
		}
		seen[s.base.RecordingID] = true
		inc := s.base
		inc.Signal, inc.Severity = signalRelayResolveRate, healthSignalSeverity[signalRelayResolveRate]
		inc.SinceText = "lease started " + s.startedAt
		inc.Diag = diagText("node", fmt.Sprintf("%s (#%d)", s.nodeName, s.nodeID),
			"resolves", fmt.Sprintf("%.0f", s.resolveCount), "per_hour", fmt.Sprintf("%.1f", rate),
			"threshold", fmt.Sprintf("%.0f/h", relayResolveChurnPerHour))
		out = append(out, inc)
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("iterate %s: %v", signalRelayResolveRate, err)
	}
	return out
}

// Stale recorder droplets. The pool controller retires an unresponsive worker
// after DROPLET_POOL_STALE_HEARTBEAT_SEC (15 minutes by default). A billable
// droplet silent for twice that means the controller itself is not acting
// (service down, provider errors, or a lease it cannot clear), which is exactly
// how droplet 1132 billed for 12 days unnoticed.
const staleRecorderDropletAlertAfter = 30 * time.Minute

type staleRecorderDroplet struct {
	ID         int64
	Name       string
	State      string
	PoolRole   string
	LastLiveAt time.Time
	LiveLeases int
}

func (d staleRecorderDroplet) alertKey() string {
	return fmt.Sprintf("%s:%d", signalDropletStale, d.ID)
}

func loadStaleRecorderDroplets(ctx context.Context, pool *pgxpool.Pool, now time.Time) ([]staleRecorderDroplet, error) {
	rows, err := pool.Query(ctx, `
		SELECT d.id, d.name, d.state, d.pool_role,
		       GREATEST(d.created_at, d.last_seen_at, n.last_heartbeat_at) AS last_live_at,
		       (SELECT count(*) FROM recording_jobs j
		         WHERE j.lease_owner=d.name AND j.status='leased' AND j.lease_expires_at>now())::int
		FROM recorder_droplets d
		LEFT JOIN nodes n ON n.id=d.node_id
		WHERE d.state IN ('provisioning','active','draining','destroying')
		  AND GREATEST(d.created_at, d.last_seen_at, n.last_heartbeat_at) < $1
		ORDER BY d.id`, now.Add(-staleRecorderDropletAlertAfter))
	if err != nil {
		return nil, fmt.Errorf("load stale recorder droplets: %w", err)
	}
	defer rows.Close()
	out := []staleRecorderDroplet{}
	for rows.Next() {
		var d staleRecorderDroplet
		if err := rows.Scan(&d.ID, &d.Name, &d.State, &d.PoolRole, &d.LastLiveAt, &d.LiveLeases); err != nil {
			return nil, fmt.Errorf("scan stale recorder droplet: %w", err)
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// opsAlertReminder re-sends a still-open ops alert once a day.
const opsAlertReminder = 24 * time.Hour

// recordOpsAlertEpisodes upserts the detected subjects of one signal into
// ops_alert_episodes, resolves that signal's subjects no longer detected, and
// returns the keys due for delivery: a new or reopened episode, or an open one
// last delivered more than opsAlertReminder ago.
func recordOpsAlertEpisodes(ctx context.Context, pool *pgxpool.Pool, signal string, keys []string, now time.Time) ([]string, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	due := []string{}
	for _, key := range keys {
		var lastAlertedAt *time.Time
		if err := tx.QueryRow(ctx, `
			INSERT INTO ops_alert_episodes (alert_key, signal, first_detected_at, last_detected_at)
			VALUES ($1, $2, $3, $3)
			ON CONFLICT (alert_key) DO UPDATE SET
			  first_detected_at = CASE WHEN ops_alert_episodes.resolved_at IS NOT NULL THEN EXCLUDED.first_detected_at ELSE ops_alert_episodes.first_detected_at END,
			  last_alerted_at = CASE WHEN ops_alert_episodes.resolved_at IS NOT NULL THEN NULL ELSE ops_alert_episodes.last_alerted_at END,
			  last_detected_at = EXCLUDED.last_detected_at,
			  resolved_at = NULL
			RETURNING last_alerted_at`, key, signal, now).Scan(&lastAlertedAt); err != nil {
			return nil, fmt.Errorf("upsert ops alert %s: %w", key, err)
		}
		if lastAlertedAt == nil || now.Sub(*lastAlertedAt) >= opsAlertReminder {
			due = append(due, key)
		}
	}
	if _, err := tx.Exec(ctx, `
		UPDATE ops_alert_episodes SET resolved_at=$3
		WHERE signal=$1 AND resolved_at IS NULL AND alert_key <> ALL($2::text[])`, signal, keys, now); err != nil {
		return nil, fmt.Errorf("resolve ops alerts %s: %w", signal, err)
	}
	return due, tx.Commit(ctx)
}

func markOpsAlertsDelivered(ctx context.Context, pool *pgxpool.Pool, keys []string, now time.Time) error {
	_, err := pool.Exec(ctx, `UPDATE ops_alert_episodes SET last_alerted_at=$2 WHERE alert_key=ANY($1::text[])`, keys, now)
	return err
}

// runStaleRecorderDropletAlerts is one sweep of the stale-droplet alert. It is
// isolated from the recording-health run: an error here is logged and never
// suppresses recording alerts.
func runStaleRecorderDropletAlerts(ctx context.Context, pool *pgxpool.Pool, cfg config.Config, dryRun bool) (detected, emailed int) {
	now := time.Now().UTC()
	stale, err := loadStaleRecorderDroplets(ctx, pool, now)
	if err != nil {
		log.Printf("recording health: stale recorder droplet check skipped: %v", err)
		return 0, 0
	}
	if dryRun {
		for _, d := range stale {
			fmt.Printf("[dry-run] %s droplet=%d name=%s state=%s role=%s last_live=%s leases=%d\n",
				signalDropletStale, d.ID, d.Name, d.State, d.PoolRole, d.LastLiveAt.UTC().Format(time.RFC3339), d.LiveLeases)
		}
		return len(stale), 0
	}
	keys := make([]string, 0, len(stale))
	byKey := map[string]staleRecorderDroplet{}
	for _, d := range stale {
		keys = append(keys, d.alertKey())
		byKey[d.alertKey()] = d
	}
	due, err := recordOpsAlertEpisodes(ctx, pool, signalDropletStale, keys, now)
	if err != nil {
		log.Printf("recording health: stale recorder droplet episodes skipped: %v", err)
		return len(stale), 0
	}
	if len(due) == 0 {
		return len(stale), 0
	}
	notify := make([]staleRecorderDroplet, 0, len(due))
	for _, key := range due {
		notify = append(notify, byKey[key])
	}
	sent, err := deliverStaleRecorderDropletEmail(ctx, pool, cfg, notify, now)
	if err != nil {
		log.Printf("recording health: stale recorder droplet alert delivery failed (retries next sweep): %v", err)
		return len(stale), sent
	}
	if err := markOpsAlertsDelivered(ctx, pool, due, now); err != nil {
		log.Printf("recording health: mark stale recorder droplet alerts delivered: %v", err)
	}
	return len(stale), sent
}

func deliverStaleRecorderDropletEmail(ctx context.Context, pool *pgxpool.Pool, cfg config.Config, droplets []staleRecorderDroplet, now time.Time) (int, error) {
	if strings.ToLower(strings.TrimSpace(cfg.EmailProvider)) != "resend" {
		return 0, fmt.Errorf("EMAIL_PROVIDER=%q is not resend", cfg.EmailProvider)
	}
	recipients := operatorRecipients(ctx, pool)
	if len(recipients) == 0 {
		return 0, fmt.Errorf("no operator recipients")
	}
	mailer, err := email.NewSender(email.Config{Provider: cfg.EmailProvider, From: cfg.EmailFrom, ReplyTo: cfg.EmailReplyTo, ResendKey: cfg.EmailResendAPIKey})
	if err != nil {
		return 0, fmt.Errorf("init email sender: %w", err)
	}
	subject, body := composeStaleRecorderDropletEmail(droplets, now)
	idem := sha256.Sum256([]byte(body))
	sent := 0
	for _, addr := range recipients {
		rcpt := sha256.Sum256([]byte(strings.ToLower(addr)))
		if _, err := mailer.Send(ctx, email.Message{To: addr, Subject: subject, PlainText: body, MessageType: "recorder_droplet_stale_alert",
			IdempotencyKey: fmt.Sprintf("recorder-droplet-stale:%x:%x", idem[:8], rcpt[:8])}); err != nil {
			return sent, fmt.Errorf("send to %s: %w", addr, err)
		}
		sent++
	}
	return sent, nil
}

func composeStaleRecorderDropletEmail(droplets []staleRecorderDroplet, now time.Time) (string, string) {
	sort.Slice(droplets, func(i, j int) bool { return droplets[i].ID < droplets[j].ID })
	subject := fmt.Sprintf("[Stoarama] %d recorder droplet(s) unresponsive and still billable", len(droplets))
	var b strings.Builder
	fmt.Fprintf(&b, "These recorder droplets are in a billable state but have not heartbeated for over %s.\n", staleRecorderDropletAlertAfter)
	b.WriteString("The pool controller should have retired them after DROPLET_POOL_STALE_HEARTBEAT_SEC; check the stoarama-recorder-control service logs.\n\n")
	for _, d := range droplets {
		fmt.Fprintf(&b, "  droplet #%d %s state=%s role=%s silent=%s live_leases=%d\n",
			d.ID, d.Name, d.State, d.PoolRole, now.Sub(d.LastLiveAt).Truncate(time.Minute), d.LiveLeases)
	}
	return subject, b.String()
}

// Digest section: daily grades and tier progress plus fleet stability.
type digestStability struct {
	Recordings []qualitygrade.Recording
	Stale      []staleRecorderDroplet
}

func loadDigestStability(ctx context.Context, pool *pgxpool.Pool, now time.Time) (*digestStability, error) {
	recs, err := qualitygrade.Load(ctx, pool, 0, now, windowGradeHistoryDays)
	if err != nil {
		return nil, err
	}
	stale, err := loadStaleRecorderDroplets(ctx, pool, now)
	if err != nil {
		return nil, err
	}
	return &digestStability{Recordings: recs, Stale: stale}, nil
}

func composeDigestStability(b *strings.Builder, s digestStability, now time.Time) {
	latest := map[qualitygrade.Grade]int{}
	goodDone, goodNear := 0, 0
	poor := []qualitygrade.Recording{}
	for _, r := range s.Recordings {
		if w, ok := r.Latest(); ok {
			latest[w.Grade]++
			if w.Grade.Poor() && now.Sub(w.WindowEndAt) <= windowGradeAlertHorizon {
				poor = append(poor, r)
			}
		}
		good := r.TierProgress(qualitygrade.TierGood)
		switch {
		case good.RunDays == qualitygrade.RunLength:
			goodDone++
		case good.RunDays >= 10:
			goodNear++
		}
	}
	b.WriteString("DAILY QUALITY GRADES (latest completed window per active continuous recording)\n")
	fmt.Fprintf(b, "  A=%d B=%d C=%d D=%d E=%d F=%d unknown=%d. Good+ 14/14 now: %d; Good+ run 10-13: %d.\n",
		latest[qualitygrade.GradeA], latest[qualitygrade.GradeB], latest[qualitygrade.GradeC], latest[qualitygrade.GradeD],
		latest[qualitygrade.GradeE], latest[qualitygrade.GradeF], latest[qualitygrade.GradeUnknown], goodDone, goodNear)
	for _, r := range poor {
		w, _ := r.Latest()
		good := r.TierProgress(qualitygrade.TierGood)
		fmt.Fprintf(b, "  #%d %s: %s %s; last14 %s; good+ run %d/14 E%d F%d\n", r.RecordingID, r.Name,
			w.LocalDate.Format("01-02"), w.Grade, gradeStrip(r.Windows, qualitygrade.RunLength), good.RunDays, good.ECount, good.FCount)
	}
	b.WriteString("\nRECORDER FLEET\n")
	if len(s.Stale) == 0 {
		fmt.Fprintf(b, "  No billable recorder droplet silent over %s.\n", staleRecorderDropletAlertAfter)
	}
	for _, d := range s.Stale {
		fmt.Fprintf(b, "  UNRESPONSIVE droplet #%d %s state=%s silent=%s live_leases=%d\n", d.ID, d.Name, d.State, now.Sub(d.LastLiveAt).Truncate(time.Minute), d.LiveLeases)
	}
	b.WriteString("\n")
}
