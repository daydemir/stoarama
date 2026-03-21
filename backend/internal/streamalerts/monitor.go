package streamalerts

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/daydemir/stoarama/backend/internal/email"
)

const incidentTypeRecordingProblem = "recording_problem"

type MonitorConfig struct {
	AppBaseURL      string
	PollInterval    time.Duration
	ProblemDelay    time.Duration
	RepeatInterval  time.Duration
	ResolutionEmail bool
}

type Monitor struct {
	pool   *pgxpool.Pool
	mailer email.Sender
	cfg    MonitorConfig
}

type problemStream struct {
	StreamID        int64
	Name            string
	Slug            string
	Provider        string
	CaptureType     string
	ExecutionClass  string
	ServerID        string
	RuntimeStatus   string
	LastFrameAt     *time.Time
	RuntimeUpdated  *time.Time
	RuntimeError    string
	RelayStatus     string
	RelayError      string
	StreamUpdatedAt time.Time
}

func NewMonitor(pool *pgxpool.Pool, mailer email.Sender, cfg MonitorConfig) *Monitor {
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = time.Minute
	}
	if cfg.ProblemDelay <= 0 {
		cfg.ProblemDelay = 5 * time.Minute
	}
	if cfg.RepeatInterval <= 0 {
		cfg.RepeatInterval = 12 * time.Hour
	}
	return &Monitor{pool: pool, mailer: mailer, cfg: cfg}
}

func (m *Monitor) Run(ctx context.Context) error {
	ticker := time.NewTicker(m.cfg.PollInterval)
	defer ticker.Stop()
	if err := m.runOnce(ctx); err != nil {
		log.Printf("stream-alert monitor poll error: %v", err)
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := m.runOnce(ctx); err != nil {
				log.Printf("stream-alert monitor poll error: %v", err)
			}
		}
	}
}

func (m *Monitor) runOnce(ctx context.Context) error {
	admins, err := m.loadAdminRecipients(ctx)
	if err != nil {
		return err
	}
	if len(admins) == 0 {
		return nil
	}
	rows, err := m.loadProblemCandidates(ctx)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	open := map[int64]problemStream{}
	for _, it := range rows {
		if m.problemReason(it, now) == "" {
			continue
		}
		open[it.StreamID] = it
	}
	if err := m.resolveRecoveredIncidents(ctx, open, admins, now); err != nil {
		return err
	}
	for _, it := range rows {
		reason := m.problemReason(it, now)
		if reason == "" {
			continue
		}
		if err := m.upsertOpenIncident(ctx, it, reason, admins, now); err != nil {
			return err
		}
	}
	return nil
}

func (m *Monitor) loadAdminRecipients(ctx context.Context) ([]string, error) {
	rows, err := m.pool.Query(ctx, `
		SELECT email
		FROM accounts
		WHERE role='admin'
		  AND status='active'
		  AND email_verified_at IS NOT NULL
		ORDER BY id ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("query admin recipients: %w", err)
	}
	defer rows.Close()
	out := make([]string, 0, 4)
	for rows.Next() {
		var emailAddr string
		if err := rows.Scan(&emailAddr); err != nil {
			return nil, fmt.Errorf("scan admin recipient: %w", err)
		}
		emailAddr = strings.TrimSpace(emailAddr)
		if emailAddr != "" {
			out = append(out, emailAddr)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate admin recipients: %w", err)
	}
	return out, nil
}

func (m *Monitor) loadProblemCandidates(ctx context.Context) ([]problemStream, error) {
	rows, err := m.pool.Query(ctx, `
		SELECT
			s.id,
			s.name,
			s.slug,
			s.provider,
			s.capture_type,
			s.execution_class,
			COALESCE(ra.server_id, ''),
			COALESCE(rt.status, ''),
			rt.last_frame_at,
			rt.updated_at,
			COALESCE(rt.last_error_text, ''),
			COALESCE(yr.status, ''),
			COALESCE(yr.error_text, ''),
			s.updated_at
		FROM streams s
		LEFT JOIN recording_assignments ra ON ra.stream_id=s.id
		LEFT JOIN stream_capture_runtime rt ON rt.stream_id=s.id
		LEFT JOIN youtube_relay_routes yr ON yr.stream_id=s.id
		WHERE s.recording_state='on'
		ORDER BY s.id ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("query problem candidates: %w", err)
	}
	defer rows.Close()
	out := make([]problemStream, 0, 64)
	for rows.Next() {
		var it problemStream
		if err := rows.Scan(
			&it.StreamID,
			&it.Name,
			&it.Slug,
			&it.Provider,
			&it.CaptureType,
			&it.ExecutionClass,
			&it.ServerID,
			&it.RuntimeStatus,
			&it.LastFrameAt,
			&it.RuntimeUpdated,
			&it.RuntimeError,
			&it.RelayStatus,
			&it.RelayError,
			&it.StreamUpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan problem candidate: %w", err)
		}
		out = append(out, it)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate problem candidates: %w", err)
	}
	return out, nil
}

func (m *Monitor) problemReason(it problemStream, now time.Time) string {
	threshold := m.cfg.ProblemDelay
	if threshold <= 0 {
		threshold = 5 * time.Minute
	}
	if strings.TrimSpace(it.ServerID) == "" && now.Sub(it.StreamUpdatedAt) >= threshold {
		return "recording_on_but_unassigned"
	}
	if it.LastFrameAt != nil && now.Sub(it.LastFrameAt.UTC()) < threshold && strings.TrimSpace(it.RuntimeStatus) == "running" {
		return ""
	}
	switch strings.TrimSpace(it.RuntimeStatus) {
	case "error":
		return "capture_runtime_error"
	case "unsupported":
		return "capture_runtime_unsupported"
	case "stopped":
		return "capture_runtime_stopped"
	}
	if strings.TrimSpace(it.ExecutionClass) == "youtube_relay" {
		switch strings.TrimSpace(it.RelayStatus) {
		case "failed":
			return "youtube_relay_failed"
		case "stopped":
			return "youtube_relay_stopped"
		}
	}
	if it.LastFrameAt == nil && now.Sub(it.StreamUpdatedAt) >= threshold {
		return "no_successful_frames"
	}
	if it.LastFrameAt != nil && now.Sub(it.LastFrameAt.UTC()) >= threshold {
		return "stale_frames"
	}
	return ""
}

func (m *Monitor) upsertOpenIncident(ctx context.Context, it problemStream, reason string, admins []string, now time.Time) error {
	details := map[string]any{
		"name":            it.Name,
		"slug":            it.Slug,
		"reason":          reason,
		"provider":        it.Provider,
		"capture_type":    it.CaptureType,
		"execution_class": it.ExecutionClass,
		"server_id":       it.ServerID,
		"runtime_status":  it.RuntimeStatus,
		"runtime_error":   strings.TrimSpace(it.RuntimeError),
		"relay_status":    it.RelayStatus,
		"relay_error":     strings.TrimSpace(it.RelayError),
		"last_frame_at":   it.LastFrameAt,
	}
	detailsBytes, err := json.Marshal(details)
	if err != nil {
		return fmt.Errorf("marshal incident details: %w", err)
	}

	tx, err := m.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin incident tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var (
		incidentID      int64
		firstObservedAt time.Time
		lastNotifiedAt  *time.Time
		notifyCount     int
	)
	err = tx.QueryRow(ctx, `
		INSERT INTO stream_recording_incidents (
			stream_id, incident_type, status,
			first_observed_at, last_observed_at, opened_at, details_jsonb, updated_at
		)
		VALUES ($1, $2, 'open', $3, $3, now(), $4::jsonb, now())
		ON CONFLICT (stream_id, incident_type) WHERE status='open'
		DO UPDATE SET
			last_observed_at=EXCLUDED.last_observed_at,
			details_jsonb=EXCLUDED.details_jsonb,
			updated_at=now()
		RETURNING id, first_observed_at, last_notified_at, notify_count
	`, it.StreamID, incidentTypeRecordingProblem, now, string(detailsBytes)).Scan(&incidentID, &firstObservedAt, &lastNotifiedAt, &notifyCount)
	if err != nil {
		return fmt.Errorf("upsert incident: %w", err)
	}

	shouldNotify := now.Sub(firstObservedAt) >= m.cfg.ProblemDelay
	if shouldNotify && lastNotifiedAt != nil && now.Sub(lastNotifiedAt.UTC()) < m.cfg.RepeatInterval {
		shouldNotify = false
	}
	if shouldNotify {
		for _, addr := range admins {
			if err := m.mailer.Send(ctx, email.Message{
				To:          addr,
				Subject:     fmt.Sprintf("[Stoarama] Recording problem: #%d %s", it.StreamID, compactAlertTitle(it.Name)),
				PlainText:   m.problemPlainText(it, reason),
				MessageType: "recording_problem",
			}); err != nil {
				return fmt.Errorf("send incident email: %w", err)
			}
		}
		if _, err := tx.Exec(ctx, `
			UPDATE stream_recording_incidents
			SET last_notified_at=$2, notify_count=notify_count+1, updated_at=now()
			WHERE id=$1
		`, incidentID, now); err != nil {
			return fmt.Errorf("update incident notification status: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit incident tx: %w", err)
	}
	_ = notifyCount
	return nil
}

func (m *Monitor) resolveRecoveredIncidents(ctx context.Context, open map[int64]problemStream, admins []string, now time.Time) error {
	rows, err := m.pool.Query(ctx, `
		SELECT id, stream_id, details_jsonb, last_notified_at
		FROM stream_recording_incidents
		WHERE status='open' AND incident_type=$1
	`, incidentTypeRecordingProblem)
	if err != nil {
		return fmt.Errorf("query open incidents: %w", err)
	}
	defer rows.Close()
	type openIncident struct {
		ID             int64
		StreamID       int64
		DetailsRaw     []byte
		LastNotifiedAt *time.Time
	}
	incidents := make([]openIncident, 0, 32)
	for rows.Next() {
		var it openIncident
		if err := rows.Scan(&it.ID, &it.StreamID, &it.DetailsRaw, &it.LastNotifiedAt); err != nil {
			return fmt.Errorf("scan open incident: %w", err)
		}
		incidents = append(incidents, it)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate open incidents: %w", err)
	}
	for _, incident := range incidents {
		if _, ok := open[incident.StreamID]; ok {
			continue
		}
		if _, err := m.pool.Exec(ctx, `
			UPDATE stream_recording_incidents
			SET status='resolved', resolved_at=$2, last_observed_at=$2, updated_at=now()
			WHERE id=$1
		`, incident.ID, now); err != nil {
			return fmt.Errorf("resolve incident %d: %w", incident.ID, err)
		}
		if m.cfg.ResolutionEmail && incident.LastNotifiedAt != nil {
			var details map[string]any
			_ = json.Unmarshal(incident.DetailsRaw, &details)
			name, _ := details["name"].(string)
			if strings.TrimSpace(name) == "" {
				name = fmt.Sprintf("stream #%d", incident.StreamID)
			}
			for _, addr := range admins {
				if err := m.mailer.Send(ctx, email.Message{
					To:          addr,
					Subject:     fmt.Sprintf("[Stoarama] Recording recovered: #%d %s", incident.StreamID, compactAlertTitle(name)),
					PlainText:   fmt.Sprintf("%s has recovered.\n\nView: %s/dashboard/stream/%d\n", name, strings.TrimRight(m.cfg.AppBaseURL, "/"), incident.StreamID),
					MessageType: "recording_problem_resolved",
				}); err != nil {
					return fmt.Errorf("send resolution email: %w", err)
				}
			}
		}
	}
	return nil
}

func (m *Monitor) problemPlainText(it problemStream, reason string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Stoarama detected a recording problem that persisted past the alert threshold.\n\n")
	fmt.Fprintf(&b, "Stream: %s (#%d)\n", it.Name, it.StreamID)
	fmt.Fprintf(&b, "Reason: %s\n", reason)
	if strings.TrimSpace(it.ServerID) != "" {
		fmt.Fprintf(&b, "Server: %s\n", it.ServerID)
	}
	if strings.TrimSpace(it.RuntimeStatus) != "" {
		fmt.Fprintf(&b, "Runtime status: %s\n", it.RuntimeStatus)
	}
	if it.LastFrameAt != nil {
		fmt.Fprintf(&b, "Last frame: %s\n", it.LastFrameAt.UTC().Format(time.RFC3339))
	}
	if errText := compactAlertError(it.RuntimeError); errText != "" {
		fmt.Fprintf(&b, "Error: %s\n", errText)
	} else if errText := compactAlertError(it.RelayError); errText != "" {
		fmt.Fprintf(&b, "Error: %s\n", errText)
	}
	if strings.TrimSpace(m.cfg.AppBaseURL) != "" {
		fmt.Fprintf(&b, "\nView: %s/dashboard/stream/%d\n", strings.TrimRight(m.cfg.AppBaseURL, "/"), it.StreamID)
	}
	return b.String()
}

func compactAlertError(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	line := raw
	if idx := strings.IndexByte(line, '\n'); idx >= 0 {
		line = line[:idx]
	}
	line = strings.TrimSpace(line)
	if len(line) > 220 {
		line = strings.TrimSpace(line[:217]) + "..."
	}
	return line
}

func compactAlertTitle(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "stream"
	}
	if len(raw) > 80 {
		return strings.TrimSpace(raw[:77]) + "..."
	}
	return raw
}
