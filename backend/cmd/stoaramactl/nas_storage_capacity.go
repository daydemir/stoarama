package main

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/email"
)

const nasStorageTelemetryFreshness = 15 * time.Minute

type nasStorageCapacityState string

const (
	nasStorageHealthy  nasStorageCapacityState = "healthy"
	nasStorageWarning  nasStorageCapacityState = "warning"
	nasStorageCritical nasStorageCapacityState = "critical"
	nasStorageUnknown  nasStorageCapacityState = "unknown"
)

type nasStorageCapacityTransition struct {
	EventID           int64
	ConnectionID      int64
	ConnectionLabel   string
	OrgName           string
	OrgEmail          string
	State             nasStorageCapacityState
	ChangedAt         time.Time
	TotalBytes        *int64
	FreeBytes         *int64
	StorageReportedAt *time.Time
}

func nasStorageStateAt(total, free *int64, reportedAt *time.Time, now time.Time) nasStorageCapacityState {
	if total == nil || free == nil || reportedAt == nil || *total <= 0 || now.Sub(*reportedAt) > nasStorageTelemetryFreshness {
		return nasStorageUnknown
	}
	percent := float64(*free) * 100 / float64(*total)
	if percent <= 5 {
		return nasStorageCritical
	}
	if percent <= 10 {
		return nasStorageWarning
	}
	return nasStorageHealthy
}

func currentNASStorageCapacity(ctx context.Context, q interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}, now time.Time) ([]nasStorageCapacityTransition, error) {
	rows, err := q.Query(ctx, `
		SELECT c.id,c.label,a.name,a.email,c.nas_storage_total_bytes,
		       c.nas_storage_free_bytes,c.nas_storage_reported_at
		FROM connections c JOIN accounts a ON a.id=c.account_id
		WHERE c.account_id=$1 AND c.kind='nas_pull'
		ORDER BY c.id
	`, relayConnectivityAlertAccountID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var states []nasStorageCapacityTransition
	for rows.Next() {
		var state nasStorageCapacityTransition
		if err := rows.Scan(&state.ConnectionID, &state.ConnectionLabel, &state.OrgName, &state.OrgEmail,
			&state.TotalBytes, &state.FreeBytes, &state.StorageReportedAt); err != nil {
			return nil, err
		}
		state.State = nasStorageStateAt(state.TotalBytes, state.FreeBytes, state.StorageReportedAt, now)
		state.ChangedAt = now
		states = append(states, state)
	}
	return states, rows.Err()
}

func recordNASStorageCapacity(ctx context.Context, pool *pgxpool.Pool, now time.Time) ([]nasStorageCapacityTransition, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var recipientCount int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM users WHERE is_operator=true AND btrim(email)<>''`).Scan(&recipientCount); err != nil {
		return nil, err
	}
	if recipientCount == 0 {
		return nil, fmt.Errorf("no operator recipients configured")
	}
	states, err := currentNASStorageCapacity(ctx, tx, now)
	if err != nil {
		return nil, err
	}
	for i := range states {
		state := &states[i]
		var previous nasStorageCapacityState
		err := tx.QueryRow(ctx, `SELECT observed_state FROM nas_storage_capacity_alert_states WHERE connection_id=$1 FOR UPDATE`, state.ConnectionID).Scan(&previous)
		if err == pgx.ErrNoRows {
			_, err = tx.Exec(ctx, `INSERT INTO nas_storage_capacity_alert_states (connection_id,observed_state,observed_at) VALUES ($1,$2,$3) ON CONFLICT (connection_id) DO NOTHING`, state.ConnectionID, state.State, now)
			if err == nil && state.State != nasStorageHealthy {
				err = queueNASStorageCapacityEvent(ctx, tx, state, now)
			}
		} else if err == nil && previous != state.State {
			_, err = tx.Exec(ctx, `UPDATE nas_storage_capacity_alert_states SET observed_state=$2,observed_at=$3 WHERE connection_id=$1`, state.ConnectionID, state.State, now)
			if err == nil {
				err = queueNASStorageCapacityEvent(ctx, tx, state, now)
			}
		}
		if err != nil {
			return nil, err
		}
	}
	pending, err := pendingNASStorageCapacity(ctx, tx)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return pending, nil
}

func queueNASStorageCapacityEvent(ctx context.Context, tx pgx.Tx, state *nasStorageCapacityTransition, now time.Time) error {
	if err := tx.QueryRow(ctx, `
		INSERT INTO nas_storage_capacity_alert_events
		  (connection_id,state,observed_at,total_bytes,free_bytes,storage_reported_at)
		VALUES ($1,$2,$3,$4,$5,$6) RETURNING id
	`, state.ConnectionID, state.State, now, state.TotalBytes, state.FreeBytes, state.StorageReportedAt).Scan(&state.EventID); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `INSERT INTO nas_storage_capacity_alert_deliveries (event_id,recipient) SELECT $1,email FROM users WHERE is_operator=true AND btrim(email)<>''`, state.EventID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("no operator recipients configured")
	}
	return nil
}

func pendingNASStorageCapacity(ctx context.Context, q interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}) ([]nasStorageCapacityTransition, error) {
	rows, err := q.Query(ctx, `
		SELECT e.id,c.id,c.label,a.name,a.email,e.state,e.observed_at,
		       e.total_bytes,e.free_bytes,e.storage_reported_at
		FROM nas_storage_capacity_alert_events e
		JOIN connections c ON c.id=e.connection_id
		JOIN accounts a ON a.id=c.account_id
		WHERE c.account_id=$1 AND e.notified_at IS NULL ORDER BY e.id
	`, relayConnectivityAlertAccountID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var pending []nasStorageCapacityTransition
	for rows.Next() {
		var state nasStorageCapacityTransition
		if err := rows.Scan(&state.EventID, &state.ConnectionID, &state.ConnectionLabel,
			&state.OrgName, &state.OrgEmail, &state.State, &state.ChangedAt,
			&state.TotalBytes, &state.FreeBytes, &state.StorageReportedAt); err != nil {
			return nil, err
		}
		pending = append(pending, state)
	}
	return pending, rows.Err()
}

func deliverNASStorageCapacityEmail(ctx context.Context, pool *pgxpool.Pool, cfg config.Config, transitions []nasStorageCapacityTransition) error {
	if strings.ToLower(strings.TrimSpace(cfg.EmailProvider)) != "resend" {
		return fmt.Errorf("EMAIL_PROVIDER must be resend, got %q", cfg.EmailProvider)
	}
	mailer, err := email.NewSender(email.Config{Provider: cfg.EmailProvider, From: cfg.EmailFrom, ReplyTo: cfg.EmailReplyTo, ResendKey: cfg.EmailResendAPIKey})
	if err != nil {
		return err
	}
	for _, transition := range transitions {
		rows, err := pool.Query(ctx, `SELECT recipient FROM nas_storage_capacity_alert_deliveries WHERE event_id=$1 AND delivered_at IS NULL ORDER BY recipient`, transition.EventID)
		if err != nil {
			return err
		}
		var recipients []string
		for rows.Next() {
			var recipient string
			if err := rows.Scan(&recipient); err != nil {
				rows.Close()
				return err
			}
			recipients = append(recipients, recipient)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		for _, recipient := range recipients {
			hash := sha256.Sum256([]byte(strings.ToLower(strings.TrimSpace(recipient))))
			_, err := mailer.Send(ctx, email.Message{To: recipient, Subject: fmt.Sprintf("[Stoarama] NAS storage is %s", transition.State), PlainText: nasStorageCapacityBody(cfg.AppBaseURL, transition), MessageType: "nas_storage_capacity_alert", IdempotencyKey: fmt.Sprintf("nas-storage-capacity-%d-%x", transition.EventID, hash[:8])})
			if err != nil {
				return err
			}
			if _, err := pool.Exec(ctx, `UPDATE nas_storage_capacity_alert_deliveries SET delivered_at=now() WHERE event_id=$1 AND recipient=$2`, transition.EventID, recipient); err != nil {
				return err
			}
		}
	}
	return nil
}

func markNASStorageCapacityNotified(ctx context.Context, pool *pgxpool.Pool, transitions []nasStorageCapacityTransition) error {
	ids := make([]int64, 0, len(transitions))
	for _, transition := range transitions {
		ids = append(ids, transition.EventID)
	}
	tag, err := pool.Exec(ctx, `UPDATE nas_storage_capacity_alert_events e SET notified_at=now() WHERE e.id=ANY($1::bigint[]) AND e.notified_at IS NULL AND EXISTS (SELECT 1 FROM nas_storage_capacity_alert_deliveries d WHERE d.event_id=e.id) AND NOT EXISTS (SELECT 1 FROM nas_storage_capacity_alert_deliveries d WHERE d.event_id=e.id AND d.delivered_at IS NULL)`, ids)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != int64(len(ids)) {
		return fmt.Errorf("marked %d NAS storage alerts notified, expected %d", tag.RowsAffected(), len(ids))
	}
	return nil
}

func nasStorageCapacityBody(baseURL string, state nasStorageCapacityTransition) string {
	var body strings.Builder
	fmt.Fprintf(&body, "NAS storage is %s for %s <%s>\nConnection: %s (#%d)\n", state.State, state.OrgName, state.OrgEmail, state.ConnectionLabel, state.ConnectionID)
	if state.TotalBytes != nil && state.FreeBytes != nil {
		percent := float64(*state.FreeBytes) * 100 / float64(*state.TotalBytes)
		fmt.Fprintf(&body, "Free: %.2f%% (%d of %d bytes)\n", percent, *state.FreeBytes, *state.TotalBytes)
	} else {
		fmt.Fprintln(&body, "Free space: unavailable")
	}
	if state.StorageReportedAt != nil {
		fmt.Fprintf(&body, "Storage telemetry: %s\n", state.StorageReportedAt.UTC().Format(time.RFC3339))
	}
	fmt.Fprintf(&body, "Changed: %s\nNo clips were deleted. Add capacity or investigate the NAS before it fills.\n", state.ChangedAt.UTC().Format(time.RFC3339))
	if base := strings.TrimRight(strings.TrimSpace(baseURL), "/"); base != "" {
		fmt.Fprintf(&body, "Connections: %s/org-settings#connections\n", base)
	}
	return body.String()
}
