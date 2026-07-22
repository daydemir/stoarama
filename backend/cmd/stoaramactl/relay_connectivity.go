package main

import (
	"context"
	"crypto/sha256"
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/email"
)

const relayOnlineThreshold = 120 * time.Second
const relayConnectivityLockID int64 = 821754932

type relayConnectivityState string

const (
	relayOnline  relayConnectivityState = "online"
	relayOffline relayConnectivityState = "offline"
)

type relayConnectivityTransition struct {
	EventID         int64
	NodeID          int64
	Name            string
	Hostname        string
	OrgName         string
	OrgEmail        string
	State           relayConnectivityState
	ChangedAt       time.Time
	LastHeartbeatAt *time.Time
}

func runRelayConnectivity(ctx context.Context, cfg config.Config, args []string) {
	if len(args) < 1 || args[0] != "run" {
		log.Fatalf("usage: stoaramactl relay-connectivity run [--dry-run]")
	}
	fs := flag.NewFlagSet("relay-connectivity run", flag.ExitOnError)
	dryRun := fs.Bool("dry-run", false, "print current relay states; do not persist state or email")
	_ = fs.Parse(args[1:])

	pool := mustOpenPool(ctx, cfg)
	defer pool.Close()
	if *dryRun {
		states, err := currentRelayConnectivity(ctx, pool, time.Now().UTC())
		if err != nil {
			log.Fatalf("load relay connectivity: %v", err)
		}
		printJSON(map[string]any{"dry_run": true, "relays": states})
		return
	}
	lockConn, err := pool.Acquire(ctx)
	if err != nil {
		log.Fatalf("acquire relay connectivity lock connection: %v", err)
	}
	defer lockConn.Release()
	var locked bool
	if err := lockConn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, relayConnectivityLockID).Scan(&locked); err != nil {
		log.Fatalf("acquire relay connectivity lock: %v", err)
	}
	if !locked {
		printJSON(map[string]any{"dry_run": false, "skipped": "already_running", "transitions": 0, "emailed": 0})
		return
	}
	defer func() { _, _ = lockConn.Exec(ctx, `SELECT pg_advisory_unlock($1)`, relayConnectivityLockID) }()

	transitions, err := recordRelayConnectivity(ctx, pool, time.Now().UTC())
	if err != nil {
		log.Fatalf("record relay connectivity: %v", err)
	}
	if len(transitions) == 0 {
		printJSON(map[string]any{"dry_run": false, "transitions": 0, "emailed": 0})
		return
	}
	if err := deliverRelayConnectivityEmail(ctx, pool, cfg, transitions); err != nil {
		log.Fatalf("deliver relay connectivity alert: %v", err)
	}
	if err := markRelayConnectivityNotified(ctx, pool, transitions); err != nil {
		log.Fatalf("mark relay connectivity alerts notified: %v", err)
	}
	printJSON(map[string]any{"dry_run": false, "transitions": len(transitions), "emailed": 1})
}

func currentRelayConnectivity(ctx context.Context, q interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}, now time.Time) ([]relayConnectivityTransition, error) {
	rows, err := q.Query(ctx, `
		SELECT n.id, n.display_name, n.hostname, a.name, a.email, n.last_heartbeat_at
		FROM nodes n
		JOIN accounts a ON a.id=n.account_id
		WHERE n.node_type='relay' AND n.status='active'
		ORDER BY n.id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	states := []relayConnectivityTransition{}
	for rows.Next() {
		var state relayConnectivityTransition
		if err := rows.Scan(&state.NodeID, &state.Name, &state.Hostname, &state.OrgName, &state.OrgEmail, &state.LastHeartbeatAt); err != nil {
			return nil, err
		}
		state.State = relayStateAt(state.LastHeartbeatAt, now)
		states = append(states, state)
	}
	return states, rows.Err()
}

func relayStateAt(lastHeartbeatAt *time.Time, now time.Time) relayConnectivityState {
	if lastHeartbeatAt != nil && now.Sub(*lastHeartbeatAt) < relayOnlineThreshold {
		return relayOnline
	}
	return relayOffline
}

func recordRelayConnectivity(ctx context.Context, pool *pgxpool.Pool, now time.Time) ([]relayConnectivityTransition, error) {
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
	states, err := currentRelayConnectivity(ctx, tx, now)
	if err != nil {
		return nil, err
	}
	for _, state := range states {
		var previous relayConnectivityState
		err := tx.QueryRow(ctx, `SELECT observed_state FROM relay_connectivity_alert_states WHERE node_id=$1 FOR UPDATE`, state.NodeID).Scan(&previous)
		if err == pgx.ErrNoRows {
			if _, err := tx.Exec(ctx, `INSERT INTO relay_connectivity_alert_states (node_id, observed_state, observed_at) VALUES ($1,$2,$3) ON CONFLICT (node_id) DO NOTHING`, state.NodeID, state.State, now); err != nil {
				return nil, err
			}
			continue
		}
		if err != nil {
			return nil, err
		}
		if previous == state.State {
			continue
		}
		if _, err := tx.Exec(ctx, `UPDATE relay_connectivity_alert_states SET observed_state=$2, observed_at=$3 WHERE node_id=$1`, state.NodeID, state.State, now); err != nil {
			return nil, err
		}
		var eventID int64
		if err := tx.QueryRow(ctx, `INSERT INTO relay_connectivity_alert_events (node_id, state, observed_at, last_heartbeat_at) VALUES ($1,$2,$3,$4) RETURNING id`, state.NodeID, state.State, now, state.LastHeartbeatAt).Scan(&eventID); err != nil {
			return nil, err
		}
		tag, err := tx.Exec(ctx, `INSERT INTO relay_connectivity_alert_deliveries (event_id, recipient) SELECT $1, email FROM users WHERE is_operator=true AND btrim(email)<>''`, eventID)
		if err != nil {
			return nil, err
		}
		if tag.RowsAffected() == 0 {
			return nil, fmt.Errorf("no operator recipients configured")
		}
	}
	pending, err := pendingRelayConnectivity(ctx, tx)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return pending, nil
}

func pendingRelayConnectivity(ctx context.Context, q interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}) ([]relayConnectivityTransition, error) {
	rows, err := q.Query(ctx, `
		SELECT e.id, n.id, n.display_name, n.hostname, a.name, a.email, e.state, e.observed_at, e.last_heartbeat_at
		FROM relay_connectivity_alert_events e
		JOIN nodes n ON n.id=e.node_id
		JOIN accounts a ON a.id=n.account_id
		WHERE e.notified_at IS NULL
		ORDER BY e.id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	pending := []relayConnectivityTransition{}
	for rows.Next() {
		var transition relayConnectivityTransition
		if err := rows.Scan(&transition.EventID, &transition.NodeID, &transition.Name, &transition.Hostname, &transition.OrgName, &transition.OrgEmail, &transition.State, &transition.ChangedAt, &transition.LastHeartbeatAt); err != nil {
			return nil, err
		}
		pending = append(pending, transition)
	}
	return pending, rows.Err()
}

func deliverRelayConnectivityEmail(ctx context.Context, pool *pgxpool.Pool, cfg config.Config, transitions []relayConnectivityTransition) error {
	if strings.ToLower(strings.TrimSpace(cfg.EmailProvider)) != "resend" {
		return fmt.Errorf("EMAIL_PROVIDER must be resend, got %q", cfg.EmailProvider)
	}
	mailer, err := email.NewSender(email.Config{Provider: cfg.EmailProvider, From: cfg.EmailFrom, ReplyTo: cfg.EmailReplyTo, ResendKey: cfg.EmailResendAPIKey})
	if err != nil {
		return err
	}
	for _, transition := range transitions {
		recipients, err := pendingRelayConnectivityRecipients(ctx, pool, transition.EventID)
		if err != nil {
			return err
		}
		for _, recipient := range recipients {
			if _, err := mailer.Send(ctx, email.Message{
				To:             recipient,
				Subject:        relayConnectivitySubject(transition),
				PlainText:      relayConnectivityBody(cfg.AppBaseURL, transition),
				MessageType:    "relay_connectivity_alert",
				IdempotencyKey: relayConnectivityIdempotencyKey(transition.EventID, recipient),
			}); err != nil {
				return fmt.Errorf("send event %d to %s: %w", transition.EventID, recipient, err)
			}
			if _, err := pool.Exec(ctx, `UPDATE relay_connectivity_alert_deliveries SET delivered_at=now() WHERE event_id=$1 AND recipient=$2`, transition.EventID, recipient); err != nil {
				return fmt.Errorf("mark event %d delivered to %s: %w", transition.EventID, recipient, err)
			}
		}
	}
	return nil
}

func pendingRelayConnectivityRecipients(ctx context.Context, pool *pgxpool.Pool, eventID int64) ([]string, error) {
	rows, err := pool.Query(ctx, `SELECT recipient FROM relay_connectivity_alert_deliveries WHERE event_id=$1 AND delivered_at IS NULL ORDER BY recipient`, eventID)
	if err != nil {
		return nil, fmt.Errorf("load recipients for event %d: %w", eventID, err)
	}
	defer rows.Close()
	recipients := []string{}
	for rows.Next() {
		var recipient string
		if err := rows.Scan(&recipient); err != nil {
			return nil, fmt.Errorf("scan recipient for event %d: %w", eventID, err)
		}
		recipients = append(recipients, recipient)
	}
	return recipients, rows.Err()
}

func relayConnectivityIdempotencyKey(eventID int64, recipient string) string {
	recipientHash := sha256.Sum256([]byte(strings.ToLower(strings.TrimSpace(recipient))))
	return fmt.Sprintf("relay-connectivity-%d-%x", eventID, recipientHash[:8])
}

func markRelayConnectivityNotified(ctx context.Context, pool *pgxpool.Pool, transitions []relayConnectivityTransition) error {
	ids := make([]int64, 0, len(transitions))
	for _, transition := range transitions {
		ids = append(ids, transition.EventID)
	}
	tag, err := pool.Exec(ctx, `
		UPDATE relay_connectivity_alert_events e
		SET notified_at=now()
		WHERE e.id=ANY($1::bigint[])
		  AND e.notified_at IS NULL
		  AND EXISTS (SELECT 1 FROM relay_connectivity_alert_deliveries d WHERE d.event_id=e.id)
		  AND NOT EXISTS (SELECT 1 FROM relay_connectivity_alert_deliveries d WHERE d.event_id=e.id AND d.delivered_at IS NULL)
	`, ids)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != int64(len(ids)) {
		return fmt.Errorf("marked %d relay alerts notified, expected %d", tag.RowsAffected(), len(ids))
	}
	return nil
}

func relayConnectivitySubject(transition relayConnectivityTransition) string {
	return fmt.Sprintf("[Stoarama] Relay %s is %s", transition.Name, transition.State)
}

func relayConnectivityBody(baseURL string, transition relayConnectivityTransition) string {
	var body strings.Builder
	fmt.Fprintf(&body, "%s is %s\n", transition.Name, transition.State)
	fmt.Fprintf(&body, "Org: %s <%s>\n", transition.OrgName, transition.OrgEmail)
	if transition.Hostname != "" {
		fmt.Fprintf(&body, "Host: %s\n", transition.Hostname)
	}
	fmt.Fprintf(&body, "Changed: %s\n", transition.ChangedAt.UTC().Format(time.RFC3339))
	fmt.Fprintf(&body, "Last heartbeat: %s\n", humanSince(transition.LastHeartbeatAt))
	if base := strings.TrimRight(strings.TrimSpace(baseURL), "/"); base != "" {
		fmt.Fprintf(&body, "Relays: %s/org-settings#relay-computers\n", base)
	}
	return body.String()
}
