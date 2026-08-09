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

type relayCapacityState string

const (
	relayCapacityHealthy  relayCapacityState = "healthy"
	relayCapacityDegraded relayCapacityState = "degraded"
)

type relayCapacityTransition struct {
	EventID            int64
	OrgName            string
	OrgEmail           string
	State              relayCapacityState
	ChangedAt          time.Time
	ActiveDemand       int
	LiveFailureDomains int
	EffectiveCapacity  int
	RemainingCapacity  int
}

func relayCapacityStateFor(demand, domains, remaining int) relayCapacityState {
	if demand > 0 && (domains < 2 || remaining < demand) {
		return relayCapacityDegraded
	}
	return relayCapacityHealthy
}

func currentRelayCapacity(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, now time.Time) (relayCapacityTransition, error) {
	var state relayCapacityTransition
	err := q.QueryRow(ctx, `
		WITH live_nodes AS (
		  SELECT n.id, n.relay_group_id, n.relay_max_streams
		  FROM nodes n
		  WHERE n.account_id=$1 AND n.node_type='relay' AND n.status='active'
		    AND n.last_heartbeat_at>$2::timestamptz-interval '120 seconds'
		), domains AS (
		  SELECT 'group:'||ln.relay_group_id::text AS domain,
		         LEAST(max(rg.max_streams), sum(ln.relay_max_streams))::integer AS capacity
		  FROM live_nodes ln JOIN relay_groups rg ON rg.id=ln.relay_group_id
		  WHERE ln.relay_group_id IS NOT NULL GROUP BY ln.relay_group_id
		  UNION ALL
		  -- An ungrouped node is its own failure domain: the control plane has no
		  -- evidence that it shares an uplink with another ungrouped relay.
		  SELECT 'node:'||id::text, relay_max_streams FROM live_nodes
		  WHERE relay_group_id IS NULL
		), demand AS (
		  -- Materialized active jobs preserve scheduler timezone/window semantics.
		  SELECT count(*)::integer AS value
		  FROM recording_jobs j JOIN recordings r ON r.id=j.recording_id
		  WHERE r.account_id=$1 AND r.capture_via='relay' AND r.status='active'
		    AND j.status IN ('pending','leased')
		    AND j.fire_at<=$2
		    AND (CASE WHEN j.kind='continuous_window' THEN j.window_end_at
		              ELSE j.fire_at + make_interval(secs=>j.clip_duration_sec) END)>$2
		), totals AS (
		  SELECT count(*)::integer AS domains, coalesce(sum(capacity),0)::integer AS capacity,
		         coalesce(sum(capacity)-max(capacity),0)::integer AS remaining FROM domains
		)
		SELECT a.name, a.email, d.value, t.domains, t.capacity, t.remaining
		FROM accounts a CROSS JOIN demand d CROSS JOIN totals t WHERE a.id=$1
	`, relayConnectivityAlertAccountID, now).Scan(&state.OrgName, &state.OrgEmail, &state.ActiveDemand,
		&state.LiveFailureDomains, &state.EffectiveCapacity, &state.RemainingCapacity)
	if err != nil {
		return state, err
	}
	state.State = relayCapacityStateFor(state.ActiveDemand, state.LiveFailureDomains, state.RemainingCapacity)
	return state, nil
}

func recordRelayCapacity(ctx context.Context, pool *pgxpool.Pool, now time.Time) ([]relayCapacityTransition, error) {
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
	state, err := currentRelayCapacity(ctx, tx, now)
	if err != nil {
		return nil, err
	}
	var previous relayCapacityState
	err = tx.QueryRow(ctx, `SELECT observed_state FROM relay_capacity_alert_states WHERE account_id=$1 FOR UPDATE`, relayConnectivityAlertAccountID).Scan(&previous)
	if err == pgx.ErrNoRows {
		_, err = tx.Exec(ctx, `INSERT INTO relay_capacity_alert_states (account_id,observed_state,observed_at) VALUES ($1,$2,$3) ON CONFLICT (account_id) DO NOTHING`, relayConnectivityAlertAccountID, state.State, now)
		// A healthy initial observation is a silent baseline. A degraded initial
		// observation must page: otherwise deploying the monitor during an
		// existing single-uplink outage would suppress the condition indefinitely.
		if err == nil && state.State == relayCapacityDegraded {
			err = queueRelayCapacityEvent(ctx, tx, &state, now)
		}
	} else if err == nil && previous != state.State {
		_, err = tx.Exec(ctx, `UPDATE relay_capacity_alert_states SET observed_state=$2,observed_at=$3 WHERE account_id=$1`, relayConnectivityAlertAccountID, state.State, now)
		if err == nil {
			err = queueRelayCapacityEvent(ctx, tx, &state, now)
		}
	}
	if err != nil {
		return nil, err
	}
	pending, err := pendingRelayCapacity(ctx, tx)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return pending, nil
}

func queueRelayCapacityEvent(ctx context.Context, tx pgx.Tx, state *relayCapacityTransition, now time.Time) error {
	if err := tx.QueryRow(ctx, `INSERT INTO relay_capacity_alert_events (account_id,state,observed_at,active_demand,live_failure_domains,effective_capacity,remaining_capacity) VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING id`, relayConnectivityAlertAccountID, state.State, now, state.ActiveDemand, state.LiveFailureDomains, state.EffectiveCapacity, state.RemainingCapacity).Scan(&state.EventID); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `INSERT INTO relay_capacity_alert_deliveries (event_id,recipient) SELECT $1,email FROM users WHERE is_operator=true AND btrim(email)<>''`, state.EventID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("no operator recipients configured")
	}
	return nil
}

func pendingRelayCapacity(ctx context.Context, q interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}) ([]relayCapacityTransition, error) {
	rows, err := q.Query(ctx, `SELECT e.id,a.name,a.email,e.state,e.observed_at,e.active_demand,e.live_failure_domains,e.effective_capacity,e.remaining_capacity FROM relay_capacity_alert_events e JOIN accounts a ON a.id=e.account_id WHERE e.account_id=$1 AND e.notified_at IS NULL ORDER BY e.id`, relayConnectivityAlertAccountID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var pending []relayCapacityTransition
	for rows.Next() {
		var state relayCapacityTransition
		if err := rows.Scan(&state.EventID, &state.OrgName, &state.OrgEmail, &state.State, &state.ChangedAt, &state.ActiveDemand, &state.LiveFailureDomains, &state.EffectiveCapacity, &state.RemainingCapacity); err != nil {
			return nil, err
		}
		pending = append(pending, state)
	}
	return pending, rows.Err()
}

func deliverRelayCapacityEmail(ctx context.Context, pool *pgxpool.Pool, cfg config.Config, transitions []relayCapacityTransition) error {
	if strings.ToLower(strings.TrimSpace(cfg.EmailProvider)) != "resend" {
		return fmt.Errorf("EMAIL_PROVIDER must be resend, got %q", cfg.EmailProvider)
	}
	mailer, err := email.NewSender(email.Config{Provider: cfg.EmailProvider, From: cfg.EmailFrom, ReplyTo: cfg.EmailReplyTo, ResendKey: cfg.EmailResendAPIKey})
	if err != nil {
		return err
	}
	for _, transition := range transitions {
		rows, err := pool.Query(ctx, `SELECT recipient FROM relay_capacity_alert_deliveries WHERE event_id=$1 AND delivered_at IS NULL ORDER BY recipient`, transition.EventID)
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
			_, err := mailer.Send(ctx, email.Message{To: recipient, Subject: fmt.Sprintf("[Stoarama] Relay failover capacity is %s", transition.State), PlainText: relayCapacityBody(cfg.AppBaseURL, transition), MessageType: "relay_capacity_alert", IdempotencyKey: fmt.Sprintf("relay-capacity-%d-%x", transition.EventID, hash[:8])})
			if err != nil {
				return err
			}
			if _, err := pool.Exec(ctx, `UPDATE relay_capacity_alert_deliveries SET delivered_at=now() WHERE event_id=$1 AND recipient=$2`, transition.EventID, recipient); err != nil {
				return err
			}
		}
	}
	return nil
}

func markRelayCapacityNotified(ctx context.Context, pool *pgxpool.Pool, transitions []relayCapacityTransition) error {
	ids := make([]int64, 0, len(transitions))
	for _, transition := range transitions {
		ids = append(ids, transition.EventID)
	}
	tag, err := pool.Exec(ctx, `UPDATE relay_capacity_alert_events e SET notified_at=now() WHERE e.id=ANY($1::bigint[]) AND e.notified_at IS NULL AND EXISTS (SELECT 1 FROM relay_capacity_alert_deliveries d WHERE d.event_id=e.id) AND NOT EXISTS (SELECT 1 FROM relay_capacity_alert_deliveries d WHERE d.event_id=e.id AND d.delivered_at IS NULL)`, ids)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != int64(len(ids)) {
		return fmt.Errorf("marked %d relay capacity alerts notified, expected %d", tag.RowsAffected(), len(ids))
	}
	return nil
}

func relayCapacityBody(baseURL string, state relayCapacityTransition) string {
	var body strings.Builder
	fmt.Fprintf(&body, "Relay failover capacity is %s for %s <%s>\n", state.State, state.OrgName, state.OrgEmail)
	fmt.Fprintf(&body, "Active relay capture demand: %d\nLive uplink failure domains: %d\nTotal effective capacity: %d\nCapacity after largest uplink loss: %d\nChanged: %s\n", state.ActiveDemand, state.LiveFailureDomains, state.EffectiveCapacity, state.RemainingCapacity, state.ChangedAt.UTC().Format(time.RFC3339))
	if base := strings.TrimRight(strings.TrimSpace(baseURL), "/"); base != "" {
		fmt.Fprintf(&body, "Relays: %s/org-settings#relay-computers\n", base)
	}
	return body.String()
}
