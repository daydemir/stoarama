package dropletpool

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Droplet is the controller's view of a recorder_droplets row.
type Droplet struct {
	ID             int64
	Name           string
	NodeID         *int64
	NodeTokenID    *int64
	DODropletID    *int64
	Region         string
	Size           string
	Capacity       int
	State          string
	IPAddress      string
	LastSeenAt     *time.Time
	IdleSince      *time.Time
	DrainStartedAt *time.Time
	CreatedAt      time.Time
}

// Store is the recorder_droplets + recorder_pool_state + node-token data access
// for the autoscaler. It owns the write-ahead provisioning row and the
// per-droplet node-token mint/revoke.
type Store struct {
	pool *pgxpool.Pool
}

func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// ResolveOperatorAccount returns the id of the active admin account that owns the
// per-droplet recorder node tokens, looked up by email. The recorder nodes are
// operator infrastructure, so they are owned by the operator's account; the
// per-job S3 presign is always done from the recording's own account, never the
// node's, so this ownership never crosses tenants.
func (s *Store) ResolveOperatorAccount(ctx context.Context, email string) (int64, error) {
	em := strings.ToLower(strings.TrimSpace(email))
	if em == "" {
		return 0, fmt.Errorf("operator email is empty")
	}
	var id int64
	var role, status string
	err := s.pool.QueryRow(ctx, `SELECT id, role, status FROM accounts WHERE lower(email)=lower($1)`, em).Scan(&id, &role, &status)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, fmt.Errorf("operator account %q not found", em)
	}
	if err != nil {
		return 0, fmt.Errorf("resolve operator account: %w", err)
	}
	if status != "active" {
		return 0, fmt.Errorf("operator account %q is not active", em)
	}
	if role != "admin" {
		return 0, fmt.Errorf("operator account %q is not an admin", em)
	}
	return id, nil
}

// MintNodeToken enrolls a fresh local_recorder node named `name`, owned by
// operatorAccountID, and issues one node token for it, all in a single tx. It
// mirrors handleNodeEnroll's inserts (no enrollment-token round trip, since the
// autoscaler is trusted server-side) and returns the plaintext token (which is
// injected only into cloud-init and never persisted) plus the node + token ids.
func (s *Store) MintNodeToken(ctx context.Context, operatorAccountID int64, name string) (token string, nodeID int64, nodeTokenID int64, err error) {
	// 'node:' is the reserved lease_owner namespace for relay nodes (their canonical
	// workerID is 'node:{id}'). A droplet's display name must never fall in it, so its
	// name can never collide with a relay's lease-owner form. Droplet names are
	// operator-generated (dropletName) and never use this prefix; this is defense in depth.
	if strings.HasPrefix(strings.TrimSpace(name), "node:") {
		return "", 0, 0, fmt.Errorf("droplet name must not start with 'node:'")
	}
	rawToken, err := generateNodeSecret(36)
	if err != nil {
		return "", 0, 0, fmt.Errorf("generate node token: %w", err)
	}
	plain := "sin_" + rawToken
	tokenHash := hashNodeSecret(plain)
	tokenPrefix := plain
	if len(tokenPrefix) > 16 {
		tokenPrefix = tokenPrefix[:16]
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return "", 0, 0, fmt.Errorf("begin mint tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err = tx.QueryRow(ctx, `
		INSERT INTO nodes (account_id, node_type, display_name, hostname, platform, status, enrolled_at, last_heartbeat_at)
		VALUES ($1, 'local_recorder', $2, '', 'digitalocean', 'active', now(), now())
		RETURNING id
	`, operatorAccountID, name).Scan(&nodeID); err != nil {
		return "", 0, 0, fmt.Errorf("create recorder node: %w", err)
	}
	if err = tx.QueryRow(ctx, `
		INSERT INTO node_tokens (node_id, key_prefix, secret_hash, last_used_at)
		VALUES ($1, $2, $3, now())
		RETURNING id
	`, nodeID, tokenPrefix, tokenHash).Scan(&nodeTokenID); err != nil {
		return "", 0, 0, fmt.Errorf("create recorder node token: %w", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return "", 0, 0, fmt.Errorf("commit mint tx: %w", err)
	}
	return plain, nodeID, nodeTokenID, nil
}

// RevokeNodeToken revokes the per-droplet node token and disables its node so a
// decommissioned droplet's credential can never be reused (S-6). Both ids may be
// nil for a droplet that failed before its token was minted.
func (s *Store) RevokeNodeToken(ctx context.Context, nodeTokenID, nodeID *int64) error {
	if nodeTokenID != nil {
		if _, err := s.pool.Exec(ctx, `UPDATE node_tokens SET revoked_at=COALESCE(revoked_at, now()), updated_at=now() WHERE id=$1`, *nodeTokenID); err != nil {
			return fmt.Errorf("revoke node token: %w", err)
		}
	}
	if nodeID != nil {
		if _, err := s.pool.Exec(ctx, `UPDATE nodes SET status='disabled', updated_at=now() WHERE id=$1`, *nodeID); err != nil {
			return fmt.Errorf("disable recorder node: %w", err)
		}
	}
	return nil
}

// InsertProvisioning writes the recorder_droplets row (name + node binding +
// capacity) BEFORE any DO CreateDroplet call (write-ahead, SRE-cap), so a crash
// between create and DB-write can never leak an uncounted billing droplet: the
// row already exists and reconcile will adopt or destroy by name. The node token
// is bound through node_id (its token id is recovered via NodeBinding on
// destroy), so no node_token_id column is needed.
func (s *Store) InsertProvisioning(ctx context.Context, name, region, size string, capacity int, nodeID int64) (int64, error) {
	var id int64
	if err := s.pool.QueryRow(ctx, `
		INSERT INTO recorder_droplets (name, node_id, region, size, capacity, state)
		VALUES ($1, $2, $3, $4, $5, 'provisioning')
		RETURNING id
	`, name, nodeID, region, size, capacity).Scan(&id); err != nil {
		return 0, fmt.Errorf("insert provisioning droplet: %w", err)
	}
	return id, nil
}

// SetDropletID records the DO droplet id (and current ip) once CreateDroplet
// returns, completing the write-ahead pair.
func (s *Store) SetDropletID(ctx context.Context, id, doDropletID int64, ip string) error {
	if _, err := s.pool.Exec(ctx, `
		UPDATE recorder_droplets SET do_droplet_id=$2, ip_address=$3, updated_at=now() WHERE id=$1
	`, id, doDropletID, strings.TrimSpace(ip)); err != nil {
		return fmt.Errorf("set droplet id: %w", err)
	}
	return nil
}

// MarkActive flips a provisioning droplet to active and clears idle tracking.
func (s *Store) MarkActive(ctx context.Context, id int64) error {
	return s.setState(ctx, id, "active", `idle_since=NULL`)
}

// MarkDraining flips an active droplet to draining and stamps drain_started_at.
// The lease query already refuses new jobs to draining droplets.
func (s *Store) MarkDraining(ctx context.Context, id int64) error {
	return s.setState(ctx, id, "draining", `drain_started_at=now()`)
}

// MarkDestroying flips a droplet to destroying just before the DeleteDroplet call.
func (s *Store) MarkDestroying(ctx context.Context, id int64) error {
	return s.setState(ctx, id, "destroying", "")
}

// MarkDestroyed flips a droplet to destroyed and stamps destroyed_at.
func (s *Store) MarkDestroyed(ctx context.Context, id int64) error {
	return s.setState(ctx, id, "destroyed", `destroyed_at=now()`)
}

// MarkFailed flips a droplet to failed with a reason.
func (s *Store) MarkFailed(ctx context.Context, id int64, reason string) error {
	if _, err := s.pool.Exec(ctx, `
		UPDATE recorder_droplets SET state='failed', provision_error=$2, updated_at=now() WHERE id=$1
	`, id, strings.TrimSpace(reason)); err != nil {
		return fmt.Errorf("mark droplet failed: %w", err)
	}
	return nil
}

func (s *Store) setState(ctx context.Context, id int64, state, extra string) error {
	q := `UPDATE recorder_droplets SET state=$2, updated_at=now()`
	if strings.TrimSpace(extra) != "" {
		q += ", " + extra
	}
	q += ` WHERE id=$1`
	if _, err := s.pool.Exec(ctx, q, id, state); err != nil {
		return fmt.Errorf("set droplet state %s: %w", state, err)
	}
	return nil
}

// NodeBinding returns the node id and node token id bound to a droplet so the
// controller can revoke the credential on destroy. The token id is the only
// non-revoked token for the droplet's node.
func (s *Store) NodeBinding(ctx context.Context, id int64) (nodeID, nodeTokenID *int64, err error) {
	var nid *int64
	if err = s.pool.QueryRow(ctx, `SELECT node_id FROM recorder_droplets WHERE id=$1`, id).Scan(&nid); err != nil {
		return nil, nil, fmt.Errorf("load droplet node binding: %w", err)
	}
	if nid == nil {
		return nil, nil, nil
	}
	var tid *int64
	if err = s.pool.QueryRow(ctx, `
		SELECT id FROM node_tokens WHERE node_id=$1 AND revoked_at IS NULL ORDER BY id DESC LIMIT 1
	`, *nid).Scan(&tid); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nid, nil, nil
		}
		return nid, nil, fmt.Errorf("load droplet node token: %w", err)
	}
	return nid, tid, nil
}

// CountLive returns the number of recorder_droplets rows in a billing-relevant
// state (provisioning, active, draining, destroying). This is the spend-cap
// denominator; a destroyed/failed droplet does not count.
func (s *Store) CountLive(ctx context.Context) (int, error) {
	var n int
	if err := s.pool.QueryRow(ctx, `
		SELECT count(*) FROM recorder_droplets
		WHERE state IN ('provisioning','active','draining','destroying')
	`).Scan(&n); err != nil {
		return 0, fmt.Errorf("count live droplets: %w", err)
	}
	return n, nil
}

// ListByStates returns droplets in the given states.
func (s *Store) ListByStates(ctx context.Context, states ...string) ([]Droplet, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, name, node_id, do_droplet_id, region, size, capacity, state,
		       ip_address, last_seen_at, idle_since, drain_started_at, created_at
		FROM recorder_droplets
		WHERE state = ANY($1)
		ORDER BY id ASC
	`, states)
	if err != nil {
		return nil, fmt.Errorf("list droplets by state: %w", err)
	}
	defer rows.Close()
	out := make([]Droplet, 0, 16)
	for rows.Next() {
		var d Droplet
		if err := rows.Scan(&d.ID, &d.Name, &d.NodeID, &d.DODropletID, &d.Region, &d.Size,
			&d.Capacity, &d.State, &d.IPAddress, &d.LastSeenAt, &d.IdleSince, &d.DrainStartedAt, &d.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan droplet: %w", err)
		}
		out = append(out, d)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate droplets: %w", err)
	}
	return out, nil
}

// FindByDODropletID returns the droplet row for a DO droplet id, or (nil) if no
// row exists (an orphan).
func (s *Store) FindByDODropletID(ctx context.Context, doDropletID int64) (*Droplet, error) {
	var d Droplet
	err := s.pool.QueryRow(ctx, `
		SELECT id, name, node_id, do_droplet_id, region, size, capacity, state,
		       ip_address, last_seen_at, idle_since, drain_started_at, created_at
		FROM recorder_droplets
		WHERE do_droplet_id=$1
	`, doDropletID).Scan(&d.ID, &d.Name, &d.NodeID, &d.DODropletID, &d.Region, &d.Size,
		&d.Capacity, &d.State, &d.IPAddress, &d.LastSeenAt, &d.IdleSince, &d.DrainStartedAt, &d.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("find droplet by do id: %w", err)
	}
	return &d, nil
}

// TouchIdleSince stamps idle_since=now() on an active droplet that currently has
// no in-flight job and no idle stamp yet; it clears idle_since on a droplet that
// regained a job. Returns nothing; idempotent.
func (s *Store) SetIdleSince(ctx context.Context, id int64, idle bool) error {
	var q string
	if idle {
		q = `UPDATE recorder_droplets SET idle_since=COALESCE(idle_since, now()), updated_at=now() WHERE id=$1 AND state='active'`
	} else {
		q = `UPDATE recorder_droplets SET idle_since=NULL, updated_at=now() WHERE id=$1 AND state='active' AND idle_since IS NOT NULL`
	}
	if _, err := s.pool.Exec(ctx, q, id); err != nil {
		return fmt.Errorf("set idle_since: %w", err)
	}
	return nil
}

// HasInflightJob reports whether the named droplet currently holds a live leased
// job. It is authoritative on the lease ledger and tolerant of expired leases
// (D-isdrained): a job whose lease has expired does not count as in-flight.
func (s *Store) HasInflightJob(ctx context.Context, name string) (bool, error) {
	var exists bool
	if err := s.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM recording_jobs
			WHERE lease_owner=$1 AND status='leased' AND lease_expires_at > now()
		)
	`, name).Scan(&exists); err != nil {
		return false, fmt.Errorf("check inflight job: %w", err)
	}
	return exists, nil
}

// ReclaimExpiredLeases requeues jobs whose lease expired (C8). The autoscaler
// runs this at the top of its tick only when the scheduler is not running on the
// same service (the scheduler owns reclaim otherwise).
func (s *Store) ReclaimExpiredLeases(ctx context.Context) error {
	if _, err := s.pool.Exec(ctx, `
		UPDATE recording_jobs
		SET status='pending', lease_owner=NULL, lease_expires_at=NULL, updated_at=now()
		WHERE status='leased' AND lease_expires_at < now()
	`); err != nil {
		return fmt.Errorf("reclaim expired leases: %w", err)
	}
	return nil
}

// PoolState is the cooldown ledger.
type PoolState struct {
	LastScaleUpAt   *time.Time
	LastScaleDownAt *time.Time
}

// LoadPoolState returns the singleton cooldown ledger.
func (s *Store) LoadPoolState(ctx context.Context) (PoolState, error) {
	var ps PoolState
	err := s.pool.QueryRow(ctx, `
		SELECT last_scale_up_at, last_scale_down_at FROM recorder_pool_state WHERE id=1
	`).Scan(&ps.LastScaleUpAt, &ps.LastScaleDownAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return ps, nil
	}
	if err != nil {
		return ps, fmt.Errorf("load pool state: %w", err)
	}
	return ps, nil
}

// StampScaleUp records the most recent scale-up instant for the cooldown.
func (s *Store) StampScaleUp(ctx context.Context, at time.Time) error {
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO recorder_pool_state (id, last_scale_up_at) VALUES (1, $1)
		ON CONFLICT (id) DO UPDATE SET last_scale_up_at=$1, updated_at=now()
	`, at.UTC()); err != nil {
		return fmt.Errorf("stamp scale up: %w", err)
	}
	return nil
}

// StampScaleDown records the most recent scale-down instant for the cooldown.
func (s *Store) StampScaleDown(ctx context.Context, at time.Time) error {
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO recorder_pool_state (id, last_scale_down_at) VALUES (1, $1)
		ON CONFLICT (id) DO UPDATE SET last_scale_down_at=$1, updated_at=now()
	`, at.UTC()); err != nil {
		return fmt.Errorf("stamp scale down: %w", err)
	}
	return nil
}
