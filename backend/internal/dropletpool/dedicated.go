package dropletpool

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	sharedPoolRole          = "shared"
	dedicatedCanaryPoolRole = "dedicated_canary"
)

func logDedicatedCleanup(operation string, err error) {
	if err != nil {
		log.Printf("dedicated canary cleanup %s: %v", operation, err)
	}
}

// DedicatedCanarySpec is an explicit, operator-authorized disposable worker
// request. It is not part of the shared autoscaler config.
type DedicatedCanarySpec struct {
	RecordingID       int64
	Owner             string
	TTL               time.Duration
	OperatorAccountID int64
	Region            string
	Size              string
	Image             string
	SSHKey            string
	ProjectID         string
	FirewallID        string
	BackendAPIURL     string
	HeartbeatSec      int
	PollSec           int
	RepoURL           string
	RepoRef           string
	BuildSHA          string
	RepoCloneToken    string
}

type DedicatedCanaryReservation struct {
	ID          uuid.UUID `json:"reservation_id"`
	RecordingID int64     `json:"recording_id"`
	WorkerName  string    `json:"worker_name"`
	Owner       string    `json:"owner"`
	ExpiresAt   time.Time `json:"expires_at"`
	State       string    `json:"state"`
	DropletID   *int64    `json:"droplet_id,omitempty"`
	WorkerState string    `json:"worker_state,omitempty"`
	WorkerReady bool      `json:"worker_ready"`
}

func validateDedicatedCanarySpec(spec DedicatedCanarySpec) error {
	if spec.RecordingID <= 0 {
		return fmt.Errorf("recording id must be positive")
	}
	if strings.TrimSpace(spec.Owner) == "" || len(strings.TrimSpace(spec.Owner)) > 128 {
		return fmt.Errorf("owner is required and must be at most 128 characters")
	}
	if spec.OperatorAccountID <= 0 {
		return fmt.Errorf("operator account id must be positive")
	}
	if spec.TTL < 10*time.Minute || spec.TTL > 24*time.Hour {
		return fmt.Errorf("ttl must be between 10m and 24h")
	}
	for name, value := range map[string]string{
		"region": spec.Region, "size": spec.Size, "image": spec.Image,
		"project id": spec.ProjectID, "firewall id": spec.FirewallID,
		"backend api url": spec.BackendAPIURL,
	} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s is required", name)
		}
	}
	return nil
}

// CreateDedicatedCanaryReservation fences one recording before a provider
// mutation. An active reservation blocks shared workers, and only its exact
// worker name may lease while the expiry is fresh.
func (s *Store) CreateDedicatedCanaryReservation(ctx context.Context, recordingID int64, owner string, ttl time.Duration) (DedicatedCanaryReservation, error) {
	owner = strings.TrimSpace(owner)
	if recordingID <= 0 || owner == "" || len(owner) > 128 {
		return DedicatedCanaryReservation{}, fmt.Errorf("recording id and owner are required")
	}
	if ttl < 10*time.Minute || ttl > 24*time.Hour {
		return DedicatedCanaryReservation{}, fmt.Errorf("ttl must be between 10m and 24h")
	}
	reservationID := uuid.New()
	// Keep the normal recorder prefix so the existing controller discovers and
	// reconciles this worker, while pool_role keeps it out of shared capacity.
	workerName := "stoarama-rec-canary-" + strings.ReplaceAll(reservationID.String(), "-", "")
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return DedicatedCanaryReservation{}, fmt.Errorf("begin dedicated canary reservation: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var captureVia, status, recordingName string
	var protected, qualified, hasWork bool
	if err := tx.QueryRow(ctx, `
		SELECT r.capture_via, r.status, r.name,
		       EXISTS (SELECT 1 FROM protected_campaign_recordings p WHERE p.recording_id=r.id),
		       EXISTS (SELECT 1 FROM recording_qualification_members m
		               JOIN recording_qualification_runs q ON q.id=m.run_id
		               WHERE m.recording_id=r.id AND q.status='active'),
		       EXISTS (SELECT 1 FROM recording_jobs j WHERE j.recording_id=r.id AND j.status IN ('pending','leased'))
		FROM recordings r WHERE r.id=$1 FOR UPDATE
	`, recordingID).Scan(&captureVia, &status, &recordingName, &protected, &qualified, &hasWork); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return DedicatedCanaryReservation{}, fmt.Errorf("recording %d not found", recordingID)
		}
		return DedicatedCanaryReservation{}, fmt.Errorf("load recording %d: %w", recordingID, err)
	}
	if strings.TrimSpace(captureVia) != "cloud" {
		return DedicatedCanaryReservation{}, fmt.Errorf("recording %d is not a cloud recording", recordingID)
	}
	if status != "active" {
		return DedicatedCanaryReservation{}, fmt.Errorf("recording %d is not active", recordingID)
	}
	if !strings.HasPrefix(strings.ToLower(strings.TrimSpace(recordingName)), "stoarama-canary-") {
		return DedicatedCanaryReservation{}, fmt.Errorf("recording %d is not explicitly disposable (name must start with stoarama-canary-)", recordingID)
	}
	if protected || qualified {
		return DedicatedCanaryReservation{}, fmt.Errorf("recording %d is protected by an active campaign or qualification run", recordingID)
	}
	if hasWork {
		return DedicatedCanaryReservation{}, fmt.Errorf("recording %d already has pending or leased work", recordingID)
	}
	var active bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS (SELECT 1 FROM recording_dedicated_canary_reservations WHERE recording_id=$1 AND state='active')
	`, recordingID).Scan(&active); err != nil {
		return DedicatedCanaryReservation{}, fmt.Errorf("check existing canary reservation: %w", err)
	}
	if active {
		return DedicatedCanaryReservation{}, fmt.Errorf("recording %d already has an active dedicated canary reservation", recordingID)
	}
	var expires time.Time
	if err := tx.QueryRow(ctx, `
		INSERT INTO recording_dedicated_canary_reservations
			(id,recording_id,worker_name,owner,expires_at)
		VALUES ($1,$2,$3,$4,now()+make_interval(secs=>$5))
		RETURNING expires_at
	`, reservationID, recordingID, workerName, owner, int(ttl/time.Second)).Scan(&expires); err != nil {
		return DedicatedCanaryReservation{}, fmt.Errorf("insert dedicated canary reservation: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return DedicatedCanaryReservation{}, fmt.Errorf("commit dedicated canary reservation: %w", err)
	}
	return DedicatedCanaryReservation{ID: reservationID, RecordingID: recordingID, WorkerName: workerName, Owner: owner, ExpiresAt: expires.UTC(), State: "active"}, nil
}

// ProvisionDedicatedCanary provisions exactly one capacity-1 worker after its
// database fence exists. Any provider/setup failure closes the reservation and
// revokes credentials; a lost provider response is left to normal reconciliation.
func ProvisionDedicatedCanary(ctx context.Context, pool *pgxpool.Pool, doClient DOClient, spec DedicatedCanarySpec) (DedicatedCanaryReservation, error) {
	if err := validateDedicatedCanarySpec(spec); err != nil {
		return DedicatedCanaryReservation{}, err
	}
	store := NewStore(pool)
	reservation, err := store.CreateDedicatedCanaryReservation(ctx, spec.RecordingID, spec.Owner, spec.TTL)
	if err != nil {
		return DedicatedCanaryReservation{}, err
	}
	token, nodeID, nodeTokenID, err := store.MintNodeToken(ctx, spec.OperatorAccountID, reservation.WorkerName)
	if err != nil {
		logDedicatedCleanup("fail reservation after token mint", store.FailDedicatedCanaryReservation(ctx, reservation.ID, spec.Owner))
		return DedicatedCanaryReservation{}, fmt.Errorf("mint dedicated canary node token: %w", err)
	}
	rowID, err := store.InsertProvisioningWithRole(ctx, reservation.WorkerName, spec.Region, spec.Size, 1, nodeID, spec.BuildSHA, dedicatedCanaryPoolRole)
	if err != nil {
		logDedicatedCleanup("revoke token after provisioning-row failure", store.RevokeNodeToken(ctx, &nodeTokenID, &nodeID))
		logDedicatedCleanup("fail reservation after provisioning-row failure", store.FailDedicatedCanaryReservation(ctx, reservation.ID, spec.Owner))
		return DedicatedCanaryReservation{}, fmt.Errorf("write dedicated canary provisioning row: %w", err)
	}
	userData, err := BuildUserData(UserDataConfig{
		ServerID: reservation.WorkerName, NodeToken: token, BackendAPIURL: spec.BackendAPIURL,
		Capacity: 1, HeartbeatSec: spec.HeartbeatSec, PollSec: spec.PollSec,
		RepoURL: spec.RepoURL, RepoRef: spec.RepoRef, BuildSHA: spec.BuildSHA,
		RepoCloneToken: spec.RepoCloneToken,
	})
	if err != nil {
		logDedicatedCleanup("mark row failed after user-data failure", store.MarkFailed(ctx, rowID, fmt.Sprintf("build user data: %v", err)))
		logDedicatedCleanup("revoke token after user-data failure", store.RevokeNodeToken(ctx, &nodeTokenID, &nodeID))
		logDedicatedCleanup("fail reservation after user-data failure", store.FailDedicatedCanaryReservation(ctx, reservation.ID, spec.Owner))
		return DedicatedCanaryReservation{}, err
	}
	droplet, err := doClient.CreateDroplet(ctx, CreateDropletInput{
		Name: reservation.WorkerName, Region: spec.Region, Size: spec.Size, Image: spec.Image,
		SSHKey: spec.SSHKey, UserData: userData, ProjectID: spec.ProjectID,
		FirewallID: spec.FirewallID, Tags: []string{"project:stoarama", "role:recorder-canary", "fleet:recorder-canary-v1", "env:prod"},
	})
	if err != nil {
		logDedicatedCleanup("mark row failed after provider failure", store.MarkFailed(ctx, rowID, fmt.Sprintf("create dedicated canary droplet: %v", err)))
		logDedicatedCleanup("revoke token after provider failure", store.RevokeNodeToken(ctx, &nodeTokenID, &nodeID))
		logDedicatedCleanup("fail reservation after provider failure", store.FailDedicatedCanaryReservation(ctx, reservation.ID, spec.Owner))
		return DedicatedCanaryReservation{}, err
	}
	if err := store.SetDropletID(ctx, rowID, droplet.ID, droplet.IP); err != nil {
		// The provider response is ambiguous. Revoke the credential and mark the
		// row failed; the existing name-based orphan reconciler will reap the
		// provider droplet after its normal timeout, without an unsafe direct delete.
		logDedicatedCleanup("mark row failed after id-recording failure", store.MarkFailed(ctx, rowID, fmt.Sprintf("record droplet id: %v", err)))
		logDedicatedCleanup("revoke token after id-recording failure", store.RevokeNodeToken(ctx, &nodeTokenID, &nodeID))
		logDedicatedCleanup("fail reservation after id-recording failure", store.FailDedicatedCanaryReservation(ctx, reservation.ID, spec.Owner))
		return DedicatedCanaryReservation{}, fmt.Errorf("record dedicated canary droplet id; provider cleanup deferred to reconciliation: %w", err)
	}
	return reservation, nil
}

func (s *Store) loadDedicatedCanaryReservation(ctx context.Context, id uuid.UUID) (DedicatedCanaryReservation, error) {
	var out DedicatedCanaryReservation
	err := s.pool.QueryRow(ctx, `
		SELECT id,recording_id,worker_name,owner,expires_at,state
		FROM recording_dedicated_canary_reservations WHERE id=$1
	`, id).Scan(&out.ID, &out.RecordingID, &out.WorkerName, &out.Owner, &out.ExpiresAt, &out.State)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return out, fmt.Errorf("dedicated canary reservation not found")
		}
		return out, err
	}
	return out, nil
}

// DedicatedCanaryStatus returns the reservation and exact worker readiness.
func (s *Store) DedicatedCanaryStatus(ctx context.Context, id uuid.UUID) (DedicatedCanaryReservation, error) {
	out, err := s.loadDedicatedCanaryReservation(ctx, id)
	if err != nil {
		return out, err
	}
	var doID *int64
	var state string
	var ready bool
	err = s.pool.QueryRow(ctx, `
		SELECT do_droplet_id,state,(last_seen_at IS NOT NULL AND last_seen_at >= now()-interval '90 seconds' AND state='active')
		FROM recorder_droplets WHERE name=$1
	`, out.WorkerName).Scan(&doID, &state, &ready)
	if errors.Is(err, pgx.ErrNoRows) {
		return out, nil
	}
	if err != nil {
		return out, fmt.Errorf("load dedicated canary worker: %w", err)
	}
	out.DropletID, out.WorkerState, out.WorkerReady = doID, state, ready
	return out, nil
}

// releaseDedicatedCanary transitions the fence first, then drains the exact
// worker. It never force-cancels an active lease.
func (s *Store) releaseDedicatedCanary(ctx context.Context, id uuid.UUID, owner string, failed bool) (DedicatedCanaryReservation, error) {
	owner = strings.TrimSpace(owner)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return DedicatedCanaryReservation{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var out DedicatedCanaryReservation
	err = tx.QueryRow(ctx, `
		SELECT id,recording_id,worker_name,owner,expires_at,state
		FROM recording_dedicated_canary_reservations WHERE id=$1 FOR UPDATE
	`, id).Scan(&out.ID, &out.RecordingID, &out.WorkerName, &out.Owner, &out.ExpiresAt, &out.State)
	if errors.Is(err, pgx.ErrNoRows) {
		return out, fmt.Errorf("dedicated canary reservation not found")
	}
	if err != nil {
		return out, err
	}
	if out.Owner != owner {
		return out, fmt.Errorf("dedicated canary owner fence mismatch")
	}
	if out.State != "active" {
		return out, fmt.Errorf("dedicated canary reservation is already %s", out.State)
	}
	newState := "released"
	if failed {
		newState = "failed"
	}
	if _, err := tx.Exec(ctx, `UPDATE recording_dedicated_canary_reservations SET state=$2,released_at=now() WHERE id=$1`, id, newState); err != nil {
		return out, err
	}
	var workerID int64
	var workerState string
	err = tx.QueryRow(ctx, `SELECT id,state FROM recorder_droplets WHERE name=$1 AND pool_role='dedicated_canary' FOR UPDATE`, out.WorkerName).Scan(&workerID, &workerState)
	if errors.Is(err, pgx.ErrNoRows) {
		if err := tx.Commit(ctx); err != nil {
			return out, err
		}
		out.State = newState
		return out, nil
	}
	if err != nil {
		return out, err
	}
	if workerState == "provisioning" || workerState == "active" {
		if _, err := tx.Exec(ctx, `UPDATE recorder_droplets SET state='draining',drain_started_at=COALESCE(drain_started_at,now()),updated_at=now() WHERE id=$1`, workerID); err != nil {
			return out, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return out, err
	}
	out.State = newState
	return out, nil
}

// ReleaseDedicatedCanary closes the owner-fenced reservation and drains its worker.
func (s *Store) ReleaseDedicatedCanary(ctx context.Context, id uuid.UUID, owner string) (DedicatedCanaryReservation, error) {
	return s.releaseDedicatedCanary(ctx, id, owner, false)
}

// FailDedicatedCanaryReservation closes a failed owner-fenced reservation.
func (s *Store) FailDedicatedCanaryReservation(ctx context.Context, id uuid.UUID, owner string) error {
	_, err := s.releaseDedicatedCanary(ctx, id, owner, true)
	return err
}
