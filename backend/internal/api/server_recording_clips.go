package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/daydemir/stoarama/backend/internal/capture"
	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/daydemir/stoarama/backend/internal/recordingapi"
	"github.com/daydemir/stoarama/backend/internal/recordingnaming"
	"github.com/daydemir/stoarama/backend/internal/surrenderplan"
	"github.com/daydemir/stoarama/backend/internal/util"
)

const (
	// recordingCaptureTimeoutMarginSec and recordingUploadMarginSec extend a
	// job's lease beyond its clip duration to cover ffmpeg startup/teardown and
	// the upload. lease = clip_duration + capture margin + upload margin.
	recordingCaptureTimeoutMarginSec = 90
	recordingUploadMarginSec         = 60
	// recordingContinuousPostWindowLeaseSec is the server-side ceiling for a
	// continuous job's final, already-accepted segment deliveries: the worker's
	// 45-minute delivery retry budget plus one bounded minute to complete the job.
	// The lease fence must stand on its own so a stale or buggy worker cannot
	// heartbeat forever past window close.
	recordingContinuousPostWindowLeaseSec = 45*60 + 60
	// recordingMaxBitrateBytesPerSec bounds the presigned upload size (S-4): a
	// generous 8 MB/s, so a 900s clip caps at ~7.2 GB.
	recordingMaxBitrateBytesPerSec = 8 * 1024 * 1024
	// recordingFreshnessGraceSec is the slack added to a job's clip duration to form
	// its schedule-integrity freshness window: a job must be leasable within
	// fire_at + clip_duration_sec + this grace, or it is an honest miss rather than a
	// silently-wrong late capture (capture has no seek-to-fire, so a late clip is the
	// wrong content). This is the single source of truth for the window; the
	// scheduler's miss-marking sweep uses the same value. The grace covers normal
	// lease/poll latency and a brief autoscaler cold-boot, never minutes.
	recordingFreshnessGraceSec         = 30
	recordingLeaseTokenHeader          = "X-Stoarama-Recording-Lease-Token"
	recordingLeaseTokenSupportedHeader = "X-Stoarama-Recording-Lease-Token-Supported"
)

// recorderWorkerID is the canonical lease_owner string for a recorder principal.
// For a relay node (node_type='relay') it is the server-derived 'node:{id}', which
// is spoof-proof (the id comes from the token lookup, never client input) and cannot
// collide with a user-chosen display name across accounts. For a cloud droplet
// (node_type='local_recorder') it is the operator-assigned display name, unchanged,
// so droplet lease/complete/ingest/heartbeat ownership is byte-identical to before.
func recorderWorkerID(principal nodePrincipal) string {
	if principal.NodeType == nodeTypeRelay {
		return fmt.Sprintf("node:%d", principal.NodeID)
	}
	return strings.TrimSpace(principal.DisplayName)
}

// recordingLeaseToken reads the exact lease issuance carried by a generation-
// aware worker. A missing token is valid only for a legacy lease whose database
// token is also NULL; SQL uses IS NOT DISTINCT FROM so a legacy process can
// never mutate a token-protected replacement lease on the same machine.
func recordingLeaseToken(r *http.Request) (*uuid.UUID, error) {
	raw := strings.TrimSpace(r.Header.Get(recordingLeaseTokenHeader))
	if raw == "" {
		return nil, nil
	}
	token, err := uuid.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("invalid recording lease token")
	}
	return &token, nil
}

type recordingLeaseResponse struct {
	JobID                int64      `json:"job_id"`
	RecordingID          int64      `json:"recording_id"`
	SourceURL            string     `json:"source_url"`
	StreamID             int64      `json:"stream_id,omitempty"`
	StreamProvider       string     `json:"stream_provider,omitempty"`
	SourcePageURL        string     `json:"source_page_url,omitempty"`
	ClipDurationSec      int        `json:"clip_duration_sec"`
	StorageDestinationID int64      `json:"storage_destination_id"`
	FireAt               time.Time  `json:"fire_at"`
	AttemptCount         int        `json:"attempt_count"`
	LeaseExpiresAt       time.Time  `json:"lease_expires_at"`
	LeaseToken           *uuid.UUID `json:"lease_token,omitempty"`
	TargetFPS            *int       `json:"target_fps"`
	// Kind is 'clip' (default, per-cron-fire) or 'continuous_window' (one window-
	// long lease driving back-to-back segment capture). WindowEndAt is the
	// continuous window's close instant (zero for a clip job).
	Kind                       string     `json:"kind"`
	WindowEndAt                *time.Time `json:"window_end_at"`
	TimestampContractSupported bool       `json:"timestamp_contract_supported"`
	SurrenderTransportVersion  int        `json:"surrender_transport_version,omitempty"`
}

// relayLeaseSQL is the relay branch of handleRecordingJobsLease, entered only for a
// node_type='relay' principal. It mirrors the cloud branch's capture gate but swaps
// the recorder_droplets liveness EXISTS for account-scoped relay-node liveness plus a
// per-node capacity bound, and partitions to capture_via='relay' recordings only.
//
// Security: n.id=$1 is the authenticated node id from the token lookup (never request
// input) and n.account_id=rec.account_id is the tenant wall, both enforced in SQL, so
// a relay can only ever lease its own account's relay recordings.
//
// Capacity and failure-domain fairness are authoritative because the lease path
// locks the account, authenticated node, and optional group before running this
// statement. Heartbeats intentionally retain only node/group locks, so a slow job
// row cannot couple independent internet groups. Params: $1=NodeID,
// $2=billingDisabled, $3=margin, $4=freshnessGrace.
const relayLeaseSQL = `
	WITH cte AS (
	  SELECT j.id
	  FROM recording_jobs j
	  JOIN recordings rec ON rec.id = j.recording_id
	  JOIN nodes n ON n.id = $1
	    AND n.account_id = rec.account_id
	    AND n.node_type = 'relay'
	    AND n.status = 'active'
	    AND n.last_heartbeat_at >= now() - interval '120 seconds'
	  WHERE j.status = 'pending'
	    AND recording_surrender_relay_candidate_eligible(j.id,$1)
	    AND (NOT $5 OR EXISTS (
	          SELECT 1 FROM recording_worker_claim_heads claim
	          JOIN node_tokens claim_token ON claim_token.id=claim.claim_token_id
	          WHERE claim.node_id=$1 AND claim.state='enabled' AND claim.claim_token_id=$6
	            AND claim_token.revoked_at IS NULL AND claim_token.recording_claim_generation=claim.generation
	            AND claim_token.recording_claim_purpose='claim_current'))
	    AND j.scheduled_for <= now()
	    AND ((j.kind = 'continuous_window' AND j.window_end_at > now())
	         OR (j.kind <> 'continuous_window'
	             AND j.fire_at + make_interval(secs => (j.clip_duration_sec + $4)) > now()))
	    AND rec.status = 'active'
	    AND rec.start_at <= now()
	    AND (rec.end_at IS NULL OR now() < rec.end_at)
	    AND rec.capture_via = 'relay'
	    AND (j.handoff_owner IS NULL
	         OR j.handoff_owner <> 'node:' || $1::text
	         OR j.handoff_until <= now()
	         OR NOT recording_surrender_relay_alternate(j.id,j.handoff_owner))
	    AND ($2 OR EXISTS (
	          SELECT 1 FROM account_billing b
	          WHERE b.account_id = rec.account_id
	            AND b.has_payment_method))
	  ORDER BY j.scheduled_for ASC, j.id ASC
	  LIMIT 1
	  FOR UPDATE SKIP LOCKED
	), cleared_canaries AS (
	  -- Production always outranks a diagnostic canary. The surrounding
	  -- account/node/group locks serialize this with reservation creation; once a
	  -- real job is selected, invalidate the failure-domain reservation. The
	  -- canary polls this state and cancels promptly; a short bounded overlap is
	  -- possible while that cancellation propagates.
	  DELETE FROM recording_canary_reservations canary
	  USING cte, nodes current_node, nodes canary_node
	  WHERE current_node.id=$1
	    AND canary_node.id=canary.node_id
	    AND canary.expires_at>now()
	    AND canary_node.account_id=current_node.account_id
	    AND ((current_node.relay_group_id IS NULL AND canary_node.id=current_node.id)
	         OR (current_node.relay_group_id IS NOT NULL AND canary_node.relay_group_id=current_node.relay_group_id))
	  RETURNING canary.id
	)
	UPDATE recording_jobs j
	SET status = 'leased',
	    lease_owner = 'node:' || $1::text,
	    lease_expires_at = now() + make_interval(secs => (j.clip_duration_sec + $3)),
	    handoff_owner = NULL,
	    handoff_until = NULL,
	    attempt_count = attempt_count + 1,
	    lease_token = CASE WHEN $5 THEN gen_random_uuid() ELSE NULL END,
	    lease_node_token_id = CASE WHEN $5 THEN $6 ELSE NULL END,
	    lease_claim_generation = CASE WHEN $5 THEN (SELECT generation FROM recording_worker_claim_heads WHERE node_id=$1 AND claim_token_id=$6 AND state='enabled') ELSE NULL END,
	    lease_credential_state = CASE WHEN $5 THEN 'exact' ELSE 'legacy_unknown' END,
	    updated_at = now()
	FROM cte, recordings rec
	LEFT JOIN streams st ON st.id = rec.stream_id
	WHERE j.id = cte.id AND rec.id = j.recording_id
	RETURNING j.id, j.recording_id, rec.stream_url, COALESCE(rec.stream_id, 0),
	          COALESCE(st.provider, ''), COALESCE(st.source_page_url, ''), j.clip_duration_sec,
	          rec.storage_destination_id, j.fire_at, j.attempt_count, j.lease_expires_at,
	          rec.target_fps, j.kind, j.window_end_at, j.lease_token
`

func (s *Server) leaseRelayRecordingJob(ctx context.Context, principal nodePrincipal, billingDisabled bool, margin int, tokenSupported bool) (recordingLeaseResponse, error) {
	var resp recordingLeaseResponse
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return resp, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock_shared(hashtextextended('recording-worker-claim-v1',0))`); err != nil {
		return resp, err
	}
	var accountID int64
	if err := tx.QueryRow(ctx, `SELECT id FROM accounts WHERE id=$1 FOR UPDATE`, principal.AccountID).Scan(&accountID); err != nil {
		return resp, err
	}
	if err := lockRelayCapacityDomain(ctx, tx, principal.AccountID); err != nil {
		return resp, err
	}
	// Start the bounded fairness turn only when the oldest due relay job is first
	// considered. Using scheduled_for would let an overdue recovery batch bypass all
	// balancing immediately; setting every pending job at once would do the same after
	// three seconds. One row per poll preserves both distribution and bounded pickup.
	if _, err := tx.Exec(ctx, `
		UPDATE recording_jobs SET relay_fairness_started_at=now()
		WHERE id=(
		  SELECT j.id FROM recording_jobs j
		  JOIN recordings rec ON rec.id=j.recording_id
		  WHERE rec.account_id=$1 AND rec.capture_via='relay' AND rec.status='active'
		    AND rec.start_at<=now() AND (rec.end_at IS NULL OR now()<rec.end_at)
		    AND j.status='pending' AND j.scheduled_for<=now()
		    AND (j.handoff_owner IS NULL
		         OR j.handoff_owner<>'node:'||$2::bigint::text
		         OR j.handoff_until<=now())
		    AND ($3 OR EXISTS (
		         SELECT 1 FROM account_billing b
		         WHERE b.account_id=rec.account_id AND b.has_payment_method))
		    AND (j.kind='continuous_window'
		         OR j.fire_at+make_interval(secs=>(j.clip_duration_sec+$4))>now())
		  ORDER BY j.scheduled_for,j.id LIMIT 1
		) AND relay_fairness_started_at IS NULL
	`, principal.AccountID, principal.NodeID, billingDisabled, recordingFreshnessGraceSec); err != nil {
		return resp, err
	}
	err = tx.QueryRow(ctx, relayLeaseSQL,
		principal.NodeID, billingDisabled, margin, recordingFreshnessGraceSec, tokenSupported, principal.NodeTokenID).Scan(
		&resp.JobID, &resp.RecordingID, &resp.SourceURL, &resp.StreamID, &resp.StreamProvider, &resp.SourcePageURL, &resp.ClipDurationSec,
		&resp.StorageDestinationID, &resp.FireAt, &resp.AttemptCount, &resp.LeaseExpiresAt,
		&resp.TargetFPS, &resp.Kind, &resp.WindowEndAt, &resp.LeaseToken,
	)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return resp, err
	}
	if err == nil && resp.TargetFPS == nil && resp.LeaseToken != nil && strings.TrimSpace(s.cfg.ContinuousSourcePTSCanary) == fmt.Sprintf("%d:%d", principal.NodeID, resp.RecordingID) {
		var capable bool
		if queryErr := tx.QueryRow(ctx, `SELECT COALESCE(jsonb_typeof(capabilities_jsonb->'continuous_source_pts_v1')='boolean' AND (capabilities_jsonb->>'continuous_source_pts_v1')::boolean,false) FROM nodes WHERE id=$1 AND account_id=$2 AND node_type='relay' AND status='active' AND last_heartbeat_at>=now()-interval '2 minutes'`, principal.NodeID, principal.AccountID).Scan(&capable); queryErr != nil {
			return resp, queryErr
		}
		if capable {
			command, admitErr := tx.Exec(ctx, `INSERT INTO recording_timestamp_contract_admissions(recording_job_id,lease_token,node_id,account_id,recording_id,policy_version) VALUES($1,$2,$3,$4,$5,'continuous-source-pts-v1')`, resp.JobID, *resp.LeaseToken, principal.NodeID, principal.AccountID, resp.RecordingID)
			if admitErr != nil {
				return resp, fmt.Errorf("persist timestamp contract admission: %w", admitErr)
			}
			if command.RowsAffected() != 1 {
				return resp, fmt.Errorf("persist timestamp contract admission: unexpected row count")
			}
			resp.TimestampContractSupported = true
		}
	}
	if err == nil && tokenSupported && resp.LeaseToken != nil && resp.Kind == "continuous_window" {
		resp.SurrenderTransportVersion = 1
	}
	if commitErr := tx.Commit(ctx); commitErr != nil {
		return resp, commitErr
	}
	return resp, err
}

func lockRelayNodeAndGroup(ctx context.Context, tx pgx.Tx, principal nodePrincipal) error {
	groupID, err := lockRelayNodeRow(ctx, tx, principal.NodeID, principal.AccountID)
	if err != nil {
		return err
	}
	if groupID == nil {
		return nil
	}
	var lockedID int64
	return tx.QueryRow(ctx, `
		SELECT id FROM relay_groups WHERE id=$1 AND account_id=$2 FOR UPDATE
	`, *groupID, principal.AccountID).Scan(&lockedID)
}

func lockRelayCapacityDomain(ctx context.Context, tx pgx.Tx, accountID int64) error {
	for _, query := range []string{
		`SELECT id FROM nodes WHERE account_id=$1 AND node_type='relay' ORDER BY id FOR UPDATE`,
		`SELECT id FROM relay_groups WHERE account_id=$1 ORDER BY id FOR UPDATE`,
	} {
		rows, err := tx.Query(ctx, query, accountID)
		if err != nil {
			return err
		}
		for rows.Next() {
			var ignored int64
			if err = rows.Scan(&ignored); err != nil {
				rows.Close()
				return err
			}
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

const cloudRecorderLockSQL = `
	SELECT capacity
	FROM recorder_droplets
	WHERE name = $1 AND node_id = $2 AND state IN ('provisioning', 'active')
	FOR UPDATE
`

const cloudRecordingJobCandidateSQL = `
	SELECT j.id,rec.account_id
	FROM recording_jobs j
	JOIN recordings rec ON rec.id=j.recording_id
	WHERE j.status='pending'
	  AND j.scheduled_for<=now()
	  AND ((j.kind='continuous_window' AND j.window_end_at>now())
	       OR (j.kind<>'continuous_window'
	           AND j.fire_at+make_interval(secs=>(j.clip_duration_sec+$3))>now()))
	  AND rec.status='active'
	  AND rec.start_at<=now()
	  AND (rec.end_at IS NULL OR now()<rec.end_at)
	  AND rec.capture_via='cloud'
	  AND (j.handoff_owner IS NULL OR j.handoff_owner<>$1 OR j.handoff_until<=now()
	       OR NOT EXISTS(
	         SELECT 1 FROM recorder_droplets alternate
	         WHERE alternate.name<>$1 AND alternate.state IN('provisioning','active')
	           AND alternate.last_seen_at>=now()-interval '2 minutes'
	           AND EXISTS(SELECT 1 FROM recording_worker_claim_heads claim
	                      JOIN node_tokens claim_token ON claim_token.id=claim.claim_token_id
	                      WHERE claim.node_id=alternate.node_id AND claim.state='enabled'
	                        AND claim_token.revoked_at IS NULL
	                        AND claim_token.recording_claim_generation=claim.generation
	                        AND claim_token.recording_claim_purpose='claim_current')
	           AND (SELECT count(*) FROM recording_jobs occupied
	                WHERE occupied.status='leased' AND occupied.lease_owner=alternate.name
	                  AND occupied.lease_expires_at>now())<alternate.capacity))
	  AND ($2 OR EXISTS(SELECT 1 FROM account_billing billing
	                    WHERE billing.account_id=rec.account_id AND billing.has_payment_method))
	ORDER BY j.scheduled_for,j.id
	LIMIT 1
`

// Managed cloud recorders are operator-owned shared infrastructure and
// intentionally lease jobs across customer accounts. Storage credentials stay
// server-side and are derived from each recording's account during upload.
const cloudRecordingJobsLeaseSQL = `
	WITH cte AS (
		  SELECT j.id
	  FROM recording_jobs j
	  JOIN recordings rec ON rec.id = j.recording_id
		  WHERE j.id=$9 AND rec.account_id=$10
		    AND ((
	          SELECT count(*)
	          FROM recording_jobs live
	          WHERE live.status = 'leased'
	            AND live.lease_owner = $1
	            AND live.lease_expires_at > now()
	        ) + recording_worker_targeted_probe_occupancy($8)) < $5
	    AND (NOT $6 OR EXISTS (
	          SELECT 1 FROM recording_worker_claim_heads claim
	          JOIN node_tokens claim_token ON claim_token.id=claim.claim_token_id
	          WHERE claim.node_id=$8 AND claim.state='enabled' AND claim.claim_token_id=$7
	            AND claim_token.revoked_at IS NULL AND claim_token.recording_claim_generation=claim.generation
	            AND claim_token.recording_claim_purpose='claim_current'))
	    AND j.status = 'pending'
	    AND (j.scheduled_for <= now()
	         OR (j.handoff_owner=$1 AND j.handoff_until>now()
	             AND NOT EXISTS (
	               SELECT 1 FROM recorder_droplets retry_alternate
		               WHERE retry_alternate.name<>$1 AND retry_alternate.state IN ('provisioning','active')
		                 AND retry_alternate.last_seen_at>=now()-interval '2 minutes'
		                 AND EXISTS(SELECT 1 FROM recording_worker_claim_heads retry_claim
		                            JOIN node_tokens retry_token ON retry_token.id=retry_claim.claim_token_id
		                            WHERE retry_claim.node_id=retry_alternate.node_id AND retry_claim.state='enabled'
		                              AND retry_token.revoked_at IS NULL
		                              AND retry_token.recording_claim_generation=retry_claim.generation
		                              AND retry_token.recording_claim_purpose='claim_current')
	                 AND (SELECT count(*) FROM recording_jobs occupied
	                      WHERE occupied.status='leased' AND occupied.lease_owner=retry_alternate.name
	                        AND occupied.lease_expires_at>now()) < retry_alternate.capacity)))
	    AND ((j.kind = 'continuous_window' AND j.window_end_at > now())
	         OR (j.kind <> 'continuous_window'
	             AND j.fire_at + make_interval(secs => (j.clip_duration_sec + $4)) > now()))
	    AND rec.status = 'active'
	    AND rec.start_at <= now()
	    AND (rec.end_at IS NULL OR now() < rec.end_at)
	    AND rec.capture_via = 'cloud'
	    AND (j.handoff_owner IS NULL
	         OR j.handoff_owner <> $1
	         OR j.handoff_until <= now()
	         OR NOT EXISTS (
	              SELECT 1 FROM recorder_droplets alternate
		              WHERE alternate.name<>$1 AND alternate.state IN ('provisioning','active')
		                AND alternate.last_seen_at>=now()-interval '2 minutes'
		                AND EXISTS(SELECT 1 FROM recording_worker_claim_heads alternate_claim
		                           JOIN node_tokens alternate_token ON alternate_token.id=alternate_claim.claim_token_id
		                           WHERE alternate_claim.node_id=alternate.node_id AND alternate_claim.state='enabled'
		                             AND alternate_token.revoked_at IS NULL
		                             AND alternate_token.recording_claim_generation=alternate_claim.generation
		                             AND alternate_token.recording_claim_purpose='claim_current')
	                AND (SELECT count(*) FROM recording_jobs occupied
	                     WHERE occupied.status='leased' AND occupied.lease_owner=alternate.name
	                       AND occupied.lease_expires_at>now()) < alternate.capacity))
	    AND ($2 OR EXISTS (
	          SELECT 1 FROM account_billing b
	          WHERE b.account_id = rec.account_id
	            AND b.has_payment_method))
	  ORDER BY j.scheduled_for ASC, j.id ASC
	  LIMIT 1
	  FOR UPDATE OF j SKIP LOCKED
	)
	UPDATE recording_jobs j
	SET status = 'leased',
	    lease_owner = $1,
	    lease_expires_at = now() + make_interval(secs => (j.clip_duration_sec + $3)),
	    handoff_owner = NULL,
	    handoff_until = NULL,
	    attempt_count = attempt_count + 1,
	    lease_token = CASE WHEN $6 THEN gen_random_uuid() ELSE NULL END,
	    lease_node_token_id = CASE WHEN $6 THEN $7 ELSE NULL END,
	    lease_claim_generation = CASE WHEN $6 THEN (SELECT generation FROM recording_worker_claim_heads WHERE node_id=$8 AND claim_token_id=$7 AND state='enabled') ELSE NULL END,
	    lease_credential_state = CASE WHEN $6 THEN 'exact' ELSE 'legacy_unknown' END,
	    updated_at = now()
	FROM cte, recordings rec
	LEFT JOIN streams st ON st.id = rec.stream_id
	WHERE j.id = cte.id AND rec.id = j.recording_id
	RETURNING j.id, j.recording_id, rec.stream_url, COALESCE(rec.stream_id, 0),
	          COALESCE(st.provider, ''), COALESCE(st.source_page_url, ''), j.clip_duration_sec,
	          rec.storage_destination_id, j.fire_at, j.attempt_count, j.lease_expires_at,
	          rec.target_fps, j.kind, j.window_end_at, j.lease_token
`

// handleRecordingJobsLease leases at most one due recording job for the calling
// recorder. Cloud leases lock the authenticated droplet row in a separate statement
// before counting active leases, so concurrent requests see the preceding commit and
// cannot exceed capacity. Relay leases keep their existing account-scoped path.
func (s *Server) handleRecordingJobsLease(w http.ResponseWriter, r *http.Request) {
	principal, ok := nodePrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	workerID := recorderWorkerID(principal)
	if workerID == "" {
		util.WriteError(w, http.StatusBadRequest, "worker has no display name")
		return
	}
	billingDisabled := s.billing == nil
	margin := recordingCaptureTimeoutMarginSec + recordingUploadMarginSec
	tokenSupported := strings.EqualFold(strings.TrimSpace(r.Header.Get(recordingLeaseTokenSupportedHeader)), "true")

	var resp recordingLeaseResponse
	var err error
	if principal.NodeType == nodeTypeRelay {
		// Relay branch (node_type='relay' only). The tenant wall (n.account_id =
		// rec.account_id) and the capture_via='relay' partition live entirely in SQL;
		// n.id is the authenticated principal's node id (token lookup), never request
		// input, so a relay can never lease another account's or a cloud recording's job.
		// $1=NodeID, $2=billingDisabled, $3=margin, $4=freshnessGrace.
		resp, err = s.leaseRelayRecordingJob(r.Context(), principal, billingDisabled, margin, tokenSupported)
	} else {
		// The operator-owned cloud pool intentionally serves every account.
		// Discover the candidate tenant before taking any row lock. The transaction
		// then takes the global claim fence, locks worker and target accounts in
		// ascending identity order, and reselects this exact candidate. A stale
		// discovery loses cleanly and is retried by the next ordinary poll.
		var candidateJobID, targetAccountID int64
		candidateErr := s.pool.QueryRow(r.Context(), cloudRecordingJobCandidateSQL,
			workerID, billingDisabled, recordingFreshnessGraceSec).Scan(&candidateJobID, &targetAccountID)
		if candidateErr != nil {
			err = candidateErr
		} else {
			tx, beginErr := s.pool.Begin(r.Context())
			if beginErr != nil {
				err = fmt.Errorf("begin cloud lease: %w", beginErr)
			} else {
				defer func() { _ = tx.Rollback(r.Context()) }()
				var capacity int
				_, err = tx.Exec(r.Context(), `SELECT pg_advisory_xact_lock_shared(hashtextextended('recording-worker-claim-v1',0))`)
				if err == nil {
					_, err = tx.Exec(r.Context(), `SELECT pg_advisory_xact_lock(hashtextextended('recording-surrender-cloud-capacity-v1',0))`)
				}
				if err == nil {
					accountIDs := []int64{principal.AccountID}
					if targetAccountID != principal.AccountID {
						if targetAccountID < principal.AccountID {
							accountIDs = []int64{targetAccountID, principal.AccountID}
						} else {
							accountIDs = append(accountIDs, targetAccountID)
						}
					}
					lockedAccounts := 0
					rows, lockErr := tx.Query(r.Context(), `SELECT id FROM accounts WHERE id=ANY($1::bigint[]) ORDER BY id FOR SHARE`, accountIDs)
					if lockErr == nil {
						for rows.Next() {
							var lockedAccountID int64
							lockErr = rows.Scan(&lockedAccountID)
							if lockErr != nil {
								break
							}
							lockedAccounts++
						}
						rows.Close()
						if lockErr == nil {
							lockErr = rows.Err()
						}
						if lockErr == nil && lockedAccounts != len(accountIDs) {
							lockErr = pgx.ErrNoRows
						}
					}
					err = lockErr
				}
				if err == nil {
					err = tx.QueryRow(r.Context(), cloudRecorderLockSQL, workerID, principal.NodeID).Scan(&capacity)
				}
				if err == nil {
					err = tx.QueryRow(r.Context(), cloudRecordingJobsLeaseSQL,
						workerID, billingDisabled, margin, recordingFreshnessGraceSec, capacity, tokenSupported, principal.NodeTokenID, principal.NodeID,
						candidateJobID, targetAccountID).Scan(
						&resp.JobID, &resp.RecordingID, &resp.SourceURL, &resp.StreamID, &resp.StreamProvider, &resp.SourcePageURL, &resp.ClipDurationSec,
						&resp.StorageDestinationID, &resp.FireAt, &resp.AttemptCount, &resp.LeaseExpiresAt,
						&resp.TargetFPS, &resp.Kind, &resp.WindowEndAt, &resp.LeaseToken,
					)
				}
				if err == nil {
					if tokenSupported {
						resp.SurrenderTransportVersion = 1
					}
					err = tx.Commit(r.Context())
				}
			}
		}
	}
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteJSON(w, http.StatusOK, map[string]any{"job": nil})
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("lease recording job: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"job": resp})
}

func (s *Server) touchDropletLiveness(ctx context.Context, workerID string, nodeID int64, buildSHA string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE recorder_droplets
		SET last_seen_at=now(),
		    build_sha=CASE WHEN $3 <> '' THEN $3 ELSE build_sha END,
		    first_seen_at=COALESCE(first_seen_at, now()),
		    activated_at=COALESCE(activated_at, CASE WHEN state='active' THEN now() END)
		WHERE name=$1 AND node_id=$2
	`, workerID, nodeID, buildSHA)
	return err
}

type recordingUploadIntentRequest struct {
	JobID    int64  `json:"job_id"`
	MimeType string `json:"mime_type"`
	SHA256   string `json:"sha256"`
	// SegmentStartMs, when > 0, is the UTC start instant (Unix millis) of a
	// continuous-capture segment. The per-segment object key is derived from it so
	// each back-to-back segment of one window job gets a unique, ordered,
	// idempotent key (a re-leased window overwrites the same per-second key). It is
	// 0/ignored for an ordinary clip job, which keys off the job's fire_at.
	SegmentStartMs int64 `json:"segment_start_ms"`
}

type recordingCaptureArtifactSealRequest struct {
	JobID           int64  `json:"job_id"`
	ProducerID      string `json:"producer_id"`
	CaptureSequence int64  `json:"capture_sequence"`
	SegmentStartMs  int64  `json:"segment_start_ms"`
	SegmentStartUS  int64  `json:"segment_start_microseconds"`
	SizeBytes       int64  `json:"size_bytes"`
	SHA256          string `json:"sha256"`
	SetID           string `json:"set_id,omitempty"`
	Ordinal         int    `json:"ordinal,omitempty"`
}

type recordingUploadIntentStatus string

func surrenderTransportVersion(enabled bool) int {
	if enabled {
		return 1
	}
	return 0
}

const (
	recordingUploadIntentPending  recordingUploadIntentStatus = "pending"
	recordingUploadIntentConsumed recordingUploadIntentStatus = "consumed"
	recordingUploadIntentExpired  recordingUploadIntentStatus = "expired"
)

// handleRecordingUploadIntent presigns a PUT against the USER's bucket for a clip
// belonging to a job the caller currently holds the lease on (S-2). User S3
// credentials never leave the API; the worker only receives a presigned URL.
func (s *Server) handleRecordingUploadIntent(w http.ResponseWriter, r *http.Request) {
	principal, ok := nodePrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	if s.secrets == nil {
		util.WriteError(w, http.StatusServiceUnavailable, "storage credential key is unset")
		return
	}
	workerID := recorderWorkerID(principal)
	leaseToken, err := recordingLeaseToken(r)
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	var req recordingUploadIntentRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.JobID <= 0 {
		util.WriteError(w, http.StatusBadRequest, "job_id is required")
		return
	}
	mimeType := strings.TrimSpace(req.MimeType)
	if mimeType == "" {
		mimeType = "video/mp4"
	}

	// Load the job + recording + destination, asserting lease ownership (S-2). A
	// continuous_window job is leased window-long by this worker, so the same
	// ownership predicate covers each per-segment intent it raises.
	var (
		recordingID     int64
		clipDurationSec int
		fireAt          time.Time
		jobKind         string
		destID          int64
		endpoint        string
		region          string
		bucket          string
		keyPrefix       string
		cronTimezone    string
		namingProfile   string
		folderName      string
		namingMetadata  []byte
		accessKeyID     string
		secretEnc       []byte
	)
	err = s.pool.QueryRow(r.Context(), `
		SELECT j.recording_id, j.clip_duration_sec, j.fire_at, j.kind,
		       sd.id, sd.endpoint, sd.region, sd.bucket, sd.key_prefix,
		       rec.cron_timezone, rec.naming_profile, rec.folder_name, rec.naming_metadata_jsonb,
		       sd.access_key_id, sd.secret_access_key_enc
		FROM recording_jobs j
		JOIN recordings rec ON rec.id = j.recording_id
		JOIN storage_destinations sd ON sd.id = rec.storage_destination_id
		WHERE j.id=$1 AND j.status='leased' AND j.lease_owner=$2
		  AND j.lease_token IS NOT DISTINCT FROM $3 AND j.lease_expires_at > transaction_timestamp()
		  AND rec.status='active'
	`, req.JobID, workerID, leaseToken).Scan(
		&recordingID, &clipDurationSec, &fireAt, &jobKind,
		&destID, &endpoint, &region, &bucket, &keyPrefix,
		&cronTimezone, &namingProfile, &folderName, &namingMetadata,
		&accessKeyID, &secretEnc,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusConflict, "job is not leased by this worker or recording is not active")
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load recording job: %v", err))
		return
	}
	if err := validateStorageEndpointHTTPS(endpoint); err != nil {
		util.WriteError(w, http.StatusBadGateway, err.Error())
		return
	}

	// A reconnect can reopen the exact same HLS segment under a fresh signed URL.
	// Skip it before presigning/uploading when its content hash is already part of
	// this window job. This removes only byte-identical media; a unique tail or a
	// visually similar but different clip is always retained.
	if sha256 := strings.TrimSpace(req.SHA256); sha256 != "" {
		var alreadyIngested bool
		if err := s.pool.QueryRow(r.Context(), `
			SELECT EXISTS(
			  SELECT 1 FROM recording_clips
			  WHERE recording_job_id=$1 AND sha256=$2
			)
		`, req.JobID, sha256).Scan(&alreadyIngested); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("check duplicate recording segment: %v", err))
			return
		}
		if alreadyIngested {
			util.WriteJSON(w, http.StatusOK, map[string]any{"already_ingested": true})
			return
		}
	}

	secret, err := s.secrets.Decrypt(secretEnc)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("decrypt destination secret: %v", err))
		return
	}
	client, err := r2.New(r.Context(), r2.Config{
		AccessKey: accessKeyID,
		SecretKey: string(secret),
		Region:    region,
		Bucket:    bucket,
		Endpoint:  endpoint,
	})
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("build destination client: %v", err))
		return
	}

	// A continuous_window job raises many per-segment intents under one lease. The
	// display path is keyed by the actual segment start, so every segment remains
	// unique while all delivery surfaces reuse one path.
	clipStartedAt := fireAt
	if jobKind == "continuous_window" {
		if req.SegmentStartMs <= 0 {
			util.WriteError(w, http.StatusBadRequest, "segment_start_ms is required for a continuous window job")
			return
		}
		clipStartedAt = time.UnixMilli(req.SegmentStartMs).UTC()
	}
	profile, err := recordingnaming.ParseProfile(namingProfile)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	metadata, err := recordingnaming.ParseMetadata(namingMetadata)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	displayPath, err := recordingnaming.BuildDisplayPath(recordingnaming.Policy{
		Profile:       profile,
		JobKind:       recordingnaming.JobKind(jobKind),
		FolderName:    folderName,
		Metadata:      metadata,
		RecordingID:   recordingID,
		JobID:         req.JobID,
		CronTimezone:  cronTimezone,
		ClipStartedAt: clipStartedAt,
	})
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	objectKey := storageObjectKey(keyPrefix, displayPath)
	maxSize := int64(clipDurationSec) * recordingMaxBitrateBytesPerSec
	expiresAt := time.Now().UTC().Add(s.cfg.R2SignPutTTL)

	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("begin upload intent reservation: %v", err))
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()
	lockKey := fmt.Sprintf("%d:%d:%s:%s:%s", req.JobID, destID, endpoint, bucket, objectKey)
	if _, err := tx.Exec(r.Context(), `SELECT pg_advisory_xact_lock(hashtext($1)::bigint)`, lockKey); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("lock upload intent reservation: %v", err))
		return
	}

	intentID := uuid.New()
	responseStatus := http.StatusCreated
	var status recordingUploadIntentStatus
	err = tx.QueryRow(r.Context(), `
			SELECT id, status
			FROM recording_upload_intents
			WHERE recording_job_id=$1 AND storage_destination_id=$2
			  AND endpoint=$3 AND bucket=$4 AND object_key=$5
			ORDER BY CASE status WHEN 'consumed' THEN 0 WHEN 'pending' THEN 1 ELSE 2 END,
			         created_at DESC, id DESC
			LIMIT 1
			FOR UPDATE
		`, req.JobID, destID, endpoint, bucket, objectKey).Scan(&intentID, &status)
	if errors.Is(err, pgx.ErrNoRows) {
		intentID = uuid.New()
		if _, err := tx.Exec(r.Context(), `
			INSERT INTO recording_upload_intents
				(id, recording_id, recording_job_id, storage_destination_id, endpoint, bucket, object_key, display_path, mime_type, max_size_bytes, status, expires_at)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'pending',$11)
		`, intentID, recordingID, req.JobID, destID, endpoint, bucket, objectKey, displayPath, mimeType, maxSize, expiresAt); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("record upload intent: %v", err))
			return
		}
	} else if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load replayed upload intent: %v", err))
		return
	} else {
		if status == recordingUploadIntentConsumed {
			if err := tx.Commit(r.Context()); err != nil {
				util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("commit replayed upload intent: %v", err))
				return
			}
			util.WriteJSON(w, http.StatusOK, map[string]any{
				"intent_id":        intentID.String(),
				"object_key":       objectKey,
				"already_ingested": true,
			})
			return
		}
		if status != recordingUploadIntentPending && status != recordingUploadIntentExpired {
			util.WriteError(w, http.StatusConflict, fmt.Sprintf("upload intent cannot be replayed from status %q", status))
			return
		}
		if _, err := tx.Exec(r.Context(), `
			UPDATE recording_upload_intents
			SET status='pending', mime_type=$2, max_size_bytes=$3, expires_at=$4
			WHERE id=$1
		`, intentID, mimeType, maxSize, expiresAt); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("refresh replayed upload intent: %v", err))
			return
		}
		responseStatus = http.StatusOK
	}
	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("commit upload intent reservation: %v", err))
		return
	}

	uploadURL, err := client.PresignPut(r.Context(), objectKey, mimeType, s.cfg.R2SignPutTTL)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("presign upload: %v", err))
		return
	}

	util.WriteJSON(w, responseStatus, map[string]any{
		"intent_id": intentID.String(), "upload_url": uploadURL, "object_key": objectKey,
		"bucket": bucket, "endpoint": endpoint, "content_type": mimeType,
		"max_size_bytes": maxSize, "expires_at": expiresAt,
	})
}

func (s *Server) handleRecordingCaptureArtifactSeal(w http.ResponseWriter, r *http.Request) {
	principal, ok := nodePrincipalFromContext(r.Context())
	if !ok || s.secrets == nil {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	intentID, err := uuid.Parse(strings.TrimSpace(chiURLParam(r, "intentId")))
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, "invalid capture artifact intent")
		return
	}
	var req recordingCaptureArtifactSealRequest
	if err = util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if strings.TrimSpace(req.SetID) != "" || req.Ordinal != 0 || req.SegmentStartUS != 0 {
		s.handleRecordingCaptureSetArtifactSeal(w, r, principal, intentID, req)
		return
	}
	recovery, recovering := recordingRecoveryFromContext(r.Context())
	leaseToken, tokenErr := recordingLeaseToken(r)
	if recovering {
		leaseToken = &recovery.LeaseToken
		if recovery.IntentID != intentID || recovery.JobID != req.JobID || recovery.ProducerID.String() != strings.TrimSpace(req.ProducerID) {
			util.WriteError(w, http.StatusForbidden, "recovery capability does not authorize this exact intent")
			return
		}
	} else if tokenErr != nil || leaseToken == nil {
		util.WriteError(w, http.StatusBadRequest, "generation-fenced lease token is required")
		return
	}
	workerID := recorderWorkerID(principal)
	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "begin capture artifact seal")
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()
	if _, err = tx.Exec(r.Context(), `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, recordingSurrenderJobLockKey(req.JobID)); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "lock capture artifact seal")
		return
	}
	var endpoint, region, bucket, objectKey, mimeType, accessKeyID string
	var secretEnc []byte
	var maxSize int64
	var status recordingUploadIntentStatus
	err = tx.QueryRow(r.Context(), `
		SELECT upload.endpoint,destination.region,upload.bucket,upload.object_key,upload.mime_type,upload.max_size_bytes,
		       upload.status,destination.access_key_id,destination.secret_access_key_enc
		FROM recording_upload_intents upload
		JOIN storage_destinations destination ON destination.id=upload.storage_destination_id
		JOIN recording_capture_artifact_intents artifact ON artifact.upload_intent_id=upload.id
		JOIN recording_capture_producers producer ON producer.id=artifact.producer_id
		JOIN recording_jobs job ON job.id=producer.recording_job_id
		WHERE upload.id=$1 AND producer.id=$2 AND producer.recording_job_id=$3 AND producer.lease_token=$4
		  AND (($5 AND EXISTS(SELECT 1 FROM recording_job_recovery_grants grant_row
		         WHERE grant_row.id=$6 AND grant_row.upload_intent_id=upload.id AND grant_row.producer_id=producer.id
		           AND grant_row.upload_grace_until>transaction_timestamp()
		           AND NOT EXISTS(SELECT 1 FROM recording_job_recovery_grant_results grant_result WHERE grant_result.grant_id=grant_row.id)))
		       OR (NOT $5 AND job.status='leased' AND job.lease_token=producer.lease_token
		           AND job.lease_owner=producer.worker_id AND producer.node_id=$7 AND job.lease_expires_at>transaction_timestamp()))
		FOR UPDATE OF upload,job
	`, intentID, strings.TrimSpace(req.ProducerID), req.JobID, leaseToken, recovering, recovery.GrantID, principal.NodeID).Scan(&endpoint, &region, &bucket, &objectKey, &mimeType, &maxSize, &status, &accessKeyID, &secretEnc)
	if err != nil {
		util.WriteError(w, http.StatusConflict, "capture artifact intent authority is stale")
		return
	}
	if status == recordingUploadIntentConsumed {
		if err = sealRecordingCaptureReplay(r.Context(), tx, req, intentID); err != nil {
			util.WriteError(w, http.StatusConflict, "bind capture replay")
			return
		}
	} else if status != recordingUploadIntentPending && status != recordingUploadIntentExpired {
		util.WriteError(w, http.StatusConflict, "capture artifact intent is unavailable")
		return
	}
	if err = sealRecordingCaptureArtifact(r.Context(), tx, req, intentID, leaseToken, workerID); err != nil {
		util.WriteError(w, http.StatusConflict, fmt.Sprintf("seal capture artifact: %v", err))
		return
	}
	if _, err = tx.Exec(r.Context(), `UPDATE recording_upload_intents SET status='pending',expires_at=transaction_timestamp()+interval '15 minutes' WHERE id=$1 AND status<>'consumed'`, intentID); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "refresh exact capture intent")
		return
	}
	if err = tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "commit capture artifact seal")
		return
	}
	if status == recordingUploadIntentConsumed {
		util.WriteJSON(w, http.StatusOK, map[string]any{"intent_id": intentID.String(), "already_ingested": true})
		return
	}
	secret, err := s.secrets.Decrypt(secretEnc)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "decrypt destination secret")
		return
	}
	client, err := r2.New(r.Context(), r2.Config{AccessKey: accessKeyID, SecretKey: string(secret), Region: region, Bucket: bucket, Endpoint: endpoint})
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "build destination client")
		return
	}
	uploadURL, err := client.PresignPut(r.Context(), objectKey, mimeType, s.cfg.R2SignPutTTL)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "presign exact capture artifact")
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"intent_id": intentID.String(), "upload_url": uploadURL, "object_key": objectKey, "bucket": bucket, "endpoint": endpoint, "content_type": mimeType, "max_size_bytes": maxSize, "expires_at": time.Now().UTC().Add(s.cfg.R2SignPutTTL)})
}

func captureArtifactSemanticSHA(setID uuid.UUID, ordinal int, sourceSHA, namingSHA string, startUS, size int64, sha string) string {
	h := sha256.New()
	_, _ = h.Write([]byte("stoarama.recording.capture-artifact-semantic.v1\x00"))
	for _, value := range []string{setID.String(), strconv.Itoa(ordinal), sourceSHA, namingSHA, strconv.FormatInt(startUS, 10), strconv.FormatInt(size, 10), sha} {
		_, _ = fmt.Fprintf(h, "%d:%s\n", len(value), value)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func (s *Server) handleRecordingCaptureSetArtifactSeal(w http.ResponseWriter, r *http.Request, principal nodePrincipal, artifactID uuid.UUID, req recordingCaptureArtifactSealRequest) {
	recovery, recovering := recordingRecoveryFromContext(r.Context())
	setID, setErr := uuid.Parse(strings.TrimSpace(req.SetID))
	leaseToken, leaseErr := recordingLeaseToken(r)
	if recovering {
		leaseToken = &recovery.LeaseToken
		leaseErr = nil
	}
	sha := strings.ToLower(strings.TrimSpace(req.SHA256))
	if setErr != nil || leaseErr != nil || leaseToken == nil || req.JobID <= 0 || req.Ordinal <= 0 || req.CaptureSequence <= 0 || req.SegmentStartUS <= 0 || req.SizeBytes <= 0 || req.SizeBytes > surrenderplan.RecoveryArtifactMaxBytes || !validLowerSHA256(sha) || (recovering && (recovery.Authority != "capture_set" || recovery.IntentID != artifactID || recovery.SetID != setID || recovery.Ordinal != req.Ordinal || recovery.JobID != req.JobID || recovery.ProducerID.String() != strings.TrimSpace(req.ProducerID))) {
		util.WriteError(w, http.StatusBadRequest, "invalid capture set artifact seal")
		return
	}
	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "begin capture set artifact seal")
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()
	if _, err = tx.Exec(r.Context(), `SELECT pg_advisory_xact_lock_shared(hashtextextended('recording-worker-claim-v1',0))`); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "lock capture set artifact authority")
		return
	}
	var recordingID, destinationID int64
	var endpoint, region, bucket, keyPrefix, cronTimezone, namingProfile, folderName, sourceSHA, namingSHA string
	var namingMetadata, secretEnc []byte
	var accessKeyID string
	var expectedSequence int64
	var tokenValid, leaseCurrent bool
	err = tx.QueryRow(r.Context(), `
		SELECT plan.recording_id,(plan.destination_naming_snapshot->>'destination_id')::bigint,
		       plan.destination_naming_snapshot->>'endpoint',plan.destination_naming_snapshot->>'region',
		       plan.destination_naming_snapshot->>'bucket',plan.destination_naming_snapshot->>'key_prefix',
		       plan.destination_naming_snapshot->>'cron_timezone',plan.destination_naming_snapshot->>'naming_profile',
		       plan.destination_naming_snapshot->>'folder_name',plan.destination_naming_snapshot->'naming_metadata',
		       plan.source_snapshot_sha256,plan.destination_naming_sha256,
		       plan.first_capture_sequence+$3-1,
		       (NOT $10 AND recording_surrender_token_can_access_lease($5,$6,job.lease_node_token_id,job.lease_claim_generation))
		       OR ($10 AND EXISTS(SELECT 1 FROM recording_capture_set_grants set_grant
		          WHERE set_grant.id=$11 AND set_grant.set_id=artifact.set_id AND set_grant.upload_grace_until>transaction_timestamp()
		            AND NOT EXISTS(SELECT 1 FROM recording_capture_security_events event
		                           WHERE event.set_id=artifact.set_id AND (event.ordinal IS NULL OR event.ordinal=artifact.ordinal)))),
		       (NOT $10 AND job.status='leased' AND job.lease_owner=$7 AND job.lease_token=$4
		         AND job.lease_expires_at>transaction_timestamp() AND job.lease_credential_state='exact')
		       OR ($10 AND plan.recording_job_id=$9 AND plan.lease_token=$4),
		       destination.access_key_id,destination.secret_access_key_enc
		FROM recording_capture_materialized_artifacts artifact
		JOIN recording_capture_reservation_sets reservation ON reservation.id=artifact.set_id
		JOIN recording_capture_set_plans plan ON plan.id=reservation.plan_id
		JOIN recording_jobs job ON job.id=plan.recording_job_id
		JOIN storage_destinations destination ON destination.id=(plan.destination_naming_snapshot->>'destination_id')::bigint
		WHERE artifact.set_id=$1 AND artifact.ordinal=$3 AND artifact.artifact_id=$2
		  AND artifact.capture_sequence=$8 AND plan.recording_job_id=$9 AND plan.lease_token=$4
		FOR UPDATE OF artifact,reservation,plan,job,destination
	`, setID, artifactID, req.Ordinal, leaseToken, principal.NodeTokenID, principal.NodeID, recorderWorkerID(principal), req.CaptureSequence, req.JobID, recovering, recovery.GrantID).Scan(
		&recordingID, &destinationID, &endpoint, &region, &bucket, &keyPrefix, &cronTimezone, &namingProfile,
		&folderName, &namingMetadata, &sourceSHA, &namingSHA, &expectedSequence, &tokenValid, &leaseCurrent,
		&accessKeyID, &secretEnc)
	if err != nil || !tokenValid || !leaseCurrent || expectedSequence != req.CaptureSequence {
		util.WriteError(w, http.StatusConflict, "capture set artifact authority is stale")
		return
	}
	profile, err := recordingnaming.ParseProfile(namingProfile)
	if err != nil {
		util.WriteError(w, http.StatusConflict, "capture naming profile is invalid")
		return
	}
	metadata, err := recordingnaming.ParseMetadata(namingMetadata)
	if err != nil {
		util.WriteError(w, http.StatusConflict, "capture naming metadata is invalid")
		return
	}
	clipStart := time.Unix(0, req.SegmentStartUS*int64(time.Microsecond)).UTC()
	displayPath, err := recordingnaming.BuildDisplayPath(recordingnaming.Policy{
		Profile: profile, JobKind: recordingnaming.JobKindContinuousWindow, FolderName: folderName,
		Metadata: metadata, RecordingID: recordingID, JobID: req.JobID, CronTimezone: cronTimezone, ClipStartedAt: clipStart,
	})
	if err != nil {
		util.WriteError(w, http.StatusConflict, "derive frozen capture destination")
		return
	}
	objectKey := storageObjectKey(keyPrefix, displayPath)
	semanticSHA := captureArtifactSemanticSHA(setID, req.Ordinal, sourceSHA, namingSHA, req.SegmentStartUS, req.SizeBytes, sha)
	if _, err = tx.Exec(r.Context(), `SELECT pg_advisory_xact_lock(hashtextextended('recording-object-key-v1:'||$1::bigint::text||':'||$2::text||':'||$3::text,0))`, destinationID, bucket, objectKey); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "lock capture destination key")
		return
	}
	rootID := uuid.New()
	tag, err := tx.Exec(r.Context(), `
		INSERT INTO recording_object_key_roots(id,storage_destination_id,bucket,object_key,owner_kind,owner_identity,semantic_identity_sha256)
		VALUES($1,$2,$3,$4,'capture_artifact',$5,$6) ON CONFLICT(storage_destination_id,bucket,object_key) DO NOTHING
	`, rootID, destinationID, bucket, objectKey, setID.String()+":"+strconv.Itoa(req.Ordinal)+":"+artifactID.String(), semanticSHA)
	if err != nil {
		util.WriteError(w, http.StatusConflict, "reserve capture destination key")
		return
	}
	if tag.RowsAffected() == 0 {
		var ownerKind, ownerIdentity, existingSemantic string
		if err = tx.QueryRow(r.Context(), `SELECT id,owner_kind,owner_identity,semantic_identity_sha256 FROM recording_object_key_roots WHERE storage_destination_id=$1 AND bucket=$2 AND object_key=$3 FOR UPDATE`, destinationID, bucket, objectKey).Scan(&rootID, &ownerKind, &ownerIdentity, &existingSemantic); err != nil || ownerKind != "capture_artifact" || ownerIdentity != setID.String()+":"+strconv.Itoa(req.Ordinal)+":"+artifactID.String() || existingSemantic != semanticSHA {
			util.WriteError(w, http.StatusConflict, "capture destination key is owned by another artifact")
			return
		}
	}
	expiresAt := time.Now().UTC().Add(s.cfg.R2SignPutTTL)
	if _, err = tx.Exec(r.Context(), `
		INSERT INTO recording_upload_intents(id,recording_id,recording_job_id,storage_destination_id,endpoint,bucket,object_key,display_path,mime_type,max_size_bytes,status,expires_at)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,'video/mp4',$9,'pending',$10)
		ON CONFLICT(id) DO NOTHING
	`, artifactID, recordingID, req.JobID, destinationID, endpoint, bucket, objectKey, displayPath, surrenderplan.RecoveryArtifactMaxBytes, expiresAt); err != nil {
		util.WriteError(w, http.StatusConflict, "create capture upload identity")
		return
	}
	var uploadStatus recordingUploadIntentStatus
	var uploadExact bool
	if err = tx.QueryRow(r.Context(), `
		SELECT recording_id=$2 AND recording_job_id=$3 AND storage_destination_id=$4
		 AND endpoint=$5 AND bucket=$6 AND object_key=$7 AND display_path=$8
		 AND mime_type='video/mp4' AND max_size_bytes=$9,status
		FROM recording_upload_intents WHERE id=$1 FOR UPDATE
	`, artifactID, recordingID, req.JobID, destinationID, endpoint, bucket, objectKey, displayPath, surrenderplan.RecoveryArtifactMaxBytes).Scan(&uploadExact, &uploadStatus); err != nil || !uploadExact {
		util.WriteError(w, http.StatusConflict, "capture upload identity replay differs")
		return
	}
	tag, err = tx.Exec(r.Context(), `
		INSERT INTO recording_capture_materialized_artifact_seals(set_id,ordinal,artifact_id,capture_sequence,segment_start_microseconds,size_bytes,sha256,storage_destination_id,object_key_root_id)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT(set_id,ordinal) DO NOTHING
	`, setID, req.Ordinal, artifactID, req.CaptureSequence, req.SegmentStartUS, req.SizeBytes, sha, destinationID, rootID)
	if err != nil {
		util.WriteError(w, http.StatusConflict, "seal capture artifact")
		return
	}
	if tag.RowsAffected() == 0 {
		var exact bool
		if err = tx.QueryRow(r.Context(), `SELECT artifact_id=$3 AND capture_sequence=$4 AND segment_start_microseconds=$5 AND size_bytes=$6 AND sha256=$7 AND storage_destination_id=$8 AND object_key_root_id=$9 FROM recording_capture_materialized_artifact_seals WHERE set_id=$1 AND ordinal=$2`, setID, req.Ordinal, artifactID, req.CaptureSequence, req.SegmentStartUS, req.SizeBytes, sha, destinationID, rootID).Scan(&exact); err != nil || !exact {
			util.WriteError(w, http.StatusConflict, "capture artifact seal replay differs")
			return
		}
	}
	if uploadStatus != recordingUploadIntentPending && uploadStatus != recordingUploadIntentExpired && uploadStatus != recordingUploadIntentConsumed {
		util.WriteError(w, http.StatusConflict, "capture upload identity is unavailable")
		return
	}
	if uploadStatus == recordingUploadIntentExpired {
		if _, err = tx.Exec(r.Context(), `UPDATE recording_upload_intents SET status='pending',expires_at=$2 WHERE id=$1 AND status='expired'`, artifactID, expiresAt); err != nil {
			util.WriteError(w, http.StatusConflict, "refresh capture upload identity")
			return
		}
		uploadStatus = recordingUploadIntentPending
	}
	if err = tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "commit capture artifact seal")
		return
	}
	if uploadStatus == recordingUploadIntentConsumed {
		util.WriteJSON(w, http.StatusOK, map[string]any{"intent_id": artifactID.String(), "already_ingested": true})
		return
	}
	if recovering {
		util.WriteJSON(w, http.StatusOK, map[string]any{"intent_id": artifactID.String(), "object_key": objectKey, "bucket": bucket, "endpoint": endpoint, "content_type": "video/mp4", "max_size_bytes": surrenderplan.RecoveryArtifactMaxBytes, "expires_at": recovery.ExpiresAt})
		return
	}
	secret, err := s.secrets.Decrypt(secretEnc)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "decrypt destination secret")
		return
	}
	client, err := r2.New(r.Context(), r2.Config{AccessKey: accessKeyID, SecretKey: string(secret), Region: region, Bucket: bucket, Endpoint: endpoint})
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "build destination client")
		return
	}
	uploadURL, err := client.PresignPut(r.Context(), objectKey, "video/mp4", s.cfg.R2SignPutTTL)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "presign capture artifact")
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"intent_id": artifactID.String(), "upload_url": uploadURL, "object_key": objectKey, "bucket": bucket, "endpoint": endpoint, "content_type": "video/mp4", "max_size_bytes": surrenderplan.RecoveryArtifactMaxBytes, "expires_at": expiresAt})
}

func sealRecordingCaptureArtifact(ctx context.Context, tx pgx.Tx, req recordingCaptureArtifactSealRequest, intentID uuid.UUID, leaseToken *uuid.UUID, workerID string) error {
	producerRaw := strings.TrimSpace(req.ProducerID)
	if producerRaw == "" {
		if req.CaptureSequence != 0 || req.SizeBytes != 0 {
			return fmt.Errorf("producer_id is required for artifact seal fields")
		}
		return nil
	}
	producerID, err := uuid.Parse(producerRaw)
	sha := strings.ToLower(strings.TrimSpace(req.SHA256))
	if err != nil || leaseToken == nil || req.CaptureSequence <= 0 || req.SegmentStartMs <= 0 || req.SizeBytes <= 0 || !validLowerSHA256(sha) {
		return fmt.Errorf("invalid capture artifact seal")
	}
	tag, err := tx.Exec(ctx, `
		INSERT INTO recording_capture_artifact_seals(upload_intent_id,producer_id,capture_sequence,segment_start_ms,size_bytes,sha256)
		SELECT $1,p.id,$2,$3,$4,$5
		FROM recording_capture_producers p
		WHERE p.id=$6 AND p.recording_job_id=$7 AND p.lease_token=$8 AND p.worker_id=$9
		  AND NOT EXISTS(SELECT 1 FROM recording_capture_producer_results r WHERE r.producer_id=p.id)
		ON CONFLICT(upload_intent_id) DO NOTHING
	`, intentID, req.CaptureSequence, req.SegmentStartMs, req.SizeBytes, sha, producerID, req.JobID, leaseToken, workerID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	var existingProducer uuid.UUID
	var sequence, segmentStart, size int64
	var existingSHA string
	if err := tx.QueryRow(ctx, `SELECT producer_id,capture_sequence,segment_start_ms,size_bytes,sha256 FROM recording_capture_artifact_seals WHERE upload_intent_id=$1`, intentID).Scan(&existingProducer, &sequence, &segmentStart, &size, &existingSHA); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("capture producer is stale or already terminal")
		}
		return err
	}
	if existingProducer != producerID || sequence != req.CaptureSequence || segmentStart != req.SegmentStartMs || size != req.SizeBytes || existingSHA != sha {
		return fmt.Errorf("capture artifact seal replay differs")
	}
	return nil
}

func sealRecordingCaptureReplay(ctx context.Context, tx pgx.Tx, req recordingCaptureArtifactSealRequest, intentID uuid.UUID) error {
	if strings.TrimSpace(req.ProducerID) == "" {
		return nil
	}
	var priorResult string
	var priorClip int64
	err := tx.QueryRow(ctx, `SELECT result,COALESCE(clip_id,0) FROM recording_capture_artifact_results WHERE upload_intent_id=$1`, intentID).Scan(&priorResult, &priorClip)
	if err == nil {
		if (priorResult != "accepted_unique" && priorResult != "exact_replay") || priorClip <= 0 {
			return fmt.Errorf("capture artifact already has a non-replay result")
		}
		return nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	var clipID, headVersion int64
	err = tx.QueryRow(ctx, `
		SELECT clip.id,head.version
		FROM recording_capture_artifact_seals seal
		JOIN recording_capture_producers producer ON producer.id=seal.producer_id
		JOIN recording_job_unique_heads head ON head.recording_job_id=producer.recording_job_id AND head.lease_token=producer.lease_token
		JOIN recording_upload_intents intent ON intent.id=seal.upload_intent_id
		JOIN recording_clips clip ON clip.recording_job_id=producer.recording_job_id
		  AND clip.storage_destination_id=intent.storage_destination_id
		  AND clip.endpoint=intent.endpoint AND clip.bucket=intent.bucket AND clip.object_key=intent.object_key
		  AND clip.size_bytes=seal.size_bytes AND clip.sha256=seal.sha256
		  AND (extract(epoch FROM clip.clip_start_at)*1000)::bigint=seal.segment_start_ms
		WHERE seal.upload_intent_id=$1
		ORDER BY clip.id LIMIT 1
	`, intentID).Scan(&clipID, &headVersion)
	if err != nil {
		return fmt.Errorf("consumed intent has no exact prior clip: %w", err)
	}
	_, err = tx.Exec(ctx, `INSERT INTO recording_capture_artifact_results(upload_intent_id,result,clip_id,head_version) VALUES($1,'exact_replay',$2,$3)`, intentID, clipID, headVersion)
	return err
}

type recordingClipIngestRequest struct {
	IntentID                 string                        `json:"intent_id"`
	JobID                    int64                         `json:"job_id"`
	SizeBytes                int64                         `json:"size_bytes"`
	ETag                     string                        `json:"etag"`
	SHA256                   string                        `json:"sha256"`
	DurationMs               int64                         `json:"duration_ms"`
	VideoCodec               string                        `json:"video_codec"`
	AudioCodec               string                        `json:"audio_codec"`
	AudioPresent             bool                          `json:"audio_present"`
	ActualFPS                *float64                      `json:"actual_fps"`
	VideoWidth               int                           `json:"video_width"`
	VideoHeight              int                           `json:"video_height"`
	Container                string                        `json:"container"`
	ResolvedURL              string                        `json:"resolved_url"`
	ClipStartAt              string                        `json:"clip_start_at"`
	ClipEndAt                string                        `json:"clip_end_at"`
	CaptureSequence          int64                         `json:"capture_sequence"`
	CaptureAttemptID         string                        `json:"capture_attempt_id"`
	TimestampContractVersion string                        `json:"timestamp_contract_version"`
	TimestampContract        *capture.TimestampContract    `json:"timestamp_contract"`
	TimestampContractStatus  string                        `json:"timestamp_contract_status"`
	TimestampContractReason  string                        `json:"timestamp_contract_reason"`
	PresentationProbe        *presentationV2IngestEnvelope `json:"presentation_probe,omitempty"`
}

func nullablePositiveInt(value int) any {
	if value > 0 {
		return value
	}
	return nil
}

func validTimestampContract(contract *capture.TimestampContract) bool {
	if contract == nil || contract.Version != 1 || contract.Mode != "muxed_source_copy" || contract.AudioSelection != "first_optional" || len(contract.Tracks) < 1 || len(contract.Tracks) > 2 {
		return false
	}
	seen := map[string]bool{}
	streamIndices := map[int]bool{}
	for _, track := range contract.Tracks {
		if track.StreamIndex < 0 || streamIndices[track.StreamIndex] || (track.MediaType != "video" && track.MediaType != "audio") || seen[track.MediaType] ||
			track.TimeBaseNum <= 0 || track.TimeBaseNum > 1_000_000_000 || track.TimeBaseDen <= 0 || track.TimeBaseDen > 1_000_000_000 || track.LastDuration <= 0 || track.UnitCount <= 0 || track.UnitCount > 100_000_000 ||
			track.LastTimestamp < track.FirstTimestamp || len(track.CodecSignatureSHA256) != 64 || !lowerHex(track.CodecSignatureSHA256) {
			return false
		}
		seen[track.MediaType] = true
		streamIndices[track.StreamIndex] = true
		if track.MediaType == "audio" && (track.SampleRate < 8000 || track.SampleRate > 768000 || track.LastSampleCount <= 0 || track.LastSampleCount > 1_000_000) {
			return false
		}
	}
	return seen["video"]
}

func nullableTimestampVersion(attemptID *uuid.UUID, status string) any {
	if attemptID == nil || status != capture.TimestampProbeComplete {
		return nil
	}
	return capture.TimestampVersionContinuousSourcePTSV1
}

func nullableTimestampStatus(attemptID *uuid.UUID, value string) any {
	if attemptID == nil {
		return nil
	}
	return value
}

func nullableTimestampReason(attemptID *uuid.UUID, value string) any {
	if attemptID == nil || value == "" {
		return nil
	}
	return value
}

// handleRecordingClipIngest records a successfully uploaded clip. In one tx it
// re-verifies the recording is still active, enforces the presigned size cap via
// a Head (S-4), inserts the clip (a 0-row ON CONFLICT is treated as an error,
// not silent success, S-3), consumes the intent, and resets recording health.
func (s *Server) handleRecordingClipIngest(w http.ResponseWriter, r *http.Request) {
	principal, ok := nodePrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	if s.secrets == nil {
		util.WriteError(w, http.StatusServiceUnavailable, "storage credential key is unset")
		return
	}
	workerID := recorderWorkerID(principal)
	leaseToken, err := recordingLeaseToken(r)
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	var req recordingClipIngestRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	recovery, recovering := recordingRecoveryFromContext(r.Context())
	if recovering {
		leaseToken = &recovery.LeaseToken
		if req.JobID != recovery.JobID {
			util.WriteError(w, http.StatusForbidden, "recovery capability does not authorize this job")
			return
		}
	}
	intentID, err := uuid.Parse(strings.TrimSpace(req.IntentID))
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, "intent_id must be a uuid")
		return
	}
	presentationAttemptID, err := validatePresentationV2IngestEnvelope(req.PresentationProbe)
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	presentationRequestSHA := ""
	if req.PresentationProbe != nil {
		if leaseToken == nil || req.CaptureSequence <= 0 || !lowerHex64(strings.ToLower(strings.TrimSpace(req.SHA256))) {
			util.WriteError(w, http.StatusBadRequest, "presentation probe requires capture_sequence and exact sha256")
			return
		}
		presentationRequestSHA = presentationV2IngestRequestSHA(intentID, req)
		prior, replayErr := loadPresentationV2IngestReplay(r.Context(), s.pool, intentID, principal, leaseToken)
		if replayErr == nil {
			if prior.RequestSHA256 != presentationRequestSHA {
				util.WriteError(w, http.StatusConflict, "presentation ingest differs from committed request")
				return
			}
			util.WriteJSON(w, http.StatusOK, presentationV2ReplayResponse(prior))
			return
		}
		if !errors.Is(replayErr, pgx.ErrNoRows) {
			util.WriteError(w, http.StatusInternalServerError, "load prior presentation ingest")
			return
		}
	}
	clipStartAt, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(req.ClipStartAt))
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, "clip_start_at must be RFC3339")
		return
	}
	clipEndAt, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(req.ClipEndAt))
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, "clip_end_at must be RFC3339")
		return
	}
	if (req.VideoWidth > 0) != (req.VideoHeight > 0) || req.VideoWidth < 0 || req.VideoHeight < 0 {
		util.WriteError(w, http.StatusBadRequest, "video_width and video_height must both be positive or both be omitted")
		return
	}
	var captureSequence *int64
	var captureAttemptID *uuid.UUID
	if req.CaptureSequence > 0 {
		captureSequence = &req.CaptureSequence
		// Rolling deploy compatibility: legacy generation-aware workers omit all
		// timestamp fields and continue to ingest. New provenance is all-or-none.
		if strings.TrimSpace(req.CaptureAttemptID) != "" || strings.TrimSpace(req.TimestampContractVersion) != "" || req.TimestampContract != nil || req.TimestampContractStatus != "" || req.TimestampContractReason != "" {
			parsed, valid := validTimestampProvenance(req)
			if !valid {
				util.WriteError(w, http.StatusBadRequest, "timestamp provenance is incomplete or invalid")
				return
			}
			captureAttemptID = &parsed
		}
	} else if strings.TrimSpace(req.CaptureAttemptID) != "" || strings.TrimSpace(req.TimestampContractVersion) != "" || req.TimestampContract != nil || req.TimestampContractStatus != "" || req.TimestampContractReason != "" {
		util.WriteError(w, http.StatusBadRequest, "timestamp provenance requires capture_sequence")
		return
	}

	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("begin ingest tx: %v", err))
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()
	if leaseToken != nil {
		if _, err := tx.Exec(r.Context(), `SELECT pg_advisory_xact_lock_shared(hashtextextended('recording-worker-claim-v1',0))`); err != nil {
			util.WriteError(w, http.StatusInternalServerError, "lock clip ingest claim authority")
			return
		}
		if _, err := tx.Exec(r.Context(), `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, recordingSurrenderJobLockKey(req.JobID)); err != nil {
			util.WriteError(w, http.StatusInternalServerError, "lock clip ingest generation")
			return
		}
	}

	// Load the intent and assert ownership via the owning job's lease (S-2).
	var (
		recordingID       int64
		jobID             int64
		destID            int64
		endpoint          string
		region            string
		bucket            string
		objectKey         string
		displayPath       string
		mimeType          string
		maxSize           int64
		fireAt            time.Time
		jobKind           string
		windowEndAt       *time.Time
		clipDurationSec   int
		recordingStatus   string
		clipNaming        string
		clipFolderName    string
		accessKeyID       string
		secretEnc         []byte
		captureLeaseToken *uuid.UUID
	)
	err = tx.QueryRow(r.Context(), `
		SELECT ui.recording_id, ui.recording_job_id, ui.storage_destination_id, ui.endpoint, sd.region,
		       ui.bucket, ui.object_key, ui.display_path, ui.mime_type, ui.max_size_bytes, j.fire_at,
		       j.kind, j.window_end_at, j.clip_duration_sec, rec.status, rec.naming_profile, rec.folder_name,
		       CASE WHEN $4 THEN $5::uuid ELSE j.lease_token END,
		       sd.access_key_id, sd.secret_access_key_enc
		FROM recording_upload_intents ui
		JOIN recording_jobs j ON j.id = ui.recording_job_id
		JOIN recordings rec ON rec.id = ui.recording_id
		JOIN storage_destinations sd ON sd.id = ui.storage_destination_id
		WHERE ui.id=$1 AND ui.status='pending' AND (
		  (NOT $4 AND j.status='leased' AND j.lease_owner=$2
		    AND j.lease_token IS NOT DISTINCT FROM $3 AND j.lease_expires_at > transaction_timestamp()
		    AND (j.lease_credential_state='legacy_unknown' OR
		         (j.lease_credential_state='exact' AND j.lease_node_token_id=$8
		          AND EXISTS(SELECT 1 FROM node_tokens token WHERE token.id=$8 AND token.node_id=$9
		            AND token.recording_claim_generation=j.lease_claim_generation
		            AND token.recording_claim_purpose IN('claim_current','existing_fence_only') AND token.revoked_at IS NULL))))
			  OR ($4 AND (($10='legacy_intent' AND EXISTS(
			    SELECT 1 FROM recording_job_recovery_grants grant_row
			    WHERE grant_row.upload_intent_id=ui.id AND grant_row.id=$6
			      AND grant_row.producer_id=$7 AND grant_row.recording_job_id=j.id AND grant_row.lease_token=$3
		      AND grant_row.upload_grace_until>transaction_timestamp()
		      AND NOT EXISTS(SELECT 1 FROM recording_job_recovery_grant_results grant_result WHERE grant_result.grant_id=grant_row.id)))
		      OR ($10='capture_set' AND EXISTS(
		        SELECT 1 FROM recording_capture_materialized_artifacts artifact
		        JOIN recording_capture_set_grants set_grant ON set_grant.set_id=artifact.set_id
		        JOIN recording_capture_reservation_sets capture_set ON capture_set.id=artifact.set_id
		        JOIN recording_capture_set_plans plan ON plan.id=capture_set.plan_id
		        WHERE artifact.artifact_id=ui.id AND artifact.set_id=$11 AND artifact.ordinal=$12
		          AND set_grant.id=$6 AND plan.producer_id=$7 AND plan.recording_job_id=j.id AND plan.lease_token=$3
		          AND set_grant.upload_grace_until>transaction_timestamp()
		          AND NOT EXISTS(SELECT 1 FROM recording_capture_artifact_grant_results result
		                         WHERE result.set_id=artifact.set_id AND result.ordinal=artifact.ordinal)
		          AND NOT EXISTS(SELECT 1 FROM recording_capture_security_events event
		                         WHERE event.set_id=artifact.set_id AND (event.ordinal IS NULL OR event.ordinal=artifact.ordinal))))))
		)
		-- Serialize ingest with generation-fenced surrender. If ingest wins, the
		-- clip commits before surrender computes had_clips; if surrender wins, this
		-- rechecks the lease after waiting and rejects the old generation.
		FOR UPDATE OF ui, j
	`, intentID, workerID, leaseToken, recovering, recovery.LeaseToken, recovery.GrantID, recovery.ProducerID, principal.NodeTokenID, principal.NodeID, recovery.Authority, recovery.SetID, recovery.Ordinal).Scan(
		&recordingID, &jobID, &destID, &endpoint, &region,
		&bucket, &objectKey, &displayPath, &mimeType, &maxSize, &fireAt,
		&jobKind, &windowEndAt, &clipDurationSec, &recordingStatus,
		&clipNaming, &clipFolderName, &captureLeaseToken,
		&accessKeyID, &secretEnc,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		if req.PresentationProbe != nil {
			prior, replayErr := loadPresentationV2IngestReplay(r.Context(), tx, intentID, principal, leaseToken)
			if replayErr == nil {
				if prior.RequestSHA256 != presentationRequestSHA {
					util.WriteError(w, http.StatusConflict, "presentation ingest differs from committed request")
					return
				}
				if err = tx.Commit(r.Context()); err != nil {
					util.WriteError(w, http.StatusInternalServerError, "commit presentation ingest replay")
					return
				}
				util.WriteJSON(w, http.StatusOK, presentationV2ReplayResponse(prior))
				return
			}
			if replayErr != nil && !errors.Is(replayErr, pgx.ErrNoRows) {
				util.WriteError(w, http.StatusInternalServerError, "load concurrent presentation ingest replay")
				return
			}
		}
		util.WriteJSON(w, http.StatusConflict, map[string]any{
			"code":  recordingapi.ErrorCodeUploadIntentUnavailable,
			"error": "upload intent not found, already consumed, or job not owned",
		})
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load upload intent: %v", err))
		return
	}
	if captureAttemptID != nil {
		if leaseToken == nil || captureLeaseToken == nil {
			util.WriteError(w, http.StatusConflict, "timestamp provenance requires a generation-fenced lease")
			return
		}
		var admitted bool
		if err := tx.QueryRow(r.Context(), `SELECT EXISTS(SELECT 1 FROM recording_timestamp_contract_admissions WHERE recording_job_id=$1 AND lease_token=$2 AND node_id=$3 AND account_id=$4 AND recording_id=$5)`, jobID, captureLeaseToken, principal.NodeID, principal.AccountID, recordingID).Scan(&admitted); err != nil || !admitted {
			util.WriteError(w, http.StatusConflict, "timestamp provenance was not admitted for this lease")
			return
		}
	}
	if recordingStatus == "canceled" {
		util.WriteError(w, http.StatusGone, "recording was canceled")
		return
	}
	// A v1 artifact seal opts this ingest into the monotonic accepted-unique
	// generation head. Lock it after the job+intent rows and before any clip
	// insertion; surrender takes the same job -> head order.
	v1Head := false
	var captureSetID *uuid.UUID
	var captureSetOrdinal *int
	var headVersion int64
	err = tx.QueryRow(r.Context(), `
		SELECT h.version
		FROM recording_capture_artifact_seals a
		JOIN recording_capture_producers p ON p.id=a.producer_id
		JOIN recording_job_unique_heads h ON h.recording_job_id=p.recording_job_id AND h.lease_token=p.lease_token
		WHERE a.upload_intent_id=$1 AND p.recording_job_id=$2 AND p.lease_token=$3
		FOR UPDATE OF h
	`, intentID, jobID, captureLeaseToken).Scan(&headVersion)
	if err == nil {
		v1Head = true
	} else if !errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusInternalServerError, "lock accepted-unique head")
		return
	}
	if !v1Head {
		var setID uuid.UUID
		var ordinal int
		err = tx.QueryRow(r.Context(), `
			SELECT seal.set_id,seal.ordinal,h.version
			FROM recording_capture_materialized_artifact_seals seal
			JOIN recording_capture_reservation_sets capture_set ON capture_set.id=seal.set_id
			JOIN recording_capture_set_plans plan ON plan.id=capture_set.plan_id
			JOIN recording_job_unique_heads h ON h.recording_job_id=plan.recording_job_id AND h.lease_token=plan.lease_token
			WHERE seal.artifact_id=$1 AND plan.recording_job_id=$2 AND plan.lease_token=$3
			FOR UPDATE OF h
		`, intentID, jobID, captureLeaseToken).Scan(&setID, &ordinal, &headVersion)
		if err == nil {
			v1Head, captureSetID, captureSetOrdinal = true, &setID, &ordinal
		} else if !errors.Is(err, pgx.ErrNoRows) {
			util.WriteError(w, http.StatusInternalServerError, "lock capture set accepted-unique head")
			return
		}
	}

	// Ingest sanity check (log only, never reject): a clip whose start lands far outside
	// its job's capture window signals a timezone bug on the recorder (e.g. a relay that
	// wrote segment strftime names in local time instead of UTC), which would otherwise
	// silently store misaligned clips. Window = [fire_at, window_end_at] for a
	// continuous_window job, else [fire_at, fire_at+clip_duration_sec]. A 15-minute slop
	// tolerates normal capture/upload latency while catching whole-hour UTC offsets.
	windowStart := fireAt
	windowEnd := fireAt.Add(time.Duration(clipDurationSec) * time.Second)
	if jobKind == "continuous_window" && windowEndAt != nil {
		windowEnd = *windowEndAt
	}
	const ingestWindowSlop = 15 * time.Minute
	if clipStartAt.Before(windowStart.Add(-ingestWindowSlop)) || clipStartAt.After(windowEnd.Add(ingestWindowSlop)) {
		log.Printf("WARNING clip ingest window sanity: clip_start_at=%s is outside job window [%s, %s] by >15m recording=%d job=%d worker=%q kind=%s (likely a recorder timezone bug)",
			clipStartAt.UTC().Format(time.RFC3339), windowStart.UTC().Format(time.RFC3339), windowEnd.UTC().Format(time.RFC3339),
			recordingID, jobID, workerID, jobKind)
	}

	// Enforce the presigned size cap by Head-ing the object in the user bucket (S-4).
	secret, err := s.secrets.Decrypt(secretEnc)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("decrypt destination secret: %v", err))
		return
	}
	client, err := r2.New(r.Context(), r2.Config{
		AccessKey: accessKeyID,
		SecretKey: string(secret),
		Region:    region,
		Bucket:    bucket,
		Endpoint:  endpoint,
	})
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("build destination client: %v", err))
		return
	}
	head, err := client.Head(r.Context(), objectKey)
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, fmt.Sprintf("uploaded object not found: %v", err))
		return
	}
	if head.SizeBytes > maxSize {
		util.WriteError(w, http.StatusRequestEntityTooLarge, fmt.Sprintf("uploaded clip %d bytes exceeds cap %d bytes", head.SizeBytes, maxSize))
		return
	}
	etag := strings.TrimSpace(req.ETag)
	if etag == "" {
		etag = head.ETag
	}

	container := strings.TrimSpace(req.Container)
	if container == "" {
		container = "mp4"
	}

	// Fallback so a probe-miss clip (recorder sent duration_ms<=0) still records a
	// real duration derived from the clip's own validated start/end span, mirroring
	// the legacy capture path. Metering never reads duration_ms (it uses the
	// timestamps), so this only corrects the stored/displayed duration.
	durationMs := req.DurationMs
	if durationMs <= 0 {
		durationMs = clipEndAt.Sub(clipStartAt).Milliseconds()
		if durationMs < 0 {
			durationMs = 0
		}
	}

	var clipID int64
	err = tx.QueryRow(r.Context(), `
		INSERT INTO recording_clips
			(recording_id, recording_job_id, storage_destination_id, endpoint, bucket, object_key, display_path,
			 mime_type, container, size_bytes, etag, sha256, duration_ms, video_codec, audio_codec,
			 audio_present, actual_fps, video_width, video_height, resolved_url, fire_at, clip_start_at, clip_end_at,
			 capture_lease_token, capture_sequence, capture_attempt_id, timestamp_contract_version, timestamp_contract, timestamp_contract_status, timestamp_contract_reason,
			 surrender_transport_version)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31)
		ON CONFLICT (bucket, object_key) DO NOTHING
		RETURNING id
	`, recordingID, jobID, destID, endpoint, bucket, objectKey,
		displayPath, mimeType, container, head.SizeBytes, etag, strings.TrimSpace(req.SHA256), durationMs,
		strings.TrimSpace(req.VideoCodec), strings.TrimSpace(req.AudioCodec), req.AudioPresent, req.ActualFPS,
		nullablePositiveInt(req.VideoWidth), nullablePositiveInt(req.VideoHeight), strings.TrimSpace(req.ResolvedURL), fireAt, clipStartAt, clipEndAt, captureLeaseToken, captureSequence, captureAttemptID, nullableTimestampVersion(captureAttemptID, req.TimestampContractStatus), req.TimestampContract, nullableTimestampStatus(captureAttemptID, req.TimestampContractStatus), nullableTimestampReason(captureAttemptID, req.TimestampContractReason), surrenderTransportVersion(v1Head)).Scan(&clipID)
	if errors.Is(err, pgx.ErrNoRows) {
		// 0-row insert means a clip already exists for this (bucket,object_key).
		// Treat as an error so the job is NOT marked done and the dropped clip
		// is never silently lost (S-3).
		util.WriteJSON(w, http.StatusConflict, map[string]any{
			"code":  recordingapi.ErrorCodeClipAlreadyIngested,
			"error": "a clip already exists for this object key",
		})
		return
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" &&
		(pgErr.ConstraintName == "uq_recording_clips_capture_sequence" || pgErr.ConstraintName == "uq_recording_clips_capture_sha256") {
		// A parallel retry can pass the preflight before the first ingest commits.
		// The generation-scoped sequence index is the concurrency backstop; expose
		// the same idempotent result as the ordinary already-ingested path.
		util.WriteJSON(w, http.StatusConflict, map[string]any{
			"code":  recordingapi.ErrorCodeClipAlreadyIngested,
			"error": "this lease generation already ingested the clip",
		})
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("insert recording clip: %v", err))
		return
	}
	// Learn the native stream's network weight from media that was actually stored.
	// This value is scheduling telemetry only: capture remains source-copy and this
	// must never become a bitrate target. Retain recent peaks and decay very slowly
	// so one quiet clip cannot make a high-bitrate stream look cheap.
	if durationMs > 0 && head.SizeBytes > 0 {
		if _, err := tx.Exec(r.Context(), `
			INSERT INTO recording_bandwidth_observations (recording_id, observed_bandwidth_bps, observed_at)
			VALUES ($1, GREATEST(1, ($3::bigint * 8000 / $2)::bigint), now())
			ON CONFLICT (recording_id) DO UPDATE SET
			  observed_bandwidth_bps=GREATEST(
			    EXCLUDED.observed_bandwidth_bps,
			    recording_bandwidth_observations.observed_bandwidth_bps * 999 / 1000),
			  observed_at=now()
		`, recordingID, durationMs, head.SizeBytes); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("update recording bandwidth observation: %v", err))
			return
		}
	}

	// Auto-enqueue a delivery transfer for a WebDAV recording. The clip was captured
	// into the account's managed staging area; if the recording has a delivery
	// destination, queue a clip_transfer_job to it (reusing the exact transfer
	// machinery as the user-initiated copy) with auto_purge_source=true so the worker
	// purges the managed staging copy after a confirmed delivery. ON CONFLICT DO
	// NOTHING keeps a retried ingest idempotent (the idempotency_key dedups on
	// clip+target). Only WebDAV recordings have a non-NULL delivery dest, so ordinary
	// recordings skip this entirely (no transfer, no purge).
	var (
		deliveryDestID *int64
		deliveryPrefix string
	)
	if err := tx.QueryRow(r.Context(), `
		SELECT rec.delivery_storage_destination_id, COALESCE(sd.key_prefix, '')
		FROM recordings rec
		LEFT JOIN storage_destinations sd ON sd.id = rec.delivery_storage_destination_id
		WHERE rec.id=$1
	`, recordingID).Scan(&deliveryDestID, &deliveryPrefix); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load delivery destination: %v", err))
		return
	}
	if deliveryDestID != nil {
		targetObjectKey := deliveryObjectKey(deliveryPrefix, recordingID, clipID, objectKey, displayPath, clipNaming, clipFolderName)
		idempotencyKey := fmt.Sprintf("xfer:%d:%d", clipID, *deliveryDestID)
		if _, err := tx.Exec(r.Context(), `
			INSERT INTO clip_transfer_jobs
				(account_id, recording_clip_id, target_storage_destination_id, target_object_key, idempotency_key, auto_purge_source)
			SELECT rec.account_id, $1, $2, $3, $4, true
			FROM recordings rec WHERE rec.id=$5
			ON CONFLICT (idempotency_key) DO NOTHING
		`, clipID, *deliveryDestID, targetObjectKey, idempotencyKey, recordingID); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("enqueue delivery transfer: %v", err))
			return
		}
	}

	if _, err := tx.Exec(r.Context(), `
		UPDATE recording_upload_intents SET status='consumed' WHERE id=$1
	`, intentID); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("consume upload intent: %v", err))
		return
	}
	if v1Head {
		if err := tx.QueryRow(r.Context(), `
			UPDATE recording_job_unique_heads
			SET version=version+1,upload_intent_id=$3,clip_id=$4,capture_sequence=$5,advanced_at=transaction_timestamp()
			WHERE recording_job_id=$1 AND lease_token=$2
			RETURNING version
		`, jobID, captureLeaseToken, intentID, clipID, captureSequence).Scan(&headVersion); err != nil {
			util.WriteError(w, http.StatusInternalServerError, "advance accepted-unique head")
			return
		}
		if captureSetID != nil && captureSetOrdinal != nil {
			if _, err := tx.Exec(r.Context(), `
				INSERT INTO recording_capture_artifact_grant_results(set_id,ordinal,result,clip_id,report_id)
				VALUES($1,$2,'accepted_unique',$3,
				  CASE WHEN $4 THEN (SELECT report.id FROM recording_capture_recovery_reports report
				                      WHERE report.set_id=$1 AND report.ordinal=$2 AND report.report_type='sealed_bytes') END)
			`, captureSetID, captureSetOrdinal, clipID, recovering && recovery.Authority == "capture_set"); err != nil {
				util.WriteError(w, http.StatusConflict, "seal accepted capture set artifact result")
				return
			}
			if recovering && recovery.Authority == "capture_set" {
				if _, err := sealCaptureSetTerminalTx(r.Context(), tx, *captureSetID); err != nil {
					util.WriteError(w, http.StatusConflict, "seal recovered capture set")
					return
				}
			}
			if _, err := tx.Exec(r.Context(), `
				INSERT INTO recording_capture_recovery_alert_events(id,set_id,event_type,dedupe_key)
				SELECT gen_random_uuid(),$1,'resolved','capture-set:'||$1::text
				WHERE EXISTS(SELECT 1 FROM recording_capture_recovery_alert_events prior
				             WHERE prior.set_id=$1 AND prior.event_type='reachable_stuck')
				ON CONFLICT(dedupe_key,event_type) DO NOTHING
			`, captureSetID); err != nil {
				util.WriteError(w, http.StatusInternalServerError, "resolve capture recovery alert")
				return
			}
		} else {
			if _, err := tx.Exec(r.Context(), `INSERT INTO recording_capture_artifact_results(upload_intent_id,result,clip_id,head_version) VALUES($1,'accepted_unique',$2,$3)`, intentID, clipID, headVersion); err != nil {
				util.WriteError(w, http.StatusConflict, "seal accepted capture artifact result")
				return
			}
		}
		if recovering && recovery.Authority == "legacy_intent" {
			if _, err := tx.Exec(r.Context(), `INSERT INTO recording_job_recovery_grant_results(grant_id,result) SELECT id,'recovery_completed' FROM recording_job_recovery_grants WHERE id=$1 AND upload_intent_id=$2 ON CONFLICT(grant_id) DO NOTHING`, recovery.GrantID, intentID); err != nil {
				util.WriteError(w, http.StatusConflict, "close exact recovery capability")
				return
			}
			if err := terminalizeRecoveredProducer(r.Context(), tx, recovery.ProducerID); err != nil {
				util.WriteError(w, http.StatusConflict, "finish recovered producer after ingest")
				return
			}
		}
		if _, err := tx.Exec(r.Context(), `
			WITH resolved AS (
			  UPDATE recording_surrender_transport_episodes
			  SET state='resolved',resolved_at=transaction_timestamp(),last_observed_at=transaction_timestamp()
			  WHERE recording_job_id=$1 AND state='open'
			  RETURNING episode_key,lease_token
			)
			INSERT INTO recording_surrender_transport_episode_events(event_key,episode_key,recording_job_id,lease_token,event_type,reason)
			SELECT gen_random_uuid(),episode_key,$1,lease_token,'resolved','accepted_unique' FROM resolved
		`, jobID); err != nil {
			util.WriteError(w, http.StatusInternalServerError, "resolve surrender transport episode")
			return
		}
	}
	var presentationResponse map[string]any
	if req.PresentationProbe != nil {
		taskID := uuid.New()
		state, retentionState := "awaiting_retention", "awaiting"
		if req.PresentationProbe.Disposition == "unavailable" {
			state, retentionState = "unavailable", "none"
		}
		responseSHA := presentationV2ResponseSHA(taskID, clipID, state)
		var deadline time.Time
		err = tx.QueryRow(r.Context(), `
			INSERT INTO recording_presentation_v2_probe_tasks(
			 id,admission_id,attempt_id,account_id,recording_id,stream_id,recording_job_id,clip_id,upload_intent_id,
				 lease_token,node_id,capture_sequence,clip_size_bytes,clip_sha256,local_upload_identity_sha256,
				 staging_identity_sha256,staging_method,staging_device_id,staging_inode_id,staging_clone_identity_sha256,
				 request_sha256,response_sha256,initial_disposition,state,retention_state,
				 unavailable_reason,absolute_deadline_at)
			SELECT $1,p.admission_id,p.id,p.account_id,p.recording_id,p.stream_id,p.recording_job_id,$2,$3,
				 p.lease_token,p.node_id,$4,$5,$6,$7,NULLIF($8,''),NULLIF($9,''),NULLIF($10,''),NULLIF($11,''),NULLIF($12,''),
				 $13,$14,$15,$16,$17,NULLIF($18,''),LEAST(now()+interval '10 minutes',a.deadline_at)
			FROM recording_presentation_v2_attempts p
			JOIN recording_presentation_v2_admissions a ON a.id=p.admission_id
			WHERE p.id=$19 AND p.account_id=$20 AND p.node_id=$21 AND p.recording_job_id=$22 AND p.lease_token=$23
			RETURNING absolute_deadline_at
		`, taskID, clipID, intentID, req.CaptureSequence, head.SizeBytes, strings.ToLower(strings.TrimSpace(req.SHA256)),
			strings.ToLower(req.PresentationProbe.LocalUploadIdentitySHA256), strings.ToLower(req.PresentationProbe.StagingIdentitySHA256),
			req.PresentationProbe.StagingMethod, req.PresentationProbe.StagingDeviceID, req.PresentationProbe.StagingInodeID,
			strings.ToLower(req.PresentationProbe.StagingCloneIdentitySHA256), presentationRequestSHA, responseSHA, req.PresentationProbe.Disposition, state, retentionState,
			req.PresentationProbe.UnavailableReason, presentationAttemptID, principal.AccountID, principal.NodeID, jobID, captureLeaseToken).Scan(&deadline)
		if errors.Is(err, pgx.ErrNoRows) {
			util.WriteError(w, http.StatusConflict, "presentation attempt unavailable for ingest")
			return
		}
		if err != nil {
			util.WriteError(w, http.StatusConflict, fmt.Sprintf("create presentation probe task: %v", err))
			return
		}
		presentationResponse = map[string]any{"task_id": taskID, "state": state, "retention_state": retentionState, "absolute_deadline_at": deadline, "request_sha256": presentationRequestSHA, "creation_response_sha256": responseSHA}
	}
	if _, err := tx.Exec(r.Context(), `
		UPDATE recordings
		SET last_clip_at=now(), consecutive_failures=0, last_error_text='', updated_at=now()
		WHERE id=$1
	`, recordingID); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("update recording last clip: %v", err))
		return
	}

	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("commit ingest tx: %v", err))
		return
	}
	response := map[string]any{"clip_id": clipID}
	if v1Head {
		response["head_version"] = headVersion
		response["head_upload_intent_id"] = intentID
	}
	if presentationResponse != nil {
		response["presentation_probe"] = presentationResponse
	}
	util.WriteJSON(w, http.StatusOK, response)
}

func validTimestampProvenance(req recordingClipIngestRequest) (uuid.UUID, bool) {
	parsed, err := uuid.Parse(strings.TrimSpace(req.CaptureAttemptID))
	complete := req.TimestampContractStatus == capture.TimestampProbeComplete && req.TimestampContractVersion == capture.TimestampVersionContinuousSourcePTSV1 && req.TimestampContractReason == "" && validTimestampContract(req.TimestampContract) && timestampContractAudioPresent(req.TimestampContract) == req.AudioPresent
	unknown := req.TimestampContractStatus == capture.TimestampProbeUnknown && req.TimestampContractVersion == "" && req.TimestampContract == nil && validTimestampContractReason(req.TimestampContractReason)
	return parsed, err == nil && (complete || unknown)
}

func validTimestampContractReason(reason string) bool {
	switch reason {
	case "missing_terminal_duration", "missing_audio_sample_count", "invalid_time_base", "probe_output_limit", "probe_unavailable":
		return true
	default:
		return false
	}
}

func timestampContractAudioPresent(contract *capture.TimestampContract) bool {
	if contract == nil {
		return false
	}
	for _, track := range contract.Tracks {
		if track.MediaType == "audio" {
			return true
		}
	}
	return false
}

const recordingJobHeartbeatSQL = `
	UPDATE recording_jobs j
	SET lease_expires_at = LEAST(
	      now() + make_interval(secs => (j.clip_duration_sec + $3)),
	      CASE WHEN j.kind='continuous_window'
	           THEN j.window_end_at + make_interval(secs => $5)
	           ELSE 'infinity'::timestamptz END
	    ), updated_at = now()
	WHERE j.id=$1 AND j.status='leased' AND j.lease_owner=$2
	  AND j.lease_token IS NOT DISTINCT FROM $4 AND j.lease_expires_at > now()
	  AND (j.lease_credential_state='legacy_unknown'
	       OR (j.lease_credential_state='exact'
	           AND recording_surrender_token_can_access_lease($6,$7,j.lease_node_token_id,j.lease_claim_generation)))
	  AND (j.kind<>'continuous_window'
	       OR (j.window_end_at IS NOT NULL
	           AND j.window_end_at + make_interval(secs => $5) > now()))
	RETURNING j.lease_expires_at
`

func (s *Server) heartbeatRecordingJob(ctx context.Context, principal nodePrincipal, jobID int64, workerID string, leaseToken *uuid.UUID) (time.Time, error) {
	expires, _, err := s.heartbeatRecordingJobState(ctx, principal, jobID, workerID, leaseToken)
	return expires, err
}

func (s *Server) heartbeatRecordingJobState(ctx context.Context, principal nodePrincipal, jobID int64, workerID string, leaseToken *uuid.UUID) (time.Time, bool, error) {
	var leaseExpiresAt time.Time
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return leaseExpiresAt, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock_shared(hashtextextended('recording-worker-claim-v1',0))`); err != nil {
		return leaseExpiresAt, false, err
	}
	if principal.NodeType == nodeTypeRelay {
		if err := lockRelayNodeAndGroup(ctx, tx, principal); err != nil {
			return leaseExpiresAt, false, err
		}
	}
	if err := tx.QueryRow(ctx, recordingJobHeartbeatSQL,
		jobID, workerID, recordingCaptureTimeoutMarginSec+recordingUploadMarginSec, leaseToken,
		recordingContinuousPostWindowLeaseSec, principal.NodeTokenID, principal.NodeID).Scan(&leaseExpiresAt); err != nil {
		return leaseExpiresAt, false, err
	}
	var stopRequired bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS(
		 SELECT 1 FROM recording_capture_producer_stop_events stop
		 JOIN recording_capture_reservation_sets capture_set ON capture_set.id=stop.set_id
		 JOIN recording_capture_set_plans plan ON plan.id=capture_set.plan_id
		 WHERE plan.recording_job_id=$1 AND plan.lease_token=$2
		   AND NOT EXISTS(SELECT 1 FROM recording_capture_producer_stop_acks ack WHERE ack.stop_event_id=stop.id))
	`, jobID, leaseToken).Scan(&stopRequired); err != nil {
		return leaseExpiresAt, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return leaseExpiresAt, false, err
	}
	return leaseExpiresAt, stopRequired, nil
}

// handleRecordingJobHeartbeat extends the lease (and touches the droplet
// liveness row). It returns 409 + a cancel signal if the job was canceled or is
// expired or no longer owned, so the worker aborts ffmpeg and skips ingest
// (D-inflight). An expired lease never revives after replacement work was leased.
func (s *Server) handleRecordingJobHeartbeat(w http.ResponseWriter, r *http.Request) {
	principal, ok := nodePrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	id, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	workerID := recorderWorkerID(principal)
	leaseToken, err := recordingLeaseToken(r)
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	leaseExpiresAt, stopRequired, err := s.heartbeatRecordingJobState(r.Context(), principal, id, workerID, leaseToken)
	if errors.Is(err, pgx.ErrNoRows) {
		// Not owned / not leased anymore (canceled, reclaimed, or completed).
		util.WriteJSON(w, http.StatusConflict, map[string]any{"cancel": true})
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("heartbeat recording job: %v", err))
		return
	}
	// Touch the droplet liveness row if this worker is a managed droplet.
	_ = s.touchDropletLiveness(r.Context(), workerID, principal.NodeID, "")
	util.WriteJSON(w, http.StatusOK, map[string]any{"cancel": false, "lease_expires_at": leaseExpiresAt, "stop_required": stopRequired})
}

// handleRecordingDropletHeartbeat records droplet liveness independent of any
// held job. An idle managed worker (no leased job) still calls this on its own
// ticker so the autoscaler can tell the worker is alive, not merely powered on:
// promotion to active and failed-node detection both key off last_seen_at. For a
// unknown local recorders are rejected by requireRecorderNodeAuth.
func (s *Server) handleRecordingDropletHeartbeat(w http.ResponseWriter, r *http.Request) {
	principal, ok := nodePrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	workerID := recorderWorkerID(principal)
	if workerID == "" {
		util.WriteError(w, http.StatusBadRequest, "worker has no display name")
		return
	}
	var req struct {
		BuildSHA string `json:"build_sha"`
	}
	if err := util.DecodeJSON(r, &req); err != nil && !errors.Is(err, io.EOF) {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	req.BuildSHA = strings.ToLower(strings.TrimSpace(req.BuildSHA))
	if req.BuildSHA != "" && !regexp.MustCompile(`^[0-9a-f]{40,64}$`).MatchString(req.BuildSHA) {
		util.WriteError(w, http.StatusBadRequest, "build_sha must be a 40-64 character lowercase hex commit")
		return
	}
	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "begin droplet heartbeat")
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()
	if _, err = tx.Exec(r.Context(), `SELECT pg_advisory_xact_lock_shared(hashtextextended('recording-worker-claim-v1',0))`); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "lock droplet heartbeat claim authority")
		return
	}
	var tokenCurrent bool
	if err = tx.QueryRow(r.Context(), `SELECT node_id=$2 AND revoked_at IS NULL FROM node_tokens WHERE id=$1 FOR SHARE`, principal.NodeTokenID, principal.NodeID).Scan(&tokenCurrent); err != nil || !tokenCurrent {
		util.WriteError(w, http.StatusUnauthorized, "droplet credential is stale")
		return
	}
	if _, err = tx.Exec(r.Context(), `
		UPDATE recorder_droplets
		SET last_seen_at=transaction_timestamp(),
		    build_sha=CASE WHEN $3<>'' THEN $3 ELSE build_sha END,
		    first_seen_at=COALESCE(first_seen_at,transaction_timestamp()),
		    activated_at=COALESCE(activated_at,CASE WHEN state='active' THEN transaction_timestamp() END)
		WHERE name=$1 AND node_id=$2
	`, workerID, principal.NodeID, req.BuildSHA); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("touch droplet liveness: %v", err))
		return
	}
	if err = tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "commit droplet heartbeat")
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// handleRecordingJobComplete marks the job done. A continuous window that produced
// no clips is a failed capture, not a successful recording.
func (s *Server) handleRecordingJobComplete(w http.ResponseWriter, r *http.Request) {
	principal, ok := nodePrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	id, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	workerID := recorderWorkerID(principal)
	leaseToken, err := recordingLeaseToken(r)
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "begin complete recording job")
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()
	if _, err = tx.Exec(r.Context(), `SELECT pg_advisory_xact_lock_shared(hashtextextended('recording-worker-claim-v1',0))`); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "lock complete recording claim authority")
		return
	}
	if err = revalidateRecordingLeaseCredential(r.Context(), tx, principal, id, leaseToken); err != nil {
		util.WriteError(w, http.StatusConflict, "job lease credential is stale")
		return
	}
	var recordingID int64
	var kind, jobError string
	err = tx.QueryRow(r.Context(), `
		SELECT j.recording_id,j.kind,COALESCE(j.error_text,'')
		FROM recording_jobs j
		WHERE j.id=$1 AND j.status='leased' AND j.lease_owner=$2
		  AND j.lease_token IS NOT DISTINCT FROM $3
		FOR UPDATE OF j
	`, id, workerID, leaseToken).Scan(&recordingID, &kind, &jobError)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusConflict, "job is not leased by this worker")
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load recording job: %v", err))
		return
	}
	var clipCount int64
	if err = tx.QueryRow(r.Context(), `SELECT count(*) FROM recording_clips WHERE recording_job_id=$1`, id).Scan(&clipCount); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "count recording job clips")
		return
	}
	if kind == "continuous_window" && clipCount == 0 {
		errText := sanitizeRecordingSurrenderError(jobError, "continuous recording produced no clips")
		if _, err := tx.Exec(r.Context(), `
			UPDATE recording_jobs
			SET status='error', completed_at=now(), lease_expires_at=NULL, error_text=$3, updated_at=now()
			WHERE id=$1 AND status='leased' AND lease_owner=$2
			  AND lease_token IS NOT DISTINCT FROM $4
		`, id, workerID, errText, leaseToken); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("mark empty continuous job failed: %v", err))
			return
		}
		if _, err := tx.Exec(r.Context(), `
			UPDATE recordings
			SET consecutive_failures = consecutive_failures + 1, last_error_text=$2, last_error_at=now(), updated_at=now()
			WHERE id=$1
		`, recordingID, errText); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("bump recording health: %v", err))
			return
		}
		if err := tx.Commit(r.Context()); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("commit complete tx: %v", err))
			return
		}
		util.WriteError(w, http.StatusConflict, errText)
		return
	}
	ct, err := tx.Exec(r.Context(), `
		UPDATE recording_jobs
		SET status='done', completed_at=now(), lease_expires_at=NULL, updated_at=now()
		WHERE id=$1 AND status='leased' AND lease_owner=$2
		  AND lease_token IS NOT DISTINCT FROM $3
	`, id, workerID, leaseToken)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("complete recording job: %v", err))
		return
	}
	if ct.RowsAffected() == 0 {
		util.WriteError(w, http.StatusConflict, "job is not leased by this worker")
		return
	}
	if err = tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "commit complete recording job")
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"ok": true})
}

type recordingJobFailRequest struct {
	ErrorText string `json:"error_text"`
}

type recordingJobSurrenderReason string

const (
	recordingJobSurrenderNoProgress   recordingJobSurrenderReason = "no_progress"
	recordingJobSurrenderDiskPressure recordingJobSurrenderReason = "disk_pressure"
	recordingJobSurrenderSelfUpdate   recordingJobSurrenderReason = "self_update"
)

func (r recordingJobSurrenderReason) valid() bool {
	return r == recordingJobSurrenderNoProgress || r == recordingJobSurrenderDiskPressure || r == recordingJobSurrenderSelfUpdate
}

type recordingJobSurrenderRequest struct {
	TransportVersion       int                         `json:"transport_version"`
	AttemptID              string                      `json:"attempt_id"`
	Reason                 recordingJobSurrenderReason `json:"reason"`
	ErrorText              string                      `json:"error_text"`
	ExpectedHeadVersion    int64                       `json:"expected_head_version"`
	ExpectedUploadIntentID string                      `json:"expected_upload_intent_id"`
	ExpectedClipID         int64                       `json:"expected_clip_id"`
	SpoolCount             int                         `json:"spool_count"`
	SpoolBytes             int64                       `json:"spool_bytes"`
	InFlightCount          int                         `json:"in_flight_count"`
}

var (
	recordingSurrenderURLRE        = regexp.MustCompile(`https?://\S+`)
	recordingSurrenderBearerRE     = regexp.MustCompile(`(?i)\b(bearer\s+)[A-Za-z0-9._~+/-]+=*`)
	recordingSurrenderTokenFieldRE = regexp.MustCompile(`(?i)\b(token|signature|credential|access_key|secret_key)=\S+`)
)

func sanitizeRecordingSurrenderError(raw, fallback string) string {
	s := strings.TrimSpace(raw)
	if s == "" {
		s = fallback
	}
	s = recordingSurrenderURLRE.ReplaceAllStringFunc(s, func(rawURL string) string {
		u, err := url.Parse(rawURL)
		if err != nil || u.Scheme == "" || u.Host == "" {
			return "[url]"
		}
		hadQuery := u.RawQuery != ""
		u.User = nil
		u.RawQuery = ""
		u.Fragment = ""
		out := u.String()
		if hadQuery {
			out += "?[query]"
		}
		return out
	})
	s = recordingSurrenderBearerRE.ReplaceAllString(s, "${1}[redacted]")
	s = recordingSurrenderTokenFieldRE.ReplaceAllString(s, "${1}=[redacted]")
	s = strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return ' '
		}
		return r
	}, s)
	runes := []rune(strings.TrimSpace(s))
	if len(runes) > 500 {
		// Keep 500 content runes plus the three-rune ellipsis pinned by the
		// regression test, for a maximum persisted length of 503 runes.
		runes = append(runes[:500], '.', '.', '.')
	}
	return string(runes)
}

const recordingJobSurrenderSQL = `
	UPDATE recording_jobs j
	SET status = 'pending',
	    scheduled_for = now(),
	    lease_owner = NULL,
	    lease_expires_at = NULL,
	    lease_token = NULL,
	    handoff_owner = $2,
	    handoff_until = now() + interval '5 minutes',
	    relay_fairness_started_at = NULL,
	    error_text = $3,
	    completed_at = NULL,
	    updated_at = now()
	WHERE j.id=$1
	  AND j.kind='continuous_window'
	  AND j.window_end_at > now()
	  AND j.status='leased'
	  AND j.lease_owner=$2
	  AND j.lease_token IS NOT DISTINCT FROM $4
	  AND j.lease_expires_at > now()
	RETURNING j.handoff_until
`

const recordingJobCloudSurrenderSQL = `
	WITH eligible AS (
	  SELECT j.id, j.attempt_count,
	         EXISTS (
	           SELECT 1 FROM recording_clips c
	           WHERE c.recording_job_id=j.id
	             AND c.capture_lease_token IS NOT DISTINCT FROM j.lease_token
	         ) AS had_clips
	  FROM recording_jobs j
	  JOIN recorder_droplets d ON d.name=$2 AND d.node_id=$5
	    AND d.state IN ('provisioning', 'active')
	  WHERE j.id=$1
	    AND j.kind='continuous_window'
	    AND j.window_end_at > now()
	    AND j.status='leased'
	    AND j.lease_owner=$2
	    AND j.lease_token IS NOT DISTINCT FROM $4
	    AND j.lease_expires_at > now()
	  FOR UPDATE OF j
	)
	UPDATE recording_jobs j
	SET status='pending',
	    scheduled_for=now() + CASE
	      WHEN eligible.had_clips THEN interval '0'
	      WHEN eligible.attempt_count <= 1 THEN interval '1 minute'
	      WHEN eligible.attempt_count = 2 THEN interval '2 minutes'
	      ELSE interval '5 minutes'
	    END,
	    lease_owner=NULL,
	    lease_expires_at=NULL,
	    lease_token=NULL,
	    handoff_owner=$2,
	    handoff_until=now()+interval '5 minutes',
	    error_text=$3,
	    completed_at=NULL,
	    updated_at=now()
	FROM eligible
	WHERE j.id=eligible.id
	RETURNING j.handoff_until, j.scheduled_for, eligible.had_clips
`

func (s *Server) handleRecordingJobSurrender(w http.ResponseWriter, r *http.Request) {
	principal, ok := nodePrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	if principal.NodeType != nodeTypeRelay && principal.NodeType != nodeTypeLocalRecorder {
		util.WriteError(w, http.StatusForbidden, "only recording workers can surrender recording jobs")
		return
	}
	leaseToken, err := recordingLeaseToken(r)
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	id, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	var req recordingJobSurrenderRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if !req.Reason.valid() {
		util.WriteError(w, http.StatusBadRequest, "invalid surrender reason")
		return
	}
	if principal.NodeType == nodeTypeLocalRecorder && req.TransportVersion == 0 && req.Reason != recordingJobSurrenderNoProgress {
		util.WriteError(w, http.StatusBadRequest, "cloud recorders can only surrender for no progress")
		return
	}
	if req.TransportVersion == 1 {
		s.handleRecordingJobSurrenderV1(w, r, principal, id, leaseToken, req)
		return
	}
	if req.TransportVersion != 0 {
		util.WriteError(w, http.StatusBadRequest, "unsupported surrender transport version")
		return
	}

	errorText := sanitizeRecordingSurrenderError(req.ErrorText, string(req.Reason))
	var handoffUntil time.Time
	var nextRetryAt time.Time
	var hadClips bool
	if principal.NodeType == nodeTypeLocalRecorder {
		err = s.pool.QueryRow(
			r.Context(),
			recordingJobCloudSurrenderSQL,
			id,
			recorderWorkerID(principal),
			errorText,
			leaseToken,
			principal.NodeID,
		).Scan(&handoffUntil, &nextRetryAt, &hadClips)
	} else {
		err = s.pool.QueryRow(
			r.Context(),
			recordingJobSurrenderSQL,
			id,
			recorderWorkerID(principal),
			errorText,
			leaseToken,
		).Scan(&handoffUntil)
		nextRetryAt = time.Now()
	}
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusConflict, "job is not an unexpired continuous lease owned by this worker")
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("surrender recording job: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{
		"handoff_until": handoffUntil,
		"next_retry_at": nextRetryAt,
		"had_clips":     hadClips,
	})
}

const recordingJobFailSQL = `
	UPDATE recording_jobs j
	SET status = CASE WHEN j.attempt_count < j.max_attempts THEN 'pending' ELSE 'error' END,
	    scheduled_for = CASE WHEN j.attempt_count < j.max_attempts THEN now() + interval '60 seconds' ELSE j.scheduled_for END,
	    lease_owner = NULL,
	    lease_expires_at = NULL,
	    lease_token = NULL,
	    error_text = $3,
	    completed_at = CASE WHEN j.attempt_count < j.max_attempts THEN NULL ELSE now() END,
	    updated_at = now()
	WHERE j.id=$1 AND j.status='leased' AND j.lease_owner=$2
	  AND j.lease_token IS NOT DISTINCT FROM $4
	  AND NOT EXISTS(
	    SELECT 1 FROM recording_capture_producers producer
	    WHERE producer.recording_job_id=j.id AND producer.lease_token=j.lease_token
	      AND NOT EXISTS(SELECT 1 FROM recording_capture_producer_results result WHERE result.producer_id=producer.id)
	  )
	  AND NOT EXISTS(
	    SELECT 1 FROM recording_capture_reservation_sets capture_set
	    JOIN recording_capture_set_plans plan ON plan.id=capture_set.plan_id
	    WHERE plan.recording_job_id=j.id AND plan.lease_token=j.lease_token
	      AND NOT EXISTS(SELECT 1 FROM recording_capture_set_results result WHERE result.set_id=capture_set.id)
	  )
	RETURNING j.recording_id
`

// handleRecordingJobFail requeues the job (status=pending, scheduled now+60s) if
// attempts remain, else marks it error, and bumps recording health fields (B-6).
func (s *Server) handleRecordingJobFail(w http.ResponseWriter, r *http.Request) {
	principal, ok := nodePrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	id, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	workerID := recorderWorkerID(principal)
	leaseToken, err := recordingLeaseToken(r)
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	var req recordingJobFailRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	errText := strings.TrimSpace(req.ErrorText)
	if errText == "" {
		errText = "recording capture failed"
	}

	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("begin fail tx: %v", err))
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()
	if _, err = tx.Exec(r.Context(), `SELECT pg_advisory_xact_lock_shared(hashtextextended('recording-worker-claim-v1',0))`); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "lock fail recording claim authority")
		return
	}
	if err = revalidateRecordingLeaseCredential(r.Context(), tx, principal, id, leaseToken); err != nil {
		util.WriteError(w, http.StatusConflict, "job lease credential is stale")
		return
	}

	var recordingID int64
	err = tx.QueryRow(r.Context(), recordingJobFailSQL, id, workerID, errText, leaseToken).Scan(&recordingID)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusConflict, "job is not leased by this worker")
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("fail recording job: %v", err))
		return
	}
	if _, err := tx.Exec(r.Context(), `
		UPDATE recordings
		SET consecutive_failures = consecutive_failures + 1, last_error_text=$2, last_error_at=now(), updated_at=now()
		WHERE id=$1
	`, recordingID, errText); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("bump recording health: %v", err))
		return
	}
	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("commit fail tx: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// buildRecordingClipObjectKey is deterministic by the cron fire instant, so a
// re-leased fire overwrites the same object (at most one clip per fire) and the
// ON CONFLICT (bucket,object_key) dedup is real (D-clip-key / D-reclaim-dupe).
func buildRecordingClipObjectKey(keyPrefix string, recordingID, jobID int64, fireAt time.Time) string {
	parts := make([]string, 0, 5)
	// Defense in depth: key_prefix is validated by sanitizeStorageKeyPrefix at
	// storage-destination create time, but destinations created before that
	// guard existed may carry unsafe segments. Drop any empty, "." or ".."
	// segment so a stored prefix can never traverse out of the recordings
	// namespace, regardless of what is in the DB.
	for _, seg := range strings.Split(strings.Trim(strings.TrimSpace(keyPrefix), "/"), "/") {
		if seg == "" || seg == "." || seg == ".." {
			continue
		}
		parts = append(parts, seg)
	}
	parts = append(parts,
		"recordings",
		fmt.Sprintf("%d", recordingID),
		fmt.Sprintf("%d", jobID),
		fmt.Sprintf("%d.mp4", fireAt.UTC().UnixMilli()),
	)
	return strings.Join(parts, "/")
}

// buildRecordingClipObjectKeyContinuous keys a continuous-capture segment on its
// per-second start instant rather than a single fire+job, so every back-to-back
// segment of one window gets a unique, ordered key and a re-leased window
// overwrites the same key (idempotent re-capture; ON CONFLICT(bucket,object_key)
// dedups). It deliberately omits the job id so a re-leased window job (a new job
// id after reclaim) lands on the SAME key for the same wall-clock second.
func buildRecordingClipObjectKeyContinuous(keyPrefix string, recordingID int64, segStart time.Time) string {
	parts := make([]string, 0, 5)
	for _, seg := range strings.Split(strings.Trim(strings.TrimSpace(keyPrefix), "/"), "/") {
		if seg == "" || seg == "." || seg == ".." {
			continue
		}
		parts = append(parts, seg)
	}
	parts = append(parts,
		"recordings",
		fmt.Sprintf("%d", recordingID),
		"continuous",
		fmt.Sprintf("%d.mp4", segStart.UTC().Unix()),
	)
	return strings.Join(parts, "/")
}

func storageObjectKey(keyPrefix, displayPath string) string {
	parts := make([]string, 0, 4)
	for _, seg := range strings.Split(strings.Trim(strings.TrimSpace(keyPrefix), "/"), "/") {
		if seg == "" || seg == "." || seg == ".." {
			continue
		}
		parts = append(parts, seg)
	}
	for _, seg := range strings.Split(strings.Trim(strings.TrimSpace(displayPath), "/"), "/") {
		if seg == "" || seg == "." || seg == ".." {
			continue
		}
		parts = append(parts, seg)
	}
	return strings.Join(parts, "/")
}

func deliveryObjectKey(keyPrefix string, recordingID, clipID int64, sourceObjectKey, displayPath, namingProfile, folderName string) string {
	if namingProfile == recordingnaming.ProfileStoaramaV1.String() && strings.TrimSpace(folderName) == "recordings" {
		return buildClipTransferObjectKey(keyPrefix, recordingID, clipID, sourceObjectKey)
	}
	return storageObjectKey(keyPrefix, displayPath)
}

// accountClipsCursorSQL forward-cursors the calling account's still-org-visible
// clips by the monotonic recording_clips.id (BIGSERIAL), so a NAS pull client can
// drain every clip exactly once and resume from its last seen id. object_key is
// never selected: the caller gets a download_path to the existing presign endpoint
// instead. Released clips (already pulled/detached) and purged clips are both
// excluded, so the working set stays small: the NAS releases each clip right after
// it pulls it, and an ordered id>cursor scan over the active partial index is cheap.
// ONLY nas_pull recordings' clips are ever handed out (r.delivery='nas_pull'), so a
// managed recording's clips can never be released by the NAS client. A commit
// watermark (accountClipsCommitWatermark) additionally hides very fresh clips until
// no older-id ingest tx can still be uncommitted.
//
// The commit-watermark interval: clip ids are BIGSERIAL, allocated at INSERT but
// only VISIBLE at COMMIT, so a raw id>cursor scan can skip a lower id whose ingest
// tx committed AFTER a higher id it already handed out (the client would advance its
// cursor past the lower id and never see it). Only offering clips whose created_at is
// at least this far in the past guarantees no older-id ingest tx can still be
// uncommitted. Clips are ~60s and the poll cadence is ~60s, so this latency is
// negligible.
const accountClipsCommitWatermark = `interval '90 seconds'`

const accountClipsCursorSQL = `
	SELECT c.id, c.recording_id, c.size_bytes, c.sha256, c.clip_start_at, c.clip_end_at, c.display_path,
	       c.recording_job_id, c.capture_lease_token, c.capture_sequence,
	       c.capture_attempt_id,c.timestamp_contract_version,c.timestamp_contract,c.timestamp_contract_status,c.timestamp_contract_reason
	FROM recording_clips c
	JOIN recordings r ON r.id = c.recording_id
	WHERE r.account_id = $1 AND c.purged_at IS NULL AND c.released_at IS NULL
	  AND r.delivery = 'nas_pull'
	  AND c.created_at < now() - ` + accountClipsCommitWatermark + `
	  AND c.id > $2
	ORDER BY c.id ASC
	LIMIT $3
`

// handleAccountClips returns one forward-cursored page of the calling account's
// unpurged clips, ordered by the monotonic clip id, for the NAS pull client.
// It is mounted under requireAccountAuth so a Bearer sir_ account API key can
// drain it. Each row carries a download_path to the existing per-recording clip
// download endpoint; object_key is never exposed. The response order is delivery
// order, not stitch order: recording_job_id, capture_generation, and
// capture_sequence give clients the canonical lossless ordering within a capture
// generation. next_after_id is the max clip id in the page (the client's next
// cursor), or null when the page is empty.
func (s *Server) handleAccountClips(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	afterID := int64(parseIntQuery(r, "after_id", 0, 0, 1<<62))
	limit := parseIntQuery(r, "limit", 100, 1, 500)

	rows, err := s.pool.Query(r.Context(), accountClipsCursorSQL, principal.AccountID, afterID, limit)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("list account clips: %v", err))
		return
	}
	defer rows.Close()

	clips := make([]map[string]any, 0, limit)
	var nextAfterID *int64
	for rows.Next() {
		var (
			clipID                           int64
			recordingID                      int64
			sizeBytes                        int64
			sha256                           string
			clipStartAt                      time.Time
			clipEndAt                        *time.Time
			displayPath                      string
			recordingJobID                   *int64
			captureLeaseToken                *uuid.UUID
			captureSequence                  *int64
			captureAttemptID                 *uuid.UUID
			timestampVersion                 *string
			timestampContract                []byte
			timestampStatus, timestampReason *string
		)
		if err := rows.Scan(&clipID, &recordingID, &sizeBytes, &sha256, &clipStartAt, &clipEndAt, &displayPath,
			&recordingJobID, &captureLeaseToken, &captureSequence, &captureAttemptID, &timestampVersion, &timestampContract, &timestampStatus, &timestampReason); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan account clip: %v", err))
			return
		}
		var endAt any
		if clipEndAt != nil {
			endAt = clipEndAt.UTC()
		}
		clips = append(clips, map[string]any{
			"clip_id":       clipID,
			"recording_id":  recordingID,
			"size_bytes":    sizeBytes,
			"sha256":        sha256,
			"clip_start_at": clipStartAt.UTC(),
			"clip_end_at":   endAt,
			"relative_path": displayPath,
			"download_path": fmt.Sprintf("/api/v1/account/recordings/%d/clips/%d/download", recordingID, clipID),
			// The feed remains ID-cursor ordered for exactly-once delivery. Concurrent
			// uploads may commit out of media order, so stitchers must order within a
			// capture generation by capture_sequence instead of response position.
			"recording_job_id":           recordingJobID,
			"capture_generation":         captureGenerationFingerprint(captureLeaseToken),
			"capture_sequence":           captureSequence,
			"capture_attempt_id":         captureAttemptID,
			"timestamp_contract_version": timestampVersion,
			"timestamp_contract":         json.RawMessage(timestampContract),
			"timestamp_contract_status":  timestampStatus,
			"timestamp_contract_reason":  timestampReason,
		})
		id := clipID
		nextAfterID = &id
	}
	if err := rows.Err(); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("iterate account clips: %v", err))
		return
	}

	util.WriteJSON(w, http.StatusOK, map[string]any{
		"clips":         clips,
		"next_after_id": nextAfterID,
	})
}
