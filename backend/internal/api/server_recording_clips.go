package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/daydemir/stoarama/backend/internal/capture"
	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/daydemir/stoarama/backend/internal/recordingapi"
	"github.com/daydemir/stoarama/backend/internal/recordingnaming"
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
	    AND j.scheduled_for <= now()
	    AND ((j.kind = 'continuous_window' AND j.window_end_at > now())
	         OR (j.kind <> 'continuous_window'
	             AND j.fire_at + make_interval(secs => (j.clip_duration_sec + $4)) > now()))
	    AND rec.status = 'active'
	    AND rec.start_at <= now()
	    AND (rec.end_at IS NULL OR now() < rec.end_at)
	    AND rec.capture_via = 'relay'
	    -- YouTube resolution is egress-sensitive. Only nodes whose fresh heartbeat
	    -- positively proves readiness may lease an authoritatively classified
	    -- YouTube stream. Other source families retain the existing availability
	    -- behavior; a missing/malformed readiness value fails closed for YouTube.
	    AND (NOT EXISTS (
	           SELECT 1 FROM streams source_stream
	           WHERE source_stream.id=rec.stream_id
	             AND source_stream.execution_class='youtube_direct')
	         OR (jsonb_typeof(n.capabilities_jsonb->'youtube_ready') = 'boolean'
	             AND (n.capabilities_jsonb->>'youtube_ready')::boolean))
	    AND (j.handoff_owner IS NULL
	         OR j.handoff_owner <> 'node:' || $1::text
	         OR j.handoff_until <= now())
	    AND ($2 OR EXISTS (
	          SELECT 1 FROM account_billing b
	          WHERE b.account_id = rec.account_id
	            AND b.has_payment_method))
	    -- The surrounding node/group row locks make these capacity bounds authoritative.
	    AND (SELECT COUNT(*) FROM recording_jobs aj
	         WHERE aj.status = 'leased'
	           AND aj.lease_owner = 'node:' || $1::text
	           AND aj.lease_expires_at > now()) < n.relay_max_streams
	    -- A recording may softly prefer one internet group. The preferred group gets
	    -- the same bounded 12-second first opportunity as ordinary fairness, but an
	    -- unavailable/full/non-polling preferred group can never strand capture.
	    AND (rec.preferred_relay_group_id IS NULL
	         OR n.relay_group_id=rec.preferred_relay_group_id
	         OR j.relay_fairness_started_at<=now()-interval '12 seconds'
	         OR NOT EXISTS (
	              SELECT 1
	              FROM relay_groups preferred_group
	              WHERE preferred_group.id=rec.preferred_relay_group_id
	                AND preferred_group.account_id=rec.account_id
	                AND (SELECT COUNT(*)
	                     FROM recording_jobs preferred_jobs
	                     JOIN nodes preferred_nodes
	                       ON preferred_jobs.lease_owner='node:'||preferred_nodes.id::text
	                     WHERE preferred_nodes.account_id=rec.account_id
	                       AND preferred_nodes.relay_group_id=preferred_group.id
	                       AND preferred_jobs.status='leased'
	                       AND preferred_jobs.lease_expires_at>now()) < preferred_group.max_streams
	                AND EXISTS (
	                     SELECT 1 FROM nodes preferred_node
	                     WHERE preferred_node.account_id=rec.account_id
	                       AND preferred_node.relay_group_id=preferred_group.id
	                       AND preferred_node.node_type='relay'
	                       AND preferred_node.status='active'
	                       AND preferred_node.last_heartbeat_at>=now()-interval '120 seconds'
	                       AND (SELECT COUNT(*) FROM recording_jobs preferred_node_jobs
	                            WHERE preferred_node_jobs.status='leased'
	                              AND preferred_node_jobs.lease_owner='node:'||preferred_node.id::text
	                              AND preferred_node_jobs.lease_expires_at>now()) < preferred_node.relay_max_streams)))
	    -- Prefer the lowest projected native-bandwidth utilization across healthy
	    -- internet groups before balancing machines inside one. Successful clip
	    -- ingests learn each recording's source-copy bitrate; unknown streams reserve
	    -- a conservative 4 Mbps. A configured group bandwidth budget lets a stronger
	    -- uplink intentionally carry more native media without changing its quality.
	    -- A group participates only while it has an online node with spare node and
	    -- group capacity. The fallback is measured from this job's first
	    -- actual lease opportunity, not its scheduled time, so an old recovery batch
	    -- still balances while a heartbeat-only peer can delay one job by at most 12s.
	    -- Twelve seconds covers two polls from legacy 5s relay builds, so an older
	    -- healthy node on an independent uplink is not starved by newer 1s pollers.
	    AND (j.relay_fairness_started_at <= now()-interval '12 seconds'
	         OR n.relay_group_id IS NULL
	         OR n.relay_group_id=rec.preferred_relay_group_id
	         OR NOT EXISTS (
	         SELECT 1
	         FROM relay_groups peer_group
	         CROSS JOIN LATERAL (
	              SELECT COUNT(*) AS lease_count,
	                     COALESCE(SUM(GREATEST(COALESCE(peer_group_bandwidth.observed_bandwidth_bps, 0), 4000000)), 0) AS bandwidth_load
	              FROM recording_jobs peer_group_jobs
	              JOIN nodes peer_group_nodes
	                ON peer_group_jobs.lease_owner='node:'||peer_group_nodes.id::text
	              JOIN recordings peer_group_recordings ON peer_group_recordings.id=peer_group_jobs.recording_id
	              LEFT JOIN recording_bandwidth_observations peer_group_bandwidth ON peer_group_bandwidth.recording_id=peer_group_recordings.id
	              WHERE peer_group_nodes.account_id=n.account_id
	                AND peer_group_nodes.relay_group_id=peer_group.id
	                AND peer_group_jobs.status='leased'
	                AND peer_group_jobs.lease_expires_at>now()
	         ) peer_group_load
	         WHERE peer_group.account_id=n.account_id
	           AND peer_group.id<>n.relay_group_id
	           AND peer_group_load.lease_count < peer_group.max_streams
	           AND EXISTS (
	                SELECT 1 FROM nodes peer_group_node
	                WHERE peer_group_node.account_id=n.account_id
	                  AND peer_group_node.relay_group_id=peer_group.id
	                  AND peer_group_node.node_type='relay'
	                  AND peer_group_node.status='active'
	                  AND peer_group_node.last_heartbeat_at>=now()-interval '120 seconds'
	                  AND (SELECT COUNT(*) FROM recording_jobs peer_node_jobs
	                       WHERE peer_node_jobs.status='leased'
	                         AND peer_node_jobs.lease_owner='node:'||peer_group_node.id::text
	                         AND peer_node_jobs.lease_expires_at>now()) < peer_group_node.relay_max_streams)
	           AND (peer_group_load.bandwidth_load + GREATEST(COALESCE((SELECT observed_bandwidth_bps FROM recording_bandwidth_observations WHERE recording_id=rec.id), 0), 4000000))::numeric
	                 / COALESCE(peer_group.bandwidth_capacity_bps, peer_group.max_streams::bigint * 4000000)
	               <
	               ((SELECT COALESCE(SUM(GREATEST(COALESCE(current_group_bandwidth.observed_bandwidth_bps, 0), 4000000)), 0)
	                FROM recording_jobs current_group_jobs
	                JOIN nodes current_group_nodes
	                  ON current_group_jobs.lease_owner='node:'||current_group_nodes.id::text
	                JOIN recordings current_group_recordings ON current_group_recordings.id=current_group_jobs.recording_id
	                LEFT JOIN recording_bandwidth_observations current_group_bandwidth ON current_group_bandwidth.recording_id=current_group_recordings.id
	                WHERE current_group_nodes.account_id=n.account_id
	                  AND current_group_nodes.relay_group_id=n.relay_group_id
	                  AND current_group_jobs.status='leased'
	                  AND current_group_jobs.lease_expires_at>now()) + GREATEST(COALESCE((SELECT observed_bandwidth_bps FROM recording_bandwidth_observations WHERE recording_id=rec.id), 0), 4000000))::numeric
	                 / COALESCE((SELECT current_group.bandwidth_capacity_bps
	                             FROM relay_groups current_group
	                             WHERE current_group.id=n.relay_group_id AND current_group.account_id=n.account_id),
	                            (SELECT current_group.max_streams::bigint * 4000000
	                             FROM relay_groups current_group
	                             WHERE current_group.id=n.relay_group_id AND current_group.account_id=n.account_id))))
	    -- Within a group, only a least-loaded healthy node may take the next job.
	    -- The surrounding group row lock makes this comparison authoritative, so
	    -- simultaneous pollers converge on an even distribution instead of the
	    -- fastest poller monopolizing long continuous-window leases.
	    AND (j.relay_fairness_started_at <= now()-interval '12 seconds' OR n.relay_group_id IS NULL OR NOT EXISTS (
	         SELECT 1 FROM nodes peer
	         WHERE peer.account_id=n.account_id
	           AND peer.relay_group_id=n.relay_group_id
	           AND peer.node_type='relay'
	           AND peer.status='active'
	           AND peer.last_heartbeat_at >= now()-interval '120 seconds'
	           AND (SELECT COUNT(*) FROM recording_jobs pj
	                WHERE pj.status='leased'
	                  AND pj.lease_owner='node:'||peer.id::text
	                  AND pj.lease_expires_at>now()) < peer.relay_max_streams
	           AND (SELECT COUNT(*) FROM recording_jobs pj
	                WHERE pj.status='leased'
	                  AND pj.lease_owner='node:'||peer.id::text
	                  AND pj.lease_expires_at>now()) <
	               (SELECT COUNT(*) FROM recording_jobs nj
	                WHERE nj.status='leased'
	                  AND nj.lease_owner='node:'||n.id::text
	                  AND nj.lease_expires_at>now())))
	    AND (n.relay_group_id IS NULL OR (
	         SELECT COUNT(*)
	         FROM recording_jobs gj
	         JOIN nodes gn ON gj.lease_owner='node:'||gn.id::text
	         WHERE gn.account_id=n.account_id
	           AND gn.relay_group_id=n.relay_group_id
	           AND gj.status='leased'
	           AND gj.lease_expires_at > now()) < (
	         SELECT g.max_streams
	         FROM relay_groups g
	         WHERE g.id=n.relay_group_id AND g.account_id=n.account_id))
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
	var accountID int64
	if err := tx.QueryRow(ctx, `SELECT id FROM accounts WHERE id=$1 FOR UPDATE`, principal.AccountID).Scan(&accountID); err != nil {
		return resp, err
	}
	if err := lockRelayNodeAndGroup(ctx, tx, principal); err != nil {
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
		principal.NodeID, billingDisabled, margin, recordingFreshnessGraceSec, tokenSupported).Scan(
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

const cloudRecorderLockSQL = `
	SELECT capacity, pool_role
	FROM recorder_droplets
	WHERE name = $1 AND node_id = $2 AND state IN ('provisioning', 'active')
	FOR UPDATE
`

// Managed cloud recorders are operator-owned shared infrastructure and
// intentionally lease jobs across customer accounts. Storage credentials stay
// server-side and are derived from each recording's account during upload.
const cloudRecordingJobsLeaseSQL = `
	WITH cte AS (
	  SELECT j.id
	  FROM recording_jobs j
	  JOIN recordings rec ON rec.id = j.recording_id
	  WHERE (
	          SELECT count(*)
	          FROM recording_jobs live
	          WHERE live.status = 'leased'
	            AND live.lease_owner = $1
	            AND live.lease_expires_at > now()
	        ) < $5
	    AND j.status = 'pending' AND j.scheduled_for <= now()
	    AND ((j.kind = 'continuous_window' AND j.window_end_at > now())
	         OR (j.kind <> 'continuous_window'
	             AND j.fire_at + make_interval(secs => (j.clip_duration_sec + $4)) > now()))
	    AND rec.status = 'active'
	    AND rec.start_at <= now()
	    AND (rec.end_at IS NULL OR now() < rec.end_at)
	    AND rec.capture_via = 'cloud'
	    AND (
	          ($7 = 'shared' AND NOT EXISTS (
	            SELECT 1 FROM recording_dedicated_canary_reservations dc
	            WHERE dc.recording_id=j.recording_id AND dc.state='active'
	          ))
	          OR ($7 = 'dedicated_canary' AND EXISTS (
	            SELECT 1 FROM recording_dedicated_canary_reservations dc
	            WHERE dc.recording_id=j.recording_id AND dc.worker_name=$1
	              AND dc.state='active' AND dc.expires_at > now()
	          ))
	    )
	    AND (j.handoff_owner IS NULL
	         OR j.handoff_owner <> $1
	         OR j.handoff_until <= now())
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
		// $1=workerID, $2=billingDisabled, $3=margin, $4=freshnessGrace, $5=capacity.
		tx, beginErr := s.pool.Begin(r.Context())
		if beginErr != nil {
			err = fmt.Errorf("begin cloud lease: %w", beginErr)
		} else {
			defer func() { _ = tx.Rollback(r.Context()) }()
			var capacity int
			var poolRole string
			err = tx.QueryRow(r.Context(), cloudRecorderLockSQL, workerID, principal.NodeID).Scan(&capacity, &poolRole)
			if err == nil {
				err = tx.QueryRow(r.Context(), cloudRecordingJobsLeaseSQL,
					workerID, billingDisabled, margin, recordingFreshnessGraceSec, capacity, tokenSupported, poolRole).Scan(
					&resp.JobID, &resp.RecordingID, &resp.SourceURL, &resp.StreamID, &resp.StreamProvider, &resp.SourcePageURL, &resp.ClipDurationSec,
					&resp.StorageDestinationID, &resp.FireAt, &resp.AttemptCount, &resp.LeaseExpiresAt,
					&resp.TargetFPS, &resp.Kind, &resp.WindowEndAt, &resp.LeaseToken,
				)
			}
			if err == nil {
				err = tx.Commit(r.Context())
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

type recordingUploadIntentStatus string

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
	leaseFence := ""
	if principal.NodeType == nodeTypeLocalRecorder {
		leaseFence = dedicatedCanaryLeaseFenceSQL
	}
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
		  AND j.lease_token IS NOT DISTINCT FROM $3 AND j.lease_expires_at > now()
		  AND rec.status='active'
	`+leaseFence, req.JobID, workerID, leaseToken).Scan(
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
		"intent_id":      intentID.String(),
		"upload_url":     uploadURL,
		"object_key":     objectKey,
		"bucket":         bucket,
		"endpoint":       endpoint,
		"content_type":   mimeType,
		"max_size_bytes": maxSize,
		"expires_at":     expiresAt,
	})
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
	leaseFence := ""
	if principal.NodeType == nodeTypeLocalRecorder {
		leaseFence = dedicatedCanaryLeaseFenceSQL
	}
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
		       j.lease_token,
		       sd.access_key_id, sd.secret_access_key_enc
		FROM recording_upload_intents ui
		JOIN recording_jobs j ON j.id = ui.recording_job_id
		JOIN recordings rec ON rec.id = ui.recording_id
		JOIN storage_destinations sd ON sd.id = ui.storage_destination_id
		WHERE ui.id=$1 AND ui.status='pending'
		  AND j.status='leased' AND j.lease_owner=$2
		  AND j.lease_token IS NOT DISTINCT FROM $3 AND j.lease_expires_at > now()
		-- Serialize ingest with generation-fenced surrender. If ingest wins, the
		-- clip commits before surrender computes had_clips; if surrender wins, this
		-- rechecks the lease after waiting and rejects the old generation.
	`+leaseFence+`
		FOR UPDATE OF ui, j
	`, intentID, workerID, leaseToken).Scan(
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
	if head.SizeBytes <= 0 || (req.SizeBytes > 0 && head.SizeBytes != req.SizeBytes) {
		// Treat a stale, truncated, or empty remote object as replayable. A 5xx keeps
		// old workers retrying during a backend-first deploy; the stable code lets
		// current workers identify the exact reason and reserve a fresh upload URL.
		util.WriteJSON(w, http.StatusServiceUnavailable, map[string]any{
			"code":  recordingapi.ErrorCodeUploadedObjectIntegrity,
			"error": "uploaded object size does not match the local clip",
		})
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
			 capture_lease_token, capture_sequence, capture_attempt_id, timestamp_contract_version, timestamp_contract, timestamp_contract_status, timestamp_contract_reason)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30)
		ON CONFLICT (bucket, object_key) DO NOTHING
		RETURNING id
	`, recordingID, jobID, destID, endpoint, bucket, objectKey,
		displayPath, mimeType, container, head.SizeBytes, etag, strings.TrimSpace(req.SHA256), durationMs,
		strings.TrimSpace(req.VideoCodec), strings.TrimSpace(req.AudioCodec), req.AudioPresent, req.ActualFPS,
		nullablePositiveInt(req.VideoWidth), nullablePositiveInt(req.VideoHeight), strings.TrimSpace(req.ResolvedURL), fireAt, clipStartAt, clipEndAt, captureLeaseToken, captureSequence, captureAttemptID, nullableTimestampVersion(captureAttemptID, req.TimestampContractStatus), req.TimestampContract, nullableTimestampStatus(captureAttemptID, req.TimestampContractStatus), nullableTimestampReason(captureAttemptID, req.TimestampContractReason)).Scan(&clipID)
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
		(pgErr.ConstraintName == "uq_recording_clips_capture_sequence" ||
			pgErr.ConstraintName == "uq_recording_clips_capture_sha256") {
		// A parallel retry can pass the preflight before the first ingest commits.
		// The generation-scoped unique indexes are the concurrency backstop; expose
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
			VALUES ($1, ($3::bigint * 8000 / $2)::bigint, now())
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

// dedicatedCanaryLeaseFenceSQL is appended to every cloud-worker mutation. A
// reservation expiry therefore cancels the exact lease instead of allowing a
// stale canary process to upload after its owner fence is gone. Shared workers
// satisfy the first branch; dedicated workers must still hold their exact,
// unexpired reservation. Every host query must alias recording_jobs as j and
// concatenate this fragment inside its WHERE clause before GROUP BY, FOR UPDATE,
// or RETURNING clauses.
const dedicatedCanaryLeaseFenceSQL = `
	AND (
		NOT EXISTS (
			SELECT 1 FROM recorder_droplets d
			WHERE d.name=j.lease_owner AND d.pool_role='dedicated_canary'
		)
		OR EXISTS (
			SELECT 1 FROM recording_dedicated_canary_reservations dc
			WHERE dc.recording_id=j.recording_id AND dc.worker_name=j.lease_owner
			  AND dc.state='active' AND dc.expires_at > now()
		)
	)
`

const recordingJobHeartbeatSQLBase = `
	UPDATE recording_jobs j
	SET lease_expires_at = LEAST(
	      now() + make_interval(secs => (j.clip_duration_sec + $3)),
	      CASE WHEN j.kind='continuous_window'
	           THEN j.window_end_at + make_interval(secs => $5)
	           ELSE 'infinity'::timestamptz END
	    ), updated_at = now()
	WHERE j.id=$1 AND j.status='leased' AND j.lease_owner=$2
	  AND j.lease_token IS NOT DISTINCT FROM $4 AND j.lease_expires_at > now()
	  AND (j.kind<>'continuous_window'
	       OR (j.window_end_at IS NOT NULL
	           AND j.window_end_at + make_interval(secs => $5) > now()))

`

const recordingJobHeartbeatSQL = recordingJobHeartbeatSQLBase + `
	RETURNING j.lease_expires_at
`

const recordingJobHeartbeatCloudSQL = recordingJobHeartbeatSQLBase + dedicatedCanaryLeaseFenceSQL + `
	RETURNING j.lease_expires_at
`

func (s *Server) heartbeatRecordingJob(ctx context.Context, principal nodePrincipal, jobID int64, workerID string, leaseToken *uuid.UUID) (time.Time, error) {
	var leaseExpiresAt time.Time
	if principal.NodeType != nodeTypeRelay {
		err := s.pool.QueryRow(ctx, recordingJobHeartbeatCloudSQL,
			jobID, workerID, recordingCaptureTimeoutMarginSec+recordingUploadMarginSec, leaseToken,
			recordingContinuousPostWindowLeaseSec).Scan(&leaseExpiresAt)
		return leaseExpiresAt, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return leaseExpiresAt, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := lockRelayNodeAndGroup(ctx, tx, principal); err != nil {
		return leaseExpiresAt, err
	}
	if err := tx.QueryRow(ctx, recordingJobHeartbeatSQL,
		jobID, workerID, recordingCaptureTimeoutMarginSec+recordingUploadMarginSec, leaseToken,
		recordingContinuousPostWindowLeaseSec).Scan(&leaseExpiresAt); err != nil {
		return leaseExpiresAt, err
	}
	if err := tx.Commit(ctx); err != nil {
		return leaseExpiresAt, err
	}
	return leaseExpiresAt, nil
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
	leaseExpiresAt, err := s.heartbeatRecordingJob(r.Context(), principal, id, workerID, leaseToken)
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
	util.WriteJSON(w, http.StatusOK, map[string]any{"cancel": false, "lease_expires_at": leaseExpiresAt})
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
	if err := s.touchDropletLiveness(r.Context(), workerID, principal.NodeID, req.BuildSHA); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("touch droplet liveness: %v", err))
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
	leaseFence := ""
	if principal.NodeType == nodeTypeLocalRecorder {
		leaseFence = dedicatedCanaryLeaseFenceSQL
	}

	var (
		recordingID int64
		kind        string
		clipCount   int64
		jobError    string
	)
	err = s.pool.QueryRow(r.Context(), `
		SELECT j.recording_id, j.kind, COUNT(c.id), COALESCE(j.error_text, '')
		FROM recording_jobs j
		LEFT JOIN recording_clips c ON c.recording_job_id=j.id
		WHERE j.id=$1 AND j.status='leased' AND j.lease_owner=$2
		  AND j.lease_token IS NOT DISTINCT FROM $3
	`+leaseFence+`
		GROUP BY j.id
	`, id, workerID, leaseToken).Scan(&recordingID, &kind, &clipCount, &jobError)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusConflict, "job is not leased by this worker")
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load recording job: %v", err))
		return
	}
	if kind == "continuous_window" && clipCount == 0 {
		errText := sanitizeRecordingSurrenderError(jobError, "continuous recording produced no clips")
		tx, err := s.pool.Begin(r.Context())
		if err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("begin complete tx: %v", err))
			return
		}
		defer func() { _ = tx.Rollback(r.Context()) }()
		if _, err := tx.Exec(r.Context(), `
			UPDATE recording_jobs j
			SET status='error', completed_at=now(), lease_expires_at=NULL, error_text=$3, updated_at=now()
			WHERE j.id=$1 AND j.status='leased' AND j.lease_owner=$2
			  AND j.lease_token IS NOT DISTINCT FROM $4
		`+leaseFence, id, workerID, errText, leaseToken); err != nil {
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
	ct, err := s.pool.Exec(r.Context(), `
		UPDATE recording_jobs j
		SET status='done', completed_at=now(), lease_expires_at=NULL, updated_at=now()
		WHERE j.id=$1 AND j.status='leased' AND j.lease_owner=$2
		  AND j.lease_token IS NOT DISTINCT FROM $3
	`+leaseFence, id, workerID, leaseToken)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("complete recording job: %v", err))
		return
	}
	if ct.RowsAffected() == 0 {
		util.WriteError(w, http.StatusConflict, "job is not leased by this worker")
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
	Reason    recordingJobSurrenderReason `json:"reason"`
	ErrorText string                      `json:"error_text"`
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
	` + dedicatedCanaryLeaseFenceSQL + `
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
	if principal.NodeType == nodeTypeLocalRecorder && req.Reason != recordingJobSurrenderNoProgress {
		util.WriteError(w, http.StatusBadRequest, "cloud recorders can only surrender for no progress")
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

const recordingJobFailSQLBase = `
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

`

const recordingJobFailSQL = recordingJobFailSQLBase + `
	RETURNING j.recording_id
`

const recordingJobFailCloudSQL = recordingJobFailSQLBase + dedicatedCanaryLeaseFenceSQL + `
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

	var recordingID int64
	failSQL := recordingJobFailSQL
	if principal.NodeType == nodeTypeLocalRecorder {
		failSQL = recordingJobFailCloudSQL
	}
	err = tx.QueryRow(r.Context(), failSQL, id, workerID, errText, leaseToken).Scan(&recordingID)
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

// accountClipsCursorSQL forward-cursors the calling account's still-org-visible,
// nonempty clips by the monotonic recording_clips.id (BIGSERIAL), so a NAS pull
// client can drain every deliverable clip exactly once and resume from its last seen id. object_key is
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
	  -- Retain zero-byte rows for audit, but never let one poison the forward
	  -- cursor page and block every later valid clip.
	  AND c.size_bytes > 0
	  AND c.created_at < now() - ` + accountClipsCommitWatermark + `
	  AND c.id > $2
	ORDER BY c.id ASC
	LIMIT $3
`

// handleAccountClips returns one forward-cursored page of the calling account's
// unpurged, nonempty clips, ordered by the monotonic clip id, for the NAS pull client.
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
