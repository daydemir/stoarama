package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/daydemir/stoarama/backend/internal/util"
)

var recordingCanaryDigestRE = regexp.MustCompile(`^[0-9a-f]{64}$`)
var recordingCanaryVersionRE = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)
var recordingCanaryCodecRE = regexp.MustCompile(`^[a-z0-9._-]{1,32}$`)

const (
	recordingCanaryReservationTTL = 3 * time.Minute
	recordingCanaryJobGuard       = 5 * time.Minute
)

type recordingCanarySpec struct {
	ReservationID string    `json:"reservation_id"`
	RecordingID   int64     `json:"recording_id"`
	NodeID        int64     `json:"node_id"`
	StreamID      int64     `json:"stream_id"`
	Provider      string    `json:"provider"`
	SourceURL     string    `json:"source_url"`
	SourcePageURL string    `json:"source_page_url,omitempty"`
	SafeUntil     time.Time `json:"safe_until"`
}

// handleNodeRecordingCanaryStart reserves a short interval that the production
// relay lease query honors. It requires the entire relay node/uplink group to be
// idle and refuses a target whose next production job is within five minutes.
// The reservation expires automatically if the CLI crashes.
func (s *Server) handleNodeRecordingCanaryStart(w http.ResponseWriter, r *http.Request) {
	principal, recordingID, ok := recordingCanaryPrincipal(w, r)
	if !ok {
		return
	}
	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "begin recording canary reservation")
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()
	var accountID int64
	if err := tx.QueryRow(r.Context(), `SELECT id FROM accounts WHERE id=$1 FOR UPDATE`, principal.AccountID).Scan(&accountID); err != nil {
		util.WriteError(w, http.StatusForbidden, "relay account unavailable")
		return
	}
	if err := lockRelayNodeAndGroup(r.Context(), tx, principal); err != nil {
		util.WriteError(w, http.StatusForbidden, "relay node unavailable")
		return
	}

	var spec recordingCanarySpec
	var preferredGroupID, nodeGroupID *int64
	var windowStart time.Time
	var preopenStage string
	err = tx.QueryRow(r.Context(), `
		SELECT rec.id, n.id, COALESCE(rec.stream_id, 0), COALESCE(st.provider, ''),
		       rec.stream_url, COALESCE(st.source_page_url, ''),
		       rec.preferred_relay_group_id, n.relay_group_id, rec.next_fire_at,
		       CASE WHEN rec.next_fire_at>now()+interval '30 minutes' THEN 'early' ELSE 'confirm' END
		FROM recordings rec
		JOIN nodes n ON n.id=$1
		LEFT JOIN streams st ON st.id=rec.stream_id
		WHERE rec.id=$2
		  AND rec.account_id=n.account_id
		  AND rec.status='active'
		  AND rec.capture_via='relay'
		  AND n.node_type='relay'
		  AND n.status='active'
		  AND n.last_heartbeat_at>=now()-interval '120 seconds'
		  AND n.capabilities_jsonb @> '{"ffmpeg_runtime":{"qualified":true,"network_probe":"host_reached"}}'::jsonb
		  AND CASE
		        WHEN jsonb_typeof(n.capabilities_jsonb#>'{ffmpeg_runtime,observed_at}')='string'
		         AND (n.capabilities_jsonb#>>'{ffmpeg_runtime,observed_at}') ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]+)?Z$'
		        THEN (n.capabilities_jsonb#>>'{ffmpeg_runtime,observed_at}')::timestamptz
		             BETWEEN now()-interval '120 seconds' AND now()+interval '30 seconds'
		        ELSE false
		      END
		  AND rec.mode='continuous' AND rec.next_fire_at>now()
		  AND rec.next_fire_at<=now()+interval '2 hours'
		FOR UPDATE OF rec
	`, principal.NodeID, recordingID).Scan(
		&spec.RecordingID, &spec.NodeID, &spec.StreamID, &spec.Provider,
		&spec.SourceURL, &spec.SourcePageURL, &preferredGroupID, &nodeGroupID, &windowStart, &preopenStage,
	)
	if err == pgx.ErrNoRows {
		util.WriteError(w, http.StatusNotFound, "active relay recording not available to this node")
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "load recording canary source")
		return
	}
	if preferredGroupID != nil && (nodeGroupID == nil || *nodeGroupID != *preferredGroupID) {
		util.WriteError(w, http.StatusForbidden, "recording is assigned to a different relay group")
		return
	}

	var groupBusy, targetBusy bool
	if err := tx.QueryRow(r.Context(), `
		SELECT EXISTS(
		  SELECT 1 FROM recording_jobs j
		  JOIN nodes owner ON j.lease_owner='node:'||owner.id::text
		  WHERE j.status='leased' AND j.lease_expires_at>now()
		    AND owner.account_id=$1
		    AND (($2::bigint IS NULL AND owner.id=$3)
		         OR ($2::bigint IS NOT NULL AND owner.relay_group_id=$2))
		) OR EXISTS(
		  SELECT 1 FROM recording_jobs j
		  JOIN recordings due_rec ON due_rec.id=j.recording_id
		  WHERE j.status='pending'
		    AND j.scheduled_for<=now()+make_interval(secs=>$4)
		    AND due_rec.account_id=$1 AND due_rec.status='active' AND due_rec.capture_via='relay'
		    AND (due_rec.preferred_relay_group_id IS NULL
		         OR due_rec.preferred_relay_group_id IS NOT DISTINCT FROM $2::bigint)
		) OR EXISTS(
		  SELECT 1 FROM recording_canary_reservations active_canary
		  JOIN nodes canary_node ON canary_node.id=active_canary.node_id
		  WHERE active_canary.expires_at>now()
		    AND canary_node.account_id=$1
		    AND (($2::bigint IS NULL AND canary_node.id=$3)
		         OR ($2::bigint IS NOT NULL AND canary_node.relay_group_id=$2))
		) OR EXISTS(
		  SELECT 1 FROM nodes active_node
		  WHERE active_node.account_id=$1
		    AND active_node.node_type='relay'
		    AND active_node.status='active'
		    AND (($2::bigint IS NULL AND active_node.id=$3)
		         OR ($2::bigint IS NOT NULL AND active_node.relay_group_id=$2))
		    AND (active_node.last_heartbeat_at IS NULL
		         OR active_node.last_heartbeat_at<=now()-interval '120 seconds'
		         OR CASE
		              WHEN NOT (active_node.capabilities_jsonb ? 'active_jobs') THEN 0
		              WHEN COALESCE(active_node.capabilities_jsonb->>'active_jobs', '') ~ '^[0-9]+$'
		              THEN (active_node.capabilities_jsonb->>'active_jobs')::bigint
		              ELSE 1
		            END>0)
		)
	`, principal.AccountID, nodeGroupID, principal.NodeID, int(recordingCanaryJobGuard/time.Second)).Scan(&groupBusy); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "check relay canary capacity")
		return
	}
	if err := tx.QueryRow(r.Context(), `
		SELECT EXISTS(
		  SELECT 1 FROM recording_jobs
		  WHERE recording_id=$1
		    AND ((status='leased' AND lease_expires_at>now())
		         OR (status='pending' AND scheduled_for<=now()+make_interval(secs=>$2)))
		)
	`, recordingID, int(recordingCanaryJobGuard/time.Second)).Scan(&targetBusy); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "check recording canary safety")
		return
	}
	if groupBusy || targetBusy {
		util.WriteError(w, http.StatusConflict, "relay group or recording has active or imminent production work")
		return
	}

	if _, err := tx.Exec(r.Context(), `
		DELETE FROM recording_canary_reservations canary
		USING nodes canary_node
		WHERE canary_node.id=canary.node_id
		  AND canary_node.account_id=$1
		  AND canary.expires_at<=now()
	`, principal.AccountID); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "clean expired recording canary reservations")
		return
	}
	var reservationID uuid.UUID
	err = tx.QueryRow(r.Context(), `
		INSERT INTO recording_canary_reservations (recording_id, node_id, expires_at, window_start_at, preopen_stage)
		SELECT $1, $2, now()+make_interval(secs=>$3), $4, $5
		WHERE NOT EXISTS (
		  SELECT 1 FROM recording_canary_reservations
		  WHERE recording_id=$1 AND expires_at>now())
		RETURNING id, expires_at
	`, recordingID, principal.NodeID, int(recordingCanaryReservationTTL/time.Second), windowStart, preopenStage).Scan(&reservationID, &spec.SafeUntil)
	if err == pgx.ErrNoRows {
		util.WriteError(w, http.StatusConflict, "recording already has an active canary")
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "reserve recording canary")
		return
	}
	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "commit recording canary reservation")
		return
	}
	spec.ReservationID = reservationID.String()
	util.WriteJSON(w, http.StatusOK, spec)
}

func (s *Server) handleNodeRecordingCanaryCheck(w http.ResponseWriter, r *http.Request) {
	principal, recordingID, ok := recordingCanaryPrincipal(w, r)
	if !ok {
		return
	}
	reservationID, err := uuid.Parse(chi.URLParam(r, "reservationId"))
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, "invalid canary reservation id")
		return
	}
	var spec recordingCanarySpec
	err = s.pool.QueryRow(r.Context(), `
		SELECT canary.id::text, rec.id, canary.node_id, COALESCE(rec.stream_id, 0),
		       COALESCE(st.provider, ''), rec.stream_url, COALESCE(st.source_page_url, ''), canary.expires_at
		FROM recording_canary_reservations canary
		JOIN recordings rec ON rec.id=canary.recording_id
		LEFT JOIN streams st ON st.id=rec.stream_id
		WHERE canary.id=$1 AND canary.recording_id=$2 AND canary.node_id=$3 AND canary.expires_at>now()
	`, reservationID, recordingID, principal.NodeID).Scan(
		&spec.ReservationID, &spec.RecordingID, &spec.NodeID, &spec.StreamID,
		&spec.Provider, &spec.SourceURL, &spec.SourcePageURL, &spec.SafeUntil,
	)
	if err == pgx.ErrNoRows {
		util.WriteError(w, http.StatusConflict, "recording canary reservation expired or unavailable")
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "check recording canary reservation")
		return
	}
	util.WriteJSON(w, http.StatusOK, spec)
}

type recordingCanaryResult struct {
	DurationMS     int64  `json:"duration_ms"`
	SizeBytes      int64  `json:"size_bytes"`
	SHA256         string `json:"sha256"`
	VideoCodec     string `json:"video_codec"`
	ProbeOK        bool   `json:"probe_ok"`
	DecodeOK       bool   `json:"decode_ok"`
	NativeCopy     bool   `json:"native_copy"`
	Uploaded       bool   `json:"uploaded"`
	RelayVersion   string `json:"relay_version"`
	SourceRevision string `json:"source_revision"`
}

// handleNodeRecordingCanaryComplete records only the outcome of the exact live
// reservation owned by this relay. It does not touch jobs, recordings, frames,
// clips, uploads, or runtime state. The ordinary Finish call releases the
// reservation afterward, so an ambiguous response remains safely retryable.
func (s *Server) handleNodeRecordingCanaryComplete(w http.ResponseWriter, r *http.Request) {
	principal, recordingID, ok := recordingCanaryPrincipal(w, r)
	if !ok {
		return
	}
	reservationID, err := uuid.Parse(chi.URLParam(r, "reservationId"))
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, "invalid canary reservation id")
		return
	}
	var result recordingCanaryResult
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&result); err != nil {
		util.WriteError(w, http.StatusBadRequest, "invalid canary result")
		return
	}
	if dec.Decode(&struct{}{}) != io.EOF {
		util.WriteError(w, http.StatusBadRequest, "invalid canary result")
		return
	}
	result.SHA256 = strings.ToLower(strings.TrimSpace(result.SHA256))
	result.VideoCodec = strings.ToLower(strings.TrimSpace(result.VideoCodec))
	if result.DurationMS < 10_000 || result.DurationMS > 30_000 || result.SizeBytes <= 0 || result.SizeBytes > 512<<20 ||
		!recordingCanaryDigestRE.MatchString(result.SHA256) || !recordingCanaryCodecRE.MatchString(result.VideoCodec) ||
		!result.ProbeOK || !result.DecodeOK || !result.NativeCopy || result.Uploaded ||
		!recordingCanaryVersionRE.MatchString(result.RelayVersion) || !recordingCanaryVersionRE.MatchString(result.SourceRevision) {
		util.WriteError(w, http.StatusBadRequest, "canary result did not prove bounded native media")
		return
	}
	// Exact retry after an ambiguous response is safe even though Finish may
	// already have released the reservation. Any field mismatch is a conflict.
	var replay recordingCanaryResult
	var replayWindow time.Time
	var replayStage string
	var replayReflected bool
	err = s.pool.QueryRow(r.Context(), `
		SELECT window_start_at,stage,duration_ms,size_bytes,media_sha256,video_codec,
		       probe_ok,decode_ok,native_copy,uploaded,relay_version,source_revision,
		       EXISTS(SELECT 1 FROM recording_preopen_checks p
		              WHERE p.recording_id=e.recording_id AND p.window_start_at=e.window_start_at
		                AND p.stage=e.stage AND p.result='pass' AND p.method='relay_canary'
		                AND p.detail LIKE '%evidence='||e.reservation_id::text)
		FROM recording_canary_preopen_evidence e
		WHERE reservation_id=$1 AND recording_id=$2 AND node_id=$3 AND account_id=$4
	`, reservationID, recordingID, principal.NodeID, principal.AccountID).Scan(
		&replayWindow, &replayStage, &replay.DurationMS, &replay.SizeBytes, &replay.SHA256, &replay.VideoCodec,
		&replay.ProbeOK, &replay.DecodeOK, &replay.NativeCopy, &replay.Uploaded, &replay.RelayVersion, &replay.SourceRevision, &replayReflected)
	if err == nil {
		if replay != result {
			util.WriteError(w, http.StatusConflict, "canary completion evidence differs from prior result")
			return
		}
		util.WriteJSON(w, http.StatusOK, map[string]any{"recorded": true, "reflected": replayReflected, "stale": !replayReflected, "window_start_at": replayWindow, "stage": replayStage})
		return
	}
	if err != pgx.ErrNoRows {
		util.WriteError(w, http.StatusInternalServerError, "load prior recording canary completion")
		return
	}
	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "begin recording canary completion")
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()
	var windowStart time.Time
	var stage string
	var reservationCreatedAt time.Time
	err = tx.QueryRow(r.Context(), `
		SELECT canary.window_start_at,canary.preopen_stage,canary.created_at
		FROM recording_canary_reservations canary
		JOIN recordings rec ON rec.id=canary.recording_id
		WHERE canary.id=$1 AND canary.recording_id=$2 AND canary.node_id=$3
		  AND canary.expires_at>now() AND rec.account_id=$4
		  AND rec.status='active' AND rec.mode='continuous' AND rec.capture_via='relay'
		  AND canary.window_start_at IS NOT NULL AND canary.preopen_stage IS NOT NULL
		  AND rec.next_fire_at=canary.window_start_at AND rec.next_fire_at>now()
		FOR UPDATE OF canary,rec
	`, reservationID, recordingID, principal.NodeID, principal.AccountID).Scan(&windowStart, &stage, &reservationCreatedAt)
	if err == pgx.ErrNoRows {
		util.WriteError(w, http.StatusConflict, "recording canary reservation expired or pre-open window unavailable")
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "validate recording canary completion")
		return
	}
	insertedEvidence, err := tx.Exec(r.Context(), `
		INSERT INTO recording_canary_preopen_evidence(
		  reservation_id,recording_id,node_id,account_id,window_start_at,stage,duration_ms,size_bytes,
		  media_sha256,video_codec,probe_ok,decode_ok,native_copy,uploaded,relay_version,source_revision,reservation_created_at)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
		ON CONFLICT(reservation_id) DO NOTHING
	`, reservationID, recordingID, principal.NodeID, principal.AccountID, windowStart, stage,
		result.DurationMS, result.SizeBytes, result.SHA256, result.VideoCodec, result.ProbeOK,
		result.DecodeOK, result.NativeCopy, result.Uploaded, result.RelayVersion, result.SourceRevision, reservationCreatedAt)
	if err != nil {
		util.WriteError(w, http.StatusConflict, "persist immutable recording canary evidence")
		return
	}
	if insertedEvidence.RowsAffected() == 0 {
		var concurrent recordingCanaryResult
		var concurrentReservation uuid.UUID
		err := tx.QueryRow(r.Context(), `
			SELECT reservation_id,duration_ms,size_bytes,media_sha256,video_codec,
			       probe_ok,decode_ok,native_copy,uploaded,relay_version,source_revision
			FROM recording_canary_preopen_evidence
			WHERE reservation_id=$1 AND recording_id=$2 AND window_start_at=$3 AND stage=$4
			FOR SHARE
		`, reservationID, recordingID, windowStart, stage).Scan(
			&concurrentReservation, &concurrent.DurationMS, &concurrent.SizeBytes, &concurrent.SHA256,
			&concurrent.VideoCodec, &concurrent.ProbeOK, &concurrent.DecodeOK, &concurrent.NativeCopy,
			&concurrent.Uploaded, &concurrent.RelayVersion, &concurrent.SourceRevision)
		if err != nil || concurrentReservation != reservationID || concurrent != result {
			util.WriteError(w, http.StatusConflict, "canary completion evidence differs from prior result")
			return
		}
	}
	detail := fmt.Sprintf("relay node=%d native duration_ms=%d size_bytes=%d codec=%s probe_ok=true decode_ok=true uploaded=false evidence=%s",
		principal.NodeID, result.DurationMS, result.SizeBytes, result.VideoCodec, reservationID.String())
	if len(detail) > 500 {
		util.WriteError(w, http.StatusBadRequest, "canary result detail too large")
		return
	}
	projection, err := tx.Exec(r.Context(), `
		INSERT INTO recording_preopen_checks(recording_id,window_start_at,stage,result,method,detail,attempt_count,next_retry_at)
		VALUES($1,$2,$3,'pass','relay_canary',$4,1,NULL)
		ON CONFLICT(recording_id,window_start_at,stage) DO UPDATE SET
		  checked_at=now(),result='pass',method='relay_canary',detail=EXCLUDED.detail,
		  attempt_count=recording_preopen_checks.attempt_count,next_retry_at=NULL
		WHERE recording_preopen_checks.checked_at<= $5
	`, recordingID, windowStart, stage, detail, reservationCreatedAt)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "persist recording canary completion")
		return
	}
	projected := projection.RowsAffected() == 1
	if !projected {
		if err := tx.Commit(r.Context()); err != nil {
			util.WriteError(w, http.StatusInternalServerError, "commit stale recording canary evidence")
			return
		}
		util.WriteJSON(w, http.StatusOK, map[string]any{"recorded": true, "reflected": false, "stale": true, "window_start_at": windowStart, "stage": stage})
		return
	}
	if _, err := tx.Exec(r.Context(), `
		UPDATE recorder_health_alerts SET resolved_at=now()
		WHERE recording_id=$1 AND signal='preopen_quality_gate' AND resolved_at IS NULL
		  AND NOT EXISTS (
		    SELECT 1 FROM recording_preopen_checks
		    WHERE recording_id=$1 AND window_start_at=$2 AND result<>'pass')
	`, recordingID, windowStart); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "resolve recording pre-open alert")
		return
	}
	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "commit recording canary completion")
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"recorded": true, "reflected": true, "window_start_at": windowStart, "stage": stage})
}

func (s *Server) handleNodeRecordingCanaryFinish(w http.ResponseWriter, r *http.Request) {
	principal, recordingID, ok := recordingCanaryPrincipal(w, r)
	if !ok {
		return
	}
	reservationID, err := uuid.Parse(chi.URLParam(r, "reservationId"))
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, "invalid canary reservation id")
		return
	}
	if _, err := s.pool.Exec(r.Context(), `
		DELETE FROM recording_canary_reservations
		WHERE id=$1 AND recording_id=$2 AND node_id=$3
	`, reservationID, recordingID, principal.NodeID); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "finish recording canary reservation")
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"finished": true})
}

func recordingCanaryPrincipal(w http.ResponseWriter, r *http.Request) (nodePrincipal, int64, bool) {
	principal, ok := nodePrincipalFromContext(r.Context())
	if !ok || principal.NodeType != nodeTypeRelay {
		util.WriteError(w, http.StatusForbidden, "relay node required")
		return nodePrincipal{}, 0, false
	}
	recordingID, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil || recordingID <= 0 {
		util.WriteError(w, http.StatusBadRequest, "invalid recording id")
		return nodePrincipal{}, 0, false
	}
	return principal, recordingID, true
}
