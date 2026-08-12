package api

import (
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/daydemir/stoarama/backend/internal/util"
)

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
	err = tx.QueryRow(r.Context(), `
		SELECT rec.id, n.id, COALESCE(rec.stream_id, 0), COALESCE(st.provider, ''),
		       rec.stream_url, COALESCE(st.source_page_url, ''),
		       rec.preferred_relay_group_id, n.relay_group_id
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
		FOR UPDATE OF rec
	`, principal.NodeID, recordingID).Scan(
		&spec.RecordingID, &spec.NodeID, &spec.StreamID, &spec.Provider,
		&spec.SourceURL, &spec.SourcePageURL, &preferredGroupID, &nodeGroupID,
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
		    AND active_node.last_heartbeat_at>now()-interval '120 seconds'
		    AND (($2::bigint IS NULL AND active_node.id=$3)
		         OR ($2::bigint IS NOT NULL AND active_node.relay_group_id=$2))
		    AND CASE
		          WHEN NOT (active_node.capabilities_jsonb ? 'active_jobs') THEN 0
		          WHEN COALESCE(active_node.capabilities_jsonb->>'active_jobs', '') ~ '^[0-9]+$'
		          THEN (active_node.capabilities_jsonb->>'active_jobs')::bigint
		          ELSE 1
		        END>0
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
		INSERT INTO recording_canary_reservations (recording_id, node_id, expires_at)
		SELECT $1, $2, now()+make_interval(secs=>$3)
		WHERE NOT EXISTS (
		  SELECT 1 FROM recording_canary_reservations
		  WHERE recording_id=$1 AND expires_at>now())
		RETURNING id, expires_at
	`, recordingID, principal.NodeID, int(recordingCanaryReservationTTL/time.Second)).Scan(&reservationID, &spec.SafeUntil)
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
