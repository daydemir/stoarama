package api

import (
	"fmt"
	"net/http"
	"time"

	"github.com/daydemir/stoarama/backend/internal/joinedrecording"
	"github.com/daydemir/stoarama/backend/internal/util"
)

const joinedAdminBatchStatusStreamDays = 462

type joinedAdminBatchStatusStreamDay struct {
	RecordingID       int64  `json:"recording_id"`
	LocalDate         string `json:"local_date"`
	State             string `json:"state"`
	SourceCount       int    `json:"source_count"`
	SourceBytes       int64  `json:"source_bytes"`
	SealRequestSHA256 string `json:"seal_request_sha256"`
}

type joinedAdminBatchStatusResponse struct {
	ProtocolVersion         int                               `json:"protocol_version"`
	BatchID                 string                            `json:"batch_id"`
	State                   string                            `json:"state"`
	FrozenDenominatorSHA256 string                            `json:"frozen_denominator_sha256"`
	FreezeStartedAt         *time.Time                        `json:"freeze_started_at,omitempty"`
	FrozenAt                *time.Time                        `json:"frozen_at,omitempty"`
	ExpectedStreamDays      int                               `json:"expected_stream_days"`
	ExpectedScheduledHours  int                               `json:"expected_scheduled_hours"`
	StreamDays              []joinedAdminBatchStatusStreamDay `json:"stream_days"`
}

func (s *Server) handleAdminJoinedBatchStatus(w http.ResponseWriter, r *http.Request) {
	if !s.joinedControlPlaneReady() {
		util.WriteError(w, http.StatusServiceUnavailable, "joined recording is disabled")
		return
	}
	query := r.URL.Query()
	batchIDs, ok := query["batch_id"]
	if !ok || len(query) != 1 || len(batchIDs) != 1 || !joinedBatchIDPattern.MatchString(batchIDs[0]) {
		util.WriteError(w, http.StatusBadRequest, "one valid batch_id is required")
		return
	}

	rows, err := s.pool.Query(r.Context(), `SELECT b.batch_id,b.state,b.frozen_denominator_sha256,
		b.freeze_started_at,b.frozen_at,b.expected_stream_days,b.expected_scheduled_hours,
		br.priority_ordinal,d.date_ordinal,d.recording_id,d.local_date::text,d.state,
		d.source_clip_count,d.source_bytes,COALESCE(d.seal_request_sha256,'')
		FROM recording_joined_batches b
		JOIN recording_joined_batch_recordings br ON br.batch_record_id=b.id
		JOIN recording_joined_stream_days d ON d.batch_record_id=b.id AND d.batch_recording_id=br.id
		WHERE b.batch_id=$1 AND b.state<>'snapshotting'
		ORDER BY br.priority_ordinal,d.date_ordinal
		LIMIT $2`, batchIDs[0], joinedAdminBatchStatusStreamDays+1)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load joined batch status: %v", err))
		return
	}
	defer rows.Close()

	response := joinedAdminBatchStatusResponse{ProtocolVersion: joinedrecording.JoinedProtocolVersion,
		StreamDays: make([]joinedAdminBatchStatusStreamDay, 0, joinedAdminBatchStatusStreamDays)}
	for rows.Next() {
		var batchID, state, denominator string
		var freezeStartedAt, frozenAt *time.Time
		var expectedDays, expectedHours, priorityOrdinal, dateOrdinal int
		var day joinedAdminBatchStatusStreamDay
		if err := rows.Scan(&batchID, &state, &denominator, &freezeStartedAt, &frozenAt, &expectedDays,
			&expectedHours, &priorityOrdinal, &dateOrdinal, &day.RecordingID, &day.LocalDate, &day.State,
			&day.SourceCount, &day.SourceBytes, &day.SealRequestSHA256); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan joined batch status: %v", err))
			return
		}
		if len(response.StreamDays) == 0 {
			response.BatchID, response.State = batchID, state
			response.FrozenDenominatorSHA256 = denominator
			response.FreezeStartedAt, response.FrozenAt = freezeStartedAt, frozenAt
			response.ExpectedStreamDays, response.ExpectedScheduledHours = expectedDays, expectedHours
		}
		wantPriority, wantDate := len(response.StreamDays)/14+1, len(response.StreamDays)%14+1
		if batchID != response.BatchID || state != response.State || denominator != response.FrozenDenominatorSHA256 ||
			expectedDays != response.ExpectedStreamDays || expectedHours != response.ExpectedScheduledHours ||
			priorityOrdinal != wantPriority || dateOrdinal != wantDate || day.RecordingID <= 0 ||
			(day.State != "pending" && day.State != "sealed") ||
			!((day.SourceCount == 0 && day.SourceBytes == 0) || (day.SourceCount > 0 && day.SourceBytes > 0)) ||
			(day.State == "pending" && day.SealRequestSHA256 != "") ||
			(day.State == "sealed" && !lowerHexSHA256(day.SealRequestSHA256)) {
			util.WriteError(w, http.StatusConflict, "joined batch status evidence differs")
			return
		}
		if parsed, parseErr := time.Parse("2006-01-02", day.LocalDate); parseErr != nil || parsed.Format("2006-01-02") != day.LocalDate {
			util.WriteError(w, http.StatusConflict, "joined batch status date differs")
			return
		}
		response.StreamDays = append(response.StreamDays, day)
	}
	if err := rows.Err(); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("iterate joined batch status: %v", err))
		return
	}
	if len(response.StreamDays) == 0 {
		util.WriteError(w, http.StatusNotFound, "joined batch not found")
		return
	}
	if response.BatchID != batchIDs[0] || !lowerHexSHA256(response.FrozenDenominatorSHA256) ||
		response.ExpectedStreamDays != joinedAdminBatchStatusStreamDays ||
		response.ExpectedScheduledHours != joinedAdminBatchStatusStreamDays*12 ||
		len(response.StreamDays) != response.ExpectedStreamDays {
		util.WriteError(w, http.StatusConflict, "joined batch status is incomplete")
		return
	}
	if response.FreezeStartedAt != nil {
		value := response.FreezeStartedAt.UTC()
		response.FreezeStartedAt = &value
	}
	if response.FrozenAt != nil {
		value := response.FrozenAt.UTC()
		response.FrozenAt = &value
	}
	util.WriteJSON(w, http.StatusOK, response)
}
