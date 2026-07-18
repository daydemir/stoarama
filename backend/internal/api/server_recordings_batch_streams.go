package api

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/daydemir/stoarama/backend/internal/util"
)

func (s *Server) handleAccountRecordingBatchStreams(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	ids := make([]int64, 0)
	for _, part := range strings.Split(strings.TrimSpace(r.URL.Query().Get("ids")), ",") {
		id, err := strconv.ParseInt(strings.TrimSpace(part), 10, 64)
		if err != nil || id <= 0 {
			util.WriteError(w, http.StatusBadRequest, "ids must be comma-separated positive integers")
			return
		}
		ids = append(ids, id)
	}
	var err error
	ids, err = uniqueBatchStreamIDs(ids)
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	rows, err := s.pool.Query(r.Context(), `
		SELECT st.id, st.name, st.location_text, st.source_url, st.tags, st.local_timezone,
		       (SELECT rec.id FROM recordings rec
		        WHERE rec.account_id=$2 AND rec.stream_id=st.id AND rec.status <> 'canceled'
		        ORDER BY rec.id DESC LIMIT 1)
		FROM streams st
		WHERE st.source_url <> '' AND st.deleted_at IS NULL AND st.id = ANY($1::bigint[])
		ORDER BY st.id
	`, ids, principal.AccountID)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("list batch streams: %v", err))
		return
	}
	defer rows.Close()
	items := make([]map[string]any, 0, len(ids))
	for rows.Next() {
		var id int64
		var name, location, sourceURL, timezone string
		var tags []string
		var recordingID *int64
		if err := rows.Scan(&id, &name, &location, &sourceURL, &tags, &timezone, &recordingID); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan batch stream: %v", err))
			return
		}
		if _, err := classifyRecordingSource(strings.TrimSpace(sourceURL)); err != nil && !isYouTubeWatchURL(sourceURL) {
			continue
		}
		items = append(items, map[string]any{"id": id, "name": name, "location_text": location, "tags": tags, "local_timezone": timezone, "recording_id": recordingID})
	}
	if err := rows.Err(); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("iterate batch streams: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"items": items})
}
