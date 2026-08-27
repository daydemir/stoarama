package api

import (
	"bytes"
	"encoding/json"
	"net/http"

	"github.com/daydemir/stoarama/backend/internal/util"
)

// writeJoinedWorkerJSON preserves canonical json.RawMessage evidence in worker
// responses. The worker verifies those exact bytes against sealed digests.
func writeJoinedWorkerJSON(w http.ResponseWriter, status int, payload any) {
	var body bytes.Buffer
	encoder := json.NewEncoder(&body)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(payload); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "encode joined worker response")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(body.Bytes())
}
