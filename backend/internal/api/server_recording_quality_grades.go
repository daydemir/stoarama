package api

import (
	"log"
	"net/http"
	"time"

	"github.com/daydemir/stoarama/backend/internal/qualitygrade"
	"github.com/daydemir/stoarama/backend/internal/util"
)

type recordingQualityGradesResponse struct {
	GeneratedAt time.Time                `json:"generated_at"`
	Days        int                      `json:"days"`
	RunLength   int                      `json:"run_length"`
	Recordings  []qualitygrade.Recording `json:"recordings"`
}

// handleAccountRecordingQualityGrades reports each active continuous
// recording's daily A-F window grades and its Fine+/Good+/Great+ 14-day tier
// progress, from the same definition the health monitor alerts on.
func (s *Server) handleAccountRecordingQualityGrades(w http.ResponseWriter, r *http.Request) {
	p, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	days := parseIntQuery(r, "days", 30, 1, 120)
	now := time.Now().UTC()
	recs, err := qualitygrade.Load(r.Context(), s.pool, p.AccountID, now, days)
	if err != nil {
		log.Printf("quality grades account=%d: %v", p.AccountID, err)
		util.WriteError(w, http.StatusInternalServerError, "read quality grades")
		return
	}
	util.WriteJSON(w, http.StatusOK, recordingQualityGradesResponse{
		GeneratedAt: now, Days: days, RunLength: qualitygrade.RunLength, Recordings: recs,
	})
}
