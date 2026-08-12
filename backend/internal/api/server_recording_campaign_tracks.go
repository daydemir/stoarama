package api

import (
	"net/http"
	"time"

	"github.com/daydemir/stoarama/backend/internal/util"
)

// handleAccountRecordingCampaignTracks is the read-only operational campaign
// board. Roster mutations are intentionally limited to audited operator SQL
// until a separately reviewed write contract exists.
func (s *Server) handleAccountRecordingCampaignTracks(w http.ResponseWriter, r *http.Request) {
	p, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	rows, err := s.pool.Query(r.Context(), `
		SELECT t.campaign_key,t.label,t.deadline_at,t.target_count,t.grade_floor,t.required_consecutive_windows,t.state,
		 e.recording_id,e.stream_id,e.scene_identity_sha256,e.role,e.rank,e.status,e.reason_codes,e.decision_at,e.evidence_observed_at,e.evidence_sha256,
		 r.name,r.status,
		 j.fire_at,j.window_end_at,j.status,
		 COALESCE(first3.clip_count,0),first3.latest_media_at,first3.latest_ingest_at,
		 joblatest.clip_end_at,joblatest.created_at,
		 early.result,early.checked_at,confirm.result,confirm.checked_at,
		 COALESCE(q.good_windows,0),COALESCE(q.total_windows,0),q.latest_coverage_pct,
		 latest.clip_end_at,latest.created_at
		FROM recording_campaign_tracks t
		JOIN recording_campaign_roster_entries e ON e.track_id=t.id
		JOIN recordings r ON r.id=e.recording_id AND r.account_id=t.account_id AND r.stream_id=e.stream_id
		LEFT JOIN LATERAL (SELECT id,fire_at,window_end_at,status FROM recording_jobs x WHERE x.recording_id=r.id AND x.kind='continuous_window' AND x.window_end_at>now() ORDER BY x.fire_at LIMIT 1) j ON true
		LEFT JOIN LATERAL (SELECT count(*)::int clip_count,max(clip_end_at) latest_media_at,max(created_at) latest_ingest_at FROM (SELECT clip_end_at,created_at FROM recording_clips c WHERE c.recording_job_id=j.id ORDER BY created_at LIMIT 3) bounded) first3 ON true
		LEFT JOIN LATERAL (SELECT clip_end_at,created_at FROM recording_clips c WHERE c.recording_job_id=j.id ORDER BY created_at DESC LIMIT 1) joblatest ON true
		LEFT JOIN recording_preopen_checks early ON early.recording_id=r.id AND early.window_start_at=j.fire_at AND early.stage='early'
		LEFT JOIN recording_preopen_checks confirm ON confirm.recording_id=r.id AND confirm.window_start_at=j.fire_at AND confirm.stage='confirm'
		LEFT JOIN LATERAL (SELECT count(*) FILTER(WHERE metric_version>=2 AND coverage_pct>=95 AND largest_gap_seconds<=900 AND gap_over_5m_count<=1 AND gap_over_30s_count<=6 AND overlap_count=0)::int good_windows,
		 count(*)::int total_windows,(array_agg(coverage_pct ORDER BY window_end_at DESC))[1] latest_coverage_pct FROM recording_window_health h WHERE h.recording_id=r.id) q ON true
		LEFT JOIN LATERAL (SELECT clip_end_at,created_at FROM recording_clips c WHERE c.recording_id=r.id ORDER BY created_at DESC LIMIT 1) latest ON true
		WHERE t.account_id=$1 AND t.state IN ('draft','active','complete')
		ORDER BY t.deadline_at,t.campaign_key,e.rank`, p.AccountID)
	if err != nil {
		util.WriteError(w, 500, "read campaign tracks")
		return
	}
	defer rows.Close()
	items := []map[string]any{}
	for rows.Next() {
		var key, label, floor, trackState, scene, role, rosterState, recName, recState string
		var deadline, decision, evidenceAt time.Time
		var evidenceSHA string
		var target, required, rank int
		var recordingID, streamID int64
		var reasons []string
		var fire, end *time.Time
		var jobState *string
		var clips int
		var firstMedia, firstIngest *time.Time
		var jobMedia, jobIngest *time.Time
		var earlyResult, confirmResult *string
		var earlyAt, confirmAt *time.Time
		var goodWindows, totalWindows int
		var latestCoverage *float64
		var latestMedia, latestIngest *time.Time
		if err := rows.Scan(&key, &label, &deadline, &target, &floor, &required, &trackState, &recordingID, &streamID, &scene, &role, &rank, &rosterState, &reasons, &decision, &evidenceAt, &evidenceSHA, &recName, &recState, &fire, &end, &jobState, &clips, &firstMedia, &firstIngest, &jobMedia, &jobIngest, &earlyResult, &earlyAt, &confirmResult, &confirmAt, &goodWindows, &totalWindows, &latestCoverage, &latestMedia, &latestIngest); err != nil {
			util.WriteError(w, 500, "scan campaign tracks")
			return
		}
		checkpoint := "off_window"
		if fire != nil {
			d := time.Until(*fire)
			switch {
			case d > 2*time.Hour && earlyResult == nil:
				checkpoint = "preopen_t_minus_2h_pending"
			case d > 30*time.Minute && earlyResult == nil:
				checkpoint = "preopen_t_minus_2h_due"
			case d > 0 && (confirmResult == nil || *confirmResult != "pass"):
				checkpoint = "preopen_t_minus_30m_due"
			case d <= 0 && (clips < 3 || firstMedia == nil || firstIngest == nil || jobMedia == nil || jobIngest == nil || jobMedia.Before(time.Now().Add(-5*time.Minute)) || jobIngest.Before(time.Now().Add(-5*time.Minute))):
				checkpoint = "first_3_clips_due"
			default:
				checkpoint = "first_3_clips_observed_current_job"
			}
		}
		items = append(items, map[string]any{"campaign_key": key, "label": label, "deadline_at": deadline, "target_count": target, "grade_floor": floor, "required_consecutive_windows": required, "track_state": trackState, "recording_id": recordingID, "stream_id": streamID, "scene_identity_sha256": scene, "role": role, "rank": rank, "roster_status": rosterState, "reason_codes": reasons, "decision_at": decision, "evidence_observed_at": evidenceAt, "evidence_sha256": evidenceSHA, "recording_name": recName, "recording_status": recState, "next_window_start_at": fire, "next_window_end_at": end, "job_status": jobState, "current_job_first3_clip_count": clips, "current_job_first3_latest_media_at": firstMedia, "current_job_first3_latest_ingest_at": firstIngest, "preopen_early_result": earlyResult, "preopen_early_checked_at": earlyAt, "preopen_confirm_result": confirmResult, "preopen_confirm_checked_at": confirmAt, "checkpoint": checkpoint, "health_metric_good_candidate_windows": goodWindows, "health_metric_windows": totalWindows, "latest_coverage_pct": latestCoverage, "latest_media_at": latestMedia, "latest_ingest_at": latestIngest})
	}
	if err := rows.Err(); err != nil {
		util.WriteError(w, 500, "read campaign tracks")
		return
	}
	util.WriteJSON(w, 200, map[string]any{"generated_at": time.Now().UTC(), "scope": "audited campaign roster; health_metric_good_candidate_windows is prioritization evidence, not strict qualification; exact streak, NAS and stitch certification remain separate", "items": items})
}
