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
	"math"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/recsched"
	"github.com/daydemir/stoarama/backend/internal/util"
	"github.com/jackc/pgx/v5"
)

const recordingQualificationDefinition = "recording-qualification-v1"
const recordingQualificationMetricVersion = 2
const qualificationWindowGeneratorVersion = "recsched-next-full-v1"

type sceneAttestRequest struct {
	RecordingID   int64  `json:"recording_id"`
	FrameID       int64  `json:"frame_id"`
	SceneIdentity string `json:"scene_identity"`
}

func decodeQualificationJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		util.WriteError(w, http.StatusBadRequest, "invalid JSON body")
		return false
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		util.WriteError(w, http.StatusBadRequest, "body must contain one JSON object")
		return false
	}
	return true
}

func sha256Hex(v []byte) string {
	sum := sha256.Sum256(v)
	return hex.EncodeToString(sum[:])
}

func normalizeSceneIdentity(raw string) (string, error) {
	v := strings.Join(strings.Fields(strings.TrimSpace(raw)), " ")
	if len(v) < 3 || len(v) > 240 {
		return "", fmt.Errorf("scene_identity must be 3-240 characters")
	}
	return strings.ToLower(v), nil
}

func (s *Server) handleAccountRecordingSceneAttest(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok || principal.UserID == 0 {
		util.WriteError(w, http.StatusForbidden, "member session required")
		return
	}
	var req sceneAttestRequest
	if !decodeQualificationJSON(w, r, &req) {
		return
	}
	identity, err := normalizeSceneIdentity(req.SceneIdentity)
	if err != nil || req.RecordingID <= 0 || req.FrameID <= 0 {
		util.WriteError(w, http.StatusBadRequest, "recording_id, frame_id, and valid scene_identity are required")
		return
	}
	sceneHash := sha256Hex([]byte("stoarama-scene-identity-v1\n" + identity))
	var evidenceID int64
	var evidenceHash string
	err = s.pool.QueryRow(r.Context(), `
		WITH authoritative AS (
		 SELECT rec.stream_id,f.raw_media_object_id,f.captured_at,lower(m.sha256) frame_sha
		 FROM recordings rec JOIN frames f ON f.id=$3 AND f.stream_id=rec.stream_id
		 JOIN media_objects m ON m.id=f.raw_media_object_id
		 WHERE rec.id=$2 AND rec.account_id=$1 AND rec.status='active'
		   AND f.capture_status='success' AND f.captured_at>=now()-interval '24 hours'
	), inserted AS (
		 INSERT INTO recording_scene_frame_evidence(account_id,stream_id,frame_id,media_object_id,captured_at,frame_sha256,scene_identity_sha256,verification_method,verified_by_user_id)
		 SELECT $1,stream_id,$3,raw_media_object_id,captured_at,frame_sha,$4,'operator_visual',$5 FROM authoritative
		 ON CONFLICT(account_id,frame_id) DO NOTHING RETURNING id,evidence_sha256
	)
	SELECT id,evidence_sha256 FROM inserted
	UNION ALL
	SELECT e.id,e.evidence_sha256 FROM recording_scene_frame_evidence e
	WHERE e.account_id=$1 AND e.frame_id=$3 AND e.scene_identity_sha256=$4 AND e.verified_by_user_id=$5
	LIMIT 1
	`, principal.AccountID, req.RecordingID, req.FrameID, sceneHash, principal.UserID).Scan(&evidenceID, &evidenceHash)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusConflict, "frame is not a current authoritative successful frame, or it was attested differently")
		return
	}
	if err != nil {
		log.Printf("scene attestation failed account_id=%d recording_id=%d: %v", principal.AccountID, req.RecordingID, err)
		util.WriteError(w, http.StatusInternalServerError, "store scene attestation")
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"evidence_id": evidenceID, "recording_id": req.RecordingID, "frame_id": req.FrameID, "scene_identity_sha256": sceneHash, "evidence_sha256": evidenceHash})
}

type qualificationBuildRequest struct {
	RecordingIDs       []int64   `json:"recording_ids"`
	SequenceStart      time.Time `json:"sequence_start_at"`
	Apply              bool      `json:"apply"`
	ExpectedPlanSHA256 string    `json:"expected_plan_sha256,omitempty"`
}

type qualificationPlanMember struct {
	RecordingID   int64                       `json:"recording_id"`
	StreamID      int64                       `json:"stream_id"`
	Name          string                      `json:"name"`
	StreamName    string                      `json:"stream_name"`
	EvidenceID    int64                       `json:"evidence_id"`
	SceneHash     string                      `json:"scene_identity_sha256"`
	Timezone      string                      `json:"timezone"`
	Start         string                      `json:"daily_window_start"`
	End           string                      `json:"daily_window_end"`
	Weekdays      int16                       `json:"active_weekdays"`
	ScheduleStart time.Time                   `json:"schedule_start_at"`
	ScheduleEnd   *time.Time                  `json:"schedule_end_at,omitempty"`
	Windows       []recsched.ContinuousWindow `json:"windows"`
}

type qualificationPlan struct {
	DefinitionVersion string                    `json:"definition_version"`
	SequenceStart     time.Time                 `json:"sequence_start_at"`
	TargetCount       int                       `json:"target_recording_count"`
	RecordingIDs      []int64                   `json:"recording_ids"`
	Members           []qualificationPlanMember `json:"members"`
	PlanSHA256        string                    `json:"plan_sha256"`
}

func (s *Server) buildQualificationPlan(ctx context.Context, accountID int64, req qualificationBuildRequest) (qualificationPlan, error) {
	ids := append([]int64(nil), req.RecordingIDs...)
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	if len(ids) < 50 || req.SequenceStart.IsZero() {
		return qualificationPlan{}, fmt.Errorf("at least 50 explicit recording_ids and sequence_start_at are required")
	}
	for i, id := range ids {
		if id <= 0 || (i > 0 && id == ids[i-1]) {
			return qualificationPlan{}, fmt.Errorf("recording_ids must be unique positive integers")
		}
	}
	rows, err := s.pool.Query(ctx, `
		SELECT r.id,r.stream_id,r.name,s.name,e.id,e.scene_identity_sha256,r.cron_timezone,
		 to_char(r.daily_window_start,'HH24:MI:SS'),to_char(r.daily_window_end,'HH24:MI:SS'),r.active_weekdays,r.start_at,r.end_at
		FROM unnest($2::bigint[]) WITH ORDINALITY q(id,ord)
		JOIN recordings r ON r.id=q.id AND r.account_id=$1 AND r.status='active' AND r.mode='continuous'
		JOIN streams s ON s.id=r.stream_id
		JOIN LATERAL (SELECT id,scene_identity_sha256 FROM recording_scene_frame_evidence WHERE account_id=$1 AND stream_id=r.stream_id ORDER BY verified_at DESC,id DESC LIMIT 1)e ON true
		ORDER BY q.ord`, accountID, ids)
	if err != nil {
		return qualificationPlan{}, err
	}
	defer rows.Close()
	plan := qualificationPlan{DefinitionVersion: recordingQualificationDefinition, SequenceStart: req.SequenceStart.UTC(), TargetCount: len(ids), RecordingIDs: ids}
	seenScenes := map[string]int64{}
	for rows.Next() {
		var m qualificationPlanMember
		if err := rows.Scan(&m.RecordingID, &m.StreamID, &m.Name, &m.StreamName, &m.EvidenceID, &m.SceneHash, &m.Timezone, &m.Start, &m.End, &m.Weekdays, &m.ScheduleStart, &m.ScheduleEnd); err != nil {
			return qualificationPlan{}, err
		}
		if m.Start != "08:00:00" || m.End != "20:00:00" {
			return qualificationPlan{}, fmt.Errorf("recording %d is not an 08:00-20:00 schedule", m.RecordingID)
		}
		if prior, exists := seenScenes[m.SceneHash]; exists {
			return qualificationPlan{}, fmt.Errorf("recordings %d and %d attest the same scene", prior, m.RecordingID)
		}
		seenScenes[m.SceneHash] = m.RecordingID
		start, err := recsched.ParseTimeOfDay(m.Start)
		if err != nil {
			return qualificationPlan{}, err
		}
		end, err := recsched.ParseTimeOfDay(m.End)
		if err != nil {
			return qualificationPlan{}, err
		}
		m.Windows, err = recsched.NextFullContinuousWindowsOn(m.Timezone, start, end, recsched.WeekdaySet(m.Weekdays), m.ScheduleStart, zeroIfNil(m.ScheduleEnd), plan.SequenceStart, 14)
		if err != nil {
			return qualificationPlan{}, fmt.Errorf("recording %d: %w", m.RecordingID, err)
		}
		plan.Members = append(plan.Members, m)
	}
	if err := rows.Err(); err != nil {
		return qualificationPlan{}, err
	}
	if len(plan.Members) != len(ids) {
		return qualificationPlan{}, fmt.Errorf("selected cohort is missing active recordings or current scene evidence: got %d of %d", len(plan.Members), len(ids))
	}
	canonical, _ := json.Marshal(plan)
	plan.PlanSHA256 = sha256Hex(canonical)
	return plan, nil
}

func zeroIfNil(v *time.Time) time.Time {
	if v == nil {
		return time.Time{}
	}
	return *v
}

func (s *Server) handleAccountRecordingQualificationBuild(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok || principal.UserID == 0 {
		util.WriteError(w, http.StatusForbidden, "member session required")
		return
	}
	var req qualificationBuildRequest
	if !decodeQualificationJSON(w, r, &req) {
		return
	}
	plan, err := s.buildQualificationPlan(r.Context(), principal.AccountID, req)
	if err != nil {
		util.WriteError(w, http.StatusConflict, err.Error())
		return
	}
	if !req.Apply {
		util.WriteJSON(w, http.StatusOK, map[string]any{"dry_run": true, "plan": plan})
		return
	}
	if len(req.ExpectedPlanSHA256) != 64 || !strings.EqualFold(req.ExpectedPlanSHA256, plan.PlanSHA256) {
		util.WriteError(w, http.StatusConflict, "expected_plan_sha256 does not match current plan")
		return
	}
	tx, err := s.pool.BeginTx(r.Context(), pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		util.WriteError(w, 500, "start qualification transaction")
		return
	}
	defer tx.Rollback(r.Context())
	if _, err = tx.Exec(r.Context(), `SELECT pg_advisory_xact_lock($1,$2)`, principal.AccountID, int64(0x5155414c)); err != nil {
		util.WriteError(w, 500, "lock qualification build")
		return
	}
	var existing int64
	var existingPlan string
	var existingCount int
	var existingStart time.Time
	err = tx.QueryRow(r.Context(), `SELECT id,COALESCE(definition_jsonb->>'plan_sha256',''),target_recording_count,window_sequence_start_at FROM recording_qualification_runs WHERE account_id=$1 AND status='active'`, principal.AccountID).Scan(&existing, &existingPlan, &existingCount, &existingStart)
	if err == nil {
		if existingPlan != plan.PlanSHA256 || existingCount != len(plan.Members) || !existingStart.Equal(plan.SequenceStart) {
			util.WriteError(w, http.StatusConflict, "a different active qualification run already exists")
			return
		}
		util.WriteJSON(w, http.StatusOK, map[string]any{"run_id": existing, "idempotent": true, "plan_sha256": plan.PlanSHA256})
		return
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, 500, "check active qualification run")
		return
	}
	definition := map[string]any{"version": recordingQualificationDefinition, "window_count": 14, "required_good_or_great": 13, "max_acceptable": 1, "plan_sha256": plan.PlanSHA256}
	var runID int64
	err = tx.QueryRow(r.Context(), `INSERT INTO recording_qualification_runs(account_id,definition_version,definition_jsonb,target_recording_count,window_sequence_start_at) VALUES($1,$2,$3,$4,$5) RETURNING id`, principal.AccountID, recordingQualificationDefinition, definition, len(plan.Members), plan.SequenceStart).Scan(&runID)
	if err != nil {
		util.WriteError(w, 500, "create qualification run")
		return
	}
	for i, m := range plan.Members {
		scheduleJSON, _ := json.Marshal(struct {
			TZ, Start, End string
			Weekdays       int16
		}{m.Timezone, m.Start, m.End, m.Weekdays})
		windowJSON, _ := json.Marshal(m.Windows)
		_, err = tx.Exec(r.Context(), `INSERT INTO recording_qualification_members(run_id,account_id,recording_id,ordinal,stream_id,recording_name,stream_name,scene_identity_sha256,scene_frame_evidence_id,cron_timezone,daily_window_start,daily_window_end,active_weekdays,schedule_start_at,schedule_end_at,window_generator_version,schedule_config_sha256,window_sequence_sha256) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)`, runID, principal.AccountID, m.RecordingID, i+1, m.StreamID, m.Name, m.StreamName, m.SceneHash, m.EvidenceID, m.Timezone, m.Start, m.End, m.Weekdays, m.ScheduleStart, m.ScheduleEnd, qualificationWindowGeneratorVersion, sha256Hex(scheduleJSON), sha256Hex(windowJSON))
		if err != nil {
			util.WriteError(w, 500, "insert qualification member")
			return
		}
		for _, win := range m.Windows {
			_, offOpen := win.LocalOpenAt.Zone()
			_, offEnd := win.LocalEndAt.Zone()
			_, err = tx.Exec(r.Context(), `INSERT INTO recording_qualification_windows(run_id,recording_id,ordinal,local_open_at,local_end_at,open_utc_offset_seconds,end_utc_offset_seconds,window_start_at,window_end_at,expected_seconds) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`, runID, m.RecordingID, win.Ordinal, win.LocalOpenAt.Format("2006-01-02 15:04:05"), win.LocalEndAt.Format("2006-01-02 15:04:05"), offOpen, offEnd, win.OpenAt, win.EndAt, int64(win.EndAt.Sub(win.OpenAt).Seconds()))
			if err != nil {
				util.WriteError(w, 500, "insert qualification window")
				return
			}
		}
	}
	if _, err = tx.Exec(r.Context(), `UPDATE recording_qualification_runs SET status='active',frozen_at=now() WHERE id=$1 AND status='building'`, runID); err != nil {
		util.WriteError(w, http.StatusConflict, "freeze qualification run: "+err.Error())
		return
	}
	if err = tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusConflict, "commit qualification run")
		return
	}
	util.WriteJSON(w, http.StatusCreated, map[string]any{"run_id": runID, "frozen": true, "target_recordings": len(plan.Members), "target_windows": 14, "plan_sha256": plan.PlanSHA256})
}

type qualificationWindowMetrics struct {
	CoveragePct      *float64
	LargestGap       *float64
	GapsOver30s      *int
	GapsOver5m       *int
	OverlapCount     *int
	MetricVersion    *int
	ExpectedSeconds  int64
	MeasuredExpected *int64
	JobCount         int
	HealthCount      int
	LateClip         bool
}

func classifyQualificationTimeline(m qualificationWindowMetrics) (string, string) {
	if m.JobCount != 1 {
		return "UNKNOWN", "expected exactly one continuous job"
	}
	if m.HealthCount != 1 || m.CoveragePct == nil || m.LargestGap == nil || m.GapsOver30s == nil ||
		m.GapsOver5m == nil || m.OverlapCount == nil || m.MetricVersion == nil || m.MeasuredExpected == nil {
		return "UNKNOWN", "timeline metrics unavailable"
	}
	if *m.MetricVersion != recordingQualificationMetricVersion || *m.MeasuredExpected != m.ExpectedSeconds || m.LateClip ||
		math.IsNaN(*m.CoveragePct) || math.IsInf(*m.CoveragePct, 0) || math.IsNaN(*m.LargestGap) || math.IsInf(*m.LargestGap, 0) ||
		*m.CoveragePct < 0 || *m.CoveragePct > 100 || *m.LargestGap < 0 || *m.GapsOver30s < 0 ||
		*m.GapsOver5m < 0 || *m.OverlapCount < 0 || *m.GapsOver5m > *m.GapsOver30s ||
		(*m.LargestGap > 30 && *m.GapsOver30s == 0) || (*m.LargestGap > 300 && *m.GapsOver5m == 0) {
		return "UNKNOWN", "timeline metrics stale or inconsistent"
	}
	if *m.OverlapCount != 0 {
		return "FAILED", "overlap detected"
	}
	if *m.CoveragePct >= 99 && *m.LargestGap <= 120 {
		return "GREAT_CANDIDATE", "native stitch certification required"
	}
	if *m.CoveragePct >= 95 && *m.LargestGap <= 900 && *m.GapsOver5m <= 1 && *m.GapsOver30s <= 6 {
		return "GOOD_CANDIDATE", "native stitch certification required"
	}
	if *m.CoveragePct >= 90 && *m.LargestGap <= 1800 && *m.GapsOver5m <= 2 {
		return "ACCEPTABLE_CANDIDATE", "native stitch certification required"
	}
	return "FAILED", "timeline below acceptable threshold"
}

type recordingQualificationWindow struct {
	Ordinal                   int       `json:"ordinal"`
	WindowStartAt             time.Time `json:"window_start_at"`
	WindowEndAt               time.Time `json:"window_end_at"`
	ExpectedSeconds           int64     `json:"expected_seconds"`
	JobCount                  int       `json:"job_count"`
	HealthCount               int       `json:"health_count"`
	TimelineGrade             string    `json:"timeline_grade"`
	Reason                    string    `json:"reason"`
	CoveragePct               *float64  `json:"coverage_pct"`
	LargestGapSeconds         *float64  `json:"largest_gap_seconds"`
	GapOver30sCount           *int      `json:"gap_over_30s_count"`
	GapOver5mCount            *int      `json:"gap_over_5m_count"`
	OverlapCount              *int      `json:"overlap_count"`
	NASByteProof              string    `json:"nas_byte_proof"`
	NativeStitchCertification string    `json:"native_stitch_certification"`
}

type recordingQualificationMember struct {
	RecordingID  int64                          `json:"recording_id"`
	Name         string                         `json:"name"`
	Ordinal      int                            `json:"ordinal"`
	Status       string                         `json:"status"`
	Strict14Good bool                           `json:"strict_14_good"`
	Windows      []recordingQualificationWindow `json:"windows"`
}

type recordingQualificationResponse struct {
	ReportVersion     string                         `json:"report_version"`
	DefinitionVersion string                         `json:"definition_version"`
	Scope             string                         `json:"scope"`
	RunID             int64                          `json:"run_id"`
	RunStatus         string                         `json:"run_status"`
	FrozenAt          time.Time                      `json:"frozen_at"`
	TargetRecordings  int                            `json:"target_recordings"`
	QualifiedCount    int                            `json:"qualified_count"`
	Strict14GoodCount int                            `json:"strict_14_good_count"`
	Members           []recordingQualificationMember `json:"members"`
}

func (s *Server) handleAccountRecordingQualification(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	var out recordingQualificationResponse
	err := s.pool.QueryRow(r.Context(), `
		SELECT id,status,frozen_at,target_recording_count,definition_version
		FROM recording_qualification_runs
		WHERE account_id=$1 AND status IN ('active','canceled') AND frozen_at IS NOT NULL
		ORDER BY (status='active') DESC,frozen_at DESC LIMIT 1
	`, principal.AccountID).Scan(&out.RunID, &out.RunStatus, &out.FrozenAt, &out.TargetRecordings, &out.DefinitionVersion)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			util.WriteError(w, http.StatusNotFound, "no frozen qualification run")
		} else {
			log.Printf("qualification run query failed account_id=%d: %v", principal.AccountID, err)
			util.WriteError(w, http.StatusInternalServerError, "read qualification run")
		}
		return
	}
	out.ReportVersion = recordingQualificationDefinition
	out.Scope = "timeline_candidates_only; NAS byte and native stitch evidence are mandatory and currently unknown"
	rows, err := s.pool.Query(r.Context(), `
		SELECT m.recording_id,m.recording_name,m.ordinal,w.ordinal,w.window_start_at,w.window_end_at,w.expected_seconds,
		       j.job_count,h.health_count,
		       h.coverage_pct,h.largest_gap_seconds,h.gap_over_30s_count,h.gap_over_5m_count,h.overlap_count,
		       h.metric_version,h.expected_seconds,
		       EXISTS (SELECT 1 FROM recording_clips c WHERE c.recording_id=m.recording_id
		          AND c.clip_end_at>w.window_start_at AND c.clip_start_at<w.window_end_at AND c.created_at>h.calculated_at) AS late_clip
		FROM recording_qualification_members m
		JOIN recording_qualification_windows w ON w.run_id=m.run_id AND w.recording_id=m.recording_id
		LEFT JOIN LATERAL (
		  SELECT count(*)::int AS job_count,min(id) AS job_id
		  FROM recording_jobs
		  WHERE recording_id=m.recording_id AND kind='continuous_window'
		    AND fire_at=w.window_start_at AND window_end_at=w.window_end_at
		) j ON true
		LEFT JOIN LATERAL (
		  SELECT count(*)::int AS health_count,max(coverage_pct) AS coverage_pct,
		    max(largest_gap_seconds) AS largest_gap_seconds,max(gap_over_30s_count) AS gap_over_30s_count,
		    max(gap_over_5m_count) AS gap_over_5m_count,max(overlap_count) AS overlap_count,
		    max(metric_version) AS metric_version,max(expected_seconds) AS expected_seconds,max(calculated_at) AS calculated_at
		  FROM recording_window_health WHERE recording_id=m.recording_id AND job_id=j.job_id
		) h ON true
		WHERE m.run_id=$1 ORDER BY m.ordinal,w.ordinal
	`, out.RunID)
	if err != nil {
		log.Printf("qualification query failed run_id=%d: %v", out.RunID, err)
		util.WriteError(w, http.StatusInternalServerError, "read qualification matrix")
		return
	}
	defer rows.Close()
	memberIndex := map[int64]int{}
	for rows.Next() {
		var recordingID int64
		var name string
		var memberOrdinal, ordinal, jobCount, healthCount int
		var start, end time.Time
		var expected int64
		var coverage, largest *float64
		var over30, over5, overlaps, version *int
		var measuredExpected *int64
		var late bool
		if err := rows.Scan(&recordingID, &name, &memberOrdinal, &ordinal, &start, &end, &expected, &jobCount, &healthCount,
			&coverage, &largest, &over30, &over5, &overlaps, &version, &measuredExpected, &late); err != nil {
			util.WriteError(w, http.StatusInternalServerError, "scan qualification matrix")
			return
		}
		i, exists := memberIndex[recordingID]
		if !exists {
			i = len(out.Members)
			memberIndex[recordingID] = i
			out.Members = append(out.Members, recordingQualificationMember{RecordingID: recordingID, Name: name, Ordinal: memberOrdinal, Status: "UNKNOWN", Windows: []recordingQualificationWindow{}})
		}
		grade, reason := classifyQualificationTimeline(qualificationWindowMetrics{
			CoveragePct: coverage, LargestGap: largest, GapsOver30s: over30, GapsOver5m: over5,
			OverlapCount: overlaps, MetricVersion: version, ExpectedSeconds: expected,
			MeasuredExpected: measuredExpected, JobCount: jobCount, HealthCount: healthCount, LateClip: late,
		})
		out.Members[i].Windows = append(out.Members[i].Windows, recordingQualificationWindow{Ordinal: ordinal, WindowStartAt: start, WindowEndAt: end, ExpectedSeconds: expected, JobCount: jobCount, HealthCount: healthCount, TimelineGrade: grade, Reason: reason, CoveragePct: coverage, LargestGapSeconds: largest, GapOver30sCount: over30, GapOver5mCount: over5, OverlapCount: overlaps, NASByteProof: "UNKNOWN_NOT_CERTIFIED", NativeStitchCertification: "UNKNOWN_NOT_CERTIFIED"})
	}
	if err := rows.Err(); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "read qualification matrix")
		return
	}
	// Qualification remains UNKNOWN until both mandatory evidence axes are durably certified.
	for i := range out.Members {
		out.Members[i].Status = "UNKNOWN"
	}
	util.WriteJSON(w, http.StatusOK, out)
}
