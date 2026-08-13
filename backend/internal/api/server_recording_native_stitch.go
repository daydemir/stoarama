package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"regexp"
	"strconv"
	"time"

	"github.com/daydemir/stoarama/backend/internal/stitchcert"
	"github.com/daydemir/stoarama/backend/internal/util"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const (
	nativeStitchLease       = 45 * time.Minute
	nativeStitchMaxAttempts = 5
)

var nativeStitchReason = regexp.MustCompile(`^[a-z][a-z0-9_]{0,79}$`)

var nativeStitchDeterministicFailures = map[string]bool{"clip_decode_failed": true, "run_concat_failed": true}

type nativeStitchQueryer interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

func loadNativeStitchManifest(ctx context.Context, q nativeStitchQueryer, accountID, recordingID, jobID int64, start, end time.Time) ([]stitchcert.ManifestClip, string, int64, error) {
	rows, err := q.Query(ctx, `
		SELECT c.id,c.recording_id,COALESCE(c.recording_job_id,0),COALESCE(c.display_path,''),c.size_bytes,lower(c.sha256),
		       c.clip_start_at,c.clip_end_at,c.capture_lease_token,c.capture_sequence,c.purged_at,
		       c.capture_attempt_id,c.timestamp_contract_version,c.timestamp_contract_status,
		       COALESCE(c.timestamp_contract_reason,''),c.timestamp_contract
		FROM recording_clips c JOIN recordings r ON r.id=c.recording_id
		WHERE r.account_id=$1 AND c.recording_id=$2 AND c.clip_end_at>$3 AND c.clip_start_at<$4
		ORDER BY c.clip_start_at,c.id
		FOR SHARE OF c`, accountID, recordingID, start, end)
	if err != nil {
		return nil, "", 0, err
	}
	defer rows.Close()
	clips := []stitchcert.ManifestClip{}
	for rows.Next() {
		var c stitchcert.ManifestClip
		var token *uuid.UUID
		var sequence *int64
		var purged *time.Time
		var attempt *uuid.UUID
		var timestampVersion, timestampStatus *string
		var timestampReason string
		var timestampContract []byte
		if err := rows.Scan(&c.ClipID, &c.RecordingID, &c.RecordingJobID, &c.RelativePath, &c.SizeBytes, &c.SHA256, &c.ClipStartAt, &c.ClipEndAt, &token, &sequence, &purged, &attempt, &timestampVersion, &timestampStatus, &timestampReason, &timestampContract); err != nil {
			return nil, "", 0, err
		}
		if c.RecordingJobID != jobID || token == nil || sequence == nil || purged != nil {
			return nil, "", 0, fmt.Errorf("window has noncanonical, cross-job, or purged media")
		}
		generation := captureGenerationFingerprint(token)
		if generation == nil {
			return nil, "", 0, fmt.Errorf("window clip lacks capture generation")
		}
		c.Ordinal = len(clips) + 1
		c.CaptureGeneration = *generation
		c.CaptureSequence = *sequence
		if attempt != nil {
			c.CaptureAttemptID = attempt.String()
		}
		if timestampVersion != nil {
			c.TimestampContractVersion = *timestampVersion
		}
		if timestampStatus != nil {
			c.TimestampContractStatus = *timestampStatus
		}
		c.TimestampContractReason = timestampReason
		if len(timestampContract) != 0 {
			var contract stitchcert.TimestampContract
			if err := json.Unmarshal(timestampContract, &contract); err != nil {
				return nil, "", 0, fmt.Errorf("invalid stored timestamp contract")
			}
			c.TimestampContractSHA256, _, err = stitchcert.CanonicalSHA(contract)
			if err != nil {
				return nil, "", 0, err
			}
		}
		clips = append(clips, c)
	}
	if err := rows.Err(); err != nil {
		return nil, "", 0, err
	}
	total, err := stitchcert.ValidateManifest(clips, recordingID, jobID, start, end)
	if err != nil {
		return nil, "", 0, err
	}
	digest, _, err := stitchcert.CanonicalSHA(clips)
	return clips, digest, total, err
}

type nativeStitchClaimResponse struct {
	TaskID               int64                     `json:"task_id"`
	ClaimToken           uuid.UUID                 `json:"claim_token"`
	LeaseExpiresAt       time.Time                 `json:"lease_expires_at"`
	RecordingID          int64                     `json:"recording_id"`
	RecordingJobID       int64                     `json:"recording_job_id"`
	WindowStartAt        time.Time                 `json:"window_start_at"`
	WindowEndAt          time.Time                 `json:"window_end_at"`
	ClipManifestSHA256   string                    `json:"clip_manifest_sha256"`
	PolicyVersion        string                    `json:"policy_version"`
	QualificationScope   string                    `json:"qualification_scope"`
	Clips                []stitchcert.ManifestClip `json:"clips"`
	InventoryGeneration  string                    `json:"inventory_generation"`
	InventoryDigest      string                    `json:"inventory_digest"`
	InventoryCompletedAt time.Time                 `json:"inventory_completed_at"`
}

func (s *Server) handleAccountNativeStitchClaim(w http.ResponseWriter, r *http.Request) {
	p, ok := accountPrincipalFromContext(r.Context())
	if !ok || p.APIKeyID == nil {
		util.WriteError(w, 403, "NAS pull key required")
		return
	}
	tx, err := s.pool.BeginTx(r.Context(), pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		util.WriteError(w, 500, "begin stitch claim")
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()
	var connectionID int64
	var generation, digest, inProgress string
	var completed *time.Time
	var skipped int64
	if err = tx.QueryRow(r.Context(), `SELECT id,inventory_generation,inventory_digest,inventory_scan_completed_at,inventory_scan_rows_skipped,inventory_in_progress_generation FROM connections WHERE api_key_id=$1 AND account_id=$2 FOR UPDATE`, *p.APIKeyID, p.AccountID).Scan(&connectionID, &generation, &digest, &completed, &skipped, &inProgress); err != nil {
		util.WriteError(w, 403, "no NAS connection for this key")
		return
	}
	if generation == "" || len(digest) != 64 || completed == nil || skipped != 0 || inProgress != "" {
		util.WriteJSON(w, 200, map[string]any{"task": nil, "reason": "inventory_not_idle_or_complete"})
		return
	}
	var backlog bool
	if err = tx.QueryRow(r.Context(), `SELECT EXISTS(SELECT 1 FROM recording_clips c JOIN recordings rec ON rec.id=c.recording_id WHERE rec.account_id=$1 AND rec.delivery='nas_pull' AND c.purged_at IS NULL AND c.released_at IS NULL AND c.created_at<now()-`+accountClipsCommitWatermark+`)`, p.AccountID).Scan(&backlog); err != nil {
		util.WriteError(w, 500, "check delivery backlog")
		return
	}
	if backlog {
		util.WriteJSON(w, 200, map[string]any{"task": nil, "reason": "delivery_backlog"})
		return
	}
	_, err = tx.Exec(r.Context(), `UPDATE recording_native_stitch_tasks SET state='pending',claim_token=NULL,claimed_connection_id=NULL,lease_expires_at=NULL,next_attempt_at=now(),last_reason_code='claim_expired' WHERE account_id=$1 AND state='leased' AND lease_expires_at<=now()`, p.AccountID)
	if err != nil {
		util.WriteError(w, 500, "expire stitch claims")
		return
	}
	if _, err = tx.Exec(r.Context(), `UPDATE recording_native_stitch_tasks SET state='stale',last_reason_code='attempt_limit_reached' WHERE account_id=$1 AND state='pending' AND attempt_count>=$2`, p.AccountID, nativeStitchMaxAttempts); err != nil {
		util.WriteError(w, 500, "retire exhausted stitch tasks")
		return
	}
	type candidate struct {
		out nativeStitchClaimResponse
		raw []byte
	}
	claimRows, err := tx.Query(r.Context(), `
		SELECT t.id,t.recording_id,t.recording_job_id,t.window_start_at,t.window_end_at,t.clip_manifest_sha256,t.policy_version,t.qualification_scope,t.clip_manifest
		FROM recording_native_stitch_tasks t
		WHERE t.account_id=$1 AND t.state='pending' AND t.next_attempt_at<=now()
		  AND (SELECT count(*) FROM nas_inventory_files i
		       JOIN recording_native_stitch_task_clips m ON m.task_id=t.id AND m.clip_id=i.clip_id
		       WHERE i.connection_id=$2 AND i.seen_generation=$3 AND i.state='present' AND i.relative_path=m.relative_path AND i.size_bytes=m.size_bytes AND i.sha256=m.sha256
		         AND NOT EXISTS(SELECT 1 FROM nas_inventory_files x WHERE x.connection_id=i.connection_id AND x.relative_path=i.relative_path AND x.clip_id<>i.clip_id AND x.state IN('present','mismatch'))
		         AND NOT EXISTS(SELECT 1 FROM nas_inventory_unmatched_files u WHERE u.connection_id=i.connection_id AND u.relative_path=i.relative_path AND u.state='present'))=t.clip_count
		ORDER BY t.priority,t.next_attempt_at,t.id FOR UPDATE OF t SKIP LOCKED LIMIT 1`, p.AccountID, connectionID, generation)
	if err != nil {
		util.WriteError(w, 500, "select stitch tasks")
		return
	}
	candidates := []candidate{}
	for claimRows.Next() {
		var c candidate
		if err := claimRows.Scan(&c.out.TaskID, &c.out.RecordingID, &c.out.RecordingJobID, &c.out.WindowStartAt, &c.out.WindowEndAt, &c.out.ClipManifestSHA256, &c.out.PolicyVersion, &c.out.QualificationScope, &c.raw); err != nil {
			claimRows.Close()
			util.WriteError(w, 500, "scan stitch task")
			return
		}
		candidates = append(candidates, c)
	}
	if err := claimRows.Err(); err != nil {
		claimRows.Close()
		util.WriteError(w, 500, "read stitch tasks")
		return
	}
	claimRows.Close()
	if len(candidates) == 0 {
		util.WriteJSON(w, 200, map[string]any{"task": nil, "reason": "empty"})
		return
	}
	var out nativeStitchClaimResponse
	var raw []byte
	found := false
	for _, candidate := range candidates {
		if err = json.Unmarshal(candidate.raw, &candidate.out.Clips); err != nil {
			util.WriteError(w, 500, "invalid frozen stitch manifest")
			return
		}
		current, currentSHA, _, loadErr := loadNativeStitchManifest(r.Context(), tx, p.AccountID, candidate.out.RecordingID, candidate.out.RecordingJobID, candidate.out.WindowStartAt, candidate.out.WindowEndAt)
		if loadErr != nil || currentSHA != candidate.out.ClipManifestSHA256 || len(current) != len(candidate.out.Clips) {
			_, _ = tx.Exec(r.Context(), `UPDATE recording_native_stitch_tasks SET state='stale',last_reason_code='server_manifest_changed' WHERE id=$1`, candidate.out.TaskID)
			continue
		}
		ready, proofErr := exactStitchNASProof(r.Context(), tx, connectionID, generation, candidate.raw, len(candidate.out.Clips))
		if proofErr != nil {
			util.WriteError(w, 500, "check stitch NAS proof")
			return
		}
		if !ready {
			continue
		}
		out, raw, found = candidate.out, candidate.raw, true
		break
	}
	_ = raw
	if !found {
		if err = tx.Commit(r.Context()); err != nil {
			util.WriteError(w, 409, "stitch eligibility raced; retry")
			return
		}
		util.WriteJSON(w, 200, map[string]any{"task": nil, "reason": "no_task_on_this_connection"})
		return
	}
	out.ClaimToken = uuid.New()
	out.InventoryGeneration = generation
	out.InventoryDigest = digest
	out.InventoryCompletedAt = *completed
	err = tx.QueryRow(r.Context(), `UPDATE recording_native_stitch_tasks SET state='leased',claim_token=$2,claimed_connection_id=$3,lease_expires_at=now()+$4::interval,attempt_count=attempt_count+1,last_reason_code='' WHERE id=$1 AND attempt_count<$5 RETURNING lease_expires_at`, out.TaskID, out.ClaimToken, connectionID, nativeStitchLease.String(), nativeStitchMaxAttempts).Scan(&out.LeaseExpiresAt)
	if err != nil {
		util.WriteError(w, 409, "stitch task attempt limit reached")
		return
	}
	if err = tx.Commit(r.Context()); err != nil {
		util.WriteError(w, 409, "stitch claim raced; retry")
		return
	}
	util.WriteJSON(w, 200, map[string]any{"task": out})
}

func exactStitchNASProof(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, connectionID int64, generation string, raw []byte, want int) (bool, error) {
	var proven int
	err := q.QueryRow(ctx, `
		SELECT count(*) FROM nas_inventory_files i
		JOIN jsonb_to_recordset($3::jsonb) m(clip_id bigint,relative_path text,size_bytes bigint,sha256 text) ON m.clip_id=i.clip_id
		WHERE i.connection_id=$1 AND i.seen_generation=$2 AND i.state='present' AND i.relative_path=m.relative_path AND i.size_bytes=m.size_bytes AND i.sha256=m.sha256
		  AND NOT EXISTS(SELECT 1 FROM nas_inventory_files x WHERE x.connection_id=i.connection_id AND x.relative_path=i.relative_path AND x.clip_id<>i.clip_id AND x.state IN('present','mismatch'))
		  AND NOT EXISTS(SELECT 1 FROM nas_inventory_unmatched_files u WHERE u.connection_id=i.connection_id AND u.relative_path=i.relative_path AND u.state='present')`, connectionID, generation, raw).Scan(&proven)
	return proven == want, err
}

type nativeStitchCompleteRequest struct {
	ClaimToken uuid.UUID       `json:"claim_token"`
	Report     json.RawMessage `json:"report"`
}

func decodeNativeStitchCompletion(w http.ResponseWriter, r *http.Request) (nativeStitchCompleteRequest, bool) {
	r.Body = http.MaxBytesReader(w, r.Body, 32<<20)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var req nativeStitchCompleteRequest
	if err := dec.Decode(&req); err != nil {
		util.WriteError(w, 400, "invalid stitch completion")
		return req, false
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		util.WriteError(w, 400, "body must contain one object")
		return req, false
	}
	return req, true
}

func strictNativeStitchReport(raw json.RawMessage) (stitchcert.Report, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var report stitchcert.Report
	if err := dec.Decode(&report); err != nil {
		return report, err
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		return report, fmt.Errorf("report must contain one object")
	}
	return report, nil
}

func validNativeFileIdentity(v stitchcert.FileIdentity) bool {
	return v.Size > 0 && v.MTimeNS > 0 && v.CTimeNS > 0 && v.Inode > 0 && v.Device > 0
}

// The server independently measures the timeline from frozen microsecond
// timestamps. Clients may use a different JSON/float implementation, so only
// counters are exact; scalar evidence is accepted within one microsecond.
func nativeStitchTimelineMatches(got, want stitchcert.Timeline) bool {
	const epsilon = 0.0000011
	close := func(a, b float64) bool { return math.Abs(a-b) <= epsilon }
	return close(got.ExpectedSeconds, want.ExpectedSeconds) &&
		close(got.CoveredSeconds, want.CoveredSeconds) &&
		close(got.CoveragePct, want.CoveragePct) &&
		close(got.LeadingGapSeconds, want.LeadingGapSeconds) &&
		close(got.LargestInternalGapSecond, want.LargestInternalGapSecond) &&
		close(got.TrailingGapSeconds, want.TrailingGapSeconds) &&
		close(got.LargestGapSeconds, want.LargestGapSeconds) &&
		got.GapCount == want.GapCount && got.GapOver30sCount == want.GapOver30sCount &&
		got.GapOver5mCount == want.GapOver5mCount && got.OverlapCount == want.OverlapCount &&
		close(got.OverlapSeconds, want.OverlapSeconds)
}

func nativeStitchWholeWindowContinuous(t stitchcert.Timeline) bool {
	const epsilon = 0.0000011
	return math.Abs(t.LeadingGapSeconds) <= epsilon && math.Abs(t.LargestInternalGapSecond) <= epsilon &&
		math.Abs(t.TrailingGapSeconds) <= epsilon && t.GapCount == 0 && t.OverlapCount == 0 &&
		math.Abs(t.OverlapSeconds) <= epsilon && math.Abs(t.CoveredSeconds-t.ExpectedSeconds) <= epsilon
}

func (s *Server) handleAccountNativeStitchComplete(w http.ResponseWriter, r *http.Request) {
	p, ok := accountPrincipalFromContext(r.Context())
	if !ok || p.APIKeyID == nil {
		util.WriteError(w, 403, "NAS pull key required")
		return
	}
	req, ok := decodeNativeStitchCompletion(w, r)
	if !ok {
		return
	}
	report, err := strictNativeStitchReport(req.Report)
	if err != nil {
		util.WriteError(w, 400, "invalid stitch report")
		return
	}
	actualSHA, _, err := stitchcert.CanonicalSHA(report)
	if err != nil {
		util.WriteError(w, 400, "invalid stitch report")
		return
	}
	reasons, err := stitchcert.NormalizeReasons(report.ReasonCodes)
	if err != nil {
		util.WriteError(w, 400, "invalid reason codes")
		return
	}
	for _, reason := range reasons {
		if !nativeStitchReason.MatchString(reason) {
			util.WriteError(w, 400, "invalid reason code")
			return
		}
	}
	if report.Status != "passed" && report.Status != "partial" && report.Status != "failed" && report.Status != "unknown" {
		util.WriteError(w, 400, "invalid report status")
		return
	}
	if err = stitchcert.ValidateAxisStatuses(report); err != nil {
		util.WriteError(w, 400, "invalid certification axis statuses")
		return
	}
	if report.SourceMediaModified || report.Reencoded || report.PersistentOutput {
		util.WriteError(w, 409, "certification attempted a forbidden media mutation")
		return
	}
	tx, err := s.pool.BeginTx(r.Context(), pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		util.WriteError(w, 500, "begin completion")
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()
	var connectionID, recordingID, jobID int64
	var start, end time.Time
	var manifestSHA, policy, state string
	var frozenRaw, healthRaw []byte
	var lease *time.Time
	var dbNow time.Time
	if err = tx.QueryRow(r.Context(), `SELECT id FROM connections WHERE api_key_id=$1 AND account_id=$2`, *p.APIKeyID, p.AccountID).Scan(&connectionID); err != nil {
		util.WriteError(w, 403, "no NAS connection for this key")
		return
	}
	err = tx.QueryRow(r.Context(), `SELECT recording_id,recording_job_id,window_start_at,window_end_at,clip_manifest_sha256,policy_version,state,clip_manifest,health_facts,lease_expires_at,now() FROM recording_native_stitch_tasks WHERE account_id=$1 AND id=$2 FOR UPDATE`, p.AccountID, report.TaskID).Scan(&recordingID, &jobID, &start, &end, &manifestSHA, &policy, &state, &frozenRaw, &healthRaw, &lease, &dbNow)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, 409, "stale stitch claim")
		return
	}
	if err != nil {
		util.WriteError(w, 500, "load stitch claim")
		return
	}
	var priorID int64
	var priorStatus, priorSHA string
	priorErr := tx.QueryRow(r.Context(), `SELECT id,status,report_sha256 FROM recording_native_stitch_certifications WHERE task_id=$1 AND claim_token=$2 AND connection_id=$3`, report.TaskID, req.ClaimToken, connectionID).Scan(&priorID, &priorStatus, &priorSHA)
	if priorErr == nil {
		if priorSHA != actualSHA {
			util.WriteError(w, 409, "stitch completion replay differs")
			return
		}
		if err = tx.Commit(r.Context()); err != nil {
			util.WriteError(w, 409, "stitch completion replay raced; retry")
			return
		}
		util.WriteJSON(w, 200, map[string]any{"ok": true, "certification_id": priorID, "status": priorStatus, "replayed": true})
		return
	}
	if !errors.Is(priorErr, pgx.ErrNoRows) {
		util.WriteError(w, 500, "load prior stitch completion")
		return
	}
	var claimedConnectionID *int64
	var claimToken *uuid.UUID
	if err = tx.QueryRow(r.Context(), `SELECT claimed_connection_id,claim_token FROM recording_native_stitch_tasks WHERE id=$1`, report.TaskID).Scan(&claimedConnectionID, &claimToken); err != nil || state != "leased" || lease == nil || lease.Before(dbNow) || claimedConnectionID == nil || *claimedConnectionID != connectionID || claimToken == nil || *claimToken != req.ClaimToken {
		util.WriteError(w, 409, "stale stitch claim")
		return
	}
	if report.RecordingID != recordingID || report.RecordingJobID != jobID || !report.WindowStartAt.Equal(start) || !report.WindowEndAt.Equal(end) || report.ClipManifestSHA256 != manifestSHA || report.PolicyVersion != policy {
		util.WriteError(w, 409, "report does not match frozen task")
		return
	}
	var frozen []stitchcert.ManifestClip
	if err = json.Unmarshal(frozenRaw, &frozen); err != nil {
		util.WriteError(w, 500, "invalid frozen manifest")
		return
	}
	leaseStart := lease.Add(-nativeStitchLease)
	if report.SchemaVersion != 1 || report.StartedAt.Before(leaseStart.Add(-3*time.Minute)) || report.CompletedAt.Before(report.StartedAt) || report.CompletedAt.Sub(report.StartedAt) > 36*time.Minute || report.CompletedAt.After(dbNow.Add(time.Minute)) || len(report.ClientVersion) < 1 || len(report.ClientVersion) > 128 || len(report.FFmpegVersion) < 1 || len(report.FFmpegVersion) > 256 || len(report.FFprobeVersion) < 1 || len(report.FFprobeVersion) > 256 || len(report.InventoryGeneration) < 1 || len(report.InventoryGeneration) > 256 || len(report.InventoryDigest) != 64 || report.SourceMediaModified || report.Reencoded || report.PersistentOutput {
		util.WriteError(w, 409, "report metadata is invalid or outside claim lease")
		return
	}
	if len(report.Clips) > len(frozen) {
		util.WriteError(w, 409, "report contains extra clip evidence")
		return
	}
	for i, c := range report.Clips {
		signatureSHA, signatureJSON, signatureErr := stitchcert.CanonicalSHA(c.NativeSignature)
		if c.ManifestClip != frozen[i] || !nativeStitchHex64(c.SidecarSHA256) || !nativeStitchHex64(c.NativeSignatureSHA256) || signatureErr != nil || len(signatureJSON) > 64<<10 || signatureSHA != c.NativeSignatureSHA256 || !validNativeFileIdentity(c.FileIdentity) || c.FileIdentity.Size != c.SizeBytes || (c.StrictDecode != "passed" && c.StrictDecode != "failed" && c.StrictDecode != "unknown") {
			util.WriteError(w, 409, "clip evidence differs from frozen manifest")
			return
		}
		if stitchcert.HasCompleteTimestampProvenance(frozen[i]) {
			if c.RecomputedTimestampContract == nil {
				util.WriteError(w, 409, "complete timestamp provenance was not recomputed from NAS bytes")
				return
			}
			contractSHA, contractJSON, contractErr := stitchcert.CanonicalSHA(c.RecomputedTimestampContract)
			if contractErr != nil || len(contractJSON) > 16<<20 || contractSHA != frozen[i].TimestampContractSHA256 {
				util.WriteError(w, 409, "recomputed timestamp contract differs from frozen ingest evidence")
				return
			}
		} else if c.RecomputedTimestampContract != nil {
			util.WriteError(w, 409, "unsupported timestamp provenance cannot gain a client-authored contract")
			return
		}
	}
	measuredTimeline := stitchcert.MeasureTimeline(frozen, start, end)
	if !nativeStitchTimelineMatches(report.Timeline, measuredTimeline) {
		util.WriteError(w, 409, "timeline evidence mismatch")
		return
	}
	if report.WindowContinuityStatus == "passed" && !nativeStitchWholeWindowContinuous(measuredTimeline) {
		util.WriteError(w, 409, "whole-window continuity requires the full scheduled envelope")
		return
	}
	revalidateExactMedia := false
	if report.Status == "passed" || report.Status == "partial" {
		if len(report.Clips) != len(frozen) || report.NASByteDecodeStatus != "passed" || report.NativeRunConcatStatus != "passed" {
			util.WriteError(w, 409, "passing report is incomplete or unsafe")
			return
		}
		if err = stitchcert.ValidateRuns(report.Clips, report.NativeRuns); err != nil {
			util.WriteError(w, 409, "invalid native run evidence")
			return
		}
		if err = stitchcert.ValidateClipTimelines(report.Clips, report.WithinRunFrameAdjacencyStatus == "passed", report.WithinRunAudioContinuityStatus == "passed"); err != nil {
			util.WriteError(w, 409, "invalid per-clip presentation evidence")
			return
		}
		if report.WithinRunFrameAdjacencyStatus == "passed" {
			err = stitchcert.ValidateSeams(report.Clips, report.NativeRuns, report.Seams)
		} else {
			err = stitchcert.ValidatePartialSeams(report.Clips, report.NativeRuns, report.Seams)
		}
		if err != nil {
			util.WriteError(w, 409, "invalid frame-seam evidence")
			return
		}
		if err = stitchcert.ValidateAudioSeams(report.Clips, report.AudioSeams, report.WithinRunAudioContinuityStatus == "passed"); err != nil {
			util.WriteError(w, 409, "invalid audio-seam evidence")
			return
		}
		if err = stitchcert.ValidateAudioAxisStatus(report.Clips, report.WithinRunAudioContinuityStatus); err != nil {
			util.WriteError(w, 409, "audio continuity axis differs from frozen clip evidence")
			return
		}
		measured := measuredTimeline
		var health struct {
			ClipCount         int     `json:"clip_count"`
			ExpectedSeconds   int64   `json:"expected_seconds"`
			CoveredSeconds    float64 `json:"covered_seconds"`
			CoveragePct       float64 `json:"coverage_pct"`
			LargestGapSeconds float64 `json:"largest_gap_seconds"`
			GapOver30sCount   int     `json:"gap_over_30s_count"`
			GapOver5mCount    int     `json:"gap_over_5m_count"`
			OverlapCount      int     `json:"overlap_count"`
			OverlapSeconds    float64 `json:"overlap_seconds"`
		}
		if err = json.Unmarshal(healthRaw, &health); err != nil || health.ClipCount != len(frozen) || health.ExpectedSeconds != int64(measured.ExpectedSeconds) || math.Abs(health.CoveredSeconds-measured.CoveredSeconds) > 1 || math.Abs(health.CoveragePct-measured.CoveragePct) > .02 || math.Abs(health.LargestGapSeconds-measured.LargestGapSeconds) > 1 || health.GapOver30sCount != measured.GapOver30sCount || health.GapOver5mCount != measured.GapOver5mCount || health.OverlapCount != measured.OverlapCount || math.Abs(health.OverlapSeconds-measured.OverlapSeconds) > 1 {
			util.WriteError(w, 409, "timeline differs from frozen metric-v2 health")
			return
		}
		revalidateExactMedia = true
	} else {
		if err = stitchcert.ValidatePartialRuns(report.Clips, report.NativeRuns); err != nil {
			util.WriteError(w, 409, "invalid partial native run evidence")
			return
		}
		// A terminal media failure may happen before seam extraction, and a
		// transient UNKNOWN deliberately publishes no partial axes. Seam facts
		// are mandatory for completed PASS/PARTIAL certification, not fabricated
		// after a failed decode/concat or interrupted attempt.
		if len(report.Seams) != 0 || len(report.AudioSeams) != 0 {
			if len(report.Clips) != len(frozen) {
				util.WriteError(w, 409, "partial seam evidence lacks the full frozen clip set")
				return
			}
			if err = stitchcert.ValidatePartialSeams(report.Clips, report.NativeRuns, report.Seams); err != nil {
				util.WriteError(w, 409, "invalid partial frame-seam evidence")
				return
			}
			if err = stitchcert.ValidateAudioSeams(report.Clips, report.AudioSeams, false); err != nil {
				util.WriteError(w, 409, "invalid partial audio-seam evidence")
				return
			}
		}
		if report.Status == "failed" {
			if err = stitchcert.ValidateDeterministicFailureEvidence(report, reasons); err != nil {
				util.WriteError(w, 409, "FAILED requires deterministic media evidence")
				return
			}
		}
		if report.Status == "unknown" {
			if err = stitchcert.ValidateUnknownEvidenceEmpty(report); err != nil {
				util.WriteError(w, 409, "UNKNOWN must not contain media facts")
				return
			}
			if nativeStitchDeterministicFailures[reasons[0]] || report.NASByteDecodeStatus == "failed" || report.NativeRunConcatStatus == "failed" || report.WithinRunFrameAdjacencyStatus == "failed" || report.WithinRunAudioContinuityStatus == "failed" {
				util.WriteError(w, 409, "UNKNOWN cannot contain deterministic failure")
				return
			}
		}
	}
	if revalidateExactMedia {
		current, currentSHA, _, e := loadNativeStitchManifest(r.Context(), tx, p.AccountID, recordingID, jobID, start, end)
		if e != nil || currentSHA != manifestSHA || len(current) != len(frozen) {
			util.WriteError(w, 409, "server manifest changed")
			return
		}
		var currentGeneration, currentDigest string
		var currentCompleted *time.Time
		var skipped int64
		if e = tx.QueryRow(r.Context(), `SELECT inventory_generation,inventory_digest,inventory_scan_completed_at,inventory_scan_rows_skipped FROM connections WHERE id=$1`, connectionID).Scan(&currentGeneration, &currentDigest, &currentCompleted, &skipped); e != nil || currentCompleted == nil || skipped != 0 || currentGeneration != report.InventoryGeneration || currentDigest != report.InventoryDigest || !currentCompleted.Equal(report.InventoryCompletedAt) {
			util.WriteError(w, 409, "inventory generation changed")
			return
		}
		ready, e := exactStitchNASProof(r.Context(), tx, connectionID, currentGeneration, frozenRaw, len(frozen))
		if e != nil || !ready {
			util.WriteError(w, 409, "exact NAS manifest proof changed")
			return
		}
	}
	var certID int64
	err = tx.QueryRow(r.Context(), `INSERT INTO recording_native_stitch_certifications(task_id,claim_token,connection_id,status,nas_byte_decode_status,native_run_concat_status,within_run_frame_adjacency_status,within_run_audio_sample_continuity_status,window_continuity_status,run_count,seam_count,audio_seam_count,inventory_generation,inventory_digest,inventory_completed_at,report,report_sha256,policy_version,client_version,ffmpeg_version,ffprobe_version,reason_codes,started_at,completed_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24) RETURNING id`, report.TaskID, req.ClaimToken, connectionID, report.Status, report.NASByteDecodeStatus, report.NativeRunConcatStatus, report.WithinRunFrameAdjacencyStatus, report.WithinRunAudioContinuityStatus, report.WindowContinuityStatus, len(report.NativeRuns), len(report.Seams), len(report.AudioSeams), report.InventoryGeneration, report.InventoryDigest, report.InventoryCompletedAt, req.Report, actualSHA, policy, report.ClientVersion, report.FFmpegVersion, report.FFprobeVersion, reasons, report.StartedAt, report.CompletedAt).Scan(&certID)
	if err != nil {
		util.WriteError(w, 409, "store stitch certification")
		return
	}
	for _, c := range report.Clips {
		_, err = tx.Exec(r.Context(), `INSERT INTO recording_native_stitch_certification_clips(certification_id,ordinal,clip_id,recording_job_id,relative_path,size_bytes,sha256,sidecar_sha256,clip_start_at,clip_end_at,capture_generation,capture_sequence,capture_attempt_id,timestamp_contract_version,timestamp_contract_status,timestamp_contract_reason,timestamp_contract_sha256,recomputed_timestamp_contract,file_identity,native_signature,native_signature_sha256,strict_decode_status,video_timeline,audio_present,audio_timeline) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25)`, certID, c.Ordinal, c.ClipID, c.RecordingJobID, c.RelativePath, c.SizeBytes, c.SHA256, c.SidecarSHA256, c.ClipStartAt, c.ClipEndAt, c.CaptureGeneration, c.CaptureSequence, nilIfEmpty(c.CaptureAttemptID), nilIfEmpty(c.TimestampContractVersion), nilIfEmpty(c.TimestampContractStatus), nilIfEmpty(c.TimestampContractReason), nilIfEmpty(c.TimestampContractSHA256), c.RecomputedTimestampContract, c.FileIdentity, c.NativeSignature, c.NativeSignatureSHA256, c.StrictDecode, c.VideoTimeline, c.AudioPresent, c.AudioTimeline)
		if err != nil {
			util.WriteError(w, 409, "store stitch clip evidence")
			return
		}
	}
	for _, run := range report.NativeRuns {
		_, err = tx.Exec(r.Context(), `INSERT INTO recording_native_stitch_certification_runs(certification_id,ordinal,first_clip_ordinal,last_clip_ordinal,clip_count,source_bytes,native_signature_sha256,capture_generation,capture_attempt_id,timestamp_contract_version,boundary_reason,validation_status) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`, certID, run.Ordinal, run.FirstClipOrdinal, run.LastClipOrdinal, run.ClipCount, run.SourceBytes, run.NativeSignatureSHA256, run.CaptureGeneration, nilIfEmpty(run.CaptureAttemptID), nilIfEmpty(run.TimestampContract), run.BoundaryReason, run.ValidationStatus)
		if err != nil {
			util.WriteError(w, 409, "store stitch run evidence")
			return
		}
	}
	for _, seam := range report.Seams {
		_, err = tx.Exec(r.Context(), `INSERT INTO recording_native_stitch_certification_seams(certification_id,ordinal,previous_clip_id,next_clip_id,evidence,verdict,reason) VALUES($1,$2,$3,$4,$5,$6,$7)`, certID, seam.Ordinal, seam.PreviousClipID, seam.NextClipID, seam, seam.Verdict, seam.Reason)
		if err != nil {
			util.WriteError(w, 409, "store stitch seam evidence")
			return
		}
	}
	for _, seam := range report.AudioSeams {
		_, err = tx.Exec(r.Context(), `INSERT INTO recording_native_stitch_certification_audio_seams(certification_id,ordinal,previous_clip_id,next_clip_id,evidence,verdict,reason) VALUES($1,$2,$3,$4,$5,$6,$7)`, certID, seam.Ordinal, seam.PreviousClipID, seam.NextClipID, seam, seam.Verdict, seam.Reason)
		if err != nil {
			util.WriteError(w, 409, "store stitch audio seam evidence")
			return
		}
	}
	if report.Status == "unknown" {
		var attempts int
		_ = tx.QueryRow(r.Context(), `SELECT attempt_count FROM recording_native_stitch_tasks WHERE id=$1`, report.TaskID).Scan(&attempts)
		backoff := time.Duration(1<<nativeStitchMin(attempts, 5)) * time.Minute
		_, err = tx.Exec(r.Context(), `UPDATE recording_native_stitch_tasks SET state='pending',claim_token=NULL,claimed_connection_id=NULL,lease_expires_at=NULL,next_attempt_at=now()+$2::interval,last_reason_code=$3 WHERE id=$1`, report.TaskID, backoff.String(), reasons[0])
	} else {
		_, err = tx.Exec(r.Context(), `UPDATE recording_native_stitch_tasks SET state=$2,claim_token=NULL,claimed_connection_id=NULL,lease_expires_at=NULL,last_reason_code=$3 WHERE id=$1`, report.TaskID, report.Status, reasons[0])
	}
	if err != nil {
		util.WriteError(w, 500, "finish stitch task")
		return
	}
	if err = tx.Commit(r.Context()); err != nil {
		util.WriteError(w, 409, "stitch completion raced; retry")
		return
	}
	util.WriteJSON(w, 200, map[string]any{"ok": true, "certification_id": certID, "status": report.Status})
}

func nativeStitchHex64(v string) bool {
	if len(v) != 64 {
		return false
	}
	for _, r := range v {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return false
		}
	}
	return true
}

func nilIfEmpty(v string) any {
	if v == "" {
		return nil
	}
	return v
}

func nativeStitchMin(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func nativeStitchQualificationEligible(scope, taskState string, certificationStatus *string, currentNAS string) bool {
	return scope == "authoritative_occurrence" && taskState == "passed" && certificationStatus != nil &&
		*certificationStatus == "passed" && currentNAS == "current"
}

func (s *Server) handleAccountNativeStitchGet(w http.ResponseWriter, r *http.Request) {
	p, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, 401, "unauthorized")
		return
	}
	id, parseErr := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if parseErr != nil || id <= 0 {
		util.WriteError(w, 400, "recording_id required")
		return
	}
	rows, err := s.pool.Query(r.Context(), `SELECT l.recording_job_id,l.window_start_at,l.window_end_at,l.qualification_scope,l.clip_manifest_sha256,l.clip_count,l.source_bytes,l.state,l.certification_id,l.status,l.nas_byte_decode_status,l.native_run_concat_status,l.within_run_frame_adjacency_status,l.within_run_audio_sample_continuity_status,l.window_continuity_status,l.run_count,l.seam_count,l.audio_seam_count,l.inventory_generation,l.inventory_digest,l.inventory_completed_at,l.report_sha256,l.reason_codes,l.completed_at, CASE WHEN l.certification_id IS NULL THEN 'unknown' WHEN c.id IS NULL OR c.inventory_scan_rows_skipped<>0 OR c.inventory_in_progress_generation<>'' OR c.inventory_generation<>l.inventory_generation THEN 'unknown' WHEN (SELECT count(*) FROM recording_native_stitch_certification_clips f JOIN nas_inventory_files i ON i.connection_id=c.id AND i.clip_id=f.clip_id AND i.seen_generation=c.inventory_generation AND i.state='present' AND i.relative_path=f.relative_path AND i.size_bytes=f.size_bytes AND i.sha256=f.sha256 WHERE f.certification_id=l.certification_id AND NOT EXISTS(SELECT 1 FROM nas_inventory_files x WHERE x.connection_id=i.connection_id AND x.relative_path=i.relative_path AND x.clip_id<>i.clip_id AND x.state IN('present','mismatch')) AND NOT EXISTS(SELECT 1 FROM nas_inventory_unmatched_files u WHERE u.connection_id=i.connection_id AND u.relative_path=i.relative_path AND u.state='present'))=l.clip_count THEN 'current' ELSE 'unknown' END current_nas_presence FROM recording_native_stitch_facts l LEFT JOIN connections c ON c.id=(SELECT connection_id FROM recording_native_stitch_certifications WHERE id=l.certification_id) WHERE l.account_id=$1 AND l.recording_id=$2 AND l.policy_version=$3 ORDER BY l.window_end_at DESC,l.completed_at DESC NULLS LAST`, p.AccountID, id, stitchcert.PolicyVersion)
	if err != nil {
		util.WriteError(w, 500, "read stitch certifications")
		return
	}
	defer rows.Close()
	items := []map[string]any{}
	for rows.Next() {
		var job, clips, bytes int64
		var start, end time.Time
		var qualificationScope, manifest, state, current string
		var cert *int64
		var status, decode, concat, withinVideo, withinAudio, windowContinuity *string
		var runCount, seamCount, audioSeamCount *int
		var generation, digest, reportSHA *string
		var invAt, doneAt *time.Time
		var reasons []string
		if err := rows.Scan(&job, &start, &end, &qualificationScope, &manifest, &clips, &bytes, &state, &cert, &status, &decode, &concat, &withinVideo, &withinAudio, &windowContinuity, &runCount, &seamCount, &audioSeamCount, &generation, &digest, &invAt, &reportSHA, &reasons, &doneAt, &current); err != nil {
			util.WriteError(w, 500, "scan stitch certifications")
			return
		}
		items = append(items, map[string]any{"recording_job_id": job, "qualification_scope": qualificationScope, "qualification_eligible": nativeStitchQualificationEligible(qualificationScope, state, status, current), "window_start_at": start, "window_end_at": end, "clip_manifest_sha256": manifest, "clip_count": clips, "source_bytes": bytes, "task_state": state, "certification_id": cert, "certification_status": status, "media_byte_decode": decode, "native_run_concat": concat, "within_run_frame_adjacency": withinVideo, "within_run_audio_sample_continuity": withinAudio, "whole_window_continuity": windowContinuity, "native_run_count": runCount, "video_seam_fact_count": seamCount, "audio_seam_fact_count": audioSeamCount, "current_nas_presence": current, "inventory_generation": generation, "inventory_digest": digest, "inventory_completed_at": invAt, "report_sha256": reportSHA, "reason_codes": reasons, "completed_at": doneAt})
	}
	if err := rows.Err(); err != nil {
		util.WriteError(w, 500, "read stitch certifications")
		return
	}
	util.WriteJSON(w, 200, map[string]any{"scope": "exact native-run media evidence; within-run frame/audio adjacency, whole-window continuity, timeline quality, and current NAS presence are separate claims", "items": items})
}
