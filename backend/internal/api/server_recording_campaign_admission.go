package api

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"image"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/daydemir/stoarama/backend/internal/capture"
	"github.com/daydemir/stoarama/backend/internal/recordability"
	"github.com/daydemir/stoarama/backend/internal/util"
)

type campaignAdmissionApprovalEntry struct {
	StreamID             int64  `json:"stream_id"`
	RecordingID          int64  `json:"recording_id"`
	SourceRevisionID     int64  `json:"source_revision_id"`
	SourceURL            string `json:"source_url"`
	SourcePageURL        string `json:"source_page_url"`
	Provider             string `json:"provider"`
	ExternalID           string `json:"external_id"`
	NormalizedLabel      string `json:"normalized_label"`
	SceneFrameEvidenceID int64  `json:"scene_frame_evidence_id"`
	SceneIdentitySHA256  string `json:"scene_identity_sha256"`
}

type campaignAdmissionStoredEntry struct {
	StreamID               int64  `json:"stream_id"`
	RecordingID            int64  `json:"recording_id"`
	SourceRevisionID       int64  `json:"source_revision_id"`
	SourceURLSHA256        string `json:"source_url_sha256"`
	SourcePageURLSHA256    string `json:"source_page_url_sha256"`
	SourceUpdatedAtUnixMic int64  `json:"source_updated_at_unix_micros"`
	Provider               string `json:"provider"`
	ExternalID             string `json:"external_id"`
	NormalizedLabel        string `json:"normalized_label"`
	SceneFrameEvidenceID   int64  `json:"scene_frame_evidence_id"`
	SceneIdentitySHA256    string `json:"scene_identity_sha256"`
}

func bindCampaignAdmissionEvidence(ctx context.Context, tx pgx.Tx, approvalID uuid.UUID, accountID int64, actorUserID int64, streams []batchStream, endAt *time.Time, currentBuildSHA string, scheduleSpec []byte, lock bool) (time.Time, error) {
	lockSQL := ""
	if lock {
		lockSQL = " FOR UPDATE OF a"
	}
	var deadline time.Time
	var approvalActor int64
	var scheduleSHA string
	err := tx.QueryRow(ctx, `SELECT deadline_at,actor_user_id,schedule_sha256 FROM recording_campaign_admission_approvals a WHERE id=$1 AND account_id=$2 AND schedule_sha256=encode(sha256(convert_to($3::jsonb::text,'UTF8')),'hex')`+lockSQL, approvalID, accountID, scheduleSpec).Scan(&deadline, &approvalActor, &scheduleSHA)
	if err != nil {
		return time.Time{}, fmt.Errorf("load exact campaign approval: %w", err)
	}
	now := time.Now().UTC()
	if !deadline.After(now) || endAt == nil || endAt.After(deadline) {
		return time.Time{}, fmt.Errorf("campaign admission schedule requires end_at no later than the live approval deadline")
	}
	if approvalActor <= 0 || actorUserID <= 0 {
		return time.Time{}, fmt.Errorf("campaign admission actor binding is missing")
	}
	rows, err := tx.Query(ctx, `
		SELECT first_e.id::text,second_e.id::text,ar.stream_id,ar.scene_identity_sha256,second_e.media_sha256,second_e.observed_at
		FROM recording_campaign_admission_approvals a
		JOIN recording_campaign_admission_reservations ar ON ar.approval_id=a.id
		JOIN streams s ON s.id=ar.stream_id AND s.deleted_at IS NULL
		JOIN LATERAL (SELECT e.*,pa.probe_build_sha,pa.source_revision_id,pa.source_url_sha256,pa.source_page_url_sha256,pa.source_updated_at,pa.challenge,pa.attempt_no FROM recording_targeted_probe_attempts pa LEFT JOIN recording_targeted_probe_evidence e ON e.attempt_id=pa.id WHERE pa.approval_id=a.id AND pa.stream_id=ar.stream_id ORDER BY pa.attempt_no DESC LIMIT 1 OFFSET 1) first_e ON true
		JOIN LATERAL (SELECT e.*,pa.probe_build_sha,pa.source_revision_id,pa.source_url_sha256,pa.source_page_url_sha256,pa.source_updated_at,pa.challenge,pa.attempt_no FROM recording_targeted_probe_attempts pa LEFT JOIN recording_targeted_probe_evidence e ON e.attempt_id=pa.id WHERE pa.approval_id=a.id AND pa.stream_id=ar.stream_id ORDER BY pa.attempt_no DESC LIMIT 1) second_e ON true
		JOIN recording_targeted_probe_scene_reviews first_review ON first_review.probe_evidence_id=first_e.id
		JOIN recording_targeted_probe_scene_reviews second_review ON second_review.probe_evidence_id=second_e.id
		WHERE a.id=$1 AND a.account_id=$2
		  AND (a.failure_domain_tag IS NULL OR a.failure_domain_tag=ANY(s.tags))
		  AND first_e.probe_build_sha=$3 AND second_e.probe_build_sha=$3
		  AND first_e.result='ok' AND second_e.result='ok'
		  AND second_e.attempt_no=first_e.attempt_no+1
		  AND second_e.observed_at>=first_e.observed_at+interval '60 seconds'
		  AND first_e.observed_at>=transaction_timestamp()-interval '6 hours'
		  AND second_e.observed_at>=transaction_timestamp()-interval '6 hours'
		  AND first_e.challenge<>second_e.challenge
		  AND first_e.frame_sha256<>second_e.frame_sha256
		  AND first_e.media_sha256<>second_e.media_sha256
		  AND first_review.approval_id=a.id AND second_review.approval_id=a.id
		  AND first_review.scene_frame_evidence_id=ar.scene_frame_evidence_id AND second_review.scene_frame_evidence_id=ar.scene_frame_evidence_id
		  AND first_review.scene_identity_sha256=ar.scene_identity_sha256 AND second_review.scene_identity_sha256=ar.scene_identity_sha256
		  AND first_review.probe_frame_sha256=first_e.frame_sha256 AND second_review.probe_frame_sha256=second_e.frame_sha256
		  AND first_review.reviewed_at>=transaction_timestamp()-interval '6 hours' AND second_review.reviewed_at>=transaction_timestamp()-interval '6 hours'
		  AND (first_e.native_signature_sha256,first_e.source_revision_id,first_e.source_url_sha256,first_e.source_page_url_sha256,first_e.source_updated_at) IS NOT DISTINCT FROM
		      (second_e.native_signature_sha256,second_e.source_revision_id,second_e.source_url_sha256,second_e.source_page_url_sha256,second_e.source_updated_at)
		  AND ar.source_revision_id IS NOT DISTINCT FROM second_e.source_revision_id
		  AND ar.source_updated_at=second_e.source_updated_at
		  AND ar.source_url_sha256=encode(sha256(convert_to(s.source_url,'UTF8')),'hex')
		  AND ar.source_page_url_sha256=encode(sha256(convert_to(COALESCE(s.source_page_url,''),'UTF8')),'hex')
		  AND NOT EXISTS(SELECT 1 FROM recording_campaign_admission_source_fence_events f WHERE f.stream_id=ar.stream_id AND f.occurred_at>=ar.reserved_at)
		ORDER BY ar.stream_id
	`, approvalID, accountID, strings.ToLower(strings.TrimSpace(currentBuildSHA)))
	if err != nil {
		return time.Time{}, fmt.Errorf("load targeted campaign evidence: %w", err)
	}
	defer rows.Close()
	type evidenceBinding struct {
		first, second, scene, media string
		observed                    time.Time
	}
	byStream := make(map[int64]evidenceBinding, len(streams))
	for rows.Next() {
		var firstID, secondID, scene, mediaSHA string
		var streamID int64
		var observed time.Time
		if err := rows.Scan(&firstID, &secondID, &streamID, &scene, &mediaSHA, &observed); err != nil {
			return time.Time{}, fmt.Errorf("scan targeted campaign evidence: %w", err)
		}
		byStream[streamID] = evidenceBinding{first: firstID, second: secondID, scene: scene, media: mediaSHA, observed: observed.UTC()}
	}
	if err := rows.Err(); err != nil {
		return time.Time{}, fmt.Errorf("iterate targeted campaign evidence: %w", err)
	}
	if len(byStream) != len(streams) {
		return time.Time{}, fmt.Errorf("every approved stream requires fresh complete current-build DO evidence")
	}
	for i := range streams {
		evidence, ok := byStream[streams[i].id]
		if !ok {
			return time.Time{}, fmt.Errorf("stream %d is not exactly approved with fresh evidence", streams[i].id)
		}
		if batchCaptureVia(streams[i].sourceURL, streams[i].provider, streams[i].captureVia) != "cloud" {
			return time.Time{}, fmt.Errorf("stream %d is not cloud-native and cannot use targeted DO admission", streams[i].id)
		}
		streams[i].admissionEvidenceID, streams[i].admissionEvidenceID2 = evidence.first, evidence.second
		streams[i].sceneIdentity, streams[i].evidenceSHA, streams[i].evidenceObservedAt, streams[i].scheduleSHA = evidence.scene, evidence.media, evidence.observed, scheduleSHA
	}
	return deadline.UTC(), nil
}

type campaignAdmissionApprovalRequest struct {
	RequestID        string                           `json:"request_id"`
	DeadlineAt       time.Time                        `json:"deadline_at"`
	AuthorityCode    string                           `json:"authority_code"`
	FailureDomainTag string                           `json:"failure_domain_tag"`
	Entries          []campaignAdmissionApprovalEntry `json:"entries"`
	Schedule         batchScheduleRequest             `json:"schedule"`
}

func normalizeCampaignAdmissionEntries(entries []campaignAdmissionApprovalEntry) ([]campaignAdmissionApprovalEntry, error) {
	if len(entries) < 1 || len(entries) > 32 {
		return nil, fmt.Errorf("entries must contain between 1 and 32 streams")
	}
	out := append([]campaignAdmissionApprovalEntry(nil), entries...)
	sort.Slice(out, func(i, j int) bool { return out[i].StreamID < out[j].StreamID })
	seenStreams, seenScenes := map[int64]bool{}, map[string]bool{}
	for i := range out {
		out[i].SourceURL = strings.TrimSpace(out[i].SourceURL)
		out[i].SourcePageURL = strings.TrimSpace(out[i].SourcePageURL)
		out[i].Provider = strings.TrimSpace(out[i].Provider)
		out[i].ExternalID = strings.TrimSpace(out[i].ExternalID)
		out[i].NormalizedLabel = normalizeCampaignAdmissionLabel(out[i].NormalizedLabel, out[i].StreamID)
		out[i].SceneIdentitySHA256 = strings.ToLower(strings.TrimSpace(out[i].SceneIdentitySHA256))
		if out[i].StreamID <= 0 || out[i].RecordingID < 0 || out[i].SourceRevisionID < 0 || out[i].SourceURL == "" || out[i].SourcePageURL == "" || out[i].NormalizedLabel == "" || out[i].SceneFrameEvidenceID <= 0 || !lowerSHA256(out[i].SceneIdentitySHA256) {
			return nil, fmt.Errorf("entry %d has invalid exact source or scene identity", i)
		}
		if seenStreams[out[i].StreamID] || seenScenes[out[i].SceneIdentitySHA256] {
			return nil, fmt.Errorf("entries must have unique streams and scene identities")
		}
		seenStreams[out[i].StreamID], seenScenes[out[i].SceneIdentitySHA256] = true, true
	}
	return out, nil
}

func normalizeCampaignAdmissionLabel(v string, streamID int64) string {
	var b strings.Builder
	for _, r := range strings.ToLower(strings.TrimSpace(v)) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		}
	}
	if b.Len() == 0 && streamID > 0 {
		return fmt.Sprintf("stream%d", streamID)
	}
	return b.String()
}

func campaignAdmissionStreamIDs(entries []campaignAdmissionApprovalEntry) []int64 {
	ids := make([]int64, len(entries))
	for i := range entries {
		ids[i] = entries[i].StreamID
	}
	return ids
}

func lowerSHA256(v string) bool {
	if len(v) != 64 {
		return false
	}
	for _, r := range v {
		if !(r >= '0' && r <= '9') && !(r >= 'a' && r <= 'f') {
			return false
		}
	}
	return true
}

func (s *Server) handleAccountCampaignAdmissionApprovalCreate(w http.ResponseWriter, r *http.Request) {
	p, ok := accountPrincipalFromContext(r.Context())
	if !ok || p.UserID == 0 || p.SessionID == nil || p.Role != accountRoleAdmin || !strings.EqualFold(strings.TrimSpace(p.Email), strings.TrimSpace(s.cfg.DropletPoolOperatorEmail)) {
		util.WriteError(w, http.StatusForbidden, "campaign admission approval requires Deniz's exact operator browser session")
		return
	}
	var req campaignAdmissionApprovalRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	entries, err := normalizeCampaignAdmissionEntries(req.Entries)
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	req.AuthorityCode = strings.TrimSpace(req.AuthorityCode)
	req.FailureDomainTag = strings.TrimSpace(req.FailureDomainTag)
	if (req.AuthorityCode != "deniz_fd_restore_20260814" || req.FailureDomainTag != "FD") &&
		(req.AuthorityCode != "deniz_scene_approval_20260814" || req.FailureDomainTag != "") {
		util.WriteError(w, http.StatusBadRequest, "authority_code/failure_domain_tag does not match an authorized Deniz decision")
		return
	}
	requestID, err := uuid.Parse(strings.TrimSpace(req.RequestID))
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, "request_id must be a UUID")
		return
	}
	scheduleIDs, err := uniqueBatchStreamIDs(req.Schedule.StreamIDs)
	if err != nil || len(scheduleIDs) != len(entries) {
		util.WriteError(w, http.StatusBadRequest, "schedule stream set must exactly match approval entries")
		return
	}
	for i := range entries {
		if scheduleIDs[i] != entries[i].StreamID {
			util.WriteError(w, http.StatusBadRequest, "schedule stream set must exactly match approval entries")
			return
		}
	}
	req.Schedule.StreamIDs = scheduleIDs
	sort.Slice(req.Schedule.StreamTimezones, func(i, j int) bool {
		return req.Schedule.StreamTimezones[i].StreamID < req.Schedule.StreamTimezones[j].StreamID
	})
	if len(req.Schedule.StreamTimezones) != len(entries) {
		util.WriteError(w, http.StatusBadRequest, "approved schedule requires one explicit timezone per stream")
		return
	}
	for i := range entries {
		if req.Schedule.StreamTimezones[i].StreamID != entries[i].StreamID || strings.TrimSpace(req.Schedule.StreamTimezones[i].Timezone) == "" {
			util.WriteError(w, http.StatusBadRequest, "approved schedule requires one explicit timezone per stream")
			return
		}
		req.Schedule.StreamTimezones[i].Timezone = strings.TrimSpace(req.Schedule.StreamTimezones[i].Timezone)
	}
	sort.Ints(req.Schedule.ActiveWeekdays)
	if req.Schedule.NamingProfile != "stoarama_v1" || req.Schedule.TargetFPS != nil || req.Schedule.DryRun || strings.TrimSpace(req.Schedule.CampaignAdmissionApprovalID) != "" {
		util.WriteError(w, http.StatusBadRequest, "approved schedule must be native, non-dry-run, and not self-referential")
		return
	}
	if req.Schedule.Delivery != string(deliveryManaged) && req.Schedule.Delivery != string(deliveryNASPull) {
		util.WriteError(w, http.StatusBadRequest, "campaign admission supports only managed or nas_pull delivery")
		return
	}
	if req.Schedule.StorageDestinationID <= 0 || req.Schedule.DeliveryStorageDestinationID != 0 {
		util.WriteError(w, http.StatusBadRequest, "campaign admission requires one server-owned managed capture destination")
		return
	}
	if req.Schedule.StartAt == nil || req.Schedule.EndAt == nil || !req.Schedule.EndAt.After(req.Schedule.StartAt.UTC()) || req.Schedule.EndAt.After(req.DeadlineAt.UTC()) || len(req.Schedule.ActiveWeekdays) == 0 {
		util.WriteError(w, http.StatusBadRequest, "approved schedule requires explicit bounded start/end/weekdays within the approval deadline")
		return
	}
	req.Schedule.TargetAccountID = p.AccountID
	canonicalRequest, err := json.Marshal(struct {
		AuthorityCode    string                           `json:"authority_code"`
		FailureDomainTag string                           `json:"failure_domain_tag"`
		DeadlineAt       time.Time                        `json:"deadline_at"`
		Entries          []campaignAdmissionApprovalEntry `json:"entries"`
		Schedule         batchScheduleRequest             `json:"schedule"`
	}{req.AuthorityCode, req.FailureDomainTag, req.DeadlineAt.UTC(), entries, req.Schedule})
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "encode canonical campaign approval request")
		return
	}
	requestSHA := hashSecret(string(canonicalRequest))
	var replayID, replayDigest string
	var replayEntriesJSON []byte
	replayErr := s.pool.QueryRow(r.Context(), `SELECT id::text,approval_sha256,entries FROM recording_campaign_admission_approvals WHERE account_id=$1 AND request_id=$2 AND request_sha256=$3`, p.AccountID, requestID, requestSHA).Scan(&replayID, &replayDigest, &replayEntriesJSON)
	if replayErr == nil {
		var replayEntries []campaignAdmissionStoredEntry
		if err := json.Unmarshal(replayEntriesJSON, &replayEntries); err != nil {
			util.WriteError(w, http.StatusInternalServerError, "decode campaign approval replay")
			return
		}
		replayIDs := make([]int64, len(replayEntries))
		for i := range replayEntries {
			replayIDs[i] = replayEntries[i].StreamID
		}
		util.WriteJSON(w, http.StatusCreated, map[string]any{"approval_id": replayID, "approval_sha256": replayDigest, "stream_ids": replayIDs})
		return
	}
	if !errors.Is(replayErr, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusInternalServerError, "load campaign approval replay")
		return
	}
	var requestIDCollision bool
	if err := s.pool.QueryRow(r.Context(), `SELECT EXISTS(SELECT 1 FROM recording_campaign_admission_approvals WHERE account_id=$1 AND request_id=$2)`, p.AccountID, requestID).Scan(&requestIDCollision); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "check campaign approval replay")
		return
	}
	if requestIDCollision {
		util.WriteError(w, http.StatusConflict, "campaign approval request_id already has a different canonical request")
		return
	}
	now := time.Now().UTC()
	if req.DeadlineAt.IsZero() || !req.DeadlineAt.After(now) || req.DeadlineAt.After(now.Add(45*24*time.Hour)) {
		util.WriteError(w, http.StatusBadRequest, "deadline_at must be within the next 45 days")
		return
	}
	tx, err := s.pool.BeginTx(r.Context(), pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "begin campaign admission approval")
		return
	}
	defer tx.Rollback(r.Context())
	if _, err := tx.Exec(r.Context(), `SELECT 1 FROM accounts WHERE id=$1 FOR UPDATE`, p.AccountID); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "lock campaign admission occupancy")
		return
	}
	storedEntries := make([]campaignAdmissionStoredEntry, 0, len(entries))
	for entryIndex, entry := range entries {
		var sourceURL, sourcePageURL, provider, externalID, streamName, streamTimezone, existingCaptureVia string
		var sourceRevisionID int64
		var sourceUpdatedAt time.Time
		err = tx.QueryRow(r.Context(), `SELECT source_url,source_page_url,COALESCE((SELECT max(id) FROM stream_source_revisions WHERE stream_id=streams.id),0),COALESCE(provider,''),COALESCE(external_id,''),name,COALESCE(local_timezone,''),updated_at,COALESCE((SELECT capture_via FROM recordings WHERE account_id=$2 AND stream_id=streams.id AND status<>'canceled' ORDER BY id DESC LIMIT 1),'') FROM streams WHERE id=$1 AND deleted_at IS NULL FOR UPDATE`, entry.StreamID, p.AccountID).Scan(&sourceURL, &sourcePageURL, &sourceRevisionID, &provider, &externalID, &streamName, &streamTimezone, &sourceUpdatedAt, &existingCaptureVia)
		if errors.Is(err, pgx.ErrNoRows) {
			util.WriteError(w, http.StatusNotFound, fmt.Sprintf("stream %d was not found", entry.StreamID))
			return
		}
		if err != nil {
			util.WriteError(w, http.StatusInternalServerError, "load campaign admission stream")
			return
		}
		if sourceURL != entry.SourceURL || sourcePageURL != entry.SourcePageURL || sourceRevisionID != entry.SourceRevisionID || provider != entry.Provider || externalID != entry.ExternalID || normalizeCampaignAdmissionLabel(streamName, entry.StreamID) != entry.NormalizedLabel || streamTimezone != req.Schedule.StreamTimezones[entryIndex].Timezone {
			util.WriteError(w, http.StatusConflict, fmt.Sprintf("stream %d source binding changed", entry.StreamID))
			return
		}
		if batchCaptureVia(sourceURL, provider, existingCaptureVia) != "cloud" {
			util.WriteError(w, http.StatusConflict, fmt.Sprintf("stream %d is not cloud-native", entry.StreamID))
			return
		}
		if entry.RecordingID > 0 {
			var recordingAccount, recordingStream int64
			var recordingStatus string
			if err = tx.QueryRow(r.Context(), `SELECT account_id,stream_id,status FROM recordings WHERE id=$1 FOR UPDATE`, entry.RecordingID).Scan(&recordingAccount, &recordingStream, &recordingStatus); err != nil || recordingAccount != p.AccountID || recordingStream != entry.StreamID || recordingStatus != "completed" {
				util.WriteError(w, http.StatusConflict, fmt.Sprintf("recording %d is not the exact completed stream recording", entry.RecordingID))
				return
			}
		}
		var sceneBound bool
		if err = tx.QueryRow(r.Context(), `SELECT EXISTS(SELECT 1 FROM recording_scene_frame_evidence WHERE id=$1 AND account_id=$2 AND stream_id=$3 AND scene_identity_sha256=$4 AND captured_at>=transaction_timestamp()-interval '6 hours' AND captured_at<=transaction_timestamp() AND verified_at>=transaction_timestamp()-interval '6 hours' AND verified_at<=transaction_timestamp())`, entry.SceneFrameEvidenceID, p.AccountID, entry.StreamID, entry.SceneIdentitySHA256).Scan(&sceneBound); err != nil || !sceneBound {
			util.WriteError(w, http.StatusConflict, fmt.Sprintf("stream %d lacks the exact fresh authoritative scene-frame evidence", entry.StreamID))
			return
		}
		var collision bool
		err = tx.QueryRow(r.Context(), `
			SELECT EXISTS(
			 SELECT 1 FROM recordings active_recording
			 JOIN streams active_stream ON active_stream.id=active_recording.stream_id
			 JOIN streams candidate ON candidate.id=$2
			 WHERE active_recording.account_id=$1 AND active_recording.status<>'canceled'
			   AND NOT(active_recording.id=NULLIF($3,0) AND active_recording.status='completed')
			   AND (active_recording.stream_id=candidate.id OR active_stream.source_url=candidate.source_url OR
			        (NULLIF(active_stream.provider,'') IS NOT NULL AND active_stream.provider=candidate.provider AND NULLIF(active_stream.external_id,'') IS NOT NULL AND active_stream.external_id=candidate.external_id) OR
			        COALESCE(NULLIF(regexp_replace(lower(active_stream.name),'[^a-z0-9]','','g'),''),'stream'||active_stream.id::text)=COALESCE(NULLIF(regexp_replace(lower(candidate.name),'[^a-z0-9]','','g'),''),'stream'||candidate.id::text))
			)`, p.AccountID, entry.StreamID, entry.RecordingID).Scan(&collision)
		if err != nil {
			util.WriteError(w, http.StatusInternalServerError, "check campaign admission deduplication")
			return
		}
		if collision {
			util.WriteError(w, http.StatusConflict, fmt.Sprintf("stream %d collides with an active recording", entry.StreamID))
			return
		}
		storedEntries = append(storedEntries, campaignAdmissionStoredEntry{
			StreamID: entry.StreamID, RecordingID: entry.RecordingID, SourceRevisionID: entry.SourceRevisionID,
			SourceURLSHA256: hashSecret(entry.SourceURL), SourcePageURLSHA256: hashSecret(entry.SourcePageURL),
			SourceUpdatedAtUnixMic: sourceUpdatedAt.UTC().UnixMicro(), Provider: entry.Provider, ExternalID: entry.ExternalID,
			NormalizedLabel: entry.NormalizedLabel, SceneFrameEvidenceID: entry.SceneFrameEvidenceID, SceneIdentitySHA256: entry.SceneIdentitySHA256,
		})
	}
	var pairwiseCollision bool
	err = tx.QueryRow(r.Context(), `
		SELECT EXISTS(
			SELECT 1
			FROM streams a JOIN streams b ON a.id<b.id
			WHERE a.id=ANY($1::bigint[]) AND b.id=ANY($1::bigint[])
			  AND (a.source_url=b.source_url OR
			       (NULLIF(a.provider,'') IS NOT NULL AND a.provider=b.provider AND NULLIF(a.external_id,'') IS NOT NULL AND a.external_id=b.external_id) OR
			       COALESCE(NULLIF(regexp_replace(lower(a.name),'[^a-z0-9]','','g'),''),'stream'||a.id::text)=COALESCE(NULLIF(regexp_replace(lower(b.name),'[^a-z0-9]','','g'),''),'stream'||b.id::text))
		)`, campaignAdmissionStreamIDs(entries)).Scan(&pairwiseCollision)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "check campaign admission pairwise deduplication")
		return
	}
	if pairwiseCollision {
		util.WriteError(w, http.StatusConflict, "approved streams collide with each other")
		return
	}
	rawEntries, err := json.Marshal(storedEntries)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "encode campaign admission approval")
		return
	}
	rawSchedule, err := json.Marshal(req.Schedule)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "encode campaign admission schedule")
		return
	}
	if err := tx.Rollback(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "release campaign approval preflight")
		return
	}
	var id, digest string
	err = s.admissionPool.QueryRow(r.Context(), `SELECT approval_id::text,approval_sha256 FROM recording_campaign_approve($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,$11::jsonb,$12)`, requestID, p.AccountID, p.UserID, *p.SessionID, p.credentialSHA256, strings.ToLower(strings.TrimSpace(p.Email)), req.AuthorityCode, req.FailureDomainTag, req.DeadlineAt.UTC(), rawEntries, rawSchedule, requestSHA).Scan(&id, &digest)
	if err != nil {
		util.WriteError(w, http.StatusConflict, "campaign admission approval was not persisted")
		return
	}
	util.WriteJSON(w, http.StatusCreated, map[string]any{"approval_id": id, "approval_sha256": digest, "stream_ids": campaignAdmissionStreamIDs(entries)})
}

type campaignAdmissionTargetsRequest struct {
	ApprovalID string  `json:"approval_id"`
	StreamIDs  []int64 `json:"stream_ids"`
	RequestID  string  `json:"request_id"`
}

type campaignAdmissionEvidenceRequest struct {
	ApprovalID string                         `json:"approval_id"`
	StreamID   int64                          `json:"stream_id"`
	AttemptID  string                         `json:"attempt_id"`
	RequestID  string                         `json:"request_id"`
	Evidence   recordability.TargetedEvidence `json:"evidence"`
}

type managedProbeRecorder struct {
	ID          int64
	DODropletID int64
	Region      string
	BuildSHA    string
}

type campaignAdmissionProbeOrderRequest struct {
	ApprovalID string `json:"approval_id"`
	StreamID   int64  `json:"stream_id"`
	RequestID  string `json:"request_id"`
}

type campaignCloudCapacityObservation struct {
	ObservedAt            time.Time
	ReadyWorkers          int
	TotalSlots            int
	LargestWorkerSlots    int
	UsableAfterWorkerLoss int
	LargestRegion         string
	LargestRegionSlots    int
	FactsSHA256           string
}

func (s *Server) observeCampaignCloudCapacity(ctx context.Context) (campaignCloudCapacityObservation, error) {
	if s.campaignDOAttest == nil || strings.TrimSpace(s.cfg.DropletPoolBuildSHA) == "" {
		return campaignCloudCapacityObservation{}, fmt.Errorf("managed cloud capacity attestation is unavailable")
	}
	type worker struct {
		ID, NodeID, DOID    int64
		Name, Region, Build string
		Capacity            int
	}
	rows, err := s.pool.Query(ctx, `SELECT d.id,d.node_id,d.do_droplet_id,d.name,d.region,d.build_sha,d.capacity FROM recorder_droplets d JOIN nodes n ON n.id=d.node_id WHERE d.state='active' AND n.status='active' AND n.node_type='local_recorder' AND d.last_seen_at BETWEEN transaction_timestamp()-interval '120 seconds' AND transaction_timestamp()+interval '30 seconds' AND d.build_sha=$1 ORDER BY d.id`, strings.ToLower(strings.TrimSpace(s.cfg.DropletPoolBuildSHA)))
	if err != nil {
		return campaignCloudCapacityObservation{}, fmt.Errorf("load current-build cloud capacity: %w", err)
	}
	defer rows.Close()
	var workers []worker
	for rows.Next() {
		var item worker
		if err := rows.Scan(&item.ID, &item.NodeID, &item.DOID, &item.Name, &item.Region, &item.Build, &item.Capacity); err != nil {
			return campaignCloudCapacityObservation{}, err
		}
		if item.DOID > 0 && item.Capacity > 0 {
			workers = append(workers, item)
		}
	}
	if err := rows.Err(); err != nil {
		return campaignCloudCapacityObservation{}, err
	}
	type fact struct {
		DropletID int64  `json:"droplet_id"`
		NodeID    int64  `json:"node_id"`
		Region    string `json:"region"`
		Capacity  int    `json:"capacity"`
		BuildSHA  string `json:"build_sha"`
	}
	observation := campaignCloudCapacityObservation{ObservedAt: time.Now().UTC()}
	regionSlots := map[string]int{}
	facts := make([]fact, 0, len(workers))
	for _, item := range workers {
		attestCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		provider, attestErr := s.campaignDOAttest(attestCtx, item.DOID, item.Name)
		cancel()
		if attestErr != nil || provider.DropletID != item.DOID || provider.Name != item.Name || provider.Region != item.Region || provider.Status != "active" {
			continue
		}
		observation.ReadyWorkers++
		observation.TotalSlots += item.Capacity
		if item.Capacity > observation.LargestWorkerSlots {
			observation.LargestWorkerSlots = item.Capacity
		}
		regionSlots[item.Region] += item.Capacity
		facts = append(facts, fact{DropletID: item.DOID, NodeID: item.NodeID, Region: item.Region, Capacity: item.Capacity, BuildSHA: item.Build})
	}
	for region, slots := range regionSlots {
		if slots > observation.LargestRegionSlots || (slots == observation.LargestRegionSlots && region < observation.LargestRegion) {
			observation.LargestRegion, observation.LargestRegionSlots = region, slots
		}
	}
	observation.UsableAfterWorkerLoss = observation.TotalSlots - observation.LargestWorkerSlots
	rawFacts, err := json.Marshal(facts)
	if err != nil {
		return campaignCloudCapacityObservation{}, err
	}
	observation.FactsSHA256 = hashSecret(string(rawFacts))
	if observation.ReadyWorkers < 2 || observation.UsableAfterWorkerLoss <= 0 || !lowerSHA256(observation.FactsSHA256) {
		return campaignCloudCapacityObservation{}, fmt.Errorf("cloud capacity cannot survive one worker loss")
	}
	return observation, nil
}

type campaignAdmissionSceneReviewRequest struct {
	ApprovalID      string `json:"approval_id"`
	ProbeEvidenceID string `json:"probe_evidence_id"`
	PresentationID  string `json:"presentation_id"`
	RequestID       string `json:"request_id"`
}

const (
	targetedProbeMediaMaxBytes int64 = 128 << 20
	targetedProbeFrameMaxBytes int64 = recordability.TargetedFrameMaxBytes
)

func (s *Server) handleAccountCampaignAdmissionSceneReviewCreate(w http.ResponseWriter, r *http.Request) {
	p, ok := accountPrincipalFromContext(r.Context())
	if !ok || p.UserID == 0 || p.SessionID == nil || p.Role != accountRoleAdmin || !strings.EqualFold(strings.TrimSpace(p.Email), strings.TrimSpace(s.cfg.DropletPoolOperatorEmail)) {
		util.WriteError(w, http.StatusForbidden, "targeted scene review requires Deniz's exact operator browser session")
		return
	}
	var req campaignAdmissionSceneReviewRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	approvalID, approvalErr := uuid.Parse(strings.TrimSpace(req.ApprovalID))
	evidenceID, evidenceErr := uuid.Parse(strings.TrimSpace(req.ProbeEvidenceID))
	presentationID, presentationErr := uuid.Parse(strings.TrimSpace(req.PresentationID))
	requestID, requestErr := uuid.Parse(strings.TrimSpace(req.RequestID))
	if approvalErr != nil || evidenceErr != nil || presentationErr != nil || requestErr != nil {
		util.WriteError(w, http.StatusBadRequest, "approval_id, probe_evidence_id, presentation_id, and request_id must be UUIDs")
		return
	}
	var replayID, replaySHA string
	replayErr := s.pool.QueryRow(r.Context(), `SELECT id::text,review_sha256 FROM recording_targeted_probe_scene_reviews WHERE account_id=$1 AND request_id=$2 AND approval_id=$3 AND probe_evidence_id=$4 AND presentation_id=$5 AND reviewed_by_user_id=$6`, p.AccountID, requestID, approvalID, evidenceID, presentationID, p.UserID).Scan(&replayID, &replaySHA)
	if replayErr == nil {
		util.WriteJSON(w, http.StatusCreated, map[string]any{"scene_review_id": replayID, "review_sha256": replaySHA, "probe_evidence_id": evidenceID})
		return
	}
	if !errors.Is(replayErr, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusInternalServerError, "load targeted scene review replay")
		return
	}
	err := s.admissionPool.QueryRow(r.Context(), `SELECT review_id::text,review_sha256 FROM recording_campaign_review_probe_scene($1,$2,$3,$4,$5,$6,$7,$8)`, requestID, approvalID, p.AccountID, p.UserID, *p.SessionID, p.credentialSHA256, evidenceID, presentationID).Scan(&replayID, &replaySHA)
	if err != nil {
		util.WriteError(w, http.StatusConflict, "targeted scene review is not exact or fresh")
		return
	}
	util.WriteJSON(w, http.StatusCreated, map[string]any{"scene_review_id": replayID, "review_sha256": replaySHA, "probe_evidence_id": evidenceID})
}

func (s *Server) handleAccountCampaignAdmissionScenePresentationGet(w http.ResponseWriter, r *http.Request) {
	p, ok := accountPrincipalFromContext(r.Context())
	store := s.campaignProbeObjects()
	if !ok || p.UserID == 0 || p.SessionID == nil || p.Role != accountRoleAdmin ||
		!strings.EqualFold(strings.TrimSpace(p.Email), strings.TrimSpace(s.cfg.DropletPoolOperatorEmail)) || store == nil {
		util.WriteError(w, http.StatusForbidden, "targeted scene presentation requires Deniz's exact operator browser session")
		return
	}
	evidenceID, evidenceErr := uuid.Parse(strings.TrimSpace(chi.URLParam(r, "evidenceId")))
	requestID, requestErr := uuid.Parse(strings.TrimSpace(r.URL.Query().Get("request_id")))
	if evidenceErr != nil || requestErr != nil {
		util.WriteError(w, http.StatusBadRequest, "evidenceId and request_id must be UUIDs")
		return
	}
	var approvalID uuid.UUID
	var key, etag, version, wantSHA string
	var size int64
	if err := s.pool.QueryRow(r.Context(), `SELECT approval_id,frame_archive_object_key,frame_archive_etag,COALESCE(frame_archive_version_id,''),frame_archive_sha256,frame_size_bytes FROM recording_targeted_probe_evidence WHERE id=$1 AND account_id=$2 AND result='ok'`, evidenceID, p.AccountID).Scan(&approvalID, &key, &etag, &version, &wantSHA, &size); err != nil {
		util.WriteError(w, http.StatusNotFound, "targeted scene evidence was not found")
		return
	}
	body, err := readExactObjectBounded(r.Context(), store.OpenExact, key, etag, version, size, targetedProbeFrameMaxBytes)
	if err != nil {
		util.WriteError(w, http.StatusConflict, "protected targeted frame is unavailable or changed")
		return
	}
	sum := sha256.Sum256(body)
	if hex.EncodeToString(sum[:]) != wantSHA {
		util.WriteError(w, http.StatusConflict, "protected targeted frame digest changed")
		return
	}
	var presentationID, presentationSHA string
	if err := s.admissionPool.QueryRow(r.Context(), `SELECT presentation_id::text,presentation_sha256 FROM recording_campaign_present_probe_scene($1,$2,$3,$4,$5,$6,$7)`, requestID, approvalID, p.AccountID, p.UserID, *p.SessionID, p.credentialSHA256, evidenceID).Scan(&presentationID, &presentationSHA); err != nil {
		util.WriteError(w, http.StatusConflict, "targeted scene presentation was not sealed")
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{
		"presentation_id": presentationID, "presentation_sha256": presentationSHA,
		"approval_id": approvalID, "probe_evidence_id": evidenceID,
		"frame_sha256": wantSHA, "content_type": "image/jpeg",
		"frame_base64": base64.StdEncoding.EncodeToString(body),
	})
}

func (s *Server) handleAccountCampaignAdmissionProbeOrderCreate(w http.ResponseWriter, r *http.Request) {
	p, ok := accountPrincipalFromContext(r.Context())
	if !ok || p.UserID == 0 || p.SessionID == nil || p.Role != accountRoleAdmin || !strings.EqualFold(strings.TrimSpace(p.Email), strings.TrimSpace(s.cfg.DropletPoolOperatorEmail)) {
		util.WriteError(w, http.StatusForbidden, "targeted probe order requires the exact operator browser session")
		return
	}
	var req campaignAdmissionProbeOrderRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	approvalID, err := uuid.Parse(strings.TrimSpace(req.ApprovalID))
	requestID, requestErr := uuid.Parse(strings.TrimSpace(req.RequestID))
	if err != nil || requestErr != nil || req.StreamID <= 0 {
		util.WriteError(w, http.StatusBadRequest, "approval_id, request_id, and stream_id are required")
		return
	}
	var orderID string
	replayErr := s.pool.QueryRow(r.Context(), `SELECT id::text FROM recording_targeted_probe_orders WHERE account_id=$1 AND request_id=$2 AND approval_id=$3 AND stream_id=$4 AND requested_by_user_id=$5`, p.AccountID, requestID, approvalID, req.StreamID, p.UserID).Scan(&orderID)
	if replayErr == nil {
		util.WriteJSON(w, http.StatusOK, map[string]any{"order_id": orderID, "approval_id": approvalID, "stream_id": req.StreamID, "desired_attempts": 2})
		return
	}
	if !errors.Is(replayErr, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusConflict, "load targeted probe order replay")
		return
	}
	err = s.admissionPool.QueryRow(r.Context(), `SELECT order_id::text FROM recording_campaign_queue_probe($1,$2,$3,$4,$5,$6,$7)`, requestID, approvalID, p.AccountID, p.UserID, *p.SessionID, p.credentialSHA256, req.StreamID).Scan(&orderID)
	if err != nil {
		util.WriteError(w, http.StatusConflict, "targeted probe order was not persisted")
		return
	}
	util.WriteJSON(w, http.StatusCreated, map[string]any{"order_id": orderID, "approval_id": approvalID, "stream_id": req.StreamID, "desired_attempts": 2})
}

func (s *Server) handleRecordingCampaignAdmissionProbeLease(w http.ResponseWriter, r *http.Request) {
	p, ok := nodePrincipalFromContext(r.Context())
	store := s.campaignProbeObjects()
	if !ok || p.NodeType != nodeTypeLocalRecorder || p.NodeTokenID <= 0 || p.NodeClaimGeneration <= 0 || p.NodeClaimPurpose != "claim_current" || store == nil {
		util.WriteError(w, http.StatusUnauthorized, "managed recorder node token required")
		return
	}
	var dbDroplet managedProbeRecorder
	var dbName string
	err := s.pool.QueryRow(r.Context(), `SELECT id,name,do_droplet_id,region,build_sha FROM recorder_droplets WHERE node_id=$1 AND name=$2 AND state='active' AND last_seen_at BETWEEN transaction_timestamp()-interval '120 seconds' AND transaction_timestamp()+interval '30 seconds'`, p.NodeID, strings.TrimSpace(p.DisplayName)).Scan(&dbDroplet.ID, &dbName, &dbDroplet.DODropletID, &dbDroplet.Region, &dbDroplet.BuildSHA)
	if err != nil || dbDroplet.DODropletID <= 0 || dbDroplet.BuildSHA != strings.ToLower(strings.TrimSpace(s.cfg.DropletPoolBuildSHA)) || s.campaignDOAttest == nil {
		util.WriteError(w, http.StatusConflict, "managed recorder is not eligible for targeted probes")
		return
	}
	attestCtx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	provider, err := s.campaignDOAttest(attestCtx, dbDroplet.DODropletID, dbName)
	cancel()
	if err != nil || provider.DropletID != dbDroplet.DODropletID || provider.Name != dbName || provider.Region != dbDroplet.Region || provider.Status != "active" {
		util.WriteError(w, http.StatusConflict, "current DigitalOcean project/firewall attestation failed")
		return
	}
	var target recordability.Target
	attemptID, requestID := uuid.New(), uuid.New()
	mediaObjectKey := fmt.Sprintf("quarantine/campaign-probe/%s/media.zip", attemptID)
	frameObjectKey := fmt.Sprintf("quarantine/campaign-probe/%s/frame.jpg", attemptID)
	challenge := hashSecret(attemptID.String() + "\n" + requestID.String() + "\n" + mediaObjectKey + "\n" + frameObjectKey + "\n" + uuid.NewString())
	target.MediaUploadURL, err = store.PresignPut(r.Context(), mediaObjectKey, "application/zip", s.cfg.R2SignPutTTL)
	if err != nil {
		util.WriteError(w, http.StatusServiceUnavailable, "reserve targeted media quarantine object")
		return
	}
	target.FrameUploadURL, err = store.PresignPut(r.Context(), frameObjectKey, "image/jpeg", s.cfg.R2SignPutTTL)
	if err != nil {
		util.WriteError(w, http.StatusServiceUnavailable, "reserve targeted frame quarantine object")
		return
	}
	var orderID, approvalID string
	err = s.admissionPool.QueryRow(r.Context(), `SELECT order_id::text,approval_id::text,stream_id,COALESCE(provider,''),source_url,COALESCE(source_page_url,''),attempt_id::text,challenge FROM recording_campaign_lease_probe($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)`, p.NodeID, p.NodeTokenID, p.NodeClaimGeneration, p.credentialSHA256, dbDroplet.ID, dbDroplet.DODropletID, dbDroplet.Region, dbDroplet.BuildSHA, hashSecret(s.cfg.DropletPoolProjectID), hashSecret(s.cfg.DropletPoolFirewallID), attemptID, requestID, challenge, hashSecret(store.Bucket()), mediaObjectKey, frameObjectKey, targetedProbeMediaMaxBytes, targetedProbeFrameMaxBytes).Scan(&orderID, &approvalID, &target.ID, &target.Provider, &target.SourceURL, &target.SourcePageURL, &target.AttemptID, &target.Challenge)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteJSON(w, http.StatusOK, map[string]any{"target": nil})
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusConflict, "create provider-attested targeted attempt")
		return
	}
	target.MediaMaxSizeBytes = targetedProbeMediaMaxBytes
	target.FrameMaxSizeBytes = targetedProbeFrameMaxBytes
	util.WriteJSON(w, http.StatusOK, map[string]any{"target": target, "approval_id": approvalID, "request_id": requestID, "order_id": orderID})
}

func (s *Server) handleRecordingCampaignAdmissionTargets(w http.ResponseWriter, r *http.Request) {
	// Intentionally unreachable from the router. Probe work is claimed by the
	// deployed recorder loop through /lease; accepting a caller-selected bearer
	// here would recreate the manual-token execution bypass.
	util.WriteError(w, http.StatusGone, "targeted probes must use the provider-attested worker lease queue")
}

func (s *Server) handleRecordingCampaignAdmissionEvidence(w http.ResponseWriter, r *http.Request) {
	p, ok := nodePrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	var req campaignAdmissionEvidenceRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	approvalID, err := uuid.Parse(strings.TrimSpace(req.ApprovalID))
	attemptID, attemptErr := uuid.Parse(strings.TrimSpace(req.AttemptID))
	requestID, requestErr := uuid.Parse(strings.TrimSpace(req.RequestID))
	if err != nil || attemptErr != nil || requestErr != nil || req.StreamID <= 0 || len(req.Evidence.Detail) > 1024 {
		util.WriteError(w, http.StatusBadRequest, "invalid approval, stream, or evidence shape")
		return
	}
	if p.NodeTokenID <= 0 || p.NodeClaimGeneration <= 0 || p.NodeClaimPurpose != "claim_current" {
		util.WriteError(w, http.StatusUnauthorized, "targeted evidence requires an authenticated node token identity")
		return
	}
	// Terminal replay is deliberately resolved before mutable worker, attempt,
	// approval, or source freshness. A committed response must remain replayable
	// after any of those live prerequisites expire.
	var replayID, replaySHA, replayResult, replayDetail string
	replayErr := s.pool.QueryRow(r.Context(), `SELECT e.id::text,e.evidence_sha256,e.result,e.detail FROM recording_targeted_probe_evidence e JOIN recording_targeted_probe_attempts a ON a.id=e.attempt_id WHERE e.attempt_id=$1 AND a.request_id=$2 AND e.approval_id=$3 AND e.stream_id=$4 AND a.node_id=$5`, attemptID, requestID, approvalID, req.StreamID, p.NodeID).Scan(&replayID, &replaySHA, &replayResult, &replayDetail)
	if replayErr == nil {
		if replayResult != req.Evidence.Result || (replayResult != recordability.ResultOK && replayDetail != req.Evidence.Detail) {
			util.WriteError(w, http.StatusConflict, "targeted evidence request idempotency conflict")
			return
		}
		util.WriteJSON(w, http.StatusCreated, map[string]any{"evidence_id": replayID, "attempt_id": attemptID, "evidence_sha256": replaySHA})
		return
	}
	if !errors.Is(replayErr, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusInternalServerError, "load targeted evidence replay")
		return
	}
	var targetAccountID int64
	var challenge, mediaKey, frameKey string
	var mediaMax, frameMax int64
	err = s.pool.QueryRow(r.Context(), `SELECT account_id,challenge,media_object_key,frame_object_key,media_max_size_bytes,frame_max_size_bytes FROM recording_targeted_probe_attempts WHERE id=$1 AND request_id=$2 AND approval_id=$3 AND stream_id=$4 AND node_id=$5`, attemptID, requestID, approvalID, req.StreamID, p.NodeID).Scan(&targetAccountID, &challenge, &mediaKey, &frameKey, &mediaMax, &frameMax)
	if err != nil {
		util.WriteError(w, http.StatusConflict, "targeted attempt identity does not match")
		return
	}
	var observation targetedQuarantineObservation
	if req.Evidence.Result == recordability.ResultOK {
		observation, err = s.verifyTargetedQuarantine(r.Context(), attemptID.String(), challenge, mediaKey, frameKey, mediaMax, frameMax)
		if err != nil {
			util.WriteError(w, http.StatusConflict, "targeted quarantine verification failed")
			return
		}
		req.Evidence = observation.Evidence
	} else {
		// Failed attempts advance the append-only queue but carry no affirmative
		// media authority. Strip every caller-authored proof field.
		req.Evidence.FrameBase64 = ""
		req.Evidence.FrameSHA256 = ""
		req.Evidence.MediaSHA256 = ""
		req.Evidence.NativeSignatureSHA256 = ""
		req.Evidence.ChallengeProofSHA256 = ""
		req.Evidence.VideoCodec = ""
		req.Evidence.AudioCodec = ""
		req.Evidence.AudioPresent = false
		req.Evidence.VideoWidth = 0
		req.Evidence.VideoHeight = 0
		req.Evidence.ActualFPS = nil
	}
	nativeSignature := req.Evidence.NativeSignatureSHA256
	challengeProof := req.Evidence.ChallengeProofSHA256
	observationJSON, err := json.Marshal(map[string]any{
		"result": req.Evidence.Result, "valid_ratio": req.Evidence.ValidRatio,
		"duration_ms": req.Evidence.DurationMs, "segment_count": req.Evidence.SegmentCount,
		"frame_sha256": req.Evidence.FrameSHA256, "media_sha256": req.Evidence.MediaSHA256,
		"native_signature_sha256": nativeSignature, "challenge_proof_sha256": challengeProof,
		"video_codec": req.Evidence.VideoCodec, "audio_codec": req.Evidence.AudioCodec,
		"audio_present": req.Evidence.AudioPresent, "video_width": req.Evidence.VideoWidth,
		"video_height": req.Evidence.VideoHeight, "actual_fps": req.Evidence.ActualFPS,
		"detail": req.Evidence.Detail, "media_size_bytes": observation.MediaSizeBytes,
		"media_etag": observation.MediaETag, "media_version_id": observation.MediaVersionID,
		"frame_size_bytes": observation.FrameSizeBytes, "frame_etag": observation.FrameETag,
		"frame_version_id": observation.FrameVersionID, "archive_bucket_sha256": observation.ArchiveBucketSHA256,
		"media_archive_object_key": observation.MediaArchiveKey, "media_archive_sha256": observation.MediaArchiveSHA256,
		"media_archive_etag": observation.MediaArchiveETag, "media_archive_version_id": observation.MediaArchiveVersion,
		"frame_archive_object_key": observation.FrameArchiveKey, "frame_archive_sha256": observation.FrameArchiveSHA256,
		"frame_archive_etag": observation.FrameArchiveETag, "frame_archive_version_id": observation.FrameArchiveVersion,
		"evidence_sha256": strings.Repeat("0", 64),
	})
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "encode targeted evidence observation")
		return
	}
	var evidenceID string
	var evidenceSHA string
	err = s.admissionPool.QueryRow(r.Context(), `SELECT evidence_id::text,evidence_sha256 FROM recording_campaign_submit_probe_evidence($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb)`, p.NodeID, p.NodeTokenID, p.NodeClaimGeneration, p.credentialSHA256, attemptID, requestID, approvalID, targetAccountID, req.StreamID, observationJSON).Scan(&evidenceID, &evidenceSHA)
	if err != nil {
		util.WriteError(w, http.StatusConflict, "targeted evidence failed server/DB attestation")
		return
	}
	util.WriteJSON(w, http.StatusCreated, map[string]any{"evidence_id": evidenceID, "attempt_id": attemptID, "evidence_sha256": evidenceSHA})
}

// canonicalizeTargetedFrameEvidence makes the API, rather than the recorder,
// authoritative for decoded-frame bytes. Successful evidence must carry one
// bounded JPEG; its hash is recomputed after decoding. Raw bytes are request
// transport only and are never persisted in the evidence tables.
func canonicalizeTargetedFrameEvidence(e *recordability.TargetedEvidence) error {
	if e == nil {
		return fmt.Errorf("targeted evidence is required")
	}
	raw := strings.TrimSpace(e.FrameBase64)
	e.FrameBase64 = ""
	if raw == "" {
		if e.Result == recordability.ResultOK {
			return fmt.Errorf("successful targeted evidence requires decoded frame bytes")
		}
		e.FrameSHA256 = ""
		return nil
	}
	if len(raw) > (recordability.TargetedFrameMaxBytes*4/3)+8 {
		return fmt.Errorf("targeted decoded frame exceeds the bounded payload")
	}
	frameBytes, err := base64.StdEncoding.DecodeString(raw)
	if err != nil || len(frameBytes) == 0 || len(frameBytes) > recordability.TargetedFrameMaxBytes {
		return fmt.Errorf("targeted decoded frame is not valid bounded base64")
	}
	cfg, format, err := image.DecodeConfig(bytes.NewReader(frameBytes))
	if err != nil || format != "jpeg" || cfg.Width <= 0 || cfg.Height <= 0 || cfg.Width > 8192 || cfg.Height > 8192 || int64(cfg.Width)*int64(cfg.Height) > 33_554_432 {
		return fmt.Errorf("targeted decoded frame must be a bounded JPEG")
	}
	frame, err := capture.BuildFrameFromBytes(frameBytes, "image/jpeg", "targeted_recordability_probe")
	if err != nil {
		return fmt.Errorf("targeted decoded frame is invalid")
	}
	claimed := strings.ToLower(strings.TrimSpace(e.FrameSHA256))
	if claimed != "" && claimed != frame.SHA256 {
		return fmt.Errorf("targeted decoded frame SHA-256 mismatch")
	}
	e.FrameSHA256 = frame.SHA256
	return nil
}

type targetedQuarantineObservation struct {
	Evidence            recordability.TargetedEvidence
	MediaSizeBytes      int64
	MediaETag           string
	MediaVersionID      string
	FrameSizeBytes      int64
	FrameETag           string
	FrameVersionID      string
	ArchiveBucketSHA256 string
	MediaArchiveKey     string
	MediaArchiveSHA256  string
	MediaArchiveETag    string
	MediaArchiveVersion string
	FrameArchiveKey     string
	FrameArchiveSHA256  string
	FrameArchiveETag    string
	FrameArchiveVersion string
}

func (s *Server) verifyTargetedQuarantine(ctx context.Context, attemptID, challenge, mediaKey, frameKey string, mediaMax, frameMax int64) (targetedQuarantineObservation, error) {
	store := s.campaignProbeObjects()
	if store == nil || mediaMax <= 0 || frameMax <= 0 {
		return targetedQuarantineObservation{}, fmt.Errorf("targeted quarantine storage is unavailable")
	}
	mediaHead, err := store.Head(ctx, mediaKey)
	if err != nil || mediaHead.SizeBytes <= 0 || mediaHead.SizeBytes > mediaMax || strings.TrimSpace(mediaHead.ETag) == "" {
		return targetedQuarantineObservation{}, fmt.Errorf("targeted media quarantine object is missing or out of bounds")
	}
	frameHead, err := store.Head(ctx, frameKey)
	if err != nil || frameHead.SizeBytes <= 0 || frameHead.SizeBytes > frameMax || strings.TrimSpace(frameHead.ETag) == "" {
		return targetedQuarantineObservation{}, fmt.Errorf("targeted frame quarantine object is missing or out of bounds")
	}
	tmpDir, err := os.MkdirTemp("", "targeted-quarantine-verify-")
	if err != nil {
		return targetedQuarantineObservation{}, fmt.Errorf("create targeted verification directory: %w", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()
	archivePath := filepath.Join(tmpDir, "media.zip")
	if err := copyExactObjectToFile(ctx, store.OpenExact, mediaKey, mediaHead.ETag, mediaHead.VersionID, archivePath, mediaHead.SizeBytes, mediaMax); err != nil {
		return targetedQuarantineObservation{}, err
	}
	frameBody, err := readExactObjectBounded(ctx, store.OpenExact, frameKey, frameHead.ETag, frameHead.VersionID, frameHead.SizeBytes, frameMax)
	if err != nil {
		return targetedQuarantineObservation{}, err
	}
	frameEvidence := recordability.TargetedEvidence{Result: recordability.ResultOK, FrameBase64: base64.StdEncoding.EncodeToString(frameBody)}
	if err := canonicalizeTargetedFrameEvidence(&frameEvidence); err != nil {
		return targetedQuarantineObservation{}, err
	}
	zr, err := zip.OpenReader(archivePath)
	if err != nil {
		return targetedQuarantineObservation{}, fmt.Errorf("open targeted media archive: %w", err)
	}
	defer zr.Close()
	if len(zr.File) != 2 {
		return targetedQuarantineObservation{}, fmt.Errorf("targeted media archive must contain exactly two segments")
	}
	var evidence recordability.TargetedEvidence
	segmentHashes := make([]string, 0, 2)
	var signature string
	for i, entry := range zr.File {
		wantName := fmt.Sprintf("segment-%d.mp4", i+1)
		if entry.Name != wantName || entry.FileInfo().IsDir() || entry.UncompressedSize64 == 0 || entry.UncompressedSize64 > uint64(mediaMax) {
			return targetedQuarantineObservation{}, fmt.Errorf("targeted media archive has invalid exact entry")
		}
		segmentPath := filepath.Join(tmpDir, wantName)
		rc, openErr := entry.Open()
		if openErr != nil {
			return targetedQuarantineObservation{}, fmt.Errorf("open targeted media segment: %w", openErr)
		}
		segmentFile, createErr := os.OpenFile(segmentPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if createErr == nil {
			_, createErr = io.CopyN(segmentFile, rc, int64(entry.UncompressedSize64))
		}
		_ = rc.Close()
		if segmentFile != nil {
			_ = segmentFile.Close()
		}
		if createErr != nil {
			return targetedQuarantineObservation{}, fmt.Errorf("extract targeted media segment: %w", createErr)
		}
		segmentBody, readErr := os.ReadFile(segmentPath)
		if readErr != nil || int64(len(segmentBody)) != int64(entry.UncompressedSize64) {
			return targetedQuarantineObservation{}, fmt.Errorf("read targeted media segment")
		}
		sum := sha256.Sum256(segmentBody)
		segmentHashes = append(segmentHashes, hex.EncodeToString(sum[:]))
		meta, inspectErr := capture.InspectSegmentFile(ctx, segmentPath)
		if inspectErr != nil {
			return targetedQuarantineObservation{}, inspectErr
		}
		if err := capture.ValidateSegmentDecode(ctx, segmentPath); err != nil {
			return targetedQuarantineObservation{}, err
		}
		segmentEvidence := recordability.TargetedEvidence{VideoCodec: strings.ToLower(strings.TrimSpace(meta.VideoCodec)), AudioCodec: strings.ToLower(strings.TrimSpace(meta.AudioCodec)), AudioPresent: meta.AudioPresent, VideoWidth: meta.VideoWidth, VideoHeight: meta.VideoHeight, ActualFPS: meta.ActualFPS}
		segmentSignature := recordability.TargetedNativeSignatureSHA256(segmentEvidence)
		if signature == "" {
			signature = segmentSignature
			evidence = segmentEvidence
			thumb, thumbErr := capture.ExtractSegmentThumbnail(ctx, segmentPath)
			if thumbErr != nil || thumb.SHA256 != frameEvidence.FrameSHA256 {
				return targetedQuarantineObservation{}, fmt.Errorf("targeted frame is not derived from the exact first media segment")
			}
		} else if segmentSignature != signature {
			return targetedQuarantineObservation{}, fmt.Errorf("targeted media native signature changed between segments")
		}
		evidence.DurationMs += meta.DurationMs
		evidence.SegmentCount++
	}
	evidence.Result = recordability.ResultOK
	evidence.ValidRatio = 1
	evidence.FrameSHA256 = frameEvidence.FrameSHA256
	evidence.MediaSHA256 = recordability.TargetedMediaSHA256(segmentHashes)
	evidence.NativeSignatureSHA256 = recordability.TargetedNativeSignatureSHA256(evidence)
	evidence.ChallengeProofSHA256 = recordability.TargetedObjectChallengeProofSHA256(challenge, attemptID, mediaHead.ETag, mediaHead.VersionID, frameHead.ETag, frameHead.VersionID, evidence)
	evidence.Detail = fmt.Sprintf("valid_ratio=%.3f segments=%d native_signature_stable=true frame=true", evidence.ValidRatio, evidence.SegmentCount)
	mediaArchiveKey := fmt.Sprintf("protected/campaign-probe/%s/media.zip", attemptID)
	frameArchiveKey := fmt.Sprintf("protected/campaign-probe/%s/frame.jpg", attemptID)
	mediaArchiveSHA, err := sha256File(archivePath)
	if err != nil {
		return targetedQuarantineObservation{}, fmt.Errorf("hash targeted media archive: %w", err)
	}
	archiveFile, err := os.Open(archivePath)
	if err != nil {
		return targetedQuarantineObservation{}, fmt.Errorf("open targeted media archive for retention: %w", err)
	}
	mediaArchiveETag, putErr := store.PutReader(ctx, mediaArchiveKey, "application/zip", archiveFile)
	closeErr := archiveFile.Close()
	if putErr != nil || closeErr != nil {
		return targetedQuarantineObservation{}, fmt.Errorf("retain targeted media evidence")
	}
	frameArchiveETag, err := store.PutBytes(ctx, frameArchiveKey, "image/jpeg", frameBody)
	if err != nil {
		return targetedQuarantineObservation{}, fmt.Errorf("retain targeted frame evidence")
	}
	mediaArchiveHead, err := store.Head(ctx, mediaArchiveKey)
	if err != nil || mediaArchiveHead.ETag != mediaArchiveETag || mediaArchiveHead.SizeBytes != mediaHead.SizeBytes {
		return targetedQuarantineObservation{}, fmt.Errorf("verify retained targeted media evidence")
	}
	frameArchiveHead, err := store.Head(ctx, frameArchiveKey)
	if err != nil || frameArchiveHead.ETag != frameArchiveETag || frameArchiveHead.SizeBytes != frameHead.SizeBytes {
		return targetedQuarantineObservation{}, fmt.Errorf("verify retained targeted frame evidence")
	}
	return targetedQuarantineObservation{
		Evidence: evidence, MediaSizeBytes: mediaHead.SizeBytes, MediaETag: mediaHead.ETag, MediaVersionID: mediaHead.VersionID,
		FrameSizeBytes: frameHead.SizeBytes, FrameETag: frameHead.ETag, FrameVersionID: frameHead.VersionID,
		ArchiveBucketSHA256: hashSecret(store.Bucket()), MediaArchiveKey: mediaArchiveKey, MediaArchiveSHA256: mediaArchiveSHA,
		MediaArchiveETag: mediaArchiveHead.ETag, MediaArchiveVersion: mediaArchiveHead.VersionID,
		FrameArchiveKey: frameArchiveKey, FrameArchiveSHA256: frameEvidence.FrameSHA256,
		FrameArchiveETag: frameArchiveHead.ETag, FrameArchiveVersion: frameArchiveHead.VersionID,
	}, nil
}

func sha256File(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

type exactObjectOpener func(context.Context, string, string, string) (io.ReadCloser, error)

func copyExactObjectToFile(ctx context.Context, open exactObjectOpener, key, etag, versionID, path string, exactSize, maxSize int64) error {
	if exactSize <= 0 || exactSize > maxSize {
		return fmt.Errorf("targeted quarantine object size is out of bounds")
	}
	rc, err := open(ctx, key, etag, versionID)
	if err != nil {
		return err
	}
	defer rc.Close()
	out, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	written, copyErr := io.CopyN(out, rc, exactSize+1)
	closeErr := out.Close()
	if copyErr != nil && !errors.Is(copyErr, io.EOF) {
		return copyErr
	}
	if closeErr != nil {
		return closeErr
	}
	if written != exactSize {
		return fmt.Errorf("targeted quarantine object changed during exact read")
	}
	return nil
}

func readExactObjectBounded(ctx context.Context, open exactObjectOpener, key, etag, versionID string, exactSize, maxSize int64) ([]byte, error) {
	if exactSize <= 0 || exactSize > maxSize {
		return nil, fmt.Errorf("targeted quarantine object size is out of bounds")
	}
	rc, err := open(ctx, key, etag, versionID)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	body, err := io.ReadAll(io.LimitReader(rc, exactSize+1))
	if err != nil || int64(len(body)) != exactSize {
		return nil, fmt.Errorf("targeted quarantine object changed during exact read")
	}
	return body, nil
}

func targetedEvidenceEqual(a, b recordability.TargetedEvidence) bool {
	return a.Result == b.Result && a.Detail == b.Detail && a.ValidRatio == b.ValidRatio && a.DurationMs == b.DurationMs && a.SegmentCount == b.SegmentCount &&
		a.FrameSHA256 == b.FrameSHA256 && a.MediaSHA256 == b.MediaSHA256 && a.NativeSignatureSHA256 == b.NativeSignatureSHA256 && a.ChallengeProofSHA256 == b.ChallengeProofSHA256 &&
		a.VideoCodec == b.VideoCodec && a.AudioCodec == b.AudioCodec && a.AudioPresent == b.AudioPresent && a.VideoWidth == b.VideoWidth && a.VideoHeight == b.VideoHeight &&
		((a.ActualFPS == nil && b.ActualFPS == nil) || (a.ActualFPS != nil && b.ActualFPS != nil && *a.ActualFPS == *b.ActualFPS))
}
