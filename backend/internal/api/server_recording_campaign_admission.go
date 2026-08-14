package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

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
		JOIN LATERAL (SELECT e.*,pa.probe_build_sha,pa.source_revision_id,pa.source_url_sha256,pa.source_page_url_sha256,pa.source_updated_at,pa.challenge FROM recording_targeted_probe_evidence e JOIN recording_targeted_probe_attempts pa ON pa.id=e.attempt_id WHERE e.approval_id=a.id AND e.stream_id=ar.stream_id AND e.result='ok' ORDER BY e.observed_at,e.id LIMIT 1) first_e ON true
		JOIN LATERAL (SELECT e.*,pa.probe_build_sha,pa.source_revision_id,pa.source_url_sha256,pa.source_page_url_sha256,pa.source_updated_at,pa.challenge FROM recording_targeted_probe_evidence e JOIN recording_targeted_probe_attempts pa ON pa.id=e.attempt_id WHERE e.approval_id=a.id AND e.stream_id=ar.stream_id AND e.result='ok' AND e.id<>first_e.id ORDER BY e.observed_at,e.id LIMIT 1) second_e ON true
		WHERE a.id=$1 AND a.account_id=$2
		  AND (a.failure_domain_tag IS NULL OR a.failure_domain_tag=ANY(s.tags))
		  AND first_e.probe_build_sha=$3 AND second_e.probe_build_sha=$3
		  AND second_e.observed_at>first_e.observed_at
		  AND first_e.observed_at>=transaction_timestamp()-interval '6 hours'
		  AND second_e.observed_at>=transaction_timestamp()-interval '6 hours'
		  AND first_e.challenge<>second_e.challenge
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

func persistCampaignAdmission(ctx context.Context, tx pgx.Tx, approvalID uuid.UUID, accountID, actorUserID int64, deadline time.Time, streams []batchStream, items []batchScheduleItem) (int64, error) {
	recordingByStream := make(map[int64]int64, len(items))
	for _, item := range items {
		recordingByStream[item.StreamID] = item.RecordingID
	}
	var trackID int64
	err := tx.QueryRow(ctx, `INSERT INTO recording_campaign_tracks(account_id,campaign_key,label,deadline_at,target_count,grade_floor,required_consecutive_windows,created_by_user_id) VALUES($1,$2,$3,$4,$5,'GOOD',0,$6) RETURNING id`,
		accountID, "targeted-admission-"+approvalID.String(), "Approved targeted admission "+approvalID.String()[:8], deadline, len(streams), actorUserID).Scan(&trackID)
	if err != nil {
		return 0, fmt.Errorf("create protected campaign track: %w", err)
	}
	for rank, stream := range streams {
		recordingID := recordingByStream[stream.id]
		if recordingID <= 0 {
			return 0, fmt.Errorf("stream %d has no scheduled recording", stream.id)
		}
		var rosterID int64
		err = tx.QueryRow(ctx, `
			INSERT INTO recording_campaign_roster_entries(track_id,recording_id,stream_id,scene_identity_sha256,role,rank,status,reason_codes,effective_at,decision_at,evidence_observed_at,evidence_sha256,updated_by_user_id)
			VALUES($1,$2,$3,$4,'primary',$5,'probation',ARRAY['deniz_approved','targeted_do_two_pass','source_fenced'],transaction_timestamp(),transaction_timestamp(),$6,$7,$8)
			RETURNING id`, trackID, recordingID, stream.id, stream.sceneIdentity, rank+1, stream.evidenceObservedAt, stream.evidenceSHA, actorUserID).Scan(&rosterID)
		if err != nil {
			return 0, fmt.Errorf("protect admitted stream %d: %w", stream.id, err)
		}
		_, err = tx.Exec(ctx, `
			INSERT INTO recording_campaign_admission_results(approval_id,first_probe_evidence_id,second_probe_evidence_id,account_id,track_id,roster_entry_id,stream_id,recording_id,actor_user_id,schedule_sha256,recording_config_sha256)
			VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`, approvalID, stream.admissionEvidenceID, stream.admissionEvidenceID2, accountID, trackID, rosterID, stream.id, recordingID, actorUserID, stream.scheduleSHA, strings.Repeat("0", 64))
		if err != nil {
			return 0, fmt.Errorf("seal admitted stream %d: %w", stream.id, err)
		}
	}
	if _, err := tx.Exec(ctx, `SELECT transition_recording_campaign_track($1,'active',ARRAY['deniz_approved','targeted_do_two_pass','atomic_schedule_protection'],$2,transaction_timestamp())`, trackID, actorUserID); err != nil {
		return 0, fmt.Errorf("activate protected campaign track: %w", err)
	}
	return trackID, nil
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
		out[i].NormalizedLabel = normalizeCampaignAdmissionLabel(out[i].NormalizedLabel)
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

func normalizeCampaignAdmissionLabel(v string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(strings.TrimSpace(v)) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		}
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
	if req.Schedule.StartAt == nil || req.Schedule.EndAt == nil || !req.Schedule.EndAt.After(req.Schedule.StartAt.UTC()) || req.Schedule.EndAt.After(req.DeadlineAt.UTC()) || len(req.Schedule.ActiveWeekdays) == 0 {
		util.WriteError(w, http.StatusBadRequest, "approved schedule requires explicit bounded start/end/weekdays within the approval deadline")
		return
	}
	req.Schedule.TargetAccountID = p.AccountID
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
	if _, err := tx.Exec(r.Context(), `INSERT INTO recording_campaign_admission_tx_authorizations(transaction_id,action,account_id,actor_user_id,account_session_id) VALUES(txid_current(),'approve',$1,$2,$3)`, p.AccountID, p.UserID, *p.SessionID); err != nil {
		util.WriteError(w, http.StatusForbidden, "campaign approval session authorization failed")
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
		if sourceURL != entry.SourceURL || sourcePageURL != entry.SourcePageURL || sourceRevisionID != entry.SourceRevisionID || provider != entry.Provider || externalID != entry.ExternalID || normalizeCampaignAdmissionLabel(streamName) != entry.NormalizedLabel || streamTimezone != req.Schedule.StreamTimezones[entryIndex].Timezone {
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
		if err = tx.QueryRow(r.Context(), `SELECT EXISTS(SELECT 1 FROM recording_scene_frame_evidence WHERE id=$1 AND account_id=$2 AND stream_id=$3 AND scene_identity_sha256=$4 AND verified_at>=transaction_timestamp()-interval '6 hours' AND verified_at<=transaction_timestamp())`, entry.SceneFrameEvidenceID, p.AccountID, entry.StreamID, entry.SceneIdentitySHA256).Scan(&sceneBound); err != nil || !sceneBound {
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
			        lower(regexp_replace(active_stream.name,'[^[:alnum:]]','','g'))=lower(regexp_replace(candidate.name,'[^[:alnum:]]','','g')))
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
			       lower(regexp_replace(a.name,'[^[:alnum:]]','','g'))=lower(regexp_replace(b.name,'[^[:alnum:]]','','g')))
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
	var id, digest string
	err = tx.QueryRow(r.Context(), `
		WITH payload AS (SELECT $1::bigint account_id,$2::bigint actor_user_id,$3::text actor_email,$4::text authority_code,NULLIF($5::text,'') failure_domain_tag,$6::timestamptz deadline_at,$7::jsonb entries,$8::jsonb schedule_spec),
		hashed AS (SELECT *,encode(sha256(convert_to(schedule_spec::text,'UTF8')),'hex') schedule_sha256 FROM payload)
		INSERT INTO recording_campaign_admission_approvals(request_id,account_id,actor_user_id,actor_email_snapshot,authority_code,failure_domain_tag,deadline_at,entries,schedule_spec,schedule_sha256,approval_sha256)
		SELECT $9,account_id,actor_user_id,actor_email,authority_code,failure_domain_tag,deadline_at,entries,schedule_spec,schedule_sha256,
		       encode(sha256(convert_to(jsonb_build_object('account_id',account_id,'actor_user_id',actor_user_id,'actor_email',lower(actor_email),'authority_code',authority_code,'failure_domain_tag',failure_domain_tag,'deadline_epoch',extract(epoch from deadline_at),'entries',entries,'schedule_sha256',schedule_sha256)::text,'UTF8')),'hex')
		FROM hashed ON CONFLICT DO NOTHING RETURNING id::text,approval_sha256`, p.AccountID, p.UserID, strings.ToLower(strings.TrimSpace(p.Email)), req.AuthorityCode, req.FailureDomainTag, req.DeadlineAt.UTC(), rawEntries, rawSchedule, requestID).Scan(&id, &digest)
	if errors.Is(err, pgx.ErrNoRows) {
		err = tx.QueryRow(r.Context(), `SELECT id::text,approval_sha256 FROM recording_campaign_admission_approvals WHERE account_id=$1 AND request_id=$2 AND actor_user_id=$3 AND actor_email_snapshot=$4 AND authority_code=$5 AND failure_domain_tag IS NOT DISTINCT FROM NULLIF($6,'') AND deadline_at=$7 AND entries=$8::jsonb AND schedule_spec=$9::jsonb`, p.AccountID, requestID, p.UserID, strings.ToLower(strings.TrimSpace(p.Email)), req.AuthorityCode, req.FailureDomainTag, req.DeadlineAt.UTC(), rawEntries, rawSchedule).Scan(&id, &digest)
	}
	if err != nil {
		util.WriteError(w, http.StatusConflict, "campaign admission approval was not persisted")
		return
	}
	if err = tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusConflict, "commit campaign admission approval")
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

func (s *Server) loadManagedProbeRecorder(ctx context.Context, tx pgx.Tx, p nodePrincipal) (managedProbeRecorder, error) {
	if p.NodeType != nodeTypeLocalRecorder {
		return managedProbeRecorder{}, fmt.Errorf("targeted campaign probes require a managed DO recorder")
	}
	var d managedProbeRecorder
	var qualified bool
	err := tx.QueryRow(ctx, `SELECT id,do_droplet_id,region,build_sha,state='active' AND last_seen_at BETWEEN transaction_timestamp()-interval '120 seconds' AND transaction_timestamp()+interval '30 seconds' FROM recorder_droplets WHERE node_id=$1 AND name=$2 FOR SHARE`, p.NodeID, strings.TrimSpace(p.DisplayName)).Scan(&d.ID, &d.DODropletID, &d.Region, &d.BuildSHA, &qualified)
	if err != nil {
		return managedProbeRecorder{}, err
	}
	if !qualified || d.DODropletID <= 0 || d.BuildSHA != strings.ToLower(strings.TrimSpace(s.cfg.DropletPoolBuildSHA)) {
		return managedProbeRecorder{}, fmt.Errorf("managed DO recorder attestation is stale or not on the exact server build")
	}
	return d, nil
}

func (s *Server) handleRecordingCampaignAdmissionTargets(w http.ResponseWriter, r *http.Request) {
	p, ok := nodePrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	var req campaignAdmissionTargetsRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	approvalID, err := uuid.Parse(strings.TrimSpace(req.ApprovalID))
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, "approval_id must be a UUID")
		return
	}
	ids, err := uniqueBatchStreamIDs(req.StreamIDs)
	requestID, requestErr := uuid.Parse(strings.TrimSpace(req.RequestID))
	if err != nil || requestErr != nil || len(ids) != 1 {
		util.WriteError(w, http.StatusBadRequest, "exactly one unique stream_id and a UUID request_id are required")
		return
	}
	tx, err := s.pool.BeginTx(r.Context(), pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "begin targeted probe target load")
		return
	}
	defer tx.Rollback(r.Context())
	droplet, err := s.loadManagedProbeRecorder(r.Context(), tx, p)
	if err != nil {
		util.WriteError(w, http.StatusConflict, err.Error())
		return
	}
	if p.TokenID <= 0 {
		util.WriteError(w, http.StatusUnauthorized, "targeted attempt requires an authenticated node token identity")
		return
	}
	if _, err := tx.Exec(r.Context(), `INSERT INTO recording_campaign_admission_tx_authorizations(transaction_id,action,approval_id,account_id,node_id,node_token_id) VALUES(txid_current(),'attempt',$1,$2,$3,$4)`, approvalID, p.AccountID, p.NodeID, p.TokenID); err != nil {
		util.WriteError(w, http.StatusForbidden, "targeted attempt node authorization failed")
		return
	}
	var target recordability.Target
	var revisionID *int64
	var sourceHash, pageHash string
	var sourceUpdated time.Time
	err = tx.QueryRow(r.Context(), `
		SELECT s.id,COALESCE(s.provider,''),s.source_url,COALESCE(s.source_page_url,''),ar.source_revision_id,ar.source_url_sha256,ar.source_page_url_sha256,ar.source_updated_at
		FROM recording_campaign_admission_approvals a
		JOIN recording_campaign_admission_reservations ar ON ar.approval_id=a.id
		JOIN streams s ON s.id=ar.stream_id
		WHERE a.id=$1 AND a.account_id=$3 AND a.deadline_at>transaction_timestamp() AND ar.stream_id=$2
		  AND ar.source_revision_id IS NOT DISTINCT FROM (SELECT max(id) FROM stream_source_revisions WHERE stream_id=s.id)
		  AND ar.source_url_sha256=encode(sha256(convert_to(s.source_url,'UTF8')),'hex')
		  AND ar.source_page_url_sha256=encode(sha256(convert_to(COALESCE(s.source_page_url,''),'UTF8')),'hex')
		  AND ar.source_updated_at=s.updated_at
		  AND NOT EXISTS(SELECT 1 FROM recording_campaign_admission_source_fence_events f WHERE f.stream_id=ar.stream_id AND f.occurred_at>=ar.reserved_at)
		FOR UPDATE OF ar`, approvalID, ids[0], p.AccountID).Scan(&target.ID, &target.Provider, &target.SourceURL, &target.SourcePageURL, &revisionID, &sourceHash, &pageHash, &sourceUpdated)
	if err != nil {
		util.WriteError(w, http.StatusConflict, "requested stream set does not exactly match a live source-fenced approval")
		return
	}
	replayErr := tx.QueryRow(r.Context(), `SELECT id::text,challenge FROM recording_targeted_probe_attempts WHERE account_id=$1 AND request_id=$2 AND approval_id=$3 AND stream_id=$4 AND node_id=$5 AND probe_build_sha=$6`, p.AccountID, requestID, approvalID, target.ID, p.NodeID, droplet.BuildSHA).Scan(&target.AttemptID, &target.Challenge)
	if replayErr == nil {
		if err := tx.Commit(r.Context()); err != nil {
			util.WriteError(w, http.StatusConflict, "commit targeted attempt replay")
			return
		}
		util.WriteJSON(w, http.StatusOK, map[string]any{"approval_id": approvalID, "targets": []recordability.Target{target}})
		return
	}
	if !errors.Is(replayErr, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusConflict, "load targeted attempt replay")
		return
	}
	challenge := hashSecret(uuid.NewString())
	err = tx.QueryRow(r.Context(), `
		INSERT INTO recording_targeted_probe_attempts(request_id,approval_id,account_id,stream_id,attempt_no,node_id,recorder_droplet_id,do_droplet_id,region,probe_build_sha,source_revision_id,source_url_sha256,source_page_url_sha256,source_updated_at,challenge,expires_at)
		SELECT $1,$2,$3,$4,COALESCE(max(attempt_no),0)+1,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,transaction_timestamp()+interval '15 minutes'
		FROM recording_targeted_probe_attempts WHERE approval_id=$2 AND stream_id=$4
		ON CONFLICT(account_id,request_id) DO NOTHING
		RETURNING id::text,challenge`, requestID, approvalID, p.AccountID, target.ID, p.NodeID, droplet.ID, droplet.DODropletID, droplet.Region, droplet.BuildSHA, revisionID, sourceHash, pageHash, sourceUpdated, challenge).Scan(&target.AttemptID, &target.Challenge)
	if errors.Is(err, pgx.ErrNoRows) {
		err = tx.QueryRow(r.Context(), `SELECT id::text,challenge FROM recording_targeted_probe_attempts WHERE account_id=$1 AND request_id=$2 AND approval_id=$3 AND stream_id=$4 AND node_id=$5 AND probe_build_sha=$6`, p.AccountID, requestID, approvalID, target.ID, p.NodeID, droplet.BuildSHA).Scan(&target.AttemptID, &target.Challenge)
	}
	if err != nil {
		util.WriteError(w, http.StatusConflict, "targeted attempt replay did not match the exact request")
		return
	}
	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusConflict, "commit targeted attempt")
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"approval_id": approvalID, "targets": []recordability.Target{target}})
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
	tx, err := s.pool.BeginTx(r.Context(), pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "begin targeted evidence insert")
		return
	}
	defer tx.Rollback(r.Context())
	droplet, err := s.loadManagedProbeRecorder(r.Context(), tx, p)
	if err != nil {
		util.WriteError(w, http.StatusConflict, err.Error())
		return
	}
	if p.TokenID <= 0 {
		util.WriteError(w, http.StatusUnauthorized, "targeted evidence requires an authenticated node token identity")
		return
	}
	if _, err := tx.Exec(r.Context(), `INSERT INTO recording_campaign_admission_tx_authorizations(transaction_id,action,approval_id,account_id,node_id,node_token_id) VALUES(txid_current(),'evidence',$1,$2,$3,$4)`, approvalID, p.AccountID, p.NodeID, p.TokenID); err != nil {
		util.WriteError(w, http.StatusForbidden, "targeted evidence node authorization failed")
		return
	}
	var accountID int64
	var challenge string
	err = tx.QueryRow(r.Context(), `SELECT account_id,challenge FROM recording_targeted_probe_attempts WHERE id=$1 AND request_id=$2 AND approval_id=$3 AND stream_id=$4 AND account_id=$5 AND node_id=$6 AND recorder_droplet_id=$7 AND probe_build_sha=$8 AND expires_at>=transaction_timestamp() FOR UPDATE`, attemptID, requestID, approvalID, req.StreamID, p.AccountID, p.NodeID, droplet.ID, droplet.BuildSHA).Scan(&accountID, &challenge)
	if err != nil {
		util.WriteError(w, http.StatusConflict, "campaign approval is missing, expired, or does not reserve this stream")
		return
	}
	nativeSignature := recordability.TargetedNativeSignatureSHA256(req.Evidence)
	challengeProof := recordability.TargetedChallengeProofSHA256(challenge, req.Evidence)
	if req.Evidence.NativeSignatureSHA256 != nativeSignature || req.Evidence.ChallengeProofSHA256 != challengeProof {
		util.WriteError(w, http.StatusConflict, "targeted evidence does not match the server challenge and typed native facts")
		return
	}
	actualFPS := any(nil)
	if req.Evidence.ActualFPS != nil {
		actualFPS = *req.Evidence.ActualFPS
	}
	var evidenceID string
	var evidenceSHA string
	err = tx.QueryRow(r.Context(), `
		INSERT INTO recording_targeted_probe_evidence(attempt_id,approval_id,account_id,stream_id,result,valid_ratio,duration_ms,segment_count,frame_sha256,media_sha256,native_signature_sha256,challenge_proof_sha256,video_codec,audio_codec,audio_present,video_width,video_height,actual_fps,detail,evidence_sha256)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,NULLIF($9,''),NULLIF($10,''),NULLIF($11,''),NULLIF($12,''),NULLIF($13,''),NULLIF($14,''),$15,NULLIF($16,0),NULLIF($17,0),$18,$19,$20)
		ON CONFLICT(attempt_id) DO NOTHING
		RETURNING id::text,evidence_sha256`, attemptID, approvalID, accountID, req.StreamID, req.Evidence.Result, req.Evidence.ValidRatio, req.Evidence.DurationMs, req.Evidence.SegmentCount, req.Evidence.FrameSHA256, req.Evidence.MediaSHA256, nativeSignature, challengeProof,
		req.Evidence.VideoCodec, req.Evidence.AudioCodec, req.Evidence.AudioPresent, req.Evidence.VideoWidth, req.Evidence.VideoHeight, actualFPS, req.Evidence.Detail, strings.Repeat("0", 64)).Scan(&evidenceID, &evidenceSHA)
	if errors.Is(err, pgx.ErrNoRows) {
		var stored recordability.TargetedEvidence
		err = tx.QueryRow(r.Context(), `SELECT id::text,result,detail,valid_ratio,duration_ms,segment_count,COALESCE(frame_sha256,''),COALESCE(media_sha256,''),COALESCE(native_signature_sha256,''),COALESCE(challenge_proof_sha256,''),COALESCE(video_codec,''),COALESCE(audio_codec,''),COALESCE(audio_present,false),COALESCE(video_width,0),COALESCE(video_height,0),actual_fps,evidence_sha256 FROM recording_targeted_probe_evidence WHERE attempt_id=$1`, attemptID).Scan(&evidenceID, &stored.Result, &stored.Detail, &stored.ValidRatio, &stored.DurationMs, &stored.SegmentCount, &stored.FrameSHA256, &stored.MediaSHA256, &stored.NativeSignatureSHA256, &stored.ChallengeProofSHA256, &stored.VideoCodec, &stored.AudioCodec, &stored.AudioPresent, &stored.VideoWidth, &stored.VideoHeight, &stored.ActualFPS, &evidenceSHA)
		if err == nil && !targetedEvidenceEqual(stored, req.Evidence) {
			err = fmt.Errorf("attempt already has different evidence")
		}
	}
	if err != nil {
		util.WriteError(w, http.StatusConflict, "targeted evidence failed server/DB attestation")
		return
	}
	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusConflict, "commit targeted evidence")
		return
	}
	util.WriteJSON(w, http.StatusCreated, map[string]any{"evidence_id": evidenceID, "attempt_id": attemptID, "evidence_sha256": evidenceSHA})
}

func targetedEvidenceEqual(a, b recordability.TargetedEvidence) bool {
	return a.Result == b.Result && a.Detail == b.Detail && a.ValidRatio == b.ValidRatio && a.DurationMs == b.DurationMs && a.SegmentCount == b.SegmentCount &&
		a.FrameSHA256 == b.FrameSHA256 && a.MediaSHA256 == b.MediaSHA256 && a.NativeSignatureSHA256 == b.NativeSignatureSHA256 && a.ChallengeProofSHA256 == b.ChallengeProofSHA256 &&
		a.VideoCodec == b.VideoCodec && a.AudioCodec == b.AudioCodec && a.AudioPresent == b.AudioPresent && a.VideoWidth == b.VideoWidth && a.VideoHeight == b.VideoHeight &&
		((a.ActualFPS == nil && b.ActualFPS == nil) || (a.ActualFPS != nil && b.ActualFPS != nil && *a.ActualFPS == *b.ActualFPS))
}
