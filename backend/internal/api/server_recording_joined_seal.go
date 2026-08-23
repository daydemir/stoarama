package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/daydemir/stoarama/backend/internal/joinedauth"
	"github.com/daydemir/stoarama/backend/internal/joinedrecording"
	"github.com/daydemir/stoarama/backend/internal/util"
)

func (s *Server) handleJoinedSealHour(w http.ResponseWriter, r *http.Request) {
	if !s.joinedControlPlaneReady() {
		util.WriteError(w, http.StatusServiceUnavailable, "joined recording is disabled")
		return
	}
	var req joinedrecording.SealHourRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	claims, ok := joinedWorkerClaimsFromContext(r.Context())
	if !ok || claims.Kind != joinedauth.KindOperation || claims.Operation != joinedauth.OperationPreflight ||
		claims.SubjectKind != joinedauth.SubjectHour || claims.SubjectID != req.HourID {
		util.WriteError(w, http.StatusForbidden, "joined preflight token scope differs")
		return
	}
	preflightToken, err := joinedCapabilityToken(claims.LeaseToken)
	if err != nil {
		util.WriteError(w, http.StatusForbidden, "joined preflight lease differs")
		return
	}
	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "begin joined hour seal")
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()
	claim, ledger, ledgerArtifactID, hourRecordID, workerID, err := loadJoinedSealFacts(r.Context(), tx, claims.BatchID,
		req.HourID, preflightToken)
	if err != nil {
		util.WriteError(w, http.StatusConflict, "joined preflight lease is stale or foreign")
		return
	}
	if err := req.Validate(claim.RecordingID, claim.MediaTool.IdentitySHA256); err != nil ||
		req.SourceClaimSHA256 != claim.SourceClaimSHA256 || !sameFrozenJoinedSources(claim.Sources, req.AccountedSources) {
		util.WriteError(w, http.StatusConflict, "joined hour seal source identity differs")
		return
	}
	plan, built, err := buildJoinedSealedPlan(claim, req)
	if err != nil {
		util.WriteError(w, http.StatusConflict, fmt.Sprintf("build canonical joined hour: %v", err))
		return
	}
	mediaIDs, err := insertJoinedMediaArtifacts(r.Context(), tx, hourRecordID, claim, plan, req, preflightToken)
	if err != nil {
		util.WriteError(w, http.StatusConflict, fmt.Sprintf("seal joined media identities: %v", err))
		return
	}
	allocation, err := joinedrecording.BuildHourManifestAllocation(ledgerArtifactID, plan, ledger)
	if err != nil {
		util.WriteError(w, http.StatusConflict, fmt.Sprintf("bind joined allocation ledger: %v", err))
		return
	}
	manifest, manifestBytes, manifestSHA, err := joinedrecording.BuildHourManifest(joinedrecording.HourManifestInput{
		Plan: plan, Allocation: allocation, MediaArtifactIDs: mediaIDs, Built: built,
		QuarantineEvidence: req.Quarantine, AllocationLedger: ledger,
	})
	if err != nil {
		util.WriteError(w, http.StatusConflict, fmt.Sprintf("build canonical joined manifest: %v", err))
		return
	}
	planJSON, err := json.Marshal(plan)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "marshal joined hour plan")
		return
	}
	var sealedID int64
	err = tx.QueryRow(r.Context(), `UPDATE recording_joined_hours SET state='sealed',claim_token=NULL,claimed_by=NULL,
		lease_expires_at=NULL,heartbeat_at=NULL,source_only_sha256=$3,canonical_plan=$4,manifest_bytes=$5,
		manifest_sha256=$6,sealed_at=now() WHERE id=$1 AND state='leased' AND claim_token=$2 AND lease_expires_at>now()
		  AND EXISTS(SELECT 1 FROM connections c WHERE c.id=recording_joined_hours.connection_id AND c.joined_protocol_version=1)
		RETURNING id`, hourRecordID, preflightToken, req.SourceClaimSHA256, planJSON, manifestBytes, manifestSHA).Scan(&sealedID)
	if err != nil {
		util.WriteError(w, http.StatusConflict, "joined hour seal lease changed")
		return
	}
	publicationToken := uuid.New()
	var leaseExpires time.Time
	relativePath := "coverage/hours/" + plan.HourID + ".json"
	objectKey := "joined/" + plan.BatchID + "/" + relativePath
	var manifestID int64
	err = tx.QueryRow(r.Context(), `INSERT INTO recording_joined_artifacts(batch_record_id,account_id,connection_id,batch_id,
		scope_kind,scope_id,stream_day_id,hour_record_id,artifact_kind,ordinal,relative_path,object_key,content_type,
		expected_size_bytes,expected_sha256,canonical_bytes,publication_state,publication_attempt_count,publication_token,
		publication_claimed_by,publication_lease_expires_at,publication_heartbeat_at)
		SELECT h.batch_record_id,h.account_id,h.connection_id,h.batch_id,'hour',h.hour_id,h.stream_day_id,h.id,
		  'hour_manifest',1,$2,$3,'application/json',$4,$5,$6,'publishing',1,$7,$8,date_trunc('second',now()+$9::interval),now()
		FROM recording_joined_hours h WHERE h.id=$1 RETURNING id,publication_lease_expires_at`, hourRecordID, relativePath, objectKey,
		int64(len(manifestBytes)), manifestSHA, manifestBytes, publicationToken, workerID, joinedLeaseDuration.String()).Scan(&manifestID, &leaseExpires)
	if err != nil {
		util.WriteError(w, http.StatusConflict, fmt.Sprintf("insert joined hour manifest: %v", err))
		return
	}
	operationToken, err := joinedauth.MintOperation(s.cfg.JoinedWorkerSigningKey, plan.BatchID, joinedauth.SubjectHour,
		plan.HourID, publicationToken, joinedauth.OperationPublish, leaseExpires)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "mint joined publication token")
		return
	}
	authority, err := joinedOutputAuthority(s.cfg.R2Endpoint)
	if err != nil {
		util.WriteError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	response := joinedrecording.WorkerClaim{ProtocolVersion: joinedWorkerProtocolVersion, HourID: plan.HourID,
		LeaseID: joinedauth.LeaseID(publicationToken), OperationToken: operationToken, LeaseExpires: leaseExpires,
		StorageAuthority: authority, StorageBucket: s.cfg.R2Bucket, Plan: plan, Allocation: manifest.Allocation,
		AllocationLedger: ledger, HourManifest: manifest, MediaArtifactIDs: mediaIDs, HourManifestArtifactID: manifestID,
		HourManifestExpectedSize: int64(len(manifestBytes)), HourManifestExpectedSHA: manifestSHA}
	if err := response.Validate(time.Now().UTC()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("validate joined hour publication: %v", err))
		return
	}
	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusConflict, "commit joined hour seal")
		return
	}
	util.WriteJSON(w, http.StatusOK, response)
}

func joinedOutputAuthority(raw string) (string, error) {
	parsed, err := url.ParseRequestURI(raw)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.Hostname() == "" || parsed.User != nil ||
		(parsed.Path != "" && parsed.Path != "/") || parsed.RawPath != "" || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", errors.New("joined output storage authority is invalid")
	}
	return parsed.Host, nil
}

func loadJoinedSealFacts(ctx context.Context, tx pgx.Tx, batchID, hourID string, token uuid.UUID) (
	joinedrecording.PreflightHourClaim, joinedrecording.StreamDayAllocation, int64, int64, string, error) {
	var claim joinedrecording.PreflightHourClaim
	var ledger joinedrecording.StreamDayAllocation
	var hourRecordID, ledgerArtifactID int64
	var metadataJSON, qualificationJSON, mediaToolJSON, ledgerJSON []byte
	var workerID string
	err := tx.QueryRow(ctx, `SELECT h.id,h.batch_id,b.generation,h.recording_id,br.timezone,h.local_date::text,h.delivery_hour,
		br.folder_name,br.naming_metadata,d.ledger_sha256,br.qualification,b.media_tool,h.source_claim_sha256,
		d.ledger_bytes,ledger.id,h.claimed_by
		FROM recording_joined_hours h JOIN recording_joined_batches b ON b.id=h.batch_record_id
		JOIN recording_joined_batch_recordings br ON br.batch_record_id=b.id AND br.recording_id=h.recording_id
		JOIN recording_joined_stream_days d ON d.id=h.stream_day_id
		JOIN recording_joined_artifacts ledger ON ledger.stream_day_id=d.id AND ledger.artifact_kind='allocation_ledger'
		JOIN connections c ON c.id=h.connection_id AND c.joined_protocol_version=1
		WHERE h.hour_id=$1 AND h.batch_id=$2 AND b.state='frozen' AND h.state='leased' AND h.claim_token=$3 AND h.lease_expires_at>now()
		  AND ledger.publication_state='published' FOR UPDATE OF h,c`, hourID, batchID, token).Scan(&hourRecordID,
		&claim.BatchID, &claim.Generation, &claim.RecordingID, &claim.Timezone, &claim.LocalDate, &claim.LocalHour,
		&claim.FolderName, &metadataJSON, &claim.AllocationLedgerSHA, &qualificationJSON, &mediaToolJSON,
		&claim.SourceClaimSHA256, &ledgerJSON, &ledgerArtifactID, &workerID)
	if err != nil || json.Unmarshal(metadataJSON, &claim.Metadata) != nil || json.Unmarshal(qualificationJSON, &claim.Qualification) != nil ||
		json.Unmarshal(mediaToolJSON, &claim.MediaTool) != nil || json.Unmarshal(ledgerJSON, &ledger) != nil {
		return joinedrecording.PreflightHourClaim{}, joinedrecording.StreamDayAllocation{}, 0, 0, "", errors.New("load joined seal facts")
	}
	rows, err := tx.Query(ctx, `SELECT clip_id,recording_id,recording_job_id,storage_destination_id,provider,endpoint,region,bucket,start_at,end_at,
		released_at,object_key,version_id,etag,size_bytes,sha256 FROM recording_joined_sources WHERE hour_record_id=$1 ORDER BY hour_ordinal`, hourRecordID)
	if err != nil {
		return claim, ledger, ledgerArtifactID, hourRecordID, workerID, err
	}
	defer rows.Close()
	for rows.Next() {
		var source joinedrecording.SourceClip
		if err := rows.Scan(&source.ClipID, &source.RecordingID, &source.RecordingJobID, &source.StorageDestinationID,
			&source.Provider, &source.Endpoint, &source.Region, &source.Bucket, &source.StartUTC, &source.EndUTC,
			&source.ReleasedAt, &source.Object.Key, &source.Object.VersionID,
			&source.Object.ETag, &source.Object.SizeBytes, &source.Object.SHA256); err != nil {
			return claim, ledger, ledgerArtifactID, hourRecordID, workerID, err
		}
		source.StartUTC, source.EndUTC = source.StartUTC.UTC(), source.EndUTC.UTC()
		if source.ReleasedAt != nil {
			releasedAt := source.ReleasedAt.UTC()
			source.ReleasedAt = &releasedAt
		}
		claim.Sources = append(claim.Sources, source)
	}
	return claim, ledger, ledgerArtifactID, hourRecordID, workerID, rows.Err()
}

func sameFrozenJoinedSources(frozen, accounted []joinedrecording.SourceClip) bool {
	if len(frozen) != len(accounted) {
		return false
	}
	for i := range frozen {
		left, right := frozen[i], accounted[i]
		if (left.ReleasedAt == nil) != (right.ReleasedAt == nil) ||
			(left.ReleasedAt != nil && !left.ReleasedAt.Equal(*right.ReleasedAt)) {
			return false
		}
		left.ReleasedAt, right.ReleasedAt = nil, nil
		right.AudioContract, right.SeamToPrevious = nil, joinedrecording.SeamEvidence{}
		if left != right {
			return false
		}
	}
	return true
}

func buildJoinedSealedPlan(claim joinedrecording.PreflightHourClaim, req joinedrecording.SealHourRequest) (
	joinedrecording.BatchPlan, []joinedrecording.BuiltOutput, error) {
	quarantinedIDs := map[int64]bool{}
	for _, evidence := range req.Quarantine {
		for _, clipID := range evidence.SourceClipIDs {
			quarantinedIDs[clipID] = true
		}
	}
	included, quarantined := []joinedrecording.SourceClip{}, []joinedrecording.SourceClip{}
	for _, source := range req.AccountedSources {
		if quarantinedIDs[source.ClipID] {
			quarantined = append(quarantined, source)
		} else {
			included = append(included, source)
		}
	}
	builtIdentities := make([]joinedrecording.BuiltArtifactIdentity, len(req.Media))
	built := make([]joinedrecording.BuiltOutput, len(req.Media))
	for i, media := range req.Media {
		builtIdentities[i] = joinedrecording.BuiltArtifactIdentity{SizeBytes: media.SizeBytes, SHA256: media.SHA256,
			MediaToolIdentity: claim.MediaTool.IdentitySHA256}
		built[i] = joinedrecording.BuiltOutput{SizeBytes: media.SizeBytes, SHA256: media.SHA256,
			SourceCount: len(media.SourceClipIDs), Verification: media.Verification, SplitEvidence: media.MaximalityEvidence}
	}
	planRequest := joinedrecording.PlanRequest{BatchID: claim.BatchID, Generation: claim.Generation,
		RecordingID: claim.RecordingID, Timezone: claim.Timezone, LocalDate: claim.LocalDate, DeliveryHour: claim.LocalHour,
		FolderName: claim.FolderName, Metadata: claim.Metadata, Qualification: claim.Qualification,
		AllocationLedgerSHA: claim.AllocationLedgerSHA, MediaTool: claim.MediaTool, Sources: included,
		QuarantinedSources: quarantined, BuiltArtifacts: builtIdentities}
	var plan joinedrecording.BatchPlan
	var err error
	if len(included) == 0 && len(quarantined) > 0 {
		planRequest.Sources = quarantined
		planRequest.QuarantinedSources = nil
		planRequest.BuiltArtifacts = nil
		plan, err = joinedrecording.BuildQuarantineOnlyHourPlan(planRequest, claim.LocalDate, claim.LocalHour,
			"deterministic_media_quarantine")
	} else {
		plan, err = joinedrecording.BuildPlan(planRequest)
	}
	if err != nil || len(plan.Outputs) != len(req.Media) {
		return joinedrecording.BatchPlan{}, nil, errors.New("worker media partition differs from canonical plan")
	}
	for i, output := range plan.Outputs {
		if len(output.Sources) != len(req.Media[i].SourceClipIDs) {
			return joinedrecording.BatchPlan{}, nil, errors.New("worker media source cardinality differs")
		}
		for j, source := range output.Sources {
			if source.ClipID != req.Media[i].SourceClipIDs[j] {
				return joinedrecording.BatchPlan{}, nil, errors.New("worker media source order differs")
			}
		}
	}
	return plan, built, nil
}

func insertJoinedMediaArtifacts(ctx context.Context, tx pgx.Tx, hourRecordID int64, claim joinedrecording.PreflightHourClaim,
	plan joinedrecording.BatchPlan, req joinedrecording.SealHourRequest, token uuid.UUID) ([]int64, error) {
	sourceIDs := map[int64]int64{}
	rows, err := tx.Query(ctx, `SELECT clip_id,id FROM recording_joined_sources WHERE hour_record_id=$1`, hourRecordID)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var clipID, sourceID int64
		if err := rows.Scan(&clipID, &sourceID); err != nil {
			rows.Close()
			return nil, err
		}
		sourceIDs[clipID] = sourceID
	}
	rows.Close()
	mediaIDs := make([]int64, len(plan.Outputs))
	for i, output := range plan.Outputs {
		err := tx.QueryRow(ctx, `INSERT INTO recording_joined_artifacts(batch_record_id,account_id,connection_id,batch_id,
			scope_kind,scope_id,stream_day_id,hour_record_id,artifact_kind,ordinal,relative_path,object_key,content_type,
			content_id,expected_size_bytes,expected_sha256)
			SELECT batch_record_id,account_id,connection_id,batch_id,'hour',hour_id,stream_day_id,id,'media',$2,$3,$4,
			  'video/mp4',$5,$6,$7 FROM recording_joined_hours WHERE id=$1 AND state='leased' AND claim_token=$8
			RETURNING id`, hourRecordID, output.Ordinal, output.RelativePath, output.ObjectKey, output.ContentID,
			output.ExpectedSize, output.ExpectedSHA, token).Scan(&mediaIDs[i])
		if err != nil {
			return nil, err
		}
		for ordinal, source := range output.Sources {
			sourceID := sourceIDs[source.ClipID]
			if sourceID == 0 {
				return nil, errors.New("joined media references foreign source")
			}
			if _, err := tx.Exec(ctx, `INSERT INTO recording_joined_media_sources(artifact_id,source_id,ordinal) VALUES($1,$2,$3)`,
				mediaIDs[i], sourceID, ordinal+1); err != nil {
				return nil, err
			}
			if _, err := tx.Exec(ctx, `INSERT INTO recording_joined_hour_dispositions
				(hour_record_id,source_id,disposition,media_artifact_id,media_ordinal) VALUES($1,$2,'included',$3,$4)`,
				hourRecordID, sourceID, mediaIDs[i], output.Ordinal); err != nil {
				return nil, err
			}
		}
	}
	for _, evidence := range req.Quarantine {
		raw, _ := json.Marshal(evidence)
		for _, clipID := range evidence.SourceClipIDs {
			sourceID := sourceIDs[clipID]
			if sourceID == 0 {
				return nil, errors.New("joined quarantine references foreign source")
			}
			if _, err := tx.Exec(ctx, `INSERT INTO recording_joined_hour_dispositions
				(hour_record_id,source_id,disposition,reason_code,quarantine_evidence)
				VALUES($1,$2,'quarantined',$3,$4)`, hourRecordID, sourceID, evidence.ReasonCode, raw); err != nil {
				return nil, err
			}
		}
	}
	return mediaIDs, nil
}

func sealJoinedGapOnlyHoursTx(ctx context.Context, tx pgx.Tx, ledgerArtifactID int64, scope joinedrecording.WorkScopeIdentity) error {
	var batchID string
	if err := tx.QueryRow(ctx, `SELECT batch_id FROM recording_joined_artifacts
		WHERE id=$1 AND artifact_kind='allocation_ledger'`, ledgerArtifactID).Scan(&batchID); err != nil {
		return err
	}
	if err := scope.Validate(batchID); err != nil {
		return fmt.Errorf("validate joined gap-only scope: %w", err)
	}
	allGapHours := scope.WorkScope == joinedrecording.WorkScopeFrozenBatch
	type gapHour struct {
		recordID int64
		claim    joinedrecording.PreflightHourClaim
		ledger   joinedrecording.StreamDayAllocation
	}
	rows, err := tx.Query(ctx, `SELECT h.id,h.batch_id,b.generation,h.hour_id,h.recording_id,br.timezone,h.local_date::text,
		h.delivery_hour,br.folder_name,br.naming_metadata,d.ledger_sha256,br.qualification,b.media_tool,
		h.source_claim_sha256,d.ledger_bytes
		FROM recording_joined_artifacts ledger JOIN recording_joined_stream_days d ON d.id=ledger.stream_day_id
		JOIN recording_joined_hours h ON h.stream_day_id=d.id JOIN recording_joined_batches b ON b.id=h.batch_record_id
		JOIN recording_joined_batch_recordings br ON br.id=d.batch_recording_id
		JOIN connections c ON c.id=h.connection_id AND c.joined_protocol_version=1
		WHERE ledger.id=$1 AND ledger.artifact_kind='allocation_ledger' AND ledger.publication_state='published'
		  AND b.state='frozen' AND h.state='pending' AND h.source_clip_count=0
		  AND ($2::boolean OR h.hour_id=ANY($3::text[])) ORDER BY h.delivery_hour
		FOR UPDATE OF h,c`, ledgerArtifactID, allGapHours, scope.CanaryHourIDs)
	if err != nil {
		return err
	}
	hours := []gapHour{}
	for rows.Next() {
		var hour gapHour
		var metadataJSON, qualificationJSON, mediaToolJSON, ledgerJSON []byte
		if err := rows.Scan(&hour.recordID, &hour.claim.BatchID, &hour.claim.Generation, &hour.claim.HourID,
			&hour.claim.RecordingID, &hour.claim.Timezone, &hour.claim.LocalDate, &hour.claim.LocalHour,
			&hour.claim.FolderName, &metadataJSON, &hour.claim.AllocationLedgerSHA, &qualificationJSON, &mediaToolJSON,
			&hour.claim.SourceClaimSHA256, &ledgerJSON); err != nil {
			rows.Close()
			return err
		}
		if json.Unmarshal(metadataJSON, &hour.claim.Metadata) != nil ||
			json.Unmarshal(qualificationJSON, &hour.claim.Qualification) != nil ||
			json.Unmarshal(mediaToolJSON, &hour.claim.MediaTool) != nil || json.Unmarshal(ledgerJSON, &hour.ledger) != nil {
			rows.Close()
			return errors.New("decode joined gap-only seal facts")
		}
		hours = append(hours, hour)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	for _, hour := range hours {
		plan, err := joinedrecording.BuildGapOnlyHourPlan(joinedrecording.PlanRequest{BatchID: hour.claim.BatchID,
			Generation: hour.claim.Generation, RecordingID: hour.claim.RecordingID, Timezone: hour.claim.Timezone,
			FolderName: hour.claim.FolderName, Metadata: hour.claim.Metadata, Qualification: hour.claim.Qualification,
			AllocationLedgerSHA: hour.claim.AllocationLedgerSHA, MediaTool: hour.claim.MediaTool}, hour.claim.LocalDate,
			hour.claim.LocalHour, "scheduled_source_gap")
		if err != nil || plan.HourID != hour.claim.HourID || plan.SourceClaimSHA256 != hour.claim.SourceClaimSHA256 {
			return errors.New("build canonical joined gap-only hour")
		}
		allocation, err := joinedrecording.BuildHourManifestAllocation(ledgerArtifactID, plan, hour.ledger)
		if err != nil {
			return err
		}
		_, manifestBytes, manifestSHA, err := joinedrecording.BuildHourManifest(joinedrecording.HourManifestInput{
			Plan: plan, Allocation: allocation, AllocationLedger: hour.ledger})
		if err != nil {
			return err
		}
		planJSON, err := json.Marshal(plan)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE recording_joined_hours SET state='sealed',source_only_sha256=$2,
			canonical_plan=$3,manifest_bytes=$4,manifest_sha256=$5,sealed_at=now() WHERE id=$1 AND state='pending'`,
			hour.recordID, plan.SourceClaimSHA256, planJSON, manifestBytes, manifestSHA); err != nil {
			return err
		}
		relativePath := "coverage/hours/" + plan.HourID + ".json"
		if _, err := tx.Exec(ctx, `INSERT INTO recording_joined_artifacts(batch_record_id,account_id,connection_id,batch_id,
			scope_kind,scope_id,stream_day_id,hour_record_id,artifact_kind,ordinal,relative_path,object_key,content_type,
			expected_size_bytes,expected_sha256,canonical_bytes,publication_state)
			SELECT batch_record_id,account_id,connection_id,batch_id,'hour',hour_id,stream_day_id,id,'hour_manifest',1,
			  $2,$3,'application/json',$4,$5,$6,'sealed' FROM recording_joined_hours WHERE id=$1`, hour.recordID,
			relativePath, "joined/"+plan.BatchID+"/"+relativePath, len(manifestBytes), manifestSHA, manifestBytes); err != nil {
			return err
		}
	}
	return nil
}
