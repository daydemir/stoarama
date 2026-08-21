package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"

	"github.com/jackc/pgx/v5"

	"github.com/daydemir/stoarama/backend/internal/joinedrecording"
	"github.com/daydemir/stoarama/backend/internal/util"
)

type joinedSealBatchIndexRequest struct {
	ProtocolVersion int    `json:"protocol_version"`
	BatchID         string `json:"batch_id"`
	Apply           bool   `json:"apply"`
	ExpectedSHA256  string `json:"expected_sha256,omitempty"`
}

func (request joinedSealBatchIndexRequest) validate() error {
	if request.ProtocolVersion != joinedrecording.JoinedProtocolVersion || !joinedBatchIDPattern.MatchString(request.BatchID) ||
		(request.Apply && !lowerHexSHA256(request.ExpectedSHA256)) || (!request.Apply && request.ExpectedSHA256 != "") {
		return errors.New("invalid joined batch-index seal request")
	}
	return nil
}

type joinedSealBatchIndexResponse struct {
	ProtocolVersion int    `json:"protocol_version"`
	BatchID         string `json:"batch_id"`
	State           string `json:"state"`
	ArtifactID      int64  `json:"artifact_id,omitempty"`
	RelativePath    string `json:"relative_path"`
	ObjectKey       string `json:"object_key"`
	SizeBytes       int64  `json:"size_bytes"`
	SHA256          string `json:"sha256"`
	AlreadySealed   bool   `json:"already_sealed"`
}

type joinedCanonicalIndex struct {
	BatchRecordID int64
	AccountID     int64
	ConnectionID  int64
	Index         joinedrecording.BatchIndex
	Bytes         []byte
	SHA256        string
}

func (s *Server) handleAdminJoinedSealBatchIndex(w http.ResponseWriter, r *http.Request) {
	if !s.joinedControlPlaneReady() || !s.joinedFrozenBatchScope() {
		util.WriteError(w, http.StatusServiceUnavailable, "joined frozen-batch work is disabled")
		return
	}
	var request joinedSealBatchIndexRequest
	if err := util.DecodeJSON(r, &request); err != nil || request.validate() != nil {
		util.WriteError(w, http.StatusBadRequest, "invalid joined batch-index seal request")
		return
	}
	if request.BatchID != s.cfg.JoinedRecordingBatchID {
		util.WriteError(w, http.StatusConflict, "joined batch-index scope differs")
		return
	}
	response, err := s.sealJoinedBatchIndex(r.Context(), request)
	if err != nil {
		util.WriteError(w, http.StatusConflict, err.Error())
		return
	}
	util.WriteJSON(w, http.StatusOK, response)
}

func (s *Server) sealJoinedBatchIndex(ctx context.Context, request joinedSealBatchIndexRequest) (joinedSealBatchIndexResponse, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		return joinedSealBatchIndexResponse{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	canonical, state, existingID, err := loadJoinedCanonicalBatchIndex(ctx, tx, request.BatchID, request.Apply)
	if err != nil {
		return joinedSealBatchIndexResponse{}, err
	}
	response := joinedSealBatchIndexResponse{ProtocolVersion: joinedrecording.JoinedProtocolVersion, BatchID: request.BatchID,
		State: state, ArtifactID: existingID, RelativePath: "coverage/batch.json", SizeBytes: int64(len(canonical.Bytes)),
		SHA256: canonical.SHA256}
	response.ObjectKey, err = joinedrecording.CanonicalBatchIndexObjectKey(request.BatchID)
	if err != nil {
		return response, err
	}
	if state == "index_sealed" || state == "published" {
		if existingID <= 0 {
			return response, errors.New("joined batch index identity is absent")
		}
		if request.Apply && request.ExpectedSHA256 != response.SHA256 {
			return response, errors.New("joined batch-index expected hash differs")
		}
		response.AlreadySealed = true
		if err := tx.Commit(ctx); err != nil {
			return response, err
		}
		return response, nil
	}
	if state != "frozen" {
		return response, errors.New("joined batch is not ready for index sealing")
	}
	if !request.Apply {
		if err := tx.Commit(ctx); err != nil {
			return response, err
		}
		return response, nil
	}
	if request.ExpectedSHA256 != response.SHA256 {
		return response, errors.New("joined batch-index expected hash differs")
	}
	if err := tx.QueryRow(ctx, `INSERT INTO recording_joined_artifacts(batch_record_id,account_id,connection_id,batch_id,
		scope_kind,scope_id,artifact_kind,ordinal,relative_path,object_key,content_type,expected_size_bytes,expected_sha256,
		canonical_bytes,publication_state) VALUES($1,$2,$3,$4,'batch_index',$4,'batch_index',1,$5,$6,'application/json',$7,$8,$9,'sealed')
		RETURNING id`, canonical.BatchRecordID, canonical.AccountID, canonical.ConnectionID, request.BatchID, response.RelativePath,
		response.ObjectKey, response.SizeBytes, response.SHA256, canonical.Bytes).Scan(&response.ArtifactID); err != nil {
		return response, fmt.Errorf("insert joined batch index: %w", err)
	}
	for ordinal, ref := range canonical.Index.AllocationLedgers {
		if _, err := tx.Exec(ctx, `INSERT INTO recording_joined_batch_index_refs(batch_record_id,index_artifact_id,
			referenced_artifact_id,reference_kind,ordinal) VALUES($1,$2,$3,'allocation_ledger',$4)`, canonical.BatchRecordID,
			response.ArtifactID, ref.ArtifactID, ordinal+1); err != nil {
			return response, fmt.Errorf("insert joined ledger index reference: %w", err)
		}
	}
	for ordinal, ref := range canonical.Index.Hours {
		if _, err := tx.Exec(ctx, `INSERT INTO recording_joined_batch_index_refs(batch_record_id,index_artifact_id,
			referenced_artifact_id,reference_kind,ordinal) VALUES($1,$2,$3,'hour_manifest',$4)`, canonical.BatchRecordID,
			response.ArtifactID, ref.HourManifestArtifactID, ordinal+1); err != nil {
			return response, fmt.Errorf("insert joined hour index reference: %w", err)
		}
	}
	command, err := tx.Exec(ctx, `UPDATE recording_joined_batches SET state='index_sealed',index_artifact_id=$2,index_sealed_at=clock_timestamp()
		WHERE id=$1 AND state='frozen' AND frozen_at IS NOT NULL AND EXISTS(SELECT 1 FROM connections c
		WHERE c.id=recording_joined_batches.connection_id AND c.joined_protocol_version=1)`, canonical.BatchRecordID, response.ArtifactID)
	if err != nil || command.RowsAffected() != 1 {
		return response, errors.New("joined batch changed before index seal")
	}
	response.State = "index_sealed"
	if err := tx.Commit(ctx); err != nil {
		return response, err
	}
	return response, nil
}

func loadJoinedCanonicalBatchIndex(ctx context.Context, tx pgx.Tx, batchID string, lock bool) (joinedCanonicalIndex, string, int64, error) {
	var canonical joinedCanonicalIndex
	var state string
	var indexArtifactID *int64
	var mediaToolJSON []byte
	var authority joinedrecording.SelectionAuthority
	var expectedRecordings, expectedLedgers, expectedHours int
	var sourceCount64, sourceBytes int64
	query := `SELECT b.id,b.account_id,b.connection_id,b.state,b.index_artifact_id,b.generation,b.frozen_at,
		b.frozen_denominator_sha256,b.expected_recordings,b.expected_stream_days,b.expected_scheduled_hours,
		b.expected_source_clips,b.expected_source_bytes,b.policy_version,b.media_tool,b.selection_basis,
		b.ordered_recording_ids_sha256,b.eligibility_cutoff,b.qualification_run_id,b.qualification_frozen_at,
		q.definition_version,b.qualification_cohort_sha256,b.qualification_windows_sha256,b.selected_qualification_windows_sha256
		FROM recording_joined_batches b JOIN connections c ON c.id=b.connection_id AND c.joined_protocol_version=1
		JOIN recording_qualification_runs q ON q.id=b.qualification_run_id AND q.account_id=b.account_id
		WHERE b.batch_id=$1`
	if lock {
		query += ` FOR UPDATE OF b FOR SHARE OF c,q`
	}
	err := tx.QueryRow(ctx, query, batchID).Scan(&canonical.BatchRecordID, &canonical.AccountID, &canonical.ConnectionID,
		&state, &indexArtifactID, &canonical.Index.Generation, &canonical.Index.FrozenAt, &canonical.Index.FrozenDenominatorSHA256,
		&expectedRecordings, &expectedLedgers, &expectedHours, &sourceCount64, &sourceBytes, &canonical.Index.PolicyVersion,
		&mediaToolJSON, &authority.SelectionBasis, &authority.OrderedRecordingIDSHA256, &authority.Cutoff,
		&authority.QualificationRunID, &authority.QualificationRunFrozenAt, &authority.QualificationRuleVersion,
		&authority.QualificationCohortSHA256, &authority.QualificationWindowsSHA256,
		&authority.SelectedQualificationWindowsSHA256)
	if err != nil {
		return canonical, state, 0, fmt.Errorf("load joined frozen batch: %w", err)
	}
	if canonical.Index.FrozenAt.IsZero() || expectedRecordings != 33 || expectedLedgers != 462 || expectedHours != 5544 ||
		sourceCount64 < 0 || sourceCount64 > math.MaxInt {
		return canonical, state, 0, errors.New("joined frozen batch denominator differs")
	}
	canonical.Index.SchemaVersion = joinedrecording.BatchIndexSchemaVersion
	canonical.Index.AllocationSchemaVersion = 1
	canonical.Index.HourManifestSchemaVersion = joinedrecording.HourManifestSchemaVersion
	canonical.Index.BatchID = batchID
	canonical.Index.SelectionAuthority = authority
	canonical.Index.ExpectedLedgerCount = expectedLedgers
	canonical.Index.ScheduledHourCount = expectedHours
	canonical.Index.SourceClipCount = int(sourceCount64)
	canonical.Index.SourceBytes = sourceBytes
	canonical.Index.FrozenAt = canonical.Index.FrozenAt.UTC()
	canonical.Index.SelectionAuthority.Cutoff = canonical.Index.SelectionAuthority.Cutoff.UTC()
	canonical.Index.SelectionAuthority.QualificationRunFrozenAt = canonical.Index.SelectionAuthority.QualificationRunFrozenAt.UTC()
	if err := json.Unmarshal(mediaToolJSON, &canonical.Index.MediaTool); err != nil {
		return canonical, state, 0, errors.New("decode joined frozen media tool")
	}
	evidence, err := loadJoinedFrozenBatchEvidence(ctx, tx, canonical.BatchRecordID, canonical.Index.SelectionAuthority,
		canonical.Index.FrozenDenominatorSHA256)
	if err != nil {
		return canonical, state, 0, err
	}
	canonical.Index.FrozenRecordings = evidence.FrozenRecordings
	canonical.Index.RecordingIDs = make([]int64, len(evidence.FrozenRecordings))
	for index, recording := range evidence.FrozenRecordings {
		canonical.Index.RecordingIDs[index] = recording.RecordingID
	}
	for ordinal := 1; ordinal <= expectedLedgers; ordinal++ {
		ref, _, err := loadJoinedLedgerReference(ctx, tx, canonical.BatchRecordID, int64(ordinal))
		if err != nil {
			return canonical, state, 0, err
		}
		canonical.Index.AllocationLedgers = append(canonical.Index.AllocationLedgers, ref)
	}
	for ordinal := 1; ordinal <= expectedHours; ordinal++ {
		ref, manifest, err := loadJoinedHourReference(ctx, tx, canonical.BatchRecordID, int64(ordinal))
		if err != nil {
			return canonical, state, 0, err
		}
		canonical.Index.Hours = append(canonical.Index.Hours, ref)
		canonical.Index.FinalMediaCount += len(manifest.Media)
	}
	canonical.Index.BatchGenerationSHA256, err = joinedrecording.ComputeBatchGenerationSHA256(canonical.Index)
	if err != nil {
		return canonical, state, 0, err
	}
	resolveEvidence := func() (joinedrecording.FrozenBatchEvidence, error) {
		return loadJoinedFrozenBatchEvidence(ctx, tx, canonical.BatchRecordID, canonical.Index.SelectionAuthority,
			canonical.Index.FrozenDenominatorSHA256)
	}
	resolveLedger := func(ref joinedrecording.AllocationLedgerRef) (joinedrecording.StreamDayAllocation, error) {
		_, ledger, err := loadJoinedLedgerReferenceByArtifact(ctx, tx, canonical.BatchRecordID, ref.ArtifactID)
		return ledger, err
	}
	resolveHour := func(ref joinedrecording.BatchIndexHour) (joinedrecording.HourManifest, error) {
		_, manifest, err := loadJoinedHourReferenceByArtifact(ctx, tx, canonical.BatchRecordID, ref.HourManifestArtifactID)
		return manifest, err
	}
	canonical.Index, canonical.Bytes, canonical.SHA256, err = joinedrecording.BuildBatchIndex(canonical.Index,
		resolveEvidence, resolveLedger, resolveHour)
	if err != nil {
		return canonical, state, 0, fmt.Errorf("build canonical joined batch index: %w", err)
	}
	if indexArtifactID != nil {
		objectKey, err := joinedrecording.CanonicalBatchIndexObjectKey(batchID)
		if err != nil {
			return canonical, state, 0, err
		}
		var relativePath, storedObjectKey, storedSHA string
		var storedSize int64
		var storedBytes []byte
		if err := tx.QueryRow(ctx, `SELECT relative_path,object_key,expected_size_bytes,expected_sha256,canonical_bytes
			FROM recording_joined_artifacts WHERE id=$1 AND batch_record_id=$2 AND artifact_kind='batch_index'`,
			*indexArtifactID, canonical.BatchRecordID).Scan(&relativePath, &storedObjectKey, &storedSize, &storedSHA, &storedBytes); err != nil ||
			relativePath != "coverage/batch.json" || storedObjectKey != objectKey || storedSize != int64(len(canonical.Bytes)) ||
			storedSHA != canonical.SHA256 || !bytes.Equal(storedBytes, canonical.Bytes) {
			return canonical, state, 0, errors.New("joined batch-index artifact differs from canonical evidence")
		}
		if err := validateJoinedBatchIndexReferences(ctx, tx, canonical.BatchRecordID, *indexArtifactID, canonical.Index); err != nil {
			return canonical, state, 0, err
		}
		return canonical, state, *indexArtifactID, nil
	}
	return canonical, state, 0, nil
}

func validateJoinedBatchIndexReferences(ctx context.Context, tx pgx.Tx, batchRecordID, indexArtifactID int64,
	index joinedrecording.BatchIndex) error {
	rows, err := tx.Query(ctx, `SELECT reference_kind,ordinal,referenced_artifact_id
		FROM recording_joined_batch_index_refs WHERE batch_record_id=$1 AND index_artifact_id=$2
		ORDER BY CASE reference_kind WHEN 'allocation_ledger' THEN 0 ELSE 1 END,ordinal`, batchRecordID, indexArtifactID)
	if err != nil {
		return err
	}
	defer rows.Close()
	position := 0
	for rows.Next() {
		var kind string
		var ordinal int
		var artifactID int64
		if err := rows.Scan(&kind, &ordinal, &artifactID); err != nil {
			return err
		}
		var expectedID int64
		switch {
		case position < len(index.AllocationLedgers):
			if kind != "allocation_ledger" || ordinal != position+1 {
				return errors.New("joined batch-index ledger reference order differs")
			}
			expectedID = index.AllocationLedgers[position].ArtifactID
		case position < len(index.AllocationLedgers)+len(index.Hours):
			hourPosition := position - len(index.AllocationLedgers)
			if kind != "hour_manifest" || ordinal != hourPosition+1 {
				return errors.New("joined batch-index hour reference order differs")
			}
			expectedID = index.Hours[hourPosition].HourManifestArtifactID
		default:
			return errors.New("joined batch-index has extra references")
		}
		if artifactID != expectedID {
			return errors.New("joined batch-index referenced artifact differs")
		}
		position++
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if position != len(index.AllocationLedgers)+len(index.Hours) {
		return errors.New("joined batch-index reference set is incomplete")
	}
	return nil
}

func loadJoinedFrozenBatchEvidence(ctx context.Context, tx pgx.Tx, batchRecordID int64, authority joinedrecording.SelectionAuthority,
	denominator string) (joinedrecording.FrozenBatchEvidence, error) {
	rows, err := tx.Query(ctx, `SELECT recording_id,priority_ordinal,selection_tier,qualification_sha256,completed_at,
		timezone,folder_name,naming_metadata,qualification FROM recording_joined_batch_recordings
		WHERE batch_record_id=$1 ORDER BY priority_ordinal`, batchRecordID)
	if err != nil {
		return joinedrecording.FrozenBatchEvidence{}, err
	}
	defer rows.Close()
	evidence := joinedrecording.FrozenBatchEvidence{SelectionAuthority: authority, FrozenDenominatorSHA256: denominator}
	for rows.Next() {
		var recording joinedrecording.FrozenRecording
		var metadataJSON, qualificationJSON []byte
		if err := rows.Scan(&recording.RecordingID, &recording.PriorityOrdinal, &recording.SelectionTier,
			&recording.QualificationSHA256, &recording.CompletedAt, &recording.Timezone, &recording.FolderName,
			&metadataJSON, &qualificationJSON); err != nil {
			return evidence, err
		}
		if err := json.Unmarshal(metadataJSON, &recording.NamingMetadata); err != nil {
			return evidence, errors.New("decode joined frozen naming metadata")
		}
		var window joinedrecording.QualificationWindow
		if err := json.Unmarshal(qualificationJSON, &window); err != nil {
			return evidence, errors.New("decode joined frozen qualification window")
		}
		recording.CompletedAt = recording.CompletedAt.UTC()
		window.FrozenAt = window.FrozenAt.UTC()
		for day := range window.Days {
			window.Days[day].WindowStart = window.Days[day].WindowStart.UTC()
			window.Days[day].WindowEnd = window.Days[day].WindowEnd.UTC()
			window.Days[day].CompletedAt = window.Days[day].CompletedAt.UTC()
		}
		evidence.FrozenRecordings = append(evidence.FrozenRecordings, recording)
		evidence.QualificationWindows = append(evidence.QualificationWindows, window)
	}
	return evidence, rows.Err()
}

func loadJoinedLedgerReference(ctx context.Context, tx pgx.Tx, batchRecordID, ordinal int64) (joinedrecording.AllocationLedgerRef,
	joinedrecording.StreamDayAllocation, error) {
	var artifactID int64
	err := tx.QueryRow(ctx, `SELECT a.id FROM recording_joined_artifacts a JOIN recording_joined_stream_days d ON d.id=a.stream_day_id
		JOIN recording_joined_batch_recordings br ON br.id=d.batch_recording_id
		WHERE a.batch_record_id=$1 AND a.artifact_kind='allocation_ledger' AND a.publication_state='published'
		AND (br.priority_ordinal-1)*14+d.date_ordinal=$2`, batchRecordID, ordinal).Scan(&artifactID)
	if err != nil {
		return joinedrecording.AllocationLedgerRef{}, joinedrecording.StreamDayAllocation{}, fmt.Errorf("load joined ledger ordinal %d: %w", ordinal, err)
	}
	return loadJoinedLedgerReferenceByArtifact(ctx, tx, batchRecordID, artifactID)
}

func loadJoinedLedgerReferenceByArtifact(ctx context.Context, tx pgx.Tx, batchRecordID, artifactID int64) (
	joinedrecording.AllocationLedgerRef, joinedrecording.StreamDayAllocation, error) {
	var raw []byte
	var relativePath, objectKey, expectedSHA string
	var expectedSize int64
	err := tx.QueryRow(ctx, `SELECT canonical_bytes,relative_path,object_key,expected_size_bytes,expected_sha256
		FROM recording_joined_artifacts WHERE id=$1 AND batch_record_id=$2 AND artifact_kind='allocation_ledger'
		AND publication_state='published'`, artifactID, batchRecordID).Scan(&raw, &relativePath, &objectKey, &expectedSize, &expectedSHA)
	if err != nil {
		return joinedrecording.AllocationLedgerRef{}, joinedrecording.StreamDayAllocation{}, err
	}
	var ledger joinedrecording.StreamDayAllocation
	if err := json.Unmarshal(raw, &ledger); err != nil {
		return joinedrecording.AllocationLedgerRef{}, ledger, errors.New("decode canonical joined allocation ledger")
	}
	ref, err := joinedrecording.BuildAllocationLedgerRef(artifactID, ledger)
	if err != nil || ref.RelativePath != relativePath || ref.ObjectKey != objectKey || ref.SizeBytes != expectedSize || ref.SHA256 != expectedSHA {
		return ref, ledger, errors.New("joined allocation ledger row differs from canonical bytes")
	}
	return ref, ledger, nil
}

func loadJoinedHourReference(ctx context.Context, tx pgx.Tx, batchRecordID, ordinal int64) (joinedrecording.BatchIndexHour,
	joinedrecording.HourManifest, error) {
	var artifactID int64
	err := tx.QueryRow(ctx, `SELECT a.id FROM recording_joined_artifacts a JOIN recording_joined_hours h ON h.id=a.hour_record_id
		WHERE a.batch_record_id=$1 AND a.artifact_kind='hour_manifest' AND a.publication_state='published'
		AND h.state='sealed' AND h.priority_ordinal=$2`, batchRecordID, ordinal).Scan(&artifactID)
	if err != nil {
		return joinedrecording.BatchIndexHour{}, joinedrecording.HourManifest{}, fmt.Errorf("load joined hour ordinal %d: %w", ordinal, err)
	}
	return loadJoinedHourReferenceByArtifact(ctx, tx, batchRecordID, artifactID)
}

func loadJoinedHourReferenceByArtifact(ctx context.Context, tx pgx.Tx, batchRecordID, artifactID int64) (
	joinedrecording.BatchIndexHour, joinedrecording.HourManifest, error) {
	var raw []byte
	var relativePath, objectKey, expectedSHA string
	var expectedSize, hourRecordID int64
	err := tx.QueryRow(ctx, `SELECT a.canonical_bytes,a.relative_path,a.object_key,a.expected_size_bytes,a.expected_sha256,a.hour_record_id
		FROM recording_joined_artifacts a JOIN recording_joined_hours h ON h.id=a.hour_record_id AND h.state='sealed'
		WHERE a.id=$1 AND a.batch_record_id=$2 AND a.artifact_kind='hour_manifest' AND a.publication_state='published'`,
		artifactID, batchRecordID).Scan(&raw, &relativePath, &objectKey, &expectedSize, &expectedSHA, &hourRecordID)
	if err != nil {
		return joinedrecording.BatchIndexHour{}, joinedrecording.HourManifest{}, err
	}
	var manifest joinedrecording.HourManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return joinedrecording.BatchIndexHour{}, manifest, errors.New("decode canonical joined hour manifest")
	}
	ref, err := joinedrecording.BuildBatchIndexHour(artifactID, manifest)
	if err != nil || ref.RelativePath != relativePath || ref.ObjectKey != objectKey || ref.SizeBytes != expectedSize || ref.SHA256 != expectedSHA {
		return ref, manifest, errors.New("joined hour-manifest row differs from canonical bytes")
	}
	rows, err := tx.Query(ctx, `SELECT id,ordinal,relative_path,object_key,content_id,expected_size_bytes,expected_sha256
		FROM recording_joined_artifacts WHERE hour_record_id=$1 AND artifact_kind='media' AND published_at IS NOT NULL ORDER BY ordinal`,
		hourRecordID)
	if err != nil {
		return ref, manifest, err
	}
	defer rows.Close()
	mediaIndex := 0
	for rows.Next() {
		if mediaIndex >= len(manifest.Media) {
			return ref, manifest, errors.New("joined hour media cardinality differs")
		}
		var id, size int64
		var ordinal int
		var path, key, contentID, sha string
		if err := rows.Scan(&id, &ordinal, &path, &key, &contentID, &size, &sha); err != nil {
			return ref, manifest, err
		}
		media := manifest.Media[mediaIndex]
		if id != media.ArtifactID || ordinal != media.Ordinal || path != media.RelativePath || key != media.ObjectKey ||
			contentID != media.ContentID || size != media.SizeBytes || sha != media.SHA256 {
			return ref, manifest, errors.New("joined published media differs from hour manifest")
		}
		mediaIndex++
	}
	if err := rows.Err(); err != nil || mediaIndex != len(manifest.Media) {
		return ref, manifest, errors.New("joined hour media cardinality differs")
	}
	return ref, manifest, nil
}
