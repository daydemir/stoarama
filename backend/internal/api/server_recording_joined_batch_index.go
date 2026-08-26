package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/daydemir/stoarama/backend/internal/joinedrecording"
	"github.com/daydemir/stoarama/backend/internal/util"
)

const (
	joinedBatchIndexOperationTimeout = 40 * time.Second
	joinedBatchIndexStatementTimeout = "35s"
	joinedBatchIndexLockTimeout      = "15s"
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
	ctx, cancel := context.WithTimeout(r.Context(), joinedBatchIndexOperationTimeout)
	defer cancel()
	r = r.WithContext(ctx)
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
	ctx, cancel := context.WithTimeout(ctx, joinedBatchIndexOperationTimeout)
	defer cancel()
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return joinedSealBatchIndexResponse{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := configureJoinedBatchIndexTransaction(ctx, tx); err != nil {
		return joinedSealBatchIndexResponse{}, err
	}

	canonical, state, existingID, err := loadJoinedCanonicalBatchIndex(ctx, tx, request.BatchID,
		s.cfg.JoinedRecordingConnectionID, request.Apply)
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
	referenceCount := len(canonical.Index.AllocationLedgers) + len(canonical.Index.Hours)
	artifactIDs := make([]int64, 0, referenceCount)
	referenceKinds := make([]string, 0, referenceCount)
	ordinals := make([]int32, 0, referenceCount)
	for ordinal, ref := range canonical.Index.AllocationLedgers {
		artifactIDs = append(artifactIDs, ref.ArtifactID)
		referenceKinds = append(referenceKinds, "allocation_ledger")
		ordinals = append(ordinals, int32(ordinal+1))
	}
	for ordinal, ref := range canonical.Index.Hours {
		artifactIDs = append(artifactIDs, ref.HourManifestArtifactID)
		referenceKinds = append(referenceKinds, "hour_manifest")
		ordinals = append(ordinals, int32(ordinal+1))
	}
	if _, err := tx.Exec(ctx, `INSERT INTO recording_joined_batch_index_refs(batch_record_id,index_artifact_id,
		referenced_artifact_id,reference_kind,ordinal) SELECT $1,$2,artifact_id,kind,ordinal
		FROM unnest($3::bigint[],$4::text[],$5::integer[]) AS refs(artifact_id,kind,ordinal)`, canonical.BatchRecordID,
		response.ArtifactID, artifactIDs, referenceKinds, ordinals); err != nil {
		return response, fmt.Errorf("insert joined batch-index references: %w", err)
	}
	command, err := tx.Exec(ctx, `UPDATE recording_joined_batches SET state='index_sealed',index_artifact_id=$2,index_sealed_at=clock_timestamp()
		WHERE id=$1 AND state='frozen' AND frozen_at IS NOT NULL AND EXISTS(SELECT 1 FROM connections c
		WHERE c.id=recording_joined_batches.connection_id AND c.id=$3)`, canonical.BatchRecordID,
		response.ArtifactID, s.cfg.JoinedRecordingConnectionID)
	if err != nil || command.RowsAffected() != 1 {
		return response, errors.New("joined batch changed before index seal")
	}
	response.State = "index_sealed"
	if err := tx.Commit(ctx); err != nil {
		return response, err
	}
	return response, nil
}

func configureJoinedBatchIndexTransaction(ctx context.Context, tx pgx.Tx) error {
	_, err := tx.Exec(ctx, `SELECT set_config('lock_timeout',$1,true),set_config('statement_timeout',$2,true)`,
		joinedBatchIndexLockTimeout, joinedBatchIndexStatementTimeout)
	return err
}

func loadJoinedCanonicalBatchIndex(ctx context.Context, tx pgx.Tx, batchID string, connectionID int, lock bool) (joinedCanonicalIndex, string, int64, error) {
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
		FROM recording_joined_batches b JOIN connections c ON c.id=b.connection_id
		JOIN recording_qualification_runs q ON q.id=b.qualification_run_id AND q.account_id=b.account_id
		WHERE b.batch_id=$1 AND c.id=$2`
	if lock {
		query += ` FOR UPDATE OF b FOR SHARE OF c,q`
	}
	err := tx.QueryRow(ctx, query, batchID, connectionID).Scan(&canonical.BatchRecordID, &canonical.AccountID, &canonical.ConnectionID,
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
	var ledgerByID map[int64]joinedrecording.StreamDayAllocation
	canonical.Index.AllocationLedgers, ledgerByID, err = loadJoinedLedgerReferences(ctx, tx, canonical.BatchRecordID,
		expectedLedgers)
	if err != nil {
		return canonical, state, 0, err
	}
	var hourByID map[int64]joinedrecording.HourManifest
	canonical.Index.Hours, hourByID, canonical.Index.FinalMediaCount, err = loadJoinedHourReferences(ctx, tx,
		canonical.BatchRecordID, expectedHours)
	if err != nil {
		return canonical, state, 0, err
	}
	canonical.Index.BatchGenerationSHA256, err = joinedrecording.ComputeBatchGenerationSHA256(canonical.Index)
	if err != nil {
		return canonical, state, 0, err
	}
	resolveEvidence := func() (joinedrecording.FrozenBatchEvidence, error) {
		return evidence, nil
	}
	resolveLedger := func(ref joinedrecording.AllocationLedgerRef) (joinedrecording.StreamDayAllocation, error) {
		ledger, ok := ledgerByID[ref.ArtifactID]
		if !ok {
			return joinedrecording.StreamDayAllocation{}, errors.New("joined allocation ledger resolver identity differs")
		}
		return ledger, nil
	}
	resolveHour := func(ref joinedrecording.BatchIndexHour) (joinedrecording.HourManifest, error) {
		manifest, ok := hourByID[ref.HourManifestArtifactID]
		if !ok {
			return joinedrecording.HourManifest{}, errors.New("joined hour-manifest resolver identity differs")
		}
		return manifest, nil
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

func loadJoinedLedgerReferences(ctx context.Context, tx pgx.Tx, batchRecordID int64, expected int) (
	[]joinedrecording.AllocationLedgerRef, map[int64]joinedrecording.StreamDayAllocation, error) {
	rows, err := tx.Query(ctx, `SELECT a.id,a.canonical_bytes,a.relative_path,a.object_key,a.expected_size_bytes,
		a.expected_sha256,(br.priority_ordinal-1)*14+d.date_ordinal
		FROM recording_joined_artifacts a JOIN recording_joined_stream_days d ON d.id=a.stream_day_id
		JOIN recording_joined_batch_recordings br ON br.id=d.batch_recording_id
		WHERE a.batch_record_id=$1 AND a.artifact_kind='allocation_ledger' AND a.publication_state='published'
		ORDER BY (br.priority_ordinal-1)*14+d.date_ordinal`, batchRecordID)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	refs := make([]joinedrecording.AllocationLedgerRef, 0, expected)
	ledgers := make(map[int64]joinedrecording.StreamDayAllocation, expected)
	for rows.Next() {
		var artifactID, expectedSize int64
		var ordinal int
		var raw []byte
		var relativePath, objectKey, expectedSHA string
		if err := rows.Scan(&artifactID, &raw, &relativePath, &objectKey, &expectedSize, &expectedSHA, &ordinal); err != nil {
			return nil, nil, err
		}
		if ordinal != len(refs)+1 || len(refs) >= expected {
			return nil, nil, errors.New("joined allocation ledger order differs")
		}
		var ledger joinedrecording.StreamDayAllocation
		if err := json.Unmarshal(raw, &ledger); err != nil {
			return nil, nil, errors.New("decode canonical joined allocation ledger")
		}
		ref, err := joinedrecording.BuildAllocationLedgerRef(artifactID, ledger)
		if err != nil || ref.RelativePath != relativePath || ref.ObjectKey != objectKey || ref.SizeBytes != expectedSize ||
			ref.SHA256 != expectedSHA {
			return nil, nil, errors.New("joined allocation ledger row differs from canonical bytes")
		}
		refs = append(refs, ref)
		ledgers[artifactID] = ledger
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	if len(refs) != expected {
		return nil, nil, errors.New("joined allocation ledger cardinality differs")
	}
	return refs, ledgers, nil
}

func loadJoinedHourReferences(ctx context.Context, tx pgx.Tx, batchRecordID int64, expected int) (
	[]joinedrecording.BatchIndexHour, map[int64]joinedrecording.HourManifest, int, error) {
	rows, err := tx.Query(ctx, `SELECT a.id,a.hour_record_id,a.canonical_bytes,a.relative_path,a.object_key,
		a.expected_size_bytes,a.expected_sha256,h.priority_ordinal
		FROM recording_joined_artifacts a JOIN recording_joined_hours h ON h.id=a.hour_record_id AND h.state='sealed'
		WHERE a.batch_record_id=$1 AND a.artifact_kind='hour_manifest' AND a.publication_state='published'
		ORDER BY h.priority_ordinal`, batchRecordID)
	if err != nil {
		return nil, nil, 0, err
	}
	refs := make([]joinedrecording.BatchIndexHour, 0, expected)
	manifests := make(map[int64]joinedrecording.HourManifest, expected)
	hourArtifacts := make(map[int64]int64, expected)
	finalMediaCount := 0
	for rows.Next() {
		var artifactID, hourRecordID, expectedSize int64
		var ordinal int
		var raw []byte
		var relativePath, objectKey, expectedSHA string
		if err := rows.Scan(&artifactID, &hourRecordID, &raw, &relativePath, &objectKey, &expectedSize, &expectedSHA,
			&ordinal); err != nil {
			rows.Close()
			return nil, nil, 0, err
		}
		if ordinal != len(refs)+1 || len(refs) >= expected {
			rows.Close()
			return nil, nil, 0, errors.New("joined hour-manifest order differs")
		}
		var manifest joinedrecording.HourManifest
		if err := json.Unmarshal(raw, &manifest); err != nil {
			rows.Close()
			return nil, nil, 0, errors.New("decode canonical joined hour manifest")
		}
		ref, err := joinedrecording.BuildBatchIndexHour(artifactID, manifest)
		if err != nil || ref.RelativePath != relativePath || ref.ObjectKey != objectKey || ref.SizeBytes != expectedSize ||
			ref.SHA256 != expectedSHA {
			rows.Close()
			return nil, nil, 0, errors.New("joined hour-manifest row differs from canonical bytes")
		}
		refs = append(refs, ref)
		manifests[artifactID] = manifest
		hourArtifacts[hourRecordID] = artifactID
		finalMediaCount += len(manifest.Media)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, nil, 0, err
	}
	rows.Close()
	if len(refs) != expected {
		return nil, nil, 0, errors.New("joined hour-manifest cardinality differs")
	}
	mediaRows, err := tx.Query(ctx, `SELECT a.hour_record_id,a.id,a.ordinal,a.relative_path,a.object_key,a.content_id,
		a.expected_size_bytes,a.expected_sha256,a.published_at IS NOT NULL
		FROM recording_joined_artifacts a JOIN recording_joined_hours h ON h.id=a.hour_record_id
		WHERE h.batch_record_id=$1 AND a.artifact_kind='media' ORDER BY h.priority_ordinal,a.ordinal`, batchRecordID)
	if err != nil {
		return nil, nil, 0, err
	}
	defer mediaRows.Close()
	mediaPositions := make(map[int64]int, expected)
	for mediaRows.Next() {
		var hourRecordID, id, size int64
		var ordinal int
		var path, key, contentID, sha string
		var published bool
		if err := mediaRows.Scan(&hourRecordID, &id, &ordinal, &path, &key, &contentID, &size, &sha, &published); err != nil {
			return nil, nil, 0, err
		}
		artifactID, ok := hourArtifacts[hourRecordID]
		manifest, exists := manifests[artifactID]
		position := mediaPositions[hourRecordID]
		if !ok || !exists || !published || position >= len(manifest.Media) {
			return nil, nil, 0, errors.New("joined hour media cardinality differs")
		}
		media := manifest.Media[position]
		if id != media.ArtifactID || ordinal != media.Ordinal || path != media.RelativePath || key != media.ObjectKey ||
			contentID != media.ContentID || size != media.SizeBytes || sha != media.SHA256 {
			return nil, nil, 0, errors.New("joined published media differs from hour manifest")
		}
		mediaPositions[hourRecordID] = position + 1
	}
	if err := mediaRows.Err(); err != nil {
		return nil, nil, 0, err
	}
	for hourRecordID, artifactID := range hourArtifacts {
		if mediaPositions[hourRecordID] != len(manifests[artifactID].Media) {
			return nil, nil, 0, errors.New("joined hour media cardinality differs")
		}
	}
	return refs, manifests, finalMediaCount, nil
}
