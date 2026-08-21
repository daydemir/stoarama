package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/daydemir/stoarama/backend/internal/joinedrecording"
	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/daydemir/stoarama/backend/internal/stitchcert"
	"github.com/daydemir/stoarama/backend/internal/util"
)

const (
	joinedStreamDayHeadConcurrency = 4
	joinedStreamDayHeadAttempts    = 3
	joinedStreamDayHeadTimeout     = 8 * time.Second
	joinedStreamDayHeadTotal       = 25 * time.Second
	joinedStreamDayHeadTTL         = 2 * time.Minute
	joinedStreamDayHeadBackoff1    = 100 * time.Millisecond
	joinedStreamDayHeadBackoff2    = 250 * time.Millisecond
)

type joinedFreezeSourceObjectStore interface {
	Bucket() string
	PresignHeadExactRequest(context.Context, string, string, string, time.Duration) (r2.PresignedRequest, error)
}

type joinedSealStreamDayRequest struct {
	ProtocolVersion int    `json:"protocol_version"`
	BatchID         string `json:"batch_id"`
	RecordingID     int64  `json:"recording_id"`
	LocalDate       string `json:"local_date"`
}

type joinedSealStreamDayResponse struct {
	ProtocolVersion   int    `json:"protocol_version"`
	BatchID           string `json:"batch_id"`
	RecordingID       int64  `json:"recording_id"`
	LocalDate         string `json:"local_date"`
	SourceSnapshotSHA string `json:"source_snapshot_sha256"`
	HeadManifestSHA   string `json:"head_manifest_sha256"`
	LedgerSHA         string `json:"ledger_sha256"`
	LedgerArtifactSHA string `json:"ledger_artifact_sha256"`
	SealRequestSHA    string `json:"seal_request_sha256"`
	LedgerArtifactID  int64  `json:"ledger_artifact_id"`
	SourceCount       int    `json:"source_count"`
	SourceBytes       int64  `json:"source_bytes"`
	AlreadySealed     bool   `json:"already_sealed"`
}

type joinedStreamDaySnapshot struct {
	ID                int64
	ClipCreatedAt     time.Time
	CaptureLeaseToken *string
	CaptureSequence   *int64
	CaptureAttemptID  *string
	TimestampVersion  *string
	TimestampContract []byte
	TimestampStatus   *string
	TimestampReason   *string
	Source            joinedrecording.SourceClip
}

type joinedStreamDayHeadObservation struct {
	SourceSnapshotID     int64     `json:"source_snapshot_id"`
	ClipID               int64     `json:"clip_id"`
	StorageDestinationID int64     `json:"storage_destination_id"`
	Provider             string    `json:"provider"`
	Endpoint             string    `json:"endpoint"`
	Region               string    `json:"region"`
	Bucket               string    `json:"bucket"`
	ObjectKey            string    `json:"object_key"`
	VersionID            string    `json:"version_id"`
	ETag                 string    `json:"etag"`
	SizeBytes            int64     `json:"size_bytes"`
	SHA256               string    `json:"sha256"`
	StartUTC             time.Time `json:"start_utc"`
	EndUTC               time.Time `json:"end_utc"`
}

type joinedStreamDayHeadManifest struct {
	SchemaVersion     int                              `json:"schema_version"`
	BatchID           string                           `json:"batch_id"`
	RecordingID       int64                            `json:"recording_id"`
	LocalDate         string                           `json:"local_date"`
	SourceSnapshotSHA string                           `json:"source_snapshot_sha256"`
	Observations      []joinedStreamDayHeadObservation `json:"observations"`
}

type joinedStreamDaySealIdentity struct {
	SchemaVersion     int    `json:"schema_version"`
	BatchID           string `json:"batch_id"`
	RecordingID       int64  `json:"recording_id"`
	LocalDate         string `json:"local_date"`
	SourceSnapshotSHA string `json:"source_snapshot_sha256"`
	HeadManifestSHA   string `json:"head_manifest_sha256"`
	LedgerSHA         string `json:"ledger_sha256"`
	LedgerArtifactSHA string `json:"ledger_artifact_sha256"`
}

type joinedStreamDayPlan struct {
	BatchRecordID, BatchRecordingID, StreamDayID int64
	AccountID, ConnectionID, RecordingID         int64
	RecordingJobID                               int64
	BatchID, LocalDate, Timezone, FolderName     string
	Generation, DateOrdinal, PriorityOrdinal     int
	ScheduledStart, ScheduledEnd                 time.Time
	Qualification                                joinedrecording.QualificationWindow
	SourceSnapshotSHA                            string
	Sources                                      []joinedStreamDaySnapshot
}

func (s *Server) joinedFreezeStore() joinedFreezeSourceObjectStore {
	if s.joinedFreezeSourceStore != nil {
		return s.joinedFreezeSourceStore
	}
	return s.r2
}

func (r joinedSealStreamDayRequest) validate() error {
	if r.ProtocolVersion != joinedrecording.JoinedProtocolVersion || !joinedBatchIDPattern.MatchString(r.BatchID) ||
		r.RecordingID <= 0 {
		return errors.New("invalid joined stream-day request")
	}
	parsed, err := time.Parse("2006-01-02", r.LocalDate)
	if err != nil || parsed.Format("2006-01-02") != r.LocalDate {
		return errors.New("invalid joined stream-day date")
	}
	return nil
}

func (s *Server) handleAdminJoinedSealStreamDay(w http.ResponseWriter, r *http.Request) {
	if !s.joinedControlPlaneReady() {
		util.WriteError(w, http.StatusServiceUnavailable, "joined recording is disabled")
		return
	}
	var req joinedSealStreamDayRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := req.validate(); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if response, ok, err := s.loadSealedJoinedStreamDay(r.Context(), req); err != nil {
		util.WriteError(w, http.StatusConflict, err.Error())
		return
	} else if ok {
		response.AlreadySealed = true
		util.WriteJSON(w, http.StatusOK, response)
		return
	}
	plan, err := s.loadPendingJoinedStreamDay(r.Context(), req)
	if err != nil {
		util.WriteError(w, http.StatusConflict, err.Error())
		return
	}
	observations, err := s.headJoinedStreamDaySources(r.Context(), plan)
	if err != nil {
		util.WriteError(w, http.StatusConflict, err.Error())
		return
	}
	response, err := s.sealJoinedStreamDay(r.Context(), req, plan, observations)
	if err != nil {
		util.WriteError(w, http.StatusConflict, err.Error())
		return
	}
	util.WriteJSON(w, http.StatusOK, response)
}

func (s *Server) loadSealedJoinedStreamDay(ctx context.Context, req joinedSealStreamDayRequest) (joinedSealStreamDayResponse, bool, error) {
	var response joinedSealStreamDayResponse
	err := s.pool.QueryRow(ctx, `SELECT d.batch_id,d.recording_id,d.local_date::text,d.source_snapshot_sha256,
		d.head_manifest_sha256,d.ledger_sha256,d.ledger_artifact_sha256,d.seal_request_sha256,a.id,
		d.source_clip_count,d.source_bytes
		FROM recording_joined_stream_days d JOIN recording_joined_batches b ON b.id=d.batch_record_id
		JOIN connections c ON c.id=d.connection_id AND c.joined_protocol_version=1
		JOIN recording_joined_artifacts a ON a.stream_day_id=d.id AND a.artifact_kind='allocation_ledger'
		WHERE d.batch_id=$1 AND d.recording_id=$2 AND d.local_date=$3::date AND d.state='sealed'
		  AND b.state='building' AND b.freeze_started_at IS NULL`, req.BatchID, req.RecordingID, req.LocalDate).Scan(
		&response.BatchID, &response.RecordingID, &response.LocalDate, &response.SourceSnapshotSHA,
		&response.HeadManifestSHA, &response.LedgerSHA, &response.LedgerArtifactSHA, &response.SealRequestSHA,
		&response.LedgerArtifactID, &response.SourceCount, &response.SourceBytes)
	if errors.Is(err, pgx.ErrNoRows) {
		return response, false, nil
	}
	if err != nil {
		return response, false, fmt.Errorf("load sealed joined stream day: %w", err)
	}
	response.ProtocolVersion = joinedrecording.JoinedProtocolVersion
	return response, true, nil
}

func (s *Server) loadPendingJoinedStreamDay(ctx context.Context, req joinedSealStreamDayRequest) (joinedStreamDayPlan, error) {
	var plan joinedStreamDayPlan
	var qualification []byte
	err := s.pool.QueryRow(ctx, `SELECT b.id,br.id,d.id,d.account_id,d.connection_id,d.recording_id,d.recording_job_id,
		d.batch_id,d.local_date::text,br.timezone,br.folder_name,b.generation,d.date_ordinal,br.priority_ordinal,
		d.scheduled_start_at,d.scheduled_end_at,br.qualification,d.source_snapshot_sha256
		FROM recording_joined_stream_days d JOIN recording_joined_batches b ON b.id=d.batch_record_id
		JOIN recording_joined_batch_recordings br ON br.id=d.batch_recording_id
		JOIN connections c ON c.id=d.connection_id AND c.joined_protocol_version=1
		WHERE d.batch_id=$1 AND d.recording_id=$2 AND d.local_date=$3::date AND d.state='pending'
		  AND b.state='building' AND b.freeze_started_at IS NULL`, req.BatchID, req.RecordingID, req.LocalDate).Scan(
		&plan.BatchRecordID, &plan.BatchRecordingID, &plan.StreamDayID, &plan.AccountID, &plan.ConnectionID,
		&plan.RecordingID, &plan.RecordingJobID, &plan.BatchID, &plan.LocalDate, &plan.Timezone, &plan.FolderName,
		&plan.Generation, &plan.DateOrdinal, &plan.PriorityOrdinal, &plan.ScheduledStart, &plan.ScheduledEnd,
		&qualification, &plan.SourceSnapshotSHA)
	if err != nil {
		return plan, fmt.Errorf("load pending joined stream day: %w", err)
	}
	if err := json.Unmarshal(qualification, &plan.Qualification); err != nil ||
		joinedrecording.ValidateQualificationWindow(plan.Qualification) != nil {
		return plan, errors.New("joined stream-day qualification differs")
	}
	rows, err := s.pool.Query(ctx, `SELECT id,clip_id,recording_id,recording_job_id,storage_destination_id,provider,
		endpoint,region,bucket,object_key,ingest_etag,size_bytes,sha256,start_at,end_at,clip_created_at,released_at,
		capture_lease_token::text,capture_sequence,capture_attempt_id::text,timestamp_contract_version,
		timestamp_contract,timestamp_contract_status,timestamp_contract_reason
		FROM recording_joined_source_snapshots WHERE stream_day_id=$1 ORDER BY day_ordinal`, plan.StreamDayID)
	if err != nil {
		return plan, err
	}
	defer rows.Close()
	for rows.Next() {
		var snapshot joinedStreamDaySnapshot
		if err := rows.Scan(&snapshot.ID, &snapshot.Source.ClipID, &snapshot.Source.RecordingID,
			&snapshot.Source.RecordingJobID, &snapshot.Source.StorageDestinationID, &snapshot.Source.Provider,
			&snapshot.Source.Endpoint, &snapshot.Source.Region, &snapshot.Source.Bucket, &snapshot.Source.Object.Key,
			&snapshot.Source.Object.ETag, &snapshot.Source.Object.SizeBytes, &snapshot.Source.Object.SHA256,
			&snapshot.Source.StartUTC, &snapshot.Source.EndUTC, &snapshot.ClipCreatedAt, &snapshot.Source.ReleasedAt,
			&snapshot.CaptureLeaseToken, &snapshot.CaptureSequence, &snapshot.CaptureAttemptID, &snapshot.TimestampVersion,
			&snapshot.TimestampContract, &snapshot.TimestampStatus, &snapshot.TimestampReason); err != nil {
			return plan, err
		}
		snapshot.Source.StartUTC, snapshot.Source.EndUTC = snapshot.Source.StartUTC.UTC(), snapshot.Source.EndUTC.UTC()
		if snapshot.Source.ReleasedAt != nil {
			released := snapshot.Source.ReleasedAt.UTC()
			snapshot.Source.ReleasedAt = &released
		}
		plan.Sources = append(plan.Sources, snapshot)
	}
	if err := rows.Err(); err != nil {
		return plan, err
	}
	projection, err := joinedrecording.BuildFrozenDenominatorDayProjection(plan.RecordingID,
		plan.Qualification.Days[plan.DateOrdinal-1], plan.Qualification.EvidenceSHA,
		joinedrecording.FrozenSourceSnapshots(joinedStreamDaySourceClips(plan.Sources)))
	if err != nil || projection.FrozenSourceSHA256 != plan.SourceSnapshotSHA || projection.SourceCount != len(plan.Sources) {
		return plan, errors.New("joined stream-day snapshot differs")
	}
	return plan, nil
}

func joinedStreamDaySourceClips(snapshots []joinedStreamDaySnapshot) []joinedrecording.SourceClip {
	sources := make([]joinedrecording.SourceClip, len(snapshots))
	for i := range snapshots {
		sources[i] = snapshots[i].Source
	}
	return sources
}

func (s *Server) headJoinedStreamDaySources(ctx context.Context, plan joinedStreamDayPlan) ([]joinedStreamDayHeadObservation, error) {
	if len(plan.Sources) == 0 {
		return []joinedStreamDayHeadObservation{}, nil
	}
	store := s.joinedFreezeStore()
	if store == nil || store.Bucket() != s.cfg.R2Bucket {
		return nil, errors.New("joined source storage is unavailable")
	}
	observations := make([]joinedStreamDayHeadObservation, len(plan.Sources))
	ctx, timeout := context.WithTimeout(ctx, joinedStreamDayHeadTotal)
	defer timeout()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	semaphore := make(chan struct{}, joinedStreamDayHeadConcurrency)
	var wg sync.WaitGroup
	var firstErr error
	var errorMu sync.Mutex
	for i := range plan.Sources {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case semaphore <- struct{}{}:
				defer func() { <-semaphore }()
			case <-ctx.Done():
				return
			}
			observation, err := s.headJoinedStreamDaySource(ctx, store, plan.Sources[i])
			if err != nil {
				errorMu.Lock()
				if firstErr == nil {
					firstErr = err
					cancel()
				}
				errorMu.Unlock()
				return
			}
			observations[i] = observation
		}()
	}
	wg.Wait()
	if firstErr != nil {
		return nil, firstErr
	}
	return observations, nil
}

func (s *Server) headJoinedStreamDaySource(ctx context.Context, store joinedFreezeSourceObjectStore,
	snapshot joinedStreamDaySnapshot) (joinedStreamDayHeadObservation, error) {
	return s.headJoinedStreamDaySourceRetry(ctx, store, snapshot, waitJoinedStreamDayHeadRetry)
}

func waitJoinedStreamDayHeadRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (s *Server) headJoinedStreamDaySourceRetry(ctx context.Context, store joinedFreezeSourceObjectStore,
	snapshot joinedStreamDaySnapshot, wait func(context.Context, time.Duration) error) (joinedStreamDayHeadObservation, error) {
	retryCtx, cancel := context.WithTimeout(ctx, joinedStreamDayHeadTotal)
	defer cancel()
	var lastErr error
	for attempt := 0; attempt < joinedStreamDayHeadAttempts; attempt++ {
		observation, retry, err := s.headJoinedStreamDaySourceAttempt(retryCtx, store, snapshot)
		if err == nil {
			return observation, nil
		}
		lastErr = err
		if !retry || attempt == joinedStreamDayHeadAttempts-1 {
			return joinedStreamDayHeadObservation{}, err
		}
		delay := joinedStreamDayHeadBackoff2
		if attempt == 0 {
			delay = joinedStreamDayHeadBackoff1
		}
		if err := wait(retryCtx, delay); err != nil {
			return joinedStreamDayHeadObservation{}, err
		}
	}
	return joinedStreamDayHeadObservation{}, lastErr
}

func (s *Server) headJoinedStreamDaySourceAttempt(ctx context.Context, store joinedFreezeSourceObjectStore,
	snapshot joinedStreamDaySnapshot) (joinedStreamDayHeadObservation, bool, error) {
	source := snapshot.Source
	if source.Provider != "r2" || source.Endpoint != s.cfg.R2Endpoint || source.Region != s.cfg.R2Region ||
		source.Bucket != s.cfg.R2Bucket {
		return joinedStreamDayHeadObservation{}, false, errors.New("joined source storage coordinates differ")
	}
	capability, err := store.PresignHeadExactRequest(ctx, source.Object.Key, source.Object.ETag, "", joinedStreamDayHeadTTL)
	if err != nil {
		return joinedStreamDayHeadObservation{}, false, fmt.Errorf("presign joined source HEAD: %w", err)
	}
	parsed, err := url.Parse(capability.URL)
	endpoint, endpointErr := url.Parse(source.Endpoint)
	expectedPath := (&url.URL{Path: "/" + source.Bucket + "/" + source.Object.Key}).EscapedPath()
	if err != nil || endpointErr != nil || capability.Method != http.MethodHead || parsed.Scheme != "https" ||
		parsed.Scheme != endpoint.Scheme || parsed.Host != endpoint.Host || parsed.EscapedPath() != expectedPath ||
		capability.Headers.Get("If-Match") != `"`+source.Object.ETag+`"` {
		return joinedStreamDayHeadObservation{}, false, errors.New("joined source HEAD capability differs")
	}
	requestCtx, cancel := context.WithTimeout(ctx, joinedStreamDayHeadTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(requestCtx, http.MethodHead, capability.URL, nil)
	if err != nil {
		return joinedStreamDayHeadObservation{}, false, err
	}
	request.Header = capability.Headers.Clone()
	transport := s.joinedFreezeTransport
	if transport == nil {
		transport = http.DefaultTransport
	}
	client := &http.Client{Transport: transport, CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
		return http.ErrUseLastResponse
	}}
	response, err := client.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return joinedStreamDayHeadObservation{}, false, ctx.Err()
		}
		return joinedStreamDayHeadObservation{}, true, fmt.Errorf("HEAD joined source: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		retry := response.StatusCode == http.StatusTooManyRequests ||
			(response.StatusCode >= 500 && response.StatusCode <= 599)
		return joinedStreamDayHeadObservation{}, retry, fmt.Errorf("HEAD joined source status %d", response.StatusCode)
	}
	etag := strings.TrimSpace(response.Header.Get("ETag"))
	if strings.HasPrefix(etag, "W/") || len(etag) < 2 || etag[0] != '"' || etag[len(etag)-1] != '"' {
		return joinedStreamDayHeadObservation{}, false, errors.New("joined source HEAD ETag differs")
	}
	etag = etag[1 : len(etag)-1]
	size := response.ContentLength
	if size < 0 {
		size, err = strconv.ParseInt(response.Header.Get("Content-Length"), 10, 64)
		if err != nil {
			return joinedStreamDayHeadObservation{}, false, errors.New("joined source HEAD size is missing")
		}
	}
	if etag != source.Object.ETag || size != source.Object.SizeBytes {
		return joinedStreamDayHeadObservation{}, false, errors.New("joined source HEAD identity drifted")
	}
	return joinedStreamDayHeadObservation{SourceSnapshotID: snapshot.ID, ClipID: source.ClipID,
		StorageDestinationID: source.StorageDestinationID, Provider: source.Provider, Endpoint: source.Endpoint,
		Region: source.Region, Bucket: source.Bucket, ObjectKey: source.Object.Key,
		VersionID: strings.TrimSpace(response.Header.Get("x-amz-version-id")), ETag: etag,
		SizeBytes: size, SHA256: source.Object.SHA256, StartUTC: source.StartUTC, EndUTC: source.EndUTC}, false, nil
}

func (s *Server) sealJoinedStreamDay(ctx context.Context, req joinedSealStreamDayRequest, loaded joinedStreamDayPlan,
	observations []joinedStreamDayHeadObservation) (joinedSealStreamDayResponse, error) {
	if len(observations) != len(loaded.Sources) {
		return joinedSealStreamDayResponse{}, errors.New("joined stream-day HEAD count differs")
	}
	sources := joinedStreamDaySourceClips(loaded.Sources)
	for i := range sources {
		if observations[i].SourceSnapshotID != loaded.Sources[i].ID || observations[i].ClipID != sources[i].ClipID ||
			observations[i].StorageDestinationID != sources[i].StorageDestinationID ||
			observations[i].ObjectKey != sources[i].Object.Key || observations[i].SHA256 != sources[i].Object.SHA256 ||
			observations[i].SizeBytes != sources[i].Object.SizeBytes {
			return joinedSealStreamDayResponse{}, errors.New("joined stream-day HEAD order differs")
		}
		sources[i].Object.ETag = observations[i].ETag
		sources[i].Object.VersionID = observations[i].VersionID
	}
	previous, next, err := s.loadJoinedStreamDayNeighbors(ctx, loaded)
	if err != nil {
		return joinedSealStreamDayResponse{}, err
	}
	planRequest := joinedrecording.PlanRequest{BatchID: loaded.BatchID, Generation: loaded.Generation,
		RecordingID: loaded.RecordingID, Timezone: loaded.Timezone, LocalDate: loaded.LocalDate,
		Qualification: loaded.Qualification, Sources: sources, PreviousDayLast: previous, NextDayFirst: next}
	var draft joinedrecording.StreamDayDraft
	if len(sources) == 0 {
		draft, err = joinedrecording.BuildGapOnlyStreamDay(planRequest, loaded.LocalDate)
	} else {
		draft, err = joinedrecording.AllocateStreamDay(planRequest)
	}
	if err != nil {
		return joinedSealStreamDayResponse{}, fmt.Errorf("allocate joined stream day: %w", err)
	}
	ledger, err := joinedrecording.SealStreamDayAllocation(draft)
	if err != nil {
		return joinedSealStreamDayResponse{}, fmt.Errorf("seal joined stream day: %w", err)
	}
	ledgerBytes, ledgerArtifactSHA, err := joinedrecording.CanonicalAllocationLedgerArtifact(ledger)
	if err != nil {
		return joinedSealStreamDayResponse{}, err
	}
	headSHA, _, err := stitchcert.CanonicalSHA(joinedStreamDayHeadManifest{SchemaVersion: 1, BatchID: loaded.BatchID,
		RecordingID: loaded.RecordingID, LocalDate: loaded.LocalDate, SourceSnapshotSHA: loaded.SourceSnapshotSHA,
		Observations: observations})
	if err != nil {
		return joinedSealStreamDayResponse{}, err
	}
	sealSHA, _, err := stitchcert.CanonicalSHA(joinedStreamDaySealIdentity{SchemaVersion: 1, BatchID: loaded.BatchID,
		RecordingID: loaded.RecordingID, LocalDate: loaded.LocalDate, SourceSnapshotSHA: loaded.SourceSnapshotSHA,
		HeadManifestSHA: headSHA, LedgerSHA: ledger.LedgerSHA256, LedgerArtifactSHA: ledgerArtifactSHA})
	if err != nil {
		return joinedSealStreamDayResponse{}, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return joinedSealStreamDayResponse{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	current, state, err := loadJoinedStreamDayForSeal(ctx, tx, req)
	if err != nil {
		return joinedSealStreamDayResponse{}, err
	}
	if state == "sealed" {
		response, err := joinedSealedStreamDayResponse(ctx, tx, req)
		if err != nil {
			return response, err
		}
		if response.SourceSnapshotSHA != loaded.SourceSnapshotSHA || response.HeadManifestSHA != headSHA ||
			response.LedgerSHA != ledger.LedgerSHA256 || response.LedgerArtifactSHA != ledgerArtifactSHA ||
			response.SealRequestSHA != sealSHA {
			return response, errors.New("joined stream-day retry identity differs")
		}
		response.AlreadySealed = true
		return response, tx.Commit(ctx)
	}
	if !sameJoinedStreamDayPlan(loaded, current) {
		return joinedSealStreamDayResponse{}, errors.New("joined stream-day snapshot changed during HEAD")
	}
	if err := insertJoinedStreamDayEvidence(ctx, tx, current, ledger, ledgerBytes, ledgerArtifactSHA, observations,
		headSHA, sealSHA); err != nil {
		return joinedSealStreamDayResponse{}, err
	}
	response, err := joinedSealedStreamDayResponse(ctx, tx, req)
	if err != nil {
		return response, err
	}
	if err := tx.Commit(ctx); err != nil {
		return response, err
	}
	return response, nil
}

func loadJoinedStreamDayForSeal(ctx context.Context, tx pgx.Tx, req joinedSealStreamDayRequest) (joinedStreamDayPlan, string, error) {
	var plan joinedStreamDayPlan
	var qualification []byte
	var state string
	err := tx.QueryRow(ctx, `SELECT b.id,br.id,d.id,d.account_id,d.connection_id,d.recording_id,d.recording_job_id,
		d.batch_id,d.local_date::text,br.timezone,br.folder_name,b.generation,d.date_ordinal,br.priority_ordinal,
		d.scheduled_start_at,d.scheduled_end_at,br.qualification,d.source_snapshot_sha256,d.state
		FROM recording_joined_stream_days d JOIN recording_joined_batches b ON b.id=d.batch_record_id
		JOIN recording_joined_batch_recordings br ON br.id=d.batch_recording_id
		JOIN connections c ON c.id=d.connection_id
		WHERE d.batch_id=$1 AND d.recording_id=$2 AND d.local_date=$3::date
		  AND b.state='building' AND b.freeze_started_at IS NULL AND c.joined_protocol_version=1
		FOR UPDATE OF d,b,c FOR SHARE OF br`, req.BatchID, req.RecordingID, req.LocalDate).Scan(
		&plan.BatchRecordID, &plan.BatchRecordingID, &plan.StreamDayID, &plan.AccountID, &plan.ConnectionID,
		&plan.RecordingID, &plan.RecordingJobID, &plan.BatchID, &plan.LocalDate, &plan.Timezone, &plan.FolderName,
		&plan.Generation, &plan.DateOrdinal, &plan.PriorityOrdinal, &plan.ScheduledStart, &plan.ScheduledEnd,
		&qualification, &plan.SourceSnapshotSHA, &state)
	if err != nil {
		return plan, state, fmt.Errorf("lock joined stream day: %w", err)
	}
	if err := json.Unmarshal(qualification, &plan.Qualification); err != nil {
		return plan, state, err
	}
	rows, err := tx.Query(ctx, `SELECT id,clip_id,recording_id,recording_job_id,storage_destination_id,provider,
		endpoint,region,bucket,object_key,ingest_etag,size_bytes,sha256,start_at,end_at,clip_created_at,released_at,
		capture_lease_token::text,capture_sequence,capture_attempt_id::text,timestamp_contract_version,
		timestamp_contract,timestamp_contract_status,timestamp_contract_reason
		FROM recording_joined_source_snapshots WHERE stream_day_id=$1 ORDER BY day_ordinal FOR SHARE`, plan.StreamDayID)
	if err != nil {
		return plan, state, err
	}
	defer rows.Close()
	for rows.Next() {
		var snapshot joinedStreamDaySnapshot
		if err := rows.Scan(&snapshot.ID, &snapshot.Source.ClipID, &snapshot.Source.RecordingID,
			&snapshot.Source.RecordingJobID, &snapshot.Source.StorageDestinationID, &snapshot.Source.Provider,
			&snapshot.Source.Endpoint, &snapshot.Source.Region, &snapshot.Source.Bucket, &snapshot.Source.Object.Key,
			&snapshot.Source.Object.ETag, &snapshot.Source.Object.SizeBytes, &snapshot.Source.Object.SHA256,
			&snapshot.Source.StartUTC, &snapshot.Source.EndUTC, &snapshot.ClipCreatedAt, &snapshot.Source.ReleasedAt,
			&snapshot.CaptureLeaseToken, &snapshot.CaptureSequence, &snapshot.CaptureAttemptID, &snapshot.TimestampVersion,
			&snapshot.TimestampContract, &snapshot.TimestampStatus, &snapshot.TimestampReason); err != nil {
			return plan, state, err
		}
		snapshot.Source.StartUTC, snapshot.Source.EndUTC = snapshot.Source.StartUTC.UTC(), snapshot.Source.EndUTC.UTC()
		if snapshot.Source.ReleasedAt != nil {
			released := snapshot.Source.ReleasedAt.UTC()
			snapshot.Source.ReleasedAt = &released
		}
		plan.Sources = append(plan.Sources, snapshot)
	}
	return plan, state, rows.Err()
}

func sameJoinedStreamDayPlan(left, right joinedStreamDayPlan) bool {
	leftSources, rightSources := left.Sources, right.Sources
	leftQualificationValue, rightQualificationValue := left.Qualification, right.Qualification
	left.Sources, right.Sources = nil, nil
	left.Qualification, right.Qualification = joinedrecording.QualificationWindow{}, joinedrecording.QualificationWindow{}
	leftQualification, _, leftQualificationErr := stitchcert.CanonicalSHA(leftQualificationValue)
	rightQualification, _, rightQualificationErr := stitchcert.CanonicalSHA(rightQualificationValue)
	leftSHA, _, leftErr := stitchcert.CanonicalSHA(leftSources)
	rightSHA, _, rightErr := stitchcert.CanonicalSHA(rightSources)
	leftPlanSHA, _, leftPlanErr := stitchcert.CanonicalSHA(left)
	rightPlanSHA, _, rightPlanErr := stitchcert.CanonicalSHA(right)
	return leftPlanErr == nil && rightPlanErr == nil && leftPlanSHA == rightPlanSHA &&
		leftQualificationErr == nil && rightQualificationErr == nil &&
		leftQualification == rightQualification && leftErr == nil && rightErr == nil && leftSHA == rightSHA
}

func (s *Server) loadJoinedStreamDayNeighbors(ctx context.Context, plan joinedStreamDayPlan) (*joinedrecording.SourceClip,
	*joinedrecording.SourceClip, error) {
	load := func(dateOrdinal, direction int) (*joinedrecording.SourceClip, error) {
		if dateOrdinal < 1 || dateOrdinal > 14 {
			return nil, nil
		}
		order := "ASC"
		if direction < 0 {
			order = "DESC"
		}
		var source joinedrecording.SourceClip
		err := s.pool.QueryRow(ctx, `SELECT snapshot.clip_id,snapshot.recording_id,snapshot.recording_job_id,
			snapshot.storage_destination_id,snapshot.provider,snapshot.endpoint,snapshot.region,snapshot.bucket,
			snapshot.object_key,snapshot.ingest_etag,snapshot.size_bytes,snapshot.sha256,snapshot.start_at,snapshot.end_at,
			snapshot.released_at FROM recording_joined_stream_days day JOIN recording_joined_source_snapshots snapshot
			ON snapshot.stream_day_id=day.id WHERE day.batch_record_id=$1 AND day.recording_id=$2 AND day.date_ordinal=$3
			ORDER BY snapshot.day_ordinal `+order+` LIMIT 1`, plan.BatchRecordID, plan.RecordingID, dateOrdinal).Scan(
			&source.ClipID, &source.RecordingID, &source.RecordingJobID, &source.StorageDestinationID, &source.Provider,
			&source.Endpoint, &source.Region, &source.Bucket, &source.Object.Key, &source.Object.ETag,
			&source.Object.SizeBytes, &source.Object.SHA256, &source.StartUTC, &source.EndUTC, &source.ReleasedAt)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		source.StartUTC, source.EndUTC = source.StartUTC.UTC(), source.EndUTC.UTC()
		if source.ReleasedAt != nil {
			released := source.ReleasedAt.UTC()
			source.ReleasedAt = &released
		}
		return &source, nil
	}
	previous, err := load(plan.DateOrdinal-1, -1)
	if err != nil {
		return nil, nil, err
	}
	next, err := load(plan.DateOrdinal+1, 1)
	return previous, next, err
}

func insertJoinedStreamDayEvidence(ctx context.Context, tx pgx.Tx, plan joinedStreamDayPlan,
	ledger joinedrecording.StreamDayAllocation, ledgerBytes []byte, ledgerArtifactSHA string,
	observations []joinedStreamDayHeadObservation, headSHA, sealSHA string) error {
	hourIDs := make(map[int]int64, 12)
	sourceByID := make(map[int64]joinedrecording.SourceClip, len(ledger.Sources))
	for _, source := range ledger.Sources {
		sourceByID[source.ClipID] = source
	}
	for i, hour := range ledger.Hours {
		var sourceBytes int64
		for _, clipID := range hour.SourceClipIDs {
			sourceBytes += sourceByID[clipID].Object.SizeBytes
		}
		hourID, err := joinedrecording.CanonicalHourID(plan.BatchID, plan.RecordingID, plan.LocalDate,
			hour.DeliveryHour, plan.Generation)
		if err != nil {
			return err
		}
		scheduledStart := plan.ScheduledStart.Add(time.Duration(i) * time.Hour)
		var id int64
		if err := tx.QueryRow(ctx, `INSERT INTO recording_joined_hours(batch_record_id,stream_day_id,account_id,
			connection_id,batch_id,recording_id,hour_id,local_date,delivery_hour,clock_hour,scheduled_start_at,
			scheduled_end_at,priority_ordinal,source_clip_count,source_bytes,source_claim_sha256)
			VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16) RETURNING id`, plan.BatchRecordID,
			plan.StreamDayID, plan.AccountID, plan.ConnectionID, plan.BatchID, plan.RecordingID, hourID, plan.LocalDate,
			hour.DeliveryHour, hour.ClockHour, scheduledStart, scheduledStart.Add(time.Hour),
			int64((plan.PriorityOrdinal-1)*168+(plan.DateOrdinal-1)*12+hour.DeliveryHour), len(hour.SourceClipIDs),
			sourceBytes, ledger.HourSourceSHA256[i]).Scan(&id); err != nil {
			return err
		}
		hourIDs[hour.DeliveryHour] = id
	}
	for i, boundary := range ledger.Boundaries {
		if _, err := tx.Exec(ctx, `INSERT INTO recording_joined_day_boundaries(stream_day_id,boundary_kind,ordinal,
			previous_delivery_hour,next_delivery_hour,previous_clip_id,next_clip_id,previous_presentation_end_at,
			next_presentation_start_at,signed_gap_nanoseconds,scheduled_at,actual_seam_at,boundary_skew_nanoseconds,
			allocation_decision,verdict,reason) VALUES($1,'cross_hour',$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)`,
			plan.StreamDayID, i+1, boundary.PreviousHour, boundary.NextHour, boundary.PreviousClipID, boundary.NextClipID,
			boundary.PreviousPresentationEndUTC, boundary.NextPresentationStartUTC, boundary.SignedGapNanoseconds,
			boundary.ScheduledUTC, boundary.ActualSeamUTC, boundary.BoundarySkewNanoseconds,
			boundary.AllocationDecision, boundary.Verdict, boundary.Reason); err != nil {
			return err
		}
	}
	for i, boundary := range ledger.CrossDayBoundaries {
		if _, err := tx.Exec(ctx, `INSERT INTO recording_joined_day_boundaries(stream_day_id,boundary_kind,ordinal,
			previous_clip_id,next_clip_id,previous_presentation_end_at,next_presentation_start_at,signed_gap_nanoseconds,
			scheduled_previous_end_at,scheduled_next_start_at,boundary_skew_nanoseconds,allocation_decision,verdict,reason)
			VALUES($1,'cross_day',$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`, plan.StreamDayID, i+1,
			boundary.PreviousClipID, boundary.NextClipID, boundary.PreviousPresentationEndUTC,
			boundary.NextPresentationStartUTC, boundary.SignedGapNanoseconds, boundary.ScheduledPreviousEndUTC,
			boundary.ScheduledNextStartUTC, boundary.BoundarySkewNanoseconds, boundary.AllocationDecision,
			boundary.Verdict, boundary.Reason); err != nil {
			return err
		}
	}
	observedAt := time.Now().UTC()
	snapshots := make(map[int64]joinedStreamDaySnapshot, len(plan.Sources))
	for _, snapshot := range plan.Sources {
		snapshots[snapshot.Source.ClipID] = snapshot
	}
	observationByClip := make(map[int64]joinedStreamDayHeadObservation, len(observations))
	for _, observation := range observations {
		if _, duplicate := observationByClip[observation.ClipID]; duplicate {
			return errors.New("duplicate joined source HEAD observation")
		}
		observationByClip[observation.ClipID] = observation
	}
	dayOrdinal := 0
	for hourIndex, hour := range ledger.Hours {
		for hourOrdinal, clipID := range hour.SourceClipIDs {
			dayOrdinal++
			snapshot, snapshotOK := snapshots[clipID]
			observation, observationOK := observationByClip[clipID]
			if !snapshotOK || !observationOK {
				return errors.New("joined source observation is missing")
			}
			seam, err := json.Marshal(ledger.Sources[dayOrdinal-1].SeamToPrevious)
			if err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `INSERT INTO recording_joined_sources(source_snapshot_id,batch_record_id,
				stream_day_id,hour_record_id,account_id,connection_id,recording_id,recording_job_id,clip_id,
				storage_destination_id,day_ordinal,hour_ordinal,provider,endpoint,region,bucket,object_key,version_id,
				etag,size_bytes,sha256,start_at,end_at,seam_to_previous,clip_created_at,released_at,observed_at)
				VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27)`,
				snapshot.ID, plan.BatchRecordID, plan.StreamDayID, hourIDs[hourIndex+1], plan.AccountID,
				plan.ConnectionID, plan.RecordingID, plan.RecordingJobID, clipID, snapshot.Source.StorageDestinationID,
				dayOrdinal, hourOrdinal+1, snapshot.Source.Provider, snapshot.Source.Endpoint, snapshot.Source.Region,
				snapshot.Source.Bucket, snapshot.Source.Object.Key, observation.VersionID, observation.ETag,
				observation.SizeBytes, snapshot.Source.Object.SHA256, snapshot.Source.StartUTC, snapshot.Source.EndUTC,
				seam, snapshot.ClipCreatedAt, snapshot.Source.ReleasedAt, observedAt); err != nil {
				return err
			}
		}
	}
	if len(observationByClip) != dayOrdinal {
		return errors.New("joined source HEAD observation count differs")
	}
	if _, err := tx.Exec(ctx, `UPDATE recording_joined_stream_days SET state='sealed',source_manifest_sha256=$2,
		head_manifest_sha256=$3,seal_request_sha256=$4,ledger_sha256=$5,ledger_bytes=$6,
		ledger_artifact_sha256=$7,sealed_at=clock_timestamp() WHERE id=$1 AND state='pending'`, plan.StreamDayID,
		ledger.SourceClaimSHA256, headSHA, sealSHA, ledger.LedgerSHA256, ledgerBytes, ledgerArtifactSHA); err != nil {
		return err
	}
	relativePath, objectKey, err := joinedrecording.CanonicalAllocationLedgerPaths(plan.BatchID, plan.RecordingID,
		plan.LocalDate)
	if err != nil {
		return err
	}
	scopeID := fmt.Sprintf("%s__recording-%d__date-%s__generation-%d", plan.BatchID, plan.RecordingID,
		plan.LocalDate, plan.Generation)
	_, err = tx.Exec(ctx, `INSERT INTO recording_joined_artifacts(batch_record_id,account_id,connection_id,batch_id,
		scope_kind,scope_id,stream_day_id,artifact_kind,ordinal,relative_path,object_key,content_type,
		expected_size_bytes,expected_sha256,canonical_bytes,publication_state)
		VALUES($1,$2,$3,$4,'ledger',$5,$6,'allocation_ledger',1,$7,$8,'application/json',$9,$10,$11,'sealed')`,
		plan.BatchRecordID, plan.AccountID, plan.ConnectionID, plan.BatchID, scopeID, plan.StreamDayID,
		relativePath, objectKey, len(ledgerBytes), ledgerArtifactSHA, ledgerBytes)
	return err
}

func joinedSealedStreamDayResponse(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, req joinedSealStreamDayRequest) (joinedSealStreamDayResponse, error) {
	response := joinedSealStreamDayResponse{ProtocolVersion: joinedrecording.JoinedProtocolVersion}
	err := q.QueryRow(ctx, `SELECT d.batch_id,d.recording_id,d.local_date::text,d.source_snapshot_sha256,
		d.head_manifest_sha256,d.ledger_sha256,d.ledger_artifact_sha256,d.seal_request_sha256,a.id,
		d.source_clip_count,d.source_bytes FROM recording_joined_stream_days d
		JOIN recording_joined_artifacts a ON a.stream_day_id=d.id AND a.artifact_kind='allocation_ledger'
		WHERE d.batch_id=$1 AND d.recording_id=$2 AND d.local_date=$3::date AND d.state='sealed'`,
		req.BatchID, req.RecordingID, req.LocalDate).Scan(&response.BatchID, &response.RecordingID, &response.LocalDate,
		&response.SourceSnapshotSHA, &response.HeadManifestSHA, &response.LedgerSHA, &response.LedgerArtifactSHA,
		&response.SealRequestSHA, &response.LedgerArtifactID, &response.SourceCount, &response.SourceBytes)
	return response, err
}
