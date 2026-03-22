package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/daydemir/stoarama/backend/internal/capture"
	"github.com/daydemir/stoarama/backend/internal/model"
	"github.com/daydemir/stoarama/backend/internal/util"
)

type apiStatusError struct {
	Status  int
	Message string
}

func (e *apiStatusError) Error() string {
	return e.Message
}

func newAPIStatusError(status int, format string, args ...any) error {
	return &apiStatusError{Status: status, Message: fmt.Sprintf(format, args...)}
}

func writeAPIError(w http.ResponseWriter, err error) {
	var statusErr *apiStatusError
	if errorsAs(err, &statusErr) {
		util.WriteError(w, statusErr.Status, statusErr.Message)
		return
	}
	util.WriteError(w, http.StatusInternalServerError, err.Error())
}

func errorsAs(err error, target **apiStatusError) bool {
	if err == nil {
		return false
	}
	v, ok := err.(*apiStatusError)
	if !ok {
		return false
	}
	*target = v
	return true
}

func mergeJSONMaps(base map[string]any, override map[string]any) map[string]any {
	if len(base) == 0 && len(override) == 0 {
		return map[string]any{}
	}
	out := make(map[string]any, len(base)+len(override))
	for k, v := range base {
		out[k] = v
	}
	for k, v := range override {
		out[k] = v
	}
	return out
}

func defaultDiscoveryExternalID(sourceURL string) string {
	raw := strings.TrimSpace(sourceURL)
	if raw == "" {
		return ""
	}
	hash := uuid.NewSHA1(uuid.NameSpaceURL, []byte(raw)).String()
	return "discovery-" + strings.ReplaceAll(hash[:12], "-", "")
}

func defaultImportedStreamName(candidate model.SourceCandidate) string {
	if title := strings.TrimSpace(candidate.Title); title != "" {
		return title
	}
	if slug := strings.TrimSpace(candidate.Slug); slug != "" {
		return slug
	}
	if provider := strings.TrimSpace(candidate.Provider); provider != "" && strings.TrimSpace(candidate.ExternalID) != "" {
		return provider + " " + candidate.ExternalID
	}
	if sourceURL := strings.TrimSpace(candidate.SourceURL); sourceURL != "" {
		return sourceURL
	}
	return fmt.Sprintf("candidate-%d", candidate.ID)
}

func (s *Server) createStreamRecord(ctx context.Context, req streamCreateRequest) (*model.Stream, error) {
	if strings.TrimSpace(req.Provider) == "" || strings.TrimSpace(req.ExternalID) == "" || strings.TrimSpace(req.Name) == "" || strings.TrimSpace(req.StreamURL) == "" {
		return nil, newAPIStatusError(http.StatusBadRequest, "provider, external_id, name, source_url are required")
	}
	slug := strings.TrimSpace(req.Slug)
	if slug == "" {
		slug = slugify(req.Provider + "-" + req.ExternalID)
	}
	requestedRecordingState := strings.TrimSpace(strings.ToLower(req.RecordingState))
	if requestedRecordingState != "" && requestedRecordingState != string(model.RecordingStateOff) {
		return nil, newAPIStatusError(http.StatusBadRequest, "create stream with recording_state=off; then set recording_state=on and assign it when you are ready to record")
	}
	profile, err := capture.DeriveCaptureProfile(
		req.Provider,
		req.StreamURL,
		req.SourcePageURL,
		req.CaptureMode,
		req.SourceFamily,
		req.ExecutionClass,
		nonNilMap(req.CaptureConfigJSON),
		req.ExpectedFPS,
		req.ExpectedImageIntervalSec,
	)
	if err != nil {
		return nil, newAPIStatusError(http.StatusBadRequest, "%s", err.Error())
	}
	metaBytes, err := json.Marshal(nonNilMap(req.MetadataJSON))
	if err != nil {
		return nil, newAPIStatusError(http.StatusBadRequest, "invalid metadata_json: %v", err)
	}
	captureCfgBytes, err := json.Marshal(nonNilMap(req.CaptureConfigJSON))
	if err != nil {
		return nil, newAPIStatusError(http.StatusBadRequest, "invalid execution_config_json: %v", err)
	}
	var id int64
	err = s.pool.QueryRow(ctx, `
		INSERT INTO streams (
			provider, external_id, name, slug, source_url, source_page_url,
			lat, lon, location_text, location_country, location_country_code, location_region, location_city, location_locality, location_source, metadata_jsonb,
			recording_state, source_family, capture_type, execution_class, capture_family, expected_fps, expected_image_interval_sec, execution_config_jsonb, tags
		)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25)
		RETURNING id
	`, strings.TrimSpace(req.Provider), strings.TrimSpace(req.ExternalID), strings.TrimSpace(req.Name), slug,
		profile.SourceURL, profile.SourcePageURL,
		req.Lat, req.Lon, strings.TrimSpace(req.LocationText), strings.TrimSpace(req.LocationCountry), strings.ToUpper(strings.TrimSpace(req.LocationCountryCode)),
		strings.TrimSpace(req.LocationRegion), strings.TrimSpace(req.LocationCity), strings.TrimSpace(req.LocationLocality), strings.TrimSpace(req.LocationSource), metaBytes,
		string(model.RecordingStateOff), profile.SourceFamily, profile.CaptureType, profile.ExecutionClass, profile.CaptureFamily, profile.ExpectedFPS, profile.ExpectedImageIntervalSec, captureCfgBytes, dedupeStrings(req.Tags),
	).Scan(&id)
	if err != nil {
		return nil, newAPIStatusError(http.StatusConflict, "create stream: %v", err)
	}
	stream, err := s.getStreamByID(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("load created stream: %w", err)
	}
	return &stream, nil
}

type sourceCandidateCreateRequest struct {
	Provider      string         `json:"provider"`
	ExternalID    string         `json:"external_id"`
	SourceFamily  string         `json:"source_family"`
	CaptureType   string         `json:"capture_type"`
	SourceURL     string         `json:"source_url"`
	SourcePageURL string         `json:"source_page_url"`
	Title         string         `json:"title"`
	Slug          string         `json:"slug"`
	MetadataJSON  map[string]any `json:"metadata_json"`
}

type sourceCandidateReviewRequest struct {
	Status       string         `json:"status"`
	Reviewer     string         `json:"reviewer"`
	Reason       string         `json:"reason"`
	MetadataJSON map[string]any `json:"metadata_json"`
}

type sourceCandidateImportRequest struct {
	Provider                 string         `json:"provider"`
	ExternalID               string         `json:"external_id"`
	Name                     string         `json:"name"`
	Slug                     string         `json:"slug"`
	SourceURL                string         `json:"source_url"`
	SourcePageURL            string         `json:"source_page_url"`
	SourceFamily             string         `json:"source_family"`
	CaptureType              string         `json:"capture_type"`
	ExecutionClass           string         `json:"execution_class"`
	ExpectedFPS              *float64       `json:"expected_fps"`
	ExpectedImageIntervalSec *int           `json:"expected_image_interval_sec"`
	ExecutionConfigJSON      map[string]any `json:"execution_config_json"`
	Tags                     []string       `json:"tags"`
	LocationText             string         `json:"location_text"`
	LocationCountry          string         `json:"location_country"`
	LocationCountryCode      string         `json:"location_country_code"`
	LocationRegion           string         `json:"location_region"`
	LocationCity             string         `json:"location_city"`
	LocationLocality         string         `json:"location_locality"`
	LocationSource           string         `json:"location_source"`
	MetadataJSON             map[string]any `json:"metadata_json"`
}

type sourceCandidateRunRequest struct {
	PipelineID   string         `json:"pipeline_id"`
	WorkerID     string         `json:"worker_id"`
	Status       string         `json:"status"`
	ErrorText    string         `json:"error_text"`
	MetadataJSON map[string]any `json:"metadata_json"`
}

type serviceSourceCandidateAutoImportRequest struct {
	Reviewer           string                       `json:"reviewer"`
	ReviewReason       string                       `json:"review_reason"`
	ReviewMetadataJSON map[string]any               `json:"review_metadata_json"`
	Import             sourceCandidateImportRequest `json:"import"`
}

func normalizeSourceCandidateReviewStatus(raw string) (string, bool) {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case "pending", "accepted", "rejected", "invalid":
		return strings.TrimSpace(strings.ToLower(raw)), true
	default:
		return "", false
	}
}

func normalizeSourceCandidateRunStatus(raw string) (string, bool) {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case "running", "success", "error":
		return strings.TrimSpace(strings.ToLower(raw)), true
	default:
		return "", false
	}
}

func (s *Server) reviewSourceCandidate(ctx context.Context, id int64, req sourceCandidateReviewRequest) (model.SourceCandidate, error) {
	status, ok := normalizeSourceCandidateReviewStatus(req.Status)
	if !ok || status == "pending" {
		return model.SourceCandidate{}, newAPIStatusError(http.StatusBadRequest, "status must be accepted|rejected|invalid")
	}
	metaBytes, err := json.Marshal(nonNilMap(req.MetadataJSON))
	if err != nil {
		return model.SourceCandidate{}, newAPIStatusError(http.StatusBadRequest, "invalid metadata_json: %v", err)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return model.SourceCandidate{}, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `
		INSERT INTO source_candidate_reviews (candidate_id, reviewer, status, reason, metadata_jsonb)
		VALUES ($1,$2,$3,$4,$5)
	`, id, strings.TrimSpace(req.Reviewer), status, strings.TrimSpace(req.Reason), metaBytes); err != nil {
		return model.SourceCandidate{}, newAPIStatusError(http.StatusConflict, "insert source candidate review: %v", err)
	}
	ct, err := tx.Exec(ctx, `
		UPDATE source_candidates
		SET review_status=$2, review_reason=$3, updated_at=now()
		WHERE id=$1
	`, id, status, strings.TrimSpace(req.Reason))
	if err != nil {
		return model.SourceCandidate{}, fmt.Errorf("update source candidate review status: %w", err)
	}
	if ct.RowsAffected() == 0 {
		return model.SourceCandidate{}, newAPIStatusError(http.StatusNotFound, "source candidate not found")
	}
	if err := tx.Commit(ctx); err != nil {
		return model.SourceCandidate{}, fmt.Errorf("commit review tx: %w", err)
	}
	item, err := s.getSourceCandidateByID(ctx, id)
	if err != nil {
		return model.SourceCandidate{}, fmt.Errorf("load source candidate: %w", err)
	}
	return item, nil
}

func (s *Server) importSourceCandidate(ctx context.Context, id int64, req sourceCandidateImportRequest) (model.SourceCandidate, *model.Stream, error) {
	candidate, err := s.getSourceCandidateByID(ctx, id)
	if err != nil {
		return model.SourceCandidate{}, nil, newAPIStatusError(http.StatusNotFound, "source candidate not found")
	}
	if candidate.ReviewStatus != "accepted" {
		return model.SourceCandidate{}, nil, newAPIStatusError(http.StatusConflict, "source candidate must be accepted before import")
	}
	if importedID, ok := candidate.MetadataJSON["imported_stream_id"]; ok {
		switch v := importedID.(type) {
		case float64:
			stream, err := s.getStreamByID(ctx, int64(v))
			if err == nil {
				return candidate, &stream, nil
			}
		case int64:
			stream, err := s.getStreamByID(ctx, v)
			if err == nil {
				return candidate, &stream, nil
			}
		case int:
			stream, err := s.getStreamByID(ctx, int64(v))
			if err == nil {
				return candidate, &stream, nil
			}
		}
	}
	provider := strings.TrimSpace(req.Provider)
	if provider == "" {
		provider = strings.TrimSpace(candidate.Provider)
	}
	if provider == "" {
		return model.SourceCandidate{}, nil, newAPIStatusError(http.StatusBadRequest, "provider is required to import source candidate")
	}
	sourceURL := strings.TrimSpace(req.SourceURL)
	if sourceURL == "" {
		sourceURL = strings.TrimSpace(candidate.SourceURL)
	}
	if sourceURL == "" {
		return model.SourceCandidate{}, nil, newAPIStatusError(http.StatusBadRequest, "source_url is required to import source candidate")
	}
	externalID := strings.TrimSpace(req.ExternalID)
	if externalID == "" {
		externalID = strings.TrimSpace(candidate.ExternalID)
	}
	if externalID == "" {
		externalID = defaultDiscoveryExternalID(sourceURL)
	}
	name := strings.TrimSpace(req.Name)
	if name == "" {
		name = defaultImportedStreamName(candidate)
	}
	metadataJSON := mergeJSONMaps(candidate.MetadataJSON, req.MetadataJSON)
	metadataJSON["source_candidate_id"] = candidate.ID
	stream, err := s.createStreamRecord(ctx, streamCreateRequest{
		Provider:                 provider,
		ExternalID:               externalID,
		Name:                     name,
		Slug:                     strings.TrimSpace(req.Slug),
		StreamURL:                sourceURL,
		SourcePageURL:            firstNonEmpty(strings.TrimSpace(req.SourcePageURL), candidate.SourcePageURL),
		SourceFamily:             firstNonEmpty(strings.TrimSpace(req.SourceFamily), candidate.SourceFamily),
		LocationText:             strings.TrimSpace(req.LocationText),
		LocationCountry:          strings.TrimSpace(req.LocationCountry),
		LocationCountryCode:      strings.TrimSpace(req.LocationCountryCode),
		LocationRegion:           strings.TrimSpace(req.LocationRegion),
		LocationCity:             strings.TrimSpace(req.LocationCity),
		LocationLocality:         strings.TrimSpace(req.LocationLocality),
		LocationSource:           strings.TrimSpace(req.LocationSource),
		MetadataJSON:             metadataJSON,
		RecordingState:           string(model.RecordingStateOff),
		CaptureMode:              firstNonEmpty(strings.TrimSpace(req.CaptureType), candidate.CaptureType),
		ExecutionClass:           strings.TrimSpace(req.ExecutionClass),
		ExpectedFPS:              req.ExpectedFPS,
		ExpectedImageIntervalSec: req.ExpectedImageIntervalSec,
		CaptureConfigJSON:        req.ExecutionConfigJSON,
		Tags:                     req.Tags,
	})
	if err != nil {
		return model.SourceCandidate{}, nil, err
	}
	importMetaBytes, err := json.Marshal(map[string]any{
		"imported_stream_id":   stream.ID,
		"imported_stream_slug": stream.Slug,
		"imported_at":          time.Now().UTC().Format(time.RFC3339),
	})
	if err != nil {
		return model.SourceCandidate{}, nil, fmt.Errorf("marshal import metadata: %w", err)
	}
	if _, err := s.pool.Exec(ctx, `
		UPDATE source_candidates
		SET metadata_jsonb = COALESCE(metadata_jsonb, '{}'::jsonb) || $2::jsonb, updated_at=now()
		WHERE id=$1
	`, candidate.ID, importMetaBytes); err != nil {
		return model.SourceCandidate{}, nil, fmt.Errorf("update source candidate import metadata: %w", err)
	}
	candidate, err = s.getSourceCandidateByID(ctx, id)
	if err != nil {
		return model.SourceCandidate{}, nil, fmt.Errorf("reload source candidate: %w", err)
	}
	return candidate, stream, nil
}

func decodeSourceCandidate(row candidateRow) (model.SourceCandidate, error) {
	var item model.SourceCandidate
	if err := json.Unmarshal(row.MetadataJSON, &item.MetadataJSON); err != nil {
		return item, fmt.Errorf("decode source candidate metadata: %w", err)
	}
	item.ID = row.ID
	item.Provider = row.Provider
	item.ExternalID = row.ExternalID
	item.SourceFamily = row.SourceFamily
	item.CaptureType = row.CaptureType
	item.SourceURL = row.SourceURL
	item.SourcePageURL = row.SourcePageURL
	item.Title = row.Title
	item.Slug = row.Slug
	item.ReviewStatus = row.ReviewStatus
	item.ReviewReason = row.ReviewReason
	item.CreatedAt = row.CreatedAt
	item.UpdatedAt = row.UpdatedAt
	return item, nil
}

type candidateRow struct {
	ID            int64
	Provider      string
	ExternalID    string
	SourceFamily  string
	CaptureType   string
	SourceURL     string
	SourcePageURL string
	Title         string
	Slug          string
	MetadataJSON  []byte
	ReviewStatus  string
	ReviewReason  string
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

func (s *Server) getSourceCandidateByID(ctx context.Context, id int64) (model.SourceCandidate, error) {
	var row candidateRow
	err := s.pool.QueryRow(ctx, `
		SELECT id, provider, external_id, source_family, capture_type, source_url, source_page_url, title, slug,
		       metadata_jsonb, review_status, review_reason, created_at, updated_at
		FROM source_candidates
		WHERE id=$1
	`, id).Scan(
		&row.ID, &row.Provider, &row.ExternalID, &row.SourceFamily, &row.CaptureType, &row.SourceURL, &row.SourcePageURL,
		&row.Title, &row.Slug, &row.MetadataJSON, &row.ReviewStatus, &row.ReviewReason, &row.CreatedAt, &row.UpdatedAt,
	)
	if err != nil {
		return model.SourceCandidate{}, err
	}
	return decodeSourceCandidate(row)
}

func (s *Server) handleSourceCandidatesUpsert(w http.ResponseWriter, r *http.Request) {
	if !s.requireIdempotency(w, r, "POST:/api/v1/source-candidates") {
		return
	}
	var req sourceCandidateCreateRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if strings.TrimSpace(req.Provider) == "" || strings.TrimSpace(req.SourceURL) == "" {
		util.WriteError(w, http.StatusBadRequest, "provider and source_url are required")
		return
	}
	fields, err := capture.DeriveCanonicalStreamFields(req.SourceURL, req.SourcePageURL, req.CaptureType, req.SourceFamily, "")
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	metaBytes, err := json.Marshal(nonNilMap(req.MetadataJSON))
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, fmt.Sprintf("invalid metadata_json: %v", err))
		return
	}
	slug := strings.TrimSpace(req.Slug)
	if slug == "" && strings.TrimSpace(req.Title) != "" {
		slug = slugify(req.Title)
	}
	provider := strings.TrimSpace(req.Provider)
	externalID := strings.TrimSpace(req.ExternalID)
	var id int64
	if provider != "" && externalID != "" {
		err = s.pool.QueryRow(r.Context(), `
			INSERT INTO source_candidates (
				provider, external_id, source_family, capture_type, source_url, source_page_url, title, slug, metadata_jsonb, review_status, review_reason
			)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'pending','')
			ON CONFLICT (provider, external_id)
			WHERE btrim(provider) <> '' AND btrim(external_id) <> ''
			DO UPDATE SET
				source_family=EXCLUDED.source_family,
				capture_type=EXCLUDED.capture_type,
				source_url=EXCLUDED.source_url,
				source_page_url=EXCLUDED.source_page_url,
				title=EXCLUDED.title,
				slug=EXCLUDED.slug,
				metadata_jsonb=EXCLUDED.metadata_jsonb,
				updated_at=now()
			RETURNING id
		`, provider, externalID, fields.SourceFamily, fields.CaptureType, fields.SourceURL, fields.SourcePageURL, strings.TrimSpace(req.Title), slug, metaBytes).Scan(&id)
	} else {
		err = s.pool.QueryRow(r.Context(), `
			INSERT INTO source_candidates (
				provider, external_id, source_family, capture_type, source_url, source_page_url, title, slug, metadata_jsonb, review_status, review_reason
			)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'pending','')
			RETURNING id
		`, provider, externalID, fields.SourceFamily, fields.CaptureType, fields.SourceURL, fields.SourcePageURL, strings.TrimSpace(req.Title), slug, metaBytes).Scan(&id)
	}
	if err != nil {
		util.WriteError(w, http.StatusConflict, fmt.Sprintf("upsert source candidate: %v", err))
		return
	}
	item, err := s.getSourceCandidateByID(r.Context(), id)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load source candidate: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusCreated, item)
}

func (s *Server) handleSourceCandidatesList(w http.ResponseWriter, r *http.Request) {
	limit := parseIntQuery(r, "limit", 200, 1, 2000)
	offset := parseIntQuery(r, "offset", 0, 0, 1000000)
	var idFilter int64
	if idPtr := parseInt64QueryPtr(r, "id"); idPtr != nil && *idPtr > 0 {
		idFilter = *idPtr
	}
	reviewStatus := strings.TrimSpace(strings.ToLower(r.URL.Query().Get("review_status")))
	provider := strings.TrimSpace(r.URL.Query().Get("provider"))
	captureType := strings.TrimSpace(strings.ToLower(r.URL.Query().Get("capture_type")))
	if reviewStatus != "" {
		if _, ok := normalizeSourceCandidateReviewStatus(reviewStatus); !ok {
			util.WriteError(w, http.StatusBadRequest, "invalid review_status")
			return
		}
	}
	if captureType != "" {
		if _, err := normalizeCaptureTypeInput(captureType); err != nil {
			util.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	where := []string{"1=1"}
	args := make([]any, 0, 8)
	add := func(clause string, value any) {
		args = append(args, value)
		where = append(where, fmt.Sprintf(clause, len(args)))
	}
	if idFilter > 0 {
		add("id=$%d", idFilter)
	}
	if reviewStatus != "" {
		add("review_status=$%d", reviewStatus)
	}
	if provider != "" {
		add("LOWER(TRIM(provider))=LOWER(TRIM($%d))", provider)
	}
	if captureType != "" {
		add("capture_type=$%d", captureType)
	}
	countSQL := "SELECT COUNT(*) FROM source_candidates WHERE " + strings.Join(where, " AND ")
	var total int64
	if err := s.pool.QueryRow(r.Context(), countSQL, args...).Scan(&total); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("count source candidates: %v", err))
		return
	}
	args = append(args, limit, offset)
	rows, err := s.pool.Query(r.Context(), `
		SELECT id, provider, external_id, source_family, capture_type, source_url, source_page_url, title, slug,
		       metadata_jsonb, review_status, review_reason, created_at, updated_at
		FROM source_candidates
		WHERE `+strings.Join(where, " AND ")+`
		ORDER BY created_at DESC, id DESC
		LIMIT $`+fmt.Sprint(len(args)-1)+` OFFSET $`+fmt.Sprint(len(args))+`
	`, args...)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("list source candidates: %v", err))
		return
	}
	defer rows.Close()
	items := make([]model.SourceCandidate, 0, limit)
	for rows.Next() {
		var row candidateRow
		if err := rows.Scan(
			&row.ID, &row.Provider, &row.ExternalID, &row.SourceFamily, &row.CaptureType, &row.SourceURL, &row.SourcePageURL,
			&row.Title, &row.Slug, &row.MetadataJSON, &row.ReviewStatus, &row.ReviewReason, &row.CreatedAt, &row.UpdatedAt,
		); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan source candidate: %v", err))
			return
		}
		item, err := decodeSourceCandidate(row)
		if err != nil {
			util.WriteError(w, http.StatusInternalServerError, err.Error())
			return
		}
		items = append(items, item)
	}
	if rows.Err() != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("iterate source candidates: %v", rows.Err()))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{
		"items":  items,
		"limit":  limit,
		"offset": offset,
		"total":  total,
	})
}

func (s *Server) handleSourceCandidateReview(w http.ResponseWriter, r *http.Request) {
	if !s.requireIdempotency(w, r, "POST:/api/v1/source-candidates/{id}/review") {
		return
	}
	id, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	var req sourceCandidateReviewRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	item, err := s.reviewSourceCandidate(r.Context(), id, req)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	util.WriteJSON(w, http.StatusOK, item)
}

func (s *Server) handleSourceCandidateRunCreate(w http.ResponseWriter, r *http.Request) {
	if !s.requireIdempotency(w, r, "POST:/api/v1/source-candidates/{id}/runs") {
		return
	}
	id, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	var req sourceCandidateRunRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	status, ok := normalizeSourceCandidateRunStatus(req.Status)
	if !ok {
		util.WriteError(w, http.StatusBadRequest, "invalid status")
		return
	}
	metaBytes, err := json.Marshal(nonNilMap(req.MetadataJSON))
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, fmt.Sprintf("invalid metadata_json: %v", err))
		return
	}
	var item model.SourceCandidateRun
	var pipelineID *string
	if trimmed := strings.TrimSpace(req.PipelineID); trimmed != "" {
		pipelineID = &trimmed
	}
	err = s.pool.QueryRow(r.Context(), `
		INSERT INTO source_candidate_runs (candidate_id, pipeline_id, worker_id, status, error_text, metadata_jsonb, finished_at)
		VALUES ($1,$2,$3,$4,$5,$6,CASE WHEN $4='running' THEN NULL ELSE now() END)
		RETURNING id, candidate_id, COALESCE(pipeline_id, ''), worker_id, status, error_text, metadata_jsonb, started_at, finished_at, created_at
	`, id, pipelineID, strings.TrimSpace(req.WorkerID), status, strings.TrimSpace(req.ErrorText), metaBytes).Scan(
		&item.ID, &item.CandidateID, &item.PipelineID, &item.WorkerID, &item.Status, &item.ErrorText, &metaBytes, &item.StartedAt, &item.FinishedAt, &item.CreatedAt,
	)
	if err != nil {
		util.WriteError(w, http.StatusConflict, fmt.Sprintf("create source candidate run: %v", err))
		return
	}
	if err := json.Unmarshal(metaBytes, &item.MetadataJSON); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("decode source candidate run metadata: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusCreated, item)
}

func (s *Server) handleSourceCandidateImport(w http.ResponseWriter, r *http.Request) {
	if !s.requireIdempotency(w, r, "POST:/api/v1/source-candidates/{id}/import") {
		return
	}
	id, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	var req sourceCandidateImportRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	candidate, stream, err := s.importSourceCandidate(r.Context(), id, req)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	util.WriteJSON(w, http.StatusCreated, map[string]any{
		"candidate": candidate,
		"stream":    stream,
	})
}

func (s *Server) handleServiceSourceCandidateAutoImport(w http.ResponseWriter, r *http.Request) {
	if !s.requireIdempotency(w, r, "POST:/api/v1/source-candidates/{id}/auto-import") {
		return
	}
	id, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	var req serviceSourceCandidateAutoImportRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	candidate, err := s.getSourceCandidateByID(r.Context(), id)
	if err != nil {
		util.WriteError(w, http.StatusNotFound, "source candidate not found")
		return
	}
	if candidate.ReviewStatus != "accepted" {
		reviewed, err := s.reviewSourceCandidate(r.Context(), id, sourceCandidateReviewRequest{
			Status:       "accepted",
			Reviewer:     strings.TrimSpace(req.Reviewer),
			Reason:       firstNonEmpty(strings.TrimSpace(req.ReviewReason), "auto-accepted by discovery pipeline"),
			MetadataJSON: req.ReviewMetadataJSON,
		})
		if err != nil {
			writeAPIError(w, err)
			return
		}
		candidate = reviewed
	}
	candidate, stream, err := s.importSourceCandidate(r.Context(), id, req.Import)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	util.WriteJSON(w, http.StatusCreated, map[string]any{
		"candidate": candidate,
		"stream":    stream,
	})
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
