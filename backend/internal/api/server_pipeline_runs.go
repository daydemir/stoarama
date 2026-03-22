package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/daydemir/stoarama/backend/internal/model"
	"github.com/daydemir/stoarama/backend/internal/queue"
	"github.com/daydemir/stoarama/backend/internal/util"
)

type pipelineVersionSpec struct {
	PipelineID string         `json:"pipeline_id"`
	VersionID  string         `json:"version_id"`
	RunnerKind string         `json:"runner_kind"`
	SpecJSON   map[string]any `json:"spec_json"`
	CreatedBy  string         `json:"created_by"`
}

type pipelineVersionSyncRequest struct {
	Versions []pipelineVersionSpec `json:"versions"`
}

type pipelineRunCreateRequest struct {
	PipelineID           string         `json:"pipeline_id"`
	VersionID            string         `json:"version_id"`
	Label                string         `json:"label"`
	WorkerKind           string         `json:"worker_kind"`
	FrameIDs             []int64        `json:"frame_ids"`
	StreamIDs            []int64        `json:"stream_ids"`
	Tags                 []string       `json:"tags"`
	LatestOnlyPerStream  bool           `json:"latest_only_per_stream"`
	Limit                int            `json:"limit"`
	MetadataJSON         map[string]any `json:"metadata_json"`
	CreatedBy            string         `json:"created_by"`
}

type pipelineRunClaimRequest struct {
	ClaimedBy  string `json:"claimed_by"`
	Limit      int    `json:"limit"`
	LeaseSec   int    `json:"lease_sec"`
	ForceRerun bool   `json:"force_rerun"`
}

func (s *Server) handlePipelineVersionsSync(w http.ResponseWriter, r *http.Request) {
	if !s.requireIdempotency(w, r, "POST:/api/v1/pipeline-versions/sync") {
		return
	}
	var req pipelineVersionSyncRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if len(req.Versions) == 0 {
		util.WriteError(w, http.StatusBadRequest, "versions is required")
		return
	}
	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("begin tx: %v", err))
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()

	for i := range req.Versions {
		spec := req.Versions[i]
		pipelineID := strings.TrimSpace(spec.PipelineID)
		versionID := strings.TrimSpace(spec.VersionID)
		if pipelineID == "" || versionID == "" {
			util.WriteError(w, http.StatusBadRequest, "pipeline_id and version_id are required")
			return
		}
		var pipelineExists bool
		if err := tx.QueryRow(r.Context(), `SELECT EXISTS(SELECT 1 FROM pipelines WHERE id=$1)`, pipelineID).Scan(&pipelineExists); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("check pipeline: %v", err))
			return
		}
		if !pipelineExists {
			util.WriteError(w, http.StatusBadRequest, fmt.Sprintf("pipeline not found: %s", pipelineID))
			return
		}
		runnerKind := strings.TrimSpace(spec.RunnerKind)
		if runnerKind == "" {
			runnerKind = "external"
		}
		specBytes, err := json.Marshal(nonNilMap(spec.SpecJSON))
		if err != nil {
			util.WriteError(w, http.StatusBadRequest, fmt.Sprintf("invalid spec_json for %s:%s: %v", pipelineID, versionID, err))
			return
		}
		var existingID int64
		var existingRunner string
		var existingSpec []byte
		err = tx.QueryRow(r.Context(), `
			SELECT id, runner_kind, spec_jsonb
			FROM pipeline_versions
			WHERE pipeline_id=$1 AND version_id=$2
		`, pipelineID, versionID).Scan(&existingID, &existingRunner, &existingSpec)
		switch {
		case err == nil:
			if existingRunner != runnerKind || string(existingSpec) != string(specBytes) {
				util.WriteError(w, http.StatusConflict, fmt.Sprintf("pipeline version is immutable once created: %s@%s", pipelineID, versionID))
				return
			}
		case err == pgx.ErrNoRows:
			if _, err := tx.Exec(r.Context(), `
				INSERT INTO pipeline_versions (pipeline_id, version_id, runner_kind, spec_jsonb, created_by)
				VALUES ($1,$2,$3,$4,$5)
			`, pipelineID, versionID, runnerKind, specBytes, strings.TrimSpace(spec.CreatedBy)); err != nil {
				util.WriteError(w, http.StatusConflict, fmt.Sprintf("insert pipeline version %s@%s: %v", pipelineID, versionID, err))
				return
			}
		default:
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load pipeline version %s@%s: %v", pipelineID, versionID, err))
			return
		}
	}
	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("commit tx: %v", err))
		return
	}
	s.handlePipelineVersionsList(w, r)
}

func (s *Server) handlePipelineVersionsList(w http.ResponseWriter, r *http.Request) {
	pipelineID := strings.TrimSpace(r.URL.Query().Get("pipeline_id"))
	args := []any{}
	query := `
		SELECT id, pipeline_id, version_id, runner_kind, spec_jsonb, created_by, created_at
		FROM pipeline_versions
	`
	if pipelineID != "" {
		args = append(args, pipelineID)
		query += ` WHERE pipeline_id=$1`
	}
	query += ` ORDER BY pipeline_id ASC, created_at DESC, id DESC`
	rows, err := s.pool.Query(r.Context(), query, args...)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("list pipeline versions: %v", err))
		return
	}
	defer rows.Close()
	items := make([]model.PipelineVersion, 0, 128)
	for rows.Next() {
		var item model.PipelineVersion
		var specBytes []byte
		if err := rows.Scan(&item.ID, &item.PipelineID, &item.VersionID, &item.RunnerKind, &specBytes, &item.CreatedBy, &item.CreatedAt); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan pipeline version: %v", err))
			return
		}
		if err := json.Unmarshal(specBytes, &item.SpecJSON); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("decode pipeline version spec_json: %v", err))
			return
		}
		items = append(items, item)
	}
	if rows.Err() != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("iterate pipeline versions: %v", rows.Err()))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"items": items})
}

func normalizePipelineRunWorkerKind(raw string) string {
	kind := strings.TrimSpace(strings.ToLower(raw))
	if kind == "" {
		return "external"
	}
	return kind
}

func normalizePipelineRunSelector(req pipelineRunCreateRequest) (map[string]any, error) {
	frameIDs := dedupeInt64s(req.FrameIDs)
	streamIDs := dedupeInt64s(req.StreamIDs)
	tags := dedupeStrings(req.Tags)
	if len(frameIDs) > 0 && (len(streamIDs) > 0 || len(tags) > 0 || req.LatestOnlyPerStream) {
		return nil, fmt.Errorf("frame_ids cannot be combined with stream_ids, tags, or latest_only_per_stream")
	}
	selector := map[string]any{}
	if len(frameIDs) > 0 {
		selector["frame_ids"] = frameIDs
	}
	if len(streamIDs) > 0 {
		selector["stream_ids"] = streamIDs
	}
	if len(tags) > 0 {
		selector["tags"] = tags
	}
	if req.LatestOnlyPerStream {
		selector["latest_only_per_stream"] = true
	}
	if req.Limit > 0 {
		selector["limit"] = req.Limit
	}
	return selector, nil
}

func dedupeInt64s(values []int64) []int64 {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[int64]struct{}, len(values))
	out := make([]int64, 0, len(values))
	for _, v := range values {
		if v <= 0 {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out
}

func materializePipelineRunTargets(ctx context.Context, tx pgx.Tx, runID int64, selector map[string]any) (int64, error) {
	frameIDs, _ := selector["frame_ids"].([]int64)
	streamIDs, _ := selector["stream_ids"].([]int64)
	tags, _ := selector["tags"].([]string)
	latestOnly, _ := selector["latest_only_per_stream"].(bool)
	limit, _ := selector["limit"].(int)

	args := []any{runID}
	where := []string{"f.capture_status='success'"}
	joinStreams := len(streamIDs) > 0 || len(tags) > 0

	if len(frameIDs) > 0 {
		args = append(args, frameIDs)
		where = append(where, fmt.Sprintf("f.id = ANY($%d)", len(args)))
	}
	if len(streamIDs) > 0 {
		args = append(args, streamIDs)
		where = append(where, fmt.Sprintf("f.stream_id = ANY($%d)", len(args)))
	}
	if len(tags) > 0 {
		args = append(args, tags)
		where = append(where, fmt.Sprintf("s.tags && $%d", len(args)))
	}

	baseJoin := ""
	if joinStreams {
		baseJoin = "JOIN streams s ON s.id = f.stream_id"
	}
	baseWhere := strings.Join(where, " AND ")
	limitClause := ""
	if limit > 0 {
		args = append(args, limit)
		limitClause = fmt.Sprintf(" LIMIT $%d", len(args))
	}

	var query string
	if latestOnly && len(frameIDs) == 0 {
		query = fmt.Sprintf(`
			INSERT INTO pipeline_run_targets (run_id, frame_id, stream_id)
			SELECT $1, picked.id, picked.stream_id
			FROM (
				SELECT DISTINCT ON (f.stream_id) f.id, f.stream_id, f.captured_at
				FROM frames f
				%s
				WHERE %s
				ORDER BY f.stream_id ASC, f.captured_at DESC, f.id DESC
			) AS picked
			ORDER BY picked.captured_at DESC, picked.id DESC
			%s
			ON CONFLICT (run_id, frame_id) DO NOTHING
		`, baseJoin, baseWhere, limitClause)
	} else {
		query = fmt.Sprintf(`
			INSERT INTO pipeline_run_targets (run_id, frame_id, stream_id)
			SELECT $1, f.id, f.stream_id
			FROM frames f
			%s
			WHERE %s
			ORDER BY f.captured_at ASC, f.id ASC
			%s
			ON CONFLICT (run_id, frame_id) DO NOTHING
		`, baseJoin, baseWhere, limitClause)
	}

	ct, err := tx.Exec(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("materialize pipeline run targets: %w", err)
	}
	return ct.RowsAffected(), nil
}

func refreshPipelineRunStatus(ctx context.Context, tx pgx.Tx, runID int64) error {
	_, err := tx.Exec(ctx, `
		WITH stats AS (
			SELECT
				COUNT(*)::bigint AS total,
				COUNT(*) FILTER (WHERE status='completed')::bigint AS completed,
				COUNT(*) FILTER (WHERE status='error')::bigint AS errored,
				COUNT(*) FILTER (WHERE status='leased')::bigint AS leased,
				COUNT(*) FILTER (WHERE status IN ('pending', 'abandoned'))::bigint AS remaining
			FROM pipeline_run_targets
			WHERE run_id=$1
		)
		UPDATE pipeline_runs pr
		SET status = CASE
				WHEN stats.total = 0 THEN 'completed'
				WHEN stats.leased = 0 AND stats.remaining = 0 AND stats.errored = 0 THEN 'completed'
				WHEN stats.leased = 0 AND stats.remaining = 0 AND stats.errored > 0 THEN 'completed_with_errors'
				WHEN stats.completed > 0 OR stats.errored > 0 OR stats.leased > 0 THEN 'running'
				ELSE pr.status
			END,
			started_at = CASE
				WHEN pr.started_at IS NULL AND (stats.completed > 0 OR stats.errored > 0 OR stats.leased > 0) THEN now()
				ELSE pr.started_at
			END,
			finished_at = CASE
				WHEN stats.total = 0 THEN COALESCE(pr.finished_at, now())
				WHEN stats.leased = 0 AND stats.remaining = 0 THEN now()
				ELSE NULL
			END
		FROM stats
		WHERE pr.id=$1
	`, runID)
	if err != nil {
		return fmt.Errorf("refresh pipeline run status: %w", err)
	}
	return nil
}

func (s *Server) queryPipelineRuns(ctx context.Context, runID int64, pipelineID string, limit int, offset int) ([]model.PipelineRun, error) {
	where := []string{"1=1"}
	args := []any{}
	if runID > 0 {
		args = append(args, runID)
		where = append(where, fmt.Sprintf("pr.id=$%d", len(args)))
	}
	if strings.TrimSpace(pipelineID) != "" {
		args = append(args, strings.TrimSpace(pipelineID))
		where = append(where, fmt.Sprintf("pr.pipeline_id=$%d", len(args)))
	}
	if limit <= 0 {
		limit = 200
	}
	if offset < 0 {
		offset = 0
	}
	args = append(args, limit, offset)
	rows, err := s.pool.Query(ctx, fmt.Sprintf(`
		SELECT
			pr.id, pr.pipeline_id, pr.pipeline_version_id, pv.version_id, pr.label, pr.status, pr.worker_kind,
			pr.selector_jsonb, pr.metadata_jsonb, pr.created_by, pr.created_at, pr.started_at, pr.finished_at,
			COUNT(prt.id)::bigint AS target_count,
			COUNT(*) FILTER (WHERE prt.status='completed')::bigint AS completed_count,
			COUNT(*) FILTER (WHERE prt.status='error')::bigint AS error_count,
			COUNT(*) FILTER (WHERE prt.status='leased')::bigint AS leased_count
		FROM pipeline_runs pr
		JOIN pipeline_versions pv ON pv.id = pr.pipeline_version_id
		LEFT JOIN pipeline_run_targets prt ON prt.run_id = pr.id
		WHERE %s
		GROUP BY pr.id, pv.version_id
		ORDER BY pr.created_at DESC, pr.id DESC
		LIMIT $%d OFFSET $%d
	`, strings.Join(where, " AND "), len(args)-1, len(args)), args...)
	if err != nil {
		return nil, fmt.Errorf("query pipeline runs: %w", err)
	}
	defer rows.Close()

	items := make([]model.PipelineRun, 0, limit)
	for rows.Next() {
		var item model.PipelineRun
		var selectorBytes []byte
		var metadataBytes []byte
		if err := rows.Scan(
			&item.ID, &item.PipelineID, &item.PipelineVersionID, &item.VersionID, &item.Label, &item.Status, &item.WorkerKind,
			&selectorBytes, &metadataBytes, &item.CreatedBy, &item.CreatedAt, &item.StartedAt, &item.FinishedAt,
			&item.TargetCount, &item.CompletedCount, &item.ErrorCount, &item.LeasedCount,
		); err != nil {
			return nil, fmt.Errorf("scan pipeline run: %w", err)
		}
		if err := json.Unmarshal(selectorBytes, &item.SelectorJSON); err != nil {
			return nil, fmt.Errorf("decode pipeline run selector: %w", err)
		}
		if err := json.Unmarshal(metadataBytes, &item.MetadataJSON); err != nil {
			return nil, fmt.Errorf("decode pipeline run metadata: %w", err)
		}
		items = append(items, item)
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("iterate pipeline runs: %w", rows.Err())
	}
	return items, nil
}

func (s *Server) handlePipelineRunsList(w http.ResponseWriter, r *http.Request) {
	limit := parseIntQuery(r, "limit", 200, 1, 2000)
	offset := parseIntQuery(r, "offset", 0, 0, 1000000)
	pipelineID := strings.TrimSpace(r.URL.Query().Get("pipeline_id"))
	items, err := s.queryPipelineRuns(r.Context(), 0, pipelineID, limit, offset)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (s *Server) handlePipelineRunGet(w http.ResponseWriter, r *http.Request) {
	runID, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	items, err := s.queryPipelineRuns(r.Context(), runID, "", 1, 0)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if len(items) == 0 {
		util.WriteError(w, http.StatusNotFound, "pipeline run not found")
		return
	}
	util.WriteJSON(w, http.StatusOK, items[0])
}

func (s *Server) handlePipelineRunsCreate(w http.ResponseWriter, r *http.Request) {
	if !s.requireIdempotency(w, r, "POST:/api/v1/pipeline-runs") {
		return
	}
	var req pipelineRunCreateRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	pipelineID := strings.TrimSpace(req.PipelineID)
	versionID := strings.TrimSpace(req.VersionID)
	if pipelineID == "" || versionID == "" {
		util.WriteError(w, http.StatusBadRequest, "pipeline_id and version_id are required")
		return
	}
	selector, err := normalizePipelineRunSelector(req)
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	selectorBytes, err := json.Marshal(selector)
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, fmt.Sprintf("invalid selector: %v", err))
		return
	}
	metadataBytes, err := json.Marshal(nonNilMap(req.MetadataJSON))
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, fmt.Sprintf("invalid metadata_json: %v", err))
		return
	}

	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("begin tx: %v", err))
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()

	var versionRowID int64
	var resolvedPipelineID string
	if err := tx.QueryRow(r.Context(), `
		SELECT pv.id, pv.pipeline_id
		FROM pipeline_versions pv
		JOIN pipelines p ON p.id = pv.pipeline_id
		WHERE pv.pipeline_id=$1 AND pv.version_id=$2 AND p.active=true
	`, pipelineID, versionID).Scan(&versionRowID, &resolvedPipelineID); err != nil {
		if err == pgx.ErrNoRows {
			util.WriteError(w, http.StatusBadRequest, "active pipeline version not found")
			return
		}
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load pipeline version: %v", err))
		return
	}

	var runID int64
	workerKind := normalizePipelineRunWorkerKind(req.WorkerKind)
	if err := tx.QueryRow(r.Context(), `
		INSERT INTO pipeline_runs (
			pipeline_id, pipeline_version_id, label, status, worker_kind, selector_jsonb, metadata_jsonb, created_by
		)
		VALUES ($1,$2,$3,'pending',$4,$5,$6,$7)
		RETURNING id
	`, resolvedPipelineID, versionRowID, strings.TrimSpace(req.Label), workerKind, selectorBytes, metadataBytes, strings.TrimSpace(req.CreatedBy)).Scan(&runID); err != nil {
		util.WriteError(w, http.StatusConflict, fmt.Sprintf("create pipeline run: %v", err))
		return
	}
	if _, err := materializePipelineRunTargets(r.Context(), tx, runID, map[string]any{
		"frame_ids":               dedupeInt64s(req.FrameIDs),
		"stream_ids":              dedupeInt64s(req.StreamIDs),
		"tags":                    dedupeStrings(req.Tags),
		"latest_only_per_stream":  req.LatestOnlyPerStream,
		"limit":                   req.Limit,
	}); err != nil {
		util.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if err := refreshPipelineRunStatus(r.Context(), tx, runID); err != nil {
		util.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("commit tx: %v", err))
		return
	}
	items, err := s.queryPipelineRuns(r.Context(), runID, "", 1, 0)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	util.WriteJSON(w, http.StatusCreated, items[0])
}

func (s *Server) handlePipelineRunClaims(w http.ResponseWriter, r *http.Request) {
	runID, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	var req pipelineRunClaimRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if strings.TrimSpace(req.ClaimedBy) == "" {
		util.WriteError(w, http.StatusBadRequest, "claimed_by is required")
		return
	}
	if req.Limit <= 0 {
		req.Limit = 100
	}
	if req.LeaseSec <= 0 {
		req.LeaseSec = 600
	}
	claims, err := queue.ClaimFramesForRun(r.Context(), s.pool, queue.RunClaimFilter{
		RunID:      runID,
		Limit:      req.Limit,
		LeaseSec:   req.LeaseSec,
		ClaimedBy:  strings.TrimSpace(req.ClaimedBy),
		ForceRerun: req.ForceRerun,
	})
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if len(claims) > 0 {
		if _, err := s.pool.Exec(r.Context(), `
			UPDATE pipeline_runs
			SET status='running', started_at=COALESCE(started_at, now())
			WHERE id=$1 AND status IN ('pending', 'running')
		`, runID); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("update pipeline run status: %v", err))
			return
		}
	}
	type claimResp struct {
		ClaimID           int64      `json:"claim_id"`
		RunID             int64      `json:"run_id"`
		FrameID           int64      `json:"frame_id"`
		StreamID          int64      `json:"stream_id"`
		CapturedAt        time.Time  `json:"captured_at"`
		PipelineID        string     `json:"pipeline_id"`
		PipelineVersionID *int64     `json:"pipeline_version_id,omitempty"`
		LeaseExpires      time.Time  `json:"lease_expires_at"`
		ObjectKey         string     `json:"object_key"`
		MIMEType          string     `json:"mime_type"`
		SizeBytes         int64      `json:"size_bytes"`
		Width             int        `json:"width"`
		Height            int        `json:"height"`
		DownloadURL       string     `json:"download_url"`
		ClaimedBy         string     `json:"claimed_by"`
	}
	items := make([]claimResp, 0, len(claims))
	for _, c := range claims {
		url, err := s.r2.PresignGet(r.Context(), c.ObjectKey, s.cfg.R2SignGetTTL)
		if err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("presign frame url: %v", err))
			return
		}
		items = append(items, claimResp{
			ClaimID:           c.ClaimID,
			RunID:             runID,
			FrameID:           c.FrameID,
			StreamID:          c.StreamID,
			CapturedAt:        c.CapturedAt,
			PipelineID:        c.PipelineID,
			PipelineVersionID: c.PipelineVersionID,
			LeaseExpires:      c.LeaseExpires,
			ObjectKey:         c.ObjectKey,
			MIMEType:          c.MIMEType,
			SizeBytes:         c.SizeBytes,
			Width:             c.Width,
			Height:            c.Height,
			DownloadURL:       url,
			ClaimedBy:         strings.TrimSpace(req.ClaimedBy),
		})
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"items": items})
}
