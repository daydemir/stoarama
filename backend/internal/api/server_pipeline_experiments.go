package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"

	"github.com/daydemir/stoarama/backend/internal/model"
	"github.com/daydemir/stoarama/backend/internal/util"
)

type evalSuiteSyncRequest struct {
	Suites []evalSuiteSyncSpec `json:"suites"`
}

type evalSuiteSyncSpec struct {
	ID             *int64                  `json:"id,omitempty"`
	OwnerAccountID *int64                  `json:"owner_account_id,omitempty"`
	OwnerEmail     string                  `json:"owner_email,omitempty"`
	Slug           string                  `json:"slug"`
	Title          string                  `json:"title"`
	Description    string                  `json:"description"`
	SourceKind     string                  `json:"source_kind"`
	PrimaryMetric  string                  `json:"primary_metric"`
	SourceURL      string                  `json:"source_url"`
	MetadataJSON   map[string]any          `json:"metadata_json"`
	CreatedBy      string                  `json:"created_by"`
	Items          []evalSuiteItemSyncSpec `json:"items"`
}

type evalSuiteItemSyncSpec struct {
	ID           *int64                   `json:"id,omitempty"`
	FrameID      *int64                   `json:"frame_id,omitempty"`
	ItemKey      string                   `json:"item_key"`
	Split        string                   `json:"split"`
	SourceLabel  string                   `json:"source_label"`
	SourceURL    string                   `json:"source_url"`
	MetadataJSON map[string]any           `json:"metadata_json"`
	Annotations  []evalAnnotationSyncSpec `json:"annotations"`
}

type evalAnnotationSyncSpec struct {
	AnnotationKind string         `json:"annotation_kind"`
	LabelJSON      map[string]any `json:"label_json"`
	CreatedBy      string         `json:"created_by"`
}

type pipelineExperimentSyncRequest struct {
	Experiments []pipelineExperimentSyncSpec `json:"experiments"`
}

type pipelineExperimentSyncSpec struct {
	ID             *int64         `json:"id,omitempty"`
	PipelineID     string         `json:"pipeline_id"`
	OwnerAccountID *int64         `json:"owner_account_id,omitempty"`
	OwnerEmail     string         `json:"owner_email,omitempty"`
	Slug           string         `json:"slug"`
	Title          string         `json:"title"`
	GoalText       string         `json:"goal_text"`
	PrimaryMetric  string         `json:"primary_metric"`
	Active         *bool          `json:"active,omitempty"`
	MetadataJSON   map[string]any `json:"metadata_json"`
	CreatedBy      string         `json:"created_by"`
}

type pipelineExperimentIterationSyncRequest struct {
	Iterations []pipelineExperimentIterationSyncSpec `json:"iterations"`
}

type pipelineExperimentIterationSyncSpec struct {
	ID                         *int64                       `json:"id,omitempty"`
	ExperimentID               *int64                       `json:"experiment_id,omitempty"`
	ExperimentSlug             string                       `json:"experiment_slug,omitempty"`
	PipelineID                 string                       `json:"pipeline_id,omitempty"`
	OwnerAccountID             *int64                       `json:"owner_account_id,omitempty"`
	OwnerEmail                 string                       `json:"owner_email,omitempty"`
	CandidatePipelineVersionID *int64                       `json:"candidate_pipeline_version_id,omitempty"`
	BaselinePipelineVersionID  *int64                       `json:"baseline_pipeline_version_id,omitempty"`
	IterationIndex             int                          `json:"iteration_index"`
	Status                     string                       `json:"status"`
	HypothesisText             string                       `json:"hypothesis_text"`
	ChangeSummary              string                       `json:"change_summary"`
	ChangeJSON                 map[string]any               `json:"change_json"`
	ResultClassification       string                       `json:"result_classification"`
	PrimaryMetricBefore        *float64                     `json:"primary_metric_before,omitempty"`
	PrimaryMetricAfter         *float64                     `json:"primary_metric_after,omitempty"`
	PrimaryMetricDelta         *float64                     `json:"primary_metric_delta,omitempty"`
	LogURL                     string                       `json:"log_url"`
	ArtifactURL                string                       `json:"artifact_url"`
	MetadataJSON               map[string]any               `json:"metadata_json"`
	CreatedBy                  string                       `json:"created_by"`
	RunIDs                     []int64                      `json:"run_ids"`
	BaselineRunIDs             []int64                      `json:"baseline_run_ids"`
	SuiteIDs                   []int64                      `json:"suite_ids"`
	Metrics                    []pipelineEvalMetricSyncSpec `json:"metrics"`
}

type pipelineEvalMetricSyncSpec struct {
	SuiteID           int64          `json:"suite_id"`
	PipelineID        string         `json:"pipeline_id"`
	PipelineVersionID *int64         `json:"pipeline_version_id,omitempty"`
	PipelineRunID     *int64         `json:"pipeline_run_id,omitempty"`
	MetricName        string         `json:"metric_name"`
	Split             string         `json:"split"`
	MetricValue       float64        `json:"metric_value"`
	MetadataJSON      map[string]any `json:"metadata_json"`
}

func (s *Server) resolveOwnerForScopedWrite(ctx context.Context, explicitID *int64, explicitEmail string) (*int64, error) {
	if principal, ok := accountPrincipalFromContext(ctx); ok {
		if explicitID != nil && *explicitID > 0 && *explicitID != principal.AccountID {
			return nil, newAPIStatusError(http.StatusForbidden, "owner_account_id must match the authenticated account")
		}
		if email := normalizeAccountEmail(explicitEmail); email != "" && email != normalizeAccountEmail(principal.Email) {
			return nil, newAPIStatusError(http.StatusForbidden, "owner_email must match the authenticated account")
		}
		owner := principal.AccountID
		return &owner, nil
	}
	return s.resolveAccountRef(ctx, explicitID, explicitEmail)
}

func validateEvalSourceKind(raw string) string {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case "", "stoarama":
		return "stoarama"
	case "public":
		return "public"
	case "hybrid":
		return "hybrid"
	default:
		return ""
	}
}

func normalizeExperimentStatus(raw string) string {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case "", "pending":
		return "pending"
	case "running":
		return "running"
	case "completed":
		return "completed"
	case "completed_with_errors":
		return "completed_with_errors"
	case "error":
		return "error"
	case "canceled":
		return "canceled"
	default:
		return ""
	}
}

func normalizeExperimentResultClassification(raw string) string {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case "", "pending":
		return "pending"
	case "better":
		return "better"
	case "neutral":
		return "neutral"
	case "worse":
		return "worse"
	case "error":
		return "error"
	default:
		return ""
	}
}

func normalizeAnnotationKind(raw string) string {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case "", "bbox":
		return "bbox"
	case "track":
		return "track"
	case "attribute":
		return "attribute"
	default:
		return ""
	}
}

func (s *Server) handleEvalSuitesSync(w http.ResponseWriter, r *http.Request) {
	if !s.requireIdempotency(w, r, "POST:/api/v1/eval-suites/sync") {
		return
	}
	var req evalSuiteSyncRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if len(req.Suites) == 0 {
		util.WriteError(w, http.StatusBadRequest, "suites is required")
		return
	}
	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("begin tx: %v", err))
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()

	for i := range req.Suites {
		spec := req.Suites[i]
		if strings.TrimSpace(spec.Slug) == "" || strings.TrimSpace(spec.Title) == "" {
			util.WriteError(w, http.StatusBadRequest, "suite slug and title are required")
			return
		}
		sourceKind := validateEvalSourceKind(spec.SourceKind)
		if sourceKind == "" {
			util.WriteError(w, http.StatusBadRequest, "invalid source_kind; expected public|stoarama|hybrid")
			return
		}
		ownerAccountID, err := s.resolveOwnerForScopedWrite(r.Context(), spec.OwnerAccountID, spec.OwnerEmail)
		if err != nil {
			writeAPIError(w, err)
			return
		}
		if ownerAccountID == nil || *ownerAccountID <= 0 {
			util.WriteError(w, http.StatusBadRequest, "owner_account_id or owner_email is required")
			return
		}
		metaBytes, err := json.Marshal(nonNilMap(spec.MetadataJSON))
		if err != nil {
			util.WriteError(w, http.StatusBadRequest, fmt.Sprintf("invalid suite metadata_json for %s: %v", spec.Slug, err))
			return
		}

		var suiteID int64
		err = tx.QueryRow(r.Context(), `
			INSERT INTO eval_suites (
				owner_account_id, slug, title, description, source_kind, primary_metric, source_url, metadata_jsonb, created_by
			)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
			ON CONFLICT (owner_account_id, slug)
			DO UPDATE SET
				title=EXCLUDED.title,
				description=EXCLUDED.description,
				source_kind=EXCLUDED.source_kind,
				primary_metric=EXCLUDED.primary_metric,
				source_url=EXCLUDED.source_url,
				metadata_jsonb=EXCLUDED.metadata_jsonb,
				updated_at=now()
			RETURNING id
		`, ownerAccountID, strings.TrimSpace(spec.Slug), strings.TrimSpace(spec.Title), strings.TrimSpace(spec.Description), sourceKind, strings.TrimSpace(spec.PrimaryMetric), strings.TrimSpace(spec.SourceURL), metaBytes, strings.TrimSpace(spec.CreatedBy)).Scan(&suiteID)
		if err != nil {
			util.WriteError(w, http.StatusConflict, fmt.Sprintf("upsert eval suite %s: %v", spec.Slug, err))
			return
		}

		for j := range spec.Items {
			item := spec.Items[j]
			itemKey := strings.TrimSpace(item.ItemKey)
			if itemKey == "" {
				util.WriteError(w, http.StatusBadRequest, fmt.Sprintf("suite item missing item_key for suite %s", spec.Slug))
				return
			}
			itemMetaBytes, err := json.Marshal(nonNilMap(item.MetadataJSON))
			if err != nil {
				util.WriteError(w, http.StatusBadRequest, fmt.Sprintf("invalid item metadata_json for suite %s item %s: %v", spec.Slug, itemKey, err))
				return
			}
			var suiteItemID int64
			err = tx.QueryRow(r.Context(), `
				INSERT INTO eval_suite_items (
					suite_id, frame_id, item_key, split, source_label, source_url, metadata_jsonb
				)
				VALUES ($1,$2,$3,$4,$5,$6,$7)
				ON CONFLICT (suite_id, item_key)
				DO UPDATE SET
					frame_id=EXCLUDED.frame_id,
					split=EXCLUDED.split,
					source_label=EXCLUDED.source_label,
					source_url=EXCLUDED.source_url,
					metadata_jsonb=EXCLUDED.metadata_jsonb,
					updated_at=now()
				RETURNING id
			`, suiteID, item.FrameID, itemKey, strings.TrimSpace(defaultString(item.Split, "benchmark")), strings.TrimSpace(item.SourceLabel), strings.TrimSpace(item.SourceURL), itemMetaBytes).Scan(&suiteItemID)
			if err != nil {
				util.WriteError(w, http.StatusConflict, fmt.Sprintf("upsert eval suite item %s/%s: %v", spec.Slug, itemKey, err))
				return
			}
			if _, err := tx.Exec(r.Context(), `DELETE FROM eval_annotations WHERE suite_item_id=$1`, suiteItemID); err != nil {
				util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("clear eval annotations %s/%s: %v", spec.Slug, itemKey, err))
				return
			}
			for k := range item.Annotations {
				ann := item.Annotations[k]
				annotationKind := normalizeAnnotationKind(ann.AnnotationKind)
				if annotationKind == "" {
					util.WriteError(w, http.StatusBadRequest, fmt.Sprintf("invalid annotation_kind for suite %s item %s", spec.Slug, itemKey))
					return
				}
				labelBytes, err := json.Marshal(nonNilMap(ann.LabelJSON))
				if err != nil {
					util.WriteError(w, http.StatusBadRequest, fmt.Sprintf("invalid label_json for suite %s item %s: %v", spec.Slug, itemKey, err))
					return
				}
				if _, err := tx.Exec(r.Context(), `
					INSERT INTO eval_annotations (suite_item_id, annotation_kind, label_jsonb, created_by)
					VALUES ($1,$2,$3,$4)
				`, suiteItemID, annotationKind, labelBytes, strings.TrimSpace(ann.CreatedBy)); err != nil {
					util.WriteError(w, http.StatusConflict, fmt.Sprintf("insert eval annotation %s/%s: %v", spec.Slug, itemKey, err))
					return
				}
			}
		}
	}
	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("commit tx: %v", err))
		return
	}
	s.handleEvalSuitesList(w, r)
}

func (s *Server) handleEvalSuitesList(w http.ResponseWriter, r *http.Request) {
	items, err := s.listEvalSuites(r.Context(), parseInt64QueryPtr(r, "pipeline_experiment_id"))
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("list eval suites: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (s *Server) listEvalSuites(ctx context.Context, experimentID *int64) ([]model.EvalSuite, error) {
	where := []string{"1=1"}
	args := []any{}
	if ownerAccountID, ok := pipelineOwnerAccountScope(ctx); ok {
		args = append(args, ownerAccountID)
		where = append(where, fmt.Sprintf("es.owner_account_id=$%d", len(args)))
	}
	if experimentID != nil && *experimentID > 0 {
		args = append(args, *experimentID)
		where = append(where, fmt.Sprintf(`EXISTS (
			SELECT 1
			FROM pipeline_experiment_iteration_suites peis
			JOIN pipeline_experiment_iterations pei ON pei.id=peis.iteration_id
			WHERE peis.suite_id=es.id AND pei.experiment_id=$%d
		)`, len(args)))
	}
	rows, err := s.pool.Query(ctx, fmt.Sprintf(`
		SELECT
			es.id, es.owner_account_id, es.slug, es.title, es.description, es.source_kind, es.primary_metric, es.source_url,
			es.metadata_jsonb, es.created_by, es.created_at, es.updated_at,
			COALESCE(items.item_count, 0)::bigint,
			COALESCE(anns.annotation_count, 0)::bigint
		FROM eval_suites es
		LEFT JOIN (
			SELECT suite_id, COUNT(*)::bigint AS item_count
			FROM eval_suite_items
			GROUP BY suite_id
		) items ON items.suite_id=es.id
		LEFT JOIN (
			SELECT esi.suite_id, COUNT(ea.id)::bigint AS annotation_count
			FROM eval_suite_items esi
			LEFT JOIN eval_annotations ea ON ea.suite_item_id=esi.id
			GROUP BY esi.suite_id
		) anns ON anns.suite_id=es.id
		WHERE %s
		ORDER BY es.created_at DESC, es.id DESC
	`, strings.Join(where, " AND ")), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]model.EvalSuite, 0, 32)
	for rows.Next() {
		var item model.EvalSuite
		var metaBytes []byte
		if err := rows.Scan(
			&item.ID, &item.OwnerAccountID, &item.Slug, &item.Title, &item.Description, &item.SourceKind, &item.PrimaryMetric, &item.SourceURL,
			&metaBytes, &item.CreatedBy, &item.CreatedAt, &item.UpdatedAt, &item.ItemCount, &item.AnnotationCount,
		); err != nil {
			return nil, err
		}
		if len(metaBytes) > 0 {
			if err := json.Unmarshal(metaBytes, &item.MetadataJSON); err != nil {
				return nil, err
			}
		}
		if item.MetadataJSON == nil {
			item.MetadataJSON = map[string]any{}
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Server) handleEvalSuiteGet(w http.ResponseWriter, r *http.Request) {
	suiteID, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	suite, items, annotationsByItem, err := s.loadEvalSuiteDetail(r.Context(), suiteID)
	if err != nil {
		if err == pgx.ErrNoRows {
			util.WriteError(w, http.StatusNotFound, "eval suite not found")
			return
		}
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load eval suite: %v", err))
		return
	}
	if ownerAccountID, ok := pipelineOwnerAccountScope(r.Context()); ok {
		if suite.OwnerAccountID == nil || *suite.OwnerAccountID != ownerAccountID {
			util.WriteError(w, http.StatusNotFound, "eval suite not found")
			return
		}
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{
		"suite":               suite,
		"items":               items,
		"annotations_by_item": annotationsByItem,
	})
}

func (s *Server) loadEvalSuiteDetail(ctx context.Context, suiteID int64) (model.EvalSuite, []model.EvalSuiteItem, map[int64][]model.EvalAnnotation, error) {
	var suite model.EvalSuite
	var metaBytes []byte
	err := s.pool.QueryRow(ctx, `
		SELECT
			es.id, es.owner_account_id, es.slug, es.title, es.description, es.source_kind, es.primary_metric, es.source_url,
			es.metadata_jsonb, es.created_by, es.created_at, es.updated_at,
			COALESCE(items.item_count, 0)::bigint,
			COALESCE(anns.annotation_count, 0)::bigint
		FROM eval_suites es
		LEFT JOIN (
			SELECT suite_id, COUNT(*)::bigint AS item_count
			FROM eval_suite_items
			GROUP BY suite_id
		) items ON items.suite_id=es.id
		LEFT JOIN (
			SELECT esi.suite_id, COUNT(ea.id)::bigint AS annotation_count
			FROM eval_suite_items esi
			LEFT JOIN eval_annotations ea ON ea.suite_item_id=esi.id
			GROUP BY esi.suite_id
		) anns ON anns.suite_id=es.id
		WHERE es.id=$1
	`, suiteID).Scan(
		&suite.ID, &suite.OwnerAccountID, &suite.Slug, &suite.Title, &suite.Description, &suite.SourceKind, &suite.PrimaryMetric, &suite.SourceURL,
		&metaBytes, &suite.CreatedBy, &suite.CreatedAt, &suite.UpdatedAt, &suite.ItemCount, &suite.AnnotationCount,
	)
	if err != nil {
		return suite, nil, nil, err
	}
	if len(metaBytes) > 0 {
		if err := json.Unmarshal(metaBytes, &suite.MetadataJSON); err != nil {
			return suite, nil, nil, err
		}
	}
	if suite.MetadataJSON == nil {
		suite.MetadataJSON = map[string]any{}
	}

	itemRows, err := s.pool.Query(ctx, `
		SELECT id, suite_id, frame_id, item_key, split, source_label, source_url, metadata_jsonb, created_at, updated_at
		FROM eval_suite_items
		WHERE suite_id=$1
		ORDER BY id ASC
		LIMIT 500
	`, suiteID)
	if err != nil {
		return suite, nil, nil, err
	}
	defer itemRows.Close()
	items := make([]model.EvalSuiteItem, 0, 128)
	itemIDs := make([]int64, 0, 128)
	for itemRows.Next() {
		var item model.EvalSuiteItem
		var itemMeta []byte
		if err := itemRows.Scan(&item.ID, &item.SuiteID, &item.FrameID, &item.ItemKey, &item.Split, &item.SourceLabel, &item.SourceURL, &itemMeta, &item.CreatedAt, &item.UpdatedAt); err != nil {
			return suite, nil, nil, err
		}
		if len(itemMeta) > 0 {
			if err := json.Unmarshal(itemMeta, &item.MetadataJSON); err != nil {
				return suite, nil, nil, err
			}
		}
		if item.MetadataJSON == nil {
			item.MetadataJSON = map[string]any{}
		}
		items = append(items, item)
		itemIDs = append(itemIDs, item.ID)
	}
	if err := itemRows.Err(); err != nil {
		return suite, nil, nil, err
	}
	annotationsByItem := map[int64][]model.EvalAnnotation{}
	if len(itemIDs) == 0 {
		return suite, items, annotationsByItem, nil
	}
	annRows, err := s.pool.Query(ctx, `
		SELECT id, suite_item_id, annotation_kind, label_jsonb, created_by, created_at
		FROM eval_annotations
		WHERE suite_item_id = ANY($1)
		ORDER BY id ASC
	`, itemIDs)
	if err != nil {
		return suite, nil, nil, err
	}
	defer annRows.Close()
	for annRows.Next() {
		var ann model.EvalAnnotation
		var labelBytes []byte
		if err := annRows.Scan(&ann.ID, &ann.SuiteItemID, &ann.AnnotationKind, &labelBytes, &ann.CreatedBy, &ann.CreatedAt); err != nil {
			return suite, nil, nil, err
		}
		if len(labelBytes) > 0 {
			if err := json.Unmarshal(labelBytes, &ann.LabelJSON); err != nil {
				return suite, nil, nil, err
			}
		}
		if ann.LabelJSON == nil {
			ann.LabelJSON = map[string]any{}
		}
		annotationsByItem[ann.SuiteItemID] = append(annotationsByItem[ann.SuiteItemID], ann)
	}
	return suite, items, annotationsByItem, annRows.Err()
}

func (s *Server) handlePipelineExperimentsSync(w http.ResponseWriter, r *http.Request) {
	if !s.requireIdempotency(w, r, "POST:/api/v1/pipeline-experiments/sync") {
		return
	}
	var req pipelineExperimentSyncRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if len(req.Experiments) == 0 {
		util.WriteError(w, http.StatusBadRequest, "experiments is required")
		return
	}
	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("begin tx: %v", err))
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()

	for i := range req.Experiments {
		spec := req.Experiments[i]
		pipelineID := strings.TrimSpace(spec.PipelineID)
		if pipelineID == "" || strings.TrimSpace(spec.Slug) == "" || strings.TrimSpace(spec.Title) == "" {
			util.WriteError(w, http.StatusBadRequest, "pipeline_id, slug, and title are required")
			return
		}
		ownerAccountID, err := s.resolveOwnerForScopedWrite(r.Context(), spec.OwnerAccountID, spec.OwnerEmail)
		if err != nil {
			writeAPIError(w, err)
			return
		}
		if ownerAccountID == nil || *ownerAccountID <= 0 {
			util.WriteError(w, http.StatusBadRequest, "owner_account_id or owner_email is required")
			return
		}
		var pipelineOwnerID *int64
		if err := tx.QueryRow(r.Context(), `SELECT owner_account_id FROM pipelines WHERE id=$1`, pipelineID).Scan(&pipelineOwnerID); err != nil {
			if err == pgx.ErrNoRows {
				util.WriteError(w, http.StatusBadRequest, fmt.Sprintf("pipeline not found: %s", pipelineID))
				return
			}
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load pipeline owner: %v", err))
			return
		}
		if pipelineOwnerID != nil && *pipelineOwnerID != *ownerAccountID {
			util.WriteError(w, http.StatusConflict, "experiment owner must match the pipeline owner")
			return
		}
		active := true
		if spec.Active != nil {
			active = *spec.Active
		}
		metaBytes, err := json.Marshal(nonNilMap(spec.MetadataJSON))
		if err != nil {
			util.WriteError(w, http.StatusBadRequest, fmt.Sprintf("invalid experiment metadata_json for %s: %v", spec.Slug, err))
			return
		}
		if _, err := tx.Exec(r.Context(), `
			INSERT INTO pipeline_experiments (
				owner_account_id, pipeline_id, slug, title, goal_text, primary_metric, active, metadata_jsonb, created_by
			)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
			ON CONFLICT (owner_account_id, pipeline_id, slug)
			DO UPDATE SET
				title=EXCLUDED.title,
				goal_text=EXCLUDED.goal_text,
				primary_metric=EXCLUDED.primary_metric,
				active=EXCLUDED.active,
				metadata_jsonb=EXCLUDED.metadata_jsonb,
				updated_at=now()
		`, ownerAccountID, pipelineID, strings.TrimSpace(spec.Slug), strings.TrimSpace(spec.Title), strings.TrimSpace(spec.GoalText), strings.TrimSpace(defaultString(spec.PrimaryMetric, "detection_f1")), active, metaBytes, strings.TrimSpace(spec.CreatedBy)); err != nil {
			util.WriteError(w, http.StatusConflict, fmt.Sprintf("upsert pipeline experiment %s/%s: %v", pipelineID, spec.Slug, err))
			return
		}
	}
	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("commit tx: %v", err))
		return
	}
	s.handlePipelineExperimentsList(w, r)
}

func (s *Server) handlePipelineExperimentsList(w http.ResponseWriter, r *http.Request) {
	items, err := s.listPipelineExperiments(r.Context(), strings.TrimSpace(r.URL.Query().Get("pipeline_id")))
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("list pipeline experiments: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (s *Server) listPipelineExperiments(ctx context.Context, pipelineID string) ([]model.PipelineExperiment, error) {
	where := []string{"1=1"}
	args := []any{}
	if ownerAccountID, ok := pipelineOwnerAccountScope(ctx); ok {
		args = append(args, ownerAccountID)
		where = append(where, fmt.Sprintf("pe.owner_account_id=$%d", len(args)))
	}
	if pipelineID != "" {
		args = append(args, pipelineID)
		where = append(where, fmt.Sprintf("pe.pipeline_id=$%d", len(args)))
	}
	rows, err := s.pool.Query(ctx, fmt.Sprintf(`
		SELECT id, owner_account_id, pipeline_id, slug, title, goal_text, primary_metric, active, metadata_jsonb, created_by, created_at, updated_at
		FROM pipeline_experiments pe
		WHERE %s
		ORDER BY created_at DESC, id DESC
	`, strings.Join(where, " AND ")), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]model.PipelineExperiment, 0, 32)
	for rows.Next() {
		var item model.PipelineExperiment
		var metaBytes []byte
		if err := rows.Scan(&item.ID, &item.OwnerAccountID, &item.PipelineID, &item.Slug, &item.Title, &item.GoalText, &item.PrimaryMetric, &item.Active, &metaBytes, &item.CreatedBy, &item.CreatedAt, &item.UpdatedAt); err != nil {
			return nil, err
		}
		if len(metaBytes) > 0 {
			if err := json.Unmarshal(metaBytes, &item.MetadataJSON); err != nil {
				return nil, err
			}
		}
		if item.MetadataJSON == nil {
			item.MetadataJSON = map[string]any{}
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Server) resolveExperimentRef(ctx context.Context, tx pgx.Tx, explicitID *int64, pipelineID, slug string, ownerAccountID *int64) (int64, error) {
	if explicitID != nil && *explicitID > 0 {
		if ownerAccountID != nil && *ownerAccountID > 0 {
			var existingOwner *int64
			if err := tx.QueryRow(ctx, `SELECT owner_account_id FROM pipeline_experiments WHERE id=$1`, *explicitID).Scan(&existingOwner); err != nil {
				if err == pgx.ErrNoRows {
					return 0, newAPIStatusError(http.StatusBadRequest, "experiment not found")
				}
				return 0, err
			}
			if existingOwner == nil || *existingOwner != *ownerAccountID {
				return 0, newAPIStatusError(http.StatusForbidden, "experiment does not belong to the authenticated account")
			}
		}
		return *explicitID, nil
	}
	if strings.TrimSpace(pipelineID) == "" || strings.TrimSpace(slug) == "" || ownerAccountID == nil || *ownerAccountID <= 0 {
		return 0, newAPIStatusError(http.StatusBadRequest, "experiment_id or pipeline_id+experiment_slug+owner is required")
	}
	var experimentID int64
	if err := tx.QueryRow(ctx, `
		SELECT id
		FROM pipeline_experiments
		WHERE owner_account_id=$1 AND pipeline_id=$2 AND slug=$3
	`, *ownerAccountID, strings.TrimSpace(pipelineID), strings.TrimSpace(slug)).Scan(&experimentID); err != nil {
		if err == pgx.ErrNoRows {
			return 0, newAPIStatusError(http.StatusBadRequest, "experiment not found for pipeline_id + experiment_slug")
		}
		return 0, err
	}
	return experimentID, nil
}

func (s *Server) handlePipelineExperimentIterationsSync(w http.ResponseWriter, r *http.Request) {
	if !s.requireIdempotency(w, r, "POST:/api/v1/pipeline-experiment-iterations/sync") {
		return
	}
	var req pipelineExperimentIterationSyncRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if len(req.Iterations) == 0 {
		util.WriteError(w, http.StatusBadRequest, "iterations is required")
		return
	}
	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("begin tx: %v", err))
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()

	for i := range req.Iterations {
		spec := req.Iterations[i]
		status := normalizeExperimentStatus(spec.Status)
		if status == "" {
			util.WriteError(w, http.StatusBadRequest, "invalid iteration status")
			return
		}
		classification := normalizeExperimentResultClassification(spec.ResultClassification)
		if classification == "" {
			util.WriteError(w, http.StatusBadRequest, "invalid result_classification")
			return
		}
		ownerAccountID, err := s.resolveOwnerForScopedWrite(r.Context(), spec.OwnerAccountID, spec.OwnerEmail)
		if err != nil {
			writeAPIError(w, err)
			return
		}
		experimentID, err := s.resolveExperimentRef(r.Context(), tx, spec.ExperimentID, spec.PipelineID, spec.ExperimentSlug, ownerAccountID)
		if err != nil {
			writeAPIError(w, err)
			return
		}
		changeBytes, err := json.Marshal(nonNilMap(spec.ChangeJSON))
		if err != nil {
			util.WriteError(w, http.StatusBadRequest, fmt.Sprintf("invalid change_json: %v", err))
			return
		}
		metaBytes, err := json.Marshal(nonNilMap(spec.MetadataJSON))
		if err != nil {
			util.WriteError(w, http.StatusBadRequest, fmt.Sprintf("invalid metadata_json: %v", err))
			return
		}
		var iterationID int64
		err = tx.QueryRow(r.Context(), `
			INSERT INTO pipeline_experiment_iterations (
				experiment_id, candidate_pipeline_version_id, baseline_pipeline_version_id,
				iteration_index, status, hypothesis_text, change_summary, change_jsonb,
				result_classification, primary_metric_before, primary_metric_after, primary_metric_delta,
				log_url, artifact_url, metadata_jsonb, created_by
			)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
			ON CONFLICT (experiment_id, iteration_index)
			DO UPDATE SET
				candidate_pipeline_version_id=EXCLUDED.candidate_pipeline_version_id,
				baseline_pipeline_version_id=EXCLUDED.baseline_pipeline_version_id,
				status=EXCLUDED.status,
				hypothesis_text=EXCLUDED.hypothesis_text,
				change_summary=EXCLUDED.change_summary,
				change_jsonb=EXCLUDED.change_jsonb,
				result_classification=EXCLUDED.result_classification,
				primary_metric_before=EXCLUDED.primary_metric_before,
				primary_metric_after=EXCLUDED.primary_metric_after,
				primary_metric_delta=EXCLUDED.primary_metric_delta,
				log_url=EXCLUDED.log_url,
				artifact_url=EXCLUDED.artifact_url,
				metadata_jsonb=EXCLUDED.metadata_jsonb,
				updated_at=now()
			RETURNING id
		`, experimentID, spec.CandidatePipelineVersionID, spec.BaselinePipelineVersionID, spec.IterationIndex, status, strings.TrimSpace(spec.HypothesisText), strings.TrimSpace(spec.ChangeSummary), changeBytes, classification, spec.PrimaryMetricBefore, spec.PrimaryMetricAfter, spec.PrimaryMetricDelta, strings.TrimSpace(spec.LogURL), strings.TrimSpace(spec.ArtifactURL), metaBytes, strings.TrimSpace(spec.CreatedBy)).Scan(&iterationID)
		if err != nil {
			util.WriteError(w, http.StatusConflict, fmt.Sprintf("upsert experiment iteration %d: %v", spec.IterationIndex, err))
			return
		}

		if _, err := tx.Exec(r.Context(), `DELETE FROM pipeline_experiment_iteration_runs WHERE iteration_id=$1`, iterationID); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("clear iteration runs: %v", err))
			return
		}
		for _, runID := range dedupeSortedInt64s(spec.BaselineRunIDs) {
			if _, err := tx.Exec(r.Context(), `
				INSERT INTO pipeline_experiment_iteration_runs (iteration_id, pipeline_run_id, run_role)
				VALUES ($1,$2,'baseline')
			`, iterationID, runID); err != nil {
				util.WriteError(w, http.StatusConflict, fmt.Sprintf("insert baseline run link: %v", err))
				return
			}
		}
		for _, runID := range dedupeSortedInt64s(spec.RunIDs) {
			role := "candidate"
			if containsInt64(spec.BaselineRunIDs, runID) {
				continue
			}
			if _, err := tx.Exec(r.Context(), `
				INSERT INTO pipeline_experiment_iteration_runs (iteration_id, pipeline_run_id, run_role)
				VALUES ($1,$2,$3)
			`, iterationID, runID, role); err != nil {
				util.WriteError(w, http.StatusConflict, fmt.Sprintf("insert candidate run link: %v", err))
				return
			}
		}

		if _, err := tx.Exec(r.Context(), `DELETE FROM pipeline_experiment_iteration_suites WHERE iteration_id=$1`, iterationID); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("clear iteration suites: %v", err))
			return
		}
		for _, suiteID := range dedupeSortedInt64s(spec.SuiteIDs) {
			if _, err := tx.Exec(r.Context(), `
				INSERT INTO pipeline_experiment_iteration_suites (iteration_id, suite_id)
				VALUES ($1,$2)
			`, iterationID, suiteID); err != nil {
				util.WriteError(w, http.StatusConflict, fmt.Sprintf("insert iteration suite link: %v", err))
				return
			}
		}

		if _, err := tx.Exec(r.Context(), `DELETE FROM pipeline_eval_metrics WHERE experiment_iteration_id=$1`, iterationID); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("clear iteration metrics: %v", err))
			return
		}
		for _, metric := range spec.Metrics {
			if metric.SuiteID <= 0 || strings.TrimSpace(metric.PipelineID) == "" || strings.TrimSpace(metric.MetricName) == "" {
				util.WriteError(w, http.StatusBadRequest, "iteration metrics require suite_id, pipeline_id, and metric_name")
				return
			}
			metricMetaBytes, err := json.Marshal(nonNilMap(metric.MetadataJSON))
			if err != nil {
				util.WriteError(w, http.StatusBadRequest, fmt.Sprintf("invalid metric metadata_json: %v", err))
				return
			}
			if _, err := tx.Exec(r.Context(), `
				INSERT INTO pipeline_eval_metrics (
					experiment_iteration_id, suite_id, pipeline_id, pipeline_version_id, pipeline_run_id, metric_name, split, metric_value, metadata_jsonb
				)
				VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
			`, iterationID, metric.SuiteID, strings.TrimSpace(metric.PipelineID), metric.PipelineVersionID, metric.PipelineRunID, strings.TrimSpace(metric.MetricName), strings.TrimSpace(metric.Split), metric.MetricValue, metricMetaBytes); err != nil {
				util.WriteError(w, http.StatusConflict, fmt.Sprintf("insert eval metric: %v", err))
				return
			}
		}
	}
	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("commit tx: %v", err))
		return
	}
	s.handlePipelineExperimentsList(w, r)
}

func (s *Server) handlePipelineExperimentGet(w http.ResponseWriter, r *http.Request) {
	experimentID, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	experiment, iterations, suiteIDsByIteration, runIDsByIteration, metricsByIteration, err := s.loadPipelineExperimentDetail(r.Context(), experimentID)
	if err != nil {
		if err == pgx.ErrNoRows {
			util.WriteError(w, http.StatusNotFound, "pipeline experiment not found")
			return
		}
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load pipeline experiment: %v", err))
		return
	}
	if ownerAccountID, ok := pipelineOwnerAccountScope(r.Context()); ok {
		if experiment.OwnerAccountID == nil || *experiment.OwnerAccountID != ownerAccountID {
			util.WriteError(w, http.StatusNotFound, "pipeline experiment not found")
			return
		}
	}
	suiteSet := map[int64]struct{}{}
	for _, ids := range suiteIDsByIteration {
		for _, id := range ids {
			suiteSet[id] = struct{}{}
		}
	}
	suiteIDs := make([]int64, 0, len(suiteSet))
	for id := range suiteSet {
		suiteIDs = append(suiteIDs, id)
	}
	sort.Slice(suiteIDs, func(i, j int) bool { return suiteIDs[i] < suiteIDs[j] })
	suites := make([]model.EvalSuite, 0, len(suiteIDs))
	if len(suiteIDs) > 0 {
		allSuites, err := s.listEvalSuites(r.Context(), &experimentID)
		if err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load experiment suites: %v", err))
			return
		}
		suites = allSuites
	}
	graph := make([]map[string]any, 0, len(iterations))
	for _, it := range iterations {
		graph = append(graph, map[string]any{
			"iteration_index":       it.IterationIndex,
			"primary_metric_after":  it.PrimaryMetricAfter,
			"primary_metric_before": it.PrimaryMetricBefore,
			"primary_metric_delta":  it.PrimaryMetricDelta,
			"result_classification": it.ResultClassification,
			"status":                it.Status,
			"iteration_id":          it.ID,
			"change_summary":        it.ChangeSummary,
			"run_ids":               runIDsByIteration[it.ID],
			"suite_ids":             suiteIDsByIteration[it.ID],
			"metrics_count":         len(metricsByIteration[it.ID]),
		})
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{
		"experiment":           experiment,
		"iterations":           iterations,
		"iteration_suite_ids":  suiteIDsByIteration,
		"iteration_run_ids":    runIDsByIteration,
		"metrics_by_iteration": metricsByIteration,
		"suites":               suites,
		"metric_graph":         graph,
	})
}

func (s *Server) loadPipelineExperimentDetail(ctx context.Context, experimentID int64) (model.PipelineExperiment, []model.PipelineExperimentIteration, map[int64][]int64, map[int64][]int64, map[int64][]model.PipelineEvalMetric, error) {
	var experiment model.PipelineExperiment
	var metaBytes []byte
	err := s.pool.QueryRow(ctx, `
		SELECT id, owner_account_id, pipeline_id, slug, title, goal_text, primary_metric, active, metadata_jsonb, created_by, created_at, updated_at
		FROM pipeline_experiments
		WHERE id=$1
	`, experimentID).Scan(&experiment.ID, &experiment.OwnerAccountID, &experiment.PipelineID, &experiment.Slug, &experiment.Title, &experiment.GoalText, &experiment.PrimaryMetric, &experiment.Active, &metaBytes, &experiment.CreatedBy, &experiment.CreatedAt, &experiment.UpdatedAt)
	if err != nil {
		return experiment, nil, nil, nil, nil, err
	}
	if len(metaBytes) > 0 {
		if err := json.Unmarshal(metaBytes, &experiment.MetadataJSON); err != nil {
			return experiment, nil, nil, nil, nil, err
		}
	}
	if experiment.MetadataJSON == nil {
		experiment.MetadataJSON = map[string]any{}
	}

	rows, err := s.pool.Query(ctx, `
		SELECT
			id, experiment_id, candidate_pipeline_version_id, baseline_pipeline_version_id,
			iteration_index, status, hypothesis_text, change_summary, change_jsonb,
			result_classification, primary_metric_before, primary_metric_after, primary_metric_delta,
			log_url, artifact_url, metadata_jsonb, created_by, created_at, updated_at
		FROM pipeline_experiment_iterations
		WHERE experiment_id=$1
		ORDER BY iteration_index ASC, id ASC
	`, experimentID)
	if err != nil {
		return experiment, nil, nil, nil, nil, err
	}
	defer rows.Close()
	iterations := make([]model.PipelineExperimentIteration, 0, 32)
	iterationIDs := make([]int64, 0, 32)
	for rows.Next() {
		var it model.PipelineExperimentIteration
		var changeBytes []byte
		var iterationMeta []byte
		if err := rows.Scan(
			&it.ID, &it.ExperimentID, &it.CandidatePipelineVersionID, &it.BaselinePipelineVersionID,
			&it.IterationIndex, &it.Status, &it.HypothesisText, &it.ChangeSummary, &changeBytes,
			&it.ResultClassification, &it.PrimaryMetricBefore, &it.PrimaryMetricAfter, &it.PrimaryMetricDelta,
			&it.LogURL, &it.ArtifactURL, &iterationMeta, &it.CreatedBy, &it.CreatedAt, &it.UpdatedAt,
		); err != nil {
			return experiment, nil, nil, nil, nil, err
		}
		if len(changeBytes) > 0 {
			if err := json.Unmarshal(changeBytes, &it.ChangeJSON); err != nil {
				return experiment, nil, nil, nil, nil, err
			}
		}
		if it.ChangeJSON == nil {
			it.ChangeJSON = map[string]any{}
		}
		if len(iterationMeta) > 0 {
			if err := json.Unmarshal(iterationMeta, &it.MetadataJSON); err != nil {
				return experiment, nil, nil, nil, nil, err
			}
		}
		if it.MetadataJSON == nil {
			it.MetadataJSON = map[string]any{}
		}
		iterations = append(iterations, it)
		iterationIDs = append(iterationIDs, it.ID)
	}
	if err := rows.Err(); err != nil {
		return experiment, nil, nil, nil, nil, err
	}
	suiteIDsByIteration := map[int64][]int64{}
	runIDsByIteration := map[int64][]int64{}
	metricsByIteration := map[int64][]model.PipelineEvalMetric{}
	if len(iterationIDs) == 0 {
		return experiment, iterations, suiteIDsByIteration, runIDsByIteration, metricsByIteration, nil
	}
	suiteRows, err := s.pool.Query(ctx, `
		SELECT iteration_id, suite_id
		FROM pipeline_experiment_iteration_suites
		WHERE iteration_id = ANY($1)
	`, iterationIDs)
	if err != nil {
		return experiment, nil, nil, nil, nil, err
	}
	defer suiteRows.Close()
	for suiteRows.Next() {
		var iterationID, suiteID int64
		if err := suiteRows.Scan(&iterationID, &suiteID); err != nil {
			return experiment, nil, nil, nil, nil, err
		}
		suiteIDsByIteration[iterationID] = append(suiteIDsByIteration[iterationID], suiteID)
	}
	if err := suiteRows.Err(); err != nil {
		return experiment, nil, nil, nil, nil, err
	}
	runRows, err := s.pool.Query(ctx, `
		SELECT iteration_id, pipeline_run_id
		FROM pipeline_experiment_iteration_runs
		WHERE iteration_id = ANY($1)
	`, iterationIDs)
	if err != nil {
		return experiment, nil, nil, nil, nil, err
	}
	defer runRows.Close()
	for runRows.Next() {
		var iterationID, runID int64
		if err := runRows.Scan(&iterationID, &runID); err != nil {
			return experiment, nil, nil, nil, nil, err
		}
		runIDsByIteration[iterationID] = append(runIDsByIteration[iterationID], runID)
	}
	if err := runRows.Err(); err != nil {
		return experiment, nil, nil, nil, nil, err
	}
	metricRows, err := s.pool.Query(ctx, `
		SELECT id, experiment_iteration_id, suite_id, pipeline_id, pipeline_version_id, pipeline_run_id, metric_name, split, metric_value, metadata_jsonb, created_at
		FROM pipeline_eval_metrics
		WHERE experiment_iteration_id = ANY($1)
		ORDER BY created_at ASC, id ASC
	`, iterationIDs)
	if err != nil {
		return experiment, nil, nil, nil, nil, err
	}
	defer metricRows.Close()
	for metricRows.Next() {
		var metric model.PipelineEvalMetric
		var metricMeta []byte
		if err := metricRows.Scan(&metric.ID, &metric.ExperimentIterationID, &metric.SuiteID, &metric.PipelineID, &metric.PipelineVersionID, &metric.PipelineRunID, &metric.MetricName, &metric.Split, &metric.MetricValue, &metricMeta, &metric.CreatedAt); err != nil {
			return experiment, nil, nil, nil, nil, err
		}
		if len(metricMeta) > 0 {
			if err := json.Unmarshal(metricMeta, &metric.MetadataJSON); err != nil {
				return experiment, nil, nil, nil, nil, err
			}
		}
		if metric.MetadataJSON == nil {
			metric.MetadataJSON = map[string]any{}
		}
		if metric.ExperimentIterationID != nil {
			metricsByIteration[*metric.ExperimentIterationID] = append(metricsByIteration[*metric.ExperimentIterationID], metric)
		}
	}
	return experiment, iterations, suiteIDsByIteration, runIDsByIteration, metricsByIteration, metricRows.Err()
}

func (s *Server) handleDashboardPipelineDetail(w http.ResponseWriter, r *http.Request) {
	pipelineID := strings.TrimSpace(chi.URLParam(r, "pipeline_id"))
	if pipelineID == "" {
		util.WriteError(w, http.StatusBadRequest, "pipeline_id is required")
		return
	}
	var ownerScope *int64
	if ownerAccountID, ok := pipelineOwnerAccountScope(r.Context()); ok {
		ownerScope = &ownerAccountID
	}
	pipelineRows, err := s.pool.Query(r.Context(), `
		SELECT id, owner_account_id, pipeline_family, kind, spec_jsonb, active, created_at, updated_at
		FROM pipelines
		WHERE id=$1
	`, pipelineID)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load pipeline: %v", err))
		return
	}
	defer pipelineRows.Close()
	var pipeline model.Pipeline
	if !pipelineRows.Next() {
		util.WriteError(w, http.StatusNotFound, "pipeline not found")
		return
	}
	var pipelineSpec []byte
	if err := pipelineRows.Scan(&pipeline.ID, &pipeline.OwnerAccountID, &pipeline.PipelineFamily, &pipeline.Kind, &pipelineSpec, &pipeline.Active, &pipeline.CreatedAt, &pipeline.UpdatedAt); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan pipeline: %v", err))
		return
	}
	if len(pipelineSpec) > 0 {
		if err := json.Unmarshal(pipelineSpec, &pipeline.SpecJSON); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("decode pipeline spec: %v", err))
			return
		}
	}
	if ownerScope != nil && (pipeline.OwnerAccountID == nil || *pipeline.OwnerAccountID != *ownerScope) {
		util.WriteError(w, http.StatusNotFound, "pipeline not found")
		return
	}
	versionsResp, err := s.pool.Query(r.Context(), `
		SELECT id, pipeline_id, owner_account_id, version_id, runner_kind, spec_jsonb, created_by, created_at
		FROM pipeline_versions
		WHERE pipeline_id=$1
		ORDER BY created_at DESC, id DESC
		LIMIT 20
	`, pipelineID)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load pipeline versions: %v", err))
		return
	}
	defer versionsResp.Close()
	versions := make([]model.PipelineVersion, 0, 20)
	for versionsResp.Next() {
		var item model.PipelineVersion
		var specBytes []byte
		if err := versionsResp.Scan(&item.ID, &item.PipelineID, &item.OwnerAccountID, &item.VersionID, &item.RunnerKind, &specBytes, &item.CreatedBy, &item.CreatedAt); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan pipeline version: %v", err))
			return
		}
		if len(specBytes) > 0 {
			if err := json.Unmarshal(specBytes, &item.SpecJSON); err != nil {
				util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("decode pipeline version spec: %v", err))
				return
			}
		}
		if item.SpecJSON == nil {
			item.SpecJSON = map[string]any{}
		}
		versions = append(versions, item)
	}
	recentRuns := make([]model.PipelineRun, 0, 20)
	runRows, err := s.pool.Query(r.Context(), `
		SELECT
			pr.id, pr.pipeline_id, pr.owner_account_id, pr.pipeline_version_id,
			pv.version_id, pv.runner_kind, pv.spec_jsonb,
			pr.label, pr.status, pr.worker_kind, pr.selector_jsonb, pr.metadata_jsonb, pr.created_by,
			COALESCE(stats.target_count, 0)::bigint,
			COALESCE(stats.completed_count, 0)::bigint,
			COALESCE(stats.error_count, 0)::bigint,
			COALESCE(stats.leased_count, 0)::bigint,
			pr.created_at, pr.started_at, pr.finished_at
		FROM pipeline_runs pr
		JOIN pipeline_versions pv ON pv.id=pr.pipeline_version_id
		LEFT JOIN (
			SELECT
				run_id,
				COUNT(*)::bigint AS target_count,
				COUNT(*) FILTER (WHERE status='completed')::bigint AS completed_count,
				COUNT(*) FILTER (WHERE status='error')::bigint AS error_count,
				COUNT(*) FILTER (WHERE status='leased')::bigint AS leased_count
			FROM pipeline_run_targets
			GROUP BY run_id
		) stats ON stats.run_id=pr.id
		WHERE pr.pipeline_id=$1
		ORDER BY pr.created_at DESC, pr.id DESC
		LIMIT 20
	`, pipelineID)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load recent pipeline runs: %v", err))
		return
	}
	defer runRows.Close()
	for runRows.Next() {
		var item model.PipelineRun
		var specBytes []byte
		var selectorBytes []byte
		var metadataBytes []byte
		if err := runRows.Scan(&item.ID, &item.PipelineID, &item.OwnerAccountID, &item.PipelineVersionID, &item.VersionID, &item.VersionRunnerKind, &specBytes, &item.Label, &item.Status, &item.WorkerKind, &selectorBytes, &metadataBytes, &item.CreatedBy, &item.TargetCount, &item.CompletedCount, &item.ErrorCount, &item.LeasedCount, &item.CreatedAt, &item.StartedAt, &item.FinishedAt); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan recent pipeline run: %v", err))
			return
		}
		if len(specBytes) > 0 {
			_ = json.Unmarshal(specBytes, &item.VersionSpecJSON)
		}
		if len(selectorBytes) > 0 {
			_ = json.Unmarshal(selectorBytes, &item.SelectorJSON)
		}
		if len(metadataBytes) > 0 {
			_ = json.Unmarshal(metadataBytes, &item.MetadataJSON)
		}
		if item.VersionSpecJSON == nil {
			item.VersionSpecJSON = map[string]any{}
		}
		if item.SelectorJSON == nil {
			item.SelectorJSON = map[string]any{}
		}
		if item.MetadataJSON == nil {
			item.MetadataJSON = map[string]any{}
		}
		recentRuns = append(recentRuns, item)
	}

	var touchedStreamsTotal int64
	var inferenceRowsTotal int64
	var detectionsTotal int64
	var lastResultAt *time.Time
	err = s.pool.QueryRow(r.Context(), `
		SELECT
			COUNT(DISTINCT f.stream_id)::bigint,
			COUNT(DISTINCT ir.id)::bigint,
			COALESCE(COUNT(d.id), 0)::bigint,
			MAX(ir.created_at)
		FROM inference_results ir
		JOIN frames f ON f.id=ir.frame_id
		LEFT JOIN detections d ON d.inference_result_id=ir.id
		WHERE ir.pipeline_id=$1
	`, pipelineID).Scan(&touchedStreamsTotal, &inferenceRowsTotal, &detectionsTotal, &lastResultAt)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load pipeline inference summary: %v", err))
		return
	}
	experiments, err := s.listPipelineExperiments(r.Context(), pipelineID)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load pipeline experiments: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{
		"pipeline":    pipeline,
		"versions":    versions,
		"recent_runs": recentRuns,
		"summary": map[string]any{
			"touched_streams_total": touchedStreamsTotal,
			"inference_rows_total":  inferenceRowsTotal,
			"detections_total":      detectionsTotal,
			"last_result_at":        lastResultAt,
		},
		"experiments": experiments,
	})
}

func (s *Server) handleDashboardPipelineStreams(w http.ResponseWriter, r *http.Request) {
	pipelineID := strings.TrimSpace(chi.URLParam(r, "pipeline_id"))
	if pipelineID == "" {
		util.WriteError(w, http.StatusBadRequest, "pipeline_id is required")
		return
	}
	limit := parseIntQuery(r, "limit", 100, 1, 500)
	offset := parseIntQuery(r, "offset", 0, 0, 1000000)
	search := strings.TrimSpace(r.URL.Query().Get("q"))
	withDetections := false
	if v := parseBoolQueryPtr(r, "with_detections"); v != nil {
		withDetections = *v
	}
	runID := parseInt64QueryPtr(r, "run_id")

	where := []string{"ir.pipeline_id=$1"}
	args := []any{pipelineID}
	if runID != nil && *runID > 0 {
		args = append(args, *runID)
		where = append(where, fmt.Sprintf("ir.pipeline_run_id=$%d", len(args)))
	}
	if search != "" {
		args = append(args, "%"+search+"%")
		where = append(where, fmt.Sprintf("(s.name ILIKE $%d OR s.provider ILIKE $%d OR s.slug ILIKE $%d OR CAST(s.id AS text) ILIKE $%d)", len(args), len(args), len(args), len(args)))
	}
	having := "1=1"
	if withDetections {
		having = "COALESCE(COUNT(d.id), 0) > 0"
	}
	args = append(args, limit, offset)
	rows, err := s.pool.Query(r.Context(), fmt.Sprintf(`
		SELECT
			s.id, s.provider, s.name, s.slug, s.capture_type, s.recording_state,
			MAX(ir.created_at) AS latest_inference_at,
			COUNT(DISTINCT ir.id)::bigint AS inference_rows,
			COALESCE(COUNT(d.id), 0)::bigint AS detections_total,
			COUNT(DISTINCT ir.pipeline_run_id)::bigint AS run_count
		FROM inference_results ir
		JOIN frames f ON f.id=ir.frame_id
		JOIN streams s ON s.id=f.stream_id
		LEFT JOIN detections d ON d.inference_result_id=ir.id
		WHERE %s
		GROUP BY s.id, s.provider, s.name, s.slug, s.capture_type, s.recording_state
		HAVING %s
		ORDER BY latest_inference_at DESC NULLS LAST, s.id ASC
		LIMIT $%d OFFSET $%d
	`, strings.Join(where, " AND "), having, len(args)-1, len(args)), args...)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load pipeline touched streams: %v", err))
		return
	}
	defer rows.Close()
	items := make([]map[string]any, 0, limit)
	for rows.Next() {
		var streamID int64
		var provider, name, slug, captureType, recordingState string
		var latestInferenceAt *time.Time
		var inferenceRows, detectionsTotal, runCount int64
		if err := rows.Scan(&streamID, &provider, &name, &slug, &captureType, &recordingState, &latestInferenceAt, &inferenceRows, &detectionsTotal, &runCount); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan touched stream: %v", err))
			return
		}
		items = append(items, map[string]any{
			"stream_id":           streamID,
			"provider":            provider,
			"name":                name,
			"slug":                slug,
			"capture_type":        captureType,
			"recording_state":     recordingState,
			"latest_inference_at": latestInferenceAt,
			"inference_rows":      inferenceRows,
			"detections_total":    detectionsTotal,
			"run_count":           runCount,
		})
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{
		"pipeline_id": pipelineID,
		"items":       items,
		"limit":       limit,
		"offset":      offset,
	})
}

func defaultString(v string, fallback string) string {
	if strings.TrimSpace(v) == "" {
		return fallback
	}
	return strings.TrimSpace(v)
}

func dedupeSortedInt64s(in []int64) []int64 {
	out := dedupeInt64s(in)
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func containsInt64(in []int64, needle int64) bool {
	for _, v := range in {
		if v == needle {
			return true
		}
	}
	return false
}

func metricGraphPoints(iterations []model.PipelineExperimentIteration) []map[string]any {
	points := make([]map[string]any, 0, len(iterations))
	for _, it := range iterations {
		points = append(points, map[string]any{
			"iteration_index":       it.IterationIndex,
			"primary_metric_after":  it.PrimaryMetricAfter,
			"primary_metric_before": it.PrimaryMetricBefore,
			"primary_metric_delta":  it.PrimaryMetricDelta,
			"status":                it.Status,
		})
	}
	return points
}

func (s *Server) handleDashboardPipelineExperimentGraph(w http.ResponseWriter, r *http.Request) {
	experimentID := parseInt64QueryPtr(r, "experiment_id")
	if experimentID == nil || *experimentID <= 0 {
		util.WriteError(w, http.StatusBadRequest, "experiment_id is required")
		return
	}
	_, iterations, _, _, _, err := s.loadPipelineExperimentDetail(r.Context(), *experimentID)
	if err != nil {
		if err == pgx.ErrNoRows {
			util.WriteError(w, http.StatusNotFound, "pipeline experiment not found")
			return
		}
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load experiment graph: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"items": metricGraphPoints(iterations)})
}

func parseIntOptional(raw string) *int {
	value := strings.TrimSpace(raw)
	if value == "" {
		return nil
	}
	n, err := strconv.Atoi(value)
	if err != nil {
		return nil
	}
	return &n
}
