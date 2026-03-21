package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/daydemir/stoarama/backend/internal/capture"
	"github.com/daydemir/stoarama/backend/internal/model"
	"github.com/daydemir/stoarama/backend/internal/util"
)

type streamSourceRevisionInput struct {
	Actor    string
	Reason   string
	Previous model.Stream
	Current  model.Stream
	Metadata map[string]any
}

func streamSourceChanged(previous model.Stream, current model.Stream) bool {
	return strings.TrimSpace(previous.SourceURL) != strings.TrimSpace(current.SourceURL) ||
		strings.TrimSpace(previous.SourcePageURL) != strings.TrimSpace(current.SourcePageURL) ||
		strings.TrimSpace(previous.SourceFamily) != strings.TrimSpace(current.SourceFamily) ||
		strings.TrimSpace(previous.CaptureType) != strings.TrimSpace(current.CaptureType) ||
		strings.TrimSpace(previous.ExecutionClass) != strings.TrimSpace(current.ExecutionClass)
}

func insertStreamSourceRevisionTx(ctx context.Context, tx pgx.Tx, in streamSourceRevisionInput) error {
	if !streamSourceChanged(in.Previous, in.Current) {
		return nil
	}
	metaBytes, err := json.Marshal(nonNilMap(in.Metadata))
	if err != nil {
		return fmt.Errorf("marshal source revision metadata: %w", err)
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO stream_source_revisions (
			stream_id, actor, reason,
			previous_source_url, new_source_url,
			previous_source_page_url, new_source_page_url,
			previous_source_family, new_source_family,
			previous_capture_type, new_capture_type,
			previous_execution_class, new_execution_class,
			metadata_jsonb
		)
		VALUES (
			$1, $2, $3,
			$4, $5,
			$6, $7,
			$8, $9,
			$10, $11,
			$12, $13,
			$14::jsonb
		)
	`,
		in.Current.ID,
		strings.TrimSpace(in.Actor),
		strings.TrimSpace(in.Reason),
		strings.TrimSpace(in.Previous.SourceURL),
		strings.TrimSpace(in.Current.SourceURL),
		strings.TrimSpace(in.Previous.SourcePageURL),
		strings.TrimSpace(in.Current.SourcePageURL),
		strings.TrimSpace(in.Previous.SourceFamily),
		strings.TrimSpace(in.Current.SourceFamily),
		strings.TrimSpace(in.Previous.CaptureType),
		strings.TrimSpace(in.Current.CaptureType),
		strings.TrimSpace(in.Previous.ExecutionClass),
		strings.TrimSpace(in.Current.ExecutionClass),
		string(metaBytes),
	)
	if err != nil {
		return fmt.Errorf("insert source revision: %w", err)
	}
	return nil
}

func (s *Server) resetYouTubeRelayRouteForSourceChangeTx(
	ctx context.Context,
	tx pgx.Tx,
	stream model.Stream,
	assignment recordingAssignmentRow,
	actor string,
	reason string,
) error {
	if strings.TrimSpace(assignment.ExecutionClass) != capture.ExecutionClassYouTubeRelay {
		return nil
	}
	if err := s.clearYouTubeRelayRouteTx(ctx, tx, stream.ID, actor, reason); err != nil {
		return fmt.Errorf("clear youtube relay route for source change: %w", err)
	}
	if _, err := s.allocateYouTubeRelayRouteTx(ctx, tx, stream, assignment.ServerID, assignment.Revision, actor, reason); err != nil {
		return fmt.Errorf("allocate youtube relay route after source change: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE stream_capture_runtime
		SET
			status='stopped',
			last_error_text=NULL,
			consecutive_errors=0,
			updated_at=now()
		WHERE stream_id=$1
	`, stream.ID); err != nil {
		return fmt.Errorf("reset capture runtime after source change: %w", err)
	}
	return nil
}

func (s *Server) handleStreamSourceRevisionsList(w http.ResponseWriter, r *http.Request) {
	streamID, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	limit := parseIntQuery(r, "limit", 100, 1, 1000)
	offset := parseIntQuery(r, "offset", 0, 0, 1000000)
	rows, err := s.pool.Query(r.Context(), `
		SELECT
			id, actor, reason,
			previous_source_url, new_source_url,
			previous_source_page_url, new_source_page_url,
			previous_source_family, new_source_family,
			previous_capture_type, new_capture_type,
			previous_execution_class, new_execution_class,
			metadata_jsonb, created_at
		FROM stream_source_revisions
		WHERE stream_id=$1
		ORDER BY created_at DESC, id DESC
		LIMIT $2 OFFSET $3
	`, streamID, limit, offset)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("query stream source revisions: %v", err))
		return
	}
	defer rows.Close()
	items := make([]map[string]any, 0, limit)
	for rows.Next() {
		var (
			id                                     int64
			actor, reason                          string
			prevURL, nextURL                       string
			prevPageURL, nextPageURL               string
			prevFamily, nextFamily                 string
			prevCaptureType, nextCaptureType       string
			prevExecutionClass, nextExecutionClass string
			metaBytes                              []byte
			createdAt                              time.Time
		)
		if err := rows.Scan(
			&id, &actor, &reason,
			&prevURL, &nextURL,
			&prevPageURL, &nextPageURL,
			&prevFamily, &nextFamily,
			&prevCaptureType, &nextCaptureType,
			&prevExecutionClass, &nextExecutionClass,
			&metaBytes, &createdAt,
		); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan stream source revision: %v", err))
			return
		}
		meta := map[string]any{}
		if len(metaBytes) > 0 {
			if err := json.Unmarshal(metaBytes, &meta); err != nil {
				util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("decode stream source revision metadata: %v", err))
				return
			}
		}
		items = append(items, map[string]any{
			"id":                       id,
			"stream_id":                streamID,
			"actor":                    actor,
			"reason":                   reason,
			"previous_source_url":      prevURL,
			"new_source_url":           nextURL,
			"previous_source_page_url": prevPageURL,
			"new_source_page_url":      nextPageURL,
			"previous_source_family":   prevFamily,
			"new_source_family":        nextFamily,
			"previous_capture_type":    prevCaptureType,
			"new_capture_type":         nextCaptureType,
			"previous_execution_class": prevExecutionClass,
			"new_execution_class":      nextExecutionClass,
			"metadata_json":            meta,
			"created_at":               createdAt.UTC(),
		})
	}
	if err := rows.Err(); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("iterate stream source revisions: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{
		"items":  items,
		"limit":  limit,
		"offset": offset,
		"total":  len(items),
	})
}
