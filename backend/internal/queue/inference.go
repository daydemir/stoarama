package queue

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type FrameClaim struct {
	ClaimID      int64
	FrameID      int64
	StreamID     int64
	CapturedAt   time.Time
	ObjectKey    string
	MIMEType     string
	SizeBytes    int64
	Width        int
	Height       int
	PipelineID   string
	PipelineVersionID *int64
	PipelineRunID     *int64
	LeaseExpires time.Time
}

type ClaimFilter struct {
	PipelineID string
	StreamIDs  []int64
	Tag        string
	Limit      int
	LeaseSec   int
	ClaimedBy  string
	ForceRerun bool
}

func MarkExpiredClaimsAbandoned(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, `
		UPDATE inference_claims
		SET status='abandoned', updated_at=now()
		WHERE status='leased' AND lease_expires_at < now()
	`)
	if err != nil {
		return fmt.Errorf("mark expired claims abandoned: %w", err)
	}
	return nil
}

func AbandonClaims(ctx context.Context, pool *pgxpool.Pool, expiredOnly bool, pipelineID string) (int64, error) {
	where := "status='leased'"
	args := []any{}
	if expiredOnly {
		where += " AND lease_expires_at < now()"
	}
	if pipelineID != "" {
		args = append(args, pipelineID)
		where += fmt.Sprintf(" AND pipeline_id=$%d", len(args))
	}
	q := fmt.Sprintf(`
		UPDATE inference_claims
		SET status='abandoned', updated_at=now()
		WHERE %s
	`, where)
	ct, err := pool.Exec(ctx, q, args...)
	if err != nil {
		return 0, fmt.Errorf("abandon claims: %w", err)
	}
	return ct.RowsAffected(), nil
}

func ClaimFrames(ctx context.Context, pool *pgxpool.Pool, f ClaimFilter) ([]FrameClaim, error) {
	if f.PipelineID == "" {
		return nil, fmt.Errorf("pipeline_id is required")
	}
	if f.Limit <= 0 || f.Limit > 2000 {
		return nil, fmt.Errorf("limit must be between 1 and 2000")
	}
	if f.LeaseSec <= 0 {
		f.LeaseSec = 600
	}
	f.ClaimedBy = strings.TrimSpace(f.ClaimedBy)
	if f.ClaimedBy == "" {
		return nil, fmt.Errorf("claimed_by is required")
	}
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin claim tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `
		UPDATE inference_claims
		SET status='abandoned', updated_at=now()
		WHERE status='leased' AND lease_expires_at < now()
	`); err != nil {
		return nil, fmt.Errorf("expire old claims: %w", err)
	}

	// A frame is claimable until this pipeline has at least one successful or queued_boxed result.
	// Error-only history stays claimable for retry.
	processedClause := `
			  AND NOT EXISTS (
			    SELECT 1 FROM inference_results ir
			    WHERE ir.frame_id=f.id AND ir.pipeline_id=$1 AND ir.status IN ('success','queued_boxed')
			  )`
	if f.ForceRerun {
		processedClause = ""
	}

	baseQuery := `
		WITH candidates AS (
			SELECT f.id AS frame_id, f.stream_id, f.captured_at, mo.object_key, mo.mime_type, mo.size_bytes,
			       COALESCE(mo.width, 0) AS width, COALESCE(mo.height, 0) AS height
			FROM frames f
			JOIN media_objects mo ON mo.id = f.raw_media_object_id
			JOIN streams s ON s.id = f.stream_id
			WHERE f.capture_status = 'success'
			  AND COALESCE((
			    SELECT sps.enabled
			    FROM stream_pipeline_settings sps
			    WHERE sps.stream_id=f.stream_id AND sps.pipeline_id=$1
			  ), true)
			  %s
			  AND NOT EXISTS (
			    SELECT 1 FROM inference_claims ic
			    WHERE ic.frame_id=f.id AND ic.pipeline_id=$1 AND ic.status='leased'
			  )
			  %s
			ORDER BY f.captured_at ASC
			LIMIT $2
			FOR UPDATE OF f SKIP LOCKED
		), ins AS (
			INSERT INTO inference_claims (pipeline_id, frame_id, claimed_by, lease_expires_at, status)
			SELECT $1, c.frame_id, $3, now() + make_interval(secs => $4), 'leased'
			FROM candidates c
			ON CONFLICT (pipeline_id, frame_id) WHERE status='leased' AND pipeline_run_id IS NULL DO NOTHING
			RETURNING id, frame_id, pipeline_id, pipeline_version_id, pipeline_run_id, lease_expires_at
		)
		SELECT i.id, i.frame_id, c.stream_id, c.captured_at, c.object_key, c.mime_type, c.size_bytes, c.width, c.height, i.pipeline_id, i.pipeline_version_id, i.pipeline_run_id, i.lease_expires_at
		FROM ins i
		JOIN candidates c ON c.frame_id = i.frame_id
	`
	whereExtra := ""
	args := []any{f.PipelineID, f.Limit, f.ClaimedBy, f.LeaseSec}
	if len(f.StreamIDs) > 0 {
		whereExtra += " AND f.stream_id = ANY($5)"
		args = append(args, f.StreamIDs)
	}
	if f.Tag != "" {
		ix := len(args) + 1
		whereExtra += fmt.Sprintf(" AND $%d = ANY(s.tags)", ix)
		args = append(args, f.Tag)
	}

	q := fmt.Sprintf(baseQuery, processedClause, whereExtra)
	rows, err := tx.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("claim frames query: %w", err)
	}
	defer rows.Close()

	claims := make([]FrameClaim, 0)
	for rows.Next() {
		var c FrameClaim
		if err := rows.Scan(&c.ClaimID, &c.FrameID, &c.StreamID, &c.CapturedAt, &c.ObjectKey, &c.MIMEType, &c.SizeBytes, &c.Width, &c.Height, &c.PipelineID, &c.PipelineVersionID, &c.PipelineRunID, &c.LeaseExpires); err != nil {
			return nil, fmt.Errorf("scan claim row: %w", err)
		}
		claims = append(claims, c)
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("iterate claim rows: %w", rows.Err())
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit claim tx: %w", err)
	}
	return claims, nil
}
