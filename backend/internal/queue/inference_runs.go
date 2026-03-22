package queue

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type RunClaimFilter struct {
	RunID      int64
	Limit      int
	LeaseSec   int
	ClaimedBy  string
	ForceRerun bool
}

func ClaimFramesForRun(ctx context.Context, pool *pgxpool.Pool, f RunClaimFilter) ([]FrameClaim, error) {
	if f.RunID <= 0 {
		return nil, fmt.Errorf("run_id is required")
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
		return nil, fmt.Errorf("begin run claim tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `
		UPDATE inference_claims
		SET status='abandoned', updated_at=now()
		WHERE status='leased' AND lease_expires_at < now() AND pipeline_run_id=$1
	`, f.RunID); err != nil {
		return nil, fmt.Errorf("expire old run claims: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE pipeline_run_targets
		SET status='abandoned', claim_id=NULL, claimed_by='', lease_expires_at=NULL, updated_at=now()
		WHERE run_id=$1 AND status='leased' AND lease_expires_at < now()
	`, f.RunID); err != nil {
		return nil, fmt.Errorf("expire old run targets: %w", err)
	}

	processedClause := `
			  AND NOT EXISTS (
			    SELECT 1 FROM inference_results ir
			    WHERE ir.frame_id=prt.frame_id AND ir.pipeline_run_id=$1 AND ir.status IN ('success','queued_boxed')
			  )`
	if f.ForceRerun {
		processedClause = ""
	}

	rows, err := tx.Query(ctx, fmt.Sprintf(`
		WITH run_meta AS (
			SELECT pr.id, pr.pipeline_id, pr.pipeline_version_id
			FROM pipeline_runs pr
			WHERE pr.id=$1
		), candidates AS (
			SELECT prt.frame_id, prt.stream_id, f.captured_at, mo.object_key, mo.mime_type, mo.size_bytes,
			       COALESCE(mo.width, 0) AS width, COALESCE(mo.height, 0) AS height,
			       rm.pipeline_id, rm.pipeline_version_id
			FROM pipeline_run_targets prt
			JOIN run_meta rm ON true
			JOIN frames f ON f.id = prt.frame_id
			JOIN media_objects mo ON mo.id = f.raw_media_object_id
			WHERE prt.run_id = $1
			  AND prt.status IN ('pending', 'abandoned')
			  %s
			  AND NOT EXISTS (
			    SELECT 1 FROM inference_claims ic
			    WHERE ic.pipeline_run_id=$1 AND ic.frame_id=prt.frame_id AND ic.status='leased'
			  )
			ORDER BY f.captured_at ASC, f.id ASC
			LIMIT $2
			FOR UPDATE OF prt, f SKIP LOCKED
		), ins AS (
			INSERT INTO inference_claims (
				pipeline_id, pipeline_version_id, pipeline_run_id, frame_id, claimed_by, lease_expires_at, status
			)
			SELECT c.pipeline_id, c.pipeline_version_id, $1, c.frame_id, $3, now() + make_interval(secs => $4), 'leased'
			FROM candidates c
			ON CONFLICT (pipeline_run_id, frame_id) WHERE status='leased' AND pipeline_run_id IS NOT NULL DO NOTHING
			RETURNING id, frame_id, pipeline_id, pipeline_version_id, pipeline_run_id, lease_expires_at
		), upd AS (
			UPDATE pipeline_run_targets prt
			SET status='leased', claim_id=i.id, claimed_by=$3, lease_expires_at=i.lease_expires_at, updated_at=now()
			FROM ins i
			WHERE prt.run_id=$1 AND prt.frame_id=i.frame_id
		)
		SELECT i.id, i.frame_id, c.stream_id, c.captured_at, c.object_key, c.mime_type, c.size_bytes, c.width, c.height, i.pipeline_id, i.pipeline_version_id, i.pipeline_run_id, i.lease_expires_at
		FROM ins i
		JOIN candidates c ON c.frame_id = i.frame_id
	`, processedClause), f.RunID, f.Limit, f.ClaimedBy, f.LeaseSec)
	if err != nil {
		return nil, fmt.Errorf("claim run frames query: %w", err)
	}
	defer rows.Close()

	claims := make([]FrameClaim, 0)
	for rows.Next() {
		var c FrameClaim
		if err := rows.Scan(&c.ClaimID, &c.FrameID, &c.StreamID, &c.CapturedAt, &c.ObjectKey, &c.MIMEType, &c.SizeBytes, &c.Width, &c.Height, &c.PipelineID, &c.PipelineVersionID, &c.PipelineRunID, &c.LeaseExpires); err != nil {
			return nil, fmt.Errorf("scan run claim row: %w", err)
		}
		claims = append(claims, c)
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("iterate run claim rows: %w", rows.Err())
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit run claim tx: %w", err)
	}
	return claims, nil
}
