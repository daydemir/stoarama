package api

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"
)

const (
	nasCleanupCertifiedStitchKind     = "certified_stitch_raw"
	nasCleanupCertifiedStitchReason   = "durham_stitch_certified"
	nasCleanupCertifiedStitchRecovery = "nas_stitch_certified"
)

type nasCleanupStitchOutput struct {
	Path     string                  `json:"relative_path"`
	SHA      string                  `json:"sha256"`
	Size     int64                   `json:"size_bytes"`
	Identity nasR2StitchFileIdentity `json:"identity"`
}

type nasCleanupStitchCertificate struct {
	HourID   int64                  `json:"hour_id"`
	Attempt  int                    `json:"attempt"`
	Plan     string                 `json:"plan_sha256"`
	Manifest string                 `json:"manifest_sha256"`
	Output   nasCleanupStitchOutput `json:"output"`
}

type nasCleanupStitchSidecar struct {
	Size int64  `json:"size_bytes"`
	SHA  string `json:"sha256"`
}

func loadNASCleanupStitchCertificate(ctx context.Context, tx pgx.Tx, item *nasCleanupItem) error {
	var certificate nasCleanupStitchCertificate
	var sidecar nasCleanupStitchSidecar
	err := tx.QueryRow(ctx, `SELECT certificate,sidecar_size,sidecar_sha
 FROM nas_cleanup_certified_stitch_raw WHERE clip_id=$1 AND bucket=$2 AND object_key=$3`,
		item.SourceID, item.Bucket, item.ObjectKey).Scan(&certificate, &sidecar.Size, &sidecar.SHA)
	output := certificate.Output
	if err != nil || certificate.HourID <= 0 || certificate.Attempt <= 0 || !stitchSHA(certificate.Plan) ||
		!stitchSHA(certificate.Manifest) || !validCleanupPath(output.Path) || !stitchSHA(output.SHA) ||
		output.Size <= 0 || output.Identity[0] != output.Size || output.Path == item.RelativePath ||
		output.Path == item.RelativePath+".stoarama.json" {
		return errors.New("exact certified replacement and completed keep-28 R2 retirement required")
	}
	item.PermitReason = nasCleanupCertifiedStitchReason
	item.StitchCertificate = &certificate
	item.StitchRawSidecar = &sidecar
	return nil
}

func cleanupCheckCertifiedStitch(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, item nasCleanupItem, requireStagingAbsent bool) error {
	body, err := json.Marshal(item)
	if err != nil {
		return err
	}
	var exact bool
	if err := q.QueryRow(ctx, `SELECT coalesce(nas_cleanup_exact_certified_stitch($1::jsonb),false)
 AND (NOT $2 OR nas_cleanup_certified_stitch_staging_absent($1::jsonb->'stitch_certificate'))`,
		string(body), requireStagingAbsent).Scan(&exact); err != nil || !exact {
		return errors.New("exact stitch custody and required R2 retirement are unavailable")
	}
	return nil
}

type cleanupAbortedStitchTarget struct {
	id                       int64
	bucket, endpointSHA, key string
}

// Include every aborted row, even when it lacks a receipt or safe abort
// provenance. Such a row must never disappear from the permit check.
const cleanupAbortedStitchTargetsSQL = `SELECT o.id,o.bucket,o.endpoint_sha256,o.object_key,
 safe.id IS NOT NULL AND a.object_id IS NOT NULL AND coalesce(safe.grants_drain_at,'-infinity')<clock_timestamp()
 FROM nas_stitch_r2_stage_objects o
 LEFT JOIN nas_stitch_r2_aborts_awaiting_absence safe ON safe.id=o.id
 LEFT JOIN nas_stitch_r2_aborted_absences a ON a.object_id=o.id
 WHERE o.state='aborted' AND `

func (s *Server) cleanupPrefetchAbortedStitchAbsences(ctx context.Context, cid int64, ids []int64, env *cleanupObserveEnv) {
	if s.r2 == nil || !s.cfg.NASCleanupDeletionEnabled || cid != 13 || env.dryRunOnly ||
		env.session.version != nasCleanupCertifiedStitchVersion || len(ids) == 0 {
		return
	}
	rows, err := s.pool.Query(ctx, cleanupAbortedStitchTargetsSQL+`o.hour_id IN (
 SELECT (m.item#>>'{stitch_certificate,hour_id}')::bigint
 FROM nas_cleanup_items i JOIN nas_cleanup_plans p ON p.id=i.plan_id
 CROSS JOIN LATERAL (SELECT coalesce(i.item,p.manifest->(i.ordinal-1)) item) m
 WHERE i.id=ANY($1) AND p.connection_id=$2 AND p.mode='enabled' AND p.approved_until>clock_timestamp()
 AND i.lease_connection_id=$2 AND i.lease_client_version=$3 AND i.lease_client_boot_id=$4
 AND (i.state='leased' AND i.lease_expires_at>clock_timestamp()
    OR i.state='delete_issued' AND i.permit_expires_at>clock_timestamp())
 AND m.item->>'permit_reason'=$5)`, ids, cid, env.session.version, env.session.boot, nasCleanupCertifiedStitchReason)
	if err != nil {
		return // No provider evidence will be available for the permit check.
	}
	var targets []cleanupAbortedStitchTarget
	for rows.Next() {
		var target cleanupAbortedStitchTarget
		var drained bool
		if err := rows.Scan(&target.id, &target.bucket, &target.endpointSHA, &target.key, &drained); err != nil {
			rows.Close()
			return
		}
		if drained {
			targets = append(targets, target)
		}
	}
	if rows.Err() != nil || len(targets) == 0 {
		return
	}
	env.abortedStitchAbsences = make(map[cleanupAbortedStitchTarget]bool, len(targets))
	for _, target := range targets {
		if ctx.Err() != nil {
			break
		}
		if err := s.r2.VerifyAbortedStitchStageAbsence(ctx, target.bucket, target.endpointSHA, target.key); err == nil {
			env.abortedStitchAbsences[target] = true
		}
	}
}

func (e *cleanupObserveEnv) certifiedStitchAbortsAbsent(ctx context.Context, tx pgx.Tx, item nasCleanupItem) bool {
	if item.StitchCertificate == nil {
		return false
	}
	rows, err := tx.Query(ctx, cleanupAbortedStitchTargetsSQL+`o.hour_id=$1`, item.StitchCertificate.HourID)
	if err != nil {
		return false
	}
	defer rows.Close()
	for rows.Next() {
		var target cleanupAbortedStitchTarget
		var drained bool
		if err := rows.Scan(&target.id, &target.bucket, &target.endpointSHA, &target.key, &drained); err != nil ||
			!drained || !e.abortedStitchAbsences[target] {
			return false
		}
	}
	return rows.Err() == nil
}
