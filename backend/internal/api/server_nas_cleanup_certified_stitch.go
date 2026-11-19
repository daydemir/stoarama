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
