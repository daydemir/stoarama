package api

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/jackc/pgx/v5"
)

type nasR2StitchReceipt struct {
	RelativePath string   `json:"relative_path"`
	SHA          string   `json:"sha256"`
	Bytes        int64    `json:"size_bytes"`
	Identity     [5]int64 `json:"identity"`
	Fsynced      bool     `json:"fsynced"`
}

type nasR2StitchObject struct {
	hourID, size                       int64
	attempt                            int
	plan, manifest, etag, version, sha string
	modified                           time.Time
}

func (o nasR2StitchObject) matches(head r2.ObjectHead) bool {
	return strings.Trim(head.ETag, "\"") == strings.Trim(o.etag, "\"") && head.VersionID == o.version &&
		head.LastModified.Equal(o.modified) && head.SizeBytes == o.size && head.SHA256Metadata == o.sha
}

func loadNASR2StagedObject(ctx context.Context, tx pgx.Tx, item NASR2CopyItem) (nasR2StitchObject, error) {
	var o nasR2StitchObject
	err := tx.QueryRow(ctx, `SELECT h.id,h.attempt,h.plan_sha256,h.staged_manifest_sha256,o.etag,
 coalesce(o.version_id,''),o.last_modified,o.size_bytes,o.sha256
 FROM nas_stitch_r2_stage_objects o JOIN nas_stitch_r2_hours h ON h.id=o.hour_id AND h.attempt=o.attempt
 JOIN nas_stitch_scopes s ON s.id=h.scope_id
 WHERE o.id=$1 AND o.bucket=$2 AND o.object_key=$3 AND o.relative_path=$4 AND o.sha256=$5 AND o.size_bytes=$6
  AND h.recording_id=$7 AND s.stream_id=$8 AND o.state='staged' AND h.state='staged'`,
		item.SourceID, item.Bucket, item.ObjectKey, item.RelativePath, item.SHA256, item.SizeBytes, item.RecordingID, item.StreamID).
		Scan(&o.hourID, &o.attempt, &o.plan, &o.manifest, &o.etag, &o.version, &o.modified, &o.size, &o.sha)
	if errors.Is(err, pgx.ErrNoRows) {
		return o, errNASR2CopySourceIdentity
	}
	return o, err
}

func finishNASR2StitchCopy(ctx context.Context, tx pgx.Tx, task nasR2CopyTask, req nasR2CopyControl) error {
	receipt := req.StitchReceipt
	if receipt == nil || !receipt.Fsynced || receipt.RelativePath != task.Item.RelativePath ||
		receipt.SHA != task.Item.SHA256 || receipt.Bytes != task.Item.SizeBytes ||
		receipt.Identity[0] != receipt.Bytes || receipt.Identity[1] <= 0 || receipt.Identity[2] <= 0 || receipt.Identity[3] <= 0 || req.Completion == "" {
		return errors.New("exact durable stitch output readback required")
	}
	staged, err := loadNASR2StagedObject(ctx, tx, task.Item)
	if err != nil {
		return err
	}
	var outer bool
	if err = tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock(7343226011173)`).Scan(&outer); err != nil || !outer {
		return errors.New("retirement holds are busy")
	}
	if _, err = tx.Exec(ctx, `SELECT id FROM nas_stitch_r2_hours WHERE id=$1 FOR UPDATE`, staged.hourID); err != nil {
		return err
	}
	body, _ := json.Marshal(receipt)
	tag, err := tx.Exec(ctx, `UPDATE nas_r2_copy_items SET state='verified',verified_at=clock_timestamp(),
 lease_token=NULL,lease_expires_at=NULL,completion=$2,committed_bytes=$3,stitch_receipt=$4,updated_at=clock_timestamp()
 WHERE id=$1 AND kind='stitch_output' AND manifest_sha256=$5 AND lease_expires_at>clock_timestamp()`,
		task.ID, req.Completion, req.CommittedBytes, string(body), staged.manifest)
	if err != nil || tag.RowsAffected() != 1 {
		return errors.New("stitch copy lease or manifest changed")
	}
	_, err = tx.Exec(ctx, `WITH proof AS (
 SELECT count(*) AS n, bool_and(COALESCE(q.state='verified' AND q.stitch_receipt IS NOT NULL AND q.manifest_sha256=$4,false)) AS complete,
  jsonb_agg(jsonb_build_object('object_key',o.object_key,'etag',o.etag,'version_id',o.version_id,
    'last_modified',o.last_modified,'readback',q.stitch_receipt) ORDER BY o.id) AS objects
 FROM nas_stitch_r2_stage_objects o LEFT JOIN nas_r2_copy_items q
  ON q.connection_id=13 AND q.kind='stitch_output' AND q.source_id=o.id
 WHERE o.hour_id=$1 AND o.attempt=$2 AND o.state='staged'
 )
 INSERT INTO nas_stitch_r2_certificates(hour_id,connection_id,attempt,plan_sha256,manifest_sha256,receipt)
 SELECT $1,13,$2,$3,$4,jsonb_build_object('objects',objects) FROM proof WHERE n>0 AND complete`,
		staged.hourID, staged.attempt, staged.plan, staged.manifest)
	return err
}
