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
	RelativePath string                  `json:"relative_path"`
	SHA          string                  `json:"sha256"`
	Bytes        int64                   `json:"size_bytes"`
	Identity     nasR2StitchFileIdentity `json:"identity"`
	Fsynced      bool                    `json:"fsynced"`
}

type nasR2StitchFileIdentity [5]int64

func (identity *nasR2StitchFileIdentity) UnmarshalJSON(data []byte) error {
	var fields []*int64
	if err := json.Unmarshal(data, &fields); err != nil || len(fields) != len(identity) {
		return errors.New("stitch receipt identity requires five integers")
	}
	for i, field := range fields {
		if field == nil {
			return errors.New("stitch receipt identity requires five integers")
		}
		identity[i] = *field
	}
	return nil
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
  AND h.recording_id=$7 AND s.stream_id=$8 AND o.state='staged' AND h.state='staged' AND nas_stitch_r2_native_proof(h)`,
		item.SourceID, item.Bucket, item.ObjectKey, item.RelativePath, item.SHA256, item.SizeBytes, item.RecordingID, item.StreamID).
		Scan(&o.hourID, &o.attempt, &o.plan, &o.manifest, &o.etag, &o.version, &o.modified, &o.size, &o.sha)
	if errors.Is(err, pgx.ErrNoRows) {
		return o, errNASR2CopySourceIdentity
	}
	return o, err
}

func nasR2StitchResultSHA(req nasR2CopyControl) string {
	body, _ := json.Marshal(req)
	return cleanupSHA(body)
}

func finishNASR2StitchCopy(ctx context.Context, tx pgx.Tx, task nasR2CopyTask, req nasR2CopyControl) error {
	receipt := req.StitchReceipt
	if receipt == nil || !receipt.Fsynced || receipt.RelativePath != task.Item.RelativePath ||
		receipt.SHA != task.Item.SHA256 || receipt.Bytes != task.Item.SizeBytes ||
		receipt.Identity[0] != receipt.Bytes || receipt.Identity[1] <= 0 || receipt.Identity[2] <= 0 || receipt.Identity[3] <= 0 ||
		receipt.Identity[4] < 0 || req.Completion == "" {
		return errors.New("exact durable stitch output readback required")
	}
	staged, err := loadNASR2StagedObject(ctx, tx, task.Item)
	if err != nil {
		return err
	}
	// This transaction owns the copy row, so it never waits: it takes every
	// lock the certificate needs (epoch, input keys in hash order, input pairs,
	// hour) without waiting, all or nothing, and the client retries.
	var locked bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM r2_inverse_ke_epoch FOR SHARE SKIP LOCKED)
 AND (SELECT lock_nas_r2_copy_keys(array_agg(bucket),array_agg(object_key),false)
  FROM nas_stitch_r2_input_holds WHERE hour_id=$1 AND released_at IS NULL)
 AND lock_nas_stitch_r2_inputs($1,false)
 AND EXISTS(SELECT 1 FROM nas_stitch_r2_hours WHERE id=$1 FOR UPDATE SKIP LOCKED)`, staged.hourID).Scan(&locked); err != nil || !locked {
		return errors.New("retirement holds are busy")
	}
	body, _ := json.Marshal(receipt)
	tag, err := tx.Exec(ctx, `UPDATE nas_r2_copy_items SET state='verified',verified_at=clock_timestamp(),
 lease_token=NULL,lease_expires_at=NULL,completion=$2,committed_bytes=$3,stitch_receipt=$4,stitch_result_sha256=$6,updated_at=clock_timestamp()
 WHERE id=$1 AND kind='stitch_output' AND manifest_sha256=$5 AND lease_expires_at>clock_timestamp()`,
		task.ID, req.Completion, req.CommittedBytes, string(body), staged.manifest, nasR2StitchResultSHA(req))
	if err != nil || tag.RowsAffected() != 1 {
		return errors.New("stitch copy lease or manifest changed")
	}
	var exact bool
	err = tx.QueryRow(ctx, `SELECT nas_stitch_r2_exact_copy_receipt(q,o) FROM nas_r2_copy_items q
 JOIN nas_stitch_r2_stage_objects o ON o.id=q.source_id WHERE q.id=$1`, task.ID).Scan(&exact)
	if err != nil || !exact {
		return errors.New("NAS copy receipt differs from the exact staged generation")
	}
	var accepted bool
	err = tx.QueryRow(ctx, `WITH proof AS MATERIALIZED (
 SELECT nas_stitch_r2_certificate_receipt($1,$2,$3,$4) AS receipt
 ), certified AS (
 INSERT INTO nas_stitch_r2_certificates(hour_id,connection_id,attempt,plan_sha256,manifest_sha256,receipt)
 SELECT $1,13,$2,$3,$4,receipt FROM proof WHERE receipt IS NOT NULL RETURNING 1
 )
 SELECT EXISTS(SELECT 1 FROM certified) OR EXISTS(
  SELECT 1 FROM nas_stitch_r2_stage_objects o JOIN nas_r2_copy_items q
   ON q.connection_id=13 AND q.kind='stitch_output' AND q.source_id=o.id
  WHERE o.hour_id=$1 AND o.attempt=$2 AND o.state='staged' AND q.manifest_sha256=$4
   AND q.state<>'verified')`, staged.hourID, staged.attempt, staged.plan, staged.manifest).Scan(&accepted)
	if err != nil {
		return err
	}
	if !accepted {
		return errors.New("complete NAS custody differs from the stored Durham manifest")
	}
	return nil
}
