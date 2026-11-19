package r2

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/jackc/pgx/v5"
)

const StitchStagePartBytes = 16 << 20

type StitchStageLease struct {
	HourID, APIKeyID int64
	Attempt          int
	Token, PlanSHA   string
}

type StitchStageUpload struct {
	ID               int64  `json:"id"`
	Size             int64  `json:"size_bytes"`
	State            string `json:"state"`
	PartBytes        int64  `json:"part_bytes"`
	key, upload, sha string
}

func (c *Client) stitchStageWriter(ctx context.Context, tx pgx.Tx, lease StitchStageLease, id int64) (StitchStageUpload, time.Duration, error) {
	var o StitchStageUpload
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock_shared(7343226011187)`); err != nil {
		return o, 0, err
	}
	err := tx.QueryRow(ctx, `SELECT o.id,o.size_bytes,o.state,o.object_key,coalesce(o.upload_id,''),o.sha256
 FROM nas_stitch_r2_stage_objects o JOIN nas_stitch_r2_hours h ON h.id=o.hour_id
 JOIN nas_stitch_scopes s ON s.id=h.scope_id JOIN account_api_keys k ON k.id=o.api_key_id
 CROSS JOIN nas_stitch_r2_control control JOIN r2_retirement_writer_fences f
 ON f.bucket=o.bucket AND f.endpoint_sha256=o.endpoint_sha256
 WHERE o.id=$1 AND o.hour_id=$2 AND o.attempt=$3 AND o.api_key_id=$4 AND h.attempt=o.attempt
  AND h.lease_api_key_id=o.api_key_id AND h.lease_token=$5 AND h.plan_sha256=$6
  AND o.bucket=$7 AND o.endpoint_sha256=$8
 FOR UPDATE OF o FOR SHARE OF h,s,k,control,f`, id, lease.HourID, lease.Attempt, lease.APIKeyID,
		lease.Token, lease.PlanSHA, c.bucket, c.EndpointSHA256()).Scan(&o.ID, &o.Size, &o.State, &o.key, &o.upload, &o.sha)
	if err != nil {
		return o, 0, err
	}
	var expires, now time.Time
	err = tx.QueryRow(ctx, `SELECT least(h.lease_expires_at,s.approved_until,coalesce(k.expires_at,'infinity')),clock_timestamp()
 FROM nas_stitch_r2_hours h JOIN nas_stitch_scopes s ON s.id=h.scope_id
 JOIN account_api_keys k ON k.id=h.lease_api_key_id CROSS JOIN nas_stitch_r2_control control
 WHERE h.id=$1 AND h.state='leased' AND NOT s.paused AND k.revoked_at IS NULL AND control.enabled
  AND NOT EXISTS(SELECT 1 FROM nas_cleanup_frozen_streams f WHERE f.stream_id=s.stream_id)`, lease.HourID).Scan(&expires, &now)
	ttl := min(expires.Sub(now).Truncate(time.Second), 3*time.Minute)
	if err != nil || ttl < time.Second {
		return o, 0, errors.New("stitch stage lease is inactive")
	}
	o.PartBytes = StitchStagePartBytes
	return o, ttl, nil
}

func (c *Client) StartStitchStage(ctx context.Context, lease StitchStageLease, id int64) (StitchStageUpload, error) {
	var o StitchStageUpload
	if c.writerDB == nil {
		return o, errors.New("stitch staging requires a durable writer ledger")
	}
	tx, err := c.writerDB.Begin(ctx)
	if err != nil {
		return o, err
	}
	defer tx.Rollback(ctx)
	o, _, err = c.stitchStageWriter(ctx, tx, lease, id)
	if err != nil {
		return o, err
	}
	if o.State == "uploading" || o.State == "completing" || o.State == "staged" {
		return o, tx.Commit(ctx)
	}
	tag, err := tx.Exec(ctx, `UPDATE nas_stitch_r2_stage_objects SET start_issued_at=clock_timestamp()
 WHERE id=$1 AND state='starting' AND start_issued_at IS NULL`, id)
	if err != nil || tag.RowsAffected() != 1 {
		return o, errors.New("stitch multipart start already issued; reconcile before retry")
	}
	if err := tx.Commit(ctx); err != nil {
		return o, err
	}
	created, err := c.s3.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(c.bucket), Key: aws.String(o.key), ContentType: aws.String("video/mp4"),
		Metadata: map[string]string{"sha256": o.sha}, ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
	}, noStitchStageRetries)
	if err != nil || aws.ToString(created.UploadId) == "" {
		return o, errors.New("stitch multipart start unconfirmed; reconcile before retry")
	}
	tag, err = c.writerDB.Exec(ctx, `UPDATE nas_stitch_r2_stage_objects SET state='uploading',upload_id=$2
 WHERE id=$1 AND state='starting' AND start_issued_at IS NOT NULL AND upload_id IS NULL`, id, *created.UploadId)
	if err != nil || tag.RowsAffected() != 1 {
		return o, errors.New("stitch multipart upload ID not durable; reconcile before retry")
	}
	o.State = "uploading"
	return o, nil
}

func noStitchStageRetries(o *s3.Options) { o.Retryer = aws.NopRetryer{} }

func (c *Client) PresignStitchStagePart(ctx context.Context, lease StitchStageLease, id int64, number int32, sha string) (PresignedRequest, error) {
	if c.writerDB == nil {
		return PresignedRequest{}, errors.New("stitch staging requires a durable writer ledger")
	}
	digest, err := hex.DecodeString(sha)
	if err != nil || len(digest) != 32 || hex.EncodeToString(digest) != sha {
		return PresignedRequest{}, errors.New("stitch part requires its exact SHA-256")
	}
	tx, err := c.writerDB.Begin(ctx)
	if err != nil {
		return PresignedRequest{}, err
	}
	defer tx.Rollback(ctx)
	o, ttl, err := c.stitchStageWriter(ctx, tx, lease, id)
	if err != nil {
		return PresignedRequest{}, err
	}
	offset := (int64(number) - 1) * StitchStagePartBytes
	if o.State != "uploading" || offset < 0 || offset >= o.Size {
		return PresignedRequest{}, errors.New("stitch part outside active upload")
	}
	if _, err = tx.Exec(ctx, `INSERT INTO nas_stitch_r2_parts(object_id,part_number,sha256)
 VALUES($1,$2,$3) ON CONFLICT DO NOTHING`, o.ID, number, sha); err != nil {
		return PresignedRequest{}, err
	}
	var frozen string
	if err = tx.QueryRow(ctx, `SELECT sha256 FROM nas_stitch_r2_parts WHERE object_id=$1 AND part_number=$2`,
		o.ID, number).Scan(&frozen); err != nil || frozen != sha {
		return PresignedRequest{}, errors.New("stitch part digest differs")
	}
	checksum := base64.StdEncoding.EncodeToString(digest)
	length := min(StitchStagePartBytes, o.Size-offset)
	cap, err := c.presignStitchPart(ctx, o, number, checksum, length, ttl)
	if err != nil {
		return PresignedRequest{}, err
	}
	if _, err = tx.Exec(ctx, `UPDATE nas_stitch_r2_stage_objects SET grants_drain_at=greatest(grants_drain_at,
 clock_timestamp()+$2::bigint*interval '1 second'+interval '2 minutes') WHERE id=$1`, o.ID, int64(ttl/time.Second)); err != nil {
		return PresignedRequest{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return PresignedRequest{}, err
	}
	return cap, nil
}

func (c *Client) presignStitchPart(ctx context.Context, o StitchStageUpload, number int32, checksum string, length int64, ttl time.Duration) (PresignedRequest, error) {
	signed, err := c.presigner.PresignUploadPart(ctx, &s3.UploadPartInput{
		Bucket: aws.String(c.bucket), Key: aws.String(o.key), UploadId: aws.String(o.upload),
		PartNumber: &number, ContentLength: &length, ChecksumSHA256: &checksum,
	}, s3.WithPresignExpires(ttl), func(options *s3.PresignOptions) {
		options.Presigner = v4.NewSigner(func(sign *v4.SignerOptions) {
			sign.DisableHeaderHoisting = true
		})
	})
	if err != nil {
		return PresignedRequest{}, err
	}
	if signed.SignedHeader.Get("x-amz-checksum-sha256") != checksum || signed.SignedHeader.Get("Content-Length") != fmt.Sprint(length) {
		return PresignedRequest{}, errors.New("stitch part signature omitted its length or checksum")
	}
	return PresignedRequest{URL: signed.URL, Method: signed.Method, Headers: signed.SignedHeader.Clone()}, nil
}

// FinishStitchStage completes exactly the frozen parts. An uncertain provider
// result leaves the row completing; subsequent calls reconcile by HEAD.
func (c *Client) FinishStitchStage(ctx context.Context, lease StitchStageLease, id int64) error {
	if c.writerDB == nil {
		return errors.New("stitch staging requires a durable writer ledger")
	}
	tx, err := c.writerDB.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	o, _, err := c.stitchStageWriter(ctx, tx, lease, id)
	if err != nil {
		return err
	}
	switch o.State {
	case "staged":
		return tx.Commit(ctx)
	case "completing":
		if err := tx.Commit(ctx); err != nil {
			return err
		}
		return c.recordStitchStage(ctx, o)
	case "uploading":
	default:
		return errors.New("stitch stage is not uploadable")
	}
	parts, err := c.stitchStageCompletedParts(ctx, tx, o)
	if err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `UPDATE nas_stitch_r2_stage_objects SET state='completing' WHERE id=$1`, id); err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return err
	}
	_, err = c.s3.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket: aws.String(c.bucket), Key: aws.String(o.key), UploadId: aws.String(o.upload),
		IfNoneMatch: aws.String("*"), MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
	}, noStitchStageRetries)
	if err != nil {
		return errors.New("stitch multipart completion unconfirmed; reconcile before retry")
	}
	return c.recordStitchStage(ctx, o)
}

func (c *Client) stitchStageCompletedParts(ctx context.Context, tx pgx.Tx, o StitchStageUpload) ([]types.CompletedPart, error) {
	rows, err := tx.Query(ctx, `SELECT part_number,sha256 FROM nas_stitch_r2_parts
 WHERE object_id=$1 ORDER BY part_number`, o.ID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	expected := make([]string, 0, (o.Size+StitchStagePartBytes-1)/StitchStagePartBytes)
	for rows.Next() {
		var number int
		var sha string
		if err := rows.Scan(&number, &sha); err != nil {
			return nil, err
		}
		if number != len(expected)+1 {
			return nil, errors.New("stitch part ledger has a gap")
		}
		digest, err := hex.DecodeString(sha)
		if err != nil || len(digest) != 32 {
			return nil, errors.New("stitch part ledger digest differs")
		}
		expected = append(expected, base64.StdEncoding.EncodeToString(digest))
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if int64(len(expected)) != (o.Size+StitchStagePartBytes-1)/StitchStagePartBytes {
		return nil, errors.New("stitch part ledger is incomplete")
	}
	actual, err := c.s3.ListParts(ctx, &s3.ListPartsInput{
		Bucket: aws.String(c.bucket), Key: aws.String(o.key), UploadId: aws.String(o.upload),
		MaxParts: aws.Int32(1000),
	})
	if err != nil || actual.IsTruncated == nil || *actual.IsTruncated || len(actual.Parts) != len(expected) {
		return nil, errors.New("stitch provider part inventory incomplete")
	}
	completion := make([]types.CompletedPart, 0, len(expected))
	for i, part := range actual.Parts {
		length := min(StitchStagePartBytes, o.Size-int64(i)*StitchStagePartBytes)
		if aws.ToInt32(part.PartNumber) != int32(i+1) || aws.ToInt64(part.Size) != length ||
			aws.ToString(part.ChecksumSHA256) != expected[i] || aws.ToString(part.ETag) == "" {
			return nil, errors.New("stitch provider part identity differs")
		}
		completion = append(completion, types.CompletedPart{
			ETag: part.ETag, PartNumber: part.PartNumber, ChecksumSHA256: part.ChecksumSHA256,
		})
	}
	return completion, nil
}

func (c *Client) recordStitchStage(ctx context.Context, o StitchStageUpload) error {
	head, err := c.Head(ctx, o.key)
	if err != nil || head.ETag == "" || head.LastModified.IsZero() ||
		head.SizeBytes != o.Size || head.SHA256Metadata != o.sha {
		return errors.New("stitch staged object identity unavailable")
	}
	tag, err := c.writerDB.Exec(ctx, `UPDATE nas_stitch_r2_stage_objects
 SET state='staged',etag=$2,version_id=nullif($3,''),last_modified=$4
 WHERE id=$1 AND state='completing' AND upload_id=$5`, o.ID, head.ETag, head.VersionID, head.LastModified, o.upload)
	if err != nil || tag.RowsAffected() != 1 {
		return errors.New("stitch staged object identity not durable")
	}
	return nil
}

// RetireStitchStage is an operator-only one-shot retirement of an isolated
// generation. A previously issued delete can only be reconciled by absence.
func (c *Client) RetireStitchStage(ctx context.Context, objectID int64, sha string, proofExpires time.Time, reconcile bool) error {
	if c.writerDB == nil || objectID <= 0 {
		return errors.New("stitch stage retirement requires the exact operator target")
	}
	// The transaction, including its fence locks, must outlive the wire attempt.
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	conn, err := c.writerDB.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	var owned bool
	if err = conn.QueryRow(ctx, `SELECT pg_try_advisory_lock(7343226011173)`).Scan(&owned); err != nil || !owned {
		return errors.New("R2 retirement lane busy")
	}
	defer conn.Exec(context.Background(), `SELECT pg_advisory_unlock(7343226011173)`)
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	if _, err = tx.Exec(ctx, `SET LOCAL idle_in_transaction_session_timeout='90s'`); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `SELECT epoch FROM r2_inverse_ke_epoch FOR SHARE`); err != nil {
		return err
	}
	if !reconcile {
		observed := time.Now()
		dbNow, err := c.stitchStageDeleteFence(ctx, tx)
		if err != nil {
			return err
		}
		if !proofExpires.After(dbNow) {
			return errors.New("fresh conditional staging delete proof required")
		}
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, observed.Add(proofExpires.Sub(dbNow)))
		defer cancel()
	}
	var key, etag, version string
	var size int64
	var modified time.Time
	var issued, absent bool
	err = tx.QueryRow(ctx, `SELECT o.object_key,o.etag,coalesce(o.version_id,''),o.size_bytes,o.last_modified,
 d.issued_at IS NOT NULL,d.absence_observed_at IS NOT NULL
 FROM nas_stitch_r2_stage_objects o JOIN nas_stitch_r2_hours h ON h.id=o.hour_id AND h.attempt=o.attempt
 JOIN nas_stitch_r2_certificates c ON c.hour_id=h.id AND c.attempt=h.attempt
  AND c.plan_sha256=h.plan_sha256 AND c.manifest_sha256=h.staged_manifest_sha256
 JOIN nas_r2_copy_items q ON q.kind='stitch_output' AND q.source_id=o.id AND q.connection_id=13
  AND q.manifest_sha256=c.manifest_sha256 AND nas_stitch_r2_exact_copy_receipt(q,o)
 LEFT JOIN nas_stitch_r2_stage_retirements d ON d.object_id=o.id
 WHERE o.id=$1 AND o.sha256=$2 AND o.bucket=$3 AND o.endpoint_sha256=$4
  AND o.state='staged' AND h.state='certified' AND COALESCE(o.grants_drain_at,'infinity')<clock_timestamp()
  AND COALESCE(q.stitch_read_grants_until,'-infinity')<clock_timestamp()
  AND COALESCE(q.range_until,'-infinity')<clock_timestamp()
  AND NOT EXISTS(SELECT 1 FROM delivery_keep_holds k WHERE (k.bucket=o.bucket AND k.object_key=o.object_key)
    OR k.relative_path=o.relative_path OR (k.sha256=o.sha256 AND k.size_bytes=o.size_bytes))
  AND NOT EXISTS(SELECT 1 FROM recording_collation_outputs j WHERE j.object_key=o.object_key OR j.nas_relative_path=o.relative_path)
 FOR SHARE OF o,h,c,q`, objectID, sha, c.bucket, c.EndpointSHA256()).Scan(&key, &etag, &version, &size, &modified, &issued, &absent)
	if err != nil {
		return errors.New("stitch stage lacks exact NAS custody, drain, or dependency clearance")
	}
	if version != "" {
		return errors.New("staging conditional delete proof covers only unversioned objects")
	}
	if absent {
		return nil
	}
	if issued != reconcile {
		return errors.New("stitch stage retirement state differs; issued targets require reconciliation")
	}
	head, err := c.Head(ctx, key)
	if reconcile {
		if !IsNotFound(err) {
			return errors.New("stitch stage absence remains unconfirmed")
		}
		if _, err = tx.Exec(ctx, `UPDATE nas_stitch_r2_stage_retirements SET absence_observed_at=clock_timestamp()
 WHERE object_id=$1 AND absence_observed_at IS NULL`, objectID); err != nil {
			return err
		}
		return tx.Commit(ctx)
	}
	if err != nil || cleanETag(head.ETag) != cleanETag(etag) || head.VersionID != version ||
		head.SizeBytes != size || head.SHA256Metadata != sha || !head.LastModified.Equal(modified) {
		return errors.New("stitch stage generation differs")
	}
	uploads, err := c.s3.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{
		Bucket: aws.String(c.bucket), Prefix: aws.String(key), MaxUploads: aws.Int32(1),
	})
	if err != nil || uploads.IsTruncated == nil || *uploads.IsTruncated || len(uploads.Uploads) != 0 {
		return errors.New("stitch stage multipart work has not drained")
	}
	if _, err := c.stitchStageDeleteFence(ctx, tx); err != nil {
		return err
	}
	// Commit the one-shot reservation on a second connection while the
	// outer lock and dependency epoch remain held through the wire attempt.
	if _, err = c.writerDB.Exec(ctx, `INSERT INTO nas_stitch_r2_stage_retirements(object_id) VALUES($1)`, objectID); err != nil {
		return err
	}
	if err := c.deleteStitchStageExact(ctx, key, etag, func(wireCtx context.Context) error {
		now, err := c.stitchStageDeleteFence(wireCtx, tx)
		if err != nil {
			return err
		}
		if !proofExpires.After(now) {
			return errors.New("conditional staging delete proof expired before transport")
		}
		return nil
	}); err != nil {
		return errors.New("stitch stage delete outcome unconfirmed; reconcile only")
	}
	if _, err = c.Head(ctx, key); !IsNotFound(err) {
		return errors.New("stitch stage delete absence unconfirmed; reconcile only")
	}
	if _, err = tx.Exec(ctx, `UPDATE nas_stitch_r2_stage_retirements SET absence_observed_at=clock_timestamp()
 WHERE object_id=$1 AND absence_observed_at IS NULL`, objectID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (c *Client) stitchStageDeleteFence(ctx context.Context, tx pgx.Tx) (time.Time, error) {
	var now time.Time
	var blocked bool
	err := tx.QueryRow(ctx, `SELECT clock_timestamp(),r2_retirement_writes_blocked($1,$2)
 FROM r2_retirement_writer_fences WHERE bucket=$1 AND endpoint_sha256=$2 FOR SHARE`,
		c.bucket, c.EndpointSHA256()).Scan(&now, &blocked)
	if err != nil || !blocked {
		return time.Time{}, errors.New("exact live R2 writer fence and drain required for staging retirement")
	}
	return now, nil
}

func (c *Client) deleteStitchStageExact(ctx context.Context, key, etag string, check func(context.Context) error) error {
	in := &s3.DeleteObjectInput{Bucket: aws.String(c.bucket), Key: aws.String(key),
		IfMatch: aws.String(`"` + cleanETag(etag) + `"`)}
	_, err := c.s3.DeleteObject(ctx, in, func(o *s3.Options) {
		noStitchStageRetries(o)
		o.HTTPClient = &http.Client{Transport: &stitchStageDeleteTransport{base: c.deleteTransport, check: check},
			CheckRedirect: refuseRetirementRedirect}
	})
	return err
}

type stitchStageDeleteTransport struct {
	base  http.RoundTripper
	check func(context.Context) error
	used  atomic.Bool
}

func (t *stitchStageDeleteTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Method != http.MethodDelete || !t.used.CompareAndSwap(false, true) {
		return nil, errors.New("staging transport allows one DELETE")
	}
	if err := t.check(req.Context()); err != nil {
		return nil, err
	}
	return t.base.RoundTrip(req)
}
