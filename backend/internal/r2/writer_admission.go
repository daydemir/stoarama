package r2

import (
	"context"
	"errors"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func (c *Client) admitWrite(ctx context.Context, ttl time.Duration) (func(), func() error, error) {
	noop := func() {}
	complete := func() error { return nil }
	if c.writerDB == nil {
		if c.ungatedWriter {
			return noop, complete, nil
		}
		return nil, nil, errors.New("R2 writes require database admission; unbound client is read-only")
	}
	tx, err := c.writerDB.Begin(ctx)
	if err != nil {
		return nil, nil, errors.New("R2 writer admission database unavailable")
	}
	release := func() { _ = tx.Rollback(context.Background()) }
	if _, err = tx.Exec(ctx, `SELECT r2_retirement_admit_writer()`); err != nil {
		release()
		return nil, nil, errors.New("R2 writer admission refused or unavailable")
	}
	var operationID int64
	if ttl > 0 {
		if ttl > 7*24*time.Hour {
			release()
			return nil, nil, errors.New("R2 PUT capability exceeds seven days")
		}
		_, err = tx.Exec(ctx, `INSERT INTO r2_retirement_writer_capabilities(bucket,expires_at) VALUES($1,clock_timestamp()+$2::bigint*interval '1 microsecond'+interval '2 minutes')`, c.bucket, ttl.Microseconds())
	} else {
		// Commit intent BEFORE any network write. Connection/process loss must not
		// make an upload disappear from drain state merely by releasing its lock.
		err = tx.QueryRow(ctx, `INSERT INTO r2_retirement_writer_operations(bucket) VALUES($1) RETURNING id`, c.bucket).Scan(&operationID)
	}
	if err == nil {
		err = tx.Commit(ctx)
	}
	if err != nil {
		release()
		return nil, nil, errors.New("R2 writer durable admission failed")
	}
	if ttl > 0 {
		return noop, complete, nil
	}
	tx, err = c.writerDB.Begin(ctx)
	if err == nil {
		_, err = tx.Exec(ctx, `SELECT r2_retirement_admit_writer()`)
	}
	if err != nil {
		if tx != nil {
			_ = tx.Rollback(context.Background())
		}
		return nil, nil, errors.New("R2 writer wire admission failed; durable intent retained")
	}
	release = func() { _ = tx.Rollback(context.Background()) }
	complete = func() error {
		release()
		finishCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		_, err := c.writerDB.Exec(finishCtx, `UPDATE r2_retirement_writer_operations SET completed_at=clock_timestamp() WHERE id=$1 AND completed_at IS NULL`, operationID)
		if err != nil {
			return errors.New("R2 writer completion not durable; intent retained")
		}
		return nil
	}
	// Only successful wire operations call complete. An error/lost response keeps
	// durable intent indefinitely; a timeout or operator attestation cannot clear it.
	return release, complete, nil
}

// RequireNoMultipartUploads inventories real provider state, including uploads
// whose parts are complete but whose final completion can still replace a key.
// Zero for the entire bucket is deliberately stronger than manifest overlap.
func (c *Client) RequireNoMultipartUploads(ctx context.Context) error {
	out, err := c.s3.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{Bucket: aws.String(c.bucket), MaxUploads: aws.Int32(1)})
	if err != nil {
		return errors.New("R2 multipart inventory unavailable")
	}
	if out.IsTruncated == nil || len(out.Uploads) != 0 || aws.ToBool(out.IsTruncated) {
		return errors.New("R2 multipart uploads have not drained")
	}
	return nil
}
