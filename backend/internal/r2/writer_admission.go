package r2

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func (c *Client) admitWrite(ctx context.Context, ttl time.Duration) (func(), func() error, error) {
	if c.collationRecoveryHourID != "" {
		return nil, nil, errors.New("collation recovery admission allows only create-only verified PUTs")
	}
	if ttl > 7*24*time.Hour {
		return nil, nil, errors.New("R2 PUT capability exceeds seven days")
	}
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

// ListMultipartUploadsPage reads exactly one multipart inventory page under the
// caller's read options.
func (c *Client) ListMultipartUploadsPage(ctx context.Context, in *s3.ListMultipartUploadsInput, read ReadOptions) (*s3.ListMultipartUploadsOutput, error) {
	return c.s3.ListMultipartUploads(ctx, in, read.apply)
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

// RequireNoMultipartOutsideStitchStaging retains the provider-side veto for
// every upload other than an exact, durably bound stitch staging attempt.
func (c *Client) RequireNoMultipartOutsideStitchStaging(
	ctx context.Context, known func(context.Context, string, string) (bool, error),
) error {
	return c.RequireNoMultipartOutsideStitchStagingPages(ctx, func(ctx context.Context, in *s3.ListMultipartUploadsInput) (*s3.ListMultipartUploadsOutput, error) {
		return c.ListMultipartUploadsPage(ctx, in, ReadOptions{})
	}, known)
}

// RequireNoMultipartOutsideStitchStagingPages is the form the inverse executor
// uses: pageRequest runs one page attempt and may retry it in place. It must
// return the page the SDK decoded for that attempt, so this loop alone decides
// what a complete inventory is.
func (c *Client) RequireNoMultipartOutsideStitchStagingPages(
	ctx context.Context,
	pageRequest func(context.Context, *s3.ListMultipartUploadsInput) (*s3.ListMultipartUploadsOutput, error),
	known func(context.Context, string, string) (bool, error),
) error {
	request := &s3.ListMultipartUploadsInput{Bucket: aws.String(c.bucket), MaxUploads: aws.Int32(1000)}
	type marker struct{ key, upload string }
	seen := map[marker]bool{}
	for {
		page, err := pageRequest(ctx, request)
		if err != nil {
			// Keep the page's own error, wrapped: it carries the status the
			// read policy classified, and the inventory still fails closed.
			return fmt.Errorf("R2 multipart inventory unavailable or incomplete: %w", err)
		}
		if page == nil || page.IsTruncated == nil {
			return errors.New("R2 multipart inventory unavailable or incomplete")
		}
		for _, object := range page.Uploads {
			key, upload := aws.ToString(object.Key), aws.ToString(object.UploadId)
			if !strings.HasPrefix(key, "stitch-staging/") || upload == "" {
				return errors.New("R2 multipart uploads have not drained")
			}
			exempt, err := known(ctx, key, upload)
			if err != nil {
				return fmt.Errorf("check R2 stitch upload identity: %w", err)
			}
			if !exempt {
				return errors.New("unregistered R2 stitch multipart upload")
			}
		}
		if !aws.ToBool(page.IsTruncated) {
			return nil
		}
		next := marker{aws.ToString(page.NextKeyMarker), aws.ToString(page.NextUploadIdMarker)}
		if next.key == "" || next.upload == "" || seen[next] {
			return errors.New("R2 multipart inventory has an invalid continuation")
		}
		seen[next] = true
		request.KeyMarker, request.UploadIdMarker = page.NextKeyMarker, page.NextUploadIdMarker
	}
}
