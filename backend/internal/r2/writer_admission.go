package r2

import (
	"context"
	"errors"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func (c *Client) admitWrite(ctx context.Context, ttl time.Duration) (func(), error) {
	// Standalone clients cannot claim to be fenced. All DB-driven production
	// constructors bind WriterDB; ungated credentials remain a universal veto.
	if c.writerDB == nil {
		if c.ungatedWriter {
			return func() {}, nil
		}
		return nil, errors.New("R2 writes require database admission; unbound client is read-only")
	}
	tx, err := c.writerDB.Begin(ctx)
	if err != nil {
		return nil, errors.New("R2 writer admission database unavailable")
	}
	release := func() { _ = tx.Rollback(context.Background()) }
	if _, err = tx.Exec(ctx, `SELECT r2_retirement_admit_writer()`); err != nil {
		release()
		return nil, errors.New("R2 writer admission refused or unavailable")
	}
	if ttl > 0 {
		if ttl > 7*24*time.Hour {
			release()
			return nil, errors.New("R2 PUT capability exceeds seven days")
		}
		// Commit before a URL can escape; consumption never revokes a capability.
		_, err = tx.Exec(ctx, `INSERT INTO r2_retirement_writer_capabilities(bucket,expires_at) VALUES($1,clock_timestamp()+$2::bigint*interval '1 microsecond'+interval '2 minutes')`, c.bucket, ttl.Microseconds())
		if err == nil {
			err = tx.Commit(ctx)
		}
		if err != nil {
			release()
			return nil, errors.New("R2 writer capability admission failed")
		}
	}
	return release, nil
}

// RequireNoMultipartUploads inventories real provider state, including uploads
// whose parts are complete but whose final completion can still replace a key.
// Zero for the entire bucket is deliberately stronger than manifest overlap.
func (c *Client) RequireNoMultipartUploads(ctx context.Context) error {
	out, err := c.s3.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{Bucket: aws.String(c.bucket), MaxUploads: aws.Int32(1)})
	if err != nil {
		return errors.New("R2 multipart inventory unavailable")
	}
	if len(out.Uploads) != 0 || aws.ToBool(out.IsTruncated) {
		return errors.New("R2 multipart uploads have not drained")
	}
	return nil
}
