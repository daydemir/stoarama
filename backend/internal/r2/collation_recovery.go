package r2

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// ForCollationRecovery returns a client whose writes are admitted only as
// hourID's collation v2 recovery PUTs (migration 0262): a restore of a
// registered hour's catalogued bytes, or a new hour in a fresh recovery- batch.
// Its generic PUTs and capabilities are refused: PutReaderIfAbsentVerified is
// its one write, and each one's durable intent names the hour and the exact
// key, size and sha256 it creates. Paths with their own admission row (PresignNASRestorePut, stitch
// staging, DeleteObjectOnce) are unchanged by the binding and stay gated by
// their own rows.
func (c *Client) ForCollationRecovery(hourID string) *Client {
	bound := *c
	bound.collationRecoveryHourID = hourID
	return &bound
}

// putIfAbsent sends one create-only PUT and reports whether it created the
// object. A recovery client first HEADs the key under its wire admission and
// sends no PUT at all when the object is present: a restore never writes over
// a present object, and readBackExact still proves its bytes.
func (c *Client) putIfAbsent(ctx context.Context, in *s3.PutObjectInput) (bool, error) {
	key := aws.ToString(in.Key)
	if c.collationRecoveryHourID != "" {
		_, err := c.Head(ctx, key)
		if err == nil {
			return false, nil
		}
		if !IsNotFound(err) {
			return false, fmt.Errorf("collation recovery HEAD %s: %w", key, err)
		}
	}
	if _, err := c.s3.PutObject(ctx, in); err != nil {
		if !isPreconditionFailed(err) {
			return false, fmt.Errorf("conditional put object %s: %w", key, err)
		}
		return false, nil
	}
	return true, nil
}

func (c *Client) admitCreateOnlyWrite(ctx context.Context, key string, size int64, sha string) (func(), func() error, error) {
	if c.collationRecoveryHourID == "" {
		return c.admitWrite(ctx, 0)
	}
	if c.writerDB == nil {
		return nil, nil, errors.New("collation recovery writes require database admission")
	}
	// The intent's insert trigger admits it only inside the approved scope; it
	// commits before any network write.
	var operationID int64
	if err := c.writerDB.QueryRow(ctx, `INSERT INTO r2_retirement_writer_operations(bucket,collation_recovery_hour_id,object_key,size_bytes,sha256)
		VALUES($1,$2,$3,$4,$5) RETURNING id`, c.bucket, c.collationRecoveryHourID, key, size, sha).Scan(&operationID); err != nil {
		return nil, nil, errors.New("collation recovery write admission refused or unavailable")
	}
	tx, err := c.writerDB.Begin(ctx)
	if err == nil {
		_, err = tx.Exec(ctx, `SELECT collation_v2_recovery_admit_wire($1)`, operationID)
	}
	if err != nil {
		if tx != nil {
			_ = tx.Rollback(context.Background())
		}
		// No PUT was sent, so the refused intent completes now.
		finishCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if _, err := c.writerDB.Exec(finishCtx, `UPDATE r2_retirement_writer_operations SET completed_at=clock_timestamp() WHERE id=$1 AND completed_at IS NULL`, operationID); err != nil {
			return nil, nil, errors.New("collation recovery wire admission refused; intent retained")
		}
		return nil, nil, errors.New("collation recovery wire admission refused; no PUT sent")
	}
	release := func() { _ = tx.Rollback(context.Background()) }
	complete := func() error {
		release()
		finishCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		// The caller has read key's exact bytes back, so this intent and any
		// earlier lost one for the same key settle together.
		if _, err := c.writerDB.Exec(finishCtx, `SELECT collation_v2_recovery_settle($1,$2,$3,$4)`, c.bucket, key, size, sha); err != nil {
			return errors.New("R2 writer completion not durable; intent retained")
		}
		return nil
	}
	return release, complete, nil
}

// CollationRecoverySettlement reports SettleCollationRecoveryIntents.
type CollationRecoverySettlement struct {
	Keys    int      `json:"keys"`
	Settled int      `json:"settled"`
	Held    []string `json:"held"`
	Applied bool     `json:"applied"`
}

// SettleCollationRecoveryIntents completes the unfinished recovery intents of
// every key whose exact bytes R2 now holds, for example after a crash between a
// PUT and its completion. A key R2 does not hold with those bytes stays held,
// and so does any intent whose PUT is still in flight.
func (c *Client) SettleCollationRecoveryIntents(ctx context.Context, apply bool) (CollationRecoverySettlement, error) {
	out := CollationRecoverySettlement{Held: []string{}, Applied: apply}
	if c.writerDB == nil {
		return out, errors.New("settling collation recovery intents requires the writer database")
	}
	type identity struct {
		key, sha string
		size     int64
	}
	rows, err := c.writerDB.Query(ctx, `SELECT DISTINCT object_key,size_bytes,sha256 FROM r2_retirement_writer_operations
		WHERE completed_at IS NULL AND collation_recovery_hour_id IS NOT NULL AND bucket=$1 ORDER BY 1,2,3`, c.bucket)
	if err != nil {
		return out, err
	}
	var pending []identity
	for rows.Next() {
		var id identity
		if err := rows.Scan(&id.key, &id.size, &id.sha); err != nil {
			rows.Close()
			return out, err
		}
		pending = append(pending, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return out, err
	}
	out.Keys = len(pending)
	for _, id := range pending {
		if _, err := c.readBackExact(ctx, id.key, id.size, id.sha); IsNotFound(err) || errors.Is(err, errImmutableMismatch) {
			out.Held = append(out.Held, id.key)
			continue
		} else if err != nil {
			return out, err
		}
		if !apply {
			continue
		}
		var n int
		if err := c.writerDB.QueryRow(ctx, `SELECT collation_v2_recovery_settle($1,$2,$3,$4)`, c.bucket, id.key, id.size, id.sha).Scan(&n); err != nil {
			return out, fmt.Errorf("settle %s: %w", id.key, err)
		}
		out.Settled += n
	}
	return out, nil
}
