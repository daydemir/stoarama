package r2

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// ForCollationRecovery returns a client whose writes are admitted only as
// hourID's collation v2 recovery PUTs (migration 0262): a restore of a
// registered hour's catalogued bytes, or a new hour in a fresh recovery- batch.
// Its generic PUTs and capabilities are refused: PutReaderIfAbsentVerified is
// its one write, and each one's durable intent names the hour and the exact
// key, size and sha256 it creates. Paths with their own admission row
// (PresignNASRestorePut, stitch staging, DeleteObjectOnce) are unchanged by the
// binding and stay gated by their own rows.
func (c *Client) ForCollationRecovery(hourID string) *Client {
	bound := *c
	bound.collationRecoveryHourID = hourID
	return &bound
}

// putCollationRecovery is PutReaderIfAbsentVerified for a recovery client.
// Under its wire admission it HEADs the key and sends no PUT when the object
// is present; otherwise it sends one create-only PUT whose ChecksumSHA256 is
// the admitted sha256, so R2 refuses any other body before committing it.
// Every path that leaves no possible write behind completes the intent at once:
// no PUT sent, or a PUT R2 definitively refused. Only an ambiguous PUT, or a
// read-back or completion failure after one, leaves the intent open for
// SettleCollationRecoveryIntents.
func (c *Client) putCollationRecovery(ctx context.Context, in *s3.PutObjectInput, size int64, sha string) (ObjectHead, bool, error) {
	key := aws.ToString(in.Key)
	if c.writerDB == nil {
		return ObjectHead{}, false, errors.New("collation recovery writes require database admission")
	}
	digest, err := hex.DecodeString(sha)
	if err != nil {
		return ObjectHead{}, false, err
	}
	in.ChecksumSHA256 = aws.String(base64.StdEncoding.EncodeToString(digest))
	// The intent's insert trigger admits it only inside the approved scope; it
	// commits before any network request.
	var operationID int64
	if err := c.writerDB.QueryRow(ctx, `INSERT INTO r2_retirement_writer_operations(bucket,collation_recovery_hour_id,object_key,size_bytes,sha256)
		VALUES($1,$2,$3,$4,$5) RETURNING id`, c.bucket, c.collationRecoveryHourID, key, size, sha).Scan(&operationID); err != nil {
		return ObjectHead{}, false, errors.New("collation recovery write admission refused or unavailable")
	}
	finish := func(cause error) error {
		finishCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if _, err := c.writerDB.Exec(finishCtx, `UPDATE r2_retirement_writer_operations SET completed_at=clock_timestamp() WHERE id=$1 AND completed_at IS NULL`, operationID); err != nil {
			return fmt.Errorf("%w; intent retained", cause)
		}
		return cause
	}
	wire, err := c.writerDB.Begin(ctx)
	if err == nil {
		_, err = wire.Exec(ctx, `SELECT collation_v2_recovery_admit_wire($1)`, operationID)
	}
	if err != nil {
		if wire != nil {
			_ = wire.Rollback(context.Background())
		}
		return ObjectHead{}, false, finish(errors.New("collation recovery wire admission refused; no PUT sent"))
	}
	// The wire transaction share-locks the intent row; it must end before this
	// session completes that row.
	release := func() { _ = wire.Rollback(context.Background()) }
	created, sent, err := c.headThenPut(ctx, in)
	release()
	if err != nil {
		if sent {
			return ObjectHead{}, false, err
		}
		return ObjectHead{}, false, finish(err)
	}
	head, err := c.readBackExact(ctx, key, size, sha)
	if err != nil {
		if created {
			return ObjectHead{}, false, err
		}
		// Our PUT wrote nothing (present, or 412): the key holds other bytes.
		return ObjectHead{}, false, finish(err)
	}
	finishCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	// At least this PUT's own intent, no longer share-locked, must complete.
	var settled int
	if err := c.writerDB.QueryRow(finishCtx, `SELECT collation_v2_recovery_settle($1,$2)`, c.bucket, key).Scan(&settled); err != nil {
		return ObjectHead{}, false, errors.New("R2 writer completion not durable; intent retained")
	}
	if settled == 0 {
		return ObjectHead{}, false, fmt.Errorf("collation recovery intent %d for %s did not complete", operationID, key)
	}
	return head, created, nil
}

// headThenPut reports whether the PUT created the object, and whether a
// request that may still write was sent: false for a present key, a HEAD
// failure, a 412 or any other definitive 4xx refusal.
func (c *Client) headThenPut(ctx context.Context, in *s3.PutObjectInput) (created, sent bool, err error) {
	key := aws.ToString(in.Key)
	if _, err := c.Head(ctx, key); err == nil {
		return false, false, nil
	} else if !IsNotFound(err) {
		return false, false, fmt.Errorf("collation recovery HEAD %s: %w", key, err)
	}
	if _, err := c.s3.PutObject(ctx, in); err != nil {
		if isPreconditionFailed(err) {
			return false, false, nil
		}
		var responseErr *awshttp.ResponseError
		refused := errors.As(err, &responseErr) && responseErr.HTTPStatusCode() >= http.StatusBadRequest && responseErr.HTTPStatusCode() < http.StatusInternalServerError
		return false, !refused, fmt.Errorf("conditional put object %s: %w", key, err)
	}
	return true, true, nil
}

// CollationRecoverySettlement reports SettleCollationRecoveryIntents by key:
// Verified holds the bytes an intent names, Absent holds nothing (its hour
// needs another run), Foreign holds other bytes.
type CollationRecoverySettlement struct {
	Keys     int      `json:"keys"`
	Settled  int      `json:"settled"`
	Verified []string `json:"verified"`
	Absent   []string `json:"absent"`
	Foreign  []string `json:"foreign"`
	Applied  bool     `json:"applied"`
}

// SettleCollationRecoveryIntents completes every unfinished recovery intent
// whose PUT is no longer in flight (its row is not share-locked). That is safe
// for every recovery key, present or absent: each is held from retirement for
// good, and any late copy of its PUT is create-only and checksum-bound. It
// reports what each key holds now.
func (c *Client) SettleCollationRecoveryIntents(ctx context.Context, apply bool) (CollationRecoverySettlement, error) {
	out := CollationRecoverySettlement{Verified: []string{}, Absent: []string{}, Foreign: []string{}, Applied: apply}
	if c.writerDB == nil {
		return out, errors.New("settling collation recovery intents requires the writer database")
	}
	rows, err := c.writerDB.Query(ctx, `SELECT object_key,size_bytes,sha256 FROM r2_retirement_writer_operations
		WHERE completed_at IS NULL AND collation_recovery_hour_id IS NOT NULL AND bucket=$1 ORDER BY 1,2,3`, c.bucket)
	if err != nil {
		return out, err
	}
	var keys []string
	idents := map[string][]recoveryIdentity{}
	for rows.Next() {
		var key string
		var id recoveryIdentity
		if err := rows.Scan(&key, &id.size, &id.sha); err != nil {
			rows.Close()
			return out, err
		}
		if len(idents[key]) == 0 {
			keys = append(keys, key)
		}
		idents[key] = append(idents[key], id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return out, err
	}
	out.Keys = len(keys)
	for _, key := range keys {
		state, err := c.recoveryKeyState(ctx, key, idents[key])
		if err != nil {
			return out, err
		}
		switch state {
		case "verified":
			out.Verified = append(out.Verified, key)
		case "absent":
			out.Absent = append(out.Absent, key)
		default:
			out.Foreign = append(out.Foreign, key)
		}
		if !apply {
			continue
		}
		var n int
		if err := c.writerDB.QueryRow(ctx, `SELECT collation_v2_recovery_settle($1,$2)`, c.bucket, key).Scan(&n); err != nil {
			return out, fmt.Errorf("settle %s: %w", key, err)
		}
		out.Settled += n
	}
	return out, nil
}

// recoveryIdentity is the exact size and sha256 one recovery intent creates.
type recoveryIdentity struct {
	size int64
	sha  string
}

func (c *Client) recoveryKeyState(ctx context.Context, key string, idents []recoveryIdentity) (string, error) {
	if _, err := c.Head(ctx, key); IsNotFound(err) {
		return "absent", nil
	} else if err != nil {
		return "", fmt.Errorf("HEAD %s: %w", key, err)
	}
	for _, id := range idents {
		_, err := c.readBackExact(ctx, key, id.size, id.sha)
		if err == nil {
			return "verified", nil
		}
		if !errors.Is(err, errImmutableMismatch) {
			return "", err
		}
	}
	return "foreign", nil
}
