package joinedrecording

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"net/http"
	"strconv"
	"time"
)

const putRetrySafetyMargin = 5 * time.Second

var putRetryDelays = [...]time.Duration{250 * time.Millisecond, time.Second, 4 * time.Second, 10 * time.Second}

type putRetryPolicy struct {
	wait func(context.Context, time.Duration) error
	now  func() time.Time
}

func defaultPutRetryPolicy() putRetryPolicy {
	return putRetryPolicy{
		now: time.Now,
		wait: func(ctx context.Context, delay time.Duration) error {
			timer := time.NewTimer(delay)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timer.C:
				return nil
			}
		},
	}
}

func (p putRetryPolicy) normalized() putRetryPolicy {
	defaults := defaultPutRetryPolicy()
	if p.now == nil {
		p.now = defaults.now
	}
	if p.wait == nil {
		p.wait = defaults.wait
	}
	return p
}

func putCreateOnlyWithRetry(ctx context.Context, client CapabilityHTTPClient, authority, bucket string, artifactID int64, key, contentType string, size int64, sha string, leaseExpires func() time.Time, leaseID string, resolve func(context.Context) (ObjectCreateCapability, error), openBody func() (io.ReadCloser, error), policy putRetryPolicy) (putObservation, error) {
	policy = policy.normalized()
	for attempt := 1; attempt <= len(putRetryDelays)+1; attempt++ {
		if err := ctx.Err(); err != nil {
			return putObservation{}, err
		}
		if leaseExpires == nil || !leaseExpires().After(policy.now()) {
			return putObservation{}, context.DeadlineExceeded
		}
		capability, err := resolve(ctx)
		if err != nil {
			err = putErrorWithAttempts(err, attempt)
			if attempt > len(putRetryDelays) || !retryableCreateOnlyPut(err) {
				return putObservation{}, err
			}
			delay := jitteredPutRetryDelay(putRetryDelays[attempt-1], leaseID, artifactID, attempt)
			if leaseExpires == nil || !leaseExpires().After(policy.now().Add(delay+putRetrySafetyMargin)) {
				return putObservation{}, err
			}
			if err := policy.wait(ctx, delay); err != nil {
				return putObservation{}, err
			}
			continue
		}
		body, err := openBody()
		if err != nil {
			return putObservation{}, err
		}
		observation, putErr := putCreateOnlyCapability(ctx, client, authority, bucket, artifactID, key, contentType, size, sha, capability, body)
		_ = body.Close()
		if putErr == nil {
			return observation, nil
		}
		putErr = putErrorWithAttempts(putErr, attempt)
		if attempt > len(putRetryDelays) || !retryableCreateOnlyPut(putErr) {
			return putObservation{}, putErr
		}
		delay := jitteredPutRetryDelay(putRetryDelays[attempt-1], leaseID, artifactID, attempt)
		// The failed URL is discarded. Bound the wait by the renewable lease;
		// the next attempt resolves and validates a fresh signed capability.
		if leaseExpires == nil || !leaseExpires().After(policy.now().Add(delay+putRetrySafetyMargin)) {
			return putObservation{}, putErr
		}
		if err := policy.wait(ctx, delay); err != nil {
			return putObservation{}, err
		}
	}
	panic("unreachable")
}

func retryableCreateOnlyPut(err error) bool {
	var storageErr *StorageCapabilityError
	if !errors.As(err, &storageErr) || (storageErr.Operation != "put" && storageErr.Operation != "create_capability") {
		return false
	}
	if storageErr.Operation == "put" && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
		return false
	}
	if storageErr.Reason == "transport" {
		return true
	}
	if storageErr.Reason != "status" {
		return false
	}
	return storageErr.StatusCode == http.StatusRequestTimeout || storageErr.StatusCode == http.StatusTooEarly || storageErr.StatusCode == http.StatusTooManyRequests || storageErr.StatusCode >= 500 && storageErr.StatusCode <= 599
}

func putErrorWithAttempts(err error, attempts int) error {
	var storageErr *StorageCapabilityError
	if !errors.As(err, &storageErr) {
		return err
	}
	copy := *storageErr
	copy.Attempts = attempts
	return &copy
}

func jitteredPutRetryDelay(base time.Duration, leaseID string, artifactID int64, attempt int) time.Duration {
	sum := sha256.Sum256([]byte(leaseID + ":" + strconv.FormatInt(artifactID, 10) + ":" + strconv.Itoa(attempt)))
	// 7500..12500 basis points, inclusive.
	basisPoints := int64(7500 + binary.BigEndian.Uint16(sum[:2])%5001)
	return time.Duration(int64(base) * basisPoints / 10000)
}
