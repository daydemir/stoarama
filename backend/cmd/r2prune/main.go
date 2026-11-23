// r2prune lists the R2 bucket directly, classifies every key against the
// owner's keep rules and deletes a reviewed manifest in DeleteObjects batches.
//
//	r2prune list   --out snapshot.tsv                      (stdout when --out is -)
//	r2prune plan   --snapshot S --identities I --db-protect P --closure C \
//	               --controller-protect J --picks K --out DIR
//	r2prune delete --manifest M --expected-sha256 H --controller-protect J \
//	               --db-protect P --ledger L
//
// The plan inputs come from the read-only queries in sql/ (identities per
// stream, closure per 20-day window, protect sets) plus the controller's
// protect-for-nas-20day.jsonl and the owner's r2-picks.json.
//
// R2 credentials come only from the environment: R2_ACCOUNT_ID (or
// R2_ENDPOINT), R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/ratelimit"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const usage = "usage: r2prune list|plan|delete [flags]"

func main() {
	if len(os.Args) < 2 {
		fail(errors.New(usage))
	}
	ctx := context.Background()
	var err error
	switch os.Args[1] {
	case "list":
		err = runList(ctx, os.Args[2:])
	case "plan":
		err = runPlan(os.Args[2:])
	case "delete":
		err = runDelete(ctx, os.Args[2:])
	default:
		err = errors.New(usage)
	}
	if err != nil {
		fail(err)
	}
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "r2prune:", err)
	os.Exit(1)
}

type store struct {
	s3     *s3.Client
	bucket string
}

func storeFromEnv() (store, error) {
	endpoint := os.Getenv("R2_ENDPOINT")
	if endpoint == "" && os.Getenv("R2_ACCOUNT_ID") != "" {
		endpoint = "https://" + os.Getenv("R2_ACCOUNT_ID") + ".r2.cloudflarestorage.com"
	}
	key, secret, bucket := os.Getenv("R2_ACCESS_KEY_ID"), os.Getenv("R2_SECRET_ACCESS_KEY"), os.Getenv("R2_BUCKET")
	if endpoint == "" || key == "" || secret == "" || bucket == "" {
		return store{}, errors.New("R2_ENDPOINT or R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY and R2_BUCKET are required")
	}
	client := s3.New(s3.Options{
		Region:       "auto",
		BaseEndpoint: aws.String(endpoint),
		UsePathStyle: true,
		Credentials:  credentials.NewStaticCredentialsProvider(key, secret, ""),
		Retryer:      newRetryer(),
		// R2 accepts the checksums S3 requires; it rejects optional defaults.
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
		ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired,
	})
	return store{s3: client, bucket: bucket}, nil
}

// newRetryer backs off on R2's 429 "Reduce your concurrent request rate"
// (code ServiceUnavailable), which the SDK does not treat as retryable by
// default; SlowDown and 5xx already are. Backoff is the SDK's capped
// exponential with jitter. Retrying is safe: list pages reuse their
// continuation token and DeleteObjects is idempotent for the same key set.
// Attempts stay bounded; a persistent refusal still fails the run closed.
func newRetryer() aws.Retryer {
	return retry.NewStandard(func(o *retry.StandardOptions) {
		o.MaxAttempts = 10
		o.MaxBackoff = 20 * time.Second
		o.RateLimiter = ratelimit.None
		o.Retryables = append([]retry.IsErrorRetryable{
			retry.RetryableHTTPStatusCode{Codes: map[int]struct{}{429: {}}},
			retry.RetryableErrorCode{Codes: map[string]struct{}{"ServiceUnavailable": {}}},
		}, o.Retryables...)
	})
}
