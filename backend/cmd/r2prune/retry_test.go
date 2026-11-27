package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func listWith(t *testing.T, failures int32, status int, code string) (int32, error) {
	t.Helper()
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) <= failures {
			w.WriteHeader(status)
			w.Write([]byte(`<?xml version="1.0"?><Error><Code>` + code + `</Code><Message>m</Message></Error>`))
			return
		}
		w.Header().Set("Content-Type", "application/xml")
		w.Write([]byte(`<?xml version="1.0"?><ListBucketResult><Name>b</Name><IsTruncated>false</IsTruncated><Contents><Key>a/k</Key><Size>3</Size><ETag>"e"</ETag><LastModified>2026-11-23T00:00:00Z</LastModified></Contents></ListBucketResult>`))
	}))
	defer srv.Close()
	r := newRetryer()
	client := s3.New(s3.Options{Region: "auto", BaseEndpoint: aws.String(srv.URL), UsePathStyle: true,
		Credentials: credentials.NewStaticCredentialsProvider("k", "s", ""),
		Retryer:     retry.AddWithMaxBackoffDelay(r, time.Millisecond)})
	_, err := listPrefix(context.Background(), store{s3: client, bucket: "b"}, "a/", false, func([]object) error { return nil })
	return calls.Load(), err
}

// R2 answers a burst of same-prefix lists with 429 ServiceUnavailable; the
// listing must back off and finish instead of failing the whole cycle. Each
// rule retries on its own, and anything else still fails fast.
func TestListRetriesR2Throttles(t *testing.T) {
	for name, c := range map[string]struct {
		status int
		code   string
		calls  int32
		ok     bool
	}{
		"r2 429":             {429, "ServiceUnavailable", 3, true},
		"429 other code":     {429, "Other", 3, true},
		"code on 400 status": {400, "ServiceUnavailable", 3, true},
		"403 fails fast":     {403, "AccessDenied", 1, false},
	} {
		calls, err := listWith(t, 2, c.status, c.code)
		if (err == nil) != c.ok || calls != c.calls {
			t.Fatalf("%s: err=%v calls=%d", name, err, calls)
		}
	}
}
