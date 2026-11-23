package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// R2 answers a burst of same-prefix lists with 429 ServiceUnavailable; the
// listing must back off and finish instead of failing the whole cycle.
func TestListRetriesR2TooManyRequests(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) <= 2 {
			w.WriteHeader(http.StatusTooManyRequests)
			w.Write([]byte(`<?xml version="1.0"?><Error><Code>ServiceUnavailable</Code><Message>Reduce your concurrent request rate for the same object.</Message></Error>`))
			return
		}
		w.Header().Set("Content-Type", "application/xml")
		w.Write([]byte(`<?xml version="1.0"?><ListBucketResult><Name>b</Name><IsTruncated>false</IsTruncated><Contents><Key>a/k</Key><Size>3</Size><ETag>"e"</ETag><LastModified>2026-11-23T00:00:00Z</LastModified></Contents></ListBucketResult>`))
	}))
	defer srv.Close()
	client := s3.New(s3.Options{Region: "auto", BaseEndpoint: aws.String(srv.URL), UsePathStyle: true,
		Credentials: credentials.NewStaticCredentialsProvider("k", "s", ""), Retryer: newRetryer()})
	var got []object
	_, err := listPrefix(context.Background(), store{s3: client, bucket: "b"}, "a/", false, func(o []object) error { got = append(got, o...); return nil })
	if err != nil || len(got) != 1 || got[0].Key != "a/k" || calls.Load() != 3 {
		t.Fatalf("err=%v got=%v calls=%d", err, got, calls.Load())
	}
}
