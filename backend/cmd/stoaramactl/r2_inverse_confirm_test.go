package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/daydemir/stoarama/backend/internal/r2"
)

// headScriptServer answers HEADs from a script.
func headScriptServer(t *testing.T, answer func(attempt int) (status int)) (*inverseSDKStore, *atomic.Int64) {
	t.Helper()
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodHead {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Length", "0")
		w.WriteHeader(answer(int(requests.Add(1))))
	}))
	t.Cleanup(server.Close)
	client, err := newInverseSDKStore(context.Background(), r2.Config{Bucket: "stoarama", Endpoint: server.URL, AccessKey: "test", SecretKey: "test", Region: "auto"})
	if err != nil {
		t.Fatal(err)
	}
	return client, &requests
}

// Every server-side status 500-599 plus 429 is transient for a confirming HEAD,
// including a proxy's 520; nothing else is.
func TestR2InverseConfirmRetriesEvery5xxAnd429(t *testing.T) {
	for _, status := range []int{429, 500, 501, 502, 503, 504, 520, 522, 527, 599} {
		if inverseConfirmRetryable.IsErrorRetryable(&httpStatusError{status}) != aws.TrueTernary {
			t.Fatalf("%d is not transient for a confirming HEAD", status)
		}
	}
	for _, status := range []int{400, 403, 404, 409, 412, 418, 601} {
		if inverseConfirmRetryable.IsErrorRetryable(&httpStatusError{status}) == aws.TrueTernary {
			t.Fatalf("%d is transient for a confirming HEAD", status)
		}
	}
}

// A 520 is a transient provider failure, not a stop: the next attempt's 404
// confirms the DELETE. The SDK alone would stop at once, as 520 is outside its
// 500/502/503/504 default.
func TestR2InverseConfirm520Then404(t *testing.T) {
	sdk, requests := headScriptServer(t, func(attempt int) int {
		if attempt == 1 {
			return 520
		}
		return http.StatusNotFound
	})
	if err := sdk.confirmAbsent(context.Background(), "managed/acct-47/ordinary/0001.mp4"); err != nil {
		t.Fatal(err)
	}
	if requests.Load() != 2 {
		t.Fatalf("%d HEADs, want 520 then 404", requests.Load())
	}
}

// httpStatusError carries only a status code, as the SDK's HTTP response error
// interface does.
type httpStatusError struct{ status int }

func (e *httpStatusError) Error() string       { return "status " + strconv.Itoa(e.status) }
func (e *httpStatusError) HTTPStatusCode() int { return e.status }
