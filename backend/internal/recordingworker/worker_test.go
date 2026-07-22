package recordingworker

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/apihttp"
	"github.com/daydemir/stoarama/backend/internal/recordingapi"
)

// TestContinuousShouldStop locks the supervisor loop's stop-vs-reconnect decision.
// The load-bearing case is (canceled=false, windowClosed=false): CaptureContinuous
// returns nil on a premature clean ffmpeg exit (HLS end-of-stream), and the loop
// MUST reconnect rather than Complete the job with hours left in the window.
func TestContinuousShouldStop(t *testing.T) {
	cases := []struct {
		name         string
		canceled     bool
		windowClosed bool
		wantStop     bool
	}{
		{name: "premature drop mid-window reconnects", canceled: false, windowClosed: false, wantStop: false},
		{name: "window closed stops", canceled: false, windowClosed: true, wantStop: true},
		{name: "canceled stops", canceled: true, windowClosed: false, wantStop: true},
		{name: "canceled and window closed stops", canceled: true, windowClosed: true, wantStop: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := continuousShouldStop(tc.canceled, tc.windowClosed); got != tc.wantStop {
				t.Fatalf("continuousShouldStop(%v, %v) = %v, want %v", tc.canceled, tc.windowClosed, got, tc.wantStop)
			}
		})
	}
}

// TestIsAlreadyIngested covers the 409 dedup detection used by the per-segment
// ingest path so a re-leased window stays idempotent.
func TestIsAlreadyIngested(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "status 409", err: errString("ingest failed status=409"), want: true},
		{name: "object key message", err: errString("a clip already exists for this object key"), want: true},
		{name: "other error", err: errString("status=500 internal"), want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isAlreadyIngested(tc.err); got != tc.want {
				t.Fatalf("isAlreadyIngested(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// TestReconnectBackoff pins the bounded, deterministic, per-job jitter used to
// avoid synchronized reconnects against a shared origin.
func TestReconnectBackoff(t *testing.T) {
	cases := []struct {
		failures int
		min      time.Duration
		max      time.Duration
	}{
		{failures: 1, min: time.Second, max: 2 * time.Second},
		{failures: 2, min: 2 * time.Second, max: 4 * time.Second},
		{failures: 3, min: 4 * time.Second, max: 8 * time.Second},
		{failures: 6, min: 32 * time.Second, max: 64 * time.Second},
		{failures: 9, min: 150 * time.Second, max: 5 * time.Minute},
		{failures: 100, min: 150 * time.Second, max: 5 * time.Minute},
	}
	for _, tc := range cases {
		got := reconnectBackoff(145539, tc.failures)
		if got < tc.min || got > tc.max {
			t.Fatalf("reconnectBackoff(%d) = %s, want %s..%s", tc.failures, got, tc.min, tc.max)
		}
		if again := reconnectBackoff(145539, tc.failures); again != got {
			t.Fatalf("reconnectBackoff is not deterministic: %s then %s", got, again)
		}
	}
	if reconnectBackoff(145539, 1) == reconnectBackoff(145540, 1) {
		t.Fatal("different jobs received identical first reconnect delay")
	}
}

func TestRetryableUploadError(t *testing.T) {
	tests := []struct {
		err  error
		want bool
	}{
		{err: &apihttp.StatusError{Code: 502}, want: true},
		{err: &apihttp.StatusError{Code: 429}, want: true},
		{err: &apihttp.StatusError{Code: 403}, want: false},
		{err: &net.DNSError{Err: "temporary", IsTemporary: true}, want: true},
		{err: context.DeadlineExceeded, want: true},
	}
	for _, tc := range tests {
		if got := retryableUploadError(context.Background(), tc.err); got != tc.want {
			t.Errorf("retryableUploadError(%v)=%v want %v", tc.err, got, tc.want)
		}
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if retryableUploadError(canceled, &apihttp.StatusError{Code: 502}) {
		t.Fatal("canceled upload is retryable")
	}
}

func TestUploadWithRetry(t *testing.T) {
	attempts := 0
	err := uploadWithRetry(context.Background(), 1, func(context.Context) error {
		attempts++
		if attempts < 3 {
			return &apihttp.StatusError{Code: 502}
		}
		return nil
	}, func(error, time.Duration) {})
	if err != nil || attempts != 3 {
		t.Fatalf("err=%v attempts=%d", err, attempts)
	}

	attempts = 0
	err = uploadWithRetry(context.Background(), 1, func(context.Context) error {
		attempts++
		return fmt.Errorf("permanent")
	}, func(error, time.Duration) {})
	if err == nil || attempts != 1 {
		t.Fatalf("permanent err=%v attempts=%d", err, attempts)
	}

	attempts = 0
	canceled, cancel := context.WithCancel(context.Background())
	err = uploadWithRetry(canceled, 1, func(context.Context) error {
		attempts++
		return &apihttp.StatusError{Code: 502}
	}, func(error, time.Duration) { cancel() })
	if !errors.Is(err, context.Canceled) || attempts != 1 {
		t.Fatalf("canceled err=%v attempts=%d", err, attempts)
	}
}

func TestRelayDiagnosticsSnapshotRedactsURLs(t *testing.T) {
	d := &RelayDiagnostics{}
	for i := int64(6); i > 0; i-- {
		d.Start(recordingapi.RecordingJob{JobID: i, RecordingID: i + 100})
	}
	d.Stage(3, "capturing")
	d.Error(3, "resolve_retry", errString("HTTP 404 https://example.com/live.m3u8?token=secret"))

	snap := d.Snapshot()
	active := snap["active"].([]map[string]any)
	if len(active) != 6 {
		t.Fatalf("active count=%d want 6", len(active))
	}
	for i, job := range active {
		if job["job_id"] != int64(i+1) {
			t.Fatalf("active[%d] job_id=%v want %d", i, job["job_id"], i+1)
		}
	}
	if got := active[2]["last_error"]; got != "HTTP 404 https://example.com/live.m3u8?[query]" {
		t.Fatalf("last_error=%q want url with redacted query", got)
	}

	segAt := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	d.Segment(3, segAt)
	d.Finish(3, "done", nil)
	snap = d.Snapshot()
	active = snap["active"].([]map[string]any)
	if len(active) != 5 {
		t.Fatalf("active count=%d want 5 after finish", len(active))
	}
	for _, job := range active {
		if job["job_id"] == int64(3) {
			t.Fatal("finished job 3 remained active")
		}
	}
	last := snap["last"].(map[string]any)
	if last["job_id"] != int64(3) {
		t.Fatalf("last job_id=%v want 3", last["job_id"])
	}
	if last["segment_count"] != 1 {
		t.Fatalf("last segment_count=%v want 1", last["segment_count"])
	}
	if last["last_segment_at"] != segAt.Format(time.RFC3339Nano) {
		t.Fatalf("last_segment_at=%v want %s", last["last_segment_at"], segAt.Format(time.RFC3339Nano))
	}
}

func TestRelayDiagnosticsSnapshotBoundsActiveJobs(t *testing.T) {
	d := &RelayDiagnostics{}
	for i := int64(1); i <= relayDiagnosticActiveLimit+1; i++ {
		d.Start(recordingapi.RecordingJob{JobID: i, RecordingID: i})
	}

	snap := d.Snapshot()
	active := snap["active"].([]map[string]any)
	if len(active) != relayDiagnosticActiveLimit {
		t.Fatalf("active count=%d want %d", len(active), relayDiagnosticActiveLimit)
	}
	for i, job := range active {
		if job["job_id"] != int64(i+1) {
			t.Fatalf("active[%d] job_id=%v want %d", i, job["job_id"], i+1)
		}
	}
	if snap["active_total"] != relayDiagnosticActiveLimit+1 {
		t.Fatalf("active_total=%v want %d", snap["active_total"], relayDiagnosticActiveLimit+1)
	}
}

func TestSanitizeDiagnosticURLCollapsesSignedGoogleVideoPath(t *testing.T) {
	got := sanitizeDiagnosticError(errString("open https://rr4---sn.example.googlevideo.com/api/manifest/hls_playlist/expire/123/sig/secret/playlist/index.m3u8?token=abc"))
	want := "open https://rr4---sn.example.googlevideo.com/.../index.m3u8?[query]"
	if got != want {
		t.Fatalf("sanitized=%q want %q", got, want)
	}
}

type errString string

func (e errString) Error() string { return string(e) }
