package recordingworker

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
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

func TestContinuousDeliveryExhaustionAfterWindowCloseDoesNotFailJob(t *testing.T) {
	tests := []struct {
		name         string
		err          error
		windowClosed bool
		wantFail     bool
	}{
		{name: "exhausted while open fails", err: errSegmentDeliveryExhausted, wantFail: true},
		{name: "exhausted after close is delivery incomplete", err: errSegmentDeliveryExhausted, windowClosed: true, wantFail: false},
		{name: "permanent after close still fails", err: errPermanentSegmentDelivery, windowClosed: true, wantFail: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := continuousDeliveryFailureShouldFail(test.err, test.windowClosed); got != test.wantFail {
				t.Fatalf("continuousDeliveryFailureShouldFail(%v, %v)=%v want %v", test.err, test.windowClosed, got, test.wantFail)
			}
		})
	}
}

func TestContinuousNoProgressExpired(t *testing.T) {
	started := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	timeout := 5 * time.Minute
	if continuousNoProgressExpired(started, started.Add(timeout-time.Nanosecond), timeout) {
		t.Fatal("no-progress window expired early")
	}
	if !continuousNoProgressExpired(started, started.Add(timeout), timeout) {
		t.Fatal("no-progress window did not expire at boundary")
	}
	if continuousNoProgressExpired(started, started.Add(time.Hour), 0) {
		t.Fatal("zero timeout must keep cloud retry behavior")
	}
	if got := continuousReconnectDelay(started, started.Add(4*time.Minute), timeout, 5*time.Minute); got != time.Minute {
		t.Fatalf("bounded reconnect delay=%s want 1m", got)
	}
	if got := continuousReconnectDelay(started, started.Add(time.Hour), 0, 5*time.Minute); got != 5*time.Minute {
		t.Fatalf("cloud reconnect delay=%s want 5m", got)
	}
}

func TestDiskHasSpaceFailsClosed(t *testing.T) {
	worker := &Worker{cfg: Config{
		DiskFreeBytes: func() (uint64, error) { return 9, nil },
	}}
	if worker.diskHasSpace(10) {
		t.Fatal("insufficient disk accepted")
	}
	worker.cfg.DiskFreeBytes = func() (uint64, error) { return 10, nil }
	if !worker.diskHasSpace(10) {
		t.Fatal("boundary disk rejected")
	}
	worker.cfg.DiskFreeBytes = func() (uint64, error) { return 0, errors.New("stat failed") }
	if worker.diskHasSpace(10) {
		t.Fatal("failed disk check accepted")
	}
}

func TestDiskPauseDiagnosticIsRateLimited(t *testing.T) {
	worker := &Worker{cfg: Config{MinLeaseFreeBytes: 10}}
	started := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	worker.logDiskPause(started)
	if !worker.lastDiskPauseLog.Equal(started) {
		t.Fatalf("first disk pause log time=%s", worker.lastDiskPauseLog)
	}
	worker.logDiskPause(started.Add(diskPauseLogInterval - time.Second))
	if !worker.lastDiskPauseLog.Equal(started) {
		t.Fatalf("rate-limited disk pause advanced log time=%s", worker.lastDiskPauseLog)
	}
	next := started.Add(diskPauseLogInterval)
	worker.logDiskPause(next)
	if !worker.lastDiskPauseLog.Equal(next) {
		t.Fatalf("disk pause did not log at interval: %s", worker.lastDiskPauseLog)
	}
}

func TestDiskCheckErrorDiagnosticIsConcurrentAndRateLimited(t *testing.T) {
	worker := &Worker{}
	started := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	var logged atomic.Int64
	var calls sync.WaitGroup
	for range 20 {
		calls.Add(1)
		go func() {
			defer calls.Done()
			if worker.shouldLogDiskError(started) {
				logged.Add(1)
			}
		}()
	}
	calls.Wait()
	if logged.Load() != 1 {
		t.Fatalf("concurrent disk error logs=%d want 1", logged.Load())
	}
	if worker.shouldLogDiskError(started.Add(diskErrorLogInterval - time.Second)) {
		t.Fatal("disk error logged before interval")
	}
	if !worker.shouldLogDiskError(started.Add(diskErrorLogInterval)) {
		t.Fatal("disk error did not log at interval")
	}
}

func TestHeartbeatStopsAtConfirmedLeaseBoundary(t *testing.T) {
	t.Run("errors cannot extend lease", func(t *testing.T) {
		worker, closeServer := heartbeatTestWorker(t, http.StatusInternalServerError, time.Time{})
		defer closeServer()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		canceled := worker.startHeartbeat(ctx, cancel, 1, time.Now().Add(80*time.Millisecond))
		waitCanceled(t, canceled)
	})

	t.Run("renewal resets boundary", func(t *testing.T) {
		worker, closeServer := heartbeatTestWorker(t, http.StatusOK, time.Now().Add(time.Second))
		defer closeServer()
		ctx, cancel := context.WithCancel(context.Background())
		canceled := worker.startHeartbeat(ctx, cancel, 2, time.Now().Add(50*time.Millisecond))
		time.Sleep(100 * time.Millisecond)
		if canceled() {
			t.Fatal("worker canceled despite confirmed renewal")
		}
		cancel()
	})

	t.Run("conflict cancels immediately", func(t *testing.T) {
		worker, closeServer := heartbeatTestWorker(t, http.StatusConflict, time.Time{})
		defer closeServer()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		canceled := worker.startHeartbeat(ctx, cancel, 3, time.Now().Add(time.Second))
		waitCanceled(t, canceled)
	})
}

func heartbeatTestWorker(t *testing.T, status int, leaseExpiresAt time.Time) (*Worker, func()) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		if status == http.StatusOK {
			_, _ = fmt.Fprintf(w, `{"cancel":false,"lease_expires_at":%q}`, leaseExpiresAt.Format(time.RFC3339Nano))
		}
	}))
	client, err := recordingapi.NewClient(recordingapi.ClientConfig{BaseURL: server.URL, NodeToken: "test"})
	if err != nil {
		server.Close()
		t.Fatalf("new client: %v", err)
	}
	return &Worker{cfg: Config{Client: client}, heartbeatInt: 10 * time.Millisecond}, server.Close
}

func waitCanceled(t *testing.T, canceled func() bool) {
	t.Helper()
	deadline := time.Now().Add(500 * time.Millisecond)
	for !canceled() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if !canceled() {
		t.Fatal("worker did not cancel")
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

func TestRetryableTransportError(t *testing.T) {
	tests := []struct {
		err  error
		want bool
	}{
		{err: &apihttp.StatusError{Code: 502}, want: true},
		{err: &apihttp.StatusError{Code: 429}, want: true},
		{err: &apihttp.StatusError{Code: 403}, want: false},
		{err: &net.DNSError{Err: "temporary", IsTemporary: true}, want: true},
		{err: &net.DNSError{Err: "permanent"}, want: false},
		{err: context.DeadlineExceeded, want: true},
	}
	for _, tc := range tests {
		if got := retryableTransportError(context.Background(), tc.err); got != tc.want {
			t.Errorf("retryableTransportError(%v)=%v want %v", tc.err, got, tc.want)
		}
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if retryableTransportError(canceled, &apihttp.StatusError{Code: 502}) {
		t.Fatal("canceled upload is retryable")
	}
}

func TestRetrySegmentDeliveryKeepsExactFileUntilAcknowledged(t *testing.T) {
	path := filepath.Join(t.TempDir(), "segment.mp4")
	if err := os.WriteFile(path, []byte("video"), 0o600); err != nil {
		t.Fatal(err)
	}
	attempts := 0
	err := retrySegmentDelivery(context.Background(), time.Millisecond, func() bool { return true }, func() error {
		attempts++
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("attempt %d lost segment: %v", attempts, err)
		}
		if attempts < 3 {
			return &apihttp.StatusError{Code: 503}
		}
		return os.Remove(path)
	}, func(error) {})
	if err != nil {
		t.Fatal(err)
	}
	if attempts != 3 {
		t.Fatalf("attempts=%d want 3", attempts)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("acknowledged segment still exists: %v", err)
	}
}

func TestRetrySegmentDeliveryStopsOnCancelOrPermanentFailure(t *testing.T) {
	for _, test := range []struct {
		name string
		ctx  func() context.Context
		err  error
		want error
	}{
		{
			name: "canceled",
			ctx: func() context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx
			},
			err:  &apihttp.StatusError{Code: 503},
			want: context.Canceled,
		},
		{
			name: "permanent",
			ctx:  context.Background,
			err:  &apihttp.StatusError{Code: 403},
			want: errPermanentSegmentDelivery,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "segment.mp4")
			if err := os.WriteFile(path, []byte("video"), 0o600); err != nil {
				t.Fatal(err)
			}
			attempts := 0
			err := retrySegmentDelivery(test.ctx(), time.Millisecond, func() bool { return true }, func() error {
				attempts++
				return test.err
			}, func(error) {})
			if !errors.Is(err, test.want) {
				t.Fatalf("error=%v want %v", err, test.want)
			}
			if test.name == "permanent" && attempts != 1 {
				t.Fatalf("attempts=%d want 1", attempts)
			}
			if _, err := os.Stat(path); err != nil {
				t.Fatalf("retry helper deleted segment: %v", err)
			}
		})
	}
}

func TestRetrySegmentDeliveryCancellationDuringAttemptIsNotPermanent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	attempts := 0
	err := retrySegmentDelivery(ctx, time.Hour, func() bool { return true }, func() error {
		attempts++
		cancel()
		return context.Canceled
	}, func(error) {})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v want canceled", err)
	}
	if errors.Is(err, errPermanentSegmentDelivery) {
		t.Fatalf("cancellation classified permanent: %v", err)
	}
	if attempts != 1 {
		t.Fatalf("attempts=%d want 1", attempts)
	}
}

func TestRetrySegmentDeliveryStopsAtWindowDeadline(t *testing.T) {
	path := filepath.Join(t.TempDir(), "segment.mp4")
	if err := os.WriteFile(path, []byte("video"), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	started := time.Now()
	err := retrySegmentDelivery(ctx, time.Hour, func() bool { return true }, func() error {
		return &apihttp.StatusError{Code: 503}
	}, func(error) {})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error=%v want deadline exceeded", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("delivery ignored window deadline for %s", elapsed)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("deadline deleted pending segment: %v", err)
	}
}

func TestFinalizedSegmentStartingBeforeWindowEndGetsDeliveryBudget(t *testing.T) {
	now := time.Now()
	windowEnd := now.Add(time.Millisecond)
	segmentCtx, cancel := continuousSegmentDeliveryContext(context.Background(), &windowEnd, now)
	defer cancel()
	deadline, ok := segmentCtx.Deadline()
	if !ok || deadline.Sub(now) < segmentDeliveryRetryBudget-time.Second {
		t.Fatalf("delivery deadline=%s want approximately %s", deadline.Sub(now), segmentDeliveryRetryBudget)
	}
	attempts := 0
	err := retrySegmentDelivery(
		segmentCtx,
		time.Millisecond,
		func() bool { return true },
		func() error {
			attempts++
			return nil
		},
		func(error) {},
	)
	if err != nil {
		t.Fatal(err)
	}
	if attempts != 1 {
		t.Fatalf("attempts=%d want 1", attempts)
	}
}

func TestSegmentDeliveryBudgetIsBoundedEarlyAndAfterWindow(t *testing.T) {
	now := time.Now()
	longWindowEnd := now.Add(12 * time.Hour)
	earlyCtx, earlyCancel := continuousSegmentDeliveryContext(context.Background(), &longWindowEnd, now)
	defer earlyCancel()
	earlyDeadline, _ := earlyCtx.Deadline()
	if got := earlyDeadline.Sub(now); got != segmentDeliveryRetryBudget {
		t.Fatalf("early delivery budget=%s want %s", got, segmentDeliveryRetryBudget)
	}

	closedWindowEnd := now.Add(-time.Minute)
	finalCtx, finalCancel := continuousSegmentDeliveryContext(context.Background(), &closedWindowEnd, now)
	defer finalCancel()
	finalDeadline, _ := finalCtx.Deadline()
	if got := finalDeadline.Sub(now); got != postWindowDeliveryGrace-time.Minute {
		t.Fatalf("post-window delivery budget=%s want %s", got, postWindowDeliveryGrace-time.Minute)
	}
}

func TestContinuousAttemptCleanupDecision(t *testing.T) {
	tests := []struct {
		name    string
		pending bool
		err     error
		want    bool
	}{
		{name: "acknowledged", pending: false, want: true},
		{name: "source drop after acknowledgements", pending: false, err: errors.New("ffmpeg failed"), want: true},
		{name: "canceled with pending bytes", pending: true, err: context.Canceled, want: true},
		{name: "disk pressure", pending: true, err: errDiskPressure, want: true},
		{name: "permanent delivery failure", pending: true, err: errPermanentSegmentDelivery, want: true},
		{name: "delivery budget exhausted", pending: true, err: errSegmentDeliveryExhausted, want: true},
		{name: "transient unclean failure", pending: true, err: errors.New("connection reset"), want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := shouldCleanupContinuousAttempt(test.pending, test.err); got != test.want {
				t.Fatalf("cleanup=%v want %v", got, test.want)
			}
		})
	}
}

func TestEnsureCaptureTempDirRecreatesMissingRoot(t *testing.T) {
	root := filepath.Join(t.TempDir(), "capture")
	worker := &Worker{cfg: Config{CaptureTempDir: root}}
	if err := worker.ensureCaptureTempDir(); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(root)
	if err != nil {
		t.Fatal(err)
	}
	if !info.IsDir() {
		t.Fatalf("%s is not a directory", root)
	}
	if err := os.RemoveAll(root); err != nil {
		t.Fatal(err)
	}
	if err := worker.ensureCaptureTempDir(); err != nil {
		t.Fatalf("recreate: %v", err)
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
