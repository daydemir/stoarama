package recordingworker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/apihttp"
	"github.com/daydemir/stoarama/backend/internal/capture"
	"github.com/daydemir/stoarama/backend/internal/recordingapi"
)

func TestContinuousCaptureForJobRoutesCanaryExplicitly(t *testing.T) {
	legacy := continuousCaptureForJob(recordingapi.RecordingJob{})
	if reflect.ValueOf(legacy).Pointer() != reflect.ValueOf(capture.CaptureContinuousWithHeaders).Pointer() {
		t.Fatal("default job did not select the legacy continuous capture path")
	}
	canary := continuousCaptureForJob(recordingapi.RecordingJob{TimestampContractSupported: true})
	if reflect.ValueOf(canary).Pointer() != reflect.ValueOf(capture.CaptureContinuousWithTimestampContract).Pointer() {
		t.Fatal("eligible job did not select the timestamp-contract capture path")
	}
}

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

func TestCloudWorkerAllowsNoProgressHandoffWithoutRelayDiagnostics(t *testing.T) {
	var gotReason recordingapi.SurrenderReason
	var gotErrorText string
	var observationsMu sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/recording/jobs/42/surrender" {
			t.Errorf("path = %q", r.URL.Path)
		}
		var body struct {
			Reason    recordingapi.SurrenderReason `json:"reason"`
			ErrorText string                       `json:"error_text"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode surrender: %v", err)
		}
		observationsMu.Lock()
		gotReason = body.Reason
		gotErrorText = body.ErrorText
		observationsMu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()
	client, err := recordingapi.NewClient(recordingapi.ClientConfig{
		BaseURL:   server.URL,
		NodeToken: "test-node-token",
	})
	if err != nil {
		t.Fatal(err)
	}
	worker, err := NewWorker(Config{
		Client:                      client,
		ContinuousNoProgressTimeout: 5 * time.Minute,
	})
	if err != nil {
		t.Fatalf("NewWorker rejected cloud no-progress handoff: %v", err)
	}
	if worker.cfg.ContinuousNoProgressTimeout != 5*time.Minute {
		t.Fatalf("timeout = %s, want 5m", worker.cfg.ContinuousNoProgressTimeout)
	}
	_, cancel := context.WithCancel(context.Background())
	if !worker.surrenderContinuousJob(context.Background(), cancel, recordingapi.RecordingJob{
		JobID: 42, LeaseToken: "lease-token",
	}, time.Now().Add(-6*time.Minute), errors.New("skyline manifest contains no playable media segments")) {
		t.Fatal("expired no-progress timeout did not surrender job")
	}
	observationsMu.Lock()
	observedReason, observedErrorText := gotReason, gotErrorText
	observationsMu.Unlock()
	if observedReason != recordingapi.SurrenderNoProgress {
		t.Fatalf("surrender reason = %q, want %q", observedReason, recordingapi.SurrenderNoProgress)
	}
	if !strings.Contains(observedErrorText, "skyline manifest contains no playable media segments") {
		t.Fatalf("surrender error_text=%q", observedErrorText)
	}
}

func TestClosedWindowCompletesWhenSurrenderRacesBoundary(t *testing.T) {
	var completed atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/recording/jobs/42/surrender":
			http.Error(w, `{"error":"window closed"}`, http.StatusConflict)
		case "/api/v1/recording/jobs/42/complete":
			completed.Store(true)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"ok":true}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := recordingapi.NewClient(recordingapi.ClientConfig{BaseURL: server.URL, NodeToken: "test-node-token"})
	if err != nil {
		t.Fatal(err)
	}
	worker, err := NewWorker(Config{Client: client})
	if err != nil {
		t.Fatal(err)
	}
	windowEnd := time.Now().Add(-time.Second)
	_, cancel := context.WithCancel(context.Background())
	if !worker.surrenderContinuousJobForReason(context.Background(), cancel, recordingapi.RecordingJob{
		JobID: 42, LeaseToken: "lease-token", WindowEndAt: &windowEnd,
	}, recordingapi.SurrenderNoProgress, errors.New("no progress")) {
		t.Fatal("closed-window surrender did not terminate processing")
	}
	if !completed.Load() {
		t.Fatal("closed-window surrender conflict did not complete the owned job")
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

func TestSegmentDeliveryReusesReservedIntentAcrossUploadRetry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	var reserveCalls, uploadCalls, ingestCalls, retryCalls int
	var uploadIntentIDs []string
	err := deliverSegmentWithRetry(ctx, time.Millisecond, func() bool { return true }, segmentDeliveryOps{
		Reserve: func() (recordingapi.ClipUploadIntent, error) {
			reserveCalls++
			return recordingapi.ClipUploadIntent{IntentID: "intent-1", UploadURL: "https://upload.test", ExpiresAt: time.Now().Add(time.Hour)}, nil
		},
		Upload: func(intent recordingapi.ClipUploadIntent) error {
			uploadCalls++
			uploadIntentIDs = append(uploadIntentIDs, intent.IntentID)
			if uploadCalls == 1 {
				return &apihttp.StatusError{Label: "upload", Code: http.StatusServiceUnavailable}
			}
			return nil
		},
		Ingest: func(intent recordingapi.ClipUploadIntent) error {
			ingestCalls++
			if intent.IntentID != "intent-1" {
				t.Fatalf("ingest intent=%q want intent-1", intent.IntentID)
			}
			return nil
		},
	}, func(error) {
		retryCalls++
	})
	if err != nil {
		t.Fatal(err)
	}
	if reserveCalls != 1 || uploadCalls != 2 || ingestCalls != 1 || retryCalls != 1 {
		t.Fatalf("reserve=%d upload=%d ingest=%d retries=%d", reserveCalls, uploadCalls, ingestCalls, retryCalls)
	}
	if len(uploadIntentIDs) != 2 || uploadIntentIDs[0] != uploadIntentIDs[1] {
		t.Fatalf("upload intent ids=%v want one stable intent", uploadIntentIDs)
	}
}

func TestSegmentDeliveryRefreshesExpiringUploadIntent(t *testing.T) {
	var reserveCalls, uploadCalls, ingestCalls, retryCalls int
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := deliverSegmentWithRetry(ctx, time.Millisecond, func() bool { return true }, segmentDeliveryOps{
		Reserve: func() (recordingapi.ClipUploadIntent, error) {
			reserveCalls++
			if reserveCalls == 1 {
				return recordingapi.ClipUploadIntent{IntentID: "intent-1", UploadURL: "https://expired.test", ExpiresAt: time.Now().Add(30 * time.Second)}, nil
			}
			return recordingapi.ClipUploadIntent{IntentID: "intent-1", UploadURL: "https://fresh.test", ExpiresAt: time.Now().Add(time.Hour)}, nil
		},
		Upload: func(intent recordingapi.ClipUploadIntent) error {
			uploadCalls++
			if intent.UploadURL != "https://fresh.test" {
				t.Fatalf("uploaded with stale URL %q", intent.UploadURL)
			}
			return nil
		},
		Ingest: func(recordingapi.ClipUploadIntent) error {
			ingestCalls++
			return nil
		},
	}, func(error) { retryCalls++ })
	if err != nil {
		t.Fatal(err)
	}
	if reserveCalls != 2 || uploadCalls != 1 || ingestCalls != 1 || retryCalls != 1 {
		t.Fatalf("reserve=%d upload=%d ingest=%d retries=%d", reserveCalls, uploadCalls, ingestCalls, retryCalls)
	}
}

func TestSegmentDeliverySkipsConsumedReplay(t *testing.T) {
	var uploadCalls, ingestCalls, replayCalls int
	err := deliverSegmentWithRetry(context.Background(), time.Millisecond, func() bool { return true }, segmentDeliveryOps{
		Reserve: func() (recordingapi.ClipUploadIntent, error) {
			return recordingapi.ClipUploadIntent{IntentID: "intent-1", AlreadyIngested: true}, nil
		},
		Upload: func(recordingapi.ClipUploadIntent) error {
			uploadCalls++
			return nil
		},
		Ingest: func(recordingapi.ClipUploadIntent) error {
			ingestCalls++
			return nil
		},
		AlreadyIngested: func() { replayCalls++ },
	}, func(error) {})
	if err != nil {
		t.Fatal(err)
	}
	if uploadCalls != 0 || ingestCalls != 0 || replayCalls != 1 {
		t.Fatalf("consumed replay uploaded=%d ingested=%d replay=%d", uploadCalls, ingestCalls, replayCalls)
	}
}

func TestClipDeliverySkipsConsumedReplay(t *testing.T) {
	var uploadCalls, ingestCalls int
	err := deliverReservedClip(
		recordingapi.ClipUploadIntent{IntentID: "intent-1", AlreadyIngested: true},
		func() error { uploadCalls++; return nil },
		func() error { ingestCalls++; return nil },
	)
	if err != nil {
		t.Fatal(err)
	}
	if uploadCalls != 0 || ingestCalls != 0 {
		t.Fatalf("consumed replay uploaded=%d ingested=%d", uploadCalls, ingestCalls)
	}
}

func TestSegmentDeliveryRereservesAfterCommittedIngestResponseLoss(t *testing.T) {
	var reserveCalls, uploadCalls, ingestCalls, replayCalls int
	err := deliverSegmentWithRetry(context.Background(), time.Millisecond, func() bool { return true }, segmentDeliveryOps{
		Reserve: func() (recordingapi.ClipUploadIntent, error) {
			reserveCalls++
			if reserveCalls == 2 {
				return recordingapi.ClipUploadIntent{IntentID: "intent-1", AlreadyIngested: true}, nil
			}
			return recordingapi.ClipUploadIntent{IntentID: "intent-1", UploadURL: "https://upload.test"}, nil
		},
		Upload: func(recordingapi.ClipUploadIntent) error {
			uploadCalls++
			return nil
		},
		Ingest: func(recordingapi.ClipUploadIntent) error {
			ingestCalls++
			return &apihttp.StatusError{
				Label: "ingest",
				Code:  http.StatusConflict,
				Body:  `{"code":"recording_upload_intent_unavailable","error":"upload intent not found, already consumed, or job not owned"}`,
			}
		},
		AlreadyIngested: func() { replayCalls++ },
	}, func(error) {})
	if err != nil {
		t.Fatal(err)
	}
	if reserveCalls != 2 || uploadCalls != 1 || ingestCalls != 1 || replayCalls != 1 {
		t.Fatalf("reserve=%d upload=%d ingest=%d replay=%d", reserveCalls, uploadCalls, ingestCalls, replayCalls)
	}
}

func TestSegmentDeliveryPoolReplayAcknowledgesWithoutUniqueProgress(t *testing.T) {
	pool := startSegmentDeliveryPool(1, nil, func(capture.Segment) error {
		return errSegmentReplayAcknowledged
	})
	if err := pool.Submit(capture.Segment{SHA256: strings.Repeat("a", 64)}); err != nil {
		t.Fatal(err)
	}
	result := pool.close()
	if result.err != nil || result.pending != 0 || result.submitted != 1 {
		t.Fatalf("replay result=%+v", result)
	}
	if result.ingested {
		t.Fatal("already-ingested replay reset the unique-media progress signal")
	}
}

func TestSegmentDeliveryPoolMixedReplayAndUniqueReportsUniqueProgress(t *testing.T) {
	pool := startSegmentDeliveryPool(1, nil, func(seg capture.Segment) error {
		if seg.SHA256 == strings.Repeat("a", 64) {
			return errSegmentReplayAcknowledged
		}
		return nil
	})
	for _, sha := range []string{strings.Repeat("a", 64), strings.Repeat("b", 64)} {
		if err := pool.Submit(capture.Segment{SHA256: sha}); err != nil {
			t.Fatal(err)
		}
	}
	result := pool.close()
	if result.err != nil || result.pending != 0 || result.submitted != 2 {
		t.Fatalf("mixed result=%+v", result)
	}
	if !result.ingested {
		t.Fatal("unique ingest was lost when the same attempt also acknowledged a replay")
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

func TestContinuousMediaLagExpired(t *testing.T) {
	now := time.Date(2026, time.August, 9, 2, 0, 0, 0, time.UTC)
	timeout := 15 * time.Minute
	for _, tc := range []struct {
		name     string
		mediaEnd time.Time
		now      time.Time
		timeout  time.Duration
		want     bool
	}{
		{name: "within bound", mediaEnd: now.Add(-timeout + time.Nanosecond), now: now, timeout: timeout},
		{name: "at bound", mediaEnd: now.Add(-timeout), now: now, timeout: timeout, want: true},
		{name: "well behind despite progress", mediaEnd: now.Add(-48 * time.Minute), now: now, timeout: timeout, want: true},
		{name: "disabled", mediaEnd: now.Add(-time.Hour), now: now, timeout: 0},
		{name: "unknown media end", now: now, timeout: timeout},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := continuousMediaLagExpired(tc.mediaEnd, tc.now, tc.timeout); got != tc.want {
				t.Fatalf("continuousMediaLagExpired()=%v want %v", got, tc.want)
			}
		})
	}
}

func TestRecordingCaptureTargetFPSAlwaysPreservesSource(t *testing.T) {
	legacy := 15
	if got := recordingCaptureTargetFPS(&legacy); got != nil {
		t.Fatalf("legacy target selected re-encode path: %v", *got)
	}
	if got := recordingCaptureTargetFPS(nil); got != nil {
		t.Fatalf("native target=%v want nil", *got)
	}
}

func TestContinuousMediaLagSurrendersOnlyAfterCleanSpoolDrain(t *testing.T) {
	if !continuousMediaLagCanSurrender(true, nil, false) {
		t.Fatal("cleanly drained lagged capture did not surrender")
	}
	if continuousMediaLagCanSurrender(true, errors.New("upload failed"), false) {
		t.Fatal("lagged capture surrendered despite an unacknowledged spool")
	}
	if continuousMediaLagCanSurrender(false, nil, false) {
		t.Fatal("unrelated capture failure surrendered as lag")
	}
	if continuousMediaLagCanSurrender(true, nil, true) {
		t.Fatal("fully drained closed window was surrendered instead of completed")
	}
}

func TestContinuousSelfUpdateSurrendersOnlyAfterCleanSpoolDrain(t *testing.T) {
	if !continuousSelfUpdateCanSurrender(true, nil, false) {
		t.Fatal("cleanly drained update did not surrender")
	}
	if continuousSelfUpdateCanSurrender(true, errors.New("upload failed"), false) {
		t.Fatal("update surrendered with unacknowledged media")
	}
	if continuousSelfUpdateCanSurrender(false, nil, false) {
		t.Fatal("ordinary capture surrendered as an update")
	}
	if continuousSelfUpdateCanSurrender(true, nil, true) {
		t.Fatal("closed window surrendered instead of completing")
	}
}

func TestUpdateDrainRefusesNewLeases(t *testing.T) {
	var leaseCalls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		leaseCalls.Add(1)
		http.Error(w, "unexpected lease", http.StatusInternalServerError)
	}))
	defer server.Close()
	client, err := recordingapi.NewClient(recordingapi.ClientConfig{BaseURL: server.URL, NodeToken: "node-token"})
	if err != nil {
		t.Fatal(err)
	}
	var drain atomic.Bool
	drain.Store(true)
	worker, err := NewWorker(Config{Client: client, DrainForUpdate: &drain})
	if err != nil {
		t.Fatal(err)
	}
	sem := make(chan struct{}, 1)
	var wg sync.WaitGroup
	worker.drain(context.Background(), sem, &wg)
	if got := leaseCalls.Load(); got != 0 {
		t.Fatalf("lease calls=%d want 0", got)
	}
}

func TestUpdateDrainStopsStalledContinuousCapture(t *testing.T) {
	oldInterval := continuousUpdateDrainPollInterval
	continuousUpdateDrainPollInterval = time.Millisecond
	t.Cleanup(func() { continuousUpdateDrainPollInterval = oldInterval })
	var drain atomic.Bool
	var aborted atomic.Int64
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		monitorContinuousUpdateDrain(stop, &drain, func() { aborted.Add(1) })
	}()
	drain.Store(true)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("stalled capture was not stopped for update")
	}
	if got := aborted.Load(); got != 1 {
		t.Fatalf("aborts=%d want 1", got)
	}
}

func TestContinuousMediaLagStopsCaptureThenDrainsBeforeSurrender(t *testing.T) {
	now := time.Date(2026, time.August, 9, 2, 0, 0, 0, time.UTC)
	var delivered atomic.Int64
	var lagged atomic.Bool
	var aborted atomic.Int64
	pool := startSegmentDeliveryPool(2, func() {}, func(seg capture.Segment) error {
		observeContinuousMediaLag(seg.EndAt, now, 15*time.Minute, &lagged, func() { aborted.Add(1) })
		delivered.Add(1)
		return nil
	})
	for sequence := int64(1); sequence <= 5; sequence++ {
		if err := pool.Submit(capture.Segment{CaptureSequence: sequence, EndAt: now.Add(-48 * time.Minute)}); err != nil {
			t.Fatalf("submit %d: %v", sequence, err)
		}
	}
	result := pool.close()
	if result.err != nil || result.pending != 0 || delivered.Load() != 5 || !lagged.Load() || aborted.Load() != 1 {
		t.Fatalf("drain result=%+v delivered=%d lagged=%v aborts=%d, want five acknowledged and one stop", result, delivered.Load(), lagged.Load(), aborted.Load())
	}
	if !continuousMediaLagCanSurrender(lagged.Load(), result.err, false) {
		t.Fatal("fully drained lagged capture did not surrender")
	}

	var failedLagged atomic.Bool
	failedPool := startSegmentDeliveryPool(1, func() {}, func(seg capture.Segment) error {
		observeContinuousMediaLag(seg.EndAt, now, 15*time.Minute, &failedLagged, func() {})
		return errSegmentDeliveryExhausted
	})
	if err := failedPool.Submit(capture.Segment{CaptureSequence: 1, EndAt: now.Add(-48 * time.Minute)}); err != nil {
		t.Fatal(err)
	}
	failed := failedPool.close()
	joined := joinSegmentDeliveryError(context.Canceled, failed.err)
	if continuousMediaLagCanSurrender(failedLagged.Load(), failed.err, false) {
		t.Fatal("lagged capture surrendered with an unacknowledged segment")
	}
	if shouldCleanupContinuousAttempt(failed.pending > 0, joined, failedLagged.Load()) {
		t.Fatal("lagged capture deleted its unacknowledged spool")
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
		canceled := worker.startHeartbeat(ctx, cancel, 1, "", time.Now().Add(80*time.Millisecond))
		waitCanceled(t, canceled)
	})

	t.Run("renewal resets boundary", func(t *testing.T) {
		worker, closeServer := heartbeatTestWorker(t, http.StatusOK, time.Now().Add(time.Second))
		defer closeServer()
		ctx, cancel := context.WithCancel(context.Background())
		canceled := worker.startHeartbeat(ctx, cancel, 2, "", time.Now().Add(50*time.Millisecond))
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
		canceled := worker.startHeartbeat(ctx, cancel, 3, "", time.Now().Add(time.Second))
		waitCanceled(t, canceled)
	})
}

func TestHeartbeatContinuesAfterCaptureWindowCloses(t *testing.T) {
	var heartbeats atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		heartbeats.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"cancel":false,"lease_expires_at":%q}`, time.Now().Add(time.Second).Format(time.RFC3339Nano))
	}))
	defer server.Close()
	client, err := recordingapi.NewClient(recordingapi.ClientConfig{BaseURL: server.URL, NodeToken: "test"})
	if err != nil {
		t.Fatal(err)
	}
	worker := &Worker{cfg: Config{Client: client}, heartbeatInt: 5 * time.Millisecond, leaseSafetyMargin: time.Millisecond}
	jobCtx, cancelJob := context.WithCancel(context.Background())
	defer cancelJob()
	canceled := worker.startHeartbeat(jobCtx, cancelJob, 4, "", time.Now().Add(time.Second))
	windowCtx, closeWindow := context.WithCancel(jobCtx)
	deliveryStarted := make(chan struct{})
	releaseDelivery := make(chan struct{})
	pool := startSegmentDeliveryPool(1, nil, func(capture.Segment) error {
		close(deliveryStarted)
		<-releaseDelivery
		return nil
	})
	if err := pool.Submit(capture.Segment{Path: "post-window.mp4"}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-deliveryStarted:
	case <-time.After(time.Second):
		t.Fatal("delivery worker did not start")
	}
	drainDone := make(chan segmentDeliveryResult, 1)
	go func() { drainDone <- pool.close() }()

	// Match processContinuousJob: capture's child window closes while the pool
	// drains on the still-live parent job context.
	heartbeatsBeforeWindowClose := heartbeats.Load()
	closeWindow()
	if windowCtx.Err() == nil {
		t.Fatal("capture window did not close")
	}
	waitFor(t, "post-window lease renewals", func() bool { return heartbeats.Load() >= heartbeatsBeforeWindowClose+3 })
	if canceled() || jobCtx.Err() != nil {
		t.Fatal("window close canceled job heartbeat during delivery drain")
	}
	select {
	case <-drainDone:
		t.Fatal("delivery drain completed while upload was still blocked")
	default:
	}
	close(releaseDelivery)
	var result segmentDeliveryResult
	select {
	case result = <-drainDone:
	case <-time.After(time.Second):
		t.Fatal("post-window delivery drain did not finish after upload release")
	}
	if result.err != nil || result.pending != 0 {
		t.Fatalf("post-window delivery result=%+v", result)
	}
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
		{name: "unrelated status 409", err: errString("ingest failed status=409"), want: false},
		{name: "job not owned", err: errString("status=409 upload intent not found, already consumed, or job not owned"), want: false},
		{name: "object key message only", err: errString("a clip already exists for this object key"), want: false},
		{name: "typed already ingested", err: &apihttp.StatusError{
			Label: "ingest", Code: http.StatusConflict,
			Body: `{"code":"recording_clip_already_ingested","error":"a clip already exists for this object key"}`,
		}, want: true},
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
		{failures: 5, min: 15 * time.Second, max: 30 * time.Second},
		{failures: 6, min: 15 * time.Second, max: 30 * time.Second},
		{failures: 9, min: 15 * time.Second, max: 30 * time.Second},
		{failures: 100, min: 15 * time.Second, max: 30 * time.Second},
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

// TestContinuousReconnectRecoversWhenHLSReturns drives processContinuousJob's
// real resolve -> SSRF gate -> CaptureContinuous -> delivery -> complete path.
// The fake ffmpeg reports seven HLS 404s, then emits native-copy-shaped MP4
// segments and stays live until the window closes. Only retry time is scaled.
func TestContinuousReconnectRecoversWhenHLSReturns(t *testing.T) {
	realFFmpeg, err := exec.LookPath("ffmpeg")
	if err != nil {
		t.Skip("ffmpeg unavailable")
	}
	temp := t.TempDir()
	attemptsPath := filepath.Join(temp, "attempts")
	argsPath := filepath.Join(temp, "args")
	fakeFFmpeg := filepath.Join(temp, "ffmpeg")
	script := `#!/bin/sh
printf '%s\n' "$*" >> "$FFMPEG_ARGS"
n=0
test ! -f "$FFMPEG_ATTEMPTS" || n=$(cat "$FFMPEG_ATTEMPTS")
n=$((n+1))
printf '%s' "$n" > "$FFMPEG_ATTEMPTS"
if test "$n" -le 7; then
  echo 'Error opening input: Server returned 404 Not Found' >&2
  exit 8
fi
for last do :; done
out=${last%/*}
"$REAL_FFMPEG" -loglevel error -f lavfi -i color=c=red:s=64x64:d=0.3 -an -c:v libx264 -pix_fmt yuv420p -y "$out/seg-20260812-120000.mp4"
"$REAL_FFMPEG" -loglevel error -f lavfi -i color=c=blue:s=64x64:d=0.3 -an -c:v libx264 -pix_fmt yuv420p -y "$out/seg-20260812-120001.mp4"
trap 'exit 0' INT TERM
while :; do sleep .1; done
`
	if err := os.WriteFile(fakeFFmpeg, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FFMPEG_BIN", fakeFFmpeg)
	t.Setenv("REAL_FFMPEG", realFFmpeg)
	t.Setenv("FFMPEG_ATTEMPTS", attemptsPath)
	t.Setenv("FFMPEG_ARGS", argsPath)

	var ingested atomic.Int32
	var completed atomic.Bool
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/heartbeat"):
			_, _ = fmt.Fprintf(w, `{"lease_expires_at":%q}`, time.Now().Add(time.Minute).Format(time.RFC3339Nano))
		case r.URL.Path == "/api/v1/recording/upload-intents":
			_, _ = fmt.Fprintf(w, `{"intent_id":"intent-%d","upload_url":%q,"expires_at":%q}`, ingested.Load()+1, server.URL+"/upload", time.Now().Add(time.Hour).Format(time.RFC3339Nano))
		case r.URL.Path == "/upload":
			w.WriteHeader(http.StatusOK)
		case r.URL.Path == "/api/v1/recording/clips/ingest":
			id := ingested.Add(1)
			_, _ = fmt.Fprintf(w, `{"clip_id":%d}`, id)
		case strings.HasSuffix(r.URL.Path, "/complete"):
			completed.Store(true)
			_, _ = w.Write([]byte(`{"ok":true}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := recordingapi.NewClient(recordingapi.ClientConfig{BaseURL: server.URL, NodeToken: "test"})
	if err != nil {
		t.Fatal(err)
	}
	worker, err := NewWorker(Config{Client: client, HeartbeatSec: 1, CaptureTempDir: temp, UploadWorkers: 1})
	if err != nil {
		t.Fatal(err)
	}
	worker.reconnectDelay = func(jobID int64, failures int) time.Duration {
		return reconnectBackoffFor(jobID, failures, time.Millisecond, 10*time.Millisecond)
	}
	windowEnd := time.Now().Add(6 * time.Second)
	worker.processContinuousJob(context.Background(), recordingapi.RecordingJob{
		JobID: 438, RecordingID: 438, SourceURL: "https://example.com/live.m3u8",
		ClipDurationSec: 1, LeaseExpiresAt: time.Now().Add(time.Minute), LeaseToken: "lease",
		Kind: "continuous_window", WindowEndAt: &windowEnd,
	})
	attemptData, err := os.ReadFile(attemptsPath)
	if err != nil {
		t.Fatal(err)
	}
	attempts, err := strconv.Atoi(strings.TrimSpace(string(attemptData)))
	if err != nil {
		t.Fatalf("parse ffmpeg attempts: %v", err)
	}
	if attempts < 8 {
		t.Fatalf("ffmpeg attempts=%d want at least 8", attempts)
	}
	argsData, err := os.ReadFile(argsPath)
	if err != nil {
		t.Fatal(err)
	}
	if args := string(argsData); !strings.Contains(args, " -c copy ") || strings.Contains(args, "libx264") {
		t.Fatalf("production capture did not stay on native stream-copy args: %q", args)
	}
	if ingested.Load() == 0 || !completed.Load() {
		t.Fatalf("ingested=%d completed=%v, want recovered media and completed window", ingested.Load(), completed.Load())
	}
}

func TestContinuousExpiredGooglevideoRetriesSameManifestOnceBeforeResolve(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/heartbeat") {
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprintf(w, `{"cancel":false,"lease_expires_at":%q}`, time.Now().Add(time.Minute).Format(time.RFC3339Nano))
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()
	client, err := recordingapi.NewClient(recordingapi.ClientConfig{BaseURL: server.URL, NodeToken: "test"})
	if err != nil {
		t.Fatal(err)
	}
	worker, err := NewWorker(Config{Client: client, HeartbeatSec: 1, CaptureTempDir: t.TempDir(), UploadWorkers: 1})
	if err != nil {
		t.Fatal(err)
	}
	worker.heartbeatInt = 5 * time.Millisecond
	worker.leaseSafetyMargin = time.Millisecond
	worker.reconnectDelay = func(int64, int) time.Duration { return time.Hour }

	var resolves atomic.Int64
	worker.resolveCaptureInput = func(context.Context, string, string, string) (string, bool, string, error) {
		if resolves.Add(1) == 1 {
			return "https://192.0.2.1/first.m3u8", false, "first-headers", nil
		}
		return "https://192.0.2.2/fresh.m3u8", false, "fresh-headers", nil
	}
	type captureCall struct {
		url, headers string
	}
	captures := make(chan captureCall, 3)
	var captureCount atomic.Int64
	worker.continuousCapture = func(ctx context.Context, sourceURL string, _ time.Duration, _ string, _ *int, _ string, _ func(capture.Segment) error, headers string) error {
		captures <- captureCall{url: sourceURL, headers: headers}
		if captureCount.Add(1) <= 2 {
			return capture.ErrContinuousExpiredGooglevideoFragment
		}
		<-ctx.Done()
		return ctx.Err()
	}

	jobCtx, cancelJob := context.WithCancel(context.Background())
	done := make(chan struct{})
	windowEnd := time.Now().Add(time.Minute)
	go func() {
		defer close(done)
		worker.processContinuousJob(jobCtx, recordingapi.RecordingJob{
			JobID: 450, RecordingID: 450, SourceURL: "https://192.0.2.10/watch",
			ClipDurationSec: 60, LeaseExpiresAt: time.Now().Add(time.Minute), LeaseToken: "lease",
			Kind: "continuous_window", WindowEndAt: &windowEnd,
		})
	}()

	want := []captureCall{
		{url: "https://192.0.2.1/first.m3u8", headers: "first-headers"},
		{url: "https://192.0.2.1/first.m3u8", headers: "first-headers"},
		{url: "https://192.0.2.2/fresh.m3u8", headers: "fresh-headers"},
	}
	for i, expected := range want {
		select {
		case got := <-captures:
			if got != expected {
				t.Fatalf("capture %d=%+v want %+v", i+1, got, expected)
			}
		case <-time.After(750 * time.Millisecond):
			t.Fatalf("capture %d did not start without generic backoff; resolves=%d", i+1, resolves.Load())
		}
	}
	cancelJob()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("continuous job did not stop after cancellation")
	}
	if resolves.Load() != 2 {
		t.Fatalf("resolver calls=%d want 2", resolves.Load())
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
		// R2 PUT failures reach the worker as a write syscall error. ECONNRESET is
		// transient even though os.SyscallError itself does not implement
		// net.Error, and aborting the capture here creates an avoidable media gap.
		{err: fmt.Errorf("upload file failed: %w", &os.SyscallError{Syscall: "write", Err: syscall.ECONNRESET}), want: true},
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

func TestFinalizedSegmentStartingBeforeWindowEndGetsPostWindowGrace(t *testing.T) {
	now := time.Now()
	windowEnd := now.Add(time.Millisecond)
	segmentCtx, cancel := continuousSegmentDeliveryContext(context.Background(), &windowEnd, now)
	defer cancel()
	deadline, ok := segmentCtx.Deadline()
	if !ok || deadline.Sub(now) < postWindowDeliveryGrace-time.Second || deadline.Sub(now) > postWindowDeliveryGrace+time.Second {
		t.Fatalf("delivery deadline=%s want approximately %s", deadline.Sub(now), postWindowDeliveryGrace)
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
	if segmentDeliveryRetryBudget < 45*time.Minute || segmentDeliveryRetryBudget > time.Hour {
		t.Fatalf("delivery budget=%s must cover the observed 30m outage without becoming unbounded", segmentDeliveryRetryBudget)
	}
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

func TestContinuousDiskMonitorAbortsAtFence(t *testing.T) {
	worker := &Worker{cfg: Config{
		MinActiveFreeBytes: 100,
		DiskFreeBytes:      func() (uint64, error) { return 99, nil },
	}}
	stop := make(chan struct{})
	var pressured atomic.Bool
	aborted := make(chan struct{})
	go worker.monitorContinuousDiskAtInterval(stop, &pressured, func() { close(aborted) }, time.Millisecond)
	select {
	case <-aborted:
	case <-time.After(time.Second):
		t.Fatal("disk monitor did not abort capture")
	}
	if !pressured.Load() {
		t.Fatal("disk monitor aborted without recording pressure")
	}
}

func TestContinuousAttemptCleanupDecision(t *testing.T) {
	tests := []struct {
		name    string
		pending bool
		err     error
		lagged  bool
		want    bool
	}{
		{name: "acknowledged", pending: false, want: true},
		{name: "source drop after acknowledgements", pending: false, err: errors.New("ffmpeg failed"), want: true},
		{name: "canceled with pending bytes", pending: true, err: context.Canceled, want: true},
		{name: "disk pressure", pending: true, err: errDiskPressure, want: true},
		{name: "permanent delivery failure", pending: true, err: errPermanentSegmentDelivery, want: true},
		{name: "delivery budget exhausted", pending: true, err: errSegmentDeliveryExhausted, want: true},
		{name: "lag plus delivery failure preserves spool", pending: true, err: errSegmentDeliveryExhausted, lagged: true, want: false},
		{name: "transient unclean failure", pending: true, err: errors.New("connection reset"), want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := shouldCleanupContinuousAttempt(test.pending, test.err, test.lagged); got != test.want {
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
	if got := active[2]["last_error"]; got != "HTTP 404 [url]" {
		t.Fatalf("last_error=%q want fully redacted url", got)
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

func TestSanitizeDiagnosticURLRemovesProviderAndSourceIdentity(t *testing.T) {
	got := sanitizeDiagnosticError(errString("open https://rr4---sn.example.googlevideo.com/api/manifest/hls_playlist/expire/123/sig/secret/playlist/index.m3u8?token=abc"))
	want := "open [url]"
	if got != want {
		t.Fatalf("sanitized=%q want %q", got, want)
	}
}

type errString string

func (e errString) Error() string { return string(e) }
