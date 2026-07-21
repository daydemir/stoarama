package recordingworker

import (
	"testing"
	"time"

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

// TestReconnectBackoff pins the exponential reconnect schedule: 30s doubling per
// consecutive failure, capped at 5m. failures==0 (never happens in the loop but
// guarded) and failures==1 both yield the 30s base, and every count past the cap
// stays at 5m. A clip-bearing attempt resets failures to 0 elsewhere, restarting
// this sequence at 30s.
func TestReconnectBackoff(t *testing.T) {
	cases := []struct {
		failures int
		want     time.Duration
	}{
		{failures: 0, want: 30 * time.Second},
		{failures: 1, want: 30 * time.Second},
		{failures: 2, want: 60 * time.Second},
		{failures: 3, want: 120 * time.Second},
		{failures: 4, want: 240 * time.Second},
		{failures: 5, want: 5 * time.Minute},
		{failures: 6, want: 5 * time.Minute},
		{failures: 100, want: 5 * time.Minute},
	}
	for _, tc := range cases {
		if got := reconnectBackoff(tc.failures); got != tc.want {
			t.Fatalf("reconnectBackoff(%d) = %s, want %s", tc.failures, got, tc.want)
		}
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
