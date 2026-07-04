package recordingworker

import (
	"testing"
	"time"
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

type errString string

func (e errString) Error() string { return string(e) }
