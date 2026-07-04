package recordingworker

import "testing"

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

type errString string

func (e errString) Error() string { return string(e) }
