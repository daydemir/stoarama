package captureapipersistent

import "testing"

func TestRelayRouteReasonForError(t *testing.T) {
	tests := []struct {
		errText string
		want    string
	}{
		{errText: "Server returned 404 Not Found", want: "relay_404"},
		{errText: "401 Unauthorized", want: "relay_auth_failed"},
		{errText: "dial tcp 10.0.0.1:18080: connect: connection refused", want: "relay_source_unreachable"},
		{errText: "ffmpeg exited with status 1", want: "relay_stream_error"},
	}
	for _, tc := range tests {
		if got := relayRouteReasonForError(tc.errText); got != tc.want {
			t.Fatalf("relayRouteReasonForError(%q)=%q want %q", tc.errText, got, tc.want)
		}
	}
}
