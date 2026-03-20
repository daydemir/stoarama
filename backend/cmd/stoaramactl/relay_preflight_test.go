package main

import "testing"

func TestRelayRouteFailureReason(t *testing.T) {
	tests := []struct {
		statusCode int
		errText    string
		want       string
	}{
		{statusCode: 404, errText: "relay preflight returned 404 Not Found", want: "relay_404"},
		{statusCode: 401, errText: "relay preflight returned 401 Unauthorized", want: "relay_auth_failed"},
		{statusCode: 0, errText: "dial tcp 10.0.0.1:18080: connect: connection refused", want: "relay_source_unreachable"},
		{statusCode: 0, errText: "unexpected upstream error", want: "relay_preflight_failed"},
	}
	for _, tc := range tests {
		if got := relayRouteFailureReason(tc.statusCode, tc.errText); got != tc.want {
			t.Fatalf("relayRouteFailureReason(%d,%q)=%q want %q", tc.statusCode, tc.errText, got, tc.want)
		}
	}
}
