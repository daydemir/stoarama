package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/config"
)

func TestJoinedContainmentHandlerRejectsUnsafeRequestsBeforeDatabase(t *testing.T) {
	const batchID = "goodplus-20260821-generation-1"
	base := &Server{cfg: config.Config{
		JoinedRecordingControlPlaneEnabled: true,
		JoinedRecordingProtocolVersion:     1,
		JoinedRecordingProtocolGeneration:  1,
		JoinedRecordingConnectionID:        13,
		JoinedRecordingBatchID:             batchID,
		JoinedRecordingWorkScope:           config.JoinedWorkScopeCanary,
		JoinedRecordingCanaryHourIDs: strings.Join([]string{
			batchID + "__recording-413__date-2026-08-20__hour-01__generation-1",
			batchID + "__recording-416__date-2026-08-20__hour-01__generation-1",
			batchID + "__recording-421__date-2026-08-20__hour-01__generation-1",
		}, ","),
		JoinedOperatorToken:        "operator-token-32-bytes-long-000000",
		JoinedWorkerBootstrapToken: "bootstrap-token-32-bytes-long-000",
		JoinedWorkerSigningKey:     "signing-key-32-bytes-long-0000000",
		ServiceToken:               "service-token-32-bytes-long-000000",
	}}
	call := func(s *Server, path string) int {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		s.handleJoinedContainment(rec, req)
		return rec.Code
	}
	tests := []struct {
		name   string
		server func() *Server
		path   string
		want   int
	}{
		{name: "missing batch", server: func() *Server { return base }, path: "/api/v1/recording/joined/containment", want: http.StatusBadRequest},
		{name: "duplicate batch", server: func() *Server { return base }, path: "/api/v1/recording/joined/containment?batch_id=" + batchID + "&batch_id=" + batchID, want: http.StatusBadRequest},
		{name: "mismatched batch", server: func() *Server { return base }, path: "/api/v1/recording/joined/containment?batch_id=other-generation-1", want: http.StatusForbidden},
		{name: "nil pool", server: func() *Server { return base }, path: "/api/v1/recording/joined/containment?batch_id=" + batchID, want: http.StatusServiceUnavailable},
		{name: "rate limited", server: func() *Server { s := &Server{cfg: base.cfg}; s.joinedContainmentAt = time.Now().UTC(); return s }, path: "/api/v1/recording/joined/containment?batch_id=" + batchID, want: http.StatusTooManyRequests},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := call(tt.server(), tt.path); got != tt.want {
				t.Fatalf("status=%d want=%d", got, tt.want)
			}
		})
	}
}
