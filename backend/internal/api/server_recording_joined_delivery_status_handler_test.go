package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/config"
)

func TestJoinedDeliveryStatusRejectsUnsafeRequestsBeforeDatabase(t *testing.T) {
	const batchID = "goodplus-20260821-generation-1"
	base := &Server{cfg: config.Config{
		JoinedRecordingControlPlaneEnabled: true,
		JoinedRecordingProtocolVersion:     1,
		JoinedRecordingProtocolGeneration:  1,
		JoinedRecordingConnectionID:        13,
		JoinedRecordingBatchID:             batchID,
		JoinedRecordingWorkScope:           config.JoinedWorkScopeSingleCanary,
		JoinedRecordingCanaryHourIDs:       batchID + "__recording-420__date-2026-08-15__hour-06__generation-1",
		JoinedOperatorToken:                "operator-token-32-bytes-long-000000",
		JoinedWorkerBootstrapToken:         "bootstrap-token-32-bytes-long-000",
		JoinedWorkerSigningKey:             "signing-key-32-bytes-long-0000000",
		ServiceToken:                       "service-token-32-bytes-long-000000",
	}}
	call := func(s *Server, path string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		s.handleJoinedDeliveryStatus(rec, req)
		return rec
	}
	tests := []struct {
		name string
		path string
		want int
	}{
		{name: "missing artifact", path: "/api/v1/recording/joined/delivery-status?batch_id=" + batchID, want: http.StatusBadRequest},
		{name: "duplicate artifact", path: "/api/v1/recording/joined/delivery-status?batch_id=" + batchID + "&artifact_id=1&artifact_id=1", want: http.StatusBadRequest},
		{name: "extra parameter", path: "/api/v1/recording/joined/delivery-status?batch_id=" + batchID + "&artifact_id=1&x=1", want: http.StatusBadRequest},
		{name: "noncanonical artifact", path: "/api/v1/recording/joined/delivery-status?batch_id=" + batchID + "&artifact_id=01", want: http.StatusBadRequest},
		{name: "bad artifact", path: "/api/v1/recording/joined/delivery-status?batch_id=" + batchID + "&artifact_id=no", want: http.StatusBadRequest},
		{name: "mismatched batch", path: "/api/v1/recording/joined/delivery-status?batch_id=other-generation-1&artifact_id=1", want: http.StatusForbidden},
		{name: "nil pool", path: "/api/v1/recording/joined/delivery-status?batch_id=" + batchID + "&artifact_id=1", want: http.StatusServiceUnavailable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := call(&Server{cfg: base.cfg}, tt.path); got.Code != tt.want {
				t.Fatalf("status=%d want=%d body=%s", got.Code, tt.want, got.Body.String())
			}
		})
	}
	rateLimited := &Server{cfg: base.cfg, joinedDeliveryStatusAt: time.Now().UTC()}
	if got := call(rateLimited, "/api/v1/recording/joined/delivery-status?batch_id="+batchID+"&artifact_id=1"); got.Code != http.StatusTooManyRequests {
		t.Fatalf("rate-limited status=%d want=%d", got.Code, http.StatusTooManyRequests)
	}
	if cache := call(&Server{cfg: base.cfg}, "/api/v1/recording/joined/delivery-status?batch_id="+batchID+"&artifact_id=1").Header().Get("Cache-Control"); !strings.Contains(cache, "no-store") {
		t.Fatalf("Cache-Control=%q", cache)
	}
}

func TestJoinedDeliveryStatusRouteRequiresDedicatedOperatorToken(t *testing.T) {
	const batchID = "goodplus-20260821-generation-1"
	s := &Server{cfg: config.Config{
		JoinedRecordingControlPlaneEnabled: true,
		JoinedRecordingProtocolVersion:     1,
		JoinedRecordingProtocolGeneration:  1,
		JoinedRecordingConnectionID:        13,
		JoinedRecordingBatchID:             batchID,
		JoinedRecordingWorkScope:           config.JoinedWorkScopeSingleCanary,
		JoinedRecordingCanaryHourIDs:       batchID + "__recording-420__date-2026-08-15__hour-06__generation-1",
		JoinedOperatorToken:                "operator-token-32-bytes-long-000000",
		JoinedWorkerBootstrapToken:         "bootstrap-token-32-bytes-long-000",
		JoinedWorkerSigningKey:             "signing-key-32-bytes-long-0000000",
		ServiceToken:                       "service-token-32-bytes-long-000000",
	}}
	path := "/api/v1/recording/joined/delivery-status?batch_id=" + batchID + "&artifact_id=492"
	for name, token := range map[string]string{
		"missing": "", "service": s.cfg.ServiceToken, "bootstrap": s.cfg.JoinedWorkerBootstrapToken,
		"signing": s.cfg.JoinedWorkerSigningKey,
	} {
		t.Run(name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, path, nil)
			if token != "" {
				req.Header.Set("Authorization", "Bearer "+token)
			}
			rec := httptest.NewRecorder()
			s.router().ServeHTTP(rec, req)
			if rec.Code != http.StatusUnauthorized {
				t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
			}
		})
	}
	req := httptest.NewRequest(http.MethodGet, path, nil)
	req.Header.Set("Authorization", "Bearer "+s.cfg.JoinedOperatorToken)
	rec := httptest.NewRecorder()
	s.router().ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("operator status=%d body=%s", rec.Code, rec.Body.String())
	}
}
