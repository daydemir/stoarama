package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/daydemir/stoarama/backend/internal/config"
)

func TestJoinedContainmentRequiresDedicatedOperatorToken(t *testing.T) {
	s := &Server{cfg: config.Config{
		JoinedOperatorToken:        "operator-token-32-bytes-long-000000",
		JoinedWorkerBootstrapToken: "bootstrap-token-32-bytes-long-000",
		JoinedWorkerSigningKey:     "signing-key-32-bytes-long-0000000",
		ServiceToken:               "service-token-32-bytes-long-000000",
	}}
	handler := s.requireJoinedOperatorAuth(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	tests := []struct {
		name   string
		token  string
		status int
	}{
		{name: "missing", status: http.StatusUnauthorized},
		{name: "bootstrap", token: s.cfg.JoinedWorkerBootstrapToken, status: http.StatusUnauthorized},
		{name: "signing", token: s.cfg.JoinedWorkerSigningKey, status: http.StatusUnauthorized},
		{name: "service", token: s.cfg.ServiceToken, status: http.StatusUnauthorized},
		{name: "wrong", token: "wrong-token", status: http.StatusUnauthorized},
		{name: "operator", token: s.cfg.JoinedOperatorToken, status: http.StatusNoContent},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/api/v1/recording/joined/containment?batch_id=test", nil)
			if tt.token != "" {
				req.Header.Set("Authorization", "Bearer "+tt.token)
			}
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			if rec.Code != tt.status {
				t.Fatalf("status=%d want=%d body=%s", rec.Code, tt.status, rec.Body.String())
			}
		})
	}
	for name, collision := range map[string]string{
		"empty":     "",
		"service":   s.cfg.ServiceToken,
		"bootstrap": s.cfg.JoinedWorkerBootstrapToken,
		"signing":   s.cfg.JoinedWorkerSigningKey,
	} {
		t.Run("rejects "+name+" operator configuration", func(t *testing.T) {
			bad := *s
			bad.cfg.JoinedOperatorToken = collision
			req := httptest.NewRequest(http.MethodGet, "/api/v1/recording/joined/containment?batch_id=test", nil)
			req.Header.Set("Authorization", "Bearer "+s.cfg.JoinedOperatorToken)
			rec := httptest.NewRecorder()
			bad.requireJoinedOperatorAuth(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusNoContent)
			})).ServeHTTP(rec, req)
			if rec.Code != http.StatusUnauthorized {
				t.Fatalf("status=%d want=%d", rec.Code, http.StatusUnauthorized)
			}
		})
	}
}
