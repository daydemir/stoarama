package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestNASStitchPullScopeAllowsOnlyClientRoutes(t *testing.T) {
	keyID := int64(5)
	pull := accountPrincipal{AccountID: 47, AuthType: "api_key", APIKeyID: &keyID, KeyScopes: []string{accountScopePull}}
	for _, tc := range []struct {
		method, path string
		want         int
	}{
		{http.MethodPost, "/api/v1/account/connections/stitch/lease", 200},
		{http.MethodPost, "/api/v1/account/connections/stitch/9/checkpoint", 200},
		{http.MethodPost, "/api/v1/account/connections/stitch/9/result", 200},
		{http.MethodPost, "/api/v1/account/connections/stitch/9/ready", 200},
		{http.MethodGet, "/api/v1/account/connections/stitch/corpus", 200},
		{http.MethodGet, "/api/v1/account/connections/stitch/tools/ffmpeg", 200},
		{http.MethodGet, "/api/v1/account/connections/stitch/tools/ffprobe", 200},
		{http.MethodGet, "/api/v1/account/connections/stitch/lease", 403},
		{http.MethodGet, "/api/v1/account/connections/stitch/9/checkpoint", 403},
		{http.MethodGet, "/api/v1/account/connections/stitch/9/result", 403},
		{http.MethodGet, "/api/v1/account/connections/stitch/9/ready", 403},
		{http.MethodPost, "/api/v1/account/connections/stitch/corpus", 403},
		{http.MethodPost, "/api/v1/account/connections/stitch/tools/ffmpeg", 403},
		{http.MethodDelete, "/api/v1/account/connections/stitch/9/result", 403},
		{http.MethodPost, "/api/v1/account/connections/stitch/x/result", 403},
		{http.MethodPost, "/api/v1/account/connections/stitch/-9/ready", 403},
		{http.MethodPost, "/api/v1/account/connections/stitch/9/delete", 403},
		{http.MethodPost, "/api/v1/account/connections/stitch/9/result/extra", 403},
		{http.MethodPost, "/api/v1/account/connections/stitch/lease/extra", 403},
		{http.MethodGet, "/api/v1/account/connections/stitch/tools/unknown", 403},
		{http.MethodGet, "/api/v1/account/connections/stitch/tools/ffmpeg/extra", 403},
		{http.MethodPost, "/api/v1/service/nas-stitch/scopes", 403},
		{http.MethodPost, "/api/v1/service/nas-stitch/scopes/1/pause", 403},
		{http.MethodPost, "/api/v1/service/nas-cleanup/plans/1398/control", 403},
	} {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			if got := runConfine(pull, tc.method, tc.path); got != tc.want {
				t.Fatalf("status = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestNASStitchPullScopePreservesLeaseHandlerGates(t *testing.T) {
	pool := cleanupTestPool(t)
	s := &Server{pool: pool}
	handler := s.confineAccountScope(http.HandlerFunc(s.handleAccountNASStitchLease))
	for _, tc := range []struct {
		name      string
		accountID int64
		keyID     int64
		enabled   bool
		want      int
	}{
		{"switch off", 47, 5, false, 200},
		{"unadmitted connection", 48, 6, true, 200},
		{"wrong account", 48, 5, true, 403},
		{"unbound key", 47, 99, true, 403},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s.cfg.NASStitchEnabled = tc.enabled
			req := httptest.NewRequest(http.MethodPost, "/api/v1/account/connections/stitch/lease", nil)
			principal := accountPrincipal{AccountID: tc.accountID, AuthType: "api_key", APIKeyID: &tc.keyID, KeyScopes: []string{accountScopePull}}
			req = req.WithContext(context.WithValue(req.Context(), accountPrincipalContextKey, principal))
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, req)
			if w.Code != tc.want || (tc.want == 200 && !strings.Contains(w.Body.String(), `"task":null`)) {
				t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
			}
		})
	}
}
