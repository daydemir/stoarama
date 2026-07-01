package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestPullPathAllowed asserts the pure allowlist that confines a NAS pull key.
// Default is DENY: only the 4 pull shapes (right method + path) pass.
func TestPullPathAllowed(t *testing.T) {
	cases := []struct {
		method string
		path   string
		want   bool
	}{
		// The 4 pull endpoints.
		{http.MethodGet, "/api/v1/account/clips", true},
		{http.MethodPost, "/api/v1/account/connections/heartbeat", true},
		{http.MethodGet, "/api/v1/account/recordings/12/clips/34/download", true},
		{http.MethodDelete, "/api/v1/account/recordings/12/clips/34", true},

		// Wrong method on a pull path.
		{http.MethodPost, "/api/v1/account/clips", false},
		{http.MethodGet, "/api/v1/account/connections/heartbeat", false},
		{http.MethodDelete, "/api/v1/account/recordings/12/clips/34/download", false},
		{http.MethodGet, "/api/v1/account/recordings/12/clips/34", false},

		// Bulk delete-all (no clipId) must NOT pass: a pull key cannot wipe a recording.
		{http.MethodDelete, "/api/v1/account/recordings/12/clips", false},

		// Non-numeric params must not slip through the anchored regexps.
		{http.MethodGet, "/api/v1/account/recordings/x/clips/34/download", false},
		{http.MethodDelete, "/api/v1/account/recordings/12/clips/abc", false},
		{http.MethodGet, "/api/v1/account/recordings/12/clips/34/download/extra", false},

		// A sampling of management/data routes that must be denied to a pull key.
		{http.MethodPost, "/api/v1/account/api-keys", false},
		{http.MethodPost, "/api/v1/account/connections", false},
		{http.MethodGet, "/api/v1/account/connections", false},
		{http.MethodGet, "/api/v1/account/billing", false},
		{http.MethodPost, "/api/v1/account/recordings", false},
		{http.MethodGet, "/api/v1/account/members", false},
		{http.MethodGet, "/api/v1/account/me", false},
	}
	for _, c := range cases {
		if got := pullPathAllowed(c.method, c.path); got != c.want {
			t.Errorf("pullPathAllowed(%s %s)=%v want %v", c.method, c.path, got, c.want)
		}
	}
}

// runConfine drives confineAccountScope around a sentinel handler with the given
// principal in context, returning the status code (200 = passed through).
func runConfine(p accountPrincipal, method, path string) int {
	s := &Server{}
	called := false
	h := s.confineAccountScope(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	}))
	req := httptest.NewRequest(method, path, nil)
	req = req.WithContext(context.WithValue(req.Context(), accountPrincipalContextKey, p))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code == http.StatusOK && !called {
		return -1
	}
	return rec.Code
}

func TestConfineAccountScopePullKeyConfined(t *testing.T) {
	keyID := int64(99)
	pull := accountPrincipal{AccountID: 7, AuthType: "api_key", APIKeyID: &keyID, KeyScopes: []string{accountScopePull}}

	// 200 on all 4 pull endpoints.
	pullPaths := []struct {
		method, path string
	}{
		{http.MethodGet, "/api/v1/account/clips"},
		{http.MethodPost, "/api/v1/account/connections/heartbeat"},
		{http.MethodGet, "/api/v1/account/recordings/12/clips/34/download"},
		{http.MethodDelete, "/api/v1/account/recordings/12/clips/34"},
	}
	for _, p := range pullPaths {
		if code := runConfine(pull, p.method, p.path); code != http.StatusOK {
			t.Errorf("pull key on %s %s = %d, want 200", p.method, p.path, code)
		}
	}

	// 403 on a sampling of non-pull endpoints.
	denyPaths := []struct {
		method, path string
	}{
		{http.MethodPost, "/api/v1/account/api-keys"},
		{http.MethodGet, "/api/v1/account/billing"},
		{http.MethodPost, "/api/v1/account/recordings"},
		{http.MethodPost, "/api/v1/account/connections"},
		{http.MethodDelete, "/api/v1/account/recordings/5/clips"},
		{http.MethodGet, "/api/v1/account/me"},
	}
	for _, p := range denyPaths {
		if code := runConfine(pull, p.method, p.path); code != http.StatusForbidden {
			t.Errorf("pull key on %s %s = %d, want 403", p.method, p.path, code)
		}
	}
}

func TestConfineAccountScopeFullKeyAndSessionUnaffected(t *testing.T) {
	keyID := int64(5)
	full := accountPrincipal{AccountID: 7, AuthType: "api_key", APIKeyID: &keyID, KeyScopes: []string{accountScopeRead}}
	sessionID := int64(3)
	session := accountPrincipal{AccountID: 7, AuthType: "session", SessionID: &sessionID}

	paths := []struct {
		method, path string
	}{
		{http.MethodPost, "/api/v1/account/api-keys"},
		{http.MethodGet, "/api/v1/account/billing"},
		{http.MethodGet, "/api/v1/account/clips"},
		{http.MethodPost, "/api/v1/account/connections"},
		{http.MethodGet, "/api/v1/account/me"},
	}
	for _, principal := range []accountPrincipal{full, session} {
		for _, p := range paths {
			if code := runConfine(principal, p.method, p.path); code != http.StatusOK {
				t.Errorf("%s on %s %s = %d, want 200 (unaffected)", principal.AuthType, p.method, p.path, code)
			}
		}
	}
}

func TestClampPollIntervalSec(t *testing.T) {
	cases := map[int]int{0: 60, 5: 10, 10: 10, 90: 90, 3600: 3600, 9000: 3600, -1: 10}
	for in, want := range cases {
		if got := clampPollIntervalSec(in); got != want {
			t.Errorf("clampPollIntervalSec(%d)=%d want %d", in, got, want)
		}
	}
}

func TestIsPullScopedPrincipal(t *testing.T) {
	keyID := int64(1)
	if isPullScopedPrincipal(accountPrincipal{SessionID: &keyID}) {
		t.Error("session principal must not be pull-scoped")
	}
	if isPullScopedPrincipal(accountPrincipal{APIKeyID: &keyID, KeyScopes: []string{accountScopeRead}}) {
		t.Error("read key must not be pull-scoped")
	}
	if !isPullScopedPrincipal(accountPrincipal{APIKeyID: &keyID, KeyScopes: []string{accountScopePull}}) {
		t.Error("pull key must be pull-scoped")
	}
}
