package api

import (
	"context"
	"crypto/subtle"
	"net/http"
	"strings"

	"github.com/daydemir/stoarama/backend/internal/util"
)

type adminOverrideContextKey string

const adminOverrideKey adminOverrideContextKey = "admin_override"

func (s *Server) requireAdminAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal, err := s.authenticateAccountRequest(r)
		if err != nil {
			util.WriteError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		if principal.Role != accountRoleAdmin {
			util.WriteError(w, http.StatusForbidden, "admin access required")
			return
		}
		ctx := context.WithValue(r.Context(), accountPrincipalContextKey, principal)
		ctx = context.WithValue(ctx, adminOverrideKey, true)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func adminOverrideFromContext(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	v, _ := ctx.Value(adminOverrideKey).(bool)
	return v
}

func (s *Server) requireServiceAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.hasServiceBearerToken(r) {
			util.WriteError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) hasServiceBearerToken(r *http.Request) bool {
	if r == nil || strings.TrimSpace(s.cfg.ServiceToken) == "" {
		return false
	}
	got := strings.TrimSpace(r.Header.Get("Authorization"))
	if !strings.HasPrefix(got, "Bearer ") {
		return false
	}
	token := strings.TrimSpace(strings.TrimPrefix(got, "Bearer "))
	if token == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(token), []byte(strings.TrimSpace(s.cfg.ServiceToken))) == 1
}

func (s *Server) requireServiceOrLocalRecorderNodeAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.hasServiceBearerToken(r) {
			next.ServeHTTP(w, r)
			return
		}
		principal, err := s.authenticateNodeRequest(r)
		if err != nil || strings.TrimSpace(principal.NodeType) != nodeTypeLocalRecorder {
			util.WriteError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		ctx := context.WithValue(r.Context(), nodePrincipalContextKey, principal)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (s *Server) requireRecordingMutationAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.hasServiceBearerToken(r) {
			next.ServeHTTP(w, r)
			return
		}
		if principal, err := s.authenticateNodeRequest(r); err == nil && strings.TrimSpace(principal.NodeType) == nodeTypeLocalRecorder {
			ctx := context.WithValue(r.Context(), nodePrincipalContextKey, principal)
			next.ServeHTTP(w, r.WithContext(ctx))
			return
		}

		principal, err := s.authenticateAccountSessionRequest(r)
		if err != nil {
			util.WriteError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		ctx := context.WithValue(r.Context(), accountPrincipalContextKey, principal)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
