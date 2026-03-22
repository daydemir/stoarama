package api

import (
	"context"
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
		if strings.TrimSpace(s.cfg.ServiceToken) == "" {
			util.WriteError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		got := strings.TrimSpace(r.Header.Get("Authorization"))
		if !strings.HasPrefix(got, "Bearer ") {
			util.WriteError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		token := strings.TrimSpace(strings.TrimPrefix(got, "Bearer "))
		if token == "" || token != strings.TrimSpace(s.cfg.ServiceToken) {
			util.WriteError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		next.ServeHTTP(w, r)
	})
}
