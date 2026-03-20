package api

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/daydemir/stoarama/backend/internal/util"
)

type servicePrincipal struct {
	TokenType string
}

type serviceContextKey string

const servicePrincipalContextKey serviceContextKey = "service_principal"

func (s *Server) requireAdminAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal, err := s.authenticateAccountRequest(r)
		if err != nil {
			util.WriteError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		if principal.Role != "admin" {
			util.WriteError(w, http.StatusForbidden, "forbidden")
			return
		}
		ctx := context.WithValue(r.Context(), accountPrincipalContextKey, principal)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (s *Server) requireAdminOrServiceAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if principal, err := s.authenticateAccountRequest(r); err == nil {
			if principal.Role != "admin" {
				util.WriteError(w, http.StatusForbidden, "forbidden")
				return
			}
			ctx := context.WithValue(r.Context(), accountPrincipalContextKey, principal)
			next.ServeHTTP(w, r.WithContext(ctx))
			return
		}

		servicePrincipal, err := s.authenticateServiceRequest(r)
		if err != nil {
			util.WriteError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		ctx := context.WithValue(r.Context(), servicePrincipalContextKey, servicePrincipal)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (s *Server) authenticateServiceRequest(r *http.Request) (servicePrincipal, error) {
	if r == nil {
		return servicePrincipal{}, fmt.Errorf("request is nil")
	}
	token := strings.TrimSpace(s.cfg.ServiceToken)
	if token == "" {
		return servicePrincipal{}, fmt.Errorf("service auth not configured")
	}
	got := strings.TrimSpace(r.Header.Get("Authorization"))
	if !strings.HasPrefix(got, "Bearer ") {
		return servicePrincipal{}, fmt.Errorf("missing service bearer token")
	}
	raw := strings.TrimSpace(strings.TrimPrefix(got, "Bearer "))
	if raw == "" || raw != token {
		return servicePrincipal{}, fmt.Errorf("invalid service bearer token")
	}
	return servicePrincipal{TokenType: "service"}, nil
}
