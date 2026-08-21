package api

import (
	"context"
	"crypto/subtle"
	"net/http"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/joinedauth"
	"github.com/daydemir/stoarama/backend/internal/util"
)

type joinedWorkerClaimsContextKey struct{}

func (s *Server) requireJoinedWorkerBootstrapAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		expected := strings.TrimSpace(s.cfg.JoinedWorkerBootstrapToken)
		serviceToken := strings.TrimSpace(s.cfg.ServiceToken)
		signingKey := strings.TrimSpace(s.cfg.JoinedWorkerSigningKey)
		authorization := strings.TrimSpace(r.Header.Get("Authorization"))
		if expected == "" || expected == serviceToken || expected == signingKey || !strings.HasPrefix(authorization, "Bearer ") ||
			subtle.ConstantTimeCompare([]byte(strings.TrimSpace(strings.TrimPrefix(authorization, "Bearer "))), []byte(expected)) != 1 {
			util.WriteError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) requireJoinedWorkerAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		signingKey := strings.TrimSpace(s.cfg.JoinedWorkerSigningKey)
		if signingKey == "" || signingKey == strings.TrimSpace(s.cfg.ServiceToken) ||
			signingKey == strings.TrimSpace(s.cfg.JoinedWorkerBootstrapToken) {
			util.WriteError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		authorization := strings.TrimSpace(r.Header.Get("Authorization"))
		if !strings.HasPrefix(authorization, "Bearer ") {
			util.WriteError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		claims, err := joinedauth.Verify(signingKey, strings.TrimSpace(strings.TrimPrefix(authorization, "Bearer ")), time.Now().UTC())
		if err != nil {
			util.WriteError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		ctx := context.WithValue(r.Context(), joinedWorkerClaimsContextKey{}, claims)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func joinedWorkerClaimsFromContext(ctx context.Context) (joinedauth.Claims, bool) {
	claims, ok := ctx.Value(joinedWorkerClaimsContextKey{}).(joinedauth.Claims)
	return claims, ok
}
