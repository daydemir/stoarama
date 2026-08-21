package api

import (
	"context"
	"crypto/subtle"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/joinedauth"
	"github.com/daydemir/stoarama/backend/internal/util"
)

type joinedWorkerClaimsContextKey struct{}

func (s *Server) validateJoinedStorageCredentialIsolation(ctx context.Context) error {
	if s.joinedCredentialCheck != nil {
		return s.joinedCredentialCheck(ctx)
	}
	if s.pool == nil {
		return errors.New("joined storage credential isolation is unavailable")
	}
	rows, err := s.pool.Query(ctx, `SELECT sd.access_key_id,sd.secret_access_key_enc
		FROM storage_destinations sd WHERE EXISTS (
		  SELECT 1 FROM recording_joined_source_snapshots snapshot
		  JOIN recording_joined_batches batch ON batch.id=snapshot.batch_record_id
		  WHERE snapshot.storage_destination_id=sd.id AND batch.state<>'terminal_failed')`)
	if err != nil {
		return err
	}
	defer rows.Close()
	bootstrap := strings.TrimSpace(s.cfg.JoinedWorkerBootstrapToken)
	signing := strings.TrimSpace(s.cfg.JoinedWorkerSigningKey)
	for rows.Next() {
		var accessKey string
		var encrypted []byte
		if err := rows.Scan(&accessKey, &encrypted); err != nil {
			return err
		}
		if s.secrets == nil {
			return errors.New("joined storage credential key is unavailable")
		}
		secret, err := s.secrets.Decrypt(encrypted)
		if err != nil {
			return err
		}
		if accessKey == bootstrap || accessKey == signing || string(secret) == bootstrap || string(secret) == signing {
			return errors.New("joined storage credential aliases worker authority")
		}
	}
	return rows.Err()
}

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
		if err := s.validateJoinedStorageCredentialIsolation(r.Context()); err != nil {
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
		if err := s.validateJoinedStorageCredentialIsolation(r.Context()); err != nil {
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
