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
	rows, err := s.pool.Query(ctx, `SELECT access_key_id,secret_access_key_enc
		FROM storage_destinations WHERE provider='r2'`)
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
		accessKey = strings.TrimSpace(accessKey)
		storageSecret := strings.TrimSpace(string(secret))
		if accessKey == bootstrap || accessKey == signing || storageSecret == bootstrap || storageSecret == signing {
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
		if claims.Kind == joinedauth.KindOperation && s.joinedControlPlaneReady() &&
			!s.joinedOperationWithinScope(r.Context(), claims) {
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

func (s *Server) joinedOperationWithinScope(ctx context.Context, claims joinedauth.Claims) bool {
	if claims.BatchID != s.cfg.JoinedRecordingBatchID {
		return false
	}
	scope, err := s.cfg.JoinedWorkScope()
	if err != nil {
		return false
	}
	if scope == "frozen_batch" {
		if s.pool == nil {
			return false
		}
		var allowed bool
		err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM recording_joined_batches b
			JOIN connections c ON c.id=b.connection_id AND c.joined_protocol_version=1
			WHERE b.batch_id=$1 AND (($2='hour' AND EXISTS(SELECT 1 FROM recording_joined_hours h
			  WHERE h.batch_record_id=b.id AND h.hour_id=$3)) OR ($2<>'hour' AND EXISTS(
			  SELECT 1 FROM recording_joined_artifacts a WHERE a.batch_record_id=b.id
			    AND a.scope_kind=$2 AND a.scope_id=$3))))`, claims.BatchID, claims.SubjectKind, claims.SubjectID).Scan(&allowed)
		return err == nil && allowed
	}
	if scope != "canary" {
		return false
	}
	hours := s.joinedCanaryHourIDs()
	switch claims.SubjectKind {
	case joinedauth.SubjectHour:
		for _, hourID := range hours {
			if claims.SubjectID == hourID {
				return true
			}
		}
		return false
	case joinedauth.SubjectLedger:
		if s.pool == nil {
			return false
		}
		var allowed bool
		err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM recording_joined_artifacts a
			JOIN recording_joined_hours h ON h.stream_day_id=a.stream_day_id
			WHERE a.batch_id=$1 AND a.artifact_kind='allocation_ledger' AND a.scope_id=$2
			  AND h.hour_id=ANY($3::text[]))`, claims.BatchID, claims.SubjectID, hours).Scan(&allowed)
		return err == nil && allowed
	case joinedauth.SubjectBatchIndex:
		return false
	default:
		return false
	}
}

func joinedWorkerClaimsFromContext(ctx context.Context) (joinedauth.Claims, bool) {
	claims, ok := ctx.Value(joinedWorkerClaimsContextKey{}).(joinedauth.Claims)
	return claims, ok
}
