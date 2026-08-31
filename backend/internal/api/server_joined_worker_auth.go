package api

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/joinedauth"
	"github.com/daydemir/stoarama/backend/internal/util"
)

type joinedWorkerClaimsContextKey struct{}

func constantTimeDigestMatch(candidate [sha256.Size]byte, allowed [][sha256.Size]byte) bool {
	matched := 0
	for _, digest := range allowed {
		matched |= subtle.ConstantTimeCompare(candidate[:], digest[:])
	}
	return matched == 1
}

func (s *Server) joinedBootstrapAuthDigests() ([][sha256.Size]byte, error) {
	if err := s.cfg.ValidateJoined(); err != nil {
		return nil, err
	}
	digests, err := s.cfg.JoinedWorkerBootstrapSHA256s()
	if err != nil {
		return nil, err
	}
	legacy := strings.TrimSpace(s.cfg.JoinedWorkerBootstrapToken)
	if legacy == "" {
		return nil, errors.New("joined worker bootstrap authority is unavailable")
	}
	return append(digests, sha256.Sum256([]byte(legacy))), nil
}

func (s *Server) joinedWorkerAuthorityDigests() ([][sha256.Size]byte, error) {
	digests, err := s.joinedBootstrapAuthDigests()
	if err != nil {
		return nil, err
	}
	signing := strings.TrimSpace(s.cfg.JoinedWorkerSigningKey)
	if signing == "" {
		return nil, errors.New("joined worker signing authority is unavailable")
	}
	return append(digests, sha256.Sum256([]byte(signing))), nil
}

func credentialDigestMatches(credential string, allowed [][sha256.Size]byte) bool {
	return constantTimeDigestMatch(sha256.Sum256([]byte(strings.TrimSpace(credential))), allowed)
}

func (s *Server) validateJoinedStorageCredentialIsolation(ctx context.Context) error {
	if s.joinedCredentialCheck != nil {
		return s.joinedCredentialCheck(ctx)
	}
	if s.pool == nil {
		return errors.New("joined storage credential isolation is unavailable")
	}
	rows, err := s.pool.Query(ctx, `SELECT access_key_id,secret_access_key_enc
		FROM storage_destinations WHERE provider IN('r2','r2_managed')`)
	if err != nil {
		return err
	}
	defer rows.Close()
	workerAuthority, err := s.joinedWorkerAuthorityDigests()
	if err != nil {
		return err
	}
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
		if credentialDigestMatches(accessKey, workerAuthority) || credentialDigestMatches(storageSecret, workerAuthority) {
			return errors.New("joined storage credential aliases worker authority")
		}
	}
	return rows.Err()
}

func (s *Server) requireJoinedWorkerBootstrapAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		allowed, err := s.joinedBootstrapAuthDigests()
		authorization := strings.TrimSpace(r.Header.Get("Authorization"))
		if err != nil || !strings.HasPrefix(authorization, "Bearer ") {
			util.WriteError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		presented := strings.TrimSpace(strings.TrimPrefix(authorization, "Bearer "))
		if len(presented) < 32 || !constantTimeDigestMatch(sha256.Sum256([]byte(presented)), allowed) {
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

func (s *Server) requireJoinedOperatorAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		expected := strings.TrimSpace(s.cfg.JoinedOperatorToken)
		serviceToken := strings.TrimSpace(s.cfg.ServiceToken)
		bootstrap := strings.TrimSpace(s.cfg.JoinedWorkerBootstrapToken)
		signingKey := strings.TrimSpace(s.cfg.JoinedWorkerSigningKey)
		authorization := strings.TrimSpace(r.Header.Get("Authorization"))
		if expected == "" || expected == serviceToken || expected == bootstrap || expected == signingKey ||
			!strings.HasPrefix(authorization, "Bearer ") ||
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
	identity, err := s.joinedWorkScopeIdentity()
	if err != nil {
		return false
	}
	scopeSHA, err := identity.SHA256(claims.BatchID)
	if err != nil {
		return false
	}
	if scope == "frozen_batch" {
		if s.pool == nil {
			return false
		}
		var allowed bool
		err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM recording_joined_batches b
			JOIN connections c ON c.id=b.connection_id AND c.id=$4
			WHERE b.batch_id=$1 AND (($2='hour' AND EXISTS(SELECT 1 FROM recording_joined_hours h
			  LEFT JOIN recording_joined_artifacts root ON root.hour_record_id=h.id AND root.artifact_kind='hour_manifest'
			  WHERE h.batch_record_id=b.id AND h.hour_id=$3
			    AND (root.id IS NULL OR h.source_clip_count>0 OR EXISTS(SELECT 1
			      FROM recording_joined_gap_only_scope_authorizations ga WHERE ga.artifact_id=root.id
			        AND ga.batch_record_id=root.batch_record_id AND ga.batch_id=root.batch_id
			        AND ga.hour_record_id=root.hour_record_id AND ga.hour_id=root.scope_id
			        AND ga.work_scope=$6 AND ga.work_scope_identity_sha256=$5
			        AND ga.authorization_source IN ('server_seal','operator_frozen')))) OR ($2<>'hour' AND EXISTS(
			  SELECT 1 FROM recording_joined_artifacts a WHERE a.batch_record_id=b.id
			    AND a.scope_kind=$2 AND a.scope_id=$3
			    AND ($2<>'batch_index' OR (a.artifact_kind='batch_index' AND NOT EXISTS(SELECT 1
			      FROM recording_joined_batch_index_refs ref
			      JOIN recording_joined_artifacts target ON target.id=ref.referenced_artifact_id
			      JOIN recording_joined_hours h ON h.id=target.hour_record_id
			      WHERE ref.index_artifact_id=a.id AND ref.reference_kind='hour_manifest' AND h.source_clip_count=0
			        AND NOT EXISTS(SELECT 1 FROM recording_joined_gap_only_scope_authorizations ga
			          WHERE ga.artifact_id=target.id AND ga.batch_record_id=target.batch_record_id
			            AND ga.batch_id=target.batch_id AND ga.hour_record_id=target.hour_record_id
			            AND ga.hour_id=target.scope_id AND ga.work_scope='frozen_batch'
			            AND ga.work_scope_identity_sha256=$5
			            AND ga.authorization_source IN ('server_seal','operator_frozen'))))))))))`, claims.BatchID, claims.SubjectKind, claims.SubjectID,
			s.cfg.JoinedRecordingConnectionID, scopeSHA, identity.WorkScope).Scan(&allowed)
		return err == nil && allowed
	}
	if !config.IsJoinedCanaryWorkScope(scope) {
		return false
	}
	hours := s.joinedCanaryHourIDs()
	switch claims.SubjectKind {
	case joinedauth.SubjectHour:
		if s.pool == nil {
			for _, hourID := range hours {
				if claims.SubjectID == hourID {
					return true
				}
			}
			return false
		}
		var allowed bool
		err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM recording_joined_hours h
			JOIN recording_joined_batches b ON b.id=h.batch_record_id AND b.connection_id=$4
			LEFT JOIN recording_joined_artifacts root ON root.hour_record_id=h.id AND root.artifact_kind='hour_manifest'
			WHERE h.batch_id=$1 AND h.hour_id=$2 AND h.hour_id=ANY($3::text[])
			  AND (root.id IS NULL OR h.source_clip_count>0 OR EXISTS(SELECT 1
			    FROM recording_joined_gap_only_scope_authorizations ga WHERE ga.artifact_id=root.id
			      AND ga.batch_record_id=root.batch_record_id AND ga.batch_id=root.batch_id
			      AND ga.hour_record_id=root.hour_record_id AND ga.hour_id=root.scope_id
			      AND ga.work_scope=$6 AND ga.work_scope_identity_sha256=$5
			      AND ga.authorization_source='server_seal')))`, claims.BatchID,
			claims.SubjectID, hours, s.cfg.JoinedRecordingConnectionID, scopeSHA, identity.WorkScope).Scan(&allowed)
		return err == nil && allowed
	case joinedauth.SubjectLedger:
		if s.pool == nil {
			return false
		}
		var allowed bool
		err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM recording_joined_artifacts a
			JOIN recording_joined_hours h ON h.stream_day_id=a.stream_day_id
			JOIN recording_joined_batches b ON b.id=a.batch_record_id AND b.connection_id=$4
			WHERE a.batch_id=$1 AND a.artifact_kind='allocation_ledger' AND a.scope_id=$2
			  AND h.hour_id=ANY($3::text[]))`, claims.BatchID, claims.SubjectID, hours,
			s.cfg.JoinedRecordingConnectionID).Scan(&allowed)
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
