package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/daydemir/stoarama/backend/internal/util"
)

// storageVerifyTimeout bounds the live S3 connectivity probe on create.
const storageVerifyTimeout = 20 * time.Second

// storageDestAccessPredicate is the single owner-or-granted authorization
// predicate for selecting a storage destination: the account owns it, OR it is a
// shared (admin-owned) destination the account has been granted. It is the same
// fragment used by the per-account list, recording-create binding, and the clip
// transfer target check, so authorization is defined once. The bound parameter is
// the account id; callers substitute the placeholder number (e.g. $1) to match
// their query. A non-owner NEVER receives the destination's credentials (the list
// handler blanks access_key_id for non-owner rows; the secret is never selected).
const storageDestAccessPredicate = `(sd.account_id = %[1]s
   OR (sd.shared AND EXISTS (
         SELECT 1 FROM storage_destination_grants g
         WHERE g.storage_destination_id = sd.id AND g.account_id = %[1]s)))`

type storageDestinationCreateRequest struct {
	Managed         bool   `json:"managed"`
	Name            string `json:"name"`
	Provider        string `json:"provider"`
	Endpoint        string `json:"endpoint"`
	Region          string `json:"region"`
	Bucket          string `json:"bucket"`
	KeyPrefix       string `json:"key_prefix"`
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
}

func (s *Server) handleAccountStorageDestinationsList(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	rows, err := s.pool.Query(r.Context(), fmt.Sprintf(`
		SELECT sd.id, sd.name, sd.provider, sd.endpoint, sd.region, sd.bucket, sd.key_prefix, sd.access_key_id,
		       sd.status, sd.last_verify_error, sd.verified_at, sd.created_at, sd.managed, sd.shared,
		       (sd.account_id = $1) AS is_owner
		FROM storage_destinations sd
		WHERE %s
		ORDER BY sd.created_at DESC, sd.id DESC
	`, fmt.Sprintf(storageDestAccessPredicate, "$1")), principal.AccountID)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("list storage destinations: %v", err))
		return
	}
	defer rows.Close()
	items := make([]map[string]any, 0, 8)
	for rows.Next() {
		var (
			id              int64
			name            string
			provider        string
			endpoint        string
			region          string
			bucket          string
			keyPrefix       string
			accessKeyID     string
			status          string
			lastVerifyError string
			verifiedAt      *time.Time
			createdAt       time.Time
			managed         bool
			shared          bool
			isOwner         bool
		)
		if err := rows.Scan(&id, &name, &provider, &endpoint, &region, &bucket, &keyPrefix, &accessKeyID, &status, &lastVerifyError, &verifiedAt, &createdAt, &managed, &shared, &isOwner); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan storage destination: %v", err))
			return
		}
		// Managed rows carry the OPERATOR's R2 coordinates and access key; blank the
		// operator-owned fields so the per-account UI never sees or leaks them.
		if managed {
			endpoint = ""
			bucket = ""
			accessKeyID = ""
		}
		// Shared destinations a non-owner was granted: the account may select it and
		// see its name/host, but access_key_id (the username) is a credential, so
		// blank it for non-owners (the secret is never selected). One blanking idiom.
		if !isOwner {
			accessKeyID = ""
		}
		items = append(items, map[string]any{
			"id":                id,
			"name":              name,
			"provider":          provider,
			"endpoint":          endpoint,
			"region":            region,
			"bucket":            bucket,
			"key_prefix":        keyPrefix,
			"access_key_id":     accessKeyID,
			"status":            status,
			"last_verify_error": lastVerifyError,
			"verified_at":       verifiedAt,
			"created_at":        createdAt.UTC(),
			"managed":           managed,
			"shared":            shared,
			"is_owner":          isOwner,
		})
	}
	if err := rows.Err(); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("iterate storage destinations: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (s *Server) handleAccountStorageDestinationsCreate(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	var req storageDestinationCreateRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.Managed {
		s.handleAccountStorageManagedCreate(w, r, principal)
		return
	}
	util.WriteError(w, http.StatusConflict, "S3 storage verification and provisioning are retired")
}

func (s *Server) handleAccountStorageDestinationDelete(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	id, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	ct, err := s.pool.Exec(r.Context(), `
		DELETE FROM storage_destinations
		WHERE id=$1 AND account_id=$2
	`, id, principal.AccountID)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23503" {
			util.WriteError(w, http.StatusConflict, "storage destination is in use by a recording")
			return
		}
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("delete storage destination: %v", err))
		return
	}
	if ct.RowsAffected() == 0 {
		util.WriteError(w, http.StatusNotFound, "storage destination not found")
		return
	}
	_ = s.insertAccountAuthEvent(r.Context(), principal.AccountID, nil, "storage_destination_deleted", "account", principal.Email, map[string]any{
		"destination_id": id,
	})
	util.WriteJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// handleAccountStorageManagedCreate resolves an existing verified destination.
// It never provisions, encrypts, bills or emits a creation event.
func (s *Server) handleAccountStorageManagedCreate(w http.ResponseWriter, r *http.Request, principal accountPrincipal) {
	id, _, err := s.resolveManagedDestination(r.Context(), s.pool, principal.AccountID)
	if errors.Is(err, errManagedUnavailable) {
		util.WriteError(w, http.StatusConflict, "managed storage provisioning is retired; no existing verified destination")
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "resolve managed destination failed")
		return
	}
	var name, provider, region, keyPrefix, status string
	var verifiedAt *time.Time
	var createdAt time.Time
	if err := s.pool.QueryRow(r.Context(), `SELECT name,provider,region,key_prefix,status,verified_at,created_at FROM storage_destinations WHERE id=$1 AND account_id=$2 AND managed AND status='verified'`, id, principal.AccountID).Scan(&name, &provider, &region, &keyPrefix, &status, &verifiedAt, &createdAt); err != nil {
		util.WriteError(w, http.StatusConflict, "existing managed destination changed")
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"id": id, "name": name, "provider": provider, "endpoint": "", "region": region, "bucket": "", "key_prefix": keyPrefix, "access_key_id": "", "status": status, "verified_at": verifiedAt, "created_at": createdAt, "managed": true})
}

// Missing managed storage cannot be provisioned during retirement.
var errManagedUnavailable = errors.New("managed storage unavailable")

const (
	managedDestinationName     = "Stoarama-managed"
	managedDestinationProvider = "r2_managed"
)

type managedDestinationStore interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func (s *Server) resolveManagedDestination(ctx context.Context, store managedDestinationStore, accountID int64) (destID int64, keyPrefix string, err error) {
	err = store.QueryRow(ctx, `SELECT id,key_prefix FROM storage_destinations WHERE account_id=$1 AND managed AND status='verified'`, accountID).Scan(&destID, &keyPrefix)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, "", errManagedUnavailable
	}
	if err != nil {
		return 0, "", fmt.Errorf("resolve managed destination: %w", err)
	}
	return destID, keyPrefix, nil
}

// sanitizeStorageKeyPrefix normalizes a user-supplied object-key prefix and
// rejects path-traversal and control-character injection. An empty prefix is
// valid (objects land at the bucket root). It strips surrounding whitespace and
// edge slashes, then refuses backslashes, control characters, and any "."/".."
// path segment, so the prefix can be safely composed into recording object keys.
func sanitizeStorageKeyPrefix(raw string) (string, error) {
	trimmed := strings.Trim(strings.TrimSpace(raw), "/")
	if trimmed == "" {
		return "", nil
	}
	for _, r := range trimmed {
		if r == '\\' {
			return "", fmt.Errorf("key_prefix must not contain backslashes")
		}
		if r < 0x20 || r == 0x7f {
			return "", fmt.Errorf("key_prefix must not contain control characters")
		}
	}
	for _, seg := range strings.Split(trimmed, "/") {
		if seg == "" {
			return "", fmt.Errorf("key_prefix must not contain empty path segments")
		}
		if seg == "." || seg == ".." {
			return "", fmt.Errorf("key_prefix must not contain '.' or '..' path segments")
		}
	}
	return trimmed, nil
}

// verifyStorageDestination is retired before it creates a client or a probe.
// Both account and admin S3 paths also refuse before credential/DB mutations.
func verifyStorageDestination(ctx context.Context, cfg r2.Config, keyPrefix string) error {
	return r2.ErrLegacyDeleteRetired
}
