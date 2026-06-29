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
	"github.com/daydemir/stoarama/backend/internal/webdav"
)

// Admin-only management of SHARED storage destinations (v1: admin-owned only).
// deniz creates a shared destination (S3-compatible or WebDAV), runs the live
// connection check, and grants specific accounts access. Granted accounts may
// SELECT it for recordings but NEVER see its credentials. All routes are under the
// admin group (role admin), so the admin's own account id owns each shared row.

type adminStorageDestinationCreateRequest struct {
	Name            string `json:"name"`
	Provider        string `json:"provider"`
	Endpoint        string `json:"endpoint"`
	Region          string `json:"region"`
	Bucket          string `json:"bucket"`
	KeyPrefix       string `json:"key_prefix"`
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
}

// handleAdminStorageDestinationCreate creates a shared destination after a live
// connection check. provider 'webdav' verifies via a WebDAV MKCOL+PUT+HEAD+GET+DELETE
// probe; any other provider verifies via the existing S3 round-trip. The secret is
// secretbox-encrypted; the row is shared=true, managed=false, status='verified'.
func (s *Server) handleAdminStorageDestinationCreate(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	if s.secrets == nil {
		util.WriteError(w, http.StatusServiceUnavailable, "storage destinations are not enabled (STORAGE_CRED_KEY is unset)")
		return
	}
	var req adminStorageDestinationCreateRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	name := strings.TrimSpace(req.Name)
	provider := strings.TrimSpace(req.Provider)
	if provider == "" {
		provider = "s3_compatible"
	}
	if provider != "webdav" && provider != "s3_compatible" {
		util.WriteError(w, http.StatusBadRequest, "provider must be 'webdav' or 's3_compatible'")
		return
	}
	endpoint := strings.TrimSpace(req.Endpoint)
	keyPrefix, err := sanitizeStorageKeyPrefix(req.KeyPrefix)
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	accessKeyID := strings.TrimSpace(req.AccessKeyID)
	secret := strings.TrimSpace(req.SecretAccessKey)
	region := strings.TrimSpace(req.Region)
	bucket := strings.TrimSpace(req.Bucket)

	// Required fields differ by transport. WebDAV reuses endpoint=base URL,
	// access_key_id=username, key_prefix=base path, secret=password; region/bucket
	// are unused and stored as ''.
	required := map[string]string{
		"name":              name,
		"endpoint":          endpoint,
		"access_key_id":     accessKeyID,
		"secret_access_key": secret,
	}
	if provider == "s3_compatible" {
		required["region"] = region
		required["bucket"] = bucket
	}
	for label, val := range required {
		if val == "" {
			util.WriteError(w, http.StatusBadRequest, fmt.Sprintf("%s is required", label))
			return
		}
	}

	var nameExists bool
	if err := s.pool.QueryRow(r.Context(), `
		SELECT EXISTS(SELECT 1 FROM storage_destinations WHERE account_id=$1 AND lower(name)=lower($2))
	`, principal.AccountID, name).Scan(&nameExists); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("check storage destination: %v", err))
		return
	}
	if nameExists {
		util.WriteError(w, http.StatusConflict, "a storage destination with that name already exists")
		return
	}

	verifyCtx, cancel := context.WithTimeout(r.Context(), storageVerifyTimeout)
	defer cancel()
	if provider == "webdav" {
		if err := verifyWebDAVDestination(verifyCtx, endpoint, accessKeyID, secret, keyPrefix); err != nil {
			util.WriteError(w, http.StatusBadRequest, fmt.Sprintf("destination verification failed: %v", err))
			return
		}
	} else {
		if err := verifyStorageDestination(verifyCtx, r2.Config{
			AccessKey: accessKeyID,
			SecretKey: secret,
			Region:    region,
			Bucket:    bucket,
			Endpoint:  endpoint,
		}, keyPrefix); err != nil {
			util.WriteError(w, http.StatusBadRequest, fmt.Sprintf("destination verification failed: %v", err))
			return
		}
	}

	sealed, err := s.secrets.Encrypt([]byte(secret))
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("encrypt secret: %v", err))
		return
	}

	now := time.Now().UTC()
	var (
		id        int64
		createdAt time.Time
	)
	err = s.pool.QueryRow(r.Context(), `
		INSERT INTO storage_destinations
			(account_id, name, provider, endpoint, region, bucket, key_prefix, access_key_id, secret_access_key_enc, status, shared, verified_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'verified',true,$10)
		RETURNING id, created_at
	`, principal.AccountID, name, provider, endpoint, region, bucket, keyPrefix, accessKeyID, sealed, now).Scan(&id, &createdAt)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("create shared storage destination: %v", err))
		return
	}

	_ = s.insertAccountAuthEvent(r.Context(), principal.AccountID, nil, "shared_storage_destination_created", "account", principal.Email, map[string]any{
		"destination_id": id,
		"name":           name,
		"provider":       provider,
		"endpoint":       endpoint,
	})

	util.WriteJSON(w, http.StatusCreated, map[string]any{
		"id":          id,
		"name":        name,
		"provider":    provider,
		"endpoint":    endpoint,
		"region":      region,
		"bucket":      bucket,
		"key_prefix":  keyPrefix,
		"status":      "verified",
		"shared":      true,
		"verified_at": now,
		"created_at":  createdAt.UTC(),
	})
}

// handleAdminStorageDestinationsList returns all shared destinations and their
// grants. It NEVER returns any secret or the access_key_id (a credential); the
// admin sees name/endpoint/provider/status + the granted accounts.
func (s *Server) handleAdminStorageDestinationsList(w http.ResponseWriter, r *http.Request) {
	rows, err := s.pool.Query(r.Context(), `
		SELECT sd.id, sd.name, sd.provider, sd.endpoint, sd.key_prefix, sd.status, sd.last_verify_error, sd.verified_at, sd.created_at,
		       COALESCE(
		         (SELECT jsonb_agg(jsonb_build_object('account_id', g.account_id, 'email', a.email) ORDER BY a.email)
		          FROM storage_destination_grants g
		          JOIN accounts a ON a.id = g.account_id
		          WHERE g.storage_destination_id = sd.id),
		         '[]'::jsonb) AS grants
		FROM storage_destinations sd
		WHERE sd.shared
		ORDER BY sd.created_at DESC, sd.id DESC
	`)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("list shared storage destinations: %v", err))
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
			keyPrefix       string
			status          string
			lastVerifyError string
			verifiedAt      *time.Time
			createdAt       time.Time
			grants          []map[string]any
		)
		if err := rows.Scan(&id, &name, &provider, &endpoint, &keyPrefix, &status, &lastVerifyError, &verifiedAt, &createdAt, &grants); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan shared storage destination: %v", err))
			return
		}
		items = append(items, map[string]any{
			"id":                id,
			"name":              name,
			"provider":          provider,
			"endpoint":          endpoint,
			"key_prefix":        keyPrefix,
			"status":            status,
			"last_verify_error": lastVerifyError,
			"verified_at":       verifiedAt,
			"created_at":        createdAt.UTC(),
			"grants":            grants,
		})
	}
	if err := rows.Err(); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("iterate shared storage destinations: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"items": items})
}

// handleAdminStorageDestinationDelete deletes a shared destination (and, via ON
// DELETE CASCADE, its grants). A 23503 FK violation means a recording still
// references it -> 409.
func (s *Server) handleAdminStorageDestinationDelete(w http.ResponseWriter, r *http.Request) {
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
		DELETE FROM storage_destinations WHERE id=$1 AND shared
	`, id)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23503" {
			util.WriteError(w, http.StatusConflict, "shared storage destination is in use by a recording")
			return
		}
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("delete shared storage destination: %v", err))
		return
	}
	if ct.RowsAffected() == 0 {
		util.WriteError(w, http.StatusNotFound, "shared storage destination not found")
		return
	}
	_ = s.insertAccountAuthEvent(r.Context(), principal.AccountID, nil, "shared_storage_destination_deleted", "account", principal.Email, map[string]any{
		"destination_id": id,
	})
	util.WriteJSON(w, http.StatusOK, map[string]any{"ok": true})
}

type adminStorageGrantRequest struct {
	AccountID int64  `json:"account_id"`
	Email     string `json:"email"`
}

// handleAdminStorageDestinationGrantCreate grants an account access to a shared
// destination. The account is resolved by account_id or email. The destination
// must be shared; granted_by records the admin.
func (s *Server) handleAdminStorageDestinationGrantCreate(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	destID, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	var req adminStorageGrantRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	// The destination must exist and be shared.
	var isShared bool
	if err := s.pool.QueryRow(r.Context(), `
		SELECT shared FROM storage_destinations WHERE id=$1
	`, destID).Scan(&isShared); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			util.WriteError(w, http.StatusNotFound, "shared storage destination not found")
			return
		}
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load shared storage destination: %v", err))
		return
	}
	if !isShared {
		util.WriteError(w, http.StatusBadRequest, "destination is not shared")
		return
	}

	// Resolve the grantee account.
	accountID := req.AccountID
	if accountID <= 0 {
		email := normalizeAccountEmail(req.Email)
		if !looksLikeEmail(email) {
			util.WriteError(w, http.StatusBadRequest, "account_id or a valid email is required")
			return
		}
		if err := s.pool.QueryRow(r.Context(), `SELECT id FROM accounts WHERE email=$1`, email).Scan(&accountID); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				util.WriteError(w, http.StatusNotFound, "account not found")
				return
			}
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("resolve account: %v", err))
			return
		}
	}

	if _, err := s.pool.Exec(r.Context(), `
		INSERT INTO storage_destination_grants (storage_destination_id, account_id, granted_by)
		VALUES ($1,$2,$3)
		ON CONFLICT (storage_destination_id, account_id) DO NOTHING
	`, destID, accountID, principal.AccountID); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23503" {
			util.WriteError(w, http.StatusNotFound, "account not found")
			return
		}
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("grant storage destination: %v", err))
		return
	}
	_ = s.insertAccountAuthEvent(r.Context(), principal.AccountID, nil, "shared_storage_grant_created", "account", principal.Email, map[string]any{
		"destination_id":  destID,
		"grantee_account": accountID,
	})
	util.WriteJSON(w, http.StatusOK, map[string]any{"ok": true, "account_id": accountID})
}

// handleAdminStorageDestinationGrantDelete revokes an account's access to a shared
// destination.
func (s *Server) handleAdminStorageDestinationGrantDelete(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	destID, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	accountID, ok := parseInt64Path(w, r, "accountId")
	if !ok {
		return
	}
	ct, err := s.pool.Exec(r.Context(), `
		DELETE FROM storage_destination_grants WHERE storage_destination_id=$1 AND account_id=$2
	`, destID, accountID)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("revoke storage destination grant: %v", err))
		return
	}
	if ct.RowsAffected() == 0 {
		util.WriteError(w, http.StatusNotFound, "grant not found")
		return
	}
	_ = s.insertAccountAuthEvent(r.Context(), principal.AccountID, nil, "shared_storage_grant_revoked", "account", principal.Email, map[string]any{
		"destination_id":  destID,
		"grantee_account": accountID,
	})
	util.WriteJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// verifyWebDAVDestination proves the WebDAV credentials can authenticate and the
// destination is writable, mirroring verifyStorageDestination's S3 round-trip:
// build a client, then MKCOL the base + write/read/delete a probe object.
func verifyWebDAVDestination(ctx context.Context, endpoint, user, pass, basePath string) error {
	client, err := webdav.New(webdav.Config{
		Endpoint: endpoint,
		User:     user,
		Pass:     pass,
		BasePath: basePath,
	})
	if err != nil {
		return fmt.Errorf("build client: %w", err)
	}
	probe, err := generateSecret(12)
	if err != nil {
		return err
	}
	return client.Probe(ctx, probe)
}
