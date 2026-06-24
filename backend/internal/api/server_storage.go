package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/daydemir/stoarama/backend/internal/util"
)

// storageVerifyTimeout bounds the live S3 connectivity probe on create.
const storageVerifyTimeout = 20 * time.Second

type storageDestinationCreateRequest struct {
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
	rows, err := s.pool.Query(r.Context(), `
		SELECT id, name, provider, endpoint, region, bucket, key_prefix, access_key_id, status, last_verify_error, verified_at, created_at
		FROM storage_destinations
		WHERE account_id=$1
		ORDER BY created_at DESC, id DESC
	`, principal.AccountID)
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
		)
		if err := rows.Scan(&id, &name, &provider, &endpoint, &region, &bucket, &keyPrefix, &accessKeyID, &status, &lastVerifyError, &verifiedAt, &createdAt); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan storage destination: %v", err))
			return
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
	if s.secrets == nil {
		util.WriteError(w, http.StatusServiceUnavailable, "storage destinations are not enabled (STORAGE_CRED_KEY is unset)")
		return
	}
	var req storageDestinationCreateRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	name := strings.TrimSpace(req.Name)
	provider := strings.TrimSpace(req.Provider)
	if provider == "" {
		provider = "s3_compatible"
	}
	endpoint := strings.TrimSpace(req.Endpoint)
	region := strings.TrimSpace(req.Region)
	bucket := strings.TrimSpace(req.Bucket)
	keyPrefix, err := sanitizeStorageKeyPrefix(req.KeyPrefix)
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	accessKeyID := strings.TrimSpace(req.AccessKeyID)
	secret := strings.TrimSpace(req.SecretAccessKey)
	for label, val := range map[string]string{
		"name":              name,
		"endpoint":          endpoint,
		"region":            region,
		"bucket":            bucket,
		"access_key_id":     accessKeyID,
		"secret_access_key": secret,
	} {
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

	// Verify the destination is reachable and writable before we store anything:
	// a full PutObject -> HeadObject -> DeleteObject roundtrip against the user's bucket.
	verifyCtx, cancel := context.WithTimeout(r.Context(), storageVerifyTimeout)
	defer cancel()
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
			(account_id, name, provider, endpoint, region, bucket, key_prefix, access_key_id, secret_access_key_enc, status, verified_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'verified',$10)
		RETURNING id, created_at
	`, principal.AccountID, name, provider, endpoint, region, bucket, keyPrefix, accessKeyID, sealed, now).Scan(&id, &createdAt)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("create storage destination: %v", err))
		return
	}

	_ = s.insertAccountAuthEvent(r.Context(), principal.AccountID, nil, "storage_destination_created", "account", principal.Email, map[string]any{
		"destination_id": id,
		"name":           name,
		"provider":       provider,
		"endpoint":       endpoint,
		"bucket":         bucket,
	})

	util.WriteJSON(w, http.StatusCreated, map[string]any{
		"id":            id,
		"name":          name,
		"provider":      provider,
		"endpoint":      endpoint,
		"region":        region,
		"bucket":        bucket,
		"key_prefix":    keyPrefix,
		"access_key_id": accessKeyID,
		"status":        "verified",
		"verified_at":   now,
		"created_at":    createdAt.UTC(),
	})
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

// verifyStorageDestination proves the credentials can write, read, and delete in
// the target bucket by round-tripping a tiny probe object under the key prefix.
func verifyStorageDestination(ctx context.Context, cfg r2.Config, keyPrefix string) error {
	client, err := r2.New(ctx, cfg)
	if err != nil {
		return fmt.Errorf("build client: %w", err)
	}
	probe, err := generateSecret(12)
	if err != nil {
		return err
	}
	key := ".stoarama-verify/" + probe
	if keyPrefix != "" {
		key = keyPrefix + "/" + key
	}
	if _, err := client.PutBytes(ctx, key, "text/plain", []byte("stoarama storage destination verification")); err != nil {
		return fmt.Errorf("write probe object: %w", err)
	}
	if _, err := client.Head(ctx, key); err != nil {
		return fmt.Errorf("read probe object: %w", err)
	}
	if err := client.DeleteObjects(ctx, []string{key}); err != nil {
		return fmt.Errorf("delete probe object: %w", err)
	}
	return nil
}
