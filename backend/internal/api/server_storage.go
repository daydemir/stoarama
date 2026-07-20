package api

import (
	"context"
	"errors"
	"fmt"
	"log"
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
	if s.secrets == nil {
		util.WriteError(w, http.StatusServiceUnavailable, "storage destinations are not enabled (STORAGE_CRED_KEY is unset)")
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

// handleAccountStorageManagedCreate provisions (or re-uses) the account's single
// managed destination: a real storage_destinations row holding the OPERATOR's R2
// coordinates + the encrypted operator secret, isolated by a per-account
// key_prefix. It skips ALL BYO validation (name/endpoint/.../verify probe) because
// the operator bucket is already trusted. After provisioning it lazily adds the
// stream_hour_month metered item to the account's existing subscription (Option A
// backfill), then returns the masked payload (operator creds blanked).
func (s *Server) handleAccountStorageManagedCreate(w http.ResponseWriter, r *http.Request, principal accountPrincipal) {
	id, keyPrefix, err := s.provisionManagedDestination(r.Context(), s.pool, principal.AccountID)
	if err != nil {
		if errors.Is(err, errManagedUnavailable) {
			util.WriteError(w, http.StatusServiceUnavailable, "managed storage is not available (operator R2 or billing is not configured)")
			return
		}
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("provision managed destination: %v", err))
		return
	}

	// Lazily add the stream_hour_month metered item to a pre-existing subscription
	// that predates managed storage, so opting in starts accruing storage charges on
	// the same subscription. New accounts already get both items at Checkout. Best
	// effort: a Stripe hiccup must not fail provisioning (the nightly metering job
	// reports nothing until the item exists, and a later opt-in retries this).
	if s.billing != nil {
		var subID *string
		if err := s.pool.QueryRow(r.Context(), `
			SELECT stripe_subscription_id FROM account_billing WHERE account_id=$1
		`, principal.AccountID).Scan(&subID); err != nil {
			if !errors.Is(err, pgx.ErrNoRows) {
				log.Printf("managed provision: read subscription for account %d: %v", principal.AccountID, err)
			}
		} else if subID != nil && strings.TrimSpace(*subID) != "" {
			if err := s.billing.EnsureStreamHourMonthItem(r.Context(), *subID); err != nil {
				log.Printf("managed provision: ensure stream_hour_month item for account %d: %v", principal.AccountID, err)
			}
		}
	}

	_ = s.insertAccountAuthEvent(r.Context(), principal.AccountID, nil, "storage_destination_created", "account", principal.Email, map[string]any{
		"destination_id": id,
		"name":           managedDestinationName,
		"provider":       managedDestinationProvider,
		"managed":        true,
	})

	// Masked payload: operator endpoint/bucket/access_key_id are never returned.
	util.WriteJSON(w, http.StatusCreated, map[string]any{
		"id":            id,
		"name":          managedDestinationName,
		"provider":      managedDestinationProvider,
		"endpoint":      "",
		"region":        s.cfg.R2Region,
		"bucket":        "",
		"key_prefix":    keyPrefix,
		"access_key_id": "",
		"status":        "verified",
		"managed":       true,
	})
}

// errManagedUnavailable is returned by provisionManagedDestination when managed
// storage is gated off (no operator R2 client / config, or no secret cipher),
// which the handler maps to 503.
var errManagedUnavailable = errors.New("managed storage unavailable")

const (
	managedDestinationName     = "Stoarama-managed"
	managedDestinationProvider = "r2_managed"
)

// provisionManagedDestination idempotently creates the account's managed
// destination row and returns its id + key_prefix. The row stores the OPERATOR's
// R2 endpoint/region/bucket/access_key_id and the secretbox-encrypted operator
// secret, so the upload-intent presign and ingest Head paths build a client and
// presign IDENTICALLY for managed and BYO (no fork). The unique partial index
// on (account_id) WHERE managed makes the INSERT ... ON CONFLICT DO NOTHING a
// no-op on re-provision; the follow-up SELECT always returns the existing id.
type managedDestinationStore interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

func (s *Server) provisionManagedDestination(ctx context.Context, store managedDestinationStore, accountID int64) (destID int64, keyPrefix string, err error) {
	if s.r2 == nil || s.secrets == nil || s.cfg.ValidateR2() != nil {
		return 0, "", errManagedUnavailable
	}
	keyPrefix = fmt.Sprintf("managed/acct-%d", accountID)
	sealed, err := s.secrets.Encrypt([]byte(s.cfg.R2SecretAccessKey))
	if err != nil {
		return 0, "", fmt.Errorf("encrypt operator secret: %w", err)
	}
	if _, err := store.Exec(ctx, `
		INSERT INTO storage_destinations
			(account_id, name, provider, endpoint, region, bucket, key_prefix, access_key_id, secret_access_key_enc, status, managed, verified_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'verified',true,now())
		ON CONFLICT (account_id) WHERE managed DO NOTHING
	`,
		accountID, managedDestinationName, managedDestinationProvider,
		s.cfg.R2Endpoint, s.cfg.R2Region, s.cfg.R2Bucket, keyPrefix, s.cfg.R2AccessKeyID, sealed,
	); err != nil {
		return 0, "", fmt.Errorf("insert managed destination: %w", err)
	}
	if err := store.QueryRow(ctx, `
		SELECT id FROM storage_destinations WHERE account_id=$1 AND managed
	`, accountID).Scan(&destID); err != nil {
		return 0, "", fmt.Errorf("read managed destination: %w", err)
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
