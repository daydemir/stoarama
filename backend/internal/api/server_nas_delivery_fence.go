package api

import (
	"bytes"
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/util"
	"github.com/jackc/pgx/v5"
)

const NASDeliveryFenceLock int64 = 7343226011185

// ActivateNASDeliveryFence is opt-in and never called at API startup. The caller
// must first deploy admission to EVERY API instance and retire old instances.
func ActivateNASDeliveryFence(ctx context.Context, tx pgx.Tx) error {
	if _, _, err := Keep28Policy(); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, NASDeliveryFenceLock); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `INSERT INTO nas_delivery_retirement_fences(connection_id,policy_bytes,policy_sha256,effective_at,legacy_expires_at)
 SELECT id,$1,$2,t,t+interval '7 days' FROM connections CROSS JOIN (SELECT clock_timestamp() t) stamp WHERE id=13 AND account_id=47 AND kind='nas_pull'`, cleanupKeep28PolicyJSON, cleanupPolicySHA)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO nas_delivery_retirement_windows SELECT 13,(w->>'stream_id')::bigint,w->>'tz',(w->>'keep_start_utc')::timestamptz,(w->>'keep_end_utc')::timestamptz FROM jsonb_array_elements($1::jsonb) w`, string(cleanupKeep28PolicyJSON))
	return err
}

type nasDeliveryResponse struct {
	header http.Header
	status int
	body   bytes.Buffer
}

func (w *nasDeliveryResponse) Header() http.Header { return w.header }
func (w *nasDeliveryResponse) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
}
func (w *nasDeliveryResponse) Write(b []byte) (int, error) {
	w.WriteHeader(200)
	return w.body.Write(b)
}

// Buffer capability responses until the durable lease commits. Holding the
// shared admission lock until lease commit serializes activation and admission.
// The lease covers the maximum S3 lifetime plus the bounded request lifetime.
// Settlement ALSO requires a NAS transfer-drain receipt.
func (s *Server) fenceNASDelivery(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, ok := accountPrincipalFromContext(r.Context())
		if !ok || p.APIKeyID == nil {
			next.ServeHTTP(w, r)
			return
		}
		cid, err := pullConnectionID(r.Context(), s.pool, p, false)
		if err == pgx.ErrNoRows || cid != 13 && err == nil {
			next.ServeHTTP(w, r)
			return
		}
		if err != nil {
			util.WriteError(w, 503, "delivery connection unavailable")
			return
		}
		if !pullPathAllowed(r.Method, r.URL.Path) {
			util.WriteError(w, 403, "NAS connection is confined to pull delivery paths")
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Minute)
		defer cancel()
		r = r.WithContext(ctx)
		tx, err := s.pool.Begin(r.Context())
		if err != nil {
			util.WriteError(w, 503, "delivery admission unavailable")
			return
		}
		defer tx.Rollback(r.Context())
		if _, err = tx.Exec(r.Context(), `SELECT pg_advisory_xact_lock_shared($1)`, NASDeliveryFenceLock); err != nil {
			util.WriteError(w, 503, "delivery admission unavailable")
			return
		}
		capability := strings.HasSuffix(r.URL.Path, "/download") || strings.HasSuffix(r.URL.Path, "/redownload") || strings.HasSuffix(r.URL.Path, "/nas-restore/lease")
		if capability {
			_, err = tx.Exec(r.Context(), `INSERT INTO nas_delivery_download_leases(connection_id,expires_at) SELECT 13,clock_timestamp()+interval '7 days 2 minutes' WHERE NOT EXISTS(SELECT 1 FROM nas_delivery_retirement_fences WHERE connection_id=13)`)
			if err != nil {
				util.WriteError(w, 503, "delivery lease unavailable")
				return
			}
		}
		if err = tx.Commit(r.Context()); err != nil {
			util.WriteError(w, 503, "delivery admission commit failed")
			return
		}
		response := &nasDeliveryResponse{header: make(http.Header)}
		next.ServeHTTP(response, r)
		if ctx.Err() != nil {
			util.WriteError(w, 503, "delivery admission expired")
			return
		}

		for key, values := range response.header {
			w.Header()[key] = values
		}
		status := response.status
		if status == 0 {
			status = 200
		}
		w.WriteHeader(status)
		_, _ = w.Write(response.body.Bytes())
	})
}

// Only a NAS key is subject to this fence; ordinary account/browser authority
// cannot be used by the confined pull key to bypass these download gates.
func (s *Server) allowNASDelivery(w http.ResponseWriter, r *http.Request, kind string, id int64) bool {
	p, ok := accountPrincipalFromContext(r.Context())
	if !ok || p.APIKeyID == nil {
		return true
	}
	cid, err := pullConnectionID(r.Context(), s.pool, p, false)
	if err == pgx.ErrNoRows {
		return true
	}
	var allowed bool
	if err == nil {
		err = s.pool.QueryRow(r.Context(), `SELECT nas_delivery_allowed($1,$2,$3)`, cid, kind, id).Scan(&allowed)
	}
	if err != nil {
		util.WriteError(w, 503, "delivery fence unavailable")
		return false
	}
	if !allowed {
		util.WriteError(w, 409, "delivery retired outside pinned keep28 window")
		return false
	}
	return true
}

// NAS presigns must fit the S3 maximum covered by the legacy drain barrier.
func nasDeliveryTTL(ttl time.Duration) time.Duration {
	if ttl <= 0 || ttl > 7*24*time.Hour {
		return time.Hour
	}
	return ttl
}
