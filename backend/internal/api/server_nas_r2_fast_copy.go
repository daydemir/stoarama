package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/daydemir/stoarama/backend/internal/util"
	"github.com/jackc/pgx/v5"
	"golang.org/x/sync/errgroup"
)

const (
	nasR2FastCopyURLTTL     = 15 * time.Minute
	nasR2FastCopyLeaseTTL   = time.Hour
	nasR2FastCopyBatchMax   = 64
	nasR2FastCopyBatchSize  = 32
	nasR2FastCopyMaxWorkers = 32
	// A fast-copy object must finish within one lease even at a slow 0.6 MB/s;
	// a partial transfer restarts from zero, so larger objects would never land.
	// Raw clips are tens of MB; anything larger stays pending and visible.
	nasR2FastCopyMaxFile int64 = 2 << 30
)

type nasR2FastCopyTask struct {
	nasR2CopyTask
	URL     string      `json:"url,omitempty"`
	Headers http.Header `json:"headers,omitempty"`
}

type nasR2FastCopyCandidate struct {
	nasR2FastCopyTask
	data      json.RawMessage
	versionID string
	reason    NASR2CopyBlockReason
}

// The fast client has one batch worker, and that worker asks for a lease only
// after every future of its previous batch has returned. A new lease therefore
// supersedes all earlier fast leases of the connection: a batch whose lease or
// result response was lost frees its items now, not when its hour-long TTL ends.
// A lease request the client already timed out can still commit after its
// retry, so a superseded token may publish bytes the server never acknowledges.
// That copy is ETag-bound and create-only; the next lease verifies it in place.
var nasR2FastCopyEligible = `q.connection_id=$1 AND q.kind='raw' AND (q.item->>'size_bytes')::bigint <= ` + strconv.FormatInt(nasR2FastCopyMaxFile, 10) + ` AND
 (q.state IN ('pending','leased') OR ($2 AND control.enabled AND q.state IN ('refresh_pending','refresh_leased')))`

// A discarded capability returns its item to the queue; only a fresh lease may grant another GET.
const nasR2FastCopyRelease = `UPDATE nas_r2_copy_items SET state=CASE WHEN work_mode='copy' THEN 'pending' ELSE 'refresh_pending' END,
 lease_token=NULL,lease_expires_at=NULL,updated_at=clock_timestamp() WHERE `

func (s *Server) handleNASR2FastCopyLease(w http.ResponseWriter, r *http.Request, cid int64, verify bool) {
	ctx := r.Context()
	if err := nasR2CopyReady(ctx, s.pool); err != nil {
		nasR2CopyReadyError(w, err)
		return
	}
	limit := s.cfg.NASR2FastCopyBatchSize
	if limit == 0 {
		limit = nasR2FastCopyBatchSize
	}
	limit = min(max(1, limit), nasR2FastCopyBatchMax)
	policy, _, err := Keep28Policy()
	if err != nil {
		util.WriteError(w, 503, "copy policy unavailable")
		return
	}
	rows, err := s.pool.Query(ctx, `SELECT q.id,q.item,q.policy_sha256,q.work_mode,coalesce(c.etag,'')
 FROM nas_r2_copy_items q CROSS JOIN r2_keep28_refresh_control control LEFT JOIN recording_clips c ON c.id=q.source_id
 WHERE `+nasR2FastCopyEligible+` ORDER BY (q.work_mode=control.last_mode),CASE WHEN q.work_mode='verify_existing' THEN q.updated_at END,q.id LIMIT $3`, cid, verify, limit)
	if err != nil {
		util.WriteError(w, 503, "copy queue unavailable")
		return
	}
	candidates, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (nasR2FastCopyCandidate, error) {
		var c nasR2FastCopyCandidate
		err := row.Scan(&c.ID, &c.data, &c.PolicySHA256, &c.WorkMode, &c.ETag)
		if err == nil {
			err = json.Unmarshal(c.data, &c.Item)
		}
		c.ETag = strings.Trim(c.ETag, `"`)
		return c, err
	})
	if err != nil {
		util.WriteError(w, 503, "copy queue unavailable")
		return
	}

	// HEAD and signing run without database locks. Nothing is returned until
	// the copy admission lock protects source revalidation and lease commit.
	for _, c := range candidates {
		if c.PolicySHA256 != policy {
			util.WriteError(w, 409, "copy policy changed")
			return
		}
	}
	var group errgroup.Group
	group.SetLimit(16)
	for i := range candidates {
		c := &candidates[i]
		group.Go(func() error {
			if ValidateNASR2CopyItem(c.Item) != nil || c.Item.Kind != NASR2CopyRaw || c.Item.Bucket != s.r2.Bucket() {
				c.reason = NASR2CopySourceChanged
				return nil
			}
			if c.ETag == "" {
				c.reason = NASR2CopyR2Identity
				return nil
			}
			work, cancel := context.WithTimeout(ctx, 750*time.Millisecond)
			defer cancel()
			head, err := s.r2.Head(work, c.Item.ObjectKey)
			if err != nil && !r2.IsNotFound(err) {
				return err
			}
			if err != nil || strings.Trim(head.ETag, `"`) != c.ETag || head.SizeBytes != c.Item.SizeBytes ||
				(head.SHA256Metadata != "" && !strings.EqualFold(head.SHA256Metadata, c.Item.SHA256)) {
				c.reason = NASR2CopyR2Identity
				return nil
			}
			c.versionID = head.VersionID
			if c.WorkMode == NASR2OrdinaryCopy {
				cap, err := s.r2.PresignGetExactRequest(work, c.Item.ObjectKey, c.ETag, c.versionID, nasR2FastCopyURLTTL)
				if err != nil {
					return err
				}
				c.URL, c.Headers = cap.URL, cap.Headers
			}
			nonce := make([]byte, 24)
			if _, err := rand.Read(nonce); err != nil {
				return err
			}
			c.Token = hex.EncodeToString(nonce)
			return nil
		})
	}
	capabilityErr := group.Wait()
	tx, err := BeginNASR2CopyTx(ctx, s.pool)
	if err != nil {
		util.WriteError(w, 503, "copy database unavailable")
		return
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, nasR2CopyLock); err != nil {
		util.WriteError(w, 503, "copy admission busy")
		return
	}
	if err = nasR2CopyReady(ctx, tx); err != nil {
		nasR2CopyReadyError(w, err)
		return
	}
	var version string
	var expires time.Time
	var workers int
	if err = tx.QueryRow(ctx, `SELECT client_version,clock_timestamp()+$2::integer*interval '1 second',r2_fast_copy_workers
 FROM connections WHERE id=$1`, cid, int(nasR2FastCopyLeaseTTL/time.Second)).Scan(&version, &expires, &workers); err != nil || !nasR2FastCopyLine(version) {
		util.WriteError(w, 409, "copy session changed during admission")
		return
	}
	if _, err = tx.Exec(ctx, nasR2FastCopyRelease+`connection_id=$1 AND kind='raw' AND state IN ('leased','refresh_leased')`, cid); err != nil {
		util.WriteError(w, 503, "copy lease supersede failed")
		return
	}
	tasks := make([]nasR2FastCopyTask, 0, len(candidates))
	for _, c := range candidates {
		if len(tasks) >= limit {
			break
		}
		if c.Token == "" && c.reason == "" {
			continue
		}
		var etag string
		err = tx.QueryRow(ctx, `SELECT coalesce(btrim(c.etag,'"'),'')
 FROM nas_r2_copy_items q CROSS JOIN r2_keep28_refresh_control control LEFT JOIN recording_clips c ON c.id=q.source_id
 WHERE `+nasR2FastCopyEligible+` AND q.id=$3 AND q.item=$4::jsonb AND q.policy_sha256=$5 AND q.work_mode=$6 FOR UPDATE OF q SKIP LOCKED`,
			cid, verify, c.ID, c.data, c.PolicySHA256, c.WorkMode).Scan(&etag)
		if errors.Is(err, pgx.ErrNoRows) || (err == nil && etag != c.ETag) {
			continue
		}
		if err == nil {
			c.Clip, err = LoadNASR2CopySource(ctx, tx, c.Item)
			if errors.Is(err, errNASR2CopySourceIdentity) {
				c.reason, err = NASR2CopySourceChanged, nil
			}
		}
		if err == nil && c.reason != "" {
			err = blockNASR2Copy(ctx, tx, c.ID, c.reason)
		} else if err == nil {
			state := "leased"
			if c.WorkMode == NASR2VerifyExisting {
				state = "refresh_leased"
			}
			_, err = tx.Exec(ctx, `UPDATE nas_r2_copy_items SET state=$2,lease_token=$3,lease_expires_at=$4,etag=$5,version_id=$6,
 range_until=NULL,range_id=NULL,range_length=0,attempts=attempts+1,last_error='',updated_at=clock_timestamp() WHERE id=$1`,
				c.ID, state, c.Token, expires, c.ETag, c.versionID)
			tasks = append(tasks, c.nasR2FastCopyTask)
		}
		if err != nil {
			util.WriteError(w, 503, "copy batch grant failed")
			return
		}
	}
	err = nil
	if len(tasks) > 0 {
		_, err = tx.Exec(ctx, `UPDATE r2_keep28_refresh_control SET last_mode=$1 WHERE singleton`, tasks[len(tasks)-1].WorkMode)
	}
	if err != nil || tx.Commit(ctx) != nil {
		util.WriteError(w, 503, "copy batch commit failed")
		return
	}
	if len(tasks) == 0 && capabilityErr != nil {
		util.WriteError(w, 503, "copy R2 capability unavailable; retryable")
		return
	}
	util.WriteJSON(w, 200, map[string]any{"tasks": tasks, "lease_expires_at": expires, "floor_bytes": NASR2CopyFloor, "workers": workers})
}

type nasR2FastCopyResult struct {
	ID              int64               `json:"id"`
	Token           string              `json:"token"`
	Completion      NASR2CopyCompletion `json:"completion,omitempty"`
	DownloadedBytes int64               `json:"downloaded_bytes,omitempty"`
	CommittedBytes  int64               `json:"committed_bytes,omitempty"`
}

func (s *Server) handleNASR2FastCopyResults(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.nasR2CopyConnection(w, r, nasConnectionCopy); !ok {
		return
	}
	version, _ := nasRequestWorkVersion(r, nasConnectionCopy)
	if !nasR2FastCopyLine(version) {
		util.WriteError(w, 409, "fast copy release required")
		return
	}
	var req struct {
		FreeBytes int64                 `json:"free_bytes"`
		Results   []nasR2FastCopyResult `json:"results"`
	}
	if util.DecodeJSON(r, &req) != nil || len(req.Results) == 0 || len(req.Results) > nasR2FastCopyBatchMax {
		util.WriteError(w, 400, "invalid copy result batch")
		return
	}
	seen := make(map[int64]bool, len(req.Results))
	for _, result := range req.Results {
		if result.ID <= 0 || len(result.Token) != 48 || seen[result.ID] || result.DownloadedBytes < 0 ||
			(result.Completion == NASR2CopyDownloaded && (result.DownloadedBytes <= 0 || result.DownloadedBytes != result.CommittedBytes)) ||
			(result.Completion != NASR2CopyDownloaded && (result.CommittedBytes != 0 || result.DownloadedBytes != 0)) ||
			(result.Completion != "" && result.Completion != NASR2CopyDownloaded && result.Completion != NASR2CopyVerifiedInPlace) {
			util.WriteError(w, 400, "invalid copy result")
			return
		}
		if result.Completion != "" && req.FreeBytes < NASR2CopyFloor+NASR2CopyRangeBytes {
			util.WriteError(w, 409, "copy free-space floor")
			return
		}
		seen[result.ID] = true
	}

	ctx := r.Context()
	tx, err := BeginNASR2CopyTx(ctx, s.pool)
	if err != nil {
		util.WriteError(w, 503, "copy database unavailable")
		return
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock_shared($1)`, nasR2CopyLock); err != nil || boundNASR2CopyRowLocks(ctx, tx) != nil {
		util.WriteError(w, 503, "copy result busy; retryable")
		return
	}
	if err = nasR2CopyReady(ctx, tx); err != nil {
		nasR2CopyReadyError(w, err)
		return
	}
	policy, _, err := Keep28Policy()
	if err != nil {
		util.WriteError(w, 503, "copy policy unavailable")
		return
	}
	verified := make([]int64, 0, len(req.Results))
	for _, result := range req.Results {
		var task nasR2CopyTask
		var data json.RawMessage
		err = tx.QueryRow(ctx, `SELECT id,item,work_mode FROM nas_r2_copy_items
 WHERE id=$1 AND connection_id=13 AND kind='raw' AND state IN ('leased','refresh_leased')
 AND lease_token=$2 AND lease_expires_at>clock_timestamp() AND policy_sha256=$3 FOR UPDATE`,
			result.ID, result.Token, policy).Scan(&task.ID, &data, &task.WorkMode)
		if errors.Is(err, pgx.ErrNoRows) {
			continue
		}
		if err == nil {
			err = json.Unmarshal(data, &task.Item)
		}
		exact := false
		if err == nil && (result.Completion == NASR2CopyVerifiedInPlace ||
			(result.Completion == NASR2CopyDownloaded && task.WorkMode == NASR2OrdinaryCopy && result.CommittedBytes == task.Item.SizeBytes)) {
			if _, err = LoadNASR2CopySource(ctx, tx, task.Item); err == nil {
				exact, err = nasR2CopyExactInventory(ctx, tx, task.Item)
			} else if errors.Is(err, errNASR2CopySourceIdentity) {
				err = nil
			}
		}
		if err == nil && exact {
			err = finishNASR2RawCopy(ctx, tx, task, nasR2CopyControl{
				Completion: result.Completion, DownloadedBytes: result.DownloadedBytes, CommittedBytes: result.CommittedBytes,
			}, true)
			verified = append(verified, task.ID)
		} else if err == nil {
			// A stale owner cannot return its replacement: the token matched above.
			_, err = tx.Exec(ctx, nasR2FastCopyRelease+`id=$1`, task.ID)
		}
		if err != nil {
			util.WriteError(w, 503, "copy result failed; retryable")
			return
		}
	}
	if tx.Commit(ctx) != nil {
		util.WriteError(w, 503, "copy result commit failed")
		return
	}
	util.WriteJSON(w, 200, map[string]any{"verified": verified})
}

// handleAdminConnectionR2FastCopy reads (GET) or sets (POST) one NAS
// connection's fast-copy stream count; the client applies it to its next batch.
func (s *Server) handleAdminConnectionR2FastCopy(w http.ResponseWriter, r *http.Request) {
	connectionID, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	var out struct {
		ConnectionID int64 `json:"connection_id"`
		Workers      int   `json:"workers"`
	}
	out.ConnectionID = connectionID
	query := `SELECT r2_fast_copy_workers FROM connections WHERE id=$1 AND kind='nas_pull'`
	args := []any{connectionID}
	if r.Method == http.MethodPost {
		var req struct {
			Workers int `json:"workers"`
		}
		if err := util.DecodeJSON(r, &req); err != nil {
			util.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
		if req.Workers < 1 || req.Workers > nasR2FastCopyMaxWorkers {
			util.WriteError(w, http.StatusBadRequest, "workers must be between 1 and 32")
			return
		}
		query = `UPDATE connections SET r2_fast_copy_workers=$2 WHERE id=$1 AND kind='nas_pull' RETURNING r2_fast_copy_workers`
		args = append(args, req.Workers)
	}
	err := s.pool.QueryRow(r.Context(), query, args...).Scan(&out.Workers)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusNotFound, "NAS connection not found")
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "fast copy workers")
		return
	}
	util.WriteJSON(w, http.StatusOK, out)
}
