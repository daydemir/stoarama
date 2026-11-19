package api

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/daydemir/stoarama/backend/internal/util"
	"github.com/jackc/pgx/v5"
)

const (
	NASR2CopyFloor      int64 = 2_000_000_000_000
	NASR2CopyWorkers          = 4
	NASR2CopyRangeBytes int64 = 16 << 20
	NASR2CopyMaxFile    int64 = 64 << 30
	nasR2CopyLock       int64 = 7343226011192
)

type NASR2CopyMode string

const (
	NASR2OrdinaryCopy   NASR2CopyMode = "copy"
	NASR2VerifyExisting NASR2CopyMode = "verify_existing"
)

// The Python control transport waits 2s after sending. Start before auth and
// delivery fencing; leave 500ms for the response and network. One budget covers
// all retries, pool acquisition, source rechecks, R2 HEAD and commit.
const nasR2CopyControlBudget = 1500 * time.Millisecond

func nasR2CopyControlRequest(r *http.Request) bool {
	return strings.HasPrefix(r.URL.Path, "/api/v1/account/connections/r2-copy/") ||
		(r.URL.Path == "/api/v1/account/connections/inventory" && r.Header.Get("X-Stoarama-Copy-Control") == "inventory")
}

func nasR2CopyCommittedResponse(status int) bool { return status >= 200 && status < 300 }

func nasDeliveryRequestDeadline(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		budget := nasR2CopyControlBudget
		if r.Method == http.MethodGet && r.URL.Path == "/api/v1/account/collated" {
			budget = collatedFeedBudget
		} else if !nasR2CopyControlRequest(r) {
			next.ServeHTTP(w, r)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), budget)
		defer cancel()
		// Bound request body reads too: context cancellation alone cannot
		// interrupt net/http's JSON decoder while the peer stops sending.
		deadline, _ := ctx.Deadline()
		transport := http.NewResponseController(w)
		if err := transport.SetReadDeadline(deadline); err != nil {
			w.Header().Set("Connection", "close")
			util.WriteError(w, 503, "delivery read deadline unavailable; retryable")
			return
		}
		defer func() { _ = transport.Flush(); _ = transport.SetReadDeadline(time.Time{}) }()
		out := &nasDeliveryResponse{header: make(http.Header)}
		next.ServeHTTP(out, r.WithContext(ctx))
		if (ctx.Err() != nil || !time.Now().Before(deadline)) && !(nasR2CopyControlRequest(r) && nasR2CopyCommittedResponse(out.status)) {
			w.Header().Set("Retry-After", "1")
			util.WriteError(w, 503, "delivery request deadline; retryable")
			return
		}
		for k, v := range out.header {
			w.Header()[k] = v
		}
		if out.status != 0 {
			w.WriteHeader(out.status)
		}
		_, _ = w.Write(out.body.Bytes())
	})
}

var errNASR2CopyMaintenance = errors.New("copy yields to maintenance")

func nasR2CopyReadyError(w http.ResponseWriter, err error) {
	if errors.Is(err, errNASR2CopyMaintenance) {
		util.WriteError(w, 409, errNASR2CopyMaintenance.Error())
		return
	}
	util.WriteError(w, 503, "copy maintenance check unavailable; retryable")
}

type NASR2CopyKind string

const (
	NASR2CopyRaw          NASR2CopyKind = "raw"
	NASR2CopyCollated     NASR2CopyKind = "collated"
	NASR2CopyStitchOutput NASR2CopyKind = "stitch_output"
)

// NASR2CopyItem is transport-independent so collated custody can be added later.
// This release admits raw only; planning preserves collated entries as blocked.
type NASR2CopyItem struct {
	Kind         NASR2CopyKind `json:"kind"`
	SourceID     int64         `json:"source_id"`
	RecordingID  int64         `json:"recording_id"`
	StreamID     int64         `json:"stream_id"`
	Bucket       string        `json:"bucket"`
	ObjectKey    string        `json:"object_key"`
	RelativePath string        `json:"relative_path"`
	SizeBytes    int64         `json:"size_bytes"`
	SHA256       string        `json:"sha256"`
	StartAt      time.Time     `json:"start_at"`
	EndAt        time.Time     `json:"end_at"`
}

func ValidateNASR2CopyItem(i NASR2CopyItem) error {
	_, windows, err := Keep28Policy()
	if err != nil {
		return err
	}
	w, ok := windows[i.StreamID]
	if !ok || (i.Kind != NASR2CopyRaw && i.Kind != NASR2CopyCollated && i.Kind != NASR2CopyStitchOutput) || i.SourceID <= 0 || i.RecordingID <= 0 ||
		i.SizeBytes <= 0 || i.SizeBytes > NASR2CopyMaxFile || len(i.SHA256) != 64 || !lowerHex(i.SHA256) ||
		!validNASRelativePath(i.RelativePath) || (i.Kind != NASR2CopyStitchOutput && nasInventoryExcludedPath(i.RelativePath)) ||
		strings.ContainsRune(i.RelativePath, 0) || strings.HasPrefix(i.RelativePath, ".") ||
		i.StartAt.IsZero() || !i.EndAt.After(i.StartAt) || !i.StartAt.Before(w.End) || !i.EndAt.After(w.Start) {
		return errors.New("invalid copy identity or object outside pinned keep28; no item may be silently omitted")
	}
	for _, p := range strings.Split(i.RelativePath, "/") {
		if strings.HasPrefix(p, ".") {
			return errors.New("hidden copy path")
		}
	}
	if i.Kind == NASR2CopyRaw && (!strings.HasPrefix(i.ObjectKey, "managed/") || i.Bucket == "") {
		return errors.New("invalid raw copy key")
	}
	if i.Kind == NASR2CopyStitchOutput && (i.Bucket == "" || !strings.HasPrefix(i.ObjectKey, "stitch-staging/") ||
		!durhamStitchOutputPath(path.Dir(i.RelativePath), i.RelativePath, i.SHA256)) {
		return errors.New("invalid stitch output copy identity")
	}
	return nil
}

// LoadNASR2CopySource uses one primary-key lookup, never a catalog scan. The
// existing redownload guard supplies the same policy/hold/retirement vetoes.
var errNASR2CopySourceIdentity = errors.New("copy source identity changed")

// NASR2CopySourceReadError separates driver/read failures from source validation.
// Callers can preserve diagnostics without guessing the driver's error type.
type NASR2CopySourceReadError struct{ Err error }

func (e *NASR2CopySourceReadError) Error() string { return e.Err.Error() }
func (e *NASR2CopySourceReadError) Unwrap() error { return e.Err }

func LoadNASR2CopySource(ctx context.Context, tx pgx.Tx, i NASR2CopyItem) (json.RawMessage, error) {
	if err := ValidateNASR2CopyItem(i); err != nil {
		return nil, errors.Join(errNASR2CopySourceIdentity, err)
	}
	if i.Kind == NASR2CopyStitchOutput {
		if _, err := loadNASR2StagedObject(ctx, tx, i); err != nil {
			return nil, err
		}
		return json.Marshal(map[string]any{"clip_id": i.SourceID, "recording_id": i.RecordingID,
			"relative_path": i.RelativePath, "sha256": i.SHA256, "size_bytes": i.SizeBytes})
	}
	if i.Kind != NASR2CopyRaw {
		return nil, fmt.Errorf("%w: %s", errNASR2CopySourceIdentity, "collated custody requires follow-up support")
	}
	var stream, size int64
	var bucket, key, sha, tz, path string
	var start, end time.Time
	var purged, held bool
	var payload []byte
	var token *string
	err := tx.QueryRow(ctx, nasRedownloadSQL, i.SourceID, int64(47)).Scan(&stream, &bucket, &key, &sha, &size, &path, &start, &end, &tz, &purged, &held, &payload, &token)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("%w: copy source unavailable", errNASR2CopySourceIdentity)
		}
		return nil, &NASR2CopySourceReadError{Err: err}
	}
	_, windows, err := Keep28Policy()
	if err != nil {
		return nil, err
	}
	if stream != i.StreamID || bucket != i.Bucket || key != i.ObjectKey || sha != i.SHA256 || size != i.SizeBytes || path != i.RelativePath ||
		!start.Equal(i.StartAt) || !end.Equal(i.EndAt) || tz != windows[stream].Timezone || purged || held {
		return nil, fmt.Errorf("%w: %s", errNASR2CopySourceIdentity, "copy source changed, held, retired or outside policy")
	}
	// Raw and collated namespaces are disjoint (joined/ is refused above).
	// A second raw catalog identity at this path is ambiguous even if bytes agree.
	// The existing nas_delivery_raw_writer_path index bounds this lookup.
	var alias bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM recording_clips WHERE display_path<>'' AND display_path=$1 AND id<>$2)`, i.RelativePath, i.SourceID).Scan(&alias); err != nil {
		return nil, &NASR2CopySourceReadError{Err: err}
	}
	if alias {
		return nil, fmt.Errorf("%w: %s", errNASR2CopySourceIdentity, "copy path has another catalog identity")
	}
	var clip map[string]json.RawMessage
	var identity struct {
		RecordingID int64 `json:"recording_id"`
	}
	if json.Unmarshal(payload, &clip) != nil || json.Unmarshal(payload, &identity) != nil || identity.RecordingID != i.RecordingID {
		return nil, fmt.Errorf("%w: %s", errNASR2CopySourceIdentity, "copy recording changed")
	}
	// Match the normal downloader's provenance representation.
	clip["capture_generation"] = json.RawMessage("null")
	if token != nil {
		sum := sha256.Sum256([]byte(*token))
		clip["capture_generation"], _ = json.Marshal(fmt.Sprintf("sha256:%x", sum[:]))
	}
	out, err := json.Marshal(clip)
	return out, err
}

// Copy transactions have hard query/memory caps even with an operator DB URL.
func BeginNASR2CopyTx(ctx context.Context, pool interface {
	Begin(context.Context) (pgx.Tx, error)
}) (pgx.Tx, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	if _, err = tx.Exec(ctx, `SET LOCAL statement_timeout='10s'; SET LOCAL lock_timeout='2s'; SET LOCAL work_mem='16MB'`); err != nil {
		_ = tx.Rollback(ctx)
		return nil, err
	}
	return tx, nil
}

func (s *Server) nasR2CopyConnection(w http.ResponseWriter, r *http.Request, purpose nasConnectionPurpose) (int64, bool) {
	cid, ok := s.nasConnection(r.Context(), w, r, purpose)
	if !ok {
		return 0, false
	}
	if cid != 13 {
		util.WriteError(w, 403, "copy requires connection 13")
		return 0, false
	}
	if !s.cfg.NASR2CopyEnabled || s.r2 == nil {
		util.WriteError(w, 409, "R2 copy disabled")
		return 0, false
	}
	return cid, true
}

// Ordinary cleanup is outside the same pinned window that copy intersects.
// Maintenance remains exclusive; per-source holds below also veto special
// stitched-source retirement. No global plan/lease/permit veto is necessary.
func nasR2CopyReady(ctx context.Context, tx pgx.Tx) error {
	var busy bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM nas_cleanup_maintenance WHERE active)`).Scan(&busy); err != nil {
		return err
	}
	if busy {
		return errNASR2CopyMaintenance
	}
	return nil
}

type nasR2CopyControl struct {
	AcceptStitch    bool                 `json:"accept_stitch_output,omitempty"`
	StitchReceipt   *nasR2StitchReceipt  `json:"stitch_receipt,omitempty"`
	VerifyExisting  bool                 `json:"verify_existing"`
	Token           string               `json:"token"`
	RangeID         string               `json:"range_id,omitempty"`
	FreeBytes       int64                `json:"free_bytes"`
	Offset          int64                `json:"offset"`
	Length          int64                `json:"length"`
	Reason          NASR2CopyBlockReason `json:"reason"`
	Completion      NASR2CopyCompletion  `json:"completion"`
	DownloadedBytes int64                `json:"downloaded_bytes"`
	CommittedBytes  int64                `json:"committed_bytes"`
}
type nasR2CopyTask struct {
	WorkMode     NASR2CopyMode   `json:"work_mode"`
	ID           int64           `json:"id"`
	Token        string          `json:"token"`
	Item         NASR2CopyItem   `json:"item"`
	Clip         json.RawMessage `json:"clip"`
	ETag         string          `json:"etag"`
	PolicySHA256 string          `json:"policy_sha256"`
}

// Preparation holds only short database row locks. It commits before HEAD;
// admission and maintenance must revalidate everything that can change.
func (s *Server) nasR2CopyPrepare(w http.ResponseWriter, r *http.Request, cid int64, verify, stitch bool) (nasR2CopyTask, []byte, NASR2CopyBlockReason, bool) {
	var task nasR2CopyTask
	var data []byte
	ctx := r.Context()
	tx, err := BeginNASR2CopyTx(ctx, s.pool)
	if err != nil {
		util.WriteError(w, 503, "copy database unavailable")
		return task, nil, "", false
	}
	defer tx.Rollback(ctx)
	if err = nasR2CopyReady(ctx, tx); err != nil {
		nasR2CopyReadyError(w, err)
		return task, nil, "", false
	}
	var active int
	if err = tx.QueryRow(ctx, `SELECT count(*) FROM nas_r2_copy_items WHERE connection_id=$1 AND state IN ('leased','refresh_leased') AND lease_expires_at>clock_timestamp()`, cid).Scan(&active); err != nil {
		util.WriteError(w, 503, "copy lease unavailable")
		return task, nil, "", false
	}
	if active >= NASR2CopyWorkers {
		return task, nil, "", true
	}
	err = tx.QueryRow(ctx, `SELECT q.id,q.item,q.policy_sha256,q.work_mode FROM nas_r2_copy_items q CROSS JOIN r2_keep28_refresh_control control WHERE connection_id=$1 AND (kind='raw' OR ($3 AND kind='stitch_output')) AND
 (state='pending' OR (state='leased' AND lease_expires_at<clock_timestamp()) OR ($2 AND control.enabled AND (state='refresh_pending' OR (state='refresh_leased' AND lease_expires_at<clock_timestamp()))))
 ORDER BY (q.kind<>'stitch_output'),(q.work_mode=control.last_mode),CASE WHEN q.work_mode='verify_existing' THEN q.updated_at END,q.id LIMIT 1 FOR UPDATE OF q SKIP LOCKED`, cid, verify, stitch).Scan(&task.ID, &data, &task.PolicySHA256, &task.WorkMode)
	if errors.Is(err, pgx.ErrNoRows) {
		return task, nil, "", true
	}
	if err != nil || json.Unmarshal(data, &task.Item) != nil {
		util.WriteError(w, 503, "copy queue unavailable")
		return task, nil, "", false
	}
	policy, _, err := Keep28Policy()
	if err != nil || policy != task.PolicySHA256 {
		util.WriteError(w, 409, "copy policy changed")
		return task, nil, "", false
	}
	reason := NASR2CopyBlockReason("")
	if _, err = LoadNASR2CopySource(ctx, tx, task.Item); err != nil {
		if !errors.Is(err, errNASR2CopySourceIdentity) {
			util.WriteError(w, 503, "copy source lookup unavailable; retryable")
			return task, nil, "", false
		}
		reason = NASR2CopySourceChanged
	}
	if err = tx.Commit(ctx); err != nil {
		util.WriteError(w, 503, "copy preparation commit failed")
		return task, nil, "", false
	}
	return task, data, reason, true
}

func (s *Server) handleNASR2CopyLease(w http.ResponseWriter, r *http.Request) {
	cid, ok := s.nasR2CopyConnection(w, r, nasConnectionCopy)
	if !ok {
		return
	}
	var req nasR2CopyControl
	if util.DecodeJSON(r, &req) != nil || req.FreeBytes < NASR2CopyFloor+NASR2CopyRangeBytes {
		util.WriteError(w, 409, "copy free-space floor")
		return
	}
	// A concurrent grant can consume the prepared row. Retry a bounded number
	// of candidates without making the client hold for a normal admission race.
	for attempt := 0; attempt < NASR2CopyWorkers; attempt++ {
		if !s.nasR2CopyLeaseAttempt(w, r, cid, req.VerifyExisting, req.AcceptStitch) {
			return
		}
	}
	util.WriteError(w, 409, "copy queue changed during preparation; retryable")
}

func (s *Server) nasR2CopyLeaseAttempt(w http.ResponseWriter, r *http.Request, cid int64, verify, stitch bool) bool {
	task, data, reason, ok := s.nasR2CopyPrepare(w, r, cid, verify, stitch)
	if !ok {
		return false
	}
	if task.ID == 0 {
		util.WriteJSON(w, 200, map[string]any{"task": nil})
		return false
	}
	ctx := r.Context()
	var head r2.ObjectHead
	var err error
	// No transaction, row lock or advisory lock survives preparation. HEAD
	// can be slow or blocked without obstructing controls or the deploy fence.
	if reason == "" {
		head, err = s.r2.Head(ctx, task.Item.ObjectKey)
		if err != nil {
			if !r2.IsNotFound(err) {
				util.WriteError(w, 503, "copy R2 HEAD unavailable; retryable")
				return false
			}
			reason = NASR2CopyR2Identity
		} else if head.ETag == "" || head.SizeBytes != task.Item.SizeBytes || (head.SHA256Metadata != "" && !strings.EqualFold(head.SHA256Metadata, task.Item.SHA256)) {
			reason = NASR2CopyR2Identity
		}
	}
	nonce := make([]byte, 24)
	if reason == "" {
		if _, err = rand.Read(nonce); err != nil {
			util.WriteError(w, 503, "copy token unavailable")
			return false
		}
	}
	tx, err := BeginNASR2CopyTx(ctx, s.pool)
	if err != nil {
		util.WriteError(w, 503, "copy database unavailable")
		return false
	}
	defer tx.Rollback(ctx)
	// EXCLUSIVE is also the four-live-lease cap, not just a maintenance fence:
	// admission must recount only after concurrent renewals have committed.
	// Keep it solely across database revalidation and grant/block commit.
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, nasR2CopyLock); err != nil {
		util.WriteError(w, 503, "copy admission busy")
		return false
	}
	if err = nasR2CopyReady(ctx, tx); err != nil {
		nasR2CopyReadyError(w, err)
		return false
	}
	var active int
	if err = tx.QueryRow(ctx, `SELECT count(*) FROM nas_r2_copy_items WHERE connection_id=$1 AND state IN ('leased','refresh_leased') AND lease_expires_at>clock_timestamp()`, cid).Scan(&active); err != nil {
		util.WriteError(w, 503, "copy lease unavailable")
		return false
	}
	if active >= NASR2CopyWorkers {
		util.WriteJSON(w, 200, map[string]any{"task": nil})
		return false
	}
	var id int64
	err = tx.QueryRow(ctx, `SELECT id FROM nas_r2_copy_items WHERE id=$1 AND connection_id=$2 AND kind=$6 AND item=$3::jsonb AND policy_sha256=$4 AND
 (state='pending' OR (state='leased' AND lease_expires_at<clock_timestamp()) OR (state='refresh_pending' OR (state='refresh_leased' AND lease_expires_at<clock_timestamp()))) AND work_mode=$5 FOR UPDATE SKIP LOCKED`, task.ID, cid, data, task.PolicySHA256, task.WorkMode, task.Item.Kind).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return true
	}
	if err != nil {
		util.WriteError(w, 503, "copy queue unavailable")
		return false
	}
	if task.Clip, err = LoadNASR2CopySource(ctx, tx, task.Item); err != nil {
		if errors.Is(err, errNASR2CopySourceIdentity) {
			s.nasR2CopyBlockResponse(w, r, tx, task.ID, NASR2CopySourceChanged)
		} else {
			util.WriteError(w, 503, "copy source lookup unavailable; retryable")
		}
		return false
	}
	if task.Item.Kind == NASR2CopyStitchOutput && reason == "" {
		staged, err := loadNASR2StagedObject(ctx, tx, task.Item)
		if err != nil {
			util.WriteError(w, 503, "stitch copy generation unavailable")
			return false
		}
		if !staged.matches(head) {
			reason = NASR2CopyR2Identity
		}
	}
	if reason == NASR2CopySourceChanged {
		return true
	} // Source recovered; obtain a fresh HEAD.
	if reason != "" {
		s.nasR2CopyBlockResponse(w, r, tx, task.ID, reason)
		return false
	}
	tag, err := tx.Exec(ctx, `UPDATE r2_keep28_refresh_control SET last_mode=$1 WHERE singleton AND (enabled OR $1='copy')`, task.WorkMode)
	if err != nil || tag.RowsAffected() != 1 {
		util.WriteError(w, 409, "refresh disabled")
		return false
	}
	leaseState := "leased"
	if task.WorkMode == NASR2VerifyExisting {
		leaseState = "refresh_leased"
	}
	task.ETag, task.Token = strings.Trim(head.ETag, "\""), hex.EncodeToString(nonce)
	_, err = tx.Exec(ctx, `UPDATE nas_r2_copy_items SET state=$5,lease_token=$2,lease_expires_at=clock_timestamp()+interval '30 seconds',etag=$3,version_id=$4,attempts=attempts+1,last_error='',updated_at=clock_timestamp() WHERE id=$1`, task.ID, task.Token, task.ETag, head.VersionID, leaseState)
	if err != nil || tx.Commit(ctx) != nil {
		util.WriteError(w, 503, "copy lease commit failed")
		return false
	}
	util.WriteJSON(w, 200, map[string]any{"task": task, "floor_bytes": NASR2CopyFloor, "range_bytes": NASR2CopyRangeBytes})
	return false
}

func (s *Server) nasR2CopyActive(w http.ResponseWriter, r *http.Request, purpose nasConnectionPurpose, allowStitchReplay bool) (pgx.Tx, nasR2CopyTask, string, nasR2CopyControl, bool) {
	var t nasR2CopyTask
	var version string
	var req nasR2CopyControl
	if _, ok := s.nasR2CopyConnection(w, r, purpose); !ok {
		return nil, t, version, req, false
	}
	id, ok := parseInt64Path(w, r, "id")
	if !ok {
		return nil, t, version, req, false
	}
	if util.DecodeJSON(r, &req) != nil || req.Token == "" || req.FreeBytes < NASR2CopyFloor+NASR2CopyRangeBytes {
		util.WriteError(w, 409, "copy controls unavailable")
		return nil, t, version, req, false
	}
	ctx := r.Context()
	tx, err := BeginNASR2CopyTx(ctx, s.pool)
	if err != nil {
		util.WriteError(w, 503, "copy database unavailable")
		return nil, t, version, req, false
	}
	fail := func() { tx.Rollback(ctx) }
	// Renewals may run together, but admission must count only after every
	// concurrent renewal commits. Always take this before the item row lock.
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock_shared($1)`, nasR2CopyLock); err != nil {
		fail()
		util.WriteError(w, 503, "copy admission busy")
		return nil, t, version, req, false
	}

	if err = nasR2CopyReady(ctx, tx); err != nil {
		fail()
		nasR2CopyReadyError(w, err)
		return nil, t, version, req, false
	}
	var data []byte
	err = tx.QueryRow(ctx, `SELECT id,item,policy_sha256,etag,coalesce(version_id,''),work_mode FROM nas_r2_copy_items WHERE id=$1 AND connection_id=13 AND state IN ('leased','refresh_leased') AND lease_token=$2 AND lease_expires_at>clock_timestamp() FOR UPDATE`, id, req.Token).Scan(&t.ID, &data, &t.PolicySHA256, &t.ETag, &version, &t.WorkMode)

	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		fail()
		util.WriteError(w, 503, "copy lease lookup unavailable; retryable")
		return nil, t, version, req, false
	}

	if errors.Is(err, pgx.ErrNoRows) && allowStitchReplay && req.StitchReceipt != nil {
		var replay bool
		replayErr := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM nas_r2_copy_items
 WHERE id=$1 AND connection_id=13 AND kind='stitch_output' AND state='verified' AND stitch_result_sha256=$2)`,
			id, nasR2StitchResultSHA(req)).Scan(&replay)
		if replayErr != nil || replay {
			fail()
			if replayErr != nil {
				util.WriteError(w, 503, "copy receipt lookup unavailable; retryable")
			} else {
				util.WriteJSON(w, 200, map[string]bool{"verified": true})
			}
			return nil, t, version, req, false
		}
	}

	policy, _, pe := Keep28Policy()
	if pe != nil {
		fail()
		util.WriteError(w, 503, "copy policy unavailable; retryable")
		return nil, t, version, req, false
	}
	if err != nil || json.Unmarshal(data, &t.Item) != nil || policy != t.PolicySHA256 {
		fail()
		util.WriteError(w, 409, "copy lease or policy expired")
		return nil, t, version, req, false
	}
	if t.Clip, err = LoadNASR2CopySource(ctx, tx, t.Item); err != nil {
		fail()
		if !errors.Is(err, errNASR2CopySourceIdentity) {
			util.WriteError(w, 503, "copy source lookup unavailable; retryable")
			return nil, t, version, req, false
		}
		util.WriteError(w, 409, "copy source changed")
		return nil, t, version, req, false
	}
	t.Token = req.Token
	return tx, t, version, req, true
}
func (s *Server) handleNASR2CopyCheckpoint(w http.ResponseWriter, r *http.Request) {
	tx, t, _, req, ok := s.nasR2CopyActive(w, r, nasConnectionMetadata, false)
	if !ok {
		return
	}
	defer tx.Rollback(r.Context())
	if req.RangeID != "" {
		tag, err := tx.Exec(r.Context(), `UPDATE nas_r2_copy_items SET range_until=NULL,range_id=NULL,downloaded_bytes=downloaded_bytes+range_length,range_length=0,updated_at=clock_timestamp() WHERE id=$1 AND range_id=$2`, t.ID, req.RangeID)
		if err != nil || tag.RowsAffected() != 1 {
			util.WriteError(w, 409, "copy range completion is stale")
			return
		}
	}
	_, err := tx.Exec(r.Context(), `UPDATE nas_r2_copy_items SET lease_expires_at=clock_timestamp()+interval '30 seconds' WHERE id=$1`, t.ID)
	if err != nil || tx.Commit(r.Context()) != nil {
		util.WriteError(w, 503, "copy checkpoint failed")
		return
	}
	util.WriteJSON(w, 200, map[string]any{"active": true})
}

func (s *Server) handleNASR2CopyRange(w http.ResponseWriter, r *http.Request) {
	tx, t, version, req, ok := s.nasR2CopyActive(w, r, nasConnectionCopy, false)
	if !ok {
		return
	}
	defer tx.Rollback(r.Context())
	if t.WorkMode == NASR2VerifyExisting {
		util.WriteError(w, 409, "verify-existing never downloads")
		return
	}
	if req.Offset < 0 || req.Length <= 0 || req.Length > NASR2CopyRangeBytes || req.Offset > t.Item.SizeBytes-req.Length {
		util.WriteError(w, 400, "invalid copy range")
		return
	}
	issuedAt := time.Now()
	// One in-flight range per item, independent of every other file.
	var ready bool
	if err := tx.QueryRow(r.Context(), `SELECT coalesce(range_until<=now(),true) FROM nas_r2_copy_items WHERE id=$1`, t.ID).Scan(&ready); err != nil || !ready {
		util.WriteError(w, 409, "copy transfer already active")
		return
	}
	nonce := make([]byte, 24)
	if _, err := rand.Read(nonce); err != nil {
		util.WriteError(w, 503, "copy range identity unavailable")
		return
	}
	rangeID := hex.EncodeToString(nonce)
	if _, err := tx.Exec(r.Context(), `UPDATE nas_r2_copy_items SET range_until=$2,range_id=$3,range_length=$4,lease_expires_at=clock_timestamp()+interval '30 seconds' WHERE id=$1`, t.ID, issuedAt.Add(10*time.Second), rangeID, req.Length); err != nil || tx.Commit(r.Context()) != nil {
		util.WriteError(w, 503, "copy range commit failed")
		return
	}
	// Range+If-Match are signed: the client cannot expand this grant. URLs
	// are ephemeral, never journaled, and consumed by one non-retrying GET only.
	ctx, cancel := context.WithDeadline(r.Context(), issuedAt.Add(10*time.Second))
	defer cancel()
	cap, err := s.r2.PresignGetExactRangeRequest(ctx, t.Item.ObjectKey, t.ETag, version, req.Offset, req.Length, 10*time.Second)
	if err != nil {
		util.WriteError(w, 503, "copy range capability unavailable")
		return
	}
	headers := map[string]string{}
	for k, v := range cap.Headers {
		if len(v) > 0 && http.CanonicalHeaderKey(k) != "Host" {
			headers[k] = v[0]
		}
	}
	util.WriteJSON(w, 200, map[string]any{"url": cap.URL, "headers": headers, "etag": t.ETag, "offset": req.Offset, "length": req.Length, "max_duration_sec": 10, "range_id": rangeID})
}
func (s *Server) handleNASR2CopyResult(w http.ResponseWriter, r *http.Request) {
	tx, t, _, req, ok := s.nasR2CopyActive(w, r, nasConnectionMetadata, true)
	if !ok {
		return
	}
	defer tx.Rollback(r.Context())
	if req.Completion != "" && req.Completion != NASR2CopyVerifiedInPlace && req.Completion != NASR2CopyDownloaded ||
		req.DownloadedBytes < 0 || req.DownloadedBytes > t.Item.SizeBytes || req.CommittedBytes < 0 ||
		(req.Completion == "" && (req.DownloadedBytes != 0 || req.CommittedBytes != 0)) ||
		(req.Completion == NASR2CopyVerifiedInPlace && (req.DownloadedBytes != 0 || req.CommittedBytes != 0)) ||
		(req.Completion == NASR2CopyDownloaded && req.CommittedBytes != t.Item.SizeBytes) {
		util.WriteError(w, 400, "invalid copy completion counters")
		return
	}
	if t.WorkMode == NASR2VerifyExisting && (req.Completion != NASR2CopyVerifiedInPlace || req.DownloadedBytes != 0 || req.CommittedBytes != 0) {
		util.WriteError(w, 400, "refresh requires verify-in-place without download")
		return
	}
	if t.Item.Kind == NASR2CopyStitchOutput {
		if err := finishNASR2StitchCopy(r.Context(), tx, t, req); err != nil || tx.Commit(r.Context()) != nil {
			util.WriteError(w, 409, "exact NAS stitch readback unavailable or certificate busy")
			return
		}
		util.WriteJSON(w, 200, map[string]any{"verified": true})
		return
	}
	if req.StitchReceipt != nil {
		util.WriteError(w, 400, "stitch receipt supplied for raw copy")
		return
	}
	// Existing inventory publication is the custody proof, never a result claim.
	var exact bool
	err := tx.QueryRow(r.Context(), `SELECT EXISTS(SELECT 1 FROM nas_inventory_files WHERE connection_id=13 AND clip_id=$1 AND relative_path=$2 AND sha256=$3 AND size_bytes=$4 AND state='present' AND verified_at BETWEEN now()-interval '5 minutes' AND now()+interval '5 minutes' AND server_received_at>=now()-interval '5 minutes' AND sidecar_relative_path=$2||'.stoarama.json' AND sidecar_size_bytes>0 AND sidecar_sha256 ~ '^[a-f0-9]{64}$')`, t.Item.SourceID, t.Item.RelativePath, t.Item.SHA256, t.Item.SizeBytes).Scan(&exact)
	if err != nil || !exact {
		util.WriteError(w, 409, "copy requires exact NAS inventory proof")
		return
	}
	if t.WorkMode == NASR2VerifyExisting {
		_, err = tx.Exec(r.Context(), `UPDATE nas_r2_copy_items SET state='verified',lease_token=NULL,lease_expires_at=NULL,custody_refreshed_at=clock_timestamp(),refresh_count=refresh_count+1,updated_at=clock_timestamp() WHERE id=$1`, t.ID)
	} else {
		_, err = tx.Exec(r.Context(), `UPDATE nas_r2_copy_items SET state='verified',verified_at=now(),lease_token=NULL,lease_expires_at=NULL,completion=$2,committed_bytes=$3,updated_at=clock_timestamp() WHERE id=$1`, t.ID, req.Completion, req.CommittedBytes)
	}
	if err != nil || tx.Commit(r.Context()) != nil {
		util.WriteError(w, 503, "copy result commit failed")
		return
	}
	util.WriteJSON(w, 200, map[string]any{"verified": true})
}
