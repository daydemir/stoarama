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
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/util"
	"github.com/jackc/pgx/v5"
)

const (
	NASR2CopyFloor      int64 = 2_000_000_000_000
	NASR2CopyDailyBytes int64 = 100_000_000_000
	NASR2CopyRate       int64 = 2_000_000
	NASR2CopyRangeBytes int64 = 8 << 20
	NASR2CopyMaxFile    int64 = 64 << 30
	nasR2CopyLock       int64 = 7343226011192
)

type NASR2CopyKind string

const (
	NASR2CopyRaw      NASR2CopyKind = "raw"
	NASR2CopyCollated NASR2CopyKind = "collated"
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
	if !ok || (i.Kind != "raw" && i.Kind != "collated") || i.SourceID <= 0 || i.RecordingID <= 0 ||
		i.SizeBytes <= 0 || i.SizeBytes > NASR2CopyMaxFile || len(i.SHA256) != 64 || !lowerHex(i.SHA256) ||
		!validNASRelativePath(i.RelativePath) || nasInventoryExcludedPath(i.RelativePath) ||
		strings.ContainsRune(i.RelativePath, 0) || strings.HasPrefix(i.RelativePath, ".") ||
		i.StartAt.IsZero() || !i.EndAt.After(i.StartAt) || !i.StartAt.Before(w.End) || !i.EndAt.After(w.Start) {
		return errors.New("invalid copy identity or object outside pinned keep28; no item may be silently omitted")
	}
	for _, p := range strings.Split(i.RelativePath, "/") {
		if strings.HasPrefix(p, ".") {
			return errors.New("hidden copy path")
		}
	}
	if i.Kind == "raw" && (!strings.HasPrefix(i.ObjectKey, "managed/") || i.Bucket == "") {
		return errors.New("invalid raw copy key")
	}
	return nil
}

// LoadNASR2CopySource uses one primary-key lookup, never a catalog scan. The
// existing redownload guard supplies the same policy/hold/retirement vetoes.
func LoadNASR2CopySource(ctx context.Context, tx pgx.Tx, i NASR2CopyItem) (json.RawMessage, error) {
	if err := ValidateNASR2CopyItem(i); err != nil {
		return nil, err
	}
	if i.Kind != "raw" {
		return nil, errors.New("collated custody requires follow-up support")
	}
	var stream, size int64
	var bucket, key, sha, tz, path string
	var start, end time.Time
	var purged, held bool
	var payload []byte
	var token *string
	err := tx.QueryRow(ctx, nasRedownloadSQL, i.SourceID, int64(47)).Scan(&stream, &bucket, &key, &sha, &size, &path, &start, &end, &tz, &purged, &held, &payload, &token)
	if err != nil {
		return nil, errors.New("copy source unavailable")
	}
	_, windows, err := Keep28Policy()
	if err != nil {
		return nil, err
	}
	if stream != i.StreamID || bucket != i.Bucket || key != i.ObjectKey || sha != i.SHA256 || size != i.SizeBytes || path != i.RelativePath ||
		!start.Equal(i.StartAt) || !end.Equal(i.EndAt) || tz != windows[stream].Timezone || purged || held {
		return nil, errors.New("copy source changed, held, retired or outside policy")
	}
	var clip map[string]json.RawMessage
	var identity struct {
		RecordingID int64 `json:"recording_id"`
	}
	if json.Unmarshal(payload, &clip) != nil || json.Unmarshal(payload, &identity) != nil || identity.RecordingID != i.RecordingID {
		return nil, errors.New("copy recording changed")
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

func (s *Server) nasR2CopyConnection(w http.ResponseWriter, r *http.Request) (int64, bool) {
	cid, ok := s.nasRestoreConnection(r.Context(), w, r)
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

// This lane only runs between cleanup batches. Also veto in-flight work from
// paused plans, so turning a plan off never manufactures copy headroom.
func nasR2CopyIdle(ctx context.Context, tx pgx.Tx) error {
	var busy bool
	err := tx.QueryRow(ctx, `SELECT
 EXISTS(SELECT 1 FROM nas_cleanup_plans WHERE connection_id=13 AND mode='enabled') OR
 EXISTS(SELECT 1 FROM nas_cleanup_items i JOIN nas_cleanup_plans p ON p.id=i.plan_id WHERE p.connection_id=13
 AND (i.state='delete_issued' OR (i.state='leased' AND i.lease_expires_at>now()))) OR
 EXISTS(SELECT 1 FROM nas_cleanup_batches WHERE connection_id=13 AND mode='enabled') OR
 EXISTS(SELECT 1 FROM nas_cleanup_maintenance WHERE active)`).Scan(&busy)
	if err != nil {
		return err
	}
	if busy {
		return errors.New("copy yields to cleanup")
	}
	return nil
}

type nasR2CopyControl struct {
	Token     string `json:"token"`
	RangeID   string `json:"range_id,omitempty"`
	FreeBytes int64  `json:"free_bytes"`
	Offset    int64  `json:"offset"`
	Length    int64  `json:"length"`
}
type nasR2CopyTask struct {
	ID           int64           `json:"id"`
	Token        string          `json:"token"`
	Item         NASR2CopyItem   `json:"item"`
	Clip         json.RawMessage `json:"clip"`
	ETag         string          `json:"etag"`
	PolicySHA256 string          `json:"policy_sha256"`
}

func (s *Server) handleNASR2CopyLease(w http.ResponseWriter, r *http.Request) {
	cid, ok := s.nasR2CopyConnection(w, r)
	if !ok {
		return
	}
	var req nasR2CopyControl
	if util.DecodeJSON(r, &req) != nil || req.FreeBytes < NASR2CopyFloor+NASR2CopyRangeBytes {
		util.WriteError(w, 409, "copy free-space floor")
		return
	}
	ctx := r.Context()
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
	if err = nasR2CopyIdle(ctx, tx); err != nil {
		util.WriteError(w, 409, "copy yields to cleanup")
		return
	}
	var busy bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM nas_r2_copy_items WHERE connection_id=$1 AND state='leased' AND lease_expires_at>now())`, cid).Scan(&busy); err != nil {
		util.WriteError(w, 503, "copy lease unavailable")
		return
	}
	if busy {
		util.WriteJSON(w, 200, map[string]any{"task": nil})
		return
	}
	var task nasR2CopyTask
	var data []byte
	err = tx.QueryRow(ctx, `SELECT id,item,policy_sha256 FROM nas_r2_copy_items WHERE connection_id=$1 AND kind='raw' AND
 (state='pending' OR (state='leased' AND lease_expires_at<now())) ORDER BY id LIMIT 1 FOR UPDATE`, cid).Scan(&task.ID, &data, &task.PolicySHA256)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteJSON(w, 200, map[string]any{"task": nil})
		return
	}
	if err != nil || json.Unmarshal(data, &task.Item) != nil {
		util.WriteError(w, 503, "copy queue unavailable")
		return
	}
	policy, _, err := Keep28Policy()
	if err != nil || policy != task.PolicySHA256 {
		util.WriteError(w, 409, "copy policy changed")
		return
	}
	if task.Clip, err = LoadNASR2CopySource(ctx, tx, task.Item); err != nil {
		_, _ = tx.Exec(ctx, `UPDATE nas_r2_copy_items SET state='blocked',last_error=$2 WHERE id=$1`, task.ID, err.Error())
		_ = tx.Commit(ctx)
		util.WriteError(w, 409, "copy source blocked; inspect queue")
		return
	}
	head, err := s.r2.Head(ctx, task.Item.ObjectKey)
	if err != nil || head.ETag == "" || head.SizeBytes != task.Item.SizeBytes || (head.SHA256Metadata != "" && !strings.EqualFold(head.SHA256Metadata, task.Item.SHA256)) {
		util.WriteError(w, 409, "copy R2 identity unavailable")
		return
	}
	task.ETag = strings.Trim(head.ETag, "\"")
	b := make([]byte, 24)
	if _, err = rand.Read(b); err != nil {
		util.WriteError(w, 503, "copy token unavailable")
		return
	}
	task.Token = hex.EncodeToString(b)
	_, err = tx.Exec(ctx, `UPDATE nas_r2_copy_items SET state='leased',lease_token=$2,lease_expires_at=now()+interval '30 seconds',etag=$3,version_id=$4,attempts=attempts+1,last_error='' WHERE id=$1`, task.ID, task.Token, task.ETag, head.VersionID)
	if err != nil || tx.Commit(ctx) != nil {
		util.WriteError(w, 503, "copy lease commit failed")
		return
	}
	util.WriteJSON(w, 200, map[string]any{"task": task, "floor_bytes": NASR2CopyFloor, "bytes_per_sec": NASR2CopyRate, "range_bytes": NASR2CopyRangeBytes})
}

func (s *Server) nasR2CopyActive(w http.ResponseWriter, r *http.Request) (pgx.Tx, nasR2CopyTask, string, nasR2CopyControl, bool) {
	var t nasR2CopyTask
	var version string
	var req nasR2CopyControl
	if _, ok := s.nasR2CopyConnection(w, r); !ok {
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
	// Same singleton as leasing: no new owner or simultaneous range can pass.
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, nasR2CopyLock); err != nil {
		fail()
		util.WriteError(w, 503, "copy admission busy")
		return nil, t, version, req, false
	}
	if err = nasR2CopyIdle(ctx, tx); err != nil {
		fail()
		util.WriteError(w, 409, "copy yields to cleanup")
		return nil, t, version, req, false
	}
	var data []byte
	err = tx.QueryRow(ctx, `SELECT id,item,policy_sha256,etag,coalesce(version_id,'') FROM nas_r2_copy_items WHERE id=$1 AND connection_id=13 AND state='leased' AND lease_token=$2 AND lease_expires_at>now() FOR UPDATE`, id, req.Token).Scan(&t.ID, &data, &t.PolicySHA256, &t.ETag, &version)
	policy, _, pe := Keep28Policy()
	if err != nil || json.Unmarshal(data, &t.Item) != nil || pe != nil || policy != t.PolicySHA256 {
		fail()
		util.WriteError(w, 409, "copy lease or policy expired")
		return nil, t, version, req, false
	}
	if t.Clip, err = LoadNASR2CopySource(ctx, tx, t.Item); err != nil {
		fail()
		util.WriteError(w, 409, "copy source changed")
		return nil, t, version, req, false
	}
	t.Token = req.Token
	return tx, t, version, req, true
}
func (s *Server) handleNASR2CopyCheckpoint(w http.ResponseWriter, r *http.Request) {
	tx, t, _, req, ok := s.nasR2CopyActive(w, r)
	if !ok {
		return
	}
	defer tx.Rollback(r.Context())
	if req.RangeID != "" {
		tag, err := tx.Exec(r.Context(), `UPDATE nas_r2_copy_items SET range_until=NULL,range_id=NULL WHERE id=$1 AND range_id=$2`, t.ID, req.RangeID)
		if err != nil || tag.RowsAffected() != 1 {
			util.WriteError(w, 409, "copy range completion is stale")
			return
		}
	}
	_, err := tx.Exec(r.Context(), `UPDATE nas_r2_copy_items SET lease_expires_at=now()+interval '30 seconds' WHERE id=$1`, t.ID)
	if err != nil || tx.Commit(r.Context()) != nil {
		util.WriteError(w, 503, "copy checkpoint failed")
		return
	}
	util.WriteJSON(w, 200, map[string]any{"active": true})
}

// Reserve every single-use client GET (including retries) before issuing a
// signed exact range. Ten-second URL TTL plus ten-second client work deadline
// fit inside the conservative 30s window charged to BOTH UTC dates.
func nasR2CopyCharge(ctx context.Context, tx pgx.Tx, n int64, now time.Time) error {
	day := now.UTC().Truncate(24 * time.Hour)
	days := []time.Time{day}
	if day.Add(24*time.Hour).Sub(now.UTC()) <= 30*time.Second {
		days = append(days, day.Add(24*time.Hour))
	}
	for _, d := range days {
		tag, err := tx.Exec(ctx, `INSERT INTO nas_r2_copy_budgets(connection_id,day,charged_bytes) VALUES(13,$1,$2)
 ON CONFLICT(connection_id,day) DO UPDATE SET charged_bytes=nas_r2_copy_budgets.charged_bytes+excluded.charged_bytes
 WHERE nas_r2_copy_budgets.charged_bytes+excluded.charged_bytes<=$3`, d, n, NASR2CopyDailyBytes)
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return errors.New("daily copy budget exhausted")
		}
	}
	return nil
}
func (s *Server) handleNASR2CopyRange(w http.ResponseWriter, r *http.Request) {
	tx, t, version, req, ok := s.nasR2CopyActive(w, r)
	if !ok {
		return
	}
	defer tx.Rollback(r.Context())
	if req.Offset < 0 || req.Length <= 0 || req.Length > NASR2CopyRangeBytes || req.Offset > t.Item.SizeBytes-req.Length {
		util.WriteError(w, 400, "invalid copy range")
		return
	}
	issuedAt := time.Now()
	if err := nasR2CopyCharge(r.Context(), tx, req.Length, issuedAt); err != nil {
		util.WriteError(w, 429, "daily copy budget exhausted")
		return
	}
	// Refuse overlapping transfers even from a stale client. Any attempt moves
	// this gate 10s forward; a timed-out request keeps its charge and cooldown.
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
	if _, err := tx.Exec(r.Context(), `UPDATE nas_r2_copy_items SET range_until=$2,range_id=$3,lease_expires_at=now()+interval '30 seconds' WHERE id=$1`, t.ID, issuedAt.Add(10*time.Second), rangeID); err != nil || tx.Commit(r.Context()) != nil {
		util.WriteError(w, 503, "copy budget commit failed")
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
	tx, t, _, _, ok := s.nasR2CopyActive(w, r)
	if !ok {
		return
	}
	defer tx.Rollback(r.Context())
	// Existing inventory publication is the custody proof, never a result claim.
	var exact bool
	err := tx.QueryRow(r.Context(), `SELECT EXISTS(SELECT 1 FROM nas_inventory_files WHERE connection_id=13 AND clip_id=$1 AND relative_path=$2 AND sha256=$3 AND size_bytes=$4 AND state='present' AND verified_at BETWEEN now()-interval '5 minutes' AND now()+interval '5 minutes' AND server_received_at>=now()-interval '5 minutes' AND sidecar_relative_path=$2||'.stoarama.json' AND sidecar_size_bytes>0 AND sidecar_sha256 ~ '^[a-f0-9]{64}$')`, t.Item.SourceID, t.Item.RelativePath, t.Item.SHA256, t.Item.SizeBytes).Scan(&exact)
	if err != nil || !exact {
		util.WriteError(w, 409, "copy requires exact NAS inventory proof")
		return
	}
	_, err = tx.Exec(r.Context(), `UPDATE nas_r2_copy_items SET state='verified',verified_at=now(),lease_token=NULL,lease_expires_at=NULL WHERE id=$1`, t.ID)
	if err != nil || tx.Commit(r.Context()) != nil {
		util.WriteError(w, 503, "copy result commit failed")
		return
	}
	util.WriteJSON(w, 200, map[string]any{"verified": true})
}
