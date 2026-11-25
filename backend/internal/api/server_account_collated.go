package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/daydemir/stoarama/backend/internal/nasdelivery"
	"log"
	"net/http"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/util"
	"github.com/jackc/pgx/v5"
)

// Generation-2 collated hour delivery (docs/NAS_JOINED_DELIVERY.md). The NAS
// pulls recording_collation_outputs to /clips/joined/<nas_relative_path> at a
// server-set rate, beside (never yielding to) raw delivery.

// The feed's per-request budgets. Variables only so the slow-CI integration
// test can widen them; production never changes them.
var (
	collatedFeedBudget           = 3 * time.Second
	collatedFeedStatementTimeout = "2s"
)

const (
	collatedFeedFenceLimit = 50
	// Candidates examined per feed request. In-window outputs sit in id runs with
	// 20k-id gaps; one request must cross a gap. Production worst case ~0.7s of the 2s budget.
	collatedFeedScanLimit    = 20000
	collatedFeedMaxLimit     = 200
	collatedFeedDefaultLimit = 50
	collatedDownloadTTL      = time.Hour
	collatedMaxErrorBytes    = 1000
	collatedMinBytesPerSec   = 1 << 20
	collatedMaxBytesPerSec   = 1 << 30
	collatedMaxParallel      = 32
	collatedDefaultPerSec    = 32 << 20
	collatedDefaultParallel  = 4
)

// collatedNASPathRe pins the NAS path contract: <folder>/<Month>/<DD-Weekday>/
// <name>_hour_<HH>[_dst2][_part_NN]_<HHMMSS>-<HHMMSS>.mp4 or the hour manifest.
var collatedNASPathRe = regexp.MustCompile(
	`^[^/]+/(January|February|March|April|May|June|July|August|September|October|November|December)/` +
		`(0[1-9]|[12][0-9]|3[01])-(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)/` +
		`[^/]+_hour_([01][0-9]|2[0-3])(_dst2)?((_part_[0-9]{2})?_[0-9]{6}-[0-9]{6}\.mp4|\.manifest\.json)$`)

// collatedV1NASPathRe is the stoarama_v1 (non-plaza) shape:
// <folder>/<recording>/joined/<YYYY-MM-DD>/<recording>_<YYYY-MM-DD>_hour_<HH>[_part_NN]_<HHMMSS>-<HHMMSS>.mp4
var collatedV1NASPathRe = regexp.MustCompile(
	`^[^/]+/([1-9][0-9]*)/joined/([0-9]{4}-[0-9]{2}-[0-9]{2})/([1-9][0-9]*)_([0-9]{4}-[0-9]{2}-[0-9]{2})_hour_([01][0-9]|2[0-3])(_part_[0-9]{2})?_[0-9]{6}-[0-9]{6}\.mp4$`)

// ValidCollatedNASPath reports whether rel is a safe relative path that obeys
// the collated NAS path contract.
func ValidCollatedNASPath(rel string) bool {
	if rel == "" || rel != strings.TrimSpace(rel) || strings.HasPrefix(rel, "/") || strings.Contains(rel, "\\") ||
		path.Clean(rel) != rel || strings.HasPrefix(rel, "joined/") || !validNASRelativePath(rel) {
		return false
	}
	for _, part := range strings.Split(rel, "/") {
		if part == "" || part == "." || part == ".." || strings.HasPrefix(part, ".") {
			return false
		}
	}
	if collatedNASPathRe.MatchString(rel) {
		return true
	}
	m := collatedV1NASPathRe.FindStringSubmatch(rel)
	return m != nil && m[1] == m[3] && m[2] == m[4]
}

type collatedPolicy struct {
	Enabled             bool  `json:"enabled"`
	DownloadBytesPerSec int64 `json:"download_bytes_per_sec"`
	DownloadParallel    int   `json:"download_parallel"`
	HandoffCheck        bool  `json:"handoff_check,omitempty"`
}

type collatedItem struct {
	OutputID        int64     `json:"output_id"`
	NASRelativePath string    `json:"nas_relative_path"`
	SizeBytes       int64     `json:"size_bytes"`
	SHA256          string    `json:"sha256"`
	DownloadPath    string    `json:"download_path"`
	HandoffRound    *int64    `json:"handoff_readback_round,omitempty"`
	HandoffMount    *[2]int64 `json:"handoff_mount,omitempty"`
}

// collatedKeep20FreeBytes stops in-window collated delivery at the R2->NAS copy
// floor, plus room for one in-flight feed page, so collated never starves raw copy.
const collatedKeep20FreeBytes = NASR2CopyFloor + 64<<30

// collatedGateSQL is the connection-wide gate for in-window collated delivery:
// the 20-day epoch is on, free space is above the floor and the storage report
// is fresh. While it is closed the feed returns nothing and resets the cursor,
// so outputs withheld by the gate are never skipped (a 15-minute heartbeat
// outage once advanced the cursor past ~2.9 TB of them).
var collatedGateSQL = `(nas_delivery_work_epoch(conn.id)
	       AND conn.nas_storage_free_bytes>=` + strconv.FormatInt(collatedKeep20FreeBytes, 10) + `
	       AND conn.nas_storage_reported_at>now()-interval '5 minutes')`

// collatedEligibleSQL selects outputs this connection still needs: newest
// generation of each recording hour, nas_pull recordings of the connection's
// account, R2-verified, not yet acknowledged by this connection, and not
// retired from R2 (r2-retire). An output qualifies by a collated-only hold or,
// under the 20-day NAS epoch, by overlapping its stream's pinned window (the
// NAS reuses those outputs instead of re-stitching; nas_delivery_allowed
// applies the exact window rule). $1=connection.
var collatedEligibilitySQL = `
	JOIN recording_collation_hours h ON h.id=o.collation_hour_id
	JOIN recordings r ON r.id=h.recording_id
	JOIN connections conn ON conn.id=$1 AND conn.kind='nas_pull' AND r.account_id=conn.account_id
	LEFT JOIN delivery_nas_handoff_copyable handoff ON conn.id=13 AND $2::boolean AND conn.delivery_nas_handoff_enabled AND handoff.output_id=o.id
	  AND handoff.roster_sha256=conn.delivery_nas_handoff_roster_sha256
 AND NOT EXISTS (SELECT 1 FROM delivery_nas_handoff_errors error
   WHERE error.output_id=o.id AND error.round=handoff.round AND error.next_attempt_at>now())
	WHERE r.delivery='nas_pull'
	  AND (
	   handoff.output_id IS NOT NULL OR (
	     conn.collated_delivery_enabled
	     AND (EXISTS (SELECT 1 FROM clip_storage_billing_contracts hold
	      JOIN unnest(o.source_clip_ids) source_id ON source_id=hold.clip_id WHERE hold.mode='` + nasdelivery.HoldContractMode + `')
	      OR (` + collatedGateSQL + `
	       AND EXISTS (SELECT 1 FROM nas_media_keep20_windows w
	        WHERE w.stream_id=r.stream_id AND o.start_at<w.end_at AND o.end_at>w.start_at)))
	     AND NOT EXISTS (SELECT 1 FROM recording_collation_hours newer
	      WHERE newer.recording_id=h.recording_id AND newer.local_date=h.local_date
	       AND newer.delivery_hour=h.delivery_hour AND newer.generation>h.generation)
	   )
	  )
	  AND CASE WHEN handoff.output_id IS NOT NULL THEN NOT EXISTS (
	    SELECT 1 FROM delivery_nas_handoff_readbacks receipt WHERE receipt.output_id=o.id AND receipt.round=handoff.round
	  ) ELSE NOT EXISTS (
	    SELECT 1 FROM recording_collation_output_acks ack WHERE ack.output_id=o.id AND ack.connection_id=$1
	  ) END
	  -- Outputs retired from R2 by stoaramactl r2-retire are gone for good.
	  AND NOT EXISTS (SELECT 1 FROM r2_object_retirements retired
	    WHERE retired.kind='collated' AND retired.source_id=o.id)`

var collatedEligibleSQL = `FROM recording_collation_outputs o ` + collatedEligibilitySQL + ` AND nas_delivery_allowed(conn.id,'collated',o.id)`

// Bound expensive source and fence checks to one PK-ordered candidate page.
// Materialization keeps the planner from running the fence on the full catalog.
// The cursor covers candidates, including withheld outputs, so an ineligible
// prefix can never starve later deliverable outputs. Downloads recheck eligibility.
var collatedFeedSQL = `WITH page AS MATERIALIZED (
 SELECT o.id FROM recording_collation_outputs o
 JOIN recording_collation_hours h ON h.id=o.collation_hour_id
 JOIN recordings r ON r.id=h.recording_id
 JOIN connections conn ON conn.id=$1 AND conn.kind='nas_pull' AND conn.account_id=r.account_id
 WHERE r.delivery='nas_pull' AND (conn.collated_delivery_enabled OR ($2::boolean AND conn.delivery_nas_handoff_enabled)) AND o.id>$3
 ORDER BY o.id LIMIT ` + strconv.Itoa(collatedFeedScanLimit) + `
), eligible AS MATERIALIZED (
 SELECT o.id,o.nas_relative_path,o.size_bytes,o.sha256,handoff.round AS handoff_readback_round,
        CASE WHEN handoff.output_id IS NOT NULL THEN ARRAY[handoff.device,handoff.inode] END AS handoff_mount FROM page
 JOIN recording_collation_outputs o ON o.id=page.id ` + collatedEligibilitySQL + collatedNotBackedOffSQL + ` ORDER BY o.id LIMIT $5
), delivered AS (
 SELECT * FROM eligible WHERE nas_delivery_allowed($1,'collated',id) ORDER BY id LIMIT $4
)
SELECT coalesce(jsonb_agg(jsonb_build_object('output_id',id,'nas_relative_path',nas_relative_path,'size_bytes',size_bytes,'sha256',sha256,'handoff_readback_round',handoff_readback_round,'handoff_mount',handoff_mount) ORDER BY id),'[]'::jsonb),coalesce((SELECT max(id) FROM eligible HAVING count(*)=$5),(SELECT max(id) FROM page),0)
FROM delivered`

// collatedNotBackedOffSQL withholds outputs whose last delivery failed until
// their backoff expires.
const collatedNotBackedOffSQL = `
	  AND (handoff.output_id IS NOT NULL OR NOT EXISTS (SELECT 1 FROM recording_collation_output_errors e
	    WHERE e.output_id=o.id AND e.connection_id=$1 AND e.next_attempt_at>now()))`

func (s *Server) collatedPullConnection(w http.ResponseWriter, r *http.Request) (int64, accountPrincipal, bool) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return 0, principal, false
	}
	connectionID, err := pullConnectionID(r.Context(), s.pool, principal, false)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusForbidden, "collated delivery requires a NAS pull key")
		return 0, principal, false
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "load NAS connection")
		return 0, principal, false
	}
	return connectionID, principal, true
}

// handleAccountCollated returns the connection's policy and one page of
// undelivered collated outputs, in output-ID order.
func collatedHandoffClient(r *http.Request) bool {
	return r.URL.Query().Get("handoff_readback") == "1"
}

func (s *Server) handleAccountCollated(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), collatedFeedBudget)
	defer cancel()
	r = r.WithContext(ctx)
	connectionID, _, ok := s.collatedPullConnection(w, r)
	if !ok {
		return
	}
	limit := parseIntQuery(r, "limit", collatedFeedDefaultLimit, 1, collatedFeedMaxLimit)
	afterID := int64(0)
	if raw := r.URL.Query().Get("after_id"); raw != "" {
		var err error
		afterID, err = strconv.ParseInt(raw, 10, 64)
		if err != nil || afterID < 0 {
			util.WriteError(w, 400, "invalid collated cursor")
			return
		}
	}
	var policy collatedPolicy
	var gateOpen bool
	if err := s.pool.QueryRow(r.Context(), `SELECT collated_delivery_enabled OR ($2 AND delivery_nas_handoff_enabled),
        collated_download_bytes_per_sec,collated_download_parallel,
        ($2 AND conn.id=13 AND EXISTS(SELECT 1 `+deliveryNASHandoffActiveCheckSQL+` AND q.checked_at IS NULL)),
        coalesce(`+collatedGateSQL+`,false)
		FROM connections conn WHERE id=$1`, connectionID, collatedHandoffClient(r)).Scan(&policy.Enabled, &policy.DownloadBytesPerSec, &policy.DownloadParallel, &policy.HandoffCheck, &gateOpen); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "load collated policy")
		return
	}
	items := make([]collatedItem, 0, limit)
	nextID := int64(0)
	if policy.Enabled && gateOpen {
		tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
		if err != nil {
			util.WriteError(w, 503, "collated feed database unavailable; retryable")
			return
		}
		defer tx.Rollback(ctx)
		if _, err = tx.Exec(ctx, "SET LOCAL statement_timeout='"+collatedFeedStatementTimeout+"'"); err != nil {
			util.WriteError(w, 503, "collated feed budget unavailable; retryable")
			return
		}
		var raw []byte
		if err := tx.QueryRow(ctx, collatedFeedSQL, connectionID, collatedHandoffClient(r), afterID, limit, collatedFeedFenceLimit).Scan(&raw, &nextID); err != nil {
			util.WriteError(w, 503, "collated feed unavailable; retryable")
			return
		}
		if err := json.Unmarshal(raw, &items); err != nil {
			util.WriteError(w, 500, "read collated outputs")
			return
		}
		if len(items) == limit {
			nextID = items[len(items)-1].OutputID
		}
		valid := items[:0]
		for _, item := range items {
			if !ValidCollatedNASPath(item.NASRelativePath) {
				log.Printf("collated output %d violates the NAS path contract; withheld", item.OutputID)
				continue
			}
			item.DownloadPath = fmt.Sprintf("/api/v1/account/collated/%d/download", item.OutputID)
			if item.HandoffRound != nil {
				item.DownloadPath += "?handoff_readback=1"
			}
			valid = append(valid, item)
		}
		items = valid
	}
	util.WriteJSON(w, 200, map[string]any{"policy": policy, "items": items, "next_after_id": nextID})
}

// handleAccountCollatedDownload presigns the exact R2 object of one eligible
// output. The ETag pins the bytes for the whole (resumable) transfer.
func (s *Server) handleAccountCollatedDownload(w http.ResponseWriter, r *http.Request) {
	connectionID, _, ok := s.collatedPullConnection(w, r)
	if !ok {
		return
	}
	outputID, ok := parseInt64Path(w, r, "outputId")
	if !ok {
		return
	}
	var objectKey, sha, handoffETag string
	var size int64
	err := s.pool.QueryRow(r.Context(), `SELECT o.object_key,o.size_bytes,o.sha256,coalesce(handoff.etag,'') `+
		collatedEligibleSQL+` AND o.id=$3`, connectionID, collatedHandoffClient(r), outputID).Scan(&objectKey, &size, &sha, &handoffETag)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusNotFound, "collated output not found")
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "load collated output")
		return
	}
	store := s.joinedOutputStore()
	if store == nil {
		util.WriteError(w, http.StatusServiceUnavailable, "object storage is unavailable")
		return
	}
	head, err := store.Head(r.Context(), objectKey)
	if err != nil || head.SizeBytes != size || head.ETag == "" ||
		(handoffETag != "" && strings.Trim(head.ETag, `"`) != handoffETag) {
		util.WriteError(w, http.StatusConflict, "collated object no longer matches its verified output")
		return
	}
	req, err := store.PresignGetExactRequest(r.Context(), objectKey, head.ETag, head.VersionID, collatedDownloadTTL)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "presign collated output")
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{
		"url": req.URL, "etag": head.ETag, "if_match": `"` + strings.Trim(head.ETag, `"`) + `"`, "size_bytes": size, "sha256": sha,
		"expires_in_sec": int(collatedDownloadTTL.Seconds()),
	})
}

type collatedAckRequest struct {
	OutputID         int64                    `json:"output_id"`
	NASRelativePath  string                   `json:"nas_relative_path"`
	SizeBytes        int64                    `json:"size_bytes"`
	SHA256           string                   `json:"sha256"`
	HandoffReadback  *nasR2StitchFileIdentity `json:"handoff_readback,omitempty"`
	HandoffRound     *int64                   `json:"handoff_readback_round,omitempty"`
	HandoffRootInode *int64                   `json:"handoff_root_inode,omitempty"`
}

func recordCollatedHandoffReadback(ctx context.Context, tx pgx.Tx, connection, output, round, rootInode int64,
	identity nasR2StitchFileIdentity) (bool, error) {
	if _, err := tx.Exec(ctx, `
 INSERT INTO delivery_nas_handoff_readbacks(output_id,connection_id,size_bytes,mtime_ns,ctime_ns,inode,device,round,root_inode)
 SELECT o.output_id,$2,$3,$4,$5,$6,$7,o.round,$9 FROM delivery_nas_handoff_copyable o WHERE o.output_id=$1 AND o.round=$8 AND o.device=$7 AND o.inode=$9
  AND NOT EXISTS (SELECT 1 FROM r2_object_retirements r WHERE r.kind='collated' AND r.source_id=o.output_id)
  AND EXISTS(SELECT 1 FROM connections c WHERE c.id=$2 AND c.delivery_nas_handoff_enabled
    AND c.delivery_nas_handoff_roster_sha256=o.roster_sha256)
  AND nas_delivery_allowed($2,'collated',o.output_id)
 ON CONFLICT(output_id,round) DO NOTHING`, output, connection, identity[0], identity[1], identity[2], identity[3], identity[4], round, rootInode); err != nil {
		return false, err
	}
	var same bool
	err := tx.QueryRow(ctx, `SELECT EXISTS(
 SELECT 1 FROM delivery_nas_handoff_readbacks r JOIN delivery_nas_handoff_copyable live USING(output_id,round)
 WHERE output_id=$1 AND connection_id=$2 AND round=$8
 AND ROW(r.size_bytes,r.mtime_ns,r.ctime_ns,r.inode,r.device,r.root_inode)=ROW($3::bigint,$4::bigint,$5::bigint,$6::bigint,$7::bigint,$9::bigint)
 )`, output, connection, identity[0], identity[1], identity[2], identity[3], identity[4], round, rootInode).Scan(&same)
	return same, err
}

// handleAccountCollatedAck records that the NAS holds the exact bytes of one
// output at its contract path. Idempotent; a different identity conflicts.
func (s *Server) handleAccountCollatedAck(w http.ResponseWriter, r *http.Request) {
	connectionID, _, ok := s.collatedPullConnection(w, r)
	if !ok {
		return
	}
	var req collatedAckRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.OutputID <= 0 || req.SizeBytes <= 0 || len(req.SHA256) != 64 || !lowerHex(req.SHA256) || !ValidCollatedNASPath(req.NASRelativePath) {
		util.WriteError(w, http.StatusBadRequest, "invalid collated acknowledgment identity")
		return
	}
	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "begin collated acknowledgment")
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()
	if err := lockNASRetirementPublisher(r.Context(), tx); err != nil {
		util.WriteError(w, 503, "NAS evidence fence unavailable")
		return
	}

	var rel, sha string
	var size int64
	err = tx.QueryRow(r.Context(), `SELECT o.nas_relative_path,o.size_bytes,o.sha256 FROM recording_collation_outputs o
		JOIN recording_collation_hours h ON h.id=o.collation_hour_id
		JOIN recordings rec ON rec.id=h.recording_id
		JOIN connections conn ON conn.id=$2 AND conn.kind='nas_pull' AND rec.account_id=conn.account_id
		WHERE o.id=$1 AND rec.delivery='nas_pull' FOR SHARE OF o`, req.OutputID, connectionID).Scan(&rel, &size, &sha)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusNotFound, "collated output not found")
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "load collated acknowledgment target")
		return
	}
	if rel != req.NASRelativePath || size != req.SizeBytes || sha != req.SHA256 {
		util.WriteError(w, http.StatusConflict, "collated acknowledgment identity differs")
		return
	}
	if req.HandoffReadback != nil || req.HandoffRound != nil || req.HandoffRootInode != nil {
		receipt := req.HandoffReadback
		if receipt == nil || req.HandoffRound == nil || *req.HandoffRound < 0 ||
			req.HandoffRootInode == nil || *req.HandoffRootInode <= 0 || connectionID != 13 || !collatedHandoffClient(r) || receipt[0] != size ||
			receipt[1] <= 0 || receipt[2] <= 0 || receipt[3] <= 0 || receipt[4] < 0 {
			util.WriteError(w, http.StatusBadRequest, "invalid NAS handoff readback")
			return
		}
		recorded, err := recordCollatedHandoffReadback(r.Context(), tx, connectionID, req.OutputID, *req.HandoffRound, *req.HandoffRootInode, *receipt)
		if err != nil {
			util.WriteError(w, http.StatusServiceUnavailable, "record NAS handoff readback")
			return
		}
		if !recorded {
			util.WriteError(w, http.StatusConflict, "NAS handoff approval or file identity differs")
			return
		}
	}
	ct, err := tx.Exec(r.Context(), `INSERT INTO recording_collation_output_acks(output_id,connection_id,nas_relative_path,size_bytes,sha256)
		VALUES($1,$2,$3,$4,$5) ON CONFLICT (output_id,connection_id) DO NOTHING`, req.OutputID, connectionID, rel, size, sha)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "acknowledge collated output")
		return
	}
	already := ct.RowsAffected() == 0
	if !already {
		if _, err := tx.Exec(r.Context(), `UPDATE connections SET collated_files_pulled=collated_files_pulled+1,
			collated_bytes_pulled=collated_bytes_pulled+$2,collated_last_ack_at=now(),
			collated_last_error=CASE WHEN collated_last_error_output_id=$3 THEN '' ELSE collated_last_error END,
			collated_last_error_output_id=CASE WHEN collated_last_error_output_id=$3 THEN NULL ELSE collated_last_error_output_id END
			WHERE id=$1`, connectionID, size, req.OutputID); err != nil {
			util.WriteError(w, http.StatusInternalServerError, "advance collated totals")
			return
		}
	}
	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "commit collated acknowledgment")
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"ok": true, "already_verified": already})
}

// handleAccountCollatedError records the NAS client's latest collated delivery
// failure so a stall is visible without logging into the NAS.
func (s *Server) handleAccountCollatedError(w http.ResponseWriter, r *http.Request) {
	connectionID, _, ok := s.collatedPullConnection(w, r)
	if !ok {
		return
	}
	var req struct {
		OutputID int64  `json:"output_id"`
		Error    string `json:"error"`
	}
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	msg := strings.ToValidUTF8(strings.TrimSpace(req.Error), "?")
	if req.OutputID < 0 || msg == "" {
		util.WriteError(w, http.StatusBadRequest, "output_id and error are required")
		return
	}
	if len(msg) > collatedMaxErrorBytes {
		msg = strings.ToValidUTF8(msg[:collatedMaxErrorBytes], "")
	}
	if collatedHandoffClient(r) {
		tag, err := s.pool.Exec(r.Context(), `
 INSERT INTO delivery_nas_handoff_errors(output_id,round,failure_count,last_error,next_attempt_at)
 SELECT o.output_id,o.round,1,$3,clock_timestamp()+interval '2 minutes'
 FROM delivery_nas_handoff_copyable o JOIN connections c ON c.id=$1 AND c.id=13 AND c.delivery_nas_handoff_enabled
  AND c.delivery_nas_handoff_roster_sha256=o.roster_sha256
 WHERE o.output_id=$2
 ON CONFLICT(output_id,round) DO UPDATE SET
  failure_count=delivery_nas_handoff_errors.failure_count+1,last_error=excluded.last_error,
  next_attempt_at=clock_timestamp()+least(interval '6 hours',interval '1 minute'*
   power(2,least(delivery_nas_handoff_errors.failure_count+1,9)))`, connectionID, req.OutputID, msg)
		if err != nil {
			util.WriteError(w, http.StatusServiceUnavailable, "record NAS handoff error")
			return
		}
		if tag.RowsAffected() != 1 {
			util.WriteError(w, http.StatusNotFound, "handoff output not found")
			return
		}
		util.WriteJSON(w, http.StatusOK, map[string]any{"ok": true})
		return
	}
	var outputID any
	if req.OutputID > 0 {
		// Only an output this connection may currently receive can carry an error.
		var eligible bool
		if err := s.pool.QueryRow(r.Context(), `SELECT EXISTS(SELECT 1 `+collatedEligibleSQL+` AND o.id=$3)`,
			connectionID, collatedHandoffClient(r), req.OutputID).Scan(&eligible); err != nil {
			util.WriteError(w, http.StatusInternalServerError, "check collated output")
			return
		}
		if !eligible {
			util.WriteError(w, http.StatusNotFound, "collated output not found")
			return
		}
		outputID = req.OutputID
	}
	if _, err := s.pool.Exec(r.Context(), `UPDATE connections SET collated_last_error=$2,collated_last_error_output_id=$3,
		collated_last_error_at=now() WHERE id=$1`, connectionID, msg, outputID); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "record collated error")
		return
	}
	if req.OutputID > 0 {
		// Back off 2, 4, 8 ... minutes, capped at six hours.
		if _, err := s.pool.Exec(r.Context(), `
			INSERT INTO recording_collation_output_errors(output_id,connection_id,failure_count,last_error,last_error_at,next_attempt_at)
			SELECT o.id,$1,1,$3,now(),now()+interval '2 minutes' FROM recording_collation_outputs o WHERE o.id=$2
			ON CONFLICT (output_id,connection_id) DO UPDATE SET
			  failure_count=recording_collation_output_errors.failure_count+1,last_error=EXCLUDED.last_error,last_error_at=now(),
			  next_attempt_at=now()+LEAST(interval '6 hours',
			    interval '1 minute'*power(2,LEAST(recording_collation_output_errors.failure_count+1,9)))`,
			connectionID, req.OutputID, msg); err != nil {
			util.WriteError(w, http.StatusInternalServerError, "record collated backoff")
			return
		}
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"ok": true})
}

type collatedPolicyRequest struct {
	Enabled             *bool  `json:"enabled"`
	DownloadBytesPerSec *int64 `json:"download_bytes_per_sec"`
	DownloadParallel    *int   `json:"download_parallel"`
}

// handleAdminConnectionCollatedDelivery reads (GET) or changes (POST) one NAS
// connection's collated delivery policy and reports its progress.
func (s *Server) handleAdminConnectionCollatedDelivery(w http.ResponseWriter, r *http.Request) {
	connectionID, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	if r.Method == http.MethodPost {
		var req collatedPolicyRequest
		if err := util.DecodeJSON(r, &req); err != nil {
			util.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
		if req.Enabled == nil && req.DownloadBytesPerSec == nil && req.DownloadParallel == nil {
			util.WriteError(w, http.StatusBadRequest, "nothing to change")
			return
		}
		if req.DownloadBytesPerSec != nil && (*req.DownloadBytesPerSec < collatedMinBytesPerSec || *req.DownloadBytesPerSec > collatedMaxBytesPerSec) {
			util.WriteError(w, http.StatusBadRequest, "download_bytes_per_sec must be between 1 MiB/s and 1 GiB/s")
			return
		}
		if req.DownloadParallel != nil && (*req.DownloadParallel < 1 || *req.DownloadParallel > collatedMaxParallel) {
			util.WriteError(w, http.StatusBadRequest, "download_parallel must be between 1 and 32")
			return
		}
		ct, err := s.pool.Exec(r.Context(), `UPDATE connections SET
			collated_delivery_enabled=COALESCE($2,collated_delivery_enabled),
			collated_download_bytes_per_sec=COALESCE($3,collated_download_bytes_per_sec),
			collated_download_parallel=COALESCE($4,collated_download_parallel),updated_at=now()
			WHERE id=$1 AND kind='nas_pull'`, connectionID, req.Enabled, req.DownloadBytesPerSec, req.DownloadParallel)
		if err != nil {
			util.WriteError(w, http.StatusInternalServerError, "update collated policy")
			return
		}
		if ct.RowsAffected() != 1 {
			util.WriteError(w, http.StatusNotFound, "NAS connection not found")
			return
		}
	}
	var out struct {
		ConnectionID    int64          `json:"connection_id"`
		Policy          collatedPolicy `json:"policy"`
		FilesPulled     int64          `json:"files_pulled"`
		BytesPulled     int64          `json:"bytes_pulled"`
		LastAckAt       *time.Time     `json:"last_ack_at"`
		LastError       string         `json:"last_error"`
		LastErrorOutput *int64         `json:"last_error_output_id"`
		LastErrorAt     *time.Time     `json:"last_error_at"`
		PendingFiles    int64          `json:"pending_files"`
		BackedOffFiles  int64          `json:"backed_off_files"`
		PendingBytes    int64          `json:"pending_bytes"`
	}
	out.ConnectionID = connectionID
	err := s.pool.QueryRow(r.Context(), `SELECT collated_delivery_enabled,collated_download_bytes_per_sec,collated_download_parallel,
		collated_files_pulled,collated_bytes_pulled,collated_last_ack_at,collated_last_error,collated_last_error_output_id,collated_last_error_at
		FROM connections WHERE id=$1 AND kind='nas_pull'`, connectionID).Scan(&out.Policy.Enabled, &out.Policy.DownloadBytesPerSec,
		&out.Policy.DownloadParallel, &out.FilesPulled, &out.BytesPulled, &out.LastAckAt, &out.LastError, &out.LastErrorOutput, &out.LastErrorAt)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusNotFound, "NAS connection not found")
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "load collated policy")
		return
	}
	// Pending is reported even while disabled, so an operator sees the backlog
	// before switching delivery on.
	if err := s.pool.QueryRow(r.Context(), `SELECT count(*),COALESCE(SUM(o.size_bytes),0) `+
		strings.Replace(collatedEligibleSQL, "conn.collated_delivery_enabled", "true", 1), connectionID, false).Scan(&out.PendingFiles, &out.PendingBytes); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "count pending collated outputs")
		return
	}
	if err := s.pool.QueryRow(r.Context(), `SELECT count(*) `+
		strings.Replace(collatedEligibleSQL, "conn.collated_delivery_enabled", "true", 1)+`
		  AND EXISTS (SELECT 1 FROM recording_collation_output_errors e
		    WHERE e.output_id=o.id AND e.connection_id=$1 AND e.next_attempt_at>now())`,
		connectionID, false).Scan(&out.BackedOffFiles); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "count backed-off collated outputs")
		return
	}
	util.WriteJSON(w, http.StatusOK, out)
}
