package api

import (
	"errors"
	"fmt"
	"github.com/daydemir/stoarama/backend/internal/nasdelivery"
	"log"
	"net/http"
	"path"
	"regexp"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/util"
	"github.com/jackc/pgx/v5"
)

// Generation-2 collated hour delivery (docs/NAS_JOINED_DELIVERY.md). The NAS
// pulls recording_collation_outputs to /clips/joined/<nas_relative_path> at a
// server-set rate, beside (never yielding to) raw delivery.

const (
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

// validCollatedNASPath reports whether rel is a safe relative path that obeys
// the collated NAS path contract.
func validCollatedNASPath(rel string) bool {
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
}

type collatedItem struct {
	OutputID        int64  `json:"output_id"`
	NASRelativePath string `json:"nas_relative_path"`
	SizeBytes       int64  `json:"size_bytes"`
	SHA256          string `json:"sha256"`
	DownloadPath    string `json:"download_path"`
}

// collatedEligibleSQL selects outputs this connection still needs: newest
// generation of each recording hour, nas_pull recordings of the connection's
// account, R2-verified, not yet acknowledged by this connection. $1=connection.
const collatedEligibleSQL = `
	FROM recording_collation_outputs o
	JOIN recording_collation_hours h ON h.id=o.collation_hour_id
	JOIN recordings r ON r.id=h.recording_id
	JOIN connections conn ON conn.id=$1 AND conn.kind='nas_pull' AND r.account_id=conn.account_id
	WHERE r.delivery='nas_pull' AND conn.collated_delivery_enabled
	  -- Scope: only outputs collated from clips held for collated-only delivery
	  -- (nasdelivery.HoldContractMode). The NAS already has the raw of every other
	  -- output (e.g. the good+ backfill), so those stay in R2.
	  AND EXISTS (SELECT 1 FROM clip_storage_billing_contracts hold
	    WHERE hold.clip_id=ANY(o.source_clip_ids) AND hold.mode='` + nasdelivery.HoldContractMode + `')
	  AND NOT EXISTS (SELECT 1 FROM recording_collation_hours newer
	    WHERE newer.recording_id=h.recording_id AND newer.local_date=h.local_date
	      AND newer.delivery_hour=h.delivery_hour AND newer.generation>h.generation)
	  AND NOT EXISTS (SELECT 1 FROM recording_collation_output_acks ack
	    WHERE ack.output_id=o.id AND ack.connection_id=$1)`

// collatedNotBackedOffSQL withholds outputs whose last delivery failed until
// their backoff expires.
const collatedNotBackedOffSQL = `
	  AND NOT EXISTS (SELECT 1 FROM recording_collation_output_errors e
	    WHERE e.output_id=o.id AND e.connection_id=$1 AND e.next_attempt_at>now())`

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
// undelivered collated outputs, oldest hour first.
func (s *Server) handleAccountCollated(w http.ResponseWriter, r *http.Request) {
	connectionID, _, ok := s.collatedPullConnection(w, r)
	if !ok {
		return
	}
	limit := parseIntQuery(r, "limit", collatedFeedDefaultLimit, 1, collatedFeedMaxLimit)
	var policy collatedPolicy
	if err := s.pool.QueryRow(r.Context(), `SELECT collated_delivery_enabled,collated_download_bytes_per_sec,collated_download_parallel
		FROM connections WHERE id=$1`, connectionID).Scan(&policy.Enabled, &policy.DownloadBytesPerSec, &policy.DownloadParallel); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "load collated policy")
		return
	}
	items := make([]collatedItem, 0, limit)
	if policy.Enabled {
		rows, err := s.pool.Query(r.Context(), `SELECT o.id,o.nas_relative_path,o.size_bytes,o.sha256 `+collatedEligibleSQL+collatedNotBackedOffSQL+`
			ORDER BY h.local_date,h.delivery_hour,h.recording_id,o.part,o.id LIMIT $2`, connectionID, limit)
		if err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("list collated outputs: %v", err))
			return
		}
		defer rows.Close()
		for rows.Next() {
			var item collatedItem
			if err := rows.Scan(&item.OutputID, &item.NASRelativePath, &item.SizeBytes, &item.SHA256); err != nil {
				util.WriteError(w, http.StatusInternalServerError, "read collated outputs")
				return
			}
			if !validCollatedNASPath(item.NASRelativePath) {
				// A contract violation is a collation bug; never hand the NAS a path it must refuse.
				log.Printf("collated output %d violates the NAS path contract; withheld", item.OutputID)
				continue
			}
			item.DownloadPath = fmt.Sprintf("/api/v1/account/collated/%d/download", item.OutputID)
			items = append(items, item)
		}
		if err := rows.Err(); err != nil {
			util.WriteError(w, http.StatusInternalServerError, "read collated outputs")
			return
		}
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"policy": policy, "items": items})
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
	var objectKey, sha string
	var size int64
	err := s.pool.QueryRow(r.Context(), `SELECT o.object_key,o.size_bytes,o.sha256 `+collatedEligibleSQL+` AND o.id=$2`,
		connectionID, outputID).Scan(&objectKey, &size, &sha)
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
	if err != nil || head.SizeBytes != size || head.ETag == "" {
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
	OutputID        int64  `json:"output_id"`
	NASRelativePath string `json:"nas_relative_path"`
	SizeBytes       int64  `json:"size_bytes"`
	SHA256          string `json:"sha256"`
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
	if req.OutputID <= 0 || req.SizeBytes <= 0 || len(req.SHA256) != 64 || !lowerHex(req.SHA256) || !validCollatedNASPath(req.NASRelativePath) {
		util.WriteError(w, http.StatusBadRequest, "invalid collated acknowledgment identity")
		return
	}
	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "begin collated acknowledgment")
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()
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
	var outputID any
	if req.OutputID > 0 {
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
		strings.Replace(collatedEligibleSQL, " AND conn.collated_delivery_enabled", "", 1), connectionID).Scan(&out.PendingFiles, &out.PendingBytes); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "count pending collated outputs")
		return
	}
	if err := s.pool.QueryRow(r.Context(), `SELECT count(*) FROM recording_collation_output_errors WHERE connection_id=$1 AND next_attempt_at>now()`,
		connectionID).Scan(&out.BackedOffFiles); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "count backed-off collated outputs")
		return
	}
	util.WriteJSON(w, http.StatusOK, out)
}
