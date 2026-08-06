package api

import (
	"encoding/csv"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/daydemir/stoarama/backend/internal/util"
)

const (
	nasInventoryMaxBatch      = 500
	nasInventoryMaxPage       = 500
	nasInventoryFreshness     = 72 * time.Hour
	nasInventoryMaxFutureSkew = 5 * time.Minute
	nasInventoryMaxPathBytes  = 1024
	nasInventoryMaxGeneration = 128
)

type nasInventoryFileReport struct {
	ClipID          int64      `json:"clip_id"`
	RecordingID     int64      `json:"recording_id"`
	RelativePath    string     `json:"relative_path"`
	SizeBytes       int64      `json:"size_bytes"`
	SHA256          string     `json:"sha256"`
	State           string     `json:"state"`
	VerifiedAt      *time.Time `json:"verified_at"`
	FileMTimeNS     int64      `json:"file_mtime_ns"`
	ClientUpdatedAt time.Time  `json:"client_updated_at"`
}

type nasInventorySyncRequest struct {
	Generation      string                   `json:"generation"`
	ScanStartedAt   *time.Time               `json:"scan_started_at"`
	ScanCompletedAt *time.Time               `json:"scan_completed_at"`
	Digest          string                   `json:"digest"`
	Complete        bool                     `json:"complete"`
	Files           []nasInventoryFileReport `json:"files"`
	Unmatched       []nasUnmatchedFileReport `json:"unmatched_files"`
}

type nasUnmatchedFileReport struct {
	RelativePath    string    `json:"relative_path"`
	SizeBytes       int64     `json:"size_bytes"`
	SHA256          string    `json:"sha256"`
	State           string    `json:"state"`
	FileMTimeNS     int64     `json:"file_mtime_ns"`
	ClientUpdatedAt time.Time `json:"client_updated_at"`
}

func validNASRelativePath(path string) bool {
	if path == "" || len(path) > nasInventoryMaxPathBytes || strings.HasPrefix(path, "/") || strings.Contains(path, "\\") {
		return false
	}
	for _, part := range strings.Split(path, "/") {
		if part == "" || part == "." || part == ".." {
			return false
		}
	}
	return true
}

func validateNASInventorySync(req nasInventorySyncRequest, now time.Time) error {
	if req.Generation == "" || len(req.Generation) > nasInventoryMaxGeneration || !relayArtifactName.MatchString(req.Generation) {
		return errors.New("invalid inventory generation")
	}
	if len(req.Files)+len(req.Unmatched) > nasInventoryMaxBatch {
		return fmt.Errorf("inventory batch exceeds %d files", nasInventoryMaxBatch)
	}
	unmatchedSeen := make(map[string]bool, len(req.Unmatched))
	for _, file := range req.Unmatched {
		if !validNASRelativePath(file.RelativePath) || unmatchedSeen[file.RelativePath] || file.SizeBytes < 0 ||
			len(file.SHA256) != 64 || !lowerHex(file.SHA256) || file.FileMTimeNS < 0 ||
			(file.State != "present" && file.State != "missing") || file.ClientUpdatedAt.IsZero() ||
			file.ClientUpdatedAt.After(now.Add(nasInventoryMaxFutureSkew)) {
			return errors.New("invalid or duplicate unmatched inventory file")
		}
		unmatchedSeen[file.RelativePath] = true
	}
	if req.Complete {
		if req.ScanStartedAt == nil || req.ScanCompletedAt == nil || req.ScanCompletedAt.Before(*req.ScanStartedAt) {
			return errors.New("completed inventory requires a valid scan interval")
		}
		if req.ScanCompletedAt.After(now.Add(nasInventoryMaxFutureSkew)) {
			return errors.New("inventory completion is too far in the future")
		}
		if len(req.Digest) != 64 || !lowerHex(req.Digest) {
			return errors.New("completed inventory requires a sha256 digest")
		}
	}
	seen := make(map[int64]bool, len(req.Files))
	for _, file := range req.Files {
		if file.ClipID <= 0 || file.RecordingID <= 0 || file.SizeBytes < 0 || file.FileMTimeNS < 0 || seen[file.ClipID] {
			return errors.New("invalid or duplicate inventory file identity")
		}
		seen[file.ClipID] = true
		if !validNASRelativePath(file.RelativePath) || len(file.SHA256) != 64 || !lowerHex(file.SHA256) {
			return errors.New("invalid inventory file path or checksum")
		}
		if file.State != "present" && file.State != "missing" && file.State != "mismatch" {
			return errors.New("invalid inventory file state")
		}
		if file.State == "present" && file.VerifiedAt == nil {
			return errors.New("present inventory file requires verified_at")
		}
		if file.VerifiedAt != nil && file.VerifiedAt.After(now.Add(nasInventoryMaxFutureSkew)) {
			return errors.New("inventory verified_at is too far in the future")
		}
		if file.ClientUpdatedAt.IsZero() || file.ClientUpdatedAt.After(now.Add(nasInventoryMaxFutureSkew)) {
			return errors.New("invalid inventory client_updated_at")
		}
	}
	return nil
}

func lowerHex(value string) bool {
	for _, ch := range value {
		if (ch < '0' || ch > '9') && (ch < 'a' || ch > 'f') {
			return false
		}
	}
	return true
}

// verifyNASInventoryForRelease is fail-closed in enforce mode. It compares the
// NAS proof with the authoritative clip identity and requires a recent checksum
// verification. Observe mode records inventory without interrupting rollout.
func (s *Server) verifyNASInventoryForRelease(r *http.Request, principal accountPrincipal, recordingID, clipID int64) (bool, string, error) {
	if principal.APIKeyID == nil {
		return true, "non-pull caller", nil
	}
	var mode string
	var state, localPath, localSHA, serverPath, serverSHA string
	var localSize, serverSize int64
	var verifiedAt *time.Time
	err := s.pool.QueryRow(r.Context(), `
		SELECT conn.inventory_mode, COALESCE(i.state,''), COALESCE(i.relative_path,''),
		       COALESCE(i.size_bytes,0), COALESCE(i.sha256,''), i.verified_at,
		       COALESCE(c.display_path,''), c.size_bytes, lower(COALESCE(c.sha256,''))
		FROM connections conn
		JOIN recording_clips c ON c.id=$3 AND c.recording_id=$4
		JOIN recordings rec ON rec.id=c.recording_id AND rec.account_id=conn.account_id
		LEFT JOIN nas_inventory_files i ON i.connection_id=conn.id AND i.clip_id=c.id
		WHERE conn.api_key_id=$1 AND conn.account_id=$2
	`, *principal.APIKeyID, principal.AccountID, clipID, recordingID).Scan(
		&mode, &state, &localPath, &localSize, &localSHA, &verifiedAt,
		&serverPath, &serverSize, &serverSHA)
	if errors.Is(err, pgx.ErrNoRows) {
		return true, "non-pull caller", nil
	}
	if err != nil {
		return false, "", err
	}
	if mode != "enforce" {
		return true, "observe mode", nil
	}
	if state != "present" || verifiedAt == nil {
		return false, "clip is not confirmed present in NAS inventory", nil
	}
	age := time.Since(verifiedAt.UTC())
	if age < -nasInventoryMaxFutureSkew || age > nasInventoryFreshness {
		return false, "NAS checksum verification is stale", nil
	}
	if localPath != serverPath || localSize != serverSize || localSHA != serverSHA {
		return false, "NAS path, size, or checksum does not match the server clip", nil
	}
	return true, "confirmed", nil
}

// handleAccountConnectionInventorySync accepts bounded, idempotent inventory
// pages from the pull key. A completed generation atomically marks only older,
// unseen rows missing; files downloaded during the scan are never swept away.
func (s *Server) handleAccountConnectionInventorySync(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	if principal.APIKeyID == nil {
		util.WriteError(w, http.StatusForbidden, "inventory sync requires a NAS pull key")
		return
	}
	var req nasInventorySyncRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	now := time.Now().UTC()
	if err := validateNASInventorySync(req, now); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("begin inventory sync: %v", err))
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()
	var connectionID int64
	if err := tx.QueryRow(r.Context(), `SELECT id FROM connections WHERE api_key_id=$1 AND account_id=$2 FOR UPDATE`, *principal.APIKeyID, principal.AccountID).Scan(&connectionID); errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusForbidden, "no connection for this key")
		return
	} else if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load inventory connection: %v", err))
		return
	}
	for _, file := range req.Files {
		_, err = tx.Exec(r.Context(), `
			INSERT INTO nas_inventory_files
				(connection_id,clip_id,recording_id,relative_path,size_bytes,sha256,state,verified_at,file_mtime_ns,seen_generation,client_updated_at,server_received_at)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,now())
			ON CONFLICT (connection_id,clip_id) DO UPDATE SET
				recording_id=EXCLUDED.recording_id, relative_path=EXCLUDED.relative_path,
				size_bytes=EXCLUDED.size_bytes, sha256=EXCLUDED.sha256, state=EXCLUDED.state,
				verified_at=EXCLUDED.verified_at, file_mtime_ns=EXCLUDED.file_mtime_ns,
				seen_generation=EXCLUDED.seen_generation, client_updated_at=EXCLUDED.client_updated_at,
				server_received_at=now()
			WHERE EXCLUDED.client_updated_at >= nas_inventory_files.client_updated_at
		`, connectionID, file.ClipID, file.RecordingID, file.RelativePath, file.SizeBytes,
			file.SHA256, file.State, file.VerifiedAt, file.FileMTimeNS, req.Generation, file.ClientUpdatedAt)
		if err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("upsert inventory file: %v", err))
			return
		}
	}
	for _, file := range req.Unmatched {
		_, err = tx.Exec(r.Context(), `
			INSERT INTO nas_inventory_unmatched_files
				(connection_id,relative_path,size_bytes,sha256,state,file_mtime_ns,seen_generation,client_updated_at,server_received_at)
			VALUES($1,$2,$3,$4,$5,$6,$7,$8,now())
			ON CONFLICT(connection_id,relative_path) DO UPDATE SET
				size_bytes=EXCLUDED.size_bytes,sha256=EXCLUDED.sha256,state=EXCLUDED.state,
				file_mtime_ns=EXCLUDED.file_mtime_ns,seen_generation=EXCLUDED.seen_generation,
				client_updated_at=EXCLUDED.client_updated_at,server_received_at=now()
			WHERE EXCLUDED.client_updated_at>=nas_inventory_unmatched_files.client_updated_at
		`, connectionID, file.RelativePath, file.SizeBytes, file.SHA256, file.State,
			file.FileMTimeNS, req.Generation, file.ClientUpdatedAt)
		if err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("upsert unmatched inventory file: %v", err))
			return
		}
	}
	if req.Complete {
		_, err = tx.Exec(r.Context(), `
			UPDATE nas_inventory_files
			SET state='missing', verified_at=NULL, client_updated_at=$3, server_received_at=now()
			WHERE connection_id=$1 AND seen_generation<>$2 AND client_updated_at <= $4
		`, connectionID, req.Generation, req.ScanCompletedAt, req.ScanStartedAt)
		if err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("finalize inventory missing files: %v", err))
			return
		}
		_, err = tx.Exec(r.Context(), `
			UPDATE nas_inventory_unmatched_files SET state='missing',client_updated_at=$3,server_received_at=now()
			WHERE connection_id=$1 AND seen_generation<>$2 AND client_updated_at<=$4
		`, connectionID, req.Generation, req.ScanCompletedAt, req.ScanStartedAt)
		if err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("finalize unmatched inventory files: %v", err))
			return
		}
		var clips, bytes, mismatches, unmatched int64
		if err := tx.QueryRow(r.Context(), `
			SELECT count(*) FILTER (WHERE state='present'),
			       COALESCE(sum(size_bytes) FILTER (WHERE state='present'),0),
			       count(*) FILTER (WHERE state='mismatch')
			FROM nas_inventory_files WHERE connection_id=$1
		`, connectionID).Scan(&clips, &bytes, &mismatches); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("summarize inventory: %v", err))
			return
		}
		if err := tx.QueryRow(r.Context(), `SELECT count(*) FROM nas_inventory_unmatched_files WHERE connection_id=$1 AND state='present'`, connectionID).Scan(&unmatched); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("summarize unmatched inventory: %v", err))
			return
		}
		_, err = tx.Exec(r.Context(), `
			UPDATE connections SET inventory_generation=$1, inventory_scan_started_at=$2,
			inventory_scan_completed_at=$3, inventory_reported_at=now(), inventory_clips=$4,
			inventory_bytes=$5, inventory_mismatches=$6, inventory_unmatched=$7,
			inventory_digest=$8, updated_at=now() WHERE id=$9
		`, req.Generation, req.ScanStartedAt, req.ScanCompletedAt, clips, bytes, mismatches, unmatched, req.Digest, connectionID)
		if err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("update inventory summary: %v", err))
			return
		}
	}
	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("commit inventory sync: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"ok": true, "accepted": len(req.Files) + len(req.Unmatched), "complete": req.Complete})
}

type nasInventoryListItem struct {
	ClipID           int64      `json:"clip_id"`
	RecordingID      int64      `json:"recording_id"`
	RelativePath     string     `json:"relative_path"`
	SizeBytes        int64      `json:"size_bytes"`
	SHA256           string     `json:"sha256"`
	State            string     `json:"state"`
	VerifiedAt       *time.Time `json:"verified_at"`
	ServerReceivedAt time.Time  `json:"server_received_at"`
	Reconciliation   string     `json:"reconciliation"`
}

func (s *Server) inventoryRows(r *http.Request, accountID, connectionID, afterID int64, limit int) (pgx.Rows, error) {
	return s.pool.Query(r.Context(), `
		SELECT i.clip_id,i.recording_id,i.relative_path,i.size_bytes,i.sha256,i.state,i.verified_at,i.server_received_at,
		       CASE WHEN c.id IS NULL THEN 'nas_only'
		            WHEN i.state='missing' THEN 'missing'
		            WHEN i.state='mismatch' OR c.size_bytes<>i.size_bytes OR lower(c.sha256)<>i.sha256 OR c.display_path<>i.relative_path THEN 'mismatch'
		            ELSE 'confirmed' END AS reconciliation
		FROM nas_inventory_files i
		LEFT JOIN (recording_clips c JOIN recordings rec ON rec.id=c.recording_id AND rec.account_id=$2) ON c.id=i.clip_id
		WHERE i.connection_id=$1 AND i.clip_id>$3
		ORDER BY i.clip_id LIMIT $4
	`, connectionID, accountID, afterID, limit)
}

func (s *Server) handleAccountConnectionInventoryList(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	connectionID, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	var exists bool
	if err := s.pool.QueryRow(r.Context(), `SELECT EXISTS(SELECT 1 FROM connections WHERE id=$1 AND account_id=$2)`, connectionID, principal.AccountID).Scan(&exists); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load connection: %v", err))
		return
	}
	if !exists {
		util.WriteError(w, http.StatusNotFound, "connection not found")
		return
	}
	afterID, _ := strconv.ParseInt(r.URL.Query().Get("after_clip_id"), 10, 64)
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit < 1 || limit > nasInventoryMaxPage {
		limit = 200
	}
	rows, err := s.inventoryRows(r, principal.AccountID, connectionID, afterID, limit)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("list inventory: %v", err))
		return
	}
	defer rows.Close()
	items := make([]nasInventoryListItem, 0, limit)
	for rows.Next() {
		var item nasInventoryListItem
		if err := rows.Scan(&item.ClipID, &item.RecordingID, &item.RelativePath, &item.SizeBytes, &item.SHA256, &item.State, &item.VerifiedAt, &item.ServerReceivedAt, &item.Reconciliation); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan inventory: %v", err))
			return
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("iterate inventory: %v", err))
		return
	}
	var serverOnly int64
	if err := s.pool.QueryRow(r.Context(), `
		SELECT count(*) FROM recording_clips c JOIN recordings rec ON rec.id=c.recording_id
		WHERE rec.account_id=$1 AND rec.delivery='nas_pull' AND c.purged_at IS NULL
		  AND NOT EXISTS (SELECT 1 FROM nas_inventory_files i WHERE i.connection_id=$2 AND i.clip_id=c.id AND i.state='present')
	`, principal.AccountID, connectionID).Scan(&serverOnly); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("count server-only clips: %v", err))
		return
	}
	unmatched := make([]map[string]any, 0)
	if afterID == 0 {
		unmatchedRows, err := s.pool.Query(r.Context(), `SELECT relative_path,size_bytes,sha256,state,server_received_at FROM nas_inventory_unmatched_files WHERE connection_id=$1 AND state='present' ORDER BY relative_path LIMIT $2`, connectionID, nasInventoryMaxPage)
		if err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("list unmatched inventory: %v", err))
			return
		}
		defer unmatchedRows.Close()
		for unmatchedRows.Next() {
			var path, sha, state string
			var size int64
			var received time.Time
			if err := unmatchedRows.Scan(&path, &size, &sha, &state, &received); err != nil {
				util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan unmatched inventory: %v", err))
				return
			}
			unmatched = append(unmatched, map[string]any{"relative_path": path, "size_bytes": size, "sha256": sha, "state": state, "server_received_at": received, "reconciliation": "nas_only"})
		}
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"items": items, "unmatched_files": unmatched, "server_only": serverOnly})
}

func (s *Server) handleAccountConnectionInventoryCSV(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	connectionID, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	var exists bool
	if err := s.pool.QueryRow(r.Context(), `SELECT EXISTS(SELECT 1 FROM connections WHERE id=$1 AND account_id=$2)`, connectionID, principal.AccountID).Scan(&exists); err != nil || !exists {
		if err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load connection: %v", err))
		} else {
			util.WriteError(w, http.StatusNotFound, "connection not found")
		}
		return
	}
	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="stoarama-nas-%d-inventory.csv"`, connectionID))
	writer := csv.NewWriter(w)
	failExport := func(err error) {
		log.Printf("NAS inventory CSV export connection_id=%d incomplete: %v", connectionID, err)
		_ = writer.Write([]string{"", "", "", "", "", "error", "", "", "INCOMPLETE EXPORT: " + err.Error()})
		writer.Flush()
	}
	if err := writer.Write([]string{"clip_id", "recording_id", "relative_path", "size_bytes", "sha256", "state", "verified_at", "server_received_at", "reconciliation"}); err != nil {
		failExport(fmt.Errorf("write header: %w", err))
		return
	}
	var afterID int64
	for {
		rows, err := s.inventoryRows(r, principal.AccountID, connectionID, afterID, nasInventoryMaxPage)
		if err != nil {
			failExport(fmt.Errorf("load inventory page: %w", err))
			return
		}
		count := 0
		for rows.Next() {
			var item nasInventoryListItem
			if err := rows.Scan(&item.ClipID, &item.RecordingID, &item.RelativePath, &item.SizeBytes, &item.SHA256, &item.State, &item.VerifiedAt, &item.ServerReceivedAt, &item.Reconciliation); err != nil {
				rows.Close()
				failExport(fmt.Errorf("scan inventory row: %w", err))
				return
			}
			verified := ""
			if item.VerifiedAt != nil {
				verified = item.VerifiedAt.UTC().Format(time.RFC3339)
			}
			if err := writer.Write([]string{strconv.FormatInt(item.ClipID, 10), strconv.FormatInt(item.RecordingID, 10), item.RelativePath, strconv.FormatInt(item.SizeBytes, 10), item.SHA256, item.State, verified, item.ServerReceivedAt.UTC().Format(time.RFC3339), item.Reconciliation}); err != nil {
				rows.Close()
				failExport(fmt.Errorf("write inventory row: %w", err))
				return
			}
			afterID = item.ClipID
			count++
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			failExport(fmt.Errorf("iterate inventory page: %w", err))
			return
		}
		rows.Close()
		writer.Flush()
		if writer.Error() != nil {
			failExport(fmt.Errorf("flush inventory page: %w", writer.Error()))
			return
		}
		if count < nasInventoryMaxPage {
			break
		}
	}
	rows, err := s.pool.Query(r.Context(), `SELECT relative_path,size_bytes,sha256,state,server_received_at FROM nas_inventory_unmatched_files WHERE connection_id=$1 ORDER BY relative_path`, connectionID)
	if err != nil {
		failExport(fmt.Errorf("load unmatched files: %w", err))
		return
	}
	defer rows.Close()
	for rows.Next() {
		var path, sha, state string
		var size int64
		var received time.Time
		if err := rows.Scan(&path, &size, &sha, &state, &received); err != nil {
			failExport(fmt.Errorf("scan unmatched file: %w", err))
			return
		}
		if err := writer.Write([]string{"", "", path, strconv.FormatInt(size, 10), sha, state, "", received.UTC().Format(time.RFC3339), "nas_only"}); err != nil {
			failExport(fmt.Errorf("write unmatched file: %w", err))
			return
		}
	}
	if err := rows.Err(); err != nil {
		failExport(fmt.Errorf("iterate unmatched files: %w", err))
		return
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		failExport(fmt.Errorf("flush unmatched files: %w", err))
	}
}

type nasInventoryModeRequest struct {
	Mode string `json:"mode"`
}

type nasReleaseBatchItem struct {
	ClipID      int64 `json:"clip_id"`
	RecordingID int64 `json:"recording_id"`
}

type nasReleaseBatchRequest struct {
	Clips []nasReleaseBatchItem `json:"clips"`
}

// handleAccountClipsReleaseBatch replaces hundreds of serial HTTP round trips
// with one atomic operation. Any invalid or unconfirmed item rolls the whole page
// back, so the NAS cursor can advance all-or-nothing without creating holes.
func (s *Server) handleAccountClipsReleaseBatch(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	if principal.APIKeyID == nil {
		util.WriteError(w, http.StatusForbidden, "batch release requires a NAS pull key")
		return
	}
	var req nasReleaseBatchRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if len(req.Clips) < 1 || len(req.Clips) > nasInventoryMaxBatch {
		util.WriteError(w, http.StatusBadRequest, fmt.Sprintf("clips must contain 1 to %d items", nasInventoryMaxBatch))
		return
	}
	seen := make(map[int64]bool, len(req.Clips))
	for _, item := range req.Clips {
		if item.ClipID <= 0 || item.RecordingID <= 0 || seen[item.ClipID] {
			util.WriteError(w, http.StatusBadRequest, "invalid or duplicate batch release item")
			return
		}
		seen[item.ClipID] = true
	}
	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("begin batch release: %v", err))
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()
	var connectionID int64
	var mode string
	if err := tx.QueryRow(r.Context(), `SELECT id,inventory_mode FROM connections WHERE api_key_id=$1 AND account_id=$2 FOR UPDATE`, *principal.APIKeyID, principal.AccountID).Scan(&connectionID, &mode); errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusForbidden, "no connection for this key")
		return
	} else if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load batch release connection: %v", err))
		return
	}
	for _, item := range req.Clips {
		var path, sha string
		var size int64
		var purgedAt, releasedAt *time.Time
		err := tx.QueryRow(r.Context(), `
			SELECT c.display_path,c.size_bytes,lower(c.sha256),c.purged_at,c.released_at
			FROM recording_clips c JOIN recordings rec ON rec.id=c.recording_id
			WHERE c.id=$1 AND c.recording_id=$2 AND rec.account_id=$3 AND rec.delivery='nas_pull'
			FOR UPDATE OF c
		`, item.ClipID, item.RecordingID, principal.AccountID).Scan(&path, &size, &sha, &purgedAt, &releasedAt)
		if errors.Is(err, pgx.ErrNoRows) {
			util.WriteError(w, http.StatusNotFound, fmt.Sprintf("clip %d not found", item.ClipID))
			return
		}
		if err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load batch clip %d: %v", item.ClipID, err))
			return
		}
		if purgedAt != nil || releasedAt != nil {
			continue
		}
		if mode == "enforce" {
			var confirmed bool
			if err := tx.QueryRow(r.Context(), `
				SELECT EXISTS(SELECT 1 FROM nas_inventory_files
				WHERE connection_id=$1 AND clip_id=$2 AND state='present' AND relative_path=$3
				  AND size_bytes=$4 AND sha256=$5 AND verified_at>=now()-$6::interval)
			`, connectionID, item.ClipID, path, size, sha, nasInventoryFreshness.String()).Scan(&confirmed); err != nil {
				util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("verify batch clip %d: %v", item.ClipID, err))
				return
			}
			if !confirmed {
				util.WriteError(w, http.StatusConflict, fmt.Sprintf("release retained: clip %d lacks a recent exact NAS checksum proof", item.ClipID))
				return
			}
		}
		if _, err := tx.Exec(r.Context(), `UPDATE recording_clips SET released_at=now() WHERE id=$1 AND released_at IS NULL`, item.ClipID); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("release batch clip %d: %v", item.ClipID, err))
			return
		}
	}
	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("commit batch release: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"ok": true, "released": len(req.Clips)})
}

// handleAccountConnectionInventoryMode makes enforcement an explicit, guarded
// rollout step. It cannot be enabled until a recent complete scan has no local
// mismatches and every active server clip is represented as present locally.
func (s *Server) handleAccountConnectionInventoryMode(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	connectionID, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	var req nasInventoryModeRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.Mode != "observe" && req.Mode != "enforce" {
		util.WriteError(w, http.StatusBadRequest, "mode must be observe or enforce")
		return
	}
	if req.Mode == "enforce" {
		var ready bool
		err := s.pool.QueryRow(r.Context(), `
			SELECT COALESCE(conn.inventory_scan_completed_at >= now()-$3::interval, false)
			       AND conn.inventory_mismatches=0
			       AND NOT EXISTS (
			         SELECT 1 FROM recording_clips c JOIN recordings rec ON rec.id=c.recording_id
			         WHERE rec.account_id=conn.account_id AND rec.delivery='nas_pull'
			           AND c.purged_at IS NULL AND c.released_at IS NULL
			           AND NOT EXISTS (
			             SELECT 1 FROM nas_inventory_files i
			             WHERE i.connection_id=conn.id AND i.clip_id=c.id AND i.state='present'
			               AND i.relative_path=c.display_path AND i.size_bytes=c.size_bytes
			               AND i.sha256=lower(c.sha256) AND i.verified_at>=now()-$3::interval
			           )
			       )
			FROM connections conn WHERE conn.id=$1 AND conn.account_id=$2
		`, connectionID, principal.AccountID, nasInventoryFreshness.String()).Scan(&ready)
		if errors.Is(err, pgx.ErrNoRows) {
			util.WriteError(w, http.StatusNotFound, "connection not found")
			return
		}
		if err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("check inventory enforcement readiness: %v", err))
			return
		}
		if !ready {
			util.WriteError(w, http.StatusConflict, "inventory enforcement requires a recent complete scan, zero mismatches, and no unconfirmed active server clips")
			return
		}
	}
	ct, err := s.pool.Exec(r.Context(), `UPDATE connections SET inventory_mode=$1,updated_at=now() WHERE id=$2 AND account_id=$3`, req.Mode, connectionID, principal.AccountID)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("update inventory mode: %v", err))
		return
	}
	if ct.RowsAffected() == 0 {
		util.WriteError(w, http.StatusNotFound, "connection not found")
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"ok": true, "mode": req.Mode})
}
