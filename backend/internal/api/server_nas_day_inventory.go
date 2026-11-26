package api

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/daydemir/stoarama/backend/internal/util"
)

// Read-only stream-day NAS inventory (migration 0272, r2-copy-v8-inventory only).
//
// stoaramactl nas-day-inventory scope approves (recording, local date) days in
// nas_restore_day_scope; request queues one unit per approved day, and only an
// approved day is leased. The client leases a unit, lists that stream's raw and joined day
// directories read-only (its pinned folder map, plus the directories of the
// catalog's clips for that day, which the lease sends), and reports every media
// file with the sha256 of the bytes it read. The report lands in
// nas_day_inventory_files. Each raw file whose name and size are those of
// exactly one clip of the recording starting on the unit's day, and of no other
// file of the unit, is that clip's NAS copy. When its bytes are the catalogued
// sha256, the clip may be restored to its own key from that path
// (nas_restore_inventoried_original); the restore re-hashes it before and after
// upload. The match is never written to nas_inventory_files: that ledger is the
// client's own sidecar-backed evidence, and cleanup, retirement and the cleanup
// runner's final check read any present row there as exact-path evidence.

const (
	nasDayInventoryLeaseTTL  = 4 * time.Hour
	nasDayInventoryIdleRetry = 600
	nasDayInventoryMaxFiles  = 50000
	nasDayInventoryMaxError  = 500
)

// nasDayInventoryKind is where a reported file lies on the NAS.
type nasDayInventoryKind string

const (
	nasDayInventoryRaw    nasDayInventoryKind = "raw"
	nasDayInventoryJoined nasDayInventoryKind = "joined"
)

type nasDayInventoryLeaseRequest struct {
	ClientVersion string `json:"client_version"`
}

type nasDayInventoryUnit struct {
	UnitID       int64     `json:"unit_id"`
	Attempt      int       `json:"attempt"`
	RecordingID  int64     `json:"recording_id"`
	StreamID     int64     `json:"stream_id"`
	LocalDate    string    `json:"local_date"`
	Timezone     string    `json:"timezone"`
	ExpectedDirs []string  `json:"expected_dirs"`
	ExpiresAt    time.Time `json:"expires_at"`
}

type nasDayInventoryLeaseResponse struct {
	RetryAfterSec int                  `json:"retry_after_sec"`
	Unit          *nasDayInventoryUnit `json:"unit"`
}

type nasDayInventoryFile struct {
	RelativePath string              `json:"relative_path"`
	Kind         nasDayInventoryKind `json:"kind"`
	SizeBytes    int64               `json:"size_bytes"`
	MTimeNS      int64               `json:"mtime_ns"`
	SHA256       string              `json:"sha256"`
}

// nasDayInventoryResult is a whole scan (Error empty) or why there is none.
type nasDayInventoryResult struct {
	Attempt       int                   `json:"attempt"`
	StartedAt     time.Time             `json:"started_at"`
	ClientVersion string                `json:"client_version"`
	Files         []nasDayInventoryFile `json:"files"`
	Error         string                `json:"error"`
}

func validateNASDayInventoryResult(req nasDayInventoryResult, now time.Time) error {
	switch {
	case req.Attempt < 1:
		return errors.New("attempt must be positive")
	case req.StartedAt.IsZero() || req.StartedAt.After(now.Add(nasInventoryMaxFutureSkew)):
		return errors.New("started_at must be set and not in the future")
	case len(req.ClientVersion) > 64 || !relayArtifactName.MatchString(req.ClientVersion):
		return errors.New("invalid client_version")
	case len(req.Error) > nasDayInventoryMaxError:
		return errors.New("error is too long")
	case req.Error != "" && len(req.Files) > 0:
		return errors.New("a failed scan reports no files")
	case len(req.Files) > nasDayInventoryMaxFiles:
		return fmt.Errorf("a unit reports at most %d files", nasDayInventoryMaxFiles)
	}
	seen := make(map[string]bool, len(req.Files))
	for _, f := range req.Files {
		joined := strings.HasPrefix(f.RelativePath, "joined/")
		switch {
		case !validNASRelativePath(f.RelativePath) || seen[f.RelativePath] || !strings.HasSuffix(f.RelativePath, ".mp4"):
			return fmt.Errorf("invalid or duplicate path %q", f.RelativePath)
		case f.Kind == nasDayInventoryRaw && (joined || nasInventoryExcludedPath(f.RelativePath)),
			f.Kind == nasDayInventoryJoined && !joined,
			f.Kind != nasDayInventoryRaw && f.Kind != nasDayInventoryJoined:
			return fmt.Errorf("path %q is not a %q file", f.RelativePath, f.Kind)
		case f.SizeBytes < 0 || f.MTimeNS < 0 || len(f.SHA256) != 64 || !lowerHex(f.SHA256):
			return fmt.Errorf("invalid size, mtime or sha256 for %q", f.RelativePath)
		}
		seen[f.RelativePath] = true
	}
	return nil
}

// The directories of the recording's clips that start on the unit's local day.
const nasDayInventoryExpectedDirsSQL = `SELECT coalesce(array_agg(DISTINCT regexp_replace(c.display_path,'/[^/]*$','') ORDER BY regexp_replace(c.display_path,'/[^/]*$','')),'{}')
 FROM recording_clips c
 WHERE c.recording_id=$1 AND c.display_path LIKE '%/%' AND c.display_path NOT LIKE 'joined/%'
  AND c.clip_start_at>=($2::date)::timestamp AT TIME ZONE $3 AND c.clip_start_at<($2::date+1)::timestamp AT TIME ZONE $3`

func (s *Server) handleAccountConnectionDayInventoryLease(w http.ResponseWriter, r *http.Request) {
	var req nasDayInventoryLeaseRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	ctx := r.Context()
	connectionID, ok := s.nasConnection(ctx, w, r, nasConnectionInventory)
	if !ok {
		return
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "begin inventory lease failed")
		return
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `UPDATE nas_day_inventory_units SET
  state=CASE WHEN attempts>=max_attempts THEN 'failed' ELSE 'pending' END,
  completed_at=CASE WHEN attempts>=max_attempts THEN now() END,
  lease_expires_at=NULL, last_error='lease expired before a result was reported'
 WHERE connection_id=$1 AND state='leased' AND lease_expires_at<now()`, connectionID); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "requeue expired inventories failed")
		return
	}
	var unit nasDayInventoryUnit
	err = tx.QueryRow(ctx, `UPDATE nas_day_inventory_units u SET state='leased', attempts=u.attempts+1, leased_at=now(),
  lease_expires_at=now()+$2::bigint*interval '1 microsecond', client_version=$3
 FROM (SELECT u.id FROM nas_day_inventory_units u WHERE u.connection_id=$1 AND u.state='pending'
   AND EXISTS(SELECT 1 FROM nas_restore_day_scope s WHERE s.recording_id=u.recording_id AND s.local_date=u.local_date)
   ORDER BY u.id LIMIT 1 FOR UPDATE SKIP LOCKED) picked,
  recordings r, nas_media_keep20_windows k
 WHERE u.id=picked.id AND r.id=u.recording_id AND k.stream_id=r.stream_id
 RETURNING u.id, u.attempts, u.recording_id, r.stream_id, u.local_date::text, k.timezone, u.lease_expires_at`,
		connectionID, nasDayInventoryLeaseTTL.Microseconds(), req.ClientVersion).Scan(
		&unit.UnitID, &unit.Attempt, &unit.RecordingID, &unit.StreamID, &unit.LocalDate, &unit.Timezone, &unit.ExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteJSON(w, http.StatusOK, nasDayInventoryLeaseResponse{RetryAfterSec: nasDayInventoryIdleRetry})
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "lease inventory failed")
		return
	}
	if err := tx.QueryRow(ctx, nasDayInventoryExpectedDirsSQL, unit.RecordingID, unit.LocalDate, unit.Timezone).Scan(&unit.ExpectedDirs); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "load expected inventory directories failed")
		return
	}
	if err := tx.Commit(ctx); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "commit inventory lease failed")
		return
	}
	util.WriteJSON(w, http.StatusOK, nasDayInventoryLeaseResponse{Unit: &unit})
}

// Each raw file of the unit whose name and size are those of exactly one clip
// of the recording starting on the unit's local day, where no other file of the
// unit claims that clip.
const nasDayInventoryMatchSQL = `WITH m AS (
 SELECT f.relative_path, c.id AS clip_id,
  count(*) OVER (PARTITION BY f.relative_path) AS clips, count(*) OVER (PARTITION BY c.id) AS files
 FROM nas_day_inventory_units u
 JOIN recordings r ON r.id=u.recording_id
 JOIN nas_media_keep20_windows w ON w.stream_id=r.stream_id
 JOIN nas_day_inventory_files f ON f.unit_id=u.id AND f.kind='raw'
 JOIN recording_clips c ON c.recording_id=u.recording_id AND c.size_bytes=f.size_bytes
  AND regexp_replace(c.display_path,'^.*/','')=regexp_replace(f.relative_path,'^.*/','')
  AND (c.clip_start_at AT TIME ZONE w.timezone)::date=u.local_date
 WHERE u.id=$1)
UPDATE nas_day_inventory_files f SET clip_id=m.clip_id FROM m
 WHERE f.unit_id=$1 AND f.relative_path=m.relative_path AND m.clips=1 AND m.files=1`

func (s *Server) handleAccountConnectionDayInventoryResult(w http.ResponseWriter, r *http.Request) {
	unitID, ok := parseInt64Path(w, r, "unitId")
	if !ok {
		return
	}
	var req nasDayInventoryResult
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	now := time.Now().UTC()
	if err := validateNASDayInventoryResult(req, now); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	ctx := r.Context()
	connectionID, ok := s.nasConnection(ctx, w, r, nasConnectionInventory)
	if !ok {
		return
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "begin inventory result failed")
		return
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var state string
	var attempts int
	err = tx.QueryRow(ctx, `SELECT state, attempts FROM nas_day_inventory_units WHERE id=$1 AND connection_id=$2 FOR UPDATE`,
		unitID, connectionID).Scan(&state, &attempts)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusNotFound, "inventory unit not found")
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "load inventory unit failed")
		return
	}
	if state != "leased" || attempts != req.Attempt {
		util.WriteError(w, http.StatusConflict, "inventory lease is no longer current")
		return
	}
	if req.Error != "" {
		if _, err := tx.Exec(ctx, `UPDATE nas_day_inventory_units SET
  state=CASE WHEN attempts>=max_attempts THEN 'failed' ELSE 'pending' END,
  completed_at=CASE WHEN attempts>=max_attempts THEN now() END,
  lease_expires_at=NULL, last_error=$2, client_version=$3 WHERE id=$1`, unitID, req.Error, req.ClientVersion); err != nil {
			util.WriteError(w, http.StatusInternalServerError, "record inventory failure failed")
			return
		}
		if err := tx.Commit(ctx); err != nil {
			util.WriteError(w, http.StatusInternalServerError, "commit inventory failure failed")
			return
		}
		util.WriteJSON(w, http.StatusOK, map[string]any{"state": "retry"})
		return
	}
	columns := [5][]any{}
	raw := 0
	for _, f := range req.Files {
		for i, v := range []any{f.RelativePath, string(f.Kind), f.SizeBytes, f.MTimeNS, f.SHA256} {
			columns[i] = append(columns[i], v)
		}
		if f.Kind == nasDayInventoryRaw {
			raw++
		}
	}
	if _, err := tx.Exec(ctx, `INSERT INTO nas_day_inventory_files(unit_id,relative_path,kind,size_bytes,mtime_ns,sha256)
 SELECT $1,p,k,s,m,h FROM unnest($2::text[],$3::text[],$4::bigint[],$5::bigint[],$6::text[]) AS t(p,k,s,m,h)`,
		unitID, columns[0], columns[1], columns[2], columns[3], columns[4]); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "record inventory files failed")
		return
	}
	matched, err := tx.Exec(ctx, nasDayInventoryMatchSQL, unitID)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "match inventory files failed")
		return
	}
	started := req.StartedAt.UTC()
	if _, err := tx.Exec(ctx, `UPDATE nas_day_inventory_units SET state='completed', completed_at=now(), lease_expires_at=NULL,
  scan_started_at=$2, client_version=$3, last_error='' WHERE id=$1`, unitID, started, req.ClientVersion); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "complete inventory unit failed")
		return
	}
	if err := tx.Commit(ctx); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "commit inventory result failed")
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"state": "completed", "raw": raw, "joined": len(req.Files) - raw, "matched": matched.RowsAffected()})
}
