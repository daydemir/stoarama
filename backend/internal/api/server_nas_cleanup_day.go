package api

import (
	"context"
	"crypto/rand"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/daydemir/stoarama/backend/internal/util"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Day-directory NAS-only cleanup. Owner (Deniz), verbatim: "yes, delete"; "if
// its outside of keep everything, then a nas copy only needs to be checked if
// its in the keep 20? otherwise it can be deleted right?"; "yea we don tneed
// exreme intense checkingm"; "lift nas cleanup dialy limit please".
//
// A unit is one stream-day of raw NAS footage in one directory: a plaza
// <folder>/<Month>/<DD-Weekday> directory, or the files of one local day in a
// flat <folder>/<recording>/{continuous,<job>} directory. Its stream comes from
// the reviewed folder map the NAS client pins, its date from the path. The
// server issues one permit per unit after checking the whole local day lies
// outside the stream's pinned 20-day window (with a one-day guard band on
// each side), the stream is not Keep Everything, and no catalogued clip of the
// unit is held. There is no R2 copy check and no daily cap.

//go:embed nas_cleanup_folder_streams.json
var cleanupFolderStreamsJSON []byte

// Same bytes and digest as the NAS client's CLEANUP_FOLDER_STREAMS_SHA256.
const cleanupFolderStreamsSHA = "94efc6fe9f0b83db55be31d4c2fad7f47cff781fd01d88bc33c4c90cae0e7ab8"

const (
	nasCleanupDayMaxUnits  = 1000
	nasCleanupDayPermitTTL = 10 * time.Minute
	nasCleanupDayDigestTag = "stoarama-nas-cleanup-day-v1"
)

type nasCleanupDayOutcome string

const (
	nasCleanupDayDeleted nasCleanupDayOutcome = "deleted"
	nasCleanupDayPartial nasCleanupDayOutcome = "partial"
	nasCleanupDayRefused nasCleanupDayOutcome = "refused"
)

var cleanupFolderStreams = sync.OnceValues(func() (map[string]int64, error) {
	if cleanupSHA(cleanupFolderStreamsJSON) != cleanupFolderStreamsSHA {
		return nil, errors.New("reviewed folder map hash differs")
	}
	var folders map[string]int64
	if err := json.Unmarshal(cleanupFolderStreamsJSON, &folders); err != nil || len(folders) == 0 {
		return nil, errors.New("invalid reviewed folder map")
	}
	policy, err := cleanupEmbeddedPolicy()
	if err != nil {
		return nil, err
	}
	for key, stream := range folders {
		if _, ok := policy.Streams[stream]; key == "" || !ok {
			return nil, errors.New("reviewed folder map names an unpinned stream")
		}
	}
	return folders, nil
})

// cleanupFolderKey mirrors the NAS client's cleanup_folder_key for raw paths.
func cleanupFolderKey(dir string) string {
	parts := strings.Split(dir, "/")
	switch {
	case parts[0] == "joined":
		return ""
	case parts[0] == "recordings":
		if len(parts) < 2 {
			return ""
		}
		return "recordings/" + parts[1]
	default:
		return parts[0]
	}
}

const (
	cleanupDayMonthRe   = `January|February|March|April|May|June|July|August|September|October|November|December`
	cleanupDayWeekdayRe = `Sunday|Monday|Tuesday|Wednesday|Thursday|Friday|Saturday`
)

var (
	cleanupDayPlazaDir  = regexp.MustCompile(`^(?:[^/]+/)+(` + cleanupDayMonthRe + `)/(0[1-9]|[12][0-9]|3[01])-(` + cleanupDayWeekdayRe + `)$`)
	cleanupDayFlatDir   = regexp.MustCompile(`^(?:[^/]+/)+[1-9][0-9]*/(continuous|[1-9][0-9]*)$`)
	cleanupDayPlazaFile = regexp.MustCompile(`^[^/]*_(20[0-9]{2})_(` + cleanupDayMonthRe + `)_W([1-6])_(` + cleanupDayWeekdayRe + `)_hour_(?:[0-9]{2}|[0-9]{6})\.mp4$`)
	cleanupDaySeconds   = regexp.MustCompile(`^([1-9][0-9]{9})\.mp4$`)
	cleanupDayMillis    = regexp.MustCompile(`^([1-9][0-9]{12})\.mp4$`)
)

func cleanupDayMonth(name string) time.Month {
	for m := time.January; m <= time.December; m++ {
		if m.String() == name {
			return m
		}
	}
	return 0
}

// cleanupDayFileDate is the local date a raw media name in dir names, or false.
// The NAS client and the preparer implement the same rule.
func cleanupDayFileDate(dir, name string, loc *time.Location) (time.Time, bool) {
	if m := cleanupDayPlazaDir.FindStringSubmatch(dir); m != nil {
		f := cleanupDayPlazaFile.FindStringSubmatch(name)
		if f == nil || f[2] != m[1] || f[4] != m[3] {
			return time.Time{}, false
		}
		year, _ := strconv.Atoi(f[1])
		day, _ := strconv.Atoi(m[2])
		week, _ := strconv.Atoi(f[3])
		d := time.Date(year, cleanupDayMonth(m[1]), day, 0, 0, 0, 0, time.UTC)
		if d.Day() != day || d.Weekday().String() != m[3] || (day-1)/7+1 != week {
			return time.Time{}, false
		}
		return d, true
	}
	m := cleanupDayFlatDir.FindStringSubmatch(dir)
	if m == nil {
		return time.Time{}, false
	}
	var at time.Time
	if m[1] == "continuous" {
		f := cleanupDaySeconds.FindStringSubmatch(name)
		if f == nil {
			return time.Time{}, false
		}
		seconds, _ := strconv.ParseInt(f[1], 10, 64)
		at = time.Unix(seconds, 0)
	} else {
		f := cleanupDayMillis.FindStringSubmatch(name)
		if f == nil {
			return time.Time{}, false
		}
		millis, _ := strconv.ParseInt(f[1], 10, 64)
		at = time.UnixMilli(millis)
	}
	local := at.In(loc)
	return time.Date(local.Year(), local.Month(), local.Day(), 0, 0, 0, 0, time.UTC), true
}

// cleanupDayOutside is true when the whole local day lies outside the window
// with a full guard day between them on either side. The guard band covers a
// clip that crosses midnight and any difference between naming and canonical
// timezones. first and last are the local dates of the window's first and
// final instants.
func cleanupDayOutside(w cleanupKeepWindow, day time.Time) bool {
	loc, err := time.LoadLocation(w.Timezone)
	if err != nil {
		return false
	}
	start, end := w.start.In(loc), w.end.Add(-time.Microsecond).In(loc)
	first := time.Date(start.Year(), start.Month(), start.Day(), 0, 0, 0, 0, time.UTC)
	last := time.Date(end.Year(), end.Month(), end.Day(), 0, 0, 0, 0, time.UTC)
	return !day.After(first.AddDate(0, 0, -2)) || !day.Before(last.AddDate(0, 0, 2))
}

type nasCleanupDayUnit struct {
	RelativeDir string `json:"relative_dir"`
	LocalDate   string `json:"local_date"`
	StreamID    int64  `json:"stream_id"`
	Files       int64  `json:"files"`
	Bytes       int64  `json:"bytes"`
}

// cleanupDayDigest binds the ordered units to the policy. The preparer and
// runner compute the same line format (paths never contain a newline).
func cleanupDayDigest(units []nasCleanupDayUnit) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s\npolicy %s\n", nasCleanupDayDigestTag, cleanupPolicySHA)
	for _, u := range units {
		fmt.Fprintf(&b, "unit %d %s %d %d %s\n", u.StreamID, u.LocalDate, u.Files, u.Bytes, u.RelativeDir)
	}
	return cleanupSHA([]byte(b.String()))
}

// cleanupDayStatic checks everything a unit's own identity decides.
func cleanupDayStatic(u nasCleanupDayUnit) (cleanupKeepWindow, time.Time, *time.Location, error) {
	policy, err := cleanupEmbeddedPolicy()
	if err != nil {
		return cleanupKeepWindow{}, time.Time{}, nil, err
	}
	folders, err := cleanupFolderStreams()
	if err != nil {
		return cleanupKeepWindow{}, time.Time{}, nil, err
	}
	if !validCleanupPath(u.RelativeDir) || (!cleanupDayPlazaDir.MatchString(u.RelativeDir) && !cleanupDayFlatDir.MatchString(u.RelativeDir)) {
		return cleanupKeepWindow{}, time.Time{}, nil, errors.New("directory is not a dated raw NAS layout")
	}
	if stream, ok := folders[cleanupFolderKey(u.RelativeDir)]; !ok || stream != u.StreamID {
		return cleanupKeepWindow{}, time.Time{}, nil, errors.New("reviewed folder map does not assign this directory to the stream")
	}
	w, ok := policy.Streams[u.StreamID]
	if !ok || w.KeepEverything {
		return cleanupKeepWindow{}, time.Time{}, nil, errors.New("stream is Keep Everything or has no pinned window")
	}
	day, err := time.Parse(time.DateOnly, u.LocalDate)
	if err != nil || day.Year() < 2020 || day.Year() >= 2028 {
		return cleanupKeepWindow{}, time.Time{}, nil, errors.New("invalid local date")
	}
	if m := cleanupDayPlazaDir.FindStringSubmatch(u.RelativeDir); m != nil {
		if dd, _ := strconv.Atoi(m[2]); day.Month().String() != m[1] || day.Day() != dd || day.Weekday().String() != m[3] {
			return cleanupKeepWindow{}, time.Time{}, nil, errors.New("directory does not name the local date")
		}
	}
	if !cleanupDayOutside(w, day) {
		return cleanupKeepWindow{}, time.Time{}, nil, errors.New("day is inside or adjacent to the pinned keep window")
	}
	loc, err := time.LoadLocation(w.Timezone)
	return w, day, loc, err
}

type cleanupDayQuerier interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

// Every catalogued clip directly in the directory ($1; $2 its LIKE pattern),
// of any stream and at any time: Go refuses another stream's clip and checks exactly those whose name
// names the unit's date, whatever their catalogued times.
const cleanupDayUnitSQL = `SELECT c.display_path,c.clip_start_at,c.clip_end_at,r.stream_id,nas_cleanup_day_clip_held(c.id),
 coalesce((SELECT f.size_bytes FROM nas_inventory_files f WHERE f.connection_id=13 AND f.clip_id=c.id
  AND f.state='present' AND f.relative_path=c.display_path),-1)
 FROM recordings r JOIN recording_clips c ON c.recording_id=r.id
 WHERE r.account_id=47 AND c.display_path LIKE $2 AND strpos(substr(c.display_path,length($1)+2),'/')=0`

// cleanupDayDirPattern is the left-anchored LIKE pattern for the directory's
// entries, sargable on recording_clips_display_path_pattern (0248).
var cleanupDayDirPattern = strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)

// cleanupDayCheck returns the unit's catalogued NAS files and bytes, or why
// the whole directory-day must stay.
func cleanupDayCheck(ctx context.Context, q cleanupDayQuerier, u nasCleanupDayUnit) (int64, int64, error) {
	w, day, loc, err := cleanupDayStatic(u)
	if err != nil {
		return 0, 0, err
	}
	var held bool
	if err := q.QueryRow(ctx, `SELECT NOT EXISTS(SELECT 1 FROM nas_media_keep20_epoch WHERE connection_id=13) OR nas_cleanup_day_dir_held($1)`, u.RelativeDir).Scan(&held); err != nil {
		return 0, 0, err
	}
	if held {
		return 0, 0, errors.New("keep20 epoch inactive or delivery path hold in directory")
	}
	rows, err := q.Query(ctx, cleanupDayUnitSQL, u.RelativeDir, cleanupDayDirPattern.Replace(u.RelativeDir)+"/%")
	if err != nil {
		return 0, 0, err
	}
	defer rows.Close()
	var files, bytes int64
	for rows.Next() {
		var p string
		var a, b *time.Time
		var stream *int64
		var clipHeld bool
		var size int64
		if err := rows.Scan(&p, &a, &b, &stream, &clipHeld, &size); err != nil {
			return 0, 0, err
		}
		if stream == nil || *stream != u.StreamID {
			return 0, 0, errors.New("catalogued clip in the directory belongs to another stream")
		}
		named, ok := cleanupDayFileDate(u.RelativeDir, path.Base(p), loc)
		if !ok {
			return 0, 0, errors.New("catalogued clip name does not name a local date")
		}
		if !named.Equal(day) {
			continue
		}
		if a == nil || b == nil || !b.After(*a) || a.Before(w.end) && b.After(w.start) {
			return 0, 0, errors.New("catalogued clip intersects the pinned keep window")
		}
		if clipHeld {
			return 0, 0, errors.New("catalogued clip is held")
		}
		if size >= 0 {
			files++
			bytes += size
		}
	}
	if err := rows.Err(); err != nil {
		return 0, 0, err
	}
	if files == 0 {
		return 0, 0, errors.New("no catalogued NAS media in the directory-day")
	}
	return files, bytes, nil
}

var cleanupDayLabel = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,79}$`)

func (s *Server) handleServiceNASCleanupDayCreate(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Label     string              `json:"label"`
		PolicySHA string              `json:"policy_sha256"`
		PlanSHA   string              `json:"plan_sha256"`
		Files     int64               `json:"files"`
		Bytes     int64               `json:"bytes"`
		Units     []nasCleanupDayUnit `json:"units"`
	}
	if util.DecodeJSON(r, &req) != nil || !cleanupDayLabel.MatchString(req.Label) || req.PolicySHA != cleanupPolicySHA ||
		len(req.Units) < 1 || len(req.Units) > nasCleanupDayMaxUnits || cleanupDayDigest(req.Units) != req.PlanSHA {
		util.WriteError(w, 400, "label, active policy, 1 to 1000 units and their exact digest required")
		return
	}
	seen := map[[2]string]bool{}
	var files, bytes int64
	for _, u := range req.Units {
		key := [2]string{u.RelativeDir, u.LocalDate}
		if seen[key] || u.Files <= 0 || u.Bytes <= 0 {
			util.WriteError(w, 400, "duplicate or empty unit")
			return
		}
		seen[key] = true
		files += u.Files
		bytes += u.Bytes
	}
	if files != req.Files || bytes != req.Bytes {
		util.WriteError(w, 400, "unit totals differ from the plan totals")
		return
	}
	ctx := r.Context()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		util.WriteError(w, 500, "day cleanup admission unavailable")
		return
	}
	defer tx.Rollback(ctx)
	for n, u := range req.Units {
		gotFiles, gotBytes, err := cleanupDayCheck(ctx, tx, u)
		if err == nil && (gotFiles != u.Files || gotBytes != u.Bytes) {
			err = fmt.Errorf("catalogued NAS media is %d files, %d bytes", gotFiles, gotBytes)
		}
		if err != nil {
			util.WriteError(w, 409, fmt.Sprintf("unit %d (%s %s): %v", n+1, u.RelativeDir, u.LocalDate, err))
			return
		}
	}
	var id int64
	if err = tx.QueryRow(ctx, `INSERT INTO nas_cleanup_day_plans(account_id,connection_id,label,policy_sha256,plan_sha256,units,files,bytes)
 VALUES(47,13,$1,$2,$3,$4,$5,$6) RETURNING id`, req.Label, cleanupPolicySHA, req.PlanSHA, len(req.Units), files, bytes).Scan(&id); err != nil {
		util.WriteError(w, 409, "day cleanup plan label or digest already admitted")
		return
	}
	units, _ := json.Marshal(req.Units)
	if _, err = tx.Exec(ctx, `INSERT INTO nas_cleanup_day_units(plan_id,ordinal,stream_id,relative_dir,local_date,files,bytes)
 SELECT $1,n,(u->>'stream_id')::bigint,u->>'relative_dir',(u->>'local_date')::date,(u->>'files')::bigint,(u->>'bytes')::bigint
 FROM jsonb_array_elements($2::jsonb) WITH ORDINALITY x(u,n)`, id, string(units)); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			util.WriteError(w, 409, "a directory-day is already in a live day cleanup plan")
		} else {
			util.WriteError(w, 500, "day cleanup unit admission failed")
		}
		return
	}
	if err = tx.Commit(ctx); err != nil {
		util.WriteError(w, 500, "day cleanup admission failed")
		return
	}
	util.WriteJSON(w, 200, map[string]any{"id": id, "label": req.Label, "plan_sha256": req.PlanSHA, "units": len(req.Units),
		"files": files, "bytes": bytes, "mode": "dry_run", "deletion_enabled": false})
}

func (s *Server) handleServiceNASCleanupDayStatus(w http.ResponseWriter, r *http.Request) {
	id, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	var body []byte
	err := s.pool.QueryRow(r.Context(), `SELECT jsonb_build_object('id',p.id,'label',p.label,'policy_sha256',p.policy_sha256,
 'plan_sha256',p.plan_sha256,'mode',p.mode,'approval_reason',p.approval_reason,'approved_until',p.approved_until,
 'files',p.files,'bytes',p.bytes,
 'units',(SELECT coalesce(jsonb_agg(jsonb_build_object('unit_id',u.id,'ordinal',u.ordinal,'stream_id',u.stream_id,
   'relative_dir',u.relative_dir,'local_date',u.local_date::text,'files',u.files,'bytes',u.bytes,'state',u.state,
   'attempt',u.attempt,'permit_live',u.state='permitted' AND u.permit_expires_at>clock_timestamp(),
   'refusal',u.refusal,'receipts',u.receipts) ORDER BY u.ordinal),'[]') FROM nas_cleanup_day_units u WHERE u.plan_id=p.id),
 'deleted_media_files',(SELECT coalesce(sum((e->>'media_files')::bigint),0) FROM nas_cleanup_day_units u
   CROSS JOIN LATERAL jsonb_array_elements(u.receipts) e WHERE u.plan_id=p.id),
 'deleted_media_bytes',(SELECT coalesce(sum((e->>'media_bytes')::bigint),0) FROM nas_cleanup_day_units u
   CROSS JOIN LATERAL jsonb_array_elements(u.receipts) e WHERE u.plan_id=p.id))
 FROM nas_cleanup_day_plans p WHERE p.id=$1 AND p.account_id=47 AND p.connection_id=13`, id).Scan(&body)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, 404, "day cleanup plan not found")
		return
	}
	if err != nil {
		util.WriteError(w, 500, "day cleanup plan unavailable")
		return
	}
	var out map[string]any
	if json.Unmarshal(body, &out) != nil {
		util.WriteError(w, 500, "day cleanup plan unavailable")
		return
	}
	out["master_deletion_enabled"] = s.cfg.NASCleanupDeletionEnabled
	util.WriteJSON(w, 200, out)
}

func (s *Server) handleServiceNASCleanupDayControl(w http.ResponseWriter, r *http.Request) {
	id, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	var req struct {
		PlanSHA       string `json:"plan_sha256"`
		Mode          string `json:"mode"`
		Reason        string `json:"reason"`
		ApprovalHours int    `json:"approval_hours"`
	}
	if util.DecodeJSON(r, &req) != nil || !stitchSHA(req.PlanSHA) || strings.TrimSpace(req.Reason) == "" || len(req.Reason) > 500 ||
		(req.Mode != "enabled" && req.Mode != "paused") || (req.Mode == "enabled" && (req.ApprovalHours < 1 || req.ApprovalHours > 24)) {
		util.WriteError(w, 400, "plan digest, enabled|paused, reason and 1 to 24 approval hours required")
		return
	}
	ctx := r.Context()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		util.WriteError(w, 500, "day cleanup control unavailable")
		return
	}
	defer tx.Rollback(ctx)
	var mode string
	if err = tx.QueryRow(ctx, `SELECT mode FROM nas_cleanup_day_plans WHERE id=$1 AND plan_sha256=$2 FOR UPDATE`, id, req.PlanSHA).Scan(&mode); err != nil {
		util.WriteError(w, 409, "day cleanup plan digest differs")
		return
	}
	if req.Mode == "paused" {
		if mode != "enabled" {
			util.WriteError(w, 409, "only an enabled day cleanup plan can pause")
			return
		}
		_, err = tx.Exec(ctx, `UPDATE nas_cleanup_day_plans SET mode='paused',approval_reason=$2,updated_at=now() WHERE id=$1`, id, req.Reason)
	} else {
		err = s.cleanupDayEnable(ctx, tx, id, mode, req.Reason, req.ApprovalHours)
	}
	if err != nil {
		util.WriteError(w, 409, err.Error())
		return
	}
	if err = tx.Commit(ctx); err != nil {
		util.WriteError(w, 500, "day cleanup control failed")
		return
	}
	util.WriteJSON(w, 200, map[string]any{"id": id, "plan_sha256": req.PlanSHA, "mode": req.Mode})
}

// cleanupDayEnable re-checks every pending unit; units that no longer qualify
// are refused, never deleted.
func (s *Server) cleanupDayEnable(ctx context.Context, tx pgx.Tx, id int64, mode, reason string, hours int) error {
	if mode != "dry_run" && mode != "paused" {
		return errors.New("only a dry-run or paused day cleanup plan can enable")
	}
	if err := cleanupMaintenanceCheck(ctx, tx); err != nil {
		return errors.New("cleanup maintenance gate unavailable or active")
	}
	var busy bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM nas_cleanup_plans WHERE mode='enabled')
 OR EXISTS(SELECT 1 FROM nas_cleanup_batches WHERE mode='enabled')
 OR EXISTS(SELECT 1 FROM nas_cleanup_day_plans WHERE mode='enabled')`).Scan(&busy); err != nil || busy {
		return errors.New("another cleanup plan is enabled")
	}
	rows, err := tx.Query(ctx, `SELECT id,stream_id,relative_dir,local_date::text,files,bytes FROM nas_cleanup_day_units
 WHERE plan_id=$1 AND state='pending' ORDER BY ordinal FOR UPDATE`, id)
	if err != nil {
		return err
	}
	type pending struct {
		id   int64
		unit nasCleanupDayUnit
	}
	var units []pending
	for rows.Next() {
		var p pending
		if err := rows.Scan(&p.id, &p.unit.StreamID, &p.unit.RelativeDir, &p.unit.LocalDate, &p.unit.Files, &p.unit.Bytes); err != nil {
			rows.Close()
			return err
		}
		units = append(units, p)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	eligible := 0
	for _, p := range units {
		if _, _, err := cleanupDayCheck(ctx, tx, p.unit); err != nil {
			if _, err := tx.Exec(ctx, `UPDATE nas_cleanup_day_units SET state='refused',refusal=$2,updated_at=now() WHERE id=$1`, p.id, err.Error()); err != nil {
				return err
			}
			continue
		}
		eligible++
	}
	if eligible == 0 {
		return errors.New("no pending unit still qualifies")
	}
	_, err = tx.Exec(ctx, `UPDATE nas_cleanup_day_plans SET mode='enabled',approval_reason=$2,
 approved_until=now()+$3::int*interval '1 hour',updated_at=now() WHERE id=$1`, id, reason, hours)
	return err
}

func (s *Server) handleServiceNASCleanupDayClose(w http.ResponseWriter, r *http.Request) {
	id, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	var req struct {
		PlanSHA string `json:"plan_sha256"`
		Reason  string `json:"reason"`
	}
	if util.DecodeJSON(r, &req) != nil || !stitchSHA(req.PlanSHA) || strings.TrimSpace(req.Reason) == "" || len(req.Reason) > 500 {
		util.WriteError(w, 400, "plan digest and reason required")
		return
	}
	ctx := r.Context()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		util.WriteError(w, 500, "day cleanup close unavailable")
		return
	}
	defer tx.Rollback(ctx)
	var mode string
	var live bool
	if err = tx.QueryRow(ctx, `SELECT mode,EXISTS(SELECT 1 FROM nas_cleanup_day_units WHERE plan_id=p.id AND state='permitted'
 AND permit_expires_at>clock_timestamp()) FROM nas_cleanup_day_plans p WHERE id=$1 AND plan_sha256=$2 FOR UPDATE`, id, req.PlanSHA).Scan(&mode, &live); err != nil {
		util.WriteError(w, 409, "day cleanup plan digest differs")
		return
	}
	if mode == "enabled" || live {
		util.WriteError(w, 409, "pause the plan and let live permits expire before close")
		return
	}
	if _, err = tx.Exec(ctx, `UPDATE nas_cleanup_day_units SET state='retired',updated_at=now() WHERE plan_id=$1 AND state IN ('pending','permitted')`, id); err != nil {
		util.WriteError(w, 500, "day cleanup close failed")
		return
	}
	if _, err = tx.Exec(ctx, `UPDATE nas_cleanup_day_plans SET mode='retired',approval_reason=$2,updated_at=now() WHERE id=$1`, id, req.Reason); err != nil || tx.Commit(ctx) != nil {
		util.WriteError(w, 500, "day cleanup close failed")
		return
	}
	util.WriteJSON(w, 200, map[string]any{"id": id, "plan_sha256": req.PlanSHA, "mode": "retired"})
}

type nasCleanupDayPermit struct {
	UnitID      int64     `json:"unit_id"`
	PlanID      int64     `json:"plan_id"`
	PlanSHA     string    `json:"plan_sha256"`
	Attempt     int       `json:"attempt"`
	Token       string    `json:"permit_token"`
	ExpiresAt   time.Time `json:"permit_expires_at"`
	RelativeDir string    `json:"relative_dir"`
	LocalDate   string    `json:"local_date"`
	StreamID    int64     `json:"stream_id"`
	Timezone    string    `json:"timezone"`
	Files       int64     `json:"files"`
	Bytes       int64     `json:"bytes"`
}

// cleanupDayConnection admits only nasCleanupDayVersions; older literals never
// receive a day permit.
func (s *Server) cleanupDayConnection(w http.ResponseWriter, r *http.Request, version string) (int64, bool) {
	if !slices.Contains(nasCleanupDayVersions, version) {
		util.WriteError(w, http.StatusConflict, "bulk NAS-only cleanup release required")
		return 0, false
	}
	return s.nasCleanupConnection(r.Context(), w, r, version)
}

func (s *Server) handleAccountNASCleanupDayPermit(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ClientVersion string `json:"client_version"`
	}
	if util.DecodeJSON(r, &req) != nil {
		util.WriteError(w, 400, "client version required")
		return
	}
	cid, ok := s.cleanupDayConnection(w, r, req.ClientVersion)
	if !ok {
		return
	}
	if !s.cfg.NASCleanupDeletionEnabled {
		util.WriteJSON(w, 200, map[string]any{"unit": nil, "reason": "deletion master off"})
		return
	}
	ctx := r.Context()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		util.WriteError(w, 500, "day cleanup permit unavailable")
		return
	}
	defer tx.Rollback(ctx)
	deadline, err := cleanupSessionDeadline(ctx, tx, cid, cleanupAdmissionSession(r, req.ClientVersion))
	if err != nil || !deadline.After(time.Now().Add(time.Minute)) {
		util.WriteError(w, 409, "fresh heartbeat from this exact client session required")
		return
	}
	if err = cleanupMaintenanceCheck(ctx, tx); err != nil {
		util.WriteError(w, 409, "cleanup maintenance gate unavailable or active")
		return
	}
	for range 25 {
		var unitID int64
		var permit nasCleanupDayPermit
		var until time.Time
		err = tx.QueryRow(ctx, `SELECT u.id,u.plan_id,p.plan_sha256,u.stream_id,u.relative_dir,u.local_date::text,u.files,u.bytes,p.approved_until
 FROM nas_cleanup_day_units u JOIN nas_cleanup_day_plans p ON p.id=u.plan_id
 WHERE p.mode='enabled' AND p.approved_until>clock_timestamp()+interval '1 minute' AND p.connection_id=$1
  AND (u.state='pending' OR (u.state='permitted' AND u.permit_expires_at<=clock_timestamp()))
 ORDER BY u.plan_id,u.ordinal LIMIT 1 FOR UPDATE OF u SKIP LOCKED FOR SHARE OF p`, cid).Scan(&unitID, &permit.PlanID, &permit.PlanSHA,
			&permit.StreamID, &permit.RelativeDir, &permit.LocalDate, &permit.Files, &permit.Bytes, &until)
		if errors.Is(err, pgx.ErrNoRows) {
			break
		}
		if err != nil {
			util.WriteError(w, 500, "day cleanup unit unavailable")
			return
		}
		unit := nasCleanupDayUnit{RelativeDir: permit.RelativeDir, LocalDate: permit.LocalDate, StreamID: permit.StreamID, Files: permit.Files, Bytes: permit.Bytes}
		if _, _, err = cleanupDayCheck(ctx, tx, unit); err != nil {
			if _, err = tx.Exec(ctx, `UPDATE nas_cleanup_day_units SET state='refused',refusal=$2,updated_at=now() WHERE id=$1`, unitID, err.Error()); err != nil {
				util.WriteError(w, 500, "day cleanup refusal failed")
				return
			}
			continue
		}
		secret := make([]byte, 32)
		if _, err = rand.Read(secret); err != nil {
			util.WriteError(w, 500, "day cleanup permit unavailable")
			return
		}
		permit.UnitID, permit.Token = unitID, hex.EncodeToString(secret)
		permit.Timezone = cleanupEmbeddedPolicyWindow(permit.StreamID).Timezone
		expires := time.Now().Add(nasCleanupDayPermitTTL)
		for _, bound := range []time.Time{until, deadline} {
			if bound.Before(expires) {
				expires = bound
			}
		}
		if err = tx.QueryRow(ctx, `UPDATE nas_cleanup_day_units SET state='permitted',attempt=attempt+1,permit_token_sha256=$2,
 permit_expires_at=$3,updated_at=now() WHERE id=$1 RETURNING attempt,permit_expires_at`, unitID, cleanupSHA([]byte(permit.Token)), expires).Scan(&permit.Attempt, &permit.ExpiresAt); err != nil {
			util.WriteError(w, 500, "day cleanup permit failed")
			return
		}
		if err = tx.Commit(ctx); err != nil {
			util.WriteError(w, 500, "day cleanup permit failed")
			return
		}
		util.WriteJSON(w, 200, map[string]any{"unit": permit})
		return
	}
	if err = tx.Commit(ctx); err != nil {
		util.WriteError(w, 500, "day cleanup refusal failed")
		return
	}
	util.WriteJSON(w, 200, map[string]any{"unit": nil})
}

func cleanupEmbeddedPolicyWindow(stream int64) cleanupKeepWindow {
	policy, _ := cleanupEmbeddedPolicy()
	return policy.Streams[stream]
}

type nasCleanupDayReceipt struct {
	Attempt        int                  `json:"attempt"`
	Outcome        nasCleanupDayOutcome `json:"outcome"`
	MediaFiles     int64                `json:"media_files"`
	MediaBytes     int64                `json:"media_bytes"`
	SidecarFiles   int64                `json:"sidecar_files"`
	SidecarBytes   int64                `json:"sidecar_bytes"`
	RemainingFiles int64                `json:"remaining_files"`
	Refusal        string               `json:"refusal"`
	ClientVersion  string               `json:"client_version"`
}

func (rc nasCleanupDayReceipt) valid() bool {
	counts := rc.MediaFiles >= 0 && rc.MediaBytes >= 0 && rc.SidecarFiles >= 0 && rc.SidecarBytes >= 0 && rc.RemainingFiles >= 0 &&
		rc.MediaFiles <= 1<<20 && rc.SidecarFiles <= 1<<20 && len(rc.Refusal) <= 500
	switch rc.Outcome {
	case nasCleanupDayDeleted:
		return counts && rc.RemainingFiles == 0 && rc.Refusal == ""
	case nasCleanupDayPartial:
		return counts && rc.RemainingFiles > 0
	case nasCleanupDayRefused:
		return counts && rc.MediaFiles == 0 && rc.SidecarFiles == 0 && rc.Refusal != ""
	default:
		return false
	}
}

// One receipt per directory permit; a retried identical receipt is a no-op.
func (s *Server) handleAccountNASCleanupDayReceipt(w http.ResponseWriter, r *http.Request) {
	var req struct {
		nasCleanupDayReceipt
		UnitID int64  `json:"unit_id"`
		Token  string `json:"permit_token"`
	}
	if util.DecodeJSON(r, &req) != nil || req.UnitID <= 0 || req.Attempt <= 0 || !stitchSHA(req.Token) || !req.valid() {
		util.WriteError(w, 400, "exact unit, attempt, permit token and a consistent receipt required")
		return
	}
	cid, ok := s.cleanupDayConnection(w, r, req.ClientVersion)
	if !ok {
		return
	}
	receipt, _ := json.Marshal(req.nasCleanupDayReceipt)
	ctx := r.Context()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		util.WriteError(w, 500, "day cleanup receipt unavailable")
		return
	}
	defer tx.Rollback(ctx)
	var state string
	var attempt int
	var tokenSHA *string
	var duplicate, conflict bool
	err = tx.QueryRow(ctx, `SELECT u.state,u.attempt,u.permit_token_sha256,
 EXISTS(SELECT 1 FROM jsonb_array_elements(u.receipts) e WHERE e=$3::jsonb),
 EXISTS(SELECT 1 FROM jsonb_array_elements(u.receipts) e WHERE (e->>'attempt')::int=$4)
 FROM nas_cleanup_day_units u JOIN nas_cleanup_day_plans p ON p.id=u.plan_id
 WHERE u.id=$1 AND p.connection_id=$2 FOR UPDATE OF u`, req.UnitID, cid, string(receipt), req.Attempt).Scan(&state, &attempt, &tokenSHA, &duplicate, &conflict)
	if err != nil || tokenSHA == nil || *tokenSHA != cleanupSHA([]byte(req.Token)) || attempt != req.Attempt {
		util.WriteError(w, 409, "receipt does not match the unit's permit")
		return
	}
	if duplicate {
		util.WriteJSON(w, 200, map[string]any{"unit_id": req.UnitID, "state": state})
		return
	}
	if conflict || (state != "permitted" && state != "retired") {
		util.WriteError(w, 409, "contradictory day cleanup receipt")
		return
	}
	next := map[nasCleanupDayOutcome]string{nasCleanupDayDeleted: "deleted", nasCleanupDayRefused: "client_refused", nasCleanupDayPartial: "pending"}[req.Outcome]
	if next == "pending" && state == "retired" {
		next = "retired"
	}
	if _, err = tx.Exec(ctx, `UPDATE nas_cleanup_day_units SET state=$2,receipts=receipts||jsonb_build_array($3::jsonb),
 refusal=CASE WHEN $2='client_refused' THEN $4 ELSE refusal END,permit_expires_at=NULL,updated_at=now() WHERE id=$1`,
		req.UnitID, next, string(receipt), req.Refusal); err != nil || tx.Commit(ctx) != nil {
		util.WriteError(w, 500, "day cleanup receipt failed")
		return
	}
	util.WriteJSON(w, 200, map[string]any{"unit_id": req.UnitID, "state": next})
}
