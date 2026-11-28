package api

import (
	"context"
	_ "embed"
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

	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/daydemir/stoarama/backend/internal/util"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"golang.org/x/sync/errgroup"
)

// Day-directory NAS cleanup is currently verification-only. Deniz's Nov 27
// rule requires a proven second copy, and non-Keep-Everything day cleanup is
// finished/disabled. For Keep Everything, only the same raw bytes qualify:
// exact NAS size and SHA256, catalogue identity, and a live matching R2 object.
// A stitched output cannot stand in for the raw file. Keep20, boundary days,
// folder/stream checks, incident and delivery holds are still enforced.
// Existing receipts remain reconcilable, but enable and permit are blocked
// until every R2 remover/replacer shares the durable survivor reservation.

//go:embed nas_cleanup_folder_streams.json
var cleanupFolderStreamsJSON []byte

// Same bytes and digest as the NAS client's CLEANUP_FOLDER_STREAMS_SHA256.
const cleanupFolderStreamsSHA = "94efc6fe9f0b83db55be31d4c2fad7f47cff781fd01d88bc33c4c90cae0e7ab8"

const (
	nasCleanupDayMaxUnits  = 1000
	nasCleanupDayMaxNames  = 100000
	nasCleanupDayDigestTag = "stoarama-nas-cleanup-day-v1"
	// nasCleanupDayHeads bounds the live R2 HEADs of one KE unit check.
	nasCleanupDayHeads            = 32
	nasCleanupDayHeadTimeout      = 2 * time.Minute
	nasCleanupDayIntegrationBlock = "day cleanup blocked: no shared persistent survivor reservation at the R2 wire boundary"
	nasCleanupDayNonKEBlock       = "non-Keep-Everything NAS-only day cleanup is disabled"
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
	// Names lists a Keep Everything unit's media files, ascending by name; any
	// other unit lists none.
	Names []nasCleanupDayName `json:"names,omitempty"`
}

// nasCleanupDayName is one media file of a Keep Everything unit, chosen by the
// preparer because R2 lists its counterpart with the catalogued size.
type nasCleanupDayName struct {
	Name      string `json:"name"`
	SizeBytes int64  `json:"size_bytes"`
}

// cleanupDayDigest binds the ordered units to the policy. The preparer and
// runner compute the same line format (paths never contain a newline). A KE
// unit's names follow its unit line; other units keep their earlier digest.
func cleanupDayDigest(units []nasCleanupDayUnit) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s\npolicy %s\n", nasCleanupDayDigestTag, cleanupPolicySHA)
	for _, u := range units {
		fmt.Fprintf(&b, "unit %d %s %d %d %s\n", u.StreamID, u.LocalDate, u.Files, u.Bytes, u.RelativeDir)
		for _, n := range u.Names {
			fmt.Fprintf(&b, "name %d %s\n", n.SizeBytes, n.Name)
		}
	}
	return cleanupSHA([]byte(b.String()))
}

// cleanupDayNamesValid: a KE unit's names are distinct media of the unit's date
// in ascending order, totalling the unit.
func cleanupDayNamesValid(u nasCleanupDayUnit, day time.Time, loc *time.Location) error {
	var bytes int64
	for i, n := range u.Names {
		if named, ok := cleanupDayFileDate(u.RelativeDir, n.Name, loc); !ok || !named.Equal(day) || n.SizeBytes <= 0 ||
			(i > 0 && n.Name <= u.Names[i-1].Name) {
			return errors.New("Keep Everything names must be ascending media of the unit's date with sizes")
		}
		bytes += n.SizeBytes
	}
	if int64(len(u.Names)) != u.Files || bytes != u.Bytes {
		return errors.New("Keep Everything names differ from the unit totals")
	}
	return nil
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
	if !ok {
		return cleanupKeepWindow{}, time.Time{}, nil, errors.New("stream has no pinned window")
	}
	if w.KeepEverything != (len(u.Names) > 0) {
		return cleanupKeepWindow{}, time.Time{}, nil, errors.New("a Keep Everything unit names its exact media; any other unit names none")
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
	if err == nil && w.KeepEverything {
		err = cleanupDayNamesValid(u, day, loc)
	}
	return w, day, loc, err
}

type cleanupDayQuerier interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

// Every catalogued clip directly in the directory ($1; $2 its LIKE pattern),
// of any stream and at any time: Go refuses another stream's clip and checks exactly those whose name
// names the unit's date, whatever their catalogued times. The held check runs
// afterwards on those clips only: a flat recordings/<id>/continuous directory
// holds every day of the recording, and evaluating it per directory clip made
// a 13-unit enable take ~130 s.
const cleanupDayUnitSQL = `SELECT c.id,c.display_path,c.clip_start_at,c.clip_end_at,r.stream_id,
 coalesce(f.size_bytes,-1),f.sidecar_relative_path,f.sidecar_size_bytes,c.size_bytes
 FROM recordings r JOIN recording_clips c ON c.recording_id=r.id
 LEFT JOIN nas_inventory_files f ON f.connection_id=13 AND f.clip_id=c.id AND f.state='present' AND f.relative_path=c.display_path
 WHERE r.account_id=47 AND c.display_path LIKE $2 AND strpos(substr(c.display_path,length($1)+2),'/')=0
 ORDER BY c.display_path`

// cleanupDayDirPattern is the left-anchored LIKE pattern for the directory's
// entries, sargable on recording_clips_display_path_pattern (0248).
var cleanupDayDirPattern = strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)

// nasCleanupDayMode is how a permit names what the NAS client may unlink.
type nasCleanupDayMode string

const (
	// nasCleanupDayNames: a flat recordings/<id>/{continuous,<job>} directory
	// holds every day of its recording, so the permit lists the exact inventory
	// entries (name and size) and the client unlinks only those.
	nasCleanupDayNames nasCleanupDayMode = "names"
	// nasCleanupDayDir: a plaza <folder>/<Month>/<DD-Weekday> directory holds
	// one day of one stream, so the permit covers every media file and sidecar
	// of that date in it, catalogued or not ("the nas keeps all keep 20
	// footage"; this day is outside it).
	nasCleanupDayDir nasCleanupDayMode = "dir"
)

// cleanupDayModeOf: a Keep Everything unit (the only kind with names) always
// goes in names mode, so a plaza directory's unnamed files stay.
func cleanupDayModeOf(u nasCleanupDayUnit) nasCleanupDayMode {
	if len(u.Names) == 0 && cleanupDayPlazaDir.MatchString(u.RelativeDir) {
		return nasCleanupDayDir
	}
	return nasCleanupDayNames
}

// Only the clip's own raw object is a survivor. A stitched output does not
// preserve the same bytes. A pending retirement intent is already unsafe.
const cleanupDayR2LiveSQL = `WITH c AS (SELECT id,bucket,object_key,size_bytes,etag,sha256,display_path,purged_at FROM recording_clips WHERE id=ANY($1::bigint[]) FOR SHARE)
 SELECT c.id,c.object_key,c.size_bytes,trim(both '"' from c.etag),lower(c.sha256) FROM c
 JOIN nas_inventory_files f ON f.connection_id=13 AND f.clip_id=c.id AND f.state='present' AND f.relative_path=c.display_path
 WHERE c.bucket=$2 AND c.purged_at IS NULL AND c.object_key<>'' AND c.etag<>'' AND f.size_bytes=c.size_bytes
  AND lower(c.sha256) ~ '^[0-9a-f]{64}$' AND lower(f.sha256)=lower(c.sha256)
  AND NOT EXISTS(SELECT 1 FROM r2_object_retirements x WHERE (x.bucket=c.bucket AND x.object_key=c.object_key) OR (x.kind='raw' AND x.source_id=c.id))
  AND NOT EXISTS(SELECT 1 FROM r2_retirement_reservations x WHERE (x.bucket=c.bucket AND x.object_key=c.object_key) OR (x.kind='raw' AND x.source_id=c.id))
  AND NOT EXISTS(SELECT 1 FROM r2_inverse_intents x WHERE (x.bucket=c.bucket AND x.object_key=c.object_key) OR x.source_id=c.id)
  AND NOT EXISTS(SELECT 1 FROM r2_keep28_intents x WHERE (x.bucket=c.bucket AND x.object_key=c.object_key) OR x.source_id=c.id)`

type cleanupDayCounterpart struct {
	key, etag, sha string
	size           int64
}

// cleanupDayR2Verify HEADs every counterpart with bounded concurrency and
// returns why each one is not live ("" when R2 holds it at its catalogued
// size). Any R2 error other than absence fails the whole check.
func cleanupDayR2Verify(ctx context.Context, store *r2.Client, counterparts []cleanupDayCounterpart) (map[cleanupDayCounterpart]string, error) {
	ctx, cancel := context.WithTimeout(ctx, nasCleanupDayHeadTimeout)
	defer cancel()
	group, ctx := errgroup.WithContext(ctx)
	group.SetLimit(nasCleanupDayHeads)
	reasons := make([]string, len(counterparts))
	for i, c := range counterparts {
		group.Go(func() error {
			head, err := store.Head(ctx, c.key)
			switch {
			case r2.IsNotFound(err):
				reasons[i] = fmt.Sprintf("%s is missing", c.key)
			case err != nil:
				return fmt.Errorf("R2 counterpart %s unverified: %w", c.key, err)
			case head.SizeBytes != c.size:
				reasons[i] = fmt.Sprintf("%s is %d bytes, catalogued %d", c.key, head.SizeBytes, c.size)
			case head.ETag != c.etag || c.etag == "":
				reasons[i] = fmt.Sprintf("%s R2 generation differs", c.key)
			case head.SHA256Metadata != "" && !strings.EqualFold(head.SHA256Metadata, c.sha):
				reasons[i] = fmt.Sprintf("%s R2 sha256 metadata differs", c.key)
			}
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}
	out := make(map[cleanupDayCounterpart]string, len(counterparts))
	for i, c := range counterparts {
		out[c] = reasons[i]
	}
	return out, nil
}

// nasCleanupDayEntry is one inventoried file of the unit: a media file or the
// sidecar beside it, by its name in the directory and exact size.
type nasCleanupDayEntry struct {
	Name      string `json:"name"`
	SizeBytes int64  `json:"size_bytes"`
	Sidecar   bool   `json:"sidecar"`
}

// cleanupDayMedia totals the media (not sidecar) entries.
func cleanupDayMedia(entries []nasCleanupDayEntry) (files, bytes int64) {
	for _, e := range entries {
		if !e.Sidecar {
			files++
			bytes += e.SizeBytes
		}
	}
	return files, bytes
}

// cleanupDayCheck returns the unit's inventoried NAS media and sidecars, or
// why the whole directory-day must stay. A Keep Everything unit returns only
// its named files whose NAS size is the catalogued size and whose R2
// counterpart is live in the catalogue; its other files stay on the NAS. Each
// returned file then needs one of those counterparts verified in R2 itself.
func cleanupDayCheck(ctx context.Context, q cleanupDayQuerier, u nasCleanupDayUnit, store *r2.Client) ([]nasCleanupDayEntry, error) {
	w, day, loc, err := cleanupDayStatic(u)
	if err != nil {
		return nil, err
	}
	var held bool
	if err := q.QueryRow(ctx, `SELECT NOT EXISTS(SELECT 1 FROM nas_media_keep20_epoch WHERE connection_id=13) OR nas_cleanup_day_dir_held($1,$2)`, u.RelativeDir, w.KeepEverything).Scan(&held); err != nil {
		return nil, err
	}
	if held {
		return nil, errors.New("keep20 epoch inactive or delivery path hold in directory")
	}
	rows, err := q.Query(ctx, cleanupDayUnitSQL, u.RelativeDir, cleanupDayDirPattern.Replace(u.RelativeDir)+"/%")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	type present struct {
		id         int64
		media      nasCleanupDayEntry
		catalogued int64
		sidecar    *nasCleanupDayEntry
	}
	var files []present
	var dated []int64
	for rows.Next() {
		var id int64
		var p string
		var a, b *time.Time
		var stream *int64
		var size, catalogued int64
		var sidecarPath *string
		var sidecarSize *int64
		if err := rows.Scan(&id, &p, &a, &b, &stream, &size, &sidecarPath, &sidecarSize, &catalogued); err != nil {
			return nil, err
		}
		if stream == nil || *stream != u.StreamID {
			return nil, errors.New("catalogued clip in the directory belongs to another stream")
		}
		named, ok := cleanupDayFileDate(u.RelativeDir, path.Base(p), loc)
		if !ok {
			return nil, errors.New("catalogued clip name does not name a local date")
		}
		if !named.Equal(day) {
			continue
		}
		if a == nil || b == nil || !b.After(*a) || a.Before(w.end) && b.After(w.start) {
			return nil, errors.New("catalogued clip intersects the pinned keep window")
		}
		dated = append(dated, id)
		if size >= 0 {
			f := present{id: id, media: nasCleanupDayEntry{Name: path.Base(p), SizeBytes: size}, catalogued: catalogued}
			if sidecarPath != nil && sidecarSize != nil && *sidecarPath == p+".stoarama.json" {
				f.sidecar = &nasCleanupDayEntry{Name: path.Base(*sidecarPath), SizeBytes: *sidecarSize, Sidecar: true}
			}
			files = append(files, f)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(files) == 0 {
		return nil, errors.New("no catalogued NAS media in the directory-day")
	}
	if err := q.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM unnest($1::bigint[]) id WHERE nas_cleanup_day_clip_held(id,$2))`, dated, w.KeepEverything).Scan(&held); err != nil {
		return nil, err
	}
	if held {
		return nil, errors.New("catalogued clip is held")
	}
	keep := func(present) bool { return true }
	live := map[int64][]cleanupDayCounterpart{}
	if w.KeepEverything {
		if store == nil {
			return nil, errors.New("R2 unavailable for the Keep Everything counterpart check")
		}
		rows, err := q.Query(ctx, cleanupDayR2LiveSQL, dated, store.Bucket())
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var id int64
			var c cleanupDayCounterpart
			if err := rows.Scan(&id, &c.key, &c.size, &c.etag, &c.sha); err != nil {
				rows.Close()
				return nil, err
			}
			live[id] = append(live[id], c)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, err
		}
		named := map[string]int64{}
		for _, n := range u.Names {
			named[n.Name] = n.SizeBytes
		}
		keep = func(f present) bool {
			size, ok := named[f.media.Name]
			return ok && size == f.media.SizeBytes && f.catalogued == size && len(live[f.id]) == 1
		}
	}
	var entries []nasCleanupDayEntry
	var kept []present
	seen := map[cleanupDayCounterpart]bool{}
	var counterparts []cleanupDayCounterpart
	for _, f := range files {
		if keep(f) {
			kept = append(kept, f)
			entries = append(entries, f.media)
			if f.sidecar != nil {
				entries = append(entries, *f.sidecar)
			}
			for _, c := range live[f.id] {
				if !seen[c] {
					seen[c] = true
					counterparts = append(counterparts, c)
				}
			}
		}
	}
	if len(entries) == 0 {
		return nil, errors.New("no named NAS media with a live R2 counterpart in the directory-day")
	}
	if !w.KeepEverything {
		return entries, nil
	}
	// Each permitted file needs at least one counterpart R2 holds at its size.
	reasons, err := cleanupDayR2Verify(ctx, store, counterparts)
	if err != nil {
		return nil, err
	}
	for _, f := range kept {
		var why []string
		for _, c := range live[f.id] {
			why = append(why, reasons[c])
		}
		if !slices.Contains(why, "") {
			return nil, fmt.Errorf("no R2 counterpart of %s is live: %s", f.media.Name, strings.Join(why, "; "))
		}
	}
	return entries, nil
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
	var files, bytes, names int64
	for _, u := range req.Units {
		key := [2]string{u.RelativeDir, u.LocalDate}
		if seen[key] || u.Files <= 0 || u.Bytes <= 0 {
			util.WriteError(w, 400, "duplicate or empty unit")
			return
		}
		seen[key] = true
		files += u.Files
		bytes += u.Bytes
		names += int64(len(u.Names))
	}
	if names > nasCleanupDayMaxNames {
		util.WriteError(w, 400, "at most 100000 Keep Everything names per plan")
		return
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
		entries, err := cleanupDayCheck(ctx, tx, u, s.r2)
		if gotFiles, gotBytes := cleanupDayMedia(entries); err == nil && (gotFiles != u.Files || gotBytes != u.Bytes) {
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
	if _, err = tx.Exec(ctx, `INSERT INTO nas_cleanup_day_units(plan_id,ordinal,stream_id,relative_dir,local_date,files,bytes,names)
 SELECT $1,n,(u->>'stream_id')::bigint,u->>'relative_dir',(u->>'local_date')::date,(u->>'files')::bigint,(u->>'bytes')::bigint,u->'names'
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
   'relative_dir',u.relative_dir,'local_date',u.local_date::text,'files',u.files,'bytes',u.bytes,'names',u.names,'dropped_names',u.dropped_names,'state',u.state,
   'attempt',u.attempt,'permit_live',u.state='permitted' AND u.permit_expires_at>clock_timestamp(),
   'refusal',u.refusal,'receipts',u.receipts) ORDER BY u.ordinal),'[]') FROM nas_cleanup_day_units u WHERE u.plan_id=p.id))
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
	var plan struct {
		Units []nasCleanupDayStatusUnit `json:"units"`
	}
	if json.Unmarshal(body, &out) != nil || json.Unmarshal(body, &plan) != nil {
		util.WriteError(w, 500, "day cleanup plan unavailable")
		return
	}
	out["deleted_media_files"], out["deleted_media_bytes"] = cleanupDayDeletedMedia(plan.Units)
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
		err = cleanupDayEnable(ctx, tx, id, mode)
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

// Day execution is disabled. Do not hold database locks across R2 network
// checks for an enable request that cannot issue authority; verification
// remains on the dry-run admission path.
func cleanupDayEnable(ctx context.Context, tx pgx.Tx, id int64, mode string) error {
	if mode != "dry_run" && mode != "paused" {
		return errors.New("only a dry-run or paused day cleanup plan can enable")
	}
	rows, err := tx.Query(ctx, `SELECT DISTINCT stream_id FROM nas_cleanup_day_units WHERE plan_id=$1 AND state='pending'`, id)
	if err != nil {
		return err
	}
	defer rows.Close()
	seen := false
	for rows.Next() {
		var stream int64
		if err := rows.Scan(&stream); err != nil {
			return err
		}
		seen = true
		if !cleanupEmbeddedPolicyWindow(stream).KeepEverything {
			return errors.New(nasCleanupDayNonKEBlock)
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if !seen {
		return errors.New("no pending unit still qualifies")
	}
	return errors.New(nasCleanupDayIntegrationBlock)
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
	// Mode and Entries go only to nasCleanupDayModeVersions; Entries only in
	// names mode.
	Mode    nasCleanupDayMode    `json:"mode,omitempty"`
	Entries []nasCleanupDayEntry `json:"entries,omitempty"`
	// DroppedNames counts a Keep Everything unit's named files this permit
	// leaves on the NAS because their R2 counterpart is no longer live.
	DroppedNames int64 `json:"dropped_names"`
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
	// Do not issue or re-issue a permit, including for a plan enabled by an
	// older binary. Existing permit receipts can still reconcile via the
	// receipt endpoint. A disabled lane with no pending work stays idle.
	var stream int64
	err = tx.QueryRow(ctx, `SELECT u.stream_id
 FROM nas_cleanup_day_units u JOIN nas_cleanup_day_plans p ON p.id=u.plan_id
 WHERE p.mode='enabled' AND p.approved_until>clock_timestamp()+interval '1 minute' AND p.connection_id=$1
  AND (u.state='pending' OR (u.state='permitted' AND u.permit_expires_at<=clock_timestamp())) AND (u.names IS NULL OR $2)
 ORDER BY u.plan_id,u.ordinal LIMIT 1`, cid, slices.Contains(nasCleanupDayKEVersions, req.ClientVersion)).Scan(&stream)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteJSON(w, 200, map[string]any{"unit": nil})
		return
	}
	if err != nil {
		util.WriteError(w, 500, "day cleanup unit unavailable")
		return
	}
	if !cleanupEmbeddedPolicyWindow(stream).KeepEverything {
		util.WriteError(w, 409, nasCleanupDayNonKEBlock)
		return
	}
	util.WriteError(w, 409, nasCleanupDayIntegrationBlock)

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

// nasCleanupDayStatusUnit is a unit as the status read returns it.
type nasCleanupDayStatusUnit struct {
	nasCleanupDayUnit
	Receipts []nasCleanupDayReceipt `json:"receipts"`
}

// cleanupDayDeletedMedia totals the media unlinked under a plan's permits. A dir
// permit's receipt counts what that attempt found and unlinked, so a unit's
// receipts add up. A names permit's receipt counts every listed name now absent,
// including those an earlier attempt unlinked, so only the unit's last
// non-refused receipt counts: a refusal unlinks nothing and reports zero.
// Receipts are appended in attempt order.
func cleanupDayDeletedMedia(units []nasCleanupDayStatusUnit) (files, bytes int64) {
	for _, u := range units {
		receipts := u.Receipts
		if cleanupDayModeOf(u.nasCleanupDayUnit) == nasCleanupDayNames {
			receipts = nil
			for i := len(u.Receipts) - 1; i >= 0; i-- {
				if u.Receipts[i].Outcome != nasCleanupDayRefused {
					receipts = u.Receipts[i : i+1]
					break
				}
			}
		}
		for _, rc := range receipts {
			files += rc.MediaFiles
			bytes += rc.MediaBytes
		}
	}
	return files, bytes
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
