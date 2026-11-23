package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// class is the one decision recorded for a key. Only delete classes ever
// enter a delete manifest.
type class string

const (
	deleteRawOutsideWindow      class = "delete_raw_outside_window"
	deleteCollatedOutsideWindow class = "delete_collated_outside_window"
	deleteWindowOnNAS           class = "delete_window_20d_on_nas"
	deleteDuplicateOfPageLink   class = "delete_duplicate_of_page_link"
	deleteKeRawStitched         class = "delete_ke_raw_stitched"

	protectKeepEverything class = "protect_keep_everything"
	protectLiveSite       class = "protect_live_site"
	protectWindowOnNAS    class = "protect_window_20d_on_nas"
	protectWindowNotOnNAS class = "protect_window_20d_not_on_nas"
	protectWindowStale    class = "protect_window_20d_nas_evidence_stale"
	protectWindowUnjoined class = "protect_window_20d_not_yet_stitched"
	protectStitchClosure  class = "protect_window_stitch_closure"
	protectNASPipeline    class = "protect_nas_pipeline"
	protectRecentWrite    class = "protect_recent_write"
	protectUnclassified   class = "protect_unclassified"
)

func (c class) deletes() bool {
	switch c {
	case deleteRawOutsideWindow, deleteCollatedOutsideWindow, deleteWindowOnNAS, deleteDuplicateOfPageLink, deleteKeRawStitched:
		return true
	}
	return false
}

type kind string

const (
	kindRaw      kind = "raw"
	kindCollated kind = "col"
	// kindDuplicate names a key whose bytes equal a live page key's (same
	// SHA-256 and size); Stream is the page row's, for the keep-everything check.
	kindDuplicate kind = "dup"
)

// releasable are the controller protections a later pass may lift, each only
// by name. nas_keep_20 is the controller's whole-window hold; lifting it
// requires live NAS evidence, so a window key goes only on fresh exact custody.
var releasable = map[string]bool{"nas_keep_20": true, "unresolved_best14_delivery_identity": true,
	"delivery_repairs_and_unresolved_stitch_provenance": true}

// evidence is the live NAS custody of one key: exact SHA-256/size match, when
// it was verified and when the NAS file last changed (0 when not reported).
type evidence struct {
	Match              bool
	Verified, NASMTime int64
}

// identity is one catalog row naming a key: a RAW clip or a collated output.
type identity struct {
	Kind       kind
	Stream     int64
	Start, End int64 // unix seconds; 0 when the catalog has no interval
	Size       int64
	OnNAS      bool // exact path, size and SHA-256 match on NAS connection 13
	Folder     string
}

type window struct{ Start, End int64 } // [Start, End) unix seconds

// keManifestPrefix holds collation manifests, which this class never touches.
const keManifestPrefix = "joined/nightly-collation-v2/manifests"

// keCoverage is one owner row saying a raw key's bytes are inside a stitched
// output already stored in R2.
type keCoverage struct {
	OutputKey           string
	RawSize, OutputSize int64
}

type prefixRule struct {
	Prefix string
	Class  class
	Reason string
}

type keyRule struct {
	Class  class
	Reason string
}

// rules is every protection input. plan and delete load it from the same
// files and run the same decide, so delete re-proves each key before the wire.
type rules struct {
	Bucket     string
	Windows    map[int64]window
	KE         map[int64]bool
	KEFolders  map[string]bool
	Keys       map[string]keyRule
	Prefixes   []prefixRule
	Closure    map[string]bool
	Identities map[string][]identity
	RecentFrom int64 // objects modified at or after this instant are protected
	Released   map[string]bool
	Evidence   map[string]evidence // nil unless nas_keep_20 is released
	// KECoverage is the owner's --ke-coverage rows per raw key, and OutputKeys
	// every stitched output they name. Outputs is the size each of those
	// outputs has in the listing being decided: the plan snapshot, or delete's
	// fresh listing.
	KECoverage map[string][]keCoverage
	OutputKeys map[string]bool
	Outputs    map[string]int64
	Fresh      map[string]object // delete only: the listing taken right before the run
}

type ruleFiles struct {
	identities, dbProtect, closure, controller, picks string
	derived, nasEvidence, release, recentFrom         string
	keCoverage                                        string
}

func (f *ruleFiles) register(fs *flag.FlagSet) {
	fs.StringVar(&f.identities, "identities", "", "catalog identities TSV (sql/identities.sql)")
	fs.StringVar(&f.dbProtect, "db-protect", "", "DB protect sets TSV (sql/protect.sql)")
	fs.StringVar(&f.closure, "closure", "", "20-day stitch input closure TSV (sql/closure.sql)")
	fs.StringVar(&f.controller, "controller-protect", "", "controller protect-for-nas-20day.jsonl")
	fs.StringVar(&f.picks, "picks", "", "owner r2-picks.json")
	fs.StringVar(&f.recentFrom, "recent-from", "", "RFC3339: objects modified at or after this stay")
	fs.StringVar(&f.derived, "derived-identities", "", "optional identities derived for keys with no catalog row")
	fs.StringVar(&f.nasEvidence, "nas-evidence", "", "live NAS custody TSV for window keys; required with --release nas_keep_20")
	fs.StringVar(&f.release, "release", "", "comma-separated controller protections to lift: "+strings.Join(sortedKeys(releasable), ","))
	fs.StringVar(&f.keCoverage, "ke-coverage", "", "optional TSV: raw_key,raw_size,output_key,output_size for KE raw already inside a stitched R2 output")
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// digests are the SHA-256 of every rule file, keyed as summary.json records them.
func (f ruleFiles) digests() (map[string]string, error) {
	out := map[string]string{}
	for name, p := range map[string]string{"identities": f.identities, "db_protect": f.dbProtect, "closure": f.closure,
		"controller_protect": f.controller, "picks": f.picks, "derived_identities": f.derived, "nas_evidence": f.nasEvidence, "ke_coverage": f.keCoverage} {
		if p == "" {
			continue
		}
		sum, err := fileSHA256(p)
		if err != nil {
			return nil, err
		}
		out[name] = sum
	}
	return out, nil
}

func (f ruleFiles) load() (*rules, error) {
	for name, v := range map[string]string{"identities": f.identities, "db-protect": f.dbProtect, "closure": f.closure, "controller-protect": f.controller, "picks": f.picks, "recent-from": f.recentFrom} {
		if v == "" {
			return nil, fmt.Errorf("--%s is required", name)
		}
	}
	recent, err := time.Parse(time.RFC3339, f.recentFrom)
	if err != nil {
		return nil, fmt.Errorf("--recent-from: %w", err)
	}
	r := &rules{Windows: map[int64]window{}, KE: map[int64]bool{}, KEFolders: map[string]bool{}, Keys: map[string]keyRule{},
		Closure: map[string]bool{}, Identities: map[string][]identity{}, RecentFrom: recent.Unix(), Released: map[string]bool{}}
	for _, name := range strings.Split(f.release, ",") {
		if name == "" {
			continue
		}
		if !releasable[name] {
			return nil, fmt.Errorf("--release %q is not a releasable controller protection", name)
		}
		r.Released[name] = true
	}
	if r.Released["nas_keep_20"] != (f.nasEvidence != "") {
		return nil, errors.New("--release nas_keep_20 and --nas-evidence go together")
	}
	if f.nasEvidence != "" {
		r.Evidence = map[string]evidence{}
		if err := r.loadEvidence(f.nasEvidence); err != nil {
			return nil, err
		}
	}
	if err := r.loadController(f.controller); err != nil {
		return nil, err
	}
	if err := r.loadPicks(f.picks); err != nil {
		return nil, err
	}
	if err := r.loadDBProtect(f.dbProtect); err != nil {
		return nil, err
	}
	if err := eachTSV(f.closure, 2, func(c []string) error { r.Closure[c[1]] = true; return nil }); err != nil {
		return nil, err
	}
	if err := r.loadIdentities(f.identities, f.derived); err != nil {
		return nil, err
	}
	if f.keCoverage != "" {
		if err := r.loadKECoverage(f.keCoverage); err != nil {
			return nil, err
		}
	}
	if len(r.Windows) == 0 || len(r.KE) == 0 || len(r.Identities) == 0 {
		return nil, errors.New("rules are empty: windows, keep-everything streams and identities are all required")
	}
	return r, nil
}

// controllerReason maps the controller's literal key and prefix reasons to a
// class. An unknown reason is an error, never a silent default.
func controllerClass(reason string) (class, error) {
	switch reason {
	case "keep_everything_stream", "owner_keep_everything":
		return protectKeepEverything, nil
	case "live_delivery_page", "live_delivery_registry_assets_and_tickets", "public_route_objects":
		return protectLiveSite, nil
	case "live_stitch_input", "outstanding_nas_copy":
		return protectNASPipeline, nil
	case "delivery_repairs_and_unresolved_stitch_provenance", "stitch_tools", "unresolved_best14_delivery_identity",
		"stitch_staging_and_provenance", "active_or_unresolved_stitch_stage":
		return protectUnclassified, nil
	}
	return "", fmt.Errorf("unknown controller reason %q", reason)
}

func (r *rules) loadController(path string) error {
	meta := false
	return eachLine(path, func(line string) error {
		var row struct {
			Type     string   `json:"type"`
			Bucket   string   `json:"bucket"`
			StreamID int64    `json:"stream_id"`
			Start    string   `json:"start_utc"`
			End      string   `json:"end_utc_exclusive"`
			Key      string   `json:"key"`
			Prefix   string   `json:"prefix"`
			Reasons  []string `json:"reasons"`
			KEIDs    []int64  `json:"keep_everything_stream_ids"`
			Unmapped string   `json:"unmapped"`
		}
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			return err
		}
		switch row.Type {
		case "meta":
			if row.Unmapped != "protect" || row.Bucket == "" {
				return errors.New("controller meta must name a bucket and protect unmapped keys")
			}
			meta, r.Bucket = true, row.Bucket
			for _, id := range row.KEIDs {
				r.KE[id] = true
			}
		case "keep_everything_stream":
			r.KE[row.StreamID] = true
		case "stream_window":
			s, err := time.Parse(time.RFC3339, row.Start)
			if err != nil {
				return err
			}
			e, err := time.Parse(time.RFC3339, row.End)
			if err != nil || !e.After(s) {
				return fmt.Errorf("stream %d: invalid window", row.StreamID)
			}
			r.Windows[row.StreamID] = window{s.Unix(), e.Unix()}
		case "key", "prefix":
			if row.Bucket != r.Bucket || !meta || len(row.Reasons) == 0 {
				return errors.New("controller key/prefix row before meta, without reasons or for another bucket")
			}
			c, err := controllerClass(row.Reasons[0])
			if err != nil {
				return err
			}
			if len(row.Reasons) == 1 && r.Released[row.Reasons[0]] {
				return nil
			}
			if row.Type == "key" {
				r.Keys[row.Key] = keyRule{c, "controller:" + row.Reasons[0]}
			} else {
				r.Prefixes = append(r.Prefixes, prefixRule{row.Prefix, c, "controller:" + row.Reasons[0]})
			}
		default:
			return fmt.Errorf("unknown controller row type %q", row.Type)
		}
		return nil
	})
}

func (r *rules) loadPicks(path string) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var doc struct {
		Choices map[string]struct {
			Choice string `json:"choice"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		return err
	}
	for id, p := range doc.Choices {
		n, err := strconv.ParseInt(id, 10, 64)
		if err != nil {
			return err
		}
		switch p.Choice {
		case "all":
			r.KE[n] = true
		case "none":
		default:
			return fmt.Errorf("stream %d: choice %q is not none or all", n, p.Choice)
		}
	}
	return nil
}

func (r *rules) loadDBProtect(path string) error {
	return eachTSV(path, 2, func(c []string) error {
		switch c[0] {
		case "page_link":
			r.Keys[c[1]] = keyRule{protectLiveSite, "db:" + c[0]}
		case "stitch_input_hold", "nas_copy_pending", "nas_restore_pending", "nas_incident":
			r.Keys[c[1]] = keyRule{protectNASPipeline, "db:" + c[0]}
		case "owner_keep_prefix":
			r.Prefixes = append(r.Prefixes, prefixRule{c[1], protectKeepEverything, "db:" + c[0]})
		case "nas_cleanup_hold", "page_exempt":
			// NAS-side cleanup holds and owner page exemptions never widen what
			// leaves R2: outside the window the owner's rule deletes, inside it
			// the window already protects.
		default:
			return fmt.Errorf("unknown db protect set %q", c[0])
		}
		return nil
	})
}

func (r *rules) loadEvidence(path string) error {
	return eachTSV(path, 6, func(c []string) error {
		var e evidence
		switch c[2] {
		case "t":
			e.Match = true
		case "f":
		default:
			return fmt.Errorf("bad evidence flag %q", c[2])
		}
		var err error
		if e.Verified, err = strconv.ParseInt(c[3], 10, 64); err != nil {
			return err
		}
		if e.NASMTime, err = strconv.ParseInt(c[4], 10, 64); err != nil {
			return err
		}
		if prior, seen := r.Evidence[c[1]]; seen { // every row for a key must hold
			e.Match = e.Match && prior.Match
			e.Verified, e.NASMTime = min(e.Verified, prior.Verified), max(e.NASMTime, prior.NASMTime)
		}
		r.Evidence[c[1]] = e
		return nil
	})
}

func (r *rules) loadIdentities(paths ...string) error {
	for _, path := range paths {
		if path == "" {
			continue
		}
		if err := r.loadIdentityFile(path); err != nil {
			return err
		}
	}
	for _, ids := range r.Identities {
		for _, id := range ids {
			if r.KE[id.Stream] && id.Folder != "" && id.Folder != genericFolder {
				r.KEFolders[id.Folder] = true
			}
		}
	}
	return nil
}

func (r *rules) loadIdentityFile(path string) error {
	return eachTSV(path, 10, func(c []string) error {
		var id identity
		switch kind(c[0]) {
		case kindRaw, kindCollated, kindDuplicate:
			id.Kind = kind(c[0])
		default:
			return fmt.Errorf("unknown identity kind %q", c[0])
		}
		var err error
		ints := []*int64{&id.Stream, &id.Start, &id.End, &id.Size}
		for i, p := range ints {
			if *p, err = strconv.ParseInt(c[2+i], 10, 64); err != nil {
				return err
			}
		}
		switch c[6] {
		case "t":
			id.OnNAS = true
		case "f":
		default:
			return fmt.Errorf("bad NAS flag %q", c[6])
		}
		id.Folder = c[8]
		if id.Kind == kindRaw {
			id.Folder = plazaFolder(c[1])
		} else if id.Folder == genericFolder {
			id.Folder = "" // shared by every stoarama_v1 recording; the stream decides
		}
		r.Identities[c[1]] = append(r.Identities[c[1]], id)
		return nil
	})
}

// keCoveredDelete reports the covering output when a Keep Everything key may
// instead go: --ke-coverage was given, every identity is a raw clip, the
// listed size matches a coverage row, one such output is in this same listing
// at the recorded size, nothing protects the key, and it is not recent.
func (r *rules) keCoveredDelete(o object, ids []identity) (string, bool) {
	if len(r.KECoverage) == 0 || len(ids) == 0 || strings.HasPrefix(o.Key, keManifestPrefix) || r.OutputKeys[o.Key] {
		return "", false
	}
	for _, id := range ids {
		if id.Kind != kindRaw {
			return "", false
		}
	}
	if _, ok := r.Keys[o.Key]; ok {
		return "", false
	}
	for _, p := range r.Prefixes {
		if strings.HasPrefix(o.Key, p.Prefix) {
			return "", false
		}
	}
	if o.MTime >= r.RecentFrom {
		return "", false
	}
	if out := r.keCovered(o); out != "" {
		return out, true
	}
	return "", false
}

// loadKECoverage reads the owner's raw_key,raw_size,output_key,output_size
// TSV. It is strict: a malformed row, an empty key, a non-positive size, an
// output also listed as raw, or a duplicate (raw, output) pair refuses the run.
func (r *rules) loadKECoverage(path string) error {
	r.KECoverage, r.OutputKeys, r.Outputs = map[string][]keCoverage{}, map[string]bool{}, map[string]int64{}
	seen := map[[2]string]bool{}
	if err := eachTSV(path, 4, func(c []string) error {
		if c[0] == "" || c[2] == "" {
			return errors.New("empty raw or output key")
		}
		rawSize, err := strconv.ParseInt(c[1], 10, 64)
		if err != nil {
			return fmt.Errorf("raw_size: %w", err)
		}
		outSize, err := strconv.ParseInt(c[3], 10, 64)
		if err != nil {
			return fmt.Errorf("output_size: %w", err)
		}
		if rawSize <= 0 || outSize <= 0 {
			return fmt.Errorf("non-positive size %d,%d", rawSize, outSize)
		}
		if strings.HasPrefix(c[2], keManifestPrefix) || strings.HasPrefix(c[0], keManifestPrefix) {
			return fmt.Errorf("manifest key %q never covers or is covered", c[0])
		}
		pair := [2]string{c[0], c[2]}
		if seen[pair] {
			return fmt.Errorf("duplicate coverage row %s -> %s", c[0], c[2])
		}
		seen[pair] = true
		if r.OutputKeys[c[0]] {
			return fmt.Errorf("raw key %q is itself a coverage output", c[0])
		}
		r.OutputKeys[c[2]] = true
		r.KECoverage[c[0]] = append(r.KECoverage[c[0]], keCoverage{c[2], rawSize, outSize})
		return nil
	}); err != nil {
		return err
	}
	for raw := range r.KECoverage {
		if r.OutputKeys[raw] {
			return fmt.Errorf("raw key %q is itself a coverage output", raw)
		}
	}
	return nil
}

// keCovered reports the first covering row whose output the listing being
// decided really holds at the recorded size, "" when none does.
func (r *rules) keCovered(o object) string {
	for _, c := range r.KECoverage[o.Key] {
		if c.RawSize == o.Size && r.Outputs[c.OutputKey] == c.OutputSize {
			return c.OutputKey
		}
	}
	return ""
}

// decide classifies one listed object. The order fixes the reported reason;
// any protection wins over deletion.
func (r *rules) decide(o object) (class, string) {
	for _, p := range r.Prefixes {
		if strings.HasPrefix(o.Key, p.Prefix) {
			return p.Class, p.Reason
		}
	}
	if k, ok := r.Keys[o.Key]; ok && k.Class == protectLiveSite {
		return k.Class, k.Reason
	}
	ids := r.Identities[o.Key]
	keReason := ""
	switch {
	case r.KEFolders[plazaFolder(o.Key)]:
		keReason = "keep_everything_folder_in_key"
	default:
		for _, id := range ids {
			if r.KE[id.Stream] {
				keReason = "keep_everything_stream"
				break
			}
			if r.KEFolders[id.Folder] {
				keReason = "keep_everything_folder"
				break
			}
		}
	}
	if keReason != "" {
		if out, ok := r.keCoveredDelete(o, ids); ok {
			return deleteKeRawStitched, "covered_by:" + topPrefix(out)
		}
		return protectKeepEverything, keReason
	}
	if len(ids) == 0 {
		return protectUnclassified, "no_catalog_identity:" + topPrefix(o.Key)
	}
	if ids[0].Kind == kindDuplicate {
		for _, id := range ids {
			if id.Kind != kindDuplicate || id.Size != o.Size {
				return protectUnclassified, "duplicate_identity_mismatch"
			}
		}
		if k, ok := r.Keys[o.Key]; ok {
			return k.Class, k.Reason
		}
		if o.MTime >= r.RecentFrom {
			return protectRecentWrite, string(kindDuplicate)
		}
		return deleteDuplicateOfPageLink, "same_sha256_and_size_as_page_key:" + topPrefix(o.Key)
	}
	inWindow, allOnNAS := false, true
	for _, id := range ids {
		if id.Kind != ids[0].Kind {
			return protectUnclassified, "mixed_identity_kinds"
		}
		w, ok := r.Windows[id.Stream]
		if !ok {
			return protectUnclassified, "stream_without_20d_window"
		}
		if id.Start <= 0 || id.End <= id.Start {
			return protectUnclassified, "missing_interval"
		}
		if id.Size != o.Size {
			return protectUnclassified, "catalog_size_differs_from_listing"
		}
		if id.Start < w.End && id.End > w.Start {
			inWindow = true
			allOnNAS = allOnNAS && id.OnNAS
		}
	}
	if inWindow && r.Evidence != nil {
		return r.decideWindowOnEvidence(o, ids[0].Kind, allOnNAS)
	}
	if inWindow && allOnNAS {
		return protectWindowOnNAS, string(ids[0].Kind)
	}
	if inWindow {
		return protectWindowNotOnNAS, string(ids[0].Kind)
	}
	if r.Closure[o.Key] {
		return protectStitchClosure, string(ids[0].Kind)
	}
	if k, ok := r.Keys[o.Key]; ok {
		return k.Class, k.Reason
	}
	if o.MTime >= r.RecentFrom {
		return protectRecentWrite, string(ids[0].Kind)
	}
	if ids[0].Kind == kindRaw {
		return deleteRawOutsideWindow, "outside_20d_window:" + topPrefix(o.Key)
	}
	return deleteCollatedOutsideWindow, "outside_20d_window:" + topPrefix(o.Key)
}

// decideWindowOnEvidence judges a 20-day key once the controller's window
// hold is lifted: it goes only on live exact NAS custody verified after both
// the NAS file and the R2 object last changed, and a RAW clip only once it is
// already a source of a stitched output (otherwise it may still need stitching).
func (r *rules) decideWindowOnEvidence(o object, k kind, catalogOnNAS bool) (class, string) {
	e, ok := r.Evidence[o.Key]
	switch {
	case (!ok || !e.Match) && catalogOnNAS:
		return protectWindowStale, "nas_evidence_missing"
	case !ok || !e.Match:
		return protectWindowNotOnNAS, string(k)
	case e.Verified < e.NASMTime:
		return protectWindowStale, "nas_file_changed_after_verify"
	case e.Verified < o.MTime:
		return protectWindowStale, "r2_object_changed_after_verify"
	case k == kindRaw && !r.Closure[o.Key]:
		return protectWindowUnjoined, string(k)
	}
	if rule, ok := r.Keys[o.Key]; ok {
		return rule.Class, rule.Reason
	}
	if o.MTime >= r.RecentFrom {
		return protectRecentWrite, string(k)
	}
	return deleteWindowOnNAS, string(k) + ":" + topPrefix(o.Key)
}

const genericFolder = "recordings"

// plazaFolder is the folder a managed RAW key files its stream under:
// managed/acct-N/<folder>/..., or recordings/<id> for stoarama_v1 naming,
// whose first component is shared by every recording. "" for other keys.
func plazaFolder(key string) string {
	parts := strings.Split(key, "/")
	if len(parts) < 4 || parts[0] != "managed" || !strings.HasPrefix(parts[1], "acct-") {
		return ""
	}
	if parts[2] == genericFolder {
		return parts[2] + "/" + parts[3]
	}
	return parts[2]
}

// topPrefix names the directory a key sits in, at most two levels deep.
func topPrefix(key string) string {
	parts := strings.Split(key, "/")
	switch {
	case len(parts) == 1:
		return "(root)"
	case len(parts) == 2:
		return parts[0]
	}
	return parts[0] + "/" + parts[1]
}

func eachLine(path string, fn func(string) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	n := 0
	for sc.Scan() {
		n++
		if sc.Text() == "" {
			continue
		}
		if err := fn(sc.Text()); err != nil {
			return fmt.Errorf("%s:%d: %w", path, n, err)
		}
	}
	return sc.Err()
}

func eachTSV(path string, cols int, fn func([]string) error) error {
	return eachLine(path, func(line string) error {
		c := strings.Split(line, "\t")
		if len(c) != cols {
			return fmt.Errorf("want %d columns, got %d", cols, len(c))
		}
		return fn(c)
	})
}
