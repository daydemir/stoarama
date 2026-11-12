package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/daydemir/stoarama/backend/internal/api"
	"github.com/daydemir/stoarama/backend/internal/r2"
)

type r2RetirePhase string

const (
	r2RetireNow     r2RetirePhase = "now"
	r2RetireLater   r2RetirePhase = "later"
	r2RetireCopy    r2RetirePhase = "copy"
	r2RetireBlocked r2RetirePhase = "blocked"
)

var r2RetirePhases = []r2RetirePhase{r2RetireNow, r2RetireLater, r2RetireCopy, r2RetireBlocked}

type r2RetirePhaseSummary struct {
	CopyBatches         map[string]string                        `json:"copy_batches_sha256"`
	Totals              map[r2RetirePhase]r2RetireStat           `json:"totals"`
	Streams             map[int64]map[r2RetirePhase]r2RetireStat `json:"streams"`
	Reasons             map[string]r2RetireStat                  `json:"reasons"`
	Phase1              r2RetireStat                             `json:"phase1"`
	Phase1SHA256        string                                   `json:"phase1_sha256"`
	Manifests           map[r2RetirePhase]string                 `json:"manifests_sha256"`
	CatalogAbsent       r2RetireStat                             `json:"catalog_already_absent_from_r2"`
	NASRawRetainedBytes int64                                    `json:"nas_raw_retained_bytes"`
	NASFreeReportedAt   *time.Time                               `json:"nas_free_reported_at"`
	NASFreeBytes        int64                                    `json:"nas_free_bytes"`
	NASReserveBytes     int64                                    `json:"nas_reserve_bytes"`
	CopyFits            bool                                     `json:"copy_fits_with_reserve"`
	// Planning-only items; never emitted in summary JSON.
	items map[r2RetirePhase][]r2RetireItem
}

func newR2RetirePhases() *r2RetirePhaseSummary {
	return &r2RetirePhaseSummary{Totals: map[r2RetirePhase]r2RetireStat{}, Streams: map[int64]map[r2RetirePhase]r2RetireStat{}, Reasons: map[string]r2RetireStat{}, Manifests: map[r2RetirePhase]string{}, items: map[r2RetirePhase][]r2RetireItem{}, NASReserveBytes: 1_250_000_000_000}
}
func (p *r2RetirePhaseSummary) add(c r2RetireCandidate, reason, choice string, eligible bool) {
	phase := r2RetireBlocked
	switch {
	case eligible:
		phase = r2RetireNow
	case reason == "after_nas_cleanup":
		phase = r2RetireLater
	case reason == "copy_to_nas":
		phase = r2RetireCopy
	}
	st := p.Totals[phase]
	st.add(c.SizeBytes)
	p.Totals[phase] = st
	if p.Streams[c.StreamID] == nil {
		p.Streams[c.StreamID] = map[r2RetirePhase]r2RetireStat{}
	}
	st = p.Streams[c.StreamID][phase]
	st.add(c.SizeBytes)
	p.Streams[c.StreamID][phase] = st
	st = p.Reasons[reason]
	st.add(c.SizeBytes)
	p.Reasons[reason] = st
	p.items[phase] = append(p.items[phase], r2RetireItem{r2RetireCandidate: c, Choice: choice, Reason: reason})
	if c.Kind == "raw" && (reason == r2RetireNASReason || reason == "copy_to_nas") {
		p.NASRawRetainedBytes += c.SizeBytes
	}
}
func (p *r2RetirePhaseSummary) write(out string, h r2RetireHeader) error {
	for _, phase := range r2RetirePhases {
		sort.Slice(p.items[phase], func(i, j int) bool {
			a, b := p.items[phase][i], p.items[phase][j]
			if a.StreamID != b.StreamID {
				return a.StreamID < b.StreamID
			}
			if a.Kind != b.Kind {
				return a.Kind < b.Kind
			}
			if a.SourceID != b.SourceID {
				return a.SourceID < b.SourceID
			}
			return a.ObjectKey < b.ObjectKey
		})
		h.Phase = phase
		h.Items = int64(len(p.items[phase]))
		h.Bytes = p.Totals[phase].Bytes
		name := string(phase) + ".jsonl"
		if phase == r2RetireNow {
			name = r2RetireManifestName
		}
		digest, err := writeR2RetireManifest(filepath.Join(out, name), h, p.items[phase])
		if err != nil {
			return err
		}
		p.Manifests[phase] = digest
	}
	p.CopyBatches = map[string]string{}
	if err := os.Mkdir(filepath.Join(out, "copy-batches"), 0700); err != nil {
		return err
	}
	copies := p.items[r2RetireCopy]
	for n := 0; len(copies) > 0; n++ {
		end := 0
		var bytes int64
		for end < len(copies) && end < 5000 && bytes+copies[end].SizeBytes <= 100_000_000_000 {
			bytes += copies[end].SizeBytes
			end++
		}
		if end == 0 {
			return fmt.Errorf("copy object exceeds 100 GB batch limit")
		}
		h.Phase = r2RetireCopy
		h.Items = int64(end)
		h.Bytes = bytes
		name := fmt.Sprintf("copy-batches/%04d.jsonl", n)
		digest, err := writeR2RetireManifest(filepath.Join(out, name), h, copies[:end])
		if err != nil {
			return err
		}
		p.CopyBatches[name] = digest
		copies = copies[end:]
	}
	var phase1 []r2RetireItem
	for _, it := range p.items[r2RetireNow] {
		if it.Reason == r2RetireReason {
			phase1 = append(phase1, it)
			p.Phase1.add(it.SizeBytes)
		}
	}
	h.Phase = r2RetireNow
	h.Items = p.Phase1.Objects
	h.Bytes = p.Phase1.Bytes
	digest, err := writeR2RetireManifest(filepath.Join(out, "phase1.jsonl"), h, phase1)
	if err != nil {
		return err
	}
	p.Phase1SHA256 = digest
	for key := range h.Choices {
		id, err := strconv.ParseInt(key, 10, 64)
		if err != nil {
			return err
		}
		if p.Streams[id] == nil {
			p.Streams[id] = map[r2RetirePhase]r2RetireStat{}
		}
	}
	ids := make([]int64, 0, len(p.Streams))
	for id := range p.Streams {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	f, err := os.OpenFile(filepath.Join(out, "streams.tsv"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	fmt.Fprintln(f, "stream_id\tchoice\tnow_bytes\tlater_bytes\tcopy_bytes\tblocked_bytes\tnow_usd_month")
	for _, id := range ids {
		s := p.Streams[id]
		fmt.Fprintf(f, "%d\t%s\t%d\t%d\t%d\t%d\t%.2f\n", id, h.Choices[fmt.Sprint(id)], s[r2RetireNow].Bytes, s[r2RetireLater].Bytes, s[r2RetireCopy].Bytes, s[r2RetireBlocked].Bytes, r2RetireUSD(s[r2RetireNow].Bytes))
	}
	return f.Sync()
}

// List exact current keys instead of treating stale catalog rows as billed R2
// bytes. ETags are bound in the manifest and conditionally checked at deletion.
func r2RetireLiveObjects(ctx context.Context, store interface {
	ListPrefix(context.Context, string, int, func(r2.ObjectInfo) error) error
}, prefixes []string) (map[string]r2.ObjectInfo, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	objects := map[string]r2.ObjectInfo{}
	var mu sync.Mutex
	var first error
	var wg sync.WaitGroup
	slots := make(chan struct{}, 8)
	for _, prefix := range prefixes {
		wg.Add(1)
		go func(prefix string) {
			defer wg.Done()
			select {
			case slots <- struct{}{}:
			case <-ctx.Done():
				return
			}
			defer func() { <-slots }()
			err := store.ListPrefix(ctx, prefix, 0, func(o r2.ObjectInfo) error { mu.Lock(); defer mu.Unlock(); objects[o.Key] = o; return nil })
			if err != nil {
				mu.Lock()
				if first == nil {
					first = err
					cancel()
				}
				mu.Unlock()
			}
		}(prefix)
	}
	wg.Wait()
	if first != nil {
		return nil, first
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return objects, nil
}

// The catalog's complete folder/version prefixes can be listed concurrently.
// Never enumerate other accounts or unrelated historical prefixes to decide
// whether these exact catalog objects exist. Unmapped keys within scope stay.
func r2RetirePrefixes(ctx context.Context, q r2RetireQuerier, accountID int64, bucket string) ([]string, error) {
	rows, err := q.Query(ctx, `SELECT DISTINCT prefix FROM (
 SELECT CASE WHEN split_part(c.object_key,'/',4)='' THEN split_part(c.object_key,'/',1)||'/'||split_part(c.object_key,'/',2)||'/'
 ELSE split_part(c.object_key,'/',1)||'/'||split_part(c.object_key,'/',2)||'/'||split_part(c.object_key,'/',3)||'/' END prefix
 FROM recording_clips c JOIN recordings r ON r.id=c.recording_id
 WHERE r.account_id=$1 AND c.bucket=$2 AND c.purged_at IS NULL AND c.object_key LIKE 'managed/%/%'
 UNION
 SELECT split_part(o.object_key,'/',1)||'/'||split_part(o.object_key,'/',2)||'/'
 FROM recording_collation_outputs o JOIN recording_collation_hours h ON h.id=o.collation_hour_id JOIN recordings r ON r.id=h.recording_id
 WHERE r.account_id=$1 AND o.object_key LIKE 'joined/%/%') p ORDER BY prefix`, accountID, bucket)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var prefixes []string
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			return nil, err
		}
		if len(prefixes) == 0 || !strings.HasPrefix(p, prefixes[len(prefixes)-1]) {
			prefixes = append(prefixes, p)
		}
	}
	return prefixes, rows.Err()
}

func writeR2RetireListing(path, bucket string, prefixes []string, objects map[string]r2.ObjectInfo) (string, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return "", err
	}
	defer f.Close()
	hash := sha256.New()
	w := bufio.NewWriter(io.MultiWriter(f, hash))
	enc := json.NewEncoder(w)
	if err := enc.Encode(map[string]any{"bucket": bucket, "listed_at": time.Now().UTC(), "prefixes": prefixes, "objects": len(objects)}); err != nil {
		return "", err
	}
	keys := make([]string, 0, len(objects))
	for key := range objects {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if err := enc.Encode(objects[key]); err != nil {
			return "", err
		}
	}
	if err := w.Flush(); err != nil {
		return "", err
	}
	if err := f.Sync(); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

// A planning manifest is not deletion authority. Every item is reselected under
// the cleanup admission/public fences and matching inventory rows are locked.
func r2RetireDecideForScope(c r2RetireCandidate, scope r2RetireScope, windows map[int64]api.Keep28Window, picks map[int64]r2RetirePick) (string, string, bool) {
	if scope.Class == r2RetireOwnerAccepted {
		if !scope.NASBacked {
			return "owner_class_requires_conditional_safety", "", false
		}
		// Only absence proof is waived. Keep the broad inventory observation,
		// and never invoke the listing/receipt evidence that can clear it.
		w, exists := windows[c.StreamID]
		if !exists || !r2RetireOutside(w, c.StartAt, c.EndAt) || !r2RetireOutside(w, c.SourceStart, c.SourceEnd) {
			return "inside_keep_window", "", false
		}
		c.Facts.NASAbsent = true
		reason, choice, eligible := r2RetireDecideMode(c, scope.Bucket, windows, picks, true)
		if eligible {
			return string(r2RetireOwnerAccepted), choice, true
		}
		return reason, choice, false
	}
	return r2RetireDecideMode(c, scope.Bucket, windows, picks, scope.NASBacked)
}

func r2RetireLockNASIdentity(ctx context.Context, q r2RetireQuerier, it r2RetireItem, connectionID int64) error {
	pathSQL := `n.relative_path=$5`
	if it.Kind == "collated" {
		pathSQL = `n.relative_path IN ($5,'joined/'||$5)`
	}
	var id int64
	err := q.QueryRow(ctx, `SELECT n.clip_id FROM nas_inventory_files n WHERE n.connection_id=$1 AND n.state='present'
 AND n.sha256=$2 AND n.size_bytes=$3 AND n.verified_at BETWEEN now()-interval '24 hours' AND now()+interval '5 minutes' AND n.server_received_at>=now()-interval '24 hours' AND ($4::bigint=0 OR n.clip_id=$4) AND `+pathSQL+` ORDER BY n.clip_id LIMIT 1 FOR SHARE`, connectionID, it.SHA256, it.SizeBytes, func() int64 {
		if it.Kind == "raw" {
			return it.SourceID
		}
		return 0
	}(), it.RelativePath).Scan(&id)
	if err != nil {
		return fmt.Errorf("exact present NAS identity no longer lockable: %w", err)
	}
	return nil
}

func (a *r2RetireApplier) reserve(ctx context.Context, it r2RetireItem) error {
	columns := "kind,source_id,bucket,object_key,size_bytes,sha256,manifest_sha256"
	values := "$1,$2,$3,$4,$5,$6,$7"
	predicate := "kind=$1 AND source_id=$2 AND bucket=$3 AND object_key=$4 AND size_bytes=$5 AND sha256=$6 AND manifest_sha256=$7"
	args := []any{it.Kind, it.SourceID, it.Bucket, it.ObjectKey, it.SizeBytes, it.SHA256, a.digest}
	if a.scope.Class == r2RetireOwnerAccepted {
		columns += ",retirement_class"
		values += ",$8"
		predicate += " AND retirement_class=$8"
		args = append(args, a.scope.Class)
	}
	tag, err := a.pool.Exec(ctx, `INSERT INTO r2_retirement_reservations(`+columns+`) VALUES(`+values+`) ON CONFLICT DO NOTHING`, args...)

	if err != nil {
		return err
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	var same bool
	err = a.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM r2_retirement_reservations WHERE `+predicate+`)`, args...).Scan(&same)
	if err != nil {
		return err
	}
	if !same {
		return r2RetireAnomalyf("R2 identity reserved by another manifest")
	}
	return nil
}

// A checksummed recent listing can be reused while refining DB-only planning.
// It is evidence only: apply still HEADs and conditionally deletes every key.
func readR2RetireListing(path, want, bucket string) (map[string]r2.ObjectInfo, error) {
	if !r2RetireLowerHex64(want) {
		return nil, fmt.Errorf("listing requires --expected-listing-sha256")
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	hash := sha256.New()
	scan := bufio.NewScanner(io.TeeReader(f, hash))
	scan.Buffer(make([]byte, 65536), 1<<20)
	if !scan.Scan() {
		return nil, fmt.Errorf("listing has no header")
	}
	var h struct {
		Bucket   string    `json:"bucket"`
		ListedAt time.Time `json:"listed_at"`
	}
	if err := json.Unmarshal(scan.Bytes(), &h); err != nil {
		return nil, err
	}
	if h.Bucket != bucket || time.Since(h.ListedAt) > 2*time.Hour || h.ListedAt.After(time.Now().Add(time.Minute)) {
		return nil, fmt.Errorf("listing bucket/age differs; list again")
	}
	objects := map[string]r2.ObjectInfo{}
	for scan.Scan() {
		var o r2.ObjectInfo
		if err := json.Unmarshal(scan.Bytes(), &o); err != nil {
			return nil, err
		}
		if o.Key == "" || o.ETag == "" || o.SizeBytes < 0 {
			return nil, fmt.Errorf("invalid listing identity")
		}
		if _, exists := objects[o.Key]; exists {
			return nil, fmt.Errorf("duplicate listing key")
		}
		objects[o.Key] = o
	}
	if err := scan.Err(); err != nil {
		return nil, err
	}
	if hex.EncodeToString(hash.Sum(nil)) != want {
		return nil, fmt.Errorf("listing digest differs")
	}
	return objects, nil
}

// R2's compatibility docs do not promise If-Match on DeleteObject. A unit
// fake only proves we send the header. Require an operator's live scratch-key
// probe rather than silently relying on a header the provider might ignore.
func checkR2RetireDeleteProof(path, want, bucket, endpointSHA string) error {
	if !r2RetireLowerHex64(want) {
		return fmt.Errorf("NAS-backed apply requires --conditional-delete-proof and --expected-delete-proof-sha256; run the operator-only probe first")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(raw)
	if hex.EncodeToString(sum[:]) != want {
		return fmt.Errorf("conditional-delete proof digest differs")
	}
	var p struct {
		Bucket         string    `json:"bucket"`
		EndpointSHA256 string    `json:"endpoint_sha256"`
		CreatedAt      time.Time `json:"created_at"`
		Rejected       bool      `json:"wrong_etag_rejected"`
		Preserved      bool      `json:"object_preserved"`
		Confirmed      bool      `json:"matching_delete_confirmed"`
	}
	if err := json.Unmarshal(raw, &p); err != nil {
		return err
	}
	if p.Bucket != bucket || p.EndpointSHA256 != endpointSHA || !p.Rejected || !p.Preserved || !p.Confirmed || time.Since(p.CreatedAt) > 24*time.Hour || p.CreatedAt.After(time.Now().Add(time.Minute)) {
		return fmt.Errorf("conditional-delete proof bucket/endpoint/results/age differ; run the operator-only probe again")
	}
	return nil
}
