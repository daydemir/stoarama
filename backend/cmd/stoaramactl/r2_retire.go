package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/daydemir/stoarama/backend/internal/api"
	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/r2"
)

// r2-retire deletes R2 objects for footage the owner decided not to keep.
//
//	plan  (read-only): exact manifest of what phase 1 would delete, per stream,
//	      with totals, $/month and the manifest digest; HEADs a sample.
//	apply: re-validates every manifest item against live DB state, then
//	      deletes ONE object at a time (never by prefix or batch), recording
//	      purged_at (raw) and an r2_object_retirements row in the same
//	      transaction, with an fsynced local JSONL ledger, a per-run byte cap,
//	      a rolling 24h budget, and a stop on the first anomaly.
//
// Phase 1 scope is defined in r2_retire_select.go.

const (
	r2RetireAppName         = "stoarama-r2-retire"
	r2RetireManifestVersion = 2
	r2RetireAdvisoryLockKey = int64(7343226011173)
	r2RetireFinishTimeout   = 2 * time.Minute
	r2RetireManifestName    = "manifest.jsonl"
	// r2RetireMaxManifestAge caps --max-manifest-age; staleness can't be disabled.
	r2RetireMaxManifestAge = 7 * 24 * time.Hour
)

type r2RetireHeader struct {
	DeliveryKeepSHA256  string            `json:"delivery_keep_sha256"`
	HeadGuardCompatible bool              `json:"head_guard_compatible,omitempty"`
	PreviewOnly         bool              `json:"preview_only,omitempty"`
	Class               r2RetireClass     `json:"retirement_class,omitempty"`
	R2ListingSHA256     string            `json:"r2_listing_sha256,omitempty"`
	NASBacked           bool              `json:"nas_backed,omitempty"`
	Phase               r2RetirePhase     `json:"phase,omitempty"`
	Version             int               `json:"version"`
	Tool                string            `json:"tool"`
	Reason              string            `json:"reason"`
	CreatedAt           time.Time         `json:"created_at"`
	PolicySHA256        string            `json:"policy_sha256"`
	PicksPath           string            `json:"picks_path"`
	PicksSHA256         string            `json:"picks_sha256"`
	PicksSavedAt        string            `json:"picks_saved_at"`
	Choices             map[string]string `json:"choices"`
	Bucket              string            `json:"bucket"`
	AccountID           int64             `json:"account_id"`
	ConnectionID        int64             `json:"connection_id"`
	GraceSeconds        int64             `json:"grace_seconds"`
	Items               int64             `json:"items"`
	Bytes               int64             `json:"bytes"`
}

type r2RetireItem struct {
	r2RetireCandidate
	Choice string `json:"choice"`
	Reason string `json:"reason"`
}

type r2RetireStat struct {
	Objects int64 `json:"objects"`
	Bytes   int64 `json:"bytes"`
}

func (s *r2RetireStat) add(n int64) { s.Objects++; s.Bytes += n }

type r2RetireStreamStat struct {
	StreamID int64        `json:"stream_id"`
	Choice   string       `json:"choice"`
	Raw      r2RetireStat `json:"raw"`
	Collated r2RetireStat `json:"collated"`
	USDMonth float64      `json:"usd_per_month"`
}

type r2RetireHeadSample struct {
	Requested     int      `json:"requested"`
	Checked       int64    `json:"checked"`
	Present       int64    `json:"present"`
	Missing       int64    `json:"missing"`
	MissingBytes  int64    `json:"missing_bytes"`
	SizeMismatch  int64    `json:"size_mismatch"`
	SHAMismatch   int64    `json:"sha_metadata_mismatch"`
	Errors        int64    `json:"errors"`
	MissingRate   float64  `json:"missing_rate"`
	MissingSample []string `json:"missing_examples,omitempty"`
}

type r2RetirePlanSummary struct {
	Projection       *r2RetireProjection                `json:"evidence_projection,omitempty"`
	Phases           *r2RetirePhaseSummary              `json:"phases,omitempty"`
	Header           r2RetireHeader                     `json:"header"`
	ManifestPath     string                             `json:"manifest_path"`
	ManifestSHA256   string                             `json:"manifest_sha256"`
	Selected         r2RetireStat                       `json:"selected"`
	SelectedByKind   map[string]r2RetireStat            `json:"selected_by_kind"`
	SavedUSDPerMonth float64                            `json:"saved_usd_per_month"`
	Scanned          map[string]r2RetireStat            `json:"scanned_by_kind"`
	Kept             map[string]map[string]r2RetireStat `json:"kept_by_kind_and_reason"`
	Streams          []r2RetireStreamStat               `json:"streams"`
	HeadSample       *r2RetireHeadSample                `json:"head_sample,omitempty"`
	ElapsedSec       float64                            `json:"elapsed_sec"`
}

func r2RetireUSD(bytes int64) float64 { return float64(bytes) / r2RetireBytesPerTB * r2RetireUSDPerTB }

type r2RetireHeadStore interface {
	Bucket() string
	Head(ctx context.Context, key string) (r2.ObjectHead, error)
}

type r2RetireStore interface {
	r2RetireHeadStore
	DeleteObject(ctx context.Context, key string) error
	EndpointSHA256() string
}

var _ r2RetireStore = (*r2.Client)(nil)

// ---------------------------------------------------------------- plan

type r2RetirePlanOptions struct {
	Delivery            api.DeliveryKeepSnapshot
	DeliveryPreview     bool
	HeadGuardCompatible bool
	Projection          *r2RetireProjection
	LiveObjects         map[string]r2.ObjectInfo
	Picks               r2RetirePicks
	Scope               r2RetireScope
	PageSize            int
	Sample              int
	SampleWorker        int
	Now                 time.Time
}

// r2RetirePlanRun is read-only: it scans the catalog, decides every object,
// and returns the summary and the sorted manifest items.
func r2RetirePlanRun(ctx context.Context, q r2RetireQuerier, opts r2RetirePlanOptions, policySHA string, windows map[int64]api.Keep28Window) (r2RetirePlanSummary, []r2RetireItem, error) {
	started := time.Now()
	if len(opts.Delivery.Objects) == 0 {
		return r2RetirePlanSummary{}, nil, errors.New("live delivery keep snapshot required")
	}
	if !opts.DeliveryPreview {
		if err := api.RequireDeliveryKeep(ctx, q, opts.Delivery); err != nil {
			return r2RetirePlanSummary{}, nil, err
		}
	}
	if opts.Scope.Class == r2RetireDeliveryOnly && !opts.DeliveryPreview && opts.HeadGuardCompatible {
		if err := r2RetireRequireDeliveryApplySchema(ctx, q); err != nil {
			return r2RetirePlanSummary{}, nil, err
		}
	}
	var catalogETags map[string]string
	if opts.HeadGuardCompatible {
		if !opts.Scope.NASBacked {
			return r2RetirePlanSummary{}, nil, errors.New("HEAD-guard planning requires a live R2 listing")
		}
		var err error
		catalogETags, err = r2RetireCatalogETags(ctx, q, opts.Scope.AccountID, opts.Scope.Bucket)
		if err != nil {
			return r2RetirePlanSummary{}, nil, err
		}
	}
	sets, err := loadR2RetireSets(ctx, q, "", opts.Scope.NASBacked, opts.Scope.Class)
	if err != nil {
		return r2RetirePlanSummary{}, nil, err
	}
	if !opts.Scope.Class.valid() {
		return r2RetirePlanSummary{}, nil, errors.New("unknown retirement class")
	}
	if opts.Scope.Class.owner() {
		if err := sets.loadOwnerProtections(ctx, q, opts.Scope, windows, opts.Picks.Choices); err != nil {
			return r2RetirePlanSummary{}, nil, err
		}
	}
	sum := r2RetirePlanSummary{SelectedByKind: map[string]r2RetireStat{}, Scanned: map[string]r2RetireStat{}, Kept: map[string]map[string]r2RetireStat{}}
	if opts.Scope.NASBacked {
		if opts.LiveObjects == nil {
			return sum, nil, errors.New("NAS-backed planning requires a live R2 listing")
		}
		sum.Phases = newR2RetirePhases()
	}
	if opts.Projection != nil && opts.Projection.Class != opts.Scope.Class {
		return sum, nil, errors.New("hold-release preview class differs")
	}
	sum.Projection = opts.Projection
	for key := range opts.Delivery.Objects {
		sets.DeliveryKeys[r2RetireObjectKey{opts.Scope.Bucket, key}] = true
	}
	seenKeys := map[string]bool{}
	var items []r2RetireItem
	streams := map[int64]*r2RetireStreamStat{}
	for _, kind := range []string{"raw", "collated"} {
		sum.Kept[kind] = map[string]r2RetireStat{}
		err := r2RetireScan(ctx, q, kind, opts.Scope, sets, opts.PageSize, func(c r2RetireCandidate) error {
			st := sum.Scanned[kind]
			st.add(c.SizeBytes)
			sum.Scanned[kind] = st
			if opts.Scope.NASBacked {
				if seenKeys[c.ObjectKey] {
					return nil
				}
				seenKeys[c.ObjectKey] = true
				o, exists := opts.LiveObjects[c.ObjectKey]
				if !exists {
					sum.Phases.CatalogAbsent.add(c.SizeBytes)
					return nil
				}
				c.ETag = o.ETag
				if o.SizeBytes != c.SizeBytes || o.ETag == "" {
					c.SizeBytes = o.SizeBytes
					sum.Phases.add(c, "r2_identity_mismatch", "", false)
					return nil
				}
			}
			reason, choice, ok := r2RetireDecideForScope(c, opts.Scope, windows, opts.Picks.Choices)
			if strings.HasPrefix(c.ObjectKey, "staging/") && !c.Facts.DeliveryHeld && !c.Facts.JoinedRef && !c.Facts.SharedKey {
				reason, ok = "staging_owner_visibility", false
			}
			if opts.HeadGuardCompatible && ok {
				if guardReason := r2RetireGuardCatalogReason(c, catalogETags); guardReason != "" {
					reason, ok = guardReason, false
				}
			}
			if opts.Projection != nil {
				opts.Projection.add(c, opts.Scope, opts.Picks, windows, ok)
			}
			if sum.Phases != nil {
				sum.Phases.add(c, reason, choice, ok)
			}
			if !ok {
				k := sum.Kept[kind][reason]
				k.add(c.SizeBytes)
				sum.Kept[kind][reason] = k
				return nil
			}
			items = append(items, r2RetireItem{r2RetireCandidate: c, Choice: choice, Reason: reason})
			ss := streams[c.StreamID]
			if ss == nil {
				ss = &r2RetireStreamStat{StreamID: c.StreamID, Choice: choice}
				streams[c.StreamID] = ss
			}
			if kind == "raw" {
				ss.Raw.add(c.SizeBytes)
			} else {
				ss.Collated.add(c.SizeBytes)
			}
			return nil
		})
		if err != nil {
			return sum, nil, err
		}
	}
	sort.Slice(items, func(i, j int) bool {
		a, b := items[i], items[j]
		if a.StreamID != b.StreamID {
			return a.StreamID < b.StreamID
		}
		if a.Kind != b.Kind {
			return a.Kind < b.Kind
		}
		return a.SourceID < b.SourceID
	})
	choices := map[string]string{}
	for id, p := range opts.Picks.Choices {
		if w, ok := windows[id]; ok {
			if c := r2RetireEffectiveChoice(p, true, w); c != "" {
				choices[strconv.FormatInt(id, 10)] = c
			}
		}
	}
	if opts.Scope.Class == r2RetireDeliveryOnly {
		choices = r2RetireDeliveryChoices(opts.Picks.Choices, windows)
	}
	if sum.Phases != nil {
		for key, o := range opts.LiveObjects {
			if !seenKeys[key] {
				reason := "uncatalogued_or_purged"
				if _, held := opts.Delivery.Objects[key]; held {
					reason = "delivery_keep_hold"
				} else if strings.HasPrefix(key, "staging/") && !sets.ArtifactKeys[key] && sets.OutputKeyCount[key] == 0 {
					reason = "staging_owner_visibility"
				}
				sum.Phases.add(r2RetireCandidate{Bucket: opts.Scope.Bucket, ObjectKey: key, SizeBytes: o.SizeBytes, ETag: o.ETag}, reason, "", false)
			}
		}
	}
	sum.Header = r2RetireHeader{DeliveryKeepSHA256: opts.Delivery.SHA256, HeadGuardCompatible: opts.HeadGuardCompatible, PreviewOnly: opts.DeliveryPreview || (opts.Scope.Class == r2RetireDeliveryOnly && !opts.HeadGuardCompatible), Class: opts.Scope.Class, NASBacked: opts.Scope.NASBacked, Version: r2RetireManifestVersion, Tool: "stoaramactl r2-retire", Reason: r2RetireReason, CreatedAt: opts.Now.UTC(),
		PolicySHA256: policySHA, PicksPath: opts.Picks.Path, PicksSHA256: opts.Picks.SHA256, PicksSavedAt: opts.Picks.SavedAt, Choices: choices,
		Bucket: opts.Scope.Bucket, AccountID: opts.Scope.AccountID, ConnectionID: opts.Scope.ConnectionID, GraceSeconds: int64(opts.Scope.Grace / time.Second)}
	if opts.Scope.Class == r2RetireDeliveryOnly {
		sum.Header.Phase = r2RetireNow
	}
	for _, it := range items {
		sum.Selected.add(it.SizeBytes)
		k := sum.SelectedByKind[it.Kind]
		k.add(it.SizeBytes)
		sum.SelectedByKind[it.Kind] = k
	}
	sum.Header.Items, sum.Header.Bytes = sum.Selected.Objects, sum.Selected.Bytes
	sum.SavedUSDPerMonth = r2RetireUSD(sum.Selected.Bytes)
	for _, ss := range streams {
		ss.USDMonth = r2RetireUSD(ss.Raw.Bytes + ss.Collated.Bytes)
		sum.Streams = append(sum.Streams, *ss)
	}
	sort.Slice(sum.Streams, func(i, j int) bool { return sum.Streams[i].StreamID < sum.Streams[j].StreamID })
	sum.ElapsedSec = time.Since(started).Seconds()
	return sum, items, nil
}

// r2RetireHeadSampleRun HEADs a random sample of manifest items (read-only).
func r2RetireHeadSampleRun(ctx context.Context, store r2RetireHeadStore, items []r2RetireItem, n, workers int, seed int64) r2RetireHeadSample {
	out := r2RetireHeadSample{Requested: n}
	if n <= 0 || len(items) == 0 {
		return out
	}
	idx := rand.New(rand.NewSource(seed)).Perm(len(items))
	if n < len(idx) {
		idx = idx[:n]
	}
	if workers < 1 {
		workers = 1
	}
	var mu sync.Mutex
	var wg sync.WaitGroup
	ch := make(chan r2RetireItem)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for it := range ch {
				h, err := store.Head(ctx, it.ObjectKey)
				mu.Lock()
				out.Checked++
				switch {
				case err != nil && r2.IsNotFound(err):
					out.Missing++
					out.MissingBytes += it.SizeBytes
					if len(out.MissingSample) < 10 {
						out.MissingSample = append(out.MissingSample, it.ObjectKey)
					}
				case err != nil:
					out.Errors++
				case h.SizeBytes != it.SizeBytes:
					out.SizeMismatch++
				case h.SHA256Metadata != "" && !strings.EqualFold(h.SHA256Metadata, it.SHA256):
					out.SHAMismatch++
				default:
					out.Present++
				}
				mu.Unlock()
			}
		}()
	}
	for _, i := range idx {
		if ctx.Err() != nil {
			break
		}
		ch <- items[i]
	}
	close(ch)
	wg.Wait()
	if out.Checked > 0 {
		out.MissingRate = float64(out.Missing) / float64(out.Checked)
	}
	return out
}

// writeR2RetireManifest writes {"header":...} then one item per line, fsyncs,
// renames into place, and returns the sha256 of the exact bytes.
func writeR2RetireManifest(path string, h r2RetireHeader, items []r2RetireItem) (string, error) {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return "", err
	}
	defer os.Remove(tmp)
	hash := sha256.New()
	var sink io.Writer = io.MultiWriter(f, hash)
	var compressed *gzip.Writer
	if strings.HasSuffix(path, ".gz") {
		compressed = gzip.NewWriter(sink)
		sink = compressed
	}
	w := bufio.NewWriterSize(sink, 1<<20)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(map[string]any{"header": h}); err != nil {
		f.Close()
		return "", err
	}
	for _, it := range items {
		if err := enc.Encode(it); err != nil {
			f.Close()
			return "", err
		}
	}
	if err := w.Flush(); err != nil {
		f.Close()
		return "", err
	}
	if compressed != nil {
		if err := compressed.Close(); err != nil {
			f.Close()
			return "", err
		}
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return "", err
	}
	if err := f.Close(); err != nil {
		return "", err
	}
	if err := os.Rename(tmp, path); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

// readR2RetireManifest parses and validates a manifest and returns its digest.
func readR2RetireManifest(path string) (r2RetireHeader, []r2RetireItem, string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return r2RetireHeader{}, nil, "", err
	}
	sum := sha256.Sum256(raw)
	digest := hex.EncodeToString(sum[:])
	if len(raw) == 0 || raw[len(raw)-1] != '\n' {
		return r2RetireHeader{}, nil, digest, errors.New("manifest is empty or truncated")
	}
	lines := bytes.Split(raw[:len(raw)-1], []byte("\n"))
	var first struct {
		Header *r2RetireHeader `json:"header"`
	}
	if err := json.Unmarshal(lines[0], &first); err != nil || first.Header == nil {
		return r2RetireHeader{}, nil, digest, errors.New("manifest has no header line")
	}
	h := *first.Header
	if !h.Class.valid() || (h.Class != "" && !h.NASBacked) || h.Version != r2RetireManifestVersion || !r2RetireLowerHex64(h.DeliveryKeepSHA256) || h.PreviewOnly || (h.Class == r2RetireDeliveryOnly && (!h.HeadGuardCompatible || h.Phase != r2RetireNow)) || h.Reason != r2RetireReason || (h.Phase != "" && h.Phase != r2RetireNow) {
		return h, nil, digest, fmt.Errorf("unsupported manifest version %d / reason %q", h.Version, h.Reason)
	}
	items := make([]r2RetireItem, 0, len(lines)-1)
	seenSource, seenKey := map[string]bool{}, map[string]bool{}
	var total int64
	for n, line := range lines[1:] {
		dec := json.NewDecoder(bytes.NewReader(line))
		dec.DisallowUnknownFields()
		var it r2RetireItem
		if err := dec.Decode(&it); err != nil {
			return h, nil, digest, fmt.Errorf("manifest line %d: %w", n+2, err)
		}
		src := it.Kind + ":" + strconv.FormatInt(it.SourceID, 10)
		if (h.Class == r2RetireDeliveryOnly && it.Choice == "all") || (it.Kind != "raw" && it.Kind != "collated") || it.SourceID <= 0 || (h.Class == r2RetireOwnerAccepted && it.Reason != string(h.Class)) || ((h.Class == "" || h.Class == r2RetireDeliveryOnly) && it.Reason != r2RetireReason && !(h.NASBacked && it.Reason == r2RetireNASReason)) || (h.NASBacked && it.ETag == "") || seenSource[src] || seenKey[it.ObjectKey] ||
			it.Choice != h.Choices[strconv.FormatInt(it.StreamID, 10)] || it.Bucket != h.Bucket || it.SizeBytes <= 0 {
			return h, nil, digest, fmt.Errorf("manifest line %d: invalid or duplicate item", n+2)
		}
		if r := r2RetireKeyProtection(it.Kind, it.Bucket, it.ObjectKey, h.Bucket); r != "" {
			return h, nil, digest, fmt.Errorf("manifest line %d: %s key %q", n+2, r, it.ObjectKey)
		}
		seenSource[src], seenKey[it.ObjectKey] = true, true
		total += it.SizeBytes
		items = append(items, it)
	}
	if int64(len(items)) != h.Items || total != h.Bytes {
		return h, nil, digest, errors.New("manifest totals do not match its header")
	}
	return h, items, digest, nil
}

// ---------------------------------------------------------------- ledger

type r2RetireOperatorAcknowledgment struct {
	DecisionReference string    `json:"decision_reference"`
	At                time.Time `json:"at"`
}
type r2RetireLedgerLine struct {
	OperatorCredentialRisk *r2RetireOperatorAcknowledgment `json:"operator_credential_risk,omitempty"`
	ETag                   string                          `json:"etag,omitempty"`
	LastModified           time.Time                       `json:"last_modified,omitzero"`
	At                     time.Time                       `json:"at"`
	Event                  string                          `json:"event"`
	ManifestSHA256         string                          `json:"manifest_sha256"`
	Kind                   string                          `json:"kind,omitempty"`
	SourceID               int64                           `json:"source_id,omitempty"`
	StreamID               int64                           `json:"stream_id,omitempty"`
	ObjectKey              string                          `json:"object_key,omitempty"`
	SizeBytes              int64                           `json:"size_bytes,omitempty"`
	SHA256                 string                          `json:"sha256,omitempty"`
	Detail                 string                          `json:"detail,omitempty"`
}

// r2RetireLedger is append-only, exclusively locked, and fsynced per line.
// An "intent" line is written (and synced) before every R2 delete, so a run
// that crashed between the delete and the commit can be completed.
type r2RetireLedger struct {
	operatorCredentialRisk *r2RetireOperatorAcknowledgment
	f                      *os.File
	path                   string
	intents                map[string]bool
	torn                   int
}

func openR2RetireLedger(path string) (*r2RetireLedger, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("ledger %s is in use by another r2-retire: %w", path, err)
	}
	l := &r2RetireLedger{f: f, path: path, intents: map[string]bool{}}
	raw, err := io.ReadAll(f)
	if err != nil {
		f.Close()
		return nil, err
	}
	lines := bytes.Split(raw, []byte("\n"))
	for _, line := range lines {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var e r2RetireLedgerLine
		if err := json.Unmarshal(line, &e); err != nil {
			// A torn line from a crash mid-write. Its fsync never completed,
			// and every delete happens only after its intent line is synced,
			// so ignoring it is safe. The file itself is never rewritten.
			l.torn++
			continue
		}
		if e.Event == "intent" {
			l.intents[e.ObjectKey] = true
		}
	}
	if len(raw) > 0 && raw[len(raw)-1] != '\n' {
		if _, err := f.Write([]byte("\n")); err != nil {
			f.Close()
			return nil, err
		}
	}
	return l, nil
}

func (l *r2RetireLedger) write(e r2RetireLedgerLine) error {
	e.At = time.Now().UTC()
	e.OperatorCredentialRisk = l.operatorCredentialRisk
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	if _, err := l.f.Write(append(b, '\n')); err != nil {
		return fmt.Errorf("write ledger: %w", err)
	}
	if err := l.f.Sync(); err != nil {
		return fmt.Errorf("fsync ledger: %w", err)
	}
	if e.Event == "intent" {
		l.intents[e.ObjectKey] = true
	}
	return nil
}

func (l *r2RetireLedger) Close() error { return l.f.Close() }

// ---------------------------------------------------------------- apply

type r2RetireApplyOptions struct {
	Delivery                       api.DeliveryKeepSnapshot
	AcceptOperatorCredentialRisk   string
	ProviderUnconditionalHeadGuard bool
	MinObjectAge                   time.Duration
	DeleteProofPath                string
	DeleteProofSHA256              string
	ManifestPath                   string
	ExpectedSHA                    string
	PicksPath                      string
	LedgerPath                     string
	MaxBytes                       int64
	Budget24h                      int64
	MaxItems                       int64
	Rate                           float64
	SkipIneligible                 bool
	RecordAbsent                   bool
	MaxManifestAge                 time.Duration
}

type r2RetireApplySummary struct {
	OperatorCredentialRisk *r2RetireOperatorAcknowledgment `json:"operator_credential_risk,omitempty"`
	ResidualTOCTOU         bool                            `json:"residual_toctou"`
	ManifestSHA256         string                          `json:"manifest_sha256"`
	ManifestItems          int64                           `json:"manifest_items"`
	Retired                int64                           `json:"retired"`
	RetiredBytes           int64                           `json:"retired_bytes"`
	RetiredAbsent          int64                           `json:"retired_already_absent"`
	AlreadyDone            int64                           `json:"already_done"`
	SkippedIneligible      map[string]int64                `json:"skipped_ineligible"`
	Used24hBefore          int64                           `json:"used_24h_bytes_before"`
	LimitReached           string                          `json:"limit_reached,omitempty"`
	Interrupted            bool                            `json:"interrupted"`
	Anomaly                string                          `json:"anomaly,omitempty"`
	LedgerPath             string                          `json:"ledger_path"`
	SavedUSDPerMonth       float64                         `json:"saved_usd_per_month"`
	ElapsedSec             float64                         `json:"elapsed_sec"`
}

type r2RetireApplier struct {
	pool     *pgxpool.Pool
	store    r2RetireStore
	opts     r2RetireApplyOptions
	header   r2RetireHeader
	digest   string
	scope    r2RetireScope
	sets     r2RetireSets
	fp       string
	picks    r2RetirePicks
	windows  map[int64]api.Keep28Window
	policySH string
	ledger   *r2RetireLedger
}

// errR2RetireAnomaly marks a stop condition that needs an operator.
type errR2RetireAnomaly struct{ msg string }

func (e errR2RetireAnomaly) Error() string { return "anomaly: " + e.msg }

func r2RetireAnomalyf(format string, args ...any) error {
	return errR2RetireAnomaly{msg: fmt.Sprintf(format, args...)}
}

// r2RetireApply runs one apply pass. Every precondition failure returns before
// any write; every per-item anomaly stops the run with nothing half-done
// except what the ledger's intent line lets the next run complete.
func r2RetireApply(ctx context.Context, pool *pgxpool.Pool, store r2RetireStore, opts r2RetireApplyOptions, policySHA string, windows map[int64]api.Keep28Window) (sum r2RetireApplySummary, err error) {
	started := time.Now()
	sum.SkippedIneligible = map[string]int64{}
	sum.LedgerPath = opts.LedgerPath
	sum.ResidualTOCTOU = opts.ProviderUnconditionalHeadGuard
	defer func() {
		sum.SavedUSDPerMonth = r2RetireUSD(sum.RetiredBytes)
		sum.ElapsedSec = time.Since(started).Seconds()
		var a errR2RetireAnomaly
		if errors.As(err, &a) {
			sum.Anomaly = a.msg
		}
	}()
	if opts.ProviderUnconditionalHeadGuard && strings.TrimSpace(opts.AcceptOperatorCredentialRisk) == "" {
		return sum, errors.New("HEAD-guard mode requires --accept-operator-credential-risk with an owner decision reference")
	}
	if !opts.ProviderUnconditionalHeadGuard && opts.AcceptOperatorCredentialRisk != "" {
		return sum, errors.New("--accept-operator-credential-risk requires HEAD-guard mode")
	}
	if opts.ProviderUnconditionalHeadGuard && opts.MinObjectAge < 7*24*time.Hour {
		return sum, errors.New("HEAD-guard mode requires --min-object-age >= 168h")
	}
	if opts.MaxBytes <= 0 || opts.Budget24h <= 0 {
		return sum, errors.New("--max-bytes and --budget-24h-bytes must be positive")
	}
	if !r2RetireLowerHex64(opts.ExpectedSHA) {
		return sum, errors.New("--expected-sha256 must be the 64-char lowercase manifest digest")
	}
	h, items, digest, err := readR2RetireManifest(opts.ManifestPath)
	sum.ManifestSHA256 = digest
	if digest != opts.ExpectedSHA {
		return sum, fmt.Errorf("manifest digest %s does not match --expected-sha256", digest)
	}
	if err != nil {
		return sum, err
	}
	if h.Class == r2RetireDeliveryOnly && !opts.ProviderUnconditionalHeadGuard {
		return sum, errors.New("owner_r2_delivery_only requires HEAD-guard mode")
	}
	if opts.Delivery.SHA256 != h.DeliveryKeepSHA256 {
		return sum, errors.New("delivery policy changed or unchecked; re-plan")
	}
	if err := api.RequireDeliveryKeep(ctx, pool, opts.Delivery); err != nil {
		return sum, err
	}
	for _, it := range items {
		if err := r2RetireDeliveryHeld(ctx, pool, it.Bucket, it.ObjectKey); err != nil {
			return sum, err
		}
	}
	if h.Class == r2RetireDeliveryOnly {
		if err := r2RetireRequireDeliveryApplySchema(ctx, pool); err != nil {
			return sum, err
		}
	}
	sum.ManifestItems = int64(len(items))
	if opts.ProviderUnconditionalHeadGuard {
		for _, it := range items {
			if it.Kind != "raw" {
				return sum, errors.New("HEAD-guard mode requires catalog ETags; re-plan with --head-guard-compatible to exclude collated outputs")
			}
		}
		catalogETags, err := r2RetireCatalogETags(ctx, pool, h.AccountID, h.Bucket)
		if err != nil {
			return sum, err
		}
		for _, it := range items {
			if reason := r2RetireGuardCatalogReason(it.r2RetireCandidate, catalogETags); reason != "" {
				return sum, fmt.Errorf("HEAD-guard catalog preflight: %s; re-plan", reason)
			}
		}
		if !h.NASBacked {
			return sum, errors.New("HEAD-guard mode requires a NAS-backed manifest with planned ETags")
		}
		if _, ok := store.(r2RetireSingleDeleteStore); !ok {
			return sum, errors.New("HEAD-guard mode requires a single-attempt DELETE transport")
		}
		for _, it := range items {
			if err := r2RetireCheckUngatedPrefix(it.ObjectKey); err != nil {
				return sum, err
			}
		}
		if err := r2RetireCheckWriterFence(ctx, pool, h.Bucket, store.EndpointSHA256()); err != nil {
			return sum, err
		}
		if err := r2RetireCheckMultipart(ctx, store); err != nil {
			return sum, err
		}
	}
	switch {
	case h.Class.owner() && opts.Budget24h > 5e12:
		return sum, errors.New("owner-accepted retirement cannot exceed 5 TB/24h")
	case h.NASBacked && (h.ConnectionID != 13 || h.AccountID != 47):
		return sum, errors.New("NAS-backed retirement requires account 47 connection 13")
	case h.PolicySHA256 != policySHA:
		return sum, errors.New("manifest was planned under a different keep28 policy")
	case h.Bucket != store.Bucket():
		return sum, fmt.Errorf("manifest bucket %q is not the configured bucket %q", h.Bucket, store.Bucket())
	case opts.MaxManifestAge <= 0 || opts.MaxManifestAge > r2RetireMaxManifestAge:
		return sum, fmt.Errorf("--max-manifest-age must be in (0, %s]", r2RetireMaxManifestAge)
	case time.Since(h.CreatedAt) > opts.MaxManifestAge:
		return sum, fmt.Errorf("manifest is older than %s; re-plan", opts.MaxManifestAge)
	case h.GraceSeconds < int64((24*time.Hour)/time.Second):
		return sum, errors.New("manifest grace is below 24h")
	}
	picksPath := opts.PicksPath
	if picksPath == "" {
		picksPath = h.PicksPath
	}
	picks, err := loadR2RetirePicks(picksPath)
	if err != nil {
		return sum, err
	}
	// Pick unchanged: every stream in the manifest must still resolve to the
	// exact choice it was planned with.
	for _, it := range items {
		w, ok := windows[it.StreamID]
		if !ok {
			return sum, r2RetireAnomalyf("stream %d has no keep window", it.StreamID)
		}
		p, has := picks.Choices[it.StreamID]
		if got := func() string {
			if h.Class == r2RetireDeliveryOnly {
				return r2RetireDeliveryChoices(picks.Choices, windows)[fmt.Sprint(it.StreamID)]
			}
			return r2RetireEffectiveChoice(p, has, w)
		}(); got != it.Choice {
			return sum, r2RetireAnomalyf("stream %d pick changed from %q to %q since the plan; re-plan", it.StreamID, it.Choice, got)
		}
	}
	has, err := r2RetireHasRetirementsTable(ctx, pool)
	if err != nil {
		return sum, err
	}
	if !has {
		return sum, errors.New("r2_object_retirements is missing (migration 0174 not applied); refusing")
	}
	if ids, err := r2RetireEnabledPlans(ctx, pool); err != nil {
		return sum, fmt.Errorf("check NAS cleanup plans: %w", err)
	} else if len(ids) > 0 && !h.NASBacked {
		return sum, fmt.Errorf("refusing to run while NAS cleanup plan(s) %v are enabled; pause them first", ids)
	}
	lockConn, err := pool.Acquire(ctx)
	if err != nil {
		return sum, err
	}
	defer lockConn.Release()
	var locked bool
	if err := lockConn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, r2RetireAdvisoryLockKey).Scan(&locked); err != nil {
		return sum, err
	}
	if !locked {
		return sum, errors.New("another r2-retire apply holds the database lock")
	}
	defer func() {
		_, _ = lockConn.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, r2RetireAdvisoryLockKey)
	}()
	if h.NASBacked {
		if err := checkR2RetireDeleteProofMode(opts.DeleteProofPath, opts.DeleteProofSHA256, h.Bucket, store.EndpointSHA256(), opts.ProviderUnconditionalHeadGuard); err != nil {
			return sum, err
		}
		var has bool
		if err := pool.QueryRow(ctx, `SELECT to_regclass('r2_retirement_reservations') IS NOT NULL`).Scan(&has); err != nil {
			return sum, err
		}
		if !has {
			return sum, errors.New("migration 0182 is required before NAS-backed apply")
		}
	}
	if opts.ProviderUnconditionalHeadGuard {
		if err := r2RetireCheckWriterFence(ctx, pool, h.Bucket, store.EndpointSHA256()); err != nil {
			return sum, err
		}
		if err := r2RetireCheckMultipart(ctx, store); err != nil {
			return sum, err
		}
	}

	ledger, err := openR2RetireLedger(opts.LedgerPath)
	if err != nil {
		return sum, err
	}
	defer ledger.Close()
	if opts.ProviderUnconditionalHeadGuard {
		sum.OperatorCredentialRisk = &r2RetireOperatorAcknowledgment{DecisionReference: opts.AcceptOperatorCredentialRisk, At: time.Now().UTC()}
		ledger.operatorCredentialRisk = sum.OperatorCredentialRisk
		if err := ledger.write(r2RetireLedgerLine{Event: "operator_credential_risk_accepted", ManifestSHA256: digest, Detail: "operator/release credentials only; no footage writes during run; residual_toctou=true"}); err != nil {
			return sum, err
		}
	}
	a := &r2RetireApplier{pool: pool, store: store, opts: opts, header: h, digest: digest, picks: picks, windows: windows, policySH: policySHA, ledger: ledger,
		scope: r2RetireScope{Class: h.Class, OwnerPolicy: func() []byte {
			if h.Class == r2RetireDeliveryOnly {
				return r2RetireDeliveryPolicy(windows, picks.Choices)
			}
			return r2RetireOwnerPolicy(windows, picks.Choices)
		}(), AccountID: h.AccountID, ConnectionID: h.ConnectionID, Bucket: h.Bucket, Grace: time.Duration(h.GraceSeconds) * time.Second, HasRetirements: true, NASBacked: h.NASBacked}}
	if a.sets, err = loadR2RetireSets(ctx, pool, digest, h.NASBacked, h.Class); err != nil {
		return sum, err
	}
	if h.Class.owner() && !a.sets.OwnerSchema {
		return sum, errors.New("migration 0186 is required before owner-accepted apply")
	}
	if a.fp, err = r2RetireFingerprint(ctx, pool); err != nil {
		return sum, err
	}
	if a.scope.NASBacked {
		a.fp = ""
	} // First item reloads sets under the cleanup fences.
	if err := r2RetireUsedBudget(ctx, pool, &sum.Used24hBefore); err != nil {
		return sum, err
	}
	done := map[string]bool{}
	rows, err := pool.Query(ctx, `SELECT kind||':'||source_id FROM r2_object_retirements WHERE manifest_sha256=$1`, digest)
	if err != nil {
		return sum, err
	}
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			rows.Close()
			return sum, err
		}
		done[k] = true
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return sum, err
	}
	if err := ledger.write(r2RetireLedgerLine{Event: "run_start", ManifestSHA256: digest,
		Detail: fmt.Sprintf("items=%d max_bytes=%d budget_24h=%d used_24h=%d head_guard=%t residual_toctou=%t min_object_age=%s delete_proof_sha256=%s", len(items), opts.MaxBytes, opts.Budget24h, sum.Used24hBefore, opts.ProviderUnconditionalHeadGuard, opts.ProviderUnconditionalHeadGuard, opts.MinObjectAge, opts.DeleteProofSHA256)}); err != nil {
		return sum, err
	}
	defer func() {
		detail := fmt.Sprintf("retired=%d bytes=%d already_done=%d limit=%s interrupted=%t", sum.Retired, sum.RetiredBytes, sum.AlreadyDone, sum.LimitReached, sum.Interrupted)
		if err != nil {
			detail += " error=" + err.Error()
		}
		_ = ledger.write(r2RetireLedgerLine{Event: "run_end", ManifestSHA256: digest, Detail: detail})
	}()
	var run int64
	for _, it := range items {
		if ctx.Err() != nil {
			sum.Interrupted = true
			return sum, nil
		}
		if done[it.Kind+":"+strconv.FormatInt(it.SourceID, 10)] {
			sum.AlreadyDone++
			continue
		}
		switch {
		case opts.MaxItems > 0 && sum.Retired >= opts.MaxItems:
			sum.LimitReached = "max_items"
		case run+it.SizeBytes > opts.MaxBytes:
			sum.LimitReached = "max_bytes"
		case sum.Used24hBefore+run+it.SizeBytes > opts.Budget24h:
			sum.LimitReached = "budget_24h"
		}
		if sum.LimitReached != "" {
			return sum, nil
		}
		itemStarted := time.Now()
		outcome, detail, err := a.applyOne(ctx, it)
		if err != nil {
			if ctx.Err() != nil && !errors.As(err, new(errR2RetireAnomaly)) {
				sum.Interrupted = true
				return sum, nil
			}
			return sum, fmt.Errorf("%s %d (%s): %w", it.Kind, it.SourceID, it.ObjectKey, err)
		}
		switch outcome {
		case "already_done":
			sum.AlreadyDone++
		case "ineligible":
			if !opts.SkipIneligible {
				return sum, r2RetireAnomalyf("%s %d (%s) is no longer eligible: %s (re-plan, or pass --skip-ineligible)", it.Kind, it.SourceID, it.ObjectKey, detail)
			}
			sum.SkippedIneligible[detail]++
			_ = ledger.write(r2RetireLedgerLine{Event: "skip", ManifestSHA256: digest, Kind: it.Kind, SourceID: it.SourceID, ObjectKey: it.ObjectKey, Detail: detail})
		case "retired", "retired_absent":
			sum.Retired++
			sum.RetiredBytes += it.SizeBytes
			run += it.SizeBytes
			if outcome == "retired_absent" {
				sum.RetiredAbsent++
			}
		}
		if opts.Rate > 0 {
			if min := time.Duration(float64(time.Second) / opts.Rate); time.Since(itemStarted) < min {
				select {
				case <-ctx.Done():
				case <-time.After(min - time.Since(itemStarted)):
				}
			}
		}
	}
	return sum, nil
}

// Charge all unresolved intents indefinitely: a successful DELETE followed by
// a failed DB commit must not free budget on restart or in a different manifest.
// Resolved reservations are charged only by their receipt's retirement time.
func r2RetireUsedBudget(ctx context.Context, q r2RetireQuerier, used *int64) error {
	var hasReservations bool
	if err := q.QueryRow(ctx, `SELECT to_regclass('r2_retirement_reservations') IS NOT NULL`).Scan(&hasReservations); err != nil {
		return err
	}
	sql := `SELECT coalesce(sum(size_bytes),0) FROM r2_object_retirements WHERE retired_at>now()-interval '24 hours'`
	if hasReservations {
		sql = `SELECT (` + sql + `) + coalesce((SELECT sum(x.size_bytes) FROM r2_retirement_reservations x
 WHERE NOT EXISTS(SELECT 1 FROM r2_object_retirements r
   WHERE (r.kind=x.kind AND r.source_id=x.source_id) OR (r.bucket=x.bucket AND r.object_key=x.object_key))),0)`
	}
	return q.QueryRow(ctx, sql).Scan(used)
}

// applyOne re-validates and retires exactly one object. Returned outcomes:
// retired, retired_absent, already_done, ineligible (detail = reason). Any
// error stops the run.
func (a *r2RetireApplier) applyOne(ctx context.Context, it r2RetireItem) (string, string, error) {
	tx, err := a.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return "", "", err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if a.opts.ProviderUnconditionalHeadGuard {
		if err := r2RetireLockStoppedWriters(ctx, tx, a.header.Bucket); err != nil {
			return "", "", err
		}
	}
	{
		// Admission takes this fence before catalog locks. Public fencing also
		// excludes new activation/references while this exact R2 delete finishes.
		if err := api.LockNASCleanupR2Retirement(ctx, tx); err != nil {
			return "", "", err
		}
	}
	if err := r2RetireDeliveryHeld(ctx, tx, it.Bucket, it.ObjectKey); err != nil {
		return "", "", err
	}
	if err := r2RetireDeliveryRegistryUnchanged(ctx, a.store, a.opts.Delivery); err != nil {
		return "", "", err
	}
	if ids, err := r2RetireEnabledPlans(ctx, tx); err != nil {
		return "", "", err
	} else if len(ids) > 0 && !a.scope.NASBacked {
		return "", "", r2RetireAnomalyf("NAS cleanup plan(s) %v became enabled", ids)
	}
	if fp, err := r2RetireFingerprint(ctx, tx); err != nil {
		return "", "", err
	} else if fp != a.fp {
		if !a.scope.NASBacked {
			return "", "", r2RetireAnomalyf("NAS cleanup plans, joined artifacts or stitched outputs changed during the run; re-plan")
		}
		a.sets, err = loadR2RetireSets(ctx, tx, a.digest, a.scope.NASBacked, a.scope.Class)
		if err != nil {
			return "", "", err
		}
		a.fp = fp
	}
	// Row lock: serializes against concurrent purged_at/identity changes and
	// against inserts whose foreign keys reference this row (e.g. NAS cleanup
	// holds). It does NOT block new NAS inventory rows (nas_inventory_files has
	// no foreign key to recording_clips), so NAS presence is re-checked again
	// immediately before the delete.
	table := "recording_clips"
	if it.Kind == "collated" {
		table = "recording_collation_outputs"
	}
	var id int64
	if err := tx.QueryRow(ctx, `SELECT id FROM `+table+` WHERE id=$1 FOR UPDATE`, it.SourceID).Scan(&id); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", "", r2RetireAnomalyf("catalog row is gone")
		}
		return "", "", err
	}
	cur, err := r2RetireLoadOne(ctx, tx, it.Kind, it.SourceID, a.scope, a.sets)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", "", r2RetireAnomalyf("catalog row left the account scope")
		}
		return "", "", err
	}
	if cur.Facts.Retired {
		return "already_done", "", nil
	}
	if cur.RecordingID != it.RecordingID || cur.StreamID != it.StreamID || cur.Bucket != it.Bucket || cur.ObjectKey != it.ObjectKey ||
		cur.SizeBytes != it.SizeBytes || cur.SHA256 != it.SHA256 || (a.scope.NASBacked && cur.RelativePath != it.RelativePath) || !cur.StartAt.Equal(it.StartAt) || !cur.EndAt.Equal(it.EndAt) {
		return "", "", r2RetireAnomalyf("catalog identity changed since the plan")
	}
	picks := a.picks
	if a.scope.NASBacked {
		picks, err = loadR2RetirePicks(a.picks.Path)
		if err != nil {
			return "", "", err
		}
		if picks.SHA256 != a.header.PicksSHA256 {
			return "", "", r2RetireAnomalyf("picks changed; re-plan")
		}
	}
	reason, choice, ok := r2RetireDecideForScope(cur, a.scope, a.windows, picks.Choices)
	if !ok {
		return "ineligible", reason, nil
	}
	if choice != it.Choice {
		return "ineligible", "pick_changed", nil
	}
	head, herr := a.store.Head(ctx, it.ObjectKey)
	absent := false
	switch {
	case herr != nil && r2.IsNotFound(herr):
		if !a.ledger.intents[it.ObjectKey] && !a.opts.RecordAbsent {
			return "", "", r2RetireAnomalyf("object is already missing from R2 and the ledger has no delete intent for it (pass --record-already-absent to mark such rows)")
		}
		absent = true
	case herr != nil:
		return "", "", fmt.Errorf("head: %w", herr)
	case a.scope.NASBacked && strings.Trim(head.ETag, `"`) != strings.Trim(it.ETag, `"`):
		return "", "", r2RetireAnomalyf("R2 ETag changed since plan")
	case head.SizeBytes != it.SizeBytes:
		return "", "", r2RetireAnomalyf("R2 size %d differs from catalog size %d", head.SizeBytes, it.SizeBytes)
	case head.SHA256Metadata != "" && !strings.EqualFold(head.SHA256Metadata, it.SHA256):
		return "", "", r2RetireAnomalyf("R2 sha256 metadata differs from the catalog")
	}
	if err := r2RetireDeliveryHeld(ctx, tx, it.Bucket, it.ObjectKey); err != nil {
		return "", "", err
	}
	if err := r2RetireDeliveryRegistryUnchanged(ctx, a.store, a.opts.Delivery); err != nil {
		return "", "", err
	}
	if a.scope.NASBacked {
		// HEAD may wait on the network; an owner pick changed during that
		// interval revokes this item before any intent or lifecycle write.
		latest, err := loadR2RetirePicks(a.picks.Path)
		if err != nil {
			return "", "", err
		}
		if latest.SHA256 != a.header.PicksSHA256 {
			return "", "", r2RetireAnomalyf("picks changed during HEAD; re-plan")
		}
	}

	if a.scope.Class.owner() {
		if err := a.reserve(ctx, it); err != nil {
			return "", "", err
		}
	}
	if _, err := tx.Exec(ctx, `INSERT INTO r2_object_retirements(kind,source_id,stream_id,bucket,object_key,size_bytes,sha256,reason,choice,manifest_sha256,policy_sha256,already_absent)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
		it.Kind, it.SourceID, it.StreamID, it.Bucket, it.ObjectKey, it.SizeBytes, it.SHA256, it.Reason, it.Choice, a.digest, a.policySH, absent); err != nil {
		return "", "", r2RetireAnomalyf("record retirement: %v", err)
	}
	if it.Kind == "raw" {
		// The NAS cleanup hold and joined retention triggers veto here, before
		// anything is deleted.
		tag, err := tx.Exec(ctx, `UPDATE recording_clips SET purged_at=now() WHERE id=$1 AND purged_at IS NULL`, it.SourceID)
		if err != nil {
			return "", "", r2RetireAnomalyf("database refused purged_at: %v", err)
		}
		if tag.RowsAffected() != 1 {
			return "", "", r2RetireAnomalyf("purged_at updated %d rows", tag.RowsAffected())
		}
	}
	if a.scope.NASBacked {
		// Lock the actual matching inventory row through R2 deletion/commit.
		// Inventory can no longer transition this proof to missing mid-delete.
		if reason == r2RetireNASReason {
			if err := r2RetireLockNASIdentity(ctx, tx, it, a.scope.ConnectionID); err != nil {
				return "", "", err
			}
		}
		latest, err := r2RetireLoadOne(ctx, tx, it.Kind, it.SourceID, a.scope, a.sets)
		if err != nil {
			return "", "", err
		}
		latest.Facts.Purged = false // Our uncommitted writes above.
		latest.Facts.Retired = false
		r, ch, eligible := r2RetireDecideForScope(latest, a.scope, a.windows, picks.Choices)
		if !eligible || ch != it.Choice || r != it.Reason {
			return "ineligible", r, nil
		}
	} else if nas, err := r2RetireNASCopyNow(ctx, tx, it.Kind, it.SourceID, a.scope.ConnectionID); err != nil {
		return "", "", err
	} else if nas {
		return "ineligible", "nas_copy", nil
	}

	if a.scope.NASBacked {
		// Commit intent on another connection BEFORE touching R2. The admission
		// and public fences remain held by tx across this independent commit.
		if err := a.reserve(ctx, it); err != nil {
			return "", "", err
		}
	}

	line := r2RetireLedgerLine{ManifestSHA256: a.digest, Kind: it.Kind, SourceID: it.SourceID, StreamID: it.StreamID, ObjectKey: it.ObjectKey, SizeBytes: it.SizeBytes, SHA256: it.SHA256}
	intent := line
	intent.Event = "intent"
	if absent {
		intent.Detail = "already_absent"
	}
	if err := a.ledger.write(intent); err != nil {
		return "", "", err
	}
	// Past this point the delete and the commit finish together.
	finishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), r2RetireFinishTimeout)
	defer cancel()
	fail := func(event string, err error) (string, string, error) {
		e := line
		e.Event, e.Detail = event, err.Error()
		_ = a.ledger.write(e)
		return "", "", r2RetireAnomalyf("%s: %v", event, err)
	}
	if !absent {
		var deleteErr error
		if a.opts.ProviderUnconditionalHeadGuard {
			if err := a.headGuard(finishCtx, tx, it); err != nil {
				return fail("head_guard_refused", err)
			}
			deleteErr = a.store.(r2RetireSingleDeleteStore).DeleteObjectOnce(finishCtx, it.ObjectKey)
		} else if a.scope.NASBacked {
			exact, ok := a.store.(interface {
				DeleteObjectExact(context.Context, string, string) error
			})
			if !ok {
				return fail("delete_error", errors.New("conditional delete is required"))
			}
			deleteErr = exact.DeleteObjectExact(finishCtx, it.ObjectKey, it.ETag)
		} else {
			deleteErr = a.store.DeleteObject(finishCtx, it.ObjectKey)
		}
		if err := deleteErr; err != nil {
			return fail("delete_error", err)
		}
		if _, err := a.store.Head(finishCtx, it.ObjectKey); !r2.IsNotFound(err) {
			if err == nil {
				err = errors.New("object still present after delete")
			}
			return fail("delete_unconfirmed", err)
		}
	}
	if err := tx.Commit(finishCtx); err != nil {
		return fail("commit_failed", fmt.Errorf("%w (the object is deleted; a rerun records it)", err))
	}
	done := line
	done.Event = "done"
	if absent {
		done.Detail = "already_absent"
	}
	if err := a.ledger.write(done); err != nil {
		return "", "", err
	}
	if absent {
		return "retired_absent", "", nil
	}
	return "retired", "", nil
}

// r2RetireNASCopyNow re-reads NAS evidence for one object inside the apply
// transaction (same evidence as the plan facts).
func r2RetireNASCopyNow(ctx context.Context, q r2RetireQuerier, kind string, id, connectionID int64) (bool, error) {
	sql := `SELECT EXISTS(SELECT 1 FROM nas_inventory_files n WHERE n.clip_id=c.id AND n.state<>'missing')
  OR EXISTS(SELECT 1 FROM nas_inventory_files n WHERE n.connection_id=$2 AND n.relative_path=c.display_path AND n.state<>'missing')
  OR EXISTS(SELECT 1 FROM nas_inventory_unmatched_files u WHERE u.connection_id=$2 AND u.relative_path=c.display_path AND u.state<>'missing')
FROM recording_clips c WHERE c.id=$1`
	if kind == "collated" {
		sql = `SELECT EXISTS(SELECT 1 FROM recording_collation_output_acks a WHERE a.output_id=o.id)
  OR EXISTS(SELECT 1 FROM nas_inventory_files n WHERE n.connection_id=$2
     AND n.relative_path IN (o.nas_relative_path, 'joined/'||o.nas_relative_path) AND n.state<>'missing')
   OR EXISTS(SELECT 1 FROM nas_inventory_unmatched_files u WHERE u.connection_id=$2
    AND u.relative_path IN (o.nas_relative_path, 'joined/'||o.nas_relative_path) AND u.state<>'missing')
FROM recording_collation_outputs o WHERE o.id=$1`
	}
	var nas bool
	if err := q.QueryRow(ctx, sql, id, connectionID).Scan(&nas); err != nil {
		return false, fmt.Errorf("re-check NAS presence: %w", err)
	}
	return nas, nil
}

// ---------------------------------------------------------------- CLI

func r2RetireOpenPool(ctx context.Context, cfg config.Config, readOnly bool) (*pgxpool.Pool, error) {
	poolCfg, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		return nil, errors.New("invalid database configuration")
	}
	poolCfg.MaxConns = 3
	poolCfg.MinConns = 0
	rp := poolCfg.ConnConfig.RuntimeParams
	rp["application_name"] = r2RetireAppName
	rp["idle_in_transaction_session_timeout"] = "300s"
	if readOnly {
		rp["default_transaction_read_only"] = "on"
		rp["statement_timeout"] = "1800s"
	} else {
		rp["statement_timeout"] = "120s"
		rp["lock_timeout"] = "5s"
	}
	return pgxpool.NewWithConfig(ctx, poolCfg)
}

func r2RetireDefaultLedger() string {
	home, err := os.UserHomeDir()
	if err != nil {
		home = "."
	}
	return filepath.Join(home, ".stoarama", "r2-retire", "ledger.jsonl")
}

func runR2Retire(ctx context.Context, cfg config.Config, args []string) {
	const usage = `usage:
  stoaramactl r2-retire writer-fence --activate --all-writers-gated
  stoaramactl r2-retire writer-fence --deactivate
  stoaramactl r2-retire delivery-fence --activate --all-api-instances-gated
  stoaramactl r2-retire delivery-fence --settle-drain-acknowledgment EVIDENCE
  stoaramactl r2-retire plan --picks FILE --out DIR [--sample 200 --grace 168h --connection-id N --page-size 5000 --json]
  stoaramactl r2-retire apply --manifest FILE --expected-sha256 SHA --max-bytes N [--budget-24h-bytes N --picks FILE
      --ledger PATH --rate 10 --max-items N --max-manifest-age 72h --skip-ineligible --record-already-absent --json]`
	if len(args) < 1 {
		log.Fatal(usage)
	}
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()
	policySHA, windows, err := api.Keep28Policy()
	if err != nil {
		log.Fatalf("r2-retire: keep28 policy unavailable (keep everything): %v", err)
	}
	switch args[0] {
	case "writer-fence":
		runR2WriterFence(ctx, cfg, args[1:])
	case "delivery-fence":
		runR2DeliveryFence(ctx, cfg, args[1:])
	case "release-holds":
		runR2HoldReleases(ctx, cfg, args[1:], policySHA, windows)
	case "plan":
		runR2RetirePlan(ctx, cfg, args[1:], usage, policySHA, windows)
	case "apply":
		runR2RetireApply(ctx, cfg, args[1:], usage, policySHA, windows)
	default:
		log.Fatal(usage)
	}
}

func runR2RetirePlan(ctx context.Context, cfg config.Config, args []string, usage, policySHA string, windows map[int64]api.Keep28Window) {
	fs := flag.NewFlagSet("r2-retire plan", flag.ContinueOnError)
	class := fs.String("class", "", "retirement class: owner_accepted_outside_window or owner_r2_delivery_only")
	deliveryPreview := fs.String("delivery-keep-preview", "", "exact keep JSONL for NONEXECUTABLE pre-migration preview")
	deliveryPreviewSHA := fs.String("expected-delivery-keep-sha256", "", "digest of delivery keep preview")
	picksPath := fs.String("picks", "", "owner's R2 picks (r2-picks.json)")
	releasePreview := fs.String("hold-release-preview", "", "dry-run hold-release manifest for non-executable projections")
	releasePreviewSHA := fs.String("expected-hold-release-sha256", "", "hold-release preview digest")
	out := fs.String("out", "", "output directory (manifest.jsonl, summary.json)")
	sample := fs.Int("sample", 200, "HEAD this many random manifest items (0 = none)")
	listingPath := fs.String("r2-listing", "", "reuse a complete checksummed R2 listing no older than 2 hours")
	listingSHA := fs.String("expected-listing-sha256", "", "SHA-256 of --r2-listing")
	headGuardCompatible := fs.Bool("head-guard-compatible", false, "plan only raw objects with matching catalog/listing ETags; still requires reviewed apply proofs")
	nasBacked := fs.Bool("nas-backed", false, "plan NAS-backed kept footage and phased manifests using a live R2 listing")
	grace := fs.Duration("grace", 7*24*time.Hour, "keep objects that ended or were registered within this age")
	connectionID := fs.Int64("connection-id", 0, "NAS pull connection (default: the only one)")
	pageSize := fs.Int("page-size", 5000, "rows per catalog page")
	asJSON := fs.Bool("json", false, "print the summary as JSON")
	if err := fs.Parse(args); err != nil || fs.NArg() != 0 || *picksPath == "" || *out == "" {
		log.Fatal(usage)
	}
	retirementClass := r2RetireClass(*class)
	if !retirementClass.valid() {
		log.Fatal("unknown retirement class")
	}
	if retirementClass.owner() || *headGuardCompatible {
		*nasBacked = true
	}
	if *grace < 24*time.Hour || *pageSize < 1 || *pageSize > 20000 || *sample < 0 {
		log.Fatal("r2-retire plan: --grace must be >= 24h, --page-size 1..20000, --sample >= 0")
	}
	picks, err := loadR2RetirePicks(*picksPath)
	if err != nil {
		log.Fatalf("r2-retire plan: %v", err)
	}
	if abs, err := filepath.Abs(*picksPath); err == nil {
		picks.Path = abs
	}
	if err := cfg.ValidateR2(); err != nil {
		log.Fatalf("r2-retire plan: R2 config required: %v", err)
	}
	if retirementClass == r2RetireDeliveryOnly && *listingPath != "" {
		if err := r2RetireRequireWholeListing(*listingPath); err != nil {
			log.Fatal(err)
		}
	}
	if (*listingPath != "" || *listingSHA != "") && !*nasBacked {
		log.Fatal("--r2-listing requires --nas-backed")
	}
	if *nasBacked {
		err = os.Mkdir(*out, 0700)
	} else {
		err = os.MkdirAll(*out, 0700)
	}
	if err != nil {
		log.Fatal(err)
	}
	pool, err := r2RetireOpenPool(ctx, cfg, true)
	if err != nil {
		log.Fatalf("connect database: %v", err)
	}
	defer pool.Close()
	conn, err := resolveNASPurgeConnection(ctx, pool, *connectionID)
	if err != nil {
		log.Fatal(err)
	}
	var accountID int64
	if err := pool.QueryRow(ctx, `SELECT account_id FROM connections WHERE id=$1`, conn).Scan(&accountID); err != nil {
		log.Fatal(err)
	}
	if *nasBacked && (conn != 13 || accountID != 47) {
		log.Fatal("NAS-backed retirement requires account 47 connection 13")
	}
	hasRet, err := r2RetireHasRetirementsTable(ctx, pool)
	if err != nil {
		log.Fatal(err)
	}
	if !hasRet {
		log.Printf("r2-retire plan: r2_object_retirements not found (migration 0174 not applied); planning without it")
	}
	enabled, err := r2RetireEnabledPlans(ctx, pool)
	if err != nil {
		log.Fatal(err)
	}
	opts := r2RetirePlanOptions{HeadGuardCompatible: *headGuardCompatible, Picks: picks, PageSize: *pageSize, Now: time.Now(),
		Scope: r2RetireScope{Class: retirementClass, AccountID: accountID, ConnectionID: conn, Bucket: cfg.R2Bucket, Grace: *grace, HasRetirements: hasRet, NASBacked: *nasBacked}}
	opts.Delivery, err = api.ReadPublicDeliveryKeep(ctx, mustArchiveR2Client(ctx, cfg, pool))
	if err != nil {
		log.Fatal(err)
	}
	if *deliveryPreview != "" {
		if err := r2RetireDeliveryPreview(*deliveryPreview, *deliveryPreviewSHA, opts.Delivery); err != nil {
			log.Fatal(err)
		}
		opts.DeliveryPreview = true
	}
	if *nasBacked {
		log.Print("r2-retire: listing catalog folder/version prefixes in R2 (read-only, 8 workers)")
		if *listingPath != "" {
			opts.LiveObjects, err = readR2RetireListing(*listingPath, *listingSHA, cfg.R2Bucket)
		} else {
			var prefixes []string
			if retirementClass != r2RetireDeliveryOnly {
				prefixes, err = r2RetirePrefixes(ctx, pool, accountID, cfg.R2Bucket)
				if err != nil {
					log.Fatal(err)
				}
			}
			store := mustArchiveR2Client(ctx, cfg, pool)
			var rootObjects []r2.ObjectInfo
			if retirementClass == r2RetireDeliveryOnly {
				prefixes, rootObjects, err = store.ListRoots(ctx)
				if err != nil {
					log.Fatal(err)
				}
			}
			opts.LiveObjects, err = r2RetireLiveObjects(ctx, store, prefixes)
			for _, o := range rootObjects {
				opts.LiveObjects[o.Key] = o
			}
			if retirementClass == r2RetireDeliveryOnly {
				prefixes = []string{""}
			}

			if err == nil {
				*listingSHA, err = writeR2RetireListing(filepath.Join(*out, "r2-listing.jsonl"), cfg.R2Bucket, prefixes, opts.LiveObjects)
			}
		}
		if err != nil {
			log.Fatal(err)
		}
	}
	if *nasBacked {
		log.Printf("r2-retire: live listing complete (%d objects); scanning catalog", len(opts.LiveObjects))
	}
	if *releasePreview != "" {
		if !*nasBacked {
			log.Fatal("hold-release preview requires --nas-backed")
		}
		opts.Projection, err = loadR2RetireProjection(ctx, pool, *releasePreview, *releasePreviewSHA, picks, policySHA)
		if err != nil {
			log.Fatal(err)
		}
		live, releaseErr := r2HoldReleasePlan(ctx, pool, picks, policySHA, windows, retirementClass)
		if releaseErr != nil {
			log.Fatal(releaseErr)
		}
		data, readErr := os.ReadFile(*releasePreview)
		if readErr != nil {
			log.Fatal(readErr)
		}
		var preview r2HoldReleaseManifest
		if json.Unmarshal(data, &preview) != nil {
			log.Fatal("invalid preview")
		}
		if err := validateR2HoldReleaseItems(preview, live); err != nil {
			log.Fatal(err)
		}
	}
	sum, items, err := r2RetirePlanRun(ctx, pool, opts, policySHA, windows)
	if err != nil {
		log.Fatalf("r2-retire plan: %v", err)
	}
	sum.Header.R2ListingSHA256 = *listingSHA
	if *sample > 0 {
		store := mustArchiveR2Client(ctx, cfg, pool)
		hs := r2RetireHeadSampleRun(ctx, store, items, *sample, 8, time.Now().UnixNano())
		sum.HeadSample = &hs
	}
	if sum.Phases != nil {
		if err := pool.QueryRow(ctx, `SELECT coalesce(nas_storage_free_bytes,0),nas_storage_reported_at FROM connections WHERE id=$1`, conn).Scan(&sum.Phases.NASFreeBytes, &sum.Phases.NASFreeReportedAt); err != nil {
			log.Fatal(err)
		}
		sum.Phases.CopyFits = sum.Phases.NASFreeReportedAt != nil && time.Since(*sum.Phases.NASFreeReportedAt) <= 5*time.Minute && sum.Phases.NASFreeBytes >= sum.Phases.Totals[r2RetireCopy].Bytes+sum.Phases.NASReserveBytes
		if err := sum.Phases.write(*out, sum.Header); err != nil {
			log.Fatal(err)
		}
		sum.Header.Phase = r2RetireNow
	}
	if sum.Projection != nil {
		if err := sum.Projection.write(*out, sum.Header); err != nil {
			log.Fatal(err)
		}
	}
	sum.ManifestPath = filepath.Join(*out, r2RetireManifestName)
	if sum.ManifestSHA256, err = writeR2RetireManifest(sum.ManifestPath, sum.Header, items); err != nil {
		log.Fatalf("write manifest: %v", err)
	}
	b, _ := json.MarshalIndent(sum, "", "  ")
	if err := os.WriteFile(filepath.Join(*out, "summary.json"), append(b, '\n'), 0o600); err != nil {
		log.Fatal(err)
	}
	if *asJSON {
		fmt.Println(string(b))
	} else {
		printR2RetirePlan(os.Stdout, sum)
	}
	if len(enabled) > 0 && !*nasBacked {
		fmt.Printf("WARNING: NAS cleanup plan(s) %v are enabled; apply will refuse until they are paused\n", enabled)
	}
}

func printR2RetirePlan(w io.Writer, sum r2RetirePlanSummary) {
	tb := func(n int64) string { return fmt.Sprintf("%.3f TB", float64(n)/r2RetireBytesPerTB) }
	fmt.Fprintf(w, "r2-retire plan (read-only)  policy=%s  picks=%s (sha %s, saved %s)\n", sum.Header.PolicySHA256[:12], sum.Header.PicksPath, sum.Header.PicksSHA256[:12], sum.Header.PicksSavedAt)
	for _, kind := range []string{"raw", "collated"} {
		fmt.Fprintf(w, "scanned %-8s %9d objects %s\n", kind, sum.Scanned[kind].Objects, tb(sum.Scanned[kind].Bytes))
	}
	fmt.Fprintf(w, "SELECTED: %d objects, %s  (raw %d / %s, stitched %d / %s)  saves $%.2f/month at $%.0f/TB\n",
		sum.Selected.Objects, tb(sum.Selected.Bytes), sum.SelectedByKind["raw"].Objects, tb(sum.SelectedByKind["raw"].Bytes),
		sum.SelectedByKind["collated"].Objects, tb(sum.SelectedByKind["collated"].Bytes), sum.SavedUSDPerMonth, r2RetireUSDPerTB)
	for _, kind := range []string{"raw", "collated"} {
		var reasons []string
		for r := range sum.Kept[kind] {
			reasons = append(reasons, r)
		}
		sort.Slice(reasons, func(i, j int) bool { return sum.Kept[kind][reasons[i]].Bytes > sum.Kept[kind][reasons[j]].Bytes })
		for _, r := range reasons {
			fmt.Fprintf(w, "  kept %-8s %-22s %9d objects %s\n", kind, r, sum.Kept[kind][r].Objects, tb(sum.Kept[kind][r].Bytes))
		}
	}
	for _, s := range sum.Streams {
		fmt.Fprintf(w, "  stream %4d %-4s raw %7d / %s  stitched %6d / %s  $%.2f/mo\n", s.StreamID, s.Choice, s.Raw.Objects, tb(s.Raw.Bytes), s.Collated.Objects, tb(s.Collated.Bytes), s.USDMonth)
	}
	if hs := sum.HeadSample; hs != nil {
		fmt.Fprintf(w, "HEAD sample: checked=%d present=%d missing=%d (%.1f%%) size_mismatch=%d sha_mismatch=%d errors=%d\n",
			hs.Checked, hs.Present, hs.Missing, 100*hs.MissingRate, hs.SizeMismatch, hs.SHAMismatch, hs.Errors)
	}
	fmt.Fprintf(w, "manifest=%s\nmanifest_sha256=%s\n", sum.ManifestPath, sum.ManifestSHA256)
}

func runR2RetireApply(ctx context.Context, cfg config.Config, args []string, usage, policySHA string, windows map[int64]api.Keep28Window) {
	fs := flag.NewFlagSet("r2-retire apply", flag.ContinueOnError)
	var opts r2RetireApplyOptions
	fs.StringVar(&opts.AcceptOperatorCredentialRisk, "accept-operator-credential-risk", "", "owner decision reference acknowledging operator/release tools will not write footage keys during this run; required in HEAD-guard mode; never waives prefix exclusions or other guards")
	fs.BoolVar(&opts.ProviderUnconditionalHeadGuard, "provider-unconditional-delete-with-head-guard", false, "explicit residual TOCTOU acceptance; requires unsupported-provider probe, durable writer fence and real multipart inventory")
	fs.DurationVar(&opts.MinObjectAge, "min-object-age", 7*24*time.Hour, "HEAD LastModified minimum age in HEAD-guard mode (minimum 168h)")
	fs.StringVar(&opts.DeleteProofPath, "conditional-delete-proof", "", "operator provider-capability probe (required for NAS-backed apply; HEAD-guard mode requires unsupported result)")
	fs.StringVar(&opts.DeleteProofSHA256, "expected-delete-proof-sha256", "", "approved SHA-256 of the conditional-delete proof")
	fs.StringVar(&opts.ManifestPath, "manifest", "", "manifest.jsonl from r2-retire plan")
	fs.StringVar(&opts.ExpectedSHA, "expected-sha256", "", "approved manifest digest")
	fs.Int64Var(&opts.MaxBytes, "max-bytes", 0, "stop before retiring more than this many bytes in this run (required)")
	fs.Int64Var(&opts.Budget24h, "budget-24h-bytes", 5e12, "stop before the rolling 24h total (all runs, from the database) exceeds this")
	fs.StringVar(&opts.PicksPath, "picks", "", "picks file (default: the path recorded in the manifest)")
	fs.StringVar(&opts.LedgerPath, "ledger", r2RetireDefaultLedger(), "append-only JSONL ledger")
	fs.Float64Var(&opts.Rate, "rate", 10, "maximum objects per second")
	fs.Int64Var(&opts.MaxItems, "max-items", 0, "stop after retiring this many objects (0 = no limit)")
	fs.DurationVar(&opts.MaxManifestAge, "max-manifest-age", 72*time.Hour, "refuse manifests older than this (max 168h; cannot be disabled)")
	fs.BoolVar(&opts.SkipIneligible, "skip-ineligible", false, "skip (keep) items that are no longer eligible instead of stopping")
	fs.BoolVar(&opts.RecordAbsent, "record-already-absent", false, "mark items whose object is already missing from R2 instead of stopping")
	asJSON := fs.Bool("json", false, "print the summary as JSON")
	if err := fs.Parse(args); err != nil || fs.NArg() != 0 || opts.ManifestPath == "" || opts.ExpectedSHA == "" || opts.MaxBytes <= 0 {
		log.Fatal(usage)
	}
	if opts.Rate <= 0 || opts.Rate > 100 || opts.MaxItems < 0 || opts.Budget24h <= 0 || opts.MaxManifestAge <= 0 || opts.MaxManifestAge > r2RetireMaxManifestAge {
		log.Fatalf("r2-retire apply: --rate must be in (0,100], --max-items >= 0, --budget-24h-bytes > 0, --max-manifest-age in (0, %s]", r2RetireMaxManifestAge)
	}
	pool, err := r2RetireOpenPool(ctx, cfg, false)
	if err != nil {
		log.Fatalf("connect database: %v", err)
	}
	defer pool.Close()
	store := mustArchiveR2Client(ctx, cfg, pool)
	opts.Delivery, err = api.ReadPublicDeliveryKeep(ctx, mustArchiveR2Client(ctx, cfg, pool))
	if err != nil {
		log.Fatal(err)
	}
	sum, err := r2RetireApply(ctx, pool, store, opts, policySHA, windows)
	if *asJSON {
		b, _ := json.MarshalIndent(sum, "", "  ")
		fmt.Println(string(b))
	} else {
		fmt.Printf("r2-retire apply manifest=%s items=%d retired=%d bytes=%d (already_absent=%d) already_done=%d skipped=%v used_24h_before=%d limit=%s interrupted=%t residual_toctou=%t saved_usd_month=%.2f elapsed=%.0fs ledger=%s\n",
			sum.ManifestSHA256, sum.ManifestItems, sum.Retired, sum.RetiredBytes, sum.RetiredAbsent, sum.AlreadyDone, sum.SkippedIneligible,
			sum.Used24hBefore, sum.LimitReached, sum.Interrupted, sum.ResidualTOCTOU, sum.SavedUSDPerMonth, sum.ElapsedSec, sum.LedgerPath)
	}
	if !*asJSON && sum.OperatorCredentialRisk != nil {
		fmt.Printf("operator_credential_risk=%q accepted_at=%s\n", sum.OperatorCredentialRisk.DecisionReference, sum.OperatorCredentialRisk.At.Format(time.RFC3339Nano))
	}
	if err != nil {
		log.Fatalf("r2-retire apply stopped: %v", err)
	}
}
