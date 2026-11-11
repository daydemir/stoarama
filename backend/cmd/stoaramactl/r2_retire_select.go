package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/api"
)

// r2-retire selection: pure decision logic, shared by plan and apply.
//
// Legacy selection and phase 1 retire outside-window objects absent from NAS.
// NAS-backed plans additionally admit kept-window "none" objects with fresh,
// exact present inventory. Cleanup holds, references and unknowns always veto.

const (
	r2RetireNASReason  = "kept_none_exact_nas_copy"
	r2RetireReason     = "outside_keep28_no_nas_copy"
	r2RetireRawPrefix  = "managed/"
	r2RetireUSDPerTB   = 15.0
	r2RetireBytesPerTB = 1e12
)

// r2RetireProtectedPrefixes are never retired, whatever the database says.
// joined/best14-20261021/ holds the V85 best14 outputs, tracked only in a
// controller sqlite archived elsewhere.
// repaired/ holds repaired MP4s (moov-box repair of truncated clips). This is
// an explicit denylist on top of the per-kind allowlist below.
var r2RetireProtectedPrefixes = []string{"joined/best14-20261021/", "repaired/", "staging/"}

// r2RetireCollatedPrefixes are the only stitched-output prefixes in scope.
var r2RetireCollatedPrefixes = []string{"joined/goodplus-", "joined/nightly-collation-v2/"}

var r2RetireValidChoices = map[string]bool{"none": true, "st": true, "keep": true, "all": true}

// r2RetirePick is one stream's saved choice from r2-picks.json. The page also
// saves the window it showed; a window that differs from the embedded policy
// means the owner decided on something else, so the stream stays undecided.
type r2RetirePick struct {
	Choice       string `json:"choice"`
	KeepStartUTC string `json:"keep_start_utc,omitempty"`
	KeepEndUTC   string `json:"keep_end_utc,omitempty"`
}

type r2RetirePicks struct {
	Path    string
	SHA256  string
	SavedAt string
	Choices map[int64]r2RetirePick
}

func loadR2RetirePicks(path string) (r2RetirePicks, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return r2RetirePicks{}, fmt.Errorf("read picks: %w", err)
	}
	p, err := parseR2RetirePicks(raw)
	if err != nil {
		return r2RetirePicks{}, fmt.Errorf("%s: %w", path, err)
	}
	p.Path = path
	return p, nil
}

// parseR2RetirePicks fails on anything it does not understand; a stream entry
// with no choice (or an empty one) is undecided.
func parseR2RetirePicks(raw []byte) (r2RetirePicks, error) {
	var doc struct {
		Choices map[string]json.RawMessage `json:"choices"`
		SavedAt string                     `json:"saved_at"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		return r2RetirePicks{}, fmt.Errorf("invalid picks JSON: %w", err)
	}
	if doc.Choices == nil {
		return r2RetirePicks{}, errors.New("picks file has no choices object")
	}
	sum := sha256.Sum256(raw)
	out := r2RetirePicks{SHA256: hex.EncodeToString(sum[:]), SavedAt: doc.SavedAt, Choices: map[int64]r2RetirePick{}}
	for k, v := range doc.Choices {
		id, err := strconv.ParseInt(k, 10, 64)
		if err != nil || id <= 0 || strconv.FormatInt(id, 10) != k {
			return r2RetirePicks{}, fmt.Errorf("invalid stream id %q in picks", k)
		}
		var p r2RetirePick
		if err := json.Unmarshal(v, &p); err != nil {
			return r2RetirePicks{}, fmt.Errorf("stream %d: invalid pick: %w", id, err)
		}
		if p.Choice == "" {
			continue
		}
		if !r2RetireValidChoices[p.Choice] {
			return r2RetirePicks{}, fmt.Errorf("stream %d: unknown choice %q", id, p.Choice)
		}
		out.Choices[id] = p
	}
	return out, nil
}

// r2RetireEffectiveChoice is the choice the selection acts on: "" (undecided)
// unless the stream has a valid pick made against the same window.
func r2RetireEffectiveChoice(p r2RetirePick, ok bool, w api.Keep28Window) string {
	if !ok || !r2RetireValidChoices[p.Choice] || !r2RetireValidSpan(w.Start, w.End) {
		return ""
	}
	for _, pair := range []struct {
		raw  string
		want time.Time
	}{{p.KeepStartUTC, w.Start}, {p.KeepEndUTC, w.End}} {
		// A pick without the window it was made against is undecided: the
		// check is never skipped.
		got, ok := r2RetireParsePickTime(pair.raw)
		if !ok || !got.Equal(pair.want) {
			return ""
		}
	}
	return p.Choice
}

// r2RetirePickTimeLayouts are the formats the picks page and select.py write:
// "2026-09-11 11:00:00-04:00" (page) and RFC 3339 "2026-09-11T15:00:00+00:00".
var r2RetirePickTimeLayouts = []string{"2006-01-02 15:04:05Z07:00", time.RFC3339}

func r2RetireParsePickTime(raw string) (time.Time, bool) {
	if raw == "" || strings.TrimSpace(raw) != raw {
		return time.Time{}, false
	}
	for _, layout := range r2RetirePickTimeLayouts {
		if t, err := time.Parse(layout, raw); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

// r2RetireFacts are the database observations for one object. Every true
// value keeps the object.
type r2RetireFacts struct {
	Purged         bool // raw: purged_at already set
	Retired        bool // already in r2_object_retirements
	NASCopy        bool // any NAS inventory/ack/unmatched evidence
	ExactNAS       bool // connection inventory state present, exact path/SHA/size
	Held           bool // any NAS cleanup plan item, hold, or delete-issued plan
	JoinedRef      bool // joined snapshot/source/scope/artifact reference
	RestorePending bool // NAS restore queued or running
	SharedKey      bool // the object key is referenced by another catalog row
	Recent         bool // ended or registered within the grace period
}

// r2RetireCandidate is one catalog row (raw clip or stitched output).
type r2RetireCandidate struct {
	Kind        string    `json:"kind"`
	SourceID    int64     `json:"source_id"`
	RecordingID int64     `json:"recording_id"`
	StreamID    int64     `json:"stream_id"`
	Bucket      string    `json:"bucket"`
	ObjectKey   string    `json:"object_key"`
	SizeBytes   int64     `json:"size_bytes"`
	SHA256      string    `json:"sha256"`
	StartAt     time.Time `json:"start_at"`
	EndAt       time.Time `json:"end_at"`
	// Not in the manifest: re-read from the database at apply time.
	Timezone     string        `json:"-"`
	SourceStart  time.Time     `json:"-"`
	SourceEnd    time.Time     `json:"-"`
	SourceCount  int           `json:"-"`
	SourceFound  int           `json:"-"`
	Facts        r2RetireFacts `json:"-"`
	ETag         string        `json:"etag,omitempty"`
	RelativePath string        `json:"relative_path,omitempty"`
}

// r2RetireValidSpan mirrors the NAS cleanup policy: a nonempty interval
// inside 2020..2027. Anything else is unknown timing.
func r2RetireValidSpan(a, b time.Time) bool {
	return !a.IsZero() && b.After(a) && a.Year() >= 2020 && b.Year() < 2028
}

// r2RetireOutside reports whether [a,b) does not intersect [w.Start, w.End).
func r2RetireOutside(w api.Keep28Window, a, b time.Time) bool {
	if !r2RetireValidSpan(a, b) || !r2RetireValidSpan(w.Start, w.End) {
		return false
	}
	return !(a.Before(w.End) && b.After(w.Start))
}

func r2RetireHasPrefix(key string, prefixes []string) bool {
	for _, p := range prefixes {
		if strings.HasPrefix(key, p) {
			return true
		}
	}
	return false
}

// r2RetireKeyProtection returns a keep reason for a key that is protected,
// malformed, in another bucket, or outside the expected prefix for its kind.
func r2RetireKeyProtection(kind, objectBucket, key, bucket string) string {
	switch {
	case key == "" || strings.TrimSpace(key) != key || strings.HasPrefix(key, "/") || strings.Contains(key, "//") ||
		strings.Contains(key, "/../") || strings.HasPrefix(key, "../") || strings.ContainsRune(key, 0):
		return "unexpected_key"
	case r2RetireHasPrefix(key, r2RetireProtectedPrefixes):
		return "protected_prefix"
	case bucket == "" || objectBucket != bucket:
		return "other_bucket"
	}
	switch kind {
	case "raw":
		if !strings.HasPrefix(key, r2RetireRawPrefix) {
			return "unexpected_key"
		}
	case "collated":
		if !r2RetireHasPrefix(key, r2RetireCollatedPrefixes) {
			return "unexpected_key"
		}
	default:
		return "unknown_kind"
	}
	return ""
}

func r2RetireLowerHex64(s string) bool {
	if len(s) != 64 {
		return false
	}
	for _, r := range s {
		if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'f')) {
			return false
		}
	}
	return true
}

// r2RetireDecide returns (reason, true) for an object phase 1 may retire, or
// (keep reason, false). The order only affects which reason is reported.
func r2RetireDecide(c r2RetireCandidate, bucket string, windows map[int64]api.Keep28Window, picks map[int64]r2RetirePick) (string, string, bool) {
	return r2RetireDecideMode(c, bucket, windows, picks, false)
}

func r2RetireDecideMode(c r2RetireCandidate, bucket string, windows map[int64]api.Keep28Window, picks map[int64]r2RetirePick, nasBacked bool) (string, string, bool) {
	if r := r2RetireKeyProtection(c.Kind, c.Bucket, c.ObjectKey, bucket); r != "" {
		return r, "", false
	}
	if c.SizeBytes <= 0 || !r2RetireLowerHex64(c.SHA256) {
		return "identity_incomplete", "", false
	}
	if c.Facts.Purged {
		return "already_purged", "", false
	}
	if c.Facts.Retired {
		return "already_retired", "", false
	}
	w, ok := windows[c.StreamID]
	if !ok || c.StreamID <= 0 {
		return "no_keep_window", "", false
	}
	if c.Timezone == "" || c.Timezone != w.Timezone {
		return "timezone_changed", "", false
	}
	if !r2RetireValidSpan(c.StartAt, c.EndAt) || !r2RetireValidSpan(c.SourceStart, c.SourceEnd) || c.SourceCount < 1 || c.SourceFound != c.SourceCount {
		return "unknown_timing", "", false
	}
	outside := r2RetireOutside(w, c.StartAt, c.EndAt) && r2RetireOutside(w, c.SourceStart, c.SourceEnd)
	if !nasBacked && !outside {
		return "inside_keep_window", "", false
	}
	switch {
	case !nasBacked && c.Facts.NASCopy:
		return "nas_copy", "", false
	case c.Facts.Held:
		return "nas_cleanup_hold", "", false
	case c.Facts.JoinedRef:
		return "joined_reference", "", false
	case c.Facts.RestorePending:
		return "restore_pending", "", false
	case c.Facts.SharedKey:
		return "shared_object_key", "", false
	case c.Facts.Recent:
		return "within_grace", "", false
	}
	p, ok := picks[c.StreamID]
	choice := r2RetireEffectiveChoice(p, ok, w)
	switch choice {
	case "":
		if ok {
			return "pick_window_mismatch", "", false
		}
		return "undecided", "", false
	case "all":
		return "pick_all", choice, false
	case "none", "st", "keep":
		if nasBacked {
			if outside {
				if c.Facts.NASCopy || c.Facts.ExactNAS {
					return "after_nas_cleanup", choice, false
				}
				return r2RetireReason, choice, true
			}
			if choice != "none" {
				return "inside_keep_window", choice, false
			}
			if c.Facts.ExactNAS {
				return r2RetireNASReason, choice, true
			}
			if c.Facts.NASCopy {
				return "nas_identity_unproven", choice, false
			}
			if c.Kind == "collated" {
				return "kept_collated_no_exact_inventory", choice, false
			}
			return "copy_to_nas", choice, false
		}
		// Per the owner's picks page, none/st/keep all drop everything outside
		// the kept period; they differ only inside it (none: nothing, st:
		// stitched only, keep: raw + stitched). Phase 1 only touches footage
		// outside the period, so the three are equivalent here.
		return r2RetireReason, choice, true
	}
	return "invalid_pick", "", false
}
