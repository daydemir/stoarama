package api

import (
	"encoding/json"
	"os"
	"sort"
	"strings"
	"testing"
	"time"
)

// The active policy is the exact reviewed 20-day manifest. It must hold one
// window per stream for the same 109 stream identities, local midnights,
// exactly 20 local days, derived from the legacy best14 winner.
func TestNASCleanupKeep20EmbeddedPolicy(t *testing.T) {
	p, err := cleanupEmbeddedPolicy()
	if err != nil {
		t.Fatal(err)
	}
	if p.SHA != cleanupPolicySHA || len(p.Streams) != 109 {
		t.Fatal("embedded keep20 policy incomplete")
	}
	if cleanupPolicySHA != "bcc670a34024c2f8e2ba3df2566397f370d03e93ff33a7bb828c4b83eb123393" {
		t.Fatalf("active policy digest changed: %s", cleanupPolicySHA)
	}
	legacy, err := cleanupEmbeddedKeep28Policy()
	if err != nil {
		t.Fatal(err)
	}
	kept := 0
	for id, w := range p.Streams {
		old, ok := legacy.Streams[id]
		if !ok || old.Timezone != w.Timezone || old.BestStart != w.BestStart {
			t.Fatalf("stream %d lost its legacy identity", id)
		}
		loc, _ := time.LoadLocation(w.Timezone)
		a, b := w.start.In(loc), w.end.In(loc)
		if a.Hour() != 0 || b.Hour() != 0 || a.Format(time.DateOnly) != w.KeepStart || b.Format(time.DateOnly) != w.KeepEndExcl || a.AddDate(0, 0, 20) != b {
			t.Fatalf("stream %d window is not exactly 20 local days", id)
		}
		best, _ := time.ParseInLocation(time.DateOnly, w.BestStart, loc)
		if !a.Equal(best.AddDate(0, 0, -3)) || !b.Equal(best.AddDate(0, 0, 17)) {
			t.Fatalf("stream %d window is not [best-3d, best+17d)", id)
		}
		if w.start.Before(old.start) || w.end.After(old.end) {
			t.Fatalf("stream %d window widened the legacy keep", id)
		}
		if w.KeepEverything {
			kept++
		}
	}
	if kept != 23 {
		t.Fatalf("keep-everything stream count %d, want 23", kept)
	}
	w := p.Streams[3]
	if w.BestStart != "2026-09-19" || w.Timezone != "Asia/Tokyo" ||
		!w.start.Equal(time.Date(2026, 9, 15, 15, 0, 0, 0, time.UTC)) || !w.end.Equal(time.Date(2026, 10, 5, 15, 0, 0, 0, time.UTC)) {
		t.Fatalf("stream 3 keep-20 window differs: %+v", w)
	}
	if _, err = parseCleanupKeep20Policy(append([]byte(" "), cleanupKeep20PolicyJSON...), cleanupPolicySHA, 109, legacy); err == nil {
		t.Fatal("tampered embedded policy accepted")
	}
	if _, err = parseCleanupKeep20Policy(cleanupKeep20PolicyJSON, cleanupPolicySHA, 109, nil); err == nil {
		t.Fatal("keep20 policy accepted without legacy provenance")
	}
}

// DST makes a local 20-day window 481 UTC hours for exactly the two streams
// whose window crosses a fall-back transition; nothing may assume 480 hours.
func TestNASCleanupKeep20DSTSpans(t *testing.T) {
	p, err := cleanupEmbeddedPolicy()
	if err != nil {
		t.Fatal(err)
	}
	spans := map[int64]time.Duration{}
	for id, w := range p.Streams {
		spans[id] = w.end.Sub(w.start)
	}
	for id, want := range map[int64]time.Duration{94: 481 * time.Hour, 17337: 481 * time.Hour} {
		if spans[id] != want {
			t.Fatalf("stream %d UTC span %s, want %s", id, spans[id], want)
		}
	}
	hours := 0
	for _, span := range spans {
		if span == 481*time.Hour {
			hours++
		}
	}
	if hours != 2 {
		t.Fatalf("481h window count %d, want 2", hours)
	}
	for id, span := range spans {
		if span != 480*time.Hour && span != 481*time.Hour {
			t.Fatalf("stream %d span %s is neither 480 nor 481 UTC hours", id, span)
		}
	}
}

// The 109 identities and their canonical timezones are the reviewed ones.
func TestNASCleanupKeep20IdentitySet(t *testing.T) {
	p, err := cleanupEmbeddedPolicy()
	if err != nil {
		t.Fatal(err)
	}
	legacy, err := cleanupEmbeddedKeep28Policy()
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]int64, 0, len(p.Streams))
	zones := map[string]int{}
	for id := range p.Streams {
		ids = append(ids, id)
		zones[p.Streams[id].Timezone]++
	}
	sort.Slice(ids, func(a, b int) bool { return ids[a] < ids[b] })
	if len(ids) != len(legacy.Streams) {
		t.Fatalf("stream count %d differs from legacy %d", len(ids), len(legacy.Streams))
	}
	for _, id := range ids {
		if _, ok := legacy.Streams[id]; !ok {
			t.Fatalf("stream %d is not in the legacy selection", id)
		}
	}
	if zones["Asia/Tokyo"] == 0 || zones["America/Los_Angeles"] == 0 || zones["Europe/Warsaw"] == 0 {
		t.Fatalf("canonical timezones incomplete: %v", zones)
	}
}

func keep28Fixture(t *testing.T, rows []map[string]any) ([]byte, string) {
	t.Helper()
	raw, _ := json.Marshal(rows)
	return raw, cleanupSHA(raw)
}

func keep28Row(id int64, best, ks, ke, su, eu string) map[string]any {
	return map[string]any{"stream_id": id, "tz": "America/Los_Angeles", "best_start": best, "keep_start": ks, "keep_end_excl": ke, "keep_start_utc": su, "keep_end_utc": eu}
}

func keep20Row(id int64, tz, start, end string, keep bool) map[string]any {
	return map[string]any{"stream_id": id, "timezone": tz, "start_at": start, "end_at": end, "keep_everything": keep}
}

func TestNASCleanupKeep28LegacyPolicyPreserved(t *testing.T) {
	p, err := cleanupEmbeddedKeep28Policy()
	if err != nil {
		t.Fatal(err)
	}
	if p.SHA != cleanupLegacyKeep28SHA || len(p.Streams) != 109 {
		t.Fatal("legacy keep28 policy changed")
	}
	for id, w := range p.Streams {
		loc, _ := time.LoadLocation(w.Timezone)
		a, b := w.start.In(loc), w.end.In(loc)
		if a.Hour() != 0 || b.Hour() != 0 || a.AddDate(0, 0, 28) != b {
			t.Fatalf("legacy stream %d window is not exactly 28 local days", id)
		}
	}
	w := p.Streams[3]
	if w.BestStart != "2026-09-19" || !w.start.Equal(time.Date(2026, 9, 11, 15, 0, 0, 0, time.UTC)) || !w.end.Equal(time.Date(2026, 10, 9, 15, 0, 0, 0, time.UTC)) {
		t.Fatalf("legacy stream 3 keep window differs: %+v", w)
	}
}

func TestNASCleanupKeep28PolicyRejectsMalformedWindows(t *testing.T) {
	good := keep28Row(7, "2026-09-10", "2026-09-03", "2026-10-01", "2026-09-03T07:00:00+00:00", "2026-10-01T07:00:00+00:00")
	raw, sha := keep28Fixture(t, []map[string]any{good})
	if _, err := parseCleanupPolicy(raw, sha, 1); err != nil {
		t.Fatalf("valid window rejected: %v", err)
	}
	for name, rows := range map[string][]map[string]any{
		"two windows one stream": {good, keep28Row(7, "2026-08-10", "2026-08-03", "2026-08-31", "2026-08-03T07:00:00+00:00", "2026-08-31T07:00:00+00:00")},
		"27 days":                {keep28Row(7, "2026-09-10", "2026-09-03", "2026-09-30", "2026-09-03T07:00:00+00:00", "2026-09-30T07:00:00+00:00")},
		"not centred on best14":  {keep28Row(7, "2026-09-11", "2026-09-03", "2026-10-01", "2026-09-03T07:00:00+00:00", "2026-10-01T07:00:00+00:00")},
		"utc not local midnight": {keep28Row(7, "2026-09-10", "2026-09-03", "2026-10-01", "2026-09-03T00:00:00+00:00", "2026-10-01T07:00:00+00:00")},
	} {
		raw, sha := keep28Fixture(t, rows)
		if _, err := parseCleanupPolicy(raw, sha, len(rows)); err == nil {
			t.Fatalf("%s accepted", name)
		}
	}
	if _, err := parseCleanupPolicy(raw, strings.Repeat("0", 64), 1); err == nil {
		t.Fatal("unpinned policy accepted")
	}
	if _, err := parseCleanupPolicy(raw, sha, 2); err == nil {
		t.Fatal("incomplete stream count accepted")
	}
}

// Any drift of the 20-day manifest from the legacy winner keeps everything:
// the derivation is a pure validation, never a repair.
func TestNASCleanupKeep20RejectsDrift(t *testing.T) {
	legacy := &nasCleanupPolicy{SHA: cleanupLegacyKeep28SHA, Streams: map[int64]cleanupKeepWindow{}}
	// best_start 2026-09-10 in Los Angeles => legacy [2026-09-03, 2026-10-01).
	w := cleanupKeepWindow{StreamID: 7, Timezone: "America/Los_Angeles", BestStart: "2026-09-10",
		KeepStart: "2026-09-03", KeepEndExcl: "2026-10-01", StartUTC: "2026-09-03T07:00:00+00:00", EndUTC: "2026-10-01T07:00:00+00:00"}
	w.start = time.Date(2026, 9, 3, 7, 0, 0, 0, time.UTC)
	w.end = time.Date(2026, 10, 1, 7, 0, 0, 0, time.UTC)
	legacy.Streams[7] = w
	good := keep20Row(7, "America/Los_Angeles", "2026-09-07T07:00:00+00:00", "2026-09-27T07:00:00+00:00", false)
	raw, sha := keep28Fixture(t, []map[string]any{good})
	if _, err := parseCleanupKeep20Policy(raw, sha, 1, legacy); err != nil {
		t.Fatalf("valid keep20 window rejected: %v", err)
	}
	for name, rows := range map[string][]map[string]any{
		"19 local days":         {keep20Row(7, "America/Los_Angeles", "2026-09-08T07:00:00+00:00", "2026-09-27T07:00:00+00:00", false)},
		"21 local days":         {keep20Row(7, "America/Los_Angeles", "2026-09-06T07:00:00+00:00", "2026-09-27T07:00:00+00:00", false)},
		"not lead-3":            {keep20Row(7, "America/Los_Angeles", "2026-09-06T07:00:00+00:00", "2026-09-26T07:00:00+00:00", false)},
		"not a local midnight":  {keep20Row(7, "America/Los_Angeles", "2026-09-07T00:00:00+00:00", "2026-09-27T07:00:00+00:00", false)},
		"timezone drifted":      {keep20Row(7, "America/Denver", "2026-09-07T07:00:00+00:00", "2026-09-27T07:00:00+00:00", false)},
		"unknown stream":        {keep20Row(8, "America/Los_Angeles", "2026-09-07T07:00:00+00:00", "2026-09-27T07:00:00+00:00", false)},
		"keep flag not boolean": {map[string]any{"stream_id": 7, "timezone": "America/Los_Angeles", "start_at": "2026-09-07T07:00:00+00:00", "end_at": "2026-09-27T07:00:00+00:00", "keep_everything": "no"}},
	} {
		raw, sha := keep28Fixture(t, rows)
		if _, err := parseCleanupKeep20Policy(raw, sha, 1, legacy); err == nil {
			t.Fatalf("%s accepted", name)
		}
	}
	// A window widened past the legacy selection is refused.
	raw, sha = keep28Fixture(t, []map[string]any{keep20Row(7, "America/Los_Angeles", "2026-09-02T07:00:00+00:00", "2026-09-22T07:00:00+00:00", false)})
	if _, err := parseCleanupKeep20Policy(raw, sha, 1, legacy); err == nil {
		t.Fatal("widened window accepted")
	}
	// A truncated selection is refused.
	raw, sha = keep28Fixture(t, []map[string]any{good})
	if _, err := parseCleanupKeep20Policy(raw, sha, 2, legacy); err == nil {
		t.Fatal("incomplete selection accepted")
	}
	if _, err := parseCleanupKeep20Policy(raw, strings.Repeat("0", 64), 1, legacy); err == nil {
		t.Fatal("unpinned keep20 selection accepted")
	}
}

func TestNASCleanupProtectedIntervalsAreHalfOpen(t *testing.T) {
	raw, sha := keep28Fixture(t, []map[string]any{keep28Row(3, "2026-09-10", "2026-09-03", "2026-10-01", "2026-09-03T07:00:00+00:00", "2026-10-01T07:00:00+00:00")})
	p, err := parseCleanupPolicy(raw, sha, 1)
	if err != nil {
		t.Fatal(err)
	}
	a := time.Date(2026, 9, 3, 7, 0, 0, 0, time.UTC)
	end := time.Date(2026, 10, 1, 7, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		sid     int64
		a, b    time.Time
		outside bool
	}{
		{3, a.Add(-time.Hour), a, true},
		{3, a.Add(-time.Hour), a.Add(time.Nanosecond), false},
		{3, a.Add(10 * 24 * time.Hour), a.Add(10*24*time.Hour + time.Minute), false},
		{3, end.Add(-time.Nanosecond), end.Add(time.Hour), false},
		{3, end, end.Add(time.Hour), true},
		{3, time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 7, 1, 1, 0, 0, 0, time.UTC), true},
		{4, a.Add(-time.Hour), a, false}, // unknown stream: KEEP
		{3, a.Add(-time.Hour), a.Add(-time.Hour), false},
		{3, time.Date(2019, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2019, 1, 1, 1, 0, 0, 0, time.UTC), false},
	} {
		if p.outside(tc.sid, tc.a, tc.b) != tc.outside {
			t.Fatalf("bad protected range decision %+v", tc)
		}
	}
}

// Pinning a stream keep-everything makes every interval KEEP, including
// intervals far outside its window.
func TestNASCleanupKeepEverythingIsNeverEligible(t *testing.T) {
	p, err := cleanupEmbeddedPolicy()
	if err != nil {
		t.Fatal(err)
	}
	// 78 is pinned keep-everything in the reviewed manifest.
	w, ok := p.Streams[78]
	if !ok || !w.KeepEverything {
		t.Fatalf("stream 78 is not pinned keep-everything: %+v", w)
	}
	far := w.start.AddDate(-1, 0, 0)
	if p.outside(78, far, far.Add(time.Hour)) {
		t.Fatal("keep-everything stream was cleanup-eligible")
	}
	if p.outside(3, far, far.Add(time.Hour)) != true {
		t.Fatal("ordinary stream lost eligibility outside its window")
	}
	// Stream 94 is keep-everything and DST-spanned at the same time.
	if w94 := p.Streams[94]; !w94.KeepEverything || w94.end.Sub(w94.start) != 481*time.Hour {
		t.Fatalf("stream 94 keep-20 pin differs: %+v", w94)
	}
}

// One pin, three publication sites: the reviewed $policy$ manifest in migration
// 0236, the embedded Go bytes and the client's zlib copy must be identical.
func TestNASCleanupKeep20PinMatchesMigrationManifest(t *testing.T) {
	raw, err := os.ReadFile("../../../infra/sql/migrations/0236_nas_media_work_keep20.sql")
	if err != nil {
		t.Fatal(err)
	}
	const open, close = "$policy$", "$policy$"
	start := strings.Index(string(raw), open)
	if start < 0 {
		t.Fatal("migration has no $policy$ manifest")
	}
	body := string(raw)[start+len(open):]
	end := strings.Index(body, close)
	if end < 0 {
		t.Fatal("migration $policy$ manifest is unterminated")
	}
	published := body[:end]
	if cleanupSHA([]byte(published)) != "bcc670a34024c2f8e2ba3df2566397f370d03e93ff33a7bb828c4b83eb123393" {
		t.Fatalf("migration manifest digest changed: %s", cleanupSHA([]byte(published)))
	}
	if published != string(cleanupKeep20PolicyJSON) {
		t.Fatal("embedded keep20 bytes differ from the migration manifest")
	}
}
