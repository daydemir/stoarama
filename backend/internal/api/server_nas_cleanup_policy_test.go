package api

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestNASCleanupKeep28EmbeddedPolicy(t *testing.T) {
	p, err := cleanupEmbeddedPolicy()
	if err != nil {
		t.Fatal(err)
	}
	if p.SHA != cleanupPolicySHA || len(p.Streams) != 109 {
		t.Fatal("embedded keep28 policy incomplete")
	}
	for id, w := range p.Streams {
		loc, _ := time.LoadLocation(w.Timezone)
		a, b := w.start.In(loc), w.end.In(loc)
		if a.Hour() != 0 || b.Hour() != 0 || a.Format(time.DateOnly) != w.KeepStart || b.Format(time.DateOnly) != w.KeepEndExcl || a.AddDate(0, 0, 28) != b {
			t.Fatalf("stream %d window is not exactly 28 local days", id)
		}
	}
	w := p.Streams[3]
	if w.BestStart != "2026-09-19" || !w.start.Equal(time.Date(2026, 9, 11, 15, 0, 0, 0, time.UTC)) || !w.end.Equal(time.Date(2026, 10, 9, 15, 0, 0, 0, time.UTC)) || w.Timezone != "Asia/Tokyo" {
		t.Fatalf("stream 3 keep window differs: %+v", w)
	}
	if _, err = parseCleanupPolicy(append([]byte(" "), cleanupKeep28PolicyJSON...), cleanupPolicySHA, 109); err == nil {
		t.Fatal("tampered embedded policy accepted")
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
