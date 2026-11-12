package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/smithy-go"

	"github.com/daydemir/stoarama/backend/internal/api"
	"github.com/daydemir/stoarama/backend/internal/r2"
)

const r2tPolicy = "9999999999999999999999999999999999999999999999999999999999999999"

const r2tSHA = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func r2tWindow(t *testing.T, stream int64, tz, keepStart, keepEnd string) api.Keep28Window {
	t.Helper()
	loc, err := time.LoadLocation(tz)
	if err != nil {
		t.Fatal(err)
	}
	s, _ := time.ParseInLocation(time.DateOnly, keepStart, loc)
	e, _ := time.ParseInLocation(time.DateOnly, keepEnd, loc)
	return api.Keep28Window{StreamID: stream, Timezone: tz, Start: s.UTC(), End: e.UTC()}
}

func r2tUTC(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t
}

func r2tCandidate(kind string, stream int64, tz string, a, b time.Time) r2RetireCandidate {
	key := "managed/acct-47/x.mp4"
	if kind == "collated" {
		key = "joined/nightly-collation-v2/objects/x.mp4"
	}
	return r2RetireCandidate{Kind: kind, SourceID: 1, RecordingID: 1, StreamID: stream, Bucket: "stoarama", ObjectKey: key,
		SizeBytes: 100, SHA256: r2tSHA, StartAt: a, EndAt: b, Timezone: tz, SourceStart: a, SourceEnd: b, SourceCount: 1, SourceFound: 1}
}

func TestR2RetireEmbeddedKeep28Policy(t *testing.T) {
	sha, windows, err := api.Keep28Policy()
	if err != nil {
		t.Fatal(err)
	}
	if sha != "060aa7070c7909d78013415f83b76bd0c027a885caf91c67c8b709ac1f4b5811" || len(windows) != 109 {
		t.Fatalf("policy sha=%s streams=%d", sha, len(windows))
	}
	for id, w := range windows {
		loc, err := time.LoadLocation(w.Timezone)
		if err != nil {
			t.Fatal(err)
		}
		s, e := w.Start.In(loc), w.End.In(loc)
		if s.Hour() != 0 || s.Minute() != 0 || e.Hour() != 0 || e.Minute() != 0 || !s.AddDate(0, 0, 28).Equal(e) || w.StreamID != id {
			t.Fatalf("stream %d window %s..%s is not 28 local days between local midnights", id, s, e)
		}
	}
}

// The window bounds are local midnights, so across a DST change the end is a
// different UTC offset than the start. [start,end) is half-open.
func TestR2RetireWindowEdgesAndDST(t *testing.T) {
	// America/Los_Angeles: PDT (-7) on 2026-10-25, PST (-8) from 2026-11-01.
	w := r2tWindow(t, 4, "America/Los_Angeles", "2026-10-25", "2026-11-22")
	if !w.Start.Equal(r2tUTC("2026-10-25T07:00:00Z")) || !w.End.Equal(r2tUTC("2026-11-22T08:00:00Z")) {
		t.Fatalf("window %s..%s", w.Start, w.End)
	}
	windows := map[int64]api.Keep28Window{4: w}
	picks := map[int64]r2RetirePick{4: {Choice: "none", KeepStartUTC: w.Start.Format(time.RFC3339), KeepEndUTC: w.End.Format(time.RFC3339)}}
	cases := []struct {
		name string
		a, b time.Time
		want string
	}{
		{"ends exactly at keep start", w.Start.Add(-time.Minute), w.Start, r2RetireReason},
		{"ends 1us after keep start", w.Start.Add(-time.Minute), w.Start.Add(time.Microsecond), "inside_keep_window"},
		{"starts exactly at keep end", w.End, w.End.Add(time.Minute), r2RetireReason},
		{"starts 1s before keep end", w.End.Add(-time.Second), w.End.Add(time.Minute), "inside_keep_window"},
		// A fixed PDT offset would put the end at 07:00Z and wrongly free this hour.
		{"last local hour after DST ended", r2tUTC("2026-11-22T07:00:00Z"), r2tUTC("2026-11-22T07:30:00Z"), "inside_keep_window"},
		{"fall-back repeated hour inside", r2tUTC("2026-11-01T08:30:00Z"), r2tUTC("2026-11-01T09:30:00Z"), "inside_keep_window"},
		{"spans the whole window", w.Start.Add(-time.Hour), w.End.Add(time.Hour), "inside_keep_window"},
		{"zero start", time.Time{}, w.Start.Add(-time.Hour), "unknown_timing"},
		{"empty interval", w.End.Add(time.Hour), w.End.Add(time.Hour), "unknown_timing"},
		{"reversed interval", w.End.Add(2 * time.Hour), w.End.Add(time.Hour), "unknown_timing"},
		{"before 2020", r2tUTC("2019-12-31T23:00:00Z"), r2tUTC("2019-12-31T23:30:00Z"), "unknown_timing"},
		{"2028 or later", r2tUTC("2028-01-01T00:00:00Z"), r2tUTC("2028-01-01T01:00:00Z"), "unknown_timing"},
	}
	for _, tc := range cases {
		for _, kind := range []string{"raw", "collated"} {
			c := r2tCandidate(kind, 4, "America/Los_Angeles", tc.a, tc.b)
			got, _, ok := r2RetireDecide(c, "stoarama", windows, picks)
			if got != tc.want || ok != (tc.want == r2RetireReason) {
				t.Errorf("%s/%s: got %s ok=%t want %s", kind, tc.name, got, ok, tc.want)
			}
		}
	}
	// A stitched file outside the window whose sources reach into it stays.
	c := r2tCandidate("collated", 4, "America/Los_Angeles", w.End, w.End.Add(time.Hour))
	c.SourceStart = w.End.Add(-time.Minute)
	if got, _, _ := r2RetireDecide(c, "stoarama", windows, picks); got != "inside_keep_window" {
		t.Fatalf("source hull inside window: %s", got)
	}
	c = r2tCandidate("collated", 4, "America/Los_Angeles", w.End, w.End.Add(time.Hour))
	c.SourceCount, c.SourceFound = 3, 2
	if got, _, _ := r2RetireDecide(c, "stoarama", windows, picks); got != "unknown_timing" {
		t.Fatalf("missing source: %s", got)
	}
}

func TestR2RetireDecideFailsClosed(t *testing.T) {
	w := r2tWindow(t, 3, "Asia/Tokyo", "2026-09-12", "2026-10-10")
	windows := map[int64]api.Keep28Window{3: w, 5: r2tWindow(t, 5, "Asia/Tokyo", "2026-09-12", "2026-10-10")}
	a, b := r2tUTC("2026-08-01T00:00:00Z"), r2tUTC("2026-08-01T00:01:00Z")
	// Page format: local wall time with a space and an offset.
	pk := func(choice string) r2RetirePick {
		return r2RetirePick{Choice: choice, KeepStartUTC: "2026-09-11 11:00:00-04:00", KeepEndUTC: "2026-10-09 11:00:00-04:00"}
	}
	picks := map[int64]r2RetirePick{3: pk("keep")}
	base := func() r2RetireCandidate { return r2tCandidate("raw", 3, "Asia/Tokyo", a, b) }
	type mut func(*r2RetireCandidate, map[int64]r2RetirePick)
	cases := []struct {
		name string
		m    mut
		want string
	}{
		{"eligible", func(*r2RetireCandidate, map[int64]r2RetirePick) {}, r2RetireReason},
		{"undecided stream", func(c *r2RetireCandidate, _ map[int64]r2RetirePick) { c.StreamID = 5 }, "undecided"},
		{"pick all", func(_ *r2RetireCandidate, p map[int64]r2RetirePick) { p[3] = pk("all") }, "pick_all"},
		{"pick none", func(_ *r2RetireCandidate, p map[int64]r2RetirePick) { p[3] = pk("none") }, r2RetireReason},
		{"pick st", func(_ *r2RetireCandidate, p map[int64]r2RetirePick) { p[3] = pk("st") }, r2RetireReason},
		{"pick made against another window", func(_ *r2RetireCandidate, p map[int64]r2RetirePick) {
			p[3] = r2RetirePick{Choice: "none", KeepStartUTC: "2026-09-01T15:00:00+00:00", KeepEndUTC: "2026-10-09T15:00:00+00:00"}
		}, "pick_window_mismatch"},
		{"pick without window fields", func(_ *r2RetireCandidate, p map[int64]r2RetirePick) { p[3] = r2RetirePick{Choice: "none"} }, "pick_window_mismatch"},
		{"pick with only a start", func(_ *r2RetireCandidate, p map[int64]r2RetirePick) {
			p[3] = r2RetirePick{Choice: "none", KeepStartUTC: "2026-09-11 11:00:00-04:00"}
		}, "pick_window_mismatch"},
		{"pick end off by one hour", func(_ *r2RetireCandidate, p map[int64]r2RetirePick) {
			p[3] = r2RetirePick{Choice: "none", KeepStartUTC: "2026-09-11 11:00:00-04:00", KeepEndUTC: "2026-10-09 12:00:00-04:00"}
		}, "pick_window_mismatch"},
		{"pick with unparseable window", func(_ *r2RetireCandidate, p map[int64]r2RetirePick) {
			p[3] = r2RetirePick{Choice: "none", KeepStartUTC: "2026-09-11", KeepEndUTC: "2026-10-09"}
		}, "pick_window_mismatch"},
		{"pick with the same window", func(_ *r2RetireCandidate, p map[int64]r2RetirePick) {
			p[3] = r2RetirePick{Choice: "none", KeepStartUTC: "2026-09-11T15:00:00+00:00", KeepEndUTC: "2026-10-09T15:00:00+00:00"}
		}, r2RetireReason},
		{"stream without window", func(c *r2RetireCandidate, _ map[int64]r2RetirePick) { c.StreamID = 99 }, "no_keep_window"},
		{"timezone changed", func(c *r2RetireCandidate, _ map[int64]r2RetirePick) { c.Timezone = "UTC" }, "timezone_changed"},
		{"nas copy", func(c *r2RetireCandidate, _ map[int64]r2RetirePick) { c.Facts.NASCopy = true }, "nas_copy"},
		{"nas cleanup hold", func(c *r2RetireCandidate, _ map[int64]r2RetirePick) { c.Facts.Held = true }, "nas_cleanup_hold"},
		{"joined reference", func(c *r2RetireCandidate, _ map[int64]r2RetirePick) { c.Facts.JoinedRef = true }, "joined_reference"},
		{"restore pending", func(c *r2RetireCandidate, _ map[int64]r2RetirePick) { c.Facts.RestorePending = true }, "restore_pending"},
		{"shared key", func(c *r2RetireCandidate, _ map[int64]r2RetirePick) { c.Facts.SharedKey = true }, "shared_object_key"},
		{"recent", func(c *r2RetireCandidate, _ map[int64]r2RetirePick) { c.Facts.Recent = true }, "within_grace"},
		{"purged", func(c *r2RetireCandidate, _ map[int64]r2RetirePick) { c.Facts.Purged = true }, "already_purged"},
		{"retired", func(c *r2RetireCandidate, _ map[int64]r2RetirePick) { c.Facts.Retired = true }, "already_retired"},
		{"no sha", func(c *r2RetireCandidate, _ map[int64]r2RetirePick) { c.SHA256 = "" }, "identity_incomplete"},
		{"upper sha", func(c *r2RetireCandidate, _ map[int64]r2RetirePick) { c.SHA256 = strings.ToUpper(r2tSHA) }, "identity_incomplete"},
		{"zero size", func(c *r2RetireCandidate, _ map[int64]r2RetirePick) { c.SizeBytes = 0 }, "identity_incomplete"},
		{"best14 prefix", func(c *r2RetireCandidate, _ map[int64]r2RetirePick) {
			c.Kind, c.ObjectKey = "collated", "joined/best14-20261021/objects/a.mp4"
		}, "protected_prefix"},
		{"staging prefix", func(c *r2RetireCandidate, _ map[int64]r2RetirePick) { c.ObjectKey = "staging/x" }, "protected_prefix"},
		{"repaired prefix", func(c *r2RetireCandidate, _ map[int64]r2RetirePick) { c.ObjectKey = "repaired/managed/acct-47/x.mp4" }, "protected_prefix"},
		{"repaired prefix (collated)", func(c *r2RetireCandidate, _ map[int64]r2RetirePick) {
			c.Kind, c.ObjectKey = "collated", "repaired/joined/x.mp4"
		}, "protected_prefix"},
		{"raw outside managed/", func(c *r2RetireCandidate, _ map[int64]r2RetirePick) { c.ObjectKey = "joined/goodplus-x/a.mp4" }, "unexpected_key"},
		{"collated outside known prefixes", func(c *r2RetireCandidate, _ map[int64]r2RetirePick) {
			c.Kind, c.ObjectKey = "collated", "joined/other/a.mp4"
		}, "unexpected_key"},
		{"padded key", func(c *r2RetireCandidate, _ map[int64]r2RetirePick) { c.ObjectKey = " managed/a.mp4" }, "unexpected_key"},
		{"dot-dot key", func(c *r2RetireCandidate, _ map[int64]r2RetirePick) {
			c.ObjectKey = "managed/../joined/best14-20261021/a"
		}, "unexpected_key"},
		{"other bucket", func(c *r2RetireCandidate, _ map[int64]r2RetirePick) { c.Bucket = "byo" }, "other_bucket"},
		{"unknown kind", func(c *r2RetireCandidate, _ map[int64]r2RetirePick) { c.Kind = "thumb" }, "unknown_kind"},
		// Protected beats everything, including facts that would otherwise allow it.
		{"protected even if in scope", func(c *r2RetireCandidate, p map[int64]r2RetirePick) {
			c.Kind, c.ObjectKey = "collated", "joined/best14-20261021/x.mp4"
			p[3] = pk("none")
		}, "protected_prefix"},
	}
	for _, tc := range cases {
		c := base()
		p := map[int64]r2RetirePick{}
		for k, v := range picks {
			p[k] = v
		}
		tc.m(&c, p)
		got, choice, ok := r2RetireDecide(c, "stoarama", windows, p)
		if got != tc.want || ok != (tc.want == r2RetireReason) || (ok && choice == "") || (ok && choice == "all") {
			t.Errorf("%s: got %s choice=%q ok=%t want %s", tc.name, got, choice, ok, tc.want)
		}
	}
}

func TestParseR2RetirePicks(t *testing.T) {
	p, err := parseR2RetirePicks([]byte(`{"choices":{"3":{"choice":"none","bytes":5},"4":{"choice":"all"},"5":{"choice":""},"6":{}},"saved_at":"2026-11-09T00:00:00Z","saved_by":"x"}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Choices) != 2 || p.Choices[3].Choice != "none" || p.Choices[4].Choice != "all" || p.SavedAt == "" || len(p.SHA256) != 64 {
		t.Fatalf("picks=%+v", p)
	}
	for _, bad := range []string{`{}`, `{"choices":{"x":{"choice":"none"}}}`, `{"choices":{"03":{"choice":"none"}}}`,
		`{"choices":{"3":{"choice":"some"}}}`, `{"choices":{"-3":{"choice":"none"}}}`, `not json`} {
		if _, err := parseR2RetirePicks([]byte(bad)); err == nil {
			t.Errorf("accepted %s", bad)
		}
	}
}

func TestR2RetireManifestRoundTripAndTamper(t *testing.T) {
	dir := t.TempDir()
	h := r2RetireHeader{Version: r2RetireManifestVersion, Reason: r2RetireReason, Bucket: "stoarama", Choices: map[string]string{"3": "none"}, GraceSeconds: 604800}
	items := []r2RetireItem{
		{r2RetireCandidate: r2RetireCandidate{Kind: "raw", SourceID: 7, StreamID: 3, Bucket: "stoarama", ObjectKey: "managed/a.mp4", SizeBytes: 10, SHA256: r2tSHA,
			StartAt: r2tUTC("2026-08-01T00:00:00Z").Add(123456 * time.Microsecond), EndAt: r2tUTC("2026-08-01T00:01:00Z")}, Choice: "none", Reason: r2RetireReason},
	}
	h.Items, h.Bytes = 1, 10
	path := filepath.Join(dir, "m.jsonl")
	digest, err := writeR2RetireManifest(path, h, items)
	if err != nil {
		t.Fatal(err)
	}
	gh, gi, gd, err := readR2RetireManifest(path)
	if err != nil || gd != digest || len(gi) != 1 || !gi[0].StartAt.Equal(items[0].StartAt) || gh.Choices["3"] != "none" {
		t.Fatalf("round trip: %v %s %+v", err, gd, gi)
	}
	raw, _ := os.ReadFile(path)
	for name, mutate := range map[string]func(string) string{
		"totals":     func(s string) string { return strings.Replace(s, `"bytes":10`, `"bytes":11`, 1) },
		"protected":  func(s string) string { return strings.Replace(s, `managed/a.mp4`, `joined/best14-20261021/a.mp4`, 1) },
		"pick all":   func(s string) string { return strings.Replace(s, `"choice":"none"`, `"choice":"all"`, 1) },
		"truncated":  func(s string) string { return strings.TrimSuffix(s, "\n") },
		"duplicated": func(s string) string { lines := strings.SplitAfter(s, "\n"); return s + lines[1] },
	} {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(mutate(string(raw))), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, _, d, err := readR2RetireManifest(p); err == nil || d == digest {
			t.Errorf("%s: tampered manifest accepted (err=%v)", name, err)
		}
	}
}

// ---------------------------------------------------------------- Postgres

type fakeR2RetireStore struct {
	mu         sync.Mutex
	objects    map[string]int64
	deleted    []string
	heads      int
	beforeHead func(key string)
	deleteErr  error
}

func (f *fakeR2RetireStore) Bucket() string         { return "stoarama" }
func (f *fakeR2RetireStore) EndpointSHA256() string { return r2tSHA }

func (f *fakeR2RetireStore) Head(_ context.Context, key string) (r2.ObjectHead, error) {
	if f.beforeHead != nil {
		f.beforeHead(key)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.heads++
	n, ok := f.objects[key]
	if !ok {
		return r2.ObjectHead{}, &smithy.GenericAPIError{Code: "NotFound"}
	}
	return r2.ObjectHead{ETag: "e", SizeBytes: n}, nil
}

func (f *fakeR2RetireStore) DeleteObject(_ context.Context, key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.deleteErr != nil {
		return f.deleteErr
	}
	delete(f.objects, key)
	f.deleted = append(f.deleted, key)
	return nil
}

func TestR2RetirePlanAndApplyPostgres(t *testing.T) {
	pool := joinedPurgeTestPool(t, "r2_retire")
	ctx := context.Background()
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := pool.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("%v\n%s", err, sql)
		}
	}
	exec(`
CREATE TABLE accounts(id bigint PRIMARY KEY);
CREATE TABLE connections(id bigint PRIMARY KEY, account_id bigint NOT NULL, kind text NOT NULL);
CREATE TABLE streams(id bigint PRIMARY KEY, local_timezone text);
CREATE TABLE recordings(id bigint PRIMARY KEY, account_id bigint NOT NULL, stream_id bigint, cron_timezone text NOT NULL);
CREATE TABLE recording_clips(id bigint PRIMARY KEY, recording_id bigint NOT NULL, recording_job_id bigint, bucket text, object_key text, size_bytes bigint,
  sha256 text, display_path text, clip_start_at timestamptz, clip_end_at timestamptz, created_at timestamptz NOT NULL, purged_at timestamptz);
CREATE TABLE recording_collation_hours(id bigint PRIMARY KEY, recording_id bigint NOT NULL, manifest jsonb NOT NULL DEFAULT '{}');
CREATE TABLE recording_collation_outputs(id bigint PRIMARY KEY, collation_hour_id bigint NOT NULL REFERENCES recording_collation_hours(id), object_key text,
  nas_relative_path text, size_bytes bigint, sha256 text, start_at timestamptz, end_at timestamptz, source_clip_ids bigint[], created_at timestamptz NOT NULL);
CREATE TABLE recording_collation_output_acks(output_id bigint NOT NULL REFERENCES recording_collation_outputs(id), connection_id bigint NOT NULL);
CREATE TABLE nas_inventory_files(connection_id bigint NOT NULL, clip_id bigint NOT NULL, relative_path text, state text NOT NULL); -- no FK, as in production
CREATE TABLE nas_inventory_unmatched_files(connection_id bigint NOT NULL, relative_path text NOT NULL, state text NOT NULL);
CREATE TABLE nas_restore_requests(id bigserial PRIMARY KEY, clip_id bigint NOT NULL, state text NOT NULL);
CREATE TABLE recording_joined_source_snapshots(id bigserial PRIMARY KEY, clip_id bigint NOT NULL);
CREATE TABLE recording_joined_batches(id bigint PRIMARY KEY, state text NOT NULL);
CREATE TABLE recording_joined_snapshot_scopes(batch_record_id bigint NOT NULL, recording_id bigint NOT NULL, recording_job_id bigint NOT NULL, high_water_clip_id bigint NOT NULL);
CREATE TABLE recording_joined_dry_run_scopes(recording_id bigint NOT NULL, recording_job_id bigint NOT NULL, high_water_clip_id bigint NOT NULL);
CREATE TABLE recording_joined_artifacts(id bigserial PRIMARY KEY, object_key text NOT NULL);
INSERT INTO accounts VALUES (47),(48);
INSERT INTO connections VALUES (13,47,'nas_pull');
INSERT INTO streams VALUES (3,'Asia/Tokyo'),(4,'America/Los_Angeles'),(5,'Asia/Tokyo');
INSERT INTO recordings VALUES (30,47,3,'UTC'),(40,47,4,'UTC'),(50,47,5,'UTC'),(60,48,3,'UTC');
`)
	for _, m := range []string{"0167_nas_cleanup_dryruns.sql", "0168_nas_cleanup_stage_timings.sql", "0169_nas_cleanup_queue_priority.sql",
		"0170_nas_cleanup_retired_plans.sql", "0171_nas_cleanup_safeguards.sql", "0172_nas_cleanup_safeguards_validate.sql", "0174_r2_object_retirements.sql"} {
		body, err := os.ReadFile(filepath.Join(joinedPurgeMigrationsDir(), m))
		if err != nil {
			t.Fatal(err)
		}
		exec(string(body))
	}
	windows := map[int64]api.Keep28Window{
		3: r2tWindow(t, 3, "Asia/Tokyo", "2026-09-12", "2026-10-10"),
		4: r2tWindow(t, 4, "America/Los_Angeles", "2026-09-12", "2026-10-10"),
		5: r2tWindow(t, 5, "Asia/Tokyo", "2026-09-12", "2026-10-10"),
	}
	store := &fakeR2RetireStore{objects: map[string]int64{}}
	outside := r2tUTC("2026-08-01T00:00:00Z")
	inside := windows[3].Start.Add(time.Hour)
	clip := func(id, rec int64, at time.Time) string {
		key := fmt.Sprintf("managed/acct-47/%d.mp4", id)
		exec(`INSERT INTO recording_clips VALUES ($1,$2,1,'stoarama',$3,$4,$5,$6,$7::timestamptz,$7::timestamptz+interval '1 minute',$7::timestamptz+interval '1 minute',NULL)`,
			id, rec, key, 1000+id, r2tSHA, fmt.Sprintf("p/%d.mp4", id), at.Add(time.Duration(id)*time.Hour))
		store.objects[key] = 1000 + id
		return key
	}
	clip(101, 30, outside) // selected
	clip(102, 30, outside) // NAS copy
	clip(103, 30, inside)  // inside the keep window
	clip(104, 40, outside) // pick "all"
	clip(105, 50, outside) // undecided stream
	clip(106, 30, outside) // paused NAS cleanup plan hold
	clip(107, 30, outside) // joined snapshot
	clip(108, 30, outside) // selected, but its R2 object is already gone
	clip(109, 30, outside) // selected; a retired plan's hold turns permanent during apply
	clip(110, 30, outside) // selected; the NAS copy appears after the plan
	clip(111, 60, outside) // another account
	clip(112, 30, outside) // NAS file at the display path, unmatched
	delete(store.objects, "managed/acct-47/108.mp4")
	exec(`
INSERT INTO nas_inventory_files VALUES (13,102,'p/102.mp4','present');
INSERT INTO nas_inventory_unmatched_files VALUES (13,'p/112.mp4','present');
INSERT INTO recording_joined_source_snapshots(clip_id) VALUES (107);
INSERT INTO nas_cleanup_plans(id,account_id,connection_id,label,report_sha256,manifest,plan_sha256,mode) VALUES
 (1,47,13,'paused-plan',repeat('a',64),'[{"kind":"raw","source_id":106,"source_ids":[106]}]',repeat('b',64),'paused'),
 (2,47,13,'retired-plan',repeat('a',64),'[{"kind":"raw","source_id":109,"source_ids":[109]}]',repeat('c',64),'retired');
INSERT INTO nas_cleanup_items(plan_id,ordinal) VALUES (2,1);
INSERT INTO nas_cleanup_source_holds VALUES (1,106),(2,109);
INSERT INTO recording_collation_hours(id,recording_id) VALUES (900,30);
`)
	output := func(id int64, key string, start time.Time, sources string) {
		exec(`INSERT INTO recording_collation_outputs VALUES ($1,900,$2,$3,$4,$5,$6::timestamptz,$6::timestamptz+interval '1 hour',$7::bigint[],$6::timestamptz+interval '1 hour')`,
			id, key, fmt.Sprintf("j/%d.mp4", id), 5000+id, r2tSHA, start, sources)
		store.objects[key] = 5000 + id
	}
	output(201, "joined/nightly-collation-v2/o/201.mp4", outside.Add(100*time.Hour), "{101}")               // selected
	output(202, "joined/nightly-collation-v2/o/202.mp4", outside.Add(100*time.Hour), "{101}")               // acked on the NAS
	output(203, "joined/best14-20261021/o/203.mp4", outside.Add(100*time.Hour), "{101}")                    // protected prefix
	output(204, "joined/goodplus-20260821-generation-1/o/204.mp4", outside.Add(100*time.Hour), "{103}")     // source inside window
	output(205, "joined/goodplus-20260821-generation-1/o/205.mp4", outside.Add(100*time.Hour), "{101,999}") // unknown source
	exec(`INSERT INTO recording_collation_output_acks VALUES (202,13)`)

	dir := t.TempDir()
	picksPath := filepath.Join(dir, "r2-picks.json")
	writePicks := func(c3 string) {
		t.Helper()
		// Exactly what the picks page saves (server.py adds saved_at/saved_by).
		body := fmt.Sprintf(`{
 "choices": {
  "3": {"choice": %q, "bytes": 123456789, "keep_start_utc": "2026-09-11 11:00:00-04:00", "keep_end_utc": "2026-10-09 11:00:00-04:00"},
  "4": {"choice": "all", "bytes": 5, "keep_start_utc": "2026-09-12 03:00:00-04:00", "keep_end_utc": "2026-10-10 03:00:00-04:00"}
 },
 "saved_at": "2026-11-09T19:00:00Z",
 "saved_by": "owner@example.com"
}`, c3)
		if err := os.WriteFile(picksPath, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	writePicks("none")
	picks, err := loadR2RetirePicks(picksPath)
	if err != nil {
		t.Fatal(err)
	}
	scope := r2RetireScope{AccountID: 47, ConnectionID: 13, Bucket: "stoarama", Grace: 7 * 24 * time.Hour, HasRetirements: true}
	sum, items, err := r2RetirePlanRun(ctx, pool, r2RetirePlanOptions{Picks: picks, Scope: scope, PageSize: 3, Now: time.Now()}, r2tPolicy, windows)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, it := range items {
		got = append(got, fmt.Sprintf("%s:%d", it.Kind, it.SourceID))
	}
	if strings.Join(got, ",") != "collated:201,raw:101,raw:108,raw:109,raw:110" {
		t.Fatalf("selected %v", got)
	}
	wantKept := map[string]map[string]int64{
		"raw":      {"nas_copy": 2, "inside_keep_window": 1, "pick_all": 1, "undecided": 1, "nas_cleanup_hold": 1, "joined_reference": 1},
		"collated": {"nas_copy": 1, "protected_prefix": 1, "inside_keep_window": 1, "unknown_timing": 1},
	}
	for kind, reasons := range wantKept {
		for r, n := range reasons {
			if sum.Kept[kind][r].Objects != n {
				t.Errorf("kept %s %s = %d want %d (all %v)", kind, r, sum.Kept[kind][r].Objects, n, sum.Kept[kind])
			}
		}
	}
	if sum.Scanned["raw"].Objects != 11 || sum.Selected.Objects != 5 || sum.Header.Choices["3"] != "none" || sum.Header.Choices["4"] != "all" {
		t.Fatalf("summary %+v", sum)
	}
	hs := r2RetireHeadSampleRun(ctx, store, items, 100, 2, 1)
	if hs.Checked != 5 || hs.Missing != 1 || hs.Present != 4 {
		t.Fatalf("head sample %+v", hs)
	}
	manifest := filepath.Join(dir, "manifest.jsonl")
	digest, err := writeR2RetireManifest(manifest, sum.Header, items)
	if err != nil {
		t.Fatal(err)
	}
	ledger := filepath.Join(dir, "ledger.jsonl")
	opts := r2RetireApplyOptions{ManifestPath: manifest, ExpectedSHA: digest, LedgerPath: ledger, MaxBytes: 1 << 40, Budget24h: 1 << 40, MaxManifestAge: time.Hour}
	apply := func(o r2RetireApplyOptions) (r2RetireApplySummary, error) {
		return r2RetireApply(ctx, pool, store, o, r2tPolicy, windows)
	}
	assertDeleted := func(want ...string) {
		t.Helper()
		sort.Strings(want)
		got := append([]string(nil), store.deleted...)
		sort.Strings(got)
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Fatalf("deleted %v want %v", got, want)
		}
	}

	// Preconditions refuse before any write.
	bad := opts
	bad.ExpectedSHA = strings.Repeat("e", 64)
	if _, err := apply(bad); err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("wrong digest: %v", err)
	}
	if _, err := r2RetireApply(ctx, pool, store, opts, "other-policy", windows); err == nil {
		t.Fatal("policy change accepted")
	}
	exec(`INSERT INTO nas_cleanup_plans(id,account_id,connection_id,label,report_sha256,manifest,plan_sha256,mode,approval_reason,approved_until) VALUES
 (3,47,13,'enabled-plan',repeat('a',64),'[]',repeat('d',64),'enabled','x',now()+interval '1 hour')`)
	if _, err := apply(opts); err == nil || !strings.Contains(err.Error(), "enabled") {
		t.Fatalf("enabled plan: %v", err)
	}
	exec(`UPDATE nas_cleanup_plans SET mode='paused' WHERE id=3`)
	writePicks("all")
	if _, err := apply(opts); err == nil || !strings.Contains(err.Error(), "pick changed") {
		t.Fatalf("pick change: %v", err)
	}
	writePicks("none")
	for _, age := range []time.Duration{0, -time.Hour, 8 * 24 * time.Hour} {
		o := opts
		o.MaxManifestAge = age
		if _, err := apply(o); err == nil || !strings.Contains(err.Error(), "max-manifest-age") {
			t.Fatalf("max manifest age %s accepted: %v", age, err)
		}
	}
	assertDeleted()

	// Byte cap: only the first item (output 201) fits.
	capped := opts
	capped.MaxBytes = 5201
	s, err := apply(capped)
	if err != nil || s.Retired != 1 || s.LimitReached != "max_bytes" {
		t.Fatalf("capped run %+v %v", s, err)
	}
	assertDeleted("joined/nightly-collation-v2/o/201.mp4")

	// Next run resumes; 101 is retired, then 108 is already missing with no
	// delete intent in the ledger: stop.
	s, err = apply(opts)
	if err == nil || !strings.Contains(err.Error(), "already missing") || s.Retired != 1 || s.AlreadyDone != 1 {
		t.Fatalf("missing-object run %+v %v", s, err)
	}
	assertDeleted("joined/nightly-collation-v2/o/201.mp4", "managed/acct-47/101.mp4")
	var purged101, purged108 bool
	if err := pool.QueryRow(ctx, `SELECT (SELECT purged_at IS NOT NULL FROM recording_clips WHERE id=101),(SELECT purged_at IS NOT NULL FROM recording_clips WHERE id=108)`).Scan(&purged101, &purged108); err != nil || !purged101 || purged108 {
		t.Fatalf("purged_at 101=%t 108=%t err=%v", purged101, purged108, err)
	}

	// A crash after the R2 delete and before the commit leaves an intent line;
	// the next run completes it. Simulate that for 108.
	f, _ := os.OpenFile(ledger, os.O_APPEND|os.O_WRONLY, 0)
	b, _ := json.Marshal(r2RetireLedgerLine{Event: "intent", ManifestSHA256: digest, Kind: "raw", SourceID: 108, ObjectKey: "managed/acct-47/108.mp4"})
	_, _ = f.Write(append(b, '\n'))
	_, _ = f.Write([]byte(`{"event":"torn`)) // torn final line from the crash
	f.Close()

	// 109: its retired plan issues a NAS deletion after our checks; the
	// database trigger vetoes purged_at and nothing is deleted.
	store.beforeHead = func(key string) {
		if key == "managed/acct-47/109.mp4" {
			if _, err := pool.Exec(ctx, `UPDATE nas_cleanup_items SET delete_ever_issued=true WHERE plan_id=2`); err != nil {
				t.Error(err)
			}
		}
	}
	s, err = apply(opts)
	if err == nil || !strings.Contains(err.Error(), "retention protected") || s.Retired != 1 || s.RetiredAbsent != 1 {
		t.Fatalf("trigger run %+v %v", s, err)
	}
	store.beforeHead = nil
	assertDeleted("joined/nightly-collation-v2/o/201.mp4", "managed/acct-47/101.mp4")
	if _, ok := store.objects["managed/acct-47/109.mp4"]; !ok {
		t.Fatal("held object was deleted")
	}

	// 110 gains a NAS copy after the plan. By default that stops the run...
	// 109 is now held: by default that stops the run...
	if _, err := apply(opts); err == nil || !strings.Contains(err.Error(), "no longer eligible") {
		t.Fatalf("ineligible: %v", err)
	}
	// ...and --skip-ineligible keeps both remaining items.
	// ...and --skip-ineligible keeps it. 110 gains a NAS copy after its facts
	// were read (no foreign key, so the row lock cannot block it); the last
	// re-check right before the delete keeps it too.
	store.beforeHead = func(key string) {
		if key == "managed/acct-47/110.mp4" {
			if _, err := pool.Exec(ctx, `INSERT INTO nas_inventory_files VALUES (13,110,'p/110.mp4','present')`); err != nil {
				t.Error(err)
			}
		}
	}
	skip := opts
	skip.SkipIneligible = true
	s, err = apply(skip)
	if err != nil || s.Retired != 0 || s.AlreadyDone != 3 || s.SkippedIneligible["nas_cleanup_hold"] != 1 || s.SkippedIneligible["nas_copy"] != 1 {
		t.Fatalf("skip run %+v %v", s, err)
	}
	store.beforeHead = nil
	assertDeleted("joined/nightly-collation-v2/o/201.mp4", "managed/acct-47/101.mp4")
	if _, ok := store.objects["managed/acct-47/110.mp4"]; !ok {
		t.Fatal("object with a new NAS copy was deleted")
	}

	var rows []string
	r, err := pool.Query(ctx, `SELECT kind||':'||source_id||':'||choice||':'||already_absent||':'||manifest_sha256 FROM r2_object_retirements ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	for r.Next() {
		var s string
		_ = r.Scan(&s)
		rows = append(rows, s)
	}
	r.Close()
	want := []string{"collated:201:none:false:" + digest, "raw:101:none:false:" + digest, "raw:108:none:true:" + digest}
	if strings.Join(rows, " ") != strings.Join(want, " ") {
		t.Fatalf("retirements %v", rows)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM r2_object_retirements`); err == nil {
		t.Fatal("retirements are not append-only")
	}
	if _, err := pool.Exec(ctx, `INSERT INTO r2_object_retirements(kind,source_id,stream_id,bucket,object_key,size_bytes,sha256,reason,choice,manifest_sha256,policy_sha256)
		VALUES ('collated',999,3,'stoarama','joined/best14-20261021/x',1,$1,'x','none',$1,$1)`, r2tSHA); err == nil {
		t.Fatal("database accepted a protected prefix")
	}
	if _, err := pool.Exec(ctx, `INSERT INTO r2_object_retirements(kind,source_id,stream_id,bucket,object_key,size_bytes,sha256,reason,choice,manifest_sha256,policy_sha256)
		VALUES ('raw',998,3,'stoarama','managed/y',1,$1,'x','all',$1,$1)`, r2tSHA); err == nil {
		t.Fatal("database accepted choice=all")
	}

	// Ledger: append-only JSONL with one intent before each delete.
	raw, _ := os.ReadFile(ledger)
	var intents, dones int
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.Contains(line, `"event":"intent"`) {
			intents++
		}
		if strings.Contains(line, `"event":"done"`) {
			dones++
		}
	}
	if intents != 4 || dones != 3 { // 201, 101, 108 (simulated + real), 109 vetoed before its intent
		t.Fatalf("ledger intents=%d dones=%d\n%s", intents, dones, raw)
	}

	// The 24h budget counts every run's retirements from the database.
	budget := opts
	budget.Budget24h = 1
	exec(`DELETE FROM nas_inventory_files WHERE clip_id=110`)
	s, err = apply(budget)
	if err != nil || s.LimitReached != "budget_24h" || s.Used24hBefore != 5201+1101+1108 {
		t.Fatalf("budget run %+v %v", s, err)
	}

	// Two appliers never run at once.
	l, err := openR2RetireLedger(ledger)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := openR2RetireLedger(ledger); err == nil {
		t.Fatal("ledger lock not exclusive")
	}
	l.Close()

	t.Run("NASBackedReservationsAndFreshness", func(t *testing.T) {
		exec(`ALTER TABLE nas_inventory_files ADD COLUMN size_bytes bigint, ADD COLUMN sha256 text,
    ADD COLUMN verified_at timestamptz, ADD COLUMN server_received_at timestamptz,
    ADD COLUMN sidecar_size_bytes bigint, ADD COLUMN sidecar_sha256 text, ADD COLUMN sidecar_relative_path text`)
		for _, m := range []string{"0177_nas_cleanup_reserved_files.sql", "0182_r2_retirement_reservations.sql"} {
			body, err := os.ReadFile(filepath.Join(joinedPurgeMigrationsDir(), m))
			if err != nil {
				t.Fatal(err)
			}
			exec(string(body))
		}
		for _, id := range []int64{301, 302, 303, 304, 305, 306, 307} {
			at := windows[3].Start.Add(time.Hour)
			if id == 302 || id == 306 || id == 307 {
				at = outside
			}
			key := fmt.Sprintf("managed/acct-47/%d.mp4", id)
			exec(`INSERT INTO recording_clips(id,recording_id,recording_job_id,bucket,object_key,size_bytes,sha256,display_path,clip_start_at,clip_end_at,created_at)
    VALUES($1,30,1,'stoarama',$2,100,$3,$4,$5::timestamptz,$5::timestamptz+interval '1 minute',$5)`, id, key, r2tSHA, fmt.Sprintf("p/%d.mp4", id), at)
			store.objects[key] = 100
		}
		exec(`INSERT INTO nas_inventory_files(connection_id,clip_id,relative_path,state,size_bytes,sha256,verified_at,server_received_at,sidecar_size_bytes,sidecar_sha256)
   SELECT 13,id,'p/'||id||'.mp4','present',100,$1,CASE WHEN id=303 THEN now()-interval '2 days' ELSE now() END,now(),100,$1 FROM unnest(ARRAY[301,303,304]) id`, r2tSHA)
		exec(`UPDATE nas_inventory_files SET sidecar_relative_path=relative_path||'.stoarama.json';UPDATE nas_inventory_files SET sha256=repeat('a',64) WHERE clip_id=304`)
		exec(`INSERT INTO nas_cleanup_plans(id,account_id,connection_id,label,report_sha256,manifest,plan_sha256,mode,approval_reason,approved_until) VALUES
   (4,47,13,'unrelated-enabled',repeat('a',64),'[{"kind":"raw","source_id":306,"source_ids":[306]}]',repeat('e',64),'enabled','x',now()+interval '1 hour')`)
		// A retired legacy plan with no item row is uncertain, even with no
		// source hold. The shared reservation view must keep this R2 object.
		exec(`INSERT INTO nas_cleanup_plans(id,account_id,connection_id,label,report_sha256,manifest,plan_sha256,mode)
 VALUES(5,47,13,'legacy-missing-item',repeat('a',64),'[{"kind":"raw","source_id":307}]',repeat('f',64),'retired')`)

		scope.NASBacked = true
		live := map[string]r2.ObjectInfo{}
		for key, n := range store.objects {
			live[key] = r2.ObjectInfo{Key: key, SizeBytes: n, ETag: "e"}
		}
		sum, items, err := r2RetirePlanRun(ctx, pool, r2RetirePlanOptions{Picks: picks, Scope: scope, PageSize: 3, Now: time.Now(), LiveObjects: live}, r2tPolicy, windows)
		if err != nil {
			t.Fatal(err)
		}
		var selected []r2RetireItem
		for _, it := range items {
			if it.SourceID >= 301 && it.Kind == "raw" {
				selected = append(selected, it)
			}
		}
		if len(selected) != 2 || selected[0].SourceID != 301 || selected[1].SourceID != 302 {
			t.Fatalf("selected %+v", selected)
		}
		if sum.Phases.Totals[r2RetireCopy].Objects != 2 {
			t.Fatalf("copy %+v", sum.Phases)
		} // 103 plus 305
		h := sum.Header
		h.Phase = r2RetireNow
		h.Items = 2
		h.Bytes = 200
		manifest := filepath.Join(t.TempDir(), "manifest.jsonl")
		digest, err := writeR2RetireManifest(manifest, h, selected)
		if err != nil {
			t.Fatal(err)
		}
		o := opts
		proofRaw := []byte(fmt.Sprintf(`{"bucket":"stoarama","endpoint_sha256":%q,"created_at":%q,"wrong_etag_rejected":true,"object_preserved":true,"matching_delete_confirmed":true}`, r2tSHA, time.Now().Format(time.RFC3339)))
		proofPath := filepath.Join(t.TempDir(), "proof.json")
		if err := os.WriteFile(proofPath, proofRaw, 0600); err != nil {
			t.Fatal(err)
		}
		proofSHA := sha256.Sum256(proofRaw)
		o.DeleteProofPath = proofPath
		o.DeleteProofSHA256 = hex.EncodeToString(proofSHA[:])
		o.ManifestPath = manifest
		o.ExpectedSHA = digest
		o.LedgerPath = filepath.Join(t.TempDir(), "ledger.jsonl")
		// A NAS proof made stale after planning blocks deletion.
		exec(`UPDATE nas_inventory_files SET verified_at=now()-interval '2 days' WHERE clip_id=301`)
		if _, err := apply(o); err == nil {
			t.Fatal("stale NAS proof accepted")
		}
		exec(`UPDATE nas_inventory_files SET verified_at=now(),sidecar_relative_path='wrong.stoarama.json' WHERE clip_id=301`)
		if _, err := apply(o); err == nil {
			t.Fatal("wrong sidecar path accepted")
		}
		exec(`UPDATE nas_inventory_files SET sidecar_relative_path=relative_path||'.stoarama.json' WHERE clip_id=301`)
		store.beforeHead = func(key string) {
			if key == "managed/acct-47/301.mp4" {
				writePicks("all")
			}
		}
		if _, err := apply(o); err == nil {
			t.Fatal("pick changed during HEAD accepted")
		}
		store.beforeHead = nil
		writePicks("none")
		store.deleteErr = errors.New("injected R2 failure")
		if _, err := apply(o); err == nil {
			t.Fatal("R2 failure accepted")
		}
		var pending int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM r2_retirement_reservations WHERE source_id=301`).Scan(&pending); err != nil || pending != 1 {
			t.Fatal("delete intent did not survive failure")
		}
		if _, err := pool.Exec(ctx, `INSERT INTO nas_cleanup_source_holds VALUES(4,301)`); err == nil {
			t.Fatal("pending R2 key used as NAS recovery")
		}
		store.deleteErr = nil
		result, err := apply(o)
		if err != nil || result.Retired != 2 {
			t.Fatalf("NAS-backed result %+v %v", result, err)
		}
		var n int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM r2_retirement_reservations`).Scan(&n); err != nil || n != 2 {
			t.Fatalf("reservations=%d %v", n, err)
		}
		// A deleted or deletion-pending R2 key can never become NAS recovery.
		if _, err := pool.Exec(ctx, `INSERT INTO nas_cleanup_source_holds VALUES(4,301)`); err == nil {
			t.Fatal("cleanup relied on retired R2 recovery")
		}
		if _, err := pool.Exec(ctx, `INSERT INTO nas_cleanup_plans(account_id,connection_id,label,report_sha256,manifest,plan_sha256)
   VALUES(47,13,'cannot-rely',repeat('a',64),'[{"kind":"raw","source_id":302}]',repeat('f',64))`); err == nil {
			t.Fatal("cleanup admitted retired R2 object")
		}
		if _, err := pool.Exec(ctx, `DELETE FROM r2_retirement_reservations`); err == nil {
			t.Fatal("reservation mutable")
		}
	})

	var a errR2RetireAnomaly
	if !errors.As(r2RetireAnomalyf("x"), &a) {
		t.Fatal("anomaly type")
	}
}

// A picks file exactly as the owner's page writes it, against the real
// embedded keep28 window of stream 3 (Asia/Tokyo, 2026-09-12..2026-10-10).
func TestR2RetirePageStylePicksSelect(t *testing.T) {
	p, err := parseR2RetirePicks([]byte(`{"choices":{
 "3":{"choice":"none","bytes":1234,"keep_start_utc":"2026-09-11 11:00:00-04:00","keep_end_utc":"2026-10-09 11:00:00-04:00"},
 "78":{"choice":"st","bytes":1,"keep_start_utc":"2026-08-19 11:00:00-04:00"}},
 "saved_at":"2026-11-09T19:00:00Z","saved_by":"owner@example.com"}`))
	if err != nil {
		t.Fatal(err)
	}
	_, windows, err := api.Keep28Policy()
	if err != nil {
		t.Fatal(err)
	}
	w := windows[3]
	if got := r2RetireEffectiveChoice(p.Choices[3], true, w); got != "none" {
		t.Fatalf("page-format pick for stream 3 resolved to %q", got)
	}
	c := r2tCandidate("raw", 3, "Asia/Tokyo", w.Start.Add(-time.Hour), w.Start)
	if reason, choice, ok := r2RetireDecide(c, "stoarama", windows, p.Choices); !ok || choice != "none" {
		t.Fatalf("page-format pick selects nothing: %s", reason)
	}
	// A missing window field is undecided: keep everything.
	if got := r2RetireEffectiveChoice(p.Choices[78], true, windows[78]); got != "" {
		t.Fatalf("pick without keep_end_utc resolved to %q", got)
	}
}

func (f *fakeR2RetireStore) DeleteObjectExact(ctx context.Context, key, etag string) error {
	if etag != "e" {
		return errors.New("ETag precondition failed")
	}
	return f.DeleteObject(ctx, key)
}

func TestR2RetireNASBackedChoices(t *testing.T) {
	w := r2tWindow(t, 3, "Asia/Tokyo", "2026-09-12", "2026-10-10")
	windows := map[int64]api.Keep28Window{3: w}
	for _, kind := range []string{"raw", "collated"} {
		for _, choice := range []string{"none", "st", "keep", "all"} {
			for _, outside := range []bool{false, true} {
				for _, nas := range []bool{false, true} {
					at := w.Start.Add(time.Hour)
					if outside {
						at = w.Start.Add(-2 * time.Hour)
					}
					c := r2tCandidate(kind, 3, w.Timezone, at, at.Add(time.Minute))
					c.Facts.NASCopy = nas
					c.Facts.ExactNAS = nas
					picks := map[int64]r2RetirePick{3: {Choice: choice, KeepStartUTC: w.Start.Format(time.RFC3339), KeepEndUTC: w.End.Format(time.RFC3339)}}
					reason, _, got := r2RetireDecideMode(c, "stoarama", windows, picks, true)
					want := choice != "all" && ((outside && !nas) || (!outside && choice == "none" && nas))
					if got != want {
						t.Fatalf("kind=%s choice=%s outside=%t nas=%t got=%t reason=%s", kind, choice, outside, nas, got, reason)
					}
					c.Facts.Held = true
					if _, _, ok := r2RetireDecideMode(c, "stoarama", windows, picks, true); ok {
						t.Fatal("held object eligible")
					}
				}
			}
		}
	}
}
