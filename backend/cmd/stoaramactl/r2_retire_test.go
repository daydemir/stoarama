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
		{"ends exactly at keep start", w.Start.Add(-time.Minute), w.Start, "nas_absence_unproven"},
		{"ends 1us after keep start", w.Start.Add(-time.Minute), w.Start.Add(time.Microsecond), "inside_keep_window"},
		{"starts exactly at keep end", w.End, w.End.Add(time.Minute), "nas_absence_unproven"},
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
		{"no NAS history", func(*r2RetireCandidate, map[int64]r2RetirePick) {}, "nas_absence_unproven"},
		{"undecided stream", func(c *r2RetireCandidate, _ map[int64]r2RetirePick) { c.StreamID = 5 }, "undecided"},
		{"pick all", func(_ *r2RetireCandidate, p map[int64]r2RetirePick) { p[3] = pk("all") }, "pick_all"},
		{"pick none", func(_ *r2RetireCandidate, p map[int64]r2RetirePick) { p[3] = pk("none") }, "nas_absence_unproven"},
		{"pick st", func(_ *r2RetireCandidate, p map[int64]r2RetirePick) { p[3] = pk("st") }, "nas_absence_unproven"},
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
		}, "nas_absence_unproven"},
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
	if len(items) != 0 || sum.Kept["raw"]["nas_absence_unproven"].Objects != 4 || sum.Kept["collated"]["nas_absence_unproven"].Objects != 1 {
		t.Fatalf("no-history rows were eligible: %+v / %+v", items, sum.Kept)
	}
	// Previously generated manifests must also be revoked at apply time.
	var oldItems []r2RetireItem
	for _, kind := range []string{"raw", "collated"} {
		id := int64(101)
		if kind == "collated" {
			id = 201
		}
		c, err := r2RetireLoadOne(ctx, pool, kind, id, scope, r2RetireSets{})
		if err != nil {
			t.Fatal(err)
		}
		oldItems = append(oldItems, r2RetireItem{r2RetireCandidate: c, Choice: "none", Reason: r2RetireReason})
	}
	manifest := filepath.Join(dir, "old-manifest.jsonl")
	h := sum.Header
	h.Items, h.Bytes = 2, oldItems[0].SizeBytes+oldItems[1].SizeBytes
	digest, err := writeR2RetireManifest(manifest, h, oldItems)
	if err != nil {
		t.Fatal(err)
	}
	opts := r2RetireApplyOptions{ManifestPath: manifest, ExpectedSHA: digest, LedgerPath: filepath.Join(dir, "ledger.jsonl"), MaxBytes: 1 << 40, Budget24h: 1 << 40, MaxManifestAge: time.Hour}
	apply := func(o r2RetireApplyOptions) (r2RetireApplySummary, error) {
		return r2RetireApply(ctx, pool, store, o, r2tPolicy, windows)
	}
	bad := opts
	bad.ExpectedSHA = strings.Repeat("e", 64)
	if _, err := apply(bad); err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("wrong digest: %v", err)
	}
	if _, err := r2RetireApply(ctx, pool, store, opts, "other-policy", windows); err == nil {
		t.Fatal("policy change accepted")
	}
	for _, age := range []time.Duration{0, -time.Hour, 8 * 24 * time.Hour} {
		bad := opts
		bad.MaxManifestAge = age
		if _, err := apply(bad); err == nil || !strings.Contains(err.Error(), "max-manifest-age") {
			t.Fatalf("manifest age: %v", err)
		}
	}

	if _, err := apply(opts); err == nil || !strings.Contains(err.Error(), "nas_absence_unproven") {
		t.Fatalf("old manifest accepted: %v", err)
	}
	if len(store.deleted) != 0 {
		t.Fatal("unproven absence caused deletion")
	}

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

		exec(`INSERT INTO nas_cleanup_items(plan_id,ordinal,state,recovery,delete_ever_issued)
 VALUES(4,1,'deleted','r2_sha_verified',true)`)
		// Stale collated presence must veto at both catalog and last recheck,
		// even though exact presence has aged out. No-history 302 stays blocked.
		exec(`INSERT INTO nas_inventory_files(connection_id,clip_id,relative_path,state,size_bytes,sha256,verified_at,server_received_at)
 VALUES(13,999,'joined/j/201.mp4','present',5201,$1,now()-interval '2 days',now())`, r2tSHA)
		for _, kind := range []string{"raw", "collated"} {
			id := int64(302)
			if kind == "collated" {
				id = 201
			}
			c, err := r2RetireLoadOne(ctx, pool, kind, id, r2RetireScope{AccountID: 47, ConnectionID: 13, Bucket: "stoarama", Grace: 7 * 24 * time.Hour, HasRetirements: true, NASBacked: true}, r2RetireSets{})
			if err != nil {
				t.Fatal(err)
			}
			reason, _, ok := r2RetireDecideMode(c, "stoarama", windows, picks.Choices, true)
			want := "nas_absence_unproven"
			if kind == "collated" {
				want = "after_nas_cleanup"
			}
			if ok || reason != want {
				t.Fatalf("%s: %s eligible=%t", kind, reason, ok)
			}
			if kind == "collated" {
				nas, err := r2RetireNASCopyNow(ctx, pool, kind, id, 13)
				if err != nil || !nas {
					t.Fatalf("stale collated presence lost: %t %v", nas, err)
				}
			}
		}

		// Even an explicit missing row has no accepted exact-path absence
		// attestation: neither missing rows nor a lack of rows grants authority.
		exec(`INSERT INTO nas_inventory_files(connection_id,clip_id,relative_path,state) VALUES(13,302,'p/302.mp4','missing')`)

		scope.NASBacked = true
		live := map[string]r2.ObjectInfo{}
		for key, n := range store.objects {
			live[key] = r2.ObjectInfo{Key: key, SizeBytes: n, ETag: "e"}
		}
		sum, items, err := r2RetirePlanRun(ctx, pool, r2RetirePlanOptions{Picks: picks, Scope: scope, PageSize: 3, Now: time.Now(), LiveObjects: live}, r2tPolicy, windows)
		if err != nil {
			t.Fatal(err)
		}
		if sum.Kept["raw"]["nas_absence_unproven"].Objects == 0 || sum.Kept["collated"]["after_nas_cleanup"].Objects == 0 || sum.Kept["raw"]["nas_cleanup_hold"].Objects < 2 {
			t.Fatalf("absence/presence/receipted hold classifications lost: %+v", sum.Kept)
		}
		for _, it := range items {
			if it.Reason == r2RetireReason {
				t.Fatal("unproven phase 1 item")
			}
		}

		var selected []r2RetireItem
		for _, it := range items {
			if it.SourceID >= 301 && it.Kind == "raw" {
				selected = append(selected, it)
			}
		}
		if len(selected) != 1 || selected[0].SourceID != 301 {
			t.Fatalf("selected %+v", selected)
		}
		if sum.Phases.Totals[r2RetireCopy].Objects != 2 {
			t.Fatalf("copy %+v", sum.Phases)
		} // 103 plus 305
		h := sum.Header
		h.Phase = r2RetireNow
		h.Items = 1
		h.Bytes = 100
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
		exec(`CREATE FUNCTION fail_retirement_commit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected commit failure'; END $$;
 CREATE CONSTRAINT TRIGGER fail_commit AFTER INSERT ON r2_object_retirements DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION fail_retirement_commit()`)
		if _, err := apply(o); err == nil || !strings.Contains(err.Error(), "commit_failed") {
			t.Fatalf("commit failure: %v", err)
		}
		if _, present := store.objects["managed/acct-47/301.mp4"]; present {
			t.Fatal("test did not delete R2 object")
		}
		var used int64
		if err := r2RetireUsedBudget(ctx, pool, &used); err != nil || used != 100 {
			t.Fatalf("unresolved budget=%d %v", used, err)
		}
		// Reservations never age out without a receipt, even beyond 24h.
		exec(`INSERT INTO r2_retirement_reservations(kind,source_id,bucket,object_key,size_bytes,sha256,manifest_sha256,created_at)
 VALUES('raw',999,'stoarama','managed/unresolved',200,repeat('a',64),repeat('b',64),now()-interval '2 days')`)
		limited := o
		limited.Budget24h = 399
		if result, err := apply(limited); err != nil || result.LimitReached != "budget_24h" || result.Used24hBefore != 300 {
			t.Fatalf("restart budget: %+v %v", result, err)
		}
		exec(`DROP TRIGGER fail_commit ON r2_object_retirements`)

		result, err := apply(o)
		if err != nil || result.Retired != 1 {
			t.Fatalf("NAS-backed result %+v %v", result, err)
		}
		if err := r2RetireUsedBudget(ctx, pool, &used); err != nil || used != 300 {
			t.Fatalf("resolved reservation double charged: %d %v", used, err)
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
   VALUES(47,13,'cannot-rely',repeat('a',64),'[{"kind":"raw","source_id":301}]',repeat('f',64))`); err == nil {
			t.Fatal("cleanup admitted retired R2 object")
		}
		if _, err := pool.Exec(ctx, `DELETE FROM r2_retirement_reservations`); err == nil {
			t.Fatal("reservation mutable")
		}
	})

	t.Run("ListingProofAndOwnerHoldRelease", func(t *testing.T) {
		exec(`ALTER TABLE connections ADD COLUMN last_cursor_id bigint NOT NULL DEFAULT 9999999;
 ALTER TABLE recording_clips ADD COLUMN released_at timestamptz;
 ALTER TABLE nas_inventory_files ADD COLUMN file_mtime_ns bigint,ADD COLUMN file_ctime_ns bigint,ADD COLUMN file_inode bigint,ADD COLUMN file_device bigint;
 ALTER TABLE nas_inventory_unmatched_files ADD COLUMN server_received_at timestamptz NOT NULL DEFAULT now();
 ALTER TABLE recording_collation_output_acks ADD COLUMN verified_at timestamptz NOT NULL DEFAULT now();`)
		for _, name := range []string{"0173_nas_cleanup_throughput.sql", "0183_nas_listing_proof.sql", "0184_r2_recovery_hold_releases.sql"} {
			raw, err := os.ReadFile(filepath.Join(joinedPurgeMigrationsDir(), name))
			if err != nil {
				t.Fatal(err)
			}
			exec(string(raw))
		}
		exec(`UPDATE connections SET nas_listing_cursor_at=now()-interval '2 hours';UPDATE recording_clips SET released_at=created_at`)
		for n, id := range []int64{401, 402, 403, 404, 405, 406, 296368} {
			at := outside
			rec := int64(30)
			state := "deleted"
			outcome := "deleted"
			if id == 402 {
				at = inside
			}
			if id == 405 {
				rec = 40
			}
			if id == 403 {
				state = "unverified"
				outcome = "unverified"
			}
			if id == 404 {
				state = "unverified"
				outcome = "media_deleted_sidecar_failed"
			}
			clip(id, rec, at)
			c, err := r2RetireLoadOne(ctx, pool, "raw", id, r2RetireScope{AccountID: 47, ConnectionID: 13, Bucket: "stoarama", Grace: 7 * 24 * time.Hour, HasRetirements: true, NASBacked: true}, r2RetireSets{})
			if err != nil {
				t.Fatal(err)
			}
			item, _ := json.Marshal(c)
			exec(`INSERT INTO nas_cleanup_plans(id,account_id,connection_id,label,report_sha256,manifest,plan_sha256,mode) VALUES($1,47,13,$2,repeat('a',64),jsonb_build_array($3::jsonb),repeat('b',64),'retired')`, 1400+n, fmt.Sprint("release-", n), item)
			exec(`INSERT INTO nas_cleanup_items(id,plan_id,ordinal,state,delete_ever_issued,recovery,item,outcome) VALUES($1,$2,1,$3,true,'r2_sha_verified',$4,$5::jsonb)`, 2400+n, 1400+n, state, item, fmt.Sprintf(`{"outcome":%q}`, outcome))
			exec(`INSERT INTO nas_cleanup_source_holds VALUES($1,$2)`, 1400+n, id)
		}
		// 406 has no receipt authority; only listing proof can authorize it.
		exec(`UPDATE nas_cleanup_items SET recovery='unchecked' WHERE id=2405`)
		picks := r2RetirePicks{Path: filepath.Join(t.TempDir(), "picks.json"), Choices: map[int64]r2RetirePick{3: {Choice: "none", KeepStartUTC: windows[3].Start.Format(time.RFC3339), KeepEndUTC: windows[3].End.Format(time.RFC3339)}, 4: {Choice: "all", KeepStartUTC: windows[4].Start.Format(time.RFC3339), KeepEndUTC: windows[4].End.Format(time.RFC3339)}}}
		raw, _ := json.Marshal(map[string]any{"choices": map[string]r2RetirePick{"3": picks.Choices[3], "4": picks.Choices[4]}})
		if err := os.WriteFile(picks.Path, raw, 0600); err != nil {
			t.Fatal(err)
		}
		picks, err = loadR2RetirePicks(picks.Path)
		if err != nil {
			t.Fatal(err)
		}
		m, err := r2HoldReleasePlan(ctx, pool, picks, r2tPolicy, windows)
		if err != nil {
			t.Fatal(err)
		}
		if len(m.Items) != 1 || m.Items[0].Candidate.SourceID != 401 {
			t.Fatalf("unsafe release set %+v", m.Items)
		}
		// Both the CLI and direct inserts must take every plan fence before
		// touching an item. Hold the last fence and prove earlier rows remain
		// available while release waits, rather than allowing a child/parent
		// inversion against a receipt or operator control.
		for _, direct := range []bool{false, true} {
			t.Run(fmt.Sprintf("HoldReleaseLockOrder/direct=%t", direct), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
				defer cancel()
				blocker, err := pool.Begin(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer blocker.Rollback(context.Background())
				if _, err := blocker.Exec(ctx, `SELECT pg_advisory_xact_lock(7343226100001406)`); err != nil {
					t.Fatal(err)
				}
				worker, err := pool.Begin(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer worker.Rollback(context.Background())
				workerPID := worker.Conn().PgConn().PID()
				done := make(chan error, 1)
				go func() {
					if direct {
						authority, _ := json.Marshal(m.Items[0])
						_, err := worker.Exec(ctx, `INSERT INTO nas_cleanup_r2_releases(item_id,manifest_sha256,picks_sha256,policy_sha256,authority,reason) VALUES($1,$2,$3,$4,$5,'lock probe')`, m.Items[0].ItemID, r2tSHA, picks.SHA256, r2tPolicy, authority)
						done <- err
					} else {
						done <- applyR2HoldReleases(ctx, worker, m, picks, r2tPolicy, r2tSHA, "lock probe", windows)
					}
				}()
				for {
					var blocked bool
					if err := pool.QueryRow(ctx, `SELECT $1::integer=ANY(pg_blocking_pids($2))`, blocker.Conn().PgConn().PID(), workerPID).Scan(&blocked); err != nil {
						t.Fatal(err)
					}
					if blocked {
						break
					}
					select {
					case err := <-done:
						t.Fatalf("release bypassed plan fence: %v", err)
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					case <-time.After(time.Millisecond):
					}
				}
				if _, err := blocker.Exec(ctx, `SELECT id FROM nas_cleanup_plans WHERE id=1400 FOR UPDATE NOWAIT; SELECT id FROM nas_cleanup_items WHERE id=2400 FOR UPDATE NOWAIT`); err != nil {
					t.Fatal("release locked a row before all plan fences", err)
				}
				if err := blocker.Rollback(ctx); err != nil {
					t.Fatal(err)
				}
				if err := <-done; err != nil {
					t.Fatal(err)
				}
			})
		}
		// A changed choice aborts before recording authority.
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		changed := picks
		changed.SHA256 = strings.Repeat("a", 64)
		if err := applyR2HoldReleases(ctx, tx, m, changed, r2tPolicy, r2tSHA, "test", windows); err == nil {
			t.Fatal("changed picks accepted")
		}
		tx.Rollback(ctx)
		tx, err = pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := applyR2HoldReleases(ctx, tx, m, picks, r2tPolicy, r2tSHA, "owner selected none", windows); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		var held bool
		if err := pool.QueryRow(ctx, `SELECT nas_cleanup_clip_held(401)`).Scan(&held); err != nil || held {
			t.Fatal("released source still held", err)
		}
		if _, err := pool.Exec(ctx, `INSERT INTO nas_cleanup_r2_releases(item_id,manifest_sha256,picks_sha256,policy_sha256,authority,reason) VALUES(2406,$1,$1,$1,'{}','test')`, r2tSHA); err == nil {
			t.Fatal("incident identity released")
		}
		if _, err := pool.Exec(ctx, `DELETE FROM nas_cleanup_r2_releases`); err == nil {
			t.Fatal("authority mutable")
		}
		scope := r2RetireScope{AccountID: 47, ConnectionID: 13, Bucket: "stoarama", Grace: 7 * 24 * time.Hour, HasRetirements: true, NASBacked: true}
		sets, err := loadR2RetireSets(ctx, pool, "", true)
		if err != nil {
			t.Fatal(err)
		}
		c, err := r2RetireLoadOne(ctx, pool, "raw", 401, scope, sets)
		if err != nil {
			t.Fatal(err)
		}
		if reason, _, ok := r2RetireDecideForScope(c, scope, windows, picks.Choices); !ok {
			t.Fatal("receipt release did not enable retirement", reason)
		}
		// A replayed receipt must never look newer than later NAS presence.
		exec(`INSERT INTO nas_inventory_files(connection_id,clip_id,relative_path,state,size_bytes,sha256,verified_at,server_received_at) VALUES(13,401,'p/401.mp4','present',1401,$1,now(),now())`, r2tSHA)
		exec(`UPDATE nas_cleanup_items SET updated_at=now()+interval '1 day' WHERE id=2400`)
		c, err = r2RetireLoadOne(ctx, pool, "raw", 401, scope, sets)
		if err != nil || !c.Facts.NASCopy {
			t.Fatal("replayed old receipt masked later presence", err)
		}
		// Delivery watermarks alone cannot prove absence, even with a fresh
		// complete listing. Cleanup receipts remain independent authority.
		clip(407, 30, outside)
		exec(`UPDATE recording_clips SET released_at=created_at WHERE id=407;
 INSERT INTO nas_listing_generations(connection_id,generation,started_at,completed_at,folders) VALUES(13,repeat('a',32),now()-interval '1 hour',now(),ARRAY['.','p']);`)
		sets, err = loadR2RetireSets(ctx, pool, "", true)
		if err != nil {
			t.Fatal(err)
		}
		c, err = r2RetireLoadOne(ctx, pool, "raw", 407, scope, sets)
		if err != nil || c.Facts.NASAbsent {
			t.Fatal("unfenced listing accepted as absence", err)
		}
		exec(`UPDATE recording_clips SET released_at=now() WHERE id=407`)
		c, err = r2RetireLoadOne(ctx, pool, "raw", 407, scope, sets)
		if err != nil || c.Facts.NASAbsent {
			t.Fatal("write after listing start accepted as absence", err)
		}
		exec(`UPDATE recording_clips SET released_at=created_at WHERE id=407`)
		exec(`INSERT INTO nas_listing_folders(connection_id,generation,folder) VALUES(13,repeat('a',32),'p');INSERT INTO nas_listing_files VALUES(13,repeat('a',32),'p','p/407.mp4',0,1,2,3,4)`)
		c, err = r2RetireLoadOne(ctx, pool, "raw", 407, scope, sets)
		if err != nil || c.Facts.NASAbsent || !c.Facts.ListingPresence {
			t.Fatal("zero-size presence failed veto", err)
		}
		exec(`UPDATE connections SET nas_listing_cursor_at=now()`)
		c, err = r2RetireLoadOne(ctx, pool, "raw", 401, scope, sets)
		if err != nil {
			t.Fatal(err)
		} // receipt still valid independently.
		exec(`UPDATE nas_listing_generations SET started_at=now()-interval '25 hours'`)
		c, err = r2RetireLoadOne(ctx, pool, "raw", 407, scope, sets)
		if err != nil || c.Facts.NASAbsent {
			t.Fatal("stale listing accepted", err)
		}
		// Fresh hash evidence cannot authorize kept footage after a listing
		// reports a changed/truncated file identity at that same path.
		clip(409, 30, inside)
		exec(`INSERT INTO nas_inventory_files(connection_id,clip_id,relative_path,state,size_bytes,sha256,verified_at,server_received_at,sidecar_relative_path,sidecar_size_bytes,sidecar_sha256,file_mtime_ns,file_ctime_ns,file_inode,file_device) VALUES(13,409,'p/409.mp4','present',1409,$1,now(),now(),'p/409.mp4.stoarama.json',100,$1,11,12,13,14)`, r2tSHA)
		exec(`INSERT INTO nas_listing_generations(connection_id,generation,folders,completed_at) VALUES(13,repeat('b',32),ARRAY['p'],now());INSERT INTO nas_listing_folders(connection_id,generation,folder) VALUES(13,repeat('b',32),'p');INSERT INTO nas_listing_files VALUES(13,repeat('b',32),'p','p/409.mp4',0,11,13,14,12)`)
		c, err = r2RetireLoadOne(ctx, pool, "raw", 409, scope, sets)
		if err != nil || c.Facts.ExactNAS || !c.Facts.ListingPresence {
			t.Fatal("changed listed identity authorized kept retirement", err)
		}
		sets, err = loadR2RetireSets(ctx, pool, "", true)
		if err != nil {
			t.Fatal(err)
		}
		c.Facts.ExactNAS = true
		sets.Evidence.apply(&c)
		if c.Facts.ExactNAS {
			t.Fatal("planning accepted changed listed identity")
		}
		// A known alternate keep28 path is still NAS presence even when the
		// catalog display path is absent, including bare/mismatched metadata.
		exec(`INSERT INTO nas_inventory_files(connection_id,clip_id,relative_path,state,size_bytes,sha256,verified_at,server_received_at) VALUES(13,407,'_V85r_keep28/407.mp4','missing',1407,$1,now(),now())`, r2tSHA)
		exec(`INSERT INTO nas_listing_folders(connection_id,generation,folder) VALUES(13,repeat('b',32),'_V85r_keep28');INSERT INTO nas_listing_files VALUES(13,repeat('b',32),'_V85r_keep28','_V85r_keep28/407.mp4',0,1,2,3,4)`)
		c, err = r2RetireLoadOne(ctx, pool, "raw", 407, scope, sets)
		if err != nil || !c.Facts.ListingPresence || c.Facts.NASAbsent {
			t.Fatal("known keep28 alias lost presence veto", err)
		}
		sets, err = loadR2RetireSets(ctx, pool, "", true)
		if err != nil {
			t.Fatal(err)
		}
		sets.Evidence.apply(&c)
		if !c.Facts.ListingPresence || c.Facts.NASAbsent {
			t.Fatal("planning lost known keep28 alias")
		}
		// Proof age must advance during an apply transaction, including time
		// spent waiting on an R2 HEAD. PostgreSQL now() is frozen at BEGIN.
		clip(410, 30, outside)
		exec(`UPDATE recording_clips SET released_at=created_at WHERE id=410`)
		ageTx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer ageTx.Rollback(ctx)
		if _, err := ageTx.Exec(ctx, `UPDATE nas_listing_generations SET completed_at=NULL;INSERT INTO nas_listing_generations(connection_id,generation,folders,started_at,completed_at) VALUES(13,repeat('c',32),ARRAY['.'],now()-interval '24 hours'+interval '100 milliseconds',now());SELECT pg_sleep(0.2)`); err != nil {
			t.Fatal(err)
		}
		c, err = r2RetireLoadOne(ctx, ageTx, "raw", 410, scope, sets)
		if err != nil || c.Facts.NASAbsent {
			t.Fatal("listing aged out during transaction but remained eligible", err)
		}
		if err := ageTx.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
		// Even if an incident receipt later becomes clean, it stays held forever.
		exec(`UPDATE nas_cleanup_items SET state='deleted',outcome='{"outcome":"deleted"}' WHERE id=2406`)
		m, err = r2HoldReleasePlan(ctx, pool, picks, r2tPolicy, windows)
		if err != nil {
			t.Fatal(err)
		}
		for _, i := range m.Items {
			if i.Candidate.SourceID == 296368 {
				t.Fatal("incident lost permanent protection")
			}
		}
	})

	t.Run("OwnerAcceptedOutsideWindow", func(t *testing.T) {
		body, err := os.ReadFile(filepath.Join(joinedPurgeMigrationsDir(), "0186_owner_accepted_r2_retirement.sql"))
		if err != nil {
			t.Fatal(err)
		}
		exec(string(body))
		writePicks("none")
		picks, err := loadR2RetirePicks(picksPath)
		if err != nil {
			t.Fatal(err)
		}
		scope := r2RetireScope{Class: r2RetireOwnerAccepted, AccountID: 47, ConnectionID: 13, Bucket: "stoarama", Grace: 7 * 24 * time.Hour, HasRetirements: true, NASBacked: true, OwnerPolicy: r2RetireOwnerPolicy(windows, picks.Choices)}
		for _, id := range []int64{701, 702, 703, 704, 705, 706, 707, 708, 709, 711} {
			clip(id, 30, outside)
		}
		exec(`INSERT INTO nas_inventory_files(connection_id,clip_id,relative_path,state) VALUES(13,702,'p/702.mp4','unknown'),(13,703,'p/703.mp4','missing'),(13,711,'p/711.mp4','present')`)
		// A kept cross-catalog alias makes an outside raw key protected.
		output(2704, "managed/acct-47/708.mp4", inside, "{708}")
		output(2701, "joined/nightly-collation-v2/o/2701.mp4", inside, "{707}")
		output(2702, "joined/nightly-collation-v2/o/2702.mp4", outside, "{104}") // own window outside, source stream is all
		output(2703, "joined/nightly-collation-v2/o/2703.mp4", outside, "{701}")
		for n, id := range []int64{704, 705, 706, 711} {
			sets, err := loadR2RetireSets(ctx, pool, "", true, scope.Class)
			if err != nil {
				t.Fatal(err)
			}
			c, err := r2RetireLoadOne(ctx, pool, "raw", id, scope, sets)
			if err != nil {
				t.Fatal(err)
			}
			raw, _ := json.Marshal(c)
			mode, state, outcome := "retired", "deleted", "deleted"
			if id == 704 {
				mode = "paused"
			}
			if id == 706 {
				state = "unverified"
				outcome = "unverified"
			}
			exec(`INSERT INTO nas_cleanup_plans(id,account_id,connection_id,label,report_sha256,manifest,plan_sha256,mode)
             VALUES($1,47,13,$2,repeat('a',64),jsonb_build_array($3::jsonb),repeat('b',64),$4)`, 2700+n, fmt.Sprint("owner-", id), raw, mode)
			exec(`INSERT INTO nas_cleanup_items(id,plan_id,ordinal,state,delete_ever_issued,recovery,item,outcome)
             VALUES($1,$2,1,$3,true,'r2_sha_verified',$4,jsonb_build_object('outcome',$5::text))`, 3700+n, 2700+n, state, raw, outcome)
			exec(`INSERT INTO nas_cleanup_source_holds VALUES($1,$2)`, 2700+n, id)
		}
		check := func(id int64, kind string, want bool) {
			t.Helper()
			sets, err := loadR2RetireSets(ctx, pool, "", true, scope.Class)
			if err != nil {
				t.Fatal(err)
			}
			c, err := r2RetireLoadOne(ctx, pool, kind, id, scope, sets)
			if err != nil {
				t.Fatal(err)
			}
			reason, _, ok := r2RetireDecideForScope(c, scope, windows, picks.Choices)
			if ok != want {
				t.Fatalf("%s/%d eligible=%t want=%t reason=%s facts=%+v", kind, id, ok, want, reason, c.Facts)
			}
		}
		check(701, "raw", true)
		check(702, "raw", false)
		check(703, "raw", true)
		for _, id := range []int64{704, 705, 706, 707, 708, 709} {
			check(id, "raw", id == 709)
		}
		check(2702, "collated", false)
		check(2703, "collated", true)
		releases, err := r2HoldReleasePlan(ctx, pool, picks, r2tPolicy, windows, r2RetireOwnerAccepted)
		if err != nil {
			t.Fatal(err)
		}
		selected := []r2HoldReleaseItem{}
		for _, i := range releases.Items {
			if i.Candidate.SourceID == 704 || i.Candidate.SourceID == 706 {
				t.Fatal("active/uncertain release", i)
			}
			if i.Candidate.SourceID == 705 || i.Candidate.SourceID == 711 {
				selected = append(selected, i)
			}
		}
		if len(selected) != 2 {
			t.Fatalf("verified receipt was not releasable: %+v", releases.Items)
		}
		releases.Items = selected
		releases.Bytes = selected[0].Candidate.SizeBytes + selected[1].Candidate.SizeBytes
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err = applyR2HoldReleases(ctx, tx, releases, picks, r2tPolicy, strings.Repeat("a", 64), "owner-approved outside window", windows); err != nil {
			tx.Rollback(ctx)
			t.Fatal(err)
		}
		if err = tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		check(705, "raw", true)
		check(711, "raw", false) // Releasing a receipt never waives positive NAS inventory.
		var legacyHeld, ownerHeld bool
		if err = pool.QueryRow(ctx, `SELECT nas_cleanup_clip_held(705),nas_cleanup_owner_clip_held(705)`).Scan(&legacyHeld, &ownerHeld); err != nil || !legacyHeld || ownerHeld {
			t.Fatal("class isolation", legacyHeld, ownerHeld, err)
		}
		// An owner reservation alone cannot lend its exception to another
		// purge path. The owner retirement record must be in the same tx.
		exec(`INSERT INTO r2_retirement_reservations(kind,source_id,bucket,object_key,size_bytes,sha256,manifest_sha256,retirement_class)
         SELECT 'raw',id,bucket,object_key,size_bytes,sha256,repeat('c',64),'owner_accepted_outside_window' FROM recording_clips WHERE id=711`)
		if _, err = pool.Exec(ctx, `UPDATE recording_clips SET purged_at=now() WHERE id=711`); err == nil {
			t.Fatal("reservation alone waived a legacy hold")
		}
		// Legacy reservations and direct purge retain their recovery hold.
		if _, err = pool.Exec(ctx, `INSERT INTO r2_retirement_reservations(kind,source_id,bucket,object_key,size_bytes,sha256,manifest_sha256) SELECT 'raw',id,bucket,object_key,size_bytes,sha256,repeat('a',64) FROM recording_clips WHERE id=705`); err == nil {
			t.Fatal("legacy consumed owner release")
		}
		if _, err = pool.Exec(ctx, `UPDATE recording_clips SET purged_at=now() WHERE id=705`); err == nil {
			t.Fatal("unreserved purge consumed release")
		}
		live := map[string]r2.ObjectInfo{}
		for k, n := range store.objects {
			live[k] = r2.ObjectInfo{Key: k, SizeBytes: n, ETag: "e"}
		}
		sum, items, err := r2RetirePlanRun(ctx, pool, r2RetirePlanOptions{Scope: scope, Picks: picks, PageSize: 500, Now: time.Now(), LiveObjects: live}, r2tPolicy, windows)
		if err != nil {
			t.Fatal(err)
		}
		var chosen []r2RetireItem
		for _, i := range items {
			if i.SourceID == 705 || i.SourceID == 709 {
				chosen = append(chosen, i)
			}
		}
		if len(chosen) != 2 {
			t.Fatal("missing selected owner items", chosen)
		}
		sum.Header.Items = 2
		sum.Header.Bytes = chosen[0].SizeBytes + chosen[1].SizeBytes
		path := filepath.Join(t.TempDir(), "owner.jsonl")
		digest, err := writeR2RetireManifest(path, sum.Header, chosen)
		if err != nil {
			t.Fatal(err)
		}
		o := opts
		o.ManifestPath = path
		o.ExpectedSHA = digest
		o.LedgerPath = filepath.Join(t.TempDir(), "ledger.jsonl")
		o.MaxBytes = 100000
		o.Budget24h = 5e12
		o.SkipIneligible = true
		proof := []byte(fmt.Sprintf(`{"bucket":"stoarama","endpoint_sha256":%q,"created_at":%q,"wrong_etag_rejected":true,"object_preserved":true,"matching_delete_confirmed":true}`, r2tSHA, time.Now().Format(time.RFC3339)))
		o.DeleteProofPath = filepath.Join(t.TempDir(), "proof.json")
		os.WriteFile(o.DeleteProofPath, proof, 0600)
		o.DeleteProofSHA256 = r2SHA(proof)
		// A newly observed NAS identity after planning revokes 709.
		exec(`INSERT INTO nas_inventory_files(connection_id,clip_id,relative_path,state) VALUES(13,709,'p/709.mp4','present')`)
		result, err := r2RetireApply(ctx, pool, store, o, r2tPolicy, windows)
		if err != nil || result.Retired != 1 {
			t.Fatalf("owner apply %+v %v", result, err)
		}
		var class string
		if err = pool.QueryRow(ctx, `SELECT retirement_class FROM r2_retirement_reservations WHERE source_id=705`).Scan(&class); err != nil || class != string(scope.Class) {
			t.Fatal(class, err)
		}
		if _, exists := store.objects["managed/acct-47/709.mp4"]; !exists {
			t.Fatal("NAS presence did not revoke deletion")
		}
		o.Budget24h = 5e12 + 1
		if _, err = r2RetireApply(ctx, pool, store, o, r2tPolicy, windows); err == nil {
			t.Fatal("owner budget exceeded 5TB")
		}
	})

	t.Run("DeliveryFenceRequiresPostSettlementScopedListing", func(t *testing.T) {
		raw, err := os.ReadFile(filepath.Join(joinedPurgeMigrationsDir(), "0185_nas_delivery_retirement_fence.sql"))
		if err != nil {
			t.Fatal(err)
		}
		exec(string(raw))
		exec(`INSERT INTO recording_clips(id,recording_id,bucket,object_key,size_bytes,sha256,display_path,clip_start_at,clip_end_at,created_at) VALUES(10000001,30,'stoarama','managed/fenced',1000,$1,'p/fenced.mp4','2026-08-01','2026-08-01 00:01:00','2026-08-01')`, r2tSHA)
		exec(`INSERT INTO recording_collation_outputs VALUES(10000002,900,'joined/nightly-collation-v2/fenced','f/fenced.mp4',2000,$1,'2026-08-01','2026-08-01 01:00:00',ARRAY[10000001]::bigint[],'2026-08-01')`, r2tSHA)
		// Council probe: released raw, cursor-delivered raw and ACKed collated
		// identities all have old watermarks and a complete pre-settlement listing.
		exec(`UPDATE recording_clips SET released_at=created_at WHERE id=10000001`)
		exec(`INSERT INTO recording_clips(id,recording_id,bucket,object_key,size_bytes,sha256,display_path,clip_start_at,clip_end_at,created_at) VALUES(10000004,30,'stoarama','managed/cursor-fenced',1000,$1,'p/cursor-fenced.mp4','2026-08-01','2026-08-01 00:01:00','2026-08-01')`, r2tSHA)
		exec(`UPDATE connections SET last_cursor_id=10000004,nas_listing_cursor_at=now()-interval '2 hours';
 INSERT INTO recording_collation_output_acks(output_id,connection_id,verified_at) VALUES(10000002,13,now()-interval '2 hours');
 UPDATE nas_listing_generations SET completed_at=NULL;
 INSERT INTO nas_listing_generations(connection_id,generation,started_at,completed_at,folders) VALUES(13,repeat('0',32),now()-interval '1 hour',now(),ARRAY['.','p','f','joined']);`)
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err = api.ActivateNASDeliveryFence(ctx, tx); err != nil {
			t.Fatal(err)
		}
		if err = tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		// Disposable test clock travel; no operator path can bypass expiry in production.
		exec(`SET session_replication_role=replica;UPDATE nas_delivery_retirement_fences SET effective_at=now()-interval '9 days',legacy_expires_at=now()-interval '2 days';RESET session_replication_role`)
		exec(`INSERT INTO nas_delivery_retirement_settlements(connection_id,drain_acknowledgment) VALUES(13,'NAS stopped and active transfers waited')`)
		scope := r2RetireScope{AccountID: 47, ConnectionID: 13, Bucket: "stoarama", Grace: 7 * 24 * time.Hour, HasRetirements: true, NASBacked: true}
		check := func(want bool) {
			t.Helper()
			sets, err := loadR2RetireSets(ctx, pool, "", true)
			if err != nil {
				t.Fatal(err)
			}
			for _, identity := range []struct {
				kind string
				id   int64
			}{{"raw", 10000001}, {"raw", 10000004}, {"collated", 10000002}} {
				c, err := r2RetireLoadOne(ctx, pool, identity.kind, identity.id, scope, sets)
				if err != nil || c.Facts.NASAbsent != want {
					t.Fatal(identity, "live absence", c.Facts.NASAbsent, want, err)
				}
				if reason, _, eligible := r2RetireDecideForScope(c, scope, windows, picks.Choices); eligible != want {
					t.Fatal(identity, "live retirementEligible", eligible, want, reason)
				}
				c.Facts.NASAbsent = false
				sets.Evidence.apply(&c)
				if c.Facts.NASAbsent != want {
					t.Fatal(identity, "bulk absence", c.Facts.NASAbsent, want)
				}
				if reason, _, eligible := r2RetireDecideForScope(c, scope, windows, picks.Choices); eligible != want {
					t.Fatal(identity, "bulk retirementEligible", eligible, want, reason)
				}
			}
		}
		check(false) // completed listing started before durable fence settlement.
		exec(`INSERT INTO nas_listing_generations(connection_id,generation,started_at,completed_at,folders) SELECT 13,repeat('1',32),settled_at,clock_timestamp(),ARRAY['.','p','f','joined'] FROM nas_delivery_retirement_settlements WHERE connection_id=13`)
		check(false) // equality is insufficient: the walk must start strictly later.
		exec(`INSERT INTO nas_listing_generations(connection_id,generation,folders) VALUES(13,repeat('2',32),ARRAY['.','p','f','joined'])`)
		check(false) // a post-settlement partial generation cannot prove absence.
		exec(`INSERT INTO nas_listing_generations(connection_id,generation,folders,completed_at) VALUES(13,repeat('d',32),ARRAY['.'],now())`)
		check(false) // complete but missing the candidate folders.
		exec(`INSERT INTO nas_listing_generations(connection_id,generation,folders,completed_at) VALUES(13,repeat('e',32),ARRAY['.','p','f','joined'],now())`)
		check(true)
		// A kept writer alias must revoke both identities' fence proof.
		exec(`INSERT INTO recording_clips(id,recording_id,bucket,object_key,size_bytes,sha256,display_path,clip_start_at,clip_end_at,created_at) VALUES(10000003,30,'stoarama','managed/kept-alias',1000,$1,'p/fenced.mp4',$2,$2::timestamptz+interval '1 minute','2026-08-01')`, r2tSHA, windows[3].Start)
		sets, err := loadR2RetireSets(ctx, pool, "", true)
		if err != nil {
			t.Fatal(err)
		}
		c, err := r2RetireLoadOne(ctx, pool, "raw", 10000001, scope, sets)
		if err != nil || c.Facts.NASAbsent {
			t.Fatal("kept path writer bypassed", err)
		}
		c.Facts.NASAbsent = false
		sets.Evidence.apply(&c)
		if c.Facts.NASAbsent {
			t.Fatal("bulk kept path writer bypassed")
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
	if reason, choice, ok := r2RetireDecide(c, "stoarama", windows, p.Choices); ok || reason != "nas_absence_unproven" || choice != "none" {
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
					want := !outside && choice == "none" && nas
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

func TestR2RetireInvalidEvidencePathCannotAuthorize(t *testing.T) {
	for _, path := range []string{"", ".", "../p.mp4", "/p.mp4", "a//b.mp4", "a\\b.mp4"} {
		c := r2RetireCandidate{Kind: "raw", SourceID: 1, RelativePath: path, Facts: r2RetireFacts{ExactNAS: true}}
		e := &r2RetireEvidence{ReleaseSchema: true, Receipts: map[string]time.Time{"raw:1": time.Now()}}
		e.apply(&c)
		if c.Facts.ExactNAS || c.Facts.NASAbsent {
			t.Fatalf("invalid path authorized proof %q", path)
		}
	}
}

func TestR2RetirePreviewAllowsAdditionalReceiptsButRechecksEverySelectedItem(t *testing.T) {
	item := r2HoldReleaseItem{ItemID: 1, Candidate: r2RetireItem{r2RetireCandidate: r2RetireCandidate{Kind: "raw", SourceID: 1, SizeBytes: 10}}}
	expected := r2HoldReleaseManifest{Items: []r2HoldReleaseItem{item}, Bytes: 10}
	extra := item
	extra.ItemID = 2
	extra.Candidate.SourceID = 2
	live := r2HoldReleaseManifest{Items: []r2HoldReleaseItem{item, extra}, Bytes: 20}
	if err := validateR2HoldReleaseItems(expected, live); err != nil {
		t.Fatal("new background receipt invalidated reviewed set", err)
	}
	live.Items[0].ReceiptSHA = "changed"
	if err := validateR2HoldReleaseItems(expected, live); err == nil {
		t.Fatal("changed reviewed receipt accepted")
	}
	live.Items = live.Items[1:]
	if err := validateR2HoldReleaseItems(expected, live); err == nil {
		t.Fatal("missing reviewed receipt accepted")
	}
}

func TestR2RetireOwnerAcceptedClass(t *testing.T) {
	w := r2tWindow(t, 3, "Asia/Tokyo", "2026-09-12", "2026-10-10")
	windows := map[int64]api.Keep28Window{3: w}
	picks := map[int64]r2RetirePick{3: {Choice: "none", KeepStartUTC: w.Start.Format(time.RFC3339), KeepEndUTC: w.End.Format(time.RFC3339)}}
	scope := r2RetireScope{Class: r2RetireOwnerAccepted, Bucket: "stoarama", NASBacked: true}
	base := r2tCandidate("raw", 3, w.Timezone, w.Start.Add(-2*time.Hour), w.Start.Add(-time.Hour))
	for name, mutate := range map[string]func(*r2RetireCandidate){
		"clean":       func(c *r2RetireCandidate) {},
		"presence":    func(c *r2RetireCandidate) { c.Facts.NASCopy = true },
		"exact":       func(c *r2RetireCandidate) { c.Facts.ExactNAS = true },
		"listing":     func(c *r2RetireCandidate) { c.Facts.ListingPresence = true },
		"hold":        func(c *r2RetireCandidate) { c.Facts.Held = true },
		"reference":   func(c *r2RetireCandidate) { c.Facts.JoinedRef = true },
		"shared":      func(c *r2RetireCandidate) { c.Facts.SharedKey = true },
		"restore":     func(c *r2RetireCandidate) { c.Facts.RestorePending = true },
		"recent":      func(c *r2RetireCandidate) { c.Facts.Recent = true },
		"kept":        func(c *r2RetireCandidate) { c.StartAt = w.Start; c.EndAt = w.End },
		"kept_source": func(c *r2RetireCandidate) { c.SourceStart = w.Start; c.SourceEnd = w.End },
	} {
		t.Run(name, func(t *testing.T) {
			c := base
			mutate(&c)
			r, _, ok := r2RetireDecideForScope(c, scope, windows, picks)
			if ok != (name == "clean") {
				t.Fatal(r, ok)
			}
			if name == "clean" && r != string(r2RetireOwnerAccepted) {
				t.Fatal(r)
			}
		})
	}
	for _, choice := range []string{"all", ""} {
		p := picks[3]
		p.Choice = choice
		if _, _, ok := r2RetireDecideForScope(base, scope, windows, map[int64]r2RetirePick{3: p}); ok {
			t.Fatal("unapproved choice", choice)
		}
	}
}
