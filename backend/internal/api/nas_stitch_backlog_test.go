package api

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// The backlog report is a read-only mirror of stitchPlanNext's own predicate.
// This fixture proves the shared SQL fragment and the planner agree exactly on
// which hours are ready, and that the report writes nothing.
//
// Stream 999 / recording 99 is deliberately outside both the pinned keep28
// policy and the cleanup fixture's seeded stream, so the counts here come only
// from this test's own clips.
func TestNASStitchBacklogCountsHoursLikeThePlanner(t *testing.T) {
	f := newCleanupFixture(t, 1)
	stitchMigration(t, f)
	backlogFixtureSchema(t, f)
	if _, err := f.pool.Exec(f.ctx, `INSERT INTO streams VALUES(999,'UTC');INSERT INTO recordings VALUES(99,47,999,'UTC')`); err != nil {
		t.Fatal(err)
	}
	// Three recording-hours on one stream: two fully present in NAS inventory,
	// one with a clip that never arrived.
	base := time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC)
	clips := []struct {
		id      int64
		hour    time.Time
		present bool
	}{
		{id: 501, hour: base, present: true},
		{id: 502, hour: base.Add(10 * time.Minute), present: true},
		{id: 503, hour: base.Add(time.Hour), present: true},
		{id: 504, hour: base.Add(time.Hour + 10*time.Minute), present: false},
		{id: 505, hour: base.Add(2 * time.Hour), present: true},
	}
	for n := range clips {
		c := &clips[n]
		sha := fmt.Sprintf("%064x", c.id)
		rel := fmt.Sprintf("stream/day/%d.mp4", c.id)
		if _, err := f.pool.Exec(f.ctx, `INSERT INTO recording_clips(id,recording_id,object_key,sha256,size_bytes,display_path,clip_start_at,clip_end_at,bucket)
 VALUES($1,99,$2,$3,3,$4,$5,$6,'bucket')`, c.id, "backlog/"+rel, sha, rel, c.hour, c.hour.Add(5*time.Minute)); err != nil {
			t.Fatal(err)
		}
		if !c.present {
			continue
		}
		if _, err := f.pool.Exec(f.ctx, `INSERT INTO nas_inventory_files(connection_id,clip_id,recording_id,relative_path,size_bytes,sha256,state,client_updated_at)
 VALUES(13,$1,99,$2,3,$3,'present',now())`, c.id, rel, sha); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := f.pool.Exec(f.ctx, `INSERT INTO nas_stitch_scopes(id,stream_id,start_at,end_at,reason,approved_until,deletion_approved)
 VALUES(1,999,'2026-09-19T00:00Z','2026-09-21T00:00Z','backlog fixture',now()+interval '1 hour',false)`); err != nil {
		t.Fatal(err)
	}
	// A prior task whose raws are no longer present must not subtract from ready work.
	plan, _ := json.Marshal(nasStitchPlan{Policy: nasStitchPolicy, Revision: nasStitchRevision})
	if _, err := f.pool.Exec(f.ctx, `INSERT INTO nas_stitch_tasks(id,scope_id,recording_id,hour_at,plan,plan_sha256,state)
 VALUES(1,1,99,$1,$2,$3,'held')`, base.Add(time.Hour), string(plan), fmt.Sprintf("%064d", 1)); err != nil {
		t.Fatal(err)
	}
	before := backlogRowCounts(t, f.pool)

	tx, err := f.pool.BeginTx(f.ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(f.ctx, "SET LOCAL statement_timeout = '600s'"); err != nil {
		t.Fatal(err)
	}
	report, err := NASStitchBacklog(f.ctx, backlogQuerier{tx})
	if err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(f.ctx); err != nil {
		t.Fatal(err)
	}
	if report.Source != "scope" {
		t.Fatalf("approved scope was not preferred: %q", report.Source)
	}
	s := backlogStream(t, report, 999)
	if s.WindowFrom.Format(time.RFC3339) != "2026-09-19T00:00:00Z" || s.WindowTo.Format(time.RFC3339) != "2026-09-21T00:00:00Z" {
		t.Fatalf("window differs: %+v", s)
	}
	if s.RecordingHours != 3 || s.ReadyHours != 2 || s.MissingClipsHours != 1 {
		t.Fatalf("hour counts differ: %+v", s)
	}
	if s.TaskHoursByState["held"] != 1 || len(s.TaskHoursByState) != 1 {
		t.Fatalf("task states differ: %+v", s.TaskHoursByState)
	}
	if s.RemainingReadyHours != 2 || s.RemainingReadyInputBytes != 9 {
		t.Fatalf("remaining ready hours: %+v", s)
	}
	// The total aggregates every reported stream, so assert this stream's share
	// rather than an exact global number (other fixture streams also count).
	if report.Total.RecordingHours < 3 || report.Total.ReadyHours < 2 || report.Total.MissingClipsHours < 1 || report.Total.TaskHoursByState["held"] < 1 {
		t.Fatalf("totals do not aggregate this stream: %+v", report.Total)
	}

	// The planner's predicate returns exactly the report's ready hours. The full
	// planner also requires the pinned corpus, which this fixture lacks, so the
	// shared fragment is compared directly.
	plannerReady := backlogPlannerReadyHours(t, f.pool, 999)
	if plannerReady != s.ReadyHours {
		t.Fatalf("backlog and planner disagree on ready hours: %d vs %d", plannerReady, s.ReadyHours)
	}
	if after := backlogRowCounts(t, f.pool); after != before {
		t.Fatalf("backlog report wrote rows: %s -> %s", before, after)
	}

	// Two disjoint scopes must not count the recording hour between them.
	if _, err := f.pool.Exec(f.ctx, `UPDATE nas_stitch_scopes SET paused=true WHERE id=1;
INSERT INTO nas_stitch_scopes(id,stream_id,start_at,end_at,reason,approved_until) VALUES
(2,999,'2026-09-20T00:00Z','2026-09-20T00:05Z','first partial hour',now()+interval '1 hour'),
(3,999,'2026-09-20T02:00Z','2026-09-20T02:30Z','separate hour',now()+interval '1 hour'),
(4,999,'2026-09-20T00:00Z','2026-09-20T00:30Z','overlapping larger hour',now()+interval '1 hour')`); err != nil {
		t.Fatal(err)
	}
	report, err = NASStitchBacklog(f.ctx, f.pool)
	if err != nil {
		t.Fatal(err)
	}
	if s := backlogStream(t, report, 999); s.RecordingHours != 2 || s.ReadyHours != 2 ||
		s.RemainingReadyHours != 2 || s.RemainingReadyInputBytes != 6 || len(s.TaskHoursByState) != 0 {
		t.Fatalf("disjoint scopes differ: %+v", s)
	}
	if _, err := f.pool.Exec(f.ctx, `INSERT INTO nas_cleanup_frozen_streams(plan_id,stream_id) VALUES(999,3)`); err != nil {
		t.Fatal(err)
	}
	report, err = NASStitchBacklog(f.ctx, f.pool)
	if err != nil {
		t.Fatal(err)
	}
	if s := backlogStream(t, report, 999); !report.CleanupFreezeActive || s.ReadyHours != 2 || s.RemainingReadyHours != 0 {
		t.Fatalf("freeze did not block admission while preserving inventory counts: %+v", s)
	}
}

// With no approved scope the report falls back to the pinned keep28 policy
// windows, still read-only. The cleanup fixture's stream 3 is a real pinned
// stream, so its window must match the policy byte for byte.
func TestNASStitchBacklogFallsBackToPinnedKeep28Windows(t *testing.T) {
	f := newCleanupFixture(t, 1)
	stitchMigration(t, f)
	backlogFixtureSchema(t, f)
	sha, windows, err := Keep28Policy()
	if err != nil || len(windows) != cleanupPolicyStreams || sha != cleanupPolicySHA {
		t.Fatalf("policy unavailable: %v %d", err, len(windows))
	}
	report, err := NASStitchBacklog(f.ctx, backlogQuerier{f.pool})
	if err != nil {
		t.Fatal(err)
	}
	if report.Source != "keep28_policy" || len(report.Streams) != cleanupPolicyStreams {
		t.Fatalf("policy fallback: source=%q streams=%d", report.Source, len(report.Streams))
	}
	seen := map[int64]bool{}
	for _, s := range report.Streams {
		if seen[s.StreamID] {
			t.Fatalf("duplicate stream %d", s.StreamID)
		}
		seen[s.StreamID] = true
		w, ok := windows[s.StreamID]
		if !ok || !s.WindowFrom.Equal(w.Start.UTC()) || !s.WindowTo.Equal(w.End.UTC()) {
			t.Fatalf("stream %d window differs: %+v vs %+v", s.StreamID, s, w)
		}
		if s.WindowSource != "keep28_policy" || len(s.ScopeIDs) != 0 {
			t.Fatalf("stream %d marked as scoped: %+v", s.StreamID, s)
		}
	}
	// The fixture's 14 seeded days for stream 3 land inside its own pinned
	// window, so they are counted and none of them is on the NAS.
	if s := backlogStream(t, report, 3); s.RecordingHours == 0 || s.ReadyHours != 0 || s.MissingClipsHours != s.RecordingHours {
		t.Fatalf("pinned stream born on the NAS only: %+v", s)
	}
}

// An approved scope is preferred over the policy fallback, and a paused or
// expired scope is not approved at all.
func TestNASStitchBacklogUsesOnlyApprovedScopes(t *testing.T) {
	f := newCleanupFixture(t, 1)
	stitchMigration(t, f)
	backlogFixtureSchema(t, f)
	if _, err := f.pool.Exec(f.ctx, `INSERT INTO nas_stitch_scopes(id,stream_id,start_at,end_at,reason,approved_until,paused)
 VALUES(1,3,'2026-09-19T00:00Z','2026-09-21T00:00Z','paused',now()+interval '1 hour',true),
       (2,3,'2026-09-19T00:00Z','2026-09-21T00:00Z','expired',now()-interval '1 hour',false)`); err != nil {
		t.Fatal(err)
	}
	report, err := NASStitchBacklog(f.ctx, backlogQuerier{f.pool})
	if err != nil {
		t.Fatal(err)
	}
	if report.Source != "keep28_policy" {
		t.Fatalf("unapproved scopes were used: %q", report.Source)
	}
	// Scope identity (including approved_until) is immutable, so approval is a
	// new scope row rather than an update.
	if _, err = f.pool.Exec(f.ctx, `INSERT INTO nas_stitch_scopes(id,stream_id,start_at,end_at,reason,approved_until)
 VALUES(3,3,'2026-09-19T00:00Z','2026-09-21T00:00Z','newly approved',now()+interval '1 hour')`); err != nil {
		t.Fatal(err)
	}
	report, err = NASStitchBacklog(f.ctx, backlogQuerier{f.pool})
	if err != nil {
		t.Fatal(err)
	}
	if report.Source != "scope" || len(report.Streams) != 1 || report.Streams[0].StreamID != 3 {
		t.Fatalf("single approved scope not used alone: %+v", report)
	}
	if len(report.Streams[0].ScopeIDs) != 1 || report.Streams[0].ScopeIDs[0] != 3 {
		t.Fatalf("paused/expired scopes still reported: %+v", report.Streams[0])
	}
}

// backlogFixtureSchema adds the NAS inventory table the shared cleanup fixture
// does not install, in the shape migration 0105 defines.
func backlogFixtureSchema(t *testing.T, f *cleanupFixture) {
	t.Helper()
	if _, err := f.pool.Exec(f.ctx, `CREATE TABLE nas_inventory_files(connection_id bigint,clip_id bigint,recording_id bigint,
 relative_path text,size_bytes bigint,sha256 text,state text,client_updated_at timestamptz)`); err != nil {
		t.Fatal(err)
	}
}

func backlogStream(t *testing.T, report NASStitchBacklogReport, streamID int64) NASStitchBacklogStream {
	t.Helper()
	for _, s := range report.Streams {
		if s.StreamID == streamID {
			return s
		}
	}
	t.Fatalf("stream %d missing from report (%d streams)", streamID, len(report.Streams))
	return NASStitchBacklogStream{}
}

func backlogPlannerReadyHours(t *testing.T, pool *pgxpool.Pool, streamID int64) int {
	t.Helper()
	var ready int
	if err := pool.QueryRow(t.Context(), `
SELECT count(DISTINCT date_trunc('hour',c.clip_start_at))
 FROM nas_stitch_scopes s JOIN recordings r ON r.stream_id=s.stream_id AND r.account_id=47
 JOIN recording_clips c ON c.recording_id=r.id AND c.clip_start_at>=s.start_at AND c.clip_end_at<=s.end_at
 JOIN nas_inventory_files n ON n.clip_id=c.id AND n.connection_id=13 AND n.state='present' AND n.sha256=c.sha256 AND n.size_bytes=c.size_bytes
 WHERE NOT s.paused AND s.approved_until>now() AND s.stream_id=$1
 AND `+nasStitchHourPresentSQL, streamID).Scan(&ready); err != nil {
		t.Fatal(err)
	}
	return ready
}

type backlogQuerier struct {
	q interface {
		Query(context.Context, string, ...any) (pgx.Rows, error)
	}
}

func (b backlogQuerier) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return b.q.Query(ctx, sql, args...)
}

func backlogRowCounts(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	var counts string
	if err := pool.QueryRow(context.Background(), `SELECT concat(
 (SELECT count(*) FROM nas_stitch_tasks),'/',
 (SELECT count(*) FROM nas_stitch_scopes),'/',
 (SELECT count(*) FROM nas_inventory_files),'/',
 (SELECT count(*) FROM recording_clips),'/',
 (SELECT count(*) FROM nas_stitch_outputs))`).Scan(&counts); err != nil {
		t.Fatal(err)
	}
	return counts
}
