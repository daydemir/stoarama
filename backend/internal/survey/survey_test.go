package survey

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// TestShouldConfirmError covers the cross-run two-strike timing without a DB: a
// first failure (no marker yet) is never confirmed; a marker younger than
// confirmMinGap is not yet confirmed; a marker at least confirmMinGap old is.
func TestShouldConfirmError(t *testing.T) {
	now := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name  string
		prev  surveyState
		found bool
		want  bool
	}{
		{name: "no prior state", prev: surveyState{}, found: false, want: false},
		{name: "row exists but no marker", prev: surveyState{}, found: true, want: false},
		{
			name:  "marker too fresh",
			prev:  surveyState{firstFailureAt: now.Add(-confirmMinGap + time.Second)},
			found: true,
			want:  false,
		},
		{
			name:  "marker exactly at gap confirms",
			prev:  surveyState{firstFailureAt: now.Add(-confirmMinGap)},
			found: true,
			want:  true,
		},
		{
			name:  "marker well past gap confirms",
			prev:  surveyState{firstFailureAt: now.Add(-2 * time.Hour)},
			found: true,
			want:  true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := shouldConfirmError(tc.prev, tc.found, now); got != tc.want {
				t.Fatalf("shouldConfirmError = %t, want %t", got, tc.want)
			}
		})
	}
}

// TestSelectTargetsQueryShape asserts the prioritization + backoff + skip-today
// clauses are present in the query, so a refactor cannot silently drop them.
func TestSelectTargetsQueryShape(t *testing.T) {
	q := selectTargetsQuery
	musts := []string{
		"survey_stream_state",                   // joins failure state
		"source_kind = 'survey'",                // survey frames only
		"consecutive_failures",                  // backoff uses the counter
		"last_attempt_at + LEAST",               // bounded backoff window
		"'error' = ANY(COALESCE(s.tags",         // needs-attention (error tag) first
		"st.stream_id IS NOT NULL) DESC",        // previously-failing first
		"ORDER BY",                              // prioritized ordering
	}
	for _, m := range musts {
		if !strings.Contains(q, m) {
			t.Fatalf("selectTargetsQuery missing %q", m)
		}
	}
	// The old blind ordering must be gone.
	if strings.Contains(q, "ORDER BY id") && !strings.Contains(q, "s.id ASC") {
		t.Fatalf("selectTargetsQuery still uses blind ORDER BY id")
	}
}

// --- DB-backed tests (skip unless STOARAMA_TEST_DATABASE_URL is set) ---

func surveyTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	databaseURL := strings.TrimSpace(os.Getenv("STOARAMA_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL to run DB-backed survey tests")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open admin pool: %v", err)
	}
	schema := fmt.Sprintf("survey_test_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA %s`, schema)); err != nil {
		admin.Close()
		t.Fatalf("create schema: %v", err)
	}
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		_, _ = admin.Exec(ctx, fmt.Sprintf(`DROP SCHEMA %s CASCADE`, schema))
		admin.Close()
		t.Fatalf("parse db url: %v", err)
	}
	if cfg.ConnConfig.RuntimeParams == nil {
		cfg.ConnConfig.RuntimeParams = map[string]string{}
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		_, _ = admin.Exec(ctx, fmt.Sprintf(`DROP SCHEMA %s CASCADE`, schema))
		admin.Close()
		t.Fatalf("open test pool: %v", err)
	}
	// Minimal schema: just the columns SelectTargets and the survey state helpers
	// touch. survey_stream_state DDL is copied verbatim from migration 0075.
	for _, stmt := range []string{
		`CREATE TABLE streams (
			id BIGSERIAL PRIMARY KEY,
			provider TEXT,
			source_url TEXT,
			source_page_url TEXT,
			capture_type TEXT,
			source_family TEXT,
			execution_class TEXT,
			tags TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,
		`CREATE TABLE frames (
			id BIGSERIAL PRIMARY KEY,
			stream_id BIGINT NOT NULL REFERENCES streams(id) ON DELETE CASCADE,
			captured_at TIMESTAMPTZ NOT NULL,
			source_kind TEXT NOT NULL
		)`,
		`CREATE TABLE survey_stream_state (
			stream_id            BIGINT PRIMARY KEY REFERENCES streams(id) ON DELETE CASCADE,
			consecutive_failures INT NOT NULL DEFAULT 0,
			first_failure_at     TIMESTAMPTZ,
			last_failure_at      TIMESTAMPTZ,
			last_attempt_at      TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,
	} {
		if _, err := pool.Exec(ctx, stmt); err != nil {
			pool.Close()
			_, _ = admin.Exec(ctx, fmt.Sprintf(`DROP SCHEMA %s CASCADE`, schema))
			admin.Close()
			t.Fatalf("create test table: %v\n%s", err, stmt)
		}
	}
	t.Cleanup(func() {
		pool.Close()
		_, _ = admin.Exec(ctx, fmt.Sprintf(`DROP SCHEMA %s CASCADE`, schema))
		admin.Close()
	})
	return pool
}

func insertStream(t *testing.T, pool *pgxpool.Pool, tags []string) int64 {
	t.Helper()
	if tags == nil {
		tags = []string{}
	}
	var id int64
	if err := pool.QueryRow(context.Background(),
		`INSERT INTO streams (provider, source_url, capture_type, tags)
		 VALUES ('generic', 'https://example.com/s.m3u8', 'hls', $1) RETURNING id`,
		tags,
	).Scan(&id); err != nil {
		t.Fatalf("insert stream: %v", err)
	}
	return id
}

func insertSurveyFrameOn(t *testing.T, pool *pgxpool.Pool, streamID int64, at time.Time) {
	t.Helper()
	if _, err := pool.Exec(context.Background(),
		`INSERT INTO frames (stream_id, captured_at, source_kind) VALUES ($1, $2, 'survey')`,
		streamID, at,
	); err != nil {
		t.Fatalf("insert survey frame: %v", err)
	}
}

func targetIDs(ts []Target) []int64 {
	ids := make([]int64, len(ts))
	for i, t := range ts {
		ids[i] = t.ID
	}
	return ids
}

func indexOf(ids []int64, id int64) int {
	for i, v := range ids {
		if v == id {
			return i
		}
	}
	return -1
}

// TestSelectTargetsPrioritizesErroredAndNeverSurveyed verifies ordering: error-
// tagged and never-surveyed streams come before an already-surveyed-earlier one,
// and a stream surveyed today is excluded entirely.
func TestSelectTargetsPrioritizesErroredAndNeverSurveyed(t *testing.T) {
	pool := surveyTestPool(t)
	ctx := context.Background()
	yesterday := time.Now().UTC().AddDate(0, 0, -1)

	errored := insertStream(t, pool, []string{"error"})     // needs attention: tagged
	neverSurveyed := insertStream(t, pool, nil)             // needs attention: no frame ever
	surveyedEarlier := insertStream(t, pool, nil)           // lower priority
	insertSurveyFrameOn(t, pool, surveyedEarlier, yesterday)
	doneToday := insertStream(t, pool, nil) // must be excluded
	insertSurveyFrameOn(t, pool, doneToday, time.Now().UTC())

	targets, err := SelectTargets(ctx, pool)
	if err != nil {
		t.Fatalf("SelectTargets: %v", err)
	}
	ids := targetIDs(targets)

	if indexOf(ids, doneToday) != -1 {
		t.Fatalf("stream surveyed today should be excluded, got %v", ids)
	}
	for _, id := range []int64{errored, neverSurveyed, surveyedEarlier} {
		if indexOf(ids, id) == -1 {
			t.Fatalf("expected stream %d in targets %v", id, ids)
		}
	}
	if indexOf(ids, errored) >= indexOf(ids, surveyedEarlier) {
		t.Fatalf("errored stream %d must precede surveyed-earlier %d; order %v", errored, surveyedEarlier, ids)
	}
	if indexOf(ids, neverSurveyed) >= indexOf(ids, surveyedEarlier) {
		t.Fatalf("never-surveyed stream %d must precede surveyed-earlier %d; order %v", neverSurveyed, surveyedEarlier, ids)
	}
}

// TestBackoffSkipsAfterKThenReChecks verifies a chronically-failing stream
// (>= backoffThreshold failures) is skipped while its backoff window is unexpired
// and re-selected once the window has passed.
func TestBackoffSkipsAfterKThenReChecks(t *testing.T) {
	pool := surveyTestPool(t)
	ctx := context.Background()

	backedOff := insertStream(t, pool, []string{"error"})
	// backoffThreshold failures, attempted just now: window = backoffBase, unexpired.
	if _, err := pool.Exec(ctx,
		`INSERT INTO survey_stream_state (stream_id, consecutive_failures, first_failure_at, last_failure_at, last_attempt_at)
		 VALUES ($1, $2, now(), now(), now())`,
		backedOff, backoffThreshold,
	); err != nil {
		t.Fatalf("seed backed-off state: %v", err)
	}
	targets, err := SelectTargets(ctx, pool)
	if err != nil {
		t.Fatalf("SelectTargets: %v", err)
	}
	if indexOf(targetIDs(targets), backedOff) != -1 {
		t.Fatalf("stream in unexpired backoff should be skipped, got %v", targetIDs(targets))
	}

	// Age last_attempt_at past the window (backoffBase for threshold count).
	if _, err := pool.Exec(ctx,
		`UPDATE survey_stream_state SET last_attempt_at = now() - $2::interval - interval '1 minute' WHERE stream_id = $1`,
		backedOff, backoffBase,
	); err != nil {
		t.Fatalf("age attempt: %v", err)
	}
	targets, err = SelectTargets(ctx, pool)
	if err != nil {
		t.Fatalf("SelectTargets after aging: %v", err)
	}
	if indexOf(targetIDs(targets), backedOff) == -1 {
		t.Fatalf("stream past backoff window should be re-checked, got %v", targetIDs(targets))
	}

	// Below the threshold, a failing stream is NOT backed off (re-checked every sweep).
	belowThreshold := insertStream(t, pool, []string{"error"})
	if _, err := pool.Exec(ctx,
		`INSERT INTO survey_stream_state (stream_id, consecutive_failures, first_failure_at, last_failure_at, last_attempt_at)
		 VALUES ($1, $2, now(), now(), now())`,
		belowThreshold, backoffThreshold-1,
	); err != nil {
		t.Fatalf("seed below-threshold state: %v", err)
	}
	targets, err = SelectTargets(ctx, pool)
	if err != nil {
		t.Fatalf("SelectTargets below-threshold: %v", err)
	}
	if indexOf(targetIDs(targets), belowThreshold) == -1 {
		t.Fatalf("below-threshold failing stream should still be selected, got %v", targetIDs(targets))
	}
}

// TestRecordSurveyFailureDoesNotBlockAndConfirmsOnSecondRun verifies the failure
// path is cheap (no worker sleep) and two-strikes across runs: the first failure
// records a marker with no error tag; a later failure past confirmMinGap tags the
// stream. It also asserts recordSurveyFailure returns promptly.
func TestRecordSurveyFailureDoesNotBlockAndConfirmsOnSecondRun(t *testing.T) {
	pool := surveyTestPool(t)
	ctx := context.Background()
	streamID := insertStream(t, pool, nil)

	// First failure: marker only, no tag. Must return without any 5-min sleep.
	start := time.Now()
	prev, found, err := loadSurveyState(ctx, pool, streamID)
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	now1 := time.Now().UTC()
	if confirm := shouldConfirmError(prev, found, now1); confirm {
		t.Fatalf("first failure must not confirm")
	}
	if err := recordSurveyFailure(ctx, pool, streamID, false, now1); err != nil {
		t.Fatalf("record first failure: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 30*time.Second {
		t.Fatalf("failure handling took %s; must not block a worker for the confirmation delay", elapsed)
	}

	var tags []string
	if err := pool.QueryRow(ctx, `SELECT tags FROM streams WHERE id = $1`, streamID).Scan(&tags); err != nil {
		t.Fatalf("read tags: %v", err)
	}
	if contains(tags, ErrorTag) {
		t.Fatalf("first failure must not tag error; tags=%v", tags)
	}
	var cf int
	if err := pool.QueryRow(ctx, `SELECT consecutive_failures FROM survey_stream_state WHERE stream_id = $1`, streamID).Scan(&cf); err != nil {
		t.Fatalf("read state: %v", err)
	}
	if cf != 0 {
		t.Fatalf("first failure consecutive_failures = %d, want 0", cf)
	}

	// Age the marker past confirmMinGap so the next failure confirms.
	if _, err := pool.Exec(ctx,
		`UPDATE survey_stream_state SET first_failure_at = now() - $2::interval - interval '1 minute' WHERE stream_id = $1`,
		streamID, confirmMinGap,
	); err != nil {
		t.Fatalf("age marker: %v", err)
	}
	prev, found, err = loadSurveyState(ctx, pool, streamID)
	if err != nil {
		t.Fatalf("reload state: %v", err)
	}
	now2 := time.Now().UTC()
	if !shouldConfirmError(prev, found, now2) {
		t.Fatalf("second failure past gap must confirm")
	}
	if err := recordSurveyFailure(ctx, pool, streamID, true, now2); err != nil {
		t.Fatalf("record confirmed failure: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT tags FROM streams WHERE id = $1`, streamID).Scan(&tags); err != nil {
		t.Fatalf("read tags after confirm: %v", err)
	}
	if !contains(tags, ErrorTag) {
		t.Fatalf("confirmed failure must tag error; tags=%v", tags)
	}
	if err := pool.QueryRow(ctx, `SELECT consecutive_failures FROM survey_stream_state WHERE stream_id = $1`, streamID).Scan(&cf); err != nil {
		t.Fatalf("read state after confirm: %v", err)
	}
	if cf != 1 {
		t.Fatalf("confirmed failure consecutive_failures = %d, want 1", cf)
	}

	// Recovery clears both the tag and the state row.
	if err := markSurveyHealthy(ctx, pool, streamID); err != nil {
		t.Fatalf("mark healthy: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT tags FROM streams WHERE id = $1`, streamID).Scan(&tags); err != nil {
		t.Fatalf("read tags after recovery: %v", err)
	}
	if contains(tags, ErrorTag) {
		t.Fatalf("recovery must clear error tag; tags=%v", tags)
	}
	var stateRows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM survey_stream_state WHERE stream_id = $1`, streamID).Scan(&stateRows); err != nil {
		t.Fatalf("count state: %v", err)
	}
	if stateRows != 0 {
		t.Fatalf("recovery must delete survey_stream_state row, found %d", stateRows)
	}
}

func contains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}
