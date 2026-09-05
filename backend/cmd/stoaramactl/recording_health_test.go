package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestShouldNotifyHealthIncident(t *testing.T) {
	now := time.Date(2026, 7, 4, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name          string
		incident      healthIncident
		newlyInserted bool
		episodeAt     time.Time
		lastAlertedAt time.Time
		lastAttemptAt *time.Time
		want          bool
	}{
		{"new non-live signal notifies", healthIncident{Signal: signalStoredClipInvalid}, true, now, now, nil, true},
		{"ordinary silent death waits", healthIncident{Signal: signalContinuousSilentDeath}, true, now, time.Time{}, nil, false},
		{"ordinary silent death matures", healthIncident{Signal: signalContinuousSilentDeath}, false, now.Add(-5 * time.Minute), time.Time{}, nil, true},
		{"stale media is immediate", healthIncident{Signal: signalContinuousSilentDeath, Immediate: true}, true, now, time.Time{}, nil, true},
		{"never attempted retries", healthIncident{Signal: signalStoredClipInvalid}, false, now, time.Time{}, nil, true},
		{"recent failed attempt backs off", healthIncident{Signal: signalStoredClipInvalid}, false, now, time.Time{}, ptrTime(now.Add(-time.Minute)), false},
		{"failed attempt retries after backoff", healthIncident{Signal: signalStoredClipInvalid}, false, now, time.Time{}, ptrTime(now.Add(-healthAlertDeliveryRetryBackoff - time.Second)), true},
		{"older than 24h renotifies", healthIncident{Signal: signalStoredClipInvalid}, false, now.Add(-25 * time.Hour), now.Add(-24*time.Hour - time.Second), nil, true},
		{"exactly 24h stays quiet", healthIncident{Signal: signalStoredClipInvalid}, false, now.Add(-25 * time.Hour), now.Add(-24 * time.Hour), nil, false},
		{"recent successful delivery stays quiet", healthIncident{Signal: signalStoredClipInvalid}, false, now.Add(-4 * time.Hour), now.Add(-3 * time.Hour), nil, false},
		{"silent reopen cooldown", healthIncident{Signal: signalContinuousSilentDeath}, false, now.Add(-5 * time.Minute), now.Add(-20 * time.Minute), nil, false},
		{"silent reopen after cooldown", healthIncident{Signal: signalContinuousSilentDeath}, false, now.Add(-5 * time.Minute), now.Add(-31 * time.Minute), nil, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := shouldNotifyHealthIncident(tc.incident, tc.newlyInserted, tc.episodeAt, tc.lastAlertedAt, tc.lastAttemptAt, now); got != tc.want {
				t.Fatalf("shouldNotifyHealthIncident=%v want %v", got, tc.want)
			}
		})
	}
}

func TestDeliverRecordingHealthEmailRejectsUndeliverableProvider(t *testing.T) {
	sent, err := deliverRecordingHealthEmail(context.Background(), nil, config.Config{EmailProvider: "log"}, sampleIncidents())
	if err == nil || sent != 0 {
		t.Fatalf("sent=%d err=%v, want zero and retryable error", sent, err)
	}
}

func TestHealthAlertDeliveryTimestampOnlyAdvancesAfterAcknowledgement(t *testing.T) {
	databaseURL := strings.TrimSpace(os.Getenv("STOARAMA_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL to run DB-backed delivery acknowledgement regression")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema := fmt.Sprintf("health_delivery_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = admin.Exec(ctx, `DROP SCHEMA `+schema+` CASCADE`) }()
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ConnConfig.RuntimeParams == nil {
		cfg.ConnConfig.RuntimeParams = map[string]string{}
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, `
		CREATE TABLE recordings (id BIGINT PRIMARY KEY);
		CREATE TABLE recorder_health_alerts (
		  recording_id BIGINT NOT NULL REFERENCES recordings(id), signal TEXT NOT NULL,
		  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
		  last_alerted_at TIMESTAMPTZ NOT NULL DEFAULT now(), last_delivery_attempt_at TIMESTAMPTZ,
		  episode_started_at TIMESTAMPTZ NOT NULL DEFAULT now(), last_detected_at TIMESTAMPTZ,
		  resolved_at TIMESTAMPTZ,
		  PRIMARY KEY(recording_id,signal));
		INSERT INTO recordings VALUES (1),(2);
	`); err != nil {
		t.Fatal(err)
	}
	inc := healthIncident{RecordingID: 1, Signal: signalStoredClipInvalid}
	inserted, episode, last, attempted, err := upsertHealthAlert(ctx, pool, 1, signalStoredClipInvalid)
	if err != nil || !inserted || !shouldNotifyHealthIncident(inc, inserted, episode, last, attempted, time.Now()) {
		t.Fatalf("first detection inserted=%v last=%v err=%v", inserted, last, err)
	}
	incidents := []healthIncident{{RecordingID: 1, Signal: signalStoredClipInvalid}}
	if err := markHealthAlertsDeliveryAttempted(ctx, pool, incidents); err != nil {
		t.Fatal(err)
	}
	inserted, episode, last, attempted, err = upsertHealthAlert(ctx, pool, 1, signalStoredClipInvalid)
	if err != nil || inserted || shouldNotifyHealthIncident(inc, inserted, episode, last, attempted, time.Now()) {
		t.Fatalf("recent failed attempt must back off inserted=%v last=%v attempted=%v err=%v", inserted, last, attempted, err)
	}
	if _, err := pool.Exec(ctx, `UPDATE recorder_health_alerts SET last_delivery_attempt_at=now()-interval '16 minutes'`); err != nil {
		t.Fatal(err)
	}
	inserted, episode, last, attempted, err = upsertHealthAlert(ctx, pool, 1, signalStoredClipInvalid)
	if err != nil || inserted || !shouldNotifyHealthIncident(inc, inserted, episode, last, attempted, time.Now()) {
		t.Fatalf("unacknowledged detection must retry after backoff inserted=%v last=%v attempted=%v err=%v", inserted, last, attempted, err)
	}
	if err := markHealthAlertsDelivered(ctx, pool, incidents); err != nil {
		t.Fatal(err)
	}
	inserted, episode, last, attempted, err = upsertHealthAlert(ctx, pool, 1, signalStoredClipInvalid)
	if err != nil || shouldNotifyHealthIncident(inc, inserted, episode, last, attempted, time.Now()) {
		t.Fatalf("acknowledged detection must dedup inserted=%v last=%v err=%v", inserted, last, err)
	}
	if _, err := pool.Exec(ctx, `UPDATE recorder_health_alerts SET resolved_at=now()`); err != nil {
		t.Fatal(err)
	}
	inserted, episode, last, attempted, err = upsertHealthAlert(ctx, pool, 1, signalStoredClipInvalid)
	if err != nil || !shouldNotifyHealthIncident(inc, inserted, episode, last, attempted, time.Now()) {
		t.Fatalf("reopened detection must notify inserted=%v last=%v err=%v", inserted, last, err)
	}
	if err := resolveClearedHealthAlerts(ctx, pool, nil, evaluatedHealthSignals(false, false)); err != nil {
		t.Fatal(err)
	}
	var resolved *time.Time
	if err := pool.QueryRow(ctx, `SELECT resolved_at FROM recorder_health_alerts WHERE recording_id=1 AND signal=$1`, signalStoredClipInvalid).Scan(&resolved); err != nil {
		t.Fatal(err)
	}
	if resolved != nil {
		t.Fatal("hourly-only sweep falsely resolved daily media incident")
	}
	if err := resolveClearedHealthAlerts(ctx, pool, nil, evaluatedHealthSignals(true, false)); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT resolved_at FROM recorder_health_alerts WHERE recording_id=1 AND signal=$1`, signalStoredClipInvalid).Scan(&resolved); err != nil || resolved == nil {
		t.Fatalf("media sweep did not resolve cleared media incident: resolved=%v err=%v", resolved, err)
	}

	silent := healthIncident{RecordingID: 2, Signal: signalContinuousSilentDeath}
	inserted, episode, last, attempted, err = upsertHealthAlert(ctx, pool, 2, signalContinuousSilentDeath)
	if err != nil || !inserted || shouldNotifyHealthIncident(silent, inserted, episode, last, attempted, time.Now()) {
		t.Fatalf("first ordinary silence must open without email inserted=%v episode=%v err=%v", inserted, episode, err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE recorder_health_alerts
		SET episode_started_at=now()-interval '5 minutes',last_detected_at=now()-interval '5 minutes'
		WHERE recording_id=2 AND signal=$1
	`, signalContinuousSilentDeath); err != nil {
		t.Fatal(err)
	}
	inserted, episode, last, attempted, err = upsertHealthAlert(ctx, pool, 2, signalContinuousSilentDeath)
	if err != nil || inserted || !shouldNotifyHealthIncident(silent, inserted, episode, last, attempted, time.Now()) {
		t.Fatalf("continuous silence did not mature inserted=%v episode=%v err=%v", inserted, episode, err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE recorder_health_alerts
		SET last_detected_at=now()-interval '13 minutes'
		WHERE recording_id=2 AND signal=$1
	`, signalContinuousSilentDeath); err != nil {
		t.Fatal(err)
	}
	inserted, episode, last, attempted, err = upsertHealthAlert(ctx, pool, 2, signalContinuousSilentDeath)
	if err != nil || inserted || shouldNotifyHealthIncident(silent, inserted, episode, last, attempted, time.Now()) {
		t.Fatalf("discontinuous silence did not reset inserted=%v episode=%v err=%v", inserted, episode, err)
	}
}

func TestRecordingHealthRunLockSerializesSweeps(t *testing.T) {
	databaseURL := strings.TrimSpace(os.Getenv("STOARAMA_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL to run advisory-lock regression")
	}
	pool, err := pgxpool.New(context.Background(), databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	release, err := acquireRecordingHealthRunLock(context.Background(), pool, false)
	if err != nil {
		t.Fatal(err)
	}
	blockedCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if secondRelease, err := acquireRecordingHealthRunLock(blockedCtx, pool, false); err == nil {
		secondRelease()
		release()
		t.Fatal("second concurrent health sweep acquired advisory lock")
	}
	release()
	recoverCtx, recoverCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer recoverCancel()
	thirdRelease, err := acquireRecordingHealthRunLock(recoverCtx, pool, false)
	if err != nil {
		t.Fatalf("lock did not recover after release: %v", err)
	}
	thirdRelease()
}

func TestRecordingHealthRunLockAllowsMediaAndTimelineConcurrently(t *testing.T) {
	databaseURL := strings.TrimSpace(os.Getenv("STOARAMA_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL to run advisory-lock regression")
	}
	pool, err := pgxpool.New(context.Background(), databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	releaseMedia, err := acquireRecordingHealthRunLock(context.Background(), pool, true)
	if err != nil {
		t.Fatal(err)
	}
	defer releaseMedia()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	releaseTimeline, err := acquireRecordingHealthRunLock(ctx, pool, false)
	if err != nil {
		t.Fatalf("media verifier blocked timeline sweep: %v", err)
	}
	releaseTimeline()
}

func TestEvaluatedHealthSignalsAreDisjointByRunClass(t *testing.T) {
	media := evaluatedHealthSignals(true, false)
	if len(media) != 1 || media[0] != signalStoredClipInvalid {
		t.Fatalf("media signals=%v", media)
	}
	for _, signal := range evaluatedHealthSignals(false, false) {
		if signal == signalStoredClipInvalid {
			t.Fatal("timeline sweep evaluates media signal")
		}
	}
	live := evaluatedHealthSignals(false, true)
	wantLive := liveRecordingHealthSignals()
	if fmt.Sprint(live) != fmt.Sprint(wantLive) || len(live) != 5 {
		t.Fatalf("live signals=%v want=%v", live, wantLive)
	}
	driftDetectorFound := false
	for _, detector := range liveRecordingHealthDetectors() {
		if detector.signal == signalClipTimestampDrift {
			driftDetectorFound = true
		}
	}
	if !driftDetectorFound {
		t.Fatal("live detector registry omits timestamp drift")
	}
	for _, signal := range live {
		if signal == signalContinuousCoverageLow || signal == signalStoredClipInvalid {
			t.Fatalf("live sweep evaluates non-live signal %q", signal)
		}
	}
	if got := healthRunClass(false, true); got != "live" {
		t.Fatalf("healthRunClass live=%q", got)
	}
}

func TestCompletedWindowHealthStageRunsOnceAndDoesNotClearSignalsOnTimeout(t *testing.T) {
	if completedWindowHealthTimeout != 4*time.Minute {
		t.Fatalf("completed-window timeout=%s, want 4m above the observed 189s normal runtime", completedWindowHealthTimeout)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	calls := 0
	base := []healthIncident{{RecordingID: 7, Signal: signalContinuousSilentDeath}}
	result := runCompletedWindowHealthStage(ctx, base, func(stageCtx context.Context) ([]healthIncident, error) {
		calls++
		if stageCtx.Err() == nil {
			t.Fatal("completed-window detector did not receive bounded canceled context")
		}
		return nil, context.Canceled
	})

	if calls != 1 {
		t.Fatalf("completed-window detector calls=%d, want exactly one bounded historical pass", calls)
	}
	if len(result.incidents) != 1 || result.incidents[0].Signal != signalContinuousSilentDeath {
		t.Fatalf("incidents=%v, want live incident preserved", result.incidents)
	}
	for _, signal := range result.evaluatedSignals {
		if signal == signalContinuousCoverageLow || signal == signalContinuousOverlap ||
			signal == signalContinuousLongGap || signal == signalContinuousFragmented ||
			signal == signalContinuousLayoutChange {
			t.Fatalf("failed historical stage falsely marked %q evaluated", signal)
		}
	}
}

func TestRecordingWindowHealthHistoricalStageBounds(t *testing.T) {
	if recordingWindowHealthMaterializationTimeout != 30*time.Second {
		t.Fatalf("materialization timeout=%s, want 30s", recordingWindowHealthMaterializationTimeout)
	}
	if completedWindowHealthTimeout+recordingWindowHealthMaterializationTimeout != 4*time.Minute+30*time.Second {
		t.Fatalf("bounded historical stages=%s, want 4m30s", completedWindowHealthTimeout+recordingWindowHealthMaterializationTimeout)
	}
	runSoftRecordingWindowHealthMaterialization(context.Background(), func(stageCtx context.Context) error {
		deadline, ok := stageCtx.Deadline()
		if !ok || time.Until(deadline) > 30*time.Second || time.Until(deadline) < 29*time.Second {
			t.Fatalf("materialization deadline=%v ok=%t", deadline, ok)
		}
		return nil
	})
}

func TestCompletedWindowHealthStageMarksSignalsEvaluatedAfterSuccess(t *testing.T) {
	base := []healthIncident{{RecordingID: 7, Signal: signalContinuousSilentDeath}}
	historical := healthIncident{RecordingID: 8, Signal: signalContinuousLayoutChange}
	result := runCompletedWindowHealthStage(context.Background(), base, func(context.Context) ([]healthIncident, error) {
		return []healthIncident{historical}, nil
	})

	if len(result.incidents) != 2 || result.incidents[1] != historical {
		t.Fatalf("incidents=%v, want live and completed-window incidents", result.incidents)
	}
	if fmt.Sprint(result.evaluatedSignals) != fmt.Sprint(completedWindowHealthSignals) {
		t.Fatalf("evaluated signals=%v want=%v", result.evaluatedSignals, completedWindowHealthSignals)
	}
	full := append([]string{
		signalContinuousSilentDeath, signalContinuousWindowEndedEarly,
		signalJobRetriesExhausted, signalStuckLease, signalSampledOverdue,
		signalClipTimestampDrift,
	}, result.evaluatedSignals...)
	if fmt.Sprint(full) != fmt.Sprint(evaluatedHealthSignals(false, false)) {
		t.Fatalf("successful full-sweep signal registry=%v want=%v", full, evaluatedHealthSignals(false, false))
	}
}

func TestCompletedWindowLayoutChangePreservesEmptyKnownCodecParity(t *testing.T) {
	base := completedWindowClip{audioPresent: true, width: 1280, height: 720}
	cases := []struct {
		name              string
		previous, current completedWindowClip
	}{
		{"video empty to known", base, completedWindowClip{videoCodec: "h264", audioPresent: true, width: 1280, height: 720}},
		{"video known to empty", completedWindowClip{videoCodec: "h264", audioPresent: true, width: 1280, height: 720}, base},
		{"audio empty to known", base, completedWindowClip{audioCodec: "aac", audioPresent: true, width: 1280, height: 720}},
		{"audio known to empty", completedWindowClip{audioCodec: "aac", audioPresent: true, width: 1280, height: 720}, base},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if !completedWindowLayoutChanged(tc.previous, tc.current) {
				t.Fatal("empty/known codec transition no longer matches former COALESCE plus IS DISTINCT FROM semantics")
			}
		})
	}
}

func TestCompletedWindowHealthStageCancelsPostgresQueryWithoutLeak(t *testing.T) {
	databaseURL := strings.TrimSpace(os.Getenv("STOARAMA_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL to run DB-backed cancellation regression")
	}
	appName := fmt.Sprintf("health_cancel_%d", time.Now().UnixNano())
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ConnConfig.RuntimeParams == nil {
		cfg.ConnConfig.RuntimeParams = map[string]string{}
	}
	cfg.ConnConfig.RuntimeParams["application_name"] = appName
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	stageCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	var queryErr error
	runCompletedWindowHealthStage(stageCtx, nil, func(queryCtx context.Context) ([]healthIncident, error) {
		_, queryErr = pool.Exec(queryCtx, `SELECT pg_sleep(10)`)
		return nil, queryErr
	})
	if queryErr == nil || !errors.Is(stageCtx.Err(), context.DeadlineExceeded) {
		t.Fatalf("query error=%v context error=%v, want deadline cancellation", queryErr, stageCtx.Err())
	}
	var one int
	if err := pool.QueryRow(context.Background(), `SELECT 1`).Scan(&one); err != nil || one != 1 {
		t.Fatalf("pool unusable after cancellation: one=%d err=%v", one, err)
	}

	admin, err := pgxpool.New(context.Background(), databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	deadline := time.Now().Add(2 * time.Second)
	for {
		var running int
		if err := admin.QueryRow(context.Background(), `
			SELECT count(*) FROM pg_stat_activity
			WHERE application_name=$1 AND state='active' AND query LIKE '%pg_sleep%'
		`, appName).Scan(&running); err != nil {
			t.Fatal(err)
		}
		if running == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("canceled historical query still active for application %q", appName)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestHistoricalStageFailureKeepsHistoricalAlertOpenWhileResolvingLiveAlert(t *testing.T) {
	databaseURL := strings.TrimSpace(os.Getenv("STOARAMA_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL to run DB-backed alert-state regression")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema := fmt.Sprintf("health_stage_alerts_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = admin.Exec(ctx, `DROP SCHEMA `+schema+` CASCADE`) }()
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ConnConfig.RuntimeParams == nil {
		cfg.ConnConfig.RuntimeParams = map[string]string{}
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, `
		CREATE TABLE recorder_health_alerts (
		  recording_id BIGINT NOT NULL, signal TEXT NOT NULL, resolved_at TIMESTAMPTZ,
		  PRIMARY KEY(recording_id,signal));
	`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO recorder_health_alerts(recording_id,signal) VALUES (1,$1),(2,$2)`,
		signalContinuousCoverageLow, signalContinuousSilentDeath); err != nil {
		t.Fatal(err)
	}

	result := runCompletedWindowHealthStage(ctx, nil, func(context.Context) ([]healthIncident, error) {
		return nil, errors.New("fixture historical scan failed")
	})
	evaluated := append([]string{signalContinuousSilentDeath}, result.evaluatedSignals...)
	if err := resolveClearedHealthAlerts(ctx, pool, result.incidents, evaluated); err != nil {
		t.Fatal(err)
	}
	var historicalResolved, liveResolved *time.Time
	if err := pool.QueryRow(ctx, `SELECT resolved_at FROM recorder_health_alerts WHERE recording_id=1`).Scan(&historicalResolved); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT resolved_at FROM recorder_health_alerts WHERE recording_id=2`).Scan(&liveResolved); err != nil {
		t.Fatal(err)
	}
	if historicalResolved != nil || liveResolved == nil {
		t.Fatalf("historical resolved=%v live resolved=%v, want historical open and live cleared", historicalResolved, liveResolved)
	}
}

func TestMaterializationFailureStillRunsRealAlertResolution(t *testing.T) {
	databaseURL := strings.TrimSpace(os.Getenv("STOARAMA_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL to run DB-backed materialization regression")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema := fmt.Sprintf("health_materialize_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = admin.Exec(ctx, `DROP SCHEMA `+schema+` CASCADE`) }()
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ConnConfig.RuntimeParams == nil {
		cfg.ConnConfig.RuntimeParams = map[string]string{}
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, `
		CREATE TABLE recorder_health_alerts (
		  recording_id BIGINT NOT NULL, signal TEXT NOT NULL, resolved_at TIMESTAMPTZ,
		  PRIMARY KEY(recording_id,signal));
	`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO recorder_health_alerts(recording_id,signal) VALUES (9,$1)`, signalContinuousSilentDeath); err != nil {
		t.Fatal(err)
	}

	runSoftRecordingWindowHealthMaterialization(ctx, func(context.Context) error {
		return errors.New("fixture materialization failed")
	})
	if err := resolveClearedHealthAlerts(ctx, pool, nil, []string{signalContinuousSilentDeath}); err != nil {
		t.Fatal(err)
	}
	var resolved *time.Time
	if err := pool.QueryRow(ctx, `SELECT resolved_at FROM recorder_health_alerts WHERE recording_id=9`).Scan(&resolved); err != nil {
		t.Fatal(err)
	}
	if resolved == nil {
		t.Fatal("materialization failure suppressed real alert resolution mutation")
	}
}

func ptrTime(v time.Time) *time.Time { return &v }

func sampleIncidents() []healthIncident {
	return []healthIncident{
		{
			RecordingID: 101, StreamID: 11, OrgName: "MIT", OrgEmail: "ops@mit.edu",
			RecName: "Kresge Plaza", StreamURL: "https://cam/1.m3u8",
			Signal: signalContinuousSilentDeath, Severity: "CRITICAL",
			SinceText: "window opened 2026-07-04T13:00:00Z, last clip never",
			Diag:      "last_error=ffmpeg exited",
		},
		{
			RecordingID: 202, StreamID: 22, OrgName: "Lab", OrgEmail: "lab@stoarama.com",
			RecName: "Corner", StreamURL: "https://cam/2.m3u8",
			Signal: signalJobRetriesExhausted, Severity: "HIGH",
			SinceText: "2026-07-04T13:20:00Z",
			Diag:      "job_id=999 kind=clip attempts=3/3 error=timeout",
		},
	}
}

func TestComposeHealthEmailSubject(t *testing.T) {
	one := sampleIncidents()[:1]
	if got := composeHealthEmailSubject(one); got != "[Stoarama] Recording 101 unhealthy: "+healthSignalLabels[signalContinuousSilentDeath] {
		t.Fatalf("single subject unexpected: %q", got)
	}
	if got := composeHealthEmailSubject(sampleIncidents()); got != "[Stoarama] 2 recording health alert(s)" {
		t.Fatalf("multi subject unexpected: %q", got)
	}
}

func TestComposeHealthEmailBody(t *testing.T) {
	body := composeHealthEmailBody("https://stoarama.test/", sampleIncidents())
	for _, want := range []string{
		"2 recording health issue(s)",
		"Recording #101 \"Kresge Plaza\"",
		"MIT <ops@mit.edu>",
		"https://cam/1.m3u8",
		"Stoarama: https://stoarama.test/streams/11",
		"Recording: https://stoarama.test/recordings/101",
		healthSignalLabels[signalContinuousSilentDeath] + " [CRITICAL]",
		"last_error=ffmpeg exited",
		"Recording #202 \"Corner\"",
		"job_id=999 kind=clip attempts=3/3 error=timeout",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("body missing %q\n---\n%s", want, body)
		}
	}
}

func TestDiagTextDropsBlanks(t *testing.T) {
	got := diagText("job_id", "5", "error", "  ", "kind", "clip")
	if got != "job_id=5 kind=clip" {
		t.Fatalf("diagText=%q", got)
	}
}

func TestContinuousSilenceDetailShowsMediaLagNotJustIngestActivity(t *testing.T) {
	winOpen := time.Date(2026, 8, 9, 4, 39, 0, 0, time.UTC)
	mediaEnd := time.Date(2026, 8, 9, 6, 48, 0, 0, time.UTC)
	ingestedAt := time.Date(2026, 8, 9, 7, 44, 30, 0, time.UTC)
	observedAt := ingestedAt.Add(30 * time.Second)
	lastClipAt := ingestedAt
	since, diag := continuousSilenceDetail(observedAt, winOpen, &mediaEnd, &ingestedAt, &lastClipAt, 56*60+30, 5*time.Minute, "")
	for _, want := range []string{"window opened 2026-08-09T04:39:00Z", "latest media ended 2026-08-09T06:48:00Z", "latest ingest 2026-08-09T07:44:30Z"} {
		if !strings.Contains(since, want) {
			t.Fatalf("since=%q missing %q", since, want)
		}
	}
	if diag != "capture_state=fresh_ingest_stale_media media_behind=56m30s recording_last_clip=2026-08-09T07:44:30Z" {
		t.Fatalf("diag=%q", diag)
	}
}

func TestContinuousSilenceDetailHandlesNoMedia(t *testing.T) {
	winOpen := time.Date(2026, 8, 9, 6, 0, 0, 0, time.UTC)
	since, diag := continuousSilenceDetail(winOpen.Add(time.Hour), winOpen, nil, nil, nil, 0, 5*time.Minute, "source returned 404")
	if !strings.Contains(since, "latest media ended never, latest ingest never") {
		t.Fatalf("since=%q", since)
	}
	if strings.Contains(diag, "media_behind") || diag != "capture_state=no_recent_ingest recording_last_clip=never last_error=source returned 404" {
		t.Fatalf("diag=%q", diag)
	}
}

func TestContinuousSilenceDetailFreshIngestBoundary(t *testing.T) {
	observedAt := time.Date(2026, 8, 9, 8, 0, 0, 0, time.UTC)
	winOpen := observedAt.Add(-time.Hour)
	mediaEnd := observedAt.Add(-20 * time.Minute)
	freshness := 5 * time.Minute
	exact := observedAt.Add(-freshness)
	_, exactDiag := continuousSilenceDetail(observedAt, winOpen, &mediaEnd, &exact, &exact, 20*60, freshness, "")
	if !strings.Contains(exactDiag, "capture_state=fresh_ingest_stale_media") {
		t.Fatalf("exact boundary diag=%q", exactDiag)
	}
	after := exact.Add(-time.Nanosecond)
	_, afterDiag := continuousSilenceDetail(observedAt, winOpen, &mediaEnd, &after, &after, 20*60, freshness, "")
	if !strings.Contains(afterDiag, "capture_state=no_recent_ingest") {
		t.Fatalf("after boundary diag=%q", afterDiag)
	}
}

func TestDetectContinuousSilentDeathReportsMediaAgeAndNoMedia(t *testing.T) {
	databaseURL := strings.TrimSpace(os.Getenv("STOARAMA_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL to run DB-backed continuous silence regression")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema := fmt.Sprintf("continuous_silence_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = admin.Exec(ctx, `DROP SCHEMA `+schema+` CASCADE`) }()
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ConnConfig.RuntimeParams == nil {
		cfg.ConnConfig.RuntimeParams = map[string]string{}
	}
	// Put the fixture schema before pg_catalog so its fixed clock controls every
	// unqualified now() in the production detector query.
	cfg.ConnConfig.RuntimeParams["search_path"] = schema + ",pg_catalog"
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, `
		CREATE FUNCTION now() RETURNS timestamptz LANGUAGE sql IMMUTABLE
		  AS $$ SELECT timestamptz '2026-08-09 08:00:00+00' $$;
		CREATE TABLE accounts (id BIGINT PRIMARY KEY,name TEXT NOT NULL,email TEXT NOT NULL);
		CREATE TABLE account_billing (account_id BIGINT PRIMARY KEY,has_payment_method BOOLEAN NOT NULL);
		CREATE TABLE recordings (
		  id BIGINT PRIMARY KEY,stream_id BIGINT,account_id BIGINT NOT NULL,name TEXT NOT NULL,stream_url TEXT NOT NULL,
		  status TEXT NOT NULL,mode TEXT NOT NULL,start_at TIMESTAMPTZ NOT NULL,end_at TIMESTAMPTZ,
		  cron_timezone TEXT NOT NULL,daily_window_start TIME NOT NULL,daily_window_end TIME NOT NULL,
		  last_clip_at TIMESTAMPTZ,last_error_text TEXT NOT NULL DEFAULT ''
		);
		CREATE TABLE recording_clips (
		  id BIGINT PRIMARY KEY,recording_id BIGINT NOT NULL,clip_start_at TIMESTAMPTZ NOT NULL,
		  clip_end_at TIMESTAMPTZ NOT NULL,created_at TIMESTAMPTZ NOT NULL
		);
		INSERT INTO accounts VALUES (1,'Ops','ops@example.com');
		INSERT INTO account_billing VALUES (1,true);
		INSERT INTO recordings VALUES
		  (1,101,1,'lagging','https://e.test/lagging','active','continuous','2026-08-01',NULL,'UTC','00:00','12:00','2026-08-09 07:44:30+00',''),
		  (2,102,1,'dead','https://e.test/dead','active','continuous','2026-08-01',NULL,'UTC','00:00','12:00',NULL,'source returned 404');
		INSERT INTO recording_clips VALUES
		  (1,1,'2026-08-09 06:47:00+00','2026-08-09 07:03:30+00','2026-08-09 07:44:30+00');
	`); err != nil {
		t.Fatal(err)
	}

	got := detectContinuousSilentDeath(ctx, pool, 15)
	if len(got) != 2 {
		t.Fatalf("incidents=%+v", got)
	}
	byID := map[int64]healthIncident{got[0].RecordingID: got[0], got[1].RecordingID: got[1]}
	lagging := byID[1]
	if !strings.Contains(lagging.SinceText, "latest media ended 2026-08-09T07:03:30Z") ||
		!strings.Contains(lagging.SinceText, "latest ingest 2026-08-09T07:44:30Z") ||
		lagging.Diag != "capture_state=no_recent_ingest media_behind=56m30s recording_last_clip=2026-08-09T07:44:30Z" {
		t.Fatalf("lagging=%+v", lagging)
	}
	dead := byID[2]
	if !strings.Contains(dead.SinceText, "latest media ended never, latest ingest never") ||
		dead.Diag != "capture_state=no_recent_ingest recording_last_clip=never last_error=source returned 404" {
		t.Fatalf("dead=%+v", dead)
	}
}

func TestMeasureStitchWindowSeparatesCoverageOverlapAndGap(t *testing.T) {
	open := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	close := open.Add(10 * time.Minute)
	leading := [][2]time.Time{{open.Add(2 * time.Minute), close}}
	if m := measureStitchWindow(open, close, leading); m.maxGap != 2*time.Minute {
		t.Fatalf("leading gap: %+v", m)
	}
	clips := [][2]time.Time{
		{open.Add(-time.Minute), open.Add(2 * time.Minute)},     // clipped at window edge
		{open.Add(90 * time.Second), open.Add(3 * time.Minute)}, // 30s overlap
		{open.Add(9 * time.Minute), close.Add(time.Minute)},     // clipped at close; 6m gap
	}
	m := measureStitchWindow(open, close, clips)
	if m.coveragePct != 40 {
		t.Fatalf("coverage=%.2f want 40", m.coveragePct)
	}
	if m.overlapClips != 1 || m.overlapSeconds != 30 {
		t.Fatalf("overlap clips=%d seconds=%.1f want 1/30", m.overlapClips, m.overlapSeconds)
	}
	if m.maxGap != 6*time.Minute {
		t.Fatalf("max gap=%s want 6m", m.maxGap)
	}
	if m.gapClips != 1 {
		t.Fatalf("gap clips=%d want 1", m.gapClips)
	}
	if m.longestRun != 3*time.Minute {
		t.Fatalf("longest run=%s want 3m", m.longestRun)
	}
}

func TestMeasureStitchWindowEdgeCases(t *testing.T) {
	open := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	close := open.Add(10 * time.Minute)
	if m := measureStitchWindow(open, close, nil); m.coveragePct != 0 || m.gapClips != 0 || m.maxGap != close.Sub(open) {
		t.Fatalf("empty window: %+v", m)
	}
	if m := measureStitchWindow(open, open, nil); m.coveragePct != 0 {
		t.Fatalf("zero-length window: %+v", m)
	}
	contained := [][2]time.Time{
		{open, open.Add(5 * time.Minute)},
		{open.Add(time.Minute), open.Add(2 * time.Minute)},
	}
	if m := measureStitchWindow(open, close, contained); m.overlapClips != 1 || m.overlapSeconds != 60 || m.maxGap != 5*time.Minute {
		t.Fatalf("contained clip: %+v", m)
	}
	adjacent := [][2]time.Time{
		{open, open.Add(time.Minute)},
		{open.Add(time.Minute + 500*time.Millisecond), close},
	}
	if m := measureStitchWindow(open, close, adjacent); m.gapClips != 0 || m.overlapClips != 0 || m.maxGap != 500*time.Millisecond || m.coveragePct != 99.91666666666667 {
		t.Fatalf("sub-tolerance join: %+v", m)
	}
	subsecondOverlap := [][2]time.Time{
		{open, open.Add(5 * time.Minute)},
		{open.Add(5*time.Minute - 500*time.Millisecond), close},
	}
	if m := measureStitchWindow(open, close, subsecondOverlap); m.overlapClips != 1 || m.overlapSeconds != 0.5 || m.coveragePct != 100 {
		t.Fatalf("sub-second overlap must remain visible: %+v", m)
	}
}

func TestMeasureStitchWindowCoverageThresholdAndAccumulatedTinyGaps(t *testing.T) {
	open := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	close := open.Add(100 * time.Second)
	exact95 := [][2]time.Time{{open, open.Add(95 * time.Second)}}
	if m := measureStitchWindow(open, close, exact95); m.coveragePct != 95 {
		t.Fatalf("exact threshold coverage=%v", m.coveragePct)
	}
	below95 := [][2]time.Time{{open, open.Add(94990 * time.Millisecond)}}
	if m := measureStitchWindow(open, close, below95); !(m.coveragePct < 95) {
		t.Fatalf("below threshold coverage=%v", m.coveragePct)
	}
	clips := make([][2]time.Time, 0, 10)
	for i := 0; i < 10; i++ {
		start := open.Add(time.Duration(i) * 10 * time.Second)
		clips = append(clips, [2]time.Time{start, start.Add(9100 * time.Millisecond)})
	}
	m := measureStitchWindow(open, close, clips)
	if m.coveragePct != 91 || m.gapClips != 0 || m.maxGap != 900*time.Millisecond {
		t.Fatalf("tiny gaps must reduce exact coverage without fragmenting: %+v", m)
	}
}

func TestMeasureStitchWindowQualificationGapThresholdsIncludeEdges(t *testing.T) {
	open := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	close := open.Add(20 * time.Minute)
	clips := [][2]time.Time{
		// Leading edge is exactly 30s and therefore does not count for the
		// strict >30s threshold.
		{open.Add(30 * time.Second), open.Add(2 * time.Minute)},
		// Internal 31s gap counts only for >30s.
		{open.Add(2*time.Minute + 31*time.Second), open.Add(8 * time.Minute)},
		// Internal gap exactly five minutes counts for >30s, not strict >5m.
		{open.Add(13 * time.Minute), open.Add(14 * time.Minute)},
		// Trailing six-minute edge counts for both thresholds.
	}
	m := measureStitchWindow(open, close, clips)
	if m.gapsOver30s != 3 || m.gapsOver5m != 1 || m.maxGap != 6*time.Minute {
		t.Fatalf("qualification gaps: %+v", m)
	}

	empty := measureStitchWindow(open, close, nil)
	if empty.gapsOver30s != 1 || empty.gapsOver5m != 1 || empty.maxGap != 20*time.Minute {
		t.Fatalf("empty qualification gap: %+v", empty)
	}
}

func TestVerifyStoredClipSHA(t *testing.T) {
	sum := sha256.Sum256([]byte("stored clip"))
	if err := verifyStoredClipSHA(sum[:], fmt.Sprintf("%x", sum[:])); err != nil {
		t.Fatalf("matching sha: %v", err)
	}
	if err := verifyStoredClipSHA(sum[:], ""); err != nil {
		t.Fatalf("legacy missing sha: %v", err)
	}
	if err := verifyStoredClipSHA(sum[:], strings.Repeat("0", 64)); err == nil {
		t.Fatal("mismatched sha accepted")
	}
	if err := verifyStoredClipSHA(sum[:], "not-a-sha"); err == nil {
		t.Fatal("invalid sha metadata accepted")
	}
	matching := fmt.Sprintf("%x", sum[:])
	if err := verifyStoredClipSHA(sum[:], "  "+strings.ToUpper(matching)+"  "); err != nil {
		t.Fatalf("normalized sha rejected: %v", err)
	}
	if err := verifyStoredClipSHA(sum[:], strings.Repeat("ab", 16)); err == nil {
		t.Fatal("short valid-hex sha accepted")
	}
}

func TestBoundedHealthDiagnostic(t *testing.T) {
	short := "brief failure"
	if got := boundedHealthDiagnostic(short); got != short {
		t.Fatalf("short diagnostic=%q", got)
	}
	got := boundedHealthDiagnostic(strings.Repeat("x", mediaHealthDiagnosticLimit+100))
	if len(got) <= mediaHealthDiagnosticLimit || !strings.HasSuffix(got, "…[truncated]") {
		t.Fatalf("diagnostic not bounded/marked: len=%d suffix=%q", len(got), got[len(got)-20:])
	}
	unicodeValue := strings.Repeat("x", mediaHealthDiagnosticLimit-1) + "é trailing"
	if got := boundedHealthDiagnostic(unicodeValue); !utf8.ValidString(got) {
		t.Fatalf("diagnostic split UTF-8 rune: %q", got[len(got)-20:])
	}
}

// TestDetectClipTimestampDriftFindsWorstClipNotNewest pins the two decisions that
// make this signal useful rather than decorative: it fires only past the drift
// limit, and it judges the WORST clip of the last hour rather than the newest one.
//
// The second matters because a drifted chain re-anchors whenever its ffmpeg
// restarts. Zanzibar's most recent clips were healthy at the very moment an hour
// of its footage was being filed under the wrong day, so a newest-clip check would
// have reported it fine and the drift would have kept running unseen.
func TestDetectClipTimestampDriftFindsWorstClipNotNewest(t *testing.T) {
	databaseURL := strings.TrimSpace(os.Getenv("STOARAMA_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL to run DB-backed clip drift regression")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema := fmt.Sprintf("clip_drift_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = admin.Exec(ctx, `DROP SCHEMA `+schema+` CASCADE`) }()
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ConnConfig.RuntimeParams == nil {
		cfg.ConnConfig.RuntimeParams = map[string]string{}
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	if _, err := pool.Exec(ctx, `
		CREATE TABLE accounts (id BIGINT PRIMARY KEY, name TEXT NOT NULL, email TEXT NOT NULL);
		CREATE TABLE recordings (
			id BIGINT PRIMARY KEY, stream_id BIGINT, account_id BIGINT NOT NULL,
			name TEXT NOT NULL, stream_url TEXT NOT NULL,
			status TEXT NOT NULL, mode TEXT NOT NULL);
		CREATE TABLE recording_clips (
			recording_id BIGINT NOT NULL,
			clip_start_at TIMESTAMPTZ NOT NULL,
			created_at TIMESTAMPTZ NOT NULL);
		INSERT INTO accounts VALUES (1, 'MIT SCL', 'scl@example.edu');
		INSERT INTO recordings VALUES
		  (10, 100, 1, 'healthy',    'https://e.test/a', 'active', 'continuous'),
		  (11, 101, 1, 'marginal',   'https://e.test/b', 'active', 'continuous'),
		  (12, 102, 1, 'reanchored', 'https://e.test/c', 'active', 'continuous'),
		  (13, 103, 1, 'stale',      'https://e.test/d', 'active', 'continuous'),
		  (14, 104, 1, 'exactly90',  'https://e.test/e', 'active', 'continuous'),
		  (15, 105, 1, 'old-timeline','https://e.test/f', 'active', 'continuous');
	`); err != nil {
		t.Fatal(err)
	}

	if _, err := pool.Exec(ctx, `
		INSERT INTO recording_clips (recording_id, clip_start_at, created_at) VALUES
		  -- healthy: stamped BEFORE ingest, which is the normal ordering
		  (10, now() - interval '5 minutes' - interval '60 seconds', now() - interval '5 minutes'),
		  -- marginal: leads by 30s, under the 90s limit, so end-of-window truncated
		  -- clips do not page anyone
		  (11, now() - interval '5 minutes' + interval '30 seconds', now() - interval '5 minutes'),
		  -- reanchored: an hour of drifted clips followed by a healthy newest clip,
		  -- exactly the shape that hid Zanzibar
		  (12, now() - interval '30 minutes' + interval '4 hours',   now() - interval '30 minutes'),
		  (12, now() - interval '1 minute'  - interval '60 seconds', now() - interval '1 minute'),
		  -- stale: badly drifted but outside the one-hour window, so it is history,
		  -- not a live incident
		  (13, now() - interval '5 hours' + interval '4 hours', now() - interval '5 hours'),
		  -- exactly at the limit: the comparison is strictly greater-than, so the
		  -- threshold itself is still healthy and must not page anyone
		  (14, now() - interval '5 minutes' + interval '90 seconds', now() - interval '5 minutes'),
		  -- recently ingested but stamped on an old timeline. The index-enabling
		  -- start-time bound drops this non-alertable negative drift without masking
		  -- recording 12's qualifying future-stamped row.
		  (15, now() - interval '2 hours', now() - interval '5 minutes');
	`); err != nil {
		t.Fatal(err)
	}

	got := detectClipTimestampDrift(ctx, pool)
	if len(got) != 1 {
		ids := []int64{}
		for _, i := range got {
			ids = append(ids, i.RecordingID)
		}
		t.Fatalf("incidents=%v (recordings %v), want exactly recording 12", len(got), ids)
	}
	if got[0].RecordingID != 12 {
		t.Fatalf("flagged recording %d, want 12 (the re-anchored one)", got[0].RecordingID)
	}
	if got[0].Signal != signalClipTimestampDrift || got[0].Severity != "CRITICAL" {
		t.Fatalf("signal=%q severity=%q", got[0].Signal, got[0].Severity)
	}
	if !strings.Contains(got[0].Diag, "ahead_of_real_time") {
		t.Fatalf("diag missing lead: %q", got[0].Diag)
	}
}

func TestDetectClipTimestampDriftAndLayoutChangeFindsNativeSeamChange(t *testing.T) {
	databaseURL := strings.TrimSpace(os.Getenv("STOARAMA_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL to run DB-backed layout regression")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema := fmt.Sprintf("clip_layout_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = admin.Exec(ctx, `DROP SCHEMA `+schema+` CASCADE`) }()
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, `
		CREATE TABLE accounts (id BIGINT PRIMARY KEY,name TEXT NOT NULL,email TEXT NOT NULL);
		CREATE TABLE account_billing (account_id BIGINT PRIMARY KEY,has_payment_method BOOLEAN NOT NULL);
		CREATE TABLE recordings (id BIGINT PRIMARY KEY,stream_id BIGINT,account_id BIGINT NOT NULL,name TEXT NOT NULL,stream_url TEXT NOT NULL,status TEXT NOT NULL,mode TEXT NOT NULL);
		CREATE TABLE recording_jobs (id BIGINT PRIMARY KEY,recording_id BIGINT NOT NULL,kind TEXT NOT NULL,fire_at TIMESTAMPTZ NOT NULL,window_end_at TIMESTAMPTZ,status TEXT NOT NULL DEFAULT 'done',error_text TEXT NOT NULL DEFAULT '');
		CREATE TABLE recording_clips (
		  id BIGINT PRIMARY KEY,recording_id BIGINT NOT NULL,recording_job_id BIGINT NOT NULL,clip_start_at TIMESTAMPTZ NOT NULL,clip_end_at TIMESTAMPTZ NOT NULL,
		  video_codec TEXT,audio_codec TEXT,audio_present BOOLEAN NOT NULL,actual_fps DOUBLE PRECISION,video_width INTEGER,video_height INTEGER);
		INSERT INTO accounts VALUES (1,'MIT SCL','scl@example.edu');
		INSERT INTO account_billing VALUES (1,true);
		INSERT INTO recordings VALUES
		  (20,120,1,'changed','https://e.test/changed','active','continuous'),
		  (21,121,1,'stable','https://e.test/stable','active','continuous'),
		  (22,122,1,'native variable fps','https://e.test/variable','active','continuous'),
		  (23,123,1,'no clips','https://e.test/empty','active','continuous'),
		  (24,124,1,'fragmented overlap','https://e.test/fragments','active','continuous');
		INSERT INTO recording_jobs VALUES
		  (200,20,'continuous_window',now()-interval '2 hours',now()-interval '1 hour'),
		  (209,21,'clip',now()-interval '3 hours',now()-interval '2 hours'),
		  (210,21,'continuous_window',now()-interval '2 hours',now()-interval '1 hour'),
		  (220,22,'continuous_window',now()-interval '2 hours',now()-interval '1 hour'),
		  (230,23,'continuous_window',now()-interval '2 hours',now()-interval '1 hour'),
		  (231,23,'continuous_window',now()-interval '10 minutes',now()+interval '1 hour'),
		  (240,24,'continuous_window',now()-interval '2 hours',now()-interval '1 hour');
		INSERT INTO recording_clips VALUES
		  (100,20,200,now()-interval '110 minutes',now()-interval '109 minutes','h264','aac',true,30,1280,720),
		  (50,20,200,now()-interval '109 minutes',now()-interval '108 minutes','h264','aac',true,30,1920,1080),
		  (60,20,200,now()-interval '109 minutes',now()-interval '108 minutes','h264','aac',true,30,1280,720),
		  (10,20,200,now()-interval '108 minutes',now()-interval '107 minutes','h264','aac',true,30,640,480),
		  (3,21,210,now()-interval '110 minutes',now()-interval '109 minutes','h264','aac',true,30,1280,720),
		  (9,21,209,now()-interval '109 minutes 30 seconds',now()-interval '108 minutes 30 seconds','h264','aac',true,30,1920,1080),
		  (8,20,210,now()-interval '109 minutes 30 seconds',now()-interval '108 minutes 30 seconds','h264','aac',true,30,1920,1080),
		  (4,21,210,now()-interval '109 minutes',now()-interval '108 minutes','h264','aac',true,30,1280,720),
		  (5,22,220,now()-interval '110 minutes',now()-interval '109 minutes','h264','aac',true,30,1280,720),
		  (6,22,220,now()-interval '109 minutes',now()-interval '108 minutes','h264','aac',true,24,1280,720),
		  (7,23,230,now()-interval '5 minutes',now()-interval '4 minutes','h264','aac',true,30,1280,720);
		INSERT INTO recording_clips
		SELECT 1000+n,24,240,now()-interval '2 hours'+n*interval '3 minutes',
		       now()-interval '2 hours'+n*interval '3 minutes'+interval '1 minute',
		       'h264','aac',true,30,1280,720
		FROM generate_series(0,11) n;
		INSERT INTO recording_clips VALUES
		  (2000,24,240,now()-interval '2 hours'+interval '30 seconds',now()-interval '2 hours'+interval '90 seconds','h264','aac',true,30,1280,720);
	`); err != nil {
		t.Fatal(err)
	}
	got, err := detectCompletedWindowHealth(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	var timeline, layout *healthIncident
	for i := range got {
		if got[i].RecordingID == 20 && got[i].Signal == signalContinuousCoverageLow {
			timeline = &got[i]
		}
		if got[i].RecordingID == 20 && got[i].Signal == signalContinuousLayoutChange {
			layout = &got[i]
		}
	}
	if timeline == nil {
		t.Fatalf("combined incidents lack recording 20 timeline result: %+v", got)
	}
	if layout == nil {
		t.Fatalf("combined incidents lack recording 20 layout result: %+v", got)
	}
	wantDiag := "seam=60->10 video=h264/h264 audio=true:aac/true:aac fps=30.000/30.000 dimensions=1280×720/640×480"
	if layout.Diag != wantDiag {
		t.Fatalf("layout diagnostic=%q want=%q", layout.Diag, wantDiag)
	}
	signalsByRecording := map[int64]map[string]bool{}
	for _, inc := range got {
		if signalsByRecording[inc.RecordingID] == nil {
			signalsByRecording[inc.RecordingID] = map[string]bool{}
		}
		signalsByRecording[inc.RecordingID][inc.Signal] = true
	}
	wantSignals := map[int64][]string{
		20: {signalContinuousCoverageLow, signalContinuousOverlap, signalContinuousLongGap, signalContinuousLayoutChange},
		21: {signalContinuousCoverageLow, signalContinuousLongGap},
		22: {signalContinuousCoverageLow, signalContinuousLongGap},
		23: {signalContinuousCoverageLow, signalContinuousLongGap},
		24: {signalContinuousCoverageLow, signalContinuousOverlap, signalContinuousLongGap, signalContinuousFragmented},
	}
	for recordingID, want := range wantSignals {
		gotSet := signalsByRecording[recordingID]
		if len(gotSet) != len(want) {
			t.Fatalf("recording %d signals=%v want exactly %v", recordingID, gotSet, want)
		}
		for _, signal := range want {
			if !gotSet[signal] {
				t.Fatalf("recording %d signals=%v missing %q", recordingID, gotSet, signal)
			}
		}
	}
	early := detectContinuousWindowEndedEarly(ctx, pool)
	if len(early) != 1 || early[0].RecordingID != 23 || !strings.Contains(early[0].Diag, "job_id=231") {
		t.Fatalf("ended-early incidents=%+v, want only recording 23 job 231", early)
	}
}
