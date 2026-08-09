package main

import (
	"context"
	"crypto/sha256"
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
		newlyInserted bool
		lastAlertedAt time.Time
		want          bool
	}{
		{"newly inserted always notifies", true, now, true},
		{"never delivered retries", false, time.Time{}, true},
		{"older than 24h renotifies", false, now.Add(-24*time.Hour - time.Second), true},
		{"exactly 24h stays quiet", false, now.Add(-24 * time.Hour), false},
		{"recent successful delivery stays quiet", false, now.Add(-3 * time.Hour), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := shouldNotifyHealthIncident(tc.newlyInserted, tc.lastAlertedAt, now); got != tc.want {
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
		  last_alerted_at TIMESTAMPTZ NOT NULL DEFAULT now(), resolved_at TIMESTAMPTZ,
		  PRIMARY KEY(recording_id,signal));
		INSERT INTO recordings VALUES (1);
	`); err != nil {
		t.Fatal(err)
	}
	inserted, last, err := upsertHealthAlert(ctx, pool, 1, signalStoredClipInvalid)
	if err != nil || !inserted || !shouldNotifyHealthIncident(inserted, last, time.Now()) {
		t.Fatalf("first detection inserted=%v last=%v err=%v", inserted, last, err)
	}
	inserted, last, err = upsertHealthAlert(ctx, pool, 1, signalStoredClipInvalid)
	if err != nil || inserted || !shouldNotifyHealthIncident(inserted, last, time.Now()) {
		t.Fatalf("unacknowledged detection must retry inserted=%v last=%v err=%v", inserted, last, err)
	}
	incidents := []healthIncident{{RecordingID: 1, Signal: signalStoredClipInvalid}}
	if err := markHealthAlertsDelivered(ctx, pool, incidents); err != nil {
		t.Fatal(err)
	}
	inserted, last, err = upsertHealthAlert(ctx, pool, 1, signalStoredClipInvalid)
	if err != nil || shouldNotifyHealthIncident(inserted, last, time.Now()) {
		t.Fatalf("acknowledged detection must dedup inserted=%v last=%v err=%v", inserted, last, err)
	}
	if _, err := pool.Exec(ctx, `UPDATE recorder_health_alerts SET resolved_at=now()`); err != nil {
		t.Fatal(err)
	}
	inserted, last, err = upsertHealthAlert(ctx, pool, 1, signalStoredClipInvalid)
	if err != nil || !shouldNotifyHealthIncident(inserted, last, time.Now()) {
		t.Fatalf("reopened detection must notify inserted=%v last=%v err=%v", inserted, last, err)
	}
	if err := resolveClearedHealthAlerts(ctx, pool, nil, evaluatedHealthSignals(false)); err != nil {
		t.Fatal(err)
	}
	var resolved *time.Time
	if err := pool.QueryRow(ctx, `SELECT resolved_at FROM recorder_health_alerts WHERE recording_id=1 AND signal=$1`, signalStoredClipInvalid).Scan(&resolved); err != nil {
		t.Fatal(err)
	}
	if resolved != nil {
		t.Fatal("hourly-only sweep falsely resolved daily media incident")
	}
	if err := resolveClearedHealthAlerts(ctx, pool, nil, evaluatedHealthSignals(true)); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT resolved_at FROM recorder_health_alerts WHERE recording_id=1 AND signal=$1`, signalStoredClipInvalid).Scan(&resolved); err != nil || resolved == nil {
		t.Fatalf("media sweep did not resolve cleared media incident: resolved=%v err=%v", resolved, err)
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
	media := evaluatedHealthSignals(true)
	if len(media) != 1 || media[0] != signalStoredClipInvalid {
		t.Fatalf("media signals=%v", media)
	}
	for _, signal := range evaluatedHealthSignals(false) {
		if signal == signalStoredClipInvalid {
			t.Fatal("timeline sweep evaluates media signal")
		}
	}
}

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
	if m := measureStitchWindow(open, close, nil); m.coveragePct != 0 || m.gapClips != 0 {
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
		  (14, 104, 1, 'exactly90',  'https://e.test/e', 'active', 'continuous');
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
		  (14, now() - interval '5 minutes' + interval '90 seconds', now() - interval '5 minutes');
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
		CREATE TABLE recording_jobs (id BIGINT PRIMARY KEY,recording_id BIGINT NOT NULL,kind TEXT NOT NULL,fire_at TIMESTAMPTZ NOT NULL,window_end_at TIMESTAMPTZ);
		CREATE TABLE recording_clips (
		  id BIGINT PRIMARY KEY,recording_id BIGINT NOT NULL,clip_start_at TIMESTAMPTZ NOT NULL,clip_end_at TIMESTAMPTZ NOT NULL,
		  video_codec TEXT,audio_codec TEXT,audio_present BOOLEAN NOT NULL,actual_fps DOUBLE PRECISION,video_width INTEGER,video_height INTEGER);
		INSERT INTO accounts VALUES (1,'MIT SCL','scl@example.edu');
		INSERT INTO account_billing VALUES (1,true);
		INSERT INTO recordings VALUES
		  (20,120,1,'changed','https://e.test/changed','active','continuous'),
		  (21,121,1,'stable','https://e.test/stable','active','continuous');
		INSERT INTO recording_jobs VALUES
		  (200,20,'continuous_window',now()-interval '2 hours',now()-interval '1 hour'),
		  (210,21,'continuous_window',now()-interval '2 hours',now()-interval '1 hour');
		INSERT INTO recording_clips VALUES
		  (1,20,now()-interval '110 minutes',now()-interval '109 minutes','h264','aac',true,30,1280,720),
		  (2,20,now()-interval '109 minutes',now()-interval '108 minutes','h264','aac',true,30,1920,1080),
		  (3,21,now()-interval '110 minutes',now()-interval '109 minutes','h264','aac',true,30,1280,720),
		  (4,21,now()-interval '109 minutes',now()-interval '108 minutes','h264','aac',true,30,1280,720);
	`); err != nil {
		t.Fatal(err)
	}
	got := detectCompletedWindowLayoutChanges(ctx, pool)
	if len(got) != 1 || got[0].RecordingID != 20 || got[0].Signal != signalContinuousLayoutChange {
		t.Fatalf("layout incidents=%+v", got)
	}
}
