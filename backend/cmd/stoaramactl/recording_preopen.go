package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/daydemir/stoarama/backend/internal/capture"
	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/netguard"
	"github.com/jackc/pgx/v5/pgxpool"
)

type preopenTarget struct {
	recordingID, streamID, accountID                   int64
	recordingName, orgName, orgEmail                   string
	provider, sourceURL, sourcePage, captureVia, stage string
	windowStart                                        time.Time
	attempt                                            int
}
type preopenResult struct {
	target                 preopenTarget
	result, method, detail string
}

var preopenSensitiveRE = regexp.MustCompile(`(?i)(https?://\S+|bearer\s+\S+|(?:token|signature|credential|access_key|secret_key)=\S+)`)

func sanitizePreopenDetail(raw string) string {
	s := preopenSensitiveRE.ReplaceAllString(strings.TrimSpace(raw), "[redacted]")
	s = strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return ' '
		}
		return r
	}, s)
	r := []rune(strings.TrimSpace(s))
	if len(r) > 500 {
		r = append(r[:497], '.', '.', '.')
	}
	return string(r)
}

func preopenStage(windowStart, now time.Time) string {
	d := windowStart.Sub(now)
	if d > 30*time.Minute && d <= 2*time.Hour {
		return "early"
	}
	if d > 0 && d <= 30*time.Minute {
		return "confirm"
	}
	return ""
}

func runRecordingPreopen(ctx context.Context, cfg config.Config, args []string) {
	fs := flag.NewFlagSet("recording-preopen run", flag.ExitOnError)
	concurrency := fs.Int("concurrency", 4, "maximum concurrent probes")
	probeSec := fs.Int("probe-sec", 20, "source-native media probe duration")
	dryRun := fs.Bool("dry-run", false, "select only")
	_ = fs.Parse(args)
	if *concurrency < 1 || *concurrency > 6 || *probeSec < 5 || *probeSec > 30 {
		log.Fatal("invalid bounded pre-open options")
	}
	pool := mustOpenPool(ctx, cfg)
	defer pool.Close()
	lockConn, err := pool.Acquire(ctx)
	if err != nil {
		log.Fatalf("acquire pre-open lock connection: %v", err)
	}
	defer lockConn.Release()
	var locked bool
	if err = lockConn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, int64(0x53544f415241504f)).Scan(&locked); err != nil {
		log.Fatalf("acquire pre-open lock: %v", err)
	}
	if !locked {
		printJSON(map[string]any{"dry_run": *dryRun, "skipped": "already_running", "selected": 0})
		return
	}
	defer func() {
		_, _ = lockConn.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, int64(0x53544f415241504f))
	}()
	now := time.Now()
	targets, err := selectPreopenTargets(ctx, pool, now)
	if err != nil {
		log.Fatalf("select pre-open targets: %v", err)
	}
	if *dryRun {
		printJSON(map[string]any{"dry_run": true, "selected": len(targets)})
		return
	}
	results := probePreopenTargets(ctx, pool, targets, *concurrency, time.Duration(*probeSec)*time.Second)
	incidents := []healthIncident{}
	counts := map[string]int{"pass": 0, "fail": 0, "unknown": 0}
	for _, res := range results {
		counts[res.result]++
		if err := persistPreopenResult(ctx, pool, res); err != nil {
			log.Fatalf("persist pre-open check: %v", err)
		}
		if res.result == "pass" {
			if err := resolvePreopenAlerts(ctx, pool, res.target.recordingID); err != nil {
				log.Fatalf("resolve pre-open alert: %v", err)
			}
			continue
		}
		incidents = append(incidents, healthIncident{RecordingID: res.target.recordingID, StreamID: res.target.streamID, AccountID: res.target.accountID, RecName: res.target.recordingName, OrgName: res.target.orgName, OrgEmail: res.target.orgEmail, Signal: signalPreopenQualityGate, Severity: "HIGH", SinceText: fmt.Sprintf("%s validation is %s for window opening %s", res.target.stage, strings.ToUpper(res.result), res.target.windowStart.UTC().Format(time.RFC3339)), Diag: res.detail, Immediate: true})
	}
	notified, err := notifyPreopenIncidents(ctx, pool, cfg, incidents, now)
	if err != nil {
		log.Fatalf("notify pre-open incidents: %v", err)
	}
	printJSON(map[string]any{"dry_run": false, "selected": len(targets), "passed": counts["pass"], "failed": counts["fail"], "unknown": counts["unknown"], "notified": notified})
}

func selectPreopenTargets(ctx context.Context, pool *pgxpool.Pool, now time.Time) ([]preopenTarget, error) {
	rows, err := pool.Query(ctx, `
	WITH candidates AS (
	 SELECT r.id,COALESCE(r.stream_id,0) stream_id,r.account_id,r.name,a.name org_name,a.email,
	   COALESCE(s.provider,''),COALESCE(s.source_url,r.stream_url),COALESCE(s.source_page_url,''),r.capture_via,r.next_fire_at,
	   CASE WHEN r.next_fire_at>$1+interval '30 minutes' THEN 'early' ELSE 'confirm' END stage
	 FROM recordings r JOIN accounts a ON a.id=r.account_id LEFT JOIN streams s ON s.id=r.stream_id
	 WHERE r.status='active' AND r.mode='continuous' AND r.next_fire_at>$1 AND r.next_fire_at<=$1+interval '2 hours'
	), due AS (
	 SELECT c.*,COALESCE(p.attempt_count,0) prior_attempt
	 FROM candidates c LEFT JOIN recording_preopen_checks p ON p.recording_id=c.id AND p.window_start_at=c.next_fire_at AND p.stage=c.stage
	 WHERE p.recording_id IS NULL OR (c.stage='early' AND p.result<>'pass' AND p.attempt_count<3 AND p.next_retry_at<=$1)
	)
	SELECT id,stream_id,account_id,name,org_name,email,provider,source_url,source_page_url,capture_via,stage,next_fire_at,prior_attempt+1
	FROM due ORDER BY next_fire_at,id`, now)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []preopenTarget
	for rows.Next() {
		var t preopenTarget
		if err := rows.Scan(&t.recordingID, &t.streamID, &t.accountID, &t.recordingName, &t.orgName, &t.orgEmail, &t.provider, &t.sourceURL, &t.sourcePage, &t.captureVia, &t.stage, &t.windowStart, &t.attempt); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func probePreopenTargets(ctx context.Context, pool *pgxpool.Pool, targets []preopenTarget, limit int, probeWindow time.Duration) []preopenResult {
	jobs := make(chan preopenTarget)
	ch := make(chan preopenResult, len(targets))
	var wg sync.WaitGroup
	workerCount := min(limit, len(targets))
	for range workerCount {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for t := range jobs {
				ch <- probePreopenTarget(ctx, pool, t, probeWindow)
			}
		}()
	}
	for _, t := range targets {
		jobs <- t
	}
	close(jobs)
	wg.Wait()
	close(ch)
	out := make([]preopenResult, 0, len(targets))
	for r := range ch {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].target.recordingID < out[j].target.recordingID })
	return out
}

func probePreopenTarget(ctx context.Context, pool *pgxpool.Pool, t preopenTarget, probeWindow time.Duration) preopenResult {
	if t.captureVia == "relay" {
		// A generic catalog frame proves the source was visible somewhere, not that
		// the recording's intended relay group/uplink can capture it. The cron does
		// not possess a relay identity and therefore cannot safely run the existing
		// node-local reservation-backed canary. Fail closed instead of certifying the
		// wrong failure domain. Relay canary orchestration belongs on an idle relay.
		return preopenResult{t, "unknown", "relay_canary", "intended relay path was not reservation-canary validated"}
	}
	probeCtx, cancel := context.WithTimeout(ctx, probeWindow+35*time.Second)
	defer cancel()
	resolved, isImage, headers, err := capture.ResolveCaptureInputWithHeaders(probeCtx, t.provider, t.sourceURL, t.sourcePage)
	if err != nil {
		return preopenResult{t, "fail", "media_probe", sanitizePreopenDetail("resolve: " + err.Error())}
	}
	if isImage {
		return preopenResult{t, "fail", "media_probe", "resolved source is an image, not video"}
	}
	if _, err = netguard.ValidatePublicURL(resolved); err != nil {
		return preopenResult{t, "fail", "media_probe", sanitizePreopenDetail("source guard: " + err.Error())}
	}
	seg, err := capture.CaptureSegmentInDirWithHeadersNoThumbnail(probeCtx, resolved, probeWindow, "", "", headers)
	if seg.Path != "" {
		_ = os.RemoveAll(filepath.Dir(seg.Path))
	}
	if err != nil {
		result := "fail"
		low := strings.ToLower(err.Error())
		if strings.Contains(low, "executable file not found") || strings.Contains(low, "no such file or directory") || strings.Contains(low, "mktemp") {
			result = "unknown"
		}
		return preopenResult{t, result, "media_probe", sanitizePreopenDetail(err.Error())}
	}
	if seg.SizeBytes <= 0 || seg.DurationMs <= 0 || strings.TrimSpace(seg.VideoCodec) == "" {
		return preopenResult{t, "fail", "media_probe", "captured segment has no verified video"}
	}
	return preopenResult{t, "pass", "media_probe", fmt.Sprintf("native segment verified duration_ms=%d size_bytes=%d codec=%s", seg.DurationMs, seg.SizeBytes, seg.VideoCodec)}
}

func persistPreopenResult(ctx context.Context, pool *pgxpool.Pool, r preopenResult) error {
	var retry any
	if r.target.stage == "early" && r.result != "pass" && r.target.attempt < 3 {
		retry = time.Now().Add(15 * time.Minute)
	}
	_, err := pool.Exec(ctx, `INSERT INTO recording_preopen_checks(recording_id,window_start_at,stage,result,method,detail,attempt_count,next_retry_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT(recording_id,window_start_at,stage) DO UPDATE SET checked_at=now(),result=EXCLUDED.result,method=EXCLUDED.method,detail=EXCLUDED.detail,attempt_count=EXCLUDED.attempt_count,next_retry_at=EXCLUDED.next_retry_at`, r.target.recordingID, r.target.windowStart, r.target.stage, r.result, r.method, r.detail, r.target.attempt, retry)
	return err
}

func notifyPreopenIncidents(ctx context.Context, pool *pgxpool.Pool, cfg config.Config, incidents []healthIncident, now time.Time) (int, error) {
	toNotify := []healthIncident{}
	for _, inc := range incidents {
		inserted, episode, last, attempt, err := upsertHealthAlert(ctx, pool, inc.RecordingID, inc.Signal)
		if err != nil {
			return 0, err
		}
		if shouldNotifyHealthIncident(inc, inserted, episode, last, attempt, now) {
			toNotify = append(toNotify, inc)
		}
	}
	if len(toNotify) == 0 {
		return 0, nil
	}
	if err := markHealthAlertsDeliveryAttempted(ctx, pool, toNotify); err != nil {
		return 0, err
	}
	sent, err := deliverRecordingHealthEmail(ctx, pool, cfg, toNotify)
	if err != nil {
		return 0, err
	}
	return sent, markHealthAlertsDelivered(ctx, pool, toNotify)
}

func resolvePreopenAlerts(ctx context.Context, pool *pgxpool.Pool, recordingID int64) error {
	_, err := pool.Exec(ctx, `UPDATE recorder_health_alerts SET resolved_at=now() WHERE recording_id=$1 AND signal=$2 AND resolved_at IS NULL`, recordingID, signalPreopenQualityGate)
	return err
}
