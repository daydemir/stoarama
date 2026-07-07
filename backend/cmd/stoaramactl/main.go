package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"image"
	_ "image/jpeg"
	_ "image/png"
	"io"
	"log"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "modernc.org/sqlite"

	"github.com/daydemir/stoarama/backend/internal/capture"
	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/db"
	"github.com/daydemir/stoarama/backend/internal/model"
	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/daydemir/stoarama/backend/internal/storage"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	ctx := context.Background()
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	switch os.Args[1] {
	case "-h", "--help", "help":
		usage()
		return
	case "migrate":
		runMigrate(ctx, cfg, os.Args[2:])
	case "capture":
		runCapture(ctx, cfg, os.Args[2:])
	case "streams":
		runStreams(ctx, cfg, os.Args[2:])
	case "accounts":
		runAccounts(ctx, cfg, os.Args[2:])
	case "discovery":
		runDiscovery(ctx, cfg, os.Args[2:])
	case "media":
		runMedia(ctx, cfg, os.Args[2:])
	case "inference":
		runInference(ctx, cfg, os.Args[2:])
	case "alerts":
		runAlerts(ctx, cfg, os.Args[2:])
	case "overview":
		runOverview(ctx, cfg, os.Args[2:])
	case "korea":
		runKorea(ctx, cfg, os.Args[2:])
	case "import":
		runImport(ctx, cfg, os.Args[2:])
	case "pipelines":
		runPipelines(ctx, cfg, os.Args[2:])
	case "nodes":
		runNodes(ctx, cfg, os.Args[2:])
	case "survey":
		runSurvey(ctx, cfg, os.Args[2:])
	case "survey-droplet":
		runSurveyDroplet(ctx, cfg, os.Args[2:])
	case "recordability":
		runRecordability(ctx, cfg, os.Args[2:])
	case "recorder-control":
		runRecorderControl(ctx, cfg, os.Args[2:])
	case "recording-worker":
		runRecordingWorker(ctx, cfg, os.Args[2:])
	case "recording-health":
		runRecordingHealth(ctx, cfg, os.Args[2:])
	case "recordings":
		runRecordings(ctx, cfg, os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	_, _ = os.Stdout.WriteString(`stoaramactl commands:
	  stoaramactl migrate up [--dir infra/sql/migrations]
	  stoaramactl capture backfill-missing [--backend-api-url URL --api-token TOKEN --limit 0 --concurrency 4 --timeout-sec 90 --dry-run --json]
	  stoaramactl capture probe (--id N | --provider P --source-url URL) [--source-page-url URL --capture-type TYPE --capture-timeout-sec 60]
	  stoaramactl capture audit --all [--concurrency 16 --timeout-sec 20 --json]
	  stoaramactl capture runtime list [--status running|unsupported|error] [--limit 200] [--json]
	  stoaramactl capture runtime show --id N [--json]
	  stoaramactl capture runtime reset --id N
	  stoaramactl streams list [--recording-state off|on --capture-type TYPE --tags a,b --limit 200]
	  stoaramactl streams detail (--id N | --slug S) [--pipeline-id P --results-limit 10 --detections-limit 50]
	  stoaramactl streams filters --kind tags|countries|cities|sources|youtube-channels [--recording-state off|on --capture-type TYPE --country C --city CITY --source SRC --youtube-channel CH --tags a,b --tags-not x,y]
	  stoaramactl streams frames [--stream-id N --pipeline-id P --uninferenced --unprocessed --sort-by captured_at --sort-dir desc --limit 200 --offset 0]
	  stoaramactl streams clips --stream-id N [--limit 100 --offset 0]
	  stoaramactl streams clip-latest --stream-id N
	  stoaramactl streams image-urls --stream-ids 1,2,3
	  stoaramactl streams add --source-url URL --name N [--provider P --external-id E --slug S --source-page-url URL --capture-type TYPE --execution-config-json JSON --location-country C --location-country-code CC --location-region R --location-city CITY --location-locality L --location-source SRC --tags a,b]
	  stoaramactl streams update --id N [--name N --slug S --source-url URL --recording-state off|on --tags a,b --capture-type TYPE --execution-config-json JSON --location-country C --location-country-code CC --location-region R --location-city CITY --location-locality L --location-source SRC]
	  stoaramactl streams soft-delete --id N --reason TEXT [--dry-run]
	  stoaramactl streams tags-add (--id N | --slug S) --tags a,b
	  stoaramactl streams tags-remove (--id N | --slug S) --tags a,b
	  stoaramactl streams cleanup-location-tags [--recording-state off|on --limit 0 --apply --json]
	  stoaramactl streams metadata-audit [--backend-api-url URL --api-token TOKEN --recording-state off|on --page-size 500 --sample-limit 40 --allow-generic-location-city --apply --apply-generic-location-fixes --max-updates 0]
	  stoaramactl streams set-capture --id N --capture-type TYPE [--config-json JSON]
	  stoaramactl streams repair-youtube [--id N --limit 1000 --only-changed --apply --report-json out.json --json]
	  stoaramactl streams repair-image-capture [--id N --source-url-like %%pattern%% --provider P --limit 1000 --only-changed --apply --json]
	  stoaramactl streams repair-canonical-capture [--id N --source-url-like %%pattern%% --provider P --limit 1000 --only-changed --only-review --legacy-imported-only=true --non-youtube-only=true --apply --json]
	  stoaramactl discovery candidates list [--id N --review-status pending|accepted|rejected|invalid --provider P --capture-type TYPE --limit 200 --offset 0]
	  stoaramactl discovery candidates review --id N --status accepted|rejected|invalid [--reviewer TEXT --reason TEXT --metadata-json JSON]
	  stoaramactl discovery candidates import --id N [--provider P --external-id E --name N --slug S --source-url URL --source-page-url URL --source-family FAMILY --capture-type TYPE --execution-class CLASS --execution-config-json JSON --tags a,b --location-country C --location-country-code CC --location-region R --location-city CITY --location-locality L --location-source SRC --metadata-json JSON]
	  stoaramactl accounts promote-admin --email EMAIL [--backend-api-url URL --api-token TOKEN]
  stoaramactl media backfill --snapshot-root local/snapshots [--concurrency 8 --dry-run]
  stoaramactl inference list [--stream-id N --pipeline-id P --status queued_boxed|success|error --class-name person --search TEXT --min-confidence 0.5 --sort-by created_at --sort-dir desc --limit 200 --offset 0]
  stoaramactl inference cleanup-unboxed [--pipeline-id P --mode requeue|delete --dry-run]
	  stoaramactl alerts send-test-email [--to email@example.com] [--stream-id N --stream-name NAME --reason capture_runtime_stopped]
	  stoaramactl alerts history [--limit 50 --status accepted|delivered|opened|bounced|failed --stream-id N]
	  stoaramactl import bellevue-streams [--cam-query-url URL --source-page-url URL --target-api-url URL --service-token TOKEN --limit 0 --concurrency 8 --probe-timeout-sec 15 --apply --report-json out.json --json]
	  stoaramactl import global-street-scores [--nils-csv PATH --vittorio-csv PATH --target-api-url URL --service-token TOKEN --limit 0 --concurrency 8 --probe-timeout-sec 60 --apply --review-approved --apply-report verify.json --cleanup-tags-report import.json --report-json out.json --json]
	  stoaramactl overview summary [--backend-api-url URL --api-token TOKEN]
	  stoaramactl overview queue-health [--backend-api-url URL --api-token TOKEN]
	  stoaramactl pipelines list
	  stoaramactl pipelines register --id P --family FAMILY [--kind detector --spec-json JSON --active=true --owner-email EMAIL] [--backend-api-url URL --api-token TOKEN]
	  stoaramactl pipelines versions sync --pipeline-id P --version-id V [--runner-kind external --spec-json JSON --created-by stoaramactl --owner-email EMAIL] [--backend-api-url URL --api-token TOKEN]
	  stoaramactl pipelines versions list [--pipeline-id P] [--backend-api-url URL --api-token TOKEN]
	  stoaramactl pipelines runs create --pipeline-id P --version-id V [--label LABEL --worker-kind external --frame-ids 1,2 --stream-ids 3,4 --tags a,b --latest-only-per-stream --limit 100 --metadata-json JSON --created-by stoaramactl --owner-email EMAIL] [--backend-api-url URL --api-token TOKEN]
	  stoaramactl pipelines runs list [--pipeline-id P --limit 200 --offset 0] [--backend-api-url URL --api-token TOKEN]
	  stoaramactl pipelines runs get --id N [--backend-api-url URL --api-token TOKEN]
	  stoaramactl pipelines runs claim --id N --claimed-by WORKER [--limit 100 --lease-sec 600 --force-rerun] [--backend-api-url URL --api-token TOKEN]
	  stoaramactl pipelines runs complete --claim-id N --pipeline-id P --pipeline-run-id N --frame-id N --claimed-by WORKER [--pipeline-version-id N --summary-json JSON --raw-output-json JSON --runner-info-json JSON --detections-json JSON --signals-json JSON --started-at RFC3339 --finished-at RFC3339 --force-rerun --revision-mode force_rerun] [--backend-api-url URL --api-token TOKEN]
	  stoaramactl pipelines runs fail --claim-id N --pipeline-id P --pipeline-run-id N --frame-id N --claimed-by WORKER --error-text TEXT [--pipeline-version-id N --runner-info-json JSON] [--backend-api-url URL --api-token TOKEN]
	  stoaramactl nodes enrollment-token create --owner-email EMAIL --node-type inference_node|local_recorder [--label LABEL --expires-at RFC3339] [--backend-api-url URL --api-token TOKEN]
	  stoaramactl pipelines overview [--backend-api-url URL --api-token TOKEN --include-inactive=true]
	  stoaramactl pipelines stream-list --id N [--backend-api-url URL --api-token TOKEN]
	  stoaramactl pipelines set --stream-id N --pipeline-id P --enabled=true|false [--updated-by stoaramactl --backend-api-url URL --api-token TOKEN]
	  stoaramactl korea inventory
	  stoaramactl korea audit
	  stoaramactl korea utic scrape [--api-url URL --service-key KEY --out report.json --json]
	  stoaramactl korea utic ingest [--api-url URL --service-key KEY --backend-api-url URL --api-token TOKEN --auto-import=true --dry-run --limit 0 --report-json out.json --json]
	  stoaramactl korea utic refresh-frames [--backend-api-url URL --api-token TOKEN --concurrency 4 --timeout-sec 90 --limit 0 --dry-run --allow-failures --report-json out.json --json]
	  stoaramactl survey run-once [--limit 0 --daily-gate --concurrency 4 --resolve-timeout-sec 60 --capture-timeout-sec 60 --json]
	  stoaramactl survey relay-worker [--backend-api-url URL --node-token TOKEN --concurrency 1 --poll-sec 30 --duration 0 --detect]
	  stoaramactl survey coverage [--json]
	  stoaramactl survey delete-stream-captures --id N --apply
	  stoaramactl recordability run-once [--batch 1 --window-sec 600 --segment-sec 60 --probe-host LABEL --json] (gated by STREAM_RECORDABILITY_PROBE_ENABLED)
	  stoaramactl recorder-control run
	  stoaramactl recording-worker run [--backend-api-url URL --node-token TOKEN --worker-id ID --concurrency 1 --heartbeat-sec 15 --poll-sec 5 --duration 0]
	  stoaramactl recording-health run [--dry-run --freshness-min 10]
	  stoaramactl recordings naming get|set|preview
`)
}

func runImport(ctx context.Context, cfg config.Config, args []string) {
	if len(args) < 1 {
		printImportUsage()
		return
	}
	switch args[0] {
	case "bellevue-streams":
		runImportBellevueStreams(ctx, cfg, args[1:])
	case "global-street-scores":
		runImportGlobalStreetScores(ctx, cfg, args[1:])
	default:
		log.Fatalf("unknown import subcommand: %s", args[0])
	}
}

func printImportUsage() {
	fmt.Print(`stoaramactl import commands:
  stoaramactl import bellevue-streams [--cam-query-url URL --source-page-url URL --target-api-url URL --service-token TOKEN --limit 0 --concurrency 8 --probe-timeout-sec 15 --apply --report-json out.json --json]
  stoaramactl import global-street-scores [--nils-csv PATH --vittorio-csv PATH --target-api-url URL --service-token TOKEN --limit 0 --concurrency 8 --probe-timeout-sec 60 --apply --review-approved --apply-report verify.json --cleanup-tags-report import.json --report-json out.json --json]
`)
}

func postJSONWithToken(ctx context.Context, baseURL, token, path string, payload, out any) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(baseURL, "/")+path, strings.NewReader(string(b)))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(token))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var errPayload map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&errPayload)
		return fmt.Errorf("POST %s: status=%d body=%v", path, resp.StatusCode, errPayload)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func normalizeSourceFamily(v string) string {
	if normalized, ok := capture.NormalizeSourceFamily(v); ok {
		return normalized
	}
	return ""
}

func normalizeCaptureType(v string) string {
	if normalized, ok := capture.NormalizeCaptureType(v); ok {
		return normalized
	}
	return ""
}

func normalizeExecutionClass(v string) string {
	if normalized, ok := capture.NormalizeExecutionClass(v); ok {
		return normalized
	}
	return ""
}

func runMigrate(ctx context.Context, cfg config.Config, args []string) {
	if len(args) < 1 || args[0] != "up" {
		log.Fatalf("usage: stoaramactl migrate up [--dir infra/sql/migrations]")
	}
	fs := flag.NewFlagSet("migrate up", flag.ExitOnError)
	dir := fs.String("dir", cfg.MigrationDir, "migration directory")
	_ = fs.Parse(args[1:])
	pool := mustOpenPool(ctx, cfg)
	defer pool.Close()
	if err := db.MigrateUp(ctx, pool, *dir); err != nil {
		log.Fatalf("migrate up: %v", err)
	}
	fmt.Println("migrations applied")
}

func runCapture(ctx context.Context, cfg config.Config, args []string) {
	if len(args) < 1 {
		log.Fatalf("usage: stoaramactl capture <backfill-missing|probe|classify|audit|runtime> ...")
	}
	registry, err := capture.NewDefaultRegistry()
	if err != nil {
		log.Fatalf("init capture registry: %v", err)
	}

	sub := args[0]
	switch sub {
	case "backfill-missing":
		runCaptureBackfillMissing(ctx, cfg, args[1:])
	case "probe":
		fs := flag.NewFlagSet("capture probe", flag.ExitOnError)
		id := fs.Int64("id", 0, "stream id from database")
		provider := fs.String("provider", "", "provider (required when --id is not provided)")
		sourceURL := fs.String("source-url", "", "source URL (required when --id is not provided)")
		sourcePageURL := fs.String("source-page-url", "", "optional source page URL")
		captureTypeRaw := fs.String("capture-type", "", "capture type override")
		captureTimeoutSec := fs.Int("capture-timeout-sec", 60, "one-frame capture timeout seconds")
		saveFrameDir := fs.String("save-frame-dir", "", "optional directory to write captured frame image")
		_ = fs.Parse(args[1:])

		if *captureTimeoutSec <= 0 {
			log.Fatalf("--capture-timeout-sec must be > 0")
		}
		captureTypeValue := strings.TrimSpace(*captureTypeRaw)
		executionClassValue := ""

		if *id > 0 {
			pool := mustOpenPool(ctx, cfg)
			defer pool.Close()
			if err := pool.QueryRow(ctx, `
				SELECT provider, source_url, source_page_url, capture_type, execution_class
				FROM streams
				WHERE id=$1
			`, *id).Scan(provider, sourceURL, sourcePageURL, &captureTypeValue, &executionClassValue); err != nil {
				log.Fatalf("load stream id=%d: %v", *id, err)
			}
		}

		*provider = strings.TrimSpace(*provider)
		*sourceURL = strings.TrimSpace(*sourceURL)
		*sourcePageURL = strings.TrimSpace(*sourcePageURL)
		if *provider == "" || *sourceURL == "" {
			log.Fatalf("provide --id, or both --provider and --source-url")
		}
		captureTypeValue, _, executionClassValue = deriveCanonicalStreamFields(*sourceURL, *sourcePageURL, captureTypeValue, "", executionClassValue)
		mode := capture.LegacyModeForStream(captureTypeValue, executionClassValue)

		spec := capture.StreamSpec{
			ID:            *id,
			Provider:      *provider,
			StreamURL:     *sourceURL,
			SourcePageURL: *sourcePageURL,
			CaptureMode:   mode,
			TargetFPS:     1,
		}
		effective := capture.EffectiveMode(spec)
		if effective == capture.ModeUnsupported {
			log.Fatalf("capture mode unsupported for input")
		}
		adapter, ok := registry.Get(effective)
		if !ok {
			log.Fatalf("adapter not found for mode %s", effective)
		}
		resolveCtx, cancelResolve := context.WithTimeout(ctx, 60*time.Second)
		resolved, err := adapter.Resolve(resolveCtx, spec)
		cancelResolve()
		if err != nil {
			log.Fatalf("resolve capture source: %v", err)
		}

		fmt.Printf("probe provider=%s\n", *provider)
		fmt.Printf("probe capture_type=%s execution_class=%s runtime_mode=%s\n", captureTypeValue, executionClassValue, effective)
		fmt.Printf("probe source_url=%s\n", *sourceURL)
		if *sourcePageURL != "" {
			fmt.Printf("probe source_page_url=%s\n", *sourcePageURL)
		}
		fmt.Printf("resolved_url=%s\n", resolved.URL)
		fmt.Printf("resolved_is_image=%t\n", resolved.IsImage)

		capCtx, cancelCap := context.WithTimeout(ctx, time.Duration(*captureTimeoutSec)*time.Second)
		defer cancelCap()
		start := time.Now()
		frame, err := capture.CaptureFrame(capCtx, resolved.URL)
		if err != nil {
			log.Fatalf("capture frame: %v", err)
		}
		elapsed := time.Since(start).Round(time.Millisecond)
		fmt.Printf("capture_ok elapsed=%s width=%d height=%d size_bytes=%d source_kind=%s sha256=%s\n",
			elapsed, frame.Width, frame.Height, frame.SizeBytes, frame.SourceKind, frame.SHA256)
		if strings.TrimSpace(*saveFrameDir) != "" {
			if err := os.MkdirAll(strings.TrimSpace(*saveFrameDir), 0o755); err != nil {
				log.Fatalf("create save-frame-dir: %v", err)
			}
			fileBase := fmt.Sprintf("probe-%d-%s-%d", time.Now().UTC().Unix(), sanitizeFilename(*provider), *id)
			ext := ".jpg"
			if strings.Contains(strings.ToLower(frame.MIMEType), "png") {
				ext = ".png"
			}
			outPath := filepath.Join(strings.TrimSpace(*saveFrameDir), fileBase+ext)
			if err := os.WriteFile(outPath, frame.Bytes, 0o644); err != nil {
				log.Fatalf("write probe frame: %v", err)
			}
			fmt.Printf("saved_frame=%s\n", outPath)
		}
	case "audit":
		fs := flag.NewFlagSet("capture audit", flag.ExitOnError)
		all := fs.Bool("all", false, "audit all streams")
		concurrency := fs.Int("concurrency", 16, "worker concurrency")
		timeoutSec := fs.Int("timeout-sec", 20, "per-stream timeout seconds")
		jsonOut := fs.Bool("json", false, "output JSON")
		_ = fs.Parse(args[1:])
		if !*all {
			log.Fatalf("capture audit currently requires --all")
		}
		if *concurrency <= 0 {
			log.Fatalf("--concurrency must be > 0")
		}
		if *timeoutSec <= 0 {
			log.Fatalf("--timeout-sec must be > 0")
		}
		pool := mustOpenPool(ctx, cfg)
		defer pool.Close()
		rows, err := pool.Query(ctx, `
				SELECT id, provider, source_url, source_page_url, capture_type, execution_class, execution_config_jsonb
				FROM streams
				ORDER BY id ASC
			`)
		if err != nil {
			log.Fatalf("query streams: %v", err)
		}
		type auditStream struct {
			ID             int64
			Provider       string
			StreamURL      string
			SourcePageURL  string
			CaptureType    string
			ExecutionClass string
			Cfg            map[string]any
		}
		streams := make([]auditStream, 0, 2048)
		for rows.Next() {
			var s auditStream
			var captureTypeRaw string
			var executionClassRaw string
			var cfgRaw []byte
			if err := rows.Scan(&s.ID, &s.Provider, &s.StreamURL, &s.SourcePageURL, &captureTypeRaw, &executionClassRaw, &cfgRaw); err != nil {
				log.Fatalf("scan stream: %v", err)
			}
			s.CaptureType = captureTypeRaw
			s.ExecutionClass = executionClassRaw
			s.Cfg = map[string]any{}
			if len(cfgRaw) > 0 {
				_ = json.Unmarshal(cfgRaw, &s.Cfg)
			}
			streams = append(streams, s)
		}
		rows.Close()
		type auditResult struct {
			ID             int64  `json:"id"`
			Provider       string `json:"provider"`
			CaptureType    string `json:"capture_type"`
			ExecutionClass string `json:"execution_class"`
			RuntimeMode    string `json:"runtime_mode"`
			ResolvedURL    string `json:"resolved_url,omitempty"`
			OK             bool   `json:"ok"`
			Reason         string `json:"reason,omitempty"`
			Width          int    `json:"width,omitempty"`
			Height         int    `json:"height,omitempty"`
			SizeBytes      int64  `json:"size_bytes,omitempty"`
		}
		workCh := make(chan auditStream)
		resCh := make(chan auditResult, len(streams))
		var wg sync.WaitGroup
		for i := 0; i < *concurrency; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for s := range workCh {
					spec := capture.StreamSpec{
						ID:                 s.ID,
						Provider:           s.Provider,
						StreamURL:          s.StreamURL,
						SourcePageURL:      s.SourcePageURL,
						CaptureMode:        capture.LegacyModeForStream(s.CaptureType, s.ExecutionClass),
						CaptureConfig:      s.Cfg,
						CaptureIntervalSec: maxInt(1, capture.GetConfigInt(s.Cfg, "poll_interval_sec", 1)),
						TargetFPS:          capture.SegmentTargetFPS,
					}
					effective := capture.EffectiveMode(spec)
					item := auditResult{
						ID:             s.ID,
						Provider:       s.Provider,
						CaptureType:    s.CaptureType,
						ExecutionClass: s.ExecutionClass,
						RuntimeMode:    string(effective),
					}
					if effective == capture.ModeUnsupported {
						item.Reason = "unsupported_capture_type"
						resCh <- item
						continue
					}
					adapter, ok := registry.Get(effective)
					if !ok {
						item.Reason = "adapter_missing"
						resCh <- item
						continue
					}
					probeCtx, cancel := context.WithTimeout(ctx, time.Duration(*timeoutSec)*time.Second)
					resolved, err := adapter.Resolve(probeCtx, spec)
					if err != nil {
						cancel()
						item.Reason = err.Error()
						resCh <- item
						continue
					}
					item.ResolvedURL = resolved.URL
					frame, err := capture.CaptureFrame(probeCtx, resolved.URL)
					cancel()
					if err != nil {
						item.Reason = err.Error()
						resCh <- item
						continue
					}
					item.OK = true
					item.Width = frame.Width
					item.Height = frame.Height
					item.SizeBytes = frame.SizeBytes
					resCh <- item
				}
			}()
		}
		go func() {
			for _, s := range streams {
				workCh <- s
			}
			close(workCh)
			wg.Wait()
			close(resCh)
		}()
		results := make([]auditResult, 0, len(streams))
		var okCount, failCount, unsupportedCount int
		for r := range resCh {
			results = append(results, r)
			if r.OK {
				okCount++
				continue
			}
			failCount++
			if r.RuntimeMode == string(capture.ModeUnsupported) {
				unsupportedCount++
			}
		}
		if *jsonOut {
			printJSON(map[string]any{
				"total":             len(results),
				"ok":                okCount,
				"fail":              failCount,
				"unsupported_count": unsupportedCount,
				"items":             results,
			})
		} else {
			for _, r := range results {
				fmt.Printf("id=%d provider=%s capture_type=%s execution_class=%s runtime_mode=%s ok=%t reason=%q size=%d\n", r.ID, r.Provider, r.CaptureType, r.ExecutionClass, r.RuntimeMode, r.OK, r.Reason, r.SizeBytes)
			}
			fmt.Printf("total=%d ok=%d fail=%d unsupported=%d\n", len(results), okCount, failCount, unsupportedCount)
		}
		if unsupportedCount > 0 {
			os.Exit(3)
		}
		if failCount > 0 {
			os.Exit(2)
		}
	case "runtime":
		if len(args) < 2 {
			log.Fatalf("usage: stoaramactl capture runtime <list|show|reset> ...")
		}
		action := args[1]
		pool := mustOpenPool(ctx, cfg)
		defer pool.Close()
		switch action {
		case "list":
			fs := flag.NewFlagSet("capture runtime list", flag.ExitOnError)
			status := fs.String("status", "", "optional status filter")
			limit := fs.Int("limit", 200, "row limit")
			jsonOut := fs.Bool("json", false, "output JSON")
			_ = fs.Parse(args[2:])
			where := "1=1"
			queryArgs := []any{*limit}
			if strings.TrimSpace(*status) != "" {
				where = "r.status=$1"
				queryArgs = []any{strings.TrimSpace(*status), *limit}
			}
			rows, err := pool.Query(ctx, fmt.Sprintf(`
				SELECT r.stream_id, s.provider, s.slug, r.status, r.execution_class, r.resolved_url, r.last_resolved_at, r.last_frame_at, r.consecutive_errors, r.last_error_text, r.updated_at
				FROM stream_capture_runtime r
				JOIN streams s ON s.id=r.stream_id
				WHERE %s
				ORDER BY r.updated_at DESC, r.stream_id ASC
				LIMIT $%d
			`, where, len(queryArgs)), queryArgs...)
			if err != nil {
				log.Fatalf("runtime list query: %v", err)
			}
			defer rows.Close()
			type rtItem struct {
				StreamID          int64      `json:"stream_id"`
				Provider          string     `json:"provider"`
				Slug              string     `json:"slug"`
				Status            string     `json:"status"`
				EffectiveMode     *string    `json:"execution_class,omitempty"`
				ResolvedURL       *string    `json:"resolved_url,omitempty"`
				LastResolvedAt    *time.Time `json:"last_resolved_at,omitempty"`
				LastFrameAt       *time.Time `json:"last_frame_at,omitempty"`
				ConsecutiveErrors int        `json:"consecutive_errors"`
				LastErrorText     *string    `json:"last_error_text,omitempty"`
				UpdatedAt         time.Time  `json:"updated_at"`
			}
			out := make([]rtItem, 0, *limit)
			for rows.Next() {
				var it rtItem
				if err := rows.Scan(&it.StreamID, &it.Provider, &it.Slug, &it.Status, &it.EffectiveMode, &it.ResolvedURL, &it.LastResolvedAt, &it.LastFrameAt, &it.ConsecutiveErrors, &it.LastErrorText, &it.UpdatedAt); err != nil {
					log.Fatalf("runtime list scan: %v", err)
				}
				out = append(out, it)
			}
			if rows.Err() != nil {
				log.Fatalf("runtime list iterate: %v", rows.Err())
			}
			if *jsonOut {
				printJSON(map[string]any{"items": out, "limit": *limit})
				return
			}
			for _, it := range out {
				fmt.Printf("stream_id=%d provider=%s slug=%s status=%s execution_class=%s errors=%d last_error=%s\n",
					it.StreamID, it.Provider, it.Slug, it.Status, derefString(it.EffectiveMode), it.ConsecutiveErrors, derefString(it.LastErrorText))
			}
		case "show":
			fs := flag.NewFlagSet("capture runtime show", flag.ExitOnError)
			id := fs.Int64("id", 0, "stream id")
			jsonOut := fs.Bool("json", false, "output JSON")
			_ = fs.Parse(args[2:])
			if *id <= 0 {
				log.Fatalf("--id is required")
			}
			type rtItem struct {
				StreamID          int64      `json:"stream_id"`
				Provider          string     `json:"provider"`
				Slug              string     `json:"slug"`
				Status            string     `json:"status"`
				EffectiveMode     *string    `json:"execution_class,omitempty"`
				ResolvedURL       *string    `json:"resolved_url,omitempty"`
				LastResolvedAt    *time.Time `json:"last_resolved_at,omitempty"`
				LastFrameAt       *time.Time `json:"last_frame_at,omitempty"`
				ConsecutiveErrors int        `json:"consecutive_errors"`
				LastErrorText     *string    `json:"last_error_text,omitempty"`
				UpdatedAt         time.Time  `json:"updated_at"`
			}
			var it rtItem
			err := pool.QueryRow(ctx, `
				SELECT r.stream_id, s.provider, s.slug, r.status, r.execution_class, r.resolved_url, r.last_resolved_at, r.last_frame_at, r.consecutive_errors, r.last_error_text, r.updated_at
				FROM stream_capture_runtime r
				JOIN streams s ON s.id=r.stream_id
				WHERE r.stream_id=$1
			`, *id).Scan(&it.StreamID, &it.Provider, &it.Slug, &it.Status, &it.EffectiveMode, &it.ResolvedURL, &it.LastResolvedAt, &it.LastFrameAt, &it.ConsecutiveErrors, &it.LastErrorText, &it.UpdatedAt)
			if err != nil {
				log.Fatalf("runtime show: %v", err)
			}
			if *jsonOut {
				printJSON(it)
				return
			}
			fmt.Printf("stream_id=%d provider=%s slug=%s status=%s execution_class=%s resolved=%s errors=%d last_error=%s\n",
				it.StreamID, it.Provider, it.Slug, it.Status, derefString(it.EffectiveMode), derefString(it.ResolvedURL), it.ConsecutiveErrors, derefString(it.LastErrorText))
		case "reset":
			fs := flag.NewFlagSet("capture runtime reset", flag.ExitOnError)
			id := fs.Int64("id", 0, "stream id")
			_ = fs.Parse(args[2:])
			if *id <= 0 {
				log.Fatalf("--id is required")
			}
			tx, err := pool.Begin(ctx)
			if err != nil {
				log.Fatalf("begin tx: %v", err)
			}
			defer func() { _ = tx.Rollback(ctx) }()
			if _, err := tx.Exec(ctx, `
				INSERT INTO stream_capture_runtime (stream_id, status, consecutive_errors, last_error_text)
				VALUES ($1, 'idle', 0, NULL)
				ON CONFLICT (stream_id)
				DO UPDATE SET status='idle', consecutive_errors=0, last_error_text=NULL, updated_at=now()
			`, *id); err != nil {
				log.Fatalf("reset runtime: %v", err)
			}
			if err := tx.Commit(ctx); err != nil {
				log.Fatalf("commit runtime reset: %v", err)
			}
			fmt.Printf("capture runtime reset for stream %d\n", *id)
		default:
			log.Fatalf("unknown capture runtime subcommand: %s", action)
		}
	default:
		log.Fatalf("unknown capture subcommand: %s", sub)
	}
}

func defaultManualExternalID(sourceURL string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(sourceURL)))
	return "manual-" + hex.EncodeToString(sum[:])[:12]
}

func slugifyForCLI(raw string) string {
	v := strings.ToLower(strings.TrimSpace(raw))
	if v == "" {
		return "stream"
	}
	var b strings.Builder
	prevDash := false
	for _, r := range v {
		ok := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')
		if ok {
			b.WriteRune(r)
			prevDash = false
			continue
		}
		if !prevDash {
			b.WriteRune('-')
			prevDash = true
		}
	}
	slug := strings.Trim(b.String(), "-")
	if slug == "" {
		return "stream"
	}
	return slug
}

func deriveCanonicalStreamFields(sourceURL string, sourcePageURL string, captureTypeRaw string, sourceFamilyRaw string, executionClassRaw string) (string, string, string) {
	fields, err := capture.DeriveCanonicalStreamFields(sourceURL, sourcePageURL, captureTypeRaw, sourceFamilyRaw, executionClassRaw)
	if err != nil {
		log.Fatalf("%v", err)
	}
	return fields.CaptureType, fields.SourceFamily, fields.ExecutionClass
}

type streamCreateCLIOptions struct {
	BackendAPIURL       string
	APIToken            string
	Provider            string
	ExternalID          string
	Name                string
	Slug                string
	SourceURL           string
	SourcePageURL       string
	TagsCSV             string
	CaptureType         string
	SourceFamily        string
	ExecutionClass      string
	ExecutionConfigJSON string
	PollIntervalSec     int
	LocationCountry     string
	LocationCountryCode string
	LocationRegion      string
	LocationCity        string
	LocationLocality    string
	LocationSource      string
}

func decodeExecutionConfigJSON(raw string, pollIntervalSec int) map[string]any {
	var cfgJSON map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(raw)), &cfgJSON); err != nil {
		log.Fatalf("invalid --execution-config-json: %v", err)
	}
	if pollIntervalSec > 0 {
		if _, ok := cfgJSON["poll_interval_sec"]; !ok {
			cfgJSON["poll_interval_sec"] = pollIntervalSec
		}
	}
	return cfgJSON
}

func createStreamFromCLI(ctx context.Context, opts streamCreateCLIOptions) (map[string]any, string, string) {
	cfgJSON := decodeExecutionConfigJSON(opts.ExecutionConfigJSON, opts.PollIntervalSec)
	captureTypeValue, sourceFamilyValue, executionClassValue := deriveCanonicalStreamFields(
		opts.SourceURL,
		opts.SourcePageURL,
		opts.CaptureType,
		opts.SourceFamily,
		opts.ExecutionClass,
	)
	payload := mustAPIRequest(ctx, http.MethodPost, strings.TrimSpace(opts.BackendAPIURL), strings.TrimSpace(opts.APIToken), "/api/v1/streams", map[string]any{
		"provider":              strings.TrimSpace(opts.Provider),
		"external_id":           strings.TrimSpace(opts.ExternalID),
		"name":                  strings.TrimSpace(opts.Name),
		"slug":                  strings.TrimSpace(opts.Slug),
		"source_url":            strings.TrimSpace(opts.SourceURL),
		"source_page_url":       strings.TrimSpace(opts.SourcePageURL),
		"tags":                  normalizeTags(splitCSV(opts.TagsCSV)),
		"recording_state":       "off",
		"capture_type":          captureTypeValue,
		"source_family":         sourceFamilyValue,
		"execution_class":       executionClassValue,
		"execution_config_json": cfgJSON,
		"location_country":      strings.TrimSpace(opts.LocationCountry),
		"location_country_code": strings.ToUpper(strings.TrimSpace(opts.LocationCountryCode)),
		"location_region":       strings.TrimSpace(opts.LocationRegion),
		"location_city":         strings.TrimSpace(opts.LocationCity),
		"location_locality":     strings.TrimSpace(opts.LocationLocality),
		"location_source":       strings.TrimSpace(opts.LocationSource),
	})
	return payload, captureTypeValue, executionClassValue
}

func printStreamsUsage() {
	fmt.Print("stoaramactl streams <list|detail|filters|frames|clips|clip-latest|image-urls|add|update|tags-add|tags-remove|cleanup-location-tags|metadata-audit|set-capture|repair-youtube|repair-image-capture|repair-canonical-capture> ...\n")
}

func printDiscoveryUsage() {
	fmt.Print("stoaramactl discovery candidates <list|review|import> ...\n")
}

func isGlobalPlaylistTag(line string) bool {
	switch {
	case line == "#EXTM3U":
		return true
	case strings.HasPrefix(line, "#EXT-X-VERSION:"):
		return true
	case strings.HasPrefix(line, "#EXT-X-INDEPENDENT-SEGMENTS"):
		return true
	case strings.HasPrefix(line, "#EXT-X-TARGETDURATION:"):
		return true
	case strings.HasPrefix(line, "#EXT-X-PLAYLIST-TYPE:"):
		return true
	case strings.HasPrefix(line, "#EXT-X-SERVER-CONTROL:"):
		return true
	case strings.HasPrefix(line, "#EXT-X-PART-INF:"):
		return true
	case strings.HasPrefix(line, "#EXT-X-START:"):
		return true
	case strings.HasPrefix(line, "#EXT-X-DISCONTINUITY-SEQUENCE:"):
		return true
	case strings.HasPrefix(line, "#EXT-X-MAP:"):
		return true
	default:
		return false
	}
}

func runStreams(ctx context.Context, cfg config.Config, args []string) {
	if len(args) >= 1 && (args[0] == "-h" || args[0] == "--help") {
		printStreamsUsage()
		return
	}
	if len(args) < 1 {
		printStreamsUsage()
		return
	}
	sub := args[0]
	switch sub {
	case "list":
		fs := flag.NewFlagSet("streams list", flag.ExitOnError)
		backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
		apiToken := fs.String("api-token", cfg.APIToken, "backend API token")
		recordingState := fs.String("recording-state", "", "filter recording state off|on")
		captureType := fs.String("capture-type", "", "filter capture type youtube_watch|hls|dash|rtsp|rtmp|http_video|still_image|webrtc|unknown")
		tags := fs.String("tags", "", "comma-separated tags (match any)")
		search := fs.String("search", "", "optional search text")
		source := fs.String("source", "", "optional source filter")
		youtubeChannel := fs.String("youtube-channel", "", "optional youtube channel filter")
		city := fs.String("city", "", "optional city filter")
		limit := fs.Int("limit", 200, "row limit")
		offset := fs.Int("offset", 0, "row offset")
		sortBy := fs.String("sort-by", "", "sort key")
		sortDir := fs.String("sort-dir", "", "sort direction asc|desc")
		asJSON := fs.Bool("json", false, "print JSON")
		_ = fs.Parse(args[1:])
		if *limit <= 0 || *limit > 2000 {
			log.Fatalf("--limit must be between 1 and 2000")
		}
		if *offset < 0 {
			log.Fatalf("--offset must be >= 0")
		}
		q := url.Values{}
		q.Set("limit", strconv.Itoa(*limit))
		q.Set("offset", strconv.Itoa(*offset))
		q.Set("include_image_urls", "false")
		if strings.TrimSpace(*recordingState) != "" {
			state := strings.ToLower(strings.TrimSpace(*recordingState))
			switch state {
			case "off", "on":
			default:
				log.Fatalf("invalid --recording-state: expected off|on")
			}
			q.Set("recording_state", state)
		}
		if strings.TrimSpace(*captureType) != "" {
			captureTypeValue, ok := capture.NormalizeCaptureType(strings.TrimSpace(*captureType))
			if !ok {
				log.Fatalf("invalid --capture-type")
			}
			q.Set("capture_type", captureTypeValue)
		}
		tagList := normalizeTags(splitCSV(*tags))
		if len(tagList) > 0 {
			q.Set("tags", strings.Join(tagList, ","))
		}
		if v := strings.TrimSpace(*search); v != "" {
			q.Set("search", v)
		}
		if v := strings.TrimSpace(*source); v != "" {
			q.Set("source", v)
		}
		if v := strings.TrimSpace(*youtubeChannel); v != "" {
			q.Set("youtube_channel", v)
		}
		if v := strings.TrimSpace(*city); v != "" {
			q.Set("city", v)
		}
		if v := strings.TrimSpace(*sortBy); v != "" {
			q.Set("sort_by", v)
		}
		if v := strings.TrimSpace(*sortDir); v != "" {
			q.Set("sort_dir", v)
		}
		path := "/api/v1/dashboard/streams?" + q.Encode()
		payload := mustAPIGet(ctx, strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken), path)
		if *asJSON {
			printJSON(payload)
			return
		}
		items, _ := payload["items"].([]any)
		for _, raw := range items {
			item := asMap(raw)
			stream := asMap(item["stream"])
			tags := asStringSlice(stream["tags"])
			fmt.Printf("id=%d provider=%s external_id=%s name=%q slug=%s recording_state=%s capture_type=%s execution_class=%s country=%q city=%q tags=%s\n",
				int64FromAny(stream["id"]),
				fmt.Sprint(stream["provider"]),
				fmt.Sprint(stream["external_id"]),
				fmt.Sprint(stream["name"]),
				fmt.Sprint(stream["slug"]),
				fmt.Sprint(stream["recording_state"]),
				fmt.Sprint(stream["capture_type"]),
				fmt.Sprint(stream["execution_class"]),
				fmt.Sprint(stream["location_country"]),
				fmt.Sprint(stream["location_city"]),
				strings.Join(tags, ","),
			)
		}
	case "detail":
		fs := flag.NewFlagSet("streams detail", flag.ExitOnError)
		backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
		apiToken := fs.String("api-token", cfg.APIToken, "backend API token")
		id := fs.Int64("id", 0, "stream id")
		slug := fs.String("slug", "", "stream slug")
		pipelineID := fs.String("pipeline-id", "", "optional pipeline filter")
		resultsLimit := fs.Int("results-limit", 10, "latest inference results to print")
		detectionsLimit := fs.Int("detections-limit", 50, "detections to print for selected/latest result")
		asJSON := fs.Bool("json", false, "print JSON")
		_ = fs.Parse(args[1:])
		if *id <= 0 && strings.TrimSpace(*slug) == "" {
			log.Fatalf("provide --id or --slug")
		}
		if *resultsLimit <= 0 {
			log.Fatalf("--results-limit must be > 0")
		}
		if *detectionsLimit <= 0 {
			log.Fatalf("--detections-limit must be > 0")
		}
		streamID := *id
		if streamID <= 0 {
			streamID = mustResolveStreamIDBySlug(ctx, strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken), strings.TrimSpace(*slug))
		}
		detailParams := url.Values{}
		detailParams.Set("limit", strconv.Itoa(*resultsLimit))
		detailParams.Set("offset", "0")
		if p := strings.TrimSpace(*pipelineID); p != "" {
			detailParams.Set("pipeline_id", p)
		}
		detail := mustAPIGet(ctx, strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken), fmt.Sprintf("/api/v1/dashboard/streams/%d?%s", streamID, detailParams.Encode()))
		detectParams := url.Values{}
		detectParams.Set("limit", strconv.Itoa(*detectionsLimit))
		if p := strings.TrimSpace(*pipelineID); p != "" {
			detectParams.Set("pipeline_id", p)
		}
		detect := mustAPIGet(ctx, strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken), fmt.Sprintf("/api/v1/dashboard/streams/%d/detections?%s", streamID, detectParams.Encode()))
		if *asJSON {
			printJSON(map[string]any{
				"detail":     detail,
				"detections": detect,
			})
			return
		}
		stream := asMap(detail["stream"])
		latestFrame := asMap(detail["latest_frame"])
		fmt.Printf("stream id=%d slug=%s provider=%s external_id=%s\n", int64FromAny(stream["id"]), fmt.Sprint(stream["slug"]), fmt.Sprint(stream["provider"]), fmt.Sprint(stream["external_id"]))
		fmt.Printf("name=%q\n", fmt.Sprint(stream["name"]))
		fmt.Printf("recording_state=%s capture_type=%s execution_class=%s\n", fmt.Sprint(stream["recording_state"]), fmt.Sprint(stream["capture_type"]), fmt.Sprint(stream["execution_class"]))
		fmt.Printf("source_url=%s\n", fmt.Sprint(stream["source_url"]))
		fmt.Printf("source_page_url=%s\n", fmt.Sprint(stream["source_page_url"]))
		if len(latestFrame) > 0 {
			fmt.Printf("latest_raw_frame captured_at=%s object_key=%s\n", fmt.Sprint(latestFrame["captured_at"]), fmt.Sprint(latestFrame["object_key"]))
		} else {
			fmt.Println("latest_raw_frame captured_at=- object_key=-")
		}
		inference, _ := detail["inference"].([]any)
		fmt.Printf("\nlatest inference results (limit=%d", *resultsLimit)
		if p := strings.TrimSpace(*pipelineID); p != "" {
			fmt.Printf(", pipeline=%s", p)
		}
		fmt.Println("):")
		if len(inference) == 0 {
			fmt.Println("  - none")
		} else {
			for _, raw := range inference {
				it := asMap(raw)
				summary := oneLineJSON(it["summary"])
				if len(summary) > 180 {
					summary = summary[:180] + "..."
				}
				fmt.Printf("  - id=%v pipeline=%v rev=%v status=%v created_at=%v finished_at=%v boxed=%v raw=%v error=%v summary=%s\n",
					it["inference_result_id"], it["pipeline_id"], it["revision"], it["status"], it["created_at"], it["finished_at"], it["boxed_object_key"], it["raw_object_key"], it["error_text"], summary)
			}
		}
		latestResult := asMap(detect["latest_result"])
		detections, _ := detect["detections"].([]any)
		if len(latestResult) == 0 {
			fmt.Println("\nlatest detections: none (no inference results)")
			return
		}
		fmt.Printf("\nlatest detections for inference_result_id=%v (limit=%d):\n", latestResult["inference_result_id"], *detectionsLimit)
		if len(detections) == 0 {
			fmt.Println("  - none")
			return
		}
		for _, raw := range detections {
			d := asMap(raw)
			conf, _ := asFloat64(d["confidence"])
			x1, _ := asFloat64(d["x1"])
			y1, _ := asFloat64(d["y1"])
			x2, _ := asFloat64(d["x2"])
			y2, _ := asFloat64(d["y2"])
			area, _ := asFloat64(d["area_px"])
			fmt.Printf("  - class=%s conf=%.4f bbox=[%.1f,%.1f,%.1f,%.1f] area=%.1f\n",
				fmt.Sprint(d["class_name"]), conf, x1, y1, x2, y2, area)
		}
	case "filters":
		fs := flag.NewFlagSet("streams filters", flag.ExitOnError)
		backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
		apiToken := fs.String("api-token", cfg.APIToken, "backend API token")
		kind := fs.String("kind", "tags", "filter kind tags|countries|cities|sources|youtube-channels")
		scope := fs.String("scope", "", "tag scope for --kind tags (all|recording)")
		query := fs.String("q", "", "optional search query for --kind tags")
		limit := fs.Int("limit", 200, "tag limit for --kind tags")
		recordingState := fs.String("recording-state", "", "optional recording state filter off|on")
		country := fs.String("country", "", "optional country filter")
		city := fs.String("city", "", "optional city filter")
		source := fs.String("source", "", "optional source filter")
		youtubeChannel := fs.String("youtube-channel", "", "optional youtube channel filter")
		captureType := fs.String("capture-type", "", "optional capture type filter")
		tags := fs.String("tags", "", "optional comma-separated tags include-any filter")
		tagsNot := fs.String("tags-not", "", "optional comma-separated tags exclude-any filter")
		asJSON := fs.Bool("json", false, "print JSON")
		_ = fs.Parse(args[1:])

		q := url.Values{}
		if v := strings.TrimSpace(*recordingState); v != "" {
			state := strings.ToLower(v)
			switch state {
			case "off", "on":
			default:
				log.Fatalf("invalid --recording-state: expected off|on")
			}
			q.Set("recording_state", state)
		}
		if v := strings.TrimSpace(*country); v != "" {
			q.Set("country", v)
		}
		if v := strings.TrimSpace(*city); v != "" {
			q.Set("city", v)
		}
		if v := strings.TrimSpace(*source); v != "" {
			q.Set("source", strings.ToLower(v))
		}
		if v := strings.TrimSpace(*youtubeChannel); v != "" {
			q.Set("youtube_channel", v)
		}
		if v := strings.TrimSpace(*captureType); v != "" {
			captureTypeValue, ok := capture.NormalizeCaptureType(v)
			if !ok {
				log.Fatalf("invalid --capture-type")
			}
			q.Set("capture_type", captureTypeValue)
		}
		if includeTags := normalizeTags(splitCSV(*tags)); len(includeTags) > 0 {
			q.Set("tags", strings.Join(includeTags, ","))
		}
		if excludeTags := normalizeTags(splitCSV(*tagsNot)); len(excludeTags) > 0 {
			q.Set("tags_not", strings.Join(excludeTags, ","))
		}

		path := ""
		switch strings.ToLower(strings.TrimSpace(*kind)) {
		case "tags":
			path = "/api/v1/dashboard/tags"
			if v := strings.ToLower(strings.TrimSpace(*scope)); v != "" {
				if v != "all" && v != "recording" {
					log.Fatalf("invalid --scope for --kind tags: expected all|recording")
				}
				q.Set("scope", v)
			}
			if v := strings.TrimSpace(*query); v != "" {
				q.Set("q", v)
			}
			if *limit <= 0 || *limit > 1000 {
				log.Fatalf("--limit must be between 1 and 1000")
			}
			q.Set("limit", strconv.Itoa(*limit))
		case "countries":
			path = "/api/v1/dashboard/countries"
		case "cities":
			path = "/api/v1/dashboard/cities"
		case "sources":
			path = "/api/v1/dashboard/sources"
		case "youtube-channels":
			path = "/api/v1/dashboard/youtube-channels"
		default:
			log.Fatalf("invalid --kind: expected tags|countries|cities|sources|youtube-channels")
		}
		if encoded := q.Encode(); encoded != "" {
			path += "?" + encoded
		}
		payload := mustAPIGet(ctx, strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken), path)
		if *asJSON {
			printJSON(payload)
			return
		}
		items, _ := payload["items"].([]any)
		for _, raw := range items {
			fmt.Println(strings.TrimSpace(fmt.Sprint(raw)))
		}
	case "frames":
		fs := flag.NewFlagSet("streams frames", flag.ExitOnError)
		backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
		apiToken := fs.String("api-token", cfg.APIToken, "backend API token")
		streamID := fs.Int64("stream-id", 0, "stream id filter")
		pipelineID := fs.String("pipeline-id", "", "pipeline id")
		uninferenced := fs.Bool("uninferenced", false, "only frames uninferenced for --pipeline-id")
		unprocessed := fs.Bool("unprocessed", false, "only frames with no inference rows for --pipeline-id")
		limit := fs.Int("limit", 200, "row limit")
		offset := fs.Int("offset", 0, "row offset")
		sortBy := fs.String("sort-by", "captured_at", "sort field captured_at|id|stream_id|status|error|source_kind|object_key|size_bytes|width|height")
		sortDir := fs.String("sort-dir", "desc", "sort direction asc|desc")
		asJSON := fs.Bool("json", false, "print JSON")
		_ = fs.Parse(args[1:])
		if *limit <= 0 || *limit > 5000 {
			log.Fatalf("--limit must be between 1 and 5000")
		}
		if *offset < 0 {
			log.Fatalf("--offset must be >= 0")
		}
		if *uninferenced || *unprocessed {
			if strings.TrimSpace(*pipelineID) == "" {
				log.Fatalf("--pipeline-id is required when --uninferenced or --unprocessed is set")
			}
		}
		dir := strings.ToLower(strings.TrimSpace(*sortDir))
		if dir != "asc" && dir != "desc" {
			log.Fatalf("--sort-dir must be asc or desc")
		}
		q := url.Values{}
		q.Set("limit", strconv.Itoa(*limit))
		q.Set("offset", strconv.Itoa(*offset))
		q.Set("sort_by", strings.TrimSpace(*sortBy))
		q.Set("sort_dir", dir)
		if *streamID > 0 {
			q.Set("stream_id", strconv.FormatInt(*streamID, 10))
		}
		if v := strings.TrimSpace(*pipelineID); v != "" {
			q.Set("pipeline_id", v)
		}
		if *uninferenced {
			q.Set("uninferenced", "true")
		}
		if *unprocessed {
			q.Set("unprocessed", "true")
		}
		payload := mustAPIGet(ctx, strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken), "/api/v1/frames?"+q.Encode())
		if *asJSON {
			printJSON(payload)
			return
		}
		items, _ := payload["items"].([]any)
		fmt.Printf("frames=%d limit=%d offset=%d\n", len(items), *limit, *offset)
		for _, raw := range items {
			it := asMap(raw)
			fmt.Printf("frame_id=%v stream_id=%v captured_at=%v status=%v error=%v source=%v object_key=%v\n",
				it["id"], it["stream_id"], it["captured_at"], it["capture_status"], it["capture_error"], it["source_kind"], it["object_key"])
		}
	case "clips":
		fs := flag.NewFlagSet("streams clips", flag.ExitOnError)
		backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
		apiToken := fs.String("api-token", cfg.APIToken, "backend API token")
		streamID := fs.Int64("stream-id", 0, "stream id")
		limit := fs.Int("limit", 100, "row limit")
		offset := fs.Int("offset", 0, "row offset")
		asJSON := fs.Bool("json", false, "print JSON")
		_ = fs.Parse(args[1:])
		if *streamID <= 0 {
			log.Fatalf("--stream-id is required")
		}
		if *limit <= 0 || *limit > 1000 {
			log.Fatalf("--limit must be between 1 and 1000")
		}
		if *offset < 0 {
			log.Fatalf("--offset must be >= 0")
		}
		q := url.Values{}
		q.Set("limit", strconv.Itoa(*limit))
		q.Set("offset", strconv.Itoa(*offset))
		payload := mustAPIGet(ctx, strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken), fmt.Sprintf("/api/v1/capture/streams/%d/segments?%s", *streamID, q.Encode()))
		if *asJSON {
			printJSON(payload)
			return
		}
		items, _ := payload["items"].([]any)
		fmt.Printf("clips=%d limit=%d offset=%d\n", len(items), *limit, *offset)
		for _, raw := range items {
			it := asMap(raw)
			fmt.Printf("segment_id=%v stream_id=%v start=%v end=%v status=%v fps=%v object_key=%v thumbnail=%v error=%v\n",
				it["id"], it["stream_id"], it["segment_start_at"], it["segment_end_at"], it["capture_status"], it["target_fps"], it["object_key"], it["thumbnail_object_key"], it["capture_error"])
		}
	case "clip-latest":
		fs := flag.NewFlagSet("streams clip-latest", flag.ExitOnError)
		backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
		apiToken := fs.String("api-token", cfg.APIToken, "backend API token")
		streamID := fs.Int64("stream-id", 0, "stream id")
		asJSON := fs.Bool("json", false, "print JSON")
		_ = fs.Parse(args[1:])
		if *streamID <= 0 {
			log.Fatalf("--stream-id is required")
		}
		payload := mustAPIGet(ctx, strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken), fmt.Sprintf("/api/v1/capture/streams/%d/segments/latest", *streamID))
		if *asJSON {
			printJSON(payload)
			return
		}
		item := asMap(payload["item"])
		if len(item) == 0 {
			fmt.Println("latest_clip=none")
			return
		}
		fmt.Printf("segment_id=%v stream_id=%v start=%v end=%v status=%v fps=%v object_key=%v thumbnail=%v download_url=%v thumbnail_download_url=%v\n",
			item["id"], item["stream_id"], item["segment_start_at"], item["segment_end_at"], item["capture_status"], item["target_fps"], item["object_key"], item["thumbnail_object_key"], item["download_url"], item["thumbnail_download_url"])
	case "image-urls":
		fs := flag.NewFlagSet("streams image-urls", flag.ExitOnError)
		backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
		apiToken := fs.String("api-token", cfg.APIToken, "backend API token")
		streamIDsRaw := fs.String("stream-ids", "", "comma-separated stream IDs")
		asJSON := fs.Bool("json", false, "print JSON")
		_ = fs.Parse(args[1:])
		ids, err := parseInt64CSV(*streamIDsRaw)
		if err != nil {
			log.Fatalf("parse --stream-ids: %v", err)
		}
		if len(ids) == 0 {
			log.Fatalf("--stream-ids must contain at least one stream id")
		}
		payloadIDs := make([]int64, 0, len(ids))
		for _, id := range ids {
			if id <= 0 {
				log.Fatalf("invalid stream id in --stream-ids: %d", id)
			}
			payloadIDs = append(payloadIDs, id)
		}
		payload := mustAPIRequest(ctx, http.MethodPost, strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken), "/api/v1/dashboard/streams/image-urls", map[string]any{
			"stream_ids": payloadIDs,
		})
		if *asJSON {
			printJSON(payload)
			return
		}
		items, _ := payload["items"].([]any)
		for _, raw := range items {
			it := asMap(raw)
			fmt.Printf("stream_id=%v image_url=%v\n", it["stream_id"], it["latest_frame_url"])
		}
	case "pipelines-list":
		log.Fatalf("streams pipelines-list is removed; use `stoaramactl pipelines stream-list --id N`")
	case "pipelines-set":
		log.Fatalf("streams pipelines-set is removed; use `stoaramactl pipelines set --stream-id N --pipeline-id P --enabled=true|false`")
	case "add":
		fs := flag.NewFlagSet("streams add", flag.ExitOnError)
		backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
		apiToken := fs.String("api-token", cfg.APIToken, "backend API token")
		provider := fs.String("provider", "MANUAL", "provider")
		externalID := fs.String("external-id", "", "external id (defaults to deterministic manual id)")
		name := fs.String("name", "", "name")
		slug := fs.String("slug", "", "slug (defaults from name)")
		sourceURL := fs.String("source-url", "", "source url")
		sourcePageURL := fs.String("source-page-url", "", "source page url")
		tags := fs.String("tags", "", "comma-separated tags")
		captureType := fs.String("capture-type", "", "capture type override")
		sourceFamily := fs.String("source-family", "", "source family override")
		executionClass := fs.String("execution-class", "", "execution class override")
		executionConfigJSON := fs.String("execution-config-json", "{}", "execution config json")
		locationCountry := fs.String("location-country", "", "hierarchy country")
		locationCountryCode := fs.String("location-country-code", "", "hierarchy ISO country code")
		locationRegion := fs.String("location-region", "", "hierarchy region/state")
		locationCity := fs.String("location-city", "", "hierarchy city")
		locationLocality := fs.String("location-locality", "", "hierarchy locality/district")
		locationSource := fs.String("location-source", "manual", "hierarchy source label")
		asJSON := fs.Bool("json", false, "print JSON")
		_ = fs.Parse(args[1:])
		if strings.TrimSpace(*name) == "" || strings.TrimSpace(*sourceURL) == "" {
			log.Fatalf("--name and --source-url are required")
		}
		externalIDValue := strings.TrimSpace(*externalID)
		if externalIDValue == "" {
			externalIDValue = defaultManualExternalID(*sourceURL)
		}
		slugValue := strings.TrimSpace(*slug)
		if slugValue == "" {
			slugValue = slugifyForCLI(strings.TrimSpace(*name))
		}
		payload, captureTypeValue, executionClassValue := createStreamFromCLI(ctx, streamCreateCLIOptions{
			BackendAPIURL:       *backendAPIURL,
			APIToken:            *apiToken,
			Provider:            strings.ToUpper(strings.TrimSpace(*provider)),
			ExternalID:          externalIDValue,
			Name:                *name,
			Slug:                slugValue,
			SourceURL:           *sourceURL,
			SourcePageURL:       *sourcePageURL,
			TagsCSV:             *tags,
			CaptureType:         *captureType,
			SourceFamily:        *sourceFamily,
			ExecutionClass:      *executionClass,
			ExecutionConfigJSON: *executionConfigJSON,
			LocationCountry:     *locationCountry,
			LocationCountryCode: *locationCountryCode,
			LocationRegion:      *locationRegion,
			LocationCity:        *locationCity,
			LocationLocality:    *locationLocality,
			LocationSource:      *locationSource,
		})
		if *asJSON {
			printJSON(payload)
			return
		}
		fmt.Printf("stream added id=%d slug=%s capture_type=%s execution_class=%s\n", int64FromAny(payload["id"]), fmt.Sprint(payload["slug"]), captureTypeValue, executionClassValue)
	case "update":
		fs := flag.NewFlagSet("streams update", flag.ExitOnError)
		backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
		apiToken := fs.String("api-token", cfg.APIToken, "backend API token")
		id := fs.Int64("id", 0, "stream id")
		name := optionalStringFlag(fs, "name", "")
		slug := optionalStringFlag(fs, "slug", "")
		sourceURL := optionalStringFlag(fs, "source-url", "")
		sourcePageURL := optionalStringFlag(fs, "source-page-url", "")
		sourceChangeReason := optionalStringFlag(fs, "source-change-reason", "")
		recordingState := optionalStringFlag(fs, "recording-state", "")
		interval := optionalIntFlag(fs, "capture-interval", 0)
		priority := optionalIntFlag(fs, "priority", 0)
		tags := optionalStringFlag(fs, "tags", "")
		captureType := optionalStringFlag(fs, "capture-type", "")
		sourceFamily := optionalStringFlag(fs, "source-family", "")
		executionClass := optionalStringFlag(fs, "execution-class", "")
		executionConfigJSON := optionalStringFlag(fs, "execution-config-json", "")
		locationCountry := optionalStringFlag(fs, "location-country", "")
		locationCountryCode := optionalStringFlag(fs, "location-country-code", "")
		locationRegion := optionalStringFlag(fs, "location-region", "")
		locationCity := optionalStringFlag(fs, "location-city", "")
		locationLocality := optionalStringFlag(fs, "location-locality", "")
		locationSource := optionalStringFlag(fs, "location-source", "")
		excluded := optionalBoolFlag(fs, "excluded")
		asJSON := fs.Bool("json", false, "print JSON")
		_ = fs.Parse(args[1:])
		if *id <= 0 {
			log.Fatalf("--id is required")
		}
		payload := map[string]any{}
		if name.set {
			payload["name"] = strings.TrimSpace(name.value)
		}
		if slug.set {
			payload["slug"] = strings.TrimSpace(slug.value)
		}
		if sourceURL.set {
			payload["source_url"] = strings.TrimSpace(sourceURL.value)
		}
		if sourcePageURL.set {
			payload["source_page_url"] = strings.TrimSpace(sourcePageURL.value)
		}
		if sourceChangeReason.set {
			payload["source_change_reason"] = strings.TrimSpace(sourceChangeReason.value)
		}
		if recordingState.set {
			state := strings.ToLower(strings.TrimSpace(recordingState.value))
			if state != string(model.RecordingStateOff) && state != string(model.RecordingStateOn) {
				log.Fatalf("--recording-state must be off|on")
			}
			payload["recording_state"] = state
		}
		if interval.set {
			log.Fatalf("--capture-interval is removed; set execution_config_json.poll_interval_sec instead")
		}
		if priority.set {
			log.Fatalf("--priority is removed")
		}
		if tags.set {
			payload["tags"] = normalizeTags(splitCSV(tags.value))
		}
		if captureType.set || sourceFamily.set || executionClass.set {
			captureTypeValue, sourceFamilyValue, executionClassValue := deriveCanonicalStreamFields(
				func() string {
					if sourceURL.set {
						return sourceURL.value
					}
					return ""
				}(),
				func() string {
					if sourcePageURL.set {
						return sourcePageURL.value
					}
					return ""
				}(),
				captureType.value,
				sourceFamily.value,
				executionClass.value,
			)
			payload["capture_type"] = captureTypeValue
			payload["source_family"] = sourceFamilyValue
			payload["execution_class"] = executionClassValue
		}
		if executionConfigJSON.set {
			payload["execution_config_json"] = decodeExecutionConfigJSON(executionConfigJSON.value, 0)
		}
		if locationCountry.set {
			payload["location_country"] = strings.TrimSpace(locationCountry.value)
		}
		if locationCountryCode.set {
			payload["location_country_code"] = strings.ToUpper(strings.TrimSpace(locationCountryCode.value))
		}
		if locationRegion.set {
			payload["location_region"] = strings.TrimSpace(locationRegion.value)
		}
		if locationCity.set {
			payload["location_city"] = strings.TrimSpace(locationCity.value)
		}
		if locationLocality.set {
			payload["location_locality"] = strings.TrimSpace(locationLocality.value)
		}
		if locationSource.set {
			payload["location_source"] = strings.TrimSpace(locationSource.value)
		}
		if excluded.set {
			log.Fatalf("--excluded is removed")
		}
		if len(payload) == 0 {
			log.Fatalf("no fields set; provide at least one update flag")
		}
		out := mustAPIRequest(ctx, http.MethodPatch, strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken), fmt.Sprintf("/api/v1/streams/%d", *id), payload)
		if *asJSON {
			printJSON(out)
			return
		}
		fmt.Printf("stream updated id=%d slug=%s\n", int64FromAny(out["id"]), fmt.Sprint(out["slug"]))
	case "soft-delete":
		fs := flag.NewFlagSet("streams soft-delete", flag.ExitOnError)
		backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
		apiToken := fs.String("api-token", cfg.APIToken, "backend API token")
		id := fs.Int64("id", 0, "stream id")
		reason := fs.String("reason", "", "required delete reason")
		dryRun := fs.Bool("dry-run", false, "validate inputs and show target without deleting")
		asJSON := fs.Bool("json", false, "print JSON")
		_ = fs.Parse(args[1:])
		if *id <= 0 {
			log.Fatalf("--id is required")
		}
		reasonValue := strings.TrimSpace(*reason)
		if reasonValue == "" {
			log.Fatalf("--reason is required")
		}
		detail := mustAPIGet(ctx, strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken), fmt.Sprintf("/api/v1/dashboard/streams/%d?limit=1", *id))
		if *dryRun {
			stream := asMap(detail["stream"])
			out := map[string]any{"dry_run": true, "stream": stream, "reason": reasonValue}
			if *asJSON {
				printJSON(out)
				return
			}
			fmt.Printf("would soft-delete stream id=%d slug=%s name=%q reason=%q\n", *id, fmt.Sprint(stream["slug"]), fmt.Sprint(stream["name"]), reasonValue)
			return
		}
		out := mustAPIRequest(ctx, http.MethodPost, strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken), fmt.Sprintf("/api/v1/admin/streams/%d/delete", *id), map[string]any{
			"reason": reasonValue,
		})
		if *asJSON {
			printJSON(out)
			return
		}
		fmt.Printf("stream soft-deleted id=%d\n", *id)
	case "tags-add":
		fs := flag.NewFlagSet("streams tags-add", flag.ExitOnError)
		backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
		apiToken := fs.String("api-token", cfg.APIToken, "backend API token")
		id := fs.Int64("id", 0, "stream id")
		slug := fs.String("slug", "", "stream slug")
		tags := fs.String("tags", "", "comma-separated tags to add")
		asJSON := fs.Bool("json", false, "print JSON")
		_ = fs.Parse(args[1:])
		if *id <= 0 && strings.TrimSpace(*slug) == "" {
			log.Fatalf("provide --id or --slug")
		}
		tagList := normalizeTags(splitCSV(*tags))
		if len(tagList) == 0 {
			log.Fatalf("--tags must contain at least one tag")
		}
		streamID := *id
		if streamID <= 0 {
			streamID = mustResolveStreamIDBySlug(ctx, strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken), *slug)
		}
		detail := mustAPIGet(ctx, strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken), fmt.Sprintf("/api/v1/dashboard/streams/%d?limit=1", streamID))
		current := asStringSlice(asMap(detail["stream"])["tags"])
		updatedTags := normalizeTags(append(current, tagList...))
		out := mustAPIRequest(ctx, http.MethodPatch, strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken), fmt.Sprintf("/api/v1/streams/%d", streamID), map[string]any{
			"tags": updatedTags,
		})
		if *asJSON {
			printJSON(out)
			return
		}
		fmt.Printf("stream %d tags=%s\n", streamID, strings.Join(updatedTags, ","))
	case "tags-remove":
		fs := flag.NewFlagSet("streams tags-remove", flag.ExitOnError)
		backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
		apiToken := fs.String("api-token", cfg.APIToken, "backend API token")
		id := fs.Int64("id", 0, "stream id")
		slug := fs.String("slug", "", "stream slug")
		tags := fs.String("tags", "", "comma-separated tags to remove")
		asJSON := fs.Bool("json", false, "print JSON")
		_ = fs.Parse(args[1:])
		if *id <= 0 && strings.TrimSpace(*slug) == "" {
			log.Fatalf("provide --id or --slug")
		}
		tagList := normalizeTags(splitCSV(*tags))
		if len(tagList) == 0 {
			log.Fatalf("--tags must contain at least one tag")
		}
		streamID := *id
		if streamID <= 0 {
			streamID = mustResolveStreamIDBySlug(ctx, strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken), *slug)
		}
		detail := mustAPIGet(ctx, strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken), fmt.Sprintf("/api/v1/dashboard/streams/%d?limit=1", streamID))
		current := asStringSlice(asMap(detail["stream"])["tags"])
		removeSet := map[string]struct{}{}
		for _, t := range tagList {
			removeSet[t] = struct{}{}
		}
		updatedTags := make([]string, 0, len(current))
		for _, t := range current {
			if _, drop := removeSet[t]; drop {
				continue
			}
			updatedTags = append(updatedTags, t)
		}
		updatedTags = normalizeTags(updatedTags)
		out := mustAPIRequest(ctx, http.MethodPatch, strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken), fmt.Sprintf("/api/v1/streams/%d", streamID), map[string]any{
			"tags": updatedTags,
		})
		if *asJSON {
			printJSON(out)
			return
		}
		fmt.Printf("stream %d tags=%s\n", streamID, strings.Join(updatedTags, ","))
	case "cleanup-location-tags":
		runStreamsCleanupLocationTags(ctx, cfg, args[1:])
	case "metadata-audit":
		runStreamMetadataAudit(ctx, cfg, args[1:])
	case "recording-interval":
		log.Fatalf("streams recording-interval is removed; sampled clip recording uses service-wide clip duration every 4-8 minutes")
	case "set-capture":
		fs := flag.NewFlagSet("streams set-capture", flag.ExitOnError)
		backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
		apiToken := fs.String("api-token", cfg.APIToken, "backend API token")
		id := fs.Int64("id", 0, "stream id")
		captureType := fs.String("capture-type", "", "capture type")
		sourceFamily := fs.String("source-family", "", "source family")
		executionClass := fs.String("execution-class", "", "execution class")
		configJSON := fs.String("config-json", "{}", "capture config json")
		asJSON := fs.Bool("json", false, "print JSON")
		_ = fs.Parse(args[1:])
		if *id <= 0 {
			log.Fatalf("--id is required")
		}
		captureTypeValue, sourceFamilyValue, executionClassValue := deriveCanonicalStreamFields("", "", *captureType, *sourceFamily, *executionClass)
		var cfgJSON map[string]any
		if err := json.Unmarshal([]byte(strings.TrimSpace(*configJSON)), &cfgJSON); err != nil {
			log.Fatalf("invalid --config-json: %v", err)
		}
		payload := mustAPIRequest(ctx, http.MethodPatch, strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken), fmt.Sprintf("/api/v1/streams/%d/capture", *id), map[string]any{
			"capture_type":          captureTypeValue,
			"execution_class":       executionClassValue,
			"source_family":         sourceFamilyValue,
			"execution_config_json": cfgJSON,
		})
		if *asJSON {
			printJSON(payload)
			return
		}
		fmt.Printf("stream %d capture_type=%s execution_class=%s updated\n", *id, captureTypeValue, executionClassValue)
	case "repair-youtube":
		runStreamsRepairYouTube(ctx, cfg, args[1:])
	case "repair-image-capture":
		runStreamsRepairImageCapture(ctx, cfg, args[1:])
	case "repair-canonical-capture":
		runStreamsRepairCanonicalCapture(ctx, cfg, args[1:])
	default:
		log.Fatalf("unknown streams subcommand: %s", sub)
	}
}

func runDiscovery(ctx context.Context, cfg config.Config, args []string) {
	if len(args) >= 1 && (args[0] == "-h" || args[0] == "--help") {
		printDiscoveryUsage()
		return
	}
	if len(args) < 2 || args[0] != "candidates" {
		printDiscoveryUsage()
		return
	}
	switch args[1] {
	case "list":
		fs := flag.NewFlagSet("discovery candidates list", flag.ExitOnError)
		backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
		apiToken := fs.String("api-token", cfg.APIToken, "backend API token")
		id := fs.Int64("id", 0, "optional candidate id filter")
		reviewStatus := fs.String("review-status", "", "optional review status pending|accepted|rejected|invalid")
		provider := fs.String("provider", "", "optional provider filter")
		captureType := fs.String("capture-type", "", "optional capture type filter")
		limit := fs.Int("limit", 200, "row limit")
		offset := fs.Int("offset", 0, "row offset")
		asJSON := fs.Bool("json", false, "print JSON")
		_ = fs.Parse(args[2:])
		q := url.Values{}
		if *id > 0 {
			q.Set("id", strconv.FormatInt(*id, 10))
		}
		if strings.TrimSpace(*reviewStatus) != "" {
			q.Set("review_status", strings.TrimSpace(*reviewStatus))
		}
		if strings.TrimSpace(*provider) != "" {
			q.Set("provider", strings.TrimSpace(*provider))
		}
		if strings.TrimSpace(*captureType) != "" {
			q.Set("capture_type", strings.TrimSpace(*captureType))
		}
		q.Set("limit", strconv.Itoa(*limit))
		q.Set("offset", strconv.Itoa(*offset))
		payload := mustAPIGet(ctx, strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken), "/api/v1/source-candidates?"+q.Encode())
		if *asJSON {
			printJSON(payload)
			return
		}
		items, _ := payload["items"].([]any)
		for _, raw := range items {
			it := asMap(raw)
			fmt.Printf(
				"id=%d provider=%s external_id=%s review_status=%s capture_type=%s source_family=%s slug=%s source_url=%s\n",
				int64FromAny(it["id"]),
				fmt.Sprint(it["provider"]),
				fmt.Sprint(it["external_id"]),
				fmt.Sprint(it["review_status"]),
				fmt.Sprint(it["capture_type"]),
				fmt.Sprint(it["source_family"]),
				fmt.Sprint(it["slug"]),
				fmt.Sprint(it["source_url"]),
			)
		}
	case "review":
		fs := flag.NewFlagSet("discovery candidates review", flag.ExitOnError)
		backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
		apiToken := fs.String("api-token", cfg.APIToken, "backend API token")
		id := fs.Int64("id", 0, "candidate id")
		status := fs.String("status", "", "review status accepted|rejected|invalid")
		reviewer := fs.String("reviewer", "stoaramactl", "reviewer label")
		reason := fs.String("reason", "", "review reason")
		metadataJSON := fs.String("metadata-json", "{}", "review metadata json")
		asJSON := fs.Bool("json", false, "print JSON")
		_ = fs.Parse(args[2:])
		if *id <= 0 {
			log.Fatalf("--id is required")
		}
		if strings.TrimSpace(*status) == "" {
			log.Fatalf("--status is required")
		}
		var meta map[string]any
		if err := json.Unmarshal([]byte(strings.TrimSpace(*metadataJSON)), &meta); err != nil {
			log.Fatalf("invalid --metadata-json: %v", err)
		}
		payload := mustAPIRequest(ctx, http.MethodPost, strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken), fmt.Sprintf("/api/v1/source-candidates/%d/review", *id), map[string]any{
			"status":        strings.TrimSpace(*status),
			"reviewer":      strings.TrimSpace(*reviewer),
			"reason":        strings.TrimSpace(*reason),
			"metadata_json": meta,
		})
		if *asJSON {
			printJSON(payload)
			return
		}
		fmt.Printf("candidate %d review_status=%s reviewer=%s\n", *id, fmt.Sprint(payload["review_status"]), strings.TrimSpace(*reviewer))
	case "import":
		fs := flag.NewFlagSet("discovery candidates import", flag.ExitOnError)
		backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
		apiToken := fs.String("api-token", cfg.APIToken, "backend API token")
		id := fs.Int64("id", 0, "candidate id")
		provider := fs.String("provider", "", "provider override")
		externalID := fs.String("external-id", "", "external id override")
		name := fs.String("name", "", "stream name override")
		slug := fs.String("slug", "", "stream slug override")
		sourceURL := fs.String("source-url", "", "source url override")
		sourcePageURL := fs.String("source-page-url", "", "source page url override")
		sourceFamily := fs.String("source-family", "", "source family override")
		captureType := fs.String("capture-type", "", "capture type override")
		executionClass := fs.String("execution-class", "", "execution class override")
		executionConfigJSON := fs.String("execution-config-json", "{}", "execution config json")
		tags := fs.String("tags", "", "comma-separated tags")
		locationText := fs.String("location-text", "", "location text override")
		locationCountry := fs.String("location-country", "", "location country override")
		locationCountryCode := fs.String("location-country-code", "", "location ISO country code override")
		locationRegion := fs.String("location-region", "", "location region override")
		locationCity := fs.String("location-city", "", "location city override")
		locationLocality := fs.String("location-locality", "", "location locality override")
		locationSource := fs.String("location-source", "", "location source override")
		metadataJSON := fs.String("metadata-json", "{}", "metadata json")
		asJSON := fs.Bool("json", false, "print JSON")
		_ = fs.Parse(args[2:])
		if *id <= 0 {
			log.Fatalf("--id is required")
		}
		var execCfg map[string]any
		if err := json.Unmarshal([]byte(strings.TrimSpace(*executionConfigJSON)), &execCfg); err != nil {
			log.Fatalf("invalid --execution-config-json: %v", err)
		}
		var meta map[string]any
		if err := json.Unmarshal([]byte(strings.TrimSpace(*metadataJSON)), &meta); err != nil {
			log.Fatalf("invalid --metadata-json: %v", err)
		}
		payload := mustAPIRequest(ctx, http.MethodPost, strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken), fmt.Sprintf("/api/v1/source-candidates/%d/import", *id), map[string]any{
			"provider":              strings.TrimSpace(*provider),
			"external_id":           strings.TrimSpace(*externalID),
			"name":                  strings.TrimSpace(*name),
			"slug":                  strings.TrimSpace(*slug),
			"source_url":            strings.TrimSpace(*sourceURL),
			"source_page_url":       strings.TrimSpace(*sourcePageURL),
			"source_family":         strings.TrimSpace(*sourceFamily),
			"capture_type":          strings.TrimSpace(*captureType),
			"execution_class":       strings.TrimSpace(*executionClass),
			"execution_config_json": execCfg,
			"tags":                  normalizeTags(splitCSV(*tags)),
			"location_text":         strings.TrimSpace(*locationText),
			"location_country":      strings.TrimSpace(*locationCountry),
			"location_country_code": strings.TrimSpace(*locationCountryCode),
			"location_region":       strings.TrimSpace(*locationRegion),
			"location_city":         strings.TrimSpace(*locationCity),
			"location_locality":     strings.TrimSpace(*locationLocality),
			"location_source":       strings.TrimSpace(*locationSource),
			"metadata_json":         meta,
		})
		if *asJSON {
			printJSON(payload)
			return
		}
		stream := asMap(payload["stream"])
		fmt.Printf("candidate %d imported stream_id=%d slug=%s capture_type=%s execution_class=%s\n", *id, int64FromAny(stream["id"]), fmt.Sprint(stream["slug"]), fmt.Sprint(stream["capture_type"]), fmt.Sprint(stream["execution_class"]))
	default:
		log.Fatalf("unknown discovery candidates subcommand: %s", args[1])
	}
}

type locationTagCleanupRow struct {
	ID                  int64
	Tags                []string
	LocationCountry     string
	LocationCountryCode string
	LocationRegion      string
	LocationCity        string
}

type locationTagCleanupResult struct {
	UpdatedTags []string
	RemovedTags []string
}

func cleanupLocationTagsForStream(row locationTagCleanupRow) locationTagCleanupResult {
	updated := make([]string, 0, len(row.Tags))
	removed := make([]string, 0, 4)
	hasCountry := strings.TrimSpace(row.LocationCountry) != "" || strings.TrimSpace(row.LocationCountryCode) != ""
	hasRegion := strings.TrimSpace(row.LocationRegion) != ""
	hasCity := strings.TrimSpace(row.LocationCity) != ""
	for _, tag := range row.Tags {
		clean := strings.TrimSpace(tag)
		if clean == "" {
			continue
		}
		lower := strings.ToLower(clean)
		drop := false
		switch {
		case strings.HasPrefix(lower, "city:"):
			drop = hasCity
		case strings.HasPrefix(lower, "country:"):
			drop = hasCountry
		case strings.HasPrefix(lower, "state:"):
			drop = hasRegion
		}
		if drop {
			removed = append(removed, clean)
			continue
		}
		updated = append(updated, clean)
	}
	updated = normalizeTags(updated)
	if updated == nil {
		updated = []string{}
	}
	return locationTagCleanupResult{
		UpdatedTags: updated,
		RemovedTags: normalizeTags(removed),
	}
}

func runStreamsCleanupLocationTags(ctx context.Context, cfg config.Config, args []string) {
	fs := flag.NewFlagSet("streams cleanup-location-tags", flag.ExitOnError)
	recordingState := fs.String("recording-state", "", "optional recording state off|on")
	limit := fs.Int("limit", 0, "optional max streams to scan")
	apply := fs.Bool("apply", false, "apply changes")
	asJSON := fs.Bool("json", false, "print JSON")
	_ = fs.Parse(args)
	if *limit < 0 {
		log.Fatalf("--limit must be >= 0")
	}
	state := strings.ToLower(strings.TrimSpace(*recordingState))
	if state != "" && state != string(model.RecordingStateOff) && state != string(model.RecordingStateOn) {
		log.Fatalf("--recording-state must be off|on")
	}

	pool := mustOpenPool(ctx, cfg)
	defer pool.Close()

	where := []string{"EXISTS (SELECT 1 FROM unnest(tags) t WHERE lower(trim(t)) ~ '^(city|country|state):')"}
	queryArgs := []any{}
	if state != "" {
		queryArgs = append(queryArgs, state)
		where = append(where, fmt.Sprintf("recording_state=$%d", len(queryArgs)))
	}
	limitClause := ""
	if *limit > 0 {
		queryArgs = append(queryArgs, *limit)
		limitClause = fmt.Sprintf(" LIMIT $%d", len(queryArgs))
	}
	rows, err := pool.Query(ctx, fmt.Sprintf(`
		SELECT id, tags, location_country, location_country_code, location_region, location_city
		FROM streams
		WHERE %s
		ORDER BY id ASC%s
	`, strings.Join(where, " AND "), limitClause), queryArgs...)
	if err != nil {
		log.Fatalf("query location-tag streams: %v", err)
	}
	candidates := make([]locationTagCleanupRow, 0, 1024)
	for rows.Next() {
		var row locationTagCleanupRow
		if err := rows.Scan(&row.ID, &row.Tags, &row.LocationCountry, &row.LocationCountryCode, &row.LocationRegion, &row.LocationCity); err != nil {
			rows.Close()
			log.Fatalf("scan location-tag stream: %v", err)
		}
		candidates = append(candidates, row)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		log.Fatalf("iterate location-tag streams: %v", err)
	}
	rows.Close()

	type changedRow struct {
		ID          int64    `json:"id"`
		RemovedTags []string `json:"removed_tags"`
		UpdatedTags []string `json:"updated_tags,omitempty"`
	}
	changed := make([]changedRow, 0, len(candidates))
	removedTotal := 0
	for _, row := range candidates {
		result := cleanupLocationTagsForStream(row)
		if len(result.RemovedTags) == 0 {
			continue
		}
		removedTotal += len(result.RemovedTags)
		changed = append(changed, changedRow{
			ID:          row.ID,
			RemovedTags: result.RemovedTags,
			UpdatedTags: result.UpdatedTags,
		})
	}

	if *apply && len(changed) > 0 {
		tx, err := pool.Begin(ctx)
		if err != nil {
			log.Fatalf("begin location-tag cleanup: %v", err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		for _, row := range changed {
			if _, err := tx.Exec(ctx, `
				UPDATE streams
				SET tags=$2, updated_at=now()
				WHERE id=$1
			`, row.ID, row.UpdatedTags); err != nil {
				log.Fatalf("update stream %d tags: %v", row.ID, err)
			}
		}
		if err := tx.Commit(ctx); err != nil {
			log.Fatalf("commit location-tag cleanup: %v", err)
		}
	}

	if *asJSON {
		printJSON(map[string]any{
			"apply":        *apply,
			"scanned":      len(candidates),
			"changed":      len(changed),
			"removed_tags": removedTotal,
			"items":        changed,
		})
		return
	}
	action := "dry_run"
	if *apply {
		action = "applied"
	}
	fmt.Printf("location_tag_cleanup=%s scanned=%d changed=%d removed_tags=%d\n", action, len(candidates), len(changed), removedTotal)
	for i, row := range changed {
		if i >= 20 {
			fmt.Printf("... %d more changed streams\n", len(changed)-i)
			break
		}
		fmt.Printf("stream_id=%d removed=%s\n", row.ID, strings.Join(row.RemovedTags, ","))
	}
}

type streamMetadataAuditPage struct {
	Items  []streamMetadataAuditItem `json:"items"`
	Limit  int                       `json:"limit"`
	Offset int                       `json:"offset"`
	Total  int64                     `json:"total"`
}

type streamMetadataAuditItem struct {
	Stream streamMetadataAuditStream `json:"stream"`
}

type streamMetadataAuditStream struct {
	ID           int64          `json:"id"`
	Provider     string         `json:"provider"`
	Name         string         `json:"name"`
	Slug         string         `json:"slug"`
	LocationText string         `json:"location_text"`
	MetadataJSON map[string]any `json:"metadata_json"`
}

type streamMetadataIssue struct {
	StreamID     int64
	Provider     string
	Slug         string
	Name         string
	LocationText string
	LocationCity string
	NameCity     string
	MetadataCity string
	MetadataKey  string
	IssueType    string
	IssueReason  string
}

func runStreamMetadataAudit(ctx context.Context, cfg config.Config, args []string) {
	fs := flag.NewFlagSet("streams metadata-audit", flag.ExitOnError)
	backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
	apiToken := fs.String("api-token", cfg.APIToken, "backend API token")
	recordingState := fs.String("recording-state", "", "optional recording state off|on")
	pageSize := fs.Int("page-size", 500, "dashboard stream page size")
	sampleLimit := fs.Int("sample-limit", 40, "max issue samples to print")
	allowGenericLocationCity := fs.Bool("allow-generic-location-city", false, "include generic discovery city buckets for metadata backfill")
	applyGenericLocationFixes := fs.Bool("apply-generic-location-fixes", false, "when --apply, also update generic-bucket location_text from inferred name city")
	apply := fs.Bool("apply", false, "apply metadata city fixes")
	maxUpdates := fs.Int("max-updates", 0, "max updates when --apply (0=all)")
	_ = fs.Parse(args)

	base := strings.TrimRight(strings.TrimSpace(*backendAPIURL), "/")
	if base == "" {
		log.Fatalf("--backend-api-url is required (or set BACKEND_API_URL)")
	}
	token := strings.TrimSpace(*apiToken)
	if token == "" {
		log.Fatalf("--api-token is required (or set API_TOKEN)")
	}
	state := strings.ToLower(strings.TrimSpace(*recordingState))
	if state != "" && state != "off" && state != "on" {
		log.Fatalf("--recording-state must be off|on")
	}
	if *pageSize <= 0 || *pageSize > 2000 {
		log.Fatalf("--page-size must be between 1 and 2000")
	}
	if *sampleLimit < 0 {
		log.Fatalf("--sample-limit must be >= 0")
	}
	if *maxUpdates < 0 {
		log.Fatalf("--max-updates must be >= 0")
	}

	client := &http.Client{Timeout: 45 * time.Second}
	offset := 0
	scanned := 0
	var totalExpected int64
	missingCity := 0
	mismatchCity := 0
	genericLocationMismatch := 0
	candidateFixes := 0
	appliedUpdates := 0
	samples := make([]streamMetadataIssue, 0, *sampleLimit)

	for {
		params := url.Values{}
		params.Set("limit", strconv.Itoa(*pageSize))
		params.Set("offset", strconv.Itoa(offset))
		params.Set("include_image_urls", "false")
		if state != "" {
			params.Set("recording_state", state)
		}
		reqURL := fmt.Sprintf("%s/api/v1/dashboard/streams?%s", base, params.Encode())
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
		if err != nil {
			log.Fatalf("build audit request: %v", err)
		}
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := client.Do(req)
		if err != nil {
			log.Fatalf("fetch stream page offset=%d: %v", offset, err)
		}
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
			_ = resp.Body.Close()
			log.Fatalf("fetch stream page offset=%d failed: status=%d body=%q", offset, resp.StatusCode, strings.TrimSpace(string(body)))
		}
		var page streamMetadataAuditPage
		if err := json.NewDecoder(resp.Body).Decode(&page); err != nil {
			_ = resp.Body.Close()
			log.Fatalf("decode stream page offset=%d: %v", offset, err)
		}
		_ = resp.Body.Close()
		if page.Total > 0 {
			totalExpected = page.Total
		}
		if len(page.Items) == 0 {
			break
		}

		for _, row := range page.Items {
			s := row.Stream
			locationCity := extractLocationCityToken(s.LocationText)
			if !isLikelyCityToken(locationCity) {
				continue
			}
			nameCity := inferNameCityToken(s.Name)
			metaCity, metaKey := extractMetadataCityToken(s.MetadataJSON)
			issueType := ""
			reason := ""
			shouldFixLocationText := false
			switch {
			case strings.EqualFold(s.Provider, "YOUTUBE") && isGenericLocationBucket(locationCity) && nameCity != "" && !cityEqual(nameCity, locationCity):
				issueType = "generic_location_city_mismatch"
				reason = "location_text is generic bucket but stream name implies a more specific city"
				genericLocationMismatch++
				shouldFixLocationText = true
			case isGenericLocationBucket(locationCity) && !*allowGenericLocationCity:
				continue
			case metaCity == "":
				issueType = "missing_city"
				reason = "metadata city missing"
				missingCity++
			case !cityEqual(metaCity, locationCity):
				issueType = "mismatch_city"
				reason = metadataCityMismatchReason(metaCity, locationCity, s.LocationText)
				mismatchCity++
			}
			if issueType == "" {
				continue
			}
			fixCity := locationCity
			if shouldFixLocationText && nameCity != "" {
				fixCity = nameCity
			}
			candidateFixes++
			issue := streamMetadataIssue{
				StreamID:     s.ID,
				Provider:     s.Provider,
				Slug:         s.Slug,
				Name:         s.Name,
				LocationText: s.LocationText,
				LocationCity: locationCity,
				NameCity:     nameCity,
				MetadataCity: metaCity,
				MetadataKey:  metaKey,
				IssueType:    issueType,
				IssueReason:  reason,
			}
			if len(samples) < cap(samples) {
				samples = append(samples, issue)
			}
			if !*apply {
				continue
			}
			if *maxUpdates > 0 && appliedUpdates >= *maxUpdates {
				continue
			}
			if shouldFixLocationText && !*applyGenericLocationFixes {
				continue
			}
			updatedMeta := cloneMetadataMap(s.MetadataJSON)
			updatedMeta["city"] = fixCity
			var locationOverride *string
			if shouldFixLocationText {
				v := fixCity
				locationOverride = &v
			}
			if err := patchStream(ctx, client, base, token, s.ID, locationOverride, updatedMeta); err != nil {
				log.Fatalf("apply metadata fix stream_id=%d slug=%s: %v", s.ID, s.Slug, err)
			}
			appliedUpdates++
			if appliedUpdates <= 20 || appliedUpdates%100 == 0 {
				if shouldFixLocationText {
					fmt.Printf("updated stream_id=%d city=%q location_text=%q (old_location=%q metadata_city=%q)\n", s.ID, fixCity, fixCity, s.LocationText, metaCity)
				} else {
					fmt.Printf("updated stream_id=%d city=%q (metadata_city=%q location_text=%q)\n", s.ID, fixCity, metaCity, s.LocationText)
				}
			}
		}

		scanned += len(page.Items)
		offset += *pageSize
		if int64(scanned) >= page.Total || len(page.Items) < *pageSize {
			break
		}
	}

	fmt.Printf(
		"metadata audit complete: scanned=%d total=%d missing_city=%d mismatch_city=%d generic_location_mismatch=%d candidate_fixes=%d applied_updates=%d\n",
		scanned, totalExpected, missingCity, mismatchCity, genericLocationMismatch, candidateFixes, appliedUpdates,
	)
	if len(samples) > 0 {
		fmt.Println("issue samples:")
		for _, it := range samples {
			fmt.Printf(
				"  id=%d provider=%s slug=%s issue=%s reason=%s metadata_city=%q metadata_key=%q location_city=%q name_city=%q location_text=%q name=%q\n",
				it.StreamID, it.Provider, it.Slug, it.IssueType, it.IssueReason, it.MetadataCity, it.MetadataKey, it.LocationCity, it.NameCity, it.LocationText, it.Name,
			)
		}
	}
	if !*apply {
		fmt.Println("dry-run only; rerun with --apply to update metadata_json.city from location_text city token (generic location_text fixes require --apply-generic-location-fixes)")
	}
}

func patchStream(ctx context.Context, client *http.Client, baseURL string, apiToken string, streamID int64, locationText *string, metadata map[string]any) error {
	payload := map[string]any{"metadata_json": metadata}
	if locationText != nil {
		payload["location_text"] = strings.TrimSpace(*locationText)
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal patch payload: %w", err)
	}
	reqURL := fmt.Sprintf("%s/api/v1/streams/%d", baseURL, streamID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPatch, reqURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build patch request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+apiToken)
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("patch request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("patch status=%d body=%q", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}
	return nil
}

func mustAPIRequest(ctx context.Context, method string, baseURL string, apiToken string, path string, payload any) map[string]any {
	m := strings.TrimSpace(strings.ToUpper(method))
	if m == "" {
		log.Fatalf("api method is required")
	}
	base := strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if base == "" {
		log.Fatalf("--backend-api-url is required")
	}
	token := strings.TrimSpace(apiToken)
	if token == "" {
		log.Fatalf("--api-token is required")
	}
	p := strings.TrimSpace(path)
	if p == "" {
		log.Fatalf("api path is required")
	}
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	reqURL := base + p
	var body io.Reader
	if payload != nil {
		raw, err := json.Marshal(payload)
		if err != nil {
			log.Fatalf("marshal api payload: %v", err)
		}
		body = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, m, reqURL, body)
	if err != nil {
		log.Fatalf("build api request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	switch m {
	case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		req.Header.Set("Idempotency-Key", fmt.Sprintf("%s:%s:%d", m, p, time.Now().UnixNano()))
	}
	client := &http.Client{Timeout: 45 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		log.Fatalf("api request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		log.Fatalf("api request failed method=%s status=%d body=%q", m, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		log.Fatalf("decode api response: %v", err)
	}
	return out
}

func mustAPIGet(ctx context.Context, baseURL string, apiToken string, path string) map[string]any {
	return mustAPIRequest(ctx, http.MethodGet, baseURL, apiToken, path, nil)
}

func cloneMetadataMap(in map[string]any) map[string]any {
	if len(in) == 0 {
		return map[string]any{}
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func metadataStringValue(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return strings.Join(strings.Fields(strings.TrimSpace(t)), " ")
	default:
		return strings.Join(strings.Fields(strings.TrimSpace(fmt.Sprint(t))), " ")
	}
}

func extractMetadataCityToken(metadata map[string]any) (city string, key string) {
	keys := []string{"city", "locality", "town", "municipality"}
	for _, k := range keys {
		if metadata == nil {
			break
		}
		v, ok := metadata[k]
		if !ok {
			continue
		}
		clean := metadataStringValue(v)
		if clean == "" {
			continue
		}
		return clean, k
	}
	return "", ""
}

func extractLocationCityToken(locationText string) string {
	raw := strings.TrimSpace(locationText)
	if raw == "" {
		return ""
	}
	city := raw
	if idx := strings.Index(raw, ","); idx >= 0 {
		city = raw[:idx]
	}
	city = strings.Join(strings.Fields(strings.TrimSpace(city)), " ")
	return city
}

func isGenericLocationBucket(v string) bool {
	norm := strings.ToLower(strings.TrimSpace(v))
	if norm == "" {
		return true
	}
	switch norm {
	case
		"nyc", "new-york-city", "new york city", "new-york-state", "new york state",
		"seattle", "ontario", "alberta", "la", "boston",
		"london", "tokyo", "seoul", "toronto", "madrid", "bangkok", "rome", "berlin", "dubai", "sydney",
		"chicago", "houston", "miami", "philadelphia", "singapore", "sao-paulo", "mexico-city",
		"us", "usa", "ca", "canada":
		return true
	default:
		return false
	}
}

func inferNameCityToken(name string) string {
	raw := strings.TrimSpace(name)
	if raw == "" {
		return ""
	}
	parts := strings.Split(raw, ",")
	candidates := make([]string, 0, 3)
	if len(parts) >= 3 {
		candidates = append(candidates, parts[len(parts)-2])
	}
	if len(parts) >= 2 {
		candidates = append(candidates, parts[0])
	}
	candidates = append(candidates, raw)
	for _, cand := range candidates {
		city := cleanNameCityCandidate(cand)
		if !isLikelyNameCityCandidate(city) {
			continue
		}
		return city
	}
	return ""
}

func cleanNameCityCandidate(v string) string {
	s := strings.TrimSpace(v)
	s = strings.Trim(s, "-|:/! ")
	lower := strings.ToLower(s)
	for _, p := range []string{"live ", "live! ", "live: "} {
		if strings.HasPrefix(lower, p) {
			s = strings.TrimSpace(s[len(p):])
			lower = strings.ToLower(s)
			break
		}
	}
	if idx := strings.Index(s, "|"); idx >= 0 {
		s = strings.TrimSpace(s[:idx])
	}
	if idx := strings.Index(s, "@"); idx >= 0 {
		s = strings.TrimSpace(s[:idx])
	}
	s = strings.Trim(s, "-|:/! ")
	return strings.Join(strings.Fields(s), " ")
}

func isLikelyNameCityCandidate(v string) bool {
	city := strings.TrimSpace(v)
	if city == "" {
		return false
	}
	lower := strings.ToLower(city)
	for _, bad := range []string{
		"live", "cam", "camera", "railcam", "webcam", "traffic", "street", "st ", "road",
		"blvd", "boulevard", "avenue", "ave", "highway", "hwy", "bridge", "station", "pkwy", "parkway", "expy", "expressway",
		" e/b", " w/b", " n/b", " s/b",
		"beach", "harbor", "harbour", "port", "airport", "square", "plaza", "park", "downtown",
		"music", "timelapse", "travel", "world",
	} {
		if strings.Contains(lower, bad) {
			return false
		}
	}
	if len(strings.Fields(city)) > 4 {
		return false
	}
	for _, r := range city {
		if unicode.IsDigit(r) {
			return false
		}
	}
	return isLikelyCityToken(city)
}

func isLikelyCityToken(v string) bool {
	city := strings.TrimSpace(v)
	if city == "" {
		return false
	}
	switch strings.ToLower(city) {
	case "-", "--", "n/a", "na", "none", "null", "unknown":
		return false
	}
	if len(city) <= 3 && city == strings.ToUpper(city) {
		return false
	}
	hasLetter := false
	for _, r := range city {
		if unicode.IsLetter(r) {
			hasLetter = true
			break
		}
	}
	return hasLetter
}

func cityEqual(a, b string) bool {
	return strings.EqualFold(strings.Join(strings.Fields(strings.TrimSpace(a)), " "), strings.Join(strings.Fields(strings.TrimSpace(b)), " "))
}

func metadataCityMismatchReason(metadataCity, locationCity, locationText string) string {
	meta := strings.ToLower(strings.TrimSpace(metadataCity))
	loc := strings.ToLower(strings.TrimSpace(locationCity))
	if meta == "" || loc == "" {
		return "city_mismatch"
	}
	parts := strings.Split(locationText, ",")
	if len(parts) > 1 {
		second := strings.ToLower(strings.TrimSpace(parts[1]))
		if second != "" && meta == second {
			return "metadata_city_matches_region_token"
		}
	}
	if strings.HasSuffix(loc, " "+meta) || strings.HasPrefix(loc, meta+" ") {
		return "metadata_city_partial_of_location_city"
	}
	return "metadata_city_mismatch"
}

func runMedia(ctx context.Context, cfg config.Config, args []string) {
	if len(args) < 1 {
		log.Fatalf("usage: stoaramactl media backfill --snapshot-root <path> [--concurrency 8 --dry-run]")
	}
	sub := args[0]
	if sub != "backfill" {
		log.Fatalf("unknown media subcommand: %s", sub)
	}
	fs := flag.NewFlagSet("media backfill", flag.ExitOnError)
	snapshotRoot := fs.String("snapshot-root", "", "snapshot root directory")
	concurrency := fs.Int("concurrency", 8, "worker count")
	dryRun := fs.Bool("dry-run", false, "dry run")
	_ = fs.Parse(args[1:])
	if strings.TrimSpace(*snapshotRoot) == "" {
		log.Fatalf("--snapshot-root is required")
	}
	if *concurrency <= 0 {
		log.Fatalf("--concurrency must be > 0")
	}

	pool := mustOpenPool(ctx, cfg)
	defer pool.Close()

	if !*dryRun {
		if err := cfg.ValidateR2(); err != nil {
			log.Fatalf("R2 config required for backfill: %v", err)
		}
	}

	streamIDs, err := loadStreamIDBySlug(ctx, pool)
	if err != nil {
		log.Fatalf("load streams: %v", err)
	}

	files, err := collectSnapshotFiles(*snapshotRoot)
	if err != nil {
		log.Fatalf("collect snapshot files: %v", err)
	}
	sort.Strings(files)
	if len(files) == 0 {
		fmt.Println("no snapshot files found")
		return
	}

	var r2c *r2.Client
	if !*dryRun {
		r2c, err = r2.New(ctx, r2.Config{
			AccountID: cfg.R2AccountID,
			AccessKey: cfg.R2AccessKeyID,
			SecretKey: cfg.R2SecretAccessKey,
			Region:    cfg.R2Region,
			Bucket:    cfg.R2Bucket,
			Endpoint:  cfg.R2Endpoint,
		})
		if err != nil {
			log.Fatalf("init r2: %v", err)
		}
	}

	type result struct {
		ok    bool
		skips bool
		err   error
	}
	workCh := make(chan string)
	resCh := make(chan result)
	var wg sync.WaitGroup

	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for p := range workCh {
				streamID, capturedAt, ok := inferStreamAndCaptureTime(*snapshotRoot, p, streamIDs)
				if !ok {
					resCh <- result{skips: true}
					continue
				}
				if *dryRun {
					resCh <- result{ok: true}
					continue
				}
				if err := backfillOneSnapshot(ctx, pool, r2c, streamID, p, capturedAt); err != nil {
					resCh <- result{err: err}
					continue
				}
				resCh <- result{ok: true}
			}
		}()
	}

	go func() {
		for _, p := range files {
			workCh <- p
		}
		close(workCh)
		wg.Wait()
		close(resCh)
	}()

	var okCount, skipCount, errCount int
	var processed int
	lastLog := time.Now()
	for r := range resCh {
		processed++
		if r.err != nil {
			errCount++
			log.Printf("backfill error: %v", r.err)
		} else if r.skips {
			skipCount++
		} else if r.ok {
			okCount++
		}
		if processed%1000 == 0 || time.Since(lastLog) >= 30*time.Second {
			lastLog = time.Now()
			log.Printf("backfill progress: processed=%d/%d uploaded=%d skipped=%d errors=%d", processed, len(files), okCount, skipCount, errCount)
		}
	}
	fmt.Printf("media backfill complete: uploaded=%d skipped=%d errors=%d dry_run=%t\n", okCount, skipCount, errCount, *dryRun)
}

func runInference(ctx context.Context, cfg config.Config, args []string) {
	if len(args) < 1 {
		log.Fatalf("usage: stoaramactl inference <list|cleanup-unboxed> ...")
	}
	sub := args[0]
	switch sub {
	case "list":
		fs := flag.NewFlagSet("inference list", flag.ExitOnError)
		backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
		apiToken := fs.String("api-token", cfg.APIToken, "backend API token")
		streamID := fs.Int64("stream-id", 0, "optional stream id")
		pipelineID := fs.String("pipeline-id", "", "optional pipeline id")
		status := fs.String("status", "", "optional status queued_boxed|success|error")
		className := fs.String("class-name", "", "optional class name filter")
		search := fs.String("search", "", "optional free text filter")
		minConfidence := fs.Float64("min-confidence", 0, "optional min confidence (0 disables)")
		createdFrom := fs.String("created-from", "", "optional created_from (RFC3339 or YYYY-MM-DD)")
		createdTo := fs.String("created-to", "", "optional created_to (RFC3339 or YYYY-MM-DD)")
		capturedFrom := fs.String("captured-from", "", "optional captured_from (RFC3339 or YYYY-MM-DD)")
		capturedTo := fs.String("captured-to", "", "optional captured_to (RFC3339 or YYYY-MM-DD)")
		hasBoxed := fs.String("has-boxed", "", "optional boxed filter true|false")
		recordingState := fs.String("recording-state", "", "optional recording state off|on")
		sortBy := fs.String("sort-by", "created_at", "sort field created_at|captured_at|pipeline_id|status|stream_id|detection_count|max_confidence|signal_count|signal_strength")
		sortDir := fs.String("sort-dir", "desc", "sort direction asc|desc")
		limit := fs.Int("limit", 200, "row limit")
		offset := fs.Int("offset", 0, "row offset")
		asJSON := fs.Bool("json", false, "print JSON")
		_ = fs.Parse(args[1:])
		if *limit <= 0 || *limit > 2000 {
			log.Fatalf("--limit must be between 1 and 2000")
		}
		if *offset < 0 {
			log.Fatalf("--offset must be >= 0")
		}
		q := url.Values{}
		q.Set("limit", strconv.Itoa(*limit))
		q.Set("offset", strconv.Itoa(*offset))
		q.Set("sort_by", strings.TrimSpace(*sortBy))
		dir := strings.ToLower(strings.TrimSpace(*sortDir))
		if dir != "asc" && dir != "desc" {
			log.Fatalf("--sort-dir must be asc or desc")
		}
		q.Set("sort_dir", dir)
		if *streamID > 0 {
			q.Set("stream_id", strconv.FormatInt(*streamID, 10))
		}
		if v := strings.TrimSpace(*pipelineID); v != "" {
			q.Set("pipeline_id", v)
		}
		if v := strings.TrimSpace(strings.ToLower(*status)); v != "" {
			switch v {
			case "queued_boxed", "success", "error":
			default:
				log.Fatalf("--status must be queued_boxed|success|error")
			}
			q.Set("status", v)
		}
		if v := strings.TrimSpace(*className); v != "" {
			q.Set("class_name", v)
		}
		if v := strings.TrimSpace(*search); v != "" {
			q.Set("q", v)
		}
		if *minConfidence > 0 {
			q.Set("min_confidence", strconv.FormatFloat(*minConfidence, 'f', -1, 64))
		}
		if v := strings.TrimSpace(*createdFrom); v != "" {
			q.Set("created_from", v)
		}
		if v := strings.TrimSpace(*createdTo); v != "" {
			q.Set("created_to", v)
		}
		if v := strings.TrimSpace(*capturedFrom); v != "" {
			q.Set("captured_from", v)
		}
		if v := strings.TrimSpace(*capturedTo); v != "" {
			q.Set("captured_to", v)
		}
		if v := strings.TrimSpace(strings.ToLower(*hasBoxed)); v != "" {
			switch v {
			case "true", "1", "yes", "y", "on":
				q.Set("has_boxed", "true")
			case "false", "0", "no", "n", "off":
				q.Set("has_boxed", "false")
			default:
				log.Fatalf("--has-boxed must be true or false")
			}
		}
		if v := strings.TrimSpace(strings.ToLower(*recordingState)); v != "" {
			switch v {
			case "off", "on":
			default:
				log.Fatalf("--recording-state must be off|on")
			}
			q.Set("recording_state", v)
		}
		payload := mustAPIGet(ctx, strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken), "/api/v1/dashboard/inference?"+q.Encode())
		if *asJSON {
			printJSON(payload)
			return
		}
		items, _ := payload["items"].([]any)
		fmt.Printf("inference_rows=%d limit=%d offset=%d\n", len(items), *limit, *offset)
		for _, raw := range items {
			it := asMap(raw)
			fmt.Printf("result_id=%v stream_id=%v frame_id=%v pipeline=%v status=%v detections=%v max_conf=%v created_at=%v boxed=%v error=%v\n",
				it["inference_result_id"], it["stream_id"], it["frame_id"], it["pipeline_id"], it["status"], it["detection_count"], it["max_confidence"], it["created_at"], it["boxed_object_key"], it["error_text"])
		}
	case "cleanup-unboxed":
		fs := flag.NewFlagSet("inference cleanup-unboxed", flag.ExitOnError)
		backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
		apiToken := fs.String("api-token", cfg.APIToken, "backend API token")
		pipelineID := fs.String("pipeline-id", "", "optional pipeline id")
		mode := fs.String("mode", "requeue", "cleanup mode requeue|delete")
		dryRun := fs.Bool("dry-run", false, "dry run")
		asJSON := fs.Bool("json", false, "print JSON")
		_ = fs.Parse(args[1:])
		modeVal := strings.ToLower(strings.TrimSpace(*mode))
		if modeVal != "requeue" && modeVal != "delete" {
			log.Fatalf("--mode must be requeue or delete")
		}
		q := url.Values{}
		q.Set("mode", modeVal)
		q.Set("dry_run", strconv.FormatBool(*dryRun))
		if v := strings.TrimSpace(*pipelineID); v != "" {
			q.Set("pipeline_id", v)
		}
		payload := mustAPIRequest(ctx, http.MethodPost, strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken), "/api/v1/dashboard/inference/cleanup-unboxed?"+q.Encode(), nil)
		if *asJSON {
			printJSON(payload)
			return
		}
		fmt.Printf("cleanup mode=%s dry_run=%t inference_candidates=%v detections_candidates=%v deleted_results=%v deleted_detections=%v requeued=%v\n",
			modeVal, *dryRun, payload["inference_results_candidates"], payload["detections_candidates"], payload["deleted_inference_results"], payload["deleted_detections"], payload["requeued_results"])
	default:
		log.Fatalf("unknown inference subcommand: %s", sub)
	}
}

func runPipelines(ctx context.Context, cfg config.Config, args []string) {
	if len(args) >= 1 && (args[0] == "-h" || args[0] == "--help") {
		printPipelinesUsage()
		return
	}
	if len(args) < 1 {
		printPipelinesUsage()
		return
	}
	switch args[0] {
	case "list":
		fs := flag.NewFlagSet("pipelines list", flag.ExitOnError)
		backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
		apiToken := fs.String("api-token", cfg.APIToken, "backend API token")
		asJSON := fs.Bool("json", false, "print JSON")
		_ = fs.Parse(args[1:])
		payload := mustAPIGet(ctx, strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken), "/api/v1/pipelines")
		if *asJSON {
			printJSON(payload)
			return
		}
		items, _ := payload["items"].([]any)
		for _, raw := range items {
			it := asMap(raw)
			fmt.Printf("id=%s family=%s kind=%s active=%t created_at=%s updated_at=%s\n",
				fmt.Sprint(it["id"]),
				fmt.Sprint(it["pipeline_family"]),
				fmt.Sprint(it["kind"]),
				boolFromAny(it["active"]),
				fmt.Sprint(it["created_at"]),
				fmt.Sprint(it["updated_at"]),
			)
		}
	case "overview":
		runOverviewSurface(ctx, cfg, append([]string{"pipelines"}, args[1:]...))
	case "register":
		runPipelineRegister(ctx, cfg, args[1:])
	case "versions":
		runPipelineVersions(ctx, cfg, args[1:])
	case "runs":
		runPipelineRuns(ctx, cfg, args[1:])
	case "stream-list":
		fs := flag.NewFlagSet("pipelines stream-list", flag.ExitOnError)
		backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
		apiToken := fs.String("api-token", cfg.APIToken, "backend API token")
		id := fs.Int64("id", 0, "stream id")
		asJSON := fs.Bool("json", false, "print JSON")
		_ = fs.Parse(args[1:])
		if *id <= 0 {
			log.Fatalf("--id is required")
		}
		payload := mustAPIGet(ctx, strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken), fmt.Sprintf("/api/v1/dashboard/streams/%d/pipelines", *id))
		if *asJSON {
			printJSON(payload)
			return
		}
		items, _ := payload["items"].([]any)
		for _, raw := range items {
			it := asMap(raw)
			fmt.Printf(
				"stream=%d pipeline=%s family=%s kind=%s active=%t enabled=%t override=%t processed=%d backlog=%d active_claims=%d\n",
				*id,
				fmt.Sprint(it["pipeline_id"]),
				fmt.Sprint(it["pipeline_family"]),
				fmt.Sprint(it["kind"]),
				boolFromAny(it["active"]),
				boolFromAny(it["enabled"]),
				boolFromAny(it["has_override"]),
				int64FromAny(it["processed_frames"]),
				int64FromAny(it["backlog_frames"]),
				int64FromAny(it["active_claims"]),
			)
		}
	case "set":
		fs := flag.NewFlagSet("pipelines set", flag.ExitOnError)
		backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
		apiToken := fs.String("api-token", cfg.APIToken, "backend API token")
		streamID := fs.Int64("stream-id", 0, "stream id")
		pipelineID := fs.String("pipeline-id", "", "pipeline id")
		enabled := fs.Bool("enabled", true, "whether this stream should run this pipeline")
		updatedBy := fs.String("updated-by", "stoaramactl", "audit label")
		asJSON := fs.Bool("json", false, "print JSON")
		_ = fs.Parse(args[1:])
		if *streamID <= 0 {
			log.Fatalf("--stream-id is required")
		}
		if strings.TrimSpace(*pipelineID) == "" {
			log.Fatalf("--pipeline-id is required")
		}
		path := fmt.Sprintf("/api/v1/dashboard/streams/%d/pipelines/%s", *streamID, url.PathEscape(strings.TrimSpace(*pipelineID)))
		payload := mustAPIRequest(ctx, http.MethodPut, strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken), path, map[string]any{
			"enabled":    *enabled,
			"updated_by": strings.TrimSpace(*updatedBy),
		})
		if *asJSON {
			printJSON(payload)
			return
		}
		fmt.Printf("stream=%d pipeline=%s enabled=%t\n", *streamID, strings.TrimSpace(*pipelineID), *enabled)
	default:
		log.Fatalf("unknown pipelines subcommand: %s", args[0])
	}
}

func runOverview(ctx context.Context, cfg config.Config, args []string) {
	if len(args) >= 1 && (args[0] == "-h" || args[0] == "--help") {
		fmt.Print("stoaramactl overview <summary|queue-health> ...\n")
		return
	}
	if len(args) < 1 {
		runOverviewSurface(ctx, cfg, []string{"overview"})
		return
	}
	if len(args) >= 2 && (args[1] == "-h" || args[1] == "--help") {
		switch args[0] {
		case "summary":
			fmt.Print("stoaramactl overview summary [--backend-api-url URL --api-token TOKEN]\n")
		case "queue-health":
			fmt.Print("stoaramactl overview queue-health [--backend-api-url URL --api-token TOKEN]\n")
		default:
			log.Fatalf("usage: stoaramactl overview <summary|queue-health> ...")
		}
		return
	}
	switch args[0] {
	case "summary":
		runOverviewSurface(ctx, cfg, append([]string{"overview"}, args[1:]...))
	case "queue-health":
		runOverviewSurface(ctx, cfg, args)
	default:
		log.Fatalf("usage: stoaramactl overview <summary|queue-health> ...")
	}
}

func runOverviewSurface(ctx context.Context, cfg config.Config, args []string) {
	if len(args) < 1 {
		log.Fatalf("usage: stoaramactl overview <summary|status|queue-health> ...")
	}
	sub := args[0]
	switch sub {
	case "overview":
		fs := flag.NewFlagSet("overview summary", flag.ExitOnError)
		backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
		apiToken := fs.String("api-token", cfg.APIToken, "backend API token")
		asJSON := fs.Bool("json", false, "print JSON")
		_ = fs.Parse(args[1:])
		payload := mustAPIGet(ctx, strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken), "/api/v1/dashboard/overview")
		if *asJSON {
			printJSON(payload)
			return
		}
		fmt.Printf("streams_total=%v recording_on=%v recording_off=%v interval_sec=%v healthy=%v degraded=%v stale=%v\n",
			payload["streams_total"], payload["recording_on"], payload["recording_off"], payload["recording_interval_sec"],
			payload["recording_healthy_total"], payload["recording_degraded_total"], payload["recording_stale_total"])
	case "servers":
		fs := flag.NewFlagSet("servers list", flag.ExitOnError)
		backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
		apiToken := fs.String("api-token", cfg.APIToken, "backend API token")
		hours := fs.Int("hours", 24*7, "lookback hours for non-active servers")
		includeStale := fs.Bool("include-stale", false, "include stale server rows")
		showProcesses := fs.Bool("show-processes", true, "print process-level detail")
		asJSON := fs.Bool("json", false, "print JSON")
		_ = fs.Parse(args[1:])
		if *hours <= 0 {
			log.Fatalf("--hours must be > 0")
		}
		path := fmt.Sprintf("/api/v1/dashboard/servers?hours=%d&include_stale=%t", *hours, *includeStale)
		payload := mustAPIGet(ctx, strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken), path)
		if *asJSON {
			printJSON(payload)
			return
		}
		items, _ := payload["items"].([]any)
		fmt.Printf("servers=%d hours=%d active=%v\n", len(items), *hours, payload["active"])
		for _, raw := range items {
			it, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			id := fmt.Sprint(it["server_id"])
			lastSeen := fmt.Sprint(it["last_seen_at"])
			active := fmt.Sprint(it["active"])
			captureGroups := 0
			captureGroupsDetail := "-"
			if arr, ok := it["execution_classes"].([]any); ok {
				type groupSummary struct {
					capacity         int
					executionClasses []string
					drainingAny      bool
					staleAny         bool
				}
				groupSummaries := map[string]*groupSummary{}
				for _, rawExecutionClass := range arr {
					executionClassItem, ok := rawExecutionClass.(map[string]any)
					if !ok {
						continue
					}
					executionClass := strings.TrimSpace(fmt.Sprint(executionClassItem["execution_class"]))
					if executionClass == "" {
						continue
					}
					group := strings.TrimSpace(fmt.Sprint(executionClassItem["capacity_group"]))
					if group == "" || group == "<nil>" {
						group = executionClass
					}
					capacity := 0
					if n, ok := asFloat64(executionClassItem["capacity"]); ok {
						capacity = int(n)
					}
					summary, exists := groupSummaries[group]
					if !exists {
						summary = &groupSummary{capacity: capacity}
						groupSummaries[group] = summary
					}
					summary.executionClasses = append(summary.executionClasses, executionClass)
					if capacity > 0 && (summary.capacity <= 0 || capacity < summary.capacity) {
						summary.capacity = capacity
					}
					if draining, ok := executionClassItem["draining"].(bool); ok && draining {
						summary.drainingAny = true
					}
					if activeMode, ok := executionClassItem["active"].(bool); ok && !activeMode {
						summary.staleAny = true
					}
				}
				captureGroups = len(groupSummaries)
				groupParts := make([]string, 0, len(groupSummaries))
				for group, summary := range groupSummaries {
					sort.Strings(summary.executionClasses)
					part := fmt.Sprintf("%s=%d[%s]", group, summary.capacity, strings.Join(summary.executionClasses, "/"))
					if summary.drainingAny {
						part += "(draining)"
					}
					if summary.staleAny {
						part += "(stale)"
					}
					groupParts = append(groupParts, part)
				}
				sort.Strings(groupParts)
				if len(groupParts) > 0 {
					captureGroupsDetail = strings.Join(groupParts, ",")
				}
			}
			inferenceGroups := 0
			if arr, ok := it["active_inference"].([]any); ok {
				inferenceGroups = len(arr)
			}
			processes := 0
			if arr, ok := it["processes"].([]any); ok {
				processes = len(arr)
			}
			activeCaptureStreams := 0
			if arr, ok := it["active_capture_stream_ids"].([]any); ok {
				activeCaptureStreams = len(arr)
			}
			connectionsDetail := summarizeServerConnectionsDashboard(it)
			fmt.Printf("server=%s active=%s last_seen=%s processes=%d capture_groups=%d groups=%s active_capture_streams=%d active_inference_groups=%d connections=%s\n",
				id, active, lastSeen, processes, captureGroups, captureGroupsDetail, activeCaptureStreams, inferenceGroups, connectionsDetail)
			if !*showProcesses {
				continue
			}
			procItems, _ := it["processes"].([]any)
			for _, pRaw := range procItems {
				p, ok := pRaw.(map[string]any)
				if !ok {
					continue
				}
				streamIDs := make([]string, 0, 4)
				if sid, ok := p["stream_id"]; ok {
					streamIDs = append(streamIDs, fmt.Sprint(sid))
				}
				if arr, ok := p["active_capture_stream_ids"].([]any); ok {
					for _, v := range arr {
						idStr := strings.TrimSpace(fmt.Sprint(v))
						if idStr == "" {
							continue
						}
						streamIDs = append(streamIDs, idStr)
					}
				}
				streamIDs = uniqueNonEmpty(streamIDs)
				streamLinks := make([]string, 0, len(streamIDs))
				for _, sid := range streamIDs {
					streamLinks = append(streamLinks, "/dashboard/stream/"+sid)
				}
				fmt.Printf("  process=%v source=%v kind=%v execution_class=%v pipeline=%v active=%v streams=%v links=%v\n",
					p["process_id"], p["source"], p["worker_kind"], p["execution_class"], p["pipeline_id"], p["active"], strings.Join(streamIDs, ","), strings.Join(streamLinks, ","))
			}
		}
	case "pipelines":
		fs := flag.NewFlagSet("pipelines list", flag.ExitOnError)
		backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
		apiToken := fs.String("api-token", cfg.APIToken, "backend API token")
		includeInactive := fs.Bool("include-inactive", true, "include inactive pipelines")
		asJSON := fs.Bool("json", false, "print JSON")
		_ = fs.Parse(args[1:])
		path := fmt.Sprintf("/api/v1/dashboard/pipelines/overview?include_inactive=%t", *includeInactive)
		payload := mustAPIGet(ctx, strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken), path)
		if *asJSON {
			printJSON(payload)
			return
		}
		items, _ := payload["items"].([]any)
		fmt.Printf("pipelines=%d backlog_frames_total=%v active_claims_total=%v\n", len(items), payload["backlog_frames_total"], payload["active_claims_total"])
		for _, raw := range items {
			it, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			fmt.Printf(
				"pipeline=%s kind=%s active=%v enabled_streams=%v recording_streams=%v backlog=%v active_claims=%v workers=%v throughput_1h=%v\n",
				it["pipeline_id"], it["kind"], it["active"], it["enabled_streams"], it["enabled_recording_streams"],
				it["backlog_frames"], it["active_claims"], it["active_workers"], it["throughput_1h"],
			)
		}
	case "queue-health":
		fs := flag.NewFlagSet("overview queue-health", flag.ExitOnError)
		backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
		apiToken := fs.String("api-token", cfg.APIToken, "backend API token")
		asJSON := fs.Bool("json", false, "print JSON")
		_ = fs.Parse(args[1:])
		payload := mustAPIGet(ctx, strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken), "/api/v1/dashboard/queue-health")
		if *asJSON {
			printJSON(payload)
			return
		}
		fmt.Printf(
			"recording_on=%v capture_sessions=%v capture_workers=%v inference_workers=%v inference_claims=%v backlog_frames=%v queued_boxed=%v box_pending=%v box_leased=%v box_error=%v pipelines=%v\n",
			payload["recording_on"],
			payload["capture_active_sessions"], payload["capture_active_workers"], payload["inference_active_workers"],
			payload["inference_active_claims"], payload["inference_backlog_frames"], payload["queued_boxed_results"],
			payload["box_jobs_pending"], payload["box_jobs_leased"], payload["box_jobs_error"], payload["pipeline_count"],
		)
	default:
		log.Fatalf("unknown overview surface subcommand: %s", sub)
	}
}

func mustOpenPool(ctx context.Context, cfg config.Config) *pgxpool.Pool {
	pool, err := db.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("open db: %v", err)
	}
	return pool
}

func importLegacySQLite(ctx context.Context, pool *pgxpool.Pool, sqlitePath string, defaultInterval int) (inserted int, updated int, skipped int, _ error) {
	sqliteDB, err := sql.Open("sqlite", sqlitePath)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("open sqlite %s: %w", sqlitePath, err)
	}
	defer sqliteDB.Close()

	query, err := buildLegacyStreamsQuery(sqliteDB)
	if err != nil {
		return 0, 0, 0, err
	}
	rows, err := sqliteDB.Query(query)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("query sqlite streams: %w", err)
	}
	defer rows.Close()

	tx, err := pool.Begin(ctx)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("begin postgres tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	for rows.Next() {
		var provider, externalID, name, slug, streamURL, sourcePageURL string
		var lat, lon sql.NullFloat64
		var locationText, metadataJSON string
		var shortlistFlag, excludedFlag int64
		if err := rows.Scan(&provider, &externalID, &name, &slug, &streamURL, &sourcePageURL, &lat, &lon, &locationText, &metadataJSON, &shortlistFlag, &excludedFlag); err != nil {
			return inserted, updated, skipped, fmt.Errorf("scan sqlite stream: %w", err)
		}
		provider = strings.TrimSpace(provider)
		externalID = strings.TrimSpace(externalID)
		name = strings.TrimSpace(name)
		slug = strings.TrimSpace(slug)
		streamURL = strings.TrimSpace(streamURL)
		if provider == "" || externalID == "" || name == "" || slug == "" || streamURL == "" {
			skipped++
			continue
		}

		meta := map[string]any{}
		if strings.TrimSpace(metadataJSON) != "" {
			if err := json.Unmarshal([]byte(metadataJSON), &meta); err != nil {
				meta = map[string]any{"legacy_metadata_parse_error": err.Error(), "legacy_metadata_raw": metadataJSON}
			}
		}
		metaBytes, err := json.Marshal(meta)
		if err != nil {
			return inserted, updated, skipped, fmt.Errorf("marshal metadata json: %w", err)
		}

		var latPtr any
		if lat.Valid {
			latPtr = lat.Float64
		}
		var lonPtr any
		if lon.Valid {
			lonPtr = lon.Float64
		}

		_ = excludedFlag
		recordingState := "off"
		if shortlistFlag != 0 {
			recordingState = "on"
		}
		cfgJSON := map[string]any{"poll_interval_sec": defaultInterval}
		cfgBytes, err := json.Marshal(cfgJSON)
		if err != nil {
			return inserted, updated, skipped, fmt.Errorf("marshal capture config: %w", err)
		}
		profile, err := capture.DeriveCaptureProfile(provider, streamURL, sourcePageURL, "", "", "", cfgJSON, nil, nil)
		if err != nil {
			return inserted, updated, skipped, fmt.Errorf("derive capture profile %s:%s: %w", provider, externalID, err)
		}
		ct, err := tx.Exec(ctx, `
				INSERT INTO streams (
					provider, external_id, name, slug, source_url, source_page_url,
					lat, lon, location_text, metadata_jsonb,
					recording_state, source_family, capture_type, execution_class, capture_family, expected_fps, expected_image_interval_sec, execution_config_jsonb, tags
				)
				VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18::jsonb,ARRAY[]::text[])
				ON CONFLICT (provider, external_id)
				DO UPDATE SET
					name=EXCLUDED.name,
					slug=EXCLUDED.slug,
					source_url=EXCLUDED.source_url,
					source_page_url=EXCLUDED.source_page_url,
					lat=EXCLUDED.lat,
					lon=EXCLUDED.lon,
					location_text=EXCLUDED.location_text,
					metadata_jsonb=EXCLUDED.metadata_jsonb,
					source_family=EXCLUDED.source_family,
					capture_type=EXCLUDED.capture_type,
					execution_class=EXCLUDED.execution_class,
					capture_family=EXCLUDED.capture_family,
					expected_fps=EXCLUDED.expected_fps,
					expected_image_interval_sec=EXCLUDED.expected_image_interval_sec,
					execution_config_jsonb=EXCLUDED.execution_config_jsonb,
					recording_state=EXCLUDED.recording_state,
					updated_at=now()
			`, provider, externalID, name, slug, profile.SourceURL, profile.SourcePageURL, latPtr, lonPtr, locationText, metaBytes, recordingState, profile.SourceFamily, profile.CaptureType, profile.ExecutionClass, profile.CaptureFamily, profile.ExpectedFPS, profile.ExpectedImageIntervalSec, cfgBytes)
		if err != nil {
			if strings.Contains(err.Error(), `duplicate key value violates unique constraint "streams_slug_key"`) {
				skipped++
				continue
			}
			return inserted, updated, skipped, fmt.Errorf("upsert stream %s:%s: %w", provider, externalID, err)
		}
		if ct.RowsAffected() == 1 {
			inserted++
		} else {
			updated++
		}
	}
	if rows.Err() != nil {
		return inserted, updated, skipped, fmt.Errorf("iterate sqlite streams: %w", rows.Err())
	}
	if err := tx.Commit(ctx); err != nil {
		return inserted, updated, skipped, fmt.Errorf("commit import tx: %w", err)
	}
	return inserted, updated, skipped, nil
}

func buildLegacyStreamsQuery(sqliteDB *sql.DB) (string, error) {
	cols, err := sqliteColumns(sqliteDB, "streams")
	if err != nil {
		return "", err
	}
	if _, ok := cols["provider"]; !ok {
		return "", fmt.Errorf("legacy sqlite missing streams.provider")
	}
	excludedExpr := "0"
	if _, ok := cols["excluded_from_campaign"]; ok {
		excludedExpr = "COALESCE(excluded_from_campaign, 0)"
	}
	query := fmt.Sprintf(`
		SELECT
			provider,
			external_id,
			name,
			slug,
			COALESCE(NULLIF(source_url, ''), image_url, ''),
			COALESCE(source_page_url, ''),
			lat,
			lon,
			COALESCE(location_text, ''),
			COALESCE(metadata_json, ''),
			COALESCE(shortlist_flag, 0),
			%s
		FROM streams
	`, excludedExpr)
	return query, nil
}

func sqliteColumns(db *sql.DB, table string) (map[string]struct{}, error) {
	rows, err := db.Query(fmt.Sprintf("PRAGMA table_info(%s)", table))
	if err != nil {
		return nil, fmt.Errorf("sqlite pragma table_info(%s): %w", table, err)
	}
	defer rows.Close()
	out := map[string]struct{}{}
	for rows.Next() {
		var cid int
		var name, colType string
		var notNull, pk int
		var defaultValue any
		if err := rows.Scan(&cid, &name, &colType, &notNull, &defaultValue, &pk); err != nil {
			return nil, fmt.Errorf("scan pragma table_info: %w", err)
		}
		out[strings.ToLower(strings.TrimSpace(name))] = struct{}{}
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	return out, nil
}

func loadStreamIDBySlug(ctx context.Context, pool *pgxpool.Pool) (map[string]int64, error) {
	rows, err := pool.Query(ctx, `SELECT id, slug FROM streams`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int64{}
	for rows.Next() {
		var id int64
		var slug string
		if err := rows.Scan(&id, &slug); err != nil {
			return nil, err
		}
		out[strings.TrimSpace(slug)] = id
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	return out, nil
}

func collectSnapshotFiles(root string) ([]string, error) {
	files := []string{}
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		name := strings.ToLower(d.Name())
		if strings.Contains(name, "_boxed") {
			return nil
		}
		ext := strings.ToLower(filepath.Ext(name))
		switch ext {
		case ".jpg", ".jpeg", ".png", ".webp":
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return files, nil
}

func inferStreamAndCaptureTime(root, path string, streamIDs map[string]int64) (int64, time.Time, bool) {
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return 0, time.Time{}, false
	}
	parts := strings.Split(filepath.ToSlash(rel), "/")
	if len(parts) < 2 {
		return 0, time.Time{}, false
	}
	var streamID int64
	var found bool
	for i := len(parts) - 2; i >= 0; i-- {
		candidate := strings.TrimSpace(parts[i])
		if candidate == "" {
			continue
		}
		if id, ok := streamIDs[candidate]; ok {
			streamID = id
			found = true
			break
		}
	}
	if !found {
		return 0, time.Time{}, false
	}
	base := strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
	capturedAt := parseCapturedAt(base)
	return streamID, capturedAt, true
}

func parseCapturedAt(base string) time.Time {
	candidate := strings.TrimSuffix(base, "_boxed")
	if len(candidate) >= len("20060102-150405") {
		candidate = candidate[:len("20060102-150405")]
	}
	if t, err := time.Parse("20060102-150405", candidate); err == nil {
		return t.UTC()
	}
	return time.Now().UTC()
}

func backfillOneSnapshot(ctx context.Context, pool *pgxpool.Pool, r2c *r2.Client, streamID int64, path string, capturedAt time.Time) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read snapshot %s: %w", path, err)
	}
	sum := sha256.Sum256(b)
	sha := hex.EncodeToString(sum[:])
	ext := strings.ToLower(filepath.Ext(path))
	mimeType := mime.TypeByExtension(ext)
	if mimeType == "" {
		mimeType = http.DetectContentType(b)
	}
	if !strings.HasPrefix(strings.ToLower(mimeType), "image/") {
		return fmt.Errorf("not an image content-type for %s: %s", path, mimeType)
	}
	width, height := imageDimensions(b)
	objectKey := fmt.Sprintf("raw/stream/%d/%04d/%02d/%02d/backfill-%s-%s",
		streamID,
		capturedAt.Year(), int(capturedAt.Month()), capturedAt.Day(),
		sha[:12],
		filepath.Base(path),
	)
	var existingMediaID int64
	err = pool.QueryRow(ctx, `
		SELECT id
		FROM media_objects
		WHERE bucket=$1 AND object_key=$2
	`, r2c.Bucket(), objectKey).Scan(&existingMediaID)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("query media object %s: %w", objectKey, err)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	mediaID := existingMediaID
	if mediaID == 0 {
		etag, putErr := r2c.PutBytes(ctx, objectKey, mimeType, b)
		if putErr != nil {
			return fmt.Errorf("upload %s: %w", path, putErr)
		}
		mediaID, err = storage.UpsertMediaObject(ctx, tx, storage.MediaObjectInput{
			StorageProvider: "r2",
			Bucket:          r2c.Bucket(),
			ObjectKey:       objectKey,
			MIMEType:        mimeType,
			SizeBytes:       int64(len(b)),
			ETag:            etag,
			SHA256:          sha,
			Width:           width,
			Height:          height,
		})
		if err != nil {
			return err
		}
	}

	ct, err := tx.Exec(ctx, `
		INSERT INTO frames (stream_id, capture_job_id, captured_at, raw_media_object_id, capture_status, capture_error, source_kind)
		VALUES ($1, NULL, $2, $3, 'success', NULL, 'snapshot_url')
		ON CONFLICT (stream_id, captured_at, raw_media_object_id) DO NOTHING
	`, streamID, capturedAt, mediaID)
	if err != nil {
		return fmt.Errorf("insert frame: %w", err)
	}
	if ct.RowsAffected() == 0 {
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit tx: %w", err)
		}
		return nil
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO stream_health (stream_id, captures_total, captures_success, captures_error, last_capture_at)
		VALUES ($1, 1, 1, 0, $2)
		ON CONFLICT (stream_id)
		DO UPDATE SET
			captures_total=stream_health.captures_total+1,
			captures_success=stream_health.captures_success+1,
			last_capture_at=GREATEST(stream_health.last_capture_at, EXCLUDED.last_capture_at),
			updated_at=now()
	`, streamID, capturedAt); err != nil {
		return fmt.Errorf("update stream_health: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}
	return nil
}

type inferenceBackfillStats struct {
	SampleInserted       int
	CampaignInserted     int
	DetectionsInserted   int
	BoxedUploaded        int
	SkippedMissingStream int
	SkippedMissingFrame  int
	SkippedExisting      int
	ParseErrors          int
}

type frameKey struct {
	StreamID   int64
	CapturedTS int64
}

func backfillLegacyInference(ctx context.Context, pool *pgxpool.Pool, r2c *r2.Client, sqlitePath string, pathPrefix string, limit int, dryRun bool) (inferenceBackfillStats, error) {
	stats := inferenceBackfillStats{}
	sqliteDB, err := sql.Open("sqlite", sqliteReadOnlyDSN(sqlitePath))
	if err != nil {
		return stats, fmt.Errorf("open sqlite %s: %w", sqlitePath, err)
	}
	defer sqliteDB.Close()

	legacyStreams, err := loadLegacyStreamMap(sqliteDB)
	if err != nil {
		return stats, err
	}
	newStreams, err := loadPostgresStreamMap(ctx, pool)
	if err != nil {
		return stats, err
	}
	legacyToNew := map[int64]int64{}
	for legacyID, key := range legacyStreams {
		if newID, ok := newStreams[key]; ok {
			legacyToNew[legacyID] = newID
		}
	}
	frameIdx, err := loadFrameIndex(ctx, pool)
	if err != nil {
		return stats, err
	}

	pipelineCache := map[string]bool{}

	if err := backfillPersonSampleRows(ctx, pool, sqliteDB, limit, dryRun, legacyToNew, frameIdx, pipelineCache, &stats); err != nil {
		return stats, err
	}
	if err := backfillCampaignSampleRows(ctx, pool, r2c, sqliteDB, pathPrefix, limit, dryRun, legacyToNew, frameIdx, pipelineCache, &stats); err != nil {
		return stats, err
	}
	return stats, nil
}

func backfillPersonSampleRows(
	ctx context.Context,
	pool *pgxpool.Pool,
	sqliteDB *sql.DB,
	limit int,
	dryRun bool,
	legacyToNew map[int64]int64,
	frameIdx map[frameKey]int64,
	pipelineCache map[string]bool,
	stats *inferenceBackfillStats,
) error {
	query := `
		SELECT
			id, run_id, stream_id, captured_at, status, error_message,
			person_count, max_bbox_area_px, max_bbox_ratio, max_confidence, bbox_json
		FROM person_sample
		ORDER BY id
	`
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}
	rows, err := sqliteDB.Query(query)
	if err != nil {
		return fmt.Errorf("query person_sample: %w", err)
	}
	defer rows.Close()

	processed := 0
	for rows.Next() {
		var rowID, runID, legacyStreamID int64
		var capturedAtRaw, statusRaw string
		var errMsg sql.NullString
		var personCount, maxArea sql.NullInt64
		var maxRatio, maxConf sql.NullFloat64
		var bboxJSON sql.NullString
		if err := rows.Scan(&rowID, &runID, &legacyStreamID, &capturedAtRaw, &statusRaw, &errMsg, &personCount, &maxArea, &maxRatio, &maxConf, &bboxJSON); err != nil {
			return fmt.Errorf("scan person_sample: %w", err)
		}
		processed++
		if processed%2000 == 0 {
			log.Printf("inference backfill person_sample progress: processed=%d inserted=%d skipped_existing=%d missing_stream=%d missing_frame=%d parse_errors=%d", processed, stats.SampleInserted, stats.SkippedExisting, stats.SkippedMissingStream, stats.SkippedMissingFrame, stats.ParseErrors)
		}

		streamID, ok := legacyToNew[legacyStreamID]
		if !ok {
			stats.SkippedMissingStream++
			continue
		}
		capturedAt, err := parseLegacyTime(capturedAtRaw)
		if err != nil {
			stats.ParseErrors++
			continue
		}
		frameID, ok := findFrameID(frameIdx, streamID, capturedAt)
		if !ok {
			stats.SkippedMissingFrame++
			continue
		}
		pipelineID := fmt.Sprintf("legacy/person_run/%d", runID)
		spec := map[string]any{
			"engine":       "legacy_backfill",
			"source_table": "person_sample",
			"run_id":       runID,
		}
		if err := ensurePipeline(ctx, pool, pipelineCache, pipelineID, spec, dryRun); err != nil {
			return err
		}

		status := "success"
		if strings.EqualFold(strings.TrimSpace(statusRaw), "error") {
			status = "error"
		}
		summary := map[string]any{
			"legacy_table":     "person_sample",
			"legacy_row_id":    rowID,
			"legacy_status":    strings.TrimSpace(statusRaw),
			"person_count":     nullInt64(personCount),
			"max_bbox_area_px": nullInt64(maxArea),
			"max_bbox_ratio":   nullFloat64(maxRatio),
			"max_confidence":   nullFloat64(maxConf),
		}
		rawOutput := map[string]any{
			"legacy_table":  "person_sample",
			"legacy_row_id": rowID,
			"legacy_status": strings.TrimSpace(statusRaw),
			"bbox_json_raw": nullString(bboxJSON),
		}
		runner := map[string]any{
			"source":           "legacy-backfill",
			"legacy_table":     "person_sample",
			"legacy_run_id":    runID,
			"legacy_stream_id": legacyStreamID,
		}
		resultID, inserted, err := insertLegacyInferenceResult(ctx, pool, pipelineID, frameID, status, summary, rawOutput, runner, capturedAt, errMsg, nil, dryRun)
		if err != nil {
			return err
		}
		if !inserted {
			stats.SkippedExisting++
			continue
		}
		stats.SampleInserted++
		if status != "success" || dryRun {
			continue
		}
		dets, parseErr := parseLegacyDetections(nullString(bboxJSON))
		if parseErr != nil {
			stats.ParseErrors++
			continue
		}
		n, err := insertDetections(ctx, pool, resultID, dets, dryRun)
		if err != nil {
			return err
		}
		stats.DetectionsInserted += n
	}
	if rows.Err() != nil {
		return fmt.Errorf("iterate person_sample: %w", rows.Err())
	}
	return nil
}

func backfillCampaignSampleRows(
	ctx context.Context,
	pool *pgxpool.Pool,
	r2c *r2.Client,
	sqliteDB *sql.DB,
	pathPrefix string,
	limit int,
	dryRun bool,
	legacyToNew map[int64]int64,
	frameIdx map[frameKey]int64,
	pipelineCache map[string]bool,
	stats *inferenceBackfillStats,
) error {
	query := `
		SELECT
			id, campaign_id, round_id, stream_id, captured_at, detect_status,
			capture_status, capture_error, person_count, max_bbox_area_px,
			max_bbox_ratio, max_confidence, bbox_json, boxed_frame_path
		FROM person_campaign_sample
		WHERE detect_status IN ('done', 'error')
		ORDER BY id
	`
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}
	rows, err := sqliteDB.Query(query)
	if err != nil {
		return fmt.Errorf("query person_campaign_sample: %w", err)
	}
	defer rows.Close()

	processed := 0
	for rows.Next() {
		var rowID, campaignID, roundID, legacyStreamID int64
		var capturedAtRaw, detectStatusRaw, captureStatusRaw string
		var captureErr, bboxJSON, boxedPath sql.NullString
		var personCount, maxArea sql.NullInt64
		var maxRatio, maxConf sql.NullFloat64
		if err := rows.Scan(
			&rowID, &campaignID, &roundID, &legacyStreamID, &capturedAtRaw, &detectStatusRaw,
			&captureStatusRaw, &captureErr, &personCount, &maxArea, &maxRatio, &maxConf, &bboxJSON, &boxedPath,
		); err != nil {
			return fmt.Errorf("scan person_campaign_sample: %w", err)
		}
		processed++
		if processed%5000 == 0 {
			log.Printf("inference backfill campaign_sample progress: processed=%d inserted=%d boxed_uploaded=%d skipped_existing=%d missing_stream=%d missing_frame=%d parse_errors=%d", processed, stats.CampaignInserted, stats.BoxedUploaded, stats.SkippedExisting, stats.SkippedMissingStream, stats.SkippedMissingFrame, stats.ParseErrors)
		}

		streamID, ok := legacyToNew[legacyStreamID]
		if !ok {
			stats.SkippedMissingStream++
			continue
		}
		capturedAt, err := parseLegacyTime(capturedAtRaw)
		if err != nil {
			stats.ParseErrors++
			continue
		}
		frameID, ok := findFrameID(frameIdx, streamID, capturedAt)
		if !ok {
			stats.SkippedMissingFrame++
			continue
		}
		pipelineID := fmt.Sprintf("legacy/campaign/%d", campaignID)
		spec := map[string]any{
			"engine":       "legacy_backfill",
			"source_table": "person_campaign_sample",
			"campaign_id":  campaignID,
		}
		if err := ensurePipeline(ctx, pool, pipelineCache, pipelineID, spec, dryRun); err != nil {
			return err
		}

		status := "success"
		if !strings.EqualFold(strings.TrimSpace(detectStatusRaw), "done") {
			status = "error"
		}
		var boxedMediaID *int64
		if status == "success" && strings.TrimSpace(nullString(boxedPath)) != "" {
			mediaID, uploaded, err := uploadLegacyBoxed(ctx, pool, r2c, pathPrefix, pipelineID, streamID, rowID, capturedAt, nullString(boxedPath), dryRun)
			if err != nil {
				stats.ParseErrors++
			} else {
				boxedMediaID = mediaID
				if uploaded {
					stats.BoxedUploaded++
				}
			}
		}

		summary := map[string]any{
			"legacy_table":          "person_campaign_sample",
			"legacy_row_id":         rowID,
			"legacy_detect_status":  strings.TrimSpace(detectStatusRaw),
			"legacy_capture_status": strings.TrimSpace(captureStatusRaw),
			"person_count":          nullInt64(personCount),
			"max_bbox_area_px":      nullInt64(maxArea),
			"max_bbox_ratio":        nullFloat64(maxRatio),
			"max_confidence":        nullFloat64(maxConf),
		}
		rawOutput := map[string]any{
			"legacy_table":          "person_campaign_sample",
			"legacy_row_id":         rowID,
			"legacy_detect_status":  strings.TrimSpace(detectStatusRaw),
			"legacy_capture_status": strings.TrimSpace(captureStatusRaw),
			"bbox_json_raw":         nullString(bboxJSON),
			"boxed_frame_path":      nullString(boxedPath),
		}
		runner := map[string]any{
			"source":             "legacy-backfill",
			"legacy_table":       "person_campaign_sample",
			"legacy_campaign_id": campaignID,
			"legacy_round_id":    roundID,
			"legacy_stream_id":   legacyStreamID,
		}
		resultID, inserted, err := insertLegacyInferenceResult(ctx, pool, pipelineID, frameID, status, summary, rawOutput, runner, capturedAt, captureErr, boxedMediaID, dryRun)
		if err != nil {
			return err
		}
		if !inserted {
			stats.SkippedExisting++
			continue
		}
		stats.CampaignInserted++
		if status != "success" || dryRun {
			continue
		}
		dets, parseErr := parseLegacyDetections(nullString(bboxJSON))
		if parseErr != nil {
			stats.ParseErrors++
			continue
		}
		n, err := insertDetections(ctx, pool, resultID, dets, dryRun)
		if err != nil {
			return err
		}
		stats.DetectionsInserted += n
	}
	if rows.Err() != nil {
		return fmt.Errorf("iterate person_campaign_sample: %w", rows.Err())
	}
	return nil
}

func insertLegacyInferenceResult(
	ctx context.Context,
	pool *pgxpool.Pool,
	pipelineID string,
	frameID int64,
	status string,
	summary map[string]any,
	rawOutput map[string]any,
	runner map[string]any,
	capturedAt time.Time,
	errMsg sql.NullString,
	boxedMediaID *int64,
	dryRun bool,
) (resultID int64, inserted bool, _ error) {
	if dryRun {
		return 0, true, nil
	}
	summaryBytes, err := json.Marshal(nonNilMap(summary))
	if err != nil {
		return 0, false, fmt.Errorf("marshal summary: %w", err)
	}
	rawBytes, err := json.Marshal(nonNilMap(rawOutput))
	if err != nil {
		return 0, false, fmt.Errorf("marshal raw output: %w", err)
	}
	runnerBytes, err := json.Marshal(nonNilMap(runner))
	if err != nil {
		return 0, false, fmt.Errorf("marshal runner info: %w", err)
	}
	var errorText any
	if strings.TrimSpace(errMsg.String) != "" {
		errorText = strings.TrimSpace(errMsg.String)
	}
	err = pool.QueryRow(ctx, `
		INSERT INTO inference_results (
			pipeline_id, frame_id, revision, status, summary_jsonb, boxed_media_object_id,
			raw_output_jsonb, error_text, runner_info_jsonb, started_at, finished_at
		)
		VALUES ($1,$2,1,$3,$4,$5,$6,$7,$8,$9,$10)
		ON CONFLICT (pipeline_id, frame_id, revision) DO NOTHING
		RETURNING id
	`, pipelineID, frameID, status, summaryBytes, boxedMediaID, rawBytes, errorText, runnerBytes, capturedAt, capturedAt).Scan(&resultID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("insert inference result pipeline=%s frame=%d: %w", pipelineID, frameID, err)
	}
	return resultID, true, nil
}

type legacyDetection struct {
	ClassID    *string
	ClassName  string
	Confidence float64
	X1         float64
	Y1         float64
	X2         float64
	Y2         float64
	AreaPx     float64
	Extra      map[string]any
}

func parseLegacyDetections(raw string) ([]legacyDetection, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "[]" {
		return []legacyDetection{}, nil
	}
	var arr []map[string]any
	if err := json.Unmarshal([]byte(raw), &arr); err != nil {
		return nil, err
	}
	out := make([]legacyDetection, 0, len(arr))
	for _, item := range arr {
		xyAny, ok := item["xyxy"]
		if !ok {
			continue
		}
		xyVals, ok := xyAny.([]any)
		if !ok || len(xyVals) < 4 {
			continue
		}
		x1, ok1 := asFloat64(xyVals[0])
		y1, ok2 := asFloat64(xyVals[1])
		x2, ok3 := asFloat64(xyVals[2])
		y2, ok4 := asFloat64(xyVals[3])
		if !ok1 || !ok2 || !ok3 || !ok4 {
			continue
		}
		conf, ok := asFloat64(item["confidence"])
		if !ok {
			conf = 0
		}
		area, ok := asFloat64(item["area_px"])
		if !ok || area <= 0 {
			area = (x2 - x1) * (y2 - y1)
			if area < 0 {
				area = 0
			}
		}
		classID := "person"
		out = append(out, legacyDetection{
			ClassID:    &classID,
			ClassName:  "person",
			Confidence: conf,
			X1:         x1,
			Y1:         y1,
			X2:         x2,
			Y2:         y2,
			AreaPx:     area,
			Extra:      map[string]any{"legacy": item},
		})
	}
	return out, nil
}

func insertDetections(ctx context.Context, pool *pgxpool.Pool, inferenceResultID int64, dets []legacyDetection, dryRun bool) (int, error) {
	if dryRun || len(dets) == 0 {
		return 0, nil
	}
	inserted := 0
	for _, d := range dets {
		extraBytes, err := json.Marshal(nonNilMap(d.Extra))
		if err != nil {
			return inserted, fmt.Errorf("marshal detection extra: %w", err)
		}
		if _, err := pool.Exec(ctx, `
			INSERT INTO detections (
				inference_result_id, class_id, class_name, confidence, x1, y1, x2, y2, area_px, extra_jsonb
			)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
		`, inferenceResultID, d.ClassID, d.ClassName, d.Confidence, d.X1, d.Y1, d.X2, d.Y2, d.AreaPx, extraBytes); err != nil {
			return inserted, fmt.Errorf("insert detection result_id=%d: %w", inferenceResultID, err)
		}
		inserted++
	}
	return inserted, nil
}

func uploadLegacyBoxed(
	ctx context.Context,
	pool *pgxpool.Pool,
	r2c *r2.Client,
	pathPrefix string,
	pipelineID string,
	streamID int64,
	legacyRowID int64,
	capturedAt time.Time,
	boxedPath string,
	dryRun bool,
) (*int64, bool, error) {
	if dryRun || r2c == nil {
		return nil, false, nil
	}
	resolvedPath, ok := resolveLegacyPath(pathPrefix, boxedPath)
	if !ok {
		return nil, false, fmt.Errorf("boxed file not found: %s", boxedPath)
	}
	b, err := os.ReadFile(resolvedPath)
	if err != nil {
		return nil, false, fmt.Errorf("read boxed file %s: %w", resolvedPath, err)
	}
	sum := sha256.Sum256(b)
	sha := hex.EncodeToString(sum[:])
	ext := strings.ToLower(filepath.Ext(resolvedPath))
	mimeType := mime.TypeByExtension(ext)
	if mimeType == "" {
		mimeType = http.DetectContentType(b)
	}
	if !strings.HasPrefix(strings.ToLower(mimeType), "image/") {
		return nil, false, fmt.Errorf("boxed file is not image: %s", resolvedPath)
	}
	width, height := imageDimensions(b)
	pipelineToken := sanitizeObjectToken(pipelineID)
	objectKey := fmt.Sprintf(
		"boxed/pipeline/%s/stream/%d/%04d/%02d/%02d/legacy-%d-%s%s",
		pipelineToken, streamID, capturedAt.Year(), int(capturedAt.Month()), capturedAt.Day(), legacyRowID, sha[:12], ext,
	)
	var existingID int64
	err = pool.QueryRow(ctx, `SELECT id FROM media_objects WHERE bucket=$1 AND object_key=$2`, r2c.Bucket(), objectKey).Scan(&existingID)
	if err == nil {
		return &existingID, false, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, false, fmt.Errorf("query media object %s: %w", objectKey, err)
	}
	etag, err := r2c.PutBytes(ctx, objectKey, mimeType, b)
	if err != nil {
		return nil, false, fmt.Errorf("upload boxed image %s: %w", resolvedPath, err)
	}
	mediaID, err := storage.UpsertMediaObject(ctx, pool, storage.MediaObjectInput{
		StorageProvider: "r2",
		Bucket:          r2c.Bucket(),
		ObjectKey:       objectKey,
		MIMEType:        mimeType,
		SizeBytes:       int64(len(b)),
		ETag:            etag,
		SHA256:          sha,
		Width:           width,
		Height:          height,
	})
	if err != nil {
		return nil, false, err
	}
	return &mediaID, true, nil
}

func resolveLegacyPath(pathPrefix string, legacyPath string) (string, bool) {
	p := strings.TrimSpace(legacyPath)
	if p == "" {
		return "", false
	}
	candidates := []string{p}
	if pathPrefix != "" {
		candidates = append(candidates, filepath.Join(pathPrefix, p))
	}
	candidates = append(candidates, filepath.Join("..", p))
	for _, c := range candidates {
		if fi, err := os.Stat(c); err == nil && !fi.IsDir() {
			return c, true
		}
	}
	return "", false
}

func sanitizeObjectToken(v string) string {
	v = strings.TrimSpace(strings.ToLower(v))
	if v == "" {
		return "unknown"
	}
	var b strings.Builder
	for _, r := range v {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		default:
			b.WriteRune('_')
		}
	}
	out := strings.Trim(b.String(), "_")
	if out == "" {
		return "unknown"
	}
	return out
}

func loadLegacyStreamMap(sqliteDB *sql.DB) (map[int64]string, error) {
	rows, err := sqliteDB.Query(`SELECT id, provider, external_id FROM streams`)
	if err != nil {
		return nil, fmt.Errorf("query legacy streams: %w", err)
	}
	defer rows.Close()
	out := map[int64]string{}
	for rows.Next() {
		var id int64
		var provider, externalID string
		if err := rows.Scan(&id, &provider, &externalID); err != nil {
			return nil, fmt.Errorf("scan legacy stream: %w", err)
		}
		out[id] = streamIdentityKey(provider, externalID)
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("iterate legacy streams: %w", rows.Err())
	}
	return out, nil
}

func loadPostgresStreamMap(ctx context.Context, pool *pgxpool.Pool) (map[string]int64, error) {
	rows, err := pool.Query(ctx, `SELECT id, provider, external_id FROM streams`)
	if err != nil {
		return nil, fmt.Errorf("query postgres streams: %w", err)
	}
	defer rows.Close()
	out := map[string]int64{}
	for rows.Next() {
		var id int64
		var provider, externalID string
		if err := rows.Scan(&id, &provider, &externalID); err != nil {
			return nil, fmt.Errorf("scan postgres stream: %w", err)
		}
		out[streamIdentityKey(provider, externalID)] = id
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("iterate postgres streams: %w", rows.Err())
	}
	return out, nil
}

func loadFrameIndex(ctx context.Context, pool *pgxpool.Pool) (map[frameKey]int64, error) {
	rows, err := pool.Query(ctx, `SELECT id, stream_id, EXTRACT(EPOCH FROM captured_at)::bigint FROM frames`)
	if err != nil {
		return nil, fmt.Errorf("query frame index: %w", err)
	}
	defer rows.Close()
	out := map[frameKey]int64{}
	for rows.Next() {
		var id, streamID, ts int64
		if err := rows.Scan(&id, &streamID, &ts); err != nil {
			return nil, fmt.Errorf("scan frame index: %w", err)
		}
		k := frameKey{StreamID: streamID, CapturedTS: ts}
		if prev, ok := out[k]; !ok || id > prev {
			out[k] = id
		}
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("iterate frame index: %w", rows.Err())
	}
	return out, nil
}

func findFrameID(frameIdx map[frameKey]int64, streamID int64, capturedAt time.Time) (int64, bool) {
	base := capturedAt.Unix()
	offsets := []int64{0, -5 * 3600, 5 * 3600}
	for _, off := range offsets {
		ts := base + off
		for delta := int64(-120); delta <= 120; delta++ {
			if id, ok := frameIdx[frameKey{StreamID: streamID, CapturedTS: ts + delta}]; ok {
				return id, true
			}
		}
	}
	return 0, false
}

func ensurePipeline(ctx context.Context, pool *pgxpool.Pool, cache map[string]bool, pipelineID string, spec map[string]any, dryRun bool) error {
	if cache[pipelineID] {
		return nil
	}
	cache[pipelineID] = true
	if dryRun {
		return nil
	}
	specBytes, err := json.Marshal(nonNilMap(spec))
	if err != nil {
		return fmt.Errorf("marshal pipeline spec %s: %w", pipelineID, err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO pipelines (id, kind, spec_jsonb, active)
		VALUES ($1, 'detector', $2, true)
		ON CONFLICT (id)
		DO UPDATE SET
			spec_jsonb=EXCLUDED.spec_jsonb,
			active=true,
			updated_at=now()
	`, pipelineID, specBytes); err != nil {
		return fmt.Errorf("upsert pipeline %s: %w", pipelineID, err)
	}
	return nil
}

func sqliteReadOnlyDSN(path string) string {
	if strings.HasPrefix(strings.TrimSpace(path), "file:") {
		return path
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		abs = path
	}
	return fmt.Sprintf("file:%s?mode=ro&immutable=1", filepath.ToSlash(abs))
}

func streamIdentityKey(provider string, externalID string) string {
	return strings.TrimSpace(provider) + "\x1f" + strings.TrimSpace(externalID)
}

func parseLegacyTime(v string) (time.Time, error) {
	v = strings.TrimSpace(v)
	if v == "" {
		return time.Time{}, fmt.Errorf("empty timestamp")
	}
	layouts := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05",
	}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, v); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("unsupported timestamp format: %s", v)
}

func nullInt64(v sql.NullInt64) int64 {
	if !v.Valid {
		return 0
	}
	return v.Int64
}

func nullFloat64(v sql.NullFloat64) float64 {
	if !v.Valid {
		return 0
	}
	return v.Float64
}

func nullString(v sql.NullString) string {
	if !v.Valid {
		return ""
	}
	return v.String
}

func asFloat64(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case float32:
		return float64(x), true
	case int:
		return float64(x), true
	case int64:
		return float64(x), true
	case int32:
		return float64(x), true
	case json.Number:
		f, err := x.Float64()
		return f, err == nil
	default:
		return 0, false
	}
}

func mapAny(v any) map[string]any {
	if v == nil {
		return nil
	}
	m, ok := v.(map[string]any)
	if !ok {
		return nil
	}
	return m
}

func stringAny(v any) string {
	if v == nil {
		return ""
	}
	s := strings.TrimSpace(fmt.Sprint(v))
	if s == "<nil>" {
		return ""
	}
	return s
}

func summarizeServerConnectionsDashboard(item map[string]any) string {
	if item == nil {
		return "-"
	}
	entries := make([]string, 0, 8)
	seen := map[string]struct{}{}
	addMeta := func(meta map[string]any) {
		if meta == nil {
			return
		}
		transport := stringAny(meta["network_transport"])
		topologyID := stringAny(meta["topology_id"])
		if topologyID == "" {
			topologyID = stringAny(meta["hub_server_id"])
		}
		role := stringAny(meta["topology_role"])
		hub := stringAny(meta["hub_server_id"])
		wgIface := stringAny(meta["wg_interface"])
		wgIP := stringAny(meta["wg_ip"])
		sourceEndpoint := stringAny(meta["source_endpoint"])
		if transport == "" && topologyID == "" && role == "" && hub == "" && wgIface == "" && wgIP == "" && sourceEndpoint == "" {
			return
		}
		parts := make([]string, 0, 8)
		if transport != "" {
			parts = append(parts, transport)
		}
		if topologyID != "" {
			parts = append(parts, "topology="+topologyID)
		}
		if role != "" {
			parts = append(parts, "role="+role)
		}
		if hub != "" {
			parts = append(parts, "hub="+hub)
		}
		if wgIface != "" || wgIP != "" {
			switch {
			case wgIface != "" && wgIP != "":
				parts = append(parts, "wg="+wgIface+"@"+wgIP)
			case wgIface != "":
				parts = append(parts, "wg="+wgIface)
			default:
				parts = append(parts, "wg_ip="+wgIP)
			}
		}
		if sourceEndpoint != "" {
			parts = append(parts, "source_endpoint="+sourceEndpoint)
		}
		entry := strings.Join(parts, " ")
		if entry == "" {
			return
		}
		if _, ok := seen[entry]; ok {
			return
		}
		seen[entry] = struct{}{}
		entries = append(entries, entry)
	}

	if executionClasses, ok := item["execution_classes"].([]any); ok {
		for _, raw := range executionClasses {
			executionClassItem := mapAny(raw)
			if executionClassItem == nil {
				continue
			}
			addMeta(mapAny(executionClassItem["metadata_json"]))
		}
	}
	if procs, ok := item["processes"].([]any); ok {
		for _, raw := range procs {
			proc := mapAny(raw)
			if proc == nil {
				continue
			}
			addMeta(mapAny(proc["metadata_json"]))
		}
	}
	if workers, ok := item["processing_workers"].([]any); ok {
		for _, raw := range workers {
			worker := mapAny(raw)
			if worker == nil {
				continue
			}
			addMeta(mapAny(worker["metadata_json"]))
		}
	}
	if len(entries) == 0 {
		return "-"
	}
	sort.Strings(entries)
	return strings.Join(entries, " | ")
}

func nonNilMap(m map[string]any) map[string]any {
	if m == nil {
		return map[string]any{}
	}
	return m
}

func cloneMap(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func printJSON(v any) {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		log.Fatalf("encode json output: %v", err)
	}
}

func envInt(key string, def int) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return def
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return def
	}
	return n
}

func envBool(key string, def bool) bool {
	raw := strings.TrimSpace(strings.ToLower(os.Getenv(key)))
	if raw == "" {
		return def
	}
	switch raw {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return def
	}
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func printStreamDetail(ctx context.Context, pool *pgxpool.Pool, streamID int64, pipelineID string, resultsLimit int, detectionsLimit int) {
	type streamDetail struct {
		ID             int64
		Provider       string
		ExternalID     string
		Name           string
		Slug           string
		SourceURL      string
		SourcePageURL  string
		RecordingState string
		CaptureType    string
		LatestCaptured *time.Time
		LatestRawKey   *string
	}
	var s streamDetail
	if err := pool.QueryRow(ctx, `
		SELECT
			s.id, s.provider, s.external_id, s.name, s.slug, s.source_url, s.source_page_url,
			s.recording_state, s.capture_type,
			lf.captured_at, mo.object_key
		FROM streams s
		LEFT JOIN LATERAL (
			SELECT f.captured_at, f.raw_media_object_id
			FROM frames f
			WHERE f.stream_id=s.id
			ORDER BY f.captured_at DESC, f.id DESC
			LIMIT 1
		) lf ON true
		LEFT JOIN media_objects mo ON mo.id=lf.raw_media_object_id
		WHERE s.id=$1
	`, streamID).Scan(
		&s.ID, &s.Provider, &s.ExternalID, &s.Name, &s.Slug, &s.SourceURL, &s.SourcePageURL,
		&s.RecordingState, &s.CaptureType,
		&s.LatestCaptured, &s.LatestRawKey,
	); err != nil {
		log.Fatalf("load stream detail: %v", err)
	}

	fmt.Printf("stream id=%d slug=%s provider=%s external_id=%s\n", s.ID, s.Slug, s.Provider, s.ExternalID)
	fmt.Printf("name=%q\n", s.Name)
	fmt.Printf("recording_state=%s capture_type=%s\n", s.RecordingState, s.CaptureType)
	fmt.Printf("source_url=%s\n", s.SourceURL)
	fmt.Printf("source_page_url=%s\n", s.SourcePageURL)
	if s.LatestCaptured != nil {
		fmt.Printf("latest_raw_frame captured_at=%s object_key=%s\n", s.LatestCaptured.Format(time.RFC3339), derefString(s.LatestRawKey))
	} else {
		fmt.Println("latest_raw_frame captured_at=- object_key=-")
	}

	where := "f.stream_id=$1"
	args := []any{s.ID, resultsLimit}
	if pipelineID != "" {
		args = append([]any{s.ID, pipelineID, resultsLimit}, args[2:]...)
		where = "f.stream_id=$1 AND ir.pipeline_id=$2"
	}

	rows, err := pool.Query(ctx, fmt.Sprintf(`
		SELECT
			ir.id, ir.pipeline_id, ir.revision, ir.status, ir.created_at, ir.finished_at,
			ir.summary_jsonb, ir.error_text, boxed.object_key, raw.object_key
		FROM inference_results ir
		JOIN frames f ON f.id=ir.frame_id
		LEFT JOIN media_objects boxed ON boxed.id=ir.boxed_media_object_id
		LEFT JOIN media_objects raw ON raw.id=f.raw_media_object_id
		WHERE %s
		ORDER BY ir.created_at DESC, ir.id DESC
		LIMIT $%d
	`, where, len(args)), args...)
	if err != nil {
		log.Fatalf("query latest inference results: %v", err)
	}
	defer rows.Close()

	type resultRow struct {
		ID        int64
		Pipeline  string
		Revision  int
		Status    string
		CreatedAt time.Time
		Finished  *time.Time
		Summary   []byte
		ErrorText *string
		BoxedKey  *string
		RawKey    *string
	}
	results := make([]resultRow, 0, resultsLimit)
	for rows.Next() {
		var r resultRow
		if err := rows.Scan(&r.ID, &r.Pipeline, &r.Revision, &r.Status, &r.CreatedAt, &r.Finished, &r.Summary, &r.ErrorText, &r.BoxedKey, &r.RawKey); err != nil {
			log.Fatalf("scan latest inference result: %v", err)
		}
		results = append(results, r)
	}
	if rows.Err() != nil {
		log.Fatalf("iterate latest inference results: %v", rows.Err())
	}

	fmt.Printf("\nlatest inference results (limit=%d", resultsLimit)
	if pipelineID != "" {
		fmt.Printf(", pipeline=%s", pipelineID)
	}
	fmt.Println("):")
	if len(results) == 0 {
		fmt.Println("  - none")
	} else {
		for _, r := range results {
			summary := "{}"
			if len(r.Summary) > 0 {
				summary = string(r.Summary)
			}
			if len(summary) > 180 {
				summary = summary[:180] + "..."
			}
			fmt.Printf("  - id=%d pipeline=%s rev=%d status=%s created_at=%s finished_at=%s boxed=%s raw=%s error=%s summary=%s\n",
				r.ID, r.Pipeline, r.Revision, r.Status, r.CreatedAt.Format(time.RFC3339),
				formatTimePtr(r.Finished), derefString(r.BoxedKey), derefString(r.RawKey), derefString(r.ErrorText), summary)
		}
	}

	var targetResultID int64
	err = pool.QueryRow(ctx, fmt.Sprintf(`
		SELECT ir.id
		FROM inference_results ir
		JOIN frames f ON f.id=ir.frame_id
		WHERE %s
		ORDER BY ir.created_at DESC, ir.id DESC
		LIMIT 1
	`, where), args[:len(args)-1]...).Scan(&targetResultID)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		log.Fatalf("select latest result for detections: %v", err)
	}
	if errors.Is(err, pgx.ErrNoRows) {
		fmt.Println("\nlatest detections: none (no inference results)")
		return
	}

	dRows, err := pool.Query(ctx, `
		SELECT class_name, confidence, x1, y1, x2, y2, area_px
		FROM detections
		WHERE inference_result_id=$1
		ORDER BY confidence DESC, id ASC
		LIMIT $2
	`, targetResultID, detectionsLimit)
	if err != nil {
		log.Fatalf("query detections: %v", err)
	}
	defer dRows.Close()

	fmt.Printf("\nlatest detections for inference_result_id=%d (limit=%d):\n", targetResultID, detectionsLimit)
	count := 0
	for dRows.Next() {
		var className string
		var conf, x1, y1, x2, y2, area float64
		if err := dRows.Scan(&className, &conf, &x1, &y1, &x2, &y2, &area); err != nil {
			log.Fatalf("scan detection: %v", err)
		}
		count++
		fmt.Printf("  - class=%s conf=%.4f bbox=[%.1f,%.1f,%.1f,%.1f] area=%.1f\n", className, conf, x1, y1, x2, y2, area)
	}
	if err := dRows.Err(); err != nil {
		log.Fatalf("iterate detections: %v", err)
	}
	if count == 0 {
		fmt.Println("  - none")
	}
}

func formatTimePtr(v *time.Time) string {
	if v == nil {
		return "-"
	}
	return v.Format(time.RFC3339)
}

func imageDimensions(b []byte) (int, int) {
	cfg, _, err := image.DecodeConfig(bytes.NewReader(b))
	if err != nil {
		return 0, 0
	}
	if cfg.Width <= 0 || cfg.Height <= 0 {
		return 0, 0
	}
	return cfg.Width, cfg.Height
}

func splitCSV(v string) []string {
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		out = append(out, p)
	}
	return out
}

func normalizeTags(tags []string) []string {
	if len(tags) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(tags))
	out := make([]string, 0, len(tags))
	for _, tag := range tags {
		clean := strings.TrimSpace(tag)
		if clean == "" {
			continue
		}
		if _, ok := seen[clean]; ok {
			continue
		}
		seen[clean] = struct{}{}
		out = append(out, clean)
	}
	return out
}

func parseInt64CSV(v string) ([]int64, error) {
	parts := splitCSV(v)
	out := make([]int64, 0, len(parts))
	for _, p := range parts {
		n, err := strconv.ParseInt(strings.TrimSpace(p), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid int64 %q: %w", p, err)
		}
		if n <= 0 {
			return nil, fmt.Errorf("id must be > 0: %d", n)
		}
		out = append(out, n)
	}
	return out, nil
}

func defaultBackendAPIURL() string {
	if v := strings.TrimSpace(os.Getenv("BACKEND_API_URL")); v != "" {
		return v
	}
	return strings.TrimSpace(os.Getenv("INFERCTL_API_URL"))
}

func doMetadataValue(path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return ""
	}
	ctx, cancel := context.WithTimeout(context.Background(), 1200*time.Millisecond)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://169.254.169.254/metadata/v1/"+path, nil)
	if err != nil {
		return ""
	}
	resp, err := (&http.Client{Timeout: 1200 * time.Millisecond}).Do(req)
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return ""
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, 1024))
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

func mustResolveStreamIDBySlug(ctx context.Context, baseURL string, apiToken string, slug string) int64 {
	streamSlug := strings.TrimSpace(slug)
	if streamSlug == "" {
		log.Fatalf("--slug must not be empty")
	}
	for offset := 0; offset <= 10000; offset += 500 {
		q := url.Values{}
		q.Set("search", streamSlug)
		q.Set("limit", "500")
		q.Set("offset", strconv.Itoa(offset))
		q.Set("include_image_urls", "false")
		payload := mustAPIGet(ctx, baseURL, apiToken, "/api/v1/dashboard/streams?"+q.Encode())
		items, _ := payload["items"].([]any)
		if len(items) == 0 {
			break
		}
		for _, raw := range items {
			item := asMap(raw)
			stream := asMap(item["stream"])
			if strings.TrimSpace(fmt.Sprint(stream["slug"])) != streamSlug {
				continue
			}
			id := int64FromAny(stream["id"])
			if id > 0 {
				return id
			}
		}
	}
	log.Fatalf("stream not found for slug=%q", streamSlug)
	return 0
}

func asMap(v any) map[string]any {
	if v == nil {
		return map[string]any{}
	}
	m, ok := v.(map[string]any)
	if !ok {
		return map[string]any{}
	}
	return m
}

func asStringSlice(v any) []string {
	raw, ok := v.([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(raw))
	for _, it := range raw {
		s := strings.TrimSpace(fmt.Sprint(it))
		if s == "" || s == "<nil>" {
			continue
		}
		out = append(out, s)
	}
	return out
}

func int64FromAny(v any) int64 {
	if n, ok := asFloat64(v); ok {
		return int64(n)
	}
	return 0
}

func boolFromAny(v any) bool {
	switch x := v.(type) {
	case bool:
		return x
	case string:
		b, err := strconv.ParseBool(strings.TrimSpace(x))
		return err == nil && b
	case json.Number:
		n, err := x.Int64()
		return err == nil && n != 0
	default:
		if n, ok := asFloat64(v); ok {
			return n != 0
		}
		return false
	}
}

func oneLineJSON(v any) string {
	if v == nil {
		return "{}"
	}
	b, err := json.Marshal(v)
	if err != nil {
		return "{}"
	}
	return string(b)
}

func sanitizeFilename(v string) string {
	v = strings.TrimSpace(v)
	if v == "" {
		return "stream"
	}
	var b strings.Builder
	b.Grow(len(v))
	for i := 0; i < len(v); i++ {
		c := v[i]
		isAlphaNum := (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
		if isAlphaNum || c == '-' || c == '_' || c == '.' {
			b.WriteByte(c)
			continue
		}
		b.WriteByte('-')
	}
	out := strings.Trim(b.String(), "-.")
	if out == "" {
		return "stream"
	}
	return out
}

func derefString(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}

type optionalString struct {
	set   bool
	value string
}

func (o *optionalString) Set(v string) error {
	o.set = true
	o.value = v
	return nil
}

func (o *optionalString) String() string { return o.value }

func optionalStringFlag(fs *flag.FlagSet, name string, defaultValue string) *optionalString {
	o := &optionalString{value: defaultValue}
	fs.Var(o, name, "")
	return o
}

type optionalInt struct {
	set   bool
	value int
}

func (o *optionalInt) Set(v string) error {
	n, err := strconv.Atoi(v)
	if err != nil {
		return err
	}
	o.set = true
	o.value = n
	return nil
}

func (o *optionalInt) String() string { return strconv.Itoa(o.value) }

func optionalIntFlag(fs *flag.FlagSet, name string, defaultValue int) *optionalInt {
	o := &optionalInt{value: defaultValue}
	fs.Var(o, name, "")
	return o
}

type optionalBool struct {
	set   bool
	value bool
}

func (o *optionalBool) Set(v string) error {
	b, err := strconv.ParseBool(v)
	if err != nil {
		return err
	}
	o.set = true
	o.value = b
	return nil
}

func (o *optionalBool) String() string {
	if !o.set {
		return ""
	}
	if o.value {
		return "true"
	}
	return "false"
}

func optionalBoolFlag(fs *flag.FlagSet, name string) *optionalBool {
	o := &optionalBool{}
	fs.Var(o, name, "")
	return o
}

func uniqueNonEmpty(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	seen := map[string]struct{}{}
	out := make([]string, 0, len(in))
	for _, raw := range in {
		v := strings.TrimSpace(raw)
		if v == "" {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func check(err error) {
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			os.Exit(2)
		}
		log.Fatal(err)
	}
}
