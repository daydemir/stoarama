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
	"github.com/daydemir/stoarama/backend/internal/captureapi"
	"github.com/daydemir/stoarama/backend/internal/capturescheduled"
	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/db"
	"github.com/daydemir/stoarama/backend/internal/model"
	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/daydemir/stoarama/backend/internal/settings"
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
	case "capture-server":
		runCaptureServer(ctx, cfg, os.Args[2:])
	case "streams":
		runStreams(ctx, cfg, os.Args[2:])
	case "discovery":
		runDiscovery(ctx, cfg, os.Args[2:])
	case "media":
		runMedia(ctx, cfg, os.Args[2:])
	case "inference":
		runInference(ctx, cfg, os.Args[2:])
	case "jobs":
		runJobs(ctx, cfg, os.Args[2:])
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
	case "recording":
		runRecording(ctx, cfg, os.Args[2:])
	case "servers":
		runServers(ctx, cfg, os.Args[2:])
	case "archive":
		runArchive(ctx, cfg, os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	_, _ = os.Stdout.WriteString(`stoaramactl commands:
	  stoaramactl migrate up [--dir infra/sql/migrations]
	  stoaramactl capture backfill-missing [--backend-api-url URL --api-token TOKEN --limit 0 --concurrency 4 --timeout-sec 90 --dry-run --json]
	  stoaramactl capture-server run [--backend-api-url URL --api-token TOKEN --server-id ID --worker-id ID --capture-shared-capacity 6 --stream-ids 1,2 --heartbeat-sec 15 --lease-sec 45 --refresh-sec 5 --metadata-json JSON --duration 0]
	  stoaramactl capture probe (--id N | --provider P --source-url URL) [--source-page-url URL --capture-type TYPE --capture-timeout-sec 60]
	  stoaramactl capture audit --all [--concurrency 16 --timeout-sec 20 --json]
	  stoaramactl capture runtime list [--status running|unsupported|error] [--limit 200] [--json]
	  stoaramactl capture runtime show --id N [--json]
	  stoaramactl capture runtime reset --id N
	  stoaramactl archive mp4-to-glacier bucket-create [--aws-profile personal --aws-bucket stoarama-archives --apply]
	  stoaramactl archive mp4-to-glacier manifest --out manifest.jsonl [--aws-bucket stoarama-archives --limit 0]
	  stoaramactl archive mp4-to-glacier copy --manifest manifest.jsonl [--aws-profile personal --apply]
	  stoaramactl archive mp4-to-glacier verify --manifest manifest.jsonl [--aws-profile personal --apply]
	  stoaramactl archive mp4-to-glacier delete-r2 --manifest manifest.jsonl [--apply]
	  stoaramactl archive mp4-to-glacier status [--json]
	  stoaramactl streams list [--recording-state off|on --capture-type TYPE --tags a,b --limit 200]
	  stoaramactl streams detail (--id N | --slug S) [--pipeline-id P --results-limit 10 --detections-limit 50]
	  stoaramactl streams page-load --id N [--recent-limit 24 --include-thumbnails=true --include-coverage --include-inference --timeout-sec 20 --json]
	  stoaramactl streams filters --kind tags|countries|cities|sources|youtube-channels [--recording-state off|on --capture-type TYPE --country C --city CITY --source SRC --youtube-channel CH --tags a,b --tags-not x,y]
	  stoaramactl streams frames [--stream-id N --pipeline-id P --uninferenced --unprocessed --sort-by captured_at --sort-dir desc --limit 200 --offset 0]
	  stoaramactl streams clips --stream-id N [--limit 100 --offset 0]
	  stoaramactl streams clip-latest --stream-id N
	  stoaramactl streams timeline --id N [--day YYYY-MM-DD --pipeline-id P]
	  stoaramactl streams image-urls --stream-ids 1,2,3
	  stoaramactl streams add --source-url URL --name N [--provider P --external-id E --slug S --source-page-url URL --capture-type TYPE --execution-config-json JSON --location-country C --location-country-code CC --location-region R --location-city CITY --location-locality L --location-source SRC --tags a,b]
	  stoaramactl streams update --id N [--name N --slug S --source-url URL --recording-state off|on --tags a,b --capture-type TYPE --execution-config-json JSON --location-country C --location-country-code CC --location-region R --location-city CITY --location-locality L --location-source SRC]
	  stoaramactl streams tags-add (--id N | --slug S) --tags a,b
	  stoaramactl streams tags-remove (--id N | --slug S) --tags a,b
	  stoaramactl streams cleanup-location-tags [--recording-state off|on --limit 0 --apply --json]
	  stoaramactl streams metadata-audit [--backend-api-url URL --api-token TOKEN --recording-state off|on --page-size 500 --sample-limit 40 --allow-generic-location-city --apply --apply-generic-location-fixes --max-updates 0]
	  stoaramactl streams set-capture --id N --capture-type TYPE [--config-json JSON]
	  stoaramactl streams migrate-v2 [--id N --limit 1000 --only-changed --only-review --apply --report-json out.json --json]
	  stoaramactl streams repair-youtube [--id N --limit 1000 --only-changed --apply --report-json out.json --json]
	  stoaramactl streams repair-image-capture [--id N --source-url-like %%pattern%% --provider P --limit 1000 --only-changed --apply --json]
	  stoaramactl streams repair-canonical-capture [--id N --source-url-like %%pattern%% --provider P --limit 1000 --only-changed --only-review --legacy-imported-only=true --non-youtube-only=true --apply --json]
	  stoaramactl streams recording-state-service --id N --recording-state off|on [--json]
	  stoaramactl discovery candidates list [--id N --review-status pending|accepted|rejected|invalid --provider P --capture-type TYPE --limit 200 --offset 0]
	  stoaramactl discovery candidates review --id N --status accepted|rejected|invalid [--reviewer TEXT --reason TEXT --metadata-json JSON]
	  stoaramactl discovery candidates import --id N [--provider P --external-id E --name N --slug S --source-url URL --source-page-url URL --source-family FAMILY --capture-type TYPE --execution-class CLASS --execution-config-json JSON --tags a,b --location-country C --location-country-code CC --location-region R --location-city CITY --location-locality L --location-source SRC --metadata-json JSON]
  stoaramactl media backfill --snapshot-root local/snapshots [--concurrency 8 --dry-run]
  stoaramactl inference list [--stream-id N --pipeline-id P --status queued_boxed|success|error --class-name person --search TEXT --min-confidence 0.5 --sort-by created_at --sort-dir desc --limit 200 --offset 0]
  stoaramactl inference cleanup-unboxed [--pipeline-id P --mode requeue|delete --dry-run]
	  stoaramactl jobs list [--status pending|leased|done|error --limit 200]
	  stoaramactl jobs retry --id N
	  stoaramactl jobs dead-letter [--limit 200]
	  stoaramactl alerts send-test-email [--to email@example.com] [--stream-id N --stream-name NAME --reason capture_runtime_stopped]
	  stoaramactl alerts history [--limit 50 --status accepted|delivered|opened|bounced|failed --stream-id N]
	  stoaramactl import legacy-live-streams [--legacy-api-url URL --legacy-api-token TOKEN --target-api-url URL --service-token TOKEN --offset 0 --limit 200 --page-size 50 --concurrency 4 --probe-timeout-sec 45 --legacy-recording-state off|on --legacy-provider P --apply --report-json out.json --json]
	  stoaramactl overview summary [--backend-api-url URL --api-token TOKEN]
	  stoaramactl overview status [--backend-api-url URL --api-token TOKEN --hours 168]
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
	  stoaramactl recording enable --id N [--backend-api-url URL --api-token TOKEN]
	  stoaramactl recording disable --id N [--backend-api-url URL --api-token TOKEN]
	  stoaramactl recording settings [--clip-duration-sec 30|90] [--backend-api-url URL --api-token TOKEN]
	  stoaramactl recording status [--id N --hours 24 --runs-limit 100 --events-limit 100] [--backend-api-url URL --api-token TOKEN]
	  stoaramactl recording runs [--stream-id N --limit 100 --hours 24] [--backend-api-url URL --api-token TOKEN]
	  stoaramactl recording queue [--hours 24] [--backend-api-url URL --api-token TOKEN]
	  stoaramactl recording coverage --id N [--days 365] [--backend-api-url URL --api-token TOKEN]
	  stoaramactl recording samples --id N [--count 42] [--backend-api-url URL --api-token TOKEN]
	  stoaramactl recording reconcile --id N [--apply --backend-api-url URL --api-token TOKEN]
	  stoaramactl recording supervisor run [--backend-api-url URL --api-token TOKEN --interval-sec 60 --limit 500 --dry-run --once]
	  stoaramactl recording supervisor incidents [--status open|resolved --limit 200 --json]
	  stoaramactl recording supervisor reconcile --id N [--apply --backend-api-url URL --api-token TOKEN]
	  stoaramactl korea inventory
	  stoaramactl korea audit
	  stoaramactl korea utic scrape [--api-url URL --service-key KEY --out report.json --json]
	  stoaramactl korea utic ingest [--api-url URL --service-key KEY --backend-api-url URL --api-token TOKEN --auto-import=true --dry-run --limit 0 --report-json out.json --json]
	  stoaramactl korea utic refresh-frames [--backend-api-url URL --api-token TOKEN --concurrency 4 --timeout-sec 90 --limit 0 --dry-run --allow-failures --report-json out.json --json]
	  stoaramactl servers list [--backend-api-url URL --api-token TOKEN --hours 168 --include-stale=false --show-processes=true]
	  stoaramactl servers assignments [--server-id ID --execution-class CLASS --limit 500 --offset 0] [--backend-api-url URL --api-token TOKEN]
	  stoaramactl servers assignments audit [--server-id ID --execution-class CLASS --limit 500 --offset 0] [--backend-api-url URL --api-token TOKEN]
	  stoaramactl servers assignments reconcile [--server-id ID --execution-class CLASS --limit 500 --offset 0 --apply --actor TEXT --reason TEXT] [--backend-api-url URL --api-token TOKEN]
	  stoaramactl servers assign --id N [--server-id ID|--auto] [--reason TEXT --actor TEXT --backend-api-url URL --api-token TOKEN]
	  stoaramactl servers unassign --id N --yes [--reason TEXT --actor TEXT --backend-api-url URL --api-token TOKEN]
	  stoaramactl servers capacity list [--include-inactive=false] [--backend-api-url URL --api-token TOKEN]
	  stoaramactl servers capacity groups [--backend-api-url URL --api-token TOKEN]
	  stoaramactl servers capacity heartbeat --server-id ID [--capture-shared-capacity N | --execution-class-capacity CLASS=N[,CLASS=N...]] [--draining-execution-classes CLASS[,CLASS...]] [--lease-sec 45 --metadata-json JSON --backend-api-url URL --api-token TOKEN]
	  stoaramactl servers capacity stopped --server-id ID [--backend-api-url URL --api-token TOKEN]
`)
}

func runCaptureServer(ctx context.Context, cfg config.Config, args []string) {
	if len(args) < 1 || args[0] != "run" {
		log.Fatalf("usage: stoaramactl capture-server run [--backend-api-url URL --api-token TOKEN --server-id ID --worker-id ID --capture-shared-capacity 6 --stream-ids 1,2 --heartbeat-sec 15 --lease-sec 45 --refresh-sec 5 --metadata-json JSON --duration 0]")
	}
	fs := flag.NewFlagSet("capture-server run", flag.ExitOnError)
	backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
	apiToken := fs.String("api-token", cfg.APIToken, "backend API token")
	workerID := fs.String("worker-id", defaultCaptureServerWorkerID(cfg.WorkerID), "worker id")
	serverID := fs.String("server-id", defaultCaptureServerID(defaultCaptureServerWorkerID(cfg.WorkerID)), "server id")
	captureSharedCapacity := fs.Int("capture-shared-capacity", envIntOrDefault("CAPTURE_SERVER_CAPTURE_SHARED_CAPACITY", 1), "concurrent sampled clip captures")
	_ = fs.String("execution-classes", strings.TrimSpace(os.Getenv("CAPTURE_SERVER_EXECUTION_CLASSES")), "removed; sampled capture worker handles all continuous video streams")
	streamIDsRaw := fs.String("stream-ids", strings.TrimSpace(os.Getenv("CAPTURE_SERVER_STREAM_IDS")), "optional comma-separated stream ids to run in stream-filter mode")
	_ = fs.String("draining-execution-classes", strings.TrimSpace(os.Getenv("CAPTURE_SERVER_DRAINING_EXECUTION_CLASSES")), "removed; sampled capture worker does not use assignments")
	heartbeatSec := fs.Int("heartbeat-sec", 15, "heartbeat interval seconds")
	leaseSec := fs.Int("lease-sec", 45, "heartbeat lease seconds")
	refreshSec := fs.Int("refresh-sec", cfg.CaptureTickSec, "capture job poll interval seconds")
	_ = fs.Int("frame-queue-size", 64, "removed; sampled capture writes one segment per job")
	_ = fs.Int("frame-enqueue-timeout-sec", 3, "removed; sampled capture writes one segment per job")
	_ = fs.Int("frame-writer-workers", 2, "removed; sampled capture writes one segment per job")
	_ = fs.Int("unsupported-threshold", cfg.CaptureUnsupportedThreshold, "removed; sampled capture alerts after repeated failures without disabling")
	metadataJSON := fs.String("metadata-json", "{}", "server metadata JSON object")
	duration := fs.Duration("duration", 0, "optional run duration (e.g. 30m, 8h)")
	_ = fs.Parse(args[1:])

	if strings.TrimSpace(*backendAPIURL) == "" {
		log.Fatalf("--backend-api-url is required")
	}
	if strings.TrimSpace(*apiToken) == "" {
		log.Fatalf("--api-token is required")
	}
	if strings.TrimSpace(*workerID) == "" {
		log.Fatalf("--worker-id is required")
	}
	if strings.TrimSpace(*serverID) == "" {
		log.Fatalf("--server-id is required")
	}
	if *heartbeatSec <= 0 {
		log.Fatalf("--heartbeat-sec must be > 0")
	}
	if *leaseSec <= 0 || *leaseSec > 3600 {
		log.Fatalf("--lease-sec must be between 1 and 3600")
	}
	if *leaseSec <= *heartbeatSec {
		log.Fatalf("--lease-sec must be greater than --heartbeat-sec")
	}
	if *refreshSec <= 0 {
		log.Fatalf("--refresh-sec must be > 0")
	}
	if *captureSharedCapacity <= 0 {
		log.Fatalf("--capture-shared-capacity must be > 0")
	}

	streamIDs, err := parseInt64CSV(*streamIDsRaw)
	if err != nil {
		log.Fatalf("parse --stream-ids: %v", err)
	}

	meta := map[string]any{}
	rawMeta := strings.TrimSpace(*metadataJSON)
	if rawMeta != "" {
		if err := json.Unmarshal([]byte(rawMeta), &meta); err != nil {
			log.Fatalf("invalid --metadata-json: %v", err)
		}
	}
	hostName := ""
	if h, err := os.Hostname(); err == nil {
		hostName = strings.TrimSpace(h)
	}
	meta["host"] = hostName
	meta["server_id"] = strings.TrimSpace(*serverID)
	meta["worker_id"] = strings.TrimSpace(*workerID)
	meta["process_name"] = "capture-server"
	meta["process_id"] = strings.TrimSpace(*workerID)

	client, err := captureapi.NewClient(captureapi.ClientConfig{
		BaseURL:  strings.TrimSpace(*backendAPIURL),
		APIToken: strings.TrimSpace(*apiToken),
	})
	if err != nil {
		log.Fatalf("init capture api client: %v", err)
	}
	registry, err := capture.NewDefaultRegistry()
	if err != nil {
		log.Fatalf("init capture registry: %v", err)
	}

	runCtx := ctx
	cancel := func() {}
	if *duration > 0 {
		runCtx, cancel = context.WithTimeout(ctx, *duration)
	}
	defer cancel()

	worker, err := capturescheduled.NewWorker(capturescheduled.Config{
		Client:            client,
		Registry:          registry,
		WorkerID:          strings.TrimSpace(*workerID),
		ServerID:          strings.TrimSpace(*serverID),
		Concurrency:       *captureSharedCapacity,
		LeaseSec:          *leaseSec,
		PollInterval:      time.Duration(*refreshSec) * time.Second,
		HeartbeatInterval: time.Duration(*heartbeatSec) * time.Second,
		MetadataJSON:      meta,
		StreamIDs:         streamIDs,
	})
	if err != nil {
		log.Fatalf("init sampled capture worker: %v", err)
	}
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer stopCancel()
		if workerStopErr := client.WorkerStopped(stopCtx, strings.TrimSpace(*workerID), capture.ExecutionClassVideoLive); workerStopErr != nil {
			log.Printf("capture-server worker stop signal failed worker_id=%s: %v", strings.TrimSpace(*workerID), workerStopErr)
		}
	}()
	if err := worker.Run(runCtx); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		log.Fatalf("capture-server run failed: %v", err)
	}
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
	fmt.Print("stoaramactl streams <list|detail|page-load|filters|frames|clips|clip-latest|timeline|image-urls|add|update|tags-add|tags-remove|cleanup-location-tags|metadata-audit|set-capture|migrate-v2|repair-youtube|repair-image-capture|repair-canonical-capture|recording-state-service|recording-state-bulk> ...\n")
}

func printDiscoveryUsage() {
	fmt.Print("stoaramactl discovery candidates <list|review|import> ...\n")
}

func printRecordingUsage() {
	fmt.Print("stoaramactl recording <interval|enable|disable|settings|status|runs|queue|coverage|samples|reconcile|supervisor> ...\n")
	fmt.Print("stoaramactl recording settings [--clip-duration-sec 30|90] [--backend-api-url URL --api-token TOKEN]\n")
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

func patchStreamRecordingState(ctx context.Context, backendAPIURL string, apiToken string, streamID int64, state model.RecordingState) map[string]any {
	if streamID <= 0 {
		log.Fatalf("--id is required")
	}
	return mustAPIRequest(ctx, http.MethodPatch, strings.TrimSpace(backendAPIURL), strings.TrimSpace(apiToken), fmt.Sprintf("/api/v1/streams/%d", streamID), map[string]any{
		"recording_state": string(state),
	})
}

type streamPageLoadOptions struct {
	RecentLimit       int
	IncludeThumbnails bool
	IncludeCoverage   bool
	IncludeInference  bool
	Timeout           time.Duration
}

type streamPageLoadStep struct {
	Name           string `json:"name"`
	Method         string `json:"method"`
	Path           string `json:"path"`
	StatusCode     int    `json:"status_code,omitempty"`
	DurationMS     int64  `json:"duration_ms"`
	OK             bool   `json:"ok"`
	Items          int    `json:"items"`
	RecentCaptures int    `json:"recent_captures"`
	Error          string `json:"error,omitempty"`
}

type streamPageLoadReport struct {
	StreamID        int64                `json:"stream_id"`
	OK              bool                 `json:"ok"`
	TotalDurationMS int64                `json:"total_duration_ms"`
	Steps           []streamPageLoadStep `json:"steps"`
}

func probeStreamPageLoad(ctx context.Context, backendAPIURL string, apiToken string, streamID int64, opts streamPageLoadOptions) streamPageLoadReport {
	started := time.Now()
	report := streamPageLoadReport{
		StreamID: streamID,
		OK:       true,
		Steps:    []streamPageLoadStep{},
	}
	add := func(name string, method string, path string, payload any) map[string]any {
		step, response := timedAPIRequest(ctx, method, backendAPIURL, apiToken, path, payload, opts.Timeout)
		step.Name = name
		if !step.OK {
			report.OK = false
		}
		report.Steps = append(report.Steps, step)
		return response
	}

	recordingQuery := url.Values{}
	recordingQuery.Set("include_recent_captures", strconv.Itoa(opts.RecentLimit))
	recordingPath := fmt.Sprintf("/api/v1/dashboard/streams/%d/recording?%s", streamID, recordingQuery.Encode())
	recording := add("recording", http.MethodGet, recordingPath, nil)
	if len(report.Steps) > 0 {
		last := &report.Steps[len(report.Steps)-1]
		last.RecentCaptures = arrayLen(recording["recent_captures"])
		last.Items = -1
	}

	if opts.IncludeThumbnails {
		thumbs := add("thumbnails", http.MethodPost, "/api/v1/dashboard/streams/image-urls", map[string]any{
			"stream_ids": []int64{streamID},
		})
		if len(report.Steps) > 0 {
			last := &report.Steps[len(report.Steps)-1]
			last.Items = arrayLen(thumbs["items"])
			last.RecentCaptures = -1
		}
	}
	if opts.IncludeCoverage {
		coverage := add("coverage", http.MethodGet, fmt.Sprintf("/api/v1/dashboard/streams/%d/coverage?days=365", streamID), nil)
		if len(report.Steps) > 0 {
			last := &report.Steps[len(report.Steps)-1]
			last.Items = arrayLen(coverage["points"])
			last.RecentCaptures = -1
		}
		samples := add("capture-samples", http.MethodGet, fmt.Sprintf("/api/v1/dashboard/streams/%d/capture-samples?count=42", streamID), nil)
		if len(report.Steps) > 0 {
			last := &report.Steps[len(report.Steps)-1]
			last.Items = arrayLen(samples["items"])
			last.RecentCaptures = -1
		}
	}
	if opts.IncludeInference {
		detailQuery := url.Values{}
		detailQuery.Set("limit", "10")
		detailQuery.Set("offset", "0")
		detailQuery.Set("sort_by", "created_at")
		detailQuery.Set("sort_dir", "desc")
		detail := add("inference", http.MethodGet, fmt.Sprintf("/api/v1/dashboard/streams/%d?%s", streamID, detailQuery.Encode()), nil)
		if len(report.Steps) > 0 {
			last := &report.Steps[len(report.Steps)-1]
			last.Items = arrayLen(detail["inference"])
			last.RecentCaptures = -1
		}
		detectionQuery := url.Values{}
		detectionQuery.Set("limit", "500")
		detections := add("detections", http.MethodGet, fmt.Sprintf("/api/v1/dashboard/streams/%d/detections?%s", streamID, detectionQuery.Encode()), nil)
		if len(report.Steps) > 0 {
			last := &report.Steps[len(report.Steps)-1]
			last.Items = arrayLen(detections["detections"])
			last.RecentCaptures = -1
		}
	}
	report.TotalDurationMS = time.Since(started).Milliseconds()
	return report
}

func timedAPIRequest(ctx context.Context, method string, baseURL string, apiToken string, path string, payload any, timeout time.Duration) (step streamPageLoadStep, out map[string]any) {
	step = streamPageLoadStep{
		Method:         strings.TrimSpace(strings.ToUpper(method)),
		Path:           path,
		Items:          -1,
		RecentCaptures: -1,
	}
	started := time.Now()
	defer func() {
		step.DurationMS = time.Since(started).Milliseconds()
	}()
	base := strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if base == "" {
		step.Error = "--backend-api-url is required"
		return step, out
	}
	token := strings.TrimSpace(apiToken)
	if token == "" {
		step.Error = "--api-token is required"
		return step, out
	}
	p := strings.TrimSpace(path)
	if p == "" {
		step.Error = "api path is required"
		return step, out
	}
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	step.Path = p
	var body io.Reader
	if payload != nil {
		raw, err := json.Marshal(payload)
		if err != nil {
			step.Error = fmt.Sprintf("marshal api payload: %v", err)
			return step, out
		}
		body = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, step.Method, base+p, body)
	if err != nil {
		step.Error = fmt.Sprintf("build api request: %v", err)
		return step, out
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	switch step.Method {
	case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		req.Header.Set("Idempotency-Key", fmt.Sprintf("%s:%s:%d", step.Method, p, time.Now().UnixNano()))
	}
	if timeout <= 0 {
		timeout = 20 * time.Second
	}
	resp, err := (&http.Client{Timeout: timeout}).Do(req)
	if err != nil {
		step.Error = err.Error()
		return step, out
	}
	defer resp.Body.Close()
	step.StatusCode = resp.StatusCode
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		step.Error = strings.TrimSpace(string(body))
		return step, out
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		step.Error = fmt.Sprintf("decode api response: %v", err)
		return step, out
	}
	step.OK = true
	return step, out
}

func loadDashboardStream(ctx context.Context, backendAPIURL string, apiToken string, streamID int64) map[string]any {
	if streamID <= 0 {
		log.Fatalf("--id is required")
	}
	payload := mustAPIGet(ctx, strings.TrimSpace(backendAPIURL), strings.TrimSpace(apiToken), fmt.Sprintf("/api/v1/dashboard/streams/%d?limit=1", streamID))
	stream := asMap(payload["stream"])
	if int64FromAny(stream["id"]) != streamID {
		log.Fatalf("stream %d not found", streamID)
	}
	return stream
}

func inferRecordingAssignmentExecutionClassesForCLI(stream map[string]any) []string {
	executionClass := strings.TrimSpace(fmt.Sprint(stream["execution_class"]))
	if executionClass != "" && executionClass != "<nil>" {
		norm, ok := capture.NormalizeExecutionClass(executionClass)
		if ok {
			if norm == capture.ExecutionClassYouTubeDirect || norm == capture.ExecutionClassYouTubeRelay {
				return []string{capture.ExecutionClassYouTubeDirect}
			}
			if norm == capture.ExecutionClassImagePoll {
				return nil
			}
			return []string{norm}
		}
	}
	captureType := strings.TrimSpace(fmt.Sprint(stream["capture_type"]))
	if captureType != "" && captureType != "<nil>" {
		if norm, ok := capture.NormalizeCaptureType(captureType); ok {
			switch norm {
			case capture.CaptureTypeYouTubeWatch:
				return []string{capture.ExecutionClassYouTubeDirect}
			case capture.CaptureTypeStillImage:
				return nil
			case capture.CaptureTypeHLS, capture.CaptureTypeDASH, capture.CaptureTypeRTSP, capture.CaptureTypeRTMP, capture.CaptureTypeHTTPVideo:
				return []string{capture.ExecutionClassVideoLive}
			}
		}
	}
	sourceFamily := strings.ToLower(strings.TrimSpace(fmt.Sprint(stream["source_family"])))
	if sourceFamily == capture.SourceFamilyWatchPage {
		return []string{capture.ExecutionClassYouTubeDirect}
	}
	return nil
}

func recordingCandidateExecutionClassListForCLI(row map[string]any) []string {
	out := make([]string, 0, 4)
	seen := map[string]struct{}{}
	push := func(raw any) {
		v := strings.TrimSpace(fmt.Sprint(raw))
		if v == "" || v == "<nil>" {
			return
		}
		norm, ok := capture.NormalizeExecutionClass(v)
		if !ok {
			return
		}
		if _, exists := seen[norm]; exists {
			return
		}
		seen[norm] = struct{}{}
		out = append(out, norm)
	}
	if arr, ok := row["available_execution_classes"].([]any); ok {
		for _, raw := range arr {
			push(raw)
		}
	}
	if arr, ok := row["execution_classes"].([]any); ok {
		for _, raw := range arr {
			push(raw)
		}
	}
	return out
}

func recordingCandidateSupportsExecutionClassesForCLI(row map[string]any, desired []string) bool {
	if len(desired) == 0 {
		return true
	}
	have := recordingCandidateExecutionClassListForCLI(row)
	if len(have) == 0 {
		return false
	}
	haveSet := map[string]struct{}{}
	for _, executionClass := range have {
		haveSet[executionClass] = struct{}{}
	}
	for _, executionClass := range desired {
		if _, ok := haveSet[executionClass]; ok {
			return true
		}
	}
	return false
}

func autoSelectRecordingServer(ctx context.Context, backendAPIURL string, apiToken string, streamID int64) string {
	stream := loadDashboardStream(ctx, backendAPIURL, apiToken, streamID)
	desiredExecutionClasses := inferRecordingAssignmentExecutionClassesForCLI(stream)
	if len(desiredExecutionClasses) == 0 {
		log.Fatalf("stream %d is not startable in the clip-native recording path", streamID)
	}
	payload := mustAPIGet(ctx, strings.TrimSpace(backendAPIURL), strings.TrimSpace(apiToken), "/api/v1/dashboard/recording/server-capacity")
	items, _ := payload["items"].([]any)
	candidates := make([]map[string]any, 0, len(items))
	for _, raw := range items {
		row := asMap(raw)
		serverID := strings.TrimSpace(fmt.Sprint(row["server_id"]))
		if serverID == "" || serverID == "<nil>" {
			continue
		}
		if !boolFromAny(row["active"]) || boolFromAny(row["draining"]) {
			continue
		}
		freeSlots := int64FromAny(row["free_slots"])
		if freeSlots <= 0 {
			continue
		}
		if !recordingCandidateSupportsExecutionClassesForCLI(row, desiredExecutionClasses) {
			continue
		}
		candidates = append(candidates, row)
	}
	if len(candidates) == 0 {
		available := make([]string, 0, 8)
		seen := map[string]struct{}{}
		for _, raw := range items {
			row := asMap(raw)
			for _, executionClass := range recordingCandidateExecutionClassListForCLI(row) {
				if _, ok := seen[executionClass]; ok {
					continue
				}
				seen[executionClass] = struct{}{}
				available = append(available, executionClass)
			}
		}
		sort.Strings(available)
		if len(desiredExecutionClasses) > 0 {
			log.Fatalf("no recording server has free capacity for %s (available execution classes: %s)", strings.Join(desiredExecutionClasses, "/"), defaultString(strings.Join(available, "/"), "none"))
		}
		log.Fatalf("no recording server has free capacity")
	}
	rank := func(row map[string]any) int {
		executionClasses := recordingCandidateExecutionClassListForCLI(row)
		if len(desiredExecutionClasses) == 0 {
			return 0
		}
		best := len(desiredExecutionClasses) + 1
		for _, executionClass := range executionClasses {
			for idx, desired := range desiredExecutionClasses {
				if executionClass == desired && idx < best {
					best = idx
				}
			}
		}
		return best
	}
	sort.Slice(candidates, func(i, j int) bool {
		rankI := rank(candidates[i])
		rankJ := rank(candidates[j])
		if rankI != rankJ {
			return rankI < rankJ
		}
		freeI := int64FromAny(candidates[i]["free_slots"])
		freeJ := int64FromAny(candidates[j]["free_slots"])
		if freeI != freeJ {
			return freeI > freeJ
		}
		return strings.TrimSpace(fmt.Sprint(candidates[i]["server_id"])) < strings.TrimSpace(fmt.Sprint(candidates[j]["server_id"]))
	})
	return strings.TrimSpace(fmt.Sprint(candidates[0]["server_id"]))
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
	case "page-load":
		fs := flag.NewFlagSet("streams page-load", flag.ExitOnError)
		backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
		apiToken := fs.String("api-token", cfg.APIToken, "backend API token")
		id := fs.Int64("id", 0, "stream id")
		recentLimit := fs.Int("recent-limit", 24, "recent captures included with the recording payload")
		includeThumbnails := fs.Bool("include-thumbnails", true, "include list thumbnail lookup")
		includeCoverage := fs.Bool("include-coverage", false, "include coverage and capture sample calls")
		includeInference := fs.Bool("include-inference", false, "include inference detail and detection calls")
		timeoutSec := fs.Int("timeout-sec", 20, "per-call timeout seconds")
		asJSON := fs.Bool("json", false, "print JSON")
		_ = fs.Parse(args[1:])
		if *id <= 0 {
			log.Fatalf("--id is required")
		}
		if *recentLimit < 0 || *recentLimit > 100 {
			log.Fatalf("--recent-limit must be between 0 and 100")
		}
		if *timeoutSec <= 0 || *timeoutSec > 300 {
			log.Fatalf("--timeout-sec must be between 1 and 300")
		}
		report := probeStreamPageLoad(ctx, strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken), *id, streamPageLoadOptions{
			RecentLimit:       *recentLimit,
			IncludeThumbnails: *includeThumbnails,
			IncludeCoverage:   *includeCoverage,
			IncludeInference:  *includeInference,
			Timeout:           time.Duration(*timeoutSec) * time.Second,
		})
		if *asJSON {
			printJSON(report)
		} else {
			fmt.Printf("stream_id=%d ok=%t total_ms=%d\n", report.StreamID, report.OK, report.TotalDurationMS)
			for _, step := range report.Steps {
				status := "-"
				if step.StatusCode > 0 {
					status = strconv.Itoa(step.StatusCode)
				}
				line := fmt.Sprintf("  %s method=%s status=%s ok=%t duration_ms=%d", step.Name, step.Method, status, step.OK, step.DurationMS)
				if step.Items >= 0 {
					line += fmt.Sprintf(" items=%d", step.Items)
				}
				if step.RecentCaptures >= 0 {
					line += fmt.Sprintf(" recent_captures=%d", step.RecentCaptures)
				}
				if step.Error != "" {
					line += fmt.Sprintf(" error=%q", step.Error)
				}
				fmt.Println(line)
			}
		}
		if !report.OK {
			os.Exit(1)
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
	case "timeline":
		fs := flag.NewFlagSet("streams timeline", flag.ExitOnError)
		backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
		apiToken := fs.String("api-token", cfg.APIToken, "backend API token")
		id := fs.Int64("id", 0, "stream id")
		day := fs.String("day", "", "UTC day YYYY-MM-DD")
		pipelineID := fs.String("pipeline-id", "", "optional pipeline id")
		asJSON := fs.Bool("json", false, "print JSON")
		_ = fs.Parse(args[1:])
		if *id <= 0 {
			log.Fatalf("--id is required")
		}
		q := url.Values{}
		if v := strings.TrimSpace(*day); v != "" {
			if _, err := time.Parse("2006-01-02", v); err != nil {
				log.Fatalf("--day must be YYYY-MM-DD")
			}
			q.Set("day", v)
		}
		if v := strings.TrimSpace(*pipelineID); v != "" {
			q.Set("pipeline_id", v)
		}
		path := fmt.Sprintf("/api/v1/dashboard/streams/%d/timeline", *id)
		if encoded := q.Encode(); encoded != "" {
			path += "?" + encoded
		}
		payload := mustAPIGet(ctx, strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken), path)
		if *asJSON {
			printJSON(payload)
			return
		}
		totals := asMap(payload["totals"])
		fmt.Printf("stream_id=%d day=%v pipeline=%v recorded_minutes=%v inferenced_minutes=%v person_minutes=%v recorded_frames=%v inference_frames=%v detections_total=%v\n",
			*id, payload["day"], payload["selected_pipeline_id"], totals["recorded_minutes"], totals["inferenced_minutes"], totals["person_minutes"], totals["recorded_total_frames"], totals["inferenced_frames"], totals["person_detections"])
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
	case "migrate-v2":
		runStreamsMigrateV2(ctx, cfg, args[1:])
	case "repair-youtube":
		runStreamsRepairYouTube(ctx, cfg, args[1:])
	case "repair-image-capture":
		runStreamsRepairImageCapture(ctx, cfg, args[1:])
	case "repair-canonical-capture":
		runStreamsRepairCanonicalCapture(ctx, cfg, args[1:])
	case "recording-state-service":
		runStreamsRecordingStateService(ctx, cfg, args[1:])
	case "recording-state-bulk":
		runStreamsRecordingStateBulk(ctx, cfg, args[1:])
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

func runJobs(ctx context.Context, cfg config.Config, args []string) {
	if len(args) < 1 {
		log.Fatalf("usage: stoaramactl jobs <list|retry|dead-letter>")
	}
	sub := args[0]
	pool := mustOpenPool(ctx, cfg)
	defer pool.Close()

	switch sub {
	case "list":
		fs := flag.NewFlagSet("jobs list", flag.ExitOnError)
		status := fs.String("status", "", "status filter")
		limit := fs.Int("limit", 200, "limit")
		_ = fs.Parse(args[1:])
		where := "1=1"
		queryArgs := []any{*limit}
		if strings.TrimSpace(*status) != "" {
			where = "status=$1"
			queryArgs = []any{*status, *limit}
		}
		rows, err := pool.Query(ctx, fmt.Sprintf(`
			SELECT id, stream_id, scheduled_for, status, lease_owner, lease_expires_at, attempt_count, max_attempts, error_text, created_at, updated_at
			FROM capture_jobs
			WHERE %s
			ORDER BY id DESC
			LIMIT $%d
		`, where, len(queryArgs)), queryArgs...)
		if err != nil {
			log.Fatalf("jobs list query: %v", err)
		}
		defer rows.Close()
		for rows.Next() {
			var id, streamID int64
			var statusVal string
			var scheduledFor, createdAt, updatedAt time.Time
			var leaseOwner *string
			var leaseExpires *time.Time
			var attempts, maxAttempts int
			var errText *string
			if err := rows.Scan(&id, &streamID, &scheduledFor, &statusVal, &leaseOwner, &leaseExpires, &attempts, &maxAttempts, &errText, &createdAt, &updatedAt); err != nil {
				log.Fatalf("jobs list scan: %v", err)
			}
			fmt.Printf("id=%d stream_id=%d status=%s scheduled_for=%s lease_owner=%s attempts=%d/%d err=%s\n",
				id, streamID, statusVal, scheduledFor.Format(time.RFC3339), derefString(leaseOwner), attempts, maxAttempts, derefString(errText))
		}
		if rows.Err() != nil {
			log.Fatalf("jobs list iterate: %v", rows.Err())
		}
	case "retry":
		fs := flag.NewFlagSet("jobs retry", flag.ExitOnError)
		id := fs.Int64("id", 0, "job id")
		_ = fs.Parse(args[1:])
		if *id <= 0 {
			log.Fatalf("--id is required")
		}
		res, err := pool.Exec(ctx, `
			UPDATE capture_jobs
			SET status='pending', lease_owner=NULL, lease_expires_at=NULL, error_text=NULL, updated_at=now()
			WHERE id=$1 AND status='error'
		`, *id)
		if err != nil {
			log.Fatalf("retry job: %v", err)
		}
		if res.RowsAffected() == 0 {
			log.Fatalf("job not found in error state")
		}
		fmt.Printf("job %d moved to pending\n", *id)
	case "dead-letter":
		fs := flag.NewFlagSet("jobs dead-letter", flag.ExitOnError)
		limit := fs.Int("limit", 200, "limit")
		_ = fs.Parse(args[1:])
		rows, err := pool.Query(ctx, `
			SELECT id, stream_id, scheduled_for, attempt_count, max_attempts, error_text
			FROM capture_jobs
			WHERE status='error'
			ORDER BY updated_at DESC, id DESC
			LIMIT $1
		`, *limit)
		if err != nil {
			log.Fatalf("dead-letter query: %v", err)
		}
		defer rows.Close()
		for rows.Next() {
			var id, streamID int64
			var scheduledFor time.Time
			var attemptCount, maxAttempts int
			var errText *string
			if err := rows.Scan(&id, &streamID, &scheduledFor, &attemptCount, &maxAttempts, &errText); err != nil {
				log.Fatalf("dead-letter scan: %v", err)
			}
			fmt.Printf("id=%d stream_id=%d scheduled_for=%s attempts=%d/%d error=%s\n", id, streamID, scheduledFor.Format(time.RFC3339), attemptCount, maxAttempts, derefString(errText))
		}
		if rows.Err() != nil {
			log.Fatalf("dead-letter iterate: %v", rows.Err())
		}
	default:
		log.Fatalf("unknown jobs subcommand: %s", sub)
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
		fmt.Print("stoaramactl overview <summary|status|queue-health> ...\n")
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
		case "status":
			fmt.Print("stoaramactl overview status [--backend-api-url URL --api-token TOKEN --hours 168]\n")
		case "queue-health":
			fmt.Print("stoaramactl overview queue-health [--backend-api-url URL --api-token TOKEN]\n")
		default:
			log.Fatalf("usage: stoaramactl overview <summary|status|queue-health> ...")
		}
		return
	}
	switch args[0] {
	case "summary":
		runOverviewSurface(ctx, cfg, append([]string{"overview"}, args[1:]...))
	case "status", "queue-health":
		runOverviewSurface(ctx, cfg, args)
	default:
		log.Fatalf("usage: stoaramactl overview <summary|status|queue-health> ...")
	}
}

func runServers(ctx context.Context, cfg config.Config, args []string) {
	if len(args) >= 1 && (args[0] == "-h" || args[0] == "--help") {
		fmt.Print("stoaramactl servers <list|status|assignments|assign|unassign|capacity> ...\n")
		return
	}
	if len(args) < 1 {
		fmt.Print("stoaramactl servers <list|status|assignments|assign|unassign|capacity> ...\n")
		return
	}
	if len(args) >= 2 && (args[1] == "-h" || args[1] == "--help") {
		switch args[0] {
		case "list", "status":
			fmt.Print("stoaramactl servers list [--backend-api-url URL --api-token TOKEN --hours 168 --include-stale=false --show-processes=true]\n")
		case "assignments":
			fmt.Print("stoaramactl servers assignments [--server-id ID --execution-class CLASS --limit 500 --offset 0]\n")
			fmt.Print("stoaramactl servers assignments audit [--server-id ID --execution-class CLASS --limit 500 --offset 0] [--backend-api-url URL --api-token TOKEN]\n")
			fmt.Print("stoaramactl servers assignments reconcile [--server-id ID --execution-class CLASS --limit 500 --offset 0 --apply --actor TEXT --reason TEXT] [--backend-api-url URL --api-token TOKEN]\n")
		case "assign":
			fmt.Print("stoaramactl servers assign --id N [--server-id ID|--auto] [--reason TEXT --actor TEXT --backend-api-url URL --api-token TOKEN]\n")
		case "unassign":
			fmt.Print("stoaramactl servers unassign --id N --yes [--reason TEXT --actor TEXT --backend-api-url URL --api-token TOKEN]\n")
		case "capacity":
			fmt.Print("stoaramactl servers capacity <list|groups|heartbeat|stopped> ...\n")
		default:
			log.Fatalf("usage: stoaramactl servers <list|status|assignments|assign|unassign|capacity> ...")
		}
		return
	}
	switch args[0] {
	case "list", "status":
		runOverviewSurface(ctx, cfg, append([]string{"servers"}, args[1:]...))
	case "assignments", "assign", "unassign", "capacity":
		runServerControl(ctx, cfg, args)
	default:
		log.Fatalf("unknown servers subcommand: %s", args[0])
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
	case "status":
		fs := flag.NewFlagSet("overview status", flag.ExitOnError)
		backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
		apiToken := fs.String("api-token", cfg.APIToken, "backend API token")
		hours := fs.Int("hours", 24*7, "lookback hours for non-active servers")
		includeStale := fs.Bool("include-stale", false, "include stale server rows in server totals")
		asJSON := fs.Bool("json", false, "print JSON")
		_ = fs.Parse(args[1:])
		if *hours <= 0 {
			log.Fatalf("--hours must be > 0")
		}
		apiURL := strings.TrimSpace(*backendAPIURL)
		token := strings.TrimSpace(*apiToken)
		queue := mustAPIGet(ctx, apiURL, token, "/api/v1/dashboard/queue-health")
		servers := mustAPIGet(ctx, apiURL, token, fmt.Sprintf("/api/v1/dashboard/servers?hours=%d&include_stale=%t", *hours, *includeStale))
		pipelines := mustAPIGet(ctx, apiURL, token, "/api/v1/dashboard/pipelines/overview?include_inactive=false")
		supervision := mustAPIGet(ctx, apiURL, token, "/api/v1/recording/supervision?limit=1")
		alertRecipientsConfigured := len(splitCSV(cfg.StreamAlertsRecipients)) > 0
		alertDeliveryMode := defaultString(strings.TrimSpace(cfg.EmailProvider), "log")
		if *asJSON {
			printJSON(map[string]any{
				"queue_health": queue,
				"servers":      servers,
				"pipelines":    pipelines,
				"supervision":  supervision,
				"alerts": map[string]any{
					"email_provider":              alertDeliveryMode,
					"configured_recipients":       alertRecipientsConfigured,
					"embedded_api_monitor_active": false,
					"supervisor_command":          "stoaramactl recording supervisor run",
				},
			})
			return
		}
		serverItems, _ := servers["items"].([]any)
		pipelineItems, _ := pipelines["items"].([]any)
		fmt.Printf(
			"recording_on=%v capture_sessions=%v capture_workers=%v inference_workers=%v inference_claims=%v backlog_frames=%v pipelines=%d active_servers=%v/%d healthy=%v down_10m=%v spotty_2h=%v incidents_open=%v alerts_provider=%s alerts_recipients=%t supervisor=cli\n",
			queue["recording_on"],
			queue["capture_active_sessions"], queue["capture_active_workers"], queue["inference_active_workers"],
			queue["inference_active_claims"], queue["inference_backlog_frames"], len(pipelineItems), servers["active"], len(serverItems),
			supervision["healthy_total"], supervision["down_10m_total"], supervision["spotty_2h_total"], supervision["incidents_open"],
			alertDeliveryMode, alertRecipientsConfigured,
		)
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

func runRecording(ctx context.Context, cfg config.Config, args []string) {
	if len(args) >= 1 && (args[0] == "-h" || args[0] == "--help") {
		printRecordingUsage()
		return
	}
	if len(args) < 1 {
		printRecordingUsage()
		return
	}
	switch args[0] {
	case "interval":
		log.Fatalf("recording interval is removed; sampled clip recording uses service-wide clip duration every 4-8 minutes")
	case "enable", "disable":
		fs := flag.NewFlagSet("recording "+args[0], flag.ExitOnError)
		streamID := fs.Int64("id", 0, "stream id")
		backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
		apiToken := fs.String("api-token", cfg.APIToken, "backend API token")
		asJSON := fs.Bool("json", false, "print JSON")
		_ = fs.Parse(args[1:])
		state := model.RecordingStateOff
		if args[0] == "enable" {
			state = model.RecordingStateOn
		}
		payload := patchStreamRecordingState(ctx, *backendAPIURL, *apiToken, *streamID, state)
		if *asJSON {
			printJSON(payload)
			return
		}
		fmt.Printf("stream_id=%d slug=%s recording_state=%s\n", int64FromAny(payload["id"]), fmt.Sprint(payload["slug"]), fmt.Sprint(payload["recording_state"]))
	case "settings":
		fs := flag.NewFlagSet("recording settings", flag.ExitOnError)
		backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
		apiToken := fs.String("api-token", cfg.APIToken, "backend API token")
		clipDurationSec := fs.Int("clip-duration-sec", 0, "set clip duration seconds; allowed values: 30, 90")
		asJSON := fs.Bool("json", false, "print JSON")
		_ = fs.Parse(args[1:])
		apiURL := strings.TrimSpace(*backendAPIURL)
		token := strings.TrimSpace(*apiToken)
		if *clipDurationSec != 0 {
			if !settings.IsAllowedClipDurationSec(*clipDurationSec) {
				log.Fatalf("--clip-duration-sec must be 30 or 90")
			}
			payload := mustAPIRequest(ctx, http.MethodPut, apiURL, token, "/api/v1/dashboard/recording/settings", map[string]any{
				"clip_duration_sec":       *clipDurationSec,
				"sample_interval_min_sec": settings.DefaultSampleIntervalMinSec,
				"sample_interval_max_sec": settings.DefaultSampleIntervalMaxSec,
				"stale_grace_sec":         settings.DefaultSampleStaleGraceSec,
			})
			if *asJSON {
				printJSON(payload)
				return
			}
			fmt.Printf("clip_duration_sec=%v sample_interval=%v-%vs stale_grace_sec=%v updated_at=%v\n",
				payload["clip_duration_sec"], payload["sample_interval_min_sec"], payload["sample_interval_max_sec"], payload["stale_grace_sec"], payload["updated_at"])
			return
		}
		client, err := captureapi.NewClient(captureapi.ClientConfig{
			BaseURL:  apiURL,
			APIToken: token,
		})
		if err != nil {
			log.Fatalf("init capture api client: %v", err)
		}
		rs, err := client.GetRecordingSettings(ctx)
		if err != nil {
			log.Fatalf("get recording settings: %v", err)
		}
		if *asJSON {
			printJSON(rs)
			return
		}
		fmt.Printf("clip_duration_sec=%v sample_interval=%v-%vs stale_grace_sec=%v updated_at=%v\n",
			rs.ClipDurationSec, rs.SampleIntervalMinSec, rs.SampleIntervalMaxSec, rs.StaleGraceSec, rs.UpdatedAt)
	case "status":
		fs := flag.NewFlagSet("recording status", flag.ExitOnError)
		streamID := fs.Int64("id", 0, "optional stream id")
		hours := fs.Int("hours", 24, "lookback hours for summary")
		runsLimit := fs.Int("runs-limit", 100, "recent runs limit")
		eventsLimit := fs.Int("events-limit", 100, "recent state events limit")
		backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
		apiToken := fs.String("api-token", cfg.APIToken, "backend API token")
		asJSON := fs.Bool("json", false, "print JSON")
		_ = fs.Parse(args[1:])
		if *hours <= 0 {
			log.Fatalf("--hours must be > 0")
		}
		if *runsLimit <= 0 {
			log.Fatalf("--runs-limit must be > 0")
		}
		if *eventsLimit <= 0 {
			log.Fatalf("--events-limit must be > 0")
		}
		apiURL := strings.TrimSpace(*backendAPIURL)
		token := strings.TrimSpace(*apiToken)
		if *streamID > 0 {
			path := fmt.Sprintf("/api/v1/dashboard/streams/%d/recording?runs_limit=%d&events_limit=%d", *streamID, *runsLimit, *eventsLimit)
			payload := mustAPIGet(ctx, apiURL, token, path)
			if *asJSON {
				printJSON(payload)
				return
			}
			runs, _ := payload["process_runs"].([]any)
			events, _ := payload["state_events"].([]any)
			runtime, _ := payload["runtime"].(map[string]any)
			stats24h, _ := payload["stats_24h"].(map[string]any)
			lossRate24h, _ := asFloat64(stats24h["loss_rate_pct"])
			fmt.Printf(
				"stream_id=%d runtime_status=%v runtime_execution_class=%v last_frame=%v runs=%d events=%d success_24h=%v loss_rate_24h=%.2f%%\n",
				*streamID, runtime["status"], runtime["execution_class"], runtime["last_frame_at"],
				len(runs), len(events), stats24h["success_frames"], lossRate24h,
			)
			return
		}
		path := fmt.Sprintf("/api/v1/dashboard/recording/summary?hours=%d&runs_limit=%d&events_limit=%d", *hours, *runsLimit, *eventsLimit)
		payload := mustAPIGet(ctx, apiURL, token, path)
		if *asJSON {
			printJSON(payload)
			return
		}
		fmt.Printf(
			"streams_total=%v on=%v off=%v healthy=%v degraded=%v stale=%v active_processes=%v stale_processes=%v interval_sec=%v\n",
			payload["streams_total"], payload["recording_on"], payload["recording_off"],
			payload["recording_healthy"], payload["recording_degraded"], payload["recording_stale"],
			payload["active_processes"], payload["stale_processes"], payload["recording_interval_sec"],
		)
	case "runs":
		fs := flag.NewFlagSet("recording runs", flag.ExitOnError)
		streamID := fs.Int64("stream-id", 0, "optional stream id")
		limit := fs.Int("limit", 100, "recent runs limit")
		hours := fs.Int("hours", 24, "lookback hours for global runs")
		backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
		apiToken := fs.String("api-token", cfg.APIToken, "backend API token")
		asJSON := fs.Bool("json", false, "print JSON")
		_ = fs.Parse(args[1:])
		if *limit <= 0 {
			log.Fatalf("--limit must be > 0")
		}
		if *hours <= 0 {
			log.Fatalf("--hours must be > 0")
		}
		apiURL := strings.TrimSpace(*backendAPIURL)
		token := strings.TrimSpace(*apiToken)
		if *streamID > 0 {
			path := fmt.Sprintf("/api/v1/dashboard/streams/%d/recording?runs_limit=%d&events_limit=1", *streamID, *limit)
			payload := mustAPIGet(ctx, apiURL, token, path)
			runs, _ := payload["process_runs"].([]any)
			if *asJSON {
				printJSON(map[string]any{"stream_id": *streamID, "runs": runs})
				return
			}
			fmt.Printf("stream_id=%d runs=%d\n", *streamID, len(runs))
			for _, raw := range runs {
				it, ok := raw.(map[string]any)
				if !ok {
					continue
				}
				fmt.Printf(
					"run_id=%v execution_class=%v server=%v process=%v status=%v started_at=%v stopped_at=%v last_frame=%v restart_count=%v\n",
					it["id"], it["execution_class"], it["server_id"], it["process_id"], it["status"], it["started_at"], it["stopped_at"], it["last_frame_at"], it["restart_count"],
				)
			}
			return
		}
		path := fmt.Sprintf("/api/v1/dashboard/recording/summary?hours=%d&runs_limit=%d&events_limit=1", *hours, *limit)
		payload := mustAPIGet(ctx, apiURL, token, path)
		runs, _ := payload["recent_runs"].([]any)
		if *asJSON {
			printJSON(map[string]any{"hours": *hours, "runs": runs})
			return
		}
		fmt.Printf("runs=%d hours=%d\n", len(runs), *hours)
		for _, raw := range runs {
			it, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			fmt.Printf(
				"run_id=%v stream_id=%v execution_class=%v server=%v process=%v status=%v started_at=%v stopped_at=%v last_frame=%v\n",
				it["id"], it["stream_id"], it["execution_class"], it["server_id"], it["process_id"], it["status"], it["started_at"], it["stopped_at"], it["last_frame_at"],
			)
		}
	case "queue":
		fs := flag.NewFlagSet("recording queue", flag.ExitOnError)
		hours := fs.Int("hours", 24, "lookback hours")
		backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
		apiToken := fs.String("api-token", cfg.APIToken, "backend API token")
		asJSON := fs.Bool("json", false, "print JSON")
		_ = fs.Parse(args[1:])
		if *hours <= 0 {
			log.Fatalf("--hours must be > 0")
		}
		path := fmt.Sprintf("/api/v1/dashboard/recording/summary?hours=%d&runs_limit=1&events_limit=1", *hours)
		payload := mustAPIGet(ctx, strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken), path)
		if *asJSON {
			printJSON(payload)
			return
		}
		fmt.Printf(
			"on=%v off=%v healthy=%v degraded=%v stale=%v active_processes=%v stale_processes=%v\n",
			payload["recording_on"], payload["recording_off"],
			payload["recording_healthy"], payload["recording_degraded"], payload["recording_stale"],
			payload["active_processes"], payload["stale_processes"],
		)
	case "coverage":
		fs := flag.NewFlagSet("recording coverage", flag.ExitOnError)
		streamID := fs.Int64("id", 0, "stream id")
		days := fs.Int("days", 365, "lookback days (14-1095)")
		backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
		apiToken := fs.String("api-token", cfg.APIToken, "backend API token")
		asJSON := fs.Bool("json", false, "print JSON")
		_ = fs.Parse(args[1:])
		if *streamID <= 0 {
			log.Fatalf("--id is required")
		}
		if *days < 14 || *days > 1095 {
			log.Fatalf("--days must be between 14 and 1095")
		}
		path := fmt.Sprintf("/api/v1/dashboard/streams/%d/coverage?days=%d", *streamID, *days)
		payload := mustAPIGet(ctx, strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken), path)
		if *asJSON {
			printJSON(payload)
			return
		}
		summary, _ := payload["summary"].(map[string]any)
		points, _ := payload["points"].([]any)
		totalHours, _ := asFloat64(summary["total_recorded_hours"])
		avgHours, _ := asFloat64(summary["avg_recorded_hours_per_day"])
		maxHours, _ := asFloat64(summary["max_recorded_hours_per_day"])
		fmt.Printf(
			"stream_id=%d days=%d recorded_days=%v/%v total_hours=%.2f avg_hours_day=%.2f max_hours_day=%.2f streak_days=%v longest_gap_days=%v last_capture=%v points=%d\n",
			*streamID, *days, summary["days_with_capture"], summary["days_total"], totalHours, avgHours, maxHours,
			summary["current_streak_days"], summary["longest_gap_days"], summary["last_capture_at"], len(points),
		)
	case "samples":
		fs := flag.NewFlagSet("recording samples", flag.ExitOnError)
		streamID := fs.Int64("id", 0, "stream id")
		count := fs.Int("count", 42, "sample count (1-180)")
		backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
		apiToken := fs.String("api-token", cfg.APIToken, "backend API token")
		asJSON := fs.Bool("json", false, "print JSON")
		_ = fs.Parse(args[1:])
		if *streamID <= 0 {
			log.Fatalf("--id is required")
		}
		if *count < 1 || *count > 180 {
			log.Fatalf("--count must be between 1 and 180")
		}
		path := fmt.Sprintf("/api/v1/dashboard/streams/%d/capture-samples?count=%d", *streamID, *count)
		payload := mustAPIGet(ctx, strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken), path)
		items, _ := payload["items"].([]any)
		if *asJSON {
			printJSON(payload)
			return
		}
		fmt.Printf(
			"stream_id=%d requested=%d available_days=%v selected_days=%v samples=%d\n",
			*streamID, *count, payload["available_days"], payload["selected_days"], len(items),
		)
		for _, raw := range items {
			it, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			segmentID := fmt.Sprint(it["segment_id"])
			if fv, ok := asFloat64(it["segment_id"]); ok {
				segmentID = strconv.FormatInt(int64(fv), 10)
			}
			fmt.Printf(
				"day=%v segment_id=%v captured_at=%v object_key=%v thumbnail=%v\n",
				it["day"], segmentID, it["captured_at"], it["object_key"], it["thumbnail_object_key"],
			)
		}
	case "reconcile":
		runRecordingReconcile(ctx, cfg, args[1:])
	case "supervisor":
		runRecordingSupervisor(ctx, cfg, args[1:])
	default:
		log.Fatalf("unknown recording subcommand: %s", args[0])
	}
}

func runServerControl(ctx context.Context, cfg config.Config, args []string) {
	switch args[0] {
	case "assign":
		fs := flag.NewFlagSet("servers assign", flag.ExitOnError)
		streamID := fs.Int64("id", 0, "stream id")
		serverID := fs.String("server-id", "", "target server id (omit with --auto to choose the best live server)")
		auto := fs.Bool("auto", false, "auto-select the best live server with matching free capacity")
		reason := fs.String("reason", "", "assignment reason")
		actor := fs.String("actor", "", "assignment actor")
		backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
		apiToken := fs.String("api-token", cfg.APIToken, "backend API token")
		asJSON := fs.Bool("json", false, "print JSON")
		_ = fs.Parse(args[1:])
		if *streamID <= 0 {
			log.Fatalf("--id must be > 0")
		}
		selectedServerID := strings.TrimSpace(*serverID)
		if *auto && selectedServerID != "" {
			log.Fatalf("use either --server-id or --auto, not both")
		}
		if selectedServerID == "" {
			selectedServerID = autoSelectRecordingServer(ctx, *backendAPIURL, *apiToken, *streamID)
		}
		path := fmt.Sprintf("/api/v1/recording/streams/%d/assign", *streamID)
		payload := mustAPIRequest(ctx, http.MethodPost, strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken), path, map[string]any{
			"server_id": selectedServerID,
			"reason":    strings.TrimSpace(*reason),
			"actor":     strings.TrimSpace(*actor),
		})
		if *asJSON {
			printJSON(payload)
			return
		}
		fmt.Printf(
			"stream_id=%d server_id=%v execution_class=%v revision=%v assigned=%v/%v free_slots=%v event=%v\n",
			*streamID, payload["server_id"], payload["execution_class"], payload["assignment_revision"], payload["assigned_count"], payload["max_active"], payload["free_slots"], payload["event_type"],
		)
	case "unassign":
		fs := flag.NewFlagSet("servers unassign", flag.ExitOnError)
		streamID := fs.Int64("id", 0, "stream id")
		confirm := fs.Bool("yes", false, "required confirmation flag")
		reason := fs.String("reason", "", "stop reason")
		actor := fs.String("actor", "", "unassignment actor")
		backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
		apiToken := fs.String("api-token", cfg.APIToken, "backend API token")
		asJSON := fs.Bool("json", false, "print JSON")
		_ = fs.Parse(args[1:])
		if *streamID <= 0 {
			log.Fatalf("--id must be > 0")
		}
		if !*confirm {
			log.Fatalf("--yes is required to unassign recording (destructive)")
		}
		path := fmt.Sprintf("/api/v1/recording/streams/%d/unassign", *streamID)
		payload := mustAPIRequest(ctx, http.MethodPost, strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken), path, map[string]any{
			"confirm": fmt.Sprintf("unassign:%d", *streamID),
			"reason":  strings.TrimSpace(*reason),
			"actor":   strings.TrimSpace(*actor),
		})
		if *asJSON {
			printJSON(payload)
			return
		}
		fmt.Printf("stream_id=%d unassigned=%v previous_server=%v previous_execution_class=%v\n", *streamID, payload["unassigned"], payload["previous_server_id"], payload["previous_execution_class"])
	case "assignments":
		if len(args) >= 2 && (args[1] == "audit" || args[1] == "reconcile") {
			fs := flag.NewFlagSet("servers assignments "+args[1], flag.ExitOnError)
			serverID := fs.String("server-id", "", "optional server id filter")
			executionClassRaw := fs.String("execution-class", "", "optional execution class filter youtube_direct|video_live|image_poll")
			limit := fs.Int("limit", 500, "row limit")
			offset := fs.Int("offset", 0, "row offset")
			apply := fs.Bool("apply", false, "apply unassignments for invalid rows (reconcile only)")
			reason := fs.String("reason", "reconcile invalid assignment", "unassignment reason (reconcile only)")
			actor := fs.String("actor", "stoaramactl.servers_assignments_reconcile", "unassignment actor (reconcile only)")
			backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
			apiToken := fs.String("api-token", cfg.APIToken, "backend API token")
			asJSON := fs.Bool("json", false, "print JSON")
			_ = fs.Parse(args[2:])
			if *limit <= 0 || *limit > 2000 {
				log.Fatalf("--limit must be between 1 and 2000")
			}
			if *offset < 0 {
				log.Fatalf("--offset must be >= 0")
			}
			executionClass := strings.TrimSpace(*executionClassRaw)
			if executionClass != "" {
				norm, ok := capture.NormalizeExecutionClass(executionClass)
				if !ok {
					log.Fatalf("--execution-class must be one of youtube_direct|video_live|image_poll")
				}
				if norm == capture.ExecutionClassYouTubeRelay {
					norm = capture.ExecutionClassYouTubeDirect
				}
				executionClass = norm
			}
			q := url.Values{}
			if v := strings.TrimSpace(*serverID); v != "" {
				q.Set("server_id", v)
			}
			if executionClass != "" {
				q.Set("execution_class", executionClass)
			}
			q.Set("limit", strconv.Itoa(*limit))
			q.Set("offset", strconv.Itoa(*offset))
			path := "/api/v1/recording/assignments/audit?" + q.Encode()
			payload := mustAPIGet(ctx, strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken), path)
			items, _ := payload["items"].([]any)
			if args[1] == "reconcile" && *apply {
				reconciled := 0
				for _, raw := range items {
					it := asMap(raw)
					issues, _ := it["issues"].([]any)
					if len(issues) == 0 {
						continue
					}
					streamID := int64FromAny(it["stream_id"])
					if streamID <= 0 {
						continue
					}
					mustAPIRequest(ctx, http.MethodPost, strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken), fmt.Sprintf("/api/v1/recording/streams/%d/unassign", streamID), map[string]any{
						"confirm": fmt.Sprintf("unassign:%d", streamID),
						"reason":  strings.TrimSpace(*reason),
						"actor":   strings.TrimSpace(*actor),
					})
					reconciled++
				}
				payload = mustAPIGet(ctx, strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken), path)
				payload["reconciled"] = reconciled
			}
			if *asJSON {
				printJSON(payload)
				return
			}
			invalidTotal := int64FromAny(payload["invalid_total"])
			fmt.Printf("assignment_audit total=%v invalid=%d limit=%d offset=%d\n", payload["total"], invalidTotal, *limit, *offset)
			if args[1] == "reconcile" && *apply {
				fmt.Printf("reconciled=%v\n", payload["reconciled"])
			}
			for _, raw := range items {
				it := asMap(raw)
				issues, _ := it["issues"].([]any)
				issueCodes := make([]string, 0, len(issues))
				for _, issueRaw := range issues {
					issue := asMap(issueRaw)
					if code := strings.TrimSpace(fmt.Sprint(issue["code"])); code != "" {
						issueCodes = append(issueCodes, code)
					}
				}
				if len(issueCodes) == 0 && args[1] == "audit" {
					continue
				}
				fmt.Printf(
					"stream_id=%v server_id=%v assignment_execution_class=%v requested_execution_class=%v recording_state=%v capture_type=%v stream_execution_class=%v issues=%s last_frame=%v slug=%v\n",
					it["stream_id"], it["server_id"], it["assignment_execution_class"], it["requested_execution_class"], it["recording_state"], it["stream_capture_type"], it["stream_execution_class"], strings.Join(issueCodes, ","), it["last_frame_at"], it["stream_slug"],
				)
			}
			return
		}
		fs := flag.NewFlagSet("servers assignments", flag.ExitOnError)
		serverID := fs.String("server-id", "", "optional server id filter")
		executionClassRaw := fs.String("execution-class", "", "optional execution class filter youtube_direct|video_live|image_poll")
		limit := fs.Int("limit", 500, "row limit")
		offset := fs.Int("offset", 0, "row offset")
		backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
		apiToken := fs.String("api-token", cfg.APIToken, "backend API token")
		asJSON := fs.Bool("json", false, "print JSON")
		_ = fs.Parse(args[1:])
		if *limit <= 0 || *limit > 2000 {
			log.Fatalf("--limit must be between 1 and 2000")
		}
		if *offset < 0 {
			log.Fatalf("--offset must be >= 0")
		}
		executionClass := strings.TrimSpace(*executionClassRaw)
		if executionClass != "" {
			norm, ok := capture.NormalizeExecutionClass(executionClass)
			if !ok {
				log.Fatalf("--execution-class must be one of youtube_direct|video_live|image_poll")
			}
			if norm == capture.ExecutionClassYouTubeRelay {
				norm = capture.ExecutionClassYouTubeDirect
			}
			executionClass = norm
		}
		q := url.Values{}
		if v := strings.TrimSpace(*serverID); v != "" {
			q.Set("server_id", v)
		}
		if executionClass != "" {
			q.Set("execution_class", executionClass)
		}
		q.Set("limit", strconv.Itoa(*limit))
		q.Set("offset", strconv.Itoa(*offset))
		path := "/api/v1/recording/assignments?" + q.Encode()
		payload := mustAPIGet(ctx, strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken), path)
		if *asJSON {
			printJSON(payload)
			return
		}
		items, _ := payload["items"].([]any)
		fmt.Printf("assignments=%d limit=%d offset=%d\n", len(items), *limit, *offset)
		for _, raw := range items {
			it, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			fmt.Printf(
				"stream_id=%v server_id=%v execution_class=%v revision=%v provider=%v slug=%v assigned_at=%v last_frame=%v\n",
				it["stream_id"], it["server_id"], it["execution_class"], it["assignment_revision"], it["provider"], it["stream_slug"], it["assigned_at"], it["last_frame_at"],
			)
		}
	case "capacity":
		if len(args) < 2 {
			log.Fatalf("usage: stoaramactl servers capacity <list|groups|heartbeat|stopped> ...")
		}
		switch args[1] {
		case "list":
			fs := flag.NewFlagSet("servers capacity list", flag.ExitOnError)
			includeInactive := fs.Bool("include-inactive", false, "include inactive/stale capacity rows")
			backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
			apiToken := fs.String("api-token", cfg.APIToken, "backend API token")
			asJSON := fs.Bool("json", false, "print JSON")
			_ = fs.Parse(args[2:])
			path := fmt.Sprintf("/api/v1/dashboard/recording/server-capacity?include_inactive=%t", *includeInactive)
			payload := mustAPIGet(ctx, strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken), path)
			if *asJSON {
				printJSON(payload)
				return
			}
			items, _ := payload["items"].([]any)
			fmt.Printf("capacity_rows=%d include_inactive=%t\n", len(items), *includeInactive)
			for _, raw := range items {
				it, ok := raw.(map[string]any)
				if !ok {
					continue
				}
				group := strings.TrimSpace(fmt.Sprint(it["capacity_group"]))
				if group == "" || group == "<nil>" {
					group = strings.TrimSpace(fmt.Sprint(it["execution_class"]))
				}
				executionClasses := "-"
				if arr, ok := it["execution_classes"].([]any); ok && len(arr) > 0 {
					parts := make([]string, 0, len(arr))
					for _, rawExecutionClass := range arr {
						v := strings.TrimSpace(fmt.Sprint(rawExecutionClass))
						if v != "" && v != "<nil>" {
							parts = append(parts, v)
						}
					}
					sort.Strings(parts)
					if len(parts) > 0 {
						executionClasses = strings.Join(parts, ",")
					}
				}
				availableExecutionClasses := "-"
				if arr, ok := it["available_execution_classes"].([]any); ok && len(arr) > 0 {
					parts := make([]string, 0, len(arr))
					for _, rawExecutionClass := range arr {
						v := strings.TrimSpace(fmt.Sprint(rawExecutionClass))
						if v != "" && v != "<nil>" {
							parts = append(parts, v)
						}
					}
					sort.Strings(parts)
					if len(parts) > 0 {
						availableExecutionClasses = strings.Join(parts, ",")
					}
				}
				fmt.Printf(
					"server_id=%v group=%s execution_classes=%s available=%s active=%v draining=%v assigned=%v/%v free=%v lease_expires_at=%v\n",
					it["server_id"], group, executionClasses, availableExecutionClasses, it["active"], it["draining"], it["assigned_count"], it["max_active"], it["free_slots"], it["lease_expires_at"],
				)
			}
		case "groups":
			fs := flag.NewFlagSet("servers capacity groups", flag.ExitOnError)
			backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
			apiToken := fs.String("api-token", cfg.APIToken, "backend API token")
			asJSON := fs.Bool("json", false, "print JSON")
			_ = fs.Parse(args[2:])
			payload := mustAPIGet(ctx, strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken), "/api/v1/dashboard/recording/capacity")
			if *asJSON {
				printJSON(payload)
				return
			}
			items, _ := payload["items"].([]any)
			fmt.Printf("capacity_groups=%d\n", len(items))
			for _, raw := range items {
				it, ok := raw.(map[string]any)
				if !ok {
					continue
				}
				group := strings.TrimSpace(fmt.Sprint(it["capacity_group"]))
				if group == "" || group == "<nil>" {
					group = strings.TrimSpace(fmt.Sprint(it["execution_class"]))
				}
				executionClasses := "-"
				if arr, ok := it["execution_classes"].([]any); ok && len(arr) > 0 {
					parts := make([]string, 0, len(arr))
					for _, rawExecutionClass := range arr {
						v := strings.TrimSpace(fmt.Sprint(rawExecutionClass))
						if v != "" && v != "<nil>" {
							parts = append(parts, v)
						}
					}
					sort.Strings(parts)
					if len(parts) > 0 {
						executionClasses = strings.Join(parts, ",")
					}
				}
				fmt.Printf("group=%s execution_classes=%s max_active=%v active=%v workers=%v managed=%v source=%v updated_at=%v\n",
					group, executionClasses, it["max_active"], it["active"], it["active_workers"], it["managed"], it["capacity_source"], it["updated_at"])
			}
		case "set", "set-bulk":
			log.Fatalf("servers capacity %s is removed; capacity is heartbeat-managed. Use `stoaramactl servers capacity heartbeat` from the server process path instead", args[1])
		case "heartbeat":
			fs := flag.NewFlagSet("servers capacity heartbeat", flag.ExitOnError)
			serverID := fs.String("server-id", "", "server id")
			captureSharedCapacity := fs.Int("capture-shared-capacity", 0, "max active video_live streams")
			executionClassCapacityRaw := fs.String("execution-class-capacity", "", "comma-separated execution_class=max_active list (e.g. video_live=8,youtube_direct=2)")
			drainingExecutionClassesRaw := fs.String("draining-execution-classes", "", "optional comma-separated execution classes to mark draining")
			leaseSec := fs.Int("lease-sec", 45, "heartbeat lease seconds")
			metadataJSON := fs.String("metadata-json", "{}", "metadata JSON object")
			backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
			apiToken := fs.String("api-token", cfg.APIToken, "backend API token")
			asJSON := fs.Bool("json", false, "print JSON")
			_ = fs.Parse(args[2:])
			if strings.TrimSpace(*serverID) == "" {
				log.Fatalf("--server-id is required")
			}
			if *leaseSec <= 0 || *leaseSec > 3600 {
				log.Fatalf("--lease-sec must be between 1 and 3600")
			}
			executionClassCapacity := map[string]int{}
			if *captureSharedCapacity > 0 {
				executionClassCapacity[capture.ExecutionClassVideoLive] = *captureSharedCapacity
			}
			if strings.TrimSpace(*executionClassCapacityRaw) != "" {
				if len(executionClassCapacity) > 0 {
					log.Fatalf("use either --capture-shared-capacity or --execution-class-capacity, not both")
				}
				parsedExecutionClassCapacity, err := parseExecutionClassCapacityCSV(*executionClassCapacityRaw)
				if err != nil {
					log.Fatalf("parse --execution-class-capacity: %v", err)
				}
				executionClassCapacity = parsedExecutionClassCapacity
			}
			if len(executionClassCapacity) == 0 {
				log.Fatalf("one of --capture-shared-capacity or --execution-class-capacity is required")
			}
			drainingExecutionClasses, err := parseExecutionClassSetCSV(*drainingExecutionClassesRaw)
			if err != nil {
				log.Fatalf("parse --draining-execution-classes: %v", err)
			}
			meta := map[string]any{}
			if raw := strings.TrimSpace(*metadataJSON); raw != "" {
				if err := json.Unmarshal([]byte(raw), &meta); err != nil {
					log.Fatalf("invalid --metadata-json: %v", err)
				}
			}
			executionClasses := make([]string, 0, len(executionClassCapacity))
			for executionClass := range executionClassCapacity {
				executionClasses = append(executionClasses, executionClass)
			}
			sort.Strings(executionClasses)
			items := make([]map[string]any, 0, len(executionClasses))
			for _, executionClass := range executionClasses {
				_, draining := drainingExecutionClasses[executionClass]
				items = append(items, map[string]any{
					"execution_class": executionClass,
					"max_active":      executionClassCapacity[executionClass],
					"draining":        draining,
				})
			}
			payload := mustAPIRequest(ctx, http.MethodPost, strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken), "/api/v1/recording/servers/heartbeat", map[string]any{
				"server_id":         strings.TrimSpace(*serverID),
				"lease_sec":         *leaseSec,
				"execution_classes": items,
				"metadata_json":     meta,
			})
			if *asJSON {
				printJSON(payload)
				return
			}
			fmt.Printf("server_id=%v heartbeat_ok=%v execution_classes=%v\n", payload["server_id"], payload["ok"], payload["execution_classes"])
		case "stopped":
			fs := flag.NewFlagSet("servers capacity stopped", flag.ExitOnError)
			serverID := fs.String("server-id", "", "server id")
			backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
			apiToken := fs.String("api-token", cfg.APIToken, "backend API token")
			asJSON := fs.Bool("json", false, "print JSON")
			_ = fs.Parse(args[2:])
			if strings.TrimSpace(*serverID) == "" {
				log.Fatalf("--server-id is required")
			}
			payload := mustAPIRequest(ctx, http.MethodPost, strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken), "/api/v1/recording/servers/stopped", map[string]any{
				"server_id": strings.TrimSpace(*serverID),
			})
			if *asJSON {
				printJSON(payload)
				return
			}
			fmt.Printf("server_id=%s stopped=%v\n", strings.TrimSpace(*serverID), payload["ok"])
		default:
			log.Fatalf("unknown servers capacity subcommand: %s", args[1])
		}
	default:
		log.Fatalf("unknown servers subcommand: %s", args[0])
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

func runWorkerHeartbeatLoop(ctx context.Context, client *captureapi.Client, req captureapi.WorkerHeartbeatRequest, interval time.Duration) error {
	if client == nil {
		return fmt.Errorf("capture api client is nil")
	}
	if strings.TrimSpace(req.WorkerID) == "" {
		return fmt.Errorf("worker heartbeat requires worker_id")
	}
	if req.Capacity <= 0 {
		return fmt.Errorf("worker heartbeat requires capacity > 0")
	}
	if _, ok := capture.NormalizeExecutionClass(req.ExecutionClass); !ok {
		return fmt.Errorf("worker heartbeat requires valid execution_class")
	}
	if interval <= 0 {
		interval = 15 * time.Second
	}
	if req.LeaseSec <= 0 {
		req.LeaseSec = int((3 * interval).Seconds())
	}
	retryDelay := 5 * time.Second
	if retryDelay > interval {
		retryDelay = interval
	}
	send := func() error {
		hbCtx, hbCancel := context.WithTimeout(ctx, 10*time.Second)
		defer hbCancel()
		return client.WorkerHeartbeat(hbCtx, req)
	}
	consecutiveFailures := 0
	var firstFailureAt time.Time
	nextDelay := time.Duration(0)
	for {
		if nextDelay > 0 {
			timer := time.NewTimer(nextDelay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
		err := send()
		if err != nil {
			consecutiveFailures++
			if firstFailureAt.IsZero() {
				firstFailureAt = time.Now().UTC()
			}
			log.Printf(
				"worker heartbeat loop: heartbeat failed consecutive=%d degraded_for=%s: %v",
				consecutiveFailures,
				time.Since(firstFailureAt).Round(time.Second),
				err,
			)
			nextDelay = retryDelay
			continue
		}
		if consecutiveFailures > 0 {
			log.Printf(
				"worker heartbeat loop: recovered after %d consecutive failures degraded_for=%s",
				consecutiveFailures,
				time.Since(firstFailureAt).Round(time.Second),
			)
			consecutiveFailures = 0
			firstFailureAt = time.Time{}
		}
		nextDelay = interval
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
}

func runRecordingServerHeartbeatLoop(ctx context.Context, client *captureapi.Client, req captureapi.RecordingServerHeartbeatRequest, interval time.Duration) error {
	if client == nil {
		return fmt.Errorf("capture api client is nil")
	}
	if strings.TrimSpace(req.ServerID) == "" {
		return fmt.Errorf("recording server heartbeat requires server_id")
	}
	if len(req.ExecutionClasses) == 0 {
		return fmt.Errorf("recording server heartbeat requires execution_classes")
	}
	if interval <= 0 {
		interval = 15 * time.Second
	}
	if req.LeaseSec <= 0 {
		req.LeaseSec = int((3 * interval).Seconds())
	}
	retryDelay := 5 * time.Second
	if retryDelay > interval {
		retryDelay = interval
	}
	send := func() error {
		hbCtx, hbCancel := context.WithTimeout(ctx, 10*time.Second)
		defer hbCancel()
		return client.RecordingServerHeartbeat(hbCtx, req)
	}
	consecutiveFailures := 0
	var firstFailureAt time.Time
	nextDelay := time.Duration(0)
	for {
		if nextDelay > 0 {
			timer := time.NewTimer(nextDelay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
		err := send()
		if err != nil {
			consecutiveFailures++
			if firstFailureAt.IsZero() {
				firstFailureAt = time.Now().UTC()
			}
			log.Printf(
				"recording server heartbeat loop: heartbeat failed consecutive=%d degraded_for=%s: %v",
				consecutiveFailures,
				time.Since(firstFailureAt).Round(time.Second),
				err,
			)
			nextDelay = retryDelay
			continue
		}
		if consecutiveFailures > 0 {
			log.Printf(
				"recording server heartbeat loop: recovered after %d consecutive failures degraded_for=%s",
				consecutiveFailures,
				time.Since(firstFailureAt).Round(time.Second),
			)
			consecutiveFailures = 0
			firstFailureAt = time.Time{}
		}
		nextDelay = interval
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
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

func parseModeCSV(v string) ([]capture.Mode, error) {
	parts := splitCSV(v)
	out := make([]capture.Mode, 0, len(parts))
	for _, p := range parts {
		mode := capture.NormalizeMode(p)
		if mode == capture.ModeUnsupported {
			return nil, fmt.Errorf("invalid capture mode %q", p)
		}
		out = append(out, mode)
	}
	return out, nil
}

func parseModeSetCSV(v string) (map[capture.Mode]struct{}, error) {
	parts := splitCSV(v)
	out := make(map[capture.Mode]struct{}, len(parts))
	for _, p := range parts {
		mode := capture.NormalizeMode(p)
		if mode == capture.ModeAuto || mode == capture.ModeUnsupported {
			return nil, fmt.Errorf("invalid capture mode %q", p)
		}
		out[mode] = struct{}{}
	}
	return out, nil
}

func parseModeCapacityCSV(v string) (map[capture.Mode]int, error) {
	parts := splitCSV(v)
	out := make(map[capture.Mode]int, len(parts))
	for _, part := range parts {
		eq := strings.Index(part, "=")
		if eq <= 0 || eq >= len(part)-1 {
			return nil, fmt.Errorf("invalid mode capacity %q, expected mode=max_active", part)
		}
		modeRaw := strings.TrimSpace(part[:eq])
		mode := capture.NormalizeMode(modeRaw)
		if mode == capture.ModeAuto || mode == capture.ModeUnsupported {
			return nil, fmt.Errorf("invalid mode %q", modeRaw)
		}
		nRaw := strings.TrimSpace(part[eq+1:])
		n, err := strconv.Atoi(nRaw)
		if err != nil {
			return nil, fmt.Errorf("invalid capacity for mode %s: %w", mode, err)
		}
		if n < 0 {
			return nil, fmt.Errorf("capacity must be >= 0 for mode %s", mode)
		}
		out[mode] = n
	}
	return out, nil
}

func parseCaptureServerExecutionClassesCSV(v string) ([]capture.Mode, error) {
	parts := splitCSV(v)
	out := make([]capture.Mode, 0, len(parts)*2)
	seen := make(map[capture.Mode]struct{}, len(parts)*2)
	for _, part := range parts {
		executionClass, ok := capture.NormalizeExecutionClass(part)
		if !ok {
			return nil, fmt.Errorf("invalid execution_class %q", part)
		}
		var modes []capture.Mode
		switch executionClass {
		case capture.ExecutionClassVideoLive:
			modes = []capture.Mode{capture.ModeHLSLive, capture.ModeFFmpegDirect}
		default:
			return nil, fmt.Errorf("execution_class %s is unsupported for capture-server", executionClass)
		}
		for _, mode := range modes {
			if _, ok := seen[mode]; ok {
				continue
			}
			seen[mode] = struct{}{}
			out = append(out, mode)
		}
	}
	return out, nil
}

func parseCaptureServerExecutionClassesSetCSV(v string) (map[capture.Mode]struct{}, error) {
	modes, err := parseCaptureServerExecutionClassesCSV(v)
	if err != nil {
		return nil, err
	}
	out := make(map[capture.Mode]struct{}, len(modes))
	for _, mode := range modes {
		out[mode] = struct{}{}
	}
	return out, nil
}

func parseExecutionClassSetCSV(v string) (map[string]struct{}, error) {
	parts := splitCSV(v)
	out := make(map[string]struct{}, len(parts))
	for _, p := range parts {
		executionClass, ok := capture.NormalizeExecutionClass(p)
		if !ok {
			return nil, fmt.Errorf("invalid execution_class %q", p)
		}
		out[executionClass] = struct{}{}
	}
	return out, nil
}

func parseExecutionClassCapacityCSV(v string) (map[string]int, error) {
	parts := splitCSV(v)
	out := make(map[string]int, len(parts))
	for _, part := range parts {
		eq := strings.Index(part, "=")
		if eq <= 0 || eq >= len(part)-1 {
			return nil, fmt.Errorf("invalid execution_class capacity %q, expected execution_class=max_active", part)
		}
		executionClassRaw := strings.TrimSpace(part[:eq])
		executionClass, ok := capture.NormalizeExecutionClass(executionClassRaw)
		if !ok {
			return nil, fmt.Errorf("invalid execution_class %q", executionClassRaw)
		}
		nRaw := strings.TrimSpace(part[eq+1:])
		n, err := strconv.Atoi(nRaw)
		if err != nil {
			return nil, fmt.Errorf("invalid capacity for execution_class %s: %w", executionClass, err)
		}
		if n < 0 {
			return nil, fmt.Errorf("capacity must be >= 0 for execution_class %s", executionClass)
		}
		out[executionClass] = n
	}
	return out, nil
}

func defaultBackendAPIURL() string {
	if v := strings.TrimSpace(os.Getenv("BACKEND_API_URL")); v != "" {
		return v
	}
	return strings.TrimSpace(os.Getenv("INFERCTL_API_URL"))
}

func envIntOrDefault(key string, fallback int) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		log.Fatalf("invalid %s: %v", key, err)
	}
	return n
}

func defaultString(v string, fallback string) string {
	if strings.TrimSpace(v) == "" {
		return fallback
	}
	return v
}

func localServerID(hostname string, fallbackWorkerID string) string {
	fallback := strings.TrimSpace(fallbackWorkerID)
	if fallback != "" {
		return strings.ToLower(sanitizeFilename(fallback))
	}
	host := strings.TrimSpace(hostname)
	if host != "" {
		if i := strings.IndexByte(host, '.'); i > 0 {
			host = host[:i]
		}
		host = sanitizeFilename(host)
		if host != "" {
			return strings.ToLower(host)
		}
	}
	return "local-runner"
}

func defaultCaptureServerWorkerID(fallback string) string {
	host := ""
	if raw, err := os.Hostname(); err == nil {
		host = strings.TrimSpace(raw)
	}
	if host != "" {
		if i := strings.IndexByte(host, '.'); i > 0 {
			host = host[:i]
		}
		host = sanitizeFilename(host)
		if host != "" {
			return "capture-server-" + strings.ToLower(host)
		}
	}
	fallback = strings.TrimSpace(fallback)
	if fallback == "" {
		fallback = "capture-server"
	}
	return "capture-server-" + sanitizeFilename(fallback)
}

func defaultCaptureServerID(fallbackWorkerID string) string {
	if rawID := strings.TrimSpace(doMetadataValue("id")); rawID != "" {
		return "do-" + sanitizeFilename(rawID)
	}
	if rawName := strings.TrimSpace(doMetadataValue("hostname")); rawName != "" {
		name := sanitizeFilename(rawName)
		if name != "" {
			return "do-" + strings.ToLower(name)
		}
	}
	host := ""
	if raw, err := os.Hostname(); err == nil {
		host = strings.TrimSpace(raw)
	}
	return localServerID(host, fallbackWorkerID)
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

func arrayLen(v any) int {
	raw, ok := v.([]any)
	if !ok {
		return 0
	}
	return len(raw)
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
