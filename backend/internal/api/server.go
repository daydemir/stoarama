package api

import (
	"archive/zip"
	"context"
	"encoding/base64"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/daydemir/stoarama/backend/internal/billing"
	"github.com/daydemir/stoarama/backend/internal/capture"
	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/email"
	"github.com/daydemir/stoarama/backend/internal/model"
	"github.com/daydemir/stoarama/backend/internal/queue"
	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/daydemir/stoarama/backend/internal/secretbox"
	"github.com/daydemir/stoarama/backend/internal/settings"
	"github.com/daydemir/stoarama/backend/internal/storage"
	"github.com/daydemir/stoarama/backend/internal/util"
)

type Server struct {
	cfg           config.Config
	pool          *pgxpool.Pool
	r2            *r2.Client
	secrets       *secretbox.Cipher
	mailer        email.Sender
	streamsHTML    []byte
	recordingHTML  []byte
	recordingsHTML []byte
	accountHTML    []byte
	docsHTML       []byte
	adminHTML      []byte
	billing        *billing.Client
	exportMu       sync.Mutex
	frameExports  map[string]*frameExportJob
	dayZipMu      sync.Mutex
	dayZips       map[string]*dayZipJob
	dayZipSlot    chan struct{}
}

const accountSessionCookie = "stoarama_session"

const (
	frameExportMaxFrames = 5000
	frameExportMaxBytes  = int64(2 << 30)
	dayZipMaxItems       = 5000
	dayZipMaxBytes       = int64(6 << 30)
)

type dayZipJob struct {
	ID          string     `json:"id"`
	StreamID    int64      `json:"stream_id"`
	Day         string     `json:"day"`
	Status      string     `json:"status"`
	ZipKey      string     `json:"zip_key,omitempty"`
	DownloadURL string     `json:"download_url,omitempty"`
	SizeBytes   int64      `json:"size_bytes"`
	ItemCount   int        `json:"item_count"`
	Processed   int        `json:"processed"`
	ErrorText   string     `json:"error_text,omitempty"`
	StartedAt   *time.Time `json:"started_at,omitempty"`
	FinishedAt  *time.Time `json:"finished_at,omitempty"`
}

type frameExportJob struct {
	ID            string     `json:"id"`
	StreamID      int64      `json:"stream_id"`
	CapturedFrom  time.Time  `json:"captured_from"`
	CapturedTo    time.Time  `json:"captured_to"`
	CaptureStatus string     `json:"capture_status"`
	Status        string     `json:"status"`
	FrameCount    int        `json:"frame_count"`
	TotalBytes    int64      `json:"total_bytes"`
	ObjectKey     string     `json:"object_key,omitempty"`
	ErrorText     string     `json:"error_text,omitempty"`
	CreatedAt     time.Time  `json:"created_at"`
	StartedAt     *time.Time `json:"started_at,omitempty"`
	FinishedAt    *time.Time `json:"finished_at,omitempty"`
}

type frameExportCreateRequest struct {
	CapturedFrom  string `json:"captured_from"`
	CapturedTo    string `json:"captured_to"`
	CaptureStatus string `json:"capture_status"`
}

type frameExportRow struct {
	ID         int64
	CapturedAt time.Time
	Status     string
	ErrorText  *string
	ObjectKey  *string
	MIMEType   *string
	SizeBytes  int64
}

func NewRouter(cfg config.Config, pool *pgxpool.Pool, r2c *r2.Client, mailer email.Sender) (http.Handler, error) {
	streamsHTML, err := loadStreamsHTML()
	if err != nil {
		return nil, err
	}
	recordingHTML, err := loadRecordingHTML()
	if err != nil {
		return nil, err
	}
	accountHTML, err := loadAccountHTML()
	if err != nil {
		return nil, err
	}
	docsHTML, err := loadDocsHTML()
	if err != nil {
		return nil, err
	}
	adminHTML, err := loadAdminHTML()
	if err != nil {
		return nil, err
	}
	recordingsHTML, err := loadRecordingsHTML()
	if err != nil {
		return nil, err
	}
	s := &Server{
		cfg:            cfg,
		pool:           pool,
		r2:             r2c,
		mailer:         mailer,
		streamsHTML:    streamsHTML,
		recordingHTML:  recordingHTML,
		recordingsHTML: recordingsHTML,
		accountHTML:    accountHTML,
		docsHTML:       docsHTML,
		adminHTML:      adminHTML,
		frameExports:   map[string]*frameExportJob{},
		dayZips:        map[string]*dayZipJob{},
		dayZipSlot:     make(chan struct{}, 1),
	}
	if key := strings.TrimSpace(cfg.StorageCredKey); key != "" {
		cipher, err := secretbox.NewFromBase64Key(key)
		if err != nil {
			return nil, fmt.Errorf("init storage credential cipher: %w", err)
		}
		s.secrets = cipher
	}
	if strings.TrimSpace(cfg.StripeSecretKey) != "" && strings.TrimSpace(cfg.StripeWebhookSecret) != "" && strings.TrimSpace(cfg.StripePriceID) != "" {
		bc, err := billing.New(cfg.StripeSecretKey, cfg.StripePriceID, cfg.AppBaseURL, cfg.StripeLivemode)
		if err != nil {
			return nil, fmt.Errorf("init stripe billing client: %w", err)
		}
		s.billing = bc
	}
	return s.router(), nil
}

func (s *Server) router() http.Handler {
	r := chi.NewRouter()

	r.Get("/healthz", s.handleHealth)
	r.Get("/", s.handleStreamsApp)
	r.Get("/streams", s.handleStreamsApp)
	r.Get("/streams/{id}", s.handleStreamsApp)
	r.Get("/recording", s.handleRecordingApp)
	r.Get("/korea", s.handleKoreaApp)
	r.Get("/docs", s.handleDocsRoot)
	r.Get("/docs/getting-started", s.handleDocsApp)
	r.Get("/docs/api", s.handleDocsApp)
	r.Get("/docs/relay-guide", s.redirectLegacyRelayGuide)
	r.Get("/docs/self-serve", s.handleDocsApp)
	r.Get("/account", s.handleAccountApp)
	r.Get("/recordings", s.handleRecordingsApp)
	r.Get("/admin", s.handleAdminApp)
	r.Get("/dashboard", s.redirectDashboard)
	r.Get("/dashboard/{tab}", s.redirectDashboard)
	r.Get("/dashboard/stream/{id}", s.redirectDashboard)
	r.Get("/auth/complete", s.handleAccountAuthComplete)
	r.Post("/webhooks/email/resend", s.handleResendWebhook)
	r.Post("/webhooks/billing/stripe", s.handleStripeWebhook)

	r.Route("/api/v1", func(api chi.Router) {
		api.Post("/auth/request-link", s.handleAccountAuthRequestLink)
		api.Post("/nodes/enroll", s.handleNodeEnroll)
		api.Route("/account", func(account chi.Router) {
			account.Use(s.requireAccountAuth)
			account.Get("/me", s.handleAccountMe)
			account.Post("/logout", s.handleAccountLogout)
			account.Get("/api-keys", s.handleAccountAPIKeysList)
			account.Post("/api-keys", s.handleAccountAPIKeysCreate)
			account.Post("/api-keys/{id}/revoke", s.handleAccountAPIKeyRevoke)
			account.Get("/streams/{id}/clips/availability", s.handleAccountStreamClipsAvailability)
			account.Get("/streams/{id}/clips/range", s.handleAccountStreamClipsRange)
			account.Get("/streams/{id}/clips", s.handleAccountStreamClipsList)
			account.Post("/clips/download-prepare", s.handleAccountClipDownloadPrepare)
			account.Get("/storage-destinations", s.handleAccountStorageDestinationsList)
			account.Post("/storage-destinations", s.handleAccountStorageDestinationsCreate)
			account.Delete("/storage-destinations/{id}", s.handleAccountStorageDestinationDelete)
			account.Get("/recordings", s.handleAccountRecordingsList)
			account.Post("/recordings", s.handleAccountRecordingsCreate)
			account.Post("/recordings/probe", s.handleAccountRecordingsProbe)
			account.Get("/recordings/{id}", s.handleAccountRecordingGet)
			account.Post("/recordings/{id}/pause", s.handleAccountRecordingPause)
			account.Post("/recordings/{id}/resume", s.handleAccountRecordingResume)
			account.Delete("/recordings/{id}", s.handleAccountRecordingDelete)
			account.Get("/billing", s.handleAccountBillingMe)
			account.Post("/billing/checkout", s.handleAccountBillingCheckout)
			account.Post("/billing/portal", s.handleAccountBillingPortal)
			account.Get("/nodes", s.handleAccountNodesList)
			account.Get("/node-enrollment-tokens", s.handleAccountNodeEnrollmentTokensList)
			account.Post("/node-enrollment-tokens", s.handleAccountNodeEnrollmentTokensCreate)
			account.Post("/node-enrollment-tokens/{id}/revoke", s.handleAccountNodeEnrollmentTokenRevoke)
			account.Post("/pipelines/sync", s.handlePipelinesSync)
			account.Get("/pipelines", s.handlePipelinesList)
			account.Post("/pipeline-versions/sync", s.handlePipelineVersionsSync)
			account.Get("/pipeline-versions", s.handlePipelineVersionsList)
			account.Post("/pipeline-runs", s.handlePipelineRunsCreate)
			account.Get("/pipeline-runs", s.handlePipelineRunsList)
			account.Get("/pipeline-runs/{id}", s.handlePipelineRunGet)
			account.Get("/pipeline-runs/{id}/targets", s.handlePipelineRunTargetsList)
			account.Post("/eval-suites/sync", s.handleEvalSuitesSync)
			account.Get("/eval-suites", s.handleEvalSuitesList)
			account.Get("/eval-suites/{id}", s.handleEvalSuiteGet)
			account.Post("/pipeline-experiments/sync", s.handlePipelineExperimentsSync)
			account.Get("/pipeline-experiments", s.handlePipelineExperimentsList)
			account.Get("/pipeline-experiments/{id}", s.handlePipelineExperimentGet)
			account.Post("/pipeline-experiment-iterations/sync", s.handlePipelineExperimentIterationsSync)
		})
		api.Route("/node", func(node chi.Router) {
			node.Use(s.requireNodeAuth)
			node.Get("/me", s.handleNodeMe)
			node.Post("/heartbeat", s.handleNodeHeartbeat)
			node.Get("/pipeline-runs/{id}", s.handlePipelineRunGet)
			node.Get("/pipeline-runs/{id}/targets", s.handlePipelineRunTargetsList)
			node.Post("/pipeline-runs/{id}/claims", s.handlePipelineRunClaims)
			node.Post("/processing/worker-heartbeat", s.handleProcessingWorkerHeartbeat)
			node.Post("/processing/worker-stopped", s.handleProcessingWorkerStopped)
			node.Post("/inference/commit", s.handleInferenceCommit)
			node.Post("/inference/fail", s.handleInferenceFail)
		})
		api.Route("/admin", func(admin chi.Router) {
			admin.Use(s.requireAdminAuth)
			admin.Get("/accounts", s.handleAdminAccountsList)
			admin.Post("/accounts/{id}/disable", s.handleAdminAccountDisable)
			admin.Post("/accounts/{id}/enable", s.handleAdminAccountEnable)
			admin.Post("/accounts/{id}/promote-admin", s.handleAdminAccountPromote)
			admin.Post("/accounts/{id}/demote-admin", s.handleAdminAccountDemote)
			admin.Get("/accounts/{id}/api-keys", s.handleAdminAccountAPIKeys)
			admin.Post("/api-keys/{id}/revoke", s.handleAdminAPIKeyRevoke)
		})

		api.Group(func(public chi.Router) {
			public.Get("/data-access-spec", s.handleDataAccessSpec)
			public.Get("/source-candidates", s.handleSourceCandidatesList)
			public.Get("/streams/{id}/clips/availability", s.handlePublicStreamClipsAvailability)
			public.Get("/streams/{id}/clips/range", s.handlePublicStreamClipsRange)
			public.Get("/streams/{id}/clips", s.handlePublicStreamClipsList)
			public.Post("/clips/download-prepare", s.handlePublicClipDownloadPrepare)
			public.Get("/recording/assignments", s.handleRecordingAssignmentsList)
			public.Get("/pipelines", s.handlePipelinesList)
			public.Get("/pipeline-versions", s.handlePipelineVersionsList)
			public.Get("/pipeline-experiments/{id}", s.handlePipelineExperimentGet)
			public.Get("/capture/schema", s.handleCaptureSchema)
			public.Get("/frames", s.handleFramesList)
			public.Get("/capture/streams/{id}/segments", s.handleCaptureStreamSegmentsList)
			public.Get("/capture/streams/{id}/segments/latest", s.handleCaptureStreamSegmentLatest)
			public.Post("/capture/streams/{id}/day-zip", s.handleDayZipCreate)
			public.Get("/capture/day-zip/{jobId}", s.handleDayZipGet)
			public.Get("/korea/inventory", s.handleKoreaInventory)
			public.Get("/dashboard/overview", s.handleDashboardOverview)
			public.Get("/dashboard/streams", s.handleDashboardStreams)
			public.Get("/dashboard/countries", s.handleDashboardCountries)
			public.Get("/dashboard/cities", s.handleDashboardCities)
			public.Get("/dashboard/sources", s.handleDashboardSources)
			public.Get("/dashboard/youtube-channels", s.handleDashboardYouTubeChannels)
			public.Get("/dashboard/tags", s.handleDashboardTags)
			public.Post("/dashboard/streams/image-urls", s.handleDashboardStreamImageURLs)
			public.Get("/dashboard/inference", s.handleDashboardInference)
			public.Get("/dashboard/recording/settings", s.handleDashboardRecordingSettingsGet)
			public.Get("/dashboard/recording/capacity", s.handleDashboardRecordingCapacityList)
			public.Get("/dashboard/recording/server-capacity", s.handleDashboardRecordingServerCapacity)
			public.Get("/dashboard/recording/summary", s.handleDashboardRecordingSummary)
			public.Get("/dashboard/servers", s.handleDashboardServers)
			public.Get("/dashboard/pipelines/overview", s.handleDashboardPipelinesOverview)
			public.Get("/dashboard/pipelines/{pipeline_id}", s.handleDashboardPipelineDetail)
			public.Get("/dashboard/pipelines/{pipeline_id}/streams", s.handleDashboardPipelineStreams)
			public.Get("/dashboard/queue-health", s.handleDashboardQueueHealth)
			public.Get("/dashboard/streams/{id}", s.handleDashboardStreamDetail)
			public.Get("/dashboard/streams/{id}/pipelines", s.handleDashboardStreamPipelinesList)
			public.Get("/dashboard/streams/{id}/timeline", s.handleDashboardStreamTimeline)
			public.Get("/dashboard/streams/{id}/coverage", s.handleDashboardStreamCoverage)
			public.Get("/dashboard/streams/{id}/recording-health", s.handleDashboardStreamRecordingHealth)
			public.Get("/dashboard/streams/{id}/capture-samples", s.handleDashboardStreamCaptureSamples)
			public.Get("/dashboard/streams/{id}/detections", s.handleDashboardStreamDetections)
			public.Get("/dashboard/streams/{id}/recording", s.handleDashboardStreamRecording)
			public.Get("/dashboard/streams/{id}/frame-manifest", s.handleDashboardStreamFrameManifest)
			public.Get("/dashboard/streams/{id}/frame-exports/{export_id}", s.handleDashboardStreamFrameExportGet)
		})

		api.Group(func(admin chi.Router) {
			admin.Use(s.requireAdminAuth)

			admin.Post("/streams", s.handleStreamsCreate)
			admin.Patch("/streams/{id}", s.handleStreamsPatch)
			admin.Get("/streams/{id}/source-revisions", s.handleStreamSourceRevisionsList)
			admin.Get("/streams", s.handleStreamsList)
			admin.Post("/source-candidates/{id}/review", s.handleSourceCandidateReview)
			admin.Post("/source-candidates/{id}/import", s.handleSourceCandidateImport)
			admin.Get("/recording/assignments/audit", s.handleRecordingAssignmentsAudit)
			admin.Get("/recorder-pool", s.handleAdminRecorderPool)
			admin.Get("/recording/supervision", s.handleRecordingSupervisionStatus)
			admin.Get("/recording/incidents", s.handleRecordingIncidentsList)
			admin.Get("/recording/alert-deliveries", s.handleAlertDeliveryEventsList)
			admin.Patch("/streams/{id}/capture", s.handleStreamsCapturePatch)
			admin.Post("/pipelines/sync", s.handlePipelinesSync)
			admin.Post("/pipeline-versions/sync", s.handlePipelineVersionsSync)
			admin.Post("/pipeline-runs", s.handlePipelineRunsCreate)
			admin.Get("/pipeline-runs", s.handlePipelineRunsList)
			admin.Get("/pipeline-runs/{id}", s.handlePipelineRunGet)
			admin.Get("/pipeline-runs/{id}/targets", s.handlePipelineRunTargetsList)
			admin.Post("/pipeline-runs/{id}/claims", s.handlePipelineRunClaims)
			admin.Post("/eval-suites/sync", s.handleEvalSuitesSync)
			admin.Get("/eval-suites", s.handleEvalSuitesList)
			admin.Get("/eval-suites/{id}", s.handleEvalSuiteGet)
			admin.Post("/pipeline-experiments/sync", s.handlePipelineExperimentsSync)
			admin.Get("/pipeline-experiments", s.handlePipelineExperimentsList)
			admin.Post("/pipeline-experiment-iterations/sync", s.handlePipelineExperimentIterationsSync)
			admin.Post("/dashboard/inference/cleanup-unboxed", s.handleDashboardInferenceCleanupUnboxed)
			admin.Put("/dashboard/recording/settings", s.handleDashboardRecordingSettingsPut)
			admin.Put("/dashboard/streams/{id}/pipelines/{pipeline_id}", s.handleDashboardStreamPipelineUpsert)
			admin.Post("/dashboard/streams/{id}/frame-exports", s.handleDashboardStreamFrameExportCreate)
		})

		api.Post("/recording/streams/{id}/state", s.handleRecordingStreamState)

		api.Group(func(recordingWrites chi.Router) {
			recordingWrites.Use(s.requireRecordingMutationAuth)
			recordingWrites.Post("/recording/streams/{id}/assign", s.handleRecordingStreamAssign)
			recordingWrites.Post("/recording/streams/{id}/unassign", s.handleRecordingStreamUnassign)
		})

		api.Group(func(rec chi.Router) {
			rec.Use(s.requireRecorderNodeAuth)

			rec.Post("/recording/jobs/lease", s.handleRecordingJobsLease)
			rec.Post("/recording/upload-intents", s.handleRecordingUploadIntent)
			rec.Post("/recording/clips/ingest", s.handleRecordingClipIngest)
			rec.Post("/recording/droplets/heartbeat", s.handleRecordingDropletHeartbeat)
			rec.Post("/recording/jobs/{id}/heartbeat", s.handleRecordingJobHeartbeat)
			rec.Post("/recording/jobs/{id}/complete", s.handleRecordingJobComplete)
			rec.Post("/recording/jobs/{id}/fail", s.handleRecordingJobFail)
		})

		api.Group(func(worker chi.Router) {
			worker.Use(s.requireServiceOrLocalRecorderNodeAuth)

			worker.Get("/recording/settings", s.handleServiceRecordingSettingsGet)
			worker.Put("/recording/settings", s.handleDashboardRecordingSettingsPut)
			worker.Get("/service/recording/assignments", s.handleRecordingAssignmentsList)
			worker.Post("/recording/servers/heartbeat", s.handleRecordingServerHeartbeat)
			worker.Post("/recording/servers/stopped", s.handleRecordingServerStopped)
			worker.Get("/service/dashboard/streams/{id}/recording-health", s.handleDashboardStreamRecordingHealth)
			worker.Get("/capture/streams", s.handleCaptureStreams)
			worker.Get("/capture/streams/{id}", s.handleCaptureStreamDetail)
			worker.Get("/capture/runtime", s.handleCaptureRuntime)
			worker.Post("/capture/runtime/stopped", s.handleCaptureRuntimeStopped)
			worker.Post("/capture/worker-heartbeat", s.handleCaptureWorkerHeartbeat)
			worker.Post("/capture/worker-stopped", s.handleCaptureWorkerStopped)
			worker.Post("/capture/jobs/enqueue-due", s.handleCaptureJobsEnqueueDue)
			worker.Post("/capture/jobs/lease", s.handleCaptureJobsLease)
			worker.Post("/capture/jobs/{id}/complete", s.handleCaptureJobsComplete)
			worker.Post("/capture/jobs/{id}/complete-without-next", s.handleCaptureJobsCompleteWithoutNext)
			worker.Post("/capture/jobs/{id}/fail", s.handleCaptureJobsFail)
			worker.Post("/recording/process/heartbeat", s.handleRecordingProcessHeartbeat)
			worker.Post("/recording/process/stopped", s.handleRecordingProcessStopped)
			worker.Post("/capture/ingest", s.handleCaptureIngest)
			worker.Post("/capture/mark-unsupported", s.handleCaptureMarkUnsupported)
			worker.Post("/media/upload-intents", s.handleUploadIntents)
		})

		api.Group(func(service chi.Router) {
			service.Use(s.requireServiceAuth)

			service.Post("/node-enrollment-tokens", s.handleServiceNodeEnrollmentTokensCreate)
			service.Post("/source-candidates", s.handleSourceCandidatesUpsert)
			service.Post("/source-candidates/{id}/runs", s.handleSourceCandidateRunCreate)
			service.Post("/source-candidates/{id}/auto-import", s.handleServiceSourceCandidateAutoImport)
			service.Post("/pipelines/sync", s.handlePipelinesSync)
			service.Get("/pipelines", s.handlePipelinesList)
			service.Post("/pipeline-versions/sync", s.handlePipelineVersionsSync)
			service.Get("/pipeline-versions", s.handlePipelineVersionsList)
			service.Post("/pipeline-runs", s.handlePipelineRunsCreate)
			service.Get("/pipeline-runs", s.handlePipelineRunsList)
			service.Get("/pipeline-runs/{id}", s.handlePipelineRunGet)
			service.Get("/pipeline-runs/{id}/targets", s.handlePipelineRunTargetsList)
			service.Post("/pipeline-runs/{id}/claims", s.handlePipelineRunClaims)
			service.Post("/eval-suites/sync", s.handleEvalSuitesSync)
			service.Get("/eval-suites", s.handleEvalSuitesList)
			service.Get("/eval-suites/{id}", s.handleEvalSuiteGet)
			service.Post("/pipeline-experiments/sync", s.handlePipelineExperimentsSync)
			service.Get("/pipeline-experiments", s.handlePipelineExperimentsList)
			service.Get("/pipeline-experiments/{id}", s.handlePipelineExperimentGet)
			service.Post("/pipeline-experiment-iterations/sync", s.handlePipelineExperimentIterationsSync)
			service.Post("/imports/streams", s.handleServiceStreamImport)
			service.Post("/imports/frames", s.handleServiceFrameImport)
			service.Post("/imports/streams/repair-canonical-capture", s.handleServiceStreamCanonicalCaptureRepair)
			service.Post("/imports/streams/repair-image-capture", s.handleServiceStreamImageCaptureRepair)
			service.Post("/imports/streams/recording-state", s.handleServiceStreamRecordingState)
			service.Post("/service/streams/{id}/tags", s.handleServiceStreamTagsAdd)
			service.Get("/recording/supervision", s.handleRecordingSupervisionStatus)
			service.Get("/recording/incidents", s.handleRecordingIncidentsList)
			service.Get("/recording/alert-deliveries", s.handleAlertDeliveryEventsList)
			service.Get("/service/capture/catalog/candidates", s.handleServiceCaptureCatalogCandidates)
			service.Post("/processing/worker-heartbeat", s.handleProcessingWorkerHeartbeat)
			service.Post("/processing/worker-stopped", s.handleProcessingWorkerStopped)
			service.Post("/inference/claims", s.handleInferenceClaims)
			service.Post("/inference/claims/abandon", s.handleInferenceClaimsAbandon)
			service.Post("/inference/commit", s.handleInferenceCommit)
			service.Post("/inference/fail", s.handleInferenceFail)
		})
	})

	return r
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	util.WriteJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func requestIsHTTPS(r *http.Request) bool {
	if r == nil {
		return false
	}
	if r.TLS != nil {
		return true
	}
	v := strings.TrimSpace(strings.ToLower(r.Header.Get("X-Forwarded-Proto")))
	return v == "https"
}

func normalizeCaptureTypeInput(raw string) (string, error) {
	captureType, ok := capture.NormalizeCaptureType(raw)
	if !ok {
		return "", fmt.Errorf("invalid capture_type")
	}
	return captureType, nil
}

func normalizeExecutionClassInput(raw string) (string, error) {
	executionClass, ok := capture.NormalizeExecutionClass(raw)
	if !ok {
		return "", fmt.Errorf("invalid execution_class")
	}
	if executionClass == capture.ExecutionClassYouTubeRelay {
		return "", fmt.Errorf("execution_class youtube_relay is gone; use youtube_direct")
	}
	return executionClass, nil
}

func normalizeSourceFamilyInput(raw string) (string, error) {
	sourceFamily, ok := capture.NormalizeSourceFamily(raw)
	if !ok {
		return "", fmt.Errorf("invalid source_family")
	}
	return sourceFamily, nil
}

type streamCreateRequest struct {
	Provider                 string         `json:"provider"`
	ExternalID               string         `json:"external_id"`
	Name                     string         `json:"name"`
	Slug                     string         `json:"slug"`
	StreamURL                string         `json:"source_url"`
	SourcePageURL            string         `json:"source_page_url"`
	SourceFamily             string         `json:"source_family"`
	Lat                      *float64       `json:"lat"`
	Lon                      *float64       `json:"lon"`
	LocationText             string         `json:"location_text"`
	LocationCountry          string         `json:"location_country"`
	LocationCountryCode      string         `json:"location_country_code"`
	LocationRegion           string         `json:"location_region"`
	LocationCity             string         `json:"location_city"`
	LocationLocality         string         `json:"location_locality"`
	LocationSource           string         `json:"location_source"`
	MetadataJSON             map[string]any `json:"metadata_json"`
	RecordingState           string         `json:"recording_state"`
	CaptureMode              string         `json:"capture_type"`
	ExecutionClass           string         `json:"execution_class"`
	ExpectedFPS              *float64       `json:"expected_fps"`
	ExpectedImageIntervalSec *int           `json:"expected_image_interval_sec"`
	CaptureConfigJSON        map[string]any `json:"execution_config_json"`
	Tags                     []string       `json:"tags"`
}

func (s *Server) handleStreamsCreate(w http.ResponseWriter, r *http.Request) {
	if !s.requireIdempotency(w, r, "POST:/api/v1/streams") {
		return
	}
	var req streamCreateRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	stream, err := s.createStreamRecord(r.Context(), req)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	util.WriteJSON(w, http.StatusCreated, stream)
}

type streamPatchRequest struct {
	Name                     *string         `json:"name"`
	Slug                     *string         `json:"slug"`
	StreamURL                *string         `json:"source_url"`
	SourcePageURL            *string         `json:"source_page_url"`
	SourceChangeReason       *string         `json:"source_change_reason"`
	SourceFamily             *string         `json:"source_family"`
	Lat                      *float64        `json:"lat"`
	Lon                      *float64        `json:"lon"`
	LocationText             *string         `json:"location_text"`
	LocationCountry          *string         `json:"location_country"`
	LocationCountryCode      *string         `json:"location_country_code"`
	LocationRegion           *string         `json:"location_region"`
	LocationCity             *string         `json:"location_city"`
	LocationLocality         *string         `json:"location_locality"`
	LocationSource           *string         `json:"location_source"`
	MetadataJSON             *map[string]any `json:"metadata_json"`
	RecordingState           *string         `json:"recording_state"`
	CaptureMode              *string         `json:"capture_type"`
	ExecutionClass           *string         `json:"execution_class"`
	ExpectedFPS              *float64        `json:"expected_fps"`
	ExpectedImageIntervalSec *int            `json:"expected_image_interval_sec"`
	CaptureConfigJSON        *map[string]any `json:"execution_config_json"`
	Tags                     *[]string       `json:"tags"`
}

func (s *Server) handleStreamsPatch(w http.ResponseWriter, r *http.Request) {
	id, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	var req streamPatchRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("begin update stream tx: %v", err))
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()
	current, err := s.loadStreamForAssignmentTx(r.Context(), tx, id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			util.WriteError(w, http.StatusNotFound, "stream not found")
			return
		}
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load stream: %v", err))
		return
	}
	sets := make([]string, 0, 16)
	args := make([]any, 0, 16)
	add := func(col string, v any) {
		args = append(args, v)
		sets = append(sets, fmt.Sprintf("%s=$%d", col, len(args)))
	}
	if req.Name != nil {
		add("name", strings.TrimSpace(*req.Name))
	}
	if req.Slug != nil {
		add("slug", slugify(*req.Slug))
	}
	if req.LocationText != nil {
		add("location_text", strings.TrimSpace(*req.LocationText))
	}
	if req.LocationCountry != nil {
		add("location_country", strings.TrimSpace(*req.LocationCountry))
	}
	if req.LocationCountryCode != nil {
		add("location_country_code", strings.ToUpper(strings.TrimSpace(*req.LocationCountryCode)))
	}
	if req.LocationRegion != nil {
		add("location_region", strings.TrimSpace(*req.LocationRegion))
	}
	if req.LocationCity != nil {
		add("location_city", strings.TrimSpace(*req.LocationCity))
	}
	if req.LocationLocality != nil {
		add("location_locality", strings.TrimSpace(*req.LocationLocality))
	}
	if req.LocationSource != nil {
		add("location_source", strings.TrimSpace(*req.LocationSource))
	}
	if req.MetadataJSON != nil {
		b, err := json.Marshal(nonNilMap(*req.MetadataJSON))
		if err != nil {
			util.WriteError(w, http.StatusBadRequest, fmt.Sprintf("invalid metadata_json: %v", err))
			return
		}
		add("metadata_jsonb", b)
	}
	recordingStateChanged := false
	targetRecordingState := current.RecordingState
	targetExecutionClass := strings.TrimSpace(current.ExecutionClass)
	sourceChangeReason := "stream source updated"
	if req.SourceChangeReason != nil && strings.TrimSpace(*req.SourceChangeReason) != "" {
		sourceChangeReason = strings.TrimSpace(*req.SourceChangeReason)
	}
	if req.RecordingState != nil {
		state, ok := parseRecordingState(strings.TrimSpace(*req.RecordingState))
		if !ok {
			util.WriteError(w, http.StatusBadRequest, "invalid recording_state; expected off|on")
			return
		}
		targetRecordingState = state
		recordingStateChanged = targetRecordingState != current.RecordingState
		add("recording_state", string(targetRecordingState))
	}
	captureProfileChanged := req.StreamURL != nil || req.SourcePageURL != nil || req.SourceFamily != nil || req.CaptureMode != nil || req.ExecutionClass != nil || req.CaptureConfigJSON != nil || req.ExpectedFPS != nil || req.ExpectedImageIntervalSec != nil
	if captureProfileChanged {
		sourceURL := current.SourceURL
		if req.StreamURL != nil {
			sourceURL = strings.TrimSpace(*req.StreamURL)
		}
		sourcePageURL := current.SourcePageURL
		if req.SourcePageURL != nil {
			sourcePageURL = strings.TrimSpace(*req.SourcePageURL)
		}
		captureTypeRaw := current.CaptureType
		if req.CaptureMode != nil {
			captureTypeRaw = strings.TrimSpace(*req.CaptureMode)
		}
		sourceFamilyRaw := current.SourceFamily
		if req.SourceFamily != nil {
			sourceFamilyRaw = strings.TrimSpace(*req.SourceFamily)
		}
		executionClassRaw := current.ExecutionClass
		if req.ExecutionClass != nil {
			executionClassRaw = strings.TrimSpace(*req.ExecutionClass)
		}
		executionConfig := current.ExecutionConfigJSON
		if req.CaptureConfigJSON != nil {
			executionConfig = nonNilMap(*req.CaptureConfigJSON)
		}
		expectedFPSOverride := current.ExpectedFPS
		if req.ExpectedFPS != nil {
			expectedFPSOverride = req.ExpectedFPS
		}
		expectedImageIntervalOverride := current.ExpectedImageInterval
		if req.ExpectedImageIntervalSec != nil {
			expectedImageIntervalOverride = req.ExpectedImageIntervalSec
		}
		profile, err := capture.DeriveCaptureProfile(current.Provider, sourceURL, sourcePageURL, captureTypeRaw, sourceFamilyRaw, executionClassRaw, executionConfig, expectedFPSOverride, expectedImageIntervalOverride)
		if err != nil {
			util.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
		if req.StreamURL != nil {
			add("source_url", profile.SourceURL)
		}
		if req.SourcePageURL != nil {
			add("source_page_url", profile.SourcePageURL)
		}
		add("source_family", profile.SourceFamily)
		add("capture_type", profile.CaptureType)
		add("execution_class", profile.ExecutionClass)
		targetExecutionClass = strings.TrimSpace(profile.ExecutionClass)
		add("capture_family", profile.CaptureFamily)
		add("expected_fps", profile.ExpectedFPS)
		add("expected_image_interval_sec", profile.ExpectedImageIntervalSec)
	}
	if req.CaptureConfigJSON != nil {
		b, err := json.Marshal(nonNilMap(*req.CaptureConfigJSON))
		if err != nil {
			util.WriteError(w, http.StatusBadRequest, fmt.Sprintf("invalid execution_config_json: %v", err))
			return
		}
		add("execution_config_jsonb", b)
	}
	if req.Tags != nil {
		add("tags", dedupeStrings(*req.Tags))
	}
	if req.Lat != nil {
		add("lat", *req.Lat)
	}
	if req.Lon != nil {
		add("lon", *req.Lon)
	}
	if targetRecordingState == model.RecordingStateOn {
		if normalizedExecutionClass, ok := capture.NormalizeExecutionClass(targetExecutionClass); ok && normalizedExecutionClass == capture.ExecutionClassImagePoll {
			util.WriteError(w, http.StatusBadRequest, "image_poll recording is deprecated; clip-native recording only")
			return
		}
	}
	if len(sets) == 0 {
		util.WriteError(w, http.StatusBadRequest, "no fields to update")
		return
	}
	if recordingStateChanged {
		if _, err := tx.Exec(r.Context(), `SELECT set_config('app.recording_actor', $1, true), set_config('app.recording_reason', $2, true)`, "api.streams_patch", "stream patch"); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("set recording audit context: %v", err))
			return
		}
	}
	args = append(args, id)
	query := fmt.Sprintf(`UPDATE streams SET %s, updated_at=now() WHERE id=$%d`, strings.Join(sets, ", "), len(args))
	res, err := tx.Exec(r.Context(), query, args...)
	if err != nil {
		util.WriteError(w, http.StatusConflict, fmt.Sprintf("update stream: %v", err))
		return
	}
	if res.RowsAffected() == 0 {
		util.WriteError(w, http.StatusNotFound, "stream not found")
		return
	}

	updated, err := s.loadStreamForAssignmentTx(r.Context(), tx, id)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("reload stream: %v", err))
		return
	}
	shouldAutoAssignRecording := updated.RecordingState == model.RecordingStateOn &&
		((recordingStateChanged && targetRecordingState == model.RecordingStateOn) || captureProfileChanged || req.RecordingState != nil)
	result, status, err := s.reconcileStreamRecordingAssignments(
		r.Context(),
		tx,
		updated.ID,
		"api.streams_patch",
		"recording enabled",
		sourceChangeReason,
		current,
		updated,
		shouldAutoAssignRecording,
	)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if status > 0 {
		util.WriteJSON(w, status, result)
		return
	}
	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("commit stream update: %v", err))
		return
	}
	stream, err := s.getStreamByID(r.Context(), id)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	util.WriteJSON(w, http.StatusOK, stream)
}

func (s *Server) reconcileStreamRecordingAssignments(
	ctx context.Context,
	tx pgx.Tx,
	streamID int64,
	actor string,
	assignReason string,
	sourceChangeReason string,
	current model.Stream,
	updated model.Stream,
	shouldAutoAssign bool,
) (map[string]any, int, error) {
	assignment, existed, err := loadRecordingAssignmentTx(ctx, tx, streamID)
	if err != nil {
		return nil, 0, fmt.Errorf("load assignment: %w", err)
	}
	sourceChanged := streamSourceChanged(current, updated)
	if sourceChanged {
		metadata := map[string]any{}
		if existed {
			metadata["assignment_server_id"] = assignment.ServerID
			metadata["assignment_revision"] = assignment.Revision
		}
		if err := insertStreamSourceRevisionTx(ctx, tx, streamSourceRevisionInput{
			Actor:    actor,
			Reason:   sourceChangeReason,
			Previous: current,
			Current:  updated,
			Metadata: metadata,
		}); err != nil {
			return nil, 0, fmt.Errorf("insert stream source revision: %w", err)
		}
	}
	if existed {
		issues := buildRecordingAssignmentAuditIssues(updated, assignment, nil)
		if len(issues) > 0 {
			if _, _, err := s.unassignRecordingStreamTx(ctx, tx, streamID, actor, issues[0].Code); err != nil {
				return nil, 0, fmt.Errorf("clear invalid assignment: %w", err)
			}
			existed = false
		}
	}
	if shouldAutoAssign && updated.RecordingState == model.RecordingStateOn && !existed {
		result, status, err := s.assignRecordingStreamTx(ctx, tx, updated, "", "", actor, assignReason)
		if err != nil {
			return nil, 0, err
		}
		return result, status, nil
	}
	return nil, 0, nil
}

func (s *Server) handleStreamsList(w http.ResponseWriter, r *http.Request) {
	limit := parseIntQuery(r, "limit", 200, 1, 2000)
	offset := parseIntQuery(r, "offset", 0, 0, 1000000)
	includeLatest := parseBoolQueryPtr(r, "include_latest")

	where := []string{"1=1"}
	args := []any{}
	if raw := strings.TrimSpace(strings.ToLower(r.URL.Query().Get("recording_state"))); raw != "" {
		state, ok := parseRecordingState(raw)
		if !ok {
			util.WriteError(w, http.StatusBadRequest, "invalid recording_state; expected off|on")
			return
		}
		args = append(args, string(state))
		where = append(where, fmt.Sprintf("s.recording_state=$%d", len(args)))
	}
	if raw := strings.TrimSpace(strings.ToLower(r.URL.Query().Get("capture_type"))); raw != "" {
		captureType, err := normalizeCaptureTypeInput(raw)
		if err != nil {
			util.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
		args = append(args, captureType)
		where = append(where, fmt.Sprintf("s.capture_type=$%d", len(args)))
	}
	if tag := strings.TrimSpace(r.URL.Query().Get("tag")); tag != "" {
		args = append(args, tag)
		where = append(where, fmt.Sprintf("$%d = ANY(s.tags)", len(args)))
	}

	if includeLatest != nil && *includeLatest {
		args = append(args, limit, offset)
		rows, err := s.pool.Query(r.Context(), fmt.Sprintf(`
			SELECT
				s.id, s.provider, s.external_id, s.name, s.slug, s.source_url, s.source_page_url,
				s.source_family,
				s.capture_family, s.expected_fps, s.expected_image_interval_sec,
				s.lat, s.lon, s.location_text, s.location_country, s.location_country_code, s.location_region, s.location_city, s.location_locality, s.location_source, s.metadata_jsonb,
				s.recording_state, s.recording_failed_reason, s.recording_failed_at, s.capture_type, s.execution_class, s.execution_config_jsonb, s.tags,
				s.created_at, s.updated_at,
				lf.frame_id,
				lf.captured_at,
				lf.capture_status,
				mo.object_key
			FROM streams s
			LEFT JOIN LATERAL (
				SELECT f.id AS frame_id, f.captured_at, f.capture_status, f.raw_media_object_id
				FROM frames f
				WHERE f.stream_id = s.id
				ORDER BY f.captured_at DESC, f.id DESC
				LIMIT 1
			) lf ON true
				LEFT JOIN media_objects mo ON mo.id = lf.raw_media_object_id
				WHERE %s
				ORDER BY CASE s.recording_state WHEN 'on' THEN 0 ELSE 1 END ASC, s.id ASC
				LIMIT $%d OFFSET $%d
			`, strings.Join(where, " AND "), len(args)-1, len(args)), args...)
		if err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("list streams: %v", err))
			return
		}
		defer rows.Close()
		type rowItem struct {
			Stream            model.Stream `json:"stream"`
			LatestFrameID     *int64       `json:"latest_frame_id,omitempty"`
			LatestCapturedAt  *time.Time   `json:"latest_captured_at,omitempty"`
			LatestCapture     *string      `json:"latest_capture_status,omitempty"`
			LatestFrameURL    string       `json:"latest_frame_url,omitempty"`
			LatestFrameObject string       `json:"latest_frame_object_key,omitempty"`
		}
		items := make([]rowItem, 0, limit)
		for rows.Next() {
			var stream model.Stream
			var metaBytes []byte
			var cfgBytes []byte
			var frameID *int64
			var capturedAt *time.Time
			var captureStatus *string
			var objectKey *string
			var state string
			if err := rows.Scan(
				&stream.ID, &stream.Provider, &stream.ExternalID, &stream.Name, &stream.Slug, &stream.SourceURL, &stream.SourcePageURL,
				&stream.SourceFamily,
				&stream.CaptureFamily, &stream.ExpectedFPS, &stream.ExpectedImageInterval,
				&stream.Lat, &stream.Lon, &stream.LocationText, &stream.LocationCountry, &stream.LocationCountryCode, &stream.LocationRegion, &stream.LocationCity, &stream.LocationLocality, &stream.LocationSource, &metaBytes,
				&state, &stream.RecordingFailedReason, &stream.RecordingFailedAt, &stream.CaptureType, &stream.ExecutionClass, &cfgBytes, &stream.Tags,
				&stream.CreatedAt, &stream.UpdatedAt,
				&frameID, &capturedAt, &captureStatus, &objectKey,
			); err != nil {
				util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan stream row: %v", err))
				return
			}
			stream.RecordingState = model.RecordingState(state)
			if err := decodeStreamPayload(&stream, metaBytes, cfgBytes); err != nil {
				util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("decode stream metadata: %v", err))
				return
			}
			if err := s.loadRuntimeIntoStream(r.Context(), &stream); err != nil {
				util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load stream runtime: %v", err))
				return
			}
			item := rowItem{
				Stream:           stream,
				LatestFrameID:    frameID,
				LatestCapturedAt: capturedAt,
				LatestCapture:    captureStatus,
			}
			if objectKey != nil && *objectKey != "" {
				item.LatestFrameObject = *objectKey
				url, err := s.r2.PresignGet(r.Context(), *objectKey, s.cfg.R2SignGetTTL)
				if err == nil {
					item.LatestFrameURL = url
				}
			}
			items = append(items, item)
		}
		if rows.Err() != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("iterate streams: %v", rows.Err()))
			return
		}
		util.WriteJSON(w, http.StatusOK, map[string]any{"items": items, "limit": limit, "offset": offset})
		return
	}

	args = append(args, limit, offset)
	rows, err := s.pool.Query(r.Context(), fmt.Sprintf(`
		SELECT
			s.id, s.provider, s.external_id, s.name, s.slug, s.source_url, s.source_page_url,
			s.source_family,
			s.capture_family, s.expected_fps, s.expected_image_interval_sec,
			s.lat, s.lon, s.location_text, s.location_country, s.location_country_code, s.location_region, s.location_city, s.location_locality, s.location_source, s.metadata_jsonb,
			s.recording_state, s.recording_failed_reason, s.recording_failed_at, s.capture_type, s.execution_class, s.execution_config_jsonb, s.tags,
			s.created_at, s.updated_at
		FROM streams s
		WHERE %s
		ORDER BY CASE s.recording_state WHEN 'on' THEN 0 ELSE 1 END ASC, s.id ASC
		LIMIT $%d OFFSET $%d
	`, strings.Join(where, " AND "), len(args)-1, len(args)), args...)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("list streams: %v", err))
		return
	}
	defer rows.Close()
	items := make([]model.Stream, 0, limit)
	for rows.Next() {
		stream, metaBytes, cfgBytes, err := scanStream(rows)
		if err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan stream: %v", err))
			return
		}
		if err := decodeStreamPayload(&stream, metaBytes, cfgBytes); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("decode stream metadata: %v", err))
			return
		}
		if err := s.loadRuntimeIntoStream(r.Context(), &stream); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load stream runtime: %v", err))
			return
		}
		items = append(items, stream)
	}
	if rows.Err() != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("iterate streams: %v", rows.Err()))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"items": items, "limit": limit, "offset": offset})
}

type streamCapturePatchRequest struct {
	CaptureMode              string         `json:"capture_type"`
	ExecutionClass           string         `json:"execution_class"`
	SourceFamily             string         `json:"source_family"`
	ExpectedFPS              *float64       `json:"expected_fps"`
	ExpectedImageIntervalSec *int           `json:"expected_image_interval_sec"`
	CaptureConfigJSON        map[string]any `json:"execution_config_json"`
}

func (s *Server) handleStreamsCapturePatch(w http.ResponseWriter, r *http.Request) {
	id, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("begin update stream capture patch tx: %v", err))
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()
	current, err := s.loadStreamForAssignmentTx(r.Context(), tx, id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			util.WriteError(w, http.StatusNotFound, "stream not found")
			return
		}
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load stream: %v", err))
		return
	}
	var req streamCapturePatchRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	captureType, err := normalizeCaptureTypeInput(req.CaptureMode)
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	executionClass := strings.TrimSpace(req.ExecutionClass)
	if executionClass == "" {
		executionClass = capture.DefaultExecutionClassForCaptureType(captureType)
	}
	if executionClass, err = normalizeExecutionClassInput(executionClass); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	sourceFamily := strings.TrimSpace(req.SourceFamily)
	if sourceFamily == "" {
		sourceFamily = capture.DefaultSourceFamilyForCaptureType(captureType)
	}
	if sourceFamily, err = normalizeSourceFamilyInput(sourceFamily); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	profile, err := capture.DeriveCaptureProfile(current.Provider, current.SourceURL, current.SourcePageURL, captureType, sourceFamily, executionClass, nonNilMap(req.CaptureConfigJSON), req.ExpectedFPS, req.ExpectedImageIntervalSec)
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	cfgBytes, err := json.Marshal(nonNilMap(req.CaptureConfigJSON))
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, fmt.Sprintf("invalid execution_config_json: %v", err))
		return
	}
	if normalizedExecutionClass, ok := capture.NormalizeExecutionClass(profile.ExecutionClass); ok && normalizedExecutionClass == capture.ExecutionClassImagePoll {
		if current.RecordingState == model.RecordingStateOn {
			util.WriteError(w, http.StatusBadRequest, "image_poll recording is deprecated; clip-native recording only")
			return
		}
	}
	_, err = tx.Exec(r.Context(), `
		UPDATE streams
		SET source_family=$2, capture_type=$3, execution_class=$4, capture_family=$5, expected_fps=$6, expected_image_interval_sec=$7, execution_config_jsonb=$8, updated_at=now()
		WHERE id=$1
	`, id, profile.SourceFamily, profile.CaptureType, profile.ExecutionClass, profile.CaptureFamily, profile.ExpectedFPS, profile.ExpectedImageIntervalSec, cfgBytes)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("update stream capture: %v", err))
		return
	}
	updated, err := s.loadStreamForAssignmentTx(r.Context(), tx, id)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("reload stream: %v", err))
		return
	}
	result, status, err := s.reconcileStreamRecordingAssignments(
		r.Context(),
		tx,
		updated.ID,
		"api.streams_capture_patch",
		"capture profile updated",
		"stream capture updated",
		current,
		updated,
		updated.RecordingState == model.RecordingStateOn,
	)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if status > 0 {
		util.WriteJSON(w, status, result)
		return
	}
	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("commit stream capture patch: %v", err))
		return
	}
	stream, err := s.getStreamByID(r.Context(), id)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	util.WriteJSON(w, http.StatusOK, stream)
}

type pipelineSpec struct {
	ID             string         `json:"id"`
	OwnerAccountID *int64         `json:"owner_account_id,omitempty"`
	OwnerEmail     string         `json:"owner_email,omitempty"`
	PipelineFamily string         `json:"pipeline_family"`
	Kind           string         `json:"kind"`
	SpecJSON       map[string]any `json:"spec_json"`
	Active         *bool          `json:"active"`
}

type pipelineSyncRequest struct {
	Pipelines []pipelineSpec `json:"pipelines"`
}

func (s *Server) handlePipelinesSync(w http.ResponseWriter, r *http.Request) {
	if !s.requireIdempotency(w, r, "POST:/api/v1/pipelines/sync") {
		return
	}
	var req pipelineSyncRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if len(req.Pipelines) == 0 {
		util.WriteError(w, http.StatusBadRequest, "pipelines is required")
		return
	}
	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("begin tx: %v", err))
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()

	for i := range req.Pipelines {
		p := req.Pipelines[i]
		if strings.TrimSpace(p.ID) == "" {
			util.WriteError(w, http.StatusBadRequest, "pipeline id is required")
			return
		}
		pipelineFamily := strings.TrimSpace(strings.ToLower(p.PipelineFamily))
		if pipelineFamily == "" {
			pipelineFamily = "inference"
		}
		switch pipelineFamily {
		case "discovery", "metadata", "inference":
		default:
			util.WriteError(w, http.StatusBadRequest, fmt.Sprintf("invalid pipeline_family for %s", p.ID))
			return
		}
		kind := strings.TrimSpace(p.Kind)
		if kind == "" {
			kind = "detector"
		}
		active := true
		if p.Active != nil {
			active = *p.Active
		}
		specBytes, err := json.Marshal(nonNilMap(p.SpecJSON))
		if err != nil {
			util.WriteError(w, http.StatusBadRequest, fmt.Sprintf("invalid pipeline spec_json for %s: %v", p.ID, err))
			return
		}
		ownerAccountID, err := s.resolvePipelineOwnerForSync(r.Context(), strings.TrimSpace(p.ID), p.OwnerAccountID, p.OwnerEmail)
		if err != nil {
			writeAPIError(w, err)
			return
		}
		if _, err := tx.Exec(r.Context(), `
			INSERT INTO pipelines (id, owner_account_id, pipeline_family, kind, spec_jsonb, active)
			VALUES ($1,$2,$3,$4,$5,$6)
			ON CONFLICT (id)
			DO UPDATE SET
				owner_account_id=COALESCE(pipelines.owner_account_id, EXCLUDED.owner_account_id),
				pipeline_family=EXCLUDED.pipeline_family,
				kind=EXCLUDED.kind,
				spec_jsonb=EXCLUDED.spec_jsonb,
				active=EXCLUDED.active,
				updated_at=now()
		`, strings.TrimSpace(p.ID), ownerAccountID, pipelineFamily, kind, specBytes, active); err != nil {
			util.WriteError(w, http.StatusConflict, fmt.Sprintf("upsert pipeline %s: %v", p.ID, err))
			return
		}
	}
	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("commit tx: %v", err))
		return
	}
	s.handlePipelinesList(w, r)
}

func (s *Server) handlePipelinesList(w http.ResponseWriter, r *http.Request) {
	where := []string{"1=1"}
	args := []any{}
	if ownerAccountID, ok := pipelineOwnerAccountScope(r.Context()); ok {
		args = append(args, ownerAccountID)
		where = append(where, fmt.Sprintf("owner_account_id=$%d", len(args)))
	} else if owner := parseInt64QueryPtr(r, "owner_account_id"); owner != nil {
		args = append(args, *owner)
		where = append(where, fmt.Sprintf("owner_account_id=$%d", len(args)))
	}
	rows, err := s.pool.Query(r.Context(), fmt.Sprintf(`
		SELECT id, owner_account_id, pipeline_family, kind, spec_jsonb, active, created_at, updated_at
		FROM pipelines
		WHERE %s
		ORDER BY id ASC
	`, strings.Join(where, " AND ")), args...)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("list pipelines: %v", err))
		return
	}
	defer rows.Close()
	items := make([]model.Pipeline, 0, 128)
	for rows.Next() {
		var p model.Pipeline
		var specBytes []byte
		if err := rows.Scan(&p.ID, &p.OwnerAccountID, &p.PipelineFamily, &p.Kind, &specBytes, &p.Active, &p.CreatedAt, &p.UpdatedAt); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan pipeline: %v", err))
			return
		}
		if err := json.Unmarshal(specBytes, &p.SpecJSON); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("decode pipeline spec_json: %v", err))
			return
		}
		items = append(items, p)
	}
	if rows.Err() != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("iterate pipelines: %v", rows.Err()))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"items": items})
}

type inferenceClaimRequest struct {
	PipelineID string  `json:"pipeline_id"`
	StreamIDs  []int64 `json:"stream_ids"`
	StreamTag  string  `json:"stream_tag"`
	Limit      int     `json:"limit"`
	LeaseSec   int     `json:"lease_sec"`
	ClaimedBy  string  `json:"claimed_by"`
	ForceRerun bool    `json:"force_rerun"`
}

func (s *Server) handleInferenceClaims(w http.ResponseWriter, r *http.Request) {
	var req inferenceClaimRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if strings.TrimSpace(req.PipelineID) == "" {
		util.WriteError(w, http.StatusBadRequest, "pipeline_id is required")
		return
	}
	if strings.TrimSpace(req.ClaimedBy) == "" {
		util.WriteError(w, http.StatusBadRequest, "claimed_by is required")
		return
	}
	if req.Limit <= 0 {
		req.Limit = 100
	}
	if req.LeaseSec <= 0 {
		req.LeaseSec = 600
	}
	var pipelineExists bool
	if err := s.pool.QueryRow(r.Context(), `SELECT EXISTS(SELECT 1 FROM pipelines WHERE id=$1 AND active=true)`, strings.TrimSpace(req.PipelineID)).Scan(&pipelineExists); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("check pipeline: %v", err))
		return
	}
	if !pipelineExists {
		util.WriteError(w, http.StatusBadRequest, "active pipeline not found")
		return
	}
	claims, err := queue.ClaimFrames(r.Context(), s.pool, queue.ClaimFilter{
		PipelineID: strings.TrimSpace(req.PipelineID),
		StreamIDs:  req.StreamIDs,
		Tag:        strings.TrimSpace(req.StreamTag),
		Limit:      req.Limit,
		LeaseSec:   req.LeaseSec,
		ClaimedBy:  strings.TrimSpace(req.ClaimedBy),
		ForceRerun: req.ForceRerun,
	})
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	type claimResp struct {
		ClaimID           int64     `json:"claim_id"`
		FrameID           int64     `json:"frame_id"`
		StreamID          int64     `json:"stream_id"`
		CapturedAt        time.Time `json:"captured_at"`
		PipelineID        string    `json:"pipeline_id"`
		PipelineVersionID *int64    `json:"pipeline_version_id,omitempty"`
		PipelineRunID     *int64    `json:"pipeline_run_id,omitempty"`
		LeaseExpires      time.Time `json:"lease_expires_at"`
		ObjectKey         string    `json:"object_key"`
		MIMEType          string    `json:"mime_type"`
		SizeBytes         int64     `json:"size_bytes"`
		Width             int       `json:"width"`
		Height            int       `json:"height"`
		DownloadURL       string    `json:"download_url"`
		ClaimedBy         string    `json:"claimed_by"`
	}
	items := make([]claimResp, 0, len(claims))
	for _, c := range claims {
		url, err := s.r2.PresignGet(r.Context(), c.ObjectKey, s.cfg.R2SignGetTTL)
		if err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("presign frame url: %v", err))
			return
		}
		items = append(items, claimResp{
			ClaimID:           c.ClaimID,
			FrameID:           c.FrameID,
			StreamID:          c.StreamID,
			CapturedAt:        c.CapturedAt,
			PipelineID:        c.PipelineID,
			PipelineVersionID: c.PipelineVersionID,
			PipelineRunID:     c.PipelineRunID,
			LeaseExpires:      c.LeaseExpires,
			ObjectKey:         c.ObjectKey,
			MIMEType:          c.MIMEType,
			SizeBytes:         c.SizeBytes,
			Width:             c.Width,
			Height:            c.Height,
			DownloadURL:       url,
			ClaimedBy:         strings.TrimSpace(req.ClaimedBy),
		})
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"items": items})
}

type inferenceAbandonClaimsRequest struct {
	ExpiredOnly bool   `json:"expired_only"`
	PipelineID  string `json:"pipeline_id"`
}

func (s *Server) handleInferenceClaimsAbandon(w http.ResponseWriter, r *http.Request) {
	if !s.requireIdempotency(w, r, "POST:/api/v1/inference/claims/abandon") {
		return
	}
	var req inferenceAbandonClaimsRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	count, err := queue.AbandonClaims(r.Context(), s.pool, req.ExpiredOnly, strings.TrimSpace(req.PipelineID))
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"abandoned": count})
}

type uploadIntentRequest struct {
	Kind           string     `json:"kind"`
	PipelineID     string     `json:"pipeline_id"`
	StreamID       int64      `json:"stream_id"`
	FrameID        int64      `json:"frame_id"`
	MimeType       string     `json:"mime_type"`
	ExpectedETag   string     `json:"expected_etag"`
	SizeBytes      *int64     `json:"size_bytes"`
	SegmentStartAt *time.Time `json:"segment_start_at"`
}

func (s *Server) handleUploadIntents(w http.ResponseWriter, r *http.Request) {
	if !s.requireIdempotency(w, r, "POST:/api/v1/media/upload-intents") {
		return
	}
	var req uploadIntentRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	kind := strings.TrimSpace(req.Kind)
	if kind == "" {
		kind = "boxed"
	}
	if kind != "boxed" && kind != "capture_segment" && kind != "capture_segment_thumbnail" {
		util.WriteError(w, http.StatusBadRequest, "kind must be boxed, capture_segment, or capture_segment_thumbnail")
		return
	}
	if kind == "boxed" {
		if strings.TrimSpace(req.PipelineID) == "" {
			util.WriteError(w, http.StatusBadRequest, "pipeline_id is required")
			return
		}
		if req.StreamID <= 0 {
			if req.FrameID <= 0 {
				util.WriteError(w, http.StatusBadRequest, "stream_id or frame_id is required")
				return
			}
			if err := s.pool.QueryRow(r.Context(), `SELECT stream_id FROM frames WHERE id=$1`, req.FrameID).Scan(&req.StreamID); err != nil {
				util.WriteError(w, http.StatusBadRequest, fmt.Sprintf("frame not found: %v", err))
				return
			}
		}
	}
	if (kind == "capture_segment" || kind == "capture_segment_thumbnail") && req.StreamID <= 0 {
		util.WriteError(w, http.StatusBadRequest, "stream_id is required for capture segment uploads")
		return
	}
	if kind == "capture_segment_thumbnail" && (req.SegmentStartAt == nil || req.SegmentStartAt.IsZero()) {
		util.WriteError(w, http.StatusBadRequest, "segment_start_at is required for capture_segment_thumbnail")
		return
	}
	mimeType := strings.TrimSpace(req.MimeType)
	now := time.Now().UTC()
	intentID := uuid.New()
	objectKey := ""
	if kind == "boxed" {
		if mimeType == "" {
			mimeType = "image/jpeg"
		}
		ext := fileExtensionFromMIME(mimeType)
		if ext == "" {
			ext = ".jpg"
		}
		objectKey = fmt.Sprintf("boxed/pipeline/%s/stream/%d/%04d/%02d/%02d/%s%s",
			sanitizePathToken(req.PipelineID), req.StreamID,
			now.Year(), int(now.Month()), now.Day(), intentID.String(), ext)
	} else if kind == "capture_segment" {
		if mimeType == "" {
			mimeType = "video/mp4"
		}
		objectKey = buildCaptureSegmentObjectKey(req.StreamID, now, mimeType)
	} else {
		if mimeType == "" {
			mimeType = "image/jpeg"
		}
		objectKey = buildCaptureSegmentThumbnailObjectKey(req.StreamID, req.SegmentStartAt.UTC())
	}
	expiresAt := now.Add(s.cfg.R2SignPutTTL)

	if _, err := s.pool.Exec(r.Context(), `
		INSERT INTO upload_intents (id, kind, bucket, object_key, mime_type, expected_size_bytes, expected_etag, status, expires_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,'pending',$8)
	`, intentID, kind, s.cfg.R2Bucket, objectKey, mimeType, req.SizeBytes, strings.TrimSpace(req.ExpectedETag), expiresAt); err != nil {
		util.WriteError(w, http.StatusConflict, fmt.Sprintf("create upload intent: %v", err))
		return
	}
	putURL, err := s.r2.PresignPut(r.Context(), objectKey, mimeType, s.cfg.R2SignPutTTL)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("presign upload: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusCreated, map[string]any{
		"intent_id":    intentID.String(),
		"bucket":       s.cfg.R2Bucket,
		"object_key":   objectKey,
		"upload_url":   putURL,
		"expires_at":   expiresAt,
		"content_type": mimeType,
	})
}

type inferenceDetection struct {
	ClassID    string         `json:"class_id"`
	ClassName  string         `json:"class_name"`
	Confidence float64        `json:"confidence"`
	X1         float64        `json:"x1"`
	Y1         float64        `json:"y1"`
	X2         float64        `json:"x2"`
	Y2         float64        `json:"y2"`
	AreaPx     float64        `json:"area_px"`
	ExtraJSON  map[string]any `json:"extra_json"`
}

type inferenceSignal struct {
	SignalType string         `json:"signal_type"`
	SignalKey  string         `json:"signal_key"`
	Confidence *float64       `json:"confidence"`
	ValueNum   *float64       `json:"value_num"`
	ValueText  *string        `json:"value_text"`
	ExtraJSON  map[string]any `json:"extra_json"`
}

type inferenceCommitRequest struct {
	ClaimID           int64                `json:"claim_id"`
	PipelineID        string               `json:"pipeline_id"`
	PipelineRunID     int64                `json:"pipeline_run_id"`
	PipelineVersionID *int64               `json:"pipeline_version_id,omitempty"`
	FrameID           int64                `json:"frame_id"`
	ClaimedBy         string               `json:"claimed_by"`
	ForceRerun        bool                 `json:"force_rerun"`
	RevisionMode      string               `json:"revision_mode"`
	Status            string               `json:"status"`
	SummaryJSON       map[string]any       `json:"summary_json"`
	RawOutputJSON     map[string]any       `json:"raw_output_json"`
	RunnerInfoJSON    map[string]any       `json:"runner_info_json"`
	ErrorText         string               `json:"error_text"`
	StartedAt         *time.Time           `json:"started_at"`
	FinishedAt        *time.Time           `json:"finished_at"`
	BoxedUploadIntent string               `json:"boxed_upload_intent_id"`
	Detections        []inferenceDetection `json:"detections"`
	Signals           []inferenceSignal    `json:"signals"`
}

func (s *Server) handleInferenceCommit(w http.ResponseWriter, r *http.Request) {
	if !s.requireIdempotency(w, r, "POST:/api/v1/inference/commit") {
		return
	}
	var req inferenceCommitRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if principal, ok := nodePrincipalFromContext(r.Context()); ok {
		claimedBy, err := normalizeNodeClaimedBy(principal, req.ClaimedBy)
		if err != nil {
			writeAPIError(w, err)
			return
		}
		req.ClaimedBy = claimedBy
	}
	if strings.TrimSpace(req.PipelineID) == "" || req.FrameID <= 0 || req.ClaimID <= 0 || strings.TrimSpace(req.ClaimedBy) == "" {
		util.WriteError(w, http.StatusBadRequest, "claim_id, pipeline_id, frame_id, claimed_by are required")
		return
	}
	if strings.TrimSpace(req.Status) == "" {
		req.Status = "success"
	}
	resultID, revision, err := s.commitInference(r.Context(), req)
	if err != nil {
		if errors.Is(err, errConflict) {
			util.WriteError(w, http.StatusConflict, err.Error())
			return
		}
		if errors.Is(err, errBadRequest) {
			util.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
		util.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	util.WriteJSON(w, http.StatusCreated, map[string]any{"result_id": resultID, "revision": revision})
}

type inferenceFailRequest struct {
	ClaimID           int64          `json:"claim_id"`
	PipelineID        string         `json:"pipeline_id"`
	PipelineRunID     int64          `json:"pipeline_run_id"`
	PipelineVersionID *int64         `json:"pipeline_version_id,omitempty"`
	FrameID           int64          `json:"frame_id"`
	ClaimedBy         string         `json:"claimed_by"`
	ErrorText         string         `json:"error_text"`
	RunnerInfoJSON    map[string]any `json:"runner_info_json"`
}

func (s *Server) handleInferenceFail(w http.ResponseWriter, r *http.Request) {
	if !s.requireIdempotency(w, r, "POST:/api/v1/inference/fail") {
		return
	}
	var req inferenceFailRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if principal, ok := nodePrincipalFromContext(r.Context()); ok {
		claimedBy, err := normalizeNodeClaimedBy(principal, req.ClaimedBy)
		if err != nil {
			writeAPIError(w, err)
			return
		}
		req.ClaimedBy = claimedBy
	}
	if strings.TrimSpace(req.PipelineID) == "" || req.FrameID <= 0 || req.ClaimID <= 0 || strings.TrimSpace(req.ClaimedBy) == "" || strings.TrimSpace(req.ErrorText) == "" {
		util.WriteError(w, http.StatusBadRequest, "claim_id, pipeline_id, frame_id, claimed_by, error_text are required")
		return
	}
	_, _, err := s.commitInference(r.Context(), inferenceCommitRequest{
		ClaimID:           req.ClaimID,
		PipelineID:        req.PipelineID,
		PipelineRunID:     req.PipelineRunID,
		PipelineVersionID: req.PipelineVersionID,
		FrameID:           req.FrameID,
		ClaimedBy:         req.ClaimedBy,
		Status:            "error",
		ErrorText:         req.ErrorText,
		RunnerInfoJSON:    req.RunnerInfoJSON,
		SummaryJSON:       map[string]any{"status": "error"},
	})
	if err != nil {
		if errors.Is(err, errConflict) {
			util.WriteError(w, http.StatusConflict, err.Error())
			return
		}
		if errors.Is(err, errBadRequest) {
			util.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
		util.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	util.WriteJSON(w, http.StatusCreated, map[string]any{"ok": true})
}

var (
	errConflict   = errors.New("conflict")
	errBadRequest = errors.New("bad_request")
)

func resolveInferenceRevision(maxRevision int, hasSuccessful bool, forceRerun bool) (int, error) {
	if maxRevision < 0 {
		return 0, fmt.Errorf("%w: invalid max revision", errBadRequest)
	}
	if hasSuccessful && !forceRerun {
		return 0, fmt.Errorf("%w: successful result already exists for pipeline/frame; rerun requires force_rerun=true", errConflict)
	}
	if maxRevision == 0 {
		return 1, nil
	}
	// Prior revisions exist. If there is no successful revision yet, keep retrying by
	// advancing revision. If a successful revision exists, force_rerun has already been
	// validated above.
	return maxRevision + 1, nil
}

func validateInferenceCommitSemantics(status string, hasDetections bool, hasSignals bool, hasBoxedUploadIntent bool) error {
	switch status {
	case "success":
		if hasBoxedUploadIntent {
			return fmt.Errorf("%w: boxed_upload_intent_id is no longer accepted; boxing is backend-managed", errBadRequest)
		}
	case "error":
		if hasDetections {
			return fmt.Errorf("%w: detections must be empty when status=error", errBadRequest)
		}
		if hasSignals {
			return fmt.Errorf("%w: signals must be empty when status=error", errBadRequest)
		}
		if hasBoxedUploadIntent {
			return fmt.Errorf("%w: boxed_upload_intent_id must be empty when status=error", errBadRequest)
		}
	default:
		return fmt.Errorf("%w: status must be success or error", errBadRequest)
	}
	return nil
}

func isInferenceResultStatus(status string) bool {
	switch strings.TrimSpace(strings.ToLower(status)) {
	case "success", "error", "queued_boxed":
		return true
	default:
		return false
	}
}

func (s *Server) commitInference(ctx context.Context, req inferenceCommitRequest) (int64, int, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var claimStatus string
	var leaseExpires time.Time
	var claimOwner string
	var claimPipelineVersionID *int64
	var claimPipelineRunID *int64
	err = tx.QueryRow(ctx, `
		SELECT status, lease_expires_at, claimed_by, pipeline_version_id, pipeline_run_id
		FROM inference_claims
		WHERE id=$1 AND pipeline_id=$2 AND frame_id=$3
		FOR UPDATE
	`, req.ClaimID, strings.TrimSpace(req.PipelineID), req.FrameID).Scan(&claimStatus, &leaseExpires, &claimOwner, &claimPipelineVersionID, &claimPipelineRunID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, 0, fmt.Errorf("%w: claim not found", errBadRequest)
		}
		return 0, 0, fmt.Errorf("load claim: %w", err)
	}
	if claimStatus != "leased" {
		return 0, 0, fmt.Errorf("%w: claim is not leased", errConflict)
	}
	if strings.TrimSpace(claimOwner) != strings.TrimSpace(req.ClaimedBy) {
		return 0, 0, fmt.Errorf("%w: claim owner mismatch", errConflict)
	}
	if req.PipelineRunID > 0 {
		if claimPipelineRunID == nil || *claimPipelineRunID != req.PipelineRunID {
			return 0, 0, fmt.Errorf("%w: claim run mismatch", errConflict)
		}
	}
	if req.PipelineVersionID != nil {
		if claimPipelineVersionID == nil || *claimPipelineVersionID != *req.PipelineVersionID {
			return 0, 0, fmt.Errorf("%w: claim version mismatch", errConflict)
		}
	}
	if leaseExpires.Before(time.Now().UTC()) {
		if _, err := tx.Exec(ctx, `UPDATE inference_claims SET status='abandoned', updated_at=now() WHERE id=$1`, req.ClaimID); err != nil {
			return 0, 0, fmt.Errorf("expire claim: %w", err)
		}
		if claimPipelineRunID != nil {
			if _, err := tx.Exec(ctx, `
				UPDATE pipeline_run_targets
				SET status='abandoned', claim_id=NULL, claimed_by='', lease_expires_at=NULL, updated_at=now()
				WHERE run_id=$1 AND frame_id=$2
			`, *claimPipelineRunID, req.FrameID); err != nil {
				return 0, 0, fmt.Errorf("expire pipeline run target: %w", err)
			}
		}
		return 0, 0, fmt.Errorf("%w: claim lease expired", errConflict)
	}

	status := strings.TrimSpace(req.Status)
	if status == "" {
		status = "success"
	}
	if status == "error" && strings.TrimSpace(req.ErrorText) == "" {
		return 0, 0, fmt.Errorf("%w: error_text is required when status=error", errBadRequest)
	}
	intentID := strings.TrimSpace(req.BoxedUploadIntent)
	if err := validateInferenceCommitSemantics(status, len(req.Detections) > 0, len(req.Signals) > 0, intentID != ""); err != nil {
		return 0, 0, err
	}
	storedStatus := status
	if status == "success" && len(req.Detections) > 0 {
		storedStatus = "queued_boxed"
	}

	var hasSuccessful bool
	if claimPipelineRunID != nil {
		if err := tx.QueryRow(ctx, `
			SELECT EXISTS(
				SELECT 1 FROM inference_results
				WHERE pipeline_run_id=$1 AND frame_id=$2 AND status='success'
			)
		`, *claimPipelineRunID, req.FrameID).Scan(&hasSuccessful); err != nil {
			return 0, 0, fmt.Errorf("check successful run result: %w", err)
		}
	} else {
		if err := tx.QueryRow(ctx, `
			SELECT EXISTS(
				SELECT 1 FROM inference_results
				WHERE pipeline_id=$1 AND frame_id=$2 AND pipeline_run_id IS NULL AND status='success'
			)
		`, strings.TrimSpace(req.PipelineID), req.FrameID).Scan(&hasSuccessful); err != nil {
			return 0, 0, fmt.Errorf("check successful result: %w", err)
		}
	}

	force := req.ForceRerun || strings.EqualFold(strings.TrimSpace(req.RevisionMode), "force_rerun")
	var maxRev int
	if claimPipelineRunID != nil {
		if err := tx.QueryRow(ctx, `SELECT COALESCE(MAX(revision), 0) FROM inference_results WHERE pipeline_run_id=$1 AND frame_id=$2`, *claimPipelineRunID, req.FrameID).Scan(&maxRev); err != nil {
			return 0, 0, fmt.Errorf("load max run revision: %w", err)
		}
	} else {
		if err := tx.QueryRow(ctx, `SELECT COALESCE(MAX(revision), 0) FROM inference_results WHERE pipeline_id=$1 AND frame_id=$2 AND pipeline_run_id IS NULL`, strings.TrimSpace(req.PipelineID), req.FrameID).Scan(&maxRev); err != nil {
			return 0, 0, fmt.Errorf("load max revision: %w", err)
		}
	}
	revision, err := resolveInferenceRevision(maxRev, hasSuccessful, force)
	if err != nil {
		return 0, 0, err
	}

	var boxedMediaID *int64

	summaryJSON, err := json.Marshal(nonNilMap(req.SummaryJSON))
	if err != nil {
		return 0, 0, fmt.Errorf("%w: invalid summary_json: %v", errBadRequest, err)
	}
	rawOutJSON, err := json.Marshal(nonNilMap(req.RawOutputJSON))
	if err != nil {
		return 0, 0, fmt.Errorf("%w: invalid raw_output_json: %v", errBadRequest, err)
	}
	runnerJSON, err := json.Marshal(nonNilMap(req.RunnerInfoJSON))
	if err != nil {
		return 0, 0, fmt.Errorf("%w: invalid runner_info_json: %v", errBadRequest, err)
	}

	startedAt := req.StartedAt
	finishedAt := req.FinishedAt
	if finishedAt == nil {
		now := time.Now().UTC()
		finishedAt = &now
	}

	var resultID int64
	if err := tx.QueryRow(ctx, `
		INSERT INTO inference_results (
			pipeline_id, pipeline_version_id, pipeline_run_id, frame_id, revision, status,
			summary_jsonb, boxed_media_object_id, raw_output_jsonb,
			error_text, runner_info_jsonb, started_at, finished_at
		)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
		RETURNING id
	`, strings.TrimSpace(req.PipelineID), claimPipelineVersionID, claimPipelineRunID, req.FrameID, revision, storedStatus,
		summaryJSON, boxedMediaID, rawOutJSON,
		nullableTrimmed(req.ErrorText), runnerJSON, startedAt, finishedAt).Scan(&resultID); err != nil {
		return 0, 0, fmt.Errorf("insert inference_result: %w", err)
	}

	for i := range req.Detections {
		d := req.Detections[i]
		if strings.TrimSpace(d.ClassName) == "" {
			return 0, 0, fmt.Errorf("%w: detection class_name is required", errBadRequest)
		}
		area := d.AreaPx
		if area <= 0 {
			area = (d.X2 - d.X1) * (d.Y2 - d.Y1)
		}
		extraJSON, err := json.Marshal(nonNilMap(d.ExtraJSON))
		if err != nil {
			return 0, 0, fmt.Errorf("%w: invalid detection extra_json: %v", errBadRequest, err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO detections (
				inference_result_id, class_id, class_name, confidence,
				x1, y1, x2, y2, area_px, extra_jsonb
			)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
		`, resultID, nullableTrimmed(d.ClassID), strings.TrimSpace(d.ClassName), d.Confidence,
			d.X1, d.Y1, d.X2, d.Y2, area, extraJSON); err != nil {
			return 0, 0, fmt.Errorf("insert detection: %w", err)
		}
	}
	for i := range req.Signals {
		sig := req.Signals[i]
		signalType := strings.TrimSpace(sig.SignalType)
		signalKey := strings.TrimSpace(sig.SignalKey)
		if signalType == "" {
			return 0, 0, fmt.Errorf("%w: signal_type is required", errBadRequest)
		}
		if signalKey == "" {
			return 0, 0, fmt.Errorf("%w: signal_key is required", errBadRequest)
		}
		extraJSON, err := json.Marshal(nonNilMap(sig.ExtraJSON))
		if err != nil {
			return 0, 0, fmt.Errorf("%w: invalid signal extra_json: %v", errBadRequest, err)
		}
		var valueText any
		if sig.ValueText != nil {
			valueText = nullableTrimmed(*sig.ValueText)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO inference_signals (
				inference_result_id, signal_type, signal_key,
				confidence, value_num, value_text, extra_jsonb
			)
			VALUES ($1,$2,$3,$4,$5,$6,$7)
		`, resultID, signalType, signalKey, sig.Confidence, sig.ValueNum, valueText, extraJSON); err != nil {
			return 0, 0, fmt.Errorf("insert inference signal: %w", err)
		}
	}
	if storedStatus == "queued_boxed" {
		maxAttempts := s.cfg.InferenceBoxMaxAttempts
		if maxAttempts <= 0 {
			maxAttempts = 8
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO inference_box_jobs (
				inference_result_id, status, max_attempts
			)
			VALUES ($1, 'pending', $2)
		`, resultID, maxAttempts); err != nil {
			return 0, 0, fmt.Errorf("insert inference_box_job: %w", err)
		}
	}

	if _, err := tx.Exec(ctx, `UPDATE inference_claims SET status='completed', updated_at=now() WHERE id=$1`, req.ClaimID); err != nil {
		return 0, 0, fmt.Errorf("complete inference claim: %w", err)
	}
	if claimPipelineRunID != nil {
		targetStatus := "completed"
		if status == "error" {
			targetStatus = "error"
		}
		if _, err := tx.Exec(ctx, `
			UPDATE pipeline_run_targets
			SET status=$3, claim_id=$4, claimed_by=$5, lease_expires_at=NULL, result_id=$6, error_text=$7, updated_at=now()
			WHERE run_id=$1 AND frame_id=$2
		`, *claimPipelineRunID, req.FrameID, targetStatus, req.ClaimID, strings.TrimSpace(req.ClaimedBy), resultID, strings.TrimSpace(req.ErrorText)); err != nil {
			return 0, 0, fmt.Errorf("update pipeline run target: %w", err)
		}
		if err := refreshPipelineRunStatus(ctx, tx, *claimPipelineRunID); err != nil {
			return 0, 0, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, 0, fmt.Errorf("commit tx: %w", err)
	}
	return resultID, revision, nil
}

type recentFrameItem struct {
	ID          int64     `json:"id"`
	StreamID    int64     `json:"stream_id"`
	CaptureJob  *int64    `json:"capture_job_id,omitempty"`
	CapturedAt  time.Time `json:"captured_at"`
	Status      string    `json:"capture_status"`
	Error       *string   `json:"capture_error,omitempty"`
	SourceKind  string    `json:"source_kind"`
	ObjectKey   *string   `json:"object_key,omitempty"`
	MIMEType    *string   `json:"mime_type,omitempty"`
	SizeBytes   *int64    `json:"size_bytes,omitempty"`
	Width       int       `json:"width"`
	Height      int       `json:"height"`
	DownloadURL string    `json:"download_url,omitempty"`
}

func (s *Server) queryRecentFrameItems(ctx context.Context, streamID int64, limit int) ([]recentFrameItem, error) {
	if streamID <= 0 {
		return nil, fmt.Errorf("stream_id is required")
	}
	if limit <= 0 {
		return []recentFrameItem{}, nil
	}
	rows, err := s.pool.Query(ctx, `
		SELECT
			f.id, f.stream_id, f.capture_job_id, f.captured_at,
			f.capture_status, f.capture_error, f.source_kind,
			mo.object_key, mo.mime_type, mo.size_bytes,
			COALESCE(mo.width, 0), COALESCE(mo.height, 0)
		FROM frames f
		LEFT JOIN media_objects mo ON mo.id = f.raw_media_object_id
		WHERE f.stream_id=$1
		ORDER BY f.captured_at DESC NULLS LAST, f.id DESC
		LIMIT $2
	`, streamID, limit)
	if err != nil {
		return nil, fmt.Errorf("query recent frames: %w", err)
	}
	defer rows.Close()
	items := make([]recentFrameItem, 0, limit)
	for rows.Next() {
		var it recentFrameItem
		if err := rows.Scan(&it.ID, &it.StreamID, &it.CaptureJob, &it.CapturedAt, &it.Status, &it.Error, &it.SourceKind, &it.ObjectKey, &it.MIMEType, &it.SizeBytes, &it.Width, &it.Height); err != nil {
			return nil, fmt.Errorf("scan recent frame: %w", err)
		}
		if it.ObjectKey != nil && strings.TrimSpace(*it.ObjectKey) != "" {
			if url, err := s.r2.PresignGet(ctx, *it.ObjectKey, s.cfg.R2SignGetTTL); err == nil {
				it.DownloadURL = url
			}
		}
		items = append(items, it)
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("iterate recent frames: %w", rows.Err())
	}
	return items, nil
}

func (s *Server) handleFramesList(w http.ResponseWriter, r *http.Request) {
	limit := parseIntQuery(r, "limit", 200, 1, 5000)
	offset := parseIntQuery(r, "offset", 0, 0, 1000000)
	pipelineID := strings.TrimSpace(r.URL.Query().Get("pipeline_id"))
	uninferenced := parseBoolQueryPtr(r, "uninferenced")
	unprocessed := parseBoolQueryPtr(r, "unprocessed")
	orderColumns := map[string]string{
		"":            "f.captured_at",
		"captured_at": "f.captured_at",
		"id":          "f.id",
		"stream_id":   "f.stream_id",
		"status":      "f.capture_status",
		"error":       "COALESCE(f.capture_error, '')",
		"source_kind": "f.source_kind",
		"object_key":  "COALESCE(mo.object_key, '')",
		"size_bytes":  "COALESCE(mo.size_bytes, 0)",
		"width":       "COALESCE(mo.width, 0)",
		"height":      "COALESCE(mo.height, 0)",
	}
	orderExpr, _, sortDir, ok := parseSortQuery(w, r, orderColumns, "captured_at", "desc")
	if !ok {
		return
	}

	where := []string{"1=1"}
	args := []any{}
	if streamID := parseInt64QueryPtr(r, "stream_id"); streamID != nil {
		args = append(args, *streamID)
		where = append(where, fmt.Sprintf("f.stream_id=$%d", len(args)))
	}
	if pipelineID != "" {
		args = append(args, pipelineID)
		where = append(where, fmt.Sprintf(`COALESCE((
			SELECT sps.enabled
			FROM stream_pipeline_settings sps
			WHERE sps.stream_id=f.stream_id AND sps.pipeline_id=$%d
		), true)`, len(args)))
	}
	if uninferenced != nil && *uninferenced {
		if pipelineID == "" {
			util.WriteError(w, http.StatusBadRequest, "pipeline_id is required when uninferenced=1")
			return
		}
		args = append(args, pipelineID)
		where = append(where, fmt.Sprintf(`NOT EXISTS (
				SELECT 1 FROM inference_results ir
				WHERE ir.frame_id=f.id AND ir.pipeline_id=$%d AND ir.status IN ('success','queued_boxed')
			)`, len(args)))
	}
	if unprocessed != nil && *unprocessed {
		if pipelineID == "" {
			util.WriteError(w, http.StatusBadRequest, "pipeline_id is required when unprocessed=1")
			return
		}
		args = append(args, pipelineID)
		where = append(where, fmt.Sprintf(`NOT EXISTS (
			SELECT 1 FROM inference_results ir
			WHERE ir.frame_id=f.id AND ir.pipeline_id=$%d
		)`, len(args)))
	}
	args = append(args, limit, offset)

	rows, err := s.pool.Query(r.Context(), fmt.Sprintf(`
		SELECT
			f.id, f.stream_id, f.capture_job_id, f.captured_at,
			f.capture_status, f.capture_error, f.source_kind,
			mo.object_key, mo.mime_type, mo.size_bytes,
			COALESCE(mo.width, 0), COALESCE(mo.height, 0)
		FROM frames f
			LEFT JOIN media_objects mo ON mo.id = f.raw_media_object_id
			WHERE %s
			ORDER BY %s %s NULLS LAST, f.id DESC
			LIMIT $%d OFFSET $%d
		`, strings.Join(where, " AND "), orderExpr, sortDir, len(args)-1, len(args)), args...)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("list frames: %v", err))
		return
	}
	defer rows.Close()
	type frameItem struct {
		ID          int64     `json:"id"`
		StreamID    int64     `json:"stream_id"`
		CaptureJob  *int64    `json:"capture_job_id,omitempty"`
		CapturedAt  time.Time `json:"captured_at"`
		Status      string    `json:"capture_status"`
		Error       *string   `json:"capture_error,omitempty"`
		SourceKind  string    `json:"source_kind"`
		ObjectKey   *string   `json:"object_key,omitempty"`
		MIMEType    *string   `json:"mime_type,omitempty"`
		SizeBytes   *int64    `json:"size_bytes,omitempty"`
		Width       int       `json:"width"`
		Height      int       `json:"height"`
		DownloadURL string    `json:"download_url,omitempty"`
	}
	items := make([]frameItem, 0, limit)
	for rows.Next() {
		var it frameItem
		if err := rows.Scan(&it.ID, &it.StreamID, &it.CaptureJob, &it.CapturedAt, &it.Status, &it.Error, &it.SourceKind, &it.ObjectKey, &it.MIMEType, &it.SizeBytes, &it.Width, &it.Height); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan frame: %v", err))
			return
		}
		if it.ObjectKey != nil && *it.ObjectKey != "" {
			url, err := s.r2.PresignGet(r.Context(), *it.ObjectKey, s.cfg.R2SignGetTTL)
			if err == nil {
				it.DownloadURL = url
			}
		}
		items = append(items, it)
	}
	if rows.Err() != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("iterate frames: %v", rows.Err()))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"items": items, "limit": limit, "offset": offset})
}

func normalizeFrameExportStatus(raw string) (string, error) {
	v := strings.ToLower(strings.TrimSpace(raw))
	if v == "" {
		return "success", nil
	}
	switch v {
	case "success", "error", "all":
		return v, nil
	default:
		return "", fmt.Errorf("capture_status must be one of success|error|all")
	}
}

func parseFrameExportWindow(fromRaw, toRaw string) (time.Time, time.Time, error) {
	from := strings.TrimSpace(fromRaw)
	to := strings.TrimSpace(toRaw)
	if from == "" || to == "" {
		return time.Time{}, time.Time{}, fmt.Errorf("captured_from and captured_to are required")
	}
	fromTime, err := time.Parse(time.RFC3339, from)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("invalid captured_from; expected RFC3339")
	}
	toTime, err := time.Parse(time.RFC3339, to)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("invalid captured_to; expected RFC3339")
	}
	if !toTime.After(fromTime) {
		return time.Time{}, time.Time{}, fmt.Errorf("captured_to must be after captured_from")
	}
	return fromTime.UTC(), toTime.UTC(), nil
}

func appendFrameExportStatusWhere(where []string, args []any, captureStatus string) ([]string, []any) {
	switch captureStatus {
	case "success":
		args = append(args, "success")
		where = append(where, fmt.Sprintf("f.capture_status=$%d", len(args)))
	case "error":
		args = append(args, "error")
		where = append(where, fmt.Sprintf("f.capture_status=$%d", len(args)))
	}
	return where, args
}

func queryFrameExportRows(ctx context.Context, pool *pgxpool.Pool, streamID int64, capturedFrom, capturedTo time.Time, captureStatus string) ([]frameExportRow, int, int64, error) {
	where := []string{
		"f.stream_id=$1",
		"f.captured_at >= $2",
		"f.captured_at < $3",
	}
	args := []any{streamID, capturedFrom, capturedTo}
	where, args = appendFrameExportStatusWhere(where, args, captureStatus)
	rows, err := pool.Query(ctx, fmt.Sprintf(`
		SELECT
			f.id,
			f.captured_at,
			f.capture_status,
			f.capture_error,
			mo.object_key,
			mo.mime_type,
			COALESCE(mo.size_bytes, 0)
		FROM frames f
		LEFT JOIN media_objects mo ON mo.id=f.raw_media_object_id
		WHERE %s
		ORDER BY f.captured_at ASC, f.id ASC
	`, strings.Join(where, " AND ")), args...)
	if err != nil {
		return nil, 0, 0, err
	}
	defer rows.Close()
	items := make([]frameExportRow, 0, 1024)
	var totalBytes int64
	for rows.Next() {
		var item frameExportRow
		if err := rows.Scan(&item.ID, &item.CapturedAt, &item.Status, &item.ErrorText, &item.ObjectKey, &item.MIMEType, &item.SizeBytes); err != nil {
			return nil, 0, 0, err
		}
		totalBytes += item.SizeBytes
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, 0, err
	}
	return items, len(items), totalBytes, nil
}

func frameExportExt(objectKey string, mimeType string) string {
	if ext := strings.ToLower(strings.TrimSpace(filepath.Ext(objectKey))); ext != "" {
		return ext
	}
	switch strings.ToLower(strings.TrimSpace(mimeType)) {
	case "image/jpeg":
		return ".jpg"
	case "image/png":
		return ".png"
	case "image/webp":
		return ".webp"
	default:
		return ".bin"
	}
}

func frameExportName(capturedAt time.Time, frameID int64, objectKey string, mimeType string) string {
	return fmt.Sprintf("frames/%s-frame-%d%s", capturedAt.UTC().Format("20060102T150405Z"), frameID, frameExportExt(objectKey, mimeType))
}

// frameExportJobMaxEntries caps how many job records are retained in memory as a
// hard backstop against unbounded growth.
const frameExportJobMaxEntries = 256

func (s *Server) setFrameExportJob(job *frameExportJob) {
	s.exportMu.Lock()
	defer s.exportMu.Unlock()
	s.reapFrameExportJobsLocked()
	s.frameExports[job.ID] = job
}

// reapFrameExportJobsLocked prunes finished (complete/error) job records: it evicts
// any whose FinishedAt is older than R2SignGetTTL, and, if the map is still over the
// hard cap, drops the oldest finished entries until under the cap. The caller must
// hold exportMu. In-flight (pending/running) jobs are never evicted.
func (s *Server) reapFrameExportJobsLocked() {
	now := time.Now()
	for id, job := range s.frameExports {
		if job.FinishedAt != nil && now.Sub(*job.FinishedAt) > s.cfg.R2SignGetTTL {
			delete(s.frameExports, id)
		}
	}
	for len(s.frameExports) >= frameExportJobMaxEntries {
		var oldestID string
		var oldest *time.Time
		for id, job := range s.frameExports {
			if job.FinishedAt == nil {
				continue
			}
			if oldest == nil || job.FinishedAt.Before(*oldest) {
				oldest = job.FinishedAt
				oldestID = id
			}
		}
		if oldestID == "" {
			break
		}
		delete(s.frameExports, oldestID)
	}
}

func (s *Server) getFrameExportJob(id string) (*frameExportJob, bool) {
	s.exportMu.Lock()
	defer s.exportMu.Unlock()
	job, ok := s.frameExports[id]
	if !ok {
		return nil, false
	}
	cp := *job
	return &cp, true
}

func (s *Server) updateFrameExportJob(id string, fn func(*frameExportJob)) {
	s.exportMu.Lock()
	defer s.exportMu.Unlock()
	if job, ok := s.frameExports[id]; ok {
		fn(job)
	}
}

func (s *Server) runFrameExportJob(jobID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	startedAt := time.Now().UTC()
	s.updateFrameExportJob(jobID, func(job *frameExportJob) {
		job.Status = "running"
		job.StartedAt = &startedAt
		job.ErrorText = ""
	})
	job, ok := s.getFrameExportJob(jobID)
	if !ok {
		return
	}
	rows, _, _, err := queryFrameExportRows(ctx, s.pool, job.StreamID, job.CapturedFrom, job.CapturedTo, job.CaptureStatus)
	if err != nil {
		s.updateFrameExportJob(jobID, func(job *frameExportJob) {
			finishedAt := time.Now().UTC()
			job.Status = "failed"
			job.ErrorText = fmt.Sprintf("query export frames: %v", err)
			job.FinishedAt = &finishedAt
		})
		return
	}
	tmp, err := os.CreateTemp("", fmt.Sprintf("stream-frame-export-%d-*.zip", job.StreamID))
	if err != nil {
		s.updateFrameExportJob(jobID, func(job *frameExportJob) {
			finishedAt := time.Now().UTC()
			job.Status = "failed"
			job.ErrorText = fmt.Sprintf("create temp export: %v", err)
			job.FinishedAt = &finishedAt
		})
		return
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	zipWriter := zip.NewWriter(tmp)
	manifestWriter, err := zipWriter.Create("manifest.csv")
	if err != nil {
		_ = tmp.Close()
		s.updateFrameExportJob(jobID, func(job *frameExportJob) {
			finishedAt := time.Now().UTC()
			job.Status = "failed"
			job.ErrorText = fmt.Sprintf("create manifest entry: %v", err)
			job.FinishedAt = &finishedAt
		})
		return
	}
	csvWriter := csv.NewWriter(manifestWriter)
	if err := csvWriter.Write([]string{"frame_id", "captured_at", "capture_status", "capture_error", "object_key", "mime_type", "size_bytes"}); err != nil {
		_ = zipWriter.Close()
		_ = tmp.Close()
		s.updateFrameExportJob(jobID, func(job *frameExportJob) {
			finishedAt := time.Now().UTC()
			job.Status = "failed"
			job.ErrorText = fmt.Sprintf("write manifest header: %v", err)
			job.FinishedAt = &finishedAt
		})
		return
	}
	for _, row := range rows {
		objectKey := ""
		if row.ObjectKey != nil {
			objectKey = strings.TrimSpace(*row.ObjectKey)
		}
		mimeType := ""
		if row.MIMEType != nil {
			mimeType = strings.TrimSpace(*row.MIMEType)
		}
		captureError := ""
		if row.ErrorText != nil {
			captureError = strings.TrimSpace(*row.ErrorText)
		}
		if err := csvWriter.Write([]string{
			strconv.FormatInt(row.ID, 10),
			row.CapturedAt.UTC().Format(time.RFC3339Nano),
			row.Status,
			captureError,
			objectKey,
			mimeType,
			strconv.FormatInt(row.SizeBytes, 10),
		}); err != nil {
			_ = zipWriter.Close()
			_ = tmp.Close()
			s.updateFrameExportJob(jobID, func(job *frameExportJob) {
				finishedAt := time.Now().UTC()
				job.Status = "failed"
				job.ErrorText = fmt.Sprintf("write manifest row: %v", err)
				job.FinishedAt = &finishedAt
			})
			return
		}
		if objectKey == "" {
			continue
		}
		entry, err := zipWriter.Create(frameExportName(row.CapturedAt, row.ID, objectKey, mimeType))
		if err != nil {
			_ = zipWriter.Close()
			_ = tmp.Close()
			s.updateFrameExportJob(jobID, func(job *frameExportJob) {
				finishedAt := time.Now().UTC()
				job.Status = "failed"
				job.ErrorText = fmt.Sprintf("create archive entry: %v", err)
				job.FinishedAt = &finishedAt
			})
			return
		}
		body, err := s.r2.Open(ctx, objectKey)
		if err != nil {
			_ = zipWriter.Close()
			_ = tmp.Close()
			s.updateFrameExportJob(jobID, func(job *frameExportJob) {
				finishedAt := time.Now().UTC()
				job.Status = "failed"
				job.ErrorText = fmt.Sprintf("read frame object %s: %v", objectKey, err)
				job.FinishedAt = &finishedAt
			})
			return
		}
		if _, err := io.Copy(entry, body); err != nil {
			_ = body.Close()
			_ = zipWriter.Close()
			_ = tmp.Close()
			s.updateFrameExportJob(jobID, func(job *frameExportJob) {
				finishedAt := time.Now().UTC()
				job.Status = "failed"
				job.ErrorText = fmt.Sprintf("copy frame object %s: %v", objectKey, err)
				job.FinishedAt = &finishedAt
			})
			return
		}
		_ = body.Close()
	}
	csvWriter.Flush()
	if err := csvWriter.Error(); err != nil {
		_ = zipWriter.Close()
		_ = tmp.Close()
		s.updateFrameExportJob(jobID, func(job *frameExportJob) {
			finishedAt := time.Now().UTC()
			job.Status = "failed"
			job.ErrorText = fmt.Sprintf("flush manifest: %v", err)
			job.FinishedAt = &finishedAt
		})
		return
	}
	if err := zipWriter.Close(); err != nil {
		_ = tmp.Close()
		s.updateFrameExportJob(jobID, func(job *frameExportJob) {
			finishedAt := time.Now().UTC()
			job.Status = "failed"
			job.ErrorText = fmt.Sprintf("close archive: %v", err)
			job.FinishedAt = &finishedAt
		})
		return
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		_ = tmp.Close()
		s.updateFrameExportJob(jobID, func(job *frameExportJob) {
			finishedAt := time.Now().UTC()
			job.Status = "failed"
			job.ErrorText = fmt.Sprintf("rewind archive: %v", err)
			job.FinishedAt = &finishedAt
		})
		return
	}
	objectKey := fmt.Sprintf("exports/stream/%d/frame-export-%s.zip", job.StreamID, job.ID)
	if _, err := s.r2.PutReader(ctx, objectKey, "application/zip", tmp); err != nil {
		_ = tmp.Close()
		s.updateFrameExportJob(jobID, func(job *frameExportJob) {
			finishedAt := time.Now().UTC()
			job.Status = "failed"
			job.ErrorText = fmt.Sprintf("upload archive: %v", err)
			job.FinishedAt = &finishedAt
		})
		return
	}
	_ = tmp.Close()
	finishedAt := time.Now().UTC()
	s.updateFrameExportJob(jobID, func(job *frameExportJob) {
		job.Status = "complete"
		job.ObjectKey = objectKey
		job.FinishedAt = &finishedAt
		job.ErrorText = ""
	})
}

func (s *Server) handleDashboardStreamFrameManifest(w http.ResponseWriter, r *http.Request) {
	streamID, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	if _, err := s.getStreamByID(r.Context(), streamID); err != nil {
		util.WriteError(w, http.StatusNotFound, err.Error())
		return
	}
	capturedFrom, capturedTo, err := parseFrameExportWindow(r.URL.Query().Get("captured_from"), r.URL.Query().Get("captured_to"))
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	captureStatus, err := normalizeFrameExportStatus(r.URL.Query().Get("capture_status"))
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	rows, _, _, err := queryFrameExportRows(r.Context(), s.pool, streamID, capturedFrom, capturedTo, captureStatus)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("query frame export manifest: %v", err))
		return
	}
	filename := fmt.Sprintf("stream-%d-frames-%s-to-%s.csv", streamID, capturedFrom.Format("20060102T150405Z"), capturedTo.Format("20060102T150405Z"))
	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", filename))
	cw := csv.NewWriter(w)
	_ = cw.Write([]string{"frame_id", "captured_at", "capture_status", "capture_error", "object_key", "mime_type", "size_bytes", "download_url"})
	for _, row := range rows {
		objectKey := ""
		if row.ObjectKey != nil {
			objectKey = strings.TrimSpace(*row.ObjectKey)
		}
		mimeType := ""
		if row.MIMEType != nil {
			mimeType = strings.TrimSpace(*row.MIMEType)
		}
		captureError := ""
		if row.ErrorText != nil {
			captureError = strings.TrimSpace(*row.ErrorText)
		}
		downloadURL := ""
		if objectKey != "" {
			if url, err := s.r2.PresignGet(r.Context(), objectKey, s.cfg.R2SignGetTTL); err == nil {
				downloadURL = url
			}
		}
		_ = cw.Write([]string{
			strconv.FormatInt(row.ID, 10),
			row.CapturedAt.UTC().Format(time.RFC3339Nano),
			row.Status,
			captureError,
			objectKey,
			mimeType,
			strconv.FormatInt(row.SizeBytes, 10),
			downloadURL,
		})
	}
	cw.Flush()
	if err := cw.Error(); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("write frame export manifest: %v", err))
		return
	}
}

func (s *Server) handleDashboardStreamFrameExportCreate(w http.ResponseWriter, r *http.Request) {
	streamID, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	if _, err := s.getStreamByID(r.Context(), streamID); err != nil {
		util.WriteError(w, http.StatusNotFound, err.Error())
		return
	}
	var req frameExportCreateRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	capturedFrom, capturedTo, err := parseFrameExportWindow(req.CapturedFrom, req.CapturedTo)
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	captureStatus, err := normalizeFrameExportStatus(req.CaptureStatus)
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	_, frameCount, totalBytes, err := queryFrameExportRows(r.Context(), s.pool, streamID, capturedFrom, capturedTo, captureStatus)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("query frame export size: %v", err))
		return
	}
	if frameCount == 0 {
		util.WriteError(w, http.StatusBadRequest, "no matching frames in selected window")
		return
	}
	if frameCount > frameExportMaxFrames {
		util.WriteError(w, http.StatusBadRequest, fmt.Sprintf("selection too large for zip export: %d frames exceeds max %d; use manifest or narrow the window", frameCount, frameExportMaxFrames))
		return
	}
	if totalBytes > frameExportMaxBytes {
		util.WriteError(w, http.StatusBadRequest, fmt.Sprintf("selection too large for zip export: %d bytes exceeds max %d; use manifest or narrow the window", totalBytes, frameExportMaxBytes))
		return
	}
	job := &frameExportJob{
		ID:            uuid.NewString(),
		StreamID:      streamID,
		CapturedFrom:  capturedFrom,
		CapturedTo:    capturedTo,
		CaptureStatus: captureStatus,
		Status:        "queued",
		FrameCount:    frameCount,
		TotalBytes:    totalBytes,
		CreatedAt:     time.Now().UTC(),
	}
	s.setFrameExportJob(job)
	go s.runFrameExportJob(job.ID)
	util.WriteJSON(w, http.StatusAccepted, job)
}

func (s *Server) handleDashboardStreamFrameExportGet(w http.ResponseWriter, r *http.Request) {
	streamID, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	exportID := strings.TrimSpace(chi.URLParam(r, "export_id"))
	if exportID == "" {
		util.WriteError(w, http.StatusBadRequest, "export_id is required")
		return
	}
	job, ok := s.getFrameExportJob(exportID)
	if !ok || job.StreamID != streamID {
		util.WriteError(w, http.StatusNotFound, "frame export not found")
		return
	}
	resp := map[string]any{
		"id":             job.ID,
		"stream_id":      job.StreamID,
		"captured_from":  job.CapturedFrom,
		"captured_to":    job.CapturedTo,
		"capture_status": job.CaptureStatus,
		"status":         job.Status,
		"frame_count":    job.FrameCount,
		"total_bytes":    job.TotalBytes,
		"object_key":     job.ObjectKey,
		"error_text":     job.ErrorText,
		"created_at":     job.CreatedAt,
		"started_at":     job.StartedAt,
		"finished_at":    job.FinishedAt,
	}
	if job.Status == "complete" && strings.TrimSpace(job.ObjectKey) != "" {
		if url, err := s.r2.PresignGet(r.Context(), job.ObjectKey, s.cfg.R2SignGetTTL); err == nil {
			resp["download_url"] = url
		}
	}
	util.WriteJSON(w, http.StatusOK, resp)
}

func (s *Server) handleCaptureSchema(w http.ResponseWriter, r *http.Request) {
	util.WriteJSON(w, http.StatusOK, map[string]any{
		"capture_types": []string{
			capture.CaptureTypeYouTubeWatch,
			capture.CaptureTypeHLS,
			capture.CaptureTypeDASH,
			capture.CaptureTypeRTSP,
			capture.CaptureTypeRTMP,
			capture.CaptureTypeHTTPVideo,
			capture.CaptureTypeStillImage,
			capture.CaptureTypeWebRTC,
			capture.CaptureTypeUnknown,
		},
		"execution_classes": []string{
			capture.ExecutionClassYouTubeDirect,
			capture.ExecutionClassVideoLive,
		},
	})
}

func (s *Server) handleCaptureStreams(w http.ResponseWriter, r *http.Request) {
	limit := parseIntQuery(r, "limit", 500, 1, 2000)
	offset := parseIntQuery(r, "offset", 0, 0, 1000000)
	stateRaw := strings.TrimSpace(strings.ToLower(r.URL.Query().Get("recording_state")))
	if stateRaw == "" {
		stateRaw = string(model.RecordingStateOn)
	}
	state, ok := parseRecordingState(stateRaw)
	if !ok {
		util.WriteError(w, http.StatusBadRequest, "invalid recording_state; expected off|on")
		return
	}

	var total int64
	if err := s.pool.QueryRow(r.Context(), `
		SELECT COUNT(*)::bigint
		FROM streams
		WHERE recording_state=$1
	`, string(state)).Scan(&total); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("capture stream count query: %v", err))
		return
	}

	rows, err := s.pool.Query(r.Context(), `
		SELECT
			id, provider, external_id, name, slug, source_url, source_page_url,
			source_family,
			capture_family, expected_fps, expected_image_interval_sec,
			lat, lon, location_text, location_country, location_country_code, location_region, location_city, location_locality, location_source, metadata_jsonb,
			recording_state, recording_failed_reason, recording_failed_at, capture_type, execution_class, execution_config_jsonb, tags,
			created_at, updated_at
		FROM streams
		WHERE recording_state=$1
		ORDER BY id ASC
		LIMIT $2 OFFSET $3
	`, string(state), limit, offset)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("capture stream list query: %v", err))
		return
	}
	defer rows.Close()

	items := make([]map[string]any, 0, limit)
	for rows.Next() {
		stream, metaBytes, cfgBytes, err := scanStream(rows)
		if err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan capture stream: %v", err))
			return
		}
		if err := decodeStreamPayload(&stream, metaBytes, cfgBytes); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("decode capture stream payload: %v", err))
			return
		}
		items = append(items, map[string]any{"stream": stream})
	}
	if rows.Err() != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("iterate capture streams: %v", rows.Err()))
		return
	}

	util.WriteJSON(w, http.StatusOK, map[string]any{
		"items":  items,
		"limit":  limit,
		"offset": offset,
		"total":  total,
	})
}

func (s *Server) handleCaptureStreamDetail(w http.ResponseWriter, r *http.Request) {
	streamID, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	stream, err := s.getStreamByID(r.Context(), streamID)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			util.WriteError(w, http.StatusNotFound, "stream not found")
			return
		}
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("query capture stream detail: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"stream": stream})
}

func (s *Server) handleCaptureRuntime(w http.ResponseWriter, r *http.Request) {
	limit := parseIntQuery(r, "limit", 200, 1, 2000)
	offset := parseIntQuery(r, "offset", 0, 0, 1000000)
	status := strings.TrimSpace(strings.ToLower(r.URL.Query().Get("status")))
	where := []string{"1=1"}
	args := []any{}
	if status != "" {
		args = append(args, status)
		where = append(where, fmt.Sprintf("r.status=$%d", len(args)))
	}
	args = append(args, limit, offset)

	rows, err := s.pool.Query(r.Context(), fmt.Sprintf(`
		SELECT
			r.stream_id, s.provider, s.name, s.slug,
			r.status, r.execution_class, r.resolved_url, r.last_resolved_at, r.last_frame_at,
			r.consecutive_errors, r.last_error_text, r.updated_at
		FROM stream_capture_runtime r
		JOIN streams s ON s.id=r.stream_id
		WHERE %s
		ORDER BY r.updated_at DESC, r.stream_id ASC
		LIMIT $%d OFFSET $%d
	`, strings.Join(where, " AND "), len(args)-1, len(args)), args...)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("query capture runtime: %v", err))
		return
	}
	defer rows.Close()
	type item struct {
		StreamID          int64      `json:"stream_id"`
		Provider          string     `json:"provider"`
		Name              string     `json:"name"`
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
	out := make([]item, 0, limit)
	for rows.Next() {
		var it item
		if err := rows.Scan(&it.StreamID, &it.Provider, &it.Name, &it.Slug, &it.Status, &it.EffectiveMode, &it.ResolvedURL, &it.LastResolvedAt, &it.LastFrameAt, &it.ConsecutiveErrors, &it.LastErrorText, &it.UpdatedAt); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan capture runtime: %v", err))
			return
		}
		out = append(out, it)
	}
	if rows.Err() != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("iterate capture runtime: %v", rows.Err()))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"items": out, "limit": limit, "offset": offset})
}

type captureRuntimeStoppedRequest struct {
	StreamID int64 `json:"stream_id"`
}

func (s *Server) handleCaptureRuntimeStopped(w http.ResponseWriter, r *http.Request) {
	var req captureRuntimeStoppedRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.StreamID <= 0 {
		util.WriteError(w, http.StatusBadRequest, "stream_id must be > 0")
		return
	}
	if _, err := s.pool.Exec(r.Context(), `
		INSERT INTO stream_capture_runtime (stream_id, status)
		VALUES ($1, 'stopped')
		ON CONFLICT (stream_id)
		DO UPDATE SET
			status='stopped',
			updated_at=now()
	`, req.StreamID); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("set capture runtime stopped: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"ok": true})
}

type captureWorkerHeartbeatRequest struct {
	WorkerID       string         `json:"worker_id"`
	ExecutionClass string         `json:"execution_class"`
	Capacity       int            `json:"capacity"`
	LeaseSec       int            `json:"lease_sec"`
	MetadataJSON   map[string]any `json:"metadata_json"`
}

func (s *Server) handleCaptureWorkerHeartbeat(w http.ResponseWriter, r *http.Request) {
	var req captureWorkerHeartbeatRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	workerID := strings.TrimSpace(req.WorkerID)
	if workerID == "" {
		util.WriteError(w, http.StatusBadRequest, "worker_id is required")
		return
	}
	executionClass, err := normalizeExecutionClassInput(req.ExecutionClass)
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.Capacity <= 0 {
		util.WriteError(w, http.StatusBadRequest, "capacity must be > 0")
		return
	}
	leaseSec := req.LeaseSec
	if leaseSec <= 0 {
		leaseSec = 45
	}
	if leaseSec > 3600 {
		util.WriteError(w, http.StatusBadRequest, "lease_sec must be <= 3600")
		return
	}
	metadata := nonNilMap(req.MetadataJSON)
	metadata["capacity"] = req.Capacity
	metadata["worker_id"] = workerID
	metadata["execution_class"] = executionClass
	if strings.TrimSpace(stringFromMetadata(metadata, "server_id")) == "" {
		metadata["server_id"] = deriveServerID(workerID, metadata)
	}
	if strings.TrimSpace(stringFromMetadata(metadata, "process_name")) == "" {
		metadata["process_name"] = fmt.Sprintf("capture:%s", executionClass)
	}
	metaBytes, err := json.Marshal(metadata)
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, fmt.Sprintf("invalid metadata_json: %v", err))
		return
	}

	if _, err := s.pool.Exec(r.Context(), `
		INSERT INTO capture_worker_heartbeats (worker_id, execution_class, capacity, heartbeat_at, lease_expires_at, updated_at)
		VALUES ($1, $2, $3, now(), now() + make_interval(secs => $4), now())
		ON CONFLICT (worker_id, execution_class)
		DO UPDATE SET
			capacity=EXCLUDED.capacity,
			heartbeat_at=EXCLUDED.heartbeat_at,
			lease_expires_at=EXCLUDED.lease_expires_at,
			updated_at=now()
	`, workerID, executionClass, req.Capacity, leaseSec); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("upsert capture worker heartbeat: %v", err))
		return
	}
	if _, err := s.pool.Exec(r.Context(), `
		INSERT INTO processing_worker_heartbeats (
			worker_id, worker_kind, execution_class, pipeline_id, metadata_jsonb,
			heartbeat_at, lease_expires_at, updated_at
		)
		VALUES ($1, 'capture', $2, '', $3::jsonb, now(), now() + make_interval(secs => $4), now())
		ON CONFLICT (worker_id, worker_kind, execution_class, pipeline_id)
		DO UPDATE SET
			metadata_jsonb=EXCLUDED.metadata_jsonb,
			heartbeat_at=EXCLUDED.heartbeat_at,
			lease_expires_at=EXCLUDED.lease_expires_at,
			updated_at=now()
	`, workerID, executionClass, string(metaBytes), leaseSec); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("upsert capture processing heartbeat: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"ok": true})
}

type captureWorkerStoppedRequest struct {
	WorkerID       string `json:"worker_id"`
	ExecutionClass string `json:"execution_class"`
}

func (s *Server) handleCaptureJobsEnqueueDue(w http.ResponseWriter, r *http.Request) {
	rs, err := settings.GetRecordingSettings(r.Context(), s.pool)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if err := queue.EnqueueDueCaptureJobs(r.Context(), s.pool, queue.CaptureSamplingPolicy{
		MinIntervalSec: rs.SampleIntervalMinSec,
		MaxIntervalSec: rs.SampleIntervalMaxSec,
	}); err != nil {
		util.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"ok": true})
}

type captureJobLeaseRequest struct {
	WorkerID  string  `json:"worker_id"`
	LeaseSec  int     `json:"lease_sec"`
	StreamIDs []int64 `json:"stream_ids"`
}

func (s *Server) handleCaptureJobsLease(w http.ResponseWriter, r *http.Request) {
	var req captureJobLeaseRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	workerID := strings.TrimSpace(req.WorkerID)
	if workerID == "" {
		util.WriteError(w, http.StatusBadRequest, "worker_id is required")
		return
	}
	leaseSec := req.LeaseSec
	if leaseSec <= 0 {
		leaseSec = 45
	}
	job, err := queue.LeaseOneCaptureJob(r.Context(), s.pool, workerID, leaseSec, req.StreamIDs)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"job": job})
}

type captureJobNextRequest struct {
	NextDelaySec int `json:"next_delay_sec"`
}

func (s *Server) handleCaptureJobsComplete(w http.ResponseWriter, r *http.Request) {
	jobID, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	var req captureJobNextRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := queue.CompleteCaptureJob(r.Context(), s.pool, jobID, req.NextDelaySec); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleCaptureJobsCompleteWithoutNext(w http.ResponseWriter, r *http.Request) {
	jobID, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	if err := queue.CompleteCaptureJobWithoutNext(r.Context(), s.pool, jobID); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"ok": true})
}

type captureJobFailRequest struct {
	ErrorText    string `json:"error_text"`
	NextDelaySec int    `json:"next_delay_sec"`
}

func (s *Server) handleCaptureJobsFail(w http.ResponseWriter, r *http.Request) {
	jobID, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	var req captureJobFailRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if strings.TrimSpace(req.ErrorText) == "" {
		util.WriteError(w, http.StatusBadRequest, "error_text is required")
		return
	}
	if err := queue.FailCaptureJob(r.Context(), s.pool, jobID, req.ErrorText, req.NextDelaySec); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleCaptureWorkerStopped(w http.ResponseWriter, r *http.Request) {
	var req captureWorkerStoppedRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	workerID := strings.TrimSpace(req.WorkerID)
	if workerID == "" {
		util.WriteError(w, http.StatusBadRequest, "worker_id is required")
		return
	}
	executionClass, err := normalizeExecutionClassInput(req.ExecutionClass)
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if _, err := s.pool.Exec(r.Context(), `
		DELETE FROM capture_worker_heartbeats
		WHERE worker_id=$1 AND execution_class=$2
	`, workerID, executionClass); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("delete capture worker heartbeat: %v", err))
		return
	}
	if _, err := s.pool.Exec(r.Context(), `
		DELETE FROM processing_worker_heartbeats
		WHERE worker_id=$1 AND worker_kind='capture' AND execution_class=$2 AND pipeline_id=''
	`, workerID, executionClass); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("delete capture processing heartbeat: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"ok": true})
}

type recordingProcessHeartbeatRequest struct {
	StreamID       int64      `json:"stream_id"`
	ExecutionClass string     `json:"execution_class"`
	ServerID       string     `json:"server_id"`
	AssignmentRev  int64      `json:"assignment_revision"`
	ProcessID      string     `json:"process_id"`
	WorkerID       string     `json:"worker_id"`
	Status         string     `json:"status"`
	LeaseSec       int        `json:"lease_sec"`
	LastFrameAt    *time.Time `json:"last_frame_at"`
	ErrorText      string     `json:"error_text"`
	StartReason    string     `json:"start_reason"`
	RestartCount   int        `json:"restart_count"`
	LastHeartbeat  *time.Time `json:"last_heartbeat_at"`
}

type recordingProcessStoppedRequest struct {
	StreamID       int64  `json:"stream_id"`
	ProcessID      string `json:"process_id"`
	WorkerID       string `json:"worker_id"`
	ServerID       string `json:"server_id"`
	ExecutionClass string `json:"execution_class"`
	AssignmentRev  int64  `json:"assignment_revision"`
	FinalStatus    string `json:"final_status"`
	StopReason     string `json:"stop_reason"`
	ErrorText      string `json:"error_text"`
	StoppedAtRaw   string `json:"stopped_at"`
}

func normalizeRecordingProcessStatus(raw string) (string, bool) {
	v := strings.TrimSpace(strings.ToLower(raw))
	switch v {
	case "starting", "running", "stopped", "crashed", "failed":
		return v, true
	default:
		return "", false
	}
}

func (s *Server) handleRecordingProcessHeartbeat(w http.ResponseWriter, r *http.Request) {
	var req recordingProcessHeartbeatRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.StreamID <= 0 {
		util.WriteError(w, http.StatusBadRequest, "stream_id must be > 0")
		return
	}
	executionClass, err := normalizeExecutionClassInput(req.ExecutionClass)
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	serverID := strings.TrimSpace(req.ServerID)
	if serverID == "" {
		util.WriteError(w, http.StatusBadRequest, "server_id is required")
		return
	}
	processID := strings.TrimSpace(req.ProcessID)
	if processID == "" {
		util.WriteError(w, http.StatusBadRequest, "process_id is required")
		return
	}
	workerID := strings.TrimSpace(req.WorkerID)
	if workerID == "" {
		util.WriteError(w, http.StatusBadRequest, "worker_id is required")
		return
	}
	status, ok := normalizeRecordingProcessStatus(req.Status)
	if !ok {
		util.WriteError(w, http.StatusBadRequest, "status must be one of starting|running|stopped|crashed|failed")
		return
	}
	if status == "stopped" || status == "crashed" || status == "failed" {
		util.WriteError(w, http.StatusBadRequest, "heartbeat status must be starting|running")
		return
	}
	leaseSec := req.LeaseSec
	if leaseSec <= 0 {
		leaseSec = 20
	}
	if leaseSec > 3600 {
		util.WriteError(w, http.StatusBadRequest, "lease_sec must be <= 3600")
		return
	}
	restartCount := req.RestartCount
	if restartCount < 0 {
		restartCount = 0
	}
	lastHeartbeat := time.Now().UTC()
	if req.LastHeartbeat != nil && !req.LastHeartbeat.IsZero() {
		lastHeartbeat = req.LastHeartbeat.UTC()
	}
	errorText := strings.TrimSpace(req.ErrorText)
	startReason := strings.TrimSpace(req.StartReason)
	if startReason == "" {
		startReason = "supervisor_start"
	}
	var currentAssignmentRev int64
	if err := s.pool.QueryRow(r.Context(), `
		SELECT assignment_revision
		FROM recording_assignments
		WHERE stream_id=$1
		  AND server_id=$2
		  AND execution_class=$3
	`, req.StreamID, serverID, executionClass).Scan(&currentAssignmentRev); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			util.WriteJSON(w, http.StatusConflict, map[string]any{
				"error":           "no active assignment for stream on this server/execution_class",
				"error_code":      "assignment_missing",
				"stream_id":       req.StreamID,
				"server_id":       serverID,
				"execution_class": executionClass,
			})
			return
		}
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load assignment revision: %v", err))
		return
	}
	assignmentRev := req.AssignmentRev
	if assignmentRev <= 0 {
		assignmentRev = currentAssignmentRev
	}
	if assignmentRev != currentAssignmentRev {
		util.WriteJSON(w, http.StatusConflict, map[string]any{
			"error":               "stale assignment revision",
			"error_code":          "stale_assignment_revision",
			"stream_id":           req.StreamID,
			"server_id":           serverID,
			"execution_class":     executionClass,
			"assignment_revision": assignmentRev,
			"current_revision":    currentAssignmentRev,
		})
		return
	}

	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("begin recording heartbeat tx: %v", err))
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()

	if _, err := tx.Exec(r.Context(), `
		UPDATE recording_process_runs
		SET status='stopped',
			stop_reason='superseded_by_heartbeat',
			stopped_at=COALESCE(stopped_at, now()),
			updated_at=now()
		WHERE stream_id=$1
		  AND status IN ('starting','running')
		  AND process_id <> $2
	`, req.StreamID, processID); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("stop superseded recording process runs: %v", err))
		return
	}

	if _, err := tx.Exec(r.Context(), `
		INSERT INTO recording_process_runs (
            stream_id, execution_class, server_id, process_id, worker_id, status,
			start_reason, started_at, last_heartbeat_at, last_frame_at,
			restart_count, last_error_text, assignment_revision, updated_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, now(), $8, $9, $10, $11, $12, now())
		ON CONFLICT (stream_id, process_id)
		DO UPDATE SET
			status=EXCLUDED.status,
			last_heartbeat_at=EXCLUDED.last_heartbeat_at,
			last_frame_at=COALESCE(EXCLUDED.last_frame_at, recording_process_runs.last_frame_at),
			restart_count=GREATEST(recording_process_runs.restart_count, EXCLUDED.restart_count),
			assignment_revision=EXCLUDED.assignment_revision,
			last_error_text=CASE
				WHEN EXCLUDED.last_error_text IS NULL OR EXCLUDED.last_error_text = '' THEN recording_process_runs.last_error_text
				ELSE EXCLUDED.last_error_text
			END,
			stopped_at=NULL,
			stop_reason='',
			updated_at=now()
	`, req.StreamID, executionClass, serverID, processID, workerID, status, startReason, lastHeartbeat, req.LastFrameAt, restartCount, errorText, assignmentRev); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("upsert recording process run: %v", err))
		return
	}

	if _, err := tx.Exec(r.Context(), `
		INSERT INTO stream_capture_runtime (stream_id, execution_class, status, last_resolved_at, last_frame_at, consecutive_errors, last_error_text)
		VALUES ($1, $2, 'running', now(), $3, 0, NULLIF($4, ''))
		ON CONFLICT (stream_id)
		DO UPDATE SET
			execution_class=EXCLUDED.execution_class,
			status='running',
			last_frame_at=COALESCE(EXCLUDED.last_frame_at, stream_capture_runtime.last_frame_at),
			last_error_text=CASE
				WHEN EXCLUDED.last_error_text IS NULL THEN stream_capture_runtime.last_error_text
				ELSE EXCLUDED.last_error_text
			END,
			updated_at=now()
	`, req.StreamID, executionClass, req.LastFrameAt, errorText); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("upsert stream runtime from process heartbeat: %v", err))
		return
	}

	if _, err := tx.Exec(r.Context(), `
		INSERT INTO processing_worker_heartbeats (
			worker_id, worker_kind, execution_class, pipeline_id, metadata_jsonb,
			heartbeat_at, lease_expires_at, updated_at
		)
		VALUES (
			$1, 'capture', $2, '',
			jsonb_build_object(
				'server_id', $3::text,
				'process_id', $4::text,
				'stream_id', $5::bigint,
				'process_name', 'recording-stream-runner'
			),
			$6::timestamptz, $6::timestamptz + make_interval(secs => $7::int), now()
		)
		ON CONFLICT (worker_id, worker_kind, execution_class, pipeline_id)
		DO UPDATE SET
			metadata_jsonb=EXCLUDED.metadata_jsonb,
			heartbeat_at=EXCLUDED.heartbeat_at,
			lease_expires_at=EXCLUDED.lease_expires_at,
			updated_at=now()
	`, workerID, executionClass, serverID, processID, req.StreamID, lastHeartbeat, leaseSec); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("upsert processing heartbeat for recording process: %v", err))
		return
	}
	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("commit recording heartbeat tx: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleRecordingProcessStopped(w http.ResponseWriter, r *http.Request) {
	var req recordingProcessStoppedRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.StreamID <= 0 {
		util.WriteError(w, http.StatusBadRequest, "stream_id must be > 0")
		return
	}
	processID := strings.TrimSpace(req.ProcessID)
	if processID == "" {
		util.WriteError(w, http.StatusBadRequest, "process_id is required")
		return
	}
	workerID := strings.TrimSpace(req.WorkerID)
	if workerID == "" {
		util.WriteError(w, http.StatusBadRequest, "worker_id is required")
		return
	}
	finalStatus, ok := normalizeRecordingProcessStatus(req.FinalStatus)
	if !ok {
		util.WriteError(w, http.StatusBadRequest, "final_status must be one of starting|running|stopped|crashed|failed")
		return
	}
	if finalStatus == "starting" || finalStatus == "running" {
		finalStatus = "stopped"
	}
	stopReason := strings.TrimSpace(req.StopReason)
	if stopReason == "" {
		stopReason = "worker_stopped"
	}
	errorText := strings.TrimSpace(req.ErrorText)
	stoppedAt := time.Now().UTC()
	stoppedAtRaw := strings.TrimSpace(req.StoppedAtRaw)
	if stoppedAtRaw != "" {
		layouts := []string{time.RFC3339Nano, time.RFC3339}
		for _, layout := range layouts {
			if parsed, err := time.Parse(layout, stoppedAtRaw); err == nil {
				stoppedAt = parsed.UTC()
				break
			}
		}
	}
	serverID := strings.TrimSpace(req.ServerID)
	executionClass, execErr := normalizeExecutionClassInput(strings.TrimSpace(req.ExecutionClass))
	if req.AssignmentRev > 0 && serverID != "" && execErr == nil {
		var currentRev int64
		if err := s.pool.QueryRow(r.Context(), `
			SELECT assignment_revision
			FROM recording_assignments
			WHERE stream_id=$1
			  AND server_id=$2
			  AND execution_class=$3
		`, req.StreamID, serverID, executionClass).Scan(&currentRev); err == nil {
			if currentRev != req.AssignmentRev {
				util.WriteJSON(w, http.StatusConflict, map[string]any{
					"error":               "stale assignment revision",
					"error_code":          "stale_assignment_revision",
					"stream_id":           req.StreamID,
					"server_id":           serverID,
					"execution_class":     executionClass,
					"assignment_revision": req.AssignmentRev,
					"current_revision":    currentRev,
				})
				return
			}
		}
	}

	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("begin recording stop tx: %v", err))
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()

	if _, err := tx.Exec(r.Context(), `
		UPDATE recording_process_runs
		SET
			status=$4,
			stop_reason=$5,
			stopped_at=COALESCE(stopped_at, $6),
			last_error_text=CASE WHEN $7 = '' THEN last_error_text ELSE $7 END,
			updated_at=now()
		WHERE stream_id=$1 AND process_id=$2 AND worker_id=$3
	`, req.StreamID, processID, workerID, finalStatus, stopReason, stoppedAt, errorText); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("update recording process stop: %v", err))
		return
	}

	if _, err := tx.Exec(r.Context(), `
		DELETE FROM processing_worker_heartbeats
		WHERE worker_id=$1 AND worker_kind='capture' AND pipeline_id=''
	`, workerID); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("cleanup processing heartbeat for stopped process: %v", err))
		return
	}

	if _, err := tx.Exec(r.Context(), `
		INSERT INTO stream_capture_runtime (stream_id, status, last_error_text)
		SELECT $1, 'stopped', NULLIF($2, '')
		WHERE NOT EXISTS (
			SELECT 1
			FROM recording_process_runs rpr
			WHERE rpr.stream_id=$1
			  AND rpr.process_id <> $3
			  AND rpr.status IN ('starting','running')
			  AND rpr.stopped_at IS NULL
			  AND COALESCE(rpr.last_heartbeat_at, rpr.updated_at, rpr.started_at) >= now() - interval '120 seconds'
		)
		ON CONFLICT (stream_id)
		DO UPDATE SET
			status='stopped',
			last_error_text=CASE
				WHEN EXCLUDED.last_error_text IS NULL THEN stream_capture_runtime.last_error_text
				ELSE EXCLUDED.last_error_text
			END,
			updated_at=now()
	`, req.StreamID, errorText, processID); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("mark stream runtime stopped from process stop: %v", err))
		return
	}

	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("commit recording stop tx: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"ok": true})
}

type processingWorkerHeartbeatRequest struct {
	WorkerID       string         `json:"worker_id"`
	WorkerKind     string         `json:"worker_kind"`
	Mode           string         `json:"mode"`
	ExecutionClass string         `json:"execution_class"`
	PipelineID     string         `json:"pipeline_id"`
	LeaseSec       int            `json:"lease_sec"`
	MetadataJSON   map[string]any `json:"metadata_json"`
}

func normalizeWorkerKind(raw string) (string, bool) {
	v := strings.TrimSpace(strings.ToLower(raw))
	switch v {
	case "capture", "inference", "inference_box", "other":
		return v, true
	default:
		return "", false
	}
}

func (s *Server) handleProcessingWorkerHeartbeat(w http.ResponseWriter, r *http.Request) {
	var req processingWorkerHeartbeatRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	workerID := strings.TrimSpace(req.WorkerID)
	if workerID == "" {
		util.WriteError(w, http.StatusBadRequest, "worker_id is required")
		return
	}
	workerKind, ok := normalizeWorkerKind(req.WorkerKind)
	if !ok {
		util.WriteError(w, http.StatusBadRequest, "worker_kind must be one of capture|inference|inference_box|other")
		return
	}
	executionClass := strings.TrimSpace(strings.ToLower(req.ExecutionClass))
	if executionClass != "" {
		normalizedExecutionClass, err := normalizeExecutionClassInput(executionClass)
		if err != nil {
			util.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
		executionClass = normalizedExecutionClass
	}
	pipelineID := strings.TrimSpace(req.PipelineID)
	leaseSec := req.LeaseSec
	if leaseSec <= 0 {
		leaseSec = 45
	}
	if leaseSec > 3600 {
		util.WriteError(w, http.StatusBadRequest, "lease_sec must be <= 3600")
		return
	}
	metaBytes, err := json.Marshal(nonNilMap(req.MetadataJSON))
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, fmt.Sprintf("invalid metadata_json: %v", err))
		return
	}

	if _, err := s.pool.Exec(r.Context(), `
		INSERT INTO processing_worker_heartbeats (
			worker_id, worker_kind, execution_class, pipeline_id, metadata_jsonb,
			heartbeat_at, lease_expires_at, updated_at
		)
		VALUES ($1, $2, $3, $4, $5, now(), now() + make_interval(secs => $6), now())
		ON CONFLICT (worker_id, worker_kind, execution_class, pipeline_id)
		DO UPDATE SET
			metadata_jsonb=EXCLUDED.metadata_jsonb,
			heartbeat_at=EXCLUDED.heartbeat_at,
			lease_expires_at=EXCLUDED.lease_expires_at,
			updated_at=now()
	`, workerID, workerKind, executionClass, pipelineID, metaBytes, leaseSec); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("upsert processing worker heartbeat: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"ok": true})
}

type processingWorkerStoppedRequest struct {
	WorkerID       string `json:"worker_id"`
	WorkerKind     string `json:"worker_kind"`
	Mode           string `json:"mode"`
	ExecutionClass string `json:"execution_class"`
	PipelineID     string `json:"pipeline_id"`
}

func (s *Server) handleProcessingWorkerStopped(w http.ResponseWriter, r *http.Request) {
	var req processingWorkerStoppedRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	workerID := strings.TrimSpace(req.WorkerID)
	if workerID == "" {
		util.WriteError(w, http.StatusBadRequest, "worker_id is required")
		return
	}
	workerKind, ok := normalizeWorkerKind(req.WorkerKind)
	if !ok {
		util.WriteError(w, http.StatusBadRequest, "worker_kind must be one of capture|inference|inference_box|other")
		return
	}
	executionClass := strings.TrimSpace(strings.ToLower(req.ExecutionClass))
	if executionClass != "" {
		var err error
		executionClass, err = normalizeExecutionClassInput(executionClass)
		if err != nil {
			util.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	pipelineID := strings.TrimSpace(req.PipelineID)
	if _, err := s.pool.Exec(r.Context(), `
		DELETE FROM processing_worker_heartbeats
		WHERE worker_id=$1 AND worker_kind=$2 AND execution_class=$3 AND pipeline_id=$4
	`, workerID, workerKind, executionClass, pipelineID); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("delete processing worker heartbeat: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"ok": true})
}

type captureIngestRequest struct {
	StreamID           int64      `json:"stream_id"`
	Status             string     `json:"status"`
	EffectiveMode      string     `json:"execution_class"`
	ResolvedURL        string     `json:"resolved_url"`
	CapturedAt         *time.Time `json:"captured_at"`
	UploadIntentID     string     `json:"upload_intent_id"`
	ObjectKey          string     `json:"object_key"`
	SizeBytes          *int64     `json:"size_bytes"`
	ETag               string     `json:"etag"`
	SHA256             string     `json:"sha256"`
	SegmentStartAt     *time.Time `json:"segment_start_at"`
	SegmentEndAt       *time.Time `json:"segment_end_at"`
	DurationMs         *int64     `json:"duration_ms"`
	TargetFPS          *int       `json:"target_fps"`
	ActualFPS          *float64   `json:"actual_fps"`
	VideoCodec         string     `json:"video_codec"`
	AudioCodec         string     `json:"audio_codec"`
	Container          string     `json:"container"`
	AudioPresent       *bool      `json:"audio_present"`
	FrameBase64        string     `json:"frame_base64"`
	ThumbnailBase64    string     `json:"thumbnail_base64"`
	ThumbnailIntentID  string     `json:"thumbnail_upload_intent_id"`
	ThumbnailObjectKey string     `json:"thumbnail_object_key"`
	ThumbnailMimeType  string     `json:"thumbnail_mime_type"`
	ThumbnailSizeBytes *int64     `json:"thumbnail_size_bytes"`
	ThumbnailSHA256    string     `json:"thumbnail_sha256"`
	MimeType           string     `json:"mime_type"`
	SourceKind         string     `json:"source_kind"`
	CaptureError       string     `json:"capture_error"`
	ErrorText          string     `json:"error_text"`
	RecordingHeartbeat bool       `json:"recording_heartbeat"`
}

func (s *Server) handleCaptureIngest(w http.ResponseWriter, r *http.Request) {
	var req captureIngestRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.StreamID <= 0 {
		util.WriteError(w, http.StatusBadRequest, "stream_id must be > 0")
		return
	}
	executionClass, err := normalizeExecutionClassInput(req.EffectiveMode)
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	req.Status = strings.TrimSpace(strings.ToLower(req.Status))
	req.SourceKind = strings.TrimSpace(req.SourceKind)
	if req.SourceKind == "" {
		req.SourceKind = "live"
	}
	req.CaptureError = strings.TrimSpace(req.CaptureError)
	if req.CaptureError == "" {
		req.CaptureError = strings.TrimSpace(req.ErrorText)
	}
	intentID, err := parseUUIDString(req.UploadIntentID)
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, "upload_intent_id must be a uuid")
		return
	}
	req.UploadIntentID = intentID
	req.FrameBase64 = strings.TrimSpace(req.FrameBase64)
	req.ThumbnailBase64 = strings.TrimSpace(req.ThumbnailBase64)
	req.ThumbnailIntentID = strings.TrimSpace(req.ThumbnailIntentID)
	req.ThumbnailObjectKey = strings.TrimSpace(req.ThumbnailObjectKey)
	if req.Status == "" {
		if req.CaptureError != "" {
			req.Status = "error"
		} else {
			req.Status = "success"
		}
	}
	if req.Status != "success" && req.Status != "error" {
		util.WriteError(w, http.StatusBadRequest, "status must be success or error")
		return
	}
	if req.Status == "error" && req.CaptureError == "" {
		util.WriteError(w, http.StatusBadRequest, "error_text or capture_error is required when status=error")
		return
	}
	hasSegmentUpload := req.UploadIntentID != "" || strings.TrimSpace(req.ObjectKey) != ""
	if req.Status == "success" && req.FrameBase64 == "" && !hasSegmentUpload {
		util.WriteError(w, http.StatusBadRequest, "frame_base64 or clip upload reference is required when status=success")
		return
	}
	if req.CaptureError == "" && req.FrameBase64 == "" && !hasSegmentUpload {
		util.WriteError(w, http.StatusBadRequest, "frame_base64, clip upload reference, or capture_error is required")
		return
	}
	if req.CaptureError != "" && (req.FrameBase64 != "" || hasSegmentUpload) {
		util.WriteError(w, http.StatusBadRequest, "provide either success payload or capture_error, not both")
		return
	}

	if req.Status == "error" {
		if executionClass != capture.ExecutionClassImagePoll {
			consecutive, err := s.persistCaptureSegmentError(r.Context(), req.StreamID, executionClass, strings.TrimSpace(req.ResolvedURL), req.SourceKind, req.CaptureError)
			if err != nil {
				util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("persist capture segment error: %v", err))
				return
			}
			util.WriteJSON(w, http.StatusOK, map[string]any{
				"ok":                 true,
				"status":             capture.RuntimeError,
				"consecutive_errors": consecutive,
				"unsupported":        false,
			})
			return
		}
		consecutive, err := s.persistCaptureError(r.Context(), req.StreamID, executionClass, strings.TrimSpace(req.ResolvedURL), req.SourceKind, req.CaptureError)
		if err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("persist capture error: %v", err))
			return
		}
		util.WriteJSON(w, http.StatusOK, map[string]any{
			"ok":                 true,
			"status":             capture.RuntimeError,
			"consecutive_errors": consecutive,
			"unsupported":        false,
		})
		return
	}

	if executionClass != capture.ExecutionClassImagePoll {
		if req.UploadIntentID == "" && strings.TrimSpace(req.ObjectKey) == "" {
			util.WriteError(w, http.StatusBadRequest, "upload_intent_id or object_key is required for live-video clip ingest")
			return
		}
		if req.SegmentStartAt == nil || req.SegmentEndAt == nil || req.SegmentStartAt.IsZero() || req.SegmentEndAt.IsZero() {
			util.WriteError(w, http.StatusBadRequest, "segment_start_at and segment_end_at are required for live-video clip ingest")
			return
		}
		targetFPS := capture.SegmentTargetFPS
		if req.TargetFPS != nil && *req.TargetFPS != capture.SegmentTargetFPS {
			util.WriteError(w, http.StatusBadRequest, fmt.Sprintf("target_fps must be %d", capture.SegmentTargetFPS))
			return
		}
		if req.ThumbnailBase64 != "" {
			util.WriteError(w, http.StatusBadRequest, "thumbnail_base64 is not accepted for live-video clip ingest; upload thumbnails directly to R2")
			return
		}
		durationMs := int64(req.SegmentEndAt.UTC().Sub(req.SegmentStartAt.UTC()) / time.Millisecond)
		if req.DurationMs != nil && *req.DurationMs > 0 {
			durationMs = *req.DurationMs
		}
		audioPresent := false
		if req.AudioPresent != nil {
			audioPresent = *req.AudioPresent
		}
		sizeBytes := int64(0)
		if req.SizeBytes != nil && *req.SizeBytes > 0 {
			sizeBytes = *req.SizeBytes
		}
		thumbnailSizeBytes := int64(0)
		if req.ThumbnailSizeBytes != nil && *req.ThumbnailSizeBytes > 0 {
			thumbnailSizeBytes = *req.ThumbnailSizeBytes
		}
		if err := s.persistCaptureSegmentSuccess(r.Context(), req.StreamID, captureSegmentFinalize{
			IntentID:           req.UploadIntentID,
			ObjectKey:          strings.TrimSpace(req.ObjectKey),
			MIMEType:           strings.TrimSpace(req.MimeType),
			SizeBytes:          sizeBytes,
			ETag:               strings.TrimSpace(req.ETag),
			SHA256:             strings.TrimSpace(req.SHA256),
			SegmentStartAt:     req.SegmentStartAt.UTC(),
			SegmentEndAt:       req.SegmentEndAt.UTC(),
			DurationMs:         durationMs,
			TargetFPS:          targetFPS,
			ActualFPS:          req.ActualFPS,
			VideoCodec:         strings.TrimSpace(req.VideoCodec),
			AudioCodec:         strings.TrimSpace(req.AudioCodec),
			Container:          firstNonEmpty(strings.TrimSpace(req.Container), "mp4"),
			AudioPresent:       audioPresent,
			SourceKind:         req.SourceKind,
			ExecutionClass:     executionClass,
			ResolvedURL:        strings.TrimSpace(req.ResolvedURL),
			CaptureType:        capture.ResolvedCaptureTypeFromURL(strings.TrimSpace(req.ResolvedURL)),
			ThumbnailIntentID:  req.ThumbnailIntentID,
			ThumbnailObjectKey: req.ThumbnailObjectKey,
			ThumbnailMIME:      strings.TrimSpace(req.ThumbnailMimeType),
			ThumbnailSizeBytes: thumbnailSizeBytes,
			ThumbnailSHA256:    strings.TrimSpace(req.ThumbnailSHA256),
		}); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("persist capture segment success: %v", err))
			return
		}
		util.WriteJSON(w, http.StatusOK, map[string]any{
			"ok":                 true,
			"status":             capture.RuntimeRunning,
			"consecutive_errors": 0,
			"unsupported":        false,
		})
		return
	}

	frameBytes, err := base64.StdEncoding.DecodeString(req.FrameBase64)
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, fmt.Sprintf("decode frame_base64: %v", err))
		return
	}
	mimeType := strings.TrimSpace(req.MimeType)
	if mimeType == "" {
		mimeType = http.DetectContentType(frameBytes)
		if !strings.HasPrefix(mimeType, "image/") {
			mimeType = "image/jpeg"
		}
	}
	frame, err := capture.BuildFrameFromBytes(frameBytes, mimeType, req.SourceKind)
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, fmt.Sprintf("invalid frame payload: %v", err))
		return
	}
	capturedAt := time.Now().UTC()
	if req.CapturedAt != nil && !req.CapturedAt.IsZero() {
		capturedAt = req.CapturedAt.UTC()
	}
	if err := s.persistCaptureSuccess(r.Context(), req.StreamID, executionClass, strings.TrimSpace(req.ResolvedURL), capturedAt, frame, req.RecordingHeartbeat); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("persist capture success: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{
		"ok":                 true,
		"status":             capture.RuntimeRunning,
		"consecutive_errors": 0,
		"unsupported":        false,
	})
}

type captureMarkUnsupportedRequest struct {
	StreamID          int64  `json:"stream_id"`
	EffectiveMode     string `json:"execution_class"`
	ResolvedURL       string `json:"resolved_url"`
	Reason            string `json:"reason"`
	ConsecutiveErrors int    `json:"consecutive_errors"`
}

func (s *Server) handleCaptureMarkUnsupported(w http.ResponseWriter, r *http.Request) {
	var req captureMarkUnsupportedRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.StreamID <= 0 {
		util.WriteError(w, http.StatusBadRequest, "stream_id must be > 0")
		return
	}
	reason := strings.TrimSpace(req.Reason)
	if reason == "" {
		util.WriteError(w, http.StatusBadRequest, "reason is required")
		return
	}
	executionClass := ""
	if strings.TrimSpace(req.EffectiveMode) != "" {
		var err error
		executionClass, err = normalizeExecutionClassInput(req.EffectiveMode)
		if err != nil {
			util.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	consecutive := req.ConsecutiveErrors
	if consecutive <= 0 {
		consecutive = s.cfg.CaptureUnsupportedThreshold
		if consecutive <= 0 {
			consecutive = 1
		}
	}

	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("begin unsupported tx: %v", err))
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()
	var executionClassArg any
	if executionClass != "" {
		executionClassArg = executionClass
	}
	resolvedCaptureType := capture.ResolvedCaptureTypeFromURL(strings.TrimSpace(req.ResolvedURL))
	var resolvedCaptureTypeArg any
	if resolvedCaptureType != "" {
		resolvedCaptureTypeArg = resolvedCaptureType
	}

	if _, err := tx.Exec(r.Context(), `
		INSERT INTO stream_capture_runtime (stream_id, execution_class, resolved_capture_type, resolved_url, status, last_resolved_at, last_frame_at, consecutive_errors, last_error_text)
		VALUES ($1, $2, $3, $4, 'unsupported', now(), NULL, $5, $6)
		ON CONFLICT (stream_id)
		DO UPDATE SET
			execution_class=EXCLUDED.execution_class,
			resolved_capture_type=COALESCE(EXCLUDED.resolved_capture_type, stream_capture_runtime.resolved_capture_type),
			resolved_url=COALESCE(NULLIF(EXCLUDED.resolved_url,''), stream_capture_runtime.resolved_url),
			status='unsupported',
			consecutive_errors=GREATEST(stream_capture_runtime.consecutive_errors, EXCLUDED.consecutive_errors),
			last_error_text=EXCLUDED.last_error_text,
			updated_at=now()
	`, req.StreamID, executionClassArg, resolvedCaptureTypeArg, strings.TrimSpace(req.ResolvedURL), consecutive, reason); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("mark runtime unsupported: %v", err))
		return
	}
	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("commit unsupported tx: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) persistCaptureSuccess(ctx context.Context, streamID int64, executionClass string, resolvedURL string, capturedAt time.Time, frame capture.Frame, recordingHeartbeat bool) error {
	objectKey := fmt.Sprintf("raw/stream/%d/%04d/%02d/%02d/live-%d.jpg",
		streamID, capturedAt.Year(), int(capturedAt.Month()), capturedAt.Day(), capturedAt.UnixNano())
	etag, err := s.r2.PutBytes(ctx, objectKey, frame.MIMEType, frame.Bytes)
	if err != nil {
		return fmt.Errorf("upload frame: %w", err)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin success tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	mediaID, err := storage.UpsertMediaObject(ctx, tx, storage.MediaObjectInput{
		StorageProvider: "r2",
		Bucket:          s.r2.Bucket(),
		ObjectKey:       objectKey,
		MIMEType:        frame.MIMEType,
		SizeBytes:       frame.SizeBytes,
		ETag:            etag,
		SHA256:          frame.SHA256,
		Width:           frame.Width,
		Height:          frame.Height,
	})
	if err != nil {
		return fmt.Errorf("upsert media object: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO frames (stream_id, capture_job_id, captured_at, raw_media_object_id, capture_status, capture_error, source_kind)
		VALUES ($1, NULL, $2, $3, 'success', NULL, $4)
	`, streamID, capturedAt, mediaID, frame.SourceKind); err != nil {
		return fmt.Errorf("insert frame success: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO stream_health (stream_id, captures_total, captures_success, captures_error, last_capture_at, last_error_at, last_error_text)
		VALUES ($1, 1, 1, 0, $2, NULL, NULL)
		ON CONFLICT (stream_id)
		DO UPDATE SET
			captures_total=stream_health.captures_total+1,
			captures_success=stream_health.captures_success+1,
			last_capture_at=EXCLUDED.last_capture_at,
			last_error_at=NULL,
			last_error_text=NULL,
			updated_at=now()
	`, streamID, capturedAt); err != nil {
		return fmt.Errorf("update stream_health success: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO stream_capture_runtime (stream_id, execution_class, resolved_capture_type, resolved_url, status, last_resolved_at, last_frame_at, consecutive_errors, last_error_text)
		VALUES ($1, $2, $3, $4, 'running', now(), $5, 0, NULL)
		ON CONFLICT (stream_id)
		DO UPDATE SET
			execution_class=EXCLUDED.execution_class,
			resolved_capture_type=COALESCE(EXCLUDED.resolved_capture_type, stream_capture_runtime.resolved_capture_type),
			resolved_url=EXCLUDED.resolved_url,
			status='running',
			last_frame_at=EXCLUDED.last_frame_at,
			consecutive_errors=0,
			last_error_text=NULL,
			updated_at=now()
	`, streamID, executionClass, capture.ResolvedCaptureTypeFromURL(resolvedURL), resolvedURL, capturedAt); err != nil {
		return fmt.Errorf("update stream_capture_runtime success: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit success tx: %w", err)
	}
	return nil
}

func (s *Server) persistCaptureError(ctx context.Context, streamID int64, executionClass string, resolvedURL string, sourceKind string, captureErr string) (int, error) {
	errText := strings.TrimSpace(captureErr)
	if errText == "" {
		errText = "capture failed"
	}
	if sourceKind == "" {
		sourceKind = "live"
	}
	now := time.Now().UTC()

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin error tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `
		INSERT INTO frames (stream_id, capture_job_id, captured_at, raw_media_object_id, capture_status, capture_error, source_kind)
		VALUES ($1, NULL, $2, NULL, 'error', $3, $4)
	`, streamID, now, errText, sourceKind); err != nil {
		return 0, fmt.Errorf("insert error frame: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO stream_health (stream_id, captures_total, captures_success, captures_error, last_capture_at, last_error_at, last_error_text)
		VALUES ($1, 1, 0, 1, $2, $2, $3)
		ON CONFLICT (stream_id)
		DO UPDATE SET
			captures_total=stream_health.captures_total+1,
			captures_error=stream_health.captures_error+1,
			last_capture_at=EXCLUDED.last_capture_at,
			last_error_at=EXCLUDED.last_error_at,
			last_error_text=EXCLUDED.last_error_text,
			updated_at=now()
	`, streamID, now, errText); err != nil {
		return 0, fmt.Errorf("update stream_health error: %w", err)
	}

	var consecutive int
	if err := tx.QueryRow(ctx, `
		INSERT INTO stream_capture_runtime (stream_id, execution_class, resolved_capture_type, resolved_url, status, last_resolved_at, last_frame_at, consecutive_errors, last_error_text)
		VALUES ($1, $2, $3, $4, 'error', now(), NULL, 1, $5)
		ON CONFLICT (stream_id)
		DO UPDATE SET
			execution_class=EXCLUDED.execution_class,
			resolved_capture_type=COALESCE(EXCLUDED.resolved_capture_type, stream_capture_runtime.resolved_capture_type),
			resolved_url=COALESCE(NULLIF(EXCLUDED.resolved_url,''), stream_capture_runtime.resolved_url),
			status='error',
			consecutive_errors=stream_capture_runtime.consecutive_errors+1,
			last_error_text=EXCLUDED.last_error_text,
			updated_at=now()
		RETURNING consecutive_errors
	`, streamID, executionClass, capture.ResolvedCaptureTypeFromURL(resolvedURL), resolvedURL, errText).Scan(&consecutive); err != nil {
		return 0, fmt.Errorf("update stream_capture_runtime error: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit error tx: %w", err)
	}
	return consecutive, nil
}

func (s *Server) handleDashboardOverview(w http.ResponseWriter, r *http.Request) {
	var streamsTotal, recordingOn, recordingOff int64
	if err := s.pool.QueryRow(r.Context(), `
		SELECT
			COUNT(*)::bigint,
			COUNT(*) FILTER (WHERE recording_state='on')::bigint,
			COUNT(*) FILTER (WHERE recording_state='off')::bigint
		FROM streams
	`).Scan(&streamsTotal, &recordingOn, &recordingOff); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("query stream counts: %v", err))
		return
	}
	recordingSettings, err := settings.GetRecordingSettings(r.Context(), s.pool)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	_ = recordingSettings
	type overviewStreamHealthRow struct {
		StreamID       int64
		ExecutionClass string
		RuntimeClass   *string
		LastSeen       *time.Time
	}
	summaryRows, err := s.pool.Query(r.Context(), `
		SELECT
			s.id,
			s.execution_class,
			rt.execution_class,
			COALESCE(rt.last_frame_at, sh.last_capture_at) AS last_seen
		FROM streams s
		LEFT JOIN stream_capture_runtime rt ON rt.stream_id=s.id
		LEFT JOIN stream_health sh ON sh.stream_id=s.id
		WHERE s.recording_state='on'
	`)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("query recording health counts: %v", err))
		return
	}
	overviewItems := make([]overviewStreamHealthRow, 0, recordingOn)
	frameIDs := make([]int64, 0, recordingOn)
	clipIDs := make([]int64, 0, recordingOn)
	for summaryRows.Next() {
		var row overviewStreamHealthRow
		if err := summaryRows.Scan(&row.StreamID, &row.ExecutionClass, &row.RuntimeClass, &row.LastSeen); err != nil {
			summaryRows.Close()
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan recording health counts: %v", err))
			return
		}
		overviewItems = append(overviewItems, row)
		if isClipNativeExecutionClass(firstNonEmpty(row.ExecutionClass, derefString(row.RuntimeClass))) {
			clipIDs = append(clipIDs, row.StreamID)
		} else {
			frameIDs = append(frameIDs, row.StreamID)
		}
	}
	summaryRows.Close()
	if summaryRows.Err() != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("iterate recording health counts: %v", summaryRows.Err()))
		return
	}
	success2h, err := s.successCaptureCountsSince(r.Context(), frameIDs, clipIDs, 2*time.Hour)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("query recording health counters 2h: %v", err))
		return
	}
	processIssueCounts2h, err := s.recordingProcessIssueCountsSince(r.Context(), 2*time.Hour)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("query recording process issue counters: %v", err))
		return
	}
	outageEpisodes2h, err := s.outageEpisodeCountsSince(r.Context(), frameIDs, clipIDs, 2*time.Hour, 0)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("query recording outage counters: %v", err))
		return
	}
	var recHealthy, recDegraded, recStale int64
	now := time.Now().UTC()
	for _, row := range overviewItems {
		mode := firstNonEmpty(row.ExecutionClass, derefString(row.RuntimeClass))
		if row.LastSeen == nil {
			recStale++
			continue
		}
		state, _, _ := classifyRecordingSupervision(now, recordingSupervisionInput{
			RecordingState:  "on",
			ModeClass:       mode,
			ServerID:        "assigned",
			RuntimeStatus:   "running",
			LastFrameAt:     row.LastSeen,
			StreamUpdatedAt: now,
			Metrics: recordingSupervisionMetrics{
				LossRate2h:       lossRateForWindow(expectedCapturesForWindow(mode, settings.DefaultRecordingIntervalSec, 2*time.Hour), success2h[row.StreamID]),
				ProcessIssues2h:  processIssueCounts2h[row.StreamID],
				OutageEpisodes2h: outageEpisodes2h[row.StreamID],
			},
		})
		switch state {
		case "down_10m":
			recStale++
		case "spotty_2h":
			recDegraded++
		default:
			recHealthy++
		}
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{
		"streams_total":            streamsTotal,
		"recording_on":             recordingOn,
		"recording_off":            recordingOff,
		"recording_interval_sec":   settings.DefaultRecordingIntervalSec,
		"recording_healthy_total":  recHealthy,
		"recording_degraded_total": recDegraded,
		"recording_stale_total":    recStale,
	})
}

func dashboardCountryExprSQL() string {
	return "TRIM(COALESCE(NULLIF(s.location_country, ''), NULLIF(s.metadata_jsonb->>'country', '')))"
}

func dashboardCityExprSQL() string {
	return "TRIM(COALESCE(NULLIF(s.location_city, ''), NULLIF(split_part(COALESCE(s.location_text,''), ',', 1), ''), NULLIF(s.metadata_jsonb->>'city',''), NULLIF(s.metadata_jsonb->>'locality',''), NULLIF(s.metadata_jsonb->>'town',''), NULLIF(s.metadata_jsonb->>'municipality','')))"
}

func dashboardSourceExprSQL() string {
	return "LOWER(TRIM(CASE WHEN s.capture_type='youtube_watch' THEN 'youtube' ELSE COALESCE(NULLIF(s.provider, ''), NULLIF(s.metadata_jsonb->>'discovery_provider', '')) END))"
}

func dashboardKoreaFamilyPredicateSQL(family string) (string, bool) {
	provider := "UPPER(REPLACE(TRIM(COALESCE(s.provider, '')), ' ', '_'))"
	sourceURL := "LOWER(COALESCE(s.source_url, ''))"
	sourcePageURL := "LOWER(COALESCE(s.source_page_url, ''))"
	sourceFamily := "LOWER(COALESCE(s.source_family, ''))"
	switch family {
	case "utic":
		return fmt.Sprintf("(%s IN ('UTIC','POLICE','UTIC_POLICE') OR %s='utic' OR %s LIKE '%%ktict.co.kr%%' OR %s LIKE '%%koroad%%' OR %s LIKE '%%utic.go.kr%%')", provider, sourceFamily, sourceURL, sourceURL, sourcePageURL), true
	case "topis":
		return fmt.Sprintf("(%s='TOPIS' OR %s='topis' OR %s LIKE '%%topiscctv%%' OR %s LIKE '%%topis.seoul.go.kr%%')", provider, sourceFamily, sourceURL, sourcePageURL), true
	case "spatic":
		return fmt.Sprintf("(%s='SPATIC' OR %s='spatic' OR %s LIKE '%%spatic.go.kr%%' OR %s LIKE '%%spatic.go.kr%%')", provider, sourceFamily, sourceURL, sourcePageURL), true
	case "kbs":
		return fmt.Sprintf("(%s='KBS' OR %s='kbs' OR %s LIKE '%%loomex.net%%' OR %s LIKE '%%d.kbs.co.kr%%')", provider, sourceFamily, sourceURL, sourcePageURL), true
	case "gigaeyes":
		return fmt.Sprintf("(%s='GIGAEYES' OR %s='gigaeyes' OR (%s LIKE '%%youtube.com%%' AND %s LIKE '%%@gigaeyeslivetv%%'))", provider, sourceFamily, sourcePageURL, sourcePageURL), true
	default:
		return "", false
	}
}

func dashboardKoreaFamilyWhereSQL(raw string) (string, error) {
	family := strings.TrimSpace(strings.ToLower(raw))
	if family == "" {
		return "", nil
	}
	if family != "all" {
		predicate, ok := dashboardKoreaFamilyPredicateSQL(family)
		if !ok {
			return "", fmt.Errorf("invalid korea_family; expected all|utic|topis|spatic|kbs|gigaeyes")
		}
		return predicate, nil
	}
	families := []string{"utic", "topis", "spatic", "kbs", "gigaeyes"}
	predicates := make([]string, 0, len(families))
	for _, name := range families {
		predicate, _ := dashboardKoreaFamilyPredicateSQL(name)
		predicates = append(predicates, predicate)
	}
	return "(" + strings.Join(predicates, " OR ") + ")", nil
}

func dashboardYouTubeChannelExprSQL() string {
	return "TRIM(COALESCE(NULLIF(s.metadata_jsonb->>'channel', ''), NULLIF(s.metadata_jsonb->>'uploader', ''), NULLIF(s.metadata_jsonb->>'channel_name', ''), NULLIF(s.metadata_jsonb->>'uploader_id', ''), NULLIF(s.metadata_jsonb->>'author', ''), CASE WHEN POSITION(':' IN s.name) BETWEEN 2 AND 64 THEN TRIM(SPLIT_PART(s.name, ':', 1)) WHEN POSITION('|' IN s.name) BETWEEN 2 AND 64 THEN TRIM(SPLIT_PART(s.name, '|', 1)) ELSE '' END))"
}

type dashboardStreamWhereConfig struct {
	IncludeSearch         bool
	IncludeSource         bool
	IncludeYouTubeChannel bool
	IncludeCaptureMode    bool
}

func dashboardBuildStreamWhereFromRequest(r *http.Request, cfg dashboardStreamWhereConfig) ([]string, []any, error) {
	recordingStateRaw := strings.TrimSpace(strings.ToLower(r.URL.Query().Get("recording_state")))
	tab := strings.TrimSpace(strings.ToLower(r.URL.Query().Get("tab")))
	q := strings.TrimSpace(r.URL.Query().Get("q"))
	country := strings.TrimSpace(r.URL.Query().Get("country"))
	city := strings.TrimSpace(r.URL.Query().Get("city"))
	source := strings.TrimSpace(strings.ToLower(r.URL.Query().Get("source")))
	koreaFamily := strings.TrimSpace(strings.ToLower(r.URL.Query().Get("korea_family")))
	youtubeChannel := strings.TrimSpace(r.URL.Query().Get("youtube_channel"))
	captureModeRaw := strings.TrimSpace(strings.ToLower(r.URL.Query().Get("capture_type")))
	touchedPipelineID := strings.TrimSpace(r.URL.Query().Get("touched_pipeline_id"))
	tags := dedupeStrings(strings.Split(r.URL.Query().Get("tags"), ","))
	tagsNot := dedupeStrings(strings.Split(r.URL.Query().Get("tags_not"), ","))

	recordingState := (*model.RecordingState)(nil)
	if recordingStateRaw != "" {
		v, ok := parseRecordingState(recordingStateRaw)
		if !ok {
			return nil, nil, fmt.Errorf("invalid recording_state; expected off|on")
		}
		recordingState = &v
	}
	captureType := ""
	if captureModeRaw != "" {
		var err error
		captureType, err = normalizeCaptureTypeInput(captureModeRaw)
		if err != nil {
			return nil, nil, err
		}
	}

	where := []string{"1=1"}
	args := []any{}
	if recordingState != nil {
		args = append(args, string(*recordingState))
		where = append(where, fmt.Sprintf("s.recording_state=$%d", len(args)))
	} else if tab == "recording" || tab == "recordings" {
		where = append(where, "s.recording_state='on'")
	}
	if cfg.IncludeSearch && q != "" {
		args = append(args, "%"+q+"%")
		where = append(where, fmt.Sprintf("(CAST(s.id AS text) ILIKE $%d OR s.provider ILIKE $%d OR s.name ILIKE $%d OR s.slug ILIKE $%d OR COALESCE(s.location_text,'') ILIKE $%d OR COALESCE(s.location_country,'') ILIKE $%d OR COALESCE(s.location_city,'') ILIKE $%d)", len(args), len(args), len(args), len(args), len(args), len(args), len(args)))
	}
	if len(tags) > 0 {
		args = append(args, tags)
		where = append(where, fmt.Sprintf("COALESCE(s.tags, ARRAY[]::text[]) && $%d::text[]", len(args)))
	}
	if len(tagsNot) > 0 {
		args = append(args, tagsNot)
		where = append(where, fmt.Sprintf("NOT (COALESCE(s.tags, ARRAY[]::text[]) && $%d::text[])", len(args)))
	}
	if country != "" {
		args = append(args, strings.ToLower(country))
		where = append(where, fmt.Sprintf("LOWER(%s) = $%d", dashboardCountryExprSQL(), len(args)))
	}
	if city != "" {
		args = append(args, strings.ToLower(city))
		where = append(where, fmt.Sprintf("LOWER(%s) = $%d", dashboardCityExprSQL(), len(args)))
	}
	if cfg.IncludeSource && source != "" {
		args = append(args, source)
		where = append(where, fmt.Sprintf("%s = $%d", dashboardSourceExprSQL(), len(args)))
	}
	if koreaFamilyWhere, err := dashboardKoreaFamilyWhereSQL(koreaFamily); err != nil {
		return nil, nil, err
	} else if koreaFamilyWhere != "" {
		where = append(where, koreaFamilyWhere)
	}
	if cfg.IncludeYouTubeChannel && youtubeChannel != "" {
		args = append(args, strings.ToLower(youtubeChannel))
		where = append(where, fmt.Sprintf("%s='youtube' AND LOWER(%s) = $%d", dashboardSourceExprSQL(), dashboardYouTubeChannelExprSQL(), len(args)))
	}
	if cfg.IncludeCaptureMode && captureModeRaw != "" {
		args = append(args, captureType)
		where = append(where, fmt.Sprintf("s.capture_type=$%d", len(args)))
	}
	if touchedPipelineID != "" {
		args = append(args, touchedPipelineID)
		where = append(where, fmt.Sprintf(`EXISTS (
			SELECT 1
			FROM frames f_touch
			JOIN inference_results ir_touch ON ir_touch.frame_id=f_touch.id
			WHERE f_touch.stream_id=s.id AND ir_touch.pipeline_id=$%d
		)`, len(args)))
	}
	return where, args, nil
}

func (s *Server) handleDashboardStreams(w http.ResponseWriter, r *http.Request) {
	limit := parseIntQuery(r, "limit", 300, 1, 2000)
	offset := parseIntQuery(r, "offset", 0, 0, 1000000)
	includeImageURLs := true
	switch strings.TrimSpace(strings.ToLower(r.URL.Query().Get("include_image_urls"))) {
	case "0", "false", "no":
		includeImageURLs = false
	}
	orderColumns := map[string]string{
		"avg_people_per_inferenced_capture": "COALESCE(sis.avg_people_per_inferenced_capture, 0)",
		"inferenced_captures":               "COALESCE(sis.inferenced_captures, 0)",
		"person_detections_total":           "COALESCE(sis.person_detections_total, 0)",
		"latest_captured_at":                "CASE WHEN s.execution_class='video_live' OR rt.execution_class='video_live' THEN COALESCE(rt.last_frame_at, sh.last_capture_at) ELSE sh.last_capture_at END",
		"captures_total":                    "COALESCE(sh.captures_total, 0)",
		"captures_success":                  "COALESCE(sh.captures_success, 0)",
		"captures_error":                    "COALESCE(sh.captures_error, 0)",
		"name":                              "s.name",
		"location":                          "COALESCE(s.location_text, '')",
		"location_country":                  dashboardCountryExprSQL(),
		"location_city":                     dashboardCityExprSQL(),
		"provider":                          "s.provider",
		"source":                            dashboardSourceExprSQL(),
		"youtube_channel":                   fmt.Sprintf("COALESCE(%s, '')", dashboardYouTubeChannelExprSQL()),
		"recording_state":                   "s.recording_state",
		"mode":                              "s.capture_type",
		"runtime_status":                    "COALESCE(rt.status, '')",
		"tags_count":                        "COALESCE(array_length(s.tags, 1), 0)",
		"id":                                "s.id",
	}
	orderExpr, _, sortDir, ok := parseSortQuery(w, r, orderColumns, "avg_people_per_inferenced_capture", "desc")
	if !ok {
		return
	}
	where, args, err := dashboardBuildStreamWhereFromRequest(r, dashboardStreamWhereConfig{
		IncludeSearch:         true,
		IncludeSource:         true,
		IncludeYouTubeChannel: true,
		IncludeCaptureMode:    true,
	})
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	recordingSettings, err := settings.GetRecordingSettings(r.Context(), s.pool)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	_ = recordingSettings

	whereSQL := strings.Join(where, " AND ")
	var total int64
	if err := s.pool.QueryRow(r.Context(), fmt.Sprintf(`
		SELECT COUNT(*)::bigint
		FROM streams s
		WHERE %s
	`, whereSQL), args...).Scan(&total); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("dashboard stream count query: %v", err))
		return
	}

	type item struct {
		Stream                        model.Stream `json:"stream"`
		LatestCaptured                *time.Time   `json:"latest_captured_at,omitempty"`
		LastCaptureAt                 *time.Time   `json:"last_capture_at,omitempty"`
		LatestFrameURL                string       `json:"latest_frame_url,omitempty"`
		CapturesTotal                 int64        `json:"captures_total"`
		CapturesSuccess               int64        `json:"captures_success"`
		CapturesError                 int64        `json:"captures_error"`
		InferencedCaptures            int64        `json:"inferenced_captures"`
		PersonDetectionsTotal         int64        `json:"person_detections_total"`
		AvgPeoplePerInferencedCapture float64      `json:"avg_people_per_inferenced_capture"`
		SuccessFrames60s              int64        `json:"success_frames_60s"`
		SuccessCaptures60s            int64        `json:"success_captures_60s"`
		TargetFPS                     int          `json:"target_fps"`
		ExpectedFrames60s             int64        `json:"expected_frames_60s"`
		ExpectedCaptures60s           int64        `json:"expected_captures_60s"`
		LossRatePct                   float64      `json:"loss_rate_pct"`
		FreshnessSec                  *int64       `json:"freshness_sec,omitempty"`
		RecordingHealth               string       `json:"recording_health"`
		CaptureUnit                   string       `json:"capture_unit"`
	}
	items := make([]item, 0, limit)
	args = append(args, limit, offset)
	rows, err := s.pool.Query(r.Context(), fmt.Sprintf(`
		SELECT
			s.id, s.provider, s.external_id, s.name, s.slug, s.source_url, s.source_page_url,
			s.source_family,
			s.capture_family, s.expected_fps, s.expected_image_interval_sec,
			s.lat, s.lon, s.location_text, s.location_country, s.location_country_code, s.location_region, s.location_city, s.location_locality, s.location_source, s.metadata_jsonb,
			s.recording_state, s.recording_failed_reason, s.recording_failed_at, s.capture_type, s.execution_class, s.execution_config_jsonb, s.tags,
			s.created_at, s.updated_at,
			sh.last_capture_at,
			COALESCE(sh.captures_total, 0),
			COALESCE(sh.captures_success, 0),
			COALESCE(sh.captures_error, 0),
			rt.status,
			rt.execution_class,
			rt.resolved_url,
			rt.last_frame_at,
			rt.consecutive_errors,
			rt.last_error_text,
			COALESCE(sis.inferenced_captures, 0)::bigint,
			COALESCE(sis.person_detections_total, 0)::bigint,
			COALESCE(sis.avg_people_per_inferenced_capture, 0)::double precision
		FROM streams s
		LEFT JOIN stream_health sh ON sh.stream_id = s.id
		LEFT JOIN stream_capture_runtime rt ON rt.stream_id = s.id
		LEFT JOIN stream_inference_stats sis ON sis.stream_id = s.id
		WHERE %s
			ORDER BY %s %s NULLS LAST, s.id ASC
			LIMIT $%d OFFSET $%d
		`, whereSQL, orderExpr, sortDir, len(args)-1, len(args)), args...)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("dashboard stream query: %v", err))
		return
	}
	defer rows.Close()
	for rows.Next() {
		var stream model.Stream
		var metaBytes []byte
		var cfgBytes []byte
		var state string
		var capturedAt *time.Time
		var capturesTotal, capturesSuccess, capturesError int64
		var inferencedCaptures, personDetectionsTotal int64
		var avgPeoplePerInferencedCapture float64
		var runtimeStatus, runtimeMode, runtimeResolved, runtimeError *string
		var runtimeLastFrame *time.Time
		var runtimeErrors *int
		if err := rows.Scan(
			&stream.ID, &stream.Provider, &stream.ExternalID, &stream.Name, &stream.Slug, &stream.SourceURL, &stream.SourcePageURL,
			&stream.SourceFamily,
			&stream.CaptureFamily, &stream.ExpectedFPS, &stream.ExpectedImageInterval,
			&stream.Lat, &stream.Lon, &stream.LocationText, &stream.LocationCountry, &stream.LocationCountryCode, &stream.LocationRegion, &stream.LocationCity, &stream.LocationLocality, &stream.LocationSource, &metaBytes,
			&state, &stream.RecordingFailedReason, &stream.RecordingFailedAt, &stream.CaptureType, &stream.ExecutionClass, &cfgBytes, &stream.Tags,
			&stream.CreatedAt, &stream.UpdatedAt,
			&capturedAt,
			&capturesTotal, &capturesSuccess, &capturesError,
			&runtimeStatus, &runtimeMode, &runtimeResolved, &runtimeLastFrame, &runtimeErrors, &runtimeError,
			&inferencedCaptures, &personDetectionsTotal, &avgPeoplePerInferencedCapture,
		); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan dashboard stream: %v", err))
			return
		}
		stream.RecordingState = model.RecordingState(state)
		if err := decodeStreamPayload(&stream, metaBytes, cfgBytes); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("decode stream metadata: %v", err))
			return
		}
		stream.CaptureRuntimeStatus = runtimeStatus
		stream.CaptureRuntimeClass = runtimeMode
		stream.CaptureRuntimeResolved = runtimeResolved
		stream.CaptureRuntimeLastSeen = runtimeLastFrame
		stream.CaptureRuntimeErrors = runtimeErrors
		stream.CaptureRuntimeError = runtimeError
		it := item{
			Stream:                        stream,
			LatestCaptured:                capturedAt,
			CapturesTotal:                 capturesTotal,
			CapturesSuccess:               capturesSuccess,
			CapturesError:                 capturesError,
			InferencedCaptures:            inferencedCaptures,
			PersonDetectionsTotal:         personDetectionsTotal,
			AvgPeoplePerInferencedCapture: avgPeoplePerInferencedCapture,
			CaptureUnit:                   captureUnitLabelForExecutionClass(firstNonEmpty(string(stream.ExecutionClass), derefString(runtimeMode))),
		}
		if isClipNativeExecutionClass(firstNonEmpty(string(stream.ExecutionClass), derefString(runtimeMode))) {
			it.TargetFPS = capture.SegmentTargetFPS
			it.ExpectedFrames60s = expectedCapturesPer60s(firstNonEmpty(string(stream.ExecutionClass), derefString(runtimeMode)), settings.DefaultRecordingIntervalSec)
			if runtimeLastFrame != nil {
				it.LatestCaptured = runtimeLastFrame
			}
		} else if stream.RecordingState == model.RecordingStateOn {
			it.TargetFPS = capture.SegmentTargetFPS
			it.ExpectedFrames60s = int64(it.TargetFPS) * 60
		} else {
			it.TargetFPS = capture.SegmentTargetFPS
			it.ExpectedFrames60s = int64(it.TargetFPS) * 60
		}
		items = append(items, it)
	}
	if rows.Err() != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("iterate dashboard streams: %v", rows.Err()))
		return
	}

	if len(items) > 0 {
		frameStreamIDs := make([]int64, 0, len(items))
		clipStreamIDs := make([]int64, 0, len(items))
		for _, it := range items {
			if isClipNativeExecutionClass(firstNonEmpty(string(it.Stream.ExecutionClass), derefString(it.Stream.CaptureRuntimeClass))) {
				clipStreamIDs = append(clipStreamIDs, it.Stream.ID)
			} else {
				frameStreamIDs = append(frameStreamIDs, it.Stream.ID)
			}
		}
		success10m, err := s.successCaptureCountsSince(r.Context(), frameStreamIDs, clipStreamIDs, 10*time.Minute)
		if err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("dashboard success counters query 10m: %v", err))
			return
		}
		success2h, err := s.successCaptureCountsSince(r.Context(), frameStreamIDs, clipStreamIDs, 2*time.Hour)
		if err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("dashboard success counters query 2h: %v", err))
			return
		}
		processIssueCounts2h, err := s.recordingProcessIssueCountsSince(r.Context(), 2*time.Hour)
		if err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("dashboard process issue counters query: %v", err))
			return
		}
		now := time.Now().UTC()
		for i := range items {
			items[i].SuccessFrames60s = success10m[items[i].Stream.ID]
			items[i].ExpectedFrames60s = expectedCapturesForWindow(firstNonEmpty(string(items[i].Stream.ExecutionClass), derefString(items[i].Stream.CaptureRuntimeClass)), settings.DefaultRecordingIntervalSec, 10*time.Minute)
			items[i].LossRatePct = lossRateForWindow(
				expectedCapturesForWindow(firstNonEmpty(string(items[i].Stream.ExecutionClass), derefString(items[i].Stream.CaptureRuntimeClass)), settings.DefaultRecordingIntervalSec, 2*time.Hour),
				success2h[items[i].Stream.ID],
			)
			lastFrame := items[i].Stream.CaptureRuntimeLastSeen
			if lastFrame == nil {
				lastFrame = items[i].LatestCaptured
			}
			if lastFrame != nil {
				fresh := int64(now.Sub(lastFrame.UTC()).Seconds())
				if fresh < 0 {
					fresh = 0
				}
				items[i].FreshnessSec = &fresh
			}
			switch items[i].Stream.RecordingState {
			case model.RecordingStateOn:
				state, _, _ := classifyRecordingSupervision(now, recordingSupervisionInput{
					RecordingState:  "on",
					ModeClass:       firstNonEmpty(string(items[i].Stream.ExecutionClass), derefString(items[i].Stream.CaptureRuntimeClass)),
					ServerID:        "assigned",
					RuntimeStatus:   strings.TrimSpace(derefString(items[i].Stream.CaptureRuntimeStatus)),
					LastFrameAt:     lastFrame,
					StreamUpdatedAt: items[i].Stream.UpdatedAt,
					Metrics: recordingSupervisionMetrics{
						LossRate10m:      lossRateForWindow(items[i].ExpectedFrames60s, items[i].SuccessFrames60s),
						LossRate2h:       items[i].LossRatePct,
						ProcessIssues2h:  processIssueCounts2h[items[i].Stream.ID],
						OutageEpisodes2h: -1,
					},
				})
				items[i].RecordingHealth = dashboardHealthFromSupervision(state)
			default:
				items[i].RecordingHealth = "off"
			}
		}
	}
	if includeImageURLs && len(items) > 0 {
		streamIDs := make([]int64, 0, len(items))
		for _, it := range items {
			streamIDs = append(streamIDs, it.Stream.ID)
		}
		rawKeys, err := s.latestPreviewObjectKeys(r.Context(), streamIDs)
		if err == nil {
			for i := range items {
				if rawKey, ok := rawKeys[items[i].Stream.ID]; ok && rawKey != "" {
					url, err := s.r2.PresignGet(r.Context(), rawKey, s.cfg.R2SignGetTTL)
					if err == nil {
						items[i].LatestFrameURL = url
					}
				}
			}
		}
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"items": items, "limit": limit, "offset": offset, "total": total})
}

func (s *Server) handleDashboardCountries(w http.ResponseWriter, r *http.Request) {
	where, args, err := dashboardBuildStreamWhereFromRequest(r, dashboardStreamWhereConfig{
		IncludeSearch:         false,
		IncludeSource:         true,
		IncludeYouTubeChannel: true,
		IncludeCaptureMode:    true,
	})
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	rows, err := s.pool.Query(r.Context(), fmt.Sprintf(`
		SELECT DISTINCT country FROM (
			SELECT %s AS country
			FROM streams s
			WHERE %s
		) x
		WHERE country IS NOT NULL AND country <> ''
		ORDER BY country
	`, dashboardCountryExprSQL(), strings.Join(where, " AND ")), args...)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("dashboard countries query: %v", err))
		return
	}
	defer rows.Close()

	items := make([]string, 0, 128)
	for rows.Next() {
		var country string
		if err := rows.Scan(&country); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan dashboard country: %v", err))
			return
		}
		items = append(items, country)
	}
	if rows.Err() != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("iterate dashboard countries: %v", rows.Err()))
		return
	}

	util.WriteJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (s *Server) handleDashboardCities(w http.ResponseWriter, r *http.Request) {
	where, args, err := dashboardBuildStreamWhereFromRequest(r, dashboardStreamWhereConfig{
		IncludeSearch:         false,
		IncludeSource:         true,
		IncludeYouTubeChannel: true,
		IncludeCaptureMode:    true,
	})
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	rows, err := s.pool.Query(r.Context(), fmt.Sprintf(`
		SELECT DISTINCT city FROM (
			SELECT %s AS city
			FROM streams s
			WHERE %s
		) x
		WHERE city IS NOT NULL AND city <> ''
		ORDER BY city
	`, dashboardCityExprSQL(), strings.Join(where, " AND ")), args...)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("dashboard cities query: %v", err))
		return
	}
	defer rows.Close()

	items := make([]string, 0, 128)
	for rows.Next() {
		var city string
		if err := rows.Scan(&city); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan dashboard city: %v", err))
			return
		}
		items = append(items, city)
	}
	if rows.Err() != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("iterate dashboard cities: %v", rows.Err()))
		return
	}

	util.WriteJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (s *Server) handleDashboardSources(w http.ResponseWriter, r *http.Request) {
	where, args, err := dashboardBuildStreamWhereFromRequest(r, dashboardStreamWhereConfig{
		IncludeSearch:         false,
		IncludeSource:         false,
		IncludeYouTubeChannel: true,
		IncludeCaptureMode:    true,
	})
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	rows, err := s.pool.Query(r.Context(), fmt.Sprintf(`
		SELECT DISTINCT source FROM (
			SELECT %s AS source
			FROM streams s
			WHERE %s
		) x
		WHERE source IS NOT NULL AND source <> ''
		ORDER BY source
	`, dashboardSourceExprSQL(), strings.Join(where, " AND ")), args...)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("dashboard sources query: %v", err))
		return
	}
	defer rows.Close()

	items := make([]string, 0, 64)
	for rows.Next() {
		var source string
		if err := rows.Scan(&source); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan dashboard source: %v", err))
			return
		}
		source = strings.TrimSpace(strings.ToLower(source))
		if source == "" {
			continue
		}
		items = append(items, source)
	}
	if rows.Err() != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("iterate dashboard sources: %v", rows.Err()))
		return
	}

	util.WriteJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (s *Server) handleDashboardYouTubeChannels(w http.ResponseWriter, r *http.Request) {
	where, args, err := dashboardBuildStreamWhereFromRequest(r, dashboardStreamWhereConfig{
		IncludeSearch:         false,
		IncludeSource:         true,
		IncludeYouTubeChannel: false,
		IncludeCaptureMode:    true,
	})
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	where = append(where, fmt.Sprintf("%s='youtube'", dashboardSourceExprSQL()))

	rows, err := s.pool.Query(r.Context(), fmt.Sprintf(`
		SELECT channel
		FROM (
			SELECT %s AS channel
			FROM streams s
			WHERE %s
		) x
		WHERE channel IS NOT NULL AND channel <> ''
		GROUP BY channel
		ORDER BY COUNT(*) DESC, channel ASC
		LIMIT 400
	`, dashboardYouTubeChannelExprSQL(), strings.Join(where, " AND ")), args...)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("dashboard youtube channels query: %v", err))
		return
	}
	defer rows.Close()

	items := make([]string, 0, 128)
	for rows.Next() {
		var channel string
		if err := rows.Scan(&channel); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan dashboard youtube channel: %v", err))
			return
		}
		channel = strings.TrimSpace(channel)
		if channel == "" {
			continue
		}
		items = append(items, channel)
	}
	if rows.Err() != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("iterate dashboard youtube channels: %v", rows.Err()))
		return
	}

	util.WriteJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (s *Server) handleDashboardTags(w http.ResponseWriter, r *http.Request) {
	scope := strings.TrimSpace(strings.ToLower(r.URL.Query().Get("scope")))
	q := strings.TrimSpace(r.URL.Query().Get("q"))
	limit := parseIntQuery(r, "limit", 200, 1, 1000)

	streamWhere := []string{"1=1"}
	if scope == "recording" || scope == "recorded" {
		streamWhere = append(streamWhere, "s.recording_state='on'")
	} else if scope != "" && scope != "all" {
		util.WriteError(w, http.StatusBadRequest, "invalid scope; expected all|recording")
		return
	}

	args := make([]any, 0, 2)
	tagWhere := []string{"BTRIM(tag) <> ''"}
	if q != "" {
		args = append(args, "%"+q+"%")
		tagWhere = append(tagWhere, fmt.Sprintf("tag ILIKE $%d", len(args)))
	}
	args = append(args, limit)

	rows, err := s.pool.Query(r.Context(), fmt.Sprintf(`
		SELECT tag
		FROM (
			SELECT unnest(COALESCE(s.tags, ARRAY[]::text[])) AS tag
			FROM streams s
			WHERE %s
		) t
		WHERE %s
		GROUP BY tag
		ORDER BY COUNT(*) DESC, tag ASC
		LIMIT $%d
	`, strings.Join(streamWhere, " AND "), strings.Join(tagWhere, " AND "), len(args)), args...)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("dashboard tags query: %v", err))
		return
	}
	defer rows.Close()

	items := make([]string, 0, limit)
	for rows.Next() {
		var tag string
		if err := rows.Scan(&tag); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan dashboard tag: %v", err))
			return
		}
		tag = strings.TrimSpace(tag)
		if tag == "" {
			continue
		}
		items = append(items, tag)
	}
	if rows.Err() != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("iterate dashboard tags: %v", rows.Err()))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"items": items})
}

type dashboardStreamImageURLsRequest struct {
	StreamIDs []int64 `json:"stream_ids"`
}

func (s *Server) latestPreviewObjectKeys(ctx context.Context, streamIDs []int64) (map[int64]string, error) {
	if len(streamIDs) == 0 {
		return map[int64]string{}, nil
	}
	rows, err := s.pool.Query(ctx, `
		SELECT
			ids.stream_id,
			COALESCE(frame_media.object_key, segment_thumb.object_key)
		FROM UNNEST($1::bigint[]) AS ids(stream_id)
		LEFT JOIN LATERAL (
			SELECT m.object_key
			FROM frames f
			JOIN media_objects m ON m.id = f.raw_media_object_id
			WHERE f.stream_id = ids.stream_id
			  AND f.capture_status = 'success'
			ORDER BY f.captured_at DESC, f.id DESC
			LIMIT 1
		) frame_media ON true
		LEFT JOIN LATERAL (
			SELECT m.object_key
			FROM capture_segments cs
			JOIN media_objects m ON m.id = cs.thumbnail_media_object_id
			WHERE cs.stream_id = ids.stream_id
			  AND cs.capture_status = 'success'
			  AND cs.thumbnail_media_object_id IS NOT NULL
			ORDER BY cs.segment_end_at DESC, cs.id DESC
			LIMIT 1
		) segment_thumb ON true
		WHERE COALESCE(frame_media.object_key, segment_thumb.object_key, '') <> ''
	`, streamIDs)
	if err != nil {
		return nil, fmt.Errorf("query latest preview keys: %w", err)
	}
	defer rows.Close()

	out := make(map[int64]string, len(streamIDs))
	for rows.Next() {
		var streamID int64
		var objectKey string
		if err := rows.Scan(&streamID, &objectKey); err != nil {
			return nil, fmt.Errorf("scan latest preview key: %w", err)
		}
		out[streamID] = objectKey
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("iterate latest preview keys: %w", rows.Err())
	}
	return out, nil
}

func (s *Server) handleDashboardStreamImageURLs(w http.ResponseWriter, r *http.Request) {
	var req dashboardStreamImageURLsRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if len(req.StreamIDs) == 0 {
		util.WriteJSON(w, http.StatusOK, map[string]any{"items": []any{}})
		return
	}

	uniq := make([]int64, 0, len(req.StreamIDs))
	seen := make(map[int64]struct{}, len(req.StreamIDs))
	for _, id := range req.StreamIDs {
		if id <= 0 {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		uniq = append(uniq, id)
		if len(uniq) >= 200 {
			break
		}
	}
	if len(uniq) == 0 {
		util.WriteJSON(w, http.StatusOK, map[string]any{"items": []any{}})
		return
	}

	rawObjectKeys, err := s.latestPreviewObjectKeys(r.Context(), uniq)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("dashboard stream images query: %v", err))
		return
	}

	type item struct {
		StreamID       int64  `json:"stream_id"`
		LatestFrameURL string `json:"latest_frame_url,omitempty"`
	}
	items := make([]item, 0, len(uniq))
	for _, streamID := range uniq {
		it := item{StreamID: streamID}
		if rawObjectKey, ok := rawObjectKeys[streamID]; ok && rawObjectKey != "" {
			if url, err := s.r2.PresignGet(r.Context(), rawObjectKey, s.cfg.R2SignGetTTL); err == nil {
				it.LatestFrameURL = url
			}
		}
		items = append(items, it)
	}

	util.WriteJSON(w, http.StatusOK, map[string]any{"items": items})
}

type dashboardRecordingSettingsRequest struct {
	ClipDurationSec      int `json:"clip_duration_sec"`
	SampleIntervalMinSec int `json:"sample_interval_min_sec"`
	SampleIntervalMaxSec int `json:"sample_interval_max_sec"`
	StaleGraceSec        int `json:"stale_grace_sec"`
}

func (s *Server) handleDashboardRecordingSettingsGet(w http.ResponseWriter, r *http.Request) {
	s.writeRecordingSettings(w, r)
}

func (s *Server) handleServiceRecordingSettingsGet(w http.ResponseWriter, r *http.Request) {
	s.writeRecordingSettings(w, r)
}

func (s *Server) writeRecordingSettings(w http.ResponseWriter, r *http.Request) {
	rs, err := settings.GetRecordingSettings(r.Context(), s.pool)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{
		"clip_duration_sec":       rs.ClipDurationSec,
		"sample_interval_min_sec": rs.SampleIntervalMinSec,
		"sample_interval_max_sec": rs.SampleIntervalMaxSec,
		"stale_grace_sec":         rs.StaleGraceSec,
		"updated_at":              rs.UpdatedAt,
	})
}

func (s *Server) handleDashboardRecordingSettingsPut(w http.ResponseWriter, r *http.Request) {
	var req dashboardRecordingSettingsRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	rs, err := settings.SetRecordingSamplingPolicy(r.Context(), s.pool, settings.RecordingSettings{
		ClipDurationSec:      req.ClipDurationSec,
		SampleIntervalMinSec: req.SampleIntervalMinSec,
		SampleIntervalMaxSec: req.SampleIntervalMaxSec,
		StaleGraceSec:        req.StaleGraceSec,
	})
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{
		"clip_duration_sec":       rs.ClipDurationSec,
		"sample_interval_min_sec": rs.SampleIntervalMinSec,
		"sample_interval_max_sec": rs.SampleIntervalMaxSec,
		"stale_grace_sec":         rs.StaleGraceSec,
		"updated_at":              rs.UpdatedAt,
	})
}

type dashboardRecordingCapacityRequest struct {
	MaxActive int `json:"max_active"`
}

type dashboardRecordingCapacityBulkItem struct {
	ExecutionClass string `json:"execution_class"`
	MaxActive      int    `json:"max_active"`
}

type dashboardRecordingCapacityBulkRequest struct {
	Items []dashboardRecordingCapacityBulkItem `json:"items"`
}

func (s *Server) handleDashboardRecordingCapacityList(w http.ResponseWriter, r *http.Request) {
	type item struct {
		CapacityGroup    string    `json:"capacity_group"`
		ExecutionClasses []string  `json:"execution_classes"`
		MaxActive        int       `json:"max_active"`
		Active           int64     `json:"active"`
		UpdatedAt        time.Time `json:"updated_at"`
		Managed          bool      `json:"managed,omitempty"`
		ActiveWorkers    int64     `json:"active_workers,omitempty"`
		CapacitySource   string    `json:"capacity_source,omitempty"`
		Invalid          bool      `json:"invalid,omitempty"`
		InvalidServers   int64     `json:"invalid_servers,omitempty"`
	}
	type groupAgg struct {
		ExecutionClasses []string
		MaxActive        int64
		Active           int64
		UpdatedAt        time.Time
		ActiveWorkers    int64
		InvalidServers   int64
	}
	snapshot, err := loadRecordingCapacitySnapshot(r.Context(), s.pool, false, "", false)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	groupTotals := map[string]*groupAgg{}
	for _, groupSnapshot := range snapshot.OrderedGroups {
		group := groupSnapshot.CapacityGroup
		agg, ok := groupTotals[group]
		if !ok {
			agg = &groupAgg{ExecutionClasses: groupSnapshot.ExecutionClasses}
			groupTotals[group] = agg
		}
		agg.Active += groupSnapshot.AssignedCount
		if !groupSnapshot.Invalid {
			agg.MaxActive += int64(groupSnapshot.MaxActive)
			agg.ActiveWorkers++
		} else {
			agg.InvalidServers++
		}
		if groupSnapshot.HeartbeatAt.After(agg.UpdatedAt) {
			agg.UpdatedAt = groupSnapshot.HeartbeatAt
		}
	}

	groupOrder := []string{
		recordingCapacityGroupCaptureShared,
		capture.ExecutionClassYouTubeDirect,
	}
	for group := range groupTotals {
		already := false
		for _, known := range groupOrder {
			if group == known {
				already = true
				break
			}
		}
		if !already {
			groupOrder = append(groupOrder, group)
		}
	}
	items := make([]item, 0, len(groupTotals))
	for _, group := range groupOrder {
		agg := groupTotals[group]
		if agg == nil {
			continue
		}
		updated := agg.UpdatedAt
		if updated.IsZero() {
			updated = time.Now().UTC()
		}
		items = append(items, item{
			CapacityGroup:    group,
			ExecutionClasses: agg.ExecutionClasses,
			MaxActive:        int(agg.MaxActive),
			Active:           agg.Active,
			UpdatedAt:        updated,
			Managed:          true,
			ActiveWorkers:    agg.ActiveWorkers,
			CapacitySource:   "server_heartbeat",
			Invalid:          agg.InvalidServers > 0,
			InvalidServers:   agg.InvalidServers,
		})
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (s *Server) handleDashboardRecordingCapacityUpsert(w http.ResponseWriter, r *http.Request) {
	util.WriteError(w, http.StatusConflict, "capacity is server-heartbeat managed; update capture server shared capacity and heartbeat instead")
}

func (s *Server) handleDashboardRecordingCapacityBulkUpsert(w http.ResponseWriter, r *http.Request) {
	util.WriteError(w, http.StatusConflict, "capacity is server-heartbeat managed; update capture server shared capacity and heartbeat instead")
}

func (s *Server) handleDashboardRecordingSummary(w http.ResponseWriter, r *http.Request) {
	hours := parseIntQuery(r, "hours", 24, 1, 24*30)
	runsLimit := parseIntQuery(r, "runs_limit", 100, 1, 1000)
	eventsLimit := parseIntQuery(r, "events_limit", 100, 1, 1000)
	cutoff := time.Now().UTC().Add(-time.Duration(hours) * time.Hour)

	recordingSettings, err := settings.GetRecordingSettings(r.Context(), s.pool)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	_ = recordingSettings

	var streamsTotal, onTotal, offTotal int64
	if err := s.pool.QueryRow(r.Context(), `
		SELECT
			COUNT(*)::bigint,
			COUNT(*) FILTER (WHERE recording_state='on')::bigint,
			COUNT(*) FILTER (WHERE recording_state='off')::bigint
		FROM streams
	`).Scan(&streamsTotal, &onTotal, &offTotal); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("query recording summary stream counts: %v", err))
		return
	}

	var healthy, degraded, stale int64
	type summaryStreamHealthRow struct {
		StreamID       int64
		ExecutionClass string
		RuntimeClass   *string
		LastSeen       *time.Time
	}
	summaryRows, err := s.pool.Query(r.Context(), `
		SELECT
			s.id,
			s.execution_class,
			rt.execution_class,
			COALESCE(rt.last_frame_at, sh.last_capture_at) AS last_seen
		FROM streams s
		LEFT JOIN stream_capture_runtime rt ON rt.stream_id=s.id
		LEFT JOIN stream_health sh ON sh.stream_id=s.id
		WHERE s.recording_state='on'
	`)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("query recording summary health: %v", err))
		return
	}
	summaryItems := make([]summaryStreamHealthRow, 0, onTotal)
	frameSummaryIDs := make([]int64, 0, onTotal)
	clipSummaryIDs := make([]int64, 0, onTotal)
	for summaryRows.Next() {
		var row summaryStreamHealthRow
		if err := summaryRows.Scan(&row.StreamID, &row.ExecutionClass, &row.RuntimeClass, &row.LastSeen); err != nil {
			summaryRows.Close()
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan recording summary health: %v", err))
			return
		}
		summaryItems = append(summaryItems, row)
		if isClipNativeExecutionClass(firstNonEmpty(row.ExecutionClass, derefString(row.RuntimeClass))) {
			clipSummaryIDs = append(clipSummaryIDs, row.StreamID)
		} else {
			frameSummaryIDs = append(frameSummaryIDs, row.StreamID)
		}
	}
	summaryRows.Close()
	if summaryRows.Err() != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("iterate recording summary health: %v", summaryRows.Err()))
		return
	}
	success2h, err := s.successCaptureCountsSince(r.Context(), frameSummaryIDs, clipSummaryIDs, 2*time.Hour)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("query recording summary counters: %v", err))
		return
	}
	processIssueCounts2h, err := s.recordingProcessIssueCountsSince(r.Context(), 2*time.Hour)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("query recording summary process issues: %v", err))
		return
	}
	outageEpisodes2h, err := s.outageEpisodeCountsSince(r.Context(), frameSummaryIDs, clipSummaryIDs, 2*time.Hour, 0)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("query recording summary outage counters: %v", err))
		return
	}
	now := time.Now().UTC()
	for _, row := range summaryItems {
		mode := firstNonEmpty(row.ExecutionClass, derefString(row.RuntimeClass))
		if row.LastSeen == nil {
			stale++
			continue
		}
		state, _, _ := classifyRecordingSupervision(now, recordingSupervisionInput{
			RecordingState:  "on",
			ModeClass:       mode,
			ServerID:        "assigned",
			RuntimeStatus:   "running",
			LastFrameAt:     row.LastSeen,
			StreamUpdatedAt: now,
			Metrics: recordingSupervisionMetrics{
				LossRate2h:       lossRateForWindow(expectedCapturesForWindow(mode, settings.DefaultRecordingIntervalSec, 2*time.Hour), success2h[row.StreamID]),
				ProcessIssues2h:  processIssueCounts2h[row.StreamID],
				OutageEpisodes2h: outageEpisodes2h[row.StreamID],
			},
		})
		switch state {
		case "down_10m":
			stale++
		case "spotty_2h":
			degraded++
		default:
			healthy++
		}
	}

	var activeProcesses, staleProcesses int64
	if err := s.pool.QueryRow(r.Context(), `
		SELECT
			COUNT(*) FILTER (
				WHERE status IN ('starting', 'running')
				  AND stopped_at IS NULL
				  AND COALESCE(last_heartbeat_at, updated_at) >= now() - interval '60 seconds'
			)::bigint AS active_processes,
			COUNT(*) FILTER (
				WHERE status IN ('starting', 'running')
				  AND stopped_at IS NULL
				  AND COALESCE(last_heartbeat_at, updated_at) < now() - interval '60 seconds'
			)::bigint AS stale_processes
		FROM recording_process_runs
	`).Scan(&activeProcesses, &staleProcesses); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("query recording process counts: %v", err))
		return
	}

	recentRuns := make([]map[string]any, 0, runsLimit)
	runRows, err := s.pool.Query(r.Context(), `
		SELECT
			r.id,
			r.stream_id,
			s.name,
			s.slug,
			r.execution_class,
			r.server_id,
			r.process_id,
			r.worker_id,
			r.status,
			r.start_reason,
			r.stop_reason,
			r.started_at,
			r.stopped_at,
			r.last_heartbeat_at,
			r.last_frame_at,
			r.restart_count,
			r.last_error_text,
			r.updated_at
		FROM recording_process_runs r
		JOIN streams s ON s.id=r.stream_id
		WHERE r.updated_at >= $1
		ORDER BY COALESCE(r.last_heartbeat_at, r.updated_at, r.started_at) DESC, r.id DESC
		LIMIT $2
	`, cutoff, runsLimit)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("query recording recent runs: %v", err))
		return
	}
	defer runRows.Close()
	for runRows.Next() {
		var id int64
		var streamID int64
		var streamName, streamSlug string
		var executionClass, serverID, processID, workerID string
		var status, startReason, stopReason string
		var startedAt, updatedAt time.Time
		var stoppedAt, lastHeartbeatAt, lastFrameAt *time.Time
		var restartCount int
		var lastErrorText *string
		if err := runRows.Scan(
			&id, &streamID, &streamName, &streamSlug, &executionClass, &serverID, &processID, &workerID,
			&status, &startReason, &stopReason, &startedAt, &stoppedAt, &lastHeartbeatAt, &lastFrameAt,
			&restartCount, &lastErrorText, &updatedAt,
		); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan recording recent run: %v", err))
			return
		}
		recentRuns = append(recentRuns, map[string]any{
			"id":                id,
			"stream_id":         streamID,
			"stream_name":       streamName,
			"stream_slug":       streamSlug,
			"execution_class":   executionClass,
			"server_id":         serverID,
			"process_id":        processID,
			"worker_id":         workerID,
			"status":            status,
			"start_reason":      startReason,
			"stop_reason":       stopReason,
			"started_at":        startedAt,
			"stopped_at":        stoppedAt,
			"last_heartbeat_at": lastHeartbeatAt,
			"last_frame_at":     lastFrameAt,
			"restart_count":     restartCount,
			"last_error_text":   lastErrorText,
			"updated_at":        updatedAt,
		})
	}
	if runRows.Err() != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("iterate recording recent runs: %v", runRows.Err()))
		return
	}

	recentEvents := make([]map[string]any, 0, eventsLimit)
	eventRows, err := s.pool.Query(r.Context(), `
		SELECT
			e.id,
			e.stream_id,
			s.name,
			s.slug,
			e.prev_state::text,
			e.next_state::text,
			e.actor,
			e.reason,
			e.metadata_jsonb,
			e.created_at
		FROM recording_state_events e
		JOIN streams s ON s.id=e.stream_id
		WHERE e.created_at >= $1
		ORDER BY e.created_at DESC, e.id DESC
		LIMIT $2
	`, cutoff, eventsLimit)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("query recording recent events: %v", err))
		return
	}
	defer eventRows.Close()
	for eventRows.Next() {
		var id int64
		var streamID int64
		var streamName, streamSlug string
		var prevState *string
		var nextState, actor, reason string
		var metadataBytes []byte
		var createdAt time.Time
		if err := eventRows.Scan(
			&id, &streamID, &streamName, &streamSlug, &prevState, &nextState, &actor, &reason, &metadataBytes, &createdAt,
		); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan recording recent event: %v", err))
			return
		}
		metadata := map[string]any{}
		if len(metadataBytes) > 0 {
			if err := json.Unmarshal(metadataBytes, &metadata); err != nil {
				util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("decode recording recent event metadata: %v", err))
				return
			}
		}
		recentEvents = append(recentEvents, map[string]any{
			"id":          id,
			"stream_id":   streamID,
			"stream_name": streamName,
			"stream_slug": streamSlug,
			"prev_state":  prevState,
			"next_state":  nextState,
			"actor":       actor,
			"reason":      reason,
			"metadata":    metadata,
			"created_at":  createdAt,
		})
	}
	if eventRows.Err() != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("iterate recording recent events: %v", eventRows.Err()))
		return
	}

	util.WriteJSON(w, http.StatusOK, map[string]any{
		"hours":                     hours,
		"recording_interval_sec":    settings.DefaultRecordingIntervalSec,
		"streams_total":             streamsTotal,
		"recording_on":              onTotal,
		"recording_off":             offTotal,
		"recording_healthy":         healthy,
		"recording_degraded":        degraded,
		"recording_stale":           stale,
		"active_processes":          activeProcesses,
		"stale_processes":           staleProcesses,
		"recent_runs_limit":         runsLimit,
		"recent_state_events_limit": eventsLimit,
		"recent_runs":               recentRuns,
		"recent_state_events":       recentEvents,
	})
}

type dashboardPipelineOverviewItem struct {
	PipelineID            string         `json:"pipeline_id"`
	Kind                  string         `json:"kind"`
	Active                bool           `json:"active"`
	SpecJSON              map[string]any `json:"spec_json"`
	EnabledStreams        int64          `json:"enabled_streams"`
	EnabledRecording      int64          `json:"enabled_recording_streams"`
	CapturedSuccessFrames int64          `json:"captured_success_frames"`
	ProcessedFrames       int64          `json:"processed_frames"`
	BacklogFrames         int64          `json:"backlog_frames"`
	ActiveClaims          int64          `json:"active_claims"`
	QueuedBoxedResults    int64          `json:"queued_boxed_results"`
	ActiveWorkers         int64          `json:"active_workers"`
	Throughput1h          int64          `json:"throughput_1h"`
	LastResultStatus      string         `json:"last_result_status,omitempty"`
	LastResultAt          *time.Time     `json:"last_result_at,omitempty"`
}

func (s *Server) loadDashboardPipelineOverview(ctx context.Context, includeInactive bool) ([]dashboardPipelineOverviewItem, error) {
	where := "1=1"
	args := []any{}
	if !includeInactive {
		where = "p.active=true"
	}
	rows, err := s.pool.Query(ctx, fmt.Sprintf(`
		SELECT p.id, p.kind, p.active, p.spec_jsonb
		FROM pipelines p
		WHERE %s
		ORDER BY p.id ASC
	`, where), args...)
	if err != nil {
		return nil, fmt.Errorf("query pipelines overview base: %w", err)
	}
	defer rows.Close()

	items := make([]dashboardPipelineOverviewItem, 0, 32)
	itemByPipeline := make(map[string]*dashboardPipelineOverviewItem, 32)
	for rows.Next() {
		var it dashboardPipelineOverviewItem
		var specBytes []byte
		if err := rows.Scan(&it.PipelineID, &it.Kind, &it.Active, &specBytes); err != nil {
			return nil, fmt.Errorf("scan pipeline overview base: %w", err)
		}
		if len(specBytes) > 0 {
			if err := json.Unmarshal(specBytes, &it.SpecJSON); err != nil {
				return nil, fmt.Errorf("decode pipeline spec %s: %w", it.PipelineID, err)
			}
		}
		if it.SpecJSON == nil {
			it.SpecJSON = map[string]any{}
		}
		items = append(items, it)
		itemByPipeline[it.PipelineID] = &items[len(items)-1]
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("iterate pipeline overview rows: %w", rows.Err())
	}

	if len(items) == 0 {
		return items, nil
	}

	enabledRows, err := s.pool.Query(ctx, fmt.Sprintf(`
		WITH totals AS (
			SELECT
				COUNT(*)::bigint AS total_streams,
				COUNT(*) FILTER (WHERE recording_state='on')::bigint AS total_recording_streams
			FROM streams
		), disabled AS (
			SELECT
				sps.pipeline_id,
				COUNT(*)::bigint AS disabled_streams,
				COUNT(*) FILTER (WHERE s.recording_state='on')::bigint AS disabled_recording_streams
			FROM stream_pipeline_settings sps
			JOIN streams s ON s.id=sps.stream_id
			WHERE sps.enabled=false
			GROUP BY sps.pipeline_id
		)
		SELECT
			p.id,
			GREATEST(t.total_streams - COALESCE(d.disabled_streams, 0), 0)::bigint AS enabled_streams,
			GREATEST(t.total_recording_streams - COALESCE(d.disabled_recording_streams, 0), 0)::bigint AS enabled_recording_streams
		FROM pipelines p
		CROSS JOIN totals t
		LEFT JOIN disabled d ON d.pipeline_id=p.id
		WHERE %s
	`, where), args...)
	if err != nil {
		return nil, fmt.Errorf("query enabled stream counts: %w", err)
	}
	for enabledRows.Next() {
		var pipelineID string
		var enabledStreams, enabledRecording int64
		if err := enabledRows.Scan(&pipelineID, &enabledStreams, &enabledRecording); err != nil {
			enabledRows.Close()
			return nil, fmt.Errorf("scan enabled stream counts: %w", err)
		}
		if it := itemByPipeline[pipelineID]; it != nil {
			it.EnabledStreams = enabledStreams
			it.EnabledRecording = enabledRecording
		}
	}
	if enabledRows.Err() != nil {
		enabledRows.Close()
		return nil, fmt.Errorf("iterate enabled stream counts: %w", enabledRows.Err())
	}
	enabledRows.Close()

	activeClaimRows, err := s.pool.Query(ctx, `
		SELECT pipeline_id, COUNT(*)::bigint
		FROM inference_claims
		WHERE status='leased' AND lease_expires_at > now()
		GROUP BY pipeline_id
	`)
	if err != nil {
		return nil, fmt.Errorf("query active claims: %w", err)
	}
	for activeClaimRows.Next() {
		var pipelineID string
		var n int64
		if err := activeClaimRows.Scan(&pipelineID, &n); err != nil {
			activeClaimRows.Close()
			return nil, fmt.Errorf("scan active claims: %w", err)
		}
		if it := itemByPipeline[pipelineID]; it != nil {
			it.ActiveClaims = n
		}
	}
	if activeClaimRows.Err() != nil {
		activeClaimRows.Close()
		return nil, fmt.Errorf("iterate active claims: %w", activeClaimRows.Err())
	}
	activeClaimRows.Close()

	queuedRows, err := s.pool.Query(ctx, `
		SELECT pipeline_id, COUNT(*)::bigint
		FROM inference_results
		WHERE status='queued_boxed'
		GROUP BY pipeline_id
	`)
	if err != nil {
		return nil, fmt.Errorf("query queued boxed counts: %w", err)
	}
	for queuedRows.Next() {
		var pipelineID string
		var n int64
		if err := queuedRows.Scan(&pipelineID, &n); err != nil {
			queuedRows.Close()
			return nil, fmt.Errorf("scan queued boxed counts: %w", err)
		}
		if it := itemByPipeline[pipelineID]; it != nil {
			it.QueuedBoxedResults = n
		}
	}
	if queuedRows.Err() != nil {
		queuedRows.Close()
		return nil, fmt.Errorf("iterate queued boxed counts: %w", queuedRows.Err())
	}
	queuedRows.Close()

	workerRows, err := s.pool.Query(ctx, `
		SELECT pipeline_id, COUNT(DISTINCT worker_id)::bigint
		FROM processing_worker_heartbeats
		WHERE worker_kind='inference'
		  AND pipeline_id <> ''
		  AND lease_expires_at > now()
		GROUP BY pipeline_id
	`)
	if err != nil {
		return nil, fmt.Errorf("query active inference workers: %w", err)
	}
	for workerRows.Next() {
		var pipelineID string
		var n int64
		if err := workerRows.Scan(&pipelineID, &n); err != nil {
			workerRows.Close()
			return nil, fmt.Errorf("scan active inference workers: %w", err)
		}
		if it := itemByPipeline[pipelineID]; it != nil {
			it.ActiveWorkers = n
		}
	}
	if workerRows.Err() != nil {
		workerRows.Close()
		return nil, fmt.Errorf("iterate active inference workers: %w", workerRows.Err())
	}
	workerRows.Close()

	// Backlog is intentionally lightweight in this endpoint: active work in-flight + queued boxing.
	for i := range items {
		items[i].CapturedSuccessFrames = 0
		items[i].ProcessedFrames = 0
		items[i].Throughput1h = 0
		items[i].LastResultStatus = ""
		items[i].LastResultAt = nil
		items[i].BacklogFrames = items[i].ActiveClaims + items[i].QueuedBoxedResults
	}

	return items, nil
}

func (s *Server) handleDashboardPipelinesOverview(w http.ResponseWriter, r *http.Request) {
	includeInactive := true
	if v := parseBoolQueryPtr(r, "include_inactive"); v != nil {
		includeInactive = *v
	}
	items, err := s.loadDashboardPipelineOverview(r.Context(), includeInactive)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load pipeline overview: %v", err))
		return
	}
	var backlogTotal int64
	var claimsTotal int64
	for i := range items {
		backlogTotal += items[i].BacklogFrames
		claimsTotal += items[i].ActiveClaims
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{
		"items":                items,
		"include_inactive":     includeInactive,
		"pipelines_total":      len(items),
		"backlog_frames_total": backlogTotal,
		"active_claims_total":  claimsTotal,
	})
}

type dashboardServerItem struct {
	ServerID               string           `json:"server_id"`
	LastSeenAt             *time.Time       `json:"last_seen_at,omitempty"`
	Active                 bool             `json:"active"`
	Processes              []map[string]any `json:"processes,omitempty"`
	ExecutionClasses       []map[string]any `json:"execution_classes,omitempty"`
	ProcessingWorkers      []map[string]any `json:"processing_workers,omitempty"`
	ActiveCaptureStreamIDs []int64          `json:"active_capture_stream_ids,omitempty"`
	ActiveInference        []map[string]any `json:"active_inference,omitempty"`
}

func maxTimePtr(current *time.Time, t time.Time) *time.Time {
	if t.IsZero() {
		return current
	}
	if current == nil || t.After(*current) {
		tt := t.UTC()
		return &tt
	}
	return current
}

func uniqueInt64Sorted(in []int64) []int64 {
	if len(in) == 0 {
		return nil
	}
	seen := map[int64]struct{}{}
	out := make([]int64, 0, len(in))
	for _, v := range in {
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func stringFromMetadata(metadata map[string]any, key string) string {
	if metadata == nil {
		return ""
	}
	v, ok := metadata[key]
	if !ok || v == nil {
		return ""
	}
	switch x := v.(type) {
	case string:
		return strings.TrimSpace(x)
	case fmt.Stringer:
		return strings.TrimSpace(x.String())
	default:
		return strings.TrimSpace(fmt.Sprint(x))
	}
}

func int64FromAny(v any) (int64, bool) {
	switch x := v.(type) {
	case int64:
		return x, true
	case int:
		return int64(x), true
	case int32:
		return int64(x), true
	case float64:
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return 0, false
		}
		return int64(x), true
	case json.Number:
		n, err := x.Int64()
		if err != nil {
			return 0, false
		}
		return n, true
	case string:
		s := strings.TrimSpace(x)
		if s == "" {
			return 0, false
		}
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return 0, false
		}
		return n, true
	default:
		return 0, false
	}
}

func normalizeServerID(raw string) string {
	v := strings.TrimSpace(strings.ToLower(raw))
	if v == "" {
		return ""
	}
	if i := strings.IndexByte(v, '.'); i > 0 {
		v = v[:i]
	}
	return v
}

func deriveServerID(workerID string, metadata map[string]any) string {
	if serverID := normalizeServerID(stringFromMetadata(metadata, "server_id")); serverID != "" {
		return serverID
	}
	if host := normalizeServerID(stringFromMetadata(metadata, "host")); host != "" {
		return host
	}
	workerID = strings.TrimSpace(workerID)
	if workerID == "" {
		return "unknown"
	}
	if strings.HasPrefix(workerID, "inferctl:") {
		parts := strings.Split(workerID, ":")
		if len(parts) >= 2 {
			if host := normalizeServerID(parts[1]); host != "" {
				return host
			}
		}
	}
	if strings.HasPrefix(workerID, "local-youtube-worker-") {
		suffix := normalizeServerID(strings.TrimPrefix(workerID, "local-youtube-worker-"))
		if suffix != "" && !isASCIIAllDigits(suffix) {
			return suffix
		}
		if host := normalizeServerID(workerID); host != "" {
			return host
		}
	}
	if strings.HasPrefix(workerID, "srv-") {
		return "render"
	}
	if strings.HasPrefix(workerID, "render-") {
		return "render"
	}
	if strings.Contains(strings.ToLower(workerID), "render") {
		return "render"
	}
	return normalizeServerID(workerID)
}

func registerWorkerServerHints(hints map[string]string, workerID string, metadata map[string]any) string {
	key := strings.TrimSpace(deriveServerID(workerID, metadata))
	if key == "" {
		key = "unknown"
	}
	aliases := []string{
		strings.TrimSpace(workerID),
		strings.TrimSpace(stringFromMetadata(metadata, "claimed_by")),
		strings.TrimSpace(stringFromMetadata(metadata, "process_id")),
	}
	for _, alias := range aliases {
		if alias == "" {
			continue
		}
		hints[alias] = key
	}
	return key
}

func isASCIIAllDigits(v string) bool {
	if strings.TrimSpace(v) == "" {
		return false
	}
	for i := 0; i < len(v); i++ {
		if v[i] < '0' || v[i] > '9' {
			return false
		}
	}
	return true
}

func (s *Server) handleDashboardServers(w http.ResponseWriter, r *http.Request) {
	hours := parseIntQuery(r, "hours", 24*7, 1, 24*30)
	includeStale := false
	if v := parseBoolQueryPtr(r, "include_stale"); v != nil {
		includeStale = *v
	}
	includeAuxiliary := false
	if v := parseBoolQueryPtr(r, "include_auxiliary"); v != nil {
		includeAuxiliary = *v
	}
	cutoff := time.Now().UTC().Add(-time.Duration(hours) * time.Hour)
	now := time.Now().UTC()
	heartbeatWhere := "heartbeat_at >= now() - interval '120 seconds'"
	heartbeatArgs := []any{}
	if includeStale {
		heartbeatWhere = "(heartbeat_at >= $1 OR lease_expires_at > now())"
		heartbeatArgs = append(heartbeatArgs, cutoff)
	}
	inferenceWhere := "(ic.status='leased' AND ic.lease_expires_at > now())"
	inferenceArgs := []any{}
	if includeStale {
		inferenceWhere = "((ic.status='leased' AND ic.lease_expires_at > now()) OR ic.updated_at >= $1)"
		inferenceArgs = append(inferenceArgs, cutoff)
	}

	serverMap := map[string]*dashboardServerItem{}
	workerServerHints := map[string]string{}
	resolveServerKey := func(workerID string, metadata map[string]any) string {
		workerID = strings.TrimSpace(workerID)
		if workerID != "" {
			if key, ok := workerServerHints[workerID]; ok && strings.TrimSpace(key) != "" {
				return key
			}
		}
		return registerWorkerServerHints(workerServerHints, workerID, metadata)
	}
	getServer := func(workerID string, metadata map[string]any) *dashboardServerItem {
		key := resolveServerKey(workerID, metadata)
		it, ok := serverMap[key]
		if ok {
			return it
		}
		it = &dashboardServerItem{ServerID: key}
		serverMap[key] = it
		return it
	}
	getServerByID := func(serverID string, metadata map[string]any) *dashboardServerItem {
		key := normalizeServerID(deriveServerID(serverID, metadata))
		if key == "" {
			key = resolveServerKey("", metadata)
		}
		it, ok := serverMap[key]
		if ok {
			return it
		}
		it = &dashboardServerItem{ServerID: key}
		serverMap[key] = it
		return it
	}
	getKnownServer := func(workerID string, metadata map[string]any) (*dashboardServerItem, bool) {
		key := resolveServerKey(workerID, metadata)
		it, ok := serverMap[key]
		return it, ok
	}
	appendProcess := func(it *dashboardServerItem, process map[string]any) {
		if it == nil {
			return
		}
		if process == nil {
			process = map[string]any{}
		}
		if _, ok := process["server_id"]; !ok {
			process["server_id"] = it.ServerID
		}
		it.Processes = append(it.Processes, process)
	}
	recordingCapacitySnapshot, err := loadRecordingCapacitySnapshot(r.Context(), s.pool, includeStale, "", false)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("query server execution class capacity: %v", err))
		return
	}
	for _, group := range recordingCapacitySnapshot.OrderedGroups {
		it := getServerByID(group.ServerID, group.MetadataJSON)
		it.LastSeenAt = maxTimePtr(it.LastSeenAt, group.HeartbeatAt)
		if group.Active {
			it.Active = true
		}
		it.ExecutionClasses = append(it.ExecutionClasses, map[string]any{
			"server_id":                   it.ServerID,
			"execution_class":             group.ExecutionClass,
			"capacity_group":              group.CapacityGroup,
			"execution_classes":           group.ExecutionClasses,
			"available_execution_classes": group.AvailableExecutionClasses,
			"execution_class_states":      group.ExecutionClassStates,
			"capacity":                    group.MaxActive,
			"assigned_count":              group.AssignedCount,
			"free_slots":                  group.FreeSlots,
			"draining":                    group.Draining,
			"invalid":                     group.Invalid,
			"invalid_reason":              group.InvalidReason,
			"metadata_json":               group.MetadataJSON,
			"heartbeat_at":                group.HeartbeatAt,
			"lease_expires_at":            group.LeaseExpiresAt,
			"active":                      group.Active,
		})
	}

	processingRows, err := s.pool.Query(r.Context(), fmt.Sprintf(`
		SELECT worker_id, worker_kind, execution_class, pipeline_id, metadata_jsonb, heartbeat_at, lease_expires_at
		FROM processing_worker_heartbeats
		WHERE %s
		ORDER BY worker_id ASC, worker_kind ASC, execution_class ASC, pipeline_id ASC
	`, heartbeatWhere), heartbeatArgs...)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("query processing worker heartbeats: %v", err))
		return
	}
	for processingRows.Next() {
		var workerID, workerKind, executionClass, pipelineID string
		var metadataBytes []byte
		var heartbeatAt, leaseExpiresAt time.Time
		if err := processingRows.Scan(&workerID, &workerKind, &executionClass, &pipelineID, &metadataBytes, &heartbeatAt, &leaseExpiresAt); err != nil {
			processingRows.Close()
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan processing worker heartbeat: %v", err))
			return
		}
		metadata := map[string]any{}
		if len(metadataBytes) > 0 {
			if err := json.Unmarshal(metadataBytes, &metadata); err != nil {
				processingRows.Close()
				util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("decode processing worker metadata: %v", err))
				return
			}
		}
		it := getServer(workerID, metadata)
		it.LastSeenAt = maxTimePtr(it.LastSeenAt, heartbeatAt)
		if leaseExpiresAt.After(now) {
			it.Active = true
		}
		it.ProcessingWorkers = append(it.ProcessingWorkers, map[string]any{
			"worker_id":        workerID,
			"worker_kind":      workerKind,
			"execution_class":  executionClass,
			"pipeline_id":      pipelineID,
			"metadata_json":    metadata,
			"heartbeat_at":     heartbeatAt,
			"lease_expires_at": leaseExpiresAt,
			"active":           leaseExpiresAt.After(now),
		})
		processName := strings.TrimSpace(stringFromMetadata(metadata, "process_name"))
		processNameLower := strings.ToLower(processName)
		isRecordingProcessHeartbeat := processNameLower == "recording-stream-runner"
		isModeSupervisorHeartbeat := processNameLower == "capture-server-mode"
		if !isRecordingProcessHeartbeat && !isModeSupervisorHeartbeat {
			appendProcess(it, map[string]any{
				"process_id":       workerID,
				"worker_id":        workerID,
				"source":           "processing_worker_heartbeat",
				"worker_kind":      workerKind,
				"execution_class":  executionClass,
				"pipeline_id":      pipelineID,
				"metadata_json":    metadata,
				"heartbeat_at":     heartbeatAt,
				"lease_expires_at": leaseExpiresAt,
				"active":           leaseExpiresAt.After(now),
			})
		}
		if streamID, ok := int64FromAny(metadata["stream_id"]); ok && streamID > 0 {
			if !isRecordingProcessHeartbeat && !isModeSupervisorHeartbeat && len(it.Processes) > 0 {
				process := it.Processes[len(it.Processes)-1]
				process["stream_id"] = streamID
				process["active_capture_stream_ids"] = []int64{streamID}
			}
		}
		if workerKind == "inference" && leaseExpiresAt.After(now) {
			found := false
			for i := range it.ActiveInference {
				pid, _ := it.ActiveInference[i]["pipeline_id"].(string)
				if pid == pipelineID {
					n := int64(0)
					if v, ok := int64FromAny(it.ActiveInference[i]["active_workers"]); ok {
						n = v
					}
					it.ActiveInference[i]["active_workers"] = n + 1
					it.ActiveInference[i]["active"] = true
					found = true
					break
				}
			}
			if !found {
				it.ActiveInference = append(it.ActiveInference, map[string]any{
					"pipeline_id":    pipelineID,
					"active_workers": int64(1),
					"active_claims":  int64(0),
					"stream_count":   int64(0),
					"active":         true,
				})
			}
		}
	}
	if processingRows.Err() != nil {
		processingRows.Close()
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("iterate processing worker heartbeats: %v", processingRows.Err()))
		return
	}
	processingRows.Close()

	activeRunWhere := "status IN ('starting','running') AND stopped_at IS NULL AND last_heartbeat_at >= now() - interval '120 seconds'"
	activeRunArgs := []any{}
	activeRunRows, err := s.pool.Query(r.Context(), fmt.Sprintf(`
		SELECT stream_id, execution_class, server_id, process_id, worker_id, status,
		       start_reason, stop_reason, started_at, stopped_at, last_heartbeat_at, last_frame_at,
		       restart_count, last_error_text, updated_at
		FROM recording_process_runs
		WHERE %s
		ORDER BY server_id ASC, stream_id ASC, process_id ASC
	`, activeRunWhere), activeRunArgs...)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("query active recording process runs: %v", err))
		return
	}
	for activeRunRows.Next() {
		var streamID int64
		var executionClass, serverID, processID, workerID, status, startReason, stopReason string
		var startedAt, updatedAt time.Time
		var stoppedAt, lastHeartbeatAt, lastFrameAt *time.Time
		var restartCount int
		var lastErrorText *string
		if err := activeRunRows.Scan(
			&streamID, &executionClass, &serverID, &processID, &workerID, &status,
			&startReason, &stopReason, &startedAt, &stoppedAt, &lastHeartbeatAt, &lastFrameAt,
			&restartCount, &lastErrorText, &updatedAt,
		); err != nil {
			activeRunRows.Close()
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan active recording process run: %v", err))
			return
		}
		it := getServerByID(serverID, nil)
		it.LastSeenAt = maxTimePtr(it.LastSeenAt, updatedAt)
		activeProc := status == "starting" || status == "running"
		if lastHeartbeatAt != nil {
			it.LastSeenAt = maxTimePtr(it.LastSeenAt, *lastHeartbeatAt)
			activeProc = activeProc && lastHeartbeatAt.After(now.Add(-120*time.Second))
		}
		if activeProc {
			it.Active = true
		}
		appendProcess(it, map[string]any{
			"process_id":                processID,
			"worker_id":                 workerID,
			"source":                    "recording_process_run",
			"worker_kind":               "capture",
			"execution_class":           executionClass,
			"status":                    status,
			"start_reason":              startReason,
			"stop_reason":               stopReason,
			"started_at":                startedAt,
			"stopped_at":                stoppedAt,
			"last_heartbeat_at":         lastHeartbeatAt,
			"last_frame_at":             lastFrameAt,
			"restart_count":             restartCount,
			"last_error_text":           lastErrorText,
			"stream_id":                 streamID,
			"active_capture_stream_ids": []int64{streamID},
			"active":                    activeProc,
		})
	}
	if activeRunRows.Err() != nil {
		activeRunRows.Close()
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("iterate active recording process runs: %v", activeRunRows.Err()))
		return
	}
	activeRunRows.Close()

	if includeAuxiliary {
		captureSessionRows, err := s.pool.Query(r.Context(), fmt.Sprintf(`
		SELECT lease_owner, ARRAY_AGG(stream_id ORDER BY stream_id), MAX(heartbeat_at), MAX(lease_expires_at)
		FROM capture_session_leases
		WHERE %s
		GROUP BY lease_owner
	`, heartbeatWhere), heartbeatArgs...)
		if err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("query capture sessions by owner: %v", err))
			return
		}
		for captureSessionRows.Next() {
			var owner string
			var streamIDs []int64
			var heartbeatAt, leaseExpiresAt time.Time
			if err := captureSessionRows.Scan(&owner, &streamIDs, &heartbeatAt, &leaseExpiresAt); err != nil {
				captureSessionRows.Close()
				util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan capture sessions by owner: %v", err))
				return
			}
			streamIDs = uniqueInt64Sorted(streamIDs)
			it, ok := getKnownServer(owner, nil)
			if !ok {
				it = getServer(owner, nil)
			}
			it.LastSeenAt = maxTimePtr(it.LastSeenAt, heartbeatAt)
			if leaseExpiresAt.After(now) {
				it.Active = true
			}
			it.ActiveCaptureStreamIDs = uniqueInt64Sorted(append(it.ActiveCaptureStreamIDs, streamIDs...))
			appendProcess(it, map[string]any{
				"process_id":                owner,
				"worker_id":                 owner,
				"source":                    "capture_session_leases",
				"worker_kind":               "capture",
				"execution_class":           "session_lease",
				"active_capture_stream_ids": streamIDs,
				"heartbeat_at":              heartbeatAt,
				"lease_expires_at":          leaseExpiresAt,
				"active":                    leaseExpiresAt.After(now),
			})
		}
		if captureSessionRows.Err() != nil {
			captureSessionRows.Close()
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("iterate capture sessions by owner: %v", captureSessionRows.Err()))
			return
		}
		captureSessionRows.Close()

		inferenceRows, err := s.pool.Query(r.Context(), fmt.Sprintf(`
		SELECT
			ic.claimed_by,
			ic.pipeline_id,
			COUNT(*)::bigint AS active_claims,
			COUNT(DISTINCT f.stream_id)::bigint AS stream_count,
			MAX(ic.updated_at) AS updated_at,
			MAX(ic.lease_expires_at) AS lease_expires_at
		FROM inference_claims ic
		JOIN frames f ON f.id=ic.frame_id
		WHERE %s
		GROUP BY ic.claimed_by, ic.pipeline_id
		ORDER BY ic.claimed_by ASC, ic.pipeline_id ASC
	`, inferenceWhere), inferenceArgs...)
		if err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("query inference claims by owner: %v", err))
			return
		}
		for inferenceRows.Next() {
			var owner, pipelineID string
			var activeClaims, streamCount int64
			var updatedAt, leaseExpiresAt time.Time
			if err := inferenceRows.Scan(&owner, &pipelineID, &activeClaims, &streamCount, &updatedAt, &leaseExpiresAt); err != nil {
				inferenceRows.Close()
				util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan inference claims by owner: %v", err))
				return
			}
			it, ok := getKnownServer(owner, nil)
			if !ok {
				it = getServer(owner, nil)
			}
			it.LastSeenAt = maxTimePtr(it.LastSeenAt, updatedAt)
			if leaseExpiresAt.After(now) {
				it.Active = true
			}
			it.ActiveInference = append(it.ActiveInference, map[string]any{
				"pipeline_id":      pipelineID,
				"active_claims":    activeClaims,
				"stream_count":     streamCount,
				"lease_expires_at": leaseExpiresAt,
				"active":           leaseExpiresAt.After(now),
			})
			appendProcess(it, map[string]any{
				"process_id":       owner,
				"worker_id":        owner,
				"source":           "inference_claims",
				"worker_kind":      "inference",
				"pipeline_id":      pipelineID,
				"active_claims":    activeClaims,
				"stream_count":     streamCount,
				"updated_at":       updatedAt,
				"lease_expires_at": leaseExpiresAt,
				"active":           leaseExpiresAt.After(now),
			})
		}
		if inferenceRows.Err() != nil {
			inferenceRows.Close()
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("iterate inference claims by owner: %v", inferenceRows.Err()))
			return
		}
		inferenceRows.Close()
	}

	// Attach active capture ownership using assignment truth instead of inferred
	// runtime-by-execution-class mapping (which is ambiguous with multiple servers per execution class).
	assignmentRows, err := s.pool.Query(r.Context(), `
		SELECT server_id, execution_class, ARRAY_AGG(stream_id ORDER BY stream_id)
		FROM recording_assignments
		GROUP BY server_id, execution_class
		ORDER BY server_id ASC, execution_class ASC
	`)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("query recording assignments by server: %v", err))
		return
	}
	for assignmentRows.Next() {
		var serverIDRaw, executionClassRaw string
		var streamIDs []int64
		if err := assignmentRows.Scan(&serverIDRaw, &executionClassRaw, &streamIDs); err != nil {
			assignmentRows.Close()
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan recording assignments by server: %v", err))
			return
		}
		executionClass, ok := capture.NormalizeExecutionClass(executionClassRaw)
		if !ok {
			continue
		}
		serverID := normalizeServerID(serverIDRaw)
		if serverID == "" {
			continue
		}
		it, ok := serverMap[serverID]
		if !ok {
			it = &dashboardServerItem{ServerID: serverID}
			serverMap[serverID] = it
		}
		streamIDs = uniqueInt64Sorted(streamIDs)
		it.ActiveCaptureStreamIDs = append(it.ActiveCaptureStreamIDs, streamIDs...)
		appendProcess(it, map[string]any{
			"process_id":                fmt.Sprintf("recording-assignments:%s:%s", serverID, executionClass),
			"source":                    "recording_assignments",
			"worker_kind":               "capture",
			"execution_class":           executionClass,
			"active_capture_stream_ids": streamIDs,
			"active":                    it.Active,
		})
	}
	if assignmentRows.Err() != nil {
		assignmentRows.Close()
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("iterate recording assignments by server: %v", assignmentRows.Err()))
		return
	}
	assignmentRows.Close()

	items := make([]dashboardServerItem, 0, len(serverMap))
	for _, it := range serverMap {
		if !includeStale && !it.Active {
			continue
		}
		it.ActiveCaptureStreamIDs = uniqueInt64Sorted(it.ActiveCaptureStreamIDs)
		sort.Slice(it.Processes, func(i, j int) bool {
			ai := false
			aj := false
			if v, ok := it.Processes[i]["active"].(bool); ok {
				ai = v
			}
			if v, ok := it.Processes[j]["active"].(bool); ok {
				aj = v
			}
			if ai != aj {
				return ai
			}
			pi := fmt.Sprint(it.Processes[i]["process_id"])
			pj := fmt.Sprint(it.Processes[j]["process_id"])
			if pi == pj {
				return fmt.Sprint(it.Processes[i]["source"]) < fmt.Sprint(it.Processes[j]["source"])
			}
			return pi < pj
		})
		items = append(items, *it)
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Active != items[j].Active {
			return items[i].Active
		}
		ti, tj := items[i].LastSeenAt, items[j].LastSeenAt
		if ti == nil && tj == nil {
			return items[i].ServerID < items[j].ServerID
		}
		if ti == nil {
			return false
		}
		if tj == nil {
			return true
		}
		if ti.Equal(*tj) {
			return items[i].ServerID < items[j].ServerID
		}
		return ti.After(*tj)
	})

	util.WriteJSON(w, http.StatusOK, map[string]any{
		"items":             items,
		"hours":             hours,
		"include_stale":     includeStale,
		"include_auxiliary": includeAuxiliary,
		"total":             len(items),
		"active": func() int {
			n := 0
			for i := range items {
				if items[i].Active {
					n++
				}
			}
			return n
		}(),
	})
}

func (s *Server) handleDashboardQueueHealth(w http.ResponseWriter, r *http.Request) {
	var recordingOn int64
	if err := s.pool.QueryRow(r.Context(), `
		SELECT
			COUNT(*) FILTER (WHERE recording_state='on')::bigint
		FROM streams
	`).Scan(&recordingOn); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("query queue health recording counts: %v", err))
		return
	}
	var captureActiveSessions int64
	if err := s.pool.QueryRow(r.Context(), `
		SELECT COUNT(*)::bigint
		FROM stream_capture_runtime scr
		JOIN streams s ON s.id=scr.stream_id
		WHERE s.recording_state='on'
		  AND scr.status='running'
		  AND scr.last_frame_at IS NOT NULL
		  AND scr.last_frame_at >= now() - interval '120 seconds'
	`).Scan(&captureActiveSessions); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("query queue health capture sessions: %v", err))
		return
	}
	var inferenceActiveClaims int64
	if err := s.pool.QueryRow(r.Context(), `
		SELECT COUNT(*)::bigint
		FROM inference_claims
		WHERE status='leased' AND lease_expires_at > now()
	`).Scan(&inferenceActiveClaims); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("query queue health active claims: %v", err))
		return
	}
	var queuedBoxed int64
	if err := s.pool.QueryRow(r.Context(), `
		SELECT COUNT(*)::bigint
		FROM inference_results
		WHERE status='queued_boxed'
	`).Scan(&queuedBoxed); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("query queue health queued boxed: %v", err))
		return
	}
	var boxJobsPending, boxJobsLeased, boxJobsError int64
	if err := s.pool.QueryRow(r.Context(), `
		SELECT
			COUNT(*) FILTER (WHERE status='pending')::bigint,
			COUNT(*) FILTER (WHERE status='leased')::bigint,
			COUNT(*) FILTER (WHERE status='error')::bigint
		FROM inference_box_jobs
	`).Scan(&boxJobsPending, &boxJobsLeased, &boxJobsError); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("query queue health box jobs: %v", err))
		return
	}
	var captureWorkers, inferenceWorkers int64
	if err := s.pool.QueryRow(r.Context(), `
		SELECT COUNT(DISTINCT worker_id)::bigint
		FROM processing_worker_heartbeats
		WHERE worker_kind='capture' AND lease_expires_at > now()
	`).Scan(&captureWorkers); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("query queue health capture workers: %v", err))
		return
	}
	if err := s.pool.QueryRow(r.Context(), `
		SELECT COUNT(DISTINCT worker_id)::bigint
		FROM processing_worker_heartbeats
		WHERE worker_kind='inference' AND lease_expires_at > now()
	`).Scan(&inferenceWorkers); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("query queue health inference workers: %v", err))
		return
	}
	var pipelineCount int64
	if err := s.pool.QueryRow(r.Context(), `
		SELECT COUNT(*)::bigint
		FROM pipelines
		WHERE active=true
	`).Scan(&pipelineCount); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("query queue health pipeline count: %v", err))
		return
	}
	backlogTotal := inferenceActiveClaims + queuedBoxed
	util.WriteJSON(w, http.StatusOK, map[string]any{
		"recording_on":             recordingOn,
		"capture_active_sessions":  captureActiveSessions,
		"capture_active_workers":   captureWorkers,
		"inference_active_workers": inferenceWorkers,
		"inference_active_claims":  inferenceActiveClaims,
		"inference_backlog_frames": backlogTotal,
		"queued_boxed_results":     queuedBoxed,
		"box_jobs_pending":         boxJobsPending,
		"box_jobs_leased":          boxJobsLeased,
		"box_jobs_error":           boxJobsError,
		"pipeline_count":           pipelineCount,
	})
}

type dashboardStreamPipelineUpdateRequest struct {
	Enabled   bool   `json:"enabled"`
	UpdatedBy string `json:"updated_by"`
}

func (s *Server) queryDashboardStreamCaptureWorkers(ctx context.Context, streamID int64) ([]map[string]any, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT
			server_id,
			process_id,
			worker_id,
			status,
			started_at,
			stopped_at,
			last_heartbeat_at,
			last_frame_at,
			restart_count,
			last_error_text
		FROM recording_process_runs
		WHERE stream_id=$1
		ORDER BY COALESCE(last_heartbeat_at, started_at) DESC, id DESC
		LIMIT 20
	`, streamID)
	if err != nil {
		return nil, fmt.Errorf("query stream capture workers: %w", err)
	}
	defer rows.Close()

	items := make([]map[string]any, 0, 4)
	now := time.Now().UTC()
	for rows.Next() {
		var serverID, processID, workerID, status string
		var startedAt time.Time
		var stoppedAt, heartbeatAt, frameAt *time.Time
		var restartCount int
		var lastErrorText *string
		if err := rows.Scan(
			&serverID,
			&processID,
			&workerID,
			&status,
			&startedAt,
			&stoppedAt,
			&heartbeatAt,
			&frameAt,
			&restartCount,
			&lastErrorText,
		); err != nil {
			return nil, fmt.Errorf("scan stream capture worker: %w", err)
		}
		active := status == "starting" || status == "running"
		if heartbeatAt != nil {
			active = active && heartbeatAt.After(now.Add(-120*time.Second))
		}
		items = append(items, map[string]any{
			"server_id":       serverID,
			"process_id":      processID,
			"worker_id":       workerID,
			"status":          status,
			"started_at":      startedAt,
			"stopped_at":      stoppedAt,
			"heartbeat_at":    heartbeatAt,
			"last_frame_at":   frameAt,
			"restart_count":   restartCount,
			"last_error_text": lastErrorText,
			"active":          active,
		})
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("iterate stream capture workers: %w", rows.Err())
	}
	return items, nil
}

func (s *Server) queryDashboardStreamPipelines(ctx context.Context, streamID int64, pipelineID string) ([]map[string]any, error) {
	where := "1=1"
	args := []any{streamID}
	if pipelineID != "" {
		args = append(args, pipelineID)
		where = fmt.Sprintf("p.id=$%d", len(args))
	}
	rows, err := s.pool.Query(ctx, fmt.Sprintf(`
		SELECT
			p.id,
			p.kind,
			p.active,
			COALESCE(sps.enabled, true) AS enabled,
			(sps.stream_id IS NOT NULL) AS has_override,
			sps.updated_by,
			sps.updated_at,
			COALESCE(proc.processed_frames, 0)::bigint,
			GREATEST(COALESCE(cap.success_frames, 0)::bigint - COALESCE(proc.processed_frames, 0)::bigint, 0)::bigint AS backlog_frames,
			COALESCE(cl.active_claims, 0)::bigint,
			COALESCE(cl.active_workers, ARRAY[]::text[]),
			proc.last_result_status,
			proc.last_result_at
		FROM pipelines p
		LEFT JOIN stream_pipeline_settings sps
			ON sps.stream_id=$1 AND sps.pipeline_id=p.id
		LEFT JOIN LATERAL (
			SELECT
				COUNT(DISTINCT ir.frame_id)::bigint AS processed_frames,
				(ARRAY_AGG(ir.status ORDER BY ir.created_at DESC, ir.id DESC))[1] AS last_result_status,
				MAX(ir.created_at) AS last_result_at
			FROM inference_results ir
			JOIN frames f ON f.id=ir.frame_id
				WHERE f.stream_id=$1
				  AND ir.pipeline_id=p.id
				  AND ir.status IN ('success','queued_boxed')
		) proc ON true
		LEFT JOIN LATERAL (
			SELECT COUNT(*)::bigint AS success_frames
			FROM frames f
			WHERE f.stream_id=$1
			  AND f.capture_status='success'
		) cap ON true
		LEFT JOIN LATERAL (
			SELECT
				COUNT(*)::bigint AS active_claims,
				ARRAY_REMOVE(ARRAY_AGG(DISTINCT ic.claimed_by ORDER BY ic.claimed_by), NULL) AS active_workers
			FROM inference_claims ic
			JOIN frames f ON f.id=ic.frame_id
			WHERE ic.pipeline_id=p.id
			  AND f.stream_id=$1
			  AND ic.status='leased'
			  AND ic.lease_expires_at > now()
		) cl ON true
		WHERE %s
		ORDER BY p.id ASC
	`, where), args...)
	if err != nil {
		return nil, fmt.Errorf("query stream pipelines: %w", err)
	}
	defer rows.Close()

	items := make([]map[string]any, 0, 32)
	for rows.Next() {
		var pid, kind string
		var active, enabled, hasOverride bool
		var updatedBy *string
		var updatedAt *time.Time
		var processedFrames, backlogFrames, activeClaims int64
		var activeWorkers []string
		var lastResultStatus *string
		var lastResultAt *time.Time
		if err := rows.Scan(
			&pid, &kind, &active, &enabled, &hasOverride, &updatedBy, &updatedAt,
			&processedFrames, &backlogFrames, &activeClaims, &activeWorkers, &lastResultStatus, &lastResultAt,
		); err != nil {
			return nil, fmt.Errorf("scan stream pipeline item: %w", err)
		}
		if !enabled {
			backlogFrames = 0
		}
		items = append(items, map[string]any{
			"pipeline_id":        pid,
			"kind":               kind,
			"active":             active,
			"enabled":            enabled,
			"has_override":       hasOverride,
			"updated_by":         updatedBy,
			"updated_at":         updatedAt,
			"processed_frames":   processedFrames,
			"backlog_frames":     backlogFrames,
			"active_claims":      activeClaims,
			"active_workers":     activeWorkers,
			"last_result_status": lastResultStatus,
			"last_result_at":     lastResultAt,
		})
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("iterate stream pipeline rows: %w", rows.Err())
	}
	return items, nil
}

func (s *Server) handleDashboardStreamPipelinesList(w http.ResponseWriter, r *http.Request) {
	streamID, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	items, err := s.queryDashboardStreamPipelines(r.Context(), streamID, "")
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load stream pipelines: %v", err))
		return
	}
	captureWorkers, err := s.queryDashboardStreamCaptureWorkers(r.Context(), streamID)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load stream capture workers: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{
		"stream_id":       streamID,
		"capture_workers": captureWorkers,
		"items":           items,
	})
}

func (s *Server) handleDashboardStreamPipelineUpsert(w http.ResponseWriter, r *http.Request) {
	streamID, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	pipelineID := strings.TrimSpace(chi.URLParam(r, "pipeline_id"))
	if pipelineID == "" {
		util.WriteError(w, http.StatusBadRequest, "pipeline_id is required")
		return
	}
	var req dashboardStreamPipelineUpdateRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	var streamExists bool
	if err := s.pool.QueryRow(r.Context(), `SELECT EXISTS(SELECT 1 FROM streams WHERE id=$1)`, streamID).Scan(&streamExists); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("check stream exists: %v", err))
		return
	}
	if !streamExists {
		util.WriteError(w, http.StatusNotFound, "stream not found")
		return
	}
	var pipelineExists bool
	if err := s.pool.QueryRow(r.Context(), `SELECT EXISTS(SELECT 1 FROM pipelines WHERE id=$1)`, pipelineID).Scan(&pipelineExists); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("check pipeline exists: %v", err))
		return
	}
	if !pipelineExists {
		util.WriteError(w, http.StatusNotFound, "pipeline not found")
		return
	}
	updatedBy := strings.TrimSpace(req.UpdatedBy)
	if req.Enabled {
		if _, err := s.pool.Exec(r.Context(), `
			DELETE FROM stream_pipeline_settings
			WHERE stream_id=$1 AND pipeline_id=$2
		`, streamID, pipelineID); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("enable stream pipeline: %v", err))
			return
		}
	} else {
		if _, err := s.pool.Exec(r.Context(), `
			INSERT INTO stream_pipeline_settings (stream_id, pipeline_id, enabled, updated_by, created_at, updated_at)
			VALUES ($1, $2, false, $3, now(), now())
			ON CONFLICT (stream_id, pipeline_id)
			DO UPDATE SET enabled=false, updated_by=EXCLUDED.updated_by, updated_at=now()
		`, streamID, pipelineID, updatedBy); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("disable stream pipeline: %v", err))
			return
		}
	}
	items, err := s.queryDashboardStreamPipelines(r.Context(), streamID, pipelineID)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("reload stream pipeline: %v", err))
		return
	}
	if len(items) == 0 {
		util.WriteError(w, http.StatusNotFound, "pipeline not found after update")
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{
		"stream_id": streamID,
		"item":      items[0],
	})
}

func (s *Server) handleDashboardInference(w http.ResponseWriter, r *http.Request) {
	limit := parseIntQuery(r, "limit", 200, 1, 2000)
	offset := parseIntQuery(r, "offset", 0, 0, 1000000)
	pipelineID := strings.TrimSpace(r.URL.Query().Get("pipeline_id"))
	status := strings.TrimSpace(strings.ToLower(r.URL.Query().Get("status")))
	className := strings.TrimSpace(r.URL.Query().Get("class_name"))
	search := strings.TrimSpace(r.URL.Query().Get("q"))
	minConfidence := parseFloat64QueryPtr(r, "min_confidence")
	createdFrom := parseTimeQueryPtr(r, "created_from")
	createdTo := parseTimeQueryPtr(r, "created_to")
	capturedFrom := parseTimeQueryPtr(r, "captured_from")
	capturedTo := parseTimeQueryPtr(r, "captured_to")
	hasBoxed := parseBoolQueryPtr(r, "has_boxed")
	recordingStateRaw := strings.TrimSpace(strings.ToLower(r.URL.Query().Get("recording_state")))

	if status != "" && !isInferenceResultStatus(status) {
		util.WriteError(w, http.StatusBadRequest, "invalid status; expected queued_boxed|success|error")
		return
	}
	if raw := strings.TrimSpace(r.URL.Query().Get("min_confidence")); raw != "" && minConfidence == nil {
		util.WriteError(w, http.StatusBadRequest, "invalid min_confidence")
		return
	}
	if minConfidence != nil && *minConfidence <= 0 {
		// Treat non-positive values as unset to avoid forcing an expensive detections EXISTS filter.
		minConfidence = nil
	}
	if raw := strings.TrimSpace(r.URL.Query().Get("created_from")); raw != "" && createdFrom == nil {
		util.WriteError(w, http.StatusBadRequest, "invalid created_from; expected RFC3339 or YYYY-MM-DD")
		return
	}
	if raw := strings.TrimSpace(r.URL.Query().Get("created_to")); raw != "" && createdTo == nil {
		util.WriteError(w, http.StatusBadRequest, "invalid created_to; expected RFC3339 or YYYY-MM-DD")
		return
	}
	if raw := strings.TrimSpace(r.URL.Query().Get("captured_from")); raw != "" && capturedFrom == nil {
		util.WriteError(w, http.StatusBadRequest, "invalid captured_from; expected RFC3339 or YYYY-MM-DD")
		return
	}
	if raw := strings.TrimSpace(r.URL.Query().Get("captured_to")); raw != "" && capturedTo == nil {
		util.WriteError(w, http.StatusBadRequest, "invalid captured_to; expected RFC3339 or YYYY-MM-DD")
		return
	}

	orderColumns := map[string]string{
		"created_at":  "ir.created_at",
		"captured_at": "f.captured_at",
		"pipeline_id": "ir.pipeline_id",
		"status":      "ir.status",
		"stream_id":   "f.stream_id",
		// Aggregate sorts are applied in-memory for the loaded page.
		"detection_count": "ir.created_at",
		"max_confidence":  "ir.created_at",
		"signal_count":    "ir.created_at",
		"signal_strength": "ir.created_at",
	}
	orderExpr, sortBy, sortDir, ok := parseSortQuery(w, r, orderColumns, "created_at", "desc")
	if !ok {
		return
	}

	where := []string{"1=1"}
	args := make([]any, 0, 24)
	if streamID := parseInt64QueryPtr(r, "stream_id"); streamID != nil {
		args = append(args, *streamID)
		where = append(where, fmt.Sprintf("f.stream_id=$%d", len(args)))
	}
	if pipelineID != "" {
		args = append(args, pipelineID)
		where = append(where, fmt.Sprintf("ir.pipeline_id=$%d", len(args)))
	}
	if status != "" {
		args = append(args, status)
		where = append(where, fmt.Sprintf("ir.status=$%d", len(args)))
	}
	if hasBoxed != nil {
		if *hasBoxed {
			where = append(where, "ir.boxed_media_object_id IS NOT NULL")
		} else {
			where = append(where, "ir.boxed_media_object_id IS NULL")
		}
	}
	if recordingStateRaw != "" {
		state, ok := parseRecordingState(recordingStateRaw)
		if !ok {
			util.WriteError(w, http.StatusBadRequest, "invalid recording_state; expected off|on")
			return
		}
		args = append(args, string(state))
		where = append(where, fmt.Sprintf("s.recording_state=$%d", len(args)))
	}
	if search != "" {
		args = append(args, "%"+search+"%")
		where = append(where, fmt.Sprintf(`(
			s.name ILIKE $%d OR
			s.provider ILIKE $%d OR
			s.slug ILIKE $%d OR
			ir.pipeline_id ILIKE $%d OR
			COALESCE(ir.error_text, '') ILIKE $%d
		)`, len(args), len(args), len(args), len(args), len(args)))
	}
	if createdFrom != nil {
		args = append(args, *createdFrom)
		where = append(where, fmt.Sprintf("ir.created_at >= $%d", len(args)))
	}
	if createdTo != nil {
		args = append(args, *createdTo)
		where = append(where, fmt.Sprintf("ir.created_at <= $%d", len(args)))
	}
	if capturedFrom != nil {
		args = append(args, *capturedFrom)
		where = append(where, fmt.Sprintf("f.captured_at >= $%d", len(args)))
	}
	if capturedTo != nil {
		args = append(args, *capturedTo)
		where = append(where, fmt.Sprintf("f.captured_at <= $%d", len(args)))
	}
	if className != "" || minConfidence != nil {
		detectionWhere := []string{"d.inference_result_id=ir.id"}
		if className != "" {
			args = append(args, className)
			detectionWhere = append(detectionWhere, fmt.Sprintf("d.class_name ILIKE $%d", len(args)))
		}
		if minConfidence != nil {
			args = append(args, *minConfidence)
			detectionWhere = append(detectionWhere, fmt.Sprintf("d.confidence >= $%d", len(args)))
		}
		where = append(where, fmt.Sprintf("EXISTS (SELECT 1 FROM detections d WHERE %s)", strings.Join(detectionWhere, " AND ")))
	}

	args = append(args, limit, offset)
	query := fmt.Sprintf(`
			SELECT
				ir.id, ir.pipeline_id, ir.revision, ir.status, ir.summary_jsonb, ir.error_text, ir.created_at, ir.finished_at,
				f.id, f.stream_id, f.captured_at,
				s.provider, s.name, s.slug, s.recording_state,
				raw.object_key, boxed.object_key
		FROM inference_results ir
		JOIN frames f ON f.id=ir.frame_id
		JOIN streams s ON s.id=f.stream_id
		LEFT JOIN media_objects raw ON raw.id=f.raw_media_object_id
		LEFT JOIN media_objects boxed ON boxed.id=ir.boxed_media_object_id
		WHERE %s
		ORDER BY %s %s, ir.id DESC
		LIMIT $%d OFFSET $%d
	`, strings.Join(where, " AND "), orderExpr, sortDir, len(args)-1, len(args))

	rows, err := s.pool.Query(r.Context(), query, args...)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("dashboard inference query: %v", err))
		return
	}
	defer rows.Close()

	type item struct {
		InferenceResultID int64          `json:"inference_result_id"`
		PipelineID        string         `json:"pipeline_id"`
		Revision          int            `json:"revision"`
		Status            string         `json:"status"`
		Summary           map[string]any `json:"summary,omitempty"`
		ErrorText         *string        `json:"error_text,omitempty"`
		CreatedAt         time.Time      `json:"created_at"`
		FinishedAt        *time.Time     `json:"finished_at,omitempty"`
		FrameID           int64          `json:"frame_id"`
		StreamID          int64          `json:"stream_id"`
		FrameCapturedAt   time.Time      `json:"frame_captured_at"`
		StreamProvider    string         `json:"stream_provider"`
		StreamName        string         `json:"stream_name"`
		StreamSlug        string         `json:"stream_slug"`
		RecordingState    string         `json:"recording_state"`
		RawObjectKey      *string        `json:"raw_object_key,omitempty"`
		RawImageURL       string         `json:"raw_image_url,omitempty"`
		BoxedObjectKey    *string        `json:"boxed_object_key,omitempty"`
		BoxedImageURL     string         `json:"boxed_image_url,omitempty"`
		DetectionCount    int64          `json:"detection_count"`
		MaxConfidence     *float64       `json:"max_confidence,omitempty"`
		SignalCount       int64          `json:"signal_count"`
		SignalSummary     string         `json:"signal_summary,omitempty"`
		MaxSignalStrength *float64       `json:"max_signal_strength,omitempty"`
	}

	items := make([]item, 0, limit)
	for rows.Next() {
		var it item
		var summaryBytes []byte
		if err := rows.Scan(
			&it.InferenceResultID, &it.PipelineID, &it.Revision, &it.Status, &summaryBytes, &it.ErrorText, &it.CreatedAt, &it.FinishedAt,
			&it.FrameID, &it.StreamID, &it.FrameCapturedAt,
			&it.StreamProvider, &it.StreamName, &it.StreamSlug, &it.RecordingState,
			&it.RawObjectKey, &it.BoxedObjectKey,
		); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan dashboard inference: %v", err))
			return
		}
		if len(summaryBytes) > 0 {
			var m map[string]any
			if err := json.Unmarshal(summaryBytes, &m); err == nil {
				it.Summary = m
			}
		}
		if it.RawObjectKey != nil && *it.RawObjectKey != "" {
			if url, err := s.r2.PresignGet(r.Context(), *it.RawObjectKey, s.cfg.R2SignGetTTL); err == nil {
				it.RawImageURL = url
			}
		}
		if it.BoxedObjectKey != nil && *it.BoxedObjectKey != "" {
			if url, err := s.r2.PresignGet(r.Context(), *it.BoxedObjectKey, s.cfg.R2SignGetTTL); err == nil {
				it.BoxedImageURL = url
			}
		}
		items = append(items, it)
	}
	if rows.Err() != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("iterate dashboard inference: %v", rows.Err()))
		return
	}
	if len(items) > 0 {
		resultIDs := make([]int64, 0, len(items))
		indexByID := make(map[int64]int, len(items))
		for i := range items {
			resultIDs = append(resultIDs, items[i].InferenceResultID)
			indexByID[items[i].InferenceResultID] = i
		}
		detRows, err := s.pool.Query(r.Context(), `
			SELECT inference_result_id, COUNT(*)::bigint, MAX(confidence)
			FROM detections
			WHERE inference_result_id = ANY($1)
			GROUP BY inference_result_id
		`, resultIDs)
		if err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("dashboard inference detections query: %v", err))
			return
		}
		for detRows.Next() {
			var resultID int64
			var count int64
			var maxConf *float64
			if err := detRows.Scan(&resultID, &count, &maxConf); err != nil {
				detRows.Close()
				util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan dashboard inference detections: %v", err))
				return
			}
			if idx, ok := indexByID[resultID]; ok {
				items[idx].DetectionCount = count
				items[idx].MaxConfidence = maxConf
			}
		}
		if err := detRows.Err(); err != nil {
			detRows.Close()
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("iterate dashboard inference detections: %v", err))
			return
		}
		detRows.Close()

		sigRows, err := s.pool.Query(r.Context(), `
			WITH ranked AS (
				SELECT
					inference_result_id,
					COALESCE(confidence, value_num) AS strength,
					CASE
						WHEN value_num IS NOT NULL THEN signal_key || '=' || trim(to_char(value_num, 'FM999999990.0000'))
						WHEN value_text IS NOT NULL AND btrim(value_text) <> '' THEN signal_key || '=' || value_text
						ELSE signal_key
					END AS item,
					ROW_NUMBER() OVER (
						PARTITION BY inference_result_id
						ORDER BY COALESCE(confidence, value_num) DESC NULLS LAST, id ASC
					) AS rn
				FROM inference_signals
				WHERE inference_result_id = ANY($1)
			)
			SELECT
				inference_result_id,
				COUNT(*)::bigint AS signal_count,
				MAX(strength) AS max_signal_strength,
				COALESCE(string_agg(item, ' | ' ORDER BY rn) FILTER (WHERE rn <= 3), '') AS signal_summary
			FROM ranked
			GROUP BY inference_result_id
		`, resultIDs)
		if err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("dashboard inference signals query: %v", err))
			return
		}
		for sigRows.Next() {
			var resultID int64
			var signalCount int64
			var maxStrength *float64
			var summary string
			if err := sigRows.Scan(&resultID, &signalCount, &maxStrength, &summary); err != nil {
				sigRows.Close()
				util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan dashboard inference signals: %v", err))
				return
			}
			if idx, ok := indexByID[resultID]; ok {
				items[idx].SignalCount = signalCount
				items[idx].MaxSignalStrength = maxStrength
				items[idx].SignalSummary = summary
			}
		}
		if err := sigRows.Err(); err != nil {
			sigRows.Close()
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("iterate dashboard inference signals: %v", err))
			return
		}
		sigRows.Close()

		switch sortBy {
		case "detection_count":
			sort.SliceStable(items, func(i, j int) bool {
				if items[i].DetectionCount == items[j].DetectionCount {
					if sortDir == "asc" {
						return items[i].InferenceResultID < items[j].InferenceResultID
					}
					return items[i].InferenceResultID > items[j].InferenceResultID
				}
				if sortDir == "asc" {
					return items[i].DetectionCount < items[j].DetectionCount
				}
				return items[i].DetectionCount > items[j].DetectionCount
			})
		case "max_confidence":
			sort.SliceStable(items, func(i, j int) bool {
				ai := -1.0
				aj := -1.0
				if items[i].MaxConfidence != nil {
					ai = *items[i].MaxConfidence
				}
				if items[j].MaxConfidence != nil {
					aj = *items[j].MaxConfidence
				}
				if ai == aj {
					if sortDir == "asc" {
						return items[i].InferenceResultID < items[j].InferenceResultID
					}
					return items[i].InferenceResultID > items[j].InferenceResultID
				}
				if sortDir == "asc" {
					return ai < aj
				}
				return ai > aj
			})
		case "signal_count":
			sort.SliceStable(items, func(i, j int) bool {
				if items[i].SignalCount == items[j].SignalCount {
					if sortDir == "asc" {
						return items[i].InferenceResultID < items[j].InferenceResultID
					}
					return items[i].InferenceResultID > items[j].InferenceResultID
				}
				if sortDir == "asc" {
					return items[i].SignalCount < items[j].SignalCount
				}
				return items[i].SignalCount > items[j].SignalCount
			})
		case "signal_strength":
			sort.SliceStable(items, func(i, j int) bool {
				ai := -1.0
				aj := -1.0
				if items[i].MaxSignalStrength != nil {
					ai = *items[i].MaxSignalStrength
				}
				if items[j].MaxSignalStrength != nil {
					aj = *items[j].MaxSignalStrength
				}
				if ai == aj {
					if sortDir == "asc" {
						return items[i].InferenceResultID < items[j].InferenceResultID
					}
					return items[i].InferenceResultID > items[j].InferenceResultID
				}
				if sortDir == "asc" {
					return ai < aj
				}
				return ai > aj
			})
		}
	}

	util.WriteJSON(w, http.StatusOK, map[string]any{
		"items":  items,
		"limit":  limit,
		"offset": offset,
	})
}

func (s *Server) handleDashboardInferenceCleanupUnboxed(w http.ResponseWriter, r *http.Request) {
	pipelineID := strings.TrimSpace(r.URL.Query().Get("pipeline_id"))
	mode := strings.TrimSpace(strings.ToLower(r.URL.Query().Get("mode")))
	if mode == "" {
		mode = "requeue"
	}
	if mode != "requeue" && mode != "delete" {
		util.WriteError(w, http.StatusBadRequest, "invalid mode; expected requeue|delete")
		return
	}
	dryRunRaw := strings.TrimSpace(strings.ToLower(r.URL.Query().Get("dry_run")))
	dryRun := false
	switch dryRunRaw {
	case "", "0", "false", "f", "no", "n", "off":
		dryRun = false
	case "1", "true", "t", "yes", "y", "on":
		dryRun = true
	default:
		util.WriteError(w, http.StatusBadRequest, "invalid dry_run; expected true|false")
		return
	}

	where := []string{
		"ir.status='success'",
		"ir.boxed_media_object_id IS NULL",
		"EXISTS (SELECT 1 FROM detections d WHERE d.inference_result_id=ir.id)",
	}
	args := make([]any, 0, 2)
	if pipelineID != "" {
		args = append(args, pipelineID)
		where = append(where, fmt.Sprintf("ir.pipeline_id=$%d", len(args)))
	}
	whereClause := strings.Join(where, " AND ")

	var candidateInferenceRows int64
	if err := s.pool.QueryRow(r.Context(), fmt.Sprintf(`
		SELECT COUNT(*)
		FROM inference_results ir
		WHERE %s
	`, whereClause), args...).Scan(&candidateInferenceRows); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("count cleanup candidates: %v", err))
		return
	}
	var candidateDetectionRows int64
	if err := s.pool.QueryRow(r.Context(), fmt.Sprintf(`
		SELECT COUNT(*)
		FROM detections d
		WHERE EXISTS (
			SELECT 1
			FROM inference_results ir
			WHERE ir.id=d.inference_result_id AND %s
		)
	`, whereClause), args...).Scan(&candidateDetectionRows); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("count cleanup detections: %v", err))
		return
	}
	if dryRun {
		util.WriteJSON(w, http.StatusOK, map[string]any{
			"dry_run":                      true,
			"mode":                         mode,
			"pipeline_id":                  pipelineID,
			"inference_results_candidates": candidateInferenceRows,
			"detections_candidates":        candidateDetectionRows,
		})
		return
	}

	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("begin cleanup tx: %v", err))
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()

	if mode == "delete" {
		var deletedInferenceRows int64
		if err := tx.QueryRow(r.Context(), fmt.Sprintf(`
			WITH doomed AS (
				SELECT ir.id
				FROM inference_results ir
				WHERE %s
				FOR UPDATE
			), deleted AS (
				DELETE FROM inference_results ir
				USING doomed d
				WHERE ir.id=d.id
				RETURNING ir.id
			)
			SELECT COUNT(*) FROM deleted
		`, whereClause), args...).Scan(&deletedInferenceRows); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("delete invalid inference results: %v", err))
			return
		}
		if err := tx.Commit(r.Context()); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("commit cleanup tx: %v", err))
			return
		}

		util.WriteJSON(w, http.StatusOK, map[string]any{
			"dry_run":                     false,
			"mode":                        mode,
			"pipeline_id":                 pipelineID,
			"inference_results_deleted":   deletedInferenceRows,
			"detections_deleted_estimate": candidateDetectionRows,
		})
		return
	}

	maxAttempts := s.cfg.InferenceBoxMaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 8
	}
	argsWithAttempts := append(args, maxAttempts)

	var queuedRows int64
	var jobsQueued int64
	if err := tx.QueryRow(r.Context(), fmt.Sprintf(`
		WITH doomed AS (
			SELECT ir.id
			FROM inference_results ir
			WHERE %s
			FOR UPDATE
		), queued AS (
			UPDATE inference_results ir
			SET status='queued_boxed',
			    error_text=NULL
			FROM doomed d
			WHERE ir.id=d.id
			RETURNING ir.id
		), upserted AS (
			INSERT INTO inference_box_jobs (
				inference_result_id, status, lease_owner, lease_expires_at,
				attempt_count, max_attempts, next_retry_at, error_text
			)
			SELECT
				q.id, 'pending', NULL, NULL,
				0, $%d, now(), NULL
			FROM queued q
			ON CONFLICT (inference_result_id)
			DO UPDATE SET
				status='pending',
				lease_owner=NULL,
				lease_expires_at=NULL,
				next_retry_at=now(),
				error_text=NULL,
				updated_at=now()
			RETURNING inference_result_id
		)
		SELECT
			(SELECT COUNT(*) FROM queued),
			(SELECT COUNT(*) FROM upserted)
	`, whereClause, len(argsWithAttempts)), argsWithAttempts...).Scan(&queuedRows, &jobsQueued); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("requeue invalid inference results: %v", err))
		return
	}
	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("commit cleanup tx: %v", err))
		return
	}

	util.WriteJSON(w, http.StatusOK, map[string]any{
		"dry_run":                    false,
		"mode":                       mode,
		"pipeline_id":                pipelineID,
		"inference_results_requeued": queuedRows,
		"box_jobs_queued":            jobsQueued,
		"detections_candidates":      candidateDetectionRows,
	})
}

func (s *Server) handleDashboardStreamDetail(w http.ResponseWriter, r *http.Request) {
	id, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	pipelineID := strings.TrimSpace(r.URL.Query().Get("pipeline_id"))
	status := strings.TrimSpace(strings.ToLower(r.URL.Query().Get("status")))
	className := strings.TrimSpace(r.URL.Query().Get("class_name"))
	search := strings.TrimSpace(r.URL.Query().Get("q"))
	minConfidence := parseFloat64QueryPtr(r, "min_confidence")
	createdFrom := parseTimeQueryPtr(r, "created_from")
	createdTo := parseTimeQueryPtr(r, "created_to")
	capturedFrom := parseTimeQueryPtr(r, "captured_from")
	capturedTo := parseTimeQueryPtr(r, "captured_to")
	hasBoxed := parseBoolQueryPtr(r, "has_boxed")
	limit := parseIntQuery(r, "limit", 50, 1, 500)
	offset := parseIntQuery(r, "offset", 0, 0, 1000000)

	if status != "" && !isInferenceResultStatus(status) {
		util.WriteError(w, http.StatusBadRequest, "invalid status; expected queued_boxed|success|error")
		return
	}
	if raw := strings.TrimSpace(r.URL.Query().Get("min_confidence")); raw != "" && minConfidence == nil {
		util.WriteError(w, http.StatusBadRequest, "invalid min_confidence")
		return
	}
	if raw := strings.TrimSpace(r.URL.Query().Get("created_from")); raw != "" && createdFrom == nil {
		util.WriteError(w, http.StatusBadRequest, "invalid created_from; expected RFC3339 or YYYY-MM-DD")
		return
	}
	if raw := strings.TrimSpace(r.URL.Query().Get("created_to")); raw != "" && createdTo == nil {
		util.WriteError(w, http.StatusBadRequest, "invalid created_to; expected RFC3339 or YYYY-MM-DD")
		return
	}
	if raw := strings.TrimSpace(r.URL.Query().Get("captured_from")); raw != "" && capturedFrom == nil {
		util.WriteError(w, http.StatusBadRequest, "invalid captured_from; expected RFC3339 or YYYY-MM-DD")
		return
	}
	if raw := strings.TrimSpace(r.URL.Query().Get("captured_to")); raw != "" && capturedTo == nil {
		util.WriteError(w, http.StatusBadRequest, "invalid captured_to; expected RFC3339 or YYYY-MM-DD")
		return
	}
	orderColumns := map[string]string{
		"created_at":      "ir.created_at",
		"captured_at":     "f.captured_at",
		"pipeline_id":     "ir.pipeline_id",
		"status":          "ir.status",
		"detection_count": "ds.detection_count",
		"max_confidence":  "COALESCE(ds.max_confidence, -1)",
		"signal_count":    "COALESCE(sig.signal_count, 0)",
		"signal_strength": "COALESCE(sig.max_signal_strength, -1)",
	}
	orderExpr, _, sortDir, ok := parseSortQuery(w, r, orderColumns, "created_at", "desc")
	if !ok {
		return
	}

	stream, err := s.getStreamByID(r.Context(), id)
	if err != nil {
		util.WriteError(w, http.StatusNotFound, err.Error())
		return
	}

	type latestFrame struct {
		FrameID      int64     `json:"frame_id"`
		CapturedAt   time.Time `json:"captured_at"`
		CaptureState string    `json:"capture_status"`
		ObjectKey    *string   `json:"object_key,omitempty"`
		DownloadURL  string    `json:"download_url,omitempty"`
	}
	var lf latestFrame
	lfErr := s.pool.QueryRow(r.Context(), `
		SELECT f.id, f.captured_at, f.capture_status, mo.object_key
		FROM frames f
		LEFT JOIN media_objects mo ON mo.id=f.raw_media_object_id
		WHERE f.stream_id=$1
		ORDER BY f.captured_at DESC, f.id DESC
		LIMIT 1
	`, id).Scan(&lf.FrameID, &lf.CapturedAt, &lf.CaptureState, &lf.ObjectKey)
	var lfPtr *latestFrame
	if lfErr == nil {
		lfPtr = &lf
		if lf.ObjectKey != nil && *lf.ObjectKey != "" {
			if url, err := s.r2.PresignGet(r.Context(), *lf.ObjectKey, s.cfg.R2SignGetTTL); err == nil {
				lf.DownloadURL = url
			}
		}
	} else if !errors.Is(lfErr, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("query latest frame: %v", lfErr))
		return
	}

	where := []string{"f.stream_id=$1"}
	args := make([]any, 0, 18)
	args = append(args, id)
	if pipelineID != "" {
		args = append(args, pipelineID)
		where = append(where, fmt.Sprintf("ir.pipeline_id=$%d", len(args)))
	}
	if status != "" {
		args = append(args, status)
		where = append(where, fmt.Sprintf("ir.status=$%d", len(args)))
	}
	if hasBoxed != nil {
		if *hasBoxed {
			where = append(where, "ir.boxed_media_object_id IS NOT NULL")
		} else {
			where = append(where, "ir.boxed_media_object_id IS NULL")
		}
	}
	if search != "" {
		args = append(args, "%"+search+"%")
		where = append(where, fmt.Sprintf(`(
			ir.pipeline_id ILIKE $%d OR
			COALESCE(ir.error_text, '') ILIKE $%d OR
			COALESCE(ir.summary_jsonb::text, '') ILIKE $%d
		)`, len(args), len(args), len(args)))
	}
	if createdFrom != nil {
		args = append(args, *createdFrom)
		where = append(where, fmt.Sprintf("ir.created_at >= $%d", len(args)))
	}
	if createdTo != nil {
		args = append(args, *createdTo)
		where = append(where, fmt.Sprintf("ir.created_at <= $%d", len(args)))
	}
	if capturedFrom != nil {
		args = append(args, *capturedFrom)
		where = append(where, fmt.Sprintf("f.captured_at >= $%d", len(args)))
	}
	if capturedTo != nil {
		args = append(args, *capturedTo)
		where = append(where, fmt.Sprintf("f.captured_at <= $%d", len(args)))
	}
	if className != "" || minConfidence != nil {
		detectionWhere := []string{"d.inference_result_id=ir.id"}
		if className != "" {
			args = append(args, className)
			detectionWhere = append(detectionWhere, fmt.Sprintf("d.class_name ILIKE $%d", len(args)))
		}
		if minConfidence != nil {
			args = append(args, *minConfidence)
			detectionWhere = append(detectionWhere, fmt.Sprintf("d.confidence >= $%d", len(args)))
		}
		where = append(where, fmt.Sprintf("EXISTS (SELECT 1 FROM detections d WHERE %s)", strings.Join(detectionWhere, " AND ")))
	}

	args = append(args, limit, offset)

	rows, err := s.pool.Query(r.Context(), fmt.Sprintf(`
		SELECT
			ir.id, ir.pipeline_id, ir.revision, ir.status, ir.summary_jsonb, ir.error_text,
			ir.created_at, ir.finished_at,
			f.id, f.captured_at,
			raw.object_key, boxed.object_key,
			ds.detection_count, ds.max_confidence,
			COALESCE(sig.signal_count, 0)::bigint,
			COALESCE(sig.signal_summary, ''),
			sig.max_signal_strength
		FROM inference_results ir
		JOIN frames f ON f.id=ir.frame_id
		LEFT JOIN media_objects raw ON raw.id=f.raw_media_object_id
		LEFT JOIN media_objects boxed ON boxed.id=ir.boxed_media_object_id
		LEFT JOIN LATERAL (
			SELECT COUNT(*)::bigint AS detection_count, MAX(d.confidence) AS max_confidence
			FROM detections d
			WHERE d.inference_result_id=ir.id
		) ds ON true
		LEFT JOIN LATERAL (
			SELECT
				COUNT(*)::bigint AS signal_count,
				MAX(COALESCE(sg.confidence, sg.value_num)) AS max_signal_strength,
				COALESCE((
					SELECT string_agg(item, ' | ')
					FROM (
						SELECT CASE
							WHEN sg2.value_num IS NOT NULL THEN sg2.signal_key || '=' || trim(to_char(sg2.value_num, 'FM999999990.0000'))
							WHEN sg2.value_text IS NOT NULL AND btrim(sg2.value_text) <> '' THEN sg2.signal_key || '=' || sg2.value_text
							ELSE sg2.signal_key
						END AS item
						FROM inference_signals sg2
						WHERE sg2.inference_result_id=ir.id
						ORDER BY COALESCE(sg2.confidence, sg2.value_num) DESC NULLS LAST, sg2.id ASC
						LIMIT 3
					) top
				), '') AS signal_summary
			FROM inference_signals sg
			WHERE sg.inference_result_id=ir.id
		) sig ON true
		WHERE %s
		ORDER BY %s %s, ir.id DESC
		LIMIT $%d OFFSET $%d
	`, strings.Join(where, " AND "), orderExpr, sortDir, len(args)-1, len(args)), args...)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("query stream inference: %v", err))
		return
	}
	defer rows.Close()

	type inferenceItem struct {
		InferenceResultID int64          `json:"inference_result_id"`
		PipelineID        string         `json:"pipeline_id"`
		Revision          int            `json:"revision"`
		Status            string         `json:"status"`
		Summary           map[string]any `json:"summary,omitempty"`
		ErrorText         *string        `json:"error_text,omitempty"`
		CreatedAt         time.Time      `json:"created_at"`
		FinishedAt        *time.Time     `json:"finished_at,omitempty"`
		FrameID           int64          `json:"frame_id"`
		FrameCapturedAt   time.Time      `json:"frame_captured_at"`
		RawObjectKey      *string        `json:"raw_object_key,omitempty"`
		RawImageURL       string         `json:"raw_image_url,omitempty"`
		BoxedObjectKey    *string        `json:"boxed_object_key,omitempty"`
		BoxedImageURL     string         `json:"boxed_image_url,omitempty"`
		DetectionCount    int64          `json:"detection_count"`
		MaxConfidence     *float64       `json:"max_confidence,omitempty"`
		SignalCount       int64          `json:"signal_count"`
		SignalSummary     string         `json:"signal_summary,omitempty"`
		MaxSignalStrength *float64       `json:"max_signal_strength,omitempty"`
	}
	items := make([]inferenceItem, 0, limit)
	for rows.Next() {
		var it inferenceItem
		var summaryBytes []byte
		if err := rows.Scan(
			&it.InferenceResultID, &it.PipelineID, &it.Revision, &it.Status, &summaryBytes, &it.ErrorText,
			&it.CreatedAt, &it.FinishedAt,
			&it.FrameID, &it.FrameCapturedAt,
			&it.RawObjectKey, &it.BoxedObjectKey,
			&it.DetectionCount, &it.MaxConfidence,
			&it.SignalCount, &it.SignalSummary, &it.MaxSignalStrength,
		); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan stream inference: %v", err))
			return
		}
		if len(summaryBytes) > 0 {
			var m map[string]any
			if err := json.Unmarshal(summaryBytes, &m); err == nil {
				it.Summary = m
			}
		}
		if it.RawObjectKey != nil && *it.RawObjectKey != "" {
			if url, err := s.r2.PresignGet(r.Context(), *it.RawObjectKey, s.cfg.R2SignGetTTL); err == nil {
				it.RawImageURL = url
			}
		}
		if it.BoxedObjectKey != nil && *it.BoxedObjectKey != "" {
			if url, err := s.r2.PresignGet(r.Context(), *it.BoxedObjectKey, s.cfg.R2SignGetTTL); err == nil {
				it.BoxedImageURL = url
			}
		}
		items = append(items, it)
	}
	if rows.Err() != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("iterate stream inference: %v", rows.Err()))
		return
	}

	util.WriteJSON(w, http.StatusOK, map[string]any{
		"stream":       stream,
		"latest_frame": lfPtr,
		"inference":    items,
		"limit":        limit,
		"offset":       offset,
	})
}

func (s *Server) handleDashboardStreamRecording(w http.ResponseWriter, r *http.Request) {
	id, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	stream, err := s.getStreamByID(r.Context(), id)
	if err != nil {
		util.WriteError(w, http.StatusNotFound, err.Error())
		return
	}

	runsLimit := parseIntQuery(r, "runs_limit", 200, 1, 1000)
	eventsLimit := parseIntQuery(r, "events_limit", 200, 1, 1000)
	includeRecentCaptures := parseIntQuery(r, "include_recent_captures", 0, 0, 100)
	recordingSettings, err := settings.GetRecordingSettings(r.Context(), s.pool)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}

	var assignment map[string]any
	var assignedServerID string
	var assignedExecutionClass string
	var assignedRevision int64
	var assignedAt *time.Time
	var assignmentUpdatedAt *time.Time
	if err := s.pool.QueryRow(r.Context(), `
		SELECT server_id, execution_class, assignment_revision, assigned_at, updated_at
		FROM recording_assignments
		WHERE stream_id=$1
	`, id).Scan(&assignedServerID, &assignedExecutionClass, &assignedRevision, &assignedAt, &assignmentUpdatedAt); err == nil {
		assignment = map[string]any{
			"server_id":           assignedServerID,
			"execution_class":     assignedExecutionClass,
			"assignment_revision": assignedRevision,
			"assigned_at":         assignedAt,
			"updated_at":          assignmentUpdatedAt,
		}
	} else if !errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("query stream assignment: %v", err))
		return
	}

	var runtimeStatus, runtimeMode, resolvedURL, runtimeErr *string
	var runtimeLastResolved, runtimeLastFrame *time.Time
	var runtimeConsecutiveErrors *int
	if err := s.pool.QueryRow(r.Context(), `
		SELECT status, execution_class, resolved_url, last_resolved_at, last_frame_at, consecutive_errors, last_error_text
		FROM stream_capture_runtime
		WHERE stream_id=$1
	`, id).Scan(
		&runtimeStatus, &runtimeMode, &resolvedURL, &runtimeLastResolved, &runtimeLastFrame, &runtimeConsecutiveErrors, &runtimeErr,
	); err != nil && !errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("query stream recording runtime: %v", err))
		return
	}
	runtime := map[string]any{
		"status":             runtimeStatus,
		"execution_class":    runtimeMode,
		"resolved_url":       resolvedURL,
		"last_resolved_at":   runtimeLastResolved,
		"last_frame_at":      runtimeLastFrame,
		"last_capture_at":    runtimeLastFrame,
		"consecutive_errors": runtimeConsecutiveErrors,
		"last_error_text":    runtimeErr,
	}

	captureWorkers, err := s.queryDashboardStreamCaptureWorkers(r.Context(), id)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	for _, item := range captureWorkers {
		if item == nil {
			continue
		}
		if _, exists := item["last_capture_at"]; !exists {
			item["last_capture_at"] = item["last_frame_at"]
		}
	}
	var activeCaptureWorker map[string]any
	for _, item := range captureWorkers {
		active, _ := item["active"].(bool)
		if !active {
			continue
		}
		serverID, _ := item["server_id"].(string)
		if assignedServerID != "" && serverID != "" && serverID != assignedServerID {
			continue
		}
		activeCaptureWorker = item
		break
	}
	if activeCaptureWorker == nil {
		for _, item := range captureWorkers {
			active, _ := item["active"].(bool)
			if active {
				activeCaptureWorker = item
				break
			}
		}
	}

	processRuns := make([]map[string]any, 0, runsLimit)
	runRows, err := s.pool.Query(r.Context(), `
		SELECT
			id, execution_class, server_id, process_id, worker_id, status, start_reason, stop_reason,
			started_at, stopped_at, last_heartbeat_at, last_frame_at, restart_count, last_error_text, created_at, updated_at
		FROM recording_process_runs
		WHERE stream_id=$1
		ORDER BY started_at DESC, id DESC
		LIMIT $2
	`, id, runsLimit)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("query stream recording process runs: %v", err))
		return
	}
	defer runRows.Close()
	for runRows.Next() {
		var runID int64
		var executionClass, serverID, processID, workerID string
		var status, startReason, stopReason string
		var startedAt, createdAt, updatedAt time.Time
		var stoppedAt, lastHeartbeatAt, lastFrameAt *time.Time
		var restartCount int
		var lastErrorText *string
		if err := runRows.Scan(
			&runID, &executionClass, &serverID, &processID, &workerID, &status, &startReason, &stopReason,
			&startedAt, &stoppedAt, &lastHeartbeatAt, &lastFrameAt, &restartCount, &lastErrorText, &createdAt, &updatedAt,
		); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan stream recording process run: %v", err))
			return
		}
		processRuns = append(processRuns, map[string]any{
			"id":                runID,
			"execution_class":   executionClass,
			"server_id":         serverID,
			"process_id":        processID,
			"worker_id":         workerID,
			"status":            status,
			"start_reason":      startReason,
			"stop_reason":       stopReason,
			"started_at":        startedAt,
			"stopped_at":        stoppedAt,
			"last_heartbeat_at": lastHeartbeatAt,
			"last_frame_at":     lastFrameAt,
			"last_capture_at":   lastFrameAt,
			"restart_count":     restartCount,
			"last_error_text":   lastErrorText,
			"created_at":        createdAt,
			"updated_at":        updatedAt,
		})
	}
	if runRows.Err() != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("iterate stream recording process runs: %v", runRows.Err()))
		return
	}

	stateEvents := make([]map[string]any, 0, eventsLimit)
	eventRows, err := s.pool.Query(r.Context(), `
		SELECT id, prev_state::text, next_state::text, actor, reason, metadata_jsonb, created_at
		FROM recording_state_events
		WHERE stream_id=$1
		ORDER BY created_at DESC, id DESC
		LIMIT $2
	`, id, eventsLimit)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("query stream recording state events: %v", err))
		return
	}
	defer eventRows.Close()
	for eventRows.Next() {
		var eventID int64
		var prevState *string
		var nextState, actor, reason string
		var metadataBytes []byte
		var createdAt time.Time
		if err := eventRows.Scan(&eventID, &prevState, &nextState, &actor, &reason, &metadataBytes, &createdAt); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan stream recording state event: %v", err))
			return
		}
		metadata := map[string]any{}
		if len(metadataBytes) > 0 {
			if err := json.Unmarshal(metadataBytes, &metadata); err != nil {
				util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("decode stream recording state event metadata: %v", err))
				return
			}
		}
		stateEvents = append(stateEvents, map[string]any{
			"id":         eventID,
			"prev_state": prevState,
			"next_state": nextState,
			"actor":      actor,
			"reason":     reason,
			"metadata":   metadata,
			"created_at": createdAt,
		})
	}
	if eventRows.Err() != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("iterate stream recording state events: %v", eventRows.Err()))
		return
	}

	captureMode := firstNonEmpty(string(stream.ExecutionClass), derefString(runtimeMode), assignedExecutionClass)
	clipNative := isClipNativeExecutionClass(captureMode)
	var recentCaptures any = []any{}
	if includeRecentCaptures > 0 {
		if clipNative {
			items, err := s.queryCaptureSegments(r.Context(), captureSegmentQueryOptions{
				StreamID:                    id,
				Limit:                       includeRecentCaptures,
				Offset:                      0,
				IncludeDownloadURL:          true,
				IncludeThumbnailDownloadURL: true,
			})
			if err != nil {
				util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("query recent stream clips: %v", err))
				return
			}
			recentCaptures = items
		} else {
			items, err := s.queryRecentFrameItems(r.Context(), id, includeRecentCaptures)
			if err != nil {
				util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("query recent stream frames: %v", err))
				return
			}
			recentCaptures = items
		}
	}
	var successFrames24h, errorFrames24h int64
	var firstCapture24h, lastCapture24h *time.Time
	if clipNative {
		if err := s.pool.QueryRow(r.Context(), `
			SELECT
				COUNT(*) FILTER (WHERE capture_status='success')::bigint,
				COUNT(*) FILTER (WHERE capture_status='error')::bigint,
				MIN(segment_end_at),
				MAX(segment_end_at)
			FROM capture_segments
			WHERE stream_id=$1
			  AND segment_end_at >= now() - interval '24 hours'
		`, id).Scan(&successFrames24h, &errorFrames24h, &firstCapture24h, &lastCapture24h); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("query stream recording clip stats: %v", err))
			return
		}
	} else {
		if err := s.pool.QueryRow(r.Context(), `
			SELECT
				COUNT(*) FILTER (WHERE capture_status='success')::bigint,
				COUNT(*) FILTER (WHERE capture_status='error')::bigint,
				MIN(captured_at),
				MAX(captured_at)
			FROM frames
			WHERE stream_id=$1
			  AND captured_at >= now() - interval '24 hours'
		`, id).Scan(&successFrames24h, &errorFrames24h, &firstCapture24h, &lastCapture24h); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("query stream recording frame stats: %v", err))
			return
		}
	}
	intervalSec := settings.DefaultRecordingIntervalSec
	if intervalSec <= 0 {
		intervalSec = 1
	}
	expectedFrames24h := int64(24 * 60 * 60 / intervalSec)
	if clipNative {
		expectedFrames24h = int64(24 * settings.DefaultSampleExpectedPerHour)
	}
	if expectedFrames24h <= 0 {
		expectedFrames24h = 1
	}
	missingFrames24h := expectedFrames24h - successFrames24h
	if missingFrames24h < 0 {
		missingFrames24h = 0
	}
	lossRate24h := 100.0 * float64(missingFrames24h) / float64(expectedFrames24h)

	lastFrameAt := runtimeLastFrame
	if activeCaptureWorker != nil {
		if t, ok := activeCaptureWorker["last_frame_at"].(*time.Time); ok && t != nil {
			lastFrameAt = t
		}
	}
	now := time.Now().UTC()
	healthyWindowSec := int(staleThresholdSecForExecutionClass(captureMode, settings.DefaultRecordingIntervalSec))
	healthState := "off"
	healthReason := "recording_off"
	healthMessage := "recording is off"
	var lastFrameAgeSec *int64
	captureUnitSingular := strings.TrimSuffix(captureUnitLabelForExecutionClass(captureMode), "s")
	captureUnitPlural := captureUnitLabelForExecutionClass(captureMode)
	if strings.EqualFold(strings.TrimSpace(string(stream.RecordingState)), "on") {
		healthState = "degraded"
		healthReason = "unassigned"
		healthMessage = "recording is on but no active assignment exists"
		switch {
		case lastFrameAt == nil:
			healthReason = "no_recent_capture"
			healthMessage = fmt.Sprintf("recording is on but no recent %s has been observed", captureUnitSingular)
		default:
			age := int64(now.Sub((*lastFrameAt).UTC()).Seconds())
			if age < 0 {
				age = 0
			}
			lastFrameAgeSec = &age
			if age <= int64(healthyWindowSec) {
				healthState = "healthy"
				healthReason = "fresh_captures"
				healthMessage = fmt.Sprintf("fresh %s are arriving", captureUnitPlural)
			} else {
				healthReason = "stale_captures"
				healthMessage = fmt.Sprintf("the latest %s is stale", captureUnitSingular)
			}
		}
	}

	util.WriteJSON(w, http.StatusOK, map[string]any{
		"stream":                stream,
		"runtime":               runtime,
		"assignment":            assignment,
		"capture_workers":       captureWorkers,
		"active_capture_worker": activeCaptureWorker,
		"process_runs":          processRuns,
		"state_events":          stateEvents,
		"recording_config": map[string]any{
			"clip_duration_sec":       recordingSettings.ClipDurationSec,
			"sample_interval_min_sec": recordingSettings.SampleIntervalMinSec,
			"sample_interval_max_sec": recordingSettings.SampleIntervalMaxSec,
			"stale_grace_sec":         recordingSettings.StaleGraceSec,
		},
		"capture_unit":    captureUnitPlural,
		"clip_native":     clipNative,
		"recent_captures": recentCaptures,
		"current_health": map[string]any{
			"state":                healthState,
			"reason":               healthReason,
			"message":              healthMessage,
			"last_frame_at":        lastFrameAt,
			"last_frame_age_sec":   lastFrameAgeSec,
			"last_capture_at":      lastFrameAt,
			"last_capture_age_sec": lastFrameAgeSec,
		},
		"stats_24h": map[string]any{
			"expected_frames":   expectedFrames24h,
			"success_frames":    successFrames24h,
			"error_frames":      errorFrames24h,
			"missing_frames":    missingFrames24h,
			"expected_captures": expectedFrames24h,
			"success_captures":  successFrames24h,
			"error_captures":    errorFrames24h,
			"missing_captures":  missingFrames24h,
			"loss_rate_pct":     lossRate24h,
			"first_capture":     firstCapture24h,
			"last_capture":      lastCapture24h,
		},
		"limits": map[string]any{
			"runs_limit":   runsLimit,
			"events_limit": eventsLimit,
		},
	})
}

type dashboardStreamTimelinePoint struct {
	Minute                int   `json:"minute"`
	RecordedTotalFrames   int64 `json:"recorded_total_frames"`
	RecordedSuccessFrames int64 `json:"recorded_success_frames"`
	RecordedErrorFrames   int64 `json:"recorded_error_frames"`
	InferencedFrames      int64 `json:"inferenced_frames"`
	PersonDetections      int64 `json:"person_detections"`
}

type dashboardStreamTimelineTotals struct {
	RecordedMinutes        int   `json:"recorded_minutes"`
	RecordedSuccessMinutes int   `json:"recorded_success_minutes"`
	RecordedErrorMinutes   int   `json:"recorded_error_minutes"`
	InferencedMinutes      int   `json:"inferenced_minutes"`
	PersonMinutes          int   `json:"person_minutes"`
	RecordedTotalFrames    int64 `json:"recorded_total_frames"`
	RecordedSuccessFrames  int64 `json:"recorded_success_frames"`
	RecordedErrorFrames    int64 `json:"recorded_error_frames"`
	InferencedFrames       int64 `json:"inferenced_frames"`
	PersonDetections       int64 `json:"person_detections"`
	MaxPeoplePerMinute     int64 `json:"max_people_per_minute"`
}

type dashboardStreamCoveragePoint struct {
	Day                     string     `json:"day"`
	RecordedMinutes         int        `json:"recorded_minutes"`
	RecordedHours           float64    `json:"recorded_hours"`
	SuccessMinutes          int        `json:"success_minutes"`
	ErrorMinutes            int        `json:"error_minutes"`
	RecordedTotalCaptures   int64      `json:"recorded_total_captures"`
	RecordedSuccessCaptures int64      `json:"recorded_success_captures"`
	RecordedErrorCaptures   int64      `json:"recorded_error_captures"`
	RecordedTotalFrames     int64      `json:"recorded_total_frames"`
	RecordedSuccess         int64      `json:"recorded_success_frames"`
	RecordedError           int64      `json:"recorded_error_frames"`
	CoveragePctOfDay        float64    `json:"coverage_pct_of_day"`
	FirstCaptureAt          *time.Time `json:"first_capture_at,omitempty"`
	LastCaptureAt           *time.Time `json:"last_capture_at,omitempty"`
}

type dashboardStreamCoverageSummary struct {
	DaysTotal           int        `json:"days_total"`
	DaysWithCapture     int        `json:"days_with_capture"`
	DaysWithoutCapture  int        `json:"days_without_capture"`
	TotalRecordedHours  float64    `json:"total_recorded_hours"`
	TotalSuccessHours   float64    `json:"total_success_hours"`
	TotalErrorHours     float64    `json:"total_error_hours"`
	AvgRecordedHoursDay float64    `json:"avg_recorded_hours_per_day"`
	MaxRecordedHoursDay float64    `json:"max_recorded_hours_per_day"`
	CurrentStreakDays   int        `json:"current_streak_days"`
	LongestGapDays      int        `json:"longest_gap_days"`
	FirstCaptureAt      *time.Time `json:"first_capture_at,omitempty"`
	LastCaptureAt       *time.Time `json:"last_capture_at,omitempty"`
}

type dashboardStreamCaptureSample struct {
	Day                  string    `json:"day"`
	SegmentID            int64     `json:"segment_id"`
	FrameID              int64     `json:"frame_id,omitempty"`
	CapturedAt           time.Time `json:"captured_at"`
	SegmentStartAt       time.Time `json:"segment_start_at"`
	SegmentEndAt         time.Time `json:"segment_end_at"`
	ObjectKey            string    `json:"object_key"`
	ArchiveStatus        string    `json:"archive_status"`
	DownloadURL          string    `json:"download_url,omitempty"`
	ThumbnailObjectKey   *string   `json:"thumbnail_object_key,omitempty"`
	ThumbnailDownloadURL string    `json:"thumbnail_download_url,omitempty"`
}

func ensureTimelinePoint(pointsByMinute map[int]*dashboardStreamTimelinePoint, minute int) *dashboardStreamTimelinePoint {
	if pt, ok := pointsByMinute[minute]; ok {
		return pt
	}
	pt := &dashboardStreamTimelinePoint{Minute: minute}
	pointsByMinute[minute] = pt
	return pt
}

func sampleEvenly(values []string, count int) []string {
	n := len(values)
	if n == 0 || count <= 0 {
		return nil
	}
	if count >= n {
		out := make([]string, 0, n)
		out = append(out, values...)
		return out
	}
	if count == 1 {
		return []string{values[n/2]}
	}
	out := make([]string, 0, count)
	seen := make(map[string]struct{}, count)
	for i := 0; i < count; i++ {
		// Rounded evenly spaced pick in [0,n-1].
		idx := int((int64(i)*(int64(n)-1) + int64(count-1)/2) / int64(count-1))
		if idx < 0 {
			idx = 0
		}
		if idx >= n {
			idx = n - 1
		}
		v := values[idx]
		if _, exists := seen[v]; exists {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	if len(out) == count {
		return out
	}
	for _, v := range values {
		if len(out) >= count {
			break
		}
		if _, exists := seen[v]; exists {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out
}

func (s *Server) handleDashboardStreamCoverage(w http.ResponseWriter, r *http.Request) {
	id, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	if _, err := s.getStreamByID(r.Context(), id); err != nil {
		util.WriteError(w, http.StatusNotFound, err.Error())
		return
	}

	days := parseIntQuery(r, "days", 30, 14, 1095)
	nowUTC := time.Now().UTC()
	endDay := time.Date(nowUTC.Year(), nowUTC.Month(), nowUTC.Day(), 0, 0, 0, 0, time.UTC)
	startDay := endDay.AddDate(0, 0, -(days - 1))
	windowStart := startDay
	windowEnd := endDay.AddDate(0, 0, 1)

	rows, err := s.pool.Query(r.Context(), `
		WITH day_series AS (
			SELECT
				gs.day::date AS day,
				(gs.day::date::timestamp AT TIME ZONE 'UTC') AS day_start,
				(((gs.day::date + 1)::timestamp) AT TIME ZONE 'UTC') AS day_end
			FROM generate_series($2::date, ($3::date - interval '1 day')::date, interval '1 day') AS gs(day)
		),
		relevant_segments AS (
			SELECT
				ds.day,
				cs.id,
				cs.capture_status,
				GREATEST(cs.segment_start_at, ds.day_start) AS overlap_start,
				LEAST(cs.segment_end_at, ds.day_end) AS overlap_end
			FROM day_series ds
			JOIN capture_segments cs
			  ON cs.stream_id=$1
			 AND cs.segment_end_at > ds.day_start
			 AND cs.segment_start_at < ds.day_end
		),
		segment_stats AS (
			SELECT
				day,
				COUNT(DISTINCT id)::bigint AS recorded_total_captures,
				COUNT(DISTINCT id) FILTER (WHERE capture_status='success')::bigint AS recorded_success_captures,
				COUNT(DISTINCT id) FILTER (WHERE capture_status='error')::bigint AS recorded_error_captures,
				MIN(overlap_start) AS first_capture_at,
				MAX(overlap_end) AS last_capture_at
			FROM relevant_segments
			GROUP BY 1
		),
		minute_stats AS (
			SELECT
				day,
				COUNT(DISTINCT minute_bucket)::int AS recorded_minutes,
				COUNT(DISTINCT minute_bucket) FILTER (WHERE capture_status='success')::int AS success_minutes,
				COUNT(DISTINCT minute_bucket) FILTER (WHERE capture_status='error')::int AS error_minutes
			FROM (
				SELECT
					rs.day,
					rs.capture_status,
					gs.minute_bucket
				FROM relevant_segments rs
				JOIN LATERAL generate_series(
					date_trunc('minute', rs.overlap_start),
					date_trunc('minute', rs.overlap_end - interval '1 microsecond'),
					interval '1 minute'
				) AS gs(minute_bucket) ON rs.overlap_end > rs.overlap_start
			) buckets
			GROUP BY 1
		)
		SELECT
			ds.day,
			COALESCE(ms.recorded_minutes, 0)::int,
			COALESCE(ms.success_minutes, 0)::int,
			COALESCE(ms.error_minutes, 0)::int,
			COALESCE(ss.recorded_total_captures, 0)::bigint,
			COALESCE(ss.recorded_success_captures, 0)::bigint,
			COALESCE(ss.recorded_error_captures, 0)::bigint,
			ss.first_capture_at,
			ss.last_capture_at
		FROM day_series ds
		LEFT JOIN minute_stats ms ON ms.day=ds.day
		LEFT JOIN segment_stats ss ON ss.day=ds.day
		ORDER BY ds.day ASC
	`, id, windowStart, windowEnd)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("query stream coverage: %v", err))
		return
	}
	defer rows.Close()

	points := make([]dashboardStreamCoveragePoint, 0, days)
	summary := dashboardStreamCoverageSummary{
		DaysTotal: days,
	}
	currentStreak := 0
	longestGap := 0
	activeGap := 0
	maxMinutes := 0
	var firstCaptureAt time.Time
	var lastCaptureAt time.Time
	hasFirstCapture := false
	hasLastCapture := false

	for rows.Next() {
		var day time.Time
		var p dashboardStreamCoveragePoint
		if err := rows.Scan(
			&day,
			&p.RecordedMinutes,
			&p.SuccessMinutes,
			&p.ErrorMinutes,
			&p.RecordedTotalCaptures,
			&p.RecordedSuccessCaptures,
			&p.RecordedErrorCaptures,
			&p.FirstCaptureAt,
			&p.LastCaptureAt,
		); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan stream coverage row: %v", err))
			return
		}
		p.Day = day.UTC().Format("2006-01-02")
		p.RecordedHours = float64(p.RecordedMinutes) / 60.0
		p.CoveragePctOfDay = 100.0 * float64(p.RecordedMinutes) / 1440.0
		p.RecordedTotalFrames = p.RecordedTotalCaptures
		p.RecordedSuccess = p.RecordedSuccessCaptures
		p.RecordedError = p.RecordedErrorCaptures
		points = append(points, p)

		if p.RecordedMinutes > 0 {
			summary.DaysWithCapture++
			summary.TotalRecordedHours += p.RecordedHours
			summary.TotalSuccessHours += float64(p.SuccessMinutes) / 60.0
			summary.TotalErrorHours += float64(p.ErrorMinutes) / 60.0
			if p.RecordedMinutes > maxMinutes {
				maxMinutes = p.RecordedMinutes
			}
			activeGap = 0
			if p.FirstCaptureAt != nil {
				t := p.FirstCaptureAt.UTC()
				if !hasFirstCapture || t.Before(firstCaptureAt) {
					firstCaptureAt = t
					hasFirstCapture = true
				}
			}
			if p.LastCaptureAt != nil {
				t := p.LastCaptureAt.UTC()
				if !hasLastCapture || t.After(lastCaptureAt) {
					lastCaptureAt = t
					hasLastCapture = true
				}
			}
		} else {
			activeGap++
			if activeGap > longestGap {
				longestGap = activeGap
			}
		}
	}
	if rows.Err() != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("iterate stream coverage rows: %v", rows.Err()))
		return
	}

	for i := len(points) - 1; i >= 0; i-- {
		if points[i].RecordedMinutes <= 0 {
			break
		}
		currentStreak++
	}

	summary.DaysWithoutCapture = summary.DaysTotal - summary.DaysWithCapture
	if summary.DaysTotal > 0 {
		summary.AvgRecordedHoursDay = summary.TotalRecordedHours / float64(summary.DaysTotal)
	}
	summary.MaxRecordedHoursDay = float64(maxMinutes) / 60.0
	summary.CurrentStreakDays = currentStreak
	summary.LongestGapDays = longestGap
	if hasFirstCapture {
		summary.FirstCaptureAt = &firstCaptureAt
	}
	if hasLastCapture {
		summary.LastCaptureAt = &lastCaptureAt
	}

	util.WriteJSON(w, http.StatusOK, map[string]any{
		"stream_id":    id,
		"timezone":     "UTC",
		"days":         days,
		"start_day":    startDay.Format("2006-01-02"),
		"end_day":      endDay.Format("2006-01-02"),
		"window_start": windowStart,
		"window_end":   windowEnd,
		"points":       points,
		"summary":      summary,
	})
}

func (s *Server) handleDashboardStreamCaptureSamples(w http.ResponseWriter, r *http.Request) {
	id, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	if _, err := s.getStreamByID(r.Context(), id); err != nil {
		util.WriteError(w, http.StatusNotFound, err.Error())
		return
	}

	count := parseIntQuery(r, "count", 42, 1, 180)

	dayRows, err := s.pool.Query(r.Context(), `
		SELECT DISTINCT (cs.segment_end_at AT TIME ZONE 'UTC')::date AS day
		FROM capture_segments cs
		JOIN media_objects mo ON mo.id=cs.media_object_id
		WHERE cs.stream_id=$1
		  AND cs.capture_status='success'
		  AND mo.object_key IS NOT NULL
		ORDER BY day ASC
	`, id)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("query stream sample days: %v", err))
		return
	}
	defer dayRows.Close()

	availableDays := make([]string, 0, 2048)
	for dayRows.Next() {
		var day time.Time
		if err := dayRows.Scan(&day); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan stream sample day: %v", err))
			return
		}
		availableDays = append(availableDays, day.UTC().Format("2006-01-02"))
	}
	if dayRows.Err() != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("iterate stream sample days: %v", dayRows.Err()))
		return
	}

	selectedDays := sampleEvenly(availableDays, count)
	if len(selectedDays) == 0 {
		util.WriteJSON(w, http.StatusOK, map[string]any{
			"stream_id":       id,
			"requested_count": count,
			"available_days":  0,
			"selected_days":   0,
			"items":           []dashboardStreamCaptureSample{},
		})
		return
	}

	rows, err := s.pool.Query(r.Context(), `
		WITH selected_days AS (
			SELECT DISTINCT day_txt::date AS day
			FROM unnest($2::text[]) AS t(day_txt)
		),
		day_bounds AS (
			SELECT
				day,
				(day::timestamp AT TIME ZONE 'UTC') AS day_start,
				((day + 1)::timestamp AT TIME ZONE 'UTC') AS day_end
			FROM selected_days
		),
		picked AS (
			SELECT
				db.day,
				seg.id AS segment_id,
				seg.segment_start_at,
				seg.segment_end_at,
				seg.segment_end_at AS captured_at,
				mo.object_key,
				COALESCE(mo.archive_status, 'none') AS archive_status,
				thumb.object_key AS thumbnail_object_key
			FROM day_bounds db
			JOIN LATERAL (
				SELECT cs.id, cs.segment_start_at, cs.segment_end_at, cs.media_object_id, cs.thumbnail_media_object_id
				FROM capture_segments cs
				WHERE cs.stream_id=$1
				  AND cs.capture_status='success'
				  AND cs.media_object_id IS NOT NULL
				  AND cs.segment_end_at >= db.day_start
				  AND cs.segment_end_at < db.day_end
				ORDER BY cs.segment_end_at DESC, cs.id DESC
				LIMIT 1
			) seg ON true
			JOIN media_objects mo ON mo.id=seg.media_object_id
			LEFT JOIN media_objects thumb ON thumb.id=seg.thumbnail_media_object_id
		)
		SELECT day, segment_id, segment_start_at, segment_end_at, captured_at, object_key, archive_status, thumbnail_object_key
		FROM picked
		ORDER BY day ASC
	`, id, selectedDays)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("query stream capture samples: %v", err))
		return
	}
	defer rows.Close()

	items := make([]dashboardStreamCaptureSample, 0, len(selectedDays))
	for rows.Next() {
		var day time.Time
		var sample dashboardStreamCaptureSample
		if err := rows.Scan(&day, &sample.SegmentID, &sample.SegmentStartAt, &sample.SegmentEndAt, &sample.CapturedAt, &sample.ObjectKey, &sample.ArchiveStatus, &sample.ThumbnailObjectKey); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan stream capture sample: %v", err))
			return
		}
		sample.Day = day.UTC().Format("2006-01-02")
		if sample.ObjectKey != "" && !model.IsSourceDeletedArchiveStatus(sample.ArchiveStatus) {
			if url, err := s.r2.PresignGet(r.Context(), sample.ObjectKey, s.cfg.R2SignGetTTL); err == nil {
				sample.DownloadURL = url
			}
		}
		if sample.ThumbnailObjectKey != nil && strings.TrimSpace(*sample.ThumbnailObjectKey) != "" {
			if url, err := s.r2.PresignGet(r.Context(), strings.TrimSpace(*sample.ThumbnailObjectKey), s.cfg.R2SignGetTTL); err == nil {
				sample.ThumbnailDownloadURL = url
			}
		}
		items = append(items, sample)
	}
	if rows.Err() != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("iterate stream capture samples: %v", rows.Err()))
		return
	}

	util.WriteJSON(w, http.StatusOK, map[string]any{
		"stream_id":       id,
		"requested_count": count,
		"available_days":  len(availableDays),
		"selected_days":   len(selectedDays),
		"items":           items,
	})
}

func (s *Server) handleDashboardStreamTimeline(w http.ResponseWriter, r *http.Request) {
	id, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	if _, err := s.getStreamByID(r.Context(), id); err != nil {
		util.WriteError(w, http.StatusNotFound, err.Error())
		return
	}

	dayRaw := strings.TrimSpace(r.URL.Query().Get("day"))
	dayStart := time.Now().UTC()
	dayStart = time.Date(dayStart.Year(), dayStart.Month(), dayStart.Day(), 0, 0, 0, 0, time.UTC)
	if dayRaw != "" {
		parsed, err := time.Parse("2006-01-02", dayRaw)
		if err != nil {
			util.WriteError(w, http.StatusBadRequest, "invalid day; expected YYYY-MM-DD")
			return
		}
		dayStart = parsed.UTC()
	}
	dayEnd := dayStart.Add(24 * time.Hour)
	pipelineID := strings.TrimSpace(r.URL.Query().Get("pipeline_id"))

	pipelineRows, err := s.pool.Query(r.Context(), `
		SELECT DISTINCT ir.pipeline_id
		FROM inference_results ir
		JOIN frames f ON f.id=ir.frame_id
		WHERE f.stream_id=$1
		ORDER BY ir.pipeline_id ASC
	`, id)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("query stream pipelines: %v", err))
		return
	}
	defer pipelineRows.Close()

	pipelineIDs := make([]string, 0, 16)
	pipelineSet := make(map[string]struct{}, 16)
	for pipelineRows.Next() {
		var p string
		if err := pipelineRows.Scan(&p); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan stream pipeline: %v", err))
			return
		}
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if _, exists := pipelineSet[p]; exists {
			continue
		}
		pipelineSet[p] = struct{}{}
		pipelineIDs = append(pipelineIDs, p)
	}
	if pipelineRows.Err() != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("iterate stream pipelines: %v", pipelineRows.Err()))
		return
	}
	if pipelineID != "" {
		if _, exists := pipelineSet[pipelineID]; !exists {
			util.WriteError(w, http.StatusBadRequest, fmt.Sprintf("invalid pipeline_id for stream %d", id))
			return
		}
	}

	pointsByMinute := make(map[int]*dashboardStreamTimelinePoint, 1440)
	recordedRows, err := s.pool.Query(r.Context(), `
		SELECT
			(EXTRACT(HOUR FROM f.captured_at AT TIME ZONE 'UTC')::int * 60 +
			 EXTRACT(MINUTE FROM f.captured_at AT TIME ZONE 'UTC')::int) AS minute_idx,
			COUNT(*)::bigint AS recorded_total_frames,
			COUNT(*) FILTER (WHERE f.capture_status='success')::bigint AS recorded_success_frames,
			COUNT(*) FILTER (WHERE f.capture_status='error')::bigint AS recorded_error_frames
		FROM frames f
		WHERE f.stream_id=$1
		  AND f.captured_at >= $2
		  AND f.captured_at < $3
		GROUP BY minute_idx
		ORDER BY minute_idx ASC
	`, id, dayStart, dayEnd)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("query timeline recorded buckets: %v", err))
		return
	}
	defer recordedRows.Close()
	for recordedRows.Next() {
		var minute int
		var total, success, captureErr int64
		if err := recordedRows.Scan(&minute, &total, &success, &captureErr); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan timeline recorded bucket: %v", err))
			return
		}
		if minute < 0 || minute >= 1440 {
			continue
		}
		pt := ensureTimelinePoint(pointsByMinute, minute)
		pt.RecordedTotalFrames = total
		pt.RecordedSuccessFrames = success
		pt.RecordedErrorFrames = captureErr
	}
	if recordedRows.Err() != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("iterate timeline recorded buckets: %v", recordedRows.Err()))
		return
	}

	inferenceRows, err := s.pool.Query(r.Context(), `
		WITH frame_window AS (
			SELECT f.id, f.captured_at
			FROM frames f
			WHERE f.stream_id=$1
			  AND f.captured_at >= $2
			  AND f.captured_at < $3
		),
		selected_results AS (
			SELECT DISTINCT ON (ir.frame_id)
				ir.id AS inference_result_id,
				fw.captured_at
			FROM inference_results ir
			JOIN frame_window fw ON fw.id=ir.frame_id
			WHERE ($4::text = '' OR ir.pipeline_id=$4)
			ORDER BY ir.frame_id, ir.revision DESC, ir.created_at DESC, ir.id DESC
		)
		SELECT
			(EXTRACT(HOUR FROM sr.captured_at AT TIME ZONE 'UTC')::int * 60 +
			 EXTRACT(MINUTE FROM sr.captured_at AT TIME ZONE 'UTC')::int) AS minute_idx,
			COUNT(*)::bigint AS inferenced_frames,
			COALESCE(SUM(CASE WHEN LOWER(d.class_name)='person' THEN 1 ELSE 0 END), 0)::bigint AS person_detections
		FROM selected_results sr
		LEFT JOIN detections d ON d.inference_result_id=sr.inference_result_id
		GROUP BY minute_idx
		ORDER BY minute_idx ASC
	`, id, dayStart, dayEnd, pipelineID)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("query timeline inference buckets: %v", err))
		return
	}
	defer inferenceRows.Close()
	for inferenceRows.Next() {
		var minute int
		var inferencedFrames, personDetections int64
		if err := inferenceRows.Scan(&minute, &inferencedFrames, &personDetections); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan timeline inference bucket: %v", err))
			return
		}
		if minute < 0 || minute >= 1440 {
			continue
		}
		pt := ensureTimelinePoint(pointsByMinute, minute)
		pt.InferencedFrames = inferencedFrames
		pt.PersonDetections = personDetections
	}
	if inferenceRows.Err() != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("iterate timeline inference buckets: %v", inferenceRows.Err()))
		return
	}

	minutes := make([]int, 0, len(pointsByMinute))
	for minute := range pointsByMinute {
		minutes = append(minutes, minute)
	}
	sort.Ints(minutes)

	points := make([]dashboardStreamTimelinePoint, 0, len(minutes))
	totals := dashboardStreamTimelineTotals{}
	for _, minute := range minutes {
		pt := pointsByMinute[minute]
		points = append(points, *pt)
		if pt.RecordedTotalFrames > 0 {
			totals.RecordedMinutes++
		}
		if pt.RecordedSuccessFrames > 0 {
			totals.RecordedSuccessMinutes++
		}
		if pt.RecordedErrorFrames > 0 {
			totals.RecordedErrorMinutes++
		}
		if pt.InferencedFrames > 0 {
			totals.InferencedMinutes++
		}
		if pt.PersonDetections > 0 {
			totals.PersonMinutes++
		}
		totals.RecordedTotalFrames += pt.RecordedTotalFrames
		totals.RecordedSuccessFrames += pt.RecordedSuccessFrames
		totals.RecordedErrorFrames += pt.RecordedErrorFrames
		totals.InferencedFrames += pt.InferencedFrames
		totals.PersonDetections += pt.PersonDetections
		if pt.PersonDetections > totals.MaxPeoplePerMinute {
			totals.MaxPeoplePerMinute = pt.PersonDetections
		}
	}

	util.WriteJSON(w, http.StatusOK, map[string]any{
		"stream_id":              id,
		"day":                    dayStart.Format("2006-01-02"),
		"day_start":              dayStart,
		"day_end":                dayEnd,
		"timezone":               "UTC",
		"selected_pipeline_id":   pipelineID,
		"available_pipeline_ids": pipelineIDs,
		"points":                 points,
		"totals":                 totals,
	})
}

func (s *Server) handleDashboardStreamDetections(w http.ResponseWriter, r *http.Request) {
	id, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	pipelineID := strings.TrimSpace(r.URL.Query().Get("pipeline_id"))
	status := strings.TrimSpace(strings.ToLower(r.URL.Query().Get("status")))
	className := strings.TrimSpace(r.URL.Query().Get("class_name"))
	search := strings.TrimSpace(r.URL.Query().Get("q"))
	minConfidence := parseFloat64QueryPtr(r, "min_confidence")
	createdFrom := parseTimeQueryPtr(r, "created_from")
	createdTo := parseTimeQueryPtr(r, "created_to")
	capturedFrom := parseTimeQueryPtr(r, "captured_from")
	capturedTo := parseTimeQueryPtr(r, "captured_to")
	hasBoxed := parseBoolQueryPtr(r, "has_boxed")
	limit := parseIntQuery(r, "limit", 200, 1, 2000)

	if status != "" && !isInferenceResultStatus(status) {
		util.WriteError(w, http.StatusBadRequest, "invalid status; expected queued_boxed|success|error")
		return
	}
	if raw := strings.TrimSpace(r.URL.Query().Get("min_confidence")); raw != "" && minConfidence == nil {
		util.WriteError(w, http.StatusBadRequest, "invalid min_confidence")
		return
	}
	if raw := strings.TrimSpace(r.URL.Query().Get("created_from")); raw != "" && createdFrom == nil {
		util.WriteError(w, http.StatusBadRequest, "invalid created_from; expected RFC3339 or YYYY-MM-DD")
		return
	}
	if raw := strings.TrimSpace(r.URL.Query().Get("created_to")); raw != "" && createdTo == nil {
		util.WriteError(w, http.StatusBadRequest, "invalid created_to; expected RFC3339 or YYYY-MM-DD")
		return
	}
	if raw := strings.TrimSpace(r.URL.Query().Get("captured_from")); raw != "" && capturedFrom == nil {
		util.WriteError(w, http.StatusBadRequest, "invalid captured_from; expected RFC3339 or YYYY-MM-DD")
		return
	}
	if raw := strings.TrimSpace(r.URL.Query().Get("captured_to")); raw != "" && capturedTo == nil {
		util.WriteError(w, http.StatusBadRequest, "invalid captured_to; expected RFC3339 or YYYY-MM-DD")
		return
	}

	where := []string{"f.stream_id=$1"}
	args := []any{id}
	if pipelineID != "" {
		args = append(args, pipelineID)
		where = append(where, fmt.Sprintf("ir.pipeline_id=$%d", len(args)))
	}
	if status != "" {
		args = append(args, status)
		where = append(where, fmt.Sprintf("ir.status=$%d", len(args)))
	}
	if hasBoxed != nil {
		if *hasBoxed {
			where = append(where, "ir.boxed_media_object_id IS NOT NULL")
		} else {
			where = append(where, "ir.boxed_media_object_id IS NULL")
		}
	}
	if search != "" {
		args = append(args, "%"+search+"%")
		where = append(where, fmt.Sprintf(`(
			ir.pipeline_id ILIKE $%d OR
			COALESCE(ir.error_text, '') ILIKE $%d OR
			COALESCE(ir.summary_jsonb::text, '') ILIKE $%d
		)`, len(args), len(args), len(args)))
	}
	if createdFrom != nil {
		args = append(args, *createdFrom)
		where = append(where, fmt.Sprintf("ir.created_at >= $%d", len(args)))
	}
	if createdTo != nil {
		args = append(args, *createdTo)
		where = append(where, fmt.Sprintf("ir.created_at <= $%d", len(args)))
	}
	if capturedFrom != nil {
		args = append(args, *capturedFrom)
		where = append(where, fmt.Sprintf("f.captured_at >= $%d", len(args)))
	}
	if capturedTo != nil {
		args = append(args, *capturedTo)
		where = append(where, fmt.Sprintf("f.captured_at <= $%d", len(args)))
	}
	if className != "" || minConfidence != nil {
		detectionWhere := []string{"d.inference_result_id=ir.id"}
		if className != "" {
			args = append(args, className)
			detectionWhere = append(detectionWhere, fmt.Sprintf("d.class_name ILIKE $%d", len(args)))
		}
		if minConfidence != nil {
			args = append(args, *minConfidence)
			detectionWhere = append(detectionWhere, fmt.Sprintf("d.confidence >= $%d", len(args)))
		}
		where = append(where, fmt.Sprintf("EXISTS (SELECT 1 FROM detections d WHERE %s)", strings.Join(detectionWhere, " AND ")))
	}

	query := fmt.Sprintf(`
		SELECT ir.id, ir.pipeline_id, ir.revision, ir.status, ir.created_at, ir.finished_at, boxed.object_key, raw.object_key
		FROM inference_results ir
		JOIN frames f ON f.id=ir.frame_id
		LEFT JOIN media_objects boxed ON boxed.id=ir.boxed_media_object_id
		LEFT JOIN media_objects raw ON raw.id=f.raw_media_object_id
		WHERE %s
		ORDER BY ir.created_at DESC, ir.id DESC
		LIMIT 1
	`, strings.Join(where, " AND "))

	var resultID int64
	var resultPipeline string
	var revision int
	var resultStatus string
	var createdAt time.Time
	var finishedAt *time.Time
	var boxedObjectKey *string
	var rawObjectKey *string
	err := s.pool.QueryRow(r.Context(), query, args...).Scan(&resultID, &resultPipeline, &revision, &resultStatus, &createdAt, &finishedAt, &boxedObjectKey, &rawObjectKey)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteJSON(w, http.StatusOK, map[string]any{
			"latest_result": nil,
			"detections":    []any{},
			"limit":         limit,
		})
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("query latest inference result: %v", err))
		return
	}

	type detectionItem struct {
		ClassID    *string        `json:"class_id,omitempty"`
		ClassName  string         `json:"class_name"`
		Confidence float64        `json:"confidence"`
		X1         float64        `json:"x1"`
		Y1         float64        `json:"y1"`
		X2         float64        `json:"x2"`
		Y2         float64        `json:"y2"`
		AreaPx     float64        `json:"area_px"`
		Extra      map[string]any `json:"extra,omitempty"`
	}
	rows, err := s.pool.Query(r.Context(), `
		SELECT class_id, class_name, confidence, x1, y1, x2, y2, area_px, extra_jsonb
		FROM detections
		WHERE inference_result_id=$1
		ORDER BY confidence DESC, id ASC
		LIMIT $2
	`, resultID, limit)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("query detections: %v", err))
		return
	}
	defer rows.Close()
	detections := make([]detectionItem, 0, limit)
	for rows.Next() {
		var d detectionItem
		var extraBytes []byte
		if err := rows.Scan(&d.ClassID, &d.ClassName, &d.Confidence, &d.X1, &d.Y1, &d.X2, &d.Y2, &d.AreaPx, &extraBytes); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan detection: %v", err))
			return
		}
		if len(extraBytes) > 0 {
			var m map[string]any
			if err := json.Unmarshal(extraBytes, &m); err == nil {
				d.Extra = m
			}
		}
		detections = append(detections, d)
	}
	if rows.Err() != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("iterate detections: %v", rows.Err()))
		return
	}

	type signalItem struct {
		SignalType string         `json:"signal_type"`
		SignalKey  string         `json:"signal_key"`
		Confidence *float64       `json:"confidence,omitempty"`
		ValueNum   *float64       `json:"value_num,omitempty"`
		ValueText  *string        `json:"value_text,omitempty"`
		Extra      map[string]any `json:"extra,omitempty"`
	}
	signalRows, err := s.pool.Query(r.Context(), `
		SELECT signal_type, signal_key, confidence, value_num, value_text, extra_jsonb
		FROM inference_signals
		WHERE inference_result_id=$1
		ORDER BY COALESCE(confidence, value_num) DESC NULLS LAST, id ASC
		LIMIT $2
	`, resultID, limit)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("query inference signals: %v", err))
		return
	}
	defer signalRows.Close()
	signals := make([]signalItem, 0, limit)
	for signalRows.Next() {
		var s signalItem
		var extraBytes []byte
		if err := signalRows.Scan(&s.SignalType, &s.SignalKey, &s.Confidence, &s.ValueNum, &s.ValueText, &extraBytes); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan inference signal: %v", err))
			return
		}
		if len(extraBytes) > 0 {
			var m map[string]any
			if err := json.Unmarshal(extraBytes, &m); err == nil {
				s.Extra = m
			}
		}
		signals = append(signals, s)
	}
	if signalRows.Err() != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("iterate inference signals: %v", signalRows.Err()))
		return
	}

	boxedURL := ""
	if boxedObjectKey != nil && *boxedObjectKey != "" {
		if url, err := s.r2.PresignGet(r.Context(), *boxedObjectKey, s.cfg.R2SignGetTTL); err == nil {
			boxedURL = url
		}
	}
	rawURL := ""
	if rawObjectKey != nil && *rawObjectKey != "" {
		if url, err := s.r2.PresignGet(r.Context(), *rawObjectKey, s.cfg.R2SignGetTTL); err == nil {
			rawURL = url
		}
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{
		"latest_result": map[string]any{
			"inference_result_id": resultID,
			"pipeline_id":         resultPipeline,
			"revision":            revision,
			"status":              resultStatus,
			"created_at":          createdAt,
			"finished_at":         finishedAt,
			"raw_object_key":      rawObjectKey,
			"raw_image_url":       rawURL,
			"boxed_object_key":    boxedObjectKey,
			"boxed_image_url":     boxedURL,
			"signal_count":        len(signals),
		},
		"detections": detections,
		"signals":    signals,
		"limit":      limit,
	})
}

func (s *Server) getStreamByID(ctx context.Context, id int64) (model.Stream, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT
			id, provider, external_id, name, slug, source_url, source_page_url,
			source_family,
			capture_family, expected_fps, expected_image_interval_sec,
			lat, lon, location_text, location_country, location_country_code, location_region, location_city, location_locality, location_source, metadata_jsonb,
			recording_state, recording_failed_reason, recording_failed_at, capture_type, execution_class, execution_config_jsonb, tags,
			created_at, updated_at
		FROM streams
		WHERE id=$1
	`, id)
	if err != nil {
		return model.Stream{}, fmt.Errorf("query stream: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		return model.Stream{}, fmt.Errorf("stream %d not found", id)
	}
	stream, metaBytes, cfgBytes, err := scanStream(rows)
	if err != nil {
		return model.Stream{}, err
	}
	if err := decodeStreamPayload(&stream, metaBytes, cfgBytes); err != nil {
		return model.Stream{}, err
	}
	if err := s.loadRuntimeIntoStream(ctx, &stream); err != nil {
		return model.Stream{}, err
	}
	return stream, nil
}

func scanStream(rows pgx.Rows) (model.Stream, []byte, []byte, error) {
	var s model.Stream
	var meta []byte
	var cfg []byte
	var recordingState string
	var sourceURL string
	var sourceFamily string
	var captureFamily string
	var captureType string
	var executionClass string
	if err := rows.Scan(
		&s.ID, &s.Provider, &s.ExternalID, &s.Name, &s.Slug, &sourceURL, &s.SourcePageURL,
		&sourceFamily,
		&captureFamily, &s.ExpectedFPS, &s.ExpectedImageInterval,
		&s.Lat, &s.Lon, &s.LocationText, &s.LocationCountry, &s.LocationCountryCode, &s.LocationRegion, &s.LocationCity, &s.LocationLocality, &s.LocationSource, &meta,
		&recordingState, &s.RecordingFailedReason, &s.RecordingFailedAt, &captureType, &executionClass, &cfg, &s.Tags,
		&s.CreatedAt, &s.UpdatedAt,
	); err != nil {
		return model.Stream{}, nil, nil, err
	}
	s.SourceURL = sourceURL
	s.SourceFamily = sourceFamily
	s.CaptureFamily = captureFamily
	s.CaptureType = captureType
	s.ExecutionClass = executionClass
	s.RecordingState = model.RecordingState(recordingState)
	return s, meta, cfg, nil
}

func decodeStreamPayload(s *model.Stream, meta []byte, captureCfg []byte) error {
	if len(meta) == 0 {
		s.MetadataJSON = map[string]any{}
	} else {
		var m map[string]any
		if err := json.Unmarshal(meta, &m); err != nil {
			return err
		}
		s.MetadataJSON = m
	}
	if len(captureCfg) == 0 {
		s.ExecutionConfigJSON = map[string]any{}
		return nil
	}
	var cfg map[string]any
	if err := json.Unmarshal(captureCfg, &cfg); err != nil {
		return err
	}
	s.ExecutionConfigJSON = cfg
	return nil
}

func (s *Server) loadRuntimeIntoStream(ctx context.Context, stream *model.Stream) error {
	if stream == nil {
		return nil
	}
	var status, executionClass, resolvedCaptureType, resolved, errText *string
	var lastFrame *time.Time
	var errorsCount *int
	err := s.pool.QueryRow(ctx, `
		SELECT status, execution_class, resolved_capture_type, resolved_url, last_frame_at, consecutive_errors, last_error_text
		FROM stream_capture_runtime
		WHERE stream_id=$1
	`, stream.ID).Scan(&status, &executionClass, &resolvedCaptureType, &resolved, &lastFrame, &errorsCount, &errText)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		return fmt.Errorf("query stream runtime: %w", err)
	}
	stream.CaptureRuntimeStatus = status
	stream.CaptureRuntimeClass = executionClass
	stream.CaptureRuntimeType = resolvedCaptureType
	stream.CaptureRuntimeResolved = resolved
	stream.CaptureRuntimeLastSeen = lastFrame
	stream.CaptureRuntimeErrors = errorsCount
	stream.CaptureRuntimeError = errText
	return nil
}

func (s *Server) streamInferenceCounts(ctx context.Context, streamIDs []int64) (map[int64]int64, map[int64]int64, error) {
	inferenced := make(map[int64]int64, len(streamIDs))
	people := make(map[int64]int64, len(streamIDs))
	if len(streamIDs) == 0 {
		return inferenced, people, nil
	}
	rows, err := s.pool.Query(ctx, `
		SELECT f.stream_id, COUNT(*)::bigint
		FROM frames f
		JOIN inference_results ir ON ir.frame_id = f.id
		WHERE f.stream_id = ANY($1)
		  AND ir.status IN ('success', 'queued_boxed')
		GROUP BY f.stream_id
	`, streamIDs)
	if err != nil {
		return nil, nil, err
	}
	for rows.Next() {
		var streamID int64
		var ct int64
		if err := rows.Scan(&streamID, &ct); err != nil {
			rows.Close()
			return nil, nil, err
		}
		inferenced[streamID] = ct
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, nil, err
	}
	rows.Close()

	rows, err = s.pool.Query(ctx, `
		SELECT f.stream_id, COUNT(*)::bigint
		FROM frames f
		JOIN inference_results ir ON ir.frame_id = f.id
		JOIN detections d ON d.inference_result_id = ir.id
		WHERE f.stream_id = ANY($1)
		  AND ir.status IN ('success', 'queued_boxed')
		  AND d.class_name = 'person'
		GROUP BY f.stream_id
	`, streamIDs)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var streamID int64
		var ct int64
		if err := rows.Scan(&streamID, &ct); err != nil {
			return nil, nil, err
		}
		people[streamID] = ct
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	return inferenced, people, nil
}

func derefString(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}

func parseRecordingState(raw string) (model.RecordingState, bool) {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case string(model.RecordingStateOff):
		return model.RecordingStateOff, true
	case string(model.RecordingStateOn):
		return model.RecordingStateOn, true
	default:
		return "", false
	}
}

func expectedFramesPer60s(intervalSec int) int64 {
	if intervalSec <= 0 {
		intervalSec = settings.DefaultRecordingIntervalSec
	}
	v := 60 / intervalSec
	if v <= 0 {
		return 1
	}
	return int64(v)
}

func runtimeModeForStream(st model.Stream) capture.Mode {
	cfg := st.ExecutionConfigJSON
	if cfg == nil {
		cfg = map[string]any{}
	}
	mode := capture.LegacyModeForStream(st.CaptureType, st.ExecutionClass)
	spec := capture.StreamSpec{
		ID:                 st.ID,
		Provider:           st.Provider,
		StreamURL:          st.SourceURL,
		SourcePageURL:      st.SourcePageURL,
		CaptureMode:        mode,
		CaptureConfig:      cfg,
		CaptureIntervalSec: capture.GetConfigInt(cfg, "poll_interval_sec", 1),
		TargetFPS:          capture.SegmentTargetFPS,
		MaxFrameBytes:      25 << 20,
	}
	return capture.EffectiveMode(spec)
}

type queryRower interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func (s *Server) modeMaxActive(ctx context.Context, q queryRower, mode capture.Mode) (maxActive int64, managedByWorkerHeartbeat bool, activeWorkers int64, err error) {
	mode = capture.NormalizeMode(string(mode))
	maxActive, activeWorkers, err = s.activeWorkerCapacity(ctx, mode)
	if err != nil {
		return 0, true, 0, err
	}
	return maxActive, true, activeWorkers, nil
}

func (s *Server) activeWorkerCapacity(ctx context.Context, mode capture.Mode) (capacity int64, activeWorkers int64, err error) {
	executionClass := capture.ModeToExecutionClass(mode)
	if executionClass == "" {
		var ok bool
		executionClass, ok = capture.NormalizeExecutionClass(string(mode))
		if !ok {
			return 0, 0, fmt.Errorf("invalid execution class for worker capacity: %q", mode)
		}
	}
	if err := s.pool.QueryRow(ctx, `
		SELECT
			COALESCE(SUM(max_active)::bigint, 0),
			COUNT(*)::bigint
		FROM server_execution_capacity
		WHERE execution_class=$1
		  AND draining=false
		  AND lease_expires_at > now()
	`, executionClass).Scan(&capacity, &activeWorkers); err != nil {
		return 0, 0, fmt.Errorf("query worker capacity for execution class %s: %w", executionClass, err)
	}
	return capacity, activeWorkers, nil
}

func (s *Server) activeCountsByRuntimeMode(ctx context.Context) (map[string]int64, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, provider, source_url, source_page_url, capture_type, execution_class, execution_config_jsonb
		FROM streams
		WHERE recording_state='on'
	`)
	if err != nil {
		return nil, fmt.Errorf("query active streams by mode: %w", err)
	}
	defer rows.Close()

	counts := map[string]int64{}
	for rows.Next() {
		var id int64
		var provider, streamURL, sourcePageURL, captureType, executionClass string
		var cfgBytes []byte
		if err := rows.Scan(&id, &provider, &streamURL, &sourcePageURL, &captureType, &executionClass, &cfgBytes); err != nil {
			return nil, fmt.Errorf("scan active stream by mode: %w", err)
		}
		cfg := map[string]any{}
		if len(cfgBytes) > 0 {
			if err := json.Unmarshal(cfgBytes, &cfg); err != nil {
				return nil, fmt.Errorf("decode active stream config %d: %w", id, err)
			}
		}
		mode := capture.EffectiveMode(capture.StreamSpec{
			ID:                 id,
			Provider:           provider,
			StreamURL:          streamURL,
			SourcePageURL:      sourcePageURL,
			CaptureMode:        capture.LegacyModeForStream(captureType, executionClass),
			CaptureConfig:      cfg,
			CaptureIntervalSec: capture.GetConfigInt(cfg, "poll_interval_sec", 1),
			TargetFPS:          capture.SegmentTargetFPS,
			MaxFrameBytes:      25 << 20,
		})
		counts[string(mode)]++
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("iterate active streams by mode: %w", rows.Err())
	}
	return counts, nil
}

func (s *Server) successFrameCountsSince(ctx context.Context, streamIDs []int64, window time.Duration) (map[int64]int64, error) {
	out := make(map[int64]int64, len(streamIDs))
	if len(streamIDs) == 0 {
		return out, nil
	}
	if window <= 0 {
		window = 60 * time.Second
	}
	seconds := int64(window.Seconds())
	if seconds <= 0 {
		seconds = 60
	}
	rows, err := s.pool.Query(ctx, `
		SELECT f.stream_id, COUNT(*)::bigint
		FROM frames f
		WHERE f.stream_id = ANY($1::bigint[])
		  AND f.capture_status='success'
		  AND f.captured_at >= now() - make_interval(secs => $2)
		GROUP BY f.stream_id
	`, streamIDs, seconds)
	if err != nil {
		return nil, fmt.Errorf("query success frame counts: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var streamID int64
		var count int64
		if err := rows.Scan(&streamID, &count); err != nil {
			return nil, fmt.Errorf("scan success frame count: %w", err)
		}
		out[streamID] = count
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("iterate success frame counts: %w", rows.Err())
	}
	return out, nil
}

func (s *Server) successSegmentCountsSince(ctx context.Context, streamIDs []int64, window time.Duration) (map[int64]int64, error) {
	out := make(map[int64]int64, len(streamIDs))
	if len(streamIDs) == 0 {
		return out, nil
	}
	if window <= 0 {
		window = 60 * time.Second
	}
	seconds := int64(window.Seconds())
	if seconds <= 0 {
		seconds = 60
	}
	rows, err := s.pool.Query(ctx, `
		SELECT cs.stream_id, COUNT(*)::bigint
		FROM capture_segments cs
		WHERE cs.stream_id = ANY($1::bigint[])
		  AND cs.capture_status='success'
		  AND cs.segment_end_at >= now() - make_interval(secs => $2)
		GROUP BY cs.stream_id
	`, streamIDs, seconds)
	if err != nil {
		return nil, fmt.Errorf("query success segment counts: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var streamID int64
		var count int64
		if err := rows.Scan(&streamID, &count); err != nil {
			return nil, fmt.Errorf("scan success segment count: %w", err)
		}
		out[streamID] = count
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("iterate success segment counts: %w", rows.Err())
	}
	return out, nil
}

func (s *Server) successCaptureCountsSince(ctx context.Context, frameStreamIDs, clipStreamIDs []int64, window time.Duration) (map[int64]int64, error) {
	out := make(map[int64]int64, len(frameStreamIDs)+len(clipStreamIDs))
	frameCounts, err := s.successFrameCountsSince(ctx, frameStreamIDs, window)
	if err != nil {
		return nil, err
	}
	for k, v := range frameCounts {
		out[k] = v
	}
	segmentCounts, err := s.successSegmentCountsSince(ctx, clipStreamIDs, window)
	if err != nil {
		return nil, err
	}
	for k, v := range segmentCounts {
		out[k] = v
	}
	return out, nil
}

func isClipNativeExecutionClass(raw string) bool {
	normalized := strings.TrimSpace(strings.ToLower(raw))
	switch normalized {
	case capture.ExecutionClassVideoLive:
		return true
	case capture.ExecutionClassYouTubeDirect:
		return true
	case capture.ExecutionClassImagePoll:
		return false
	}
	mode := capture.NormalizeMode(normalized)
	switch mode {
	case capture.ModeYouTubeLive, capture.ModeHLSLive, capture.ModeFFmpegDirect:
		return true
	default:
		return false
	}
}

func captureUnitLabelForExecutionClass(raw string) string {
	if isClipNativeExecutionClass(raw) {
		return "clips"
	}
	return "frames"
}

func expectedCapturesPer60s(raw string, recordingIntervalSec int) int64 {
	if isClipNativeExecutionClass(raw) {
		return 0
	}
	return expectedFramesPer60s(recordingIntervalSec)
}

func staleThresholdSecForExecutionClass(raw string, recordingIntervalSec int) int64 {
	if isClipNativeExecutionClass(raw) {
		return settings.DefaultSampleStaleWindowSec
	}
	threshold := int64(recordingIntervalSec * 5)
	if threshold < 15 {
		threshold = 15
	}
	return threshold
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func (s *Server) requireIdempotency(w http.ResponseWriter, r *http.Request, endpoint string) bool {
	key := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if key == "" {
		util.WriteError(w, http.StatusBadRequest, "missing Idempotency-Key header")
		return false
	}
	created, err := s.reserveIdempotency(r.Context(), endpoint, key)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("reserve idempotency key: %v", err))
		return false
	}
	if !created {
		util.WriteError(w, http.StatusConflict, "duplicate Idempotency-Key for endpoint")
		return false
	}
	return true
}

func (s *Server) reserveIdempotency(ctx context.Context, endpoint, key string) (bool, error) {
	ct, err := s.pool.Exec(ctx, `
		INSERT INTO api_idempotency (endpoint, idempotency_key)
		VALUES ($1,$2)
		ON CONFLICT DO NOTHING
	`, endpoint, key)
	if err != nil {
		return false, err
	}
	return ct.RowsAffected() == 1, nil
}

func parseInt64Path(w http.ResponseWriter, r *http.Request, key string) (int64, bool) {
	raw := chi.URLParam(r, key)
	id, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || id <= 0 {
		util.WriteError(w, http.StatusBadRequest, fmt.Sprintf("invalid path %s", key))
		return 0, false
	}
	return id, true
}

func parseSortQuery(w http.ResponseWriter, r *http.Request, orderColumns map[string]string, defaultSortBy, defaultSortDir string) (orderExpr, sortBy, sortDir string, ok bool) {
	sortBy = strings.TrimSpace(strings.ToLower(r.URL.Query().Get("sort_by")))
	if sortBy == "" {
		sortBy = strings.TrimSpace(strings.ToLower(defaultSortBy))
	}
	orderExpr, ok = orderColumns[sortBy]
	if !ok {
		util.WriteError(w, http.StatusBadRequest, "invalid sort_by")
		return "", "", "", false
	}
	sortDir = strings.TrimSpace(strings.ToLower(r.URL.Query().Get("sort_dir")))
	if sortDir == "" {
		sortDir = strings.TrimSpace(strings.ToLower(defaultSortDir))
		if sortDir == "" {
			sortDir = "desc"
		}
	}
	if sortDir != "asc" && sortDir != "desc" {
		util.WriteError(w, http.StatusBadRequest, "invalid sort_dir; expected asc|desc")
		return "", "", "", false
	}
	return orderExpr, sortBy, sortDir, true
}

func parseIntQuery(r *http.Request, key string, def, min, max int) int {
	raw := strings.TrimSpace(r.URL.Query().Get(key))
	if raw == "" {
		return def
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return def
	}
	if v < min {
		return min
	}
	if v > max {
		return max
	}
	return v
}

func parseInt64QueryPtr(r *http.Request, key string) *int64 {
	raw := strings.TrimSpace(r.URL.Query().Get(key))
	if raw == "" {
		return nil
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return nil
	}
	return &v
}

func parseFloat64QueryPtr(r *http.Request, key string) *float64 {
	raw := strings.TrimSpace(r.URL.Query().Get(key))
	if raw == "" {
		return nil
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return nil
	}
	return &v
}

func parseTimeQueryPtr(r *http.Request, key string) *time.Time {
	raw := strings.TrimSpace(r.URL.Query().Get(key))
	if raw == "" {
		return nil
	}
	layouts := []string{time.RFC3339Nano, time.RFC3339, "2006-01-02"}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, raw); err == nil {
			utc := t.UTC()
			return &utc
		}
	}
	return nil
}

func parseBoolQueryPtr(r *http.Request, key string) *bool {
	raw := strings.TrimSpace(strings.ToLower(r.URL.Query().Get(key)))
	if raw == "" {
		return nil
	}
	switch raw {
	case "1", "true", "t", "yes", "y", "on":
		v := true
		return &v
	case "0", "false", "f", "no", "n", "off":
		v := false
		return &v
	default:
		return nil
	}
}

func nonNilMap(in map[string]any) map[string]any {
	if in == nil {
		return map[string]any{}
	}
	return in
}

func dedupeStrings(in []string) []string {
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
	return out
}

func slugify(raw string) string {
	v := strings.ToLower(strings.TrimSpace(raw))
	if v == "" {
		return "stream"
	}
	b := strings.Builder{}
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

func sanitizePathToken(raw string) string {
	if raw == "" {
		return "unknown"
	}
	return slugify(raw)
}

func fileExtensionFromMIME(m string) string {
	m = strings.ToLower(strings.TrimSpace(strings.Split(m, ";")[0]))
	switch m {
	case "image/jpeg", "image/jpg":
		return ".jpg"
	case "image/png":
		return ".png"
	case "image/webp":
		return ".webp"
	case "video/mp4":
		return ".mp4"
	case "video/webm":
		return ".webm"
	default:
		return ""
	}
}

func nullableTrimmed(v string) any {
	v = strings.TrimSpace(v)
	if v == "" {
		return nil
	}
	return v
}
