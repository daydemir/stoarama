package config

import (
	"fmt"
	"net/netip"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Port                             int
	DatabaseURL                      string
	AdmissionDatabaseURL             string
	AdmissionExecutorRole            string
	AdmissionAuthorityRole           string
	APIToken                         string
	ServiceToken                     string
	BootstrapAdminEmail              string
	MigrationDir                     string
	AutoMigrate                      bool
	R2AccountID                      string
	R2AccessKeyID                    string
	R2SecretAccessKey                string
	R2Bucket                         string
	R2Region                         string
	R2Endpoint                       string
	R2SignGetTTL                     time.Duration
	R2SignPutTTL                     time.Duration
	StorageCredKey                   string
	AppBaseURL                       string
	MagicLinkTTL                     time.Duration
	SessionTTL                       time.Duration
	SharedRecordingsAccountID        int64
	SharedRecordingsSlug             string
	SharedRecordingsPublic           bool
	SharedRecordingsPassword         string
	SharedRecordingsCookieSigningKey string
	SharedRecordingsProxyCIDRs       []netip.Prefix
	EmailProvider                    string
	EmailFrom                        string
	EmailReplyTo                     string
	EmailResendAPIKey                string
	StreamAlertsRecipients           string
	StreamAlertsEnabled              bool
	StreamAlertsPollSec              int
	StreamAlertsProblemDelaySec      int
	StreamAlertsRepeatSec            int
	StreamAlertsResolutionEmail      bool
	CaptureTickSec                   int
	CaptureConcurrency               int
	CaptureModeAllowlist             string
	ContinuousSourcePTSCanary        string
	CaptureLeaseSec                  int
	CaptureUnsupportedThreshold      int
	CaptureFrameQueueSize            int
	CaptureFrameEnqueueTimeout       int
	CaptureFrameWriters              int
	InferenceBoxPollSec              int
	InferenceBoxConcurrency          int
	InferenceBoxLeaseSec             int
	InferenceBoxMaxAttempts          int
	InferenceBoxRetryBaseSec         int
	InferenceBoxRetryMaxSec          int
	BoxWorkerEmbedded                bool
	WorkerID                         string
	SurveyEnabled                    bool
	SurveyConcurrency                int
	SurveyResolveTimeoutSec          int
	SurveyCaptureTimeoutSec          int

	// Survey inline yolo11x detection (#47). Consumed only by
	// `stoaramactl survey run-once --detect` on the unified survey+detection
	// droplet. SurveyModelKey/SurveyModelSHA256 are used by cloud-init to fetch +
	// verify the model; SurveyModelPath is the local path the binary loads. The
	// onnxruntime shared library path is read directly from ONNXRUNTIME_LIB_PATH.
	SurveyModelPath             string
	SurveyModelKey              string
	SurveyModelSHA256           string
	SurveyDetectConf            float64
	SurveyDetectIoU             float64
	SurveyDetectImgsz           int
	SurveyDetectIntraOpThreads  int
	SurveyDetectSampleRate      float64
	SurveyDetectPipelineVersion string

	// Cloud-recordability probe. Default OFF (ship-dark): with this false the probe
	// never runs, so no droplet, no ffmpeg, zero spend, and the recordability tables
	// stay empty. The auto-route wiring reads those (empty) tables regardless and is
	// inert until a probe writes a row.
	StreamRecordabilityProbeEnabled bool

	// Standalone stream recorder: cron scheduler (runs on the dedicated control service).
	RecSchedEnabled        bool
	RecSchedTickSec        int
	RecSchedCatchupSec     int
	RecSchedMinIntervalSec int
	RecSchedMaxJobsPerTick int

	// Standalone stream recorder: Stripe billing (set on stoarama-api; nil/empty disables).
	StripeSecretKey      string
	StripeWebhookSecret  string
	StripePriceID        string
	StripeMeterID        string
	StripeGBMonthPriceID string // env STRIPE_GB_MONTH_PRICE_ID (now holds the stream_hour_month metered price id)
	StripeGBMonthMeterID string // env STRIPE_GB_MONTH_METER_ID (now holds the stream_hour_month meter id; parsed for symmetry, unused like StripeMeterID)
	StripeLivemode       bool

	// Standalone stream recorder: worker (consumed on the recorder droplet/node).
	RecordingWorkerConcurrency  int
	RecordingWorkerHeartbeatSec int
	RecordingWorkerPollSec      int
	// RecordingFrozenHLSQuiescenceAllowlist is an exact, default-empty list of
	// worker/recording pairs allowed to use the frozen-live-edge watcher. Merely
	// deploying the code cannot enable it for any recording.
	RecordingFrozenHLSQuiescenceAllowlist string
	// RelayUploadWorkers bounds how many segment uploads ONE continuous recording
	// job keeps in flight. Segment delivery used to be strictly serial per job, so
	// a single job could not exceed one reserve+upload+ingest round trip at a time
	// and a high-bitrate stream built a delivery backlog that never drained.
	//
	// This is PER JOB, so a node's total simultaneous uploads is
	// RecordingWorkerConcurrency * RelayUploadWorkers (a relay running 8 streams at
	// the default 4 workers opens up to 32 concurrent uploads). Size the two
	// together against the node's uplink and CPU -- TLS for many parallel PUTs is
	// not free on a Raspberry Pi. Raise RELAY_UPLOAD_WORKERS only while a node's
	// aggregate throughput still scales with concurrency; once per-flow throughput
	// starts falling as flows are added, the link is saturated and more workers
	// only add contention.
	RelayUploadWorkers int

	// Standalone stream recorder: droplet-pool autoscaler (runs on the dedicated
	// control service alongside the scheduler). Empty/disabled by default.
	DOAPIToken                      string
	DropletPoolEnabled              bool
	DropletPoolTickSec              int
	DropletPoolLookaheadSec         int
	DropletPoolCapacity             int
	DropletPoolProvisionLeadSec     int
	DropletPoolProvisionTimeoutSec  int
	DropletPoolIdleGraceSec         int
	DropletPoolDrainTimeoutSec      int
	DropletPoolScaleUpCooldownSec   int
	DropletPoolScaleDownCooldownSec int
	DropletPoolMin                  int
	DropletPoolMax                  int
	DropletPoolMaxScaleUpBatch      int
	DropletPoolRegion               string
	DropletPoolSize                 string
	DropletPoolImage                string
	DropletPoolSSHKey               string
	DropletPoolProjectID            string
	DropletPoolFirewallID           string
	DropletPoolOperatorEmail        string
	DropletPoolRepoURL              string
	DropletPoolRepoRef              string
	DropletPoolRepoCloneToken       string
	DropletPoolBackendAPIURL        string
	DropletPoolBuildSHA             string
}

func Load() (Config, error) {
	cfg := Config{
		Port:                             intEnv("PORT", 8080),
		DatabaseURL:                      os.Getenv("DATABASE_URL"),
		AdmissionDatabaseURL:             os.Getenv("ADMISSION_DATABASE_URL"),
		AdmissionExecutorRole:            strings.TrimSpace(os.Getenv("STOARAMA_ADMISSION_EXECUTOR_ROLE")),
		AdmissionAuthorityRole:           strings.TrimSpace(os.Getenv("STOARAMA_ADMISSION_AUTHORITY_ROLE")),
		APIToken:                         firstNonEmpty(os.Getenv("SERVICE_TOKEN"), os.Getenv("API_TOKEN")),
		ServiceToken:                     firstNonEmpty(os.Getenv("SERVICE_TOKEN"), os.Getenv("API_TOKEN")),
		BootstrapAdminEmail:              strings.ToLower(strings.TrimSpace(os.Getenv("BOOTSTRAP_ADMIN_EMAIL"))),
		MigrationDir:                     strEnv("MIGRATION_DIR", ""),
		AutoMigrate:                      boolEnv("AUTO_MIGRATE", false),
		R2AccountID:                      os.Getenv("R2_ACCOUNT_ID"),
		R2AccessKeyID:                    os.Getenv("R2_ACCESS_KEY_ID"),
		R2SecretAccessKey:                os.Getenv("R2_SECRET_ACCESS_KEY"),
		R2Bucket:                         os.Getenv("R2_BUCKET"),
		R2Region:                         strEnv("R2_REGION", "auto"),
		R2Endpoint:                       os.Getenv("R2_ENDPOINT"),
		R2SignGetTTL:                     durEnv("R2_SIGN_GET_TTL", 10*time.Minute),
		R2SignPutTTL:                     durEnv("R2_SIGN_PUT_TTL", 15*time.Minute),
		StorageCredKey:                   strings.TrimSpace(os.Getenv("STORAGE_CRED_KEY")),
		AppBaseURL:                       strings.TrimRight(strEnv("APP_BASE_URL", strEnv("RESEARCH_APP_BASE_URL", "")), "/"),
		MagicLinkTTL:                     durEnv("MAGIC_LINK_TTL", durEnv("RESEARCH_MAGIC_LINK_TTL", 60*time.Minute)),
		SessionTTL:                       durEnv("SESSION_TTL", durEnv("RESEARCH_SESSION_TTL", 24*30*time.Hour)),
		SharedRecordingsAccountID:        int64(intEnv("MIT_SCL_RECORDINGS_READ_ACCOUNT_ID", 0)),
		SharedRecordingsSlug:             strEnv("MIT_SCL_RECORDINGS_READ_SLUG", "mit-scl"),
		SharedRecordingsPublic:           boolEnv("MIT_SCL_RECORDINGS_PUBLIC", false),
		SharedRecordingsPassword:         strings.TrimSpace(os.Getenv("MIT_SCL_RECORDINGS_READ_PASSWORD")),
		SharedRecordingsCookieSigningKey: strings.TrimSpace(os.Getenv("MIT_SCL_RECORDINGS_COOKIE_SIGNING_KEY")),
		EmailProvider:                    strEnv("EMAIL_PROVIDER", strEnv("RESEARCH_EMAIL_PROVIDER", "log")),
		EmailFrom:                        firstNonEmpty(os.Getenv("EMAIL_FROM"), os.Getenv("RESEARCH_EMAIL_FROM")),
		EmailReplyTo:                     firstNonEmpty(os.Getenv("EMAIL_REPLY_TO"), os.Getenv("RESEARCH_EMAIL_REPLY_TO")),
		EmailResendAPIKey:                firstNonEmpty(os.Getenv("EMAIL_RESEND_API_KEY"), os.Getenv("RESEARCH_EMAIL_RESEND_API_KEY")),
		StreamAlertsRecipients:           firstNonEmpty(os.Getenv("STREAM_ALERTS_RECIPIENTS"), os.Getenv("RESEARCH_STREAM_ALERTS_RECIPIENTS")),
		StreamAlertsEnabled:              boolEnv("STREAM_ALERTS_ENABLED", false),
		StreamAlertsPollSec:              intEnv("STREAM_ALERTS_POLL_SEC", 60),
		StreamAlertsProblemDelaySec:      intEnv("STREAM_ALERTS_PROBLEM_DELAY_SEC", 300),
		StreamAlertsRepeatSec:            intEnv("STREAM_ALERTS_REPEAT_SEC", 43200),
		StreamAlertsResolutionEmail:      boolEnv("STREAM_ALERTS_RESOLUTION_EMAIL", true),
		CaptureTickSec:                   intEnv("CAPTURE_TICK_SEC", 1),
		CaptureConcurrency:               intEnv("CAPTURE_CONCURRENCY", 8),
		CaptureModeAllowlist:             strEnv("CAPTURE_MODE_ALLOWLIST", ""),
		ContinuousSourcePTSCanary:        strEnv("CONTINUOUS_SOURCE_PTS_CANARY", ""),
		CaptureLeaseSec:                  intEnv("CAPTURE_LEASE_SEC", 30),
		CaptureUnsupportedThreshold:      intEnv("CAPTURE_UNSUPPORTED_THRESHOLD", 8),
		CaptureFrameQueueSize:            intEnv("CAPTURE_FRAME_QUEUE_SIZE", 16),
		CaptureFrameEnqueueTimeout:       intEnv("CAPTURE_FRAME_ENQUEUE_TIMEOUT_SEC", 3),
		CaptureFrameWriters:              intEnv("CAPTURE_FRAME_WRITERS", 1),
		InferenceBoxPollSec:              intEnv("BOX_WORKER_POLL_SEC", 2),
		InferenceBoxConcurrency:          intEnv("BOX_WORKER_CONCURRENCY", 2),
		InferenceBoxLeaseSec:             intEnv("BOX_WORKER_LEASE_SEC", 300),
		InferenceBoxMaxAttempts:          intEnv("BOX_WORKER_MAX_ATTEMPTS", 8),
		InferenceBoxRetryBaseSec:         intEnv("BOX_WORKER_RETRY_BASE_SEC", 5),
		InferenceBoxRetryMaxSec:          intEnv("BOX_WORKER_RETRY_MAX_SEC", 300),
		BoxWorkerEmbedded:                boolEnv("BOX_WORKER_EMBEDDED", false),
		WorkerID:                         strEnv("WORKER_ID", "capture-worker-1"),
		SurveyEnabled:                    boolEnv("SURVEY_ENABLED", false),
		SurveyConcurrency:                intEnv("SURVEY_CONCURRENCY", 4),
		SurveyResolveTimeoutSec:          intEnv("SURVEY_RESOLVE_TIMEOUT_SEC", 60),
		SurveyCaptureTimeoutSec:          intEnv("SURVEY_CAPTURE_TIMEOUT_SEC", 60),

		SurveyModelPath:             strEnv("SURVEY_MODEL_PATH", "/opt/stoarama/models/yolo11x-1600.onnx"),
		SurveyModelKey:              strEnv("SURVEY_MODEL_KEY", "survey/models/yolo11x-1600-74c2734984aa83a832cf377efbf8c2169a5f0d0f0b31b0123a852af3ad89c83f.onnx"),
		SurveyModelSHA256:           strEnv("SURVEY_MODEL_SHA256", "74c2734984aa83a832cf377efbf8c2169a5f0d0f0b31b0123a852af3ad89c83f"),
		SurveyDetectConf:            floatEnv("SURVEY_DETECT_CONF", 0.10),
		SurveyDetectIoU:             floatEnv("SURVEY_DETECT_IOU", 0.45),
		SurveyDetectImgsz:           intEnv("SURVEY_DETECT_IMGSZ", 1600),
		SurveyDetectIntraOpThreads:  intEnv("SURVEY_DETECT_INTRA_OP_THREADS", 2),
		SurveyDetectSampleRate:      floatEnv("SURVEY_DETECT_SAMPLE_RATE", 0.15),
		SurveyDetectPipelineVersion: strEnv("SURVEY_DETECT_PIPELINE_VERSION", "yolo11x-img1600-conf010-notile-v1"),

		StreamRecordabilityProbeEnabled: boolEnv("STREAM_RECORDABILITY_PROBE_ENABLED", false),

		RecSchedEnabled:        boolEnv("REC_SCHED_ENABLED", false),
		RecSchedTickSec:        intEnv("REC_SCHED_TICK_SEC", 15),
		RecSchedCatchupSec:     intEnv("REC_SCHED_CATCHUP_SEC", 900),
		RecSchedMinIntervalSec: intEnv("REC_SCHED_MIN_INTERVAL_SEC", 600),
		RecSchedMaxJobsPerTick: intEnv("REC_SCHED_MAX_JOBS_PER_TICK", 500),

		StripeSecretKey:      strings.TrimSpace(os.Getenv("STRIPE_SECRET_KEY")),
		StripeWebhookSecret:  strings.TrimSpace(os.Getenv("STRIPE_WEBHOOK_SECRET")),
		StripePriceID:        strings.TrimSpace(os.Getenv("STRIPE_PRICE_ID")),
		StripeMeterID:        strings.TrimSpace(os.Getenv("STRIPE_METER_ID")),
		StripeGBMonthPriceID: strings.TrimSpace(os.Getenv("STRIPE_GB_MONTH_PRICE_ID")),
		StripeGBMonthMeterID: strings.TrimSpace(os.Getenv("STRIPE_GB_MONTH_METER_ID")),
		StripeLivemode:       boolEnv("STRIPE_LIVEMODE", false),

		RecordingWorkerConcurrency:            intEnv("RECORDING_WORKER_CONCURRENCY", 1),
		RecordingWorkerHeartbeatSec:           intEnv("RECORDING_WORKER_HEARTBEAT_SEC", 15),
		RecordingWorkerPollSec:                intEnv("RECORDING_WORKER_POLL_SEC", 5),
		RecordingFrozenHLSQuiescenceAllowlist: strEnv("RECORDING_FROZEN_HLS_QUIESCENCE_ALLOWLIST", ""),
		RelayUploadWorkers:                    RelayUploadWorkersFromEnv(),

		DOAPIToken:                      strings.TrimSpace(os.Getenv("DO_API_TOKEN")),
		DropletPoolEnabled:              boolEnv("DROPLET_POOL_ENABLED", false),
		DropletPoolTickSec:              intEnv("DROPLET_POOL_TICK_SEC", 30),
		DropletPoolLookaheadSec:         intEnv("DROPLET_POOL_LOOKAHEAD_SEC", 1800),
		DropletPoolCapacity:             intEnv("DROPLET_POOL_CAPACITY", 1),
		DropletPoolProvisionLeadSec:     intEnv("DROPLET_POOL_PROVISION_LEAD_SEC", 600),
		DropletPoolProvisionTimeoutSec:  intEnv("DROPLET_POOL_PROVISION_TIMEOUT_SEC", 900),
		DropletPoolIdleGraceSec:         intEnv("DROPLET_POOL_IDLE_GRACE_SEC", 600),
		DropletPoolDrainTimeoutSec:      intEnv("DROPLET_POOL_DRAIN_TIMEOUT_SEC", 600),
		DropletPoolScaleUpCooldownSec:   intEnv("DROPLET_POOL_SCALEUP_COOLDOWN_SEC", 60),
		DropletPoolScaleDownCooldownSec: intEnv("DROPLET_POOL_SCALEDOWN_COOLDOWN_SEC", 300),
		DropletPoolMin:                  intEnv("DROPLET_POOL_MIN", 0),
		DropletPoolMax:                  intEnv("DROPLET_POOL_MAX", 5),
		DropletPoolMaxScaleUpBatch:      intEnv("DROPLET_POOL_MAX_SCALEUP_BATCH", 4),
		DropletPoolRegion:               strEnv("DROPLET_POOL_REGION", "nyc1"),
		DropletPoolSize:                 strEnv("DROPLET_POOL_SIZE", "s-2vcpu-4gb"),
		DropletPoolImage:                strEnv("DROPLET_POOL_IMAGE", "ubuntu-24-04-x64"),
		DropletPoolSSHKey:               strings.TrimSpace(os.Getenv("DROPLET_POOL_SSH_KEY")),
		DropletPoolProjectID:            strings.TrimSpace(os.Getenv("DROPLET_POOL_PROJECT_ID")),
		DropletPoolFirewallID:           strings.TrimSpace(os.Getenv("DROPLET_POOL_FIREWALL_ID")),
		DropletPoolOperatorEmail:        strings.ToLower(strings.TrimSpace(firstNonEmpty(os.Getenv("DROPLET_POOL_OPERATOR_EMAIL"), os.Getenv("BOOTSTRAP_ADMIN_EMAIL")))),
		DropletPoolRepoURL:              strings.TrimSpace(os.Getenv("DROPLET_POOL_REPO_URL")),
		DropletPoolRepoRef:              strEnv("DROPLET_POOL_REPO_REF", "main"),
		DropletPoolRepoCloneToken:       strings.TrimSpace(os.Getenv("DROPLET_POOL_REPO_CLONE_TOKEN")),
		DropletPoolBackendAPIURL:        strings.TrimRight(strings.TrimSpace(firstNonEmpty(os.Getenv("DROPLET_POOL_BACKEND_API_URL"), os.Getenv("BACKEND_API_URL"))), "/"),
		DropletPoolBuildSHA:             strings.ToLower(strings.TrimSpace(os.Getenv("RENDER_GIT_COMMIT"))),
	}
	var err error
	cfg.SharedRecordingsProxyCIDRs, err = prefixesEnv("MIT_SCL_RECORDINGS_TRUSTED_PROXY_CIDRS")
	if err != nil {
		return Config{}, err
	}
	if cfg.R2Endpoint == "" && cfg.R2AccountID != "" {
		cfg.R2Endpoint = fmt.Sprintf("https://%s.r2.cloudflarestorage.com", cfg.R2AccountID)
	}
	if cfg.MagicLinkTTL <= 0 {
		return Config{}, fmt.Errorf("invalid MAGIC_LINK_TTL")
	}
	if cfg.SessionTTL <= 0 {
		return Config{}, fmt.Errorf("invalid SESSION_TTL")
	}
	if cfg.SharedRecordingsAccountID < 0 {
		return Config{}, fmt.Errorf("MIT_SCL_RECORDINGS_READ_ACCOUNT_ID must be positive")
	}
	if cfg.SharedRecordingsAccountID == 0 && (cfg.SharedRecordingsPassword != "" || cfg.SharedRecordingsPublic) {
		return Config{}, fmt.Errorf("MIT_SCL_RECORDINGS_READ_ACCOUNT_ID is required when shared recordings are enabled")
	}
	if cfg.SharedRecordingsAccountID > 0 && cfg.SharedRecordingsPassword == "" && !cfg.SharedRecordingsPublic {
		return Config{}, fmt.Errorf("MIT_SCL_RECORDINGS_READ_PASSWORD is required unless MIT_SCL_RECORDINGS_PUBLIC=true")
	}
	if !validSharedRecordingsSlug(cfg.SharedRecordingsSlug) {
		return Config{}, fmt.Errorf("MIT_SCL_RECORDINGS_READ_SLUG must contain only lowercase letters, numbers, and single hyphens")
	}
	if cfg.SharedRecordingsAccountID > 0 && !cfg.SharedRecordingsPublic && len(cfg.SharedRecordingsCookieSigningKey) < 32 {
		return Config{}, fmt.Errorf("MIT_SCL_RECORDINGS_COOKIE_SIGNING_KEY must contain at least 32 characters when shared recordings are enabled")
	}
	if cfg.SharedRecordingsPassword != "" && len(cfg.SharedRecordingsPassword) < 8 {
		return Config{}, fmt.Errorf("MIT_SCL_RECORDINGS_READ_PASSWORD must contain at least 8 characters")
	}
	return cfg, nil
}

func validSharedRecordingsSlug(slug string) bool {
	if slug == "" || slug[0] == '-' || slug[len(slug)-1] == '-' {
		return false
	}
	previousHyphen := false
	for _, char := range slug {
		hyphen := char == '-'
		if !hyphen && (char < 'a' || char > 'z') && (char < '0' || char > '9') {
			return false
		}
		if hyphen && previousHyphen {
			return false
		}
		previousHyphen = hyphen
	}
	return true
}

func (c Config) ValidateAPI() error {
	if strings.TrimSpace(c.AdmissionDatabaseURL) == "" || strings.TrimSpace(c.AdmissionDatabaseURL) == strings.TrimSpace(c.DatabaseURL) {
		return fmt.Errorf("ADMISSION_DATABASE_URL must be present and distinct from DATABASE_URL")
	}
	if c.AdmissionExecutorRole != "stoarama_admission_executor" || c.AdmissionAuthorityRole != "stoarama_admission_authority" {
		return fmt.Errorf("campaign admission database roles must match the reviewed executor/authority manifest")
	}
	if err := c.ValidateR2(); err != nil {
		return err
	}
	return c.ValidateStripe()
}

// StripeBillingEnabled reports whether the complete billing configuration is
// present. Call ValidateStripe first: a partially configured service must fail
// closed rather than quietly booting in free mode.
func (c Config) StripeBillingEnabled() bool {
	return strings.TrimSpace(c.StripeSecretKey) != "" &&
		strings.TrimSpace(c.StripeWebhookSecret) != "" &&
		strings.TrimSpace(c.StripePriceID) != "" &&
		strings.TrimSpace(c.StripeMeterID) != "" &&
		strings.TrimSpace(c.StripeGBMonthPriceID) != "" &&
		strings.TrimSpace(c.StripeGBMonthMeterID) != ""
}

// ValidateStripe rejects partial, malformed, and mixed-mode billing
// configuration. Billing remains optional only when every Stripe object is
// absent; once any Stripe setting is supplied, all six objects must agree on
// live/test mode so capture and metering cannot silently diverge.
func (c Config) ValidateStripe() error {
	values := []struct {
		name  string
		value string
	}{
		{"STRIPE_SECRET_KEY", c.StripeSecretKey},
		{"STRIPE_WEBHOOK_SECRET", c.StripeWebhookSecret},
		{"STRIPE_PRICE_ID", c.StripePriceID},
		{"STRIPE_METER_ID", c.StripeMeterID},
		{"STRIPE_GB_MONTH_PRICE_ID", c.StripeGBMonthPriceID},
		{"STRIPE_GB_MONTH_METER_ID", c.StripeGBMonthMeterID},
	}
	configured := 0
	for _, item := range values {
		if strings.TrimSpace(item.value) != "" {
			configured++
		}
	}
	if configured == 0 {
		if c.StripeLivemode {
			return fmt.Errorf("STRIPE_LIVEMODE is true but Stripe billing is not configured")
		}
		return nil
	}
	if configured != len(values) {
		missing := make([]string, 0, len(values)-configured)
		for _, item := range values {
			if strings.TrimSpace(item.value) == "" {
				missing = append(missing, item.name)
			}
		}
		return fmt.Errorf("partial Stripe billing configuration; missing %s", strings.Join(missing, ", "))
	}

	key := strings.TrimSpace(c.StripeSecretKey)
	if c.StripeLivemode && !strings.HasPrefix(key, "sk_live_") {
		return fmt.Errorf("STRIPE_LIVEMODE=true requires an sk_live_ STRIPE_SECRET_KEY")
	}
	if !c.StripeLivemode && !strings.HasPrefix(key, "sk_test_") {
		return fmt.Errorf("STRIPE_LIVEMODE=false requires an sk_test_ STRIPE_SECRET_KEY")
	}
	if !strings.HasPrefix(strings.TrimSpace(c.StripeWebhookSecret), "whsec_") {
		return fmt.Errorf("STRIPE_WEBHOOK_SECRET must start with whsec_")
	}
	for _, item := range []struct{ name, value string }{
		{"STRIPE_PRICE_ID", c.StripePriceID},
		{"STRIPE_GB_MONTH_PRICE_ID", c.StripeGBMonthPriceID},
	} {
		if !strings.HasPrefix(strings.TrimSpace(item.value), "price_") {
			return fmt.Errorf("%s must start with price_", item.name)
		}
	}
	for _, item := range []struct{ name, value string }{
		{"STRIPE_METER_ID", c.StripeMeterID},
		{"STRIPE_GB_MONTH_METER_ID", c.StripeGBMonthMeterID},
	} {
		meterID := strings.TrimSpace(item.value)
		if !strings.HasPrefix(meterID, "mtr_") {
			return fmt.Errorf("%s must start with mtr_", item.name)
		}
		if c.StripeLivemode && strings.HasPrefix(meterID, "mtr_test_") {
			return fmt.Errorf("%s is a test meter but STRIPE_LIVEMODE is true", item.name)
		}
		if !c.StripeLivemode && !strings.HasPrefix(meterID, "mtr_test_") {
			return fmt.Errorf("%s is a live meter but STRIPE_LIVEMODE is false", item.name)
		}
	}
	return nil
}

func (c Config) ValidateWorker() error {
	return c.ValidateR2()
}

func (c Config) ValidateR2() error {
	if strings.TrimSpace(c.R2AccountID) == "" || strings.TrimSpace(c.R2AccessKeyID) == "" || strings.TrimSpace(c.R2SecretAccessKey) == "" || strings.TrimSpace(c.R2Bucket) == "" {
		return fmt.Errorf("missing required R2 env vars")
	}
	if strings.TrimSpace(c.R2Endpoint) == "" {
		return fmt.Errorf("missing R2 endpoint")
	}
	return nil
}

// ValidatePool enforces the autoscaler's required config when the pool is
// enabled. The droplet-side worker concurrency MUST equal the per-droplet
// capacity the forecaster divides by (C-cap), or the pool would over- or
// under-provision. Provisioning needs the DO token, an operator account to own
// the per-droplet node tokens, a backend URL for the droplet to call home, and
// the existing DO project + firewall so droplets are spend-audited and egress
// -restricted (S-1).
func (c Config) ValidatePool() error {
	if !regexp.MustCompile(`^[0-9a-f]{40}$`).MatchString(c.DropletPoolBuildSHA) {
		return fmt.Errorf("DROPLET_POOL_ENABLED requires RENDER_GIT_COMMIT as a 40-character lowercase git SHA")
	}
	if strings.TrimSpace(c.DOAPIToken) == "" {
		return fmt.Errorf("DROPLET_POOL_ENABLED requires DO_API_TOKEN")
	}
	if strings.TrimSpace(c.DropletPoolBackendAPIURL) == "" {
		return fmt.Errorf("DROPLET_POOL_ENABLED requires DROPLET_POOL_BACKEND_API_URL (or BACKEND_API_URL)")
	}
	if strings.TrimSpace(c.DropletPoolProjectID) == "" {
		return fmt.Errorf("DROPLET_POOL_ENABLED requires DROPLET_POOL_PROJECT_ID")
	}
	if strings.TrimSpace(c.DropletPoolFirewallID) == "" {
		return fmt.Errorf("DROPLET_POOL_ENABLED requires DROPLET_POOL_FIREWALL_ID (egress block, S-1)")
	}
	if strings.TrimSpace(c.DropletPoolOperatorEmail) == "" {
		return fmt.Errorf("DROPLET_POOL_ENABLED requires DROPLET_POOL_OPERATOR_EMAIL (or BOOTSTRAP_ADMIN_EMAIL) to own per-droplet node tokens")
	}
	if c.DropletPoolCapacity <= 0 {
		return fmt.Errorf("DROPLET_POOL_CAPACITY must be > 0")
	}
	if c.DropletPoolCapacity != c.RecordingWorkerConcurrency {
		return fmt.Errorf("DROPLET_POOL_CAPACITY (%d) must equal RECORDING_WORKER_CONCURRENCY (%d) (C-cap)", c.DropletPoolCapacity, c.RecordingWorkerConcurrency)
	}
	if c.DropletPoolMax <= 0 {
		return fmt.Errorf("DROPLET_POOL_MAX must be > 0 (hard spend cap)")
	}
	if c.DropletPoolMin < 0 || c.DropletPoolMin > c.DropletPoolMax {
		return fmt.Errorf("DROPLET_POOL_MIN must be between 0 and DROPLET_POOL_MAX")
	}
	if c.DropletPoolMaxScaleUpBatch <= 0 {
		return fmt.Errorf("DROPLET_POOL_MAX_SCALEUP_BATCH must be > 0")
	}
	if strings.TrimSpace(c.DropletPoolRegion) == "" || strings.TrimSpace(c.DropletPoolSize) == "" || strings.TrimSpace(c.DropletPoolImage) == "" {
		return fmt.Errorf("DROPLET_POOL_REGION, DROPLET_POOL_SIZE, and DROPLET_POOL_IMAGE are required")
	}
	return nil
}

// DefaultRelayUploadWorkers is the per-job segment upload concurrency used when
// RELAY_UPLOAD_WORKERS is unset. Kept deliberately small: the point is to stop a
// single job from being capped at one round trip, not to saturate the uplink.
//
// In practice this constant IS the fleet-wide value: neither service template
// sets any environment (cmd/stoarama-relay/templates/{systemd.service,launchd.plist}.tmpl),
// and refreshSystemdUnit rewrites the Pi unit from the embedded template on every
// start, so a hand-added Environment= line does not survive a self-update. Changing
// per-node upload concurrency therefore means changing this default and cutting a
// release -- treat it as a code constant, not an ops knob.
//
// 2 is chosen from measurement, not taste. Fixed per-segment overhead is 30-67s
// against a 60s clip, so one flow needs 63-97s per clip and backlogs without bound;
// two flows bring that to 31-49s, draining with 20-48% headroom on the affected
// streams (3.87-4.12 Mbit/s). Raising it costs concurrent TLS PUTs on 2-core
// Raspberry Pi nodes that are also running ffmpeg for every active stream, for
// headroom nothing has measured a need for.
const DefaultRelayUploadWorkers = 2

// RelayUploadWorkersFromEnv reads RELAY_UPLOAD_WORKERS for binaries that never
// build a full Config (the relay reads only its enrollment file).
func RelayUploadWorkersFromEnv() int {
	return intEnv("RELAY_UPLOAD_WORKERS", DefaultRelayUploadWorkers)
}

func intEnv(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		panic(fmt.Sprintf("invalid int env %s=%q: %v", key, v, err))
	}
	return n
}

func prefixesEnv(key string) ([]netip.Prefix, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return nil, nil
	}
	parts := strings.Split(raw, ",")
	prefixes := make([]netip.Prefix, 0, len(parts))
	for _, part := range parts {
		prefix, err := netip.ParsePrefix(strings.TrimSpace(part))
		if err != nil {
			return nil, fmt.Errorf("invalid CIDR in %s: %w", key, err)
		}
		prefixes = append(prefixes, prefix)
	}
	return prefixes, nil
}

func floatEnv(key string, def float64) float64 {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		panic(fmt.Sprintf("invalid float env %s=%q: %v", key, v, err))
	}
	return f
}

func durEnv(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		panic(fmt.Sprintf("invalid duration env %s=%q: %v", key, v, err))
	}
	return d
}

func strEnv(key, def string) string {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	return v
}

func boolEnv(key string, def bool) bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv(key)))
	if v == "" {
		return def
	}
	switch v {
	case "1", "true", "t", "yes", "y", "on":
		return true
	case "0", "false", "f", "no", "n", "off":
		return false
	default:
		panic(fmt.Sprintf("invalid bool env %s=%q", key, v))
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
