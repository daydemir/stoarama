package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Port                        int
	DatabaseURL                 string
	APIToken                    string
	ServiceToken                string
	BootstrapAdminEmail         string
	MigrationDir                string
	AutoMigrate                 bool
	R2AccountID                 string
	R2AccessKeyID               string
	R2SecretAccessKey           string
	R2Bucket                    string
	R2Region                    string
	R2Endpoint                  string
	R2SignGetTTL                time.Duration
	R2SignPutTTL                time.Duration
	StorageCredKey              string
	AppBaseURL                  string
	MagicLinkTTL                time.Duration
	SessionTTL                  time.Duration
	EmailProvider               string
	EmailFrom                   string
	EmailReplyTo                string
	EmailResendAPIKey           string
	StreamAlertsRecipients      string
	StreamAlertsEnabled         bool
	StreamAlertsPollSec         int
	StreamAlertsProblemDelaySec int
	StreamAlertsRepeatSec       int
	StreamAlertsResolutionEmail bool
	CaptureTickSec              int
	CaptureConcurrency          int
	CaptureModeAllowlist        string
	CaptureLeaseSec             int
	CaptureUnsupportedThreshold int
	CaptureFrameQueueSize       int
	CaptureFrameEnqueueTimeout  int
	CaptureFrameWriters         int
	InferenceBoxPollSec         int
	InferenceBoxConcurrency     int
	InferenceBoxLeaseSec        int
	InferenceBoxMaxAttempts     int
	InferenceBoxRetryBaseSec    int
	InferenceBoxRetryMaxSec     int
	BoxWorkerEmbedded           bool
	WorkerID                    string
	SurveyEnabled               bool
	SurveyConcurrency           int
	SurveyResolveTimeoutSec     int
	SurveyCaptureTimeoutSec     int

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
}

func Load() (Config, error) {
	cfg := Config{
		Port:                        intEnv("PORT", 8080),
		DatabaseURL:                 os.Getenv("DATABASE_URL"),
		APIToken:                    firstNonEmpty(os.Getenv("SERVICE_TOKEN"), os.Getenv("API_TOKEN")),
		ServiceToken:                firstNonEmpty(os.Getenv("SERVICE_TOKEN"), os.Getenv("API_TOKEN")),
		BootstrapAdminEmail:         strings.ToLower(strings.TrimSpace(os.Getenv("BOOTSTRAP_ADMIN_EMAIL"))),
		MigrationDir:                strEnv("MIGRATION_DIR", ""),
		AutoMigrate:                 boolEnv("AUTO_MIGRATE", false),
		R2AccountID:                 os.Getenv("R2_ACCOUNT_ID"),
		R2AccessKeyID:               os.Getenv("R2_ACCESS_KEY_ID"),
		R2SecretAccessKey:           os.Getenv("R2_SECRET_ACCESS_KEY"),
		R2Bucket:                    os.Getenv("R2_BUCKET"),
		R2Region:                    strEnv("R2_REGION", "auto"),
		R2Endpoint:                  os.Getenv("R2_ENDPOINT"),
		R2SignGetTTL:                durEnv("R2_SIGN_GET_TTL", 10*time.Minute),
		R2SignPutTTL:                durEnv("R2_SIGN_PUT_TTL", 15*time.Minute),
		StorageCredKey:              strings.TrimSpace(os.Getenv("STORAGE_CRED_KEY")),
		AppBaseURL:                  strings.TrimRight(strEnv("APP_BASE_URL", strEnv("RESEARCH_APP_BASE_URL", "")), "/"),
		MagicLinkTTL:                durEnv("MAGIC_LINK_TTL", durEnv("RESEARCH_MAGIC_LINK_TTL", 20*time.Minute)),
		SessionTTL:                  durEnv("SESSION_TTL", durEnv("RESEARCH_SESSION_TTL", 24*30*time.Hour)),
		EmailProvider:               strEnv("EMAIL_PROVIDER", strEnv("RESEARCH_EMAIL_PROVIDER", "log")),
		EmailFrom:                   firstNonEmpty(os.Getenv("EMAIL_FROM"), os.Getenv("RESEARCH_EMAIL_FROM")),
		EmailReplyTo:                firstNonEmpty(os.Getenv("EMAIL_REPLY_TO"), os.Getenv("RESEARCH_EMAIL_REPLY_TO")),
		EmailResendAPIKey:           firstNonEmpty(os.Getenv("EMAIL_RESEND_API_KEY"), os.Getenv("RESEARCH_EMAIL_RESEND_API_KEY")),
		StreamAlertsRecipients:      firstNonEmpty(os.Getenv("STREAM_ALERTS_RECIPIENTS"), os.Getenv("RESEARCH_STREAM_ALERTS_RECIPIENTS")),
		StreamAlertsEnabled:         boolEnv("STREAM_ALERTS_ENABLED", false),
		StreamAlertsPollSec:         intEnv("STREAM_ALERTS_POLL_SEC", 60),
		StreamAlertsProblemDelaySec: intEnv("STREAM_ALERTS_PROBLEM_DELAY_SEC", 300),
		StreamAlertsRepeatSec:       intEnv("STREAM_ALERTS_REPEAT_SEC", 43200),
		StreamAlertsResolutionEmail: boolEnv("STREAM_ALERTS_RESOLUTION_EMAIL", true),
		CaptureTickSec:              intEnv("CAPTURE_TICK_SEC", 1),
		CaptureConcurrency:          intEnv("CAPTURE_CONCURRENCY", 8),
		CaptureModeAllowlist:        strEnv("CAPTURE_MODE_ALLOWLIST", ""),
		CaptureLeaseSec:             intEnv("CAPTURE_LEASE_SEC", 30),
		CaptureUnsupportedThreshold: intEnv("CAPTURE_UNSUPPORTED_THRESHOLD", 8),
		CaptureFrameQueueSize:       intEnv("CAPTURE_FRAME_QUEUE_SIZE", 16),
		CaptureFrameEnqueueTimeout:  intEnv("CAPTURE_FRAME_ENQUEUE_TIMEOUT_SEC", 3),
		CaptureFrameWriters:         intEnv("CAPTURE_FRAME_WRITERS", 1),
		InferenceBoxPollSec:         intEnv("BOX_WORKER_POLL_SEC", 2),
		InferenceBoxConcurrency:     intEnv("BOX_WORKER_CONCURRENCY", 2),
		InferenceBoxLeaseSec:        intEnv("BOX_WORKER_LEASE_SEC", 300),
		InferenceBoxMaxAttempts:     intEnv("BOX_WORKER_MAX_ATTEMPTS", 8),
		InferenceBoxRetryBaseSec:    intEnv("BOX_WORKER_RETRY_BASE_SEC", 5),
		InferenceBoxRetryMaxSec:     intEnv("BOX_WORKER_RETRY_MAX_SEC", 300),
		BoxWorkerEmbedded:           boolEnv("BOX_WORKER_EMBEDDED", false),
		WorkerID:                    strEnv("WORKER_ID", "capture-worker-1"),
		SurveyEnabled:               boolEnv("SURVEY_ENABLED", false),
		SurveyConcurrency:           intEnv("SURVEY_CONCURRENCY", 4),
		SurveyResolveTimeoutSec:     intEnv("SURVEY_RESOLVE_TIMEOUT_SEC", 60),
		SurveyCaptureTimeoutSec:     intEnv("SURVEY_CAPTURE_TIMEOUT_SEC", 60),

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

		RecordingWorkerConcurrency:  intEnv("RECORDING_WORKER_CONCURRENCY", 1),
		RecordingWorkerHeartbeatSec: intEnv("RECORDING_WORKER_HEARTBEAT_SEC", 15),
		RecordingWorkerPollSec:      intEnv("RECORDING_WORKER_POLL_SEC", 5),

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
	return cfg, nil
}

func (c Config) ValidateAPI() error {
	if err := c.ValidateR2(); err != nil {
		return err
	}
	return c.ValidateStripe()
}

// ValidateStripe asserts the configured mode matches the secret-key prefix, so a
// live key with STRIPE_LIVEMODE=false (which would make the webhook reject every
// live event with 400 and silently leave all accounts unbillable) refuses to
// start instead of failing silently in production. Billing is optional: an empty
// or non-sk_ key skips the check (the server boots in free mode).
func (c Config) ValidateStripe() error {
	key := strings.TrimSpace(c.StripeSecretKey)
	if strings.HasPrefix(key, "sk_live_") && !c.StripeLivemode {
		return fmt.Errorf("STRIPE_SECRET_KEY is a live key but STRIPE_LIVEMODE is false; set STRIPE_LIVEMODE=true")
	}
	if strings.HasPrefix(key, "sk_test_") && c.StripeLivemode {
		return fmt.Errorf("STRIPE_SECRET_KEY is a test key but STRIPE_LIVEMODE is true; set STRIPE_LIVEMODE=false")
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
