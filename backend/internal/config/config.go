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
