package config

import (
	"crypto/sha256"
	"encoding/hex"
	"net/netip"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

func completeStripeConfig(livemode bool) Config {
	key := "sk_test_example"
	meterPrefix := "mtr_test_"
	if livemode {
		key = "sk_live_example"
		meterPrefix = "mtr_"
	}
	return Config{
		StripeSecretKey:      key,
		StripeWebhookSecret:  "whsec_example",
		StripePriceID:        "price_recording",
		StripeMeterID:        meterPrefix + "recording",
		StripeGBMonthPriceID: "price_storage",
		StripeGBMonthMeterID: meterPrefix + "storage",
		StripeLivemode:       livemode,
	}
}

func joinedCanaryScope(batch string) string {
	return strings.Join([]string{
		batch + "__recording-377__date-2026-08-01__hour-01__generation-1",
		batch + "__recording-335__date-2026-08-01__hour-02__generation-1",
		batch + "__recording-337__date-2026-08-01__hour-03__generation-1",
	}, ",")
}

func TestJoinedBatchIDGrammar(t *testing.T) {
	for _, value := range []string{"a", "batch-", strings.Repeat("a", 63)} {
		if !validJoinedRecordingBatchID(value) {
			t.Fatalf("valid joined batch ID rejected: %q", value)
		}
	}
	for _, value := range []string{"", "-batch", "Batch", "batch_name", strings.Repeat("a", 64)} {
		if validJoinedRecordingBatchID(value) {
			t.Fatalf("invalid joined batch ID accepted: %q", value)
		}
	}
}

func TestValidateStripeFailsClosed(t *testing.T) {
	if err := (Config{}).ValidateStripe(); err != nil {
		t.Fatalf("empty optional config: %v", err)
	}
	for _, livemode := range []bool{false, true} {
		cfg := completeStripeConfig(livemode)
		if err := cfg.ValidateStripe(); err != nil {
			t.Fatalf("valid livemode=%v config: %v", livemode, err)
		}
		if !cfg.StripeBillingEnabled() {
			t.Fatalf("complete livemode=%v config reported disabled", livemode)
		}
	}

	tests := []struct {
		name string
		edit func(*Config)
	}{
		{"partial", func(c *Config) { c.StripeGBMonthMeterID = "" }},
		{"live key in test mode", func(c *Config) { c.StripeSecretKey = "sk_live_wrong" }},
		{"test meter in live mode", func(c *Config) { c.StripeLivemode = true; c.StripeSecretKey = "sk_live_ok" }},
		{"bad webhook", func(c *Config) { c.StripeWebhookSecret = "secret" }},
		{"bad price", func(c *Config) { c.StripePriceID = "prod_wrong" }},
		{"bad meter", func(c *Config) { c.StripeMeterID = "meter_wrong" }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := completeStripeConfig(false)
			tc.edit(&cfg)
			if err := cfg.ValidateStripe(); err == nil {
				t.Fatal("invalid Stripe configuration was accepted")
			}
		})
	}
	if err := (Config{StripeLivemode: true}).ValidateStripe(); err == nil {
		t.Fatal("livemode without Stripe objects was accepted")
	}
}

func TestValidateJoinedCredentialsFailStartupOnAliasOrPartialConfig(t *testing.T) {
	if err := (Config{}).ValidateJoined(); err != nil {
		t.Fatalf("dormant unconfigured joined release: %v", err)
	}
	const bootstrap = "joined-bootstrap-credential-32bytes"
	const signing = "joined-signing-credential-32-bytes"
	const batch = "tier1-2026-08"
	hour := joinedCanaryScope(batch)
	valid := Config{JoinedRecordingControlPlaneEnabled: true, JoinedRecordingProtocolVersion: 1,
		JoinedRecordingConnectionID: 77, JoinedRecordingProtocolGeneration: 1,
		JoinedRecordingBatchID: batch, JoinedRecordingCanaryHourIDs: hour, ServiceToken: "service",
		JoinedRecordingWorkScope:   JoinedWorkScopeCanary,
		JoinedWorkerBootstrapToken: bootstrap, JoinedWorkerSigningKey: signing}
	if err := valid.ValidateJoined(); err != nil {
		t.Fatalf("valid joined credentials: %v", err)
	}
	for _, cfg := range []Config{
		{JoinedRecordingControlPlaneEnabled: true, ServiceToken: "service", JoinedWorkerBootstrapToken: "", JoinedWorkerSigningKey: signing},
		{JoinedRecordingControlPlaneEnabled: true, ServiceToken: bootstrap, JoinedWorkerBootstrapToken: bootstrap, JoinedWorkerSigningKey: signing},
		{JoinedRecordingControlPlaneEnabled: true, ServiceToken: "service", JoinedWorkerBootstrapToken: bootstrap, JoinedWorkerSigningKey: bootstrap},
		{JoinedRecordingControlPlaneEnabled: true, DatabaseURL: "postgres://user:" + signing + "@db.example.test/db", JoinedWorkerBootstrapToken: bootstrap, JoinedWorkerSigningKey: signing},
		{JoinedRecordingControlPlaneEnabled: true, DatabaseURL: "host=db.example.test user=test password='" + signing + "'", JoinedWorkerBootstrapToken: bootstrap, JoinedWorkerSigningKey: signing},
		{JoinedRecordingControlPlaneEnabled: true, R2SecretAccessKey: bootstrap, JoinedWorkerBootstrapToken: bootstrap, JoinedWorkerSigningKey: signing},
		{JoinedRecordingControlPlaneEnabled: true, R2AccessKeyID: "  " + bootstrap + "  ", JoinedWorkerBootstrapToken: bootstrap, JoinedWorkerSigningKey: signing},
		{JoinedRecordingControlPlaneEnabled: true, R2SecretAccessKey: "  " + signing + "  ", JoinedWorkerBootstrapToken: bootstrap, JoinedWorkerSigningKey: signing},
		{JoinedRecordingControlPlaneEnabled: true, JoinedWorkerBootstrapToken: strings.Repeat("b", 31), JoinedWorkerSigningKey: signing},
		{JoinedRecordingControlPlaneEnabled: true, JoinedWorkerBootstrapToken: bootstrap, JoinedWorkerSigningKey: strings.Repeat("s", 31)},
	} {
		if err := cfg.ValidateJoined(); err == nil {
			t.Fatalf("unsafe joined credential configuration accepted: %+v", cfg)
		}
	}
}

func TestJoinedWorkerBootstrapSHA256AllowlistFailsClosed(t *testing.T) {
	t.Parallel()
	const bootstrap = "joined-bootstrap-credential-32bytes"
	const signing = "joined-signing-credential-32-bytes"
	workerDigest := sha256.Sum256([]byte("dedicated-worker-token-32-bytes-0001"))
	valid := Config{
		JoinedWorkerBootstrapToken:  bootstrap,
		JoinedWorkerBootstrapHashes: hex.EncodeToString(workerDigest[:]),
		JoinedWorkerSigningKey:      signing,
	}
	if err := valid.ValidateJoined(); err != nil {
		t.Fatalf("valid joined worker digest allowlist: %v", err)
	}
	digests, err := valid.JoinedWorkerBootstrapSHA256s()
	if err != nil || len(digests) != 1 || digests[0] != workerDigest {
		t.Fatalf("parsed worker digests=%x err=%v", digests, err)
	}

	protected := sha256.Sum256([]byte(signing))
	for _, raw := range []string{
		"abc",
		strings.Repeat("A", sha256.Size*2),
		hex.EncodeToString(workerDigest[:]) + ", " + strings.Repeat("a", sha256.Size*2),
		hex.EncodeToString(workerDigest[:]) + "," + hex.EncodeToString(workerDigest[:]),
		hex.EncodeToString(protected[:]),
		strings.Repeat("a", sha256.Size*2) + ",",
		strings.Repeat("a", sha256.Size*2+1),
		strings.TrimSuffix(strings.Repeat(strings.Repeat("a", sha256.Size*2)+",", 65), ","),
	} {
		candidate := valid
		candidate.JoinedWorkerBootstrapHashes = raw
		if err := candidate.ValidateJoined(); err == nil {
			t.Fatalf("unsafe joined worker digest allowlist accepted: %q", raw)
		}
	}

	for name, credential := range map[string]string{
		"legacy bootstrap": bootstrap,
		"service":          "generic-service-key",
		"operator":         "joined-operator-token-32-bytes-0001",
		"storage":          "joined-storage-token-32-bytes-0001",
	} {
		t.Run("protected "+name, func(t *testing.T) {
			candidate := valid
			candidate.ServiceToken = "generic-service-key"
			candidate.JoinedOperatorToken = "joined-operator-token-32-bytes-0001"
			candidate.R2SecretAccessKey = "joined-storage-token-32-bytes-0001"
			digest := sha256.Sum256([]byte(credential))
			candidate.JoinedWorkerBootstrapHashes = hex.EncodeToString(digest[:])
			if err := candidate.ValidateJoined(); err == nil {
				t.Fatalf("%s credential hash accepted as joined worker authority", name)
			}
		})
	}
}

func TestRenderServicesDeclareIdenticalStripeVariables(t *testing.T) {
	renderPath := filepath.Join("..", "..", "..", "render.yaml")
	data, err := os.ReadFile(renderPath)
	if err != nil {
		t.Fatal(err)
	}
	serviceKeys := func(name string) []string {
		marker := "name: " + name
		start := strings.Index(string(data), marker)
		if start < 0 {
			t.Fatalf("service %s not found in render.yaml", name)
		}
		section := string(data)[start:]
		if next := strings.Index(section[len(marker):], "\n  - type:"); next >= 0 {
			section = section[:len(marker)+next]
		}
		re := regexp.MustCompile(`(?m)^\s+- key: (STRIPE_[A-Z_]+)\s*$`)
		keys := make([]string, 0)
		for _, match := range re.FindAllStringSubmatch(section, -1) {
			keys = append(keys, match[1])
		}
		sort.Strings(keys)
		return keys
	}
	want := []string{
		"STRIPE_GB_MONTH_METER_ID", "STRIPE_GB_MONTH_PRICE_ID", "STRIPE_LIVEMODE",
		"STRIPE_METER_ID", "STRIPE_PRICE_ID", "STRIPE_SECRET_KEY", "STRIPE_WEBHOOK_SECRET",
	}
	apiKeys := serviceKeys("stoarama-api")
	controllerKeys := serviceKeys("stoarama-recorder-control")
	if !reflect.DeepEqual(apiKeys, want) || !reflect.DeepEqual(controllerKeys, want) {
		t.Fatalf("Stripe env drift: api=%v controller=%v want=%v", apiKeys, controllerKeys, want)
	}
}

func TestRenderJoinedWorkScopesSeparateActiveAPIFromDisabledWorker(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "..", "render.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	serviceSection := func(name string) string {
		marker := "name: " + name
		start := strings.Index(string(data), marker)
		if start < 0 {
			t.Fatalf("service %s not found", name)
		}
		section := string(data)[start:]
		if next := strings.Index(section[len(marker):], "\n  - type:"); next >= 0 {
			section = section[:len(marker)+next]
		}
		return section
	}
	worker, api := serviceSection("stoarama-joined-worker"), serviceSection("stoarama-api")
	if !strings.Contains(worker, "- key: STOARAMA_JOINED_WORK_SCOPE\n        value: disabled") ||
		strings.Contains(worker, "- key: STOARAMA_JOINED_WORK_SCOPE\n        value: frozen_batch") {
		t.Fatal("joined worker must remain disabled")
	}
	if !strings.Contains(api, "- key: STOARAMA_JOINED_WORK_SCOPE\n        value: frozen_batch") ||
		strings.Contains(api, "- key: STOARAMA_JOINED_WORK_SCOPE\n        value: disabled") {
		t.Fatal("joined API must remain scoped to the frozen batch")
	}
}

func TestJoinedRecordingDefaultsShipDark(t *testing.T) {
	for _, key := range []string{
		"JOINED_RECORDING_CONTROL_PLANE_ENABLED",
		"JOINED_RECORDING_NAS_DELIVERY_ENABLED",
		"JOINED_RECORDING_ENABLED",
		"JOINED_RECORDING_PROTOCOL_VERSION",
		"JOINED_RECORDING_CONNECTION_ID",
		"JOINED_RECORDING_PROTOCOL_GENERATION",
		"JOINED_RECORDING_ROLLING_ENABLED",
		"STOARAMA_JOINED_WORK_SCOPE",
		"JOINED_RECORDING_BATCH_ID",
		"JOINED_RECORDING_CANARY_HOUR_IDS",
		"JOINED_RECORDING_SCRATCH_ROOT",
		"JOINED_RECORDING_STORAGE_AUTHORITY",
		"JOINED_RECORDING_FFMPEG_ARCHIVE_URL",
		"JOINED_RECORDING_FFMPEG_ARCHIVE_SHA256",
		"JOINED_RECORDING_FFMPEG_BINARY_SHA256",
		"JOINED_RECORDING_FFPROBE_BINARY_SHA256",
		"STOARAMA_JOINED_WORKER_TOKEN",
		"JOINED_WORKER_BOOTSTRAP_TOKEN",
		"JOINED_WORKER_BOOTSTRAP_TOKEN_SHA256_ALLOWLIST",
		"JOINED_WORKER_SIGNING_KEY",
		"STOARAMA_JOINED_OPERATOR_TOKEN",
	} {
		t.Setenv(key, "")
	}
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.JoinedRecordingControlPlaneEnabled || cfg.JoinedRecordingNASDeliveryEnabled || cfg.JoinedRecordingEnabled || cfg.JoinedRecordingRollingEnabled || cfg.JoinedRecordingProtocolVersion != 0 {
		t.Fatalf("joined worker unexpectedly enabled: %+v", cfg)
	}
	if cfg.JoinedRecordingConnectionID != 0 || cfg.JoinedRecordingProtocolGeneration != 0 {
		t.Fatalf("joined remote protocol unexpectedly targeted: %+v", cfg)
	}
	if cfg.JoinedRecordingBatchID != "" || cfg.JoinedRecordingScratchRoot != "/tmp/stoarama-joined" {
		t.Fatalf("joined defaults: batch=%q scratch=%q", cfg.JoinedRecordingBatchID, cfg.JoinedRecordingScratchRoot)
	}
	if cfg.JoinedRecordingWorkScope != JoinedWorkScopeDisabled {
		t.Fatalf("joined default work scope=%q", cfg.JoinedRecordingWorkScope)
	}
	if err := cfg.ValidateJoinedRecording(); err != nil {
		t.Fatalf("disabled validation: %v", err)
	}
}

func TestJoinedNASDeliveryRequiresReadyControlPlane(t *testing.T) {
	cfg := validJoinedAPIConfigForTest("tier1-2026-08", joinedCanaryScope("tier1-2026-08"))
	cfg.JoinedRecordingNASDeliveryEnabled = true
	if err := cfg.ValidateJoined(); err != nil {
		t.Fatal(err)
	}
	cfg.JoinedRecordingControlPlaneEnabled = false
	if err := cfg.ValidateJoined(); err == nil {
		t.Fatal("NAS joined delivery enabled without control plane")
	}
}

func TestValidateJoinedRecordingActivation(t *testing.T) {
	valid := Config{
		JoinedRecordingEnabled:             true,
		JoinedRecordingProtocolVersion:     1,
		JoinedRecordingWorkScope:           JoinedWorkScopeCanary,
		JoinedRecordingBatchID:             "tier1-2026-08",
		JoinedRecordingCanaryHourIDs:       joinedCanaryScope("tier1-2026-08"),
		JoinedRecordingScratchRoot:         "/tmp/stoarama-joined",
		JoinedRecordingStorageAuthority:    "example.r2.cloudflarestorage.com",
		JoinedRecordingFFmpegArchiveURL:    "https://example.com/ffmpeg/7.1.1/linux64.tar.xz",
		JoinedRecordingFFmpegArchiveSHA256: strings.Repeat("a", 64),
		JoinedRecordingFFmpegSHA256:        strings.Repeat("b", 64),
		JoinedRecordingFFprobeSHA256:       strings.Repeat("c", 64),
		JoinedRecordingWorkerToken:         strings.Repeat("w", 32),
	}
	if err := valid.ValidateJoinedRecording(); err != nil {
		t.Fatal(err)
	}
	frozenBatch := valid
	frozenBatch.JoinedRecordingWorkScope = JoinedWorkScopeFrozenBatch
	frozenBatch.JoinedRecordingCanaryHourIDs = ""
	if err := frozenBatch.ValidateJoinedRecording(); err != nil {
		t.Fatalf("frozen-batch worker activation: %v", err)
	}
	tests := []Config{
		{JoinedRecordingRollingEnabled: true},
		{JoinedRecordingEnabled: true, JoinedRecordingScratchRoot: "/tmp/stoarama-joined"},
		{JoinedRecordingEnabled: true, JoinedRecordingBatchID: "Tier1", JoinedRecordingScratchRoot: "/tmp/stoarama-joined"},
		{JoinedRecordingEnabled: true, JoinedRecordingBatchID: "tier1", JoinedRecordingScratchRoot: "relative"},
		{JoinedRecordingEnabled: true, JoinedRecordingBatchID: "tier1", JoinedRecordingScratchRoot: "/tmp/../joined"},
	}
	for i, cfg := range tests {
		if err := cfg.ValidateJoinedRecording(); err == nil {
			t.Fatalf("invalid config %d accepted: %+v", i, cfg)
		}
	}
	latest := valid
	latest.JoinedRecordingFFmpegArchiveURL = "https://example.com/ffmpeg/latest/linux64.tar.xz"
	if err := latest.ValidateJoinedRecording(); err == nil {
		t.Fatal("mutable latest archive URL was accepted")
	}
	missingBootstrap := valid
	missingBootstrap.JoinedRecordingWorkerToken = ""
	if err := missingBootstrap.ValidateJoinedRecording(); err == nil || !strings.Contains(err.Error(), "STOARAMA_JOINED_WORKER_TOKEN") {
		t.Fatalf("missing bootstrap error=%v", err)
	}
	shortBootstrap := valid
	shortBootstrap.JoinedRecordingWorkerToken = strings.Repeat("w", 31)
	if err := shortBootstrap.ValidateJoinedRecording(); err == nil || !strings.Contains(err.Error(), "at least 32 bytes") {
		t.Fatalf("short bootstrap error=%v", err)
	}
	trimmedBootstrap := valid
	trimmedBootstrap.JoinedRecordingWorkerToken = " \t" + strings.Repeat("w", 32) + "\n"
	if err := trimmedBootstrap.ValidateJoinedRecording(); err != nil {
		t.Fatalf("trimmed 32-byte bootstrap: %v", err)
	}
}

func TestJoinedCanaryScopeAndRoleSeparationFailClosed(t *testing.T) {
	const batch = "tier1-2026-08"
	hour := joinedCanaryScope(batch)
	hours := strings.Split(hour, ",")
	base := Config{JoinedRecordingProtocolVersion: 1, JoinedRecordingBatchID: batch,
		JoinedRecordingCanaryHourIDs: hour, JoinedRecordingWorkScope: JoinedWorkScopeCanary}
	invalidScopes := map[string]string{
		"empty":        "",
		"one hour":     hours[0],
		"two hours":    strings.Join(hours[:2], ","),
		"four hours":   hour + "," + batch + "__recording-355__date-2026-08-01__hour-04__generation-1",
		"malformed":    strings.Join([]string{batch + "__recording-0377__date-2026-08-01__hour-01__generation-1", hours[1], hours[2]}, ","),
		"invalid date": strings.Join([]string{batch + "__recording-377__date-2026-02-30__hour-01__generation-1", hours[1], hours[2]}, ","),
		"wrong batch":  strings.Join([]string{"other-batch__recording-377__date-2026-08-01__hour-01__generation-1", hours[1], hours[2]}, ","),
		"duplicate":    strings.Join([]string{hours[0], hours[0], hours[2]}, ","),
	}
	for name, scope := range invalidScopes {
		t.Run(name, func(t *testing.T) {
			cfg := base
			cfg.JoinedRecordingCanaryHourIDs = scope
			if _, err := cfg.JoinedCanaryHourIDs(); err == nil {
				t.Fatalf("unsafe canary scope accepted: %q", scope)
			}
		})
	}

	worker := base
	worker.JoinedRecordingEnabled = true
	worker.JoinedRecordingScratchRoot = "/tmp/stoarama-joined"
	worker.JoinedRecordingStorageAuthority = "example.r2.cloudflarestorage.com"
	worker.JoinedRecordingFFmpegArchiveURL = "https://example.com/ffmpeg/7.1.1/linux64.tar.xz"
	worker.JoinedRecordingFFmpegArchiveSHA256 = strings.Repeat("a", 64)
	worker.JoinedRecordingFFmpegSHA256 = strings.Repeat("b", 64)
	worker.JoinedRecordingFFprobeSHA256 = strings.Repeat("c", 64)
	worker.JoinedRecordingWorkerToken = "joined-worker-bootstrap-token-32bytes"
	if worker.JoinedWorkerBootstrapToken != "" || worker.JoinedWorkerSigningKey != "" || worker.ValidateJoined() != nil || worker.ValidateJoinedRecording() != nil {
		t.Fatal("worker activation incorrectly requires or receives backend signing authority")
	}
	for name, scope := range invalidScopes {
		t.Run("activation "+name, func(t *testing.T) {
			api := validJoinedAPIConfigForTest(batch, scope)
			badWorker := worker
			badWorker.JoinedRecordingCanaryHourIDs = scope
			if api.ValidateJoined() == nil || badWorker.ValidateJoinedRecording() == nil {
				t.Fatalf("API or worker activated with unsafe canary scope %q", scope)
			}
		})
	}

	api := base
	api.JoinedRecordingControlPlaneEnabled = true
	if err := api.ValidateJoined(); err == nil {
		t.Fatal("enabled API accepted missing bootstrap and signing credentials")
	}

	for name, token := range map[string]string{
		"short":             "short",
		"backend bootstrap": "joined-bootstrap-credential-32bytes",
		"backend signing":   "joined-signing-credential-32-bytes",
		"worker":            worker.JoinedRecordingWorkerToken,
	} {
		t.Run("operator "+name, func(t *testing.T) {
			cfg := validJoinedAPIConfigForTest(batch, hour)
			cfg.JoinedRecordingWorkerToken = worker.JoinedRecordingWorkerToken
			cfg.JoinedOperatorToken = token
			if err := cfg.ValidateJoined(); err == nil {
				t.Fatalf("unsafe operator token accepted: %s", name)
			}
		})
	}
	if err := (Config{JoinedOperatorToken: strings.Repeat("o", 32)}).ValidateJoined(); err != nil {
		t.Fatalf("standalone operator credential: %v", err)
	}
}

func TestJoinedWorkScopeIsExplicitAndBounded(t *testing.T) {
	const batch = "tier1-2026-08"
	canary := validJoinedAPIConfigForTest(batch, joinedCanaryScope(batch))
	if scope, err := canary.JoinedWorkScope(); err != nil || scope != JoinedWorkScopeCanary {
		t.Fatalf("canary scope=%q err=%v", scope, err)
	}
	frozen := canary
	frozen.JoinedRecordingWorkScope = JoinedWorkScopeFrozenBatch
	frozen.JoinedRecordingCanaryHourIDs = ""
	if frozen.ValidateJoined() == nil {
		t.Fatal("frozen-batch API accepted no active-task cap")
	}
	frozen.JoinedRecordingMaxActiveTasks = 2
	frozen.JoinedRecordingFrozenExcludedPublicationArtifactIDs = "470,468,469"
	if scope, err := frozen.JoinedWorkScope(); err != nil || scope != JoinedWorkScopeFrozenBatch {
		t.Fatalf("frozen scope=%q err=%v", scope, err)
	}
	if ids, err := frozen.JoinedFrozenExcludedPublicationArtifactIDs(); err != nil || !slices.Equal(ids, []int64{468, 469, 470}) {
		t.Fatalf("frozen publication deny set=%v err=%v", ids, err)
	}
	for name, raw := range map[string]string{
		"missing": "", "partial": "468,469", "extra": "468,469,470,471", "duplicate": "468,469,469",
		"zero": "0,468,469", "negative": "-1,468,469", "overflow": "468,469,9223372036854775808",
		"junk": "468,469,nope", "wrong": "468,469,471",
	} {
		t.Run("frozen publication deny "+name, func(t *testing.T) {
			candidate := frozen
			candidate.JoinedRecordingFrozenExcludedPublicationArtifactIDs = raw
			if err := candidate.ValidateJoined(); err == nil {
				t.Fatalf("invalid frozen publication deny set %q accepted", raw)
			}
		})
	}
	canary.JoinedRecordingFrozenExcludedPublicationArtifactIDs = "junk"
	if err := canary.ValidateJoined(); err != nil {
		t.Fatalf("non-frozen scope used publication deny set: %v", err)
	}
	for name, edit := range map[string]func(*Config){
		"implicit enabled": func(c *Config) { c.JoinedRecordingWorkScope = "" },
		"disabled enabled": func(c *Config) { c.JoinedRecordingWorkScope = JoinedWorkScopeDisabled },
		"frozen canary":    func(c *Config) { c.JoinedRecordingWorkScope = JoinedWorkScopeFrozenBatch },
		"unknown":          func(c *Config) { c.JoinedRecordingWorkScope = "rolling" },
		"rolling":          func(c *Config) { c.JoinedRecordingRollingEnabled = true },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := canary
			edit(&candidate)
			if err := candidate.ValidateJoined(); err == nil {
				t.Fatal("unsafe joined work scope was accepted")
			}
		})
	}
	if scope, err := (Config{}).JoinedWorkScope(); err != nil || scope != JoinedWorkScopeDisabled {
		t.Fatalf("dormant default scope=%q err=%v", scope, err)
	}
	if scope, err := (Config{JoinedRecordingCanaryHourIDs: joinedCanaryScope(batch)}).JoinedWorkScope(); err != nil || scope != JoinedWorkScopeDisabled {
		t.Fatalf("disabled scope was affected by inert canary IDs scope=%q err=%v", scope, err)
	}
	single := canary
	single.JoinedRecordingWorkScope = JoinedWorkScopeSingleCanary
	single.JoinedRecordingCanaryHourIDs = strings.Split(canary.JoinedRecordingCanaryHourIDs, ",")[0]
	if scope, err := single.JoinedWorkScope(); err != nil || scope != JoinedWorkScopeSingleCanary {
		t.Fatalf("single-canary scope=%q err=%v", scope, err)
	}
	single.JoinedRecordingCanaryHourIDs = canary.JoinedRecordingCanaryHourIDs
	if _, err := single.JoinedWorkScope(); err == nil {
		t.Fatal("single-canary scope accepted more than one hour")
	}
	allowlist := canary
	allowlist.JoinedRecordingWorkScope = JoinedWorkScopeAllowlist50
	allowlist.JoinedRecordingCanaryHourIDs = joinedAllowlistHours(batch, 50)
	allowlist.JoinedRecordingMaxActiveTasks = 2
	if scope, err := allowlist.JoinedWorkScope(); err != nil || scope != JoinedWorkScopeAllowlist50 || allowlist.ValidateJoined() != nil {
		t.Fatalf("allowlist scope=%q err=%v", scope, err)
	}
	for _, count := range []int{49, 51} {
		candidate := allowlist
		candidate.JoinedRecordingCanaryHourIDs = joinedAllowlistHours(batch, count)
		if candidate.ValidateJoined() == nil {
			t.Fatalf("allowlist accepted %d hours", count)
		}
	}
	duplicate := allowlist
	duplicateHours := strings.Split(duplicate.JoinedRecordingCanaryHourIDs, ",")
	duplicateHours[49] = duplicateHours[0]
	duplicate.JoinedRecordingCanaryHourIDs = strings.Join(duplicateHours, ",")
	if duplicate.ValidateJoined() == nil {
		t.Fatal("allowlist accepted a duplicate canonical hour")
	}
	allowlist.JoinedRecordingMaxActiveTasks = 1
	if allowlist.ValidateJoined() == nil {
		t.Fatal("allowlist accepted a non-two-worker active-task cap")
	}
}

func joinedAllowlistHours(batch string, count int) string {
	hours := make([]string, count)
	for i := range hours {
		hours[i] = batch + "__recording-" + strconv.Itoa(i+1) + "__date-2026-08-01__hour-01__generation-1"
	}
	return strings.Join(hours, ",")
}

func validJoinedAPIConfigForTest(batch, hour string) Config {
	return Config{JoinedRecordingControlPlaneEnabled: true, JoinedRecordingProtocolVersion: 1,
		JoinedRecordingConnectionID: 77, JoinedRecordingProtocolGeneration: 1,
		JoinedRecordingBatchID: batch, JoinedRecordingCanaryHourIDs: hour, JoinedRecordingWorkScope: JoinedWorkScopeCanary,
		JoinedWorkerBootstrapToken: "joined-bootstrap-credential-32bytes",
		JoinedWorkerSigningKey:     "joined-signing-credential-32-bytes"}
}

func TestLoadAllowsWorkerWithoutBackendAuthority(t *testing.T) {
	for key, value := range map[string]string{
		"JOINED_RECORDING_CONTROL_PLANE_ENABLED": "false",
		"JOINED_RECORDING_ENABLED":               "true",
		"JOINED_RECORDING_PROTOCOL_VERSION":      "1",
		"JOINED_RECORDING_BATCH_ID":              "tier1-2026-08",
		"JOINED_RECORDING_CANARY_HOUR_IDS":       joinedCanaryScope("tier1-2026-08"),
		"STOARAMA_JOINED_WORK_SCOPE":             JoinedWorkScopeCanary,
		"JOINED_WORKER_BOOTSTRAP_TOKEN":          "",
		"JOINED_WORKER_SIGNING_KEY":              "",
		"STOARAMA_JOINED_OPERATOR_TOKEN":         "",
		"STOARAMA_JOINED_WORKER_TOKEN":           "joined-worker-bootstrap-token-32bytes",
		"JOINED_RECORDING_STORAGE_AUTHORITY":     "example.r2.cloudflarestorage.com",
		"JOINED_RECORDING_FFMPEG_ARCHIVE_URL":    "https://example.com/ffmpeg/7.1.1/linux64.tar.xz",
		"JOINED_RECORDING_FFMPEG_ARCHIVE_SHA256": strings.Repeat("a", 64),
		"JOINED_RECORDING_FFMPEG_BINARY_SHA256":  strings.Repeat("b", 64),
		"JOINED_RECORDING_FFPROBE_BINARY_SHA256": strings.Repeat("c", 64),
	} {
		t.Setenv(key, value)
	}
	cfg, err := Load()
	if err != nil || cfg.JoinedWorkerBootstrapToken != "" || cfg.JoinedWorkerSigningKey != "" {
		t.Fatalf("isolated worker load err=%v bootstrap=%t signing=%t", err,
			cfg.JoinedWorkerBootstrapToken != "", cfg.JoinedWorkerSigningKey != "")
	}
	if err := cfg.ValidateJoinedRecording(); err != nil {
		t.Fatalf("isolated worker activation: %v", err)
	}
}

func TestRenderJoinedWorkerIsFixedDormantAndUnprivileged(t *testing.T) {
	renderPath := filepath.Join("..", "..", "..", "render.yaml")
	data, err := os.ReadFile(renderPath)
	if err != nil {
		t.Fatal(err)
	}
	const marker = "name: stoarama-joined-worker"
	if strings.Count(string(data), marker) != 1 {
		t.Fatalf("joined worker declarations=%d want 1", strings.Count(string(data), marker))
	}
	section := string(data)[strings.Index(string(data), marker):]
	if next := strings.Index(section[len(marker):], "\n  - type:"); next >= 0 {
		section = section[:len(marker)+next]
	}
	for _, required := range []string{
		"numInstances: 1",
		"startCommand: ./bin/stoaramactl recording-joined worker run",
		"bash ./scripts/install-pinned-media-tools.sh optional",
		"key: JOINED_RECORDING_ENABLED\n        value: \"false\"",
		"key: JOINED_RECORDING_PROTOCOL_VERSION\n        value: \"0\"",
		"key: JOINED_RECORDING_ROLLING_ENABLED\n        value: \"false\"",
		"key: JOINED_RECORDING_CANARY_HOUR_IDS\n        sync: false",
		"key: FFMPEG_BIN\n        value: ./bin/ffmpeg",
		"key: FFPROBE_BIN\n        value: ./bin/ffprobe",
		"key: STOARAMA_JOINED_WORKER_TOKEN\n        sync: false",
	} {
		if !strings.Contains(section, required) {
			t.Fatalf("joined worker missing %q", required)
		}
	}
	for _, forbidden := range []string{"DATABASE_URL", "R2_", "STORAGE_CRED_KEY", "SERVICE_TOKEN", "JOINED_WORKER_SIGNING_KEY", "JOINED_WORKER_BOOTSTRAP_TOKEN", "STOARAMA_JOINED_OPERATOR_TOKEN"} {
		if strings.Contains(section, forbidden) {
			t.Fatalf("joined worker contains privileged setting %q", forbidden)
		}
	}
}

func TestRenderJoinedControlPlaneIsActiveFrozenBatchAndScoped(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "..", "render.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	marker := "name: stoarama-api"
	section := string(data)[strings.Index(string(data), marker):]
	if next := strings.Index(section[len(marker):], "\n  - type:"); next >= 0 {
		section = section[:len(marker)+next]
	}
	for _, required := range []string{
		"key: JOINED_RECORDING_CONTROL_PLANE_ENABLED\n        value: \"true\"",
		"key: JOINED_RECORDING_NAS_DELIVERY_ENABLED\n        value: \"false\"",
		"key: JOINED_RECORDING_PROTOCOL_VERSION\n        value: \"1\"",
		"key: JOINED_RECORDING_CONNECTION_ID\n        value: \"13\"",
		"key: JOINED_RECORDING_PROTOCOL_GENERATION\n        value: \"7\"",
		"key: JOINED_RECORDING_MAX_ACTIVE_TASKS\n        value: \"3\"",
		"key: STOARAMA_JOINED_WORK_SCOPE\n        value: frozen_batch",
		"key: JOINED_RECORDING_BATCH_ID\n        sync: false",
		"key: JOINED_RECORDING_CANARY_HOUR_IDS\n        sync: false",
		"key: JOINED_WORKER_BOOTSTRAP_TOKEN\n        sync: false",
		"key: JOINED_WORKER_BOOTSTRAP_TOKEN_SHA256_ALLOWLIST\n        sync: false",
		"key: JOINED_WORKER_SIGNING_KEY\n        sync: false",
	} {
		if !strings.Contains(section, required) {
			t.Fatalf("joined API missing %q", required)
		}
	}
	if strings.Contains(section, "key: JOINED_RECORDING_NAS_DELIVERY_ENABLED\n        value: \"true\"") {
		t.Fatal("joined NAS delivery was enabled in source configuration")
	}
	if strings.Contains(section, "STOARAMA_JOINED_OPERATOR_TOKEN") {
		t.Fatal("joined operator credential must not be deployed to the API")
	}
}

func TestRenderAPIAndJoinedWorkerSharePinnedMediaTools(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "..", "render.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	service := func(name string) string {
		marker := "name: " + name
		start := strings.Index(string(data), marker)
		if start < 0 {
			t.Fatalf("service %s not found", name)
		}
		section := string(data)[start:]
		if next := strings.Index(section[len(marker):], "\n  - type:"); next >= 0 {
			section = section[:len(marker)+next]
		}
		return section
	}
	api, worker := service("stoarama-api"), service("stoarama-joined-worker")
	if strings.Contains(api, "install-pinned-media-tools.sh optional") || !strings.Contains(worker, "install-pinned-media-tools.sh optional") {
		t.Fatal("API must require pinned tools while the ship-dark worker may omit them")
	}
	for _, section := range []string{api, worker} {
		for _, required := range []string{
			"scripts/install-pinned-media-tools.sh",
			"key: JOINED_RECORDING_FFMPEG_ARCHIVE_URL",
			"key: JOINED_RECORDING_FFMPEG_ARCHIVE_SHA256",
			"key: JOINED_RECORDING_FFMPEG_BINARY_SHA256",
			"key: JOINED_RECORDING_FFPROBE_BINARY_SHA256",
			"key: FFMPEG_BIN\n        value: ./bin/ffmpeg",
			"key: FFPROBE_BIN\n        value: ./bin/ffprobe",
		} {
			if strings.Count(section, required) != 1 {
				t.Fatalf("service media-tool declaration %q count=%d", required, strings.Count(section, required))
			}
		}
		if strings.Contains(strings.ToLower(section), "/latest/") {
			t.Fatal("service accepts a mutable latest media-tool archive")
		}
	}
	script, err := os.ReadFile(filepath.Join("..", "..", "scripts", "install-pinned-media-tools.sh"))
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{"/latest/", "sha256sum -c -", "bin/ffmpeg", "bin/ffprobe"} {
		if !strings.Contains(string(script), required) {
			t.Fatalf("pinned media-tool installer lacks %q", required)
		}
	}
}

func testCookieSigningKey() string {
	return strings.Repeat("test", 8)
}

func TestDefaultMagicLinkTTLIsOneHour(t *testing.T) {
	t.Setenv("MAGIC_LINK_TTL", "")
	t.Setenv("RESEARCH_MAGIC_LINK_TTL", "")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.MagicLinkTTL != time.Hour {
		t.Fatalf("MagicLinkTTL=%s want %s", cfg.MagicLinkTTL, time.Hour)
	}
}

func TestMITSharedRecordingsConfiguration(t *testing.T) {
	t.Setenv("MIT_SCL_RECORDINGS_READ_ACCOUNT_ID", "47")
	t.Setenv("MIT_SCL_RECORDINGS_READ_SLUG", "mit-scl")
	t.Setenv("MIT_SCL_RECORDINGS_READ_PASSWORD", "manywalks")
	t.Setenv("MIT_SCL_RECORDINGS_COOKIE_SIGNING_KEY", testCookieSigningKey())
	t.Setenv("MIT_SCL_RECORDINGS_TRUSTED_PROXY_CIDRS", "10.0.0.0/8,2001:db8::/32")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.SharedRecordingsAccountID != 47 {
		t.Fatalf("shared recordings account id=%d", cfg.SharedRecordingsAccountID)
	}
	if cfg.SharedRecordingsPassword != "manywalks" {
		t.Fatalf("shared recordings password=%q", cfg.SharedRecordingsPassword)
	}
	if cfg.SharedRecordingsSlug != "mit-scl" {
		t.Fatalf("shared recordings slug=%q", cfg.SharedRecordingsSlug)
	}
	if cfg.SharedRecordingsCookieSigningKey != testCookieSigningKey() {
		t.Fatal("shared recordings cookie signing key was not loaded")
	}
	want := []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8"), netip.MustParsePrefix("2001:db8::/32")}
	if len(cfg.SharedRecordingsProxyCIDRs) != len(want) || cfg.SharedRecordingsProxyCIDRs[0] != want[0] || cfg.SharedRecordingsProxyCIDRs[1] != want[1] {
		t.Fatalf("trusted proxy CIDRs=%v", cfg.SharedRecordingsProxyCIDRs)
	}
}

func TestMITSharedRecordingsPublicModeNeedsNoPasswordOrSigningKey(t *testing.T) {
	t.Setenv("MIT_SCL_RECORDINGS_READ_ACCOUNT_ID", "47")
	t.Setenv("MIT_SCL_RECORDINGS_PUBLIC", "true")
	t.Setenv("MIT_SCL_RECORDINGS_READ_PASSWORD", "")
	t.Setenv("MIT_SCL_RECORDINGS_COOKIE_SIGNING_KEY", "")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.SharedRecordingsPublic || cfg.SharedRecordingsAccountID != 47 {
		t.Fatalf("public shared recordings public=%v account_id=%d", cfg.SharedRecordingsPublic, cfg.SharedRecordingsAccountID)
	}
}

func TestMITSharedRecordingsPublicModeRequiresAccount(t *testing.T) {
	t.Setenv("MIT_SCL_RECORDINGS_READ_ACCOUNT_ID", "")
	t.Setenv("MIT_SCL_RECORDINGS_PUBLIC", "true")
	t.Setenv("MIT_SCL_RECORDINGS_READ_PASSWORD", "")
	t.Setenv("MIT_SCL_RECORDINGS_COOKIE_SIGNING_KEY", "")
	t.Setenv("MIT_SCL_RECORDINGS_READ_SLUG", "mit-scl")
	t.Setenv("MIT_SCL_RECORDINGS_TRUSTED_PROXY_CIDRS", "")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "MIT_SCL_RECORDINGS_READ_ACCOUNT_ID") {
		t.Fatalf("Load error=%v, want MIT_SCL_RECORDINGS_READ_ACCOUNT_ID requirement", err)
	}
}

func TestMITSharedRecordingsTrustedProxyCIDRsMustBeValid(t *testing.T) {
	t.Setenv("MIT_SCL_RECORDINGS_TRUSTED_PROXY_CIDRS", "not-a-cidr")
	if _, err := Load(); err == nil {
		t.Fatal("Load succeeded with an invalid trusted proxy CIDR")
	}
}

func TestMITSharedRecordingsPasswordMustContainEightCharacters(t *testing.T) {
	t.Setenv("MIT_SCL_RECORDINGS_READ_ACCOUNT_ID", "47")
	t.Setenv("MIT_SCL_RECORDINGS_READ_PASSWORD", "seven77")
	t.Setenv("MIT_SCL_RECORDINGS_COOKIE_SIGNING_KEY", testCookieSigningKey())
	t.Setenv("MIT_SCL_RECORDINGS_TRUSTED_PROXY_CIDRS", "10.0.0.0/8")
	if _, err := Load(); err == nil {
		t.Fatal("Load succeeded with a seven-character shared recordings password")
	}
}

func TestMITSharedRecordingsAccountIDCannotBeNegative(t *testing.T) {
	t.Setenv("MIT_SCL_RECORDINGS_READ_ACCOUNT_ID", "-1")
	t.Setenv("MIT_SCL_RECORDINGS_READ_PASSWORD", "")
	if _, err := Load(); err == nil {
		t.Fatal("Load succeeded with a negative shared recordings account id")
	}
}

func TestMITSharedRecordingsConfigurationMustBePaired(t *testing.T) {
	for _, tc := range []struct {
		name, accountID, password string
	}{
		{name: "account only", accountID: "47"},
		{name: "password only", password: "team-password"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("MIT_SCL_RECORDINGS_READ_ACCOUNT_ID", tc.accountID)
			t.Setenv("MIT_SCL_RECORDINGS_READ_PASSWORD", tc.password)
			if _, err := Load(); err == nil {
				t.Fatal("Load succeeded with incomplete shared recordings configuration")
			}
		})
	}
}

func TestMITSharedRecordingsAllowsEmptyTrustedProxyCIDRs(t *testing.T) {
	t.Setenv("MIT_SCL_RECORDINGS_READ_ACCOUNT_ID", "47")
	t.Setenv("MIT_SCL_RECORDINGS_READ_PASSWORD", "team-password")
	t.Setenv("MIT_SCL_RECORDINGS_COOKIE_SIGNING_KEY", testCookieSigningKey())
	t.Setenv("MIT_SCL_RECORDINGS_TRUSTED_PROXY_CIDRS", "")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.SharedRecordingsProxyCIDRs) != 0 {
		t.Fatalf("trusted proxy CIDRs=%v want none", cfg.SharedRecordingsProxyCIDRs)
	}
}

func TestMITSharedRecordingsRequiresCookieSigningKey(t *testing.T) {
	t.Setenv("MIT_SCL_RECORDINGS_READ_ACCOUNT_ID", "47")
	t.Setenv("MIT_SCL_RECORDINGS_READ_PASSWORD", "team-password")
	t.Setenv("MIT_SCL_RECORDINGS_COOKIE_SIGNING_KEY", "")
	if _, err := Load(); err == nil {
		t.Fatal("Load succeeded without a shared recordings cookie signing key")
	}
}

func TestMITSharedRecordingsSlugMustBeURLSafe(t *testing.T) {
	t.Setenv("MIT_SCL_RECORDINGS_READ_SLUG", "MIT SCL")
	if _, err := Load(); err == nil {
		t.Fatal("Load succeeded with an invalid shared recordings slug")
	}
}
