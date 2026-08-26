package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/joinedrecording"
)

func TestParseHistoricalQualificationEvidenceIsExact(t *testing.T) {
	evidence := struct {
		RecordingJobs []joinedHistoricalQualificationJobs `json:"recording_jobs"`
	}{RecordingJobs: make([]joinedHistoricalQualificationJobs, len(joinedrecording.Tier1RecordingIDs))}
	var jobID int64 = 1
	for i, recordingID := range joinedrecording.Tier1RecordingIDs {
		evidence.RecordingJobs[i] = joinedHistoricalQualificationJobs{RecordingID: recordingID, JobIDs: make([]int64, 14)}
		for day := range evidence.RecordingJobs[i].JobIDs {
			evidence.RecordingJobs[i].JobIDs[day] = jobID
			jobID++
		}
	}
	encoded, err := json.Marshal(evidence)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "historical-jobs.json")
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	args := []string{"--connection-id", "9", "--evidence-file", path}
	request, err := parseJoinedImportHistoricalQualification(config.Config{}, args)
	if err != nil || request.ConnectionID != 9 || len(request.RecordingJobs) != 33 || request.Apply {
		t.Fatalf("exact historical dry-run evidence rejected: request=%+v err=%v", request, err)
	}
	if _, err := parseJoinedImportHistoricalQualification(config.Config{}, append(args, "--apply")); err == nil {
		t.Fatal("historical apply without expected preview hash was accepted")
	}

	evidence.RecordingJobs[1].JobIDs[0] = evidence.RecordingJobs[0].JobIDs[0]
	encoded, _ = json.Marshal(evidence)
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := parseJoinedImportHistoricalQualification(config.Config{}, args); err == nil {
		t.Fatal("duplicate historical job evidence was accepted")
	}
}

type fakeJoinedOperator struct {
	historicalReq   joinedImportHistoricalQualificationRequest
	freezeReq       joinedFreezeTier1Request
	checkpointedReq joinedFreezeTier1Request
	dayReq          joinedSealStreamDayRequest
	remainingReq    joinedSealRemainingDaysRequest
	validationReq   joinedFinalFreezeRequest
	finalReq        joinedFinalFreezeRequest
	indexReq        joinedSealBatchIndexRequest
	workerReq       joinedWorkerRequest
	statusReq       joinedStatusRequest
	deliveryReq     joinedDeliveryStatusRequest
	startupReq      joinedWorkerRequest
	startupErr      error
	workerRuns      int
	runWorker       func(context.Context, joinedWorkerRequest) error
	admissionStatus joinedrecording.ClaimAdmissionStatus
	admissionReads  []joinedrecording.ClaimAdmissionStatus
	admissionSets   []joinedrecording.ClaimAdmissionRequest
}

func (f *fakeJoinedOperator) ImportHistoricalQualification(_ context.Context,
	req joinedImportHistoricalQualificationRequest) (any, error) {
	f.historicalReq = req
	return map[string]any{"status": "planned"}, nil
}

func validJoinedWorkerConfig() config.Config {
	const batch = "tier1-2026-08"
	return config.Config{
		JoinedRecordingEnabled:         true,
		JoinedRecordingProtocolVersion: 1,
		JoinedRecordingWorkScope:       config.JoinedWorkScopeCanary,
		JoinedRecordingBatchID:         batch,
		JoinedRecordingCanaryHourIDs: strings.Join([]string{
			batch + "__recording-377__date-2026-08-01__hour-01__generation-1",
			batch + "__recording-335__date-2026-08-01__hour-02__generation-1",
			batch + "__recording-337__date-2026-08-01__hour-03__generation-1",
		}, ","),
		JoinedRecordingScratchRoot:         "/tmp/stoarama-joined",
		JoinedRecordingStorageAuthority:    "example.r2.cloudflarestorage.com",
		JoinedRecordingFFmpegArchiveURL:    "https://example.com/ffmpeg/7.1.1/linux64.tar.xz",
		JoinedRecordingFFmpegArchiveSHA256: strings.Repeat("a", 64),
		JoinedRecordingFFmpegSHA256:        strings.Repeat("b", 64),
		JoinedRecordingFFprobeSHA256:       strings.Repeat("c", 64),
		JoinedRecordingWorkerToken:         strings.Repeat("w", 32),
	}
}

func (f *fakeJoinedOperator) FreezeTier1(_ context.Context, req joinedFreezeTier1Request) (any, error) {
	f.freezeReq = req
	return map[string]any{"status": "planned"}, nil
}

func (f *fakeJoinedOperator) FreezeTier1Checkpointed(_ context.Context, req joinedFreezeTier1Request) (any, error) {
	f.checkpointedReq = req
	return map[string]any{"state": "ready", "request_sha256": strings.Repeat("a", 64)}, nil
}

func (f *fakeJoinedOperator) SealStreamDay(_ context.Context, req joinedSealStreamDayRequest) (any, error) {
	f.dayReq = req
	return map[string]any{"status": "sealed"}, nil
}

func (f *fakeJoinedOperator) SealRemainingDays(_ context.Context, req joinedSealRemainingDaysRequest) (any, error) {
	f.remainingReq = req
	return map[string]any{"status": "sealed"}, nil
}

func (f *fakeJoinedOperator) FinalFreeze(_ context.Context, req joinedFinalFreezeRequest) (any, error) {
	f.finalReq = req
	return map[string]any{"status": "frozen"}, nil
}

func (f *fakeJoinedOperator) FinalValidation(_ context.Context, req joinedFinalFreezeRequest) (any, error) {
	f.validationReq = req
	return map[string]any{"state": "ready"}, nil
}

func (f *fakeJoinedOperator) SealBatchIndex(_ context.Context, req joinedSealBatchIndexRequest) (any, error) {
	f.indexReq = req
	return map[string]any{"sha256": req.ExpectedSHA256}, nil
}

func (f *fakeJoinedOperator) RunWorker(ctx context.Context, req joinedWorkerRequest) error {
	f.workerReq = req
	f.workerRuns++
	if f.runWorker != nil {
		return f.runWorker(ctx, req)
	}
	return nil
}

func (f *fakeJoinedOperator) CheckWorkerStartup(_ context.Context, req joinedWorkerRequest) error {
	f.startupReq = req
	return f.startupErr
}

func (f *fakeJoinedOperator) Status(_ context.Context, req joinedStatusRequest) (any, error) {
	f.statusReq = req
	return map[string]any{"status": "running"}, nil
}

func (f *fakeJoinedOperator) DeliveryStatus(_ context.Context, req joinedDeliveryStatusRequest) (any, error) {
	f.deliveryReq = req
	return map[string]any{"acknowledged": true}, nil
}

func (f *fakeJoinedOperator) ClaimAdmissionStatus(_ context.Context, _ string) (joinedrecording.ClaimAdmissionStatus, error) {
	if len(f.admissionReads) > 0 {
		status := f.admissionReads[0]
		f.admissionReads = f.admissionReads[1:]
		return status, nil
	}
	return f.admissionStatus, nil
}

func (f *fakeJoinedOperator) SetClaimAdmission(_ context.Context, req joinedrecording.ClaimAdmissionRequest) (joinedrecording.ClaimAdmissionStatus, error) {
	f.admissionSets = append(f.admissionSets, req)
	return f.admissionStatus, nil
}

func TestJoinedWorkerDisabledBeforeFactory(t *testing.T) {
	cfg := config.Config{}
	factoryCalled := false
	factory := func(context.Context, config.Config) (joinedOperatorService, error) {
		factoryCalled = true
		return nil, errors.New("must not initialize")
	}

	got, err := runRecordingJoinedWith(context.Background(), cfg, []string{"worker", "run"}, factory)
	if err != nil {
		t.Fatal(err)
	}
	if factoryCalled {
		t.Fatal("disabled worker initialized its service")
	}
	want := map[string]any{"enabled": false, "status": "disabled"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("result=%v want %v", got, want)
	}
}

func TestJoinedWorkerDisabledIdleIsCancellable(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		waitForJoinedWorkerShutdown(ctx)
		close(done)
	}()
	cancel()
	<-done
}

func TestJoinedWorkerEnabledProcessHandlesSIGTERM(t *testing.T) {
	if os.Getenv("STOARAMA_TEST_JOINED_SIGTERM_HELPER") == "1" {
		readyPath := os.Getenv("STOARAMA_TEST_JOINED_SIGTERM_READY")
		fake := &fakeJoinedOperator{runWorker: func(ctx context.Context, _ joinedWorkerRequest) error {
			if err := os.WriteFile(readyPath, []byte("ready"), 0o600); err != nil {
				return err
			}
			<-ctx.Done()
			fmt.Println("enabled-worker-admission-canceled")
			return nil
		}}
		_, err := runRecordingJoinedCommand(context.Background(), validJoinedWorkerConfig(), []string{"worker", "run"},
			func(context.Context, config.Config) (joinedOperatorService, error) { return fake, nil })
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		return
	}

	readyPath := filepath.Join(t.TempDir(), "ready")
	cmd := exec.Command(os.Args[0], "-test.run=^TestJoinedWorkerEnabledProcessHandlesSIGTERM$")
	cmd.Env = append(os.Environ(), "STOARAMA_TEST_JOINED_SIGTERM_HELPER=1", "STOARAMA_TEST_JOINED_SIGTERM_READY="+readyPath)
	var output bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &output
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(readyPath); err == nil {
			break
		}
		if time.Now().After(deadline) {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
			t.Fatalf("enabled worker did not start: %s", output.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		_ = cmd.Process.Kill()
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		if err != nil || !strings.Contains(output.String(), "enabled-worker-admission-canceled") {
			t.Fatalf("SIGTERM result err=%v output=%q", err, output.String())
		}
	case <-time.After(5 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatal("enabled worker ignored SIGTERM")
	}
}

func TestJoinedAdmissionDrainPausesBeforeZeroLeaseGate(t *testing.T) {
	cfg := validJoinedWorkerConfig()
	fake := &fakeJoinedOperator{admissionStatus: joinedrecording.ClaimAdmissionStatus{
		ProtocolVersion: joinedrecording.JoinedProtocolVersion, BatchID: cfg.JoinedRecordingBatchID,
		ClaimsPaused: true, ActiveLeaseCount: 0, UpdatedAt: time.Now(),
	}}
	result, err := runRecordingJoinedWith(context.Background(), cfg, []string{"admission", "drain", "--timeout-sec", "30"},
		func(context.Context, config.Config) (joinedOperatorService, error) { return fake, nil })
	status, ok := result.(joinedrecording.ClaimAdmissionStatus)
	if err != nil || !ok || !status.ClaimsPaused || status.ActiveLeaseCount != 0 || len(fake.admissionSets) != 1 || !fake.admissionSets[0].ClaimsPaused {
		t.Fatalf("admission drain result=%+v sets=%+v err=%v", result, fake.admissionSets, err)
	}
}

func TestJoinedAdmissionParsingIsBounded(t *testing.T) {
	cfg := validJoinedWorkerConfig()
	if req, err := parseJoinedAdmission(cfg, []string{"drain", "--timeout-sec", "60"}); err != nil || req.Action != "drain" || req.Timeout != time.Minute {
		t.Fatalf("parse drain req=%+v err=%v", req, err)
	}
	for _, args := range [][]string{{"drain", "--timeout-sec", "0"}, {"drain", "--timeout-sec", "7201"}, {"unknown"}} {
		if _, err := parseJoinedAdmission(cfg, args); err == nil {
			t.Fatalf("unsafe admission args accepted: %v", args)
		}
	}
}

func TestJoinedWorkerParsesFixedWorkerEnvelope(t *testing.T) {
	fake := &fakeJoinedOperator{}
	factoryCalls := 0
	factory := func(context.Context, config.Config) (joinedOperatorService, error) {
		factoryCalls++
		return fake, nil
	}
	cfg := validJoinedWorkerConfig()
	_, err := runRecordingJoinedWith(context.Background(), cfg, []string{
		"worker", "run", "--worker-id", "joined-01",
	}, factory)
	if err != nil {
		t.Fatal(err)
	}
	if factoryCalls != 1 {
		t.Fatalf("factory calls=%d want 1", factoryCalls)
	}
	want := joinedWorkerRequest{
		BatchID:     "tier1-2026-08",
		WorkerID:    "joined-01",
		ScratchRoot: "/tmp/stoarama-joined",
	}
	if fake.workerReq != want {
		t.Fatalf("worker request=%+v want %+v", fake.workerReq, want)
	}
	if fake.startupReq != want || fake.workerRuns != 1 {
		t.Fatalf("startup=%+v worker runs=%d", fake.startupReq, fake.workerRuns)
	}
}

func TestJoinedWorkerAllowsExplicitActivationEnvelope(t *testing.T) {
	fake := &fakeJoinedOperator{}
	var factoryCfg config.Config
	factory := func(_ context.Context, cfg config.Config) (joinedOperatorService, error) {
		factoryCfg = cfg
		return fake, nil
	}
	cfg := validJoinedWorkerConfig()
	cfg.JoinedRecordingBatchID = ""
	cfg.JoinedRecordingScratchRoot = ""
	_, err := runRecordingJoinedWith(context.Background(), cfg, []string{
		"worker", "run",
		"--batch-id", "tier1-2026-08",
		"--worker-id", "joined-01",
		"--scratch-root", "/tmp/stoarama-joined",
	}, factory)
	if err != nil {
		t.Fatal(err)
	}
	if fake.workerReq.BatchID != "tier1-2026-08" || fake.workerReq.ScratchRoot != "/tmp/stoarama-joined" {
		t.Fatalf("worker request=%+v", fake.workerReq)
	}
	if factoryCfg.JoinedRecordingBatchID != "tier1-2026-08" || factoryCfg.JoinedRecordingScratchRoot != "/tmp/stoarama-joined" {
		t.Fatalf("factory config batch=%q scratch=%q", factoryCfg.JoinedRecordingBatchID, factoryCfg.JoinedRecordingScratchRoot)
	}
}

func TestJoinedBatchIDGrammarMatchesSharedContract(t *testing.T) {
	for _, value := range []string{"a", "batch-", strings.Repeat("a", 63)} {
		if !joinedrecording.ValidBatchID(value) {
			t.Fatalf("shared grammar rejected %q", value)
		}
		if err := validateJoinedBatchID(value); err != nil {
			t.Fatalf("CLI grammar rejected %q: %v", value, err)
		}
	}
	for _, value := range []string{"", "-batch", "Batch", "batch_name", strings.Repeat("a", 64)} {
		if joinedrecording.ValidBatchID(value) || validateJoinedBatchID(value) == nil {
			t.Fatalf("joined batch grammars accepted %q", value)
		}
	}

	_, err := parseJoinedFreezeTier1(config.Config{}, []string{
		"--connection-id", "44", "--batch-id", "batch-", "--source-endpoint",
		"https://0123456789abcdef0123456789abcdef.r2.cloudflarestorage.com", "--qualification-run-id", "7",
	})
	if err == nil || !strings.Contains(err.Error(), "exact --generation") {
		t.Fatalf("Tier-1 generation suffix error=%v", err)
	}
}

func TestJoinedWorkerStartupFailurePreventsClaimLoop(t *testing.T) {
	fake := &fakeJoinedOperator{startupErr: errors.New("ffmpeg SHA mismatch")}
	factory := func(context.Context, config.Config) (joinedOperatorService, error) { return fake, nil }
	_, err := runRecordingJoinedWith(context.Background(), validJoinedWorkerConfig(), []string{
		"worker", "run", "--worker-id", "joined-01",
	}, factory)
	if err == nil || !strings.Contains(err.Error(), "startup check") {
		t.Fatalf("error=%v", err)
	}
	if fake.workerRuns != 0 {
		t.Fatalf("worker ran %d times after failed startup check", fake.workerRuns)
	}
}

func TestJoinedMutationsRequireExpectedHashWhenApplied(t *testing.T) {
	cfg := config.Config{JoinedRecordingEnabled: true, JoinedRecordingControlPlaneEnabled: true, JoinedRecordingProtocolVersion: 1, JoinedRecordingBatchID: "tier1-2026-08-generation-1"}
	factory := func(context.Context, config.Config) (joinedOperatorService, error) {
		t.Fatal("invalid request reached factory")
		return nil, nil
	}
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"freeze-tier1", "--connection-id", "44", "--source-endpoint", "https://0123456789abcdef0123456789abcdef.r2.cloudflarestorage.com", "--qualification-run-id", "7", "--apply"}, "expected-request-sha256"},
		{[]string{"final-freeze", "--apply"}, "expected-frozen-denominator-sha256"},
	} {
		_, err := runRecordingJoinedWith(context.Background(), cfg, tc.args, factory)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("args=%v error=%v", tc.args, err)
		}
	}
	checkpointedArgs := []string{"freeze-tier1-checkpointed", "--connection-id", "44", "--source-endpoint", "https://0123456789abcdef0123456789abcdef.r2.cloudflarestorage.com", "--qualification-run-id", "7", "--apply"}
	if _, err := runRecordingJoinedWith(context.Background(), cfg, checkpointedArgs, nil); err == nil || !strings.Contains(err.Error(), "flag provided but not defined") {
		t.Fatalf("checkpointed command exposed apply: %v", err)
	}
}

func TestJoinedMutationsRejectAmbiguousLegacyExpectedHashFlag(t *testing.T) {
	cfg := config.Config{JoinedRecordingEnabled: true, JoinedRecordingProtocolVersion: 1, JoinedRecordingBatchID: "tier1-2026-08-generation-1"}
	for _, args := range [][]string{
		{"freeze-tier1", "--connection-id", "44", "--expected-manifest-sha256", strings.Repeat("a", 64)},
		{"final-freeze", "--expected-manifest-sha256", strings.Repeat("a", 64)},
	} {
		_, err := runRecordingJoinedWith(context.Background(), cfg, args, nil)
		if err == nil || !strings.Contains(err.Error(), "flag provided but not defined") {
			t.Fatalf("args=%v error=%v", args, err)
		}
	}
}

func TestJoinedOperatorCommandsDispatchTypedRequests(t *testing.T) {
	fake := &fakeJoinedOperator{}
	factory := func(context.Context, config.Config) (joinedOperatorService, error) { return fake, nil }
	cfg := config.Config{JoinedRecordingEnabled: true, JoinedRecordingControlPlaneEnabled: true, JoinedRecordingProtocolVersion: 1, JoinedRecordingBatchID: "tier1-2026-08-generation-1"}
	hash := strings.Repeat("a", 64)
	endpoint := "https://0123456789abcdef0123456789abcdef.r2.cloudflarestorage.com"

	if _, err := runRecordingJoinedWith(context.Background(), cfg, []string{
		"freeze-tier1", "--connection-id", "44", "--source-endpoint", endpoint, "--qualification-run-id", "7",
		"--expected-request-sha256", hash, "--apply",
	}, factory); err != nil {
		t.Fatal(err)
	}
	if fake.freezeReq != (joinedFreezeTier1Request{ConnectionID: 44, BatchID: "tier1-2026-08-generation-1",
		Generation: 1, SourceEndpoint: endpoint, QualificationRunID: 7, ExpectedRequestSHA256: hash, Apply: true}) {
		t.Fatalf("freeze request=%+v", fake.freezeReq)
	}
	if _, err := runRecordingJoinedWith(context.Background(), cfg, []string{
		"freeze-tier1-checkpointed", "--connection-id", "44", "--source-endpoint", endpoint, "--qualification-run-id", "7",
	}, factory); err != nil {
		t.Fatal(err)
	}
	if fake.checkpointedReq != (joinedFreezeTier1Request{ConnectionID: 44, BatchID: "tier1-2026-08-generation-1",
		Generation: 1, SourceEndpoint: endpoint, QualificationRunID: 7}) {
		t.Fatalf("checkpointed freeze request=%+v", fake.checkpointedReq)
	}

	// The operator checkpoint remains callable while the joined worker is
	// disabled; only the API control plane and protocol need to be enabled.
	disabledWorkerCfg := cfg
	disabledWorkerCfg.JoinedRecordingEnabled = false
	disabledWorkerCfg.JoinedRecordingControlPlaneEnabled = true
	if _, err := runRecordingJoinedWith(context.Background(), disabledWorkerCfg, []string{
		"freeze-tier1-checkpointed", "--connection-id", "44", "--source-endpoint", endpoint, "--qualification-run-id", "7",
	}, factory); err != nil {
		t.Fatalf("checkpointed dry-run with disabled worker: %v", err)
	}
	if _, err := runRecordingJoinedWith(context.Background(), cfg, []string{
		"seal-stream-day", "--recording-id", "377", "--local-date", "2026-08-01", "--apply",
	}, factory); err != nil {
		t.Fatal(err)
	}
	if fake.dayReq != (joinedSealStreamDayRequest{BatchID: cfg.JoinedRecordingBatchID, RecordingID: 377,
		LocalDate: "2026-08-01", Apply: true}) {
		t.Fatalf("stream-day request=%+v", fake.dayReq)
	}
	if _, err := runRecordingJoinedWith(context.Background(), cfg, []string{
		"seal-remaining-days", "--canary-recording-id", "377", "--canary-local-date", "2026-08-01",
		"--expected-canary-seal-request-sha256", hash, "--apply",
	}, factory); err != nil {
		t.Fatal(err)
	}
	if fake.remainingReq != (joinedSealRemainingDaysRequest{BatchID: cfg.JoinedRecordingBatchID,
		CanaryRecordingID: 377, CanaryLocalDate: "2026-08-01", ExpectedCanarySealRequestSHA256: hash, Apply: true}) {
		t.Fatalf("remaining-days request=%+v", fake.remainingReq)
	}
	if _, err := runRecordingJoinedWith(context.Background(), cfg, []string{
		"final-freeze", "--expected-frozen-denominator-sha256", hash, "--apply",
	}, factory); err != nil {
		t.Fatal(err)
	}
	if fake.finalReq != (joinedFinalFreezeRequest{BatchID: cfg.JoinedRecordingBatchID,
		ExpectedFrozenDenominatorSHA256: hash, Apply: true}) {
		t.Fatalf("final-freeze request=%+v", fake.finalReq)
	}
	if fake.validationReq != fake.finalReq {
		t.Fatalf("final-validation request=%+v final=%+v", fake.validationReq, fake.finalReq)
	}
	if _, err := runRecordingJoinedWith(context.Background(), cfg, []string{
		"seal-batch-index", "--expected-sha256", hash, "--apply",
	}, factory); err != nil {
		t.Fatal(err)
	}
	if fake.indexReq != (joinedSealBatchIndexRequest{BatchID: cfg.JoinedRecordingBatchID,
		ExpectedSHA256: hash, Apply: true}) {
		t.Fatalf("batch-index request=%+v", fake.indexReq)
	}

	if _, err := runRecordingJoinedWith(context.Background(), cfg, []string{"status"}, factory); err != nil {
		t.Fatal(err)
	}
	if fake.statusReq.BatchID != cfg.JoinedRecordingBatchID {
		t.Fatalf("status request=%+v", fake.statusReq)
	}

}

func TestJoinedMutationsDormantBeforeFactory(t *testing.T) {
	for _, args := range [][]string{
		{"freeze-tier1", "--connection-id", "44", "--batch-id", "tier1-2026-08-generation-1"},
		{"seal-stream-day", "--batch-id", "tier1-2026-08-generation-1"},
		{"seal-remaining-days", "--batch-id", "tier1-2026-08-generation-1"},
		{"final-freeze", "--batch-id", "tier1-2026-08-generation-1"},
		{"seal-batch-index", "--batch-id", "tier1-2026-08-generation-1"},
	} {
		for _, cfg := range []config.Config{
			{},
			{JoinedRecordingEnabled: true, JoinedRecordingProtocolVersion: 0},
			{JoinedRecordingEnabled: true, JoinedRecordingProtocolVersion: 2},
		} {
			factoryCalled := false
			factory := func(context.Context, config.Config) (joinedOperatorService, error) {
				factoryCalled = true
				return nil, errors.New("must remain dormant")
			}
			_, err := runRecordingJoinedWith(context.Background(), cfg, args, factory)
			if err == nil || !strings.Contains(err.Error(), "PROTOCOL_VERSION=1") {
				t.Fatalf("args=%v cfg=%+v error=%v", args, cfg, err)
			}
			if factoryCalled {
				t.Fatalf("args=%v cfg=%+v initialized service", args, cfg)
			}
		}
	}
}

func TestJoinedWorkerRejectsLocalConcurrencyFlag(t *testing.T) {
	cfg := validJoinedWorkerConfig()
	_, err := runRecordingJoinedWith(context.Background(), cfg, []string{"worker", "run", "--concurrency", "8"}, nil)
	if err == nil || !strings.Contains(err.Error(), "flag provided but not defined") {
		t.Fatalf("error=%v", err)
	}
}

func TestJoinedDeliveryStatusDispatchesExactArtifact(t *testing.T) {
	cfg := validJoinedWorkerConfig()
	fake := &fakeJoinedOperator{}
	result, err := runRecordingJoinedWith(context.Background(), cfg, []string{
		"delivery-status", "--batch-id", cfg.JoinedRecordingBatchID, "--artifact-id", "429",
	}, func(context.Context, config.Config) (joinedOperatorService, error) { return fake, nil })
	if err != nil {
		t.Fatal(err)
	}
	if fake.deliveryReq != (joinedDeliveryStatusRequest{BatchID: cfg.JoinedRecordingBatchID, ArtifactID: 429}) {
		t.Fatalf("delivery status request=%+v", fake.deliveryReq)
	}
	if got := result.(map[string]any)["acknowledged"]; got != true {
		t.Fatalf("acknowledged=%v", got)
	}
}

func TestJoinedDeliveryStatusRejectsMissingOrInvalidArtifact(t *testing.T) {
	cfg := validJoinedWorkerConfig()
	for _, args := range [][]string{
		{"delivery-status"},
		{"delivery-status", "--artifact-id", "0"},
		{"delivery-status", "--artifact-id", "-1"},
		{"delivery-status", "--artifact-id", "1", "extra"},
	} {
		if _, err := runRecordingJoinedWith(context.Background(), cfg, args, nil); err == nil {
			t.Fatalf("args=%v accepted", args)
		}
	}
}
