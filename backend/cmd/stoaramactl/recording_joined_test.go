package main

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/daydemir/stoarama/backend/internal/config"
)

type fakeJoinedOperator struct {
	freezeReq    joinedFreezeTier1Request
	dayReq       joinedSealStreamDayRequest
	remainingReq joinedSealRemainingDaysRequest
	finalReq     joinedFinalFreezeRequest
	workerReq    joinedWorkerRequest
	statusReq    joinedStatusRequest
	startupReq   joinedWorkerRequest
	startupErr   error
	workerRuns   int
}

func validJoinedWorkerConfig() config.Config {
	return config.Config{
		JoinedRecordingEnabled:             true,
		JoinedRecordingProtocolVersion:     1,
		JoinedRecordingBatchID:             "tier1-2026-08",
		JoinedRecordingScratchRoot:         "/tmp/stoarama-joined",
		JoinedRecordingStorageAuthority:    "example.r2.cloudflarestorage.com",
		JoinedRecordingFFmpegArchiveURL:    "https://example.com/ffmpeg/7.1.1/linux64.tar.xz",
		JoinedRecordingFFmpegArchiveSHA256: strings.Repeat("a", 64),
		JoinedRecordingFFmpegSHA256:        strings.Repeat("b", 64),
		JoinedRecordingFFprobeSHA256:       strings.Repeat("c", 64),
		JoinedRecordingWorkerToken:         "joined-bootstrap-test-token",
	}
}

func (f *fakeJoinedOperator) FreezeTier1(_ context.Context, req joinedFreezeTier1Request) (any, error) {
	f.freezeReq = req
	return map[string]any{"status": "planned"}, nil
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

func (f *fakeJoinedOperator) RunWorker(_ context.Context, req joinedWorkerRequest) error {
	f.workerReq = req
	f.workerRuns++
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
	factory := func(context.Context, config.Config) (joinedOperatorService, error) { return fake, nil }
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
	cfg := config.Config{JoinedRecordingEnabled: true, JoinedRecordingProtocolVersion: 1, JoinedRecordingBatchID: "tier1-2026-08-generation-1"}
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
	cfg := config.Config{JoinedRecordingEnabled: true, JoinedRecordingProtocolVersion: 1, JoinedRecordingBatchID: "tier1-2026-08-generation-1"}
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
