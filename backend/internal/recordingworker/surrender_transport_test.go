package recordingworker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/capture"
	"github.com/daydemir/stoarama/backend/internal/recordingapi"
	"github.com/google/uuid"
)

func TestCaptureProducerReservationResponseLossReplaysSameDurableIdentity(t *testing.T) {
	var mu sync.Mutex
	var calls int
	var producerIDs []string
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/recording/jobs/41/capture-producers" {
			http.NotFound(w, r)
			return
		}
		var body struct {
			ProducerID     string `json:"producer_id"`
			CaptureOrdinal int64  `json:"capture_ordinal"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		mu.Lock()
		calls++
		producerIDs = append(producerIDs, body.ProducerID)
		call := calls
		mu.Unlock()
		if call == 1 {
			hj, ok := w.(http.Hijacker)
			if !ok {
				t.Fatal("response writer cannot simulate lost response")
			}
			conn, _, err := hj.Hijack()
			if err != nil {
				t.Fatal(err)
			}
			_ = conn.Close()
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"producer_id": body.ProducerID, "capture_ordinal": body.CaptureOrdinal})
	}))
	defer server.Close()
	client, err := recordingapi.NewClient(recordingapi.ClientConfig{BaseURL: server.URL, NodeToken: "test"})
	if err != nil {
		t.Fatal(err)
	}
	w, err := NewWorker(Config{Client: client, WorkerID: "worker", CaptureTempDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	job := recordingapi.RecordingJob{JobID: 41, LeaseToken: "20ea197f-3b7d-4d18-a74e-cb63215a4527", SurrenderTransportVersion: 1}
	got, err := w.reserveCaptureProducer(context.Background(), job, 1, "/capture/one", "https://media.example/live.m3u8")
	if err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if calls != 2 || len(producerIDs) != 2 || producerIDs[0] == "" || producerIDs[0] != producerIDs[1] || got.ProducerID != producerIDs[0] {
		t.Fatalf("calls=%d ids=%v got=%+v", calls, producerIDs, got)
	}
}

func TestRecoveryCannotRaceActiveOrCrossLeaseGeneration(t *testing.T) {
	var statusCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		statusCalls++
		http.Error(w, "unexpected recovery call", http.StatusInternalServerError)
	}))
	defer server.Close()
	client, err := recordingapi.NewClient(recordingapi.ClientConfig{BaseURL: server.URL, NodeToken: "node"})
	if err != nil {
		t.Fatal(err)
	}
	w, err := NewWorker(Config{Client: client, WorkerID: "worker", CaptureTempDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	current := recordingapi.RecordingJob{JobID: 42, LeaseToken: uuid.NewString(), SurrenderTransportVersion: 1}
	state, err := w.beginActiveSurrenderJob(current)
	if err != nil {
		t.Fatal(err)
	}
	journal := &captureProducerJournal{JobID: current.JobID, LeaseToken: current.LeaseToken, ProducerID: uuid.NewString(), RecoverySecret: strings.Repeat("1a", 32), CaptureOrdinal: 1}
	done, err := w.recoverProducerJournal(context.Background(), journal)
	if err != nil || done || statusCalls != 0 {
		t.Fatalf("active recovery done=%v calls=%d err=%v", done, statusCalls, err)
	}
	w.endActiveSurrenderJob(state)

	state.mu.Lock()
	state.producer = journal
	state.mu.Unlock()
	if _, err := w.beginActiveSurrenderJob(recordingapi.RecordingJob{JobID: current.JobID, LeaseToken: uuid.NewString(), SurrenderTransportVersion: 1}); err == nil {
		t.Fatal("new lease generation crossed prior recoverable producer")
	}
}

func TestRecoveryCapabilityDrainsFinalizedBytesWithoutMainLease(t *testing.T) {
	producerID := "5a7adfda-b8e8-48c8-a755-690e8a84c32a"
	leaseToken := "20ea197f-3b7d-4d18-a74e-cb63215a4527"
	secret := strings.Repeat("1a", 32)
	var mu sync.Mutex
	var reserveCalls, uploadCalls, ingestCalls, finishCalls int
	sequences := make([]int64, 0, 2)
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api/v1/recording/recovery/") || r.URL.Path == "/api/v1/recording/upload-intents" || r.URL.Path == "/api/v1/recording/clips/ingest" {
			if r.Header.Get("X-Stoarama-Recording-Recovery-Producer") != producerID || r.Header.Get("X-Stoarama-Recording-Recovery-Secret") != secret {
				http.Error(w, "missing recovery capability", http.StatusUnauthorized)
				return
			}
		}
		switch {
		case strings.HasSuffix(r.URL.Path, "/status"):
			mu.Lock()
			done := ingestCalls
			mu.Unlock()
			artifacts := []map[string]any{{"intent_id": "5d351c0c-7f6e-47ff-913e-3d555c190a78", "capture_sequence": 1, "segment_start_ms": time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC).UnixMilli(), "size_bytes": 7, "sha256": strings.Repeat("a", 64)}}
			if done == 2 {
				artifacts[0]["result"] = "accepted_unique"
				artifacts = append(artifacts, map[string]any{"intent_id": "6e462d1d-8a7f-48ff-a24f-4e666d201b89", "capture_sequence": 2, "segment_start_ms": time.Date(2026, 8, 14, 12, 1, 0, 0, time.UTC).UnixMilli(), "size_bytes": 7, "sha256": strings.Repeat("a", 64), "result": "accepted_unique"})
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"producer_id": producerID, "job_id": 41, "lease_token": leaseToken, "expires_at": time.Now().Add(time.Minute), "next_capture_sequence": 2, "artifacts": artifacts})
		case r.URL.Path == "/api/v1/recording/upload-intents":
			var body struct {
				CaptureSequence int64 `json:"capture_sequence"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			mu.Lock()
			reserveCalls++
			sequences = append(sequences, body.CaptureSequence)
			mu.Unlock()
			intentID := "5d351c0c-7f6e-47ff-913e-3d555c190a78"
			if body.CaptureSequence == 2 {
				intentID = "6e462d1d-8a7f-48ff-a24f-4e666d201b89"
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"intent_id": intentID, "upload_url": server.URL + "/put", "content_type": "video/mp4", "max_size_bytes": 1024, "expires_at": time.Now().Add(time.Minute)})
		case r.URL.Path == "/put":
			uploadCalls++
			w.WriteHeader(http.StatusOK)
		case r.URL.Path == "/api/v1/recording/clips/ingest":
			mu.Lock()
			ingestCalls++
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]any{"clip_id": 71, "head_version": 1, "head_upload_intent_id": "5d351c0c-7f6e-47ff-913e-3d555c190a78"})
		case strings.HasSuffix(r.URL.Path, "/finish"):
			finishCalls++
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := recordingapi.NewClient(recordingapi.ClientConfig{BaseURL: server.URL, NodeToken: "revoked-main-token"})
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	outDir, err := os.MkdirTemp(root, "capture-continuous-")
	if err != nil {
		t.Fatal(err)
	}
	segmentPath := filepath.Join(outDir, "seg-20260814-120000.mp4")
	if err = os.WriteFile(segmentPath, []byte("footage"), 0o600); err != nil {
		t.Fatal(err)
	}
	secondPath := filepath.Join(outDir, "seg-20260814-120100.mp4")
	if err = os.WriteFile(secondPath, []byte("footage"), 0o600); err != nil {
		t.Fatal(err)
	}
	w, err := NewWorker(Config{Client: client, WorkerID: "worker", CaptureTempDir: root})
	if err != nil {
		t.Fatal(err)
	}
	w.recoverContinuousSegment = func(_ context.Context, path string, _ time.Duration) (capture.Segment, error) {
		start := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
		if path == secondPath {
			start = start.Add(time.Minute)
		}
		return capture.Segment{Path: path, MIMEType: "video/mp4", SizeBytes: 7, SHA256: strings.Repeat("a", 64), StartAt: start, EndAt: start.Add(time.Minute), DurationMs: 60_000, Container: "mp4", VideoCodec: "h264"}, nil
	}
	done, err := w.recoverProducerJournal(context.Background(), &captureProducerJournal{JobID: 41, LeaseToken: leaseToken, ProducerID: producerID, RecoverySecret: secret, RecoverySecretSHA256: strings.Repeat("b", 64), CaptureOrdinal: 1, OutputDir: outDir, ResolvedURL: "https://media.example/live.m3u8", ClipDurationSec: 60})
	if err != nil || !done {
		t.Fatalf("done=%v err=%v", done, err)
	}
	if reserveCalls != 2 || uploadCalls != 2 || ingestCalls != 2 || finishCalls != 1 || len(sequences) != 2 || sequences[0] != 1 || sequences[1] != 2 {
		t.Fatalf("reserve=%d upload=%d ingest=%d finish=%d", reserveCalls, uploadCalls, ingestCalls, finishCalls)
	}
	if _, err = os.Stat(segmentPath); !os.IsNotExist(err) {
		t.Fatalf("recovered segment still present: %v", err)
	}
	if _, err = os.Stat(secondPath); !os.IsNotExist(err) {
		t.Fatalf("second recovered segment still present: %v", err)
	}
}

func TestRecoveryCapabilityRetainsUnfinalizedProducerBytes(t *testing.T) {
	producerID, leaseToken := uuid.NewString(), uuid.NewString()
	secret := strings.Repeat("1a", 32)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/status") {
			http.Error(w, "partial bytes must not reach upload or finish", http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"producer_id": producerID, "job_id": 42, "lease_token": leaseToken,
			"authority": "recovery_grant", "expires_at": time.Now().Add(time.Minute),
			"next_capture_sequence": 1, "artifacts": []any{},
		})
	}))
	defer server.Close()
	client, err := recordingapi.NewClient(recordingapi.ClientConfig{BaseURL: server.URL, NodeToken: "revoked-main-token"})
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	outDir, err := os.MkdirTemp(root, "capture-continuous-")
	if err != nil {
		t.Fatal(err)
	}
	partialPath := filepath.Join(outDir, "seg-20260814-120000.mp4")
	if err = os.WriteFile(partialPath, []byte("partial-media"), 0o600); err != nil {
		t.Fatal(err)
	}
	w, err := NewWorker(Config{Client: client, WorkerID: "worker", CaptureTempDir: root})
	if err != nil {
		t.Fatal(err)
	}
	w.recoverContinuousSegment = func(context.Context, string, time.Duration) (capture.Segment, error) {
		return capture.Segment{}, errors.New("missing terminal media metadata")
	}
	done, recoverErr := w.recoverProducerJournal(context.Background(), &captureProducerJournal{
		JobID: 42, LeaseToken: leaseToken, ProducerID: producerID, RecoverySecret: secret,
		CaptureOrdinal: 1, OutputDir: outDir, ResolvedURL: "https://media.example/live.m3u8", ClipDurationSec: 60,
	})
	if done || recoverErr == nil || !strings.Contains(recoverErr.Error(), "not finalized") {
		t.Fatalf("done=%v err=%v", done, recoverErr)
	}
	if _, err = os.Stat(partialPath); err != nil {
		t.Fatalf("unfinalized producer bytes were removed: %v", err)
	}
}

func TestCurrentLeaseRestartFinishesThroughGenerationFencedProducerRoute(t *testing.T) {
	producerID := uuid.NewString()
	leaseToken := uuid.NewString()
	secret := strings.Repeat("1a", 32)
	var ordinaryFinishes, recoveryFinishes int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/status"):
			_ = json.NewEncoder(w).Encode(map[string]any{
				"producer_id": producerID, "job_id": 71, "lease_token": leaseToken,
				"authority": "current_lease", "expires_at": time.Now().Add(time.Minute),
				"next_capture_sequence": 1, "artifacts": []any{},
			})
		case r.URL.Path == "/api/v1/recording/jobs/71/capture-producers/"+producerID+"/finish":
			ordinaryFinishes++
			w.WriteHeader(http.StatusNoContent)
		case strings.HasSuffix(r.URL.Path, "/finish"):
			recoveryFinishes++
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := recordingapi.NewClient(recordingapi.ClientConfig{BaseURL: server.URL, NodeToken: "current-node"})
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	outDir, err := os.MkdirTemp(root, "capture-continuous-")
	if err != nil {
		t.Fatal(err)
	}
	w, err := NewWorker(Config{Client: client, WorkerID: "worker", CaptureTempDir: root})
	if err != nil {
		t.Fatal(err)
	}
	done, err := w.recoverProducerJournal(context.Background(), &captureProducerJournal{
		JobID: 71, LeaseToken: leaseToken, ProducerID: producerID, RecoverySecret: secret,
		RecoverySecretSHA256: strings.Repeat("b", 64), CaptureOrdinal: 1,
		OutputDir: outDir, ResolvedURL: "https://media.example/live.m3u8", ClipDurationSec: 60,
	})
	if err != nil || !done || ordinaryFinishes != 1 || recoveryFinishes != 0 {
		t.Fatalf("done=%v ordinary=%d recovery=%d err=%v", done, ordinaryFinishes, recoveryFinishes, err)
	}
}

func TestPersistProducerJournalIgnoresAbandonedTemporarySibling(t *testing.T) {
	root := t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, ".capture-producer-abandoned.tmp"), []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}
	journal := &captureProducerJournal{JobID: 81, ProducerID: uuid.NewString()}
	if err := persistProducerJournal(root, journal); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(journal.path); err != nil {
		t.Fatalf("journal not persisted: %v", err)
	}
}

func TestRunRegistersRecoveryJournalBeforeFirstLeasePoll(t *testing.T) {
	root := t.TempDir()
	journalRoot := filepath.Join(root, ".stoarama-surrender-v1")
	secret := strings.Repeat("1a", 32)
	secretBytes, err := hex.DecodeString(secret)
	if err != nil {
		t.Fatal(err)
	}
	secretHash := sha256.Sum256(secretBytes)
	journal := &captureProducerJournal{
		JobID: 91, LeaseToken: uuid.NewString(), ProducerID: uuid.NewString(),
		CaptureOrdinal: 1, RecoverySecret: secret, RecoverySecretSHA256: hex.EncodeToString(secretHash[:]),
		OutputDir: filepath.Join(root, "capture-continuous-pending"), ResolvedURL: "https://media.example/live.m3u8", ClipDurationSec: 60,
	}
	if err = persistProducerJournal(journalRoot, journal); err != nil {
		t.Fatal(err)
	}
	var recoverySeen atomic.Bool
	var leaseBeforeRecovery atomic.Bool
	leaseSeen := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/status"):
			recoverySeen.Store(true)
			http.Error(w, "temporarily unavailable", http.StatusServiceUnavailable)
		case r.URL.Path == "/api/v1/recording/jobs/lease":
			if !recoverySeen.Load() {
				leaseBeforeRecovery.Store(true)
			}
			select {
			case leaseSeen <- struct{}{}:
			default:
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"job": nil})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := recordingapi.NewClient(recordingapi.ClientConfig{BaseURL: server.URL, NodeToken: "node"})
	if err != nil {
		t.Fatal(err)
	}
	w, err := NewWorker(Config{Client: client, WorkerID: "worker", CaptureTempDir: root, SkipDropletHeartbeat: true, PollInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	select {
	case <-leaseSeen:
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("worker never reached first lease poll")
	}
	cancel()
	if err = <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error=%v", err)
	}
	if leaseBeforeRecovery.Load() {
		t.Fatal("worker polled for a new lease before registering durable recovery")
	}
	state := w.surrenderState(journal.JobID)
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.producer == nil || state.producer.ProducerID != journal.ProducerID {
		t.Fatal("failed recovery did not retain the producer admission fence")
	}
}

func TestCaptureProducerNeverDeletesPendingSpoolAcrossSafetyExits(t *testing.T) {
	for _, cause := range []error{context.Canceled, errDiskPressure, errPermanentSegmentDelivery, errSegmentDeliveryExhausted} {
		if shouldCleanupCaptureProducerAttempt(true, true, true, cause, false) {
			t.Fatalf("v1 pending spool cleaned for %v", cause)
		}
	}
	if shouldCleanupCaptureProducerAttempt(true, true, false, errPermanentSegmentDelivery, false) {
		t.Fatal("unsubmitted partial producer spool was removed")
	}
	if !shouldCleanupCaptureProducerAttempt(true, false, false, errPermanentSegmentDelivery, false) {
		t.Fatal("fully acknowledged producer prevented safe directory cleanup")
	}
}

func TestSurrenderTransportFourJobBurstRetriesStableAttempts(t *testing.T) {
	var mu sync.Mutex
	counts := map[string]int{}
	jobs := map[string]int64{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			AttemptID string `json:"attempt_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "bad body", http.StatusBadRequest)
			return
		}
		mu.Lock()
		counts[body.AttemptID]++
		call := counts[body.AttemptID]
		jobs[body.AttemptID]++
		mu.Unlock()
		if call == 1 {
			hijacker, ok := w.(http.Hijacker)
			if !ok {
				http.Error(w, "no hijacker", http.StatusInternalServerError)
				return
			}
			conn, _, err := hijacker.Hijack()
			if err != nil {
				return
			}
			_ = conn.Close()
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"result": "committed", "current_head_version": 0, "handoff_until": time.Now(), "next_retry_at": time.Now(), "had_clips": false, "alternate_available": true})
	}))
	defer server.Close()
	client, err := recordingapi.NewClient(recordingapi.ClientConfig{BaseURL: server.URL, NodeToken: "node"})
	if err != nil {
		t.Fatal(err)
	}
	w, err := NewWorker(Config{Client: client, WorkerID: "worker", CaptureTempDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	errs := make(chan error, 4)
	var wg sync.WaitGroup
	for i := int64(1); i <= 4; i++ {
		wg.Add(1)
		go func(jobID int64) {
			defer wg.Done()
			<-start
			job := recordingapi.RecordingJob{JobID: jobID, LeaseToken: uuid.NewString(), SurrenderTransportVersion: 1}
			result, err := w.surrenderRecordingJobV1(context.Background(), job, recordingapi.SurrenderNoProgress, "no output")
			if err == nil && result.Result != "committed" {
				err = fmt.Errorf("result=%q", result.Result)
			}
			errs <- err
		}(i)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(counts) != 4 {
		t.Fatalf("attempt identities=%d want 4", len(counts))
	}
	for attemptID, count := range counts {
		if count != 2 || jobs[attemptID] != 2 {
			t.Fatalf("attempt %s calls=%d", attemptID, count)
		}
	}
}

func TestSurrenderTransportKeepsLeaseHeartbeatUntilCommit(t *testing.T) {
	var mu sync.Mutex
	heartbeats, surrenders := 0, 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/heartbeat"):
			mu.Lock()
			heartbeats++
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]any{"cancel": false, "lease_expires_at": time.Now().Add(time.Minute)})
		case strings.HasSuffix(r.URL.Path, "/surrender"):
			mu.Lock()
			surrenders++
			call := surrenders
			mu.Unlock()
			if call == 1 {
				hijacker := w.(http.Hijacker)
				conn, _, _ := hijacker.Hijack()
				_ = conn.Close()
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"result": "committed", "current_head_version": 0, "handoff_until": time.Now(), "next_retry_at": time.Now(), "had_clips": false, "alternate_available": true})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := recordingapi.NewClient(recordingapi.ClientConfig{BaseURL: server.URL, NodeToken: "node"})
	if err != nil {
		t.Fatal(err)
	}
	w, err := NewWorker(Config{Client: client, WorkerID: "worker", CaptureTempDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	w.heartbeatInt = 20 * time.Millisecond
	job := recordingapi.RecordingJob{JobID: 51, LeaseToken: uuid.NewString(), SurrenderTransportVersion: 1, LeaseExpiresAt: time.Now().Add(time.Minute)}
	jobCtx, cancel := context.WithCancel(context.Background())
	_ = w.startHeartbeat(jobCtx, cancel, job.JobID, job.LeaseToken, job.LeaseExpiresAt)
	if !w.surrenderContinuousJobForReason(jobCtx, cancel, job, recordingapi.SurrenderNoProgress, errors.New("no progress")) {
		t.Fatal("committed surrender did not stop job")
	}
	time.Sleep(25 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if surrenders != 2 || heartbeats < 3 || jobCtx.Err() == nil {
		t.Fatalf("surrenders=%d heartbeats=%d context=%v", surrenders, heartbeats, jobCtx.Err())
	}
}

func TestSurrenderStaleProgressReconcilesServerAcceptedUniqueHead(t *testing.T) {
	intentID := uuid.NewString()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"result": "stale_progress", "current_head_version": 3, "current_upload_intent_id": intentID, "current_clip_id": 91})
	}))
	defer server.Close()
	client, err := recordingapi.NewClient(recordingapi.ClientConfig{BaseURL: server.URL, NodeToken: "node"})
	if err != nil {
		t.Fatal(err)
	}
	w, err := NewWorker(Config{Client: client, WorkerID: "worker", CaptureTempDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	job := recordingapi.RecordingJob{JobID: 61, LeaseToken: uuid.NewString(), SurrenderTransportVersion: 1}
	result, err := w.surrenderRecordingJobV1(context.Background(), job, recordingapi.SurrenderNoProgress, "no output")
	if err != nil || result.Result != "stale_progress" {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	head, _ := w.surrenderState(job.JobID).snapshot()
	if head.Version != 3 || head.UploadIntentID != intentID || head.ClipID != 91 {
		t.Fatalf("head=%+v", head)
	}
}
