package recordingworker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/capture"
	"github.com/daydemir/stoarama/backend/internal/recordingapi"
	"github.com/daydemir/stoarama/backend/internal/surrenderplan"
	"github.com/google/uuid"
	"golang.org/x/sys/unix"
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
	got, err := w.reserveCaptureProducer(context.Background(), job, 1, "/capture/one")
	if err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if calls != 2 || len(producerIDs) != 2 || producerIDs[0] == "" || producerIDs[0] != producerIDs[1] || got.ProducerID != producerIDs[0] {
		t.Fatalf("calls=%d ids=%v got=%+v", calls, producerIDs, got)
	}
}

func TestSurrenderTransportRequiresExplicitNonTemporaryDurableRoot(t *testing.T) {
	for _, root := range []string{"", os.TempDir(), filepath.Join(os.TempDir(), "stoarama-manual-worker")} {
		worker := &Worker{cfg: Config{CaptureTempDir: root}}
		if worker.surrenderTransportEnabled() {
			t.Fatalf("temporary capture root advertised v1: %q", root)
		}
	}
	worker := &Worker{cfg: Config{CaptureTempDir: filepath.Join(string(os.PathSeparator), "var", "lib", "stoarama-capture")}}
	if !worker.surrenderTransportEnabled() {
		t.Fatal("explicit private non-system-temp root did not advertise v1")
	}
}

func TestCaptureTimestampUsesExactTruncatedMicroseconds(t *testing.T) {
	base := time.Unix(1_700_000_000, 0).UTC()
	for _, nanos := range []int{499_000, 500_000, 999_000} {
		got := captureUnixMicro(base.Add(time.Duration(nanos)))
		want := base.Unix()*int64(time.Second/time.Microsecond) + int64(time.Duration(nanos)/time.Microsecond)
		if got != want {
			t.Fatalf("nanoseconds=%d microseconds=%d want=%d", nanos, got, want)
		}
	}
}

func TestCommittedCaptureSetPreflightStopsBeforeProcessLaunch(t *testing.T) {
	var heartbeat, ack, finish atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/heartbeat"):
			heartbeat.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{"lease_expires_at": time.Now().Add(time.Minute), "stop_required": true})
		case strings.HasSuffix(r.URL.Path, "/stop-ack"):
			var body recordingapi.CaptureStopAck
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil || len(body.Members) != 0 || body.InventorySHA256 == "" {
				http.Error(w, "invalid empty stop ACK", http.StatusBadRequest)
				return
			}
			ack.Add(1)
			w.WriteHeader(http.StatusNoContent)
		case strings.HasSuffix(r.URL.Path, "/finish"):
			finish.Add(1)
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := recordingapi.NewClient(recordingapi.ClientConfig{BaseURL: server.URL, NodeToken: "node"})
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	worker, err := NewWorker(Config{Client: client, WorkerID: "worker", CaptureTempDir: root})
	if err != nil {
		t.Fatal(err)
	}
	outputDir, err := os.MkdirTemp(root, "capture-continuous-")
	if err != nil {
		t.Fatal(err)
	}
	job := recordingapi.RecordingJob{JobID: 99, LeaseToken: uuid.NewString()}
	producer := &captureProducerJournal{
		JobID: job.JobID, LeaseToken: job.LeaseToken, ProducerID: uuid.NewString(), OutputDir: outputDir,
		CaptureSet: &captureSetJournal{SetID: uuid.NewString(), Plan: &recordingapi.CaptureSetPlan{ArtifactCount: 1}},
	}
	worker.surrenderState(job.JobID).producer = producer
	launch, err := worker.preflightCommittedCaptureSet(context.Background(), job, producer, outputDir)
	if err != nil || launch || heartbeat.Load() != 1 || ack.Load() != 1 || finish.Load() != 1 {
		t.Fatalf("launch=%v heartbeat=%d ack=%d finish=%d err=%v", launch, heartbeat.Load(), ack.Load(), finish.Load(), err)
	}
}

func TestRecoveryReplaysDurableStopAckBeforeAnyFurtherAuthority(t *testing.T) {
	var calls []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls = append(calls, r.URL.Path)
		if strings.HasSuffix(r.URL.Path, "/stop-ack") {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		http.Error(w, "unexpected authority before stop ACK", http.StatusConflict)
	}))
	defer server.Close()
	client, err := recordingapi.NewClient(recordingapi.ClientConfig{BaseURL: server.URL, NodeToken: "node"})
	if err != nil {
		t.Fatal(err)
	}
	worker, err := NewWorker(Config{Client: client, WorkerID: "worker", CaptureTempDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	ack := recordingapi.CaptureStopAck{AckID: uuid.NewString(), RetainedDirectoryDevice: 1, RetainedDirectoryInode: 2, Members: []recordingapi.CaptureStopAckMember{}}
	ack.InventorySHA256, err = recordingapi.CaptureStopInventorySHA(ack)
	if err != nil {
		t.Fatal(err)
	}
	journal := &captureProducerJournal{JobID: 77, LeaseToken: uuid.NewString(), CaptureSet: &captureSetJournal{SetID: uuid.NewString(), StopAck: &ack}}
	if _, err = worker.replayCaptureSetStopAck(context.Background(), journal); err != nil {
		t.Fatal(err)
	}
	if len(calls) != 1 || !strings.HasSuffix(calls[0], "/stop-ack") {
		t.Fatalf("calls=%v", calls)
	}
}

func TestDescriptorWalkPinsAncestorAcrossParentSwap(t *testing.T) {
	root := t.TempDir()
	parent := filepath.Join(root, "capture-root")
	original := filepath.Join(parent, "capture-continuous-original")
	if err := os.MkdirAll(original, 0o700); err != nil {
		t.Fatal(err)
	}
	fd, err := openDirectoryNoFollow(parent)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(fd)
	moved := parent + ".moved"
	if err = os.Rename(parent, moved); err != nil {
		t.Fatal(err)
	}
	if err = os.MkdirAll(filepath.Join(parent, "capture-continuous-original"), 0o700); err != nil {
		t.Fatal(err)
	}
	childFD, err := unix.Openat(fd, "capture-continuous-original", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(childFD)
	var got unix.Stat_t
	if err = unix.Fstat(childFD, &got); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(moved, "capture-continuous-original"))
	if err != nil {
		t.Fatal(err)
	}
	stat, ok := infoSyscallStat(info)
	if !ok {
		t.Fatal("missing original directory identity")
	}
	if uint64(got.Dev) != uint64(stat.Dev) || uint64(got.Ino) != uint64(stat.Ino) {
		t.Fatalf("descriptor escaped swapped parent: got=(%d,%d) want=(%d,%d)", got.Dev, got.Ino, stat.Dev, stat.Ino)
	}
}

func TestZeroByteStoppedLeafRecoversAsNoBytes(t *testing.T) {
	root := t.TempDir()
	outDir, err := os.MkdirTemp(root, "capture-continuous-zero.retained-")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(outDir, "seg-20260814-120000.mp4")
	if err = os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	paths, err := listCaptureArtifactPaths(outDir)
	if err != nil || len(paths) != 1 || paths[0].Size != 0 {
		t.Fatalf("zero-byte inventory=%+v err=%v", paths, err)
	}
	planAt := time.Now().UTC().Truncate(time.Microsecond)
	canonical, err := surrenderplan.Build(planAt, planAt.Add(5*time.Second), 5)
	if err != nil {
		t.Fatal(err)
	}
	planID, setID, producerID, leaseToken := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	plan := recordingapi.CaptureSetPlan{
		PlanID: planID.String(), SetID: setID.String(), ProducerID: producerID.String(), CaptureOrdinal: 1,
		FirstCaptureSequence: 1, AccountID: 1, RecordingID: 2, JobID: 3, LeaseToken: leaseToken.String(),
		OriginClaimGeneration: 1, SourceSnapshotSHA256: strings.Repeat("a", 64), DestinationNamingSHA256: strings.Repeat("b", 64),
		PlanAt: planAt, WindowEndAt: planAt.Add(5 * time.Second), DurationMicroseconds: canonical.DurationMicro,
		ClipDurationSeconds: 5, ArtifactCount: canonical.ArtifactCount, SegmentTimesArgument: canonical.SplitTimesArgument,
		MaxArtifactBytes: surrenderplan.RecoveryArtifactMaxBytes,
	}
	identity, err := captureSetIdentity(plan)
	if err != nil {
		t.Fatal(err)
	}
	seed := sha256.Sum256([]byte("zero-byte-open-before-first-write"))
	tree, err := surrenderplan.BuildTree(seed, identity)
	if err != nil {
		t.Fatal(err)
	}
	derived, err := surrenderplan.DeriveArtifact(seed, identity, 1)
	if err != nil {
		t.Fatal(err)
	}
	proof, err := tree.Proof(1)
	if err != nil {
		t.Fatal(err)
	}
	proofHex := make([]string, len(proof.Siblings))
	for i := range proof.Siblings {
		proofHex[i] = hex.EncodeToString(proof.Siblings[i][:])
	}
	ack := recordingapi.CaptureStopAck{AckID: uuid.NewString(), RetainedDirectoryDevice: paths[0].Device, RetainedDirectoryInode: paths[0].Inode + 1,
		Members: []recordingapi.CaptureStopAckMember{{Ordinal: 1, ArtifactID: derived.ID.String(), CaptureSequence: 1,
			RecoverySecretSHA256: hex.EncodeToString(derived.RecoverySecretHash[:]), Proof: proofHex,
			Device: paths[0].Device, Inode: paths[0].Inode, RelativeName: filepath.Base(path)}}}
	ack.InventorySHA256, err = recordingapi.CaptureStopInventorySHA(ack)
	if err != nil {
		t.Fatal(err)
	}
	var reported atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/stop-ack") || strings.Contains(r.URL.Path, "/materialize"):
			w.WriteHeader(http.StatusNoContent)
		case strings.HasSuffix(r.URL.Path, "/heartbeat"):
			_ = json.NewEncoder(w).Encode(map[string]any{"cancel": true})
		case strings.HasSuffix(r.URL.Path, "/status"):
			_ = json.NewEncoder(w).Encode(map[string]any{"intent_id": derived.ID.String(), "producer_id": producerID.String(), "job_id": 3,
				"lease_token": leaseToken.String(), "authority": "capture_set_grant", "expires_at": time.Now().Add(time.Minute),
				"artifacts": []any{map[string]any{"intent_id": derived.ID.String(), "capture_sequence": 1}}})
		case strings.HasSuffix(r.URL.Path, "/report"):
			var report recordingapi.CaptureRecoveryReport
			if decodeErr := json.NewDecoder(r.Body).Decode(&report); decodeErr != nil || report.ReportType != "no_bytes" || report.SizeBytes != nil || report.SHA256 != "" {
				http.Error(w, "not exact no_bytes", http.StatusBadRequest)
				return
			}
			reported.Store(true)
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := recordingapi.NewClient(recordingapi.ClientConfig{BaseURL: server.URL, NodeToken: "node"})
	if err != nil {
		t.Fatal(err)
	}
	worker, err := NewWorker(Config{Client: client, WorkerID: "worker", CaptureTempDir: root})
	if err != nil {
		t.Fatal(err)
	}
	rootHash := tree.Root()
	journal := &captureProducerJournal{JobID: 3, LeaseToken: leaseToken.String(), ProducerID: producerID.String(), CaptureOrdinal: 1,
		OutputDir: outDir, ClipDurationSec: 5, CaptureSet: &captureSetJournal{PlanID: planID.String(), SetID: setID.String(),
			Seed: hex.EncodeToString(seed[:]), FirstCaptureSequence: 1, Plan: &plan, MerkleRootSHA256: hex.EncodeToString(rootHash[:]), Committed: true, StopAck: &ack}}
	done, recoverErr := worker.recoverProducerJournal(context.Background(), journal)
	if recoverErr != nil || !done || !reported.Load() {
		t.Fatalf("done=%v reported=%v err=%v", done, reported.Load(), recoverErr)
	}
	if _, err = os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("zero-byte retained leaf not cleaned after no_bytes seal: %v", err)
	}
}

func TestSourceStopOpenBeforeFirstByteTerminalizesOnCurrentFence(t *testing.T) {
	root := t.TempDir()
	outDir, err := os.MkdirTemp(root, "capture-continuous-zero-live.retained-")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(outDir, "seg-20260814-120000.mp4")
	if err = os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	paths, err := listCaptureArtifactPaths(outDir)
	if err != nil || len(paths) != 1 || paths[0].Size != 0 {
		t.Fatalf("zero-byte inventory=%+v err=%v", paths, err)
	}
	planAt := time.Now().UTC().Truncate(time.Microsecond)
	canonical, err := surrenderplan.Build(planAt, planAt.Add(5*time.Second), 5)
	if err != nil {
		t.Fatal(err)
	}
	planID, setID, producerID, leaseToken := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	plan := recordingapi.CaptureSetPlan{
		PlanID: planID.String(), SetID: setID.String(), ProducerID: producerID.String(), CaptureOrdinal: 1,
		FirstCaptureSequence: 1, AccountID: 1, RecordingID: 2, JobID: 3, LeaseToken: leaseToken.String(),
		OriginClaimGeneration: 1, SourceSnapshotSHA256: strings.Repeat("a", 64), DestinationNamingSHA256: strings.Repeat("b", 64),
		PlanAt: planAt, WindowEndAt: planAt.Add(5 * time.Second), DurationMicroseconds: canonical.DurationMicro,
		ClipDurationSeconds: 5, ArtifactCount: canonical.ArtifactCount, SegmentTimesArgument: canonical.SplitTimesArgument,
		MaxArtifactBytes: surrenderplan.RecoveryArtifactMaxBytes,
	}
	identity, err := captureSetIdentity(plan)
	if err != nil {
		t.Fatal(err)
	}
	seed := sha256.Sum256([]byte("zero-byte-current-fence"))
	tree, err := surrenderplan.BuildTree(seed, identity)
	if err != nil {
		t.Fatal(err)
	}
	var stopACKs, materializations, heartbeats, finishes, recoveryCalls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/stop-ack"):
			stopACKs.Add(1)
			_ = json.NewEncoder(w).Encode(recordingapi.CaptureStopAckResult{NoBytesOrdinals: []int{1}})
		case strings.Contains(r.URL.Path, "/materialize"):
			materializations.Add(1)
			w.WriteHeader(http.StatusNoContent)
		case strings.HasSuffix(r.URL.Path, "/heartbeat"):
			heartbeats.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{"cancel": false, "lease_expires_at": time.Now().Add(time.Minute)})
		case strings.HasSuffix(r.URL.Path, "/finish"):
			finishes.Add(1)
			w.WriteHeader(http.StatusNoContent)
		case strings.HasSuffix(r.URL.Path, "/status"), strings.HasSuffix(r.URL.Path, "/report"), strings.Contains(r.URL.Path, "/recovery/"):
			recoveryCalls.Add(1)
			http.Error(w, "recovery authority is forbidden for a current fence", http.StatusConflict)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := recordingapi.NewClient(recordingapi.ClientConfig{BaseURL: server.URL, NodeToken: "node"})
	if err != nil {
		t.Fatal(err)
	}
	worker, err := NewWorker(Config{Client: client, WorkerID: "worker", CaptureTempDir: root})
	if err != nil {
		t.Fatal(err)
	}
	rootHash := tree.Root()
	journal := &captureProducerJournal{JobID: 3, LeaseToken: leaseToken.String(), ProducerID: producerID.String(), CaptureOrdinal: 1,
		OutputDir: outDir, ClipDurationSec: 5, CaptureSet: &captureSetJournal{PlanID: planID.String(), SetID: setID.String(),
			Seed: hex.EncodeToString(seed[:]), FirstCaptureSequence: 1, Plan: &plan, MerkleRootSHA256: hex.EncodeToString(rootHash[:]), Committed: true}}
	state := worker.surrenderState(3)
	state.producer = journal
	pool := startSegmentDeliveryPool(1, func() {}, func(capture.Segment) error { return nil })
	sequence := int64(0)
	retained, barrierErr := worker.captureSetStopBarrier(recordingapi.RecordingJob{JobID: 3, LeaseToken: leaseToken.String()}, journal, pool, &sequence)(context.Background(), outDir)
	pool.close()
	if barrierErr != nil || retained == "" || len(journal.Artifacts) != 1 || !journal.Artifacts[0].Done || journal.Artifacts[0].LocalPath != "" {
		t.Fatalf("retained=%q artifact=%+v err=%v", retained, journal.Artifacts, barrierErr)
	}
	done, recoverErr := worker.recoverProducerJournal(context.Background(), journal)
	if recoverErr != nil || !done {
		t.Fatalf("done=%v err=%v", done, recoverErr)
	}
	if stopACKs.Load() != 2 || materializations.Load() != 0 || heartbeats.Load() != 1 || finishes.Load() != 1 || recoveryCalls.Load() != 0 || journal.RecoveryGrantSeen {
		t.Fatalf("ack=%d materialize=%d heartbeat=%d finish=%d recovery=%d grant=%v", stopACKs.Load(), materializations.Load(), heartbeats.Load(), finishes.Load(), recoveryCalls.Load(), journal.RecoveryGrantSeen)
	}
	if _, err = os.Stat(filepath.Join(retained, filepath.Base(path))); !os.IsNotExist(err) {
		t.Fatalf("current-fence zero-byte retained leaf was not removed: %v", err)
	}
}

func TestRecoveryRotationRequirementSurvivesCrashesBeforeAndAfterJournalRetirement(t *testing.T) {
	root := filepath.Join(t.TempDir(), ".stoarama-surrender-v1")
	if err := ensurePrivateSurrenderJournalRoot(root); err != nil {
		t.Fatal(err)
	}
	journal := filepath.Join(root, "job-1-"+uuid.NewString()+".json")
	if err := os.WriteFile(journal, []byte("retired"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := persistRecoveryRotationRequirement(root); err != nil {
		t.Fatal(err)
	}
	state, err := loadClaimSuccessorState(root)
	if err != nil || state == nil || !state.RotationRequired || state.ProposalID != "" {
		t.Fatalf("state=%+v err=%v", state, err)
	}
	if _, err = os.Stat(journal); err != nil {
		t.Fatalf("journal was retired before rotation authority became durable: %v", err)
	}
	if err = unlinkPrivateLeaf(root, journal); err != nil {
		t.Fatal(err)
	}
	state, err = loadClaimSuccessorState(root)
	if err != nil || state == nil || !state.RotationRequired {
		t.Fatalf("rotation authority was lost after journal deletion: state=%+v err=%v", state, err)
	}
}

func durableSurrenderTestRoot(t *testing.T) string {
	t.Helper()
	root, err := os.MkdirTemp(".", ".surrender-transport-test-")
	if err != nil {
		t.Fatal(err)
	}
	root, err = filepath.Abs(root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	return root
}

func TestRetainedArtifactCleanupRejectsPathReplacement(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "seg-20260814-120000.mp4")
	if err := os.WriteFile(path, []byte("retained"), 0o600); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	stat, ok := infoSyscallStat(info)
	if !ok {
		t.Fatal("missing retained artifact identity")
	}
	original := path + ".original"
	if err = os.Rename(path, original); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, []byte("replacement"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err = removeRetainedArtifact(path, uint64(stat.Dev), uint64(stat.Ino)); err == nil {
		t.Fatal("path replacement was deleted")
	}
	if body, readErr := os.ReadFile(path); readErr != nil || string(body) != "replacement" {
		t.Fatalf("replacement changed: body=%q err=%v", body, readErr)
	}
}

func TestRetainedArtifactRecoveryRejectsPathReplacementBeforeProbe(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "seg-20260814-120000.mp4")
	if err := os.WriteFile(path, []byte("retained"), 0o600); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	stat, ok := infoSyscallStat(info)
	if !ok {
		t.Fatal("missing retained artifact identity")
	}
	if err = os.Rename(path, path+".original"); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, []byte("replacement"), 0o600); err != nil {
		t.Fatal(err)
	}
	worker := &Worker{recoverContinuousSegmentFile: func(context.Context, *os.File, string, time.Duration) (capture.Segment, error) {
		t.Fatal("replacement reached recovery probe")
		return capture.Segment{}, nil
	}}
	_, err = worker.recoverContinuousSegmentExact(context.Background(), captureArtifactPath{Path: path, Device: uint64(stat.Dev), Inode: uint64(stat.Ino)}, time.Minute)
	if err == nil {
		t.Fatal("path replacement entered recovery probe")
	}
}

func TestRetainedArtifactOpenRejectsSymlinkAndAdditionalHardlink(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "seg-20260814-120000.mp4.target")
	if err := os.WriteFile(target, []byte("retained"), 0o600); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(target)
	if err != nil {
		t.Fatal(err)
	}
	stat, ok := infoSyscallStat(info)
	if !ok {
		t.Fatal("missing retained artifact identity")
	}
	symlink := filepath.Join(root, "seg-20260814-120000.mp4")
	if err = os.Symlink(target, symlink); err != nil {
		t.Fatal(err)
	}
	if file, _, openErr := openRetainedArtifact(symlink, uint64(stat.Dev), uint64(stat.Ino)); openErr == nil {
		_ = file.Close()
		t.Fatal("symlink artifact opened")
	}
	if err = os.Remove(symlink); err != nil {
		t.Fatal(err)
	}
	if err = os.Link(target, symlink); err != nil {
		t.Fatal(err)
	}
	if file, _, openErr := openRetainedArtifact(symlink, uint64(stat.Dev), uint64(stat.Ino)); openErr == nil {
		_ = file.Close()
		t.Fatal("multiply-linked artifact opened")
	}
}

func TestCaptureArtifactReservationResponseLossReplaysExactPreByteIntents(t *testing.T) {
	var mu sync.Mutex
	var bodies [][]byte
	var producer *captureProducerJournal
	var durableBeforeServer bool
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/recording/jobs/51/capture-producers/3cc7341c-d96b-4f38-b7de-cc47229837f9/artifacts/reserve" {
			http.NotFound(w, r)
			return
		}
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		mu.Lock()
		bodies = append(bodies, raw)
		call := len(bodies)
		mu.Unlock()
		journalRaw, readErr := os.ReadFile(producer.path)
		if readErr != nil {
			t.Fatalf("server observed reservation before durable journal: %v", readErr)
		}
		durableBeforeServer = len(producer.Artifacts) == 2
		for _, artifact := range producer.Artifacts {
			durableBeforeServer = durableBeforeServer && bytes.Contains(journalRaw, []byte(artifact.RecoverySecret))
		}
		if call == 1 {
			conn, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Fatal(err)
			}
			_ = conn.Close()
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	client, err := recordingapi.NewClient(recordingapi.ClientConfig{BaseURL: server.URL, NodeToken: "test"})
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(t.TempDir(), ".surrender")
	w, err := NewWorker(Config{Client: client, WorkerID: "worker", CaptureTempDir: root})
	if err != nil {
		t.Fatal(err)
	}
	producer = &captureProducerJournal{JobID: 51, LeaseToken: uuid.NewString(), ProducerID: surrenderAttemptNamespace.String(), CaptureOrdinal: 1, OutputDir: filepath.Join(root, "capture"), ClipDurationSec: 60}
	state := w.surrenderState(51)
	state.producer = producer
	job := recordingapi.RecordingJob{JobID: 51, LeaseToken: producer.LeaseToken, SurrenderTransportVersion: 1}
	if err = w.reserveCaptureArtifactSlots(context.Background(), job, producer, 7, 2); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(bodies) != 2 || !bytes.Equal(bodies[0], bodies[1]) || len(producer.Artifacts) != 2 || !durableBeforeServer {
		t.Fatalf("calls=%d artifacts=%d", len(bodies), len(producer.Artifacts))
	}
	raw, err := os.ReadFile(producer.path)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(raw, []byte("resolved_url")) || bytes.Contains(raw, []byte("source_url")) || bytes.Contains(raw, []byte("headers")) {
		t.Fatalf("producer journal persisted source authority: %s", raw)
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
	journal := &captureProducerJournal{JobID: current.JobID, LeaseToken: current.LeaseToken, ProducerID: uuid.NewString(), CaptureOrdinal: 1}
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

func TestCurrentLeaseCrashAbandonsEveryPreReservedUnsealedIntent(t *testing.T) {
	producerID, leaseToken := uuid.NewString(), uuid.NewString()
	var finished atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/status"):
			_ = json.NewEncoder(w).Encode(map[string]any{"producer_id": producerID, "found": true, "current_lease": true, "intent_count": 2})
		case strings.HasSuffix(r.URL.Path, "/finish"):
			if r.Header.Get("X-Stoarama-Recording-Lease-Token") != leaseToken {
				http.Error(w, "missing lease", http.StatusUnauthorized)
				return
			}
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body["result"] != "abandoned_empty" {
				http.Error(w, "wrong result", http.StatusBadRequest)
				return
			}
			finished.Store(true)
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := recordingapi.NewClient(recordingapi.ClientConfig{BaseURL: server.URL, NodeToken: "node"})
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	outDir := filepath.Join(root, "capture-empty")
	if err = os.Mkdir(outDir, 0o700); err != nil {
		t.Fatal(err)
	}
	w, err := NewWorker(Config{Client: client, WorkerID: "worker", CaptureTempDir: root})
	if err != nil {
		t.Fatal(err)
	}
	journal := &captureProducerJournal{JobID: 52, LeaseToken: leaseToken, ProducerID: producerID, CaptureOrdinal: 1, OutputDir: outDir, ClipDurationSec: 60,
		Artifacts: []captureArtifactJournal{{IntentID: uuid.NewString(), CaptureSequence: 1}, {IntentID: uuid.NewString(), CaptureSequence: 2}}}
	done, err := w.recoverProducerJournal(context.Background(), journal)
	if err != nil || !done || !finished.Load() {
		t.Fatalf("done=%v finished=%v err=%v", done, finished.Load(), err)
	}
}

func TestCrashBeforeProducerReservationLeavesNoServerOrByteAuthority(t *testing.T) {
	producerID := uuid.NewString()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/status") {
			_ = json.NewEncoder(w).Encode(map[string]any{"producer_id": producerID, "found": false, "intent_count": 0})
			return
		}
		http.Error(w, "unexpected mutation", http.StatusInternalServerError)
	}))
	defer server.Close()
	client, err := recordingapi.NewClient(recordingapi.ClientConfig{BaseURL: server.URL, NodeToken: "node"})
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	outDir := filepath.Join(root, "capture-never-opened")
	if err = os.Mkdir(outDir, 0o700); err != nil {
		t.Fatal(err)
	}
	w, err := NewWorker(Config{Client: client, WorkerID: "worker", CaptureTempDir: root})
	if err != nil {
		t.Fatal(err)
	}
	done, err := w.recoverProducerJournal(context.Background(), &captureProducerJournal{JobID: 53, LeaseToken: uuid.NewString(), ProducerID: producerID, CaptureOrdinal: 1, OutputDir: outDir, ClipDurationSec: 60})
	if err != nil || !done {
		t.Fatalf("done=%v err=%v", done, err)
	}
}

func TestRecoveryCapabilityDrainsFinalizedBytesWithoutMainLease(t *testing.T) {
	producerID, leaseToken := uuid.NewString(), uuid.NewString()
	intentIDs := []string{uuid.NewString(), uuid.NewString()}
	secrets := []string{strings.Repeat("1a", 32), strings.Repeat("2b", 32)}
	var mu sync.Mutex
	ingested := map[string]bool{}
	var sealCalls, uploadCalls, ingestCalls int
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		intentIndex := -1
		for i, intentID := range intentIDs {
			if strings.Contains(r.URL.Path, intentID) || r.Header.Get("X-Stoarama-Recording-Recovery-Intent") == intentID {
				intentIndex = i
				break
			}
		}
		if !strings.HasPrefix(r.URL.Path, "/put/") && (intentIndex < 0 || r.Header.Get("X-Stoarama-Recording-Recovery-Secret") != secrets[intentIndex]) {
			http.Error(w, "missing exact-intent recovery capability", http.StatusUnauthorized)
			return
		}
		switch {
		case strings.HasSuffix(r.URL.Path, "/status"):
			mu.Lock()
			done := ingested[intentIDs[intentIndex]]
			mu.Unlock()
			artifact := map[string]any{"intent_id": intentIDs[intentIndex], "capture_sequence": intentIndex + 1}
			if done {
				artifact["result"] = "accepted_unique"
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"intent_id": intentIDs[intentIndex], "producer_id": producerID, "job_id": 41, "lease_token": leaseToken, "expires_at": time.Now().Add(time.Minute), "artifacts": []any{artifact}})
		case strings.HasSuffix(r.URL.Path, "/seal"):
			sealCalls++
			if intentIndex == 0 && sealCalls == 1 {
				// The server may have committed this exact seal even though the
				// response vanished. Recovery must retain the file/journal and retry
				// the same intent+bytes, never reserve a replacement intent.
				conn, _, err := w.(http.Hijacker).Hijack()
				if err != nil {
					t.Fatal(err)
				}
				_ = conn.Close()
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"intent_id": intentIDs[intentIndex], "upload_url": server.URL + "/put/" + intentIDs[intentIndex], "content_type": "video/mp4", "max_size_bytes": 1024, "expires_at": time.Now().Add(time.Minute)})
		case r.URL.Path == "/put":
			t.Fatal("non-exact upload path")
		case strings.HasPrefix(r.URL.Path, "/put/"):
			uploadCalls++
			w.WriteHeader(http.StatusOK)
		case r.URL.Path == "/api/v1/recording/clips/ingest":
			mu.Lock()
			ingestCalls++
			ingested[intentIDs[intentIndex]] = true
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]any{"clip_id": 71 + intentIndex, "head_version": intentIndex + 1, "head_upload_intent_id": intentIDs[intentIndex]})
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
	w.recoverContinuousSegmentFile = func(_ context.Context, _ *os.File, leaf string, _ time.Duration) (capture.Segment, error) {
		path := filepath.Join(outDir, leaf)
		return w.recoverContinuousSegment(context.Background(), path, time.Minute)
	}
	artifacts := make([]captureArtifactJournal, 2)
	for i := range artifacts {
		secretBytes, decodeErr := hex.DecodeString(secrets[i])
		if decodeErr != nil {
			t.Fatal(decodeErr)
		}
		hash := sha256.Sum256(secretBytes)
		artifacts[i] = captureArtifactJournal{IntentID: intentIDs[i], RecoverySecret: secrets[i], RecoverySecretSHA256: hex.EncodeToString(hash[:]), CaptureSequence: int64(i + 1)}
	}
	journal := &captureProducerJournal{JobID: 41, LeaseToken: leaseToken, ProducerID: producerID, CaptureOrdinal: 1, OutputDir: outDir, ClipDurationSec: 60, Artifacts: artifacts}
	done, err := w.recoverProducerJournal(context.Background(), journal)
	if err == nil || done {
		t.Fatalf("lost seal response did not preserve recovery: done=%v err=%v", done, err)
	}
	if _, statErr := os.Stat(segmentPath); statErr != nil {
		t.Fatalf("seal response loss removed exact bytes: %v", statErr)
	}
	done, err = w.recoverProducerJournal(context.Background(), journal)
	if err != nil || !done {
		t.Fatalf("done=%v err=%v", done, err)
	}
	if sealCalls != 3 || uploadCalls != 2 || ingestCalls != 2 {
		t.Fatalf("seal=%d upload=%d ingest=%d", sealCalls, uploadCalls, ingestCalls)
	}
	if _, err = os.Stat(segmentPath); !os.IsNotExist(err) {
		t.Fatalf("recovered segment still present: %v", err)
	}
	if _, err = os.Stat(secondPath); !os.IsNotExist(err) {
		t.Fatalf("second recovered segment still present: %v", err)
	}
}

func TestRecoveryCapabilityRetainsUnfinalizedProducerBytes(t *testing.T) {
	producerID, leaseToken, intentID := uuid.NewString(), uuid.NewString(), uuid.NewString()
	secret := strings.Repeat("1a", 32)
	var finished string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Stoarama-Recording-Recovery-Intent") != intentID || r.Header.Get("X-Stoarama-Recording-Recovery-Secret") != secret {
			http.Error(w, "wrong exact intent", http.StatusUnauthorized)
			return
		}
		if strings.HasSuffix(r.URL.Path, "/status") {
			_ = json.NewEncoder(w).Encode(map[string]any{"intent_id": intentID, "producer_id": producerID, "job_id": 42, "lease_token": leaseToken, "expires_at": time.Now().Add(time.Minute), "artifacts": []any{map[string]any{"intent_id": intentID, "capture_sequence": 1}}})
			return
		}
		if strings.HasSuffix(r.URL.Path, "/finish") {
			var body struct {
				Result string `json:"result"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			finished = body.Result
			w.WriteHeader(http.StatusNoContent)
			return
		}
		http.Error(w, "partial bytes must not upload", http.StatusInternalServerError)
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
	w.recoverContinuousSegmentFile = func(context.Context, *os.File, string, time.Duration) (capture.Segment, error) {
		return capture.Segment{}, errors.New("missing terminal media metadata")
	}
	secretBytes, _ := hex.DecodeString(secret)
	hash := sha256.Sum256(secretBytes)
	done, recoverErr := w.recoverProducerJournal(context.Background(), &captureProducerJournal{JobID: 42, LeaseToken: leaseToken, ProducerID: producerID, CaptureOrdinal: 1, OutputDir: outDir, ClipDurationSec: 60, Artifacts: []captureArtifactJournal{{IntentID: intentID, RecoverySecret: secret, RecoverySecretSHA256: hex.EncodeToString(hash[:]), CaptureSequence: 1}}})
	if done || recoverErr == nil || !strings.Contains(recoverErr.Error(), "retained") || finished != "unrecoverable_partial" {
		t.Fatalf("done=%v err=%v", done, recoverErr)
	}
	if _, err = os.Stat(partialPath); err != nil {
		t.Fatalf("unfinalized producer bytes were removed: %v", err)
	}
}

func TestRecoveryCapabilityCannotCrossIntent(t *testing.T) {
	intentID, secret := uuid.NewString(), strings.Repeat("1a", 32)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, intentID) || r.Header.Get("X-Stoarama-Recording-Recovery-Intent") != intentID || r.Header.Get("X-Stoarama-Recording-Recovery-Secret") != secret {
			http.Error(w, "cross-intent authority", http.StatusForbidden)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"intent_id": intentID})
	}))
	defer server.Close()
	client, err := recordingapi.NewClient(recordingapi.ClientConfig{BaseURL: server.URL, NodeToken: "node"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = client.RecordingRecoveryStatus(context.Background(), intentID, secret); err != nil {
		t.Fatal(err)
	}
	if _, err = client.RecordingRecoveryStatus(context.Background(), uuid.NewString(), secret); err == nil {
		t.Fatal("cross-intent recovery succeeded")
	}
}

func TestRecoveryCapabilityAcknowledgesAcceptedArtifactBeforeLocalCleanup(t *testing.T) {
	producerID, leaseToken, intentID := uuid.NewString(), uuid.NewString(), uuid.NewString()
	secret := strings.Repeat("3c", 32)
	root := t.TempDir()
	outDir, err := os.MkdirTemp(root, "capture-continuous-")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(outDir, "seg-20260814-120000.mp4")
	if err = os.WriteFile(path, []byte("already-safe"), 0o600); err != nil {
		t.Fatal(err)
	}
	var acknowledged atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Stoarama-Recording-Recovery-Intent") != intentID || r.Header.Get("X-Stoarama-Recording-Recovery-Secret") != secret {
			http.Error(w, "exact recovery required", http.StatusUnauthorized)
			return
		}
		switch {
		case strings.HasSuffix(r.URL.Path, "/status"):
			_ = json.NewEncoder(w).Encode(map[string]any{"intent_id": intentID, "producer_id": producerID, "job_id": 44, "lease_token": leaseToken, "expires_at": time.Now().Add(time.Minute), "artifacts": []any{map[string]any{"intent_id": intentID, "capture_sequence": 1, "result": "accepted_unique"}}})
		case strings.HasSuffix(r.URL.Path, "/finish"):
			var body struct {
				Result string `json:"result"`
			}
			if decodeErr := json.NewDecoder(r.Body).Decode(&body); decodeErr != nil || body.Result != "acknowledged_terminal" {
				http.Error(w, "wrong acknowledgment", http.StatusBadRequest)
				return
			}
			if _, statErr := os.Stat(path); statErr != nil {
				t.Errorf("local bytes were removed before server acknowledgment: %v", statErr)
			}
			acknowledged.Store(true)
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
	w, err := NewWorker(Config{Client: client, WorkerID: "worker", CaptureTempDir: root})
	if err != nil {
		t.Fatal(err)
	}
	secretBytes, _ := hex.DecodeString(secret)
	secretHash := sha256.Sum256(secretBytes)
	segment := capture.Segment{Path: path, CaptureSequence: 1, SizeBytes: 12, SHA256: strings.Repeat("a", 64), StartAt: time.Now().UTC()}
	journal := &captureProducerJournal{JobID: 44, LeaseToken: leaseToken, ProducerID: producerID, CaptureOrdinal: 1, OutputDir: outDir, ClipDurationSec: 60,
		Artifacts: []captureArtifactJournal{{IntentID: intentID, RecoverySecret: secret, RecoverySecretSHA256: hex.EncodeToString(secretHash[:]), CaptureSequence: 1, Segment: &segment}}}
	done, err := w.recoverProducerJournal(context.Background(), journal)
	if err != nil || !done || !acknowledged.Load() {
		t.Fatalf("done=%v acknowledged=%v err=%v", done, acknowledged.Load(), err)
	}
	if _, err = os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("acknowledged local replay still exists: %v", err)
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

func TestProducerJournalCarriesMaximumPreByteIntentSetWithinHardBounds(t *testing.T) {
	captureRoot := t.TempDir()
	root := filepath.Join(captureRoot, ".stoarama-surrender-v1")
	journal := &captureProducerJournal{
		JobID: 82, LeaseToken: uuid.NewString(), ProducerID: uuid.NewString(), CaptureOrdinal: 1,
		OutputDir: filepath.Join(captureRoot, "capture-continuous-test"), ClipDurationSec: 60,
		Artifacts: make([]captureArtifactJournal, 2048),
	}
	for index := range journal.Artifacts {
		secret := sha256.Sum256([]byte(fmt.Sprintf("intent-secret-%d", index)))
		secretHash := sha256.Sum256(secret[:])
		journal.Artifacts[index] = captureArtifactJournal{
			IntentID: uuid.NewString(), RecoverySecret: hex.EncodeToString(secret[:]),
			RecoverySecretSHA256: hex.EncodeToString(secretHash[:]), CaptureSequence: int64(index + 1),
		}
	}
	if err := persistProducerJournal(root, journal); err != nil {
		t.Fatal(err)
	}
	loaded, err := loadCaptureProducerJournals(root)
	if err != nil || len(loaded) != 1 || len(loaded[0].Artifacts) != 2048 {
		t.Fatalf("loaded=%d artifacts=%d err=%v", len(loaded), func() int {
			if len(loaded) == 0 {
				return 0
			}
			return len(loaded[0].Artifacts)
		}(), err)
	}
	journal.Artifacts = append(journal.Artifacts, captureArtifactJournal{})
	if err = persistProducerJournal(root, journal); err == nil {
		t.Fatal("producer journal exceeded the pre-byte artifact bound")
	}
}

func TestSurrenderTransportObservationJournalIsBoundedAndFlushesExactly(t *testing.T) {
	var available atomic.Bool
	var received atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/surrender/observations") {
			http.NotFound(w, r)
			return
		}
		if !available.Load() {
			http.Error(w, "offline", http.StatusServiceUnavailable)
			return
		}
		var body struct {
			Observations []recordingapi.SurrenderTransportObservation `json:"observations"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		received.Add(int64(len(body.Observations)))
		w.WriteHeader(http.StatusNoContent)
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
	observation := queuedSurrenderTransportObservation{JobID: 73, SurrenderTransportObservation: recordingapi.SurrenderTransportObservation{
		ID: uuid.NewString(), LeaseToken: uuid.NewString(), AttemptID: uuid.NewString(), Type: "request_started", ObservedAt: time.Now().UTC(), RequestSHA256: strings.Repeat("a", 64),
	}}
	if err = w.appendSurrenderTransportObservation(observation); err != nil {
		t.Fatal(err)
	}
	w.flushSurrenderTransportObservations(context.Background())
	if queued, err := loadSurrenderTransportObservations(w.surrenderJournalRoot()); err != nil || len(queued) != 1 {
		t.Fatalf("offline queue=%d err=%v", len(queued), err)
	}
	available.Store(true)
	w.flushSurrenderTransportObservations(context.Background())
	if received.Load() != 1 {
		t.Fatalf("received=%d", received.Load())
	}
	if queued, err := loadSurrenderTransportObservations(w.surrenderJournalRoot()); err != nil || len(queued) != 0 {
		t.Fatalf("flushed queue=%d err=%v", len(queued), err)
	}
	full := make([]queuedSurrenderTransportObservation, 256)
	for index := range full {
		full[index] = observation
		full[index].ID = uuid.NewString()
	}
	if err = persistSurrenderTransportObservations(w.surrenderJournalRoot(), full); err != nil {
		t.Fatal(err)
	}
	if err = w.appendSurrenderTransportObservation(observation); err == nil {
		t.Fatal("observation journal exceeded its hard bound")
	}
}

func TestRunRegistersRecoveryJournalBeforeFirstLeasePoll(t *testing.T) {
	root := durableSurrenderTestRoot(t)
	journalRoot := filepath.Join(root, ".stoarama-surrender-v1")
	secret := strings.Repeat("1a", 32)
	secretBytes, err := hex.DecodeString(secret)
	if err != nil {
		t.Fatal(err)
	}
	secretHash := sha256.Sum256(secretBytes)
	journal := &captureProducerJournal{
		JobID: 91, LeaseToken: uuid.NewString(), ProducerID: uuid.NewString(),
		CaptureOrdinal: 1, OutputDir: filepath.Join(root, "capture-continuous-pending"), ClipDurationSec: 60,
		Artifacts: []captureArtifactJournal{{IntentID: uuid.NewString(), RecoverySecret: secret, RecoverySecretSHA256: hex.EncodeToString(secretHash[:]), CaptureSequence: 1}},
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

func TestRunFailsClosedBeforeLeasePollOnMixedCorruptRecoveryInventory(t *testing.T) {
	root := durableSurrenderTestRoot(t)
	journalRoot := filepath.Join(root, ".stoarama-surrender-v1")
	secretBytes := bytes.Repeat([]byte{0x2a}, 32)
	secretHash := sha256.Sum256(secretBytes)
	journal := &captureProducerJournal{
		JobID: 92, LeaseToken: uuid.NewString(), ProducerID: uuid.NewString(), CaptureOrdinal: 1,
		OutputDir: filepath.Join(root, "capture-continuous-valid"), ClipDurationSec: 60,
		Artifacts: []captureArtifactJournal{{IntentID: uuid.NewString(), RecoverySecret: hex.EncodeToString(secretBytes), RecoverySecretSHA256: hex.EncodeToString(secretHash[:]), CaptureSequence: 1}},
	}
	if err := persistProducerJournal(journalRoot, journal); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(journalRoot, "job-93-corrupt.json"), []byte("{\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var leaseCalls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/recording/jobs/lease" {
			leaseCalls.Add(1)
		}
		http.NotFound(w, r)
	}))
	defer server.Close()
	client, err := recordingapi.NewClient(recordingapi.ClientConfig{BaseURL: server.URL, NodeToken: "node"})
	if err != nil {
		t.Fatal(err)
	}
	worker, err := NewWorker(Config{Client: client, WorkerID: "worker", CaptureTempDir: root, SkipDropletHeartbeat: true})
	if err != nil {
		t.Fatal(err)
	}
	if err = worker.Run(context.Background()); err == nil || !strings.Contains(err.Error(), "validate surrender recovery inventory") {
		t.Fatalf("Run error=%v", err)
	}
	if leaseCalls.Load() != 0 {
		t.Fatalf("lease calls=%d", leaseCalls.Load())
	}
}

func TestRecoveryInventoryRejectsSymlinkAndNonPrivateStateLeaves(t *testing.T) {
	for _, test := range []struct {
		name string
		make func(*testing.T, string)
	}{
		{name: "symlink producer journal", make: func(t *testing.T, root string) {
			target := filepath.Join(t.TempDir(), "target.json")
			if err := os.WriteFile(target, []byte("{}\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(target, filepath.Join(root, "job-1-"+uuid.NewString()+".json")); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "nonprivate producer journal", make: func(t *testing.T, root string) {
			if err := os.WriteFile(filepath.Join(root, "job-1-"+uuid.NewString()+".json"), []byte("{}\n"), 0o644); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), ".stoarama-surrender-v1")
			if err := ensurePrivateSurrenderJournalRoot(root); err != nil {
				t.Fatal(err)
			}
			test.make(t, root)
			if _, err := loadCaptureProducerJournals(root); err == nil {
				t.Fatal("unsafe recovery inventory was accepted")
			}
		})
	}
}

func TestObservationAndSuccessorStateRejectSymlinkLeaves(t *testing.T) {
	for _, leaf := range []string{filepath.Base(surrenderTransportObservationPath("root")), claimSuccessorStateFile} {
		t.Run(leaf, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), ".stoarama-surrender-v1")
			if err := ensurePrivateSurrenderJournalRoot(root); err != nil {
				t.Fatal(err)
			}
			target := filepath.Join(t.TempDir(), "target")
			if err := os.WriteFile(target, []byte("[]\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(target, filepath.Join(root, leaf)); err != nil {
				t.Fatal(err)
			}
			if leaf == claimSuccessorStateFile {
				if _, err := loadClaimSuccessorState(root); err == nil {
					t.Fatal("symlink successor state was accepted")
				}
			} else if _, err := loadSurrenderTransportObservations(root); err == nil {
				t.Fatal("symlink observation state was accepted")
			}
		})
	}
}

func TestRunFailsClosedBeforeLeasePollOnCorruptObservationJournal(t *testing.T) {
	root := durableSurrenderTestRoot(t)
	journalRoot := filepath.Join(root, ".stoarama-surrender-v1")
	if err := ensurePrivateSurrenderJournalRoot(journalRoot); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(surrenderTransportObservationPath(journalRoot), []byte("{\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var leaseCalls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/recording/jobs/lease" {
			leaseCalls.Add(1)
		}
		http.NotFound(w, r)
	}))
	defer server.Close()
	client, err := recordingapi.NewClient(recordingapi.ClientConfig{BaseURL: server.URL, NodeToken: "node"})
	if err != nil {
		t.Fatal(err)
	}
	worker, err := NewWorker(Config{Client: client, WorkerID: "worker", CaptureTempDir: root, SkipDropletHeartbeat: true})
	if err != nil {
		t.Fatal(err)
	}
	if err = worker.Run(context.Background()); err == nil || !strings.Contains(err.Error(), "validate surrender observation journal") {
		t.Fatalf("Run error=%v", err)
	}
	if leaseCalls.Load() != 0 {
		t.Fatalf("lease calls=%d", leaseCalls.Load())
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

func TestRelaySurrenderTransportFourJobBurstRetriesStableAttempts(t *testing.T) {
	var mu sync.Mutex
	counts := map[string]int{}
	jobs := map[string]int64{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/observations") {
			w.WriteHeader(http.StatusNoContent)
			return
		}
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
	w, err := NewWorker(Config{Client: client, WorkerID: "relay", CaptureTempDir: t.TempDir(), SkipDropletHeartbeat: true})
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

func TestSurrenderTransportFourJobBurstRetainsEveryLeaseUntilCommit(t *testing.T) {
	var mu sync.Mutex
	surrenders := map[int64]int{}
	heartbeats := map[int64]int{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		if len(parts) < 2 {
			http.NotFound(w, r)
			return
		}
		jobID, _ := strconv.ParseInt(parts[len(parts)-2], 10, 64)
		switch parts[len(parts)-1] {
		case "heartbeat":
			mu.Lock()
			heartbeats[jobID]++
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]any{"cancel": false, "lease_expires_at": time.Now().Add(time.Minute)})
		case "surrender":
			mu.Lock()
			surrenders[jobID]++
			attempt := surrenders[jobID]
			mu.Unlock()
			if attempt == 1 {
				http.Error(w, "temporary transport failure", http.StatusServiceUnavailable)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"result": "committed", "current_head_version": 0, "handoff_until": time.Now(), "next_retry_at": time.Now(), "had_clips": false, "alternate_available": true})
		default:
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	defer server.Close()
	client, err := recordingapi.NewClient(recordingapi.ClientConfig{BaseURL: server.URL, NodeToken: "node", HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	worker, err := NewWorker(Config{Client: client, WorkerID: "relay", CaptureTempDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	worker.heartbeatInt = 20 * time.Millisecond
	done := make(chan error, 4)
	for index := range 4 {
		job := recordingapi.RecordingJob{JobID: int64(100 + index), LeaseToken: uuid.NewString(), SurrenderTransportVersion: 1, LeaseExpiresAt: time.Now().Add(time.Minute)}
		go func() {
			jobCtx, cancel := context.WithCancel(context.Background())
			_ = worker.startHeartbeat(jobCtx, cancel, job.JobID, job.LeaseToken, job.LeaseExpiresAt)
			if !worker.surrenderContinuousJobForReason(jobCtx, cancel, job, recordingapi.SurrenderNoProgress, errors.New("no progress")) {
				done <- errors.New("surrender did not commit")
				return
			}
			if jobCtx.Err() == nil {
				done <- errors.New("committed surrender retained job context")
				return
			}
			done <- nil
		}()
	}
	for range 4 {
		if err = <-done; err != nil {
			t.Fatal(err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	for index := range 4 {
		jobID := int64(100 + index)
		if surrenders[jobID] != 2 || heartbeats[jobID] < 2 {
			t.Fatalf("job=%d surrender_calls=%d heartbeats=%d", jobID, surrenders[jobID], heartbeats[jobID])
		}
	}
}

func TestClaimSuccessorStateRestoresBearerBeforeAnyWorkerRequest(t *testing.T) {
	var authorization string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authorization = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	client, err := recordingapi.NewClient(recordingapi.ClientConfig{BaseURL: server.URL, NodeToken: "stale-bootstrap-token", HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	worker, err := NewWorker(Config{Client: client, WorkerID: "relay", CaptureTempDir: root})
	if err != nil {
		t.Fatal(err)
	}
	raw := "sin_" + strings.Repeat("x", 48)
	digest := sha256.Sum256([]byte(raw))
	state := claimSuccessorState{
		CurrentRawToken: raw, CurrentKeyPrefix: raw[:16],
		CurrentSecretSHA: hex.EncodeToString(digest[:]),
	}
	if err = persistClaimSuccessorState(worker.surrenderJournalRoot(), state); err != nil {
		t.Fatal(err)
	}
	if err = worker.restoreClaimCredential(); err != nil {
		t.Fatal(err)
	}
	if err = client.TouchDroplet(context.Background(), "build"); err != nil {
		t.Fatal(err)
	}
	if authorization != "Bearer "+raw {
		t.Fatalf("authorization=%q", authorization)
	}
	if err = os.Chmod(claimSuccessorStatePath(worker.surrenderJournalRoot()), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err = loadClaimSuccessorState(worker.surrenderJournalRoot()); err == nil {
		t.Fatal("world-readable successor credential was accepted")
	}
}

func TestEnabledClaimSuccessorReplaysAcknowledgmentWithSuccessorBearer(t *testing.T) {
	var authorization, path, touchAuthorization string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/claim-successor/") {
			authorization = r.Header.Get("Authorization")
			path = r.URL.Path
			_ = json.NewEncoder(w).Encode(map[string]any{"predecessor_retired": false})
			return
		}
		touchAuthorization = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	client, err := recordingapi.NewClient(recordingapi.ClientConfig{BaseURL: server.URL, NodeToken: "retiring-predecessor", HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	worker, err := NewWorker(Config{Client: client, WorkerID: "relay", CaptureTempDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	raw := "sin_" + strings.Repeat("y", 48)
	digest := sha256.Sum256([]byte(raw))
	state := claimSuccessorState{
		ProposalID: uuid.NewString(), RawToken: raw, KeyPrefix: raw[:16],
		SecretSHA: hex.EncodeToString(digest[:]), Enabled: true,
	}
	if err = persistClaimSuccessorState(worker.surrenderJournalRoot(), state); err != nil {
		t.Fatal(err)
	}
	if err = worker.maybeRotateClaimCredential(context.Background()); err != nil {
		t.Fatal(err)
	}
	if authorization != "Bearer "+raw || !strings.HasSuffix(path, "/recording/claim-successor/"+state.ProposalID+"/ack") {
		t.Fatalf("authorization=%q path=%q", authorization, path)
	}
	if err = client.TouchDroplet(context.Background(), "build"); err != nil {
		t.Fatal(err)
	}
	if touchAuthorization != "Bearer "+raw {
		t.Fatalf("enabled successor did not become the ordinary bearer: %q", touchAuthorization)
	}
	if _, err = os.Stat(claimSuccessorStatePath(worker.surrenderJournalRoot())); err != nil {
		t.Fatalf("enabled successor current state was not persisted: %v", err)
	}
}

func TestEnabledClaimSuccessorRetirementPersistsCurrentForNextGeneration(t *testing.T) {
	var authorization, path string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authorization = r.Header.Get("Authorization")
		path = r.URL.Path
		_ = json.NewEncoder(w).Encode(map[string]any{"predecessor_retired": true})
	}))
	defer server.Close()
	client, err := recordingapi.NewClient(recordingapi.ClientConfig{BaseURL: server.URL, NodeToken: "retiring-predecessor", HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	worker, err := NewWorker(Config{Client: client, WorkerID: "relay", CaptureTempDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	raw := "sin_" + strings.Repeat("z", 48)
	digest := sha256.Sum256([]byte(raw))
	state := claimSuccessorState{
		ProposalID: uuid.NewString(), RawToken: raw, KeyPrefix: raw[:16],
		SecretSHA: hex.EncodeToString(digest[:]), Enabled: true,
	}
	if err = persistClaimSuccessorState(worker.surrenderJournalRoot(), state); err != nil {
		t.Fatal(err)
	}
	if err = worker.maybeRotateClaimCredential(context.Background()); err != nil {
		t.Fatal(err)
	}
	if authorization != "Bearer "+raw || !strings.HasSuffix(path, "/recording/claim-successor/"+state.ProposalID+"/ack") {
		t.Fatalf("authorization=%q path=%q", authorization, path)
	}
	persisted, err := loadClaimSuccessorState(worker.surrenderJournalRoot())
	if err != nil || persisted == nil || persisted.CurrentRawToken != raw || persisted.ProposalID != "" || persisted.Enabled {
		t.Fatalf("retired predecessor did not become durable current credential: state=%+v err=%v", persisted, err)
	}
}

func TestUnacknowledgedClaimSuccessorNeverReplacesBootstrapBearer(t *testing.T) {
	var authorization string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authorization = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	client, err := recordingapi.NewClient(recordingapi.ClientConfig{BaseURL: server.URL, NodeToken: "bootstrap-predecessor", HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	worker, err := NewWorker(Config{Client: client, WorkerID: "relay", CaptureTempDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	pending, err := newClaimSuccessorState()
	if err != nil {
		t.Fatal(err)
	}
	if err = persistClaimSuccessorState(worker.surrenderJournalRoot(), pending); err != nil {
		t.Fatal(err)
	}
	if err = worker.restoreClaimCredential(); err != nil {
		t.Fatal(err)
	}
	if err = client.TouchDroplet(context.Background(), "build"); err != nil {
		t.Fatal(err)
	}
	if authorization != "Bearer bootstrap-predecessor" {
		t.Fatalf("unacknowledged successor replaced bootstrap bearer: %q", authorization)
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
