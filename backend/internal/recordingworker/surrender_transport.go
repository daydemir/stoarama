package recordingworker

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/daydemir/stoarama/backend/internal/capture"
	"github.com/daydemir/stoarama/backend/internal/recordingapi"
	"github.com/daydemir/stoarama/backend/internal/surrenderplan"
	"github.com/google/uuid"
)

const surrenderTransportRetryBudget = time.Minute

const maxCaptureProducerJournalBytes = 1 << 20

var surrenderAttemptNamespace = uuid.MustParse("3cc7341c-d96b-4f38-b7de-cc47229837f9")
var captureSegmentLeafRE = regexp.MustCompile(`^seg-[0-9]{8}-[0-9]{6}\.mp4$`)

func captureUnixMicro(value time.Time) int64 {
	value = value.UTC()
	return value.Unix()*int64(time.Second/time.Microsecond) + int64(value.Nanosecond())/int64(time.Microsecond)
}

type acceptedUniqueHead struct {
	Version        int64
	UploadIntentID string
	ClipID         int64
}

func loadCaptureProducerJournals(root string) ([]*captureProducerJournal, error) {
	rootInfo, statErr := os.Lstat(root)
	if os.IsNotExist(statErr) {
		return nil, nil
	}
	if statErr != nil || !rootInfo.IsDir() || rootInfo.Mode()&os.ModeSymlink != 0 || rootInfo.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("capture producer journal root is not private")
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, err
	}
	journals := make([]*captureProducerJournal, 0, len(entries))
	for _, entry := range entries {
		if entry.Name() == filepath.Base(surrenderTransportObservationPath(root)) {
			continue
		}
		if entry.Type()&os.ModeSymlink != 0 || entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") || !entry.Type().IsRegular() {
			return nil, fmt.Errorf("ambiguous surrender recovery inventory entry")
		}
		path := filepath.Join(root, entry.Name())
		raw, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		if len(raw) == 0 || len(raw) > maxCaptureProducerJournalBytes {
			return nil, fmt.Errorf("invalid capture producer journal size")
		}
		var journal captureProducerJournal
		if err := json.Unmarshal(raw, &journal); err != nil {
			return nil, fmt.Errorf("decode capture producer journal: %w", err)
		}
		producerID, producerErr := uuid.Parse(journal.ProducerID)
		leaseToken, leaseErr := uuid.Parse(journal.LeaseToken)
		if producerErr != nil || leaseErr != nil || journal.JobID <= 0 || journal.CaptureOrdinal <= 0 || journal.ClipDurationSec <= 0 || strings.TrimSpace(journal.OutputDir) == "" || len(journal.Artifacts) > 2048 {
			return nil, fmt.Errorf("invalid capture producer journal identity")
		}
		captureRoot := filepath.Dir(root)
		outputDir, pathErr := filepath.Abs(journal.OutputDir)
		captureRoot, rootErr := filepath.Abs(captureRoot)
		rel, relErr := filepath.Rel(captureRoot, outputDir)
		if pathErr != nil || rootErr != nil || relErr != nil || rel == "." || strings.Contains(rel, string(filepath.Separator)) || !strings.HasPrefix(rel, "capture-continuous-") {
			return nil, fmt.Errorf("capture producer output path is outside its private root")
		}
		if info, pathErr := os.Lstat(outputDir); pathErr == nil {
			if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
				return nil, fmt.Errorf("capture producer output path is not a real directory")
			}
		} else if !os.IsNotExist(pathErr) {
			return nil, pathErr
		}
		seenIntents := make(map[string]struct{}, len(journal.Artifacts))
		seenSequences := make(map[int64]struct{}, len(journal.Artifacts))
		for _, artifact := range journal.Artifacts {
			intentID, intentErr := uuid.Parse(strings.TrimSpace(artifact.IntentID))
			secret, secretErr := hex.DecodeString(artifact.RecoverySecret)
			hash := sha256.Sum256(secret)
			seg := artifact.Segment
			if intentErr != nil || intentID.String() != strings.ToLower(strings.TrimSpace(artifact.IntentID)) || artifact.CaptureSequence <= 0 || len(secret) != 32 || secretErr != nil || hex.EncodeToString(hash[:]) != artifact.RecoverySecretSHA256 {
				return nil, fmt.Errorf("invalid capture producer artifact journal")
			}
			if seg != nil {
				segmentPath, segmentErr := filepath.Abs(seg.Path)
				if segmentErr != nil || filepath.Dir(segmentPath) != outputDir || !captureSegmentLeafRE.MatchString(filepath.Base(segmentPath)) || seg.CaptureSequence != artifact.CaptureSequence || seg.SizeBytes <= 0 || seg.StartAt.IsZero() || len(seg.SHA256) != 64 || strings.ToLower(seg.SHA256) != seg.SHA256 {
					return nil, fmt.Errorf("invalid sealed capture producer artifact journal")
				}
				info, statErr := os.Lstat(segmentPath)
				stat, statOK := infoSyscallStat(info)
				if statErr != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || !statOK || stat.Nlink != 1 {
					return nil, fmt.Errorf("capture artifact path identity is unsafe")
				}
				if _, err := hex.DecodeString(seg.SHA256); err != nil {
					return nil, fmt.Errorf("invalid capture producer artifact digest")
				}
			}
			if _, duplicate := seenIntents[intentID.String()]; duplicate {
				return nil, fmt.Errorf("duplicate capture producer artifact intent")
			}
			if _, duplicate := seenSequences[artifact.CaptureSequence]; duplicate {
				return nil, fmt.Errorf("duplicate capture producer artifact sequence")
			}
			seenIntents[intentID.String()] = struct{}{}
			seenSequences[artifact.CaptureSequence] = struct{}{}
		}
		if journal.CaptureSet != nil {
			if err := validateCaptureSetJournal(&journal); err != nil {
				return nil, err
			}
		}
		journal.ProducerID = producerID.String()
		journal.LeaseToken = leaseToken.String()
		journal.path = path
		journals = append(journals, &journal)
	}
	return journals, nil
}

func infoSyscallStat(info os.FileInfo) (*syscall.Stat_t, bool) {
	if info == nil {
		return nil, false
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	return stat, ok
}

func captureSetIdentity(plan recordingapi.CaptureSetPlan) (surrenderplan.SetIdentity, error) {
	setID, setErr := uuid.Parse(strings.TrimSpace(plan.SetID))
	lease, leaseErr := uuid.Parse(strings.TrimSpace(plan.LeaseToken))
	producer, producerErr := uuid.Parse(strings.TrimSpace(plan.ProducerID))
	if setErr != nil || leaseErr != nil || producerErr != nil {
		return surrenderplan.SetIdentity{}, fmt.Errorf("invalid capture set plan identity")
	}
	set := surrenderplan.SetIdentity{
		SetID: setID, AccountID: plan.AccountID, RecordingID: plan.RecordingID, JobID: plan.JobID,
		LeaseToken: lease, OriginClaimGeneration: plan.OriginClaimGeneration, ProducerID: producer,
		SnapshotSHA256: plan.SourceSnapshotSHA256, DestinationNamingSHA256: plan.DestinationNamingSHA256,
		ArtifactCount: plan.ArtifactCount, MIME: "video/mp4", MaxBytes: plan.MaxArtifactBytes,
	}
	return set, set.Validate()
}

func validateCaptureSetJournal(journal *captureProducerJournal) error {
	setJournal := journal.CaptureSet
	if setJournal == nil {
		return nil
	}
	planID, planErr := uuid.Parse(strings.TrimSpace(setJournal.PlanID))
	setID, setErr := uuid.Parse(strings.TrimSpace(setJournal.SetID))
	seedBytes, seedErr := hex.DecodeString(strings.TrimSpace(setJournal.Seed))
	if planErr != nil || setErr != nil || len(seedBytes) != 32 || seedErr != nil || setJournal.FirstCaptureSequence <= 0 {
		return fmt.Errorf("invalid capture set journal identity")
	}
	if setJournal.Plan == nil {
		if setJournal.Committed || setJournal.MerkleRootSHA256 != "" {
			return fmt.Errorf("capture set journal claims commitment without a plan")
		}
		return nil
	}
	plan := *setJournal.Plan
	if plan.PlanID != planID.String() || plan.SetID != setID.String() || plan.ProducerID != journal.ProducerID || plan.JobID != journal.JobID || plan.LeaseToken != journal.LeaseToken || plan.CaptureOrdinal != journal.CaptureOrdinal || plan.FirstCaptureSequence != setJournal.FirstCaptureSequence {
		return fmt.Errorf("capture set plan differs from durable identity")
	}
	canonicalPlan, err := surrenderplan.Build(plan.PlanAt, plan.WindowEndAt, plan.ClipDurationSeconds)
	if err != nil || canonicalPlan.DurationMicro != plan.DurationMicroseconds || canonicalPlan.ArtifactCount != plan.ArtifactCount || canonicalPlan.SplitTimesArgument != plan.SegmentTimesArgument || plan.MaxArtifactBytes != surrenderplan.RecoveryArtifactMaxBytes {
		return fmt.Errorf("capture set plan is not canonical")
	}
	set, err := captureSetIdentity(plan)
	if err != nil {
		return err
	}
	var seed [32]byte
	copy(seed[:], seedBytes)
	tree, err := surrenderplan.BuildTree(seed, set)
	if err != nil {
		return err
	}
	rootValue := tree.Root()
	root := hex.EncodeToString(rootValue[:])
	if setJournal.MerkleRootSHA256 != "" && setJournal.MerkleRootSHA256 != root {
		return fmt.Errorf("capture set root differs from durable seed")
	}
	if setJournal.Committed && setJournal.MerkleRootSHA256 == "" {
		return fmt.Errorf("committed capture set has no root")
	}
	setJournal.tree = tree
	return nil
}

func (w *Worker) recoveryLoop(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		w.flushSurrenderTransportObservations(ctx)
		if err := w.recoverProducerJournals(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("recording worker recovery journal error: %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

type queuedSurrenderTransportObservation struct {
	JobID int64 `json:"job_id"`
	recordingapi.SurrenderTransportObservation
}

func surrenderTransportObservationPath(root string) string {
	return filepath.Join(root, "transport-observations-v1.queue")
}

func loadSurrenderTransportObservations(path string) ([]queuedSurrenderTransportObservation, error) {
	raw, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil || len(raw) == 0 || len(raw) > 256<<10 {
		return nil, fmt.Errorf("invalid surrender transport observation journal")
	}
	var observations []queuedSurrenderTransportObservation
	if err := json.Unmarshal(raw, &observations); err != nil || len(observations) > 256 {
		return nil, fmt.Errorf("invalid surrender transport observation journal")
	}
	return observations, nil
}

func persistSurrenderTransportObservations(root string, observations []queuedSurrenderTransportObservation) error {
	if err := ensurePrivateSurrenderJournalRoot(root); err != nil {
		return err
	}
	path := surrenderTransportObservationPath(root)
	if len(observations) == 0 {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
		return nil
	}
	tmp, err := os.CreateTemp(root, ".transport-observations-*.tmp")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	if err = tmp.Chmod(0o600); err == nil {
		err = json.NewEncoder(tmp).Encode(observations)
	}
	if err == nil {
		err = tmp.Sync()
	}
	closeErr := tmp.Close()
	if err == nil {
		err = closeErr
	}
	if err == nil {
		err = os.Rename(tmpPath, path)
	}
	if err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	dir, err := os.Open(root)
	if err != nil {
		return err
	}
	err = dir.Sync()
	closeErr = dir.Close()
	if err == nil {
		err = closeErr
	}
	return err
}

func (w *Worker) appendSurrenderTransportObservation(observation queuedSurrenderTransportObservation) error {
	w.surrenderObservationMu.Lock()
	defer w.surrenderObservationMu.Unlock()
	root := w.surrenderJournalRoot()
	observations, err := loadSurrenderTransportObservations(surrenderTransportObservationPath(root))
	if err != nil {
		return err
	}
	if len(observations) >= 256 {
		return fmt.Errorf("surrender transport observation journal is full")
	}
	observations = append(observations, observation)
	return persistSurrenderTransportObservations(root, observations)
}

func (w *Worker) flushSurrenderTransportObservations(ctx context.Context) {
	w.surrenderObservationMu.Lock()
	defer w.surrenderObservationMu.Unlock()
	root := w.surrenderJournalRoot()
	observations, err := loadSurrenderTransportObservations(surrenderTransportObservationPath(root))
	if err != nil || len(observations) == 0 {
		return
	}
	jobID := observations[0].JobID
	batch := make([]recordingapi.SurrenderTransportObservation, 0, 64)
	consumed := 0
	for consumed < len(observations) && consumed < 64 && observations[consumed].JobID == jobID {
		batch = append(batch, observations[consumed].SurrenderTransportObservation)
		consumed++
	}
	callCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	err = w.cfg.Client.RecordSurrenderTransportObservations(callCtx, jobID, batch)
	cancel()
	if err == nil {
		_ = persistSurrenderTransportObservations(root, observations[consumed:])
	}
}

func surrenderRequestDigest(req recordingapi.SurrenderRequest) string {
	values := []string{
		"1", strings.TrimSpace(req.AttemptID), string(req.Reason), strings.TrimSpace(req.ErrorText),
		strconv.FormatInt(req.ExpectedHeadVersion, 10), strings.TrimSpace(req.ExpectedUploadIntent),
		strconv.FormatInt(req.ExpectedClipID, 10), strconv.Itoa(req.SpoolCount),
		strconv.FormatInt(req.SpoolBytes, 10), strconv.Itoa(req.InFlightCount),
	}
	h := sha256.New()
	_, _ = h.Write([]byte("recording-surrender-request-v1\n"))
	for _, value := range values {
		_, _ = fmt.Fprintf(h, "%d:%s\n", len([]byte(value)), value)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func surrenderTransportErrorClass(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	return "transport_error"
}

func (w *Worker) recoverProducerJournals(ctx context.Context) error {
	journals, err := loadCaptureProducerJournals(w.surrenderJournalRoot())
	if err != nil {
		return fmt.Errorf("validate surrender recovery inventory: %w", err)
	}
	for _, journal := range journals {
		done, err := w.recoverProducerJournalV2(ctx, journal)
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("recording worker job=%d capture recovery pending: %v", journal.JobID, err)
		}
		if done {
			_ = os.Remove(journal.path)
			_ = os.RemoveAll(journal.OutputDir)
		}
	}
	return nil
}

func (w *Worker) recoverProducerJournalV2(ctx context.Context, journal *captureProducerJournal) (bool, error) {
	state := w.surrenderState(journal.JobID)
	state.mu.Lock()
	if state.active || state.recovering || (state.producer != nil && state.producer.ProducerID != journal.ProducerID) {
		state.mu.Unlock()
		return false, nil
	}
	state.recovering, state.producer = true, journal
	state.mu.Unlock()
	done := false
	defer func() {
		state.mu.Lock()
		state.recovering = false
		if done && state.producer != nil && state.producer.ProducerID == journal.ProducerID {
			state.producer = nil
		}
		state.mu.Unlock()
	}()
	rootInput := strings.TrimSpace(w.cfg.CaptureTempDir)
	if rootInput == "" {
		rootInput = os.TempDir()
	}
	root, err := filepath.Abs(rootInput)
	if err != nil {
		return false, err
	}
	outDir, err := filepath.Abs(journal.OutputDir)
	if err != nil {
		return false, err
	}
	rel, err := filepath.Rel(root, outDir)
	if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return false, fmt.Errorf("recovery output directory escaped capture root")
	}
	paths, err := filepath.Glob(filepath.Join(outDir, "seg-*.mp4"))
	if err != nil {
		return false, err
	}
	sort.Strings(paths)
	if producerStatus, statusErr := w.cfg.Client.CaptureProducerStatus(ctx, journal.JobID, journal.ProducerID); statusErr == nil {
		if producerStatus.ProducerID != journal.ProducerID {
			return false, fmt.Errorf("capture producer status identity mismatch")
		}
		if !producerStatus.Found {
			if len(paths) == 0 {
				done = true
				return true, nil
			}
			return false, fmt.Errorf("unregistered capture producer has retained local bytes")
		}
		if producerStatus.Result != "" {
			if len(paths) == 0 {
				done = true
				return true, nil
			}
			return false, fmt.Errorf("terminal producer still has retained local bytes")
		}
		if producerStatus.IntentCount == 0 {
			if len(paths) != 0 {
				return false, fmt.Errorf("capture producer has bytes without pre-reserved intents")
			}
			if err := w.cfg.Client.FinishCaptureProducer(ctx, journal.JobID, journal.LeaseToken, journal.ProducerID, "abandoned_empty", "no_artifact_reservation"); err != nil {
				return false, err
			}
			done = true
			return true, nil
		}
		if producerStatus.IntentCount != len(journal.Artifacts) {
			return false, fmt.Errorf("capture producer intent count differs from durable journal")
		}
		if producerStatus.CurrentLease {
			if err := w.recoverProducerUnderCurrentLease(ctx, journal, paths); err != nil {
				return false, err
			}
			done = true
			return true, nil
		}
	}
	boundPaths := make(map[string]struct{}, len(journal.Artifacts))
	for _, artifact := range journal.Artifacts {
		if artifact.Segment != nil {
			boundPaths[artifact.Segment.Path] = struct{}{}
		}
	}
	unboundPaths := make([]string, 0, len(paths))
	for _, path := range paths {
		if _, bound := boundPaths[path]; !bound {
			unboundPaths = append(unboundPaths, path)
		}
	}
	pathIndex := 0
	for index := range journal.Artifacts {
		artifact := &journal.Artifacts[index]
		if artifact.Done {
			continue
		}
		status, statusErr := w.cfg.Client.RecordingRecoveryStatus(ctx, artifact.IntentID, artifact.RecoverySecret)
		if statusErr != nil {
			return false, statusErr
		}
		if status.IntentID != artifact.IntentID || status.ProducerID != journal.ProducerID || status.JobID != journal.JobID || status.LeaseToken != journal.LeaseToken || len(status.Artifacts) != 1 || status.Artifacts[0].CaptureSequence != artifact.CaptureSequence {
			return false, fmt.Errorf("recovery status identity mismatch")
		}
		serverArtifact := status.Artifacts[0]
		if serverArtifact.Result != "" {
			if serverArtifact.Result == "unrecoverable_partial" {
				return false, fmt.Errorf("partial capture bytes retained for operator recovery")
			}
			if err := w.cfg.Client.FinishRecordingRecovery(ctx, artifact.IntentID, artifact.RecoverySecret, "acknowledged_terminal"); err != nil {
				return false, err
			}
			if artifact.Segment != nil {
				capture.RemoveSegmentFile(*artifact.Segment)
			}
			artifact.Done = true
			artifact.Segment = nil
			if err := persistProducerJournal(w.surrenderJournalRoot(), journal); err != nil {
				return false, err
			}
			continue
		}
		path := ""
		if artifact.Segment != nil {
			path = artifact.Segment.Path
		}
		if path == "" && pathIndex < len(unboundPaths) {
			path = unboundPaths[pathIndex]
			pathIndex++
		}
		if path == "" {
			if err := w.cfg.Client.FinishRecordingRecovery(ctx, artifact.IntentID, artifact.RecoverySecret, "abandoned_unsealed"); err != nil {
				return false, err
			}
			artifact.Done = true
			if err := persistProducerJournal(w.surrenderJournalRoot(), journal); err != nil {
				return false, err
			}
			continue
		}
		probeCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		seg, probeErr := w.recoverContinuousSegment(probeCtx, path, time.Duration(journal.ClipDurationSec)*time.Second)
		cancel()
		if probeErr != nil {
			if err := w.cfg.Client.FinishRecordingRecovery(ctx, artifact.IntentID, artifact.RecoverySecret, "unrecoverable_partial"); err != nil {
				return false, errors.Join(fmt.Errorf("recovery artifact is not finalized"), err)
			}
			return false, fmt.Errorf("partial capture bytes retained for operator recovery")
		}
		if artifact.Segment != nil {
			if artifact.Segment.SHA256 != seg.SHA256 || artifact.Segment.SizeBytes != seg.SizeBytes {
				return false, fmt.Errorf("recovery artifact bytes changed after journal seal")
			}
			seg = *artifact.Segment
		} else {
			seg.CaptureSequence = artifact.CaptureSequence
			artifact.Segment = &seg
			if err := persistProducerJournal(w.surrenderJournalRoot(), journal); err != nil {
				return false, err
			}
		}
		intent, err := w.cfg.Client.SealCaptureArtifactRecovery(ctx, journal.JobID, journal.LeaseToken, artifact.IntentID, artifact.RecoverySecret, journal.ProducerID, artifact.CaptureSequence, seg.StartAt.UTC().UnixMilli(), seg.SizeBytes, seg.SHA256)
		if err != nil {
			return false, err
		}
		if !intent.AlreadyIngested {
			if err = w.cfg.Client.UploadFile(ctx, intent.UploadURL, seg.Path, seg.MIMEType); err != nil {
				return false, err
			}
			if _, err = w.cfg.Client.IngestClipRecovery(ctx, recoveryIngestRequest(journal, seg, artifact.IntentID, artifact.CaptureSequence), artifact.IntentID, artifact.RecoverySecret); err != nil {
				return false, err
			}
		}
		capture.RemoveSegmentFile(seg)
		artifact.Done = true
		artifact.Segment = nil
		if err = persistProducerJournal(w.surrenderJournalRoot(), journal); err != nil {
			return false, err
		}
	}
	remaining, err := filepath.Glob(filepath.Join(outDir, "seg-*.mp4"))
	if err != nil || len(remaining) != 0 {
		return false, err
	}
	done = true
	return true, nil
}

func (w *Worker) recoverProducerUnderCurrentLease(ctx context.Context, journal *captureProducerJournal, paths []string) error {
	boundPaths := make(map[string]struct{}, len(journal.Artifacts))
	for _, artifact := range journal.Artifacts {
		if artifact.Segment != nil {
			boundPaths[artifact.Segment.Path] = struct{}{}
		}
	}
	unboundPaths := make([]string, 0, len(paths))
	for _, path := range paths {
		if _, bound := boundPaths[path]; !bound {
			unboundPaths = append(unboundPaths, path)
		}
	}
	pathIndex := 0
	accepted := false
	for index := range journal.Artifacts {
		artifact := &journal.Artifacts[index]
		if artifact.Done {
			accepted = true
			continue
		}
		path := ""
		if artifact.Segment != nil {
			path = artifact.Segment.Path
		}
		if path == "" && pathIndex < len(unboundPaths) {
			path = unboundPaths[pathIndex]
			pathIndex++
		}
		if path == "" {
			continue
		}
		probeCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		seg, err := w.recoverContinuousSegment(probeCtx, path, time.Duration(journal.ClipDurationSec)*time.Second)
		cancel()
		if err != nil {
			return fmt.Errorf("capture artifact remains partial under current lease: %w", err)
		}
		if artifact.Segment != nil {
			if artifact.Segment.SHA256 != seg.SHA256 || artifact.Segment.SizeBytes != seg.SizeBytes {
				return fmt.Errorf("capture artifact bytes changed after journal seal")
			}
			seg = *artifact.Segment
		} else {
			seg.CaptureSequence = artifact.CaptureSequence
			artifact.Segment = &seg
			if err = persistProducerJournal(w.surrenderJournalRoot(), journal); err != nil {
				return err
			}
		}
		intent, err := w.cfg.Client.SealCaptureArtifact(ctx, journal.JobID, journal.LeaseToken, artifact.IntentID, journal.ProducerID, artifact.CaptureSequence, seg.StartAt.UTC().UnixMilli(), seg.SizeBytes, seg.SHA256)
		if err != nil {
			return err
		}
		if !intent.AlreadyIngested {
			if err = w.cfg.Client.UploadFile(ctx, intent.UploadURL, seg.Path, seg.MIMEType); err != nil {
				return err
			}
			if _, err = w.cfg.Client.IngestClipWithResult(ctx, recoveryIngestRequest(journal, seg, artifact.IntentID, artifact.CaptureSequence)); err != nil {
				return err
			}
		}
		capture.RemoveSegmentFile(seg)
		artifact.Done = true
		artifact.Segment = nil
		accepted = true
		if err = persistProducerJournal(w.surrenderJournalRoot(), journal); err != nil {
			return err
		}
	}
	remaining, err := filepath.Glob(filepath.Join(journal.OutputDir, "seg-*.mp4"))
	if err != nil || len(remaining) != 0 {
		return fmt.Errorf("capture producer retains unbound bytes")
	}
	result := "abandoned_empty"
	if accepted {
		result = "completed"
	}
	return w.cfg.Client.FinishCaptureProducer(ctx, journal.JobID, journal.LeaseToken, journal.ProducerID, result, "")
}

// recoverProducerJournal is retained as a narrow test seam. All runtime
// recovery uses the exact-intent implementation above.
func (w *Worker) recoverProducerJournal(ctx context.Context, journal *captureProducerJournal) (bool, error) {
	return w.recoverProducerJournalV2(ctx, journal)
}

func recoveryIngestRequest(journal *captureProducerJournal, seg capture.Segment, intentID string, sequence int64) recordingapi.IngestClipRequest {
	return recordingapi.IngestClipRequest{
		IntentID: intentID, JobID: journal.JobID, SizeBytes: seg.SizeBytes, SHA256: seg.SHA256,
		DurationMs: seg.DurationMs, VideoCodec: seg.VideoCodec, AudioCodec: seg.AudioCodec,
		AudioPresent: seg.AudioPresent, ActualFPS: seg.ActualFPS, VideoWidth: seg.VideoWidth,
		VideoHeight: seg.VideoHeight, Container: seg.Container,
		ClipStartAt: seg.StartAt, ClipEndAt: seg.EndAt, LeaseToken: journal.LeaseToken,
		CaptureSequence: sequence, CaptureAttemptID: seg.CaptureAttemptID,
		TimestampContract: seg.TimestampContract, TimestampContractStatus: seg.TimestampContractStatus,
		TimestampContractReason: seg.TimestampContractReason,
	}
}

type captureProducerJournal struct {
	JobID               int64                    `json:"job_id"`
	LeaseToken          string                   `json:"lease_token"`
	ProducerID          string                   `json:"producer_id"`
	CaptureOrdinal      int64                    `json:"capture_ordinal"`
	OutputDir           string                   `json:"output_dir"`
	ClipDurationSec     int                      `json:"clip_duration_sec"`
	Artifacts           []captureArtifactJournal `json:"artifacts,omitempty"`
	CaptureSet          *captureSetJournal       `json:"capture_set,omitempty"`
	HadAcceptedArtifact bool                     `json:"had_accepted_artifact,omitempty"`
	path                string
}

type captureSetJournal struct {
	PlanID               string                       `json:"plan_id"`
	SetID                string                       `json:"set_id"`
	Seed                 string                       `json:"seed"`
	FirstCaptureSequence int64                        `json:"first_capture_sequence"`
	Plan                 *recordingapi.CaptureSetPlan `json:"plan,omitempty"`
	MerkleRootSHA256     string                       `json:"merkle_root_sha256,omitempty"`
	Committed            bool                         `json:"committed,omitempty"`
	tree                 *surrenderplan.Tree
}

type captureArtifactJournal struct {
	Ordinal              int              `json:"ordinal,omitempty"`
	IntentID             string           `json:"intent_id"`
	RecoverySecret       string           `json:"recovery_secret"`
	RecoverySecretSHA256 string           `json:"recovery_secret_sha256"`
	CaptureSequence      int64            `json:"capture_sequence"`
	Segment              *capture.Segment `json:"segment,omitempty"`
	Done                 bool             `json:"done,omitempty"`
}

func (w *Worker) materializeCaptureSetArtifact(ctx context.Context, job recordingapi.RecordingJob, producer *captureProducerJournal, sequence int64) (*captureArtifactJournal, error) {
	if producer == nil || producer.CaptureSet == nil || !producer.CaptureSet.Committed || producer.CaptureSet.Plan == nil {
		return nil, fmt.Errorf("capture set is not committed")
	}
	setJournal := producer.CaptureSet
	ordinal64 := sequence - setJournal.FirstCaptureSequence + 1
	if ordinal64 < 1 || ordinal64 > int64(setJournal.Plan.ArtifactCount) {
		return nil, fmt.Errorf("capture sequence is outside committed set")
	}
	ordinal := int(ordinal64)
	if setJournal.tree == nil {
		if err := validateCaptureSetJournal(producer); err != nil {
			return nil, err
		}
	}
	seedBytes, _ := hex.DecodeString(setJournal.Seed)
	var seed [32]byte
	copy(seed[:], seedBytes)
	set, err := captureSetIdentity(*setJournal.Plan)
	if err != nil {
		return nil, err
	}
	derived, err := surrenderplan.DeriveArtifact(seed, set, ordinal)
	if err != nil {
		return nil, err
	}
	proof, err := setJournal.tree.Proof(ordinal)
	if err != nil {
		return nil, err
	}
	proofHex := make([]string, len(proof.Siblings))
	for index := range proof.Siblings {
		proofHex[index] = hex.EncodeToString(proof.Siblings[index][:])
	}
	secretHex := hex.EncodeToString(derived.RecoverySecret[:])
	secretHashHex := hex.EncodeToString(derived.RecoverySecretHash[:])
	artifact := captureArtifactJournal{Ordinal: ordinal, IntentID: derived.ID.String(), RecoverySecret: secretHex, RecoverySecretSHA256: secretHashHex, CaptureSequence: sequence}
	state := w.surrenderState(job.JobID)
	state.mu.Lock()
	found := false
	for index := range producer.Artifacts {
		if producer.Artifacts[index].CaptureSequence == sequence {
			existing := &producer.Artifacts[index]
			if existing.Ordinal != ordinal || existing.IntentID != artifact.IntentID || existing.RecoverySecret != artifact.RecoverySecret || existing.RecoverySecretSHA256 != artifact.RecoverySecretSHA256 {
				state.mu.Unlock()
				return nil, fmt.Errorf("materialized capture artifact replay differs")
			}
			artifact = *existing
			found = true
			break
		}
	}
	if !found {
		producer.Artifacts = append(producer.Artifacts, artifact)
		if err := persistProducerJournal(w.surrenderJournalRoot(), producer); err != nil {
			producer.Artifacts = producer.Artifacts[:len(producer.Artifacts)-1]
			state.mu.Unlock()
			return nil, err
		}
	}
	state.mu.Unlock()

	if err := w.cfg.Client.MaterializeCaptureArtifact(ctx, job.JobID, job.LeaseToken, setJournal.SetID, ordinal, recordingapi.CaptureArtifactMaterialization{
		ArtifactID: artifact.IntentID, CaptureSequence: sequence, RecoverySecretSHA256: secretHashHex, Proof: proofHex,
	}); err != nil {
		return nil, err
	}
	return &artifact, nil
}

type surrenderJobState struct {
	mu         sync.Mutex
	head       acceptedUniqueHead
	producer   *captureProducerJournal
	leaseToken string
	active     bool
	recovering bool
}

func (w *Worker) surrenderState(jobID int64) *surrenderJobState {
	state, _ := w.surrenderJobs.LoadOrStore(jobID, &surrenderJobState{})
	return state.(*surrenderJobState)
}

func (w *Worker) beginActiveSurrenderJob(job recordingapi.RecordingJob) (*surrenderJobState, error) {
	state := w.surrenderState(job.JobID)
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.active || state.recovering {
		return nil, fmt.Errorf("recording surrender lifecycle already active")
	}
	if state.producer != nil {
		return nil, fmt.Errorf("prior capture producer still has recoverable authority")
	}
	if state.leaseToken != job.LeaseToken {
		state.leaseToken = job.LeaseToken
		state.head = acceptedUniqueHead{}
	}
	state.active = true
	return state, nil
}

func (w *Worker) endActiveSurrenderJob(state *surrenderJobState) {
	state.mu.Lock()
	state.active = false
	state.mu.Unlock()
}

func (s *surrenderJobState) markHead(result recordingapi.ClipIngestResult) {
	if result.HeadVersion <= 0 || strings.TrimSpace(result.UploadIntentID) == "" || result.ClipID <= 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if result.HeadVersion > s.head.Version {
		s.head = acceptedUniqueHead{Version: result.HeadVersion, UploadIntentID: result.UploadIntentID, ClipID: result.ClipID}
	}
}

func (s *surrenderJobState) snapshot() (acceptedUniqueHead, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.head, s.producer == nil
}

func (w *Worker) recordProducerArtifact(jobID int64, producer *captureProducerJournal, seg capture.Segment, intentID string) error {
	if producer == nil {
		return nil
	}
	state := w.surrenderState(jobID)
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.producer == nil || state.producer.ProducerID != producer.ProducerID {
		return fmt.Errorf("capture producer journal is not current")
	}
	for _, artifact := range producer.Artifacts {
		if artifact.CaptureSequence == seg.CaptureSequence {
			if artifact.IntentID != intentID {
				return fmt.Errorf("capture artifact intent differs from pre-byte reservation")
			}
			if artifact.Segment != nil && (artifact.Segment.Path != seg.Path || artifact.Segment.SHA256 != seg.SHA256 || artifact.Segment.SizeBytes != seg.SizeBytes) {
				return fmt.Errorf("capture artifact journal replay differs")
			}
			for index := range producer.Artifacts {
				if producer.Artifacts[index].CaptureSequence == seg.CaptureSequence {
					producer.Artifacts[index].Segment = &seg
					return persistProducerJournal(w.surrenderJournalRoot(), producer)
				}
			}
		}
	}
	return fmt.Errorf("capture artifact was not reserved before bytes")
}

func (w *Worker) acknowledgeProducerArtifact(jobID int64, producer *captureProducerJournal, path string) error {
	if producer == nil {
		return nil
	}
	state := w.surrenderState(jobID)
	state.mu.Lock()
	defer state.mu.Unlock()
	for index, artifact := range producer.Artifacts {
		if artifact.Segment == nil || artifact.Segment.Path != path {
			continue
		}
		if producer.CaptureSet != nil {
			producer.HadAcceptedArtifact = true
			producer.Artifacts = append(producer.Artifacts[:index], producer.Artifacts[index+1:]...)
		} else {
			producer.Artifacts[index].Done = true
			producer.Artifacts[index].Segment = nil
		}
		return persistProducerJournal(w.surrenderJournalRoot(), producer)
	}
	return nil
}

func (w *Worker) surrenderJournalRoot() string {
	root := strings.TrimSpace(w.cfg.CaptureTempDir)
	if root == "" {
		root = os.TempDir()
	}
	return filepath.Join(root, ".stoarama-surrender-v1")
}

func (w *Worker) surrenderTransportEnabled() bool {
	root := strings.TrimSpace(w.cfg.CaptureTempDir)
	if root == "" {
		return false
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return false
	}
	temp, err := filepath.Abs(os.TempDir())
	return err == nil && abs != temp
}

func ensurePrivateSurrenderJournalRoot(root string) error {
	if err := os.MkdirAll(root, 0o700); err != nil {
		return err
	}
	rootInfo, err := os.Lstat(root)
	if err != nil || !rootInfo.IsDir() || rootInfo.Mode()&os.ModeSymlink != 0 || rootInfo.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("surrender transport journal root is not private")
	}
	return nil
}

func persistProducerJournal(root string, journal *captureProducerJournal) error {
	if err := ensurePrivateSurrenderJournalRoot(root); err != nil {
		return err
	}
	if journal == nil || len(journal.Artifacts) > 2048 {
		return fmt.Errorf("capture producer journal exceeds artifact bound")
	}
	raw, err := json.Marshal(journal)
	if err != nil {
		return err
	}
	if len(raw) == 0 || len(raw)+1 > maxCaptureProducerJournalBytes {
		return fmt.Errorf("capture producer journal exceeds hard byte bound")
	}
	raw = append(raw, '\n')
	path := filepath.Join(root, fmt.Sprintf("job-%d-%s.json", journal.JobID, strings.ToLower(journal.ProducerID)))
	tmp, err := os.CreateTemp(root, ".capture-producer-*.tmp")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	if err = tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	_, encErr := tmp.Write(raw)
	if encErr == nil {
		encErr = tmp.Sync()
	}
	closeErr := tmp.Close()
	if encErr != nil {
		_ = os.Remove(tmpPath)
		return encErr
	}
	if closeErr != nil {
		_ = os.Remove(tmpPath)
		return closeErr
	}
	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	dir, err := os.Open(root)
	if err != nil {
		return err
	}
	err = dir.Sync()
	closeErr = dir.Close()
	if err == nil {
		err = closeErr
	}
	journal.path = path
	return err
}

func (w *Worker) reserveCaptureProducer(ctx context.Context, job recordingapi.RecordingJob, ordinal int64, outputDir string) (*captureProducerJournal, error) {
	if strings.TrimSpace(job.LeaseToken) == "" || job.SurrenderTransportVersion != 1 {
		return nil, nil
	}
	state := w.surrenderState(job.JobID)
	state.mu.Lock()
	var producer *captureProducerJournal
	if state.producer != nil {
		producer = state.producer
		if producer.CaptureOrdinal != ordinal {
			state.mu.Unlock()
			return nil, fmt.Errorf("capture producer %d remains nonterminal", producer.CaptureOrdinal)
		}
		if producer.OutputDir != outputDir {
			paths, globErr := filepath.Glob(filepath.Join(producer.OutputDir, "seg-*.mp4"))
			if globErr != nil || len(paths) != 0 {
				state.mu.Unlock()
				return nil, fmt.Errorf("capture producer %d retains durable bytes", producer.CaptureOrdinal)
			}
			producer.OutputDir = outputDir
			producer.ClipDurationSec = job.ClipDurationSec
			if err := persistProducerJournal(w.surrenderJournalRoot(), producer); err != nil {
				state.mu.Unlock()
				return nil, fmt.Errorf("reparent empty capture producer journal: %w", err)
			}
		}
	} else {
		producer = &captureProducerJournal{
			JobID: job.JobID, LeaseToken: job.LeaseToken, ProducerID: uuid.NewString(), CaptureOrdinal: ordinal,
			OutputDir: outputDir, ClipDurationSec: job.ClipDurationSec,
		}
		if err := persistProducerJournal(w.surrenderJournalRoot(), producer); err != nil {
			state.mu.Unlock()
			return nil, fmt.Errorf("persist capture producer reservation: %w", err)
		}
		state.producer = producer
	}
	state.mu.Unlock()
	limit := w.cfg.UploadWorkers
	if limit <= 0 {
		limit = defaultUploadWorkers
	}
	if limit > 8 {
		limit = 8
	}
	deadline := time.Now().Add(surrenderTransportRetryBudget)
	delays := []time.Duration{time.Second, 2 * time.Second, 5 * time.Second}
	for attempt := 0; ; attempt++ {
		callCtx, cancel, bounded := surrenderTransportCallContext(ctx, deadline)
		if !bounded {
			return nil, context.DeadlineExceeded
		}
		reserved, err := w.cfg.Client.ReserveCaptureProducer(callCtx, job.JobID, job.LeaseToken, producer.ProducerID, ordinal, limit)
		cancel()
		if err == nil {
			if reserved.ProducerID != producer.ProducerID || reserved.CaptureOrdinal != ordinal {
				return nil, fmt.Errorf("capture producer reservation response mismatch")
			}
			return producer, nil
		}
		if !retryableTransportError(ctx, err) {
			return nil, err
		}
		delay := delays[min(attempt, len(delays)-1)]
		if time.Now().Add(delay).After(deadline) {
			return nil, err
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, errors.Join(err, ctx.Err())
		case <-timer.C:
		}
	}
}

func (w *Worker) reserveCaptureSet(ctx context.Context, job recordingapi.RecordingJob, captureOrdinal, firstCaptureSequence int64, outputDir string) (*captureProducerJournal, surrenderplan.Plan, error) {
	if strings.TrimSpace(job.LeaseToken) == "" || job.SurrenderTransportVersion != 1 {
		return nil, surrenderplan.Plan{}, nil
	}
	state := w.surrenderState(job.JobID)
	state.mu.Lock()
	producer := state.producer
	if producer == nil {
		seed := make([]byte, 32)
		if _, err := rand.Read(seed); err != nil {
			state.mu.Unlock()
			return nil, surrenderplan.Plan{}, err
		}
		producer = &captureProducerJournal{
			JobID: job.JobID, LeaseToken: job.LeaseToken, ProducerID: uuid.NewString(), CaptureOrdinal: captureOrdinal,
			OutputDir: outputDir, ClipDurationSec: job.ClipDurationSec,
			CaptureSet: &captureSetJournal{PlanID: uuid.NewString(), SetID: uuid.NewString(), Seed: hex.EncodeToString(seed), FirstCaptureSequence: firstCaptureSequence},
		}
		if err := persistProducerJournal(w.surrenderJournalRoot(), producer); err != nil {
			state.mu.Unlock()
			return nil, surrenderplan.Plan{}, fmt.Errorf("persist pending capture set identity: %w", err)
		}
		state.producer = producer
	} else if producer.CaptureOrdinal != captureOrdinal || producer.JobID != job.JobID || producer.LeaseToken != job.LeaseToken || producer.CaptureSet == nil || producer.CaptureSet.FirstCaptureSequence != firstCaptureSequence {
		state.mu.Unlock()
		return nil, surrenderplan.Plan{}, fmt.Errorf("capture set replay differs from durable producer")
	}
	state.mu.Unlock()

	deadline := time.Now().Add(surrenderTransportRetryBudget)
	if producer.CaptureSet.Plan == nil {
		var plan recordingapi.CaptureSetPlan
		var lastErr error
		for attempt := 0; ; attempt++ {
			callCtx, cancel, bounded := surrenderTransportCallContext(ctx, deadline)
			if !bounded {
				return nil, surrenderplan.Plan{}, errors.Join(lastErr, context.DeadlineExceeded)
			}
			plan, lastErr = w.cfg.Client.PlanCaptureSet(callCtx, job.JobID, job.LeaseToken, producer.CaptureSet.PlanID, producer.CaptureSet.SetID, producer.ProducerID, captureOrdinal, firstCaptureSequence)
			cancel()
			if lastErr == nil {
				break
			}
			if !retryableTransportError(ctx, lastErr) || !waitSurrenderRetry(ctx, deadline, attempt) {
				return nil, surrenderplan.Plan{}, lastErr
			}
		}
		producer.CaptureSet.Plan = &plan
		if err := validateCaptureSetJournal(producer); err != nil {
			return nil, surrenderplan.Plan{}, err
		}
		root := producer.CaptureSet.tree.Root()
		producer.CaptureSet.MerkleRootSHA256 = hex.EncodeToString(root[:])
		if err := persistProducerJournal(w.surrenderJournalRoot(), producer); err != nil {
			return nil, surrenderplan.Plan{}, fmt.Errorf("persist server-authored capture plan: %w", err)
		}
	}
	if producer.CaptureSet.tree == nil {
		if err := validateCaptureSetJournal(producer); err != nil {
			return nil, surrenderplan.Plan{}, err
		}
	}
	if !producer.CaptureSet.Committed {
		var lastErr error
		for attempt := 0; ; attempt++ {
			callCtx, cancel, bounded := surrenderTransportCallContext(ctx, deadline)
			if !bounded {
				return nil, surrenderplan.Plan{}, errors.Join(lastErr, context.DeadlineExceeded)
			}
			lastErr = w.cfg.Client.CommitCaptureSet(callCtx, job.JobID, job.LeaseToken, producer.CaptureSet.PlanID, producer.CaptureSet.MerkleRootSHA256)
			cancel()
			if lastErr == nil {
				break
			}
			if !retryableTransportError(ctx, lastErr) || !waitSurrenderRetry(ctx, deadline, attempt) {
				return nil, surrenderplan.Plan{}, lastErr
			}
		}
		producer.CaptureSet.Committed = true
		if err := persistProducerJournal(w.surrenderJournalRoot(), producer); err != nil {
			return nil, surrenderplan.Plan{}, fmt.Errorf("persist accepted capture set: %w", err)
		}
	}
	plan := producer.CaptureSet.Plan
	return producer, surrenderplan.Plan{
		PlanAt: plan.PlanAt, WindowEnd: plan.WindowEndAt, ClipDurationSecond: plan.ClipDurationSeconds,
		DurationMicro: plan.DurationMicroseconds, ArtifactCount: plan.ArtifactCount, SplitTimesArgument: plan.SegmentTimesArgument,
	}, nil
}

func waitSurrenderRetry(ctx context.Context, deadline time.Time, attempt int) bool {
	delays := []time.Duration{time.Second, 2 * time.Second, 5 * time.Second}
	delay := delays[min(attempt, len(delays)-1)]
	if time.Now().Add(delay).After(deadline) {
		return false
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func captureArtifactReservationCount(job recordingapi.RecordingJob, now time.Time) (int, error) {
	if job.WindowEndAt == nil || job.ClipDurationSec <= 0 || !now.Before(job.WindowEndAt.UTC()) {
		return 0, fmt.Errorf("capture window has no bounded remaining artifact horizon")
	}
	clip := time.Duration(job.ClipDurationSec) * time.Second
	count := int((job.WindowEndAt.UTC().Sub(now)+clip-1)/clip) + 8
	if count < 1 || count > 2048 {
		return 0, fmt.Errorf("capture artifact horizon exceeds reservation bound")
	}
	return count, nil
}

func (w *Worker) reserveCaptureArtifactSlots(ctx context.Context, job recordingapi.RecordingJob, producer *captureProducerJournal, firstSequence int64, count int) error {
	if producer == nil {
		return nil
	}
	state := w.surrenderState(job.JobID)
	state.mu.Lock()
	if state.producer == nil || state.producer.ProducerID != producer.ProducerID {
		state.mu.Unlock()
		return fmt.Errorf("capture producer journal is not current")
	}
	if len(producer.Artifacts) == 0 {
		producer.Artifacts = make([]captureArtifactJournal, 0, count)
		for offset := 0; offset < count; offset++ {
			secret := make([]byte, 32)
			if _, err := rand.Read(secret); err != nil {
				state.mu.Unlock()
				return err
			}
			hash := sha256.Sum256(secret)
			producer.Artifacts = append(producer.Artifacts, captureArtifactJournal{IntentID: uuid.NewString(), RecoverySecret: hex.EncodeToString(secret), RecoverySecretSHA256: hex.EncodeToString(hash[:]), CaptureSequence: firstSequence + int64(offset)})
		}
		// The exact per-intent secrets reach stable storage before the server call and
		// before ffmpeg can open any output file.
		if err := persistProducerJournal(w.surrenderJournalRoot(), producer); err != nil {
			producer.Artifacts = nil
			state.mu.Unlock()
			return err
		}
	} else if len(producer.Artifacts) != count || producer.Artifacts[0].CaptureSequence != firstSequence {
		state.mu.Unlock()
		return fmt.Errorf("capture artifact reservation replay differs")
	}
	inputs := make([]recordingapi.CaptureArtifactReservationInput, len(producer.Artifacts))
	for index, artifact := range producer.Artifacts {
		inputs[index] = recordingapi.CaptureArtifactReservationInput{IntentID: artifact.IntentID, RecoverySecretSHA256: artifact.RecoverySecretSHA256, CaptureSequence: artifact.CaptureSequence}
	}
	state.mu.Unlock()
	deadline := time.Now().Add(surrenderTransportRetryBudget)
	delays := []time.Duration{time.Second, 2 * time.Second, 5 * time.Second}
	var lastErr error
	for attempt := 0; ; attempt++ {
		callCtx, cancel, bounded := surrenderTransportCallContext(ctx, deadline)
		if !bounded {
			if lastErr != nil {
				return lastErr
			}
			return context.DeadlineExceeded
		}
		lastErr = w.cfg.Client.ReserveCaptureArtifacts(callCtx, job.JobID, job.LeaseToken, producer.ProducerID, inputs)
		cancel()
		if lastErr == nil {
			return nil
		}
		if !retryableTransportError(ctx, lastErr) {
			return lastErr
		}
		delay := delays[min(attempt, len(delays)-1)]
		if time.Now().Add(delay).After(deadline) {
			return lastErr
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return errors.Join(lastErr, ctx.Err())
		case <-timer.C:
		}
	}
}

func captureArtifactForSequence(producer *captureProducerJournal, sequence int64) (*captureArtifactJournal, error) {
	if producer == nil {
		return nil, nil
	}
	for index := range producer.Artifacts {
		if producer.Artifacts[index].CaptureSequence == sequence {
			return &producer.Artifacts[index], nil
		}
	}
	return nil, fmt.Errorf("capture produced sequence %d without a pre-byte upload intent", sequence)
}

func captureProducerTerminalResult(producer *captureProducerJournal) string {
	if producer != nil {
		if producer.HadAcceptedArtifact {
			return "completed"
		}
		for _, artifact := range producer.Artifacts {
			if artifact.Done {
				return "completed"
			}
		}
	}
	return "abandoned_empty"
}

func (w *Worker) finishActiveCaptureProducer(ctx context.Context, job recordingapi.RecordingJob) error {
	state := w.surrenderState(job.JobID)
	state.mu.Lock()
	producer := state.producer
	state.mu.Unlock()
	if producer == nil {
		return nil
	}
	paths, err := filepath.Glob(filepath.Join(producer.OutputDir, "seg-*.mp4"))
	if err != nil {
		return err
	}
	if len(paths) != 0 {
		return fmt.Errorf("capture producer retains %d local artifacts", len(paths))
	}
	return w.finishCaptureProducer(ctx, job, producer, captureProducerTerminalResult(producer), "")
}

func (w *Worker) finishCaptureProducer(ctx context.Context, job recordingapi.RecordingJob, producer *captureProducerJournal, result, detail string) error {
	if producer == nil {
		return nil
	}
	deadline := time.Now().Add(surrenderTransportRetryBudget)
	delays := []time.Duration{time.Second, 2 * time.Second, 5 * time.Second}
	var lastErr error
	for attempt := 0; ; attempt++ {
		callCtx, cancel, bounded := surrenderTransportCallContext(ctx, deadline)
		if !bounded {
			if lastErr != nil {
				return lastErr
			}
			return context.DeadlineExceeded
		}
		if producer.CaptureSet != nil {
			lastErr = w.cfg.Client.FinishCaptureSet(callCtx, job.JobID, job.LeaseToken, producer.CaptureSet.SetID)
		} else {
			lastErr = w.cfg.Client.FinishCaptureProducer(callCtx, job.JobID, job.LeaseToken, producer.ProducerID, result, detail)
		}
		cancel()
		if lastErr == nil {
			state := w.surrenderState(job.JobID)
			state.mu.Lock()
			if state.producer != nil && state.producer.ProducerID == producer.ProducerID {
				state.producer = nil
			}
			state.mu.Unlock()
			if producer.path != "" {
				_ = os.Remove(producer.path)
			}
			return nil
		}
		if !retryableTransportError(ctx, lastErr) {
			return lastErr
		}
		delay := delays[min(attempt, len(delays)-1)]
		if time.Now().Add(delay).After(deadline) {
			return lastErr
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return errors.Join(lastErr, ctx.Err())
		case <-timer.C:
		}
	}
}

func surrenderAttemptID(job recordingapi.RecordingJob, reason recordingapi.SurrenderReason, errorText string, head acceptedUniqueHead) string {
	identity := fmt.Sprintf("%d\n%s\n%s\n%d:%s\n%d\n%s\n%d", job.JobID, job.LeaseToken, reason, len([]byte(errorText)), errorText, head.Version, head.UploadIntentID, head.ClipID)
	return uuid.NewSHA1(surrenderAttemptNamespace, []byte(identity)).String()
}

func canonicalSurrenderError(errorText string, reason recordingapi.SurrenderReason) string {
	text := strings.TrimSpace(errorText)
	if text == "" {
		text = string(reason)
	}
	text = strings.Map(func(r rune) rune {
		if r < 0x20 || r > 0x7e {
			return ' '
		}
		return r
	}, text)
	text = strings.TrimSpace(text)
	if len(text) > 500 {
		text = text[:500] + "..."
	}
	return text
}

func (w *Worker) surrenderRecordingJobV1(ctx context.Context, job recordingapi.RecordingJob, reason recordingapi.SurrenderReason, errorText string) (recordingapi.SurrenderResult, error) {
	if err := w.finishActiveCaptureProducer(ctx, job); err != nil {
		return recordingapi.SurrenderResult{}, fmt.Errorf("seal capture producer before surrender: %w", err)
	}
	head, empty := w.surrenderState(job.JobID).snapshot()
	if !empty {
		return recordingapi.SurrenderResult{Result: "ineligible_spool", CurrentHeadVersion: head.Version}, nil
	}
	errorText = canonicalSurrenderError(errorText, reason)
	req := recordingapi.SurrenderRequest{
		AttemptID: surrenderAttemptID(job, reason, errorText, head), Reason: reason, ErrorText: errorText,
		ExpectedHeadVersion: head.Version, ExpectedUploadIntent: head.UploadIntentID, ExpectedClipID: head.ClipID,
	}
	requestSHA := surrenderRequestDigest(req)
	var lastErr error
	appendObservation := func(observationType, errorClass string) error {
		return w.appendSurrenderTransportObservation(queuedSurrenderTransportObservation{
			JobID: job.JobID,
			SurrenderTransportObservation: recordingapi.SurrenderTransportObservation{
				ID: uuid.NewString(), LeaseToken: job.LeaseToken, AttemptID: req.AttemptID,
				Type: observationType, ErrorClass: errorClass, ObservedAt: time.Now().UTC(), RequestSHA256: requestSHA,
			},
		})
	}
	appendBudgetExhausted := func() {
		if err := appendObservation("transport_budget_exhausted", surrenderTransportErrorClass(lastErr)); err == nil {
			w.flushSurrenderTransportObservations(ctx)
		}
	}
	deadline := time.Now().Add(surrenderTransportRetryBudget)
	delays := []time.Duration{time.Second, 2 * time.Second, 5 * time.Second}
	for attempt := 0; ; attempt++ {
		callCtx, cancel, bounded := surrenderTransportCallContext(ctx, deadline)
		if !bounded {
			appendBudgetExhausted()
			if lastErr != nil {
				return recordingapi.SurrenderResult{}, lastErr
			}
			return recordingapi.SurrenderResult{}, context.DeadlineExceeded
		}
		if err := appendObservation("request_started", ""); err != nil {
			cancel()
			return recordingapi.SurrenderResult{}, err
		}
		result, err := w.cfg.Client.SurrenderRecordingJobV1(callCtx, job.JobID, job.LeaseToken, req)
		cancel()
		if err == nil {
			if observationErr := appendObservation("request_result_received", ""); observationErr != nil {
				return recordingapi.SurrenderResult{}, observationErr
			}
			w.flushSurrenderTransportObservations(ctx)
			w.surrenderState(job.JobID).markHead(recordingapi.ClipIngestResult{HeadVersion: result.CurrentHeadVersion, UploadIntentID: result.CurrentUploadIntentID, ClipID: result.CurrentClipID})
			return result, nil
		}
		if observationErr := appendObservation("request_transport_failed", surrenderTransportErrorClass(err)); observationErr != nil {
			return recordingapi.SurrenderResult{}, errors.Join(err, observationErr)
		}
		lastErr = err
		if !retryableTransportError(ctx, lastErr) {
			w.flushSurrenderTransportObservations(ctx)
			return recordingapi.SurrenderResult{}, lastErr
		}
		delay := delays[min(attempt, len(delays)-1)]
		if time.Now().Add(delay).After(deadline) {
			appendBudgetExhausted()
			return recordingapi.SurrenderResult{}, lastErr
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return recordingapi.SurrenderResult{}, errors.Join(lastErr, ctx.Err())
		case <-timer.C:
		}
	}
}

func surrenderTransportCallContext(ctx context.Context, absoluteDeadline time.Time) (context.Context, context.CancelFunc, bool) {
	now := time.Now()
	if !now.Before(absoluteDeadline) {
		return ctx, func() {}, false
	}
	callDeadline := now.Add(15 * time.Second)
	if absoluteDeadline.Before(callDeadline) {
		callDeadline = absoluteDeadline
	}
	callCtx, cancel := context.WithDeadline(ctx, callDeadline)
	return callCtx, cancel, true
}
