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
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/daydemir/stoarama/backend/internal/capture"
	"github.com/daydemir/stoarama/backend/internal/recordingapi"
	"github.com/google/uuid"
)

const surrenderTransportRetryBudget = time.Minute

var surrenderAttemptNamespace = uuid.MustParse("3cc7341c-d96b-4f38-b7de-cc47229837f9")

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
		if entry.Type()&os.ModeSymlink != 0 || entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		path := filepath.Join(root, entry.Name())
		raw, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		if len(raw) == 0 || len(raw) > 256<<10 {
			return nil, fmt.Errorf("invalid capture producer journal size")
		}
		var journal captureProducerJournal
		if err := json.Unmarshal(raw, &journal); err != nil {
			return nil, fmt.Errorf("decode capture producer journal: %w", err)
		}
		producerID, producerErr := uuid.Parse(journal.ProducerID)
		leaseToken, leaseErr := uuid.Parse(journal.LeaseToken)
		secret, secretErr := hex.DecodeString(journal.RecoverySecret)
		hash := sha256.Sum256(secret)
		if producerErr != nil || leaseErr != nil || journal.JobID <= 0 || journal.CaptureOrdinal <= 0 || journal.ClipDurationSec <= 0 || len(secret) != 32 || secretErr != nil || hex.EncodeToString(hash[:]) != journal.RecoverySecretSHA256 || strings.TrimSpace(journal.OutputDir) == "" || strings.TrimSpace(journal.ResolvedURL) == "" || len(journal.Artifacts) > 8 {
			return nil, fmt.Errorf("invalid capture producer journal identity")
		}
		seenIntents := make(map[string]struct{}, len(journal.Artifacts))
		seenSequences := make(map[int64]struct{}, len(journal.Artifacts))
		for _, artifact := range journal.Artifacts {
			intentID, intentErr := uuid.Parse(strings.TrimSpace(artifact.IntentID))
			seg := artifact.Segment
			if intentErr != nil || intentID.String() != strings.ToLower(strings.TrimSpace(artifact.IntentID)) || strings.TrimSpace(seg.Path) == "" || seg.CaptureSequence <= 0 || seg.SizeBytes <= 0 || seg.StartAt.IsZero() || len(seg.SHA256) != 64 || strings.ToLower(seg.SHA256) != seg.SHA256 {
				return nil, fmt.Errorf("invalid capture producer artifact journal")
			}
			if _, err := hex.DecodeString(seg.SHA256); err != nil {
				return nil, fmt.Errorf("invalid capture producer artifact digest")
			}
			if _, duplicate := seenIntents[intentID.String()]; duplicate {
				return nil, fmt.Errorf("duplicate capture producer artifact intent")
			}
			if _, duplicate := seenSequences[seg.CaptureSequence]; duplicate {
				return nil, fmt.Errorf("duplicate capture producer artifact sequence")
			}
			seenIntents[intentID.String()] = struct{}{}
			seenSequences[seg.CaptureSequence] = struct{}{}
		}
		journal.ProducerID = producerID.String()
		journal.LeaseToken = leaseToken.String()
		journal.path = path
		journals = append(journals, &journal)
	}
	return journals, nil
}

func (w *Worker) recoveryLoop(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		w.recoverProducerJournals(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (w *Worker) recoverProducerJournals(ctx context.Context) {
	journals, err := loadCaptureProducerJournals(w.surrenderJournalRoot())
	if err != nil {
		log.Printf("recording worker recovery journal error: %v", err)
		return
	}
	for _, journal := range journals {
		done, err := w.recoverProducerJournal(ctx, journal)
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("recording worker job=%d capture recovery pending: %v", journal.JobID, err)
		}
		if done {
			_ = os.Remove(journal.path)
			_ = os.RemoveAll(journal.OutputDir)
		}
	}
}

func (w *Worker) recoverProducerJournal(ctx context.Context, journal *captureProducerJournal) (bool, error) {
	state := w.surrenderState(journal.JobID)
	state.mu.Lock()
	if state.active || state.recovering {
		state.mu.Unlock()
		return false, nil
	}
	if state.producer != nil && state.producer.ProducerID != journal.ProducerID {
		state.mu.Unlock()
		return false, fmt.Errorf("different capture producer is active")
	}
	state.recovering = true
	state.producer = journal
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
	status, err := w.cfg.Client.RecordingRecoveryStatus(ctx, journal.ProducerID, journal.RecoverySecret)
	if err != nil {
		return false, err
	}
	if status.ProducerID != journal.ProducerID || status.JobID != journal.JobID || status.LeaseToken != journal.LeaseToken {
		return false, fmt.Errorf("recovery status identity mismatch")
	}
	switch status.ProducerResult {
	case "completed", "abandoned_empty", "host_unreachable_unrecoverable", "security_revoked":
		return true, nil
	}
	if !time.Now().Before(status.ExpiresAt) {
		return false, fmt.Errorf("expired recovery has no terminal server result")
	}
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
	byIntent := make(map[string]recordingapi.RecoveryArtifact, len(status.Artifacts))
	byByteRun := make(map[string]recordingapi.RecoveryArtifact, len(status.Artifacts))
	for _, artifact := range status.Artifacts {
		if _, duplicate := byIntent[artifact.IntentID]; duplicate {
			return false, fmt.Errorf("ambiguous recovery artifact identity")
		}
		byIntent[artifact.IntentID] = artifact
		key := recoveryByteRunKey(artifact.SegmentStartMs, artifact.SizeBytes, artifact.SHA256)
		if _, duplicate := byByteRun[key]; duplicate {
			return false, fmt.Errorf("ambiguous recovery byte-run correlation")
		}
		byByteRun[key] = artifact
	}
	journalByPath := make(map[string]captureArtifactJournal, len(journal.Artifacts))
	for _, artifact := range journal.Artifacts {
		journalByPath[artifact.Segment.Path] = artifact
	}
	nextSequence := status.NextCaptureSequence
	for _, path := range paths {
		probeCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		seg, probeErr := w.recoverContinuousSegment(probeCtx, path, time.Duration(journal.ClipDurationSec)*time.Second)
		cancel()
		if probeErr != nil {
			return false, fmt.Errorf("recovery artifact is not finalized")
		}
		sequence := nextSequence
		if local, exists := journalByPath[path]; exists {
			if local.Segment.SHA256 != seg.SHA256 || local.Segment.SizeBytes != seg.SizeBytes {
				return false, fmt.Errorf("recovery artifact bytes changed after journal seal")
			}
			seg = local.Segment
			sequence = local.Segment.CaptureSequence
			artifact, serverKnown := byIntent[local.IntentID]
			if !serverKnown || artifact.CaptureSequence != sequence || artifact.SegmentStartMs != seg.StartAt.UTC().UnixMilli() || artifact.SHA256 != seg.SHA256 || artifact.SizeBytes != seg.SizeBytes {
				return false, fmt.Errorf("recovery artifact journal differs from server seal")
			}
			if artifact.Result != "" {
				capture.RemoveSegmentFile(seg)
				_ = w.acknowledgeProducerArtifact(journal.JobID, journal, path)
				continue
			}
		} else if artifact, exists := byByteRun[recoveryByteRunKey(seg.StartAt.UTC().UnixMilli(), seg.SizeBytes, seg.SHA256)]; exists {
			sequence = artifact.CaptureSequence
			if artifact.Result != "" {
				capture.RemoveSegmentFile(seg)
				continue
			}
		} else {
			nextSequence++
		}
		seg.CaptureSequence = sequence
		intent, err := w.cfg.Client.ReserveClipUploadRecovery(ctx, journal.JobID, journal.LeaseToken, seg.MIMEType, seg.SHA256, seg.StartAt.UTC().UnixMilli(), journal.ProducerID, journal.RecoverySecret, sequence, seg.SizeBytes)
		if err != nil {
			return false, err
		}
		if !intent.AlreadyIngested {
			if err = w.recordProducerArtifact(journal.JobID, journal, seg, intent.IntentID); err != nil {
				return false, err
			}
		}
		if !intent.AlreadyIngested {
			if err = w.cfg.Client.UploadFile(ctx, intent.UploadURL, seg.Path, seg.MIMEType); err != nil {
				return false, err
			}
			_, err = w.cfg.Client.IngestClipRecovery(ctx, recoveryIngestRequest(journal, seg, intent.IntentID, sequence), journal.ProducerID, journal.RecoverySecret)
			if err != nil {
				return false, err
			}
		}
		capture.RemoveSegmentFile(seg)
		if err = w.acknowledgeProducerArtifact(journal.JobID, journal, path); err != nil {
			return false, err
		}
	}
	for _, local := range append([]captureArtifactJournal(nil), journal.Artifacts...) {
		if _, statErr := os.Stat(local.Segment.Path); statErr == nil || !os.IsNotExist(statErr) {
			continue
		}
		artifact, exists := byIntent[local.IntentID]
		if !exists {
			return false, fmt.Errorf("journaled recovery artifact has no server seal")
		}
		if artifact.Result == "" {
			if _, err = w.cfg.Client.IngestClipRecovery(ctx, recoveryIngestRequest(journal, local.Segment, local.IntentID, local.Segment.CaptureSequence), journal.ProducerID, journal.RecoverySecret); err != nil {
				return false, err
			}
		}
		if err = w.acknowledgeProducerArtifact(journal.JobID, journal, local.Segment.Path); err != nil {
			return false, err
		}
	}
	remaining, err := filepath.Glob(filepath.Join(outDir, "seg-*.mp4"))
	if err != nil || len(remaining) != 0 {
		return false, err
	}
	status, err = w.cfg.Client.RecordingRecoveryStatus(ctx, journal.ProducerID, journal.RecoverySecret)
	if err != nil {
		return false, err
	}
	for _, artifact := range status.Artifacts {
		if artifact.Result == "" {
			return false, fmt.Errorf("recovery artifact remains unresolved")
		}
	}
	result := "completed"
	if len(status.Artifacts) == 0 {
		result = "abandoned_empty"
	}
	if status.Authority == "current_lease" {
		if err := w.cfg.Client.FinishCaptureProducer(ctx, journal.JobID, journal.LeaseToken, journal.ProducerID, result, ""); err != nil {
			return false, err
		}
	} else if err := w.cfg.Client.FinishRecordingRecovery(ctx, journal.ProducerID, journal.RecoverySecret, result); err != nil {
		return false, err
	}
	done = true
	return true, nil
}

func recoveryByteRunKey(segmentStartMs, sizeBytes int64, sha string) string {
	return fmt.Sprintf("%d:%d:%s", segmentStartMs, sizeBytes, sha)
}

func recoveryIngestRequest(journal *captureProducerJournal, seg capture.Segment, intentID string, sequence int64) recordingapi.IngestClipRequest {
	return recordingapi.IngestClipRequest{
		IntentID: intentID, JobID: journal.JobID, SizeBytes: seg.SizeBytes, SHA256: seg.SHA256,
		DurationMs: seg.DurationMs, VideoCodec: seg.VideoCodec, AudioCodec: seg.AudioCodec,
		AudioPresent: seg.AudioPresent, ActualFPS: seg.ActualFPS, VideoWidth: seg.VideoWidth,
		VideoHeight: seg.VideoHeight, Container: seg.Container, ResolvedURL: journal.ResolvedURL,
		ClipStartAt: seg.StartAt, ClipEndAt: seg.EndAt, LeaseToken: journal.LeaseToken,
		CaptureSequence: sequence, CaptureAttemptID: seg.CaptureAttemptID,
		TimestampContract: seg.TimestampContract, TimestampContractStatus: seg.TimestampContractStatus,
		TimestampContractReason: seg.TimestampContractReason,
	}
}

type captureProducerJournal struct {
	JobID                int64                    `json:"job_id"`
	LeaseToken           string                   `json:"lease_token"`
	ProducerID           string                   `json:"producer_id"`
	CaptureOrdinal       int64                    `json:"capture_ordinal"`
	RecoverySecret       string                   `json:"recovery_secret"`
	RecoverySecretSHA256 string                   `json:"recovery_secret_sha256"`
	OutputDir            string                   `json:"output_dir"`
	ResolvedURL          string                   `json:"resolved_url"`
	ClipDurationSec      int                      `json:"clip_duration_sec"`
	Artifacts            []captureArtifactJournal `json:"artifacts,omitempty"`
	path                 string
}

type captureArtifactJournal struct {
	IntentID string          `json:"intent_id"`
	Segment  capture.Segment `json:"segment"`
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
		if artifact.Segment.Path == seg.Path || artifact.Segment.CaptureSequence == seg.CaptureSequence {
			if artifact.IntentID != intentID || artifact.Segment.Path != seg.Path || artifact.Segment.CaptureSequence != seg.CaptureSequence || artifact.Segment.SHA256 != seg.SHA256 || artifact.Segment.SizeBytes != seg.SizeBytes {
				return fmt.Errorf("capture artifact journal replay differs")
			}
			return nil
		}
	}
	producer.Artifacts = append(producer.Artifacts, captureArtifactJournal{IntentID: intentID, Segment: seg})
	if err := persistProducerJournal(w.surrenderJournalRoot(), producer); err != nil {
		producer.Artifacts = producer.Artifacts[:len(producer.Artifacts)-1]
		return err
	}
	return nil
}

func (w *Worker) acknowledgeProducerArtifact(jobID int64, producer *captureProducerJournal, path string) error {
	if producer == nil {
		return nil
	}
	state := w.surrenderState(jobID)
	state.mu.Lock()
	defer state.mu.Unlock()
	for index, artifact := range producer.Artifacts {
		if artifact.Segment.Path != path {
			continue
		}
		producer.Artifacts = append(producer.Artifacts[:index], producer.Artifacts[index+1:]...)
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

func persistProducerJournal(root string, journal *captureProducerJournal) error {
	if err := os.MkdirAll(root, 0o700); err != nil {
		return err
	}
	rootInfo, err := os.Lstat(root)
	if err != nil || !rootInfo.IsDir() || rootInfo.Mode()&os.ModeSymlink != 0 || rootInfo.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("capture producer journal root is not private")
	}
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
	encErr := json.NewEncoder(tmp).Encode(journal)
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

func (w *Worker) reserveCaptureProducer(ctx context.Context, job recordingapi.RecordingJob, ordinal int64, outputDir, resolvedURL string) (*captureProducerJournal, error) {
	if w.cfg.SkipDropletHeartbeat || strings.TrimSpace(job.LeaseToken) == "" || job.SurrenderTransportVersion != 1 {
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
			producer.ResolvedURL = resolvedURL
			producer.ClipDurationSec = job.ClipDurationSec
			if err := persistProducerJournal(w.surrenderJournalRoot(), producer); err != nil {
				state.mu.Unlock()
				return nil, fmt.Errorf("reparent empty capture producer journal: %w", err)
			}
		}
	} else {
		secret := make([]byte, 32)
		if _, err := rand.Read(secret); err != nil {
			state.mu.Unlock()
			return nil, err
		}
		secretHash := sha256.Sum256(secret)
		producer = &captureProducerJournal{
			JobID: job.JobID, LeaseToken: job.LeaseToken, ProducerID: uuid.NewString(), CaptureOrdinal: ordinal,
			RecoverySecret: hex.EncodeToString(secret), RecoverySecretSHA256: hex.EncodeToString(secretHash[:]),
			OutputDir: outputDir, ResolvedURL: resolvedURL, ClipDurationSec: job.ClipDurationSec,
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
		reserved, err := w.cfg.Client.ReserveCaptureProducer(callCtx, job.JobID, job.LeaseToken, producer.ProducerID, producer.RecoverySecretSHA256, ordinal, limit)
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
		lastErr = w.cfg.Client.FinishCaptureProducer(callCtx, job.JobID, job.LeaseToken, producer.ProducerID, result, detail)
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

func (w *Worker) surrenderRecordingJobV1(ctx context.Context, job recordingapi.RecordingJob, reason recordingapi.SurrenderReason, errorText string) (recordingapi.SurrenderResult, error) {
	head, empty := w.surrenderState(job.JobID).snapshot()
	if !empty {
		return recordingapi.SurrenderResult{Result: "ineligible_spool", CurrentHeadVersion: head.Version}, nil
	}
	req := recordingapi.SurrenderRequest{
		AttemptID: surrenderAttemptID(job, reason, errorText, head), Reason: reason, ErrorText: errorText,
		ExpectedHeadVersion: head.Version, ExpectedUploadIntent: head.UploadIntentID, ExpectedClipID: head.ClipID,
	}
	deadline := time.Now().Add(surrenderTransportRetryBudget)
	delays := []time.Duration{time.Second, 2 * time.Second, 5 * time.Second}
	var lastErr error
	for attempt := 0; ; attempt++ {
		callCtx, cancel, bounded := surrenderTransportCallContext(ctx, deadline)
		if !bounded {
			if lastErr != nil {
				return recordingapi.SurrenderResult{}, lastErr
			}
			return recordingapi.SurrenderResult{}, context.DeadlineExceeded
		}
		result, err := w.cfg.Client.SurrenderRecordingJobV1(callCtx, job.JobID, job.LeaseToken, req)
		cancel()
		if err == nil {
			w.surrenderState(job.JobID).markHead(recordingapi.ClipIngestResult{HeadVersion: result.CurrentHeadVersion, UploadIntentID: result.CurrentUploadIntentID, ClipID: result.CurrentClipID})
			return result, nil
		}
		lastErr = err
		if !retryableTransportError(ctx, lastErr) {
			return recordingapi.SurrenderResult{}, lastErr
		}
		delay := delays[min(attempt, len(delays)-1)]
		if time.Now().Add(delay).After(deadline) {
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
