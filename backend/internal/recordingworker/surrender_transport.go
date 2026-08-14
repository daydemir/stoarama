package recordingworker

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/daydemir/stoarama/backend/internal/apihttp"
	"github.com/daydemir/stoarama/backend/internal/capture"
	"github.com/daydemir/stoarama/backend/internal/recordingapi"
	"github.com/daydemir/stoarama/backend/internal/surrenderplan"
	"github.com/google/uuid"
	"golang.org/x/sys/unix"
)

const surrenderTransportRetryBudget = time.Minute

const maxCaptureProducerJournalBytes = 1 << 20

var surrenderAttemptNamespace = uuid.MustParse("3cc7341c-d96b-4f38-b7de-cc47229837f9")
var captureRecoveryReportNamespace = uuid.MustParse("3b3dd24e-199c-4b28-a9d4-3ad1fbd01da9")
var captureRecoverySessionNamespace = uuid.MustParse("ddeca08a-f772-4073-bc70-ec739f412681")
var captureSegmentLeafRE = regexp.MustCompile(`^seg-[0-9]{8}-[0-9]{6}\.mp4$`)

const claimSuccessorStateFile = ".claim-successor.json"

type claimSuccessorState struct {
	CurrentRawToken  string `json:"current_raw_token,omitempty"`
	CurrentKeyPrefix string `json:"current_key_prefix,omitempty"`
	CurrentSecretSHA string `json:"current_secret_sha256,omitempty"`
	ProposalID       string `json:"proposal_id,omitempty"`
	RawToken         string `json:"raw_token,omitempty"`
	KeyPrefix        string `json:"key_prefix,omitempty"`
	SecretSHA        string `json:"secret_sha256,omitempty"`
	Enabled          bool   `json:"enabled"`
	RotationRequired bool   `json:"rotation_required,omitempty"`
}

func captureUnixMicro(value time.Time) int64 {
	value = value.UTC()
	return value.Unix()*int64(time.Second/time.Microsecond) + int64(value.Nanosecond())/int64(time.Microsecond)
}

func hashRetainedArtifact(path string, expectedDevice, expectedInode uint64) (int64, string, error) {
	file, stat, err := openRetainedArtifact(path, expectedDevice, expectedInode)
	if err != nil {
		return 0, "", fmt.Errorf("retained capture artifact identity is unsafe")
	}
	defer file.Close()
	h := sha256.New()
	written, err := io.Copy(h, io.LimitReader(file, surrenderplan.RecoveryArtifactMaxBytes+1))
	if err != nil || written != stat.Size {
		return 0, "", fmt.Errorf("hash retained capture artifact: %w", err)
	}
	return written, hex.EncodeToString(h.Sum(nil)), nil
}

func openRetainedArtifact(path string, expectedDevice, expectedInode uint64) (*os.File, unix.Stat_t, error) {
	directory, leaf := filepath.Split(filepath.Clean(path))
	if directory == "" || !captureSegmentLeafRE.MatchString(leaf) {
		return nil, unix.Stat_t{}, fmt.Errorf("invalid retained artifact path")
	}
	directoryFD, err := unix.Open(filepath.Clean(directory), unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, unix.Stat_t{}, err
	}
	defer unix.Close(directoryFD)
	fileFD, err := unix.Openat(directoryFD, leaf, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, unix.Stat_t{}, err
	}
	var stat unix.Stat_t
	if err = unix.Fstat(fileFD, &stat); err != nil || stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Nlink != 1 || stat.Size <= 0 || stat.Size > surrenderplan.RecoveryArtifactMaxBytes || (expectedDevice != 0 && uint64(stat.Dev) != expectedDevice) || (expectedInode != 0 && uint64(stat.Ino) != expectedInode) {
		_ = unix.Close(fileFD)
		return nil, unix.Stat_t{}, fmt.Errorf("retained artifact identity changed")
	}
	file := os.NewFile(uintptr(fileFD), leaf)
	if file == nil {
		_ = unix.Close(fileFD)
		return nil, unix.Stat_t{}, fmt.Errorf("open retained artifact")
	}
	return file, stat, nil
}

type captureArtifactPath struct {
	Path   string
	Device uint64
	Inode  uint64
}

func listCaptureArtifactPaths(path string) ([]captureArtifactPath, error) {
	fd, err := unix.Open(filepath.Clean(path), unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if errors.Is(err, unix.ENOENT) || errors.Is(err, unix.ENOTDIR) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer unix.Close(fd)
	dupFD, err := unix.Dup(fd)
	if err != nil {
		return nil, err
	}
	directory := os.NewFile(uintptr(dupFD), filepath.Base(path))
	if directory == nil {
		_ = unix.Close(dupFD)
		return nil, fmt.Errorf("open capture namespace")
	}
	entries, readErr := directory.ReadDir(-1)
	closeErr := directory.Close()
	if readErr != nil {
		return nil, readErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	result := make([]captureArtifactPath, 0, len(entries))
	for _, entry := range entries {
		if !captureSegmentLeafRE.MatchString(entry.Name()) {
			return nil, fmt.Errorf("capture namespace contains an unauthorized leaf")
		}
		artifactFD, openErr := unix.Openat(fd, entry.Name(), unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
		if openErr != nil {
			return nil, openErr
		}
		var stat unix.Stat_t
		statErr := unix.Fstat(artifactFD, &stat)
		_ = unix.Close(artifactFD)
		if statErr != nil || stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Nlink != 1 || stat.Size <= 0 || stat.Size > surrenderplan.RecoveryArtifactMaxBytes {
			return nil, fmt.Errorf("capture artifact identity is unsafe")
		}
		result = append(result, captureArtifactPath{Path: filepath.Join(path, entry.Name()), Device: uint64(stat.Dev), Inode: uint64(stat.Ino)})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Path < result[j].Path })
	return result, nil
}

func (w *Worker) recoverContinuousSegmentExact(ctx context.Context, artifact captureArtifactPath, fallback time.Duration) (capture.Segment, error) {
	file, _, err := openRetainedArtifact(artifact.Path, artifact.Device, artifact.Inode)
	if err != nil {
		return capture.Segment{}, err
	}
	defer file.Close()
	if w.recoverContinuousSegmentFile == nil {
		return capture.Segment{}, fmt.Errorf("fd-bound recovery probe is unavailable")
	}
	segment, err := w.recoverContinuousSegmentFile(ctx, file, filepath.Base(artifact.Path), fallback)
	segment.Path = artifact.Path
	return segment, err
}

func removeRetainedArtifact(path string, expectedDevice, expectedInode uint64) error {
	file, _, err := openRetainedArtifact(path, expectedDevice, expectedInode)
	if err != nil {
		return err
	}
	if err = file.Close(); err != nil {
		return err
	}
	directory, leaf := filepath.Split(filepath.Clean(path))
	directoryFD, err := unix.Open(filepath.Clean(directory), unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	defer unix.Close(directoryFD)
	// Re-open and compare immediately before unlink so a pathname substitution
	// can never make recovery delete bytes other than the inventoried artifact.
	checkFD, err := unix.Openat(directoryFD, leaf, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	var stat unix.Stat_t
	statErr := unix.Fstat(checkFD, &stat)
	_ = unix.Close(checkFD)
	if statErr != nil || uint64(stat.Dev) != expectedDevice || uint64(stat.Ino) != expectedInode || stat.Nlink != 1 {
		return fmt.Errorf("retained artifact changed before cleanup")
	}
	return unix.Unlinkat(directoryFD, leaf, 0)
}

func removeEmptyRetainedDirectory(path string) error {
	parent, leaf := filepath.Split(filepath.Clean(path))
	if parent == "" || leaf == "" || leaf == "." || leaf == string(filepath.Separator) {
		return fmt.Errorf("invalid retained directory path")
	}
	parentFD, err := unix.Open(filepath.Clean(parent), unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	defer unix.Close(parentFD)
	directoryFD, err := unix.Openat(parentFD, leaf, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if errors.Is(err, unix.ENOENT) {
		return nil
	}
	if err != nil {
		return err
	}
	dupFD, err := unix.Dup(directoryFD)
	_ = unix.Close(directoryFD)
	if err != nil {
		return err
	}
	directory := os.NewFile(uintptr(dupFD), leaf)
	if directory == nil {
		_ = unix.Close(dupFD)
		return fmt.Errorf("open retained directory")
	}
	entries, readErr := directory.ReadDir(1)
	closeErr := directory.Close()
	if readErr == nil || len(entries) != 0 {
		return fmt.Errorf("retained directory is not empty")
	}
	if !errors.Is(readErr, io.EOF) {
		return readErr
	}
	if closeErr != nil {
		return closeErr
	}
	return unix.Unlinkat(parentFD, leaf, unix.AT_REMOVEDIR)
}

func removeCaptureNamespaceSentinel(path string) error {
	parent, leaf := filepath.Split(filepath.Clean(path))
	if parent == "" || leaf == "" {
		return fmt.Errorf("invalid capture sentinel path")
	}
	parentFD, err := unix.Open(filepath.Clean(parent), unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	defer unix.Close(parentFD)
	fd, err := unix.Openat(parentFD, leaf, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if errors.Is(err, unix.ENOENT) {
		return nil
	}
	if err != nil {
		return err
	}
	var stat unix.Stat_t
	statErr := unix.Fstat(fd, &stat)
	_ = unix.Close(fd)
	if statErr != nil || stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Nlink != 1 || stat.Size != 0 {
		return fmt.Errorf("capture namespace sentinel identity is unsafe")
	}
	return unix.Unlinkat(parentFD, leaf, 0)
}

type acceptedUniqueHead struct {
	Version        int64
	UploadIntentID string
	ClipID         int64
}

func openPrivateDirectory(path string) (int, error) {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return -1, err
	}
	var stat unix.Stat_t
	if err = unix.Fstat(fd, &stat); err != nil || stat.Mode&unix.S_IFMT != unix.S_IFDIR || stat.Mode&0o077 != 0 {
		_ = unix.Close(fd)
		return -1, fmt.Errorf("private directory identity is unsafe")
	}
	return fd, nil
}

func readPrivateRegularAt(directoryFD int, name string, maxBytes int64) ([]byte, error) {
	if name == "" || filepath.Base(name) != name {
		return nil, fmt.Errorf("invalid private state leaf")
	}
	fd, err := unix.Openat(directoryFD, name, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), name)
	if file == nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("open private state leaf")
	}
	defer file.Close()
	var stat unix.Stat_t
	if err = unix.Fstat(fd, &stat); err != nil || stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Nlink != 1 || stat.Mode&0o077 != 0 || stat.Size <= 0 || stat.Size > maxBytes {
		return nil, fmt.Errorf("private state leaf identity is unsafe")
	}
	raw, err := io.ReadAll(io.LimitReader(file, maxBytes+1))
	if err != nil || int64(len(raw)) != stat.Size {
		return nil, fmt.Errorf("read private state leaf: %w", err)
	}
	return raw, nil
}

func writePrivateRegularAt(directoryFD int, name string, raw []byte) error {
	if name == "" || filepath.Base(name) != name || len(raw) == 0 {
		return fmt.Errorf("invalid private state write")
	}
	tempName := ".pending-" + uuid.NewString()
	fd, err := unix.Openat(directoryFD, tempName, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0o600)
	if err != nil {
		return err
	}
	file := os.NewFile(uintptr(fd), tempName)
	if file == nil {
		_ = unix.Close(fd)
		return fmt.Errorf("open private state temporary leaf")
	}
	_, writeErr := file.Write(raw)
	if writeErr == nil {
		writeErr = file.Sync()
	}
	closeErr := file.Close()
	if writeErr == nil {
		writeErr = closeErr
	}
	if writeErr == nil {
		writeErr = unix.Renameat(directoryFD, tempName, directoryFD, name)
	}
	if writeErr != nil {
		_ = unix.Unlinkat(directoryFD, tempName, 0)
		return writeErr
	}
	return unix.Fsync(directoryFD)
}

func unlinkPrivateLeaf(root, path string) error {
	if filepath.Dir(path) != root || filepath.Base(path) == "." {
		return fmt.Errorf("private state deletion escaped root")
	}
	rootFD, err := openPrivateDirectory(root)
	if err != nil {
		return err
	}
	defer unix.Close(rootFD)
	if err = unix.Unlinkat(rootFD, filepath.Base(path), 0); errors.Is(err, unix.ENOENT) {
		return nil
	}
	if err != nil {
		return err
	}
	return unix.Fsync(rootFD)
}

func infoSyscallStat(info os.FileInfo) (*syscall.Stat_t, bool) {
	if info == nil {
		return nil, false
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	return stat, ok
}

func loadCaptureProducerJournals(root string) ([]*captureProducerJournal, error) {
	rootFD, statErr := openPrivateDirectory(root)
	if errors.Is(statErr, unix.ENOENT) {
		return nil, nil
	}
	if statErr != nil {
		return nil, fmt.Errorf("capture producer journal root is not private")
	}
	defer unix.Close(rootFD)
	dupFD, err := unix.Dup(rootFD)
	if err != nil {
		return nil, err
	}
	rootFile := os.NewFile(uintptr(dupFD), filepath.Base(root))
	entries, err := rootFile.ReadDir(-1)
	closeErr := rootFile.Close()
	if err != nil {
		return nil, err
	}
	if closeErr != nil {
		return nil, closeErr
	}
	journals := make([]*captureProducerJournal, 0, len(entries))
	for _, entry := range entries {
		if entry.Name() == filepath.Base(surrenderTransportObservationPath(root)) || entry.Name() == claimSuccessorStateFile {
			continue
		}
		if !strings.HasSuffix(entry.Name(), ".json") {
			return nil, fmt.Errorf("ambiguous surrender recovery inventory entry")
		}
		path := filepath.Join(root, entry.Name())
		raw, err := readPrivateRegularAt(rootFD, entry.Name(), maxCaptureProducerJournalBytes)
		if err != nil {
			return nil, err
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
		outputFD, outputErr := unix.Open(outputDir, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
		if outputErr == nil {
			_ = unix.Close(outputFD)
		} else if !errors.Is(outputErr, unix.ENOENT) && !errors.Is(outputErr, unix.ENOTDIR) {
			return nil, outputErr
		}
		seenIntents := make(map[string]struct{}, len(journal.Artifacts))
		seenSequences := make(map[int64]struct{}, len(journal.Artifacts))
		for _, artifact := range journal.Artifacts {
			intentID, intentErr := uuid.Parse(strings.TrimSpace(artifact.IntentID))
			secret, secretErr := hex.DecodeString(artifact.RecoverySecret)
			hash := sha256.Sum256(secret)
			seg := artifact.Segment
			localPath := strings.TrimSpace(artifact.LocalPath)
			if seg != nil && localPath == "" {
				localPath = seg.Path
			}
			if intentErr != nil || intentID.String() != strings.ToLower(strings.TrimSpace(artifact.IntentID)) || artifact.CaptureSequence <= 0 || len(secret) != 32 || secretErr != nil || hex.EncodeToString(hash[:]) != artifact.RecoverySecretSHA256 {
				return nil, fmt.Errorf("invalid capture producer artifact journal")
			}
			if localPath != "" {
				absolutePath, pathErr := filepath.Abs(localPath)
				if pathErr != nil || filepath.Dir(absolutePath) != outputDir || !captureSegmentLeafRE.MatchString(filepath.Base(absolutePath)) || artifact.Device == 0 || artifact.Inode == 0 {
					return nil, fmt.Errorf("invalid capture artifact local path")
				}
				file, _, identityErr := openRetainedArtifact(absolutePath, artifact.Device, artifact.Inode)
				if identityErr != nil {
					return nil, fmt.Errorf("capture artifact local identity changed")
				}
				if err := file.Close(); err != nil {
					return nil, err
				}
			}
			if seg != nil {
				segmentPath, segmentErr := filepath.Abs(seg.Path)
				if segmentErr != nil || filepath.Dir(segmentPath) != outputDir || !captureSegmentLeafRE.MatchString(filepath.Base(segmentPath)) || seg.CaptureSequence != artifact.CaptureSequence || seg.SizeBytes <= 0 || seg.StartAt.IsZero() || len(seg.SHA256) != 64 || strings.ToLower(seg.SHA256) != seg.SHA256 {
					return nil, fmt.Errorf("invalid sealed capture producer artifact journal")
				}
				file, _, statErr := openRetainedArtifact(segmentPath, artifact.Device, artifact.Inode)
				if statErr != nil {
					return nil, fmt.Errorf("capture artifact path identity is unsafe")
				}
				if err := file.Close(); err != nil {
					return nil, err
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
			if journal.CaptureSet.StopAck != nil {
				members := make(map[int]recordingapi.CaptureStopAckMember, len(journal.CaptureSet.StopAck.Members))
				for _, member := range journal.CaptureSet.StopAck.Members {
					members[member.Ordinal] = member
				}
				for _, artifact := range journal.Artifacts {
					if artifact.LocalPath == "" {
						continue
					}
					member, exists := members[artifact.Ordinal]
					if !exists || member.Device != artifact.Device || member.Inode != artifact.Inode || member.RelativeName != filepath.Base(artifact.LocalPath) {
						return nil, fmt.Errorf("capture stop inventory differs from local artifact")
					}
				}
			}
		}
		journal.ProducerID = producerID.String()
		journal.LeaseToken = leaseToken.String()
		journal.path = path
		journals = append(journals, &journal)
	}
	return journals, nil
}

func claimSuccessorStatePath(root string) string { return filepath.Join(root, claimSuccessorStateFile) }

func validClaimCredential(raw, prefix, digest string) bool {
	secretHash := sha256.Sum256([]byte(strings.TrimSpace(raw)))
	return strings.HasPrefix(raw, "sin_") && len(prefix) >= 8 && len(prefix) <= 32 && strings.HasPrefix(raw, prefix) && hex.EncodeToString(secretHash[:]) == digest
}

func persistClaimSuccessorState(root string, state claimSuccessorState) error {
	if err := ensurePrivateSurrenderJournalRoot(root); err != nil {
		return err
	}
	raw, err := json.Marshal(state)
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	rootFD, err := openPrivateDirectory(root)
	if err != nil {
		return err
	}
	defer unix.Close(rootFD)
	return writePrivateRegularAt(rootFD, claimSuccessorStateFile, raw)
}

func loadClaimSuccessorState(root string) (*claimSuccessorState, error) {
	rootFD, err := openPrivateDirectory(root)
	if errors.Is(err, unix.ENOENT) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer unix.Close(rootFD)
	raw, err := readPrivateRegularAt(rootFD, claimSuccessorStateFile, 4096)
	if errors.Is(err, unix.ENOENT) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("claim successor state is not a private regular file")
	}
	var state claimSuccessorState
	if err = json.Unmarshal(raw, &state); err != nil {
		return nil, err
	}
	hasCurrent := state.CurrentRawToken != "" || state.CurrentKeyPrefix != "" || state.CurrentSecretSHA != ""
	hasPending := state.ProposalID != "" || state.RawToken != "" || state.KeyPrefix != "" || state.SecretSHA != ""
	_, proposalErr := uuid.Parse(state.ProposalID)
	if (!hasCurrent && !hasPending && !state.RotationRequired) || (hasCurrent && !validClaimCredential(state.CurrentRawToken, state.CurrentKeyPrefix, state.CurrentSecretSHA)) || (hasPending && (!validClaimCredential(state.RawToken, state.KeyPrefix, state.SecretSHA) || proposalErr != nil)) || (state.Enabled && !hasPending) {
		return nil, fmt.Errorf("claim successor state identity is invalid")
	}
	return &state, nil
}

func newClaimSuccessorState() (claimSuccessorState, error) {
	random := make([]byte, 36)
	if _, err := rand.Read(random); err != nil {
		return claimSuccessorState{}, err
	}
	raw := "sin_" + base64.RawURLEncoding.EncodeToString(random)
	prefix := raw
	if len(prefix) > 16 {
		prefix = prefix[:16]
	}
	digest := sha256.Sum256([]byte(raw))
	return claimSuccessorState{ProposalID: uuid.New().String(), RawToken: raw, KeyPrefix: prefix, SecretSHA: hex.EncodeToString(digest[:])}, nil
}

func (w *Worker) restoreClaimCredential() error {
	state, err := loadClaimSuccessorState(w.surrenderJournalRoot())
	if err != nil || state == nil {
		return err
	}
	raw := state.CurrentRawToken
	if raw == "" {
		// A locally prepared but server-unacknowledged successor never replaces
		// the bootstrap/predecessor bearer on restart.
		return nil
	}
	return w.cfg.Client.SetNodeToken(raw)
}

func promoteClaimSuccessorState(root string, state *claimSuccessorState) error {
	state.CurrentRawToken = state.RawToken
	state.CurrentKeyPrefix = state.KeyPrefix
	state.CurrentSecretSHA = state.SecretSHA
	state.ProposalID, state.RawToken, state.KeyPrefix, state.SecretSHA = "", "", "", ""
	state.Enabled = false
	state.RotationRequired = false
	return persistClaimSuccessorState(root, *state)
}

func (w *Worker) maybeRotateClaimCredential(ctx context.Context) error {
	root := w.surrenderJournalRoot()
	state, err := loadClaimSuccessorState(root)
	if err != nil {
		return err
	}
	if state != nil {
		raw := state.CurrentRawToken
		if raw != "" {
			err = w.cfg.Client.SetNodeToken(raw)
		}
		if err != nil {
			return err
		}
	}
	created := state == nil || state.ProposalID == ""
	if created {
		pending, createErr := newClaimSuccessorState()
		if createErr != nil {
			return createErr
		}
		if state == nil {
			state = &claimSuccessorState{}
		}
		state.ProposalID, state.RawToken, state.KeyPrefix, state.SecretSHA = pending.ProposalID, pending.RawToken, pending.KeyPrefix, pending.SecretSHA
		state.Enabled = false
		state.RotationRequired = true
		if err = persistClaimSuccessorState(root, *state); err != nil {
			return err
		}
	}
	if state.Enabled {
		// The enabled successor can claim new work and service older exact
		// same-node fences immediately. Predecessor retirement may wait for
		// unrelated leases and does not delay the local bearer switch.
		_, ackErr := w.cfg.Client.AckClaimSuccessor(ctx, state.ProposalID, state.RawToken)
		if ackErr != nil {
			return ackErr
		}
		if err = w.cfg.Client.SetNodeToken(state.RawToken); err != nil {
			return err
		}
		return promoteClaimSuccessorState(root, state)
	}
	_, err = w.cfg.Client.ProposeClaimSuccessor(ctx, state.ProposalID, state.KeyPrefix, state.SecretSHA)
	if err != nil {
		var statusErr *apihttp.StatusError
		if errors.As(err, &statusErr) && statusErr.Code == http.StatusTooEarly {
			return nil
		}
		if created && errors.As(err, &statusErr) && statusErr.Code == http.StatusConflict {
			if state.CurrentRawToken == "" && !state.RotationRequired {
				_ = unlinkPrivateLeaf(root, claimSuccessorStatePath(root))
			} else {
				state.ProposalID, state.RawToken, state.KeyPrefix, state.SecretSHA = "", "", "", ""
				state.Enabled = false
				_ = persistClaimSuccessorState(root, *state)
			}
			return nil
		}
		return err
	}
	// Persist the successor-issued phase before its ACK. The ACK uses the new
	// bearer explicitly. Once acknowledged it is also authorized for the old
	// same-node fences, so the ordinary client switches without waiting for the
	// predecessor's independent retirement.
	state.Enabled = true
	if err = persistClaimSuccessorState(root, *state); err != nil {
		return err
	}
	_, err = w.cfg.Client.AckClaimSuccessor(ctx, state.ProposalID, state.RawToken)
	if err != nil {
		return err
	}
	if err = w.cfg.Client.SetNodeToken(state.RawToken); err != nil {
		return err
	}
	return promoteClaimSuccessorState(root, state)
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
	if setJournal.StopAck != nil {
		ackID, ackErr := uuid.Parse(strings.TrimSpace(setJournal.StopAck.AckID))
		inventorySHA, inventoryErr := recordingapi.CaptureStopInventorySHA(*setJournal.StopAck)
		if ackErr != nil || ackID.String() != setJournal.StopAck.AckID || inventoryErr != nil || inventorySHA != setJournal.StopAck.InventorySHA256 || setJournal.StopAck.RetainedDirectoryInode == 0 || len(setJournal.StopAck.Members) > plan.ArtifactCount {
			return fmt.Errorf("capture stop acknowledgment journal is invalid")
		}
		seen := make(map[int]struct{}, len(setJournal.StopAck.Members))
		for _, member := range setJournal.StopAck.Members {
			if member.Ordinal < 1 || member.Ordinal > plan.ArtifactCount || member.CaptureSequence != plan.FirstCaptureSequence+int64(member.Ordinal-1) || member.Inode == 0 || !captureSegmentLeafRE.MatchString(member.RelativeName) {
				return fmt.Errorf("capture stop acknowledgment member is invalid")
			}
			if _, duplicate := seen[member.Ordinal]; duplicate {
				return fmt.Errorf("capture stop acknowledgment member is duplicated")
			}
			seen[member.Ordinal] = struct{}{}
		}
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

func loadSurrenderTransportObservations(root string) ([]queuedSurrenderTransportObservation, error) {
	rootFD, err := openPrivateDirectory(root)
	if errors.Is(err, unix.ENOENT) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer unix.Close(rootFD)
	raw, err := readPrivateRegularAt(rootFD, filepath.Base(surrenderTransportObservationPath(root)), 256<<10)
	if errors.Is(err, unix.ENOENT) {
		return nil, nil
	}
	if err != nil || len(raw) == 0 {
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
		rootFD, openErr := openPrivateDirectory(root)
		if errors.Is(openErr, unix.ENOENT) {
			return nil
		}
		if openErr != nil {
			return openErr
		}
		defer unix.Close(rootFD)
		if err := unix.Unlinkat(rootFD, filepath.Base(path), 0); err != nil && !errors.Is(err, unix.ENOENT) {
			return err
		}
		return unix.Fsync(rootFD)
	}
	raw, err := json.Marshal(observations)
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	rootFD, err := openPrivateDirectory(root)
	if err != nil {
		return err
	}
	defer unix.Close(rootFD)
	return writePrivateRegularAt(rootFD, filepath.Base(path), raw)
}

func (w *Worker) appendSurrenderTransportObservation(observation queuedSurrenderTransportObservation) error {
	w.surrenderObservationMu.Lock()
	defer w.surrenderObservationMu.Unlock()
	root := w.surrenderJournalRoot()
	observations, err := loadSurrenderTransportObservations(root)
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
	observations, err := loadSurrenderTransportObservations(root)
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
	rotateAfterRecovery := false
	for _, journal := range journals {
		done, err := w.recoverProducerJournalV2(ctx, journal)
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("recording worker job=%d capture recovery pending: %v", journal.JobID, err)
		}
		if done {
			if journal.RecoveryGrantSeen {
				rotateAfterRecovery = true
				if stateErr := persistRecoveryRotationRequirement(w.surrenderJournalRoot()); stateErr != nil {
					return stateErr
				}
			}
			if err := unlinkPrivateLeaf(w.surrenderJournalRoot(), journal.path); err != nil {
				return fmt.Errorf("retire recovered capture journal: %w", err)
			}
			if err := removeEmptyRetainedDirectory(journal.OutputDir); err != nil && !errors.Is(err, unix.ENOENT) {
				return fmt.Errorf("retire recovered capture directory: %w", err)
			}
		}
	}
	claimState, stateErr := loadClaimSuccessorState(w.surrenderJournalRoot())
	if stateErr != nil {
		return stateErr
	}
	if rotateAfterRecovery || claimState != nil && (claimState.RotationRequired || claimState.ProposalID != "") {
		if err := w.maybeRotateClaimCredential(ctx); err != nil {
			return fmt.Errorf("rotate recovered claim credential: %w", err)
		}
	}
	return nil
}

func persistRecoveryRotationRequirement(root string) error {
	state, err := loadClaimSuccessorState(root)
	if err != nil {
		return err
	}
	if state == nil {
		state = &claimSuccessorState{}
	}
	state.RotationRequired = true
	if err = persistClaimSuccessorState(root, *state); err != nil {
		return fmt.Errorf("persist recovery rotation requirement: %w", err)
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
	paths, err := listCaptureArtifactPaths(outDir)
	if err != nil {
		return false, err
	}
	pathIdentity := make(map[string]captureArtifactPath, len(paths))
	for _, path := range paths {
		pathIdentity[path.Path] = path
	}
	for index := range journal.Artifacts {
		artifact := &journal.Artifacts[index]
		localPath := artifact.LocalPath
		if artifact.Segment != nil {
			localPath = artifact.Segment.Path
		}
		if identity, exists := pathIdentity[localPath]; exists {
			if (artifact.Device != 0 && artifact.Device != identity.Device) || (artifact.Inode != 0 && artifact.Inode != identity.Inode) {
				return false, fmt.Errorf("capture artifact identity changed before recovery")
			}
			artifact.LocalPath, artifact.Device, artifact.Inode = identity.Path, identity.Device, identity.Inode
		}
	}
	if journal.CaptureSet != nil {
		setDone, setErr := w.recoverCaptureSetJournal(ctx, journal, paths)
		done = setDone
		return setDone, setErr
	}
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
	unboundPaths := make([]captureArtifactPath, 0, len(paths))
	for _, path := range paths {
		if _, bound := boundPaths[path.Path]; !bound {
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
				if err := removeRetainedArtifact(artifact.Segment.Path, artifact.Device, artifact.Inode); err != nil {
					return false, err
				}
			}
			artifact.Done = true
			artifact.Segment = nil
			if err := persistProducerJournal(w.surrenderJournalRoot(), journal); err != nil {
				return false, err
			}
			continue
		}
		path := captureArtifactPath{}
		if artifact.Segment != nil {
			path = captureArtifactPath{Path: artifact.Segment.Path, Device: artifact.Device, Inode: artifact.Inode}
		}
		if path.Path == "" && pathIndex < len(unboundPaths) {
			path = unboundPaths[pathIndex]
			pathIndex++
		}
		if path.Path == "" {
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
		seg, probeErr := w.recoverContinuousSegmentExact(probeCtx, path, time.Duration(journal.ClipDurationSec)*time.Second)
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
			artifact.LocalPath, artifact.Device, artifact.Inode = path.Path, path.Device, path.Inode
			if err := persistProducerJournal(w.surrenderJournalRoot(), journal); err != nil {
				return false, err
			}
		}
		intent, err := w.cfg.Client.SealCaptureArtifactRecovery(ctx, journal.JobID, journal.LeaseToken, artifact.IntentID, artifact.RecoverySecret, journal.ProducerID, artifact.CaptureSequence, seg.StartAt.UTC().UnixMilli(), seg.SizeBytes, seg.SHA256)
		if err != nil {
			return false, err
		}
		if !intent.AlreadyIngested {
			if err = w.cfg.Client.UploadFileExact(ctx, intent.UploadURL, seg.Path, seg.MIMEType, artifact.Device, artifact.Inode, seg.SizeBytes); err != nil {
				return false, err
			}
			if _, err = w.cfg.Client.IngestClipRecovery(ctx, recoveryIngestRequest(journal, seg, artifact.IntentID, artifact.CaptureSequence), artifact.IntentID, artifact.RecoverySecret); err != nil {
				return false, err
			}
		}
		if err = removeRetainedArtifact(seg.Path, artifact.Device, artifact.Inode); err != nil {
			return false, err
		}
		artifact.Done = true
		artifact.Segment = nil
		if err = persistProducerJournal(w.surrenderJournalRoot(), journal); err != nil {
			return false, err
		}
	}
	remaining, err := listCaptureArtifactPaths(outDir)
	if err != nil || len(remaining) != 0 {
		return false, err
	}
	done = true
	return true, nil
}

func (w *Worker) recoverCaptureSetJournal(ctx context.Context, journal *captureProducerJournal, paths []captureArtifactPath) (bool, error) {
	if err := validateCaptureSetJournal(journal); err != nil {
		return false, err
	}
	// The local ACK is fsynced before its network call. Replay it before any
	// materialize/seal/finish request so a restart cannot bypass the server's
	// immutable stop-inventory boundary.
	if err := w.replayCaptureSetStopAck(ctx, journal); err != nil {
		return false, err
	}
	bound := make(map[string]struct{}, len(journal.Artifacts))
	nextSequence := journal.CaptureSet.FirstCaptureSequence
	if journal.LastAcceptedSequence >= nextSequence {
		nextSequence = journal.LastAcceptedSequence + 1
	}
	for _, artifact := range journal.Artifacts {
		if artifact.CaptureSequence >= nextSequence {
			nextSequence = artifact.CaptureSequence + 1
		}
		if artifact.Segment != nil {
			bound[artifact.Segment.Path] = struct{}{}
		} else if artifact.LocalPath != "" {
			bound[artifact.LocalPath] = struct{}{}
		}
	}
	for _, path := range paths {
		if _, exists := bound[path.Path]; exists {
			continue
		}
		probeCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		segment, probeErr := w.recoverContinuousSegmentExact(probeCtx, path, time.Duration(journal.ClipDurationSec)*time.Second)
		cancel()
		artifact, _, deriveErr := deriveCaptureSetArtifact(journal, nextSequence)
		if deriveErr != nil {
			return false, deriveErr
		}
		artifact.LocalPath = path.Path
		artifact.Device, artifact.Inode = path.Device, path.Inode
		if probeErr == nil {
			segment.CaptureSequence = nextSequence
			artifact.Segment = &segment
		}
		journal.Artifacts = append(journal.Artifacts, artifact)
		nextSequence++
	}
	for index := range journal.Artifacts {
		artifact := &journal.Artifacts[index]
		if artifact.Segment != nil || artifact.LocalPath == "" {
			continue
		}
		probeCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		segment, probeErr := w.recoverContinuousSegmentExact(probeCtx, captureArtifactPath{Path: artifact.LocalPath, Device: artifact.Device, Inode: artifact.Inode}, time.Duration(journal.ClipDurationSec)*time.Second)
		cancel()
		if probeErr == nil {
			segment.CaptureSequence = artifact.CaptureSequence
			artifact.Segment = &segment
		}
	}
	sort.Slice(journal.Artifacts, func(i, j int) bool {
		return journal.Artifacts[i].CaptureSequence < journal.Artifacts[j].CaptureSequence
	})
	if err := persistProducerJournal(w.surrenderJournalRoot(), journal); err != nil {
		return false, err
	}

	heartbeatCtx, cancelHeartbeat := context.WithTimeout(ctx, 10*time.Second)
	heartbeat, heartbeatErr := w.cfg.Client.HeartbeatRecordingJobState(heartbeatCtx, journal.JobID, journal.LeaseToken)
	cancelHeartbeat()
	currentLease := heartbeatErr == nil && !heartbeat.Cancel
	if !currentLease && len(journal.Artifacts) == 0 {
		reportID := uuid.NewSHA1(captureRecoveryReportNamespace, []byte("empty-set:"+journal.CaptureSet.SetID)).String()
		if err := w.cfg.Client.FinishEmptyCaptureSetRecovery(ctx, journal.JobID, journal.LeaseToken, journal.CaptureSet.SetID, reportID); err != nil {
			return false, err
		}
		journal.RecoveryGrantSeen = true
		if err := persistProducerJournal(w.surrenderJournalRoot(), journal); err != nil {
			return false, err
		}
		return true, nil
	}
	for index := range journal.Artifacts {
		artifact := &journal.Artifacts[index]
		if artifact.Done {
			continue
		}
		_, proof, err := deriveCaptureSetArtifact(journal, artifact.CaptureSequence)
		if err != nil {
			return false, err
		}
		materialization := recordingapi.CaptureArtifactMaterialization{
			ArtifactID: artifact.IntentID, CaptureSequence: artifact.CaptureSequence,
			RecoverySecretSHA256: artifact.RecoverySecretSHA256, Proof: proof,
		}
		if err = w.cfg.Client.MaterializeCaptureArtifact(ctx, journal.JobID, journal.LeaseToken, journal.CaptureSet.SetID, artifact.Ordinal, materialization); err != nil {
			return false, err
		}
		if currentLease {
			if artifact.Segment == nil {
				continue
			}
			segment := *artifact.Segment
			intent, sealErr := w.cfg.Client.SealCaptureSetArtifact(ctx, journal.JobID, journal.LeaseToken, journal.CaptureSet.SetID, artifact.Ordinal, artifact.IntentID, journal.ProducerID, artifact.CaptureSequence, captureUnixMicro(segment.StartAt), segment.SizeBytes, segment.SHA256)
			if sealErr != nil {
				return false, sealErr
			}
			if !intent.AlreadyIngested {
				if err = w.cfg.Client.UploadFileExact(ctx, intent.UploadURL, segment.Path, segment.MIMEType, artifact.Device, artifact.Inode, segment.SizeBytes); err != nil {
					return false, err
				}
				if _, err = w.cfg.Client.IngestClipWithResult(ctx, recoveryIngestRequest(journal, segment, artifact.IntentID, artifact.CaptureSequence)); err != nil {
					return false, err
				}
			}
			if err = removeRetainedArtifact(segment.Path, artifact.Device, artifact.Inode); err != nil {
				return false, err
			}
			artifact.Done, artifact.Segment = true, nil
			journal.HadAcceptedArtifact = true
			journal.LastAcceptedSequence = max(journal.LastAcceptedSequence, artifact.CaptureSequence)
			if err = persistProducerJournal(w.surrenderJournalRoot(), journal); err != nil {
				return false, err
			}
			continue
		}

		status, statusErr := w.cfg.Client.RecordingRecoveryStatus(ctx, artifact.IntentID, artifact.RecoverySecret)
		if statusErr != nil {
			return false, statusErr
		}
		if status.Authority != "capture_set_grant" || status.IntentID != artifact.IntentID || status.ProducerID != journal.ProducerID || status.JobID != journal.JobID || status.LeaseToken != journal.LeaseToken || len(status.Artifacts) != 1 || status.Artifacts[0].CaptureSequence != artifact.CaptureSequence {
			return false, fmt.Errorf("capture set recovery status identity mismatch")
		}
		if !journal.RecoveryGrantSeen {
			journal.RecoveryGrantSeen = true
			if err = persistProducerJournal(w.surrenderJournalRoot(), journal); err != nil {
				return false, err
			}
		}
		if terminal := status.Artifacts[0].Result; terminal != "" {
			if terminal == "unrecoverable_partial" {
				return false, fmt.Errorf("partial capture bytes retained for operator recovery")
			}
			if artifact.Segment != nil {
				if err = removeRetainedArtifact(artifact.Segment.Path, artifact.Device, artifact.Inode); err != nil {
					return false, err
				}
			}
			artifact.Done, artifact.Segment = true, nil
			if err = persistProducerJournal(w.surrenderJournalRoot(), journal); err != nil {
				return false, err
			}
			continue
		}
		reportID := uuid.NewSHA1(captureRecoveryReportNamespace, []byte(artifact.IntentID)).String()
		if artifact.Segment == nil && artifact.LocalPath == "" {
			report := recordingapi.CaptureRecoveryReport{ReportID: reportID, ReportType: "no_bytes", LocalObservedAt: time.Now().UTC()}
			if err = w.cfg.Client.ReportRecoveryArtifact(ctx, artifact.IntentID, artifact.RecoverySecret, report); err != nil {
				return false, err
			}
			artifact.Done = true
			if err = persistProducerJournal(w.surrenderJournalRoot(), journal); err != nil {
				return false, err
			}
			continue
		}
		if artifact.Segment == nil {
			size, partialSHA, hashErr := hashRetainedArtifact(artifact.LocalPath, artifact.Device, artifact.Inode)
			if hashErr != nil {
				return false, hashErr
			}
			report := recordingapi.CaptureRecoveryReport{ReportID: reportID, ReportType: "partial_bytes", SizeBytes: &size, SHA256: partialSHA, LocalObservedAt: time.Now().UTC()}
			if err = w.cfg.Client.ReportRecoveryArtifact(ctx, artifact.IntentID, artifact.RecoverySecret, report); err != nil {
				return false, err
			}
			return false, fmt.Errorf("partial capture bytes retained for operator recovery")
		}
		segment := *artifact.Segment
		probeCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		probed, probeErr := w.recoverContinuousSegmentExact(probeCtx, captureArtifactPath{Path: segment.Path, Device: artifact.Device, Inode: artifact.Inode}, time.Duration(journal.ClipDurationSec)*time.Second)
		cancel()
		if probeErr != nil || probed.SizeBytes != segment.SizeBytes || probed.SHA256 != segment.SHA256 {
			size := segment.SizeBytes
			report := recordingapi.CaptureRecoveryReport{ReportID: reportID, ReportType: "partial_bytes", SizeBytes: &size, SHA256: segment.SHA256, LocalObservedAt: time.Now().UTC()}
			if err = w.cfg.Client.ReportRecoveryArtifact(ctx, artifact.IntentID, artifact.RecoverySecret, report); err != nil {
				return false, errors.Join(probeErr, err)
			}
			return false, fmt.Errorf("partial capture bytes retained for operator recovery")
		}
		if _, err = w.cfg.Client.SealCaptureSetArtifactRecovery(ctx, journal.JobID, journal.CaptureSet.SetID, artifact.Ordinal, artifact.IntentID, artifact.RecoverySecret, journal.ProducerID, artifact.CaptureSequence, captureUnixMicro(segment.StartAt), segment.SizeBytes, segment.SHA256); err != nil {
			return false, err
		}
		size := segment.SizeBytes
		report := recordingapi.CaptureRecoveryReport{ReportID: reportID, ReportType: "sealed_bytes", SizeBytes: &size, SHA256: segment.SHA256, LocalObservedAt: time.Now().UTC()}
		if err = w.cfg.Client.ReportRecoveryArtifact(ctx, artifact.IntentID, artifact.RecoverySecret, report); err != nil {
			return false, err
		}
		if artifact.RecoveryRevision < 1 {
			artifact.RecoveryRevision = 1
		}
		sessionID := uuid.NewSHA1(captureRecoverySessionNamespace, []byte(fmt.Sprintf("%s:%d", artifact.IntentID, artifact.RecoveryRevision))).String()
		if err = w.cfg.Client.UploadRecoveryArtifact(ctx, artifact.IntentID, artifact.RecoverySecret, sessionID, segment.Path, artifact.Device, artifact.Inode); err != nil {
			artifact.RecoveryRevision++
			if persistErr := persistProducerJournal(w.surrenderJournalRoot(), journal); persistErr != nil {
				return false, errors.Join(err, persistErr)
			}
			return false, err
		}
		if _, err = w.cfg.Client.IngestClipRecovery(ctx, recoveryIngestRequest(journal, segment, artifact.IntentID, artifact.CaptureSequence), artifact.IntentID, artifact.RecoverySecret); err != nil {
			return false, err
		}
		if err = removeRetainedArtifact(segment.Path, artifact.Device, artifact.Inode); err != nil {
			return false, err
		}
		artifact.Done, artifact.Segment = true, nil
		journal.HadAcceptedArtifact = true
		journal.LastAcceptedSequence = max(journal.LastAcceptedSequence, artifact.CaptureSequence)
		if err = persistProducerJournal(w.surrenderJournalRoot(), journal); err != nil {
			return false, err
		}
	}
	remaining, err := listCaptureArtifactPaths(journal.OutputDir)
	if err != nil || len(remaining) != 0 {
		return false, fmt.Errorf("capture set recovery retains local bytes")
	}
	if currentLease {
		if err = w.cfg.Client.FinishCaptureSet(ctx, journal.JobID, journal.LeaseToken, journal.CaptureSet.SetID); err != nil {
			return false, err
		}
	}
	return true, nil
}

func (w *Worker) replayCaptureSetStopAck(ctx context.Context, journal *captureProducerJournal) error {
	if journal == nil || journal.CaptureSet == nil || journal.CaptureSet.StopAck == nil {
		return nil
	}
	if err := w.cfg.Client.AckCaptureSetStop(ctx, journal.JobID, journal.LeaseToken, journal.CaptureSet.SetID, *journal.CaptureSet.StopAck); err != nil {
		return fmt.Errorf("replay capture stop acknowledgment: %w", err)
	}
	return nil
}

func (w *Worker) recoverProducerUnderCurrentLease(ctx context.Context, journal *captureProducerJournal, paths []captureArtifactPath) error {
	boundPaths := make(map[string]struct{}, len(journal.Artifacts))
	for _, artifact := range journal.Artifacts {
		if artifact.Segment != nil {
			boundPaths[artifact.Segment.Path] = struct{}{}
		}
	}
	unboundPaths := make([]captureArtifactPath, 0, len(paths))
	for _, path := range paths {
		if _, bound := boundPaths[path.Path]; !bound {
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
		path := captureArtifactPath{}
		if artifact.Segment != nil {
			path = captureArtifactPath{Path: artifact.Segment.Path, Device: artifact.Device, Inode: artifact.Inode}
		}
		if path.Path == "" && pathIndex < len(unboundPaths) {
			path = unboundPaths[pathIndex]
			pathIndex++
		}
		if path.Path == "" {
			continue
		}
		probeCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		seg, err := w.recoverContinuousSegmentExact(probeCtx, path, time.Duration(journal.ClipDurationSec)*time.Second)
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
			if err = w.cfg.Client.UploadFileExact(ctx, intent.UploadURL, seg.Path, seg.MIMEType, artifact.Device, artifact.Inode, seg.SizeBytes); err != nil {
				return err
			}
			if _, err = w.cfg.Client.IngestClipWithResult(ctx, recoveryIngestRequest(journal, seg, artifact.IntentID, artifact.CaptureSequence)); err != nil {
				return err
			}
		}
		if err = removeRetainedArtifact(seg.Path, artifact.Device, artifact.Inode); err != nil {
			return err
		}
		artifact.Done = true
		artifact.Segment = nil
		accepted = true
		if err = persistProducerJournal(w.surrenderJournalRoot(), journal); err != nil {
			return err
		}
	}
	remaining, err := listCaptureArtifactPaths(journal.OutputDir)
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
	JobID                int64                    `json:"job_id"`
	LeaseToken           string                   `json:"lease_token"`
	ProducerID           string                   `json:"producer_id"`
	CaptureOrdinal       int64                    `json:"capture_ordinal"`
	OutputDir            string                   `json:"output_dir"`
	ClipDurationSec      int                      `json:"clip_duration_sec"`
	Artifacts            []captureArtifactJournal `json:"artifacts,omitempty"`
	CaptureSet           *captureSetJournal       `json:"capture_set,omitempty"`
	HadAcceptedArtifact  bool                     `json:"had_accepted_artifact,omitempty"`
	LastAcceptedSequence int64                    `json:"last_accepted_sequence,omitempty"`
	RecoveryGrantSeen    bool                     `json:"recovery_grant_seen,omitempty"`
	path                 string
}

type captureSetJournal struct {
	PlanID               string                       `json:"plan_id"`
	SetID                string                       `json:"set_id"`
	Seed                 string                       `json:"seed"`
	FirstCaptureSequence int64                        `json:"first_capture_sequence"`
	Plan                 *recordingapi.CaptureSetPlan `json:"plan,omitempty"`
	MerkleRootSHA256     string                       `json:"merkle_root_sha256,omitempty"`
	Committed            bool                         `json:"committed,omitempty"`
	StopAck              *recordingapi.CaptureStopAck `json:"stop_ack,omitempty"`
	tree                 *surrenderplan.Tree
}

type captureArtifactJournal struct {
	Ordinal              int              `json:"ordinal,omitempty"`
	IntentID             string           `json:"intent_id"`
	RecoverySecret       string           `json:"recovery_secret"`
	RecoverySecretSHA256 string           `json:"recovery_secret_sha256"`
	CaptureSequence      int64            `json:"capture_sequence"`
	Segment              *capture.Segment `json:"segment,omitempty"`
	LocalPath            string           `json:"local_path,omitempty"`
	Device               uint64           `json:"device,omitempty"`
	Inode                uint64           `json:"inode,omitempty"`
	Done                 bool             `json:"done,omitempty"`
	RecoveryRevision     int              `json:"recovery_revision,omitempty"`
}

func deriveCaptureSetArtifact(producer *captureProducerJournal, sequence int64) (captureArtifactJournal, []string, error) {
	if producer == nil || producer.CaptureSet == nil || !producer.CaptureSet.Committed || producer.CaptureSet.Plan == nil {
		return captureArtifactJournal{}, nil, fmt.Errorf("capture set is not committed")
	}
	setJournal := producer.CaptureSet
	ordinal64 := sequence - setJournal.FirstCaptureSequence + 1
	if ordinal64 < 1 || ordinal64 > int64(setJournal.Plan.ArtifactCount) {
		return captureArtifactJournal{}, nil, fmt.Errorf("capture sequence is outside committed set")
	}
	if setJournal.tree == nil {
		if err := validateCaptureSetJournal(producer); err != nil {
			return captureArtifactJournal{}, nil, err
		}
	}
	seedBytes, _ := hex.DecodeString(setJournal.Seed)
	var seed [32]byte
	copy(seed[:], seedBytes)
	set, err := captureSetIdentity(*setJournal.Plan)
	if err != nil {
		return captureArtifactJournal{}, nil, err
	}
	ordinal := int(ordinal64)
	derived, err := surrenderplan.DeriveArtifact(seed, set, ordinal)
	if err != nil {
		return captureArtifactJournal{}, nil, err
	}
	proof, err := setJournal.tree.Proof(ordinal)
	if err != nil {
		return captureArtifactJournal{}, nil, err
	}
	proofHex := make([]string, len(proof.Siblings))
	for index := range proof.Siblings {
		proofHex[index] = hex.EncodeToString(proof.Siblings[index][:])
	}
	return captureArtifactJournal{
		Ordinal: ordinal, IntentID: derived.ID.String(), RecoverySecret: hex.EncodeToString(derived.RecoverySecret[:]),
		RecoverySecretSHA256: hex.EncodeToString(derived.RecoverySecretHash[:]), CaptureSequence: sequence,
	}, proofHex, nil
}

func (w *Worker) materializeCaptureSetArtifact(ctx context.Context, job recordingapi.RecordingJob, producer *captureProducerJournal, sequence int64) (*captureArtifactJournal, error) {
	artifact, proofHex, err := deriveCaptureSetArtifact(producer, sequence)
	if err != nil {
		return nil, err
	}
	setJournal := producer.CaptureSet
	ordinal := artifact.Ordinal
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
		ArtifactID: artifact.IntentID, CaptureSequence: sequence, RecoverySecretSHA256: artifact.RecoverySecretSHA256, Proof: proofHex,
	}); err != nil {
		return nil, err
	}
	return &artifact, nil
}

type retainedCaptureNamespace struct {
	path   string
	device uint64
	inode  uint64
	fd     int
	names  []string
}

func (n *retainedCaptureNamespace) close() {
	if n != nil && n.fd >= 0 {
		_ = unix.Close(n.fd)
		n.fd = -1
	}
}

func (n *retainedCaptureNamespace) artifactIdentity(name string) (uint64, uint64, error) {
	if n == nil || n.fd < 0 || !captureSegmentLeafRE.MatchString(name) {
		return 0, 0, fmt.Errorf("invalid retained capture leaf")
	}
	fd, err := unix.Openat(n.fd, name, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return 0, 0, err
	}
	defer unix.Close(fd)
	var stat unix.Stat_t
	if err = unix.Fstat(fd, &stat); err != nil || stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Nlink != 1 || stat.Ino == 0 {
		return 0, 0, fmt.Errorf("retained capture leaf identity is unsafe")
	}
	return uint64(stat.Dev), uint64(stat.Ino), nil
}

// isolateCaptureNamespace keeps the original directory inode open while it is
// renamed and inventoried. All namespace operations are relative to held
// no-follow descriptors, so a root process cannot be tricked by path replacement
// between Rename/Lstat/ReadDir/Open.
func isolateCaptureNamespace(outputDir string) (*retainedCaptureNamespace, error) {
	absolute, err := filepath.Abs(outputDir)
	if err != nil {
		return nil, err
	}
	parentPath, leaf := filepath.Dir(absolute), filepath.Base(absolute)
	if leaf == "." || leaf == string(filepath.Separator) || strings.ContainsRune(leaf, filepath.Separator) {
		return nil, fmt.Errorf("invalid capture output namespace")
	}
	parentFD, err := unix.Open(parentPath, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	defer unix.Close(parentFD)
	dirFD, err := unix.Openat(parentFD, leaf, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	fail := func(cause error) (*retainedCaptureNamespace, error) {
		_ = unix.Close(dirFD)
		return nil, cause
	}
	var stat unix.Stat_t
	if err = unix.Fstat(dirFD, &stat); err != nil || stat.Ino == 0 {
		return fail(fmt.Errorf("capture output directory identity is unsafe"))
	}
	retainedLeaf := leaf + ".retained-" + uuid.NewString()
	if err = unix.Renameat(parentFD, leaf, parentFD, retainedLeaf); err != nil {
		return fail(fmt.Errorf("isolate capture output namespace: %w", err))
	}
	sentinelFD, err := unix.Openat(parentFD, leaf, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return fail(fmt.Errorf("install capture namespace sentinel: %w", err))
	}
	if err = unix.Fsync(sentinelFD); err == nil {
		err = unix.Close(sentinelFD)
	} else {
		_ = unix.Close(sentinelFD)
	}
	if err != nil {
		return fail(fmt.Errorf("sync capture namespace sentinel: %w", err))
	}
	if err = unix.Fsync(parentFD); err != nil {
		return fail(fmt.Errorf("sync capture namespace parent: %w", err))
	}
	dupFD, err := unix.Dup(dirFD)
	if err != nil {
		return fail(err)
	}
	dirFile := os.NewFile(uintptr(dupFD), retainedLeaf)
	entries, readErr := dirFile.ReadDir(-1)
	closeErr := dirFile.Close()
	if readErr != nil {
		return fail(readErr)
	}
	if closeErr != nil {
		return fail(closeErr)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !captureSegmentLeafRE.MatchString(entry.Name()) {
			return fail(fmt.Errorf("retained capture namespace contains an unauthorized leaf"))
		}
		if _, _, err = (&retainedCaptureNamespace{fd: dirFD}).artifactIdentity(entry.Name()); err != nil {
			return fail(err)
		}
		names = append(names, entry.Name())
	}
	sort.Strings(names)
	return &retainedCaptureNamespace{
		path: filepath.Join(parentPath, retainedLeaf), device: uint64(stat.Dev), inode: uint64(stat.Ino), fd: dirFD, names: names,
	}, nil
}

func (w *Worker) captureSetStopBarrier(job recordingapi.RecordingJob, producer *captureProducerJournal, pool *segmentDeliveryPool, captureSequence *int64) capture.ContinuousStopBarrier {
	return func(ctx context.Context, outputDir string) (string, error) {
		if producer == nil || producer.CaptureSet == nil || producer.CaptureSet.Plan == nil || pool == nil || captureSequence == nil {
			return "", fmt.Errorf("capture stop authority is incomplete")
		}
		if err := pool.waitIdle(ctx); err != nil {
			return "", fmt.Errorf("drain pre-stop deliveries: %w", err)
		}
		retained, err := isolateCaptureNamespace(outputDir)
		if err != nil {
			return "", err
		}
		defer retained.close()
		members := make([]recordingapi.CaptureStopAckMember, 0, len(retained.names))
		artifacts := make([]captureArtifactJournal, 0, len(retained.names))
		for index, name := range retained.names {
			path := filepath.Join(retained.path, name)
			device, inode, identityErr := retained.artifactIdentity(name)
			if identityErr != nil {
				return "", identityErr
			}
			sequence := *captureSequence + int64(index+1)
			artifact, proof, deriveErr := deriveCaptureSetArtifact(producer, sequence)
			if deriveErr != nil {
				return "", deriveErr
			}
			artifact.LocalPath = path
			artifact.Device, artifact.Inode = device, inode
			artifacts = append(artifacts, artifact)
			members = append(members, recordingapi.CaptureStopAckMember{
				Ordinal: artifact.Ordinal, ArtifactID: artifact.IntentID, CaptureSequence: artifact.CaptureSequence,
				RecoverySecretSHA256: artifact.RecoverySecretSHA256, Proof: proof,
				Device: device, Inode: inode, RelativeName: name,
			})
		}
		ack := recordingapi.CaptureStopAck{
			AckID: uuid.NewString(), RetainedDirectoryDevice: retained.device, RetainedDirectoryInode: retained.inode, Members: members,
		}
		ack.InventorySHA256, err = recordingapi.CaptureStopInventorySHA(ack)
		if err != nil {
			return "", err
		}
		state := w.surrenderState(job.JobID)
		state.mu.Lock()
		producer.OutputDir = retained.path
		producer.CaptureSet.StopAck = &ack
		for _, artifact := range artifacts {
			found := false
			for _, existing := range producer.Artifacts {
				if existing.CaptureSequence == artifact.CaptureSequence {
					if existing.IntentID != artifact.IntentID || existing.RecoverySecretSHA256 != artifact.RecoverySecretSHA256 {
						state.mu.Unlock()
						return "", fmt.Errorf("stop inventory artifact differs from journal")
					}
					found = true
					break
				}
			}
			if !found {
				producer.Artifacts = append(producer.Artifacts, artifact)
			}
		}
		err = persistProducerJournal(w.surrenderJournalRoot(), producer)
		state.mu.Unlock()
		if err != nil {
			return "", fmt.Errorf("persist stopped capture inventory: %w", err)
		}
		if err = w.cfg.Client.AckCaptureSetStop(ctx, job.JobID, job.LeaseToken, producer.CaptureSet.SetID, ack); err != nil {
			return "", err
		}
		return retained.path, nil
	}
}

func (w *Worker) finishStoppedCaptureSetBeforeExec(ctx context.Context, job recordingapi.RecordingJob, producer *captureProducerJournal, outputDir string) error {
	if producer == nil || producer.CaptureSet == nil || producer.CaptureSet.Plan == nil {
		return fmt.Errorf("pre-exec capture stop authority is incomplete")
	}
	retained, err := isolateCaptureNamespace(outputDir)
	if err != nil {
		return err
	}
	defer retained.close()
	if len(retained.names) != 0 {
		return fmt.Errorf("pre-exec capture namespace unexpectedly contains bytes")
	}
	ack := recordingapi.CaptureStopAck{
		AckID: uuid.NewString(), RetainedDirectoryDevice: retained.device,
		RetainedDirectoryInode: retained.inode, Members: []recordingapi.CaptureStopAckMember{},
	}
	ack.InventorySHA256, err = recordingapi.CaptureStopInventorySHA(ack)
	if err != nil {
		return err
	}
	state := w.surrenderState(job.JobID)
	state.mu.Lock()
	producer.OutputDir = retained.path
	producer.CaptureSet.StopAck = &ack
	err = persistProducerJournal(w.surrenderJournalRoot(), producer)
	state.mu.Unlock()
	if err != nil {
		return err
	}
	if err = w.cfg.Client.AckCaptureSetStop(ctx, job.JobID, job.LeaseToken, producer.CaptureSet.SetID, ack); err != nil {
		return err
	}
	if err = w.cfg.Client.FinishCaptureSet(ctx, job.JobID, job.LeaseToken, producer.CaptureSet.SetID); err != nil {
		return err
	}
	state.mu.Lock()
	if state.producer != nil && state.producer.ProducerID == producer.ProducerID {
		state.producer = nil
	}
	state.mu.Unlock()
	if producer.path != "" {
		_ = unlinkPrivateLeaf(w.surrenderJournalRoot(), producer.path)
	}
	_ = removeEmptyRetainedDirectory(retained.path)
	_ = removeCaptureNamespaceSentinel(outputDir)
	return nil
}

// preflightCommittedCaptureSet closes the commitment-response/source-mutation
// race immediately before process launch. A stop observed here seals the exact
// empty namespace and returns false; callers must not invoke FFmpeg.
func (w *Worker) preflightCommittedCaptureSet(ctx context.Context, job recordingapi.RecordingJob, producer *captureProducerJournal, outputDir string) (bool, error) {
	heartbeatCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	heartbeat, err := w.cfg.Client.HeartbeatRecordingJobState(heartbeatCtx, job.JobID, job.LeaseToken)
	cancel()
	if err != nil || heartbeat.Cancel {
		return false, errors.Join(err, fmt.Errorf("capture set pre-exec authority is unavailable"))
	}
	if !heartbeat.StopRequired {
		return true, nil
	}
	if err = w.finishStoppedCaptureSetBeforeExec(ctx, job, producer, outputDir); err != nil {
		return false, err
	}
	return false, nil
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

func (w *Worker) recordProducerArtifact(jobID int64, producer *captureProducerJournal, seg capture.Segment, intentID string) (uint64, uint64, error) {
	if producer == nil {
		return 0, 0, nil
	}
	state := w.surrenderState(jobID)
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.producer == nil || state.producer.ProducerID != producer.ProducerID {
		return 0, 0, fmt.Errorf("capture producer journal is not current")
	}
	file, stat, statErr := openRetainedArtifact(seg.Path, 0, 0)
	if statErr != nil || stat.Ino == 0 {
		return 0, 0, fmt.Errorf("capture artifact identity is unsafe")
	}
	if err := file.Close(); err != nil {
		return 0, 0, err
	}
	for _, artifact := range producer.Artifacts {
		if artifact.CaptureSequence == seg.CaptureSequence {
			if artifact.IntentID != intentID {
				return 0, 0, fmt.Errorf("capture artifact intent differs from pre-byte reservation")
			}
			if artifact.Segment != nil && (artifact.Segment.Path != seg.Path || artifact.Segment.SHA256 != seg.SHA256 || artifact.Segment.SizeBytes != seg.SizeBytes) {
				return 0, 0, fmt.Errorf("capture artifact journal replay differs")
			}
			if (artifact.Device != 0 && artifact.Device != uint64(stat.Dev)) || (artifact.Inode != 0 && artifact.Inode != uint64(stat.Ino)) {
				return 0, 0, fmt.Errorf("capture artifact inode differs from acknowledged inventory")
			}
			for index := range producer.Artifacts {
				if producer.Artifacts[index].CaptureSequence == seg.CaptureSequence {
					producer.Artifacts[index].Segment = &seg
					producer.Artifacts[index].LocalPath = seg.Path
					producer.Artifacts[index].Device = uint64(stat.Dev)
					producer.Artifacts[index].Inode = uint64(stat.Ino)
					err := persistProducerJournal(w.surrenderJournalRoot(), producer)
					return uint64(stat.Dev), uint64(stat.Ino), err
				}
			}
		}
	}
	return 0, 0, fmt.Errorf("capture artifact was not reserved before bytes")
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
			if producer.Artifacts[index].CaptureSequence > producer.LastAcceptedSequence {
				producer.LastAcceptedSequence = producer.Artifacts[index].CaptureSequence
			}
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
	if err != nil || abs == temp {
		return false
	}
	rel, err := filepath.Rel(temp, abs)
	if err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return false
	}
	return true
}

func ensurePrivateSurrenderJournalRoot(root string) error {
	if err := os.MkdirAll(root, 0o700); err != nil {
		return err
	}
	rootFD, err := openPrivateDirectory(root)
	if err != nil {
		return fmt.Errorf("surrender transport journal root is not private")
	}
	_ = unix.Close(rootFD)
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
	rootFD, err := openPrivateDirectory(root)
	if err != nil {
		return err
	}
	defer unix.Close(rootFD)
	if err = writePrivateRegularAt(rootFD, filepath.Base(path), raw); err != nil {
		return err
	}
	journal.path = path
	return nil
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
			paths, globErr := listCaptureArtifactPaths(producer.OutputDir)
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
	paths, err := listCaptureArtifactPaths(producer.OutputDir)
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
				_ = unlinkPrivateLeaf(w.surrenderJournalRoot(), producer.path)
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
