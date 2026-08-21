package joinedrecording

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

const mediaCommandOutputLimit = 64 << 10

type LocalSource struct {
	ClipID    int64
	Path      string
	SizeBytes int64
	SHA256    string
}

type TrackFingerprint struct {
	MediaType         string `json:"media_type"`
	PacketCount       int64  `json:"packet_count"`
	PacketChainSHA256 string `json:"packet_chain_sha256"`
	DecodedFrames     int64  `json:"decoded_frames"`
	DecodedSamples    int64  `json:"decoded_samples,omitempty"`
	FirstTimestamp    int64  `json:"first_timestamp"`
	LastTimestamp     int64  `json:"last_timestamp"`
	TimestampStatus   string `json:"timestamp_status"`
}

type MediaFingerprint struct {
	DurationSeconds float64                      `json:"duration_seconds"`
	Tracks          map[string]*TrackFingerprint `json:"tracks"`
}

type Verification struct {
	Status                   string           `json:"status"`
	PacketPayloadOrderStatus string           `json:"packet_payload_order_status"`
	DecodedFrameTotalsStatus string           `json:"decoded_frame_totals_status"`
	DecodedAudioTotalsStatus string           `json:"decoded_audio_totals_status"`
	OutputTimestampStatus    string           `json:"output_timestamp_status"`
	StrictDecodeStatus       string           `json:"strict_decode_status"`
	SourceFingerprint        MediaFingerprint `json:"source_fingerprint"`
	OutputFingerprint        MediaFingerprint `json:"output_fingerprint"`
}

type BuiltOutput struct {
	Path         string
	SizeBytes    int64
	SHA256       string
	SourceCount  int
	Verification Verification
}

// BuildLargestPassingPrefix is a pre-seal operation: it peels exactly one
// trailing clip after each failed candidate so the planner can seal immutable
// prefix/remainder tasks. It must never shorten an already claimed task.
func BuildLargestPassingPrefix(ctx context.Context, sources []LocalSource, scratchDir string) (BuiltOutput, error) {
	if len(sources) == 0 || strings.TrimSpace(scratchDir) == "" {
		return BuiltOutput{}, fmt.Errorf("bounded sources and scratch directory are required")
	}
	if err := os.MkdirAll(scratchDir, 0700); err != nil {
		return BuiltOutput{}, fmt.Errorf("create scratch: %w", err)
	}
	for _, source := range sources {
		if err := verifyLocalIdentity(source); err != nil {
			return BuiltOutput{}, err
		}
	}
	var lastErr error
	for n := len(sources); n >= 1; n-- {
		built, err := buildAndVerify(ctx, sources[:n], scratchDir)
		if err == nil {
			return built, nil
		}
		lastErr = err
	}
	return BuiltOutput{}, fmt.Errorf("no passing stream-copy prefix: %w", lastErr)
}

// BuildSealedOutput verifies the complete frozen claim or fails. Runtime media
// failure invalidates that campaign generation; there is no runtime tail peel.
func BuildSealedOutput(ctx context.Context, sources []LocalSource, scratchDir string) (BuiltOutput, error) {
	if len(sources) == 0 || strings.TrimSpace(scratchDir) == "" {
		return BuiltOutput{}, fmt.Errorf("bounded sources and scratch directory are required")
	}
	if err := os.MkdirAll(scratchDir, 0700); err != nil {
		return BuiltOutput{}, fmt.Errorf("create scratch: %w", err)
	}
	for _, source := range sources {
		if err := verifyLocalIdentity(source); err != nil {
			return BuiltOutput{}, err
		}
	}
	return buildAndVerify(ctx, sources, scratchDir)
}

func buildAndVerify(ctx context.Context, sources []LocalSource, scratchDir string) (BuiltOutput, error) {
	manifest, err := os.CreateTemp(scratchDir, "concat-*.txt")
	if err != nil {
		return BuiltOutput{}, err
	}
	manifestPath := manifest.Name()
	defer os.Remove(manifestPath)
	for _, source := range sources {
		if strings.ContainsAny(source.Path, "\r\n") {
			_ = manifest.Close()
			return BuiltOutput{}, fmt.Errorf("source path contains newline")
		}
		escaped := strings.ReplaceAll(source.Path, "'", "'\\''")
		if _, err := fmt.Fprintf(manifest, "file '%s'\n", escaped); err != nil {
			_ = manifest.Close()
			return BuiltOutput{}, err
		}
	}
	if err := manifest.Close(); err != nil {
		return BuiltOutput{}, err
	}
	handle, err := os.CreateTemp(scratchDir, "joined-*.mp4")
	if err != nil {
		return BuiltOutput{}, err
	}
	outputPath := handle.Name()
	_ = handle.Close()
	_ = os.Remove(outputPath)
	keep := false
	defer func() {
		if !keep {
			_ = os.Remove(outputPath)
		}
	}()
	cmd := exec.CommandContext(ctx, ffmpegBinary(), "-nostdin", "-v", "error", "-f", "concat", "-safe", "0", "-i", manifestPath, "-copyts", "-map", "0:v:0", "-map", "0:a?", "-c", "copy", "-movflags", "+faststart", outputPath)
	if err := runBounded(cmd); err != nil {
		return BuiltOutput{}, fmt.Errorf("stream-copy join: %w", err)
	}
	verification, err := VerifyJoinedMedia(ctx, sources, outputPath)
	if err != nil {
		return BuiltOutput{}, err
	}
	size, sha, err := localIdentity(outputPath)
	if err != nil {
		return BuiltOutput{}, err
	}
	keep = true
	return BuiltOutput{Path: outputPath, SizeBytes: size, SHA256: sha, SourceCount: len(sources), Verification: verification}, nil
}

func VerifyJoinedMedia(ctx context.Context, sources []LocalSource, outputPath string) (Verification, error) {
	expectedAccumulator := newMediaAccumulator()
	for _, source := range sources {
		if err := probeMediaInto(ctx, source.Path, expectedAccumulator); err != nil {
			return Verification{}, fmt.Errorf("probe source %d: %w", source.ClipID, err)
		}
	}
	expected := expectedAccumulator.fingerprint("source_clips_independent")
	actual, err := probeMedia(ctx, outputPath)
	if err != nil {
		return Verification{}, fmt.Errorf("probe joined output: %w", err)
	}
	verification := Verification{Status: "failed", SourceFingerprint: expected, OutputFingerprint: actual}
	if err := compareFingerprints(expected, actual); err != nil {
		return verification, err
	}
	verification.PacketPayloadOrderStatus = "passed"
	verification.DecodedFrameTotalsStatus = "passed"
	verification.DecodedAudioTotalsStatus = "passed"
	verification.OutputTimestampStatus = "passed"
	decode := exec.CommandContext(ctx, ffmpegBinary(), "-nostdin", "-v", "error", "-xerror", "-err_detect", "explode", "-i", outputPath, "-map", "0:v:0", "-map", "0:a?", "-f", "null", "-")
	if err := runBounded(decode); err != nil {
		return verification, fmt.Errorf("strict joined decode: %w", err)
	}
	verification.StrictDecodeStatus = "passed"
	verification.Status = "passed"
	return verification, nil
}

func compareFingerprints(expected, actual MediaFingerprint) error {
	if len(expected.Tracks) != len(actual.Tracks) || actual.DurationSeconds <= 0 || actual.DurationSeconds > 3600.5 || math.Abs(actual.DurationSeconds-expected.DurationSeconds) > 2 {
		return fmt.Errorf("joined duration or stream cardinality mismatch")
	}
	for mediaType, want := range expected.Tracks {
		got := actual.Tracks[mediaType]
		if got == nil || got.TimestampStatus != "monotonic" || want.PacketCount != got.PacketCount || want.PacketChainSHA256 != got.PacketChainSHA256 || want.DecodedFrames != got.DecodedFrames || (mediaType == "audio" && want.DecodedSamples != got.DecodedSamples) {
			return fmt.Errorf("joined %s packet/frame/timestamp mismatch", mediaType)
		}
	}
	return nil
}

type trackAccumulator struct {
	fingerprint TrackFingerprint
	packetHash  hash.Hash
}

type mediaAccumulator struct {
	duration float64
	tracks   map[string]*trackAccumulator
}

func newMediaAccumulator() *mediaAccumulator {
	return &mediaAccumulator{tracks: map[string]*trackAccumulator{}}
}

func (a *mediaAccumulator) fingerprint(timestampStatus string) MediaFingerprint {
	out := MediaFingerprint{DurationSeconds: a.duration, Tracks: map[string]*TrackFingerprint{}}
	for mediaType, track := range a.tracks {
		copy := track.fingerprint
		copy.PacketChainSHA256 = hex.EncodeToString(track.packetHash.Sum(nil))
		copy.TimestampStatus = timestampStatus
		out.Tracks[mediaType] = &copy
	}
	return out
}

func probeMedia(ctx context.Context, mediaPath string) (MediaFingerprint, error) {
	accumulator := newMediaAccumulator()
	if err := probeMediaInto(ctx, mediaPath, accumulator); err != nil {
		return MediaFingerprint{}, err
	}
	return accumulator.fingerprint("monotonic"), nil
}

func probeMediaInto(ctx context.Context, mediaPath string, accumulator *mediaAccumulator) error {
	duration, indexType, err := probeMediaMetadata(ctx, mediaPath)
	if err != nil {
		return err
	}
	for _, mediaType := range indexType {
		if accumulator.tracks[mediaType] == nil {
			accumulator.tracks[mediaType] = &trackAccumulator{fingerprint: TrackFingerprint{MediaType: mediaType}, packetHash: sha256.New()}
		}
	}
	accumulator.duration += duration
	cmd := exec.CommandContext(ctx, ffprobeBinary(), "-v", "error", "-err_detect", "explode", "-show_packets", "-show_frames", "-show_data_hash", "sha256", "-show_entries", "packet=stream_index,data_hash:frame=stream_index,media_type,best_effort_timestamp,nb_samples", "-of", "compact=p=1:nk=0", mediaPath)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	var stderr limitedOutput
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		return err
	}
	consumeErr := consumeCompactEvidence(stdout, indexType, accumulator)
	waitErr := cmd.Wait()
	if consumeErr != nil {
		return consumeErr
	}
	if waitErr != nil {
		return fmt.Errorf("ffprobe evidence: %w (%s)", waitErr, stderr.String())
	}
	return nil
}

func probeMediaMetadata(ctx context.Context, mediaPath string) (float64, map[int]string, error) {
	cmd := exec.CommandContext(ctx, ffprobeBinary(), "-v", "error", "-show_streams", "-show_format", "-show_entries", "format=duration:stream=index,codec_type", "-of", "json", mediaPath)
	var stderr limitedOutput
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return 0, nil, fmt.Errorf("ffprobe metadata: %w (%s)", err, stderr.String())
	}
	var payload struct {
		Format struct {
			Duration string `json:"duration"`
		} `json:"format"`
		Streams []struct {
			Index     int    `json:"index"`
			CodecType string `json:"codec_type"`
		} `json:"streams"`
	}
	if err := json.Unmarshal(out, &payload); err != nil {
		return 0, nil, err
	}
	duration, err := strconv.ParseFloat(payload.Format.Duration, 64)
	if err != nil || duration <= 0 {
		return 0, nil, fmt.Errorf("invalid media duration")
	}
	indexType := map[int]string{}
	seenType := map[string]bool{}
	for _, stream := range payload.Streams {
		mediaType := strings.TrimSpace(stream.CodecType)
		if mediaType != "video" && mediaType != "audio" {
			continue
		}
		if seenType[mediaType] {
			return 0, nil, fmt.Errorf("multiple %s streams are unsupported", mediaType)
		}
		seenType[mediaType] = true
		indexType[stream.Index] = mediaType
	}
	if !seenType["video"] {
		return 0, nil, fmt.Errorf("video stream missing")
	}
	return duration, indexType, nil
}

func consumeCompactEvidence(reader io.Reader, indexType map[int]string, accumulator *mediaAccumulator) error {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 4096), 1<<20)
	localSeen := map[string]bool{}
	localLast := map[string]int64{}
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Split(line, "|")
		if len(fields) < 2 {
			continue
		}
		values := map[string]string{}
		for _, field := range fields[1:] {
			key, value, ok := strings.Cut(field, "=")
			if ok {
				values[key] = value
			}
		}
		streamIndex, err := strconv.Atoi(values["stream_index"])
		if err != nil || indexType[streamIndex] == "" {
			return fmt.Errorf("evidence has unknown stream")
		}
		mediaType := indexType[streamIndex]
		track := accumulator.tracks[mediaType]
		switch fields[0] {
		case "packet":
			packetSHA := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(values["data_hash"])), "sha256:")
			if !lowerHex64(packetSHA) {
				return fmt.Errorf("missing packet payload hash")
			}
			_, _ = io.WriteString(track.packetHash, packetSHA+"\n")
			track.fingerprint.PacketCount++
		case "frame":
			if values["media_type"] != mediaType {
				return fmt.Errorf("decoded frame stream type differs")
			}
			pts, err := strconv.ParseInt(values["best_effort_timestamp"], 10, 64)
			if err != nil {
				return fmt.Errorf("missing decoded %s timestamp", mediaType)
			}
			if localSeen[mediaType] && pts <= localLast[mediaType] {
				return fmt.Errorf("nonmonotonic decoded %s timestamp", mediaType)
			}
			if !localSeen[mediaType] && track.fingerprint.DecodedFrames == 0 {
				track.fingerprint.FirstTimestamp = pts
			}
			localSeen[mediaType] = true
			localLast[mediaType] = pts
			track.fingerprint.LastTimestamp = pts
			track.fingerprint.DecodedFrames++
			if mediaType == "audio" {
				samples, err := strconv.ParseInt(values["nb_samples"], 10, 64)
				if err != nil || samples <= 0 {
					return fmt.Errorf("missing decoded audio samples")
				}
				track.fingerprint.DecodedSamples += samples
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	for _, mediaType := range indexType {
		track := accumulator.tracks[mediaType]
		if !localSeen[mediaType] || track.fingerprint.PacketCount == 0 {
			return fmt.Errorf("incomplete %s packet/frame evidence", mediaType)
		}
	}
	return nil
}

func verifyLocalIdentity(source LocalSource) error {
	if source.ClipID <= 0 || source.SizeBytes <= 0 || !lowerHex64(source.SHA256) {
		return fmt.Errorf("invalid local source identity")
	}
	size, sha, err := localIdentity(source.Path)
	if err != nil {
		return err
	}
	if size != source.SizeBytes || sha != source.SHA256 {
		return fmt.Errorf("local source %d identity mismatch", source.ClipID)
	}
	return nil
}

func localIdentity(path string) (int64, string, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, "", err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 {
		return 0, "", fmt.Errorf("invalid local media file")
	}
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return 0, "", err
	}
	return info.Size(), hex.EncodeToString(h.Sum(nil)), nil
}

type limitedOutput struct {
	buf bytes.Buffer
}

func (w *limitedOutput) Write(p []byte) (int, error) {
	written := len(p)
	remaining := mediaCommandOutputLimit - w.buf.Len()
	if remaining > 0 {
		if len(p) > remaining {
			p = p[:remaining]
		}
		_, _ = w.buf.Write(p)
	}
	return written, nil
}

func (w *limitedOutput) String() string { return strings.TrimSpace(w.buf.String()) }

func runBounded(cmd *exec.Cmd) error {
	var output limitedOutput
	cmd.Stdout, cmd.Stderr = &output, &output
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%w (%s)", err, output.String())
	}
	return nil
}

func ffmpegBinary() string {
	if value := strings.TrimSpace(os.Getenv("FFMPEG_BIN")); value != "" {
		return value
	}
	return "ffmpeg"
}

func ffprobeBinary() string {
	if value := strings.TrimSpace(os.Getenv("FFPROBE_BIN")); value != "" {
		return value
	}
	return "ffprobe"
}

func SafeScratchOutput(path, scratchDir string) bool {
	absPath, errPath := filepath.Abs(path)
	absScratch, errScratch := filepath.Abs(scratchDir)
	if errPath != nil || errScratch != nil {
		return false
	}
	rel, err := filepath.Rel(absScratch, absPath)
	return err == nil && rel != "." && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}
