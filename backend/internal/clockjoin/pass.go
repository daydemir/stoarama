package clockjoin

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"time"

	"github.com/daydemir/stoarama/backend/internal/collation"
)

const (
	PassVersion = "clock-join-v1"
	// ProductionVersion is the page's existing label for these assemblies
	// (coverage-growth.js modelLabel: "Clock-prestitched").
	ProductionVersion = "clock-prestitched"
	Method            = "CLOCK_20"
	// MaxGapSeconds: the clock runs on across the seam, 0-1 s apart.
	MaxGapSeconds = 1
	// MaxRelativeFPSError: |N - fps x span| / (fps x span) per part.
	MaxRelativeFPSError = 0.20
	// MaxDBGapSeconds bounds a db_gap split the clock may still join.
	MaxDBGapSeconds = 2.0
	// maxAssemblyBytes keeps an assembly a single-part R2 upload, so its ETag
	// is its MD5 and the delivery_keep_holds row can state it.
	maxAssemblyBytes = 5 << 30
)

// Collation-v2 split reasons the clock may overrule: visual or low-confidence
// evidence, where the metadata gate passed. Every other split stays.
var eligibleReasons = map[string]bool{
	collation.ReasonFramePrefix + collation.MatchLowMotion: true,
	collation.ReasonFramePrefix + collation.MatchJump:      true,
	collation.ReasonFramePrefix + collation.MatchTooShort:  true,
	collation.ReasonFrameMatchMissing:                      true,
	collation.ReasonDBGap:                                  true,
}

// Reason is why a seam was joined or kept split.
type Reason string

const (
	Joined                 Reason = "clock_continuous"
	PartNotDelivered       Reason = "part_not_delivered"
	AlreadyAssembled       Reason = "already_assembled"
	NoDirectSeam           Reason = "no_direct_seam"
	SeamNotEligible        Reason = "seam_not_eligible"
	KnownBadSource         Reason = "known_bad_source"
	QuarantinedClipBetween Reason = "quarantined_clip_between"
	CodecParamsDiffer      Reason = "codec_params_differ"
	ClockUnreadable        Reason = "clock_unreadable"
	ClockInconsistent      Reason = "clock_inconsistent"
	ClockGap               Reason = "clock_gap"
	ClockOverlap           Reason = "clock_overlap"
	ClockSpanTooShort      Reason = "clock_span_too_short"
	FrameCountMismatch     Reason = "frame_count_mismatch"
)

// HourStatus is one hour's outcome.
type HourStatus string

const (
	HourJoined      HourStatus = "joined"
	HourNoJoin      HourStatus = "no_join"
	HourNoProfile   HourStatus = "no_clock_profile"
	HourVetoed      HourStatus = "recording_vetoed"
	HourSinglePart  HourStatus = "single_part"
	HourNotCollated HourStatus = "not_collated"
)

// AudioStatus says what an assembly carries.
type AudioStatus string

const (
	AudioCopied   AudioStatus = "original_audio_stream_copied"
	AudioNoSource AudioStatus = "original_sources_no_audio"
)

// Delivered is one file of the parent delivery generation.
type Delivered struct {
	ID          int64
	Path        string
	SHA256      string
	SizeBytes   int64
	RecordingID int64
}

// Source reads hour manifests and makes part bytes available locally.
type Source interface {
	Read(ctx context.Context, key string) ([]byte, error)
	// Fetch returns a local path holding key's bytes. The caller verifies them.
	Fetch(ctx context.Context, key, scratch string) (string, error)
}

// DirSource reads a local tree laid out by R2 key (collation-v2 run
// --publish-dir, or a read-only mirror).
type DirSource struct{ Root string }

func (s DirSource) Read(_ context.Context, key string) ([]byte, error) {
	return os.ReadFile(filepath.Join(s.Root, filepath.FromSlash(key)))
}

func (s DirSource) Fetch(_ context.Context, key, _ string) (string, error) {
	p := filepath.Join(s.Root, filepath.FromSlash(key))
	_, err := os.Stat(p)
	return p, err
}

type Env struct {
	Tools  collation.Tools
	Reader Reader
	Source Source
	// Out is the output tree, laid out by R2 key. Scratch work happens in
	// Out/.scratch so every result is placed by a same-filesystem link.
	Out       string
	Batch     string // clock-prestitch-YYYYMMDD
	Profiles  map[int64]Profile
	Delivered map[string]Delivered // parent generation files by object key
	// Assembled: output ids an assembly of the parent generation already replaces.
	Assembled map[int64]bool
	// Paths: every delivery path of the parent generation.
	Paths          map[string]bool
	VetoClips      map[int64]bool
	VetoRecordings map[int64]bool
	MediaTool      string
	ClockTool      string
}

var batchName = regexp.MustCompile(`^clock-prestitch-[0-9]{8}$`)

func ValidBatch(b string) bool { return batchName.MatchString(b) }

// PartCheck is one part's clock evidence and its frame-count check.
type PartCheck struct {
	OutputID         int64    `json:"output_id"`
	Part             int      `json:"part"`
	Head             EdgeRead `json:"head"`
	Tail             EdgeRead `json:"tail"`
	SpanSeconds      int      `json:"clock_span_seconds"`
	FrameSeconds     float64  `json:"frame_seconds"`
	VideoPackets     int      `json:"video_packets"`
	ExpectedFrames   float64  `json:"expected_frames"`
	RelativeFPSError float64  `json:"relative_fps_error"`
	Reason           Reason   `json:"reason,omitempty"` // "" when the part passes
}

// CheckPart applies the frame-count rule: N (collation-v2's verified video
// packets) against the nominal fps (packet cadence, never derived from N)
// times the clock span, within 20%.
func CheckPart(id int64, part, packets int, frameSeconds float64, head, tail EdgeRead) PartCheck {
	c := PartCheck{OutputID: id, Part: part, Head: head, Tail: tail, FrameSeconds: frameSeconds, VideoPackets: packets}
	switch {
	case head.Status == ReadUnreadable || tail.Status == ReadUnreadable:
		c.Reason = ClockUnreadable
		return c
	case head.Status != ReadOK || tail.Status != ReadOK:
		c.Reason = ClockInconsistent
		return c
	}
	c.SpanSeconds = ClockDiff(head.Clock, tail.Clock)
	if c.SpanSeconds <= 0 || frameSeconds <= 0 {
		c.Reason = ClockSpanTooShort
		return c
	}
	c.ExpectedFrames = float64(c.SpanSeconds) / frameSeconds
	c.RelativeFPSError = math.Abs(float64(packets)-c.ExpectedFrames) / c.ExpectedFrames
	if c.RelativeFPSError > MaxRelativeFPSError {
		c.Reason = FrameCountMismatch
	}
	return c
}

// SeamVerdict is the clock pass's decision on one collation-v2 split.
type SeamVerdict struct {
	PrevOutputID    int64      `json:"prev_output_id"`
	NextOutputID    int64      `json:"next_output_id"`
	PrevPart        int        `json:"prev_part"`
	NextPart        int        `json:"next_part"`
	CollationReason string     `json:"collation_reason,omitempty"`
	Join            bool       `json:"join"`
	Reason          Reason     `json:"reason"`
	GapSeconds      *int       `json:"clock_gap_seconds,omitempty"`
	Prev            *PartCheck `json:"prev,omitempty"`
	Next            *PartCheck `json:"next,omitempty"`
}

// DecideClock applies the clock rule to a seam whose cheaper vetoes passed.
func DecideClock(v SeamVerdict, prev, next PartCheck) SeamVerdict {
	v.Prev, v.Next = &prev, &next
	// The two reads at the seam come first: they decide continuity.
	for _, r := range []EdgeRead{prev.Tail, next.Head} {
		if r.Status == ReadUnreadable {
			v.Reason = ClockUnreadable
			return v
		}
		if r.Status != ReadOK {
			v.Reason = ClockInconsistent
			return v
		}
	}
	gap := ClockDiff(prev.Tail.Clock, next.Head.Clock)
	v.GapSeconds = &gap
	switch {
	case gap < 0:
		v.Reason = ClockOverlap
	case gap > MaxGapSeconds:
		v.Reason = ClockGap
	case prev.Reason != "":
		v.Reason = prev.Reason
	case next.Reason != "":
		v.Reason = next.Reason
	default:
		v.Join, v.Reason = true, Joined
	}
	return v
}

// The page counts an annotation only with exactly these values.
const (
	annotationScope  = "boundary"
	annotationStatus = "verified"
)

// Annotation is the page's CLOCK_20 boundary evidence
// (coverage-growth.js clockBoundaryLabel).
type Annotation struct {
	Scope            string   `json:"scope"`
	Method           string   `json:"method"`
	Status           string   `json:"status"`
	ArtifactSHA256   string   `json:"artifact_sha256"`
	ProofSHA256      string   `json:"proof_sha256"`
	SourceInputIDs   [2]int64 `json:"source_input_ids"`
	MaxRelativeError float64  `json:"maximum_relative_fps_error"`
}

// Assembly is one joined run, shaped as the generation's growth.json repair
// plus what registration needs (keys, ETags, proof).
type Assembly struct {
	SHA256                     string       `json:"sha256"`
	SizeBytes                  int64        `json:"size_bytes"`
	ETag                       string       `json:"etag"`
	Key                        string       `json:"key"`
	ProofKey                   string       `json:"proof_key"`
	ProofSHA256                string       `json:"proof_sha256"`
	ProofSizeBytes             int64        `json:"proof_size_bytes"`
	ProofETag                  string       `json:"proof_etag"`
	RecordingID                int64        `json:"recording_id"`
	LocalDate                  string       `json:"local_date"`
	Hour                       int          `json:"hour"`
	Seconds                    float64      `json:"seconds"`
	MediaSecondsMeasurement    string       `json:"media_seconds_measurement"`
	InputIDs                   []int64      `json:"input_ids"`
	InputParts                 int          `json:"input_parts"`
	PartsReduced               int          `json:"parts_reduced"`
	Path                       string       `json:"path"`
	Kind                       string       `json:"kind"`
	ProductionVersion          string       `json:"production_version"`
	ClockBoundaryAnnotations   []Annotation `json:"clock_boundary_annotations"`
	AudioPackets               int          `json:"audio_packets"`
	OriginalSourceAudioPresent bool         `json:"original_source_audio_present"`
	TemporaryNoAudio           bool         `json:"temporary_no_audio"`
	AudioDeliveryStatus        AudioStatus  `json:"audio_delivery_status"`
}

// HourResult is results/<hour_id>.json: every seam verdict and assembly.
type HourResult struct {
	PassVersion    string        `json:"pass_version"`
	HourID         string        `json:"hour_id"`
	RecordingID    int64         `json:"recording_id"`
	LocalDate      string        `json:"local_date"`
	DeliveryHour   int           `json:"delivery_hour"`
	ManifestKey    string        `json:"manifest_key"`
	ManifestSHA256 string        `json:"manifest_sha256"`
	Status         HourStatus    `json:"status"`
	Seams          []SeamVerdict `json:"seams"`
	Assemblies     []Assembly    `json:"assemblies"`
	FinishedAt     time.Time     `json:"finished_at"`
}

// ResultKey is where an hour's result lives under Env.Out.
func ResultKey(hourID string) string { return path.Join("results", hourID+".json") }

// AssemblyKey and ProofKey are the pass's new R2 layout.
func AssemblyKey(batch, sha string) string { return path.Join("joined", batch, "objects", sha+".mp4") }
func ProofKey(batch, sha string) string    { return path.Join("joined", batch, "proofs", sha+".json") }

type part struct {
	out       collation.Output
	delivered Delivered
	ok        bool // in the parent generation
	local     *collation.LocalClip
	check     *PartCheck
}

// ProcessHour evaluates every split of one hour manifest, builds an assembly
// per joined run and writes it create-only under env.Out.
func ProcessHour(ctx context.Context, env Env, manifestKey string) (HourResult, error) {
	body, err := env.Source.Read(ctx, manifestKey)
	if err != nil {
		return HourResult{}, fmt.Errorf("manifest %s: %w", manifestKey, err)
	}
	var m collation.HourManifest
	if err := json.Unmarshal(body, &m); err != nil {
		return HourResult{}, fmt.Errorf("manifest %s: %w", manifestKey, err)
	}
	if err := m.Validate(); err != nil || collation.ManifestKey(m.BatchID, m.HourID) != manifestKey {
		return HourResult{}, fmt.Errorf("manifest %s invalid: %v", manifestKey, err)
	}
	sum := sha256.Sum256(body)
	res := HourResult{PassVersion: PassVersion, HourID: m.HourID, RecordingID: m.RecordingID, LocalDate: m.LocalDate,
		DeliveryHour: m.DeliveryHour, ManifestKey: manifestKey, ManifestSHA256: hex.EncodeToString(sum[:]),
		Seams: []SeamVerdict{}, Assemblies: []Assembly{}}
	profile, hasProfile := env.Profiles[m.RecordingID]
	switch {
	case m.Status != collation.StatusCollated:
		res.Status = HourNotCollated
	case env.VetoRecordings[m.RecordingID]:
		res.Status = HourVetoed
	case !hasProfile:
		res.Status = HourNoProfile
	case len(m.Outputs) < 2:
		res.Status = HourSinglePart
	}
	if res.Status != "" {
		return res, nil
	}
	if err := os.MkdirAll(filepath.Join(env.Out, ".scratch"), 0o700); err != nil {
		return res, err
	}
	dir, err := os.MkdirTemp(filepath.Join(env.Out, ".scratch"), "clockjoin-")
	if err != nil {
		return res, err
	}
	defer os.RemoveAll(dir)
	reader := env.Reader
	reader.Scratch = dir

	parts := make([]*part, len(m.Outputs))
	for i, o := range m.Outputs {
		d, ok := env.Delivered[o.ObjectKey]
		if ok && (d.SHA256 != o.SHA256 || d.SizeBytes != o.SizeBytes || d.RecordingID != m.RecordingID) {
			return res, fmt.Errorf("part %d: delivery identity differs from its manifest", o.Part)
		}
		parts[i] = &part{out: o, delivered: d, ok: ok}
	}
	load := func(p *part) error {
		if p.local != nil {
			return nil
		}
		local, err := env.Source.Fetch(ctx, p.out.ObjectKey, dir)
		if err == nil {
			// ffmpeg's concat list resolves relative names against the list's own directory.
			local, err = filepath.Abs(local)
		}
		if err != nil {
			return fmt.Errorf("part %d: %w", p.out.Part, err)
		}
		if size, sha, err := identity(local); err != nil || size != p.out.SizeBytes || sha != p.out.SHA256 {
			return fmt.Errorf("part %d bytes differ from its manifest (%v)", p.out.Part, err)
		}
		probe, err := collation.ProbeFile(ctx, env.Tools, local)
		if err != nil {
			return err
		}
		if !probe.Media.Playable || probe.Media.VideoPackets != p.out.Verification.VideoPackets {
			return fmt.Errorf("part %d probe differs from its verified %d video packets", p.out.Part, p.out.Verification.VideoPackets)
		}
		p.local = &collation.LocalClip{Clip: collation.Clip{ClipID: p.delivered.ID}, Path: local, Probe: probe}
		return nil
	}
	check := func(p *part) (PartCheck, error) {
		if p.check != nil {
			return *p.check, nil
		}
		head, err := reader.ReadEdge(ctx, p.local.Path, profile, Head)
		if err != nil {
			return PartCheck{}, err
		}
		tail, err := reader.ReadEdge(ctx, p.local.Path, profile, Tail)
		if err != nil {
			return PartCheck{}, err
		}
		c := CheckPart(p.delivered.ID, p.out.Part, p.out.Verification.VideoPackets, p.local.Probe.Media.FrameSeconds, head, tail)
		p.check = &c
		return c, nil
	}

	seamOf := map[[2]int64]collation.SeamDecision{}
	for _, s := range m.Seams {
		seamOf[[2]int64{s.PrevClipID, s.NextClipID}] = s
	}
	joins := make([]bool, len(parts)-1)
	for k := range joins {
		a, b := parts[k], parts[k+1]
		v := SeamVerdict{PrevOutputID: a.delivered.ID, NextOutputID: b.delivered.ID, PrevPart: a.out.Part, NextPart: b.out.Part}
		v.Reason = vetoSeam(env, m, a, b, seamOf, &v)
		if v.Reason == "" {
			if err := load(a); err != nil {
				return res, err
			}
			if err := load(b); err != nil {
				return res, err
			}
			if a.local.Probe.Media.CodecSignature != b.local.Probe.Media.CodecSignature {
				v.Reason = CodecParamsDiffer
			} else {
				pc, err := check(a)
				if err != nil {
					return res, err
				}
				nc, err := check(b)
				if err != nil {
					return res, err
				}
				v = DecideClock(v, pc, nc)
			}
		}
		joins[k] = v.Join
		res.Seams = append(res.Seams, v)
	}
	for start := 0; start < len(parts); {
		end := start
		for end < len(joins) && joins[end] {
			end++
		}
		if end > start {
			a, err := buildAssembly(ctx, env, m, parts, start, end, res.Seams[start:end], dir)
			if err != nil {
				return res, fmt.Errorf("parts %d-%d: %w", start+1, end+1, err)
			}
			res.Assemblies = append(res.Assemblies, a)
		}
		start = end + 1
	}
	res.Status = HourNoJoin
	if len(res.Assemblies) > 0 {
		res.Status = HourJoined
	}
	return res, nil
}

// vetoSeam applies every check that needs no media, in order. It returns ""
// when the seam goes to the clock.
func vetoSeam(env Env, m collation.HourManifest, a, b *part, seamOf map[[2]int64]collation.SeamDecision, v *SeamVerdict) Reason {
	if !a.ok || !b.ok {
		return PartNotDelivered
	}
	if env.Assembled[a.delivered.ID] || env.Assembled[b.delivered.ID] {
		return AlreadyAssembled
	}
	prevClip, nextClip := a.out.SourceClipIDs[len(a.out.SourceClipIDs)-1], b.out.SourceClipIDs[0]
	s, ok := seamOf[[2]int64{prevClip, nextClip}]
	if !ok {
		return NoDirectSeam
	}
	v.CollationReason = s.Reason
	if s.Decision != collation.DecisionSplit || !eligibleReasons[s.Reason] || (s.Reason == collation.ReasonDBGap && math.Abs(s.DBGapSeconds) > MaxDBGapSeconds) {
		return SeamNotEligible
	}
	for _, o := range []collation.Output{a.out, b.out} {
		for _, id := range o.SourceClipIDs {
			if env.VetoClips[id] {
				return KnownBadSource
			}
		}
	}
	var prevEnd, nextStart time.Time
	for _, c := range m.Clips {
		switch c.Clip.ClipID {
		case prevClip:
			prevEnd = c.Clip.EndUTC
		case nextClip:
			nextStart = c.Clip.StartUTC
		}
	}
	for _, c := range m.Clips {
		if c.Disposition == "quarantined" && c.Clip.EndUTC.After(prevEnd.Add(-time.Second)) && c.Clip.StartUTC.Before(nextStart.Add(time.Second)) {
			return QuarantinedClipBetween
		}
	}
	return ""
}

// partName is a collated delivery name: <stem>_hour_HH[_dst2][_part_NN].mp4
// (recordingnaming.BuildCollatedPath; _dst2 is a DST repeat hour).
var partName = regexp.MustCompile(`^(.*_hour_[0-9]{2}(?:_dst2)?)(_part_[0-9]{2,})?\.mp4$`)

// AssemblyPath names an assembly in its parts' own delivery format: a run
// covering the whole hour takes the hour's single-part name, any other run
// takes its first part's name (that part is no longer delivered).
func AssemblyPath(inputs []string, wholeHour bool) (string, error) {
	m := partName.FindStringSubmatch(inputs[0])
	if m == nil || m[2] == "" {
		return "", fmt.Errorf("delivery path %q is not a part name", inputs[0])
	}
	for _, p := range inputs[1:] {
		if n := partName.FindStringSubmatch(p); n == nil || n[1] != m[1] || n[2] == "" {
			return "", fmt.Errorf("delivery paths %q and %q are not parts of one hour", inputs[0], p)
		}
	}
	if wholeHour {
		return m[1] + ".mp4", nil
	}
	return inputs[0], nil
}

func buildAssembly(ctx context.Context, env Env, m collation.HourManifest, parts []*part, start, end int, seams []SeamVerdict, dir string) (Assembly, error) {
	run := parts[start : end+1]
	locals := make([]collation.LocalClip, len(run))
	paths := make([]string, len(run))
	ids := make([]int64, len(run))
	own := map[string]bool{}
	seconds := 0.0
	for i, p := range run {
		locals[i], paths[i], ids[i] = *p.local, p.delivered.Path, p.delivered.ID
		own[p.delivered.Path] = true
		seconds += p.local.Probe.Media.ContentSeconds
	}
	deliveryPath, err := AssemblyPath(paths, len(run) == len(parts))
	if err != nil {
		return Assembly{}, err
	}
	if env.Paths[deliveryPath] && !own[deliveryPath] {
		return Assembly{}, fmt.Errorf("delivery path %s is already another file's", deliveryPath)
	}
	tmp := filepath.Join(dir, fmt.Sprintf("assembly-%d.mp4", ids[0]))
	built, err := collation.BuildPart(ctx, env.Tools, locals, tmp)
	if err != nil {
		return Assembly{}, err
	}
	if err := collation.VerifyFullDecode(ctx, env.Tools, tmp); err != nil {
		return Assembly{}, fmt.Errorf("full decode: %w", err)
	}
	// VerifyPart proved the assembly's audio is exactly the parts' audio packets.
	audio := built.Verification.AudioPackets
	if built.SizeBytes >= maxAssemblyBytes {
		return Assembly{}, fmt.Errorf("assembly is %d bytes, over a single-part upload", built.SizeBytes)
	}
	key := AssemblyKey(env.Batch, built.SHA256)
	if err := placeCreateOnly(tmp, filepath.Join(env.Out, filepath.FromSlash(key)), built.SHA256); err != nil {
		return Assembly{}, err
	}
	etag, err := fileMD5(filepath.Join(env.Out, filepath.FromSlash(key)))
	if err != nil {
		return Assembly{}, err
	}
	proof, err := json.Marshal(map[string]any{
		"pass_version": PassVersion, "production_version": ProductionVersion, "method": Method,
		"policy":     map[string]any{"max_clock_gap_seconds": MaxGapSeconds, "max_relative_fps_error": MaxRelativeFPSError, "max_db_gap_seconds": MaxDBGapSeconds, "eligible_collation_reasons": eligibleReasons},
		"media_tool": env.MediaTool, "clock_tool": env.ClockTool, "profile": env.Profiles[m.RecordingID],
		"recording_id": m.RecordingID, "hour_id": m.HourID, "local_date": m.LocalDate, "delivery_hour": m.DeliveryHour,
		"manifest_key": collation.ManifestKey(m.BatchID, m.HourID),
		"inputs":       inputsProof(run), "seams": seams,
		"output": map[string]any{"key": key, "sha256": built.SHA256, "size_bytes": built.SizeBytes, "verification": built.Verification},
	})
	if err != nil {
		return Assembly{}, err
	}
	proofSum := sha256.Sum256(proof)
	proofSHA := hex.EncodeToString(proofSum[:])
	proofKey := ProofKey(env.Batch, built.SHA256)
	proofTmp := filepath.Join(dir, "proof.json")
	if err := os.WriteFile(proofTmp, proof, 0o600); err != nil {
		return Assembly{}, err
	}
	if err := placeCreateOnly(proofTmp, filepath.Join(env.Out, filepath.FromSlash(proofKey)), proofSHA); err != nil {
		return Assembly{}, err
	}
	proofMD5 := md5.Sum(proof)
	a := Assembly{SHA256: built.SHA256, SizeBytes: built.SizeBytes, ETag: etag, Key: key,
		ProofKey: proofKey, ProofSHA256: proofSHA, ProofSizeBytes: int64(len(proof)), ProofETag: hex.EncodeToString(proofMD5[:]),
		RecordingID: m.RecordingID, LocalDate: m.LocalDate, Hour: m.DeliveryHour, Seconds: math.Round(seconds*1e6) / 1e6,
		MediaSecondsMeasurement: "verified_native_video_presentation_span",
		InputIDs:                ids, InputParts: len(ids), PartsReduced: len(ids) - 1, Path: deliveryPath,
		Kind: "clock", ProductionVersion: ProductionVersion, AudioPackets: audio,
		OriginalSourceAudioPresent: audio > 0, AudioDeliveryStatus: AudioNoSource}
	if audio > 0 {
		a.AudioDeliveryStatus = AudioCopied
	}
	for _, s := range seams {
		a.ClockBoundaryAnnotations = append(a.ClockBoundaryAnnotations, Annotation{Scope: annotationScope, Method: Method, Status: annotationStatus,
			ArtifactSHA256: built.SHA256, ProofSHA256: proofSHA, SourceInputIDs: [2]int64{s.PrevOutputID, s.NextOutputID},
			MaxRelativeError: math.Max(s.Prev.RelativeFPSError, s.Next.RelativeFPSError)})
	}
	return a, nil
}

func inputsProof(run []*part) []map[string]any {
	out := make([]map[string]any, len(run))
	for i, p := range run {
		out[i] = map[string]any{"output_id": p.delivered.ID, "part": p.out.Part, "object_key": p.out.ObjectKey,
			"sha256": p.out.SHA256, "size_bytes": p.out.SizeBytes, "delivery_path": p.delivered.Path, "clock": p.check}
	}
	return out
}

// placeCreateOnly moves src to dst unless dst exists. An existing dst must
// already hold exactly sha (a rerun); anything else is refused.
func placeCreateOnly(src, dst, sha string) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
		return err
	}
	if err := os.Link(src, dst); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return err
		}
		if _, got, err := identity(dst); err != nil || got != sha {
			return fmt.Errorf("%s exists with other bytes; refusing to overwrite", dst)
		}
	}
	return os.Remove(src)
}

// WriteResult writes results/<hour_id>.json create-only.
func WriteResult(out string, r HourResult) error {
	body, err := json.MarshalIndent(r, "", " ")
	if err != nil {
		return err
	}
	dst := filepath.Join(out, filepath.FromSlash(ResultKey(r.HourID)))
	if err := os.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
		return err
	}
	f, err := os.OpenFile(dst, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(append(body, '\n')); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

func identity(p string) (int64, string, error) {
	f, err := os.Open(p)
	if err != nil {
		return 0, "", err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	return n, hex.EncodeToString(h.Sum(nil)), err
}

func fileMD5(p string) (string, error) {
	f, err := os.Open(p)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := md5.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
