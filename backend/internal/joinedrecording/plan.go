package joinedrecording

import (
	"encoding/hex"
	"fmt"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/daydemir/stoarama/backend/internal/recordingnaming"
	"github.com/daydemir/stoarama/backend/internal/stitchcert"
)

const PlanPolicyVersion = "joined-delivery-v1"

var safeBatchID = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,62}$`)

// ValidBatchID reports whether batchID is one canonical joined batch identity.
func ValidBatchID(batchID string) bool {
	return safeBatchID.MatchString(batchID)
}

type ObjectIdentity struct {
	Key       string `json:"key"`
	VersionID string `json:"version_id,omitempty"`
	ETag      string `json:"etag"`
	SizeBytes int64  `json:"size_bytes"`
	SHA256    string `json:"sha256"`
}

type SeamEvidence struct {
	Verdict              string `json:"verdict"`
	Reason               string `json:"reason"`
	SignedGapNanoseconds int64  `json:"signed_gap_nanoseconds"`
}

type SourceClip struct {
	ClipID               int64                  `json:"clip_id"`
	RecordingID          int64                  `json:"recording_id"`
	RecordingJobID       int64                  `json:"recording_job_id"`
	StorageDestinationID int64                  `json:"storage_destination_id"`
	Provider             string                 `json:"provider"`
	Endpoint             string                 `json:"endpoint"`
	Region               string                 `json:"region"`
	Bucket               string                 `json:"bucket"`
	StartUTC             time.Time              `json:"start_utc"`
	EndUTC               time.Time              `json:"end_utc"`
	ReleasedAt           *time.Time             `json:"released_at"`
	Object               ObjectIdentity         `json:"object"`
	AudioContract        *AudioSequenceContract `json:"audio_sequence_contract,omitempty"`
	SeamToPrevious       SeamEvidence           `json:"seam_to_previous,omitempty"`
}

type PlanRequest struct {
	BatchID             string
	Generation          int
	RecordingID         int64
	Timezone            string
	LocalDate           string
	DeliveryHour        int
	FolderName          string
	Metadata            recordingnaming.Metadata
	Qualification       QualificationWindow
	AllocationLedgerSHA string
	MediaTool           MediaToolEvidence
	Sources             []SourceClip
	QuarantinedSources  []SourceClip
	BuiltArtifacts      []BuiltArtifactIdentity
	PreviousDayLast     *SourceClip
	NextDayFirst        *SourceClip
}

type BuiltArtifactIdentity struct {
	SizeBytes         int64  `json:"size_bytes"`
	SHA256            string `json:"sha256"`
	MediaToolIdentity string `json:"media_tool_identity"`
}

type Gap struct {
	PreviousClipID       int64     `json:"previous_clip_id"`
	NextClipID           int64     `json:"next_clip_id"`
	AtUTC                time.Time `json:"at_utc"`
	SignedGapNanoseconds int64     `json:"signed_gap_nanoseconds"`
	Reason               string    `json:"reason"`
}

type OutputPlan struct {
	Ordinal      int          `json:"ordinal"`
	Hour         int          `json:"hour"`
	Part         int          `json:"part"`
	Parts        int          `json:"parts"`
	ActualStart  time.Time    `json:"actual_start_utc"`
	ActualEnd    time.Time    `json:"actual_end_utc"`
	UTCOffsetSec int          `json:"utc_offset_seconds"`
	RelativePath string       `json:"relative_path"`
	ObjectKey    string       `json:"object_key"`
	ContentID    string       `json:"content_id"`
	ExpectedSize int64        `json:"expected_size_bytes"`
	ExpectedSHA  string       `json:"expected_sha256"`
	MediaToolID  string       `json:"media_tool_identity"`
	Sources      []SourceClip `json:"sources"`
}

type BatchPlan struct {
	SchemaVersion       int                      `json:"schema_version"`
	PolicyVersion       string                   `json:"policy_version"`
	BatchID             string                   `json:"batch_id"`
	Generation          int                      `json:"generation"`
	HourID              string                   `json:"hour_id"`
	RecordingID         int64                    `json:"recording_id"`
	Timezone            string                   `json:"timezone"`
	FolderName          string                   `json:"folder_name"`
	Metadata            recordingnaming.Metadata `json:"naming_metadata"`
	Qualification       QualificationWindow      `json:"qualification_window"`
	AllocationLedgerSHA string                   `json:"allocation_ledger_sha256"`
	MediaTool           MediaToolEvidence        `json:"media_tool"`
	LocalDate           string                   `json:"local_date"`
	LocalHour           int                      `json:"local_hour"`
	SourceClaimSHA256   string                   `json:"source_claim_sha256"`
	ExpectedOutputCount int                      `json:"expected_output_count"`
	CoverageObjectKey   string                   `json:"coverage_object_key"`
	GapOnly             bool                     `json:"gap_only"`
	GapOnlyReason       string                   `json:"gap_only_reason,omitempty"`
	QuarantineReason    string                   `json:"quarantine_reason_code"`
	Sources             []SourceClip             `json:"sources"`
	QuarantinedSources  []SourceClip             `json:"quarantined_sources"`
	Gaps                []Gap                    `json:"gaps"`
	Outputs             []OutputPlan             `json:"outputs"`
}

type draftOutput struct {
	hour    int
	dayKey  string
	sources []SourceClip
}

func BuildPlan(req PlanRequest) (BatchPlan, error) {
	return buildPlan(req, true)
}

type HourDraft struct {
	LocalDate string       `json:"local_date"`
	LocalHour int          `json:"local_hour"`
	Gaps      []Gap        `json:"gaps"`
	Parts     []OutputPlan `json:"parts"`
}

// DiscoverHourPlan determines all final part boundaries and delivery names.
// The caller builds these exact parts once, then supplies their identities to
// BuildPlan for immutable sealing and upload of the same scratch artifacts.
func DiscoverHourPlan(req PlanRequest) (HourDraft, error) {
	plan, err := buildPlan(req, false)
	if err != nil {
		return HourDraft{}, err
	}
	return HourDraft{LocalDate: plan.LocalDate, LocalHour: plan.LocalHour, Gaps: plan.Gaps, Parts: plan.Outputs}, nil
}

func buildPlan(req PlanRequest, seal bool) (BatchPlan, error) {
	if !safeBatchID.MatchString(req.BatchID) || req.Generation <= 0 || req.RecordingID <= 0 || req.DeliveryHour < 1 || req.DeliveryHour > 12 || len(req.Sources) == 0 {
		return BatchPlan{}, fmt.Errorf("invalid bounded joined plan request")
	}
	if ValidateQualificationWindow(req.Qualification) != nil || req.Qualification.RecordingID != req.RecordingID || req.Qualification.Timezone != req.Timezone || !lowerHex64(req.AllocationLedgerSHA) || ValidateMediaToolEvidence(req.MediaTool) != nil {
		return BatchPlan{}, fmt.Errorf("exact frozen qualification window is required")
	}
	loc, err := time.LoadLocation(strings.TrimSpace(req.Timezone))
	if err != nil {
		return BatchPlan{}, fmt.Errorf("load joined timezone: %w", err)
	}
	folderName, err := recordingnaming.BuildFolderName(recordingnaming.ProfilePlazaHourlyV1, req.RecordingID, req.Metadata, req.FolderName)
	if err != nil {
		return BatchPlan{}, err
	}
	req.FolderName = folderName
	accountedSources, err := mergeAccountedSources(req.Sources, req.QuarantinedSources)
	if err != nil {
		return BatchPlan{}, err
	}
	quarantinedIDs := make(map[int64]bool, len(req.QuarantinedSources))
	seenIDs, seenKeys := map[int64]bool{}, map[string]bool{}
	for i, source := range req.QuarantinedSources {
		if validatePreflightSource(source, req.RecordingID) != nil || !req.Qualification.permits(source) {
			return BatchPlan{}, fmt.Errorf("quarantined source %d differs", i+1)
		}
		quarantinedIDs[source.ClipID] = true
	}
	if len(accountedSources) > 0 {
		accountedSources[0].SeamToPrevious = SeamEvidence{}
	}
	applyAccountedSources(&req, accountedSources)
	gaps := []Gap{}
	drafts := []draftOutput{}
	var current *draftOutput
	var planDayKey string
	var planHour int
	for i, clip := range accountedSources {
		if !quarantinedIDs[clip.ClipID] {
			if err := validateSource(clip, req.RecordingID); err != nil {
				return BatchPlan{}, fmt.Errorf("source %d: %w", i+1, err)
			}
		} else if err := validatePreflightSource(clip, req.RecordingID); err != nil {
			return BatchPlan{}, fmt.Errorf("source %d: %w", i+1, err)
		}
		if !req.Qualification.permits(clip) {
			return BatchPlan{}, fmt.Errorf("source %d is outside the frozen jobs or requested window", i+1)
		}
		storageKey := sourceStorageKey(clip)
		if seenIDs[clip.ClipID] || seenKeys[storageKey] {
			return BatchPlan{}, fmt.Errorf("duplicate source identity")
		}
		seenIDs[clip.ClipID], seenKeys[storageKey] = true, true
		local := clip.StartUTC.In(loc)
		hour := req.DeliveryHour
		endLocal := clip.EndUTC.In(loc)
		if local.Format("2006-01-02") != req.LocalDate || endLocal.Format("2006-01-02") != req.LocalDate {
			return BatchPlan{}, fmt.Errorf("source %d is outside one local delivery day", i+1)
		}
		dayKey := req.LocalDate
		if i == 0 {
			planDayKey, planHour = dayKey, hour
		} else if dayKey != planDayKey || hour != planHour {
			return BatchPlan{}, fmt.Errorf("one batch must contain exactly one local delivery hour")
		}
		continuous := i == 0
		if i > 0 {
			prev := accountedSources[i-1]
			actualGap := clip.StartUTC.Sub(prev.EndUTC).Nanoseconds()
			if clip.StartUTC.Before(prev.StartUTC) || (clip.StartUTC.Equal(prev.StartUTC) && clip.ClipID <= prev.ClipID) || validateDerivedSeam(prev, clip) != nil {
				return BatchPlan{}, fmt.Errorf("sources are not in chronological order")
			}
			continuous = !quarantinedIDs[prev.ClipID] && !quarantinedIDs[clip.ClipID] && clip.SeamToPrevious.Verdict == "continuous" && clip.SeamToPrevious.Reason != "" && clip.SeamToPrevious.SignedGapNanoseconds == 0
			if !continuous {
				gap := actualGap
				reason := clip.SeamToPrevious.Reason
				if quarantinedIDs[prev.ClipID] || quarantinedIDs[clip.ClipID] {
					reason = "source_quarantined"
				}
				gaps = append(gaps, Gap{PreviousClipID: prev.ClipID, NextClipID: clip.ClipID, AtUTC: prev.EndUTC, SignedGapNanoseconds: gap, Reason: reason})
			}
		}
		if quarantinedIDs[clip.ClipID] {
			current = nil
			continue
		}
		if current == nil || !continuous || current.hour != hour || current.dayKey != dayKey {
			drafts = append(drafts, draftOutput{hour: hour, dayKey: dayKey, sources: []SourceClip{clip}})
			current = &drafts[len(drafts)-1]
		} else {
			current.sources = append(current.sources, clip)
		}
	}
	if seal && len(req.BuiltArtifacts) != len(drafts) {
		return BatchPlan{}, fmt.Errorf("every preflight-built hour part identity is required before seal")
	}
	return finalizePlan(req, drafts, gaps, loc, seal)
}

func sourceStorageKey(source SourceClip) string {
	return strings.Join([]string{strconv.FormatInt(source.StorageDestinationID, 10), source.Provider, source.Endpoint, source.Region, source.Bucket, source.Object.Key, source.Object.VersionID, source.Object.ETag}, "\x00")
}

func validateDerivedSeam(previous, next SourceClip) error {
	signedGap := next.StartUTC.Sub(previous.EndUTC).Nanoseconds()
	seam := next.SeamToPrevious
	validVerdict := (seam.Verdict == "continuous" && signedGap == 0) || (seam.Verdict == "gap" && signedGap > 0) || (seam.Verdict == "overlap" && signedGap < 0) || seam.Verdict == "incompatible"
	if !validVerdict || !reasonCode.MatchString(seam.Reason) || seam.SignedGapNanoseconds != signedGap {
		return fmt.Errorf("derived source seam differs")
	}
	return nil
}

func applyAccountedSources(req *PlanRequest, accounted []SourceClip) {
	byID := make(map[int64]SourceClip, len(accounted))
	for _, source := range accounted {
		byID[source.ClipID] = source
	}
	req.Sources = append([]SourceClip(nil), req.Sources...)
	req.QuarantinedSources = append([]SourceClip(nil), req.QuarantinedSources...)
	for i := range req.Sources {
		req.Sources[i] = byID[req.Sources[i].ClipID]
	}
	for i := range req.QuarantinedSources {
		req.QuarantinedSources[i] = byID[req.QuarantinedSources[i].ClipID]
	}
}

// BuildGapOnlyHourPlan accounts for one completely missing scheduled hour.
// It publishes coverage evidence only and can never produce NAS media.
func BuildGapOnlyHourPlan(req PlanRequest, localDate string, deliveryHour int, reason string) (BatchPlan, error) {
	if !safeBatchID.MatchString(req.BatchID) || req.Generation <= 0 || req.RecordingID <= 0 || len(req.Sources) != 0 || deliveryHour < 1 || deliveryHour > 12 || !reasonCode.MatchString(reason) || ValidateQualificationWindow(req.Qualification) != nil || req.Qualification.RecordingID != req.RecordingID || req.Qualification.Timezone != req.Timezone || !lowerHex64(req.AllocationLedgerSHA) || ValidateMediaToolEvidence(req.MediaTool) != nil {
		return BatchPlan{}, fmt.Errorf("invalid gap-only local hour")
	}
	loc, err := time.LoadLocation(req.Timezone)
	if err != nil {
		return BatchPlan{}, err
	}
	date, err := time.ParseInLocation("2006-01-02", localDate, loc)
	if err != nil || !qualificationContainsDate(req.Qualification, localDate) {
		return BatchPlan{}, fmt.Errorf("gap-only hour is outside qualification window")
	}
	_ = date
	folder, err := recordingnaming.BuildFolderName(recordingnaming.ProfilePlazaHourlyV1, req.RecordingID, req.Metadata, req.FolderName)
	if err != nil {
		return BatchPlan{}, err
	}
	empty := []SourceClip{}
	manifestSHA, _, err := sourceClaimSHA(empty)
	if err != nil {
		return BatchPlan{}, err
	}
	hourID, err := canonicalHourID(req, localDate, deliveryHour, manifestSHA)
	if err != nil {
		return BatchPlan{}, err
	}
	plan := BatchPlan{SchemaVersion: 1, PolicyVersion: PlanPolicyVersion, BatchID: req.BatchID, Generation: req.Generation, HourID: hourID, RecordingID: req.RecordingID, Timezone: req.Timezone, FolderName: folder, Metadata: req.Metadata, Qualification: req.Qualification, AllocationLedgerSHA: req.AllocationLedgerSHA, MediaTool: req.MediaTool, LocalDate: localDate, LocalHour: deliveryHour, SourceClaimSHA256: manifestSHA, ExpectedOutputCount: 0, GapOnly: true, GapOnlyReason: reason, Sources: []SourceClip{}, Gaps: []Gap{}, Outputs: []OutputPlan{}}
	plan.CoverageObjectKey = canonicalBatchCoverageKey(plan)
	return plan, nil
}

// BuildQuarantineOnlyHourPlan accounts for sources that cannot safely produce
// media. It preserves every exact source identity and emits only the canonical
// hour manifest. The reason is a stable, non-secret operator code.
func BuildQuarantineOnlyHourPlan(req PlanRequest, localDate string, deliveryHour int, reason string) (BatchPlan, error) {
	if !safeBatchID.MatchString(req.BatchID) || req.Generation <= 0 || req.RecordingID <= 0 || len(req.Sources) == 0 || deliveryHour < 1 || deliveryHour > 12 || !reasonCode.MatchString(reason) || ValidateQualificationWindow(req.Qualification) != nil || req.Qualification.RecordingID != req.RecordingID || req.Qualification.Timezone != req.Timezone || !lowerHex64(req.AllocationLedgerSHA) || ValidateMediaToolEvidence(req.MediaTool) != nil {
		return BatchPlan{}, fmt.Errorf("invalid quarantine-only local hour")
	}
	loc, err := time.LoadLocation(req.Timezone)
	if err != nil || !qualificationContainsDate(req.Qualification, localDate) {
		return BatchPlan{}, fmt.Errorf("quarantine-only hour is outside qualification window")
	}
	req.Sources = append([]SourceClip(nil), req.Sources...)
	req.Sources[0].SeamToPrevious = SeamEvidence{}
	seen := map[int64]bool{}
	for i, source := range req.Sources {
		if validatePreflightSource(source, req.RecordingID) != nil || (i > 0 && validateDerivedSeam(req.Sources[i-1], source) != nil) || !req.Qualification.permits(source) || seen[source.ClipID] || source.StartUTC.In(loc).Format("2006-01-02") != localDate || source.EndUTC.In(loc).Format("2006-01-02") != localDate {
			return BatchPlan{}, fmt.Errorf("quarantine source %d differs", i+1)
		}
		seen[source.ClipID] = true
	}
	folder, err := recordingnaming.BuildFolderName(recordingnaming.ProfilePlazaHourlyV1, req.RecordingID, req.Metadata, req.FolderName)
	if err != nil {
		return BatchPlan{}, err
	}
	manifestSHA, _, err := sourceClaimSHA(req.Sources)
	if err != nil {
		return BatchPlan{}, err
	}
	hourID, err := canonicalHourID(req, localDate, deliveryHour, manifestSHA)
	if err != nil {
		return BatchPlan{}, err
	}
	gaps := make([]Gap, 0, len(req.Sources)-1)
	for i := 1; i < len(req.Sources); i++ {
		previous, next := req.Sources[i-1], req.Sources[i]
		gaps = append(gaps, Gap{PreviousClipID: previous.ClipID, NextClipID: next.ClipID, AtUTC: previous.EndUTC, SignedGapNanoseconds: next.SeamToPrevious.SignedGapNanoseconds, Reason: "source_quarantined"})
	}
	plan := BatchPlan{SchemaVersion: 1, PolicyVersion: PlanPolicyVersion, BatchID: req.BatchID, Generation: req.Generation, HourID: hourID, RecordingID: req.RecordingID, Timezone: req.Timezone, FolderName: folder, Metadata: req.Metadata, Qualification: req.Qualification, AllocationLedgerSHA: req.AllocationLedgerSHA, MediaTool: req.MediaTool, LocalDate: localDate, LocalHour: deliveryHour, SourceClaimSHA256: manifestSHA, ExpectedOutputCount: 0, QuarantineReason: reason, Sources: append([]SourceClip(nil), req.Sources...), QuarantinedSources: append([]SourceClip(nil), req.Sources...), Gaps: gaps, Outputs: []OutputPlan{}}
	plan.CoverageObjectKey = canonicalBatchCoverageKey(plan)
	return plan, nil
}

func finalizePlan(req PlanRequest, drafts []draftOutput, gaps []Gap, loc *time.Location, seal bool) (BatchPlan, error) {
	allSources, err := mergeAccountedSources(req.Sources, req.QuarantinedSources)
	if err != nil {
		return BatchPlan{}, err
	}
	manifestSHA, _, err := sourceClaimSHA(allSources)
	if err != nil {
		return BatchPlan{}, err
	}
	hourID, err := canonicalHourID(req, drafts[0].dayKey, drafts[0].hour, manifestSHA)
	if err != nil {
		return BatchPlan{}, err
	}
	batchID := req.BatchID
	partCounts := map[string]int{}
	for _, d := range drafts {
		partCounts[fmt.Sprintf("%s/%02d", d.dayKey, d.hour)]++
	}
	partSeen := map[string]int{}
	outputs := make([]OutputPlan, 0, len(drafts))
	for i, d := range drafts {
		artifact := BuiltArtifactIdentity{}
		if seal {
			artifact = req.BuiltArtifacts[i]
			if artifact.SizeBytes <= 0 || artifact.SizeBytes > r2.MaxConditionalPutBytes || !lowerHex64(artifact.SHA256) || artifact.MediaToolIdentity != req.MediaTool.IdentitySHA256 {
				return BatchPlan{}, fmt.Errorf("invalid preflight-built artifact identity")
			}
		}
		bucket := fmt.Sprintf("%s/%02d", d.dayKey, d.hour)
		partSeen[bucket]++
		start, end := d.sources[0].StartUTC, d.sources[len(d.sources)-1].EndUTC
		relative, err := recordingnaming.BuildJoinedPath(recordingnaming.JoinedPolicy{FolderName: req.FolderName, Metadata: req.Metadata, CronTimezone: req.Timezone, ActualStart: start, ActualEnd: end, Hour: d.hour, Part: partSeen[bucket], Parts: partCounts[bucket]})
		if err != nil {
			return BatchPlan{}, err
		}
		contentID := artifact.SHA256
		_, offset := start.In(loc).Zone()
		objectKey := ""
		if seal {
			objectKey = path.Join("joined", batchID, "objects", contentID+".mp4")
		}
		outputs = append(outputs, OutputPlan{Ordinal: i + 1, Hour: d.hour, Part: partSeen[bucket], Parts: partCounts[bucket], ActualStart: start, ActualEnd: end, UTCOffsetSec: offset, RelativePath: relative, ObjectKey: objectKey, ContentID: contentID, ExpectedSize: artifact.SizeBytes, ExpectedSHA: artifact.SHA256, MediaToolID: artifact.MediaToolIdentity, Sources: d.sources})
	}
	plan := BatchPlan{SchemaVersion: 1, PolicyVersion: PlanPolicyVersion, BatchID: batchID, Generation: req.Generation, HourID: hourID, RecordingID: req.RecordingID, Timezone: req.Timezone, FolderName: req.FolderName, Metadata: req.Metadata, Qualification: req.Qualification, AllocationLedgerSHA: req.AllocationLedgerSHA, MediaTool: req.MediaTool, LocalDate: drafts[0].dayKey, LocalHour: drafts[0].hour, SourceClaimSHA256: manifestSHA, ExpectedOutputCount: len(outputs), Sources: allSources, QuarantinedSources: append([]SourceClip{}, req.QuarantinedSources...), Gaps: gaps, Outputs: outputs}
	if !seal {
		return plan, nil
	}
	plan.CoverageObjectKey = canonicalBatchCoverageKey(plan)
	return plan, nil
}

func canonicalHourID(req PlanRequest, localDate string, localHour int, manifestSHA string) (string, error) {
	if !lowerHex64(manifestSHA) || !safeBatchID.MatchString(req.BatchID) || req.Generation <= 0 || req.RecordingID <= 0 || localHour < 1 || localHour > 12 {
		return "", fmt.Errorf("invalid canonical hour identity")
	}
	if _, err := time.Parse("2006-01-02", localDate); err != nil {
		return "", fmt.Errorf("invalid canonical hour date")
	}
	return canonicalHourIDValue(req.BatchID, req.RecordingID, localDate, localHour, req.Generation), nil
}

func canonicalHourIDValue(batchID string, recordingID int64, localDate string, deliveryHour, generation int) string {
	value, _ := CanonicalHourID(batchID, recordingID, localDate, deliveryHour, generation)
	return value
}

// CanonicalHourID returns the one logical hour identity used on every wire
// contract. Database surrogate IDs are deliberately excluded.
func CanonicalHourID(batchID string, recordingID int64, localDate string, deliveryHour, generation int) (string, error) {
	if !safeBatchID.MatchString(batchID) || recordingID <= 0 || !validLocalDate(localDate) || deliveryHour < 1 || deliveryHour > 12 || generation <= 0 {
		return "", fmt.Errorf("invalid canonical hour identity")
	}
	return fmt.Sprintf("%s__recording-%d__date-%s__hour-%02d__generation-%d", batchID, recordingID, localDate, deliveryHour, generation), nil
}

func mergeAccountedSources(included, quarantined []SourceClip) ([]SourceClip, error) {
	all := append(append([]SourceClip{}, included...), quarantined...)
	sort.SliceStable(all, func(i, j int) bool {
		if all[i].StartUTC.Equal(all[j].StartUTC) {
			return all[i].ClipID < all[j].ClipID
		}
		return all[i].StartUTC.Before(all[j].StartUTC)
	})
	seen := map[int64]bool{}
	for _, source := range all {
		if seen[source.ClipID] {
			return nil, fmt.Errorf("accounted source appears as both included and quarantined")
		}
		seen[source.ClipID] = true
	}
	return all, nil
}

// sourceClaimSHA excludes media-derived audio and seam evidence so inspection
// cannot change the source-only lease identity.
func sourceClaimSHA(sources []SourceClip) (string, []byte, error) {
	return CanonicalSourceClaim(sources)
}

// CanonicalSourceClaim is the one source-only projection used for preflight,
// allocation ledgers, hour manifests, and denominator evidence.
func CanonicalSourceClaim(sources []SourceClip) (string, []byte, error) {
	claims := sourceOnlyClips(sources)
	return stitchcert.CanonicalSHA(claims)
}

func ValidateSourceClaim(sources []SourceClip, expectedSHA256 string) error {
	digest, _, err := CanonicalSourceClaim(sources)
	if err != nil || !lowerHex64(expectedSHA256) || digest != expectedSHA256 {
		return fmt.Errorf("source-only claim differs")
	}
	return nil
}

func sourceOnlyClips(sources []SourceClip) []SourceClip {
	claims := append([]SourceClip(nil), sources...)
	for i := range claims {
		claims[i].AudioContract = nil
		claims[i].SeamToPrevious = SeamEvidence{}
	}
	return claims
}

func candidateSourceClaimSHA(sources []SourceClip) (string, error) {
	claims := make([]struct {
		ClipID            int64  `json:"clip_id"`
		SourceClaimSHA256 string `json:"source_claim_sha256"`
	}, len(sources))
	for i, source := range sources {
		digest, _, err := sourceClaimSHA([]SourceClip{source})
		if err != nil {
			return "", err
		}
		claims[i] = struct {
			ClipID            int64  `json:"clip_id"`
			SourceClaimSHA256 string `json:"source_claim_sha256"`
		}{source.ClipID, digest}
	}
	digest, _, err := stitchcert.CanonicalSHA(claims)
	return digest, err
}

func ValidatePlan(plan BatchPlan) error {
	var err error
	statusCount := 0
	if len(plan.Outputs) > 0 {
		statusCount++
	}
	if plan.GapOnly {
		statusCount++
	}
	if plan.QuarantineReason != "" {
		statusCount++
	}
	if plan.SchemaVersion != 1 || plan.PolicyVersion != PlanPolicyVersion || !safeBatchID.MatchString(plan.BatchID) || plan.LocalHour < 1 || plan.LocalHour > 12 || plan.HourID != canonicalHourIDValue(plan.BatchID, plan.RecordingID, plan.LocalDate, plan.LocalHour, plan.Generation) || plan.Generation <= 0 || plan.ExpectedOutputCount != len(plan.Outputs) || plan.CoverageObjectKey != canonicalBatchCoverageKey(plan) || statusCount != 1 || (plan.GapOnly && (!reasonCode.MatchString(plan.GapOnlyReason) || len(plan.Sources) != 0)) || (!plan.GapOnly && plan.GapOnlyReason != "") || (plan.QuarantineReason != "" && (!reasonCode.MatchString(plan.QuarantineReason) || len(plan.Sources) == 0)) {
		return fmt.Errorf("joined batch plan is not sealed")
	}
	if plan.GapOnly {
		rebuilt, rebuildErr := BuildGapOnlyHourPlan(PlanRequest{BatchID: plan.BatchID, Generation: plan.Generation, RecordingID: plan.RecordingID, Timezone: plan.Timezone, FolderName: plan.FolderName, Metadata: plan.Metadata, Qualification: plan.Qualification, AllocationLedgerSHA: plan.AllocationLedgerSHA, MediaTool: plan.MediaTool}, plan.LocalDate, plan.LocalHour, plan.GapOnlyReason)
		if rebuildErr != nil {
			return fmt.Errorf("rebuild gap-only hour: %w", rebuildErr)
		}
		_, original, _ := stitchcert.CanonicalSHA(plan)
		_, canonical, _ := stitchcert.CanonicalSHA(rebuilt)
		if string(original) != string(canonical) {
			return fmt.Errorf("gap-only hour differs from canonical reconstruction")
		}
		return nil
	}
	if plan.QuarantineReason != "" {
		rebuilt, rebuildErr := BuildQuarantineOnlyHourPlan(PlanRequest{BatchID: plan.BatchID, Generation: plan.Generation, RecordingID: plan.RecordingID, Timezone: plan.Timezone, FolderName: plan.FolderName, Metadata: plan.Metadata, Qualification: plan.Qualification, AllocationLedgerSHA: plan.AllocationLedgerSHA, MediaTool: plan.MediaTool, Sources: plan.Sources}, plan.LocalDate, plan.LocalHour, plan.QuarantineReason)
		if rebuildErr != nil {
			return fmt.Errorf("rebuild quarantine-only hour: %w", rebuildErr)
		}
		_, original, _ := stitchcert.CanonicalSHA(plan)
		_, canonical, _ := stitchcert.CanonicalSHA(rebuilt)
		if string(original) != string(canonical) {
			return fmt.Errorf("quarantine-only hour differs from canonical reconstruction")
		}
		return nil
	}
	seenClips := map[int64]bool{}
	flat := []SourceClip{}
	for _, output := range plan.Outputs {
		for _, source := range output.Sources {
			if seenClips[source.ClipID] || validateSource(source, plan.RecordingID) != nil {
				return fmt.Errorf("joined plan assigns a source more than once")
			}
			seenClips[source.ClipID] = true
			flat = append(flat, source)
		}
	}
	accounted, err := mergeAccountedSources(flat, plan.QuarantinedSources)
	if err != nil {
		return err
	}
	for _, source := range plan.QuarantinedSources {
		if validatePreflightSource(source, plan.RecordingID) != nil {
			return fmt.Errorf("joined plan has invalid quarantined source")
		}
	}
	_, flatJSON, err := stitchcert.CanonicalSHA(accounted)
	if err != nil {
		return err
	}
	_, sourceJSON, err := stitchcert.CanonicalSHA(plan.Sources)
	if err != nil || string(flatJSON) != string(sourceJSON) {
		return fmt.Errorf("joined plan top-level source assignment differs")
	}
	artifacts := make([]BuiltArtifactIdentity, len(plan.Outputs))
	for i, output := range plan.Outputs {
		artifacts[i] = BuiltArtifactIdentity{SizeBytes: output.ExpectedSize, SHA256: output.ExpectedSHA, MediaToolIdentity: output.MediaToolID}
	}
	rebuilt, err := BuildPlan(PlanRequest{BatchID: plan.BatchID, Generation: plan.Generation, RecordingID: plan.RecordingID, Timezone: plan.Timezone, LocalDate: plan.LocalDate, DeliveryHour: plan.LocalHour, FolderName: plan.FolderName, Metadata: plan.Metadata, Qualification: plan.Qualification, AllocationLedgerSHA: plan.AllocationLedgerSHA, MediaTool: plan.MediaTool, Sources: flat, QuarantinedSources: plan.QuarantinedSources, BuiltArtifacts: artifacts})
	if err != nil {
		return fmt.Errorf("rebuild joined plan: %w", err)
	}
	_, original, err := stitchcert.CanonicalSHA(plan)
	if err != nil {
		return err
	}
	_, canonical, err := stitchcert.CanonicalSHA(rebuilt)
	if err != nil || string(original) != string(canonical) {
		return fmt.Errorf("joined plan differs from canonical reconstruction")
	}
	return nil
}

func canonicalBatchCoverageKey(plan BatchPlan) string {
	return path.Join("joined", plan.BatchID, "coverage", "hours", plan.HourID+".json")
}

func validateSource(c SourceClip, recordingID int64) error {
	_, endpointErr := CanonicalSourceEndpointAuthority(c.Endpoint)
	if c.ClipID <= 0 || c.RecordingID != recordingID || c.RecordingJobID <= 0 || c.StorageDestinationID <= 0 || strings.TrimSpace(c.Provider) == "" || endpointErr != nil || strings.TrimSpace(c.Region) == "" || strings.TrimSpace(c.Bucket) == "" || !c.EndUTC.After(c.StartUTC) || c.EndUTC.Sub(c.StartUTC) > 15*time.Minute {
		return fmt.Errorf("invalid clip identity or range")
	}
	if !safeObjectKey(c.Object.Key) || !validObjectIdentity(c.Object.ETag, c.Object.VersionID) || c.Object.SizeBytes <= 0 || !lowerHex64(c.Object.SHA256) {
		return fmt.Errorf("invalid exact R2 identity")
	}
	if c.AudioContract != nil && validateAudioContract(*c.AudioContract) != nil {
		return fmt.Errorf("invalid frozen audio sequence contract")
	}
	return nil
}

func validObjectIdentity(etag, versionID string) bool {
	if len(etag) == 0 || len(etag) > 256 || strings.HasPrefix(etag, "W/") || strings.ContainsRune(etag, '"') || !visibleASCII(etag) {
		return false
	}
	return versionID == "" || (len(versionID) <= 1024 && visibleASCII(versionID))
}

func visibleASCII(value string) bool {
	for i := 0; i < len(value); i++ {
		if value[i] < 0x21 || value[i] > 0x7e {
			return false
		}
	}
	return value != ""
}

func safeObjectKey(key string) bool {
	return key != "" && key != "." && key != ".." && !strings.HasPrefix(key, "/") && path.Clean(key) == key && !strings.Contains(key, "\\") && !strings.ContainsRune(key, rune(0))
}

func lowerHex64(raw string) bool {
	if len(raw) != 64 || raw != strings.ToLower(raw) {
		return false
	}
	_, err := hex.DecodeString(raw)
	return err == nil
}
