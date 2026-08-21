package joinedrecording

import (
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/stitchcert"
)

const HourManifestSchemaVersion = 1

type HourManifestStatus string

const (
	HourStatusMedia          HourManifestStatus = "media"
	HourStatusGapOnly        HourManifestStatus = "gap_only"
	HourStatusQuarantineOnly HourManifestStatus = "quarantine_only"
)

// HourManifestAllocation is the exact allocation evidence needed to map this
// hour back to the source-only recording and stream-day ledgers.
type HourManifestAllocation struct {
	ArtifactID         int64               `json:"artifact_id"`
	RelativePath       string              `json:"relative_path"`
	ObjectKey          string              `json:"object_key"`
	SizeBytes          int64               `json:"size_bytes"`
	SHA256             string              `json:"sha256"`
	LedgerSHA256       string              `json:"ledger_sha256"`
	HourSourceSHA256   string              `json:"hour_source_claim_sha256"`
	Boundaries         []CrossHourBoundary `json:"boundaries"`
	CrossDayBoundaries []CrossDayBoundary  `json:"cross_day_boundaries"`
}

// QuarantineEvidence records a typed terminal failure without pretending that
// the affected sources produced valid media.
type QuarantineEvidence struct {
	ReasonCode        string          `json:"reason_code"`
	SourceClipIDs     []int64         `json:"source_clip_ids"`
	SourceClaimSHA256 string          `json:"source_claim_sha256"`
	PolicyVersion     string          `json:"policy_version"`
	NormalizedFacts   json.RawMessage `json:"normalized_failure_facts"`
	FailureSHA256     string          `json:"failure_sha256"`
	EvidenceSHA256    string          `json:"evidence_sha256"`
	AttemptCount      int             `json:"isolated_attempt_count"`
	MediaToolIdentity string          `json:"media_tool_identity"`
}

type ScheduledGapEvidence struct {
	ReasonCode           string `json:"reason_code"`
	SignedGapNanoseconds int64  `json:"signed_gap_nanoseconds"`
	NoAllocatableSources bool   `json:"no_allocatable_sources"`
}

type SourceDisposition struct {
	ClipID          int64  `json:"clip_id"`
	Disposition     string `json:"disposition"`
	MediaArtifactID int64  `json:"media_artifact_id"`
	MediaOrdinal    int    `json:"media_ordinal"`
	ReasonCode      string `json:"reason_code"`
}

// HourManifestMedia is deliberately byte-bound and contains no R2 ETag. The
// database inserts media rows first and supplies their IDs to this serializer
// in the same fenced seal transaction.
type HourManifestMedia struct {
	ArtifactID         int64                `json:"artifact_id"`
	Ordinal            int                  `json:"ordinal"`
	Part               int                  `json:"part"`
	Parts              int                  `json:"parts"`
	RelativePath       string               `json:"relative_path"`
	ObjectKey          string               `json:"object_key"`
	ContentID          string               `json:"content_id"`
	SizeBytes          int64                `json:"size_bytes"`
	SHA256             string               `json:"sha256"`
	ActualStartUTC     time.Time            `json:"actual_start_utc"`
	ActualEndUTC       time.Time            `json:"actual_end_utc"`
	UTCOffsetSeconds   int                  `json:"utc_offset_seconds"`
	MediaToolIdentity  string               `json:"media_tool_identity"`
	SourceClipIDs      []int64              `json:"source_clip_ids"`
	Verification       Verification         `json:"verification"`
	MaximalityEvidence []MaximalityEvidence `json:"maximality_evidence"`
}

// HourManifest is the one canonical cloud, database, feed, and NAS contract.
// Do not add a second compact schema or per-media sidecar.
type HourManifest struct {
	SchemaVersion        int                    `json:"schema_version"`
	PolicyVersion        string                 `json:"policy_version"`
	Status               HourManifestStatus     `json:"status"`
	BatchID              string                 `json:"batch_id"`
	HourID               string                 `json:"hour_id"`
	RecordingID          int64                  `json:"recording_id"`
	Timezone             string                 `json:"timezone"`
	LocalDate            string                 `json:"local_date"`
	DeliveryHour         int                    `json:"delivery_hour"`
	ClockHour            int                    `json:"clock_hour"`
	ScheduledStartUTC    time.Time              `json:"scheduled_start_utc"`
	ScheduledEndUTC      time.Time              `json:"scheduled_end_utc"`
	QualificationDay     QualifiedDay           `json:"qualification_day"`
	QualificationSHA256  string                 `json:"qualification_sha256"`
	Allocation           HourManifestAllocation `json:"allocation"`
	MediaTool            MediaToolEvidence      `json:"media_tool"`
	SourceClaimSHA256    string                 `json:"source_claim_sha256"`
	SourceCount          int                    `json:"source_count"`
	Sources              []SourceClip           `json:"sources"`
	SourceDispositions   []SourceDisposition    `json:"source_dispositions"`
	Gaps                 []Gap                  `json:"gaps"`
	ScheduledGap         *ScheduledGapEvidence  `json:"scheduled_gap"`
	QuarantineReasonCode string                 `json:"quarantine_reason_code"`
	QuarantineEvidence   []QuarantineEvidence   `json:"quarantine_evidence"`
	Media                []HourManifestMedia    `json:"media"`
}

type HourManifestInput struct {
	Plan               BatchPlan
	Allocation         HourManifestAllocation
	MediaArtifactIDs   []int64
	Built              []BuiltOutput
	QuarantineEvidence []QuarantineEvidence
	AllocationLedger   StreamDayAllocation
}

// BuildHourManifestAllocation constructs the exact ledger artifact and
// boundary subset bound into one canonical hour manifest.
func BuildHourManifestAllocation(artifactID int64, plan BatchPlan, ledger StreamDayAllocation) (HourManifestAllocation, error) {
	if err := ValidatePlan(plan); err != nil {
		return HourManifestAllocation{}, err
	}
	relativePath, objectKey, pathErr := CanonicalAllocationLedgerPaths(plan.BatchID, plan.RecordingID, plan.LocalDate)
	ledgerBytes, artifactSHA, ledgerErr := CanonicalAllocationLedgerArtifact(ledger)
	wantQualificationDay, qualificationOK := qualifiedDay(plan.Qualification, plan.LocalDate)
	if pathErr != nil || ledgerErr != nil || !qualificationOK || artifactID <= 0 || ledger.BatchID != plan.BatchID || ledger.Generation != plan.Generation || ledger.RecordingID != plan.RecordingID || ledger.Timezone != plan.Timezone || ledger.LocalDate != plan.LocalDate || ledger.QualificationSHA != plan.Qualification.EvidenceSHA || !sameCanonical([]QualifiedDay{ledger.QualificationDay}, []QualifiedDay{wantQualificationDay}) || ledger.LedgerSHA256 != plan.AllocationLedgerSHA || ledger.HourSourceSHA256[plan.LocalHour-1] != plan.SourceClaimSHA256 || !sameClipIDs(ledger.Hours[plan.LocalHour-1].SourceClipIDs, plan.Sources) {
		return HourManifestAllocation{}, fmt.Errorf("hour manifest allocation evidence differs")
	}
	boundaries, crossDayBoundaries := hourBoundarySubset(ledger, plan.LocalHour)
	return HourManifestAllocation{
		ArtifactID:         artifactID,
		RelativePath:       relativePath,
		ObjectKey:          objectKey,
		SizeBytes:          int64(len(ledgerBytes)),
		SHA256:             artifactSHA,
		LedgerSHA256:       ledger.LedgerSHA256,
		HourSourceSHA256:   plan.SourceClaimSHA256,
		Boundaries:         boundaries,
		CrossDayBoundaries: crossDayBoundaries,
	}, nil
}

func ValidateHourManifestAllocation(allocation HourManifestAllocation, plan BatchPlan, ledger StreamDayAllocation) error {
	want, err := BuildHourManifestAllocation(allocation.ArtifactID, plan, ledger)
	if err != nil || !sameCanonical([]HourManifestAllocation{allocation}, []HourManifestAllocation{want}) {
		return fmt.Errorf("hour manifest allocation evidence differs")
	}
	return nil
}

// BuildHourManifest runs only after the fenced seal transaction has obtained
// media artifact IDs. It returns the exact bytes, size, and full SHA used to
// insert the manifest artifact row before the hour is atomically sealed.
func BuildHourManifest(input HourManifestInput) (HourManifest, []byte, string, error) {
	plan := input.Plan
	if err := ValidatePlan(plan); err != nil {
		return HourManifest{}, nil, "", err
	}
	if err := ValidateHourManifestAllocation(input.Allocation, plan, input.AllocationLedger); err != nil {
		return HourManifest{}, nil, "", err
	}
	status := HourStatusMedia
	if plan.GapOnly {
		status = HourStatusGapOnly
	} else if plan.QuarantineReason != "" {
		status = HourStatusQuarantineOnly
	}
	if status == HourStatusMedia {
		if len(input.MediaArtifactIDs) != len(plan.Outputs) || len(input.Built) != len(plan.Outputs) {
			return HourManifest{}, nil, "", fmt.Errorf("media manifest requires every sealed artifact")
		}
	} else if len(input.MediaArtifactIDs) != 0 || len(input.Built) != 0 {
		return HourManifest{}, nil, "", fmt.Errorf("non-media manifest cannot contain media artifacts")
	}
	if (len(plan.QuarantinedSources) > 0) != (len(input.QuarantineEvidence) > 0) {
		return HourManifest{}, nil, "", fmt.Errorf("every quarantined source requires typed failure evidence")
	}
	loc, err := time.LoadLocation(plan.Timezone)
	if err != nil {
		return HourManifest{}, nil, "", err
	}
	day, err := time.ParseInLocation("2006-01-02", plan.LocalDate, loc)
	if err != nil {
		return HourManifest{}, nil, "", err
	}
	clockHour := plan.LocalHour + 7
	scheduledStart := time.Date(day.Year(), day.Month(), day.Day(), clockHour, 0, 0, 0, loc).UTC()
	scheduledEnd := scheduledStart.Add(time.Hour)
	media := make([]HourManifestMedia, len(plan.Outputs))
	seenArtifactIDs := map[int64]bool{}
	included := map[int64]SourceDisposition{}
	for i, output := range plan.Outputs {
		artifactID, built := input.MediaArtifactIDs[i], input.Built[i]
		if artifactID <= 0 || seenArtifactIDs[artifactID] || validatePassedVerification(built.Verification) != nil || built.SourceCount != len(output.Sources) || built.SizeBytes != output.ExpectedSize || built.SHA256 != output.ExpectedSHA {
			return HourManifest{}, nil, "", fmt.Errorf("hour media artifact %d differs from sealed plan", i+1)
		}
		seenArtifactIDs[artifactID] = true
		clipIDs := make([]int64, len(output.Sources))
		for j, source := range output.Sources {
			clipIDs[j] = source.ClipID
			included[source.ClipID] = SourceDisposition{ClipID: source.ClipID, Disposition: "included", MediaArtifactID: artifactID, MediaOrdinal: output.Ordinal}
		}
		for _, evidence := range built.SplitEvidence {
			candidateSources, sourceErr := sourceSubsetByIDs(plan.Sources, evidence.CandidateClipIDs)
			expectedClaim, claimErr := candidateSourceClaimSHA(candidateSources)
			if sourceErr != nil || claimErr != nil || validateMaximalityEvidence(evidence, plan.MediaTool.IdentitySHA256, expectedClaim) != nil {
				return HourManifest{}, nil, "", fmt.Errorf("invalid maximality evidence")
			}
		}
		if len(built.SplitEvidence) > 0 {
			adjacent := built.SplitEvidence[len(built.SplitEvidence)-1]
			if len(adjacent.CandidateClipIDs) != len(output.Sources)+1 {
				return HourManifest{}, nil, "", fmt.Errorf("maximality evidence does not bind one adjacent extension")
			}
			for sourceIndex := range output.Sources {
				if adjacent.CandidateClipIDs[sourceIndex] != output.Sources[sourceIndex].ClipID {
					return HourManifest{}, nil, "", fmt.Errorf("maximality evidence source order differs")
				}
			}
			lastPosition := -1
			for sourceIndex := range plan.Sources {
				if plan.Sources[sourceIndex].ClipID == output.Sources[len(output.Sources)-1].ClipID {
					lastPosition = sourceIndex
					break
				}
			}
			if lastPosition < 0 || lastPosition+1 >= len(plan.Sources) || adjacent.CandidateClipIDs[len(adjacent.CandidateClipIDs)-1] != plan.Sources[lastPosition+1].ClipID {
				return HourManifest{}, nil, "", fmt.Errorf("maximality evidence is not the immediate source extension")
			}
		}
		media[i] = HourManifestMedia{ArtifactID: artifactID, Ordinal: output.Ordinal, Part: output.Part, Parts: output.Parts, RelativePath: output.RelativePath, ObjectKey: output.ObjectKey, ContentID: output.ContentID, SizeBytes: output.ExpectedSize, SHA256: output.ExpectedSHA, ActualStartUTC: output.ActualStart, ActualEndUTC: output.ActualEnd, UTCOffsetSeconds: output.UTCOffsetSec, MediaToolIdentity: output.MediaToolID, SourceClipIDs: clipIDs, Verification: built.Verification, MaximalityEvidence: append([]MaximalityEvidence{}, built.SplitEvidence...)}
	}
	quarantined := map[int64]SourceDisposition{}
	for _, evidence := range input.QuarantineEvidence {
		quarantineSources, sourceErr := sourceSubsetByIDs(plan.Sources, evidence.SourceClipIDs)
		expectedClaim, claimErr := candidateSourceClaimSHA(quarantineSources)
		if sourceErr != nil || claimErr != nil || validateQuarantineEvidence(evidence, plan.MediaTool.IdentitySHA256, expectedClaim) != nil {
			return HourManifest{}, nil, "", fmt.Errorf("invalid quarantine evidence")
		}
		for _, clipID := range evidence.SourceClipIDs {
			if _, duplicate := quarantined[clipID]; duplicate {
				return HourManifest{}, nil, "", fmt.Errorf("quarantine evidence overlaps")
			}
			quarantined[clipID] = SourceDisposition{ClipID: clipID, Disposition: "quarantined", ReasonCode: evidence.ReasonCode}
		}
	}
	dispositions := make([]SourceDisposition, len(plan.Sources))
	for i, source := range plan.Sources {
		disposition, ok := included[source.ClipID]
		if !ok {
			disposition, ok = quarantined[source.ClipID]
		}
		if !ok {
			return HourManifest{}, nil, "", fmt.Errorf("accounted source lacks exactly one disposition")
		}
		dispositions[i] = disposition
	}
	if len(included)+len(quarantined) != len(plan.Sources) {
		return HourManifest{}, nil, "", fmt.Errorf("source disposition differs from frozen claim")
	}
	qualificationDay, ok := qualifiedDay(plan.Qualification, plan.LocalDate)
	if !ok {
		return HourManifest{}, nil, "", fmt.Errorf("hour lacks corresponding qualification day")
	}
	var scheduledGap *ScheduledGapEvidence
	if status == HourStatusGapOnly {
		scheduledGap = &ScheduledGapEvidence{ReasonCode: plan.GapOnlyReason, SignedGapNanoseconds: scheduledEnd.Sub(scheduledStart).Nanoseconds(), NoAllocatableSources: true}
	}
	manifest := HourManifest{SchemaVersion: HourManifestSchemaVersion, PolicyVersion: PlanPolicyVersion, Status: status, BatchID: plan.BatchID, HourID: plan.HourID, RecordingID: plan.RecordingID, Timezone: plan.Timezone, LocalDate: plan.LocalDate, DeliveryHour: plan.LocalHour, ClockHour: clockHour, ScheduledStartUTC: scheduledStart, ScheduledEndUTC: scheduledEnd, QualificationDay: qualificationDay, QualificationSHA256: plan.Qualification.EvidenceSHA, Allocation: input.Allocation, MediaTool: plan.MediaTool, SourceClaimSHA256: plan.SourceClaimSHA256, SourceCount: len(plan.Sources), Sources: append([]SourceClip{}, plan.Sources...), SourceDispositions: dispositions, Gaps: append([]Gap{}, plan.Gaps...), ScheduledGap: scheduledGap, QuarantineReasonCode: plan.QuarantineReason, QuarantineEvidence: append([]QuarantineEvidence{}, input.QuarantineEvidence...), Media: media}
	canonical, sha, err := CanonicalHourManifestArtifact(manifest)
	return manifest, canonical, sha, err
}

// CanonicalHourManifestArtifact validates the independently stored canonical
// artifact used to derive final-index hour references.
func CanonicalHourManifestArtifact(manifest HourManifest) ([]byte, string, error) {
	hourPrefix := fmt.Sprintf("%s__recording-%d__date-%s__hour-%02d__generation-", manifest.BatchID, manifest.RecordingID, manifest.LocalDate, manifest.DeliveryHour)
	generation, generationErr := strconv.Atoi(strings.TrimPrefix(manifest.HourID, hourPrefix))
	hourID, hourErr := CanonicalHourID(manifest.BatchID, manifest.RecordingID, manifest.LocalDate, manifest.DeliveryHour, generation)
	loc, timezoneErr := time.LoadLocation(manifest.Timezone)
	if timezoneErr != nil {
		return nil, "", fmt.Errorf("canonical hour manifest timezone differs")
	}
	day, dateErr := time.ParseInLocation("2006-01-02", manifest.LocalDate, loc)
	if generationErr != nil || hourErr != nil || dateErr != nil || manifest.SchemaVersion != HourManifestSchemaVersion || manifest.PolicyVersion != PlanPolicyVersion || manifest.HourID != hourID || manifest.ClockHour != manifest.DeliveryHour+7 || ValidateMediaToolEvidence(manifest.MediaTool) != nil || !lowerHex64(manifest.QualificationSHA256) || validateQualifiedLedgerDay(manifest.QualificationDay, manifest.RecordingID, manifest.Timezone, manifest.LocalDate) != nil {
		return nil, "", fmt.Errorf("canonical hour manifest identity differs")
	}
	scheduledStart := time.Date(day.Year(), day.Month(), day.Day(), manifest.ClockHour, 0, 0, 0, loc).UTC()
	if !manifest.ScheduledStartUTC.Equal(scheduledStart) || !manifest.ScheduledEndUTC.Equal(scheduledStart.Add(time.Hour)) || manifest.SourceCount != len(manifest.Sources) {
		return nil, "", fmt.Errorf("canonical hour manifest schedule differs")
	}
	ledgerRelative, ledgerObject, ledgerPathErr := CanonicalAllocationLedgerPaths(manifest.BatchID, manifest.RecordingID, manifest.LocalDate)
	if ledgerPathErr != nil || manifest.Allocation.ArtifactID <= 0 || manifest.Allocation.RelativePath != ledgerRelative || manifest.Allocation.ObjectKey != ledgerObject || manifest.Allocation.SizeBytes <= 0 || !lowerHex64(manifest.Allocation.SHA256) || !lowerHex64(manifest.Allocation.LedgerSHA256) || manifest.Allocation.HourSourceSHA256 != manifest.SourceClaimSHA256 {
		return nil, "", fmt.Errorf("canonical hour manifest allocation differs")
	}
	claimSHA, _, claimErr := CanonicalSourceClaim(manifest.Sources)
	seenSources := make(map[int64]SourceClip, len(manifest.Sources))
	sourcePositions := make(map[int64]int, len(manifest.Sources))
	seenStorage := make(map[string]bool, len(manifest.Sources))
	var sourceBytes int64
	for i, source := range manifest.Sources {
		storageKey := sourceStorageKey(source)
		if validatePreflightSource(source, manifest.RecordingID) != nil || seenSources[source.ClipID].ClipID != 0 || seenStorage[storageKey] || source.Object.SizeBytes > math.MaxInt64-sourceBytes || (i > 0 && (source.StartUTC.Before(manifest.Sources[i-1].StartUTC) || (source.StartUTC.Equal(manifest.Sources[i-1].StartUTC) && source.ClipID <= manifest.Sources[i-1].ClipID))) {
			return nil, "", fmt.Errorf("canonical hour manifest source order differs")
		}
		seenSources[source.ClipID] = source
		sourcePositions[source.ClipID] = i
		seenStorage[storageKey] = true
		sourceBytes += source.Object.SizeBytes
	}
	if claimErr != nil || claimSHA != manifest.SourceClaimSHA256 || len(manifest.SourceDispositions) != len(manifest.Sources) {
		return nil, "", fmt.Errorf("canonical hour manifest source claim differs")
	}
	included := map[int64]SourceDisposition{}
	quarantined := map[int64]SourceDisposition{}
	seenArtifacts := map[int64]bool{}
	splitPairs := make(map[[2]int64]bool, len(manifest.Gaps))
	for _, gap := range manifest.Gaps {
		splitPairs[[2]int64{gap.PreviousClipID, gap.NextClipID}] = true
	}
	lastMediaSourcePosition := -1
	for i, media := range manifest.Media {
		_, utcOffset := media.ActualStartUTC.In(loc).Zone()
		if media.ArtifactID <= 0 || seenArtifacts[media.ArtifactID] || media.Ordinal != i+1 || media.Part != i+1 || media.Parts != len(manifest.Media) || media.ContentID != media.SHA256 || !lowerHex64(media.SHA256) || media.SizeBytes <= 0 || media.ObjectKey != path.Join("joined", manifest.BatchID, "objects", media.ContentID+".mp4") || len(media.SourceClipIDs) == 0 || !media.ActualEndUTC.After(media.ActualStartUTC) || media.UTCOffsetSeconds != utcOffset || media.MediaToolIdentity != manifest.MediaTool.IdentitySHA256 || validatePassedVerification(media.Verification) != nil {
			return nil, "", fmt.Errorf("canonical hour manifest media differs")
		}
		firstPosition := sourcePositions[media.SourceClipIDs[0]]
		if seenSources[media.SourceClipIDs[0]].ClipID == 0 || firstPosition <= lastMediaSourcePosition {
			return nil, "", fmt.Errorf("canonical hour manifest media source order differs")
		}
		previousPosition := firstPosition - 1
		for sourceIndex, clipID := range media.SourceClipIDs {
			position, ok := sourcePositions[clipID]
			if !ok || position != previousPosition+1 {
				return nil, "", fmt.Errorf("canonical hour manifest media sources are not consecutive")
			}
			if sourceIndex > 0 && splitPairs[[2]int64{media.SourceClipIDs[sourceIndex-1], clipID}] {
				return nil, "", fmt.Errorf("canonical hour manifest media crosses a recorded gap")
			}
			previousPosition = position
		}
		lastPosition := sourcePositions[media.SourceClipIDs[len(media.SourceClipIDs)-1]]
		if !media.ActualStartUTC.Equal(manifest.Sources[firstPosition].StartUTC) || !media.ActualEndUTC.Equal(manifest.Sources[lastPosition].EndUTC) {
			return nil, "", fmt.Errorf("canonical hour manifest media range differs from its sources")
		}
		for _, evidence := range media.MaximalityEvidence {
			candidateSources, sourceErr := sourceSubsetByIDs(manifest.Sources, evidence.CandidateClipIDs)
			expectedClaim, claimErr := candidateSourceClaimSHA(candidateSources)
			if sourceErr != nil || claimErr != nil || validateMaximalityEvidence(evidence, manifest.MediaTool.IdentitySHA256, expectedClaim) != nil {
				return nil, "", fmt.Errorf("canonical hour manifest maximality evidence differs")
			}
		}
		if len(media.MaximalityEvidence) > 0 {
			adjacent := media.MaximalityEvidence[len(media.MaximalityEvidence)-1]
			if len(adjacent.CandidateClipIDs) != len(media.SourceClipIDs)+1 {
				return nil, "", fmt.Errorf("canonical hour manifest maximality extension differs")
			}
			for sourceIndex, clipID := range media.SourceClipIDs {
				if adjacent.CandidateClipIDs[sourceIndex] != clipID {
					return nil, "", fmt.Errorf("canonical hour manifest maximality source order differs")
				}
			}
			if lastPosition+1 >= len(manifest.Sources) || adjacent.CandidateClipIDs[len(adjacent.CandidateClipIDs)-1] != manifest.Sources[lastPosition+1].ClipID {
				return nil, "", fmt.Errorf("canonical hour manifest maximality is not the immediate source extension")
			}
		}
		lastMediaSourcePosition = lastPosition
		seenArtifacts[media.ArtifactID] = true
		for _, clipID := range media.SourceClipIDs {
			if seenSources[clipID].ClipID == 0 || included[clipID].ClipID != 0 {
				return nil, "", fmt.Errorf("canonical hour manifest media source differs")
			}
			included[clipID] = SourceDisposition{ClipID: clipID, Disposition: "included", MediaArtifactID: media.ArtifactID, MediaOrdinal: media.Ordinal}
		}
	}
	for _, evidence := range manifest.QuarantineEvidence {
		sources, err := sourceSubsetByIDs(manifest.Sources, evidence.SourceClipIDs)
		expectedClaim, claimErr := candidateSourceClaimSHA(sources)
		if err != nil || claimErr != nil || validateQuarantineEvidence(evidence, manifest.MediaTool.IdentitySHA256, expectedClaim) != nil {
			return nil, "", fmt.Errorf("canonical hour manifest quarantine differs")
		}
		for _, clipID := range evidence.SourceClipIDs {
			if included[clipID].ClipID != 0 || quarantined[clipID].ClipID != 0 {
				return nil, "", fmt.Errorf("canonical hour manifest quarantine overlaps")
			}
			quarantined[clipID] = SourceDisposition{ClipID: clipID, Disposition: "quarantined", ReasonCode: evidence.ReasonCode}
		}
	}
	for i, source := range manifest.Sources {
		want := included[source.ClipID]
		if want.ClipID == 0 {
			want = quarantined[source.ClipID]
		}
		if want.ClipID == 0 || !sameCanonical([]SourceDisposition{manifest.SourceDispositions[i]}, []SourceDisposition{want}) {
			return nil, "", fmt.Errorf("canonical hour manifest disposition differs")
		}
	}
	validatedGaps := make(map[[2]int64]bool, len(manifest.Gaps))
	for _, gap := range manifest.Gaps {
		previousPosition, previousOK := sourcePositions[gap.PreviousClipID]
		nextPosition, nextOK := sourcePositions[gap.NextClipID]
		pair := [2]int64{gap.PreviousClipID, gap.NextClipID}
		if !previousOK || !nextOK || nextPosition != previousPosition+1 || validatedGaps[pair] || !reasonCode.MatchString(gap.Reason) || !gap.AtUTC.Equal(manifest.Sources[previousPosition].EndUTC) || gap.SignedGapNanoseconds != manifest.Sources[nextPosition].StartUTC.Sub(manifest.Sources[previousPosition].EndUTC).Nanoseconds() {
			return nil, "", fmt.Errorf("canonical hour manifest gap differs")
		}
		validatedGaps[pair] = true
	}
	brokenAdjacencies := 0
	for i := 1; i < len(manifest.Sources); i++ {
		previousID, nextID := manifest.Sources[i-1].ClipID, manifest.Sources[i].ClipID
		previousMedia, nextMedia := included[previousID].MediaArtifactID, included[nextID].MediaArtifactID
		sameMediaRun := previousMedia > 0 && previousMedia == nextMedia
		hasGap := validatedGaps[[2]int64{previousID, nextID}]
		if sameMediaRun == hasGap {
			return nil, "", fmt.Errorf("canonical hour manifest run boundary differs")
		}
		if !sameMediaRun {
			brokenAdjacencies++
		}
	}
	if brokenAdjacencies != len(manifest.Gaps) {
		return nil, "", fmt.Errorf("canonical hour manifest gaps do not exactly cover run boundaries")
	}
	switch manifest.Status {
	case HourStatusMedia:
		if len(manifest.Sources) == 0 || len(manifest.Media) == 0 || manifest.ScheduledGap != nil || manifest.QuarantineReasonCode != "" {
			return nil, "", fmt.Errorf("canonical media hour differs")
		}
	case HourStatusGapOnly:
		if len(manifest.Sources) != 0 || len(manifest.Media) != 0 || len(manifest.SourceDispositions) != 0 || manifest.ScheduledGap == nil || !manifest.ScheduledGap.NoAllocatableSources || !reasonCode.MatchString(manifest.ScheduledGap.ReasonCode) || manifest.ScheduledGap.SignedGapNanoseconds != time.Hour.Nanoseconds() || manifest.QuarantineReasonCode != "" || len(manifest.QuarantineEvidence) != 0 {
			return nil, "", fmt.Errorf("canonical gap-only hour differs")
		}
	case HourStatusQuarantineOnly:
		if len(manifest.Sources) == 0 || len(manifest.Media) != 0 || len(quarantined) != len(manifest.Sources) || !reasonCode.MatchString(manifest.QuarantineReasonCode) || manifest.ScheduledGap != nil {
			return nil, "", fmt.Errorf("canonical quarantine-only hour differs")
		}
	default:
		return nil, "", fmt.Errorf("canonical hour manifest status differs")
	}
	sha, canonical, err := stitchcert.CanonicalSHA(manifest)
	if err != nil || len(canonical) > MaxCanonicalJSONBytes {
		return nil, "", fmt.Errorf("hour manifest exceeds canonical limit")
	}
	return canonical, sha, nil
}

// ValidateHourManifestLedgerBinding proves that one canonical hour is exactly
// the corresponding ordered source subset and allocation evidence from its
// canonical stream-day ledger.
func ValidateHourManifestLedgerBinding(manifest HourManifest, ledgerRef AllocationLedgerRef, ledger StreamDayAllocation) error {
	if _, _, err := CanonicalHourManifestArtifact(manifest); err != nil {
		return err
	}
	if ValidateAllocationLedgerRef(ledgerRef, ledger) != nil || manifest.BatchID != ledger.BatchID || manifest.RecordingID != ledger.RecordingID || manifest.Timezone != ledger.Timezone || manifest.LocalDate != ledger.LocalDate || manifest.DeliveryHour < 1 || manifest.DeliveryHour > 12 || manifest.QualificationSHA256 != ledger.QualificationSHA || !sameCanonical([]QualifiedDay{manifest.QualificationDay}, []QualifiedDay{ledger.QualificationDay}) {
		return fmt.Errorf("hour manifest belongs to a different allocation ledger")
	}
	hour := ledger.Hours[manifest.DeliveryHour-1]
	boundaries, crossDayBoundaries := hourBoundarySubset(ledger, manifest.DeliveryHour)
	wantAllocation := HourManifestAllocation{
		ArtifactID:         ledgerRef.ArtifactID,
		RelativePath:       ledgerRef.RelativePath,
		ObjectKey:          ledgerRef.ObjectKey,
		SizeBytes:          ledgerRef.SizeBytes,
		SHA256:             ledgerRef.SHA256,
		LedgerSHA256:       ledgerRef.LedgerSHA256,
		HourSourceSHA256:   ledger.HourSourceSHA256[manifest.DeliveryHour-1],
		Boundaries:         boundaries,
		CrossDayBoundaries: crossDayBoundaries,
	}
	if !sameCanonical([]HourManifestAllocation{manifest.Allocation}, []HourManifestAllocation{wantAllocation}) || manifest.SourceClaimSHA256 != ledger.HourSourceSHA256[manifest.DeliveryHour-1] || len(manifest.Sources) != len(hour.SourceClipIDs) {
		return fmt.Errorf("hour manifest allocation differs from canonical ledger")
	}
	ledgerSources := make(map[int64]SourceClip, len(ledger.Sources))
	hourPositions := make(map[int64]int, len(hour.SourceClipIDs))
	for _, source := range ledger.Sources {
		ledgerSources[source.ClipID] = source
	}
	expectedSources := make([]SourceClip, len(hour.SourceClipIDs))
	for i, clipID := range hour.SourceClipIDs {
		source, ok := ledgerSources[clipID]
		if !ok || manifest.Sources[i].ClipID != clipID {
			return fmt.Errorf("hour manifest source order differs from canonical ledger")
		}
		expectedSources[i] = source
		hourPositions[clipID] = i
	}
	if !sameCanonical(sourceOnlyClips(manifest.Sources), sourceOnlyClips(expectedSources)) {
		return fmt.Errorf("hour manifest source identity differs from canonical ledger")
	}
	seenGaps := map[[2]int64]bool{}
	for _, gap := range manifest.Gaps {
		pair := [2]int64{gap.PreviousClipID, gap.NextClipID}
		previous, previousOK := ledgerSources[gap.PreviousClipID]
		next, nextOK := ledgerSources[gap.NextClipID]
		if !previousOK || !nextOK || hourPositions[gap.NextClipID] != hourPositions[gap.PreviousClipID]+1 || seenGaps[pair] || !reasonCode.MatchString(gap.Reason) || !gap.AtUTC.Equal(previous.EndUTC) || gap.SignedGapNanoseconds != next.StartUTC.Sub(previous.EndUTC).Nanoseconds() {
			return fmt.Errorf("hour manifest gap differs from canonical source timing")
		}
		seenGaps[pair] = true
	}
	return nil
}

func validateMaximalityEvidence(e MaximalityEvidence, toolIdentity, expectedSourceClaim string) error {
	minimumRepeats := 2
	if e.ReasonCode == "output_exceeds_put_cap" {
		minimumRepeats = 1
	}
	failureSHA, canonicalFacts, err := stitchcert.CanonicalSHA(e.FailureFacts)
	proof := struct {
		SourceClaimSHA256 string `json:"source_claim_sha256"`
		ReasonCode        string `json:"reason_code"`
		FailureSHA256     string `json:"failure_sha256"`
		PolicyVersion     string `json:"policy_version"`
		MediaToolIdentity string `json:"media_tool_identity"`
		RepeatCount       int    `json:"repeat_count"`
	}{e.SourceClaimSHA256, e.ReasonCode, e.FailureSHA256, e.PolicyVersion, e.MediaToolIdentity, e.RepeatCount}
	evidenceSHA, _, proofErr := stitchcert.CanonicalSHA(proof)
	if err != nil || proofErr != nil || len(e.CandidateClipIDs) == 0 || !reasonCode.MatchString(e.ReasonCode) || e.SourceClaimSHA256 != expectedSourceClaim || e.PolicyVersion != PlanPolicyVersion || len(canonicalFacts) == 0 || failureSHA != e.FailureSHA256 || evidenceSHA != e.EvidenceSHA256 || e.RepeatCount != minimumRepeats || e.MediaToolIdentity != toolIdentity {
		return fmt.Errorf("invalid maximality evidence")
	}
	return nil
}

func validateQuarantineEvidence(e QuarantineEvidence, toolIdentity, expectedSourceClaim string) error {
	failureSHA, canonicalFacts, err := stitchcert.CanonicalSHA(e.NormalizedFacts)
	proof := struct {
		SourceClaimSHA256 string `json:"source_claim_sha256"`
		ReasonCode        string `json:"reason_code"`
		FailureSHA256     string `json:"failure_sha256"`
		PolicyVersion     string `json:"policy_version"`
		MediaToolIdentity string `json:"media_tool_identity"`
		RepeatCount       int    `json:"repeat_count"`
	}{e.SourceClaimSHA256, e.ReasonCode, e.FailureSHA256, e.PolicyVersion, e.MediaToolIdentity, e.AttemptCount}
	evidenceSHA, _, proofErr := stitchcert.CanonicalSHA(proof)
	if err != nil || proofErr != nil || !reasonCode.MatchString(e.ReasonCode) || len(e.SourceClipIDs) == 0 || e.SourceClaimSHA256 != expectedSourceClaim || e.PolicyVersion != PlanPolicyVersion || len(canonicalFacts) == 0 || failureSHA != e.FailureSHA256 || evidenceSHA != e.EvidenceSHA256 || e.AttemptCount != 2 || e.MediaToolIdentity != toolIdentity {
		return fmt.Errorf("invalid quarantine evidence")
	}
	return nil
}

func sourceSubsetByIDs(accounted []SourceClip, ids []int64) ([]SourceClip, error) {
	byID := make(map[int64]SourceClip, len(accounted))
	for _, source := range accounted {
		byID[source.ClipID] = source
	}
	out := make([]SourceClip, len(ids))
	seen := make(map[int64]bool, len(ids))
	for i, id := range ids {
		if seen[id] {
			return nil, fmt.Errorf("evidence source is duplicated")
		}
		seen[id] = true
		source, ok := byID[id]
		if !ok {
			return nil, fmt.Errorf("evidence source is outside accounted claim")
		}
		out[i] = source
	}
	return out, nil
}

func validatePassedVerification(v Verification) error {
	if v.Status != "passed" || v.PacketPayloadOrderStatus != "passed" || v.DecodedFrameTotalsStatus != "passed" || v.DecodedAudioTotalsStatus != "passed" || v.OutputTimestampStatus != "passed" || v.StrictDecodeStatus != "passed" || validateFingerprint(v.SourceFingerprint, false) != nil || validateFingerprint(v.OutputFingerprint, true) != nil || compareFingerprints(v.SourceFingerprint, v.OutputFingerprint) != nil {
		return fmt.Errorf("complete media verification did not pass")
	}
	return nil
}

func validateFingerprint(f MediaFingerprint, output bool) error {
	if f.DurationSeconds <= 0 || len(f.Tracks) == 0 || len(f.Tracks) > 2 || f.Tracks["video"] == nil {
		return fmt.Errorf("empty media fingerprint")
	}
	for mediaType, track := range f.Tracks {
		if track == nil || track.MediaType != mediaType || (mediaType != "video" && mediaType != "audio") || track.PacketCount <= 0 || !lowerHex64(track.PacketChainSHA256) || !lowerHex64(track.PacketTimingSHA256) || len(track.PacketTimeBases) == 0 || !validRational(track.FirstPacketPTSSeconds, false) || !validRational(track.LastPacketPTSSeconds, false) || !validRational(track.FirstPacketDTSSeconds, false) || !validRational(track.LastPacketDTSSeconds, false) || !validRational(track.PacketDurationSeconds, true) || !validRational(track.DecodeTimelineSpanSeconds, true) || (output && track.DecodeTimelineSpanSeconds != track.PacketDurationSeconds) || track.DecodedFrames <= 0 || (mediaType == "audio" && track.DecodedSamples <= 0) {
			return fmt.Errorf("invalid media track fingerprint")
		}
		for _, timeBase := range track.PacketTimeBases {
			if _, _, err := parseTimeBase(timeBase); err != nil {
				return fmt.Errorf("invalid packet time base evidence")
			}
		}
		if output && track.TimestampStatus != "monotonic" {
			return fmt.Errorf("output timestamps are not monotonic")
		}
	}
	if f.Tracks["audio"] != nil {
		if f.EffectiveAudioBytes <= 0 || f.EffectiveAudioFrames <= 0 || !lowerHex64(f.EffectiveAudioSHA256) || len(f.AudioContracts) == 0 || (output && len(f.AudioContracts) != 1) {
			return fmt.Errorf("invalid decoded audio fingerprint")
		}
		for _, contract := range f.AudioContracts {
			if validateAudioContract(contract) != nil {
				return fmt.Errorf("invalid audio contract evidence")
			}
		}
	} else if f.EffectiveAudioBytes != 0 || f.EffectiveAudioFrames != 0 || f.EffectiveAudioSHA256 != "" || len(f.AudioContracts) != 0 {
		return fmt.Errorf("audio evidence exists without an audio track")
	}
	return nil
}

func validRational(raw string, positive bool) bool {
	value, ok := new(big.Rat).SetString(raw)
	return ok && (!positive || value.Sign() > 0)
}

func sameCanonical[T any](a, b []T) bool {
	_, aJSON, aErr := stitchcert.CanonicalSHA(a)
	_, bJSON, bErr := stitchcert.CanonicalSHA(b)
	return aErr == nil && bErr == nil && string(aJSON) == string(bJSON)
}

func hourBoundarySubset(ledger StreamDayAllocation, deliveryHour int) ([]CrossHourBoundary, []CrossDayBoundary) {
	hour := []CrossHourBoundary{}
	day := []CrossDayBoundary{}
	if deliveryHour > 1 {
		hour = append(hour, ledger.Boundaries[deliveryHour-2])
	}
	if deliveryHour < 12 {
		hour = append(hour, ledger.Boundaries[deliveryHour-1])
	}
	if deliveryHour == 1 {
		day = append(day, ledger.CrossDayBoundaries[0])
	}
	if deliveryHour == 12 {
		day = append(day, ledger.CrossDayBoundaries[1])
	}
	return hour, day
}

func sameClipIDs(ids []int64, sources []SourceClip) bool {
	if len(ids) != len(sources) {
		return false
	}
	for i := range ids {
		if ids[i] != sources[i].ClipID {
			return false
		}
	}
	return true
}
