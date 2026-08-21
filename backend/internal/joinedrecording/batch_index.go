package joinedrecording

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/recordingnaming"
	"github.com/daydemir/stoarama/backend/internal/stitchcert"
)

const (
	BatchIndexSchemaVersion            = 1
	FrozenDenominatorProjectionVersion = 1
	MaxCanonicalJSONBytes              = 16 << 20
)

// FrozenDenominatorLedgerProjection is the one compact identity projection
// consumed in final-index ledger order. SourceClaimSHA256 already commits the
// ledger's exact ordered source-only canonical projection.
type FrozenDenominatorLedgerProjection struct {
	RecordingID       int64  `json:"recording_id"`
	LocalDate         string `json:"local_date"`
	SourceClaimSHA256 string `json:"source_claim_sha256"`
	SourceCount       int    `json:"source_count"`
	SourceBytes       int64  `json:"source_bytes"`
}

type FrozenDenominatorProjection struct {
	ProjectionVersion int                                 `json:"projection_version"`
	Ledgers           []FrozenDenominatorLedgerProjection `json:"ledgers"`
}

type BatchIndexHour struct {
	HourManifestArtifactID int64              `json:"hour_manifest_artifact_id"`
	HourID                 string             `json:"hour_id"`
	RecordingID            int64              `json:"recording_id"`
	LocalDate              string             `json:"local_date"`
	DeliveryHour           int                `json:"delivery_hour"`
	Status                 HourManifestStatus `json:"status"`
	RelativePath           string             `json:"relative_path"`
	ObjectKey              string             `json:"object_key"`
	SizeBytes              int64              `json:"size_bytes"`
	SHA256                 string             `json:"sha256"`
	SourceCount            int                `json:"source_count"`
	SourceBytes            int64              `json:"source_bytes"`
	MediaArtifactCount     int                `json:"media_artifact_count"`
}

type AllocationLedgerRef struct {
	ArtifactID        int64    `json:"artifact_id"`
	RecordingID       int64    `json:"recording_id"`
	LocalDate         string   `json:"local_date"`
	QualificationSHA  string   `json:"qualification_sha256"`
	SourceClaimSHA256 string   `json:"source_claim_sha256"`
	RelativePath      string   `json:"relative_path"`
	ObjectKey         string   `json:"object_key"`
	SizeBytes         int64    `json:"size_bytes"`
	SHA256            string   `json:"sha256"`
	LedgerSHA256      string   `json:"ledger_sha256"`
	SourceCount       int      `json:"source_count"`
	SourceBytes       int64    `json:"source_bytes"`
	ScheduledHourIDs  []string `json:"scheduled_hour_ids"`
}

type FrozenRecording struct {
	RecordingID       int64                    `json:"recording_id"`
	PriorityOrdinal   int                      `json:"priority_ordinal"`
	EligibilityTier   string                   `json:"eligibility_tier"`
	EligibilityCutoff time.Time                `json:"eligibility_cutoff"`
	CompletedAt       time.Time                `json:"completed_at"`
	Timezone          string                   `json:"timezone"`
	FolderName        string                   `json:"folder_name"`
	NamingMetadata    recordingnaming.Metadata `json:"naming_metadata"`
}

type BatchIndex struct {
	SchemaVersion             int                   `json:"schema_version"`
	PolicyVersion             string                `json:"policy_version"`
	AllocationSchemaVersion   int                   `json:"allocation_schema_version"`
	HourManifestSchemaVersion int                   `json:"hour_manifest_schema_version"`
	BatchID                   string                `json:"batch_id"`
	Generation                int                   `json:"generation"`
	FrozenAt                  time.Time             `json:"frozen_at"`
	BatchGenerationSHA256     string                `json:"batch_generation_sha256"`
	FrozenDenominatorSHA256   string                `json:"frozen_denominator_sha256"`
	RecordingIDs              []int64               `json:"recording_ids"`
	RecordingIDSHA256         string                `json:"recording_ids_sha256"`
	FrozenRecordings          []FrozenRecording     `json:"frozen_recordings"`
	MediaTool                 MediaToolEvidence     `json:"media_tool"`
	ExpectedLedgerCount       int                   `json:"expected_ledger_count"`
	ScheduledHourCount        int                   `json:"scheduled_hour_count"`
	SourceClipCount           int                   `json:"source_clip_count"`
	SourceBytes               int64                 `json:"source_bytes"`
	FinalMediaCount           int                   `json:"final_media_artifact_count"`
	AllocationLedgers         []AllocationLedgerRef `json:"allocation_ledgers"`
	Hours                     []BatchIndexHour      `json:"hours"`
}

type AllocationLedgerResolver func(AllocationLedgerRef) (StreamDayAllocation, error)
type HourManifestResolver func(BatchIndexHour) (HourManifest, error)

// BuildAllocationLedgerRef derives every caller-visible reference fact from
// the exact canonical ledger artifact. Only its database artifact ID is
// supplied separately.
func BuildAllocationLedgerRef(artifactID int64, ledger StreamDayAllocation) (AllocationLedgerRef, error) {
	canonical, artifactSHA, err := CanonicalAllocationLedgerArtifact(ledger)
	if err != nil || artifactID <= 0 {
		return AllocationLedgerRef{}, fmt.Errorf("canonical allocation ledger artifact is required")
	}
	relativePath, objectKey, err := CanonicalAllocationLedgerPaths(ledger.BatchID, ledger.RecordingID, ledger.LocalDate)
	if err != nil {
		return AllocationLedgerRef{}, err
	}
	hourIDs := make([]string, 12)
	for hour := 1; hour <= 12; hour++ {
		hourIDs[hour-1], err = CanonicalHourID(ledger.BatchID, ledger.RecordingID, ledger.LocalDate, hour, ledger.Generation)
		if err != nil {
			return AllocationLedgerRef{}, err
		}
	}
	return AllocationLedgerRef{
		ArtifactID:        artifactID,
		RecordingID:       ledger.RecordingID,
		LocalDate:         ledger.LocalDate,
		QualificationSHA:  ledger.QualificationSHA,
		SourceClaimSHA256: ledger.SourceClaimSHA256,
		RelativePath:      relativePath,
		ObjectKey:         objectKey,
		SizeBytes:         int64(len(canonical)),
		SHA256:            artifactSHA,
		LedgerSHA256:      ledger.LedgerSHA256,
		SourceCount:       ledger.SourceClipCount,
		SourceBytes:       ledger.SourceBytes,
		ScheduledHourIDs:  hourIDs,
	}, nil
}

func ValidateAllocationLedgerRef(ref AllocationLedgerRef, ledger StreamDayAllocation) error {
	want, err := BuildAllocationLedgerRef(ref.ArtifactID, ledger)
	if err != nil || !sameCanonical([]AllocationLedgerRef{ref}, []AllocationLedgerRef{want}) {
		return fmt.Errorf("allocation ledger reference differs from canonical artifact")
	}
	return nil
}

func BuildBatchIndexHour(artifactID int64, manifest HourManifest) (BatchIndexHour, error) {
	canonical, artifactSHA, err := CanonicalHourManifestArtifact(manifest)
	if err != nil || artifactID <= 0 {
		return BatchIndexHour{}, fmt.Errorf("canonical hour manifest artifact is required")
	}
	var sourceBytes int64
	for _, source := range manifest.Sources {
		if source.Object.SizeBytes > math.MaxInt64-sourceBytes {
			return BatchIndexHour{}, fmt.Errorf("hour manifest source bytes overflow")
		}
		sourceBytes += source.Object.SizeBytes
	}
	relativePath := path.Join("coverage", "hours", manifest.HourID+".json")
	return BatchIndexHour{
		HourManifestArtifactID: artifactID,
		HourID:                 manifest.HourID,
		RecordingID:            manifest.RecordingID,
		LocalDate:              manifest.LocalDate,
		DeliveryHour:           manifest.DeliveryHour,
		Status:                 manifest.Status,
		RelativePath:           relativePath,
		ObjectKey:              path.Join("joined", manifest.BatchID, relativePath),
		SizeBytes:              int64(len(canonical)),
		SHA256:                 artifactSHA,
		SourceCount:            manifest.SourceCount,
		SourceBytes:            sourceBytes,
		MediaArtifactCount:     len(manifest.Media),
	}, nil
}

func ValidateBatchIndexHour(ref BatchIndexHour, manifest HourManifest) error {
	want, err := BuildBatchIndexHour(ref.HourManifestArtifactID, manifest)
	if err != nil || !sameCanonical([]BatchIndexHour{ref}, []BatchIndexHour{want}) {
		return fmt.Errorf("hour-manifest reference differs from canonical artifact")
	}
	return nil
}

func BuildBatchIndex(index BatchIndex, resolveLedger AllocationLedgerResolver, resolveHour HourManifestResolver) (BatchIndex, []byte, string, error) {
	if resolveLedger == nil || resolveHour == nil {
		return BatchIndex{}, nil, "", fmt.Errorf("canonical ledger and hour resolvers are required")
	}
	return buildBatchIndex(index, resolveLedger, resolveHour)
}

func canonicalSealedBatchIndex(index BatchIndex) (BatchIndex, []byte, string, error) {
	return buildBatchIndex(index, nil, nil)
}

func buildBatchIndex(index BatchIndex, resolveLedger AllocationLedgerResolver, resolveHour HourManifestResolver) (BatchIndex, []byte, string, error) {
	if index.SchemaVersion != BatchIndexSchemaVersion || index.PolicyVersion != PlanPolicyVersion || index.AllocationSchemaVersion != 1 || index.HourManifestSchemaVersion != HourManifestSchemaVersion || !safeBatchID.MatchString(index.BatchID) || index.Generation <= 0 || index.FrozenAt.IsZero() || !lowerHex64(index.BatchGenerationSHA256) || len(index.RecordingIDs) == 0 || len(index.FrozenRecordings) != len(index.RecordingIDs) || recordingIDsSHA(index.RecordingIDs) != index.RecordingIDSHA256 || ValidateMediaToolEvidence(index.MediaTool) != nil || index.ExpectedLedgerCount != len(index.AllocationLedgers) || index.ExpectedLedgerCount != len(index.RecordingIDs)*14 || index.ScheduledHourCount != len(index.Hours) || index.ScheduledHourCount != index.ExpectedLedgerCount*12 || index.SourceClipCount < 0 || index.SourceBytes < 0 || index.FinalMediaCount < 0 {
		return BatchIndex{}, nil, "", fmt.Errorf("batch index denominator differs")
	}
	seenRecording := map[int64]bool{}
	frozenRecordings := make(map[int64]FrozenRecording, len(index.FrozenRecordings))
	for i, id := range index.RecordingIDs {
		if id <= 0 || seenRecording[id] {
			return BatchIndex{}, nil, "", fmt.Errorf("batch recording identity differs")
		}
		seenRecording[id] = true
		frozen := index.FrozenRecordings[i]
		folder, folderErr := recordingnaming.BuildFolderName(recordingnaming.ProfilePlazaHourlyV1, id, frozen.NamingMetadata, frozen.FolderName)
		_, timezoneErr := time.LoadLocation(frozen.Timezone)
		if frozen.RecordingID != id || frozen.PriorityOrdinal != i+1 || frozen.EligibilityTier != "good+" || frozen.EligibilityCutoff.IsZero() || frozen.CompletedAt.IsZero() || frozen.CompletedAt.After(frozen.EligibilityCutoff) || timezoneErr != nil || folderErr != nil || folder != frozen.FolderName {
			return BatchIndex{}, nil, "", fmt.Errorf("frozen recording metadata differs")
		}
		frozenRecordings[id] = frozen
	}
	seenArtifacts, expectedHours := map[int64]bool{}, map[string]AllocationLedgerRef{}
	var sourceCount int
	var sourceBytes int64
	var previousDate time.Time
	for ledgerIndex, ledger := range index.AllocationLedgers {
		recordingIndex, dayIndex := ledgerIndex/14, ledgerIndex%14
		if recordingIndex >= len(index.RecordingIDs) || ledger.RecordingID != index.RecordingIDs[recordingIndex] {
			return BatchIndex{}, nil, "", fmt.Errorf("batch allocation ledger order differs")
		}
		date, dateErr := time.Parse("2006-01-02", ledger.LocalDate)
		if dateErr != nil || (dayIndex > 0 && !date.Equal(previousDate.AddDate(0, 0, 1))) {
			return BatchIndex{}, nil, "", fmt.Errorf("batch allocation ledger dates are not 14 consecutive days")
		}
		previousDate = date
		relative, objectKey, err := CanonicalAllocationLedgerPaths(index.BatchID, ledger.RecordingID, ledger.LocalDate)
		if err != nil || !seenRecording[ledger.RecordingID] || ledger.ArtifactID <= 0 || seenArtifacts[ledger.ArtifactID] || ledger.RelativePath != relative || ledger.ObjectKey != objectKey || ledger.SizeBytes <= 0 || !lowerHex64(ledger.SHA256) || !lowerHex64(ledger.LedgerSHA256) || !lowerHex64(ledger.QualificationSHA) || !lowerHex64(ledger.SourceClaimSHA256) || ledger.SourceCount < 0 || ledger.SourceBytes < 0 || len(ledger.ScheduledHourIDs) != 12 {
			return BatchIndex{}, nil, "", fmt.Errorf("batch allocation ledger differs")
		}
		seenArtifacts[ledger.ArtifactID] = true
		for hour := 1; hour <= 12; hour++ {
			want := canonicalHourIDValue(index.BatchID, ledger.RecordingID, ledger.LocalDate, hour, index.Generation)
			if ledger.ScheduledHourIDs[hour-1] != want || expectedHours[want].ArtifactID != 0 {
				return BatchIndex{}, nil, "", fmt.Errorf("ledger scheduled hour identity differs")
			}
			expectedHours[want] = ledger
		}
		if ledger.SourceCount > math.MaxInt-sourceCount || ledger.SourceBytes > math.MaxInt64-sourceBytes {
			return BatchIndex{}, nil, "", fmt.Errorf("batch ledger denominator overflow")
		}
		sourceCount += ledger.SourceCount
		sourceBytes += ledger.SourceBytes
	}
	var hourSources, mediaCount int
	var hourBytes int64
	var canonicalLedger StreamDayAllocation
	var previousCanonicalLedger StreamDayAllocation
	for hourIndex, hour := range index.Hours {
		ledger := index.AllocationLedgers[hourIndex/12]
		if resolveHour != nil && hourIndex%12 == 0 {
			var resolveErr error
			nextLedger, resolveErr := resolveLedger(ledger)
			if resolveErr != nil || ValidateAllocationLedgerRef(ledger, nextLedger) != nil || nextLedger.Timezone != frozenRecordings[ledger.RecordingID].Timezone {
				return BatchIndex{}, nil, "", fmt.Errorf("batch allocation ledger does not match its canonical artifact")
			}
			if previousCanonicalLedger.RecordingID == nextLedger.RecordingID && validateCrossDayLedgerLink(previousCanonicalLedger, nextLedger) != nil {
				return BatchIndex{}, nil, "", fmt.Errorf("consecutive allocation ledgers disagree on their shared day boundary")
			}
			canonicalLedger = nextLedger
			previousCanonicalLedger = nextLedger
		}
		expected := expectedHours[hour.HourID]
		wantRelative := path.Join("coverage", "hours", hour.HourID+".json")
		if expected.ArtifactID == 0 || expected.ArtifactID != ledger.ArtifactID || hour.DeliveryHour != hourIndex%12+1 || hour.HourManifestArtifactID <= 0 || seenArtifacts[hour.HourManifestArtifactID] || hour.RecordingID != ledger.RecordingID || hour.LocalDate != ledger.LocalDate || hour.HourID != canonicalHourIDValue(index.BatchID, hour.RecordingID, hour.LocalDate, hour.DeliveryHour, index.Generation) || hour.RelativePath != wantRelative || hour.ObjectKey != path.Join("joined", index.BatchID, wantRelative) || hour.SizeBytes <= 0 || !lowerHex64(hour.SHA256) || hour.SourceCount < 0 || hour.SourceBytes < 0 || hour.MediaArtifactCount < 0 {
			return BatchIndex{}, nil, "", fmt.Errorf("batch hour identity differs")
		}
		seenArtifacts[hour.HourManifestArtifactID] = true
		if resolveHour != nil {
			canonicalManifest, resolveErr := resolveHour(hour)
			if resolveErr != nil {
				return BatchIndex{}, nil, "", fmt.Errorf("resolve canonical hour manifest: %w", resolveErr)
			}
			if err := ValidateBatchIndexHour(hour, canonicalManifest); err != nil {
				return BatchIndex{}, nil, "", fmt.Errorf("batch hour does not match its canonical manifest artifact: %w", err)
			}
			if !sameCanonical([]MediaToolEvidence{canonicalManifest.MediaTool}, []MediaToolEvidence{index.MediaTool}) {
				return BatchIndex{}, nil, "", fmt.Errorf("batch hour media tool differs from frozen batch tool")
			}
			if err := validateHourManifestDelivery(canonicalManifest, frozenRecordings[hour.RecordingID]); err != nil {
				return BatchIndex{}, nil, "", fmt.Errorf("batch hour delivery path differs from frozen naming: %w", err)
			}
			if err := ValidateHourManifestLedgerBinding(canonicalManifest, ledger, canonicalLedger); err != nil {
				return BatchIndex{}, nil, "", fmt.Errorf("batch hour does not match its canonical allocation ledger: %w", err)
			}
			for _, media := range canonicalManifest.Media {
				if seenArtifacts[media.ArtifactID] {
					return BatchIndex{}, nil, "", fmt.Errorf("batch media artifact identity is reused")
				}
				seenArtifacts[media.ArtifactID] = true
			}
		}
		switch hour.Status {
		case HourStatusMedia:
			if hour.SourceCount == 0 || hour.MediaArtifactCount == 0 {
				return BatchIndex{}, nil, "", fmt.Errorf("media hour lacks sources or artifacts")
			}
		case HourStatusGapOnly:
			if hour.SourceCount != 0 || hour.SourceBytes != 0 || hour.MediaArtifactCount != 0 {
				return BatchIndex{}, nil, "", fmt.Errorf("gap-only hour accounts sources")
			}
		case HourStatusQuarantineOnly:
			if hour.SourceCount == 0 || hour.MediaArtifactCount != 0 {
				return BatchIndex{}, nil, "", fmt.Errorf("quarantine-only hour differs")
			}
		default:
			return BatchIndex{}, nil, "", fmt.Errorf("unknown terminal hour status")
		}
		delete(expectedHours, hour.HourID)
		if hour.SourceCount > math.MaxInt-hourSources || hour.SourceBytes > math.MaxInt64-hourBytes || hour.MediaArtifactCount > math.MaxInt-mediaCount {
			return BatchIndex{}, nil, "", fmt.Errorf("batch hour denominator overflow")
		}
		hourSources += hour.SourceCount
		hourBytes += hour.SourceBytes
		mediaCount += hour.MediaArtifactCount
	}
	if len(expectedHours) != 0 || sourceCount != index.SourceClipCount || sourceBytes != index.SourceBytes || hourSources != sourceCount || hourBytes != sourceBytes || mediaCount != index.FinalMediaCount {
		return BatchIndex{}, nil, "", fmt.Errorf("batch proof root does not exactly account its denominator")
	}
	denominatorSHA, err := ComputeFrozenDenominatorSHA256(index.AllocationLedgers)
	if err != nil || denominatorSHA != index.FrozenDenominatorSHA256 {
		return BatchIndex{}, nil, "", fmt.Errorf("frozen denominator evidence differs")
	}
	if generationSHA, err := ComputeBatchGenerationSHA256(index); err != nil || generationSHA != index.BatchGenerationSHA256 {
		return BatchIndex{}, nil, "", fmt.Errorf("batch generation evidence differs")
	}
	sha, canonical, err := stitchcert.CanonicalSHA(index)
	if err != nil || len(canonical) > MaxCanonicalJSONBytes {
		return BatchIndex{}, nil, "", fmt.Errorf("batch index exceeds canonical limit")
	}
	return index, canonical, sha, nil
}

func validateHourManifestDelivery(manifest HourManifest, frozen FrozenRecording) error {
	if manifest.RecordingID != frozen.RecordingID || manifest.Timezone != frozen.Timezone {
		return fmt.Errorf("hour manifest recording naming scope differs")
	}
	for _, media := range manifest.Media {
		want, err := recordingnaming.BuildJoinedPath(recordingnaming.JoinedPolicy{
			FolderName:   frozen.FolderName,
			Metadata:     frozen.NamingMetadata,
			CronTimezone: frozen.Timezone,
			ActualStart:  media.ActualStartUTC,
			ActualEnd:    media.ActualEndUTC,
			Hour:         manifest.DeliveryHour,
			Part:         media.Part,
			Parts:        media.Parts,
		})
		if err != nil || media.RelativePath != want {
			return fmt.Errorf("media delivery path differs")
		}
	}
	return nil
}

func validateCrossDayLedgerLink(previous, next StreamDayAllocation) error {
	if previous.RecordingID != next.RecordingID || previous.BatchID != next.BatchID || previous.Generation != next.Generation || previous.Timezone != next.Timezone || len(previous.CrossDayBoundaries) != 2 || len(next.CrossDayBoundaries) != 2 {
		return fmt.Errorf("cross-day ledger scope differs")
	}
	right, left := previous.CrossDayBoundaries[1], next.CrossDayBoundaries[0]
	type sharedBoundaryFact struct {
		PreviousClipID             *int64     `json:"previous_clip_id"`
		NextClipID                 *int64     `json:"next_clip_id"`
		PreviousPresentationEndUTC *time.Time `json:"previous_presentation_end_utc"`
		NextPresentationStartUTC   *time.Time `json:"next_presentation_start_utc"`
		SignedGapNanoseconds       *int64     `json:"signed_gap_nanoseconds"`
		ScheduledPreviousEndUTC    time.Time  `json:"scheduled_previous_end_utc"`
		ScheduledNextStartUTC      time.Time  `json:"scheduled_next_start_utc"`
		BoundarySkewNanoseconds    *int64     `json:"boundary_skew_nanoseconds"`
		Verdict                    string     `json:"verdict"`
	}
	project := func(boundary CrossDayBoundary) sharedBoundaryFact {
		return sharedBoundaryFact{boundary.PreviousClipID, boundary.NextClipID, boundary.PreviousPresentationEndUTC, boundary.NextPresentationStartUTC, boundary.SignedGapNanoseconds, boundary.ScheduledPreviousEndUTC, boundary.ScheduledNextStartUTC, boundary.BoundarySkewNanoseconds, boundary.Verdict}
	}
	if !sameCanonical([]sharedBoundaryFact{project(right)}, []sharedBoundaryFact{project(left)}) {
		return fmt.Errorf("cross-day neighbor facts differ")
	}
	return nil
}

// ComputeFrozenDenominatorSHA256 hashes the exact ordered ledger-source
// identity projection used by the backend and independently reproduced by the
// NAS after validating each referenced ledger. The projection is bounded by
// the fixed ledger denominator, not by the source-clip count.
func ComputeFrozenDenominatorSHA256(ledgers []AllocationLedgerRef) (string, error) {
	if len(ledgers) == 0 {
		return "", fmt.Errorf("frozen denominator requires allocation ledgers")
	}
	projection := FrozenDenominatorProjection{
		ProjectionVersion: FrozenDenominatorProjectionVersion,
		Ledgers:           make([]FrozenDenominatorLedgerProjection, len(ledgers)),
	}
	for i, ledger := range ledgers {
		if ledger.RecordingID <= 0 || !validLocalDate(ledger.LocalDate) || !lowerHex64(ledger.SourceClaimSHA256) || ledger.SourceCount < 0 || ledger.SourceBytes < 0 {
			return "", fmt.Errorf("frozen denominator ledger %d differs", i+1)
		}
		projection.Ledgers[i] = FrozenDenominatorLedgerProjection{
			RecordingID:       ledger.RecordingID,
			LocalDate:         ledger.LocalDate,
			SourceClaimSHA256: ledger.SourceClaimSHA256,
			SourceCount:       ledger.SourceCount,
			SourceBytes:       ledger.SourceBytes,
		}
	}
	digest, _, err := stitchcert.CanonicalSHA(projection)
	return digest, err
}

func validLocalDate(value string) bool {
	parsed, err := time.Parse("2006-01-02", value)
	return err == nil && parsed.Format("2006-01-02") == value
}

func ComputeBatchGenerationSHA256(index BatchIndex) (string, error) {
	type ledgerEvidence struct {
		RecordingID       int64  `json:"recording_id"`
		LocalDate         string `json:"local_date"`
		QualificationSHA  string `json:"qualification_sha256"`
		SourceClaimSHA256 string `json:"source_claim_sha256"`
		LedgerSHA256      string `json:"ledger_sha256"`
		SourceCount       int    `json:"source_count"`
		SourceBytes       int64  `json:"source_bytes"`
	}
	ledgers := make([]ledgerEvidence, len(index.AllocationLedgers))
	for i, ledger := range index.AllocationLedgers {
		ledgers[i] = ledgerEvidence{ledger.RecordingID, ledger.LocalDate, ledger.QualificationSHA, ledger.SourceClaimSHA256, ledger.LedgerSHA256, ledger.SourceCount, ledger.SourceBytes}
	}
	evidence := struct {
		SchemaVersion           int               `json:"schema_version"`
		PolicyVersion           string            `json:"policy_version"`
		BatchID                 string            `json:"batch_id"`
		Generation              int               `json:"generation"`
		FrozenAt                time.Time         `json:"frozen_at"`
		FrozenDenominatorSHA256 string            `json:"frozen_denominator_sha256"`
		RecordingIDSHA256       string            `json:"recording_ids_sha256"`
		FrozenRecordings        []FrozenRecording `json:"frozen_recordings"`
		MediaToolIdentity       string            `json:"media_tool_identity"`
		ExpectedLedgerCount     int               `json:"expected_ledger_count"`
		ScheduledHourCount      int               `json:"scheduled_hour_count"`
		SourceClipCount         int               `json:"source_clip_count"`
		SourceBytes             int64             `json:"source_bytes"`
		Ledgers                 []ledgerEvidence  `json:"ledgers"`
	}{index.SchemaVersion, index.PolicyVersion, index.BatchID, index.Generation, index.FrozenAt, index.FrozenDenominatorSHA256, index.RecordingIDSHA256, index.FrozenRecordings, index.MediaTool.IdentitySHA256, index.ExpectedLedgerCount, index.ScheduledHourCount, index.SourceClipCount, index.SourceBytes, ledgers}
	digest, _, err := stitchcert.CanonicalSHA(evidence)
	return digest, err
}

func recordingIDsSHA(ids []int64) string {
	digest, _ := RecordingIDsSHA256(ids)
	return digest
}

// RecordingIDsSHA256 hashes the ordered base-10 ID list with one LF after
// every ID, including the terminal LF.
func RecordingIDsSHA256(ids []int64) (string, error) {
	if len(ids) == 0 {
		return "", fmt.Errorf("recording identity list is empty")
	}
	var payload strings.Builder
	seen := make(map[int64]bool, len(ids))
	for _, id := range ids {
		if id <= 0 || seen[id] {
			return "", fmt.Errorf("recording identity list differs")
		}
		seen[id] = true
		payload.WriteString(strconv.FormatInt(id, 10))
		payload.WriteByte('\n')
	}
	sum := sha256.Sum256([]byte(payload.String()))
	return hex.EncodeToString(sum[:]), nil
}

func CanonicalBatchIndexObjectKey(batchID string) (string, error) {
	if !safeBatchID.MatchString(batchID) {
		return "", fmt.Errorf("invalid joined batch identity")
	}
	return path.Join("joined", batchID, "coverage", "batch.json"), nil
}
