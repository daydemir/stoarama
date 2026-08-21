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
	BatchIndexSchemaVersion = 1
	MaxCanonicalJSONBytes   = 16 << 20
)

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

func BuildBatchIndex(index BatchIndex) (BatchIndex, []byte, string, error) {
	if index.SchemaVersion != BatchIndexSchemaVersion || index.PolicyVersion != PlanPolicyVersion || index.AllocationSchemaVersion != 1 || index.HourManifestSchemaVersion != HourManifestSchemaVersion || !safeBatchID.MatchString(index.BatchID) || index.Generation <= 0 || index.FrozenAt.IsZero() || !lowerHex64(index.BatchGenerationSHA256) || !lowerHex64(index.FrozenDenominatorSHA256) || len(index.RecordingIDs) == 0 || len(index.FrozenRecordings) != len(index.RecordingIDs) || recordingIDsSHA(index.RecordingIDs) != index.RecordingIDSHA256 || ValidateMediaToolEvidence(index.MediaTool) != nil || index.ExpectedLedgerCount != len(index.AllocationLedgers) || index.ExpectedLedgerCount != len(index.RecordingIDs)*14 || index.ScheduledHourCount != len(index.Hours) || index.ScheduledHourCount != index.ExpectedLedgerCount*12 || index.SourceClipCount < 0 || index.SourceBytes < 0 || index.FinalMediaCount < 0 {
		return BatchIndex{}, nil, "", fmt.Errorf("batch index denominator differs")
	}
	seenRecording := map[int64]bool{}
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
	for hourIndex, hour := range index.Hours {
		ledger := index.AllocationLedgers[hourIndex/12]
		expected := expectedHours[hour.HourID]
		wantRelative := path.Join("coverage", "hours", hour.HourID+".json")
		if expected.ArtifactID == 0 || expected.ArtifactID != ledger.ArtifactID || hour.DeliveryHour != hourIndex%12+1 || hour.HourManifestArtifactID <= 0 || seenArtifacts[hour.HourManifestArtifactID] || hour.RecordingID != ledger.RecordingID || hour.LocalDate != ledger.LocalDate || hour.HourID != canonicalHourIDValue(index.BatchID, hour.RecordingID, hour.LocalDate, hour.DeliveryHour, index.Generation) || hour.RelativePath != wantRelative || hour.ObjectKey != path.Join("joined", index.BatchID, wantRelative) || hour.SizeBytes <= 0 || !lowerHex64(hour.SHA256) || hour.SourceCount < 0 || hour.SourceBytes < 0 || hour.MediaArtifactCount < 0 {
			return BatchIndex{}, nil, "", fmt.Errorf("batch hour identity differs")
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
		seenArtifacts[hour.HourManifestArtifactID] = true
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
	if generationSHA, err := ComputeBatchGenerationSHA256(index); err != nil || generationSHA != index.BatchGenerationSHA256 {
		return BatchIndex{}, nil, "", fmt.Errorf("batch generation evidence differs")
	}
	sha, canonical, err := stitchcert.CanonicalSHA(index)
	if err != nil || len(canonical) > MaxCanonicalJSONBytes {
		return BatchIndex{}, nil, "", fmt.Errorf("batch index exceeds canonical limit")
	}
	return index, canonical, sha, nil
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
	var payload strings.Builder
	for _, id := range ids {
		payload.WriteString(strconv.FormatInt(id, 10))
		payload.WriteByte('\n')
	}
	sum := sha256.Sum256([]byte(payload.String()))
	return hex.EncodeToString(sum[:])
}

func CanonicalBatchIndexObjectKey(batchID string) (string, error) {
	if !safeBatchID.MatchString(batchID) {
		return "", fmt.Errorf("invalid joined batch identity")
	}
	return path.Join("joined", batchID, "coverage", "batch.json"), nil
}
