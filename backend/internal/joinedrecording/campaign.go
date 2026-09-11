package joinedrecording

import (
	"strconv"
	"strings"
)

const (
	Tier1FrozenAt                       = "2026-08-21T06:59:07.534131Z"
	Tier1BatchID                        = "goodplus-20260821-generation-1"
	Tier1RecordingIDSHA                 = "6038d4a23be9b0b5c2bb29ea933743a5ceb7f06b8875e417a3f16b44051ebd71"
	Tier1HistoricalQualificationVersion = "recording-qualification-tier1-historical-import-v1"
	Tier1HistoricalAuthorityKind        = "historical_operator_import_v1"
	PlannerAdvisoryLock                 = "recording_joined_output_planner"
	SeptemberBatchID                    = "goodplus-20260911-generation-1"
)

var Tier1RecordingIDs = []int64{377, 335, 337, 355, 385, 350, 382, 384, 348, 403, 380, 379, 383, 404, 401, 408, 406, 409, 422, 418, 419, 413, 420, 428, 423, 425, 416, 421, 437, 440, 429, 431, 439}

// ApprovedCohort is an operator-approved immutable selection, not an open-ended
// cohort builder. September adds only recordings absent from the original batch.
type ApprovedCohort struct {
	RecordingIDs   []int64
	RecordingIDSHA string
	FrozenAt       string
	FirstDates     []string
}

// CohortForBatch preserves the original freeze protocol's legacy batch aliases.
// Only the exact September batch receives the additional nine-recording scope.
func CohortForBatch(batchID string) ApprovedCohort {
	if batchID == SeptemberBatchID {
		return ApprovedCohort{
			RecordingIDs:   []int64{339, 407, 417, 424, 427, 430, 441, 444, 445},
			RecordingIDSHA: "85b4fc9db34b50aa53666f1d364bef8ce267b581d56d17411d5342bd20648888",
			FrozenAt:       "2026-09-11T05:15:24.544Z",
			FirstDates:     []string{"2026-08-26", "2026-08-27", "2026-08-15", "2026-08-07", "2026-08-13", "2026-08-08", "2026-08-07", "2026-08-08", "2026-08-12"},
		}
	}
	return ApprovedCohort{RecordingIDs: append([]int64(nil), Tier1RecordingIDs...),
		RecordingIDSHA: Tier1RecordingIDSHA, FrozenAt: Tier1FrozenAt}
}

func Tier1Payload() []byte {
	var payload strings.Builder
	for _, id := range Tier1RecordingIDs {
		payload.WriteString(strconv.FormatInt(id, 10))
		payload.WriteByte('\n')
	}
	return []byte(payload.String())
}
