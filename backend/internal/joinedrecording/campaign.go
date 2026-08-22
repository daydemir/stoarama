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
)

var Tier1RecordingIDs = []int64{377, 335, 337, 355, 385, 350, 382, 384, 348, 403, 380, 379, 383, 404, 401, 408, 406, 409, 422, 418, 419, 413, 420, 428, 423, 425, 416, 421, 437, 440, 429, 431, 439}

func Tier1Payload() []byte {
	var payload strings.Builder
	for _, id := range Tier1RecordingIDs {
		payload.WriteString(strconv.FormatInt(id, 10))
		payload.WriteByte('\n')
	}
	return []byte(payload.String())
}
