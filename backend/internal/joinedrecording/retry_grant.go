package joinedrecording

import (
	"errors"
	"strings"
	"time"
)

// ExactRetryGrantRequest authorizes one additional attempt, never an attempt reset.
type ExactRetryGrantRequest struct {
	ProtocolVersion      int    `json:"protocol_version"`
	BatchID              string `json:"batch_id"`
	HourID               string `json:"hour_id"`
	ExpectedAttemptCount int    `json:"expected_attempt_count"`
	Reason               string `json:"reason"`
}

func (r ExactRetryGrantRequest) Validate() error {
	if r.ProtocolVersion != JoinedProtocolVersion || r.ExpectedAttemptCount < 1 || r.ExpectedAttemptCount >= 8 {
		return errors.New("exact retry requires protocol 1 and expected attempt between 1 and 7")
	}
	for _, value := range []string{r.BatchID, r.HourID, r.Reason} {
		if strings.TrimSpace(value) != value || value == "" || len(value) > 1024 || strings.ContainsAny(value, "\r\n\x00") {
			return errors.New("exact retry requires bounded batch, hour and audit reason")
		}
	}
	return nil
}

type ExactRetryGrant struct {
	BatchID              string     `json:"batch_id"`
	HourID               string     `json:"hour_id"`
	ExpectedAttemptCount int        `json:"expected_attempt_count"`
	FailureID            int64      `json:"failure_id"`
	Reason               string     `json:"reason"`
	GrantedAt            time.Time  `json:"granted_at"`
	ConsumedAt           *time.Time `json:"consumed_at,omitempty"`
}
