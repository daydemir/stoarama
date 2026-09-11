package joinedrecording

import "testing"

func TestExactRetryGrantRequiresBoundedHourAttempt(t *testing.T) {
	valid := ExactRetryGrantRequest{ProtocolVersion: 1, BatchID: "batch", HourID: "hour", ExpectedAttemptCount: 1, Reason: "Reviewed preseal timeout; retry once on corrected worker"}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*ExactRetryGrantRequest){
		func(r *ExactRetryGrantRequest) { r.ProtocolVersion = 0 },
		func(r *ExactRetryGrantRequest) { r.BatchID = "" },
		func(r *ExactRetryGrantRequest) { r.HourID = "" },
		func(r *ExactRetryGrantRequest) { r.ExpectedAttemptCount = 0 },
		func(r *ExactRetryGrantRequest) { r.ExpectedAttemptCount = 8 },
		func(r *ExactRetryGrantRequest) { r.Reason = " " },
	} {
		r := valid
		mutate(&r)
		if err := r.Validate(); err == nil {
			t.Fatalf("unsafe grant accepted: %+v", r)
		}
	}
}
