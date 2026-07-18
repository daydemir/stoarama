package main

import (
	"strings"
	"testing"
)

func TestDecodeRecordingBatchSpecStrict(t *testing.T) {
	valid := `{"stream_ids":[1],"mode":"continuous","active_weekdays":[1,2,3,4,5]}`
	spec, err := decodeRecordingBatchSpec(strings.NewReader(valid))
	if err != nil || spec.Mode != recordingScheduleContinuous {
		t.Fatalf("decode valid spec: mode=%q err=%v", spec.Mode, err)
	}
	for _, raw := range []string{
		`{"stream_ids":[1],"mode":"sometimes"}`,
		`{"stream_ids":[1],"mode":"sampled","unknown":true}`,
		`{"stream_ids":[],"mode":"sampled"}`,
		`{"stream_ids":[1,1],"mode":"sampled"}`,
		`{"stream_ids":[1],"mode":"continuous","active_weekdays":[8]}`,
		`{"stream_ids":[1],"stream_timezones":[{"stream_id":2,"timezone":"UTC"}],"mode":"sampled"}`,
		valid + `{}`,
	} {
		if _, err := decodeRecordingBatchSpec(strings.NewReader(raw)); err == nil {
			t.Fatalf("expected strict decode failure for %s", raw)
		}
	}
}
