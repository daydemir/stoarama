package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestDecodeRecordingBatchSpecStrict(t *testing.T) {
	valid := `{"stream_ids":[1],"naming_profile":"plaza_hourly_v1","mode":"continuous","delivery":"managed","storage_destination_id":1,"active_weekdays":[1,2,3,4,5]}`
	spec, err := decodeRecordingBatchSpec(strings.NewReader(valid))
	if err != nil || spec.Mode != recordingScheduleContinuous {
		t.Fatalf("decode valid spec: mode=%q err=%v", spec.Mode, err)
	}
	for _, raw := range []string{
		`{"stream_ids":[1],"mode":"sometimes"}`,
		`{"stream_ids":[1],"mode":"sampled","delivery":"elsewhere"}`,
		`{"stream_ids":[1],"mode":"sampled"}`,
		`{"stream_ids":[1],"mode":"sampled","delivery":"managed"}`,
		`{"stream_ids":[1],"mode":"sampled","delivery":"managed","storage_destination_id":1,"delivery_storage_destination_id":2}`,
		`{"stream_ids":[1],"mode":"sampled","delivery":"nas_pull","delivery_storage_destination_id":2}`,
		`{"stream_ids":[1],"mode":"sampled","unknown":true}`,
		`{"stream_ids":[],"mode":"sampled"}`,
		`{"stream_ids":[1,1],"mode":"sampled"}`,
		`{"stream_ids":[1],"naming_profile":"plaza_hourly_v1","mode":"continuous","active_weekdays":[8]}`,
		`{"stream_ids":[1],"stream_timezones":[{"stream_id":2,"timezone":"UTC"}],"mode":"sampled"}`,
		valid + `{}`,
	} {
		if _, err := decodeRecordingBatchSpec(strings.NewReader(raw)); err == nil {
			t.Fatalf("expected strict decode failure for %s", raw)
		}
	}
}

func TestDecodeRecordingBatchSpecLimit(t *testing.T) {
	for count, wantErr := range map[int]bool{200: false, 201: true} {
		streamIDs := make([]int64, count)
		for i := range streamIDs {
			streamIDs[i] = int64(i + 1)
		}
		raw, err := json.Marshal(recordingBatchSpec{StreamIDs: streamIDs, NamingProfile: "stoarama_v1", Mode: recordingScheduleSampled, Delivery: recordingDeliveryManaged, StorageDestinationID: 1})
		if err != nil {
			t.Fatal(err)
		}
		_, err = decodeRecordingBatchSpec(strings.NewReader(string(raw)))
		if (err != nil) != wantErr {
			t.Fatalf("count=%d err=%v", count, err)
		}
	}
}
