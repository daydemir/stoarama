package main

import (
	"encoding/json"
	"flag"
	"strings"
	"testing"
)

func TestScheduleBatchDryRunFlagSupportsExplicitFalse(t *testing.T) {
	for _, tc := range []struct {
		args      []string
		wantValue bool
	}{
		{args: []string{"--dry-run"}, wantValue: true},
		{args: []string{"--dry-run=false"}, wantValue: false},
	} {
		fs := flag.NewFlagSet("test", flag.ContinueOnError)
		dryRun := optionalBoolFlag(fs, "dry-run")
		if err := fs.Parse(tc.args); err != nil {
			t.Fatalf("parse %v: %v", tc.args, err)
		}
		if !dryRun.set || dryRun.value != tc.wantValue {
			t.Fatalf("parse %v = (set=%t value=%t), want value=%t", tc.args, dryRun.set, dryRun.value, tc.wantValue)
		}
	}
}

func TestDecodeRecordingBatchSpecStrict(t *testing.T) {
	valid := `{"stream_ids":[1],"naming_profile":"plaza_hourly_v1","mode":"continuous","delivery":"managed","storage_destination_id":1,"active_weekdays":[1,2,3,4,5]}`
	spec, err := decodeRecordingBatchSpec(strings.NewReader(valid))
	if err != nil || spec.Mode != recordingScheduleContinuous {
		t.Fatalf("decode valid spec: mode=%q err=%v", spec.Mode, err)
	}
	spaced := strings.Replace(valid, `"plaza_hourly_v1"`, `" plaza_hourly_v1 "`, 1)
	spec, err = decodeRecordingBatchSpec(strings.NewReader(spaced))
	if err != nil || spec.NamingProfile != "plaza_hourly_v1" {
		t.Fatalf("normalize naming profile: profile=%q err=%v", spec.NamingProfile, err)
	}
	for _, raw := range []string{
		`{"stream_ids":[1],"naming_profile":"stoarama_v1","mode":"sometimes"}`,
		`{"stream_ids":[1],"naming_profile":"stoarama_v1","mode":"sampled","delivery":"elsewhere"}`,
		`{"stream_ids":[1],"mode":"sampled","delivery":"managed","storage_destination_id":1}`,
		`{"stream_ids":[1],"naming_profile":"unsupported","mode":"sampled","delivery":"managed","storage_destination_id":1}`,
		`{"stream_ids":[1],"naming_profile":"stoarama_v1","mode":"sampled"}`,
		`{"stream_ids":[1],"naming_profile":"stoarama_v1","mode":"sampled","delivery":"managed"}`,
		`{"stream_ids":[1],"naming_profile":"stoarama_v1","mode":"sampled","delivery":"managed","storage_destination_id":1,"delivery_storage_destination_id":2}`,
		`{"stream_ids":[1],"naming_profile":"stoarama_v1","mode":"sampled","delivery":"nas_pull","delivery_storage_destination_id":2}`,
		`{"stream_ids":[1],"naming_profile":"stoarama_v1","mode":"sampled","unknown":true}`,
		`{"stream_ids":[],"naming_profile":"stoarama_v1","mode":"sampled"}`,
		`{"stream_ids":[1,1],"naming_profile":"stoarama_v1","mode":"sampled","delivery":"managed","storage_destination_id":1}`,
		`{"stream_ids":[1],"naming_profile":"plaza_hourly_v1","mode":"continuous","active_weekdays":[8],"delivery":"managed","storage_destination_id":1}`,
		`{"stream_ids":[1],"stream_timezones":[{"stream_id":2,"timezone":"UTC"}],"naming_profile":"stoarama_v1","mode":"sampled","delivery":"managed","storage_destination_id":1}`,
		`{"stream_ids":[1],"naming_profile":"stoarama_v1","mode":"sampled","delivery":"managed","storage_destination_id":1,"required_relay_slots":-1}`,
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
