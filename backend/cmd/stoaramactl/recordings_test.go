package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
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
		`{"target_account_id":-1,"stream_ids":[1],"naming_profile":"stoarama_v1","mode":"sampled","delivery":"managed","storage_destination_id":1}`,
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

func TestDecodeCampaignAdmissionSpecStrictSingleObject(t *testing.T) {
	valid := `{"request_id":"00000000-0000-0000-0000-000000000001","entries":[]}`
	got, err := decodeCampaignAdmissionSpec(strings.NewReader(valid))
	if err != nil || string(got) != valid {
		t.Fatalf("decode valid approval: got=%s err=%v", got, err)
	}
	for _, invalid := range []string{`[]`, `null`, `{}` + `{}`, ``} {
		if _, err := decodeCampaignAdmissionSpec(strings.NewReader(invalid)); err == nil {
			t.Fatalf("accepted invalid approval envelope %q", invalid)
		}
	}
}

func TestCandidateScenePresentationRequestAndResponse(t *testing.T) {
	frame := []byte("jpeg-proof")
	frameSHA := fmt.Sprintf("%x", sha256.Sum256(frame))
	presentationID := uuid.New()
	requestID := uuid.New()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/api/v1/account/recordings/qualification/scene-presentations/22" || r.URL.Query().Get("stream_id") != "11" || r.URL.Query().Get("authority_code") != "decision" || r.URL.Query().Get("request_id") != requestID.String() || r.Header.Get("Cookie") != "stoarama_session=secret" || r.Header.Get("Accept") != "image/jpeg" {
			t.Fatalf("unexpected presentation request: method=%s url=%s", r.Method, r.URL.String())
		}
		w.Header().Set("Content-Type", "image/jpeg")
		w.Header().Set("X-Stoarama-Presentation-ID", presentationID.String())
		w.Header().Set("X-Content-SHA256", frameSHA)
		_, _ = w.Write(frame)
	}))
	defer server.Close()
	got, err := getRecordingCandidateScenePresentation(context.Background(), server.URL, "stoarama_session=secret", 11, 22, "decision", requestID)
	if err != nil || got.PresentationID != presentationID.String() || got.FrameSHA256 != frameSHA || string(got.Body) != string(frame) {
		t.Fatalf("presentation=%+v err=%v", got, err)
	}
}

func TestRecordingSceneAttestPayloadModesAndRequest(t *testing.T) {
	presentationID := uuid.NewString()
	candidate, err := recordingSceneAttestPayload(0, 22, presentationID, "  Exact Scene  ")
	if err != nil || candidate["presentation_id"] != presentationID || candidate["scene_identity"] != "Exact Scene" || candidate["stream_id"] != nil || candidate["authority_code"] != nil {
		t.Fatalf("candidate payload=%v err=%v", candidate, err)
	}
	recording, err := recordingSceneAttestPayload(33, 22, "", "Exact Scene")
	if err != nil || recording["recording_id"] != int64(33) {
		t.Fatalf("recording payload=%v err=%v", recording, err)
	}
	for _, args := range []struct {
		recordingID int64
		receipt     string
	}{{33, presentationID}, {0, "bad"}} {
		if _, err := recordingSceneAttestPayload(args.recordingID, 22, args.receipt, "Exact Scene"); err == nil {
			t.Fatalf("accepted ambiguous payload %+v", args)
		}
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		body, _ := io.ReadAll(r.Body)
		if r.Method != http.MethodPost || r.URL.Path != "/api/v1/account/recordings/qualification/scene-attest" || r.Header.Get("Cookie") != "stoarama_session=secret" || strings.Contains(string(body), `"stream_id"`) || strings.Contains(string(body), `"authority_code"`) || !strings.Contains(string(body), `"presentation_id":"`+presentationID+`"`) {
			t.Fatalf("unexpected attest request: method=%s path=%s body=%s", r.Method, r.URL.Path, body)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"evidence_id":44}`))
	}))
	defer server.Close()
	if got := postRecordingSessionJSON(context.Background(), server.URL, "stoarama_session=secret", "/api/v1/account/recordings/qualification/scene-attest", candidate); got["evidence_id"] != float64(44) {
		t.Fatalf("attest response=%v", got)
	}
}
