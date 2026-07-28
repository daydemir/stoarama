package main

import (
	"bytes"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/recordingnaming"
)

func TestCampaignPostflightRejectsMissingAndUnhealthyState(t *testing.T) {
	streamID := int64(17242)
	recordings := campaignRecordingsResponse{
		FleetRelayOnline: 1,
		Items: []campaignRecording{{
			ID: 383, StreamID: &streamID, Status: "active", CaptureVia: "relay",
			HasRelayOnline: true, HasRelayAssigned: false, CaptureHealth: "critical",
			ExpectedClipCount: 5, Delivery: recordingDeliveryManaged,
			Naming: campaignRecordingNaming{Profile: recordingnaming.ProfileStoaramaV1},
		}},
	}
	connections := campaignConnectionsResponse{Items: []campaignNASConnection{{
		ID: 13, Health: "stale", ClientPhase: "degraded", PendingClips: 2,
	}}}
	report := evaluateCampaignPostflight(time.Now(), nil, []int64{17242, 999}, recordings, connections, 0)
	if report.Healthy {
		t.Fatal("unhealthy campaign accepted")
	}
	if !slices.Equal(report.MissingStreamIDs, []int64{999}) {
		t.Fatalf("missing streams=%v", report.MissingStreamIDs)
	}
	if len(report.Recordings) != 1 || report.Recordings[0].Healthy {
		t.Fatalf("recording checks=%+v", report.Recordings)
	}
}

func TestCampaignPostflightAcceptsReadyCampaign(t *testing.T) {
	streamID := int64(17242)
	node := "mit-mac-1"
	now := time.Now().UTC()
	recordings := campaignRecordingsResponse{
		FleetRelayOnline: 1,
		Items: []campaignRecording{{
			ID: 383, StreamID: &streamID, Status: "active", CaptureVia: "relay",
			RelayNodeName: &node, HasRelayOnline: true, HasRelayAssigned: true,
			CaptureHealth: "healthy", ExpectedClipCount: 5, CapturedClipCount: 5,
			Delivery: recordingDeliveryNASPull,
			Naming:   campaignRecordingNaming{Profile: recordingnaming.ProfilePlazaHourlyV1},
		}},
	}
	connections := campaignConnectionsResponse{Items: []campaignNASConnection{{
		ID: 13, Health: "healthy", ClientPhase: "idle", LastSeenAt: &now,
		LastSuccessAt: &now,
	}}}
	report := evaluateCampaignPostflight(now, []int64{383}, nil, recordings, connections, 0)
	if !report.Healthy {
		t.Fatalf("ready campaign rejected: %+v", report)
	}
	recordings.Items[0].HasRelayAssigned = false
	report = evaluateCampaignPostflight(now, []int64{383}, nil, recordings, connections, 0)
	if report.Healthy {
		t.Fatal("relay recording without an assignment accepted")
	}
}

func TestCampaignPostflightTargetsAreExclusiveAndStrict(t *testing.T) {
	if _, _, err := campaignPostflightTargets("", "", ""); err == nil {
		t.Fatal("missing target accepted")
	}
	if _, _, err := campaignPostflightTargets("1", "batch.json", ""); err == nil {
		t.Fatal("multiple targets accepted")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "batch.json")
	if err := os.WriteFile(path, []byte(`{"items":[{"stream_id":7,"recording_id":9}],"created":1}`), 0o600); err != nil {
		t.Fatal(err)
	}
	recordingIDs, streamIDs, err := campaignPostflightTargets("", path, "")
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(recordingIDs, []int64{9}) || len(streamIDs) != 0 {
		t.Fatalf("recording ids=%v stream ids=%v", recordingIDs, streamIDs)
	}
}

func TestReadCampaignSessionCookie(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cookie")
	if err := os.WriteFile(path, []byte("session-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := readCampaignSessionCookie(path)
	if err != nil {
		t.Fatal(err)
	}
	if got != "stoarama_session=session-token" {
		t.Fatalf("cookie=%q", got)
	}
}

func TestGetRecordingJSONSupportsTokenAndSessionCookie(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer token" || r.Header.Get("Cookie") != "stoarama_session=session" {
			t.Fatalf("authorization=%q cookie=%q", r.Header.Get("Authorization"), r.Header.Get("Cookie"))
		}
		_, _ = w.Write([]byte(`{"items":[]}`))
	}))
	defer server.Close()
	var response campaignRecordingsResponse
	if err := getRecordingJSON(t.Context(), server.URL, "token", "stoarama_session=session", "/recordings", &response); err != nil {
		t.Fatal(err)
	}
}

func TestMITTop50CampaignTemplatesCoverExactlyEighteenLocalDays(t *testing.T) {
	type cohort struct {
		timezone string
		ids      []int64
	}
	cohorts := map[string]cohort{
		"america-los-angeles.json": {timezone: "America/Los_Angeles", ids: []int64{94, 178, 179, 182, 293, 295, 469, 487, 2666, 2667, 2681, 2693, 2713, 2717, 2718, 2720, 2726, 2963, 2997, 3001, 3002, 3006, 3007, 3018, 3021, 3022, 3023, 3025, 3027, 3039, 3040, 3049, 17238, 17241, 17242, 17244, 17245, 17248, 17249}},
		"america-new-york.json":    {timezone: "America/New_York", ids: []int64{17243}},
		"asia-bangkok.json":        {timezone: "Asia/Bangkok", ids: []int64{17247}},
		"asia-manila.json":         {timezone: "Asia/Manila", ids: []int64{17235}},
		"asia-seoul.json":          {timezone: "Asia/Seoul", ids: []int64{78, 415, 17237}},
		"asia-tokyo.json":          {timezone: "Asia/Tokyo", ids: []int64{17239, 17240}},
		"europe-rome.json":         {timezone: "Europe/Rome", ids: []int64{17233}},
		"europe-warsaw.json":       {timezone: "Europe/Warsaw", ids: []int64{17216, 17219}},
	}
	var actual, expected []int64
	root := filepath.Join("..", "..", "campaigns", "mit-scl-top-50")
	for name, cohort := range cohorts {
		raw, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			t.Fatal(err)
		}
		spec, err := decodeRecordingBatchSpec(bytes.NewReader(raw))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if !slices.Equal(spec.StreamIDs, cohort.ids) {
			t.Fatalf("%s stream ids=%v want %v", name, spec.StreamIDs, cohort.ids)
		}
		location, err := time.LoadLocation(cohort.timezone)
		if err != nil {
			t.Fatal(err)
		}
		if spec.StartAt == nil || spec.EndAt == nil || !spec.StartAt.In(location).AddDate(0, 0, 18).Equal(spec.EndAt.In(location)) {
			t.Fatalf("%s does not cover exactly 18 local days", name)
		}
		if start := spec.StartAt.In(location).Format("15:04"); start != "00:00" {
			t.Fatalf("%s local start=%s", name, start)
		}
		if spec.DailyWindowStart != "08:00" || spec.DailyWindowEnd != "20:00" ||
			spec.ClipDurationSec != 60 || spec.NamingProfile != recordingnaming.ProfilePlazaHourlyV1 ||
			spec.Delivery != recordingDeliveryNASPull || !spec.DryRun ||
			spec.StorageDestinationID != math.MaxInt64 {
			t.Fatalf("%s unsafe or incorrect campaign template", name)
		}
		actual = append(actual, spec.StreamIDs...)
		expected = append(expected, cohort.ids...)
	}
	slices.Sort(actual)
	slices.Sort(expected)
	if len(actual) != 50 {
		t.Fatalf("campaign contains %d streams, want 50", len(actual))
	}
	if err := validateCampaignIDs(actual); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(actual, expected) {
		t.Fatalf("campaign stream ids differ:\nactual=%v\nexpected=%v", actual, expected)
	}
}
