package api

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func pf(v float64) *float64 { return &v }
func pi(v int) *int         { return &v }
func pi64(v int64) *int64   { return &v }

func TestClassifyQualificationTimeline(t *testing.T) {
	base := qualificationWindowMetrics{CoveragePct: pf(99), LargestGap: pf(120), GapsOver30s: pi(1), GapsOver5m: pi(0), OverlapCount: pi(0), MetricVersion: pi(2), ExpectedSeconds: 43200, MeasuredExpected: pi64(43200), JobCount: 1, HealthCount: 1}
	if got, _ := classifyQualificationTimeline(base); got != "GREAT_CANDIDATE" {
		t.Fatalf("great boundary=%s", got)
	}
	good := base
	good.CoveragePct = pf(95)
	good.LargestGap = pf(900)
	good.GapsOver30s = pi(6)
	good.GapsOver5m = pi(1)
	if got, _ := classifyQualificationTimeline(good); got != "GOOD_CANDIDATE" {
		t.Fatalf("good boundary=%s", got)
	}
	acceptable := good
	acceptable.CoveragePct = pf(90)
	acceptable.LargestGap = pf(1800)
	acceptable.GapsOver30s = pi(9)
	acceptable.GapsOver5m = pi(2)
	if got, _ := classifyQualificationTimeline(acceptable); got != "ACCEPTABLE_CANDIDATE" {
		t.Fatalf("acceptable boundary=%s", got)
	}
	failed := acceptable
	failed.CoveragePct = pf(89.999)
	if got, _ := classifyQualificationTimeline(failed); got != "FAILED" {
		t.Fatalf("failed=%s", got)
	}
	unknown := base
	unknown.MetricVersion = pi(1)
	if got, _ := classifyQualificationTimeline(unknown); got != "UNKNOWN" {
		t.Fatalf("old metric=%s", got)
	}
	inconsistent := base
	inconsistent.LargestGap = pf(301)
	inconsistent.GapsOver5m = pi(0)
	if got, _ := classifyQualificationTimeline(inconsistent); got != "UNKNOWN" {
		t.Fatalf("inconsistent=%s", got)
	}
	for _, tc := range []struct {
		name   string
		mutate func(*qualificationWindowMetrics)
		want   string
	}{
		{"job count", func(m *qualificationWindowMetrics) { m.JobCount = 0 }, "UNKNOWN"},
		{"overlap", func(m *qualificationWindowMetrics) { m.OverlapCount = pi(1) }, "FAILED"},
		{"expected duration", func(m *qualificationWindowMetrics) { m.MeasuredExpected = pi64(43199) }, "UNKNOWN"},
		{"late clip", func(m *qualificationWindowMetrics) { m.LateClip = true }, "UNKNOWN"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := base
			tc.mutate(&m)
			if got, _ := classifyQualificationTimeline(m); got != tc.want {
				t.Fatalf("got=%s want=%s", got, tc.want)
			}
		})
	}
}

func TestQualificationBuildFailsClosedBelowFiftyBeforeDatabase(t *testing.T) {
	ids := make([]int64, 49)
	for i := range ids {
		ids[i] = int64(i + 1)
	}
	_, err := (&Server{}).buildQualificationPlan(context.Background(), 1, qualificationBuildRequest{RecordingIDs: ids, SequenceStart: time.Now()})
	if err == nil || !strings.Contains(err.Error(), "at least 50") {
		t.Fatalf("err=%v", err)
	}
}

func TestQualificationBuildRejectsDuplicateIDsBeforeDatabase(t *testing.T) {
	ids := make([]int64, 50)
	for i := range ids {
		ids[i] = int64(i + 1)
	}
	ids[49] = 1
	_, err := (&Server{}).buildQualificationPlan(context.Background(), 1, qualificationBuildRequest{RecordingIDs: ids, SequenceStart: time.Now()})
	if err == nil || !strings.Contains(err.Error(), "unique") {
		t.Fatalf("err=%v", err)
	}
}

func TestSceneAttestRequiresMemberSession(t *testing.T) {
	for _, principal := range []accountPrincipal{{AccountID: 1, UserID: 0}, {}} {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/account/recordings/qualification/scene-attest", bytes.NewBufferString(`{"recording_id":1,"frame_id":2,"scene_identity":"place"}`))
		if principal.AccountID != 0 {
			req = withPrincipal(req, principal, "")
		}
		rec := httptest.NewRecorder()
		(&Server{}).handleAccountRecordingSceneAttest(rec, req)
		if rec.Code != http.StatusForbidden {
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
		}
	}
}

func TestQualificationBuildRequiresMemberSession(t *testing.T) {
	for _, principal := range []accountPrincipal{{AccountID: 1, UserID: 0}, {}} {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/account/recordings/qualification/build", bytes.NewBufferString(`{"recording_ids":[],"sequence_start_at":"2026-08-13T00:00:00Z","apply":false}`))
		if principal.AccountID != 0 {
			req = withPrincipal(req, principal, "")
		}
		rec := httptest.NewRecorder()
		(&Server{}).handleAccountRecordingQualificationBuild(rec, req)
		if rec.Code != http.StatusForbidden {
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
		}
	}
}

func TestNormalizeSceneIdentityStableAndBounded(t *testing.T) {
	a, err := normalizeSceneIdentity("  Anguk   Station, Seoul ")
	if err != nil {
		t.Fatal(err)
	}
	b, err := normalizeSceneIdentity("anguk station, seoul")
	if err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Fatalf("%q != %q", a, b)
	}
	if _, err := normalizeSceneIdentity(strings.Repeat("x", 241)); err == nil {
		t.Fatal("long identity accepted")
	}
}
