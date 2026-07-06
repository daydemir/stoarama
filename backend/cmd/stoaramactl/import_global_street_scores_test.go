package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestParseStoaramaStreamID(t *testing.T) {
	tests := []struct {
		raw  string
		want int64
		ok   bool
	}{
		{raw: "https://stoarama.com/streams/14303", want: 14303, ok: true},
		{raw: "https://stoarama-api.onrender.com/streams/94", want: 94, ok: true},
		{raw: "https://stoarama.com/api/v1/dashboard/streams/415", want: 415, ok: true},
		{raw: "https://example.com/streams/14303", ok: false},
		{raw: "not a url", ok: false},
	}
	for _, tt := range tests {
		t.Run(tt.raw, func(t *testing.T) {
			got, ok := parseStoaramaStreamID(tt.raw)
			if ok != tt.ok {
				t.Fatalf("ok=%t want %t", ok, tt.ok)
			}
			if got != tt.want {
				t.Fatalf("id=%d want %d", got, tt.want)
			}
		})
	}
}

func TestGSSStreamHostMatchesTarget(t *testing.T) {
	if !gssStreamHostAccepted("stoarama.com", "https://stoarama.com") {
		t.Fatalf("expected stoarama.com to match production target")
	}
	if gssStreamHostAccepted("stoarama-api.onrender.com", "https://stoarama.com") {
		t.Fatalf("onrender host must not match production target")
	}
	if gssStreamHostAccepted("stoarama-api.onrender.com", "https://stoarama-api.onrender.com") {
		t.Fatalf("onrender stream refs must be manual review even when target API is onrender")
	}
}

func TestClassifyGSSCandidate(t *testing.T) {
	tests := []struct {
		name string
		row  gssRow
		want gssCandidateKind
	}{
		{
			name: "existing source",
			row:  gssTestRow(map[string]string{"source": "https://stoarama.com/streams/14303"}),
			want: gssCandidateExistingStream,
		},
		{
			name: "youtube source",
			row:  gssTestRow(map[string]string{"source": "https://youtu.be/CkNeltsc5ps"}),
			want: gssCandidatePlayableURL,
		},
		{
			name: "hls source",
			row:  gssTestRow(map[string]string{"source": "https://example.com/live/playlist.m3u8"}),
			want: gssCandidatePlayableURL,
		},
		{
			name: "page source",
			row:  gssTestRow(map[string]string{"source": "https://www.skylinewebcams.com/en/webcam/example.html"}),
			want: gssCandidatePageURL,
		},
		{
			name: "location url fallback",
			row:  gssTestRow(map[string]string{"location": "https://stoarama.com/streams/7"}),
			want: gssCandidateExistingStream,
		},
		{
			name: "manual",
			row:  gssTestRow(map[string]string{"location": "Town square"}),
			want: gssCandidateManual,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := classifyGSSCandidate(tt.row, "https://stoarama.com")
			if got.Kind != tt.want {
				t.Fatalf("kind=%q want %q", got.Kind, tt.want)
			}
		})
	}
}

func TestClassifyGSSCandidateRejectsMismatchedStoaramaHost(t *testing.T) {
	row := gssTestRow(map[string]string{"source": "https://stoarama-api.onrender.com/streams/94"})
	got := classifyGSSCandidate(row, "https://stoarama.com")
	if got.Kind != gssCandidateManual {
		t.Fatalf("kind=%q want %q", got.Kind, gssCandidateManual)
	}
}

func TestBuildGSSTagsUsesBoundedColumnTags(t *testing.T) {
	row := gssTestRow(map[string]string{
		"country":   "South Korea",
		"collector": "Donghwan (Don)",
		"valid":     "No",
		"comments":  strings.Repeat("long comment ", 20),
	})
	row.List = gssListVittorio
	tags := buildGSSTags(row)
	want := []string{
		"list-vittorio",
		"gss:valid:no",
	}
	for _, tag := range want {
		if !gssContainsString(tags, tag) {
			t.Fatalf("tags missing %q in %#v", tag, tags)
		}
	}
	for _, tag := range tags {
		if tag != "list-vittorio" && tag != "gss:valid:no" {
			t.Fatalf("unexpected high-cardinality tag: %q", tag)
		}
	}
}

func TestGSSSlugIsASCIIAndBounded(t *testing.T) {
	got := gssSlug("São Paulo / Praça Central "+strings.Repeat("abc", 50), 32)
	if got == "" {
		t.Fatalf("slug is empty")
	}
	if len(got) > 32 {
		t.Fatalf("slug length=%d want <=32: %q", len(got), got)
	}
	for _, r := range got {
		if !((r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-') {
			t.Fatalf("slug contains non-ascii slug char %q in %q", r, got)
		}
	}
}

func TestReadGSSCSV(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "gss.csv")
	body := "Continent,Country,CIty,Location,Scale,Collector,Source,Hosted,Valid (Yes / No),Why,Comments\n" +
		"Europe,Italy,Assisi,Town Square,,Nils,https://youtu.be/CkNeltsc5ps,,Yes,street,good\n"
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("write csv: %v", err)
	}
	rows, err := readGSSCSV(gssListNils, path, 0)
	if err != nil {
		t.Fatalf("read csv: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("rows=%d want 1", len(rows))
	}
	if rows[0].RowNumber != 2 {
		t.Fatalf("row number=%d want 2", rows[0].RowNumber)
	}
	if rows[0].value("city") != "Assisi" {
		t.Fatalf("city=%q", rows[0].value("city"))
	}
}

func TestPreflightGSSReportPathCreatesDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "report.json")
	if err := preflightGSSReportPath(path); err != nil {
		t.Fatalf("preflight report path: %v", err)
	}
	if _, err := os.Stat(filepath.Dir(path)); err != nil {
		t.Fatalf("expected report dir: %v", err)
	}
}

func TestLoadApprovedGSSApplyReportAcceptsCleanVerifyReport(t *testing.T) {
	path := filepath.Join(t.TempDir(), "verify.json")
	report := gssTestVerifyReport()
	if err := writeGSSReport(path, report); err != nil {
		t.Fatalf("write report: %v", err)
	}
	got, err := loadApprovedGSSApplyReport(path, gssOptions{TargetAPIURL: gssProductionAPIURL})
	if err != nil {
		t.Fatalf("load approved report: %v", err)
	}
	if got.RowsProcessed != 3 {
		t.Fatalf("rows_processed=%d want 3", got.RowsProcessed)
	}
}

func TestLoadApprovedGSSApplyReportRejectsUnsafeReports(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*gssReport)
	}{
		{
			name: "already applied",
			mutate: func(report *gssReport) {
				report.Apply = true
			},
		},
		{
			name: "target mismatch",
			mutate: func(report *gssReport) {
				report.TargetAPIURL = "https://stoarama-api.onrender.com"
			},
		},
		{
			name: "status count mismatch",
			mutate: func(report *gssReport) {
				report.CountsByStatus[gssStatusVerifiedExisting] = 99
			},
		},
		{
			name: "missing importable probe",
			mutate: func(report *gssReport) {
				report.Results[1].Probe = nil
			},
		},
		{
			name: "prior apply metadata",
			mutate: func(report *gssReport) {
				report.Results[0].Applied = true
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "verify.json")
			report := gssTestVerifyReport()
			tt.mutate(&report)
			if err := writeGSSReport(path, report); err != nil {
				t.Fatalf("write report: %v", err)
			}
			if _, err := loadApprovedGSSApplyReport(path, gssOptions{TargetAPIURL: gssProductionAPIURL}); err == nil {
				t.Fatalf("expected unsafe report rejection")
			}
		})
	}
}

func TestBuildGSSTagCleanupReportRemovesOnlyNonSemanticGSSTags(t *testing.T) {
	source := gssTestVerifyReport()
	source.Apply = true
	source.Results[0].Applied = true
	source.Results[0].AppliedStreamID = 123
	source.Results[0].Tags = []string{
		"list-nils",
		"gss:valid:no",
		"gss:country:italy",
		"gss:location:town-square",
		"legacy-tag",
	}
	source.Results[1].Applied = true
	source.Results[1].AppliedStreamID = 456
	source.Results[1].Tags = []string{
		"list-vittorio",
		"gss:valid:yes",
		"gss:source:https-example-com-live",
	}

	report := buildGSSTagCleanupReport(gssOptions{
		TargetAPIURL:      gssProductionAPIURL,
		CleanupTagsReport: "import.json",
	}, source)
	if report.StreamsToChange != 2 {
		t.Fatalf("streams_to_change=%d want 2", report.StreamsToChange)
	}
	if report.TagsToRemove != 3 {
		t.Fatalf("tags_to_remove=%d want 3", report.TagsToRemove)
	}
	first := report.Items[0]
	if first.StreamID != 123 {
		t.Fatalf("first stream id=%d want 123", first.StreamID)
	}
	for _, removed := range []string{"gss:country:italy", "gss:location:town-square"} {
		if !gssContainsString(first.RemoveTags, removed) {
			t.Fatalf("missing removed tag %q in %#v", removed, first.RemoveTags)
		}
	}
	if !gssContainsString(first.KeepTags, "gss:valid:no") {
		t.Fatalf("valid tag should be kept: %#v", first.KeepTags)
	}
	if gssContainsString(first.RemoveTags, "legacy-tag") {
		t.Fatalf("non-GSS tag must not be removed")
	}
}

func TestLoadApprovedGSSTagCleanupSourceReportRejectsUnsafeReports(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*gssReport)
	}{
		{
			name: "not apply report",
			mutate: func(report *gssReport) {
				report.Apply = false
			},
		},
		{
			name: "not reviewed",
			mutate: func(report *gssReport) {
				report.ReviewApproved = false
			},
		},
		{
			name: "wrong target",
			mutate: func(report *gssReport) {
				report.TargetAPIURL = "https://stoarama-api.onrender.com"
			},
		},
		{
			name: "apply errors",
			mutate: func(report *gssReport) {
				report.ApplyErrors = 1
			},
		},
		{
			name: "bad apply counts",
			mutate: func(report *gssReport) {
				report.CountsByApplyAction[gssApplyTaggedExisting] = 99
			},
		},
		{
			name: "applied non-applyable status",
			mutate: func(report *gssReport) {
				report.Results[2].Applied = true
				report.Results[2].AppliedStreamID = 777
				report.Results[2].ApplyAction = gssApplyTaggedExisting
				report.CountsByApplyAction = gssCountsByApplyAction(report.Results)
			},
		},
		{
			name: "missing applied stream id",
			mutate: func(report *gssReport) {
				report.Results[0].AppliedStreamID = 0
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "apply.json")
			report := gssTestApplyReport()
			tt.mutate(&report)
			if err := writeGSSReport(path, report); err != nil {
				t.Fatalf("write report: %v", err)
			}
			if _, err := loadApprovedGSSTagCleanupSourceReport(path, gssOptions{TargetAPIURL: gssProductionAPIURL}); err == nil {
				t.Fatalf("expected unsafe cleanup report rejection")
			}
		})
	}
}

func TestApplyGSSResultsPersistsCreatedStreamBeforeTagFailure(t *testing.T) {
	reportPath := filepath.Join(t.TempDir(), "report.json")
	var createCalled bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/service/streams/by-external-id":
			writeTestJSON(t, w, map[string]any{"ok": true, "found": false})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/service/streams":
			createCalled = true
			writeTestJSON(t, w, map[string]any{
				"ok":      true,
				"created": true,
				"stream": map[string]any{
					"id":          123,
					"slug":        "gss-created",
					"provider":    gssProvider,
					"external_id": "nils:test",
					"name":        "Created",
					"source_url":  "https://example.com/live/playlist.m3u8",
				},
			})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/service/streams/123/tags":
			http.Error(w, "tag failure", http.StatusInternalServerError)
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
		}
	}))
	defer srv.Close()

	results := []gssResult{{
		Seq:       1,
		List:      gssListNils,
		RowNumber: 2,
		Status:    gssStatusVerifiedImportable,
		SourceURL: "https://example.com/live/playlist.m3u8",
		Tags:      []string{"list-nils"},
		Values: map[string]string{
			"city":     "Assisi",
			"location": "Town Square",
			"country":  "Italy",
		},
	}}
	opts := gssOptions{
		TargetAPIURL:   srv.URL,
		ServiceToken:   "test-token",
		ReportJSON:     reportPath,
		ImportTimeout:  time.Second,
		ReviewApproved: true,
		Apply:          true,
	}
	err := applyGSSResults(context.Background(), opts, 1, results)
	if err == nil {
		t.Fatalf("expected tag failure")
	}
	if !createCalled {
		t.Fatalf("expected create call")
	}
	var report gssReport
	b, readErr := os.ReadFile(reportPath)
	if readErr != nil {
		t.Fatalf("read report: %v", readErr)
	}
	if err := json.Unmarshal(b, &report); err != nil {
		t.Fatalf("decode report: %v", err)
	}
	got := report.Results[0]
	if got.StreamID != 123 || got.AppliedStreamID != 123 || !got.Created {
		t.Fatalf("created stream not persisted in report: %#v", got)
	}
	if got.Applied {
		t.Fatalf("row should not be marked applied after tag failure")
	}
	if got.ApplyError == "" {
		t.Fatalf("expected apply_error in report")
	}
}

func gssTestRow(values map[string]string) gssRow {
	out := gssRow{
		List:      gssListNils,
		RowNumber: 2,
		Values:    map[string]string{},
	}
	for _, col := range gssColumns {
		out.Values[col.Key] = ""
	}
	for k, v := range values {
		out.Values[k] = v
	}
	return out
}

func writeTestJSON(t *testing.T, w http.ResponseWriter, payload any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		t.Fatalf("write json: %v", err)
	}
}

func gssTestVerifyReport() gssReport {
	results := []gssResult{
		{
			Seq:       1,
			List:      gssListNils,
			RowNumber: 2,
			Candidate: gssCandidate{
				Kind:     gssCandidateExistingStream,
				StreamID: 123,
				Host:     "stoarama.com",
				URL:      "https://stoarama.com/streams/123",
			},
			Status:   gssStatusVerifiedExisting,
			StreamID: 123,
			Tags:     []string{"list-nils"},
			Values:   map[string]string{"country": "Italy"},
		},
		{
			Seq:         2,
			List:        gssListVittorio,
			RowNumber:   2,
			Candidate:   gssCandidate{Kind: gssCandidatePlayableURL, URL: "https://example.com/live.m3u8"},
			Status:      gssStatusVerifiedImportable,
			SourceURL:   "https://example.com/live.m3u8",
			ResolvedURL: "https://example.com/live.m3u8",
			Probe:       &gssProbe{ResolvedURL: "https://example.com/live.m3u8", Width: 640, Height: 480, SizeBytes: 1024, SHA256: "abc123"},
			Tags:        []string{"list-vittorio"},
			Values:      map[string]string{"country": "Italy"},
		},
		{
			Seq:       3,
			List:      gssListVittorio,
			RowNumber: 3,
			Candidate: gssCandidate{Kind: gssCandidateManual},
			Status:    gssStatusManualReview,
			Tags:      []string{"list-vittorio"},
			Values:    map[string]string{"country": "Italy"},
		},
	}
	return gssReport{
		TargetAPIURL:        gssProductionAPIURL,
		RowsTotal:           3,
		RowsProcessed:       len(results),
		CountsByStatus:      gssCountsByStatus(results),
		CountsByApplyAction: map[gssApplyAction]int{},
		Results:             results,
	}
}

func gssTestApplyReport() gssReport {
	report := gssTestVerifyReport()
	report.Apply = true
	report.ReviewApproved = true
	report.Results[0].Applied = true
	report.Results[0].AppliedStreamID = 123
	report.Results[0].ApplyAction = gssApplyTaggedExisting
	report.Results[1].Applied = true
	report.Results[1].AppliedStreamID = 456
	report.Results[1].ApplyAction = gssApplyCreatedAndTagged
	report.Results[1].Created = true
	report.CountsByApplyAction = gssCountsByApplyAction(report.Results)
	return report
}
