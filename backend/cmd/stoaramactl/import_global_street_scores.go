package main

import (
	"context"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/daydemir/stoarama/backend/internal/capture"
	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/model"
)

const (
	gssProvider           = "global-street-scores"
	gssProductionAPIURL   = "https://stoarama.com"
	gssDefaultNilsCSV     = "/Users/deniz/Build/Global Street Scores - List_Nils.csv"
	gssDefaultVittorioCSV = "/Users/deniz/Build/Global Street Scores - List_Vittorio.csv"
)

type gssList string

const (
	gssListNils     gssList = "nils"
	gssListVittorio gssList = "vittorio"
)

func (l gssList) Tag() string {
	switch l {
	case gssListNils:
		return "list-nils"
	case gssListVittorio:
		return "list-vittorio"
	default:
		return ""
	}
}

func (l gssList) String() string {
	return string(l)
}

type gssCandidateKind string

const (
	gssCandidateExistingStream gssCandidateKind = "existing_stream"
	gssCandidatePlayableURL    gssCandidateKind = "playable_url"
	gssCandidatePageURL        gssCandidateKind = "page_url"
	gssCandidateManual         gssCandidateKind = "manual"
)

type gssStatus string

const (
	gssStatusVerifiedExisting   gssStatus = "verified_existing"
	gssStatusVerifiedImportable gssStatus = "verified_importable"
	gssStatusResolverMissing    gssStatus = "resolver_missing"
	gssStatusProbeFailed        gssStatus = "probe_failed"
	gssStatusManualReview       gssStatus = "manual_review"
	gssStatusAPIError           gssStatus = "api_error"
)

var gssStatuses = []gssStatus{
	gssStatusVerifiedExisting,
	gssStatusVerifiedImportable,
	gssStatusResolverMissing,
	gssStatusProbeFailed,
	gssStatusManualReview,
	gssStatusAPIError,
}

func (s gssStatus) AllowApply() bool {
	return s == gssStatusVerifiedExisting || s == gssStatusVerifiedImportable
}

type gssApplyAction string

const (
	gssApplyNone                 gssApplyAction = ""
	gssApplyTaggedExisting       gssApplyAction = "tagged_existing"
	gssApplyTaggedExistingSource gssApplyAction = "tagged_existing_source"
	gssApplyCreatedAndTagged     gssApplyAction = "created_and_tagged"
)

type gssColumn struct {
	Key    string
	Header string
}

var gssColumns = []gssColumn{
	{Key: "continent", Header: "Continent"},
	{Key: "country", Header: "Country"},
	{Key: "city", Header: "CIty"},
	{Key: "location", Header: "Location"},
	{Key: "scale", Header: "Scale"},
	{Key: "collector", Header: "Collector"},
	{Key: "source", Header: "Source"},
	{Key: "hosted", Header: "Hosted"},
	{Key: "valid", Header: "Valid (Yes / No)"},
	{Key: "why", Header: "Why"},
	{Key: "comments", Header: "Comments"},
}

type gssRow struct {
	Seq       int               `json:"seq"`
	List      gssList           `json:"list"`
	RowNumber int               `json:"row_number"`
	Values    map[string]string `json:"values"`
}

func (r gssRow) value(key string) string {
	return strings.TrimSpace(r.Values[key])
}

type gssCandidate struct {
	Kind     gssCandidateKind `json:"kind"`
	StreamID int64            `json:"stream_id,omitempty"`
	Host     string           `json:"host,omitempty"`
	URL      string           `json:"url,omitempty"`
	Reason   string           `json:"reason,omitempty"`
}

type gssProbe struct {
	ResolvedURL string `json:"resolved_url,omitempty"`
	Width       int    `json:"width,omitempty"`
	Height      int    `json:"height,omitempty"`
	SizeBytes   int64  `json:"size_bytes,omitempty"`
	SHA256      string `json:"sha256,omitempty"`
}

type gssResult struct {
	Seq             int               `json:"seq"`
	List            gssList           `json:"list"`
	RowNumber       int               `json:"row_number"`
	Candidate       gssCandidate      `json:"candidate"`
	Status          gssStatus         `json:"status"`
	Reason          string            `json:"reason,omitempty"`
	StreamID        int64             `json:"stream_id,omitempty"`
	StreamSlug      string            `json:"stream_slug,omitempty"`
	SourceURL       string            `json:"source_url,omitempty"`
	ResolvedURL     string            `json:"resolved_url,omitempty"`
	Probe           *gssProbe         `json:"probe,omitempty"`
	Tags            []string          `json:"tags"`
	Values          map[string]string `json:"values"`
	ApplyAction     gssApplyAction    `json:"apply_action,omitempty"`
	Applied         bool              `json:"applied"`
	AppliedStreamID int64             `json:"applied_stream_id,omitempty"`
	Created         bool              `json:"created"`
	ApplyError      string            `json:"apply_error,omitempty"`
	StartedAt       time.Time         `json:"started_at"`
	FinishedAt      time.Time         `json:"finished_at"`
	DurationMs      int64             `json:"duration_ms"`
}

type gssReport struct {
	TargetAPIURL        string                 `json:"target_api_url"`
	Apply               bool                   `json:"apply"`
	ReviewApproved      bool                   `json:"review_approved"`
	GeneratedAt         time.Time              `json:"generated_at"`
	NilsCSV             string                 `json:"nils_csv"`
	VittorioCSV         string                 `json:"vittorio_csv"`
	Limit               int                    `json:"limit"`
	Concurrency         int                    `json:"concurrency"`
	ProbeTimeout        string                 `json:"probe_timeout"`
	ApplySourceReport   string                 `json:"apply_source_report,omitempty"`
	RowsTotal           int                    `json:"rows_total"`
	RowsProcessed       int                    `json:"rows_processed"`
	CountsByStatus      map[gssStatus]int      `json:"counts_by_status"`
	CountsByApplyAction map[gssApplyAction]int `json:"counts_by_apply_action"`
	ApplyErrors         int                    `json:"apply_errors"`
	Results             []gssResult            `json:"results"`
}

type gssTagCleanupItem struct {
	StreamID   int64    `json:"stream_id"`
	ListTags   []string `json:"list_tags"`
	RemoveTags []string `json:"remove_tags"`
	KeepTags   []string `json:"keep_tags"`
	RowRefs    []string `json:"row_refs"`
	Applied    bool     `json:"applied"`
	Error      string   `json:"error,omitempty"`
}

type gssTagCleanupReport struct {
	TargetAPIURL    string              `json:"target_api_url"`
	Apply           bool                `json:"apply"`
	ReviewApproved  bool                `json:"review_approved"`
	GeneratedAt     time.Time           `json:"generated_at"`
	SourceReport    string              `json:"source_report"`
	Policy          string              `json:"policy"`
	StreamsToChange int                 `json:"streams_to_change"`
	TagsToRemove    int                 `json:"tags_to_remove"`
	TagsToKeep      int                 `json:"tags_to_keep"`
	AppliedStreams  int                 `json:"applied_streams"`
	Errors          int                 `json:"errors"`
	Items           []gssTagCleanupItem `json:"items"`
}

type gssOptions struct {
	TargetAPIURL      string
	ServiceToken      string
	NilsCSV           string
	VittorioCSV       string
	Limit             int
	Concurrency       int
	ProbeTimeout      time.Duration
	ImportTimeout     time.Duration
	Apply             bool
	ReviewApproved    bool
	ReportJSON        string
	ApplyReport       string
	CleanupTagsReport string
	AsJSON            bool
}

func runImportGlobalStreetScores(ctx context.Context, cfg config.Config, args []string) {
	fs := flag.NewFlagSet("global-street-scores", flag.ExitOnError)
	targetAPIURL := fs.String("target-api-url", defaultBackendAPIURL(), "target Stoarama API base URL")
	serviceToken := fs.String("service-token", cfg.ServiceToken, "target Stoarama service token")
	nilsCSV := fs.String("nils-csv", gssDefaultNilsCSV, "Global Street Scores Nils CSV path")
	vittorioCSV := fs.String("vittorio-csv", gssDefaultVittorioCSV, "Global Street Scores Vittorio CSV path")
	limit := fs.Int("limit", 0, "maximum rows to process after parsing both CSVs (0 means all)")
	concurrency := fs.Int("concurrency", 8, "verification/import worker concurrency")
	probeTimeoutSec := fs.Int("probe-timeout-sec", 60, "per-row probe timeout seconds")
	importTimeoutSec := fs.Int("import-timeout-sec", 30, "per-row import/tag request timeout seconds")
	apply := fs.Bool("apply", false, "apply verified imports/tags to the target Stoarama API")
	reviewApproved := fs.Bool("review-approved", false, "required with --apply after review-agent approval")
	reportJSON := fs.String("report-json", "local/reports/global-street-scores-import-report.json", "report JSON path")
	applyReport := fs.String("apply-report", "", "apply from a previously approved verify-only report instead of re-probing rows")
	cleanupTagsReport := fs.String("cleanup-tags-report", "", "remove non-useful GSS tags from streams in a completed import report")
	asJSON := fs.Bool("json", false, "print JSON report")
	_ = fs.Parse(args)

	opts := gssOptions{
		TargetAPIURL:      strings.TrimRight(strings.TrimSpace(*targetAPIURL), "/"),
		ServiceToken:      strings.TrimSpace(*serviceToken),
		NilsCSV:           strings.TrimSpace(*nilsCSV),
		VittorioCSV:       strings.TrimSpace(*vittorioCSV),
		Limit:             *limit,
		Concurrency:       *concurrency,
		ProbeTimeout:      time.Duration(*probeTimeoutSec) * time.Second,
		ImportTimeout:     time.Duration(*importTimeoutSec) * time.Second,
		Apply:             *apply,
		ReviewApproved:    *reviewApproved,
		ReportJSON:        strings.TrimSpace(*reportJSON),
		ApplyReport:       strings.TrimSpace(*applyReport),
		CleanupTagsReport: strings.TrimSpace(*cleanupTagsReport),
		AsJSON:            *asJSON,
	}
	if err := validateGSSOptions(opts); err != nil {
		log.Fatalf("global-street-scores: %v", err)
	}
	if opts.ApplyReport != "" {
		if err := applyGSSReport(ctx, opts); err != nil {
			log.Fatalf("global-street-scores apply report: %v", err)
		}
		return
	}
	if opts.CleanupTagsReport != "" {
		if err := cleanupGSSTags(ctx, opts); err != nil {
			log.Fatalf("global-street-scores cleanup tags: %v", err)
		}
		return
	}

	rows, err := loadGSSRows(opts.NilsCSV, opts.VittorioCSV)
	if err != nil {
		log.Fatalf("load Global Street Scores rows: %v", err)
	}
	rowsTotal := len(rows)
	if opts.Limit > 0 && len(rows) > opts.Limit {
		rows = rows[:opts.Limit]
	}
	if opts.Apply && opts.ReportJSON != "" {
		if err := preflightGSSReportPath(opts.ReportJSON); err != nil {
			log.Fatalf("preflight Global Street Scores report path: %v", err)
		}
	}

	results := processGSSRows(ctx, rows, opts)
	if opts.ReportJSON != "" {
		if err := writeGSSReport(opts.ReportJSON, buildGSSReport(opts, rowsTotal, results)); err != nil {
			log.Fatalf("write Global Street Scores pre-apply report: %v", err)
		}
	}
	var applyErr error
	if opts.Apply {
		applyErr = applyGSSResults(ctx, opts, rowsTotal, results)
	}
	report := buildGSSReport(opts, rowsTotal, results)
	if opts.ReportJSON != "" {
		if err := writeGSSReport(opts.ReportJSON, report); err != nil {
			log.Fatalf("write Global Street Scores report: %v", err)
		}
	}
	if applyErr != nil {
		log.Fatalf("apply Global Street Scores results: %v; report=%s", applyErr, opts.ReportJSON)
	}
	if opts.AsJSON {
		printJSON(report)
		return
	}
	fmt.Printf("Global Street Scores: rows=%d verified_existing=%d verified_importable=%d resolver_missing=%d probe_failed=%d manual_review=%d api_error=%d applied=%d apply_errors=%d report=%s\n",
		report.RowsProcessed,
		report.CountsByStatus[gssStatusVerifiedExisting],
		report.CountsByStatus[gssStatusVerifiedImportable],
		report.CountsByStatus[gssStatusResolverMissing],
		report.CountsByStatus[gssStatusProbeFailed],
		report.CountsByStatus[gssStatusManualReview],
		report.CountsByStatus[gssStatusAPIError],
		report.CountsByApplyAction[gssApplyTaggedExisting]+report.CountsByApplyAction[gssApplyTaggedExistingSource]+report.CountsByApplyAction[gssApplyCreatedAndTagged],
		report.ApplyErrors,
		opts.ReportJSON,
	)
}

func validateGSSOptions(opts gssOptions) error {
	if opts.TargetAPIURL == "" {
		return fmt.Errorf("--target-api-url is required")
	}
	if opts.NilsCSV == "" {
		return fmt.Errorf("--nils-csv is required")
	}
	if opts.VittorioCSV == "" {
		return fmt.Errorf("--vittorio-csv is required")
	}
	if opts.Concurrency <= 0 {
		return fmt.Errorf("--concurrency must be > 0")
	}
	if opts.ProbeTimeout <= 0 {
		return fmt.Errorf("--probe-timeout-sec must be > 0")
	}
	if opts.ImportTimeout <= 0 {
		return fmt.Errorf("--import-timeout-sec must be > 0")
	}
	if opts.Apply && opts.ServiceToken == "" {
		return fmt.Errorf("--service-token is required with --apply")
	}
	if opts.Apply && !opts.ReviewApproved {
		return fmt.Errorf("--review-approved is required with --apply")
	}
	if opts.Apply && opts.TargetAPIURL != gssProductionAPIURL {
		return fmt.Errorf("--target-api-url must be %s with --apply", gssProductionAPIURL)
	}
	if opts.ApplyReport != "" && !opts.Apply {
		return fmt.Errorf("--apply-report requires --apply")
	}
	if opts.ApplyReport != "" && opts.CleanupTagsReport != "" {
		return fmt.Errorf("--apply-report and --cleanup-tags-report cannot be combined")
	}
	return nil
}

func applyGSSReport(ctx context.Context, opts gssOptions) error {
	if opts.ReportJSON != "" {
		if err := preflightGSSReportPath(opts.ReportJSON); err != nil {
			return fmt.Errorf("preflight Global Street Scores report path: %w", err)
		}
	}
	source, err := loadApprovedGSSApplyReport(opts.ApplyReport, opts)
	if err != nil {
		return err
	}
	results := append([]gssResult(nil), source.Results...)
	if opts.ReportJSON != "" {
		if err := writeGSSReport(opts.ReportJSON, buildGSSReport(opts, source.RowsTotal, results)); err != nil {
			return fmt.Errorf("write Global Street Scores pre-apply report: %w", err)
		}
	}
	applyErr := applyGSSResults(ctx, opts, source.RowsTotal, results)
	report := buildGSSReport(opts, source.RowsTotal, results)
	if opts.ReportJSON != "" {
		if err := writeGSSReport(opts.ReportJSON, report); err != nil {
			return fmt.Errorf("write Global Street Scores report: %w", err)
		}
	}
	if applyErr != nil {
		return fmt.Errorf("%w; report=%s", applyErr, opts.ReportJSON)
	}
	if opts.AsJSON {
		printJSON(report)
		return nil
	}
	fmt.Printf("Global Street Scores: rows=%d verified_existing=%d verified_importable=%d resolver_missing=%d probe_failed=%d manual_review=%d api_error=%d applied=%d apply_errors=%d report=%s\n",
		report.RowsProcessed,
		report.CountsByStatus[gssStatusVerifiedExisting],
		report.CountsByStatus[gssStatusVerifiedImportable],
		report.CountsByStatus[gssStatusResolverMissing],
		report.CountsByStatus[gssStatusProbeFailed],
		report.CountsByStatus[gssStatusManualReview],
		report.CountsByStatus[gssStatusAPIError],
		report.CountsByApplyAction[gssApplyTaggedExisting]+report.CountsByApplyAction[gssApplyTaggedExistingSource]+report.CountsByApplyAction[gssApplyCreatedAndTagged],
		report.ApplyErrors,
		opts.ReportJSON,
	)
	return nil
}

func loadApprovedGSSApplyReport(path string, opts gssOptions) (gssReport, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return gssReport{}, err
	}
	var report gssReport
	if err := json.Unmarshal(b, &report); err != nil {
		return gssReport{}, err
	}
	report.TargetAPIURL = strings.TrimRight(strings.TrimSpace(report.TargetAPIURL), "/")
	if report.Apply {
		return gssReport{}, fmt.Errorf("--apply-report must point to a verify-only report")
	}
	if report.ReviewApproved {
		return gssReport{}, fmt.Errorf("--apply-report must not be a prior reviewed apply report")
	}
	if report.ApplyErrors != 0 {
		return gssReport{}, fmt.Errorf("--apply-report has apply_errors=%d", report.ApplyErrors)
	}
	if report.TargetAPIURL != opts.TargetAPIURL {
		return gssReport{}, fmt.Errorf("--apply-report target_api_url=%q does not match --target-api-url=%q", report.TargetAPIURL, opts.TargetAPIURL)
	}
	if report.RowsTotal <= 0 {
		return gssReport{}, fmt.Errorf("--apply-report rows_total must be > 0")
	}
	if report.RowsProcessed != len(report.Results) {
		return gssReport{}, fmt.Errorf("--apply-report rows_processed=%d but results=%d", report.RowsProcessed, len(report.Results))
	}
	if report.RowsProcessed > report.RowsTotal {
		return gssReport{}, fmt.Errorf("--apply-report rows_processed cannot exceed rows_total")
	}
	if report.CountsByStatus[gssStatusAPIError] != 0 {
		return gssReport{}, fmt.Errorf("--apply-report contains api_error rows")
	}
	expectedCounts := gssCountsByStatus(report.Results)
	if !gssStatusCountsEqual(report.CountsByStatus, expectedCounts) {
		return gssReport{}, fmt.Errorf("--apply-report status counts do not match results")
	}
	for i := range report.Results {
		if err := validateGSSApplyReportResult(report.Results[i]); err != nil {
			return gssReport{}, fmt.Errorf("--apply-report result %d row %d: %w", i, report.Results[i].RowNumber, err)
		}
	}
	return report, nil
}

func cleanupGSSTags(ctx context.Context, opts gssOptions) error {
	source, err := loadApprovedGSSTagCleanupSourceReport(opts.CleanupTagsReport, opts)
	if err != nil {
		return err
	}
	report := buildGSSTagCleanupReport(opts, source)
	if opts.ReportJSON != "" {
		if err := preflightGSSReportPath(opts.ReportJSON); err != nil {
			return fmt.Errorf("preflight Global Street Scores cleanup report path: %w", err)
		}
		if err := writeGSSTagCleanupReport(opts.ReportJSON, report); err != nil {
			return fmt.Errorf("write Global Street Scores cleanup pre-apply report: %w", err)
		}
	}
	if opts.Apply {
		for i := range report.Items {
			if len(report.Items[i].RemoveTags) == 0 {
				continue
			}
			if err := removeGSSStreamTags(ctx, opts, report.Items[i].StreamID, report.Items[i].RemoveTags); err != nil {
				report.Items[i].Error = err.Error()
				report.Errors++
				_ = writeGSSTagCleanupReport(opts.ReportJSON, report)
				return fmt.Errorf("remove tags from stream %d: %w", report.Items[i].StreamID, err)
			}
			report.Items[i].Applied = true
			report.AppliedStreams++
			if opts.ReportJSON != "" {
				if err := writeGSSTagCleanupReport(opts.ReportJSON, report); err != nil {
					return fmt.Errorf("write Global Street Scores cleanup report after stream %d: %w", report.Items[i].StreamID, err)
				}
			}
		}
	}
	report.GeneratedAt = time.Now().UTC()
	if opts.ReportJSON != "" {
		if err := writeGSSTagCleanupReport(opts.ReportJSON, report); err != nil {
			return fmt.Errorf("write Global Street Scores cleanup report: %w", err)
		}
	}
	if opts.AsJSON {
		printJSON(report)
		return nil
	}
	fmt.Printf("Global Street Scores tag cleanup: streams_to_change=%d tags_to_remove=%d tags_to_keep=%d applied_streams=%d errors=%d report=%s\n",
		report.StreamsToChange,
		report.TagsToRemove,
		report.TagsToKeep,
		report.AppliedStreams,
		report.Errors,
		opts.ReportJSON,
	)
	return nil
}

func loadGSSReport(path string) (gssReport, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return gssReport{}, err
	}
	var report gssReport
	if err := json.Unmarshal(b, &report); err != nil {
		return gssReport{}, err
	}
	return report, nil
}

func loadApprovedGSSTagCleanupSourceReport(path string, opts gssOptions) (gssReport, error) {
	report, err := loadGSSReport(path)
	if err != nil {
		return gssReport{}, err
	}
	report.TargetAPIURL = strings.TrimRight(strings.TrimSpace(report.TargetAPIURL), "/")
	if !report.Apply {
		return gssReport{}, fmt.Errorf("--cleanup-tags-report must point to an apply report")
	}
	if !report.ReviewApproved {
		return gssReport{}, fmt.Errorf("--cleanup-tags-report must be review approved")
	}
	if report.TargetAPIURL != opts.TargetAPIURL {
		return gssReport{}, fmt.Errorf("--cleanup-tags-report target_api_url=%q does not match --target-api-url=%q", report.TargetAPIURL, opts.TargetAPIURL)
	}
	if report.TargetAPIURL != gssProductionAPIURL {
		return gssReport{}, fmt.Errorf("--cleanup-tags-report target_api_url must be %s", gssProductionAPIURL)
	}
	if report.ApplyErrors != 0 {
		return gssReport{}, fmt.Errorf("--cleanup-tags-report has apply_errors=%d", report.ApplyErrors)
	}
	if report.RowsTotal <= 0 {
		return gssReport{}, fmt.Errorf("--cleanup-tags-report rows_total must be > 0")
	}
	if report.RowsProcessed != len(report.Results) {
		return gssReport{}, fmt.Errorf("--cleanup-tags-report rows_processed=%d but results=%d", report.RowsProcessed, len(report.Results))
	}
	if report.RowsProcessed > report.RowsTotal {
		return gssReport{}, fmt.Errorf("--cleanup-tags-report rows_processed cannot exceed rows_total")
	}
	expectedStatusCounts := gssCountsByStatus(report.Results)
	if !gssStatusCountsEqual(report.CountsByStatus, expectedStatusCounts) {
		return gssReport{}, fmt.Errorf("--cleanup-tags-report status counts do not match results")
	}
	expectedApplyCounts := gssCountsByApplyAction(report.Results)
	if !gssApplyActionCountsEqual(report.CountsByApplyAction, expectedApplyCounts) {
		return gssReport{}, fmt.Errorf("--cleanup-tags-report apply action counts do not match results")
	}
	for i := range report.Results {
		if err := validateGSSTagCleanupSourceResult(report.Results[i]); err != nil {
			return gssReport{}, fmt.Errorf("--cleanup-tags-report result %d row %d: %w", i, report.Results[i].RowNumber, err)
		}
	}
	return report, nil
}

func validateGSSTagCleanupSourceResult(result gssResult) error {
	if result.ApplyError != "" {
		return fmt.Errorf("contains apply_error")
	}
	if !result.Applied {
		if result.ApplyAction != gssApplyNone || result.AppliedStreamID != 0 || result.Created {
			return fmt.Errorf("unapplied result contains apply metadata")
		}
		return nil
	}
	if !result.Status.AllowApply() {
		return fmt.Errorf("applied result has non-applyable status %q", result.Status)
	}
	if result.AppliedStreamID <= 0 {
		return fmt.Errorf("applied result has no applied_stream_id")
	}
	switch result.ApplyAction {
	case gssApplyTaggedExisting, gssApplyTaggedExistingSource, gssApplyCreatedAndTagged:
	default:
		return fmt.Errorf("applied result has invalid apply_action %q", result.ApplyAction)
	}
	if !gssContainsString(result.Tags, result.List.Tag()) {
		return fmt.Errorf("missing list tag %q", result.List.Tag())
	}
	return nil
}

func buildGSSTagCleanupReport(opts gssOptions, source gssReport) gssTagCleanupReport {
	itemsByStream := map[int64]*gssTagCleanupItem{}
	for _, result := range source.Results {
		if !result.Applied {
			continue
		}
		streamID := result.AppliedStreamID
		if streamID <= 0 {
			streamID = result.StreamID
		}
		if streamID <= 0 {
			continue
		}
		item := itemsByStream[streamID]
		if item == nil {
			item = &gssTagCleanupItem{StreamID: streamID}
			itemsByStream[streamID] = item
		}
		item.RowRefs = append(item.RowRefs, fmt.Sprintf("%s:%d", result.List.String(), result.RowNumber))
		for _, tag := range result.Tags {
			switch {
			case tag == gssListNils.Tag() || tag == gssListVittorio.Tag():
				item.ListTags = append(item.ListTags, tag)
			case strings.HasPrefix(tag, "gss:") && keepGSSSemanticTag(tag):
				item.KeepTags = append(item.KeepTags, tag)
			case strings.HasPrefix(tag, "gss:"):
				item.RemoveTags = append(item.RemoveTags, tag)
			}
		}
	}
	items := make([]gssTagCleanupItem, 0, len(itemsByStream))
	for _, item := range itemsByStream {
		item.ListTags = normalizeTags(item.ListTags)
		item.KeepTags = normalizeTags(item.KeepTags)
		item.RemoveTags = normalizeTags(item.RemoveTags)
		item.RowRefs = normalizeTags(item.RowRefs)
		if len(item.RemoveTags) == 0 {
			continue
		}
		items = append(items, *item)
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].StreamID < items[j].StreamID
	})
	report := gssTagCleanupReport{
		TargetAPIURL:   opts.TargetAPIURL,
		Apply:          opts.Apply,
		ReviewApproved: opts.ReviewApproved,
		GeneratedAt:    time.Now().UTC(),
		SourceReport:   opts.CleanupTagsReport,
		Policy:         "remove imported gss:* tags except gss:valid:yes and gss:valid:no; preserve list tags and non-GSS tags",
		Items:          items,
	}
	report.StreamsToChange = len(items)
	for _, item := range items {
		report.TagsToRemove += len(item.RemoveTags)
		report.TagsToKeep += len(item.KeepTags)
	}
	return report
}

func keepGSSSemanticTag(tag string) bool {
	return tag == "gss:valid:yes" || tag == "gss:valid:no"
}

func validateGSSApplyReportResult(result gssResult) error {
	if result.ApplyAction != gssApplyNone || result.Applied || result.AppliedStreamID != 0 || result.Created || result.ApplyError != "" {
		return fmt.Errorf("contains prior apply metadata")
	}
	if !result.Status.AllowApply() {
		return nil
	}
	if !gssContainsString(result.Tags, result.List.Tag()) {
		return fmt.Errorf("missing list tag %q", result.List.Tag())
	}
	switch result.Status {
	case gssStatusVerifiedExisting:
		if result.StreamID <= 0 {
			return fmt.Errorf("verified existing stream has no stream_id")
		}
		if result.Candidate.Host != "" && !gssStreamHostAccepted(result.Candidate.Host, gssProductionAPIURL) {
			return fmt.Errorf("verified existing stream host %q is not production", result.Candidate.Host)
		}
	case gssStatusVerifiedImportable:
		if strings.TrimSpace(result.SourceURL) == "" {
			return fmt.Errorf("verified importable stream has no source_url")
		}
		if strings.TrimSpace(result.ResolvedURL) == "" {
			return fmt.Errorf("verified importable stream has no resolved_url")
		}
		if result.Probe == nil || result.Probe.Width <= 0 || result.Probe.Height <= 0 || result.Probe.SizeBytes <= 0 || strings.TrimSpace(result.Probe.SHA256) == "" {
			return fmt.Errorf("verified importable stream has no usable probe")
		}
	}
	return nil
}

func gssCountsByStatus(results []gssResult) map[gssStatus]int {
	out := map[gssStatus]int{}
	for _, result := range results {
		out[result.Status]++
	}
	return out
}

func gssCountsByApplyAction(results []gssResult) map[gssApplyAction]int {
	out := map[gssApplyAction]int{}
	for _, result := range results {
		if result.ApplyAction != gssApplyNone {
			out[result.ApplyAction]++
		}
	}
	return out
}

func gssApplyActionCountsEqual(a map[gssApplyAction]int, b map[gssApplyAction]int) bool {
	for _, action := range []gssApplyAction{
		gssApplyTaggedExisting,
		gssApplyTaggedExistingSource,
		gssApplyCreatedAndTagged,
	} {
		if a[action] != b[action] {
			return false
		}
	}
	for action, count := range a {
		if count != 0 {
			switch action {
			case gssApplyTaggedExisting, gssApplyTaggedExistingSource, gssApplyCreatedAndTagged:
			default:
				return false
			}
		}
	}
	return true
}

func gssStatusCountsEqual(a map[gssStatus]int, b map[gssStatus]int) bool {
	for _, status := range gssStatuses {
		if a[status] != b[status] {
			return false
		}
	}
	for status, count := range a {
		if count != 0 {
			known := false
			for _, want := range gssStatuses {
				if status == want {
					known = true
					break
				}
			}
			if !known {
				return false
			}
		}
	}
	return true
}

func gssContainsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func loadGSSRows(nilsPath string, vittorioPath string) ([]gssRow, error) {
	rows := make([]gssRow, 0, 4096)
	nils, err := readGSSCSV(gssListNils, nilsPath, len(rows))
	if err != nil {
		return nil, err
	}
	rows = append(rows, nils...)
	vittorio, err := readGSSCSV(gssListVittorio, vittorioPath, len(rows))
	if err != nil {
		return nil, err
	}
	rows = append(rows, vittorio...)
	return rows, nil
}

func readGSSCSV(list gssList, path string, seqOffset int) ([]gssRow, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := csv.NewReader(f)
	r.FieldsPerRecord = -1
	headers, err := r.Read()
	if err != nil {
		return nil, fmt.Errorf("%s: read header: %w", path, err)
	}
	headerIndex := map[string]int{}
	for i, h := range headers {
		h = strings.TrimSpace(h)
		if h == "" {
			continue
		}
		if _, exists := headerIndex[h]; !exists {
			headerIndex[h] = i
		}
	}
	for _, col := range gssColumns {
		if _, ok := headerIndex[col.Header]; !ok {
			if col.Header == "Valid (Yes / No)" || col.Header == "Why" || col.Header == "Comments" {
				continue
			}
			return nil, fmt.Errorf("%s: missing required header %q", path, col.Header)
		}
	}

	rows := make([]gssRow, 0, 512)
	line := 1
	for {
		record, err := r.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("%s: read row %d: %w", path, line+1, err)
		}
		line++
		values := map[string]string{}
		nonBlank := false
		for _, col := range gssColumns {
			idx, ok := headerIndex[col.Header]
			if !ok || idx >= len(record) {
				continue
			}
			v := normalizeGSSText(record[idx])
			if v != "" {
				nonBlank = true
			}
			values[col.Key] = v
		}
		if !nonBlank {
			continue
		}
		rows = append(rows, gssRow{
			Seq:       seqOffset + len(rows) + 1,
			List:      list,
			RowNumber: line,
			Values:    values,
		})
	}
	return rows, nil
}

func processGSSRows(ctx context.Context, rows []gssRow, opts gssOptions) []gssResult {
	results := make([]gssResult, len(rows))
	sem := make(chan struct{}, opts.Concurrency)
	var wg sync.WaitGroup
	for i := range rows {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			results[i] = verifyGSSRow(ctx, rows[i], opts)
		}()
	}
	wg.Wait()
	sort.Slice(results, func(i, j int) bool {
		return results[i].Seq < results[j].Seq
	})
	return results
}

func verifyGSSRow(ctx context.Context, row gssRow, opts gssOptions) (result gssResult) {
	start := time.Now().UTC()
	result = gssResult{
		Seq:       row.Seq,
		List:      row.List,
		RowNumber: row.RowNumber,
		Candidate: classifyGSSCandidate(row, opts.TargetAPIURL),
		Tags:      buildGSSTags(row),
		Values:    row.Values,
		StartedAt: start,
	}
	defer func() {
		result.FinishedAt = time.Now().UTC()
		result.DurationMs = result.FinishedAt.Sub(start).Milliseconds()
	}()

	switch result.Candidate.Kind {
	case gssCandidateExistingStream:
		verifyCtx, cancel := context.WithTimeout(ctx, opts.ProbeTimeout)
		defer cancel()
		stream, err := fetchGSSStreamDetail(verifyCtx, opts.TargetAPIURL, result.Candidate.StreamID)
		if err != nil {
			result.Status = gssStatusAPIError
			result.Reason = err.Error()
			return result
		}
		result.StreamID = stream.ID
		result.StreamSlug = stream.Slug
		result.SourceURL = stream.SourceURL
		resolved, err := fetchGSSResolvedURL(verifyCtx, opts.TargetAPIURL, stream.ID)
		if err != nil {
			result.Status = gssStatusProbeFailed
			result.Reason = err.Error()
			return result
		}
		result.ResolvedURL = resolved
		if probe, err := probeGSSResolvedURL(verifyCtx, resolved); err != nil {
			result.Status = gssStatusProbeFailed
			result.Reason = err.Error()
			return result
		} else {
			result.Probe = probe
		}
		result.Status = gssStatusVerifiedExisting
		return result
	case gssCandidatePlayableURL:
		verifyCtx, cancel := context.WithTimeout(ctx, opts.ProbeTimeout)
		defer cancel()
		resolved, _, err := capture.ResolveCaptureInput(verifyCtx, gssProvider, result.Candidate.URL, "")
		if err != nil {
			result.Status = gssStatusProbeFailed
			result.Reason = err.Error()
			return result
		}
		result.SourceURL = result.Candidate.URL
		result.ResolvedURL = resolved
		if probe, err := probeGSSResolvedURL(verifyCtx, resolved); err != nil {
			result.Status = gssStatusProbeFailed
			result.Reason = err.Error()
			return result
		} else {
			result.Probe = probe
		}
		result.Status = gssStatusVerifiedImportable
		return result
	case gssCandidatePageURL:
		result.Status = gssStatusResolverMissing
		result.Reason = result.Candidate.Reason
		return result
	case gssCandidateManual:
		result.Status = gssStatusManualReview
		result.Reason = result.Candidate.Reason
		return result
	default:
		result.Status = gssStatusManualReview
		result.Reason = "unknown candidate kind"
		return result
	}
}

func classifyGSSCandidate(row gssRow, targetAPIURL string) gssCandidate {
	if ref, ok := parseStoaramaStreamRef(row.value("source")); ok {
		if !gssStreamHostAccepted(ref.Host, targetAPIURL) {
			return gssCandidate{Kind: gssCandidateManual, URL: row.value("source"), Reason: "stoarama stream host does not match target API host"}
		}
		return gssCandidate{Kind: gssCandidateExistingStream, StreamID: ref.ID, Host: ref.Host}
	}
	source := row.value("source")
	if source != "" {
		if !isHTTPishURL(source) {
			return gssCandidate{Kind: gssCandidateManual, Reason: "source is not a URL"}
		}
		if isPlayableGSSReference(source) {
			return gssCandidate{Kind: gssCandidatePlayableURL, URL: source}
		}
		return gssCandidate{Kind: gssCandidatePageURL, URL: source, Reason: "source URL has no supported direct stream resolver"}
	}

	location := row.value("location")
	if ref, ok := parseStoaramaStreamRef(location); ok {
		if !gssStreamHostAccepted(ref.Host, targetAPIURL) {
			return gssCandidate{Kind: gssCandidateManual, URL: location, Reason: "stoarama stream host does not match target API host"}
		}
		return gssCandidate{Kind: gssCandidateExistingStream, StreamID: ref.ID, Host: ref.Host}
	}
	if isHTTPishURL(location) {
		if isPlayableGSSReference(location) {
			return gssCandidate{Kind: gssCandidatePlayableURL, URL: location}
		}
		return gssCandidate{Kind: gssCandidatePageURL, URL: location, Reason: "location URL has no supported direct stream resolver"}
	}
	return gssCandidate{Kind: gssCandidateManual, Reason: "no source URL"}
}

type gssStreamRef struct {
	ID   int64
	Host string
}

func parseStoaramaStreamID(raw string) (int64, bool) {
	ref, ok := parseStoaramaStreamRef(raw)
	return ref.ID, ok
}

func parseStoaramaStreamRef(raw string) (gssStreamRef, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return gssStreamRef{}, false
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return gssStreamRef{}, false
	}
	host := strings.ToLower(u.Hostname())
	if host != "stoarama.com" && host != "www.stoarama.com" && host != "stoarama-api.onrender.com" {
		return gssStreamRef{}, false
	}
	parts := strings.Split(strings.Trim(u.Path, "/"), "/")
	for i := 0; i+1 < len(parts); i++ {
		if parts[i] == "streams" {
			id, err := strconv.ParseInt(parts[i+1], 10, 64)
			return gssStreamRef{ID: id, Host: host}, err == nil && id > 0
		}
	}
	return gssStreamRef{}, false
}

func gssStreamHostAccepted(streamHost string, targetAPIURL string) bool {
	u, err := url.Parse(strings.TrimSpace(targetAPIURL))
	if err != nil || u.Host == "" {
		return false
	}
	targetHost := strings.ToLower(u.Hostname())
	streamHost = strings.ToLower(strings.TrimSpace(streamHost))
	if targetHost == "www.stoarama.com" {
		targetHost = "stoarama.com"
	}
	if streamHost == "www.stoarama.com" {
		streamHost = "stoarama.com"
	}
	if streamHost == "stoarama-api.onrender.com" {
		return false
	}
	return streamHost == targetHost
}

func isHTTPishURL(raw string) bool {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return false
	}
	return (u.Scheme == "http" || u.Scheme == "https" || u.Scheme == "rtsp" || u.Scheme == "rtmp") && u.Host != ""
}

func isPlayableGSSReference(raw string) bool {
	v := strings.ToLower(strings.TrimSpace(raw))
	if strings.HasPrefix(v, "rtsp://") || strings.HasPrefix(v, "rtmp://") {
		return true
	}
	if strings.Contains(v, "!hls") || strings.Contains(v, ".m3u8") || strings.Contains(v, ".mpd") {
		return true
	}
	if strings.Contains(v, "youtube.com/") || strings.Contains(v, "youtu.be/") {
		return true
	}
	u, err := url.Parse(v)
	if err != nil {
		return false
	}
	path := strings.ToLower(u.Path)
	for _, ext := range []string{".mp4", ".webm", ".mov", ".mjpeg", ".mjpg", ".jpg", ".jpeg", ".png"} {
		if strings.HasSuffix(path, ext) {
			return true
		}
	}
	return false
}

func fetchGSSStreamDetail(ctx context.Context, baseURL string, streamID int64) (model.Stream, error) {
	var payload struct {
		Stream model.Stream `json:"stream"`
	}
	if err := getGSSJSON(ctx, baseURL, fmt.Sprintf("/api/v1/dashboard/streams/%d", streamID), &payload); err != nil {
		return model.Stream{}, err
	}
	if payload.Stream.ID <= 0 {
		return model.Stream{}, fmt.Errorf("stream %d detail returned no stream", streamID)
	}
	return payload.Stream, nil
}

func fetchGSSResolvedURL(ctx context.Context, baseURL string, streamID int64) (string, error) {
	var payload struct {
		ResolvedURL string `json:"resolved_url"`
	}
	if err := getGSSJSON(ctx, baseURL, fmt.Sprintf("/api/v1/dashboard/streams/%d/resolve", streamID), &payload); err != nil {
		return "", err
	}
	if strings.TrimSpace(payload.ResolvedURL) == "" {
		return "", fmt.Errorf("stream %d resolve returned no resolved_url", streamID)
	}
	return strings.TrimSpace(payload.ResolvedURL), nil
}

func getGSSJSON(ctx context.Context, baseURL string, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(baseURL, "/")+path, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var errPayload map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&errPayload)
		return fmt.Errorf("GET %s: status=%d body=%v", path, resp.StatusCode, errPayload)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func getGSSJSONWithToken(ctx context.Context, baseURL string, token string, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(baseURL, "/")+path, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(token))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var errPayload map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&errPayload)
		return fmt.Errorf("GET %s: status=%d body=%v", path, resp.StatusCode, errPayload)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func deleteJSONWithToken(ctx context.Context, baseURL string, token string, path string, payload any, out any) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, strings.TrimRight(baseURL, "/")+path, strings.NewReader(string(b)))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(token))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var errPayload map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&errPayload)
		return fmt.Errorf("DELETE %s: status=%d body=%v", path, resp.StatusCode, errPayload)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func probeGSSResolvedURL(ctx context.Context, resolvedURL string) (*gssProbe, error) {
	frame, err := capture.CaptureFrame(ctx, resolvedURL)
	if err != nil {
		return nil, fmt.Errorf("capture frame: %w", err)
	}
	if frame.Width <= 0 || frame.Height <= 0 || frame.SizeBytes <= 0 {
		return nil, fmt.Errorf("capture frame returned empty dimensions or bytes")
	}
	return &gssProbe{
		ResolvedURL: resolvedURL,
		Width:       frame.Width,
		Height:      frame.Height,
		SizeBytes:   frame.SizeBytes,
		SHA256:      frame.SHA256,
	}, nil
}

func applyGSSResults(ctx context.Context, opts gssOptions, rowsTotal int, results []gssResult) error {
	persist := func() error {
		if opts.ReportJSON == "" {
			return nil
		}
		return writeGSSReport(opts.ReportJSON, buildGSSReport(opts, rowsTotal, results))
	}
	for i := range results {
		if !results[i].Status.AllowApply() {
			continue
		}
		switch results[i].Status {
		case gssStatusVerifiedExisting:
			if err := tagGSSStream(ctx, opts, results[i].StreamID, results[i].Tags); err != nil {
				results[i].ApplyError = err.Error()
				_ = persist()
				return fmt.Errorf("tag existing stream %d from row %d: %w", results[i].StreamID, results[i].RowNumber, err)
			}
			results[i].Applied = true
			results[i].AppliedStreamID = results[i].StreamID
			results[i].ApplyAction = gssApplyTaggedExisting
			if err := persist(); err != nil {
				return fmt.Errorf("write report after tagging existing stream %d from row %d: %w", results[i].StreamID, results[i].RowNumber, err)
			}
		case gssStatusVerifiedImportable:
			externalID := gssExternalID(results[i])
			existing, found, err := fetchGSSStreamByExternalID(ctx, opts, externalID)
			if err != nil {
				results[i].ApplyError = err.Error()
				_ = persist()
				return fmt.Errorf("lookup existing stream from row %d: %w", results[i].RowNumber, err)
			}
			if found {
				results[i].StreamID = existing.ID
				results[i].StreamSlug = existing.Slug
				results[i].AppliedStreamID = existing.ID
				if err := tagGSSStream(ctx, opts, existing.ID, results[i].Tags); err != nil {
					results[i].ApplyError = err.Error()
					_ = persist()
					return fmt.Errorf("tag existing global-street-scores stream %d from row %d: %w", existing.ID, results[i].RowNumber, err)
				}
				results[i].Applied = true
				results[i].ApplyAction = gssApplyTaggedExistingSource
				if err := persist(); err != nil {
					return fmt.Errorf("write report after tagging existing global-street-scores stream %d from row %d: %w", existing.ID, results[i].RowNumber, err)
				}
				continue
			}
			stream, err := createGSSStream(ctx, opts, results[i])
			if err != nil {
				if existing, found, lookupErr := fetchGSSStreamByExternalID(ctx, opts, externalID); lookupErr == nil && found {
					results[i].StreamID = existing.ID
					results[i].StreamSlug = existing.Slug
					results[i].AppliedStreamID = existing.ID
					if tagErr := tagGSSStream(ctx, opts, existing.ID, results[i].Tags); tagErr != nil {
						results[i].ApplyError = tagErr.Error()
						_ = persist()
						return fmt.Errorf("tag existing global-street-scores stream %d after create conflict from row %d: %w", existing.ID, results[i].RowNumber, tagErr)
					}
					results[i].Applied = true
					results[i].ApplyAction = gssApplyTaggedExistingSource
					if persistErr := persist(); persistErr != nil {
						return fmt.Errorf("write report after create-conflict tag stream %d from row %d: %w", existing.ID, results[i].RowNumber, persistErr)
					}
					continue
				}
				results[i].ApplyError = err.Error()
				_ = persist()
				return fmt.Errorf("create stream from row %d: %w", results[i].RowNumber, err)
			}
			results[i].AppliedStreamID = stream.ID
			results[i].StreamID = stream.ID
			results[i].StreamSlug = stream.Slug
			results[i].Created = true
			if err := persist(); err != nil {
				return fmt.Errorf("write report after creating stream %d from row %d: %w", stream.ID, results[i].RowNumber, err)
			}
			if err := tagGSSStream(ctx, opts, stream.ID, results[i].Tags); err != nil {
				results[i].ApplyError = err.Error()
				_ = persist()
				return fmt.Errorf("tag created stream %d from row %d: %w", stream.ID, results[i].RowNumber, err)
			}
			results[i].Applied = true
			results[i].ApplyAction = gssApplyCreatedAndTagged
			if err := persist(); err != nil {
				return fmt.Errorf("write report after tagging created stream %d from row %d: %w", stream.ID, results[i].RowNumber, err)
			}
		}
	}
	return nil
}

func tagGSSStream(ctx context.Context, opts gssOptions, streamID int64, tags []string) error {
	reqCtx, cancel := context.WithTimeout(ctx, opts.ImportTimeout)
	defer cancel()
	var response struct {
		OK bool `json:"ok"`
	}
	if err := postJSONWithToken(reqCtx, opts.TargetAPIURL, opts.ServiceToken, fmt.Sprintf("/api/v1/service/streams/%d/tags", streamID), map[string]any{
		"tags": tags,
	}, &response); err != nil {
		return err
	}
	if !response.OK {
		return fmt.Errorf("tag stream %d returned ok=false", streamID)
	}
	return nil
}

func removeGSSStreamTags(ctx context.Context, opts gssOptions, streamID int64, tags []string) error {
	reqCtx, cancel := context.WithTimeout(ctx, opts.ImportTimeout)
	defer cancel()
	var response struct {
		OK bool `json:"ok"`
	}
	if err := deleteJSONWithToken(reqCtx, opts.TargetAPIURL, opts.ServiceToken, fmt.Sprintf("/api/v1/service/streams/%d/tags", streamID), map[string]any{
		"tags": tags,
	}, &response); err != nil {
		return err
	}
	if !response.OK {
		return fmt.Errorf("remove tags from stream %d returned ok=false", streamID)
	}
	return nil
}

func fetchGSSStreamByExternalID(ctx context.Context, opts gssOptions, externalID string) (model.Stream, bool, error) {
	reqCtx, cancel := context.WithTimeout(ctx, opts.ImportTimeout)
	defer cancel()
	q := url.Values{}
	q.Set("provider", gssProvider)
	q.Set("external_id", strings.TrimSpace(externalID))
	var response struct {
		OK     bool         `json:"ok"`
		Found  bool         `json:"found"`
		Stream model.Stream `json:"stream"`
	}
	path := "/api/v1/service/streams/by-external-id?" + q.Encode()
	if err := getGSSJSONWithToken(reqCtx, opts.TargetAPIURL, opts.ServiceToken, path, &response); err != nil {
		return model.Stream{}, false, err
	}
	if !response.OK {
		return model.Stream{}, false, fmt.Errorf("lookup stream by external id returned ok=false")
	}
	if !response.Found {
		return model.Stream{}, false, nil
	}
	if response.Stream.ID <= 0 {
		return model.Stream{}, false, fmt.Errorf("lookup stream by external id returned invalid stream")
	}
	return response.Stream, true, nil
}

func createGSSStream(ctx context.Context, opts gssOptions, result gssResult) (model.Stream, error) {
	fields, err := capture.DeriveCanonicalStreamFields(result.SourceURL, "", "", "", "")
	if err != nil {
		return model.Stream{}, err
	}
	payload := map[string]any{
		"provider":              gssProvider,
		"external_id":           gssExternalID(result),
		"name":                  gssStreamName(result),
		"source_url":            result.SourceURL,
		"source_page_url":       "",
		"source_family":         fields.SourceFamily,
		"capture_type":          fields.CaptureType,
		"execution_class":       fields.ExecutionClass,
		"execution_config_json": map[string]any{},
		"recording_state":       "off",
		"tags":                  []string{},
		"location_text":         gssLocationText(result),
		"location_country":      result.Values["country"],
		"location_city":         result.Values["city"],
		"location_source":       "global_street_scores_csv",
		"metadata_json": map[string]any{
			"import_source": "global_street_scores",
			"list":          result.List.String(),
			"row_number":    result.RowNumber,
			"csv_values":    result.Values,
			"verified_at":   result.FinishedAt.Format(time.RFC3339Nano),
			"resolved_url":  result.ResolvedURL,
		},
	}
	var response struct {
		OK      bool         `json:"ok"`
		Created bool         `json:"created"`
		Stream  model.Stream `json:"stream"`
	}
	reqCtx, cancel := context.WithTimeout(ctx, opts.ImportTimeout)
	defer cancel()
	if err := postJSONWithToken(reqCtx, opts.TargetAPIURL, opts.ServiceToken, "/api/v1/service/streams", payload, &response); err != nil {
		return model.Stream{}, err
	}
	if !response.OK || !response.Created || response.Stream.ID <= 0 {
		return model.Stream{}, fmt.Errorf("create stream returned invalid response")
	}
	return response.Stream, nil
}

func gssExternalID(result gssResult) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(result.SourceURL)))
	return fmt.Sprintf("%s:%s", result.List.String(), hex.EncodeToString(sum[:])[:16])
}

func gssStreamName(result gssResult) string {
	parts := []string{}
	for _, key := range []string{"city", "location"} {
		if v := strings.TrimSpace(result.Values[key]); v != "" {
			parts = append(parts, v)
		}
	}
	if len(parts) == 0 {
		return "Global Street Scores stream"
	}
	return strings.Join(parts, " - ")
}

func gssLocationText(result gssResult) string {
	parts := []string{}
	for _, key := range []string{"location", "city", "country"} {
		if v := strings.TrimSpace(result.Values[key]); v != "" {
			parts = append(parts, v)
		}
	}
	return strings.Join(parts, ", ")
}

func buildGSSReport(opts gssOptions, rowsTotal int, results []gssResult) gssReport {
	report := gssReport{
		TargetAPIURL:        opts.TargetAPIURL,
		Apply:               opts.Apply,
		ReviewApproved:      opts.ReviewApproved,
		GeneratedAt:         time.Now().UTC(),
		NilsCSV:             opts.NilsCSV,
		VittorioCSV:         opts.VittorioCSV,
		Limit:               opts.Limit,
		Concurrency:         opts.Concurrency,
		ProbeTimeout:        opts.ProbeTimeout.String(),
		ApplySourceReport:   opts.ApplyReport,
		RowsTotal:           rowsTotal,
		RowsProcessed:       len(results),
		CountsByStatus:      map[gssStatus]int{},
		CountsByApplyAction: map[gssApplyAction]int{},
		Results:             results,
	}
	for _, result := range results {
		report.CountsByStatus[result.Status]++
		if result.ApplyAction != gssApplyNone {
			report.CountsByApplyAction[result.ApplyAction]++
		}
		if result.ApplyError != "" {
			report.ApplyErrors++
		}
	}
	return report
}

func writeGSSReport(path string, report gssReport) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}

func writeGSSTagCleanupReport(path string, report gssTagCleanupReport) error {
	if path == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}

func preflightGSSReportPath(path string) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, ".gss-report-preflight-*")
	if err != nil {
		return err
	}
	name := f.Name()
	closeErr := f.Close()
	removeErr := os.Remove(name)
	if closeErr != nil {
		return closeErr
	}
	return removeErr
}

func buildGSSTags(row gssRow) []string {
	tags := []string{row.List.Tag()}
	if tag := gssColumnTag("valid", row.value("valid")); keepGSSSemanticTag(tag) {
		tags = append(tags, tag)
	}
	return normalizeTags(tags)
}

func gssColumnTag(column string, value string) string {
	columnSlug := gssSlug(column, 48)
	valueSlug := gssSlug(value, 80)
	if columnSlug == "" || valueSlug == "" {
		return ""
	}
	return "gss:" + columnSlug + ":" + valueSlug
}

func gssSlug(raw string, maxLen int) string {
	v := strings.ToLower(strings.TrimSpace(raw))
	if v == "" {
		return ""
	}
	var b strings.Builder
	prevDash := false
	for _, r := range v {
		ok := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')
		if ok {
			b.WriteRune(r)
			prevDash = false
			continue
		}
		if !prevDash {
			b.WriteByte('-')
			prevDash = true
		}
	}
	out := strings.Trim(b.String(), "-")
	if out == "" {
		return ""
	}
	if maxLen > 0 && len(out) > maxLen {
		sum := sha256.Sum256([]byte(v))
		suffix := "-" + hex.EncodeToString(sum[:])[:8]
		keep := maxLen - len(suffix)
		if keep < 1 {
			keep = maxLen
			suffix = ""
		}
		out = strings.Trim(out[:keep], "-") + suffix
	}
	return out
}

func normalizeGSSText(raw string) string {
	return strings.Join(strings.Fields(strings.TrimSpace(raw)), " ")
}
