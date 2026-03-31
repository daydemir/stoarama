package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/daydemir/stoarama/backend/internal/capture"
	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/model"
)

const (
	bellevueProvider          = "BELLEVUE_TRAFFICMAP"
	bellevueCamQueryURL       = "https://services1.arcgis.com/EYzEZbDhXZjURPbP/ArcGIS/rest/services/TrafficCameras_Public_View/FeatureServer/0/query?where=Media+%3D+%27Stream%27&outFields=*&orderByFields=Latitude+DESC&returnGeometry=true&f=pgeojson"
	bellevueSourcePageURL     = "https://trafficmap.bellevuewa.gov/"
	bellevueManifestURLFormat = "https://trafficcams.bellevuewa.gov:443/traffic-edge/%sL.stream/playlist.m3u8"
	bellevueLocationSource    = "bellevue_trafficmap_arcgis"
)

type bellevueFeatureCollection struct {
	Features []bellevueFeature `json:"features"`
}

type bellevueFeature struct {
	Geometry   bellevueGeometry   `json:"geometry"`
	Properties bellevueProperties `json:"properties"`
}

type bellevueGeometry struct {
	Coordinates []float64 `json:"coordinates"`
}

type bellevueProperties struct {
	ID             string `json:"ID"`
	Address        string `json:"Address"`
	DisplayAddress string `json:"Display_Address"`
	Latitude       any    `json:"Latitude"`
	Longitude      any    `json:"Longitude"`
	OwnedBy        string `json:"OwnedBy"`
	Media          string `json:"Media"`
	Channel        any    `json:"Channel"`
	CameraType     string `json:"CameraType"`
}

type bellevuePreparedStream struct {
	ExternalID       string
	Name             string
	SourceURL        string
	SourcePageURL    string
	Owner            string
	OwnerTag         string
	Lat              *float64
	Lon              *float64
	LocationText     string
	LocationCountry  string
	LocationCode     string
	LocationRegion   string
	LocationCity     string
	LocationLocality string
	LocationSource   string
	Tags             []string
	MetadataJSON     map[string]any
}

type bellevueProbeResult struct {
	ResolvedURL string `json:"resolved_url"`
	MIMEType    string `json:"mime_type,omitempty"`
	StatusCode  int    `json:"status_code,omitempty"`
	Preview     string `json:"preview,omitempty"`
}

type bellevueImportResult struct {
	ExternalID       string               `json:"external_id"`
	Name             string               `json:"name"`
	OwnedBy          string               `json:"owned_by,omitempty"`
	OwnerTag         string               `json:"owner_tag,omitempty"`
	SourceURL        string               `json:"source_url,omitempty"`
	LocationLocality string               `json:"location_locality,omitempty"`
	Lat              *float64             `json:"lat,omitempty"`
	Lon              *float64             `json:"lon,omitempty"`
	ProbeOK          bool                 `json:"probe_ok"`
	Probe            *bellevueProbeResult `json:"probe,omitempty"`
	SkipReason       string               `json:"skip_reason,omitempty"`
	Imported         bool                 `json:"imported"`
	Created          bool                 `json:"created"`
	ImportedStreamID int64                `json:"imported_stream_id,omitempty"`
	ImportedSlug     string               `json:"imported_slug,omitempty"`
	ImportError      string               `json:"import_error,omitempty"`
	StartedAt        time.Time            `json:"started_at"`
	FinishedAt       time.Time            `json:"finished_at"`
	DurationMs       int64                `json:"duration_ms"`
}

type bellevueImportReport struct {
	CamQueryURL     string                 `json:"cam_query_url"`
	SourcePageURL   string                 `json:"source_page_url"`
	TargetAPIURL    string                 `json:"target_api_url"`
	Provider        string                 `json:"provider"`
	Limit           int                    `json:"limit"`
	Concurrency     int                    `json:"concurrency"`
	ProbeTimeout    string                 `json:"probe_timeout"`
	Apply           bool                   `json:"apply"`
	GeneratedAt     time.Time              `json:"generated_at"`
	CatalogCount    int                    `json:"catalog_count"`
	EligibleCount   int                    `json:"eligible_count"`
	Processed       int                    `json:"processed"`
	ProbedOK        int                    `json:"probed_ok"`
	ProbeFailed     int                    `json:"probe_failed"`
	Imported        int                    `json:"imported"`
	Created         int                    `json:"created"`
	Updated         int                    `json:"updated"`
	ImportFailed    int                    `json:"import_failed"`
	SkippedPreProbe int                    `json:"skipped_pre_probe"`
	Results         []bellevueImportResult `json:"results"`
}

func runImportBellevueStreams(ctx context.Context, cfg config.Config, args []string) {
	fs := flag.NewFlagSet("bellevue-streams", flag.ExitOnError)
	camQueryURL := fs.String("cam-query-url", bellevueCamQueryURL, "Bellevue ArcGIS camera query URL")
	sourcePageURL := fs.String("source-page-url", bellevueSourcePageURL, "Bellevue traffic map page URL")
	targetAPIURL := fs.String("target-api-url", defaultBackendAPIURL(), "target Stoarama API base URL")
	serviceToken := fs.String("service-token", cfg.ServiceToken, "target Stoarama service token")
	limit := fs.Int("limit", 0, "maximum eligible Bellevue streams to process (0 means all)")
	concurrency := fs.Int("concurrency", 8, "probe/import worker concurrency")
	probeTimeoutSec := fs.Int("probe-timeout-sec", 15, "per-stream probe timeout seconds")
	apply := fs.Bool("apply", false, "import Bellevue streams into Stoarama")
	reportJSON := fs.String("report-json", "local/reports/bellevue-import-report.json", "optional report JSON path")
	asJSON := fs.Bool("json", false, "print JSON report")
	_ = fs.Parse(args)

	if strings.TrimSpace(*camQueryURL) == "" {
		log.Fatalf("--cam-query-url is required")
	}
	if strings.TrimSpace(*sourcePageURL) == "" {
		log.Fatalf("--source-page-url is required")
	}
	if strings.TrimSpace(*targetAPIURL) == "" {
		log.Fatalf("--target-api-url is required")
	}
	if *apply && strings.TrimSpace(*serviceToken) == "" {
		log.Fatalf("--service-token is required with --apply")
	}
	if *concurrency <= 0 {
		log.Fatalf("--concurrency must be > 0")
	}
	if *probeTimeoutSec <= 0 {
		log.Fatalf("--probe-timeout-sec must be > 0")
	}

	features, err := fetchBellevueCameraCatalog(ctx, strings.TrimSpace(*camQueryURL))
	if err != nil {
		log.Fatalf("fetch Bellevue camera catalog: %v", err)
	}
	prepared, skipped := prepareBellevueStreams(features, strings.TrimSpace(*camQueryURL), strings.TrimSpace(*sourcePageURL))
	sort.Slice(prepared, func(i, j int) bool {
		if prepared[i].LocationLocality == prepared[j].LocationLocality {
			return prepared[i].ExternalID < prepared[j].ExternalID
		}
		return prepared[i].LocationLocality < prepared[j].LocationLocality
	})
	if *limit > 0 && len(prepared) > *limit {
		prepared = prepared[:*limit]
	}

	results := append([]bellevueImportResult{}, skipped...)
	if len(prepared) > 0 {
		processed := processBellevuePreparedStreams(ctx, prepared, strings.TrimSpace(*targetAPIURL), strings.TrimSpace(*serviceToken), time.Duration(*probeTimeoutSec)*time.Second, *apply, *concurrency)
		results = append(results, processed...)
	}
	sort.Slice(results, func(i, j int) bool {
		if results[i].LocationLocality == results[j].LocationLocality {
			return results[i].ExternalID < results[j].ExternalID
		}
		return results[i].LocationLocality < results[j].LocationLocality
	})

	report := bellevueImportReport{
		CamQueryURL:     strings.TrimSpace(*camQueryURL),
		SourcePageURL:   strings.TrimSpace(*sourcePageURL),
		TargetAPIURL:    strings.TrimSpace(*targetAPIURL),
		Provider:        bellevueProvider,
		Limit:           *limit,
		Concurrency:     *concurrency,
		ProbeTimeout:    (time.Duration(*probeTimeoutSec) * time.Second).String(),
		Apply:           *apply,
		GeneratedAt:     time.Now().UTC(),
		CatalogCount:    len(features),
		EligibleCount:   len(prepared),
		SkippedPreProbe: len(skipped),
		Results:         results,
	}
	for _, item := range results {
		report.Processed++
		if item.ProbeOK {
			report.ProbedOK++
		} else if item.SkipReason == "" || item.SkipReason == "probe_failed" {
			report.ProbeFailed++
		}
		if item.Imported {
			report.Imported++
			if item.Created {
				report.Created++
			} else {
				report.Updated++
			}
		}
		if item.ImportError != "" {
			report.ImportFailed++
		}
	}

	if path := strings.TrimSpace(*reportJSON); path != "" {
		if err := writeBellevueImportReport(path, report); err != nil {
			log.Fatalf("write Bellevue import report: %v", err)
		}
	}
	if *asJSON {
		b, err := json.MarshalIndent(report, "", "  ")
		if err != nil {
			log.Fatalf("marshal Bellevue import report: %v", err)
		}
		fmt.Println(string(b))
		return
	}
	fmt.Printf("Bellevue import: catalog=%d eligible=%d processed=%d probed_ok=%d probe_failed=%d imported=%d created=%d updated=%d import_failed=%d skipped_pre_probe=%d\n",
		report.CatalogCount, report.EligibleCount, report.Processed, report.ProbedOK, report.ProbeFailed, report.Imported, report.Created, report.Updated, report.ImportFailed, report.SkippedPreProbe)
	if strings.TrimSpace(*reportJSON) != "" {
		fmt.Printf("report_json=%s\n", strings.TrimSpace(*reportJSON))
	}
}

func fetchBellevueCameraCatalog(ctx context.Context, camQueryURL string) ([]bellevueFeature, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, camQueryURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "stoaramactl-bellevue-import")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("GET Bellevue catalog: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var payload bellevueFeatureCollection
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, err
	}
	return payload.Features, nil
}

func prepareBellevueStreams(features []bellevueFeature, camQueryURL, sourcePageURL string) ([]bellevuePreparedStream, []bellevueImportResult) {
	prepared := make([]bellevuePreparedStream, 0, len(features))
	skipped := make([]bellevueImportResult, 0)
	for _, feature := range features {
		item, skipReason := prepareBellevueStream(feature, camQueryURL, sourcePageURL)
		if skipReason != "" {
			skipped = append(skipped, bellevueImportResult{
				ExternalID:       normalizeBellevueCameraID(feature.Properties.ID),
				Name:             trimBellevueText(feature.Properties.DisplayAddress),
				OwnedBy:          trimBellevueText(feature.Properties.OwnedBy),
				LocationLocality: trimBellevueText(feature.Properties.DisplayAddress),
				SkipReason:       skipReason,
			})
			continue
		}
		prepared = append(prepared, item)
	}
	return prepared, skipped
}

func prepareBellevueStream(feature bellevueFeature, camQueryURL, sourcePageURL string) (bellevuePreparedStream, string) {
	props := feature.Properties
	if !strings.EqualFold(strings.TrimSpace(props.Media), "Stream") {
		return bellevuePreparedStream{}, "media_not_stream"
	}
	owner := trimBellevueText(props.OwnedBy)
	if !allowBellevueOwner(owner) {
		return bellevuePreparedStream{}, "owner_excluded"
	}
	externalID := normalizeBellevueCameraID(props.ID)
	if externalID == "" {
		return bellevuePreparedStream{}, "missing_external_id"
	}
	name := trimBellevueText(props.DisplayAddress)
	if name == "" {
		name = trimBellevueText(props.Address)
	}
	if name == "" {
		name = externalID
	}
	lat, lon := bellevueCoordinates(feature)
	if lat == nil || lon == nil {
		return bellevuePreparedStream{}, "missing_geometry"
	}
	ownerTag := bellevueOwnerTag(owner)
	locationLocality := name
	sourceURL := fmt.Sprintf(bellevueManifestURLFormat, externalID)
	tags := []string{
		"provider:bellevuewa.gov",
		"source:traffic-camera",
		"source:trafficmap.bellevuewa.gov",
		"country:us",
		"state:wa",
		"city:bellevue",
		ownerTag,
	}
	metadata := map[string]any{
		"provider_site":         "trafficmap.bellevuewa.gov",
		"catalog_query_url":     camQueryURL,
		"camera_id":             externalID,
		"owned_by":              owner,
		"channel":               props.Channel,
		"camera_type":           trimBellevueText(props.CameraType),
		"address":               trimBellevueText(props.Address),
		"display_address":       locationLocality,
		"raw_media":             trimBellevueText(props.Media),
		"source_catalog_system": "arcgis_feature_server",
	}
	return bellevuePreparedStream{
		ExternalID:       externalID,
		Name:             name,
		SourceURL:        sourceURL,
		SourcePageURL:    sourcePageURL,
		Owner:            owner,
		OwnerTag:         ownerTag,
		Lat:              lat,
		Lon:              lon,
		LocationText:     fmt.Sprintf("%s, Bellevue, Washington, United States", locationLocality),
		LocationCountry:  "United States",
		LocationCode:     "US",
		LocationRegion:   "Washington",
		LocationCity:     "Bellevue",
		LocationLocality: locationLocality,
		LocationSource:   bellevueLocationSource,
		Tags:             dedupeBellevueTags(tags),
		MetadataJSON:     metadata,
	}, ""
}

func processBellevuePreparedStreams(ctx context.Context, items []bellevuePreparedStream, targetAPIURL, serviceToken string, probeTimeout time.Duration, apply bool, concurrency int) []bellevueImportResult {
	results := make([]bellevueImportResult, len(items))
	jobs := make(chan int)
	var wg sync.WaitGroup
	workers := concurrency
	if workers > len(items) {
		workers = len(items)
	}
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range jobs {
				results[idx] = processBellevuePreparedStream(ctx, items[idx], targetAPIURL, serviceToken, probeTimeout, apply)
			}
		}()
	}
	for i := range items {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	return results
}

func processBellevuePreparedStream(ctx context.Context, item bellevuePreparedStream, targetAPIURL, serviceToken string, probeTimeout time.Duration, apply bool) (result bellevueImportResult) {
	started := time.Now().UTC()
	result = bellevueImportResult{
		ExternalID:       item.ExternalID,
		Name:             item.Name,
		OwnedBy:          item.Owner,
		OwnerTag:         item.OwnerTag,
		SourceURL:        item.SourceURL,
		LocationLocality: item.LocationLocality,
		Lat:              item.Lat,
		Lon:              item.Lon,
		StartedAt:        started,
	}
	defer func() {
		result.FinishedAt = time.Now().UTC()
		result.DurationMs = result.FinishedAt.Sub(started).Milliseconds()
	}()

	probeCtx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()
	probe, err := probeBellevueManifest(probeCtx, item.SourceURL)
	if err != nil {
		result.SkipReason = "probe_failed"
		result.ImportError = ""
		result.Probe = probe
		return result
	}
	result.ProbeOK = true
	result.Probe = probe

	if !apply {
		return result
	}

	metadata := cloneJSONMap(item.MetadataJSON)
	metadata["imported_from"] = map[string]any{
		"system":      "bellevue-trafficmap",
		"imported_at": time.Now().UTC().Format(time.RFC3339Nano),
		"owned_by":    item.Owner,
	}
	if probe != nil {
		metadata["import_probe"] = map[string]any{
			"resolved_url": probe.ResolvedURL,
			"mime_type":    probe.MIMEType,
			"status_code":  probe.StatusCode,
			"preview":      probe.Preview,
		}
	}

	payload := map[string]any{
		"provider":              bellevueProvider,
		"external_id":           item.ExternalID,
		"name":                  item.Name,
		"source_url":            item.SourceURL,
		"source_page_url":       item.SourcePageURL,
		"source_family":         capture.SourceFamilyVideoManifest,
		"capture_type":          capture.CaptureTypeHLS,
		"execution_class":       capture.ExecutionClassVideoLive,
		"lat":                   item.Lat,
		"lon":                   item.Lon,
		"execution_config_json": map[string]any{},
		"tags":                  item.Tags,
		"location_text":         item.LocationText,
		"location_country":      item.LocationCountry,
		"location_country_code": item.LocationCode,
		"location_region":       item.LocationRegion,
		"location_city":         item.LocationCity,
		"location_locality":     item.LocationLocality,
		"location_source":       item.LocationSource,
		"metadata_json":         metadata,
	}
	var response struct {
		OK      bool         `json:"ok"`
		Created bool         `json:"created"`
		Stream  model.Stream `json:"stream"`
	}
	if err := postJSONWithToken(ctx, targetAPIURL, serviceToken, "/api/v1/imports/streams", payload, &response); err != nil {
		result.ImportError = err.Error()
		return result
	}
	result.Imported = true
	result.Created = response.Created
	result.ImportedStreamID = response.Stream.ID
	result.ImportedSlug = response.Stream.Slug
	return result
}

func probeBellevueManifest(ctx context.Context, sourceURL string) (*bellevueProbeResult, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, sourceURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "stoaramactl-bellevue-import")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	probe := &bellevueProbeResult{
		ResolvedURL: resp.Request.URL.String(),
		MIMEType:    strings.TrimSpace(resp.Header.Get("Content-Type")),
		StatusCode:  resp.StatusCode,
		Preview:     strings.TrimSpace(string(body)),
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return probe, fmt.Errorf("status=%d", resp.StatusCode)
	}
	if !strings.Contains(probe.Preview, "#EXTM3U") {
		return probe, fmt.Errorf("response is not an HLS manifest")
	}
	return probe, nil
}

func writeBellevueImportReport(path string, report bellevueImportReport) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}

func normalizeBellevueCameraID(raw string) string {
	return strings.ToUpper(trimBellevueText(raw))
}

func trimBellevueText(raw string) string {
	return strings.Join(strings.Fields(strings.TrimSpace(raw)), " ")
}

func allowBellevueOwner(raw string) bool {
	switch strings.ToLower(trimBellevueText(raw)) {
	case "bellevue", "cob", "":
		return true
	default:
		return false
	}
}

func bellevueOwnerTag(raw string) string {
	switch strings.ToLower(trimBellevueText(raw)) {
	case "bellevue":
		return "owner:bellevue"
	case "cob":
		return "owner:cob"
	default:
		return "owner:unknown"
	}
}

func bellevueCoordinates(feature bellevueFeature) (*float64, *float64) {
	if len(feature.Geometry.Coordinates) >= 2 {
		lon := feature.Geometry.Coordinates[0]
		lat := feature.Geometry.Coordinates[1]
		return &lat, &lon
	}
	return nil, nil
}

func cloneJSONMap(in map[string]any) map[string]any {
	if len(in) == 0 {
		return map[string]any{}
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func dedupeBellevueTags(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	out := make([]string, 0, len(in))
	seen := make(map[string]struct{}, len(in))
	for _, raw := range in {
		tag := strings.TrimSpace(raw)
		if tag == "" {
			continue
		}
		if _, ok := seen[tag]; ok {
			continue
		}
		seen[tag] = struct{}{}
		out = append(out, tag)
	}
	return out
}
