package model

import "time"

type RecordingState string

const (
	RecordingStateOff RecordingState = "off"
	RecordingStateOn  RecordingState = "on"
)

type Stream struct {
	ID                     int64          `json:"id"`
	Provider               string         `json:"provider"`
	ExternalID             string         `json:"external_id"`
	Name                   string         `json:"name"`
	Slug                   string         `json:"slug"`
	SourceURL              string         `json:"source_url"`
	SourcePageURL          string         `json:"source_page_url"`
	SourceFamily           string         `json:"source_family"`
	Lat                    *float64       `json:"lat,omitempty"`
	Lon                    *float64       `json:"lon,omitempty"`
	LocationText           string         `json:"location_text"`
	LocationCountryCode    string         `json:"location_country_code"`
	LocationCountry        string         `json:"location_country"`
	LocationRegion         string         `json:"location_region"`
	LocationCity           string         `json:"location_city"`
	LocationLocality       string         `json:"location_locality"`
	LocationSource         string         `json:"location_source"`
	MetadataJSON           map[string]any `json:"metadata_json"`
	RecordingState         RecordingState `json:"recording_state"`
	RecordingFailedReason  *string        `json:"recording_failed_reason,omitempty"`
	RecordingFailedAt      *time.Time     `json:"recording_failed_at,omitempty"`
	CaptureType            string         `json:"capture_type"`
	ExecutionClass         string         `json:"execution_class"`
	CaptureFamily          string         `json:"capture_family"`
	ExpectedFPS            *float64       `json:"expected_fps,omitempty"`
	ExpectedImageInterval  *int           `json:"expected_image_interval_sec,omitempty"`
	ExecutionConfigJSON    map[string]any `json:"execution_config_json"`
	Tags                   []string       `json:"tags"`
	CaptureRuntimeStatus   *string        `json:"capture_runtime_status,omitempty"`
	CaptureRuntimeClass    *string        `json:"capture_runtime_execution_class,omitempty"`
	CaptureRuntimeType     *string        `json:"capture_runtime_resolved_capture_type,omitempty"`
	CaptureRuntimeResolved *string        `json:"capture_runtime_resolved_url,omitempty"`
	CaptureRuntimeLastSeen *time.Time     `json:"capture_runtime_last_frame_at,omitempty"`
	CaptureRuntimeErrors   *int           `json:"capture_runtime_consecutive_errors,omitempty"`
	CaptureRuntimeError    *string        `json:"capture_runtime_last_error,omitempty"`
	CreatedAt              time.Time      `json:"created_at"`
	UpdatedAt              time.Time      `json:"updated_at"`
}

func (s Stream) IsRecordingOn() bool {
	return s.RecordingState == RecordingStateOn
}

type Pipeline struct {
	ID             string         `json:"id"`
	OwnerAccountID *int64         `json:"owner_account_id,omitempty"`
	PipelineFamily string         `json:"pipeline_family"`
	Kind           string         `json:"kind"`
	SpecJSON       map[string]any `json:"spec_json"`
	Active         bool           `json:"active"`
	CreatedAt      time.Time      `json:"created_at"`
	UpdatedAt      time.Time      `json:"updated_at"`
}

type PipelineVersion struct {
	ID             int64          `json:"id"`
	PipelineID     string         `json:"pipeline_id"`
	OwnerAccountID *int64         `json:"owner_account_id,omitempty"`
	VersionID      string         `json:"version_id"`
	RunnerKind     string         `json:"runner_kind"`
	SpecJSON       map[string]any `json:"spec_json"`
	CreatedBy      string         `json:"created_by"`
	CreatedAt      time.Time      `json:"created_at"`
}

type PipelineRun struct {
	ID                int64          `json:"id"`
	PipelineID        string         `json:"pipeline_id"`
	OwnerAccountID    *int64         `json:"owner_account_id,omitempty"`
	PipelineVersionID int64          `json:"pipeline_version_id"`
	VersionID         string         `json:"version_id"`
	VersionRunnerKind string         `json:"version_runner_kind"`
	VersionSpecJSON   map[string]any `json:"version_spec_json"`
	Label             string         `json:"label"`
	Status            string         `json:"status"`
	WorkerKind        string         `json:"worker_kind"`
	SelectorJSON      map[string]any `json:"selector_json"`
	MetadataJSON      map[string]any `json:"metadata_json"`
	CreatedBy         string         `json:"created_by"`
	TargetCount       int64          `json:"target_count"`
	CompletedCount    int64          `json:"completed_count"`
	ErrorCount        int64          `json:"error_count"`
	LeasedCount       int64          `json:"leased_count"`
	CreatedAt         time.Time      `json:"created_at"`
	StartedAt         *time.Time     `json:"started_at,omitempty"`
	FinishedAt        *time.Time     `json:"finished_at,omitempty"`
}

type PipelineRunTarget struct {
	ID           int64      `json:"id"`
	RunID        int64      `json:"run_id"`
	FrameID      int64      `json:"frame_id"`
	StreamID     int64      `json:"stream_id"`
	Status       string     `json:"status"`
	ClaimID      *int64     `json:"claim_id,omitempty"`
	ClaimedBy    string     `json:"claimed_by"`
	LeaseExpires *time.Time `json:"lease_expires_at,omitempty"`
	ResultID     *int64     `json:"result_id,omitempty"`
	ErrorText    string     `json:"error_text"`
	CapturedAt   time.Time  `json:"captured_at"`
	ObjectKey    string     `json:"object_key"`
	MIMEType     string     `json:"mime_type"`
	SizeBytes    int64      `json:"size_bytes"`
	Width        int        `json:"width"`
	Height       int        `json:"height"`
}

type EvalSuite struct {
	ID              int64          `json:"id"`
	OwnerAccountID  *int64         `json:"owner_account_id,omitempty"`
	Slug            string         `json:"slug"`
	Title           string         `json:"title"`
	Description     string         `json:"description"`
	SourceKind      string         `json:"source_kind"`
	PrimaryMetric   string         `json:"primary_metric"`
	SourceURL       string         `json:"source_url"`
	MetadataJSON    map[string]any `json:"metadata_json"`
	CreatedBy       string         `json:"created_by"`
	ItemCount       int64          `json:"item_count"`
	AnnotationCount int64          `json:"annotation_count"`
	CreatedAt       time.Time      `json:"created_at"`
	UpdatedAt       time.Time      `json:"updated_at"`
}

type EvalSuiteItem struct {
	ID           int64          `json:"id"`
	SuiteID      int64          `json:"suite_id"`
	FrameID      *int64         `json:"frame_id,omitempty"`
	ItemKey      string         `json:"item_key"`
	Split        string         `json:"split"`
	SourceLabel  string         `json:"source_label"`
	SourceURL    string         `json:"source_url"`
	MetadataJSON map[string]any `json:"metadata_json"`
	CreatedAt    time.Time      `json:"created_at"`
	UpdatedAt    time.Time      `json:"updated_at"`
}

type EvalAnnotation struct {
	ID             int64          `json:"id"`
	SuiteItemID    int64          `json:"suite_item_id"`
	AnnotationKind string         `json:"annotation_kind"`
	LabelJSON      map[string]any `json:"label_json"`
	CreatedBy      string         `json:"created_by"`
	CreatedAt      time.Time      `json:"created_at"`
}

type PipelineExperiment struct {
	ID             int64          `json:"id"`
	OwnerAccountID *int64         `json:"owner_account_id,omitempty"`
	PipelineID     string         `json:"pipeline_id"`
	Slug           string         `json:"slug"`
	Title          string         `json:"title"`
	GoalText       string         `json:"goal_text"`
	PrimaryMetric  string         `json:"primary_metric"`
	Active         bool           `json:"active"`
	MetadataJSON   map[string]any `json:"metadata_json"`
	CreatedBy      string         `json:"created_by"`
	CreatedAt      time.Time      `json:"created_at"`
	UpdatedAt      time.Time      `json:"updated_at"`
}

type PipelineExperimentIteration struct {
	ID                         int64          `json:"id"`
	ExperimentID               int64          `json:"experiment_id"`
	CandidatePipelineVersionID *int64         `json:"candidate_pipeline_version_id,omitempty"`
	BaselinePipelineVersionID  *int64         `json:"baseline_pipeline_version_id,omitempty"`
	IterationIndex             int            `json:"iteration_index"`
	Status                     string         `json:"status"`
	HypothesisText             string         `json:"hypothesis_text"`
	ChangeSummary              string         `json:"change_summary"`
	ChangeJSON                 map[string]any `json:"change_json"`
	ResultClassification       string         `json:"result_classification"`
	PrimaryMetricBefore        *float64       `json:"primary_metric_before,omitempty"`
	PrimaryMetricAfter         *float64       `json:"primary_metric_after,omitempty"`
	PrimaryMetricDelta         *float64       `json:"primary_metric_delta,omitempty"`
	LogURL                     string         `json:"log_url"`
	ArtifactURL                string         `json:"artifact_url"`
	MetadataJSON               map[string]any `json:"metadata_json"`
	CreatedBy                  string         `json:"created_by"`
	CreatedAt                  time.Time      `json:"created_at"`
	UpdatedAt                  time.Time      `json:"updated_at"`
}

type PipelineEvalMetric struct {
	ID                    int64          `json:"id"`
	ExperimentIterationID *int64         `json:"experiment_iteration_id,omitempty"`
	SuiteID               int64          `json:"suite_id"`
	PipelineID            string         `json:"pipeline_id"`
	PipelineVersionID     *int64         `json:"pipeline_version_id,omitempty"`
	PipelineRunID         *int64         `json:"pipeline_run_id,omitempty"`
	MetricName            string         `json:"metric_name"`
	Split                 string         `json:"split"`
	MetricValue           float64        `json:"metric_value"`
	MetadataJSON          map[string]any `json:"metadata_json"`
	CreatedAt             time.Time      `json:"created_at"`
}

type SourceCandidate struct {
	ID            int64          `json:"id"`
	Provider      string         `json:"provider"`
	ExternalID    string         `json:"external_id"`
	SourceFamily  string         `json:"source_family"`
	CaptureType   string         `json:"capture_type"`
	SourceURL     string         `json:"source_url"`
	SourcePageURL string         `json:"source_page_url"`
	Title         string         `json:"title"`
	Slug          string         `json:"slug"`
	MetadataJSON  map[string]any `json:"metadata_json"`
	ReviewStatus  string         `json:"review_status"`
	ReviewReason  string         `json:"review_reason"`
	CreatedAt     time.Time      `json:"created_at"`
	UpdatedAt     time.Time      `json:"updated_at"`
}

type SourceCandidateReview struct {
	ID           int64          `json:"id"`
	CandidateID  int64          `json:"candidate_id"`
	Reviewer     string         `json:"reviewer"`
	Status       string         `json:"status"`
	Reason       string         `json:"reason"`
	MetadataJSON map[string]any `json:"metadata_json"`
	CreatedAt    time.Time      `json:"created_at"`
}

type SourceCandidateRun struct {
	ID           int64          `json:"id"`
	CandidateID  int64          `json:"candidate_id"`
	PipelineID   string         `json:"pipeline_id"`
	WorkerID     string         `json:"worker_id"`
	Status       string         `json:"status"`
	ErrorText    string         `json:"error_text"`
	MetadataJSON map[string]any `json:"metadata_json"`
	StartedAt    time.Time      `json:"started_at"`
	FinishedAt   *time.Time     `json:"finished_at,omitempty"`
	CreatedAt    time.Time      `json:"created_at"`
}
