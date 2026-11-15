// Code generated from the frozen NAS stitching attestation schema.
// Schema SHA256: 03d9a5fa930b67249036a6b522dcffa59f6bba9744f79abc0747f85d9ee63abc
// All object fields are required; the strict decoder enforces presence.
package stitchproof

type File struct {
	File      string `json:"file"`
	SHA256    string `json:"sha256"`
	SizeBytes int64  `json:"size_bytes"`
}

type Media struct {
	RelativePath string `json:"relative_path"`
	SHA256       string `json:"sha256"`
	SizeBytes    int64  `json:"size_bytes"`
}

type Input struct {
	ClipID       int64  `json:"clip_id"`
	RelativePath string `json:"relative_path"`
	SHA256       string `json:"sha256"`
	SizeBytes    int64  `json:"size_bytes"`
}

type Output struct {
	RelativePath   string  `json:"relative_path"`
	SHA256         string  `json:"sha256"`
	SizeBytes      int64   `json:"size_bytes"`
	SourceClipIDs  []int64 `json:"source_clip_ids"`
	ReadbackSHA256 string  `json:"readback_sha256"`
}

type Policy struct {
	Name                  string `json:"name"`
	Revision              string `json:"revision"`
	Keep28SHA256          string `json:"keep28_sha256"`
	CorpusSHA256          string `json:"corpus_sha256"`
	PackageManifestSHA256 string `json:"package_manifest_sha256"`
}

type Pipeline struct {
	Repository           string `json:"repository"`
	BaseCommitSHA        string `json:"base_commit_sha"`
	ReviewedHeadSHA      string `json:"reviewed_head_sha"`
	MergedCommitSHA      string `json:"merged_commit_sha"`
	ClientVersion        string `json:"client_version"`
	ClientArtifactSHA256 string `json:"client_artifact_sha256"`
}

type Run struct {
	ID               string  `json:"id"`
	ClientBootID     string  `json:"client_boot_id"`
	Host             RunHost `json:"host"`
	StartedAt        string  `json:"started_at"`
	CompletedAt      string  `json:"completed_at"`
	MediaSeconds     float64 `json:"media_seconds"`
	BuildWallSeconds float64 `json:"build_wall_seconds"`
}

type Payload struct {
	Schema       string              `json:"schema"`
	ConnectionID int64               `json:"connection_id"`
	AccountID    int64               `json:"account_id"`
	Scope        PayloadScope        `json:"scope"`
	Policy       Policy              `json:"policy"`
	Pipeline     Pipeline            `json:"pipeline"`
	Tools        PayloadTools        `json:"tools"`
	Run          Run                 `json:"run"`
	Inputs       []Input             `json:"inputs"`
	Outputs      []Output            `json:"outputs"`
	Evidence     PayloadEvidence     `json:"evidence"`
	Verification PayloadVerification `json:"verification"`
}

type Validation struct {
	Schema               string                  `json:"schema"`
	ValidatedAt          string                  `json:"validated_at"`
	RunID                string                  `json:"run_id"`
	ProofPlanSHA256      string                  `json:"proof_plan_sha256"`
	ClientArtifactSHA256 string                  `json:"client_artifact_sha256"`
	Policy               Policy                  `json:"policy"`
	Tools                PayloadTools            `json:"tools"`
	ServerBinding        ValidationServerBinding `json:"server_binding"`
	NasReadback          ValidationNasReadback   `json:"nas_readback"`
}

type Review struct {
	Schema              string         `json:"schema"`
	Reviewer            ReviewReviewer `json:"reviewer"`
	ReviewedAt          string         `json:"reviewed_at"`
	Verdict             string         `json:"verdict"`
	PayloadSHA256       string         `json:"payload_sha256"`
	BundleSHA256        string         `json:"bundle_sha256"`
	ReviewedCodeHeadSHA string         `json:"reviewed_code_head_sha"`
	Observations        File           `json:"observations"`
}

type FFmpeg struct {
	Version   string `json:"version"`
	SHA256    string `json:"sha256"`
	SizeBytes int64  `json:"size_bytes"`
}

type FFprobe struct {
	Version   string `json:"version"`
	SHA256    string `json:"sha256"`
	SizeBytes int64  `json:"size_bytes"`
}

type Attestation struct {
	Schema            string `json:"schema"`
	Synthetic         bool   `json:"synthetic"`
	Payload           File   `json:"payload"`
	IndependentReview File   `json:"independent_review"`
}

type RunHost struct {
	ConnectionID int64  `json:"connection_id"`
	System       string `json:"system"`
	Machine      string `json:"machine"`
	CpuModel     string `json:"cpu_model"`
	CpuCount     int64  `json:"cpu_count"`
}

type PayloadScope struct {
	StreamID         int64   `json:"stream_id"`
	RecordingID      int64   `json:"recording_id"`
	SourceTaskID     int64   `json:"source_task_id"`
	SourcePlanSHA256 string  `json:"source_plan_sha256"`
	ProofPlanSHA256  string  `json:"proof_plan_sha256"`
	ClipIDs          []int64 `json:"clip_ids"`
}

type PayloadTools struct {
	FFmpeg  FFmpeg  `json:"ffmpeg"`
	FFprobe FFprobe `json:"ffprobe"`
}

type PayloadEvidence struct {
	NativeBundle      File `json:"native_bundle"`
	LocalRegistration File `json:"local_registration"`
	SourcePlan        File `json:"source_plan"`
	ServerValidation  File `json:"server_validation"`
	Verifier          File `json:"verifier"`
	ClientArtifact    File `json:"client_artifact"`
}

type PayloadVerification struct {
	CurrentPolicy         string `json:"current_policy"`
	NativeInventoryReplay string `json:"native_inventory_replay"`
	ServerBinding         string `json:"server_binding"`
	ResultSHA256          string `json:"result_sha256"`
}

type ValidationServerBinding struct {
	Verified              bool   `json:"verified"`
	Kind                  string `json:"kind"`
	PlanSHA256            string `json:"plan_sha256"`
	OutputSHA256          string `json:"output_sha256"`
	RegisteredLocallyOnly bool   `json:"registered_locally_only"`
	GrantsRetirement      bool   `json:"grants_retirement"`
}

type ValidationNasReadback struct {
	VerifiedAt    string  `json:"verified_at"`
	ClientBootID  string  `json:"client_boot_id"`
	RunID         string  `json:"run_id"`
	ReceiptSHA256 string  `json:"receipt_sha256"`
	Outputs       []Media `json:"outputs"`
}

type ReviewReviewer struct {
	Name      string `json:"name"`
	Provider  string `json:"provider"`
	Model     string `json:"model"`
	Reference string `json:"reference"`
}
