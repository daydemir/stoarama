package joinedrecording

import (
	"encoding/hex"
	"fmt"
	"math"
	"path"
	"regexp"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/recordingnaming"
	"github.com/daydemir/stoarama/backend/internal/stitchcert"
)

const PlanPolicyVersion = "joined-delivery-v1"

var safeCampaignID = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,62}$`)

type ObjectIdentity struct {
	Key       string `json:"key"`
	VersionID string `json:"version_id,omitempty"`
	ETag      string `json:"etag"`
	SizeBytes int64  `json:"size_bytes"`
	SHA256    string `json:"sha256"`
}

type SeamEvidence struct {
	Verdict    string  `json:"verdict"`
	Reason     string  `json:"reason"`
	GapSeconds float64 `json:"gap_seconds"`
}

type SourceClip struct {
	ClipID                int64          `json:"clip_id"`
	RecordingID           int64          `json:"recording_id"`
	RecordingJobID        int64          `json:"recording_job_id"`
	CaptureGeneration     string         `json:"capture_generation"`
	CaptureSequence       int64          `json:"capture_sequence"`
	NativeSignatureSHA256 string         `json:"native_signature_sha256"`
	StartUTC              time.Time      `json:"start_utc"`
	EndUTC                time.Time      `json:"end_utc"`
	Object                ObjectIdentity `json:"object"`
	SeamToPrevious        SeamEvidence   `json:"seam_to_previous,omitempty"`
}

type PlanRequest struct {
	CampaignID  string
	RecordingID int64
	Timezone    string
	FolderName  string
	Metadata    recordingnaming.Metadata
	Sources     []SourceClip
}

type Gap struct {
	PreviousClipID int64     `json:"previous_clip_id"`
	NextClipID     int64     `json:"next_clip_id"`
	AtUTC          time.Time `json:"at_utc"`
	Seconds        float64   `json:"seconds"`
	Reason         string    `json:"reason"`
}

type OutputPlan struct {
	Ordinal      int          `json:"ordinal"`
	Hour         int          `json:"hour"`
	Part         int          `json:"part"`
	Parts        int          `json:"parts"`
	ActualStart  time.Time    `json:"actual_start_utc"`
	ActualEnd    time.Time    `json:"actual_end_utc"`
	UTCOffsetSec int          `json:"utc_offset_seconds"`
	RelativePath string       `json:"relative_path"`
	ObjectKey    string       `json:"object_key"`
	CoverageKey  string       `json:"coverage_object_key"`
	ContentID    string       `json:"content_id"`
	Sources      []SourceClip `json:"sources"`
}

type BatchPlan struct {
	SchemaVersion       int          `json:"schema_version"`
	PolicyVersion       string       `json:"policy_version"`
	CampaignID          string       `json:"campaign_id"`
	BatchID             string       `json:"batch_id"`
	RecordingID         int64        `json:"recording_id"`
	Timezone            string       `json:"timezone"`
	SourceManifestSHA   string       `json:"source_manifest_sha256"`
	ExpectedOutputCount int          `json:"expected_output_count"`
	CoverageObjectKey   string       `json:"coverage_object_key"`
	Gaps                []Gap        `json:"gaps"`
	Outputs             []OutputPlan `json:"outputs"`
	PlanSHA256          string       `json:"plan_sha256"`
}

type draftOutput struct {
	hour    int
	dayKey  string
	sources []SourceClip
}

func BuildPlan(req PlanRequest) (BatchPlan, error) {
	if !safeCampaignID.MatchString(req.CampaignID) || req.RecordingID <= 0 || len(req.Sources) == 0 {
		return BatchPlan{}, fmt.Errorf("invalid bounded joined plan request")
	}
	loc, err := time.LoadLocation(strings.TrimSpace(req.Timezone))
	if err != nil {
		return BatchPlan{}, fmt.Errorf("load joined timezone: %w", err)
	}
	folderName, err := recordingnaming.BuildFolderName(recordingnaming.ProfilePlazaHourlyV1, req.RecordingID, req.Metadata, req.FolderName)
	if err != nil {
		return BatchPlan{}, err
	}
	req.FolderName = folderName
	seenIDs, seenKeys := map[int64]bool{}, map[string]bool{}
	gaps := []Gap{}
	drafts := []draftOutput{}
	var current *draftOutput
	for i, clip := range req.Sources {
		if err := validateSource(clip, req.RecordingID); err != nil {
			return BatchPlan{}, fmt.Errorf("source %d: %w", i+1, err)
		}
		if seenIDs[clip.ClipID] || seenKeys[clip.Object.Key] {
			return BatchPlan{}, fmt.Errorf("duplicate source identity")
		}
		seenIDs[clip.ClipID], seenKeys[clip.Object.Key] = true, true
		local := clip.StartUTC.In(loc)
		hour := local.Hour() - 7
		endLocal := clip.EndUTC.In(loc)
		if hour < 1 || hour > 12 || endLocal.YearDay() != local.YearDay() || endLocal.Year() != local.Year() {
			return BatchPlan{}, fmt.Errorf("source %d is outside one local delivery day", i+1)
		}
		dayKey := local.Format("2006-01-02")
		continuous := i == 0
		if i > 0 {
			prev := req.Sources[i-1]
			if clip.StartUTC.Before(prev.StartUTC) || strings.TrimSpace(clip.SeamToPrevious.Reason) == "" || strings.TrimSpace(clip.SeamToPrevious.Verdict) == "" {
				return BatchPlan{}, fmt.Errorf("sources are not in chronological order")
			}
			if math.IsNaN(clip.SeamToPrevious.GapSeconds) || math.IsInf(clip.SeamToPrevious.GapSeconds, 0) || clip.SeamToPrevious.GapSeconds < 0 {
				return BatchPlan{}, fmt.Errorf("source %d has invalid seam gap", i+1)
			}
			continuous = clip.SeamToPrevious.Verdict == "continuous" && clip.SeamToPrevious.Reason != "" && clip.SeamToPrevious.GapSeconds == 0 && clip.NativeSignatureSHA256 == prev.NativeSignatureSHA256 && clip.CaptureGeneration == prev.CaptureGeneration && clip.CaptureSequence == prev.CaptureSequence+1
			if !continuous {
				seconds := clip.SeamToPrevious.GapSeconds
				if seconds == 0 && clip.StartUTC.After(prev.EndUTC) {
					seconds = clip.StartUTC.Sub(prev.EndUTC).Seconds()
				}
				gaps = append(gaps, Gap{PreviousClipID: prev.ClipID, NextClipID: clip.ClipID, AtUTC: prev.EndUTC, Seconds: seconds, Reason: clip.SeamToPrevious.Reason})
			}
		}
		if current == nil || !continuous || current.hour != hour || current.dayKey != dayKey || clip.EndUTC.Sub(current.sources[0].StartUTC) > time.Hour {
			drafts = append(drafts, draftOutput{hour: hour, dayKey: dayKey, sources: []SourceClip{clip}})
			current = &drafts[len(drafts)-1]
		} else {
			current.sources = append(current.sources, clip)
		}
	}
	return finalizePlan(req, drafts, gaps, loc)
}

func finalizePlan(req PlanRequest, drafts []draftOutput, gaps []Gap, loc *time.Location) (BatchPlan, error) {
	manifestSHA, _, err := stitchcert.CanonicalSHA(req.Sources)
	if err != nil {
		return BatchPlan{}, err
	}
	batchID := req.CampaignID
	partCounts := map[string]int{}
	for _, d := range drafts {
		partCounts[fmt.Sprintf("%s/%02d", d.dayKey, d.hour)]++
	}
	partSeen := map[string]int{}
	outputs := make([]OutputPlan, 0, len(drafts))
	for i, d := range drafts {
		bucket := fmt.Sprintf("%s/%02d", d.dayKey, d.hour)
		partSeen[bucket]++
		start, end := d.sources[0].StartUTC, d.sources[len(d.sources)-1].EndUTC
		relative, err := recordingnaming.BuildJoinedPath(recordingnaming.JoinedPolicy{FolderName: req.FolderName, Metadata: req.Metadata, CronTimezone: req.Timezone, ActualStart: start, ActualEnd: end, Hour: d.hour, Part: partSeen[bucket], Parts: partCounts[bucket]})
		if err != nil {
			return BatchPlan{}, err
		}
		contentID, _, err := stitchcert.CanonicalSHA(struct {
			Policy  string       `json:"policy"`
			Sources []SourceClip `json:"sources"`
		}{PlanPolicyVersion, d.sources})
		if err != nil {
			return BatchPlan{}, err
		}
		_, offset := start.In(loc).Zone()
		objectKey := path.Join("joined", batchID, relative)
		outputs = append(outputs, OutputPlan{Ordinal: i + 1, Hour: d.hour, Part: partSeen[bucket], Parts: partCounts[bucket], ActualStart: start, ActualEnd: end, UTCOffsetSec: offset, RelativePath: relative, ObjectKey: objectKey, CoverageKey: objectKey + ".coverage.json", ContentID: contentID, Sources: d.sources})
	}
	plan := BatchPlan{SchemaVersion: 1, PolicyVersion: PlanPolicyVersion, CampaignID: req.CampaignID, BatchID: batchID, RecordingID: req.RecordingID, Timezone: req.Timezone, SourceManifestSHA: manifestSHA, ExpectedOutputCount: len(outputs), CoverageObjectKey: path.Join("joined", batchID, "coverage.json"), Gaps: gaps, Outputs: outputs}
	digest, _, err := stitchcert.CanonicalSHA(plan)
	if err != nil {
		return BatchPlan{}, err
	}
	plan.PlanSHA256 = digest
	return plan, nil
}

func ValidatePlan(plan BatchPlan) error {
	want := plan.PlanSHA256
	plan.PlanSHA256 = ""
	got, _, err := stitchcert.CanonicalSHA(plan)
	if err != nil || got != want || !lowerHex64(want) || plan.SchemaVersion != 1 || plan.PolicyVersion != PlanPolicyVersion || plan.BatchID != plan.CampaignID || plan.ExpectedOutputCount != len(plan.Outputs) || len(plan.Outputs) == 0 {
		return fmt.Errorf("joined batch plan is not sealed")
	}
	prefix := "joined/" + plan.CampaignID + "/"
	seenClips, seenPaths := map[int64]bool{}, map[string]bool{}
	flat := []SourceClip{}
	for i, output := range plan.Outputs {
		contentID, _, contentErr := stitchcert.CanonicalSHA(struct {
			Policy  string       `json:"policy"`
			Sources []SourceClip `json:"sources"`
		}{PlanPolicyVersion, output.Sources})
		if contentErr != nil || output.Ordinal != i+1 || !output.ActualEnd.After(output.ActualStart) || output.ActualEnd.Sub(output.ActualStart) > time.Hour || output.ContentID != contentID || !strings.HasPrefix(output.ObjectKey, prefix) || output.CoverageKey != output.ObjectKey+".coverage.json" || seenPaths[output.RelativePath] {
			return fmt.Errorf("joined output plan identity differs")
		}
		seenPaths[output.RelativePath] = true
		for _, source := range output.Sources {
			if seenClips[source.ClipID] {
				return fmt.Errorf("joined plan assigns a source more than once")
			}
			seenClips[source.ClipID] = true
			flat = append(flat, source)
		}
	}
	manifestSHA, _, err := stitchcert.CanonicalSHA(flat)
	if err != nil || manifestSHA != plan.SourceManifestSHA {
		return fmt.Errorf("joined source manifest differs")
	}
	return nil
}

func validateSource(c SourceClip, recordingID int64) error {
	if c.ClipID <= 0 || c.RecordingID != recordingID || c.RecordingJobID <= 0 || c.CaptureGeneration == "" || c.CaptureSequence <= 0 || !lowerHex64(c.NativeSignatureSHA256) || !c.EndUTC.After(c.StartUTC) || c.EndUTC.Sub(c.StartUTC) > 15*time.Minute {
		return fmt.Errorf("invalid clip identity or range")
	}
	if !safeObjectKey(c.Object.Key) || c.Object.ETag == "" || c.Object.SizeBytes <= 0 || !lowerHex64(c.Object.SHA256) {
		return fmt.Errorf("invalid exact R2 identity")
	}
	return nil
}

func safeObjectKey(key string) bool {
	return key != "" && key != "." && key != ".." && !strings.HasPrefix(key, "/") && path.Clean(key) == key && !strings.Contains(key, "\\") && !strings.ContainsRune(key, rune(0))
}

func lowerHex64(raw string) bool {
	if len(raw) != 64 || raw != strings.ToLower(raw) {
		return false
	}
	_, err := hex.DecodeString(raw)
	return err == nil
}
