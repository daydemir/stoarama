package stitchproof

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os/exec"
	"time"
)

type Replay struct {
	Schema                    string  `json:"schema"`
	NativeReplay              string  `json:"native_replay"`
	ConnectionID              int64   `json:"connection_id"`
	RunID                     string  `json:"run_id"`
	ReceiptSHA256             string  `json:"receipt_sha256"`
	PlanSHA256                string  `json:"plan_sha256"`
	ClientArtifactSHA256      string  `json:"client_artifact_sha256"`
	OutputSHA256              string  `json:"output_sha256"`
	OutputSizeBytes           int64   `json:"output_size_bytes"`
	MediaSeconds              float64 `json:"media_seconds"`
	BuildWallSeconds          float64 `json:"build_wall_seconds"`
	RecordingHoursPerWallHour float64 `json:"recording_hours_per_wall_hour"`
}

type cappedOutput struct {
	bytes.Buffer
	cancel context.CancelFunc
}

func (w *cappedOutput) Write(p []byte) (int, error) {
	if w.Len()+len(p) > 64<<10 {
		w.cancel()
		return 0, errors.New("native replay output limit")
	}
	return w.Buffer.Write(p)
}

func replay(ctx context.Context, verifier, client, plan, bundle []byte, planSHA, bundleSHA string) (Replay, error) {
	var result Replay
	if len(verifier) > 64<<10 || digest(verifier) != VerifierSHA || len(client) > 8<<20 || digest(client) != ClientSHA || len(plan) > 1<<20 || digest(plan) != planSHA || len(bundle) > 1<<20 || digest(bundle) != bundleSHA {
		return result, errors.New("native replay input identity differs")
	}
	body, err := json.Marshal(struct {
		Schema string `json:"schema"`
		Client string `json:"client_source_b64"`
		Plan   string `json:"source_plan_b64"`
		Bundle string `json:"native_bundle_b64"`
	}{"stoarama.nas-stitch-replay-input.v1", base64.StdEncoding.EncodeToString(client), base64.StdEncoding.EncodeToString(plan), base64.StdEncoding.EncodeToString(bundle)})
	if err != nil || len(body) > 16<<20 {
		return result, errors.New("native replay input bound")
	}
	ctx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	// System Python is absolute, isolated and receives no inherited credentials.
	// The verified script bytes are the -c argument; no source path is reopened.
	cmd := exec.CommandContext(ctx, "/usr/bin/python3", "-I", "-S", "-B", "-c", string(verifier), "replay-stream", "--expected-client-sha256", ClientSHA, "--expected-sha256", bundleSHA, "--expected-plan-sha256", planSHA)
	cmd.Env = []string{"PATH=/usr/bin:/bin", "LC_ALL=C"}
	cmd.Dir = "/"
	cmd.Stdin = bytes.NewReader(body)
	stdout, stderr := &cappedOutput{cancel: cancel}, &cappedOutput{cancel: cancel}
	cmd.Stdout, cmd.Stderr = stdout, stderr
	if err := cmd.Run(); err != nil {
		return result, errors.New("pinned native inventory replay failed")
	}
	if err := decode(stdout.Bytes(), &result); err != nil {
		return result, err
	}
	return result, nil
}
