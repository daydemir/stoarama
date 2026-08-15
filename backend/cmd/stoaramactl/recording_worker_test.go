package main

import (
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/recordingworker"
)

func TestCloudRecorderMediaLagFence(t *testing.T) {
	cfg := recordingworker.Config{}
	applyCloudRecorderContinuousSafety(&cfg)
	if cfg.ContinuousMaxMediaLag != 15*time.Minute {
		t.Fatalf("cloud media lag fence=%s, want 15m", cfg.ContinuousMaxMediaLag)
	}
	if cfg.ContinuousNoProgressTimeout != 5*time.Minute {
		t.Fatalf("cloud no-progress timeout=%s, want 5m", cfg.ContinuousNoProgressTimeout)
	}
}
