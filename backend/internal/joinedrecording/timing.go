package joinedrecording

import (
	"context"
	"time"
)

// StageTimingEvent reports one bounded worker stage without exposing storage
// capabilities, object keys, or credentials.
type StageTimingEvent struct {
	Stage           string
	ElapsedMS       int64
	Outcome         string
	FailureStage    UploadVerifyFailureStage
	ArtifactID      int64
	ArtifactOrdinal int
}

// UploadVerifyFailureStage is a closed, non-sensitive publication checkpoint.
// It never contains an error string or storage request data.
type UploadVerifyFailureStage string

const (
	UploadVerifyFailureManifestIdentity   UploadVerifyFailureStage = "manifest_identity"
	UploadVerifyFailurePartScratch        UploadVerifyFailureStage = "part_scratch"
	UploadVerifyFailurePartLocalIdentity  UploadVerifyFailureStage = "part_local_identity"
	UploadVerifyFailurePartUpload         UploadVerifyFailureStage = "part_upload"
	UploadVerifyFailureManifestUpload     UploadVerifyFailureStage = "manifest_upload"
	UploadVerifyFailureManifestCapability UploadVerifyFailureStage = "manifest_capability"
	UploadVerifyFailureManifestReconcile  UploadVerifyFailureStage = "manifest_reconcile"
)

func (s UploadVerifyFailureStage) Valid() bool {
	switch s {
	case UploadVerifyFailureManifestIdentity, UploadVerifyFailurePartScratch,
		UploadVerifyFailurePartLocalIdentity, UploadVerifyFailurePartUpload,
		UploadVerifyFailureManifestUpload, UploadVerifyFailureManifestCapability,
		UploadVerifyFailureManifestReconcile:
		return true
	default:
		return false
	}
}

type stageTimingObserverKey struct{}

// WithStageTimingObserver installs process-local timing observation. It does
// not change the joined-recording protocol or persist timing data.
func WithStageTimingObserver(ctx context.Context, observer func(StageTimingEvent)) context.Context {
	if observer == nil {
		return ctx
	}
	return context.WithValue(ctx, stageTimingObserverKey{}, observer)
}

func emitStageTiming(ctx context.Context, stage string, elapsed time.Duration, err error) {
	observer, _ := ctx.Value(stageTimingObserverKey{}).(func(StageTimingEvent))
	if observer == nil {
		return
	}
	outcome := "ok"
	if err != nil {
		outcome = "error"
	}
	observer(StageTimingEvent{Stage: stage, ElapsedMS: elapsed.Milliseconds(), Outcome: outcome})
}

func emitUploadVerifyFailure(ctx context.Context, started time.Time, failureStage UploadVerifyFailureStage,
	artifactID int64, ordinal int) {
	observer, _ := ctx.Value(stageTimingObserverKey{}).(func(StageTimingEvent))
	if observer == nil {
		return
	}
	if !failureStage.Valid() {
		failureStage = ""
	}
	if artifactID < 0 {
		artifactID = 0
	}
	if ordinal < 0 {
		ordinal = 0
	}
	observer(StageTimingEvent{Stage: "upload_verify", ElapsedMS: time.Since(started).Milliseconds(), Outcome: "error",
		FailureStage: failureStage, ArtifactID: artifactID, ArtifactOrdinal: ordinal})
}
