package joinedrecording

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestStageTimingObserverReceivesSafeDeterministicFields(t *testing.T) {
	var got []StageTimingEvent
	ctx := WithStageTimingObserver(context.Background(), func(event StageTimingEvent) { got = append(got, event) })
	emitStageTiming(ctx, "download", 1234*time.Millisecond, nil)
	emitStageTiming(ctx, "upload_verify", 7*time.Millisecond, errors.New("signed URL must not escape"))
	want := []StageTimingEvent{
		{Stage: "download", ElapsedMS: 1234, Outcome: "ok"},
		{Stage: "upload_verify", ElapsedMS: 7, Outcome: "error"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("events=%+v want=%+v", got, want)
	}
}

func TestNilStageTimingObserverLeavesContextUsable(t *testing.T) {
	ctx := WithStageTimingObserver(context.Background(), nil)
	emitStageTiming(ctx, "download", time.Second, nil)
	if err := ctx.Err(); err != nil {
		t.Fatal(err)
	}
}

func TestUploadVerifyFailureStagesAreClosedAndBounded(t *testing.T) {
	stages := []UploadVerifyFailureStage{
		UploadVerifyFailureManifestIdentity, UploadVerifyFailurePartScratch, UploadVerifyFailurePartLocalIdentity,
		UploadVerifyFailurePartUpload, UploadVerifyFailureManifestUpload, UploadVerifyFailureManifestCapability,
		UploadVerifyFailureManifestReconcile,
	}
	for _, stage := range stages {
		if !stage.Valid() {
			t.Fatalf("declared stage %q is invalid", stage)
		}
	}
	if UploadVerifyFailureStage("signed-url-secret").Valid() {
		t.Fatal("arbitrary diagnostic stage was accepted")
	}
	var got StageTimingEvent
	ctx := WithStageTimingObserver(context.Background(), func(event StageTimingEvent) { got = event })
	emitUploadVerifyFailure(ctx, time.Now().Add(-time.Millisecond), UploadVerifyFailurePartUpload, 646, 29)
	if got.Stage != "upload_verify" || got.Outcome != "error" || got.FailureStage != UploadVerifyFailurePartUpload ||
		got.ArtifactID != 646 || got.ArtifactOrdinal != 29 {
		t.Fatalf("bounded diagnostic differs: %+v", got)
	}
}

func TestStageTimingExtractsOnlySealValidationCoordinates(t *testing.T) {
	var got StageTimingEvent
	ctx := WithStageTimingObserver(context.Background(), func(event StageTimingEvent) { got = event })
	err := fmt.Errorf("%w: %w", ErrPreflightSealRequestInvalid,
		newSealValidationError(SealValidationMaximalityProof, 59, 1, "joined hour seal maximality differs"))
	emitStageTiming(ctx, "build_verify", time.Millisecond, err)
	if got.ValidationClass != SealValidationMaximalityProof || got.MediaOrdinal != 59 || got.ProofOrdinal != 1 {
		t.Fatalf("validation timing coordinates differ: %+v", got)
	}
	if strings.Contains(fmt.Sprint(got), "joined hour seal maximality differs") {
		t.Fatalf("validation payload escaped into timing event: %+v", got)
	}
}
