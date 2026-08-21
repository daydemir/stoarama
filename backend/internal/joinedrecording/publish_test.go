package joinedrecording

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/google/uuid"
)

type memoryImmutableStore struct {
	objects map[string][]byte
}

func (s *memoryImmutableStore) PutReaderIfAbsentVerified(_ context.Context, key, _ string, body io.Reader, size int64, sha string) (r2.ObjectHead, bool, error) {
	b, err := io.ReadAll(body)
	if err != nil {
		return r2.ObjectHead{}, false, err
	}
	sum := sha256.Sum256(b)
	if int64(len(b)) != size || hex.EncodeToString(sum[:]) != sha {
		return r2.ObjectHead{}, false, errors.New("identity mismatch")
	}
	existing, ok := s.objects[key]
	if ok && !bytes.Equal(existing, b) {
		return r2.ObjectHead{}, false, errors.New("immutable conflict")
	}
	if !ok {
		s.objects[key] = append([]byte(nil), b...)
	}
	return r2.ObjectHead{ETag: "etag", VersionID: "version", SizeBytes: size}, !ok, nil
}

func oneOutputClaim(t *testing.T) WorkerClaim {
	t.Helper()
	start := time.Date(2026, time.May, 4, 8, 0, 0, 0, time.UTC)
	plan, err := BuildPlan(testRequest([]SourceClip{testSource(1, start)}))
	if err != nil {
		t.Fatal(err)
	}
	return WorkerClaim{TaskID: 7, ClaimToken: uuid.NewString(), LeaseExpires: time.Now().Add(time.Hour), BatchPlanSHA: plan.PlanSHA256, BatchID: plan.BatchID, CampaignID: plan.CampaignID, RecordingID: plan.RecordingID, SourcePlanSHA: plan.SourceManifestSHA, ExpectedCount: plan.ExpectedOutputCount, Output: plan.Outputs[0]}
}

func TestPublishClaimedOutputReconcilesImmutableOrphanAndCleansOnlyScratch(t *testing.T) {
	claim := oneOutputClaim(t)
	root := t.TempDir()
	scratch, err := claim.ScratchDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(scratch, 0700); err != nil {
		t.Fatal(err)
	}
	mediaPath := filepath.Join(scratch, "joined.mp4")
	media := []byte("verified joined media")
	if err := os.WriteFile(mediaPath, media, 0600); err != nil {
		t.Fatal(err)
	}
	size, sha, err := localIdentity(mediaPath)
	if err != nil {
		t.Fatal(err)
	}
	built := BuiltOutput{Path: mediaPath, SizeBytes: size, SHA256: sha, SourceCount: 1, Verification: Verification{Status: "passed", PacketPayloadOrderStatus: "passed", DecodedFrameTotalsStatus: "passed", DecodedAudioTotalsStatus: "passed", OutputTimestampStatus: "passed", StrictDecodeStatus: "passed"}}
	store := &memoryImmutableStore{objects: map[string][]byte{}}
	if _, err := PublishClaimedOutput(context.Background(), store, claim, built, scratch, func(context.Context, WorkerClaim, PublishedOutput) error { return errors.New("database unavailable") }); err == nil {
		t.Fatal("database orphan was reported as finalized")
	}
	if _, err := os.Stat(mediaPath); err != nil {
		t.Fatal("scratch output removed before fenced finalize")
	}
	if len(store.objects) != 2 {
		t.Fatalf("objects=%d want media+coverage", len(store.objects))
	}
	var finalized bool
	published, err := PublishClaimedOutput(context.Background(), store, claim, built, scratch, func(_ context.Context, got WorkerClaim, output PublishedOutput) error {
		finalized = got.ClaimToken == claim.ClaimToken && output.TaskID == claim.TaskID
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !finalized || published.Created || !lowerHex64(published.CoverageSHA256) {
		t.Fatalf("orphan not reconciled: %+v", published)
	}
	if _, err := os.Stat(mediaPath); !os.IsNotExist(err) {
		t.Fatal("finalized scratch output was not removed")
	}
}

func TestWorkerClaimRejectsExpiredAndOverHourTasks(t *testing.T) {
	claim := oneOutputClaim(t)
	claim.LeaseExpires = time.Now().Add(-time.Second)
	if err := claim.Validate(time.Now()); err == nil {
		t.Fatal("expired claim accepted")
	}
	claim = oneOutputClaim(t)
	claim.Output.ActualEnd = claim.Output.ActualStart.Add(time.Hour + time.Nanosecond)
	if err := claim.Validate(time.Now()); err == nil {
		t.Fatal("over-hour task accepted")
	}
}
