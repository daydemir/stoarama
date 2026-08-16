package surrenderplan

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestBuildWorstSupportedWindowFitsCanonicalArgument(t *testing.T) {
	start := time.Date(2026, 8, 14, 7, 0, 0, 123456000, time.UTC)
	plan, err := Build(start, start.Add(14*time.Hour), 5)
	if err != nil {
		t.Fatal(err)
	}
	if plan.ArtifactCount != 10_080 || len(plan.SplitTimesArgument) > MaxSegmentTimesArgumentLen {
		t.Fatalf("count=%d argument_bytes=%d", plan.ArtifactCount, len(plan.SplitTimesArgument))
	}
	if strings.ContainsAny(plan.SplitTimesArgument, ".eE+ ") || !strings.HasPrefix(plan.SplitTimesArgument, "5,10,15,") || !strings.HasSuffix(plan.SplitTimesArgument, "50395") {
		t.Fatalf("noncanonical segment_times argument")
	}
	if _, err := Build(start, start.Add(MaxWindow+time.Microsecond), 5); err == nil {
		t.Fatal("overlong window accepted")
	}
	if ExecFits("/ffmpeg", []string{"-segment_times", plan.SplitTimesArgument}, []string{"SECRET=never-log-this"}, 70_000) {
		t.Fatal("unsafe argv margin accepted")
	}
	if !ExecFits("/ffmpeg", []string{"-segment_times", plan.SplitTimesArgument}, []string{"SECRET=never-log-this"}, 1<<20) {
		t.Fatal("bounded argv rejected")
	}
}

func TestMerkleProofBindsEveryAuthorityField(t *testing.T) {
	set := testSet(3)
	seed := sha256.Sum256([]byte("fixed surrender plan seed"))
	root, proof, err := RootAndProof(seed, set, 2)
	if err != nil {
		t.Fatal(err)
	}
	artifact, err := DeriveArtifact(seed, set, 2)
	if err != nil {
		t.Fatal(err)
	}
	if !VerifyProof(root, set, artifact, proof) {
		t.Fatal("valid proof rejected")
	}
	mutations := []SetIdentity{set, set, set, set, set, set, set, set}
	mutations[0].AccountID++
	mutations[1].JobID++
	mutations[2].LeaseToken = uuid.MustParse("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")
	mutations[3].OriginClaimGeneration++
	mutations[4].SnapshotSHA256 = strings.Repeat("c", 64)
	mutations[5].DestinationNamingSHA256 = strings.Repeat("d", 64)
	mutations[6].MaxBytes--
	mutations[7].SetID = uuid.MustParse("cccccccc-cccc-4ccc-8ccc-cccccccccccc")
	for index, mutation := range mutations {
		if VerifyProof(root, mutation, artifact, proof) {
			t.Fatalf("mutation %d retained proof authority", index)
		}
	}
	artifact.RecoverySecret[0] ^= 1
	if VerifyProof(root, set, artifact, proof) {
		t.Fatal("secret substitution retained proof authority")
	}
	if got := hex.EncodeToString(root[:]); got == strings.Repeat("0", 64) {
		t.Fatal("zero Merkle root")
	}
}

func TestMerkleOddPaddingAndProofDepth(t *testing.T) {
	seed := sha256.Sum256([]byte("odd tree"))
	for _, count := range []int{1, 2, 3, 7, 8, 9} {
		set := testSet(count)
		for ordinal := 1; ordinal <= count; ordinal++ {
			root, proof, err := RootAndProof(seed, set, ordinal)
			if err != nil {
				t.Fatalf("count=%d ordinal=%d: %v", count, ordinal, err)
			}
			artifact, _ := DeriveArtifact(seed, set, ordinal)
			if !VerifyProof(root, set, artifact, proof) {
				t.Fatalf("count=%d ordinal=%d proof rejected", count, ordinal)
			}
		}
	}
}

func testSet(count int) SetIdentity {
	return SetIdentity{
		SetID:                   uuid.MustParse("11111111-1111-4111-8111-111111111111"),
		AccountID:               7,
		RecordingID:             11,
		JobID:                   13,
		LeaseToken:              uuid.MustParse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
		OriginClaimGeneration:   2,
		ProducerID:              uuid.MustParse("22222222-2222-4222-8222-222222222222"),
		SnapshotSHA256:          strings.Repeat("a", 64),
		DestinationNamingSHA256: strings.Repeat("b", 64),
		ArtifactCount:           count,
		MIME:                    "video/mp4",
		MaxBytes:                RecoveryArtifactMaxBytes,
	}
}
