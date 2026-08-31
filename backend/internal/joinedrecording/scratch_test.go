package joinedrecording

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

func TestRequiredScratchBytesReservesLosslessFallbackAndMargin(t *testing.T) {
	t.Setenv("JOINED_LOSSLESS_NORMALIZATION_ENABLED", "true")
	sources := []SourceClip{
		{ClipID: 1, Object: ObjectIdentity{SizeBytes: 100}},
		{ClipID: 2, Object: ObjectIdentity{SizeBytes: 250}},
	}
	got, err := RequiredScratchBytes(sources)
	if err != nil {
		t.Fatal(err)
	}
	if want := uint64((1+losslessNormalizationScratchOutputMultiplier)*350) + ScratchSafetyMarginBytes; got != want {
		t.Fatalf("required scratch=%d want %d", got, want)
	}
}

func TestRequiredScratchBytesReservesAllRetainedLosslessParts(t *testing.T) {
	t.Setenv("JOINED_LOSSLESS_NORMALIZATION_ENABLED", "true")
	const sourceBytes int64 = 1 << 30
	got, err := RequiredScratchBytes([]SourceClip{{ClipID: 1, Object: ObjectIdentity{SizeBytes: sourceBytes}}})
	if err != nil {
		t.Fatal(err)
	}
	want := uint64(sourceBytes*(1+losslessNormalizationScratchOutputMultiplier)) + ScratchSafetyMarginBytes
	if got != want {
		t.Fatalf("multipart lossless scratch=%d want %d", got, want)
	}
}

func TestRequiredScratchBytesKeepsStreamCopyBudgetWithoutFallback(t *testing.T) {
	t.Setenv("JOINED_LOSSLESS_NORMALIZATION_ENABLED", "")
	got, err := RequiredScratchBytes([]SourceClip{{ClipID: 1, Object: ObjectIdentity{SizeBytes: 350}}})
	if err != nil {
		t.Fatal(err)
	}
	if want := uint64(2*350) + ScratchSafetyMarginBytes; got != want {
		t.Fatalf("required scratch=%d want %d", got, want)
	}
}

func TestWorkerTaskBudgetCannotAdmitMoreThanLosslessPreflightFits(t *testing.T) {
	const available int64 = 2 << 30
	t.Setenv("JOINED_LOSSLESS_NORMALIZATION_ENABLED", "true")
	taskBudget, err := WorkerTaskBudgetBytes(available)
	if err != nil {
		t.Fatal(err)
	}
	if taskBudget <= 0 || taskBudget >= available {
		t.Fatalf("lossless task budget=%d available=%d", taskBudget, available)
	}
	maxSource := (taskBudget - JoinedScratchFixedBytes) / 2
	if maxSource <= 0 || maxSource+maxSource*losslessNormalizationScratchOutputMultiplier > available {
		t.Fatalf("server-admitted source=%d exceeds local lossless budget=%d", maxSource, available)
	}
	nextSource := maxSource + 1
	if nextSource+nextSource*losslessNormalizationScratchOutputMultiplier <= available {
		t.Fatalf("lossless task budget was not maximal: next source=%d still fits available=%d", nextSource, available)
	}

	t.Setenv("JOINED_LOSSLESS_NORMALIZATION_ENABLED", "")
	legacy, err := WorkerTaskBudgetBytes(available)
	if err != nil || legacy != available {
		t.Fatalf("disabled fallback changed admission: budget=%d err=%v", legacy, err)
	}
}

func TestRequiredScratchBytesRejectsInvalidOrOverflowingSources(t *testing.T) {
	if _, err := RequiredScratchBytes([]SourceClip{{ClipID: 1, Object: ObjectIdentity{SizeBytes: 0}}}); err == nil {
		t.Fatal("zero-sized source was accepted")
	}
	if _, err := RequiredScratchBytes([]SourceClip{{ClipID: 1, Object: ObjectIdentity{SizeBytes: -1}}}); err == nil {
		t.Fatal("negative-sized source was accepted")
	}
}

func TestCheckScratchHeadroomFailsClosed(t *testing.T) {
	if err := checkScratchHeadroom(99, 100); err == nil {
		t.Fatal("insufficient scratch headroom was accepted")
	}
	if err := checkScratchHeadroom(100, 100); err != nil {
		t.Fatalf("exact scratch headroom rejected: %v", err)
	}
}

func TestEnsureScratchHeadroomAcceptsPrivateRoot(t *testing.T) {
	root := filepath.Join(t.TempDir(), "nested", "joined")
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := EnsureScratchHeadroom(root, []SourceClip{{ClipID: 1, Object: ObjectIdentity{SizeBytes: 1}}}); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(root)
	if err != nil {
		t.Fatal(err)
	}
	if !info.IsDir() || info.Mode().Perm() != 0o700 {
		t.Fatalf("scratch root mode=%o, want private 0700", info.Mode().Perm())
	}
}

func TestEnsureScratchHeadroomRejectsSymlinkRoot(t *testing.T) {
	parent := t.TempDir()
	target := filepath.Join(parent, "target")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(parent, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if err := EnsureScratchHeadroom(link, []SourceClip{{ClipID: 1, Object: ObjectIdentity{SizeBytes: 1}}}); err == nil {
		t.Fatal("symlink scratch root was accepted")
	}
}

func TestAvailableScratchBudgetLeavesFixedReserve(t *testing.T) {
	root := filepath.Join(t.TempDir(), "scratch")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	var stat unix.Statfs_t
	if err := unix.Statfs(root, &stat); err != nil {
		t.Fatal(err)
	}
	available := stat.Bavail * uint64(stat.Bsize)
	got, err := AvailableScratchBudget(root)
	if err != nil {
		t.Fatal(err)
	}
	want := available - ScratchSafetyMarginBytes
	if uint64(got) != want {
		t.Fatalf("scratch budget=%d want %d", got, want)
	}
}

func TestPreflightHeadroomFailsBeforeSourceCapability(t *testing.T) {
	claim, _, _ := downloadClaimFixture(t)
	tool, err := InspectMediaToolEvidence(context.Background())
	if err != nil {
		t.Skipf("pinned media tools unavailable: %v", err)
	}
	claim.MediaTool = tool
	claim.Sources[0].Object.SizeBytes = math.MaxInt64
	var resolved bool
	_, _, err = runPreflightHourRenewing(context.Background(), claim, t.TempDir(), nil,
		testSourceAuthority, noHeartbeat,
		func(context.Context, PreflightHourClaim, SourceClip, string) (SourceReadCapability, error) {
			resolved = true
			return SourceReadCapability{}, nil
		}, func(context.Context, PreflightHourClaim, SealHourRequest) (WorkerClaim, error) {
			t.Fatal("headroom failure reached seal")
			return WorkerClaim{}, nil
		}, func(ctx context.Context, initial OperationCredentials, _ HeartbeatOperation,
			work func(context.Context, func() OperationCredentials) error) error {
			return work(ctx, func() OperationCredentials { return initial })
		})
	if err == nil || !strings.Contains(err.Error(), "scratch requirement overflows") || resolved {
		t.Fatalf("headroom failure did not stop before source access: err=%v resolved=%v", err, resolved)
	}
}
