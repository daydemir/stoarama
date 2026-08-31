package joinedrecording

import (
	"fmt"
	"math"
	"os"

	"github.com/daydemir/stoarama/backend/internal/r2"
	"golang.org/x/sys/unix"
)

// AvailableScratchBudget reports the bytes a new broad-rollout task may claim.
// It leaves the fixed scratch margin unallocated before admission. The task's
// own source/output requirement retains the same margin as a second guard.
func AvailableScratchBudget(root string) (int64, error) {
	if err := validatePrivateScratchRoot(root); err != nil {
		return 0, err
	}
	var stat unix.Statfs_t
	if err := unix.Statfs(root, &stat); err != nil {
		return 0, fmt.Errorf("read joined scratch headroom: %w", err)
	}
	available := stat.Bavail * uint64(stat.Bsize)
	if available <= ScratchSafetyMarginBytes {
		return 0, &ScratchHeadroomError{Available: available, Required: ScratchSafetyMarginBytes + 1}
	}
	budget := available - ScratchSafetyMarginBytes
	if budget > math.MaxInt64 {
		budget = math.MaxInt64
	}
	return int64(budget), nil
}

// WorkerTaskBudgetBytes translates the worker's real free-space budget into
// the legacy 2x admission model used by the claim API. With normalization
// disabled it preserves the existing broad-rollout behavior exactly. With it
// enabled, every source set admitted by the server also fits the local
// source-plus-bounded-QP0-output preflight.
func WorkerTaskBudgetBytes(available int64) (int64, error) {
	if available <= 0 {
		return 0, fmt.Errorf("joined scratch admission budget is invalid")
	}
	if !losslessNormalizationEnabled() {
		return available, nil
	}
	fits := func(source int64) bool {
		legacy, err := RequiredScratchBudgetBytes(source)
		if err != nil || legacy > available {
			return false
		}
		output := int64(r2.MaxConditionalPutBytes)
		if source <= math.MaxInt64/losslessNormalizationExpansionLimit {
			if expanded := source * losslessNormalizationExpansionLimit; expanded < output {
				output = expanded
			}
		}
		return source <= available-output
	}
	low, high := int64(0), available
	for low < high {
		mid := low + (high-low+1)/2
		if fits(mid) {
			low = mid
		} else {
			high = mid - 1
		}
	}
	if low <= 0 {
		return 0, &ScratchHeadroomError{Available: uint64(available), Required: uint64(JoinedScratchFixedBytes) + 8}
	}
	taskBudget, err := RequiredScratchBudgetBytes(low)
	if err != nil || taskBudget > available {
		return 0, fmt.Errorf("derive joined lossless scratch admission budget")
	}
	return taskBudget, nil
}

// ScratchSafetyMarginBytes leaves room for filesystem metadata, ffmpeg
// temporary files, and the final verification pass. It is deliberately large
// because a failed space check must stop before any source download begins.
const ScratchSafetyMarginBytes uint64 = 256 << 20

type ScratchHeadroomError struct {
	Available uint64
	Required  uint64
}

func (e *ScratchHeadroomError) Error() string {
	return fmt.Sprintf("joined scratch headroom is insufficient: available=%d required=%d", e.Available, e.Required)
}

// RequiredScratchBytes reserves the complete frozen source set plus the
// bounded QP 0 fallback output, then adds a fixed safety margin. The fallback
// may expand compact source media by at most seven times and can never exceed
// the single-object conditional PUT cap.
func RequiredScratchBytes(sources []SourceClip) (uint64, error) {
	return requiredScratchBytes(sources, losslessNormalizationEnabled())
}

func requiredScratchBytes(sources []SourceClip, lossless bool) (uint64, error) {
	var sourceBytes uint64
	for _, source := range sources {
		if source.Object.SizeBytes <= 0 {
			return 0, fmt.Errorf("joined source %d has invalid size", source.ClipID)
		}
		size := uint64(source.Object.SizeBytes)
		if sourceBytes > math.MaxUint64-size {
			return 0, fmt.Errorf("joined source scratch size overflows")
		}
		sourceBytes += size
	}
	outputBytes := sourceBytes
	if lossless {
		outputBytes = uint64(r2.MaxConditionalPutBytes)
		if sourceBytes <= math.MaxUint64/uint64(losslessNormalizationExpansionLimit) {
			if expanded := sourceBytes * uint64(losslessNormalizationExpansionLimit); expanded < outputBytes {
				outputBytes = expanded
			}
		}
	}
	maxScratch := uint64(math.MaxInt64)
	if outputBytes > maxScratch-ScratchSafetyMarginBytes || sourceBytes > maxScratch-outputBytes-ScratchSafetyMarginBytes {
		return 0, fmt.Errorf("joined scratch requirement overflows")
	}
	return sourceBytes + outputBytes + ScratchSafetyMarginBytes, nil
}

func checkScratchHeadroom(available, required uint64) error {
	if available < required {
		return &ScratchHeadroomError{Available: available, Required: required}
	}
	return nil
}

// EnsureScratchHeadroom fails closed before a worker downloads any source.
func EnsureScratchHeadroom(root string, sources []SourceClip) error {
	return ensureScratchHeadroom(root, sources, losslessNormalizationEnabled())
}

func ensureScratchHeadroom(root string, sources []SourceClip, lossless bool) error {
	required, err := requiredScratchBytes(sources, lossless)
	if err != nil {
		return err
	}
	info, err := os.Lstat(root)
	if err != nil {
		return fmt.Errorf("stat joined scratch root: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("joined scratch root must be a private directory")
	}
	// The worker startup gate validates the dedicated root's private mode. Do
	// not chmod or otherwise mutate an existing directory in this per-claim
	// check; this helper only rejects a replaced final-component symlink.
	var stat unix.Statfs_t
	if err := unix.Statfs(root, &stat); err != nil {
		return fmt.Errorf("read joined scratch headroom: %w", err)
	}
	available := stat.Bavail * uint64(stat.Bsize)
	return checkScratchHeadroom(available, required)
}
