package joinedrecording

import (
	"fmt"
	"math"
	"os"

	"golang.org/x/sys/unix"
)

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

// RequiredScratchBytes reserves the complete frozen source set plus an equal
// amount for the largest joined output, then adds a fixed safety margin. The
// worker currently keeps all verified source files while ffmpeg builds output.
func RequiredScratchBytes(sources []SourceClip) (uint64, error) {
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
	if sourceBytes > (math.MaxUint64-ScratchSafetyMarginBytes)/2 {
		return 0, fmt.Errorf("joined scratch requirement overflows")
	}
	return sourceBytes*2 + ScratchSafetyMarginBytes, nil
}

func checkScratchHeadroom(available, required uint64) error {
	if available < required {
		return &ScratchHeadroomError{Available: available, Required: required}
	}
	return nil
}

// EnsureScratchHeadroom fails closed before a worker downloads any source.
func EnsureScratchHeadroom(root string, sources []SourceClip) error {
	required, err := RequiredScratchBytes(sources)
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
