package joinedrecording

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const scratchReapPrefix = ".reap-"
const scratchLeaseProofLimit = 256

// ScratchLeaseProof marks leases that the API proved cannot own live work.
// Missing and false entries are retained. Cleanup must fail closed when proof
// is unavailable or incomplete.
type ScratchLeaseProof func(context.Context, []string) (map[string]bool, error)

// CleanupInactiveLeaseScratch removes only direct lease directories whose
// exact lease IDs the API proves inactive or expired. It never follows a
// symlink and leaves unrelated root entries untouched.
func CleanupInactiveLeaseScratch(ctx context.Context, root string, prove ScratchLeaseProof) ([]string, error) {
	if prove == nil {
		return nil, fmt.Errorf("joined scratch lease proof is required")
	}
	if err := validatePrivateScratchRoot(root); err != nil {
		return nil, err
	}
	pathInfo, err := os.Lstat(root)
	if err != nil {
		return nil, fmt.Errorf("stat joined scratch root: %w", err)
	}
	rootHandle, err := os.OpenRoot(root)
	if err != nil {
		return nil, fmt.Errorf("open joined scratch root: %w", err)
	}
	defer rootHandle.Close()
	openedInfo, err := rootHandle.Stat(".")
	if err != nil || !os.SameFile(pathInfo, openedInfo) {
		return nil, fmt.Errorf("joined scratch root changed while opening")
	}
	rootDir, err := rootHandle.Open(".")
	if err != nil {
		return nil, fmt.Errorf("open joined scratch directory: %w", err)
	}
	entries, err := rootDir.ReadDir(-1)
	closeErr := rootDir.Close()
	if err != nil {
		return nil, fmt.Errorf("read joined scratch root: %w", err)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("close joined scratch root: %w", closeErr)
	}
	leaseSet := map[string]bool{}
	for _, entry := range entries {
		leaseID := entry.Name()
		if strings.HasPrefix(leaseID, scratchReapPrefix) {
			leaseID = strings.TrimPrefix(leaseID, scratchReapPrefix)
		}
		if validLeaseID(leaseID) {
			leaseSet[leaseID] = true
		}
	}
	leaseIDs := make([]string, 0, len(leaseSet))
	for leaseID := range leaseSet {
		leaseIDs = append(leaseIDs, leaseID)
	}
	sort.Strings(leaseIDs)
	if len(leaseIDs) == 0 {
		return nil, nil
	}
	proof := make(map[string]bool, len(leaseIDs))
	for start := 0; start < len(leaseIDs); start += scratchLeaseProofLimit {
		end := min(start+scratchLeaseProofLimit, len(leaseIDs))
		page, err := prove(ctx, leaseIDs[start:end])
		if err != nil {
			return nil, fmt.Errorf("prove joined scratch leases inactive: %w", err)
		}
		for leaseID, inactive := range page {
			if !leaseSet[leaseID] {
				return nil, fmt.Errorf("joined scratch lease proof included an unrequested lease")
			}
			if _, duplicate := proof[leaseID]; duplicate {
				return nil, fmt.Errorf("joined scratch lease proof repeated a lease")
			}
			proof[leaseID] = inactive
		}
	}
	for _, leaseID := range leaseIDs {
		if _, ok := proof[leaseID]; !ok {
			return nil, fmt.Errorf("joined scratch lease proof omitted lease %s", leaseID)
		}
	}

	removed := []string{}
	for _, leaseID := range leaseIDs {
		if !proof[leaseID] {
			continue
		}
		quarantine := scratchReapPrefix + leaseID
		original := leaseID
		if err := removeProvenScratchPath(rootHandle, quarantine); err != nil {
			return removed, err
		}
		if info, err := rootHandle.Lstat(original); err == nil {
			if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
				return removed, fmt.Errorf("joined lease scratch is not a directory: %s", leaseID)
			}
			if err := rootHandle.Rename(original, quarantine); err != nil {
				return removed, fmt.Errorf("fence joined lease scratch %s: %w", leaseID, err)
			}
			if err := removeProvenScratchPath(rootHandle, quarantine); err != nil {
				return removed, err
			}
		} else if !os.IsNotExist(err) {
			return removed, fmt.Errorf("stat joined lease scratch %s: %w", leaseID, err)
		}
		removed = append(removed, leaseID)
	}
	return removed, nil
}

func validatePrivateScratchRoot(root string) error {
	if !filepath.IsAbs(root) || filepath.Clean(root) != root || root == string(filepath.Separator) {
		return fmt.Errorf("joined scratch root must be a clean absolute non-root path")
	}
	info, err := os.Lstat(root)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() || info.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("joined scratch root must be a private directory")
	}
	return nil
}

func removeProvenScratchPath(root *os.Root, name string) error {
	if root == nil || filepath.Base(name) != name || filepath.Clean(name) != name {
		return fmt.Errorf("refusing joined scratch cleanup outside root")
	}
	info, err := root.Lstat(name)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("stat joined scratch cleanup path: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("joined scratch cleanup path is not a directory")
	}
	if err := root.RemoveAll(name); err != nil {
		return fmt.Errorf("remove inactive joined scratch: %w", err)
	}
	return nil
}
