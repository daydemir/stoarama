package api

// These immutable releases execute exactly the cleanup-batch-v6 protocol.
// Additions require review of the complete client artifact, never a prefix test.
type nasCleanupV6Version string

const (
	nasCleanupV6Legacy    nasCleanupV6Version = nasCleanupRollingCapsClientVersion
	nasCleanupV6Inventory nasCleanupV6Version = nasCleanupInventoryClientVersion
	nasCleanupV6R2Copy    nasCleanupV6Version = "r2-copy-v1"
)

func nasCleanupSupportsV6(version string) bool {
	switch nasCleanupV6Version(version) {
	case nasCleanupV6Legacy, nasCleanupV6Inventory, nasCleanupV6R2Copy:
		return true
	default:
		return false
	}
}
