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

// V7 retains v6 admission/session/lease gates, with bounded bulk observations.
const nasCleanupBulkDurabilityClientVersion = "cleanup-batch-v7"
const nasCleanupItemTruthClientVersion = "cleanup-batch-v8"

const nasCleanupR2CopyClientVersion = "r2-copy-v2"
const nasCleanupProofClientVersion = "v85r-nas-proof-v2"

func nasCleanupSupportsBulk(version string) bool {
	switch version {
	case nasCleanupBulkDurabilityClientVersion, nasCleanupItemTruthClientVersion, nasCleanupR2CopyClientVersion, "v85r-nas-proof-v1", nasCleanupProofClientVersion:
		return true
	default:
		return false
	}
}

func nasCleanupSupportsGroupedExecution(version string) bool {
	return nasCleanupSupportsV6(version) || nasCleanupSupportsBulk(version)
}
