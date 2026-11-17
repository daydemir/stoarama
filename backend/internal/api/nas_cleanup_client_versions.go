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
const nasCleanupR2CopyDiagnosticsClientVersion = "r2-copy-v5"
const nasCleanupR2CopyProgressClientVersion = "r2-copy-v4"
const nasCleanupR2CopyControlClientVersion = "r2-copy-v3"
const nasCleanupProofClientVersion = "v85r-nas-proof-v2"
const nasCleanupFreshPermitClientVersion = "v85r-nas-proof-v2.2"
const nasCleanupPermitTransportClientVersion = "v85r-nas-proof-v2.1"

// Capability eligibility only. Native producer/current reader artifact profiles
// and explicit operator authorization remain separate, exact bindings.
type nasStitchProofVersion string

const (
	nasStitchProofV2Version        nasStitchProofVersion = nasCleanupProofClientVersion
	nasStitchProofCopyVersion      nasStitchProofVersion = nasCleanupR2CopyControlClientVersion
	nasStitchProofItemTruthVersion nasStitchProofVersion = nasCleanupItemTruthClientVersion
)

func nasClientSupportsStitchProof(version string) bool {
	switch nasStitchProofVersion(version) {
	case nasStitchProofV2Version, nasStitchProofCopyVersion, nasStitchProofItemTruthVersion:
		return true
	default:
		return false
	}
}

func nasCleanupSupportsBulk(version string) bool {
	switch version {
	case nasCleanupR2CopyDiagnosticsClientVersion, nasCleanupR2CopyProgressClientVersion, nasCleanupBulkDurabilityClientVersion, nasCleanupItemTruthClientVersion, nasCleanupR2CopyClientVersion, nasCleanupR2CopyControlClientVersion, "v85r-nas-proof-v1", nasCleanupProofClientVersion, nasCleanupPermitTransportClientVersion, nasCleanupFreshPermitClientVersion:
		return true
	default:
		return false
	}
}

func nasCleanupSupportsGroupedExecution(version string) bool {
	return nasCleanupSupportsV6(version) || nasCleanupSupportsBulk(version)
}
