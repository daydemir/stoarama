package api

import (
	"crypto/rand"
	"encoding/hex"
	"time"
)

const NASStorageBudgetClientVersion = "r2-copy-v5"

// NASStorageBudgetMaxAge is server-clock policy, independent of client timers.
const NASStorageBudgetMaxAge time.Duration = 180 * time.Second

// SupportsNASStorageBudget is shared with every finish-controller admission
// gate. Each immutable release binds the same sample/nonce contract.
func SupportsNASStorageBudget(version string) bool {
	switch version {
	case NASStorageBudgetClientVersion, NASR2CopyPagedClientVersion, nasCleanupCertifiedStitchVersion, nasLowMemoryCopyClientVersion, nasLowMemoryCleanupClientVersion, nasKeep20CleanupClientVersion, nasFastCopyClientVersion, nasTunableCopyClientVersion, nasTunableDayClientVersion, nasCleanupDayModesClientVersion, nasCleanupDayKEClientVersion, nasOutputRestoreClientVersion:
		return true
	default:
		return false
	}
}
func nasStorageBudgetVersion(version string) string {
	if SupportsNASStorageBudget(version) {
		return version
	}
	return ""
}

func newNASStorageChallenge() (string, error) {
	var nonce [32]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(nonce[:]), nil
}

func storageChallenge(storage *connectionStorageStatus) string {
	if storage == nil {
		return ""
	}
	return storage.ChallengeNonce
}
