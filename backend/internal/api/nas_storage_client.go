package api

import (
	"crypto/rand"
	"encoding/hex"
	"time"
)

const NASStorageBudgetClientVersion = "r2-copy-v5"

// NASStorageBudgetMaxAge is server-clock policy, independent of client timers.
const NASStorageBudgetMaxAge time.Duration = 180 * time.Second

func nasStorageBudgetVersion(version string) string {
	if version == NASStorageBudgetClientVersion {
		return NASStorageBudgetClientVersion
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
