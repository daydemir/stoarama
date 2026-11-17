package api

// NASStorageBudgetClientVersion refreshes storage after payload assembly and
// before every actual heartbeat send/retry. A receipt from any other literal
// cannot establish the send-time age bound used by finish admission.
const NASStorageBudgetClientVersion = "r2-copy-v5"

func nasStorageBudgetVersion(version string) string {
	if version == NASStorageBudgetClientVersion {
		return NASStorageBudgetClientVersion
	}
	return ""
}
