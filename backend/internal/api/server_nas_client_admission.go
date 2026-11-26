package api

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"strings"

	"github.com/daydemir/stoarama/backend/internal/util"
	"github.com/jackc/pgx/v5"
)

type nasConnectionPurpose uint8

const (
	nasConnectionMetadata nasConnectionPurpose = iota
	nasConnectionCleanup
	nasConnectionCopy
	nasConnectionProof
	nasConnectionInventory
)

func nasCleanupSupportsExecution(version string) bool {
	switch version {
	case nasCleanupExecutionClientVersion, nasCleanupSerialExecutionClientVersion,
		nasCleanupDedicatedExecutionClientVersion, nasCleanupRawCreditDrainClientVersion,
		nasCleanupFenceProgressClientVersion, nasCleanupJoinedMarkerClientVersion,
		nasCleanupV85RClientVersion, nasCleanupV85RServerSwitchClientVersion,
		nasCleanupV85RCorpusUAClientVersion, nasCleanupRefusalClientVersion,
		nasCleanupListingProofClientVersion, nasCleanupListingFenceClientVersion,
		nasCleanupPipelineClientVersion, nasCleanupGroupClientVersion,
		nasCleanupSerialGroupClientVersion, nasCleanupDurableGroupClientVersion,
		nasCleanupBatchedCapsClientVersion, nasCleanupDeliveryHoldClientVersion:
		return true
	default:
		return nasCleanupSupportsGroupedExecution(version)
	}
}

const nasLowMemoryCopyClientVersion = "r2-copy-v8-low-memory"

// The cleanup release is the same low-memory artifact line plus the reviewed
// cleanup-batch-v8 protocol. It is a separate immutable literal: copy-only
// admission for the older release stays exactly as deployed.
const nasLowMemoryCleanupClientVersion = "r2-copy-v8-low-memory-cleanup"

// The keep20 cleanup release is the same artifact line built after the
// 20-day cleanup pin (#589), so its independent deletion guard embeds the
// 20-day windows. Separate immutable literal, identical capabilities.
const nasKeep20CleanupClientVersion = "r2-copy-v8-keep20-cleanup"

const nasFastCopyClientVersion = "r2-copy-v8-fast-copy"

// The tunable release is the fast-copy artifact line with its stream count
// supplied by each lease (connections.r2_fast_copy_workers). The first literal also
// marks name+size recovery evidence, whichever fast release observed it.
const nasTunableCopyClientVersion = "r2-copy-v8-tunable-copy"

// The tunable-day release is the tunable line whose day-directory unlink pass
// no longer waits for app writers to drain (2026-11-24).
const nasTunableDayClientVersion = "r2-copy-v8-tunable-day"

// The tunable-modes release receives day permits in names or dir mode
// (server_nas_cleanup_day.go) instead of filtering a listing by date.
const nasCleanupDayModesClientVersion = "r2-copy-v8-tunable-modes"

// The ke-day release is the tunable-modes line that also accepts Keep
// Everything day permits, always in names mode, listing only files whose R2
// counterpart the server found live (server_nas_cleanup_day.go).
const nasCleanupDayKEClientVersion = "r2-copy-v8-ke-day"

// The output-restore release is the ke-day line that also leases NAS-only
// stitched-output restores (it sends output_restores, server_nas_restore.go).
// Its server capabilities are exactly ke-day's.
const nasOutputRestoreClientVersion = "r2-copy-v8-output-restore"

// The inventory release is the output-restore line that also leases read-only
// stream-day NAS inventories (server_nas_day_inventory.go). Every other server
// capability is exactly output-restore's.
const nasDayInventoryClientVersion = "r2-copy-v8-inventory"

// Only these releases lease stream-day inventories.
var nasDayInventoryVersions = []string{nasDayInventoryClientVersion}

// These releases receive day permits with a mode.
var nasCleanupDayModeVersions = []string{nasCleanupDayModesClientVersion, nasCleanupDayKEClientVersion, nasOutputRestoreClientVersion, nasDayInventoryClientVersion}

// Only these releases receive Keep Everything day permits (names mode).
var nasCleanupDayKEVersions = []string{nasCleanupDayKEClientVersion, nasOutputRestoreClientVersion, nasDayInventoryClientVersion}

var nasFastCopyVersions = []string{nasFastCopyClientVersion, nasTunableCopyClientVersion, nasTunableDayClientVersion, nasCleanupDayModesClientVersion, nasCleanupDayKEClientVersion, nasOutputRestoreClientVersion, nasDayInventoryClientVersion}

// Only these releases receive day-directory permits.
var nasCleanupDayVersions = []string{nasTunableCopyClientVersion, nasTunableDayClientVersion, nasCleanupDayModesClientVersion, nasCleanupDayKEClientVersion, nasOutputRestoreClientVersion, nasDayInventoryClientVersion}

func nasFastCopyLine(version string) bool {
	return slices.Contains(nasFastCopyVersions, version)
}

// These releases have raw-copy authority only.
func nasLowMemoryRawCopyOnly(version string) bool {
	switch version {
	case nasLowMemoryCopyClientVersion, nasLowMemoryCleanupClientVersion, nasKeep20CleanupClientVersion, nasFastCopyClientVersion, nasTunableCopyClientVersion, nasTunableDayClientVersion, nasCleanupDayModesClientVersion, nasCleanupDayKEClientVersion, nasOutputRestoreClientVersion, nasDayInventoryClientVersion:
		return true
	default:
		return false
	}
}

// These literals name releases with the current raw-copy contract. A version
// prefix or a source checkout's default label never qualifies for media work.
func nasCopySupportsVersion(version string) bool {
	switch version {
	case nasCleanupCertifiedStitchVersion, NASR2CopyPagedClientVersion, nasCleanupR2CopyDiagnosticsClientVersion, nasCleanupR2CopyProgressClientVersion, nasCleanupR2CopyClientVersion, "v85r-nas-proof-v1", nasCleanupProofClientVersion,
		nasCleanupR2CopyControlClientVersion, nasCleanupItemTruthClientVersion, nasCleanupFreshPermitClientVersion,
		nasLowMemoryCopyClientVersion, nasLowMemoryCleanupClientVersion, nasKeep20CleanupClientVersion, nasFastCopyClientVersion, nasTunableCopyClientVersion, nasTunableDayClientVersion, nasCleanupDayModesClientVersion, nasCleanupDayKEClientVersion, nasOutputRestoreClientVersion, nasDayInventoryClientVersion:
		return true
	default:
		return false
	}
}

func nasRequestWorkVersion(r *http.Request, purpose nasConnectionPurpose) (string, bool) {
	values := r.Header.Values("User-Agent")
	if len(values) != 1 {
		return "", false
	}
	version, ok := strings.CutPrefix(values[0], "stoarama-nas-pull/")
	if !ok {
		return "", false
	}
	switch purpose {
	case nasConnectionCleanup:
		return version, nasCleanupSupportsExecution(version)
	case nasConnectionCopy:
		return version, nasCopySupportsVersion(version)
	case nasConnectionProof:
		return version, nasClientSupportsStitchProof(version)
	case nasConnectionInventory:
		return version, slices.Contains(nasDayInventoryVersions, version)
	default:
		return "", false
	}
}

// The request identity closes the restart-before-heartbeat gap. Matching the
// stored version uses the existing indexed connection lookup, without another
// query or row lock. Existing lane-specific freshness and session guards remain.
func (s *Server) nasConnection(ctx context.Context, w http.ResponseWriter, r *http.Request, purpose nasConnectionPurpose) (int64, bool) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return 0, false
	}
	if principal.APIKeyID == nil {
		util.WriteError(w, http.StatusForbidden, "operation requires a NAS pull key")
		return 0, false
	}
	query := `SELECT id FROM connections WHERE api_key_id=$1 AND account_id=$2 AND kind='nas_pull'`
	args := []any{*principal.APIKeyID, principal.AccountID}
	if purpose != nasConnectionMetadata {
		version, admitted := nasRequestWorkVersion(r, purpose)
		if !admitted {
			util.WriteError(w, http.StatusConflict, "reviewed NAS client release required")
			return 0, false
		}
		query += ` AND client_version=$3`
		args = append(args, version)
	}
	var connectionID int64
	err := s.pool.QueryRow(ctx, query, args...).Scan(&connectionID)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusForbidden, "no connection for this key")
		return 0, false
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "load restore connection failed")
		return 0, false
	}
	return connectionID, true
}
