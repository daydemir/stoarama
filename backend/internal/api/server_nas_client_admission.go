package api

import (
	"context"
	"errors"
	"net/http"
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

// These literals name releases with the current raw-copy contract. A version
// prefix or a source checkout's default label never qualifies for media work.
func nasCopySupportsVersion(version string) bool {
	switch version {
	case nasCleanupR2CopyProgressClientVersion, nasCleanupR2CopyClientVersion, "v85r-nas-proof-v1", nasCleanupProofClientVersion,
		nasCleanupR2CopyControlClientVersion, nasCleanupItemTruthClientVersion, nasCleanupFreshPermitClientVersion:
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
