package joinedrecording

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"
)

type LedgerPublicationClaim struct {
	ProtocolVersion  int                 `json:"protocol_version"`
	ArtifactID       int64               `json:"artifact_id"`
	ScopeID          string              `json:"scope_id"`
	LeaseID          string              `json:"lease_id"`
	OperationToken   string              `json:"operation_token"`
	LeaseExpires     time.Time           `json:"lease_expires_at"`
	StorageAuthority string              `json:"storage_authority"`
	StorageBucket    string              `json:"storage_bucket"`
	BatchID          string              `json:"batch_id"`
	Ledger           StreamDayAllocation `json:"ledger"`
	ExpectedSize     int64               `json:"expected_size_bytes"`
	ExpectedSHA256   string              `json:"expected_sha256"`
}

type PublishedLedger struct {
	ArtifactID int64  `json:"artifact_id"`
	ObjectKey  string `json:"object_key"`
	ETag       string `json:"etag"`
	VersionID  string `json:"version_id,omitempty"`
	SizeBytes  int64  `json:"size_bytes"`
	SHA256     string `json:"sha256"`
}

type FinalizeLedger func(context.Context, LedgerPublicationClaim, PublishedLedger) error
type LedgerReadCapabilityResolver func(context.Context, LedgerPublicationClaim) (ObjectReadCapability, error)
type LedgerCreateCapabilityResolver func(context.Context, LedgerPublicationClaim) (ObjectCreateCapability, error)

func (c LedgerPublicationClaim) Validate(now time.Time) ([]byte, string, error) {
	canonical, sha, err := CanonicalAllocationLedgerArtifact(c.Ledger)
	_, objectKey, pathErr := CanonicalAllocationLedgerPaths(c.BatchID, c.Ledger.RecordingID, c.Ledger.LocalDate)
	if err != nil || pathErr != nil || c.ProtocolVersion != JoinedProtocolVersion || c.BatchID != c.Ledger.BatchID || c.ScopeID != canonicalLedgerID(c.BatchID, c.Ledger.RecordingID, c.Ledger.LocalDate, c.Ledger.Generation) || c.ArtifactID <= 0 || !validLeaseID(c.LeaseID) || !validOperationToken(c.OperationToken) || !c.LeaseExpires.After(now) || c.StorageAuthority == "" || c.StorageBucket == "" || c.ExpectedSize != int64(len(canonical)) || c.ExpectedSHA256 != sha {
		return nil, "", fmt.Errorf("invalid or expired allocation-ledger publication claim")
	}
	return canonical, objectKey, nil
}

// PublishAllocationLedger uses an independent renewable publication lease;
// hour preflight can start only after its exact ledger artifact is published.
func publishAllocationLedger(ctx context.Context, client CapabilityHTTPClient, claim LedgerPublicationClaim, resolveCreate LedgerCreateCapabilityResolver, resolveRead LedgerReadCapabilityResolver, finalize FinalizeLedger) (PublishedLedger, error) {
	return publishAllocationLedgerWithDeadline(ctx, client, claim, resolveCreate, resolveRead, finalize, func() time.Time { return claim.LeaseExpires })
}

func publishAllocationLedgerWithDeadline(ctx context.Context, client CapabilityHTTPClient, claim LedgerPublicationClaim, resolveCreate LedgerCreateCapabilityResolver, resolveRead LedgerReadCapabilityResolver, finalize FinalizeLedger, leaseExpires func() time.Time) (PublishedLedger, error) {
	if client == nil || resolveCreate == nil || resolveRead == nil || finalize == nil {
		return PublishedLedger{}, fmt.Errorf("capability client and fenced ledger finalizer are required")
	}
	canonical, objectKey, err := claim.Validate(time.Now().UTC())
	if err != nil {
		return PublishedLedger{}, err
	}
	_, err = putCreateOnlyWithRetry(ctx, client, claim.StorageAuthority, claim.StorageBucket, claim.ArtifactID, objectKey, "application/json", claim.ExpectedSize, claim.ExpectedSHA256, leaseExpires, claim.LeaseID,
		func(callCtx context.Context) (ObjectCreateCapability, error) { return resolveCreate(callCtx, claim) },
		func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(canonical)), nil }, defaultPutRetryPolicy())
	if err != nil {
		return PublishedLedger{}, err
	}
	readCapability, err := resolveRead(ctx, claim)
	if err != nil {
		return PublishedLedger{}, fmt.Errorf("resolve exact allocation-ledger reread capability: %w", err)
	}
	head, err := reconcileExactCapability(ctx, client, claim.StorageAuthority, claim.StorageBucket, claim.ArtifactID, objectKey, claim.ExpectedSize, claim.ExpectedSHA256, readCapability.ETag, readCapability.VersionID, readCapability)
	if err != nil {
		return PublishedLedger{}, err
	}
	published := PublishedLedger{ArtifactID: claim.ArtifactID, ObjectKey: objectKey, ETag: head.ETag, VersionID: head.VersionID, SizeBytes: claim.ExpectedSize, SHA256: claim.ExpectedSHA256}
	if err := finalize(ctx, claim, published); err != nil {
		return PublishedLedger{}, fmt.Errorf("immutable allocation ledger verified but fenced reconciliation remains pending: %w", err)
	}
	return published, nil
}

func (c LedgerPublicationClaim) WithOperation(credentials OperationCredentials) (LedgerPublicationClaim, error) {
	if credentials.LeaseID != c.LeaseID || !validOperationToken(credentials.OperationToken) || !credentials.ExpiresAt.After(time.Now()) || credentials.ExpiresAt.Before(c.LeaseExpires) {
		return LedgerPublicationClaim{}, fmt.Errorf("renewed ledger operation differs")
	}
	c.OperationToken, c.LeaseExpires = credentials.OperationToken, credentials.ExpiresAt
	return c, nil
}

func PublishAllocationLedgerRenewing(ctx context.Context, client CapabilityHTTPClient, storageAuthority string, claim LedgerPublicationClaim, heartbeat HeartbeatOperation, resolveCreate LedgerCreateCapabilityResolver, resolveRead LedgerReadCapabilityResolver, finalize FinalizeLedger) (PublishedLedger, error) {
	if storageAuthority == "" || claim.StorageAuthority != storageAuthority {
		return PublishedLedger{}, fmt.Errorf("ledger publication authority differs from configured storage")
	}
	return publishAllocationLedgerRenewing(ctx, client, claim, heartbeat, resolveCreate, resolveRead, finalize, defaultRenewableRunner)
}

func publishAllocationLedgerRenewing(ctx context.Context, client CapabilityHTTPClient, claim LedgerPublicationClaim, heartbeat HeartbeatOperation, resolveCreate LedgerCreateCapabilityResolver, resolveRead LedgerReadCapabilityResolver, finalize FinalizeLedger, run renewableRunner) (PublishedLedger, error) {
	var published PublishedLedger
	initial := OperationCredentials{LeaseID: claim.LeaseID, OperationToken: claim.OperationToken, ExpiresAt: claim.LeaseExpires}
	err := run(ctx, initial, heartbeat, func(workCtx context.Context, current func() OperationCredentials) error {
		fresh := func() (LedgerPublicationClaim, error) { return claim.WithOperation(current()) }
		create := func(callCtx context.Context, _ LedgerPublicationClaim) (ObjectCreateCapability, error) {
			if err := callCtx.Err(); err != nil {
				return ObjectCreateCapability{}, err
			}
			currentClaim, err := fresh()
			if err != nil {
				return ObjectCreateCapability{}, err
			}
			return resolveCreate(callCtx, currentClaim)
		}
		read := func(callCtx context.Context, _ LedgerPublicationClaim) (ObjectReadCapability, error) {
			if err := callCtx.Err(); err != nil {
				return ObjectReadCapability{}, err
			}
			currentClaim, err := fresh()
			if err != nil {
				return ObjectReadCapability{}, err
			}
			capability, err := resolveRead(callCtx, currentClaim)
			if err == nil && capability.ExpiresAt.After(currentClaim.LeaseExpires) {
				return ObjectReadCapability{}, fmt.Errorf("ledger read capability outlives current publication lease")
			}
			return capability, err
		}
		finish := func(callCtx context.Context, _ LedgerPublicationClaim, output PublishedLedger) error {
			if err := callCtx.Err(); err != nil {
				return err
			}
			currentClaim, err := fresh()
			if err != nil {
				return err
			}
			return finalize(callCtx, currentClaim, output)
		}
		var err error
		published, err = publishAllocationLedgerWithDeadline(workCtx, client, claim, create, read, finish, func() time.Time { return current().ExpiresAt })
		return err
	})
	return published, err
}
