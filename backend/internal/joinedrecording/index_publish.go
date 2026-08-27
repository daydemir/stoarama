package joinedrecording

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"
)

type BatchIndexPublicationClaim struct {
	ProtocolVersion  int        `json:"protocol_version"`
	ScopeID          string     `json:"scope_id"`
	ArtifactID       int64      `json:"artifact_id"`
	LeaseID          string     `json:"lease_id"`
	OperationToken   string     `json:"operation_token"`
	LeaseExpires     time.Time  `json:"lease_expires_at"`
	StorageAuthority string     `json:"storage_authority"`
	StorageBucket    string     `json:"storage_bucket"`
	Index            BatchIndex `json:"index"`
	ExpectedSize     int64      `json:"expected_size_bytes"`
	ExpectedSHA256   string     `json:"expected_sha256"`
}

type PublishedBatchIndex struct {
	ArtifactID int64  `json:"artifact_id"`
	ObjectKey  string `json:"object_key"`
	ETag       string `json:"etag"`
	VersionID  string `json:"version_id,omitempty"`
	SizeBytes  int64  `json:"size_bytes"`
	SHA256     string `json:"sha256"`
}

type BatchIndexCreateCapabilityResolver func(context.Context, BatchIndexPublicationClaim) (ObjectCreateCapability, error)
type BatchIndexReadCapabilityResolver func(context.Context, BatchIndexPublicationClaim) (ObjectReadCapability, error)
type FinalizeBatchIndex func(context.Context, BatchIndexPublicationClaim, PublishedBatchIndex) error

func (c BatchIndexPublicationClaim) Validate(now time.Time) ([]byte, string, error) {
	_, canonical, sha, err := canonicalSealedBatchIndex(c.Index)
	objectKey, keyErr := CanonicalBatchIndexObjectKey(c.Index.BatchID)
	if err != nil || keyErr != nil || c.ProtocolVersion != JoinedProtocolVersion || c.ScopeID != c.Index.BatchID || c.ArtifactID <= 0 || !validLeaseID(c.LeaseID) || !validOperationToken(c.OperationToken) || !c.LeaseExpires.After(now) || c.StorageAuthority == "" || c.StorageBucket == "" || c.ExpectedSize != int64(len(canonical)) || c.ExpectedSHA256 != sha {
		return nil, "", fmt.Errorf("invalid or expired batch-index publication claim")
	}
	return canonical, objectKey, nil
}

func (c BatchIndexPublicationClaim) WithOperation(credentials OperationCredentials) (BatchIndexPublicationClaim, error) {
	if credentials.LeaseID != c.LeaseID || !validOperationToken(credentials.OperationToken) || !credentials.ExpiresAt.After(time.Now()) || credentials.ExpiresAt.Before(c.LeaseExpires) {
		return BatchIndexPublicationClaim{}, fmt.Errorf("renewed batch-index operation differs")
	}
	c.OperationToken, c.LeaseExpires = credentials.OperationToken, credentials.ExpiresAt
	return c, nil
}

func publishBatchIndex(ctx context.Context, client CapabilityHTTPClient, claim BatchIndexPublicationClaim, resolveCreate BatchIndexCreateCapabilityResolver, resolveRead BatchIndexReadCapabilityResolver, finalize FinalizeBatchIndex) (PublishedBatchIndex, error) {
	return publishBatchIndexWithDeadline(ctx, client, claim, resolveCreate, resolveRead, finalize, func() time.Time { return claim.LeaseExpires })
}

func publishBatchIndexWithDeadline(ctx context.Context, client CapabilityHTTPClient, claim BatchIndexPublicationClaim, resolveCreate BatchIndexCreateCapabilityResolver, resolveRead BatchIndexReadCapabilityResolver, finalize FinalizeBatchIndex, leaseExpires func() time.Time) (PublishedBatchIndex, error) {
	canonical, objectKey, err := claim.Validate(time.Now().UTC())
	if client == nil || resolveCreate == nil || resolveRead == nil || finalize == nil || err != nil {
		return PublishedBatchIndex{}, fmt.Errorf("valid capability client and fenced batch-index claim are required")
	}
	if _, err = putCreateOnlyWithRetry(ctx, client, claim.StorageAuthority, claim.StorageBucket, claim.ArtifactID, objectKey, "application/json", claim.ExpectedSize, claim.ExpectedSHA256, leaseExpires, claim.LeaseID,
		func(callCtx context.Context) (ObjectCreateCapability, error) { return resolveCreate(callCtx, claim) },
		func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(canonical)), nil }, defaultPutRetryPolicy()); err != nil {
		return PublishedBatchIndex{}, err
	}
	read, err := resolveRead(ctx, claim)
	if err != nil {
		return PublishedBatchIndex{}, err
	}
	head, err := reconcileExactCapability(ctx, client, claim.StorageAuthority, claim.StorageBucket, claim.ArtifactID, objectKey, claim.ExpectedSize, claim.ExpectedSHA256, read.ETag, read.VersionID, read)
	if err != nil {
		return PublishedBatchIndex{}, err
	}
	published := PublishedBatchIndex{ArtifactID: claim.ArtifactID, ObjectKey: objectKey, ETag: head.ETag, VersionID: head.VersionID, SizeBytes: claim.ExpectedSize, SHA256: claim.ExpectedSHA256}
	if err := finalize(ctx, claim, published); err != nil {
		return PublishedBatchIndex{}, fmt.Errorf("immutable batch index verified but fenced reconciliation remains pending: %w", err)
	}
	return published, nil
}

func PublishBatchIndexRenewing(ctx context.Context, client CapabilityHTTPClient, storageAuthority string, claim BatchIndexPublicationClaim, heartbeat HeartbeatOperation, resolveCreate BatchIndexCreateCapabilityResolver, resolveRead BatchIndexReadCapabilityResolver, finalize FinalizeBatchIndex) (PublishedBatchIndex, error) {
	if storageAuthority == "" || claim.StorageAuthority != storageAuthority {
		return PublishedBatchIndex{}, fmt.Errorf("batch-index publication authority differs from configured storage")
	}
	return publishBatchIndexRenewing(ctx, client, claim, heartbeat, resolveCreate, resolveRead, finalize, defaultRenewableRunner)
}

func publishBatchIndexRenewing(ctx context.Context, client CapabilityHTTPClient, claim BatchIndexPublicationClaim, heartbeat HeartbeatOperation, resolveCreate BatchIndexCreateCapabilityResolver, resolveRead BatchIndexReadCapabilityResolver, finalize FinalizeBatchIndex, run renewableRunner) (PublishedBatchIndex, error) {
	var published PublishedBatchIndex
	initial := OperationCredentials{LeaseID: claim.LeaseID, OperationToken: claim.OperationToken, ExpiresAt: claim.LeaseExpires}
	err := run(ctx, initial, heartbeat, func(workCtx context.Context, current func() OperationCredentials) error {
		fresh := func() (BatchIndexPublicationClaim, error) { return claim.WithOperation(current()) }
		create := func(callCtx context.Context, _ BatchIndexPublicationClaim) (ObjectCreateCapability, error) {
			if err := callCtx.Err(); err != nil {
				return ObjectCreateCapability{}, err
			}
			currentClaim, err := fresh()
			if err != nil {
				return ObjectCreateCapability{}, err
			}
			return resolveCreate(callCtx, currentClaim)
		}
		read := func(callCtx context.Context, _ BatchIndexPublicationClaim) (ObjectReadCapability, error) {
			if err := callCtx.Err(); err != nil {
				return ObjectReadCapability{}, err
			}
			currentClaim, err := fresh()
			if err != nil {
				return ObjectReadCapability{}, err
			}
			capability, err := resolveRead(callCtx, currentClaim)
			if err == nil && capability.ExpiresAt.After(currentClaim.LeaseExpires) {
				return ObjectReadCapability{}, fmt.Errorf("batch-index read capability outlives current publication lease")
			}
			return capability, err
		}
		finish := func(callCtx context.Context, _ BatchIndexPublicationClaim, output PublishedBatchIndex) error {
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
		published, err = publishBatchIndexWithDeadline(workCtx, client, claim, create, read, finish, func() time.Time { return current().ExpiresAt })
		return err
	})
	return published, err
}
