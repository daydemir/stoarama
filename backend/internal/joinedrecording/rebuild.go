package joinedrecording

import (
	"context"
	"fmt"
	"time"
)

type RebuildSourceCapability func(context.Context, WorkerClaim, SourceClip, string) (SourceReadCapability, error)

// RebuildSealedHourRenewing recreates only the bytes already frozen by a
// sealed hour after its prior worker scratch was lost. It cannot rediscover
// parts, alter quarantine evidence, or change the sealed manifest identity.
func RebuildSealedHourRenewing(ctx context.Context, claim WorkerClaim, scratchRoot string,
	client CapabilityHTTPClient, storageAuthority string, heartbeat HeartbeatOperation,
	resolveSource RebuildSourceCapability) (WorkerClaim, SealedHourScratch, error) {
	return rebuildSealedHourRenewing(ctx, claim, scratchRoot, client, storageAuthority, heartbeat, resolveSource, defaultRenewableRunner)
}

func rebuildSealedHourRenewing(ctx context.Context, claim WorkerClaim, scratchRoot string,
	client CapabilityHTTPClient, storageAuthority string, heartbeat HeartbeatOperation,
	resolveSource RebuildSourceCapability, run renewableRunner) (WorkerClaim, SealedHourScratch, error) {
	if resolveSource == nil || storageAuthority == "" || claim.StorageAuthority != storageAuthority {
		return WorkerClaim{}, SealedHourScratch{}, fmt.Errorf("sealed-hour rebuild authority is incomplete")
	}
	if err := claim.Validate(time.Now().UTC()); err != nil {
		return WorkerClaim{}, SealedHourScratch{}, err
	}
	initial := OperationCredentials{LeaseID: claim.LeaseID, OperationToken: claim.OperationToken, ExpiresAt: claim.LeaseExpires}
	currentClaim := claim
	var scratch SealedHourScratch
	err := run(ctx, initial, heartbeat, func(workCtx context.Context, current func() OperationCredentials) error {
		fresh := func() (WorkerClaim, error) { return claim.WithOperation(current()) }
		refreshed, err := fresh()
		if err != nil {
			return err
		}
		currentClaim = refreshed
		actualTool, err := InspectMediaToolEvidence(workCtx)
		if err != nil || !sameCanonical([]MediaToolEvidence{actualTool}, []MediaToolEvidence{claim.Plan.MediaTool}) {
			return fmt.Errorf("installed media tool differs from sealed hour")
		}
		requiresLosslessScratch := false
		for _, media := range claim.HourManifest.Media {
			if media.Verification.AcceptanceMode == "lossless_native_timeline_normalized" {
				requiresLosslessScratch = true
				break
			}
		}
		if err := ensureScratchHeadroom(scratchRoot, claim.Plan.Sources, requiresLosslessScratch); err != nil {
			return err
		}
		sourceOnly := sourceOnlyClips(claim.Plan.Sources)
		preflight := PreflightHourClaim{ProtocolVersion: JoinedProtocolVersion, HourID: claim.HourID,
			LeaseID: claim.LeaseID, OperationToken: currentClaim.OperationToken, LeaseExpires: currentClaim.LeaseExpires,
			BatchID: claim.Plan.BatchID, Generation: claim.Plan.Generation, RecordingID: claim.Plan.RecordingID,
			Timezone: claim.Plan.Timezone, LocalDate: claim.Plan.LocalDate, LocalHour: claim.Plan.LocalHour,
			FolderName: claim.Plan.FolderName, Metadata: claim.Plan.Metadata, AllocationLedgerSHA: claim.Plan.AllocationLedgerSHA,
			Qualification: claim.Plan.Qualification, MediaTool: claim.Plan.MediaTool,
			SourceClaimSHA256: claim.Plan.SourceClaimSHA256, Sources: sourceOnly}
		capability := func(callCtx context.Context, source SourceClip, operation string) (SourceReadCapability, error) {
			currentClaim, err := fresh()
			if err != nil {
				return SourceReadCapability{}, err
			}
			result, err := resolveSource(callCtx, currentClaim, source, operation)
			if err == nil && result.ExpiresAt.After(currentClaim.LeaseExpires) {
				return SourceReadCapability{}, fmt.Errorf("source capability outlives sealed publication lease")
			}
			return result, err
		}
		locals, directory, err := downloadClaimSources(workCtx, preflight, scratchRoot, client, storageAuthority, capability)
		if err != nil {
			return err
		}
		localByID := make(map[int64]LocalSource, len(locals))
		for i, source := range claim.Plan.Sources {
			local := locals[i]
			local.AudioContract = source.AudioContract
			localByID[source.ClipID] = local
		}
		built := make([]BuiltOutput, len(claim.Plan.Outputs))
		for i, output := range claim.Plan.Outputs {
			partSources := make([]LocalSource, len(output.Sources))
			for j, source := range output.Sources {
				local, ok := localByID[source.ClipID]
				if !ok {
					return fmt.Errorf("sealed output source is absent from rebuild")
				}
				partSources[j] = local
			}
			expected := claim.HourManifest.Media[i]
			part, err := BuildSealedOutputForVerification(workCtx, partSources, directory, expected.Verification)
			if err != nil {
				return fmt.Errorf("rebuild sealed part %d: %w", i+1, err)
			}
			if part.SizeBytes != output.ExpectedSize || part.SHA256 != output.ExpectedSHA ||
				part.SourceCount != len(output.Sources) || !sameCanonical([]Verification{part.Verification}, []Verification{expected.Verification}) {
				return fmt.Errorf("rebuilt sealed part %d identity differs", i+1)
			}
			part.SplitEvidence = append([]MaximalityEvidence(nil), expected.MaximalityEvidence...)
			built[i] = part
		}
		currentClaim, err = fresh()
		if err != nil || workCtx.Err() != nil {
			return fmt.Errorf("sealed publication lease ended before rebuild binding")
		}
		scratch, err = BindRebuiltSealedHourScratch(currentClaim, scratchRoot, directory, built,
			append([]QuarantineEvidence(nil), claim.HourManifest.QuarantineEvidence...))
		return err
	})
	if err != nil {
		return WorkerClaim{}, SealedHourScratch{}, err
	}
	return currentClaim, scratch, nil
}
