package joinedrecording

import (
	"context"
	"fmt"
)

type PreflightSourceCapability func(context.Context, PreflightHourClaim, SourceClip, string) (SourceReadCapability, error)
type SealPreflightHour func(context.Context, PreflightHourClaim, SealHourRequest) (WorkerClaim, error)

// RunPreflightHourRenewing owns the complete source-only lease lifecycle. It
// heartbeats while downloading, inspecting, building, and verifying, then
// seals once with the newest same-lease token and binds those exact scratch
// bytes to the returned publication lease.
func RunPreflightHourRenewing(ctx context.Context, claim PreflightHourClaim, scratchRoot string, client CapabilityHTTPClient, storageAuthority string, heartbeat HeartbeatOperation, resolveSource PreflightSourceCapability, seal SealPreflightHour) (WorkerClaim, SealedHourScratch, error) {
	return runPreflightHourRenewing(ctx, claim, scratchRoot, client, storageAuthority, heartbeat, resolveSource, seal, defaultRenewableRunner)
}

func runPreflightHourRenewing(ctx context.Context, claim PreflightHourClaim, scratchRoot string, client CapabilityHTTPClient, storageAuthority string, heartbeat HeartbeatOperation, resolveSource PreflightSourceCapability, seal SealPreflightHour, run renewableRunner) (WorkerClaim, SealedHourScratch, error) {
	if resolveSource == nil || seal == nil || run == nil {
		return WorkerClaim{}, SealedHourScratch{}, fmt.Errorf("preflight capability resolver and sealer are required")
	}
	initial := OperationCredentials{LeaseID: claim.LeaseID, OperationToken: claim.OperationToken, ExpiresAt: claim.LeaseExpires}
	var sealed WorkerClaim
	var sealedScratch SealedHourScratch
	err := run(ctx, initial, heartbeat, func(workCtx context.Context, current func() OperationCredentials) error {
		fresh := func() (PreflightHourClaim, error) { return claim.WithOperation(current()) }
		actualTool, err := InspectMediaToolEvidence(workCtx)
		if err != nil || !sameCanonical([]MediaToolEvidence{actualTool}, []MediaToolEvidence{claim.MediaTool}) {
			return fmt.Errorf("installed media tool differs from frozen claim")
		}
		sourceCapability := func(callCtx context.Context, source SourceClip, operation string) (SourceReadCapability, error) {
			if err := callCtx.Err(); err != nil {
				return SourceReadCapability{}, err
			}
			currentClaim, err := fresh()
			if err != nil {
				return SourceReadCapability{}, err
			}
			capability, err := resolveSource(callCtx, currentClaim, source, operation)
			if err == nil && capability.ExpiresAt.After(currentClaim.LeaseExpires) {
				return SourceReadCapability{}, fmt.Errorf("source capability outlives current preflight lease")
			}
			return capability, err
		}
		locals, scratchDir, err := downloadClaimSources(workCtx, claim, scratchRoot, client, storageAuthority, sourceCapability)
		if err != nil {
			return err
		}
		includedSources, frozenLocals, initialQuarantined, initialQuarantines, err := freezeDownloadedAudioForPreflight(workCtx, claim.Sources, locals, claim.MediaTool.IdentitySHA256)
		if err != nil {
			return err
		}
		includedSources, initialQuarantined = deriveClaimedHourSeams(claim.Sources, includedSources, initialQuarantined)
		request := PlanRequest{BatchID: claim.BatchID, Generation: claim.Generation, RecordingID: claim.RecordingID, Timezone: claim.Timezone, LocalDate: claim.LocalDate, DeliveryHour: claim.LocalHour, FolderName: claim.FolderName, Metadata: claim.Metadata, Qualification: claim.Qualification, AllocationLedgerSHA: claim.AllocationLedgerSHA, MediaTool: claim.MediaTool, Sources: includedSources, QuarantinedSources: initialQuarantined}
		preflight := HourPreflight{}
		if len(includedSources) > 0 {
			draft, discoverErr := DiscoverHourPlan(request)
			if discoverErr != nil {
				return discoverErr
			}
			preflight, err = PreflightHour(workCtx, draft, frozenLocals, scratchDir, claim.MediaTool.IdentitySHA256)
			if err != nil {
				return err
			}
		}
		request.Sources = preflight.Sources
		request.QuarantinedSources = append(append([]SourceClip(nil), initialQuarantined...), preflight.Quarantined...)
		request.BuiltArtifacts = make([]BuiltArtifactIdentity, len(preflight.Built))
		for i, built := range preflight.Built {
			request.BuiltArtifacts[i] = BuiltArtifactIdentity{SizeBytes: built.SizeBytes, SHA256: built.SHA256, MediaToolIdentity: claim.MediaTool.IdentitySHA256}
		}
		var plan BatchPlan
		if len(preflight.Built) == 0 && len(request.QuarantinedSources) == len(claim.Sources) {
			quarantineRequest := request
			quarantineRequest.Sources, quarantineRequest.QuarantinedSources, quarantineRequest.BuiltArtifacts = deriveAllClaimedHourSeams(claim.Sources), nil, nil
			plan, err = BuildQuarantineOnlyHourPlan(quarantineRequest, claim.LocalDate, claim.LocalHour, "deterministic_media_quarantine")
		} else {
			plan, err = BuildPlan(request)
		}
		if err != nil {
			return err
		}
		quarantine := quarantineEvidenceFromBuilds(append(initialQuarantines, preflight.Quarantines...))
		sealRequest := sealHourRequest(claim, plan, preflight.Built, quarantine)
		currentClaim, err := fresh()
		if err != nil || workCtx.Err() != nil {
			return fmt.Errorf("preflight lease ended before seal")
		}
		sealed, err = seal(workCtx, currentClaim, sealRequest)
		if err != nil {
			return err
		}
		verified := verifiedHourScratch{HourID: claim.HourID, SourceClaimSHA256: plan.SourceClaimSHA256, OriginLeaseID: claim.LeaseID, Directory: scratchDir, Built: preflight.Built, Quarantine: quarantine}
		sealedScratch, err = bindSealedHourScratch(verified, sealed)
		return err
	})
	return sealed, sealedScratch, err
}

func deriveClaimedHourSeams(original, included, quarantined []SourceClip) ([]SourceClip, []SourceClip) {
	includedByID := make(map[int64]SourceClip, len(included))
	quarantinedByID := make(map[int64]SourceClip, len(quarantined))
	for _, source := range included {
		includedByID[source.ClipID] = source
	}
	for _, source := range quarantined {
		quarantinedByID[source.ClipID] = source
	}
	includedOut, quarantinedOut := make([]SourceClip, 0, len(included)), make([]SourceClip, 0, len(quarantined))
	for _, derived := range deriveAllClaimedHourSeams(original) {
		if source, ok := includedByID[derived.ClipID]; ok {
			source.SeamToPrevious = derived.SeamToPrevious
			includedOut = append(includedOut, source)
		} else if source, ok := quarantinedByID[derived.ClipID]; ok {
			source.SeamToPrevious = derived.SeamToPrevious
			quarantinedOut = append(quarantinedOut, source)
		}
	}
	return includedOut, quarantinedOut
}

func deriveAllClaimedHourSeams(sources []SourceClip) []SourceClip {
	out := sourceOnlyClips(sources)
	for i := 1; i < len(out); i++ {
		gap := out[i].StartUTC.Sub(out[i-1].EndUTC).Nanoseconds()
		out[i].SeamToPrevious = SeamEvidence{Verdict: "continuous", Reason: "timestamp_adjacent_preflight_candidate", SignedGapNanoseconds: gap}
		if gap != 0 {
			out[i].SeamToPrevious.Verdict = "gap"
			out[i].SeamToPrevious.Reason = "signed_presentation_gap"
		}
	}
	return out
}

func quarantineEvidenceFromBuilds(builds []QuarantinedBuild) []QuarantineEvidence {
	out := make([]QuarantineEvidence, len(builds))
	for i, build := range builds {
		evidence := build.Evidence
		out[i] = QuarantineEvidence{ReasonCode: evidence.ReasonCode, SourceClipIDs: append([]int64(nil), evidence.CandidateClipIDs...), SourceClaimSHA256: evidence.SourceClaimSHA256, PolicyVersion: evidence.PolicyVersion, NormalizedFacts: append([]byte(nil), evidence.FailureFacts...), FailureSHA256: evidence.FailureSHA256, EvidenceSHA256: evidence.EvidenceSHA256, AttemptCount: evidence.RepeatCount, MediaToolIdentity: evidence.MediaToolIdentity}
	}
	return out
}

func sealHourRequest(claim PreflightHourClaim, plan BatchPlan, built []BuiltOutput, quarantine []QuarantineEvidence) SealHourRequest {
	media := make([]SealHourMedia, len(built))
	for i, output := range plan.Outputs {
		ids := make([]int64, len(output.Sources))
		for j := range output.Sources {
			ids[j] = output.Sources[j].ClipID
		}
		media[i] = SealHourMedia{Ordinal: i + 1, SourceClipIDs: ids, SizeBytes: built[i].SizeBytes, SHA256: built[i].SHA256, Verification: built[i].Verification, MaximalityEvidence: append([]MaximalityEvidence(nil), built[i].SplitEvidence...)}
	}
	return SealHourRequest{ProtocolVersion: JoinedProtocolVersion, HourID: claim.HourID, SourceClaimSHA256: plan.SourceClaimSHA256, AccountedSources: append([]SourceClip(nil), plan.Sources...), Media: media, Quarantine: quarantine}
}
