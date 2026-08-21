package joinedrecording

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"time"

	"github.com/daydemir/stoarama/backend/internal/recordingnaming"
)

// PreflightHourClaim is the source-only lease used before output identities
// exist. Final outputs are built once from these sources, then token-sealed.
type PreflightHourClaim struct {
	ProtocolVersion     int                      `json:"protocol_version"`
	HourID              string                   `json:"hour_id"`
	LeaseID             string                   `json:"lease_id"`
	OperationToken      string                   `json:"operation_token"`
	LeaseExpires        time.Time                `json:"lease_expires_at"`
	BatchID             string                   `json:"batch_id"`
	Generation          int                      `json:"generation"`
	RecordingID         int64                    `json:"recording_id"`
	Timezone            string                   `json:"timezone"`
	LocalDate           string                   `json:"local_date"`
	LocalHour           int                      `json:"local_hour"`
	FolderName          string                   `json:"folder_name"`
	Metadata            recordingnaming.Metadata `json:"naming_metadata"`
	AllocationLedgerSHA string                   `json:"allocation_ledger_sha256"`
	Qualification       QualificationWindow      `json:"qualification_window"`
	MediaTool           MediaToolEvidence        `json:"media_tool"`
	SourceClaimSHA256   string                   `json:"source_claim_sha256"`
	Sources             []SourceClip             `json:"sources"`
}

func (c PreflightHourClaim) Validate(now time.Time) error {
	if c.ProtocolVersion != JoinedProtocolVersion || !validLeaseID(c.LeaseID) || !validOperationToken(c.OperationToken) || !c.LeaseExpires.After(now) || !safeBatchID.MatchString(c.BatchID) || c.Generation <= 0 || c.RecordingID <= 0 || c.LocalHour < 1 || c.LocalHour > 12 || c.HourID != canonicalHourIDValue(c.BatchID, c.RecordingID, c.LocalDate, c.LocalHour, c.Generation) || !lowerHex64(c.AllocationLedgerSHA) || ValidateQualificationWindow(c.Qualification) != nil || ValidateMediaToolEvidence(c.MediaTool) != nil || len(c.Sources) == 0 {
		return fmt.Errorf("invalid preflight hour lease")
	}
	seen := map[int64]bool{}
	for _, source := range c.Sources {
		if seen[source.ClipID] || source.AudioContract != nil || source.SeamToPrevious != (SeamEvidence{}) || validatePreflightSource(source, c.RecordingID) != nil || !c.Qualification.permits(source) {
			return fmt.Errorf("preflight hour source identity differs")
		}
		seen[source.ClipID] = true
	}
	digest, _, err := sourceClaimSHA(c.Sources)
	if err != nil || digest != c.SourceClaimSHA256 {
		return fmt.Errorf("preflight hour source manifest differs")
	}
	return nil
}

func validatePreflightSource(source SourceClip, recordingID int64) error {
	_, endpointErr := CanonicalSourceEndpointAuthority(source.Endpoint)
	if source.ClipID <= 0 || source.RecordingID != recordingID || source.RecordingJobID <= 0 || source.Provider == "" || endpointErr != nil || source.Region == "" || source.Bucket == "" || !source.EndUTC.After(source.StartUTC) || !safeObjectKey(source.Object.Key) || !validObjectIdentity(source.Object.ETag, source.Object.VersionID) || source.Object.SizeBytes <= 0 || !lowerHex64(source.Object.SHA256) {
		return fmt.Errorf("invalid exact preflight source identity")
	}
	return nil
}

func (c PreflightHourClaim) ScratchDir(root string) (string, error) {
	if err := c.Validate(time.Now().UTC()); err != nil {
		return "", err
	}
	if !filepath.IsAbs(root) {
		return "", fmt.Errorf("scratch root is required")
	}
	return leaseScratchDir(root, c.LeaseID), nil
}

func (c PreflightHourClaim) WithOperation(credentials OperationCredentials) (PreflightHourClaim, error) {
	if credentials.LeaseID != c.LeaseID || !validOperationToken(credentials.OperationToken) || !credentials.ExpiresAt.After(time.Now()) || credentials.ExpiresAt.Before(c.LeaseExpires) {
		return PreflightHourClaim{}, fmt.Errorf("renewed preflight operation differs")
	}
	c.OperationToken, c.LeaseExpires = credentials.OperationToken, credentials.ExpiresAt
	return c, nil
}

func leaseScratchDir(root, leaseID string) string {
	return filepath.Join(root, leaseID)
}

func validOperationToken(token string) bool { return len(token) >= 16 && len(token) <= 8192 }

var leaseIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{43}$`)

func validLeaseID(id string) bool { return leaseIDPattern.MatchString(id) }

// freezeDownloadedAudioForPreflight probes each exact source independently.
// Only a byte-identical deterministic failure from two fresh ffprobe
// processes may quarantine a source; infrastructure failures retry the hour.
func freezeDownloadedAudioForPreflight(ctx context.Context, sources []SourceClip, locals []LocalSource, mediaToolIdentity string) ([]SourceClip, []LocalSource, []SourceClip, []QuarantinedBuild, error) {
	if len(sources) != len(locals) || !lowerHex64(mediaToolIdentity) {
		return nil, nil, nil, nil, fmt.Errorf("downloaded source cardinality differs")
	}
	includedSources := make([]SourceClip, 0, len(sources))
	includedLocals := make([]LocalSource, 0, len(locals))
	quarantinedSources := []SourceClip{}
	quarantines := []QuarantinedBuild{}
	for i := range sources {
		source := sourceOnlyClips([]SourceClip{sources[i]})[0]
		local := locals[i]
		if source.ClipID != local.ClipID || verifyLocalIdentity(local) != nil {
			return nil, nil, nil, nil, fmt.Errorf("downloaded source identity differs")
		}
		claimSHA, _, err := sourceClaimSHA([]SourceClip{source})
		if err != nil {
			return nil, nil, nil, nil, err
		}
		local.SourceClaimSHA256 = claimSHA
		_, _, audio, probeErr := probeMediaMetadata(ctx, local.Path)
		if probeErr == nil {
			source.AudioContract, local.AudioContract = audio, audio
			includedSources, includedLocals = append(includedSources, source), append(includedLocals, local)
			continue
		}
		firstErr := deterministicEvidenceFailure(ctx, "corrupt_source_media", probeErr)
		var first *deterministicMediaError
		if !errors.As(firstErr, &first) {
			return nil, nil, nil, nil, firstErr
		}
		_, _, _, repeatProbeErr := probeMediaMetadata(ctx, local.Path)
		repeatErr := deterministicEvidenceFailure(ctx, "corrupt_source_media", repeatProbeErr)
		var repeated *deterministicMediaError
		if repeatProbeErr == nil || !errors.As(repeatErr, &repeated) || first.code != repeated.code || first.evidenceSHA256 != repeated.evidenceSHA256 {
			return nil, nil, nil, nil, fmt.Errorf("source media probe failure was not repeatable")
		}
		quarantinedSources = append(quarantinedSources, source)
		quarantines = append(quarantines, QuarantinedBuild{Source: local, Evidence: maximalityEvidence([]LocalSource{local}, first, 2, mediaToolIdentity)})
	}
	return includedSources, includedLocals, quarantinedSources, quarantines, nil
}
