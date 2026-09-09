package joinedrecording

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const scratchReapPrefix = ".reap-"
const scratchLeaseProofLimit = 256
const failedEvidenceDirectory = ".failed-evidence"

type FailedPreflightDiagnostic struct {
	ReasonCode     string `json:"reason_code"`
	FailureCode    string `json:"failure_code,omitempty"`
	EvidenceSHA256 string `json:"evidence_sha256,omitempty"`
	SealClass      string `json:"seal_class,omitempty"`
	MediaOrdinal   int    `json:"media_ordinal,omitempty"`
	ProofOrdinal   int    `json:"proof_ordinal,omitempty"`
}

func PreserveFailedPreflightEvidence(root string, claim PreflightHourClaim, diagnostic FailedPreflightDiagnostic) error {
	if err := validatePrivateScratchRoot(root); err != nil {
		return err
	}
	if !validLeaseID(claim.LeaseID) || claim.HourID == "" || !lowerHex64(claim.SourceClaimSHA256) || len(claim.Sources) == 0 || !safeReasonCode(diagnostic.ReasonCode) {
		return fmt.Errorf("invalid joined failed preflight evidence")
	}
	mediaDiagnostic := safeReasonCode(diagnostic.FailureCode) && (diagnostic.EvidenceSHA256 == "" || lowerHex64(diagnostic.EvidenceSHA256)) && diagnostic.SealClass == "" && diagnostic.MediaOrdinal == 0 && diagnostic.ProofOrdinal == 0
	sealClass := SealValidationClass(diagnostic.SealClass)
	sealDiagnostic := diagnostic.FailureCode == "" && diagnostic.EvidenceSHA256 == "" && sealClass.Valid() && diagnostic.MediaOrdinal >= 0 && diagnostic.ProofOrdinal >= 0
	if !mediaDiagnostic && !sealDiagnostic {
		return fmt.Errorf("invalid joined failed preflight diagnostic")
	}
	type source struct {
		ClipID int64          `json:"clip_id"`
		Object ObjectIdentity `json:"object"`
	}
	record := struct {
		Schema            int                       `json:"schema"`
		BatchID           string                    `json:"batch_id"`
		HourID            string                    `json:"hour_id"`
		LeaseID           string                    `json:"lease_id"`
		RecordingID       int64                     `json:"recording_id"`
		LocalDate         string                    `json:"local_date"`
		LocalHour         int                       `json:"local_hour"`
		SourceClaimSHA256 string                    `json:"source_claim_sha256"`
		Sources           []source                  `json:"sources"`
		MediaToolIdentity string                    `json:"media_tool_identity_sha256"`
		Diagnostic        FailedPreflightDiagnostic `json:"diagnostic"`
	}{1, claim.BatchID, claim.HourID, claim.LeaseID, claim.RecordingID, claim.LocalDate, claim.LocalHour, claim.SourceClaimSHA256, make([]source, len(claim.Sources)), claim.MediaTool.IdentitySHA256, diagnostic}
	for i, item := range claim.Sources {
		record.Sources[i] = source{item.ClipID, item.Object}
	}
	payload, err := json.Marshal(record)
	if err != nil || len(payload) > 1<<20 {
		return fmt.Errorf("encode joined failed preflight evidence")
	}
	payload = append(payload, '\n')
	rootHandle, err := os.OpenRoot(root)
	if err != nil {
		return fmt.Errorf("open joined scratch root: %w", err)
	}
	defer rootHandle.Close()
	createdDirectory := false
	if err := rootHandle.Mkdir(failedEvidenceDirectory, 0o700); err == nil {
		createdDirectory = true
	} else if !os.IsExist(err) {
		return fmt.Errorf("create joined failed evidence directory: %w", err)
	}
	if createdDirectory {
		rootDir, err := rootHandle.Open(".")
		if err != nil {
			return fmt.Errorf("open joined scratch root directory: %w", err)
		}
		err = rootDir.Sync()
		closeErr := rootDir.Close()
		if err != nil {
			return fmt.Errorf("sync joined scratch root: %w", err)
		}
		if closeErr != nil {
			return fmt.Errorf("close joined scratch root: %w", closeErr)
		}
	}
	if info, err := rootHandle.Lstat(failedEvidenceDirectory); err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() || info.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("joined failed evidence directory is not private")
	}
	name := failedEvidenceDirectory + "/" + claim.LeaseID + ".json"
	file, err := rootHandle.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if os.IsExist(err) {
		info, statErr := rootHandle.Lstat(name)
		if statErr != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
			return fmt.Errorf("joined failed evidence file is invalid")
		}
		existing, openErr := rootHandle.Open(name)
		if openErr != nil {
			return fmt.Errorf("open joined failed evidence: %w", openErr)
		}
		got, readErr := io.ReadAll(io.LimitReader(existing, 1<<20+1))
		closeErr := existing.Close()
		if readErr != nil || closeErr != nil || !bytes.Equal(got, payload) {
			return fmt.Errorf("joined failed evidence differs")
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("create joined failed evidence: %w", err)
	}
	var written int
	if written, err = file.Write(payload); err == nil && written != len(payload) {
		err = io.ErrShortWrite
	}
	if err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err != nil {
		return fmt.Errorf("write joined failed evidence: %w", err)
	}
	if closeErr != nil {
		return fmt.Errorf("close joined failed evidence: %w", closeErr)
	}
	leaseDir, err := rootHandle.Open(failedEvidenceDirectory)
	if err != nil {
		return fmt.Errorf("open joined failed evidence directory: %w", err)
	}
	err = leaseDir.Sync()
	closeErr = leaseDir.Close()
	if err != nil {
		return fmt.Errorf("sync joined failed evidence directory: %w", err)
	}
	if closeErr != nil {
		return fmt.Errorf("close joined failed evidence directory: %w", closeErr)
	}
	return nil
}

// ScratchLeaseProof marks leases that the API proved cannot own live work.
// Missing and false entries are retained. Cleanup must fail closed when proof
// is unavailable or incomplete.
type ScratchLeaseProof func(context.Context, []string) (map[string]bool, error)

// CleanupInactiveLeaseScratch removes only direct lease directories whose
// exact lease IDs the API proves inactive or expired. It never follows a
// symlink and leaves unrelated root entries untouched.
func CleanupInactiveLeaseScratch(ctx context.Context, root string, prove ScratchLeaseProof) ([]string, error) {
	if prove == nil {
		return nil, fmt.Errorf("joined scratch lease proof is required")
	}
	if err := validatePrivateScratchRoot(root); err != nil {
		return nil, err
	}
	pathInfo, err := os.Lstat(root)
	if err != nil {
		return nil, fmt.Errorf("stat joined scratch root: %w", err)
	}
	rootHandle, err := os.OpenRoot(root)
	if err != nil {
		return nil, fmt.Errorf("open joined scratch root: %w", err)
	}
	defer rootHandle.Close()
	openedInfo, err := rootHandle.Stat(".")
	if err != nil || !os.SameFile(pathInfo, openedInfo) {
		return nil, fmt.Errorf("joined scratch root changed while opening")
	}
	rootDir, err := rootHandle.Open(".")
	if err != nil {
		return nil, fmt.Errorf("open joined scratch directory: %w", err)
	}
	entries, err := rootDir.ReadDir(-1)
	closeErr := rootDir.Close()
	if err != nil {
		return nil, fmt.Errorf("read joined scratch root: %w", err)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("close joined scratch root: %w", closeErr)
	}
	leaseSet := map[string]bool{}
	for _, entry := range entries {
		leaseID := entry.Name()
		if strings.HasPrefix(leaseID, scratchReapPrefix) {
			leaseID = strings.TrimPrefix(leaseID, scratchReapPrefix)
		}
		if validLeaseID(leaseID) {
			leaseSet[leaseID] = true
		}
	}
	leaseIDs := make([]string, 0, len(leaseSet))
	for leaseID := range leaseSet {
		leaseIDs = append(leaseIDs, leaseID)
	}
	sort.Strings(leaseIDs)
	if len(leaseIDs) == 0 {
		return nil, nil
	}
	proof := make(map[string]bool, len(leaseIDs))
	for start := 0; start < len(leaseIDs); start += scratchLeaseProofLimit {
		end := min(start+scratchLeaseProofLimit, len(leaseIDs))
		page, err := prove(ctx, leaseIDs[start:end])
		if err != nil {
			return nil, fmt.Errorf("prove joined scratch leases inactive: %w", err)
		}
		for leaseID, inactive := range page {
			if !leaseSet[leaseID] {
				return nil, fmt.Errorf("joined scratch lease proof included an unrequested lease")
			}
			if _, duplicate := proof[leaseID]; duplicate {
				return nil, fmt.Errorf("joined scratch lease proof repeated a lease")
			}
			proof[leaseID] = inactive
		}
	}
	for _, leaseID := range leaseIDs {
		if _, ok := proof[leaseID]; !ok {
			return nil, fmt.Errorf("joined scratch lease proof omitted lease %s", leaseID)
		}
	}

	removed := []string{}
	for _, leaseID := range leaseIDs {
		if !proof[leaseID] {
			continue
		}
		quarantine := scratchReapPrefix + leaseID
		original := leaseID
		if err := removeProvenScratchPath(rootHandle, quarantine); err != nil {
			return removed, err
		}
		if info, err := rootHandle.Lstat(original); err == nil {
			if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
				return removed, fmt.Errorf("joined lease scratch is not a directory: %s", leaseID)
			}
			if err := rootHandle.Rename(original, quarantine); err != nil {
				return removed, fmt.Errorf("fence joined lease scratch %s: %w", leaseID, err)
			}
			if err := removeProvenScratchPath(rootHandle, quarantine); err != nil {
				return removed, err
			}
		} else if !os.IsNotExist(err) {
			return removed, fmt.Errorf("stat joined lease scratch %s: %w", leaseID, err)
		}
		removed = append(removed, leaseID)
	}
	return removed, nil
}

func validatePrivateScratchRoot(root string) error {
	if !filepath.IsAbs(root) || filepath.Clean(root) != root || root == string(filepath.Separator) {
		return fmt.Errorf("joined scratch root must be a clean absolute non-root path")
	}
	info, err := os.Lstat(root)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() || info.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("joined scratch root must be a private directory")
	}
	return nil
}

func removeProvenScratchPath(root *os.Root, name string) error {
	if root == nil || filepath.Base(name) != name || filepath.Clean(name) != name {
		return fmt.Errorf("refusing joined scratch cleanup outside root")
	}
	info, err := root.Lstat(name)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("stat joined scratch cleanup path: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("joined scratch cleanup path is not a directory")
	}
	if err := root.RemoveAll(name); err != nil {
		return fmt.Errorf("remove inactive joined scratch: %w", err)
	}
	return nil
}
