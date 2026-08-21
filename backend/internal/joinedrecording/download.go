package joinedrecording

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
)

type sourceCapability func(context.Context, SourceClip, string) (SourceReadCapability, error)

// downloadClaimSources reads only exact R2 generations into the claim's unique
// scratch directory. Existing scratch files are reused only after full hashing.
func downloadClaimSources(ctx context.Context, claim PreflightHourClaim, scratchRoot string, client CapabilityHTTPClient, storageAuthority string, sourceCapability sourceCapability) ([]LocalSource, string, error) {
	if client == nil || sourceCapability == nil || storageAuthority == "" {
		return nil, "", fmt.Errorf("source capability client and resolver are required")
	}
	scratchDir, err := claim.ScratchDir(scratchRoot)
	if err != nil {
		return nil, "", err
	}
	if err := os.MkdirAll(scratchDir, 0700); err != nil {
		return nil, "", err
	}
	locals := make([]LocalSource, 0)
	for _, source := range claim.Sources {
		finalPath := filepath.Join(scratchDir, "clip-"+strconv.FormatInt(source.ClipID, 10)+".mp4")
		sourceClaim, _, claimErr := sourceClaimSHA([]SourceClip{source})
		if claimErr != nil {
			return nil, "", claimErr
		}
		local := LocalSource{ClipID: source.ClipID, Path: finalPath, SizeBytes: source.Object.SizeBytes, SHA256: source.Object.SHA256, SourceClaimSHA256: sourceClaim}
		if err := verifyLocalIdentity(local); err == nil {
			locals = append(locals, local)
			continue
		}
		headCapability, err := sourceCapability(ctx, source, "head")
		if err != nil || verifyExactSourceHeadCapability(ctx, client, storageAuthority, source, headCapability) != nil {
			return nil, "", fmt.Errorf("source clip %d exact HEAD verification failed", source.ClipID)
		}
		capability, err := sourceCapability(ctx, source, "get")
		if err != nil {
			return nil, "", err
		}
		rc, err := openExactCapability(ctx, client, storageAuthority, source, capability)
		if err != nil {
			return nil, "", err
		}
		_ = os.Remove(finalPath + ".part")
		part, err := os.OpenFile(finalPath+".part", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			_ = rc.Close()
			return nil, "", fmt.Errorf("create source scratch: %w", err)
		}
		hash := sha256.New()
		n, copyErr := io.Copy(io.MultiWriter(part, hash), io.LimitReader(rc, source.Object.SizeBytes+1))
		syncErr := part.Sync()
		closePartErr := part.Close()
		closeSourceErr := rc.Close()
		if copyErr != nil || syncErr != nil || closePartErr != nil || closeSourceErr != nil || n != source.Object.SizeBytes || hex.EncodeToString(hash.Sum(nil)) != source.Object.SHA256 {
			_ = os.Remove(finalPath + ".part")
			return nil, "", fmt.Errorf("source clip %d exact download verification failed", source.ClipID)
		}
		if err := os.Link(finalPath+".part", finalPath); err != nil {
			_ = os.Remove(finalPath + ".part")
			return nil, "", fmt.Errorf("publish source scratch without overwrite: %w", err)
		}
		_ = os.Remove(finalPath + ".part")
		locals = append(locals, local)
	}
	return locals, scratchDir, nil
}
