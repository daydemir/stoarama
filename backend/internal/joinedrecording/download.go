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

	"github.com/daydemir/stoarama/backend/internal/r2"
)

type ExactSourceStore interface {
	Head(context.Context, string) (r2.ObjectHead, error)
	OpenExact(context.Context, string, string, string) (io.ReadCloser, error)
}

type SourceStore func(context.Context, SourceClip) (ExactSourceStore, error)

// DownloadClaimSources reads only exact R2 generations into the claim's unique
// scratch directory. Existing scratch files are reused only after full hashing.
func DownloadClaimSources(ctx context.Context, claim WorkerClaim, scratchRoot string, sourceStore SourceStore) ([]LocalSource, string, error) {
	if sourceStore == nil {
		return nil, "", fmt.Errorf("source store resolver is required")
	}
	scratchDir, err := claim.ScratchDir(scratchRoot)
	if err != nil {
		return nil, "", err
	}
	if err := os.MkdirAll(scratchDir, 0700); err != nil {
		return nil, "", err
	}
	locals := make([]LocalSource, 0, len(claim.Output.Sources))
	for _, source := range claim.Output.Sources {
		finalPath := filepath.Join(scratchDir, "clip-"+strconv.FormatInt(source.ClipID, 10)+".mp4")
		local := LocalSource{ClipID: source.ClipID, Path: finalPath, SizeBytes: source.Object.SizeBytes, SHA256: source.Object.SHA256}
		if err := verifyLocalIdentity(local); err == nil {
			locals = append(locals, local)
			continue
		}
		store, err := sourceStore(ctx, source)
		if err != nil {
			return nil, "", err
		}
		head, err := store.Head(ctx, source.Object.Key)
		if err != nil || head.SizeBytes != source.Object.SizeBytes || head.ETag != source.Object.ETag || (source.Object.VersionID != "" && head.VersionID != source.Object.VersionID) {
			return nil, "", fmt.Errorf("source clip %d R2 identity drifted", source.ClipID)
		}
		rc, err := store.OpenExact(ctx, source.Object.Key, head.ETag, head.VersionID)
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
