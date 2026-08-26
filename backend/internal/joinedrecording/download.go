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
	"sync"
)

type sourceCapability func(context.Context, SourceClip, string) (SourceReadCapability, error)

const joinedSourceDownloadConcurrency = 8

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
	locals := make([]LocalSource, len(claim.Sources))
	errs := make([]error, len(claim.Sources))
	semaphore := make(chan struct{}, joinedSourceDownloadConcurrency)
	var workers sync.WaitGroup
	for i, source := range claim.Sources {
		workers.Add(1)
		go func(ordinal int, source SourceClip) {
			defer workers.Done()
			select {
			case semaphore <- struct{}{}:
				defer func() { <-semaphore }()
			case <-ctx.Done():
				errs[ordinal] = ctx.Err()
				return
			}
			locals[ordinal], errs[ordinal] = downloadClaimSource(ctx, source, scratchDir, client, storageAuthority, sourceCapability)
		}(i, source)
	}
	workers.Wait()
	for i, err := range errs {
		if err != nil {
			return nil, "", fmt.Errorf("download source ordinal=%d clip_id=%d: %w", i+1, claim.Sources[i].ClipID, err)
		}
	}
	return locals, scratchDir, nil
}

func downloadClaimSource(ctx context.Context, source SourceClip, scratchDir string, client CapabilityHTTPClient, storageAuthority string, sourceCapability sourceCapability) (LocalSource, error) {
	finalPath := filepath.Join(scratchDir, "clip-"+strconv.FormatInt(source.ClipID, 10)+".mp4")
	sourceClaim, _, claimErr := sourceClaimSHA([]SourceClip{source})
	if claimErr != nil {
		return LocalSource{}, claimErr
	}
	local := LocalSource{ClipID: source.ClipID, Path: finalPath, SizeBytes: source.Object.SizeBytes, SHA256: source.Object.SHA256, SourceClaimSHA256: sourceClaim}
	if err := verifyLocalIdentity(local); err == nil {
		return local, nil
	}
	if err := os.Remove(finalPath); err != nil && !os.IsNotExist(err) {
		return LocalSource{}, fmt.Errorf("remove unverified source scratch: %w", err)
	}
	headCapability, err := sourceCapability(ctx, source, "head")
	if err != nil {
		return LocalSource{}, fmt.Errorf("resolve exact HEAD capability: %w", err)
	}
	if err := verifyExactSourceHeadCapability(ctx, client, storageAuthority, source, headCapability); err != nil {
		return LocalSource{}, fmt.Errorf("exact HEAD verification failed: %w", err)
	}
	capability, err := sourceCapability(ctx, source, "get")
	if err != nil {
		return LocalSource{}, fmt.Errorf("resolve exact GET capability: %w", err)
	}
	rc, err := openExactCapability(ctx, client, storageAuthority, source, capability)
	if err != nil {
		return LocalSource{}, fmt.Errorf("open exact GET capability: %w", err)
	}
	partPath := finalPath + ".part"
	_ = os.Remove(partPath)
	part, err := os.OpenFile(partPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		_ = rc.Close()
		return LocalSource{}, fmt.Errorf("create source scratch: %w", err)
	}
	hash := sha256.New()
	n, copyErr := io.Copy(io.MultiWriter(part, hash), io.LimitReader(rc, source.Object.SizeBytes+1))
	syncErr := part.Sync()
	closePartErr := part.Close()
	closeSourceErr := rc.Close()
	if copyErr != nil || syncErr != nil || closePartErr != nil || closeSourceErr != nil || n != source.Object.SizeBytes || hex.EncodeToString(hash.Sum(nil)) != source.Object.SHA256 {
		_ = os.Remove(partPath)
		if contextErr := ctx.Err(); contextErr != nil {
			return LocalSource{}, contextErr
		}
		return LocalSource{}, fmt.Errorf("exact download verification failed")
	}
	if err := ctx.Err(); err != nil {
		_ = os.Remove(partPath)
		return LocalSource{}, err
	}
	if err := os.Link(partPath, finalPath); err != nil {
		_ = os.Remove(partPath)
		return LocalSource{}, fmt.Errorf("publish source scratch without overwrite: %w", err)
	}
	_ = os.Remove(partPath)
	return local, nil
}
