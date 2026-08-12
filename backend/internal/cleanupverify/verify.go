package cleanupverify

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/daydemir/stoarama/backend/internal/r2"
)

const DefaultMaxObjectBytes int64 = 2 << 30

type ObjectStore interface {
	Head(context.Context, string) (r2.ObjectHead, error)
	OpenIfMatch(context.Context, string, string, string) (r2.ObjectReader, error)
}

type Result struct {
	ETag      string
	VersionID string
	SizeBytes int64
	SHA256    string
}

type VerificationError struct {
	Code string
	Err  error
}

func (e *VerificationError) Error() string { return e.Code + ": " + e.Err.Error() }
func (e *VerificationError) Unwrap() error { return e.Err }

func fail(code, format string, args ...any) error {
	return &VerificationError{Code: code, Err: fmt.Errorf(format, args...)}
}

func ErrorCode(err error) string {
	var target *VerificationError
	if errors.As(err, &target) {
		return target.Code
	}
	return "transport_error"
}

// Verify hashes a conditional, bounded streaming GET and requires stable object
// identity before, during, and after the read. It never buffers media bytes.
func Verify(ctx context.Context, store ObjectStore, key string, expectedSize int64, expectedSHA string, maxObjectBytes int64) (Result, error) {
	if strings.TrimSpace(key) == "" || expectedSize <= 0 || len(expectedSHA) != 64 {
		return Result{}, fail("invalid_expected_identity", "missing key, positive size, or sha256")
	}
	if maxObjectBytes <= 0 {
		maxObjectBytes = DefaultMaxObjectBytes
	}
	if expectedSize > maxObjectBytes {
		return Result{}, fail("object_too_large", "expected %d bytes exceeds %d-byte cap", expectedSize, maxObjectBytes)
	}
	before, err := store.Head(ctx, key)
	if err != nil {
		return Result{}, fmt.Errorf("head before: %w", err)
	}
	if before.SizeBytes != expectedSize || strings.TrimSpace(before.ETag) == "" {
		return Result{}, fail("head_identity_mismatch", "head size=%d expected=%d or etag empty", before.SizeBytes, expectedSize)
	}
	opened, err := store.OpenIfMatch(ctx, key, before.ETag, before.VersionID)
	if err != nil {
		return Result{}, fmt.Errorf("conditional get: %w", err)
	}
	defer opened.Body.Close()
	if opened.SizeBytes != expectedSize || opened.ETag != before.ETag || opened.VersionID != before.VersionID {
		return Result{}, fail("get_identity_mismatch", "conditional response identity changed")
	}
	h := sha256.New()
	n, err := io.Copy(h, io.LimitReader(opened.Body, expectedSize+1))
	if err != nil {
		return Result{}, fmt.Errorf("stream object: %w", err)
	}
	if n != expectedSize {
		return Result{}, fail("body_size_mismatch", "read %d bytes expected %d", n, expectedSize)
	}
	actualSHA := hex.EncodeToString(h.Sum(nil))
	if actualSHA != strings.ToLower(expectedSHA) {
		return Result{}, fail("content_sha_mismatch", "content sha256 differs")
	}
	after, err := store.Head(ctx, key)
	if err != nil {
		return Result{}, fmt.Errorf("head after: %w", err)
	}
	if after != before {
		return Result{}, fail("object_changed", "object identity changed during verification")
	}
	return Result{ETag: before.ETag, VersionID: before.VersionID, SizeBytes: n, SHA256: actualSHA}, nil
}
