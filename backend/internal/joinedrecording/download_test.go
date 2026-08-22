package joinedrecording

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

type downloadFenceClient struct {
	source   SourceClip
	body     []byte
	failHead bool
}

func (*downloadFenceClient) joinedRedirectSafe() {}

func (c *downloadFenceClient) Do(request *http.Request) (*http.Response, error) {
	if request.Method == http.MethodHead {
		if c.failHead {
			return capabilityResponse(http.StatusPreconditionFailed, nil, "", ""), nil
		}
		return capabilityResponse(http.StatusOK, make([]byte, c.source.Object.SizeBytes),
			c.source.Object.ETag, c.source.Object.VersionID), nil
	}
	response := capabilityResponse(http.StatusOK, c.body, c.source.Object.ETag, c.source.Object.VersionID)
	response.ContentLength = c.source.Object.SizeBytes
	response.Header.Set("Content-Length", strconv.FormatInt(c.source.Object.SizeBytes, 10))
	return response, nil
}

func downloadClaimFixture(t *testing.T) (PreflightHourClaim, SourceClip, []byte) {
	t.Helper()
	body := []byte("exact raw clip bytes")
	sum := sha256.Sum256(body)
	source := testSource(1, time.Date(2026, time.May, 4, 8, 0, 0, 0, time.UTC))
	source.Object.SizeBytes = int64(len(body))
	source.Object.SHA256 = hex.EncodeToString(sum[:])
	source.Object.ETag = objectETag(body)
	source.Object.VersionID = "version"
	plan, err := buildTestPlan(testRequest([]SourceClip{source}))
	if err != nil {
		t.Fatal(err)
	}
	claim := PreflightHourClaim{ProtocolVersion: JoinedProtocolVersion, HourID: plan.HourID,
		LeaseID: strings.Repeat("L", 43), OperationToken: strings.Repeat("t", 32),
		LeaseExpires: time.Now().Add(time.Hour), BatchID: plan.BatchID, Generation: plan.Generation,
		RecordingID: plan.RecordingID, Timezone: plan.Timezone, LocalDate: plan.LocalDate,
		LocalHour: plan.LocalHour, AllocationLedgerSHA: plan.AllocationLedgerSHA,
		Qualification: plan.Qualification, MediaTool: plan.MediaTool,
		SourceClaimSHA256: plan.SourceClaimSHA256, Sources: sourceOnlyClips(plan.Outputs[0].Sources)}
	return claim, source, body
}

func downloadCapability(source SourceClip) sourceCapability {
	return func(_ context.Context, _ SourceClip, operation string) (SourceReadCapability, error) {
		capability := sourceReadCapability(source.Object.Key, source.Object.ETag, source.Object.VersionID, operation)
		capability.SizeBytes, capability.SHA256 = source.Object.SizeBytes, source.Object.SHA256
		return capability, nil
	}
}

func TestDownloadClaimSourcesPinsHeadAndHashesBeforePublication(t *testing.T) {
	body := "exact raw clip bytes"
	sum := sha256.Sum256([]byte(body))
	source := testSource(1, time.Date(2026, time.May, 4, 8, 0, 0, 0, time.UTC))
	source.Object.SizeBytes = int64(len(body))
	source.Object.SHA256 = hex.EncodeToString(sum[:])
	source.Object.ETag = "etag-" + hex.EncodeToString(sum[:4])
	source.Object.VersionID = "version"
	plan, err := buildTestPlan(testRequest([]SourceClip{source}))
	if err != nil {
		t.Fatal(err)
	}
	claim := PreflightHourClaim{ProtocolVersion: JoinedProtocolVersion, HourID: plan.HourID, LeaseID: strings.Repeat("L", 43), OperationToken: strings.Repeat("t", 32), LeaseExpires: time.Now().Add(time.Hour), BatchID: plan.BatchID, Generation: plan.Generation, RecordingID: plan.RecordingID, Timezone: plan.Timezone, LocalDate: plan.LocalDate, LocalHour: plan.LocalHour, AllocationLedgerSHA: plan.AllocationLedgerSHA, Qualification: plan.Qualification, MediaTool: plan.MediaTool, SourceClaimSHA256: plan.SourceClaimSHA256, Sources: sourceOnlyClips(plan.Outputs[0].Sources)}
	client := &memoryCapabilityClient{objects: map[string][]byte{source.Object.Key: []byte(body)}}
	locals, scratch, err := downloadClaimSources(context.Background(), claim, t.TempDir(), client, testSourceAuthority, func(_ context.Context, _ SourceClip, operation string) (SourceReadCapability, error) {
		capability := sourceReadCapability(source.Object.Key, source.Object.ETag, source.Object.VersionID, operation)
		capability.SizeBytes, capability.SHA256 = source.Object.SizeBytes, source.Object.SHA256
		return capability, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(locals) != 1 || !SafeScratchOutput(locals[0].Path, scratch) {
		t.Fatalf("bad local source: %+v", locals)
	}
	if err := verifyLocalIdentity(locals[0]); err != nil {
		t.Fatal(err)
	}
}

func TestDownloadClaimSourcesRejectsUnverifiedBytesWithoutPublication(t *testing.T) {
	for _, test := range []struct {
		name     string
		body     func([]byte) []byte
		failHead bool
	}{
		{name: "sha mismatch", body: func(body []byte) []byte {
			changed := append([]byte(nil), body...)
			changed[0] ^= 0xff
			return changed
		}},
		{name: "size mismatch", body: func(body []byte) []byte { return append([]byte(nil), body[:len(body)-1]...) }},
		{name: "oversize body", body: func(body []byte) []byte { return append(append([]byte(nil), body...), 'x') }},
		{name: "HEAD verification", body: func(body []byte) []byte { return append([]byte(nil), body...) }, failHead: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			claim, source, body := downloadClaimFixture(t)
			root := t.TempDir()
			client := &downloadFenceClient{source: source, body: test.body(body), failHead: test.failHead}
			_, scratch, err := downloadClaimSources(context.Background(), claim, root, client,
				testSourceAuthority, downloadCapability(source))
			if err == nil {
				t.Fatal("unverified source bytes were accepted")
			}
			if scratch != "" {
				t.Fatalf("failed download returned published scratch %q", scratch)
			}
			directory, dirErr := claim.ScratchDir(root)
			if dirErr != nil {
				t.Fatal(dirErr)
			}
			finalPath := filepath.Join(directory, "clip-1.mp4")
			if _, statErr := os.Stat(finalPath); !os.IsNotExist(statErr) {
				t.Fatalf("failed download left final source: %v", statErr)
			}
			if _, statErr := os.Stat(finalPath + ".part"); !os.IsNotExist(statErr) {
				t.Fatalf("failed download left partial source: %v", statErr)
			}
		})
	}
}

func TestDownloadClaimSourcesReplacesOnlyUnverifiedCurrentLeaseScratch(t *testing.T) {
	claim, source, body := downloadClaimFixture(t)
	root := t.TempDir()
	directory, err := claim.ScratchDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(directory, 0700); err != nil {
		t.Fatal(err)
	}
	finalPath := filepath.Join(directory, "clip-1.mp4")
	if err := os.WriteFile(finalPath, []byte("truncated"), 0600); err != nil {
		t.Fatal(err)
	}
	locals, scratch, err := downloadClaimSources(context.Background(), claim, root,
		&downloadFenceClient{source: source, body: body}, testSourceAuthority, downloadCapability(source))
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(finalPath)
	if err != nil || !bytes.Equal(got, body) || scratch != directory || len(locals) != 1 {
		t.Fatalf("stale scratch recovery differs: bytes=%q scratch=%q locals=%d err=%v", got, scratch, len(locals), err)
	}
	if _, err := os.Stat(finalPath + ".part"); !os.IsNotExist(err) {
		t.Fatalf("stale scratch recovery left partial source: %v", err)
	}
}
