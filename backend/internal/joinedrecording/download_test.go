package joinedrecording

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type downloadFenceClient struct {
	source   SourceClip
	body     []byte
	failHead bool
}

type concurrentDownloadClient struct {
	sources   map[string]SourceClip
	bodies    map[string][]byte
	gate      chan struct{}
	releaseAt int

	mu        sync.Mutex
	active    int
	maxActive int
	started   int
}

type cancellationBody struct {
	ctx context.Context
}

func (b cancellationBody) Read([]byte) (int, error) {
	<-b.ctx.Done()
	return 0, b.ctx.Err()
}

func (cancellationBody) Close() error { return nil }

type cancellationDownloadClient struct {
	source  SourceClip
	started chan struct{}
}

func (*cancellationDownloadClient) joinedRedirectSafe() {}

func (c *cancellationDownloadClient) Do(request *http.Request) (*http.Response, error) {
	if request.Method == http.MethodHead {
		return capabilityResponse(http.StatusOK, make([]byte, c.source.Object.SizeBytes),
			c.source.Object.ETag, c.source.Object.VersionID), nil
	}
	close(c.started)
	response := capabilityResponse(http.StatusOK, nil, c.source.Object.ETag, c.source.Object.VersionID)
	response.Body = io.NopCloser(cancellationBody{ctx: request.Context()})
	response.ContentLength = c.source.Object.SizeBytes
	response.Header.Set("Content-Length", strconv.FormatInt(c.source.Object.SizeBytes, 10))
	return response, nil
}

func (*concurrentDownloadClient) joinedRedirectSafe() {}

func (c *concurrentDownloadClient) Do(request *http.Request) (*http.Response, error) {
	var source SourceClip
	var body []byte
	for key, candidate := range c.sources {
		if strings.HasSuffix(request.URL.Path, "/"+key) {
			source, body = candidate, c.bodies[key]
			break
		}
	}
	if source.ClipID == 0 {
		return capabilityResponse(http.StatusNotFound, nil, "", ""), nil
	}
	if request.Method == http.MethodHead {
		return capabilityResponse(http.StatusOK, make([]byte, source.Object.SizeBytes),
			source.Object.ETag, source.Object.VersionID), nil
	}

	c.mu.Lock()
	c.active++
	c.started++
	if c.active > c.maxActive {
		c.maxActive = c.active
	}
	if c.started == c.releaseAt {
		close(c.gate)
	}
	c.mu.Unlock()
	select {
	case <-c.gate:
	case <-request.Context().Done():
		c.mu.Lock()
		c.active--
		c.mu.Unlock()
		return nil, request.Context().Err()
	}
	c.mu.Lock()
	c.active--
	c.mu.Unlock()
	response := capabilityResponse(http.StatusOK, body, source.Object.ETag, source.Object.VersionID)
	response.ContentLength = source.Object.SizeBytes
	response.Header.Set("Content-Length", strconv.FormatInt(source.Object.SizeBytes, 10))
	return response, nil
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
	return downloadClaimForSources(t, []SourceClip{source}), source, body
}

func downloadClaimForSources(t *testing.T, sources []SourceClip) PreflightHourClaim {
	t.Helper()
	plan, err := buildTestPlan(testRequest(sources))
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
	return claim
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

func TestDownloadClaimSourcesDownloadsConcurrentlyWithinBoundAndPreservesOrdinal(t *testing.T) {
	const expectedConcurrency = 8
	_, template, _ := downloadClaimFixture(t)
	sourceCount := expectedConcurrency + 2
	sources := make([]SourceClip, sourceCount)
	client := &concurrentDownloadClient{
		sources:   make(map[string]SourceClip, sourceCount),
		bodies:    make(map[string][]byte, sourceCount),
		gate:      make(chan struct{}),
		releaseAt: expectedConcurrency,
	}
	for i := range sources {
		body := []byte(fmt.Sprintf("exact raw clip bytes %02d", i))
		sum := sha256.Sum256(body)
		source := template
		source.ClipID = int64(100 + i)
		source.StartUTC = template.StartUTC.Add(time.Duration(i) * time.Minute)
		source.EndUTC = template.EndUTC.Add(time.Duration(i) * time.Minute)
		source.Object.Key = fmt.Sprintf("raw/clip-%02d.mp4", i)
		source.Object.SizeBytes = int64(len(body))
		source.Object.SHA256 = hex.EncodeToString(sum[:])
		source.Object.ETag = objectETag(body)
		source.Object.VersionID = fmt.Sprintf("version-%02d", i)
		sources[i] = source
		client.sources[source.Object.Key] = source
		client.bodies[source.Object.Key] = body
	}
	claim := downloadClaimForSources(t, sources)
	resolver := func(_ context.Context, source SourceClip, operation string) (SourceReadCapability, error) {
		capability := sourceReadCapability(source.Object.Key, source.Object.ETag, source.Object.VersionID, operation)
		capability.SizeBytes, capability.SHA256 = source.Object.SizeBytes, source.Object.SHA256
		return capability, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	locals, _, err := downloadClaimSources(ctx, claim, t.TempDir(), client, testSourceAuthority, resolver)
	if err != nil {
		t.Fatal(err)
	}
	client.mu.Lock()
	maxActive := client.maxActive
	client.mu.Unlock()
	if maxActive != expectedConcurrency {
		t.Fatalf("maximum concurrent downloads=%d want=%d", maxActive, expectedConcurrency)
	}
	if len(locals) != len(claim.Sources) {
		t.Fatalf("local sources=%d want=%d", len(locals), len(claim.Sources))
	}
	for i := range locals {
		if locals[i].ClipID != claim.Sources[i].ClipID {
			t.Fatalf("local ordinal %d clip_id=%d want=%d", i+1, locals[i].ClipID, claim.Sources[i].ClipID)
		}
	}
}

func TestDownloadClaimSourcesReturnsLowestOrdinalAttributedErrorDeterministically(t *testing.T) {
	_, template, _ := downloadClaimFixture(t)
	sources := make([]SourceClip, 3)
	for i := range sources {
		source := template
		source.ClipID = int64(701 + i)
		source.StartUTC = template.StartUTC.Add(time.Duration(i) * time.Minute)
		source.EndUTC = template.EndUTC.Add(time.Duration(i) * time.Minute)
		source.Object.Key = fmt.Sprintf("raw/error-%d.mp4", i+1)
		sources[i] = source
	}
	claim := downloadClaimForSources(t, sources)
	release := make(chan struct{})
	var mu sync.Mutex
	started := 0
	resolver := func(_ context.Context, source SourceClip, _ string) (SourceReadCapability, error) {
		mu.Lock()
		started++
		if started == len(claim.Sources) {
			close(release)
		}
		mu.Unlock()
		<-release
		if source.ClipID == claim.Sources[0].ClipID {
			time.Sleep(25 * time.Millisecond)
		}
		return SourceReadCapability{}, fmt.Errorf("capability failure for %d", source.ClipID)
	}
	_, scratch, err := downloadClaimSources(context.Background(), claim, t.TempDir(),
		&downloadFenceClient{}, testSourceAuthority, resolver)
	if err == nil {
		t.Fatal("concurrent source failures were accepted")
	}
	if scratch != "" {
		t.Fatalf("failed download returned published scratch %q", scratch)
	}
	want := "download source ordinal=1 clip_id=701: resolve exact HEAD capability: capability failure for 701"
	if err.Error() != want {
		t.Fatalf("error=%q want=%q", err, want)
	}
}

func TestDownloadClaimSourcesPreservesCancellationAndRemovesPartialScratch(t *testing.T) {
	claim, source, _ := downloadClaimFixture(t)
	root := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	client := &cancellationDownloadClient{source: source, started: make(chan struct{})}
	done := make(chan error, 1)
	go func() {
		_, _, err := downloadClaimSources(ctx, claim, root, client, testSourceAuthority, downloadCapability(source))
		done <- err
	}()
	<-client.started
	cancel()
	err := <-done
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("download cancellation error=%v", err)
	}
	directory, dirErr := claim.ScratchDir(root)
	if dirErr != nil {
		t.Fatal(dirErr)
	}
	finalPath := filepath.Join(directory, "clip-1.mp4")
	if _, statErr := os.Stat(finalPath); !os.IsNotExist(statErr) {
		t.Fatalf("canceled download left final source: %v", statErr)
	}
	if _, statErr := os.Stat(finalPath + ".part"); !os.IsNotExist(statErr) {
		t.Fatalf("canceled download left partial source: %v", statErr)
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
