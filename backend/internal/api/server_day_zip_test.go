package api

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

// fakeOpener returns the given bytes for any key.
func fakeOpener(payload []byte) func(context.Context, string) (io.ReadCloser, error) {
	return func(context.Context, string) (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(payload)), nil
	}
}

// readZipEntry returns the contents of the named entry in the zip archive.
func readZipEntry(t *testing.T, archive []byte, name string) ([]byte, bool) {
	t.Helper()
	zr, err := zip.NewReader(bytes.NewReader(archive), int64(len(archive)))
	if err != nil {
		t.Fatalf("open zip: %v", err)
	}
	for _, f := range zr.File {
		if f.Name != name {
			continue
		}
		rc, err := f.Open()
		if err != nil {
			t.Fatalf("open entry %s: %v", name, err)
		}
		defer rc.Close()
		data, err := io.ReadAll(rc)
		if err != nil {
			t.Fatalf("read entry %s: %v", name, err)
		}
		return data, true
	}
	return nil, false
}

func TestBuildDayZipManifestNonEmptyWithEntries(t *testing.T) {
	start := time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)
	rows := []dayZipSegmentRow{
		{ID: 1, SegmentStartAt: start, DurationMs: 30000, ObjectKey: "obj/1", MIMEType: "video/mp4", SizeBytes: 5},
		{ID: 2, SegmentStartAt: start.Add(30 * time.Second), DurationMs: 30000, ObjectKey: "obj/2", MIMEType: "video/mp4", SizeBytes: 5},
	}

	var buf bytes.Buffer
	processed, err := buildDayZip(context.Background(), &buf, rows, "teststream", 7, fakeOpener([]byte("hello")), nil)
	if err != nil {
		t.Fatalf("buildDayZip: %v", err)
	}
	if processed != 2 {
		t.Fatalf("processed = %d, want 2", processed)
	}

	manifest, ok := readZipEntry(t, buf.Bytes(), "manifest.csv")
	if !ok {
		t.Fatal("manifest.csv not present in archive")
	}
	if len(manifest) == 0 {
		t.Fatal("manifest.csv is empty")
	}
	text := string(manifest)
	if !strings.Contains(text, "filename,segment_start_at,duration_ms,bytes,status") {
		t.Fatalf("manifest missing header row: %q", text)
	}
	// One header + two data rows.
	lines := strings.Split(strings.TrimRight(text, "\n"), "\n")
	if len(lines) != 3 {
		t.Fatalf("manifest line count = %d, want 3; content=%q", len(lines), text)
	}

	// Media entries must carry the copied payload.
	for _, name := range []string{
		dayZipItemName("teststream", 7, rows[0].SegmentStartAt, rows[0].ID, rows[0].MIMEType),
		dayZipItemName("teststream", 7, rows[1].SegmentStartAt, rows[1].ID, rows[1].MIMEType),
	} {
		data, ok := readZipEntry(t, buf.Bytes(), name)
		if !ok {
			t.Fatalf("media entry %s not present", name)
		}
		if string(data) != "hello" {
			t.Fatalf("media entry %s = %q, want %q", name, string(data), "hello")
		}
	}
}

func TestBuildDayZipClipManifestSchema(t *testing.T) {
	start := time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)
	end := start.Add(15 * time.Second)
	jobID, sequence := int64(99), int64(7)
	rows := []dayZipSegmentRow{
		{ID: 42, SegmentStartAt: start, ClipEndAt: &end, DurationMs: 15000, ObjectKey: "clips/42.mp4", MIMEType: "video/mp4", SizeBytes: 5,
			RecordingJobID: &jobID, CaptureSequence: &sequence, CaptureGeneration: "sha256:generation", SHA256: "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"},
	}

	var buf bytes.Buffer
	processed, err := buildDayZip(context.Background(), &buf, rows, "seoul-crosswalk", 1, fakeOpener([]byte("hello")), nil)
	if err != nil {
		t.Fatalf("buildDayZip: %v", err)
	}
	if processed != 1 {
		t.Fatalf("processed = %d, want 1", processed)
	}

	manifest, ok := readZipEntry(t, buf.Bytes(), "manifest.csv")
	if !ok {
		t.Fatal("manifest.csv not present in archive")
	}
	text := string(manifest)
	if !strings.Contains(text, "id,filename,start,end,duration_ms,size_bytes,object_key,status,recording_job_id,capture_generation,capture_sequence,sha256") {
		t.Fatalf("clip manifest missing header row: %q", text)
	}
	if !strings.Contains(text, "clips/42.mp4") {
		t.Fatalf("clip manifest missing object_key: %q", text)
	}
	if !strings.Contains(text, end.Format(time.RFC3339Nano)) {
		t.Fatalf("clip manifest missing end time: %q", text)
	}
	if !strings.Contains(text, ",ok,99,sha256:generation,7,2cf24dba") {
		t.Fatalf("clip manifest missing stitch provenance: %q", text)
	}
	lines := strings.Split(strings.TrimRight(text, "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("clip manifest line count = %d, want 2; content=%q", len(lines), text)
	}
	concatManifest, ok := readZipEntry(t, buf.Bytes(), "concat.txt")
	if !ok || !strings.Contains(string(concatManifest), "file 'seoul-crosswalk-20260610T120000Z-42.mp4'") {
		t.Fatalf("concat.txt missing ordered clip: %q", string(concatManifest))
	}
	instructions, ok := readZipEntry(t, buf.Bytes(), "STITCHING.txt")
	if !ok || !strings.Contains(string(instructions), "-c copy") {
		t.Fatalf("STITCHING.txt missing quality-preserving command: %q", string(instructions))
	}
}

func TestBuildDayZipConcatExcludesIntegrityMismatch(t *testing.T) {
	start := time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)
	end := start.Add(time.Minute)
	rows := []dayZipSegmentRow{
		{ID: 1, SegmentStartAt: start, ClipEndAt: &end, DurationMs: 60000, ObjectKey: "clips/1.mp4", MIMEType: "video/mp4", SizeBytes: 6},
		{ID: 2, SegmentStartAt: start.Add(time.Minute), ClipEndAt: &end, DurationMs: 60000, ObjectKey: "clips/2.mp4", MIMEType: "video/mp4", SizeBytes: 5, SHA256: strings.Repeat("0", 64)},
	}
	var buf bytes.Buffer
	if _, err := buildDayZip(context.Background(), &buf, rows, "teststream", 7, fakeOpener([]byte("hello")), nil); err != nil {
		t.Fatalf("buildDayZip: %v", err)
	}
	concatManifest, _ := readZipEntry(t, buf.Bytes(), "concat.txt")
	if got := string(concatManifest); got != "" {
		t.Fatalf("concat.txt includes integrity failures: %q", got)
	}
	manifest, _ := readZipEntry(t, buf.Bytes(), "manifest.csv")
	text := string(manifest)
	if !strings.Contains(text, "size mismatch: copied 5, expected 6") || !strings.Contains(text, "sha256 mismatch") {
		t.Fatalf("manifest missing integrity failures: %q", text)
	}
}

func TestBuildDayZipConcatPreservesRowOrderAndSkipsFailedObjects(t *testing.T) {
	start := time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)
	end1, end2, end3 := start.Add(time.Minute), start.Add(2*time.Minute), start.Add(3*time.Minute)
	rows := []dayZipSegmentRow{
		{ID: 1, SegmentStartAt: start, ClipEndAt: &end1, DurationMs: 60000, ObjectKey: "clips/1.mp4", MIMEType: "video/mp4", SizeBytes: 5},
		{ID: 2, SegmentStartAt: start.Add(time.Minute), ClipEndAt: &end2, DurationMs: 60000, ObjectKey: "clips/missing.mp4", MIMEType: "video/mp4", SizeBytes: 5},
		{ID: 3, SegmentStartAt: start.Add(2 * time.Minute), ClipEndAt: &end3, DurationMs: 60000, ObjectKey: "clips/3.mp4", MIMEType: "video/mp4", SizeBytes: 5},
	}
	opener := func(_ context.Context, key string) (io.ReadCloser, error) {
		if key == "clips/missing.mp4" {
			return nil, errors.New("not found")
		}
		return io.NopCloser(strings.NewReader("hello")), nil
	}

	var buf bytes.Buffer
	if _, err := buildDayZip(context.Background(), &buf, rows, "teststream", 7, opener, nil); err != nil {
		t.Fatalf("buildDayZip: %v", err)
	}
	concatManifest, ok := readZipEntry(t, buf.Bytes(), "concat.txt")
	if !ok {
		t.Fatal("concat.txt not present in archive")
	}
	want := "file 'teststream-20260610T120000Z-1.mp4'\nfile 'teststream-20260610T120200Z-3.mp4'\n"
	if got := string(concatManifest); got != want {
		t.Fatalf("concat.txt = %q, want %q", got, want)
	}
	manifest, _ := readZipEntry(t, buf.Bytes(), "manifest.csv")
	if !strings.Contains(string(manifest), "open error: not found") {
		t.Fatalf("manifest missing failed-object status: %q", string(manifest))
	}
}

func TestBuildDayZipEmptyDay(t *testing.T) {
	var buf bytes.Buffer
	processed, err := buildDayZip(context.Background(), &buf, nil, "teststream", 7, fakeOpener(nil), nil)
	if err != nil {
		t.Fatalf("buildDayZip: %v", err)
	}
	if processed != 0 {
		t.Fatalf("processed = %d, want 0", processed)
	}
	manifest, ok := readZipEntry(t, buf.Bytes(), "manifest.csv")
	if !ok {
		t.Fatal("manifest.csv not present in archive")
	}
	if !strings.Contains(string(manifest), "filename,segment_start_at,duration_ms,bytes,status") {
		t.Fatalf("manifest missing header row: %q", string(manifest))
	}
}
