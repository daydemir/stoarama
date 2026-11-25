package collation

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func (s *dirStore) Exists(_ context.Context, key string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.published[key]
	return ok, nil
}

// A second, independent stitch of the same hour from the same raw bytes (other
// scratch paths, later clock) reproduces every output byte for byte, so its
// content-addressed key is the registered key. A restore republishes exactly
// those keys and never writes a manifest; any difference publishes nothing.
func TestRestoreRebuildIsByteIdenticalAndPublishesOnlyRegisteredKeys(t *testing.T) {
	tools := requireFFmpeg(t)
	dir := t.TempDir()
	src := synth(t, tools, dir)
	for name, r := range map[string][2]float64{"a.mp4": {0, 10}, "b.mp4": {10, 10}, "c.mp4": {23, 10}} {
		cut(t, tools, src, filepath.Join(dir, name), r[0], r[1])
	}
	ctx := context.Background()
	t0 := time.Date(2026, 8, 13, 17, 0, 0, 0, time.UTC)
	w := testWork()
	w.HourID, _ = HourIDFor(w.BatchID, w.RecordingID, w.LocalDate, w.DeliveryHour)
	w.ScheduledStart, w.ScheduledEnd = t0, t0.Add(time.Hour)
	at := t0
	for i, name := range []string{"a.mp4", "b.mp4", "c.mp4"} {
		p, err := ProbeFile(ctx, tools, filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		n, sha, err := fileIdentity(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		dur := time.Duration(p.Media.ContentSeconds * float64(time.Second))
		w.Clips = append(w.Clips, Clip{ClipID: int64(i + 1), RecordingID: w.RecordingID, JobID: 9, CaptureSequence: int64(i + 1),
			StartUTC: at, EndUTC: at.Add(dur), ObjectKey: name, SizeBytes: n, SHA256: sha})
		at = at.Add(dur)
	}
	env := func(store *dirStore, scratch string, now time.Time) Env {
		return Env{Tools: tools, Policy: DefaultSeamPolicy(), ScratchRoot: filepath.Join(dir, scratch), Store: store, MediaTool: "test",
			CPU: make(chan struct{}, 4), Net: make(chan struct{}, 4), Now: func() time.Time { return now }}
	}
	first := &dirStore{src: dir, published: map[string]string{}}
	m, err := ProcessHour(ctx, env(first, "scratch-1", t0), w)
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Outputs) != 2 {
		t.Fatalf("outputs %+v", m.Outputs)
	}
	restore := RestoreHour{MediaTool: "test"}
	for _, o := range m.Outputs {
		restore.Outputs = append(restore.Outputs, RestoreOutput{ObjectKey: o.ObjectKey, SizeBytes: o.SizeBytes, SHA256: o.SHA256})
	}

	rw := w
	rw.Restore = &restore
	second := &dirStore{src: dir, published: map[string]string{}}
	again, err := ProcessHour(ctx, env(second, "scratch-2", t0.Add(48*time.Hour)), rw)
	if err != nil {
		t.Fatalf("rebuild differs from the first stitch: %v", err)
	}
	for i, o := range again.Outputs {
		if o.ObjectKey != m.Outputs[i].ObjectKey || o.SHA256 != m.Outputs[i].SHA256 || o.SizeBytes != m.Outputs[i].SizeBytes {
			t.Fatalf("part %d: %+v vs %+v", i+1, o, m.Outputs[i])
		}
	}
	if len(second.published) != len(restore.Outputs) {
		t.Fatalf("restore published %v", second.published)
	}
	for _, o := range restore.Outputs {
		if second.published[o.ObjectKey] != o.SHA256 {
			t.Fatalf("restore did not republish %s", o.ObjectKey)
		}
	}

	for name, bad := range map[string]RestoreHour{
		"bytes": {MediaTool: "test", Outputs: []RestoreOutput{restore.Outputs[0], {ObjectKey: ObjectKey(w.BatchID, strings.Repeat("0", 64)), SizeBytes: 1, SHA256: strings.Repeat("0", 64)}}},
		"parts": {MediaTool: "test", Outputs: restore.Outputs[:1]},
		"tool":  {MediaTool: "ffmpeg version other", Outputs: restore.Outputs},
	} {
		bw := w
		bw.Restore = &bad
		store := &dirStore{src: dir, published: map[string]string{}}
		if _, err := ProcessHour(ctx, env(store, "scratch-"+name, t0), bw); !errors.Is(err, ErrRestoreDiffers) || len(store.published) != 0 {
			t.Fatalf("%s: err=%v published=%v", name, err, store.published)
		}
	}

	// The runner restores missing registered objects, skips present ones, and
	// reports a difference without publishing.
	var results bytes.Buffer
	missing := &dirStore{src: dir, published: map[string]string{}}
	if err := RunWorklist(ctx, env(missing, "scratch-run", t0), missing, []HourWork{rw}, 1, &results); err != nil {
		t.Fatal(err)
	}
	present := &dirStore{src: dir, published: map[string]string{}}
	for _, o := range restore.Outputs {
		present.published[o.ObjectKey] = o.SHA256
	}
	if err := RunWorklist(ctx, env(present, "scratch-run", t0), present, []HourWork{rw}, 1, &results); err != nil {
		t.Fatal(err)
	}
	differs := rw
	differs.Restore = &RestoreHour{MediaTool: "test", Outputs: restore.Outputs[:1]}
	empty := &dirStore{src: dir, published: map[string]string{}}
	if err := RunWorklist(ctx, env(empty, "scratch-run", t0), empty, []HourWork{differs}, 1, &results); err != nil {
		t.Fatal(err)
	}
	got := results.String()
	for _, want := range []string{`"outcome":"restored"`, `"outcome":"skipped_existing"`, `"outcome":"differs"`} {
		if !strings.Contains(got, want) {
			t.Fatalf("results missing %s:\n%s", want, got)
		}
	}
	if len(missing.published) != 2 || len(empty.published) != 0 {
		t.Fatalf("published restored=%v differs=%v", missing.published, empty.published)
	}
}
