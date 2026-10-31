package collation

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// hrdSynth encodes a demanding (noisy, moving) pattern with NAL HRD buffering
// period and picture timing SEI and a fixed 1 s GOP, then segments it by
// stream copy exactly like the capture segmenter (cut in front of keyframes,
// per-file timestamps reset).
func hrdSynth(t *testing.T, tools Tools, dir string) []string {
	src := filepath.Join(dir, "hrd-src.mp4")
	cmd := exec.Command(tools.FFmpeg, "-nostdin", "-v", "error", "-f", "lavfi", "-i", "testsrc2=size=320x240:rate=25:duration=36,noise=alls=60:allf=t",
		"-c:v", "libx264", "-preset", "veryfast", "-x264-params", "nal-hrd=vbr:keyint=25:min-keyint=25:scenecut=0:bframes=0",
		"-maxrate", "600k", "-bufsize", "1200k", "-pix_fmt", "yuv420p", src)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Skipf("cannot synthesize HRD test media (libx264 needed): %v %s", err, out)
	}
	seg := exec.Command(tools.FFmpeg, "-nostdin", "-v", "error", "-i", src, "-map", "0:v:0", "-c", "copy", "-f", "segment", "-segment_time", "6",
		"-reset_timestamps", "1", filepath.Join(dir, "seg%02d.mp4"))
	if out, err := seg.CombinedOutput(); err != nil {
		t.Fatalf("segment: %v %s", err, out)
	}
	paths, _ := filepath.Glob(filepath.Join(dir, "seg*.mp4"))
	if len(paths) < 5 {
		t.Fatalf("segments: %v", paths)
	}
	return paths
}

func TestHRDSyntheticSegmentsProveAdjacency(t *testing.T) {
	tools := requireFFmpeg(t)
	segs := hrdSynth(t, tools, t.TempDir())
	pol := DefaultHRDPolicy()
	ev := func(a, b string) SourceEvidence {
		e := hrdSeamEvidence(pol, a, b)
		return e
	}
	// The encoder's first GOPs fill an empty buffer (not yet the steady
	// identity), so the first segment calibrates nothing: start at the second.
	if e := ev(segs[0], segs[1]); e.Status == SourceContradicts {
		t.Fatalf("first seam contradicts: %+v", e)
	}
	for i := 1; i+1 < len(segs); i++ {
		if e := ev(segs[i], segs[i+1]); e.Status != SourceAdjacent {
			t.Fatalf("consecutive segments %d->%d: %+v", i, i+1, e)
		}
	}
	// A skipped segment (a reconnect with the same framing 6 s later) and a
	// replayed one are discontinuities.
	if e := ev(segs[1], segs[3]); e.Status != SourceContradicts {
		t.Fatalf("skip: %+v", e)
	}
	if e := ev(segs[3], segs[2]); e.Status != SourceContradicts {
		t.Fatalf("rewind: %+v", e)
	}
}

// TestProcessHourV3JoinsProvenCutShortSeam runs a whole hour under v3. The
// first clip is shorter than the nominal clip length, so v2 splits A->B
// (prev_clip_cut_short) although the footage is continuous; the source proves
// it and v3 joins. B->D skips a segment under forged contiguous stamps and
// consecutive sequence numbers: it must split.
func TestProcessHourV3JoinsProvenCutShortSeam(t *testing.T) {
	tools := requireFFmpeg(t)
	dir := t.TempDir()
	segs := hrdSynth(t, tools, dir)
	ctx := context.Background()
	t0 := time.Date(2026, 8, 13, 17, 0, 0, 0, time.UTC)
	w := testWork()
	w.PolicyVersion = PolicyVersionV3
	w.HourID, _ = HourIDForGeneration(w.BatchID, w.RecordingID, w.LocalDate, w.DeliveryHour, GenerationV3)
	w.ScheduledStart, w.ScheduledEnd = t0, t0.Add(time.Hour)
	at := t0
	for i, p := range []string{segs[1], segs[2], segs[4]} {
		probe, err := ProbeFile(ctx, tools, p)
		if err != nil || !probe.Media.Playable {
			t.Fatalf("probe %s: %v", p, err)
		}
		n, sha, err := fileIdentity(p)
		if err != nil {
			t.Fatal(err)
		}
		dur := time.Duration(probe.Media.ContentSeconds * float64(time.Second))
		w.Clips = append(w.Clips, Clip{ClipID: int64(i + 1), RecordingID: w.RecordingID, JobID: 9, CaptureLeaseToken: "lease", CaptureSequence: int64(i + 1),
			StartUTC: at, EndUTC: at.Add(dur), ObjectKey: filepath.Base(p), SizeBytes: n, SHA256: sha, NominalSeconds: 9})
		at = at.Add(dur)
	}
	store := &dirStore{src: dir, published: map[string]string{}}
	env := Env{Tools: tools, Policy: SeamPolicyFor(PolicyV3), ScratchRoot: filepath.Join(dir, "scratch"), Store: store, MediaTool: "test",
		CPU: make(chan struct{}, 2), Net: make(chan struct{}, 2), Now: time.Now}
	m, err := ProcessHour(ctx, env, w)
	if err != nil {
		t.Fatal(err)
	}
	js, _ := json.MarshalIndent(m.Seams, "", " ")
	if len(m.Seams) != 2 || m.Seams[0].Decision != DecisionJoin || m.Seams[0].Reason != "source_adjacent" || m.Seams[1].Decision != DecisionSplit {
		t.Fatalf("seams %s", js)
	}
	if m.PolicyVersion != PolicyVersionV3 || m.Generation != GenerationV3 || len(m.Outputs) != 2 || fmt.Sprint(m.Outputs[0].SourceClipIDs) != "[1 2]" {
		t.Fatalf("manifest %s/%d outputs %+v seams %s", m.PolicyVersion, m.Generation, m.Outputs, js)
	}
	if err := m.Validate(); err != nil {
		t.Fatal(err)
	}
	// The same hour under v2 keeps the heuristic split.
	w2 := w
	w2.PolicyVersion = ""
	w2.HourID, _ = HourIDFor(w.BatchID, w.RecordingID, w.LocalDate, w.DeliveryHour)
	env2 := env
	env2.Policy = DefaultSeamPolicy()
	env2.Store = &dirStore{src: dir, published: map[string]string{}}
	m2, err := ProcessHour(ctx, env2, w2)
	if err != nil {
		t.Fatal(err)
	}
	if m2.Seams[0].Decision != DecisionSplit || m2.Seams[0].Reason != "prev_clip_cut_short" || m2.Seams[0].Source != nil {
		t.Fatalf("v2 seam %+v", m2.Seams[0])
	}
}
