package collation

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"
)

// offlinePair is one seam between two local source clips, for offline
// re-evaluation of a policy against frozen clip metadata.
type offlinePair struct {
	ID       string `json:"id"`
	Prev     Clip   `json:"prev"`
	Next     Clip   `json:"next"`
	PrevPath string `json:"prev_path"`
	NextPath string `json:"next_path"`
}

type offlineResult struct {
	ID string       `json:"id"`
	V2 SeamDecision `json:"v2"`
	V3 SeamDecision `json:"v3"`
	// Error is set when a clip could not be probed (the seam is not scored).
	Error string `json:"error,omitempty"`
}

// TestOfflineSeamEval re-decides seams under collation-v2 and collation-v3.
// COLLATION_EVAL_IN is a JSONL file of offlinePair; results are written as
// JSONL to COLLATION_EVAL_OUT. Read-only on the clips. Not run in CI.
func TestOfflineSeamEval(t *testing.T) {
	in, outPath := os.Getenv("COLLATION_EVAL_IN"), os.Getenv("COLLATION_EVAL_OUT")
	if in == "" || outPath == "" {
		t.Skip("COLLATION_EVAL_IN/COLLATION_EVAL_OUT not set")
	}
	tools := ToolsFromEnv()
	ctx := context.Background()
	f, err := os.Open(in)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	out, err := os.Create(outPath)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()
	envFor := func(spec PolicySpec) Env {
		// Two concurrent decoders at most: this runs next to live capture.
		return Env{Tools: tools, Policy: SeamPolicyFor(spec), CPU: make(chan struct{}, 2), Net: make(chan struct{}, 1), Now: time.Now}
	}
	v2, v3 := envFor(PolicyV2), envFor(PolicyV3)
	probes := map[string]Probe{}
	probe := func(path string) (Probe, error) {
		if p, ok := probes[path]; ok {
			return p, nil
		}
		p, err := ProbeFile(ctx, tools, path)
		if err == nil {
			probes[path] = p
		}
		return p, err
	}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	enc := json.NewEncoder(out)
	for sc.Scan() {
		var p offlinePair
		if err := json.Unmarshal(sc.Bytes(), &p); err != nil {
			t.Fatal(err)
		}
		r := offlineResult{ID: p.ID}
		pa, errA := probe(p.PrevPath)
		pb, errB := probe(p.NextPath)
		if errA != nil || errB != nil || !pa.Media.Playable || !pb.Media.Playable {
			r.Error = "probe failed"
			_ = enc.Encode(r)
			continue
		}
		a := LocalClip{Clip: p.Prev, Path: p.PrevPath, Probe: pa}
		b := LocalClip{Clip: p.Next, Path: p.NextPath, Probe: pb}
		r.V2 = DecideLocalSeam(ctx, v2, a, b)
		r.V3 = DecideLocalSeam(ctx, v3, a, b)
		if err := enc.Encode(r); err != nil {
			t.Fatal(err)
		}
		t.Logf("%s v2=%s/%s v3=%s/%s", p.ID, r.V2.Decision, r.V2.Reason, r.V3.Decision, r.V3.Reason)
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
}

// TestOfflineHRDEval computes only the HRD source evidence of seams. Inputs
// may be sparse copies holding just the moov box, the previous clip's last
// TailUnits samples and the next clip's first sample (COLLATION_HRD_IN JSONL:
// id, prev_path, next_path, prev_tail_units; results to COLLATION_HRD_OUT).
// Not run in CI.
func TestOfflineHRDEval(t *testing.T) {
	in, outPath := os.Getenv("COLLATION_HRD_IN"), os.Getenv("COLLATION_HRD_OUT")
	if in == "" || outPath == "" {
		t.Skip("COLLATION_HRD_IN/COLLATION_HRD_OUT not set")
	}
	f, err := os.Open(in)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	out, err := os.Create(outPath)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()
	enc := json.NewEncoder(out)
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var p struct {
			ID        string `json:"id"`
			PrevPath  string `json:"prev_path"`
			NextPath  string `json:"next_path"`
			TailUnits int    `json:"prev_tail_units"`
		}
		if err := json.Unmarshal(sc.Bytes(), &p); err != nil {
			t.Fatal(err)
		}
		pol := DefaultHRDPolicy()
		pol.TailUnits = min(pol.TailUnits, p.TailUnits)
		ev := hrdSeamEvidence(pol, p.PrevPath, p.NextPath)
		if err := enc.Encode(map[string]any{"id": p.ID, "source": ev}); err != nil {
			t.Fatal(err)
		}
	}
}
