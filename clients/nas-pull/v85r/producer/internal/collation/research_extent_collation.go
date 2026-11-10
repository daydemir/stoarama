// Package researchspan supplies an isolated research adapter after the owner
// has authenticated exact proof bytes, source SHA/size, tool/config and ACK.
// It never decides a join or alters packet duration/lineage evidence.
package collation

type SourceContext struct {
	SourceSHA             string
	SourceBytes           int64
	OwnerPinnedProofSHA   string
	VerifiedProofSHA      string
	StrictDecodePassed    bool
	FramePTS              []float64
	LastFrameDuration     float64
	RegisteredCaptureSpan float64
}
type Context struct {
	DecodedExtent        float64
	PacketDurationSum    float64
	CanonicalNominal     float64
	ResearchExpectedSpan float64
	PhysicalClock        string
	AudioContinuity      string
}
type failure string

func (f failure) Error() string { return string(f) }
func researchSpanFinite(v float64) bool {
	return v == v && v <= 1.7976931348623157e308 && v >= -1.7976931348623157e308
}
func researchSpanValidSHA(s string) bool {
	if len(s) != 64 {
		return false
	}
	for _, c := range s {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
}
func researchSpanAbs(v float64) float64 {
	if v < 0 {
		return -v
	}
	return v
}
func researchSpanMax(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

// Derive receives references already checked against an immutable owner ACK;
// the caller must hash actual bytes, not accept user-supplied booleans/digests.
func Derive(p SourceContext, expectedSHA string, expectedBytes int64, packetSum, nominal float64) (Context, error) {
	fail := func(s string) (Context, error) { return Context{}, failure(s) }
	if !researchSpanValidSHA(expectedSHA) || p.SourceSHA != expectedSHA || expectedBytes <= 0 || p.SourceBytes != expectedBytes {
		return fail("source binding mismatch")
	}
	if !researchSpanValidSHA(p.OwnerPinnedProofSHA) || p.VerifiedProofSHA != p.OwnerPinnedProofSHA || !p.StrictDecodePassed {
		return fail("proof authentication/decode failure")
	}
	n := len(p.FramePTS)
	if n < 100 {
		return fail("insufficient decoded frames")
	}
	steps := make([]float64, 0, n-1)
	for i, t := range p.FramePTS {
		if !researchSpanFinite(t) {
			return fail("nonfinite source PTS")
		}
		if i > 0 {
			step := t - p.FramePTS[i-1]
			if step <= 0 {
				return fail("nonmonotonic source PTS")
			}
			steps = append(steps, step)
		}
	}
	// Small retained frame vectors permit bounded insertion sorting with no I/O.
	sorted := append([]float64(nil), steps...)
	for i := 1; i < len(sorted); i++ {
		v := sorted[i]
		j := i
		for j > 0 && sorted[j-1] > v {
			sorted[j] = sorted[j-1]
			j--
		}
		sorted[j] = v
	}
	median := sorted[len(sorted)/2]
	if len(sorted)%2 == 0 {
		median = (sorted[len(sorted)/2-1] + median) / 2
	}
	for _, step := range steps {
		if step > researchSpanMax(.5, 5*median) {
			return fail("actual source PTS gap")
		}
	}
	if !researchSpanFinite(p.LastFrameDuration) || p.LastFrameDuration <= 0 || p.LastFrameDuration > researchSpanMax(.5, 5*median) {
		return fail("unknown final frame duration")
	}
	start := p.FramePTS[0]
	extent := p.FramePTS[n-1] + p.LastFrameDuration - start
	if !researchSpanFinite(packetSum) || packetSum <= 0 || !researchSpanFinite(nominal) || nominal < 0 || !researchSpanFinite(p.RegisteredCaptureSpan) || p.RegisteredCaptureSpan < 20 || researchSpanAbs(extent-p.RegisteredCaptureSpan) > researchSpanMax(.2, 3*median) {
		return fail("capture/decoded extent mismatch")
	}
	return Context{extent, packetSum, nominal, p.RegisteredCaptureSpan, "UNKNOWN", "UNKNOWN"}, nil
}

// ApplyResearchExtent changes a copied research input only. The caller must
// authenticate the SourceContext against a root-pinned immutable owner ACK,
// whole raw source SHA/size and the strict decode/PTS sidecar bytes. Ordinary
// DecideLocalSeam/MetadataGate/ReplayOverlap/GOP/HRD remain unchanged.
// Never pass this copy to ProcessHour or Build: it is not publication authority.
func ApplyResearchExtent(clip Clip, probe Probe, source SourceContext) (Clip, Probe, Context, error) {
	context, err := Derive(source, clip.SHA256, clip.SizeBytes, probe.Media.ContentSeconds, clip.NominalSeconds)
	if err != nil {
		return clip, probe, Context{}, err
	}
	// Preserve packet lists/durations, codec signature, audio inventory, minimum
	// frame duration and all metadata except the explicitly labelled requested span.
	researchClip, researchProbe := clip, probe
	researchClip.NominalSeconds = context.ResearchExpectedSpan
	researchProbe.Media.ContentSeconds = context.DecodedExtent
	return researchClip, researchProbe, context, nil
}
