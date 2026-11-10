package collation

// ResearchPairVeto must be populated from a separately hash-pinned current
// immutable source/human inventory before any feature evaluation. IDs are not
// part of the policy: exact whole-source SHA aliases and ordered SHA/size pairs
// preserve rejection under arbitrary clip-ID renaming.
type ResearchPairVeto struct {
	SourceSHA    map[string]bool
	OrderedPairs map[ResearchSourcePair]bool
}
type ResearchSourcePair struct {
	PrevSHA   string
	PrevBytes int64
	NextSHA   string
	NextBytes int64
}

func (v ResearchPairVeto) RejectResearchPair(a, b Clip) bool {
	return v.SourceSHA[a.SHA256] || v.SourceSHA[b.SHA256] || v.OrderedPairs[ResearchSourcePair{a.SHA256, a.SizeBytes, b.SHA256, b.SizeBytes}]
}

// PrepareResearchPair is the route boundary after the owner has verified the
// immutable context manifest, ACK, proof bytes, raw whole SHA/size and tools.
// It neither calls a decoder nor nominates a join. The request route must use
// DecideLocalSeam on its returned copies to obtain genuine visual/GOP/HRD
// observations; a missing window stays UNKNOWN and is never synthesized.
func PrepareResearchPair(a, b LocalClip, ac, bc SourceContext, veto ResearchPairVeto) (LocalClip, LocalClip, Context, Context, error) {
	if len(veto.SourceSHA) < 69 || len(veto.OrderedPairs) < 12 {
		return a, b, Context{}, Context{}, failure("uninitialized current source/human veto inventory")
	}
	if veto.RejectResearchPair(a.Clip, b.Clip) {
		return a, b, Context{}, Context{}, failure("known source/human veto")
	}
	ar, apr, actx, err := ApplyResearchExtent(a.Clip, a.Probe, ac)
	if err != nil {
		return a, b, Context{}, Context{}, err
	}
	br, bpr, bctx, err := ApplyResearchExtent(b.Clip, b.Probe, bc)
	if err != nil {
		return a, b, Context{}, Context{}, err
	}
	// Copy the LocalClip values; original paths and packet-bearing Probes retain
	// their identity. No ProcessHour/build/publication route receives these copies.
	aa, bb := a, b
	aa.Clip = ar
	aa.Probe = apr
	bb.Clip = br
	bb.Probe = bpr
	return aa, bb, actx, bctx, nil
}
