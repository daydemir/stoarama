package collation

// Research-only callpoint alongside the ordinary EvaluateFrames result.
// Callers serialize Diagnostic separately; Ordinary remains the exact canonical
// value. No canonical producer, queue, or production code is changed here.
type SpatialResearchFrameResult struct {
	Ordinary   MatchEvidence     `json:"ordinary"`
	Diagnostic SpatialDiagnostic `json:"spatial_temporal_diagnostic"`
}

func EvaluateFramesSpatialResearch(policy SeamPolicy, tail, head []Frame, tailKeys, headKeys []bool, frameSeconds float64) SpatialResearchFrameResult {
	return SpatialResearchFrameResult{Ordinary: EvaluateFrames(policy, tail, head, tailKeys, headKeys, frameSeconds), Diagnostic: DiagnoseSpatialSeam(tail, head, policy.FrameWidth, policy.FrameHeight)}
}
