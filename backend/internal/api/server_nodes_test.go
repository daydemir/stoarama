package api

import "testing"

func TestNormalizeNodeTypeLocalRecorder(t *testing.T) {
	got, ok := normalizeNodeType("local_recorder")
	if !ok {
		t.Fatalf("normalizeNodeType(local_recorder) ok=false")
	}
	if got != nodeTypeLocalRecorder {
		t.Fatalf("normalizeNodeType(local_recorder)=%q want %q", got, nodeTypeLocalRecorder)
	}
}
