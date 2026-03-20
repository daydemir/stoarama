package api

import "testing"

func TestSampleEvenlyEmptyAndBounds(t *testing.T) {
	t.Parallel()

	if out := sampleEvenly(nil, 10); len(out) != 0 {
		t.Fatalf("expected empty slice for nil input, got=%v", out)
	}
	if out := sampleEvenly([]string{"a", "b"}, 0); len(out) != 0 {
		t.Fatalf("expected empty slice for count=0, got=%v", out)
	}
	if out := sampleEvenly([]string{"a", "b"}, 5); len(out) != 2 || out[0] != "a" || out[1] != "b" {
		t.Fatalf("expected all values when count>=len, got=%v", out)
	}
}

func TestSampleEvenlySinglePickUsesMiddle(t *testing.T) {
	t.Parallel()

	in := []string{"d0", "d1", "d2", "d3", "d4"}
	out := sampleEvenly(in, 1)
	if len(out) != 1 {
		t.Fatalf("expected one item, got=%v", out)
	}
	if out[0] != "d2" {
		t.Fatalf("expected middle item d2, got=%q", out[0])
	}
}

func TestSampleEvenlyProducesDeterministicSpacedUniqueOrder(t *testing.T) {
	t.Parallel()

	in := []string{"d0", "d1", "d2", "d3", "d4", "d5", "d6", "d7", "d8", "d9"}
	out := sampleEvenly(in, 4)
	want := []string{"d0", "d3", "d6", "d9"}
	if len(out) != len(want) {
		t.Fatalf("unexpected length got=%d want=%d out=%v", len(out), len(want), out)
	}
	for i := range want {
		if out[i] != want[i] {
			t.Fatalf("unexpected out[%d]=%q want=%q (out=%v)", i, out[i], want[i], out)
		}
	}
	seen := make(map[string]struct{}, len(out))
	for _, v := range out {
		if _, exists := seen[v]; exists {
			t.Fatalf("sample contains duplicate value %q: %v", v, out)
		}
		seen[v] = struct{}{}
	}
}
