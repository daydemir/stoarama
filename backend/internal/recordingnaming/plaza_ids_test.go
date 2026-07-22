package recordingnaming

import "testing"

func TestParsePlazaIDNormalizesZeroPadding(t *testing.T) {
	want, err := parsePlazaID("8")
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{"08", "008"} {
		got, err := parsePlazaID(raw)
		if err != nil || got != want {
			t.Fatalf("parsePlazaID(%q)=(%d,%v), want (%d,nil)", raw, got, err, want)
		}
	}
}
