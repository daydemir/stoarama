package api

import "testing"

func TestNormalizeLocalTimezone(t *testing.T) {
	for _, zone := range []string{"", "Europe/London", "Asia/Seoul"} {
		if got, err := normalizeLocalTimezone(zone); err != nil || got != zone {
			t.Fatalf("normalizeLocalTimezone(%q) = %q, %v", zone, got, err)
		}
	}
	if _, err := normalizeLocalTimezone("London"); err == nil {
		t.Fatal("expected invalid timezone error")
	}
}
