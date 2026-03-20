package inferencebox

import (
	"testing"
	"time"
)

func TestSanitizeObjectKeyToken(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{in: "yolo11x__tile640-o25-img1280__balanced", want: "yolo11x-tile640-o25-img1280-balanced"},
		{in: "   ", want: "unknown"},
		{in: "---", want: "unknown"},
		{in: "A/B C", want: "a-b-c"},
	}
	for _, tc := range tests {
		got := sanitizeObjectKeyToken(tc.in)
		if got != tc.want {
			t.Fatalf("sanitizeObjectKeyToken(%q)=%q want=%q", tc.in, got, tc.want)
		}
	}
}

func TestBackoffForAttempt(t *testing.T) {
	base := 5 * time.Second
	max := 40 * time.Second
	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{attempt: 1, want: 5 * time.Second},
		{attempt: 2, want: 10 * time.Second},
		{attempt: 3, want: 20 * time.Second},
		{attempt: 4, want: 40 * time.Second},
		{attempt: 5, want: 40 * time.Second},
	}
	for _, tc := range tests {
		got := backoffForAttempt(tc.attempt, base, max)
		if got != tc.want {
			t.Fatalf("backoffForAttempt(%d)=%s want=%s", tc.attempt, got, tc.want)
		}
	}
}
