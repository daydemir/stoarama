package api

import (
	"testing"
	"time"
)

func TestEffectiveRecordingStart(t *testing.T) {
	now := time.Date(2026, time.July, 22, 15, 48, 54, 0, time.UTC)
	past := now.Add(-5 * time.Minute)
	equal := now
	future := now.Add(time.Hour)

	for _, tc := range []struct {
		name      string
		requested *time.Time
		want      time.Time
	}{
		{name: "missing", want: now},
		{name: "past", requested: &past, want: now},
		{name: "equal", requested: &equal, want: now},
		{name: "future", requested: &future, want: future},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := effectiveRecordingStart(tc.requested, now); !got.Equal(tc.want) {
				t.Fatalf("got %s want %s", got, tc.want)
			}
		})
	}
}
