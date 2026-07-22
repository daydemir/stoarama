package api

import (
	"testing"
	"time"
)

func TestEffectiveRecordingStart(t *testing.T) {
	local := time.FixedZone("test", -4*60*60)
	now := time.Date(2026, time.July, 22, 11, 48, 54, 0, local)
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
			got := effectiveRecordingStart(tc.requested, now)
			if !got.Equal(tc.want) {
				t.Fatalf("got %s want %s", got, tc.want)
			}
			if got.Location() != time.UTC {
				t.Fatalf("location = %s, want UTC", got.Location())
			}
		})
	}
}
