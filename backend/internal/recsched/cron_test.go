package recsched

import (
	"testing"
	"time"
)

func TestParseCron(t *testing.T) {
	valid := []string{"*/5 * * * *", "0 9 * * 1", "0 0 1 1 *", "15,45 * * * *"}
	for _, e := range valid {
		if _, err := ParseCron(e); err != nil {
			t.Fatalf("expected %q to parse, got %v", e, err)
		}
	}
	invalid := []string{
		"",                // empty
		"@hourly",         // descriptor
		"@every 1h",       // descriptor
		"*/5 * * * * *",   // 6 fields (seconds)
		"* * * *",         // 4 fields
		"99 * * * *",      // out of range minute
		"not a cron",      // garbage
	}
	for _, e := range invalid {
		if _, err := ParseCron(e); err == nil {
			t.Fatalf("expected %q to be rejected", e)
		}
	}
}

func TestValidateCronForCreate(t *testing.T) {
	const minInterval = 600 // 10 minutes

	// Every 5 minutes is below the 10-minute floor -> rejected.
	if err := ValidateCronForCreate("*/5 * * * *", "UTC", minInterval, 60); err == nil {
		t.Fatal("expected */5 to be rejected as too frequent")
	}
	// Every 15 minutes clears the floor with a 60s clip.
	if err := ValidateCronForCreate("*/15 * * * *", "UTC", minInterval, 60); err != nil {
		t.Fatalf("expected */15 with 60s clip to pass, got %v", err)
	}
	// Clip duration >= the min gap (900s clip vs 900s gap) -> rejected.
	if err := ValidateCronForCreate("*/15 * * * *", "UTC", minInterval, 900); err == nil {
		t.Fatal("expected 900s clip against a 900s gap to be rejected")
	}
	// Unknown timezone -> rejected.
	if err := ValidateCronForCreate("*/15 * * * *", "Mars/Phobos", minInterval, 60); err == nil {
		t.Fatal("expected unknown timezone to be rejected")
	}
	// Known non-UTC timezone is accepted.
	if err := ValidateCronForCreate("0 9 * * *", "America/New_York", minInterval, 60); err != nil {
		t.Fatalf("expected America/New_York daily cron to pass, got %v", err)
	}
}

func TestNextFireUTC(t *testing.T) {
	// 09:00 in New York on a winter day is 14:00 UTC (EST, UTC-5).
	after := time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC)
	got, err := NextFireUTC("0 9 * * *", "America/New_York", after)
	if err != nil {
		t.Fatalf("NextFireUTC error: %v", err)
	}
	want := time.Date(2026, 1, 15, 14, 0, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Fatalf("expected %s, got %s", want, got)
	}
	if got.Location() != time.UTC {
		t.Fatalf("expected UTC location, got %s", got.Location())
	}
}
