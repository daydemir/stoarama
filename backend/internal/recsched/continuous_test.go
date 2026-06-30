package recsched

import (
	"testing"
	"time"
)

func tod(h, m int) TimeOfDay { return TimeOfDay{Hour: h, Minute: m} }

func TestValidateContinuousWindowForCreate(t *testing.T) {
	if err := ValidateContinuousWindowForCreate(tod(9, 0), tod(21, 0), 60); err != nil {
		t.Fatalf("valid window rejected: %v", err)
	}
	if err := ValidateContinuousWindowForCreate(tod(21, 0), tod(9, 0), 60); err == nil {
		t.Fatalf("expected rejection of start>=end (midnight crossing)")
	}
	if err := ValidateContinuousWindowForCreate(tod(9, 0), tod(9, 0), 60); err == nil {
		t.Fatalf("expected rejection of zero-length window")
	}
	if err := ValidateContinuousWindowForCreate(tod(9, 0), tod(21, 0), 1); err == nil {
		t.Fatalf("expected rejection of sub-5s segment length")
	}
	if err := ValidateContinuousWindowForCreate(tod(9, 0), tod(21, 0), 1000); err == nil {
		t.Fatalf("expected rejection of >900s segment length")
	}
}

func TestCurrentOpenContinuousWindow(t *testing.T) {
	env := time.Date(2026, 6, 30, 0, 0, 0, 0, time.UTC)
	// 10:00 UTC is inside a 09:00-21:00 UTC window.
	now := time.Date(2026, 6, 30, 10, 0, 0, 0, time.UTC)
	open, openUTC, closeUTC, err := currentOpenContinuousWindow("UTC", tod(9, 0), tod(21, 0), env, time.Time{}, now)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !open {
		t.Fatalf("expected window open at 10:00")
	}
	if !openUTC.Equal(time.Date(2026, 6, 30, 9, 0, 0, 0, time.UTC)) {
		t.Fatalf("openUTC=%s", openUTC)
	}
	if !closeUTC.Equal(time.Date(2026, 6, 30, 21, 0, 0, 0, time.UTC)) {
		t.Fatalf("closeUTC=%s", closeUTC)
	}
	// 22:00 UTC is outside.
	nowClosed := time.Date(2026, 6, 30, 22, 0, 0, 0, time.UTC)
	open, _, _, err = currentOpenContinuousWindow("UTC", tod(9, 0), tod(21, 0), env, time.Time{}, nowClosed)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if open {
		t.Fatalf("expected window closed at 22:00")
	}
}

func TestNextWindowOpenUTC(t *testing.T) {
	env := time.Date(2026, 6, 30, 0, 0, 0, 0, time.UTC)
	// At 22:00 the next 09:00 open is the next day.
	now := time.Date(2026, 6, 30, 22, 0, 0, 0, time.UTC)
	next, err := NextWindowOpenUTC("UTC", tod(9, 0), env, time.Time{}, now)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	want := time.Date(2026, 7, 1, 9, 0, 0, 0, time.UTC)
	if !next.Equal(want) {
		t.Fatalf("next=%s want %s", next, want)
	}
}
