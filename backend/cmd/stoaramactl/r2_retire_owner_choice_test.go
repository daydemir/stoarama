package main

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/api"
)

func TestR2RetireDeliveryUnknownOwnerCannotBecomeNone(t *testing.T) {
	w := r2tWindow(t, 3, "Asia/Tokyo", "2026-09-12", "2026-10-10")
	w.BestStart = "2026-09-19"
	windows := map[int64]api.Keep28Window{3: w}
	scope := r2RetireScope{Class: r2RetireDeliveryOnly, Bucket: "stoarama", NASBacked: true}
	for _, tc := range []struct {
		name  string
		picks map[int64]r2RetirePick
	}{
		{"missing", map[int64]r2RetirePick{}},
		{"empty", map[int64]r2RetirePick{3: {KeepStartUTC: w.Start.Format(time.RFC3339), KeepEndUTC: w.End.Format(time.RFC3339)}}},
		{"invalid", map[int64]r2RetirePick{3: {Choice: "unknown", KeepStartUTC: w.Start.Format(time.RFC3339), KeepEndUTC: w.End.Format(time.RFC3339)}}},
		{"unbound", map[int64]r2RetirePick{3: {Choice: "none"}}},
		{"stale", map[int64]r2RetirePick{3: {Choice: "none", KeepStartUTC: w.Start.Add(time.Hour).Format(time.RFC3339), KeepEndUTC: w.End.Format(time.RFC3339)}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if choice := r2RetireDeliveryChoices(tc.picks, windows)["3"]; choice != "" {
				t.Fatalf("header manufactured choice %q", choice)
			}
			if policy := string(r2RetireDeliveryPolicy(windows, tc.picks)); policy != "[]" {
				t.Fatalf("unknown owner admitted to source policy: %s", policy)
			}
			for _, kept := range []bool{false, true} {
				start := w.Start.Add(-time.Hour)
				if kept {
					start = w.Start.Add(time.Hour)
				}
				candidate := r2tCandidate("raw", 3, w.Timezone, start, start.Add(time.Minute))
				candidate.Facts.ExactNAS = kept
				reason, choice, eligible := r2RetireDecideForScope(candidate, scope, windows, tc.picks)
				if eligible || choice == "none" || reason == "" {
					t.Errorf("unproven owner choice became authority: kept=%v eligible=%v choice=%q reason=%q", kept, eligible, choice, reason)
				}
			}
		})
	}
}

func TestR2RetireDeliveryBoundChoiceAndAllAgreeAcrossSites(t *testing.T) {
	w := r2tWindow(t, 3, "Asia/Tokyo", "2026-09-12", "2026-10-10")
	windows := map[int64]api.Keep28Window{3: w}
	scope := r2RetireScope{Class: r2RetireDeliveryOnly, Bucket: "stoarama", NASBacked: true}
	candidate := r2tCandidate("raw", 3, w.Timezone, w.Start.Add(-time.Hour), w.Start)
	for _, choice := range []string{"none", "keep", "st", "all"} {
		t.Run(choice, func(t *testing.T) {
			picks := map[int64]r2RetirePick{3: {Choice: choice, KeepStartUTC: w.Start.Format(time.RFC3339), KeepEndUTC: w.End.Format(time.RFC3339)}}
			if choice == "all" {
				picks[3] = r2RetirePick{Choice: "all"} // Even an unbound All vetoes.
			}
			want := "none"
			if choice == "all" {
				want = "all"
			}
			reason, actual, eligible := r2RetireDecideForScope(candidate, scope, windows, picks)
			if actual != want || eligible != (choice != "all") || (choice == "all" && reason != "pick_all") {
				t.Fatalf("classification: %q %q %v", reason, actual, eligible)
			}
			if actual := r2RetireDeliveryChoices(picks, windows)["3"]; actual != want {
				t.Fatalf("header differs: %q", actual)
			}
			var rows []struct {
				Stream int64 `json:"stream"`
			}
			if err := json.Unmarshal(r2RetireDeliveryPolicy(windows, picks), &rows); err != nil {
				t.Fatal(err)
			}
			if choice == "all" {
				if len(rows) != 0 {
					t.Fatal("All entered source policy")
				}
			} else if len(rows) != 1 || rows[0].Stream != 3 {
				t.Fatal("bound non-All missing from source policy", rows)
			}
		})
	}
}
