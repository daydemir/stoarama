package main

import (
	"context"
	"github.com/daydemir/stoarama/backend/internal/api"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestR2RetireDeliveryOnlyOrderingAndAllPicks(t *testing.T) {
	w := r2tWindow(t, 3, "Asia/Tokyo", "2026-09-12", "2026-10-10")
	windows := map[int64]api.Keep28Window{3: w}
	scope := r2RetireScope{Class: r2RetireDeliveryOnly, Bucket: "stoarama", NASBacked: true}
	base := r2tCandidate("raw", 3, w.Timezone, w.Start.Add(-time.Hour), w.Start)
	for _, tc := range []struct {
		name     string
		inside   bool
		facts    r2RetireFacts
		reason   string
		eligible bool
	}{
		{"unknown outside", false, r2RetireFacts{}, "after_nas_cleanup", false},
		{"present outside", false, r2RetireFacts{NASCopy: true}, "after_nas_cleanup", false},
		{"verified copy outside", false, r2RetireFacts{ExactNAS: true}, "after_nas_cleanup", false},
		{"completed cleanup", false, r2RetireFacts{NASAbsent: true}, r2RetireReason, true},
		{"kept missing", true, r2RetireFacts{}, "copy_to_nas", false},
		{"kept exact", true, r2RetireFacts{ExactNAS: true}, r2RetireNASReason, true},
		{"held", false, r2RetireFacts{NASAbsent: true, DeliveryHeld: true}, "delivery_keep_hold", false},
		{"reserved", false, r2RetireFacts{NASAbsent: true, Held: true}, "nas_cleanup_hold", false},
		{"referenced", false, r2RetireFacts{NASAbsent: true, JoinedRef: true}, "joined_reference", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := base
			c.Facts = tc.facts
			if tc.inside {
				c.StartAt = w.Start
				c.EndAt = w.Start.Add(time.Minute)
				c.SourceStart = c.StartAt
				c.SourceEnd = c.EndAt
			}
			for _, choice := range []string{"none", "keep", "st", ""} {
				picks := map[int64]r2RetirePick{3: {Choice: choice}}
				reason, _, eligible := r2RetireDecideForScope(c, scope, windows, picks)
				if reason != tc.reason || eligible != tc.eligible {
					t.Fatal(reason, eligible, tc)
				}
			}
			if _, _, eligible := r2RetireDecideForScope(c, scope, windows, map[int64]r2RetirePick{3: {Choice: "all"}}); eligible {
				t.Fatal("all pick lost with stale/missing window")
			}
		})
	}
}
func TestR2RetireLegacyAndPreviewManifestsUnexecutable(t *testing.T) {
	for _, tc := range []struct {
		version int
		sha     string
		preview bool
	}{{1, r2tSHA, false}, {2, "", false}, {2, r2tSHA, true}} {
		path := filepath.Join(t.TempDir(), "manifest.jsonl")
		h := r2RetireHeader{Version: tc.version, DeliveryKeepSHA256: tc.sha, PreviewOnly: tc.preview, Reason: r2RetireReason}
		if _, err := writeR2RetireManifest(path, h, nil); err != nil {
			t.Fatal(err)
		}
		if _, _, _, err := readR2RetireManifest(path); err == nil {
			t.Fatal("legacy/unchecked/preview accepted", tc)
		}
	}
}
func TestR2RetireDeliveryPreviewExactIdentity(t *testing.T) {
	snapshot := r2tDelivery()
	path := filepath.Join(t.TempDir(), "keep.jsonl")
	raw := []byte("{\"key\":\"test/delivery-hold\",\"etag\":\"test-etag\",\"size\":5}\n")
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	// Malformed digests never produce preview authority.
	if err := r2RetireDeliveryPreview(path, strings.Repeat("0", 64), snapshot); err == nil {
		t.Fatal("bad checksum accepted")
	}
}

func TestR2RetireDeliveryClassCannotReleaseHolds(t *testing.T) {
	ctx := context.Background()
	picks := r2RetirePicks{SHA256: r2tSHA}
	if _, err := r2HoldReleasePlan(ctx, nil, picks, r2tPolicy, nil, r2RetireDeliveryOnly); err == nil {
		t.Fatal("planning-only class created hold-release authority")
	}
	m := r2HoldReleaseManifest{Class: r2RetireDeliveryOnly, Version: 1, Tool: "r2-retire release-holds", PolicySHA: r2tPolicy, PicksSHA: r2tSHA, CreatedAt: time.Now().UTC()}
	if err := applyR2HoldReleases(ctx, nil, m, picks, r2tPolicy, r2tSHA, "reviewed", nil); err == nil {
		t.Fatal("planning-only class applied hold releases")
	}
}
