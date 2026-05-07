package korea

import (
	"testing"
	"time"
)

func TestBuildInventoryGroupsKnownFamilies(t *testing.T) {
	inv := BuildInventory([]StreamRecord{
		{
			ID:             1,
			Provider:       "NAVER_MAP",
			Name:           "Sejong-daero",
			Slug:           "sejong-daero",
			SourceURL:      "https://cctvsecn01.ktict.co.kr:8082/koroad01306/L010273/playlist.m3u8",
			SourcePageURL:  "https://map.naver.com/p",
			SourceHost:     "cctvsecn01.ktict.co.kr",
			SourcePageHost: "map.naver.com",
		},
		{
			ID:             2,
			Provider:       "TOPIS",
			Name:           "Seoul Plaza",
			Slug:           "seoul-plaza",
			SourceURL:      "https://topiscctv1.eseoul.go.kr/live/cctv1/playlist.m3u8",
			SourcePageURL:  "https://topis.seoul.go.kr/openEngIntro.do",
			SourceHost:     "topiscctv1.eseoul.go.kr",
			SourcePageHost: "topis.seoul.go.kr",
		},
		{
			ID:             3,
			Provider:       "SPATIC",
			Name:           "Gwanghwamun",
			Slug:           "gwanghwamun",
			SourceURL:      "https://strm1.spatic.go.kr:443/live/1.stream/playlist.m3u8",
			SourcePageURL:  "https://www.spatic.go.kr/spatic/main/index.do",
			SourceHost:     "strm1.spatic.go.kr",
			SourcePageHost: "spatic.go.kr",
		},
		{
			ID:             4,
			Provider:       "KBS",
			Name:           "Seoul Station",
			Slug:           "seoul-station",
			SourceURL:      "https://kbsapi.loomex.net/v1/api/cctvRequest/123/x!hls",
			SourcePageURL:  "https://d.kbs.co.kr/special/cctv",
			SourceHost:     "kbsapi.loomex.net",
			SourcePageHost: "d.kbs.co.kr",
		},
		{
			ID:             5,
			Provider:       "GIGAEYES",
			Name:           "Seokchon Lake",
			Slug:           "seokchon-lake",
			SourceURL:      "https://www.youtube.com/watch?v=abc123",
			SourcePageURL:  "https://www.youtube.com/@GiGAeyesLiveTV",
			SourceHost:     "youtube.com",
			SourcePageHost: "youtube.com",
		},
		{
			ID:             6,
			Provider:       "YOUTUBE",
			Name:           "Generic Seoul Walk",
			Slug:           "generic-seoul-walk",
			SourceURL:      "https://www.youtube.com/watch?v=def456",
			SourcePageURL:  "https://www.youtube.com/watch?v=def456",
			SourceHost:     "youtube.com",
			SourcePageHost: "youtube.com",
		},
	}, time.Date(2026, 5, 7, 0, 0, 0, 0, time.UTC))

	if !inv.Summary.Complete {
		t.Fatalf("summary complete=false want true: %#v", inv.Summary)
	}
	if got, want := inv.Summary.TotalFamilies, 5; got != want {
		t.Fatalf("total_families=%d want=%d", got, want)
	}
	if got, want := inv.Summary.ResolvedStreams, 5; got != want {
		t.Fatalf("resolved_streams=%d want=%d", got, want)
	}
	if got, want := inv.Summary.UnresolvedStreams, 0; got != want {
		t.Fatalf("unresolved_streams=%d want=%d", got, want)
	}
	if got, want := inv.Summary.CoveredFamilies, 5; got != want {
		t.Fatalf("covered_families=%d want=%d", got, want)
	}
	utic := inv.Families[0]
	if utic.Family != FamilyUTIC || utic.StreamCount != 1 || len(utic.Entrypoints) != 1 {
		t.Fatalf("unexpected UTIC family: %#v", utic)
	}
	if utic.Entrypoints[0].Entrypoint != EntrypointNaverMap {
		t.Fatalf("entrypoint=%q want %q", utic.Entrypoints[0].Entrypoint, EntrypointNaverMap)
	}
}

func TestBuildInventorySurfacesUnresolvedCandidate(t *testing.T) {
	inv := BuildInventory([]StreamRecord{
		{
			ID:             1,
			Provider:       "NAVER_MAP",
			Name:           "Broken source",
			Slug:           "broken-source",
			SourceURL:      "https://example.com/live.m3u8",
			SourcePageURL:  "https://map.naver.com/p",
			SourceHost:     "example.com",
			SourcePageHost: "map.naver.com",
		},
	}, time.Date(2026, 5, 7, 0, 0, 0, 0, time.UTC))

	if inv.Summary.Complete {
		t.Fatalf("summary complete=true want false")
	}
	if got, want := inv.Summary.UnresolvedStreams, 1; got != want {
		t.Fatalf("unresolved_streams=%d want=%d", got, want)
	}
	if len(inv.Unresolved) != 1 {
		t.Fatalf("unresolved len=%d want=1", len(inv.Unresolved))
	}
	if inv.Unresolved[0].Reason == "" {
		t.Fatal("unresolved reason is empty")
	}
}
