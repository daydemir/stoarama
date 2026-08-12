package api

import (
	"testing"
	"time"
)

func TestNormalizeCandidateRecordingIDs(t *testing.T) {
	got, err := normalizeCandidateRecordingIDs([]int64{9, 2, 5})
	if err != nil || got[0] != 2 || got[1] != 5 || got[2] != 9 {
		t.Fatalf("got=%v err=%v", got, err)
	}
	for _, bad := range [][]int64{nil, {0}, {1, 1}} {
		if _, err := normalizeCandidateRecordingIDs(bad); err == nil {
			t.Fatalf("accepted %v", bad)
		}
	}
}

func TestCandidateDigestBindsEvidence(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 4, time.UTC)
	x := nasCleanupCandidateItem{ClipID: 1, RecordingID: 2, Start: now, End: now.Add(time.Minute), Path: "r/c.mp4", Size: 3, SHA: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Verified: now, MTime: 1, CTime: 2, Inode: 3, Device: 4, SidecarPath: "r/c.mp4.stoarama.json", SidecarSize: 5, SidecarSHA: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", DestinationID: 6, Endpoint: "https://example.invalid", Bucket: "bucket", ObjectKey: "key", ETag: "etag"}
	a := candidateDigest(7, 8, []int64{2}, "gen", "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd", now, now, []nasCleanupCandidateItem{x})
	x.Inode++
	b := candidateDigest(7, 8, []int64{2}, "gen", "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd", now, now, []nasCleanupCandidateItem{x})
	if len(a) != 64 || a == b {
		t.Fatalf("digest did not bind evidence: %q %q", a, b)
	}
}
