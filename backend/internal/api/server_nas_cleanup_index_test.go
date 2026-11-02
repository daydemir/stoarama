package api

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
)

func cleanupIndexTestBlob(t *testing.T, entries []cleanupFrozenIndexedEntry, parent string) (cleanupFrozenShard, []byte) {
	t.Helper()
	plain, _ := json.Marshal(map[string]any{"parent": parent, "entries": entries})
	var b bytes.Buffer
	z := gzip.NewWriter(&b)
	z.Write(plain)
	z.Close()
	raw := b.Bytes()
	sha := cleanupSHA(raw)
	return cleanupFrozenShard{Key: cleanupFrozenIndexPrefix + sha + "/files.json.gz", SHA: sha, Bytes: int64(len(raw)), Entries: len(entries)}, raw
}
func TestNASCleanupIndexedFrozenStatBindingAndFailClosed(t *testing.T) {
	base := []cleanupFrozenIndexedEntry{
		{Path: "a/b.mp4", Kind: "file", Links: 1, Frozen: [5]int64{3, 11, 12, 13, 14}},
		{Path: "a/b.mp4.stoarama.json", Kind: "file", Links: 1, Frozen: [5]int64{100, 21, 22, 23, 14}},
		{Path: "a/unlisted", Kind: "file", Links: 1, Frozen: [5]int64{99, 1, 2, 3, 14}},
	}
	for _, tc := range []struct {
		name   string
		modify func([]cleanupFrozenIndexedEntry) []cleanupFrozenIndexedEntry
		bad    bool
	}{
		{"exact", func(e []cleanupFrozenIndexedEntry) []cleanupFrozenIndexedEntry { return e }, false},
		{"hardlinked", func(e []cleanupFrozenIndexedEntry) []cleanupFrozenIndexedEntry { e[0].Links = 2; return e }, true},
		{"error", func(e []cleanupFrozenIndexedEntry) []cleanupFrozenIndexedEntry { e[0].Error = "unreadable"; return e }, true},
		{"raw_bytes", func(e []cleanupFrozenIndexedEntry) []cleanupFrozenIndexedEntry { e[0].RawBytes = "AA=="; return e }, true},
		{"symlink", func(e []cleanupFrozenIndexedEntry) []cleanupFrozenIndexedEntry { e[0].Kind = "other"; return e }, true},
		{"size", func(e []cleanupFrozenIndexedEntry) []cleanupFrozenIndexedEntry { e[0].Frozen[0] = 4; return e }, true},
		{"missing_sidecar", func(e []cleanupFrozenIndexedEntry) []cleanupFrozenIndexedEntry { return e[:1] }, true},
		{"oversized_sidecar", func(e []cleanupFrozenIndexedEntry) []cleanupFrozenIndexedEntry { e[1].Frozen[0] = 2 << 20; return e }, true},
		{"duplicate", func(e []cleanupFrozenIndexedEntry) []cleanupFrozenIndexedEntry { return append(e, e[0]) }, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			entries := tc.modify(append([]cleanupFrozenIndexedEntry(nil), base...))
			ref, raw := cleanupIndexTestBlob(t, entries, "a")
			index := &cleanupFrozenIndex{Parents: map[string]cleanupFrozenParent{"a": {Owner: 3, Shards: []cleanupFrozenShard{ref}, Entries: len(entries)}}}
			items := []nasCleanupItem{{RelativePath: "a/b.mp4", SizeBytes: 3, StreamID: 3, SidecarRequired: true}}
			var mu sync.Mutex
			reads := 0
			err := bindCleanupIndexedItems(context.Background(), items, index, func(_ context.Context, key string, _ int64) ([]byte, error) {
				mu.Lock()
				defer mu.Unlock()
				reads++
				if key != ref.Key {
					t.Error("read outside required shard")
				}
				return raw, nil
			})
			if (err != nil) != tc.bad {
				t.Fatalf("unexpected bind outcome: %v", err)
			}
			if tc.bad {
				if items[0].Frozen != [5]int64{} {
					t.Fatal("failed proof partially assigned")
				}
			} else {
				if items[0].Frozen != base[0].Frozen || items[0].FrozenSidecar != base[1].Frozen {
					t.Fatal("exact frozen identities changed")
				}
				if reads != 1 {
					t.Fatal("admission re-read unrelated inventory")
				}
			}
		})
	}
}
func TestNASCleanupIndexRejectsCorruptionProvenanceAndCrossParent(t *testing.T) {
	index := cleanupFrozenIndex{Schema: "nas-cleanup-frozen-index-v1", AuditSHA: cleanupFrozenAuditSHA, AuditID: "6cd48108-ca05-4df7-988b-0bd7e4d320fe", AuditBytes: 88667812, ReportSHA: nasCleanupReportSHA, Parents: map[string]cleanupFrozenParent{}}
	index.Totals.Entries = 2943864
	var report nasCleanupReport
	raw, _ := json.Marshal(map[string]any{"complete_node_actions": []map[string]any{{"path": "a", "direct_files_action": "RETENTION_DELETE_CLEARED", "owner_stream_ids": []int64{3}}}})
	json.Unmarshal(raw, &report)
	ref, blob := cleanupIndexTestBlob(t, []cleanupFrozenIndexedEntry{{Path: "a/b", Kind: "file", Links: 1, Frozen: [5]int64{3, 1, 2, 3, 4}}}, "a")
	index.Parents["a"] = cleanupFrozenParent{Owner: 3, Shards: []cleanupFrozenShard{ref}, Entries: 1}
	if err := validateCleanupIndex(&index, report); err != nil {
		t.Fatal(err)
	}
	index.AuditSHA = strings.Repeat("a", 64)
	if validateCleanupIndex(&index, report) == nil {
		t.Fatal("unbound source audit accepted")
	}
	index.AuditSHA = cleanupFrozenAuditSHA
	items := []nasCleanupItem{{RelativePath: "a/b", SizeBytes: 3, StreamID: 3}}
	if bindCleanupIndexedItems(context.Background(), items, &index, func(context.Context, string, int64) ([]byte, error) {
		return append([]byte(nil), blob[:len(blob)-1]...), nil
	}) == nil {
		t.Fatal("truncated metadata accepted")
	}
	if bindCleanupIndexedItems(context.Background(), items, &index, func(context.Context, string, int64) ([]byte, error) { return nil, errors.New("unavailable") }) == nil {
		t.Fatal("unavailable shard accepted")
	}
	items[0].StreamID = 4
	if bindCleanupIndexedItems(context.Background(), items, &index, func(context.Context, string, int64) ([]byte, error) { t.Error("wrong owner read R2"); return blob, nil }) == nil {
		t.Fatal("cross owner accepted")
	}
}
func TestNASCleanupIndexRejectsCrossShardDuplicatesAndSidecarOverlap(t *testing.T) {
	entry := cleanupFrozenIndexedEntry{Path: "a/b.mp4", Kind: "file", Links: 1, Frozen: [5]int64{3, 1, 2, 3, 4}}
	ref, raw := cleanupIndexTestBlob(t, []cleanupFrozenIndexedEntry{entry}, "a")
	idx := &cleanupFrozenIndex{Parents: map[string]cleanupFrozenParent{"a": {Owner: 3, Shards: []cleanupFrozenShard{ref, ref}, Entries: 2}}}
	items := []nasCleanupItem{{RelativePath: entry.Path, SizeBytes: 3, StreamID: 3}}
	if bindCleanupIndexedItems(context.Background(), items, idx, func(context.Context, string, int64) ([]byte, error) { return raw, nil }) == nil {
		t.Fatal("duplicate across chunks accepted")
	}
	items = []nasCleanupItem{{RelativePath: "a/b.mp4.stoarama.json", SizeBytes: 100, StreamID: 3}, {RelativePath: "a/b.mp4", SizeBytes: 3, StreamID: 3, SidecarRequired: true}}
	if bindCleanupIndexedItems(context.Background(), items, idx, func(context.Context, string, int64) ([]byte, error) { t.Error("overlap reached R2"); return raw, nil }) == nil {
		t.Fatal("media/sidecar overlap accepted")
	}
}
