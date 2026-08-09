package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestParseNASInventoryArgs(t *testing.T) {
	for _, args := range [][]string{nil, {"list"}, {"report"}, {"report", "--connection-id", "0"}, {"report", "--connection-id", "1", "--limit", "0"}, {"report", "--connection-id", "1", "--limit", "501"}, {"report", "--connection-id", "7", "typo"}} {
		if _, err := parseNASInventoryArgs(args); err == nil {
			t.Errorf("accepted invalid args %v", args)
		}
	}
	opts, err := parseNASInventoryArgs([]string{"report", "--connection-id", "7", "--limit", "25", "--json"})
	if err != nil || opts.connectionID != 7 || opts.limit != 25 || !opts.asJSON {
		t.Fatalf("opts=%+v err=%v", opts, err)
	}
}

func TestWriteNASInventoryReportEmptyAndPopulated(t *testing.T) {
	for _, summary := range []nasInventorySummary{
		{ConnectionID: 1, Items: []map[string]any{}},
		{ConnectionID: 2, Label: "NAS", Mode: "observe", Items: []map[string]any{{"clip_id": int64(3), "state": "mismatch", "relative_path": "clips/3.mp4", "verified_at": nil}}},
	} {
		var jsonOut bytes.Buffer
		if err := writeNASInventoryReport(&jsonOut, summary, true); err != nil {
			t.Fatal(err)
		}
		var decoded nasInventorySummary
		if err := json.Unmarshal(jsonOut.Bytes(), &decoded); err != nil || decoded.ConnectionID != summary.ConnectionID {
			t.Fatalf("decoded=%+v err=%v", decoded, err)
		}
		var textOut bytes.Buffer
		if err := writeNASInventoryReport(&textOut, summary, false); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(textOut.String(), "connection=") {
			t.Fatalf("missing report header: %q", textOut.String())
		}
		for _, marker := range []string{"snapshot_available=", "snapshot_consistent=", "storage_total=", "last_batch_clips="} {
			if !strings.Contains(textOut.String(), marker) {
				t.Fatalf("missing status marker %q: %q", marker, textOut.String())
			}
		}
	}
}
