package api

import (
	"strings"
	"testing"
	"time"
)

func TestNormalizeNodeTypeLocalRecorder(t *testing.T) {
	got, ok := normalizeNodeType("local_recorder")
	if !ok {
		t.Fatalf("normalizeNodeType(local_recorder) ok=false")
	}
	if got != nodeTypeLocalRecorder {
		t.Fatalf("normalizeNodeType(local_recorder)=%q want %q", got, nodeTypeLocalRecorder)
	}
}

func TestNodeTokenAllowedStatusesIncludesDisabled(t *testing.T) {
	got := nodeTokenAllowedStatuses()
	want := map[string]bool{"active": true, "disabled": true}
	if len(got) != len(want) {
		t.Fatalf("allowed statuses=%v want active+disabled", got)
	}
	for _, status := range got {
		if !want[status] {
			t.Fatalf("unexpected allowed status %q in %v", status, got)
		}
		delete(want, status)
	}
	if len(want) != 0 {
		t.Fatalf("missing allowed statuses: %v", want)
	}
}

func TestValidateOfflineDiagnostics(t *testing.T) {
	now := time.Now().UTC()
	valid := map[string]any{
		offlineDiagnosticsKey: []map[string]any{{
			"kind":           "heartbeat_outage",
			"error_class":    "dns_failed",
			"started_at":     now.Add(-time.Minute).Format(time.RFC3339Nano),
			"last_failed_at": now.Add(-30 * time.Second).Format(time.RFC3339Nano),
			"recovered_at":   now.Format(time.RFC3339Nano),
			"failure_count":  2,
		}},
	}
	if err := validateOfflineDiagnostics(valid); err != nil {
		t.Fatalf("valid diagnostics: %v", err)
	}
	if err := validateOfflineDiagnostics(map[string]any{}); err != nil {
		t.Fatalf("omitted diagnostics: %v", err)
	}

	tooMany := make([]map[string]any, offlineDiagnosticsMaxEvents+1)
	for i := range tooMany {
		tooMany[i] = valid[offlineDiagnosticsKey].([]map[string]any)[0]
	}
	if err := validateOfflineDiagnostics(map[string]any{offlineDiagnosticsKey: tooMany}); err == nil {
		t.Fatal("too many diagnostics accepted")
	}

	invalid := valid[offlineDiagnosticsKey].([]map[string]any)[0]
	invalid["error_class"] = "raw_error"
	if err := validateOfflineDiagnostics(map[string]any{offlineDiagnosticsKey: []map[string]any{invalid}}); err == nil {
		t.Fatal("invalid error class accepted")
	}
	invalid["error_class"] = "dns_failed"
	invalid["raw_error"] = "must never be stored"
	if err := validateOfflineDiagnostics(map[string]any{offlineDiagnosticsKey: []map[string]any{invalid}}); err == nil {
		t.Fatal("unknown diagnostic field accepted")
	}

	oversize := strings.Repeat("x", offlineDiagnosticsMaxBytes)
	if err := validateOfflineDiagnostics(map[string]any{offlineDiagnosticsKey: oversize}); err == nil {
		t.Fatal("oversize diagnostics accepted")
	}

	capabilities := map[string]any{offlineDiagnosticsKey: oversize, "active_jobs": 6}
	dropInvalidOfflineDiagnostics(capabilities)
	if _, ok := capabilities[offlineDiagnosticsKey]; ok {
		t.Fatal("invalid optional diagnostics retained")
	}
	if capabilities["active_jobs"] != 6 {
		t.Fatal("unrelated heartbeat capability changed")
	}
}

func TestDecideNodeEnroll(t *testing.T) {
	tests := []struct {
		name       string
		matches    []nodeNameMatch
		nodeType   string
		wantAction nodeEnrollAction
		wantID     int64
	}{
		{
			name:       "no matches inserts new",
			matches:    nil,
			nodeType:   nodeTypeRelay,
			wantAction: nodeEnrollInsert,
		},
		{
			name:       "disabled same type reactivates",
			matches:    []nodeNameMatch{{ID: 7, NodeType: nodeTypeRelay, Status: "disabled"}},
			nodeType:   nodeTypeRelay,
			wantAction: nodeEnrollReactivate,
			wantID:     7,
		},
		{
			name:       "active same name conflicts",
			matches:    []nodeNameMatch{{ID: 3, NodeType: nodeTypeRelay, Status: "active"}},
			nodeType:   nodeTypeRelay,
			wantAction: nodeEnrollConflict,
		},
		{
			name:       "active wins over disabled -> conflict",
			matches:    []nodeNameMatch{{ID: 7, NodeType: nodeTypeRelay, Status: "disabled"}, {ID: 3, NodeType: nodeTypeRelay, Status: "active"}},
			nodeType:   nodeTypeRelay,
			wantAction: nodeEnrollConflict,
		},
		{
			name:       "disabled different type inserts new",
			matches:    []nodeNameMatch{{ID: 9, NodeType: nodeTypeInferenceNode, Status: "disabled"}},
			nodeType:   nodeTypeRelay,
			wantAction: nodeEnrollInsert,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotAction, gotID := decideNodeEnroll(tc.matches, tc.nodeType)
			if gotAction != tc.wantAction {
				t.Fatalf("decideNodeEnroll action=%d want %d", gotAction, tc.wantAction)
			}
			if gotAction == nodeEnrollReactivate && gotID != tc.wantID {
				t.Fatalf("decideNodeEnroll id=%d want %d", gotID, tc.wantID)
			}
		})
	}
}
