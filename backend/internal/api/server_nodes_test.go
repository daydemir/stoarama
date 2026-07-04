package api

import "testing"

func TestNormalizeNodeTypeLocalRecorder(t *testing.T) {
	got, ok := normalizeNodeType("local_recorder")
	if !ok {
		t.Fatalf("normalizeNodeType(local_recorder) ok=false")
	}
	if got != nodeTypeLocalRecorder {
		t.Fatalf("normalizeNodeType(local_recorder)=%q want %q", got, nodeTypeLocalRecorder)
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
