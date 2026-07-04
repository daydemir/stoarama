package api

import "testing"

// A relay principal's canonical lease_owner is the server-derived 'node:{id}', never
// the display name, so relay ownership can neither collide with a user-chosen name nor
// be spoofed by client input.
func TestRecorderWorkerIDRelayUsesNodeID(t *testing.T) {
	got := recorderWorkerID(nodePrincipal{NodeID: 42, NodeType: nodeTypeRelay, DisplayName: "MacBook Pro"})
	if got != "node:42" {
		t.Fatalf("recorderWorkerID(relay)=%q want %q", got, "node:42")
	}
}

// A cloud droplet principal keeps the trimmed display name byte-identical to before
// the relay branch existed, so droplet lease/complete/ingest ownership is unchanged.
func TestRecorderWorkerIDDropletUsesDisplayName(t *testing.T) {
	got := recorderWorkerID(nodePrincipal{NodeID: 7, NodeType: nodeTypeLocalRecorder, DisplayName: "  stoarama-rec-123-0  "})
	if got != "stoarama-rec-123-0" {
		t.Fatalf("recorderWorkerID(local_recorder)=%q want %q", got, "stoarama-rec-123-0")
	}
}

// normalizeNodeType must accept 'relay' (the new recorder node type) alongside the
// existing types, so relay enrollment and requireRecorderNodeAuth admit it.
func TestNormalizeNodeTypeRelay(t *testing.T) {
	got, ok := normalizeNodeType("relay")
	if !ok {
		t.Fatalf("normalizeNodeType(relay) ok=false")
	}
	if got != nodeTypeRelay {
		t.Fatalf("normalizeNodeType(relay)=%q want %q", got, nodeTypeRelay)
	}
	if _, ok := normalizeNodeType("droplet"); ok {
		t.Fatalf("normalizeNodeType(droplet) ok=true, want false")
	}
}

func TestNormalizeCaptureVia(t *testing.T) {
	cases := []struct {
		in     string
		want   string
		wantOK bool
	}{
		{"", "cloud", true},
		{"cloud", "cloud", true},
		{"relay", "relay", true},
		{"  relay  ", "relay", true},
		{"CLOUD", "", false},
		{"other", "", false},
	}
	for _, c := range cases {
		got, ok := normalizeCaptureVia(c.in)
		if got != c.want || ok != c.wantOK {
			t.Fatalf("normalizeCaptureVia(%q)=(%q,%v) want (%q,%v)", c.in, got, ok, c.want, c.wantOK)
		}
	}
}

func TestIsReservedNodeDisplayName(t *testing.T) {
	if !isReservedNodeDisplayName("node:1") {
		t.Fatalf("isReservedNodeDisplayName(node:1)=false want true")
	}
	if !isReservedNodeDisplayName("  node:99") {
		t.Fatalf("isReservedNodeDisplayName(leading-space node:99)=false want true")
	}
	if isReservedNodeDisplayName("MacBook Pro") {
		t.Fatalf("isReservedNodeDisplayName(MacBook Pro)=true want false")
	}
	if isReservedNodeDisplayName("stoarama-rec-1-0") {
		t.Fatalf("isReservedNodeDisplayName(droplet name)=true want false")
	}
}
