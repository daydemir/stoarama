package api

import "testing"

func TestNormalizeServerID(t *testing.T) {
	if got, want := normalizeServerID("Mini-A.Local"), "mini-a"; got != want {
		t.Fatalf("normalizeServerID=%q want %q", got, want)
	}
	if got := normalizeServerID("   "); got != "" {
		t.Fatalf("normalizeServerID blank=%q want empty", got)
	}
}

func TestDeriveServerID(t *testing.T) {
	tests := []struct {
		name     string
		workerID string
		metadata map[string]any
		want     string
	}{
		{
			name:     "metadata server id wins",
			workerID: "inferctl:mini-a",
			metadata: map[string]any{"server_id": "Do-555140997"},
			want:     "do-555140997",
		},
		{
			name:     "metadata host fallback",
			workerID: "inferctl:mini-a",
			metadata: map[string]any{"host": "Mini-A.local"},
			want:     "mini-a",
		},
		{
			name:     "inferctl worker host",
			workerID: "inferctl:mbp64:s1:w0",
			metadata: nil,
			want:     "mbp64",
		},
		{
			name:     "render prefix maps render",
			workerID: "render-capture-worker-1",
			metadata: nil,
			want:     "render",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := deriveServerID(tc.workerID, tc.metadata); got != tc.want {
				t.Fatalf("deriveServerID=%q want %q", got, tc.want)
			}
		})
	}
}

func TestRegisterWorkerServerHintsAliasesClaimedBy(t *testing.T) {
	hints := map[string]string{}
	key := registerWorkerServerHints(hints, "inferctl:mini-a", map[string]any{
		"server_id":  "mini-a",
		"claimed_by": "inferctl:mini-a:s0:w1",
	})
	if got, want := key, "mini-a"; got != want {
		t.Fatalf("register key=%q want %q", got, want)
	}
	if got, ok := hints["inferctl:mini-a"]; !ok || got != "mini-a" {
		t.Fatalf("expected worker alias mapped to mini-a, got=%q ok=%v", got, ok)
	}
	if got, ok := hints["inferctl:mini-a:s0:w1"]; !ok || got != "mini-a" {
		t.Fatalf("expected claimed_by alias mapped to mini-a, got=%q ok=%v", got, ok)
	}
}
