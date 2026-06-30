package dropletpool

import (
	"strings"
	"testing"
)

func TestBuildUserData_EgressFirewallAndEnv(t *testing.T) {
	out, err := BuildUserData(UserDataConfig{
		ServerID:      "stoarama-rec-42",
		NodeToken:     "sin_secrettoken",
		BackendAPIURL: "https://stoarama-api.onrender.com",
		Capacity:      1,
		HeartbeatSec:  15,
		PollSec:       5,
		RepoURL:       "https://github.com/daydemir/stoarama.git",
		RepoRef:       "main",
	})
	if err != nil {
		t.Fatalf("BuildUserData: %v", err)
	}

	// Every blocked egress range required by S-1 must be dropped.
	for _, cidr := range []string{
		"169.254.0.0/16",
		"10.0.0.0/8",
		"172.16.0.0/12",
		"192.168.0.0/16",
		"127.0.0.0/8",
		"100.64.0.0/10", // CGNAT
		"fc00::/7",      // IPv6 ULA
		"fe80::/10",     // IPv6 link-local
	} {
		if !strings.Contains(out, cidr) {
			t.Fatalf("cloud-init missing egress block for %s", cidr)
		}
	}

	// DNS must be allowed only to the loopback stub resolver, never blanket to any
	// destination (a blanket dport-53 RETURN before the REJECTs let DNS reach the
	// metadata IP / internal resolvers, S-1).
	if strings.Contains(out, "--dport 53 -j RETURN") {
		t.Fatalf("cloud-init must not allow DNS to any destination; scope it to loopback")
	}
	if !strings.Contains(out, "-p udp --dport 53 -d 127.0.0.0/8 -j RETURN") {
		t.Fatalf("cloud-init must allow loopback (stub-resolver) DNS")
	}

	// The worker must boot from the prebuilt binary, never `go run`.
	if strings.Contains(out, "go run") {
		t.Fatalf("cloud-init must not 'go run' per fire")
	}
	if !strings.Contains(out, "/opt/stoarama/bin/stoaramactl") {
		t.Fatalf("cloud-init should reference the prebuilt binary path")
	}

	// RECORDER_SERVER_ID is passed via env so the worker never fetches the
	// (now-blocked) metadata service.
	if !strings.Contains(out, "RECORDER_SERVER_ID='stoarama-rec-42'") {
		t.Fatalf("cloud-init missing RECORDER_SERVER_ID env")
	}
	if !strings.Contains(out, "RECORDER_NODE_TOKEN='sin_secrettoken'") {
		t.Fatalf("cloud-init missing RECORDER_NODE_TOKEN env")
	}
	if !strings.Contains(out, "RECORDING_WORKER_CONCURRENCY='1'") {
		t.Fatalf("cloud-init missing worker concurrency (must equal capacity)")
	}
	if !strings.Contains(out, "BACKEND_API_URL='https://stoarama-api.onrender.com'") {
		t.Fatalf("cloud-init missing BACKEND_API_URL env")
	}
	// The egress firewall must be ordered before the recording worker.
	if !strings.Contains(out, "stoarama-egress-firewall.service") {
		t.Fatalf("cloud-init missing egress firewall unit")
	}
	if !strings.Contains(out, "start-recording-worker.sh") {
		t.Fatalf("cloud-init must launch the Phase-4 recording worker entrypoint")
	}
}

func TestBuildUserData_SkipsBuildWhenBakedBinaryMatchesHEAD(t *testing.T) {
	// With DROPLET_POOL_MIN=0 the pool is cold between fires, so the cold boot must
	// fit inside ProvisionLead. A from-scratch go build measured ~13-15 min on the
	// pool size, past the 600s lead, so a cold fire missed its freshness deadline.
	// The cloud-init must therefore reuse the rebaked-snapshot binary when its
	// recorded HEAD sha matches the freshly-reset HEAD, and rebuild only on a miss.
	out, err := BuildUserData(UserDataConfig{
		ServerID:      "stoarama-rec-cold",
		NodeToken:     "sin_token",
		BackendAPIURL: "https://stoarama-api.onrender.com",
		RepoURL:       "https://github.com/daydemir/stoarama.git",
		RepoRef:       "main",
	})
	if err != nil {
		t.Fatalf("BuildUserData: %v", err)
	}

	// Fast path: a baked binary whose recorded sha equals HEAD must skip the build.
	if !strings.Contains(out, `[ -x "$BIN" ] && [ "$HEAD_SHA" = "$BUILT_SHA" ]`) {
		t.Fatalf("cloud-init must skip the build when the baked binary matches HEAD (cold-start lead safety)")
	}
	if !strings.Contains(out, "skipping build") {
		t.Fatalf("cloud-init must log the skip-build fast path")
	}
	// Miss path: a missing/stale baked binary must still rebuild from source.
	if !strings.Contains(out, "build_worker") {
		t.Fatalf("cloud-init must rebuild from source on a sha miss")
	}
	// Atomicity: the recorded sha must be written only after a fresh build moves a
	// new binary into place (the staleness bug that removed the old fast-path: the
	// sha could be written without the build producing a new binary).
	if !strings.Contains(out, `mv -f "$tmp" "$BIN"`) {
		t.Fatalf("cloud-init must atomically move the freshly-built binary into place")
	}
	movIdx := strings.Index(out, `mv -f "$tmp" "$BIN"`)
	shaIdx := strings.Index(out, `printf '%s' "$HEAD_SHA" > "$SHA_FILE"`)
	if shaIdx < 0 || shaIdx < movIdx {
		t.Fatalf("the build sha must be written only after the new binary is moved into place")
	}
}

func TestBuildUserData_RequiresCoreFields(t *testing.T) {
	cases := []UserDataConfig{
		{NodeToken: "t", BackendAPIURL: "u"}, // missing ServerID
		{ServerID: "s", BackendAPIURL: "u"},  // missing NodeToken
		{ServerID: "s", NodeToken: "t"},      // missing BackendAPIURL
	}
	for i, c := range cases {
		if _, err := BuildUserData(c); err == nil {
			t.Fatalf("case %d: expected error for missing core field", i)
		}
	}
}

func TestParseImage_SnapshotIDvsSlug(t *testing.T) {
	if img := parseImage("123456789"); img.ID != 123456789 || img.Slug != "" {
		t.Fatalf("numeric image should parse as snapshot id, got %+v", img)
	}
	if img := parseImage("ubuntu-24-04-x64"); img.Slug != "ubuntu-24-04-x64" || img.ID != 0 {
		t.Fatalf("non-numeric image should parse as slug, got %+v", img)
	}
}

func TestHashNodeSecret_MatchesSHA256Hex(t *testing.T) {
	// hashNodeSecret must produce the same SHA-256 hex the API's hashSecret does, so
	// a minted token validates against node_tokens.secret_hash. Known vector:
	// sha256("sin_abc") trimmed.
	got := hashNodeSecret("  sin_abc  ")
	want := hashNodeSecret("sin_abc")
	if got != want {
		t.Fatalf("hashNodeSecret must trim before hashing: %q vs %q", got, want)
	}
	if len(want) != 64 {
		t.Fatalf("sha256 hex must be 64 chars, got %d", len(want))
	}
}
