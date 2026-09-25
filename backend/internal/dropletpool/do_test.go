package dropletpool

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/digitalocean/godo"
)

func TestListDropletsPageRetriesOnlyTransientMTLSEdgeFailure(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempt := calls.Add(1)
		if attempt < doFleetReadAttempts {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = fmt.Fprintf(w, `{"message":"mTLS verification failed","request_id":"edge-%d"}`, attempt)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"droplets":[{"id":42,"name":"stoarama-rec-42","status":"active"}],"links":{},"meta":{}}`)
	}))
	defer server.Close()

	client := godo.NewClient(server.Client())
	baseURL, err := url.Parse(server.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	client.BaseURL = baseURL
	droplets, _, err := listDropletsPageWithRetry(context.Background(), client, &godo.ListOptions{PerPage: 200}, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != doFleetReadAttempts || len(droplets) != 1 || droplets[0].ID != 42 {
		t.Fatalf("calls=%d droplets=%+v want recovery on attempt %d", calls.Load(), droplets, doFleetReadAttempts)
	}
}

func TestListDropletsPageDoesNotRetryOrdinaryUnauthorized(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = fmt.Fprint(w, `{"message":"Unable to authenticate you","request_id":"auth-1"}`)
	}))
	defer server.Close()

	client := godo.NewClient(server.Client())
	baseURL, err := url.Parse(server.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	client.BaseURL = baseURL
	_, _, err = listDropletsPageWithRetry(context.Background(), client, &godo.ListOptions{}, time.Millisecond)
	if err == nil || calls.Load() != 1 {
		t.Fatalf("error=%v calls=%d want ordinary 401 fail closed without retry", err, calls.Load())
	}
}

func TestListDropletsPageExhaustsTransientMTLSEdgeRetries(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = fmt.Fprint(w, `{"message":"mTLS verification failed","request_id":"edge-final"}`)
	}))
	defer server.Close()

	client := godo.NewClient(server.Client())
	baseURL, err := url.Parse(server.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	client.BaseURL = baseURL
	_, _, err = listDropletsPageWithRetry(context.Background(), client, &godo.ListOptions{}, time.Millisecond)
	if err == nil || calls.Load() != doFleetReadAttempts {
		t.Fatalf("error=%v calls=%d want exact transient exhaustion after %d attempts", err, calls.Load(), doFleetReadAttempts)
	}
}

func TestListDropletsPageDoesNotRetryMTLSMessageOnOtherStatus(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_, _ = fmt.Fprint(w, `{"message":"mTLS verification failed","request_id":"not-401"}`)
	}))
	defer server.Close()

	client := godo.NewClient(server.Client())
	baseURL, err := url.Parse(server.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	client.BaseURL = baseURL
	_, _, err = listDropletsPageWithRetry(context.Background(), client, &godo.ListOptions{}, time.Millisecond)
	if err == nil || calls.Load() != 1 {
		t.Fatalf("error=%v calls=%d want non-401 fail closed without retry", err, calls.Load())
	}
}

func TestJitteredDOReadRetryDelayIsBounded(t *testing.T) {
	for attempt := 1; attempt <= 3; attempt++ {
		ceiling := time.Second * time.Duration(attempt)
		for range 100 {
			got := jitteredDOReadRetryDelay(time.Second, attempt)
			if got < ceiling/2 || got > ceiling {
				t.Fatalf("attempt=%d delay=%s want [%s,%s]", attempt, got, ceiling/2, ceiling)
			}
		}
	}
}

func TestFleetReadIncidentLatchesOnceAndRecovers(t *testing.T) {
	controller := &Controller{}
	started := time.Date(2026, 8, 3, 15, 0, 0, 0, time.UTC)
	if controller.noteFleetReadFailure(started) {
		t.Fatal("first fleet-read failure alerted before sustained threshold")
	}
	if alert, _, _, recovered := controller.noteFleetReadSuccess(started.Add(time.Minute)); alert || recovered {
		t.Fatal("one successful tick cleared or alerted an intermittent incident")
	}
	if controller.noteFleetReadFailure(started.Add(2 * time.Minute)) {
		t.Fatal("second intermittent failure alerted early")
	}
	if alert, _, _, recovered := controller.noteFleetReadSuccess(started.Add(3 * time.Minute)); alert || recovered {
		t.Fatal("intermediate success cleared incident before healthy dwell")
	}
	if controller.noteFleetReadFailure(started.Add(4 * time.Minute)) {
		t.Fatal("third intermittent failure alerted before span threshold")
	}
	alert, duration, failures, recovered := controller.noteFleetReadSuccess(started.Add(5 * time.Minute))
	if !alert || recovered || duration != 5*time.Minute || failures != 3 {
		t.Fatalf("threshold success alert=%t duration=%s failures=%d recovered=%t", alert, duration, failures, recovered)
	}
	if alert, _, _, recovered := controller.noteFleetReadSuccess(started.Add(8 * time.Minute)); alert || recovered {
		t.Fatal("incident duplicated alert or recovered before healthy dwell")
	}
	if alert, _, _, recovered := controller.noteFleetReadSuccess(started.Add(9 * time.Minute)); alert || recovered {
		t.Fatal("incident recovered before five continuous healthy minutes")
	}
	alert, duration, failures, recovered = controller.noteFleetReadSuccess(started.Add(10 * time.Minute))
	if alert || !recovered || duration != 10*time.Minute || failures != 3 {
		t.Fatalf("recovery alert=%t duration=%s failures=%d recovered=%t", alert, duration, failures, recovered)
	}
	if _, _, _, recovered := controller.noteFleetReadSuccess(started.Add(11 * time.Minute)); recovered {
		t.Fatal("fleet-read recovery emitted twice")
	}
	if controller.noteFleetReadFailure(started.Add(12 * time.Minute)) {
		t.Fatal("fresh incident inherited prior alert state")
	}
}

func TestFleetReadIncidentRequiresContinuousSuccessfulDwell(t *testing.T) {
	controller := &Controller{}
	started := time.Date(2026, 8, 3, 15, 0, 0, 0, time.UTC)
	controller.noteFleetReadFailure(started)

	if alert, _, _, recovered := controller.noteFleetReadSuccess(started.Add(10 * time.Minute)); alert || recovered {
		t.Fatal("a single delayed success recovered the incident")
	}
	if !controller.noteFleetReadFailure(started.Add(12 * time.Minute)) {
		t.Fatal("sustained interrupted incident did not alert")
	}
	if alert, _, _, recovered := controller.noteFleetReadSuccess(started.Add(17 * time.Minute)); alert || recovered {
		t.Fatal("first success after interruption recovered immediately or duplicated alert")
	}
	if alert, _, _, recovered := controller.noteFleetReadSuccess(started.Add(21*time.Minute + 59*time.Second)); alert || recovered {
		t.Fatal("incident recovered before restarted healthy dwell completed")
	}
	if alert, _, failures, recovered := controller.noteFleetReadSuccess(started.Add(22 * time.Minute)); alert || !recovered || failures != 2 {
		t.Fatalf("recovery alert=%t failures=%d recovered=%t", alert, failures, recovered)
	}
}

func TestFleetReadIncidentAlertsBeforeSameTickRecovery(t *testing.T) {
	controller := &Controller{}
	started := time.Date(2026, 8, 3, 15, 0, 0, 0, time.UTC)
	controller.noteFleetReadFailure(started)
	controller.noteFleetReadFailure(started.Add(time.Minute))
	controller.noteFleetReadSuccess(started.Add(2 * time.Minute))

	alert, duration, failures, recovered := controller.noteFleetReadSuccess(started.Add(7 * time.Minute))
	if !alert || !recovered || duration != 7*time.Minute || failures != 2 {
		t.Fatalf("alert=%t duration=%s failures=%d recovered=%t", alert, duration, failures, recovered)
	}
}

type failingFleetDO struct {
	lists   atomic.Int64
	creates atomic.Int64
	deletes atomic.Int64
	listErr error
}

func (f *failingFleetDO) ListDropletsByName(context.Context, string, string) ([]DODroplet, error) {
	f.lists.Add(1)
	if f.listErr != nil {
		return nil, f.listErr
	}
	return nil, errors.New("provider fleet unavailable")
}

func TestReconcileClassifiesOnlyProviderFleetReadFailure(t *testing.T) {
	providerErr := errors.New("provider fleet unavailable")
	controller := &Controller{do: &failingFleetDO{listErr: providerErr}}
	err := controller.reconcile(context.Background(), time.Now())
	if !errors.Is(err, errFleetRead) || !errors.Is(err, providerErr) {
		t.Fatalf("reconcile error=%v want fleet-read marker and provider cause", err)
	}
	storeErr := errors.New("store unavailable")
	if errors.Is(storeErr, errFleetRead) {
		t.Fatal("unmarked store error classified as fleet-read failure")
	}
}

func TestControllerTickPropagatesCancellationWithoutDegradationLatch(t *testing.T) {
	provider := &failingFleetDO{listErr: fmt.Errorf("list interrupted: %w", context.Canceled)}
	controller := &Controller{do: provider}
	err := controller.tick(context.Background())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("tick error=%v want wrapped cancellation", err)
	}
	if controller.fleetReadFailures != 0 || !controller.fleetReadFailureSince.IsZero() {
		t.Fatalf("cancellation latched as outage: failures=%d since=%s", controller.fleetReadFailures, controller.fleetReadFailureSince)
	}
}

func (f *failingFleetDO) CreateDroplet(context.Context, CreateDropletInput) (DODroplet, error) {
	f.creates.Add(1)
	return DODroplet{}, nil
}

func (f *failingFleetDO) DeleteDroplet(context.Context, int64) error {
	f.deletes.Add(1)
	return nil
}

func TestControllerTickMakesNoProviderMutationWithoutFreshFleet(t *testing.T) {
	provider := &failingFleetDO{}
	controller := &Controller{do: provider}
	if err := controller.tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if provider.lists.Load() != 1 || provider.creates.Load() != 0 || provider.deletes.Load() != 0 {
		t.Fatalf("lists=%d creates=%d deletes=%d want one read and zero mutations",
			provider.lists.Load(), provider.creates.Load(), provider.deletes.Load())
	}
}

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
		BuildSHA:      strings.Repeat("a", 40),
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

	// DNS must be allowed only to the loopback stub resolver (and its configured
	// upstreams, see TestBuildUserData_AllowsDNSToConfiguredUpstreamOnly), never blanket to any
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
	fetchIdx := strings.Index(out, "git -C /opt/stoarama fetch --depth 1 origin "+strings.Repeat("a", 40))
	checkoutIdx := strings.Index(out, "git -C /opt/stoarama checkout --detach FETCH_HEAD")
	headIdx := strings.Index(out, `printf "export RECORDER_BUILD_SHA='%s'\n" "$HEAD_SHA"`)
	if fetchIdx < 0 {
		t.Fatalf("cloud-init must fetch the controller's immutable build commit")
	}
	if checkoutIdx <= fetchIdx || headIdx <= checkoutIdx {
		t.Fatalf("cloud-init must fetch, check out, then report the immutable build commit")
	}
	if !strings.Contains(out, `printf "export RECORDER_BUILD_SHA='%s'\n" "$HEAD_SHA"`) {
		t.Fatalf("cloud-init must report the verified binary source commit")
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

// TestBuildUserData_AllowsDNSToConfiguredUpstreamOnly guards the 2026-09-25
// outage: new DO droplets got a private VPC upstream resolver (10.116.15.254)
// behind the systemd-resolved stub, the RFC1918 REJECT blocked it, nothing
// resolved, git fetch failed, and the worker never heartbeated. The firewall must
// allow port 53 to exactly the configured upstream resolvers (read from
// resolved's resolv.conf, persisted for reboots) before the private-range
// REJECTs, and still never allow blanket DNS or metadata/link-local upstreams.
func TestBuildUserData_AllowsDNSToConfiguredUpstreamOnly(t *testing.T) {
	out, err := BuildUserData(UserDataConfig{
		ServerID:      "stoarama-rec-dns",
		NodeToken:     "sin_token",
		BackendAPIURL: "https://stoarama-api.onrender.com",
		RepoURL:       "https://github.com/daydemir/stoarama.git",
		BuildSHA:      strings.Repeat("b", 40),
	})
	if err != nil {
		t.Fatalf("BuildUserData: %v", err)
	}
	script := extractEgressFirewallScript(t, out)

	if !strings.Contains(script, "/run/systemd/resolve/resolv.conf") {
		t.Fatalf("firewall must read the upstream resolvers from systemd-resolved's resolv.conf")
	}
	if !strings.Contains(script, `load_resolvers "$(cat "$UPSTREAM_FILE" 2>/dev/null || true)"`) {
		t.Fatalf("persisted upstreams must be the fallback when resolv.conf yields no eligible upstream")
	}
	if !strings.Contains(script, "UPSTREAM_FILE=/etc/stoarama/dns-upstreams") {
		t.Fatalf("firewall must persist upstream resolvers for early-boot reruns")
	}
	for _, rule := range []string{
		`echo "-A STOARAMA_EGRESS -p udp --dport 53 -d $ns/32 -j RETURN"`,
		`echo "-A STOARAMA_EGRESS -p tcp --dport 53 -d $ns/32 -j RETURN"`,
		`echo "-A STOARAMA_EGRESS -p udp --dport 53 -d $ns/128 -j RETURN"`,
		`echo "-A STOARAMA_EGRESS -p tcp --dport 53 -d $ns/128 -j RETURN"`,
	} {
		if !strings.Contains(script, rule) {
			t.Fatalf("firewall missing upstream DNS allowance %q", rule)
		}
	}
	// The upstream allowance must precede the private-range REJECTs, otherwise a
	// VPC resolver in 10.0.0.0/8 is still rejected.
	allowIdx := strings.Index(script, `-d $ns/32 -j RETURN`)
	rejectIdx := strings.Index(script, `echo "-A STOARAMA_EGRESS -d $cidr -j REJECT"`)
	if allowIdx < 0 || rejectIdx < 0 || allowIdx > rejectIdx {
		t.Fatalf("upstream DNS allowance must come before the private-range REJECTs")
	}
	// Metadata / link-local / loopback nameserver entries are never allowlisted.
	if !strings.Contains(script, `""|0.*|127.*|169.254.*|::|::1|fe[89ab]?:*) continue ;;`) || !strings.Contains(script, `tr 'A-F' 'a-f'`) {
		t.Fatalf("firewall must skip loopback and link-local/metadata nameserver entries")
	}
	if strings.Contains(script, "--dport 53 -j RETURN") {
		t.Fatalf("firewall must never allow DNS to any destination")
	}

	if bash, err := exec.LookPath("bash"); err == nil {
		cmd := exec.Command(bash, "-n")
		cmd.Stdin = strings.NewReader(script)
		if msg, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("firewall script does not parse: %v\n%s", err, msg)
		}
	}
}

// TestBuildUserData_RefreshesFirewallWhenDNSUpstreamChanges guards CodeRabbit's
// follow-up on #374: the DNS allowance is derived from resolved's upstreams at
// run time, so a later upstream change (DHCP renewal, resolved reconfiguration,
// or resolv.conf appearing after an early-boot run) must re-run the firewall.
func TestBuildUserData_RefreshesFirewallWhenDNSUpstreamChanges(t *testing.T) {
	out, err := BuildUserData(UserDataConfig{
		ServerID:      "stoarama-rec-dns-refresh",
		NodeToken:     "sin_token",
		BackendAPIURL: "https://stoarama-api.onrender.com",
		RepoURL:       "https://github.com/daydemir/stoarama.git",
		BuildSHA:      strings.Repeat("d", 40),
	})
	if err != nil {
		t.Fatalf("BuildUserData: %v", err)
	}
	for _, want := range []string{
		"path: /etc/systemd/system/stoarama-egress-firewall-refresh.path",
		"PathChanged=/run/systemd/resolve/resolv.conf",
		"Unit=stoarama-egress-firewall-refresh.service",
		"path: /etc/systemd/system/stoarama-egress-firewall-refresh.service",
		"systemctl enable --now stoarama-egress-firewall-refresh.path",
		// Refresh once right after arming the watcher, and once per boot after it.
		"  - systemctl enable --now stoarama-egress-firewall-refresh.path\n  - systemctl enable stoarama-egress-firewall-refresh.service\n  - systemctl start stoarama-egress-firewall-refresh.service\n",
		"After=stoarama-egress-firewall.service stoarama-egress-firewall-refresh.path systemd-resolved.service",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("cloud-init missing resolver-change refresh wiring %q", want)
		}
	}
	// The refresh unit must actually re-run the same firewall script on every
	// trigger; RemainAfterExit would make a second start a no-op.
	start := strings.Index(out, "path: /etc/systemd/system/stoarama-egress-firewall-refresh.service")
	end := strings.Index(out, "path: /etc/systemd/system/stoarama-egress-firewall-refresh.path")
	if start < 0 || end < start {
		t.Fatalf("refresh service must be rendered before its path unit")
	}
	refresh := out[start:end]
	if strings.Contains(refresh, "RemainAfterExit") || !strings.Contains(refresh, "ExecStart=/usr/local/sbin/stoarama-egress-firewall.sh") {
		t.Fatalf("refresh unit must re-run the firewall script on every trigger:\n%s", refresh)
	}
	if !strings.Contains(refresh, "WantedBy=multi-user.target") {
		t.Fatalf("refresh unit must also run once per boot:\n%s", refresh)
	}
	if !strings.Contains(extractEgressFirewallScript(t, out), "flock 9") {
		t.Fatalf("firewall runs must be serialized so boot and refresh cannot interleave")
	}
}

// firewallRun is the observable result of executing the rendered egress
// firewall script against stubbed iptables tooling.
type firewallRun struct {
	v4, v6    string // last iptables-restore / ip6tables-restore transaction
	v4Commits int
	persisted string
	output    string
	err       error
}

// firewallFixture sets up the droplet-side files the firewall script reads.
type firewallFixture struct {
	live         *string // resolv.conf contents; nil: absent (early boot)
	persisted    *string // /etc/stoarama/dns-upstreams; nil: absent
	onFirstApply string  // shell run once inside the first iptables-restore
	failRestore  bool    // iptables-restore rejects the transaction
}

func runEgressFirewall(t *testing.T, fx firewallFixture) firewallRun {
	t.Helper()
	bash, err := exec.LookPath("bash")
	if err != nil {
		t.Skip("bash not available")
	}
	out, err := BuildUserData(UserDataConfig{
		ServerID:      "stoarama-rec-dns-exec",
		NodeToken:     "sin_token",
		BackendAPIURL: "https://stoarama-api.onrender.com",
		RepoURL:       "https://github.com/daydemir/stoarama.git",
		BuildSHA:      strings.Repeat("e", 40),
	})
	if err != nil {
		t.Fatalf("BuildUserData: %v", err)
	}
	script := extractEgressFirewallScript(t, out)

	dir := t.TempDir()
	bin := filepath.Join(dir, "bin")
	stateDir := filepath.Join(dir, "etc")
	resolv := filepath.Join(dir, "resolv.conf")
	upstreamFile := filepath.Join(stateDir, "dns-upstreams")
	for _, d := range []string{bin, stateDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	write := func(path, body string, mode os.FileMode) {
		t.Helper()
		if err := os.WriteFile(path, []byte(body), mode); err != nil {
			t.Fatal(err)
		}
	}
	for _, fam := range []string{"iptables", "ip6tables"} {
		log := filepath.Join(dir, fam+".restore")
		write(filepath.Join(bin, fam), "#!/bin/sh\nexit 0\n", 0o755)
		write(filepath.Join(bin, fam+"-restore"), "#!/bin/sh\n"+
			"body=$(cat)\n"+
			"hook='"+filepath.Join(dir, "hook")+"'\n"+
			"if [ -f \"$hook\" ]; then sh \"$hook\"; rm -f \"$hook\"; fi\n"+
			"if [ -f '"+filepath.Join(dir, "fail")+"' ]; then exit 1; fi\n"+
			"printf '%s\\n@@COMMIT@@\\n' \"$body\" >> '"+log+"'\n", 0o755)
	}
	write(filepath.Join(bin, "flock"), "#!/bin/sh\nexit 0\n", 0o755)
	if fx.live != nil {
		write(resolv, *fx.live, 0o644)
	}
	if fx.persisted != nil {
		write(upstreamFile, *fx.persisted, 0o644)
	}
	if fx.onFirstApply != "" {
		write(filepath.Join(dir, "hook"), strings.ReplaceAll(fx.onFirstApply, "$RESOLV", resolv), 0o644)
	}
	if fx.failRestore {
		write(filepath.Join(dir, "fail"), "", 0o644)
	}
	s := strings.NewReplacer(
		"/run/systemd/resolve/resolv.conf", resolv,
		"/run/stoarama-egress-firewall.lock", filepath.Join(dir, "lock"),
		"/etc/stoarama", stateDir,
	).Replace(script)
	cmd := exec.Command(bash, "-c", s)
	cmd.Env = append(os.Environ(), "PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	msg, runErr := cmd.CombinedOutput()

	var res firewallRun
	res.err = runErr
	res.output = string(msg)
	last := func(fam string) (string, int) {
		raw, _ := os.ReadFile(filepath.Join(dir, fam+".restore"))
		commits := strings.Split(strings.TrimSuffix(string(raw), "@@COMMIT@@\n"), "@@COMMIT@@\n")
		if len(raw) == 0 {
			return "", 0
		}
		return commits[len(commits)-1], len(commits)
	}
	res.v4, res.v4Commits = last("iptables")
	res.v6, _ = last("ip6tables")
	got, _ := os.ReadFile(upstreamFile)
	res.persisted = string(got)
	return res
}

// TestEgressFirewallScript_DNSUpstreamSelection executes the rendered firewall
// script against stubbed iptables to pin which resolvers get a port-53 allowance.
func TestEgressFirewallScript_DNSUpstreamSelection(t *testing.T) {
	const vpc = "10.116.15.254"
	const vpcNew = "10.116.15.253"
	strPtr := func(s string) *string { return &s }
	cases := []struct {
		name      string
		fx        firewallFixture
		allowed   []string
		denied    []string
		persist   string
		v4Commits int // 0: don't check
	}{
		{
			name:    "live VPC upstream allowed and persisted; stub, metadata, link-local skipped",
			fx:      firewallFixture{live: strPtr("nameserver 127.0.0.53\nnameserver " + vpc + "\nnameserver 169.254.169.254\nnameserver FE90::1\n")},
			allowed: []string{vpc + "/32"},
			denied:  []string{"127.0.0.53/32", "169.254.169.254/32", "fe90::1/128"},
			persist: vpc + "\n",
		},
		{
			name:    "early boot without resolv.conf uses persisted list",
			fx:      firewallFixture{persisted: strPtr(vpc + "\n")},
			allowed: []string{vpc + "/32"},
			persist: vpc + "\n",
		},
		{
			name:    "readable resolv.conf with no eligible upstream falls back to persisted",
			fx:      firewallFixture{live: strPtr("nameserver 127.0.0.53\n"), persisted: strPtr(vpc + "\n")},
			allowed: []string{vpc + "/32"},
			persist: vpc + "\n",
		},
		{
			name:    "changed upstream replaces the old one",
			fx:      firewallFixture{live: strPtr("nameserver " + vpcNew + "\n"), persisted: strPtr(vpc + "\n")},
			allowed: []string{vpcNew + "/32"},
			denied:  []string{vpc + "/32"},
			persist: vpcNew + "\n",
		},
		{
			name:    "no upstream anywhere allows none",
			fx:      firewallFixture{live: strPtr("nameserver 127.0.0.53\n")},
			denied:  []string{vpc + "/32"},
			persist: "",
		},
		{
			name:    "live IPv6 upstream allowed with scope id stripped and lowercased",
			fx:      firewallFixture{live: strPtr("nameserver FD00::53%eth1\nnameserver ::1\n")},
			allowed: []string{"fd00::53/128"},
			persist: "fd00::53\n", // ::1 is only the fixed loopback-stub rule, never an upstream
		},
		{
			name: "malformed resolver values are skipped, never abort the rebuild",
			fx: firewallFixture{live: strPtr("nameserver 10.0.0.256\nnameserver 1.2.3\nnameserver fd00::zz\n" +
				"nameserver 1:2:3:4:5:6:7:8:9\nnameserver fd00:::1\nnameserver 10.0.0.1/8\nnameserver " + vpc + "\n")},
			allowed: []string{vpc + "/32"},
			denied:  []string{"10.0.0.256/32", "1.2.3/32", "fd00::zz/128", "1:2:3:4:5:6:7:8:9/128", "fd00:::1/128"},
			persist: vpc + "\n",
		},
		{
			name: "resolver change during a refresh is re-applied before exiting",
			fx: firewallFixture{
				live:         strPtr("nameserver " + vpc + "\n"),
				onFirstApply: "printf 'nameserver " + vpcNew + "\\n' > '$RESOLV'\n",
			},
			allowed:   []string{vpcNew + "/32"},
			denied:    []string{vpc + "/32"},
			persist:   vpcNew + "\n",
			v4Commits: 2,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res := runEgressFirewall(t, tc.fx)
			if res.err != nil {
				t.Fatalf("firewall script failed: %v\n%s", res.err, res.output)
			}
			rules := res.v4 + res.v6
			for _, ns := range tc.allowed {
				for _, proto := range []string{"udp", "tcp"} {
					want := "-A STOARAMA_EGRESS -p " + proto + " --dport 53 -d " + ns + " -j RETURN"
					if !strings.Contains(rules, want) {
						t.Fatalf("missing DNS allowance %q in:\n%s", want, rules)
					}
				}
			}
			for _, ns := range tc.denied {
				if strings.Contains(rules, "--dport 53 -d "+ns+" ") {
					t.Fatalf("unexpected DNS allowance for %s in:\n%s", ns, rules)
				}
			}
			// Each family is one complete transaction: allowances precede the
			// private-range REJECTs and the chain ends in COMMIT.
			for fam, tx := range map[string]string{"v4": res.v4, "v6": res.v6} {
				reject := "-A STOARAMA_EGRESS -d 10.0.0.0/8 -j REJECT"
				if fam == "v6" {
					reject = "-A STOARAMA_EGRESS -d fc00::/7 -j REJECT"
				}
				rejectIdx := strings.Index(tx, reject)
				if !strings.HasPrefix(tx, "*filter\n") || rejectIdx < 0 || !strings.HasSuffix(tx, "COMMIT\n") {
					t.Fatalf("%s transaction incomplete:\n%s", fam, tx)
				}
				if i := strings.LastIndex(tx, "--dport 53"); i > rejectIdx {
					t.Fatalf("%s DNS allowance after the private-range REJECT:\n%s", fam, tx)
				}
			}
			if res.persisted != tc.persist {
				t.Fatalf("persisted upstreams = %q, want %q", res.persisted, tc.persist)
			}
			if tc.v4Commits != 0 && res.v4Commits != tc.v4Commits {
				t.Fatalf("iptables-restore commits = %d, want %d", res.v4Commits, tc.v4Commits)
			}
		})
	}
}

// TestEgressFirewallScript_FailedRestoreKeepsPreviousState: a rejected
// iptables-restore transaction commits nothing (the previous chain stays in
// force), so the script must fail loudly and not persist the unapplied set.
func TestEgressFirewallScript_FailedRestoreKeepsPreviousState(t *testing.T) {
	live := "nameserver 10.116.15.253\n"
	persisted := "10.116.15.254\n"
	res := runEgressFirewall(t, firewallFixture{live: &live, persisted: &persisted, failRestore: true})
	if res.err == nil {
		t.Fatalf("firewall script must fail when iptables-restore rejects the transaction:\n%s", res.output)
	}
	if res.v4Commits != 0 {
		t.Fatalf("no transaction may be recorded as committed, got %d", res.v4Commits)
	}
	if res.persisted != persisted {
		t.Fatalf("persisted upstreams changed after failed apply: %q", res.persisted)
	}
}

// TestBuildUserData_FailsFastWhenSourceFetchFails guards the silent-stale-binary
// failure: a failed git fetch must abort cloud-init runcmd, not fall through to
// the snapshot's baked binary.
func TestBuildUserData_FailsFastWhenSourceFetchFails(t *testing.T) {
	out, err := BuildUserData(UserDataConfig{
		ServerID:      "stoarama-rec-failfast",
		NodeToken:     "sin_token",
		BackendAPIURL: "https://stoarama-api.onrender.com",
		RepoURL:       "https://github.com/daydemir/stoarama.git",
		BuildSHA:      strings.Repeat("c", 40),
	})
	if err != nil {
		t.Fatalf("BuildUserData: %v", err)
	}
	runcmd := out[strings.Index(out, "\nruncmd:\n"):]
	setIdx := strings.Index(runcmd, "set -e")
	fetchIdx := strings.Index(runcmd, "git -C /opt/stoarama fetch --depth 1 origin")
	cloneIdx := strings.Index(runcmd, "git clone")
	if setIdx < 0 || fetchIdx < 0 || cloneIdx < 0 || setIdx > cloneIdx || setIdx > fetchIdx {
		t.Fatalf("runcmd must enable set -e before cloning/fetching the build commit")
	}
}

// extractEgressFirewallScript returns the de-indented firewall script body from
// the rendered cloud-init.
func extractEgressFirewallScript(t *testing.T, cloudInit string) string {
	t.Helper()
	start := strings.Index(cloudInit, "#!/usr/bin/env bash\n")
	end := strings.Index(cloudInit, "netfilter-persistent save")
	if start < 0 || end < start {
		t.Fatalf("rendered cloud-init has no egress firewall script")
	}
	var b strings.Builder
	for _, line := range strings.Split(cloudInit[start:end], "\n") {
		b.WriteString(strings.TrimPrefix(line, "      "))
		b.WriteString("\n")
	}
	return b.String()
}
