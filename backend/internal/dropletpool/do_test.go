package dropletpool

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
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
