package api

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
)

const copyControlLeasePath = "/api/v1/account/connections/r2-copy/lease"

type copyControlFixtureRequest struct {
	Operation int    `json:"operation"`
	Path      string `json:"path"`
	Body      any    `json:"body"`
}

type copyControlFixtureResult struct {
	Results []struct {
		OK     bool   `json:"ok"`
		Status int    `json:"status"`
		TaskID *int64 `json:"task_id"`
		Active bool   `json:"active"`
	} `json:"results"`
	Timings  []string `json:"timings"`
	Children int      `json:"children"`
	Reaped   bool     `json:"reaped"`
}

func runCopyControlFixture(t *testing.T, url, ca string, timeout time.Duration, requests []copyControlFixtureRequest) copyControlFixtureResult {
	return runCopyControlFixtureMode(t, url, ca, timeout, requests, false)
}
func runCopyControlFixtureMode(t *testing.T, url, ca string, timeout time.Duration, requests []copyControlFixtureRequest, sequential bool) copyControlFixtureResult {
	return runCopyControlFixtureLimits(t, url, ca, timeout, requests, sequential, 15*time.Second)
}
func runCopyControlFixtureLimits(t *testing.T, url, ca string, timeout time.Duration, requests []copyControlFixtureRequest, sequential bool, connectTimeout time.Duration) copyControlFixtureResult {
	t.Helper()
	python := os.Getenv("STOARAMA_TEST_PYTHON")
	if python == "" {
		python = "python3"
	}
	script, err := filepath.Abs("../../../clients/nas-pull/copy_control_fixture.py")
	if err != nil {
		t.Fatal(err)
	}
	args := []string{"-I", "-B", script, "--url", url, "--timeout", fmt.Sprint(timeout.Seconds()), "--connect-timeout", fmt.Sprint(connectTimeout.Seconds())}
	if ca != "" {
		args = append(args, "--ca-file", ca)
	}
	children := len(requests)
	if sequential {
		args = append(args, "--sequential")
		children = 1
	}
	data, err := json.Marshal(requests)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, python, args...)
	cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "PYTHONDONTWRITEBYTECODE=1"}
	cmd.Stdin = strings.NewReader(string(data))
	cmd.Cancel = func() error { return cmd.Process.Signal(syscall.SIGTERM) }
	cmd.WaitDelay = 5 * time.Second
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("local control fixture: %v %s", err, out)
	}
	if strings.Contains(string(out), "fixture-key") || strings.Contains(string(out), "Bearer ") || strings.Contains(string(out), "Authorization") {
		t.Fatal("control fixture leaked credentials")
	}
	var result copyControlFixtureResult
	if err = json.Unmarshal(out, &result); err != nil {
		t.Fatalf("fixture result: %v %s", err, out)
	}
	if !result.Reaped || result.Children != children || len(result.Results) != len(requests) {
		t.Fatalf("fixture did not drain its owned processes: %+v", result)
	}
	t.Logf("local fixture timings: %v", result.Timings)
	return result
}

func authenticatedCopyFixture(t *testing.T) (*cleanupFixture, *httptest.Server) {
	t.Helper()
	f, first := copyTestFixture(t)
	f.useClientVersion(nasCleanupR2CopyControlClientVersion)
	f.s.cfg.NASR2CopyEnabled = true
	_, err := f.pool.Exec(f.ctx, `ALTER TABLE accounts ADD COLUMN email text DEFAULT 'fixture@example.test', ADD COLUMN name text DEFAULT 'fixture', ADD COLUMN role text DEFAULT 'member', ADD COLUMN status text DEFAULT 'active';
CREATE TABLE account_api_keys(id bigint PRIMARY KEY,account_id bigint,secret_hash text,scopes text[],revoked_at timestamptz,expires_at timestamptz,last_used_at timestamptz)`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.pool.Exec(f.ctx, `INSERT INTO account_api_keys(id,account_id,secret_hash,scopes) VALUES(5,47,$1,$2)`, hashSecret("fixture-key"), []string{accountScopePull}); err != nil {
		t.Fatal(err)
	}
	for n := 1; n < NASR2CopyWorkers; n++ {
		item := first
		item.SourceID += int64(n)
		item.RelativePath = fmt.Sprintf("p/copy-%d.mp4", n)
		item.ObjectKey = fmt.Sprintf("managed/acct-47/copy-%d.mp4", n)
		body := []byte(fmt.Sprintf("fixture kept media %d", n))
		sum := sha256.Sum256(body)
		item.SHA256, item.SizeBytes = hex.EncodeToString(sum[:]), int64(len(body))
		f.bucket.put(item.ObjectKey, body)
		_, err = f.pool.Exec(f.ctx, `INSERT INTO recording_clips(id,recording_id,object_key,sha256,size_bytes,display_path,clip_start_at,clip_end_at,bucket) VALUES($1,2,$2,$3,$4,$5,$6,$7,'bucket')`,
			item.SourceID, item.ObjectKey, item.SHA256, item.SizeBytes, item.RelativePath, item.StartAt, item.EndAt)
		if err != nil {
			t.Fatal(err)
		}
		data, _ := json.Marshal(item)
		policy, _, _ := Keep28Policy()
		if _, err = f.pool.Exec(f.ctx, `INSERT INTO nas_r2_copy_items(connection_id,kind,source_id,manifest_sha256,policy_sha256,item) VALUES(13,'raw',$1,repeat('a',64),$2,$3)`, item.SourceID, policy, data); err != nil {
			t.Fatal(err)
		}
	}
	router := chi.NewRouter()
	router.Use(f.s.requireAccountAuth, f.s.confineAccountScope, f.s.fenceNASDelivery)
	router.Get("/api/v1/account/connections/stitch/evidence/admission", f.s.handleAccountNASStitchEvidenceAdmission)
	router.Post(copyControlLeasePath, f.s.handleNASR2CopyLease)
	router.Post("/api/v1/account/connections/r2-copy/{id}/checkpoint", f.s.handleNASR2CopyCheckpoint)
	srv := httptest.NewServer(router)
	t.Cleanup(srv.Close)
	return f, srv
}

func TestNASR2CopyControlAuthenticatedAdmission(t *testing.T) {
	for _, delay := range []time.Duration{100 * time.Millisecond, 600 * time.Millisecond} {
		t.Run(delay.String(), func(t *testing.T) {
			f, srv := authenticatedCopyFixture(t)
			f.bucket.headDelay = delay
			requests := make([]copyControlFixtureRequest, NASR2CopyWorkers)
			for n := range requests {
				requests[n] = copyControlFixtureRequest{1, copyControlLeasePath, nasR2CopyControl{FreeBytes: NASR2CopyFloor + 1e9}}
			}
			result := runCopyControlFixture(t, srv.URL, "", 2*time.Second, requests)
			successes := 0
			for _, response := range result.Results {
				if response.OK && response.TaskID != nil {
					successes++
				}
			}
			if delay == 100*time.Millisecond && successes != NASR2CopyWorkers {
				t.Fatalf("fast full-auth admission did not fit unchanged2s request budget: %+v", result)
			}
			f.bucket.mu.Lock()
			peak := f.bucket.peakHeads
			f.bucket.mu.Unlock()
			if delay == 600*time.Millisecond && (successes == 0 || peak <= 1) {
				t.Fatalf("full-auth HEAD requests did not overlap: peak=%d result=%+v", peak, result)
			}
			var leases, ranges int
			var authenticated bool
			if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FILTER(WHERE state='leased'),count(*) FILTER(WHERE range_id IS NOT NULL) FROM nas_r2_copy_items`).Scan(&leases, &ranges); err != nil {
				t.Fatal(err)
			}
			if err := f.pool.QueryRow(f.ctx, `SELECT last_used_at IS NOT NULL FROM account_api_keys WHERE id=5`).Scan(&authenticated); err != nil {
				t.Fatal(err)
			}
			if !authenticated || leases > NASR2CopyWorkers || ranges != 0 {
				t.Fatalf("auth/authority bounds: authenticated=%t leases=%d ranges=%d", authenticated, leases, ranges)
			}
		})
	}
}

func TestNASR2CopyControlRenewalCompetesWithAdmissions(t *testing.T) {
	f, srv := authenticatedCopyFixture(t)
	first := runCopyControlFixture(t, srv.URL, "", 2*time.Second, []copyControlFixtureRequest{{1, copyControlLeasePath, nasR2CopyControl{FreeBytes: NASR2CopyFloor + 1e9}}})
	if !first.Results[0].OK || first.Results[0].TaskID == nil {
		t.Fatal("first authenticated lease refused")
	}
	id := *first.Results[0].TaskID
	var token string
	var before time.Time
	if err := f.pool.QueryRow(f.ctx, `SELECT lease_token,lease_expires_at FROM nas_r2_copy_items WHERE id=$1`, id).Scan(&token, &before); err != nil {
		t.Fatal(err)
	}
	f.bucket.headDelay = 100 * time.Millisecond
	requests := []copyControlFixtureRequest{
		{1, copyControlLeasePath, nasR2CopyControl{FreeBytes: NASR2CopyFloor + 1e9}},
		{1, copyControlLeasePath, nasR2CopyControl{FreeBytes: NASR2CopyFloor + 1e9}},
		{1, copyControlLeasePath, nasR2CopyControl{FreeBytes: NASR2CopyFloor + 1e9}},
		{2, fmt.Sprintf("/api/v1/account/connections/r2-copy/%d/checkpoint", id), nasR2CopyControl{Token: token, FreeBytes: NASR2CopyFloor + 1e9}},
	}
	result := runCopyControlFixture(t, srv.URL, "", 2*time.Second, requests)
	for _, response := range result.Results {
		if !response.OK {
			t.Fatalf("fast admission/renewal refused: %+v", result)
		}
	}
	var after time.Time
	var attempts, leases int
	if err := f.pool.QueryRow(f.ctx, `SELECT lease_expires_at,attempts FROM nas_r2_copy_items WHERE id=$1 AND lease_token=$2`, id, token).Scan(&after, &attempts); err != nil {
		t.Fatal(err)
	}
	if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM nas_r2_copy_items WHERE state='leased' AND lease_expires_at>clock_timestamp()`).Scan(&leases); err != nil {
		t.Fatal(err)
	}
	if !result.Results[3].Active || !after.After(before) || attempts != 1 || leases != NASR2CopyWorkers {
		t.Fatalf("renewal lost authority: active=%t renewed=%t attempts=%d leases=%d", result.Results[3].Active, after.After(before), attempts, leases)
	}
}

func TestNASR2CopyControlHTTPS(t *testing.T) {
	requests := []copyControlFixtureRequest{{1, "/control", map[string]any{}}}
	var connections atomic.Int32
	var slowHandshake atomic.Bool
	var forwarded atomic.Int32
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		forwarded.Add(1)
		w.WriteHeader(500)
	}))
	defer destination.Close()
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/redirect" {
			http.Redirect(w, r, destination.URL, http.StatusFound)
			return
		}
		if r.URL.Path == "/close" {
			w.Header().Set("Connection", "close")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("{\"task\":null}"))
	}))
	srv.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			connections.Add(1)
		}
	}
	srv.TLS = &tls.Config{GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) {
		if slowHandshake.Load() {
			time.Sleep(1500 * time.Millisecond)
		}
		return nil, nil
	}}
	srv.StartTLS()
	defer srv.Close()
	ca := filepath.Join(t.TempDir(), "fixture-public-ca.pem")
	if err := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}), 0600); err != nil {
		t.Fatal(err)
	}
	t.Run("trusted", func(t *testing.T) {
		result := runCopyControlFixture(t, srv.URL, ca, 2*time.Second, requests)
		if !result.Results[0].OK {
			t.Fatalf("trusted fixture refused: %+v", result)
		}
	})
	t.Run("persistent", func(t *testing.T) {
		before := connections.Load()
		repeated := []copyControlFixtureRequest{{1, "/control", nil}, {2, "/control", nil}, {3, "/control", nil}}
		result := runCopyControlFixtureMode(t, srv.URL, ca, 2*time.Second, repeated, true)
		if len(result.Results) != 3 || connections.Load()-before != 1 {
			t.Fatalf("HTTPS connection not reused: connections=%d", connections.Load()-before)
		}
	})
	t.Run("slow-handshake-and-reconnect-outside-request-deadline", func(t *testing.T) {
		slowHandshake.Store(true)
		defer slowHandshake.Store(false)
		before := connections.Load()
		repeated := []copyControlFixtureRequest{{1, "/control", nil}, {2, "/close", nil}, {3, "/control", nil}}
		result := runCopyControlFixtureMode(t, srv.URL, ca, 300*time.Millisecond, repeated, true)
		if connections.Load()-before != 2 {
			t.Fatalf("expected one reconnect, connections=%d", connections.Load()-before)
		}
		slow := 0
		for _, line := range result.Timings {
			if strings.Contains(line, "phase=3 ") {
				m := regexp.MustCompile(`connect_ready_ms=(\d+)`).FindStringSubmatch(line)
				if len(m) == 2 {
					ms, _ := strconv.Atoi(m[1])
					if ms >= 1500 {
						slow++
					}
				}
			}
			if strings.Contains(line, "phase=2 ") {
				m := regexp.MustCompile(`total_ms=(\d+)`).FindStringSubmatch(line)
				if len(m) != 2 {
					t.Fatal(line)
				}
				ms, _ := strconv.Atoi(m[1])
				if ms >= 300 || !strings.HasSuffix(line, "status=200") {
					t.Fatal(line)
				}
			}
		}
		if slow != 2 {
			t.Fatalf("cold/reconnected TLS not measured independently: %v", result.Timings)
		}
	})
	t.Run("untrusted", func(t *testing.T) {
		result := runCopyControlFixture(t, srv.URL, "", 2*time.Second, requests)
		if result.Results[0].OK || !strings.Contains(strings.Join(result.Timings, "\n"), "connect_ready_ms=-1") {
			t.Fatal("untrusted fixture was accepted")
		}
	})
	t.Run("hostname", func(t *testing.T) {
		result := runCopyControlFixture(t, strings.Replace(srv.URL, "127.0.0.1", "localhost", 1), ca, 2*time.Second, requests)
		if result.Results[0].OK {
			t.Fatal("hostname mismatch was accepted")
		}
	})
	t.Run("redirect", func(t *testing.T) {
		result := runCopyControlFixture(t, srv.URL, ca, 2*time.Second, []copyControlFixtureRequest{{1, "/redirect", map[string]any{}}})
		if result.Results[0].OK || forwarded.Load() != 0 || !strings.Contains(strings.Join(result.Timings, "\n"), "status=302") {
			t.Fatal("redirect forwarded a credential-bearing request or lost status")
		}
	})
	t.Run("handshake-deadline", func(t *testing.T) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		done, drained := make(chan struct{}), make(chan struct{})
		go func() {
			defer close(drained)
			conn, err := listener.Accept()
			if err == nil {
				defer conn.Close()
				<-done
			}
		}()
		defer func() { close(done); listener.Close(); <-drained }()
		result := runCopyControlFixtureLimits(t, "https://"+listener.Addr().String(), "", 300*time.Millisecond, requests, false, 300*time.Millisecond)
		if result.Results[0].OK || !strings.Contains(strings.Join(result.Timings, "\n"), "connect_ready_ms=-1") {
			t.Fatal("stalled TLS handshake was accepted")
		}
	})
}

func TestNASR2CopyControlCapabilityUnion(t *testing.T) {
	v := nasCleanupR2CopyControlClientVersion
	if !nasCleanupSupportsBulk(v) || !nasCleanupSupportsGroupedExecution(v) || nasCleanupSupportsV6(v) || stitchProofSelector(1, 13, v) != 1 || stitchProofSelector(0, 13, v) != 0 {
		t.Fatal("copy control release lost combined capability or gained legacy/default proof activation")
	}
	for _, unknown := range []string{v + "-extra", "r2-copy-v", "r2-copy-v30"} {
		if nasCleanupSupportsBulk(unknown) || nasCleanupSupportsGroupedExecution(unknown) {
			t.Fatal("copy capability prefix admitted")
		}
	}
}
