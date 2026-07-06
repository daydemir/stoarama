package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime"
	"strings"
	"time"
)

// runEnroll consumes an sie_ enrollment token via the public POST /api/v1/nodes/enroll
// endpoint with node_type='relay', then persists the returned node id + sin_ node
// token + api_url to ~/.stoarama/config.json (0600).
func runEnroll(args []string) error {
	fs := flag.NewFlagSet("enroll", flag.ContinueOnError)
	token := fs.String("token", "", "enrollment token (sie_...)")
	apiURL := fs.String("api-url", defaultAPIURL, "Stoarama API base URL")
	name := fs.String("name", "", "display name for this computer (default: hostname)")
	concurrency := fs.Int("concurrency", defaultConcurrency, "max concurrent recordings on this computer")
	if err := fs.Parse(args); err != nil {
		return err
	}

	tok := strings.TrimSpace(*token)
	if tok == "" {
		return fmt.Errorf("--token is required")
	}
	base := strings.TrimRight(strings.TrimSpace(*apiURL), "/")
	if base == "" {
		base = defaultAPIURL
	}
	display := strings.TrimSpace(*name)
	if display == "" {
		display = defaultDisplayName()
	}
	conc := *concurrency
	if conc <= 0 {
		conc = defaultConcurrency
	}

	payload := map[string]any{
		"token":             tok,
		"node_type":         "relay",
		"display_name":      display,
		"hostname":          hostname(),
		"platform":          runtime.GOOS + "/" + runtime.GOARCH,
		"relay_max_streams": conc,
		"capabilities_json": map[string]any{
			"max_concurrent_streams": conc,
			"youtube_mode":           "cookieless",
			"youtube_ready":          false,
			"relay_version":          version,
		},
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal enroll request: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/api/v1/nodes/enroll", bytes.NewReader(b))
	if err != nil {
		return fmt.Errorf("build enroll request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("enroll request failed: %w", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 32*1024))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("enroll failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	var out struct {
		Node struct {
			ID int64 `json:"id"`
		} `json:"node"`
		NodeToken string `json:"node_token"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return fmt.Errorf("decode enroll response: %w", err)
	}
	if out.Node.ID == 0 || strings.TrimSpace(out.NodeToken) == "" {
		return fmt.Errorf("enroll response missing node id or node_token")
	}

	cfg := relayConfig{
		NodeID:      out.Node.ID,
		NodeToken:   strings.TrimSpace(out.NodeToken),
		APIURL:      base,
		Concurrency: conc,
		InstalledAt: time.Now().UTC(),
	}
	if err := saveConfig(cfg); err != nil {
		return err
	}
	p, _ := configPath()
	fmt.Printf("Enrolled as node %d. Config written to %s\n", cfg.NodeID, p)
	return nil
}

func defaultDisplayName() string {
	if h := hostname(); h != "" {
		return h
	}
	return "relay"
}

func hostname() string {
	h, err := os.Hostname()
	if err != nil {
		return ""
	}
	h = strings.TrimSpace(h)
	if i := strings.IndexByte(h, '.'); i > 0 {
		h = h[:i]
	}
	return h
}
