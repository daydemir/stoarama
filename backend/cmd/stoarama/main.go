package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

type cliConfig struct {
	APIBaseURL string                    `json:"api_base_url,omitempty"`
	APIKey     string                    `json:"api_key,omitempty"`
	Node       *cliNodeConfig            `json:"node,omitempty"`
	Nodes      map[string]*cliNodeConfig `json:"nodes,omitempty"`
}

type cliNodeConfig struct {
	ID          int64  `json:"id"`
	NodeType    string `json:"node_type"`
	DisplayName string `json:"display_name"`
	Hostname    string `json:"hostname,omitempty"`
	Platform    string `json:"platform,omitempty"`
	Token       string `json:"token"`
	APIBaseURL  string `json:"api_base_url,omitempty"`
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "-h", "--help", "help":
		usage()
	case "auth":
		runAuth(os.Args[2:])
	case "node":
		runNode(os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Print(`stoarama commands:
  stoarama auth configure [--api-base-url URL --api-key KEY]
  stoarama auth request-link --email EMAIL [--api-base-url URL]
  stoarama auth whoami [--api-base-url URL --api-key KEY]
  stoarama auth api-keys list [--api-base-url URL --api-key KEY]
  stoarama auth api-keys create [--label LABEL --expires-at RFC3339 --save] [--api-base-url URL --api-key KEY]
  stoarama auth api-keys revoke --id N [--api-base-url URL --api-key KEY]

  stoarama node enrollment-tokens list [--api-base-url URL --api-key KEY]
  stoarama node enrollment-tokens create --node-type inference_node|local_recorder [--label LABEL --expires-at RFC3339] [--api-base-url URL --api-key KEY]
  stoarama node enrollment-tokens revoke --id N [--api-base-url URL --api-key KEY]
  stoarama node enroll --token TOKEN --node-type inference_node|local_recorder [--display-name NAME --hostname HOST --platform PLATFORM --api-base-url URL]
  stoarama node whoami [--node-type inference_node|local_recorder --api-base-url URL --node-token TOKEN]
  stoarama node heartbeat [--node-type inference_node|local_recorder --api-base-url URL --node-token TOKEN]
  stoarama node doctor [--node-type inference_node|local_recorder]
`)
}

func runAuth(args []string) {
	if len(args) < 1 {
		usage()
		os.Exit(2)
	}
	switch args[0] {
	case "configure":
		runAuthConfigure(args[1:])
	case "request-link":
		runAuthRequestLink(args[1:])
	case "whoami":
		runAuthWhoAmI(args[1:])
	case "api-keys":
		runAuthAPIKeys(args[1:])
	default:
		usage()
		os.Exit(2)
	}
}

func runNode(args []string) {
	if len(args) < 1 {
		usage()
		os.Exit(2)
	}
	switch args[0] {
	case "enrollment-tokens":
		runNodeEnrollmentTokens(args[1:])
	case "enroll":
		runNodeEnroll(args[1:])
	case "whoami", "status":
		runNodeWhoAmI(args[1:])
	case "heartbeat":
		runNodeHeartbeat(args[1:])
	case "doctor":
		runNodeDoctor(args[1:])
	default:
		usage()
		os.Exit(2)
	}
}

func runAuthConfigure(args []string) {
	fs := flag.NewFlagSet("auth configure", flag.ExitOnError)
	apiBaseURL := fs.String("api-base-url", "", "Stoarama API base URL")
	apiKey := fs.String("api-key", "", "account API key")
	_ = fs.Parse(args)

	cfg, _ := loadCLIConfig()
	if strings.TrimSpace(*apiBaseURL) != "" {
		cfg.APIBaseURL = normalizeBaseURL(*apiBaseURL)
	}
	if strings.TrimSpace(*apiKey) != "" {
		cfg.APIKey = strings.TrimSpace(*apiKey)
	}
	if err := saveCLIConfig(cfg); err != nil {
		fatalf("save config: %v", err)
	}
	printJSON(cfg)
}

func runAuthRequestLink(args []string) {
	fs := flag.NewFlagSet("auth request-link", flag.ExitOnError)
	apiBaseURL := fs.String("api-base-url", defaultAPIBaseURL(), "Stoarama API base URL")
	email := fs.String("email", "", "account email")
	_ = fs.Parse(args)

	if strings.TrimSpace(*email) == "" {
		fatalf("--email is required")
	}
	var resp map[string]any
	err := apiRequest("POST", normalizeBaseURL(*apiBaseURL)+"/api/v1/auth/request-link", map[string]any{
		"email": *email,
	}, "", &resp)
	if err != nil {
		fatalf("request sign-in link: %v", err)
	}
	printJSON(resp)
}

func runAuthWhoAmI(args []string) {
	fs := flag.NewFlagSet("auth whoami", flag.ExitOnError)
	apiBaseURL := fs.String("api-base-url", "", "Stoarama API base URL")
	apiKey := fs.String("api-key", "", "account API key")
	_ = fs.Parse(args)

	baseURL, token := mustResolveUserAuth(*apiBaseURL, *apiKey)
	var resp map[string]any
	if err := apiRequest("GET", baseURL+"/api/v1/account/me", nil, token, &resp); err != nil {
		fatalf("load account: %v", err)
	}
	printJSON(resp)
}

func runAuthAPIKeys(args []string) {
	if len(args) < 1 {
		usage()
		os.Exit(2)
	}
	switch args[0] {
	case "list":
		fs := flag.NewFlagSet("auth api-keys list", flag.ExitOnError)
		apiBaseURL := fs.String("api-base-url", "", "Stoarama API base URL")
		apiKey := fs.String("api-key", "", "account API key")
		_ = fs.Parse(args[1:])
		baseURL, token := mustResolveUserAuth(*apiBaseURL, *apiKey)
		var resp map[string]any
		if err := apiRequest("GET", baseURL+"/api/v1/account/api-keys", nil, token, &resp); err != nil {
			fatalf("list api keys: %v", err)
		}
		printJSON(resp)
	case "create":
		fs := flag.NewFlagSet("auth api-keys create", flag.ExitOnError)
		apiBaseURL := fs.String("api-base-url", "", "Stoarama API base URL")
		apiKey := fs.String("api-key", "", "account API key")
		label := fs.String("label", "default", "API key label")
		expiresAt := fs.String("expires-at", "", "optional RFC3339 expiry")
		save := fs.Bool("save", false, "save returned token into local config")
		_ = fs.Parse(args[1:])
		baseURL, token := mustResolveUserAuth(*apiBaseURL, *apiKey)
		var resp map[string]any
		if err := apiRequest("POST", baseURL+"/api/v1/account/api-keys", map[string]any{
			"label":      *label,
			"expires_at": strings.TrimSpace(*expiresAt),
		}, token, &resp); err != nil {
			fatalf("create api key: %v", err)
		}
		if *save {
			cfg, _ := loadCLIConfig()
			cfg.APIBaseURL = baseURL
			if raw, ok := resp["token"].(string); ok && strings.TrimSpace(raw) != "" {
				cfg.APIKey = strings.TrimSpace(raw)
			}
			if err := saveCLIConfig(cfg); err != nil {
				fatalf("save config: %v", err)
			}
		}
		printJSON(resp)
	case "revoke":
		fs := flag.NewFlagSet("auth api-keys revoke", flag.ExitOnError)
		apiBaseURL := fs.String("api-base-url", "", "Stoarama API base URL")
		apiKey := fs.String("api-key", "", "account API key")
		id := fs.Int64("id", 0, "API key id")
		_ = fs.Parse(args[1:])
		if *id <= 0 {
			fatalf("--id is required")
		}
		baseURL, token := mustResolveUserAuth(*apiBaseURL, *apiKey)
		var resp map[string]any
		if err := apiRequest("POST", fmt.Sprintf("%s/api/v1/account/api-keys/%d/revoke", baseURL, *id), map[string]any{}, token, &resp); err != nil {
			fatalf("revoke api key: %v", err)
		}
		printJSON(resp)
	default:
		usage()
		os.Exit(2)
	}
}

func runNodeEnrollmentTokens(args []string) {
	if len(args) < 1 {
		usage()
		os.Exit(2)
	}
	switch args[0] {
	case "list":
		fs := flag.NewFlagSet("node enrollment-tokens list", flag.ExitOnError)
		apiBaseURL := fs.String("api-base-url", "", "Stoarama API base URL")
		apiKey := fs.String("api-key", "", "account API key")
		_ = fs.Parse(args[1:])
		baseURL, token := mustResolveUserAuth(*apiBaseURL, *apiKey)
		var resp map[string]any
		if err := apiRequest("GET", baseURL+"/api/v1/account/node-enrollment-tokens", nil, token, &resp); err != nil {
			fatalf("list node enrollment tokens: %v", err)
		}
		printJSON(resp)
	case "create":
		fs := flag.NewFlagSet("node enrollment-tokens create", flag.ExitOnError)
		apiBaseURL := fs.String("api-base-url", "", "Stoarama API base URL")
		apiKey := fs.String("api-key", "", "account API key")
		nodeType := fs.String("node-type", "", "inference_node or local_recorder")
		label := fs.String("label", "", "token label")
		expiresAt := fs.String("expires-at", "", "optional RFC3339 expiry")
		_ = fs.Parse(args[1:])
		if strings.TrimSpace(*nodeType) == "" {
			fatalf("--node-type is required")
		}
		if strings.TrimSpace(*nodeType) == "yt_relay_source" {
			fatalf("yt_relay_source enrollment is disabled; use local_recorder")
		}
		baseURL, token := mustResolveUserAuth(*apiBaseURL, *apiKey)
		var resp map[string]any
		if err := apiRequest("POST", baseURL+"/api/v1/account/node-enrollment-tokens", map[string]any{
			"node_type":  strings.TrimSpace(*nodeType),
			"label":      strings.TrimSpace(*label),
			"expires_at": strings.TrimSpace(*expiresAt),
		}, token, &resp); err != nil {
			fatalf("create node enrollment token: %v", err)
		}
		printJSON(resp)
	case "revoke":
		fs := flag.NewFlagSet("node enrollment-tokens revoke", flag.ExitOnError)
		apiBaseURL := fs.String("api-base-url", "", "Stoarama API base URL")
		apiKey := fs.String("api-key", "", "account API key")
		id := fs.Int64("id", 0, "token id")
		_ = fs.Parse(args[1:])
		if *id <= 0 {
			fatalf("--id is required")
		}
		baseURL, token := mustResolveUserAuth(*apiBaseURL, *apiKey)
		var resp map[string]any
		if err := apiRequest("POST", fmt.Sprintf("%s/api/v1/account/node-enrollment-tokens/%d/revoke", baseURL, *id), map[string]any{}, token, &resp); err != nil {
			fatalf("revoke node enrollment token: %v", err)
		}
		printJSON(resp)
	default:
		usage()
		os.Exit(2)
	}
}

func runNodeEnroll(args []string) {
	fs := flag.NewFlagSet("node enroll", flag.ExitOnError)
	apiBaseURL := fs.String("api-base-url", defaultAPIBaseURL(), "Stoarama API base URL")
	token := fs.String("token", "", "node enrollment token")
	nodeType := fs.String("node-type", "", "inference_node or local_recorder")
	displayName := fs.String("display-name", defaultNodeDisplayName(), "node display name")
	hostname := fs.String("hostname", defaultHostname(), "node hostname")
	platform := fs.String("platform", defaultPlatform(), "node platform")
	_ = fs.Parse(args)

	if strings.TrimSpace(*token) == "" {
		fatalf("--token is required")
	}
	if strings.TrimSpace(*nodeType) == "" {
		fatalf("--node-type is required")
	}
	if strings.TrimSpace(*nodeType) == "yt_relay_source" {
		fatalf("yt_relay_source enrollment is disabled; use local_recorder or `stoarama recording youtube run`")
	}

	payload := map[string]any{
		"token":             strings.TrimSpace(*token),
		"node_type":         strings.TrimSpace(*nodeType),
		"display_name":      strings.TrimSpace(*displayName),
		"hostname":          strings.TrimSpace(*hostname),
		"platform":          strings.TrimSpace(*platform),
		"capabilities_json": defaultCapabilities(strings.TrimSpace(*nodeType)),
		"metadata_json": map[string]any{
			"cli_version": "phase1",
		},
	}
	var resp struct {
		Node struct {
			ID          int64  `json:"id"`
			NodeType    string `json:"node_type"`
			DisplayName string `json:"display_name"`
			Hostname    string `json:"hostname"`
			Platform    string `json:"platform"`
		} `json:"node"`
		NodeToken string `json:"node_token"`
	}
	if err := apiRequest("POST", normalizeBaseURL(*apiBaseURL)+"/api/v1/nodes/enroll", payload, "", &resp); err != nil {
		fatalf("enroll node: %v", err)
	}
	cfg, _ := loadCLIConfig()
	setNodeConfig(&cfg, &cliNodeConfig{
		ID:          resp.Node.ID,
		NodeType:    resp.Node.NodeType,
		DisplayName: resp.Node.DisplayName,
		Hostname:    resp.Node.Hostname,
		Platform:    resp.Node.Platform,
		Token:       strings.TrimSpace(resp.NodeToken),
		APIBaseURL:  normalizeBaseURL(*apiBaseURL),
	})
	if err := saveCLIConfig(cfg); err != nil {
		fatalf("save config: %v", err)
	}
	printJSON(resp)
}

func runNodeWhoAmI(args []string) {
	fs := flag.NewFlagSet("node whoami", flag.ExitOnError)
	nodeType := fs.String("node-type", "", "inference_node or local_recorder")
	apiBaseURL := fs.String("api-base-url", "", "Stoarama API base URL")
	nodeToken := fs.String("node-token", "", "node bearer token")
	_ = fs.Parse(args)

	baseURL, token := mustResolveNodeAuthForType(*apiBaseURL, *nodeToken, *nodeType)
	var resp map[string]any
	if err := apiRequest("GET", baseURL+"/api/v1/node/me", nil, token, &resp); err != nil {
		fatalf("load node: %v", err)
	}
	printJSON(resp)
}

func runNodeHeartbeat(args []string) {
	fs := flag.NewFlagSet("node heartbeat", flag.ExitOnError)
	nodeType := fs.String("node-type", "", "inference_node or local_recorder")
	apiBaseURL := fs.String("api-base-url", "", "Stoarama API base URL")
	nodeToken := fs.String("node-token", "", "node bearer token")
	_ = fs.Parse(args)

	baseURL, token := mustResolveNodeAuthForType(*apiBaseURL, *nodeToken, *nodeType)
	cfg, _ := loadCLIConfig()
	effectiveType := strings.TrimSpace(*nodeType)
	if effectiveType == "" {
		if nodeCfg := nodeConfigForType(cfg, ""); nodeCfg != nil {
			effectiveType = nodeCfg.NodeType
		}
	}
	var resp map[string]any
	if err := apiRequest("POST", baseURL+"/api/v1/node/heartbeat", map[string]any{
		"capabilities_json": defaultCapabilities(effectiveType),
		"metadata_json": map[string]any{
			"cli_version": "phase1",
			"hostname":    defaultHostname(),
			"platform":    defaultPlatform(),
		},
	}, token, &resp); err != nil {
		fatalf("heartbeat: %v", err)
	}
	printJSON(resp)
}

func runNodeDoctor(args []string) {
	fs := flag.NewFlagSet("node doctor", flag.ExitOnError)
	nodeType := fs.String("node-type", "", "inference_node or local_recorder")
	_ = fs.Parse(args)

	cfg, _ := loadCLIConfig()
	effectiveType := strings.TrimSpace(*nodeType)
	if effectiveType == "" {
		if nodeCfg := nodeConfigForType(cfg, ""); nodeCfg != nil {
			effectiveType = nodeCfg.NodeType
		}
	}
	if effectiveType == "" {
		effectiveType = "local_recorder"
	}
	report := map[string]any{
		"node_type": effectiveType,
		"checks": []map[string]any{
			checkBinary("ffmpeg", true),
			checkBinary("yt-dlp", effectiveType == "local_recorder"),
			checkBinary("ffprobe", effectiveType == "local_recorder"),
		},
	}
	printJSON(report)
}

func mustResolveUserAuth(apiBaseURLFlag, apiKeyFlag string) (string, string) {
	cfg, _ := loadCLIConfig()
	baseURL := normalizeBaseURL(firstNonEmpty(strings.TrimSpace(apiBaseURLFlag), cfg.APIBaseURL, defaultAPIBaseURL()))
	apiKey := strings.TrimSpace(firstNonEmpty(apiKeyFlag, cfg.APIKey, os.Getenv("STOARAMA_API_KEY")))
	if apiKey == "" {
		fatalf("missing API key; pass --api-key or run `stoarama auth configure --api-key ...`")
	}
	return baseURL, apiKey
}

func mustResolveNodeAuth(apiBaseURLFlag, nodeTokenFlag string) (string, string) {
	return mustResolveNodeAuthForType(apiBaseURLFlag, nodeTokenFlag, "")
}

func mustResolveNodeAuthForType(apiBaseURLFlag, nodeTokenFlag, explicitNodeType string) (string, string) {
	cfg, _ := loadCLIConfig()
	baseURL := normalizeBaseURL(firstNonEmpty(strings.TrimSpace(apiBaseURLFlag), nodeAPIBaseURLForType(cfg, explicitNodeType), defaultAPIBaseURL()))
	token := strings.TrimSpace(firstNonEmpty(nodeTokenFlag, nodeTokenForType(cfg, explicitNodeType), os.Getenv("STOARAMA_NODE_TOKEN")))
	if token == "" {
		if strings.TrimSpace(explicitNodeType) != "" {
			fatalf("missing node token for %s; pass --node-token or run `stoarama node enroll --node-type %s ...`", strings.TrimSpace(explicitNodeType), strings.TrimSpace(explicitNodeType))
		}
		fatalf("missing node token; pass --node-token or run `stoarama node enroll ...`")
	}
	return baseURL, token
}

func apiRequest(method, url string, body any, bearer string, out any) error {
	var reqBody io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reqBody = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, url, reqBody)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if strings.TrimSpace(bearer) != "" {
		req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(bearer))
	}
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode >= 400 {
		var payload map[string]any
		if json.Unmarshal(b, &payload) == nil {
			if msg, ok := payload["error"].(string); ok && strings.TrimSpace(msg) != "" {
				return fmt.Errorf("%s (%d)", msg, resp.StatusCode)
			}
		}
		return fmt.Errorf("%s (%d)", strings.TrimSpace(string(b)), resp.StatusCode)
	}
	if out == nil || len(bytes.TrimSpace(b)) == 0 {
		return nil
	}
	return json.Unmarshal(b, out)
}

func loadCLIConfig() (cliConfig, error) {
	path, err := cliConfigPath()
	if err != nil {
		return cliConfig{}, err
	}
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return cliConfig{}, nil
	}
	if err != nil {
		return cliConfig{}, err
	}
	var cfg cliConfig
	if err := json.Unmarshal(b, &cfg); err != nil {
		return cliConfig{}, err
	}
	if cfg.Nodes == nil {
		cfg.Nodes = map[string]*cliNodeConfig{}
	}
	if cfg.Node != nil && strings.TrimSpace(cfg.Node.NodeType) != "" {
		nodeType := strings.TrimSpace(cfg.Node.NodeType)
		if _, ok := cfg.Nodes[nodeType]; !ok {
			cp := *cfg.Node
			cfg.Nodes[nodeType] = &cp
		}
	}
	cfg.Node = nil
	return cfg, nil
}

func saveCLIConfig(cfg cliConfig) error {
	path, err := cliConfigPath()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	cfg.Node = nil
	b, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o600)
}

func cliConfigPath() (string, error) {
	dir, err := os.UserConfigDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "stoarama", "config.json"), nil
}

func printJSON(v any) {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		fatalf("marshal json: %v", err)
	}
	fmt.Printf("%s\n", string(b))
}

func normalizeBaseURL(raw string) string {
	v := strings.TrimSpace(raw)
	if v == "" {
		return ""
	}
	return strings.TrimRight(v, "/")
}

func defaultAPIBaseURL() string {
	return normalizeBaseURL(firstNonEmpty(
		os.Getenv("STOARAMA_API_URL"),
		os.Getenv("APP_BASE_URL"),
		os.Getenv("BACKEND_API_URL"),
		"http://127.0.0.1:8080",
	))
}

func defaultHostname() string {
	host, err := os.Hostname()
	if err != nil || strings.TrimSpace(host) == "" {
		return "unknown-host"
	}
	return strings.TrimSpace(host)
}

func defaultPlatform() string {
	return runtime.GOOS + "/" + runtime.GOARCH
}

func defaultNodeDisplayName() string {
	return defaultHostname()
}

func defaultCapabilities(nodeType string) map[string]any {
	return map[string]any{
		"node_type":         nodeType,
		"platform":          defaultPlatform(),
		"hostname":          defaultHostname(),
		"ffmpeg_available":  hasBinary("ffmpeg"),
		"yt_dlp_available":  hasBinary("yt-dlp"),
		"enrollment_source": "stoarama_cli",
	}
}

func checkBinary(name string, required bool) map[string]any {
	path, err := exec.LookPath(name)
	return map[string]any{
		"name":     name,
		"required": required,
		"ok":       err == nil,
		"path":     path,
	}
}

func hasBinary(name string) bool {
	_, err := exec.LookPath(name)
	return err == nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func nodeAPIBaseURL(cfg cliConfig) string {
	return nodeAPIBaseURLForType(cfg, "")
}

func nodeAPIBaseURLForType(cfg cliConfig, nodeType string) string {
	node := nodeConfigForType(cfg, nodeType)
	if node == nil {
		return ""
	}
	return strings.TrimSpace(node.APIBaseURL)
}

func nodeToken(cfg cliConfig) string {
	return nodeTokenForType(cfg, "")
}

func nodeTokenForType(cfg cliConfig, nodeType string) string {
	node := nodeConfigForType(cfg, nodeType)
	if node == nil {
		return ""
	}
	return strings.TrimSpace(node.Token)
}

func nodeConfigForType(cfg cliConfig, requestedType string) *cliNodeConfig {
	want := strings.TrimSpace(requestedType)
	if want != "" {
		if cfg.Nodes != nil {
			if node := cfg.Nodes[want]; node != nil {
				return node
			}
		}
		return nil
	}
	if len(cfg.Nodes) == 1 {
		for _, node := range cfg.Nodes {
			if node != nil {
				return node
			}
		}
	}
	return nil
}

func setNodeConfig(cfg *cliConfig, node *cliNodeConfig) {
	if cfg == nil || node == nil {
		return
	}
	if cfg.Nodes == nil {
		cfg.Nodes = map[string]*cliNodeConfig{}
	}
	cp := *node
	cfg.Nodes[strings.TrimSpace(cp.NodeType)] = &cp
	cfg.Node = nil
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
