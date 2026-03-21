package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/daydemir/stoarama/backend/internal/captureapi"
	"github.com/daydemir/stoarama/backend/internal/youtuberelay"
)

const ytRelaySourceLaunchdLabel = "io.stoarama.youtube-relay-source"

func runNodeYTRelaySource(args []string) {
	if len(args) < 1 {
		usage()
		os.Exit(2)
	}
	switch args[0] {
	case "run":
		runNodeYTRelaySourceRun(args[1:])
	case "install-launchd":
		runNodeYTRelaySourceInstallLaunchd(args[1:])
	case "uninstall-launchd":
		runNodeYTRelaySourceUninstallLaunchd(args[1:])
	default:
		usage()
		os.Exit(2)
	}
}

func runNodeYTRelaySourceRun(args []string) {
	cfg, _ := loadCLIConfig()
	current := defaultYTRelaySourceConfig(cfg)

	fs := flag.NewFlagSet("node yt-relay-source run", flag.ExitOnError)
	apiBaseURL := fs.String("api-base-url", nodeAPIBaseURL(cfg), "Stoarama API base URL")
	nodeToken := fs.String("node-token", nodeToken(cfg), "node bearer token")
	publicBaseURL := fs.String("public-base-url", current.PublicBaseURL, "public relay base URL")
	bindAddr := fs.String("bind-addr", current.BindAddr, "relay bind address")
	shardID := fs.String("shard-id", current.ShardID, "relay shard id")
	capacity := fs.Int("capacity", current.Capacity, "max active relay routes")
	heartbeatSec := fs.Int("heartbeat-sec", current.HeartbeatSec, "heartbeat interval seconds")
	leaseSec := fs.Int("lease-sec", current.LeaseSec, "heartbeat lease seconds")
	refreshSec := fs.Int("refresh-sec", current.RefreshSec, "route refresh seconds")
	resolveTimeoutSec := fs.Int("resolve-timeout-sec", current.ResolveTimeoutSec, "resolve timeout seconds")
	resolveFailureThreshold := fs.Int("resolve-failure-threshold", current.ResolveFailureThreshold, "resolve failure threshold")
	sharedToken := fs.String("shared-token", current.SharedToken, "relay shared token")
	cacheFile := fs.String("cache-file", current.CacheFile, "relay cache file")
	cookiesFile := fs.String("cookies-file", current.CookiesFile, "yt-dlp cookies file")
	cookiesFromBrowser := fs.String("cookies-from-browser", current.CookiesFromBrowser, "yt-dlp cookies-from-browser value")
	ytDlpBin := fs.String("yt-dlp-bin", current.YTDLPBin, "optional yt-dlp binary path")
	ytDlpFormat := fs.String("yt-dlp-format", current.YTDLPFormat, "optional yt-dlp format selector")
	ytDlpFormatSort := fs.String("yt-dlp-format-sort", current.YTDLPFormatSort, "optional yt-dlp format sort selector")
	networkTransport := fs.String("network-transport", current.NetworkTransport, "network transport label")
	topologyID := fs.String("topology-id", current.TopologyID, "topology id")
	topologyRole := fs.String("topology-role", current.TopologyRole, "topology role")
	hubServerID := fs.String("hub-server-id", current.HubServerID, "hub server id")
	wgInterface := fs.String("wg-interface", current.WGInterface, "wireguard interface")
	wgIP := fs.String("wg-ip", current.WGIP, "wireguard source IP")
	sourceEndpoint := fs.String("source-endpoint", current.SourceEndpoint, "source endpoint host:port")
	_ = fs.Parse(args)

	baseURL, token := mustResolveNodeAuth(*apiBaseURL, *nodeToken)
	node := mustLoadNodeInfo(baseURL, token)
	if strings.TrimSpace(node.Node.NodeType) != "yt_relay_source" {
		fatalf("node type %q cannot run yt relay source", node.Node.NodeType)
	}

	merged := &cliYTRelaySourceConfig{
		PublicBaseURL:           strings.TrimSpace(*publicBaseURL),
		BindAddr:                strings.TrimSpace(*bindAddr),
		ShardID:                 strings.TrimSpace(*shardID),
		Capacity:                *capacity,
		HeartbeatSec:            *heartbeatSec,
		LeaseSec:                *leaseSec,
		RefreshSec:              *refreshSec,
		ResolveTimeoutSec:       *resolveTimeoutSec,
		ResolveFailureThreshold: *resolveFailureThreshold,
		SharedToken:             strings.TrimSpace(*sharedToken),
		CacheFile:               strings.TrimSpace(*cacheFile),
		CookiesFile:             strings.TrimSpace(*cookiesFile),
		CookiesFromBrowser:      strings.TrimSpace(*cookiesFromBrowser),
		YTDLPBin:                strings.TrimSpace(*ytDlpBin),
		YTDLPFormat:             strings.TrimSpace(*ytDlpFormat),
		YTDLPFormatSort:         strings.TrimSpace(*ytDlpFormatSort),
		NetworkTransport:        strings.TrimSpace(*networkTransport),
		TopologyID:              strings.TrimSpace(*topologyID),
		TopologyRole:            strings.TrimSpace(*topologyRole),
		HubServerID:             strings.TrimSpace(*hubServerID),
		WGInterface:             strings.TrimSpace(*wgInterface),
		WGIP:                    strings.TrimSpace(*wgIP),
		SourceEndpoint:          strings.TrimSpace(*sourceEndpoint),
	}
	if strings.TrimSpace(merged.SharedToken) == "" {
		merged.SharedToken = mustGenerateSecret(24)
	}
	cfg.YTRelaySource = merged
	if err := saveCLIConfig(cfg); err != nil {
		fatalf("save config: %v", err)
	}

	client, err := captureapi.NewClient(captureapi.ClientConfig{
		BaseURL:  baseURL,
		APIToken: token,
	})
	if err != nil {
		fatalf("init node capture api client: %v", err)
	}
	serverID := fmt.Sprintf("node-%d-yt-relay-source", node.Node.ID)
	err = youtuberelay.RunSource(context.Background(), youtuberelay.NodeSourceAPI{Client: client}, youtuberelay.SourceRunnerOptions{
		ServerID:                serverID,
		ShardID:                 merged.ShardID,
		Capacity:                merged.Capacity,
		HeartbeatSec:            merged.HeartbeatSec,
		LeaseSec:                merged.LeaseSec,
		RefreshSec:              merged.RefreshSec,
		ResolveTimeoutSec:       merged.ResolveTimeoutSec,
		ResolveFailureThreshold: merged.ResolveFailureThreshold,
		BindAddr:                merged.BindAddr,
		PublicBaseURL:           merged.PublicBaseURL,
		SharedToken:             merged.SharedToken,
		CacheFile:               merged.CacheFile,
		NetworkTransport:        merged.NetworkTransport,
		TopologyID:              merged.TopologyID,
		TopologyRole:            merged.TopologyRole,
		HubServerID:             merged.HubServerID,
		WGInterface:             merged.WGInterface,
		WGIP:                    merged.WGIP,
		SourceEndpoint:          merged.SourceEndpoint,
		MetadataJSON: map[string]any{
			"node_id":           node.Node.ID,
			"node_display_name": node.Node.DisplayName,
			"node_hostname":     node.Node.Hostname,
			"node_platform":     node.Node.Platform,
			"cli_binary":        "stoarama",
		},
		CookiesFile:             merged.CookiesFile,
		CookiesFromBrowser:      merged.CookiesFromBrowser,
		YTDLPBin:                merged.YTDLPBin,
		YTDLPFormat:             merged.YTDLPFormat,
		YTDLPFormatSort:         merged.YTDLPFormatSort,
		FFMPEGJPEGQuality:       2,
		FFMPEGThreads:           1,
		FFMPEGHWAccel:           "",
		FFMPEGReconnect:         true,
		FFMPEGReconnectDelayMax: 2,
	})
	if err != nil {
		fatalf("run yt relay source: %v", err)
	}
}

func runNodeYTRelaySourceInstallLaunchd(args []string) {
	if runtime.GOOS != "darwin" {
		fatalf("install-launchd is only supported on macOS")
	}
	cfg, _ := loadCLIConfig()
	current := defaultYTRelaySourceConfig(cfg)
	fs := flag.NewFlagSet("node yt-relay-source install-launchd", flag.ExitOnError)
	publicBaseURL := fs.String("public-base-url", current.PublicBaseURL, "public relay base URL")
	bindAddr := fs.String("bind-addr", current.BindAddr, "relay bind address")
	shardID := fs.String("shard-id", current.ShardID, "relay shard id")
	capacity := fs.Int("capacity", current.Capacity, "max active relay routes")
	cookiesFile := fs.String("cookies-file", current.CookiesFile, "yt-dlp cookies file")
	cookiesFromBrowser := fs.String("cookies-from-browser", current.CookiesFromBrowser, "yt-dlp cookies-from-browser value")
	sharedToken := fs.String("shared-token", current.SharedToken, "relay shared token")
	_ = fs.Parse(args)

	nodeCfg := cfg.Node
	if nodeCfg == nil || strings.TrimSpace(nodeCfg.NodeType) != "yt_relay_source" {
		fatalf("enroll a yt_relay_source node first with `stoarama node enroll ...`")
	}
	merged := defaultYTRelaySourceConfig(cfg)
	merged.PublicBaseURL = strings.TrimSpace(*publicBaseURL)
	merged.BindAddr = strings.TrimSpace(*bindAddr)
	merged.ShardID = strings.TrimSpace(*shardID)
	merged.Capacity = *capacity
	merged.CookiesFile = strings.TrimSpace(*cookiesFile)
	merged.CookiesFromBrowser = strings.TrimSpace(*cookiesFromBrowser)
	merged.SharedToken = strings.TrimSpace(*sharedToken)
	if strings.TrimSpace(merged.SharedToken) == "" {
		merged.SharedToken = mustGenerateSecret(24)
	}
	if strings.TrimSpace(merged.PublicBaseURL) == "" {
		fatalf("--public-base-url is required")
	}
	if strings.TrimSpace(merged.CookiesFile) == "" && strings.TrimSpace(merged.CookiesFromBrowser) == "" {
		fatalf("set --cookies-file or --cookies-from-browser")
	}
	cfg.YTRelaySource = merged
	if err := saveCLIConfig(cfg); err != nil {
		fatalf("save config: %v", err)
	}

	exePath, err := os.Executable()
	if err != nil {
		fatalf("resolve executable: %v", err)
	}
	homeDir, err := os.UserHomeDir()
	if err != nil {
		fatalf("resolve home dir: %v", err)
	}
	agentsDir := filepath.Join(homeDir, "Library", "LaunchAgents")
	logDir := filepath.Join(homeDir, "Library", "Logs", "stoarama")
	if err := os.MkdirAll(agentsDir, 0o755); err != nil {
		fatalf("create launch agents dir: %v", err)
	}
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		fatalf("create log dir: %v", err)
	}
	plistPath := filepath.Join(agentsDir, ytRelaySourceLaunchdLabel+".plist")
	plist := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
  <dict>
    <key>Label</key>
    <string>%s</string>
    <key>ProgramArguments</key>
    <array>
      <string>%s</string>
      <string>node</string>
      <string>yt-relay-source</string>
      <string>run</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <dict>
      <key>SuccessfulExit</key>
      <false/>
    </dict>
    <key>WorkingDirectory</key>
    <string>%s</string>
    <key>EnvironmentVariables</key>
    <dict>
      <key>PATH</key>
      <string>/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin</string>
    </dict>
    <key>ProcessType</key>
    <string>Background</string>
    <key>StandardOutPath</key>
    <string>%s</string>
    <key>StandardErrorPath</key>
    <string>%s</string>
  </dict>
</plist>
`, ytRelaySourceLaunchdLabel, exePath, filepath.Dir(exePath), filepath.Join(logDir, "youtube-relay-source.out.log"), filepath.Join(logDir, "youtube-relay-source.err.log"))
	if err := os.WriteFile(plistPath, []byte(plist), 0o644); err != nil {
		fatalf("write plist: %v", err)
	}
	domain := fmt.Sprintf("gui/%d", os.Getuid())
	_ = exec.Command("launchctl", "bootout", domain, plistPath).Run()
	if out, err := exec.Command("launchctl", "bootstrap", domain, plistPath).CombinedOutput(); err != nil {
		fatalf("launchctl bootstrap: %v: %s", err, strings.TrimSpace(string(out)))
	}
	_, _ = exec.Command("launchctl", "enable", domain+"/"+ytRelaySourceLaunchdLabel).CombinedOutput()
	if out, err := exec.Command("launchctl", "kickstart", "-k", domain+"/"+ytRelaySourceLaunchdLabel).CombinedOutput(); err != nil {
		fatalf("launchctl kickstart: %v: %s", err, strings.TrimSpace(string(out)))
	}
	printJSON(map[string]any{
		"ok":         true,
		"label":      ytRelaySourceLaunchdLabel,
		"plist_path": plistPath,
		"log_dir":    logDir,
	})
}

func runNodeYTRelaySourceUninstallLaunchd(_ []string) {
	if runtime.GOOS != "darwin" {
		fatalf("uninstall-launchd is only supported on macOS")
	}
	homeDir, err := os.UserHomeDir()
	if err != nil {
		fatalf("resolve home dir: %v", err)
	}
	plistPath := filepath.Join(homeDir, "Library", "LaunchAgents", ytRelaySourceLaunchdLabel+".plist")
	domain := fmt.Sprintf("gui/%d", os.Getuid())
	_ = exec.Command("launchctl", "bootout", domain, plistPath).Run()
	_ = exec.Command("launchctl", "disable", domain+"/"+ytRelaySourceLaunchdLabel).Run()
	_ = os.Remove(plistPath)
	printJSON(map[string]any{
		"ok":         true,
		"label":      ytRelaySourceLaunchdLabel,
		"plist_path": plistPath,
	})
}

type nodeMeResponse struct {
	Node struct {
		ID          int64  `json:"id"`
		NodeType    string `json:"node_type"`
		DisplayName string `json:"display_name"`
		Hostname    string `json:"hostname"`
		Platform    string `json:"platform"`
	} `json:"node"`
}

func mustLoadNodeInfo(baseURL, token string) nodeMeResponse {
	var resp nodeMeResponse
	if err := apiRequest("GET", normalizeBaseURL(baseURL)+"/api/v1/node/me", nil, token, &resp); err != nil {
		fatalf("load node info: %v", err)
	}
	return resp
}

func defaultYTRelaySourceConfig(cfg cliConfig) *cliYTRelaySourceConfig {
	if cfg.YTRelaySource != nil {
		cp := *cfg.YTRelaySource
		if cp.BindAddr == "" {
			cp.BindAddr = "0.0.0.0:18080"
		}
		if cp.ShardID == "" {
			cp.ShardID = "yt-account-1"
		}
		if cp.Capacity <= 0 {
			cp.Capacity = 4
		}
		if cp.HeartbeatSec <= 0 {
			cp.HeartbeatSec = 15
		}
		if cp.LeaseSec <= 0 {
			cp.LeaseSec = 45
		}
		if cp.RefreshSec <= 0 {
			cp.RefreshSec = 20
		}
		if cp.ResolveTimeoutSec <= 0 {
			cp.ResolveTimeoutSec = 60
		}
		if cp.ResolveFailureThreshold <= 0 {
			cp.ResolveFailureThreshold = 3
		}
		if cp.CacheFile == "" {
			cp.CacheFile = defaultYTRelaySourceCacheFile()
		}
		if cp.NetworkTransport == "" {
			cp.NetworkTransport = "wireguard"
		}
		if cp.TopologyID == "" {
			cp.TopologyID = "stoarama-youtube-relay"
		}
		if cp.TopologyRole == "" {
			cp.TopologyRole = "source"
		}
		if cp.HubServerID == "" {
			cp.HubServerID = cp.TopologyID
		}
		if cp.WGInterface == "" {
			cp.WGInterface = "wg0"
		}
		return &cp
	}
	return &cliYTRelaySourceConfig{
		BindAddr:                "0.0.0.0:18080",
		ShardID:                 "yt-account-1",
		Capacity:                4,
		HeartbeatSec:            15,
		LeaseSec:                45,
		RefreshSec:              20,
		ResolveTimeoutSec:       60,
		ResolveFailureThreshold: 3,
		CacheFile:               defaultYTRelaySourceCacheFile(),
		NetworkTransport:        "wireguard",
		TopologyID:              "stoarama-youtube-relay",
		TopologyRole:            "source",
		HubServerID:             "stoarama-youtube-relay",
		WGInterface:             "wg0",
	}
}

func defaultYTRelaySourceCacheFile() string {
	dir, err := os.UserConfigDir()
	if err != nil {
		return "youtube-relay-source-cache.json"
	}
	return filepath.Join(dir, "stoarama", "youtube-relay-source-cache.json")
}

func mustGenerateSecret(n int) string {
	if n <= 0 {
		n = 24
	}
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		fatalf("generate secret: %v", err)
	}
	return strings.TrimRight(base64.RawURLEncoding.EncodeToString(buf), "=")
}
