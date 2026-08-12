package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/daydemir/stoarama/backend/internal/apihttp"
	"github.com/daydemir/stoarama/backend/internal/recordingapi"
	"github.com/daydemir/stoarama/backend/internal/recordingworker"
)

const heartbeatInterval = 30 * time.Second
const dnsProbeInterval = 5 * time.Minute
const dnsProbeTimeout = 2 * time.Second
const offlineDiagnosticLimit = 8
const offlineDiagnosticMaxBytes = 8 << 10
const recoveryStateMaxBytes = 16 << 10
const captureTempScanEntryLimit = 512

var errCaptureTempScanTruncated = errors.New("capture temp scan truncated")

const (
	relayExitClean          = "clean"
	relayExitSelfUpdate     = "self_update"
	relayExitUncleanProcess = "unclean_process"
	relayExitProcessError   = "process_error"
)

type offlineDiagnosticKind string

const heartbeatOutage offlineDiagnosticKind = "heartbeat_outage"

type offlineErrorClass string

const (
	offlineDNS        offlineErrorClass = "dns_failed"
	offlineTimeout    offlineErrorClass = "timeout"
	offlineConnection offlineErrorClass = "connection_failed"
	offlineHTTP       offlineErrorClass = "http_failed"
	offlineOther      offlineErrorClass = "other"
)

type offlineDiagnostic struct {
	Kind         offlineDiagnosticKind `json:"kind"`
	ErrorClass   offlineErrorClass     `json:"error_class"`
	StartedAt    time.Time             `json:"started_at"`
	LastFailedAt time.Time             `json:"last_failed_at"`
	RecoveredAt  *time.Time            `json:"recovered_at,omitempty"`
	FailureCount int                   `json:"failure_count"`
}

type heartbeatDiagnostics struct {
	path    string
	current *offlineDiagnostic
	recent  []offlineDiagnostic
	dirty   bool
}

type relayRecoveryState struct {
	ServiceInstanceID     string    `json:"service_instance_id,omitempty"`
	HeartbeatSuccessCount uint64    `json:"heartbeat_success_count,omitempty"`
	BootID                string    `json:"boot_id"`
	StartedAt             time.Time `json:"started_at"`
	PreviousExit          string    `json:"previous_exit"`
	LastHeartbeatAt       time.Time `json:"last_heartbeat_at,omitempty"`
	LastCaptureAt         time.Time `json:"last_capture_at,omitempty"`
	LastUploadAt          time.Time `json:"last_upload_at,omitempty"`
	LastUpdaterAt         time.Time `json:"last_updater_at,omitempty"`
	LastError             string    `json:"last_error,omitempty"`
	ErrorTail             []string  `json:"error_tail,omitempty"`
}

type networkCounters struct {
	RXBytes   uint64 `json:"rx_bytes"`
	RXPackets uint64 `json:"rx_packets"`
	RXDrops   uint64 `json:"rx_drops"`
	RXErrors  uint64 `json:"rx_errors"`
	TXBytes   uint64 `json:"tx_bytes"`
	TXPackets uint64 `json:"tx_packets"`
	TXDrops   uint64 `json:"tx_drops"`
	TXErrors  uint64 `json:"tx_errors"`
}

type dnsProbeTelemetry struct {
	CheckedAt time.Time         `json:"checked_at"`
	LatencyMS int64             `json:"latency_ms"`
	OK        bool              `json:"ok"`
	Error     offlineErrorClass `json:"error_class,omitempty"`
}

var recoveryStateMu sync.Mutex
var plannedSelfUpdate atomic.Bool

func recoveryStatePath() string {
	home, err := stoaramaHome()
	if err != nil {
		return ""
	}
	return filepath.Join(home, "relay-recovery.json")
}

func loadRecoveryState(path string) (*relayRecoveryState, error) {
	state := &relayRecoveryState{}
	if path == "" {
		return state, nil
	}
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return state, nil
	}
	if err != nil {
		return state, err
	}
	if len(b) > recoveryStateMaxBytes {
		return state, fmt.Errorf("recovery state exceeds %d bytes", recoveryStateMaxBytes)
	}
	if err := json.Unmarshal(b, state); err != nil {
		return state, err
	}
	return state, nil
}

func (s *relayRecoveryState) persist(path string) error {
	if path == "" {
		return nil
	}
	recoveryStateMu.Lock()
	defer recoveryStateMu.Unlock()
	state := *s
	if plannedSelfUpdate.Load() {
		state.PreviousExit = relayExitSelfUpdate
	}
	if len(state.ErrorTail) > 8 {
		state.ErrorTail = state.ErrorTail[len(state.ErrorTail)-8:]
	}
	b, err := json.Marshal(&state)
	if err != nil {
		return err
	}
	if len(b) > recoveryStateMaxBytes {
		return fmt.Errorf("recovery state exceeds %d bytes", recoveryStateMaxBytes)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".new-")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if err := f.Chmod(0o600); err != nil {
		_ = f.Close()
		return err
	}
	if _, err := f.Write(b); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func bootID() string {
	if runtime.GOOS != "linux" {
		return ""
	}
	b, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

func networkCounterSnapshot() (networkCounters, bool) {
	switch runtime.GOOS {
	case "linux":
		return linuxNetworkCounters()
	case "darwin":
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		output, err := exec.CommandContext(ctx, "/usr/sbin/netstat", "-ibn").Output()
		if err != nil {
			return networkCounters{}, false
		}
		counters, err := parseDarwinNetworkCounters(bytes.NewReader(output))
		return counters, err == nil
	default:
		return networkCounters{}, false
	}
}

func linuxNetworkCounters() (networkCounters, bool) {
	file, err := os.Open("/proc/net/dev")
	if err != nil {
		return networkCounters{}, false
	}
	defer file.Close()
	counters, err := parseNetworkCounters(io.LimitReader(file, 64<<10))
	return counters, err == nil
}

func parseNetworkCounters(reader io.Reader) (networkCounters, error) {
	data, err := io.ReadAll(reader)
	if err != nil {
		return networkCounters{}, err
	}
	var total networkCounters
	interfaces := 0
	for _, line := range strings.Split(string(data), "\n") {
		name, fieldsRaw, ok := strings.Cut(line, ":")
		if !ok || strings.TrimSpace(name) == "lo" {
			continue
		}
		fields := strings.Fields(fieldsRaw)
		if len(fields) < 16 {
			continue
		}
		values := make([]uint64, 16)
		valid := true
		for i := range values {
			values[i], err = strconv.ParseUint(fields[i], 10, 64)
			if err != nil {
				valid = false
				break
			}
		}
		if !valid {
			continue
		}
		total.RXBytes += values[0]
		total.RXPackets += values[1]
		total.RXErrors += values[2]
		total.RXDrops += values[3]
		total.TXBytes += values[8]
		total.TXPackets += values[9]
		total.TXErrors += values[10]
		total.TXDrops += values[11]
		interfaces++
	}
	if interfaces == 0 {
		return networkCounters{}, fmt.Errorf("no network interfaces")
	}
	return total, nil
}

func parseDarwinNetworkCounters(reader io.Reader) (networkCounters, error) {
	data, err := io.ReadAll(io.LimitReader(reader, 256<<10))
	if err != nil {
		return networkCounters{}, err
	}
	indexes := map[string]int{}
	perInterface := map[string]networkCounters{}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		if fields[0] == "Name" {
			clear(indexes)
			for index, name := range fields {
				indexes[name] = index
			}
			continue
		}
		if len(indexes) == 0 || strings.HasPrefix(fields[0], "lo") {
			continue
		}
		required := []string{"Ipkts", "Ierrs", "Ibytes", "Opkts", "Oerrs", "Obytes"}
		valid := true
		values := map[string]uint64{}
		for _, name := range required {
			index, ok := indexes[name]
			if !ok || index >= len(fields) {
				valid = false
				break
			}
			values[name], err = strconv.ParseUint(fields[index], 10, 64)
			if err != nil {
				valid = false
				break
			}
		}
		if !valid {
			continue
		}
		current := perInterface[fields[0]]
		current.RXPackets = max(current.RXPackets, values["Ipkts"])
		current.RXErrors = max(current.RXErrors, values["Ierrs"])
		current.RXBytes = max(current.RXBytes, values["Ibytes"])
		current.TXPackets = max(current.TXPackets, values["Opkts"])
		current.TXErrors = max(current.TXErrors, values["Oerrs"])
		current.TXBytes = max(current.TXBytes, values["Obytes"])
		if index, ok := indexes["Drop"]; ok && index < len(fields) {
			if drops, parseErr := strconv.ParseUint(fields[index], 10, 64); parseErr == nil {
				current.TXDrops = max(current.TXDrops, drops)
			}
		}
		perInterface[fields[0]] = current
	}
	if len(perInterface) == 0 {
		return networkCounters{}, fmt.Errorf("no network interfaces")
	}
	var total networkCounters
	for _, counters := range perInterface {
		total.RXBytes += counters.RXBytes
		total.RXPackets += counters.RXPackets
		total.RXErrors += counters.RXErrors
		total.RXDrops += counters.RXDrops
		total.TXBytes += counters.TXBytes
		total.TXPackets += counters.TXPackets
		total.TXErrors += counters.TXErrors
		total.TXDrops += counters.TXDrops
	}
	return total, nil
}

func dnsProbeHost(apiURL string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(apiURL))
	if err != nil {
		return "", err
	}
	host := parsed.Hostname()
	if host == "" {
		return "", fmt.Errorf("API URL has no hostname")
	}
	return host, nil
}

func dnsProbe(ctx context.Context, host string) dnsProbeTelemetry {
	started := time.Now()
	probeCtx, cancel := context.WithTimeout(ctx, dnsProbeTimeout)
	defer cancel()
	_, err := net.DefaultResolver.LookupHost(probeCtx, host)
	return newDNSProbeTelemetry(started, time.Now(), err)
}

func newDNSProbeTelemetry(started, finished time.Time, err error) dnsProbeTelemetry {
	result := dnsProbeTelemetry{
		CheckedAt: finished.UTC(),
		LatencyMS: finished.Sub(started).Milliseconds(),
		OK:        err == nil,
	}
	if err != nil {
		result.Error = classifyOfflineError(err)
	}
	return result
}

func dnsProbeLoop(ctx context.Context, host string, latest *atomic.Pointer[dnsProbeTelemetry]) {
	run := func() {
		result := dnsProbe(ctx, host)
		latest.Store(&result)
	}
	run()
	ticker := time.NewTicker(dnsProbeInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			run()
		}
	}
}

func relayHealthSnapshot(dataDir, tempDir string) map[string]any {
	health := map[string]any{"runtime_goos": runtime.GOOS, "runtime_goarch": runtime.GOARCH}
	if id := bootID(); id != "" {
		health["boot_id"] = id
	}
	if load, err := os.ReadFile("/proc/loadavg"); err == nil {
		fields := strings.Fields(string(load))
		if len(fields) > 0 {
			health["load_1m"] = fields[0]
		}
	}
	if mem, err := os.ReadFile("/proc/meminfo"); err == nil {
		for _, line := range strings.Split(string(mem), "\n") {
			fields := strings.Fields(line)
			if len(fields) == 3 && (fields[0] == "MemAvailable:" || fields[0] == "MemTotal:") {
				if value, err := strconv.ParseInt(fields[1], 10, 64); err == nil {
					health[strings.TrimSuffix(fields[0], ":")+"_kb"] = value
				}
			}
		}
	}
	if network, ok := networkCounterSnapshot(); ok {
		health["network"] = network
	}
	var stat syscall.Statfs_t
	if dataDir != "" {
		if err := syscall.Statfs(dataDir, &stat); err == nil && stat.Blocks > 0 {
			free := stat.Bavail * uint64(stat.Bsize)
			health["disk_free_bytes"] = free
			if reason := relayCapacityBlock(free); reason != "" {
				health["capacity_blocked"] = reason
			}
		}
	}
	if tempDir != "" {
		if bytes, dirs, truncated, err := directoryUsage(tempDir); err == nil {
			health["capture_temp_bytes"] = bytes
			health["capture_temp_directories"] = dirs
			if truncated {
				health["capture_temp_scan_truncated"] = true
			}
		}
	}
	return health
}

func relayCapacityBlock(free uint64) string {
	if free < relayMinLeaseFreeBytes {
		return "disk_pressure"
	}
	return ""
}

func directoryUsage(root string) (uint64, int, bool, error) {
	var bytes uint64
	dirs := 0
	entries := 0
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			if removedDuringDirectoryScan(root, path, err) {
				return nil
			}
			return err
		}
		if path == root {
			return nil
		}
		entries++
		if entries > captureTempScanEntryLimit {
			return errCaptureTempScanTruncated
		}
		if entry.IsDir() {
			dirs++
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}
		bytes += uint64(info.Size())
		return nil
	})
	if errors.Is(err, errCaptureTempScanTruncated) {
		return bytes, dirs, true, nil
	}
	return bytes, dirs, false, err
}

func removedDuringDirectoryScan(root, path string, err error) bool {
	return path != root && errors.Is(err, os.ErrNotExist)
}

func appendDiagnosticErrors(existing, incoming []string) []string {
	for _, raw := range incoming {
		value := recordingworker.SanitizeDiagnosticError(errors.New(raw))
		if value == "" {
			continue
		}
		for i := len(existing) - 1; i >= 0; i-- {
			if existing[i] == value {
				existing = append(existing[:i], existing[i+1:]...)
			}
		}
		existing = append(existing, value)
	}
	if len(existing) > 8 {
		existing = existing[len(existing)-8:]
	}
	return existing
}

func markRelayExit(reason string) {
	if reason == relayExitSelfUpdate {
		plannedSelfUpdate.Store(true)
	}
	path := recoveryStatePath()
	if path == "" {
		return
	}
	state, err := loadRecoveryState(path)
	if err != nil {
		log.Printf("relay recovery state load error: %v", err)
		state = &relayRecoveryState{}
	}
	state.PreviousExit = reason
	if err := state.persist(path); err != nil {
		log.Printf("relay recovery state persist error: %v", err)
	}
}

func recordSystemdExit(args []string) error {
	values := map[string]string{}
	for _, arg := range args {
		name, value, ok := strings.Cut(arg, "=")
		if !ok {
			continue
		}
		switch name {
		case "--service-result", "--exit-code", "--exit-status":
			value = strings.TrimSpace(value)
			if !safeExitFact(value) {
				continue
			}
			values[name] = value
		}
	}
	result := values["--service-result"]
	if result == "" {
		return nil
	}
	reason := relayExitClean
	if result != "success" {
		reason = "systemd_" + result
		if value := values["--exit-code"]; value != "" {
			reason += "_" + value
		}
		if value := values["--exit-status"]; value != "" {
			reason += "_" + value
		}
	}
	path := recoveryStatePath()
	if path == "" {
		return nil
	}
	state, err := loadRecoveryState(path)
	if err != nil {
		state = &relayRecoveryState{}
	}
	if state.PreviousExit == relayExitSelfUpdate {
		return nil
	}
	state.PreviousExit = reason
	return state.persist(path)
}

func safeExitFact(value string) bool {
	if value == "" || len(value) > 32 {
		return false
	}
	for _, char := range value {
		if (char < 'a' || char > 'z') &&
			(char < 'A' || char > 'Z') &&
			(char < '0' || char > '9') &&
			char != '_' && char != '-' {
			return false
		}
	}
	return true
}

func restartAfterSelfUpdate() error {
	markRelayExit(relayExitSelfUpdate)
	if err := restartService(); err != nil {
		plannedSelfUpdate.Store(false)
		markRelayExit(relayExitUncleanProcess)
		return err
	}
	return nil
}

func loadHeartbeatDiagnostics(path string) (*heartbeatDiagnostics, error) {
	d := &heartbeatDiagnostics{path: path}
	if path == "" {
		return d, nil
	}
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return d, nil
	}
	if err != nil {
		return d, err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, offlineDiagnosticMaxBytes+1))
	if err != nil {
		return d, err
	}
	if len(b) > offlineDiagnosticMaxBytes {
		return d, fmt.Errorf("diagnostics file exceeds %d bytes", offlineDiagnosticMaxBytes)
	}
	var events []offlineDiagnostic
	decoder := json.NewDecoder(bytes.NewReader(b))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&events); err != nil {
		return d, err
	}
	if len(events) > offlineDiagnosticLimit {
		return d, fmt.Errorf("diagnostics file contains more than %d events", offlineDiagnosticLimit)
	}
	for i := range events {
		if err := events[i].validate(); err != nil {
			return d, fmt.Errorf("diagnostic %d: %w", i, err)
		}
		if events[i].RecoveredAt == nil {
			if i != len(events)-1 {
				return d, fmt.Errorf("only the last diagnostic may be open")
			}
			d.current = &events[i]
			continue
		}
		d.recent = append(d.recent, events[i])
	}
	d.dirty = len(events) > 0
	return d, nil
}

func (d *offlineDiagnostic) validate() error {
	if d.Kind != heartbeatOutage {
		return fmt.Errorf("invalid kind")
	}
	switch d.ErrorClass {
	case offlineDNS, offlineTimeout, offlineConnection, offlineHTTP, offlineOther:
	default:
		return fmt.Errorf("invalid error class")
	}
	if d.StartedAt.IsZero() || d.LastFailedAt.Before(d.StartedAt) || d.FailureCount < 1 {
		return fmt.Errorf("invalid outage fields")
	}
	if d.RecoveredAt != nil && d.RecoveredAt.Before(d.LastFailedAt) {
		return fmt.Errorf("invalid recovery time")
	}
	return nil
}

func (d *heartbeatDiagnostics) Failed(err error) error {
	if d == nil || err == nil {
		return nil
	}
	now := time.Now().UTC()
	first := d.current == nil
	if d.current == nil {
		d.current = &offlineDiagnostic{
			Kind:      heartbeatOutage,
			StartedAt: now,
		}
	}
	if now.Before(d.current.LastFailedAt) {
		now = d.current.LastFailedAt
	}
	d.current.ErrorClass = classifyOfflineError(err)
	d.current.LastFailedAt = now
	d.current.FailureCount++
	d.dirty = true
	if first {
		return d.persist()
	}
	return nil
}

func (d *heartbeatDiagnostics) SucceededAt(now time.Time) error {
	if d == nil {
		return nil
	}
	if d.current == nil {
		return nil
	}
	now = now.UTC()
	if now.Before(d.current.LastFailedAt) {
		now = d.current.LastFailedAt
	}
	d.current.RecoveredAt = &now
	d.recent = append(d.recent, *d.current)
	if len(d.recent) > offlineDiagnosticLimit {
		d.recent = d.recent[len(d.recent)-offlineDiagnosticLimit:]
	}
	d.current = nil
	d.dirty = true
	return d.persist()
}

func (d *heartbeatDiagnostics) Succeeded() error {
	return d.SucceededAt(time.Now().UTC())
}

func (d *heartbeatDiagnostics) SnapshotForAttempt(recoveredAt time.Time) ([]offlineDiagnostic, bool) {
	events, ok := d.Snapshot()
	if !ok || d.current == nil || len(events) == 0 {
		return events, ok
	}
	recoveredAt = recoveredAt.UTC()
	if recoveredAt.Before(d.current.LastFailedAt) {
		recoveredAt = d.current.LastFailedAt
	}
	events[len(events)-1].RecoveredAt = &recoveredAt
	return events, true
}

func (d *heartbeatDiagnostics) Snapshot() ([]offlineDiagnostic, bool) {
	if d == nil {
		return nil, false
	}
	if !d.dirty {
		return nil, false
	}
	events := d.events()
	return events, true
}

func (d *heartbeatDiagnostics) events() []offlineDiagnostic {
	events := append([]offlineDiagnostic(nil), d.recent...)
	if d.current != nil {
		events = append(events, *d.current)
	}
	if len(events) > offlineDiagnosticLimit {
		events = events[len(events)-offlineDiagnosticLimit:]
	}
	return events
}

func (d *heartbeatDiagnostics) Sent() {
	if d == nil {
		return
	}
	d.dirty = false
}

func (d *heartbeatDiagnostics) persist() error {
	if d == nil || d.path == "" {
		return nil
	}
	b, err := json.Marshal(d.events())
	if err != nil {
		return err
	}
	if len(b) > offlineDiagnosticMaxBytes {
		return fmt.Errorf("diagnostics payload exceeds %d bytes", offlineDiagnosticMaxBytes)
	}
	if err := os.MkdirAll(filepath.Dir(d.path), 0o700); err != nil {
		return err
	}
	tmp := d.path + ".new"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	if err := os.Chmod(tmp, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, d.path)
}

func classifyOfflineError(err error) offlineErrorClass {
	if err == nil {
		return offlineOther
	}
	message := strings.ToLower(err.Error())
	switch {
	case strings.Contains(message, "lookup "), strings.Contains(message, "no such host"), strings.Contains(message, "dns"):
		return offlineDNS
	case strings.Contains(message, "timeout"), strings.Contains(message, "deadline exceeded"):
		return offlineTimeout
	case strings.Contains(message, "dial tcp"), strings.Contains(message, "connection"), strings.Contains(message, "network is unreachable"), strings.Contains(message, "no route to host"), strings.Contains(message, "tls handshake"):
		return offlineConnection
	case strings.Contains(message, "status="):
		return offlineHTTP
	default:
		return offlineOther
	}
}

type relayDiagnostics interface {
	Snapshot() map[string]any
}

type ffmpegTelemetry struct {
	version       string
	networkProbe  string
	systemVersion string
	systemProbe   string
	runtime       ffmpegRuntimeEvidence
}

const ffmpegTelemetryRefreshInterval = time.Minute

func loadFFmpegTelemetry(binDir string) *ffmpegTelemetry {
	active := relayFFmpegBin(binDir)
	result := &ffmpegTelemetry{
		version:      ffmpegVersion(active),
		networkProbe: ffmpegNetworkProbe(active),
	}
	result.runtime = attestFFmpegRuntime(binDir, active, result.version, result.networkProbe)
	if active == "/usr/bin/ffmpeg" {
		result.systemVersion = result.version
		result.systemProbe = result.networkProbe
		return result
	}
	result.systemVersion = ffmpegVersion("/usr/bin/ffmpeg")
	result.systemProbe = ffmpegNetworkProbe("/usr/bin/ffmpeg")
	return result
}

// relayHeartbeatLoop reports this relay's liveness and current in-memory state every
// 30s. External probes run independently so a slow resolver cannot block liveness.
// POST /api/v1/node/heartbeat sets last_heartbeat_at and merges the reported keys into
// nodes.capabilities_jsonb.
func relayHeartbeatLoop(ctx context.Context, client *recordingapi.Client, pr *probe, active *atomic.Int64, diag relayDiagnostics, startedAt time.Time, tempDir, apiURL string, firstSent chan<- struct{}) {
	diagnosticsPath := ""
	relayDataDir := ""
	if home, err := stoaramaHome(); err == nil {
		relayDataDir = home
		diagnosticsPath = filepath.Join(home, "offline-diagnostics.json")
	}
	recoveryPath := recoveryStatePath()
	recovery, recoveryErr := loadRecoveryState(recoveryPath)
	if recoveryErr != nil {
		log.Printf("relay recovery state load error: %v", recoveryErr)
		recovery = &relayRecoveryState{}
	}
	previousRecovery := *recovery
	recovery.BootID = bootID()
	recovery.StartedAt = startedAt
	recovery.ServiceInstanceID = os.Getenv("STOARAMA_SERVICE_INSTANCE_ID")
	recoveryPending, reportedPreviousExit := classifyPreviousRelayExit(previousRecovery, recovery.BootID)
	recovery.PreviousExit = relayExitUncleanProcess
	if err := recovery.persist(recoveryPath); err != nil {
		log.Printf("relay recovery state persist error: %v", err)
	}
	heartbeatDiag, err := loadHeartbeatDiagnostics(diagnosticsPath)
	if err != nil {
		log.Printf("relay diagnostics load error: %v", err)
		heartbeatDiag = &heartbeatDiagnostics{path: diagnosticsPath}
	}
	bd, _ := binDir()
	var ffmpegInfo atomic.Pointer[ffmpegTelemetry]
	var dnsInfo atomic.Pointer[dnsProbeTelemetry]
	go refreshFFmpegTelemetry(ctx, bd, ffmpegTelemetryRefreshInterval, &ffmpegInfo, loadFFmpegTelemetry)
	if host, err := dnsProbeHost(apiURL); err != nil {
		log.Printf("relay DNS probe disabled: %v", err)
	} else {
		go dnsProbeLoop(ctx, host, &dnsInfo)
	}

	send := func() {
		probe := pr.snapshot()
		mode := "cookieless"
		if experimentalCookieMode() {
			mode = "with_cookies"
		}
		health := relayHealthSnapshot(relayDataDir, tempDir)
		if info := dnsInfo.Load(); info != nil {
			health["dns_probe"] = info
		}
		caps := map[string]any{
			"youtube_mode":             mode,
			"active_jobs":              active.Load(),
			"relay_version":            version,
			"relay_source_revision":    sourceRevision,
			"relay_started_at":         startedAt,
			"health":                   health,
			"continuous_source_pts_v1": true,
		}
		if recoveryPending {
			caps["recovery"] = map[string]any{"recovered_at": time.Now().UTC(), "previous_exit": reportedPreviousExit, "boot_id": previousRecovery.BootID, "started_at": previousRecovery.StartedAt, "last_heartbeat_at": previousRecovery.LastHeartbeatAt, "last_capture_at": previousRecovery.LastCaptureAt, "last_upload_at": previousRecovery.LastUploadAt, "last_updater_at": previousRecovery.LastUpdaterAt, "error_tail": previousRecovery.ErrorTail, "log_tail": relayLogTail(relayLogPath(relayDataDir))}
		}
		if probe.ranOnce {
			caps["youtube_ready"] = probe.ready
			caps["youtube_error"] = probe.err
		}
		if probe.version != "" {
			caps["ytdlp_version"] = probe.version
		}
		if info := ffmpegInfo.Load(); info != nil {
			caps["ffmpeg_version"] = info.version
			caps["ffmpeg_network_probe"] = info.networkProbe
			caps["system_ffmpeg_version"] = info.systemVersion
			caps["system_ffmpeg_probe"] = info.systemProbe
			caps["ffmpeg_runtime"] = info.runtime
		}
		if diag != nil {
			recording := diag.Snapshot()
			caps["recording_job"] = recording
			if value, ok := recording["last_capture_at"].(string); ok {
				if parsed, parseErr := time.Parse(time.RFC3339Nano, value); parseErr == nil {
					recovery.LastCaptureAt = parsed
				}
			}
			if value, ok := recording["last_upload_at"].(string); ok {
				if parsed, parseErr := time.Parse(time.RFC3339Nano, value); parseErr == nil {
					recovery.LastUploadAt = parsed
				}
			}
			if errors, ok := recording["error_tail"].([]string); ok {
				recovery.ErrorTail = appendDiagnosticErrors(recovery.ErrorTail, errors)
			}
		}
		if updater := lastUpdaterUnix.Load(); updater > 0 {
			recovery.LastUpdaterAt = time.Unix(0, updater).UTC()
		}
		recoveredAt := time.Now().UTC()
		offline, hasOffline := heartbeatDiag.SnapshotForAttempt(recoveredAt)
		if hasOffline {
			caps["offline_diagnostics"] = offline
		}
		err := sendNodeHeartbeat(ctx, client, caps)
		if err != nil && ctx.Err() == nil {
			sanitized := recordingworker.SanitizeDiagnosticError(err)
			recovery.LastError = sanitized
			recovery.ErrorTail = appendDiagnosticErrors(recovery.ErrorTail, []string{sanitized})
			if persistErr := recovery.persist(recoveryPath); persistErr != nil {
				log.Printf("relay recovery state persist error: %v", persistErr)
			}
			if persistErr := heartbeatDiag.Failed(err); persistErr != nil {
				log.Printf("relay diagnostics persist error: %v", persistErr)
			}
			log.Printf("relay heartbeat error: %s", sanitized)
		} else if err == nil {
			recovery.LastHeartbeatAt = time.Now().UTC()
			recovery.HeartbeatSuccessCount++
			recovery.LastError = ""
			if persistErr := recovery.persist(recoveryPath); persistErr != nil {
				log.Printf("relay recovery state persist error: %v", persistErr)
			} else {
				recoveryPending = false
			}
			if persistErr := heartbeatDiag.SucceededAt(recoveredAt); persistErr != nil {
				log.Printf("relay diagnostics persist error: %v", persistErr)
			} else if hasOffline {
				heartbeatDiag.Sent()
			}
		}
	}

	send()
	close(firstSent)
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			send()
		}
	}
}

func refreshFFmpegTelemetry(ctx context.Context, binDir string, interval time.Duration, destination *atomic.Pointer[ffmpegTelemetry], load func(string) *ffmpegTelemetry) {
	if interval <= 0 {
		return
	}
	refresh := func() { destination.Store(load(binDir)) }
	refresh()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			refresh()
		}
	}
}

func classifyPreviousRelayExit(previous relayRecoveryState, currentBootID string) (bool, string) {
	if previous.PreviousExit == relayExitClean || previous.PreviousExit == relayExitSelfUpdate {
		return false, ""
	}
	if previous.StartedAt.IsZero() {
		if previous.PreviousExit == "" {
			return false, ""
		}
		return true, "unknown"
	}
	if previous.BootID != "" && currentBootID != "" && previous.BootID != currentBootID {
		return true, "unclean_reboot"
	}
	if previous.PreviousExit == "" {
		return true, relayExitUncleanProcess
	}
	return true, previous.PreviousExit
}

func relayLogTail(path string) []string {
	if path == "" {
		return nil
	}
	file, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer file.Close()
	const maxTailBytes = 64 << 10
	info, err := file.Stat()
	if err != nil {
		return nil
	}
	offset := max(int64(0), info.Size()-maxTailBytes)
	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		return nil
	}
	data, err := io.ReadAll(io.LimitReader(file, maxTailBytes))
	if err != nil {
		return nil
	}
	lines := strings.Split(string(data), "\n")
	tail := make([]string, 0, 8)
	for _, line := range lines {
		if category := relayLogCategory(line); category != "" {
			tail = append(tail, category)
		}
	}
	if len(tail) > 8 {
		tail = tail[len(tail)-8:]
	}
	return tail
}

func relayLogCategory(line string) string {
	line = strings.ToLower(line)
	relayCategories := [...]struct {
		needle string
		label  string
	}{
		{"relay heartbeat error", "relay.heartbeat_error"},
		{"relay recovery state", "relay.recovery_state_error"},
		{"relay diagnostics", "relay.diagnostics_error"},
		{"relay self-update", "relay.self_update"},
		{"stoarama-relay removed", "relay.temp_cleanup"},
		{"stoarama-relay run", "relay.started"},
	}
	for _, category := range relayCategories {
		if strings.Contains(line, category.needle) {
			return category.label
		}
	}
	if !strings.Contains(line, "recording worker") {
		return ""
	}
	categories := [...]struct {
		needle string
		label  string
	}{
		{"segment delivery failed", "recording_worker.segment_delivery_failed"},
		{"continuous resolve failed", "recording_worker.resolve_failed"},
		{"continuous source dropped", "recording_worker.source_dropped"},
		{"continuous ssrf guard rejected", "recording_worker.ssrf_rejected"},
		{"lease paused", "recording_worker.lease_paused"},
		{"lease expired", "recording_worker.lease_expired"},
		{"lease error", "recording_worker.lease_error"},
		{"heartbeat error", "recording_worker.heartbeat_error"},
		{"disk check failed", "recording_worker.disk_check_failed"},
		{"surrender failed", "recording_worker.surrender_failed"},
		{"complete failed", "recording_worker.complete_failed"},
		{"recording worker", "recording_worker.event"},
	}
	for _, category := range categories {
		if strings.Contains(line, category.needle) {
			return category.label
		}
	}
	return ""
}

func sendNodeHeartbeat(ctx context.Context, client *recordingapi.Client, caps map[string]any) error {
	const attempts = 3
	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		hctx, cancel := context.WithTimeout(ctx, 6*time.Second)
		lastErr = client.NodeHeartbeat(hctx, caps)
		cancel()
		if lastErr == nil || ctx.Err() != nil || !retryableNodeHeartbeatError(lastErr) {
			return lastErr
		}
		if attempt < attempts {
			timer := time.NewTimer(time.Second)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
	}
	return lastErr
}

func retryableNodeHeartbeatError(err error) bool {
	if err == nil {
		return false
	}
	var statusErr *apihttp.StatusError
	if errors.As(err, &statusErr) {
		return statusErr.Code == 408 || statusErr.Code == 425 || statusErr.Code == 429 || statusErr.Code >= 500
	}
	var networkErr net.Error
	return errors.Is(err, context.DeadlineExceeded) ||
		(errors.As(err, &networkErr) && (networkErr.Timeout() || networkErr.Temporary()))
}
