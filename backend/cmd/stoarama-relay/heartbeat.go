package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/daydemir/stoarama/backend/internal/recordingapi"
)

const heartbeatInterval = 30 * time.Second
const offlineDiagnosticLimit = 8
const offlineDiagnosticMaxBytes = 8 << 10

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

func (d *heartbeatDiagnostics) Succeeded() error {
	if d == nil {
		return nil
	}
	if d.current == nil {
		return nil
	}
	now := time.Now().UTC()
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
}

func loadFFmpegTelemetry(binDir string) *ffmpegTelemetry {
	active := relayFFmpegBin(binDir)
	result := &ffmpegTelemetry{
		version:      ffmpegVersion(active),
		networkProbe: ffmpegNetworkProbe(active),
	}
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
func relayHeartbeatLoop(ctx context.Context, client *recordingapi.Client, pr *probe, active *atomic.Int64, cfg relayConfig, diag relayDiagnostics, startedAt time.Time, firstSent chan<- struct{}) {
	diagnosticsPath := ""
	if home, err := stoaramaHome(); err == nil {
		diagnosticsPath = filepath.Join(home, "offline-diagnostics.json")
	}
	heartbeatDiag, err := loadHeartbeatDiagnostics(diagnosticsPath)
	if err != nil {
		log.Printf("relay diagnostics load error: %v", err)
		heartbeatDiag = &heartbeatDiagnostics{path: diagnosticsPath}
	}
	bd, _ := binDir()
	var ffmpegInfo atomic.Pointer[ffmpegTelemetry]
	go func() { ffmpegInfo.Store(loadFFmpegTelemetry(bd)) }()

	send := func() {
		probe := pr.snapshot()
		mode := "cookieless"
		if experimentalCookieMode() {
			mode = "with_cookies"
		}
		caps := map[string]any{
			"youtube_mode":           mode,
			"active_jobs":            active.Load(),
			"relay_version":          version,
			"relay_started_at":       startedAt,
			"max_concurrent_streams": cfg.Concurrency,
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
		}
		if diag != nil {
			caps["recording_job"] = diag.Snapshot()
		}
		offline, hasOffline := heartbeatDiag.Snapshot()
		if hasOffline {
			caps["offline_diagnostics"] = offline
		}
		hctx, cancel := context.WithTimeout(ctx, 20*time.Second)
		err := client.NodeHeartbeat(hctx, caps)
		cancel()
		if err != nil && ctx.Err() == nil {
			if persistErr := heartbeatDiag.Failed(err); persistErr != nil {
				log.Printf("relay diagnostics persist error: %v", persistErr)
			}
			log.Printf("relay heartbeat error: %v", err)
		} else if err == nil {
			if hasOffline {
				heartbeatDiag.Sent()
			}
			if persistErr := heartbeatDiag.Succeeded(); persistErr != nil {
				log.Printf("relay diagnostics persist error: %v", persistErr)
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
