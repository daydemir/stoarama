package captureapipersistent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/daydemir/stoarama/backend/internal/capture"
	"github.com/daydemir/stoarama/backend/internal/captureapi"
	"github.com/daydemir/stoarama/backend/internal/model"
	"github.com/daydemir/stoarama/backend/internal/settings"
)

type ManagerConfig struct {
	WorkerID                  string
	ServerID                  string
	RefreshInterval           time.Duration
	ProcessHeartbeatInterval  time.Duration
	ProcessHeartbeatLeaseSec  int
	ProcessStartReason        string
	ProcessTelemetry          bool
	MaxSessions               int
	MaxFrameBytes             int
	FrameQueueSize            int
	FrameEnqueueTimeout       time.Duration
	FrameWriterWorkers        int
	StreamIDs                 []int64
	PreferAssignedStreamIDs   bool
	ModeAllowlist             []capture.Mode
	RecordingHeartbeat        bool
	UnsupportedErrorThreshold int
	Registry                  *capture.Registry
}

type Manager struct {
	client       *captureapi.Client
	cfg          ManagerConfig
	streamFilter map[int64]struct{}
	modeFilter   map[capture.Mode]struct{}

	mu       sync.Mutex
	sessions map[int64]*sessionState
}

type streamConfig struct {
	ID                 int64
	Provider           string
	StreamURL          string
	SourcePageURL      string
	CaptureMode        capture.Mode
	AssignmentRevision int64
	Assigned           bool
	CaptureIntervalSec int
	CaptureConfig      map[string]any
	fingerprint        string
}

type sessionState struct {
	cfg    streamConfig
	cancel context.CancelFunc
	done   chan struct{}
}

type frameEvent struct {
	frame       capture.Frame
	capturedAt  time.Time
	effective   capture.Mode
	resolvedURL string
}

type recordingProcessReporter struct {
	client      *captureapi.Client
	stream      streamConfig
	mode        capture.Mode
	serverID    string
	workerID    string
	processID   string
	leaseSec    int
	interval    time.Duration
	startReason string

	mu           sync.Mutex
	lastFrameAt  *time.Time
	lastError    string
	restartCount int
}

func NewManager(client *captureapi.Client, cfg ManagerConfig) *Manager {
	if client == nil {
		panic("capture API client is nil")
	}
	if cfg.RefreshInterval <= 0 {
		cfg.RefreshInterval = 5 * time.Second
	}
	if cfg.MaxFrameBytes <= 0 {
		cfg.MaxFrameBytes = 25 << 20
	}
	if cfg.FrameQueueSize <= 0 {
		cfg.FrameQueueSize = 64
	}
	if cfg.FrameEnqueueTimeout <= 0 {
		cfg.FrameEnqueueTimeout = 3 * time.Second
	}
	if cfg.FrameWriterWorkers <= 0 {
		cfg.FrameWriterWorkers = 2
	}
	if cfg.ProcessHeartbeatInterval <= 0 {
		cfg.ProcessHeartbeatInterval = 15 * time.Second
	}
	if cfg.ProcessHeartbeatLeaseSec <= 0 {
		cfg.ProcessHeartbeatLeaseSec = 45
	}
	if cfg.ProcessHeartbeatLeaseSec > 3600 {
		cfg.ProcessHeartbeatLeaseSec = 3600
	}
	if strings.TrimSpace(cfg.ProcessStartReason) == "" {
		cfg.ProcessStartReason = "capture_session_start"
	}
	if strings.TrimSpace(cfg.WorkerID) == "" {
		cfg.WorkerID = "capture-api-persistent-1"
	}
	if cfg.UnsupportedErrorThreshold <= 0 {
		cfg.UnsupportedErrorThreshold = 8
	}
	if cfg.Registry == nil {
		reg, err := capture.NewDefaultRegistry()
		if err != nil {
			panic(fmt.Sprintf("init capture registry: %v", err))
		}
		cfg.Registry = reg
	}

	filter := make(map[int64]struct{}, len(cfg.StreamIDs))
	for _, id := range cfg.StreamIDs {
		if id > 0 {
			filter[id] = struct{}{}
		}
	}
	modeFilter := make(map[capture.Mode]struct{}, len(cfg.ModeAllowlist))
	for _, m := range cfg.ModeAllowlist {
		norm := capture.NormalizeMode(string(m))
		if norm == capture.ModeUnsupported {
			continue
		}
		modeFilter[norm] = struct{}{}
	}

	return &Manager{
		client:       client,
		cfg:          cfg,
		streamFilter: filter,
		modeFilter:   modeFilter,
		sessions:     make(map[int64]*sessionState),
	}
}

func (m *Manager) Run(ctx context.Context) error {
	log.Printf("capture-api-persistent manager start worker_id=%s refresh=%s max_sessions=%d stream_filter=%d mode_filter=%d",
		m.cfg.WorkerID, m.cfg.RefreshInterval, m.cfg.MaxSessions, len(m.streamFilter), len(m.modeFilter))

	if err := m.reconcile(ctx); err != nil {
		return err
	}

	ticker := time.NewTicker(m.cfg.RefreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			m.stopAll()
			return ctx.Err()
		case <-ticker.C:
			if err := m.reconcile(ctx); err != nil {
				return err
			}
		}
	}
}

func (m *Manager) stopAll() {
	m.mu.Lock()
	sessions := make([]*sessionState, 0, len(m.sessions))
	for id, sess := range m.sessions {
		log.Printf("capture-api-persistent stopping stream_id=%d", id)
		sessions = append(sessions, sess)
		delete(m.sessions, id)
	}
	m.mu.Unlock()

	for _, sess := range sessions {
		sess.cancel()
		<-sess.done
	}
}

func (m *Manager) reconcile(ctx context.Context) error {
	desired, err := m.loadDesiredStreams(ctx)
	if err != nil {
		return err
	}
	desiredByID := make(map[int64]streamConfig, len(desired))
	for _, s := range desired {
		desiredByID[s.ID] = s
	}

	var stopList []*sessionState
	var startList []streamConfig

	m.mu.Lock()
	for id, sess := range m.sessions {
		next, ok := desiredByID[id]
		if !ok || sess.cfg.fingerprint != next.fingerprint {
			stopList = append(stopList, sess)
			delete(m.sessions, id)
			continue
		}
		select {
		case <-sess.done:
			log.Printf("capture-api-persistent stream_id=%d session exited; restarting", id)
			delete(m.sessions, id)
		default:
		}
	}

	for _, s := range desired {
		if _, ok := m.sessions[s.ID]; ok {
			continue
		}
		sessionCtx, cancel := context.WithCancel(ctx)
		done := make(chan struct{})
		state := &sessionState{cfg: s, cancel: cancel, done: done}
		m.sessions[s.ID] = state
		startList = append(startList, s)
		go func(cfg streamConfig, done chan struct{}) {
			defer close(done)
			m.runManagedStream(sessionCtx, cfg)
		}(s, done)
	}
	active := len(m.sessions)
	m.mu.Unlock()

	for _, sess := range stopList {
		sess.cancel()
		<-sess.done
	}

	if len(startList) > 0 || len(stopList) > 0 {
		log.Printf("capture-api-persistent reconcile active=%d started=%d stopped=%d", active, len(startList), len(stopList))
	}
	return nil
}

func (m *Manager) loadDesiredStreams(ctx context.Context) ([]streamConfig, error) {
	recordingSettings, err := m.client.GetRecordingSettings(ctx)
	if err != nil {
		return nil, fmt.Errorf("load recording settings: %w", err)
	}
	_ = recordingSettings
	if len(m.streamFilter) > 0 {
		if m.cfg.PreferAssignedStreamIDs && strings.TrimSpace(m.cfg.ServerID) != "" {
			assigned, assignedErr := m.loadAssignedStreamFilterTargets(ctx, settings.DefaultRecordingIntervalSec)
			if assignedErr != nil {
				return nil, assignedErr
			}
			if len(assigned) > 0 {
				return assigned, nil
			}
		}
		return m.loadStreamFilterTargets(ctx, settings.DefaultRecordingIntervalSec)
	}
	serverID := strings.TrimSpace(m.cfg.ServerID)
	if serverID == "" {
		return nil, fmt.Errorf("server_id is required when stream filter is empty")
	}
	return m.loadAssignedStreams(ctx, settings.DefaultRecordingIntervalSec)
}

func (m *Manager) loadAssignedStreams(ctx context.Context, captureIntervalSec int) ([]streamConfig, error) {
	assignments, err := m.client.ListRecordingAssignments(ctx, strings.TrimSpace(m.cfg.ServerID), "", 1000, 0)
	if err != nil {
		return nil, fmt.Errorf("list recording assignments: %w", err)
	}
	out := make([]streamConfig, 0, len(assignments))
	for _, asn := range assignments {
		s := streamConfigFromAssignment(asn, captureIntervalSec)
		if s.ID <= 0 || (strings.TrimSpace(s.StreamURL) == "" && strings.TrimSpace(s.SourcePageURL) == "") {
			continue
		}
		if !m.modeAllowed(s) {
			continue
		}
		out = append(out, s)
	}
	return prioritizeAndCap(out, m.cfg.MaxSessions), nil
}

func (m *Manager) loadAssignedStreamFilterTargets(ctx context.Context, captureIntervalSec int) ([]streamConfig, error) {
	assignments, err := m.client.ListRecordingAssignments(ctx, strings.TrimSpace(m.cfg.ServerID), "", 1000, 0)
	if err != nil {
		return nil, fmt.Errorf("list recording assignments: %w", err)
	}
	out := make([]streamConfig, 0, len(assignments))
	for _, asn := range assignments {
		if _, ok := m.streamFilter[asn.StreamID]; !ok {
			continue
		}
		s := streamConfigFromAssignment(asn, captureIntervalSec)
		if s.ID <= 0 || (strings.TrimSpace(s.StreamURL) == "" && strings.TrimSpace(s.SourcePageURL) == "") {
			continue
		}
		if !m.modeAllowed(s) {
			continue
		}
		out = append(out, s)
	}
	return prioritizeAndCap(out, m.cfg.MaxSessions), nil
}

func streamConfigFromAssignment(asn captureapi.RecordingAssignment, captureIntervalSec int) streamConfig {
	mode := capture.LegacyModeForStream(asn.CaptureType, asn.ExecutionClass)
	if mode == capture.ModeYouTubeRelay {
		mode = capture.ModeYouTubeLive
	}
	s := streamConfig{
		ID:                 asn.StreamID,
		Provider:           asn.Provider,
		StreamURL:          asn.StreamURL,
		SourcePageURL:      asn.SourcePageURL,
		CaptureMode:        mode,
		AssignmentRevision: asn.AssignmentRevision,
		Assigned:           true,
	}
	cfg := asn.CaptureConfigJSON
	if cfg == nil {
		cfg = map[string]any{}
	} else {
		cloned := make(map[string]any, len(cfg))
		for k, v := range cfg {
			cloned[k] = v
		}
		cfg = cloned
	}
	s.CaptureConfig = cfg
	if captureIntervalSec <= 0 {
		captureIntervalSec = settings.DefaultRecordingIntervalSec
	}
	s.CaptureIntervalSec = captureIntervalSec
	cfgRaw, _ := json.Marshal(s.CaptureConfig)
	s.fingerprint = fmt.Sprintf(
		"%d|%s|%s|%s|%s|%d|%s|%s|%d",
		s.ID, s.Provider, s.StreamURL, s.SourcePageURL, s.CaptureMode, s.CaptureIntervalSec,
		strings.TrimSpace(string(cfgRaw)), strings.TrimSpace(asn.ServerID), asn.AssignmentRevision,
	)
	return s
}

func (m *Manager) loadStreamFilterTargets(ctx context.Context, captureIntervalSec int) ([]streamConfig, error) {
	ids := make([]int64, 0, len(m.streamFilter))
	for id := range m.streamFilter {
		if id > 0 {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	out := make([]streamConfig, 0, len(ids))
	for _, id := range ids {
		st, err := m.client.GetStream(ctx, id)
		if err != nil {
			return nil, fmt.Errorf("load stream_id=%d: %w", id, err)
		}
		if st.ID <= 0 {
			continue
		}
		s := toStreamConfig(st, captureIntervalSec)
		if strings.TrimSpace(s.StreamURL) == "" && strings.TrimSpace(s.SourcePageURL) == "" {
			continue
		}
		if !m.modeAllowed(s) {
			continue
		}
		out = append(out, s)
	}

	return prioritizeAndCap(out, m.cfg.MaxSessions), nil
}

func prioritizeAndCap(streams []streamConfig, maxSessions int) []streamConfig {
	sort.SliceStable(streams, func(i, j int) bool {
		return streams[i].ID < streams[j].ID
	})
	if maxSessions > 0 && len(streams) > maxSessions {
		log.Printf(
			"capture-api-persistent desired streams=%d exceed max_sessions=%d; running first %d streams by id",
			len(streams), maxSessions, maxSessions,
		)
		streams = streams[:maxSessions]
	}
	return streams
}

func toStreamConfig(s model.Stream, captureIntervalSec int) streamConfig {
	cfgRaw, _ := json.Marshal(s.ExecutionConfigJSON)
	mode := capture.LegacyModeForStream(s.CaptureType, s.ExecutionClass)
	if captureIntervalSec <= 0 {
		captureIntervalSec = settings.DefaultRecordingIntervalSec
	}
	return streamConfig{
		ID:                 s.ID,
		Provider:           s.Provider,
		StreamURL:          s.SourceURL,
		SourcePageURL:      s.SourcePageURL,
		CaptureMode:        mode,
		CaptureIntervalSec: captureIntervalSec,
		CaptureConfig:      s.ExecutionConfigJSON,
		fingerprint:        fmt.Sprintf("%d|%s|%s|%s|%s|%d|%s", s.ID, s.Provider, s.SourceURL, s.SourcePageURL, mode, captureIntervalSec, strings.TrimSpace(string(cfgRaw))),
	}
}

func (m *Manager) shouldProcessTelemetry(s streamConfig) bool {
	return m.cfg.ProcessTelemetry &&
		s.Assigned &&
		s.AssignmentRevision > 0 &&
		strings.TrimSpace(m.cfg.ServerID) != ""
}

func (m *Manager) newProcessReporter(s streamConfig, mode capture.Mode) *recordingProcessReporter {
	baseWorkerID := strings.TrimSpace(m.cfg.WorkerID)
	processWorkerID := fmt.Sprintf("%s-stream-%d", baseWorkerID, s.ID)
	return &recordingProcessReporter{
		client:      m.client,
		stream:      s,
		mode:        mode,
		serverID:    strings.TrimSpace(m.cfg.ServerID),
		workerID:    processWorkerID,
		processID:   processWorkerID,
		leaseSec:    m.cfg.ProcessHeartbeatLeaseSec,
		interval:    m.cfg.ProcessHeartbeatInterval,
		startReason: strings.TrimSpace(m.cfg.ProcessStartReason),
	}
}

func (r *recordingProcessReporter) setLastFrame(capturedAt time.Time) {
	if capturedAt.IsZero() {
		capturedAt = time.Now().UTC()
	}
	t := capturedAt.UTC()
	r.mu.Lock()
	r.lastFrameAt = &t
	r.lastError = ""
	r.mu.Unlock()
}

func (r *recordingProcessReporter) setLastError(errText string) {
	r.mu.Lock()
	r.lastError = strings.TrimSpace(errText)
	r.mu.Unlock()
}

func (r *recordingProcessReporter) setRestartCount(n int) {
	if n < 0 {
		n = 0
	}
	r.mu.Lock()
	r.restartCount = n
	r.mu.Unlock()
}

func (r *recordingProcessReporter) snapshot() (*time.Time, string, int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var frameAt *time.Time
	if r.lastFrameAt != nil && !r.lastFrameAt.IsZero() {
		t := r.lastFrameAt.UTC()
		frameAt = &t
	}
	return frameAt, r.lastError, r.restartCount
}

func (r *recordingProcessReporter) sendHeartbeat(ctx context.Context, status string) error {
	frameAt, errText, restartCount := r.snapshot()
	return r.client.RecordingProcessHeartbeat(ctx, captureapi.RecordingProcessHeartbeatRequest{
		StreamID:       r.stream.ID,
		ExecutionClass: capture.ModeToExecutionClass(r.mode),
		ServerID:       r.serverID,
		AssignmentRev:  r.stream.AssignmentRevision,
		ProcessID:      r.processID,
		WorkerID:       r.workerID,
		Status:         status,
		LeaseSec:       r.leaseSec,
		LastFrameAt:    frameAt,
		ErrorText:      errText,
		StartReason:    r.startReason,
		RestartCount:   restartCount,
	})
}

func (r *recordingProcessReporter) run(ctx context.Context) error {
	retryDelay := 5 * time.Second
	if r.interval > 0 && retryDelay > r.interval {
		retryDelay = r.interval
	}
	status := "starting"
	consecutiveFailures := 0
	var firstFailureAt time.Time
	nextDelay := time.Duration(0)
	for {
		if nextDelay > 0 {
			timer := time.NewTimer(nextDelay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil
			case <-timer.C:
			}
		}
		beatCtx, beatCancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := r.sendHeartbeat(beatCtx, status)
		beatCancel()
		if err != nil {
			consecutiveFailures++
			if firstFailureAt.IsZero() {
				firstFailureAt = time.Now().UTC()
			}
			log.Printf(
				"recording process heartbeat degraded stream_id=%d process_id=%s consecutive=%d degraded_for=%s: %v",
				r.stream.ID,
				r.processID,
				consecutiveFailures,
				time.Since(firstFailureAt).Round(time.Second),
				err,
			)
			nextDelay = retryDelay
			continue
		}
		if consecutiveFailures > 0 {
			log.Printf(
				"recording process heartbeat recovered stream_id=%d process_id=%s consecutive=%d degraded_for=%s",
				r.stream.ID,
				r.processID,
				consecutiveFailures,
				time.Since(firstFailureAt).Round(time.Second),
			)
			consecutiveFailures = 0
			firstFailureAt = time.Time{}
		}
		status = "running"
		nextDelay = r.interval
		select {
		case <-ctx.Done():
			return nil
		default:
		}
	}
}

func (r *recordingProcessReporter) stop(ctx context.Context, finalStatus, stopReason string) error {
	_, errText, _ := r.snapshot()
	return r.client.RecordingProcessStopped(ctx, captureapi.RecordingProcessStoppedRequest{
		StreamID:       r.stream.ID,
		ProcessID:      r.processID,
		WorkerID:       r.workerID,
		ServerID:       r.serverID,
		ExecutionClass: capture.ModeToExecutionClass(r.mode),
		AssignmentRev:  r.stream.AssignmentRevision,
		FinalStatus:    finalStatus,
		StopReason:     stopReason,
		ErrorText:      errText,
	})
}

func (m *Manager) runManagedStream(ctx context.Context, s streamConfig) {
	log.Printf("capture-api-persistent stream start stream_id=%d mode=%s", s.ID, s.CaptureMode)
	defer log.Printf("capture-api-persistent stream stop stream_id=%d", s.ID)
	finalStatus := "stopped"
	finalStopReason := "session_end"
	effectiveMode := capture.ModeUnsupported
	var reporter *recordingProcessReporter
	var telemetryDone chan struct{}
	var telemetryErrCh chan error
	runCtx := ctx
	runCancel := func() {}
	defer func() {
		runCancel()
		if telemetryDone != nil {
			<-telemetryDone
		}
		if reporter != nil {
			stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
			if err := reporter.stop(stopCtx, finalStatus, finalStopReason); err != nil {
				log.Printf("capture-api-persistent stream_id=%d process stop telemetry failed: %v", s.ID, err)
			}
			stopCancel()
		}
		if err := m.client.SetRuntimeStopped(context.Background(), s.ID); err != nil {
			log.Printf("capture-api-persistent stream_id=%d runtime stop update failed: %v", s.ID, err)
		}
	}()

	spec := m.buildSpec(s)
	effectiveMode = capture.EffectiveMode(spec)
	if effectiveMode == capture.ModeUnsupported {
		reason := fmt.Sprintf("unsupported capture runtime mapping for provider=%s url=%s", s.Provider, s.StreamURL)
		if err := m.client.MarkUnsupported(context.Background(), s.ID, capture.ModeUnsupported, "", reason); err != nil {
			log.Printf("capture-api-persistent stream_id=%d mark unsupported failed: %v", s.ID, err)
		}
		return
	}

	adapter, ok := m.cfg.Registry.Get(effectiveMode)
	if !ok {
		reason := fmt.Sprintf("capture adapter %q is not registered", effectiveMode)
		if err := m.client.MarkUnsupported(context.Background(), s.ID, effectiveMode, "", reason); err != nil {
			log.Printf("capture-api-persistent stream_id=%d mark unsupported failed: %v", s.ID, err)
		}
		return
	}

	if m.shouldProcessTelemetry(s) {
		reporter = m.newProcessReporter(s, effectiveMode)
		runCtx, runCancel = context.WithCancel(ctx)
		telemetryDone = make(chan struct{})
		telemetryErrCh = make(chan error, 1)
		go func() {
			defer close(telemetryDone)
			if err := reporter.run(runCtx); err != nil && runCtx.Err() == nil {
				select {
				case telemetryErrCh <- err:
				default:
				}
				runCancel()
			}
		}()
	}

	restartCount := 0
	for {
		if telemetryErrCh != nil {
			select {
			case terr := <-telemetryErrCh:
				if terr != nil {
					log.Printf("capture-api-persistent stream_id=%d process telemetry error: %v", s.ID, terr)
					reporter.setLastError(terr.Error())
					finalStatus = "failed"
					finalStopReason = "telemetry_error"
					return
				}
			default:
			}
		}
		if runCtx.Err() != nil {
			if ctx.Err() != nil {
				finalStopReason = "context_done"
			} else {
				finalStatus = "failed"
				finalStopReason = "session_context_canceled"
			}
			return
		}
		if reporter != nil {
			reporter.setRestartCount(restartCount)
		}

		resolved, err := adapter.Resolve(runCtx, spec)
		if err != nil {
			if reporter != nil {
				reporter.setLastError(err.Error())
			}
			stop, recErr := m.recordSessionError(runCtx, s, effectiveMode, "", err)
			if recErr != nil {
				log.Printf("capture-api-persistent stream_id=%d record resolve error failed: %v", s.ID, recErr)
			}
			if stop {
				finalStatus = "failed"
				finalStopReason = "unsupported_threshold"
				return
			}
			select {
			case <-runCtx.Done():
				if ctx.Err() != nil {
					finalStopReason = "context_done"
				}
				return
			case <-time.After(2 * time.Second):
			}
			continue
		}

		if effectiveMode != capture.ModeImagePoll {
			segmentCtx, cancelSegment := context.WithTimeout(runCtx, capture.SegmentCaptureTimeout())
			seg, err := capture.CaptureSegment(segmentCtx, resolved.URL)
			cancelSegment()
			if err == nil {
				persistCtx, cancelPersist := persistCallContext(runCtx)
				err = m.persistSegmentSuccess(persistCtx, s, effectiveMode, resolved.URL, seg)
				cancelPersist()
				capture.CleanupSegment(seg)
			}
			if runCtx.Err() != nil {
				if ctx.Err() != nil {
					finalStopReason = "context_done"
				}
				return
			}
			restartCount++
			if err == nil {
				if reporter != nil {
					reporter.setLastFrame(seg.EndAt)
				}
				continue
			}
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, io.EOF) {
				continue
			}
			if reporter != nil {
				reporter.setLastError(err.Error())
			}
			stop, recErr := m.recordSessionError(runCtx, s, effectiveMode, resolved.URL, err)
			if recErr != nil {
				log.Printf("capture-api-persistent stream_id=%d record capture error failed: %v", s.ID, recErr)
			}
			if stop {
				finalStatus = "failed"
				finalStopReason = "unsupported_threshold"
				return
			}
			select {
			case <-runCtx.Done():
				if ctx.Err() != nil {
					finalStopReason = "context_done"
				}
				return
			case <-time.After(2 * time.Second):
			}
			continue
		}

		frameCh := make(chan frameEvent, m.cfg.FrameQueueSize)
		var writerWG sync.WaitGroup
		for i := 0; i < m.cfg.FrameWriterWorkers; i++ {
			writerWG.Add(1)
			go func() {
				defer writerWG.Done()
				for ev := range frameCh {
					persistCtx, cancelPersist := persistCallContext(runCtx)
					err := m.persistSuccess(persistCtx, s, ev)
					cancelPersist()
					if err != nil {
						if reporter != nil {
							reporter.setLastError(err.Error())
						}
						log.Printf("capture-api-persistent stream_id=%d persist success failed: %v", s.ID, err)
						continue
					}
					if reporter != nil {
						reporter.setLastFrame(ev.capturedAt)
					}
				}
			}()
		}

		sessionCtx, cancelSession := context.WithCancel(runCtx)
		var refreshStop chan struct{}
		if resolved.RefreshAfter > 0 {
			refreshStop = make(chan struct{})
			go func() {
				t := time.NewTimer(resolved.RefreshAfter)
				defer t.Stop()
				select {
				case <-t.C:
					cancelSession()
				case <-refreshStop:
				}
			}()
		}

		emit := m.emitFrame(frameCh, effectiveMode, resolved.URL)
		err = adapter.StartSession(sessionCtx, spec, resolved, emit)
		if refreshStop != nil {
			close(refreshStop)
		}
		cancelSession()
		close(frameCh)
		writerWG.Wait()

		if runCtx.Err() != nil {
			if ctx.Err() != nil {
				finalStopReason = "context_done"
			}
			return
		}
		restartCount++
		if err == nil {
			continue
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, io.EOF) {
			continue
		}
		if reporter != nil {
			reporter.setLastError(err.Error())
		}

		stop, recErr := m.recordSessionError(runCtx, s, effectiveMode, resolved.URL, err)
		if recErr != nil {
			log.Printf("capture-api-persistent stream_id=%d record capture error failed: %v", s.ID, recErr)
		}
		if stop {
			finalStatus = "failed"
			finalStopReason = "unsupported_threshold"
			return
		}

		select {
		case <-runCtx.Done():
			if ctx.Err() != nil {
				finalStopReason = "context_done"
			}
			return
		case <-time.After(2 * time.Second):
		}
	}
}

func (m *Manager) buildSpec(s streamConfig) capture.StreamSpec {
	intervalSec := s.CaptureIntervalSec
	if intervalSec <= 0 {
		intervalSec = settings.DefaultRecordingIntervalSec
	}
	return capture.StreamSpec{
		ID:                 s.ID,
		Provider:           s.Provider,
		StreamURL:          s.StreamURL,
		SourcePageURL:      s.SourcePageURL,
		CaptureMode:        s.CaptureMode,
		CaptureConfig:      s.CaptureConfig,
		CaptureIntervalSec: intervalSec,
		TargetFPS:          capture.SegmentTargetFPS,
		MaxFrameBytes:      m.cfg.MaxFrameBytes,
	}
}

func (m *Manager) modeAllowed(s streamConfig) bool {
	if len(m.modeFilter) == 0 {
		return true
	}
	effective := capture.EffectiveMode(m.buildSpec(s))
	_, ok := m.modeFilter[effective]
	return ok
}

func (m *Manager) emitFrame(frameCh chan frameEvent, effective capture.Mode, resolvedURL string) capture.EmitFrameFunc {
	return func(ctx context.Context, frame capture.Frame, capturedAt time.Time) error {
		ev := frameEvent{frame: frame, capturedAt: capturedAt, effective: effective, resolvedURL: resolvedURL}
		return capture.EnqueueWithTimeout(ctx, frameCh, ev, m.cfg.FrameEnqueueTimeout, "captureapipersistent.frameCh")
	}
}

func (m *Manager) persistSuccess(ctx context.Context, s streamConfig, ev frameEvent) error {
	capturedAt := ev.capturedAt
	if capturedAt.IsZero() {
		capturedAt = time.Now().UTC()
	}
	return m.client.IngestSuccess(ctx, captureapi.IngestSuccessRequest{
		StreamID:           s.ID,
		CapturedAt:         capturedAt,
		SourceKind:         ev.frame.SourceKind,
		EffectiveMode:      ev.effective,
		ResolvedURL:        ev.resolvedURL,
		MIMEType:           ev.frame.MIMEType,
		FrameBytes:         ev.frame.Bytes,
		RecordingHeartbeat: m.shouldRecordingHeartbeat(ev.effective),
	})
}

func (m *Manager) persistSegmentSuccess(ctx context.Context, s streamConfig, effective capture.Mode, resolvedURL string, seg capture.Segment) error {
	intent, err := m.client.ReserveSegmentUpload(ctx, captureapi.SegmentUploadIntentRequest{
		StreamID:  s.ID,
		MimeType:  seg.MIMEType,
		SizeBytes: seg.SizeBytes,
	})
	if err != nil {
		return fmt.Errorf("reserve segment upload: %w", err)
	}
	if err := m.client.UploadFile(ctx, intent.UploadURL, seg.Path, seg.MIMEType); err != nil {
		return fmt.Errorf("upload segment: %w", err)
	}
	var thumbnailIntent *captureapi.SegmentUploadIntent
	if seg.Thumbnail != nil && strings.TrimSpace(seg.Thumbnail.Path) != "" {
		intent, err := m.client.ReserveSegmentThumbnailUpload(ctx, captureapi.SegmentUploadIntentRequest{
			StreamID:  s.ID,
			MimeType:  seg.Thumbnail.MIMEType,
			SizeBytes: seg.Thumbnail.SizeBytes,
			StartAt:   seg.StartAt,
		})
		if err != nil {
			log.Printf("capture segment thumbnail upload skipped stream_id=%d start=%s error=%v", s.ID, seg.StartAt.UTC().Format(time.RFC3339), err)
		} else if err := m.client.UploadFile(ctx, intent.UploadURL, seg.Thumbnail.Path, seg.Thumbnail.MIMEType); err != nil {
			log.Printf("capture segment thumbnail upload skipped stream_id=%d start=%s error=%v", s.ID, seg.StartAt.UTC().Format(time.RFC3339), err)
		} else {
			thumbnailIntent = &intent
		}
	}
	return m.client.IngestSegmentSuccess(ctx, captureapi.IngestSegmentSuccessRequest{
		StreamID:           s.ID,
		SourceKind:         seg.SourceKind,
		EffectiveMode:      effective,
		ResolvedURL:        resolvedURL,
		UploadIntentID:     intent.IntentID,
		ObjectKey:          intent.ObjectKey,
		MIMEType:           seg.MIMEType,
		SizeBytes:          seg.SizeBytes,
		SHA256:             seg.SHA256,
		SegmentStartAt:     seg.StartAt,
		SegmentEndAt:       seg.EndAt,
		DurationMs:         seg.DurationMs,
		TargetFPS:          capture.SegmentTargetFPS,
		ActualFPS:          seg.ActualFPS,
		VideoCodec:         seg.VideoCodec,
		AudioCodec:         seg.AudioCodec,
		Container:          seg.Container,
		AudioPresent:       seg.AudioPresent,
		RecordingHeartbeat: m.shouldRecordingHeartbeat(effective),
		ThumbnailIntent:    thumbnailIntent,
		ThumbnailSizeBytes: thumbnailSizeBytes(seg.Thumbnail),
		ThumbnailSHA256:    thumbnailSHA256(seg.Thumbnail),
	})
}

func thumbnailSizeBytes(thumb *capture.SegmentThumbnail) int64 {
	if thumb == nil {
		return 0
	}
	return thumb.SizeBytes
}

func thumbnailSHA256(thumb *capture.SegmentThumbnail) string {
	if thumb == nil {
		return ""
	}
	return strings.TrimSpace(thumb.SHA256)
}

func (m *Manager) shouldRecordingHeartbeat(effective capture.Mode) bool {
	return m.cfg.RecordingHeartbeat && effective == capture.ModeYouTubeLive
}

func (m *Manager) recordSessionError(ctx context.Context, s streamConfig, effective capture.Mode, resolvedURL string, runErr error) (bool, error) {
	errText := strings.TrimSpace(runErr.Error())
	consecutive, err := m.client.IngestError(ctx, captureapi.IngestErrorRequest{
		StreamID:      s.ID,
		CapturedAt:    time.Now().UTC(),
		SourceKind:    sourceKindForMode(effective),
		EffectiveMode: effective,
		ResolvedURL:   resolvedURL,
		ErrorText:     errText,
	})
	if err != nil {
		return false, err
	}
	if consecutive >= m.cfg.UnsupportedErrorThreshold {
		reason := fmt.Sprintf("capture disabled after %d consecutive errors: %s", consecutive, errText)
		if err := m.client.MarkUnsupported(ctx, s.ID, effective, resolvedURL, reason); err != nil {
			return true, err
		}
		return true, nil
	}
	return false, nil
}

func sourceKindForMode(mode capture.Mode) string {
	if mode == capture.ModeImagePoll {
		return "snapshot_url"
	}
	return "live"
}

func persistCallContext(parent context.Context) (context.Context, context.CancelFunc) {
	_ = parent
	return context.WithTimeout(context.Background(), 20*time.Second)
}
