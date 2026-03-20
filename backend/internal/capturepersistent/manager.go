package capturepersistent

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

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/daydemir/stoarama/backend/internal/capture"
	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/daydemir/stoarama/backend/internal/settings"
	"github.com/daydemir/stoarama/backend/internal/storage"
)

type ManagerConfig struct {
	WorkerID                  string
	ServerID                  string
	RefreshInterval           time.Duration
	MaxSessions               int
	MaxFrameBytes             int
	FrameQueueSize            int
	FrameEnqueueTimeout       time.Duration
	FrameWriterWorkers        int
	StreamIDs                 []int64
	ModeAllowlist             []capture.Mode
	RecordingHeartbeat        bool
	LeaseDuration             time.Duration
	LeaseRenewInterval        time.Duration
	UnsupportedErrorThreshold int
	Registry                  *capture.Registry
}

type Manager struct {
	pool         *pgxpool.Pool
	r2c          *r2.Client
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

func NewManager(pool *pgxpool.Pool, r2c *r2.Client, cfg ManagerConfig) *Manager {
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
	if strings.TrimSpace(cfg.WorkerID) == "" {
		cfg.WorkerID = "capture-persistent-1"
	}
	if strings.TrimSpace(cfg.ServerID) == "" {
		cfg.ServerID = strings.ToLower(strings.TrimSpace(cfg.WorkerID))
	}
	if cfg.LeaseDuration <= 0 {
		cfg.LeaseDuration = 30 * time.Second
	}
	if cfg.LeaseRenewInterval <= 0 {
		cfg.LeaseRenewInterval = 10 * time.Second
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
		pool:         pool,
		r2c:          r2c,
		cfg:          cfg,
		streamFilter: filter,
		modeFilter:   modeFilter,
		sessions:     make(map[int64]*sessionState),
	}
}

func (m *Manager) Run(ctx context.Context) error {
	log.Printf("capture-persistent manager start worker_id=%s server_id=%s refresh=%s max_sessions=%d stream_filter=%d mode_filter=%d",
		m.cfg.WorkerID, m.cfg.ServerID, m.cfg.RefreshInterval, m.cfg.MaxSessions, len(m.streamFilter), len(m.modeFilter))

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
		log.Printf("capture-persistent stopping stream_id=%d", id)
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
		}
	}

	for _, s := range desired {
		if _, ok := m.sessions[s.ID]; ok {
			continue
		}
		acquired, err := m.acquireLease(ctx, s.ID)
		if err != nil {
			log.Printf("capture-persistent stream_id=%d lease acquire error: %v", s.ID, err)
			continue
		}
		if !acquired {
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
		log.Printf("capture-persistent reconcile active=%d started=%d stopped=%d", active, len(startList), len(stopList))
	}
	return nil
}

func (m *Manager) loadDesiredStreams(ctx context.Context) ([]streamConfig, error) {
	recordingSettings, err := settings.GetRecordingSettings(ctx, m.pool)
	if err != nil {
		return nil, fmt.Errorf("load recording settings: %w", err)
	}

	serverID := strings.TrimSpace(m.cfg.ServerID)
	query := `
		SELECT s.id, s.provider, s.source_url, s.source_page_url, s.capture_type, s.execution_class, s.execution_config_jsonb
		FROM recording_assignments ra
		JOIN streams s ON s.id=ra.stream_id
		WHERE ra.server_id=$1
		ORDER BY s.id ASC
	`
	rows, err := m.pool.Query(ctx, query, serverID)
	if err != nil {
		return nil, fmt.Errorf("query desired streams: %w", err)
	}
	defer rows.Close()

	out := make([]streamConfig, 0, 64)
	for rows.Next() {
		var s streamConfig
		var captureType string
		var executionClass string
		var cfgBytes []byte
		if err := rows.Scan(&s.ID, &s.Provider, &s.StreamURL, &s.SourcePageURL, &captureType, &executionClass, &cfgBytes); err != nil {
			return nil, fmt.Errorf("scan desired stream: %w", err)
		}
		if s.ID <= 0 {
			continue
		}
		if len(m.streamFilter) > 0 {
			if _, ok := m.streamFilter[s.ID]; !ok {
				continue
			}
		}
		if strings.TrimSpace(s.StreamURL) == "" && strings.TrimSpace(s.SourcePageURL) == "" {
			continue
		}
		s.CaptureMode = capture.LegacyModeForStream(captureType, executionClass)
		s.CaptureConfig = map[string]any{}
		if len(cfgBytes) > 0 {
			if err := json.Unmarshal(cfgBytes, &s.CaptureConfig); err != nil {
				s.CaptureConfig = map[string]any{"config_parse_error": err.Error()}
			}
		}
		cfgRaw := strings.TrimSpace(string(cfgBytes))
		s.CaptureIntervalSec = recordingSettings.CaptureIntervalSec
		s.fingerprint = fmt.Sprintf("%d|%s|%s|%s|%s|%d|%s", s.ID, s.Provider, s.StreamURL, s.SourcePageURL, s.CaptureMode, s.CaptureIntervalSec, cfgRaw)
		if !m.modeAllowed(s) {
			continue
		}
		out = append(out, s)
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("iterate desired streams: %w", rows.Err())
	}

	return prioritizeAndCap(out, m.cfg.MaxSessions), nil
}

func prioritizeAndCap(streams []streamConfig, maxSessions int) []streamConfig {
	sort.SliceStable(streams, func(i, j int) bool {
		return streams[i].ID < streams[j].ID
	})
	if maxSessions > 0 && len(streams) > maxSessions {
		log.Printf(
			"capture-persistent desired streams=%d exceed max_sessions=%d; running first %d streams by id",
			len(streams), maxSessions, maxSessions,
		)
		streams = streams[:maxSessions]
	}
	return streams
}

func (m *Manager) runManagedStream(ctx context.Context, s streamConfig) {
	log.Printf("capture-persistent stream start stream_id=%d mode=%s", s.ID, s.CaptureMode)
	defer log.Printf("capture-persistent stream stop stream_id=%d", s.ID)
	defer m.releaseLease(context.Background(), s.ID)
	defer m.setRuntimeStopped(context.Background(), s.ID)

	hbCtx, hbCancel := context.WithCancel(ctx)
	defer hbCancel()
	go m.renewLeaseLoop(hbCtx, s.ID)

	spec := m.buildSpec(s)
	effectiveMode := capture.EffectiveMode(spec)
	if effectiveMode == capture.ModeUnsupported {
		reason := fmt.Sprintf("unsupported capture runtime mapping for provider=%s url=%s", s.Provider, s.StreamURL)
		m.markUnsupported(context.Background(), s.ID, reason, capture.ModeUnsupported, "")
		return
	}

	adapter, ok := m.cfg.Registry.Get(effectiveMode)
	if !ok {
		reason := fmt.Sprintf("capture adapter %q is not registered", effectiveMode)
		m.markUnsupported(context.Background(), s.ID, reason, effectiveMode, "")
		return
	}

	for {
		if ctx.Err() != nil {
			return
		}
		_ = m.setRuntimeResolving(ctx, s.ID, effectiveMode)

		resolved, err := adapter.Resolve(ctx, spec)
		if err != nil {
			stop, recErr := m.recordSessionError(ctx, s, effectiveMode, "", err)
			if recErr != nil {
				log.Printf("capture-persistent stream_id=%d record resolve error failed: %v", s.ID, recErr)
			}
			if stop {
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Second):
			}
			continue
		}

		_ = m.setRuntimeRunning(ctx, s.ID, effectiveMode, resolved.URL, false)

		frameCh := make(chan frameEvent, m.cfg.FrameQueueSize)
		var writerWG sync.WaitGroup
		for i := 0; i < m.cfg.FrameWriterWorkers; i++ {
			writerWG.Add(1)
			go func() {
				defer writerWG.Done()
				for ev := range frameCh {
					persistCtx, cancelPersist := persistCallContext(ctx)
					err := m.persistSuccess(persistCtx, s, ev)
					cancelPersist()
					if err != nil {
						log.Printf("capture-persistent stream_id=%d persist success failed: %v", s.ID, err)
					}
				}
			}()
		}

		sessionCtx, cancelSession := context.WithCancel(ctx)
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

		if ctx.Err() != nil {
			return
		}
		if err == nil {
			continue
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, io.EOF) {
			continue
		}

		stop, recErr := m.recordSessionError(ctx, s, effectiveMode, resolved.URL, err)
		if recErr != nil {
			log.Printf("capture-persistent stream_id=%d record capture error failed: %v", s.ID, recErr)
		}
		if stop {
			return
		}

		select {
		case <-ctx.Done():
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
		TargetFPS:          1,
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
		return capture.EnqueueWithTimeout(ctx, frameCh, ev, m.cfg.FrameEnqueueTimeout, "capturepersistent.frameCh")
	}
}

func (m *Manager) persistSuccess(ctx context.Context, s streamConfig, ev frameEvent) error {
	capturedAt := ev.capturedAt
	if capturedAt.IsZero() {
		capturedAt = time.Now().UTC()
	}
	objectKey := fmt.Sprintf("raw/stream/%d/%04d/%02d/%02d/live-%d.jpg",
		s.ID, capturedAt.Year(), int(capturedAt.Month()), capturedAt.Day(), capturedAt.UnixNano())

	etag, err := m.r2c.PutBytes(ctx, objectKey, ev.frame.MIMEType, ev.frame.Bytes)
	if err != nil {
		_, _ = m.persistError(ctx, s, ev.effective, ev.resolvedURL, fmt.Errorf("upload frame: %w", err))
		return fmt.Errorf("upload frame: %w", err)
	}

	tx, err := m.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	mediaID, err := storage.UpsertMediaObject(ctx, tx, storage.MediaObjectInput{
		StorageProvider: "r2",
		Bucket:          m.r2c.Bucket(),
		ObjectKey:       objectKey,
		MIMEType:        ev.frame.MIMEType,
		SizeBytes:       ev.frame.SizeBytes,
		ETag:            etag,
		SHA256:          ev.frame.SHA256,
		Width:           ev.frame.Width,
		Height:          ev.frame.Height,
	})
	if err != nil {
		return fmt.Errorf("upsert media object: %w", err)
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO frames (stream_id, capture_job_id, captured_at, raw_media_object_id, capture_status, capture_error, source_kind)
		VALUES ($1, NULL, $2, $3, 'success', NULL, $4)
	`, s.ID, capturedAt, mediaID, ev.frame.SourceKind); err != nil {
		return fmt.Errorf("insert frame success: %w", err)
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO stream_health (stream_id, captures_total, captures_success, captures_error, last_capture_at, last_error_at, last_error_text)
		VALUES ($1, 1, 1, 0, $2, NULL, NULL)
		ON CONFLICT (stream_id)
		DO UPDATE SET
			captures_total=stream_health.captures_total+1,
			captures_success=stream_health.captures_success+1,
			last_capture_at=EXCLUDED.last_capture_at,
			last_error_at=NULL,
			last_error_text=NULL,
			updated_at=now()
	`, s.ID, capturedAt); err != nil {
		return fmt.Errorf("update stream_health success: %w", err)
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO stream_capture_runtime (stream_id, execution_class, resolved_url, status, last_resolved_at, last_frame_at, consecutive_errors, last_error_text)
		VALUES ($1, $2, $3, 'running', now(), $4, 0, NULL)
		ON CONFLICT (stream_id)
		DO UPDATE SET
			execution_class=EXCLUDED.execution_class,
			resolved_url=EXCLUDED.resolved_url,
			status='running',
			last_frame_at=EXCLUDED.last_frame_at,
			consecutive_errors=0,
			last_error_text=NULL,
			updated_at=now()
	`, s.ID, string(ev.effective), ev.resolvedURL, capturedAt); err != nil {
		return fmt.Errorf("update stream_capture_runtime success: %w", err)
	}
	if m.shouldRecordingHeartbeat(ev.effective) {
		// no-op: assignment table is source of truth for desired recording ownership.
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit success tx: %w", err)
	}
	return nil
}

func (m *Manager) shouldRecordingHeartbeat(effective capture.Mode) bool {
	return m.cfg.RecordingHeartbeat && effective == capture.ModeYouTubeLive
}

func (m *Manager) persistError(ctx context.Context, s streamConfig, effective capture.Mode, resolvedURL string, captureErr error) (int, error) {
	now := time.Now().UTC()
	errText := strings.TrimSpace(captureErr.Error())
	sourceKind := "live"
	if effective == capture.ModeImagePoll {
		sourceKind = "snapshot_url"
	}

	tx, err := m.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin error tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `
		INSERT INTO frames (stream_id, capture_job_id, captured_at, raw_media_object_id, capture_status, capture_error, source_kind)
		VALUES ($1, NULL, $2, NULL, 'error', $3, $4)
	`, s.ID, now, errText, sourceKind); err != nil {
		return 0, fmt.Errorf("insert error frame: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO stream_health (stream_id, captures_total, captures_success, captures_error, last_capture_at, last_error_at, last_error_text)
		VALUES ($1, 1, 0, 1, $2, $2, $3)
		ON CONFLICT (stream_id)
		DO UPDATE SET
			captures_total=stream_health.captures_total+1,
			captures_error=stream_health.captures_error+1,
			last_capture_at=EXCLUDED.last_capture_at,
			last_error_at=EXCLUDED.last_error_at,
			last_error_text=EXCLUDED.last_error_text,
			updated_at=now()
	`, s.ID, now, errText); err != nil {
		return 0, fmt.Errorf("update stream_health error: %w", err)
	}

	var consecutive int
	if err := tx.QueryRow(ctx, `
		INSERT INTO stream_capture_runtime (stream_id, execution_class, resolved_url, status, last_resolved_at, last_frame_at, consecutive_errors, last_error_text)
		VALUES ($1, $2, $3, 'error', now(), NULL, 1, $4)
		ON CONFLICT (stream_id)
		DO UPDATE SET
			execution_class=EXCLUDED.execution_class,
			resolved_url=COALESCE(NULLIF(EXCLUDED.resolved_url,''), stream_capture_runtime.resolved_url),
			status='error',
			consecutive_errors=stream_capture_runtime.consecutive_errors+1,
			last_error_text=EXCLUDED.last_error_text,
			updated_at=now()
		RETURNING consecutive_errors
	`, s.ID, string(effective), resolvedURL, errText).Scan(&consecutive); err != nil {
		return 0, fmt.Errorf("update stream_capture_runtime error: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit error tx: %w", err)
	}
	return consecutive, nil
}

func (m *Manager) recordSessionError(ctx context.Context, s streamConfig, effective capture.Mode, resolvedURL string, runErr error) (bool, error) {
	consecutive, err := m.persistError(ctx, s, effective, resolvedURL, runErr)
	if err != nil {
		return false, err
	}
	if consecutive >= m.cfg.UnsupportedErrorThreshold {
		reason := fmt.Sprintf("capture disabled after %d consecutive errors: %s", consecutive, strings.TrimSpace(runErr.Error()))
		if err := m.markUnsupported(ctx, s.ID, reason, effective, resolvedURL); err != nil {
			return true, err
		}
		return true, nil
	}
	return false, nil
}

func (m *Manager) markUnsupported(ctx context.Context, streamID int64, reason string, effective capture.Mode, resolvedURL string) error {
	tx, err := m.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin unsupported tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `
		INSERT INTO stream_capture_runtime (stream_id, execution_class, resolved_url, status, last_resolved_at, last_frame_at, consecutive_errors, last_error_text)
		VALUES ($1, $2, $3, 'unsupported', now(), NULL, $4, $5)
		ON CONFLICT (stream_id)
		DO UPDATE SET
			execution_class=EXCLUDED.execution_class,
			resolved_url=COALESCE(NULLIF(EXCLUDED.resolved_url,''), stream_capture_runtime.resolved_url),
			status='unsupported',
			consecutive_errors=GREATEST(stream_capture_runtime.consecutive_errors, EXCLUDED.consecutive_errors),
			last_error_text=EXCLUDED.last_error_text,
			updated_at=now()
	`, streamID, string(effective), resolvedURL, m.cfg.UnsupportedErrorThreshold, reason); err != nil {
		return fmt.Errorf("mark runtime unsupported: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit unsupported tx: %w", err)
	}
	return nil
}

func (m *Manager) setRuntimeResolving(ctx context.Context, streamID int64, effective capture.Mode) error {
	_, err := m.pool.Exec(ctx, `
		INSERT INTO stream_capture_runtime (stream_id, execution_class, status, last_resolved_at, consecutive_errors)
		VALUES ($1, $2, 'resolving', now(), 0)
		ON CONFLICT (stream_id)
		DO UPDATE SET
			execution_class=EXCLUDED.execution_class,
			status='resolving',
			last_resolved_at=EXCLUDED.last_resolved_at,
			updated_at=now()
	`, streamID, string(effective))
	return err
}

func (m *Manager) setRuntimeRunning(ctx context.Context, streamID int64, effective capture.Mode, resolvedURL string, resetErrors bool) error {
	if resetErrors {
		_, err := m.pool.Exec(ctx, `
			INSERT INTO stream_capture_runtime (stream_id, execution_class, resolved_url, status, last_resolved_at, consecutive_errors, last_error_text)
			VALUES ($1, $2, $3, 'running', now(), 0, NULL)
			ON CONFLICT (stream_id)
			DO UPDATE SET
				execution_class=EXCLUDED.execution_class,
				resolved_url=EXCLUDED.resolved_url,
				status='running',
				last_resolved_at=EXCLUDED.last_resolved_at,
				consecutive_errors=0,
				last_error_text=NULL,
				updated_at=now()
		`, streamID, string(effective), resolvedURL)
		return err
	}
	_, err := m.pool.Exec(ctx, `
		INSERT INTO stream_capture_runtime (stream_id, execution_class, resolved_url, status, last_resolved_at)
		VALUES ($1, $2, $3, 'running', now())
		ON CONFLICT (stream_id)
		DO UPDATE SET
			execution_class=EXCLUDED.execution_class,
			resolved_url=EXCLUDED.resolved_url,
			status='running',
			last_resolved_at=EXCLUDED.last_resolved_at,
			updated_at=now()
	`, streamID, string(effective), resolvedURL)
	return err
}

func (m *Manager) setRuntimeStopped(ctx context.Context, streamID int64) {
	_, _ = m.pool.Exec(ctx, `
		INSERT INTO stream_capture_runtime (stream_id, status)
		VALUES ($1, 'stopped')
		ON CONFLICT (stream_id)
		DO UPDATE SET
			status='stopped',
			updated_at=now()
	`, streamID)
}

func (m *Manager) acquireLease(ctx context.Context, streamID int64) (bool, error) {
	now := time.Now().UTC()
	expires := now.Add(m.cfg.LeaseDuration)
	ct, err := m.pool.Exec(ctx, `
		INSERT INTO capture_session_leases (stream_id, lease_owner, lease_expires_at, heartbeat_at, acquired_at)
		VALUES ($1, $2, $3, $4, $4)
		ON CONFLICT (stream_id)
		DO UPDATE SET
			lease_owner=EXCLUDED.lease_owner,
			lease_expires_at=EXCLUDED.lease_expires_at,
			heartbeat_at=EXCLUDED.heartbeat_at,
			acquired_at=CASE WHEN capture_session_leases.lease_owner=EXCLUDED.lease_owner THEN capture_session_leases.acquired_at ELSE EXCLUDED.acquired_at END,
			updated_at=now()
		WHERE capture_session_leases.lease_expires_at < now() OR capture_session_leases.lease_owner = EXCLUDED.lease_owner
	`, streamID, m.cfg.WorkerID, expires, now)
	if err != nil {
		return false, fmt.Errorf("acquire lease stream_id=%d: %w", streamID, err)
	}
	return ct.RowsAffected() > 0, nil
}

func (m *Manager) renewLeaseLoop(ctx context.Context, streamID int64) {
	ticker := time.NewTicker(m.cfg.LeaseRenewInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := m.renewLease(ctx, streamID); err != nil {
				log.Printf("capture-persistent stream_id=%d lease renew error: %v", streamID, err)
			}
		}
	}
}

func (m *Manager) renewLease(ctx context.Context, streamID int64) error {
	now := time.Now().UTC()
	expires := now.Add(m.cfg.LeaseDuration)
	ct, err := m.pool.Exec(ctx, `
		UPDATE capture_session_leases
		SET lease_expires_at=$3, heartbeat_at=$4, updated_at=now()
		WHERE stream_id=$1 AND lease_owner=$2
	`, streamID, m.cfg.WorkerID, expires, now)
	if err != nil {
		return fmt.Errorf("renew lease stream_id=%d: %w", streamID, err)
	}
	if ct.RowsAffected() == 0 {
		return fmt.Errorf("lease lost")
	}
	return nil
}

func (m *Manager) releaseLease(ctx context.Context, streamID int64) {
	_, _ = m.pool.Exec(ctx, `DELETE FROM capture_session_leases WHERE stream_id=$1 AND lease_owner=$2`, streamID, m.cfg.WorkerID)
}

func persistCallContext(parent context.Context) (context.Context, context.CancelFunc) {
	_ = parent
	return context.WithTimeout(context.Background(), 20*time.Second)
}
