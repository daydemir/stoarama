package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/email"
)

const (
	supervisionIncidentDown10m  = "down_10m"
	supervisionIncidentSpotty2h = "spotty_2h"
	supervisionNotifyRepeat     = 12 * time.Hour
	supervisionRemediateRetry   = 10 * time.Minute
)

type supervisorIncidentUpdate struct {
	IncidentID      int64
	ShouldNotify    bool
	ShouldRemediate bool
}

func runRecordingSupervisor(ctx context.Context, cfg config.Config, args []string) {
	if len(args) >= 1 && (args[0] == "-h" || args[0] == "--help") {
		fmt.Print("stoaramactl recording supervisor <run|incidents|reconcile> ...\n")
		return
	}
	if len(args) < 1 {
		fmt.Print("stoaramactl recording supervisor <run|incidents|reconcile> ...\n")
		return
	}
	switch args[0] {
	case "run":
		runRecordingSupervisorLoop(ctx, cfg, args[1:])
	case "incidents":
		runRecordingIncidentsList(ctx, cfg, args[1:])
	case "reconcile":
		runRecordingReconcile(ctx, cfg, args[1:])
	default:
		log.Fatalf("unknown recording supervisor subcommand: %s", args[0])
	}
}

func runRecordingSupervisorLoop(ctx context.Context, cfg config.Config, args []string) {
	fs := flag.NewFlagSet("recording supervisor run", flag.ExitOnError)
	backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
	apiToken := fs.String("api-token", cfg.APIToken, "backend API token")
	intervalSec := fs.Int("interval-sec", 60, "loop interval seconds")
	limit := fs.Int("limit", 500, "supervision row limit")
	dryRun := fs.Bool("dry-run", false, "log actions without mutating")
	once := fs.Bool("once", false, "run once then exit")
	_ = fs.Parse(args)

	if *intervalSec <= 0 {
		log.Fatalf("--interval-sec must be > 0")
	}
	if *limit <= 0 {
		log.Fatalf("--limit must be > 0")
	}

	pool := mustOpenPool(ctx, cfg)
	defer pool.Close()

	mailer, err := email.NewSender(email.Config{
		Provider:  cfg.EmailProvider,
		From:      cfg.EmailFrom,
		ReplyTo:   cfg.EmailReplyTo,
		ResendKey: cfg.EmailResendAPIKey,
	})
	if err != nil {
		log.Fatalf("init email sender: %v", err)
	}
	if strings.TrimSpace(cfg.EmailProvider) == "" || strings.EqualFold(strings.TrimSpace(cfg.EmailProvider), "log") {
		log.Printf("recording supervisor warning: email provider=%q; alerts will not be delivered externally", defaultString(strings.TrimSpace(cfg.EmailProvider), "log"))
	}

	runOnce := func() {
		items, err := fetchRecordingSupervisionItems(ctx, strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken), *limit)
		if err != nil {
			log.Printf("recording supervisor fetch error: %v", err)
			return
		}
		recipients, err := loadSupervisorRecipients(ctx, pool, cfg)
		if err != nil {
			log.Printf("recording supervisor recipient load error: %v", err)
			return
		}
		if len(recipients) == 0 {
			log.Printf("recording supervisor warning: no alert recipients configured; set STREAM_ALERTS_RECIPIENTS or add an active verified admin")
		}
		if err := resolveInactiveSupervisorIncidents(ctx, pool); err != nil {
			log.Printf("recording supervisor resolve inactive incidents: %v", err)
		}
		for _, item := range items {
			state := strings.TrimSpace(fmt.Sprint(item["supervision_state"]))
			streamID := int64FromAny(item["stream_id"])
			if state == "" || streamID <= 0 {
				continue
			}
			if state == "healthy" {
				if err := resolveSupervisorIncidents(ctx, pool, streamID); err != nil {
					log.Printf("recording supervisor resolve incident stream_id=%d: %v", streamID, err)
				}
				continue
			}
			incidentType := state
			if incidentType != supervisionIncidentDown10m && incidentType != supervisionIncidentSpotty2h {
				continue
			}
			update, err := upsertSupervisorIncident(ctx, pool, item, incidentType, supervisionNotifyRepeat, supervisionRemediateRetry)
			if err != nil {
				log.Printf("recording supervisor upsert incident stream_id=%d: %v", streamID, err)
				continue
			}
			if update.ShouldNotify && len(recipients) > 0 {
				notified := false
				for _, addr := range recipients {
					deliveryID, deliveryErr := createAlertDeliveryAttempt(ctx, pool, update.IncidentID, item, addr)
					if deliveryErr != nil {
						log.Printf("recording supervisor create alert delivery stream_id=%d to=%s: %v", streamID, addr, deliveryErr)
					}
					receipt, err := mailer.Send(ctx, email.Message{
						To:          addr,
						Subject:     supervisorIncidentSubject(item),
						PlainText:   supervisorIncidentBody(strings.TrimSpace(*backendAPIURL), item),
						MessageType: "recording_problem",
					})
					if err != nil {
						if deliveryID > 0 {
							_ = markAlertDeliveryFailed(ctx, pool, deliveryID, err)
						}
						log.Printf("recording supervisor send alert stream_id=%d to=%s: %v", streamID, addr, err)
						continue
					}
					notified = true
					if deliveryID > 0 {
						_ = markAlertDeliverySent(ctx, pool, deliveryID, receipt)
					}
					log.Printf("recording supervisor sent alert stream_id=%d incident_id=%d to=%s provider=%s message_id=%s status=%s", streamID, update.IncidentID, addr, receipt.Provider, receipt.MessageID, receipt.Status)
				}
				if notified {
					if err := markSupervisorIncidentNotified(ctx, pool, update.IncidentID); err != nil {
						log.Printf("recording supervisor mark notified stream_id=%d incident_id=%d: %v", streamID, update.IncidentID, err)
					}
				}
			}
			if update.ShouldRemediate {
				if err := supervisorReassign(ctx, strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken), item, *dryRun); err != nil {
					log.Printf("recording supervisor reassign stream_id=%d: %v", streamID, err)
				} else if !*dryRun {
					if err := markSupervisorIncidentRemediated(ctx, pool, streamID, incidentType); err != nil {
						log.Printf("recording supervisor mark remediated stream_id=%d: %v", streamID, err)
					}
				}
			}
		}
	}

	runOnce()
	if *once {
		return
	}
	ticker := time.NewTicker(time.Duration(*intervalSec) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			runOnce()
		}
	}
}

func runRecordingReconcile(ctx context.Context, cfg config.Config, args []string) {
	fs := flag.NewFlagSet("recording supervisor reconcile", flag.ExitOnError)
	streamID := fs.Int64("id", 0, "stream id")
	backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
	apiToken := fs.String("api-token", cfg.APIToken, "backend API token")
	apply := fs.Bool("apply", false, "perform unassign/reassign instead of dry-run")
	asJSON := fs.Bool("json", false, "print JSON")
	_ = fs.Parse(args)

	if *streamID <= 0 {
		log.Fatalf("--id must be > 0")
	}

	item, err := fetchRecordingSupervisionItem(ctx, strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken), *streamID)
	if err != nil {
		log.Fatalf("fetch recording supervision stream_id=%d: %v", *streamID, err)
	}
	if item == nil {
		log.Fatalf("stream_id=%d not found in desired-on supervision set", *streamID)
	}

	state := strings.TrimSpace(fmt.Sprint(item["supervision_state"]))
	if state == "" {
		state = "unknown"
	}
	dryRun := !*apply
	if state == "healthy" {
		if *asJSON {
			printJSON(map[string]any{
				"stream_id":          *streamID,
				"supervision_state":  state,
				"supervision_reason": item["supervision_reason"],
				"action":             "noop",
				"applied":            false,
			})
			return
		}
		fmt.Printf("stream_id=%d supervision_state=%s reason=%v action=noop\n", *streamID, state, item["supervision_reason"])
		return
	}
	if err := supervisorReassign(ctx, strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken), item, dryRun); err != nil {
		log.Fatalf("reconcile stream_id=%d: %v", *streamID, err)
	}
	if *asJSON {
		printJSON(map[string]any{
			"stream_id":          *streamID,
			"supervision_state":  state,
			"supervision_reason": item["supervision_reason"],
			"action":             "reassign",
			"applied":            *apply,
		})
		return
	}
	action := "dry-run"
	if *apply {
		action = "reassigned"
	}
	fmt.Printf("stream_id=%d supervision_state=%s reason=%v action=%s\n", *streamID, state, item["supervision_reason"], action)
}

func runRecordingIncidentsList(ctx context.Context, cfg config.Config, args []string) {
	fs := flag.NewFlagSet("recording supervisor incidents", flag.ExitOnError)
	status := fs.String("status", "open", "incident status open|resolved")
	limit := fs.Int("limit", 200, "row limit")
	asJSON := fs.Bool("json", false, "print JSON")
	_ = fs.Parse(args)

	if *status != "open" && *status != "resolved" {
		log.Fatalf("--status must be open|resolved")
	}
	if *limit <= 0 {
		log.Fatalf("--limit must be > 0")
	}

	pool := mustOpenPool(ctx, cfg)
	defer pool.Close()
	rows, err := pool.Query(ctx, `
		SELECT
			i.id,
			i.stream_id,
			s.name,
			s.slug,
			i.incident_type,
			i.status,
			i.first_observed_at,
			i.last_observed_at,
			i.opened_at,
			i.resolved_at,
			i.last_notified_at,
			i.notify_count,
			i.details_jsonb
		FROM stream_recording_incidents i
		JOIN streams s ON s.id=i.stream_id
		WHERE i.status=$1
		ORDER BY i.last_observed_at DESC, i.id DESC
		LIMIT $2
	`, *status, *limit)
	if err != nil {
		log.Fatalf("query incidents: %v", err)
	}
	defer rows.Close()
	items := make([]map[string]any, 0, *limit)
	for rows.Next() {
		var (
			id, streamID                              int64
			name, slug, incidentType, rowStatus       string
			firstObservedAt, lastObservedAt, openedAt time.Time
			resolvedAt, lastNotifiedAt                *time.Time
			notifyCount                               int
			detailsRaw                                []byte
		)
		if err := rows.Scan(&id, &streamID, &name, &slug, &incidentType, &rowStatus, &firstObservedAt, &lastObservedAt, &openedAt, &resolvedAt, &lastNotifiedAt, &notifyCount, &detailsRaw); err != nil {
			log.Fatalf("scan incidents: %v", err)
		}
		details := map[string]any{}
		_ = json.Unmarshal(detailsRaw, &details)
		items = append(items, map[string]any{
			"id":                id,
			"stream_id":         streamID,
			"stream_name":       name,
			"stream_slug":       slug,
			"incident_type":     incidentType,
			"status":            rowStatus,
			"first_observed_at": firstObservedAt,
			"last_observed_at":  lastObservedAt,
			"opened_at":         openedAt,
			"resolved_at":       resolvedAt,
			"last_notified_at":  lastNotifiedAt,
			"notify_count":      notifyCount,
			"details":           details,
		})
	}
	if rows.Err() != nil {
		log.Fatalf("iterate incidents: %v", rows.Err())
	}
	if *asJSON {
		printJSON(map[string]any{"items": items, "status": *status, "limit": *limit})
		return
	}
	fmt.Printf("incidents=%d status=%s\n", len(items), *status)
	for _, item := range items {
		fmt.Printf("stream_id=%v slug=%v type=%v first=%v last=%v notified=%v count=%v\n",
			item["stream_id"], item["stream_slug"], item["incident_type"], item["first_observed_at"], item["last_observed_at"], item["last_notified_at"], item["notify_count"])
	}
}

func fetchRecordingSupervisionItems(ctx context.Context, backendAPIURL, apiToken string, limit int) ([]map[string]any, error) {
	payload, err := supervisorAPIGet(ctx, backendAPIURL, apiToken, fmt.Sprintf("/api/v1/recording/supervision?limit=%d", limit))
	if err != nil {
		return nil, err
	}
	rawItems, _ := payload["items"].([]any)
	items := make([]map[string]any, 0, len(rawItems))
	for _, raw := range rawItems {
		items = append(items, asMap(raw))
	}
	return items, nil
}

func fetchRecordingSupervisionItem(ctx context.Context, backendAPIURL, apiToken string, streamID int64) (map[string]any, error) {
	payload, err := supervisorAPIGet(ctx, backendAPIURL, apiToken, fmt.Sprintf("/api/v1/recording/supervision?stream_id=%d&limit=1", streamID))
	if err != nil {
		return nil, err
	}
	rawItems, _ := payload["items"].([]any)
	if len(rawItems) == 0 {
		return nil, nil
	}
	return asMap(rawItems[0]), nil
}

func supervisorReassign(ctx context.Context, backendAPIURL, apiToken string, item map[string]any, dryRun bool) error {
	streamID := int64FromAny(item["stream_id"])
	if streamID <= 0 {
		return fmt.Errorf("missing stream_id")
	}
	reason := strings.TrimSpace(fmt.Sprint(item["supervision_reason"]))
	if reason == "" {
		reason = "recording_supervisor"
	}
	actor := "stoaramactl.recording_supervisor"
	if dryRun {
		log.Printf("recording supervisor dry-run stream_id=%d state=%s reason=%s", streamID, item["supervision_state"], reason)
		return nil
	}
	if _, err := supervisorAPIRequest(ctx, http.MethodPost, backendAPIURL, apiToken, fmt.Sprintf("/api/v1/recording/streams/%d/unassign", streamID), map[string]any{
		"confirm": fmt.Sprintf("unassign:%d", streamID),
		"reason":  "supervisor recycle: " + reason,
		"actor":   actor,
	}); err != nil {
		if !strings.Contains(strings.ToLower(err.Error()), "not found") {
			return fmt.Errorf("unassign current recording: %w", err)
		}
	}
	selectedServerID, err := autoSelectRecordingServerSafe(ctx, backendAPIURL, apiToken, streamID)
	if err != nil {
		return err
	}
	_, err = supervisorAPIRequest(ctx, http.MethodPost, backendAPIURL, apiToken, fmt.Sprintf("/api/v1/recording/streams/%d/assign", streamID), map[string]any{
		"server_id": selectedServerID,
		"reason":    "supervisor recover: " + reason,
		"actor":     actor,
	})
	return err
}

func autoSelectRecordingServerSafe(ctx context.Context, backendAPIURL string, apiToken string, streamID int64) (string, error) {
	if streamID <= 0 {
		return "", fmt.Errorf("stream id is required")
	}
	streamPayload, err := supervisorAPIGet(ctx, strings.TrimSpace(backendAPIURL), strings.TrimSpace(apiToken), fmt.Sprintf("/api/v1/dashboard/streams/%d?limit=1", streamID))
	if err != nil {
		return "", err
	}
	stream := asMap(streamPayload["stream"])
	if int64FromAny(stream["id"]) != streamID {
		return "", fmt.Errorf("stream %d not found", streamID)
	}
	desiredExecutionClasses := inferRecordingAssignmentExecutionClassesForCLI(stream)
	if len(desiredExecutionClasses) == 0 {
		return "", fmt.Errorf("stream %d is not startable in the clip-native recording path", streamID)
	}
	payload, err := supervisorAPIGet(ctx, strings.TrimSpace(backendAPIURL), strings.TrimSpace(apiToken), "/api/v1/dashboard/recording/server-capacity")
	if err != nil {
		return "", err
	}
	items, _ := payload["items"].([]any)
	candidates := make([]map[string]any, 0, len(items))
	for _, raw := range items {
		row := asMap(raw)
		serverID := strings.TrimSpace(fmt.Sprint(row["server_id"]))
		if serverID == "" || serverID == "<nil>" {
			continue
		}
		if !boolFromAny(row["active"]) || boolFromAny(row["draining"]) {
			continue
		}
		freeSlots := int64FromAny(row["free_slots"])
		if freeSlots <= 0 {
			continue
		}
		if !recordingCandidateSupportsExecutionClassesForCLI(row, desiredExecutionClasses) {
			continue
		}
		candidates = append(candidates, row)
	}
	if len(candidates) == 0 {
		return "", fmt.Errorf("no recording server has free capacity for %s", strings.Join(desiredExecutionClasses, "/"))
	}
	rank := func(row map[string]any) int {
		executionClasses := recordingCandidateExecutionClassListForCLI(row)
		best := len(desiredExecutionClasses) + 1
		for _, executionClass := range executionClasses {
			for idx, desired := range desiredExecutionClasses {
				if executionClass == desired && idx < best {
					best = idx
				}
			}
		}
		return best
	}
	sort.Slice(candidates, func(i, j int) bool {
		rankI := rank(candidates[i])
		rankJ := rank(candidates[j])
		if rankI != rankJ {
			return rankI < rankJ
		}
		freeI := int64FromAny(candidates[i]["free_slots"])
		freeJ := int64FromAny(candidates[j]["free_slots"])
		if freeI != freeJ {
			return freeI > freeJ
		}
		return strings.TrimSpace(fmt.Sprint(candidates[i]["server_id"])) < strings.TrimSpace(fmt.Sprint(candidates[j]["server_id"]))
	})
	return strings.TrimSpace(fmt.Sprint(candidates[0]["server_id"])), nil
}

func supervisorAPIGet(ctx context.Context, baseURL string, apiToken string, path string) (map[string]any, error) {
	return supervisorAPIRequest(ctx, http.MethodGet, baseURL, apiToken, path, nil)
}

func supervisorAPIRequest(ctx context.Context, method string, baseURL string, apiToken string, path string, payload any) (map[string]any, error) {
	m := strings.TrimSpace(strings.ToUpper(method))
	base := strings.TrimRight(strings.TrimSpace(baseURL), "/")
	token := strings.TrimSpace(apiToken)
	p := strings.TrimSpace(path)
	if m == "" || base == "" || token == "" || p == "" {
		return nil, fmt.Errorf("invalid api request configuration")
	}
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	var body io.Reader
	if payload != nil {
		raw, err := json.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf("marshal api payload: %w", err)
		}
		body = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, m, base+p, body)
	if err != nil {
		return nil, fmt.Errorf("build api request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	switch m {
	case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		req.Header.Set("Idempotency-Key", fmt.Sprintf("%s:%s:%d", m, p, time.Now().UnixNano()))
	}
	resp, err := (&http.Client{Timeout: 45 * time.Second}).Do(req)
	if err != nil {
		return nil, fmt.Errorf("api request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("api request failed method=%s status=%d body=%q", m, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	out := map[string]any{}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode api response: %w", err)
	}
	return out, nil
}

func loadSupervisorRecipients(ctx context.Context, pool *pgxpool.Pool, cfg config.Config) ([]string, error) {
	seen := map[string]struct{}{}
	out := make([]string, 0, 8)
	add := func(raw string) {
		addr := strings.ToLower(strings.TrimSpace(raw))
		if addr == "" {
			return
		}
		if _, ok := seen[addr]; ok {
			return
		}
		seen[addr] = struct{}{}
		out = append(out, addr)
	}
	for _, raw := range splitCSV(cfg.StreamAlertsRecipients) {
		add(raw)
	}
	rows, err := pool.Query(ctx, `
		SELECT email
		FROM accounts
		WHERE role='admin'
		  AND status='active'
		  AND email_verified_at IS NOT NULL
		ORDER BY id ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var emailAddr string
		if err := rows.Scan(&emailAddr); err != nil {
			return nil, err
		}
		add(emailAddr)
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	return out, nil
}

func upsertSupervisorIncident(ctx context.Context, pool *pgxpool.Pool, item map[string]any, incidentType string, repeatInterval time.Duration, remediationRetry time.Duration) (supervisorIncidentUpdate, error) {
	streamID := int64FromAny(item["stream_id"])
	if streamID <= 0 {
		return supervisorIncidentUpdate{}, fmt.Errorf("missing stream_id")
	}
	now := time.Now().UTC()
	details := map[string]any{
		"name":                strings.TrimSpace(fmt.Sprint(item["name"])),
		"slug":                strings.TrimSpace(fmt.Sprint(item["slug"])),
		"reason":              strings.TrimSpace(fmt.Sprint(item["supervision_reason"])),
		"execution_class":     strings.TrimSpace(fmt.Sprint(item["execution_class"])),
		"server_id":           strings.TrimSpace(fmt.Sprint(item["server_id"])),
		"assignment_revision": int64FromAny(item["assignment_revision"]),
		"runtime_status":      strings.TrimSpace(fmt.Sprint(item["runtime_status"])),
		"relay_status":        strings.TrimSpace(fmt.Sprint(item["relay_status"])),
		"last_error_text":     strings.TrimSpace(fmt.Sprint(item["last_error_text"])),
		"relay_error_text":    strings.TrimSpace(fmt.Sprint(item["relay_error_text"])),
		"loss_rate_10m":       item["loss_rate_10m"],
		"loss_rate_2h":        item["loss_rate_2h"],
		"process_issues_2h":   item["process_issues_2h"],
		"outage_episodes_2h":  item["outage_episodes_2h"],
		"last_frame_at":       item["last_frame_at"],
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return supervisorIncidentUpdate{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var (
		incidentID      int64
		firstObservedAt time.Time
		lastNotifiedAt  *time.Time
		notifyCount     int
		existingRaw     []byte
		existingDetails map[string]any
		existing        bool
	)
	err = tx.QueryRow(ctx, `
		SELECT id, first_observed_at, last_notified_at, notify_count, details_jsonb
		FROM stream_recording_incidents
		WHERE stream_id=$1
		  AND incident_type=$2
		  AND status='open'
		FOR UPDATE
	`, streamID, incidentType).Scan(&incidentID, &firstObservedAt, &lastNotifiedAt, &notifyCount, &existingRaw)
	if err == nil {
		existing = true
		existingDetails = map[string]any{}
		_ = json.Unmarshal(existingRaw, &existingDetails)
	} else if !strings.Contains(strings.ToLower(err.Error()), "no rows") {
		return supervisorIncidentUpdate{}, err
	}

	currentRevision := int64FromAny(item["assignment_revision"])
	shouldRemediate := !existing
	if existing {
		existingRevision := int64FromAny(existingDetails["assignment_revision"])
		lastRemediatedAt := parseAnyTime(existingDetails["last_remediated_at"])
		if currentRevision > 0 && existingRevision > 0 && existingRevision != currentRevision {
			if _, err := tx.Exec(ctx, `
				UPDATE stream_recording_incidents
				SET status='resolved', resolved_at=$2, last_observed_at=$2, updated_at=now()
				WHERE id=$1
			`, incidentID, now); err != nil {
				return supervisorIncidentUpdate{}, err
			}
			existing = false
			shouldRemediate = true
		} else if lastRemediatedAt == nil || now.Sub(lastRemediatedAt.UTC()) >= remediationRetry {
			shouldRemediate = true
		}
	}

	if existing {
		if value, ok := existingDetails["last_remediated_at"]; ok && value != nil {
			details["last_remediated_at"] = value
		}
		if value, ok := existingDetails["remediation_count"]; ok && value != nil {
			details["remediation_count"] = value
		}
	}
	detailsBytes, err := json.Marshal(details)
	if err != nil {
		return supervisorIncidentUpdate{}, err
	}

	if !existing {
		if err := tx.QueryRow(ctx, `
			INSERT INTO stream_recording_incidents (
				stream_id, incident_type, status,
				first_observed_at, last_observed_at, opened_at, details_jsonb, updated_at
			)
			VALUES ($1, $2, 'open', $3, $3, now(), $4::jsonb, now())
			RETURNING id, first_observed_at, last_notified_at, notify_count
		`, streamID, incidentType, now, string(detailsBytes)).Scan(&incidentID, &firstObservedAt, &lastNotifiedAt, &notifyCount); err != nil {
			return supervisorIncidentUpdate{}, err
		}
	} else {
		if _, err := tx.Exec(ctx, `
			UPDATE stream_recording_incidents
			SET last_observed_at=$2, details_jsonb=$3::jsonb, updated_at=now()
			WHERE id=$1
		`, incidentID, now, string(detailsBytes)); err != nil {
			return supervisorIncidentUpdate{}, err
		}
	}

	shouldNotify := !existing || lastNotifiedAt == nil || now.Sub(lastNotifiedAt.UTC()) >= repeatInterval
	if err := tx.Commit(ctx); err != nil {
		return supervisorIncidentUpdate{}, err
	}
	_ = firstObservedAt
	_ = notifyCount
	return supervisorIncidentUpdate{
		IncidentID:      incidentID,
		ShouldNotify:    shouldNotify,
		ShouldRemediate: shouldRemediate,
	}, nil
}

func markSupervisorIncidentNotified(ctx context.Context, pool *pgxpool.Pool, incidentID int64) error {
	if incidentID <= 0 {
		return fmt.Errorf("incident id is required")
	}
	_, err := pool.Exec(ctx, `
		UPDATE stream_recording_incidents
		SET last_notified_at=now(), notify_count=notify_count+1, updated_at=now()
		WHERE id=$1
	`, incidentID)
	return err
}

func resolveSupervisorIncidents(ctx context.Context, pool *pgxpool.Pool, streamID int64) error {
	_, err := pool.Exec(ctx, `
		UPDATE stream_recording_incidents
		SET status='resolved', resolved_at=now(), last_observed_at=now(), updated_at=now()
		WHERE stream_id=$1
		  AND status='open'
		  AND incident_type IN ($2, $3)
	`, streamID, supervisionIncidentDown10m, supervisionIncidentSpotty2h)
	return err
}

func resolveInactiveSupervisorIncidents(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, `
		UPDATE stream_recording_incidents i
		SET status='resolved', resolved_at=now(), last_observed_at=now(), updated_at=now()
		FROM streams s
		WHERE s.id=i.stream_id
		  AND s.recording_state <> 'on'
		  AND i.status='open'
		  AND i.incident_type IN ($1, $2)
	`, supervisionIncidentDown10m, supervisionIncidentSpotty2h)
	return err
}

func markSupervisorIncidentRemediated(ctx context.Context, pool *pgxpool.Pool, streamID int64, incidentType string) error {
	if streamID <= 0 || strings.TrimSpace(incidentType) == "" {
		return fmt.Errorf("stream id and incident type are required")
	}
	_, err := pool.Exec(ctx, `
		UPDATE stream_recording_incidents
		SET
			details_jsonb=jsonb_set(
				jsonb_set(
					COALESCE(details_jsonb, '{}'::jsonb),
					'{last_remediated_at}',
					to_jsonb(now()),
					true
				),
				'{remediation_count}',
				to_jsonb(COALESCE((details_jsonb->>'remediation_count')::int, 0) + 1),
				true
			),
			updated_at=now()
		WHERE stream_id=$1
		  AND incident_type=$2
		  AND status='open'
	`, streamID, strings.TrimSpace(incidentType))
	return err
}

func createAlertDeliveryAttempt(ctx context.Context, pool *pgxpool.Pool, incidentID int64, item map[string]any, recipient string) (int64, error) {
	streamID := int64FromAny(item["stream_id"])
	if streamID <= 0 {
		return 0, fmt.Errorf("stream id is required")
	}
	payload := map[string]any{
		"stream_id":          streamID,
		"stream_name":        strings.TrimSpace(fmt.Sprint(item["name"])),
		"stream_slug":        strings.TrimSpace(fmt.Sprint(item["slug"])),
		"supervision_state":  strings.TrimSpace(fmt.Sprint(item["supervision_state"])),
		"supervision_reason": strings.TrimSpace(fmt.Sprint(item["supervision_reason"])),
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return 0, err
	}
	var id int64
	err = pool.QueryRow(ctx, `
		INSERT INTO alert_delivery_events (
			incident_id, stream_id, recipient, message_type, provider, provider_status, subject, payload_jsonb
		)
		VALUES ($1, $2, $3, 'recording_problem', '', 'pending', $4, $5::jsonb)
		RETURNING id
	`, nullableInt64(incidentID), streamID, strings.TrimSpace(recipient), supervisorIncidentSubject(item), string(payloadBytes)).Scan(&id)
	return id, err
}

func markAlertDeliverySent(ctx context.Context, pool *pgxpool.Pool, deliveryID int64, receipt email.DeliveryReceipt) error {
	if deliveryID <= 0 {
		return fmt.Errorf("delivery id is required")
	}
	payloadBytes, err := json.Marshal(nonNilMap(receipt.ProviderPayload))
	if err != nil {
		return err
	}
	_, err = pool.Exec(ctx, `
		UPDATE alert_delivery_events
		SET
			provider=$2,
			provider_message_id=$3,
			provider_status=$4,
			provider_payload_jsonb=$5::jsonb,
			sent_at=now(),
			error_text='',
			updated_at=now()
		WHERE id=$1
	`, deliveryID, strings.TrimSpace(receipt.Provider), strings.TrimSpace(receipt.MessageID), defaultString(strings.TrimSpace(receipt.Status), "accepted"), string(payloadBytes))
	return err
}

func markAlertDeliveryFailed(ctx context.Context, pool *pgxpool.Pool, deliveryID int64, sendErr error) error {
	if deliveryID <= 0 {
		return fmt.Errorf("delivery id is required")
	}
	_, err := pool.Exec(ctx, `
		UPDATE alert_delivery_events
		SET provider_status='failed', error_text=$2, updated_at=now()
		WHERE id=$1
	`, deliveryID, strings.TrimSpace(fmt.Sprint(sendErr)))
	return err
}

func supervisorIncidentSubject(item map[string]any) string {
	streamID := int64FromAny(item["stream_id"])
	name := strings.TrimSpace(fmt.Sprint(item["name"]))
	state := strings.TrimSpace(fmt.Sprint(item["supervision_state"]))
	if name == "" {
		name = fmt.Sprintf("stream #%d", streamID)
	}
	return fmt.Sprintf("[Stoarama] Recording %s: #%d %s", strings.ToUpper(state), streamID, name)
}

func supervisorIncidentBody(appBaseURL string, item map[string]any) string {
	streamID := int64FromAny(item["stream_id"])
	name := strings.TrimSpace(fmt.Sprint(item["name"]))
	if name == "" {
		name = fmt.Sprintf("stream #%d", streamID)
	}
	return fmt.Sprintf(
		"Stoarama detected a recording problem that requires intervention.\n\nStream: %s (#%d)\nState: %s\nReason: %s\nServer: %s\nAssignment revision: %d\nRuntime status: %s\nRelay status: %s\nLast frame: %v\nLoss rate 2h: %v\nProcess issues 2h: %v\n\nView: %s/streams/%d\n",
		name,
		streamID,
		strings.TrimSpace(fmt.Sprint(item["supervision_state"])),
		strings.TrimSpace(fmt.Sprint(item["supervision_reason"])),
		strings.TrimSpace(fmt.Sprint(item["server_id"])),
		int64FromAny(item["assignment_revision"]),
		strings.TrimSpace(fmt.Sprint(item["runtime_status"])),
		strings.TrimSpace(fmt.Sprint(item["relay_status"])),
		item["last_frame_at"],
		item["loss_rate_2h"],
		item["process_issues_2h"],
		strings.TrimRight(strings.TrimSpace(appBaseURL), "/"),
		streamID,
	)
}

func parseAnyTime(v any) *time.Time {
	switch t := v.(type) {
	case time.Time:
		t = t.UTC()
		return &t
	case *time.Time:
		if t == nil {
			return nil
		}
		ts := t.UTC()
		return &ts
	case string:
		if ts, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(t)); err == nil {
			ts = ts.UTC()
			return &ts
		}
	}
	return nil
}

func nullableInt64(v int64) any {
	if v <= 0 {
		return nil
	}
	return v
}
