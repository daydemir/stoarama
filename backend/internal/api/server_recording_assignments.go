package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/daydemir/stoarama/backend/internal/capture"
	"github.com/daydemir/stoarama/backend/internal/model"
	"github.com/daydemir/stoarama/backend/internal/util"
)

type recordingAssignRequest struct {
	ServerID string `json:"server_id"`
	Reason   string `json:"reason"`
	Actor    string `json:"actor"`
}

type recordingUnassignRequest struct {
	Confirm string `json:"confirm"`
	Reason  string `json:"reason"`
	Actor   string `json:"actor"`
}

type recordingStateRequest struct {
	RecordingState string `json:"recording_state"`
	Reason         string `json:"reason"`
	Actor          string `json:"actor"`
}

type recordingAssignmentItem struct {
	StreamID            int64      `json:"stream_id"`
	ServerID            string     `json:"server_id"`
	ExecutionClass      string     `json:"execution_class"`
	AssignmentRevision  int64      `json:"assignment_revision"`
	AssignedBy          string     `json:"assigned_by"`
	AssignedReason      string     `json:"assigned_reason"`
	AssignedAt          time.Time  `json:"assigned_at"`
	UpdatedAt           time.Time  `json:"updated_at"`
	StreamName          string     `json:"stream_name,omitempty"`
	StreamSlug          string     `json:"stream_slug,omitempty"`
	Provider            string     `json:"provider,omitempty"`
	StreamURL           string     `json:"source_url,omitempty"`
	SourcePageURL       string     `json:"source_page_url,omitempty"`
	CaptureType         string     `json:"capture_type,omitempty"`
	CaptureConfigJSON   any        `json:"execution_config_json,omitempty"`
	RelayPullURL        string     `json:"relay_pull_url,omitempty"`
	RelayStatus         string     `json:"relay_status,omitempty"`
	RecordingRuntimeAt  *time.Time `json:"last_frame_at,omitempty"`
	RecordingRuntimeErr *string    `json:"last_error_text,omitempty"`
}

type recordingAssignmentAuditIssue struct {
	Code   string `json:"code"`
	Detail string `json:"detail,omitempty"`
}

type recordingAssignmentAuditItem struct {
	StreamID                 int64                           `json:"stream_id"`
	ServerID                 string                          `json:"server_id"`
	AssignmentExecutionClass string                          `json:"assignment_execution_class"`
	RequestedExecutionClass  string                          `json:"requested_execution_class"`
	AllowedExecutionClasses  []string                        `json:"allowed_execution_classes"`
	RecordingState           string                          `json:"recording_state"`
	StreamCaptureType        string                          `json:"stream_capture_type"`
	StreamExecutionClass     string                          `json:"stream_execution_class"`
	Provider                 string                          `json:"provider,omitempty"`
	StreamSlug               string                          `json:"stream_slug,omitempty"`
	StreamName               string                          `json:"stream_name,omitempty"`
	AssignedAt               time.Time                       `json:"assigned_at"`
	LastFrameAt              *time.Time                      `json:"last_frame_at,omitempty"`
	Issues                   []recordingAssignmentAuditIssue `json:"issues"`
}

type serverExecutionCapacityHeartbeatItem struct {
	ExecutionClass string `json:"execution_class"`
	MaxActive      int    `json:"max_active"`
	Draining       bool   `json:"draining"`
}

type recordingServerHeartbeatRequest struct {
	ServerID         string                                 `json:"server_id"`
	LeaseSec         int                                    `json:"lease_sec"`
	ExecutionClasses []serverExecutionCapacityHeartbeatItem `json:"execution_classes"`
	MetadataJSON     map[string]any                         `json:"metadata_json"`
}

type recordingServerStoppedRequest struct {
	ServerID string `json:"server_id"`
}

const recordingCapacityGroupCaptureShared = "capture_shared"

const (
	youtubeRelayRouteStatusAssigned    = "assigned"
	youtubeRelayRouteStatusSourceReady = "source_ready"
	youtubeRelayRouteStatusRunning     = "running"
	youtubeRelayRouteStatusStopped     = "stopped"
	youtubeRelayRouteStatusFailed      = "failed"
)

func validYouTubeRelayRouteStatus(status string) bool {
	switch strings.TrimSpace(strings.ToLower(status)) {
	case youtubeRelayRouteStatusAssigned,
		youtubeRelayRouteStatusSourceReady,
		youtubeRelayRouteStatusRunning,
		youtubeRelayRouteStatusStopped,
		youtubeRelayRouteStatusFailed:
		return true
	default:
		return false
	}
}

func recordingCapacityGroup(executionClass string) string {
	switch normalized, ok := capture.NormalizeExecutionClass(executionClass); {
	case !ok:
		return strings.TrimSpace(executionClass)
	case normalized == capture.ExecutionClassVideoLive || normalized == capture.ExecutionClassImagePoll:
		return recordingCapacityGroupCaptureShared
	case normalized == capture.ExecutionClassYouTubeDirect:
		return capture.ExecutionClassYouTubeDirect
	case normalized == capture.ExecutionClassYouTubeRelay:
		return capture.ExecutionClassYouTubeRelay
	default:
		return normalized
	}
}

func recordingCapacityGroupModes(executionClass string) []string {
	switch recordingCapacityGroup(executionClass) {
	case recordingCapacityGroupCaptureShared:
		return []string{capture.ExecutionClassVideoLive, capture.ExecutionClassImagePoll}
	default:
		if normalized, ok := capture.NormalizeExecutionClass(executionClass); ok {
			return []string{normalized}
		}
		return []string{strings.TrimSpace(executionClass)}
	}
}

func effectiveGroupMaxActive(requestedMaxActive int, liveExecutionCapacities map[string]int, groupExecutionClasses []string) int {
	effective := requestedMaxActive
	for _, executionClass := range groupExecutionClasses {
		v, ok := liveExecutionCapacities[executionClass]
		if !ok || v <= 0 {
			continue
		}
		if effective <= 0 || v < effective {
			effective = v
		}
	}
	return effective
}

type recordingCapacityQuerier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type recordingCapacityClassState struct {
	ExecutionClass string     `json:"execution_class"`
	MaxActive      int        `json:"max_active"`
	Draining       bool       `json:"draining"`
	Active         bool       `json:"active"`
	Present        bool       `json:"present"`
	HeartbeatAt    *time.Time `json:"heartbeat_at,omitempty"`
	LeaseExpiresAt *time.Time `json:"lease_expires_at,omitempty"`
}

type recordingCapacityGroupSnapshot struct {
	ServerID                  string                        `json:"server_id"`
	CapacityGroup             string                        `json:"capacity_group"`
	ExecutionClass            string                        `json:"execution_class"`
	ExecutionClasses          []string                      `json:"execution_classes"`
	AvailableExecutionClasses []string                      `json:"available_execution_classes"`
	ExecutionClassStates      []recordingCapacityClassState `json:"execution_class_states"`
	MaxActive                 int                           `json:"max_active"`
	AssignedCount             int64                         `json:"assigned_count"`
	FreeSlots                 int64                         `json:"free_slots"`
	Draining                  bool                          `json:"draining"`
	HeartbeatAt               time.Time                     `json:"heartbeat_at"`
	LeaseExpiresAt            time.Time                     `json:"lease_expires_at"`
	Active                    bool                          `json:"active"`
	MetadataJSON              map[string]any                `json:"metadata_json"`
	Invalid                   bool                          `json:"invalid"`
	InvalidReason             string                        `json:"invalid_reason,omitempty"`
	stateByExecutionClass     map[string]recordingCapacityClassState
}

type recordingCapacitySnapshot struct {
	OrderedGroups   []*recordingCapacityGroupSnapshot
	GroupsByServer  map[string]map[string]*recordingCapacityGroupSnapshot
	AssignmentCount map[string]int64
}

type recordingAssignmentRow struct {
	StreamID       int64
	ServerID       string
	ExecutionClass string
	Revision       int64
	AssignedAt     time.Time
	UpdatedAt      time.Time
}

func (g *recordingCapacityGroupSnapshot) stateForExecutionClass(executionClass string) (recordingCapacityClassState, bool) {
	if g == nil {
		return recordingCapacityClassState{}, false
	}
	if g.stateByExecutionClass == nil {
		return recordingCapacityClassState{}, false
	}
	state, ok := g.stateByExecutionClass[executionClass]
	return state, ok
}

func recordingRequestedExecutionClass(stream model.Stream) (string, error) {
	if captureType, ok := capture.NormalizeCaptureType(stream.CaptureType); ok && captureType == capture.CaptureTypeYouTubeWatch {
		return capture.ExecutionClassYouTubeRelay, nil
	}
	executionMode := runtimeModeForStream(stream)
	executionClass := capture.ModeToExecutionClass(executionMode)
	if strings.TrimSpace(executionClass) == "" {
		return "", fmt.Errorf("unsupported stream execution class for assignment")
	}
	return executionClass, nil
}

func recordingAllowedExecutionClasses(stream model.Stream) (string, []string, error) {
	requestedExecutionClass, err := recordingRequestedExecutionClass(stream)
	if err != nil {
		return "", nil, err
	}
	return requestedExecutionClass, []string{requestedExecutionClass}, nil
}

type recordingAssignmentTarget struct {
	ServerID       string
	ExecutionClass string
	Group          *recordingCapacityGroupSnapshot
	State          recordingCapacityClassState
	AssignedCount  int64
}

func selectRecordingAssignmentTarget(capacitySnapshot *recordingCapacitySnapshot, serverID string, executionClassCandidates []string) (*recordingAssignmentTarget, map[string]any) {
	if len(executionClassCandidates) == 0 {
		return nil, map[string]any{
			"error":      "unsupported stream execution class for assignment",
			"error_code": "recording_mode_unsupported",
		}
	}
	if strings.TrimSpace(serverID) != "" {
		serverGroups := capacitySnapshot.GroupsByServer[strings.TrimSpace(serverID)]
		for _, candidate := range executionClassCandidates {
			if serverGroups == nil {
				continue
			}
			groupSnapshot := serverGroups[recordingCapacityGroup(candidate)]
			if groupSnapshot == nil {
				continue
			}
			state, ok := groupSnapshot.stateForExecutionClass(candidate)
			if !ok || !state.Present || !state.Active {
				continue
			}
			if groupSnapshot.Invalid {
				return nil, map[string]any{
					"error":           "selected server has invalid shared capacity state",
					"error_code":      "invalid_capacity_state",
					"server_id":       strings.TrimSpace(serverID),
					"execution_class": candidate,
					"capacity_group":  groupSnapshot.CapacityGroup,
					"invalid_reason":  groupSnapshot.InvalidReason,
				}
			}
			if state.Draining {
				return nil, map[string]any{
					"error":           "selected server execution class is draining",
					"error_code":      "server_draining",
					"server_id":       strings.TrimSpace(serverID),
					"execution_class": candidate,
				}
			}
			return &recordingAssignmentTarget{
				ServerID:       strings.TrimSpace(serverID),
				ExecutionClass: candidate,
				Group:          groupSnapshot,
				State:          state,
				AssignedCount:  groupSnapshot.AssignedCount,
			}, nil
		}
		return nil, map[string]any{
			"error":                     "selected server has no live capacity for execution_class",
			"error_code":                "server_unavailable",
			"server_id":                 strings.TrimSpace(serverID),
			"desired_execution_classes": executionClassCandidates,
		}
	}

	type candidateRow struct {
		serverID       string
		executionClass string
		group          *recordingCapacityGroupSnapshot
		state          recordingCapacityClassState
		priority       int
	}
	candidates := make([]candidateRow, 0, len(capacitySnapshot.OrderedGroups))
	for _, groupSnapshot := range capacitySnapshot.OrderedGroups {
		if groupSnapshot == nil || !groupSnapshot.Active || groupSnapshot.Draining || groupSnapshot.Invalid || groupSnapshot.FreeSlots <= 0 {
			continue
		}
		for idx, candidate := range executionClassCandidates {
			state, ok := groupSnapshot.stateForExecutionClass(candidate)
			if !ok || !state.Present || !state.Active || state.Draining {
				continue
			}
			candidates = append(candidates, candidateRow{
				serverID:       groupSnapshot.ServerID,
				executionClass: candidate,
				group:          groupSnapshot,
				state:          state,
				priority:       idx,
			})
			break
		}
	}
	if len(candidates) == 0 {
		return nil, map[string]any{
			"error":                     "no recording server has free capacity for execution_class",
			"error_code":                "server_unavailable",
			"desired_execution_classes": executionClassCandidates,
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].priority != candidates[j].priority {
			return candidates[i].priority < candidates[j].priority
		}
		if candidates[i].group.FreeSlots != candidates[j].group.FreeSlots {
			return candidates[i].group.FreeSlots > candidates[j].group.FreeSlots
		}
		return candidates[i].serverID < candidates[j].serverID
	})
	selected := candidates[0]
	return &recordingAssignmentTarget{
		ServerID:       selected.serverID,
		ExecutionClass: selected.executionClass,
		Group:          selected.group,
		State:          selected.state,
		AssignedCount:  selected.group.AssignedCount,
	}, nil
}

func (s *Server) assignRecordingStreamTx(ctx context.Context, tx pgx.Tx, stream model.Stream, serverID string, actor string, reason string) (map[string]any, int, error) {
	if stream.RecordingState != model.RecordingStateOn {
		return map[string]any{
			"error":           "stream recording_state must be on before assignment",
			"error_code":      "recording_state_off",
			"recording_state": stream.RecordingState,
			"stream_id":       stream.ID,
		}, http.StatusConflict, nil
	}
	requestedExecutionClass, executionClassCandidates, err := recordingAllowedExecutionClasses(stream)
	if err != nil {
		return map[string]any{
			"error":           "unsupported stream execution class for assignment",
			"error_code":      "recording_mode_unsupported",
			"execution_class": requestedExecutionClass,
			"stream_id":       stream.ID,
		}, http.StatusConflict, nil
	}
	capacitySnapshot, err := loadRecordingCapacitySnapshot(ctx, tx, false, strings.TrimSpace(serverID), true)
	if err != nil {
		return nil, 0, fmt.Errorf("load server capacity snapshot: %w", err)
	}
	target, conflict := selectRecordingAssignmentTarget(capacitySnapshot, serverID, executionClassCandidates)
	if conflict != nil {
		conflict["stream_id"] = stream.ID
		if _, ok := conflict["execution_class"]; !ok {
			conflict["execution_class"] = requestedExecutionClass
		}
		if _, ok := conflict["server_id"]; !ok && strings.TrimSpace(serverID) != "" {
			conflict["server_id"] = strings.TrimSpace(serverID)
		}
		return conflict, http.StatusConflict, nil
	}

	existingServerID := ""
	existingExecutionClass := ""
	existingRevision := int64(0)
	existing := false
	if err := tx.QueryRow(ctx, `
		SELECT server_id, execution_class, assignment_revision
		FROM recording_assignments
		WHERE stream_id=$1
		FOR UPDATE
	`, stream.ID).Scan(&existingServerID, &existingExecutionClass, &existingRevision); err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			return nil, 0, fmt.Errorf("load existing assignment: %w", err)
		}
	} else {
		existing = true
	}

	assignedCount := target.AssignedCount
	if existing && existingServerID == target.ServerID && recordingCapacityGroup(existingExecutionClass) == target.Group.CapacityGroup {
		if assignedCount > 0 {
			assignedCount--
		}
	}
	if int64(target.Group.MaxActive) <= assignedCount && !(existing && existingServerID == target.ServerID && existingExecutionClass == target.ExecutionClass) {
		return map[string]any{
			"error":           "server execution class capacity reached",
			"error_code":      "capacity_reached",
			"server_id":       target.ServerID,
			"execution_class": target.ExecutionClass,
			"capacity_group":  target.Group.CapacityGroup,
			"max_active":      target.Group.MaxActive,
			"assigned_count":  assignedCount,
			"stream_id":       stream.ID,
		}, http.StatusConflict, nil
	}

	nextRevision := int64(1)
	eventType := "assign"
	if existing {
		nextRevision = existingRevision
		if existingServerID != target.ServerID || existingExecutionClass != target.ExecutionClass {
			nextRevision++
			eventType = "reassign"
		}
	}

	var relayRoute map[string]any
	if target.ExecutionClass == capture.ExecutionClassYouTubeRelay {
		relayRoute, err = s.allocateYouTubeRelayRouteTx(ctx, tx, stream, target.ServerID, nextRevision, actor, reason)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return map[string]any{
					"error":           "no active youtube relay source capacity",
					"error_code":      "youtube_relay_source_unavailable",
					"server_id":       target.ServerID,
					"execution_class": target.ExecutionClass,
					"stream_id":       stream.ID,
				}, http.StatusConflict, nil
			}
			return nil, 0, fmt.Errorf("allocate youtube relay route: %w", err)
		}
	} else if existing && strings.TrimSpace(existingExecutionClass) == capture.ExecutionClassYouTubeRelay {
		if err := s.clearYouTubeRelayRouteTx(ctx, tx, stream.ID, actor, "assignment execution class changed"); err != nil {
			return nil, 0, fmt.Errorf("clear youtube relay route: %w", err)
		}
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO recording_assignments (
			stream_id, server_id, execution_class, assignment_revision,
			assigned_by, assigned_reason, assigned_at, updated_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, now(), now())
		ON CONFLICT (stream_id)
		DO UPDATE SET
			server_id=EXCLUDED.server_id,
			execution_class=EXCLUDED.execution_class,
			assignment_revision=EXCLUDED.assignment_revision,
			assigned_by=EXCLUDED.assigned_by,
			assigned_reason=EXCLUDED.assigned_reason,
			assigned_at=now(),
			updated_at=now()
	`, stream.ID, target.ServerID, target.ExecutionClass, nextRevision, actor, reason); err != nil {
		return nil, 0, fmt.Errorf("upsert assignment: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO recording_assignment_events (
			stream_id, server_id, execution_class, assignment_revision, event_type, actor, reason, metadata_jsonb
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, '{}'::jsonb)
	`, stream.ID, target.ServerID, target.ExecutionClass, nextRevision, eventType, actor, reason); err != nil {
		return nil, 0, fmt.Errorf("insert assignment event: %w", err)
	}

	freeSlots := target.Group.MaxActive - int(assignedCount+1)
	if freeSlots < 0 {
		freeSlots = 0
	}
	leaseExpiresAt := target.Group.LeaseExpiresAt
	if target.State.LeaseExpiresAt != nil {
		leaseExpiresAt = target.State.LeaseExpiresAt.UTC()
	}
	return map[string]any{
		"ok":                  true,
		"stream_id":           stream.ID,
		"server_id":           target.ServerID,
		"execution_class":     target.ExecutionClass,
		"assignment_revision": nextRevision,
		"event_type":          eventType,
		"capacity_group":      target.Group.CapacityGroup,
		"max_active":          target.Group.MaxActive,
		"assigned_count":      assignedCount + 1,
		"free_slots":          freeSlots,
		"lease_expires_at":    leaseExpiresAt,
		"youtube_relay_route": relayRoute,
	}, 0, nil
}

func stringInSlice(values []string, needle string) bool {
	for _, value := range values {
		if strings.TrimSpace(value) == strings.TrimSpace(needle) {
			return true
		}
	}
	return false
}

func buildRecordingAssignmentAuditIssues(stream model.Stream, assignment recordingAssignmentRow, capacitySnapshot *recordingCapacitySnapshot) []recordingAssignmentAuditIssue {
	issues := make([]recordingAssignmentAuditIssue, 0, 4)
	if stream.RecordingState != model.RecordingStateOn {
		issues = append(issues, recordingAssignmentAuditIssue{
			Code:   "recording_state_off",
			Detail: "stream is assigned but recording_state is not on",
		})
	}
	_, allowedExecutionClasses, err := recordingAllowedExecutionClasses(stream)
	if err != nil {
		issues = append(issues, recordingAssignmentAuditIssue{
			Code:   "stream_execution_unsupported",
			Detail: err.Error(),
		})
		return issues
	}
	if !stringInSlice(allowedExecutionClasses, assignment.ExecutionClass) {
		issues = append(issues, recordingAssignmentAuditIssue{
			Code:   "assignment_execution_class_mismatch",
			Detail: fmt.Sprintf("assignment execution_class=%s not allowed for stream", assignment.ExecutionClass),
		})
	}
	if capacitySnapshot == nil {
		return issues
	}
	serverGroups := capacitySnapshot.GroupsByServer[strings.TrimSpace(assignment.ServerID)]
	if serverGroups == nil {
		issues = append(issues, recordingAssignmentAuditIssue{
			Code:   "server_unavailable",
			Detail: "assigned server has no live recording capacity heartbeat",
		})
		return issues
	}
	groupSnapshot := serverGroups[recordingCapacityGroup(assignment.ExecutionClass)]
	if groupSnapshot == nil {
		issues = append(issues, recordingAssignmentAuditIssue{
			Code:   "server_unavailable",
			Detail: "assigned server has no live capacity group for assignment execution_class",
		})
		return issues
	}
	if groupSnapshot.Invalid {
		issues = append(issues, recordingAssignmentAuditIssue{
			Code:   "invalid_capacity_state",
			Detail: groupSnapshot.InvalidReason,
		})
		return issues
	}
	state, ok := groupSnapshot.stateForExecutionClass(assignment.ExecutionClass)
	if !ok || !state.Active {
		issues = append(issues, recordingAssignmentAuditIssue{
			Code:   "server_unavailable",
			Detail: "assignment execution_class is not active on assigned server",
		})
	}
	return issues
}

func recordingSharedCapacityInvalidReason(rows map[string]recordingCapacityClassState, executionClasses []string) string {
	if len(executionClasses) <= 1 {
		return ""
	}
	expected := -1
	present := 0
	for _, executionClass := range executionClasses {
		state, ok := rows[executionClass]
		if !ok || !state.Present {
			continue
		}
		present++
		if expected < 0 {
			expected = state.MaxActive
			continue
		}
		if state.MaxActive != expected {
			return fmt.Sprintf("shared group capacity mismatch across %s", strings.Join(executionClasses, ","))
		}
	}
	if present < 2 {
		return ""
	}
	return ""
}

func validateRecordingHeartbeatSharedCapacity(shared map[string]int) error {
	videoLive, hasVideoLive := shared[capture.ExecutionClassVideoLive]
	imagePoll, hasImagePoll := shared[capture.ExecutionClassImagePoll]
	if hasVideoLive && hasImagePoll && videoLive != imagePoll {
		return fmt.Errorf(
			"shared capture execution classes %s and %s must advertise matching max_active",
			capture.ExecutionClassVideoLive,
			capture.ExecutionClassImagePoll,
		)
	}
	return nil
}

func recordingHeartbeatSharedCapacities(items []serverExecutionCapacityHeartbeatItem) map[string]int {
	shared := map[string]int{}
	for _, item := range items {
		executionClass, ok := capture.NormalizeExecutionClass(item.ExecutionClass)
		if !ok {
			continue
		}
		if executionClass != capture.ExecutionClassVideoLive && executionClass != capture.ExecutionClassImagePoll {
			continue
		}
		shared[executionClass] = item.MaxActive
	}
	return shared
}

func loadRecordingCapacitySnapshot(ctx context.Context, q recordingCapacityQuerier, includeInactive bool, serverID string, forUpdate bool) (*recordingCapacitySnapshot, error) {
	whereParts := make([]string, 0, 2)
	args := make([]any, 0, 1)
	if includeInactive {
		whereParts = append(whereParts, "1=1")
	} else {
		whereParts = append(whereParts, "c.lease_expires_at > now()")
	}
	if serverID = strings.TrimSpace(serverID); serverID != "" {
		args = append(args, serverID)
		whereParts = append(whereParts, fmt.Sprintf("c.server_id=$%d", len(args)))
	}
	lockClause := ""
	if forUpdate {
		lockClause = " FOR UPDATE"
	}
	rows, err := q.Query(ctx, fmt.Sprintf(`
		SELECT
			c.server_id,
			c.execution_class,
			c.max_active,
			c.draining,
			c.heartbeat_at,
			c.lease_expires_at,
			c.metadata_jsonb
		FROM server_execution_capacity c
		WHERE %s
		ORDER BY c.server_id ASC, c.execution_class ASC%s
	`, strings.Join(whereParts, " AND "), lockClause), args...)
	if err != nil {
		return nil, fmt.Errorf("query recording server capacity: %w", err)
	}
	defer rows.Close()

	type capRow struct {
		serverID       string
		executionClass string
		maxActive      int
		draining       bool
		heartbeatAt    time.Time
		leaseExpiresAt time.Time
		metadataBytes  []byte
	}
	capRows := make([]capRow, 0, 64)
	for rows.Next() {
		var row capRow
		if err := rows.Scan(
			&row.serverID,
			&row.executionClass,
			&row.maxActive,
			&row.draining,
			&row.heartbeatAt,
			&row.leaseExpiresAt,
			&row.metadataBytes,
		); err != nil {
			return nil, fmt.Errorf("scan recording server capacity: %w", err)
		}
		capRows = append(capRows, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate recording server capacity: %w", err)
	}

	assignWhere := "1=1"
	assignArgs := make([]any, 0, 1)
	if serverID != "" {
		assignArgs = append(assignArgs, serverID)
		assignWhere = fmt.Sprintf("server_id=$%d", len(assignArgs))
	}
	assignRows, err := q.Query(ctx, fmt.Sprintf(`
		SELECT server_id, execution_class, COUNT(*)::bigint
		FROM recording_assignments
		WHERE %s
		GROUP BY server_id, execution_class
	`, assignWhere), assignArgs...)
	if err != nil {
		return nil, fmt.Errorf("query recording assignment counts: %w", err)
	}
	defer assignRows.Close()

	assignedByServerClass := map[string]int64{}
	for assignRows.Next() {
		var assignServerID, assignExecutionClass string
		var count int64
		if err := assignRows.Scan(&assignServerID, &assignExecutionClass, &count); err != nil {
			return nil, fmt.Errorf("scan recording assignment counts: %w", err)
		}
		assignedByServerClass[strings.TrimSpace(assignServerID)+"|"+strings.TrimSpace(assignExecutionClass)] = count
	}
	if err := assignRows.Err(); err != nil {
		return nil, fmt.Errorf("iterate recording assignment counts: %w", err)
	}

	now := time.Now().UTC()
	serverRows := map[string]map[string]capRow{}
	groupSeen := map[string]struct{}{}
	groupOrder := make([]string, 0, len(capRows))
	for _, row := range capRows {
		serverKey := strings.TrimSpace(row.serverID)
		executionClassKey := strings.TrimSpace(row.executionClass)
		if serverKey == "" || executionClassKey == "" {
			continue
		}
		if _, ok := serverRows[serverKey]; !ok {
			serverRows[serverKey] = map[string]capRow{}
		}
		serverRows[serverKey][executionClassKey] = row
		groupKey := serverKey + "|" + recordingCapacityGroup(executionClassKey)
		if _, ok := groupSeen[groupKey]; ok {
			continue
		}
		groupSeen[groupKey] = struct{}{}
		groupOrder = append(groupOrder, groupKey)
	}
	sort.Strings(groupOrder)

	snapshot := &recordingCapacitySnapshot{
		OrderedGroups:   make([]*recordingCapacityGroupSnapshot, 0, len(groupOrder)),
		GroupsByServer:  map[string]map[string]*recordingCapacityGroupSnapshot{},
		AssignmentCount: assignedByServerClass,
	}
	for _, groupKey := range groupOrder {
		parts := strings.SplitN(groupKey, "|", 2)
		if len(parts) != 2 {
			continue
		}
		serverKey := parts[0]
		group := parts[1]
		serverCapRows := serverRows[serverKey]
		if serverCapRows == nil {
			continue
		}
		groupExecutionClasses := recordingCapacityGroupModes(group)
		sort.Strings(groupExecutionClasses)

		stateByExecutionClass := make(map[string]recordingCapacityClassState, len(groupExecutionClasses))
		states := make([]recordingCapacityClassState, 0, len(groupExecutionClasses))
		availableExecutionClasses := make([]string, 0, len(groupExecutionClasses))
		liveExecutionCapacities := map[string]int{}
		assignedCount := int64(0)
		groupHeartbeatAt := time.Time{}
		groupLeaseExpiresAt := time.Time{}
		groupActive := false
		groupDraining := true
		metadata := map[string]any{}

		for _, executionClass := range groupExecutionClasses {
			row, ok := serverCapRows[executionClass]
			state := recordingCapacityClassState{
				ExecutionClass: executionClass,
				Present:        ok,
			}
			if ok {
				heartbeatAt := row.heartbeatAt.UTC()
				leaseExpiresAt := row.leaseExpiresAt.UTC()
				state.MaxActive = row.maxActive
				state.Draining = row.draining
				state.Active = leaseExpiresAt.After(now)
				state.HeartbeatAt = &heartbeatAt
				state.LeaseExpiresAt = &leaseExpiresAt
				if state.Active {
					groupActive = true
				}
				if heartbeatAt.After(groupHeartbeatAt) {
					groupHeartbeatAt = heartbeatAt
				}
				if leaseExpiresAt.After(groupLeaseExpiresAt) {
					groupLeaseExpiresAt = leaseExpiresAt
				}
				if len(metadata) == 0 && len(row.metadataBytes) > 0 {
					if err := json.Unmarshal(row.metadataBytes, &metadata); err != nil {
						return nil, fmt.Errorf("decode recording server capacity metadata: %w", err)
					}
				}
				assignedCount += assignedByServerClass[serverKey+"|"+executionClass]
				if state.Active && !state.Draining && state.MaxActive > 0 {
					availableExecutionClasses = append(availableExecutionClasses, executionClass)
					liveExecutionCapacities[executionClass] = state.MaxActive
					groupDraining = false
				}
			}
			stateByExecutionClass[executionClass] = state
			states = append(states, state)
		}

		sort.Strings(availableExecutionClasses)
		invalidReason := ""
		if group == recordingCapacityGroupCaptureShared {
			invalidReason = recordingSharedCapacityInvalidReason(stateByExecutionClass, groupExecutionClasses)
		}
		invalid := strings.TrimSpace(invalidReason) != ""
		effectiveMaxActive := effectiveGroupMaxActive(0, liveExecutionCapacities, groupExecutionClasses)
		freeSlots := int64(effectiveMaxActive) - assignedCount
		if freeSlots < 0 {
			freeSlots = 0
		}
		if invalid {
			effectiveMaxActive = 0
			freeSlots = 0
			groupDraining = true
			availableExecutionClasses = nil
		}

		groupSnapshot := &recordingCapacityGroupSnapshot{
			ServerID:                  serverKey,
			CapacityGroup:             group,
			ExecutionClass:            group,
			ExecutionClasses:          groupExecutionClasses,
			AvailableExecutionClasses: availableExecutionClasses,
			ExecutionClassStates:      states,
			MaxActive:                 effectiveMaxActive,
			AssignedCount:             assignedCount,
			FreeSlots:                 freeSlots,
			Draining:                  groupDraining,
			HeartbeatAt:               groupHeartbeatAt,
			LeaseExpiresAt:            groupLeaseExpiresAt,
			Active:                    groupActive,
			MetadataJSON:              metadata,
			Invalid:                   invalid,
			InvalidReason:             invalidReason,
			stateByExecutionClass:     stateByExecutionClass,
		}
		if _, ok := snapshot.GroupsByServer[serverKey]; !ok {
			snapshot.GroupsByServer[serverKey] = map[string]*recordingCapacityGroupSnapshot{}
		}
		snapshot.GroupsByServer[serverKey][group] = groupSnapshot
		snapshot.OrderedGroups = append(snapshot.OrderedGroups, groupSnapshot)
	}

	return snapshot, nil
}

func (s *Server) handleRecordingStreamAssign(w http.ResponseWriter, r *http.Request) {
	streamID, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	var req recordingAssignRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	serverID := strings.TrimSpace(req.ServerID)
	if serverID == "" {
		util.WriteError(w, http.StatusBadRequest, "server_id is required")
		return
	}
	reason := strings.TrimSpace(req.Reason)
	if reason == "" {
		reason = "operator assign"
	}
	actor := strings.TrimSpace(req.Actor)
	if actor == "" {
		actor = "api.recording_assign"
	}
	principalEmail := ""
	principalAuthType := ""
	principalSession := false
	if principal, ok := accountPrincipalFromContext(r.Context()); ok {
		actor = accountActorLabel(principal, actor)
		principalEmail = strings.TrimSpace(principal.Email)
		principalAuthType = strings.TrimSpace(principal.AuthType)
		principalSession = principal.SessionID != nil
	}
	log.Printf(
		"recording assign request stream_id=%d server_id=%q actor=%q host=%s origin=%q referer=%q principal_email=%q principal_auth_type=%q principal_session=%t",
		streamID,
		serverID,
		actor,
		strings.TrimSpace(r.Host),
		strings.TrimSpace(r.Header.Get("Origin")),
		strings.TrimSpace(r.Header.Get("Referer")),
		principalEmail,
		principalAuthType,
		principalSession,
	)

	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("begin assign tx: %v", err))
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()

	stream, err := s.loadStreamForAssignmentTx(r.Context(), tx, streamID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			util.WriteError(w, http.StatusNotFound, "stream not found")
			return
		}
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load stream: %v", err))
		return
	}
	result, status, err := s.assignRecordingStreamTx(r.Context(), tx, stream, serverID, actor, reason)
	if err != nil {
		log.Printf("recording assign error stream_id=%d server_id=%q actor=%q err=%v", streamID, serverID, actor, err)
		util.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if status > 0 {
		log.Printf("recording assign conflict stream_id=%d server_id=%q actor=%q status=%d result=%v", streamID, serverID, actor, status, result)
		util.WriteJSON(w, status, result)
		return
	}
	if err := tx.Commit(r.Context()); err != nil {
		log.Printf("recording assign commit error stream_id=%d server_id=%q actor=%q err=%v", streamID, serverID, actor, err)
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("commit assign tx: %v", err))
		return
	}
	log.Printf("recording assign success stream_id=%d server_id=%q actor=%q", streamID, serverID, actor)
	util.WriteJSON(w, http.StatusOK, result)
}

func (s *Server) handleRecordingStreamUnassign(w http.ResponseWriter, r *http.Request) {
	streamID, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	var req recordingUnassignRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	expectedConfirm := fmt.Sprintf("unassign:%d", streamID)
	if strings.TrimSpace(req.Confirm) != expectedConfirm {
		util.WriteError(w, http.StatusBadRequest, fmt.Sprintf("invalid confirm token; expected %q", expectedConfirm))
		return
	}
	reason := strings.TrimSpace(req.Reason)
	if reason == "" {
		reason = "operator unassign"
	}
	actor := strings.TrimSpace(req.Actor)
	if actor == "" {
		actor = "api.recording_unassign"
	}
	if principal, ok := accountPrincipalFromContext(r.Context()); ok {
		actor = accountActorLabel(principal, actor)
	}

	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("begin unassign tx: %v", err))
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()
	assignment, existed, err := s.unassignRecordingStreamTx(r.Context(), tx, streamID, actor, reason)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("unassign recording stream: %v", err))
		return
	}
	if !existed {
		util.WriteError(w, http.StatusNotFound, "assignment not found")
		return
	}

	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("commit unassign tx: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{
		"ok":                       true,
		"stream_id":                streamID,
		"unassigned":               true,
		"previous_server_id":       assignment.ServerID,
		"previous_execution_class": assignment.ExecutionClass,
		"assignment_revision":      assignment.Revision,
	})
}

func (s *Server) handleRecordingStreamState(w http.ResponseWriter, r *http.Request) {
	streamID, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	var req recordingStateRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	state, ok := parseRecordingState(strings.TrimSpace(req.RecordingState))
	if !ok {
		util.WriteError(w, http.StatusBadRequest, "invalid recording_state; expected off|on")
		return
	}
	reason := strings.TrimSpace(req.Reason)
	if reason == "" {
		reason = "recording state update"
	}
	actor := strings.TrimSpace(req.Actor)
	if actor == "" {
		actor = "api.recording_state"
	}

	hasAuth := false
	serviceToken := strings.TrimSpace(s.cfg.ServiceToken)
	if serviceToken != "" {
		got := strings.TrimSpace(r.Header.Get("Authorization"))
		if strings.HasPrefix(got, "Bearer ") {
			token := strings.TrimSpace(strings.TrimPrefix(got, "Bearer "))
			if token != "" && token == serviceToken {
				hasAuth = true
			}
		}
	}
	if !hasAuth {
		if principal, err := s.authenticateAccountSessionRequest(r); err == nil {
			hasAuth = true
			ctx := context.WithValue(r.Context(), accountPrincipalContextKey, principal)
			r = r.WithContext(ctx)
		}
	}
	if state == model.RecordingStateOff && !hasAuth {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	if state == model.RecordingStateOn && !hasAuth {
		actor = "public.recording_state"
		reason = "public recording state start request"
	}

	if principal, ok := accountPrincipalFromContext(r.Context()); ok {
		actor = accountActorLabel(principal, actor)
	}

	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("begin tx: %v", err))
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()

	result, status, err := s.setStreamRecordingStateTx(r.Context(), tx, streamID, state, actor, reason)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if status > 0 {
		util.WriteJSON(w, status, result)
		return
	}
	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("commit stream recording_state: %v", err))
		return
	}
	updated, err := s.getStreamByID(r.Context(), streamID)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("reload stream: %v", err))
		return
	}
	result["stream"] = updated
	util.WriteJSON(w, http.StatusOK, result)
}

func loadRecordingAssignmentTx(ctx context.Context, tx pgx.Tx, streamID int64) (recordingAssignmentRow, bool, error) {
	var assignment recordingAssignmentRow
	err := tx.QueryRow(ctx, `
		SELECT stream_id, server_id, execution_class, assignment_revision, assigned_at, updated_at
		FROM recording_assignments
		WHERE stream_id=$1
		FOR UPDATE
	`, streamID).Scan(
		&assignment.StreamID,
		&assignment.ServerID,
		&assignment.ExecutionClass,
		&assignment.Revision,
		&assignment.AssignedAt,
		&assignment.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return recordingAssignmentRow{}, false, nil
		}
		return recordingAssignmentRow{}, false, err
	}
	return assignment, true, nil
}

func (s *Server) setStreamRecordingStateTx(ctx context.Context, tx pgx.Tx, streamID int64, state model.RecordingState, actor string, reason string) (map[string]any, int, error) {
	stream, err := s.loadStreamForAssignmentTx(ctx, tx, streamID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return map[string]any{"error": "stream not found"}, http.StatusNotFound, nil
		}
		return nil, 0, fmt.Errorf("load stream: %v", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE streams
		SET recording_state=$2, updated_at=now()
		WHERE id=$1
	`, streamID, string(state)); err != nil {
		return nil, 0, fmt.Errorf("update stream recording_state: %v", err)
	}
	assignment, existed, err := loadRecordingAssignmentTx(ctx, tx, streamID)
	if err != nil {
		return nil, 0, fmt.Errorf("load assignment: %v", err)
	}
	var serverID string
	switch state {
	case model.RecordingStateOff:
		if existed {
			serverID = assignment.ServerID
			if _, _, err := s.unassignRecordingStreamTx(ctx, tx, streamID, actor, reason); err != nil {
				return nil, 0, fmt.Errorf("unassign stream: %v", err)
			}
		}
	case model.RecordingStateOn:
		stream.RecordingState = model.RecordingStateOn
		if !existed {
			result, status, err := s.assignRecordingStreamTx(ctx, tx, stream, "", actor, reason)
			if err != nil {
				return nil, 0, err
			}
			if status > 0 {
				return result, status, nil
			}
			serverID = strings.TrimSpace(fmt.Sprint(result["server_id"]))
		} else {
			serverID = assignment.ServerID
		}
	}
	return map[string]any{
		"ok":              true,
		"stream_id":       streamID,
		"recording_state": string(state),
		"server_id":       strings.TrimSpace(serverID),
	}, 0, nil
}

func (s *Server) unassignRecordingStreamTx(ctx context.Context, tx pgx.Tx, streamID int64, actor string, reason string) (recordingAssignmentRow, bool, error) {
	assignment, existed, err := loadRecordingAssignmentTx(ctx, tx, streamID)
	if err != nil || !existed {
		return assignment, existed, err
	}
	if strings.TrimSpace(assignment.ExecutionClass) == capture.ExecutionClassYouTubeRelay {
		if err := s.clearYouTubeRelayRouteTx(ctx, tx, streamID, actor, reason); err != nil {
			return recordingAssignmentRow{}, false, err
		}
	}
	if _, err := tx.Exec(ctx, `DELETE FROM recording_assignments WHERE stream_id=$1`, streamID); err != nil {
		return recordingAssignmentRow{}, false, err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO recording_assignment_events (
			stream_id, server_id, execution_class, assignment_revision, event_type, actor, reason, metadata_jsonb
		)
		VALUES ($1, $2, $3, $4, 'unassign', $5, $6, '{}'::jsonb)
	`, streamID, assignment.ServerID, assignment.ExecutionClass, assignment.Revision, actor, reason); err != nil {
		return recordingAssignmentRow{}, false, err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE recording_process_runs
		SET
			status='stopped',
			stop_reason=$2,
			stopped_at=COALESCE(stopped_at, now()),
			updated_at=now()
		WHERE stream_id=$1
		  AND status IN ('starting', 'running')
		  AND stopped_at IS NULL
	`, streamID, reason); err != nil {
		return recordingAssignmentRow{}, false, err
	}
	return assignment, true, nil
}

func (s *Server) handleRecordingAssignmentsList(w http.ResponseWriter, r *http.Request) {
	serverID := strings.TrimSpace(r.URL.Query().Get("server_id"))
	executionClassRaw := strings.TrimSpace(r.URL.Query().Get("execution_class"))
	executionClass, ok := capture.NormalizeExecutionClass(executionClassRaw)
	limit := parseIntQuery(r, "limit", 500, 1, 2000)
	offset := parseIntQuery(r, "offset", 0, 0, 1000000)

	where := []string{"1=1"}
	args := make([]any, 0, 4)
	if serverID != "" {
		args = append(args, serverID)
		where = append(where, fmt.Sprintf("ra.server_id=$%d", len(args)))
	}
	if executionClassRaw != "" {
		if !ok {
			util.WriteError(w, http.StatusBadRequest, "invalid execution_class")
			return
		}
		args = append(args, executionClass)
		where = append(where, fmt.Sprintf("ra.execution_class=$%d", len(args)))
	}
	countArgs := append([]any(nil), args...)
	var total int
	if err := s.pool.QueryRow(r.Context(), fmt.Sprintf(`
		SELECT COUNT(*)::int
		FROM recording_assignments ra
		WHERE %s
	`, strings.Join(where, " AND ")), countArgs...).Scan(&total); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("count recording assignments: %v", err))
		return
	}

	args = append(args, limit, offset)

	rows, err := s.pool.Query(r.Context(), fmt.Sprintf(`
		SELECT
			ra.stream_id,
			ra.server_id,
			ra.execution_class,
			ra.assignment_revision,
			ra.assigned_by,
			ra.assigned_reason,
			ra.assigned_at,
			ra.updated_at,
			s.name,
			s.slug,
			s.provider,
			s.source_url,
			s.source_page_url,
			s.capture_type,
			s.execution_config_jsonb,
			COALESCE(yr.relay_pull_url, '') AS relay_pull_url,
			COALESCE(yr.status, '') AS relay_status,
			rt.last_frame_at,
			rt.last_error_text
		FROM recording_assignments ra
		JOIN streams s ON s.id=ra.stream_id
		LEFT JOIN youtube_relay_routes yr ON yr.stream_id=ra.stream_id
		LEFT JOIN stream_capture_runtime rt ON rt.stream_id=ra.stream_id
		WHERE %s
		ORDER BY ra.server_id ASC, ra.execution_class ASC, ra.stream_id ASC
		LIMIT $%d OFFSET $%d
	`, strings.Join(where, " AND "), len(args)-1, len(args)), args...)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("query recording assignments: %v", err))
		return
	}
	defer rows.Close()

	items := make([]recordingAssignmentItem, 0, limit)
	for rows.Next() {
		var it recordingAssignmentItem
		var cfgBytes []byte
		if err := rows.Scan(
			&it.StreamID, &it.ServerID, &it.ExecutionClass, &it.AssignmentRevision, &it.AssignedBy,
			&it.AssignedReason, &it.AssignedAt, &it.UpdatedAt,
			&it.StreamName, &it.StreamSlug, &it.Provider, &it.StreamURL, &it.SourcePageURL,
			&it.CaptureType, &cfgBytes, &it.RelayPullURL, &it.RelayStatus, &it.RecordingRuntimeAt, &it.RecordingRuntimeErr,
		); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan recording assignment: %v", err))
			return
		}
		if len(cfgBytes) > 0 {
			cfg := map[string]any{}
			if err := json.Unmarshal(cfgBytes, &cfg); err == nil {
				it.CaptureConfigJSON = cfg
			}
		}
		if it.CaptureConfigJSON == nil {
			it.CaptureConfigJSON = map[string]any{}
		}
		items = append(items, it)
	}
	if rows.Err() != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("iterate recording assignments: %v", rows.Err()))
		return
	}

	util.WriteJSON(w, http.StatusOK, map[string]any{
		"items":  items,
		"limit":  limit,
		"offset": offset,
		"total":  total,
	})
}

func (s *Server) handleRecordingAssignmentsAudit(w http.ResponseWriter, r *http.Request) {
	serverID := strings.TrimSpace(r.URL.Query().Get("server_id"))
	executionClassRaw := strings.TrimSpace(r.URL.Query().Get("execution_class"))
	executionClass := ""
	if executionClassRaw != "" {
		var ok bool
		executionClass, ok = capture.NormalizeExecutionClass(executionClassRaw)
		if !ok {
			util.WriteError(w, http.StatusBadRequest, "invalid execution_class")
			return
		}
	}
	limit := parseIntQuery(r, "limit", 500, 1, 2000)
	offset := parseIntQuery(r, "offset", 0, 0, 1000000)

	where := []string{"1=1"}
	args := make([]any, 0, 4)
	if serverID != "" {
		args = append(args, serverID)
		where = append(where, fmt.Sprintf("ra.server_id=$%d", len(args)))
	}
	if executionClass != "" {
		args = append(args, executionClass)
		where = append(where, fmt.Sprintf("ra.execution_class=$%d", len(args)))
	}
	args = append(args, limit, offset)

	capacitySnapshot, err := loadRecordingCapacitySnapshot(r.Context(), s.pool, false, "", false)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load capacity snapshot: %v", err))
		return
	}

	rows, err := s.pool.Query(r.Context(), fmt.Sprintf(`
		SELECT
			ra.stream_id,
			ra.server_id,
			ra.execution_class,
			ra.assignment_revision,
			ra.assigned_at,
			s.provider,
			s.external_id,
			s.name,
			s.slug,
			s.source_url,
			s.source_page_url,
			s.source_family,
			s.metadata_jsonb,
			s.recording_state,
			s.recording_failed_reason,
			s.recording_failed_at,
			s.capture_type,
			s.execution_class,
			s.execution_config_jsonb,
			s.tags,
			s.created_at,
			s.updated_at,
			rt.last_frame_at
		FROM recording_assignments ra
		JOIN streams s ON s.id=ra.stream_id
		LEFT JOIN stream_capture_runtime rt ON rt.stream_id=ra.stream_id
		WHERE %s
		ORDER BY ra.server_id ASC, ra.execution_class ASC, ra.stream_id ASC
		LIMIT $%d OFFSET $%d
	`, strings.Join(where, " AND "), len(args)-1, len(args)), args...)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("query recording assignment audit: %v", err))
		return
	}
	defer rows.Close()

	items := make([]recordingAssignmentAuditItem, 0, limit)
	total := 0
	invalidTotal := 0
	for rows.Next() {
		var assignment recordingAssignmentRow
		var stream model.Stream
		var recordingState string
		var metaBytes []byte
		var cfgBytes []byte
		var lastFrameAt *time.Time
		if err := rows.Scan(
			&assignment.StreamID,
			&assignment.ServerID,
			&assignment.ExecutionClass,
			&assignment.Revision,
			&assignment.AssignedAt,
			&stream.Provider,
			&stream.ExternalID,
			&stream.Name,
			&stream.Slug,
			&stream.SourceURL,
			&stream.SourcePageURL,
			&stream.SourceFamily,
			&metaBytes,
			&recordingState,
			&stream.RecordingFailedReason,
			&stream.RecordingFailedAt,
			&stream.CaptureType,
			&stream.ExecutionClass,
			&cfgBytes,
			&stream.Tags,
			&stream.CreatedAt,
			&stream.UpdatedAt,
			&lastFrameAt,
		); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan recording assignment audit: %v", err))
			return
		}
		stream.ID = assignment.StreamID
		stream.RecordingState = model.RecordingState(recordingState)
		if err := decodeStreamPayload(&stream, metaBytes, cfgBytes); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("decode recording assignment audit stream: %v", err))
			return
		}
		total++
		requestedExecutionClass, allowedExecutionClasses, allowedErr := recordingAllowedExecutionClasses(stream)
		issues := buildRecordingAssignmentAuditIssues(stream, assignment, capacitySnapshot)
		if len(issues) > 0 {
			invalidTotal++
		}
		if allowedErr != nil && len(issues) == 0 {
			issues = append(issues, recordingAssignmentAuditIssue{Code: "stream_execution_unsupported", Detail: allowedErr.Error()})
			invalidTotal++
		}
		items = append(items, recordingAssignmentAuditItem{
			StreamID:                 assignment.StreamID,
			ServerID:                 assignment.ServerID,
			AssignmentExecutionClass: assignment.ExecutionClass,
			RequestedExecutionClass:  requestedExecutionClass,
			AllowedExecutionClasses:  allowedExecutionClasses,
			RecordingState:           string(stream.RecordingState),
			StreamCaptureType:        stream.CaptureType,
			StreamExecutionClass:     stream.ExecutionClass,
			Provider:                 stream.Provider,
			StreamSlug:               stream.Slug,
			StreamName:               stream.Name,
			AssignedAt:               assignment.AssignedAt,
			LastFrameAt:              lastFrameAt,
			Issues:                   issues,
		})
	}
	if rows.Err() != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("iterate recording assignment audit: %v", rows.Err()))
		return
	}

	util.WriteJSON(w, http.StatusOK, map[string]any{
		"items":         items,
		"limit":         limit,
		"offset":        offset,
		"total":         total,
		"invalid_total": invalidTotal,
	})
}

func (s *Server) handleRecordingServerHeartbeat(w http.ResponseWriter, r *http.Request) {
	var req recordingServerHeartbeatRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	serverID := strings.TrimSpace(req.ServerID)
	if serverID == "" {
		util.WriteError(w, http.StatusBadRequest, "server_id is required")
		return
	}
	if len(req.ExecutionClasses) == 0 {
		util.WriteError(w, http.StatusBadRequest, "execution_classes is required")
		return
	}
	leaseSec := req.LeaseSec
	if leaseSec <= 0 {
		leaseSec = 45
	}
	if leaseSec > 3600 {
		util.WriteError(w, http.StatusBadRequest, "lease_sec must be <= 3600")
		return
	}
	metadataBytes, err := json.Marshal(nonNilMap(req.MetadataJSON))
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, fmt.Sprintf("invalid metadata_json: %v", err))
		return
	}

	type normalizedExecutionClass struct {
		executionClass string
		maxActive      int
		draining       bool
	}
	seen := map[string]struct{}{}
	normalized := make([]normalizedExecutionClass, 0, len(req.ExecutionClasses))
	for _, item := range req.ExecutionClasses {
		executionClass, ok := capture.NormalizeExecutionClass(item.ExecutionClass)
		if !ok {
			util.WriteError(w, http.StatusBadRequest, fmt.Sprintf("invalid execution_class %q", item.ExecutionClass))
			return
		}
		if item.MaxActive < 0 {
			util.WriteError(w, http.StatusBadRequest, fmt.Sprintf("invalid max_active for execution_class %s", executionClass))
			return
		}
		key := executionClass
		if _, ok := seen[key]; ok {
			util.WriteError(w, http.StatusBadRequest, fmt.Sprintf("duplicate execution_class %s", key))
			return
		}
		seen[key] = struct{}{}
		normalized = append(normalized, normalizedExecutionClass{
			executionClass: key,
			maxActive:      item.MaxActive,
			draining:       item.Draining,
		})
	}
	if err := validateRecordingHeartbeatSharedCapacity(recordingHeartbeatSharedCapacities(req.ExecutionClasses)); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("begin server heartbeat tx: %v", err))
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()

	executionClasses := make([]string, 0, len(normalized))
	for _, item := range normalized {
		executionClasses = append(executionClasses, item.executionClass)
		if _, err := tx.Exec(r.Context(), `
			INSERT INTO server_execution_capacity (
				server_id, execution_class, max_active, draining, heartbeat_at, lease_expires_at, metadata_jsonb, updated_at
			)
			VALUES ($1, $2, $3, $4, now(), now() + make_interval(secs => $5), $6::jsonb, now())
			ON CONFLICT (server_id, execution_class)
			DO UPDATE SET
				max_active=EXCLUDED.max_active,
				draining=EXCLUDED.draining,
				heartbeat_at=EXCLUDED.heartbeat_at,
				lease_expires_at=EXCLUDED.lease_expires_at,
				metadata_jsonb=EXCLUDED.metadata_jsonb,
				updated_at=now()
		`, serverID, item.executionClass, item.maxActive, item.draining, leaseSec, string(metadataBytes)); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("upsert server execution class capacity: %v", err))
			return
		}
	}
	if _, err := tx.Exec(r.Context(), `
		DELETE FROM server_execution_capacity
		WHERE server_id=$1
		  AND NOT (execution_class = ANY($2::text[]))
	`, serverID, executionClasses); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("cleanup omitted server execution classes: %v", err))
		return
	}
	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("commit server heartbeat tx: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{
		"ok":                true,
		"server_id":         serverID,
		"execution_classes": executionClasses,
	})
}

func (s *Server) handleRecordingServerStopped(w http.ResponseWriter, r *http.Request) {
	var req recordingServerStoppedRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	serverID := strings.TrimSpace(req.ServerID)
	if serverID == "" {
		util.WriteError(w, http.StatusBadRequest, "server_id is required")
		return
	}
	if _, err := s.pool.Exec(r.Context(), `DELETE FROM server_execution_capacity WHERE server_id=$1`, serverID); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("delete server execution class capacity: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleDashboardRecordingServerCapacity(w http.ResponseWriter, r *http.Request) {
	includeInactive := false
	if v := parseBoolQueryPtr(r, "include_inactive"); v != nil {
		includeInactive = *v
	}
	snapshot, err := loadRecordingCapacitySnapshot(r.Context(), s.pool, includeInactive, "", false)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	items := make([]map[string]any, 0, len(snapshot.OrderedGroups))
	for _, group := range snapshot.OrderedGroups {
		items = append(items, map[string]any{
			"server_id":                   group.ServerID,
			"capacity_group":              group.CapacityGroup,
			"execution_class":             group.ExecutionClass,
			"execution_classes":           group.ExecutionClasses,
			"available_execution_classes": group.AvailableExecutionClasses,
			"execution_class_states":      group.ExecutionClassStates,
			"max_active":                  group.MaxActive,
			"assigned_count":              group.AssignedCount,
			"free_slots":                  group.FreeSlots,
			"draining":                    group.Draining,
			"heartbeat_at":                group.HeartbeatAt,
			"lease_expires_at":            group.LeaseExpiresAt,
			"active":                      group.Active,
			"metadata_json":               group.MetadataJSON,
			"invalid":                     group.Invalid,
			"invalid_reason":              group.InvalidReason,
		})
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{
		"items":            items,
		"include_inactive": includeInactive,
		"total":            len(items),
	})
}

func (s *Server) allocateYouTubeRelayRouteTx(
	ctx context.Context,
	tx pgx.Tx,
	stream model.Stream,
	sinkServerID string,
	assignmentRevision int64,
	actor string,
	reason string,
) (map[string]any, error) {
	rows, err := tx.Query(ctx, `
		SELECT
			src.server_id,
			src.shard_id,
			src.max_active,
			(
				SELECT COUNT(*)::bigint
				FROM youtube_relay_routes r
				WHERE r.source_server_id=src.server_id
				  AND r.status IN ('assigned', 'source_ready', 'running')
				  AND r.stream_id <> $1
			) AS active_count
		FROM youtube_relay_sources src
		WHERE src.lease_expires_at > now()
		  AND src.draining=false
		ORDER BY active_count ASC, src.server_id ASC
		FOR UPDATE OF src
	`, stream.ID)
	if err != nil {
		return nil, err
	}

	sourceServerID := ""
	sourceShardID := ""
	sourceMaxActive := 0
	sourceActiveCount := int64(0)
	for rows.Next() {
		var serverID string
		var shardID string
		var maxActive int
		var activeCount int64
		if err := rows.Scan(&serverID, &shardID, &maxActive, &activeCount); err != nil {
			return nil, err
		}
		if maxActive <= int(activeCount) {
			continue
		}
		sourceServerID = strings.TrimSpace(serverID)
		sourceShardID = strings.TrimSpace(shardID)
		sourceMaxActive = maxActive
		sourceActiveCount = activeCount
		break
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	if sourceServerID == "" {
		return nil, pgx.ErrNoRows
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO youtube_relay_routes (
			stream_id, source_server_id, sink_server_id, assignment_revision,
			status, relay_pull_url, error_text, started_at, stopped_at, metadata_jsonb, created_at, updated_at
		)
		VALUES ($1, $2, $3, $4, 'assigned', '', '', NULL, NULL, '{}'::jsonb, now(), now())
		ON CONFLICT (stream_id)
		DO UPDATE SET
			source_server_id=EXCLUDED.source_server_id,
			sink_server_id=EXCLUDED.sink_server_id,
			assignment_revision=EXCLUDED.assignment_revision,
			status='assigned',
			relay_pull_url='',
			error_text='',
			started_at=NULL,
			stopped_at=NULL,
			metadata_jsonb='{}'::jsonb,
			updated_at=now()
	`, stream.ID, sourceServerID, sinkServerID, assignmentRevision); err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO youtube_relay_events (
			stream_id, source_server_id, sink_server_id, status, actor, reason, error_text, metadata_jsonb
		)
		VALUES ($1, $2, $3, 'assigned', $4, $5, '', '{}'::jsonb)
	`, stream.ID, sourceServerID, sinkServerID, actor, strings.TrimSpace(reason)); err != nil {
		return nil, err
	}
	return map[string]any{
		"stream_id":             stream.ID,
		"source_server_id":      sourceServerID,
		"source_shard_id":       sourceShardID,
		"sink_server_id":        sinkServerID,
		"status":                youtubeRelayRouteStatusAssigned,
		"source_max_active":     sourceMaxActive,
		"source_assigned_count": sourceActiveCount + 1,
	}, nil
}

func (s *Server) clearYouTubeRelayRouteTx(
	ctx context.Context,
	tx pgx.Tx,
	streamID int64,
	actor string,
	reason string,
) error {
	var sourceServerID, sinkServerID string
	if err := tx.QueryRow(ctx, `
		SELECT source_server_id, sink_server_id
		FROM youtube_relay_routes
		WHERE stream_id=$1
	`, streamID).Scan(&sourceServerID, &sinkServerID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM youtube_relay_routes WHERE stream_id=$1`, streamID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO youtube_relay_events (
			stream_id, source_server_id, sink_server_id, status, actor, reason, error_text, metadata_jsonb
		)
		VALUES ($1, $2, $3, 'stopped', $4, $5, '', '{}'::jsonb)
	`, streamID, sourceServerID, sinkServerID, strings.TrimSpace(actor), strings.TrimSpace(reason)); err != nil {
		return err
	}
	return nil
}

func (s *Server) loadStreamForAssignmentTx(ctx context.Context, tx pgx.Tx, streamID int64) (model.Stream, error) {
	var stream model.Stream
	var recordingState string
	var metaBytes []byte
	var cfgBytes []byte
	var sourceURL string
	var sourceFamily string
	var captureFamily string
	var captureType string
	var executionClass string

	if err := tx.QueryRow(ctx, `
		SELECT
			id, provider, external_id, name, slug, source_url, source_page_url,
			source_family, capture_family, expected_fps, expected_image_interval_sec,
			lat, lon, location_text, location_country, location_country_code, location_region, location_city, location_locality, location_source, metadata_jsonb,
			recording_state, recording_failed_reason, recording_failed_at, capture_type, execution_class, execution_config_jsonb, tags,
			created_at, updated_at
		FROM streams
		WHERE id=$1
		FOR UPDATE
	`, streamID).Scan(
		&stream.ID, &stream.Provider, &stream.ExternalID, &stream.Name, &stream.Slug, &sourceURL, &stream.SourcePageURL,
		&sourceFamily, &captureFamily, &stream.ExpectedFPS, &stream.ExpectedImageInterval,
		&stream.Lat, &stream.Lon, &stream.LocationText, &stream.LocationCountry, &stream.LocationCountryCode, &stream.LocationRegion, &stream.LocationCity, &stream.LocationLocality, &stream.LocationSource, &metaBytes,
		&recordingState, &stream.RecordingFailedReason, &stream.RecordingFailedAt, &captureType, &executionClass, &cfgBytes, &stream.Tags,
		&stream.CreatedAt, &stream.UpdatedAt,
	); err != nil {
		return model.Stream{}, err
	}
	stream.SourceURL = sourceURL
	stream.SourceFamily = sourceFamily
	stream.CaptureFamily = captureFamily
	stream.CaptureType = captureType
	stream.ExecutionClass = executionClass
	stream.RecordingState = model.RecordingState(recordingState)
	if err := decodeStreamPayload(&stream, metaBytes, cfgBytes); err != nil {
		return model.Stream{}, err
	}
	return stream, nil
}
