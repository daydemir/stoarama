package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/daydemir/stoarama/backend/internal/capture"
	"github.com/daydemir/stoarama/backend/internal/model"
)

type recordingAssignmentAuditIssue struct {
	Code   string `json:"code"`
	Detail string `json:"detail,omitempty"`
}

type serverExecutionCapacityHeartbeatItem struct {
	ExecutionClass string `json:"execution_class"`
	MaxActive      int    `json:"max_active"`
	Draining       bool   `json:"draining"`
}

const recordingCapacityGroupCaptureShared = "capture_shared"

func recordingCapacityGroup(executionClass string) string {
	switch normalized, ok := capture.NormalizeExecutionClass(executionClass); {
	case !ok:
		return strings.TrimSpace(executionClass)
	case normalized == capture.ExecutionClassVideoLive:
		return recordingCapacityGroupCaptureShared
	case normalized == capture.ExecutionClassYouTubeDirect:
		return capture.ExecutionClassYouTubeDirect
	default:
		return normalized
	}
}

func recordingCapacityGroupModes(executionClass string) []string {
	switch recordingCapacityGroup(executionClass) {
	case recordingCapacityGroupCaptureShared:
		return []string{capture.ExecutionClassVideoLive}
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
		return capture.ExecutionClassYouTubeDirect, nil
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

func recordingAllowedExecutionClassesWithPreference(stream model.Stream, preferredExecutionClass string) (string, []string, error) {
	preferred := strings.TrimSpace(preferredExecutionClass)
	if preferred == "" {
		return recordingAllowedExecutionClasses(stream)
	}
	preferred, ok := capture.NormalizeExecutionClass(preferred)
	if !ok {
		return "", nil, fmt.Errorf("invalid preferred execution_class")
	}
	captureType, _ := capture.NormalizeCaptureType(stream.CaptureType)
	if captureType == capture.CaptureTypeYouTubeWatch {
		switch preferred {
		case capture.ExecutionClassYouTubeDirect:
			return preferred, []string{preferred}, nil
		case capture.ExecutionClassYouTubeRelay:
			return "", nil, fmt.Errorf("preferred execution_class youtube_relay is disabled for new youtube assignments")
		default:
			return "", nil, fmt.Errorf("preferred execution_class is not compatible with youtube_watch")
		}
	}
	requestedExecutionClass, candidates, err := recordingAllowedExecutionClasses(stream)
	if err != nil {
		return "", nil, err
	}
	if preferred != requestedExecutionClass {
		return "", nil, fmt.Errorf("preferred execution_class is not compatible with stream")
	}
	return requestedExecutionClass, candidates, nil
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

func (s *Server) assignRecordingStreamTx(ctx context.Context, tx pgx.Tx, stream model.Stream, serverID string, preferredExecutionClass string, actor string, reason string) (map[string]any, int, error) {
	if stream.RecordingState != model.RecordingStateOn {
		return map[string]any{
			"error":           "stream recording_state must be on before assignment",
			"error_code":      "recording_state_off",
			"recording_state": stream.RecordingState,
			"stream_id":       stream.ID,
		}, http.StatusConflict, nil
	}
	requestedExecutionClass, executionClassCandidates, err := recordingAllowedExecutionClassesWithPreference(stream, preferredExecutionClass)
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

func (s *Server) setStreamRecordingStateTx(ctx context.Context, tx pgx.Tx, streamID int64, state model.RecordingState, preferredExecutionClass string, actor string, reason string) (map[string]any, int, error) {
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
			result, status, err := s.assignRecordingStreamTx(ctx, tx, stream, "", preferredExecutionClass, actor, reason)
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
			lat, lon, location_text, location_country, location_country_code, location_region, location_city, local_timezone, location_locality, location_source, metadata_jsonb,
			recording_state, recording_failed_reason, recording_failed_at, capture_type, execution_class, execution_config_jsonb, tags,
			created_at, updated_at
		FROM streams
		WHERE id=$1
		FOR UPDATE
	`, streamID).Scan(
		&stream.ID, &stream.Provider, &stream.ExternalID, &stream.Name, &stream.Slug, &sourceURL, &stream.SourcePageURL,
		&sourceFamily, &captureFamily, &stream.ExpectedFPS, &stream.ExpectedImageInterval,
		&stream.Lat, &stream.Lon, &stream.LocationText, &stream.LocationCountry, &stream.LocationCountryCode, &stream.LocationRegion, &stream.LocationCity, &stream.LocalTimezone, &stream.LocationLocality, &stream.LocationSource, &metaBytes,
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
