package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/util"
)

func (s *Server) handleAlertDeliveryEventsList(w http.ResponseWriter, r *http.Request) {
	limit := parseIntQuery(r, "limit", 200, 1, 1000)
	offset := parseIntQuery(r, "offset", 0, 0, 1000000)
	status := strings.TrimSpace(strings.ToLower(r.URL.Query().Get("status")))
	streamID := parseInt64QueryPtr(r, "stream_id")
	incidentID := parseInt64QueryPtr(r, "incident_id")
	where := []string{"1=1"}
	args := make([]any, 0, 8)
	if status != "" {
		args = append(args, status)
		where = append(where, fmt.Sprintf("e.provider_status=$%d", len(args)))
	}
	if streamID != nil && *streamID > 0 {
		args = append(args, *streamID)
		where = append(where, fmt.Sprintf("e.stream_id=$%d", len(args)))
	}
	if incidentID != nil && *incidentID > 0 {
		args = append(args, *incidentID)
		where = append(where, fmt.Sprintf("e.incident_id=$%d", len(args)))
	}
	args = append(args, limit, offset)
	rows, err := s.pool.Query(r.Context(), fmt.Sprintf(`
		SELECT
			e.id,
			e.incident_id,
			e.stream_id,
			s.name,
			e.recipient,
			e.message_type,
			e.provider,
			e.provider_message_id,
			e.provider_status,
			e.subject,
			e.payload_jsonb,
			e.provider_payload_jsonb,
			e.error_text,
			e.sent_at,
			e.delivered_at,
			e.opened_at,
			e.bounced_at,
			e.created_at,
			e.updated_at
		FROM alert_delivery_events e
		JOIN streams s ON s.id=e.stream_id
		WHERE %s
		ORDER BY e.created_at DESC, e.id DESC
		LIMIT $%d OFFSET $%d
	`, strings.Join(where, " AND "), len(args)-1, len(args)), args...)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("query alert delivery events: %v", err))
		return
	}
	defer rows.Close()
	items := make([]map[string]any, 0, limit)
	for rows.Next() {
		var (
			id, streamID                             int64
			incidentID                               *int64
			streamName                               string
			recipient                                string
			messageType                              string
			provider                                 string
			messageID                                string
			providerStatus                           string
			subject                                  string
			payloadRaw                               []byte
			providerRaw                              []byte
			errorText                                string
			sentAt, deliveredAt, openedAt, bouncedAt *time.Time
			createdAt, updatedAt                     time.Time
		)
		if err := rows.Scan(
			&id, &incidentID, &streamID, &streamName, &recipient, &messageType, &provider,
			&messageID, &providerStatus, &subject, &payloadRaw, &providerRaw, &errorText,
			&sentAt, &deliveredAt, &openedAt, &bouncedAt, &createdAt, &updatedAt,
		); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan alert delivery event: %v", err))
			return
		}
		payload := map[string]any{}
		_ = json.Unmarshal(payloadRaw, &payload)
		providerPayload := map[string]any{}
		_ = json.Unmarshal(providerRaw, &providerPayload)
		items = append(items, map[string]any{
			"id":                  id,
			"incident_id":         incidentID,
			"stream_id":           streamID,
			"stream_name":         streamName,
			"recipient":           recipient,
			"message_type":        messageType,
			"provider":            provider,
			"provider_message_id": messageID,
			"provider_status":     providerStatus,
			"subject":             subject,
			"payload_json":        payload,
			"provider_payload":    providerPayload,
			"error_text":          errorText,
			"sent_at":             sentAt,
			"delivered_at":        deliveredAt,
			"opened_at":           openedAt,
			"bounced_at":          bouncedAt,
			"created_at":          createdAt.UTC(),
			"updated_at":          updatedAt.UTC(),
		})
	}
	if err := rows.Err(); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("iterate alert delivery events: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{
		"items":  items,
		"limit":  limit,
		"offset": offset,
		"total":  len(items),
	})
}

func (s *Server) handleResendWebhook(w http.ResponseWriter, r *http.Request) {
	var payload map[string]any
	if err := util.DecodeJSON(r, &payload); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	eventType := strings.TrimSpace(fmt.Sprint(payload["type"]))
	providerEventID := strings.TrimSpace(r.Header.Get("svix-id"))
	if providerEventID == "" {
		providerEventID = strings.TrimSpace(fmt.Sprint(payload["id"]))
	}
	if providerEventID == "" {
		util.WriteError(w, http.StatusBadRequest, "missing provider event id")
		return
	}
	data, _ := payload["data"].(map[string]any)
	providerMessageID := strings.TrimSpace(fmt.Sprint(data["email_id"]))
	if providerMessageID == "" {
		providerMessageID = strings.TrimSpace(fmt.Sprint(data["id"]))
	}
	raw, err := json.Marshal(nonNilMap(payload))
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, fmt.Sprintf("invalid webhook payload: %v", err))
		return
	}
	status, deliveredAt, openedAt, bouncedAt := resendWebhookStatusAndTimes(eventType, data)
	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("begin resend webhook tx: %v", err))
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()
	if _, err := tx.Exec(r.Context(), `
		INSERT INTO email_webhook_events (provider, provider_event_id, event_type, provider_message_id, payload_jsonb, processed_at, created_at)
		VALUES ('resend', $1, $2, $3, $4::jsonb, now(), now())
		ON CONFLICT (provider, provider_event_id) DO NOTHING
	`, providerEventID, eventType, providerMessageID, string(raw)); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("insert resend webhook event: %v", err))
		return
	}
	if providerMessageID != "" && status != "" {
		if _, err := tx.Exec(r.Context(), `
			UPDATE alert_delivery_events
			SET
				provider_status=$2,
				provider_payload_jsonb=$3::jsonb,
				delivered_at=COALESCE($4, delivered_at),
				opened_at=COALESCE($5, opened_at),
				bounced_at=COALESCE($6, bounced_at),
				updated_at=now()
			WHERE provider='resend'
			  AND provider_message_id=$1
		`, providerMessageID, status, string(raw), deliveredAt, openedAt, bouncedAt); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("update alert delivery from webhook: %v", err))
			return
		}
	}
	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("commit resend webhook tx: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{
		"ok":                  true,
		"provider":            "resend",
		"provider_event_id":   providerEventID,
		"provider_message_id": providerMessageID,
		"event_type":          eventType,
		"provider_status":     status,
	})
}

func resendWebhookStatusAndTimes(eventType string, data map[string]any) (string, *time.Time, *time.Time, *time.Time) {
	eventType = strings.TrimSpace(strings.ToLower(eventType))
	parsedAt := func(keys ...string) *time.Time {
		for _, key := range keys {
			raw := strings.TrimSpace(fmt.Sprint(data[key]))
			if raw == "" || raw == "<nil>" {
				continue
			}
			if ts, err := time.Parse(time.RFC3339Nano, raw); err == nil {
				u := ts.UTC()
				return &u
			}
		}
		return nil
	}
	switch eventType {
	case "email.sent", "sent":
		return "accepted", nil, nil, nil
	case "email.delivered", "delivered":
		return "delivered", parsedAt("delivered_at", "created_at"), nil, nil
	case "email.opened", "opened":
		return "opened", nil, parsedAt("opened_at", "created_at"), nil
	case "email.bounced", "bounced":
		return "bounced", nil, nil, parsedAt("bounced_at", "created_at")
	case "email.complained", "complained":
		return "complained", nil, nil, nil
	default:
		return "", nil, nil, nil
	}
}
