package api

import (
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/nasdelivery"
	"github.com/daydemir/stoarama/backend/internal/util"
	"github.com/jackc/pgx/v5"
)

// Collated-only NAS delivery is an operator setting: a held clip is exempt from
// managed-storage metering, so accounts can never set it themselves.

const maxNASDeliveryModeRecordings = 200

type nasDeliveryModeRequest struct {
	AccountID    int64   `json:"account_id"`
	RecordingIDs []int64 `json:"recording_ids"`
	Mode         string  `json:"mode"`
	Reason       string  `json:"reason"`
	DryRun       bool    `json:"dry_run"`
}

type nasDeliveryModeItem struct {
	RecordingID      int64      `json:"recording_id"`
	Name             string     `json:"name"`
	Status           string     `json:"status"`
	PreviousMode     string     `json:"previous_mode,omitempty"`
	Mode             string     `json:"mode"`
	ModeUpdatedAt    *time.Time `json:"mode_updated_at"`
	Reason           string     `json:"reason"`
	HeldClips        int64      `json:"held_clips"`
	HeldBytes        int64      `json:"held_bytes"`
	OldestHeldClipAt *time.Time `json:"oldest_held_clip_at"`
	NewestHeldClipAt *time.Time `json:"newest_held_clip_at"`
}

func validateNASDeliveryModeRequest(req *nasDeliveryModeRequest) error {
	req.Mode = strings.TrimSpace(req.Mode)
	req.Reason = strings.TrimSpace(req.Reason)
	if req.AccountID <= 0 {
		return errors.New("account_id is required")
	}
	if !nasdelivery.ValidMode(req.Mode) {
		return fmt.Errorf("mode must be %q or %q", nasdelivery.ModeRaw, nasdelivery.ModeCollatedOnly)
	}
	if req.Reason == "" || len(req.Reason) > 512 {
		return errors.New("reason is required (at most 512 bytes)")
	}
	if len(req.RecordingIDs) == 0 || len(req.RecordingIDs) > maxNASDeliveryModeRecordings {
		return fmt.Errorf("recording_ids must contain 1 to %d ids", maxNASDeliveryModeRecordings)
	}
	seen := make(map[int64]struct{}, len(req.RecordingIDs))
	for _, id := range req.RecordingIDs {
		if id <= 0 {
			return errors.New("recording_ids must be positive")
		}
		if _, ok := seen[id]; ok {
			return fmt.Errorf("duplicate recording id %d", id)
		}
		seen[id] = struct{}{}
	}
	sort.Slice(req.RecordingIDs, func(i, j int) bool { return req.RecordingIDs[i] < req.RecordingIDs[j] })
	return nil
}

// handleAdminRecordingNASDeliveryMode switches nas_pull recordings between raw
// and collated_only NAS delivery. The mode is frozen onto each clip at insert, so
// the switch only affects clips recorded afterwards: clips already offered to the
// NAS keep flowing raw, and held clips stay held if the recording switches back.
func (s *Server) handleAdminRecordingNASDeliveryMode(w http.ResponseWriter, r *http.Request) {
	var req nasDeliveryModeRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := validateNASDeliveryModeRequest(&req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "begin NAS delivery mode change")
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()
	rows, err := tx.Query(r.Context(), `
		SELECT id,account_id,delivery,storage_retention_tier,nas_delivery_mode
		FROM recordings WHERE id=ANY($1) ORDER BY id FOR UPDATE`, req.RecordingIDs)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "lock recordings")
		return
	}
	previous := make(map[int64]string, len(req.RecordingIDs))
	var problems []string
	for rows.Next() {
		var id, accountID int64
		var delivery, tier, mode string
		if err := rows.Scan(&id, &accountID, &delivery, &tier, &mode); err != nil {
			rows.Close()
			util.WriteError(w, http.StatusInternalServerError, "read recordings")
			return
		}
		previous[id] = mode
		switch {
		case accountID != req.AccountID:
			problems = append(problems, fmt.Sprintf("recording %d belongs to another account", id))
		case delivery != string(deliveryNASPull):
			problems = append(problems, fmt.Sprintf("recording %d is not nas_pull", id))
		case req.Mode == nasdelivery.ModeCollatedOnly && tier == "yearly_prepaid":
			problems = append(problems, fmt.Sprintf("recording %d is yearly_prepaid", id))
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "read recordings")
		return
	}
	for _, id := range req.RecordingIDs {
		if _, ok := previous[id]; !ok {
			problems = append(problems, fmt.Sprintf("recording %d not found", id))
		}
	}
	if len(problems) > 0 {
		sort.Strings(problems)
		util.WriteError(w, http.StatusConflict, strings.Join(problems, "; "))
		return
	}
	if !req.DryRun {
		if _, err := tx.Exec(r.Context(), `
			UPDATE recordings SET nas_delivery_mode=$2,nas_delivery_mode_reason=$3,
			  nas_delivery_mode_updated_at=now(),updated_at=now()
			WHERE id=ANY($1) AND nas_delivery_mode IS DISTINCT FROM $2`, req.RecordingIDs, req.Mode, req.Reason); err != nil {
			util.WriteError(w, http.StatusInternalServerError, "update NAS delivery mode")
			return
		}
	}
	items, err := loadNASDeliveryModeItems(r, tx, req.RecordingIDs)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if req.DryRun {
		for i := range items {
			items[i].Mode = req.Mode
		}
	}
	for i := range items {
		items[i].PreviousMode = previous[items[i].RecordingID]
	}
	if err := tx.Commit(r.Context()); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "commit NAS delivery mode change")
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"dry_run": req.DryRun, "mode": req.Mode, "items": items})
}

// handleAdminRecordingNASDeliveryStatus reports each nas_pull recording of one
// account that is collated_only or still holds clips, with its held backlog.
func (s *Server) handleAdminRecordingNASDeliveryStatus(w http.ResponseWriter, r *http.Request) {
	accountID, err := strconv.ParseInt(strings.TrimSpace(r.URL.Query().Get("account_id")), 10, 64)
	if err != nil || accountID <= 0 {
		util.WriteError(w, http.StatusBadRequest, "account_id is required")
		return
	}
	tx, err := s.pool.BeginTx(r.Context(), pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "begin NAS delivery status")
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()
	var ids []int64
	rows, err := tx.Query(r.Context(), `
		SELECT r.id FROM recordings r
		WHERE r.account_id=$1 AND r.delivery='nas_pull'
		  AND (r.nas_delivery_mode<>'raw' OR r.nas_delivery_mode_updated_at IS NOT NULL)
		ORDER BY r.id`, accountID)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, "list NAS delivery recordings")
		return
	}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			util.WriteError(w, http.StatusInternalServerError, "read NAS delivery recordings")
			return
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		util.WriteError(w, http.StatusInternalServerError, "read NAS delivery recordings")
		return
	}
	items, err := loadNASDeliveryModeItems(r, tx, ids)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	var heldClips, heldBytes int64
	for _, item := range items {
		heldClips += item.HeldClips
		heldBytes += item.HeldBytes
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{
		"account_id": accountID, "items": items, "held_clips": heldClips, "held_bytes": heldBytes,
	})
}

func loadNASDeliveryModeItems(r *http.Request, tx pgx.Tx, ids []int64) ([]nasDeliveryModeItem, error) {
	items := make([]nasDeliveryModeItem, 0, len(ids))
	if len(ids) == 0 {
		return items, nil
	}
	rows, err := tx.Query(r.Context(), `
		SELECT r.id,r.name,r.status,r.nas_delivery_mode,r.nas_delivery_mode_updated_at,r.nas_delivery_mode_reason,
		       COALESCE(h.clips,0),COALESCE(h.bytes,0),h.oldest_at,h.newest_at
		FROM recordings r
		LEFT JOIN LATERAL (
		  SELECT count(*) AS clips,SUM(c.size_bytes) AS bytes,MIN(c.clip_start_at) AS oldest_at,MAX(c.clip_start_at) AS newest_at
		  FROM recording_clips c
		  JOIN clip_storage_billing_contracts bc ON bc.clip_id=c.id AND bc.mode='`+nasdelivery.HoldContractMode+`'
		  WHERE c.recording_id=r.id AND c.purged_at IS NULL AND c.released_at IS NULL
		) h ON true
		WHERE r.id=ANY($1) ORDER BY r.id`, ids)
	if err != nil {
		return nil, errors.New("load NAS delivery modes")
	}
	defer rows.Close()
	for rows.Next() {
		var item nasDeliveryModeItem
		if err := rows.Scan(&item.RecordingID, &item.Name, &item.Status, &item.Mode, &item.ModeUpdatedAt, &item.Reason,
			&item.HeldClips, &item.HeldBytes, &item.OldestHeldClipAt, &item.NewestHeldClipAt); err != nil {
			return nil, errors.New("read NAS delivery modes")
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, errors.New("read NAS delivery modes")
	}
	return items, nil
}
