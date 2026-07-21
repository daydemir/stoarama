package api

import (
	"fmt"
	"net/http"
	"time"

	"github.com/daydemir/stoarama/backend/internal/dropletpool"
	"github.com/daydemir/stoarama/backend/internal/util"
)

// handleAdminRecorderPool returns the recorder droplet pool state for operator
// observability (SRE-1): the live droplets, the current demand forecast, and the
// scale cooldown ledger. It is read-only and never provisions anything. The
// forecast lookahead defaults to 30 minutes.
func (s *Server) handleAdminRecorderPool(w http.ResponseWriter, r *http.Request) {
	now := time.Now().UTC()
	lookahead := 30 * time.Minute

	store := dropletpool.NewStore(s.pool)
	forecast, err := dropletpool.ForecastDemand(r.Context(), s.pool, s.billing != nil, now, lookahead)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("forecast demand: %v", err))
		return
	}
	ps, err := store.LoadPoolState(r.Context())
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load pool state: %v", err))
		return
	}

	rows, err := s.pool.Query(r.Context(), `
		SELECT id, name, do_droplet_id, region, size, capacity, state, ip_address,
		       last_seen_at, first_seen_at, activated_at, idle_since, drain_started_at,
		       provision_error, created_at
		FROM recorder_droplets
		WHERE state IN ('provisioning','active','draining','destroying')
		ORDER BY created_at ASC, id ASC
	`)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("list recorder droplets: %v", err))
		return
	}
	defer rows.Close()
	droplets := make([]map[string]any, 0, 8)
	for rows.Next() {
		var (
			id             int64
			name           string
			doDropletID    *int64
			region         string
			size           string
			capacity       int
			state          string
			ip             string
			lastSeenAt     *time.Time
			firstSeenAt    *time.Time
			activatedAt    *time.Time
			idleSince      *time.Time
			drainStartedAt *time.Time
			provisionError string
			createdAt      time.Time
		)
		if err := rows.Scan(&id, &name, &doDropletID, &region, &size, &capacity, &state, &ip,
			&lastSeenAt, &firstSeenAt, &activatedAt, &idleSince, &drainStartedAt, &provisionError, &createdAt); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan recorder droplet: %v", err))
			return
		}
		droplets = append(droplets, map[string]any{
			"id":               id,
			"name":             name,
			"do_droplet_id":    doDropletID,
			"region":           region,
			"size":             size,
			"capacity":         capacity,
			"state":            state,
			"ip_address":       ip,
			"last_seen_at":     lastSeenAt,
			"first_seen_at":    firstSeenAt,
			"activated_at":     activatedAt,
			"idle_since":       idleSince,
			"drain_started_at": drainStartedAt,
			"provision_error":  provisionError,
			"created_at":       createdAt.UTC(),
		})
	}
	if err := rows.Err(); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("iterate recorder droplets: %v", err))
		return
	}

	util.WriteJSON(w, http.StatusOK, map[string]any{
		"droplets": droplets,
		"forecast": map[string]any{
			"peak_concurrent": forecast.PeakConcurrent,
			"next_fire_at":    nullableTime(forecast.NextFireAt),
			"lookahead_sec":   int(lookahead / time.Second),
		},
		"cooldown": map[string]any{
			"last_scale_up_at":   ps.LastScaleUpAt,
			"last_scale_down_at": ps.LastScaleDownAt,
		},
	})
}

// nullableTime returns nil for a zero time so JSON emits null instead of the zero
// instant.
func nullableTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t.UTC()
}
