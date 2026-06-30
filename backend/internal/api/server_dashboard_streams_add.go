package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"net/url"
	"strings"

	"github.com/daydemir/stoarama/backend/internal/capture"
	"github.com/daydemir/stoarama/backend/internal/netguard"
	"github.com/daydemir/stoarama/backend/internal/util"
)

// dashboardStreamAddRequest is the add-stream form payload submitted by a
// signed-in account. city and country are USER-ENTERED free text and are stored
// verbatim; the server never auto-derives location from the URL or geo-IP
// because the catalog's auto-derived location metadata is unreliable.
type dashboardStreamAddRequest struct {
	Name     string   `json:"name"`
	URL      string   `json:"url"`
	Provider string   `json:"provider"`
	City     string   `json:"city"`
	Country  string   `json:"country"`
	Lat      *float64 `json:"lat"`
	Lon      *float64 `json:"lon"`
	Tags     []string `json:"tags"`
}

func validatePublicHLSStreamURL(raw string) error {
	u, err := url.ParseRequestURI(strings.TrimSpace(raw))
	if err != nil || u == nil || !u.IsAbs() || !strings.EqualFold(u.Scheme, "https") || strings.TrimSpace(u.Host) == "" {
		return errors.New("url must be an https HLS (.m3u8) URL")
	}
	if !strings.HasSuffix(strings.ToLower(u.Path), ".m3u8") {
		return errors.New("url must be an https HLS (.m3u8) URL")
	}
	return nil
}

// handleDashboardStreamAdd lets a signed-in account add a public stream straight
// into the shared catalog (no moderation queue). The submitted URL is untrusted,
// so it MUST pass the SSRF guard and a reachability probe before any insert. The
// row is wired identically to an admin import (same capture profile + recordable
// state) and flagged user_created with the caller as created_by_account_id.
func (s *Server) handleDashboardStreamAdd(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok || principal.AccountID <= 0 {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}

	var req dashboardStreamAddRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	name := strings.TrimSpace(req.Name)
	streamURL := strings.TrimSpace(req.URL)
	city := strings.TrimSpace(req.City)
	country := strings.TrimSpace(req.Country)
	if name == "" {
		util.WriteError(w, http.StatusBadRequest, "name is required")
		return
	}
	if streamURL == "" {
		util.WriteError(w, http.StatusBadRequest, "url is required")
		return
	}
	if city == "" {
		util.WriteError(w, http.StatusBadRequest, "city is required")
		return
	}
	if country == "" {
		util.WriteError(w, http.StatusBadRequest, "country is required")
		return
	}
	if err := validatePublicHLSStreamURL(streamURL); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	// Provider namespace defaults to "custom"; the caller may pin one explicitly.
	provider := strings.TrimSpace(req.Provider)
	if provider == "" {
		provider = "custom"
	}

	// Stable, deduplicating external_id: the same URL always collides on the
	// streams UNIQUE(provider, external_id) constraint.
	sum := sha256.Sum256([]byte(streamURL))
	externalID := provider + ":" + hex.EncodeToString(sum[:])

	// SSRF guard FIRST on the untrusted URL. Rejects non-http(s), embedded
	// credentials, and any host resolving to a private/internal/metadata address.
	if _, err := netguard.ValidatePublicURL(streamURL); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	// Reachability probe AFTER the SSRF guard passes. Do NOT insert an unplayable
	// stream. The error is already sanitized ("stream not reachable").
	probeCtx, cancel := context.WithTimeout(r.Context(), recordingProbeTimeout)
	defer cancel()
	if err := capture.ProbeReachable(probeCtx, streamURL, ""); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	locationText := city + ", " + country
	createdBy := principal.AccountID
	importReq := serviceStreamImportRequest{
		Provider:        provider,
		ExternalID:      externalID,
		Name:            name,
		SourceURL:       streamURL,
		Tags:            req.Tags,
		Lat:             req.Lat,
		Lon:             req.Lon,
		LocationText:    locationText,
		LocationCity:    city,
		LocationCountry: country,
		LocationSource:  "user",
	}

	stream, created, err := s.upsertImportedStream(r, importReq, true, &createdBy)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	if !created {
		util.WriteError(w, http.StatusConflict, "This stream is already in the catalog.")
		return
	}

	util.WriteJSON(w, http.StatusOK, map[string]any{
		"ok":      true,
		"created": true,
		"stream":  stream,
	})
}
