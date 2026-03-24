package api

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/util"
)

const accountClipBatchLimit = 120

type dataAccessSpecEndpoint struct {
	Key         string            `json:"key"`
	Method      string            `json:"method"`
	Path        string            `json:"path"`
	Auth        string            `json:"auth"`
	Description string            `json:"description"`
	Query       map[string]string `json:"query,omitempty"`
	Limit       int               `json:"limit,omitempty"`
}

type accountClipDownloadPrepareRequest struct {
	StreamID   int64   `json:"stream_id"`
	SegmentIDs []int64 `json:"segment_ids"`
}

type accountClipDownloadItem struct {
	ID           int64     `json:"id"`
	StreamID     int64     `json:"stream_id"`
	SegmentStart time.Time `json:"segment_start_at"`
	SegmentEnd   time.Time `json:"segment_end_at"`
	DownloadURL  string    `json:"download_url"`
	Filename     string    `json:"filename"`
}

func (s *Server) handleDataAccessSpec(w http.ResponseWriter, r *http.Request) {
	util.WriteJSON(w, http.StatusOK, map[string]any{
		"auth_model": map[string]any{
			"public_reads":    "Stream metadata and public previews are available without auth.",
			"account_session": "A signed-in browser session is used for recording changes and account clip browsing.",
			"api_key":         "API keys act as account credentials for CLI/script access to authenticated data APIs.",
		},
		"batch_limits": map[string]any{
			"clip_download_prepare_max_segments": accountClipBatchLimit,
		},
		"endpoints": []dataAccessSpecEndpoint{
			{
				Key:         "stream_search",
				Method:      http.MethodGet,
				Path:        "/api/v1/dashboard/streams",
				Auth:        "public",
				Description: "Search the public stream catalog.",
				Query: map[string]string{
					"q":                  "free-text stream search",
					"recording_state":    "use on for recorded streams only",
					"limit":              "page size",
					"offset":             "page offset",
					"include_image_urls": "0 to skip preview URLs",
				},
				Limit: 2000,
			},
			{
				Key:         "stream_detail",
				Method:      http.MethodGet,
				Path:        "/api/v1/dashboard/streams/{id}",
				Auth:        "public",
				Description: "Load public stream detail, including preview-oriented metadata.",
			},
			{
				Key:         "account_clip_list",
				Method:      http.MethodGet,
				Path:        "/api/v1/account/streams/{id}/clips",
				Auth:        "account",
				Description: "Browse recorded clips for one stream with session or API key auth.",
				Query: map[string]string{
					"limit":  "page size",
					"offset": "page offset",
				},
				Limit: 200,
			},
			{
				Key:         "account_clip_download_prepare",
				Method:      http.MethodPost,
				Path:        "/api/v1/account/clips/download-prepare",
				Auth:        "account",
				Description: "Prepare up to 120 authenticated clip downloads for a selected stream.",
				Limit:       accountClipBatchLimit,
			},
			{
				Key:         "recording_assign",
				Method:      http.MethodPost,
				Path:        "/api/v1/recording/streams/{id}/assign",
				Auth:        "session",
				Description: "Turn recording on for a stream from a signed-in browser session.",
			},
			{
				Key:         "recording_unassign",
				Method:      http.MethodPost,
				Path:        "/api/v1/recording/streams/{id}/unassign",
				Auth:        "session",
				Description: "Turn recording off for a stream from a signed-in browser session.",
			},
		},
	})
}

func (s *Server) handleAccountStreamClipsList(w http.ResponseWriter, r *http.Request) {
	streamID, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	if _, err := s.getStreamByID(r.Context(), streamID); err != nil {
		util.WriteError(w, http.StatusNotFound, err.Error())
		return
	}
	limit := parseIntQuery(r, "limit", 60, 1, 200)
	offset := parseIntQuery(r, "offset", 0, 0, 1_000_000)
	items, err := s.queryCaptureSegments(r.Context(), captureSegmentQueryOptions{
		StreamID:                    streamID,
		Limit:                       limit,
		Offset:                      offset,
		IncludeDownloadURL:          true,
		IncludeThumbnailDownloadURL: true,
	})
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{
		"stream_id": streamID,
		"limit":     limit,
		"offset":    offset,
		"items":     items,
	})
}

func (s *Server) handleAccountClipDownloadPrepare(w http.ResponseWriter, r *http.Request) {
	var req accountClipDownloadPrepareRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.StreamID <= 0 {
		util.WriteError(w, http.StatusBadRequest, "stream_id is required")
		return
	}
	segmentIDs := uniquePositiveInt64s(req.SegmentIDs)
	if len(segmentIDs) == 0 {
		util.WriteError(w, http.StatusBadRequest, "segment_ids is required")
		return
	}
	if len(segmentIDs) > accountClipBatchLimit {
		util.WriteError(w, http.StatusBadRequest, fmt.Sprintf("segment_ids limit exceeded; max=%d", accountClipBatchLimit))
		return
	}
	stream, err := s.getStreamByID(r.Context(), req.StreamID)
	if err != nil {
		util.WriteError(w, http.StatusNotFound, err.Error())
		return
	}
	items, err := s.queryCaptureSegments(r.Context(), captureSegmentQueryOptions{
		StreamID:                    req.StreamID,
		SegmentIDs:                  segmentIDs,
		Limit:                       len(segmentIDs),
		Offset:                      0,
		IncludeDownloadURL:          true,
		IncludeThumbnailDownloadURL: false,
	})
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	byID := make(map[int64]captureSegmentListItem, len(items))
	for _, item := range items {
		byID[item.ID] = item
	}
	out := make([]accountClipDownloadItem, 0, len(segmentIDs))
	for _, id := range segmentIDs {
		item, ok := byID[id]
		if !ok {
			util.WriteError(w, http.StatusBadRequest, fmt.Sprintf("segment %d not found for stream %d", id, req.StreamID))
			return
		}
		if strings.TrimSpace(item.DownloadURL) == "" {
			util.WriteError(w, http.StatusBadRequest, fmt.Sprintf("segment %d is not downloadable", id))
			return
		}
		out = append(out, accountClipDownloadItem{
			ID:           item.ID,
			StreamID:     item.StreamID,
			SegmentStart: item.SegmentStartAt,
			SegmentEnd:   item.SegmentEndAt,
			DownloadURL:  item.DownloadURL,
			Filename:     buildAccountClipFilename(stream.Slug, item),
		})
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{
		"stream_id":       req.StreamID,
		"requested_count": len(segmentIDs),
		"items":           out,
		"max_segments":    accountClipBatchLimit,
	})
}

func buildAccountClipFilename(streamSlug string, item captureSegmentListItem) string {
	slug := strings.TrimSpace(streamSlug)
	if slug == "" {
		slug = fmt.Sprintf("stream-%d", item.StreamID)
	}
	ext := fileExtensionFromMIME(derefString(item.MIMEType))
	if ext == "" {
		ext = ".mp4"
	}
	return fmt.Sprintf(
		"%s-%s%s",
		slug,
		item.SegmentStartAt.UTC().Format("20060102T150405Z"),
		ext,
	)
}

func uniquePositiveInt64s(in []int64) []int64 {
	out := make([]int64, 0, len(in))
	seen := make(map[int64]struct{}, len(in))
	for _, v := range in {
		if v <= 0 {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out
}
