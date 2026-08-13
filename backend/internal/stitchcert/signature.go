package stitchcert

import (
	"fmt"
	"sort"
)

// StableNativeSignatureV1 retains decoder/layout compatibility fields and
// deliberately excludes stream index plus clip-local PTS, start, duration and
// counters. Those values cannot define a native media boundary.
func StableNativeSignatureV1(format string, streams []map[string]any) (map[string]any, error) {
	if format == "" || len(streams) < 1 || len(streams) > 2 {
		return nil, fmt.Errorf("unsupported native stream structure")
	}
	common := []string{"codec_type", "codec_name", "codec_tag_string", "profile", "level", "time_base", "extradata_hash"}
	video := []string{"pix_fmt", "width", "height", "coded_width", "coded_height", "field_order", "sample_aspect_ratio", "display_aspect_ratio", "avg_frame_rate", "r_frame_rate", "color_range", "color_space", "color_transfer", "color_primaries", "chroma_location"}
	audio := []string{"sample_fmt", "sample_rate", "channels", "channel_layout", "bits_per_sample"}
	out := make([]map[string]any, 0, len(streams))
	videoCount, audioCount := 0, 0
	for _, stream := range streams {
		kind, _ := stream["codec_type"].(string)
		if kind != "video" && kind != "audio" {
			return nil, fmt.Errorf("unsupported stream kind")
		}
		if kind == "video" {
			videoCount++
		} else {
			audioCount++
		}
		keys := append([]string{}, common...)
		if kind == "video" {
			keys = append(keys, video...)
		} else {
			keys = append(keys, audio...)
		}
		canonical := map[string]any{}
		for _, key := range keys {
			canonical[key] = stream[key]
		}
		if canonical["codec_name"] == nil || canonical["extradata_hash"] == nil {
			return nil, fmt.Errorf("incomplete native signature")
		}
		out = append(out, canonical)
	}
	if videoCount != 1 || audioCount > 1 {
		return nil, fmt.Errorf("unsupported native stream structure")
	}
	sort.Slice(out, func(i, j int) bool { return out[i]["codec_type"].(string) < out[j]["codec_type"].(string) })
	return map[string]any{"schema_version": 1, "format_name": format, "streams": out}, nil
}
