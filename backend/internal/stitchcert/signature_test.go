package stitchcert

import "testing"

func TestStableNativeSignatureIgnoresClipLocalTimingAndIndex(t *testing.T) {
	a := map[string]any{"index": 0, "codec_type": "video", "codec_name": "h264", "profile": "High", "level": 40, "time_base": "1/90000", "extradata_hash": "SHA256:x", "pix_fmt": "yuv420p", "width": 1920, "height": 1080, "start_pts": 1, "duration_ts": 10, "start_time": "1.0", "duration": "2.0"}
	b := map[string]any{}
	for k, v := range a {
		b[k] = v
	}
	b["index"], b["start_pts"], b["duration_ts"], b["start_time"], b["duration"] = 9, 999, 888, "99", "88"
	sa, err := StableNativeSignatureV1("mp4", []map[string]any{a})
	if err != nil {
		t.Fatal(err)
	}
	sb, err := StableNativeSignatureV1("mp4", []map[string]any{b})
	if err != nil {
		t.Fatal(err)
	}
	ha, _, _ := CanonicalSHA(sa)
	hb, _, _ := CanonicalSHA(sb)
	if ha != hb {
		t.Fatalf("clip-local timing fragmented signature: %s %s", ha, hb)
	}
}

func TestStableNativeSignatureChangesOnDecoderLayout(t *testing.T) {
	base := map[string]any{"codec_type": "video", "codec_name": "h264", "profile": "High", "level": 40, "time_base": "1/90000", "extradata_hash": "SHA256:x", "pix_fmt": "yuv420p", "width": 1920, "height": 1080}
	first, _ := StableNativeSignatureV1("mp4", []map[string]any{base})
	firstSHA, _, _ := CanonicalSHA(first)
	for _, key := range []string{"extradata_hash", "width", "pix_fmt", "codec_name"} {
		changed := map[string]any{}
		for k, v := range base {
			changed[k] = v
		}
		changed[key] = "different"
		signature, _ := StableNativeSignatureV1("mp4", []map[string]any{changed})
		got, _, _ := CanonicalSHA(signature)
		if got == firstSHA {
			t.Fatalf("%s change did not create boundary", key)
		}
	}
}

func TestStableNativeSignatureCanonicalizesStreamOrder(t *testing.T) {
	video := map[string]any{"index": 0, "codec_type": "video", "codec_name": "h264", "time_base": "1/90000", "extradata_hash": "SHA256:v", "pix_fmt": "yuv420p", "width": 1920, "height": 1080}
	audio := map[string]any{"index": 1, "codec_type": "audio", "codec_name": "aac", "time_base": "1/48000", "extradata_hash": "SHA256:a", "sample_fmt": "fltp", "sample_rate": "48000", "channels": 2}
	first, err := StableNativeSignatureV1("mov,mp4", []map[string]any{video, audio})
	if err != nil {
		t.Fatal(err)
	}
	video["index"], audio["index"] = 9, 3
	second, err := StableNativeSignatureV1("mov,mp4", []map[string]any{audio, video})
	if err != nil {
		t.Fatal(err)
	}
	firstSHA, _, _ := CanonicalSHA(first)
	secondSHA, _, _ := CanonicalSHA(second)
	if firstSHA != secondSHA {
		t.Fatalf("stream index/order changed stable signature: %s != %s", firstSHA, secondSHA)
	}
}
