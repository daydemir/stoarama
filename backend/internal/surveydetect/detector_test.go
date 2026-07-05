package surveydetect

import (
	"os"
	"strconv"
	"testing"
)

// TestDetectReferenceImage is the real-data proof required by #47: it loads the
// actual yolo11x-1600 ONNX model and runs it on a human-verified reference image,
// asserting the counts match the known truth within tolerance. This catches a
// mis-decoded output tensor (the yolo11 [1,84,anchors] transposed / no-objectness
// postprocess trap), where a wrong decode can yield plausible-but-nonzero counts.
//
// It is SKIPPED unless the model + onnxruntime library + reference image are
// provided via env, so `go test ./...` stays green in CI without the 218MB model
// or the cgo shared library:
//
//	SURVEY_DETECT_MODEL=/path/yolo11x-1600.onnx \
//	ONNXRUNTIME_LIB_PATH=/path/libonnxruntime.dylib \
//	SURVEY_DETECT_REF_IMAGE=/path/bus.jpg \
//	SURVEY_DETECT_REF_PERSON=4 SURVEY_DETECT_REF_BUS=1 \
//	go test ./internal/surveydetect/ -run TestDetectReferenceImage -v
func TestDetectReferenceImage(t *testing.T) {
	modelPath := os.Getenv("SURVEY_DETECT_MODEL")
	refPath := os.Getenv("SURVEY_DETECT_REF_IMAGE")
	if modelPath == "" || refPath == "" {
		t.Skip("set SURVEY_DETECT_MODEL, ONNXRUNTIME_LIB_PATH, SURVEY_DETECT_REF_IMAGE to run the real-inference proof")
	}

	det, err := NewDetector(Config{
		ModelPath:       modelPath,
		Imgsz:           1600,
		ConfThreshold:   0.10,
		IoUThreshold:    0.45,
		IntraOpThreads:  4,
		PipelineVersion: "yolo11x-img1600-conf010-notile-v1",
	})
	if err != nil {
		t.Fatalf("NewDetector: %v", err)
	}
	defer det.Close()

	jpegBytes, err := os.ReadFile(refPath)
	if err != nil {
		t.Fatalf("read reference image: %v", err)
	}
	counts, ms, err := det.Detect(jpegBytes)
	if err != nil {
		t.Fatalf("Detect: %v", err)
	}
	t.Logf("reference counts: person=%d bicycle=%d car=%d motorcycle=%d bus=%d truck=%d (detect_ms=%d)",
		counts.Person, counts.Bicycle, counts.Car, counts.Motorcycle, counts.Bus, counts.Truck, ms)

	// Human-verified reference truth (bus.jpg = 4 persons + 1 bus). Assert within
	// tolerance so a correct decode is required, not merely nonzero output.
	assertNear(t, "person", counts.Person, intEnvDefault("SURVEY_DETECT_REF_PERSON", 4), 1)
	assertNear(t, "bus", counts.Bus, intEnvDefault("SURVEY_DETECT_REF_BUS", 1), 1)
}

func assertNear(t *testing.T, label string, got, want, tol int) {
	t.Helper()
	if got < want-tol || got > want+tol {
		t.Errorf("%s count = %d, want %d +/- %d (wrong-but-nonzero decode?)", label, got, want, tol)
	}
}

func intEnvDefault(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
