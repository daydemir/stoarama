package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/survey"
	"github.com/daydemir/stoarama/backend/internal/surveydetect"
)

// surveyDetectorAdapter bridges the ONNX detector (which returns
// surveydetect.Counts) to the survey.Detector interface, keeping the survey
// package free of the ONNX/cgo dependency.
type surveyDetectorAdapter struct{ d *surveydetect.Detector }

func (a surveyDetectorAdapter) Detect(jpegBytes []byte) (survey.DetectionCounts, int, error) {
	c, ms, err := a.d.Detect(jpegBytes)
	if err != nil {
		return survey.DetectionCounts{}, 0, err
	}
	return survey.DetectionCounts{
		Person:     c.Person,
		Bicycle:    c.Bicycle,
		Car:        c.Car,
		Motorcycle: c.Motorcycle,
		Bus:        c.Bus,
		Truck:      c.Truck,
	}, ms, nil
}

func (a surveyDetectorAdapter) PipelineVersion() string { return a.d.PipelineVersion() }
func (a surveyDetectorAdapter) ConfThreshold() float64  { return a.d.ConfThreshold() }
func (a surveyDetectorAdapter) Imgsz() int              { return a.d.Imgsz() }

// newSurveyDetector builds the yolo11x ONNX detector from the LOCKED survey
// detection config. The onnxruntime shared library path comes from
// ONNXRUNTIME_LIB_PATH (set by cloud-init on the droplet).
func newSurveyDetector(cfg config.Config) (*surveydetect.Detector, error) {
	return surveydetect.NewDetector(surveydetect.Config{
		ModelPath:       cfg.SurveyModelPath,
		Imgsz:           cfg.SurveyDetectImgsz,
		ConfThreshold:   cfg.SurveyDetectConf,
		IoUThreshold:    cfg.SurveyDetectIoU,
		IntraOpThreads:  cfg.SurveyDetectIntraOpThreads,
		PipelineVersion: cfg.SurveyDetectPipelineVersion,
	})
}

// runSurveyDetectImage runs the detector on a single local JPEG and prints the
// per-class counts. It is the operator/droplet verification tool: prove the model
// loads and produces sane counts on a real frame (a busy scene -> nonzero, an
// empty scene -> ~0) before trusting any survey_detections rows.
func runSurveyDetectImage(_ context.Context, cfg config.Config, args []string) {
	fs := flag.NewFlagSet("survey detect-image", flag.ExitOnError)
	path := fs.String("path", "", "path to a JPEG to run detection on")
	asJSON := fs.Bool("json", false, "print JSON")
	_ = fs.Parse(args)
	if strings.TrimSpace(*path) == "" {
		log.Fatalf("--path is required")
	}
	jpegBytes, err := os.ReadFile(*path)
	if err != nil {
		log.Fatalf("read image %s: %v", *path, err)
	}
	d, err := newSurveyDetector(cfg)
	if err != nil {
		log.Fatalf("init survey detector: %v", err)
	}
	defer d.Close()
	c, ms, err := d.Detect(jpegBytes)
	if err != nil {
		log.Fatalf("detect: %v", err)
	}
	if *asJSON {
		printJSON(map[string]any{
			"path":             *path,
			"pipeline_version": d.PipelineVersion(),
			"conf_threshold":   d.ConfThreshold(),
			"imgsz":            d.Imgsz(),
			"detect_ms":        ms,
			"person":           c.Person,
			"bicycle":          c.Bicycle,
			"car":              c.Car,
			"motorcycle":       c.Motorcycle,
			"bus":              c.Bus,
			"truck":            c.Truck,
		})
		return
	}
	fmt.Printf("detect path=%s pipeline=%s detect_ms=%d person=%d bicycle=%d car=%d motorcycle=%d bus=%d truck=%d\n",
		*path, d.PipelineVersion(), ms, c.Person, c.Bicycle, c.Car, c.Motorcycle, c.Bus, c.Truck)
}

// runSurveyDownloadModel fetches the pinned yolo11x ONNX model from R2 and writes
// it to the local path after verifying its sha256. It is invoked by the droplet
// cloud-init at boot; a sha256 mismatch is fail-fast (no fallback, no start).
func runSurveyDownloadModel(ctx context.Context, cfg config.Config, args []string) {
	fs := flag.NewFlagSet("survey download-model", flag.ExitOnError)
	key := fs.String("key", cfg.SurveyModelKey, "R2 object key of the model")
	sha := fs.String("sha256", cfg.SurveyModelSHA256, "expected sha256 hex of the model")
	out := fs.String("out", cfg.SurveyModelPath, "local path to write the model")
	_ = fs.Parse(args)
	if strings.TrimSpace(*key) == "" || strings.TrimSpace(*sha) == "" || strings.TrimSpace(*out) == "" {
		log.Fatalf("--key, --sha256, --out are all required (or set SURVEY_MODEL_KEY/SHA256/PATH)")
	}
	r2c := mustArchiveR2Client(ctx, cfg)
	b, err := r2c.Get(ctx, *key)
	if err != nil {
		log.Fatalf("download model %s: %v", *key, err)
	}
	sum := sha256.Sum256(b)
	got := hex.EncodeToString(sum[:])
	if !strings.EqualFold(got, strings.TrimSpace(*sha)) {
		log.Fatalf("model sha256 mismatch: got %s want %s (refusing to write)", got, *sha)
	}
	if err := os.MkdirAll(dirOf(*out), 0o755); err != nil {
		log.Fatalf("mkdir for %s: %v", *out, err)
	}
	if err := os.WriteFile(*out, b, 0o644); err != nil {
		log.Fatalf("write model %s: %v", *out, err)
	}
	fmt.Printf("survey download-model: wrote %s (%d bytes, sha256=%s)\n", *out, len(b), got)
}

func dirOf(p string) string {
	i := strings.LastIndex(p, "/")
	if i <= 0 {
		return "."
	}
	return p[:i]
}
