// Package surveydetect runs yolo11x ONNX detection in-process on a survey JPEG
// and returns per-class COUNTS only (no boxes, no crops, no footage). It is the
// greenfield in-process runner for the unified survey+detection droplet (#47):
// the same binary that captures the frame feeds frame.Bytes straight to the model
// with no network hop.
//
// LOCKED config: yolo11x, ONNX, whole-frame imgsz=1600, conf=0.10, NO tiling.
// Classes counted (COCO): person 0, bicycle 1, car 2, motorcycle 3, bus 5,
// truck 7 (train 6 intentionally excluded). Output is metrics-only.
//
// The ONNX runtime is the cgo binding github.com/yalue/onnxruntime_go, which
// dlopen's a prebuilt onnxruntime CPU shared library at runtime. The library path
// comes from ONNXRUNTIME_LIB_PATH (set by cloud-init on the droplet, or locally
// for verification); the package never bundles the .so. Model load or a runtime
// failure is fail-fast (no fallback, no stub).
package surveydetect

import (
	"bytes"
	"fmt"
	"image"
	"image/color"
	"image/jpeg"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	ort "github.com/yalue/onnxruntime_go"
	xdraw "golang.org/x/image/draw"
)

// COCO class ids counted (LOCKED). Any change here is a pipeline_version bump.
const (
	classPerson     = 0
	classBicycle    = 1
	classCar        = 2
	classMotorcycle = 3
	classBus        = 5
	classTruck      = 7
)

// countedClasses is the fixed set of COCO class ids the survey counts, mapped to
// the survey_detections column each contributes to. Train (6) is excluded.
var countedClasses = map[int]struct{}{
	classPerson: {}, classBicycle: {}, classCar: {},
	classMotorcycle: {}, classBus: {}, classTruck: {},
}

// numClasses is the COCO class count in the yolo11x head (person..toothbrush).
const numClasses = 80

// grayPad is the letterbox fill (ultralytics default 114) used for the padded
// border so the aspect ratio is preserved before the square model input.
const grayPad = 114

// Counts is the metrics-only detection result for one frame: per-class survivor
// counts after conf-thresholding and per-class NMS. No boxes are retained.
type Counts struct {
	Person     int
	Bicycle    int
	Car        int
	Motorcycle int
	Bus        int
	Truck      int
}

// Config is the LOCKED detection configuration. It is derived from env on the
// droplet so a config change is an env/render.yaml change plus a version bump.
type Config struct {
	ModelPath       string  // local path to yolo11x-1600.onnx (downloaded + sha-verified by cloud-init)
	Imgsz           int     // model square input, LOCKED 1600
	ConfThreshold   float64 // LOCKED 0.10
	IoUThreshold    float64 // per-class NMS IoU, 0.45
	IntraOpThreads  int     // ONNX intra-op threads (2 on s-2vcpu-4gb)
	PipelineVersion string  // e.g. yolo11x-img1600-conf010-notile-v1
	InputName       string  // ONNX input tensor name (default "images")
	OutputName      string  // ONNX output tensor name (default "output0")
}

// ortInitOnce guards the process-global onnxruntime environment. The library is
// initialized exactly once for the whole process (all detectors share it).
var (
	ortInitOnce sync.Once
	ortInitErr  error
)

func initORT() error {
	ortInitOnce.Do(func() {
		if lib := strings.TrimSpace(os.Getenv("ONNXRUNTIME_LIB_PATH")); lib != "" {
			ort.SetSharedLibraryPath(lib)
		}
		if err := ort.InitializeEnvironment(); err != nil {
			ortInitErr = fmt.Errorf("initialize onnxruntime (ONNXRUNTIME_LIB_PATH=%q): %w", os.Getenv("ONNXRUNTIME_LIB_PATH"), err)
		}
	})
	return ortInitErr
}

// Detector holds a loaded yolo11x ONNX session reused across every frame in a
// sweep (loading the model per frame would be fatal to throughput). It is NOT
// safe for concurrent Detect calls: the input/output tensors are fixed buffers,
// so callers serialize detection (the survey sweep runs detection on a single
// gate per stream; CPU-bound at 2 vCPU it is serial anyway).
type Detector struct {
	cfg     Config
	session *ort.AdvancedSession
	input   *ort.Tensor[float32]
	output  *ort.Tensor[float32]
	anchors int // output anchor count (e.g. 52500 at imgsz 1600)
	mu      sync.Mutex
}

// NewDetector loads the model and creates a CPU inference session. Fail-fast: a
// missing model, missing/mismatched onnxruntime library, or a shape that does not
// match the LOCKED imgsz returns an error and no Detector (never a stub).
func NewDetector(cfg Config) (*Detector, error) {
	if cfg.Imgsz <= 0 {
		return nil, fmt.Errorf("surveydetect: imgsz must be > 0")
	}
	if cfg.ConfThreshold <= 0 || cfg.ConfThreshold >= 1 {
		return nil, fmt.Errorf("surveydetect: conf threshold must be in (0,1), got %v", cfg.ConfThreshold)
	}
	if cfg.IoUThreshold <= 0 || cfg.IoUThreshold >= 1 {
		cfg.IoUThreshold = 0.45
	}
	if strings.TrimSpace(cfg.ModelPath) == "" {
		return nil, fmt.Errorf("surveydetect: model path is empty")
	}
	if _, err := os.Stat(cfg.ModelPath); err != nil {
		return nil, fmt.Errorf("surveydetect: model not readable at %q: %w", cfg.ModelPath, err)
	}
	if cfg.InputName == "" {
		cfg.InputName = "images"
	}
	if cfg.OutputName == "" {
		cfg.OutputName = "output0"
	}
	if cfg.IntraOpThreads <= 0 {
		cfg.IntraOpThreads = 2
	}
	if err := initORT(); err != nil {
		return nil, err
	}

	// Discover the output anchor count from the model so the output buffer matches
	// exactly (52500 at imgsz 1600, but derived, not assumed).
	inputs, outputs, err := ort.GetInputOutputInfo(cfg.ModelPath)
	if err != nil {
		return nil, fmt.Errorf("surveydetect: inspect model io %q: %w", cfg.ModelPath, err)
	}
	anchors, err := outputAnchorCount(outputs, cfg.OutputName)
	if err != nil {
		return nil, err
	}
	if err := validateInputShape(inputs, cfg.InputName, int64(cfg.Imgsz)); err != nil {
		return nil, err
	}

	inputShape := ort.NewShape(1, 3, int64(cfg.Imgsz), int64(cfg.Imgsz))
	inTensor, err := ort.NewEmptyTensor[float32](inputShape)
	if err != nil {
		return nil, fmt.Errorf("surveydetect: alloc input tensor: %w", err)
	}
	outShape := ort.NewShape(1, int64(4+numClasses), int64(anchors))
	outTensor, err := ort.NewEmptyTensor[float32](outShape)
	if err != nil {
		inTensor.Destroy()
		return nil, fmt.Errorf("surveydetect: alloc output tensor: %w", err)
	}

	opts, err := ort.NewSessionOptions()
	if err != nil {
		inTensor.Destroy()
		outTensor.Destroy()
		return nil, fmt.Errorf("surveydetect: session options: %w", err)
	}
	defer opts.Destroy()
	_ = opts.SetIntraOpNumThreads(cfg.IntraOpThreads)

	session, err := ort.NewAdvancedSession(cfg.ModelPath,
		[]string{cfg.InputName}, []string{cfg.OutputName},
		[]ort.Value{inTensor}, []ort.Value{outTensor}, opts)
	if err != nil {
		inTensor.Destroy()
		outTensor.Destroy()
		return nil, fmt.Errorf("surveydetect: create session: %w", err)
	}

	return &Detector{cfg: cfg, session: session, input: inTensor, output: outTensor, anchors: anchors}, nil
}

// Close releases the ONNX session and tensors. The process-global environment is
// left initialized (shared, cheap) for the process lifetime.
func (d *Detector) Close() error {
	if d == nil {
		return nil
	}
	if d.session != nil {
		d.session.Destroy()
	}
	if d.input != nil {
		d.input.Destroy()
	}
	if d.output != nil {
		d.output.Destroy()
	}
	return nil
}

// PipelineVersion returns the configured version string stamped on every row.
func (d *Detector) PipelineVersion() string { return d.cfg.PipelineVersion }

// ConfThreshold returns the configured detection confidence threshold.
func (d *Detector) ConfThreshold() float64 { return d.cfg.ConfThreshold }

// Imgsz returns the configured model input size.
func (d *Detector) Imgsz() int { return d.cfg.Imgsz }

// Detect decodes the JPEG, letterboxes to the square input, runs one forward
// pass, applies conf-thresholding + per-class NMS, and returns per-class counts
// plus the wall-clock inference time (detect_ms). It is serialized by a mutex.
func (d *Detector) Detect(jpegBytes []byte) (Counts, int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	img, err := jpeg.Decode(bytes.NewReader(jpegBytes))
	if err != nil {
		return Counts{}, 0, fmt.Errorf("surveydetect: decode jpeg: %w", err)
	}
	if err := d.letterboxInto(img); err != nil {
		return Counts{}, 0, err
	}

	start := time.Now()
	if err := d.session.Run(); err != nil {
		return Counts{}, 0, fmt.Errorf("surveydetect: run inference: %w", err)
	}
	elapsedMs := int(time.Since(start).Milliseconds())

	counts := d.postprocess(d.output.GetData())
	return counts, elapsedMs, nil
}

// letterboxInto resizes img preserving aspect ratio onto a square imgsz canvas
// padded with gray (114), then writes the RGB /255 CHW float32 planes straight
// into the reused input tensor buffer.
func (d *Detector) letterboxInto(img image.Image) error {
	sz := d.cfg.Imgsz
	b := img.Bounds()
	w0, h0 := b.Dx(), b.Dy()
	if w0 <= 0 || h0 <= 0 {
		return fmt.Errorf("surveydetect: empty image bounds")
	}
	scale := float64(sz) / float64(w0)
	if s := float64(sz) / float64(h0); s < scale {
		scale = s
	}
	newW := int(float64(w0) * scale)
	newH := int(float64(h0) * scale)
	if newW < 1 {
		newW = 1
	}
	if newH < 1 {
		newH = 1
	}
	padX := (sz - newW) / 2
	padY := (sz - newH) / 2

	// Gray canvas, then scale the source into the centered rectangle.
	canvas := image.NewRGBA(image.Rect(0, 0, sz, sz))
	gray := image.NewUniform(color.RGBA{R: grayPad, G: grayPad, B: grayPad, A: 255})
	xdraw.Draw(canvas, canvas.Bounds(), gray, image.Point{}, xdraw.Src)
	dst := image.Rect(padX, padY, padX+newW, padY+newH)
	xdraw.CatmullRom.Scale(canvas, dst, img, b, xdraw.Over, nil)

	data := d.input.GetData()
	plane := sz * sz
	for y := 0; y < sz; y++ {
		for x := 0; x < sz; x++ {
			r, g, bl, _ := canvas.At(x, y).RGBA() // 16-bit
			idx := y*sz + x
			data[idx] = float32(r>>8) / 255.0            // R plane
			data[plane+idx] = float32(g>>8) / 255.0      // G plane
			data[2*plane+idx] = float32(bl>>8) / 255.0   // B plane
		}
	}
	return nil
}

// postprocess decodes the [1, 84, anchors] output (channels-first, decoded boxes
// in input-pixel coords + class probabilities, NO objectness) into per-class
// counts. For each anchor it takes the best COUNTED class above conf, then runs
// per-class greedy NMS at the configured IoU. Boxes are discarded after counting.
func (d *Detector) postprocess(out []float32) Counts {
	n := d.anchors
	type det struct {
		x1, y1, x2, y2 float32
		score          float32
	}
	perClass := make(map[int][]det)

	for a := 0; a < n; a++ {
		bestClass := -1
		var bestScore float32
		for c := 0; c < numClasses; c++ {
			if _, ok := countedClasses[c]; !ok {
				continue
			}
			s := out[(4+c)*n+a]
			if s > bestScore {
				bestScore = s
				bestClass = c
			}
		}
		if bestClass < 0 || float64(bestScore) < d.cfg.ConfThreshold {
			continue
		}
		cx := out[0*n+a]
		cy := out[1*n+a]
		w := out[2*n+a]
		h := out[3*n+a]
		perClass[bestClass] = append(perClass[bestClass], det{
			x1: cx - w/2, y1: cy - h/2, x2: cx + w/2, y2: cy + h/2, score: bestScore,
		})
	}

	counts := Counts{}
	for cls, dets := range perClass {
		sort.Slice(dets, func(i, j int) bool { return dets[i].score > dets[j].score })
		kept := 0
		suppressed := make([]bool, len(dets))
		for i := 0; i < len(dets); i++ {
			if suppressed[i] {
				continue
			}
			kept++
			for j := i + 1; j < len(dets); j++ {
				if suppressed[j] {
					continue
				}
				if iou(dets[i].x1, dets[i].y1, dets[i].x2, dets[i].y2,
					dets[j].x1, dets[j].y1, dets[j].x2, dets[j].y2) > float32(d.cfg.IoUThreshold) {
					suppressed[j] = true
				}
			}
		}
		switch cls {
		case classPerson:
			counts.Person = kept
		case classBicycle:
			counts.Bicycle = kept
		case classCar:
			counts.Car = kept
		case classMotorcycle:
			counts.Motorcycle = kept
		case classBus:
			counts.Bus = kept
		case classTruck:
			counts.Truck = kept
		}
	}
	return counts
}

func iou(ax1, ay1, ax2, ay2, bx1, by1, bx2, by2 float32) float32 {
	ix1 := maxf(ax1, bx1)
	iy1 := maxf(ay1, by1)
	ix2 := minf(ax2, bx2)
	iy2 := minf(ay2, by2)
	iw := ix2 - ix1
	ih := iy2 - iy1
	if iw <= 0 || ih <= 0 {
		return 0
	}
	inter := iw * ih
	areaA := (ax2 - ax1) * (ay2 - ay1)
	areaB := (bx2 - bx1) * (by2 - by1)
	union := areaA + areaB - inter
	if union <= 0 {
		return 0
	}
	return inter / union
}

func maxf(a, b float32) float32 {
	if a > b {
		return a
	}
	return b
}

func minf(a, b float32) float32 {
	if a < b {
		return a
	}
	return b
}
