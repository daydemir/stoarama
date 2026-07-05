package surveydetect

import (
	"fmt"

	ort "github.com/yalue/onnxruntime_go"
)

// outputAnchorCount finds the named output and returns its anchor dimension. The
// yolo11 export output is [1, 4+numClasses, anchors] (channels-first). We assert
// the channel dim is 4+numClasses and return the trailing anchor count so the
// output buffer is sized exactly, never assumed.
func outputAnchorCount(outputs []ort.InputOutputInfo, name string) (int, error) {
	for _, o := range outputs {
		if o.Name != name {
			continue
		}
		d := o.Dimensions
		if len(d) != 3 {
			return 0, fmt.Errorf("surveydetect: output %q rank %d, want 3 ([1,%d,anchors])", name, len(d), 4+numClasses)
		}
		if d[1] != int64(4+numClasses) {
			return 0, fmt.Errorf("surveydetect: output %q channel dim %d, want %d (4 bbox + %d classes)", name, d[1], 4+numClasses, numClasses)
		}
		if d[2] <= 0 {
			return 0, fmt.Errorf("surveydetect: output %q anchor dim %d is not static; dynamic export unsupported", name, d[2])
		}
		return int(d[2]), nil
	}
	return 0, fmt.Errorf("surveydetect: output %q not found in model", name)
}

// validateInputShape asserts the model input is [1,3,imgsz,imgsz] so a model
// exported at a different imgsz fails fast rather than producing garbage.
func validateInputShape(inputs []ort.InputOutputInfo, name string, imgsz int64) error {
	for _, in := range inputs {
		if in.Name != name {
			continue
		}
		d := in.Dimensions
		if len(d) != 4 || d[0] != 1 || d[1] != 3 || d[2] != imgsz || d[3] != imgsz {
			return fmt.Errorf("surveydetect: input %q shape %v, want [1 3 %d %d]", name, []int64(d), imgsz, imgsz)
		}
		return nil
	}
	return fmt.Errorf("surveydetect: input %q not found in model", name)
}
