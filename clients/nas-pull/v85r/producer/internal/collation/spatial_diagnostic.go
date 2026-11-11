package collation

import "math"

// SpatialDiagnostic is research evidence only. It never sets a join verdict.
// Offsets are pixels/frame, NOT physical camera velocity or a clock proof.
type SpatialDiagnostic struct {
	Version                    string   `json:"version"`
	Status                     string   `json:"status"`
	TailDX                     float64  `json:"tail_dx_pixels_per_frame"`
	TailDY                     float64  `json:"tail_dy_pixels_per_frame"`
	HeadDX                     float64  `json:"head_dx_pixels_per_frame"`
	HeadDY                     float64  `json:"head_dy_pixels_per_frame"`
	ForwardResidual            float64  `json:"forward_heldout_residual_mad"`
	BackwardResidual           float64  `json:"backward_heldout_residual_mad"`
	InternalResidual           float64  `json:"internal_heldout_residual_mad"`
	DirectionSpeedDisagreement float64  `json:"direction_speed_disagreement"`
	MaxAcceleration            float64  `json:"max_acceleration_pixels_per_frame_squared"`
	TextureMAD                 float64  `json:"texture_gradient_mad"`
	MinimumNoveltyMargin       float64  `json:"minimum_spatial_novelty_margin"`
	BoundaryUnwarpedMAD        float64  `json:"boundary_unwarped_mad"`
	HeldOutPixels              int      `json:"heldout_pixels"`
	MotionObservable           bool     `json:"motion_observable"`
	Ambiguities                []string `json:"ambiguities"`
}
type spatialShift struct {
	dx, dy           int
	residual, margin float64
}

func spatialAbs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}
func spatialMedian(v []float64) float64 {
	a := append([]float64(nil), v...)
	for i := 1; i < len(a); i++ {
		for j := i; j > 0 && a[j] < a[j-1]; j-- {
			a[j], a[j-1] = a[j-1], a[j]
		}
	}
	n := len(a)
	if n == 0 {
		return 0
	}
	if n%2 == 0 {
		return (a[n/2-1] + a[n/2]) / 2
	}
	return a[n/2]
}

// Fixed common interior for EVERY candidate prevents smaller shifted support
// from making a candidate look good. Fit pixels and held-out pixels are disjoint.
func spatialMAD(a, b Frame, w, h, r, dx, dy, split int) (float64, int) {
	var sum float64
	n := 0
	for y := r; y < h-r; y++ {
		for x := r; x < w-r; x++ {
			held := (x+2*y)%3 == 0
			if (split == 1 && !held) || (split == 0 && held) {
				continue
			}
			sum += spatialAbs(float64(a[y*w+x]) - float64(b[(y+dy)*w+x+dx]))
			n++
		}
	}
	if n == 0 {
		return 0, 0
	}
	return sum / float64(n), n
}
func spatialFit(a, b Frame, w, h, r int) spatialShift {
	best, second := math.Inf(1), math.Inf(1)
	v := spatialShift{}
	for dy := -r; dy <= r; dy++ {
		for dx := -r; dx <= r; dx++ {
			m, _ := spatialMAD(a, b, w, h, r, dx, dy, 0)
			if m < best {
				second = best
				best = m
				v.dx = dx
				v.dy = dy
			} else if m < second {
				second = m
			}
		}
	}
	v.residual, _ = spatialMAD(a, b, w, h, r, v.dx, v.dy, 1)
	v.margin = second - best
	return v
}
func spatialSide(f []Frame, w, h, r int) (float64, float64, float64, float64, float64) {
	xs, ys, ms := []float64{}, []float64{}, []float64{}
	margin := math.Inf(1)
	acc := 0.
	var last spatialShift
	for i := 1; i < len(f); i++ {
		v := spatialFit(f[i-1], f[i], w, h, r)
		xs = append(xs, float64(v.dx))
		ys = append(ys, float64(v.dy))
		ms = append(ms, v.residual)
		margin = math.Min(margin, v.margin)
		if i > 1 {
			acc = math.Max(acc, float64(absSpatialInt(v.dx-last.dx)+absSpatialInt(v.dy-last.dy)))
		}
		last = v
	}
	return spatialMedian(xs), spatialMedian(ys), spatialMedian(ms), margin, acc
}
func absSpatialInt(v int) int {
	if v < 0 {
		return -v
	}
	return v
}
func spatialTexture(f Frame, w, h, r int) float64 {
	var s float64
	n := 0
	for y := r; y < h-r; y++ {
		for x := r; x < w-r; x++ {
			s += spatialAbs(float64(f[y*w+x]) - float64(f[y*w+x+1]))
			n++
		}
	}
	if n == 0 {
		return 0
	}
	return s / float64(n)
}

// DiagnoseSpatialSeam fits ONLY within-side frame pairs. It deliberately never
// optimizes a warp using the seam itself, which could conceal an unrelated cut.
// Six frames each side, translation-only integer search radius4. Input frames
// must already be in source presentation order; source/PTS qualification is
// external and remains mandatory. This version does not support VFR scaling.
func DiagnoseSpatialSeam(tail, head []Frame, w, h int) SpatialDiagnostic {
	d := SpatialDiagnostic{Version: "v85-spatial-translation-diagnostic-v1", Status: "unavailable"}
	r := 4
	if w <= 2*r+1 || h <= 2*r+1 || len(tail) < 6 || len(head) < 6 {
		d.Ambiguities = []string{"insufficient_geometry_or_frames"}
		return d
	}
	a, b := tail[len(tail)-6:], head[:6]
	for _, frames := range [][]Frame{a, b} {
		for _, f := range frames {
			if len(f) != w*h {
				d.Ambiguities = []string{"malformed_frame_geometry"}
				return d
			}
		}
	}
	tx, ty, tr, tm, ta := spatialSide(a, w, h, r)
	hx, hy, hr, hm, ha := spatialSide(b, w, h, r)
	d.TailDX, d.TailDY, d.HeadDX, d.HeadDY = tx, ty, hx, hy
	d.InternalResidual = math.Max(tr, hr)
	d.MinimumNoveltyMargin = math.Min(tm, hm)
	d.MaxAcceleration = math.Max(ta, ha)
	d.DirectionSpeedDisagreement = math.Hypot(tx-hx, ty-hy)
	// Forward uses tail's forecast; backward uses head's independently inferred
	// velocity with inverse source/target ordering. Neither fits endpoint frames.
	d.ForwardResidual, d.HeldOutPixels = spatialMAD(a[5], b[0], w, h, r, int(math.Round(tx)), int(math.Round(ty)), 1)
	d.BackwardResidual, _ = spatialMAD(b[0], a[5], w, h, r, -int(math.Round(hx)), -int(math.Round(hy)), 1)
	d.BoundaryUnwarpedMAD, _ = spatialMAD(a[5], b[0], w, h, r, 0, 0, 1)
	d.TextureMAD = math.Min(spatialTexture(a[5], w, h, r), spatialTexture(b[0], w, h, r))
	d.MotionObservable = math.Min(math.Hypot(tx, ty), math.Hypot(hx, hy)) >= .5 && d.TextureMAD >= 2 && d.MinimumNoveltyMargin >= 1
	if d.TextureMAD < 2 {
		d.Ambiguities = append(d.Ambiguities, "low_texture")
	}
	if !d.MotionObservable {
		d.Ambiguities = append(d.Ambiguities, "no_observable_motion")
	}
	if d.MinimumNoveltyMargin < 1 {
		d.Ambiguities = append(d.Ambiguities, "nonunique_spatial_translation")
	}
	if d.DirectionSpeedDisagreement >= 1 {
		d.Ambiguities = append(d.Ambiguities, "side_motion_disagreement")
	}
	if d.MaxAcceleration >= 1 {
		d.Ambiguities = append(d.Ambiguities, "nonconstant_motion")
	}
	if math.Max(math.Max(spatialAbs(tx), spatialAbs(ty)), math.Max(spatialAbs(hx), spatialAbs(hy))) >= float64(r) {
		d.Ambiguities = append(d.Ambiguities, "search_radius_saturated")
	}
	d.Status = "diagnostic_only"
	return d
}
