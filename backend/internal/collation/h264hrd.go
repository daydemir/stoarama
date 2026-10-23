package collation

import (
	"errors"
	"fmt"
	"math"
	"sort"
)

// H.264 HRD source-continuity evidence (policy collation-v3).
//
// Many live sources (YouTube transcodes among them) carry NAL HRD buffering
// period (BP) and picture timing (PT) SEI. At every BP access unit the encoder
// writes initial_cpb_removal_delay (ICRD): its own coded-picture-buffer state.
// While the buffer is not saturated, that state evolves deterministically from
// the previous BP's state, the bits the encoder emitted in between and the
// removal-time distance it writes into the next BP's PT SEI:
//
//	ICRD[k+1] = ICRD[k] + 90000*(cpb_removal_delay[k+1]*tick - bits[k]/bit_rate) + c
//
// with c a stream constant (0 for a conforming encoder). Stream copy keeps
// every byte, so this identity also holds across a clip seam exactly when the
// next clip's first BP access unit is the encoder's very next BP after the
// previous clip's last one. Any skipped or replayed access unit changes the
// accounted bits or the encoder state and breaks the identity by hundreds of
// 90 kHz ticks or more. It is evidence about the source's own time line,
// independent of capture timestamps, sequence numbers and pixels.

// HRDParams are the NAL HRD and timing parameters of one SPS.
type HRDParams struct {
	BitRate        int64 `json:"bit_rate"`
	CPBSize        int64 `json:"cpb_size"`
	CBR            bool  `json:"cbr"`
	NumUnitsInTick int64 `json:"num_units_in_tick"`
	TimeScale      int64 `json:"time_scale"`
	ICRDLength     int   `json:"icrd_length"`
	CPBDelayLength int   `json:"cpb_delay_length"`
	DPBDelayLength int   `json:"dpb_delay_length"`
	VCLHRD         bool  `json:"vcl_hrd,omitempty"`
	CPBCount       int   `json:"cpb_count"`
}

// Tick is the PT clock tick in seconds.
func (p HRDParams) Tick() float64 { return float64(p.NumUnitsInTick) / float64(p.TimeScale) }

// ClampTicks is the ICRD of a full coded picture buffer (90 kHz).
func (p HRDParams) ClampTicks() float64 { return 90000 * float64(p.CPBSize) / float64(p.BitRate) }

// HRDUnit is the HRD-relevant content of one stored access unit.
type HRDUnit struct {
	Bytes    int64 `json:"bytes"`
	Key      bool  `json:"key,omitempty"`
	BP       bool  `json:"bp,omitempty"`
	ICRD     int64 `json:"icrd,omitempty"`
	HasPT    bool  `json:"pt,omitempty"`
	CPBDelay int64 `json:"cpb,omitempty"`
}

// HRDTrack is the HRD view of a clip edge: the stream's HRD parameters and
// the access units of the tail (previous clip) or head (next clip).
type HRDTrack struct {
	Params HRDParams `json:"params"`
	Units  []HRDUnit `json:"units"`
}

type bitReader struct {
	b   []byte
	pos int // bit position
}

var errBits = errors.New("bitstream truncated")

func (r *bitReader) u(n int) (uint64, error) {
	if n < 0 || n > 64 || r.pos+n > 8*len(r.b) {
		return 0, errBits
	}
	var v uint64
	for i := 0; i < n; i++ {
		v = v<<1 | uint64(r.b[(r.pos)>>3]>>(7-uint(r.pos&7))&1)
		r.pos++
	}
	return v, nil
}

func (r *bitReader) flag() (bool, error) {
	v, err := r.u(1)
	return v == 1, err
}

func (r *bitReader) ue() (uint64, error) {
	zeros := 0
	for {
		b, err := r.u(1)
		if err != nil {
			return 0, err
		}
		if b == 1 {
			break
		}
		if zeros++; zeros > 31 {
			return 0, errors.New("exp-golomb code too long")
		}
	}
	v, err := r.u(zeros)
	return (1<<uint(zeros) - 1) + v, err
}

func (r *bitReader) se() (int64, error) {
	v, err := r.ue()
	if v&1 == 1 {
		return int64((v + 1) / 2), err
	}
	return -int64(v / 2), err
}

// rbsp removes emulation-prevention bytes from a NAL payload.
func rbsp(nal []byte) []byte {
	out := make([]byte, 0, len(nal))
	zeros := 0
	for _, c := range nal {
		if zeros >= 2 && c == 3 {
			zeros = 0
			continue
		}
		if c == 0 {
			zeros++
		} else {
			zeros = 0
		}
		out = append(out, c)
	}
	return out
}

// parseSPSHRD reads an SPS NAL (header byte included) up to its NAL HRD
// parameters. ok is false when the SPS carries no NAL HRD.
func parseSPSHRD(nal []byte) (p HRDParams, ok bool, err error) {
	if len(nal) < 4 || nal[0]&0x1f != 7 {
		return p, false, errors.New("not an sps")
	}
	r := &bitReader{b: rbsp(nal[1:])}
	profile, _ := r.u(8)
	r.u(8) // constraint flags
	r.u(8) // level
	if _, err := r.ue(); err != nil {
		return p, false, err
	}
	switch profile {
	case 100, 110, 122, 244, 44, 83, 86, 118, 128, 138, 139, 134, 135:
		chroma, err := r.ue()
		if err != nil {
			return p, false, err
		}
		if chroma == 3 {
			r.u(1)
		}
		r.ue()
		r.ue()
		r.u(1)
		scaling, err := r.flag()
		if err != nil {
			return p, false, err
		}
		if scaling {
			lists := 8
			if chroma == 3 {
				lists = 12
			}
			for i := 0; i < lists; i++ {
				present, err := r.flag()
				if err != nil {
					return p, false, err
				}
				if !present {
					continue
				}
				size := 16
				if i >= 6 {
					size = 64
				}
				last, next := int64(8), int64(8)
				for j := 0; j < size; j++ {
					if next != 0 {
						delta, err := r.se()
						if err != nil {
							return p, false, err
						}
						next = (last + delta + 256) % 256
					}
					if next != 0 {
						last = next
					}
				}
			}
		}
	}
	r.ue() // log2_max_frame_num_minus4
	pocType, err := r.ue()
	if err != nil {
		return p, false, err
	}
	switch pocType {
	case 0:
		r.ue()
	case 1:
		r.u(1)
		r.se()
		r.se()
		n, err := r.ue()
		if err != nil || n > 255 {
			return p, false, errors.New("sps poc cycle invalid")
		}
		for i := uint64(0); i < n; i++ {
			r.se()
		}
	}
	r.ue() // max_num_ref_frames
	r.u(1)
	r.ue()
	r.ue()
	frameMbsOnly, _ := r.flag()
	if !frameMbsOnly {
		r.u(1)
	}
	r.u(1)
	if crop, _ := r.flag(); crop {
		r.ue()
		r.ue()
		r.ue()
		r.ue()
	}
	vui, err := r.flag()
	if err != nil || !vui {
		return p, false, err
	}
	if ar, _ := r.flag(); ar {
		if idc, _ := r.u(8); idc == 255 {
			r.u(16)
			r.u(16)
		}
	}
	if overscan, _ := r.flag(); overscan {
		r.u(1)
	}
	if signal, _ := r.flag(); signal {
		r.u(3)
		r.u(1)
		if colour, _ := r.flag(); colour {
			r.u(24)
		}
	}
	if loc, _ := r.flag(); loc {
		r.ue()
		r.ue()
	}
	timing, err := r.flag()
	if err != nil || !timing {
		return p, false, err
	}
	nu, _ := r.u(32)
	ts, _ := r.u(32)
	r.u(1)
	p.NumUnitsInTick, p.TimeScale = int64(nu), int64(ts)
	nalHRD, err := r.flag()
	if err != nil || !nalHRD {
		return p, false, err
	}
	cnt, err := r.ue()
	if err != nil || cnt > 31 {
		return p, false, errors.New("hrd cpb count invalid")
	}
	brScale, _ := r.u(4)
	csScale, _ := r.u(4)
	for i := uint64(0); i <= cnt; i++ {
		br, _ := r.ue()
		cs, _ := r.ue()
		cbr, err := r.flag()
		if err != nil {
			return p, false, err
		}
		if i == 0 {
			p.BitRate = int64(br+1) << (6 + brScale)
			p.CPBSize = int64(cs+1) << (4 + csScale)
			p.CBR = cbr
		}
	}
	icrd, _ := r.u(5)
	cpbLen, _ := r.u(5)
	dpbLen, _ := r.u(5)
	if _, err := r.u(5); err != nil {
		return p, false, err
	}
	p.ICRDLength, p.CPBDelayLength, p.DPBDelayLength, p.CPBCount = int(icrd)+1, int(cpbLen)+1, int(dpbLen)+1, int(cnt)+1
	vcl, err := r.flag()
	if err != nil {
		return p, false, err
	}
	p.VCLHRD = vcl
	if p.NumUnitsInTick <= 0 || p.TimeScale <= 0 || p.BitRate <= 0 || p.CPBSize <= 0 {
		return p, false, errors.New("hrd parameters invalid")
	}
	return p, true, nil
}

// parseSEIHRD extracts BP ICRD (first NAL scheduler) and PT cpb_removal_delay.
func parseSEIHRD(nal []byte, p HRDParams, u *HRDUnit) error {
	b := rbsp(nal[1:])
	for off := 0; off < len(b) && !(len(b)-off == 1 && b[off] == 0x80); {
		typ, size := 0, 0
		for off < len(b) && b[off] == 0xff {
			typ += 255
			off++
		}
		if off >= len(b) {
			return errBits
		}
		typ += int(b[off])
		off++
		for off < len(b) && b[off] == 0xff {
			size += 255
			off++
		}
		if off >= len(b) {
			return errBits
		}
		size += int(b[off])
		off++
		if off+size > len(b) {
			return errBits
		}
		r := &bitReader{b: b[off : off+size]}
		switch typ {
		case 0: // buffering period
			if _, err := r.ue(); err != nil {
				return err
			}
			v, err := r.u(p.ICRDLength)
			if err != nil {
				return err
			}
			u.BP, u.ICRD = true, int64(v)
		case 1: // picture timing (CpbDpbDelaysPresentFlag holds: NAL HRD present)
			v, err := r.u(p.CPBDelayLength)
			if err != nil {
				return err
			}
			if _, err := r.u(p.DPBDelayLength); err != nil {
				return err
			}
			u.HasPT, u.CPBDelay = true, int64(v)
		}
		off += size
	}
	return nil
}

// ReadHRDTrack reads the HRD facts of the last tailUnits (tail=true) or the
// first headUnits access units of an MP4. ok is false when the stream carries
// no NAL HRD (then there is no evidence, never an error).
func ReadHRDTrack(path string, tail bool, n int) (HRDTrack, bool, error) {
	v, err := openMP4Video(path)
	if err != nil {
		return HRDTrack{}, false, err
	}
	defer v.Close()
	var t HRDTrack
	var spsSeen bool
	setSPS := func(nal []byte) (bool, error) {
		p, ok, err := parseSPSHRD(nal)
		if err != nil || !ok {
			return false, err
		}
		if spsSeen && p != t.Params {
			return false, errors.New("hrd parameters change inside the clip")
		}
		t.Params, spsSeen = p, true
		return true, nil
	}
	for _, ps := range v.paramSets {
		if len(ps) > 0 && ps[0]&0x1f == 7 {
			if ok, err := setSPS(ps); err != nil || !ok {
				return HRDTrack{}, false, err
			}
		}
	}
	total := len(v.sizes)
	from, to := 0, min(n, total)
	if tail {
		from, to = max(total-n, 0), total
	}
	for i := from; i < to; i++ {
		s, err := v.sample(i)
		if err != nil {
			return HRDTrack{}, false, err
		}
		nals, err := v.nalUnits(s)
		if err != nil {
			return HRDTrack{}, false, err
		}
		u := HRDUnit{Bytes: v.sizes[i]}
		var seis [][]byte
		for _, nal := range nals {
			switch nal[0] & 0x1f {
			case 5:
				u.Key = true
			case 7:
				if ok, err := setSPS(nal); err != nil || !ok {
					return HRDTrack{}, false, err
				}
			case 6:
				seis = append(seis, nal)
			}
		}
		if !spsSeen {
			return HRDTrack{}, false, nil
		}
		for _, sei := range seis {
			if err := parseSEIHRD(sei, t.Params, &u); err != nil {
				return HRDTrack{}, false, fmt.Errorf("sample %d sei: %w", i, err)
			}
		}
		t.Units = append(t.Units, u)
	}
	return t, spsSeen, nil
}

const (
	SourceAdjacent    = "adjacent"
	SourceContradicts = "contradicts"
	SourceUnavailable = "unavailable"
)

// SourceEvidence is the independent source-time verdict for one seam.
type SourceEvidence struct {
	Channel string `json:"channel"`
	Status  string `json:"status"`
	Reason  string `json:"reason,omitempty"`
	// Calibration: BP->BP transitions inside the previous clip's tail that
	// fit the identity, their median residual (the stream constant) and spread.
	Transitions    int     `json:"calibration_transitions,omitempty"`
	ConstantTicks  float64 `json:"stream_constant_ticks,omitempty"`
	SpreadTicks    float64 `json:"calibration_spread_ticks,omitempty"`
	SeamResidual   float64 `json:"seam_residual_ticks,omitempty"`
	CPBStep        int64   `json:"cpb_step,omitempty"`
	PrevLastCPB    int64   `json:"prev_last_cpb,omitempty"`
	NextFirstCPB   int64   `json:"next_first_cpb,omitempty"`
	PrevLastGOPAUs int     `json:"prev_last_gop_units,omitempty"`
}

// HRDPolicy holds the thresholds of the HRD continuity proof.
type HRDPolicy struct {
	// TailUnits: access units read from the previous clip's end.
	TailUnits int `json:"tail_units"`
	// MinTransitions consistent BP->BP transitions must calibrate the stream.
	MinTransitions int `json:"min_transitions"`
	// The calibration residuals must agree within SpreadTicks (90 kHz)...
	SpreadTicks float64 `json:"spread_ticks"`
	// ...and the seam's residual must equal the stream constant within
	// AdjacentTicks to prove adjacency.
	AdjacentTicks float64 `json:"adjacent_ticks"`
	// A seam residual beyond ContradictTicks on a calibrated, unsaturated
	// stream proves a discontinuity (split even if pixels look continuous).
	ContradictTicks float64 `json:"contradict_ticks"`
}

func DefaultHRDPolicy() HRDPolicy {
	return HRDPolicy{TailUnits: 2400, MinTransitions: 3, SpreadTicks: 2, AdjacentTicks: 2, ContradictTicks: 50}
}

type bpRef struct {
	idx  int
	icrd float64
	cpb  int64
}

func bps(units []HRDUnit) []bpRef {
	var out []bpRef
	for i, u := range units {
		if u.BP && u.HasPT {
			out = append(out, bpRef{i, float64(u.ICRD), u.CPBDelay})
		}
	}
	return out
}

// EvaluateHRDSeam decides whether next's first access unit is the encoder's
// next buffering period after prev's last one.
func EvaluateHRDSeam(pol HRDPolicy, prev, next HRDTrack) SourceEvidence {
	ev := SourceEvidence{Channel: "h264_hrd", Status: SourceUnavailable}
	p := prev.Params
	if p != next.Params {
		ev.Reason = "hrd_params_differ"
		return ev
	}
	if len(next.Units) == 0 || !next.Units[0].BP || !next.Units[0].HasPT || !next.Units[0].Key {
		ev.Reason = "next_not_buffering_period_keyframe"
		return ev
	}
	b := bps(prev.Units)
	if len(b) < pol.MinTransitions+1 {
		ev.Reason = "too_few_buffering_periods"
		return ev
	}
	last := prev.Units[len(prev.Units)-1]
	if !last.HasPT {
		ev.Reason = "prev_last_unit_has_no_picture_timing"
		return ev
	}
	tick, rate := p.Tick(), float64(p.BitRate)
	bitsBetween := func(from, to int) float64 {
		var n int64
		for _, u := range prev.Units[from:to] {
			n += u.Bytes
		}
		return 8 * float64(n)
	}
	residual := func(icrd0 float64, cpb1 int64, icrd1, bits float64) float64 {
		return icrd1 - (icrd0 + 90000*(float64(cpb1)*tick-bits/rate))
	}
	// The PT clock step between consecutive pictures inside a GOP.
	var steps []int64
	for i := b[0].idx + 1; i < len(prev.Units); i++ {
		u, q := prev.Units[i-1], prev.Units[i]
		switch {
		case q.BP || !u.HasPT || !q.HasPT:
		case u.BP:
			// A BP picture is the zero of the following pictures' delays.
			steps = append(steps, q.CPBDelay)
		default:
			steps = append(steps, q.CPBDelay-u.CPBDelay)
		}
	}
	if len(steps) == 0 {
		ev.Reason = "no_picture_timing_steps"
		return ev
	}
	step := steps[0]
	for _, s := range steps {
		if s != step || s <= 0 {
			ev.Reason = "irregular_picture_timing"
			return ev
		}
	}
	ev.CPBStep, ev.PrevLastCPB, ev.NextFirstCPB = step, last.CPBDelay, next.Units[0].CPBDelay
	ev.PrevLastGOPAUs = len(prev.Units) - b[len(b)-1].idx
	clamp := p.ClampTicks()
	// A standard encoder keeps ICRD below a full buffer; when one does, any
	// state at or above that level is saturated and carries no information.
	clampAware := true
	for _, x := range b {
		if x.icrd > clamp+1 {
			clampAware = false
		}
	}
	saturated := func(v float64) bool { return clampAware && v >= clamp-2 }
	var res []float64
	use := b
	if len(use) > 9 {
		use = use[len(use)-9:]
	}
	for k := 0; k+1 < len(use); k++ {
		if saturated(use[k].icrd) || saturated(use[k+1].icrd) {
			continue
		}
		r := residual(use[k].icrd, use[k+1].cpb, use[k+1].icrd, bitsBetween(use[k].idx, use[k+1].idx))
		if clampAware && use[k+1].icrd-r >= clamp-2 {
			continue // predicted state saturated
		}
		res = append(res, r)
	}
	if len(res) < pol.MinTransitions {
		ev.Reason = "calibration_saturated_or_short"
		ev.Transitions = len(res)
		return ev
	}
	sorted := append([]float64(nil), res...)
	sort.Float64s(sorted)
	c := sorted[len(sorted)/2]
	ev.Transitions, ev.ConstantTicks, ev.SpreadTicks = len(res), round4(c), round4(sorted[len(sorted)-1]-sorted[0])
	if ev.SpreadTicks > pol.SpreadTicks {
		ev.Reason = "calibration_inconsistent"
		return ev
	}
	lb := b[len(b)-1]
	n0 := next.Units[0]
	r := residual(lb.icrd, n0.CPBDelay, float64(n0.ICRD), bitsBetween(lb.idx, len(prev.Units)))
	ev.SeamResidual = round4(r)
	predicted := float64(n0.ICRD) - r + c
	if saturated(lb.icrd) || saturated(float64(n0.ICRD)) || saturated(predicted) {
		ev.Reason = "seam_state_saturated"
		return ev
	}
	latticeOK := n0.CPBDelay == last.CPBDelay+step
	switch d := math.Abs(r - c); {
	case d <= pol.AdjacentTicks && latticeOK:
		ev.Status = SourceAdjacent
	case d > pol.ContradictTicks || !latticeOK:
		ev.Status = SourceContradicts
		if !latticeOK {
			ev.Reason = "picture_timing_lattice_broken"
		} else {
			ev.Reason = "buffer_state_discontinuous"
		}
	default:
		ev.Reason = "seam_residual_ambiguous"
	}
	return ev
}

// GOPEvidence is the keyframe-lattice check at a seam. The capture segmenter
// only ever cuts in front of a keyframe, so a previous clip that ends inside a
// GOP was cut by something else (a killed or restarted capture): the rest of
// that GOP is missing, or the next clip replays it. When the stream has a
// fixed GOP length, a previous clip whose final GOP is not exactly that long
// is therefore a discontinuity, whatever the pixels or timestamps say.
type GOPEvidence struct {
	Status      string `json:"status"` // complete | truncated | irregular
	GOPLength   int    `json:"gop_length,omitempty"`
	PrevLastGOP int    `json:"prev_last_gop"`
	Observed    int    `json:"complete_gops_observed"`
}

const (
	GOPComplete  = "complete"
	GOPTruncated = "truncated"
	GOPIrregular = "irregular"
)

// gopLengths returns the lengths (in packets, decode order) of the complete
// GOPs of a track and the length of its final, open GOP.
func gopLengths(s *StreamPackets) (complete []int, last int) {
	prev := -1
	for i, pk := range s.Packets {
		if !pk.Key {
			continue
		}
		if prev >= 0 {
			complete = append(complete, i-prev)
		}
		prev = i
	}
	if prev < 0 {
		return nil, 0
	}
	return complete, len(s.Packets) - prev
}

// EvaluateGOPLattice checks the previous clip's final GOP against the GOP
// length shared by every complete GOP observed near the seam (the last
// `window` of the previous clip and the first `window` of the next).
func EvaluateGOPLattice(prev, next *StreamPackets, window, minObserved int) GOPEvidence {
	if prev == nil || next == nil {
		return GOPEvidence{Status: GOPIrregular}
	}
	pc, last := gopLengths(prev)
	nc, _ := gopLengths(next)
	return EvaluateGOPLengths(pc, last, nc, window, minObserved)
}

// EvaluateGOPLengths is EvaluateGOPLattice on GOP lengths: the previous clip's
// complete GOPs and final GOP, and the next clip's complete GOPs.
func EvaluateGOPLengths(prevComplete []int, prevLast int, nextComplete []int, window, minObserved int) GOPEvidence {
	ev := GOPEvidence{Status: GOPIrregular, PrevLastGOP: prevLast}
	pc, nc := prevComplete, nextComplete
	if len(pc) > window {
		pc = pc[len(pc)-window:]
	}
	if len(nc) > window {
		nc = nc[:window]
	}
	all := append(append([]int(nil), pc...), nc...)
	ev.Observed = len(all)
	if len(all) < minObserved || prevLast <= 0 {
		return ev
	}
	for _, n := range all {
		if n != all[0] {
			return ev
		}
	}
	ev.GOPLength = all[0]
	if prevLast == all[0] {
		ev.Status = GOPComplete
	} else {
		ev.Status = GOPTruncated
	}
	return ev
}
