// Package clockjoin is the clock-based second stitching pass over collation-v2
// hours. Where collation-v2 split two adjacent parts on visual or
// low-confidence evidence, the burned-in seconds clock decides: the parts are
// joined by stream copy (audio kept) only when the clock runs on across the
// seam with a 0-1 s gap and each part's verified frame count matches its
// clock span within 20%.
package clockjoin

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/daydemir/stoarama/backend/internal/collation"
)

// Channel is the image plane the clock is read from. Luma suits white or
// black text; a colour plane separates coloured text from a bright sky (435's
// orange clock is dark in the blue plane, the sky light).
type Channel string

const (
	Luma  Channel = "luma"
	Red   Channel = "red"
	Green Channel = "green"
	Blue  Channel = "blue"
)

var channelPlane = map[Channel]string{Red: "r", Green: "g", Blue: "b"}

// Profile locates one recording's burned-in clock: a crop box as fractions of
// the frame, the plane to read and whether the text is light on dark.
type Profile struct {
	RecordingID int64   `json:"recording_id"`
	X           float64 `json:"x"`
	Y           float64 `json:"y"`
	W           float64 `json:"w"`
	H           float64 `json:"h"`
	Channel     Channel `json:"channel"`
	Negate      bool    `json:"negate"`
}

func (p Profile) Validate() error {
	if p.RecordingID <= 0 || p.X < 0 || p.Y < 0 || p.W <= 0 || p.H <= 0 || p.X+p.W > 1 || p.Y+p.H > 1 {
		return fmt.Errorf("clock profile for recording %d is not a crop inside the frame", p.RecordingID)
	}
	if _, ok := channelPlane[p.Channel]; !ok && p.Channel != Luma {
		return fmt.Errorf("clock profile for recording %d: channel must be luma, red, green or blue", p.RecordingID)
	}
	return nil
}

// filter crops the clock, upscales it 3x and reduces it to one gray plane
// (inverted when Negate is set), which is what tesseract reads best.
func (p Profile) filter() string {
	f := fmt.Sprintf("crop=w=trunc(iw*%.6f/2)*2:h=trunc(ih*%.6f/2)*2:x=trunc(iw*%.6f):y=trunc(ih*%.6f),scale=iw*3:ih*3:flags=bicubic",
		p.W, p.H, p.X, p.Y)
	if plane, ok := channelPlane[p.Channel]; ok {
		f += ",format=gbrp,extractplanes=" + plane
	} else {
		f += ",format=gray"
	}
	if p.Negate {
		f += ",negate"
	}
	return f
}

// Edge is a part's first (Head) or last (Tail) frame.
type Edge string

const (
	Head Edge = "head"
	Tail Edge = "tail"
)

// ReadStatus is the outcome of reading one edge.
type ReadStatus string

const (
	ReadOK           ReadStatus = "ok"
	ReadUnreadable   ReadStatus = "unreadable"
	ReadInconsistent ReadStatus = "inconsistent"
)

// EdgeRead is the clock at a part's edge frame, cross-checked against the
// frame one second inward: the two reads must differ by 0-2 s in the right
// direction, which rejects a misread digit.
type EdgeRead struct {
	Edge   Edge       `json:"edge"`
	Raw    [2]string  `json:"raw"` // edge frame, inner frame
	Clock  int        `json:"clock_seconds_of_day"`
	Status ReadStatus `json:"status"`
}

const day = 24 * 3600

var clockText = regexp.MustCompile(`([0-9]{2}):([0-9]{2}):([0-9]{2})`)

// ParseClock returns the seconds of day of the last HH:MM:SS in text.
func ParseClock(text string) (int, bool) {
	all := clockText.FindAllStringSubmatch(text, -1)
	if len(all) == 0 {
		return 0, false
	}
	m := all[len(all)-1]
	h, _ := strconv.Atoi(m[1])
	mi, _ := strconv.Atoi(m[2])
	s, _ := strconv.Atoi(m[3])
	if h > 23 || mi > 59 || s > 59 {
		return 0, false
	}
	return h*3600 + mi*60 + s, true
}

// ClockDiff is b-a on a 24 h clock, in (-12 h, 12 h].
func ClockDiff(a, b int) int {
	d := ((b-a)%day + day) % day
	if d > day/2 {
		d -= day
	}
	return d
}

// EvaluateEdge turns the two raw reads of an edge into an EdgeRead.
func EvaluateEdge(e Edge, raw [2]string) EdgeRead {
	r := EdgeRead{Edge: e, Raw: raw, Status: ReadUnreadable}
	edge, ok1 := ParseClock(raw[0])
	inner, ok2 := ParseClock(raw[1])
	if !ok1 || !ok2 {
		return r
	}
	// The inner frame is one second after the head, or before the tail.
	step := ClockDiff(edge, inner)
	if e == Tail {
		step = -step
	}
	if step < 0 || step > 2 {
		r.Status = ReadInconsistent
		return r
	}
	r.Clock, r.Status = edge, ReadOK
	return r
}

// Reader OCRs clock crops with ffmpeg and tesseract, on CPU.
type Reader struct {
	Tools     collation.Tools
	Tesseract string
	Scratch   string
}

// ReadEdge reads the clock at path's head or tail.
func (r Reader) ReadEdge(ctx context.Context, path string, p Profile, e Edge) (EdgeRead, error) {
	// Head: the first frame, and the first frame from 1 s in. Tail: the last
	// frame (written over until the end) and the first frame of the last second.
	var edgeArgs, innerArgs []string
	switch e {
	case Head:
		edgeArgs = []string{"-i", path, "-frames:v", "1"}
		innerArgs = []string{"-ss", "1", "-i", path, "-frames:v", "1"}
	case Tail:
		edgeArgs = []string{"-sseof", "-1", "-i", path, "-update", "1"}
		innerArgs = []string{"-sseof", "-1", "-i", path, "-frames:v", "1"}
	default:
		return EdgeRead{}, fmt.Errorf("unknown edge %q", e)
	}
	var raw [2]string
	for i, args := range [][]string{edgeArgs, innerArgs} {
		text, err := r.ocrFrame(ctx, args, p)
		if err != nil {
			return EdgeRead{}, err
		}
		raw[i] = text
	}
	return EvaluateEdge(e, raw), nil
}

func (r Reader) ocrFrame(ctx context.Context, input []string, p Profile) (string, error) {
	dir, err := os.MkdirTemp(r.Scratch, "clock-")
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(dir)
	png := filepath.Join(dir, "clock.png")
	args := append([]string{"-nostdin", "-v", "error"}, input...)
	args = append(args, "-an", "-vf", p.filter(), "-y", png)
	var stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, r.Tools.FFmpeg, args...)
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("clock frame: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	if _, err := os.Stat(png); os.IsNotExist(err) {
		return "", nil // no frame there (a part shorter than the read window): unreadable
	}
	out, err := exec.CommandContext(ctx, r.Tesseract, png, "stdout", "--psm", "7", "-c", "tessedit_char_whitelist=0123456789:").Output()
	if err != nil {
		return "", fmt.Errorf("tesseract: %w", err)
	}
	return strings.TrimSpace(string(out)), nil
}

// Version is the first line of `tesseract --version`, for the proof.
func (r Reader) Version(ctx context.Context) (string, error) {
	out, err := exec.CommandContext(ctx, r.Tesseract, "--version").CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("tesseract --version: %w", err)
	}
	line, _, _ := strings.Cut(string(out), "\n")
	return strings.TrimSpace(line), nil
}
