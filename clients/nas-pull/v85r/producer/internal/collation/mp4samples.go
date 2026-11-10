package collation

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

// mp4Video is the sample table of the first H.264 video track of a
// non-fragmented MP4: exactly the stored access units, in decode order, with
// their byte offsets and sizes. It reads no media payload by itself.
type mp4Video struct {
	f          *os.File
	lengthSize int      // NAL length prefix size from avcC (1, 2 or 4)
	paramSets  [][]byte // SPS/PPS NAL units carried in avcC
	offsets    []int64
	sizes      []int64
}

var errNoAVCTrack = errors.New("no avc1/avc3 video track")

const maxMP4Samples = 1 << 22

type mp4Box struct {
	typ         string
	start, data int64 // box start and payload start
	end         int64
}

func readBoxes(r io.ReaderAt, from, to int64) ([]mp4Box, error) {
	var out []mp4Box
	for off := from; off+8 <= to; {
		var h [16]byte
		if _, err := r.ReadAt(h[:8], off); err != nil {
			return nil, err
		}
		size := int64(binary.BigEndian.Uint32(h[:4]))
		typ := string(h[4:8])
		data := off + 8
		switch size {
		case 1:
			if _, err := r.ReadAt(h[8:16], off+8); err != nil {
				return nil, err
			}
			size = int64(binary.BigEndian.Uint64(h[8:16]))
			data = off + 16
		case 0:
			size = to - off
		}
		if size < data-off || off+size > to {
			return nil, fmt.Errorf("mp4 box %q overruns its parent", typ)
		}
		out = append(out, mp4Box{typ: typ, start: off, data: data, end: off + size})
		off += size
	}
	return out, nil
}

func child(r io.ReaderAt, b mp4Box, typ string) (mp4Box, error) {
	kids, err := readBoxes(r, b.data, b.end)
	if err != nil {
		return mp4Box{}, err
	}
	for _, k := range kids {
		if k.typ == typ {
			return k, nil
		}
	}
	return mp4Box{}, fmt.Errorf("mp4 box %q has no %q", b.typ, typ)
}

func payload(r io.ReaderAt, b mp4Box, max int64) ([]byte, error) {
	n := b.end - b.data
	if n < 0 || n > max {
		return nil, fmt.Errorf("mp4 box %q payload size %d out of range", b.typ, n)
	}
	buf := make([]byte, n)
	_, err := r.ReadAt(buf, b.data)
	return buf, err
}

// openMP4Video parses the sample table of the first AVC video track.
func openMP4Video(path string) (*mp4Video, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	v, err := parseMP4Video(f)
	if err != nil {
		f.Close()
		return nil, err
	}
	return v, nil
}

func (v *mp4Video) Close() error { return v.f.Close() }

func parseMP4Video(f *os.File) (*mp4Video, error) {
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	top, err := readBoxes(f, 0, st.Size())
	if err != nil {
		return nil, err
	}
	var moov *mp4Box
	for i := range top {
		switch top[i].typ {
		case "moov":
			moov = &top[i]
		case "moof":
			return nil, errors.New("fragmented mp4 is not supported")
		}
	}
	if moov == nil {
		return nil, errors.New("mp4 has no moov")
	}
	traks, err := readBoxes(f, moov.data, moov.end)
	if err != nil {
		return nil, err
	}
	for _, trak := range traks {
		if trak.typ != "trak" {
			continue
		}
		mdia, err := child(f, trak, "mdia")
		if err != nil {
			continue
		}
		hdlr, err := child(f, mdia, "hdlr")
		if err != nil {
			continue
		}
		hb, err := payload(f, hdlr, 1<<16)
		if err != nil || len(hb) < 12 || string(hb[8:12]) != "vide" {
			continue
		}
		minf, err := child(f, mdia, "minf")
		if err != nil {
			return nil, err
		}
		stbl, err := child(f, minf, "stbl")
		if err != nil {
			return nil, err
		}
		return parseSampleTable(f, stbl)
	}
	return nil, errNoAVCTrack
}

func parseSampleTable(f *os.File, stbl mp4Box) (*mp4Video, error) {
	v := &mp4Video{f: f}
	stsd, err := child(f, stbl, "stsd")
	if err != nil {
		return nil, err
	}
	sd, err := payload(f, stsd, 1<<20)
	if err != nil {
		return nil, err
	}
	// stsd: version/flags(4) entry_count(4) then the first sample entry box.
	if len(sd) < 16 {
		return nil, errors.New("stsd too short")
	}
	entrySize := int(binary.BigEndian.Uint32(sd[8:12]))
	entryType := string(sd[12:16])
	if entryType != "avc1" && entryType != "avc3" {
		return nil, errNoAVCTrack
	}
	if entrySize < 8+78 || 8+entrySize > len(sd) {
		return nil, errors.New("avc sample entry malformed")
	}
	entry := sd[8 : 8+entrySize]
	// VisualSampleEntry fields take 78 bytes after the 8-byte entry header.
	for off := 8 + 78; off+8 <= len(entry); {
		size := int(binary.BigEndian.Uint32(entry[off : off+4]))
		if size < 8 || off+size > len(entry) {
			break
		}
		if string(entry[off+4:off+8]) == "avcC" {
			if err := v.parseAVCC(entry[off+8 : off+size]); err != nil {
				return nil, err
			}
		}
		off += size
	}
	if v.lengthSize == 0 {
		return nil, errors.New("avcC missing")
	}

	stsz, err := child(f, stbl, "stsz")
	if err != nil {
		return nil, err
	}
	sz, err := payload(f, stsz, 64<<20)
	if err != nil || len(sz) < 12 {
		return nil, errors.New("stsz malformed")
	}
	fixed := int64(binary.BigEndian.Uint32(sz[4:8]))
	count := int(binary.BigEndian.Uint32(sz[8:12]))
	// A fixed sample size leaves the count unbounded by the payload; no clip
	// comes near this many samples (38 h at 30 fps).
	if count > maxMP4Samples {
		return nil, fmt.Errorf("stsz sample count %d exceeds %d", count, maxMP4Samples)
	}
	if fixed == 0 && len(sz) < 12+4*count {
		return nil, errors.New("stsz truncated")
	}
	v.sizes = make([]int64, count)
	for i := range v.sizes {
		if fixed != 0 {
			v.sizes[i] = fixed
		} else {
			v.sizes[i] = int64(binary.BigEndian.Uint32(sz[12+4*i:]))
		}
	}

	var chunkOffsets []int64
	if co, err := child(f, stbl, "stco"); err == nil {
		b, err := payload(f, co, 64<<20)
		if err != nil || len(b) < 8 {
			return nil, errors.New("stco malformed")
		}
		n := int(binary.BigEndian.Uint32(b[4:8]))
		if len(b) < 8+4*n {
			return nil, errors.New("stco truncated")
		}
		for i := 0; i < n; i++ {
			chunkOffsets = append(chunkOffsets, int64(binary.BigEndian.Uint32(b[8+4*i:])))
		}
	} else if co, err := child(f, stbl, "co64"); err == nil {
		b, err := payload(f, co, 64<<20)
		if err != nil || len(b) < 8 {
			return nil, errors.New("co64 malformed")
		}
		n := int(binary.BigEndian.Uint32(b[4:8]))
		if len(b) < 8+8*n {
			return nil, errors.New("co64 truncated")
		}
		for i := 0; i < n; i++ {
			chunkOffsets = append(chunkOffsets, int64(binary.BigEndian.Uint64(b[8+8*i:])))
		}
	} else {
		return nil, errors.New("no chunk offsets")
	}

	stscBox, err := child(f, stbl, "stsc")
	if err != nil {
		return nil, err
	}
	sc, err := payload(f, stscBox, 64<<20)
	if err != nil || len(sc) < 8 {
		return nil, errors.New("stsc malformed")
	}
	runs := int(binary.BigEndian.Uint32(sc[4:8]))
	if len(sc) < 8+12*runs || runs == 0 {
		return nil, errors.New("stsc truncated")
	}
	v.offsets = make([]int64, 0, count)
	sample := 0
	for r := 0; r < runs; r++ {
		first := int(binary.BigEndian.Uint32(sc[8+12*r:]))
		per := int(binary.BigEndian.Uint32(sc[12+12*r:]))
		last := len(chunkOffsets) + 1
		if r+1 < runs {
			last = int(binary.BigEndian.Uint32(sc[8+12*(r+1):]))
		}
		if first < 1 || last < first || last > len(chunkOffsets)+1 {
			return nil, errors.New("stsc chunk range invalid")
		}
		for c := first; c < last; c++ {
			off := chunkOffsets[c-1]
			for k := 0; k < per && sample < count; k++ {
				v.offsets = append(v.offsets, off)
				off += v.sizes[sample]
				sample++
			}
		}
	}
	if len(v.offsets) != count {
		return nil, fmt.Errorf("sample table maps %d of %d samples", len(v.offsets), count)
	}
	return v, nil
}

func (v *mp4Video) parseAVCC(b []byte) error {
	if len(b) < 7 {
		return errors.New("avcC too short")
	}
	v.lengthSize = int(b[4]&3) + 1
	if v.lengthSize == 3 {
		return errors.New("avcC length size 3 is invalid")
	}
	off := 6
	for i, n := 0, int(b[5]&0x1f); i < n; i++ {
		if off+2 > len(b) {
			return errors.New("avcC sps truncated")
		}
		l := int(binary.BigEndian.Uint16(b[off:]))
		if off+2+l > len(b) {
			return errors.New("avcC sps truncated")
		}
		v.paramSets = append(v.paramSets, b[off+2:off+2+l])
		off += 2 + l
	}
	return nil
}

// sample returns the stored bytes of access unit i (decode order).
func (v *mp4Video) sample(i int) ([]byte, error) {
	if i < 0 || i >= len(v.sizes) || v.sizes[i] > 64<<20 {
		return nil, fmt.Errorf("sample %d out of range", i)
	}
	buf := make([]byte, v.sizes[i])
	_, err := v.f.ReadAt(buf, v.offsets[i])
	return buf, err
}

// nalUnits splits one length-prefixed sample into NAL units.
func (v *mp4Video) nalUnits(sample []byte) ([][]byte, error) {
	var out [][]byte
	for off := 0; off < len(sample); {
		if off+v.lengthSize > len(sample) {
			return nil, errors.New("nal length prefix truncated")
		}
		n := 0
		for k := 0; k < v.lengthSize; k++ {
			n = n<<8 | int(sample[off+k])
		}
		off += v.lengthSize
		if n == 0 || off+n > len(sample) {
			return nil, errors.New("nal unit overruns its sample")
		}
		out = append(out, sample[off:off+n])
		off += n
	}
	return out, nil
}
