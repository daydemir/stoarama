package collation

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func box(typ string, payload ...[]byte) []byte {
	n := 8
	for _, p := range payload {
		n += len(p)
	}
	b := make([]byte, 8, n)
	binary.BigEndian.PutUint32(b, uint32(n))
	copy(b[4:], typ)
	for _, p := range payload {
		b = append(b, p...)
	}
	return b
}

func u32s(v ...uint32) []byte {
	b := make([]byte, 4*len(v))
	for i, x := range v {
		binary.BigEndian.PutUint32(b[4*i:], x)
	}
	return b
}

// A fixed-size stsz may not claim more samples than any clip can hold.
func TestMP4RejectsHugeFixedSampleCount(t *testing.T) {
	entry := append(make([]byte, 78), box("avcC", []byte{1, 100, 0, 30, 0xff, 0xe0, 0})...)
	stsd := box("stsd", u32s(0, 1), box("avc1", entry))
	stbl := box("stbl", stsd, box("stsz", u32s(0, 4, 0xfffffff0)), box("stsc", u32s(0, 1, 1, 1, 1)), box("stco", u32s(0, 1, 0)))
	hdlr := box("hdlr", u32s(0, 0), []byte("vide"), make([]byte, 12))
	moov := box("moov", box("trak", box("mdia", hdlr, box("minf", stbl))))
	path := filepath.Join(t.TempDir(), "huge.mp4")
	if err := os.WriteFile(path, append(box("ftyp", []byte("isom")), moov...), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := openMP4Video(path); err == nil || !strings.Contains(err.Error(), "sample count") {
		t.Fatalf("huge stsz count: %v", err)
	}
}
