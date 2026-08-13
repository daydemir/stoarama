package presentationprobe

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
)

const configVersion uint16 = 1

type FileIdentity struct {
	Method        string
	Device        string
	Inode         string
	CloneIdentity string
}

type StreamExpectation struct {
	Index       int32
	CodecType   string
	CodecName   string
	TimeBaseNum int64
	TimeBaseDen int64
}

type Limits struct {
	InputBytes       int64
	ReadBytes        int64
	Seeks            uint64
	Units            uint64
	StdoutBytes      int64
	StderrBytes      int64
	WallMilliseconds uint64
}

func HardLimits() Limits {
	return Limits{MaximumInputBytes, MaximumReadBytes, MaximumSeeks, MaximumUnits,
		MaximumStdoutBytes, MaximumStderrBytes, MaximumWallMilliseconds}
}

func (l Limits) Validate() error {
	h := HardLimits()
	if l.InputBytes <= 0 || l.InputBytes > h.InputBytes || l.ReadBytes <= 0 || l.ReadBytes > h.ReadBytes ||
		l.Seeks == 0 || l.Seeks > h.Seeks || l.Units == 0 || l.Units > h.Units ||
		l.StdoutBytes <= 0 || l.StdoutBytes > h.StdoutBytes || l.StderrBytes <= 0 || l.StderrBytes > h.StderrBytes ||
		l.WallMilliseconds == 0 || l.WallMilliseconds > h.WallMilliseconds {
		return errors.New("requested limit is zero or exceeds a compiled hard limit")
	}
	return nil
}

type Config struct {
	InvocationID         [32]byte
	InputSize            int64
	InputSHA256          string
	Identity             FileIdentity
	ParserSchema         string
	ExpectedToolSHA256   string
	ExpectedNativeSHA256 string
	Video                StreamExpectation
	Audio                *StreamExpectation
	Limits               Limits
}

func (c Config) Validate() error {
	if c.InvocationID == ([32]byte{}) {
		return errors.New("invocation ID is zero")
	}
	if c.InputSize <= 0 || c.InputSize > MaximumInputBytes || c.Limits.InputBytes < c.InputSize {
		return errors.New("input size is outside the configured bound")
	}
	if !ValidSHA256(c.InputSHA256) || !ValidSHA256(c.ExpectedToolSHA256) || !ValidSHA256(c.ExpectedNativeSHA256) {
		return errors.New("config SHA field is invalid")
	}
	if c.ParserSchema != ParserSchema {
		return errors.New("parser schema mismatch")
	}
	if err := c.Limits.Validate(); err != nil {
		return err
	}
	if err := validateFileIdentity(c.Identity); err != nil {
		return err
	}
	if err := validateStreamExpectation(c.Video, "video"); err != nil {
		return err
	}
	if c.Video.CodecType != "video" {
		return errors.New("selected video stream is not video")
	}
	if c.Audio != nil {
		if err := validateStreamExpectation(*c.Audio, "audio"); err != nil {
			return err
		}
		if c.Audio.CodecType != "audio" || c.Audio.Index == c.Video.Index {
			return errors.New("selected audio stream is invalid")
		}
	}
	return nil
}

func validateFileIdentity(v FileIdentity) error {
	switch v.Method {
	case "inode":
		if !canonicalPositiveUint64(v.Device) || !canonicalPositiveUint64(v.Inode) || v.CloneIdentity != "" {
			return errors.New("invalid inode identity")
		}
	case "sealed_memfd":
		if v.Device != "" || v.Inode != "" || v.CloneIdentity == "" || !ValidSHA256(v.CloneIdentity) {
			return errors.New("invalid sealed memfd identity")
		}
	default:
		return errors.New("unsupported file identity method")
	}
	return nil
}

func canonicalPositiveUint64(value string) bool {
	if value == "" || len(value) > 20 || (len(value) > 1 && value[0] == '0') {
		return false
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	return err == nil && parsed > 0 && strconv.FormatUint(parsed, 10) == value
}

func validateStreamExpectation(v StreamExpectation, name string) error {
	if v.Index < 0 || v.TimeBaseNum == 0 || v.TimeBaseDen <= 0 {
		return fmt.Errorf("invalid %s stream numeric fact", name)
	}
	if err := (Rational{Num: v.TimeBaseNum, Den: v.TimeBaseDen}).Validate(); err != nil {
		return fmt.Errorf("invalid %s stream timebase: %w", name, err)
	}
	if err := ValidateASCII(v.CodecType, name+" codec type", 16); err != nil {
		return err
	}
	return ValidateASCII(v.CodecName, name+" codec name", 64)
}

func EncodeConfig(c Config) ([]byte, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	var body bytes.Buffer
	writeString := func(value string) error {
		length, err := checkedUint32Length(len(value), 1<<20)
		if err != nil {
			return err
		}
		if err := binary.Write(&body, binary.BigEndian, length); err != nil {
			return err
		}
		_, err = body.WriteString(value)
		return err
	}
	_, _ = body.Write(c.InvocationID[:])
	_ = binary.Write(&body, binary.BigEndian, c.InputSize)
	for _, value := range []string{c.InputSHA256, c.Identity.Method, c.Identity.Device, c.Identity.Inode,
		c.Identity.CloneIdentity, c.ParserSchema, c.ExpectedToolSHA256, c.ExpectedNativeSHA256} {
		if err := writeString(value); err != nil {
			return nil, err
		}
	}
	writeStream := func(s StreamExpectation) error {
		if err := binary.Write(&body, binary.BigEndian, s.Index); err != nil {
			return err
		}
		if err := writeString(s.CodecType); err != nil {
			return err
		}
		if err := writeString(s.CodecName); err != nil {
			return err
		}
		if err := binary.Write(&body, binary.BigEndian, s.TimeBaseNum); err != nil {
			return err
		}
		return binary.Write(&body, binary.BigEndian, s.TimeBaseDen)
	}
	if err := writeStream(c.Video); err != nil {
		return nil, err
	}
	if c.Audio == nil {
		_ = body.WriteByte(0)
	} else {
		_ = body.WriteByte(1)
		if err := writeStream(*c.Audio); err != nil {
			return nil, err
		}
	}
	for _, v := range []any{c.Limits.InputBytes, c.Limits.ReadBytes, c.Limits.Seeks, c.Limits.Units,
		c.Limits.StdoutBytes, c.Limits.StderrBytes, c.Limits.WallMilliseconds} {
		if err := binary.Write(&body, binary.BigEndian, v); err != nil {
			return nil, err
		}
	}
	var out bytes.Buffer
	_, _ = out.WriteString(ConfigDomain)
	_ = out.WriteByte(0)
	_ = binary.Write(&out, binary.BigEndian, configVersion)
	bodyLength, err := checkedUint32Length(body.Len(), math.MaxUint32)
	if err != nil {
		return nil, err
	}
	_ = binary.Write(&out, binary.BigEndian, bodyLength)
	_, _ = out.Write(body.Bytes())
	return out.Bytes(), nil
}

func DecodeConfig(data []byte) (Config, error) {
	var c Config
	r := bytes.NewReader(data)
	domain := make([]byte, len(ConfigDomain)+1)
	if _, err := io.ReadFull(r, domain); err != nil || string(domain) != ConfigDomain+"\x00" {
		return c, errors.New("config domain mismatch")
	}
	var version uint16
	var length uint32
	if err := binary.Read(r, binary.BigEndian, &version); err != nil || version != configVersion {
		return c, errors.New("config version or length mismatch")
	}
	if err := binary.Read(r, binary.BigEndian, &length); err != nil {
		return c, errors.New("config version or length mismatch")
	}
	remaining := r.Len()
	if remaining < 0 || uint64(length) != uint64(remaining) {
		return c, errors.New("config version or length mismatch")
	}
	readString := func() (string, error) {
		var n uint32
		if err := binary.Read(r, binary.BigEndian, &n); err != nil {
			return "", errors.New("invalid config string length")
		}
		remaining := r.Len()
		if n > 1<<20 || remaining < 0 || uint64(n) > uint64(remaining) {
			return "", errors.New("invalid config string length")
		}
		b := make([]byte, n)
		_, err := io.ReadFull(r, b)
		return string(b), err
	}
	if _, err := io.ReadFull(r, c.InvocationID[:]); err != nil {
		return c, err
	}
	if err := binary.Read(r, binary.BigEndian, &c.InputSize); err != nil {
		return c, err
	}
	values := []*string{&c.InputSHA256, &c.Identity.Method, &c.Identity.Device, &c.Identity.Inode,
		&c.Identity.CloneIdentity, &c.ParserSchema, &c.ExpectedToolSHA256, &c.ExpectedNativeSHA256}
	for _, target := range values {
		value, err := readString()
		if err != nil {
			return c, err
		}
		*target = value
	}
	readStream := func(s *StreamExpectation) error {
		if err := binary.Read(r, binary.BigEndian, &s.Index); err != nil {
			return err
		}
		var err error
		if s.CodecType, err = readString(); err != nil {
			return err
		}
		if s.CodecName, err = readString(); err != nil {
			return err
		}
		if err = binary.Read(r, binary.BigEndian, &s.TimeBaseNum); err != nil {
			return err
		}
		return binary.Read(r, binary.BigEndian, &s.TimeBaseDen)
	}
	if err := readStream(&c.Video); err != nil {
		return c, err
	}
	present, err := r.ReadByte()
	if err != nil || present > 1 {
		return c, errors.New("invalid audio presence byte")
	}
	if present == 1 {
		c.Audio = &StreamExpectation{}
		if err := readStream(c.Audio); err != nil {
			return c, err
		}
	}
	for _, v := range []any{&c.Limits.InputBytes, &c.Limits.ReadBytes, &c.Limits.Seeks, &c.Limits.Units,
		&c.Limits.StdoutBytes, &c.Limits.StderrBytes, &c.Limits.WallMilliseconds} {
		if err := binary.Read(r, binary.BigEndian, v); err != nil {
			return c, err
		}
	}
	if r.Len() != 0 {
		return c, errors.New("trailing config bytes")
	}
	return c, c.Validate()
}

func checkedUint32Length(value int, maximum uint64) (uint32, error) {
	if value < 0 || uint64(value) > maximum || uint64(value) > math.MaxUint32 {
		return 0, errors.New("config length exceeds uint32 bound")
	}
	return uint32(value), nil
}
