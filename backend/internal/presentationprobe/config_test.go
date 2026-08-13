package presentationprobe

import (
	"bytes"
	"testing"
)

func validConfig() Config {
	var invocation [32]byte
	copy(invocation[:], bytes.Repeat([]byte{0x42}, 32))
	return Config{
		InvocationID: invocation, InputSize: 4096, InputSHA256: SHA256([]byte("input")),
		Identity:     FileIdentity{Method: "inode", Device: "16777234", Inode: "9007199254740993"},
		ParserSchema: ParserSchema, ExpectedToolSHA256: SHA256([]byte("tool")), ExpectedNativeSHA256: SHA256([]byte("native")),
		Video: StreamExpectation{Index: 0, CodecType: "video", CodecName: "h264", TimeBaseNum: 1, TimeBaseDen: 90000},
		Audio: &StreamExpectation{Index: 1, CodecType: "audio", CodecName: "aac", TimeBaseNum: 1, TimeBaseDen: 48000}, Limits: HardLimits(),
	}
}

func TestConfigGoldenAndStrictRoundTrip(t *testing.T) {
	c := validConfig()
	raw, err := EncodeConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	const want = "0e2d4a7dff2c4ef3dcecc8caaad08ef1bdb9eb27ed89305c09bd0f05244ea897"
	if got := SHA256(raw); got != want {
		t.Fatalf("config golden changed: got %s want %s", got, want)
	}
	decoded, err := DecodeConfig(raw)
	if err != nil {
		t.Fatal(err)
	}
	again, err := EncodeConfig(decoded)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw, again) {
		t.Fatal("config round trip changed bytes")
	}
	for _, suffix := range [][]byte{{0}, {1, 2, 3}} {
		if _, err := DecodeConfig(append(append([]byte(nil), raw...), suffix...)); err == nil {
			t.Fatal("trailing config bytes accepted")
		}
	}
}
func TestConfigRejectsLimitAndSelectionSubstitution(t *testing.T) {
	tests := []func(*Config){func(c *Config) { c.Limits.Units = MaximumUnits + 1 }, func(c *Config) { c.InputSize = c.Limits.InputBytes + 1 }, func(c *Config) { c.Video.CodecType = "audio" }, func(c *Config) { c.Audio.Index = c.Video.Index }, func(c *Config) { c.ExpectedToolSHA256 = "A" + c.ExpectedToolSHA256[1:] }}
	for i, mutate := range tests {
		c := validConfig()
		mutate(&c)
		if _, err := EncodeConfig(c); err == nil {
			t.Fatalf("mutation %d accepted", i)
		}
	}
}

func TestConfigRejectsNoncanonicalFileIdentity(t *testing.T) {
	values := []string{"", "0", "01", "-1", "+1", " 1", "1 ", "18446744073709551616", "123456789012345678901", "１２", "1\n"}
	for _, value := range values {
		t.Run(value, func(t *testing.T) {
			for _, field := range []string{"device", "inode"} {
				config := validConfig()
				if field == "device" {
					config.Identity.Device = value
				} else {
					config.Identity.Inode = value
				}
				if _, err := EncodeConfig(config); err == nil {
					t.Fatalf("%s accepted noncanonical value %q", field, value)
				}
			}
		})
	}
}
