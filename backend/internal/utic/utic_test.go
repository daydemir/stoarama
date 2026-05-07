package utic

import (
	"strings"
	"testing"
)

func TestParseCameras(t *testing.T) {
	const fixture = `
<response>
  <data>
    <cctvid>UTIC-1</cctvid>
    <cctvname>Gangnam</cctvname>
    <cctvurl>https://example.test/live/1.m3u8</cctvurl>
    <coordx>127.1</coordx>
    <coordy>37.5</coordy>
    <kind>Seoul</kind>
  </data>
  <data>
    <cctvid>UTIC-2</cctvid>
    <cctvname>Mapo</cctvname>
    <cctvurl>http://example.test/live/2.m3u8</cctvurl>
  </data>
</response>`
	cameras, err := ParseCameras(strings.NewReader(fixture))
	if err != nil {
		t.Fatalf("ParseCameras: %v", err)
	}
	if len(cameras) != 2 {
		t.Fatalf("len=%d want 2", len(cameras))
	}
	if cameras[0].ID != "UTIC-1" || cameras[0].Name != "Gangnam" || cameras[0].Lon == nil || cameras[0].Lat == nil {
		t.Fatalf("unexpected first camera: %+v", cameras[0])
	}
}

func TestParseCamerasAcceptsOfficialFieldAliases(t *testing.T) {
	const fixture = `<response><item><CCTV_ID>UTIC-3</CCTV_ID><CCTV_NM>Jongno</CCTV_NM><URL>https://example.test/live/3.m3u8</URL><X>127.0</X><Y>37.6</Y></item></response>`
	cameras, err := ParseCameras(strings.NewReader(fixture))
	if err != nil {
		t.Fatalf("ParseCameras: %v", err)
	}
	if len(cameras) != 1 {
		t.Fatalf("len=%d want 1", len(cameras))
	}
	if cameras[0].ID != "UTIC-3" || cameras[0].Lon == nil || cameras[0].Lat == nil {
		t.Fatalf("unexpected camera: %+v", cameras[0])
	}
}

func TestParseCamerasRejectsMalformedCamera(t *testing.T) {
	const fixture = `<response><data><cctvid>UTIC-1</cctvid><cctvname>Broken</cctvname></data></response>`
	if _, err := ParseCameras(strings.NewReader(fixture)); err == nil {
		t.Fatalf("expected missing stream url error")
	}
}

func TestRequestURL(t *testing.T) {
	got, err := requestURL("https://example.test/cctv?foo=bar", "secret key")
	if err != nil {
		t.Fatalf("requestURL: %v", err)
	}
	if got != "https://example.test/cctv?foo=bar&key=secret+key" {
		t.Fatalf("url=%q", got)
	}
}
