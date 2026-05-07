package utic

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	DefaultEndpoint = "https://www.utic.go.kr/guide/openDataCctvStream.jsp"
	SourcePageURL   = "https://www.utic.go.kr/traffic/cctvList.do?first=true"
)

type Field string

const (
	FieldID        Field = "cctvid"
	FieldName      Field = "cctvname"
	FieldStreamURL Field = "cctvurl"
	FieldCoordX    Field = "coordx"
	FieldCoordY    Field = "coordy"
	FieldKind      Field = "kind"
	FieldFormat    Field = "format"
)

type ClientConfig struct {
	Endpoint   string
	ServiceKey string
	HTTPClient *http.Client
}

type Camera struct {
	ID        string         `json:"id"`
	Name      string         `json:"name"`
	StreamURL string         `json:"stream_url"`
	Lat       *float64       `json:"lat,omitempty"`
	Lon       *float64       `json:"lon,omitempty"`
	Kind      string         `json:"kind,omitempty"`
	Format    string         `json:"format,omitempty"`
	Raw       map[string]any `json:"raw"`
}

func FetchCameras(ctx context.Context, cfg ClientConfig) ([]Camera, error) {
	endpoint := strings.TrimSpace(cfg.Endpoint)
	if endpoint == "" {
		endpoint = DefaultEndpoint
	}
	reqURL, err := requestURL(endpoint, strings.TrimSpace(cfg.ServiceKey))
	if err != nil {
		return nil, err
	}
	httpc := cfg.HTTPClient
	if httpc == nil {
		httpc = &http.Client{Timeout: 45 * time.Second}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("build UTIC request: %w", err)
	}
	req.Header.Set("Accept", "application/xml,text/xml,*/*")
	resp, err := httpc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch UTIC CCTV list: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return nil, fmt.Errorf("fetch UTIC CCTV list status=%d body=%q", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	cameras, err := ParseCameras(resp.Body)
	if err != nil {
		return nil, err
	}
	if len(cameras) == 0 {
		return nil, fmt.Errorf("UTIC CCTV list contained zero valid cameras")
	}
	return cameras, nil
}

func requestURL(endpoint string, serviceKey string) (string, error) {
	if strings.TrimSpace(serviceKey) == "" {
		return "", fmt.Errorf("UTIC service key is required")
	}
	if strings.Contains(endpoint, "{key}") {
		return strings.ReplaceAll(endpoint, "{key}", url.QueryEscape(serviceKey)), nil
	}
	u, err := url.Parse(endpoint)
	if err != nil {
		return "", fmt.Errorf("parse UTIC endpoint: %w", err)
	}
	q := u.Query()
	if q.Get("key") == "" && q.Get("serviceKey") == "" {
		q.Set("key", serviceKey)
	}
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func ParseCameras(r io.Reader) ([]Camera, error) {
	decoder := xml.NewDecoder(r)
	stack := make([]string, 0, 8)
	items := make([]map[string]string, 0, 1024)
	var current map[string]string
	for {
		tok, err := decoder.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("parse UTIC XML: %w", err)
		}
		switch t := tok.(type) {
		case xml.StartElement:
			name := canonicalXMLName(t.Name.Local)
			stack = append(stack, name)
			if startsCameraItem(name) && current == nil {
				current = map[string]string{}
			}
		case xml.CharData:
			if current == nil || len(stack) == 0 {
				continue
			}
			key := normalizeField(stack[len(stack)-1])
			if key == "" {
				continue
			}
			value := strings.TrimSpace(string(t))
			if value != "" {
				current[key] = value
			}
		case xml.EndElement:
			name := canonicalXMLName(t.Name.Local)
			if current != nil && len(stack) > 0 && stack[len(stack)-1] == name && startsCameraItem(name) {
				items = append(items, current)
				current = nil
			}
			if len(stack) > 0 {
				stack = stack[:len(stack)-1]
			}
		}
	}
	if current != nil && len(current) > 0 {
		items = append(items, current)
	}
	return decodeCameras(items)
}

func startsCameraItem(name string) bool {
	switch name {
	case "data", "item", "row", "cctv":
		return true
	default:
		return false
	}
}

func canonicalXMLName(raw string) string {
	return strings.ToLower(strings.TrimSpace(raw))
}

func normalizeField(raw string) string {
	v := strings.ToLower(strings.ReplaceAll(strings.TrimSpace(raw), "_", ""))
	switch v {
	case "cctvid", "id":
		return string(FieldID)
	case "cctvname", "cctvnm", "name":
		return string(FieldName)
	case "cctvurl", "url", "streamurl":
		return string(FieldStreamURL)
	case "coordx", "xcoord", "x", "lon", "lng", "longitude":
		return string(FieldCoordX)
	case "coordy", "ycoord", "y", "lat", "latitude":
		return string(FieldCoordY)
	case "kind":
		return string(FieldKind)
	case "format":
		return string(FieldFormat)
	default:
		return ""
	}
}

func decodeCameras(items []map[string]string) ([]Camera, error) {
	out := make([]Camera, 0, len(items))
	seen := map[string]bool{}
	for _, item := range items {
		cam, ok, err := decodeCamera(item)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		key := cam.ID + "\x00" + cam.StreamURL
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, cam)
	}
	return out, nil
}

func decodeCamera(item map[string]string) (Camera, bool, error) {
	id := strings.TrimSpace(item[string(FieldID)])
	name := strings.TrimSpace(item[string(FieldName)])
	streamURL := strings.TrimSpace(item[string(FieldStreamURL)])
	if id == "" && name == "" && streamURL == "" {
		return Camera{}, false, nil
	}
	if id == "" {
		return Camera{}, false, fmt.Errorf("UTIC camera missing id")
	}
	if name == "" {
		return Camera{}, false, fmt.Errorf("UTIC camera %s missing name", id)
	}
	if streamURL == "" {
		return Camera{}, false, fmt.Errorf("UTIC camera %s missing stream url", id)
	}
	if err := validateHTTPURL(streamURL); err != nil {
		return Camera{}, false, fmt.Errorf("UTIC camera %s stream url: %w", id, err)
	}
	lon, err := parseOptionalFloat(item[string(FieldCoordX)])
	if err != nil {
		return Camera{}, false, fmt.Errorf("UTIC camera %s coordx: %w", id, err)
	}
	lat, err := parseOptionalFloat(item[string(FieldCoordY)])
	if err != nil {
		return Camera{}, false, fmt.Errorf("UTIC camera %s coordy: %w", id, err)
	}
	raw := make(map[string]any, len(item))
	for k, v := range item {
		raw[k] = v
	}
	return Camera{
		ID:        id,
		Name:      name,
		StreamURL: streamURL,
		Lat:       lat,
		Lon:       lon,
		Kind:      strings.TrimSpace(item[string(FieldKind)]),
		Format:    strings.TrimSpace(item[string(FieldFormat)]),
		Raw:       raw,
	}, true, nil
}

func validateHTTPURL(raw string) error {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return err
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("scheme must be http or https")
	}
	if u.Host == "" {
		return fmt.Errorf("host is required")
	}
	return nil
}

func parseOptionalFloat(raw string) (*float64, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return nil, nil
	}
	f, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return nil, err
	}
	return &f, nil
}
