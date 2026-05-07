package korea

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

type UpstreamFamily string

const (
	FamilyUTIC     UpstreamFamily = "utic"
	FamilyTOPIS    UpstreamFamily = "topis"
	FamilySPATIC   UpstreamFamily = "spatic"
	FamilyKBS      UpstreamFamily = "kbs"
	FamilyGiGAeyes UpstreamFamily = "gigaeyes"
)

func (f UpstreamFamily) Label() string {
	switch f {
	case FamilyUTIC:
		return "UTIC / 경찰청"
	case FamilyTOPIS:
		return "TOPIS"
	case FamilySPATIC:
		return "SPATIC"
	case FamilyKBS:
		return "KBS"
	case FamilyGiGAeyes:
		return "GiGAeyes"
	default:
		return ""
	}
}

func allFamilies() []UpstreamFamily {
	return []UpstreamFamily{FamilyUTIC, FamilyTOPIS, FamilySPATIC, FamilyKBS, FamilyGiGAeyes}
}

type EntrypointType string

const (
	EntrypointNaverMap       EntrypointType = "naver_map"
	EntrypointOfficialPortal EntrypointType = "official_portal"
	EntrypointYouTubeChannel EntrypointType = "youtube_channel"
	EntrypointDirectHLS      EntrypointType = "direct_hls"
)

func (e EntrypointType) Label() string {
	switch e {
	case EntrypointNaverMap:
		return "Naver Map"
	case EntrypointOfficialPortal:
		return "Official portal"
	case EntrypointYouTubeChannel:
		return "YouTube channel"
	case EntrypointDirectHLS:
		return "Direct HLS"
	default:
		return ""
	}
}

func allEntrypoints() []EntrypointType {
	return []EntrypointType{EntrypointNaverMap, EntrypointOfficialPortal, EntrypointYouTubeChannel, EntrypointDirectHLS}
}

type StreamRecord struct {
	ID                  int64  `json:"id"`
	Provider            string `json:"provider"`
	ExternalID          string `json:"external_id"`
	Name                string `json:"name"`
	Slug                string `json:"slug"`
	SourceURL           string `json:"source_url"`
	SourcePageURL       string `json:"source_page_url"`
	SourceFamily        string `json:"source_family"`
	CaptureType         string `json:"capture_type"`
	RecordingState      string `json:"recording_state"`
	LocationCountry     string `json:"location_country"`
	LocationCountryCode string `json:"location_country_code"`
	LocationCity        string `json:"location_city"`
	LocationText        string `json:"location_text"`
	SourceHost          string `json:"source_host"`
	SourcePageHost      string `json:"source_page_host"`
}

type StreamAttribution struct {
	Stream          StreamRecord   `json:"stream"`
	Family          UpstreamFamily `json:"family"`
	FamilyLabel     string         `json:"family_label"`
	Entrypoint      EntrypointType `json:"entrypoint"`
	EntrypointLabel string         `json:"entrypoint_label"`
}

type EntrypointGroup struct {
	Entrypoint  EntrypointType      `json:"entrypoint"`
	Label       string              `json:"label"`
	StreamCount int                 `json:"stream_count"`
	Streams     []StreamAttribution `json:"streams"`
}

type FamilyGroup struct {
	Family      UpstreamFamily    `json:"family"`
	Label       string            `json:"label"`
	StreamCount int               `json:"stream_count"`
	Present     bool              `json:"present"`
	Entrypoints []EntrypointGroup `json:"entrypoints"`
}

type UnresolvedStream struct {
	Stream StreamRecord `json:"stream"`
	Reason string       `json:"reason"`
}

type Summary struct {
	TotalFamilies     int  `json:"total_families"`
	CoveredFamilies   int  `json:"covered_families"`
	TotalStreams      int  `json:"total_streams"`
	ResolvedStreams   int  `json:"resolved_streams"`
	UnresolvedStreams int  `json:"unresolved_streams"`
	Complete          bool `json:"complete"`
}

type Inventory struct {
	RetrievedAt time.Time          `json:"retrieved_at"`
	Summary     Summary            `json:"summary"`
	Families    []FamilyGroup      `json:"families"`
	Unresolved  []UnresolvedStream `json:"unresolved"`
}

type queryer interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

func LoadInventory(ctx context.Context, q queryer) (Inventory, error) {
	rows, err := q.Query(ctx, `
		SELECT
			id, provider, external_id, name, slug, source_url, source_page_url,
			source_family, capture_type, recording_state,
			location_country, location_country_code, location_city, location_text,
			created_at, updated_at
		FROM streams
		ORDER BY id ASC
	`)
	if err != nil {
		return Inventory{}, fmt.Errorf("query korea inventory streams: %w", err)
	}
	defer rows.Close()

	streams := make([]StreamRecord, 0, 2048)
	for rows.Next() {
		stream, err := scanStreamRecord(rows)
		if err != nil {
			return Inventory{}, fmt.Errorf("scan korea stream: %w", err)
		}
		stream.SourceHost = hostFromURL(stream.SourceURL)
		stream.SourcePageHost = hostFromURL(stream.SourcePageURL)
		streams = append(streams, stream)
	}
	if rows.Err() != nil {
		return Inventory{}, fmt.Errorf("iterate korea streams: %w", rows.Err())
	}
	return BuildInventory(streams, time.Now().UTC()), nil
}

func BuildInventory(streams []StreamRecord, retrievedAt time.Time) Inventory {
	families := make([]FamilyGroup, 0, len(allFamilies()))
	familyIndex := make(map[UpstreamFamily]int, len(allFamilies()))
	for i, family := range allFamilies() {
		families = append(families, FamilyGroup{
			Family:      family,
			Label:       family.Label(),
			Present:     false,
			Entrypoints: make([]EntrypointGroup, 0, len(allEntrypoints())),
		})
		familyIndex[family] = i
	}

	unresolved := make([]UnresolvedStream, 0)
	for _, stream := range streams {
		if !isKoreaCandidate(stream) {
			continue
		}
		attribution, reason, ok := classifyStream(stream)
		if !ok {
			unresolved = append(unresolved, UnresolvedStream{Stream: stream, Reason: reason})
			continue
		}
		idx := familyIndex[attribution.Family]
		fam := &families[idx]
		fam.StreamCount++
		fam.Present = true
		fam.Entrypoints = appendEntrypointStream(fam.Entrypoints, attribution)
	}

	for i := range families {
		sort.SliceStable(families[i].Entrypoints, func(a, b int) bool {
			ai := entrypointOrder(families[i].Entrypoints[a].Entrypoint)
			bi := entrypointOrder(families[i].Entrypoints[b].Entrypoint)
			if ai != bi {
				return ai < bi
			}
			return families[i].Entrypoints[a].Entrypoint < families[i].Entrypoints[b].Entrypoint
		})
		for j := range families[i].Entrypoints {
			sort.SliceStable(families[i].Entrypoints[j].Streams, func(a, b int) bool {
				left := families[i].Entrypoints[j].Streams[a]
				right := families[i].Entrypoints[j].Streams[b]
				if left.Stream.Name != right.Stream.Name {
					return left.Stream.Name < right.Stream.Name
				}
				return left.Stream.ID < right.Stream.ID
			})
		}
	}

	sort.SliceStable(unresolved, func(i, j int) bool {
		if unresolved[i].Stream.Provider != unresolved[j].Stream.Provider {
			return unresolved[i].Stream.Provider < unresolved[j].Stream.Provider
		}
		if unresolved[i].Stream.Name != unresolved[j].Stream.Name {
			return unresolved[i].Stream.Name < unresolved[j].Stream.Name
		}
		return unresolved[i].Stream.ID < unresolved[j].Stream.ID
	})

	resolvedStreams := 0
	coveredFamilies := 0
	for _, fam := range families {
		resolvedStreams += fam.StreamCount
		if fam.Present {
			coveredFamilies++
		}
	}

	summary := Summary{
		TotalFamilies:     len(families),
		CoveredFamilies:   coveredFamilies,
		TotalStreams:      resolvedStreams + len(unresolved),
		ResolvedStreams:   resolvedStreams,
		UnresolvedStreams: len(unresolved),
		Complete:          len(unresolved) == 0 && coveredFamilies == len(families),
	}

	return Inventory{
		RetrievedAt: retrievedAt,
		Summary:     summary,
		Families:    families,
		Unresolved:  unresolved,
	}
}

func appendEntrypointStream(groups []EntrypointGroup, a StreamAttribution) []EntrypointGroup {
	for i := range groups {
		if groups[i].Entrypoint == a.Entrypoint {
			groups[i].StreamCount++
			groups[i].Streams = append(groups[i].Streams, a)
			return groups
		}
	}
	groups = append(groups, EntrypointGroup{
		Entrypoint:  a.Entrypoint,
		Label:       a.EntrypointLabel,
		StreamCount: 1,
		Streams:     []StreamAttribution{a},
	})
	return groups
}

func classifyStream(stream StreamRecord) (StreamAttribution, string, bool) {
	family, ok := familyForStream(stream)
	if !ok {
		return StreamAttribution{}, "unrecognized_korea_family", false
	}
	entrypoint, ok := entrypointForStream(stream)
	if !ok {
		return StreamAttribution{}, "unrecognized_korea_entrypoint", false
	}
	return StreamAttribution{
		Stream:          stream,
		Family:          family,
		FamilyLabel:     family.Label(),
		Entrypoint:      entrypoint,
		EntrypointLabel: entrypoint.Label(),
	}, "", true
}

func familyForStream(stream StreamRecord) (UpstreamFamily, bool) {
	if providerEq(stream.Provider, "GIGAEYES") || containsYouTubeGiGAeyes(stream.SourcePageURL) {
		return FamilyGiGAeyes, true
	}
	if providerEq(stream.Provider, "KBS") || hostHasSuffix(stream.SourceHost, "loomex.net") || hostEq(stream.SourcePageHost, "d.kbs.co.kr") {
		return FamilyKBS, true
	}
	if providerEq(stream.Provider, "SPATIC") || hostHasSuffix(stream.SourceHost, "spatic.go.kr") || hostEq(stream.SourcePageHost, "spatic.go.kr") {
		return FamilySPATIC, true
	}
	if providerEq(stream.Provider, "TOPIS") || strings.Contains(strings.ToLower(stream.SourceHost), "topiscctv") || hostEq(stream.SourcePageHost, "topis.seoul.go.kr") {
		return FamilyTOPIS, true
	}
	if providerEq(stream.Provider, "UTIC") || providerEq(stream.Provider, "POLICE") || providerEq(stream.Provider, "UTIC_POLICE") || hostHasSuffix(stream.SourceHost, "ktict.co.kr") || strings.Contains(strings.ToLower(stream.SourceURL), "koroad") {
		return FamilyUTIC, true
	}
	return "", false
}

func entrypointForStream(stream StreamRecord) (EntrypointType, bool) {
	switch {
	case hostEq(stream.SourcePageHost, "map.naver.com"):
		return EntrypointNaverMap, true
	case hostHasSuffix(stream.SourcePageHost, "youtube.com"):
		return EntrypointYouTubeChannel, true
	case hostEq(stream.SourcePageHost, "topis.seoul.go.kr") || hostEq(stream.SourcePageHost, "spatic.go.kr") || hostEq(stream.SourcePageHost, "d.kbs.co.kr") || hostEq(stream.SourcePageHost, "utic.go.kr"):
		return EntrypointOfficialPortal, true
	case stream.SourcePageHost == "":
		return EntrypointDirectHLS, true
	default:
		return "", false
	}
}

func entrypointOrder(entrypoint EntrypointType) int {
	switch entrypoint {
	case EntrypointNaverMap:
		return 0
	case EntrypointOfficialPortal:
		return 1
	case EntrypointYouTubeChannel:
		return 2
	case EntrypointDirectHLS:
		return 3
	default:
		return 99
	}
}

func isKoreaCandidate(stream StreamRecord) bool {
	if family, ok := familyForStream(stream); ok && family != "" {
		return true
	}
	if providerEq(stream.Provider, "NAVER_MAP") || hostEq(stream.SourcePageHost, "map.naver.com") {
		return true
	}
	return false
}

func providerEq(raw, want string) bool {
	return normalizeToken(raw) == normalizeToken(want)
}

func normalizeToken(raw string) string {
	return strings.ToUpper(strings.ReplaceAll(strings.TrimSpace(raw), " ", "_"))
}

func hostEq(raw, want string) bool {
	return normalizeHost(raw) == normalizeHost(want)
}

func hostHasSuffix(raw, suffix string) bool {
	host := normalizeHost(raw)
	suf := normalizeHost(suffix)
	return host == suf || strings.HasSuffix(host, "."+suf)
}

func normalizeHost(raw string) string {
	return strings.TrimPrefix(strings.ToLower(strings.TrimSpace(raw)), "www.")
}

func hostFromURL(raw string) string {
	v := strings.TrimSpace(raw)
	if v == "" {
		return ""
	}
	parsed, err := url.Parse(v)
	if err != nil {
		return ""
	}
	return normalizeHost(parsed.Hostname())
}

func containsYouTubeGiGAeyes(raw string) bool {
	v := strings.ToLower(strings.TrimSpace(raw))
	return strings.Contains(v, "youtube.com") && strings.Contains(v, "@gigaeyeslivetv")
}

func scanStreamRecord(rows pgx.Rows) (StreamRecord, error) {
	var s StreamRecord
	var createdAt, updatedAt time.Time
	if err := rows.Scan(
		&s.ID, &s.Provider, &s.ExternalID, &s.Name, &s.Slug, &s.SourceURL, &s.SourcePageURL,
		&s.SourceFamily, &s.CaptureType, &s.RecordingState,
		&s.LocationCountry, &s.LocationCountryCode, &s.LocationCity, &s.LocationText,
		&createdAt, &updatedAt,
	); err != nil {
		return StreamRecord{}, err
	}
	return s, nil
}

func MarshalInventory(inv Inventory) ([]byte, error) {
	return json.MarshalIndent(inv, "", "  ")
}
