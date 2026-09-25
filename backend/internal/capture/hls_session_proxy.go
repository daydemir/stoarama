package capture

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/daydemir/stoarama/backend/internal/netguard"
)

// Seattle DOT publishes its cameras through a Wowza StreamLock origin. The
// master playlist (playlist.m3u8) hands each viewer a session-scoped variant
// URI (chunklist_w<session>.m3u8, children media_w<session>_<seq>.ts), and the
// origin expires that session after a few minutes: the chunklist and its
// segments then return 403 while the master playlist keeps issuing fresh
// sessions. FFmpeg's HLS demuxer reads the master playlist exactly once, so a
// 403 on the chunklist ends the FFmpeg process and the recorder has to restart
// capture (a gap plus a new capture attempt every few minutes).
//
// The session proxy sits between FFmpeg and the origin for these sources only.
// FFmpeg reads one stable local media playlist; the proxy re-reads the master
// playlist whenever the session URL returns 403 and continues from the fresh
// session. Wowza keeps media sequence numbers across sessions, so FFmpeg sees
// an uninterrupted live playlist and keeps its single persistent muxer.
const seattleStreamLockHost = "61e0c5d388c2e.streamlock.net"

// hlsSessionRefreshHost and hlsSessionDialControl are package vars so tests
// can point the proxy at a loopback httptest server. Production matches only
// the Seattle DOT StreamLock host and rejects private/metadata dials.
var (
	hlsSessionRefreshHost = func(host string) bool {
		return strings.EqualFold(strings.TrimSuffix(host, "."), seattleStreamLockHost)
	}
	hlsSessionDialControl = netguard.ControlReject
)

const (
	hlsSessionPlaylistTimeout = 15 * time.Second
	hlsSessionMaxPlaylistSize = 1 << 20
	hlsSessionMediaPath       = "/media.m3u8"
	hlsSessionSegmentPath     = "/segment"
)

// hlsSessionRefreshApplies reports whether a continuous capture input should be
// read through the session proxy. It is limited to single-input HLS playlists
// on the session-expiring origin so no other source changes behavior.
func hlsSessionRefreshApplies(input CaptureInput, pinHost string) bool {
	if input.AudioURL != "" || pinHost != "" || !isHLSManifestURL(input.URL) {
		return false
	}
	u, err := url.Parse(strings.TrimSpace(input.URL))
	if err != nil || (u.Scheme != "https" && u.Scheme != "http") {
		return false
	}
	return hlsSessionRefreshHost(u.Hostname())
}

type hlsSessionProxy struct {
	master   *url.URL
	headers  http.Header
	client   *http.Client
	listener net.Listener
	server   *http.Server
	ctx      context.Context
	cancel   context.CancelFunc

	mu         sync.Mutex
	mediaURL   string
	generation int64
	// FFmpeg must see one monotonic media sequence. When the origin restarts
	// the camera stream, a fresh session numbers segments from 1 again; the
	// proxy then starts a new epoch whose offset continues after the highest
	// sequence FFmpeg has already been shown.
	epoch          int64
	offset         int64
	lastUpstream   int64
	lastGeneration int64
}

func startHLSSessionProxy(input CaptureInput) (*hlsSessionProxy, error) {
	master, err := url.Parse(strings.TrimSpace(input.URL))
	if err != nil {
		return nil, fmt.Errorf("parse hls session master url: %w", err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("listen hls session proxy: %w", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	p := &hlsSessionProxy{
		master:       master,
		headers:      parseHLSSessionHeaders(input.Headers),
		listener:     listener,
		ctx:          ctx,
		cancel:       cancel,
		lastUpstream: -1,
	}
	p.client = &http.Client{
		Transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout:   10 * time.Second,
				KeepAlive: 30 * time.Second,
				Control:   hlsSessionDialControl,
			}).DialContext,
			TLSHandshakeTimeout:   10 * time.Second,
			ResponseHeaderTimeout: hlsSessionPlaylistTimeout,
			MaxIdleConnsPerHost:   4,
			IdleConnTimeout:       90 * time.Second,
		},
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 5 {
				return errors.New("too many hls session redirects")
			}
			if !p.sameOrigin(req.URL) {
				return errors.New("hls session redirect left the source origin")
			}
			return nil
		},
	}
	mux := http.NewServeMux()
	mux.HandleFunc(hlsSessionMediaPath, p.serveMedia)
	mux.HandleFunc(hlsSessionSegmentPath, p.serveSegment)
	p.server = &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		ErrorLog:          log.New(io.Discard, "", 0),
		BaseContext:       func(net.Listener) context.Context { return ctx },
	}
	go func() { _ = p.server.Serve(listener) }()
	return p, nil
}

// URL is the stable local media playlist FFmpeg reads for the whole attempt.
func (p *hlsSessionProxy) URL() string {
	return "http://" + p.listener.Addr().String() + hlsSessionMediaPath
}

func (p *hlsSessionProxy) Close() {
	p.cancel()
	_ = p.server.Close()
	if transport, ok := p.client.Transport.(*http.Transport); ok {
		transport.CloseIdleConnections()
	}
}

func (p *hlsSessionProxy) sameOrigin(u *url.URL) bool {
	return u != nil && strings.EqualFold(u.Scheme, p.master.Scheme) && strings.EqualFold(u.Host, p.master.Host)
}

// session returns the current session media playlist URL, reading the master
// playlist on first use.
func (p *hlsSessionProxy) session(ctx context.Context) (string, int64, error) {
	p.mu.Lock()
	mediaURL, generation := p.mediaURL, p.generation
	p.mu.Unlock()
	if mediaURL != "" {
		return mediaURL, generation, nil
	}
	return p.refresh(ctx, generation)
}

// refresh re-reads the master playlist for a fresh session URL unless another
// request already replaced the stale generation.
func (p *hlsSessionProxy) refresh(ctx context.Context, stale int64) (string, int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.generation != stale && p.mediaURL != "" {
		return p.mediaURL, p.generation, nil
	}
	body, status, err := p.fetchPlaylist(ctx, p.master.String())
	if err != nil {
		return "", p.generation, err
	}
	if status != http.StatusOK {
		return "", p.generation, &hlsSessionStatusError{status: status}
	}
	mediaURL, err := hlsSessionVariantURL(p.master, body)
	if err != nil {
		return "", p.generation, err
	}
	p.mediaURL = mediaURL
	p.generation++
	return p.mediaURL, p.generation, nil
}

type hlsSessionStatusError struct{ status int }

func (e *hlsSessionStatusError) Error() string {
	return fmt.Sprintf("hls session upstream status=%d", e.status)
}

func (p *hlsSessionProxy) fetchPlaylist(ctx context.Context, rawURL string) ([]byte, int, error) {
	ctx, cancel := context.WithTimeout(ctx, hlsSessionPlaylistTimeout)
	defer cancel()
	resp, err := p.get(ctx, rawURL, "")
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, hlsSessionMaxPlaylistSize))
		return nil, resp.StatusCode, nil
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, hlsSessionMaxPlaylistSize+1))
	if err != nil {
		return nil, 0, fmt.Errorf("read hls session playlist: %w", err)
	}
	if len(body) > hlsSessionMaxPlaylistSize {
		return nil, 0, errors.New("hls session playlist too large")
	}
	return body, resp.StatusCode, nil
}

func (p *hlsSessionProxy) get(ctx context.Context, rawURL, byteRange string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, err
	}
	for key, values := range p.headers {
		for _, value := range values {
			req.Header.Add(key, value)
		}
	}
	if byteRange != "" {
		req.Header.Set("Range", byteRange)
	}
	return p.client.Do(req)
}

// mediaPlaylist fetches the current session's media playlist, re-reading the
// master playlist once when the session has expired.
func (p *hlsSessionProxy) mediaPlaylist(ctx context.Context) (string, int64, []byte, int, error) {
	mediaURL, generation, err := p.session(ctx)
	if err != nil {
		return "", 0, nil, 0, err
	}
	body, status, err := p.fetchPlaylist(ctx, mediaURL)
	if err != nil {
		return "", 0, nil, 0, err
	}
	if !hlsSessionExpiredStatus(status) {
		return mediaURL, generation, body, status, nil
	}
	mediaURL, generation, err = p.refresh(ctx, generation)
	if err != nil {
		return "", 0, nil, 0, err
	}
	body, status, err = p.fetchPlaylist(ctx, mediaURL)
	return mediaURL, generation, body, status, err
}

func (p *hlsSessionProxy) serveMedia(w http.ResponseWriter, r *http.Request) {
	mediaURL, generation, body, status, err := p.mediaPlaylist(r.Context())
	if err != nil {
		writeHLSSessionError(w, err)
		return
	}
	if status != http.StatusOK {
		w.WriteHeader(status)
		return
	}
	base, err := url.Parse(mediaURL)
	if err != nil {
		w.WriteHeader(http.StatusBadGateway)
		return
	}
	epoch, offset := p.observe(generation, body)
	w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
	w.Header().Set("Cache-Control", "no-cache")
	_, _ = w.Write(p.rewriteMediaPlaylist(base, generation, epoch, offset, body))
}

// observe records the upstream sequence range a session served and returns
// the numbering epoch and offset FFmpeg should see for it. Only a newer
// session whose sequence moved backwards starts a new epoch; a stale reload
// inside one session never renumbers media FFmpeg already consumed.
func (p *hlsSessionProxy) observe(generation int64, body []byte) (int64, int64) {
	first, count := hlsMediaSequenceRange(body)
	p.mu.Lock()
	defer p.mu.Unlock()
	if count == 0 {
		return p.epoch, p.offset
	}
	last := first + count - 1
	if p.lastUpstream >= 0 && generation != p.lastGeneration && last < p.lastUpstream {
		p.offset = p.lastUpstream + p.offset + 1 - first
		p.epoch++
		p.lastUpstream = last
	} else if last > p.lastUpstream {
		p.lastUpstream = last
	}
	p.lastGeneration = generation
	return p.epoch, p.offset
}

var hlsSessionURIAttr = regexp.MustCompile(`URI="([^"]*)"`)

// rewriteMediaPlaylist points every segment at the proxy, carrying its
// upstream media sequence number so an expired segment URL can be re-found in
// the next session. Same-origin tag URIs (keys, init maps) are proxied too;
// off-origin URIs are left absolute, exactly as FFmpeg would read them directly.
func (p *hlsSessionProxy) rewriteMediaPlaylist(base *url.URL, generation, epoch, offset int64, body []byte) []byte {
	var out strings.Builder
	seq := int64(0)
	for _, line := range strings.Split(string(body), "\n") {
		line = strings.TrimSpace(line)
		switch {
		case line == "":
			continue
		case strings.HasPrefix(line, "#EXT-X-MEDIA-SEQUENCE:"):
			if n, err := strconv.ParseInt(strings.TrimPrefix(line, "#EXT-X-MEDIA-SEQUENCE:"), 10, 64); err == nil {
				seq = n
				line = "#EXT-X-MEDIA-SEQUENCE:" + strconv.FormatInt(n+offset, 10)
			}
		case strings.HasPrefix(line, "#"):
			line = hlsSessionURIAttr.ReplaceAllStringFunc(line, func(attr string) string {
				ref, err := url.Parse(strings.TrimSuffix(strings.TrimPrefix(attr, `URI="`), `"`))
				if err != nil {
					return attr
				}
				abs := base.ResolveReference(ref)
				if !p.sameOrigin(abs) {
					return `URI="` + abs.String() + `"`
				}
				// Keys and init maps go through the same origin-pinned, dial-guarded
				// handler as segments. Without a sequence they are fetched as-is.
				q := url.Values{}
				q.Set("u", abs.String())
				return `URI="` + hlsSessionSegmentPath + "?" + q.Encode() + `"`
			})
		default:
			if ref, err := url.Parse(line); err == nil {
				abs := base.ResolveReference(ref)
				if p.sameOrigin(abs) {
					q := url.Values{}
					q.Set("seq", strconv.FormatInt(seq, 10))
					q.Set("gen", strconv.FormatInt(generation, 10))
					q.Set("epoch", strconv.FormatInt(epoch, 10))
					q.Set("u", abs.String())
					line = hlsSessionSegmentPath + "?" + q.Encode()
				} else {
					line = abs.String()
				}
			}
			seq++
		}
		out.WriteString(line)
		out.WriteByte('\n')
	}
	return []byte(out.String())
}

func (p *hlsSessionProxy) serveSegment(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	target, err := url.Parse(query.Get("u"))
	if err != nil || !p.sameOrigin(target) {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	seq, seqErr := strconv.ParseInt(query.Get("seq"), 10, 64)
	generation, genErr := strconv.ParseInt(query.Get("gen"), 10, 64)
	epoch, epochErr := strconv.ParseInt(query.Get("epoch"), 10, 64)
	byteRange := r.Header.Get("Range")
	resp, err := p.get(r.Context(), target.String(), byteRange)
	if err != nil {
		writeHLSSessionError(w, err)
		return
	}
	if hlsSessionExpiredStatus(resp.StatusCode) && seqErr == nil && genErr == nil && epochErr == nil {
		// The session expired between the playlist read and this segment. Find the
		// same media sequence number in a fresh session instead of dropping it.
		resp.Body.Close()
		resp = nil
		if fresh, ok := p.segmentInFreshSession(r.Context(), generation, epoch, seq); ok {
			resp, err = p.get(r.Context(), fresh, byteRange)
			if err != nil {
				writeHLSSessionError(w, err)
				return
			}
		}
		if resp == nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}
	}
	defer resp.Body.Close()
	for _, key := range []string{"Content-Type", "Content-Length", "Content-Range", "Accept-Ranges"} {
		if value := resp.Header.Get(key); value != "" {
			w.Header().Set(key, value)
		}
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

func (p *hlsSessionProxy) segmentInFreshSession(ctx context.Context, generation, epoch, seq int64) (string, bool) {
	mediaURL, freshGeneration, err := p.refresh(ctx, generation)
	if err != nil {
		return "", false
	}
	body, status, err := p.fetchPlaylist(ctx, mediaURL)
	if err != nil || status != http.StatusOK {
		return "", false
	}
	if freshEpoch, _ := p.observe(freshGeneration, body); freshEpoch != epoch {
		// The camera stream restarted: this sequence number now names different
		// media, so let FFmpeg skip the segment instead of splicing wrong footage.
		return "", false
	}
	base, err := url.Parse(mediaURL)
	if err != nil {
		return "", false
	}
	uri, ok := hlsSegmentURIForSequence(body, seq)
	if !ok {
		return "", false
	}
	ref, err := url.Parse(uri)
	if err != nil {
		return "", false
	}
	abs := base.ResolveReference(ref)
	if !p.sameOrigin(abs) {
		return "", false
	}
	return abs.String(), true
}

// hlsMediaSequenceRange returns a media playlist's first sequence number and
// its segment count.
func hlsMediaSequenceRange(body []byte) (int64, int64) {
	first, count := int64(0), int64(0)
	for _, line := range strings.Split(string(body), "\n") {
		line = strings.TrimSpace(line)
		switch {
		case line == "":
		case strings.HasPrefix(line, "#EXT-X-MEDIA-SEQUENCE:"):
			if n, err := strconv.ParseInt(strings.TrimPrefix(line, "#EXT-X-MEDIA-SEQUENCE:"), 10, 64); err == nil {
				first = n
			}
		case strings.HasPrefix(line, "#"):
		default:
			count++
		}
	}
	return first, count
}

// hlsSessionExpiredStatus is how the origin answers a request on an expired
// session: 403, or 404 once the camera stream restarted under a new session.
func hlsSessionExpiredStatus(status int) bool {
	return status == http.StatusForbidden || status == http.StatusNotFound
}

func hlsSegmentURIForSequence(body []byte, want int64) (string, bool) {
	seq := int64(0)
	for _, line := range strings.Split(string(body), "\n") {
		line = strings.TrimSpace(line)
		switch {
		case line == "":
		case strings.HasPrefix(line, "#EXT-X-MEDIA-SEQUENCE:"):
			if n, err := strconv.ParseInt(strings.TrimPrefix(line, "#EXT-X-MEDIA-SEQUENCE:"), 10, 64); err == nil {
				seq = n
			}
		case strings.HasPrefix(line, "#"):
		default:
			if seq == want {
				return line, true
			}
			seq++
		}
	}
	return "", false
}

// hlsSessionVariantURL picks the highest-bandwidth variant from a master
// playlist. A body that is already a media playlist is used as-is.
func hlsSessionVariantURL(master *url.URL, body []byte) (string, error) {
	lines := strings.Split(string(body), "\n")
	best := ""
	bestBandwidth := int64(-1)
	isMedia := false
	for i := 0; i < len(lines); i++ {
		line := strings.TrimSpace(lines[i])
		if strings.HasPrefix(line, "#EXTINF") || strings.HasPrefix(line, "#EXT-X-TARGETDURATION") {
			isMedia = true
		}
		if !strings.HasPrefix(line, "#EXT-X-STREAM-INF:") {
			continue
		}
		bandwidth := hlsStreamInfBandwidth(line)
		for i+1 < len(lines) {
			i++
			uri := strings.TrimSpace(lines[i])
			if uri == "" || strings.HasPrefix(uri, "#") {
				continue
			}
			if bandwidth > bestBandwidth {
				best, bestBandwidth = uri, bandwidth
			}
			break
		}
	}
	if best == "" {
		if isMedia {
			return master.String(), nil
		}
		return "", errors.New("hls session master playlist has no variant")
	}
	ref, err := url.Parse(best)
	if err != nil {
		return "", fmt.Errorf("parse hls session variant: %w", err)
	}
	abs := master.ResolveReference(ref)
	if !strings.EqualFold(abs.Scheme, master.Scheme) || !strings.EqualFold(abs.Host, master.Host) {
		return "", errors.New("hls session variant left the source origin")
	}
	return abs.String(), nil
}

func hlsStreamInfBandwidth(line string) int64 {
	for _, attr := range strings.Split(strings.TrimPrefix(line, "#EXT-X-STREAM-INF:"), ",") {
		key, value, ok := strings.Cut(attr, "=")
		if ok && strings.EqualFold(strings.TrimSpace(key), "BANDWIDTH") {
			if n, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64); err == nil {
				return n
			}
		}
	}
	return 0
}

func parseHLSSessionHeaders(raw string) http.Header {
	headers := http.Header{}
	for _, line := range strings.Split(strings.ReplaceAll(raw, "\r\n", "\n"), "\n") {
		key, value, ok := strings.Cut(line, ":")
		if ok && strings.TrimSpace(key) != "" {
			headers.Add(strings.TrimSpace(key), strings.TrimSpace(value))
		}
	}
	return headers
}

func writeHLSSessionError(w http.ResponseWriter, err error) {
	var statusErr *hlsSessionStatusError
	if errors.As(err, &statusErr) {
		w.WriteHeader(statusErr.status)
		return
	}
	w.WriteHeader(http.StatusBadGateway)
}
