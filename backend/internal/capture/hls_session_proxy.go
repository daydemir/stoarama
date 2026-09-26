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
//
// KBS's Korean road cameras (kbsapi.loomex.net '!hls' references) resolve to a
// Wowza origin behind the NCE CDN at kbscctv-cache.loomex.net. The token is in
// the path and lasts 24 hours, but the CDN answers a bare 403 to a fraction of
// cache-miss and revalidation requests for some cameras (9996 in 2026-09: about
// one chunklist request in five, in bursts of a few seconds). FFmpeg ends the
// whole process on a single failed playlist reload and skips a segment after a
// single failed fetch, so recording 337 restarted every couple of minutes and
// lost ~18% of its window. The same proxy absorbs those transient failures: it
// retries briefly and, if the chunklist is still refused, serves the last good
// chunklist so FFmpeg simply polls again instead of exiting.
const (
	seattleStreamLockHost = "61e0c5d388c2e.streamlock.net"
	kbsLoomexCDNHost      = "kbscctv-cache.loomex.net"
)

// hlsSessionRefreshHost, hlsSessionDialControl, and hlsSessionRetryDelays are
// package vars so tests can point the proxy at a loopback httptest server and
// shorten its retry schedule. Production matches only the listed origins and
// rejects private/metadata dials.
var (
	hlsSessionRefreshHost = productionHLSSessionRefreshHost
	hlsSessionDialControl = netguard.ControlReject
	// hlsSessionRetryDelays is the backoff between attempts at one upstream
	// request that failed transiently. Its ~7s total stays well inside FFmpeg's
	// 15s rw_timeout and a 10s HLS target duration.
	hlsSessionRetryDelays = []time.Duration{250 * time.Millisecond, 500 * time.Millisecond, time.Second, 2 * time.Second, 3 * time.Second}
)

func productionHLSSessionRefreshHost(host string) bool {
	host = strings.TrimSuffix(host, ".")
	return strings.EqualFold(host, seattleStreamLockHost) || strings.EqualFold(host, kbsLoomexCDNHost)
}

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

	// lastGood is the most recent media playlist the origin served, replayed
	// when a reload keeps failing so FFmpeg polls again instead of exiting.
	lastGood *hlsSessionPlaylist
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
	if mediaURL != p.mediaURL {
		// Only a new session URL is a new generation. An origin whose variant URL
		// is stable (token in the path) must not look like a new session, or a
		// briefly stale CDN copy would be mistaken for a camera restart.
		p.mediaURL = mediaURL
		p.generation++
		// The last good playlist lists the old session's URLs; never replay it.
		p.lastGood = nil
	}
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

type hlsSessionPlaylist struct {
	mediaURL   string
	generation int64
	body       []byte
}

// mediaPlaylist fetches the current session's media playlist. An expired
// session re-reads the master playlist; a transient refusal is retried on a
// short backoff. When every attempt fails, the last good playlist is replayed:
// FFmpeg sees no new segments and reloads again, rather than treating one
// failed reload as the end of the stream. A source that stays dead still
// reaches the recorder's no-progress watchdog and a fresh resolve.
func (p *hlsSessionProxy) mediaPlaylist(ctx context.Context) (string, int64, []byte, int, error) {
	var lastErr error
	lastStatus := 0
	for attempt := 0; ; attempt++ {
		mediaURL, generation, body, status, err := p.fetchMediaPlaylist(ctx)
		if err == nil && status == http.StatusOK {
			playlist := p.recordGoodPlaylist(&hlsSessionPlaylist{mediaURL: mediaURL, generation: generation, body: body})
			return playlist.mediaURL, playlist.generation, playlist.body, status, nil
		}
		lastErr, lastStatus = err, status
		if err == nil && !hlsSessionTransientStatus(status) {
			return mediaURL, generation, body, status, nil
		}
		if attempt >= len(hlsSessionRetryDelays) || !sleepContext(ctx, hlsSessionRetryDelays[attempt]) {
			break
		}
	}
	p.mu.Lock()
	lastGood := p.lastGood
	p.mu.Unlock()
	if lastGood != nil && ctx.Err() == nil {
		return lastGood.mediaURL, lastGood.generation, lastGood.body, http.StatusOK, nil
	}
	if lastErr != nil {
		return "", 0, nil, 0, lastErr
	}
	return "", 0, nil, lastStatus, nil
}

// recordGoodPlaylist remembers a successfully fetched playlist for replay and
// returns the playlist to serve. A playlist from a session that has since been
// replaced is served but never cached. A stale CDN copy whose sequence went
// backwards within the same session is neither cached nor served: FFmpeg gets
// the newer last good playlist instead of a regressed media sequence.
func (p *hlsSessionProxy) recordGoodPlaylist(playlist *hlsSessionPlaylist) *hlsSessionPlaylist {
	p.mu.Lock()
	defer p.mu.Unlock()
	if playlist.generation != p.generation {
		return playlist
	}
	if first, count := hlsMediaSequenceRange(playlist.body); p.lastGood != nil && count > 0 &&
		p.lastGood.generation == playlist.generation && playlist.generation == p.lastGeneration &&
		first+count-1 < p.lastUpstream {
		return p.lastGood
	}
	p.lastGood = playlist
	return playlist
}

// fetchMediaPlaylist is one attempt: read the session's media playlist, and on
// an expired-session status re-read the master playlist once and try again.
func (p *hlsSessionProxy) fetchMediaPlaylist(ctx context.Context) (string, int64, []byte, int, error) {
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

// rewriteMediaPlaylist points every URI FFmpeg would fetch at the proxy, so
// all of them are origin-pinned and dial-guarded (an off-origin URI is refused
// by serveSegment rather than fetched). Each carries the upstream media
// sequence it belongs to (a tag URI such as a key or init map: the next
// segment's), so an expired session URL can be re-found in the next session.
func (p *hlsSessionProxy) rewriteMediaPlaylist(base *url.URL, generation, epoch, offset int64, body []byte) []byte {
	var out strings.Builder
	seq := int64(0)
	proxied := func(abs *url.URL, tag string) string {
		q := url.Values{}
		q.Set("seq", strconv.FormatInt(seq, 10))
		q.Set("gen", strconv.FormatInt(generation, 10))
		q.Set("epoch", strconv.FormatInt(epoch, 10))
		if tag != "" {
			q.Set("tag", tag)
		}
		q.Set("u", abs.String())
		return hlsSessionSegmentPath + "?" + q.Encode()
	}
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
			tag := hlsTagName(line)
			line = hlsSessionURIAttr.ReplaceAllStringFunc(line, func(attr string) string {
				ref, err := url.Parse(strings.TrimSuffix(strings.TrimPrefix(attr, `URI="`), `"`))
				if err != nil {
					return attr
				}
				return `URI="` + proxied(base.ResolveReference(ref), tag) + `"`
			})
		default:
			if ref, err := url.Parse(line); err == nil {
				line = proxied(base.ResolveReference(ref), "")
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
	tag := query.Get("tag")
	byteRange := r.Header.Get("Range")
	var resp *http.Response
	located := seqErr == nil && genErr == nil && epochErr == nil
	segment := hlsSegmentRequest{target: target, seq: seq, generation: generation, epoch: epoch, tag: tag}
	for attempt := 0; ; attempt++ {
		var sessionConfirmed bool
		resp, sessionConfirmed, err = p.fetchSegment(r.Context(), &segment, byteRange, located)
		if sessionConfirmed {
			// A completed lookup showed the session URL is unchanged, so the
			// refusal is the CDN's: later attempts retry the segment alone
			// instead of re-reading the master and media playlists each time.
			located = false
		}
		if err == nil && (resp == nil || !hlsSessionTransientStatus(resp.StatusCode)) {
			break
		}
		if attempt >= len(hlsSessionRetryDelays) {
			break
		}
		if resp != nil {
			resp.Body.Close()
			resp = nil
		}
		// The CDN refuses some cache-miss requests for a few seconds. A brief
		// retry keeps the segment; FFmpeg would otherwise skip it after one 403.
		if !sleepContext(r.Context(), hlsSessionRetryDelays[attempt]) {
			break
		}
	}
	if err != nil {
		writeHLSSessionError(w, err)
		return
	}
	if resp == nil {
		w.WriteHeader(http.StatusNotFound)
		return
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

// hlsSegmentRequest is the segment (or key/map) FFmpeg asked for: the
// upstream URL, its media sequence, and the session generation and numbering
// epoch of the playlist that listed it.
type hlsSegmentRequest struct {
	target     *url.URL
	seq        int64
	generation int64
	epoch      int64
	tag        string
}

// fetchSegment is one attempt at a segment (or key/map). A request whose
// session expired between the playlist read and this fetch is re-found by
// media sequence in a fresh session instead of being dropped; the request then
// follows that session's URL, so later attempts retry the replacement rather
// than the expired URL. A nil response with a nil error means the segment no
// longer exists in any session. sessionConfirmed reports a completed
// fresh-session lookup that found the session URL unchanged, so the refusal
// was not an expiry.
func (p *hlsSessionProxy) fetchSegment(ctx context.Context, segment *hlsSegmentRequest, byteRange string, located bool) (resp *http.Response, sessionConfirmed bool, err error) {
	resp, err = p.get(ctx, segment.target.String(), byteRange)
	if err != nil {
		return nil, false, err
	}
	if !hlsSessionExpiredStatus(resp.StatusCode) || !located {
		return resp, false, nil
	}
	fresh, freshGeneration, ok, complete := p.segmentInFreshSession(ctx, segment.generation, segment.epoch, segment.seq, segment.tag)
	stable := p.stableSession(segment.generation)
	switch {
	case ok && fresh != segment.target.String():
		replacement, parseErr := url.Parse(fresh)
		if parseErr != nil {
			return resp, false, nil
		}
		resp.Body.Close()
		segment.target, segment.generation = replacement, freshGeneration
		resp, err = p.get(ctx, fresh, byteRange)
		return resp, false, err
	case !ok && !stable:
		// A new session no longer lists this media (the camera stream restarted),
		// so let FFmpeg skip it rather than splice different footage.
		resp.Body.Close()
		return nil, false, nil
	default:
		// The session URL is unchanged (a path-token origin), so the refusal was
		// not an expiry: return it for the caller to retry.
		return resp, complete && stable, nil
	}
}

// stableSession reports whether the session the segment was listed in is
// still current, i.e. the origin did not hand out a new session URL.
func (p *hlsSessionProxy) stableSession(generation int64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.generation == generation
}

// segmentInFreshSession looks the segment up by media sequence in the current
// (refreshed) session and returns its URL with that session's generation. complete is false when the refresh or the playlist read
// failed, so the lookup told us nothing and may be repeated.
func (p *hlsSessionProxy) segmentInFreshSession(ctx context.Context, generation, epoch, seq int64, tag string) (uri string, freshGeneration int64, ok, complete bool) {
	mediaURL, freshGeneration, err := p.refresh(ctx, generation)
	if err != nil {
		return "", 0, false, false
	}
	body, status, err := p.fetchPlaylist(ctx, mediaURL)
	if err != nil || status != http.StatusOK {
		return "", 0, false, false
	}
	if freshEpoch, _ := p.observe(freshGeneration, body); freshEpoch != epoch {
		// The camera stream restarted: this sequence number now names different
		// media, so let FFmpeg skip the segment instead of splicing wrong footage.
		return "", 0, false, true
	}
	base, err := url.Parse(mediaURL)
	if err != nil {
		return "", 0, false, true
	}
	found, ok := hlsURIForSequence(body, seq, tag)
	if !ok {
		return "", 0, false, true
	}
	ref, err := url.Parse(found)
	if err != nil {
		return "", 0, false, true
	}
	abs := base.ResolveReference(ref)
	if !p.sameOrigin(abs) {
		return "", 0, false, true
	}
	return abs.String(), freshGeneration, true, true
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

// hlsSessionTransientStatus is a refusal worth retrying briefly: an expired
// or CDN-refused request, a throttle, or an upstream server error.
func hlsSessionTransientStatus(status int) bool {
	return hlsSessionExpiredStatus(status) || status == http.StatusRequestTimeout ||
		status == http.StatusTooManyRequests || status >= http.StatusInternalServerError
}

func sleepContext(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// hlsTagName returns "EXT-X-KEY" for "#EXT-X-KEY:METHOD=...".
func hlsTagName(line string) string {
	name, _, _ := strings.Cut(strings.TrimPrefix(line, "#"), ":")
	return name
}

// hlsURIForSequence finds the URI of media sequence want in a media playlist.
// With a tag name it instead returns the URI of the last such tag (a key or
// init map) in effect for that segment.
func hlsURIForSequence(body []byte, want int64, tag string) (string, bool) {
	seq := int64(0)
	tagURI := ""
	for _, line := range strings.Split(string(body), "\n") {
		line = strings.TrimSpace(line)
		switch {
		case line == "":
		case strings.HasPrefix(line, "#EXT-X-MEDIA-SEQUENCE:"):
			if n, err := strconv.ParseInt(strings.TrimPrefix(line, "#EXT-X-MEDIA-SEQUENCE:"), 10, 64); err == nil {
				seq = n
			}
		case strings.HasPrefix(line, "#"):
			if tag != "" && hlsTagName(line) == tag {
				if m := hlsSessionURIAttr.FindStringSubmatch(line); m != nil {
					tagURI = m[1]
				}
			}
		default:
			if seq == want {
				if tag == "" {
					return line, true
				}
				return tagURI, tagURI != ""
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
