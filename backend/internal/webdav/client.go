// Package webdav is a minimal WebDAV-over-HTTPS object store client built on
// net/http (no third-party dependency). It exposes exactly the verbs the clip
// transfer worker and the connection check need: PUT (streamed write), HEAD
// (size), GET (read-back), DELETE (idempotent purge), and MKCOL (create the
// destination collection chain). It is the WebDAV arm of the objectStore seam:
// an S3/R2 recording presigns and uploads as before, while a recording targeting
// a WebDAV destination stages in managed R2 and is then transferred here.
//
// Transport: every request uses HTTP Basic auth and an https endpoint (a generic
// WebDAV root or a Synology QuickConnect WebDAV URL, which relays DSM's WebDAV
// service over normal HTTPS). The dialer installs netguard.ControlReject so a
// QuickConnect relay redirect cannot be steered at a private/loopback/metadata
// address (SSRF / DNS-rebinding guard). TLS is validated normally; there is no
// InsecureSkipVerify fallback.
package webdav

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/netguard"
)

// requestTimeout bounds a single WebDAV request. It must outlast a multi-GB PUT
// streamed body, so it is generous; the clip-transfer worker's lease is the real
// upper bound on a transfer.
const requestTimeout = 30 * time.Minute

// Client is a WebDAV destination: a base URL, Basic-auth credentials, and a base
// path under which object keys are written. It is safe for concurrent use.
type Client struct {
	httpClient *http.Client
	baseURL    *url.URL
	user       string
	pass       string
	basePath   string
}

// Config constructs a Client. Endpoint is the base WebDAV URL (https only). User
// and Pass are the WebDAV/DSM credentials. BasePath is the directory under the
// endpoint that object keys are written beneath (the destination's key_prefix).
type Config struct {
	Endpoint string
	User     string
	Pass     string
	BasePath string
}

// ObjectHead mirrors r2.ObjectHead's shape for the size the worker records. ETag
// is empty for WebDAV (no strong content hash is guaranteed by the protocol).
type ObjectHead struct {
	SizeBytes int64
}

// New parses and validates the endpoint (https required), then builds a Client
// whose http.Client refuses to dial a non-public address (guards QuickConnect
// relay redirects against SSRF/rebind).
func New(cfg Config) (*Client, error) {
	raw := strings.TrimSpace(cfg.Endpoint)
	if raw == "" {
		return nil, fmt.Errorf("webdav endpoint is empty")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("parse webdav endpoint: %w", err)
	}
	if strings.ToLower(u.Scheme) != "https" {
		return nil, fmt.Errorf("webdav endpoint must be https, got %q", u.Scheme)
	}
	if u.Host == "" {
		return nil, fmt.Errorf("webdav endpoint has no host")
	}
	if u.User != nil {
		return nil, fmt.Errorf("webdav endpoint must not contain embedded credentials")
	}
	user := strings.TrimSpace(cfg.User)
	if user == "" {
		return nil, fmt.Errorf("webdav username is required")
	}
	if strings.TrimSpace(cfg.Pass) == "" {
		return nil, fmt.Errorf("webdav password is required")
	}

	transport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout: 30 * time.Second,
			Control: netguard.ControlReject,
		}).DialContext,
		ForceAttemptHTTP2:   true,
		MaxIdleConns:        4,
		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: 15 * time.Second,
	}
	return &Client{
		httpClient: &http.Client{Transport: transport, Timeout: requestTimeout},
		baseURL:    u,
		user:       user,
		pass:       cfg.Pass,
		basePath:   strings.Trim(strings.TrimSpace(cfg.BasePath), "/"),
	}, nil
}

// objectURL builds the absolute URL for a key under basePath, escaping each path
// segment so a user-controlled key can never inject query/fragment or traverse.
func (c *Client) objectURL(key string) string {
	return c.baseURL.ResolveReference(&url.URL{Path: c.objectPath(key)}).String()
}

// objectPath returns the full URL path (joined under the endpoint path + basePath)
// with each segment individually escaped. Empty/"."/".." segments are dropped so a
// stored prefix or key cannot traverse out of the base path.
func (c *Client) objectPath(key string) string {
	segs := make([]string, 0, 8)
	add := func(s string) {
		for _, seg := range strings.Split(s, "/") {
			if seg == "" || seg == "." || seg == ".." {
				continue
			}
			segs = append(segs, url.PathEscape(seg))
		}
	}
	add(c.baseURL.Path)
	add(c.basePath)
	add(key)
	return "/" + strings.Join(segs, "/")
}

// ancestorDirs returns the collection URLs that must exist before a PUT of key:
// every directory from basePath down to (but excluding) the object itself.
func (c *Client) ancestorDirs(key string) []string {
	segs := make([]string, 0, 8)
	add := func(s string) {
		for _, seg := range strings.Split(s, "/") {
			if seg == "" || seg == "." || seg == ".." {
				continue
			}
			segs = append(segs, url.PathEscape(seg))
		}
	}
	add(c.baseURL.Path)
	add(c.basePath)
	add(key)
	// Drop the final segment (the object file itself).
	if len(segs) > 0 {
		segs = segs[:len(segs)-1]
	}
	dirs := make([]string, 0, len(segs))
	for i := 1; i <= len(segs); i++ {
		p := "/" + strings.Join(segs[:i], "/") + "/"
		dirs = append(dirs, c.baseURL.ResolveReference(&url.URL{Path: p}).String())
	}
	return dirs
}

func (c *Client) do(req *http.Request) (*http.Response, error) {
	req.SetBasicAuth(c.user, c.pass)
	return c.httpClient.Do(req)
}

// Mkcol creates the collection chain for key (every ancestor directory). 201
// (created) and 405 (already a collection) are both success; most WebDAV servers
// will not auto-create parents on PUT, so this is called before the first PUT and
// in the connection check.
func (c *Client) Mkcol(ctx context.Context, key string) error {
	for _, dir := range c.ancestorDirs(key) {
		req, err := http.NewRequestWithContext(ctx, "MKCOL", dir, nil)
		if err != nil {
			return fmt.Errorf("build MKCOL request: %w", err)
		}
		resp, err := c.do(req)
		if err != nil {
			return fmt.Errorf("MKCOL %s: %w", dir, err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		// 201 created; 405 method-not-allowed and 301/308 mean the collection
		// already exists. Anything else is a real failure.
		switch resp.StatusCode {
		case http.StatusCreated, http.StatusMethodNotAllowed, http.StatusMovedPermanently, http.StatusPermanentRedirect:
		default:
			return fmt.Errorf("MKCOL %s: unexpected status %d", dir, resp.StatusCode)
		}
	}
	return nil
}

// PutMultipart streams body to key with a single WebDAV PUT (WebDAV has no
// multipart; the name matches the objectStore seam so r2.Client and this client
// share one interface). It creates the ancestor collections first, then PUTs the
// reader as the request body (no full-object buffering). The returned ETag is
// empty: the worker re-Heads for the real size and never uses a WebDAV PUT ETag.
func (c *Client) PutMultipart(ctx context.Context, key, contentType string, body io.Reader) (string, error) {
	if err := c.Mkcol(ctx, key); err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.objectURL(key), body)
	if err != nil {
		return "", fmt.Errorf("build PUT request: %w", err)
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := c.do(req)
	if err != nil {
		return "", fmt.Errorf("PUT %s: %w", key, err)
	}
	defer func() { _, _ = io.Copy(io.Discard, resp.Body); _ = resp.Body.Close() }()
	switch resp.StatusCode {
	case http.StatusOK, http.StatusCreated, http.StatusNoContent:
		return "", nil
	default:
		return "", fmt.Errorf("PUT %s: unexpected status %d", key, resp.StatusCode)
	}
}

// Head returns the object's size via a HEAD (Content-Length). A 404 surfaces as an
// error so callers can tell "not delivered" from "size N".
func (c *Client) Head(ctx context.Context, key string) (ObjectHead, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, c.objectURL(key), nil)
	if err != nil {
		return ObjectHead{}, fmt.Errorf("build HEAD request: %w", err)
	}
	resp, err := c.do(req)
	if err != nil {
		return ObjectHead{}, fmt.Errorf("HEAD %s: %w", key, err)
	}
	defer func() { _, _ = io.Copy(io.Discard, resp.Body); _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return ObjectHead{}, fmt.Errorf("HEAD %s: unexpected status %d", key, resp.StatusCode)
	}
	if resp.ContentLength < 0 {
		return ObjectHead{}, fmt.Errorf("HEAD %s: server did not report Content-Length", key)
	}
	return ObjectHead{SizeBytes: resp.ContentLength}, nil
}

// Open GETs the object body. It is used only by the connection-check read-back; a
// transfer's source is always managed R2, never a WebDAV destination.
func (c *Client) Open(ctx context.Context, key string) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.objectURL(key), nil)
	if err != nil {
		return nil, fmt.Errorf("build GET request: %w", err)
	}
	resp, err := c.do(req)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", key, err)
	}
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		return nil, fmt.Errorf("GET %s: unexpected status %d", key, resp.StatusCode)
	}
	return resp.Body, nil
}

// DeleteObjects DELETEs each key. A 404 is treated as success so the auto-purge of
// a managed staging copy is idempotent (matches r2.Client.DeleteObjects' shape so
// both satisfy the objectStore seam). On WebDAV this deletes the object on the NAS;
// for the managed-staging purge the worker calls the source r2 client instead.
func (c *Client) DeleteObjects(ctx context.Context, keys []string) error {
	for _, key := range keys {
		if strings.TrimSpace(key) == "" {
			return fmt.Errorf("delete object key is empty")
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.objectURL(key), nil)
		if err != nil {
			return fmt.Errorf("build DELETE request: %w", err)
		}
		resp, err := c.do(req)
		if err != nil {
			return fmt.Errorf("DELETE %s: %w", key, err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		switch resp.StatusCode {
		case http.StatusOK, http.StatusNoContent, http.StatusNotFound:
		default:
			return fmt.Errorf("DELETE %s: unexpected status %d", key, resp.StatusCode)
		}
	}
	return nil
}

// Probe is the connection check: ensure the base collection chain exists, then
// write, read back, and delete a tiny probe object. It proves the credentials can
// authenticate and the destination is writable, mirroring the S3
// verifyStorageDestination round-trip.
func (c *Client) Probe(ctx context.Context, probeName string) error {
	key := ".stoarama-verify/" + probeName
	const payload = "stoarama webdav destination verification"
	if _, err := c.PutMultipart(ctx, key, "text/plain", strings.NewReader(payload)); err != nil {
		return fmt.Errorf("write probe object: %w", err)
	}
	rc, err := c.Open(ctx, key)
	if err != nil {
		_ = c.DeleteObjects(ctx, []string{key})
		return fmt.Errorf("read probe object: %w", err)
	}
	got, readErr := io.ReadAll(rc)
	_ = rc.Close()
	if readErr != nil {
		_ = c.DeleteObjects(ctx, []string{key})
		return fmt.Errorf("read probe object body: %w", readErr)
	}
	if string(got) != payload {
		_ = c.DeleteObjects(ctx, []string{key})
		return fmt.Errorf("probe read-back mismatch")
	}
	if err := c.DeleteObjects(ctx, []string{key}); err != nil {
		return fmt.Errorf("delete probe object: %w", err)
	}
	return nil
}
