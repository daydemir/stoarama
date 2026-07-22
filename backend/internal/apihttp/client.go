package apihttp

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

type Client struct {
	baseURL string
	token   string
	httpc   *http.Client
}

type StatusError struct {
	Label string
	Code  int
	Body  string
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("%s status=%d body=%s", e.Label, e.Code, e.Body)
}

func New(baseURL, token string, httpc *http.Client, timeout time.Duration) (*Client, error) {
	base := strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if base == "" {
		return nil, fmt.Errorf("missing BaseURL")
	}
	tok := strings.TrimSpace(token)
	if tok == "" {
		return nil, fmt.Errorf("missing token")
	}
	if httpc == nil {
		httpc = &http.Client{Timeout: timeout}
	}
	return &Client{baseURL: base, token: tok, httpc: httpc}, nil
}

func (c *Client) PostJSON(ctx context.Context, path string, payload any, out any) error {
	return c.PostJSONWithHeaders(ctx, path, payload, nil, out)
}

func (c *Client) PostJSONWithHeaders(ctx context.Context, path string, payload any, headers map[string]string, out any) error {
	status, body, err := c.PostRawWithHeaders(ctx, path, payload, headers)
	if err != nil {
		return err
	}
	if status < 200 || status >= 300 {
		return fmt.Errorf("request %s status=%d body=%s", path, status, strings.TrimSpace(string(body)))
	}
	if out == nil || len(bytes.TrimSpace(body)) == 0 {
		return nil
	}
	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("decode %s response: %w", path, err)
	}
	return nil
}

func (c *Client) PostRaw(ctx context.Context, path string, payload any) (int, []byte, error) {
	return c.PostRawWithHeaders(ctx, path, payload, nil)
}

func (c *Client) PostRawWithHeaders(ctx context.Context, path string, payload any, headers map[string]string) (int, []byte, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return 0, nil, fmt.Errorf("marshal request payload: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return 0, nil, fmt.Errorf("build request %s: %w", path, err)
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	for k, v := range headers {
		if strings.TrimSpace(k) != "" && strings.TrimSpace(v) != "" {
			req.Header.Set(k, v)
		}
	}
	resp, err := c.httpc.Do(req)
	if err != nil {
		return 0, nil, fmt.Errorf("request %s failed: %w", path, err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 16*1024))
	return resp.StatusCode, respBody, nil
}

func (c *Client) PutFile(ctx context.Context, uploadURL, path, mimeType string) error {
	if strings.TrimSpace(uploadURL) == "" {
		return fmt.Errorf("upload_url is required")
	}
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open upload file: %w", err)
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat upload file: %w", err)
	}
	return c.put(ctx, uploadURL, f, st.Size(), mimeType, "upload file")
}

func (c *Client) PutBytes(ctx context.Context, uploadURL string, body []byte, mimeType string) error {
	return c.put(ctx, uploadURL, bytes.NewReader(body), int64(len(body)), mimeType, "upload segment")
}

func (c *Client) put(ctx context.Context, uploadURL string, body io.Reader, length int64, mimeType, label string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, uploadURL, body)
	if err != nil {
		return fmt.Errorf("build %s request: %w", label, err)
	}
	req.ContentLength = length
	if strings.TrimSpace(mimeType) != "" {
		req.Header.Set("Content-Type", strings.TrimSpace(mimeType))
	}
	resp, err := c.httpc.Do(req)
	if err != nil {
		return fmt.Errorf("%s failed: %w", label, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 16*1024))
		return &StatusError{Label: label, Code: resp.StatusCode, Body: strings.TrimSpace(string(body))}
	}
	return nil
}

func IdempotencyKey(prefix string, id int64) string {
	p := strings.TrimSpace(prefix)
	if p == "" {
		p = "request"
	}
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return fmt.Sprintf("%s-%d-%d", p, id, time.Now().UTC().UnixNano())
	}
	return fmt.Sprintf("%s-%d-%x", p, id, buf[:])
}
