// stoarama-collation-job runs one frozen collation hour using expiring source
// capabilities. It holds no bucket credentials and stages outputs locally.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/daydemir/stoarama/backend/internal/collation"
)

type capability struct {
	URL     string            `json:"url"`
	Headers map[string]string `json:"headers"`
}
type request struct {
	Work           collation.HourWork    `json:"work"`
	Sources        map[string]capability `json:"sources"`
	TimeoutSeconds int                   `json:"timeout_seconds"`
}
type proof struct {
	Bytes            int64     `json:"bytes"`
	SHA256           string    `json:"sha256"`
	StrictFullDecode bool      `json:"strict_full_decode"`
	VerifiedAt       time.Time `json:"verified_at"`
}
type store struct {
	publishMu sync.Mutex
	root      string
	caps      map[string]capability
	client    *http.Client
	tools     collation.Tools
	proofs    map[string]proof
}

func safeDestination(root, key string) (string, error) {
	if !strings.HasPrefix(key, "joined/best14-20261021/") || strings.Contains(key, "\\") || filepath.IsAbs(key) {
		return "", errors.New("output namespace refused")
	}
	clean := filepath.Clean(key)
	if clean != key || strings.HasPrefix(clean, "../") {
		return "", errors.New("output path refused")
	}
	return filepath.Join(root, key), nil
}
func (s *store) Download(ctx context.Context, c collation.Clip, dst string) (int64, string, error) {
	cap, ok := s.caps[fmt.Sprint(c.ClipID)]
	if !ok {
		return 0, "", errors.New("source capability missing")
	}
	u, err := url.Parse(cap.URL)
	if err != nil || u.Scheme != "https" || !strings.HasSuffix(u.Hostname(), ".r2.cloudflarestorage.com") || u.User != nil {
		return 0, "", errors.New("source endpoint refused")
	}
	req, err := http.NewRequestWithContext(ctx, "GET", cap.URL, nil)
	if err != nil {
		return 0, "", errors.New("source request invalid")
	}
	for k, v := range cap.Headers {
		req.Header.Set(k, v)
	}
	res, err := s.client.Do(req)
	if err != nil {
		return 0, "", errors.New("source download failed")
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK || res.ContentLength != c.SizeBytes {
		return 0, "", errors.New("source status or size differs")
	}
	f, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return 0, "", err
	}
	h := sha256.New()
	n, copyErr := io.Copy(io.MultiWriter(f, h), io.LimitReader(res.Body, c.SizeBytes+1))
	closeErr := f.Close()
	if err := errors.Join(copyErr, closeErr); err != nil {
		return 0, "", errors.New("source write failed")
	}
	if n != c.SizeBytes {
		return n, "", errors.New("source body size differs")
	}
	return n, hex.EncodeToString(h.Sum(nil)), nil
}
func (s *store) PublishVerified(ctx context.Context, key, contentType, path string, size int64, sha string) error {
	s.publishMu.Lock()
	defer s.publishMu.Unlock()
	dst, err := safeDestination(s.root, key)
	if err != nil {
		return err
	}
	// Full strict decoding supplements the library's packet identity, timing,
	// seam proof and edge decode. Publication qualification remains separate.
	if err := collation.VerifyFullDecode(ctx, s.tools, path); err != nil {
		return errors.New("strict full output media decode failed")
	}

	if err := os.MkdirAll(filepath.Dir(dst), 0700); err != nil {
		return err
	}
	in, err := os.Open(path)
	if err != nil {
		return err
	}
	defer in.Close()
	f, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	h := sha256.New()
	n, copyErr := io.Copy(io.MultiWriter(f, h), in)
	closeErr := f.Close()
	if err := errors.Join(copyErr, closeErr); err != nil {
		return err
	}
	if n != size || hex.EncodeToString(h.Sum(nil)) != sha {
		return errors.New("staged output identity differs")
	}
	s.proofs[key] = proof{n, sha, true, time.Now().UTC()}
	return nil
}

// ProcessHour calls this only after staging outputs. A normal R2 coverage
// manifest is deliberately not created by this staging-only worker.
func (s *store) PutManifestIfAbsent(ctx context.Context, key string, body []byte) error {
	if _, err := safeDestination(s.root, key); err != nil {
		return err
	}
	var m collation.HourManifest
	if err := json.Unmarshal(body, &m); err != nil {
		return errors.New("invalid staged manifest")
	}
	return m.Validate()
}

func run() error {
	input := flag.String("input", "", "job JSON with expiring source requests")
	root := flag.String("root", "", "new job-owned directory")
	flag.Parse()
	if *input == "" || *root == "" {
		return errors.New("input and root required")
	}
	file, err := os.Open(*input)
	if err != nil {
		return err
	}
	defer file.Close()
	body, err := io.ReadAll(io.LimitReader(file, (4<<20)+1))
	if err != nil {
		return err
	}
	if len(body) > 4<<20 {
		return errors.New("job metadata too large")
	}
	var r request
	if err := json.Unmarshal(body, &r); err != nil {
		return errors.New("invalid job metadata")
	}
	if r.Work.BatchID != "best14-20261021" || r.TimeoutSeconds < 30 || r.TimeoutSeconds > 1800 {
		return errors.New("job scope or deadline invalid")
	}
	want, err := collation.HourIDFor(r.Work.BatchID, r.Work.RecordingID, r.Work.LocalDate, r.Work.DeliveryHour)
	if err != nil || want != r.Work.HourID {
		return errors.New("job identity invalid")
	}
	if len(r.Work.Clips) < 1 || len(r.Work.Clips) > 240 || !r.Work.ScheduledEnd.After(r.Work.ScheduledStart) || r.Work.ScheduledEnd.Sub(r.Work.ScheduledStart) > time.Hour {
		return errors.New("source or scheduled-window count invalid")
	}
	seen := map[int64]bool{}
	var total int64
	for _, c := range r.Work.Clips {
		digest, e := hex.DecodeString(c.SHA256)
		if c.ClipID <= 0 || seen[c.ClipID] || c.RecordingID != r.Work.RecordingID || c.SizeBytes <= 0 || c.SizeBytes > 1<<30 || e != nil || len(digest) != 32 || c.StartUTC.Before(r.Work.ScheduledStart) || !c.StartUTC.Before(r.Work.ScheduledEnd) || !c.EndUTC.After(c.StartUTC) || c.EndUTC.Sub(c.StartUTC) > 15*time.Minute {
			return errors.New("frozen source shape invalid")
		}
		seen[c.ClipID] = true
		total += c.SizeBytes
	}
	if total > 8<<30 || len(r.Sources) != len(r.Work.Clips) {
		return errors.New("source capacity or accounting invalid")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(r.TimeoutSeconds)*time.Second)
	defer cancel()
	if err := os.Mkdir(*root, 0700); err != nil {
		return errors.New("job root must be new")
	}
	tools := collation.ToolsFromEnv()
	version, err := tools.Version(ctx)
	if err != nil {
		return err
	}
	st := &store{root: filepath.Join(*root, "outputs"), caps: r.Sources, tools: tools, proofs: map[string]proof{}, client: &http.Client{Timeout: 2 * time.Minute, CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("source redirect refused") }}}
	env := collation.Env{Tools: tools, Policy: collation.DefaultSeamPolicy(), ScratchRoot: filepath.Join(*root, "scratch"), Store: st, MediaTool: version, CPU: make(chan struct{}, 2), Net: make(chan struct{}, 8), Now: time.Now}
	m, err := collation.ProcessHour(ctx, env, r.Work)
	if err != nil {
		return err
	}
	for i := range m.Outputs {
		m.Outputs[i].R2VerifiedAt = time.Time{}
	}
	result := struct {
		Manifest       collation.HourManifest `json:"manifest"`
		Proofs         map[string]proof       `json:"full_decode_proofs"`
		ReadyToPublish bool                   `json:"ready_to_publish"`
		Sources        int                    `json:"planned_sources"`
	}{m, st.proofs, false, len(r.Work.Clips)}
	b, err := json.Marshal(result)
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(*root, "result.json"), b, 0600); err != nil {
		return err
	}
	fmt.Printf("status=%s outputs=%d planned_sources=%d ready_to_publish=false\n", m.Status, len(m.Outputs), len(r.Work.Clips))
	return nil
}
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "job failed:", err)
		os.Exit(1)
	}
}
