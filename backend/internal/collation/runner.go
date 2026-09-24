package collation

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/daydemir/stoarama/backend/internal/r2"
)

// R2Store implements Store on the joined bucket. Sources must live in the same
// bucket as the joined layout.
type R2Store struct{ Client *r2.Client }

func (s R2Store) Download(ctx context.Context, clip Clip, dst string) (int64, string, error) {
	if clip.Bucket != s.Client.Bucket() {
		return 0, "", fmt.Errorf("clip %d lives in bucket %q, not %q", clip.ClipID, clip.Bucket, s.Client.Bucket())
	}
	var lastErr error
	for attempt := 0; attempt < 4; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(attempt*attempt) * time.Second)
		}
		size, sha, err := s.downloadOnce(ctx, clip.ObjectKey, dst)
		if err == nil || r2.IsNotFound(err) || ctx.Err() != nil {
			return size, sha, err
		}
		lastErr = err
	}
	return 0, "", lastErr
}

func (s R2Store) downloadOnce(ctx context.Context, key, dst string) (int64, string, error) {
	rc, err := s.Client.Open(ctx, key)
	if err != nil {
		return 0, "", err
	}
	defer rc.Close()
	f, err := os.OpenFile(dst, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return 0, "", err
	}
	h := sha256.New()
	n, copyErr := io.Copy(io.MultiWriter(f, h), rc)
	closeErr := f.Close()
	if err := errors.Join(copyErr, closeErr); err != nil {
		return 0, "", err
	}
	return n, hex.EncodeToString(h.Sum(nil)), nil
}

func (s R2Store) PublishVerified(ctx context.Context, key, contentType, path string, size int64, sha string) error {
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(attempt*2) * time.Second)
		}
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		_, _, err = s.Client.PutReaderIfAbsentVerified(ctx, key, contentType, f, size, sha)
		f.Close()
		if err == nil || ctx.Err() != nil {
			return err
		}
		lastErr = err
	}
	return lastErr
}

func (s R2Store) PutManifestIfAbsent(ctx context.Context, key string, body []byte) error {
	sum := sha256.Sum256(body)
	_, _, err := s.Client.PutReaderIfAbsentVerified(ctx, key, "application/json", bytes.NewReader(body), int64(len(body)), hex.EncodeToString(sum[:]))
	return err
}

// Exists reports whether key is already published.
func (s R2Store) Exists(ctx context.Context, key string) (bool, error) {
	_, err := s.Client.Head(ctx, key)
	if err == nil {
		return true, nil
	}
	if r2.IsNotFound(err) {
		return false, nil
	}
	return false, err
}

// ReadWorklist parses a JSONL worklist of HourWork.
func ReadWorklist(r io.Reader) ([]HourWork, error) {
	var out []HourWork
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 1<<20), 64<<20)
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		var w HourWork
		if err := json.Unmarshal(line, &w); err != nil {
			return nil, fmt.Errorf("worklist line %d: %w", len(out)+1, err)
		}
		out = append(out, w)
	}
	return out, sc.Err()
}

// HourResult is one line of the runner's results log.
type HourResult struct {
	HourID       string         `json:"hour_id"`
	RecordingID  int64          `json:"recording_id"`
	Outcome      string         `json:"outcome"` // published | skipped_existing | deferred | failed
	Status       string         `json:"status,omitempty"`
	Clips        int            `json:"clips"`
	Parts        int            `json:"parts"`
	Joins        int            `json:"joins"`
	Splits       int            `json:"splits"`
	Quarantined  int            `json:"quarantined"`
	OutputBytes  int64          `json:"output_bytes"`
	Seconds      float64        `json:"seconds"`
	Error        string         `json:"error,omitempty"`
	FinishedAt   time.Time      `json:"finished_at"`
	ManifestKey  string         `json:"manifest_key,omitempty"`
	SplitReasons map[string]int `json:"split_reasons,omitempty"`
}

// RunWorklist processes hours with bounded parallelism, skipping hours whose
// manifest is already published. It is safe to rerun after any interruption.
// ExistenceChecker reports whether an hour's manifest is already published.
type ExistenceChecker interface {
	Exists(ctx context.Context, key string) (bool, error)
}

func RunWorklist(ctx context.Context, env Env, store ExistenceChecker, work []HourWork, hourWorkers int, results io.Writer) error {
	if hourWorkers <= 0 {
		return fmt.Errorf("hourWorkers must be > 0, got %d", hourWorkers)
	}
	jobs := make(chan HourWork)
	var mu sync.Mutex
	enc := json.NewEncoder(results)
	emit := func(r HourResult) {
		mu.Lock()
		defer mu.Unlock()
		_ = enc.Encode(r)
		log.Printf("collation hour=%s outcome=%s status=%s parts=%d joins=%d splits=%d secs=%.1f err=%s", r.HourID, r.Outcome, r.Status, r.Parts, r.Joins, r.Splits, r.Seconds, r.Error)
	}
	var wg sync.WaitGroup
	for i := 0; i < hourWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for w := range jobs {
				started := time.Now()
				res := HourResult{HourID: w.HourID, RecordingID: w.RecordingID, Clips: len(w.Clips), ManifestKey: ManifestKey(w.BatchID, w.HourID)}
				exists, err := store.Exists(ctx, res.ManifestKey)
				switch {
				case err != nil:
					res.Outcome, res.Error = "failed", err.Error()
				case exists:
					res.Outcome = "skipped_existing"
				default:
					m, err := ProcessHour(ctx, env, w)
					switch {
					case errors.Is(err, ErrDeferred):
						res.Outcome = "deferred"
					case err != nil:
						res.Outcome, res.Error = "failed", err.Error()
					default:
						res.Outcome, res.Status = "published", m.Status
						res.Parts = len(m.Outputs)
						res.SplitReasons = map[string]int{}
						for _, s := range m.Seams {
							if s.Decision == DecisionJoin {
								res.Joins++
							} else {
								res.Splits++
								res.SplitReasons[s.Reason]++
							}
						}
						for _, c := range m.Clips {
							if c.Disposition != "included" {
								res.Quarantined++
							}
						}
						for _, o := range m.Outputs {
							res.OutputBytes += o.SizeBytes
						}
					}
				}
				res.Seconds = time.Since(started).Seconds()
				res.FinishedAt = time.Now().UTC()
				emit(res)
			}
		}()
	}
	for _, w := range work {
		select {
		case jobs <- w:
		case <-ctx.Done():
		}
		if ctx.Err() != nil {
			break
		}
	}
	close(jobs)
	wg.Wait()
	return ctx.Err()
}

// LocalPublishStore reads sources from R2 but publishes outputs and manifests
// under a local directory (same key layout). Used for canaries and dry runs.
type LocalPublishStore struct {
	R2Store
	Dir string
}

func (s LocalPublishStore) PublishVerified(_ context.Context, key, _, path string, size int64, sha string) error {
	dst := filepath.Join(s.Dir, filepath.FromSlash(key))
	if err := os.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
		return err
	}
	if err := os.Rename(path, dst); err != nil {
		return err
	}
	n, got, err := fileIdentity(dst)
	if err != nil || n != size || got != sha {
		return fmt.Errorf("local publish identity differs")
	}
	return nil
}

func (s LocalPublishStore) PutManifestIfAbsent(_ context.Context, key string, body []byte) error {
	dst := filepath.Join(s.Dir, filepath.FromSlash(key))
	if err := os.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
		return err
	}
	f, err := os.OpenFile(dst, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(body); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

func (s LocalPublishStore) Exists(_ context.Context, key string) (bool, error) {
	_, err := os.Stat(filepath.Join(s.Dir, filepath.FromSlash(key)))
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}
