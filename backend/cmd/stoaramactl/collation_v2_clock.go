package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/daydemir/stoarama/backend/internal/clockjoin"
	"github.com/daydemir/stoarama/backend/internal/collation"
	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/r2"
)

const clockJoinUsage = `usage:
  stoaramactl collation-v2 clock-join --hours FILE (--source-dir DIR | --source-r2) --registry KEY --profiles FILE --veto-clips FILE --batch clock-prestitch-YYYYMMDD --out DIR [--veto-recordings 433,...]
    FILE (--hours) lines: joined/<batch>/coverage/hours/<hour_id>.json manifest keys.
    --source-dir reads a tree laid out by R2 key (collation-v2 run --publish-dir);
    --source-r2 reads R2 with a client that has no write admission.
    --veto-clips is the known-bad clip list (one id per line); it is required, and may be empty.
    --registry is the parent delivery generation's key (datasets/global-street-scores-delivery/47/public/generations/<id>.json);
    its growth.json is read from the key the registry lists, sha256-checked.
    Writes only under --out: joined/<batch>/objects|proofs/ and results/<hour_id>.json,
    each create-only. An hour whose result exists is skipped.
  stoaramactl collation-v2 clock-read --profiles FILE --recording ID --file PART.mp4
    Prints the head and tail clock reads of one part (crop tuning).`

func runClockJoin(ctx context.Context, cfg config.Config, args []string) {
	fs := flag.NewFlagSet("collation-v2 clock-join", flag.ExitOnError)
	hours := fs.String("hours", "", "file of hour manifest keys")
	sourceDir := fs.String("source-dir", "", "local tree laid out by R2 key")
	sourceR2 := fs.Bool("source-r2", false, "read R2 (no write admission)")
	registryKey := fs.String("registry", "", "parent delivery generation registry key")
	profilesPath := fs.String("profiles", "", "JSON array of clock profiles")
	batch := fs.String("batch", "", "clock-prestitch-YYYYMMDD")
	out := fs.String("out", "", "output directory")
	vetoClips := fs.String("veto-clips", "", "file of known-bad clip ids (one per line; required, may be empty)")
	vetoRecordings := fs.String("veto-recordings", "", "comma-separated known-bad recording ids")
	_ = fs.Parse(args)
	if *hours == "" || (*sourceDir == "") == !*sourceR2 || *vetoClips == "" || *registryKey == "" || *profilesPath == "" || !clockjoin.ValidBatch(*batch) || *out == "" {
		log.Fatal(clockJoinUsage)
	}
	keys, err := readManifestKeys(*hours)
	if err != nil {
		log.Fatal(err)
	}
	profiles, err := readClockProfiles(*profilesPath)
	if err != nil {
		log.Fatal(err)
	}
	var src clockjoin.Source = clockjoin.DirSource{Root: *sourceDir}
	if *sourceR2 {
		src = readOnlyR2Source(ctx, cfg)
	}
	env, err := clockParentGeneration(ctx, src, *registryKey)
	if err != nil {
		log.Fatal(err)
	}
	if _, err := os.Stat(*vetoClips); err != nil {
		log.Fatal(err) // an absent list is not an empty one
	}
	if env.VetoClips, err = readIDSet(*vetoClips); err != nil {
		log.Fatal(err)
	}
	if env.VetoRecordings, err = parseIDList(*vetoRecordings); err != nil {
		log.Fatal(err)
	}
	env.Profiles, env.Batch, env.Out, env.Source = profiles, *batch, *out, src
	env.Tools = collation.ToolsFromEnv()
	env.Reader = clockjoin.Reader{Tools: env.Tools, Tesseract: tesseractBin()}
	if env.MediaTool, err = env.Tools.Version(ctx); err != nil {
		log.Fatal(err)
	}
	if env.ClockTool, err = env.Reader.Version(ctx); err != nil {
		log.Fatal(err)
	}
	if err := os.MkdirAll(*out, 0o700); err != nil {
		log.Fatal(err)
	}
	summary := clockJoinSummary{Statuses: map[clockjoin.HourStatus]int{}, Reasons: map[clockjoin.Reason]int{}}
	for _, key := range keys {
		hourID := strings.TrimSuffix(path.Base(key), ".json")
		if _, err := os.Stat(filepath.Join(*out, filepath.FromSlash(clockjoin.ResultKey(hourID)))); err == nil {
			summary.SkippedExisting++
			continue
		}
		res, err := clockjoin.ProcessHour(ctx, env, key)
		if err == nil {
			err = clockjoin.WriteResult(*out, res)
		}
		if err != nil {
			if ctx.Err() != nil {
				log.Fatal(ctx.Err())
			}
			summary.Failed++
			log.Printf("clock-join hour=%s failed: %v", hourID, err)
			continue
		}
		summary.add(res)
		log.Printf("clock-join hour=%s status=%s seams=%d assemblies=%d", hourID, res.Status, len(res.Seams), len(res.Assemblies))
	}
	body, _ := json.Marshal(summary)
	fmt.Println(string(body))
	if summary.Failed > 0 {
		log.Fatalf("%d hours failed; rerun to retry them", summary.Failed)
	}
}

type clockJoinSummary struct {
	Hours           int                          `json:"hours"`
	SkippedExisting int                          `json:"skipped_existing"`
	Failed          int                          `json:"failed"`
	Statuses        map[clockjoin.HourStatus]int `json:"statuses"`
	Seams           int                          `json:"seams"`
	Joins           int                          `json:"joins"`
	Reasons         map[clockjoin.Reason]int     `json:"reasons"`
	Assemblies      int                          `json:"assemblies"`
	PartsReplaced   int                          `json:"parts_replaced"`
	AssemblyBytes   int64                        `json:"assembly_bytes"`
}

func (s *clockJoinSummary) add(r clockjoin.HourResult) {
	s.Hours++
	s.Statuses[r.Status]++
	for _, v := range r.Seams {
		s.Seams++
		s.Reasons[v.Reason]++
		if v.Join {
			s.Joins++
		}
	}
	for _, a := range r.Assemblies {
		s.Assemblies++
		s.PartsReplaced += a.InputParts
		s.AssemblyBytes += a.SizeBytes
	}
}

func runClockRead(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("collation-v2 clock-read", flag.ExitOnError)
	profilesPath := fs.String("profiles", "", "JSON array of clock profiles")
	recording := fs.Int64("recording", 0, "recording id")
	file := fs.String("file", "", "local part")
	_ = fs.Parse(args)
	if *profilesPath == "" || *recording <= 0 || *file == "" {
		log.Fatal(clockJoinUsage)
	}
	profiles, err := readClockProfiles(*profilesPath)
	if err != nil {
		log.Fatal(err)
	}
	p, ok := profiles[*recording]
	if !ok {
		log.Fatalf("no clock profile for recording %d", *recording)
	}
	tools := collation.ToolsFromEnv()
	scratch, err := os.MkdirTemp("", "clock-read-")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(scratch)
	reader := clockjoin.Reader{Tools: tools, Tesseract: tesseractBin(), Scratch: scratch}
	enc := json.NewEncoder(os.Stdout)
	for _, e := range []clockjoin.Edge{clockjoin.Head, clockjoin.Tail} {
		r, err := reader.ReadEdge(ctx, *file, p, e)
		if err != nil {
			log.Fatal(err)
		}
		_ = enc.Encode(r)
	}
}

func tesseractBin() string {
	if b := os.Getenv("TESSERACT_BIN"); b != "" {
		return b
	}
	return "tesseract"
}

// readOnlyR2Source reads R2 with a client that has no writer database: every
// write it could attempt is refused by r2.Client itself.
func readOnlyR2Source(ctx context.Context, cfg config.Config) clockjoin.Source {
	client, err := r2.New(ctx, archiveR2Config(cfg, nil))
	if err != nil {
		log.Fatalf("open R2 client: %v", err)
	}
	return r2ClockSource{client}
}

type r2ClockSource struct{ c *r2.Client }

func (s r2ClockSource) Read(ctx context.Context, key string) ([]byte, error) {
	return s.c.Get(ctx, key)
}

func (s r2ClockSource) Fetch(ctx context.Context, key, scratch string) (string, error) {
	rc, err := s.c.Open(ctx, key)
	if err != nil {
		return "", err
	}
	defer rc.Close()
	dst := filepath.Join(scratch, path.Base(key))
	f, err := os.OpenFile(dst, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return "", err
	}
	_, copyErr := io.Copy(f, rc)
	return dst, errors.Join(copyErr, f.Close())
}

func readManifestKeys(p string) ([]string, error) {
	f, err := os.Open(p)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var keys []string
	seen := map[string]bool{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		k := strings.TrimSpace(sc.Text())
		if k == "" {
			continue
		}
		parts := strings.Split(k, "/")
		if len(parts) != 5 || parts[0] != "joined" || parts[2] != "coverage" || parts[3] != "hours" || !strings.HasSuffix(k, ".json") ||
			!strings.HasPrefix(parts[4], parts[1]+"__") {
			return nil, fmt.Errorf("%s: %q is not an hour manifest key", p, k)
		}
		if !seen[k] {
			seen[k] = true
			keys = append(keys, k)
		}
	}
	return keys, sc.Err()
}

func readClockProfiles(p string) (map[int64]clockjoin.Profile, error) {
	body, err := os.ReadFile(p)
	if err != nil {
		return nil, err
	}
	var list []clockjoin.Profile
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&list); err != nil {
		return nil, fmt.Errorf("%s: %w", p, err)
	}
	out := map[int64]clockjoin.Profile{}
	for _, pr := range list {
		if err := pr.Validate(); err != nil {
			return nil, err
		}
		if _, dup := out[pr.RecordingID]; dup {
			return nil, fmt.Errorf("%s: recording %d has two profiles", p, pr.RecordingID)
		}
		out[pr.RecordingID] = pr
	}
	return out, nil
}

// clockParentGeneration reads what the pass needs from the parent delivery
// generation: each collated file by object key, every delivery path, and the
// outputs its assemblies already replace (from the growth.json it lists).
func clockParentGeneration(ctx context.Context, src clockjoin.Source, registryKey string) (clockjoin.Env, error) {
	var reg struct {
		Generation string `json:"generation"`
		Assets     map[string]struct {
			Key    string `json:"key"`
			SHA256 string `json:"sha256"`
		} `json:"assets"`
		Files map[string]struct {
			Key         string `json:"key"`
			Path        string `json:"path"`
			SHA256      string `json:"sha256"`
			SizeBytes   int64  `json:"size_bytes"`
			RecordingID int64  `json:"recording_id"`
		} `json:"files"`
	}
	var growth struct {
		Generation string `json:"generation"`
		Repairs    []struct {
			SHA256 string  `json:"sha256"`
			Inputs []int64 `json:"input_ids"`
		} `json:"repairs"`
	}
	raw, err := src.Read(ctx, registryKey)
	if err != nil {
		return clockjoin.Env{}, fmt.Errorf("registry %s: %w", registryKey, err)
	}
	if err := unmarshalMaybeGzip(raw, &reg); err != nil {
		return clockjoin.Env{}, fmt.Errorf("registry %s: %w", registryKey, err)
	}
	asset, ok := reg.Assets["/public/growth.json"]
	if !ok {
		return clockjoin.Env{}, fmt.Errorf("registry %s lists no /public/growth.json", registryKey)
	}
	raw, err = src.Read(ctx, asset.Key)
	if err != nil {
		return clockjoin.Env{}, fmt.Errorf("growth %s: %w", asset.Key, err)
	}
	if sum := sha256.Sum256(raw); hex.EncodeToString(sum[:]) != asset.SHA256 {
		return clockjoin.Env{}, fmt.Errorf("growth %s differs from the registry's sha256", asset.Key)
	}
	if err := unmarshalMaybeGzip(raw, &growth); err != nil {
		return clockjoin.Env{}, fmt.Errorf("growth %s: %w", asset.Key, err)
	}
	if reg.Generation == "" || growth.Generation != reg.Generation {
		return clockjoin.Env{}, fmt.Errorf("growth generation %q is not the registry's %q", growth.Generation, reg.Generation)
	}
	env := clockjoin.Env{Delivered: map[string]clockjoin.Delivered{}, Paths: map[string]bool{}, Assembled: map[int64]bool{}}
	for id, f := range reg.Files {
		env.Paths[f.Path] = true
		n, err := strconv.ParseInt(id, 10, 64)
		if err != nil {
			continue // an assembly (sha256:...)
		}
		if _, dup := env.Delivered[f.Key]; dup {
			return clockjoin.Env{}, fmt.Errorf("registry lists %s twice", f.Key)
		}
		env.Delivered[f.Key] = clockjoin.Delivered{ID: n, Path: f.Path, SHA256: f.SHA256, SizeBytes: f.SizeBytes, RecordingID: f.RecordingID}
	}
	for _, r := range growth.Repairs {
		if _, live := reg.Files["sha256:"+r.SHA256]; live {
			for _, id := range r.Inputs {
				env.Assembled[id] = true
			}
		}
	}
	return env, nil
}

func unmarshalMaybeGzip(raw []byte, v any) error {
	if len(raw) > 1 && raw[0] == 0x1f && raw[1] == 0x8b {
		zr, err := gzip.NewReader(bytes.NewReader(raw))
		if err != nil {
			return err
		}
		if raw, err = io.ReadAll(zr); err != nil {
			return err
		}
	}
	return json.Unmarshal(raw, v)
}
