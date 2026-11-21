package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// manifestRow is one key to delete with the facts it was judged on.
type manifestRow struct {
	Key    string `json:"key"`
	Size   int64  `json:"size"`
	ETag   string `json:"etag"`
	MTime  int64  `json:"mtime"`
	Class  class  `json:"class"`
	Reason string `json:"reason"`
}

type tally struct {
	Objects int64   `json:"objects"`
	Bytes   int64   `json:"bytes"`
	TB      float64 `json:"tb"`
}

func (t *tally) add(size int64) {
	t.Objects++
	t.Bytes += size
	t.TB = float64(t.Bytes) / 1e12
}

type summary struct {
	Bucket         string                      `json:"bucket"`
	Inputs         map[string]string           `json:"inputs_sha256"`
	RecentFrom     string                      `json:"recent_from"`
	Release        string                      `json:"release"`
	Listed         tally                       `json:"listed"`
	Delete         tally                       `json:"delete"`
	Protected      tally                       `json:"protected"`
	ByClass        map[class]*tally            `json:"by_class"`
	ByReason       map[class]map[string]*tally `json:"by_class_reason"`
	ManifestSHA256 string                      `json:"manifest_sha256"`
}

func runPlan(args []string) error {
	fs := flag.NewFlagSet("plan", flag.ExitOnError)
	var files ruleFiles
	files.register(fs)
	snapshot := fs.String("snapshot", "", "listing TSV from r2prune list")
	out := fs.String("out", "", "new output directory")
	fs.Parse(args)
	if *snapshot == "" || *out == "" {
		return errors.New("--snapshot and --out are required")
	}
	r, err := files.load()
	if err != nil {
		return err
	}
	if err := os.Mkdir(*out, 0o700); err != nil {
		return err
	}
	sum := summary{Bucket: r.Bucket, RecentFrom: files.recentFrom, Release: files.release, ByClass: map[class]*tally{}, ByReason: map[class]map[string]*tally{}}
	if sum.Inputs, err = files.digests(); err != nil {
		return err
	}
	if sum.Inputs["snapshot"], err = fileSHA256(*snapshot); err != nil {
		return err
	}
	classified, err := os.OpenFile(filepath.Join(*out, "classified.tsv"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	defer classified.Close()
	cw := bufio.NewWriterSize(classified, 1<<20)
	var rows []manifestRow
	err = eachSnapshot(*snapshot, func(o object) error {
		c, reason := r.decide(o)
		sum.Listed.add(o.Size)
		if sum.ByClass[c] == nil {
			sum.ByClass[c], sum.ByReason[c] = &tally{}, map[string]*tally{}
		}
		sum.ByClass[c].add(o.Size)
		if sum.ByReason[c][reason] == nil {
			sum.ByReason[c][reason] = &tally{}
		}
		sum.ByReason[c][reason].add(o.Size)
		if c.deletes() {
			sum.Delete.add(o.Size)
			rows = append(rows, manifestRow{o.Key, o.Size, o.ETag, o.MTime, c, reason})
		} else {
			sum.Protected.add(o.Size)
		}
		_, err := fmt.Fprintf(cw, "%s\t%d\t%s\t%s\n", o.Key, o.Size, c, reason)
		return err
	})
	if err != nil {
		return err
	}
	if err := cw.Flush(); err != nil {
		return err
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Key < rows[j].Key })
	if sum.ManifestSHA256, err = writeManifest(filepath.Join(*out, "delete-manifest.jsonl"), rows); err != nil {
		return err
	}
	data, err := json.MarshalIndent(sum, "", " ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(*out, "summary.json"), append(data, '\n'), 0o600)
}

func writeManifest(path string, rows []manifestRow) (string, error) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	w := bufio.NewWriterSize(io.MultiWriter(f, h), 1<<20)
	enc := json.NewEncoder(w)
	for _, row := range rows {
		if err := enc.Encode(row); err != nil {
			return "", err
		}
	}
	if err := w.Flush(); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), f.Sync()
}

func eachSnapshot(path string, fn func(object) error) error {
	seen := map[string]bool{}
	return eachTSV(path, 4, func(c []string) error {
		size, err := strconv.ParseInt(c[1], 10, 64)
		if err != nil {
			return err
		}
		mtime, err := strconv.ParseInt(c[3], 10, 64)
		if err != nil {
			return err
		}
		if seen[c[0]] || strings.TrimSpace(c[0]) == "" {
			return fmt.Errorf("duplicate or empty key %q", c[0])
		}
		seen[c[0]] = true
		return fn(object{Key: c[0], Size: size, ETag: c[2], MTime: mtime})
	})
}

func fileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
