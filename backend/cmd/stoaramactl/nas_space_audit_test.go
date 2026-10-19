package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseNASSpaceAuditArgs(t *testing.T) {
	for _, args := range [][]string{nil, {"request"}, {"report"}, {"report", "--connection-id", "-1"}, {"report", "--connection-id", "7", "--limit", "0"}, {"report", "--connection-id", "7", "--limit", "101"}, {"report", "--connection-id", "7", "extra"}} {
		if _, err := parseNASSpaceAuditArgs(args); err == nil {
			t.Errorf("accepted %v", args)
		}
	}
	opts, err := parseNASSpaceAuditArgs([]string{"report", "--connection-id", "7", "--json", "--download", "audit.gz"})
	if err != nil || opts.connectionID != 7 || opts.limit != 5 || !opts.asJSON || opts.download != "audit.gz" {
		t.Fatalf("%+v %v", opts, err)
	}
}

func spaceAuditArtifact(data []byte) nasSpaceAuditRow {
	h := sha256.Sum256(data)
	row := nasSpaceAuditRow{ConnectionID: 7, AuditID: "6ecf232b-23cd-42d3-bb56-bb1827af513b", State: "complete", ArtifactSizeBytes: int64(len(data)), ArtifactSHA256: hex.EncodeToString(h[:]), Summary: json.RawMessage(`{"files":3}`)}
	row.ArtifactObjectKey = "nas-space-audits/7/" + row.AuditID + "/" + row.ArtifactSHA256 + ".jsonl.gz"
	return row
}

func TestNASSpaceAuditArtifactIdentity(t *testing.T) {
	valid := spaceAuditArtifact([]byte("gzip bytes"))
	if err := validateNASSpaceAuditArtifact(valid); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*nasSpaceAuditRow){
		func(r *nasSpaceAuditRow) { r.ConnectionID = 8 }, func(r *nasSpaceAuditRow) { r.ArtifactObjectKey = "clips/media.mp4" },
		func(r *nasSpaceAuditRow) { r.AuditID = "../escape" }, func(r *nasSpaceAuditRow) { r.ArtifactSizeBytes = 0 },
		func(r *nasSpaceAuditRow) { r.ArtifactSizeBytes = 2<<30 + 1 }, func(r *nasSpaceAuditRow) { r.ArtifactSHA256 = "invalid" },
	} {
		row := valid
		mutate(&row)
		if err := validateNASSpaceAuditArtifact(row); err == nil {
			t.Fatalf("accepted invalid artifact %+v", row)
		}
	}
}

func TestNASSpaceAuditReport(t *testing.T) {
	row := spaceAuditArtifact([]byte("gzip bytes"))
	var out bytes.Buffer
	if err := writeNASSpaceAuditReport(&out, 7, []nasSpaceAuditRow{row}, true); err != nil {
		t.Fatal(err)
	}
	var decoded struct {
		ConnectionID int64              `json:"connection_id"`
		Audits       []nasSpaceAuditRow `json:"audits"`
	}
	if err := json.Unmarshal(out.Bytes(), &decoded); err != nil || decoded.ConnectionID != 7 || len(decoded.Audits) != 1 {
		t.Fatalf("%s %v", out.String(), err)
	}
	out.Reset()
	if err := writeNASSpaceAuditReport(&out, 7, []nasSpaceAuditRow{row}, false); err != nil {
		t.Fatal(err)
	}
	for _, marker := range []string{"connection=7", "state=complete", row.ArtifactObjectKey, `summary={"files":3}`} {
		if !strings.Contains(out.String(), marker) {
			t.Fatalf("missing %s", marker)
		}
	}
	out.Reset()
	if err := writeNASSpaceAuditReport(&out, 7, nil, true); err != nil || !strings.Contains(out.String(), `"audits":[]`) {
		t.Fatalf("%s %v", out.String(), err)
	}
}

func TestNASSpaceAuditDownloadVerifiesAndNeverOverwrites(t *testing.T) {
	data := []byte("compressed metadata fixture")
	row := spaceAuditArtifact(data)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write(data) }))
	defer server.Close()
	target := filepath.Join(t.TempDir(), "audit.jsonl.gz")
	if err := downloadNASSpaceAuditArtifact(context.Background(), server.URL+"?signature=secret", target, row); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(target)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("%q %v", got, err)
	}
	if err := downloadNASSpaceAuditArtifact(context.Background(), server.URL, target, row); err == nil {
		t.Fatal("overwrote existing file")
	}
	row.ArtifactSHA256 = strings.Repeat("0", 64)
	row.ArtifactObjectKey = "nas-space-audits/7/" + row.AuditID + "/" + row.ArtifactSHA256 + ".jsonl.gz"
	bad := filepath.Join(t.TempDir(), "bad.gz")
	if err := downloadNASSpaceAuditArtifact(context.Background(), server.URL, bad, row); err == nil {
		t.Fatal("accepted wrong digest")
	}
	if info, err := os.Stat(bad); err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("failed download must retain its private output")
	}
}

func TestNASSpaceAuditDownloadPreservesConcurrentReplacement(t *testing.T) {
	for _, status := range []int{http.StatusServiceUnavailable, http.StatusOK} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			data := []byte("compressed metadata fixture")
			row := spaceAuditArtifact(data)
			target := filepath.Join(t.TempDir(), "audit.jsonl.gz")
			moved := target + ".moved"
			sentinel := []byte("UNRELATED MEDIA MUST SURVIVE")
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if err := os.Rename(target, moved); err != nil {
					t.Error(err)
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				if err := os.WriteFile(target, sentinel, 0600); err != nil {
					t.Error(err)
				}
				w.WriteHeader(status)
				if status == http.StatusOK {
					w.Write(data)
				}
			}))
			defer server.Close()
			if err := downloadNASSpaceAuditArtifact(context.Background(), server.URL, target, row); err == nil {
				t.Fatal("accepted replaced output as a verified download")
			}
			if got, err := os.ReadFile(target); err != nil || !bytes.Equal(got, sentinel) {
				t.Fatalf("unrelated replacement changed or deleted: %q %v", got, err)
			}
			if _, err := os.Stat(moved); err != nil {
				t.Fatalf("original output path was removed: %v", err)
			}
		})
	}
}
