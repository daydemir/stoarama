package main

import (
	"bytes"
	"context"
	"github.com/daydemir/stoarama/backend/internal/db"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWriterReconcileRefusesApplyAndDefaultsToReadOnly(t *testing.T) {
	opts, err := parseR2WriterReconcileArgs([]string{"--out", "report.jsonl", "--bucket", "stoarama"})
	if err != nil || opts.out != "report.jsonl" || opts.bucket != "stoarama" {
		t.Fatalf("dry-run args: %+v %v", opts, err)
	}
	for _, args := range [][]string{{"--apply", "--out", "x"}, {"--out", "x", "extra"}, {"--out", "x", "--bucket", ""}, nil} {
		if _, err := parseR2WriterReconcileArgs(args); err == nil {
			t.Fatalf("unsafe/incomplete args accepted: %v", args)
		}
	}
}

func TestWriterReconcileObservesRealGateWithoutChangingLifecycle(t *testing.T) {
	pool := joinedPurgeTestPool(t, "writer_reconcile")
	ctx := context.Background()
	if err := db.MigrateUp(ctx, pool, joinedPurgeMigrationsDir()); err != nil {
		t.Fatal(err)
	}
	h, items, err := r2WriterAudit(ctx, pool, "stoarama")
	if err != nil {
		t.Fatal(err)
	}
	if !h.DryRun || !h.WritersDrained || !h.IndexesReady || len(items) != 0 || len(h.Counts) != 19 || h.ApplyCommand != nil {
		t.Fatalf("empty gate report: %+v items=%d", h, len(items))
	}
	_, err = pool.Exec(ctx, `INSERT INTO streams(id,provider,external_id,name,slug,source_url,recording_state,capture_type,source_family,execution_class,capture_family,expected_fps) VALUES(991,'test','991','test','writer-reconcile-test','https://secret.invalid/never-report-me','on','hls','video_manifest','video_live','continuous_video',30);
 INSERT INTO stream_capture_runtime(stream_id,execution_class,status,resolved_url,updated_at,last_frame_at) VALUES(991,'video_live','running','https://secret.invalid/never-report-me',now()-interval '1 year',now()-interval '1 year')`)
	if err != nil {
		t.Fatal(err)
	}
	h, items, err = r2WriterAudit(ctx, pool, "stoarama")
	if err != nil {
		t.Fatal(err)
	}
	if h.WritersDrained || h.ProvenDead != 0 || len(items) != 2 || h.Counts[r2WriterStreams] != 1 || h.Counts[r2WriterRuntime] != 1 {
		t.Fatalf("old lifecycle lost: %+v items=%d", h, len(items))
	}
	path := filepath.Join(t.TempDir(), "audit.jsonl")
	if err := writeR2WriterAudit(path, h, items); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "never-report-me") || strings.Contains(string(data), "resolved_url") {
		t.Fatalf("secret-bearing fields in report: %s", data)
	}
	for _, item := range items {
		if item.ProvenDead || len(item.ObservationSHA256) != 64 || item.Blocker == "" {
			t.Fatalf("age became authority: %+v", item)
		}
	}
	if err := writeR2WriterAudit(path, h, nil); err == nil {
		t.Fatal("overwrote evidence")
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(data, after) {
		t.Fatal("existing evidence changed", err)
	}
	// Repeat public audit: dry-run must leave the same lifecycle blockers.
	again, _, err := r2WriterAudit(ctx, pool, "stoarama")
	if err != nil || again.WritersDrained || again.Counts[r2WriterRuntime] != 1 || again.Counts[r2WriterStreams] != 1 {
		t.Fatalf("dry-run changed writers: %+v %v", again, err)
	}
}
