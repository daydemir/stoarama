package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"image"
	"image/color"
	"image/jpeg"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/capture"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestAuthoritativeFrameHandlerRejectsHeartbeatAndWrongAccount(t *testing.T) {
	img := image.NewRGBA(image.Rect(0, 0, 1, 1))
	var buf bytes.Buffer
	if err := jpeg.Encode(&buf, img, nil); err != nil {
		t.Fatal(err)
	}
	frame, err := capture.BuildFrameFromBytes(buf.Bytes(), "image/jpeg", "authoritative_frame_refresh")
	if err != nil {
		t.Fatal(err)
	}
	body := func(account int64, heartbeat bool) *http.Request {
		raw, _ := json.Marshal(map[string]any{"account_id": account, "stream_id": 9, "status": "success", "captured_at": time.Now().UTC(), "mime_type": "image/jpeg", "frame_base64": base64.StdEncoding.EncodeToString(buf.Bytes()), "frame_sha256": frame.SHA256, "recording_heartbeat": heartbeat, "authoritative_frame_only": true})
		req := httptest.NewRequest(http.MethodPost, "/api/v1/capture/ingest", bytes.NewReader(raw))
		return req.WithContext(context.WithValue(req.Context(), nodePrincipalContextKey, nodePrincipal{AccountID: 47, NodeType: nodeTypeLocalRecorder}))
	}
	s := &Server{}
	rr := httptest.NewRecorder()
	s.handleCaptureIngest(rr, body(47, true))
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("heartbeat status=%d body=%s", rr.Code, rr.Body.String())
	}
	rr = httptest.NewRecorder()
	s.handleCaptureIngest(rr, body(48, false))
	if rr.Code != http.StatusForbidden {
		t.Fatalf("account status=%d body=%s", rr.Code, rr.Body.String())
	}
}

func TestAuthoritativeFrameHandlerRejectsOversizedJPEGHeaderBeforePersistence(t *testing.T) {
	// Valid JPEG SOF metadata declaring 65535x65535 pixels. DecodeConfig can
	// read it, but the handler must reject it before any persistence call.
	huge := []byte{0xff, 0xd8, 0xff, 0xc0, 0x00, 0x11, 0x08, 0xff, 0xff, 0xff, 0xff, 0x03, 0x01, 0x11, 0x00, 0x02, 0x11, 0x00, 0x03, 0x11, 0x00, 0xff, 0xd9}
	sum := sha256.Sum256(huge)
	raw, _ := json.Marshal(map[string]any{"account_id": 47, "stream_id": 9, "status": "success", "captured_at": time.Now().UTC(), "mime_type": "image/jpeg", "frame_base64": base64.StdEncoding.EncodeToString(huge), "frame_sha256": hex.EncodeToString(sum[:]), "authoritative_frame_only": true})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/capture/ingest", bytes.NewReader(raw))
	req = req.WithContext(context.WithValue(req.Context(), nodePrincipalContextKey, nodePrincipal{AccountID: 47, NodeType: nodeTypeLocalRecorder}))
	rr := httptest.NewRecorder()
	(&Server{}).handleCaptureIngest(rr, req)
	if rr.Code != http.StatusBadRequest || !strings.Contains(rr.Body.String(), "invalid authoritative JPEG") {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
}

func TestPersistAuthoritativeFrameDoesNotMutateRecordingOrRuntime(t *testing.T) {
	databaseURL := strings.TrimSpace(os.Getenv("STOARAMA_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL to run authoritative-frame DB regression")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema := fmt.Sprintf("authoritative_frame_%d", time.Now().UnixNano())
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	defer admin.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE")
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ConnConfig.RuntimeParams == nil {
		cfg.ConnConfig.RuntimeParams = map[string]string{}
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	for _, ddl := range []string{
		`CREATE TABLE streams(id bigserial primary key)`,
		`CREATE TABLE recordings(id bigserial primary key,account_id bigint not null,stream_id bigint not null,status text not null,stream_url text not null)`,
		`CREATE TABLE media_objects(id bigserial primary key,storage_provider text not null,bucket text not null,object_key text not null,mime_type text not null,size_bytes bigint not null,etag text not null default '',sha256 text,width integer,height integer,created_at timestamptz not null default now(),unique(bucket,object_key))`,
		`CREATE TABLE frames(id bigserial primary key,stream_id bigint not null,capture_job_id bigint,captured_at timestamptz not null,raw_media_object_id bigint,capture_status text not null,capture_error text,source_kind text not null check(source_kind in ('live','snapshot_url','survey','authoritative_frame_refresh')),created_at timestamptz not null default now())`,
		`CREATE UNIQUE INDEX idx_frames_authoritative_identity ON frames(stream_id,captured_at) WHERE source_kind='authoritative_frame_refresh'`,
		`CREATE TABLE stream_health(stream_id bigint primary key,captures_total bigint not null default 0,captures_success bigint not null default 0,captures_error bigint not null default 0,last_error_at timestamptz,last_error_text text,last_capture_at timestamptz,updated_at timestamptz not null default now())`,
		`CREATE TABLE stream_capture_runtime(stream_id bigint primary key,execution_class text,resolved_capture_type text,resolved_url text,status text,last_resolved_at timestamptz,last_frame_at timestamptz,consecutive_errors integer not null default 0,last_error_text text,created_at timestamptz not null default now(),updated_at timestamptz not null default now())`,
	} {
		if _, err = pool.Exec(ctx, ddl); err != nil {
			t.Fatal(err)
		}
	}
	accountID := int64(47)
	var streamID, recordingID int64
	if err = pool.QueryRow(ctx, `INSERT INTO streams DEFAULT VALUES RETURNING id`).Scan(&streamID); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, `INSERT INTO recordings(account_id,stream_url,stream_id,status) VALUES($1,'https://example.test/live.m3u8',$2,'active') RETURNING id`, accountID, streamID).Scan(&recordingID); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO stream_capture_runtime(stream_id,execution_class,resolved_capture_type,resolved_url,status,last_resolved_at,last_frame_at,consecutive_errors,last_error_text) VALUES($1,'video_live','hls','https://secret.example/live.m3u8','running',now(),now()-interval '2 minutes',0,NULL)`, streamID); err != nil {
		t.Fatal(err)
	}
	img := image.NewRGBA(image.Rect(0, 0, 2, 2))
	img.Set(0, 0, color.White)
	var buf bytes.Buffer
	if err = jpeg.Encode(&buf, img, nil); err != nil {
		t.Fatal(err)
	}
	frame, err := capture.BuildFrameFromBytes(buf.Bytes(), "image/jpeg", "authoritative_frame_refresh")
	if err != nil {
		t.Fatal(err)
	}
	var putCount atomic.Int32
	s := &Server{pool: pool}
	capturedAt := time.Now().UTC()
	store := func() error {
		return s.persistAuthoritativeFrameSuccessWithStorage(ctx, accountID, streamID, capturedAt, frame, "test-bucket", func(_ context.Context, key, mime string, body []byte) (string, error) {
			putCount.Add(1)
			if !strings.Contains(key, "/authoritative-") || mime != "image/jpeg" || len(body) == 0 {
				t.Fatal("invalid upload")
			}
			return "etag", nil
		})
	}
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for range 2 {
		wg.Add(1)
		go func() { defer wg.Done(); errs <- store() }()
	}
	wg.Wait()
	close(errs)
	for err = range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if putCount.Load() != 1 {
		t.Fatalf("uploads=%d", putCount.Load())
	}
	var status, recURL, execClass, resolvedURL, runtimeStatus, sourceKind, mediaSHA string
	var frames, successes int
	if err = pool.QueryRow(ctx, `SELECT r.status,r.stream_url,rt.execution_class,rt.resolved_url,rt.status,f.source_kind,m.sha256,sh.captures_success FROM recordings r JOIN stream_capture_runtime rt ON rt.stream_id=r.stream_id JOIN frames f ON f.stream_id=r.stream_id JOIN media_objects m ON m.id=f.raw_media_object_id JOIN stream_health sh ON sh.stream_id=r.stream_id WHERE r.id=$1`, recordingID).Scan(&status, &recURL, &execClass, &resolvedURL, &runtimeStatus, &sourceKind, &mediaSHA, &successes); err != nil {
		t.Fatal(err)
	}
	if status != "active" || recURL != "https://example.test/live.m3u8" || execClass != "video_live" || resolvedURL != "https://secret.example/live.m3u8" || runtimeStatus != "running" {
		t.Fatalf("recording/runtime mutated status=%s url=%s class=%s resolved=%s runtime=%s", status, recURL, execClass, resolvedURL, runtimeStatus)
	}
	if sourceKind != "authoritative_frame_refresh" || mediaSHA != frame.SHA256 || successes != 1 {
		t.Fatalf("frame provenance kind=%s sha=%s successes=%d", sourceKind, mediaSHA, successes)
	}
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM frames WHERE stream_id=$1`, streamID).Scan(&frames); err != nil || frames != 1 {
		t.Fatalf("frames=%d err=%v", frames, err)
	}
	putCount.Store(0)
	if err = s.persistAuthoritativeFrameSuccessWithStorage(ctx, accountID+1, streamID, time.Now().UTC(), frame, "test-bucket", func(context.Context, string, string, []byte) (string, error) { putCount.Add(1); return "etag", nil }); err == nil || putCount.Load() != 0 {
		t.Fatalf("foreign account err=%v uploads=%d", err, putCount.Load())
	}
	// A post-upload DB failure rolls back rows but intentionally preserves the
	// deterministic R2 object. Its ownership may be ambiguous; the exact-hash
	// retry below must safely reuse the same key and create one evidence row.
	if _, err = pool.Exec(ctx, `DROP TABLE stream_health`); err != nil {
		t.Fatal(err)
	}
	putCount.Store(0)
	failureAt := time.Now().Add(time.Second).UTC()
	var preservedKey string
	err = s.persistAuthoritativeFrameSuccessWithStorage(ctx, accountID, streamID, failureAt, frame, "test-bucket", func(_ context.Context, key, _ string, body []byte) (string, error) {
		putCount.Add(1)
		preservedKey = key
		if sha256.Sum256(body) != sha256.Sum256(frame.Bytes) {
			t.Fatal("post-upload failure changed object bytes")
		}
		return "etag", nil
	})
	if err == nil || putCount.Load() != 1 {
		t.Fatalf("post-upload failure err=%v uploads=%d", err, putCount.Load())
	}
	if _, err = pool.Exec(ctx, `CREATE TABLE stream_health(stream_id bigint primary key,captures_total bigint not null default 0,captures_success bigint not null default 0,captures_error bigint not null default 0,last_error_at timestamptz,last_error_text text,last_capture_at timestamptz,updated_at timestamptz not null default now())`); err != nil {
		t.Fatal(err)
	}
	err = s.persistAuthoritativeFrameSuccessWithStorage(ctx, accountID, streamID, failureAt, frame, "test-bucket", func(_ context.Context, key, _ string, body []byte) (string, error) {
		putCount.Add(1)
		if key != preservedKey || sha256.Sum256(body) != sha256.Sum256(frame.Bytes) {
			t.Fatal("retry did not reuse deterministic object identity")
		}
		return "etag", nil
	})
	if err != nil || putCount.Load() != 2 {
		t.Fatalf("retry err=%v uploads=%d", err, putCount.Load())
	}
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM frames WHERE stream_id=$1 AND captured_at=$2 AND source_kind='authoritative_frame_refresh'`, streamID, failureAt).Scan(&frames); err != nil || frames != 1 {
		t.Fatalf("retry frames=%d err=%v", frames, err)
	}

	// A Put error is also ambiguous: the object may already exist. The helper
	// leaves it alone and a retry uses the identical key and bytes.
	ambiguousAt := failureAt.Add(time.Second)
	var ambiguousKey string
	err = s.persistAuthoritativeFrameSuccessWithStorage(ctx, accountID, streamID, ambiguousAt, frame, "test-bucket", func(_ context.Context, key, _ string, body []byte) (string, error) {
		ambiguousKey = key
		if sha256.Sum256(body) != sha256.Sum256(frame.Bytes) {
			t.Fatal("ambiguous upload changed object bytes")
		}
		return "", fmt.Errorf("ambiguous transport failure")
	})
	if err == nil {
		t.Fatal("ambiguous upload unexpectedly succeeded")
	}
	err = s.persistAuthoritativeFrameSuccessWithStorage(ctx, accountID, streamID, ambiguousAt, frame, "test-bucket", func(_ context.Context, key, _ string, body []byte) (string, error) {
		if key != ambiguousKey || sha256.Sum256(body) != sha256.Sum256(frame.Bytes) {
			t.Fatal("ambiguous retry did not reuse deterministic object identity")
		}
		return "etag", nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM frames WHERE stream_id=$1 AND captured_at=$2 AND source_kind='authoritative_frame_refresh'`, streamID, ambiguousAt).Scan(&frames); err != nil || frames != 1 {
		t.Fatalf("ambiguous retry frames=%d err=%v", frames, err)
	}
}
