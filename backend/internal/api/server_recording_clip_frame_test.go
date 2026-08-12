package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/capture"
	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/jackc/pgx/v5/pgxpool"
)

type fakeClipFrameObject struct {
	head       r2.ObjectHead
	body       []byte
	openedETag string
	openedVer  string
	headCalls  int
}

func (f *fakeClipFrameObject) Head(context.Context, string) (r2.ObjectHead, error) {
	f.headCalls++
	return f.head, nil
}
func (f *fakeClipFrameObject) OpenExact(_ context.Context, _ string, etag, version string) (io.ReadCloser, error) {
	f.openedETag, f.openedVer = etag, version
	return io.NopCloser(bytes.NewReader(f.body)), nil
}

func TestVerifiedFrameFromClipPinsAndVerifiesExactObject(t *testing.T) {
	body := []byte("exact verified native clip")
	sum := sha256.Sum256(body)
	src := recordingClipFrameSource{clipSHA: hex.EncodeToString(sum[:]), clipETag: "etag", dest: clipDestination{objectKey: "clip.mp4", sizeBytes: int64(len(body))}}
	obj := &fakeClipFrameObject{head: r2.ObjectHead{ETag: "etag", SizeBytes: int64(len(body)), VersionID: "version"}, body: body}
	want := capture.Frame{SHA256: strings.Repeat("b", 64)}
	got, version, err := verifiedFrameFromClip(context.Background(), obj, src, func(_ context.Context, r io.Reader) (capture.Frame, error) {
		seen, readErr := io.ReadAll(r)
		if readErr != nil || !bytes.Equal(seen, body) {
			t.Fatalf("decode input=%q err=%v", seen, readErr)
		}
		return want, nil
	})
	if err != nil || got.SHA256 != want.SHA256 || version != "version" || obj.openedETag != "etag" || obj.openedVer != "version" || obj.headCalls != 2 {
		t.Fatalf("got=%+v version=%q etag=%q openVersion=%q heads=%d err=%v", got, version, obj.openedETag, obj.openedVer, obj.headCalls, err)
	}
}

func TestVerifiedFrameFromClipRejectsObjectMismatchBeforeDecode(t *testing.T) {
	body := []byte("clip")
	sum := sha256.Sum256(body)
	src := recordingClipFrameSource{clipSHA: hex.EncodeToString(sum[:]), clipETag: "etag", dest: clipDestination{objectKey: "clip.mp4", sizeBytes: int64(len(body))}}
	cases := []struct {
		name string
		head r2.ObjectHead
		body []byte
	}{
		{"head size", r2.ObjectHead{ETag: "etag", SizeBytes: 99}, body},
		{"head etag", r2.ObjectHead{ETag: "wrong", SizeBytes: int64(len(body))}, body},
		{"get size", r2.ObjectHead{ETag: "etag", SizeBytes: int64(len(body))}, append(body, '!')},
		{"get sha", r2.ObjectHead{ETag: "etag", SizeBytes: int64(len(body))}, []byte("nope")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			decoded := false
			_, _, err := verifiedFrameFromClip(context.Background(), &fakeClipFrameObject{head: tc.head, body: tc.body}, src, func(context.Context, io.Reader) (capture.Frame, error) {
				decoded = true
				return capture.Frame{}, nil
			})
			if err == nil || decoded {
				t.Fatalf("err=%v decoded=%v", err, decoded)
			}
		})
	}
}

func TestClipFrameFFmpegDisablesNestedIOAndDecodesOneJPEG(t *testing.T) {
	bin, err := exec.LookPath("ffmpeg")
	if err != nil {
		t.Skip("ffmpeg unavailable")
	}
	t.Setenv("FFMPEG_BIN", bin)
	// concat would read a nested file if the protocol gate were missing.
	playlist := []byte("ffconcat version 1.0\nfile '/etc/passwd'\n")
	if _, err := ffmpegFrameFromClip(context.Background(), bytes.NewReader(playlist)); err == nil {
		t.Fatal("nested file playlist unexpectedly decoded")
	}
	args := strings.Join(clipFrameFFmpegArgs(), " ")
	for _, required := range []string{"-protocol_whitelist pipe", "-protocol_blacklist file,http,https,tcp,tls,udp,rtp,ftp", "-frames:v 1"} {
		if !strings.Contains(args, required) {
			t.Fatalf("missing ffmpeg safety arg %q in %q", required, args)
		}
	}
}

func TestBoundedWriterFailsClosed(t *testing.T) {
	w := &boundedWriter{n: 3}
	if _, err := w.Write([]byte("1234")); err == nil || w.b.Len() != 0 {
		t.Fatalf("oversized write err=%v len=%d", err, w.b.Len())
	}
}

func TestCanonicalClipStorageEndpoint(t *testing.T) {
	got, err := canonicalClipStorageEndpoint("HTTPS://STORAGE.EXAMPLE.TEST:443/prefix/")
	if err != nil || got != "https://storage.example.test:443/prefix" {
		t.Fatalf("got=%q err=%v", got, err)
	}
	for _, raw := range []string{"http://storage.example.test", "https://user:pass@storage.example.test", "https://"} {
		if _, err := canonicalClipStorageEndpoint(raw); err == nil {
			t.Fatalf("accepted endpoint %q", raw)
		}
	}
}

func TestRecordingClipAuthoritativeFrameRequiresServiceAuth(t *testing.T) {
	s := &Server{cfg: config.Config{ServiceToken: "service-secret"}}
	request := func(token string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodPost, "/api/v1/recordings/1/clips/2/authoritative-frame", strings.NewReader(`{"account_id":0}`))
		if token != "" {
			r.Header.Set("Authorization", "Bearer "+token)
		}
		r.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		s.router().ServeHTTP(w, r)
		return w
	}
	for _, token := range []string{"", "wrong"} {
		if got := request(token); got.Code != http.StatusUnauthorized {
			t.Fatalf("token=%q status=%d body=%s", token, got.Code, got.Body.String())
		}
	}
	if got := request("service-secret"); got.Code != http.StatusBadRequest {
		t.Fatalf("service status=%d body=%s", got.Code, got.Body.String())
	}
}

func TestPersistClipBackedAuthoritativeFrameIsIdempotentAndDoesNotMutateRuntime(t *testing.T) {
	databaseURL := strings.TrimSpace(os.Getenv("STOARAMA_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema := "clip_frame_" + strings.ReplaceAll(time.Now().UTC().Format("150405.000000000"), ".", "")
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	defer admin.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE")
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	for _, ddl := range []string{
		`CREATE TABLE recordings(id bigint primary key,account_id bigint not null,stream_id bigint not null,status text not null,stream_url text not null)`,
		`CREATE TABLE storage_destinations(id bigint primary key,account_id bigint not null,status text not null,managed boolean not null,region text,bucket text,endpoint text,access_key_id text,secret_access_key_enc bytea)`,
		`CREATE TABLE recording_clips(id bigint primary key,recording_id bigint not null,storage_destination_id bigint not null,sha256 text not null,etag text not null,purged_at timestamptz,released_at timestamptz,clip_start_at timestamptz,clip_end_at timestamptz,object_key text,size_bytes bigint)`,
		`CREATE TABLE media_objects(id bigserial primary key,storage_provider text not null,bucket text not null,object_key text not null,mime_type text not null,size_bytes bigint not null,etag text not null default '',sha256 text,width integer,height integer,created_at timestamptz not null default now(),unique(bucket,object_key))`,
		`CREATE TABLE frames(id bigserial primary key,stream_id bigint not null,capture_job_id bigint,captured_at timestamptz not null,raw_media_object_id bigint,capture_status text not null,capture_error text,source_kind text not null,source_recording_clip_id bigint unique,source_recording_clip_sha256 text,source_recording_clip_etag text,source_recording_clip_version_id text,source_recording_destination_id bigint,source_recording_endpoint text,source_recording_bucket text,source_recording_object_key text,source_recording_size_bytes bigint)`,
		`CREATE TABLE stream_capture_runtime(stream_id bigint primary key,execution_class text,resolved_url text,status text,last_frame_at timestamptz)`,
		`INSERT INTO recordings VALUES(10,47,99,'active','https://unchanged.example/live')`,
		`INSERT INTO storage_destinations VALUES(20,47,'verified',true,'auto','bucket','https://storage.example.test','key','secret')`,
		`INSERT INTO recording_clips VALUES(30,10,20,'` + strings.Repeat("a", 64) + `','clip-etag',NULL,now(),now()-interval '1 minute',now(),'clip.mp4',4)`,
		`INSERT INTO stream_capture_runtime VALUES(99,'video_live','https://secret.example/live','running',now()-interval '5 minutes')`,
	} {
		if _, err = pool.Exec(ctx, ddl); err != nil {
			t.Fatal(err)
		}
	}
	// Released clips remain intentionally eligible when their immutable managed
	// object is retained. Purging is the fail-closed boundary.
	loaded, err := (&Server{pool: pool}).loadRecordingClipFrameSource(ctx, 47, 10, 30)
	if err != nil || loaded.clipID != 30 {
		t.Fatalf("released managed clip not eligible loaded=%+v err=%v", loaded, err)
	}
	if _, err = pool.Exec(ctx, `UPDATE recording_clips SET purged_at=now() WHERE id=30`); err != nil {
		t.Fatal(err)
	}
	if _, err = (&Server{pool: pool}).loadRecordingClipFrameSource(ctx, 47, 10, 30); err == nil {
		t.Fatal("purged clip unexpectedly eligible")
	}
	if _, err = pool.Exec(ctx, `UPDATE recording_clips SET purged_at=NULL WHERE id=30`); err != nil {
		t.Fatal(err)
	}
	frame := capture.Frame{Bytes: []byte("jpeg"), MIMEType: "image/jpeg", SHA256: strings.Repeat("b", 64), SizeBytes: 4, Width: 1, Height: 1}
	end := time.Now().UTC()
	src := recordingClipFrameSource{accountID: 47, recordingID: 10, streamID: 99, clipID: 30, clipStartAt: end.Add(-time.Minute), clipEndAt: &end, clipSHA: strings.Repeat("a", 64), clipETag: "clip-etag", dest: clipDestination{id: 20, endpoint: "https://storage.example.test", bucket: "bucket", objectKey: "clip.mp4", sizeBytes: 4}}
	s := &Server{pool: pool}
	var puts atomic.Int32
	store := func() (int64, string, error) {
		return s.persistClipBackedAuthoritativeFrame(ctx, src, "v1", frame, "bucket", func(_ context.Context, key, mime string, body []byte) (string, error) {
			puts.Add(1)
			if !strings.Contains(key, "authoritative-clip-30-") || mime != "image/jpeg" || !bytes.Equal(body, frame.Bytes) {
				t.Errorf("invalid deterministic frame upload key=%q mime=%q", key, mime)
				return "", fmt.Errorf("invalid deterministic frame upload")
			}
			return "frame-etag", nil
		})
	}
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for range 2 {
		wg.Add(1)
		go func() { defer wg.Done(); _, _, callErr := store(); errs <- callErr }()
	}
	wg.Wait()
	close(errs)
	for callErr := range errs {
		if callErr != nil {
			t.Fatal(callErr)
		}
	}
	if puts.Load() != 1 {
		t.Fatalf("uploads=%d", puts.Load())
	}
	var frames int
	var recStatus, recURL, runtimeClass, runtimeURL, runtimeStatus, clipETag string
	var purgedAt *time.Time
	if err = pool.QueryRow(ctx, `SELECT count(*),(SELECT status FROM recordings WHERE id=10),(SELECT stream_url FROM recordings WHERE id=10),(SELECT execution_class FROM stream_capture_runtime WHERE stream_id=99),(SELECT resolved_url FROM stream_capture_runtime WHERE stream_id=99),(SELECT status FROM stream_capture_runtime WHERE stream_id=99),(SELECT etag FROM recording_clips WHERE id=30),(SELECT purged_at FROM recording_clips WHERE id=30) FROM frames WHERE source_recording_clip_id=30`).Scan(&frames, &recStatus, &recURL, &runtimeClass, &runtimeURL, &runtimeStatus, &clipETag, &purgedAt); err != nil {
		t.Fatal(err)
	}
	if frames != 1 || recStatus != "active" || recURL != "https://unchanged.example/live" || runtimeClass != "video_live" || runtimeURL != "https://secret.example/live" || runtimeStatus != "running" || clipETag != "clip-etag" || purgedAt != nil {
		t.Fatalf("unexpected mutation frames=%d rec=%s/%s runtime=%s/%s/%s clip=%s purged=%v", frames, recStatus, recURL, runtimeClass, runtimeURL, runtimeStatus, clipETag, purgedAt)
	}
}

func TestClipFramePersistenceConflictsAreTyped(t *testing.T) {
	databaseURL := strings.TrimSpace(os.Getenv("STOARAMA_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema := "clip_frame_conflict_" + strings.ReplaceAll(time.Now().UTC().Format("150405.000000000"), ".", "")
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	defer admin.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE")
	cfg, _ := pgxpool.ParseConfig(databaseURL)
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	for _, ddl := range []string{
		`CREATE TABLE recordings(id bigint primary key,account_id bigint,stream_id bigint,status text)`,
		`CREATE TABLE storage_destinations(id bigint primary key,account_id bigint,status text,managed boolean)`,
		`CREATE TABLE recording_clips(id bigint primary key,recording_id bigint,storage_destination_id bigint,sha256 text,etag text,purged_at timestamptz,clip_end_at timestamptz)`,
		`CREATE TABLE media_objects(id bigserial primary key,storage_provider text,bucket text,object_key text,mime_type text,size_bytes bigint,etag text,sha256 text,width integer,height integer,created_at timestamptz default now(),unique(bucket,object_key))`,
		`CREATE TABLE frames(id bigserial primary key,stream_id bigint,captured_at timestamptz,raw_media_object_id bigint,capture_status text,capture_error text,source_kind text,capture_job_id bigint,source_recording_clip_id bigint unique,source_recording_clip_sha256 text,source_recording_clip_etag text,source_recording_clip_version_id text,source_recording_destination_id bigint,source_recording_endpoint text,source_recording_bucket text,source_recording_object_key text,source_recording_size_bytes bigint)`,
		`INSERT INTO recordings VALUES(10,47,99,'paused')`,
		`INSERT INTO storage_destinations VALUES(20,47,'verified',true)`,
		`INSERT INTO recording_clips VALUES(30,10,20,'` + strings.Repeat("a", 64) + `','etag',NULL,now())`,
	} {
		if _, err = pool.Exec(ctx, ddl); err != nil {
			t.Fatal(err)
		}
	}
	src := recordingClipFrameSource{accountID: 47, recordingID: 10, streamID: 99, clipID: 30, clipStartAt: time.Now().UTC(), clipSHA: strings.Repeat("a", 64), clipETag: "etag"}
	frame := capture.Frame{Bytes: []byte("jpeg"), MIMEType: "image/jpeg", SHA256: strings.Repeat("b", 64), SizeBytes: 4, Width: 1, Height: 1}
	_, _, err = (&Server{pool: pool}).persistClipBackedAuthoritativeFrame(ctx, src, "", frame, "bucket", func(context.Context, string, string, []byte) (string, error) { return "etag", nil })
	var typed *clipFrameHTTPError
	if !errors.As(err, &typed) || typed.status != http.StatusConflict {
		t.Fatalf("err=%v typed=%+v", err, typed)
	}
}

func TestClipFrameIdentityLockRejectsConcurrentObjectPointerDrift(t *testing.T) {
	databaseURL := strings.TrimSpace(os.Getenv("STOARAMA_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema := "clip_frame_drift_" + strings.ReplaceAll(time.Now().UTC().Format("150405.000000000"), ".", "")
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	defer admin.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE")
	cfg, _ := pgxpool.ParseConfig(databaseURL)
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	for _, ddl := range []string{
		`CREATE TABLE recordings(id bigint primary key,account_id bigint,stream_id bigint,status text)`,
		`CREATE TABLE storage_destinations(id bigint primary key,account_id bigint,status text,managed boolean,endpoint text,bucket text)`,
		`CREATE TABLE recording_clips(id bigint primary key,recording_id bigint,storage_destination_id bigint,sha256 text,etag text,purged_at timestamptz,clip_end_at timestamptz,object_key text,size_bytes bigint)`,
		`INSERT INTO recordings VALUES(10,47,99,'active')`,
		`INSERT INTO storage_destinations VALUES(20,47,'verified',true,'https://storage.example.test','bucket')`,
		`INSERT INTO recording_clips VALUES(30,10,20,'` + strings.Repeat("a", 64) + `','etag',NULL,now(),'old.mp4',4)`,
	} {
		if _, err = pool.Exec(ctx, ddl); err != nil {
			t.Fatal(err)
		}
	}
	src := recordingClipFrameSource{accountID: 47, recordingID: 10, streamID: 99, clipID: 30, clipSHA: strings.Repeat("a", 64), clipETag: "etag", dest: clipDestination{id: 20, endpoint: "https://storage.example.test", bucket: "bucket", objectKey: "old.mp4", sizeBytes: 4}}
	// Simulate the exact race window: the clip retains the same content SHA/ETag
	// but its storage pointer changes after object verification and before the
	// persistence transaction. The locked identity must expose that drift.
	if _, err = pool.Exec(ctx, `UPDATE recording_clips SET object_key='new.mp4' WHERE id=30`); err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	got, err := lockRecordingClipFrameIdentity(ctx, tx, src)
	if err != nil {
		t.Fatal(err)
	}
	if got.objectKey == src.dest.objectKey || got.sha != src.clipSHA || got.etag != src.clipETag {
		t.Fatalf("drift not isolated got=%+v source=%+v", got, src.dest)
	}
}
