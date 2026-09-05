package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/go-chi/chi/v5"
)

func TestJoinedArchivePublicAndAccountCapabilitiesResolveToSameScopedMedia(t *testing.T) {
	pool := joinedBrowserTestPool(t)
	if _, err := pool.Exec(context.Background(), `CREATE TABLE recording_joined_batches(id bigint PRIMARY KEY,account_id bigint NOT NULL,batch_id text NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	seedJoinedBrowserTestData(t, pool)
	if _, err := pool.Exec(context.Background(), `INSERT INTO recording_joined_batches VALUES(1,47,'batch-1'),(2,99,'foreign-batch-1')`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(context.Background(), `
		INSERT INTO recording_joined_artifacts(id,hour_record_id,batch_record_id,account_id,artifact_kind,publication_state,published_at,etag,version_id,content_type,relative_path,expected_size_bytes,expected_sha256,object_key,ordinal)
		SELECT 2000+value*2,201,1,47,'media',NULL,now(),'media-bulk-'||value,'','video/mp4',
		       '20_Europe_Poland_Luban/May/Monday/hour_01_part_'||lpad(value::text,4,'0')||'.mp4',10,lpad(to_hex(value),64,'0'),'joined/private/media-bulk-'||value||'.mp4',100+value
		FROM generate_series(1,510) value;
		INSERT INTO recording_joined_artifacts(id,hour_record_id,batch_record_id,account_id,artifact_kind,publication_state,published_at,etag,version_id,content_type,relative_path,expected_size_bytes,expected_sha256,object_key,ordinal)
		SELECT 2001+value*2,201,1,47,'hour_manifest','published',now(),'manifest-bulk-'||value,'','application/json',
		       'coverage/hours/hour-bulk-'||lpad(value::text,4,'0')||'.json',10,lpad(to_hex(value),64,'0'),'joined/private/manifest-bulk-'||value||'.json',100+value
		FROM generate_series(1,510) value`); err != nil {
		t.Fatal(err)
	}
	const capabilityKey = "archive-capability-key-at-least-32-bytes"
	const workerToken = "archive-worker-token-at-least-32-bytes"
	s := &Server{pool: pool, cfg: config.Config{
		JoinedArchiveWorkerURL:     "https://joined-download.example.test",
		JoinedArchiveCapabilityKey: capabilityKey,
		JoinedArchiveWorkerToken:   workerToken,
		SharedRecordingsAccountID:  47, SharedRecordingsSlug: "mit-scl", SharedRecordingsPublic: true,
	}}

	capability := func(t *testing.T, shared bool) string {
		t.Helper()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/account/recordings/20/joined/folder/archive?folder=May%2FMonday", nil)
		route := chi.NewRouteContext()
		route.URLParams.Add("id", strconv.FormatInt(20, 10))
		ctx := context.WithValue(req.Context(), chi.RouteCtxKey, route)
		if !shared {
			ctx = context.WithValue(ctx, accountPrincipalContextKey, accountPrincipal{AccountID: 47, AuthType: "session"})
		}
		response := httptest.NewRecorder()
		if shared {
			s.handleSharedRecordingJoinedFolderArchive(response, req.WithContext(ctx))
		} else {
			s.handleAccountRecordingJoinedFolderArchive(response, req.WithContext(ctx))
		}
		if response.Code != http.StatusTemporaryRedirect || response.Header().Get("Cache-Control") != "private, no-store" || response.Header().Get("Referrer-Policy") != "no-referrer" {
			t.Fatalf("shared=%t status=%d headers=%v body=%s", shared, response.Code, response.Header(), response.Body.String())
		}
		location, err := url.Parse(response.Header().Get("Location"))
		if err != nil || location.Host != "joined-download.example.test" || location.Path != "/archive" || location.Query().Get("token") == "" {
			t.Fatalf("location=%q err=%v", response.Header().Get("Location"), err)
		}
		return location.Query().Get("token")
	}

	load := func(t *testing.T, token string) joinedArchiveManifest {
		t.Helper()
		request := httptest.NewRequest(http.MethodGet, "/api/v1/recording/joined/archive/manifest?token="+url.QueryEscape(token), nil)
		request.Header.Set("Authorization", "Bearer "+workerToken)
		response := httptest.NewRecorder()
		s.handleJoinedArchiveManifest(response, request)
		if response.Code != http.StatusOK {
			t.Fatalf("manifest status=%d body=%s", response.Code, response.Body.String())
		}
		var manifest joinedArchiveManifest
		if err := json.Unmarshal(response.Body.Bytes(), &manifest); err != nil {
			t.Fatal(err)
		}
		for _, forbidden := range []string{"object_key", "joined/test/", "cloudflarestorage.com", "access_key", "secret"} {
			if strings.Contains(strings.ToLower(response.Body.String()), forbidden) {
				t.Fatalf("manifest leaked %q: %s", forbidden, response.Body.String())
			}
		}
		return manifest
	}

	auth := load(t, capability(t, false))
	shared := load(t, capability(t, true))
	if auth.SchemaVersion != 1 || auth.TotalBytes != 5120 || len(auth.Files) != 512 || auth.ArchiveName != "Monday.zip" {
		t.Fatalf("auth manifest=%+v", auth)
	}
	paths := make(map[string]struct{}, len(auth.Files))
	for _, file := range auth.Files {
		if _, exists := paths[file.RelativePath]; exists {
			t.Fatalf("duplicate archive path %q", file.RelativePath)
		}
		paths[file.RelativePath] = struct{}{}
	}
	if auth.ArchiveName != shared.ArchiveName || auth.TotalBytes != shared.TotalBytes || !reflect.DeepEqual(auth.Files, shared.Files) {
		t.Fatalf("account/shared archive scope differs:\nauth=%+v\nshared=%+v", auth, shared)
	}
	for _, file := range auth.Files {
		if file.BatchID != "batch-1" || file.ContentType != "video/mp4" || !strings.HasSuffix(file.RelativePath, ".mp4") {
			t.Fatalf("file=%+v", file)
		}
	}

	unauthorized := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/api/v1/recording/joined/archive/manifest?token="+url.QueryEscape(capability(t, false)), nil)
	request.Header.Set("Authorization", "Bearer wrong-worker-token-at-least-32-bytes")
	s.handleJoinedArchiveManifest(unauthorized, request)
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized manifest status=%d body=%s", unauthorized.Code, unauthorized.Body.String())
	}
}
