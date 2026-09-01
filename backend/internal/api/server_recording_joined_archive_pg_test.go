package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/go-chi/chi/v5"
)

func TestJoinedArchiveRedirectAndManifestUsePublishedAccountScopedMediaOnly(t *testing.T) {
	workerPublic, workerPrivate := joinedArchiveWorkerTestIdentity(t)
	pool := joinedBrowserTestPool(t)
	if _, err := pool.Exec(context.Background(), `CREATE TABLE recording_joined_batches(id bigint PRIMARY KEY,account_id bigint NOT NULL,batch_id text NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	seedJoinedBrowserTestData(t, pool)
	if _, err := pool.Exec(context.Background(), `INSERT INTO recording_joined_batches VALUES(1,47,'batch-1'),(2,99,'foreign-batch-1')`); err != nil {
		t.Fatal(err)
	}
	s := &Server{pool: pool, cfg: config.Config{
		JoinedWorkerSigningKey:       "archive-signing-key-32-bytes-long",
		JoinedArchiveWorkerURL:       "https://joined-download.example.test",
		JoinedArchiveWorkerPublicKey: workerPublic,
		SharedRecordingsAccountID:    47,
		SharedRecordingsSlug:         "mit-scl",
		SharedRecordingsPublic:       true,
	}}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/account/recordings/20/joined/folder/archive?folder=May%2FMonday", nil)
	route := chi.NewRouteContext()
	route.URLParams.Add("id", strconv.FormatInt(20, 10))
	ctx := context.WithValue(req.Context(), chi.RouteCtxKey, route)
	ctx = context.WithValue(ctx, accountPrincipalContextKey, accountPrincipal{AccountID: 47, AuthType: "session"})
	response := httptest.NewRecorder()
	s.handleAccountRecordingJoinedFolderArchive(response, req.WithContext(ctx))
	if response.Code != http.StatusTemporaryRedirect || response.Header().Get("Cache-Control") != "private, no-store" {
		t.Fatalf("status=%d headers=%v body=%s", response.Code, response.Header(), response.Body.String())
	}
	location, err := url.Parse(response.Header().Get("Location"))
	if err != nil || location.Host != "joined-download.example.test" || location.Path != "/archive" || location.Query().Get("token") == "" {
		t.Fatalf("location=%q err=%v", response.Header().Get("Location"), err)
	}
	admissionResponse := httptest.NewRecorder()
	admissionRequest := httptest.NewRequest(http.MethodGet, "/api/v1/recording/joined/archive/admission?token="+url.QueryEscape(location.Query().Get("token")), nil)
	signJoinedArchiveWorkerRequest(admissionRequest, workerPrivate, "admission", time.Now().UTC())
	s.handleJoinedArchiveAdmission(admissionResponse, admissionRequest)
	var admission joinedArchiveAdmission
	if admissionResponse.Code != http.StatusOK || json.Unmarshal(admissionResponse.Body.Bytes(), &admission) != nil || len(admission.RateScope) < 32 || len(admission.CapabilityID) < 32 {
		t.Fatalf("admission status=%d body=%s", admissionResponse.Code, admissionResponse.Body.String())
	}
	if strings.Contains(admissionResponse.Body.String(), "batch-1") || strings.Contains(admissionResponse.Body.String(), ".mp4") {
		t.Fatalf("admission leaked artifact metadata: %s", admissionResponse.Body.String())
	}

	manifestResponse := httptest.NewRecorder()
	manifestRequest := httptest.NewRequest(http.MethodGet, "/api/v1/recording/joined/archive/manifest?token="+url.QueryEscape(location.Query().Get("token")), nil)
	signJoinedArchiveWorkerRequest(manifestRequest, workerPrivate, "manifest", time.Now().UTC())
	s.handleJoinedArchiveManifest(manifestResponse, manifestRequest)
	if manifestResponse.Code != http.StatusOK {
		t.Fatalf("manifest status=%d body=%s", manifestResponse.Code, manifestResponse.Body.String())
	}
	var manifest joinedArchiveManifest
	if err := json.Unmarshal(manifestResponse.Body.Bytes(), &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.SchemaVersion != 1 || manifest.TotalBytes != 20 || len(manifest.Files) != 2 || manifest.ArchiveName != "Monday.zip" {
		t.Fatalf("manifest=%+v", manifest)
	}
	for _, file := range manifest.Files {
		if file.BatchID != "batch-1" || file.ContentType != "video/mp4" || !strings.HasSuffix(file.RelativePath, ".mp4") {
			t.Fatalf("file=%+v", file)
		}
	}
	for _, forbidden := range []string{"object_" + "key", "joined/" + "private/", "cloudflare" + "storage.com", "access_" + "key", "sec" + "ret"} {
		if strings.Contains(manifestResponse.Body.String(), forbidden) {
			t.Fatalf("manifest leaked %q: %s", forbidden, manifestResponse.Body.String())
		}
	}

	tampered := httptest.NewRecorder()
	tamperedRequest := httptest.NewRequest(http.MethodGet, "/api/v1/recording/joined/archive/manifest?token="+url.QueryEscape(location.Query().Get("token")+"x"), nil)
	signJoinedArchiveWorkerRequest(tamperedRequest, workerPrivate, "manifest", time.Now().UTC())
	s.handleJoinedArchiveManifest(tampered, tamperedRequest)
	if tampered.Code != http.StatusUnauthorized {
		t.Fatalf("tampered status=%d body=%s", tampered.Code, tampered.Body.String())
	}
}
