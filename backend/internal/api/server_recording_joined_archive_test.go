package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/go-chi/chi/v5"
)

func TestJoinedArchiveCapabilityIsScopedTamperEvidentAndShortLived(t *testing.T) {
	now := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	claims := joinedArchiveClaims{
		AccountID: 47, RecordingID: 377, Folder: []string{"August", "Thursday"}, ExpiresAt: now.Add(5 * time.Minute).Unix(),
		Nonce: "0123456789abcdef", ClientScope: strings.Repeat("c", 43),
	}
	key := "archive-capability-key-at-least-32-bytes"
	token, err := mintJoinedArchiveCapability(key, claims)
	if err != nil {
		t.Fatal(err)
	}
	verified, err := verifyJoinedArchiveCapability(key, token, now)
	if err != nil || verified.AccountID != 47 || verified.RecordingID != 377 || strings.Join(verified.Folder, "/") != "August/Thursday" {
		t.Fatalf("claims=%+v err=%v", verified, err)
	}
	if _, err := verifyJoinedArchiveCapability("different-capability-key-at-least-32-bytes", token, now); err == nil {
		t.Fatal("changed signing key accepted")
	}
	if _, err := verifyJoinedArchiveCapability(key, token+"x", now); err == nil {
		t.Fatal("tampered capability accepted")
	}
	if _, err := verifyJoinedArchiveCapability(key, token, now.Add(6*time.Minute)); err == nil {
		t.Fatal("expired capability accepted")
	}
}

func TestJoinedArchiveWorkerAuthenticationAndPortablePaths(t *testing.T) {
	want := "archive-worker-token-at-least-32-bytes"
	if !joinedArchiveWorkerAuthorized(want, want) || joinedArchiveWorkerAuthorized(want, want+"x") || joinedArchiveWorkerAuthorized("short", "short") {
		t.Fatal("worker token comparison accepted an invalid credential")
	}
	server := &Server{cfg: config.Config{
		JoinedArchiveWorkerURL: "https://joined.example.test", JoinedArchiveCapabilityKey: "archive-capability-key-at-least-32-bytes", JoinedArchiveWorkerToken: want,
	}}
	request := httptest.NewRequest(http.MethodGet, "/api/v1/recording/joined/archive/manifest", nil)
	request.Header.Set("Authorization", want)
	response := httptest.NewRecorder()
	server.handleJoinedArchiveManifest(response, request)
	if response.Code != http.StatusUnauthorized || !strings.Contains(response.Body.String(), "worker authentication") {
		t.Fatalf("non-Bearer credential status=%d body=%s", response.Code, response.Body.String())
	}
	valid := joinedArchiveArtifact{
		BatchID: "batch-1", ETag: "etag", ContentType: "video/mp4",
		RelativePath: "377_Europe_Poland_Luban/August/Thursday/a.mp4", SizeBytes: 1, SHA256: strings.Repeat("a", 64),
	}
	for _, relative := range []string{"../raw.mp4", "/absolute.mp4", "a\\b.mp4", "a/../b.mp4", "C:/outside.mp4", "folder/CON.mp4", "folder/name. ", "folder/name?.mp4", "a\x00b.mp4"} {
		artifact := valid
		artifact.RelativePath = relative
		if _, err := validateJoinedArchive([]joinedArchiveArtifact{artifact}); err == nil {
			t.Fatalf("unsafe path %q accepted", relative)
		}
	}
	second := valid
	second.RelativePath = "377_Europe_Poland_Luban/August/Thursday/A.MP4"
	if _, err := validateJoinedArchive([]joinedArchiveArtifact{valid, second}); err == nil {
		t.Fatal("case-insensitive extraction collision accepted")
	}
	second.RelativePath = "other-recording/August/Thursday/b.mp4"
	if _, err := validateJoinedArchive([]joinedArchiveArtifact{valid, second}); err == nil {
		t.Fatal("multiple recording roots accepted")
	}
	second = valid
	second.ContentType = "application/octet-stream"
	if _, err := validateJoinedArchive([]joinedArchiveArtifact{second}); err == nil {
		t.Fatal("non-MP4 media accepted")
	}
}

func TestJoinedArchiveScopeAndWorkerURL(t *testing.T) {
	all := []joinedArchiveArtifact{
		{RelativePath: "377_Europe_Poland_Luban/August/Thursday/a.mp4"},
		{RelativePath: "377_Europe_Poland_Luban/August/Friday/b.mp4"},
	}
	root, name, ok := scopeJoinedArchive(all, nil)
	if !ok || name != "377_Europe_Poland_Luban" || len(root) != 2 {
		t.Fatalf("root=%+v name=%q ok=%v", root, name, ok)
	}
	day, name, ok := scopeJoinedArchive(all, []string{"August", "Thursday"})
	if !ok || name != "Thursday" || len(day) != 1 || day[0].RelativePath != all[0].RelativePath {
		t.Fatalf("day=%+v name=%q ok=%v", day, name, ok)
	}
	for _, base := range []string{"http://downloads.example.test", "https://user@example.test", "https://example.test/?query=1", "https://example.test/base"} {
		if _, err := joinedArchiveWorkerURL(base, "token"); err == nil {
			t.Fatalf("unsafe worker URL %q accepted", base)
		}
	}
	got, err := joinedArchiveWorkerURL("https://joined.example.test", "a.b")
	if err != nil || got != "https://joined.example.test/archive?token=a.b" {
		t.Fatalf("worker URL=%q err=%v", got, err)
	}
}

func TestJoinedArchiveRedirectRequiresPrincipal(t *testing.T) {
	route := chi.NewRouteContext()
	route.URLParams.Add("id", "377")
	req := httptest.NewRequest(http.MethodGet, "/api/v1/account/recordings/377/joined/folder/archive", nil)
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, route))
	response := httptest.NewRecorder()
	(&Server{}).handleAccountRecordingJoinedFolderArchive(response, req)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestJoinedArchiveManifestDistinguishesCapabilityFailure(t *testing.T) {
	server := &Server{cfg: config.Config{
		JoinedArchiveWorkerURL:     "https://joined.example.test",
		JoinedArchiveCapabilityKey: "archive-capability-key-at-least-32-bytes",
		JoinedArchiveWorkerToken:   "archive-worker-token-at-least-32-bytes",
	}}
	request := httptest.NewRequest(http.MethodGet, "/api/v1/recording/joined/archive/manifest?token=invalid", nil)
	request.Header.Set("Authorization", "Bearer archive-worker-token-at-least-32-bytes")
	response := httptest.NewRecorder()
	server.handleJoinedArchiveManifest(response, request)
	if response.Code != http.StatusGone || response.Header().Get("Cache-Control") != "private, no-store" || !strings.Contains(response.Body.String(), "invalid or expired") {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
}
