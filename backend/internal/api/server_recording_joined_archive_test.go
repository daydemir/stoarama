package api

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
)

func joinedArchiveWorkerTestIdentity(t *testing.T) (string, ed25519.PrivateKey) {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return base64.RawURLEncoding.EncodeToString(public), private
}

func signJoinedArchiveWorkerRequest(req *http.Request, private ed25519.PrivateKey, operation string, now time.Time) {
	timestamp := now.UTC().Format(time.RFC3339)
	digest := sha256.Sum256([]byte(req.URL.Query().Get("token")))
	message := joinedArchiveCapabilityAudience + "\x00" + operation + "\x00" + timestamp + "\x00" + base64.RawURLEncoding.EncodeToString(digest[:])
	req.Header.Set("X-Stoarama-Archive-Timestamp", timestamp)
	req.Header.Set("X-Stoarama-Archive-Signature", base64.RawURLEncoding.EncodeToString(ed25519.Sign(private, []byte(message))))
}

func TestJoinedArchiveCapabilityIsScopedTamperEvidentAndShortLived(t *testing.T) {
	now := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	claims := joinedArchiveClaims{AccountID: 47, RecordingID: 377, Folder: []string{"August", "Thursday"}, ExpiresAt: now.Add(5 * time.Minute).Unix(), Nonce: "0123456789abcdef", ClientScope: strings.Repeat("c", 43)}
	token, err := mintJoinedArchiveCapability("archive-signing-key-32-bytes-long", claims)
	if err != nil {
		t.Fatal(err)
	}
	verified, err := verifyJoinedArchiveCapability("archive-signing-key-32-bytes-long", token, now)
	if err != nil || verified.AccountID != 47 || verified.RecordingID != 377 || strings.Join(verified.Folder, "/") != "August/Thursday" {
		t.Fatalf("claims=%+v err=%v", verified, err)
	}
	if _, err := verifyJoinedArchiveCapability("other-signing-key-32-bytes-long", token, now); err == nil {
		t.Fatal("changed signing key accepted")
	}
	if _, err := verifyJoinedArchiveCapability("archive-signing-key-32-bytes-long", token+"x", now); err == nil {
		t.Fatal("tampered capability accepted")
	}
	if _, err := verifyJoinedArchiveCapability("archive-signing-key-32-bytes-long", token, now.Add(6*time.Minute)); err == nil {
		t.Fatal("expired capability accepted")
	}
}

func TestJoinedArchiveWorkerSignatureRejectsTamperAndStaleRequests(t *testing.T) {
	public, private := joinedArchiveWorkerTestIdentity(t)
	now := time.Now().UTC().Truncate(time.Second)
	digest := sha256.Sum256([]byte("capability"))
	message := joinedArchiveCapabilityAudience + "\x00manifest\x00" + now.Format(time.RFC3339) + "\x00" + base64.RawURLEncoding.EncodeToString(digest[:])
	signature := base64.RawURLEncoding.EncodeToString(ed25519.Sign(private, []byte(message)))
	if !verifyJoinedArchiveWorker(public, "manifest", now.Format(time.RFC3339), signature, "capability", now) {
		t.Fatal("valid worker signature rejected")
	}
	if verifyJoinedArchiveWorker(public, "manifest", now.Format(time.RFC3339), signature, "other", now) {
		t.Fatal("signature accepted for another capability")
	}
	if verifyJoinedArchiveWorker(public, "manifest", now.Add(-time.Minute).Format(time.RFC3339), signature, "capability", now) {
		t.Fatal("stale signature accepted")
	}
}

func TestJoinedArchiveRejectsTraversalWindowsPathsAndPortableCollisions(t *testing.T) {
	valid := joinedArchiveArtifact{ArtifactID: 1, BatchID: "batch-1", RelativePath: "377_Europe_Poland_Luban/August/Thursday/a.mp4", SizeBytes: 1, SHA256: strings.Repeat("a", 64)}
	for _, relative := range []string{"../raw.mp4", "/absolute.mp4", "a\\b.mp4", "a/../b.mp4", "C:/outside.mp4", "folder/CON.mp4", "folder/name. ", "a\x00b.mp4"} {
		artifact := valid
		artifact.RelativePath = relative
		if _, err := validateJoinedArchive([]joinedArchiveArtifact{artifact}); err == nil {
			t.Fatalf("unsafe path %q accepted", relative)
		}
	}
	second := valid
	second.ArtifactID = 2
	second.RelativePath = strings.ToUpper(valid.RelativePath)
	if _, err := validateJoinedArchive([]joinedArchiveArtifact{valid, second}); err == nil {
		t.Fatal("case-insensitive extraction collision accepted")
	}
}

func TestJoinedArchiveScopeSelectsRootMonthAndWeekday(t *testing.T) {
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
	if _, _, ok := scopeJoinedArchive(all, []string{"September"}); ok {
		t.Fatal("missing folder accepted")
	}
}

func TestJoinedArchiveRedirectRequiresPrincipalAndValidWorkerHTTPSURL(t *testing.T) {
	route := chi.NewRouteContext()
	route.URLParams.Add("id", "377")
	req := httptest.NewRequest(http.MethodGet, "/api/v1/account/recordings/377/joined/folder/archive", nil)
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, route))
	response := httptest.NewRecorder()
	(&Server{}).handleAccountRecordingJoinedFolderArchive(response, req)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	for _, base := range []string{"http://downloads.example.test", "https://user@example.test", "https://example.test/?query=1"} {
		if _, err := joinedArchiveWorkerURL(base, "token"); err == nil {
			t.Fatalf("unsafe worker URL %q accepted", base)
		}
	}
	got, err := joinedArchiveWorkerURL("https://joined.example.test/base", "a.b")
	if err != nil || got != "https://joined.example.test/base/archive?token=a.b" {
		t.Fatalf("worker URL=%q err=%v", got, err)
	}
}
