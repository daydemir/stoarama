package api

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/r2"
)

func TestPublicJoinedFolderArchiveIsAccountScopedAndContainsPublishedMP4AndJSON(t *testing.T) {
	pool := joinedBrowserTestPool(t)
	seedJoinedBrowserTestData(t, pool)
	body := []byte("0123456789")
	digest := sha256.Sum256(body)
	if _, err := pool.Exec(context.Background(), `UPDATE recording_joined_artifacts SET expected_sha256=$1 WHERE expected_size_bytes=10`, hex.EncodeToString(digest[:])); err != nil {
		t.Fatal(err)
	}
	store := &joinedArchiveStoreStub{
		bodies: map[string][]byte{
			"joined/private/media-1.mp4": body,
			"joined/private/media-2.mp4": body,
		},
		heads: map[string]r2.ObjectHead{
			"joined/private/media-1.mp4": {ETag: "media-1", SizeBytes: 10},
			"joined/private/media-2.mp4": {ETag: "media-2", SizeBytes: 10},
		},
		openErr: map[string]error{},
	}
	s := &Server{pool: pool, joinedOutputStorage: store, cfg: config.Config{
		SharedRecordingsAccountID: 47,
		SharedRecordingsSlug:      "mit-scl",
		SharedRecordingsPublic:    true,
	}}
	response := httptest.NewRecorder()
	s.router().ServeHTTP(response, httptest.NewRequest(http.MethodGet,
		"/api/v1/shared/mit-scl/recordings/20/joined/folder/archive", nil))
	if response.Code != http.StatusOK || response.Header().Get("Content-Type") != "application/zip" {
		t.Fatalf("status=%d headers=%v body=%s", response.Code, response.Header(), response.Body.String())
	}
	archive, err := zip.NewReader(bytes.NewReader(response.Body.Bytes()), int64(response.Body.Len()))
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{
		"joined-files.json":                        true,
		"May/Monday/hour_01_part_01_0800-0801.mp4": true,
		"May/Monday/hour_01_part_02_0801-0802.mp4": true,
	}
	if len(archive.File) != len(want) {
		t.Fatalf("entries=%v", archive.File)
	}
	for _, file := range archive.File {
		if !want[file.Name] {
			t.Fatalf("unexpected entry %q", file.Name)
		}
	}

	foreign := httptest.NewRecorder()
	s.router().ServeHTTP(foreign, httptest.NewRequest(http.MethodGet,
		"/api/v1/shared/mit-scl/recordings/50/joined/folder/archive", nil))
	if foreign.Code != http.StatusNotFound {
		t.Fatalf("foreign status=%d body=%s", foreign.Code, foreign.Body.String())
	}
}
