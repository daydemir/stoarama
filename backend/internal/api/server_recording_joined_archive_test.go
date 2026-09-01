package api

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/r2"
)

type joinedArchiveStoreStub struct {
	bodies    map[string][]byte
	heads     map[string]r2.ObjectHead
	openErr   map[string]error
	opened    []string
	openCheck func(context.Context)
}

func (s *joinedArchiveStoreStub) Head(_ context.Context, key string) (r2.ObjectHead, error) {
	head, ok := s.heads[key]
	if !ok {
		return r2.ObjectHead{}, errors.New("missing")
	}
	return head, nil
}

func (s *joinedArchiveStoreStub) OpenExact(ctx context.Context, key, _, _ string) (io.ReadCloser, error) {
	s.opened = append(s.opened, key)
	if s.openCheck != nil {
		s.openCheck(ctx)
	}
	if err := s.openErr[key]; err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(s.bodies[key])), nil
}

func (s *joinedArchiveStoreStub) PresignPutCreateOnlyRequest(context.Context, string, string, int64, string, time.Duration) (r2.PresignedRequest, error) {
	panic("archive must never mutate storage")
}

func (s *joinedArchiveStoreStub) PresignGetExactRequest(context.Context, string, string, string, time.Duration) (r2.PresignedRequest, error) {
	panic("archive streams through exact reads")
}

func TestJoinedFolderArchiveStreamsCanonicalMP4AndJSON(t *testing.T) {
	artifacts := []joinedArchiveArtifact{
		{ArtifactID: 1, ObjectKey: "joined/o1", ETag: "e1", RelativePath: "377_Europe_Poland_Luban/August/Thursday/a.mp4", ContentType: "video/mp4", SizeBytes: 3, SHA256: ""},
	}
	store := &joinedArchiveStoreStub{
		bodies: map[string][]byte{"joined/o1": []byte("mp4")},
		heads: map[string]r2.ObjectHead{
			"joined/o1": {ETag: "e1", SizeBytes: 3},
		},
		openErr: map[string]error{},
	}
	if err := preflightJoinedArchive(context.Background(), store, artifacts); err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	if err := streamJoinedArchive(context.Background(), recorder, store, artifacts); err != nil {
		t.Fatal(err)
	}
	archive, err := zip.NewReader(bytes.NewReader(recorder.Body.Bytes()), int64(recorder.Body.Len()))
	if err != nil {
		t.Fatal(err)
	}
	if len(archive.File) != 2 || archive.File[0].Name != "joined-files.json" || archive.File[1].Name != artifacts[0].RelativePath {
		t.Fatalf("archive entries=%v", archive.File)
	}
	for index, contains := range []string{`"artifact_id":1`, "mp4"} {
		body, err := archive.File[index].Open()
		if err != nil {
			t.Fatal(err)
		}
		got, _ := io.ReadAll(body)
		body.Close()
		if !strings.Contains(string(got), contains) {
			t.Fatalf("entry %d=%q missing %q", index, got, contains)
		}
	}
	indexBody, _ := archive.File[0].Open()
	indexJSON, _ := io.ReadAll(indexBody)
	indexBody.Close()
	for _, forbidden := range []string{"joined/o1", "e1", "version_id", "object_key"} {
		if strings.Contains(string(indexJSON), forbidden) {
			t.Fatalf("safe index leaked %q: %s", forbidden, indexJSON)
		}
	}
}

func TestJoinedFolderArchiveRejectsUnsafeOrDuplicatePathsBeforeStorageRead(t *testing.T) {
	for _, relative := range []string{"../raw/private.mp4", "/absolute.mp4", "a\\b.mp4", "a/../b.mp4", "a\x00b.mp4"} {
		store := &joinedArchiveStoreStub{heads: map[string]r2.ObjectHead{}, bodies: map[string][]byte{}, openErr: map[string]error{}}
		err := preflightJoinedArchive(context.Background(), store, []joinedArchiveArtifact{{ObjectKey: "x", RelativePath: relative, SizeBytes: 1}})
		if err == nil || len(store.opened) != 0 {
			t.Fatalf("path %q err=%v opened=%v", relative, err, store.opened)
		}
	}
	duplicate := []joinedArchiveArtifact{{ObjectKey: "a", RelativePath: "x.mp4", SizeBytes: 1}, {ObjectKey: "b", RelativePath: "x.mp4", SizeBytes: 1}}
	if err := validateJoinedArchive(duplicate); err == nil {
		t.Fatal("duplicate ZIP path accepted")
	}
}

func TestJoinedFolderArchivePreflightRejectsIdentityDriftBeforeResponse(t *testing.T) {
	artifact := joinedArchiveArtifact{ObjectKey: "joined/o1", ETag: "ledger", VersionID: "v1", RelativePath: "root/a.mp4", SizeBytes: 3}
	store := &joinedArchiveStoreStub{heads: map[string]r2.ObjectHead{"joined/o1": {ETag: "changed", VersionID: "v1", SizeBytes: 3}}, bodies: map[string][]byte{}, openErr: map[string]error{}}
	if err := preflightJoinedArchive(context.Background(), store, []joinedArchiveArtifact{artifact}); err == nil {
		t.Fatal("identity drift accepted")
	}
	if len(store.opened) != 0 {
		t.Fatalf("opened before clean preflight: %v", store.opened)
	}
}

func TestJoinedFolderArchiveStopsOnReadErrorAndClientCancellation(t *testing.T) {
	artifacts := []joinedArchiveArtifact{{ObjectKey: "joined/o1", ETag: "e1", RelativePath: "root/a.mp4", SizeBytes: 3}}
	readFailure := &joinedArchiveStoreStub{heads: map[string]r2.ObjectHead{"joined/o1": {ETag: "e1", SizeBytes: 3}}, bodies: map[string][]byte{}, openErr: map[string]error{"joined/o1": errors.New("r2 read failed")}}
	if err := streamJoinedArchive(context.Background(), io.Discard, readFailure, artifacts); err == nil || !strings.Contains(err.Error(), "r2 read failed") {
		t.Fatalf("read error=%v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cancelled := &joinedArchiveStoreStub{heads: map[string]r2.ObjectHead{}, bodies: map[string][]byte{}, openErr: map[string]error{}, openCheck: func(got context.Context) {
		if got.Err() == nil {
			t.Fatal("cancelled request context was not propagated")
		}
	}}
	if err := streamJoinedArchive(ctx, io.Discard, cancelled, artifacts); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel error=%v", err)
	}
}

func TestJoinedFolderArchiveScopeUsesCanonicalFolderSegments(t *testing.T) {
	all := []joinedArchiveArtifact{
		{RelativePath: "377_Europe_Poland_Luban/August/Thursday/a.mp4"},
		{RelativePath: "377_Europe_Poland_Luban/August/Friday/b.mp4"},
	}
	root, name, ok := scopeJoinedArchive(all, nil)
	if !ok || name != "377_Europe_Poland_Luban" || len(root) != 2 {
		t.Fatalf("root=%+v name=%q ok=%v", root, name, ok)
	}
	day, name, ok := scopeJoinedArchive(all, []string{"August", "Thursday"})
	if !ok || name != "Thursday" || len(day) != 1 || day[0].RelativePath != "377_Europe_Poland_Luban/August/Thursday/a.mp4" {
		t.Fatalf("day=%+v name=%q ok=%v", day, name, ok)
	}
	if _, _, ok := scopeJoinedArchive(all, []string{"September"}); ok {
		t.Fatal("missing folder accepted")
	}
}
