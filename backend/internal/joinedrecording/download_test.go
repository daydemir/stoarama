package joinedrecording

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"strings"
	"testing"

	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/daydemir/stoarama/backend/internal/stitchcert"
)

type memorySourceStore struct {
	head r2.ObjectHead
	body string
}

func (s memorySourceStore) Head(context.Context, string) (r2.ObjectHead, error) { return s.head, nil }
func (s memorySourceStore) OpenExact(_ context.Context, _ string, etag, version string) (io.ReadCloser, error) {
	if etag != s.head.ETag || version != s.head.VersionID {
		return nil, io.ErrUnexpectedEOF
	}
	return io.NopCloser(strings.NewReader(s.body)), nil
}

func TestDownloadClaimSourcesPinsHeadAndHashesBeforePublication(t *testing.T) {
	claim := oneOutputClaim(t)
	body := "exact raw clip bytes"
	sum := sha256.Sum256([]byte(body))
	claim.Output.Sources[0].Object.SizeBytes = int64(len(body))
	claim.Output.Sources[0].Object.SHA256 = hex.EncodeToString(sum[:])
	claim.Output.Sources[0].Object.ETag = "source-etag"
	claim.Output.Sources[0].Object.VersionID = "source-version"
	contentID, _, err := stitchcert.CanonicalSHA(struct {
		Policy  string       `json:"policy"`
		Sources []SourceClip `json:"sources"`
	}{PlanPolicyVersion, claim.Output.Sources})
	if err != nil {
		t.Fatal(err)
	}
	claim.Output.ContentID = contentID
	store := memorySourceStore{head: r2.ObjectHead{ETag: "source-etag", VersionID: "source-version", SizeBytes: int64(len(body))}, body: body}
	locals, scratch, err := DownloadClaimSources(context.Background(), claim, t.TempDir(), func(context.Context, SourceClip) (ExactSourceStore, error) { return store, nil })
	if err != nil {
		t.Fatal(err)
	}
	if len(locals) != 1 || !SafeScratchOutput(locals[0].Path, scratch) {
		t.Fatalf("bad local source: %+v", locals)
	}
	if err := verifyLocalIdentity(locals[0]); err != nil {
		t.Fatal(err)
	}
}
