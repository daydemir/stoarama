package presentationprobe

import (
	"archive/tar"
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"fmt"
	"testing"
	"time"
)

func makeArtifact(t *testing.T) ([]byte, []byte, []byte, ArtifactManifest, ed25519.PublicKey) {
	t.Helper()
	contents := map[string][]byte{"bin/presentation-probe-v2": []byte("executable"), "NOTICE": []byte("license")}
	var archive bytes.Buffer
	tw := tar.NewWriter(&archive)
	names := []string{"NOTICE", "bin/presentation-probe-v2"}
	files := make([]ArtifactFile, 0, len(names))
	for _, name := range names {
		mode := int64(0o444)
		role := "notice"
		if name[0:3] == "bin" {
			mode = 0o555
			role = "executable"
		}
		body := contents[name]
		h := &tar.Header{Name: name, Mode: mode, Size: int64(len(body)), ModTime: time.Unix(0, 0), AccessTime: time.Time{}, ChangeTime: time.Time{}, Typeflag: tar.TypeReg, Format: tar.FormatUSTAR}
		if err := tw.WriteHeader(h); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write(body); err != nil {
			t.Fatal(err)
		}
		files = append(files, ArtifactFile{Path: name, SHA256: SHA256(body), Size: int64(len(body)), Mode: uint32(mode), Role: role})
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	m := ArtifactManifest{Schema: "presentation-probe-artifact-v2", ProvenanceClass: ProvenanceCI, RootID: "ci-root-v1", TargetOS: "darwin", TargetArch: "arm64", BuildID: "test-build", ParserSchema: ParserSchema, ArchiveSHA256: SHA256(archive.Bytes()), Files: files}
	raw, err := canonicalArtifactManifest(m)
	if err != nil {
		t.Fatal(err)
	}
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return raw, ed25519.Sign(private, append([]byte(ArtifactDomain+"\x00"), raw...)), archive.Bytes(), m, public
}

func TestArtifactSignatureIsDomainSeparated(t *testing.T) {
	raw, _, archive, manifest, public := makeArtifact(t)
	_, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	trust := ArtifactTrust{Roots: map[string]ed25519.PublicKey{manifest.RootID: public}}
	// The control root differs, so first prove the same root can sign validly.
	trust.Roots[manifest.RootID] = private.Public().(ed25519.PublicKey)
	if _, err := trust.Verify(raw, ed25519.Sign(private, append([]byte(ArtifactDomain+"\x00"), raw...)), archive); err != nil {
		t.Fatal(err)
	}
	for _, domain := range []string{"", CapabilityDomain, ReportDomain, ConfigDomain} {
		signature := ed25519.Sign(private, append([]byte(domain+"\x00"), raw...))
		if _, err := trust.Verify(raw, signature, archive); err == nil {
			t.Fatalf("cross-domain artifact signature accepted for %q", domain)
		}
	}
}
func TestSignedUStarVerifyInMemory(t *testing.T) {
	raw, sig, archive, m, pub := makeArtifact(t)
	got, err := (ArtifactTrust{Roots: map[string]ed25519.PublicKey{m.RootID: pub}}).Verify(raw, sig, archive)
	if err != nil {
		t.Fatal(err)
	}
	if got.ArchiveSHA256 != SHA256(archive) {
		t.Fatal("verified in-memory archive identity mismatch")
	}
}
func TestArtifactRejectsSignatureArchiveAndUStarTampering(t *testing.T) {
	raw, sig, archive, m, pub := makeArtifact(t)
	trust := ArtifactTrust{Roots: map[string]ed25519.PublicKey{m.RootID: pub}}
	badSig := append([]byte(nil), sig...)
	badSig[0] ^= 1
	if _, err := trust.Verify(raw, badSig, archive); err == nil {
		t.Fatal("bad signature accepted")
	}
	badArchive := append([]byte(nil), archive...)
	badArchive[600] ^= 1
	if _, err := trust.Verify(raw, sig, badArchive); err == nil {
		t.Fatal("changed archive accepted")
	}
	badPadding := append([]byte(nil), archive...)
	firstSize := m.Files[0].Size
	pad := 512 + int(firstSize)
	badPadding[pad] = 1
	m.ArchiveSHA256 = SHA256(badPadding)
	newRaw, err := canonicalArtifactManifest(m)
	if err != nil {
		t.Fatal(err)
	}
	_, private, _ := ed25519.GenerateKey(rand.Reader)
	_ = private
	if err := VerifyUStar(badPadding, m); err == nil {
		t.Fatal("nonzero ustar padding accepted")
	}
	_ = newRaw
}

func rewriteUStarChecksum(header []byte) {
	for i := 148; i < 156; i++ {
		header[i] = ' '
	}
	var sum int64
	for _, value := range header {
		sum += int64(value)
	}
	copy(header[148:156], fmt.Sprintf("%06o\x00 ", sum))
}

func TestUStarRejectsEveryIgnoredOrAlternateHeaderEncoding(t *testing.T) {
	_, _, archive, manifest, _ := makeArtifact(t)
	nameEnd := bytes.IndexByte(archive[0:100], 0)
	if nameEnd < 0 || nameEnd+1 >= 100 {
		t.Fatal("fixture name has no padding to mutate")
	}
	mutations := map[string]func([]byte){
		"reserved byte":         func(header []byte) { header[500] = 1 },
		"name padding":          func(header []byte) { header[nameEnd+1] = 1 },
		"owner padding":         func(header []byte) { header[266] = 1 },
		"group padding":         func(header []byte) { header[298] = 1 },
		"link padding":          func(header []byte) { header[158] = 1 },
		"prefix padding":        func(header []byte) { header[346] = 1 },
		"nul regular typeflag":  func(header []byte) { header[156] = 0 },
		"space-padded mode":     func(header []byte) { header[100] = ' ' },
		"space mode terminator": func(header []byte) { header[107] = ' ' },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			changed := append([]byte(nil), archive...)
			mutate(changed[:512])
			rewriteUStarChecksum(changed[:512])
			if err := VerifyUStar(changed, manifest); err == nil {
				t.Fatal("alternate ustar header encoding accepted")
			}
		})
	}

	t.Run("alternate checksum padding", func(t *testing.T) {
		changed := append([]byte(nil), archive...)
		changed[148] = ' '
		if err := VerifyUStar(changed, manifest); err == nil {
			t.Fatal("alternate checksum encoding accepted")
		}
	})
}

func TestArtifactManifestRejectsProductionAndUnsafePaths(t *testing.T) {
	_, _, _, m, _ := makeArtifact(t)
	m.ProvenanceClass = "production"
	if err := m.Validate(); err == nil {
		t.Fatal("production artifact class accepted by C2")
	}
	m.ProvenanceClass = ProvenanceCI
	m.Files[0].Path = "../escape"
	if err := m.Validate(); err == nil {
		t.Fatal("unsafe artifact path accepted")
	}
}
