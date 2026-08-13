package presentationprobe

import (
	"bytes"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"
)

const (
	MaximumArchiveBytes = int64(512 << 20)
	MaximumArchiveFiles = 64
	MaximumArtifactFile = int64(256 << 20)
)

type ArtifactFile struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
	Size   int64  `json:"size"`
	Mode   uint32 `json:"mode"`
	Role   string `json:"role"`
}

type ArtifactManifest struct {
	Schema          string          `json:"schema"`
	ProvenanceClass ProvenanceClass `json:"provenance_class"`
	RootID          string          `json:"root_id"`
	TargetOS        string          `json:"target_os"`
	TargetArch      string          `json:"target_arch"`
	BuildID         string          `json:"build_id"`
	ParserSchema    string          `json:"parser_schema"`
	ArchiveSHA256   string          `json:"archive_sha256"`
	Files           []ArtifactFile  `json:"files"`
}

type ArtifactTrust struct {
	Roots map[string]ed25519.PublicKey
}

func DecodeArtifactManifest(data []byte) (ArtifactManifest, error) {
	var manifest ArtifactManifest
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil {
		return manifest, fmt.Errorf("decode artifact manifest: %w", err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return manifest, err
	}
	return manifest, manifest.Validate()
}

func (m ArtifactManifest) Validate() error {
	if m.Schema != "presentation-probe-artifact-v2" || m.ParserSchema != ParserSchema || !m.ProvenanceClass.Valid() {
		return errors.New("artifact schema, parser, or provenance class invalid")
	}
	if !ValidToken(m.RootID) || !ValidToken(m.BuildID) || !ValidSHA256(m.ArchiveSHA256) {
		return errors.New("artifact identity invalid")
	}
	if !validTarget(m.TargetOS, m.TargetArch) {
		return errors.New("unsupported artifact target")
	}
	if len(m.Files) == 0 || len(m.Files) > MaximumArchiveFiles {
		return errors.New("artifact file count invalid")
	}
	seen := map[string]bool{}
	var aggregate int64
	var executable int
	previous := ""
	for _, file := range m.Files {
		if err := validateArchivePath(file.Path); err != nil {
			return err
		}
		if seen[file.Path] || (previous != "" && file.Path <= previous) {
			return errors.New("artifact files are duplicate or unsorted")
		}
		seen[file.Path] = true
		previous = file.Path
		if !ValidSHA256(file.SHA256) || file.Size < 0 || file.Size > MaximumArtifactFile {
			return errors.New("artifact file identity invalid")
		}
		if file.Mode != 0o444 && file.Mode != 0o555 {
			return errors.New("artifact file mode invalid")
		}
		if file.Role != "executable" && file.Role != "runtime" && file.Role != "notice" {
			return errors.New("artifact file role invalid")
		}
		if file.Role == "executable" {
			executable++
		}
		if aggregate > MaximumArchiveBytes-file.Size {
			return errors.New("artifact aggregate size overflow")
		}
		aggregate += file.Size
	}
	if executable != 1 {
		return errors.New("artifact must contain exactly one executable")
	}
	return nil
}

func validTarget(goos, goarch string) bool {
	return (goos == "darwin" || goos == "linux") && (goarch == "arm64" || goarch == "amd64")
}

func (t ArtifactTrust) Verify(manifestBytes, signature, archive []byte) (ArtifactManifest, error) {
	manifest, err := DecodeArtifactManifest(manifestBytes)
	if err != nil {
		return manifest, err
	}
	key := t.Roots[manifest.RootID]
	canonical, err := canonicalArtifactManifest(manifest)
	if err != nil || !bytes.Equal(canonical, manifestBytes) {
		return manifest, errors.New("artifact manifest is not canonical")
	}
	if len(key) != ed25519.PublicKeySize || !ed25519.Verify(key, append([]byte(ArtifactDomain+"\x00"), canonical...), signature) {
		return manifest, errors.New("artifact manifest signature invalid")
	}
	if int64(len(archive)) > MaximumArchiveBytes || SHA256(archive) != manifest.ArchiveSHA256 {
		return manifest, errors.New("artifact archive hash or size mismatch")
	}
	return manifest, VerifyUStar(archive, manifest)
}

// VerifyUStar accepts a deterministic uncompressed POSIX ustar subset. The
// standard reader supplies data boundaries; raw-header checks reject PAX/GNU,
// links, non-zero padding, concatenated archives, and metadata variation.
func VerifyUStar(archive []byte, manifest ArtifactManifest) error {
	if len(archive)%512 != 0 || len(archive) < 1024 {
		return errors.New("ustar size or terminator invalid")
	}
	files := make(map[string]ArtifactFile, len(manifest.Files))
	for _, file := range manifest.Files {
		files[file.Path] = file
	}
	seen := map[string]bool{}
	offset := 0
	zeroBlocks := 0
	for offset+512 <= len(archive) {
		header := archive[offset : offset+512]
		if allZero(header) {
			zeroBlocks++
			offset += 512
			if zeroBlocks == 2 {
				break
			}
			continue
		}
		if zeroBlocks != 0 {
			return errors.New("nonterminal zero archive block")
		}
		if string(header[257:263]) != "ustar\x00" || string(header[263:265]) != "00" {
			return errors.New("archive is not POSIX ustar")
		}
		if header[156] != '0' {
			return errors.New("archive member is not regular")
		}
		if !allZero(header[157:257]) || !allZero(header[265:329]) || !allZero(header[345:512]) {
			return errors.New("archive contains link, owner, prefix, or extension metadata")
		}
		name, err := parseCanonicalString(header[0:100])
		if err != nil {
			return err
		}
		if err := validateArchivePath(name); err != nil {
			return err
		}
		mode, err := parseCanonicalOctal(header[100:108])
		if err != nil {
			return err
		}
		uid, err := parseCanonicalOctal(header[108:116])
		if err != nil {
			return err
		}
		gid, err := parseCanonicalOctal(header[116:124])
		if err != nil {
			return err
		}
		size, err := parseCanonicalOctal(header[124:136])
		if err != nil {
			return err
		}
		mtime, err := parseCanonicalOctal(header[136:148])
		if err != nil {
			return err
		}
		if uid != 0 || gid != 0 || mtime != 0 {
			return errors.New("archive ownership or time is not canonical")
		}
		major, err := parseCanonicalOctal(header[329:337])
		if err != nil {
			return err
		}
		minor, err := parseCanonicalOctal(header[337:345])
		if err != nil {
			return err
		}
		if major != 0 || minor != 0 {
			return errors.New("archive device metadata is not zero")
		}
		stored, err := parseChecksum(header[148:156])
		if err != nil {
			return err
		}
		copyHeader := append([]byte(nil), header...)
		for i := 148; i < 156; i++ {
			copyHeader[i] = ' '
		}
		var checksum int64
		for _, b := range copyHeader {
			checksum += int64(b)
		}
		if checksum != stored {
			return errors.New("archive header checksum mismatch")
		}
		file, ok := files[name]
		if !ok || seen[name] || size != file.Size || uint32(mode) != file.Mode {
			return errors.New("archive member does not match manifest")
		}
		seen[name] = true
		start := offset + 512
		end := start + int(size)
		if end < start || end > len(archive) || SHA256(archive[start:end]) != file.SHA256 {
			return errors.New("archive member bytes mismatch")
		}
		paddedEnd := start + int((size+511)/512*512)
		if paddedEnd > len(archive) || !allZero(archive[end:paddedEnd]) {
			return errors.New("archive padding is nonzero")
		}
		offset = paddedEnd
	}
	if zeroBlocks != 2 || offset != len(archive) {
		return errors.New("archive missing exact terminator or has trailing bytes")
	}
	if len(seen) != len(files) {
		return errors.New("archive member missing")
	}
	return nil
}

func allZero(data []byte) bool {
	for _, value := range data {
		if value != 0 {
			return false
		}
	}
	return true
}
func parseCanonicalString(data []byte) (string, error) {
	end := bytes.IndexByte(data, 0)
	if end < 0 || !allZero(data[end:]) {
		return "", errors.New("archive string field is not canonically zero padded")
	}
	return string(data[:end]), nil
}

func parseCanonicalOctal(data []byte) (int64, error) {
	if len(data) < 2 || data[len(data)-1] != 0 {
		return 0, errors.New("archive octal field terminator invalid")
	}
	value := data[:len(data)-1]
	var result int64
	for _, c := range value {
		if c < '0' || c > '7' {
			return 0, errors.New("non-octal archive field")
		}
		if result > (1<<62)/8 {
			return 0, errors.New("octal overflow")
		}
		result = result*8 + int64(c-'0')
	}
	return result, nil
}
func parseChecksum(data []byte) (int64, error) {
	if len(data) != 8 || data[6] != 0 || data[7] != ' ' {
		return 0, errors.New("archive checksum field terminator invalid")
	}
	return parseCanonicalOctal(append(append([]byte(nil), data[:6]...), 0))
}

func validateArchivePath(name string) error {
	if name == "" || len(name) > 255 || path.IsAbs(name) || strings.Contains(name, "\\") || path.Clean(name) != name {
		return errors.New("unsafe archive path")
	}
	for _, component := range strings.Split(name, "/") {
		if component == "" || component == "." || component == ".." || len(component) > 100 {
			return errors.New("unsafe archive component")
		}
		for _, c := range component {
			if c < 0x21 || c > 0x7e {
				return errors.New("archive path is not safe ASCII")
			}
		}
	}
	return nil
}

func requireJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("trailing JSON value or bytes")
	}
	return nil
}

func canonicalArtifactManifest(m ArtifactManifest) ([]byte, error) {
	if err := m.Validate(); err != nil {
		return nil, err
	}
	// Struct field order is the canonical order. SetEscapeHTML is disabled, but
	// all free text has already been constrained to safe tokens/paths.
	var out bytes.Buffer
	encoder := json.NewEncoder(&out)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(m); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(out.Bytes(), []byte("\n")), nil
}

func DecodeSignatureHex(value string) ([]byte, error) {
	if len(value) != ed25519.SignatureSize*2 {
		return nil, errors.New("signature length invalid")
	}
	decoded, err := hex.DecodeString(value)
	if err != nil {
		return nil, err
	}
	return decoded, nil
}
