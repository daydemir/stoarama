package stitchproof

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"

	"golang.org/x/sys/unix"
)

const smallLimit int64 = 1 << 20

var digestPattern = regexp.MustCompile(`^[a-f0-9]{64}$`)
var filePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._/-]*$`)

func digest(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func canonicalFile(name string) bool {
	return filePattern.MatchString(name) && path.Clean(name) == name && !strings.HasPrefix(name, "/") && !strings.HasPrefix(name, "../") && name != "." && name != ".."
}

// Media paths are NAS identities, not artifact file references. The frozen
// contract permits Unicode and spaces; traversal and separators still refuse.
func canonicalMedia(name string) bool {
	return name != "" && len(name) <= 2048 && path.Clean(name) == name && !path.IsAbs(name) && name != "." && name != ".." && !strings.HasPrefix(name, "../") && !strings.ContainsAny(name, "\\\x00\r\n")
}

// openRegular walks pinned directory descriptors. No component may be a
// symlink, including ancestors of the artifact root. os.OpenRoot supplies the
// containment root; openat O_NOFOLLOW prevents symlink races within it.
func openRegular(dir, name string) (*os.File, error) {
	if !canonicalFile(name) || !filepath.IsAbs(dir) || filepath.Clean(dir) != dir {
		return nil, errors.New("artifact path is not canonical")
	}
	base, err := os.Open("/")
	if err != nil {
		return nil, err
	}
	for _, part := range strings.Split(strings.TrimPrefix(dir, "/"), "/") {
		if part == "" {
			continue
		}
		next, err := unix.Openat(int(base.Fd()), part, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		base.Close()
		if err != nil {
			return nil, err
		}
		base = os.NewFile(uintptr(next), part)
	}
	defer base.Close()
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	check, err := root.Open(".")
	if err != nil {
		return nil, err
	}
	defer check.Close()
	a, err := base.Stat()
	if err != nil {
		return nil, err
	}
	b, err := check.Stat()
	if err != nil || !os.SameFile(a, b) {
		return nil, errors.New("artifact root changed")
	}
	parent := base
	parts := strings.Split(name, "/")
	for i, part := range parts {
		flags := unix.O_RDONLY | unix.O_NOFOLLOW | unix.O_CLOEXEC | unix.O_NONBLOCK
		if i < len(parts)-1 {
			flags |= unix.O_DIRECTORY
		}
		fd, err := unix.Openat(int(parent.Fd()), part, flags, 0)
		if parent != base {
			parent.Close()
		}
		if err != nil {
			return nil, err
		}
		parent = os.NewFile(uintptr(fd), part)
	}
	return parent, nil
}

type snapshot struct {
	dir   string
	seen  map[string]bool
	total int64
}

func (s *snapshot) read(ref File, limit int64) ([]byte, error) {
	if s.seen[ref.File] || !digestPattern.MatchString(ref.SHA256) || ref.SizeBytes <= 0 || ref.SizeBytes > limit || s.total+ref.SizeBytes > 64<<20 {
		return nil, errors.New("duplicate, invalid or oversized artifact reference")
	}
	s.seen[ref.File] = true
	f, err := openRegular(s.dir, ref.File)
	if err != nil {
		return nil, fmt.Errorf("artifact %s: %w", ref.File, err)
	}
	defer f.Close()
	before, err := f.Stat()
	if err != nil || !before.Mode().IsRegular() || before.Size() != ref.SizeBytes {
		return nil, errors.New("artifact type/size differs")
	}
	raw, err := io.ReadAll(io.LimitReader(f, ref.SizeBytes+1))
	if err != nil {
		return nil, err
	}
	after, err := f.Stat()
	if err != nil || before.Size() != after.Size() || !before.ModTime().Equal(after.ModTime()) || int64(len(raw)) != ref.SizeBytes || digest(raw) != ref.SHA256 {
		return nil, errors.New("artifact changed or checksum differs")
	}
	s.total += ref.SizeBytes
	return raw, nil
}
