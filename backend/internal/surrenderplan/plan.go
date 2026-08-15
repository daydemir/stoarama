// Package surrenderplan defines the deterministic, bounded pre-byte authority
// shared by the recorder API and worker. It deliberately contains no network,
// filesystem, database, or process side effects.
package surrenderplan

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	Version                    = 1
	MaxArtifacts               = 12_288
	MaxWindow                  = 14 * time.Hour
	MaxSegmentTimesArgumentLen = 65_536
	ExecArgumentSafetyMargin   = 65_536
	RecoveryArtifactMaxBytes   = 32 << 20
)

var (
	leafDomain       = []byte("stoarama.recording.capture-artifact-leaf.v1\x00")
	nodeDomain       = []byte("stoarama.recording.capture-artifact-merkle-node.v1\x00")
	emptyDomain      = []byte("stoarama.recording.capture-artifact-empty-leaf.v1\x00")
	secretDomain     = []byte("stoarama.recording.capture-artifact-secret.v1\x00")
	artifactIDDomain = []byte("stoarama.recording.capture-artifact-id.v1\x00")
)

// Plan is the server-authored finite split plan. SplitTimesArgument is a
// canonical minimal base-10 list consumed as one FFmpeg -segment_times value.
type Plan struct {
	PlanAt             time.Time
	WindowEnd          time.Time
	ClipDurationSecond int
	DurationMicro      int64
	ArtifactCount      int
	SplitTimesArgument string
}

// Build derives the only accepted artifact cardinality from DB-authored time
// and the immutable job window. Callers cannot supply ArtifactCount.
func Build(planAt, windowEnd time.Time, clipDurationSecond int) (Plan, error) {
	planAt, windowEnd = planAt.UTC(), windowEnd.UTC()
	if planAt.IsZero() || windowEnd.IsZero() || !windowEnd.After(planAt) {
		return Plan{}, errors.New("window is not open")
	}
	if clipDurationSecond < 5 || clipDurationSecond > 900 {
		return Plan{}, errors.New("clip duration is outside the supported bound")
	}
	duration := windowEnd.Sub(planAt)
	if duration > MaxWindow {
		return Plan{}, errors.New("window exceeds the supported bound")
	}
	durationMicro := duration.Microseconds()
	stepMicro := int64(clipDurationSecond) * int64(time.Second/time.Microsecond)
	count := int((durationMicro + stepMicro - 1) / stepMicro)
	if count < 1 || count > MaxArtifacts {
		return Plan{}, errors.New("artifact count exceeds the supported bound")
	}
	var split strings.Builder
	for ordinal := 1; ordinal < count; ordinal++ {
		if ordinal > 1 {
			split.WriteByte(',')
		}
		split.WriteString(strconv.FormatInt(int64(ordinal)*int64(clipDurationSecond), 10))
	}
	if split.Len() > MaxSegmentTimesArgumentLen {
		return Plan{}, errors.New("segment_times argument exceeds the supported bound")
	}
	return Plan{
		PlanAt:             planAt,
		WindowEnd:          windowEnd,
		ClipDurationSecond: clipDurationSecond,
		DurationMicro:      durationMicro,
		ArtifactCount:      count,
		SplitTimesArgument: split.String(),
	}, nil
}

// ExecFits proves the exact executable+argv+environment representation fits a
// platform ARG_MAX budget without logging or returning any inherited value.
func ExecFits(executable string, argv, environment []string, argMax int) bool {
	if argMax <= ExecArgumentSafetyMargin || len(executable) == 0 {
		return false
	}
	bytes := len(executable) + 1
	entries := 1
	for _, value := range argv {
		bytes += len(value) + 1
		entries++
	}
	for _, value := range environment {
		bytes += len(value) + 1
		entries++
	}
	// POSIX exec also copies argv/env pointer arrays. Use eight bytes even on a
	// 32-bit target so the proof is conservative.
	bytes += (entries + 2) * 8
	return bytes <= argMax-ExecArgumentSafetyMargin
}

type SetIdentity struct {
	SetID                   uuid.UUID
	AccountID               int64
	RecordingID             int64
	JobID                   int64
	LeaseToken              uuid.UUID
	OriginClaimGeneration   int64
	ProducerID              uuid.UUID
	SnapshotSHA256          string
	DestinationNamingSHA256 string
	ArtifactCount           int
	MIME                    string
	MaxBytes                int64
}

func (s SetIdentity) Validate() error {
	if s.SetID == uuid.Nil || s.AccountID <= 0 || s.RecordingID <= 0 || s.JobID <= 0 || s.LeaseToken == uuid.Nil || s.OriginClaimGeneration <= 0 || s.ProducerID == uuid.Nil {
		return errors.New("set identity is incomplete")
	}
	if s.ArtifactCount < 1 || s.ArtifactCount > MaxArtifacts || s.MaxBytes < 1 || s.MaxBytes > RecoveryArtifactMaxBytes {
		return errors.New("set bounds are invalid")
	}
	if s.MIME != "video/mp4" || !validSHA256(s.SnapshotSHA256) || !validSHA256(s.DestinationNamingSHA256) {
		return errors.New("set media or digest identity is invalid")
	}
	return nil
}

type Artifact struct {
	Ordinal            int
	ID                 uuid.UUID
	RecoverySecret     [32]byte
	RecoverySecretHash [32]byte
}

func DeriveArtifact(seed [32]byte, set SetIdentity, ordinal int) (Artifact, error) {
	if err := set.Validate(); err != nil {
		return Artifact{}, err
	}
	if ordinal < 1 || ordinal > set.ArtifactCount {
		return Artifact{}, errors.New("artifact ordinal is outside the set")
	}
	context := make([]byte, 0, 20)
	context = append(context, set.SetID[:]...)
	var encoded [4]byte
	binary.BigEndian.PutUint32(encoded[:], uint32(ordinal))
	context = append(context, encoded[:]...)
	secretBytes := derive(seed[:], secretDomain, context)
	idBytes := derive(seed[:], artifactIDDomain, context)
	// RFC 4122 variant/version bits produce a canonical UUID while retaining
	// 122 deterministic bits from the domain-separated PRF.
	idBytes[6] = (idBytes[6] & 0x0f) | 0x50
	idBytes[8] = (idBytes[8] & 0x3f) | 0x80
	id, err := uuid.FromBytes(idBytes[:16])
	if err != nil {
		return Artifact{}, err
	}
	var secret, secretHash [32]byte
	copy(secret[:], secretBytes)
	secretHash = sha256.Sum256(secret[:])
	return Artifact{Ordinal: ordinal, ID: id, RecoverySecret: secret, RecoverySecretHash: secretHash}, nil
}

func LeafHash(set SetIdentity, artifact Artifact) ([32]byte, error) {
	if sha256.Sum256(artifact.RecoverySecret[:]) != artifact.RecoverySecretHash {
		return [32]byte{}, errors.New("artifact recovery secret is invalid")
	}
	return LeafHashCommitment(set, artifact.Ordinal, artifact.ID, artifact.RecoverySecretHash)
}

// LeafHashCommitment allows the server to authenticate a materialized leaf
// while retaining only H(secret). The plaintext recovery secret never crosses
// the pre-byte commitment request.
func LeafHashCommitment(set SetIdentity, ordinal int, artifactID uuid.UUID, recoverySecretHash [32]byte) ([32]byte, error) {
	if err := set.Validate(); err != nil {
		return [32]byte{}, err
	}
	if ordinal < 1 || ordinal > set.ArtifactCount || artifactID == uuid.Nil {
		return [32]byte{}, errors.New("artifact identity is invalid")
	}
	h := sha256.New()
	_, _ = h.Write(leafDomain)
	writeUUID(h, 1, set.SetID)
	writeInt64(h, 2, set.AccountID)
	writeInt64(h, 3, set.RecordingID)
	writeInt64(h, 4, set.JobID)
	writeUUID(h, 5, set.LeaseToken)
	writeInt64(h, 6, set.OriginClaimGeneration)
	writeUUID(h, 7, set.ProducerID)
	writeBytes(h, 8, mustDecodeSHA256(set.SnapshotSHA256))
	writeBytes(h, 9, mustDecodeSHA256(set.DestinationNamingSHA256))
	writeInt64(h, 10, int64(set.ArtifactCount))
	writeInt64(h, 11, int64(ordinal))
	writeUUID(h, 12, artifactID)
	writeBytes(h, 13, recoverySecretHash[:])
	writeString(h, 14, set.MIME)
	writeInt64(h, 15, set.MaxBytes)
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out, nil
}

// VerifyCommittedProof verifies a materialized leaf without receiving the
// recovery secret itself.
func VerifyCommittedProof(root [32]byte, set SetIdentity, ordinal int, artifactID uuid.UUID, recoverySecretHash [32]byte, proof Proof) bool {
	if proof.Ordinal != ordinal || set.Validate() != nil {
		return false
	}
	leaf, err := LeafHashCommitment(set, ordinal, artifactID, recoverySecretHash)
	if err != nil {
		return false
	}
	return verifyLeafProof(root, set, ordinal, leaf, proof)
}

type Proof struct {
	Ordinal  int
	Siblings [][32]byte
}

// Tree is the bounded in-memory commitment tree for one capture set. Workers
// build it once before launch; per-file materialization is then O(log N).
type Tree struct {
	set    SetIdentity
	levels [][][32]byte
}

func BuildTree(seed [32]byte, set SetIdentity) (*Tree, error) {
	if err := set.Validate(); err != nil {
		return nil, err
	}
	width := nextPowerOfTwo(set.ArtifactCount)
	level := make([][32]byte, width)
	for index := 0; index < width; index++ {
		if index < set.ArtifactCount {
			artifact, err := DeriveArtifact(seed, set, index+1)
			if err != nil {
				return nil, err
			}
			level[index], err = LeafHash(set, artifact)
			if err != nil {
				return nil, err
			}
		} else {
			level[index] = emptyLeaf(set.SetID, set.ArtifactCount, index+1)
		}
	}
	tree := &Tree{set: set, levels: [][][32]byte{level}}
	for len(level) > 1 {
		next := make([][32]byte, len(level)/2)
		for index := range next {
			next[index] = parent(level[index*2], level[index*2+1])
		}
		level = next
		tree.levels = append(tree.levels, level)
	}
	return tree, nil
}

func (t *Tree) Root() [32]byte {
	if t == nil || len(t.levels) == 0 {
		return [32]byte{}
	}
	return t.levels[len(t.levels)-1][0]
}

func (t *Tree) Proof(ordinal int) (Proof, error) {
	if t == nil || ordinal < 1 || ordinal > t.set.ArtifactCount {
		return Proof{}, errors.New("proof ordinal is outside the set")
	}
	proof := Proof{Ordinal: ordinal}
	position := ordinal - 1
	for level := 0; level < len(t.levels)-1; level++ {
		proof.Siblings = append(proof.Siblings, t.levels[level][position^1])
		position /= 2
	}
	return proof, nil
}

func RootAndProof(seed [32]byte, set SetIdentity, proofOrdinal int) ([32]byte, Proof, error) {
	tree, err := BuildTree(seed, set)
	if err != nil {
		return [32]byte{}, Proof{}, err
	}
	proof, err := tree.Proof(proofOrdinal)
	return tree.Root(), proof, err
}

func VerifyProof(root [32]byte, set SetIdentity, artifact Artifact, proof Proof) bool {
	if proof.Ordinal != artifact.Ordinal || set.Validate() != nil {
		return false
	}
	leaf, err := LeafHash(set, artifact)
	if err != nil {
		return false
	}
	return verifyLeafProof(root, set, artifact.Ordinal, leaf, proof)
}

func verifyLeafProof(root [32]byte, set SetIdentity, ordinal int, leaf [32]byte, proof Proof) bool {
	width := nextPowerOfTwo(set.ArtifactCount)
	wantDepth := 0
	for width > 1 {
		wantDepth++
		width /= 2
	}
	if len(proof.Siblings) != wantDepth {
		return false
	}
	position := ordinal - 1
	current := leaf
	for _, sibling := range proof.Siblings {
		if position&1 == 0 {
			current = parent(current, sibling)
		} else {
			current = parent(sibling, current)
		}
		position /= 2
	}
	return hmac.Equal(current[:], root[:])
}

func derive(key, domain, context []byte) []byte {
	h := hmac.New(sha256.New, key)
	_, _ = h.Write(domain)
	_, _ = h.Write(context)
	return h.Sum(nil)
}

func parent(left, right [32]byte) [32]byte {
	h := sha256.New()
	_, _ = h.Write(nodeDomain)
	_, _ = h.Write(left[:])
	_, _ = h.Write(right[:])
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

func emptyLeaf(setID uuid.UUID, count, index int) [32]byte {
	h := sha256.New()
	_, _ = h.Write(emptyDomain)
	_, _ = h.Write(setID[:])
	var encoded [8]byte
	binary.BigEndian.PutUint32(encoded[0:4], uint32(count))
	binary.BigEndian.PutUint32(encoded[4:8], uint32(index))
	_, _ = h.Write(encoded[:])
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

func nextPowerOfTwo(value int) int {
	width := 1
	for width < value {
		width <<= 1
	}
	return width
}

type hashWriter interface{ Write([]byte) (int, error) }

func writeUUID(w hashWriter, tag byte, value uuid.UUID) { writeBytes(w, tag, value[:]) }
func writeString(w hashWriter, tag byte, value string)  { writeBytes(w, tag, []byte(value)) }
func writeInt64(w hashWriter, tag byte, value int64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], uint64(value))
	writeBytes(w, tag, encoded[:])
}
func writeBytes(w hashWriter, tag byte, value []byte) {
	var prefix [6]byte
	prefix[0] = tag
	prefix[1] = 1 // present; zero is reserved for an explicit NULL value.
	binary.BigEndian.PutUint32(prefix[2:], uint32(len(value)))
	_, _ = w.Write(prefix[:])
	_, _ = w.Write(value)
}

func validSHA256(value string) bool {
	if len(value) != 64 || strings.ToLower(value) != value {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func mustDecodeSHA256(value string) []byte {
	decoded, err := hex.DecodeString(value)
	if err != nil {
		panic(fmt.Sprintf("invalid validated SHA-256: %v", err))
	}
	return decoded
}
