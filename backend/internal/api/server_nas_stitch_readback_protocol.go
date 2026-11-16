package api

import (
	"errors"
	"path"
	"regexp"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/stitchproof"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
)

// New/unfrozen v2 contract: e38f8e95588264fa579e53a66c1eb39bab84370c680dd8c2aac05865afe562b1.
// These wire checks do not accept a profile, create authority or replay native
// inventories. Endpoint integration must first use the sealed registry lookup.
const nasStitchReadbackSchema = "stoarama.nas-stitch-readback.v2"
const nasStitchReadbackMaxAge = 24 * time.Hour
const nasStitchReadbackFutureSkew = 5 * time.Minute
const nasStitchReaderFreshness = 120 * time.Second

type nasStitchReaderExecution struct {
	ConnectionID int64               `json:"connection_id"`
	AccountID    int64               `json:"account_id"`
	BootID       string              `json:"client_boot_id"`
	Host         stitchproof.RunHost `json:"host"`
	Version      string              `json:"client_version"`
	ArtifactSHA  string              `json:"client_artifact_sha256"`
	ArtifactSize int64               `json:"client_artifact_size_bytes"`
}

type nasStitchReadbackReceipt struct {
	Schema             string                   `json:"schema"`
	NativeRunID        string                   `json:"native_run_id"`
	NativeBundleSHA    string                   `json:"native_bundle_sha256"`
	NativePlanSHA      string                   `json:"native_plan_sha256"`
	ProducerProfileSHA string                   `json:"producer_profile_sha256"`
	ReaderProfileSHA   string                   `json:"reader_profile_sha256"`
	PairSHA            string                   `json:"profile_pair_sha256"`
	ID                 string                   `json:"id"`
	StartedAt          string                   `json:"started_at"`
	VerifiedAt         string                   `json:"verified_at"`
	Reader             nasStitchReaderExecution `json:"reader_execution"`
	Outputs            []stitchproof.Media      `json:"outputs"`
	Policy             stitchproof.Policy       `json:"policy"`
	Tools              stitchproof.PayloadTools `json:"tools"`
	NativeRegistration string                   `json:"native_registration_sha256"`
}

type nasStitchReadbackFacts struct {
	HeartbeatAt         pgtype.Timestamptz
	HostAt              pgtype.Timestamptz
	StatusAt            pgtype.Timestamptz
	InventoryMismatches pgtype.Int8
	Host                nasHostFacts
}

var nasStitchReadbackUTC = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?Z$`)

func parseNASStitchReadbackUTC(value string) (time.Time, error) {
	if !nasStitchReadbackUTC.MatchString(value) {
		return time.Time{}, errors.New("readback requires bounded UTC timestamps")
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil || parsed.Year() < 1 {
		return time.Time{}, errors.New("readback timestamp invalid")
	}
	return parsed, nil
}

func nasStitchReadbackUUID(value string) bool {
	parsed, err := uuid.Parse(value)
	return err == nil && parsed != uuid.Nil && parsed.String() == value
}

func nasStitchReadbackPolicy() (stitchproof.Policy, error) {
	policy, err := cleanupEmbeddedPolicy()
	if err != nil {
		return stitchproof.Policy{}, err
	}
	return stitchproof.Policy{Name: nasStitchPolicy, Revision: nasStitchRevision,
		Keep28SHA256: policy.SHA, CorpusSHA256: nasStitchCorpusSHA, PackageManifestSHA256: nasStitchManifestSHA}, nil
}

func nasStitchReadbackTools() stitchproof.PayloadTools {
	const version = "N-126826-gc0e8b139fd-20260924"
	return stitchproof.PayloadTools{
		FFmpeg:  stitchproof.FFmpeg{Version: version, SHA256: "2388de8e05ad71c913ee1a9c4504a1aa5de09ac100a1de92858dd9ec08e9f98b", SizeBytes: 174165384},
		FFprobe: stitchproof.FFprobe{Version: version, SHA256: "9b2bac862606a48f5bc36091d38e5334cf241538a6f4cc47c60f68e7973a365c", SizeBytes: 173923656},
	}
}

func validNASStitchReaderExecution(r nasStitchReaderExecution) bool {
	return r.ConnectionID == 13 && r.AccountID == 47 && nasStitchReadbackUUID(r.BootID) &&
		r.Version == nasCleanupR2CopyControlClientVersion && stitchSHA(r.ArtifactSHA) && r.ArtifactSize > 0 && r.ArtifactSize <= 8*1024*1024 &&
		r.Host.ConnectionID == 13 && r.Host.System == "Linux" && r.Host.Machine == "x86_64" && r.Host.CpuCount > 0 &&
		r.Host.CpuCount <= 4096 && r.Host.CpuModel != "" && len(r.Host.CpuModel) <= 2048 &&
		strings.IndexFunc(r.Host.CpuModel, func(r rune) bool { return r < 32 || r == 127 }) < 0
}

func decodeNASStitchReadback(raw []byte) (nasStitchReadbackReceipt, error) {
	var receipt nasStitchReadbackReceipt
	if len(raw) == 0 || len(raw) > 65536 || stitchproof.DecodeExact(raw, &receipt) != nil {
		return receipt, errors.New("strict bounded readback receipt required")
	}
	if receipt.Schema != nasStitchReadbackSchema || !nasStitchReadbackUUID(receipt.NativeRunID) || !nasStitchReadbackUUID(receipt.ID) {
		return receipt, errors.New("readback schema/run identity differs")
	}
	for _, digest := range []string{receipt.NativeBundleSHA, receipt.NativePlanSHA, receipt.ProducerProfileSHA,
		receipt.ReaderProfileSHA, receipt.PairSHA, receipt.NativeRegistration, receipt.Reader.ArtifactSHA} {
		if !stitchSHA(digest) {
			return receipt, errors.New("readback digest identity differs")
		}
	}
	r := receipt.Reader
	if !validNASStitchReaderExecution(r) {
		return receipt, errors.New("readback executing reader identity differs")
	}
	if len(receipt.Outputs) != 1 {
		return receipt, errors.New("readback requires the single preserved joined output")
	}
	output := receipt.Outputs[0]
	if !stitchSHA(output.SHA256) || output.SizeBytes <= 0 || output.SizeBytes > 128*1024*1024 || len(output.RelativePath) > 2048 ||
		output.RelativePath == "" || path.IsAbs(output.RelativePath) || path.Clean(output.RelativePath) != output.RelativePath ||
		output.RelativePath == "." || output.RelativePath == ".." || strings.HasPrefix(output.RelativePath, "../") || strings.ContainsAny(output.RelativePath, "\\\x00") {
		return receipt, errors.New("readback output binding differs")
	}
	policy, err := nasStitchReadbackPolicy()
	if err != nil || receipt.Policy != policy || receipt.Tools != nasStitchReadbackTools() {
		return receipt, errors.New("readback policy/tool drift requires new native proof")
	}
	started, err := parseNASStitchReadbackUTC(receipt.StartedAt)
	verified, verifyErr := parseNASStitchReadbackUTC(receipt.VerifiedAt)
	if err != nil || verifyErr != nil || verified.Before(started) {
		return receipt, errors.New("readback time order differs")
	}
	return receipt, nil
}

func validateNASStitchReadbackReader(receipt nasStitchReadbackReceipt, expected nasStitchReaderExecution, facts nasStitchReadbackFacts, now time.Time) error {
	// The caller supplies DB clock_timestamp(), never process/exporter time.
	if now.IsZero() || !facts.InventoryMismatches.Valid || facts.InventoryMismatches.Int64 != 0 ||
		facts.Host.Release == nil || !facts.Host.Release.Stable || facts.Host.Release.Candidate || receipt.Reader != expected {
		return errors.New("current reader inventory/release/identity unavailable")
	}
	if facts.Host.System != expected.Host.System || facts.Host.Machine != expected.Host.Machine ||
		facts.Host.CPUModel != expected.Host.CpuModel || int64(facts.Host.CPUCount) != expected.Host.CpuCount {
		return errors.New("current reader host differs")
	}
	for _, observed := range []pgtype.Timestamptz{facts.HeartbeatAt, facts.HostAt, facts.StatusAt} {
		if !observed.Valid || observed.InfinityModifier != pgtype.Finite || observed.Time.Before(now.Add(-nasStitchReaderFreshness)) || observed.Time.After(now.Add(nasStitchReadbackFutureSkew)) {
			return errors.New("current reader observation stale or unknown")
		}
	}
	verified, err := parseNASStitchReadbackUTC(receipt.VerifiedAt)
	if err != nil || verified.Before(now.Add(-nasStitchReadbackMaxAge)) || verified.After(now.Add(nasStitchReadbackFutureSkew)) {
		return errors.New("preserved readback expired or in future")
	}
	return nil
}
