package presentationprobe

import (
	"bytes"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
)

const (
	CapabilitySchema       = "presentation-probe-capability-evidence-v2"
	CapabilityEnvelopeType = "presentation-probe-capability-assessment-v2"
	CapabilityUnrunnable   = "target_unrunnable"
	PinnedDevelopmentRoot  = "stoarama-c2-dev-capability-2026-08"
)

var pinnedDevelopmentKey = [ed25519.PublicKeySize]byte{
	0x8c, 0x1f, 0x8f, 0x10, 0x16, 0x39, 0x9c, 0x09,
	0xac, 0x4d, 0x62, 0xba, 0xdb, 0x0e, 0x98, 0x3a,
	0x8e, 0x7e, 0xb0, 0xdb, 0x8c, 0x0d, 0x12, 0x54,
	0xb9, 0xa0, 0x26, 0x3f, 0xe0, 0x51, 0x1e, 0x9b,
}

var ErrTargetUnrunnable = errors.New(CapabilityUnrunnable)

// TargetCapabilityEvidence is an exact, non-production observation. It does
// not authorize execution and is not a media report, corpus proof, or axis
// fact. Runtime observations apply only to their exact target and OS build.
type TargetCapabilityEvidence struct {
	Schema            string                   `json:"schema"`
	ProvenanceClass   ProvenanceClass          `json:"provenance_class"`
	TargetOS          string                   `json:"target_os"`
	TargetArch        string                   `json:"target_arch"`
	ContractSHA256    string                   `json:"contract_sha256"`
	AssessmentKind    string                   `json:"assessment_kind"`
	HostOS            string                   `json:"host_os"`
	HostArch          string                   `json:"host_arch"`
	ProductVersion    string                   `json:"product_version"`
	OSBuild           string                   `json:"os_build"`
	KernelRelease     string                   `json:"kernel_release"`
	KernelBuild       string                   `json:"kernel_build"`
	ObservationSource string                   `json:"observation_source_sha256"`
	ObservationLog    string                   `json:"observation_log_sha256"`
	Seatbelt          *DarwinSeatbeltEvidence  `json:"darwin_seatbelt,omitempty"`
	Memory            *DarwinMemoryEvidence    `json:"darwin_memory,omitempty"`
	StaticProbe       *StaticProbeToolEvidence `json:"static_probe,omitempty"`
}

type DarwinSeatbeltEvidence struct {
	SandboxInitSucceeded       bool  `json:"sandbox_init_succeeded"`
	PostSandboxMediaPReadBytes int64 `json:"post_sandbox_media_pread_bytes"`
	PostSandboxMediaSeek       bool  `json:"post_sandbox_media_seek_succeeded"`
	PreopenedOutsidePReadBytes int64 `json:"preopened_outside_pread_bytes"`
	NewOutsideOpenErrno        int32 `json:"new_outside_open_errno"`
}

type DarwinMemoryEvidence struct {
	RlimitASSetErrno   int32 `json:"rlimit_as_set_errno"`
	RlimitDataSetErrno int32 `json:"rlimit_data_set_errno"`
	MallocBurstBytes   int64 `json:"malloc_burst_bytes"`
	MallocBurstTouched bool  `json:"malloc_burst_touched"`
	MmapBurstBytes     int64 `json:"mmap_burst_bytes"`
	MmapBurstTouched   bool  `json:"mmap_burst_touched"`
}

type StaticProbeToolEvidence struct {
	ToolchainAvailable bool   `json:"toolchain_available"`
	Reason             string `json:"reason"`
}

type CapabilityAssessment struct {
	Status  string   `json:"status"`
	Reasons []string `json:"reasons"`
}

type CapabilityEnvelope struct {
	Schema          string                   `json:"schema"`
	ProvenanceClass ProvenanceClass          `json:"provenance_class"`
	RootID          string                   `json:"root_id"`
	EvidenceSHA256  string                   `json:"evidence_sha256"`
	Evidence        TargetCapabilityEvidence `json:"evidence"`
	Assessment      CapabilityAssessment     `json:"assessment"`
}

func parseCapabilityEvidence(data []byte) (TargetCapabilityEvidence, error) {
	var evidence TargetCapabilityEvidence
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&evidence); err != nil {
		return evidence, err
	}
	if err := requireJSONEOF(decoder); err != nil {
		return evidence, err
	}
	if _, err := assessTargetCapability(evidence); err != nil {
		return evidence, err
	}
	return evidence, nil
}

func assessTargetCapability(e TargetCapabilityEvidence) (CapabilityAssessment, error) {
	var assessment CapabilityAssessment
	if e.Schema != CapabilitySchema || !e.ProvenanceClass.Valid() || !validTarget(e.TargetOS, e.TargetArch) {
		return assessment, errors.New("capability evidence header invalid")
	}
	if e.ContractSHA256 != ContractRevisionSHA256 || !ValidSHA256(e.ObservationSource) || !ValidSHA256(e.ObservationLog) {
		return assessment, errors.New("capability evidence contract or observation digest invalid")
	}
	for name, value := range map[string]string{"host_os": e.HostOS, "host_arch": e.HostArch, "product_version": e.ProductVersion, "os_build": e.OSBuild, "kernel_release": e.KernelRelease, "kernel_build": e.KernelBuild} {
		if err := ValidateASCII(value, name, 240); err != nil {
			return assessment, err
		}
	}

	reasons := make([]string, 0, 3)
	switch e.AssessmentKind {
	case "pinned_darwin_runtime_spike":
		if e.TargetOS != "darwin" || e.TargetArch != e.HostArch || e.HostOS != "darwin" || e.Seatbelt == nil || e.Memory == nil || e.StaticProbe != nil {
			return assessment, errors.New("Darwin runtime evidence shape invalid")
		}
		if e.TargetArch != "arm64" || e.HostArch != "arm64" || e.ProductVersion != "26.5.2" || e.OSBuild != "25F84" || e.KernelRelease != "25.5.0" || e.KernelBuild != "Darwin Kernel Version 25.5.0: Tue Jun  9 22:26:22 PDT 2026; root:xnu-12377.121.10~1/RELEASE_ARM64_T8132" || e.ObservationSource != "921608282ae400eaa35c66743808748a73c13f43e400e5f8856b88af5f2fe450" || e.ObservationLog != "9b5aaa50b26f70b717eef30fbf03b145a61baf47e17884425714a07565a51803" {
			return assessment, errors.New("Darwin runtime evidence is not the pinned 25F84 observation")
		}
		if !e.Seatbelt.SandboxInitSucceeded || e.Seatbelt.PostSandboxMediaPReadBytes != 5 || !e.Seatbelt.PostSandboxMediaSeek || e.Seatbelt.PreopenedOutsidePReadBytes != 7 {
			return assessment, errors.New("Darwin spike did not reach the required data path")
		}
		reasons = append(reasons, "seatbelt_preopened_vnode_readable")
		if e.Seatbelt.NewOutsideOpenErrno != 1 { // EPERM on the pinned Darwin observation.
			return assessment, errors.New("Darwin outside-open observation is not pinned EPERM")
		}
		if e.Memory.RlimitASSetErrno != 22 || e.Memory.RlimitDataSetErrno != 22 || !e.Memory.MallocBurstTouched || !e.Memory.MmapBurstTouched || e.Memory.MallocBurstBytes != 256<<20 || e.Memory.MmapBurstBytes != 256<<20 {
			return assessment, errors.New("Darwin memory observation does not match the pinned EINVAL/burst result")
		}
		reasons = append(reasons, "hard_memory_limit_unavailable")
	case "toolchain_unavailable":
		if e.Seatbelt != nil || e.Memory != nil || e.StaticProbe == nil || e.StaticProbe.ToolchainAvailable {
			return assessment, errors.New("toolchain-unavailable evidence shape invalid")
		}
		if err := ValidateASCII(e.StaticProbe.Reason, "static probe reason", 128); err != nil {
			return assessment, err
		}
		if e.TargetOS == "linux" {
			reasons = append(reasons, "static_whole_probe_unavailable")
		} else {
			reasons = append(reasons, "darwin_native_probe_unavailable")
		}
	default:
		return assessment, errors.New("unknown capability assessment kind")
	}
	if len(reasons) == 0 {
		// C2 has no runnable native target or implementation. A future positive
		// assessment requires a new contract and code path, not an environment
		// switch or a favorable edit to this evidence.
		return assessment, errors.New("C2 evidence does not establish an unrunnable target")
	}
	sort.Strings(reasons)
	return CapabilityAssessment{Status: CapabilityUnrunnable, Reasons: reasons}, nil
}

func canonicalCapabilityEvidence(e TargetCapabilityEvidence) ([]byte, error) {
	if _, err := assessTargetCapability(e); err != nil {
		return nil, err
	}
	return canonicalJSON(e)
}

func canonicalCapabilityEnvelope(e CapabilityEnvelope) ([]byte, error) {
	assessment, err := assessTargetCapability(e.Evidence)
	if err != nil {
		return nil, err
	}
	evidenceBytes, err := canonicalCapabilityEvidence(e.Evidence)
	if err != nil {
		return nil, err
	}
	if e.Schema != CapabilityEnvelopeType || !e.ProvenanceClass.Valid() || e.ProvenanceClass != e.Evidence.ProvenanceClass || !ValidToken(e.RootID) || e.EvidenceSHA256 != SHA256(evidenceBytes) || e.Assessment.Status != assessment.Status || !equalStrings(e.Assessment.Reasons, assessment.Reasons) {
		return nil, errors.New("capability envelope is incoherent")
	}
	return canonicalJSON(e)
}

func verifyCapabilityEnvelope(data, signature []byte, roots map[string]ed25519.PublicKey) (CapabilityEnvelope, error) {
	var envelope CapabilityEnvelope
	// Checked-in canonical JSON text files may carry exactly one POSIX LF. The
	// authenticated envelope bytes and signature domain exclude that LF.
	data = bytes.TrimSuffix(data, []byte{'\n'})
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&envelope); err != nil {
		return envelope, err
	}
	if err := requireJSONEOF(decoder); err != nil {
		return envelope, err
	}
	canonical, err := canonicalCapabilityEnvelope(envelope)
	if err != nil {
		return envelope, err
	}
	if !bytes.Equal(canonical, data) {
		return envelope, errors.New("capability envelope is not canonical")
	}
	public, ok := roots[envelope.RootID]
	if !ok || len(public) != ed25519.PublicKeySize || len(signature) != ed25519.SignatureSize || !ed25519.Verify(public, append([]byte(CapabilityDomain+"\x00"), data...), signature) {
		return envelope, errors.New("capability envelope signature invalid")
	}
	return envelope, nil
}

// VerifyPinnedCapabilityEnvelope authenticates only the immutable C2
// development trust root. Callers cannot provide or substitute authority.
func VerifyPinnedCapabilityEnvelope(data, signature []byte) (CapabilityEnvelope, error) {
	key := ed25519.PublicKey(pinnedDevelopmentKey[:])
	envelope, err := verifyCapabilityEnvelope(data, signature, map[string]ed25519.PublicKey{PinnedDevelopmentRoot: key})
	if err != nil {
		return CapabilityEnvelope{}, err
	}
	if envelope.RootID != PinnedDevelopmentRoot || envelope.ProvenanceClass != ProvenanceDevelopment || envelope.Evidence.ProvenanceClass != ProvenanceDevelopment {
		return CapabilityEnvelope{}, errors.New("capability envelope class/root is not pinned")
	}
	return envelope, nil
}

// requireTargetRunnable is an intentionally closed gate in C2. It validates
// the exact evidence so malformed bytes cannot masquerade as a supported
// target, then always returns target_unrunnable before any snapshot, handoff,
// parse, launch, report, or fact-producing action exists.
func requireTargetRunnable(e TargetCapabilityEvidence) error {
	assessment, err := assessTargetCapability(e)
	if err != nil {
		return err
	}
	if assessment.Status != CapabilityUnrunnable {
		return fmt.Errorf("unexpected capability status %q", assessment.Status)
	}
	return ErrTargetUnrunnable
}

func canonicalJSON(value any) ([]byte, error) {
	var out bytes.Buffer
	encoder := json.NewEncoder(&out)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(out.Bytes(), []byte{'\n'}), nil
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
