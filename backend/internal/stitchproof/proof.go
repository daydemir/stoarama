// Package stitchproof verifies the separately reviewed real NAS stitch prerequisite.
// It grants no keep-set, custody, fence, writer or provider exception.
package stitchproof

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	VerifierSHA   = "81ff91e5accd93d32ff475024c10281177684b583e121a6c5d0c1f1938d49532"
	ClientSHA     = "06425b3a518ae327cf322b7ef19d8c35917737e14a78182b9d8671b8f3dfc411"
	clientHead    = "4e433a0f9203b579f8f7e55f2b3e25557367d3e4"
	clientMerge   = "2ea3ef181cb5d07ba57a809993ce4c9104a6326e"
	sourcePlanSHA = "0dfa6b172f33bd55e409729ecd517d492cdee03427b028ac226788e530a72726"
	MaxAge        = 24 * time.Hour
	futureSkew    = 5 * time.Minute
)

// Provenance is safe to persist in a retirement ledger. It contains no media,
// credentials, executable bytes or mutable references to parsed evidence.
type Provenance struct {
	OuterSHA    string    `json:"outer_sha256"`
	PayloadSHA  string    `json:"payload_sha256"`
	BundleSHA   string    `json:"bundle_sha256"`
	ReviewSHA   string    `json:"review_sha256"`
	ClientSHA   string    `json:"client_sha256"`
	VerifierSHA string    `json:"verifier_sha256"`
	CodeHead    string    `json:"code_head"`
	NASRunID    string    `json:"nas_run_id"`
	ExpiresAt   time.Time `json:"expires_at"`
}

// Authority can only be initialized by successful verification. Its fields and
// clock are private, and BindRetirement returns a copy rather than extending it.
type Authority struct {
	utc, observed              time.Time
	provenance                 Provenance
	manifest, bucket, endpoint string
}

func (a *Authority) Provenance() Provenance { return a.provenance }
func (a *Authority) ManifestSHA() string    { return a.manifest }

// CurrentUTC also permits recording an observed failure after expiry. It never
// renews authority; Check/RequireWire remain mandatory before protected work.
func (a *Authority) CurrentUTC() (time.Time, error) {
	if a == nil || a.utc.IsZero() || a.observed.IsZero() {
		return time.Time{}, errors.New("missing verified DB clock")
	}
	return a.now(), nil
}

func (a *Authority) now() time.Time { return a.utc.Add(time.Since(a.observed)) }

func (a *Authority) Check() error {
	if a == nil || a.utc.IsZero() || a.observed.IsZero() || a.provenance.OuterSHA == "" || a.provenance.ExpiresAt.IsZero() || time.Since(a.observed) < 0 || !a.now().Before(a.provenance.ExpiresAt) {
		return errors.New("verified NAS stitching authority is missing or expired")
	}
	return nil
}

func (a *Authority) Remaining() (time.Duration, error) {
	if err := a.Check(); err != nil {
		return 0, err
	}
	remaining := a.provenance.ExpiresAt.Sub(a.now())
	if remaining <= 0 {
		return 0, errors.New("NAS stitching authority expired before transport")
	}
	return remaining, nil
}

func (a *Authority) BindRetirement(manifest string, created time.Time, age time.Duration, bucket, endpoint string) (*Authority, error) {
	if err := a.Check(); err != nil {
		return nil, err
	}
	if !digestPattern.MatchString(manifest) || !digestPattern.MatchString(endpoint) || bucket != "stoarama" || age <= 0 || age > MaxAge || created.IsZero() || created.After(a.now().Add(futureSkew)) {
		return nil, errors.New("invalid retirement manifest binding")
	}
	b := *a
	b.manifest, b.bucket, b.endpoint = manifest, bucket, endpoint
	if expiry := created.Add(age); expiry.Before(b.provenance.ExpiresAt) {
		b.provenance.ExpiresAt = expiry
	}
	return &b, b.Check()
}

// LimitDeadline returns an independent authority capped by another immutable
// evidence deadline. A provider or per-object custody check can only shorten
// authority; it cannot refresh the original proof or another object's lifetime.
func (a *Authority) LimitDeadline(expiry time.Time) (*Authority, error) {
	if err := a.Check(); err != nil {
		return nil, err
	}
	if expiry.IsZero() {
		return nil, errors.New("missing evidence expiry")
	}
	b := *a
	if expiry.Before(b.provenance.ExpiresAt) {
		b.provenance.ExpiresAt = expiry
	}
	return &b, b.Check()
}

// RequireWire is also checked in the shared SDK immediately before its one wire
// attempt. A proof verified for inspection alone is not a retirement binding.
func (a *Authority) RequireWire(bucket, endpoint string) error {
	if err := a.Check(); err != nil {
		return err
	}
	if a.manifest == "" || a.bucket != bucket || a.endpoint != endpoint {
		return errors.New("NAS stitch authority is not bound to this retirement target")
	}
	return nil
}

// Verify uses authoritative DB UTC observed at the caller's monotonic sample
// taken BEFORE its clock query. Network/replay time consumes authority; wall
// clock changes cannot extend it. All execution inputs are copied before Python.
func Verify(ctx context.Context, filename, expectedSHA string, databaseUTC, observed time.Time) (*Authority, error) {
	if !digestPattern.MatchString(expectedSHA) || !filepath.IsAbs(filename) || filepath.Clean(filename) != filename || databaseUTC.IsZero() || observed.IsZero() || time.Since(observed) < 0 {
		return nil, errors.New("canonical artifact path, external digest and DB clock required")
	}
	s := snapshot{dir: filepath.Dir(filename), seen: map[string]bool{}}
	f, err := openRegular(s.dir, filepath.Base(filename))
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	f.Close()
	if err != nil {
		return nil, err
	}
	raw, err := s.read(File{File: filepath.Base(filename), SHA256: expectedSHA, SizeBytes: st.Size()}, smallLimit)
	if err != nil {
		return nil, err
	}
	var outer Attestation
	if err := decode(raw, &outer); err != nil {
		return nil, err
	}
	if outer.Schema != "stoarama.nas-stitch-attestation.v1" || outer.Synthetic {
		return nil, errors.New("real NAS attestation required; synthetic evidence has no authority")
	}
	payloadRaw, err := s.read(outer.Payload, smallLimit)
	if err != nil {
		return nil, err
	}
	reviewRaw, err := s.read(outer.IndependentReview, smallLimit)
	if err != nil {
		return nil, err
	}
	var p Payload
	var r Review
	if err := decode(payloadRaw, &p); err != nil {
		return nil, err
	}
	if err := decode(reviewRaw, &r); err != nil {
		return nil, err
	}
	if err := pins(p); err != nil {
		return nil, err
	}
	if r.Schema != "stoarama.nas-stitch-review.v1" || r.Verdict != "approve" || r.PayloadSHA256 != outer.Payload.SHA256 || r.BundleSHA256 != p.Evidence.NativeBundle.SHA256 || r.ReviewedCodeHeadSHA != clientHead {
		return nil, errors.New("independent postrun review does not bind this proof")
	}
	for _, value := range []string{r.Reviewer.Name, r.Reviewer.Provider, r.Reviewer.Model, r.Reviewer.Reference} {
		if strings.TrimSpace(value) == "" {
			return nil, errors.New("independent reviewer identity/reference missing")
		}
	}
	if _, err := s.read(r.Observations, smallLimit); err != nil {
		return nil, err
	}
	validationRaw, err := s.read(p.Evidence.ServerValidation, smallLimit)
	if err != nil {
		return nil, err
	}
	var v Validation
	if err := decode(validationRaw, &v); err != nil {
		return nil, err
	}
	inputs := []struct {
		ref   File
		limit int64
	}{
		{p.Evidence.Verifier, 64 << 10}, {p.Evidence.ClientArtifact, 8 << 20},
		{p.Evidence.SourcePlan, smallLimit}, {p.Evidence.NativeBundle, smallLimit}, {p.Evidence.LocalRegistration, 16 << 20},
	}
	data := make([][]byte, len(inputs))
	for i, in := range inputs {
		data[i], err = s.read(in.ref, in.limit)
		if err != nil {
			return nil, err
		}
	}
	verifier, client, plan, bundle, registration := data[0], data[1], data[2], data[3], data[4]
	decoded, err := inflate(bundle)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(decoded, registration) {
		return nil, errors.New("local registration differs from complete native receipt")
	}
	// The pinned verifier owns strict native/plan decoding and their complete
	// schema. The outer schema's null prohibition must not redefine raw_proof.
	result, err := replay(ctx, verifier, client, plan, bundle, p.Evidence.SourcePlan.SHA256, p.Evidence.NativeBundle.SHA256)
	if err != nil {
		return nil, err
	}
	if err := bindings(p, v, r, decoded, plan, result); err != nil {
		return nil, err
	}
	a := &Authority{utc: databaseUTC, observed: observed, provenance: Provenance{OuterSHA: expectedSHA, PayloadSHA: outer.Payload.SHA256, BundleSHA: p.Evidence.NativeBundle.SHA256, ReviewSHA: outer.IndependentReview.SHA256, ClientSHA: ClientSHA, VerifierSHA: VerifierSHA, CodeHead: clientHead, NASRunID: p.Run.ID}}
	if err := a.times(p, v, r); err != nil {
		return nil, err
	}
	return a, a.Check()
}

func pins(p Payload) error {
	policy := Policy{"v85r", "v85r-rebuilt-1", "060aa7070c7909d78013415f83b76bd0c027a885caf91c67c8b709ac1f4b5811", "5b2765ecb4f6483c8e48b5a6c958c7c0d8082764d4da57901626a22569c6346b", "337c606985e930abf1df459cbbda0c674f13bd9d92e11013131e2b2348e78a06"}
	pipeline := Pipeline{"daydemir/stoarama", "a474f0d52f8558a8033813d1d158ca8567786bdd", clientHead, clientMerge, "v85r-nas-proof-v1", ClientSHA}
	tools := PayloadTools{FFmpeg{"N-126826-gc0e8b139fd-20260924", "2388de8e05ad71c913ee1a9c4504a1aa5de09ac100a1de92858dd9ec08e9f98b", 174165384}, FFprobe{"N-126826-gc0e8b139fd-20260924", "9b2bac862606a48f5bc36091d38e5334cf241538a6f4cc47c60f68e7973a365c", 173923656}}
	if p.Schema != "stoarama.nas-stitch-payload.v1" || p.ConnectionID != 13 || p.AccountID != 47 || p.Policy != policy || p.Pipeline != pipeline || p.Tools != tools || p.Scope.StreamID != 17208 || p.Scope.RecordingID != 444 || p.Scope.SourceTaskID != 1 || p.Scope.SourcePlanSHA256 != sourcePlanSHA || !digestPattern.MatchString(p.Scope.ProofPlanSHA256) || !reflect.DeepEqual(p.Scope.ClipIDs, []int64{373218, 373262}) || p.Run.Host.ConnectionID != 13 || p.Run.Host.System != "Linux" || p.Run.Host.Machine != "x86_64" || len(p.Inputs) != 2 || len(p.Outputs) != 1 || p.Verification.CurrentPolicy != "passed" || p.Verification.NativeInventoryReplay != "passed" || p.Verification.ServerBinding != "passed" || !digestPattern.MatchString(p.Verification.ResultSHA256) || p.Evidence.Verifier.SHA256 != VerifierSHA || p.Evidence.Verifier.SizeBytes != 18817 || p.Evidence.ClientArtifact.SHA256 != ClientSHA || p.Evidence.ClientArtifact.SizeBytes != 4822304 {
		return errors.New("NAS stitching proof scope, policy, pipeline or executable pin differs")
	}
	for _, id := range []string{p.Run.ID, p.Run.ClientBootID} {
		u, err := uuid.Parse(id)
		if err != nil || u.String() != id || u == uuid.Nil {
			return errors.New("invalid proof UUID")
		}
	}
	for i, in := range p.Inputs {
		if in.ClipID != []int64{373218, 373262}[i] || !canonicalMedia(in.RelativePath) || !digestPattern.MatchString(in.SHA256) {
			return errors.New("proof input identity differs")
		}
	}
	out := p.Outputs[0]
	if !canonicalMedia(out.RelativePath) || !digestPattern.MatchString(out.SHA256) || out.ReadbackSHA256 != out.SHA256 || !reflect.DeepEqual(out.SourceClipIDs, p.Scope.ClipIDs) {
		return errors.New("proof output identity differs")
	}
	return nil
}

func inflate(bundle []byte) ([]byte, error) {
	in := bytes.NewReader(bundle)
	g, err := gzip.NewReader(in)
	if err != nil {
		return nil, err
	}
	g.Multistream(false)
	raw, err := io.ReadAll(io.LimitReader(g, 16<<20+1))
	closeErr := g.Close()
	if err != nil || closeErr != nil || len(raw) > 16<<20 || in.Len() != 0 {
		return nil, errors.New("invalid, trailing or oversized native gzip")
	}
	return raw, nil
}

func bindings(p Payload, v Validation, r Review, raw, plan []byte, replayed Replay) error {
	var receipt struct {
		Run struct {
			ID           string  `json:"id"`
			ClientBootID string  `json:"client_boot_id"`
			Host         RunHost `json:"host"`
			StartedAt    string  `json:"started_at"`
			CompletedAt  string  `json:"completed_at"`
		} `json:"run"`
		ClientVersion string          `json:"client_version"`
		ClientSHA     string          `json:"client_artifact_sha256"`
		PlanSHA       string          `json:"plan_sha256"`
		SourceSHA     string          `json:"source_plan_sha256"`
		Result        json.RawMessage `json:"result"`
		Wall          float64         `json:"build_wall_seconds"`
		VerifiedAt    string          `json:"verified_at"`
	}
	// The exact pinned Python replay has already validated the complete native
	// schema. This partial decode only joins its facts to the outer artifact.
	if err := json.Unmarshal(raw, &receipt); err != nil {
		return err
	}
	var nativeResult struct {
		Outputs []struct {
			RelativePath   string `json:"relative_path"`
			SHA256         string `json:"sha256"`
			SizeBytes      int64  `json:"size_bytes"`
			ReadbackSHA256 string `json:"readback_sha256"`
		} `json:"outputs"`
	}
	if err := json.Unmarshal(receipt.Result, &nativeResult); err != nil {
		return err
	}
	var task struct {
		PlanSHA string `json:"plan_sha256"`
		Plan    struct {
			Clips []struct {
				SourceID     int64  `json:"source_id"`
				RelativePath string `json:"relative_path"`
				SHA256       string `json:"sha256"`
				SizeBytes    int64  `json:"size_bytes"`
			} `json:"clips"`
		} `json:"plan"`
	}
	if err := json.Unmarshal(plan, &task); err != nil {
		return err
	}
	if receipt.Run.ID != p.Run.ID || receipt.Run.ClientBootID != p.Run.ClientBootID || receipt.Run.Host != p.Run.Host || receipt.Run.StartedAt != p.Run.StartedAt || receipt.Run.CompletedAt != p.Run.CompletedAt || receipt.Wall != p.Run.BuildWallSeconds || receipt.ClientSHA != ClientSHA || receipt.ClientVersion != p.Pipeline.ClientVersion || receipt.SourceSHA != sourcePlanSHA || receipt.PlanSHA != p.Scope.ProofPlanSHA256 || task.PlanSHA != receipt.PlanSHA || digest(receipt.Result) != p.Verification.ResultSHA256 || len(task.Plan.Clips) != 2 {
		return errors.New("native receipt/plan/payload binding differs")
	}
	for i, c := range task.Plan.Clips {
		if p.Inputs[i] != (Input{c.SourceID, c.RelativePath, c.SHA256, c.SizeBytes}) {
			return errors.New("native source identity differs")
		}
	}
	out := p.Outputs[0]
	if len(nativeResult.Outputs) != 1 || nativeResult.Outputs[0].RelativePath != out.RelativePath || nativeResult.Outputs[0].SHA256 != out.SHA256 || nativeResult.Outputs[0].SizeBytes != out.SizeBytes || nativeResult.Outputs[0].ReadbackSHA256 != out.ReadbackSHA256 {
		return errors.New("native output path/identity differs")
	}
	verified, err := utc(receipt.VerifiedAt)
	if err != nil {
		return err
	}
	completed, err := utc(p.Run.CompletedAt)
	if err != nil {
		return err
	}
	readback, err := utc(v.NasReadback.VerifiedAt)
	if err != nil || verified.Before(completed) || verified.After(readback) {
		return errors.New("native/readback timestamps conflict")
	}
	if replayed.Schema != "stoarama.nas-stitch-native-replay.v1" || replayed.NativeReplay != "passed" || replayed.ConnectionID != 13 || replayed.RunID != p.Run.ID || replayed.ReceiptSHA256 != p.Evidence.NativeBundle.SHA256 || replayed.PlanSHA256 != p.Scope.ProofPlanSHA256 || replayed.ClientArtifactSHA256 != ClientSHA || replayed.OutputSHA256 != out.SHA256 || replayed.OutputSizeBytes != out.SizeBytes || replayed.MediaSeconds != p.Run.MediaSeconds || replayed.BuildWallSeconds != p.Run.BuildWallSeconds || replayed.RecordingHoursPerWallHour != replayed.MediaSeconds/replayed.BuildWallSeconds {
		return errors.New("native replay output differs from payload")
	}
	if v.Schema != "stoarama.nas-stitch-validation.v1" || v.RunID != p.Run.ID || v.ProofPlanSHA256 != p.Scope.ProofPlanSHA256 || v.ClientArtifactSHA256 != ClientSHA || v.Policy != p.Policy || v.Tools != p.Tools || v.ServerBinding != (ValidationServerBinding{true, "nas-only-v85r-proof-v1", p.Scope.ProofPlanSHA256, out.SHA256, true, false}) || v.NasReadback.RunID != p.Run.ID || v.NasReadback.ReceiptSHA256 != p.Evidence.NativeBundle.SHA256 || len(v.NasReadback.Outputs) != 1 || v.NasReadback.Outputs[0] != (Media{out.RelativePath, out.SHA256, out.SizeBytes}) {
		return errors.New("fresh server/NAS readback binding differs")
	}
	u, err := uuid.Parse(v.NasReadback.ClientBootID)
	if err != nil || u.String() != v.NasReadback.ClientBootID || u == uuid.Nil {
		return errors.New("invalid current NAS readback boot")
	}
	return nil
}

func utc(raw string) (time.Time, error) {
	t, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil || !strings.HasSuffix(raw, "Z") {
		return time.Time{}, fmt.Errorf("invalid UTC evidence timestamp %q", raw)
	}
	return t, nil
}

func (a *Authority) times(p Payload, v Validation, r Review) error {
	var previous time.Time
	for i, raw := range []string{p.Run.StartedAt, p.Run.CompletedAt, v.NasReadback.VerifiedAt, v.ValidatedAt, r.ReviewedAt} {
		t, err := utc(raw)
		if err != nil {
			return err
		}
		if t.Before(previous) || t.After(a.now().Add(futureSkew)) || i == 1 && !t.After(previous) {
			return errors.New("proof timestamps conflict or are in the future")
		}
		if i >= 2 {
			if a.now().Sub(t) > MaxAge {
				return errors.New("NAS output readback, validation or independent review is stale")
			}
			if expiry := t.Add(MaxAge); a.provenance.ExpiresAt.IsZero() || expiry.Before(a.provenance.ExpiresAt) {
				a.provenance.ExpiresAt = expiry
			}
		}
		previous = t
	}
	return nil
}
