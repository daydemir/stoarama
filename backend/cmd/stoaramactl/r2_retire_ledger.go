package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/daydemir/stoarama/backend/internal/stitchproof"
	"github.com/google/uuid"
)

type r2RetireLedgerEvent string

const (
	r2LedgerAuthorized r2RetireLedgerEvent = "run_authorized"
	r2LedgerIntent     r2RetireLedgerEvent = "intent"
	r2LedgerResume     r2RetireLedgerEvent = "resume_authorized"
)

type r2RetireTargetKind string

const r2RetireTargetRaw r2RetireTargetKind = "raw"

// Comparable complete identity. UTC timestamps are normalized before indexing.
type r2RetireTarget struct {
	ManifestSHA  string             `json:"manifest_sha256"`
	Bucket       string             `json:"bucket"`
	EndpointSHA  string             `json:"endpoint_sha256"`
	Kind         r2RetireTargetKind `json:"kind"`
	SourceID     int64              `json:"source_id"`
	RecordingID  int64              `json:"recording_id"`
	StreamID     int64              `json:"stream_id"`
	Key          string             `json:"key"`
	RelativePath string             `json:"relative_path"`
	StartAt      time.Time          `json:"start_at"`
	EndAt        time.Time          `json:"end_at"`
	SizeBytes    int64              `json:"size_bytes"`
	SHA256       string             `json:"sha256"`
	ETag         string             `json:"etag"`
}

func (t r2RetireTarget) valid() bool {
	return r2RetireLowerHex64(t.ManifestSHA) && r2RetireLowerHex64(t.EndpointSHA) && t.Bucket == "stoarama" && t.Kind == r2RetireTargetRaw && t.SourceID > 0 && t.RecordingID > 0 && t.StreamID > 0 && t.Key != "" && strings.TrimSpace(t.Key) == t.Key && t.RelativePath != "" && r2RetireValidSpan(t.StartAt, t.EndAt) && t.SizeBytes > 0 && r2RetireLowerHex64(t.SHA256) && t.ETag != ""
}

type r2RetireAuthorization struct {
	RunID       string                 `json:"run_id"`
	ManifestSHA string                 `json:"manifest_sha256"`
	Bucket      string                 `json:"bucket"`
	EndpointSHA string                 `json:"endpoint_sha256"`
	Proof       stitchproof.Provenance `json:"proof"`
}

type r2RetireLedgerLine struct {
	Version                int                             `json:"version"`
	RecordID               string                          `json:"record_id"`
	At                     time.Time                       `json:"at"`
	Event                  r2RetireLedgerEvent             `json:"event"`
	ManifestSHA256         string                          `json:"manifest_sha256"`
	Authorization          *r2RetireAuthorization          `json:"authorization,omitempty"`
	AuthorizationID        string                          `json:"authorization_id,omitempty"`
	AuthorizationSHA       string                          `json:"authorization_sha256,omitempty"`
	Target                 *r2RetireTarget                 `json:"target,omitempty"`
	OriginalIntentID       string                          `json:"original_intent_id,omitempty"`
	OriginalIntentSHA      string                          `json:"original_intent_sha256,omitempty"`
	OperatorCredentialRisk *r2RetireOperatorAcknowledgment `json:"operator_credential_risk,omitempty"`
	ETag                   string                          `json:"etag,omitempty"`
	LastModified           time.Time                       `json:"last_modified,omitzero"`
	Kind                   string                          `json:"kind,omitempty"`
	SourceID               int64                           `json:"source_id,omitempty"`
	StreamID               int64                           `json:"stream_id,omitempty"`
	ObjectKey              string                          `json:"object_key,omitempty"`
	SizeBytes              int64                           `json:"size_bytes,omitempty"`
	SHA256                 string                          `json:"sha256,omitempty"`
	Detail                 string                          `json:"detail,omitempty"`
}

type r2RetireLedgerRecord struct {
	line r2RetireLedgerLine
	sha  string
}
type r2RetireLedger struct {
	operatorCredentialRisk *r2RetireOperatorAcknowledgment
	f                      *os.File
	path                   string
	intents                map[r2RetireTarget]r2RetireLedgerRecord
	records                map[string]r2RetireLedgerRecord
	current                r2RetireLedgerRecord
	authority              *stitchproof.Authority
}

func ledgerSHA(raw []byte) string { sum := sha256.Sum256(raw); return hex.EncodeToString(sum[:]) }

func openR2RetireLedger(path string) (*r2RetireLedger, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return nil, err
	}
	fail := func(err error) (*r2RetireLedger, error) { f.Close(); return nil, err }
	st, err := f.Stat()
	if err != nil || !st.Mode().IsRegular() {
		return fail(errors.New("retirement ledger must be a regular file"))
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return fail(fmt.Errorf("retirement ledger is in use: %w", err))
	}
	l := &r2RetireLedger{f: f, path: path, intents: map[r2RetireTarget]r2RetireLedgerRecord{}, records: map[string]r2RetireLedgerRecord{}}
	r := bufio.NewReaderSize(f, 1<<20)
	for {
		raw, err := r.ReadSlice('\n')
		if len(raw) > 1<<20 {
			return fail(errors.New("oversized retirement ledger record"))
		}
		if err == io.EOF && len(raw) == 0 {
			break
		}
		if err != nil {
			return fail(errors.New("torn retirement ledger; preserve it for reviewed recovery"))
		}
		raw = raw[:len(raw)-1]
		var e r2RetireLedgerLine
		if err := stitchproof.DecodeExact(raw, &e); err != nil {
			return fail(fmt.Errorf("invalid retirement ledger: %w", err))
		}
		if err := l.accept(e, ledgerSHA(raw), true); err != nil {
			return fail(err)
		}
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return fail(err)
	}
	err = dir.Sync()
	dir.Close()
	if err != nil {
		return fail(err)
	}
	return l, nil
}

func (l *r2RetireLedger) accept(e r2RetireLedgerLine, sha string, commit bool) error {
	if e.Version != 3 || e.RecordID == "" || e.At.IsZero() || !r2RetireLowerHex64(e.ManifestSHA256) {
		return errors.New("unsupported retirement ledger history; no legacy key-only authority")
	}
	if u, err := uuid.Parse(e.RecordID); err != nil || u.String() != e.RecordID {
		return errors.New("invalid ledger record identity")
	}
	if _, exists := l.records[e.RecordID]; exists {
		return errors.New("duplicate ledger record identity")
	}
	switch e.Event {
	case r2LedgerAuthorized:
		a := e.Authorization
		if a == nil || a.RunID != e.RecordID || a.ManifestSHA != e.ManifestSHA256 || a.Bucket != "stoarama" || !r2RetireLowerHex64(a.EndpointSHA) || a.Proof.ClientSHA != stitchproof.ClientSHA || a.Proof.VerifierSHA != stitchproof.VerifierSHA || a.Proof.NASRunID == "" || a.Proof.CodeHead == "" || !a.Proof.ExpiresAt.After(e.At) {
			return errors.New("incomplete ledger proof authorization")
		}
		for _, d := range []string{a.Proof.OuterSHA, a.Proof.PayloadSHA, a.Proof.BundleSHA, a.Proof.ReviewSHA} {
			if !r2RetireLowerHex64(d) {
				return errors.New("invalid proof authorization digest")
			}
		}
	case r2LedgerIntent, r2LedgerResume, "reconcile_absent", "done":
		prior, ok := l.records[e.AuthorizationID]
		if !ok || prior.sha != e.AuthorizationSHA || prior.line.Event != r2LedgerAuthorized || e.Target == nil || !e.Target.valid() {
			return errors.New("ledger operation lacks a complete fsynced authorization and target")
		}
		a := prior.line.Authorization
		if e.Target.ManifestSHA != a.ManifestSHA || e.Target.Bucket != a.Bucket || e.Target.EndpointSHA != a.EndpointSHA || e.ManifestSHA256 != a.ManifestSHA || !e.At.Before(a.Proof.ExpiresAt) {
			return errors.New("ledger target/proof scope or expiry differs")
		}
		if e.Event == r2LedgerIntent {
			if _, exists := l.intents[*e.Target]; exists {
				return errors.New("unresolved intent exists; never automatically retransmit")
			}
		}
		if e.Event == r2LedgerResume {
			old, ok := l.records[e.OriginalIntentID]
			if !ok || old.line.Event != r2LedgerIntent || old.sha != e.OriginalIntentSHA || old.line.Target == nil || *old.line.Target != *e.Target || old.line.AuthorizationID == e.AuthorizationID {
				return errors.New("resume does not bind the exact original intent to a new authorization")
			}
		}
	case "operator_credential_risk_accepted", "run_start", "run_end", "skip", "head_guard_passed", "head_guard_refused", "delete_error", "delete_unconfirmed", "commit_failed":
		// Diagnostic records grant no reconciliation/deletion authority.
	default:
		return errors.New("unknown retirement ledger event")
	}
	if commit {
		record := r2RetireLedgerRecord{e, sha}
		l.records[e.RecordID] = record
		if e.Event == r2LedgerIntent {
			l.intents[*e.Target] = record
		}
	}
	return nil
}

func (l *r2RetireLedger) write(e r2RetireLedgerLine) error {
	e.Version = 3
	if e.RecordID == "" {
		e.RecordID = uuid.NewString()
	}
	var err error
	e.At, err = l.authority.CurrentUTC()
	if err != nil {
		return err
	}
	e.OperatorCredentialRisk = l.operatorCredentialRisk
	raw, err := json.Marshal(e)
	if err != nil {
		return err
	}
	sha := ledgerSHA(raw)
	if err := l.accept(e, sha, false); err != nil {
		return err
	}
	if _, err := l.f.Write(append(raw, '\n')); err != nil {
		return fmt.Errorf("write retirement ledger: %w", err)
	}
	if err := l.f.Sync(); err != nil {
		return fmt.Errorf("fsync retirement ledger: %w", err)
	}
	return l.accept(e, sha, true)
}

func (l *r2RetireLedger) authorize(manifest, bucket, endpoint string, proof *stitchproof.Authority) error {
	if err := proof.RequireWire(bucket, endpoint); err != nil {
		return err
	}
	if proof.ManifestSHA() != manifest {
		return errors.New("ledger manifest differs from verified retirement binding")
	}
	l.authority = proof
	id := uuid.NewString()
	e := r2RetireLedgerLine{RecordID: id, Event: r2LedgerAuthorized, ManifestSHA256: manifest, Authorization: &r2RetireAuthorization{id, manifest, bucket, endpoint, proof.Provenance()}}
	if err := l.write(e); err != nil {
		return err
	}
	l.current = l.records[id]
	return nil
}

func (l *r2RetireLedger) operation(target r2RetireTarget, event r2RetireLedgerEvent) r2RetireLedgerLine {
	return r2RetireLedgerLine{Event: event, ManifestSHA256: target.ManifestSHA, AuthorizationID: l.current.line.RecordID, AuthorizationSHA: l.current.sha, Target: &target, Kind: string(target.Kind), SourceID: target.SourceID, StreamID: target.StreamID, ObjectKey: target.Key, SizeBytes: target.SizeBytes, SHA256: target.SHA256}
}

func (l *r2RetireLedger) resume(target r2RetireTarget) error {
	old, ok := l.intents[target]
	if !ok {
		return errors.New("absent object has no exact manifest/target delete intent; flags cannot waive this")
	}
	e := l.operation(target, r2LedgerResume)
	e.OriginalIntentID, e.OriginalIntentSHA = old.line.RecordID, old.sha
	return l.write(e)
}

func (l *r2RetireLedger) Close() error { return l.f.Close() }
