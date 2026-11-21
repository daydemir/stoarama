package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"os"
	"sync"
	"syscall"
	"time"
)

type inverseEvent string

const (
	inverseIntent inverseEvent = "intent"
	inverseAbsent inverseEvent = "absent"
)

type inverseLedgerLane string

const (
	inverseLegacyLedger inverseLedgerLane = ""
	inverseKeep28Ledger inverseLedgerLane = "keep28_raw_v1"
	inverseNonrawLedger inverseLedgerLane = "nonraw_collated_v1"
)

type inverseLedgerRecord struct {
	Lane           inverseLedgerLane `json:"lane,omitempty"`
	EndpointSHA256 string            `json:"endpoint_sha256"`
	Event          inverseEvent      `json:"event"`
	Bucket         string            `json:"bucket"`
	Key            string            `json:"key"`
	ManifestSHA256 string            `json:"manifest_sha256"`
	At             time.Time         `json:"at"`
	SizeBytes      int64             `json:"size_bytes"`
}
type inverseLedger struct {
	lane inverseLedgerLane
	file *os.File
	mu   sync.Mutex
	seen map[string]inverseLedgerRecord
}

func openInverseLedger(path string) (*inverseLedger, error) {
	return openInverseLaneLedger(path, inverseLegacyLedger)
}
func openKeep28Ledger(path string) (*inverseLedger, error) {
	return openInverseLaneLedger(path, inverseKeep28Ledger)
}
func openNonrawLedger(path string) (*inverseLedger, error) {
	return openInverseLaneLedger(path, inverseNonrawLedger)
}
func openInverseLaneLedger(path string, lane inverseLedgerLane) (*inverseLedger, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return nil, err
	}
	fail := func(err error) (*inverseLedger, error) { f.Close(); return nil, err }
	if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return fail(err)
	}
	info, err := f.Stat()
	if err != nil {
		return fail(err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		return fail(errors.New("ledger must be a private regular file"))
	}
	l := &inverseLedger{lane: lane, file: f, seen: map[string]inverseLedgerRecord{}}
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 4096), 1024*1024)
	var consumed int64
	for scanner.Scan() {
		line := scanner.Bytes()
		consumed += int64(len(line) + 1)
		if lane == inverseLegacyLedger {
			var fields map[string]json.RawMessage
			if json.Unmarshal(line, &fields) != nil {
				return fail(errors.New("invalid legacy ledger record"))
			}
			if _, exists := fields["lane"]; exists {
				return fail(errors.New("B1 refuses a lane-tagged ledger"))
			}
		}
		var rec inverseLedgerRecord
		if json.Unmarshal(line, &rec) != nil || !validInverseRecord(rec) || rec.Lane != lane {
			return fail(errors.New("invalid ledger record; preserve and reconcile"))
		}
		key := inverseLedgerKey(rec)
		if _, ok := l.seen[key]; ok {
			return fail(errors.New("duplicate ledger event; preserve and reconcile"))
		}
		l.seen[key] = rec
	}
	if err = scanner.Err(); err != nil {
		return fail(err)
	}
	if consumed != info.Size() {
		return fail(errors.New("incomplete ledger tail; preserve and reconcile"))
	}
	return l, nil
}
func inverseLedgerKey(r inverseLedgerRecord) string {
	return r.Bucket + "\x00" + r.EndpointSHA256 + "\x00" + r.Key + "\x00" + string(r.Event)
}
func validInverseRecord(r inverseLedgerRecord) bool {
	return (r.Event == inverseIntent || r.Event == inverseAbsent) && r2RetireLowerHex64(r.EndpointSHA256) && r.Bucket != "" && r.Key != "" && r.SizeBytes > 0 && r2RetireLowerHex64(r.ManifestSHA256) && !r.At.IsZero()
}
func (l *inverseLedger) append(r inverseLedgerRecord) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if r.Lane == "" {
		r.Lane = l.lane
	}
	if !validInverseRecord(r) || r.Lane != l.lane {
		return errors.New("invalid inverse ledger record")
	}
	key := inverseLedgerKey(r)
	if old, ok := l.seen[key]; ok {
		if old.ManifestSHA256 != r.ManifestSHA256 || old.SizeBytes != r.SizeBytes {
			return errors.New("ledger identity changed")
		}
		return nil
	}
	data, err := json.Marshal(r)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	n, err := l.file.Write(data)
	if err != nil {
		return err
	}
	if n != len(data) {
		return errors.New("short ledger write")
	}
	if err = l.file.Sync(); err != nil {
		return err
	}
	l.seen[key] = r
	return nil
}
func (l *inverseLedger) close() error { return l.file.Close() }
