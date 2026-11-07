package api

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"path"
	"sync"
)

// Produced from the complete immutable audit by
// scripts/nas-cleanup/build_frozen_index.py (schema v2). The pinned root binds
// every shard to the original source audit and to reviewed single-stream
// folder ownership. v2 covers every folder owned by exactly one stream, not
// only the old RETENTION_DELETE_CLEARED folders, so outside-window files in any
// stream folder can be bound to their audited filesystem identity. The root is
// deterministic; publish it with build_frozen_index.py --publish before use.
const cleanupFrozenIndexSHA = "82b067b28b4e1ad0226f1ef32d670a22af11d825bf8bd1bc04d2ab26b90c4e20"
const cleanupFrozenIndexParents = 3288
const cleanupFrozenIndexPrefix = "staging/best14-20261021/nas-retention-20261025/frozen-index-20261102/"
const cleanupFrozenAuditSHA = "480d85a49288d79e201b14405dc72d0f6fc979a0fd06f1b7a95663053583865b"
const cleanupFrozenIndexKey = cleanupFrozenIndexPrefix + cleanupFrozenIndexSHA + "/index.json.gz"

type cleanupFrozenShard struct {
	Key     string `json:"key"`
	SHA     string `json:"sha256"`
	Bytes   int64  `json:"bytes"`
	Entries int    `json:"entries"`
}
type cleanupFrozenParent struct {
	Owner   int64                `json:"owner_stream_id"`
	Shards  []cleanupFrozenShard `json:"shards"`
	Entries int                  `json:"entries"`
}
type cleanupFrozenIndex struct {
	Schema     string `json:"schema"`
	AuditSHA   string `json:"source_audit_sha256"`
	AuditID    string `json:"audit_id"`
	AuditBytes int64  `json:"source_audit_bytes"`
	ReportSHA  string `json:"report_sha256"`
	Totals     struct {
		Entries int64 `json:"entries"`
	} `json:"entry_totals"`
	Parents map[string]cleanupFrozenParent `json:"parents"`
}
type cleanupFrozenIndexedEntry struct {
	Path     string   `json:"path"`
	RawBytes string   `json:"path_bytes_b64"`
	Kind     string   `json:"kind"`
	Error    string   `json:"error"`
	Links    int64    `json:"links"`
	Frozen   [5]int64 `json:"frozen"`
}

func decodeCleanupIndexPayload(raw []byte, sha string, plainLimit int64, out any) error {
	if cleanupSHA(raw) != sha {
		return errors.New("frozen index metadata SHA differs")
	}
	z, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		return err
	}
	defer z.Close()
	plain, err := io.ReadAll(io.LimitReader(z, plainLimit+1))
	if err != nil || int64(len(plain)) > plainLimit {
		return errors.New("invalid bounded frozen index metadata")
	}
	return json.Unmarshal(plain, out)
}
func validateCleanupIndex(index *cleanupFrozenIndex, report nasCleanupReport) error {
	if index.Schema != "nas-cleanup-frozen-index-v2" || index.AuditSHA != cleanupFrozenAuditSHA || index.ReportSHA != nasCleanupReportSHA || index.AuditID != "6cd48108-ca05-4df7-988b-0bd7e4d320fe" || index.AuditBytes != 88667812 || index.Totals.Entries != 2943864 {
		return errors.New("frozen index provenance differs")
	}
	expected := map[string]int64{}
	for _, n := range report.Nodes {
		if len(n.Owners) == 1 {
			expected[n.Path] = n.Owners[0]
		}
	}
	if len(expected) != len(index.Parents) || (len(report.Nodes) == 3304 && len(index.Parents) != cleanupFrozenIndexParents) {
		return errors.New("incomplete frozen index ownership")
	}
	for parent, p := range index.Parents {
		if !validCleanupPath(parent) || expected[parent] != p.Owner || p.Owner <= 0 {
			return errors.New("frozen index owner differs")
		}
		entries := 0
		for _, ref := range p.Shards {
			if len(ref.SHA) != 64 || !lowerHex(ref.SHA) || ref.Key != cleanupFrozenIndexPrefix+ref.SHA+"/files.json.gz" || ref.Bytes < 1 || ref.Bytes > 131072 || ref.Entries < 1 || ref.Entries > 20000 {
				return errors.New("invalid frozen index shard")
			}
			entries += ref.Entries
		}
		if entries != p.Entries || p.Entries < 0 {
			return errors.New("frozen index entry accounting differs")
		}
	}
	return nil
}
func (s *Server) readCleanupIndexObject(ctx context.Context, key string, limit int64) ([]byte, error) {
	if s.r2 == nil {
		return nil, errors.New("frozen index store unavailable")
	}
	r, err := s.r2.Open(ctx, key)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	raw, err := io.ReadAll(io.LimitReader(r, limit+1))
	if err != nil || int64(len(raw)) > limit {
		return nil, errors.New("frozen index object exceeds bound")
	}
	return raw, nil
}
func (s *Server) cleanupFrozenIndex(ctx context.Context) (*cleanupFrozenIndex, error) {
	s.nasCleanupMu.Lock()
	cached := s.nasCleanupInventoryIndex
	s.nasCleanupMu.Unlock()
	if cached != nil {
		return cached, nil
	}
	report, err := s.cleanupReport(ctx)
	if err != nil {
		return nil, err
	}
	raw, err := s.readCleanupIndexObject(ctx, cleanupFrozenIndexKey, 4<<20)
	if err != nil {
		return nil, err
	}
	var index cleanupFrozenIndex
	if err = decodeCleanupIndexPayload(raw, cleanupFrozenIndexSHA, 32<<20, &index); err != nil {
		return nil, err
	}
	if err = validateCleanupIndex(&index, report); err != nil {
		return nil, err
	}
	s.nasCleanupMu.Lock()
	if s.nasCleanupInventoryIndex == nil {
		s.nasCleanupInventoryIndex = &index
	}
	cached = s.nasCleanupInventoryIndex
	s.nasCleanupMu.Unlock()
	return cached, nil
}

type cleanupIndexRead func(context.Context, string, int64) ([]byte, error)

func bindCleanupIndexedItems(ctx context.Context, items []nasCleanupItem, index *cleanupFrozenIndex, read cleanupIndexRead) error {
	wanted := map[string]map[string]bool{}
	for _, i := range items {
		parent := path.Dir(i.RelativePath)
		owner, ok := index.Parents[parent]
		if !ok || owner.Owner != i.StreamID || !validCleanupPath(i.RelativePath) {
			return errors.New("source outside frozen index owner")
		}
		if wanted[parent] == nil {
			wanted[parent] = map[string]bool{}
		}
		names := []string{i.RelativePath}
		if i.SidecarRequired {
			names = append(names, i.RelativePath+".stoarama.json")
		}
		for _, name := range names {
			if wanted[parent][name] {
				return errors.New("overlapping cleanup media/sidecar identities")
			}
			wanted[parent][name] = true
		}
	}
	work, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan string, len(wanted))
	for parent := range wanted {
		jobs <- parent
	}
	close(jobs)
	found := map[string][5]int64{}
	var mu sync.Mutex
	var first error
	var wg sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for parent := range jobs {
				if work.Err() != nil {
					return
				}
				local, err := lookupCleanupIndexedParent(work, parent, index.Parents[parent], wanted[parent], read)
				mu.Lock()
				if err != nil {
					if first == nil {
						first = err
						cancel()
					}
				} else {
					for name, fp := range local {
						found[name] = fp
					}
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if first != nil {
		return first
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// Assign nothing until all referenced shards have been checked.
	for _, i := range items {
		fp, ok := found[i.RelativePath]
		if !ok || fp[0] != i.SizeBytes {
			return errors.New("file absent or size changed since frozen safe set; KEEP")
		}
		if i.SidecarRequired {
			fp, ok = found[i.RelativePath+".stoarama.json"]
			if !ok || fp[0] < 1 || fp[0] > 1<<20 {
				return errors.New("bound sidecar unavailable; KEEP")
			}
		}
	}
	for n := range items {
		items[n].Frozen = found[items[n].RelativePath]
		if items[n].SidecarRequired {
			items[n].FrozenSidecar = found[items[n].RelativePath+".stoarama.json"]
		}
	}
	return nil
}
func lookupCleanupIndexedParent(ctx context.Context, parent string, p cleanupFrozenParent, wanted map[string]bool, read cleanupIndexRead) (map[string][5]int64, error) {
	found := map[string][5]int64{}
	seen := map[string]bool{}
	for _, ref := range p.Shards {
		raw, err := read(ctx, ref.Key, ref.Bytes)
		if err != nil {
			return nil, err
		}
		if int64(len(raw)) != ref.Bytes {
			return nil, errors.New("frozen shard length differs")
		}
		var shard struct {
			Parent  string                      `json:"parent"`
			Entries []cleanupFrozenIndexedEntry `json:"entries"`
		}
		if err = decodeCleanupIndexPayload(raw, ref.SHA, 2<<20, &shard); err != nil {
			return nil, err
		}
		if shard.Parent != parent || len(shard.Entries) != ref.Entries {
			return nil, errors.New("frozen shard parent/accounting differs")
		}
		for _, e := range shard.Entries {
			if !wanted[e.Path] {
				continue
			}
			if seen[e.Path] || path.Dir(e.Path) != parent || e.RawBytes != "" || e.Kind != "file" || e.Error != "" || e.Links != 1 || e.Frozen[3] <= 0 || e.Frozen[4] <= 0 {
				return nil, errors.New("ambiguous frozen observation; KEEP")
			}
			seen[e.Path] = true
			found[e.Path] = e.Frozen
		}
	}
	return found, nil
}

func (s *Server) bindCleanupFrozenInventory(ctx context.Context, items []nasCleanupItem) error {
	index, err := s.cleanupFrozenIndex(ctx)
	if err != nil {
		return err
	}
	return bindCleanupIndexedItems(ctx, items, index, s.readCleanupIndexObject)
}
