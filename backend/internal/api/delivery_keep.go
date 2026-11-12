package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"sort"
	"strconv"
	"strings"

	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/jackc/pgx/v5"
)

// DeliveryKeepIdentity is shared by serving, retirement and NAS copy planning.
// A hold protects the key even when its current content no longer agrees.
type DeliveryKeepIdentity struct {
	Key         string `json:"key"`
	ETag        string `json:"etag"`
	Size        int64  `json:"size"`
	SHA256      string `json:"sha256,omitempty"`
	Path        string `json:"path,omitempty"`
	RecordingID int64  `json:"recording_id,omitempty"`
	Kind        string `json:"kind"`
}
type DeliveryKeepSnapshot struct {
	OutputDependencies map[int64]int64
	Objects            map[string]DeliveryKeepIdentity
	SHA256             string
}
type DeliveryKeepStore interface {
	Head(context.Context, string) (r2.ObjectHead, error)
	OpenExact(context.Context, string, string, string) (io.ReadCloser, error)
}

// ReadPublicDeliveryKeep uses exactly the public registry decoder/validator and
// path contract used by the serving API, including supporting assets and both
// current and generation-pinned registry objects. Never infer a keep prefix.
func ReadPublicDeliveryKeep(ctx context.Context, store DeliveryKeepStore) (DeliveryKeepSnapshot, error) {
	out := DeliveryKeepSnapshot{Objects: map[string]DeliveryKeepIdentity{}}
	var current *globalDeliveryRegistry
	for _, key := range []string{globalDeliveryPrefix + "public/current.json", ""} {
		if key == "" {
			key = globalDeliveryPrefix + "public/generations/" + current.Generation + ".json"
		}
		head, err := store.Head(ctx, key)
		if err != nil || head.SizeBytes <= 0 || head.SizeBytes > 64<<20 || head.ETag == "" {
			return out, errors.New("delivery registry identity unavailable")
		}
		body, err := store.OpenExact(ctx, key, head.ETag, head.VersionID)
		if err != nil {
			return out, errors.New("delivery registry changed")
		}
		raw, err := io.ReadAll(io.LimitReader(body, head.SizeBytes+1))
		body.Close()
		if err != nil || int64(len(raw)) != head.SizeBytes {
			return out, errors.New("delivery registry length differs")
		}
		decoded, err := decodeGlobalDeliveryRegistry(raw)
		if err != nil {
			return out, err
		}
		var registry globalDeliveryRegistry
		if json.Unmarshal(decoded, &registry) != nil || validateGlobalDeliveryRegistry(&registry) != nil || !registry.PublicDelivery || registry.SelectionScope != "available" || len(registry.Generation) != 32 {
			return out, errors.New("delivery registry invalid")
		}
		if current != nil {
			a, _ := json.Marshal(current)
			b, _ := json.Marshal(registry)
			if string(a) != string(b) {
				return out, errors.New("current and pinned delivery registries disagree")
			}
		} else {
			current = &registry
		}
		sum := sha256.Sum256(raw)
		out.Objects[key] = DeliveryKeepIdentity{Key: key, ETag: strings.Trim(head.ETag, `"`), Size: head.SizeBytes, SHA256: hex.EncodeToString(sum[:]), Kind: "registry"}
		for name, o := range registry.Assets {
			if !publicGlobalDeliveryAsset(name, registry.RecordingIDs) {
				return out, errors.New("private asset in public delivery")
			}
			if err := out.add(o, "asset"); err != nil {
				return out, err
			}
		}
		for id, o := range registry.Files {
			kind := "video_assembly"
			if _, err := strconv.ParseInt(id, 10, 64); err == nil {
				kind = "video_collated"
			}
			if err := out.add(o, kind); err != nil {
				return out, err
			}
		}
	}
	if err := out.readAssemblyDependencies(ctx, store); err != nil {
		return out, err
	}
	keys := make([]string, 0, len(out.Objects))
	for key := range out.Objects {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	hash := sha256.New()
	enc := json.NewEncoder(hash)
	for _, key := range keys {
		if err := enc.Encode(out.Objects[key]); err != nil {
			return out, err
		}
	}
	ids := make([]int64, 0, len(out.OutputDependencies))
	for id := range out.OutputDependencies {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		if err := enc.Encode([2]int64{id, out.OutputDependencies[id]}); err != nil {
			return out, err
		}
	}
	out.SHA256 = hex.EncodeToString(hash.Sum(nil))
	return out, nil
}
func (s *DeliveryKeepSnapshot) add(o globalDeliveryObject, kind string) error {
	x := DeliveryKeepIdentity{Key: o.Key, ETag: strings.Trim(o.ETag, `"`), Size: o.SizeBytes, SHA256: o.SHA256, Path: o.Path, RecordingID: o.RecordingID, Kind: kind}
	if old, ok := s.Objects[x.Key]; ok && (old.ETag != x.ETag || old.Size != x.Size || old.SHA256 != x.SHA256) {
		return errors.New("delivery aliases disagree")
	}
	s.Objects[x.Key] = x
	return nil
}

type deliveryKeepQuerier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

// RequireDeliveryKeep checks all exact identities in one statement. Missing
// schema/rows, or any identity drift, revoke every retirement class. It is
// also the publication gate: links cannot be served before their holds exist.
func RequireDeliveryKeep(ctx context.Context, q deliveryKeepQuerier, s DeliveryKeepSnapshot) error {
	if len(s.Objects) == 0 {
		return errors.New("delivery keep snapshot missing")
	}
	rows := make([]DeliveryKeepIdentity, 0, len(s.Objects))
	for _, o := range s.Objects {
		rows = append(rows, o)
	}
	raw, err := json.Marshal(rows)
	if err != nil {
		return err
	}
	var matches bool
	err = q.QueryRow(ctx, `SELECT NOT EXISTS(SELECT 1 FROM jsonb_to_recordset($1::jsonb) x(key text,etag text,size bigint)
 LEFT JOIN delivery_keep_holds h ON h.bucket=$2 AND h.object_key=x.key
 WHERE h.object_key IS NULL OR h.etag<>x.etag OR h.size_bytes<>x.size)`, string(raw), "stoarama").Scan(&matches)
	if err != nil {
		return errors.New("delivery keep holds unavailable (migration 0188 required)")
	}
	if !matches {
		return errors.New("live delivery and durable keep holds disagree; retirement/publication refused")
	}
	if len(s.OutputDependencies) > 0 {
		type dep struct {
			ID        int64 `json:"id"`
			Recording int64 `json:"recording"`
		}
		deps := make([]dep, 0, len(s.OutputDependencies))
		for id, recording := range s.OutputDependencies {
			deps = append(deps, dep{id, recording})
		}
		raw, err := json.Marshal(deps)
		if err != nil {
			return err
		}
		if err := q.QueryRow(ctx, `SELECT NOT EXISTS(SELECT 1 FROM jsonb_to_recordset($1::jsonb) x(id bigint,recording bigint)
 LEFT JOIN delivery_keep_output_dependencies d ON d.output_id=x.id AND d.recording_id=x.recording
 LEFT JOIN recording_collation_outputs o ON o.id=d.output_id LEFT JOIN recording_collation_hours h ON h.id=o.collation_hour_id
 WHERE d.output_id IS NULL OR h.recording_id IS DISTINCT FROM x.recording)`, string(raw)).Scan(&matches); err != nil || !matches {
			return errors.New("delivery assembly dependency holds disagree/unavailable")
		}
	}
	return nil
}

func deliveryNASHeld(ctx context.Context, q deliveryKeepQuerier, i nasCleanupItem) (bool, error) {
	var held bool
	if err := q.QueryRow(ctx, `SELECT delivery_nas_held($1,$2,$3,$4,$5)`, i.Kind, i.SourceID, i.RelativePath, i.SHA256, i.SizeBytes).Scan(&held); err != nil {
		return false, errors.New("delivery NAS hold check unavailable; KEEP")
	}
	return held, nil
}
func deliveryNASUnheld(ctx context.Context, q deliveryKeepQuerier, i nasCleanupItem) error {
	held, err := deliveryNASHeld(ctx, q, i)
	if err != nil {
		return err
	}
	if held {
		return errors.New("DELIVERY KEEP HOLD: NAS file/dependency must remain")
	}
	return nil
}

// Assembly source IDs come from the exact public growth representation, never
// inferred from video names, recording windows, or a separate stale catalogue.
func (s *DeliveryKeepSnapshot) readAssemblyDependencies(ctx context.Context, store DeliveryKeepStore) error {
	assemblies := map[string]DeliveryKeepIdentity{}
	var growth DeliveryKeepIdentity
	for _, o := range s.Objects {
		if o.Kind == "video_assembly" {
			assemblies[o.SHA256] = o
		}
		if o.Kind == "asset" && strings.HasSuffix(o.Key, "/growth.json") {
			growth = o
		}
	}
	s.OutputDependencies = map[int64]int64{}
	if len(assemblies) == 0 {
		return nil
	}
	if growth.Key == "" {
		return errors.New("assembly dependency metadata missing")
	}
	body, err := store.OpenExact(ctx, growth.Key, growth.ETag, "")
	if err != nil {
		return errors.New("assembly dependency metadata unavailable")
	}
	raw, err := io.ReadAll(io.LimitReader(body, growth.Size+1))
	body.Close()
	sum := sha256.Sum256(raw)
	if err != nil || int64(len(raw)) != growth.Size || hex.EncodeToString(sum[:]) != growth.SHA256 {
		return errors.New("assembly dependency metadata identity differs")
	}
	var doc struct {
		Repairs []struct {
			SHA       string  `json:"sha256"`
			Recording int64   `json:"recording_id"`
			Size      int64   `json:"size_bytes"`
			Inputs    []int64 `json:"input_ids"`
			Parts     int     `json:"input_parts"`
		} `json:"repairs"`
	}
	if json.Unmarshal(raw, &doc) != nil {
		return errors.New("assembly dependencies unreadable")
	}
	seen := map[string]bool{}
	for _, r := range doc.Repairs {
		o, ok := assemblies[r.SHA]
		if !ok {
			continue
		}
		if seen[r.SHA] || o.Size != r.Size || o.RecordingID != r.Recording || len(r.Inputs) == 0 || len(r.Inputs) != r.Parts {
			return errors.New("assembly dependency identity differs")
		}
		seen[r.SHA] = true
		for _, id := range r.Inputs {
			if id <= 0 {
				return errors.New("assembly input identity invalid")
			}
			if record, exists := s.OutputDependencies[id]; exists && record != r.Recording {
				return errors.New("assembly input crosses recordings")
			}
			s.OutputDependencies[id] = r.Recording
		}
	}
	if len(seen) != len(assemblies) {
		return errors.New("assembly dependency closure incomplete")
	}
	return nil
}
