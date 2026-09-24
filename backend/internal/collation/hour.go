package collation

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// ErrDeferred marks an hour whose sources are not all available (e.g. purged
// from R2 and not yet restored). Deferred hours publish nothing.
var ErrDeferred = errors.New("hour deferred: sources unavailable")

// Store is the object storage the hour pipeline reads sources from and
// publishes verified outputs to.
type Store interface {
	// Download writes the exact source object to dst and returns its size and sha256.
	Download(ctx context.Context, clip Clip, dst string) (int64, string, error)
	// PublishVerified uploads a file create-only and re-reads it to prove the
	// stored bytes match (size, sha256).
	PublishVerified(ctx context.Context, key, contentType, path string, size int64, sha string) error
	PutManifestIfAbsent(ctx context.Context, key string, body []byte) error
}

type Env struct {
	Tools       Tools
	Policy      SeamPolicy
	ScratchRoot string
	Store       Store
	MediaTool   string
	// CPU bounds concurrent ffmpeg/ffprobe processes across all hours.
	CPU chan struct{}
	// Net bounds concurrent downloads across all hours.
	Net chan struct{}
	Now func() time.Time
}

func acquire(ctx context.Context, sem chan struct{}) error {
	select {
	case sem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ProcessHour plans, builds, verifies and publishes one hour. It returns the
// published manifest.
func ProcessHour(ctx context.Context, env Env, w HourWork) (HourManifest, error) {
	for _, c := range w.Clips {
		if c.Purged {
			return HourManifest{}, ErrDeferred
		}
	}
	m := HourManifest{SchemaVersion: 1, PolicyVersion: PolicyVersion, Generation: Generation, BatchID: w.BatchID, HourID: w.HourID,
		SupersedesHourID: w.SupersedesHourID, RecordingID: w.RecordingID, Timezone: w.Timezone, LocalDate: w.LocalDate,
		DeliveryHour: w.DeliveryHour, ScheduledStart: w.ScheduledStart, ScheduledEnd: w.ScheduledEnd, SeamPolicy: env.Policy,
		MediaTool: env.MediaTool, Clips: []ClipDisposition{}, Seams: []SeamDecision{}, Outputs: []Output{}}
	if want, err := HourIDFor(w.BatchID, w.RecordingID, w.LocalDate, w.DeliveryHour); err != nil || want != w.HourID {
		return m, fmt.Errorf("work hour identity differs")
	}
	root, err := filepath.Abs(env.ScratchRoot)
	if err != nil {
		return m, err
	}
	dir := filepath.Join(root, w.HourID)
	if err := os.RemoveAll(dir); err != nil {
		return m, err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return m, err
	}
	defer os.RemoveAll(dir)

	clips := append([]Clip(nil), w.Clips...)
	sort.SliceStable(clips, func(i, j int) bool {
		if clips[i].StartUTC.Equal(clips[j].StartUTC) {
			return clips[i].ClipID < clips[j].ClipID
		}
		return clips[i].StartUTC.Before(clips[j].StartUTC)
	})
	disp := make([]ClipDisposition, len(clips))
	seenSHA, seenKey := map[string]int64{}, map[string]int64{}
	for i, c := range clips {
		disp[i] = ClipDisposition{Clip: c, Disposition: "included"}
		if first, ok := seenKey[c.ObjectKey]; ok {
			disp[i].Disposition, disp[i].Reason = "duplicate", fmt.Sprintf("same_object_as_clip_%d", first)
			continue
		}
		if first, ok := seenSHA[c.SHA256]; ok && c.SHA256 != "" {
			disp[i].Disposition, disp[i].Reason = "duplicate", fmt.Sprintf("same_bytes_as_clip_%d", first)
			continue
		}
		seenKey[c.ObjectKey], seenSHA[c.SHA256] = c.ClipID, c.ClipID
		if c.KnownBroken {
			disp[i].Disposition, disp[i].Reason = "quarantined", "known_unplayable_no_moov"
		}
	}

	// Download and probe every candidate.
	locals := make([]LocalClip, len(clips))
	var wg sync.WaitGroup
	errs := make([]error, len(clips))
	for i := range clips {
		if disp[i].Disposition != "included" {
			continue
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = func() error {
				c := clips[i]
				dst := filepath.Join(dir, fmt.Sprintf("%d.mp4", c.ClipID))
				if err := acquire(ctx, env.Net); err != nil {
					return err
				}
				size, sha, err := env.Store.Download(ctx, c, dst)
				<-env.Net
				if err != nil {
					return fmt.Errorf("download clip %d: %w", c.ClipID, err)
				}
				if size != c.SizeBytes || (c.SHA256 != "" && sha != c.SHA256) {
					disp[i].Disposition, disp[i].Reason = "quarantined", "source_identity_mismatch"
					return nil
				}
				if err := acquire(ctx, env.CPU); err != nil {
					return err
				}
				probe, err := ProbeFile(ctx, env.Tools, dst)
				<-env.CPU
				if err != nil {
					return err
				}
				disp[i].Media = probe.Media
				if !probe.Media.Playable {
					disp[i].Disposition, disp[i].Reason = "quarantined", "unplayable"
					return nil
				}
				locals[i] = LocalClip{Clip: c, Path: dst, Probe: probe}
				return nil
			}()
		}(i)
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		return m, err
	}

	// Seams between time-adjacent included clips.
	idx := []int{}
	for i := range disp {
		if disp[i].Disposition == "included" {
			idx = append(idx, i)
		}
	}
	seams := make([]SeamDecision, max(len(idx)-1, 0))
	for s := range seams {
		wg.Add(1)
		go func(s int) {
			defer wg.Done()
			a, b := locals[idx[s]], locals[idx[s+1]]
			var match *MatchEvidence
			if MetadataGate(env.Policy, a.Clip, b.Clip, a.Probe.Media, b.Probe.Media) == "" {
				ev := matchSeam(ctx, env, a, b)
				match = &ev
			}
			seams[s] = DecideSeam(env.Policy, a.Clip, b.Clip, a.Probe.Media, b.Probe.Media, match)
		}(s)
	}
	wg.Wait()
	if err := ctx.Err(); err != nil {
		return m, err
	}
	m.Seams = seams

	// Group into parts.
	var parts [][]int
	for k, i := range idx {
		if k == 0 || seams[k-1].Decision != DecisionJoin {
			parts = append(parts, nil)
		}
		parts[len(parts)-1] = append(parts[len(parts)-1], i)
		disp[i].Part = len(parts)
	}
	for i := range disp {
		if disp[i].Disposition != "included" {
			disp[i].Part = 0
		}
	}
	m.Clips = disp

	outputs := make([]Output, len(parts))
	buildErrs := make([]error, len(parts))
	for p := range parts {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			buildErrs[p] = func() error {
				members := make([]LocalClip, len(parts[p]))
				ids := make([]int64, len(parts[p]))
				for k, i := range parts[p] {
					members[k], ids[k] = locals[i], clips[i].ClipID
				}
				if err := acquire(ctx, env.CPU); err != nil {
					return err
				}
				built, err := BuildPart(ctx, env.Tools, members, partPath(dir, p+1))
				<-env.CPU
				if err != nil {
					return fmt.Errorf("part %d: %w", p+1, err)
				}
				start, end := members[0].Clip.StartUTC, members[len(members)-1].Clip.EndUTC
				rel, err := DeliveryPath(w, start, end, p+1, len(parts))
				if err != nil {
					return fmt.Errorf("part %d delivery path: %w", p+1, err)
				}
				key := ObjectKey(w.BatchID, built.SHA256)
				if err := env.Store.PublishVerified(ctx, key, "video/mp4", built.Path, built.SizeBytes, built.SHA256); err != nil {
					return fmt.Errorf("part %d publish: %w", p+1, err)
				}
				_ = os.Remove(built.Path)
				outputs[p] = Output{Part: p + 1, Parts: len(parts), ObjectKey: key, NASRelativePath: rel, SizeBytes: built.SizeBytes,
					SHA256: built.SHA256, StartUTC: start, EndUTC: end, SourceClipIDs: ids, Verification: built.Verification, R2VerifiedAt: env.Now().UTC()}
				return nil
			}()
		}(p)
	}
	wg.Wait()
	if err := errors.Join(buildErrs...); err != nil {
		return m, err
	}
	m.Outputs = outputs
	switch {
	case len(outputs) > 0:
		m.Status = StatusCollated
	case len(clips) == 0:
		m.Status = StatusGapOnly
	default:
		m.Status = StatusQuarantineOnly
	}
	m.CreatedAt = env.Now().UTC()
	if err := m.Validate(); err != nil {
		return m, fmt.Errorf("manifest invalid: %w", err)
	}
	body, err := MarshalManifest(m)
	if err != nil {
		return m, err
	}
	if err := env.Store.PutManifestIfAbsent(ctx, ManifestKey(w.BatchID, w.HourID), body); err != nil {
		return m, err
	}
	return m, nil
}

func matchSeam(ctx context.Context, env Env, a, b LocalClip) MatchEvidence {
	if err := acquire(ctx, env.CPU); err != nil {
		return MatchEvidence{Verdict: MatchDecodeFail, DecodeError: err.Error()}
	}
	tail, err := ExtractWindow(ctx, env.Tools, env.Policy, a.Path, true)
	var head []Frame
	if err == nil {
		head, err = ExtractWindow(ctx, env.Tools, env.Policy, b.Path, false)
	}
	<-env.CPU
	if err != nil {
		return MatchEvidence{Verdict: MatchDecodeFail, DecodeError: trimStderr(err.Error())}
	}
	return EvaluateFrames(env.Policy, tail, head, a.Probe.Video.WindowKeys(len(tail), true), b.Probe.Video.WindowKeys(len(head), false),
		math.Max(a.Probe.Media.FrameSeconds, b.Probe.Media.FrameSeconds))
}
