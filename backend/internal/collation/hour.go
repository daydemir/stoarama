package collation

import (
	"context"
	"errors"
	"fmt"
	"log"
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
	// KeepScratch keeps each hour's downloaded sources (canary review only).
	KeepScratch bool
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
	if !env.KeepScratch {
		defer os.RemoveAll(dir)
	}

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

	stageStart := time.Now()
	stage := func(name string) {
		log.Printf("collation stage hour=%s stage=%s secs=%.1f", w.HourID, name, time.Since(stageStart).Seconds())
		stageStart = time.Now()
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
	stage("download_probe")

	// Each included clip gets exactly one seam to its predecessor: the earlier
	// clip of the same job that ends where it starts (its capture chain; two
	// recorders writing one job interleave in time), or else simply the
	// time-previous clip. A clip can continue at most one successor.
	idx := []int{}
	for i := range disp {
		if disp[i].Disposition == "included" {
			idx = append(idx, i)
		}
	}
	type seamPair struct{ prev, next int }
	pairs := make([]seamPair, 0, len(idx))
	claimed := map[int]bool{}
	for k := 1; k < len(idx); k++ {
		next := idx[k]
		prev := idx[k-1]
		for j := k - 1; j >= 0; j-- {
			cand := idx[j]
			if claimed[cand] || locals[cand].Clip.JobID != locals[next].Clip.JobID {
				continue
			}
			frame := math.Max(locals[cand].Probe.Media.FrameSeconds, locals[next].Probe.Media.FrameSeconds)
			if math.Abs(locals[next].Clip.StartUTC.Sub(locals[cand].Clip.EndUTC).Seconds()) <= env.Policy.GapFrameSlack*frame {
				prev = cand
				break
			}
		}
		claimed[prev] = true
		pairs = append(pairs, seamPair{prev, next})
	}
	seams := make([]SeamDecision, len(pairs))
	for s := range seams {
		wg.Add(1)
		go func(s int) {
			defer wg.Done()
			a, b := locals[pairs[s].prev], locals[pairs[s].next]
			if overlap, ok := ReplayOverlap(a.Probe, b.Probe); ok {
				seams[s] = DecideSeam(env.Policy, a.Clip, b.Clip, a.Probe.Media, b.Probe.Media, nil)
				seams[s].Decision, seams[s].Reason, seams[s].OverlapSeconds = DecisionSplit, "packet_replay", overlap
				return
			}
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
	stage("seams")

	// Group into parts: a joined seam extends its predecessor's part, anything
	// else starts a new one.
	partOf := map[int]int{}
	var parts [][]int
	if len(idx) > 0 {
		parts = append(parts, []int{idx[0]})
		partOf[idx[0]] = 0
	}
	for s, pr := range pairs {
		if seams[s].Decision == DecisionJoin {
			p := partOf[pr.prev]
			parts[p] = append(parts[p], pr.next)
			partOf[pr.next] = p
			continue
		}
		parts = append(parts, []int{pr.next})
		partOf[pr.next] = len(parts) - 1
	}
	parts = dropDuplicateCaptures(parts, clips, disp)

	// Build and verify every part. A part that fails verification is isolated:
	// clips that do not strictly decode on their own are quarantined and the
	// rest is rebuilt as separate pieces (never joined across a removed clip);
	// a piece that still fails is split into single clips. The hour never fails
	// because of one bad clip.
	type builtPart struct {
		members []int
		built   Built
	}
	var mu sync.Mutex
	var done []builtPart
	buildErrs := make([]error, len(parts))
	var build func(members []int) error
	build = func(members []int) error {
		local := make([]LocalClip, len(members))
		for k, i := range members {
			local[k] = locals[i]
		}
		if err := acquire(ctx, env.CPU); err != nil {
			return err
		}
		out := filepath.Join(dir, fmt.Sprintf("out-%d.mp4", clips[members[0]].ClipID))
		built, err := BuildPart(ctx, env.Tools, local, out)
		<-env.CPU
		if err == nil {
			mu.Lock()
			done = append(done, builtPart{members: members, built: built})
			mu.Unlock()
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		_ = os.Remove(out)
		log.Printf("collation isolate hour=%s first_clip=%d clips=%d err=%v", w.HourID, clips[members[0]].ClipID, len(members), err)
		var pieces [][]int
		var piece []int
		for _, i := range members {
			if err := acquire(ctx, env.CPU); err != nil {
				return err
			}
			decodeErr := strictDecode(ctx, env.Tools, locals[i].Path)
			<-env.CPU
			if decodeErr != nil {
				mu.Lock()
				disp[i].Disposition, disp[i].Reason = "quarantined", "strict_decode_failed"
				mu.Unlock()
				if len(piece) > 0 {
					pieces = append(pieces, piece)
					piece = nil
				}
				continue
			}
			piece = append(piece, i)
		}
		if len(piece) > 0 {
			pieces = append(pieces, piece)
		}
		if len(pieces) == 1 && len(pieces[0]) == len(members) {
			// Every clip decodes alone: the join itself is the problem.
			if len(members) == 1 {
				mu.Lock()
				disp[members[0]].Disposition, disp[members[0]].Reason = "quarantined", "build_verify_failed"
				mu.Unlock()
				return nil
			}
			pieces = nil
			for _, i := range members {
				pieces = append(pieces, []int{i})
			}
		}
		for _, pc := range pieces {
			if err := build(pc); err != nil {
				return err
			}
		}
		return nil
	}
	for p := range parts {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			buildErrs[p] = build(parts[p])
		}(p)
	}
	wg.Wait()
	if err := errors.Join(buildErrs...); err != nil {
		return m, err
	}
	sort.Slice(done, func(i, j int) bool {
		return clips[done[i].members[0]].StartUTC.Before(clips[done[j].members[0]].StartUTC) ||
			(clips[done[i].members[0]].StartUTC.Equal(clips[done[j].members[0]].StartUTC) && clips[done[i].members[0]].ClipID < clips[done[j].members[0]].ClipID)
	})
	// Seams whose clips no longer share a part after isolation become splits.
	finalPart := map[int64]int{}
	for p, bp := range done {
		for _, i := range bp.members {
			finalPart[clips[i].ClipID] = p + 1
		}
	}
	for k := range seams {
		if seams[k].Decision == DecisionJoin && finalPart[seams[k].PrevClipID] != finalPart[seams[k].NextClipID] {
			seams[k].Decision, seams[k].Reason = DecisionSplit, "isolated_after_verify_failure"
		}
	}
	m.Seams = seams
	for i := range disp {
		disp[i].Part = 0
		if disp[i].Disposition == "included" {
			disp[i].Part = finalPart[clips[i].ClipID]
		}
	}
	m.Clips = disp

	outputs := make([]Output, len(done))
	pubErrs := make([]error, len(done))
	for p := range done {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			pubErrs[p] = func() error {
				bp := done[p]
				ids := make([]int64, len(bp.members))
				for k, i := range bp.members {
					ids[k] = clips[i].ClipID
				}
				start, end := clips[bp.members[0]].StartUTC, clips[bp.members[len(bp.members)-1]].EndUTC
				rel, err := DeliveryPath(w, start, end, p+1, len(done))
				if err != nil {
					return fmt.Errorf("part %d delivery path: %w", p+1, err)
				}
				key := ObjectKey(w.BatchID, bp.built.SHA256)
				if err := env.Store.PublishVerified(ctx, key, "video/mp4", bp.built.Path, bp.built.SizeBytes, bp.built.SHA256); err != nil {
					return fmt.Errorf("part %d publish: %w", p+1, err)
				}
				_ = os.Remove(bp.built.Path)
				outputs[p] = Output{Part: p + 1, Parts: len(done), ObjectKey: key, NASRelativePath: rel, SizeBytes: bp.built.SizeBytes,
					SHA256: bp.built.SHA256, StartUTC: start, EndUTC: end, SourceClipIDs: ids, Verification: bp.built.Verification, R2VerifiedAt: env.Now().UTC()}
				return nil
			}()
		}(p)
	}
	wg.Wait()
	if err := errors.Join(pubErrs...); err != nil {
		return m, err
	}
	m.Outputs = outputs
	stage("build_verify_publish")
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

// matchSeam proves continuity on a short window first (cheap); only a seam
// that does not pass there is re-examined on the full window, which is also
// what measures longer overlaps. A join always passes the full rule on the
// window that produced it.
func matchSeam(ctx context.Context, env Env, a, b LocalClip) MatchEvidence {
	frame := math.Max(a.Probe.Media.FrameSeconds, b.Probe.Media.FrameSeconds)
	var ev MatchEvidence
	for _, window := range env.Policy.windows() {
		policy := env.Policy
		policy.WindowSeconds = window
		if err := acquire(ctx, env.CPU); err != nil {
			return MatchEvidence{Verdict: MatchDecodeFail, DecodeError: err.Error()}
		}
		tail, err := ExtractWindow(ctx, env.Tools, policy, a.Path, true)
		var head []Frame
		if err == nil {
			head, err = ExtractWindow(ctx, env.Tools, policy, b.Path, false)
		}
		<-env.CPU
		if err != nil {
			return MatchEvidence{Verdict: MatchDecodeFail, DecodeError: trimStderr(err.Error()), WindowSeconds: window}
		}
		ev = EvaluateFrames(policy, tail, head, a.Probe.Video.WindowKeys(len(tail), true), b.Probe.Video.WindowKeys(len(head), false), frame)
		ev.WindowSeconds = window
		if ev.Verdict == MatchContinuous {
			return ev
		}
	}
	return ev
}

// dropDuplicateCaptures keeps one capture where two chains of one recording
// cover the same time (two recorders on one job): a part that overlaps longer
// kept parts for more than half of its own span is dropped as a duplicate
// capture. Short overlaps (a rewound restart) keep both parts.
func dropDuplicateCaptures(parts [][]int, clips []Clip, disp []ClipDisposition) [][]int {
	span := func(p []int) (time.Time, time.Time) { return clips[p[0]].StartUTC, clips[p[len(p)-1]].EndUTC }
	order := make([]int, len(parts))
	for i := range order {
		order[i] = i
	}
	sort.SliceStable(order, func(i, j int) bool {
		si, ei := span(parts[order[i]])
		sj, ej := span(parts[order[j]])
		return ei.Sub(si) > ej.Sub(sj)
	})
	var kept [][]int
	for _, pi := range order {
		s, e := span(parts[pi])
		var overlap time.Duration
		for _, k := range kept {
			ks, ke := span(k)
			lo, hi := s, e
			if ks.After(lo) {
				lo = ks
			}
			if ke.Before(hi) {
				hi = ke
			}
			if hi.After(lo) {
				overlap += hi.Sub(lo)
			}
		}
		if overlap*2 > e.Sub(s) {
			for _, i := range parts[pi] {
				disp[i].Disposition, disp[i].Reason = "duplicate", "duplicate_capture_chain"
			}
			continue
		}
		kept = append(kept, parts[pi])
	}
	return kept
}
