// Proposed additive research instrumentation for internal/collation/probe.go.
// Existing ReplayOverlap and DecideLocalSeam remain unchanged.
// Research evidence only: does not change any production decision.
package collation

import (
	"math/big"
	"sort"
)

type ReplayOrderAudit struct {
	Status                           string              `json:"status"`
	PhysicalReplayQualification      string              `json:"physical_replay_qualification"`
	PrevTimeline                     PacketTimelineAudit `json:"prev_packet_timeline"`
	NextTimeline                     PacketTimelineAudit `json:"next_packet_timeline"`
	ChainNovelty                     string              `json:"chain_novelty"`
	NextPacketsWithPayloadAbsentPrev int                 `json:"next_packets_payload_absent_prev"`
	NextDistinctPayloadsAbsentPrev   int                 `json:"next_distinct_payloads_absent_prev"`
	FirstChainAnchorMultiplicity     int                 `json:"first_chain_anchor_multiplicity"`
	OriginalReplayHold               bool                `json:"original_replay_hold"`
	OriginalOverlapSeconds           float64             `json:"original_overlap_seconds"`
	PayloadMatches                   int                 `json:"payload_matches"`
	OrderedDistinctWindows           int                 `json:"ordered_distinct_windows"`
	FirstAIndex                      int                 `json:"first_a_index"`
	FirstBIndex                      int                 `json:"first_b_index"`
	// A chain is byte correspondence, not physical-source rewind proof.
	PhysicalReplayProved    bool `json:"physical_replay_proved"`
	AutomaticJoinAuthorized bool `json:"automatic_join_authorized"`
}

// AuditReplayOrder distinguishes the original heuristic from an ordered
// 16-packet, >=8-distinct-payload, coherently shifted correspondence.
// All old holds stay held. This channel only prioritizes further source proof.
func AuditReplayOrder(a, b Probe) ReplayOrderAudit {
	sec, old := ReplayOverlap(a, b)
	ev := ReplayOrderAudit{Status: "unknown_packets", OriginalReplayHold: old,
		OriginalOverlapSeconds: sec, FirstAIndex: -1, FirstBIndex: -1, PhysicalReplayQualification: "UNKNOWN", ChainNovelty: "UNKNOWN"}
	if a.Video == nil || b.Video == nil || len(a.Video.Packets) == 0 || len(b.Video.Packets) == 0 {
		return ev
	}
	ev.PrevTimeline = auditPacketTimeline(a.Video)
	ev.NextTimeline = auditPacketTimeline(b.Video)
	index := make(map[string][]int)
	for i, p := range a.Video.Packets {
		if p.Hash != "" {
			index[p.Hash] = append(index[p.Hash], i)
		}
	}
	novel := map[string]bool{}
	for _, p := range b.Video.Packets {
		if p.Hash != "" && len(index[p.Hash]) == 0 {
			ev.NextPacketsWithPayloadAbsentPrev++
			novel[p.Hash] = true
		}
	}
	ev.NextDistinctPayloadsAbsentPrev = len(novel)
	const n = 16
	tol := big.NewRat(1, 1000)
	for j, y := range b.Video.Packets {
		if j >= replayScanPackets {
			break
		}
		candidates := index[y.Hash]
		ev.PayloadMatches += len(candidates)
		for _, i := range candidates {
			if i+n > len(a.Video.Packets) || j+n > len(b.Video.Packets) {
				continue
			}
			x0, y0 := a.Video.Packets[i], b.Video.Packets[j]
			if x0.PTS == nil || y0.PTS == nil {
				continue
			}
			shift := new(big.Rat).Sub(x0.PTS, y0.PTS)
			distinct := map[string]bool{}
			good := true
			for k := 0; k < n; k++ {
				x, y := a.Video.Packets[i+k], b.Video.Packets[j+k]
				if x.Hash == "" || x.Hash != y.Hash || x.Key != y.Key || x.PTS == nil || y.PTS == nil || x.Dur == nil || y.Dur == nil || x.Dur.Sign() <= 0 || y.Dur.Sign() <= 0 {
					good = false
					break
				}
				dt := new(big.Rat).Sub(new(big.Rat).Sub(x.PTS, y.PTS), shift)
				dd := new(big.Rat).Sub(x.Dur, y.Dur)
				if new(big.Rat).Abs(dt).Cmp(tol) > 0 || new(big.Rat).Abs(dd).Cmp(tol) > 0 {
					good = false
					break
				}
				distinct[x.Hash] = true
			}
			if !good || len(distinct) < 8 {
				continue
			}
			if ev.OrderedDistinctWindows == 0 {
				ev.FirstAIndex = i
				ev.FirstBIndex = j
				ev.FirstChainAnchorMultiplicity = len(index[y.Hash])
			}
			ev.OrderedDistinctWindows++
		}
	}
	ev.Status = "no_matching_payload"
	if ev.PayloadMatches > 0 {
		ev.Status = "ambiguous_payload_collision"
	}
	if ev.OrderedDistinctWindows > 0 {
		ev.Status = "ordered_distinct_payload_chain"
		ev.ChainNovelty = "distinct16_packet_correspondence_only_unique_source_novelty_UNKNOWN"
	}
	return ev
}

// Packet timeline evidence is never a substitute for actual decoded frame PTS.
type PacketTimelineAudit struct {
	Status                    string `json:"status"`
	PacketCount               int    `json:"packet_count"`
	InvalidTiming             int    `json:"invalid_timing"`
	NonIncreasingDTS          int    `json:"non_increasing_dts"`
	DuplicatePresentationPTS  int    `json:"duplicate_presentation_pts"`
	PresentationReordered     bool   `json:"presentation_reordered"`
	UncoveredIntervals        int    `json:"uncovered_intervals"`
	MaxUncoveredSeconds       string `json:"max_uncovered_seconds_rational"`
	OriginSeconds             string `json:"origin_seconds_rational"`
	PresentedExtentSeconds    string `json:"presented_extent_seconds_rational"`
	DecodedFrameQualification string `json:"decoded_frame_qualification"`
}

func auditPacketTimeline(v *StreamPackets) PacketTimelineAudit {
	ev := PacketTimelineAudit{Status: "unknown_packets", DecodedFrameQualification: "UNKNOWN", MaxUncoveredSeconds: "0"}
	if v == nil || len(v.Packets) == 0 {
		return ev
	}
	ev.PacketCount = len(v.Packets)
	sorted := make([]Packet, 0, len(v.Packets))
	for i, p := range v.Packets {
		if p.PTS == nil || p.DTS == nil || p.Dur == nil || p.Dur.Sign() <= 0 {
			ev.InvalidTiming++
			continue
		}
		if p.PTS.Cmp(p.DTS) != 0 {
			ev.PresentationReordered = true
		}
		if i > 0 && v.Packets[i-1].DTS != nil && p.DTS.Cmp(v.Packets[i-1].DTS) <= 0 {
			ev.NonIncreasingDTS++
		}
		sorted = append(sorted, p)
	}
	if ev.InvalidTiming > 0 || len(sorted) == 0 {
		ev.Status = "invalid_packet_timing_hold"
		return ev
	}
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].PTS.Cmp(sorted[j].PTS) < 0 })
	origin := new(big.Rat).Set(sorted[0].PTS)
	end := new(big.Rat).Add(sorted[0].PTS, sorted[0].Dur)
	maxGap := new(big.Rat)
	for i, p := range sorted[1:] {
		if p.PTS.Cmp(sorted[i].PTS) == 0 {
			ev.DuplicatePresentationPTS++
		}
		if p.PTS.Cmp(end) > 0 {
			gap := new(big.Rat).Sub(p.PTS, end)
			ev.UncoveredIntervals++
			if gap.Cmp(maxGap) > 0 {
				maxGap.Set(gap)
			}
		}
		e := new(big.Rat).Add(p.PTS, p.Dur)
		if e.Cmp(end) > 0 {
			end.Set(e)
		}
	}
	ev.OriginSeconds = origin.RatString()
	ev.PresentedExtentSeconds = new(big.Rat).Sub(end, origin).RatString()
	ev.MaxUncoveredSeconds = maxGap.RatString()
	ev.Status = "packet_intervals_covered_decoded_source_UNKNOWN"
	if ev.NonIncreasingDTS > 0 || ev.DuplicatePresentationPTS > 0 {
		ev.Status = "packet_timing_ambiguity_hold"
	} else if ev.UncoveredIntervals > 0 {
		ev.Status = "uncovered_packet_presentation_intervals_hold"
	}
	return ev
}
