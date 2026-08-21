package joinedrecording

import (
	"fmt"
	"math"
	"path"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/stitchcert"
)

type HourAllocation struct {
	DeliveryHour int          `json:"delivery_hour"`
	ClockHour    int          `json:"clock_hour"`
	Sources      []SourceClip `json:"sources"`
}

type CrossHourBoundary struct {
	PreviousHour               int        `json:"previous_delivery_hour"`
	NextHour                   int        `json:"next_delivery_hour"`
	PreviousClipID             *int64     `json:"previous_clip_id"`
	NextClipID                 *int64     `json:"next_clip_id"`
	PreviousPresentationEndUTC *time.Time `json:"previous_presentation_end_utc"`
	NextPresentationStartUTC   *time.Time `json:"next_presentation_start_utc"`
	SignedGapNanoseconds       *int64     `json:"signed_gap_nanoseconds"`
	ScheduledUTC               time.Time  `json:"scheduled_utc"`
	ActualSeamUTC              *time.Time `json:"actual_seam_utc"`
	BoundarySkewNanoseconds    *int64     `json:"boundary_skew_nanoseconds"`
	AllocationDecision         string     `json:"allocation_decision"`
	Verdict                    string     `json:"verdict"`
	Reason                     string     `json:"reason"`
}

type CrossDayBoundary struct {
	PreviousClipID             *int64     `json:"previous_clip_id"`
	NextClipID                 *int64     `json:"next_clip_id"`
	PreviousPresentationEndUTC *time.Time `json:"previous_presentation_end_utc"`
	NextPresentationStartUTC   *time.Time `json:"next_presentation_start_utc"`
	SignedGapNanoseconds       *int64     `json:"signed_gap_nanoseconds"`
	ScheduledPreviousEndUTC    time.Time  `json:"scheduled_previous_end_utc"`
	ScheduledNextStartUTC      time.Time  `json:"scheduled_next_start_utc"`
	BoundarySkewNanoseconds    *int64     `json:"boundary_skew_nanoseconds"`
	AllocationDecision         string     `json:"allocation_decision"`
	Verdict                    string     `json:"verdict"`
	Reason                     string     `json:"reason"`
}

type StreamDayDraft struct {
	BatchID            string              `json:"batch_id"`
	Generation         int                 `json:"generation"`
	RecordingID        int64               `json:"recording_id"`
	Timezone           string              `json:"timezone"`
	LocalDate          string              `json:"local_date"`
	QualificationDay   QualifiedDay        `json:"qualification_day"`
	QualificationSHA   string              `json:"qualification_sha256"`
	Hours              []HourAllocation    `json:"hours"`
	Boundaries         []CrossHourBoundary `json:"cross_hour_boundaries"`
	CrossDayBoundaries []CrossDayBoundary  `json:"cross_day_boundaries"`
}

// AllocateStreamDay validates the ordered run evidence before assigning exact
// source clips to the 12 delivery hours. A source crossing a clock boundary is
// kept whole and the next verified source seam records the boundary skew.
func AllocateStreamDay(req PlanRequest) (StreamDayDraft, error) {
	if len(req.Sources) == 0 || ValidateQualificationWindow(req.Qualification) != nil {
		return StreamDayDraft{}, fmt.Errorf("qualified stream-day sources are required")
	}
	loc, err := time.LoadLocation(req.Timezone)
	if err != nil {
		return StreamDayDraft{}, err
	}
	localDate := req.Sources[0].StartUTC.In(loc).Format("2006-01-02")
	if req.LocalDate != localDate || !qualificationContainsDate(req.Qualification, localDate) {
		return StreamDayDraft{}, fmt.Errorf("stream day is outside qualification")
	}
	hours := make([]HourAllocation, 12)
	for i := range hours {
		hours[i] = HourAllocation{DeliveryHour: i + 1, ClockHour: i + 8, Sources: []SourceClip{}}
	}
	for _, source := range req.Sources {
		if validatePreflightSource(source, req.RecordingID) != nil || !req.Qualification.permits(source) || source.StartUTC.In(loc).Format("2006-01-02") != localDate || source.EndUTC.In(loc).Format("2006-01-02") != localDate {
			return StreamDayDraft{}, fmt.Errorf("stream day contains an ineligible source")
		}
	}
	if err := validateOrderedAllocationSources(req.Sources, req.RecordingID, loc, localDate); err != nil {
		return StreamDayDraft{}, err
	}
	type seamCandidate struct {
		position int
		at       time.Time
		clipID   int64
	}
	candidates := make([]seamCandidate, 0, len(req.Sources)*2+2)
	for position := 0; position <= len(req.Sources); position++ {
		if position > 0 {
			candidates = append(candidates, seamCandidate{position: position, at: req.Sources[position-1].EndUTC, clipID: req.Sources[position-1].ClipID})
		}
		if position < len(req.Sources) {
			candidates = append(candidates, seamCandidate{position: position, at: req.Sources[position].StartUTC, clipID: req.Sources[position].ClipID})
		}
	}
	boundaries := make([]CrossHourBoundary, 0, 11)
	splits := make([]int, 11)
	minimumPosition := 0
	for boundaryIndex := 0; boundaryIndex < 11; boundaryIndex++ {
		scheduled := time.Date(req.Sources[0].StartUTC.In(loc).Year(), req.Sources[0].StartUTC.In(loc).Month(), req.Sources[0].StartUTC.In(loc).Day(), 9+boundaryIndex, 0, 0, 0, loc).UTC()
		best := seamCandidate{position: -1}
		var bestDistance time.Duration
		for _, candidate := range candidates {
			if candidate.position < minimumPosition {
				continue
			}
			distance := candidate.at.Sub(scheduled)
			if distance < 0 {
				distance = -distance
			}
			if best.position < 0 || distance < bestDistance || (distance == bestDistance && (candidate.at.Before(best.at) || (candidate.at.Equal(best.at) && (candidate.position < best.position || (candidate.position == best.position && candidate.clipID < best.clipID))))) {
				best, bestDistance = candidate, distance
			}
		}
		if best.position < 0 {
			return StreamDayDraft{}, fmt.Errorf("no verified seam for local hour boundary")
		}
		splits[boundaryIndex], minimumPosition = best.position, best.position
		var previousID, nextID *int64
		var previousEnd, nextStart *time.Time
		if best.position > 0 {
			previousID = int64Pointer(req.Sources[best.position-1].ClipID)
			previousEnd = timePointer(req.Sources[best.position-1].EndUTC)
		}
		if best.position < len(req.Sources) {
			next := req.Sources[best.position]
			nextID = int64Pointer(next.ClipID)
			nextStart = timePointer(next.StartUTC)
		}
		verdict, reason, decision := "allocated", "closest_source_boundary", "split_before_next_source"
		var signedGap, skew *int64
		actualSeam := timePointer(best.at)
		if previousEnd != nil && nextStart != nil {
			signedGap = int64Pointer(nextStart.Sub(*previousEnd).Nanoseconds())
			skew = int64Pointer(best.at.Sub(scheduled).Nanoseconds())
		} else {
			verdict, actualSeam = "absent_source", nil
			if previousEnd == nil {
				reason, decision = "previous_source_absent", "no_source_before_boundary"
			} else {
				reason, decision = "next_source_absent", "no_source_after_boundary"
			}
		}
		boundaries = append(boundaries, CrossHourBoundary{PreviousHour: boundaryIndex + 1, NextHour: boundaryIndex + 2, PreviousClipID: previousID, NextClipID: nextID, PreviousPresentationEndUTC: previousEnd, NextPresentationStartUTC: nextStart, SignedGapNanoseconds: signedGap, ScheduledUTC: scheduled, ActualSeamUTC: actualSeam, BoundarySkewNanoseconds: skew, AllocationDecision: decision, Verdict: verdict, Reason: reason})
	}
	start := 0
	for i := range hours {
		end := len(req.Sources)
		if i < len(splits) {
			end = splits[i]
		}
		hours[i].Sources = append(hours[i].Sources, req.Sources[start:end]...)
		start = end
	}
	qualificationDay, ok := qualifiedDay(req.Qualification, localDate)
	if !ok {
		return StreamDayDraft{}, fmt.Errorf("stream day lacks frozen qualification evidence")
	}
	crossDay, err := crossDayBoundaries(req, req.Sources[0], req.Sources[len(req.Sources)-1], localDate)
	if err != nil {
		return StreamDayDraft{}, err
	}
	return StreamDayDraft{BatchID: req.BatchID, Generation: req.Generation, RecordingID: req.RecordingID, Timezone: req.Timezone, LocalDate: localDate, QualificationDay: qualificationDay, QualificationSHA: req.Qualification.EvidenceSHA, Hours: hours, Boundaries: boundaries, CrossDayBoundaries: crossDay}, nil
}

func BuildGapOnlyStreamDay(req PlanRequest, localDate string) (StreamDayDraft, error) {
	if len(req.Sources) != 0 || req.RecordingID <= 0 || ValidateQualificationWindow(req.Qualification) != nil || !qualificationContainsDate(req.Qualification, localDate) {
		return StreamDayDraft{}, fmt.Errorf("invalid source-free qualified stream day")
	}
	hours := make([]HourAllocation, 12)
	for i := range hours {
		hours[i] = HourAllocation{DeliveryHour: i + 1, ClockHour: i + 8, Sources: []SourceClip{}}
	}
	loc, err := time.LoadLocation(req.Timezone)
	if err != nil {
		return StreamDayDraft{}, err
	}
	day, err := time.ParseInLocation("2006-01-02", localDate, loc)
	if err != nil {
		return StreamDayDraft{}, err
	}
	boundaries := make([]CrossHourBoundary, 11)
	for i := range boundaries {
		scheduled := time.Date(day.Year(), day.Month(), day.Day(), i+9, 0, 0, 0, loc).UTC()
		boundaries[i] = CrossHourBoundary{PreviousHour: i + 1, NextHour: i + 2, ScheduledUTC: scheduled, AllocationDecision: "no_sources", Verdict: "absent_source", Reason: "both_sources_absent"}
	}
	qualificationDay, ok := qualifiedDay(req.Qualification, localDate)
	if !ok {
		return StreamDayDraft{}, fmt.Errorf("stream day lacks frozen qualification evidence")
	}
	crossDay, err := emptyCrossDayBoundaries(req, localDate)
	if err != nil {
		return StreamDayDraft{}, err
	}
	return StreamDayDraft{BatchID: req.BatchID, Generation: req.Generation, RecordingID: req.RecordingID, Timezone: req.Timezone, LocalDate: localDate, QualificationDay: qualificationDay, QualificationSHA: req.Qualification.EvidenceSHA, Hours: hours, Boundaries: boundaries, CrossDayBoundaries: crossDay}, nil
}

func absentCrossDayBoundaries(loc *time.Location, localDate string) []CrossDayBoundary {
	day, _ := time.ParseInLocation("2006-01-02", localDate, loc)
	start := time.Date(day.Year(), day.Month(), day.Day(), 8, 0, 0, 0, loc).UTC()
	end := time.Date(day.Year(), day.Month(), day.Day(), 20, 0, 0, 0, loc).UTC()
	previous := day.AddDate(0, 0, -1)
	next := day.AddDate(0, 0, 1)
	return []CrossDayBoundary{
		{ScheduledPreviousEndUTC: time.Date(previous.Year(), previous.Month(), previous.Day(), 20, 0, 0, 0, loc).UTC(), ScheduledNextStartUTC: start, AllocationDecision: "no_previous_day_source", Verdict: "absent_source", Reason: "previous_source_absent"},
		{ScheduledPreviousEndUTC: end, ScheduledNextStartUTC: time.Date(next.Year(), next.Month(), next.Day(), 8, 0, 0, 0, loc).UTC(), AllocationDecision: "no_next_day_source", Verdict: "absent_source", Reason: "next_source_absent"},
	}
}

func emptyCrossDayBoundaries(req PlanRequest, localDate string) ([]CrossDayBoundary, error) {
	loc, err := time.LoadLocation(req.Timezone)
	if err != nil {
		return nil, err
	}
	edges := absentCrossDayBoundaries(loc, localDate)
	if req.PreviousDayLast != nil {
		if err := validateNeighbor(*req.PreviousDayLast, req.RecordingID, loc, localDate, -1); err != nil {
			return nil, err
		}
		edges[0].PreviousClipID = int64Pointer(req.PreviousDayLast.ClipID)
		edges[0].PreviousPresentationEndUTC = timePointer(req.PreviousDayLast.EndUTC)
		edges[0].AllocationDecision, edges[0].Reason = "empty_day_after_previous_source", "next_source_absent"
	}
	if req.NextDayFirst != nil {
		if err := validateNeighbor(*req.NextDayFirst, req.RecordingID, loc, localDate, 1); err != nil {
			return nil, err
		}
		edges[1].NextClipID = int64Pointer(req.NextDayFirst.ClipID)
		edges[1].NextPresentationStartUTC = timePointer(req.NextDayFirst.StartUTC)
		edges[1].AllocationDecision, edges[1].Reason = "empty_day_before_next_source", "previous_source_absent"
	}
	return edges, nil
}

func validateNeighbor(source SourceClip, recordingID int64, loc *time.Location, localDate string, dayOffset int) error {
	day, err := time.ParseInLocation("2006-01-02", localDate, loc)
	if err != nil {
		return err
	}
	want := day.AddDate(0, 0, dayOffset).Format("2006-01-02")
	if validatePreflightSource(source, recordingID) != nil || source.StartUTC.In(loc).Format("2006-01-02") != want || source.EndUTC.In(loc).Format("2006-01-02") != want {
		return fmt.Errorf("cross-day neighbor differs from frozen recording date")
	}
	return nil
}

func crossDayBoundaries(req PlanRequest, first, last SourceClip, localDate string) ([]CrossDayBoundary, error) {
	loc, err := time.LoadLocation(req.Timezone)
	if err != nil {
		return nil, err
	}
	edges := absentCrossDayBoundaries(loc, localDate)
	day, err := time.ParseInLocation("2006-01-02", localDate, loc)
	if err != nil {
		return nil, err
	}
	if req.PreviousDayLast != nil {
		if err := validateNeighbor(*req.PreviousDayLast, req.RecordingID, loc, localDate, -1); err != nil {
			return nil, err
		}
		edges[0], err = buildCrossDayBoundary(*req.PreviousDayLast, first, req.Timezone, day.AddDate(0, 0, -1).Format("2006-01-02"), localDate)
		if err != nil {
			return nil, err
		}
	} else {
		edges[0].NextClipID = int64Pointer(first.ClipID)
		edges[0].NextPresentationStartUTC = timePointer(first.StartUTC)
	}
	if req.NextDayFirst != nil {
		if err := validateNeighbor(*req.NextDayFirst, req.RecordingID, loc, localDate, 1); err != nil {
			return nil, err
		}
		edges[1], err = buildCrossDayBoundary(last, *req.NextDayFirst, req.Timezone, localDate, day.AddDate(0, 0, 1).Format("2006-01-02"))
		if err != nil {
			return nil, err
		}
	} else {
		edges[1].PreviousClipID = int64Pointer(last.ClipID)
		edges[1].PreviousPresentationEndUTC = timePointer(last.EndUTC)
	}
	return edges, nil
}

func buildCrossDayBoundary(previous, next SourceClip, timezone, previousDate, nextDate string) (CrossDayBoundary, error) {
	loc, err := time.LoadLocation(timezone)
	if err != nil {
		return CrossDayBoundary{}, err
	}
	previousDay, err := time.ParseInLocation("2006-01-02", previousDate, loc)
	if err != nil {
		return CrossDayBoundary{}, err
	}
	nextDay, err := time.ParseInLocation("2006-01-02", nextDate, loc)
	if err != nil {
		return CrossDayBoundary{}, err
	}
	scheduledEnd := time.Date(previousDay.Year(), previousDay.Month(), previousDay.Day(), 20, 0, 0, 0, loc).UTC()
	scheduledStart := time.Date(nextDay.Year(), nextDay.Month(), nextDay.Day(), 8, 0, 0, 0, loc).UTC()
	signedGap := next.StartUTC.Sub(previous.EndUTC).Nanoseconds()
	verdict := "scheduled_gap"
	if signedGap < 0 {
		verdict = "overlap"
	}
	skew := signedGap - scheduledStart.Sub(scheduledEnd).Nanoseconds()
	return CrossDayBoundary{PreviousClipID: int64Pointer(previous.ClipID), NextClipID: int64Pointer(next.ClipID), PreviousPresentationEndUTC: timePointer(previous.EndUTC), NextPresentationStartUTC: timePointer(next.StartUTC), SignedGapNanoseconds: int64Pointer(signedGap), ScheduledPreviousEndUTC: scheduledEnd, ScheduledNextStartUTC: scheduledStart, BoundarySkewNanoseconds: int64Pointer(skew), AllocationDecision: "separate_local_days", Verdict: verdict, Reason: "scheduled_day_boundary"}, nil
}

func int64Pointer(value int64) *int64        { return &value }
func timePointer(value time.Time) *time.Time { return &value }

// StreamDayAllocation is the sealed ledger proving that local-hour batches
// neither overlap nor omit any source in one recording day.
type StreamDayAllocation struct {
	SchemaVersion      int                 `json:"schema_version"`
	BatchID            string              `json:"batch_id"`
	Generation         int                 `json:"generation"`
	RecordingID        int64               `json:"recording_id"`
	Timezone           string              `json:"timezone"`
	LocalDate          string              `json:"local_date"`
	QualificationDay   QualifiedDay        `json:"qualification_day"`
	QualificationSHA   string              `json:"qualification_sha256"`
	SourceClaimSHA256  string              `json:"source_claim_sha256"`
	SourceClipCount    int                 `json:"source_clip_count"`
	SourceBytes        int64               `json:"source_bytes"`
	FirstClipID        *int64              `json:"first_clip_id"`
	LastClipID         *int64              `json:"last_clip_id"`
	ConsecutivePairs   []SourcePair        `json:"consecutive_pairs"`
	Sources            []SourceClip        `json:"sources"`
	Hours              []LedgerHour        `json:"hours"`
	HourSourceSHA256   []string            `json:"hour_source_claim_sha256"`
	Boundaries         []CrossHourBoundary `json:"cross_hour_boundaries"`
	CrossDayBoundaries []CrossDayBoundary  `json:"cross_day_boundaries"`
	LedgerSHA256       string              `json:"ledger_sha256"`
}

type SourcePair struct {
	PreviousClipID             int64     `json:"previous_clip_id"`
	NextClipID                 int64     `json:"next_clip_id"`
	PreviousPresentationEndUTC time.Time `json:"previous_presentation_end_utc"`
	NextPresentationStartUTC   time.Time `json:"next_presentation_start_utc"`
	SignedGapNanoseconds       int64     `json:"signed_gap_nanoseconds"`
}

type LedgerHour struct {
	DeliveryHour  int     `json:"delivery_hour"`
	ClockHour     int     `json:"clock_hour"`
	SourceClipIDs []int64 `json:"source_clip_ids"`
}

func SealStreamDayAllocation(draft StreamDayDraft) (StreamDayAllocation, error) {
	if !safeBatchID.MatchString(draft.BatchID) || draft.Generation <= 0 || draft.RecordingID <= 0 || !lowerHex64(draft.QualificationSHA) || validateQualifiedLedgerDay(draft.QualificationDay, draft.RecordingID, draft.Timezone, draft.LocalDate) != nil || len(draft.Hours) != 12 || len(draft.Boundaries) != 11 || len(draft.CrossDayBoundaries) != 2 {
		return StreamDayAllocation{}, fmt.Errorf("stream-day sources and hour plans are required")
	}
	if err := validateDraftBoundaries(draft); err != nil {
		return StreamDayAllocation{}, err
	}
	sources := []SourceClip{}
	var sourceBytes int64
	for _, hour := range draft.Hours {
		for _, source := range hour.Sources {
			if source.Object.SizeBytes > math.MaxInt64-sourceBytes {
				return StreamDayAllocation{}, fmt.Errorf("stream-day source bytes overflow")
			}
			sourceBytes += source.Object.SizeBytes
			sources = append(sources, sourceOnlyClips([]SourceClip{source})[0])
		}
	}
	hourSHAs := make([]string, 12)
	ledgerHours := make([]LedgerHour, 12)
	for i, hour := range draft.Hours {
		if hour.DeliveryHour != i+1 || hour.ClockHour != i+8 {
			return StreamDayAllocation{}, fmt.Errorf("stream-day hour identity differs")
		}
		hourSHAs[i], _, _ = sourceClaimSHA(hour.Sources)
		ids := make([]int64, len(hour.Sources))
		for j, source := range hour.Sources {
			ids[j] = source.ClipID
		}
		ledgerHours[i] = LedgerHour{DeliveryHour: hour.DeliveryHour, ClockHour: hour.ClockHour, SourceClipIDs: ids}
	}
	manifestSHA, _, err := sourceClaimSHA(sources)
	if err != nil {
		return StreamDayAllocation{}, err
	}
	var firstClipID, lastClipID *int64
	pairs := make([]SourcePair, 0, max(0, len(sources)-1))
	if len(sources) > 0 {
		firstClipID, lastClipID = int64Pointer(sources[0].ClipID), int64Pointer(sources[len(sources)-1].ClipID)
	}
	for i := 1; i < len(sources); i++ {
		pairs = append(pairs, SourcePair{PreviousClipID: sources[i-1].ClipID, NextClipID: sources[i].ClipID, PreviousPresentationEndUTC: sources[i-1].EndUTC, NextPresentationStartUTC: sources[i].StartUTC, SignedGapNanoseconds: sources[i].StartUTC.Sub(sources[i-1].EndUTC).Nanoseconds()})
	}
	ledger := StreamDayAllocation{SchemaVersion: 1, BatchID: draft.BatchID, Generation: draft.Generation, RecordingID: draft.RecordingID, Timezone: draft.Timezone, LocalDate: draft.LocalDate, QualificationDay: draft.QualificationDay, QualificationSHA: draft.QualificationSHA, SourceClaimSHA256: manifestSHA, SourceClipCount: len(sources), SourceBytes: sourceBytes, FirstClipID: firstClipID, LastClipID: lastClipID, ConsecutivePairs: pairs, Sources: append([]SourceClip{}, sources...), Hours: ledgerHours, HourSourceSHA256: hourSHAs, Boundaries: append([]CrossHourBoundary{}, draft.Boundaries...), CrossDayBoundaries: append([]CrossDayBoundary{}, draft.CrossDayBoundaries...)}
	digest, _, err := stitchcert.CanonicalSHA(ledger)
	if err != nil {
		return StreamDayAllocation{}, err
	}
	ledger.LedgerSHA256 = digest
	return ledger, nil
}

func validateDraftBoundaries(draft StreamDayDraft) error {
	loc, err := time.LoadLocation(draft.Timezone)
	if err != nil {
		return err
	}
	day, err := time.ParseInLocation("2006-01-02", draft.LocalDate, loc)
	if err != nil {
		return err
	}
	if len(draft.Hours) != 12 || len(draft.Boundaries) != 11 {
		return fmt.Errorf("stream-day must contain 12 hours and 11 boundaries")
	}
	if err := validateCrossDaySchedule(draft.CrossDayBoundaries, day, loc); err != nil {
		return err
	}
	sources := []SourceClip{}
	for _, hour := range draft.Hours {
		sources = append(sources, hour.Sources...)
	}
	if err := validateOrderedAllocationSources(sources, draft.RecordingID, loc, draft.LocalDate); err != nil {
		return err
	}
	if len(sources) == 0 {
		for i, boundary := range draft.Boundaries {
			scheduled := time.Date(day.Year(), day.Month(), day.Day(), 9+i, 0, 0, 0, loc).UTC()
			if boundary.PreviousHour != i+1 || boundary.NextHour != i+2 || !boundary.ScheduledUTC.Equal(scheduled) || boundary.PreviousClipID != nil || boundary.NextClipID != nil || boundary.PreviousPresentationEndUTC != nil || boundary.NextPresentationStartUTC != nil || boundary.SignedGapNanoseconds != nil || boundary.ActualSeamUTC != nil || boundary.BoundarySkewNanoseconds != nil || boundary.AllocationDecision != "no_sources" || boundary.Verdict != "absent_source" || boundary.Reason != "both_sources_absent" {
				return fmt.Errorf("source-free stream-day boundary %d differs", i+1)
			}
		}
		return validateCrossDayBoundaries(draft.CrossDayBoundaries, sources)
	}
	minimum := 0
	consumed := 0
	for i := 0; i < 11; i++ {
		consumed += len(draft.Hours[i].Sources)
		scheduled := time.Date(day.Year(), day.Month(), day.Day(), 9+i, 0, 0, 0, loc).UTC()
		bestPosition := -1
		var bestAt time.Time
		var bestDistance time.Duration
		var bestClipID int64
		consider := func(position int, at time.Time, clipID int64) {
			if position < minimum {
				return
			}
			distance := at.Sub(scheduled)
			if distance < 0 {
				distance = -distance
			}
			if bestPosition < 0 || distance < bestDistance || (distance == bestDistance && (at.Before(bestAt) || (at.Equal(bestAt) && (position < bestPosition || (position == bestPosition && clipID < bestClipID))))) {
				bestPosition, bestAt, bestDistance, bestClipID = position, at, distance, clipID
			}
		}
		for position := minimum; position <= len(sources); position++ {
			if position > 0 {
				consider(position, sources[position-1].EndUTC, sources[position-1].ClipID)
			}
			if position < len(sources) {
				consider(position, sources[position].StartUTC, sources[position].ClipID)
			}
		}
		if consumed != bestPosition {
			return fmt.Errorf("stream-day hour allocation is not at the closest frozen seam")
		}
		expected := crossHourBoundary(sources, bestPosition, scheduled, i+1, bestAt)
		_, got, _ := stitchcert.CanonicalSHA(draft.Boundaries[i])
		_, want, _ := stitchcert.CanonicalSHA(expected)
		if string(got) != string(want) {
			return fmt.Errorf("stream-day boundary %d differs", i+1)
		}
		minimum = bestPosition
	}
	return validateCrossDayBoundaries(draft.CrossDayBoundaries, sources)
}

func validateOrderedAllocationSources(sources []SourceClip, recordingID int64, loc *time.Location, localDate string) error {
	seenClips := map[int64]bool{}
	seenObjects := map[string]bool{}
	for i, source := range sources {
		if validatePreflightSource(source, recordingID) != nil || source.StartUTC.In(loc).Format("2006-01-02") != localDate || source.EndUTC.In(loc).Format("2006-01-02") != localDate || seenClips[source.ClipID] {
			return fmt.Errorf("stream day source identity or date differs")
		}
		objectIdentity := strings.Join([]string{source.Provider, source.Endpoint, source.Region, source.Bucket, source.Object.Key, source.Object.VersionID, source.Object.ETag}, "\x00")
		if seenObjects[objectIdentity] {
			return fmt.Errorf("stream day duplicates a frozen storage object")
		}
		seenClips[source.ClipID], seenObjects[objectIdentity] = true, true
		if i > 0 {
			previous := sources[i-1]
			if source.StartUTC.Before(previous.StartUTC) || (source.StartUTC.Equal(previous.StartUTC) && source.ClipID <= previous.ClipID) {
				return fmt.Errorf("stream day lacks frozen source ordering")
			}
		}
	}
	return nil
}

func validateCrossDaySchedule(boundaries []CrossDayBoundary, day time.Time, loc *time.Location) error {
	if len(boundaries) != 2 {
		return fmt.Errorf("stream-day must classify both cross-day boundaries")
	}
	want := [][2]time.Time{
		{time.Date(day.AddDate(0, 0, -1).Year(), day.AddDate(0, 0, -1).Month(), day.AddDate(0, 0, -1).Day(), 20, 0, 0, 0, loc).UTC(), time.Date(day.Year(), day.Month(), day.Day(), 8, 0, 0, 0, loc).UTC()},
		{time.Date(day.Year(), day.Month(), day.Day(), 20, 0, 0, 0, loc).UTC(), time.Date(day.AddDate(0, 0, 1).Year(), day.AddDate(0, 0, 1).Month(), day.AddDate(0, 0, 1).Day(), 8, 0, 0, 0, loc).UTC()},
	}
	for i := range boundaries {
		if !boundaries[i].ScheduledPreviousEndUTC.Equal(want[i][0]) || !boundaries[i].ScheduledNextStartUTC.Equal(want[i][1]) {
			return fmt.Errorf("cross-day boundary %d has wrong scheduled local time", i+1)
		}
	}
	return validateCrossDayBoundaries(boundaries, nil)
}

func validateCrossDayBoundaries(boundaries []CrossDayBoundary, sources []SourceClip) error {
	if len(boundaries) != 2 {
		return fmt.Errorf("stream-day must classify both cross-day boundaries")
	}
	for i, boundary := range boundaries {
		if boundary.ScheduledPreviousEndUTC.IsZero() || boundary.ScheduledNextStartUTC.IsZero() || !boundary.ScheduledPreviousEndUTC.Before(boundary.ScheduledNextStartUTC) || !reasonCode.MatchString(boundary.Reason) || !reasonCode.MatchString(boundary.AllocationDecision) {
			return fmt.Errorf("cross-day boundary %d lacks scheduled evidence", i+1)
		}
		previousPresent := boundary.PreviousClipID != nil && boundary.PreviousPresentationEndUTC != nil
		nextPresent := boundary.NextClipID != nil && boundary.NextPresentationStartUTC != nil
		if (boundary.PreviousClipID == nil) != (boundary.PreviousPresentationEndUTC == nil) || (boundary.NextClipID == nil) != (boundary.NextPresentationStartUTC == nil) {
			return fmt.Errorf("cross-day boundary %d has partial source identity", i+1)
		}
		if boundary.Verdict == "absent_source" {
			if previousPresent && nextPresent || boundary.SignedGapNanoseconds != nil || boundary.BoundarySkewNanoseconds != nil {
				return fmt.Errorf("cross-day absent boundary %d differs", i+1)
			}
			wantReason, wantDecision := "previous_source_absent", "no_previous_day_source"
			if i == 0 && previousPresent {
				wantReason, wantDecision = "next_source_absent", "empty_day_after_previous_source"
			} else if i == 1 {
				wantReason, wantDecision = "next_source_absent", "no_next_day_source"
				if nextPresent {
					wantReason, wantDecision = "previous_source_absent", "empty_day_before_next_source"
				}
			}
			if boundary.Reason != wantReason || boundary.AllocationDecision != wantDecision {
				return fmt.Errorf("cross-day absent boundary %d lacks exact reason", i+1)
			}
		} else {
			if !previousPresent || !nextPresent || boundary.SignedGapNanoseconds == nil || *boundary.SignedGapNanoseconds != boundary.NextPresentationStartUTC.Sub(*boundary.PreviousPresentationEndUTC).Nanoseconds() || boundary.BoundarySkewNanoseconds == nil {
				return fmt.Errorf("cross-day boundary %d lacks exact adjacent timing", i+1)
			}
			expectedSkew := *boundary.SignedGapNanoseconds - boundary.ScheduledNextStartUTC.Sub(boundary.ScheduledPreviousEndUTC).Nanoseconds()
			if *boundary.BoundarySkewNanoseconds != expectedSkew {
				return fmt.Errorf("cross-day boundary %d skew differs", i+1)
			}
			wantVerdict := "scheduled_gap"
			if *boundary.SignedGapNanoseconds < 0 {
				wantVerdict = "overlap"
			}
			if boundary.Verdict != wantVerdict || boundary.AllocationDecision != "separate_local_days" || boundary.Reason != "scheduled_day_boundary" {
				return fmt.Errorf("cross-day boundary %d classification differs", i+1)
			}
		}
	}
	if len(sources) > 0 {
		first, last := sources[0], sources[len(sources)-1]
		if boundaries[0].NextClipID == nil || *boundaries[0].NextClipID != first.ClipID || boundaries[0].NextPresentationStartUTC == nil || !boundaries[0].NextPresentationStartUTC.Equal(first.StartUTC) || boundaries[1].PreviousClipID == nil || *boundaries[1].PreviousClipID != last.ClipID || boundaries[1].PreviousPresentationEndUTC == nil || !boundaries[1].PreviousPresentationEndUTC.Equal(last.EndUTC) {
			return fmt.Errorf("cross-day boundary does not bind stream-day edges")
		}
	}
	return nil
}

func crossHourBoundary(sources []SourceClip, position int, scheduled time.Time, hour int, actual time.Time) CrossHourBoundary {
	boundary := CrossHourBoundary{PreviousHour: hour, NextHour: hour + 1, ScheduledUTC: scheduled}
	if position > 0 {
		boundary.PreviousClipID = int64Pointer(sources[position-1].ClipID)
		boundary.PreviousPresentationEndUTC = timePointer(sources[position-1].EndUTC)
	}
	if position < len(sources) {
		boundary.NextClipID = int64Pointer(sources[position].ClipID)
		boundary.NextPresentationStartUTC = timePointer(sources[position].StartUTC)
	}
	if boundary.PreviousClipID == nil || boundary.NextClipID == nil {
		boundary.AllocationDecision, boundary.Verdict = "no_source_before_boundary", "absent_source"
		boundary.Reason = "previous_source_absent"
		if boundary.PreviousClipID != nil {
			boundary.AllocationDecision, boundary.Reason = "no_source_after_boundary", "next_source_absent"
		}
		return boundary
	}
	gap := boundary.NextPresentationStartUTC.Sub(*boundary.PreviousPresentationEndUTC).Nanoseconds()
	skew := actual.Sub(scheduled).Nanoseconds()
	boundary.SignedGapNanoseconds, boundary.ActualSeamUTC, boundary.BoundarySkewNanoseconds = int64Pointer(gap), timePointer(actual), int64Pointer(skew)
	boundary.AllocationDecision, boundary.Verdict, boundary.Reason = "split_before_next_source", "allocated", "closest_source_boundary"
	return boundary
}

func ValidateStreamDayAllocation(ledger StreamDayAllocation, draft StreamDayDraft) error {
	want := ledger.LedgerSHA256
	ledger.LedgerSHA256 = ""
	digest, _, err := stitchcert.CanonicalSHA(ledger)
	if err != nil || digest != want || !lowerHex64(want) {
		return fmt.Errorf("stream-day allocation is not sealed")
	}
	rebuilt, err := SealStreamDayAllocation(draft)
	if err != nil || rebuilt.LedgerSHA256 != want {
		return fmt.Errorf("stream-day allocation differs from canonical source assignment")
	}
	return nil
}

func CanonicalAllocationLedgerArtifact(ledger StreamDayAllocation) ([]byte, string, error) {
	want := ledger.LedgerSHA256
	logical := ledger
	logical.LedgerSHA256 = ""
	digest, _, err := stitchcert.CanonicalSHA(logical)
	if err != nil || digest != want || !lowerHex64(want) || ledger.SchemaVersion != 1 || !safeBatchID.MatchString(ledger.BatchID) || ledger.Generation <= 0 || ledger.RecordingID <= 0 || !lowerHex64(ledger.QualificationSHA) || validateQualifiedLedgerDay(ledger.QualificationDay, ledger.RecordingID, ledger.Timezone, ledger.LocalDate) != nil || len(ledger.Hours) != 12 || len(ledger.HourSourceSHA256) != 12 || ledger.SourceClipCount != len(ledger.Sources) || ledger.SourceBytes < 0 || len(ledger.ConsecutivePairs) != max(0, len(ledger.Sources)-1) {
		return nil, "", fmt.Errorf("allocation ledger is not canonically sealed")
	}
	var sourceBytes int64
	flatIDs := make([]int64, 0, len(ledger.Sources))
	for i, source := range ledger.Sources {
		if validatePreflightSource(source, ledger.RecordingID) != nil || source.AudioContract != nil || source.SeamToPrevious != (SeamEvidence{}) || source.Object.SizeBytes > math.MaxInt64-sourceBytes {
			return nil, "", fmt.Errorf("allocation ledger source %d differs", i+1)
		}
		sourceBytes += source.Object.SizeBytes
		if i > 0 && (ledger.ConsecutivePairs[i-1].PreviousClipID != ledger.Sources[i-1].ClipID || ledger.ConsecutivePairs[i-1].NextClipID != source.ClipID || !ledger.ConsecutivePairs[i-1].PreviousPresentationEndUTC.Equal(ledger.Sources[i-1].EndUTC) || !ledger.ConsecutivePairs[i-1].NextPresentationStartUTC.Equal(source.StartUTC) || ledger.ConsecutivePairs[i-1].SignedGapNanoseconds != source.StartUTC.Sub(ledger.Sources[i-1].EndUTC).Nanoseconds()) {
			return nil, "", fmt.Errorf("allocation ledger consecutive pair differs")
		}
	}
	hours := make([]HourAllocation, len(ledger.Hours))
	cursor := 0
	for i, hour := range ledger.Hours {
		if hour.DeliveryHour != i+1 || hour.ClockHour != i+8 {
			return nil, "", fmt.Errorf("allocation ledger hour differs")
		}
		flatIDs = append(flatIDs, hour.SourceClipIDs...)
		for _, clipID := range hour.SourceClipIDs {
			if cursor >= len(ledger.Sources) || ledger.Sources[cursor].ClipID != clipID {
				return nil, "", fmt.Errorf("allocation ledger hour source differs")
			}
			hours[i].Sources = append(hours[i].Sources, ledger.Sources[cursor])
			cursor++
		}
		hours[i].DeliveryHour, hours[i].ClockHour = hour.DeliveryHour, hour.ClockHour
	}
	if sourceBytes != ledger.SourceBytes || len(flatIDs) != len(ledger.Sources) || (len(ledger.Sources) == 0 && (ledger.FirstClipID != nil || ledger.LastClipID != nil)) || (len(ledger.Sources) > 0 && (ledger.FirstClipID == nil || ledger.LastClipID == nil || *ledger.FirstClipID != ledger.Sources[0].ClipID || *ledger.LastClipID != ledger.Sources[len(ledger.Sources)-1].ClipID)) {
		return nil, "", fmt.Errorf("allocation ledger denominator differs")
	}
	for i, source := range ledger.Sources {
		if flatIDs[i] != source.ClipID {
			return nil, "", fmt.Errorf("allocation ledger omits or reorders a source")
		}
	}
	draft := StreamDayDraft{BatchID: ledger.BatchID, Generation: ledger.Generation, RecordingID: ledger.RecordingID, Timezone: ledger.Timezone, LocalDate: ledger.LocalDate, QualificationDay: ledger.QualificationDay, QualificationSHA: ledger.QualificationSHA, Hours: hours, Boundaries: ledger.Boundaries, CrossDayBoundaries: ledger.CrossDayBoundaries}
	if err := validateDraftBoundaries(draft); err != nil {
		return nil, "", fmt.Errorf("allocation ledger boundary proof differs: %w", err)
	}
	for i, hour := range hours {
		if digest, _, err := sourceClaimSHA(hour.Sources); err != nil || digest != ledger.HourSourceSHA256[i] {
			return nil, "", fmt.Errorf("allocation ledger hour source identity differs")
		}
	}
	if digest, _, err := sourceClaimSHA(ledger.Sources); err != nil || digest != ledger.SourceClaimSHA256 {
		return nil, "", fmt.Errorf("allocation ledger source identity differs")
	}
	artifactSHA, canonical, err := stitchcert.CanonicalSHA(ledger)
	if err != nil || len(canonical) > MaxCanonicalJSONBytes {
		return nil, "", fmt.Errorf("allocation ledger artifact exceeds canonical limit")
	}
	return canonical, artifactSHA, nil
}

func CanonicalAllocationLedgerPaths(batchID string, recordingID int64, localDate string) (string, string, error) {
	if !safeBatchID.MatchString(batchID) || recordingID <= 0 {
		return "", "", fmt.Errorf("invalid allocation ledger identity")
	}
	if _, err := time.Parse("2006-01-02", localDate); err != nil {
		return "", "", err
	}
	relative := path.Join("coverage", "ledgers", fmt.Sprintf("%d", recordingID), localDate+".json")
	return relative, path.Join("joined", batchID, relative), nil
}

func validateQualifiedLedgerDay(day QualifiedDay, recordingID int64, timezone, localDate string) error {
	loc, err := time.LoadLocation(timezone)
	if err != nil || recordingID <= 0 || day.LocalDate != localDate || day.JobID <= 0 || day.QualityTier != "good+" || day.CompletedAt.Before(day.WindowEnd) {
		return fmt.Errorf("allocation ledger qualification differs")
	}
	start, end := day.WindowStart.In(loc), day.WindowEnd.In(loc)
	if !day.WindowEnd.After(day.WindowStart) || day.WindowEnd.Sub(day.WindowStart) != 12*time.Hour || start.Format("2006-01-02") != localDate || end.Format("2006-01-02") != localDate || start.Hour() != 8 || start.Minute() != 0 || start.Second() != 0 || start.Nanosecond() != 0 || end.Hour() != 20 || end.Minute() != 0 || end.Second() != 0 || end.Nanosecond() != 0 {
		return fmt.Errorf("allocation ledger qualification clock differs")
	}
	return nil
}

func canonicalLedgerID(batchID string, recordingID int64, localDate string, generation int) string {
	return fmt.Sprintf("%s__recording-%d__date-%s__generation-%d", batchID, recordingID, localDate, generation)
}
