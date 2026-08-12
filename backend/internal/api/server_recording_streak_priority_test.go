package api

import "testing"

func TestFinishStreakPriorityProtectsNearGoal(t *testing.T) {
	item := streakPriorityRecording{}
	for i := 0; i < 13; i++ {
		item.RecentWindows = append(item.RecentWindows, streakWindow{Grade: "GOOD_CANDIDATE"})
	}
	finishStreakPriority(&item, "active")
	if item.CurrentStreak != 13 || item.WindowsTo14 != 1 || item.Protection != "critical_13_of_14" || item.Action != "protect_no_nonessential_change" {
		t.Fatalf("item=%+v", item)
	}
}

func TestFinishStreakPriorityAllowsOnlyOneAcceptable(t *testing.T) {
	item := streakPriorityRecording{RecentWindows: []streakWindow{{Grade: "GOOD_CANDIDATE"}, {Grade: "ACCEPTABLE_CANDIDATE"}, {Grade: "GOOD_CANDIDATE"}, {Grade: "ACCEPTABLE_CANDIDATE"}, {Grade: "GOOD_CANDIDATE"}}}
	finishStreakPriority(&item, "active")
	if item.CurrentStreak != 3 || item.AcceptableInRun != 1 {
		t.Fatalf("item=%+v", item)
	}
}

func TestFinishStreakPriorityFlagsEarlyAndRepeatedFailures(t *testing.T) {
	early := streakPriorityRecording{RecentWindows: []streakWindow{{Grade: "FAILED"}}}
	finishStreakPriority(&early, "active")
	if early.Lifecycle != "probation" || early.Action != "early_failure_review_replace_or_reprobe" {
		t.Fatalf("early=%+v", early)
	}
	repeated := streakPriorityRecording{RecentWindows: []streakWindow{{Grade: "FAILED"}, {Grade: "GOOD_CANDIDATE"}, {Grade: "UNKNOWN"}, {Grade: "GOOD_CANDIDATE"}}}
	finishStreakPriority(&repeated, "active")
	if repeated.Action != "repeated_failure_replace_or_source_repair" {
		t.Fatalf("repeated=%+v", repeated)
	}
}

func TestFinishStreakPriorityReplacementKeepsOwnZeroStreak(t *testing.T) {
	item := streakPriorityRecording{RecentWindows: []streakWindow{{Grade: "UNKNOWN"}}}
	finishStreakPriority(&item, "paused")
	if item.Lifecycle != "candidate" || item.CurrentStreak != 0 || item.WindowsTo14 != 14 {
		t.Fatalf("item=%+v", item)
	}
}
