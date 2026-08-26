package api

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/daydemir/stoarama/backend/internal/joinedrecording"
)

func TestFrozenBatchClaimAdmissionRequiresReportedCapacity(t *testing.T) {
	legacy := joinedrecording.WorkClaimRequest{}
	if _, err := joinedClaimCapacity(legacy, true); err == nil {
		t.Fatal("broad claim admitted without a capacity report")
	}
	legacy.ScratchAvailableBytes = 2 << 30
	legacy.TaskBudgetBytes = 1536 << 20
	got, err := joinedClaimCapacity(legacy, true)
	if err != nil || got != 1536<<20 {
		t.Fatalf("capacity=%d err=%v", got, err)
	}
	if got, err = joinedClaimCapacity(joinedrecording.WorkClaimRequest{}, false); err != nil || got != 1<<63-1 {
		t.Fatalf("bounded canary capacity=%d err=%v", got, err)
	}
}

func TestJoinedFailureBackoffIsStableCappedAndExhausts(t *testing.T) {
	now := time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC)
	token := uuid.MustParse("35d99714-2469-4e3b-bdca-31749b42f261")
	state, first := joinedFailureDisposition("transient", 1, token, now)
	stateAgain, again := joinedFailureDisposition("transient", 1, token, now)
	if state != "retry" || stateAgain != state || !first.Equal(again) || !first.After(now) {
		t.Fatalf("unstable first backoff state=%q first=%s again=%s", state, first, again)
	}
	_, capped := joinedFailureDisposition("resource", 7, token, now)
	if capped.After(now.Add(joinedFailureBackoffMax)) {
		t.Fatalf("backoff exceeded cap: %s", capped.Sub(now))
	}
	if state, retry := joinedFailureDisposition("transient", joinedMaxAttempts, token, now); state != "terminal" || !retry.IsZero() {
		t.Fatalf("attempt exhaustion state=%q retry=%s", state, retry)
	}
	if state, retry := joinedFailureDisposition("deterministic", 1, token, now); state != "terminal" || !retry.IsZero() {
		t.Fatalf("deterministic failure state=%q retry=%s", state, retry)
	}
}

func TestJoinedFailureBackoffStartsAfterLiveLease(t *testing.T) {
	now := time.Date(2026, 8, 26, 7, 0, 0, 500_000_000, time.UTC)
	leaseExpires := now.Add(5 * time.Minute).Truncate(time.Second)
	token := uuid.MustParse("35d99714-2469-4e3b-bdca-31749b42f261")
	state, retry := joinedFailureDispositionAfterLease("transient", 1, token, leaseExpires, now)
	if state != "retry" || !retry.After(leaseExpires) {
		t.Fatalf("live lease retry state=%q retry=%s lease_expires=%s", state, retry, leaseExpires)
	}
	if retry.Sub(leaseExpires) < 10*time.Second || retry.Sub(leaseExpires) > 20*time.Second {
		t.Fatalf("live lease retry lost bounded backoff: %s", retry.Sub(leaseExpires))
	}
	state, retry = joinedFailureDispositionAfterLease("deterministic", 1, token, leaseExpires, now)
	if state != "terminal" || !retry.IsZero() {
		t.Fatalf("terminal failure gained retry state=%q retry=%s", state, retry)
	}
}
