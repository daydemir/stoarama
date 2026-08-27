package joinedrecording

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestJoinedLifecycleRequestWireContracts(t *testing.T) {
	frozen, err := NewWorkScopeIdentity("tier1-generation-1", WorkScopeFrozenBatch, nil)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		got  any
		want string
	}{
		{"bootstrap", WorkerBootstrapRequest{ProtocolVersion: 1, BatchID: "tier1-generation-1", WorkScopeIdentity: frozen}, `{"protocol_version":1,"batch_id":"tier1-generation-1","work_scope":"frozen_batch"}`},
		{"claim", WorkClaimRequest{ProtocolVersion: 1, BatchID: "tier1-generation-1", WorkerID: "joined-worker-01"}, `{"protocol_version":1,"batch_id":"tier1-generation-1","worker_id":"joined-worker-01"}`},
		{"publication claim", PublicationClaimRequest{ProtocolVersion: 1, BatchID: "tier1-generation-1", WorkerID: "joined-worker-01"}, `{"protocol_version":1,"batch_id":"tier1-generation-1","worker_id":"joined-worker-01"}`},
		{"heartbeat", HeartbeatRequest{ProtocolVersion: 1, ScopeKind: "hour", ScopeID: "canonical-hour"}, `{"protocol_version":1,"scope_kind":"hour","scope_id":"canonical-hour"}`},
		{"source capability", SourceCapabilityRequest{ProtocolVersion: 1, HourID: "canonical-hour", ClipID: 7, Operation: "get"}, `{"protocol_version":1,"hour_id":"canonical-hour","clip_id":7,"operation":"get"}`},
		{"artifact capability", ArtifactCapabilityRequest{ProtocolVersion: 1, ScopeKind: "ledger", ScopeID: "canonical-ledger", ArtifactID: 9, Operation: "put"}, `{"protocol_version":1,"scope_kind":"ledger","scope_id":"canonical-ledger","artifact_id":9,"operation":"put"}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			encoded, err := json.Marshal(tc.got)
			if err != nil || string(encoded) != tc.want {
				t.Fatalf("wire=%s want=%s err=%v", encoded, tc.want, err)
			}
		})
	}
}

func TestBroadClaimRequiresACompleteCapacityPair(t *testing.T) {
	base := WorkClaimRequest{ProtocolVersion: 1, BatchID: "tier1-generation-1", WorkerID: "worker-1"}
	if err := base.Validate(); err != nil {
		t.Fatalf("legacy bounded claim: %v", err)
	}
	base.ScratchAvailableBytes = 2 << 30
	if err := base.Validate(); err == nil {
		t.Fatal("claim accepted half of a capacity report")
	}
	base.TaskBudgetBytes = 1536 << 20
	if err := base.Validate(); err != nil {
		t.Fatalf("complete capacity report: %v", err)
	}
	want := int64(2*537405614 + 256<<20)
	if got, err := RequiredScratchBudgetBytes(537405614); err != nil || got != want {
		t.Fatalf("required scratch=%d want=%d err=%v", got, want, err)
	}
}

func TestFailureAndLeaseStatusContractsRejectUnboundedInput(t *testing.T) {
	failure := WorkFailureRequest{ProtocolVersion: 1, ScopeKind: "hour", ScopeID: "hour-1", FailureClass: "resource", ReasonCode: "scratch_exhausted"}
	if err := failure.Validate(); err != nil {
		t.Fatalf("valid failure: %v", err)
	}
	for _, class := range []string{"", "fatal", "RESOURCE"} {
		failure.FailureClass = class
		if failure.Validate() == nil {
			t.Fatalf("accepted failure class %q", class)
		}
	}
	lease := LeaseStatusRequest{ProtocolVersion: 1, BatchID: "tier1-generation-1", LeaseIDs: []string{strings.Repeat("l", 43)}}
	if err := lease.Validate(); err != nil {
		t.Fatalf("valid lease proof: %v", err)
	}
	lease.LeaseIDs = append(lease.LeaseIDs, lease.LeaseIDs[0])
	if lease.Validate() == nil {
		t.Fatal("accepted duplicate lease proof request")
	}
}

func TestClaimAdmissionContract(t *testing.T) {
	req := ClaimAdmissionRequest{ProtocolVersion: JoinedProtocolVersion, BatchID: "tier1-generation-1", ClaimsPaused: true}
	if err := req.Validate(); err != nil {
		t.Fatalf("valid request rejected: %v", err)
	}
	bad := req
	bad.BatchID = "BAD"
	if err := bad.Validate(); err == nil {
		t.Fatal("invalid request accepted")
	}
	status := ClaimAdmissionStatus{ProtocolVersion: JoinedProtocolVersion, BatchID: req.BatchID, ClaimsPaused: true,
		ActiveHourLeases: 2, ActivePublicationLeases: 1, ActiveLeaseCount: 3, UpdatedAt: time.Now().UTC()}
	if err := status.Validate(); err != nil {
		t.Fatalf("valid status rejected: %v", err)
	}
	status.ActiveLeaseCount = 2
	if err := status.Validate(); err == nil {
		t.Fatal("inconsistent active lease total accepted")
	}
}

func TestClaimAdmissionOneShotContract(t *testing.T) {
	digest := strings.Repeat("a", 64)
	req := ClaimAdmissionRequest{ProtocolVersion: JoinedProtocolVersion, BatchID: "tier1-generation-1",
		ClaimsPaused: true, ExpectedActiveClaimsSHA256: digest, MaxNewClaims: 1}
	if err := req.Validate(); err != nil {
		t.Fatalf("valid one-shot request rejected: %v", err)
	}
	for _, mutate := range []func(*ClaimAdmissionRequest){
		func(r *ClaimAdmissionRequest) { r.ClaimsPaused = false },
		func(r *ClaimAdmissionRequest) { r.MaxNewClaims = 2 },
		func(r *ClaimAdmissionRequest) { r.ExpectedActiveClaimsSHA256 = "" },
	} {
		bad := req
		mutate(&bad)
		if err := bad.Validate(); err == nil {
			t.Fatalf("invalid one-shot request accepted: %+v", bad)
		}
	}
	status := ClaimAdmissionStatus{ProtocolVersion: JoinedProtocolVersion, BatchID: req.BatchID, ClaimsPaused: true,
		ActiveLeaseCount: 0, ActiveClaimsSHA256: digest, UpdatedAt: time.Now().UTC()}
	if err := status.Validate(); err != nil {
		t.Fatalf("valid digest status rejected: %v", err)
	}
}

func TestWorkScopeIdentityBindsExactCanaryAndFrozenBatch(t *testing.T) {
	const batchID = "tier1-generation-1"
	hours := []string{
		batchID + "__recording-1__date-2026-08-01__hour-01__generation-1",
		batchID + "__recording-2__date-2026-08-01__hour-02__generation-1",
		batchID + "__recording-3__date-2026-08-01__hour-03__generation-1",
	}
	canary, err := NewWorkScopeIdentity(batchID, WorkScopeCanary, hours)
	if err != nil || canary.CanaryHourIDsSHA256 == "" || !canary.Equal(canary) {
		t.Fatalf("canary=%+v err=%v", canary, err)
	}
	reordered := canary
	reordered.CanaryHourIDs = append([]string(nil), canary.CanaryHourIDs...)
	reordered.CanaryHourIDs[0], reordered.CanaryHourIDs[1] = reordered.CanaryHourIDs[1], reordered.CanaryHourIDs[0]
	if reordered.Validate(batchID) == nil || canary.Equal(reordered) {
		t.Fatal("canary order drift preserved authority")
	}
	tampered := canary
	tampered.CanaryHourIDsSHA256 = strings.Repeat("0", 64)
	if tampered.Validate(batchID) == nil {
		t.Fatal("canary digest drift preserved authority")
	}
	for _, count := range []int{0, 1, 2, 4} {
		candidate := append([]string(nil), hours...)
		if count <= len(candidate) {
			candidate = candidate[:count]
		} else {
			candidate = append(candidate, batchID+"__recording-4__date-2026-08-01__hour-04__generation-1")
		}
		if _, err := NewWorkScopeIdentity(batchID, WorkScopeCanary, candidate); err == nil {
			t.Fatalf("canary accepted %d hours", count)
		}
	}
	frozen, err := NewWorkScopeIdentity(batchID, WorkScopeFrozenBatch, nil)
	if err != nil || !frozen.Equal(WorkScopeIdentity{WorkScope: WorkScopeFrozenBatch}) {
		t.Fatalf("frozen=%+v err=%v", frozen, err)
	}
	for _, scope := range []string{"", "disabled", "rolling"} {
		if _, err := NewWorkScopeIdentity(batchID, scope, nil); err == nil {
			t.Fatalf("unsafe work scope %q accepted", scope)
		}
	}
	single, err := NewWorkScopeIdentity(batchID, WorkScopeSingleCanary, hours[:1])
	if err != nil || single.CanaryHourIDsSHA256 == "" {
		t.Fatalf("single-canary=%+v err=%v", single, err)
	}
	for _, candidate := range [][]string{nil, hours[:2]} {
		if _, err := NewWorkScopeIdentity(batchID, WorkScopeSingleCanary, candidate); err == nil {
			t.Fatalf("single-canary accepted %d hours", len(candidate))
		}
	}
	allowlistHours := make([]string, 50)
	for i := range allowlistHours {
		allowlistHours[i] = fmt.Sprintf("%s__recording-%d__date-2026-08-01__hour-01__generation-1", batchID, i+1)
	}
	allowlist, err := NewWorkScopeIdentity(batchID, WorkScopeAllowlist50, allowlistHours)
	if err != nil || !allowlist.Equal(allowlist) || allowlist.CanaryHourIDsSHA256 == "" {
		t.Fatalf("allowlist=%+v err=%v", allowlist, err)
	}
	for _, candidate := range [][]string{allowlistHours[:49], append(append([]string(nil), allowlistHours...), allowlistHours[0])} {
		if _, err := NewWorkScopeIdentity(batchID, WorkScopeAllowlist50, candidate); err == nil {
			t.Fatalf("allowlist accepted %d hours", len(candidate))
		}
	}
	duplicate := append([]string(nil), allowlistHours...)
	duplicate[49] = duplicate[0]
	if _, err := NewWorkScopeIdentity(batchID, WorkScopeAllowlist50, duplicate); err == nil {
		t.Fatal("allowlist accepted a duplicate hour")
	}
}

func TestGapOnlyHourSkipsPreflightAndUsesPublicationClaim(t *testing.T) {
	start := time.Date(2026, time.May, 4, 8, 0, 0, 0, time.UTC)
	req := testRequest([]SourceClip{testSource(1, start)})
	req.Sources = nil
	ledger, err := testLedger(req, req.LocalDate)
	if err != nil {
		t.Fatal(err)
	}
	req.AllocationLedgerSHA = ledger.LedgerSHA256
	plan, err := BuildGapOnlyHourPlan(req, req.LocalDate, req.DeliveryHour, "no_source_clips")
	if err != nil {
		t.Fatal(err)
	}
	if err := (PreflightHourClaim{}).Validate(time.Now()); err == nil {
		t.Fatal("empty hour received a media-preflight claim")
	}
	hour := sealedClaim(t, 1, plan, nil, nil)
	if _, err := PublishClaimedHourRenewing(context.Background(), nil, "other.test", hour, SealedHourScratch{}, nil, nil, nil, nil); err == nil {
		t.Fatal("hour publication trusted claim-supplied storage authority")
	}
	response := PublicationClaimResponse{ProtocolVersion: JoinedProtocolVersion, Kind: "hour", Hour: &hour}
	if err := response.Validate(time.Now()); err != nil {
		t.Fatalf("server-sealed gap-only publication was rejected: %v", err)
	}
	response.Kind = "ledger"
	if err := response.Validate(time.Now()); err == nil {
		t.Fatal("publication discriminant did not bind its typed claim")
	}
}

func TestJoinedLifecycleRejectsVersionZeroAndRequestLeaseID(t *testing.T) {
	frozen, err := NewWorkScopeIdentity("tier1-generation-1", WorkScopeFrozenBatch, nil)
	if err != nil {
		t.Fatal(err)
	}
	if (WorkerBootstrapRequest{ProtocolVersion: 0, BatchID: "tier1-generation-1", WorkScopeIdentity: frozen}).Validate() == nil {
		t.Fatal("dormant protocol claimed work")
	}
	if (WorkClaimRequest{ProtocolVersion: 1, BatchID: "tier1-generation-1", WorkerID: strings.Repeat("w", 257)}).Validate() == nil {
		t.Fatal("unbounded worker identity accepted")
	}
	encoded, _ := json.Marshal(HeartbeatRequest{ProtocolVersion: 1, ScopeKind: "hour", ScopeID: "canonical-hour"})
	if strings.Contains(string(encoded), "lease_id") || strings.Contains(string(encoded), "operation_token") {
		t.Fatal("response-only lease credentials leaked into request DTO")
	}
	response := HeartbeatResponse{ProtocolVersion: 1, ScopeKind: "hour", ScopeID: "canonical-hour", LeaseID: strings.Repeat("L", 43), OperationToken: strings.Repeat("t", 32), ExpiresAt: time.Now().Add(time.Minute)}
	if err := response.Validate(time.Now()); err != nil {
		t.Fatal(err)
	}
}

func TestJoinedFinalizeWireIsBearerOnlyAndTyped(t *testing.T) {
	requests := []any{
		FinalizeLedgerRequest{ProtocolVersion: JoinedProtocolVersion, Published: PublishedLedger{}},
		FinalizeHourRequest{ProtocolVersion: JoinedProtocolVersion, Published: PublishedHour{}},
		FinalizeBatchIndexRequest{ProtocolVersion: JoinedProtocolVersion, Published: PublishedBatchIndex{}},
	}
	for _, request := range requests {
		encoded, err := json.Marshal(request)
		if err != nil {
			t.Fatal(err)
		}
		var top map[string]json.RawMessage
		if err := json.Unmarshal(encoded, &top); err != nil || len(top) != 2 || top["protocol_version"] == nil || top["published"] == nil || strings.Contains(string(encoded), "operation_token") || strings.Contains(string(encoded), "lease_id") {
			t.Fatalf("finalize wire carries ambiguous credentials or fields: %s", encoded)
		}
	}
}
