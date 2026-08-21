package joinedrecording

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestJoinedLifecycleRequestWireContracts(t *testing.T) {
	tests := []struct {
		name string
		got  any
		want string
	}{
		{"bootstrap", WorkerBootstrapRequest{ProtocolVersion: 1, BatchID: "tier1-generation-1"}, `{"protocol_version":1,"batch_id":"tier1-generation-1"}`},
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
	if (WorkerBootstrapRequest{ProtocolVersion: 0, BatchID: "tier1-generation-1"}).Validate() == nil {
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
