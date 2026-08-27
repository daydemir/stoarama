package joinedrecording

import "testing"

func TestWorkScopeIdentitySHA256BindsExactScope(t *testing.T) {
	const batchID = "tier1-generation-1"
	hours := []string{
		batchID + "__recording-1__date-2026-08-01__hour-01__generation-1",
		batchID + "__recording-2__date-2026-08-01__hour-01__generation-1",
		batchID + "__recording-3__date-2026-08-01__hour-01__generation-1",
	}
	canary, err := NewWorkScopeIdentity(batchID, WorkScopeCanary, hours)
	if err != nil {
		t.Fatal(err)
	}
	canarySHA, err := canary.SHA256(batchID)
	if err != nil {
		t.Fatal(err)
	}
	if canarySHA != "3910a7c8b57cf715556dcc31267257d00fa704204f95801ebbb6cb30d599dd70" {
		t.Fatalf("canary scope sha=%s", canarySHA)
	}
	reordered, err := NewWorkScopeIdentity(batchID, WorkScopeCanary, []string{hours[1], hours[0], hours[2]})
	if err != nil {
		t.Fatal(err)
	}
	reorderedSHA, err := reordered.SHA256(batchID)
	if err != nil {
		t.Fatal(err)
	}
	if reorderedSHA == canarySHA {
		t.Fatal("canary scope digest ignored order")
	}
	single, err := NewWorkScopeIdentity(batchID, WorkScopeSingleCanary, hours[:1])
	if err != nil {
		t.Fatal(err)
	}
	singleSHA, err := single.SHA256(batchID)
	if err != nil {
		t.Fatal(err)
	}
	frozen, err := NewWorkScopeIdentity(batchID, WorkScopeFrozenBatch, nil)
	if err != nil {
		t.Fatal(err)
	}
	frozenSHA, err := frozen.SHA256(batchID)
	if err != nil {
		t.Fatal(err)
	}
	if singleSHA == canarySHA || frozenSHA == canarySHA || frozenSHA == singleSHA {
		t.Fatal("different joined work scopes share a digest")
	}
	if _, err := (WorkScopeIdentity{WorkScope: WorkScopeFrozenBatch, CanaryHourIDs: hours}).SHA256(batchID); err == nil {
		t.Fatal("invalid joined work scope produced a digest")
	}
}
