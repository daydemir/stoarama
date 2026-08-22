package joinedauth

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/joinedrecording"
	"github.com/google/uuid"
)

func testClaimWorkScope(t *testing.T, batchID string) joinedrecording.WorkScopeIdentity {
	t.Helper()
	scope, err := joinedrecording.NewWorkScopeIdentity(batchID, joinedrecording.WorkScopeFrozenBatch, nil)
	if err != nil {
		t.Fatal(err)
	}
	return scope
}

func TestJoinedWorkerTokenScopeAndExpiry(t *testing.T) {
	now := time.Unix(2_000_000_000, 0).UTC()
	claimToken, err := MintClaim("signing-key", "batch-test", testClaimWorkScope(t, "batch-test"), now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	claimScope, err := Verify("signing-key", claimToken, now)
	if err != nil || claimScope.Kind != KindClaim || claimScope.BatchID != "batch-test" || claimScope.SubjectID != "" ||
		!claimScope.WorkScopeIdentity.Equal(testClaimWorkScope(t, "batch-test")) {
		t.Fatalf("claim scope=%+v err=%v", claimScope, err)
	}
	claim := uuid.New()
	token, err := MintOperation("signing-key", "batch-test", SubjectHour, "hour-test", claim, OperationPreflight, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	got, err := Verify("signing-key", token, now)
	if err != nil || got.BatchID != "batch-test" || got.SubjectKind != SubjectHour || got.SubjectID != "hour-test" ||
		got.LeaseToken != claim.String() || got.Operation != OperationPreflight {
		t.Fatalf("claims=%+v err=%v", got, err)
	}
	refreshed, err := MintOperation("signing-key", "batch-test", SubjectHour, "hour-test", claim, OperationPreflight, now.Add(2*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Verify("signing-key", token, now.Add(90*time.Second)); err == nil {
		t.Fatal("old operation token survived its fake-clock expiry")
	}
	refreshedScope, err := Verify("signing-key", refreshed, now.Add(90*time.Second))
	if err != nil || refreshedScope.LeaseToken != claim.String() {
		t.Fatalf("refreshed token lost stable lease: claims=%+v err=%v", refreshedScope, err)
	}
	for _, tc := range []struct {
		name, key, token string
		now              time.Time
	}{
		{name: "missing", key: "signing-key", now: now},
		{name: "invalid signature", key: "other-key", token: token, now: now},
		{name: "expired", key: "signing-key", token: token, now: now.Add(2 * time.Minute)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Verify(tc.key, tc.token, tc.now); err == nil {
				t.Fatal("unsafe joined worker token accepted")
			}
		})
	}
	parts := strings.Split(token, ".")
	payload, _ := base64.RawURLEncoding.DecodeString(parts[0])
	var claims Claims
	_ = json.Unmarshal(payload, &claims)
	claims.Audience = "another-service"
	payload, _ = json.Marshal(claims)
	encoded := base64.RawURLEncoding.EncodeToString(payload)
	wrongAudience := encoded + "." + signature("signing-key", encoded)
	if _, err := Verify("signing-key", wrongAudience, now); err == nil {
		t.Fatal("wrong-audience joined worker token accepted")
	}
}

func TestJoinedClaimTokenBindsExactCanaryIdentity(t *testing.T) {
	const batchID = "tier1-generation-1"
	hours := []string{
		batchID + "__recording-1__date-2026-08-01__hour-01__generation-1",
		batchID + "__recording-2__date-2026-08-01__hour-02__generation-1",
		batchID + "__recording-3__date-2026-08-01__hour-03__generation-1",
	}
	scope, err := joinedrecording.NewWorkScopeIdentity(batchID, joinedrecording.WorkScopeCanary, hours)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(2_000_000_000, 0).UTC()
	token, err := MintClaim("signing-key", batchID, scope, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	got, err := Verify("signing-key", token, now)
	if err != nil || !got.WorkScopeIdentity.Equal(scope) {
		t.Fatalf("claims=%+v err=%v", got, err)
	}
	parts := strings.Split(token, ".")
	payload, _ := base64.RawURLEncoding.DecodeString(parts[0])
	var claims Claims
	if err := json.Unmarshal(payload, &claims); err != nil {
		t.Fatal(err)
	}
	claims.CanaryHourIDsSHA256 = strings.Repeat("0", 64)
	payload, _ = json.Marshal(claims)
	encoded := base64.RawURLEncoding.EncodeToString(payload)
	if _, err := Verify("signing-key", encoded+"."+signature("signing-key", encoded), now); err == nil {
		t.Fatal("validly signed canary digest drift was accepted")
	}
	if _, err := MintClaim("signing-key", batchID, joinedrecording.WorkScopeIdentity{WorkScope: "rolling"}, now.Add(time.Minute)); err == nil {
		t.Fatal("rolling claim authority was minted")
	}
}

func TestLeaseIDIsStablePathSafeAndNotFenceToken(t *testing.T) {
	lease := uuid.MustParse("123e4567-e89b-42d3-a456-426614174000")
	got := LeaseID(lease)
	if got == lease.String() || len(got) != 43 || strings.ContainsAny(got, "/=+") || got != LeaseID(lease) || got == LeaseID(uuid.New()) {
		t.Fatalf("unsafe lease_id %q for fence %q", got, lease)
	}
}

func TestJoinedWorkerTokenRejectsUnsafeBatchID(t *testing.T) {
	now := time.Unix(2_000_000_000, 0).UTC()
	lease := uuid.New()
	for _, batchID := range []string{"", " batch", "batch/name", "Batch-test", "batch.test", "batch_test", strings.Repeat("a", 64)} {
		if _, err := MintClaim("signing-key", batchID, joinedrecording.WorkScopeIdentity{WorkScope: joinedrecording.WorkScopeFrozenBatch}, now.Add(time.Minute)); err == nil {
			t.Fatalf("unsafe batch ID minted a token: %q", batchID)
		}
		if _, err := MintOperation("signing-key", batchID, SubjectHour, "hour-test", lease, OperationPreflight, now.Add(time.Minute)); err == nil {
			t.Fatalf("unsafe batch ID minted an operation token: %q", batchID)
		}
		forged, err := mint("signing-key", Claims{Version: 1, Audience: Audience, ExpiresAt: now.Add(time.Minute).Unix(),
			Kind: KindOperation, BatchID: batchID, SubjectKind: SubjectHour, SubjectID: "hour-test",
			LeaseToken: lease.String(), Operation: OperationPreflight})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := Verify("signing-key", forged, now); err == nil {
			t.Fatalf("unsafe signed batch ID verified: %q", batchID)
		}
	}
}
