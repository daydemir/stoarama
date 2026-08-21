package joinedauth

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestJoinedWorkerTokenScopeAndExpiry(t *testing.T) {
	now := time.Unix(2_000_000_000, 0).UTC()
	claimToken, err := MintClaim("signing-key", "batch-test", now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	claimScope, err := Verify("signing-key", claimToken, now)
	if err != nil || claimScope.Kind != KindClaim || claimScope.BatchID != "batch-test" || claimScope.SubjectID != "" {
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

func TestLeaseIDIsStablePathSafeAndNotFenceToken(t *testing.T) {
	lease := uuid.MustParse("123e4567-e89b-42d3-a456-426614174000")
	got := LeaseID(lease)
	if got == lease.String() || len(got) != 43 || strings.ContainsAny(got, "/=+") || got != LeaseID(lease) || got == LeaseID(uuid.New()) {
		t.Fatalf("unsafe lease_id %q for fence %q", got, lease)
	}
}

func TestJoinedWorkerTokenRejectsUnsafeBatchID(t *testing.T) {
	for _, batchID := range []string{"", " batch", "batch/name", strings.Repeat("a", 129)} {
		if _, err := MintClaim("signing-key", batchID, time.Unix(2_000_000_000, 0).UTC()); err == nil {
			t.Fatalf("unsafe batch ID minted a token: %q", batchID)
		}
	}
}
