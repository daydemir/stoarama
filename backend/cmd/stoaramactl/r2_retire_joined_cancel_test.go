package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestR2JoinedWriterCancelRequiresBoundApplyAndDefaultsDryRun(t *testing.T) {
	o, err := parseR2JoinedCancelArgs([]string{"--batch-record-id", "5", "--out", "new.jsonl"})
	if err != nil || o.apply {
		t.Fatalf("defaults %+v %v", o, err)
	}
	for _, args := range [][]string{{"--batch-record-id", "5", "--out", "new", "--apply"}, {"--batch-record-id", "5", "--out", "new", "--owner-map", "x"}, {"--batch-record-id", "0", "--out", "new"}, {"--batch-record-id", "5", "--out", "new", "extra"}} {
		if _, err := parseR2JoinedCancelArgs(args); err == nil {
			t.Fatalf("incomplete authority accepted %v", args)
		}
	}
}

func TestR2JoinedOwnerStopNeedsExactHistoricalIdentityAndFreshPrimaryProof(t *testing.T) {
	path := filepath.Join(t.TempDir(), "historical-primary-resource.json")
	body := []byte(`{"account_uuid":"original-primary-account","droplet":{"id":123,"name":"stoarama-joined-test"}}`)
	if err := os.WriteFile(path, body, 0600); err != nil {
		t.Fatal(err)
	}
	sha := sha256.Sum256(body)
	mapping := r2JoinedOwnerMapping{Owner: "stoarama-joined-test", Kind: r2JoinedDODestroyed, ResourceID: "123", HistoricalResourceFile: path, HistoricalResourceSHA256: hex.EncodeToString(sha[:])}
	owners := []string{mapping.Owner}
	status := http.StatusNotFound
	calls := 0
	read := func(_ context.Context, url string, _ string) (int, []byte, error) {
		calls++
		if strings.HasSuffix(url, "/account") {
			return http.StatusOK, []byte(`{"account":{"uuid":"original-primary-account"}}`), nil
		}
		return status, nil, nil
	}
	evidence, err := r2JoinedVerifyOwners(context.Background(), []r2JoinedOwnerMapping{mapping}, owners, read, "", "local-token-never-recorded")
	if err != nil || len(evidence) != 1 || evidence[0].ObservedAt.IsZero() || len(evidence[0].EvidenceSHA256) != 64 {
		t.Fatalf("proof %v %v", evidence, err)
	}
	b, _ := json.Marshal(evidence)
	if strings.Contains(string(b), "local-token") {
		t.Fatal("credential entered audit")
	}
	for _, code := range []int{200, 401, 403, 429, 500} {
		status = code
		if _, err := r2JoinedVerifyOwners(context.Background(), []r2JoinedOwnerMapping{mapping}, owners, read, "", "key"); err == nil {
			t.Fatalf("provider status%d became absence", code)
		}
	}
	status = 404
	wrong := mapping
	wrong.ResourceID = "124"
	prior := calls
	if _, err := r2JoinedVerifyOwners(context.Background(), []r2JoinedOwnerMapping{wrong}, owners, read, "", "key"); err == nil || calls != prior {
		t.Fatal("unbound resource reached provider")
	}
	wrong = mapping
	wrong.Owner = "another-owner"
	if _, err := r2JoinedVerifyOwners(context.Background(), []r2JoinedOwnerMapping{wrong}, owners, read, "", "key"); err == nil {
		t.Fatal("owner roster drift accepted")
	}
	wrong = mapping
	wrong.HistoricalResourceSHA256 = strings.Repeat("0", 64)
	if _, err := r2JoinedVerifyOwners(context.Background(), []r2JoinedOwnerMapping{wrong}, owners, read, "", "key"); err == nil {
		t.Fatal("changed primary association accepted")
	}
	if _, err := r2JoinedVerifyOwners(context.Background(), []r2JoinedOwnerMapping{mapping, mapping}, owners, read, "", "key"); err == nil {
		t.Fatal("duplicate mapping accepted")
	}
}
