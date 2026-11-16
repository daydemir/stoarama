package api

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

func syntheticNASStitchReaderStatus(t *testing.T) (nasStitchReaderStatus, nasStitchReadbackSnapshot, nasStitchReadbackReceipt, string) {
	t.Helper()
	receipt, facts, now := syntheticNASStitchReadback(t)
	raw, _ := json.Marshal(receipt)
	sha := cleanupSHA(raw)
	status := nasStitchReaderStatus{Schema: "stoarama.nas-stitch-reader-status.v1", Reader: receipt.Reader, Integrity: "clear",
		History: []nasStitchReadbackSummary{{ID: receipt.ID, VerifiedAt: receipt.VerifiedAt, SHA: sha}},
		Native: &nasStitchNativeStatus{RunID: receipt.NativeRunID, BundleSHA: receipt.NativeBundleSHA, PlanSHA: receipt.NativePlanSHA,
			TaskSHA: strings.Repeat("1", 64), ResultSHA: strings.Repeat("2", 64), ProducerSHA: receipt.ProducerProfileSHA,
			ReaderSHA: receipt.ReaderProfileSHA, PairSHA: receipt.PairSHA, RegistrationSHA: receipt.NativeRegistration,
			Outputs: receipt.Outputs, Policy: receipt.Policy, Tools: receipt.Tools}}
	return status, nasStitchReadbackSnapshot{Now: now, ApprovedUntil: now.Add(time.Hour), Version: receipt.Reader.Version, BootID: receipt.Reader.BootID, Facts: facts}, receipt, sha
}

func TestNASStitchReadbackStatusBindsActualCurrentIdentityAndHistory(t *testing.T) {
	status, snapshot, receipt, sha := syntheticNASStitchReaderStatus(t)
	raw, _ := json.Marshal(status)
	value, err := decodeNASStitchReaderStatus(raw, snapshot)
	if err != nil || matchNASStitchReadbackStatus(receipt, sha, value) != nil {
		t.Fatalf("synthetic protocol binding refused: %v", err)
	}
	for _, mutate := range []func(*nasStitchReaderStatus){
		func(v *nasStitchReaderStatus) { v.Integrity = "refused" },
		func(v *nasStitchReaderStatus) { v.Reader.Version = "development" },
		func(v *nasStitchReaderStatus) { v.Reader.BootID = "22222222-2222-4222-8222-222222222222" },
		func(v *nasStitchReaderStatus) { v.Native = nil },
		func(v *nasStitchReaderStatus) { v.History = append(v.History, v.History[0]) },
		func(v *nasStitchReaderStatus) { v.Native.BundleSHA = "unknown" },
		func(v *nasStitchReaderStatus) { v.Native.Tools.FFprobe.SHA256 = strings.Repeat("0", 64) },
	} {
		var changed nasStitchReaderStatus
		json.Unmarshal(raw, &changed)
		mutate(&changed)
		body, _ := json.Marshal(changed)
		if _, err := decodeNASStitchReaderStatus(body, snapshot); err == nil {
			t.Fatal("changed identity/integrity/history admitted")
		}
	}
	for _, mutate := range []func(*nasStitchReaderStatus){
		func(v *nasStitchReaderStatus) { v.Native.Outputs[0].SHA256 = strings.Repeat("0", 64) },
		func(v *nasStitchReaderStatus) { v.Native.BundleSHA = strings.Repeat("0", 64) },
		func(v *nasStitchReaderStatus) { v.History[0].SHA = strings.Repeat("0", 64) },
		func(v *nasStitchReaderStatus) { v.Reader.ArtifactSize++ },
	} {
		var changed nasStitchReaderStatus
		json.Unmarshal(raw, &changed)
		mutate(&changed)
		if matchNASStitchReadbackStatus(receipt, sha, changed) == nil {
			t.Fatal("different receipt/current status relationship accepted")
		}
	}
	if _, err := acceptedNASStitchReadback(receipt); err == nil {
		t.Fatal("synthetic fixture manufactured registry authority")
	}
	status.Native = nil
	status.History = []nasStitchReadbackSummary{}
	raw, _ = json.Marshal(status)
	if _, err := decodeNASStitchReaderStatus(raw, snapshot); err != nil {
		t.Fatalf("startup identity without native evidence refused: %v", err)
	}
	status.Reader.Host.CpuCount = 0
	snapshot.Facts.Host.CPUCount = 0
	raw, _ = json.Marshal(status)
	if _, err := decodeNASStitchReaderStatus(raw, snapshot); err == nil {
		t.Fatal("mutually matching unknown CPU identity admitted")
	}
}

func TestNASStitchReadbackAdmissionQueriesAndPullScopeAreClosed(t *testing.T) {
	const id = "11111111-1111-4111-8111-111111111111"
	path := "/api/v1/account/connections/stitch/evidence/admission"
	for _, c := range []struct {
		query string
		mode  nasStitchProofMode
		ok    bool
	}{
		{"run_id=" + id, nasStitchProofNative, true},
		{"producer_profile_sha256=" + strings.Repeat("a", 64), nasStitchProofReadback, true},
		{"run_id=" + id, nasStitchProofReadback, false},
		{"run_id=" + id, nasStitchProofDisabled, false},
		{"run_id=" + id + "&run_id=" + id, nasStitchProofNative, false},
		{"run_id=" + id + "&other=1", nasStitchProofNative, false},
		{"run_id=00000000-0000-0000-0000-000000000000", nasStitchProofNative, false},
		{"run_id=%zz", nasStitchProofNative, false},
	} {
		r := httptest.NewRequest(http.MethodGet, path+"?"+c.query, nil)
		_, err := parseNASStitchAdmissionQuery(r, c.mode)
		if (err == nil) != c.ok {
			t.Fatalf("mode/query admitted incorrectly: %d %s %v", c.mode, c.query, err)
		}
	}
	if !pullPathAllowed(http.MethodGet, path) {
		t.Fatal("scoped metadata GET blocked")
	}
	for _, p := range []string{path + "/", path + "x", "/api/v1/service/nas-stitch/readback/status"} {
		if pullPathAllowed(http.MethodGet, p) {
			t.Fatal("neighbor/service route exposed to pull credential")
		}
	}
	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete} {
		if pullPathAllowed(method, path) {
			t.Fatal("metadata route admits mutation")
		}
	}
}

func TestNASStitchReadbackComponentsAreExactImmutableBytes(t *testing.T) {
	raw := []byte(`{"SYNTHETIC":true}`)
	encoded := base64.StdEncoding.EncodeToString(raw)
	sha := cleanupSHA(raw)
	if got, err := decodeNASStitchReadbackComponent(encoded, sha, len(raw)); err != nil || string(got) != string(raw) {
		t.Fatal("exact byte component refused")
	}
	for _, c := range []struct {
		body, sha string
		limit     int
	}{
		{encoded + "\n", sha, len(raw)}, {encoded, strings.Repeat("0", 64), len(raw)},
		{encoded, sha, len(raw) - 1}, {"", sha, len(raw)}, {encoded[:len(encoded)-1], sha, len(raw)},
	} {
		if _, err := decodeNASStitchReadbackComponent(c.body, c.sha, c.limit); err == nil {
			t.Fatal("noncanonical, changed or oversized component admitted")
		}
	}
}

func TestNASStitchReadbackServerIdentityHasNoDeploymentFallback(t *testing.T) {
	for key, value := range map[string]string{"RENDER_GIT_REPO_SLUG": "daydemir/stoarama", "RENDER_GIT_COMMIT": strings.Repeat("a", 40), "RENDER_SERVICE_ID": nasStitchReadbackServiceID, "RENDER_INSTANCE_ID": "instance-SYNTHETIC"} {
		t.Setenv(key, value)
	}
	value, err := nasStitchReadbackServerExecution()
	if err != nil || value.InstanceID != "instance-SYNTHETIC" {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(value)
	if strings.Contains(string(raw), "deployment") {
		t.Fatal("instance aliased as deployment")
	}
	for _, key := range []string{"RENDER_GIT_REPO_SLUG", "RENDER_GIT_COMMIT", "RENDER_SERVICE_ID", "RENDER_INSTANCE_ID"} {
		original := map[string]string{"RENDER_GIT_REPO_SLUG": "daydemir/stoarama", "RENDER_GIT_COMMIT": strings.Repeat("a", 40), "RENDER_SERVICE_ID": nasStitchReadbackServiceID, "RENDER_INSTANCE_ID": "instance-SYNTHETIC"}[key]
		t.Setenv(key, "")
		if _, err := nasStitchReadbackServerExecution(); err == nil {
			t.Fatal("missing runtime identity received a default")
		}
		t.Setenv(key, original)
	}
}

func TestNASStitchReadbackServerResponsesMatchAgreedClosedFieldNames(t *testing.T) {
	_, snapshot, receipt, sha := syntheticNASStitchReaderStatus(t)
	server := nasStitchServerExecution{Repository: "daydemir/stoarama", Commit: strings.Repeat("a", 40), ServiceID: nasStitchReadbackServiceID, InstanceID: "instance-SYNTHETIC"}
	binding := bindNASStitchReadback(receipt, sha, snapshot, server)
	values := map[string]any{
		"validation":      nasStitchReadbackValidation{Schema: "stoarama.nas-stitch-validation.v2", ValidatedAt: snapshot.Now.Format(time.RFC3339Nano), nasStitchReadbackBinding: binding},
		"current_context": nasStitchReadbackCurrent{Schema: "stoarama.nas-stitch-current-context.v1", CheckedAt: snapshot.Now.Format(time.RFC3339Nano), StatusAt: snapshot.Now.Format(time.RFC3339Nano), VerifiedAt: receipt.VerifiedAt, ID: receipt.ID, ApprovedUntil: snapshot.ApprovedUntil.Format(time.RFC3339Nano), Integrity: "clear", nasStitchReadbackBinding: binding},
	}
	file, err := os.ReadFile("../../../clients/nas-pull/stitch-proof/attestation-v2.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	var schema map[string]any
	if json.Unmarshal(file, &schema) != nil {
		t.Fatal("agreed schema unreadable")
	}
	for name, value := range values {
		raw, _ := json.Marshal(value)
		var wire map[string]any
		json.Unmarshal(raw, &wire)
		definition := schema["$defs"].(map[string]any)[name].(map[string]any)
		properties := definition["properties"].(map[string]any)
		if len(wire) != len(properties) {
			t.Fatalf("response closed shape differs for %s: %s", name, raw)
		}
		for _, required := range definition["required"].([]any) {
			if _, ok := wire[required.(string)]; !ok {
				t.Fatalf("missing agreed field %s/%s", name, required)
			}
		}
		for field, property := range properties {
			if constant, ok := property.(map[string]any)["const"]; ok && !reflect.DeepEqual(wire[field], constant) {
				t.Fatalf("agreed const differs %s/%s", name, field)
			}
		}
		if wire["reader_inventory_mismatch_count"] != float64(snapshot.Facts.InventoryMismatches.Int64) {
			t.Fatal("actual inventory counter was not bound")
		}
		if _, ok := wire["reader_version_mismatch_count"]; ok {
			t.Fatal("invented version counter alias")
		}
	}
}
