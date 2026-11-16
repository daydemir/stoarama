package api

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

var updateNASStitchWireFixtures = flag.Bool("update-nas-stitch-wire-fixtures", false, "regenerate synthetic actual-wire fixtures")

type nasStitchWireFixture struct {
	Name              string `json:"name"`
	DBInstant         string `json:"db_instant"`
	NativeExpiry      string `json:"native_expiry"`
	ReadbackExpiry    string `json:"readback_expiry"`
	CopiedVerifiedAt  string `json:"copied_verified_at"`
	LegacyNanoExpiry  string `json:"legacy_nano_expiry"`
	LegacyTaskB64     string `json:"legacy_task_b64"`
	NativeBodyB64     string `json:"native_body_b64"`
	ReadbackBodyB64   string `json:"readback_body_b64"`
	ValidationBodyB64 string `json:"validation_body_b64"`
	CurrentBodyB64    string `json:"current_body_b64"`
}

func syntheticNASStitchWireBody(t *testing.T, value any) []byte {
	t.Helper()
	w := httptest.NewRecorder()
	w.Header().Set("Cache-Control", "private, no-store")
	writeNASStitchWireJSON(w, value)
	if w.Code != http.StatusOK || w.Header().Get("Content-Type") != "application/json" || w.Header().Get("Cache-Control") != "private, no-store" {
		t.Fatalf("synthetic wire response refused: %d %s", w.Code, w.Body.Bytes())
	}
	return w.Body.Bytes()
}

func TestNASStitchWireTimePreservesExactDBInstants(t *testing.T) {
	for _, zone := range []*time.Location{time.UTC, time.FixedZone("synthetic-offset", 19800)} {
		for _, micros := range []int{0, 1, 100000, 120000, 123000, 123400, 123450, 123456, 999999} {
			value := time.Date(2026, 1, 1, 0, 0, 0, micros*1000, zone)
			raw, err := json.Marshal(nasStitchWireTime(value))
			if err != nil {
				t.Fatal(err)
			}
			var stamp string
			if json.Unmarshal(raw, &stamp) != nil || len(stamp) != 27 || !strings.HasSuffix(stamp, "Z") {
				t.Fatalf("not fixed UTC6: %s", raw)
			}
			decoded, err := time.Parse(time.RFC3339Nano, stamp)
			if err != nil || !decoded.Equal(value) {
				t.Fatalf("instant rounded or changed: %s %s %v", value, stamp, err)
			}
		}
	}
}

func TestNASStitchWireEncodingRefusesBeforeSuccessfulStatus(t *testing.T) {
	for _, value := range []time.Time{{}, time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 1, 1, 0, 0, 0, 123450001, time.UTC)} {
		if _, err := json.Marshal(nasStitchWireTime(value)); err == nil {
			t.Fatal("unrepresentable instant encoded")
		}
		w := httptest.NewRecorder()
		writeNASStitchWireJSON(w, struct {
			At nasStitchWireTime `json:"checked_at"`
		}{nasStitchWireTime(value)})
		if w.Code != http.StatusConflict || strings.Contains(w.Body.String(), "checked_at") {
			t.Fatal("invalid precision committed successful response bytes")
		}
	}
}

func TestNASStitchWireTaskHasOneExactExpiryAndEncoding(t *testing.T) {
	const run = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	const activation = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
	for _, clamped := range []bool{false, true} {
		for _, micros := range []int{0, 1, 100000, 120000, 123000, 123400, 123450, 123456, 999999} {
			now := time.Date(2026, 11, 16, 5, 0, 0, micros*1000, time.UTC)
			snapshot := nasStitchReadbackSnapshot{Now: now, ApprovedUntil: now.Add(time.Hour), SourcePlan: proofPlanFixture(), SourceSHA: nasStitchProofSourceSHA}
			expiry := now.Add(15 * time.Minute)
			if clamped {
				snapshot.ApprovedUntil = now.Add(45 * time.Second)
				expiry = snapshot.ApprovedUntil
			}
			value, err := buildNASStitchAdmissionResponse(snapshot, nasStitchProfileComponents{}, nasStitchProofNative, nasStitchAdmissionQuery{run: run, activation: activation})
			if err != nil {
				t.Fatal(err)
			}
			body := syntheticNASStitchWireBody(t, value)
			var wire struct {
				Task json.RawMessage `json:"task"`
				B64  string          `json:"task_b64"`
				SHA  string          `json:"task_sha256"`
				At   string          `json:"expires_at"`
			}
			if json.Unmarshal(body, &wire) != nil {
				t.Fatal("wire unavailable")
			}
			raw, err := base64.StdEncoding.DecodeString(wire.B64)
			if err != nil || !bytes.Equal(raw, wire.Task) || !bytes.Equal(raw, value.Task) || cleanupSHA(raw) != wire.SHA {
				t.Fatal("authoritative task bytes differ from parsed wire/base64/SHA")
			}
			var task nasStitchProof
			if json.Unmarshal(raw, &task) != nil || !task.Expires.Equal(expiry) || task.ActivationID != activation || value.NativeRunID != run {
				t.Fatal("task instant/identity changed")
			}
			var fields map[string]json.RawMessage
			json.Unmarshal(raw, &fields)
			var taskExpiry string
			json.Unmarshal(fields["expires_at"], &taskExpiry)
			if taskExpiry != wire.At || wire.At != expiry.Format("2006-01-02T15:04:05.000000Z") {
				t.Fatal("task/outer expiry differs or extended")
			}
		}
	}
	if _, err := buildNASStitchAdmissionResponse(nasStitchReadbackSnapshot{}, nasStitchProfileComponents{}, nasStitchProofDisabled, nasStitchAdmissionQuery{}); err == nil {
		t.Fatal("disabled mode constructed admission")
	}
}

func TestNASStitchWireReadbackExpiryIsClampedWithoutTask(t *testing.T) {
	now := time.Date(2026, 11, 16, 5, 0, 0, 123450000, time.UTC)
	for _, scope := range []time.Duration{45 * time.Second, time.Hour} {
		snapshot := nasStitchReadbackSnapshot{Now: now, ApprovedUntil: now.Add(scope)}
		value, err := buildNASStitchAdmissionResponse(snapshot, nasStitchProfileComponents{}, nasStitchProofReadback, nasStitchAdmissionQuery{})
		if err != nil {
			t.Fatal(err)
		}
		expected := now.Add(120 * time.Second)
		if snapshot.ApprovedUntil.Before(expected) {
			expected = snapshot.ApprovedUntil
		}
		if !time.Time(value.Expires).Equal(expected) || len(value.Task) != 0 || value.TaskB64 != "" || value.TaskSHA != "" || value.NativeRunID != "" || value.ActivationID != "" {
			t.Fatal("readback expiry/task contract changed")
		}
		var wire map[string]json.RawMessage
		json.Unmarshal(syntheticNASStitchWireBody(t, value), &wire)
		if len(wire) != 4 {
			t.Fatal("readback emitted native fields")
		}
	}
}

// Golden bodies are emitted by the actual production builder/writer. They are
// explicitly synthetic serializer fixtures, never registry entries/admissions.
// The Python 3.9 CI job replays these exact bytes through the unchanged parser.
func TestNASStitchWireSerializedFixtures(t *testing.T) {
	var cases []nasStitchWireFixture
	for _, micros := range []int{0, 1, 100000, 120000, 123000, 123400, 123450, 123456, 999999} {
		_, snapshot, receipt, sha := syntheticNASStitchReaderStatus(t)
		now := time.Date(2026, 11, 16, 5, 0, 0, micros*1000, time.UTC)
		snapshot.Now, snapshot.ApprovedUntil = now, now.Add(45*time.Second)
		snapshot.Facts.HeartbeatAt.Time, snapshot.Facts.StatusAt.Time, snapshot.Facts.HostAt.Time = now, now, now
		snapshot.SourcePlan, snapshot.SourceSHA = proofPlanFixture(), nasStitchProofSourceSHA
		receipt.VerifiedAt = "2026-11-16T04:59:59.123Z" // copied strings MUST NOT be padded
		receiptRaw, _ := json.Marshal(receipt)
		sha = cleanupSHA(receiptRaw)
		query := nasStitchAdmissionQuery{run: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", activation: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"}
		native, err := buildNASStitchAdmissionResponse(snapshot, nasStitchProfileComponents{}, nasStitchProofNative, query)
		if err != nil {
			t.Fatal(err)
		}
		var legacy nasStitchProof
		if err := json.Unmarshal(native.Task, &legacy); err != nil {
			t.Fatal(err)
		}
		legacyRaw, err := json.Marshal(legacy)
		if err != nil {
			t.Fatal(err)
		}
		readback, err := buildNASStitchAdmissionResponse(snapshot, nasStitchProfileComponents{}, nasStitchProofReadback, nasStitchAdmissionQuery{})
		if err != nil {
			t.Fatal(err)
		}
		server := nasStitchServerExecution{Repository: "daydemir/stoarama", Commit: strings.Repeat("a", 40), ServiceID: nasStitchReadbackServiceID, InstanceID: "instance-SYNTHETIC"}
		binding := bindNASStitchReadback(receipt, sha, snapshot, server)
		validation := nasStitchReadbackValidation{Schema: "stoarama.nas-stitch-validation.v2", ValidatedAt: nasStitchWireTime(now), nasStitchReadbackBinding: binding}
		current := nasStitchReadbackCurrent{Schema: "stoarama.nas-stitch-current-context.v1", CheckedAt: nasStitchWireTime(now), StatusAt: nasStitchWireTime(now), VerifiedAt: receipt.VerifiedAt,
			ID: receipt.ID, ApprovedUntil: nasStitchWireTime(snapshot.ApprovedUntil), Integrity: "clear", nasStitchReadbackBinding: binding}
		cases = append(cases, nasStitchWireFixture{Name: fmt.Sprintf("microseconds-%06d", micros), DBInstant: now.Format("2006-01-02T15:04:05.000000Z"),
			NativeExpiry: snapshot.ApprovedUntil.Format("2006-01-02T15:04:05.000000Z"), ReadbackExpiry: snapshot.ApprovedUntil.Format("2006-01-02T15:04:05.000000Z"), CopiedVerifiedAt: receipt.VerifiedAt,
			LegacyNanoExpiry: snapshot.ApprovedUntil.Format(time.RFC3339Nano), LegacyTaskB64: base64.StdEncoding.EncodeToString(legacyRaw),
			NativeBodyB64: base64.StdEncoding.EncodeToString(syntheticNASStitchWireBody(t, native)), ReadbackBodyB64: base64.StdEncoding.EncodeToString(syntheticNASStitchWireBody(t, readback)),
			ValidationBodyB64: base64.StdEncoding.EncodeToString(syntheticNASStitchWireBody(t, validation)), CurrentBodyB64: base64.StdEncoding.EncodeToString(syntheticNASStitchWireBody(t, current))})
	}
	fixture := struct {
		Schema    string                 `json:"schema"`
		Synthetic bool                   `json:"synthetic"`
		Cases     []nasStitchWireFixture `json:"cases"`
	}{"stoarama.synthetic-stitch-wire-fixtures.v1", true, cases}
	raw, err := json.MarshalIndent(fixture, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	raw = append(raw, '\n')
	const filename = "testdata/nas_stitch_wire_utc6.json"
	if *updateNASStitchWireFixtures {
		if err := os.WriteFile(filename, raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	saved, err := os.ReadFile(filename)
	if err != nil || !bytes.Equal(saved, raw) {
		t.Fatal("actual wire fixtures differ; regenerate explicitly with -update-nas-stitch-wire-fixtures")
	}
}
